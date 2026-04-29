import hashlib
from pathlib import Path
from datetime import datetime
from sqlalchemy import select, delete
from sqlalchemy.ext.asyncio import AsyncSession

from config import settings
from core.database import Document
from core.vector_store import upsert_chunks, delete_by_file
from core.embeddings import embed_texts
from ingest.parsers import PARSERS, SUPPORTED_EXTENSIONS


CHUNK_SIZE    = 500
CHUNK_OVERLAP = 80

MARKET_PATTERNS = {
    "merval":  ["merval", "argentina", "arg", "byma", ".ba"],
    "bovespa": ["bovespa", "brasil", "bra", ".sa", "b3"],
    "sp500":   ["sp500", "s&p", "usa", "eeuu", "nasdaq"],
}


def _hash_file(path: str) -> str:
    h = hashlib.md5()
    with open(path, "rb") as f:
        for chunk in iter(lambda: f.read(8192), b""):
            h.update(chunk)
    return h.hexdigest()


def _detect_market(file_path: str) -> str | None:
    text = file_path.lower()
    for market, keywords in MARKET_PATTERNS.items():
        if any(kw in text for kw in keywords):
            return market
    return None


def _chunk_text(text: str) -> list[str]:
    words = text.split()
    chunks, i = [], 0
    while i < len(words):
        chunks.append(" ".join(words[i:i + CHUNK_SIZE]))
        i += CHUNK_SIZE - CHUNK_OVERLAP
    return [c for c in chunks if len(c.strip()) > 50]


async def ingest_file(file_path: str, session: AsyncSession) -> dict:
    path = Path(file_path)
    ext  = path.suffix.lower()

    if ext not in SUPPORTED_EXTENSIONS:
        return {"status": "skipped", "reason": "unsupported extension"}

    file_hash   = _hash_file(file_path)
    modified_at = datetime.fromtimestamp(path.stat().st_mtime)

    result   = await session.execute(select(Document).where(Document.file_path == str(path.resolve())))
    existing = result.scalar_one_or_none()

    if existing and existing.file_hash == file_hash:
        return {"status": "skipped", "reason": "unchanged"}

    try:
        raw_text = PARSERS[ext](file_path)
    except Exception as e:
        return {"status": "error", "reason": str(e)}

    if not raw_text.strip():
        return {"status": "skipped", "reason": "empty content"}

    chunks = _chunk_text(raw_text)
    if not chunks:
        return {"status": "skipped", "reason": "no chunks"}

    embeddings = embed_texts(chunks)
    market     = _detect_market(file_path)
    abs_path   = str(path.resolve())

    delete_by_file(abs_path)

    upsert_chunks(
        ids       = [f"{file_hash}_{i}" for i in range(len(chunks))],
        embeddings= embeddings,
        documents = chunks,
        metadatas = [
            {
                "file_path":   abs_path,
                "file_name":   path.name,
                "file_type":   ext.lstrip("."),
                "market":      market or "general",
                "chunk_index": i,
            }
            for i in range(len(chunks))
        ]
    )

    if existing:
        existing.file_hash   = file_hash
        existing.chunk_count = len(chunks)
        existing.indexed_at  = datetime.utcnow()
        existing.modified_at = modified_at
        existing.market      = market
        existing.size_bytes  = path.stat().st_size
    else:
        session.add(Document(
            file_path   = abs_path,
            file_name   = path.name,
            file_type   = ext.lstrip("."),
            market      = market,
            file_hash   = file_hash,
            chunk_count = len(chunks),
            modified_at = modified_at,
            size_bytes  = path.stat().st_size,
        ))

    await session.commit()
    return {"status": "indexed", "chunks": len(chunks)}


async def reindex_folder(session: AsyncSession) -> dict:
    folder = settings.docs_path
    if not folder.exists():
        return {"processed": 0, "skipped": 0, "errors": 1, "message": f"Carpeta no encontrada: {folder}"}

    processed = skipped = errors = 0
    for path in folder.rglob("*"):
        if not path.is_file():
            continue
        r = await ingest_file(str(path), session)
        if   r["status"] == "indexed": processed += 1
        elif r["status"] == "error":   errors    += 1
        else:                          skipped   += 1

    return {
        "processed": processed,
        "skipped":   skipped,
        "errors":    errors,
        "message":   f"Reindexación completa: {processed} procesados, {skipped} sin cambios, {errors} errores"
    }


async def delete_file_index(file_path: str, session: AsyncSession):
    abs_path = str(Path(file_path).resolve())
    delete_by_file(abs_path)
    await session.execute(delete(Document).where(Document.file_path == abs_path))
    await session.commit()
