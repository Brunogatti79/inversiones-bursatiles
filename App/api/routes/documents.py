from fastapi import APIRouter, HTTPException
from sqlalchemy import select, func
from pathlib import Path

from api.dependencies import DBSession
from core.database import Document
from core.vector_store import collection_count
from ingest.pipeline import reindex_folder, ingest_file, delete_file_index
from models.schemas import DocumentOut, ReindexResponse, StatusResponse
from config import settings

router = APIRouter(prefix="/documents", tags=["documents"])


@router.get("/", response_model=list[DocumentOut])
async def list_documents(session: DBSession, market: str | None = None, limit: int = 100, offset: int = 0):
    q = select(Document).order_by(Document.indexed_at.desc()).limit(limit).offset(offset)
    if market:
        q = q.where(Document.market == market)
    return (await session.execute(q)).scalars().all()


@router.get("/status", response_model=StatusResponse)
async def get_status(session: DBSession):
    total_docs = await session.scalar(select(func.count(Document.id)))
    return StatusResponse(
        docs_folder     = str(settings.docs_path),
        total_documents = total_docs or 0,
        total_chunks    = collection_count(),
        llm_provider    = settings.llm_provider,
        embedding_model = settings.embedding_model
    )


@router.post("/reindex", response_model=ReindexResponse)
async def reindex(session: DBSession):
    return ReindexResponse(**(await reindex_folder(session)))


@router.post("/ingest")
async def ingest_single(file_path: str, session: DBSession):
    if not Path(file_path).exists():
        raise HTTPException(status_code=404, detail="Archivo no encontrado")
    return await ingest_file(file_path, session)


@router.delete("/{file_name}")
async def delete_document(file_name: str, session: DBSession):
    result = await session.execute(select(Document).where(Document.file_name == file_name))
    doc    = result.scalar_one_or_none()
    if not doc:
        raise HTTPException(status_code=404, detail="Documento no encontrado")
    await delete_file_index(doc.file_path, session)
    return {"message": f"Documento '{file_name}' eliminado del índice"}
