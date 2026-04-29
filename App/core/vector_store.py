import chromadb
from chromadb.config import Settings as ChromaSettings
from config import settings
from typing import Optional


_client: Optional[chromadb.PersistentClient] = None
_collection = None
COLLECTION_NAME = "sintesis_inversor"


def get_client() -> chromadb.PersistentClient:
    global _client
    if _client is None:
        _client = chromadb.PersistentClient(
            path=str(settings.chroma_dir),
            settings=ChromaSettings(anonymized_telemetry=False)
        )
    return _client


def get_collection():
    global _collection
    if _collection is None:
        client = get_client()
        _collection = client.get_or_create_collection(
            name=COLLECTION_NAME,
            metadata={"hnsw:space": "cosine"}
        )
    return _collection


def upsert_chunks(
    ids:       list[str],
    embeddings: list[list[float]],
    documents: list[str],
    metadatas: list[dict]
):
    get_collection().upsert(
        ids=ids,
        embeddings=embeddings,
        documents=documents,
        metadatas=metadatas
    )


def delete_by_file(file_path: str):
    col = get_collection()
    results = col.get(where={"file_path": file_path})
    if results["ids"]:
        col.delete(ids=results["ids"])


def query(
    query_embedding: list[float],
    n_results: int = 8,
    where: Optional[dict] = None
) -> dict:
    col = get_collection()
    kwargs = {
        "query_embeddings": [query_embedding],
        "n_results":        n_results,
        "include":          ["documents", "metadatas", "distances"]
    }
    if where:
        kwargs["where"] = where
    return col.query(**kwargs)


def collection_count() -> int:
    return get_collection().count()
