from fastapi import APIRouter
from api.dependencies import DBSession
from core.embeddings import embed_query
from core.vector_store import query as vstore_query
from models.schemas import SearchRequest, SearchResponse, SearchResult

router = APIRouter(prefix="/search", tags=["search"])


@router.post("/", response_model=SearchResponse)
async def semantic_search(req: SearchRequest, session: DBSession):
    q_embedding = embed_query(req.query)
    where       = {"market": req.market} if req.market else None
    results     = vstore_query(q_embedding, n_results=req.n_results, where=where)

    docs  = results.get("documents", [[]])[0]
    metas = results.get("metadatas", [[]])[0]
    dists = results.get("distances", [[]])[0]

    items = [
        SearchResult(
            text        = doc,
            file_name   = meta.get("file_name", ""),
            file_type   = meta.get("file_type", ""),
            market      = meta.get("market"),
            score       = round(1 - dist, 4),
            chunk_index = meta.get("chunk_index", 0)
        )
        for doc, meta, dist in zip(docs, metas, dists)
    ]
    return SearchResponse(results=items, total=len(items))
