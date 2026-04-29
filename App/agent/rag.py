import time
from typing import AsyncIterator
from sqlalchemy.ext.asyncio import AsyncSession

from core.embeddings import embed_query
from core.vector_store import query as vstore_query
from core.database import QueryLog
from agent.llm_client import get_llm_client
from agent.prompts import build_prompt
from models.schemas import AgentResponse


def _build_context(results: dict) -> tuple[str, list[str]]:
    docs   = results.get("documents", [[]])[0]
    metas  = results.get("metadatas", [[]])[0]
    dists  = results.get("distances", [[]])[0]
    parts, sources = [], []

    for i, (doc, meta, dist) in enumerate(zip(docs, metas, dists)):
        score = round(1 - dist, 3)
        src   = f"{meta.get('file_name', '?')} (mercado: {meta.get('market', '?')}, relevancia: {score})"
        parts.append(f"[Fuente {i+1}: {src}]\n{doc}")
        if src not in sources:
            sources.append(src)

    return "\n\n---\n\n".join(parts), sources


async def run_agent(
    query:       str,
    output_type: str,
    session:     AsyncSession,
    market:      str | None = None,
    n_results:   int = 8
) -> AgentResponse:
    start       = time.monotonic()
    q_embedding = embed_query(query)
    where       = {"market": market} if market else None
    results     = vstore_query(q_embedding, n_results=n_results, where=where)
    context, sources = _build_context(results)
    chunks_used = len(results.get("documents", [[]])[0])

    if not context.strip():
        answer = "No se encontró información relevante en los documentos indexados para esta consulta."
    else:
        system, user = build_prompt(query, context, output_type)
        answer = await get_llm_client().complete(system, user)

    latency = (time.monotonic() - start) * 1000
    session.add(QueryLog(
        query            = query,
        response_summary = answer[:500],
        market_filter    = market,
        chunks_used      = chunks_used,
        latency_ms       = round(latency, 1)
    ))
    await session.commit()

    return AgentResponse(
        answer      = answer,
        sources     = sources,
        chunks_used = chunks_used,
        output_type = output_type
    )


async def stream_agent(
    query:       str,
    output_type: str,
    market:      str | None = None,
    n_results:   int = 8
) -> AsyncIterator[str]:
    q_embedding = embed_query(query)
    where       = {"market": market} if market else None
    results     = vstore_query(q_embedding, n_results=n_results, where=where)
    context, sources = _build_context(results)

    yield f"data: sources={','.join(sources)}\n\n"

    if not context.strip():
        yield "data: No se encontró información relevante.\n\n"
        yield "data: [DONE]\n\n"
        return

    system, user = build_prompt(query, context, output_type)
    async for token in get_llm_client().stream(system, user):
        yield f"data: {token}\n\n"

    yield "data: [DONE]\n\n"
