from fastapi import APIRouter
from fastapi.responses import StreamingResponse
from api.dependencies import DBSession
from agent.rag import run_agent, stream_agent
from models.schemas import AgentRequest, AgentResponse

router = APIRouter(prefix="/agent", tags=["agent"])

VALID_OUTPUT_TYPES = {"resumen", "señales", "riesgos", "kpis", "narrativa"}


@router.post("/query", response_model=AgentResponse)
async def query_agent(req: AgentRequest, session: DBSession):
    otype = req.output_type if req.output_type in VALID_OUTPUT_TYPES else "resumen"
    return await run_agent(query=req.query, output_type=otype, session=session, market=req.market)


@router.post("/stream")
async def stream_query(req: AgentRequest):
    otype = req.output_type if req.output_type in VALID_OUTPUT_TYPES else "resumen"
    return StreamingResponse(
        stream_agent(query=req.query, output_type=otype, market=req.market),
        media_type = "text/event-stream",
        headers    = {"Cache-Control": "no-cache", "X-Accel-Buffering": "no"}
    )


@router.get("/output-types")
async def list_output_types():
    return {"types": [
        {"id": "resumen",   "label": "Resumen ejecutivo"},
        {"id": "señales",   "label": "Señales de inversión"},
        {"id": "riesgos",   "label": "Análisis de riesgos"},
        {"id": "kpis",      "label": "KPIs y métricas"},
        {"id": "narrativa", "label": "Narrativa de mercado"},
    ]}
