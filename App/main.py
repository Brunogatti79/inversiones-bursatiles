import asyncio
import logging
import uvicorn
from contextlib import asynccontextmanager
from fastapi import FastAPI
from fastapi.middleware.cors import CORSMiddleware
from fastapi.staticfiles import StaticFiles
from pathlib import Path

from config import settings
from core.database import init_db, SessionLocal
from ingest.pipeline import reindex_folder
from ingest.watcher import start_watcher, stop_watcher
from api.routes import documents, search, agent

logging.basicConfig(
    level  = logging.INFO,
    format = "%(asctime)s [%(levelname)s] %(name)s: %(message)s"
)
logger = logging.getLogger(__name__)


async def _background_reindex():
    logger.info("Reindexación en background iniciada...")
    async with SessionLocal() as session:
        result = await reindex_folder(session)
        logger.info(result["message"])


@asynccontextmanager
async def lifespan(app: FastAPI):
    logger.info("Inicializando base de datos...")
    await init_db()

    # Indexar en background — el servidor arranca de inmediato
    asyncio.create_task(_background_reindex())

    logger.info("Iniciando watcher...")
    start_watcher(asyncio.get_event_loop())

    yield

    logger.info("Deteniendo watcher...")
    stop_watcher()


app = FastAPI(
    title       = "Síntesis Inversor API",
    description = "Backend RAG para síntesis de información financiera local",
    version     = "1.0.0",
    lifespan    = lifespan
)

app.add_middleware(
    CORSMiddleware,
    allow_origins = ["*"],
    allow_methods = ["*"],
    allow_headers = ["*"],
)

app.include_router(documents.router, prefix="/api")
app.include_router(search.router,    prefix="/api")
app.include_router(agent.router,     prefix="/api")


@app.get("/health")
async def health():
    return {"status": "ok", "llm": settings.llm_provider}


# Servir frontend — debe ir al final para no capturar rutas de la API
frontend_path = Path(__file__).parent / "frontend"
if frontend_path.exists():
    app.mount("/", StaticFiles(directory=str(frontend_path), html=True), name="frontend")


if __name__ == "__main__":
    uvicorn.run("main:app", host=settings.host, port=settings.port, reload=False)
