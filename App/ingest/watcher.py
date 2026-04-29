import asyncio
import logging
from pathlib import Path
from watchdog.observers import Observer
from watchdog.events import FileSystemEventHandler

from config import settings
from core.database import SessionLocal
from ingest.pipeline import ingest_file, delete_file_index
from ingest.parsers import SUPPORTED_EXTENSIONS

logger = logging.getLogger(__name__)


class IngestHandler(FileSystemEventHandler):
    def __init__(self, loop: asyncio.AbstractEventLoop):
        self.loop = loop

    def _valid(self, path: str) -> bool:
        return Path(path).suffix.lower() in SUPPORTED_EXTENSIONS

    def _run(self, coro):
        asyncio.run_coroutine_threadsafe(coro, self.loop)

    def on_created(self, event):
        if not event.is_directory and self._valid(event.src_path):
            self._run(self._ingest(event.src_path))

    def on_modified(self, event):
        if not event.is_directory and self._valid(event.src_path):
            self._run(self._ingest(event.src_path))

    def on_deleted(self, event):
        if not event.is_directory and self._valid(event.src_path):
            self._run(self._delete(event.src_path))

    async def _ingest(self, path: str):
        async with SessionLocal() as session:
            result = await ingest_file(path, session)
            logger.info(f"[watcher] {Path(path).name}: {result}")

    async def _delete(self, path: str):
        async with SessionLocal() as session:
            await delete_file_index(path, session)
            logger.info(f"[watcher] Eliminado: {Path(path).name}")


_observer: Observer | None = None


def start_watcher(loop: asyncio.AbstractEventLoop):
    global _observer
    handler   = IngestHandler(loop)
    _observer = Observer()
    _observer.schedule(handler, str(settings.docs_path), recursive=True)
    _observer.start()
    logger.info(f"[watcher] Monitoreando: {settings.docs_path}")


def stop_watcher():
    global _observer
    if _observer:
        _observer.stop()
        _observer.join()
