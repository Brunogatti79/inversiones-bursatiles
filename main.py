"""
main.py - Entry point
Servidor HTTP en hilo principal (Railway necesita respuesta en PORT)
Bot de Telegram + Scheduler en hilos daemon
"""

import logging
import os
import sys
import threading
from datetime import datetime

import pytz
from apscheduler.schedulers.background import BackgroundScheduler
from apscheduler.triggers.cron import CronTrigger
from dotenv import load_dotenv

load_dotenv()

LOG_LEVEL = logging.DEBUG if os.getenv("DEBUG_MODE","false").lower()=="true" else logging.INFO
logging.basicConfig(
    level=LOG_LEVEL,
    format="%(asctime)s [%(levelname)s] %(name)s - %(message)s",
    handlers=[logging.StreamHandler(sys.stdout)]
)
logger = logging.getLogger(__name__)

from src.pipeline import run_pipeline
from src.notifier import send_startup_message
from src.bot      import build_application


def start_scheduler():
    run_time = os.getenv("RUN_TIME_UTC", "15:00")
    h, m = run_time.split(":")
    tz = os.getenv("TIMEZONE", "America/Argentina/Buenos_Aires")
    scheduler = BackgroundScheduler(timezone=pytz.UTC)
    scheduler.add_job(
        func=run_pipeline,
        trigger=CronTrigger(hour=int(h), minute=int(m), timezone=pytz.UTC),
        id="daily_analysis",
        name="Analisis diario de mercados",
        misfire_grace_time=3600,
        coalesce=True,
    )
    scheduler.start()
    logger.info(f"Scheduler activo - ejecucion diaria a las {run_time} UTC ({tz})")
    return scheduler


def run_bot_thread():
    """Corre el bot en un hilo separado (no bloqueante para el servidor HTTP)."""
    try:
        import asyncio
        loop = asyncio.new_event_loop()
        asyncio.set_event_loop(loop)
        app = build_application()
        logger.info("Bot de Telegram iniciado - modo polling")
        app.run_polling(allowed_updates=["message"])
    except Exception as e:
        logger.error(f"Error en bot: {e}")


def run_http_server():
    """Servidor HTTP en hilo principal - Railway necesita esto."""
    import os
    from http.server import HTTPServer, SimpleHTTPRequestHandler

    OUTPUT_DIR = "outputs"
    os.makedirs(OUTPUT_DIR, exist_ok=True)
    PORT = int(os.getenv("PORT", 8080))

    class Handler(SimpleHTTPRequestHandler):
        def __init__(self, *args, **kwargs):
            super().__init__(*args, directory=OUTPUT_DIR, **kwargs)
        def log_message(self, format, *args):
            pass

    httpd = HTTPServer(("0.0.0.0", PORT), Handler)
    logger.info(f"Servidor HTTP activo en puerto {PORT}")
    httpd.serve_forever()


def main():
    logger.info("=== Inversiones Bursatiles - Iniciando ===")
    logger.info(f"Python {sys.version}")
    logger.info(f"RUN_TIME_UTC: {os.getenv('RUN_TIME_UTC','15:00')}")
    logger.info(f"TIMEZONE: {os.getenv('TIMEZONE','America/Argentina/Buenos_Aires')}")

    for var in ["TELEGRAM_BOT_TOKEN", "TELEGRAM_CHAT_ID"]:
        if not os.getenv(var):
            logger.error(f"Variable de entorno faltante: {var}")
            sys.exit(1)

    # Scheduler en hilo daemon
    scheduler = start_scheduler()

    # Bot de Telegram en hilo daemon
    bot_thread = threading.Thread(target=run_bot_thread, daemon=True)
    bot_thread.start()

    # Notificar inicio (con pequeña pausa para que el bot arranque)
    import time
    time.sleep(3)
    send_startup_message()

    # Ejecutar pipeline ahora si se pasa el argumento
    if "--run-now" in sys.argv:
        logger.info("Ejecutando pipeline ahora...")
        t = threading.Thread(target=run_pipeline, daemon=True)
        t.start()

    # Servidor HTTP en hilo PRINCIPAL (bloquea aqui - Railway necesita esto)
    try:
        run_http_server()
    except KeyboardInterrupt:
        logger.info("Deteniendo...")
    finally:
        scheduler.shutdown()
        logger.info("Adios.")


if __name__ == "__main__":
    main()
