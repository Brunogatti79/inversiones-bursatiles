"""
main.py
Entry point: scheduler + bot de Telegram + servidor HTTP para el dashboard.
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
    format="%(asctime)s [%(levelname)s] %(name)s — %(message)s",
    handlers=[logging.StreamHandler(sys.stdout)]
)
logger = logging.getLogger(__name__)

from src.pipeline import run_pipeline
from src.notifier import send_startup_message
from src.bot      import build_application
from server       import start_server_thread


def start_scheduler():
    tz       = os.getenv("TIMEZONE", "America/Argentina/Buenos_Aires")
    run_time = os.getenv("RUN_TIME_UTC", "15:00")
    h, m     = run_time.split(":")

    scheduler = BackgroundScheduler(timezone=pytz.UTC)
    scheduler.add_job(
        func=run_pipeline,
        trigger=CronTrigger(hour=int(h), minute=int(m), timezone=pytz.UTC),
        id="daily_analysis",
        name="Análisis diario de mercados",
        misfire_grace_time=3600,
        coalesce=True,
    )
    scheduler.start()
    logger.info(f"Scheduler activo — ejecución diaria a las {run_time} UTC ({tz})")
    return scheduler


def run_bot():
    app = build_application()
    logger.info("Bot de Telegram iniciado — modo polling")
    app.run_polling(allowed_updates=["message"])


def main():
    logger.info("═══ Inversiones Bursátiles — Iniciando ═══")

    for var in ["TELEGRAM_BOT_TOKEN", "TELEGRAM_CHAT_ID"]:
        if not os.getenv(var):
            logger.error(f"Variable de entorno faltante: {var}")
            sys.exit(1)

    # Servidor HTTP para el dashboard (hilo daemon)
    start_server_thread()

    # Scheduler en background
    scheduler = start_scheduler()

    # Notificar inicio
    send_startup_message()

    # Ejecutar ahora si se pasa el argumento
    if "--run-now" in sys.argv:
        logger.info("Ejecutando pipeline ahora...")
        t = threading.Thread(target=run_pipeline, daemon=True)
        t.start()

    try:
        run_bot()
    except KeyboardInterrupt:
        logger.info("Deteniendo...")
    finally:
        scheduler.shutdown()
        logger.info("Adiós.")


if __name__ == "__main__":
    main()
