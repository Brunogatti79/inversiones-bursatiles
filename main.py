"""
main.py
Punto de entrada de la aplicación.
Levanta:
  - APScheduler con el cron diario
  - Bot de Telegram en modo polling
Ambos corren en el mismo proceso con threading.
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

# ─── Cargar .env (sólo en desarrollo local) ───────────────────────
load_dotenv()

# ─── Logging ──────────────────────────────────────────────────────
LOG_LEVEL = logging.DEBUG if os.getenv("DEBUG_MODE","false").lower()=="true" else logging.INFO
logging.basicConfig(
    level=LOG_LEVEL,
    format="%(asctime)s [%(levelname)s] %(name)s — %(message)s",
    handlers=[logging.StreamHandler(sys.stdout)]
)
logger = logging.getLogger(__name__)

# ─── Imports propios (después de load_dotenv) ─────────────────────
from src.pipeline import run_pipeline
from src.notifier import send_startup_message
from src.bot      import build_application


def start_scheduler():
    """Inicia el cron diario con APScheduler."""
    tz       = os.getenv("TIMEZONE", "America/Argentina/Buenos_Aires")
    run_time = os.getenv("RUN_TIME_UTC", "21:30")
    h, m     = run_time.split(":")

    scheduler = BackgroundScheduler(timezone=pytz.UTC)
    scheduler.add_job(
        func=run_pipeline,
        trigger=CronTrigger(hour=int(h), minute=int(m), timezone=pytz.UTC),
        id="daily_analysis",
        name="Análisis diario de mercados",
        misfire_grace_time=3600,   # tolera hasta 1h de retraso
        coalesce=True,
    )
    scheduler.start()
    logger.info(f"Scheduler activo — ejecución diaria a las {run_time} UTC ({tz})")
    return scheduler


def run_bot():
    """Corre el bot de Telegram en el hilo principal (blocking)."""
    app = build_application()
    logger.info("Bot de Telegram iniciado — modo polling")
    app.run_polling(allowed_updates=["message"])


def main():
    logger.info("═══ Inversiones Bursátiles — Iniciando ═══")
    logger.info(f"Python {sys.version}")
    logger.info(f"RUN_TIME_UTC: {os.getenv('RUN_TIME_UTC','21:30')}")
    logger.info(f"TIMEZONE: {os.getenv('TIMEZONE','America/Argentina/Buenos_Aires')}")

    # Validar variables obligatorias
    for var in ["TELEGRAM_BOT_TOKEN", "TELEGRAM_CHAT_ID"]:
        if not os.getenv(var):
            logger.error(f"Variable de entorno faltante: {var}")
            sys.exit(1)

    # Iniciar scheduler en hilo separado (no bloqueante)
    scheduler = start_scheduler()

    # Notificar inicio por Telegram
    send_startup_message()

    # Si se pasa arg --run-now, ejecutar inmediatamente
    if "--run-now" in sys.argv:
        logger.info("Argumento --run-now detectado. Ejecutando pipeline ahora...")
        t = threading.Thread(target=run_pipeline, daemon=True)
        t.start()

    # Bot en hilo principal (bloqueante hasta Ctrl+C)
    try:
        run_bot()
    except KeyboardInterrupt:
        logger.info("Deteniendo...")
    finally:
        scheduler.shutdown()
        logger.info("Scheduler detenido. Adiós.")


if __name__ == "__main__":
    main()
