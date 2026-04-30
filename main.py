"""
main.py
Bot de Telegram + Scheduler.
El servidor HTTP lo maneja start_server.py (Railway entry point).
"""

import logging
import os
import sys
import threading
import time
import asyncio

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


async def run_bot_async():
    from src.bot import build_application
    app = build_application()
    await app.initialize()
    await app.start()
    logger.info("Bot de Telegram activo")
    await app.updater.start_polling(allowed_updates=["message"])
    while True:
        await asyncio.sleep(3600)


def run_bot_thread():
    loop = asyncio.new_event_loop()
    asyncio.set_event_loop(loop)
    try:
        loop.run_until_complete(run_bot_async())
    except Exception as e:
        logger.error(f"Error en bot: {e}")
    finally:
        loop.close()


def main():
    logger.info("=== Inversiones Bursatiles - Iniciando ===")
    logger.info(f"RUN_TIME_UTC: {os.getenv('RUN_TIME_UTC','15:00')}")
    logger.info(f"TIMEZONE: {os.getenv('TIMEZONE','America/Argentina/Buenos_Aires')}")

    for var in ["TELEGRAM_BOT_TOKEN", "TELEGRAM_CHAT_ID"]:
        if not os.getenv(var):
            logger.error(f"Variable faltante: {var}")
            sys.exit(1)

    # Scheduler en background
    scheduler = start_scheduler()

    # Bot en hilo daemon
    bot_thread = threading.Thread(target=run_bot_thread, daemon=True)
    bot_thread.start()

    # Notificar inicio
    time.sleep(3)
    send_startup_message()

    # Pipeline inmediato si se pide
    if "--run-now" in sys.argv:
        t = threading.Thread(target=run_pipeline, daemon=True)
        t.start()

    # Mantener el proceso vivo
    try:
        while True:
            time.sleep(60)
    except KeyboardInterrupt:
        logger.info("Deteniendo...")
    finally:
        scheduler.shutdown()
        logger.info("Adios.")


if __name__ == "__main__":
    main()
