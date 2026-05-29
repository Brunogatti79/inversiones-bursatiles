"""
main.py — Bot de Telegram + Scheduler.
start_server.py es el entry point de Railway (HTTP server + webhook).
"""
import logging, os, sys, threading, time, asyncio
import pytz
from apscheduler.schedulers.background import BackgroundScheduler
from apscheduler.triggers.cron import CronTrigger
from dotenv import load_dotenv

load_dotenv()

logging.basicConfig(
    level=logging.DEBUG if os.getenv("DEBUG_MODE","false").lower()=="true" else logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s - %(message)s",
    handlers=[logging.StreamHandler(sys.stdout)]
)
logger = logging.getLogger(__name__)

from src.pipeline import run_pipeline
from src.notifier import send_startup_message


def start_scheduler():
    run_time = os.getenv("RUN_TIME_UTC", "15:00")
    h, m = run_time.split(":")
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
    logger.info(f"Scheduler activo - ejecucion diaria a las {run_time} UTC")
    return scheduler


async def _bot_lifecycle():
    """
    Ciclo de vida del bot usando API de bajo nivel (sin run_polling)
    para evitar conflictos de event loop en threads secundarios.
    """
    from src.bot import build_application
    app = build_application()

    # Reset webhook y limpiar updates pendientes
    await app.bot.delete_webhook(drop_pending_updates=True)

    # Inicializar y arrancar componentes internos
    await app.initialize()
    await app.start()

    # Arrancar el updater (polling) — bajo nivel, sin signal handlers
    await app.updater.start_polling(
        poll_interval=1.0,
        timeout=10,
        drop_pending_updates=True,
    )

    logger.info("Bot de Telegram activo — escuchando comandos")

    # Mantener corriendo indefinidamente
    stop_event = asyncio.Event()
    try:
        await stop_event.wait()
    finally:
        await app.updater.stop()
        await app.stop()
        await app.shutdown()


def run_bot_thread():
    """Corre el bot en un thread daemon con su propio event loop."""
    loop = asyncio.new_event_loop()
    asyncio.set_event_loop(loop)
    try:
        loop.run_until_complete(_bot_lifecycle())
    except Exception as e:
        logger.error(f"Error en bot: {e}")
    # No cerramos el loop — es un daemon thread, muere con el proceso


def main():
    logger.info("=== Inversiones Bursatiles - Iniciando ===")
    logger.info(f"RUN_TIME_UTC: {os.getenv('RUN_TIME_UTC','15:00')}")
    logger.info(f"TIMEZONE: {os.getenv('TIMEZONE','America/Argentina/Buenos_Aires')}")

    for var in ["TELEGRAM_BOT_TOKEN", "TELEGRAM_CHAT_ID"]:
        if not os.getenv(var):
            logger.error(f"Variable faltante: {var}")
            sys.exit(1)

    scheduler = start_scheduler()

    bot_thread = threading.Thread(target=run_bot_thread, daemon=True)
    bot_thread.start()

    time.sleep(3)
    send_startup_message()

    if "--run-now" in sys.argv:
        threading.Thread(target=run_pipeline, daemon=True).start()

    try:
        while True:
            time.sleep(60)
    except KeyboardInterrupt:
        logger.info("Deteniendo...")
    finally:
        scheduler.shutdown()


if __name__ == "__main__":
    main()
