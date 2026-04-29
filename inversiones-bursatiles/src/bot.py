"""
src/bot.py
Bot de Telegram con comandos interactivos.
Corre en paralelo al scheduler.

Comandos:
  /run     — Ejecuta el pipeline completo ahora
  /status  — Muestra estado: última ejecución, próxima, errores
  /señales — Lista señales actuales del modelo
  /help    — Ayuda
"""

import logging
import json
import os
from datetime import datetime
import pytz
from telegram import Update
from telegram.ext import (
    Application, CommandHandler, ContextTypes
)

logger = logging.getLogger(__name__)

BOT_TOKEN = os.getenv("TELEGRAM_BOT_TOKEN", "")
TIMEZONE  = os.getenv("TIMEZONE", "America/Argentina/Buenos_Aires")
SIGNALS_PATH = "data/signals_prev.json"
STATUS_PATH  = "data/last_run_status.json"


# ─────────────────────────────────────────────
# Estado global (compartido con el scheduler)
# ─────────────────────────────────────────────

_pipeline_running = False

def set_pipeline_running(val: bool):
    global _pipeline_running
    _pipeline_running = val


# ─────────────────────────────────────────────
# Handlers de comandos
# ─────────────────────────────────────────────

async def cmd_help(update: Update, context: ContextTypes.DEFAULT_TYPE):
    text = (
        "📊 <b>Bot Inversiones Bursátiles</b>\n\n"
        "/run — Ejecutar análisis completo ahora\n"
        "/status — Estado del sistema\n"
        "/señales — Señales activas del modelo\n"
        "/help — Esta ayuda\n\n"
        "El análisis se ejecuta automáticamente al cierre de mercado cada día hábil."
    )
    await update.message.reply_text(text, parse_mode="HTML")


async def cmd_status(update: Update, context: ContextTypes.DEFAULT_TYPE):
    tz  = pytz.timezone(TIMEZONE)
    now = datetime.now(tz).strftime("%d/%m/%Y %H:%M")

    if _pipeline_running:
        await update.message.reply_text(
            f"⏳ Pipeline en ejecución ahora mismo…\nHora: {now}",
            parse_mode="HTML"
        )
        return

    # Leer último estado guardado
    if os.path.exists(STATUS_PATH):
        with open(STATUS_PATH) as f:
            status = json.load(f)
        last_run   = status.get("last_run", "—")
        last_ok    = status.get("success", False)
        duration   = status.get("duration_sec", 0)
        next_run   = status.get("next_run", "—")
        icon = "✅" if last_ok else "❌"
        text = (
            f"📡 <b>Estado del sistema</b>\n"
            f"Hora actual: {now}\n\n"
            f"{icon} Última ejecución: {last_run}\n"
            f"   Duración: {duration:.0f}s\n"
            f"⏰ Próxima: {next_run}\n"
        )
    else:
        text = f"📡 Sistema activo. Aún no hay ejecuciones registradas.\nHora: {now}"

    await update.message.reply_text(text, parse_mode="HTML")


async def cmd_señales(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if not os.path.exists(SIGNALS_PATH):
        await update.message.reply_text(
            "⚠️ No hay señales disponibles aún. Ejecutá /run para generar.",
            parse_mode="HTML"
        )
        return

    with open(SIGNALS_PATH) as f:
        signals = json.load(f)

    # Filtrar solo compras y ventas
    compras = [s for s in signals if "COMPRA" in s.get("signal","")]
    ventas  = [s for s in signals if "VENTA"  in s.get("signal","")]

    lines = ["📋 <b>Señales activas del modelo</b>\n"]

    if compras:
        lines.append("<b>Compras:</b>")
        for s in compras[:8]:
            mkt = "🇦🇷" if s["mercado"]=="MERVAL" else "🇧🇷" if s["mercado"]=="BOVESPA" else "🇺🇸"
            lines.append(
                f"  {mkt} {s['signal']} <code>{s['ticker']}</code> "
                f"Score {s['score_final']:.0f}"
            )

    if ventas:
        lines.append("\n<b>Reducciones/Ventas:</b>")
        for s in ventas[:5]:
            mkt = "🇦🇷" if s["mercado"]=="MERVAL" else "🇧🇷" if s["mercado"]=="BOVESPA" else "🇺🇸"
            lines.append(f"  {mkt} {s['signal']} <code>{s['ticker']}</code>")

    fecha = signals[0].get("fecha","") if signals else ""
    if fecha:
        lines.append(f"\n<i>Actualizado: {fecha}</i>")

    await update.message.reply_text("\n".join(lines), parse_mode="HTML")


async def cmd_run(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Ejecuta el pipeline manualmente desde Telegram."""
    global _pipeline_running

    if _pipeline_running:
        await update.message.reply_text("⏳ Ya hay un análisis en curso. Esperá unos minutos.")
        return

    await update.message.reply_text(
        "🚀 Iniciando análisis completo…\nEsto puede tomar 2-4 minutos.",
        parse_mode="HTML"
    )

    # Import aquí para evitar circular
    import asyncio
    from src.pipeline import run_pipeline
    try:
        set_pipeline_running(True)
        loop = asyncio.get_event_loop()
        await loop.run_in_executor(None, run_pipeline)
        await update.message.reply_text("✅ Análisis completado. Revisá los mensajes anteriores.")
    except Exception as e:
        logger.error(f"Error en /run: {e}")
        await update.message.reply_text(f"❌ Error en el análisis:\n<code>{str(e)[:300]}</code>",
                                         parse_mode="HTML")
    finally:
        set_pipeline_running(False)


# ─────────────────────────────────────────────
# Inicialización
# ─────────────────────────────────────────────

def build_application() -> Application:
    """Construye y retorna la aplicación del bot."""
    if not BOT_TOKEN:
        raise ValueError("TELEGRAM_BOT_TOKEN no está configurado.")

    app = Application.builder().token(BOT_TOKEN).build()

    app.add_handler(CommandHandler("help",     cmd_help))
    app.add_handler(CommandHandler("start",    cmd_help))
    app.add_handler(CommandHandler("status",   cmd_status))
    app.add_handler(CommandHandler("señales",  cmd_señales))
    app.add_handler(CommandHandler("senales",  cmd_señales))  # alias sin tilde
    app.add_handler(CommandHandler("run",      cmd_run))

    return app
