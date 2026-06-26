"""
src/bot.py
Bot de Telegram con comandos interactivos.
Corre en paralelo al scheduler.
 
Comandos:
  /run     — Ejecuta el pipeline completo ahora
  /status  — Muestra estado: última ejecución, próxima, errores
  /señales — Lista señales actuales del modelo
  /compra  — Registrar compra: /compra TICKER PRECIO CANTIDAD
  /venta   — Registrar venta: /venta TICKER PRECIO CANTIDAD
  /portfolio — Ver posiciones actuales
  /backfill_stops — Asignar stop/target a posiciones sin uno (one-shot, post-fix 26/06/2026)
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
PORTFOLIO_PATH = "data/portfolio.json"
 
 
# ─────────────────────────────────────────────
# Estado global (compartido con el scheduler)
# ─────────────────────────────────────────────
 
_pipeline_running = False
 
def set_pipeline_running(val: bool):
    global _pipeline_running
    _pipeline_running = val
 
 
# ─────────────────────────────────────────────
# Helpers de portfolio
# ─────────────────────────────────────────────
 
def _load_portfolio():
    """Carga el portfolio.json."""
    if os.path.exists(PORTFOLIO_PATH):
        with open(PORTFOLIO_PATH) as f:
            return json.load(f)
    return {"last_updated": "", "positions": [], "reglas": {}}
 
 
def _save_portfolio(portfolio):
    """Guarda el portfolio.json."""
    portfolio["last_updated"] = datetime.now().strftime("%Y-%m-%d %H:%M")
    os.makedirs(os.path.dirname(PORTFOLIO_PATH), exist_ok=True)
    with open(PORTFOLIO_PATH, "w") as f:
        json.dump(portfolio, f, ensure_ascii=False, indent=2)
 
 
def _push_portfolio_to_github():
    """Intenta pushear el portfolio.json actualizado a GitHub."""
    from src.github_persistence import push_file
    return push_file(PORTFOLIO_PATH, f"bot: actualización portfolio {datetime.now().strftime('%Y-%m-%d %H:%M')}")
 
 
# ─────────────────────────────────────────────
# Handlers de comandos
# ─────────────────────────────────────────────
 
async def cmd_help(update: Update, context: ContextTypes.DEFAULT_TYPE):
    text = (
        "📊 <b>Bot Inversiones Bursátiles</b>\n\n"
        "/run — Ejecutar análisis completo ahora\n"
        "/status — Estado del sistema\n"
        "/señales — Señales activas del modelo\n"
        "/compra TICKER PRECIO_USD CANTIDAD — Registrar compra (precio en USD/acc)\n"
        "/venta TICKER PRECIO_USD CANTIDAD — Registrar venta (precio en USD/acc)\n"
        "/portfolio — Ver posiciones actuales\n"
        "/backfill_stops — Asignar stop/target a posiciones sin uno (correr 1 vez post-fix)\n"
        "/bootstrap_macro — Precarga 3 años de historia FRED (1 sola vez)\n"
        "/help — Esta ayuda\n\n"
        "Ejemplos:\n"
        "<code>/compra GGAL.BA 1.59 100</code>  (precio en USD)\n"
        "<code>/venta MSFT 14.73 5</code>  (precio en USD)\n\n"
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
 
 
async def cmd_compra(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Registrar compra: /compra TICKER PRECIO_USD CANTIDAD (precio en USD/acción, igual que broker)

    FIX 26/06/2026: esta lógica era una TERCERA implementación independiente
    de "registrar compra" (las otras dos: tracker.py/start_server.py, ya
    consolidadas en src/execution/order_engine.py) -- también hardcodeaba
    stop_loss=None, target=None. Se delega a order_engine.execute_compra()
    para que una compra registrada por Telegram tenga el mismo stop/target
    real que una registrada desde el dashboard, y para no mantener un
    cuarto lugar con la misma lógica de promediado/persistencia.
    """
    args = context.args
    if not args or len(args) < 3:
        await update.message.reply_text(
            "⚠️ Formato: <code>/compra TICKER PRECIO_USD CANTIDAD</code>\n"
            "Ejemplo: <code>/compra GGAL.BA 1.59 100</code>  (precio en USD/acc)\n"
            "Ejemplo: <code>/compra MSFT 14.73 36</code>",
            parse_mode="HTML"
        )
        return

    ticker = args[0].upper()
    try:
        precio = float(args[1].replace(",", "."))
        cantidad = int(args[2])
    except ValueError:
        await update.message.reply_text("❌ Precio y cantidad deben ser números.", parse_mode="HTML")
        return

    if precio <= 0 or cantidad <= 0:
        await update.message.reply_text("❌ Precio y cantidad deben ser positivos.", parse_mode="HTML")
        return

    from src.execution import order_engine
    result = order_engine.execute_compra(
        ticker=ticker, precio_raw=precio, total_usd_raw=0, cantidad=cantidad,
    )

    if result.get("status") != "ok":
        await update.message.reply_text(f"❌ {result.get('error', 'Error desconocido')}", parse_mode="HTML")
        return

    msg = f"✅ <b>{result['msg']}</b>"
    if not result.get("pushed"):
        msg += f"\n\n⚠️ {result.get('push_warning', 'GitHub no disponible — guardado local')}"
    else:
        msg += "\n\n📤 Portfolio actualizado en GitHub"

    await update.message.reply_text(msg, parse_mode="HTML")
    logger.info(f"Compra registrada (bot): {ticker} {cantidad} @ {precio}")


async def cmd_venta(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Registrar venta: /venta TICKER PRECIO CANTIDAD

    FIX 26/06/2026: delega en order_engine.execute_venta() -- ver nota en
    cmd_compra (misma consolidación, mismo motivo).
    """
    args = context.args
    if not args or len(args) < 3:
        await update.message.reply_text(
            "⚠️ Formato: <code>/venta TICKER PRECIO CANTIDAD</code>\n"
            "Ejemplo: <code>/venta MSFT 420 36</code>",
            parse_mode="HTML"
        )
        return

    ticker = args[0].upper()
    try:
        precio_venta = float(args[1].replace(",", "."))
        cantidad_venta = int(args[2])
    except ValueError:
        await update.message.reply_text("❌ Precio y cantidad deben ser números.", parse_mode="HTML")
        return

    if precio_venta <= 0 or cantidad_venta <= 0:
        await update.message.reply_text("❌ Precio y cantidad deben ser positivos.", parse_mode="HTML")
        return

    from src.execution import order_engine
    result = order_engine.execute_venta(
        ticker=ticker, precio_raw=precio_venta, cantidad=cantidad_venta,
    )

    if result.get("status") != "ok":
        await update.message.reply_text(f"❌ {result.get('error', 'Error desconocido')}", parse_mode="HTML")
        return

    msg = f"💰 <b>{result['msg']}</b>"
    if not result.get("pushed"):
        msg += f"\n\n⚠️ {result.get('push_warning', 'GitHub no disponible — guardado local')}"
    else:
        msg += "\n\n📤 Portfolio actualizado en GitHub"

    await update.message.reply_text(msg, parse_mode="HTML")
    logger.info(f"Venta registrada (bot): {ticker} {cantidad_venta} @ {precio_venta}")


async def cmd_portfolio(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Muestra el portfolio actual."""
    portfolio = _load_portfolio()
    positions = portfolio.get("positions", [])
 
    if not positions:
        await update.message.reply_text("📭 No tenés posiciones abiertas.", parse_mode="HTML")
        return
 
    lines = [f"💼 <b>Portfolio — {len(positions)} posiciones</b>\n"]
 

    total_ini_usd = sum(p.get("valor_inicial_usd", p.get("precio_compra",0)*p.get("cantidad",0)) for p in positions)
    total_act_usd = sum(p.get("valor_actual_usd", p.get("valor_inicial_usd",0)) for p in positions)
    total_rend    = sum(p.get("rend_usd", 0) for p in positions)
    pnl_pct_total = round((total_rend / total_ini_usd) * 100, 2) if total_ini_usd > 0 else 0

    merc_map = {"MERVAL": "🇦🇷", "BOVESPA": "🇧🇷", "SP500": "🇺🇸"}
    for p in positions:
        ini  = p.get("valor_inicial_usd", p.get("precio_compra",0)*p.get("cantidad",0))
        act  = p.get("valor_actual_usd", ini)
        rend = p.get("rend_usd", 0)
        pct  = round((rend/ini)*100, 1) if ini > 0 else 0
        flag = merc_map.get(p.get("mercado","SP500"), "🌍")
        sign = "+" if rend >= 0 else ""
        lines.append(
            f"{flag} <code>{p['ticker']:<10}</code> {p['cantidad']:>6} | "
            f"USD {act:>7,.0f} | <b>{sign}{rend:,.0f} ({sign}{pct:.1f}%)</b>"
        )

    sign_t = "+" if total_rend >= 0 else ""
    lines.append(f"\n💵 <b>Invertido: USD {total_ini_usd:,.0f}</b>")
    lines.append(f"📈 <b>Actual:    USD {total_act_usd:,.0f}</b>")
    lines.append(f"💰 <b>P&L Total: {sign_t}USD {total_rend:,.0f} ({sign_t}{pnl_pct_total:.2f}%)</b>")
    lines.append(f"\n<i>Actualizado: {portfolio.get('last_updated', '—')}</i>")
 
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
 
 
async def cmd_bootstrap_macro(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """
    Precarga data/macro_raw_history.json con ~3 años de historia real de FRED
    para las 9 variables de USA (mejora 2.1 — normalización macro con
    percentil rolling). Corre 1 sola vez, no hace falta repetirlo: sin esto,
    el percentil rolling tarda ~2 meses en acumular suficiente historia por
    sí solo; con esto arranca con historia real desde el día 1.
    """
    await update.message.reply_text(
        "📊 Bootstrapeando historia macro de FRED (3 años, 9 variables de USA)…\nEsto puede tardar 10-20 segundos.",
        parse_mode="HTML"
    )

    import asyncio
    from src.macro_auto import bootstrap_fred_history
    try:
        loop = asyncio.get_event_loop()
        history = await loop.run_in_executor(None, bootstrap_fred_history)
        if not history:
            await update.message.reply_text(
                "⚠️ No se cargó nada. Verificá que FRED_API_KEY esté configurado en las variables de entorno de Railway."
            )
            return
        resumen = "\n".join(f"  • {k}: {len(v)} obs" for k, v in history.items() if v)
        await update.message.reply_text(
            f"✅ Historia macro bootstrapeada y pusheada a GitHub:\n<code>{resumen}</code>",
            parse_mode="HTML"
        )
    except Exception as e:
        logger.error(f"Error en /bootstrap_macro: {e}")
        await update.message.reply_text(f"❌ Error:\n<code>{str(e)[:300]}</code>", parse_mode="HTML")


async def cmd_backfill_stops(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """
    /backfill_stops          -> vista previa (dry-run), no modifica nada
    /backfill_stops aplicar  -> asigna stop_loss/target de verdad y pushea

    Asigna stop_loss/target a posiciones que se abrieron ANTES del fix de
    causa raíz del 26/06/2026 (ATR estaba en 0.0 siempre -> /api/compra no
    tenía de dónde leer un stop real, así que quedaba en None). Pensado
    para correrse UNA vez, después del primer pipeline en producción con
    el fix ya desplegado -- antes de eso, signals_history.json todavía
    tiene el ATR viejo en 0 y esto no va a encontrar nada para asignar.

    Usa risk_engine.backfill_missing_stops(): solo toca posiciones con
    stop_loss/target en None — no pisa nada que ya esté seteado.
    """
    args = context.args
    aplicar = bool(args) and args[0].strip().lower() in ("aplicar", "confirmar", "si", "sí")

    portfolio = _load_portfolio()
    if not portfolio.get("positions"):
        await update.message.reply_text("📭 No tenés posiciones abiertas.", parse_mode="HTML")
        return

    from src.execution.risk_engine import backfill_missing_stops
    propuestas = backfill_missing_stops(portfolio, dry_run=not aplicar)

    if not propuestas:
        await update.message.reply_text(
            "✅ Todas las posiciones abiertas ya tienen stop_loss/target asignado — nada para backfillear.",
            parse_mode="HTML"
        )
        return

    con_dato = [p for p in propuestas if p.get("stop_loss_propuesto") is not None]
    sin_dato = [p for p in propuestas if p.get("stop_loss_propuesto") is None]
    ya_en_riesgo = [p for p in con_dato if p.get("ya_por_debajo_del_stop")]

    titulo = "🛡️ Backfill de stops — APLICADO" if aplicar else "🛡️ Backfill de stops — vista previa (nada aplicado todavía)"
    lines = [f"<b>{titulo}</b>\n"]

    if ya_en_riesgo:
        lines.append("🔴 <b>YA están por debajo del stop calculado — revisar HOY:</b>")
        for p in ya_en_riesgo:
            lines.append(
                f"  <code>{p['ticker']:<10}</code> precio USD {p['precio_actual_usd']:.4f} "
                f"≤ stop USD {p['stop_loss_propuesto']:.4f}"
            )
        lines.append("")

    otros = [p for p in con_dato if not p.get("ya_por_debajo_del_stop")]
    if otros:
        lines.append("Stop/target propuesto:" if not aplicar else "Stop/target asignado:")
        for p in otros:
            lines.append(
                f"  <code>{p['ticker']:<10}</code> stop USD {p['stop_loss_propuesto']:.4f} | "
                f"target USD {p['target_propuesto']:.4f}"
            )
        lines.append("")

    if sin_dato:
        lines.append("<i>Sin datos suficientes todavía (probar de nuevo después del próximo pipeline):</i>")
        for p in sin_dato:
            lines.append(f"  <code>{p['ticker']:<10}</code> — {p['metodo']}")
        lines.append("")

    if aplicar:
        _save_portfolio(portfolio)
        pushed = _push_portfolio_to_github()
        lines.append("📤 Guardado y sincronizado con GitHub." if pushed else
                      "⚠️ Guardado local — push a GitHub pendiente.")
    else:
        lines.append("👉 Para aplicar de verdad: <code>/backfill_stops aplicar</code>")

    await update.message.reply_text("\n".join(lines), parse_mode="HTML")
    logger.info(f"Backfill de stops ({'aplicado' if aplicar else 'dry-run'}): "
                f"{len(con_dato)} con dato, {len(sin_dato)} sin dato, {len(ya_en_riesgo)} ya en riesgo")


# ─────────────────────────────────────────────
# Inicialización
# ─────────────────────────────────────────────
 
def build_application() -> Application:
    """Construye y retorna la aplicación del bot."""
    if not BOT_TOKEN:
        raise ValueError("TELEGRAM_BOT_TOKEN no está configurado.")
 
    app = Application.builder().token(BOT_TOKEN).build()
 
    app.add_handler(CommandHandler("help",      cmd_help))
    app.add_handler(CommandHandler("start",     cmd_help))
    app.add_handler(CommandHandler("status",    cmd_status))
    app.add_handler(CommandHandler("senales",   cmd_señales))
    app.add_handler(CommandHandler("senales",   cmd_señales))
    app.add_handler(CommandHandler("run",       cmd_run))
    app.add_handler(CommandHandler("compra",    cmd_compra))
    app.add_handler(CommandHandler("venta",     cmd_venta))
    app.add_handler(CommandHandler("portfolio", cmd_portfolio))
    app.add_handler(CommandHandler("bootstrap_macro", cmd_bootstrap_macro))
    app.add_handler(CommandHandler("backfill_stops", cmd_backfill_stops))
 
    return app
 
