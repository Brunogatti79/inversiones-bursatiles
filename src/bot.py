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
    """Registrar compra: /compra TICKER PRECIO_USD CANTIDAD (precio en USD/acción, igual que broker)"""
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
 
    portfolio = _load_portfolio()
 
    # Buscar si ya existe la posición
    existing = None
    for p in portfolio["positions"]:
        if p["ticker"] == ticker:
            existing = p
            break
 
    tz = pytz.timezone(TIMEZONE)
    fecha = datetime.now(tz).strftime("%Y-%m-%d")
 
    if existing:
        # Promediar precio de compra
        old_ini = existing.get("valor_inicial_usd") or (existing.get("precio_compra", 0) * existing["cantidad"])
        new_ini = precio * cantidad
        new_cantidad = existing["cantidad"] + cantidad
        new_precio = round((old_ini + new_ini) / new_cantidad, 4)
        existing["precio_compra"]     = new_precio
        existing["precio_compra_usd"] = new_precio
        existing["cantidad"]          = new_cantidad
        existing["valor_inicial_usd"] = round(old_ini + new_ini, 2)
        existing["valor_actual_usd"]  = existing["valor_inicial_usd"]  # se actualiza al próx. run
        existing["rend_usd"]          = 0
        existing["fecha_compra"] = fecha
        existing["notas"] = f"Promediado {fecha}: +{cantidad} @ USD {precio}"
        msg = (
            f"✅ <b>Compra agregada a posición existente</b>\n\n"
            f"📌 <code>{ticker}</code>\n"
            f"Cantidad nueva: {new_cantidad}\n"
            f"Precio promedio: USD {new_precio:,.4f}/acc\n"
            f"Invertido total: USD {existing['valor_inicial_usd']:,.2f}\n"
            f"Fecha: {fecha}"
        )
    else:
        # Nueva posición
        new_pos = {
            "ticker": ticker,
            "nombre": ticker,
            "mercado": "MERVAL" if ".BA" in ticker else "BOVESPA" if ".SA" in ticker else "SP500",
            "moneda": "USD",  # Todos los precios en USD (broker)
            "precio_compra": precio,           # USD/acc backward compat
            "precio_compra_usd": precio,        # USD/acc
            "precio_actual_usd": precio,        # inicia igual a compra
            "cantidad": cantidad,
            "valor_inicial_usd": round(precio * cantidad, 2),
            "valor_actual_usd": round(precio * cantidad, 2),
            "rend_usd": 0,
            "fecha_compra": fecha,
            "stop_loss": None,
            "target": None,
            "notas": f"Compra {fecha} via Telegram",
        }
        portfolio["positions"].append(new_pos)
        msg = (
            f"✅ <b>Nueva posición registrada</b>\n\n"
            f"📌 <code>{ticker}</code>\n"
            f"Precio: USD {precio:,.4f}/acc\n"
            f"Cantidad: {cantidad}\n"
            f"Total invertido: USD {precio * cantidad:,.2f}\n"
            f"Fecha: {fecha}"
        )
 
    _save_portfolio(portfolio)
    pushed = _push_portfolio_to_github()
    if pushed:
        msg += "\n\n📤 Portfolio actualizado en GitHub"
    else:
        msg += "\n\n⚠️ Guardado local (GitHub push pendiente)"
 
    await update.message.reply_text(msg, parse_mode="HTML")
    logger.info(f"Compra registrada: {ticker} {cantidad} @ {precio}")
 
 
async def cmd_venta(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Registrar venta: /venta TICKER PRECIO CANTIDAD"""
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
 
    portfolio = _load_portfolio()
 
    # Buscar posición existente
    existing = None
    for p in portfolio["positions"]:
        if p["ticker"] == ticker:
            existing = p
            break
 
    if not existing:
        await update.message.reply_text(
            f"❌ No tenés posición abierta en <code>{ticker}</code>",
            parse_mode="HTML"
        )
        return
 
    if cantidad_venta > existing["cantidad"]:
        await update.message.reply_text(
            f"❌ Querés vender {cantidad_venta} pero solo tenés {existing['cantidad']} de <code>{ticker}</code>",
            parse_mode="HTML"
        )
        return
 
    tz = pytz.timezone(TIMEZONE)
    fecha = datetime.now(tz).strftime("%Y-%m-%d")
 
    # Calcular P&L en USD
    precio_compra_orig = existing.get("precio_compra_usd") or existing.get("precio_compra", 0)
    pnl_por_unidad = precio_venta - precio_compra_orig
    pnl_total = round(pnl_por_unidad * cantidad_venta, 2)
    pnl_pct = round((precio_venta / precio_compra_orig - 1) * 100, 2) if precio_compra_orig > 0 else 0

    if cantidad_venta == existing["cantidad"]:
        # Venta total — eliminar posición
        portfolio["positions"] = [p for p in portfolio["positions"] if p["ticker"] != ticker]
        tipo = "VENTA TOTAL"
    else:
        # Venta parcial — reducir cantidad y actualizar USD fields
        restantes = existing["cantidad"] - cantidad_venta
        existing["cantidad"] = restantes
        existing["valor_inicial_usd"] = round(precio_compra_orig * restantes, 2)
        existing["valor_actual_usd"]  = round(precio_venta * restantes, 2)
        existing["rend_usd"] = round((precio_venta - precio_compra_orig) * restantes, 2)
        existing["notas"] = f"Venta parcial {fecha}: -{cantidad_venta} @ USD {precio_venta}"
        tipo = "VENTA PARCIAL"
 
    icon = "💰" if pnl_total >= 0 else "📉"
    color_pnl = "+" if pnl_total >= 0 else ""
 
    msg = (
        f"{icon} <b>{tipo}</b>\n\n"
        f"📌 <code>{ticker}</code>\n"
        f"Vendido: {cantidad_venta} @ USD {precio_venta:,.4f}/acc\n"
        f"Compra fue: USD {precio_compra_orig:,.4f}/acc\n"
        f"P&L: {color_pnl}USD {pnl_total:,.2f} ({color_pnl}{pnl_pct}%)\n"
        f"Fecha: {fecha}"
    )
 
    if tipo == "VENTA PARCIAL":
        msg += f"\n\nPosición restante: {existing['cantidad']} unidades"
 
    _save_portfolio(portfolio)
    pushed = _push_portfolio_to_github()
    if pushed:
        msg += "\n\n📤 Portfolio actualizado en GitHub"
    else:
        msg += "\n\n⚠️ Guardado local (GitHub push pendiente)"
 
    await update.message.reply_text(msg, parse_mode="HTML")
    logger.info(f"Venta registrada: {ticker} {cantidad_venta} @ {precio_venta} P&L={pnl_total}")
 
 
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
 
    return app
 
