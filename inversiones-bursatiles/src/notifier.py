import logging
import os
import base64
from datetime import datetime
import pytz
import requests
 
logger = logging.getLogger(__name__)
 
BOT_TOKEN  = os.getenv("TELEGRAM_BOT_TOKEN", "")
CHAT_ID    = os.getenv("TELEGRAM_CHAT_ID", "")
DASH_URL   = os.getenv("DASHBOARD_BASE_URL", "https://brunogatti79.github.io/inversiones-bursatiles/outputs")
TIMEZONE   = os.getenv("TIMEZONE", "America/Argentina/Buenos_Aires")
GH_TOKEN   = os.getenv("GH_TOKEN", "")
GH_USER    = os.getenv("GH_USER", "Brunogatti79")
GH_REPO    = os.getenv("GH_REPO", "inversiones-bursatiles")
 
 
def _send_message(text, parse_mode="HTML"):
    if not BOT_TOKEN or not CHAT_ID:
        logger.warning("TELEGRAM_BOT_TOKEN o TELEGRAM_CHAT_ID no configurados.")
        return False
    url = f"https://api.telegram.org/bot{BOT_TOKEN}/sendMessage"
    payload = {"chat_id": CHAT_ID, "text": text, "parse_mode": parse_mode,
               "disable_web_page_preview": False}
    try:
        r = requests.post(url, json=payload, timeout=15)
        r.raise_for_status()
        return True
    except Exception as e:
        logger.error(f"Error enviando mensaje Telegram: {e}")
        return False
 
 
def _send_document(file_path, caption=""):
    if not BOT_TOKEN or not CHAT_ID:
        return False
    url = f"https://api.telegram.org/bot{BOT_TOKEN}/sendDocument"
    try:
        with open(file_path, "rb") as f:
            r = requests.post(url, data={"chat_id": CHAT_ID, "caption": caption},
                              files={"document": f}, timeout=30)
        r.raise_for_status()
        return True
    except Exception as e:
        logger.error(f"Error enviando archivo Telegram: {e}")
        return False
 
 
def publish_dashboard(local_path, filename):
    """
    Sube el HTML generado a GitHub Pages via GitHub API.
    Usa GH_TOKEN, GH_USER, GH_REPO del entorno de Railway.
    """
    if not GH_TOKEN:
        logger.warning("GH_TOKEN no configurado — no se puede publicar dashboard.")
        return False
 
    try:
        with open(local_path, "r", encoding="utf-8") as f:
            content = f.read()
 
        content_b64 = base64.b64encode(content.encode("utf-8")).decode("utf-8")
        api_path = f"outputs/{filename}"
        api_url  = f"https://api.github.com/repos/{GH_USER}/{GH_REPO}/contents/{api_path}"
        headers  = {
            "Authorization": f"token {GH_TOKEN}",
            "Accept": "application/vnd.github.v3+json",
        }
 
        # Verificar si ya existe (necesitamos el SHA para actualizar)
        sha = None
        r = requests.get(api_url, headers=headers, timeout=15)
        if r.status_code == 200:
            sha = r.json().get("sha")
 
        tz = pytz.timezone(TIMEZONE)
        now = datetime.now(tz).strftime("%d/%m/%Y %H:%M")
        payload = {
            "message": f"dashboard: actualizacion automatica {now}",
            "content": content_b64,
        }
        if sha:
            payload["sha"] = sha
 
        r = requests.put(api_url, headers=headers, json=payload, timeout=30)
        r.raise_for_status()
        logger.info(f"Dashboard publicado en GitHub Pages: {api_path}")
        return True
 
    except Exception as e:
        logger.error(f"Error publicando dashboard: {e}")
        return False
 
 
def _signal_section(signals, market):
    market_signals = [s for s in signals if s["mercado"] == market]
    if not market_signals:
        return ""
    compras = [s for s in market_signals if "COMPRA" in s["signal"]]
    ventas  = [s for s in market_signals if "VENTA"  in s["signal"]]
    lines = [f"\n<b>{'🇦🇷' if market=='MERVAL' else '🇧🇷' if market=='BOVESPA' else '🇺🇸'} {market}</b>"]
    if compras:
        lines.append("  <b>Compras:</b>")
        for s in compras[:5]:
            lines.append(f"  {s['signal']} <code>{s['ticker']}</code> {s['empresa'][:20]} "
                         f"— Score {s['score_final']:.0f} | Sem {s['ret_sem']:+.1f}%")
    if ventas:
        lines.append("  <b>Reducciones:</b>")
        for s in ventas[:3]:
            lines.append(f"  {s['signal']} <code>{s['ticker']}</code> {s['empresa'][:20]}")
    if not compras and not ventas:
        lines.append("  🟡 Sin señales de compra/venta activas")
    return "\n".join(lines)
 
 
def _index_line(stats, market):
    flag = "🇦🇷" if market == "MERVAL" else "🇧🇷" if market == "BOVESPA" else "🇺🇸"
    ret  = stats.get("ret_anual", 0)
    sign = "+" if ret >= 0 else ""
    vol  = stats.get("volatilidad", 0)
    return (f"{flag} <b>{market}</b> {stats.get('actual', 0):,.0f}  "
            f"<code>{sign}{ret:.1f}%</code> 12m  |  Vol {vol:.1f}%")
 
 
def send_daily_report(all_signals, index_stats, dashboard_filename, run_date=None):
    tz  = pytz.timezone(TIMEZONE)
    now = datetime.now(tz)
    run_date = run_date or now.strftime("%d/%m/%Y %H:%M")
    dash_url = f"{DASH_URL}/{dashboard_filename}"
 
    header = (
        f"📊 <b>Inversiones Bursátiles — {run_date}</b>\n"
        f"{'─' * 32}\n"
    )
    indices_block = "<b>Índices (12 meses)</b>\n"
    for market_key, display in [("merval","MERVAL"),("bovespa","BOVESPA"),("sp500","S&P 500")]:
        stats = index_stats.get(market_key, {})
        if stats:
            indices_block += _index_line(stats, display) + "\n"
 
    signals_block = "\n<b>Señales activas del modelo</b>"
    for market in ["MERVAL", "BOVESPA", "SP500"]:
        signals_block += _signal_section(all_signals, market)
 
    top3 = [s for s in all_signals if "COMPRA" in s["signal"]][:3]
    if top3:
        ranking_block = "\n\n<b>🏆 Top 3 global</b>\n"
        for i, s in enumerate(top3, 1):
            ranking_block += (f"  {i}. {s['signal']} <code>{s['ticker']}</code> "
                              f"— Score {s['score_final']:.0f} | "
                              f"Sem {s['ret_sem']:+.1f}% | Anual {s['ret_anual']:+.1f}%\n")
    else:
        ranking_block = ""
 
    footer = (
        f"\n{'─' * 32}\n"
        f"🔗 <a href='{dash_url}'>Ver dashboard completo</a>\n"
        f"⏱ Próxima actualización: mañana al cierre"
    )
 
    full_msg = header + indices_block + signals_block + ranking_block + footer
    if len(full_msg) > 4000:
        full_msg = full_msg[:3990] + "\n…"
 
    return _send_message(full_msg)
 
 
def send_signal_change_alerts(changes):
    if not changes:
        return True
    lines = ["🚨 <b>Cambios de señal detectados</b>\n"]
    for c in changes:
        flag = "🇦🇷" if c["mercado"] == "MERVAL" else "🇧🇷" if c["mercado"] == "BOVESPA" else "🇺🇸"
        lines.append(
            f"{flag} <code>{c['ticker']}</code> <b>{c['empresa'][:22]}</b>\n"
            f"   {c['prev_signal']} → {c['new_signal']}"
        )
    return _send_message("\n".join(lines))
 
 
def send_excel(file_path, market=""):
    caption = f"📁 Fichas de inversión {market} — {datetime.now().strftime('%d/%m/%Y')}"
    return _send_document(file_path, caption)
 
 
def send_error_notification(error_msg):
    text = f"❌ <b>Error en pipeline Inversiones</b>\n<code>{error_msg[:500]}</code>"
    return _send_message(text)
 
 
def send_startup_message():
    tz  = pytz.timezone(TIMEZONE)
    now = datetime.now(tz).strftime("%d/%m/%Y %H:%M")
    text = (
        f"✅ <b>Bot Inversiones Bursátiles activo</b>\n"
        f"Inicio: {now} ({TIMEZONE})\n"
        f"Comandos disponibles:\n"
        f"  /run — Ejecutar análisis ahora\n"
        f"  /status — Ver estado del sistema\n"
        f"  /señales — Ver señales actuales\n"
        f"  /help — Ayuda"
    )
    return _send_message(text)
 
