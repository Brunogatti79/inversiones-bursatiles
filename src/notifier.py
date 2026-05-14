"""
src/notifier.py
Envía notificaciones a Telegram y publica el dashboard en GitHub Pages.
NUEVO: Bloque de validación de datos en el mensaje diario.
"""
 
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
 
FLAG    = {"MERVAL": "🇦🇷", "BOVESPA": "🇧🇷", "SP500": "🇺🇸"}
MARKETS = ["MERVAL", "BOVESPA", "SP500"]
 
 
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
        sha = None
        r = requests.get(api_url, headers=headers, timeout=15)
        if r.status_code == 200:
            sha = r.json().get("sha")
        tz  = pytz.timezone(TIMEZONE)
        now = datetime.now(tz).strftime("%d/%m/%Y %H:%M")
        payload = {"message": f"dashboard: actualizacion automatica {now}", "content": content_b64}
        if sha:
            payload["sha"] = sha
        r = requests.put(api_url, headers=headers, json=payload, timeout=30)
        r.raise_for_status()
        logger.info(f"Dashboard publicado en GitHub Pages: {api_path}")
        return True
    except Exception as e:
        logger.error(f"Error publicando dashboard: {e}")
        return False


def publish_index_html(dashboard_filename):
    """Publica index.html que redirige al último dashboard."""
    if not GH_TOKEN:
        return False
    try:
        content = f'<!DOCTYPE html><html><head><meta http-equiv="refresh" content="0;url={dashboard_filename}"></head><body></body></html>'
        content_b64 = base64.b64encode(content.encode("utf-8")).decode("utf-8")
        api_url = f"https://api.github.com/repos/{GH_USER}/{GH_REPO}/contents/outputs/index.html"
        headers = {
            "Authorization": f"token {GH_TOKEN}",
            "Accept": "application/vnd.github.v3+json",
        }
        sha = None
        r = requests.get(api_url, headers=headers, timeout=15)
        if r.status_code == 200:
            sha = r.json().get("sha")
        payload = {"message": "index.html: redirect al ultimo dashboard", "content": content_b64}
        if sha:
            payload["sha"] = sha
        r = requests.put(api_url, headers=headers, json=payload, timeout=30)
        r.raise_for_status()
        logger.info("index.html publicado en GitHub Pages")
        return True
    except Exception as e:
        logger.error(f"Error publicando index.html: {e}")
        return False
     
def _index_line(stats, market):
    flag  = FLAG.get(market, "")
    ret   = stats.get("ret_anual", 0)
    sign  = "+" if ret >= 0 else ""
    vol   = stats.get("volatilidad", 0)
    act   = stats.get("actual", 0)
    label = "S&P 500" if market == "SP500" else market
    return (f"{flag} <b>{label}</b> {act:,.0f}  "
            f"<code>{sign}{ret:.1f}%</code> 12m  |  Vol {vol:.1f}%")
 
 
def _validacion_section(validacion: dict) -> str:
    """
    Bloque de validación de datos para el mensaje Telegram.
    Muestra nivel global + detalle por mercado.
    """
    if not validacion:
        return ""
 
    nivel = validacion.get("nivel_global", "OK")
    ts    = validacion.get("timestamp", "")
 
    # Icono según nivel
    if nivel == "ERROR":
        icono_global = "🔴"
        titulo = "ALERTA — Datos con problemas"
    elif nivel == "WARNING":
        icono_global = "🟡"
        titulo = "Advertencia de datos"
    else:
        icono_global = "🟢"
        titulo = "Datos OK"
 
    lines = [f"\n{icono_global} <b>{titulo}</b>"]
 
    market_map = {"merval": "MERVAL", "bovespa": "BOVESPA", "sp500": "SP500"}
    for key, market in market_map.items():
        res = validacion.get("mercados", {}).get(key, {})
        if not res:
            continue
        flag         = FLAG.get(market, "")
        nivel_m      = res.get("nivel", "OK")
        ultima_fecha = res.get("ultima_fecha", "—")
        icono_m      = "🟢" if nivel_m == "OK" else "🟡" if nivel_m == "WARNING" else "🔴"
        label        = "S&P 500" if market == "SP500" else market
 
        lines.append(f"  {icono_m} {flag} <b>{label}</b> — último cierre: <code>{ultima_fecha}</code>")
 
        # Mostrar errores críticos
        for err in res.get("errors", []):
            # Extraer solo el mensaje sin el prefijo [MARKET]
            msg = err.split("]")[-1].strip() if "]" in err else err
            lines.append(f"    ⚠️ {msg[:80]}")
 
    return "\n".join(lines)
 
 
def _radar_section(signals):
    """Top 5 del Radar por score compuesto."""
    def radar_score(s):
        score = min(s["score_final"] / 100, 1) * 35
        rsi = s.get("rsi", 50)
        if 28 <= rsi <= 45:   score += 20
        elif 45 < rsi <= 55:  score += 12
        elif 55 < rsi <= 65:  score += 6
        rs, rm = s.get("ret_sem", 0), s.get("ret_mes", 0)
        if rs > 0 and rm < 5:          score += 20
        elif rs > 0 and rm < 15:       score += 12
        elif rs > 0:                   score += 6
        if s.get("ma_cross"):          score += 15
        pa, mx = s.get("precio_actual", 0), s.get("max_12m", 0)
        if pa and mx > 0:
            p = (mx - pa) / mx
            if p > 0.35:   score += 10
            elif p > 0.20: score += 7
            elif p > 0.10: score += 4
        return min(round(score), 100)
 
    universe = [s for s in signals if "VENTA" not in s.get("signal", "")]
    ranked   = sorted(universe, key=radar_score, reverse=True)[:5]
 
    lines = ["\n🔭 <b>Radar de Oportunidades</b>"]
    for market in MARKETS:
        market_ranked = [s for s in ranked if s["mercado"] == market]
        if not market_ranked:
            continue
        flag  = FLAG.get(market, "")
        label = "S&P 500" if market == "SP500" else market
        lines.append(f"  {flag} <b>{label}</b>")
        for s in market_ranked:
            sc = radar_score(s)
            lines.append(f"  #{ranked.index(s)+1} <code>{s['ticker']}</code> {s['empresa'][:18]} "
                         f"— Radar {sc} | RSI {s['rsi']:.0f}")
    return "\n".join(lines)
 
 
def _compras_section(signals):
    lines = ["\n✅ <b>Compras</b>"]
    for market in MARKETS:
        flag    = FLAG.get(market, "")
        label   = "S&P 500" if market == "SP500" else market
        compras = [s for s in signals if s["mercado"] == market and "COMPRA" in s.get("signal", "")]
        lines.append(f"  {flag} <b>{label}</b>")
        if compras:
            for s in compras[:4]:
                lines.append(f"  {s['signal']} <code>{s['ticker']}</code> {s['empresa'][:18]} "
                             f"— Score {s['score_final']:.0f} | Sem {s['ret_sem']:+.1f}%")
        else:
            lines.append("  🟡 Sin señales de compra")
    return "\n".join(lines)
 
 
def _reducciones_section(signals):
    lines = ["\n🔴 <b>Reducciones</b>"]
    for market in MARKETS:
        flag   = FLAG.get(market, "")
        label  = "S&P 500" if market == "SP500" else market
        ventas = [s for s in signals if s["mercado"] == market and "VENTA" in s.get("signal", "")]
        lines.append(f"  {flag} <b>{label}</b>")
        if ventas:
            for s in ventas[:3]:
                lines.append(f"  {s['signal']} <code>{s['ticker']}</code> {s['empresa'][:18]}")
        else:
            lines.append("  🟡 Sin señales de reducción")
    return "\n".join(lines)
 
 
def send_daily_report(all_signals, index_stats, dashboard_filename,
                      run_date=None, validacion=None):
    tz       = pytz.timezone(TIMEZONE)
    now      = datetime.now(tz)
    run_date = run_date or now.strftime("%d/%m/%Y %H:%M")
    dash_url = f"{DASH_URL}/{dashboard_filename}"
 
    sep = "─" * 32
 
    header = f"📊 <b>Inversiones Bursátiles — {run_date}</b>\n{sep}\n"
 
    indices_block = "<b>Índices (12 meses)</b>\n"
    for market_key, display in [("merval","MERVAL"),("bovespa","BOVESPA"),("sp500","SP500")]:
        stats = index_stats.get(market_key, {})
        if stats:
            indices_block += _index_line(stats, display) + "\n"
 
    # Bloque de validación — va inmediatamente después de los índices
    validacion_block = _validacion_section(validacion) if validacion else ""
 
    senales_block  = "\n<b>Señales activas del modelo</b>"
    senales_block += _radar_section(all_signals)
    senales_block += _compras_section(all_signals)
    senales_block += _reducciones_section(all_signals)
 
    footer = (
        f"\n{sep}\n"
        f"🔗 <a href='{dash_url}'>Ver dashboard completo</a>\n"
        f"⏱ Próxima actualización: mañana al cierre"
    )
 
    full_msg = header + indices_block + validacion_block + senales_block + footer
 
    if len(full_msg) > 4000:
        full_msg = full_msg[:3990] + "\n…"
 
    return _send_message(full_msg)
 
 
def send_signal_change_alerts(changes):
    if not changes:
        return True
    lines = ["🚨 <b>Cambios de señal detectados</b>\n"]
    for c in changes:
        flag = FLAG.get(c["mercado"], "")
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
 
