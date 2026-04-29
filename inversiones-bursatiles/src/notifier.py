"""
src/notifier.py
Envía notificaciones a Telegram:
  - Resumen de señales compra/venta por mercado
  - Link al dashboard HTML
  - Alertas de cambio de señal vs día anterior
  - (Opcional) Archivo Excel adjunto
"""

import logging
import os
from datetime import datetime
import pytz
import requests

logger = logging.getLogger(__name__)

BOT_TOKEN  = os.getenv("TELEGRAM_BOT_TOKEN", "")
CHAT_ID    = os.getenv("TELEGRAM_CHAT_ID", "")
DASH_URL   = os.getenv("DASHBOARD_BASE_URL", "https://tu-app.up.railway.app")
TIMEZONE   = os.getenv("TIMEZONE", "America/Argentina/Buenos_Aires")


# ─────────────────────────────────────────────
# Helpers de bajo nivel
# ─────────────────────────────────────────────

def _send_message(text: str, parse_mode: str = "HTML") -> bool:
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


def _send_document(file_path: str, caption: str = "") -> bool:
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


# ─────────────────────────────────────────────
# Formateo de mensajes
# ─────────────────────────────────────────────

def _signal_section(signals: list[dict], market: str) -> str:
    """Genera sección de señales para un mercado."""
    market_signals = [s for s in signals if s["mercado"] == market]
    if not market_signals:
        return ""

    # Filtrar solo compras y ventas (no neutral)
    compras = [s for s in market_signals if "COMPRA" in s["signal"]]
    ventas  = [s for s in market_signals if "VENTA" in s["signal"]]

    lines = [f"\n<b>{'🇦🇷' if market=='MERVAL' else '🇧🇷' if market=='BOVESPA' else '🇺🇸'} {market}</b>"]

    if compras:
        lines.append("  <b>Compras:</b>")
        for s in compras[:5]:
            score_str = f"{s['score_final']:.0f}"
            ret_str   = f"{s['ret_sem']:+.1f}%"
            lines.append(f"  {s['signal']} <code>{s['ticker']}</code> {s['empresa'][:20]} "
                         f"— Score {score_str} | Sem {ret_str}")

    if ventas:
        lines.append("  <b>Reducciones:</b>")
        for s in ventas[:3]:
            lines.append(f"  {s['signal']} <code>{s['ticker']}</code> {s['empresa'][:20]}")

    if not compras and not ventas:
        lines.append("  🟡 Sin señales de compra/venta activas")

    return "\n".join(lines)


def _index_line(stats: dict, market: str) -> str:
    flag = "🇦🇷" if market == "MERVAL" else "🇧🇷" if market == "BOVESPA" else "🇺🇸"
    ret  = stats.get("ret_anual", 0)
    sign = "+" if ret >= 0 else ""
    vol  = stats.get("volatilidad", 0)
    return (f"{flag} <b>{market}</b> {stats.get('actual', '—'):,.0f}  "
            f"<code>{sign}{ret:.1f}%</code> 12m  |  Vol {vol:.1f}%")


# ─────────────────────────────────────────────
# Mensajes públicos
# ─────────────────────────────────────────────

def send_daily_report(
    all_signals: list[dict],
    index_stats: dict,         # {"merval": {...}, "bovespa": {...}, "sp500": {...}}
    dashboard_filename: str,
    run_date: str = None,
) -> bool:
    """
    Envía el resumen diario completo.
    """
    tz  = pytz.timezone(TIMEZONE)
    now = datetime.now(tz)
    run_date = run_date or now.strftime("%d/%m/%Y %H:%M")

    dash_url = f"{DASH_URL}/{dashboard_filename}"

    # ── Cabecera ──
    header = (
        f"📊 <b>Inversiones Bursátiles — {run_date}</b>\n"
        f"{'─' * 32}\n"
    )

    # ── Índices ──
    indices_block = "<b>Índices (12 meses)</b>\n"
    for market_key, display in [("merval","MERVAL"),("bovespa","BOVESPA"),("sp500","S&P 500")]:
        stats = index_stats.get(market_key, {})
        if stats:
            indices_block += _index_line(stats, display) + "\n"

    # ── Señales por mercado ──
    signals_block = "\n<b>Señales activas del modelo</b>"
    for market in ["MERVAL", "BOVESPA", "SP500"]:
        signals_block += _signal_section(all_signals, market)

    # ── Ranking top 3 global ──
    top3 = [s for s in all_signals if "COMPRA" in s["signal"]][:3]
    if top3:
        ranking_block = "\n\n<b>🏆 Top 3 global</b>\n"
        for i, s in enumerate(top3, 1):
            ranking_block += (f"  {i}. {s['signal']} <code>{s['ticker']}</code> "
                              f"— Score {s['score_final']:.0f} | "
                              f"Sem {s['ret_sem']:+.1f}% | Anual {s['ret_anual']:+.1f}%\n")
    else:
        ranking_block = ""

    # ── Footer + link ──
    footer = (
        f"\n{'─' * 32}\n"
        f"🔗 <a href='{dash_url}'>Ver dashboard completo</a>\n"
        f"⏱ Próxima actualización: mañana al cierre"
    )

    full_msg = header + indices_block + signals_block + ranking_block + footer

    # Telegram tiene límite de 4096 chars
    if len(full_msg) > 4000:
        full_msg = full_msg[:3990] + "\n…"

    return _send_message(full_msg)


def send_signal_change_alerts(changes: list[dict]) -> bool:
    """
    Envía alertas cuando una señal cambia respecto al día anterior.
    """
    if not changes:
        return True

    lines = ["🚨 <b>Cambios de señal detectados</b>\n"]
    for c in changes:
        flag = "🇦🇷" if c["mercado"] == "MERVAL" else "🇧🇷" if c["mercado"] == "BOVESPA" else "🇺🇸"
        lines.append(
            f"{flag} <code>{c['ticker']}</code> <b>{c['empresa'][:22]}</b>\n"
            f"   {c['prev_signal']} → {c['new_signal']}"
        )

    text = "\n".join(lines)
    return _send_message(text)


def send_excel(file_path: str, market: str = "") -> bool:
    """Envía el archivo Excel de fichas por Telegram."""
    caption = f"📁 Fichas de inversión {market} — {datetime.now().strftime('%d/%m/%Y')}"
    return _send_document(file_path, caption)


def send_error_notification(error_msg: str) -> bool:
    """Notifica errores al operador."""
    text = f"❌ <b>Error en pipeline Inversiones</b>\n<code>{error_msg[:500]}</code>"
    return _send_message(text)


def send_startup_message() -> bool:
    """Confirmación de que el bot arrancó correctamente."""
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
