"""
src/execution/order_engine.py

Ejecución de compra/venta de posiciones del portfolio.

FIX 26/06/2026: esta lógica vivía embebida directamente dentro del handler
HTTP de start_server.py (_handle_portfolio_op) — mezclando parseo de
request, lógica de negocio y persistencia en un solo método de ~80 líneas.
Se extrae acá para que start_server.py quede como una capa delgada que solo
traduce HTTP ↔ estas funciones (eso es, en concreto, lo que significa
"desacoplar el execution layer de la capa de decisión/HTTP").

Cambio funcional real (no solo de organización): execute_compra() ahora
asigna stop_loss/target reales a la posición nueva, usando
risk_engine.compute_initial_stop_target() — antes de este fix, toda
posición nueva se creaba con stop_loss=None, target=None hardcodeado, sin
leer el atr_stop/atr_target que el analyzer ya había calculado para esa
señal en ese mismo momento. Para CEDEARs (SP500_CSV) sigue sin asignarse
stop en USD por la misma razón documentada en pricing_engine.py / risk_engine.py
(pricing CEDEAR sin fuente confiable todavía).

El resto del comportamiento (promediado de compras, venta parcial/total,
push a GitHub, notificación Telegram) es el mismo que tenía start_server.py
— se preservó deliberadamente para no introducir regresiones en una
funcionalidad que ya estaba en uso real con dinero real.
"""

import os
import json
import logging
from datetime import datetime

from src.execution import risk_engine

logger = logging.getLogger(__name__)

PORTFOLIO_PATH = "data/portfolio.json"


def _load_portfolio() -> dict:
    if os.path.exists(PORTFOLIO_PATH):
        with open(PORTFOLIO_PATH) as f:
            return json.load(f)
    return {"positions": [], "reglas": {}}


def _save_and_push(portfolio: dict, commit_msg: str) -> tuple:
    """Guarda local + pushea a GitHub. Devuelve (pushed: bool, error: str|None)."""
    portfolio["last_updated"] = datetime.now().strftime("%Y-%m-%d %H:%M")
    try:
        with open(PORTFOLIO_PATH, "w") as f:
            json.dump(portfolio, f, ensure_ascii=False, indent=2)
    except Exception as e:
        return False, f"error guardando local: {e}"

    from src.github_persistence import push_file
    pushed = push_file(PORTFOLIO_PATH, commit_msg)
    return pushed, (None if pushed else "push a GitHub falló (ver logs) — portfolio guardado localmente en Railway")


def _notify_telegram(op_type: str, ticker: str, cantidad: int, precio_unitario: float,
                      total_invertido: float, pushed: bool, push_error: str = None):
    try:
        import requests as _req_tg
        bot_token = os.environ.get("TELEGRAM_BOT_TOKEN", "")
        chat_ids = [c.strip() for c in os.environ.get(
            "CHAT_IDS", os.environ.get("TELEGRAM_CHAT_ID", "")
        ).split(",") if c.strip()]
        if not (bot_token and chat_ids):
            return
        icon = "🟢" if op_type == "compra" else "🔴"
        push_status = "✅ Sincronizado con GitHub" if pushed else f"⚠️ <b>Push falló</b>: {push_error}"
        text = (
            f"{icon} <b>{op_type.upper()} registrada</b>\n"
            f"Ticker: <b>{ticker}</b> — {cantidad} @ USD {precio_unitario:.4f}\n"
            f"Total: USD {total_invertido:.2f}\n"
            f"{push_status}"
        )
        for cid in chat_ids:
            try:
                _req_tg.post(
                    f"https://api.telegram.org/bot{bot_token}/sendMessage",
                    json={"chat_id": cid, "text": text, "parse_mode": "HTML"},
                    timeout=10,
                )
            except Exception:
                pass
    except Exception as e:
        logger.warning(f"[order_engine] Telegram notify error: {e}")


def execute_compra(
    ticker: str,
    precio_raw: float,
    total_usd_raw: float,
    cantidad: int,
    nombre: str = None,
    mercado_form: str = "",
    precio_fuente_form: str = "",
    ratio_cedear_form: float = 1.0,
) -> dict:
    """
    Registra una compra nueva o promedia una posición existente.
    Devuelve {"status": "ok"|"error", "msg"/"error": str, "pushed": bool, ...}
    """
    ticker = (ticker or "").upper()
    nombre = nombre or ticker
    if not ticker or precio_raw <= 0 or cantidad <= 0:
        return {"status": "error", "error": "ticker, precio y nominales requeridos", "http_code": 400}

    precio_unitario = round(precio_raw, 6)
    total_invertido = round(precio_unitario * cantidad, 2) if total_usd_raw <= 0 else round(total_usd_raw, 2)

    portfolio = _load_portfolio()
    fecha = datetime.now().strftime("%Y-%m-%d")

    existing = next((p for p in portfolio.get("positions", []) if p["ticker"] == ticker), None)

    if existing:
        old_total = existing.get("total_invertido", existing["precio_compra"] * existing["cantidad"])
        new_total_inv = old_total + total_invertido
        new_cant = existing["cantidad"] + cantidad
        new_precio_usd = round(new_total_inv / new_cant, 6)
        existing["precio_compra"]     = new_precio_usd
        existing["precio_compra_usd"] = new_precio_usd
        existing["cantidad"]          = new_cant
        existing["valor_inicial_usd"] = round(new_total_inv, 2)
        existing["valor_actual_usd"]  = round(new_total_inv, 2)
        existing["rend_usd"]          = 0
        # FIX 29/06/2026: fecha_compra ya NO se reescribe al promediar. Antes esto
        # resetaba la antigüedad real de la posición cada vez que se sumaba a un
        # ticker existente (afecta el TIME STOP de tracker.py, que mide días desde
        # fecha_compra). La fecha de entrada original ahora se conserva; el detalle
        # del add-on queda solo en "notas".
        existing["notas"] = f"Promediado {fecha}: +{cantidad} nom, +U$D {total_invertido}"
        msg = f"Compra agregada a {ticker}, total: U$D {new_total_inv:.0f}, {new_cant} nom"
    else:
        mercado = mercado_form or (
            "MERVAL" if ticker.endswith(".BA") else
            "BOVESPA" if ticker.endswith(".SA") else
            "SP500"
        )
        precio_fuente = precio_fuente_form or (
            "MERVAL_CSV" if ticker.endswith(".BA") else
            "BOVESPA_CSV" if ticker.endswith(".SA") else
            "SP500_CSV"
        )
        ratio_cedear = float(ratio_cedear_form or 1.0)

        # FIX 26/06/2026: stop/target real desde el ATR de la señal vigente,
        # en vez de None hardcodeado. Devuelve (None, None, motivo) si no
        # hay datos confiables todavía (ver risk_engine.py) — se guarda el
        # motivo en la posición para que sea visible por qué no tiene stop.
        stop_usd, target_usd, stop_metodo = risk_engine.compute_initial_stop_target(
            ticker, mercado, precio_fuente
        )

        new_pos = {
            "ticker": ticker, "nombre": nombre,
            "mercado": mercado, "moneda": "USD",
            "precio_compra":     precio_unitario,
            "precio_compra_usd": precio_unitario,
            "precio_actual_usd": precio_unitario,
            "cantidad": cantidad,
            "valor_inicial_usd": round(total_invertido, 2),
            "valor_actual_usd":  round(total_invertido, 2),
            "rend_usd": 0,
            "fecha_compra": fecha,
            "stop_loss": stop_usd,
            "target": target_usd,
            "stop_metodo": stop_metodo,
            "precio_fuente": precio_fuente,
            "ratio_cedear":  ratio_cedear if precio_fuente == "SP500_CSV" else 1.0,
            "notas": f"Compra {fecha} via Dashboard — {precio_fuente}",
        }
        portfolio.setdefault("positions", []).append(new_pos)
        stop_info = f", stop USD {stop_usd:.4f}" if stop_usd else " (sin stop automático aún)"
        msg = f"Nueva posición: {ticker} {cantidad} nom @ U$D {total_invertido:.0f} total [{precio_fuente}]{stop_info}"

    pushed, push_error = _save_and_push(portfolio, f"api: compra {ticker} {fecha}")
    _notify_telegram("compra", ticker, cantidad, precio_unitario, total_invertido, pushed, push_error)

    result = {"status": "ok", "msg": msg, "pushed": pushed, "http_code": 200}
    if not pushed:
        result["push_warning"] = push_error
    return result


def execute_venta(ticker: str, precio_raw: float, cantidad: int) -> dict:
    """
    Registra una venta parcial o total de una posición existente.
    Devuelve {"status": "ok"|"error", "msg"/"error": str, "pushed": bool, ...}
    """
    ticker = (ticker or "").upper()
    if not ticker or precio_raw <= 0 or cantidad <= 0:
        return {"status": "error", "error": "ticker, precio y nominales requeridos", "http_code": 400}

    precio_unitario = round(precio_raw, 6)
    portfolio = _load_portfolio()
    fecha = datetime.now().strftime("%Y-%m-%d")

    existing = next((p for p in portfolio.get("positions", []) if p["ticker"] == ticker), None)
    if not existing:
        return {"status": "error", "error": f"No hay posición en {ticker}", "http_code": 404}
    if cantidad > existing["cantidad"]:
        return {"status": "error", "error": f"Solo tenés {existing['cantidad']} de {ticker}", "http_code": 400}

    pc = existing.get("precio_compra_usd") or existing.get("precio_compra", 0)
    pnl_pct = round((precio_unitario / pc - 1) * 100, 2) if pc > 0 else 0
    pnl_abs = round((precio_unitario - pc) * cantidad, 2)

    if cantidad == existing["cantidad"]:
        portfolio["positions"] = [p for p in portfolio["positions"] if p["ticker"] != ticker]
        msg = f"Venta total {ticker}. P&L: {pnl_abs} ({pnl_pct}%)"
    else:
        restantes = existing["cantidad"] - cantidad
        existing["cantidad"] = restantes
        existing["valor_inicial_usd"] = round(pc * restantes, 2)
        existing["valor_actual_usd"]  = round(precio_unitario * restantes, 2)
        existing["rend_usd"] = round((precio_unitario - pc) * restantes, 2)
        existing["notas"] = f"Venta parcial {fecha}: -{cantidad} @ USD {precio_unitario}"
        msg = f"Venta parcial {ticker}. P&L: {pnl_abs} ({pnl_pct}%)"

    total_invertido = round(precio_unitario * cantidad, 2)
    pushed, push_error = _save_and_push(portfolio, f"api: venta {ticker} {fecha}")
    _notify_telegram("venta", ticker, cantidad, precio_unitario, total_invertido, pushed, push_error)

    result = {"status": "ok", "msg": msg, "pushed": pushed, "http_code": 200}
    if not pushed:
        result["push_warning"] = push_error
    return result


def execute_edit_fecha_compra(ticker: str, nueva_fecha: str) -> dict:
    """
    NUEVO 29/06/2026: edita manualmente fecha_compra de una posición existente —
    para corregir el valor por defecto (fecha de carga al sistema, o fecha del
    último promediado antes del fix de arriba) cuando la posición real es más
    antigua. No toca precio/cantidad/stop. Usado por el input editable de la
    columna "Días" en el dashboard (Portfolio tab).
    Devuelve {"status": "ok"|"error", "msg"/"error": str, "pushed": bool, ...}
    """
    ticker = (ticker or "").upper()
    if not ticker:
        return {"status": "error", "error": "ticker requerido", "http_code": 400}

    try:
        fc_nueva = datetime.strptime(nueva_fecha, "%Y-%m-%d")
    except (ValueError, TypeError):
        return {"status": "error", "error": "fecha debe tener formato YYYY-MM-DD", "http_code": 400}
    if fc_nueva > datetime.now():
        return {"status": "error", "error": "la fecha de compra no puede ser futura", "http_code": 400}

    portfolio = _load_portfolio()
    existing = next((p for p in portfolio.get("positions", []) if p["ticker"] == ticker), None)
    if not existing:
        return {"status": "error", "error": f"No hay posición en {ticker}", "http_code": 404}

    fecha_anterior = existing.get("fecha_compra")
    existing["fecha_compra"] = nueva_fecha
    msg = f"{ticker}: fecha_compra {fecha_anterior or '—'} → {nueva_fecha}"

    pushed, push_error = _save_and_push(portfolio, f"api: editar fecha_compra {ticker} -> {nueva_fecha}")

    result = {"status": "ok", "msg": msg, "pushed": pushed, "http_code": 200}
    if not pushed:
        result["push_warning"] = push_error
    return result
