"""
src/trailing_stop.py — Profit Locking Dinámico

PROBLEMA QUE RESUELVE:
  El exit_model calcula stops al momento de la señal (entrada).
  Una vez que una posición sube +15%, el stop sigue donde estaba.
  Resultado: se devuelve ganancia evitable.

LÓGICA:
  R = ATR en el momento de entrada (atr_entrada)
  unrealized_R = (precio_actual - precio_compra) / atr_entrada

  Si unrealized_R > 1.5 → mover stop a breakeven (precio_compra)
  Si unrealized_R > 2.5 → mover stop a precio_compra + 1R
  Si unrealized_R > 4.0 → mover stop a precio_compra + 2R

  En todos los casos: el stop nunca retrocede (sólo sube).

INTEGRACIÓN:
  Corre desde pipeline.py DESPUÉS de update_portfolio_usd,
  actualiza portfolio.json con los nuevos stops.
  Envía alerta Telegram si un stop fue ajustado.

USO:
    from src.trailing_stop import apply_trailing_stops
    stop_events = apply_trailing_stops(all_signals)
"""

import json
import os
import logging
from datetime import datetime

logger = logging.getLogger(__name__)

PORTFOLIO_PATH = "data/portfolio.json"

# Umbrales en múltiplos de R
R_BREAKEVEN   = 1.5   # → stop a precio_compra (breakeven)
R_LOCK_1R     = 2.5   # → stop a precio_compra + 1R
R_LOCK_2R     = 4.0   # → stop a precio_compra + 2R


def apply_trailing_stops(all_signals: list[dict]) -> list[dict]:
    """
    Revisa posiciones abiertas y actualiza stops según profit locking.

    Args:
        all_signals: señales actuales del pipeline (para precio actual y ATR)

    Returns:
        lista de eventos de ajuste de stop (para notificaciones)
    """
    if not os.path.exists(PORTFOLIO_PATH):
        return []

    try:
        with open(PORTFOLIO_PATH) as f:
            portfolio = json.load(f)
    except Exception as e:
        logger.warning(f"[trailing_stop] Error leyendo portfolio: {e}")
        return []

    positions = portfolio.get("positions", [])
    if not positions:
        return []

    # Mapa ticker → señal actual (para precio y ATR)
    sig_map = {}
    for s in all_signals:
        t = s.get("ticker", "")
        sig_map[t] = s
        # También mapear sin sufijo
        sig_map[t.replace(".BA", "").replace(".SA", "")] = s

    events = []
    updated = False

    for pos in positions:
        ticker       = pos.get("ticker", "")
        precio_compra = float(pos.get("precio_compra_usd", 0) or 0)
        precio_actual = float(pos.get("precio_actual_usd", 0) or 0)
        stop_actual   = float(pos.get("atr_stop_dinamico", 0) or
                              pos.get("stop_loss", 0) or 0)

        if precio_compra <= 0 or precio_actual <= 0:
            continue

        # Obtener ATR desde señal actual
        sig = sig_map.get(ticker) or sig_map.get(ticker.replace(".BA","").replace(".SA",""))
        if not sig:
            continue

        # ATR en USD (mismo proceso que para el precio)
        atr_raw = float(sig.get("atr", 0) or 0)
        ccl     = _get_ccl()
        mercado = pos.get("mercado", "MERVAL")

        if atr_raw <= 0:
            continue

        # Convertir ATR a USD según mercado
        if mercado == "MERVAL" and ccl > 0:
            atr_usd = atr_raw / ccl
        elif mercado == "SP500":
            atr_usd = atr_raw
        else:
            atr_usd = atr_raw / max(ccl, 1) if ccl > 0 else atr_raw

        if atr_usd <= 0:
            continue

        # Calcular unrealized_R
        ganancia        = precio_actual - precio_compra
        unrealized_R    = ganancia / atr_usd

        # Calcular nuevo stop según nivel de profit locking
        nuevo_stop = stop_actual  # por defecto sin cambio

        if unrealized_R >= R_LOCK_2R:
            # Traba en precio_compra + 2R
            candidato = precio_compra + 2 * atr_usd
            nuevo_stop = max(stop_actual, candidato)
            nivel = "2R (precio+2R)"

        elif unrealized_R >= R_LOCK_1R:
            # Traba en precio_compra + 1R
            candidato = precio_compra + 1 * atr_usd
            nuevo_stop = max(stop_actual, candidato)
            nivel = "1R (precio+1R)"

        elif unrealized_R >= R_BREAKEVEN:
            # Traba en breakeven
            candidato = precio_compra
            nuevo_stop = max(stop_actual, candidato)
            nivel = "breakeven"

        else:
            # Todavía no alcanzó 1.5R — no mover
            continue

        # Solo actualizar si el stop realmente sube (nunca retrocede)
        if nuevo_stop > stop_actual + 0.0001:
            pos["atr_stop_dinamico"] = round(nuevo_stop, 4)
            updated = True
            event = {
                "ticker":        ticker,
                "tipo":          "trailing_stop",
                "nivel":         nivel,
                "unrealized_R":  round(unrealized_R, 2),
                "stop_anterior": round(stop_actual, 4),
                "stop_nuevo":    round(nuevo_stop, 4),
                "precio_actual": round(precio_actual, 4),
                "precio_compra": round(precio_compra, 4),
                "ts":            datetime.now().isoformat(),
            }
            events.append(event)
            logger.info(
                f"[trailing_stop] {ticker}: stop {stop_actual:.4f} → {nuevo_stop:.4f} "
                f"| {nivel} | unrealized_R={unrealized_R:.1f}"
            )

    if updated:
        portfolio["last_updated"] = datetime.now().strftime("%Y-%m-%d %H:%M")
        try:
            with open(PORTFOLIO_PATH, "w") as f:
                json.dump(portfolio, f, ensure_ascii=False, indent=2)
            _push_to_github()
        except Exception as e:
            logger.warning(f"[trailing_stop] Error guardando portfolio: {e}")

    if events:
        logger.info(f"[trailing_stop] {len(events)} stops ajustados")

    return events


def _get_ccl() -> float:
    """Lee CCL del cache. Rechaza valores fuera de rango plausible (FIX
    29/07/2026, mismo incidente que pricing_engine.get_ccl(): un CCL
    corrupto acá infla/desinfla directamente el unrealized_R usado para
    mover trailing stops reales -- más motivo todavía para no confiar en
    cualquier número positivo sin chequear orden de magnitud."""
    CCL_PLAUSIBLE_MIN, CCL_PLAUSIBLE_MAX = 300.0, 6000.0
    try:
        ccl_path = "data/ccl_cache.json"
        if os.path.exists(ccl_path):
            with open(ccl_path) as f:
                ccl = float(json.load(f).get("compra", 0) or 0)
            if CCL_PLAUSIBLE_MIN <= ccl <= CCL_PLAUSIBLE_MAX:
                return ccl
            if ccl > 0:
                logger.warning(
                    f"[trailing_stop] CCL cacheado ({ccl}) fuera de rango "
                    f"plausible -- se ignora para no distorsionar unrealized_R."
                )
    except Exception:
        pass
    return 0.0


def _push_to_github():
    """Push portfolio.json a GitHub tras actualizar stops."""
    from src.github_persistence import push_file
    push_file(PORTFOLIO_PATH, f"auto: trailing stops actualizados {datetime.now().strftime('%Y-%m-%d %H:%M')}")
