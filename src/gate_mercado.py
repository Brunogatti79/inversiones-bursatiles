"""
src/gate_mercado.py — Gate de señales de compra POR MERCADO (19/10/2026)
=======================================================================

Pedido de Bruno (19/10/2026), aplicando su propio principio del 09/09:
"si no está confirmado, no se debe informar para ningún tipo de acción".

Evidencia que lo motivó (backtest_results.json → benchmark.by_market, neto
de costos Balanz, 21 ruedas): las COMPRA del modelo rindieron PEOR o igual
que comprar toda la canasta equiponderada del mismo mercado en los tres
mercados (MERVAL −1,1 pp, BOVESPA −2,1 pp, S&P −0,1 pp). El +13% de EV que
mostraba BOVESPA era el mercado subiendo, no la selección.

Regla (fijada ANTES de ver datos nuevos, no se optimiza):
  Las señales de compra de un mercado se muestran como COMPRA solo si el
  alpha neto de las COMPRA contra la canasta del mismo mercado es:
    - positivo,
    - significativo al 95% (significativo_95 del backtester),
    - medido por RACHA (trades independientes, no días superpuestos) cuando
      ese dato existe; si no, por día,
    - con >= MIN_VENTANAS ventanas independientes de 21 ruedas y
      >= MIN_SAMPLES operaciones.
  Si no se cumple, toda COMPRA / COMPRA FUERTE de ese mercado pasa a
  "🔵 SIN CONFIRMAR" (o "🔵 SIN CONFIRMAR ⚠️" si el alpha es negativo y
  significativo). Se reabre sola cuando la evidencia aparezca.

Solo capa de presentación (dashboard y Telegram). NO toca signal_v2 en
analyzer/tracker/signals_history, ni Kelly/portfolio_optimizer: el
backtester sigue midiendo la señal sin filtrar, que es lo que permite
saber cuándo reabrir el gate.
"""

from __future__ import annotations

import json
import logging

logger = logging.getLogger(__name__)

BACKTEST_PATH = "data/backtest_results.json"
MERCADOS = ("MERVAL", "BOVESPA", "SP500")
SENALES_COMPRA = ("🟢 COMPRA", "⭐ COMPRA FUERTE")
MIN_VENTANAS = 3.0
MIN_SAMPLES = 15


def cargar_backtest(path: str = BACKTEST_PATH) -> dict:
    try:
        with open(path, encoding="utf-8") as f:
            return json.load(f) or {}
    except Exception:
        return {}


def estado_por_mercado(backtest: dict) -> dict:
    """{mercado: {habilitado, alpha_pp, p_value, n, ventanas, nivel,
    negativo_sig, ret_compras, ret_canasta, motivo}}."""
    bm = ((backtest or {}).get("benchmark") or {}).get("by_market") or {}
    out = {}
    for mk in MERCADOS:
        compra = (bm.get(mk) or {}).get("compra") or {}
        nivel = "racha" if compra.get("racha") else "dia"
        blk = compra.get(nivel) or {}
        alpha = blk.get("alpha_ew_neto") or {}
        avg = alpha.get("avg_ret")
        sig = bool(alpha.get("significativo_95"))
        n = alpha.get("samples") or blk.get("n") or 0
        vent = blk.get("ventanas_independientes") or 0.0
        e = {
            "habilitado": False,
            "alpha_pp": round(float(avg), 2) if avg is not None else None,
            "p_value": alpha.get("p_value"),
            "n": int(n),
            "ventanas": round(float(vent), 1),
            "nivel": nivel,
            "negativo_sig": bool(avg is not None and avg < 0 and sig),
            "ret_compras": blk.get("ev_neto"),
            "ret_canasta": blk.get("ret_ew"),
        }
        if avg is None:
            e["motivo"] = "sin medición contra la canasta todavía"
        elif n < MIN_SAMPLES or vent < MIN_VENTANAS:
            e["motivo"] = (f"muestra insuficiente (n={int(n)}, {vent:.1f} ventanas; "
                           f"se exige n≥{MIN_SAMPLES} y ≥{MIN_VENTANAS:.0f} ventanas)")
        elif avg <= 0:
            e["motivo"] = (f"las compras rindieron {avg:+.2f} pp vs comprar toda la canasta"
                           + (" (significativo)" if sig else " (no significativo)"))
        elif not sig:
            e["motivo"] = f"alpha {avg:+.2f} pp vs canasta, pero no significativo (p={alpha.get('p_value')})"
        else:
            e["habilitado"] = True
            e["motivo"] = f"alpha {avg:+.2f} pp vs canasta, significativo"
        out[mk] = e
    return out


def etiqueta_gateada(signal: str, mercado: str, estados: dict):
    """Devuelve la etiqueta de reemplazo si la señal es de compra y su
    mercado no está habilitado; None si no hay que tocarla. El texto NUNCA
    contiene la palabra "COMPRA" (los filtros JS buscan esa substring)."""
    if signal not in SENALES_COMPRA:
        return None
    e = (estados or {}).get(mercado)
    if e and e.get("habilitado"):
        return None
    return "🔵 SIN CONFIRMAR ⚠️" if (e and e.get("negativo_sig")) else "🔵 SIN CONFIRMAR"
