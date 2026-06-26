"""
src/execution/risk_engine.py

"¿Qué stop/target le corresponde a una posición nueva (o existente sin
stop) ahora mismo?" — usa el ATR que analyzer.py ya calcula por señal.

FIX 26/06/2026: hasta esta misma sesión esto no tenía sentido implementar
todavía — atr/atr_stop/atr_target venían en 0.0 el 100% de las veces (ver
fix de causa raíz en src/analyzer.py::_atr: los CSV de cierres reales no
tienen columnas High/Low, así que el ATR clásico nunca se podía calcular).
Ahora que ATR tiene un valor real (proxy close-only, ver analyzer.py), este
módulo puede calcular un stop/target real para asignarlo a una posición.

División de responsabilidades (para no duplicar):
  - check_portfolio_alerts() en tracker.py YA detecta cuando un stop es
    tocado (precio_actual <= stop, en moneda nativa de la señal — eso es
    independiente del pricing USD del portfolio y ya empieza a funcionar
    solo con el fix de ATR, sin tocar nada más). Eso NO se duplica acá.
  - Este módulo es sobre el otro extremo: ASIGNAR el stop/target en USD a
    una posición en el momento en que se abre. Antes de este fix,
    /api/compra creaba cada posición con stop_loss=None hardcodeado, sin
    leer el atr_stop/atr_target que el analyzer ya había calculado para
    esa señal en ese mismo momento.

Nota sobre CEDEAR (GLOB, MELI, MSFT, etc.): mientras el pricing USD de
estas posiciones no tenga una fuente confiable (ver advertencia en
pricing_engine.py — los ratio_cedear guardados no coinciden con los
ratios reales de BYMA), este módulo tampoco les asigna stop_loss/target en
USD: hacerlo crearía un descalce de monedas (compararía un precio_actual_usd
congelado/no confiable contra un stop calculado con datos frescos). La
protección real que SÍ tienen estas posiciones hoy es la de
check_portfolio_alerts, que compara en la moneda nativa de la señal
(USD-NYSE) y es independiente de este problema.
"""

import json
import os
import logging

from src.execution.pricing_engine import get_ccl, get_brl_usd

logger = logging.getLogger(__name__)

SIGNALS_HISTORY_PATH = "data/signals_history.json"
SIGNALS_PREV_PATH = "data/signals_prev.json"


def _latest_signal_for_ticker(ticker: str):
    """
    Busca la señal más reciente disponible para un ticker.
    Prioridad: signals_prev.json (más fresco, pero efímero — puede no
    existir justo después de un redeploy de Railway, ver auditoría: este
    archivo nunca se persiste a GitHub) → último día de
    signals_history.json (sí persistido, siempre disponible).
    """
    if os.path.exists(SIGNALS_PREV_PATH):
        try:
            with open(SIGNALS_PREV_PATH) as f:
                prev = json.load(f)
            for s in prev:
                if s.get("ticker") == ticker:
                    return s
        except Exception:
            pass

    if os.path.exists(SIGNALS_HISTORY_PATH):
        try:
            with open(SIGNALS_HISTORY_PATH) as f:
                hist = json.load(f)
            if hist:
                last_date = sorted(hist.keys())[-1]
                for s in hist[last_date]:
                    if s.get("ticker") == ticker:
                        return s
        except Exception:
            pass

    return None


def compute_initial_stop_target(
    ticker: str,
    mercado: str,
    precio_fuente: str,
    ccl: float = None,
    brl_usd: float = None,
    signal: dict = None,
) -> tuple:
    """
    Devuelve (stop_loss_usd, target_usd, metodo) para una posición, a
    partir del atr_stop/atr_target que analyzer.py calculó para la señal
    vigente de ese ticker (en moneda nativa: ARS para MERVAL/BOVESPA, USD
    para SP500), convertido a USD con la misma lógica que el precio.

    Si no hay señal, ATR todavía no es válido, o la moneda no es
    confiable (caso CEDEAR, ver docstring del módulo) devuelve
    (None, None, motivo) — mejor no asignar nada que asignar un stop mal
    calculado, que podría disparar una falsa alerta o, peor, no disparar
    una real.
    """
    sig = signal or _latest_signal_for_ticker(ticker)
    if not sig:
        return None, None, "sin_senal"

    atr_stop_native   = sig.get("atr_stop", 0) or 0
    atr_target_native = sig.get("atr_target", 0) or 0
    atr_metodo        = sig.get("atr_metodo", "desconocido")

    if atr_stop_native <= 0 or atr_target_native <= 0:
        return None, None, "atr_no_disponible"

    if precio_fuente == "MERVAL_CSV":
        ccl = ccl if ccl is not None else get_ccl()
        if ccl <= 0:
            return None, None, "sin_ccl"
        stop_usd   = round(atr_stop_native / ccl, 6)
        target_usd = round(atr_target_native / ccl, 6)

    elif precio_fuente == "BOVESPA_CSV":
        brl_usd = brl_usd if brl_usd is not None else get_brl_usd()
        if brl_usd <= 0:
            return None, None, "sin_brl"
        stop_usd   = round(atr_stop_native / brl_usd, 6)
        target_usd = round(atr_target_native / brl_usd, 6)

    elif precio_fuente == "SP500_CSV":
        # Ver advertencia en docstring del módulo y en pricing_engine.py:
        # el pricing USD de estas posiciones (CEDEAR) no tiene fuente
        # confiable todavía — no se asigna stop en USD para evitar un
        # descalce de monedas. La protección real hoy viene de
        # check_portfolio_alerts (moneda nativa, independiente de esto).
        return None, None, "cedear_pricing_no_confiable"

    else:
        return None, None, "fuente_desconocida"

    return stop_usd, target_usd, f"atr_{atr_metodo}"


def backfill_missing_stops(portfolio: dict, dry_run: bool = True) -> list:
    """
    Recorre las posiciones abiertas con stop_loss/target en None y propone
    (o, si dry_run=False, aplica) el stop/target calculado por
    compute_initial_stop_target().

    Operación de mantenimiento de una sola vez — no corre automáticamente
    en el pipeline. Pensada para proteger retroactivamente las posiciones
    reales abiertas antes de este fix (17 posiciones, todas con
    stop_loss=None al 26/06/2026).

    dry_run=True (default): NO modifica `portfolio` — solo devuelve la
    lista de cambios propuestos para revisión antes de aplicar.

    Devuelve lista de dicts con, por posición:
      ticker, stop_loss_propuesto, target_propuesto, precio_actual_usd,
      ya_por_debajo_del_stop (bool — señal de que ya habría que vender
      HOY si este stop hubiera estado vigente), metodo.
    """
    ccl = get_ccl()
    brl_usd = get_brl_usd()
    propuestas = []

    for pos in portfolio.get("positions", []):
        if pos.get("stop_loss") not in (None, 0):
            continue  # ya tiene stop asignado, no tocar

        ticker  = pos.get("ticker", "")
        mercado = pos.get("mercado", "")
        precio_fuente = pos.get("precio_fuente") or (
            "MERVAL_CSV" if ticker.endswith(".BA") else
            "BOVESPA_CSV" if ticker.endswith(".SA") else
            "SP500_CSV"
        )
        stop_usd, target_usd, metodo = compute_initial_stop_target(
            ticker, mercado, precio_fuente, ccl=ccl, brl_usd=brl_usd
        )

        if stop_usd is None:
            propuestas.append({
                "ticker": ticker,
                "stop_loss_propuesto": None,
                "target_propuesto": None,
                "metodo": metodo,
            })
            continue

        precio_actual_usd = pos.get("precio_actual_usd", 0) or 0
        ya_por_debajo = precio_actual_usd > 0 and precio_actual_usd <= stop_usd

        propuestas.append({
            "ticker": ticker,
            "stop_loss_propuesto": stop_usd,
            "target_propuesto": target_usd,
            "precio_actual_usd": precio_actual_usd,
            "ya_por_debajo_del_stop": ya_por_debajo,
            "metodo": metodo,
        })

        if not dry_run:
            pos["stop_loss"] = stop_usd
            pos["target"] = target_usd

    return propuestas
