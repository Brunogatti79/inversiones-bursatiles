"""
src/tracker.py
Mejoras 7 y 10:
  7. Histórico de señales (últimos 30 días)
  10. Tracking de aciertos del modelo

Se llama desde pipeline.py después de analyzer.
"""

import json
import os
import logging
from datetime import datetime, timedelta

logger = logging.getLogger(__name__)

HISTORY_PATH = "data/signals_history.json"
ACCURACY_PATH = "data/accuracy_report.json"


def update_history(signals: list[dict], max_days: int = 60):
    """
    Mejora 7: Acumula señales diarias en signals_history.json.
    Formato: { "2026-05-16": [ {ticker, signal, signal_v2, precio, score_final_v2, ranking_accionable}, ... ] }
    """
    os.makedirs(os.path.dirname(HISTORY_PATH), exist_ok=True)

    history = {}
    if os.path.exists(HISTORY_PATH):
        try:
            with open(HISTORY_PATH) as f:
                history = json.load(f)
        except Exception:
            history = {}

    today = datetime.now().strftime("%Y-%m-%d")

    # Guardar snapshot de hoy (solo campos esenciales para no inflar el JSON)
    history[today] = [
        {
            "ticker": s["ticker"],
            "mercado": s.get("mercado", ""),
            "precio": s.get("precio_actual", 0),
            "signal": s.get("signal", ""),
            "signal_v2": s.get("signal_v2", ""),
            "score_v1": s.get("score_final", 0),
            "score_v2": s.get("score_final_v2", 0),
            "ranking": s.get("ranking_accionable", 0),
            "rr_ratio": s.get("rr_ratio", 0),
            "asset_quality": s.get("asset_quality", 0),
            "entry_score": s.get("entry_score", 0),
        }
        for s in signals
    ]

    # Purgar días viejos
    cutoff = (datetime.now() - timedelta(days=max_days)).strftime("%Y-%m-%d")
    history = {d: v for d, v in history.items() if d >= cutoff}

    with open(HISTORY_PATH, "w") as f:
        json.dump(history, f, ensure_ascii=False, indent=1)

    logger.info(f"Histórico actualizado: {len(history)} días guardados")
    return history


def compute_accuracy(history: dict) -> dict:
    """
    Mejora 10: Calcula hit rate por tipo de señal.
    
    Para cada señal emitida hace N días, compara el precio de ese día
    con el precio actual (último día en el historial).
    
    Retorna:
    {
      "⭐ COMPRA FUERTE": { "count": 5, "avg_ret_5d": 2.1, "avg_ret_20d": 5.3, "hit_rate_20d": 0.80 },
      "🟢 COMPRA": { ... },
      ...
    }
    """
    if not history:
        return {}

    sorted_dates = sorted(history.keys())
    if len(sorted_dates) < 6:
        logger.info("Menos de 6 días de historia, accuracy no disponible aún")
        return {}

    latest_date = sorted_dates[-1]
    latest_prices = {s["ticker"]: s["precio"] for s in history[latest_date]}

    results = {}
    lookback_windows = [5, 10, 20]

    for date_idx, date in enumerate(sorted_dates[:-5]):  # excluir últimos 5 días
        for s in history[date]:
            ticker = s["ticker"]
            signal = s.get("signal_v2") or s.get("signal", "")
            precio_entry = s["precio"]

            if not signal or precio_entry <= 0:
                continue

            if signal not in results:
                results[signal] = {"count": 0, "returns": {w: [] for w in lookback_windows}}

            results[signal]["count"] += 1

            for w in lookback_windows:
                future_idx = date_idx + w
                if future_idx < len(sorted_dates):
                    future_date = sorted_dates[future_idx]
                    future_prices = {ss["ticker"]: ss["precio"] for ss in history[future_date]}
                    future_price = future_prices.get(ticker)
                    if future_price and future_price > 0:
                        ret = ((future_price / precio_entry) - 1) * 100
                        results[signal]["returns"][w].append(ret)

    # Calcular estadísticas
    report = {}
    for signal, data in results.items():
        entry = {"count": data["count"]}
        for w in lookback_windows:
            rets = data["returns"][w]
            if rets:
                entry[f"avg_ret_{w}d"] = round(sum(rets) / len(rets), 2)
                entry[f"hit_rate_{w}d"] = round(len([r for r in rets if r > 0]) / len(rets), 2)
                entry[f"samples_{w}d"] = len(rets)
            else:
                entry[f"avg_ret_{w}d"] = None
                entry[f"hit_rate_{w}d"] = None
                entry[f"samples_{w}d"] = 0
        report[signal] = entry

    # Guardar reporte
    os.makedirs(os.path.dirname(ACCURACY_PATH), exist_ok=True)
    accuracy_output = {
        "generated": datetime.now().isoformat(),
        "total_days_history": len(history),
        "signals": report,
    }
    with open(ACCURACY_PATH, "w") as f:
        json.dump(accuracy_output, f, ensure_ascii=False, indent=2)

    logger.info(f"Accuracy report generado: {len(report)} tipos de señal analizados")
    return report
