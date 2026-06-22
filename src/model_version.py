"""
src/model_version.py

Versionado del modelo + tracking de cambios (mejora 1.2).

Complementa a opportunities_log.py: ese mide la efectividad de cada
recomendación individual mostrada en el dashboard; esto mide la efectividad
del MODELO EN SU CONJUNTO, versión por versión, para poder responder
"¿esto que implementamos mejoró algo medible?" con datos en vez de a ojo
(el punto que quedó pendiente en la sección 9 de la arquitectura v4).

El CHANGELOG es manual a propósito: un commit de UI no es un cambio de
modelo, y automatizar esto a partir de git log generaría ruido. Cada vez que
se shippea un cambio que puede afectar señales/scores/predicciones, se agrega
una entrada acá con la versión nueva.
"""

from datetime import datetime

MODEL_VERSION = "4.1"

CHANGELOG = [
    {
        "version": "4.1",
        "date": "2026-06-22",
        "changes": [
            "Fix CRÍTICO: optimizer.py tenía load_optimized_weights() roto (buscaba "
            "data['weights'], clave que run_weight_optimization() nunca escribió) — los "
            "pesos optimizados por weight_optimizer.py se calculaban y guardaban bien "
            "pero NUNCA se aplicaban al analyzer. Consolidado en weight_optimizer.py, "
            "optimizer.py eliminado.",
            "Fix: TAMAR (ARG) en vivo vía BCRA en vez de valor fijo congelado desde mayo.",
            "Fix: eliminado fallback ISM→MANEMP (unidades incompatibles, distorsionaba "
            "el score macro USA).",
            "Fix: BRL/USD del portfolio usaba dos variables inexistentes (NameError "
            "silencioso atrapado por except genérico) — update_portfolio_usd nunca "
            "corría con BRL/USD real.",
            "Fix: signals_history.json no se persistía a GitHub — backtester/weight_optimizer "
            "nunca acumulaban más de ~1 día de historia real entre redeploys.",
            "Persistencia unificada en github_persistence.py (antes 8 implementaciones "
            "duplicadas del mismo patrón GET-sha/PUT).",
            "Nuevo: opportunities_log.py registra y evalúa efectividad real de las "
            "Oportunidades mostradas en el dashboard.",
        ],
    },
]

PERFORMANCE_HISTORY_PATH = "data/model_performance_history.json"
VERSION_PATH = "data/model_version.json"
MAX_HISTORY_DAYS = 365


def current_version() -> dict:
    return {"version": MODEL_VERSION, "changelog": CHANGELOG}


def log_model_run(backtest_results: dict = None):
    """
    Guarda un snapshot de {versión activa + métricas de backtest del día} en
    data/model_performance_history.json. Llamar 1x por run, después de
    backtester.run_backtest(), para tener trazabilidad real entre versión de
    código y resultado medible.
    """
    from src.github_persistence import load_json, save_json

    backtest_results = backtest_results or {}
    today = datetime.now().strftime("%Y-%m-%d")

    by_market_clean = {}
    for mkt, v in (backtest_results.get("by_market") or {}).items():
        if not isinstance(v, dict):
            continue
        by_market_clean[mkt] = {
            "n": v.get("n"),
            "win_rate": v.get("win_rate"),
            "avg_ret_21d": v.get("avg_ret_21d"),
            "expected_value": v.get("expected_value"),
            "sharpe": v.get("sharpe"),
            "max_drawdown": v.get("max_drawdown"),
        }

    snapshot = {
        "date": today,
        "version": MODEL_VERSION,
        "total_trades": backtest_results.get("total_trades"),
        "days_history": backtest_results.get("days_history"),
        "by_market": by_market_clean,
        "predictor_accuracy": (backtest_results.get("predictor") or {}).get("directional_accuracy"),
    }

    history = load_json(PERFORMANCE_HISTORY_PATH, default=[])
    history = [h for h in history if h.get("date") != today]  # dedupe por día (rerun mismo día)
    history.append(snapshot)
    history.sort(key=lambda h: h.get("date", ""))
    history = history[-MAX_HISTORY_DAYS:]

    save_json(PERFORMANCE_HISTORY_PATH, history, message=f"auto: model_performance {today} (v{MODEL_VERSION})")
    save_json(VERSION_PATH, current_version(), message=f"auto: model_version v{MODEL_VERSION}")


def compare_versions(v1: str, v2: str) -> dict:
    """
    Compara métricas promedio entre dos versiones del modelo usando
    model_performance_history.json. Pensado para responder, con datos,
    si una mejora propuesta por una IA externa funcionó o no una vez que
    haya suficientes runs en ambas versiones.
    """
    from src.github_persistence import load_json
    history = load_json(PERFORMANCE_HISTORY_PATH, default=[])

    def _avg_for(version):
        rows = [h for h in history if h.get("version") == version]
        if not rows:
            return None
        markets = set()
        for r in rows:
            markets.update((r.get("by_market") or {}).keys())
        out = {"n_runs": len(rows)}
        for mkt in markets:
            wrs = [r["by_market"][mkt]["win_rate"] for r in rows
                   if r.get("by_market", {}).get(mkt, {}).get("win_rate") is not None]
            evs = [r["by_market"][mkt]["expected_value"] for r in rows
                   if r.get("by_market", {}).get(mkt, {}).get("expected_value") is not None]
            out[mkt] = {
                "win_rate_avg": round(sum(wrs) / len(wrs), 1) if wrs else None,
                "ev_avg": round(sum(evs) / len(evs), 2) if evs else None,
            }
        return out

    return {
        "version_1": {"id": v1, "metrics": _avg_for(v1)},
        "version_2": {"id": v2, "metrics": _avg_for(v2)},
    }
