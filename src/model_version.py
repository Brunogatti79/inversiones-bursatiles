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

MODEL_VERSION = "4.3"

CHANGELOG = [
    {
        "version": "4.3",
        "date": "2026-06-24",
        "changes": [
            "Fix: quality_check.py (13 checks cruzados, semáforo por señal) estaba "
            "documentado como activo desde v2 pero nunca se llamaba desde ningún lado "
            "— confirmado con grep en todo el repo. Activado en pipeline.py justo antes "
            "del confidence score por señal. Efecto en señales: el campo quality_flag "
            "que confidence_score.py lee para el componente 'Quality checks (20%)' del "
            "Kelly ajustado por confianza pasa de ser siempre 🟢 (default no-op) a "
            "reflejar datos reales.",
            "Feat: confidence score GLOBAL del run + kill switch (confidence_score.py + "
            "monitor.py). Circuit breaker de sistema (no por ticker): si la confianza "
            "ponderada del run (calidad de datos 30% / predictor 25% / macro 20% / "
            "integridad de CSVs 10% / SLA 15%) cae por debajo de 35, o si "
            "data_validator marca ERROR, o si hay ≥5 alertas críticas de quality_check, "
            "se frena kelly_half/kelly_half_adj a 0 en TODAS las señales (no toca "
            "posiciones ya abiertas). Persistido en data/system_confidence.json. "
            "Alerta Telegram solo en transición de estado, no en cada run. Sin validar "
            "todavía contra una corrida real con el kill switch activo.",
            "Feat: tracker.update_history() ahora persiste también aq_weight_used/ "
            "es_weight_used (peso dinámico AQ vs ES por mercado, para auditar "
            "weight_optimizer después del hecho) y consenso (si V1/V2 coincidieron) "
            "— estaban en la señal en vivo de analyzer.py pero no en el historial.",
            "Tests: +7 archivos / +67 tests (de 99 a 166) con foco en watchPatterns, "
            "github_persistence (retry/sha/backoff), toggle ENABLE_RF_PREDICTOR, "
            "contrato de sync al arrancar, y todo lo de este changelog. Incluye fix de "
            "un test preexistente (test_weight_optimizer.py) con timestamp hardcodeado "
            "que ya había cruzado el umbral de staleness de 7 días.",
        ],
    },
    {
        "version": "4.2",
        "date": "2026-06-22",
        "changes": [
            "Fix CRÍTICO (1.3): optimizer.py tenía load_optimized_weights() roto (buscaba "
            "data['weights'], clave que run_weight_optimization() nunca escribió) — los "
            "pesos optimizados se calculaban y guardaban bien pero NUNCA se aplicaban al "
            "analyzer. Consolidado en weight_optimizer.py, optimizer.py eliminado.",
            "Fix (4.1): portfolio_optimizer._covariance_adjustment() calculaba la matriz de "
            "covarianza con shrinkage pero solo usaba la diagonal (= inverse-variance puro, "
            "ignoraba la correlación por completo). Ahora usa minimum-variance real "
            "(w ∝ inv(Σ)·1) — activos correlacionados reciben menos peso conjunto.",
            "Feat (2.1): normalización macro con percentil rolling sobre historia real "
            "(data/macro_raw_history.json), con fallback automático a rangos fijos "
            "mientras no haya ≥60 observaciones. Incluye bootstrap manual vía FRED "
            "histórico para las 9 variables de USA.",
            "Feat (2.2): AQ_weight/ES_weight de V2 ahora se ajustan dinámicamente por "
            "régimen de volatilidad del activo (antes fijos por mercado).",
            "Feat (2.3): nuevo feature de drawdown reciente (30d/90d) — penaliza activos "
            "que acaban de colapsar en el corto plazo, antes podían parecer 'gangas'.",
            "Feat (2.4): predictor ensemble pasa de 2 a 4 modelos (Holt-Winters + Gradient "
            "Boosting + Random Forest + Ridge linear baseline). Trade-off conocido: ~25-30% "
            "más tiempo de cómputo en predictor.py por la adición de Random Forest.",
            "Feat (4.2): stop dinámico ahora también escala por fuerza de señal V2 (señal "
            "fuerte → stop más holgado, señal débil → stop más ajustado), además de "
            "mercado/volatilidad/régimen.",
            "Feat (1.1): persistencia unificada en github_persistence.py — reemplaza 8 "
            "implementaciones duplicadas del patrón GET-sha/PUT (tracker, monitor, "
            "backtester, macro_auto, opportunities_log, trailing_stop, bot, start_server).",
            "Feat (1.2): este módulo (model_version.py) — versionado + tracking de "
            "performance por versión, para poder atribuir cambios en backtest_results.json "
            "a un cambio de código concreto.",
        ],
    },
    {
        "version": "4.1",
        "date": "2026-06-22",
        "changes": [
            "Fix: TAMAR (ARG) en vivo vía BCRA en vez de valor fijo congelado desde mayo.",
            "Fix: eliminado fallback ISM→MANEMP (unidades incompatibles, distorsionaba "
            "el score macro USA).",
            "Fix: BRL/USD del portfolio usaba dos variables inexistentes (NameError "
            "silencioso atrapado por except genérico) — update_portfolio_usd nunca "
            "corría con BRL/USD real.",
            "Fix: signals_history.json no se persistía a GitHub — backtester/weight_optimizer "
            "nunca acumulaban más de ~1 día de historia real entre redeploys.",
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
