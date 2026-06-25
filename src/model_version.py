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

MODEL_VERSION = "4.5"

CHANGELOG = [
    {
        "version": "4.5",
        "date": "2026-06-25",
        "changes": [
            "Fix CRÍTICO: apply_prediction_override() (pipeline.py) estaba "
            "completamente definida (4 reglas, docstring, log de resumen) "
            "pero NUNCA se llamaba desde ningún lado — confirmado con grep "
            "en todo el repo. El tooltip del dashboard (columna pred21) "
            "afirmaba 'Si negativo + señal COMPRA → override automático "
            "degrada la señal' desde siempre; era falso hasta este fix. "
            "Se activa ahora, gateada por predictor_health: si el "
            "predictor está DEGRADED se omite la Regla 1 (la basada en "
            "pred_21d); la Regla 2 (estructural, ret_anual<-40%) nunca se "
            "omite. Corre ANTES del portfolio optimizer para que éste no "
            "asigne capital a una señal que ya fue degradada de COMPRA a "
            "NEUTRAL/VENTA. Efecto en señales: por primera vez, señales de "
            "COMPRA con predicción 21d negativa se ajustan automáticamente "
            "— antes no pasaba nada.",
            "Fix: predictor_health.py dependía 100% de backtest_results.json "
            "(necesita ≥6 días de signals_history.json — hoy hay 3) y "
            "devolvía siempre UNKNOWN/factor=1.00 mientras tanto, aunque "
            "predictor_validation.json YA tenía el resultado real (672 "
            "snapshots, accuracy 0.51, banda WARNING) desde el día anterior. "
            "Ahora cae a predictor_validation.json como fuente secundaria "
            "cuando no hay backtest real. Con los datos reales de hoy: "
            "health pasa de UNKNOWN a WARNING, confidence_factor de 1.00 a "
            "0.90 — el predictor pesa un poco menos en pred_confidence de "
            "todas las señales.",
            "Fix: data_validator.py — con datos perfectamente sanos, "
            "nivel/nivel_global NUNCA llegaba a 'OK', se quedaba en "
            "'WARNING' para siempre. Causa: los mensajes de confirmación "
            "('✅ Dato fresco', '✅ Consistencia OK', '✅ Integridad OK') "
            "vivían en la misma lista 'warnings' que las advertencias "
            "reales. Esto afectaba el banner del dashboard (🟡 todos los "
            "días, nunca 🟢) y, más importante, confidence_score.py: "
            "components['integridad_datos'] pegado en 50.0 (WARNING) en "
            "vez de 100.0 (OK) TODOS los días, incluso con datos perfectos "
            "— confirmado contra el snapshot real (system_confidence.json: "
            "integridad_datos=50.0). Con el fix, en un día sano el global "
            "score sube ~5 puntos (63.8 → ~68.8 con los demás componentes "
            "del último run real sin cambios). Los ✅ ahora van a una "
            "lista 'info' separada, nunca cuentan como warning.",
            "Feat: weight_optimizer.weights_provenance() — 'conciencia "
            "operativa' sobre los pesos V1: cuando optimized_weights.json "
            "viene 100% de replay sintético (n_real_entries=0, hoy el caso "
            "en los 3 mercados), se hace explícito en 3 lugares: el "
            "mensaje de Telegram (_synthetic_weights_section en "
            "notifier.py), health_metrics.json (monitor.py, vía "
            "/api/health), y el log del pipeline. Antes esta información "
            "existía en el JSON pero no llegaba a ningún lado visible.",
            "Tests: +49 tests (predictor_health + override: 16, "
            "weights_provenance: 12, data_validator — primera suite real "
            "de este módulo: 18, volatility_regime — primera suite real: "
            "24, consistencia del universo de 78 tickers: 9 — son 79, "
            "ajustado por solapamiento de fixtures compartidos). Suite "
            "completa: 370/370 verde.",
            "Confirmado: universo de tickers en código son 78 (22 MERVAL + "
            "25 BOVESPA + 31 SP500/CEDEARs/ETFs), no ~67-70 como "
            "documentaban v4.0-v7.0. El último run real (24/06) procesó "
            "67 — los 8 tickers nuevos (YPFD.BA, BBAR.BA, B3SA3.SA, "
            "EMBR3.SA, JBSS3.SA, ITSA4.SA, SANB11.SA, VIVT3.SA) aún no "
            "corrieron en producción. De paso: 11 tickers (6 nuevos + 5 "
            "preexistentes: MELI, RIO, PBR, QCOM, GLOB) no tienen datos "
            "fundamentales reales en ratios_consolidado_quant.csv — tarea "
            "de carga manual pendiente, no un bug de código. Documentado "
            "explícitamente en tests/test_ticker_universe_consistency.py "
            "para que un gap nuevo no se pierda en el ruido.",
        ],
    },
    {
        "version": "4.4",
        "date": "2026-06-25",
        "changes": [
            "Feat (Prioridad 5, roadmap externo): portfolio_optimizer.py ahora "
            "escala kelly_f/kelly_half por regime_factor de volatility_regime.py "
            "(LOW vol ×1.10, NORMAL ×1.00, HIGH vol ×0.75). Este módulo prometía "
            "la integración en su propio docstring desde que se creó pero nunca "
            "se conectó — verificado con grep en todo el repo, cero referencias "
            "a regime_factor en portfolio_optimizer.py antes de este fix. "
            "exit_model.py (stops) y confidence_score.py (componente de "
            "confianza) sí lo consumían; este era el consumidor faltante. "
            "El cap de riesgo de 20% por posición se aplica DESPUÉS del ajuste "
            "de régimen y nunca se relaja, ni en LOW vol. allocation_notes "
            "menciona explícitamente cuándo el régimen amplió/recortó el Kelly. "
            "Efecto en señales: kelly_f/kelly_half por ticker de compra cambia "
            "según el régimen de volatilidad sistémica del día; suggested_pct "
            "(asignación relativa entre tickers) NO cambia por diseño — "
            "regime_factor es un escalar uniforme que se cancela en la "
            "normalización del blend Kelly/RiskParity (ver docstring del "
            "módulo para el razonamiento completo).",
            "Refactor (Prioridad 4, roadmap externo): quality_check.py — cada "
            "check declara explícitamente 'categoria' (data|model|signal) y "
            "'es_estructural' (bool) en su propio dict, reemplazando la "
            "inferencia por nombre hardcodeado que causó el falso positivo del "
            "kill switch en 4.3. Comportamiento numérico verificado idéntico "
            "(criticas/criticas_estructurales sin cambios para los mismos "
            "inputs). No afecta señales/Kelly directamente — se incluye en "
            "este bump junto con el cambio de portfolio_optimizer.py de la "
            "misma sesión.",
            "Feat (Prioridad 1, roadmap externo): backtester.py — nuevos "
            "breakdowns by_consenso, consenso_vs_no, by_confidence_label, "
            "confidence_quantiles (top/bottom 20% real) y ranking_top_vs_rest. "
            "tracker.py ahora persiste confidence_score/confidence_label en "
            "signals_history.json (no se guardaban antes). No afecta señales/"
            "Kelly en vivo — es instrumentación de backtesting, incluida en "
            "este bump por completitud de la sesión.",
            "Feat (Prioridad 2, roadmap externo): nuevo kill_switch_log.py — "
            "bitácora histórica append-only del kill switch + evaluación "
            "retroactiva contra precios reales (forward return 5d del índice "
            "en días con kill switch activo vs baseline). No afecta señales/"
            "Kelly — instrumentación.",
            "Tests: +62 tests en total para la sesión completa (de 229 a "
            "291, incluyendo kill_switch_log que se shippeó en esta misma "
            "sesión sin bump de versión por ser pura instrumentación): "
            "kill_switch_log (16), backtester consenso/confianza (11), "
            "tracker confidence_score (2), quality_check — primera suite de "
            "este módulo (15), portfolio_optimizer — primera suite de este "
            "módulo (17), regresión de sync de arranque (1).",
        ],
    },
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
            "Fix CRÍTICO descubierto en la primera corrida real con esto activo "
            "(24/06, 14:07 UTC): el kill switch se activó en su primer run de "
            "producción por 8/67 tickers con V1 vs V2 en desacuerdo (ej. V1=COMPRA / "
            "V2=VENTA PARCIAL en JNJ, KO, GS, GE, UNH, BAC, JPM) — comportamiento "
            "ESPERADO del modelo (V1 y V2 miden cosas distintas a propósito), no un "
            "error de datos, y ninguno de los 8 tenía precio inválido ni otro problema "
            "estructural real. quality_check.py ahora separa "
            "resumen['criticas_estructurales'] (precio inválido / índice sin datos) de "
            "'criticas' (que sigue incluyendo V1vsV2 para el score ponderado, más "
            "permisivo). El trigger DURO del kill switch usa solo el subconjunto "
            "estructural; el desacuerdo V1/V2 sigue penalizando un poco el score "
            "ponderado pero ya no puede gatillar el freno de capital por sí solo.",
            "Tests: +8 archivos / +70 tests en total para la mejora 4.3 completa "
            "(de 99 a 169), incluyendo 3 específicos de regresión para el fix del "
            "kill switch arriba. Incluye además el fix de un test preexistente "
            "(test_weight_optimizer.py) con timestamp hardcodeado que ya había "
            "cruzado el umbral de staleness de 7 días.",
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
