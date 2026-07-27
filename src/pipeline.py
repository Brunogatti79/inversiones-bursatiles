"""
src/pipeline.py — Fase 2 + Fase 0 (Backtesting)
Orquestador del pipeline completo con análisis fundamental y macro real.
NUEVO: Validación de consistencia de datos antes de generar dashboard.
FASE 0: Backtesting automático post-run sobre historial de señales.
"""
 
import logging
import os
import re
import time
import json
from datetime import datetime
import pytz
 
from src.downloader     import download_all, save_csvs, load_ohlc_extra, MERVAL_TICKERS, BOVESPA_TICKERS, SP500_TICKERS
from src.analyzer       import (analyze_market, detect_signal_changes, save_signals, get_index_stats)
from src.macro_loader   import load_xlsx_signals
from src.macro_auto     import fetch_all_macro, get_cached_macro
from src.fundamental    import load_fundamental_scores
from src.data_validator import validar_todos
from src.notifier       import (send_daily_report, send_signal_change_alerts,
                                 send_excel, send_error_notification, publish_dashboard,
                                 publish_index_html)
from src.generator      import generate_dashboard, generate_excel
from src.tracker        import update_history, compute_accuracy
from src.backtester     import run_backtest
from src.cross_market   import compute_cross_market_context
from src.exit_model     import enrich_exit_levels
from src.weight_optimizer    import run_weight_optimization, load_optimized_weights, apply_optimized_weights
from src.monitor             import update_health_metrics, check_sla, persist_global_confidence
from src.kill_switch_log     import log_kill_switch_event, evaluate_kill_switch_history
from src.historical_replay    import run_historical_replay
from src.predictor_validation import run_predictor_validation
from src.volatility_regime   import compute_volatility_regime
from src.confidence_score    import enrich_confidence_scores, compute_global_confidence, apply_kill_switch, compute_exposure_factor, apply_exposure_factor
from src.quality_check       import validar_señales, inyectar_semaforo
from src.trailing_stop       import apply_trailing_stops
from src.predictor_health    import compute_predictor_health, apply_health_to_signals
from src.portfolio_optimizer import optimize_portfolio_allocation
 
logger = logging.getLogger(__name__)
 
TIMEZONE     = os.getenv("TIMEZONE", "America/Argentina/Buenos_Aires")
SEND_EXCEL   = os.getenv("SEND_EXCEL", "true").lower() == "true"
ALERT_CHANGE = os.getenv("SEND_ALERT_ON_CHANGE", "true").lower() == "true"
OUTPUT_DIR   = "outputs"
DATA_DIR     = "data"
 
 

def apply_prediction_override(signals: list[dict], predictor_health: dict = None) -> list[dict]:
    """
    Post-proceso: ajusta señales cuando predictor contradice V1.
    Principio: el predictor tiene la ultima palabra en timing de entrada.
    Reglas:
      - pred_21d < 0%  + COMPRA* → NEUTRAL  (no entrar si baja proyectada)
      - pred_21d < -5% + COMPRA* → VENTA PARCIAL (proyeccion muy negativa)
      - pred_21d <-10% + cualquier señal → VENTA  (baja fuerte proyectada)
      - ret_anual < -40% + COMPRA* → NEUTRAL  (caida estructural severa)

    Gating por salud del predictor (Prioridad 1, roadmap externo, 25/06/2026):
    esta función estuvo definida pero nunca invocada durante meses — el
    predictor pudo tener edge débil o nulo (ver predictor_validation.py:
    51% directional accuracy real, peor que el baseline histórico simple en
    MAE) sin que esto afectara nada porque la función jamás corría. Ahora
    que se activa, no tiene sentido dejarle el "última palabra" a un
    predictor que el propio sistema marca como DEGRADED (accuracy < 0.45)
    -- en ese caso se omite la Regla 1 (la más agresiva en volumen, dispara
    con solo pred_21d<0) y se deja intacta la Regla 2 (estructural, no
    depende del predictor). Con WARNING (0.45-0.54, que es el estado real
    de hoy) la Regla 1 sigue activa pero ya viene atenuada indirectamente:
    apply_health_to_signals() ya bajó pred_confidence por el mismo motivo.

    Gating adicional por mercado (auditoría 26/06/2026, fase 2): el breakdown
    de predictor_validation.json muestra que el predictor es directamente
    contraproducente en MERVAL específicamente — directional accuracy 45,6%
    (peor que monedazo) y correlación -0,111 (negativa: cuando predice más
    suba, en promedio pasa lo contrario) — mientras que en BOVESPA y SP500
    sí muestra accuracy >52% con correlación positiva débil. El gate global
    por health_status mezcla los 3 mercados y no captura esto: con
    WARNING global, la Regla 1 seguía aplicándose también a señales MERVAL,
    pese a que ahí el predictor empeora la decisión en vez de mejorarla. Se
    omite la Regla 1 para MERVAL independientemente del health_status
    global, hasta que predictor_validation muestre accuracy >50% con
    correlación no-negativa específicamente para ese mercado.
    """
    health_status = (predictor_health or {}).get("health", "UNKNOWN")
    skip_pred_rule = health_status == "DEGRADED"

    overrides = 0
    for s in signals:
        pred_21d  = s.get("pred_21d")
        ret_anual = s.get("ret_anual", 0) or 0
        signal    = s.get("signal", "")
        mercado   = s.get("mercado", "")
        is_buy    = "COMPRA" in signal

        reasons = []

        # Regla 1: prediccion negativa → no comprar
        # (omitida si el predictor está DEGRADED globalmente — ver docstring —
        #  o si el mercado es MERVAL, donde el predictor mide peor que
        #  monedazo con correlación negativa — ver docstring)
        skip_pred_rule_this_signal = skip_pred_rule or (mercado == "MERVAL")
        if pred_21d is not None and is_buy and not skip_pred_rule_this_signal:
            if pred_21d < -10:
                s["signal"] = "🔴 VENTA"
                reasons.append(f"Pred21d {pred_21d:.1f}% (BAJA FUERTE)")
            elif pred_21d < -5:
                s["signal"] = "🟠 VENTA PARCIAL"
                reasons.append(f"Pred21d {pred_21d:.1f}% (<-5%)")
            elif pred_21d < 0:
                s["signal"] = "🟡 NEUTRAL/ESPERAR"
                reasons.append(f"Pred21d {pred_21d:.1f}% (negativa)")

        # Regla 2: caida estructural anual severa (no depende del predictor,
        # nunca se omite)
        if ret_anual < -40 and "COMPRA" in s.get("signal", ""):
            s["signal"] = "🟡 NEUTRAL/ESPERAR"
            reasons.append(f"Ret.anual {ret_anual:.1f}% (<-40% estructural)")

        if reasons:
            s["signal_override"] = " | ".join(reasons)
            # Sincronizar signal_v2 si también era compra
            if "COMPRA" in s.get("signal_v2", ""):
                s["signal_v2"] = s["signal"]
            overrides += 1

    if skip_pred_rule:
        logger.info(
            f"Prediction override: Regla 1 OMITIDA (predictor_health=DEGRADED) | "
            f"{overrides} señales ajustadas solo por Regla 2 (estructural)"
        )
    else:
        logger.info(f"Prediction override: {overrides} señales ajustadas por predictor/tendencia "
                     f"(predictor_health={health_status})")
    return signals


def _weight_optimizer_esta_stale(opt_path: str = "data/optimized_weights.json", max_age_hours: float = 20) -> bool:
    """
    FIX 24/07/2026 (hallazgo real, no teórico): antes se medía la antigüedad
    con os.path.getmtime() -- la fecha de modificación del archivo EN EL
    DISCO de Railway. El filesystem de Railway es efímero (ver arquitectura
    v4 §3): en cada redeploy se re-sincroniza este archivo desde GitHub, lo
    que le da una fecha local fresca aunque el CONTENIDO tenga días. Con los
    redeploys frecuentes de una sesión de desarrollo activa, la antigüedad
    medida por mtime nunca superaba las 20hs y el optimizer quedó sin correr
    5 días a pesar de haber suficiente historia real para reoptimizar
    (optimized_weights.json decía "generated": 2026-07-19 mientras el mtime
    local decía 2026-07-22 -- 3 días de diferencia, todos los redeploys de
    por medio). Ahora se lee el timestamp real desde adentro del JSON -- ese
    sí sobrevive a los redeploys porque viene del contenido, no del
    filesystem. Si no se puede leer, cae al mtime como antes (mejor una
    señal imperfecta que ninguna).

    Devuelve True si corresponde re-optimizar (archivo viejo o inexistente).
    """
    if not os.path.exists(opt_path):
        return True
    try:
        with open(opt_path) as _owf:
            _generated_str = json.load(_owf).get("generated")
        if not _generated_str:
            return True  # sin timestamp adentro -> tratar como viejo, re-optimizar
        _generated_dt = datetime.fromisoformat(_generated_str)
        _age = (datetime.now() - _generated_dt).total_seconds()
    except Exception as _e_age:
        logger.warning(f"No se pudo leer 'generated' de {opt_path}, usando mtime como fallback: {_e_age}")
        _age = time.time() - os.path.getmtime(opt_path)
    return _age > 3600 * max_age_hours


def run_pipeline():
    tz       = pytz.timezone(TIMEZONE)
    start_ts = time.time()
    run_date = datetime.now(tz).strftime("%d/%m/%Y %H:%M")
    logger.info(f"Pipeline iniciado: {run_date}")
 
    try:
        # 1. DESCARGA
        logger.info("1/8 Descargando datos...")
        data = download_all(data_dir=DATA_DIR)
        save_csvs(data, DATA_DIR)
        merval_df  = data["merval"]
        bovespa_df = data["bovespa"]
        sp500_df   = data["sp500"]

        # Fix 27/07/2026 (roadmap externo P6): High/Low reales, cargados
        # APARTE del dict 'data' de siempre a propósito -- ver docstring de
        # downloader.load_ohlc_extra(). validar_todos()/compute_volatility_regime()/
        # weight_optimizer._build_price_index() más abajo siguen recibiendo
        # exactamente 'data' sin modificar. None por mercado si
        # scripts/download_data.py todavía no generó estos archivos (ej. el
        # workflow .yml no fue actualizado para commitearlos) -- analyze_market
        # cae al proxy close-only de siempre en ese caso, no rompe nada.
        ohlc_extra = load_ohlc_extra(data_dir=DATA_DIR)
 
        # 1b. VALIDACIÓN DE DATOS ─────────────────────────────────────────────
        logger.info("1b/8 Validando consistencia de datos...")
 
        def _idx_col(df, keyword):
            cols = [c for c in df.columns if keyword in c]
            return cols[0] if cols else ""
 
        index_cols = {
            "merval":  _idx_col(merval_df,  "MERVAL"),
            "bovespa": _idx_col(bovespa_df, "BOVESPA"),
            "sp500":   _idx_col(sp500_df,   "S&P"),
        }
        n_tickers = {
            "merval":  len(MERVAL_TICKERS),
            "bovespa": len(BOVESPA_TICKERS),
            "sp500":   len(SP500_TICKERS),
        }
        validacion = validar_todos(data, index_cols, n_tickers)
 
        # Log resultado
        nivel = validacion["nivel_global"]
        for key, res in validacion["mercados"].items():
            for msg in res["warnings"]: logger.info(msg)
            for msg in res["errors"]:   logger.error(msg)
 
        if nivel == "ERROR":
            logger.error(f"[VALIDACIÓN] ❌ Errores críticos de datos — pipeline continúa con advertencia")
        elif nivel == "WARNING":
            logger.warning(f"[VALIDACIÓN] ⚠️ Advertencias de datos detectadas")
        else:
            logger.info(f"[VALIDACIÓN] ✅ Todos los controles OK")
 
        # Guardar resultado validación
        os.makedirs(DATA_DIR, exist_ok=True)
        with open(f"{DATA_DIR}/validation_status.json", "w") as f:
            json.dump(validacion, f, ensure_ascii=False, indent=2, default=str)
        # ─────────────────────────────────────────────────────────────────────
 
        # 2. CARGAR MODELO MACRO + FUNDAMENTAL
        logger.info("2/8 Cargando modelo macro y fundamental...")
        # Intentar macro automático primero, fallback a xlsx
        macro_auto = None
        try:
            macro_auto = fetch_all_macro()
            if macro_auto and macro_auto.get("macro_scores"):
                xlsx_signals = load_xlsx_signals(f"{DATA_DIR}/modelo_macro_micro_señales.xlsx")
                xlsx_signals["macro_scores"] = macro_auto["macro_scores"]
                xlsx_signals["macro_confidence"] = macro_auto.get("macro_confidence", {})
                # Complemento nuevo (jul-2026, roadmap externo #5): cobertura real
                # de variables macro por mercado (ej. "9/9"), para poder auditar
                # después señal por señal con cuántos datos se calculó cada score
                # -- sin esto, dos corridas del mismo MODEL_VERSION con distinta
                # cobertura de datos (6/9 vs 9/9, como pasó con ARG este mes)
                # producen señales distintas sin forma de distinguirlas.
                xlsx_signals["variables_obtenidas"] = macro_auto.get("variables_obtenidas", {})
                logger.info(f"Macro AUTO: {macro_auto['macro_scores']}")
            else:
                raise ValueError("Macro auto sin datos")
        except Exception as e:
            logger.warning(f"Macro auto falló ({e}), usando xlsx/fallback")
            xlsx_signals = load_xlsx_signals(f"{DATA_DIR}/modelo_macro_micro_señales.xlsx")
        fund_scores  = load_fundamental_scores(f"{DATA_DIR}/ratios_consolidado_quant.csv")
        macro_scores = xlsx_signals.get("macro_scores", {})
        logger.info(f"Macro scores (pre-cross): {macro_scores}")
        logger.info(f"Fundamental scores cargados: {len(fund_scores)} tickers")

        # Mapeo ticker → col_nombre (usado por exit_model, backtester y cross_market)
        ticker_cols = {}
        ticker_cols.update(MERVAL_TICKERS)
        ticker_cols.update(BOVESPA_TICKERS)
        ticker_cols.update(SP500_TICKERS)

        # ── PESOS OPTIMIZADOS (Fase 2) ────────────────────────────────────────
        # Cargar pesos de la última optimización y aplicarlos al analyzer
        try:
            opt_weights = load_optimized_weights()
            if opt_weights:
                apply_optimized_weights(opt_weights)
                logger.info(f"Pesos optimizados aplicados: {list(opt_weights.keys())}")
            else:
                logger.info("Sin pesos optimizados aún — usando defaults del modelo")
        except Exception as e_ow:
            logger.warning(f"Pesos optimizados no críticos — continuando: {e_ow}")
        # ─────────────────────────────────────────────────────────────────────

        # ── GUARDAR CCL A GITHUB (para que persista entre redeploys) ───────────
        try:
            import json as _j2
            ccl_cache_path = "data/ccl_cache.json"
            if os.path.exists(ccl_cache_path):
                import requests as _req, base64 as _b64
                _tok = os.environ.get("GH_TOKEN","")
                if _tok:
                    with open(ccl_cache_path) as _f: _ccl_content = _f.read()
                    _url = "https://api.github.com/repos/Brunogatti79/inversiones-bursatiles/contents/data/ccl_cache.json"
                    _hdrs = {"Authorization": f"token {_tok}", "Content-Type": "application/json"}
                    _r_sha = _req.get(_url, headers=_hdrs, timeout=8)
                    _old_sha = _r_sha.json().get("sha","") if _r_sha.ok else ""
                    _payload = {"message": "auto: ccl_cache update", "content": _b64.b64encode(_ccl_content.encode()).decode()}
                    if _old_sha: _payload["sha"] = _old_sha
                    _req.put(_url, json=_payload, headers=_hdrs, timeout=10)
                    logger.info("CCL cache pusheado a GitHub")
        except Exception as e_ccl_push:
            logger.debug(f"CCL push no crítico: {e_ccl_push}")
        # ────────────────────────────────────────────────────────────────────────

        # ── CROSS-MARKET (Fase 1) ─────────────────────────────────────────────
        cross_market = {}
        try:
            cross_market = compute_cross_market_context(
                merval_df, bovespa_df, sp500_df, index_cols
            )
            # Aplicar ajuste al macro_score antes de analyze_market
            for mkt, adj in cross_market.get("score_adjustments", {}).items():
                if mkt in macro_scores and abs(adj) > 0.01:
                    old = macro_scores[mkt]
                    macro_scores[mkt] = round(max(0, min(100, old + adj)), 1)
                    logger.info(f"  Macro adj {mkt}: {old} → {macro_scores[mkt]} ({adj:+.2f} cross-market)")
            # Propagar macro_scores ajustados a xlsx_signals
            xlsx_signals["macro_scores"] = macro_scores
            logger.info(f"Cross-market: régimen={cross_market.get('regime')} | {cross_market.get('narrative','')[:80]}")
        except Exception as e_cm:
            logger.warning(f"Cross-market no crítico — continuando: {e_cm}")
        # ─────────────────────────────────────────────────────────────────────

        # ── VOLATILITY REGIME (Fase 5) ────────────────────────────────────────
        vol_regime = {}
        try:
            vol_regime = compute_volatility_regime(
                price_data={"merval": merval_df, "bovespa": bovespa_df, "sp500": sp500_df},
                index_cols=index_cols,
            )
            logger.info(
                f"Vol regime: {vol_regime.get('global_regime')} "
                f"(factor={vol_regime.get('regime_factor')}) "
                f"vol_score={vol_regime.get('global_vol_score')}"
            )
        except Exception as e_vr:
            logger.warning(f"Vol regime no crítico — continuando: {e_vr}")
        # ─────────────────────────────────────────────────────────────────────

        # 3. ANÁLISIS
        logger.info(f"Predictor ensemble: Random Forest {'ACTIVADO' if os.getenv('ENABLE_RF_PREDICTOR','false').lower()=='true' else 'DESACTIVADO'} (ENABLE_RF_PREDICTOR)")
        logger.info("3/8 Calculando señales...")

        signals_merval  = analyze_market(merval_df,  "MERVAL",  MERVAL_TICKERS,
                                         xlsx_signals=xlsx_signals, fund_scores=fund_scores,
                                         vol_regime=vol_regime, ohlc_extra=ohlc_extra.get("MERVAL"))
        signals_bovespa = analyze_market(bovespa_df, "BOVESPA", BOVESPA_TICKERS,
                                         xlsx_signals=xlsx_signals, fund_scores=fund_scores,
                                         vol_regime=vol_regime, ohlc_extra=ohlc_extra.get("BOVESPA"))
        signals_sp500   = analyze_market(sp500_df,   "SP500",   SP500_TICKERS,
                                         xlsx_signals=xlsx_signals, fund_scores=fund_scores,
                                         vol_regime=vol_regime, ohlc_extra=ohlc_extra.get("SP500"))
        all_signals = signals_merval + signals_bovespa + signals_sp500
        all_signals.sort(key=lambda x: x["score_final"], reverse=True)

        # ── Audit trail inmutable por señal (roadmap externo "Institucional
        # PRO", jul-2026): market_trends (regime detection por país, jul-2026)
        # se calculaba en cross_market.py pero nunca quedaba grabado en cada
        # señal individual -- solo se usaba de forma agregada. Sin esto, en 6
        # meses no hay forma de responder "¿el modelo funcionaba peor cuando
        # el mercado local estaba en tendencia bajista?" porque el contexto
        # de régimen en el momento de la señal no quedó registrado. Mismo
        # criterio que data_completeness/model_version: se graba en el
        # momento de emisión, no se puede reconstruir después.
        for _s in all_signals:
            _mt = cross_market.get("market_trends", {}).get(_s.get("mercado"), {})
            _s["market_trend"] = _mt.get("trend")
            _s["market_trend_score"] = _mt.get("score")
            _s["cross_market_regime"] = cross_market.get("regime", "NEUTRAL")
 
        # 4b. PREDICCIONES ENSEMBLE (5d / 10d / 21d)
        logger.info("4b/8 Generando predicciones ensemble...")
        try:
            from src.predictor import run_predictions
            predictor_context = {
                "sp500_trend_score":   cross_market.get("sp500_trend_score", 50),
                "macro_score":         macro_scores.get("SP500", 50),
                "vol_regime":          vol_regime.get("global_regime", "NORMAL"),
                "cross_market_regime": cross_market.get("regime", "NEUTRAL"),
            }
            all_signals = run_predictions(
                all_signals,
                {"merval": merval_df, "bovespa": bovespa_df, "sp500": sp500_df},
                ticker_cols=ticker_cols,
                context=predictor_context,
            )
        except Exception as e:
            logger.warning(f"Predicciones no disponibles: {e}")

        # ── PREDICTOR HEALTH (Fase 6) ─────────────────────────────────────────
        try:
            predictor_health = compute_predictor_health()
            all_signals = apply_health_to_signals(all_signals, predictor_health)
        except Exception as e_ph:
            logger.warning(f"Predictor health no crítico — continuando: {e_ph}")
            predictor_health = {}
        # ─────────────────────────────────────────────────────────────────────

        # ── PREDICTION OVERRIDE (Prioridad 1, roadmap externo, 25/06/2026) ────
        # apply_prediction_override() existía en este archivo desde hacía
        # tiempo (docstring completo, lógica de 4 reglas, hasta un log de
        # resumen) pero NUNCA se llamaba desde ningún lado — confirmado con
        # grep en todo el repo. El tooltip del dashboard (generator.py,
        # columna pred21) afirma "Si negativo + señal COMPRA → override
        # automático degrada la señal" desde siempre; eso era falso hasta
        # esta línea. Se activa AHORA, gateada por predictor_health: si el
        # predictor está DEGRADED, se omite por completo (pred_health_override
        # ya se seteaba en apply_health_to_signals pero nada lo leía — tampoco
        # esto). Corre ANTES del portfolio optimizer a propósito: si una señal
        # se degrada de COMPRA a NEUTRAL acá, el optimizer no debe asignarle
        # capital como si siguiera siendo COMPRA.
        try:
            all_signals = apply_prediction_override(all_signals, predictor_health)
        except Exception as e_apo:
            logger.warning(f"Prediction override no crítico — continuando: {e_apo}")
        # ─────────────────────────────────────────────────────────────────────

        # ── PORTFOLIO OPTIMIZER (Fase 4) ──────────────────────────────────────
        try:
            backtest_results = {}
            if os.path.exists("data/backtest_results.json"):
                import json as _j
                with open("data/backtest_results.json") as _f:
                    backtest_results = _j.load(_f)
            all_signals = optimize_portfolio_allocation(
                all_signals, backtest_results,
                price_data={"merval": merval_df, "bovespa": bovespa_df, "sp500": sp500_df},
                ticker_cols=ticker_cols,
                regime_factor=vol_regime.get("regime_factor", 1.0),
            )
        except Exception as e_po:
            logger.warning(f"Portfolio optimizer no crítico — continuando: {e_po}")
        # ─────────────────────────────────────────────────────────────────────

        # ── QUALITY CHECK (Nivel 1+2 — validación cruzada + semáforo) ──────────
        # NOTA: este módulo existía en el repo pero nunca se llamaba desde
        # ningún lado (verificado jun/2026) — el campo quality_flag que lee
        # confidence_score.py por señal siempre caía al default 🟢. Se activa
        # acá porque el confidence score GLOBAL (más abajo) depende de tener
        # un quality_resumen real, no del default.
        quality_resumen = {}
        try:
            quality = validar_señales(all_signals)
            all_signals = inyectar_semaforo(all_signals, quality)
            quality_resumen = quality.get("resumen", {})
            logger.info(
                f"Quality check: {quality_resumen.get('nivel_global','?')} "
                f"({quality_resumen.get('criticas',0)} críticas, "
                f"{quality_resumen.get('advertencias',0)} advertencias, "
                f"{quality_resumen.get('ok',0)} OK)"
            )
        except Exception as e_qc:
            logger.warning(f"Quality check no crítico — continuando: {e_qc}")
        # ─────────────────────────────────────────────────────────────────────

        # ── CONFIDENCE SCORE (Fase 5) ────────────────────────────────────────
        try:
            all_signals = enrich_confidence_scores(all_signals, vol_regime)
        except Exception as e_cs:
            logger.warning(f"Confidence score no crítico — continuando: {e_cs}")
        # ─────────────────────────────────────────────────────────────────────

        # ── CONFIDENCE SCORE GLOBAL + KILL SWITCH (mejora 3.1 + 3.5) ──────────
        # Circuit breaker de sistema: si la calidad de datos, la salud del
        # predictor, la confianza macro o la integridad de los CSVs caen lo
        # suficiente (o si quality_check encuentra demasiadas contradicciones
        # críticas), se frena la asignación de capital NUEVO en todas las
        # señales. No toca posiciones ya abiertas.
        global_conf = {}
        sla_status_run = "OK"
        try:
            sla_now = check_sla(send_telegram=False)
            sla_status_run = sla_now.get("status", "OK")
            global_conf = compute_global_confidence(
                n_signals=len(all_signals),
                quality_resumen=quality_resumen,
                predictor_health=predictor_health,
                macro_confidence=(macro_auto.get("macro_confidence", {}) if isinstance(macro_auto, dict) else {}),
                validacion_nivel=nivel,
                sla_status=sla_status_run,
            )
            all_signals = apply_kill_switch(all_signals, global_conf)
            persist_global_confidence(global_conf)
            logger.info(
                f"Confidence global: {global_conf['global_score']} ({global_conf['label']}) | "
                f"kill_switch={'ACTIVO 🔴' if global_conf['kill_switch_active'] else 'inactivo'}"
            )
        except Exception as e_gc:
            logger.warning(f"Confidence global / kill switch no crítico — continuando: {e_gc}")
        # ─────────────────────────────────────────────────────────────────────

        # ── HISTORIAL DEL KILL SWITCH (Prioridad 2, roadmap externo) ──────────
        # Bitácora append-only, separada del confidence global de arriba a
        # propósito: system_confidence.json se pisa en cada corrida, esto no.
        # Sin esto no hay manera de calibrar los umbrales (35 / 5 críticas)
        # con datos reales más adelante -- solo con "a ojo".
        try:
            log_kill_switch_event(
                global_conf,
                quality_resumen=quality_resumen,
                validacion_nivel=nivel,
                sla_status=sla_status_run,
                n_signals=len(all_signals),
            )
        except Exception as e_ksl:
            logger.warning(f"Kill switch log no crítico — continuando: {e_ksl}")
        # ─────────────────────────────────────────────────────────────────────

        # ── EXPOSURE TOTAL — ACTIVADO (26/06/2026, a pedido explícito de Bruno) ──
        # Generaliza el escalón binario del kill switch (score<35 -> 0,
        # score>=35 -> full) a una rampa continua 35-70, × regime_factor.
        # Reusa los cortes ya existentes (70/50/35) del label de confidence
        # global -- cero números nuevos sin validar. Ver docstring de
        # compute_exposure_factor()/apply_exposure_factor() en
        # confidence_score.py para la advertencia completa: portfolio
        # optimizer sigue corriendo con pesos V1 100% sintéticos hoy.
        exposure = {}
        try:
            exposure = compute_exposure_factor(global_conf, regime_factor=vol_regime.get("regime_factor", 1.0))
            all_signals = apply_exposure_factor(all_signals, exposure)
            logger.info(
                f"[exposure_total] ACTIVO factor={exposure['exposure_factor']} "
                f"= confidence({exposure['confidence_component']}) × regime({exposure['regime_component']})"
            )
        except Exception as e_exp:
            logger.warning(f"Exposure total no crítico — continuando: {e_exp}")
        # ─────────────────────────────────────────────────────────────────────

        # ── EXIT MODEL (Fase 1) ───────────────────────────────────────────────
        try:
            regime = cross_market.get("regime", "NEUTRAL")
            all_signals = enrich_exit_levels(
                all_signals,
                price_data={"merval": merval_df, "bovespa": bovespa_df, "sp500": sp500_df},
                ticker_cols=ticker_cols,
                regime=regime,
            )
        except Exception as e_em:
            logger.warning(f"Exit model no crítico — continuando: {e_em}")
        # ─────────────────────────────────────────────────────────────────────

        # 4. ESTADÍSTICAS DE ÍNDICES
        logger.info("4/8 Calculando estadísticas...")
        index_stats = {
            "merval":  get_index_stats(merval_df,  index_cols["merval"]  or ""),
            "bovespa": get_index_stats(bovespa_df, index_cols["bovespa"] or ""),
            "sp500":   get_index_stats(sp500_df,   index_cols["sp500"]   or ""),
        }
 
        empty_markets = [k for k, v in index_stats.items() if not v or v.get("actual", 0) == 0]
        if len(empty_markets) == 3:
            raise RuntimeError(f"index_stats vacío para los 3 mercados {empty_markets}.")
        if empty_markets:
            logger.warning(f"index_stats vacío para: {empty_markets}")
 
        # Agregar info de validación a index_stats para el dashboard
        for key in ["merval", "bovespa", "sp500"]:
            if key in index_stats and index_stats[key]:
                res = validacion["mercados"].get(key, {})
                index_stats[key]["data_nivel"]    = res.get("nivel", "OK")
                index_stats[key]["data_warnings"] = res.get("warnings", [])
                index_stats[key]["data_errors"]   = res.get("errors", [])
                index_stats[key]["ultima_fecha"]  = res.get("ultima_fecha", "—")

        # Agregar contexto cross-market a index_stats (disponible para generator/dashboard)
        if cross_market:
            index_stats["cross_market"] = cross_market
        if predictor_health:
            index_stats["predictor_health"] = predictor_health
 
        # 5. CAMBIOS
        logger.info("5/8 Detectando cambios...")
        changes = detect_signal_changes(all_signals, f"{DATA_DIR}/signals_prev.json")
        save_signals(all_signals, f"{DATA_DIR}/signals_prev.json")
        history = update_history(all_signals)
        compute_accuracy(history)

        # ── BACKTEST (Fase 0) ──────────────────────────────────────────────────
        try:
            bt_results = run_backtest(
                price_data={"merval": merval_df, "bovespa": bovespa_df, "sp500": sp500_df},
                ticker_cols=ticker_cols,
            )
            from src.model_version import log_model_run
            log_model_run(bt_results)
        except Exception as e_bt:
            logger.warning(f"Backtester no crítico — continuando: {e_bt}")
        # ──────────────────────────────────────────────────────────────────────

        # ── EFECTIVIDAD DE OPORTUNIDADES MOSTRADAS (registro + evaluación) ─────
        try:
            from src.opportunities_log import evaluate_opportunities
            evaluate_opportunities(
                price_data={"merval": merval_df, "bovespa": bovespa_df, "sp500": sp500_df},
                ticker_cols=ticker_cols,
            )
        except Exception as e_opl:
            logger.warning(f"Evaluación de opportunities_log no crítica — continuando: {e_opl}")
        # ──────────────────────────────────────────────────────────────────────

        # ── HISTORICAL REPLAY (Fase 5) ── alimenta weight optimizer ─────────
        try:
            run_historical_replay(
                price_data={"merval": merval_df, "bovespa": bovespa_df, "sp500": sp500_df},
                ticker_cols=ticker_cols,
                macro_scores=macro_scores,
                fund_scores=fund_scores,
            )
        except Exception as e_hr:
            logger.warning(f"Historical replay no crítico — continuando: {e_hr}")
        # ─────────────────────────────────────────────────────────────────────

        # ── PREDICTOR VALIDATION (Prioridad 3, roadmap externo) ──────────────
        # 1x/semana (mismo gate por contenido que historical_replay, no por
        # mtime). Corre el predictor real retroactivamente sobre los 12 meses
        # de precios y lo compara contra 3 baselines simples (zero/momentum/
        # promedio histórico) -- NOTA: la semana que le toca correr, esto
        # agrega ~5 minutos a esa corrida puntual del pipeline (ensemble
        # completo × ~67 tickers × ~6-7 snapshots cada uno); el resto de las
        # semanas no hace nada porque el contenido sigue fresco.
        try:
            run_predictor_validation(
                price_data={"merval": merval_df, "bovespa": bovespa_df, "sp500": sp500_df},
                ticker_cols=ticker_cols,
            )
        except Exception as e_pv:
            logger.warning(f"Predictor validation no crítico — continuando: {e_pv}")
        # ─────────────────────────────────────────────────────────────────────

        # ── VALIDACIÓN RETROACTIVA DEL KILL SWITCH (Prioridad 2, roadmap externo) ─
        # 1x/semana (mismo gate por contenido que predictor_validation, arriba).
        # Mide si los días con kill switch activo estuvieron seguidos de una
        # baja del índice (kill switch "ayudó") vs. los días sin kill switch
        # activo, como baseline de contraste. Mientras haya pocas corridas
        # reales con kill switch activo (hoy: 0 casos válidos, el único caso
        # real fue un falso positivo ya corregido) esto va a devolver muestras
        # chicas a propósito -- es preferible ver "n=0" que inventar una
        # conclusión.
        try:
            evaluate_kill_switch_history(
                price_data={"merval": merval_df, "bovespa": bovespa_df, "sp500": sp500_df},
                index_cols=index_cols,
            )
        except Exception as e_ksv:
            logger.warning(f"Kill switch validation no crítica — continuando: {e_ksv}")
        # ─────────────────────────────────────────────────────────────────────

        # ── WEIGHT OPTIMIZER (Fase 2) ──────────────────────────────────────────
        # Corre solo 1 vez por día (la primera ejecución). Las siguientes usan
        # los pesos guardados en data/optimized_weights.json. Ver docstring de
        # _weight_optimizer_esta_stale() para el fix del 24/07/2026 (bug real
        # de mtime vs filesystem efímero de Railway).
        try:
            _opt_path = "data/optimized_weights.json"
            if _weight_optimizer_esta_stale(_opt_path):
                run_weight_optimization(
                    price_data={"merval": merval_df, "bovespa": bovespa_df, "sp500": sp500_df},
                    ticker_cols=ticker_cols,
                )
        except Exception as e_wo:
            logger.warning(f"Weight optimizer no crítico — continuando: {e_wo}")
        # ──────────────────────────────────────────────────────────────────────

        from src.tracker import check_portfolio_alerts, update_portfolio_usd, log_portfolio_value_history
        portfolio_alerts = check_portfolio_alerts(all_signals)
        # Actualizar precios USD del portfolio con precios vigentes + CCL
        try:
            # Obtener BRL/USD desde macro_auto que ya lo calculó
            _brl_usd = macro_auto.get("detalles", {}).get("BRA", {}).get("brl_usd", {}).get("valor", 0) if isinstance(macro_auto, dict) else 0
            if not _brl_usd or float(_brl_usd) < 3:
                _brl_usd = 0
            logger.info(f"BRL/USD pasado a portfolio: {_brl_usd}")
            update_portfolio_usd(all_signals, brl_usd_ext=float(_brl_usd) if _brl_usd else 0.0)
            logger.info("Portfolio USD actualizado correctamente")
            # Logging diario de valor total (roadmap externo #8): base para
            # un drawdown real del portfolio en el futuro -- ver docstring
            # de log_portfolio_value_history() en tracker.py.
            try:
                log_portfolio_value_history()
            except Exception as e_pvh:
                logger.warning(f"log_portfolio_value_history falló (no crítico): {e_pvh}")

            # Portfolio Risk Engine (roadmap "Institucional PRO", 24/07/2026):
            # correlaciones + VaR paramétrico + exposición por mercado/sector
            # del portfolio actual. VaR paramétrico (no histórico) a
            # propósito -- portfolio_value_history.json recién empieza a
            # acumularse, no alcanza todavía para un VaR histórico confiable.
            # Ver docstring de src/portfolio_risk.py para el detalle completo.
            try:
                from src.portfolio_risk import compute_portfolio_risk
                from src.analyzer import SECTOR_MAP
                from src.github_persistence import load_json as _pr_load, save_json as _pr_save
                with open("data/portfolio.json") as _pf:
                    _portfolio_fresh = json.load(_pf)
                _risk = compute_portfolio_risk(
                    _portfolio_fresh,
                    {"merval": merval_df, "bovespa": bovespa_df, "sp500": sp500_df},
                    ticker_cols,
                    sector_by_ticker=SECTOR_MAP,
                )
                _risk["generated"] = datetime.now().isoformat()
                _pr_save("data/portfolio_risk.json", _risk, message=f"auto: portfolio_risk {datetime.now().strftime('%Y-%m-%d')}")
                logger.info(
                    f"Portfolio Risk: VaR95%1d={_risk['var_parametrico'].get('var_pct')}% "
                    f"| correlaciones status={_risk['correlaciones'].get('status')}"
                )
            except Exception as e_risk:
                logger.warning(f"Portfolio Risk Engine falló (no crítico): {e_risk}")
        except Exception as e:
            import traceback
            logger.error(f"update_portfolio_usd falló: {e}\n{traceback.format_exc()}")
        # ── TRAILING STOPS (Fase 6) ───────────────────────────────────────────
        try:
            trail_events = apply_trailing_stops(all_signals)
            if trail_events:
                logger.info(f"Trailing stops: {len(trail_events)} posiciones ajustadas")
                # Enviar notificación Telegram si hay ajustes
                for ev in trail_events:
                    try:
                        from src.notifier import send_telegram
                        msg = (
                            f"📈 Trailing Stop — {ev['ticker']}"
                            + chr(10) + f"Nivel: {ev['nivel']} (R={ev['unrealized_R']:.1f}x)"
                            + chr(10) + f"Stop: {ev['stop_anterior']:.4f} → {ev['stop_nuevo']:.4f} USD"
                        )
                        send_telegram(msg)
                    except Exception:
                        pass
        except Exception as e_ts:
            logger.warning(f"Trailing stop no crítico — continuando: {e_ts}")
        # ───────────────────────────────────────────────────────────────────── 
        if portfolio_alerts:
            from src.notifier import send_portfolio_alerts
            criticas = [a for a in portfolio_alerts if a.get("tipo") not in ("📊 P&L",)]
            if criticas:
                send_portfolio_alerts(portfolio_alerts)
        # 6. DASHBOARD
        logger.info("6/8 Generando dashboard...")

        # Procedencia de los pesos V1 (Prioridad 1, roadmap externo, 25/06/2026)
        # -- se mueve acá (antes vivía en la sección de Telegram) para que el
        # dashboard también pueda mostrarlo, no solo el mensaje de Telegram.
        try:
            from src.weight_optimizer import weights_provenance
            wp = weights_provenance()
            if wp.get("is_synthetic"):
                logger.warning(
                    f"⚠️ Pesos V1 sintéticos: modo={wp.get('mode')} "
                    f"días_historia={wp.get('days_history')} — "
                    f"mercados 100% replay: "
                    f"{[m for m, v in wp.get('markets', {}).items() if v.get('is_synthetic')]}"
                )
        except Exception as e_wp:
            logger.warning(f"weights_provenance no crítico — continuando: {e_wp}")
            wp = {}

        os.makedirs(OUTPUT_DIR, exist_ok=True)
        dashboard_name = datetime.now(tz).strftime("informe_inversiones_%m%Y.html")
        dashboard_path = f"{OUTPUT_DIR}/{dashboard_name}"
        generate_dashboard(
            signals=all_signals,
            index_stats=index_stats,
            output_path=dashboard_path,
            run_date=run_date,
            price_data={"merval": merval_df, "bovespa": bovespa_df, "sp500": sp500_df},
            validacion=validacion,
            weights_provenance=wp,
            exposure=exposure,
        )
        logger.info(f"Dashboard generado: {dashboard_path}")

        # 6b. INDEX.HTML + PUBLICAR EN GITHUB PAGES
        index_path = f"{OUTPUT_DIR}/index.html"
        with open(index_path, 'w') as f:
            f.write(f'<!DOCTYPE html><html><head><meta http-equiv="refresh" content="0;url={dashboard_name}"></head><body></body></html>')
        logger.info(f"index.html generado -> {dashboard_name}")

        logger.info("6b/8 Publicando en GitHub Pages...")
        published = publish_dashboard(dashboard_path, dashboard_name)
        if published:
            publish_index_html(dashboard_name)
            logger.info("Dashboard + index.html publicados en GitHub Pages")
        else:
            logger.warning("No se pudo publicar en GitHub Pages (revisar GH_TOKEN)")

        # 6c. DASHBOARD PUBLICO — identico al privado con portfolio vaciado
        try:
            public_name = dashboard_name.replace(".html", "_publico.html")
            public_path = f"{OUTPUT_DIR}/{public_name}"
            with open(dashboard_path, "r", encoding="utf-8") as pf:
                html_pub = pf.read()
            # Vaciar datos privados del portfolio manteniendo layout
            marker_start = "var PORTFOLIO = {"
            marker_end   = "var PORTFOLIO_ALERTS ="
            if marker_start in html_pub and marker_end in html_pub:
                idx_s = html_pub.index(marker_start)
                idx_e = html_pub.index(marker_end)
                html_pub = html_pub[:idx_s] + "var PORTFOLIO = {};" + "\n" + html_pub[idx_e:]
            with open(public_path, "w", encoding="utf-8") as pf:
                f_write = pf.write(html_pub)
            pub_ok = publish_dashboard(public_path, public_name)
            logger.info(f"Dashboard publico publicado: {public_name} ok={pub_ok}")
        except Exception as e_pub:
            logger.warning(f"Error dashboard publico: {e_pub}")

        # 7. EXCEL
        excel_path = None
        if SEND_EXCEL:
            logger.info("7/8 Generando Excel...")
            excel_name = datetime.now(tz).strftime("fichas_inversion_%m%Y.xlsx")
            excel_path = f"{OUTPUT_DIR}/{excel_name}"
            generate_excel(all_signals, index_stats, excel_path)
 
        # 8. TELEGRAM
        logger.info("8/8 Enviando Telegram...")
        if ALERT_CHANGE and changes:
            send_signal_change_alerts(changes)

        send_daily_report(
            all_signals=all_signals,
            index_stats=index_stats,
            dashboard_filename=dashboard_name,
            run_date=run_date,
            validacion=validacion,
            weights_provenance=wp,
            exposure=exposure,
        )
        if SEND_EXCEL and excel_path and os.path.exists(excel_path):
            send_excel(excel_path)
 
        duration = time.time() - start_ts
        _save_status(run_date=run_date, success=True, duration=duration, tz=tz,
                     validacion_nivel=nivel)
        # ── MONITOR (Fase 3) ──────────────────────────────────────────────
        try:
            update_health_metrics({
                "success":          True,
                "duration_sec":     duration,
                "all_signals":      all_signals,
                "cross_market":     cross_market,
                "validacion_nivel": nivel,
                "run_date":         run_date,
                "global_confidence": global_conf,
                "exposure_shadow":  exposure,
            })
        except Exception as e_mon:
            logger.warning(f"Monitor no crítico — continuando: {e_mon}")
        # ─────────────────────────────────────────────────────────────────
        logger.info(f"Pipeline completado en {duration:.1f}s — Validación: {nivel}")

    except Exception as e:
        duration = time.time() - start_ts
        logger.error(f"Pipeline ERROR: {e}", exc_info=True)
        _save_status(run_date=run_date, success=False, duration=duration, error=str(e), tz=tz)
        try:
            update_health_metrics({
                "success": False, "duration_sec": duration,
                "all_signals": [], "cross_market": {}, "validacion_nivel": "ERROR",
            })
        except Exception:
            pass
        send_error_notification(str(e))
        raise
 
 
def _save_status(run_date, success, duration, tz, error="", validacion_nivel="—"):
    run_time = os.getenv("RUN_TIME_UTC", "20:30")
    status = {
        "last_run":          run_date,
        "success":           success,
        "duration_sec":      round(duration, 1),
        "error":             error,
        "validacion_nivel":  validacion_nivel,
        "next_run":          f"Mañana a las {run_time} UTC",
    }
    os.makedirs(DATA_DIR, exist_ok=True)
    with open(f"{DATA_DIR}/last_run_status.json", "w") as f:
        json.dump(status, f, ensure_ascii=False, indent=2)
 
