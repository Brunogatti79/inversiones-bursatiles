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
 
from src.downloader     import download_all, save_csvs, MERVAL_TICKERS, BOVESPA_TICKERS, SP500_TICKERS
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
from src.optimizer      import run_optimization, load_optimized_weights, apply_optimized_weights
 
logger = logging.getLogger(__name__)
 
TIMEZONE     = os.getenv("TIMEZONE", "America/Argentina/Buenos_Aires")
SEND_EXCEL   = os.getenv("SEND_EXCEL", "true").lower() == "true"
ALERT_CHANGE = os.getenv("SEND_ALERT_ON_CHANGE", "true").lower() == "true"
OUTPUT_DIR   = "outputs"
DATA_DIR     = "data"
 
 

def apply_prediction_override(signals: list[dict]) -> list[dict]:
    """
    Post-proceso: ajusta señales cuando predictor contradice V1.
    Principio: el predictor tiene la ultima palabra en timing de entrada.
    Reglas:
      - pred_21d < 0%  + COMPRA* → NEUTRAL  (no entrar si baja proyectada)
      - pred_21d < -5% + COMPRA* → VENTA PARCIAL (proyeccion muy negativa)
      - pred_21d <-10% + cualquier señal → VENTA  (baja fuerte proyectada)
      - ret_anual < -40% + COMPRA* → NEUTRAL  (caida estructural severa)
    """
    overrides = 0
    for s in signals:
        pred_21d  = s.get("pred_21d")
        ret_anual = s.get("ret_anual", 0) or 0
        signal    = s.get("signal", "")
        is_buy    = "COMPRA" in signal

        reasons = []

        # Regla 1: prediccion negativa → no comprar
        if pred_21d is not None and is_buy:
            if pred_21d < -10:
                s["signal"] = "🔴 VENTA"
                reasons.append(f"Pred21d {pred_21d:.1f}% (BAJA FUERTE)")
            elif pred_21d < -5:
                s["signal"] = "🟠 VENTA PARCIAL"
                reasons.append(f"Pred21d {pred_21d:.1f}% (<-5%)")
            elif pred_21d < 0:
                s["signal"] = "🟡 NEUTRAL/ESPERAR"
                reasons.append(f"Pred21d {pred_21d:.1f}% (negativa)")

        # Regla 2: caida estructural anual severa
        if ret_anual < -40 and "COMPRA" in s.get("signal", ""):
            s["signal"] = "🟡 NEUTRAL/ESPERAR"
            reasons.append(f"Ret.anual {ret_anual:.1f}% (<-40% estructural)")

        if reasons:
            s["signal_override"] = " | ".join(reasons)
            # Sincronizar signal_v2 si también era compra
            if "COMPRA" in s.get("signal_v2", ""):
                s["signal_v2"] = s["signal"]
            overrides += 1

    logger.info(f"Prediction override: {overrides} señales ajustadas por predictor/tendencia")
    return signals


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
        try:
            macro_auto = fetch_all_macro()
            if macro_auto and macro_auto.get("macro_scores"):
                xlsx_signals = load_xlsx_signals(f"{DATA_DIR}/modelo_macro_micro_señales.xlsx")
                xlsx_signals["macro_scores"] = macro_auto["macro_scores"]
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

        # 3. ANÁLISIS
        logger.info("3/8 Calculando señales...")

        signals_merval  = analyze_market(merval_df,  "MERVAL",  MERVAL_TICKERS,
                                         xlsx_signals=xlsx_signals, fund_scores=fund_scores)
        signals_bovespa = analyze_market(bovespa_df, "BOVESPA", BOVESPA_TICKERS,
                                         xlsx_signals=xlsx_signals, fund_scores=fund_scores)
        signals_sp500   = analyze_market(sp500_df,   "SP500",   SP500_TICKERS,
                                         xlsx_signals=xlsx_signals, fund_scores=fund_scores)
        all_signals = signals_merval + signals_bovespa + signals_sp500
        all_signals.sort(key=lambda x: x["score_final"], reverse=True)
 
        # 4b. PREDICCIONES ENSEMBLE (5d / 10d / 21d)
        logger.info("4b/8 Generando predicciones ensemble...")
        try:
            from src.predictor import run_predictions
            all_signals = run_predictions(
                all_signals,
                {"merval": merval_df, "bovespa": bovespa_df, "sp500": sp500_df}
            )
        except Exception as e:
            logger.warning(f"Predicciones no disponibles: {e}")

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
 
        # 5. CAMBIOS
        logger.info("5/8 Detectando cambios...")
        changes = detect_signal_changes(all_signals, f"{DATA_DIR}/signals_prev.json")
        save_signals(all_signals, f"{DATA_DIR}/signals_prev.json")
        history = update_history(all_signals)
        compute_accuracy(history)

        # ── BACKTEST (Fase 0) ──────────────────────────────────────────────────
        try:
            run_backtest(
                price_data={"merval": merval_df, "bovespa": bovespa_df, "sp500": sp500_df},
                ticker_cols=ticker_cols,
            )
        except Exception as e_bt:
            logger.warning(f"Backtester no crítico — continuando: {e_bt}")
        # ──────────────────────────────────────────────────────────────────────

        # ── OPTIMIZER (Fase 2) ─────────────────────────────────────────────────
        # Corre solo cada OPTIMIZE_EVERY_DAYS días para no ralentizar el pipeline
        try:
            run_optimization(
                price_data={"merval": merval_df, "bovespa": bovespa_df, "sp500": sp500_df},
                ticker_cols=ticker_cols,
                xlsx_signals=xlsx_signals,
                fund_scores=fund_scores,
            )
        except Exception as e_opt:
            logger.warning(f"Optimizer no crítico — continuando: {e_opt}")
        # ──────────────────────────────────────────────────────────────────────

        from src.tracker import check_portfolio_alerts, update_portfolio_usd
        portfolio_alerts = check_portfolio_alerts(all_signals)
        # Actualizar precios USD del portfolio con precios vigentes + CCL
        try:
            update_portfolio_usd(all_signals)
        except Exception as e:
            logger.warning(f"update_portfolio_usd falló: {e}") 
        if portfolio_alerts:
            from src.notifier import send_portfolio_alerts
            criticas = [a for a in portfolio_alerts if a.get("tipo") not in ("📊 P&L",)]
            if criticas:
                send_portfolio_alerts(portfolio_alerts)
        # 6. DASHBOARD
        logger.info("6/8 Generando dashboard...")
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
        )
        if SEND_EXCEL and excel_path and os.path.exists(excel_path):
            send_excel(excel_path)
 
        duration = time.time() - start_ts
        _save_status(run_date=run_date, success=True, duration=duration, tz=tz,
                     validacion_nivel=nivel)
        logger.info(f"Pipeline completado en {duration:.1f}s — Validación: {nivel}")
 
    except Exception as e:
        duration = time.time() - start_ts
        logger.error(f"Pipeline ERROR: {e}", exc_info=True)
        _save_status(run_date=run_date, success=False, duration=duration, error=str(e), tz=tz)
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
 
