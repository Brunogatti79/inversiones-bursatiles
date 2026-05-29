"""
src/pipeline.py — Fase 2
Orquestador del pipeline completo con análisis fundamental y macro real.
NUEVO: Validación de consistencia de datos antes de generar dashboard.
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
        logger.info(f"Macro scores: {macro_scores}")
        logger.info(f"Fundamental scores cargados: {len(fund_scores)} tickers") 
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
 
        # 5. CAMBIOS
        logger.info("5/8 Detectando cambios...")
        changes = detect_signal_changes(all_signals, f"{DATA_DIR}/signals_prev.json")
        save_signals(all_signals, f"{DATA_DIR}/signals_prev.json")
        history = update_history(all_signals)
        compute_accuracy(history)
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

        # 6c. DASHBOARD PUBLICO — copia identica sin datos de portfolio
        try:
            public_name = dashboard_name.replace(".html", "_publico.html")
            public_path = f"{OUTPUT_DIR}/{public_name}"
            with open(dashboard_path, "r", encoding="utf-8") as f:
                html_pub = f.read()
            # Solo vaciar los datos del portfolio (layout intacto)
            html_pub = html_pub.replace(
                "var PORTFOLIO = {",
                "var PORTFOLIO = {{/* datos privados */}} || {{"
            ).replace(
                "var PORTFOLIO = {",
                "var PORTFOLIO = {}"
            )
            with open(public_path, "w", encoding="utf-8") as f:
                f.write(html_pub)
            pub_ok = publish_dashboard(public_path, public_name)
            logger.info(f"Dashboard publico: {'OK' if pub_ok else 'error'} — {public_name}")
        except Exception as e:
            logger.warning(f"Error generando dashboard publico: {e}")

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
 
