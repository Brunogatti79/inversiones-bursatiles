"""
src/pipeline.py
Orquestador del pipeline completo.

Flujo:
  1. Descargar precios (Yahoo Finance)
  2. Guardar CSVs
  3. Calcular señales y rankings
  4. Detectar cambios de señal
  5. Generar dashboard HTML
  6. Generar fichas Excel
  7. Notificar por Telegram
  8. Guardar estado de la ejecución
"""

import logging
import os
import time
import json
from datetime import datetime
import pytz

from src.downloader import download_all, save_csvs, MERVAL_TICKERS, BOVESPA_TICKERS, SP500_TICKERS
from src.analyzer   import (analyze_market, detect_signal_changes, save_signals,
                             get_index_stats)
from src.notifier   import (send_daily_report, send_signal_change_alerts,
                             send_excel, send_error_notification)
from src.generator  import generate_dashboard, generate_excel

logger = logging.getLogger(__name__)

TIMEZONE     = os.getenv("TIMEZONE", "America/Argentina/Buenos_Aires")
SEND_EXCEL   = os.getenv("SEND_EXCEL", "true").lower() == "true"
ALERT_CHANGE = os.getenv("SEND_ALERT_ON_CHANGE", "true").lower() == "true"
OUTPUT_DIR   = "outputs"
DATA_DIR     = "data"


def run_pipeline():
    """Ejecuta el pipeline completo. Llamado por el scheduler o por /run."""
    tz       = pytz.timezone(TIMEZONE)
    start_ts = time.time()
    run_date = datetime.now(tz).strftime("%d/%m/%Y %H:%M")
    logger.info(f"═══ Pipeline iniciado: {run_date} ═══")

    try:
        # ── 1. DESCARGA ──────────────────────────────────────────────
        logger.info("1/7 Descargando datos de Yahoo Finance...")
        data = download_all()
        save_csvs(data, DATA_DIR)

        merval_df  = data["merval"]
        bovespa_df = data["bovespa"]
        sp500_df   = data["sp500"]

        # ── 2. ANÁLISIS ──────────────────────────────────────────────
        logger.info("2/7 Calculando señales del modelo...")
        signals_merval  = analyze_market(merval_df,  "MERVAL",  MERVAL_TICKERS)  if merval_df is not None and not merval_df.empty else []
        signals_bovespa = analyze_market(bovespa_df, "BOVESPA", BOVESPA_TICKERS) if bovespa_df is not None and not bovespa_df.empty else []
        signals_sp500   = analyze_market(sp500_df,   "SP500",   SP500_TICKERS)   if sp500_df is not None and not sp500_df.empty else []
        all_signals     = signals_merval + signals_bovespa + signals_sp500
        all_signals.sort(key=lambda x: x["score_final"], reverse=True)

        # ── 3. ESTADÍSTICAS DE ÍNDICES ───────────────────────────────
        logger.info("3/7 Calculando estadísticas de índices...")

        def _idx_col(df, keyword):
            cols = [c for c in df.columns if keyword in c]
            return cols[0] if cols else None

        def _safe_stats(df, keyword):
            if df is None or df.empty:
                return {}
            col = _idx_col(df, keyword)
            if not col:
                return {}
            try:
                return get_index_stats(df, col)
            except Exception as e:
                logger.warning(f"Error en stats {keyword}: {e}")
                return {}

        index_stats = {
            "merval":  _safe_stats(merval_df,  "MERVAL"),
            "bovespa": _safe_stats(bovespa_df, "BOVESPA"),
            "sp500":   _safe_stats(sp500_df,   "S&P"),
        }

        # ── 4. DETECCIÓN DE CAMBIOS ──────────────────────────────────
        logger.info("4/7 Detectando cambios de señal...")
        changes = detect_signal_changes(all_signals, f"{DATA_DIR}/signals_prev.json")
        save_signals(all_signals, f"{DATA_DIR}/signals_prev.json")

        # ── 5. DASHBOARD HTML ────────────────────────────────────────
        logger.info("5/7 Generando dashboard HTML...")
        os.makedirs(OUTPUT_DIR, exist_ok=True)
        dashboard_name = datetime.now(tz).strftime("informe_inversiones_%m%Y.html")
        dashboard_path = f"{OUTPUT_DIR}/{dashboard_name}"
        generate_dashboard(
            signals=all_signals,
            index_stats=index_stats,
            output_path=dashboard_path,
            run_date=run_date,
        )
        logger.info(f"Dashboard generado: {dashboard_path}")

        # ── 6. EXCEL (OPCIONAL) ──────────────────────────────────────
        excel_path = None
        if SEND_EXCEL:
            logger.info("6/7 Generando fichas Excel...")
            excel_name = datetime.now(tz).strftime("fichas_inversion_%m%Y.xlsx")
            excel_path = f"{OUTPUT_DIR}/{excel_name}"
            generate_excel(all_signals, index_stats, excel_path)

        # ── 7. NOTIFICACIONES TELEGRAM ───────────────────────────────
        logger.info("7/7 Enviando notificaciones a Telegram...")

        # a) Alertas de cambio de señal (van PRIMERO, son urgentes)
        if ALERT_CHANGE and changes:
            send_signal_change_alerts(changes)

        # b) Informe diario
        send_daily_report(
            all_signals=all_signals,
            index_stats=index_stats,
            dashboard_filename=dashboard_name,
            run_date=run_date,
        )

        # c) Archivo Excel adjunto
        if SEND_EXCEL and excel_path and os.path.exists(excel_path):
            send_excel(excel_path)

        # ── 8. GUARDAR ESTADO ────────────────────────────────────────
        duration = time.time() - start_ts
        _save_status(run_date=run_date, success=True, duration=duration, tz=tz)
        logger.info(f"═══ Pipeline completado en {duration:.1f}s ═══")

    except Exception as e:
        duration = time.time() - start_ts
        logger.error(f"Pipeline ERROR: {e}", exc_info=True)
        _save_status(run_date=run_date, success=False, duration=duration, error=str(e), tz=tz)
        send_error_notification(str(e))
        raise


def _save_status(run_date, success, duration, tz, error=""):
    """Persiste el estado de la última ejecución para /status."""
    from apscheduler.triggers.cron import CronTrigger
    run_time = os.getenv("RUN_TIME_UTC", "21:30")
    h, m = run_time.split(":")
    status = {
        "last_run":    run_date,
        "success":     success,
        "duration_sec": round(duration, 1),
        "error":       error,
        "next_run":    f"Mañana a las {run_time} UTC",
    }
    os.makedirs(DATA_DIR, exist_ok=True)
    with open(f"{DATA_DIR}/last_run_status.json", "w") as f:
        json.dump(status, f, ensure_ascii=False, indent=2)
