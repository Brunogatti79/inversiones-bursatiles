"""
src/monitor.py — Fase 3: Monitoreo del Sistema

Genera y persiste métricas de salud del pipeline.
SLA check: alerta si el pipeline lleva >N horas sin correr.

MÉTRICAS QUE RASTREA (data/health_metrics.json):
  • last_run / last_success / last_failure
  • duration_sec (últimas 10 ejecuciones) → avg / p95
  • signals_count / buy_signals / sell_signals
  • cross_market_regime
  • backtest_ev_21d (de la última optimización)
  • predictor_accuracy
  • optimized_weights_age_h
  • pipeline_runs_today / pipeline_runs_week
  • sla_status: OK | WARNING | CRITICAL
  • data_freshness: age en horas del CSV más reciente

USO desde pipeline.py:
    from src.monitor import update_health_metrics, check_sla
    update_health_metrics(run_context)
    check_sla()   # envía alerta Telegram si pipeline muerto
"""

import json
import os
import logging
import time
from datetime import datetime, timedelta

logger = logging.getLogger(__name__)

HEALTH_PATH   = "data/health_metrics.json"
STATUS_PATH   = "data/last_run_status.json"
BACKTEST_PATH = "data/backtest_results.json"
WEIGHTS_PATH  = "data/optimized_weights.json"
CSV_PATHS     = [
    "data/merval_cierres.csv",
    "data/bovespa_cierres.csv",
    "data/sp500_cierres.csv",
]

# SLA: alertar si no corrió en este tiempo
SLA_WARN_HOURS     = 8    # warning si >8h sin run exitoso
SLA_CRITICAL_HOURS = 14   # crítico si >14h (saltó 2 ventanas + margen)
MAX_DURATION_HIST  = 10   # cantidad de duraciones a conservar


# ── Entrypoint desde pipeline ───────────────────────────────────────────────

def update_health_metrics(run_context: dict) -> dict:
    """
    Actualiza data/health_metrics.json con datos del run actual.

    run_context: {
        "success": bool,
        "duration_sec": float,
        "all_signals": list,
        "cross_market": dict,
        "validacion_nivel": str,
        "run_date": str,
    }
    """
    health = _load_health()

    now_iso = datetime.now().isoformat()
    success  = run_context.get("success", False)
    duration = run_context.get("duration_sec", 0)

    # Timestamps
    health["last_run"]     = now_iso
    health["last_run_utc"] = datetime.utcnow().isoformat()
    if success:
        health["last_success"] = now_iso
        health["consecutive_failures"] = 0
    else:
        health["last_failure"] = now_iso
        health["consecutive_failures"] = health.get("consecutive_failures", 0) + 1

    # Historial de duraciones
    durations = health.get("duration_history", [])
    durations.append(round(duration, 1))
    health["duration_history"] = durations[-MAX_DURATION_HIST:]
    if durations:
        import numpy as np
        health["duration_avg_sec"]  = round(float(np.mean(durations)), 1)
        health["duration_p95_sec"]  = round(float(np.percentile(durations, 95)), 1)
        health["duration_last_sec"] = round(duration, 1)

    # Señales
    signals = run_context.get("all_signals", [])
    if signals:
        health["signals_count"]   = len(signals)
        health["buy_signals"]     = len([s for s in signals if "COMPRA" in s.get("signal", "")])
        health["sell_signals"]    = len([s for s in signals if "VENTA" in s.get("signal", "")])
        health["neutral_signals"] = len([s for s in signals if "NEUTRAL" in s.get("signal", "")])

    # Cross-market
    cross = run_context.get("cross_market", {})
    if cross:
        health["cross_market_regime"]     = cross.get("regime", "")
        health["cross_market_sp500_trend"] = cross.get("sp500_trend", "")
        health["cross_market_narrative"]  = cross.get("narrative", "")[:120]

    # Validación de datos
    health["validacion_nivel"] = run_context.get("validacion_nivel", "")

    # Runs hoy / semana
    today = datetime.now().strftime("%Y-%m-%d")
    week_start = (datetime.now() - timedelta(days=7)).strftime("%Y-%m-%d")
    runs_log = health.get("runs_log", [])
    runs_log.append({"ts": now_iso[:10], "success": success, "duration": round(duration, 1)})
    runs_log = [r for r in runs_log if r["ts"] >= week_start]
    health["runs_log"]         = runs_log[-50:]
    health["pipeline_runs_today"]  = len([r for r in runs_log if r["ts"] == today])
    health["pipeline_runs_week"]   = len(runs_log)

    # Métricas externas (backtest, pesos)
    health.update(_read_backtest_metrics())
    health.update(_read_weights_metrics())
    health.update(_check_data_freshness())

    # SLA
    sla = _compute_sla(health)
    health["sla_status"] = sla["status"]
    health["sla_hours_since_success"] = sla["hours_since_success"]

    _save_health(health)
    logger.info(
        f"[monitor] SLA={sla['status']} | Runs hoy={health['pipeline_runs_today']} | "
        f"Buy={health.get('buy_signals',0)} | Dur={health.get('duration_last_sec',0)}s"
    )
    return health


def check_sla(send_telegram: bool = True) -> dict:
    """
    Verifica SLA y envía alerta si el pipeline está muerto.
    Llamar desde start_server.py en cada /webhook/status.
    """
    health = _load_health()
    sla    = _compute_sla(health)

    if sla["status"] == "CRITICAL" and send_telegram:
        _send_sla_alert(sla, health)

    return sla


# ── SLA check ───────────────────────────────────────────────────────────────

def _compute_sla(health: dict) -> dict:
    """Calcula el estado del SLA basado en last_success."""
    last_success = health.get("last_success")
    if not last_success:
        return {"status": "UNKNOWN", "hours_since_success": None}

    try:
        last_dt   = datetime.fromisoformat(last_success)
        hours_ago = (datetime.now() - last_dt).total_seconds() / 3600

        if hours_ago > SLA_CRITICAL_HOURS:
            status = "CRITICAL"
        elif hours_ago > SLA_WARN_HOURS:
            status = "WARNING"
        else:
            status = "OK"

        return {"status": status, "hours_since_success": round(hours_ago, 1)}

    except Exception:
        return {"status": "UNKNOWN", "hours_since_success": None}


def _send_sla_alert(sla: dict, health: dict):
    """Envía alerta Telegram cuando el pipeline está muerto."""
    try:
        import requests
        bot_token = os.environ.get("TELEGRAM_BOT_TOKEN", "")
        chat_id   = os.environ.get("TELEGRAM_CHAT_ID", "")
        if not bot_token or not chat_id:
            return

        hours = sla.get("hours_since_success", "?")
        last_run = health.get("last_run", "?")[:16]
        failures = health.get("consecutive_failures", 0)

        text = (
            "🚨 <b>ALERTA PIPELINE — INVERSIONES BURSÁTILES</b>\n\n"
            f"⏰ Sin run exitoso hace <b>{hours}h</b>\n"
            f"📅 Último intento: {last_run}\n"
            f"❌ Fallos consecutivos: {failures}\n\n"
            "Verificar Railway → logs del servicio."
        )

        requests.post(
            f"https://api.telegram.org/bot{bot_token}/sendMessage",
            json={"chat_id": chat_id, "text": text, "parse_mode": "HTML"},
            timeout=10,
        )
        logger.warning(f"[monitor] Alerta SLA enviada por Telegram (hace {hours}h)")

    except Exception as e:
        logger.warning(f"[monitor] No se pudo enviar alerta Telegram: {e}")


# ── Lectores de métricas externas ───────────────────────────────────────────

def _read_backtest_metrics() -> dict:
    """Lee métricas del último backtest."""
    if not os.path.exists(BACKTEST_PATH):
        return {}
    try:
        with open(BACKTEST_PATH) as f:
            bt = json.load(f)
        summary = bt.get("signal_summary", [])
        # EV de la señal más usada (COMPRA o COMPRA FUERTE)
        ev = None
        for row in summary:
            if "COMPRA" in row.get("signal", ""):
                ev = row.get("expected_value")
                break
        pred = bt.get("predictor", {})
        return {
            "backtest_ev_21d":       ev,
            "backtest_total_trades": bt.get("total_trades", 0),
            "predictor_dir_acc":     pred.get("directional_accuracy"),
            "predictor_mae":         pred.get("mae"),
            "backtest_generated":    bt.get("generated", "")[:10],
        }
    except Exception:
        return {}


def _read_weights_metrics() -> dict:
    """Lee antigüedad de los pesos optimizados."""
    if not os.path.exists(WEIGHTS_PATH):
        return {"optimized_weights_age_h": None, "optimized_weights_mode": "none"}
    try:
        age_h = (time.time() - os.path.getmtime(WEIGHTS_PATH)) / 3600
        with open(WEIGHTS_PATH) as f:
            wdata = json.load(f)
        return {
            "optimized_weights_age_h": round(age_h, 1),
            "optimized_weights_mode":  wdata.get("mode", "unknown"),
            "optimized_weights_days":  wdata.get("days_history", 0),
        }
    except Exception:
        return {}


def _check_data_freshness() -> dict:
    """Calcula antigüedad del CSV más reciente."""
    oldest_h = None
    for path in CSV_PATHS:
        if os.path.exists(path):
            age_h = (time.time() - os.path.getmtime(path)) / 3600
            if oldest_h is None or age_h < oldest_h:
                oldest_h = age_h
    return {
        "data_freshness_h": round(oldest_h, 1) if oldest_h is not None else None,
        "data_stale":       oldest_h is not None and oldest_h > 10,
    }


# ── Persistencia ────────────────────────────────────────────────────────────

def _load_health() -> dict:
    os.makedirs("data", exist_ok=True)
    if not os.path.exists(HEALTH_PATH):
        return {}
    try:
        with open(HEALTH_PATH) as f:
            return json.load(f)
    except Exception:
        return {}


def _save_health(health: dict):
    os.makedirs("data", exist_ok=True)
    with open(HEALTH_PATH, "w") as f:
        json.dump(health, f, ensure_ascii=False, indent=2)
