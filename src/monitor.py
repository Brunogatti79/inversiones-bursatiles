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
  • global_confidence_score / global_confidence_label / kill_switch_active
    (mejora 3.1 + 3.5 — ver data/system_confidence.json y
    src/confidence_score.compute_global_confidence)

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
CCL_CACHE_PATH     = "data/ccl_cache.json"
CCL_STALE_MINUTES  = 240  # 4h -- mismo umbral que macro_auto.get_ccl_data() usa para el cache


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

    # Confidence global + kill switch (mejora 3.1 + 3.5), si el caller lo pasó
    global_conf = run_context.get("global_confidence")
    if global_conf:
        health["global_confidence_score"] = global_conf.get("global_score")
        health["global_confidence_label"] = global_conf.get("label")
        health["kill_switch_active"]      = global_conf.get("kill_switch_active", False)

    # Exposure Total — ACTIVO desde el 25/06/2026 (a pedido explícito de
    # Bruno). exposure_factor_shadow es un nombre heredado del período en
    # modo sombra; el valor que guarda ya es el real aplicado.
    exposure_shadow = run_context.get("exposure_shadow")
    if exposure_shadow:
        health["exposure_factor_shadow"] = exposure_shadow.get("exposure_factor")

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
    health.update(_check_ccl_status())

    # SLA
    sla = _compute_sla(health)
    health["sla_status"] = sla["status"]
    health["sla_hours_since_success"] = sla["hours_since_success"]

    _save_health(health)
    _push_health_to_github()
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
    """
    Lee antigüedad de los pesos optimizados + procedencia (Prioridad 1,
    roadmap externo, 25/06/2026): reusa weight_optimizer.weights_provenance()
    en vez de duplicar la detección de "100% replay sintético" acá -- antes
    este helper solo exponía 'mode', que no distingue si UN mercado puntual
    sigue en 0 entradas reales aunque el modo global ya parezca avanzado.
    """
    if not os.path.exists(WEIGHTS_PATH):
        return {"optimized_weights_age_h": None, "optimized_weights_mode": "none",
                "optimized_weights_is_synthetic": None}
    try:
        age_h = (time.time() - os.path.getmtime(WEIGHTS_PATH)) / 3600
        with open(WEIGHTS_PATH) as f:
            wdata = json.load(f)

        result = {
            "optimized_weights_age_h": round(age_h, 1),
            "optimized_weights_mode":  wdata.get("mode", "unknown"),
            "optimized_weights_days":  wdata.get("days_history", 0),
        }
        try:
            from src.weight_optimizer import weights_provenance
            prov = weights_provenance()
            result["optimized_weights_is_synthetic"] = prov.get("is_synthetic", False)
            result["optimized_weights_synthetic_markets"] = [
                m for m, v in prov.get("markets", {}).items() if v.get("is_synthetic")
            ]
        except Exception:
            result["optimized_weights_is_synthetic"] = None
        return result
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


def _check_ccl_status() -> dict:
    """Observabilidad del CCL (sugerida por auditoría externa v19, 29/07/2026,
    punto 1): el fix de trailing_stop.py (ATR ARS -> ATR USD para MERVAL) que
    depende de data/ccl_cache.json ahora persiste correctamente entre
    redeploys (ver start_server.py::_sync_all_data_from_github), pero eso
    solo garantiza que el archivo sobreviva -- no que el valor adentro sea
    reciente. La fuente (scraping de Ámbito) no tiene SLA propio: si cambia
    el HTML/JSON de la fuente, el CCL puede quedar viejo silenciosamente sin
    que nada lo marque hasta que quality_check detecte un síntoma indirecto
    (score macro en 0/44 default). Esto lo hace visible directamente."""
    try:
        if not os.path.exists(CCL_CACHE_PATH):
            return {
                "ccl_source_status": "SIN_ARCHIVO",
                "ccl_age_minutes":   None,
                "last_ccl_value":    None,
            }
        with open(CCL_CACHE_PATH, encoding="utf-8") as f:
            cached = json.load(f)

        valor = cached.get("compra")
        age_min = (time.time() - os.path.getmtime(CCL_CACHE_PATH)) / 60

        if not valor or float(valor) <= 0:
            status = "SIN_VALOR"
        elif age_min > CCL_STALE_MINUTES:
            status = "STALE"
        else:
            status = "OK"

        return {
            "ccl_source_status": status,
            "ccl_age_minutes":   round(age_min, 1),
            "last_ccl_value":    float(valor) if valor else None,
        }
    except Exception as e:
        logger.warning(f"[monitor] No se pudo evaluar estado de CCL: {e}")
        return {
            "ccl_source_status": "ERROR",
            "ccl_age_minutes":   None,
            "last_ccl_value":    None,
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


def _push_health_to_github():
    """Pushea health_metrics.json a GitHub (el filesystem de Railway es efímero)."""
    from src.github_persistence import push_file
    push_file(HEALTH_PATH, f"auto: health_metrics {datetime.now().strftime('%Y-%m-%d %H:%M')}")


# ── Confidence score global + kill switch (mejora 3.1 + 3.5) ───────────────
#
# El cálculo puro vive en src/confidence_score.compute_global_confidence().
# Acá solo se persiste el resultado y se decide si corresponde alertar por
# Telegram -- y la regla es: alertar SOLO en transición de estado (de
# inactivo a activo, o de activo a recuperado), nunca en cada run. El
# pipeline corre 4x/día; si el kill switch queda activo varios runs
# seguidos, un mensaje nuevo cada vez sería puro ruido y entrena a Bruno a
# ignorar la alerta justo cuando más importa.

GLOBAL_CONFIDENCE_PATH = "data/system_confidence.json"


def persist_global_confidence(global_conf: dict, send_telegram: bool = True) -> dict:
    """
    Persiste data/system_confidence.json (sobrevive a redeploys vía
    github_persistence) y envía alerta Telegram únicamente cuando el
    kill switch cambia de estado respecto a la corrida anterior.

    Llamar desde pipeline.py después de confidence_score.apply_kill_switch().
    """
    prev = _load_global_confidence()
    was_active = bool(prev.get("kill_switch_active", False))
    is_active  = bool(global_conf.get("kill_switch_active", False))

    _save_global_confidence(global_conf)
    _push_global_confidence_to_github()

    if send_telegram and is_active and not was_active:
        _send_kill_switch_alert(global_conf, recovered=False)
    elif send_telegram and was_active and not is_active:
        _send_kill_switch_alert(global_conf, recovered=True)

    return global_conf


def _load_global_confidence() -> dict:
    if not os.path.exists(GLOBAL_CONFIDENCE_PATH):
        return {}
    try:
        with open(GLOBAL_CONFIDENCE_PATH) as f:
            return json.load(f)
    except Exception:
        return {}


def _save_global_confidence(global_conf: dict):
    os.makedirs("data", exist_ok=True)
    with open(GLOBAL_CONFIDENCE_PATH, "w") as f:
        json.dump(global_conf, f, ensure_ascii=False, indent=2)


def _push_global_confidence_to_github():
    from src.github_persistence import push_file
    push_file(
        GLOBAL_CONFIDENCE_PATH,
        f"auto: system_confidence {datetime.now().strftime('%Y-%m-%d %H:%M')}",
    )


def _send_kill_switch_alert(global_conf: dict, recovered: bool):
    """Alerta Telegram en transición de estado del kill switch (no en cada run)."""
    try:
        import requests
        bot_token = os.environ.get("TELEGRAM_BOT_TOKEN", "")
        chat_id   = os.environ.get("TELEGRAM_CHAT_ID", "")
        if not bot_token or not chat_id:
            return

        score = global_conf.get("global_score", "?")
        label = global_conf.get("label", "?")

        if recovered:
            text = (
                "✅ <b>KILL SWITCH DESACTIVADO — INVERSIONES BURSÁTILES</b>\n\n"
                f"Confidence global recuperado a <b>{score}</b> ({label}).\n"
                "Asignación de capital nueva reanudada."
            )
        else:
            reasons = "\n".join(f"• {r}" for r in global_conf.get("kill_switch_reasons", []))
            text = (
                "🔴 <b>KILL SWITCH ACTIVADO — INVERSIONES BURSÁTILES</b>\n\n"
                f"Confidence global: <b>{score}</b> ({label})\n\n"
                f"Motivo(s):\n{reasons}\n\n"
                "⛔ Asignación de capital nueva frenada (posiciones existentes no afectadas).\n"
                "Verificar dashboard / logs de Railway."
            )

        requests.post(
            f"https://api.telegram.org/bot{bot_token}/sendMessage",
            json={"chat_id": chat_id, "text": text, "parse_mode": "HTML"},
            timeout=10,
        )
        logger.warning(f"[monitor] Alerta kill switch enviada (activo={not recovered})")

    except Exception as e:
        logger.warning(f"[monitor] No se pudo enviar alerta de kill switch: {e}")
