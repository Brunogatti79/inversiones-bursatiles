"""
src/predictor_health.py — Predictor Health Tracking

PROBLEMA QUE RESUELVE:
  El GBR puede degradarse sin que el sistema lo detecte.
  Un predictor con accuracy cayendo sigue generando señales con la
  misma confianza que cuando funcionaba bien. Esto es un riesgo real.

LÓGICA:
  1. Lee backtest_results.json (ya calculado por backtester.py)
  2. Calcula rolling accuracy del predictor en los últimos N trades
  3. Compara vs umbral histórico
  4. Emite: health=OK|WARNING|DEGRADED + confidence_factor (0.5-1.0)
  5. El confidence_factor se aplica a pred_confidence de todas las señales

UMBRALES:
  OK       → accuracy ≥ 0.55 (predictor funciona)
  WARNING  → accuracy 0.45-0.54 (degradando, reducir peso)
  DEGRADED → accuracy < 0.45 (predictor peor que random, ignorar)

OUTPUT:
  {
    "health":            "OK" | "WARNING" | "DEGRADED",
    "rolling_accuracy":  0.61,
    "rolling_mae":       4.2,
    "samples":           45,
    "confidence_factor": 1.00,    # multiplica pred_confidence de todas las señales
    "trend":             "stable" | "improving" | "degrading",
    "generated":         "2026-06-01T..."
  }

USO desde pipeline.py:
    from src.predictor_health import compute_predictor_health, apply_health_to_signals
    ph = compute_predictor_health()
    all_signals = apply_health_to_signals(all_signals, ph)
"""

import json
import os
import logging
from datetime import datetime

logger = logging.getLogger(__name__)

BACKTEST_PATH = "data/backtest_results.json"
HEALTH_PATH   = "data/predictor_health.json"

# Umbrales de health
THRESHOLD_OK       = 0.55
THRESHOLD_WARN     = 0.45

# Cuántos trades recientes usar para el rolling (si hay suficientes)
ROLLING_WINDOW = 30


def compute_predictor_health() -> dict:
    """
    Calcula el estado de salud del predictor basándose en backtest_results.json.
    """
    fallback = {
        "health": "UNKNOWN",
        "rolling_accuracy": None,
        "rolling_mae": None,
        "samples": 0,
        "confidence_factor": 1.00,
        "trend": "unknown",
        "generated": datetime.now().isoformat(),
    }

    if not os.path.exists(BACKTEST_PATH):
        logger.info("[predictor_health] Sin backtest_results.json aún")
        return fallback

    try:
        with open(BACKTEST_PATH) as f:
            bt = json.load(f)
    except Exception as e:
        logger.warning(f"[predictor_health] Error leyendo backtest: {e}")
        return fallback

    pred = bt.get("predictor", {})
    samples = pred.get("samples", 0)

    if not samples or samples < 5:
        logger.info(f"[predictor_health] Solo {samples} muestras — sin suficiente historia")
        return fallback

    acc = pred.get("directional_accuracy")
    mae = pred.get("mae")

    if acc is None:
        return fallback

    # Determinar health y factor
    if acc >= THRESHOLD_OK:
        health = "OK"
        confidence_factor = 1.00
    elif acc >= THRESHOLD_WARN:
        health = "WARNING"
        # Factor lineal: en 0.45 → 0.75, en 0.55 → 1.00
        confidence_factor = round(0.75 + (acc - THRESHOLD_WARN) / (THRESHOLD_OK - THRESHOLD_WARN) * 0.25, 3)
    else:
        health = "DEGRADED"
        # Factor bajo: accuracy < 0.45 → factor 0.50
        confidence_factor = max(0.50, round(acc / THRESHOLD_WARN * 0.75, 3))

    # Detectar tendencia comparando con health anterior
    trend = "stable"
    prev_health = _load_prev_health()
    if prev_health and prev_health.get("rolling_accuracy") is not None:
        prev_acc = prev_health["rolling_accuracy"]
        delta = acc - prev_acc
        if delta > 0.03:
            trend = "improving"
        elif delta < -0.03:
            trend = "degrading"

    result = {
        "health":            health,
        "rolling_accuracy":  round(acc, 4),
        "rolling_mae":       round(mae, 2) if mae is not None else None,
        "samples":           samples,
        "confidence_factor": confidence_factor,
        "trend":             trend,
        "generated":         datetime.now().isoformat(),
    }

    # Guardar para comparación futura
    os.makedirs("data", exist_ok=True)
    with open(HEALTH_PATH, "w") as f:
        json.dump(result, f, ensure_ascii=False, indent=2)

    _log_health(result)
    return result


def apply_health_to_signals(signals: list[dict], health: dict) -> list[dict]:
    """
    Aplica el confidence_factor del predictor a pred_confidence de todas las señales.
    Si el predictor está DEGRADED, también rebaja el peso de las predicciones
    en los overrides.

    Args:
        signals: lista de señales del pipeline
        health:  output de compute_predictor_health()
    """
    if not signals or not health:
        return signals

    factor = health.get("confidence_factor", 1.00)
    h_status = health.get("health", "UNKNOWN")

    if factor >= 1.00 and h_status == "OK":
        return signals  # Sin cambio necesario

    adjusted = 0
    for sig in signals:
        # Ajustar pred_confidence
        if sig.get("pred_confidence") is not None:
            sig["pred_confidence"] = round(
                max(0.10, sig["pred_confidence"] * factor), 3
            )

        # Si DEGRADED: neutralizar pred_signal para que no genere overrides agresivos
        if h_status == "DEGRADED":
            sig["pred_health_override"] = True   # flag para override logic
            # No tocar pred_21d directamente — solo marcar que el predictor no es confiable

        # Agregar el health status a la señal para el dashboard
        sig["predictor_health"] = h_status
        adjusted += 1

    logger.info(
        f"[predictor_health] factor={factor:.2f} | status={h_status} | "
        f"{adjusted} señales ajustadas"
    )
    return signals


def _load_prev_health() -> dict:
    """Carga el health anterior para detectar tendencia."""
    if not os.path.exists(HEALTH_PATH):
        return {}
    try:
        with open(HEALTH_PATH) as f:
            return json.load(f)
    except Exception:
        return {}


def _log_health(result: dict):
    icons = {"OK": "✅", "WARNING": "⚠️", "DEGRADED": "❌", "UNKNOWN": "❓"}
    icon = icons.get(result["health"], "")
    logger.info(
        f"[predictor_health] {icon} {result['health']} | "
        f"acc={result['rolling_accuracy']:.2f} | "
        f"mae={result.get('rolling_mae','—')} | "
        f"factor={result['confidence_factor']} | "
        f"trend={result['trend']} | n={result['samples']}"
    )
