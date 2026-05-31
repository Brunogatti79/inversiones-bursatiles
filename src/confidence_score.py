"""
src/confidence_score.py — Score de Confianza Compuesto por Señal

Combina múltiples fuentes de evidencia en un único número (0-100)
que indica cuánta confianza merece cada señal individual.

COMPONENTES:
  1. Acuerdo predictor + modelo      (35%) — ¿coinciden en dirección?
  2. Alignment de timeframes         (25%) — ¿diario+semanal+mensual alineados?
  3. Quality checks sin alertas      (20%) — ¿sin flags rojos/amarillos?
  4. Consistencia V1/V2              (10%) — ¿ambos scores apuntan igual?
  5. Régimen de volatilidad          (10%) — alta vol penaliza, baja vol bonifica

INTERPRETACIÓN:
  ≥ 75 → Alta confianza  → señal sólida, Kelly sin restricción adicional
  55-74 → Media confianza → Kelly × 0.85, reducir tamaño
  35-54 → Baja confianza → Kelly × 0.60, posición mínima o esperar
  < 35 → Muy baja confianza → ignorar señal aunque score V2 sea bueno

USO desde pipeline.py (post-quality_check, post-exit_model):
    from src.confidence_score import enrich_confidence_scores
    all_signals = enrich_confidence_scores(all_signals, vol_regime)
"""

import logging

logger = logging.getLogger(__name__)

# ── Entrypoint ──────────────────────────────────────────────────────────

def enrich_confidence_scores(signals: list[dict], vol_regime: dict = None) -> list[dict]:
    """
    Agrega campo 'confidence_score' (0-100) y 'confidence_label' a cada señal.
    También ajusta 'kelly_half' si ya fue calculado por portfolio_optimizer.

    Args:
        signals:    lista de señales del pipeline
        vol_regime: output de compute_volatility_regime()
    """
    if not signals:
        return signals

    vol_regime = vol_regime or {}
    global_regime = vol_regime.get("global_regime", "NORMAL")
    enriched = 0

    for sig in signals:
        try:
            cs, label = _calc_confidence(sig, global_regime)
            sig["confidence_score"] = cs
            sig["confidence_label"] = label

            # Si kelly_half ya fue calculado, ajustarlo por confianza
            if sig.get("kelly_half") is not None:
                adj = _confidence_kelly_factor(cs)
                sig["kelly_half_adj"] = round(sig["kelly_half"] * adj, 1)
                sig["confidence_adj_factor"] = round(adj, 2)

            enriched += 1
        except Exception:
            sig["confidence_score"] = 50
            sig["confidence_label"] = "Media"

    buy_count = len([s for s in signals if s.get("confidence_score", 0) >= 75
                     and "COMPRA" in (s.get("signal_v2") or "")])
    logger.info(f"[confidence] {enriched} señales enriquecidas | Alta conf+compra: {buy_count}")
    return signals


# ── Cálculo del score ───────────────────────────────────────────────────

def _calc_confidence(sig: dict, global_regime: str) -> tuple[float, str]:
    """Calcula confidence score 0-100 para una señal."""
    score = 0.0

    # ── 1. Acuerdo predictor + modelo (35%) ─────────────────────────────
    pred_21d  = sig.get("pred_21d")
    signal    = sig.get("signal_v2") or sig.get("signal", "")
    is_buy    = "COMPRA" in signal
    is_sell   = "VENTA"  in signal

    if pred_21d is not None:
        pred_confirms = (is_buy and pred_21d > 0) or (is_sell and pred_21d < 0)
        pred_conf     = sig.get("pred_confidence", 0.5) or 0.5

        if pred_confirms:
            # Predictor confirma + confianza del predictor
            score += 35 * min(1.0, pred_conf / 0.70)
        else:
            # Predictor contradice → penalización fuerte
            score += 35 * (1 - min(1.0, pred_conf / 0.70)) * 0.30
    else:
        score += 15  # sin datos del predictor, contribución parcial

    # ── 2. Alignment de timeframes (25%) ────────────────────────────────
    alignment = sig.get("alignment_label", "")
    align_map = {
        "TRIPLE CONFIRMACIÓN": 25,
        "DOBLE CONFIRMACIÓN":  18,
        "SIN DATOS":           12,
        "SEÑALES MIXTAS":       8,
        "CONFLICTO PARCIAL":    5,
        "CONFLICTO TOTAL":      0,
    }
    score += align_map.get(alignment, 12)

    # ── 3. Quality checks (20%) ──────────────────────────────────────────
    quality = sig.get("quality_flag", "🟢")
    quality_score = {"🟢": 20, "🟡": 12, "🔴": 3}.get(quality, 12)
    score += quality_score

    # ── 4. Consistencia V1/V2 (10%) ──────────────────────────────────────
    score_v1 = float(sig.get("score_final", 50) or 50)
    score_v2 = float(sig.get("score_final_v2", 50) or 50)
    diff = abs(score_v1 - score_v2)
    if diff < 10:
        score += 10   # muy consistentes
    elif diff < 20:
        score += 7
    elif diff < 30:
        score += 4
    else:
        score += 1    # V1 y V2 muy distintos

    # ── 5. Régimen de volatilidad (10%) ──────────────────────────────────
    vol_pts = {"LOW": 10, "NORMAL": 7, "HIGH": 3}.get(global_regime, 7)
    score += vol_pts

    # Clamp
    score = round(min(100, max(0, score)), 1)

    # Label
    if score >= 75:
        label = "🟢 Alta"
    elif score >= 55:
        label = "🟡 Media"
    elif score >= 35:
        label = "🟠 Baja"
    else:
        label = "🔴 Muy baja"

    return score, label


def _confidence_kelly_factor(confidence_score: float) -> float:
    """
    Factor multiplicador para Kelly basado en confianza.
    Implementa: f_adj = Kelly * min(1, sqrt(confidence/75))
    """
    if confidence_score >= 75:
        return 1.00
    elif confidence_score >= 55:
        return round((confidence_score / 75) ** 0.5, 3)
    elif confidence_score >= 35:
        return round((confidence_score / 75) ** 0.75, 3)
    else:
        return 0.30   # mínimo: 30% del Kelly calculado
