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
from datetime import datetime

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


# ════════════════════════════════════════════════════════════════════════
# CONFIDENCE SCORE GLOBAL + KILL SWITCH (mejora 3.1 + 3.5)
# ════════════════════════════════════════════════════════════════════════
#
# A diferencia del confidence_score por señal (arriba), esto evalúa la
# confianza del RUN COMPLETO del pipeline -- es un circuit breaker a nivel
# sistema, no un filtro por ticker.
#
# Componentes (100 pts):
#   1. Calidad de datos (quality_check, % tickers limpios)        30%
#   2. Salud del predictor (predictor_health.py)                  25%
#   3. Confianza de datos macro (macro_auto, 3 mercados)           20%
#   4. Integridad de datos de mercado (data_validator, CSVs)       10%
#   5. Operación del pipeline / SLA (monitor.py)                   15%
#
# INTERPRETACIÓN:
#   ≥ 70  → 🟢 Confiable      → operar normalmente
#   50-69 → 🟡 Reducida       → Kelly ya viene recortado por confidence_score
#                               por-señal, no se frena capital nuevo
#   35-49 → 🟠 Baja           → alerta, sin frenar capital todavía
#   < 35  → 🔴 Crítica        → KILL SWITCH: frena asignación de capital NUEVO
#
# El kill switch tiene además 2 triggers duros, independientes del score
# ponderado (cualquiera de los dos lo activa aunque el score global no haya
# cruzado el umbral):
#   - data_validator marcó "ERROR" (datos de mercado corruptos o faltantes)
#   - quality_check encontró >= MIN_CRITICAL_FOR_KILL alertas 🔴 CRÍTICA
#     (ej. contradicciones V1/V2, precios inválidos) -- estas son señales
#     de que algo está estructuralmente roto, no solo "bajo" en confianza.
#
# Importante: el kill switch frena recomendaciones de capital NUEVO
# (kelly_half / kelly_half_adj → 0). NO toca posiciones ya abiertas en
# portfolio.json -- decidir qué hacer con posiciones existentes durante un
# kill switch es una decisión de Bruno, no automática.
#
# USO desde pipeline.py (con todo el contexto del run ya disponible):
#     from src.confidence_score import compute_global_confidence, apply_kill_switch
#     global_conf = compute_global_confidence(
#         n_signals=len(all_signals), quality_resumen=quality_resumen,
#         predictor_health=predictor_health, macro_confidence=macro_confidence,
#         validacion_nivel=nivel, sla_status=sla_now["status"],
#     )
#     all_signals = apply_kill_switch(all_signals, global_conf)
#     from src.monitor import persist_global_confidence
#     persist_global_confidence(global_conf)   # persiste + alerta Telegram en transición

KILL_SWITCH_THRESHOLD = 35      # score global por debajo de esto -> kill switch
MIN_CRITICAL_FOR_KILL = 5       # alertas 🔴 críticas de quality_check -> kill switch duro

GLOBAL_CONFIDENCE_WEIGHTS = {
    "calidad_datos":    0.30,
    "predictor":        0.25,
    "macro":            0.20,
    "integridad_datos": 0.10,
    "sla":              0.15,
}


def compute_global_confidence(
    n_signals: int = 0,
    quality_resumen: dict = None,
    predictor_health: dict = None,
    macro_confidence: dict = None,
    validacion_nivel: str = "OK",
    sla_status: str = "OK",
) -> dict:
    """
    Calcula el confidence score GLOBAL del run (0-100) y decide si
    corresponde activar el kill switch.

    Función pura (sin I/O) a propósito -- persistencia y alertas Telegram
    viven en monitor.persist_global_confidence(), así esto es trivial de
    testear con distintas combinaciones de inputs.
    """
    quality_resumen  = quality_resumen or {}
    predictor_health = predictor_health or {}
    macro_confidence = macro_confidence or {}
    components = {}

    # ── 1. Calidad de datos (30%) ───────────────────────────────────────
    # resumen["ok"] = cantidad de señales sin ningún check disparado.
    # resumen["criticas"]/["advertencias"] son conteos de ALERTAS (un
    # ticker puede acumular varias), no de tickers -- por eso se usan
    # como penalización aditiva en vez de como fracción del total.
    n_ok = quality_resumen.get("ok", 0)
    criticas = quality_resumen.get("criticas", 0)
    # criticas_estructurales excluye "V1 vs V2 contradicción" (comportamiento
    # esperado del modelo, no error de datos -- ver quality_check.py). Si el
    # campo no está (resumen viejo o de un test), cae a `criticas` completo
    # por compatibilidad, aceptando ser algo más conservador en ese caso.
    criticas_estructurales = quality_resumen.get("criticas_estructurales", criticas)
    advertencias = quality_resumen.get("advertencias", 0)
    if n_signals > 0:
        clean_ratio = n_ok / n_signals
    else:
        # Sin datos de quality_check todavía (ej. corrida vieja antes de
        # wirearlo, o n_signals no provisto) -> neutral, no penalizar.
        clean_ratio = 1.0
    critical_penalty = min(40.0, criticas * 8.0)
    warning_penalty  = min(15.0, advertencias * 1.0)
    data_quality_score = max(0.0, min(100.0, clean_ratio * 100 - critical_penalty - warning_penalty))
    components["calidad_datos"] = round(data_quality_score, 1)

    # ── 2. Salud del predictor (25%) ────────────────────────────────────
    ph_status = predictor_health.get("health", "UNKNOWN")
    ph_map = {"OK": 100.0, "WARNING": 55.0, "DEGRADED": 15.0, "UNKNOWN": 65.0}
    components["predictor"] = ph_map.get(ph_status, 65.0)

    # ── 3. Confianza de datos macro (20%) — promedio de los 3 mercados ──
    if macro_confidence:
        macro_scores = [
            v.get("score", 50.0) for v in macro_confidence.values()
            if isinstance(v, dict) and v.get("score") is not None
        ]
        components["macro"] = round(sum(macro_scores) / len(macro_scores), 1) if macro_scores else 50.0
    else:
        components["macro"] = 50.0  # sin macro_confidence disponible -> neutral

    # ── 4. Integridad de datos de mercado (10%) — data_validator ────────
    val_map = {"OK": 100.0, "WARNING": 50.0, "ERROR": 0.0}
    components["integridad_datos"] = val_map.get(validacion_nivel, 50.0)

    # ── 5. Operación del pipeline / SLA (15%) ───────────────────────────
    sla_map = {"OK": 100.0, "WARNING": 50.0, "CRITICAL": 0.0, "UNKNOWN": 70.0}
    components["sla"] = sla_map.get(sla_status, 70.0)

    global_score = round(
        sum(components[k] * w for k, w in GLOBAL_CONFIDENCE_WEIGHTS.items()), 1
    )

    # ── Triggers duros (independientes del score ponderado) ─────────────
    hard_triggers = []
    if validacion_nivel == "ERROR":
        hard_triggers.append(
            "data_validator marcó ERROR (datos de mercado corruptos o faltantes)"
        )
    if criticas_estructurales >= MIN_CRITICAL_FOR_KILL:
        hard_triggers.append(
            f"quality_check encontró {criticas_estructurales} alertas 🔴 críticas "
            f"estructurales (precio inválido / índice sin datos; excluye desacuerdo "
            f"V1/V2, que es comportamiento esperado del modelo) — umbral={MIN_CRITICAL_FOR_KILL}"
        )

    kill_switch_active = bool(hard_triggers) or global_score < KILL_SWITCH_THRESHOLD
    if global_score < KILL_SWITCH_THRESHOLD and not hard_triggers:
        hard_triggers.append(
            f"score global={global_score} por debajo del umbral={KILL_SWITCH_THRESHOLD}"
        )

    if global_score >= 70:
        label = "🟢 Confiable"
    elif global_score >= 50:
        label = "🟡 Reducida"
    elif global_score >= 35:
        label = "🟠 Baja"
    else:
        label = "🔴 Crítica"

    return {
        "global_score":         global_score,
        "label":                label,
        "components":           components,
        "weights":              dict(GLOBAL_CONFIDENCE_WEIGHTS),
        "kill_switch_active":   kill_switch_active,
        "kill_switch_reasons":  hard_triggers,
        "n_signals":            n_signals,
        "generated":            datetime.now().isoformat(),
    }


def apply_kill_switch(signals: list[dict], global_conf: dict) -> list[dict]:
    """
    Marca cada señal con el estado del kill switch. Si está activo, frena
    la asignación de capital NUEVA (kelly_half / kelly_half_adj -> 0) en
    TODAS las señales, sin distinguir por confianza individual -- es un
    circuit breaker de sistema, no un filtro por ticker.

    No toca posiciones ya abiertas (eso vive en portfolio.json / tracker.py)
    ni ningún otro campo de la señal.
    """
    if not signals:
        return signals

    active  = bool(global_conf.get("kill_switch_active", False))
    reasons = global_conf.get("kill_switch_reasons", [])

    for sig in signals:
        sig["kill_switch_active"] = active
        if active:
            sig["kill_switch_reasons"] = reasons
            if sig.get("kelly_half") is not None:
                sig["kelly_half"] = 0.0
            if sig.get("kelly_half_adj") is not None:
                sig["kelly_half_adj"] = 0.0

    return signals


# ── EXPOSURE TOTAL — DISEÑO, NO ACTIVADO EN PRODUCCIÓN ──────────────────────
# (devolución externa 25/06/2026: "falta una prioridad clave, exposure
# total -- el sistema decide qué comprar y cuánto relativo, pero no
# cuánto capital total usar")
#
# Por qué esto generaliza lo que YA existe, en vez de agregar una capa
# nueva sin coordinar:
#   - El kill switch (arriba) ya es una forma de exposure total -- pero
#     binaria: a score>=35, kelly_half full; a score<35, cero. Es un
#     escalón, no una rampa.
#   - regime_factor (volatility_regime.py, Prioridad 5) ya escala
#     kelly_f/kelly_half por incertidumbre SISTÉMICA -- pero solo por
#     régimen de volatilidad, no por confianza global.
#   - compute_exposure_factor() combina ambos en un solo número, y
#     generaliza el escalón del kill switch en la zona 35-70 (hoy: nada
#     pasa ahí, kelly_half es full hasta justo antes de 35) a una rampa
#     continua -- exactamente la crítica de la devolución sobre el
#     escalón del kill switch, aplicada acá también.
#
# Los 3 cortes (70/50/35) NO son números nuevos: son los mismos que ya
# definen el label de confidence global ("🟢 Confiable" >=70, "🟡 Reducida"
# >=50, "🟠 Baja" >=35, "🔴 Crítica" <35) y 35 es literalmente
# KILL_SWITCH_THRESHOLD. Cero números inventados a propósito -- inventar
# un cuarto criterio de calibración (ej. "confidence>70+LOW vol→100%") sin
# datos reales es exactamente el error que ya causó el incidente del kill
# switch el 24/06.
#
# NO SE ACTIVA TODAVÍA: esta función se calcula y se expone (Telegram +
# health_metrics, mismo patrón que weights_provenance/kill_switch_log) pero
# NO multiplica kelly_f/suggested_pct en portfolio_optimizer.py. Activarla
# es una decisión separada y explícita -- la Prioridad 5 original ya
# advertía "ninguna capa nueva sin validar la anterior", y portfolio
# Optimizer todavía corre con pesos 100% sintéticos (ver weights_provenance).
# Mientras esté en sombra, se puede comparar contra qué hubiera pasado.

def compute_exposure_factor(global_conf: dict, regime_factor: float = 1.0) -> dict:
    """
    Calcula qué fracción del capital "normal" del día debería usarse,
    combinando confianza global (rampa continua 35-70, reusa los cortes
    del label) y regime_factor (volatility_regime.py). Ver nota de diseño
    arriba -- NO se aplica a kelly_f/suggested_pct todavía.

    Args:
        global_conf:   output de compute_global_confidence() (necesita
                       'global_score' y 'kill_switch_active').
        regime_factor: output de compute_volatility_regime()['regime_factor'].

    Returns dict con exposure_factor (0.0 a ~1.10) + el detalle de cada
    componente, para poder loguear/exponer sin recalcular nada.
    """
    score = global_conf.get("global_score", 50.0)
    kill_switch_active = bool(global_conf.get("kill_switch_active", False))

    if kill_switch_active or score < KILL_SWITCH_THRESHOLD:
        # Mismo resultado que el kill switch -- esta función NUNCA da más
        # exposición que el kill switch en su zona, solo agrega gradiente
        # por ENCIMA del umbral.
        confidence_component = 0.0
    elif score >= 70:
        confidence_component = 1.00
    elif score >= 50:
        confidence_component = round(0.60 + (score - 50) / 20 * 0.40, 3)
    else:  # KILL_SWITCH_THRESHOLD <= score < 50
        confidence_component = round(0.30 + (score - KILL_SWITCH_THRESHOLD) / 15 * 0.30, 3)

    exposure_factor = round(confidence_component * regime_factor, 3)

    return {
        "exposure_factor":      exposure_factor,
        "confidence_component": confidence_component,
        "regime_component":     regime_factor,
        "global_score":         score,
        "kill_switch_active":   kill_switch_active,
        "active_in_production": False,  # modo sombra -- ver nota de diseño arriba
        "generated":            datetime.now().isoformat(),
    }

