"""
src/confidence_score.py — Score de Confianza Compuesto por Señal

Combina múltiples fuentes de evidencia en un único número (0-100)
que indica cuánta confianza merece cada señal individual.

COMPONENTES:
  1. Acuerdo predictor + modelo      (15%) — ¿coinciden en dirección?
  2. Alignment de timeframes         (33%) — ¿diario+semanal+mensual alineados?
  3. Quality checks sin alertas      (26%) — ¿sin flags rojos/amarillos?
  4. Consistencia V1/V2              (13%) — ¿ambos scores apuntan igual?
  5. Régimen de volatilidad          (13%) — alta vol penaliza, baja vol bonifica

NOTA 24/08/2026 (auditoría con Claude, evidencia en backtest_results.json):
peso del predictor bajado de 35% a 15% (los 20pts liberados se redistribuyeron
proporcionalmente a los otros 4 componentes: 25→33, 20→26, 10→13, 10→13,
manteniendo el total en 100). Motivo: confidence_calibration.monotona=false
en el backtest (quintil de mayor confianza es el de PEOR resultado real,
28.7% WR / -3.50% EV, contra 47.3% WR / -0.63% EV del quintil más bajo), y
predictor_contribution mostraba que "el predictor confirma la señal" rendía
peor (-3.15% EV a 21d, n=173) que "el predictor contradice" (-1.92% EV,
n=225) -- efecto persistente en las dos ventanas temporales con datos
suficientes (30d y 45d de historia). Era el componente de mayor peso (35%)
y estaba empujando la confianza en la dirección equivocada. No se invirtió
el signo del componente (sería ajustar a una muestra de 47 días que podría
ser ruido de régimen, no una relación causal demostrada) -- se redujo su
peso para que domine menos mientras no haya más evidencia de que aporta
valor marginal real. Revisar de nuevo cuando predictor_health.py muestre
salud sostenida y/o haya más historia para confirmar si el efecto persiste.

INTERPRETACIÓN:
  ≥ 75 → Alta confianza  → señal sólida
  55-74 → Media confianza → reducir tamaño
  35-54 → Baja confianza → posición mínima o esperar
  < 35 → Muy baja confianza → ignorar señal aunque score V2 sea bueno

USO desde pipeline.py (post-quality_check, post-exit_model):
    from src.confidence_score import enrich_confidence_scores
    all_signals = enrich_confidence_scores(all_signals, vol_regime)

NOTA 10/08/2026 (auditoría real, no teórica): este módulo calculaba
kelly_half_adj = kelly_half * _confidence_kelly_factor(confidence_score)
como un segundo ajuste de Kelly por confianza, independiente del que
hace portfolio_optimizer.py. Verificado con grep exhaustivo contra
generator.py, notifier.py, bot.py y la generación de Excel: ningún
consumidor downstream leía kelly_half_adj -- se calculaba en cada
corrida sin ningún efecto sobre lo que se muestra en dashboard/Telegram
(lo que sí se muestra, suggested_pct, nunca pasaba por acá). No era un
doble castigo real porque el campo era código muerto, pero SÍ hubiera
sido un doble castigo real si se hubiera conectado a algo, porque desde
el fix del mismo día en portfolio_optimizer.py::_calc_kelly_weights(),
kelly_half YA viene segmentado por confidence_label (usa
confidence_x_signal[label] antes que el agregado). Aplicar
_confidence_kelly_factor() otra vez encima habría penalizado la misma
señal dos veces por el mismo motivo. Eliminado en vez de conectado --
ver _confidence_kelly_factor() más abajo, que queda sin uso a propósito
(no se borra la función por si se decide reintroducir el ajuste algún
día, pero con ese trade-off explícito en mente).
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
            cs, label, breakdown = _calc_confidence(sig, global_regime)
            sig["confidence_score"] = cs
            sig["confidence_label"] = label
            # ── Instrumentación 24/08/2026 (auditoría con Claude): antes de
            # esto, alignment_label y quality_flag se USABAN para calcular
            # confidence_score pero no quedaban persistidos en
            # signals_history.json -- ni ellos ni la contribución de cada
            # componente al score final. Sin esto, cualquier contrafactual
            # futuro sobre los pesos (ej. "¿qué hubiera dado el predictor a
            # 15% vs 35% en la historia real?") no se puede reconstruir con
            # precisión, solo aproximar. Se persiste el desglose completo,
            # no solo el total.
            sig["confidence_breakdown"] = breakdown
            enriched += 1
        except Exception:
            sig["confidence_score"] = 50
            sig["confidence_label"] = "Media"
            sig["confidence_breakdown"] = {}

    buy_count = len([s for s in signals if s.get("confidence_score", 0) >= 75
                     and "COMPRA" in (s.get("signal_v2") or "")])
    logger.info(f"[confidence] {enriched} señales enriquecidas | Alta conf+compra: {buy_count}")
    return signals


# ── Cálculo del score ───────────────────────────────────────────────────

def _calc_confidence(sig: dict, global_regime: str) -> tuple[float, str, dict]:
    """Calcula confidence score 0-100 para una señal.

    Devuelve (score, label, breakdown) -- breakdown incluye los puntos que
    aportó cada componente y los valores crudos usados (alignment_label,
    quality_flag), para poder persistirlo por señal (ver
    enrich_confidence_scores) y auditar/recalcular contrafactuales después,
    sin tener que reconstruir esos valores desde otro lado.
    """
    score = 0.0

    # ── 1. Acuerdo predictor + modelo (15%, bajado de 35% el 24/08/2026) ─
    pred_21d  = sig.get("pred_21d")
    signal    = sig.get("signal_v2") or sig.get("signal", "")
    is_buy    = "COMPRA" in signal
    is_sell   = "VENTA"  in signal

    if pred_21d is not None:
        pred_confirms = (is_buy and pred_21d > 0) or (is_sell and pred_21d < 0)
        pred_conf     = sig.get("pred_confidence", 0.5) or 0.5

        if pred_confirms:
            # Predictor confirma + confianza del predictor
            pred_pts = 15 * min(1.0, pred_conf / 0.70)
        else:
            # Predictor contradice → penalización fuerte
            pred_pts = 15 * (1 - min(1.0, pred_conf / 0.70)) * 0.30
    else:
        pred_confirms = None
        pred_pts = 6.4  # sin datos del predictor, contribución parcial (15/35 * 15)
    score += pred_pts

    # ── 2. Alignment de timeframes (33%) ────────────────────────────────
    alignment = sig.get("alignment_label", "")
    align_map = {
        "TRIPLE CONFIRMACIÓN": 33,
        "DOBLE CONFIRMACIÓN":  24,
        "SIN DATOS":           16,
        "SEÑALES MIXTAS":      11,
        "CONFLICTO PARCIAL":    7,
        "CONFLICTO TOTAL":      0,
    }
    align_pts = align_map.get(alignment, 16)
    score += align_pts

    # ── 3. Quality checks (26%) ──────────────────────────────────────────
    quality = sig.get("quality_flag", "🟢")
    quality_score = {"🟢": 26, "🟡": 16, "🔴": 4}.get(quality, 16)
    score += quality_score

    # ── 4. Consistencia V1/V2 (13%) ──────────────────────────────────────
    score_v1 = float(sig.get("score_final", 50) or 50)
    score_v2 = float(sig.get("score_final_v2", 50) or 50)
    diff = abs(score_v1 - score_v2)
    if diff < 10:
        consist_pts = 13   # muy consistentes
    elif diff < 20:
        consist_pts = 9
    elif diff < 30:
        consist_pts = 5
    else:
        consist_pts = 1    # V1 y V2 muy distintos
    score += consist_pts

    # ── 5. Régimen de volatilidad (13%) ───────────────────────────────────
    vol_pts = {"LOW": 13, "NORMAL": 9, "HIGH": 4}.get(global_regime, 9)
    score += vol_pts

    breakdown = {
        "predictor_pts":      round(pred_pts, 2),
        "predictor_confirma": pred_confirms,
        "alignment_label":    alignment,
        "alignment_pts":      align_pts,
        "quality_flag":       quality,
        "quality_pts":        quality_score,
        "consistency_pts":    consist_pts,
        "vol_regime_usado":   global_regime,
        "vol_pts":            vol_pts,
    }

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

    return score, label, breakdown


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
# (kelly_half → 0). NO toca posiciones ya abiertas en
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
    la asignación de capital NUEVA (kelly_half -> 0) en TODAS las
    señales, sin distinguir por confianza individual -- es un circuit
    breaker de sistema, no un filtro por ticker.

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

    return signals


# ── EXPOSURE TOTAL — ACTIVADO (26/06/2026, a pedido explícito de Bruno) ────
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
# KILL_SWITCH_THRESHOLD. Cero números inventados sin datos.
#
# ACTIVADO a pedido explícito de Bruno el 25/06/2026, después de quedar en
# modo sombra (commit 6a7b574). Advertencia que se le hizo antes de activar
# y que sigue vigente, no resuelta por esta activación: portfolio_optimizer
# corre HOY con pesos V1 100% replay sintético (ver weights_provenance) y
# solo 3 días de signals_history.json real -- exposure_factor depende de
# global_score, que a su vez depende de señales calculadas con esos pesos
# sintéticos. Activar esto no espera a que esa capa de abajo esté validada;
# es una decisión consciente de Bruno, no una recomendación de Claude.
#
# apply_exposure_factor() (al final de este bloque) escala kelly_f/
# kelly_half/suggested_pct YA CALCULADOS por portfolio_optimizer.py, en un
# paso final post-proceso -- mismo patrón que apply_kill_switch(), no
# requirió reordenar el pipeline ni threadear el factor a través de Kelly/
# Risk Parity. A diferencia de regime_factor (que sí entra DENTRO de
# _calc_kelly_weights y por eso se cancela en la normalización del blend),
# exposure_factor se aplica DESPUÉS de la normalización -- por eso sí
# afecta suggested_pct, que es justo lo que la devolución pedía y que
# regime_factor (Prioridad 5) explícitamente no hacía.

def compute_exposure_factor(global_conf: dict, regime_factor: float = 1.0) -> dict:
    """
    Calcula qué fracción del capital "normal" del día debería usarse,
    combinando confianza global (rampa continua 35-70, reusa los cortes
    del label) y regime_factor (volatility_regime.py). Ver nota de diseño
    arriba. Activo en producción desde el 25/06/2026 — ver
    apply_exposure_factor() para dónde se aplica.

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
        "active_in_production": True,
        "generated":            datetime.now().isoformat(),
    }


def apply_exposure_factor(signals: list[dict], exposure: dict) -> list[dict]:
    """
    Escala kelly_f / kelly_half / suggested_pct (ya calculados por
    portfolio_optimizer.optimize_portfolio_allocation()) por
    exposure['exposure_factor']. Solo toca señales que ya tienen esos
    campos (es decir, señales de COMPRA que portfolio_optimizer procesó) --
    el resto queda intacto.

    Si exposure_factor >= 1.0 no hay nada que escalar (no reescribe los
    campos sin necesidad, evita ruido de redondeo en runs sin ajuste).

    Si el kill switch ya está activo, apply_kill_switch() (arriba) ya puso
    kelly_half en 0 antes de que esto corra -- multiplicar por
    exposure_factor (que también sería 0 en ese caso) es redundante pero
    no dañino, y cubre kelly_f/suggested_pct que apply_kill_switch no toca.
    """
    if not signals or not exposure:
        return signals

    factor = exposure.get("exposure_factor", 1.0)

    for sig in signals:
        sig["exposure_factor_applied"] = factor
        if factor >= 1.0:
            continue
        if sig.get("kelly_f") is not None:
            sig["kelly_f"] = round(sig["kelly_f"] * factor, 4)
        if sig.get("kelly_half") is not None:
            sig["kelly_half"] = round(sig["kelly_half"] * factor, 1)
        if sig.get("suggested_pct") is not None:
            sig["suggested_pct"] = round(sig["suggested_pct"] * factor, 1)
            nota_exposure = f" Exposure Total recortó a {factor*100:.0f}% (confianza/régimen del sistema)."
            sig["allocation_notes"] = (sig.get("allocation_notes") or "") + nota_exposure

    return signals

