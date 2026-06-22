"""
src/exit_model.py — Fase 1: Exit Timing Model

Reemplaza el modelo simple ATR×2 / ATR×3 con un sistema dinámico que considera:
  1. Volatilidad del activo   → stops más anchos en activos volátiles
  2. Régimen del mercado      → risk_off = stops más ajustados en todos
  3. R/R objetivo por mercado → multiplicadores diferenciados
  4. Exit score               → señal compuesta de "cuándo salir" (0-100)

MEJORAS vs modelo actual (ATR×2 / ATR×3 fijo):
  • MERVAL alta vol: usa ATR×1.5 stop / ATR×2.0 target (stops más apretados)
  • SP500 baja vol:  usa ATR×2.5 stop / ATR×4.0 target (stops más holgados)
  • Risk-off global: todos los stops se ajustan ×0.85 (más conservadores)
  • Exit score:      0-100, combina RSI overbought + MA cross + vol seca

MULTIPLICADORES POR MERCADO (base, ajustados dinámicamente):
  MERVAL:  stop_mult=1.5  target_mult=2.5  (volátil, R/R conservador)
  BOVESPA: stop_mult=2.0  target_mult=3.0  (moderado)
  SP500:   stop_mult=2.5  target_mult=4.0  (líquido, R/R amplio)

USO desde pipeline.py (post-analyze_market, post-predictor):
    from src.exit_model import enrich_exit_levels
    all_signals = enrich_exit_levels(all_signals, price_data, ticker_cols, regime)
"""

import logging
import numpy as np
import pandas as pd
from typing import Optional

logger = logging.getLogger(__name__)

# ── Multiplicadores base por mercado ──────────────────────────────────────
BASE_MULT = {
    "MERVAL":  {"stop": 1.5, "target": 2.5},
    "BOVESPA": {"stop": 2.0, "target": 3.0},
    "SP500":   {"stop": 2.5, "target": 4.0},
}
DEFAULT_MULT = {"stop": 2.0, "target": 3.0}

# Factor de ajuste por volatilidad (se aplica al multiplicador base)
# vol_score: 0-100, donde 100 = muy volátil
# Activos muy volátiles necesitan stops más anchos para no ser triggereados por ruido
VOL_STOP_SCALE   = {(0, 25): 0.80, (25, 50): 1.00, (50, 75): 1.25, (75, 101): 1.50}
VOL_TARGET_SCALE = {(0, 25): 1.20, (25, 50): 1.00, (50, 75): 0.90, (75, 101): 0.80}

# Mejora 4.2: stop dinámico por fuerza de señal V2 — más convicción (score alto)
# tolera más ruido sin saltar el stop; señal débil corta pérdidas más rápido.
SIGNAL_STRENGTH_STOP_SCALE = {(0, 35): 0.75, (35, 45): 0.85, (45, 58): 1.00, (58, 70): 1.10, (70, 101): 1.20}

# Factor por régimen de mercado
REGIME_FACTOR = {
    "RISK_ON":  1.00,   # sin cambio
    "RISK_OFF": 0.80,   # stops 20% más ajustados en riesgo
    "NEUTRAL":  0.95,
}

# Umbral mínimo de R/R para no generar targets irracionales
MIN_RR = 1.2


# ── Entrypoint principal ───────────────────────────────────────────────────

def enrich_exit_levels(
    signals: list[dict],
    price_data: dict,
    ticker_cols: dict,
    regime: str = "NEUTRAL",
) -> list[dict]:
    """
    Recalcula atr_stop y atr_target para cada señal con multiplicadores dinámicos.
    Agrega exit_score y exit_recommendation a cada signal.

    Args:
        signals:     lista de dicts de señales (output de analyze_market)
        price_data:  {"merval": df, "bovespa": df, "sp500": df}
        ticker_cols: {ticker: col_name}
        regime:      régimen cross-market ("RISK_ON"|"RISK_OFF"|"NEUTRAL")
    """
    price_series = _build_price_series(price_data, ticker_cols)
    regime_factor = REGIME_FACTOR.get(regime, 1.0)
    enriched = 0

    for sig in signals:
        try:
            ticker       = sig.get("ticker", "")
            market       = sig.get("mercado", "SP500")
            precio       = float(sig.get("precio_actual", 0) or 0)
            atr_val      = float(sig.get("atr", 0) or 0)
            vol_score    = float(sig.get("volatility_score", 50) or 50)
            score_v2     = float(sig.get("score_final_v2", sig.get("score_final", 50)) or 50)

            if precio <= 0 or atr_val <= 0:
                continue

            # Obtener serie de precios para exit_score
            serie = price_series.get(ticker)

            # Multiplicadores dinámicos
            mult = _dynamic_multipliers(market, vol_score, regime_factor, score_v2)

            # Nuevos stop / target
            new_stop   = round(precio - atr_val * mult["stop"], 2)
            new_target = round(precio + atr_val * mult["target"], 2)

            # Sanity check R/R
            rr = (new_target - precio) / max(precio - new_stop, 0.01)
            if rr < MIN_RR:
                # Ampliar target para mantener mínimo R/R
                new_target = round(precio + (precio - new_stop) * MIN_RR, 2)

            # Exit score
            exit_sc, exit_rec = _calc_exit_score(sig, serie, regime)

            # Actualizar el signal
            sig["atr_stop"]             = new_stop
            sig["atr_target"]           = new_target
            sig["stop_mult_used"]       = round(mult["stop"], 2)
            sig["target_mult_used"]     = round(mult["target"], 2)
            sig["exit_score"]           = exit_sc
            sig["exit_recommendation"]  = exit_rec
            sig["regime_factor_used"]   = regime_factor

            enriched += 1

        except Exception as e:
            logger.debug(f"[exit_model] {sig.get('ticker','?')}: {e}")
            continue

    logger.info(
        f"[exit_model] Régimen={regime} (factor={regime_factor}) | "
        f"{enriched}/{len(signals)} señales enriquecidas"
    )
    return signals


# ── Multiplicadores dinámicos ──────────────────────────────────────────────

def _dynamic_multipliers(market: str, vol_score: float, regime_factor: float, score_v2: float = 50.0) -> dict:
    """
    Calcula multiplicadores de stop y target ajustados por volatilidad, régimen
    y fuerza de la señal V2 (mejora 4.2: señal fuerte → stop más holgado,
    señal débil → stop más ajustado).
    """
    base = BASE_MULT.get(market, DEFAULT_MULT).copy()

    # Ajuste por volatilidad
    stop_scale   = _lookup_scale(VOL_STOP_SCALE, vol_score)
    target_scale = _lookup_scale(VOL_TARGET_SCALE, vol_score)
    strength_scale = _lookup_scale(SIGNAL_STRENGTH_STOP_SCALE, score_v2)

    stop_mult   = base["stop"]   * stop_scale   * regime_factor * strength_scale
    target_mult = base["target"] * target_scale

    # Bounds razonables: stop 1.0–3.5 ATR, target 1.5–6.0 ATR
    stop_mult   = max(1.0, min(3.5, stop_mult))
    target_mult = max(1.5, min(6.0, target_mult))

    return {"stop": round(stop_mult, 2), "target": round(target_mult, 2)}


def _lookup_scale(scale_dict: dict, value: float) -> float:
    """Busca el factor en un dict de rangos {(min,max): factor}."""
    for (lo, hi), factor in scale_dict.items():
        if lo <= value < hi:
            return factor
    return 1.0


# ── Exit Score ─────────────────────────────────────────────────────────────

def _calc_exit_score(sig: dict, serie: Optional[pd.Series], regime: str) -> tuple[float, str]:
    """
    Exit score 0-100: cuánto urge salir / reducir posición.
      0-30:  Mantener — no hay señales de salida
      31-55: Monitorear — señales débiles
      56-75: Considerar reducción parcial
      76-100: Salida urgente

    Componentes:
      1. RSI overbought (>72): señal de reversión
      2. V2 deterioro: score_v2 < 40 → activo se debilitando
      3. Señal V1/V2 de venta
      4. Régimen risk-off: sube la urgencia de salida
      5. Predictor bajista para 21d
    """
    score = 0.0
    reasons = []

    rsi       = float(sig.get("rsi", 50) or 50)
    score_v2  = float(sig.get("score_final_v2", 50) or 50)
    signal    = sig.get("signal", "")
    signal_v2 = sig.get("signal_v2", "")
    pred_21d  = sig.get("pred_21d")
    ret_anual = float(sig.get("ret_anual", 0) or 0)

    # 1. RSI sobrecompra
    if rsi > 78:
        score += 25
        reasons.append(f"RSI {rsi:.0f} (sobrecompra)")
    elif rsi > 72:
        score += 15
        reasons.append(f"RSI {rsi:.0f} (zona de cautela)")

    # 2. Score V2 bajo (activo perdiendo fuerza)
    if score_v2 < 35:
        score += 25
        reasons.append(f"V2={score_v2:.0f} (activo débil)")
    elif score_v2 < 42:
        score += 12
        reasons.append(f"V2={score_v2:.0f} (debilitando)")

    # 3. Señal de venta explícita
    if "VENTA" in signal_v2:
        score += 30
        reasons.append(f"Señal V2: {signal_v2}")
    elif "VENTA" in signal:
        score += 20
        reasons.append(f"Señal V1: {signal}")

    # 4. Predictor bajista
    if pred_21d is not None:
        if pred_21d < -10:
            score += 25
            reasons.append(f"Predictor 21d: {pred_21d:.1f}% (baja fuerte)")
        elif pred_21d < -3:
            score += 12
            reasons.append(f"Predictor 21d: {pred_21d:.1f}% (bajista)")

    # 5. Régimen risk-off amplifica urgencia
    if regime == "RISK_OFF" and score > 20:
        bonus = min(score * 0.25, 15)
        score += bonus
        reasons.append("Risk-off global")

    # 6. Indicadores técnicos de la serie (si disponible)
    if serie is not None and len(serie) >= 20:
        try:
            price_now = float(serie.iloc[-1])
            ma20 = float(serie.rolling(20).mean().iloc[-1])
            if price_now < ma20 * 0.98:  # Precio bajo MA20 con margen
                score += 15
                reasons.append("Precio < MA20")
        except Exception:
            pass

    # Clamp
    score = round(min(100, max(0, score)), 1)

    # Recomendación textual
    if score >= 76:
        rec = "🔴 Salida urgente"
    elif score >= 56:
        rec = "🟠 Reducir posición"
    elif score >= 31:
        rec = "🟡 Monitorear señales"
    else:
        rec = "🟢 Mantener"

    if reasons:
        rec += f" ({', '.join(reasons[:2])})"

    return score, rec


# ── Construcción del índice de precios ─────────────────────────────────────

def _build_price_series(price_data: dict, ticker_cols: dict) -> dict:
    """
    Retorna {ticker: pd.Series} para series de precios por ticker.
    """
    result = {}
    col_to_ticker = {v: k for k, v in ticker_cols.items()}

    for market_key, df in price_data.items():
        if df is None or df.empty:
            continue
        for col in df.columns:
            ticker = col_to_ticker.get(col, col)
            serie = df[col].dropna()
            if len(serie) >= 5:
                result[ticker] = serie
                if col != ticker:
                    result[col] = serie

    return result
