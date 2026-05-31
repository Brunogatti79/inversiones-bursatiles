"""
src/portfolio_optimizer.py — Fase 4: Portfolio Optimizer

Convierte señales de compra en recomendaciones de asignación de capital.

METODOLOGÍA:
  Combina dos enfoques complementarios:

  1. Kelly Criterion (fraccionado al 50%)
     f* = (win_rate × avg_win - loss_rate × avg_loss) / avg_win
     Usa métricas reales del backtester por tipo de señal y mercado.
     Si no hay datos de backtest, usa win_rate conservador del score.

  2. Risk Parity (1/volatilidad normalizado)
     Activos más volátiles reciben menos capital.
     Usa volatility_score y ATR del modelo.

  PESO FINAL:
     suggested_pct = Kelly × 0.60 + RiskParity × 0.40

RESTRICCIONES:
  • Max por posición: MAX_POS_PCT (15%)
  • Max por mercado: MAX_MARKET_PCT (40%)
  • Max por sector: MAX_SECTOR_PCT (25%)
  • Solo señales COMPRA o COMPRA FUERTE
  • Posiciones ya en cartera reciben menor asignación adicional

OUTPUT por señal (campos agregados al signal dict):
  kelly_f              → fracción Kelly pura
  kelly_half           → Kelly al 50% (conservador)
  risk_parity_pct      → peso por riesgo inverso
  suggested_pct        → recomendación final combinada
  allocation_cap       → limitación aplicada (max_pos / max_market / max_sector)
  allocation_notes     → texto explicativo

USO desde pipeline.py:
    from src.portfolio_optimizer import optimize_portfolio_allocation
    all_signals = optimize_portfolio_allocation(all_signals, backtest_results)
"""

import json
import os
import logging
import numpy as np
from typing import Optional

logger = logging.getLogger(__name__)

BACKTEST_PATH = "data/backtest_results.json"
PORTFOLIO_PATH = "data/portfolio.json"

# ── Límites de concentración ───────────────────────────────────────────────
MAX_POS_PCT    = 15.0   # máximo por posición individual
MAX_MARKET_PCT = 40.0   # máximo en un solo mercado
MAX_SECTOR_PCT = 25.0   # máximo en un solo sector
MIN_POS_PCT    =  2.0   # mínimo para ser accionable (filtrar ruido)

# Peso del blend Kelly/RiskParity
KELLY_WEIGHT = 0.60
RISKP_WEIGHT = 0.40

# Descuento por posición ya existente en cartera
EXISTING_POSITION_DISCOUNT = 0.50   # si ya tengo el ticker, asignar 50% de lo normal

# Señales elegibles para allocación
BUY_SIGNALS = {"⭐ COMPRA FUERTE", "🟢 COMPRA"}


# ── Entrypoint principal ────────────────────────────────────────────────────

def optimize_portfolio_allocation(
    signals: list[dict],
    backtest_results: Optional[dict] = None,
) -> list[dict]:
    """
    Enriquece cada señal de compra con recomendaciones de asignación de capital.
    Las señales no-compra no se modifican.

    Args:
        signals:          lista de dicts de señales del pipeline
        backtest_results: resultado de run_backtest() (opcional, mejora Kelly)
    """
    if not signals:
        return signals

    # Cargar backtest si no se pasó
    if backtest_results is None:
        backtest_results = _load_backtest()

    # Cargar posiciones actuales del portfolio
    existing_tickers = _load_existing_tickers()

    # Filtrar señales de compra
    buy_signals = [s for s in signals if s.get("signal_v2") in BUY_SIGNALS
                   or s.get("signal") in BUY_SIGNALS]

    if not buy_signals:
        return signals

    logger.info(f"[portfolio_optimizer] {len(buy_signals)} señales de compra a optimizar")

    # 1. Kelly por señal
    kelly_weights = _calc_kelly_weights(buy_signals, backtest_results)

    # 2. Risk parity por señal
    rp_weights = _calc_risk_parity_weights(buy_signals)

    # 3. Blend y aplicar restricciones
    final_weights = _blend_and_cap(
        buy_signals, kelly_weights, rp_weights, existing_tickers
    )

    # 4. Actualizar signal dicts
    weight_map = {s["ticker"]: final_weights.get(i, {})
                  for i, s in enumerate(buy_signals)}

    for sig in signals:
        if sig.get("signal_v2") in BUY_SIGNALS or sig.get("signal") in BUY_SIGNALS:
            alloc = weight_map.get(sig["ticker"], {})
            sig.update(alloc)

    _log_allocation_summary(buy_signals, final_weights)
    return signals


# ── Kelly Criterion ─────────────────────────────────────────────────────────

def _calc_kelly_weights(buy_signals: list[dict], backtest: dict) -> list[float]:
    """
    Calcula Kelly fraction para cada señal de compra.
    Retorna lista de fracciones raw (0-1) en mismo orden que buy_signals.
    """
    weights = []

    # Extraer métricas de backtest por señal/mercado
    by_signal = backtest.get("by_signal", {}) if backtest else {}
    by_market = backtest.get("by_market", {}) if backtest else {}

    for sig in buy_signals:
        signal_key = sig.get("signal_v2") or sig.get("signal", "")
        market     = sig.get("mercado", "")
        score_v1   = float(sig.get("score_final", 0) or 0)

        # Buscar métricas en backtest (señal específica → mercado → fallback)
        metrics = (
            _get_backtest_metrics(by_signal, signal_key, horizon="h21d") or
            _get_backtest_metrics(by_market, market, horizon="h21d") or
            None
        )

        if metrics and metrics.get("samples", 0) >= 5:
            win_rate = float(metrics["win_rate"])
            avg_win  = float(metrics["avg_win"])
            avg_loss = float(metrics["avg_loss"])
            if avg_win > 0:
                kelly_f = (win_rate * avg_win - (1 - win_rate) * avg_loss) / avg_win
            else:
                kelly_f = 0.0
        else:
            # Fallback basado en score: score 70 → Kelly ~0.08, score 58 → ~0.04
            kelly_f = max(0.0, (score_v1 - 50) / 250)

        # Confidence-adjusted Kelly: f* = Kelly × min(1, sqrt(n/100))
        # Penaliza Kelly cuando hay poca historia estadística (< 30 trades → máx 55% Kelly)
        n_samples = metrics.get("samples", 0) if metrics else 0
        conf_adj  = min(1.0, (max(1, n_samples) / 100) ** 0.5)
        kelly_f   = kelly_f * conf_adj

        # Clamp: Kelly entre 0 y 0.20 (máximo 20% de capital en una posición)
        kelly_f = max(0.0, min(0.20, kelly_f))
        weights.append(kelly_f)

    return weights


def _get_backtest_metrics(group: dict, key: str, horizon: str) -> Optional[dict]:
    """Extrae métricas de backtest para una clave y horizonte."""
    if key not in group:
        return None
    data = group[key]
    h = data.get(horizon)
    if not h or h.get("samples", 0) < 5:
        return None
    return h


# ── Risk Parity ─────────────────────────────────────────────────────────────

def _calc_risk_parity_weights(buy_signals: list[dict]) -> list[float]:
    """
    Pesos inversos a volatilidad normalizados a suma = 1.
    Activos más volátiles reciben menos capital.
    """
    if not buy_signals:
        return []

    # Usar volatility_score (0-100) del exit_model si disponible,
    # sino derivar del ATR como % del precio
    vol_scores = []
    for sig in buy_signals:
        vs = float(sig.get("volatility_score", 50) or 50)
        vol_scores.append(max(1.0, vs))

    # Peso inversamente proporcional a volatilidad
    inv_vols = [1.0 / v for v in vol_scores]
    total    = sum(inv_vols)
    if total <= 0:
        return [1.0 / len(buy_signals)] * len(buy_signals)

    return [iv / total for iv in inv_vols]


# ── Blend y restricciones ────────────────────────────────────────────────────

def _blend_and_cap(
    buy_signals: list[dict],
    kelly_weights: list[float],
    rp_weights: list[float],
    existing_tickers: set,
) -> dict:
    """
    Combina Kelly + RiskParity, convierte a porcentajes y aplica caps.
    Retorna dict {idx: {suggested_pct, kelly_f, kelly_half, risk_parity_pct, ...}}
    """
    n = len(buy_signals)
    if n == 0:
        return {}

    # Normalizar Kelly a suma = 1
    kelly_sum = sum(kelly_weights)
    if kelly_sum > 0:
        kelly_norm = [k / kelly_sum for k in kelly_weights]
    else:
        kelly_norm = [1.0 / n] * n

    # Blend
    blended = [
        KELLY_WEIGHT * kelly_norm[i] + RISKP_WEIGHT * rp_weights[i]
        for i in range(n)
    ]

    # Convertir a % del capital total (suma = 100%, pero capreada)
    # Escalar para que la señal más fuerte no supere MAX_POS_PCT
    max_blend = max(blended)
    if max_blend > 0:
        scale = MAX_POS_PCT / (max_blend * 100)
        pcts  = [b * 100 * scale for b in blended]
    else:
        pcts = [MAX_POS_PCT / n] * n

    # Pre-calcular concentración sectorial (correlación implícita intra-portfolio)
    sector_counts: dict[str, int] = {}
    for _sig in buy_signals:
        _s = _sig.get("sector", "UNKNOWN") or "UNKNOWN"
        sector_counts[_s] = sector_counts.get(_s, 0) + 1

    # Tracking de caps por mercado/sector
    market_used: dict[str, float] = {}
    sector_used: dict[str, float] = {}
    result = {}

    for i, (sig, pct) in enumerate(zip(buy_signals, pcts)):
        ticker  = sig.get("ticker", "")
        market  = sig.get("mercado", "UNKNOWN")
        sector  = sig.get("sector", "UNKNOWN")
        cap_reason = None

        # Descuento si ya en portfolio
        if ticker in existing_tickers:
            pct = pct * EXISTING_POSITION_DISCOUNT
            cap_reason = "ya_en_cartera"

        # Penalización por cluster sectorial (correlación implícita)
        sector_count = sector_counts.get(sector, 1)
        if sector_count >= 3:
            cluster_factor = max(0.60, 1.0 - (sector_count - 2) * 0.15)
            pct = pct * cluster_factor
            if cap_reason is None:
                cap_reason = f"cluster_{sector[:6]}"

        # Cap por posición
        if pct > MAX_POS_PCT:
            pct = MAX_POS_PCT
            cap_reason = "max_posicion"

        # Cap por mercado
        market_used[market] = market_used.get(market, 0) + pct
        if market_used[market] > MAX_MARKET_PCT:
            overflow = market_used[market] - MAX_MARKET_PCT
            pct = max(0, pct - overflow)
            market_used[market] = MAX_MARKET_PCT
            cap_reason = "max_mercado"

        # Cap por sector
        sector_used[sector] = sector_used.get(sector, 0) + pct
        if sector_used[sector] > MAX_SECTOR_PCT:
            overflow = sector_used[sector] - MAX_SECTOR_PCT
            pct = max(0, pct - overflow)
            sector_used[sector] = MAX_SECTOR_PCT
            cap_reason = "max_sector"

        pct = round(pct, 1)

        # Nota
        notes = _build_notes(pct, cap_reason, kelly_weights[i], sig)

        result[i] = {
            "kelly_f":          round(kelly_weights[i], 4),
            "kelly_half":       round(kelly_weights[i] * 0.5 * 100, 1),  # % capital
            "risk_parity_pct":  round(rp_weights[i] * 100, 1),
            "suggested_pct":    pct,
            "allocation_cap":   cap_reason or "none",
            "allocation_notes": notes,
            "is_actionable":    pct >= MIN_POS_PCT,
        }

    return result


def _build_notes(pct: float, cap: Optional[str], kelly_f: float, sig: dict) -> str:
    """Genera texto explicativo de la recomendación."""
    signal = sig.get("signal_v2") or sig.get("signal", "")
    score  = sig.get("score_final", 0)
    rr     = sig.get("rr_ratio", 0)

    parts = [f"Allocar {pct}% del capital."]

    if kelly_f > 0.05:
        parts.append(f"Kelly ({kelly_f:.1%}) sugiere posición relevante.")
    elif kelly_f > 0:
        parts.append("Kelly sugiere posición pequeña (señal débil en backtest).")
    else:
        parts.append("Sin datos backtest — posición conservadora.")

    if rr and rr > 1.5:
        parts.append(f"R/R={rr:.1f}x favorable.")

    if cap == "ya_en_cartera":
        parts.append("Descuento: ticker ya en cartera.")
    elif cap == "max_posicion":
        parts.append("Capreado al 15% por concentración.")
    elif cap == "max_mercado":
        parts.append("Capreado por límite de mercado (40%).")
    elif cap == "max_sector":
        parts.append("Capreado por límite de sector (25%).")

    if pct < MIN_POS_PCT:
        parts.append("⚠️ Por debajo del mínimo accionable — considerar no abrir.")

    return " ".join(parts)


# ── Helpers ─────────────────────────────────────────────────────────────────

def _load_backtest() -> dict:
    if not os.path.exists(BACKTEST_PATH):
        return {}
    try:
        with open(BACKTEST_PATH) as f:
            return json.load(f)
    except Exception:
        return {}


def _load_existing_tickers() -> set:
    """Carga tickers actualmente en cartera."""
    if not os.path.exists(PORTFOLIO_PATH):
        return set()
    try:
        with open(PORTFOLIO_PATH) as f:
            port = json.load(f)
        return {p["ticker"] for p in port.get("positions", [])}
    except Exception:
        return set()


def _log_allocation_summary(buy_signals: list[dict], final_weights: dict):
    logger.info("══════ PORTFOLIO ALLOCATION ══════")
    for i, sig in enumerate(buy_signals):
        alloc = final_weights.get(i, {})
        pct   = alloc.get("suggested_pct", 0)
        kelly = alloc.get("kelly_f", 0)
        flag  = "✅" if alloc.get("is_actionable") else "⚠️"
        logger.info(
            f"  {flag} {sig['ticker']:<12} {sig.get('mercado',''):<8} "
            f"→ {pct:>4.1f}% | Kelly={kelly:.1%} | "
            f"Score={sig.get('score_final',0):.0f} | "
            f"RR={sig.get('rr_ratio',0):.1f}"
        )
    total = sum(w.get("suggested_pct", 0) for w in final_weights.values())
    logger.info(f"  Total asignado: {total:.1f}% | Señales: {len(buy_signals)}")
    logger.info("══════════════════════════════════")
