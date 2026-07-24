"""
src/portfolio_optimizer.py — Fase 4: Portfolio Optimizer

Convierte señales de compra en recomendaciones de asignación de capital.

METODOLOGÍA:
  Combina dos enfoques complementarios:

  1. Kelly Criterion (fraccionado al 50%)
     f* = (win_rate × avg_win - loss_rate × avg_loss) / avg_win
     Usa métricas reales del backtester por tipo de señal y mercado.
     Si no hay datos de backtest, usa win_rate conservador del score.
     Escalado por regime_factor (Prioridad 5, roadmap externo, 25/06/2026):
     LOW vol → ×1.10 (algo más agresivo), HIGH vol → ×0.75 (más
     conservador) -- ver compute_volatility_regime() en volatility_regime.py.
     Ese módulo prometía esta integración en su propio docstring desde que
     se creó, pero nunca se conectó (verificado con grep en todo el repo:
     cero referencias a regime_factor en este archivo antes de este fix) --
     exit_model.py y confidence_score.py sí lo consumían, este no.

  2. Risk Parity (1/volatilidad normalizado)
     Activos más volátiles reciben menos capital.
     Usa volatility_score y ATR del modelo. No se reescala por regime_factor
     a propósito: ya captura volatilidad POR ACTIVO; regime_factor es la
     dimensión de incertidumbre SISTÉMICA (todo el mercado), una señal
     complementaria, no redundante.

  PESO FINAL:
     suggested_pct = Kelly × 0.60 + RiskParity × 0.40
     (el componente Kelly de este blend se normaliza a suma=1 entre las
     señales del día -- por diseño, regime_factor NO cambia la proporción
     RELATIVA entre tickers acá adentro, solo el tamaño ABSOLUTO de
     kelly_f/kelly_half por señal, porque se cancela en esta normalización.
     Escalar también el tamaño TOTAL de capital deployado es una decisión
     de portfolio distinta -- eso es lo que hace exposure_factor
     [confidence_score.apply_exposure_factor(), activado 25/06/2026] como
     paso POSTERIOR a este módulo, sobre kelly_f/kelly_half/suggested_pct
     ya calculados. Ver pipeline.py para el orden exacto.)

RESTRICCIONES:
  • Max por posición: MAX_POS_PCT (15%)
  • Max por mercado: MAX_MARKET_PCT (40%)
  • Max por sector: MAX_SECTOR_PCT (25%)
  • Solo señales COMPRA o COMPRA FUERTE
  • Posiciones ya en cartera reciben menor asignación adicional
  • Kelly cappeado a 20% por posición SIEMPRE después de aplicar
    regime_factor -- el cap de riesgo no se relaja nunca por estar en
    régimen de baja volatilidad.

OUTPUT por señal (campos agregados al signal dict):
  kelly_f              → fracción Kelly pura (ya incluye regime_factor)
  kelly_half           → Kelly al 50% (conservador)
  risk_parity_pct      → peso por riesgo inverso
  suggested_pct        → recomendación final combinada
  allocation_cap       → limitación aplicada (max_pos / max_market / max_sector)
  allocation_notes     → texto explicativo

USO desde pipeline.py:
    from src.portfolio_optimizer import optimize_portfolio_allocation
    all_signals = optimize_portfolio_allocation(
        all_signals, backtest_results,
        regime_factor=vol_regime.get("regime_factor", 1.0),
    )
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
    price_data: dict = None,
    ticker_cols: dict = None,
    regime_factor: float = 1.0,
) -> list[dict]:
    """
    Enriquece cada señal de compra con recomendaciones de asignación de capital.
    Las señales no-compra no se modifican.

    Args:
        signals:          lista de dicts de señales del pipeline
        backtest_results: resultado de run_backtest() (opcional, mejora Kelly)
        regime_factor:    multiplicador de volatility_regime.compute_volatility_regime()
                          (LOW=1.10, NORMAL=1.00, HIGH=0.75). Escala kelly_f/
                          kelly_half por incertidumbre sistémica del mercado,
                          ver docstring del módulo.
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

    logger.info(f"[portfolio_optimizer] {len(buy_signals)} señales de compra a optimizar "
                f"(regime_factor={regime_factor})")

    # 1. Kelly por señal
    kelly_weights = _calc_kelly_weights(buy_signals, backtest_results, regime_factor=regime_factor)

    # 2. Risk parity por señal
    rp_weights = _calc_risk_parity_weights(buy_signals)

    # 3. Blend y aplicar restricciones
    final_weights = _blend_and_cap(
        buy_signals, kelly_weights, rp_weights, existing_tickers,
        price_data=price_data, ticker_cols=ticker_cols, regime_factor=regime_factor,
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

def _calc_kelly_weights(buy_signals: list[dict], backtest: dict, regime_factor: float = 1.0) -> list[float]:
    """
    Calcula Kelly fraction para cada señal de compra.
    Retorna lista de fracciones raw (0-1) en mismo orden que buy_signals.

    regime_factor: multiplicador de incertidumbre sistémica (Prioridad 5,
    roadmap externo, 25/06/2026) -- LOW vol=1.10, NORMAL=1.00, HIGH vol=0.75.

    FIX 24/07/2026: (1) usa el mejor horizonte disponible en vez de h21d
    hardcodeado -- ver docstring de _get_backtest_metrics_best_horizon()
    para el porqué. (2) Blendea el win_rate del bucket (por tipo de señal/
    mercado, agregado) con la probabilidad calibrada isotónicamente para
    el confidence_score PUNTUAL de esta señal (roadmap "Institucional PRO"
    -- calibración construida el 24/07 pero hasta este fix quedaba
    calculada y guardada sin usarse en ninguna decisión real). El bucket
    da "cómo le fue en promedio a este tipo de señal"; la curva calibrada
    da "qué tan bien predice el confidence_score de ESTA señal en
    particular" -- son dos fuentes de información distintas y
    complementarias, no una reemplaza a la otra.
    """
    from src.backtester import calibrated_win_probability

    weights = []
    calibration_curve = ((backtest or {}).get("confidence_calibration_curve") or {}).get("curva")

    # Extraer métricas de backtest por señal/mercado
    by_signal = backtest.get("by_signal", {}) if backtest else {}
    by_market = backtest.get("by_market", {}) if backtest else {}

    for sig in buy_signals:
        signal_key = sig.get("signal_v2") or sig.get("signal", "")
        market     = sig.get("mercado", "")
        score_v1   = float(sig.get("score_final", 0) or 0)

        # Buscar métricas en backtest (señal específica → mercado → fallback)
        _, metrics = _get_backtest_metrics_best_horizon(by_signal, signal_key)
        if metrics is None:
            _, metrics = _get_backtest_metrics_best_horizon(by_market, market)

        if metrics and metrics.get("samples", 0) >= 5:
            win_rate = float(metrics["win_rate"])
            avg_win  = float(metrics["avg_win"])
            avg_loss = float(metrics["avg_loss"])

            # Blend con la probabilidad calibrada isotónicamente para el
            # confidence_score puntual de esta señal, si hay curva
            # disponible (necesita ≥30 trades en el backtest para existir
            # -- ver _isotonic_calibration_curve() en backtester.py).
            confidence_score = sig.get("confidence_score")
            if calibration_curve and confidence_score is not None:
                p_calibrada = calibrated_win_probability(float(confidence_score), calibration_curve)
                if p_calibrada is not None:
                    win_rate = (win_rate + p_calibrada) / 2.0

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

        # Régimen de volatilidad sistémica (Prioridad 5, roadmap externo):
        # se aplica DESPUÉS del ajuste de confianza estadística y ANTES del
        # cap de riesgo -- el cap de 20% es una pared absoluta, nunca se
        # relaja por estar en régimen de baja volatilidad.
        kelly_f = kelly_f * regime_factor

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


def _get_backtest_metrics_best_horizon(group: dict, key: str) -> tuple:
    """
    FIX 24/07/2026 (hallazgo real, no teórico): _calc_kelly_weights() estaba
    hardcodeado a horizon="h21d" -- con solo 16 días de historia real, h21d
    sigue en null para TODO, así que Kelly caía siempre al fallback crudo
    por score (kelly_f = (score-50)/250), ignorando por completo el
    backtest real ya disponible a 5 y 10 días. Mismo patrón de bug ya
    encontrado y corregido 3 veces esta sesión en otros módulos
    (confidence_quantiles, ranking_top_vs_rest, log_model_run) -- acá vivía
    sin corregir en el módulo que más importa (el que decide cuánto
    capital poner en cada posición).

    Elige el horizonte más largo con muestra disponible (21d > 10d > 5d),
    igual criterio que _best_ret() en backtester.py.

    Devuelve (horizonte_usado, metrics) o (None, None).
    """
    for horizon in ("h21d", "h10d", "h5d"):
        m = _get_backtest_metrics(group, key, horizon)
        if m is not None:
            return horizon, m
    return None, None


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

def _covariance_adjustment(
    buy_signals: list[dict],
    price_data: dict,
    ticker_cols: dict,
) -> list[float]:
    """
    Calcula pesos ajustados por covarianza rolling (60 días).
    Implementación: weights ∝ diagonal de inv(cov_matrix) (Risk Parity mejorado).
    Retorna lista de pesos normalizados, en el mismo orden que buy_signals.

    Si falla (pocas observaciones, tickers no encontrados), retorna pesos iguales.
    """
    n = len(buy_signals)
    if n == 0:
        return []
    if n == 1:
        return [1.0]

    try:
        import numpy as np
        col_to_ticker = {v: k for k, v in ticker_cols.items()}
        COV_WINDOW    = 60

        # Construir matriz de retornos (tickers × fechas)
        returns_dict = {}
        for _, df in price_data.items():
            if df is None or df.empty:
                continue
            for col in df.columns:
                ticker = col_to_ticker.get(col, col)
                series = df[col].pct_change(fill_method=None).dropna()
                if len(series) >= COV_WINDOW:
                    returns_dict[ticker] = series.tail(COV_WINDOW).values

        # Extraer retornos de los tickers que vamos a comprar
        rets_matrix = []
        valid_idx   = []
        for i, sig in enumerate(buy_signals):
            t = sig.get("ticker", "")
            r = returns_dict.get(t)
            if r is not None and len(r) == COV_WINDOW:
                rets_matrix.append(r)
                valid_idx.append(i)

        if len(valid_idx) < 2:
            return [1.0 / n] * n  # fallback: pesos iguales

        R   = np.array(rets_matrix)          # shape: (n_valid, 60)
        cov = np.cov(R)                       # covarianza 60d
        if cov.ndim == 0:  # caso n_valid==1 ya cubierto arriba, pero por si acaso
            return [1.0 / n] * n

        # Regularización (Ledoit-Wolf simplificado): shrink toward diagonal
        diag   = np.diag(np.diag(cov))
        cov_s  = 0.7 * cov + 0.3 * diag     # shrinkage factor

        # Minimum-variance weights: w ∝ inv(Σ)·1 — a diferencia de usar solo
        # la diagonal (= inverse-variance puro, ignora correlación por completo),
        # esto SÍ penaliza pares de activos correlacionados: dos posiciones con
        # alta correlación positiva reciben menos peso conjunto que si fueran
        # independientes, porque inv(Σ) captura la covarianza cruzada, no solo
        # la varianza individual. (Mejora 4.1 — antes esta función calculaba cov_s
        # con shrinkage pero solo usaba np.diag(cov_s), que es matemáticamente
        # idéntico a np.diag(cov) sin shrinkage: la correlación se calculaba y
        # se descartaba sin afectar el resultado.)
        try:
            ones = np.ones(len(valid_idx))
            inv_cov = np.linalg.pinv(cov_s)  # pseudo-inversa: más robusta que inv() si está mal condicionada
            w_raw = inv_cov @ ones
            if np.any(w_raw < 0):
                # Minimum-variance sin restricciones puede dar pesos negativos
                # (posiciones "short" implícitas) — no aplica para un portfolio
                # long-only de acciones, así que se clampea a 0 y se renormaliza.
                w_raw = np.maximum(w_raw, 0)
            if w_raw.sum() <= 0:
                raise ValueError("pesos minimum-variance degenerados")
            w_raw = w_raw / w_raw.sum()
        except Exception:
            # Fallback: inverse-variance puro (el comportamiento viejo)
            diag_inv = 1.0 / np.maximum(np.diag(cov_s), 1e-10)
            w_raw    = diag_inv / diag_inv.sum()

        # Mapear de vuelta a posiciones originales
        weights = [1.0 / n] * n  # fallback para los que no tienen datos
        for idx_local, idx_orig in enumerate(valid_idx):
            weights[idx_orig] = float(w_raw[idx_local])

        # Renormalizar
        total = sum(weights)
        if total > 0:
            weights = [w / total for w in weights]

        return weights

    except Exception as e:
        logger.debug(f"[portfolio] Covarianza fallback: {e}")
        return [1.0 / n] * n


def _blend_and_cap(
    buy_signals: list[dict],
    kelly_weights: list[float],
    rp_weights: list[float],
    existing_tickers: set,
    price_data: dict = None,
    ticker_cols: dict = None,
    regime_factor: float = 1.0,
) -> dict:
    """
    Combina Kelly + RiskParity, convierte a porcentajes y aplica caps.
    Retorna dict {idx: {suggested_pct, kelly_f, kelly_half, risk_parity_pct, ...}}

    regime_factor: solo se usa acá para anotar el motivo en allocation_notes
    -- el efecto numérico real ya está aplicado en kelly_weights (vienen
    pre-escalados desde _calc_kelly_weights).
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

    # Covarianza (si hay datos disponibles)
    cov_weights = None
    if price_data and ticker_cols:
        cov_weights = _covariance_adjustment(buy_signals, price_data, ticker_cols)

    # Blend: Kelly + RiskParity + Covarianza (opcional)
    if cov_weights and len(cov_weights) == n:
        # Híbrido: 50% Kelly/RiskParity + 40% covarianza-ajustado + 10% buffer
        kr_base = [KELLY_WEIGHT * kelly_norm[i] + RISKP_WEIGHT * rp_weights[i] for i in range(n)]
        blended = [0.60 * kr_base[i] + 0.40 * cov_weights[i] for i in range(n)]
    else:
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
        notes = _build_notes(pct, cap_reason, kelly_weights[i], sig, regime_factor=regime_factor)

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


def _build_notes(pct: float, cap: Optional[str], kelly_f: float, sig: dict,
                  regime_factor: float = 1.0) -> str:
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

    if regime_factor > 1.0:
        parts.append(f"Kelly ampliado ×{regime_factor:.2f} (régimen de baja volatilidad).")
    elif regime_factor < 1.0:
        parts.append(f"Kelly recortado ×{regime_factor:.2f} (régimen de alta volatilidad).")

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
