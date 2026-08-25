"""
src/cross_market.py — Fase 1: Correlación Cross-Market

Detecta el régimen de mercado global y calcula ajustes al score macro
basados en la influencia del S&P 500 sobre MERVAL y BOVESPA.

LÓGICA:
  • El S&P 500 es el mercado líder global. Su tendencia impacta a emergentes.
  • Si SP500 alcista → MERVAL/BOVESPA reciben un boost macro moderado (máx +6).
  • Si SP500 bajista  → penalización moderada (máx -6).
  • Si los 3 mercados divergen (baja correlación) → señal idiosincrática,
    los ajustes se reducen (el mercado está respondiendo a drivers locales).
  • El ajuste se aplica al macro_score ANTES de analyze_market en pipeline.

OUTPUT:
  {
    "regime": "RISK_ON" | "RISK_OFF" | "NEUTRAL",
    "sp500_trend": "ALCISTA" | "BAJISTA" | "LATERAL",
    "sp500_trend_score": 72.4,   # 0-100
    "correlations": {
        "merval_sp500": 0.72,
        "bovespa_sp500": 0.61,
        "merval_bovespa": 0.68,
        "avg": 0.67,
    },
    "divergence": {
        "merval_diverge": False,   # MERVAL se mueve distinto a SP500
        "bovespa_diverge": False,
        "global_divergence": False,
    },
    "score_adjustments": {
        "MERVAL": +3.2,    # se suma al macro_score
        "BOVESPA": +2.1,
        "SP500": 0.0,      # SP500 no se autoajusta
    },
    "narrative": "Mercados convergentes. S&P 500 en tendencia alcista ...",
  }

Uso desde pipeline.py:
    from src.cross_market import compute_cross_market_context
    cross = compute_cross_market_context(merval_df, bovespa_df, sp500_df, index_cols)
    # Aplicar ajustes al macro antes de analyze_market:
    for market, adj in cross["score_adjustments"].items():
        if market in macro_scores:
            macro_scores[market] = round(macro_scores[market] + adj, 1)
    # Guardar en index_stats para el dashboard
    index_stats["cross_market"] = cross
"""

import logging
import numpy as np
import pandas as pd
from typing import Optional

logger = logging.getLogger(__name__)

# ── Parámetros ─────────────────────────────────────────────────────────────
CORR_WINDOW      = 21    # días para la correlación rolling
TREND_WINDOW     = 20    # días para detectar tendencia
DIVERGE_THRESH   = 0.20  # correlación < umbral → divergencia
CONVERGE_THRESH  = 0.55  # correlación > umbral → convergencia

# Máximo ajuste al macro_score por influencia cross-market
MAX_BOOST        = 6.0   # puntos positivos
MAX_PENALTY      = 6.0   # puntos negativos (se aplica como negativo)

# Peso de la influencia del SP500 sobre emergentes (empírico)
SP500_INFLUENCE_ON_MERVAL  = 0.65   # MERVAL es más volátil, reacciona más
SP500_INFLUENCE_ON_BOVESPA = 0.75   # BOVESPA más correlado históricamente


# ── Entrypoint principal ───────────────────────────────────────────────────

def compute_cross_market_context(
    merval_df: pd.DataFrame,
    bovespa_df: pd.DataFrame,
    sp500_df: pd.DataFrame,
    index_cols: dict,       # {"merval": col, "bovespa": col, "sp500": col}
) -> dict:
    """
    Calcula el contexto cross-market completo y los ajustes al macro_score.
    """
    fallback = {
        "regime": "NEUTRAL",
        "sp500_trend": "LATERAL",
        "sp500_trend_score": 50.0,
        "market_trends": {
            "MERVAL":  {"trend": "LATERAL", "score": 50.0},
            "BOVESPA": {"trend": "LATERAL", "score": 50.0},
            "SP500":   {"trend": "LATERAL", "score": 50.0},
        },
        "correlations": {"merval_sp500": 0, "bovespa_sp500": 0, "merval_bovespa": 0, "avg": 0},
        "divergence": {"merval_diverge": False, "bovespa_diverge": False, "global_divergence": False},
        "score_adjustments": {"MERVAL": 0.0, "BOVESPA": 0.0, "SP500": 0.0},
        "score_adjustments_shadow_gate_local": {"MERVAL": 0.0, "BOVESPA": 0.0, "SP500": 0.0},
        "market_exposure_shadow": {"MERVAL": 1.0, "BOVESPA": 1.0, "SP500": 1.0},
        "narrative": "Sin datos suficientes para análisis cross-market.",
    }

    try:
        # Extraer series de índices
        mv_col = index_cols.get("merval", "")
        bv_col = index_cols.get("bovespa", "")
        sp_col = index_cols.get("sp500", "")

        mv_serie = _get_index_series(merval_df, mv_col)
        bv_serie = _get_index_series(bovespa_df, bv_col)
        sp_serie = _get_index_series(sp500_df, sp_col)

        if mv_serie is None or bv_serie is None or sp_serie is None:
            logger.warning("[cross_market] Una o más series de índice no disponibles")
            return fallback

        # Alinear fechas (usar intersección)
        combined = pd.DataFrame({
            "mv": mv_serie.pct_change(fill_method=None),
            "bv": bv_serie.pct_change(fill_method=None),
            "sp": sp_serie.pct_change(fill_method=None),
        }).dropna()

        if len(combined) < CORR_WINDOW + 5:
            logger.info(f"[cross_market] Datos insuficientes: {len(combined)} días")
            return fallback

        # 1. Tendencia SP500 (global, ya existía)
        sp_trend_score, sp_trend_label = _trend_score(sp_serie)

        # 1b. Tendencia por país (roadmap externo #8, jul-2026: "regime
        # detection", Bull/Neutral/Bear por mercado -- no para operar, para
        # interpretar señales). _trend_score() ya era genérica y ya se
        # llamaba sobre sp_serie; mv_serie/bv_serie ya estaban cargadas acá
        # mismo para el cálculo de correlación, solo faltaba llamarla
        # también sobre ellas.
        mv_trend_score, mv_trend_label = _trend_score(mv_serie)
        bv_trend_score, bv_trend_label = _trend_score(bv_serie)

        # 2. Correlaciones rolling (último valor)
        tail = combined.tail(CORR_WINDOW * 2)
        corr_mv_sp  = _rolling_corr_last(tail["mv"], tail["sp"])
        corr_bv_sp  = _rolling_corr_last(tail["bv"], tail["sp"])
        corr_mv_bv  = _rolling_corr_last(tail["mv"], tail["bv"])
        avg_corr    = float(np.nanmean([corr_mv_sp, corr_bv_sp, corr_mv_bv]))

        correlations = {
            "merval_sp500":   round(corr_mv_sp, 3),
            "bovespa_sp500":  round(corr_bv_sp, 3),
            "merval_bovespa": round(corr_mv_bv, 3),
            "avg":            round(avg_corr, 3),
        }

        # 3. Divergencias
        mv_diverge = corr_mv_sp < DIVERGE_THRESH
        bv_diverge = corr_bv_sp < DIVERGE_THRESH
        global_div = avg_corr < DIVERGE_THRESH

        divergence = {
            "merval_diverge":   mv_diverge,
            "bovespa_diverge":  bv_diverge,
            "global_divergence": global_div,
        }

        # 4. Régimen global
        regime = _determine_regime(sp_trend_score, avg_corr)

        # 5. Ajustes al score macro
        # FIX 24/08/2026 → REVERTIDO A SHADOW MODE el mismo día (auditoría
        # con Claude + 2 revisiones externas): el gate por tendencia local
        # es un mecanismo razonable, pero al medirlo contra el Top20/Bottom20
        # real del backtest se encontró que el 100% de las señales que
        # componen ese resultado histórico tienen cross_market_regime=UNKNOWN
        # (el campo recién se empezó a persistir a mediados de julio) -- o
        # sea, el mecanismo NO explica el problema medido, aunque siga
        # siendo plausible hacia adelante. "Mecanismo plausible + resultado
        # histórico desconectado = fix NO justificado todavía" (síntesis de
        # la revisión externa). Se aplica en producción el cálculo VIEJO
        # (sin gate, comportamiento idéntico al de antes de esta sesión) y
        # se calcula en paralelo la versión con gate SOLO para loguearla
        # (score_adjustments_shadow_gate_local) -- no se aplica a
        # macro_score todavía. Reactivar cuando haya semanas de historia
        # real con el shadow persistido para medir impacto en Top/Bottom.
        adjustments = _calc_score_adjustments(
            sp_trend_score, corr_mv_sp, corr_bv_sp, mv_diverge, bv_diverge,
        )
        adjustments_shadow = _calc_score_adjustments(
            sp_trend_score, corr_mv_sp, corr_bv_sp, mv_diverge, bv_diverge,
            mv_trend_score=mv_trend_score, bv_trend_score=bv_trend_score,
        )

        # 6. Narrativa
        narrative = _build_narrative(
            regime, sp_trend_label, sp_trend_score,
            correlations, divergence, adjustments
        )

        result = {
            "regime":           regime,
            "sp500_trend":      sp_trend_label,
            "sp500_trend_score": round(sp_trend_score, 1),
            # Regime detection por país (roadmap externo #8) -- Bull/Neutral/
            # Bear locales, distintos del "regime" global (que es sobre SP500
            # + correlación, y se usa para ajustar el score macro). Esto es
            # descriptivo/interpretativo, no se usa todavía para ajustar nada.
            "market_trends": {
                "MERVAL":  {"trend": mv_trend_label, "score": round(mv_trend_score, 1)},
                "BOVESPA": {"trend": bv_trend_label, "score": round(bv_trend_score, 1)},
                "SP500":   {"trend": sp_trend_label, "score": round(sp_trend_score, 1)},
            },
            "correlations":     correlations,
            "divergence":       divergence,
            "score_adjustments": adjustments,
            # ── Shadow mode (24/08/2026) -- NO se aplica a macro_score, solo
            # se persiste por señal para poder medir impacto real antes de
            # activar. Ver nota arriba y src/confidence_score.py para el
            # criterio de cuándo pasar esto a producción.
            "score_adjustments_shadow_gate_local": adjustments_shadow,
            # ── Shadow mode (25/08/2026) -- NO se aplica a Kelly/tamaño de
            # posición, solo se persiste por señal. Ver docstring de
            # compute_market_exposure_shadow() para la evidencia que lo
            # motivó (paradoja de Simpson en ranking_top_vs_rest global).
            "market_exposure_shadow": {
                "MERVAL":  compute_market_exposure_shadow(mv_trend_score),
                "BOVESPA": compute_market_exposure_shadow(bv_trend_score),
                "SP500":   compute_market_exposure_shadow(sp_trend_score),
            },
            "narrative":        narrative,
        }

        logger.info(
            f"[cross_market] Régimen={regime} | SP500={sp_trend_label}({sp_trend_score:.0f}) | "
            f"Corr avg={avg_corr:.2f} | Adj MERVAL={adjustments['MERVAL']:+.1f} BOVESPA={adjustments['BOVESPA']:+.1f} | "
            f"Shadow(gate local) MERVAL={adjustments_shadow['MERVAL']:+.1f} BOVESPA={adjustments_shadow['BOVESPA']:+.1f} | "
            f"Shadow(exposure) MERVAL={result['market_exposure_shadow']['MERVAL']:.2f} BOVESPA={result['market_exposure_shadow']['BOVESPA']:.2f}"
        )

        return result

    except Exception as e:
        logger.warning(f"[cross_market] Error en cálculo: {e}", exc_info=True)
        return fallback


# ── Helpers internos ───────────────────────────────────────────────────────

def _get_index_series(df: pd.DataFrame, col: str) -> Optional[pd.Series]:
    """Extrae la serie del índice del DataFrame. Busca por col exacto o keyword."""
    if df is None or df.empty:
        return None
    if col and col in df.columns:
        serie = df[col].dropna()
        return serie if len(serie) >= CORR_WINDOW else None
    # Fallback: buscar columna con keyword
    for c in df.columns:
        if any(kw in str(c).upper() for kw in ["MERVAL", "BOVESPA", "S&P", "SP500", "^"]):
            serie = df[c].dropna()
            if len(serie) >= CORR_WINDOW:
                return serie
    return None


def _trend_score(serie: pd.Series) -> tuple[float, str]:
    """
    Calcula un trend score 0-100 para la serie del índice.
    Combina: pendiente MA20, posición relativa precio vs MA20/MA50, momentum.
    """
    if len(serie) < 50:
        return 50.0, "LATERAL"

    prices = serie.dropna()
    price  = float(prices.iloc[-1])
    ma20   = float(prices.rolling(20).mean().iloc[-1])
    ma50   = float(prices.rolling(50).mean().iloc[-1])

    # Pendiente MA20 (% anualizado)
    ma20_series = prices.rolling(20).mean().dropna()
    if len(ma20_series) >= 10:
        slope = (float(ma20_series.iloc[-1]) - float(ma20_series.iloc[-10])) / float(ma20_series.iloc[-10]) * 100
    else:
        slope = 0.0

    # Momentum 21d
    if len(prices) >= 22:
        mom = (price / float(prices.iloc[-22]) - 1) * 100
    else:
        mom = 0.0

    # Score por componente (0-100)
    # Precio vs MAs
    above_ma20 = price > ma20
    above_ma50 = price > ma50
    ma_score   = (65 if above_ma20 and above_ma50 else
                  55 if above_ma20 else
                  45 if above_ma50 else 35)

    # Pendiente MA20
    slope_score = (75 if slope > 3 else
                   62 if slope > 1 else
                   50 if slope > -1 else
                   38 if slope > -3 else 25)

    # Momentum
    mom_score = (72 if mom > 8 else
                 60 if mom > 3 else
                 50 if mom > -3 else
                 40 if mom > -8 else 28)

    trend_score = ma_score * 0.40 + slope_score * 0.35 + mom_score * 0.25

    label = ("ALCISTA" if trend_score >= 58 else
             "BAJISTA" if trend_score <= 42 else "LATERAL")

    return round(trend_score, 1), label


def _rolling_corr_last(s1: pd.Series, s2: pd.Series, window: int = CORR_WINDOW) -> float:
    """Correlación rolling en la ventana y toma el último valor."""
    try:
        combined = pd.concat([s1, s2], axis=1).dropna()
        if len(combined) < window:
            return 0.0
        corr = combined.iloc[:, 0].rolling(window).corr(combined.iloc[:, 1]).iloc[-1]
        return float(corr) if np.isfinite(corr) else 0.0
    except Exception:
        return 0.0


def _determine_regime(sp_trend_score: float, avg_corr: float) -> str:
    """
    RISK_ON: SP500 alcista + mercados correlados (todos suben juntos)
    RISK_OFF: SP500 bajista
    NEUTRAL: entre medio
    """
    if sp_trend_score >= 58:
        return "RISK_ON"
    elif sp_trend_score <= 42:
        return "RISK_OFF"
    return "NEUTRAL"


def compute_market_exposure_shadow(market_trend_score: float | None) -> float:
    """
    🔵 SHADOW MODE (25/08/2026, auditoría con Claude) -- NO se aplica a
    ningún cálculo de capital todavía. Se calcula y se persiste por señal
    (ver pipeline.py/tracker.py) puramente para medir impacto antes de
    decidir activarlo, mismo criterio que score_adjustments_shadow_gate_local.

    Motivo: se encontró que ranking_top_vs_rest GLOBAL (que mezcla los 3
    mercados) mostraba una paradoja de Simpson -- MERVAL/BOVESPA rankean
    más alto en promedio que SP500 pese a tener peor EV real del período,
    así que el Top20% global queda dominado por señales de mercados que
    vienen mal. Verificado con ranking_top_vs_rest_by_market (backtester.py)
    que el ranking SÍ ordena razonablemente bien DENTRO de cada mercado
    (top20% > bottom20% en los 3). Eso sugiere que el problema no es la
    selección de acciones dentro de un mercado, sino que no existe ninguna
    señal de "este mercado completo viene mal, reducir exposición acá"
    independiente del ranking interno -- que es justo lo que
    market_trend_score (ya calculado por _trend_score(), sin uso downstream
    hasta ahora salvo el gate ya revertido de _calc_score_adjustments) podría
    aportar.

    No se pudo validar retroactivamente contra el backtest real: solo 195
    de 974 señales medibles tienen market_trend_score persistido, y
    ninguna cayó nunca por debajo de 42 en esa ventana -- mismo problema de
    cobertura histórica que bloqueó la validación del gate de cross_market
    (ver v20 §5). Por eso arranca en shadow, no aplicado.

    Misma forma de rampa que confidence_score.py::compute_exposure_factor,
    reusando los cortes 42/58 ya establecidos como convención en este
    módulo (RISK_OFF/RISK_ON), no números nuevos.

    Returns: multiplicador sugerido 0.20-1.00 (1.00 = sin recorte).
    """
    if market_trend_score is None:
        return 1.0  # sin dato -> no penalizar, no hay base para hacerlo
    if market_trend_score >= 58:
        return 1.0
    elif market_trend_score >= 42:
        return round(0.60 + (market_trend_score - 42) / 16 * 0.40, 3)
    else:
        return round(max(0.20, 0.60 * market_trend_score / 42), 3)


def _calc_score_adjustments(
    sp_trend_score: float,
    corr_mv_sp: float,
    corr_bv_sp: float,
    mv_diverge: bool,
    bv_diverge: bool,
    mv_trend_score: float = 50.0,
    bv_trend_score: float = 50.0,
) -> dict:
    """
    Calcula el ajuste al macro_score por influencia cross-market.

    Principios:
    • SP500 alcista (trend_score > 58) → boost a emergentes, proporcional a correlación
    • SP500 bajista (trend_score < 42) → penalización, proporcional a correlación
    • Si el mercado diverge (correlación < DIVERGE_THRESH) → el ajuste se reduce
    • El ajuste es una señal débil: máx ±6 puntos sobre el macro_score

    FIX 24/08/2026 (auditoría con Claude, evidencia en backtest_results.json):
    el gate de divergencia por correlación no alcanza a frenar el boost cuando
    un mercado emergente cae fuerte EN TÉRMINOS PROPIOS mientras SP500 sube y
    la correlación diaria se mantiene "moderada" (no por debajo de
    DIVERGE_THRESH). Correlación mide co-movimiento día a día, no que ambos
    mercados vayan en la misma dirección acumulada -- son cosas distintas.
    Caso real detectado: régimen RISK_ON activo, MERVAL con boost positivo,
    mientras MERVAL individualmente tenía EV -7.48%/WR 12.3% a 21d en el
    mismo período (ver backtest_results.json, by_market). market_trends ya
    calculaba la tendencia LOCAL de cada mercado (mv_trend_score/bv_trend_score,
    misma función _trend_score que SP500) pero no se usaba para nada más que
    mostrarlo en el dashboard -- acá se usa para frenar el ajuste cuando
    contradice la tendencia propia del mercado. Reutiliza el mismo factor de
    atenuación (0.30) que ya existía para divergencia por correlación, no se
    inventa un número nuevo. Defaults en 50.0 (neutral) para no romper
    llamadas existentes que no pasen estos parámetros.
    """
    # Señal base de SP500 (-1 a +1)
    # Neutral en 50, max efecto en 0 y 100
    sp_signal = (sp_trend_score - 50) / 50  # rango: -1 a +1

    # Tendencia local contradice la dirección del ajuste (usa los mismos
    # cortes 42/58 que ya definen RISK_OFF/RISK_ON en _determine_regime).
    mv_local_bearish = mv_trend_score < 42
    mv_local_bullish = mv_trend_score > 58
    bv_local_bearish = bv_trend_score < 42
    bv_local_bullish = bv_trend_score > 58

    # Ajuste MERVAL
    mv_base = sp_signal * MAX_BOOST * SP500_INFLUENCE_ON_MERVAL
    if mv_diverge:
        mv_base *= 0.30  # diverge → mucho menos influencia
    if (sp_signal > 0 and mv_local_bearish) or (sp_signal < 0 and mv_local_bullish):
        mv_base *= 0.30  # tendencia local contradice el ajuste → mucho menos influencia
    # Escalar por correlación (si correlación baja, SP500 importa menos)
    corr_mv_safe = max(0.0, min(1.0, corr_mv_sp))
    mv_adj = mv_base * corr_mv_safe

    # Ajuste BOVESPA
    bv_base = sp_signal * MAX_BOOST * SP500_INFLUENCE_ON_BOVESPA
    if bv_diverge:
        bv_base *= 0.30
    if (sp_signal > 0 and bv_local_bearish) or (sp_signal < 0 and bv_local_bullish):
        bv_base *= 0.30
    corr_bv_safe = max(0.0, min(1.0, corr_bv_sp))
    bv_adj = bv_base * corr_bv_safe

    # Clamp para no salirse de rangos razonables
    mv_adj = max(-MAX_PENALTY, min(MAX_BOOST, mv_adj))
    bv_adj = max(-MAX_PENALTY, min(MAX_BOOST, bv_adj))

    return {
        "MERVAL":  round(mv_adj, 2),
        "BOVESPA": round(bv_adj, 2),
        "SP500":   0.0,
    }


def _build_narrative(
    regime: str, sp_trend: str, sp_score: float,
    correlations: dict, divergence: dict, adjustments: dict
) -> str:
    """Genera texto descriptivo del contexto cross-market para el dashboard."""
    avg_corr = correlations["avg"]

    regime_txt = {
        "RISK_ON":  "Apetito global por riesgo activo",
        "RISK_OFF": "Aversión al riesgo global",
        "NEUTRAL":  "Régimen de mercado mixto",
    }.get(regime, regime)

    corr_txt = ("mercados altamente correlados" if avg_corr > CONVERGE_THRESH else
                "baja correlación entre mercados" if avg_corr < DIVERGE_THRESH else
                "correlación moderada entre mercados")

    div_parts = []
    if divergence["merval_diverge"]:
        div_parts.append("MERVAL en modo idiosincrático")
    if divergence["bovespa_diverge"]:
        div_parts.append("BOVESPA en modo idiosincrático")

    adj_mv  = adjustments["MERVAL"]
    adj_bv  = adjustments["BOVESPA"]
    adj_txt = (f"Ajuste macro: MERVAL {adj_mv:+.1f}pt | BOVESPA {adj_bv:+.1f}pt "
               f"por influencia SP500.")

    parts = [
        f"{regime_txt}.",
        f"S&P 500 {sp_trend.lower()} (score {sp_score:.0f}/100), {corr_txt}.",
    ]
    if div_parts:
        parts.append(" | ".join(div_parts) + ".")
    parts.append(adj_txt)

    return " ".join(parts)
