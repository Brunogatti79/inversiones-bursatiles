"""
tests/test_cross_market.py
Tests para src/cross_market.py
"""

import sys
import os
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

import numpy as np
import pandas as pd
import pytest

from src.cross_market import (
    _trend_score,
    _rolling_corr_last,
    _determine_regime,
    _calc_score_adjustments,
    compute_cross_market_context,
    MAX_BOOST,
    MAX_PENALTY,
)


class TestTrendScore:

    def test_strong_uptrend_returns_alcista(self):
        """Precios subiendo → ALCISTA."""
        prices = pd.Series([float(i) for i in range(50, 110)])
        score, label = _trend_score(prices)
        assert label == "ALCISTA"
        assert score > 58

    def test_strong_downtrend_returns_bajista(self):
        """Precios bajando → BAJISTA."""
        prices = pd.Series([float(i) for i in range(110, 50, -1)])
        score, label = _trend_score(prices)
        assert label == "BAJISTA"
        assert score < 42

    def test_flat_market_returns_lateral(self):
        """Precios planos → LATERAL."""
        prices = pd.Series([100.0] * 60)
        score, label = _trend_score(prices)
        assert label == "LATERAL"

    def test_short_series_returns_50(self):
        """Menos de 50 datos → fallback LATERAL con score 50."""
        prices = pd.Series([100.0] * 20)
        score, label = _trend_score(prices)
        assert label == "LATERAL"
        assert score == 50.0

    def test_score_range(self):
        """Score siempre en [0, 100]."""
        for _ in range(10):
            np.random.seed(42)
            prices = pd.Series(np.random.randn(60).cumsum() + 100)
            score, _ = _trend_score(prices)
            assert 0 <= score <= 100


class TestRollingCorr:

    def test_perfect_positive_correlation(self):
        """Dos series idénticas → correlación ~1."""
        s = pd.Series([float(i) for i in range(50)])
        corr = _rolling_corr_last(s.pct_change().dropna(), s.pct_change().dropna())
        assert corr > 0.95

    def test_negative_correlation(self):
        """Una serie sube cuando la otra baja → correlación negativa."""
        np.random.seed(42)
        n = 60
        base = np.random.randn(n)
        s1 = pd.Series(base)
        s2 = pd.Series(-base)  # perfectamente invertida
        corr = _rolling_corr_last(s1, s2)
        assert corr < 0, f"Correlación esperada negativa, got {corr}"

    def test_short_series_returns_zero(self):
        """Menos datos que la ventana → 0."""
        s = pd.Series([1.0, 2.0, 3.0])
        corr = _rolling_corr_last(s, s)
        assert corr == 0.0


class TestDetermineRegime:

    def test_bullish_sp500_returns_risk_on(self):
        assert _determine_regime(65.0, 0.7) == "RISK_ON"

    def test_bearish_sp500_returns_risk_off(self):
        assert _determine_regime(35.0, 0.7) == "RISK_OFF"

    def test_neutral_sp500_returns_neutral(self):
        assert _determine_regime(50.0, 0.5) == "NEUTRAL"


class TestScoreAdjustments:

    def test_risk_on_boosts_emerging_markets(self):
        """SP500 alcista → ajuste positivo para MERVAL y BOVESPA."""
        adj = _calc_score_adjustments(
            sp_trend_score=70.0,
            corr_mv_sp=0.7,
            corr_bv_sp=0.65,
            mv_diverge=False,
            bv_diverge=False,
        )
        assert adj["MERVAL"] > 0
        assert adj["BOVESPA"] > 0
        assert adj["SP500"] == 0.0

    def test_risk_off_penalizes_emerging_markets(self):
        """SP500 bajista → ajuste negativo."""
        adj = _calc_score_adjustments(
            sp_trend_score=30.0,
            corr_mv_sp=0.7,
            corr_bv_sp=0.65,
            mv_diverge=False,
            bv_diverge=False,
        )
        assert adj["MERVAL"] < 0
        assert adj["BOVESPA"] < 0

    def test_divergence_reduces_adjustment(self):
        """Divergencia reduce el ajuste."""
        adj_no_div = _calc_score_adjustments(70.0, 0.7, 0.65, False, False)
        adj_div    = _calc_score_adjustments(70.0, 0.7, 0.65, True,  False)
        assert abs(adj_div["MERVAL"]) < abs(adj_no_div["MERVAL"])

    def test_adjustment_bounded(self):
        """Ajuste nunca supera MAX_BOOST ni es menor que -MAX_PENALTY."""
        adj = _calc_score_adjustments(100.0, 1.0, 1.0, False, False)
        assert adj["MERVAL"]  <= MAX_BOOST
        assert adj["BOVESPA"] <= MAX_BOOST
        adj2 = _calc_score_adjustments(0.0, 1.0, 1.0, False, False)
        assert adj2["MERVAL"]  >= -MAX_PENALTY
        assert adj2["BOVESPA"] >= -MAX_PENALTY

    def test_neutral_trend_near_zero(self):
        """Tendencia neutral (score ~50) → ajuste cercano a 0."""
        adj = _calc_score_adjustments(50.0, 0.5, 0.5, False, False)
        assert abs(adj["MERVAL"])  < 1.0
        assert abs(adj["BOVESPA"]) < 1.0

    # ── FIX 24/08/2026: gate por tendencia local, no solo correlación ──────
    # Caso real que motivó el fix: régimen RISK_ON (SP500 alcista) con
    # MERVAL cayendo en términos propios (EV -7.48%/WR 12.3% a 21d en
    # backtest_results.json) mientras la correlación se mantenía "moderada"
    # (no cruzaba DIVERGE_THRESH=0.20) -- el ajuste seguía siendo positivo.

    def test_local_bearish_trend_suppresses_risk_on_boost(self):
        """SP500 alcista pero MERVAL con tendencia local BAJISTA propia →
        el boost debe atenuarse fuerte, no aplicarse a pleno como si MERVAL
        estuviera de acuerdo con SP500."""
        adj_agree    = _calc_score_adjustments(
            70.0, 0.7, 0.65, False, False,
            mv_trend_score=70.0, bv_trend_score=70.0,  # MERVAL/BOVESPA también alcistas
        )
        adj_contra   = _calc_score_adjustments(
            70.0, 0.7, 0.65, False, False,
            mv_trend_score=30.0, bv_trend_score=70.0,  # MERVAL bajista propio, BOVESPA alcista
        )
        # MERVAL contradiciendo su propia tendencia local debe pesar mucho
        # menos que cuando coincide con SP500 -- ya no boost casi pleno.
        assert adj_contra["MERVAL"] < adj_agree["MERVAL"]
        assert adj_contra["MERVAL"] >= 0  # sigue sin ser negativo, solo atenuado
        # BOVESPA no debería verse afectado por lo que pasa en MERVAL
        assert adj_contra["BOVESPA"] == adj_agree["BOVESPA"]

    def test_local_bullish_trend_suppresses_risk_off_penalty(self):
        """Caso simétrico: SP500 bajista pero un mercado con tendencia local
        propia alcista → la penalización debe atenuarse, no aplicarse a
        pleno."""
        adj_agree  = _calc_score_adjustments(
            30.0, 0.7, 0.65, False, False,
            mv_trend_score=30.0, bv_trend_score=30.0,
        )
        adj_contra = _calc_score_adjustments(
            30.0, 0.7, 0.65, False, False,
            mv_trend_score=70.0, bv_trend_score=30.0,  # MERVAL alcista propio pese a SP500 bajista
        )
        assert adj_contra["MERVAL"] > adj_agree["MERVAL"]  # penalización más chica (menos negativa)
        assert adj_contra["BOVESPA"] == adj_agree["BOVESPA"]

    def test_default_trend_score_preserves_old_behavior(self):
        """Sin pasar mv_trend_score/bv_trend_score (llamadas viejas, ej. el
        resto de esta suite de tests), el default neutral (50.0) no debe
        gatillar ninguna atenuación por tendencia local -- comportamiento
        idéntico al que existía antes del fix."""
        adj_sin_kwargs = _calc_score_adjustments(70.0, 0.7, 0.65, False, False)
        adj_default_explicito = _calc_score_adjustments(
            70.0, 0.7, 0.65, False, False,
            mv_trend_score=50.0, bv_trend_score=50.0,
        )
        assert adj_sin_kwargs == adj_default_explicito

    def test_local_trend_agrees_no_extra_suppression(self):
        """Si la tendencia local coincide con la dirección del ajuste
        (ambas alcistas o ambas bajistas), no debe activarse la atenuación
        -- el ajuste debe seguir siendo prácticamente el mismo que antes
        del fix (mismo cálculo, solo corr_mv_sp/mv_diverge)."""
        adj_con_gate = _calc_score_adjustments(
            70.0, 0.7, 0.65, False, False,
            mv_trend_score=65.0, bv_trend_score=65.0,
        )
        adj_sin_gate = _calc_score_adjustments(70.0, 0.7, 0.65, False, False)
        assert adj_con_gate == adj_sin_gate


class TestComputeCrossMarketContext:

    def _make_df(self, n=100, trend=1.0, col="IDX"):
        """Helper: crea DataFrame con un índice de precios."""
        prices = pd.Series([100.0 + i * trend + np.random.randn() * 0.5
                            for i in range(n)], name=col)
        return pd.DataFrame({col: prices})

    def test_fallback_on_missing_data(self):
        """DataFrames vacíos → retorna fallback con NEUTRAL."""
        result = compute_cross_market_context(
            pd.DataFrame(), pd.DataFrame(), pd.DataFrame(),
            {"merval": "", "bovespa": "", "sp500": ""}
        )
        assert result["regime"] == "NEUTRAL"

    def test_returns_expected_keys(self):
        """Output contiene todas las claves esperadas."""
        mv = self._make_df(col="MERVAL")
        bv = self._make_df(col="BOVESPA")
        sp = self._make_df(col="SP500")
        result = compute_cross_market_context(
            mv, bv, sp,
            {"merval": "MERVAL", "bovespa": "BOVESPA", "sp500": "SP500"}
        )
        for key in ["regime", "sp500_trend", "correlations", "score_adjustments", "narrative"]:
            assert key in result

    def test_sp500_always_gets_zero_adjustment(self):
        """SP500 nunca se auto-ajusta."""
        mv = self._make_df(col="MERVAL")
        bv = self._make_df(col="BOVESPA")
        sp = self._make_df(col="SP500")
        result = compute_cross_market_context(
            mv, bv, sp,
            {"merval": "MERVAL", "bovespa": "BOVESPA", "sp500": "SP500"}
        )
        assert result["score_adjustments"]["SP500"] == 0.0

    def test_market_trends_incluye_los_3_mercados(self):
        """Roadmap externo #8 (jul-2026): regime detection por país -- antes
        _trend_score() solo se llamaba sobre sp_serie, aunque mv_serie/
        bv_serie ya estaban cargadas acá mismo para el cálculo de
        correlación. market_trends expone tendencia local independiente
        por mercado, distinta del "regime" global (que es sobre SP500)."""
        mv = self._make_df(col="MERVAL")
        bv = self._make_df(col="BOVESPA")
        sp = self._make_df(col="SP500")
        result = compute_cross_market_context(
            mv, bv, sp,
            {"merval": "MERVAL", "bovespa": "BOVESPA", "sp500": "SP500"}
        )
        assert "market_trends" in result
        for mkt in ("MERVAL", "BOVESPA", "SP500"):
            assert mkt in result["market_trends"]
            assert result["market_trends"][mkt]["trend"] in ("ALCISTA", "BAJISTA", "LATERAL")
            assert 0 <= result["market_trends"][mkt]["score"] <= 100

    def test_market_trends_distingue_mercados_con_tendencias_distintas(self):
        """Con MERVAL en tendencia clara alcista y BOVESPA clara bajista,
        no deberían quedar pegados al mismo label -- eso pasaría si
        siguiera usando solo el trend de SP500 para todos."""
        np.random.seed(7)
        n = 300
        merval_prices = 100 * np.cumprod(1 + np.random.normal(0.006, 0.008, n))
        bovespa_prices = 100 * np.cumprod(1 + np.random.normal(-0.006, 0.008, n))
        sp500_prices = 100 * np.cumprod(1 + np.random.normal(0.0, 0.008, n))
        mv = pd.DataFrame({"MERVAL": merval_prices})
        bv = pd.DataFrame({"BOVESPA": bovespa_prices})
        sp = pd.DataFrame({"SP500": sp500_prices})

        result = compute_cross_market_context(
            mv, bv, sp,
            {"merval": "MERVAL", "bovespa": "BOVESPA", "sp500": "SP500"}
        )
        assert result["market_trends"]["MERVAL"]["trend"] == "ALCISTA"
        assert result["market_trends"]["BOVESPA"]["trend"] == "BAJISTA"

    def test_fallback_tambien_incluye_market_trends(self):
        """El fallback (datos insuficientes) debe tener la misma forma que
        el resultado normal, para que un consumidor no reviente por
        KeyError si cae en ese camino."""
        result = compute_cross_market_context(
            pd.DataFrame(), pd.DataFrame(), pd.DataFrame(),
            {"merval": "", "bovespa": "", "sp500": ""}
        )
        assert "market_trends" in result
        for mkt in ("MERVAL", "BOVESPA", "SP500"):
            assert mkt in result["market_trends"]

    # ── Shadow mode (24/08/2026): el gate por tendencia local se calcula
    # pero NO se aplica todavía -- ver nota en compute_cross_market_context.
    # Motivo: al medir el Top20/Bottom20 real del backtest se encontró que
    # el 100% de esas señales tienen cross_market_regime=UNKNOWN (el campo
    # es reciente), así que el mecanismo no tiene evidencia de explicar el
    # problema medido, aunque siga siendo plausible hacia adelante.

    def test_score_adjustments_applied_ignores_local_trend_gate(self):
        """El score_adjustments que se aplica en pipeline.py debe ser
        IDÉNTICO al comportamiento de antes de esta sesión -- sin importar
        la tendencia local de cada mercado."""
        np.random.seed(3)
        n = 300
        # SP500 fuerte alcista, MERVAL cayendo en términos propios -- el
        # caso exacto que el gate atenuaría si estuviera activo.
        merval_prices = 100 * np.cumprod(1 + np.random.normal(-0.006, 0.008, n))
        bovespa_prices = 100 * np.cumprod(1 + np.random.normal(0.004, 0.008, n))
        sp500_prices = 100 * np.cumprod(1 + np.random.normal(0.006, 0.008, n))
        mv = pd.DataFrame({"MERVAL": merval_prices})
        bv = pd.DataFrame({"BOVESPA": bovespa_prices})
        sp = pd.DataFrame({"SP500": sp500_prices})

        result = compute_cross_market_context(
            mv, bv, sp,
            {"merval": "MERVAL", "bovespa": "BOVESPA", "sp500": "SP500"}
        )
        assert result["market_trends"]["MERVAL"]["trend"] == "BAJISTA"
        assert result["regime"] == "RISK_ON"
        # El aplicado (score_adjustments) NO debe verse recortado por la
        # tendencia local bajista de MERVAL -- shadow mode, no se aplica.
        applied_no_gate = _calc_score_adjustments(
            result["sp500_trend_score"],
            result["correlations"]["merval_sp500"],
            result["correlations"]["bovespa_sp500"],
            result["divergence"]["merval_diverge"],
            result["divergence"]["bovespa_diverge"],
        )
        assert result["score_adjustments"]["MERVAL"] == applied_no_gate["MERVAL"]

    def test_shadow_field_present_and_can_differ_from_applied(self):
        """score_adjustments_shadow_gate_local debe estar presente, y en el
        caso RISK_ON + MERVAL localmente bajista debe venir MÁS CHICO (más
        conservador) que el aplicado -- así se puede medir el efecto antes
        de activarlo, comparando ambos campos persistidos por señal."""
        np.random.seed(3)
        n = 300
        merval_prices = 100 * np.cumprod(1 + np.random.normal(-0.006, 0.008, n))
        bovespa_prices = 100 * np.cumprod(1 + np.random.normal(0.004, 0.008, n))
        sp500_prices = 100 * np.cumprod(1 + np.random.normal(0.006, 0.008, n))
        mv = pd.DataFrame({"MERVAL": merval_prices})
        bv = pd.DataFrame({"BOVESPA": bovespa_prices})
        sp = pd.DataFrame({"SP500": sp500_prices})

        result = compute_cross_market_context(
            mv, bv, sp,
            {"merval": "MERVAL", "bovespa": "BOVESPA", "sp500": "SP500"}
        )
        assert "score_adjustments_shadow_gate_local" in result
        shadow = result["score_adjustments_shadow_gate_local"]
        applied = result["score_adjustments"]
        if result["market_trends"]["MERVAL"]["trend"] == "BAJISTA" and applied["MERVAL"] > 0:
            assert shadow["MERVAL"] <= applied["MERVAL"]

    def test_fallback_includes_shadow_key(self):
        """El fallback también debe tener la clave shadow, mismo criterio
        que market_trends -- evita KeyError en consumidores downstream."""
        result = compute_cross_market_context(
            pd.DataFrame(), pd.DataFrame(), pd.DataFrame(),
            {"merval": "", "bovespa": "", "sp500": ""}
        )
        assert "score_adjustments_shadow_gate_local" in result
