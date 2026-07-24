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
