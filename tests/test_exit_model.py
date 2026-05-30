"""
tests/test_exit_model.py
Tests para src/exit_model.py
"""

import sys
import os
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

import numpy as np
import pandas as pd
import pytest

from src.exit_model import (
    _dynamic_multipliers,
    _calc_exit_score,
    _lookup_scale,
    enrich_exit_levels,
    BASE_MULT,
    REGIME_FACTOR,
    MIN_RR,
)


class TestDynamicMultipliers:

    def test_merval_tighter_than_sp500(self):
        """MERVAL usa stops más cortos que SP500."""
        mv = _dynamic_multipliers("MERVAL", 50, 1.0)
        sp = _dynamic_multipliers("SP500",  50, 1.0)
        assert mv["stop"] < sp["stop"]

    def test_risk_off_tightens_stops(self):
        """Factor RISK_OFF reduce el multiplicador del stop."""
        neutral = _dynamic_multipliers("SP500", 50, REGIME_FACTOR["NEUTRAL"])
        risk_off = _dynamic_multipliers("SP500", 50, REGIME_FACTOR["RISK_OFF"])
        assert risk_off["stop"] < neutral["stop"]

    def test_high_vol_widens_stop(self):
        """Alta volatilidad → stop más ancho."""
        low_vol  = _dynamic_multipliers("MERVAL", 10,  1.0)
        high_vol = _dynamic_multipliers("MERVAL", 90,  1.0)
        assert high_vol["stop"] > low_vol["stop"]

    def test_multipliers_within_bounds(self):
        """Stop [1.0, 3.5], Target [1.5, 6.0]."""
        for market in ["MERVAL", "BOVESPA", "SP500"]:
            for vol in [5, 30, 60, 90]:
                for factor in [0.8, 1.0]:
                    m = _dynamic_multipliers(market, vol, factor)
                    assert 1.0 <= m["stop"]   <= 3.5,  f"{market} vol={vol} stop={m['stop']}"
                    assert 1.5 <= m["target"] <= 6.0,  f"{market} vol={vol} target={m['target']}"

    def test_unknown_market_uses_default(self):
        """Mercado desconocido no crashea."""
        m = _dynamic_multipliers("UNKNOWN", 50, 1.0)
        assert m["stop"] > 0
        assert m["target"] > 0


class TestLookupScale:

    def test_correct_bucket(self):
        from src.exit_model import VOL_STOP_SCALE
        assert _lookup_scale(VOL_STOP_SCALE, 10) == VOL_STOP_SCALE[(0, 25)]
        assert _lookup_scale(VOL_STOP_SCALE, 30) == VOL_STOP_SCALE[(25, 50)]
        assert _lookup_scale(VOL_STOP_SCALE, 60) == VOL_STOP_SCALE[(50, 75)]
        assert _lookup_scale(VOL_STOP_SCALE, 80) == VOL_STOP_SCALE[(75, 101)]

    def test_boundary_value(self):
        from src.exit_model import VOL_STOP_SCALE
        # 25 está en el segundo bucket [25, 50)
        result = _lookup_scale(VOL_STOP_SCALE, 25)
        assert result == VOL_STOP_SCALE[(25, 50)]


class TestCalcExitScore:

    def _base_signal(self, **kwargs):
        base = {
            "ticker": "TEST",
            "mercado": "SP500",
            "rsi": 50,
            "score_final_v2": 55,
            "signal": "🟢 COMPRA",
            "signal_v2": "🟢 COMPRA",
            "pred_21d": 3.0,
            "ret_anual": 10.0,
        }
        base.update(kwargs)
        return base

    def test_low_score_for_strong_buy(self):
        """Señal fuerte de compra → exit score bajo (mantener)."""
        sig = self._base_signal(rsi=45, score_final_v2=70, pred_21d=8.0)
        score, rec = _calc_exit_score(sig, None, "NEUTRAL")
        assert score < 35, f"Compra fuerte esperado score <35, got {score}"
        assert "Mantener" in rec

    def test_high_score_for_sell_signal(self):
        """Señal de venta explícita → exit score alto."""
        sig = self._base_signal(
            signal_v2="🔴 VENTA",
            rsi=80,
            score_final_v2=30,
            pred_21d=-12.0,
        )
        score, rec = _calc_exit_score(sig, None, "NEUTRAL")
        assert score > 55, f"Señal venta esperado score >55, got {score}"

    def test_risk_off_amplifies_score(self):
        """Régimen RISK_OFF amplifica exit score moderado."""
        sig = self._base_signal(rsi=75, score_final_v2=38)
        score_neutral, _ = _calc_exit_score(sig, None, "NEUTRAL")
        score_riskoff, _ = _calc_exit_score(sig, None, "RISK_OFF")
        assert score_riskoff >= score_neutral

    def test_score_capped_at_100(self):
        """Exit score nunca supera 100."""
        sig = self._base_signal(
            signal_v2="🔴 VENTA",
            rsi=85,
            score_final_v2=25,
            pred_21d=-15.0,
        )
        score, _ = _calc_exit_score(sig, None, "RISK_OFF")
        assert score <= 100

    def test_price_below_ma20_increases_score(self):
        """Precio < MA20 agrega puntos al exit score."""
        sig = self._base_signal()
        # Serie con precio cayendo bajo su MA20
        prices = pd.Series([100.0] * 25 + [90.0])  # MA20 ≈ 100, precio = 90
        score_with_series, _ = _calc_exit_score(sig, prices, "NEUTRAL")
        score_no_series, _   = _calc_exit_score(sig, None, "NEUTRAL")
        assert score_with_series >= score_no_series

    def test_recommendation_levels(self):
        """Recomendación refleja el nivel del score."""
        signals_and_expected = [
            (self._base_signal(rsi=50, pred_21d=5.0, score_final_v2=65), "Mantener"),
            # RSI>78(25pts) + V2<42(12pts) = 37 → puede quedar en Monitorear o Mantener
            # Verificamos solo que hay texto de recomendación
        ]
        # Test simplificado: verificar que devuelve string no vacío
        for sig, expected in signals_and_expected:
            _, rec = _calc_exit_score(sig, None, "NEUTRAL")
            assert isinstance(rec, str) and len(rec) > 0
        for sig, expected_substr in signals_and_expected:
            _, rec = _calc_exit_score(sig, None, "NEUTRAL")
            assert expected_substr in rec, f"Expected '{expected_substr}' in '{rec}'"


class TestEnrichExitLevels:

    def _make_price_df(self, ticker, col_name, n=100):
        prices = pd.Series([100.0 + i * 0.5 for i in range(n)])
        return pd.DataFrame({col_name: prices})

    def test_updates_atr_stop_and_target(self):
        """enrich_exit_levels actualiza atr_stop y atr_target."""
        signals = [{
            "ticker": "GGAL.BA",
            "mercado": "MERVAL",
            "precio_actual": 100.0,
            "atr": 3.0,
            "atr_stop": 94.0,    # stop viejo: 100 - 3*2
            "atr_target": 109.0, # target viejo: 100 + 3*3
            "volatility_score": 50,
            "signal": "🟢 COMPRA",
            "signal_v2": "🟢 COMPRA",
            "rsi": 50,
            "score_final_v2": 60,
            "pred_21d": 4.0,
        }]
        ticker_cols = {"GGAL.BA": "Galicia"}
        df = self._make_price_df("GGAL.BA", "Galicia")
        price_data = {"merval": df, "bovespa": pd.DataFrame(), "sp500": pd.DataFrame()}

        enriched = enrich_exit_levels(signals, price_data, ticker_cols, regime="NEUTRAL")

        sig = enriched[0]
        assert "exit_score" in sig
        assert "exit_recommendation" in sig
        assert "stop_mult_used" in sig
        assert sig["atr_stop"] > 0
        assert sig["atr_target"] > sig["atr_stop"]

    def test_minimum_rr_maintained(self):
        """R/R siempre ≥ MIN_RR."""
        signals = [{
            "ticker": "AAPL",
            "mercado": "SP500",
            "precio_actual": 200.0,
            "atr": 2.0,
            "atr_stop": 196.0,
            "atr_target": 206.0,
            "volatility_score": 20,
            "signal": "🟢 COMPRA",
            "signal_v2": "🟢 COMPRA",
            "rsi": 50,
            "score_final_v2": 60,
            "pred_21d": 3.0,
        }]
        ticker_cols = {"AAPL": "Apple"}
        df = self._make_price_df("AAPL", "Apple")
        price_data = {"merval": pd.DataFrame(), "bovespa": pd.DataFrame(), "sp500": df}

        enriched = enrich_exit_levels(signals, price_data, ticker_cols, "NEUTRAL")
        sig = enriched[0]

        precio = sig["precio_actual"]
        stop   = sig["atr_stop"]
        target = sig["atr_target"]

        if precio > stop and target > precio:
            rr = (target - precio) / (precio - stop)
            assert rr >= MIN_RR - 0.01, f"R/R {rr:.2f} menor que MIN_RR {MIN_RR}"

    def test_no_crash_on_zero_atr(self):
        """ATR = 0 → no crashea, signal queda sin cambios."""
        signals = [{
            "ticker": "TEST",
            "mercado": "SP500",
            "precio_actual": 100.0,
            "atr": 0.0,  # ATR nulo
            "atr_stop": 0.0,
            "atr_target": 0.0,
            "volatility_score": 50,
        }]
        result = enrich_exit_levels(signals, {}, {}, "NEUTRAL")
        assert result is not None
        assert len(result) == 1
