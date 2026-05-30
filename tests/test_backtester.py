"""
tests/test_backtester.py
Tests unitarios para src/backtester.py

Cubre:
  - calc_stop_target_exit: stop hit, target hit, time exit
  - _metrics_from_rets: cálculo de win_rate, EV, sharpe, drawdown
  - _predictor_accuracy: directional accuracy
  - _get_future_prices: lookup en price_index
"""

import sys
import os
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

import numpy as np
import pytest

from src.backtester import (
    _calc_stop_target_exit,
    _metrics_from_rets,
    _predictor_accuracy,
    _get_future_prices,
)


# ── Stop/Target Exit ────────────────────────────────────────────────────────

class TestCalcStopTargetExit:

    def test_stop_hit_before_target(self):
        """Precio cae al stop antes de llegar al target → exit tipo 'stop'."""
        entry   = 100.0
        stop    = 95.0
        target  = 115.0
        # Precios: baja a 94 en día 3
        futures = [99.0, 97.0, 94.0, 96.0, 100.0, 110.0]
        result  = _calc_stop_target_exit(futures, entry, stop, target)
        assert result["st_exit_type"] == "stop"
        assert result["st_ret"] < 0
        assert result["st_exit_day"] == 3

    def test_target_hit_before_stop(self):
        """Precio sube al target antes de tocar el stop → exit tipo 'target'."""
        entry   = 100.0
        stop    = 90.0
        target  = 110.0
        futures = [102.0, 106.0, 111.0, 108.0]
        result  = _calc_stop_target_exit(futures, entry, stop, target)
        assert result["st_exit_type"] == "target"
        assert result["st_ret"] > 0
        assert result["st_exit_day"] == 3

    def test_time_exit_when_neither_hit(self):
        """Ni stop ni target tocados → exit tipo 'time' al día 21."""
        entry   = 100.0
        stop    = 85.0
        target  = 120.0
        # 21 precios entre 95 y 115
        futures = [100 + i * 0.3 for i in range(25)]
        result  = _calc_stop_target_exit(futures, entry, stop, target, max_days=21)
        assert result["st_exit_type"] == "time"
        assert result["st_exit_day"] == 21

    def test_pending_when_insufficient_data(self):
        """Menos datos que max_days → 'pending'."""
        entry   = 100.0
        stop    = 90.0
        target  = 115.0
        futures = [102.0, 104.0]  # solo 2 días
        result  = _calc_stop_target_exit(futures, entry, stop, target, max_days=21)
        assert result["st_exit_type"] == "pending"
        assert result["st_ret"] is None

    def test_no_data_returns_no_data(self):
        """Sin precios futuros → 'no_data'."""
        result = _calc_stop_target_exit([], 100.0, 95.0, 110.0)
        assert result["st_exit_type"] == "no_data"

    def test_stop_above_entry_ignored(self):
        """Stop mayor que entry (stop inválido) → no se activa como stop."""
        entry   = 100.0
        stop    = 110.0   # INVÁLIDO: stop > entry
        target  = 120.0
        futures = [98.0, 95.0, 92.0] + [100.0] * 22
        result  = _calc_stop_target_exit(futures, entry, stop, target, max_days=21)
        # No debe triggear stop ya que stop > entry
        assert result["st_exit_type"] != "stop" or result["st_ret"] is None

    def test_exact_stop_level(self):
        """Precio exactamente igual al stop → debe triggerear."""
        entry   = 100.0
        stop    = 95.0
        target  = 115.0
        futures = [99.0, 95.0, 97.0]
        result  = _calc_stop_target_exit(futures, entry, stop, target)
        assert result["st_exit_type"] == "stop"
        assert result["st_exit_day"] == 2


# ── Métricas desde retornos ────────────────────────────────────────────────

class TestMetricsFromRets:

    def test_empty_returns_none(self):
        assert _metrics_from_rets([]) is None

    def test_all_positive(self):
        rets = [5.0, 3.0, 8.0, 2.0, 10.0]
        m = _metrics_from_rets(rets)
        assert m["win_rate"] == 1.0
        assert m["avg_ret"] > 0
        assert m["avg_loss"] == 0.0
        assert m["expected_value"] > 0

    def test_all_negative(self):
        rets = [-5.0, -3.0, -8.0]
        m = _metrics_from_rets(rets)
        assert m["win_rate"] == 0.0
        assert m["avg_ret"] < 0
        assert m["avg_win"] == 0.0
        assert m["expected_value"] < 0

    def test_mixed_returns(self):
        rets = [10.0, -5.0, 8.0, -3.0, 12.0]  # 3 wins, 2 losses
        m = _metrics_from_rets(rets)
        assert m["win_rate"] == pytest.approx(0.6, abs=0.01)
        assert m["samples"] == 5
        assert m["avg_win"] > 0
        assert m["avg_loss"] > 0

    def test_sharpe_positive_for_positive_avg(self):
        rets = [2.0, 3.0, 1.5, 2.5, 3.5]
        m = _metrics_from_rets(rets)
        assert m["sharpe"] > 0

    def test_max_drawdown_negative(self):
        """Drawdown siempre ≤ 0."""
        rets = [5.0, -8.0, 3.0, -4.0, 6.0]
        m = _metrics_from_rets(rets)
        assert m["max_drawdown"] <= 0

    def test_single_return(self):
        """Un solo retorno → no crashea."""
        m = _metrics_from_rets([5.0])
        assert m is not None
        assert m["samples"] == 1


# ── Accuracy del predictor ────────────────────────────────────────────────

class TestPredictorAccuracy:

    def test_empty_trades_returns_zero_samples(self):
        result = _predictor_accuracy([])
        assert result["samples"] == 0

    def test_trades_without_pred21d_ignored(self):
        trades = [
            {"ret_21d": 5.0, "pred_21d": None, "pred_signal": ""},
            {"ret_21d": -3.0, "pred_21d": None, "pred_signal": ""},
        ]
        result = _predictor_accuracy(trades)
        assert result["samples"] == 0

    def test_perfect_directional_accuracy(self):
        """Predictor siempre acierta la dirección → accuracy = 1.0."""
        trades = [
            {"ret_21d": 5.0,  "pred_21d": 3.0,  "pred_signal": "📈 SUBA"},
            {"ret_21d": 3.0,  "pred_21d": 2.0,  "pred_signal": "📈 SUBA"},
            {"ret_21d": -4.0, "pred_21d": -2.0, "pred_signal": "📉 BAJA"},
        ]
        result = _predictor_accuracy(trades)
        assert result["directional_accuracy"] == pytest.approx(1.0)
        assert result["samples"] == 3

    def test_zero_directional_accuracy(self):
        """Predictor siempre se equivoca → accuracy = 0.0."""
        trades = [
            {"ret_21d": -5.0, "pred_21d": 3.0, "pred_signal": "📈 SUBA"},
            {"ret_21d": -3.0, "pred_21d": 2.0, "pred_signal": "📈 SUBA"},
            {"ret_21d":  4.0, "pred_21d": -2.0, "pred_signal": "📉 BAJA"},
        ]
        result = _predictor_accuracy(trades)
        assert result["directional_accuracy"] == pytest.approx(0.0)

    def test_mae_calculation(self):
        """MAE = promedio de |pred - real|."""
        trades = [
            {"ret_21d": 5.0,  "pred_21d": 3.0,  "pred_signal": "📈 SUBA"},  # error = 2
            {"ret_21d": -3.0, "pred_21d": -5.0, "pred_signal": "📉 BAJA"},  # error = 2
        ]
        result = _predictor_accuracy(trades)
        assert result["mae"] == pytest.approx(2.0)


# ── Lookup de precios futuros ─────────────────────────────────────────────

class TestGetFuturePrices:

    def setup_method(self):
        self.price_index = {
            "GGAL.BA": {
                "2026-05-01": 100.0,
                "2026-05-02": 101.0,
                "2026-05-05": 102.0,
                "2026-05-06": 103.0,
                "2026-05-07": 104.0,
                "2026-05-08": 105.0,
            }
        }

    def test_returns_prices_after_signal_date(self):
        result = _get_future_prices("GGAL.BA", "2026-05-01", self.price_index, max_horizon=3)
        assert result == [101.0, 102.0, 103.0]

    def test_unknown_ticker_returns_empty(self):
        result = _get_future_prices("UNKN.BA", "2026-05-01", self.price_index)
        assert result == []

    def test_last_date_returns_empty(self):
        """No hay precios después del último día."""
        result = _get_future_prices("GGAL.BA", "2026-05-08", self.price_index)
        assert result == []

    def test_max_horizon_respected(self):
        result = _get_future_prices("GGAL.BA", "2026-05-01", self.price_index, max_horizon=2)
        assert len(result) <= 2
