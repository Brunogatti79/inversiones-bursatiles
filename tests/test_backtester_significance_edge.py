"""
tests/test_backtester_significance_edge.py

Tests para las mejoras incorporadas a partir de la auditoría externa de
28/07/2026 (revisión de una IA especializada sobre la arquitectura v17):

  1. Significancia estadística (p_value, IC95%, profit_factor) en
     _metrics_from_rets.
  2. Cruce de 3 vías confidence_label × signal × mercado
     (_aggregate_cross3).
  3. Ablation práctica del predictor (_predictor_contribution_analysis):
     ¿el resultado real difiere según si el predictor confirma o
     contradice la señal?
  4. Historical Edge Score informativo (_compute_historical_edge_scores):
     NO debe conectarse a ranking_accionable -- estos tests también
     verifican que el campo se comporta como diagnóstico puro.
"""

import sys
import os
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

import pytest

from src.backtester import (
    _metrics_from_rets,
    _aggregate_cross3,
    _predictor_contribution_analysis,
    _compute_historical_edge_scores,
)


# ── 1. Significancia estadística ────────────────────────────────────────────

class TestMetricsSignificance:

    def test_profit_factor_normal_case(self):
        # 2 wins de +10, 1 loss de -5 -> gross_profit=20, gross_loss=5 -> pf=4.0
        m = _metrics_from_rets([10.0, 10.0, -5.0])
        assert m["profit_factor"] == 4.0

    def test_profit_factor_none_when_no_losses(self):
        m = _metrics_from_rets([5.0, 3.0, 8.0])
        assert m["profit_factor"] is None  # infinito no es informativo

    def test_profit_factor_zero_when_no_wins(self):
        m = _metrics_from_rets([-1.0, -2.0])
        assert m["profit_factor"] == 0.0

    def test_p_value_and_ic95_present_with_n_ge_2(self):
        m = _metrics_from_rets([1.0, 2.0, -1.0, 3.0, 0.5])
        assert m["p_value"] is not None
        assert m["ic95"] is not None
        assert len(m["ic95"]) == 2
        assert m["ic95"][0] <= m["avg_ret"] <= m["ic95"][1]

    def test_p_value_none_with_single_sample(self):
        m = _metrics_from_rets([5.0])
        assert m["p_value"] is None
        assert m["ic95"] is None
        assert m["significativo_95"] is False

    def test_significativo_95_true_for_strong_consistent_signal(self):
        # Retornos consistentemente positivos y con poca dispersión ->
        # debería ser significativo.
        rets = [5.0, 5.5, 4.8, 5.2, 5.1, 4.9, 5.3, 5.0, 4.7, 5.4]
        m = _metrics_from_rets(rets)
        assert m["p_value"] < 0.05
        assert m["significativo_95"] is True

    def test_significativo_95_false_for_noisy_signal_around_zero(self):
        rets = [3.0, -4.0, 2.0, -1.0, 0.5, -2.5, 1.0, -0.5]
        m = _metrics_from_rets(rets)
        assert m["significativo_95"] is False

    def test_metrics_none_when_empty(self):
        assert _metrics_from_rets([]) is None

    def test_existing_fields_unchanged(self):
        # No debe romper el contrato previo (sharpe, max_drawdown, etc.)
        m = _metrics_from_rets([1.0, 2.0, -1.0])
        for field in ("samples", "win_rate", "avg_ret", "avg_win",
                      "avg_loss", "expected_value", "sharpe", "max_drawdown"):
            assert field in m


# ── 2. Cruce de 3 vías ──────────────────────────────────────────────────────

class TestAggregateCross3:

    def _mk_trade(self, conf, sig, mkt, ret21=1.0):
        return {"confidence_label": conf, "signal": sig, "mercado": mkt,
                "ret_5d": ret21, "ret_10d": ret21, "ret_21d": ret21,
                "st_exit_type": "no_data"}

    def test_structure_nested_by_three_keys(self):
        trades = [
            self._mk_trade("Alta", "COMPRA", "merval"),
            self._mk_trade("Alta", "COMPRA", "merval"),
            self._mk_trade("Alta", "COMPRA", "sp500"),
        ]
        cross = _aggregate_cross3(trades, "confidence_label", "signal", "mercado")
        assert "Alta" in cross
        assert "COMPRA" in cross["Alta"]
        assert set(cross["Alta"]["COMPRA"].keys()) == {"merval", "sp500"}
        assert cross["Alta"]["COMPRA"]["merval"]["count"] == 2
        assert cross["Alta"]["COMPRA"]["sp500"]["count"] == 1

    def test_muestra_insuficiente_flag(self):
        trades = [self._mk_trade("Alta", "COMPRA", "merval")]  # solo 1 trade
        cross = _aggregate_cross3(trades, "confidence_label", "signal", "mercado",
                                   min_cell_samples=5)
        assert cross["Alta"]["COMPRA"]["merval"]["muestra_insuficiente"] is True

    def test_missing_key_falls_back_to_unknown(self):
        trades = [{"signal": "COMPRA", "ret_5d": 1.0, "ret_10d": 1.0, "ret_21d": 1.0}]
        cross = _aggregate_cross3(trades, "confidence_label", "signal", "mercado")
        assert "UNKNOWN" in cross  # confidence_label ausente


# ── 3. Ablation del predictor ───────────────────────────────────────────────

class TestPredictorContribution:

    def _mk_trade(self, signal, pred_21d, ret21):
        return {"signal": signal, "pred_21d": pred_21d,
                "ret_5d": ret21, "ret_10d": ret21, "ret_21d": ret21}

    def test_confirma_group_when_buy_and_positive_pred(self):
        trades = [self._mk_trade("COMPRA", 5.0, 3.0)]
        result = _predictor_contribution_analysis(trades)
        assert result["confirma"]["count"] == 1
        assert result["contradice"]["count"] == 0

    def test_contradice_group_when_buy_and_negative_pred(self):
        trades = [self._mk_trade("COMPRA", -5.0, -1.0)]
        result = _predictor_contribution_analysis(trades)
        assert result["contradice"]["count"] == 1
        assert result["confirma"]["count"] == 0

    def test_venta_confirma_with_negative_pred(self):
        trades = [self._mk_trade("VENTA", -3.0, -2.0)]
        result = _predictor_contribution_analysis(trades)
        assert result["confirma"]["count"] == 1

    def test_sin_dato_when_pred_missing(self):
        trades = [self._mk_trade("COMPRA", None, 1.0)]
        result = _predictor_contribution_analysis(trades)
        assert result["sin_dato"]["count"] == 1

    def test_sin_dato_when_signal_neutral(self):
        trades = [self._mk_trade("NEUTRAL", 5.0, 1.0)]
        result = _predictor_contribution_analysis(trades)
        assert result["sin_dato"]["count"] == 1

    def test_resumen_delta_computed(self):
        trades = [
            self._mk_trade("COMPRA", 5.0, 10.0),   # confirma, ret alto
            self._mk_trade("COMPRA", 5.0, 8.0),     # confirma
            self._mk_trade("COMPRA", -5.0, -10.0),  # contradice, ret bajo
            self._mk_trade("COMPRA", -5.0, -8.0),   # contradice
        ]
        result = _predictor_contribution_analysis(trades)
        assert result["_resumen"]["delta_ev_21d"] is not None
        assert result["_resumen"]["delta_ev_21d"] > 0  # confirma rindió mejor en este mock

    def test_empty_trades_no_crash(self):
        result = _predictor_contribution_analysis([])
        assert result["confirma"]["count"] == 0
        assert result["_resumen"]["delta_ev_21d"] is None


# ── 4. Historical Edge Score (informativo) ─────────────────────────────────

class TestHistoricalEdgeScore:

    def test_none_score_when_insufficient_sample(self):
        cross = {
            "Alta": {"COMPRA": {"h5d": {"samples": 3, "expected_value": 2.0,
                                         "significativo_95": False},
                                 "h10d": {"samples": 2, "expected_value": 1.5,
                                          "significativo_95": False}}}
        }
        result = _compute_historical_edge_scores(cross, min_samples=15)
        assert result["Alta + COMPRA"]["score"] is None
        assert result["Alta + COMPRA"]["nota"] == "muestra insuficiente"

    def test_score_computed_when_enough_samples(self):
        cross = {
            "Alta": {"COMPRA": {"h5d": {"samples": 40, "expected_value": 2.0,
                                         "significativo_95": True},
                                 "h10d": {"samples": 20, "expected_value": 1.0,
                                          "significativo_95": False}}}
        }
        result = _compute_historical_edge_scores(cross, min_samples=15)
        entry = result["Alta + COMPRA"]
        assert entry["score"] is not None
        assert 0 <= entry["score"] <= 100
        # EV=2.0 (positivo) + significativo + n>=50? no (n=40) -> sin ese bonus
        # base 50 + min(30, 2.0*6=12) = 62, + 15 (signif) = 77
        assert entry["score"] == 77.0

    def test_score_clamped_at_100(self):
        cross = {
            "Alta": {"COMPRA": {"h5d": {"samples": 100, "expected_value": 50.0,
                                         "significativo_95": True}}}
        }
        result = _compute_historical_edge_scores(cross, min_samples=15)
        assert result["Alta + COMPRA"]["score"] == 100.0

    def test_negative_ev_reduces_score_below_50(self):
        cross = {
            "Media": {"COMPRA": {"h5d": {"samples": 30, "expected_value": -3.0,
                                          "significativo_95": False}}}
        }
        result = _compute_historical_edge_scores(cross, min_samples=15)
        assert result["Media + COMPRA"]["score"] < 50

    def test_does_not_mutate_input_cross(self):
        cross = {
            "Alta": {"COMPRA": {"h5d": {"samples": 40, "expected_value": 2.0,
                                         "significativo_95": True}}}
        }
        import copy
        original = copy.deepcopy(cross)
        _compute_historical_edge_scores(cross, min_samples=15)
        assert cross == original


if __name__ == "__main__":
    pytest.main([__file__, "-v"])
