"""
tests/test_weight_optimizer.py
Tests para src/weight_optimizer.py
"""

import sys
import os
import json
import tempfile
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

import numpy as np
import pytest

from src.weight_optimizer import (
    _evaluate_weights,
    _get_future_price,
    load_optimized_weights,
    W_CURRENT,
    W_SECTOR_FIXED,
    MARKETS,
)


class TestEvaluateWeights:

    def _make_entries(self, n=20, win_rate=0.6):
        """Crea entries sintéticos con win_rate controlado."""
        entries = []
        for i in range(n):
            ret = 5.0 if i < int(n * win_rate) else -3.0
            entries.append({
                "ticker":      f"TICK{i}",
                "signal_date": f"2026-05-{i+1:02d}",
                "precio":      100.0,
                "s_macro":     70.0,   # scores altos para superar threshold de compra (58)
                "s_tec":       70.0,
                "s_fund":      65.0,
                "ret_21d":     ret,
            })
        return entries

    def test_returns_none_on_empty(self):
        assert _evaluate_weights(
            {"macro": 0.35, "tecnico": 0.25, "sector": 0.10, "fundamental": 0.30},
            []
        ) is None

    def test_returns_none_when_too_few_buy_signals(self):
        """Si el conjunto de pesos no genera suficientes señales de compra → None."""
        entries = [{
            "ticker": "X", "signal_date": "2026-05-01", "precio": 100.0,
            "s_macro": 10.0, "s_tec": 10.0, "s_fund": 10.0,  # score total muy bajo
            "ret_21d": 5.0
        }] * 3
        result = _evaluate_weights(
            {"macro": 0.35, "tecnico": 0.25, "sector": 0.10, "fundamental": 0.30},
            entries
        )
        assert result is None  # < 5 señales de compra

    def test_metrics_structure(self):
        """Retorna todas las métricas esperadas."""
        entries = self._make_entries(30, win_rate=0.7)
        result = _evaluate_weights(
            {"macro": 0.35, "tecnico": 0.25, "sector": 0.10, "fundamental": 0.30},
            entries
        )
        assert result is not None
        for key in ["ev_21d", "win_rate_21d", "avg_ret_21d", "sharpe_21d", "samples"]:
            assert key in result

    def test_higher_win_rate_entries_give_better_ev(self):
        """Entries con más ganadores → EV positivo."""
        good = self._make_entries(30, win_rate=0.80)
        bad  = self._make_entries(30, win_rate=0.20)
        w = {"macro": 0.35, "tecnico": 0.25, "sector": 0.10, "fundamental": 0.30}
        result_good = _evaluate_weights(w, good)
        result_bad  = _evaluate_weights(w, bad)
        if result_good and result_bad:
            assert result_good["ev_21d"] > result_bad["ev_21d"]

    def test_weights_sum_matters(self):
        """Sector fijo = W_SECTOR_FIXED."""
        entries = self._make_entries(20)
        w = {"macro": 0.35, "tecnico": 0.25, "sector": W_SECTOR_FIXED, "fundamental": 0.30}
        result = _evaluate_weights(w, entries)
        # No crashea con pesos correctos
        assert result is None or isinstance(result, dict)


class TestGetFuturePrice:

    def setup_method(self):
        self.price_index = {
            "GGAL.BA": {
                "2026-05-01": 100.0,
                "2026-05-02": 101.0,
                "2026-05-05": 105.0,
                "2026-05-06": 108.0,
            }
        }

    def test_gets_correct_future_price(self):
        """A 2 días desde 2026-05-01 → precio de 2026-05-05."""
        result = _get_future_price("GGAL.BA", "2026-05-01", self.price_index, horizon=2)
        assert result == 105.0

    def test_unknown_ticker_returns_none(self):
        result = _get_future_price("UNKN", "2026-05-01", self.price_index)
        assert result is None

    def test_beyond_available_data_returns_none(self):
        result = _get_future_price("GGAL.BA", "2026-05-01", self.price_index, horizon=10)
        assert result is None


class TestLoadOptimizedWeights:

    def test_returns_empty_dict_when_file_missing(self):
        """Sin archivo → dict vacío."""
        # Apuntar a un path que no existe
        import src.weight_optimizer as wo
        original = wo.WEIGHTS_PATH
        wo.WEIGHTS_PATH = "/tmp/nonexistent_weights_12345.json"
        result = load_optimized_weights()
        wo.WEIGHTS_PATH = original
        assert result == {}

    def test_loads_valid_file(self):
        """Archivo válido con pesos correctos → los carga."""
        valid_data = {
            "generated": "2026-05-30T12:00:00",
            "MERVAL": {"macro": 0.30, "tecnico": 0.30, "sector": 0.10, "fundamental": 0.30},
            "BOVESPA": {"macro": 0.25, "tecnico": 0.35, "sector": 0.10, "fundamental": 0.30},
            "SP500":   {"macro": 0.20, "tecnico": 0.30, "sector": 0.10, "fundamental": 0.40},
        }
        import src.weight_optimizer as wo
        original = wo.WEIGHTS_PATH

        with tempfile.NamedTemporaryFile(mode="w", suffix=".json", delete=False) as f:
            json.dump(valid_data, f)
            tmp_path = f.name

        wo.WEIGHTS_PATH = tmp_path
        result = load_optimized_weights()
        wo.WEIGHTS_PATH = original
        os.unlink(tmp_path)

        assert "MERVAL" in result
        assert result["MERVAL"]["macro"] == 0.30

    def test_rejects_invalid_weights_sum(self):
        """Pesos que no suman ~1 → no se cargan."""
        invalid_data = {
            "generated": "2026-05-30T12:00:00",
            "MERVAL": {"macro": 0.50, "tecnico": 0.50, "sector": 0.50, "fundamental": 0.50},
        }
        import src.weight_optimizer as wo
        original = wo.WEIGHTS_PATH

        with tempfile.NamedTemporaryFile(mode="w", suffix=".json", delete=False) as f:
            json.dump(invalid_data, f)
            tmp_path = f.name

        wo.WEIGHTS_PATH = tmp_path
        result = load_optimized_weights()
        wo.WEIGHTS_PATH = original
        os.unlink(tmp_path)

        assert "MERVAL" not in result

    def test_rejects_stale_file(self):
        """Archivo de hace >7 días → no se carga."""
        old_data = {
            "generated": "2020-01-01T00:00:00",  # muy viejo
            "MERVAL": {"macro": 0.30, "tecnico": 0.30, "sector": 0.10, "fundamental": 0.30},
        }
        import src.weight_optimizer as wo
        original = wo.WEIGHTS_PATH

        with tempfile.NamedTemporaryFile(mode="w", suffix=".json", delete=False) as f:
            json.dump(old_data, f)
            tmp_path = f.name

        wo.WEIGHTS_PATH = tmp_path
        result = load_optimized_weights()
        wo.WEIGHTS_PATH = original
        os.unlink(tmp_path)

        assert result == {}


class TestWCurrentIntegrity:
    """Verifica que los pesos hardcoded del fallback sean válidos."""

    def test_all_markets_covered(self):
        for market in MARKETS + ["DEFAULT"]:
            assert market in W_CURRENT

    def test_weights_sum_to_one(self):
        for market, w in W_CURRENT.items():
            total = w["macro"] + w["tecnico"] + w["sector"] + w["fundamental"]
            assert abs(total - 1.0) < 0.01, f"{market} pesos suman {total}"

    def test_sector_weight_is_fixed(self):
        for market, w in W_CURRENT.items():
            assert w["sector"] == W_SECTOR_FIXED, f"{market} sector != {W_SECTOR_FIXED}"

    def test_all_weights_positive(self):
        for market, w in W_CURRENT.items():
            for k, v in w.items():
                assert v > 0, f"{market}.{k} = {v} (debe ser > 0)"
