"""
tests/test_predictor_health_and_override.py

Cubre dos fixes de la misma sesión (Prioridad 1, roadmap externo,
25/06/2026), relacionados pero en módulos distintos:

  1. predictor_health.py: compute_predictor_health() ahora cae a
     predictor_validation.json cuando backtest_results.json no existe o
     no tiene muestras suficientes. Antes de este fix, devolvía siempre
     UNKNOWN/factor=1.00 mientras la evidencia de un predictor en banda
     WARNING ya estaba disponible en otro archivo.

  2. pipeline.py: apply_prediction_override() estaba definida con lógica
     completa pero NUNCA se llamaba desde ningún lado (confirmado con grep
     en todo el repo) — el tooltip del dashboard describía un
     comportamiento que no existía. Se activa en esta sesión, gateada por
     predictor_health: si el predictor está DEGRADED, se omite la Regla 1
     (la basada en pred_21d).
"""
import sys
import os
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

import json
import pytest

import src.predictor_health as ph
from src.pipeline import apply_prediction_override


# ── predictor_health.py: fallback a predictor_validation.json ──────────

@pytest.fixture(autouse=True)
def _isolate_paths(tmp_path, monkeypatch):
    monkeypatch.setattr(ph, "BACKTEST_PATH", str(tmp_path / "backtest_results.json"))
    monkeypatch.setattr(ph, "VALIDATION_PATH", str(tmp_path / "predictor_validation.json"))
    monkeypatch.setattr(ph, "HEALTH_PATH", str(tmp_path / "predictor_health.json"))
    monkeypatch.chdir(tmp_path)
    os.makedirs("data", exist_ok=True)


def _write(path_attr, content):
    path = getattr(ph, path_attr)
    with open(path, "w") as f:
        json.dump(content, f)


class TestComputePredictorHealthFallback:

    def test_neither_source_available_returns_unknown(self):
        result = ph.compute_predictor_health()
        assert result["health"] == "UNKNOWN"
        assert result["source"] == "unknown"
        assert result["confidence_factor"] == 1.00

    def test_backtest_available_uses_backtest_not_validation(self):
        _write("BACKTEST_PATH", {"predictor": {"samples": 40, "directional_accuracy": 0.60, "mae": 5.0}})
        _write("VALIDATION_PATH", {"global": {"predictor": {"n": 2000, "directional_accuracy": 0.20, "mae": 20.0}}})

        result = ph.compute_predictor_health()
        assert result["source"] == "backtest"
        assert result["rolling_accuracy"] == 0.60
        assert result["health"] == "OK"

    def test_falls_back_to_validation_when_no_backtest(self):
        """Caso real de hoy: sin backtest_results.json (necesita 6 días,
        hay 3), pero predictor_validation.json ya tiene 672 snapshots con
        accuracy 0.51 -- debe usarse en vez de devolver UNKNOWN."""
        _write("VALIDATION_PATH", {"global": {"predictor": {"n": 2016, "directional_accuracy": 0.51, "mae": 8.62}}})

        result = ph.compute_predictor_health()
        assert result["source"] == "predictor_validation"
        assert result["health"] == "WARNING"
        assert result["rolling_accuracy"] == 0.51
        assert 0.0 < result["confidence_factor"] < 1.0

    def test_validation_with_too_few_samples_is_ignored(self):
        """predictor_validation.json exige un piso de muestras más alto
        que backtest real (30 vs 5) -- es una fuente menos directa."""
        _write("VALIDATION_PATH", {"global": {"predictor": {"n": 10, "directional_accuracy": 0.51, "mae": 8.62}}})

        result = ph.compute_predictor_health()
        assert result["health"] == "UNKNOWN"
        assert result["source"] == "unknown"

    def test_backtest_with_too_few_samples_falls_back_to_validation(self):
        _write("BACKTEST_PATH", {"predictor": {"samples": 2, "directional_accuracy": 0.80, "mae": 2.0}})
        _write("VALIDATION_PATH", {"global": {"predictor": {"n": 2016, "directional_accuracy": 0.51, "mae": 8.62}}})

        result = ph.compute_predictor_health()
        assert result["source"] == "predictor_validation"

    def test_degraded_accuracy_from_validation(self):
        _write("VALIDATION_PATH", {"global": {"predictor": {"n": 500, "directional_accuracy": 0.30, "mae": 12.0}}})
        result = ph.compute_predictor_health()
        assert result["health"] == "DEGRADED"
        assert result["confidence_factor"] <= 0.75

    def test_real_production_snapshot_gives_warning(self):
        """Mismos valores reales de data/predictor_validation.json al
        momento de este fix (25/06/2026) -- regresión directa."""
        _write("VALIDATION_PATH", {
            "generated": "2026-06-24T17:18:57.826484",
            "n_snapshots": 672,
            "global": {"predictor": {"n": 2016, "directional_accuracy": 0.51,
                                       "mae": 8.622, "correlation": -0.02}},
        })
        result = ph.compute_predictor_health()
        assert result["health"] == "WARNING"
        assert result["source"] == "predictor_validation"


# ── pipeline.py: apply_prediction_override (activación + gating) ───────

def _signal(**overrides):
    base = {"ticker": "GGAL.BA", "signal": "🟢 COMPRA", "signal_v2": "🟢 COMPRA",
            "pred_21d": -8.0, "ret_anual": 5.0}
    base.update(overrides)
    return base


class TestApplyPredictionOverride:

    def test_rule1_downgrades_buy_on_negative_prediction(self):
        """Caso base: sin health (None) -> Regla 1 se aplica normalmente,
        igual que el comportamiento original de la función (que nunca se
        había ejecutado en producción, pero la lógica interna era ésta)."""
        signals = [_signal(pred_21d=-2.0)]
        out = apply_prediction_override(signals, predictor_health=None)
        assert out[0]["signal"] == "🟡 NEUTRAL/ESPERAR"
        assert "signal_override" in out[0]

    def test_rule1_strong_sell_below_minus_10(self):
        signals = [_signal(pred_21d=-12.0)]
        out = apply_prediction_override(signals, predictor_health={"health": "OK"})
        assert out[0]["signal"] == "🔴 VENTA"

    def test_rule1_partial_sell_below_minus_5(self):
        signals = [_signal(pred_21d=-7.0)]
        out = apply_prediction_override(signals, predictor_health={"health": "WARNING"})
        assert out[0]["signal"] == "🟠 VENTA PARCIAL"

    def test_rule1_skipped_when_predictor_degraded(self):
        """El fix central de esta sesión: con predictor DEGRADED, la Regla
        1 (basada en pred_21d) se omite por completo -- no tiene sentido
        dejarle la 'última palabra' a un predictor que el propio sistema
        marca como peor que random."""
        signals = [_signal(pred_21d=-12.0)]  # hubiera sido VENTA fuerte
        out = apply_prediction_override(signals, predictor_health={"health": "DEGRADED"})
        assert out[0]["signal"] == "🟢 COMPRA"  # sin cambios
        assert "signal_override" not in out[0]

    def test_rule2_structural_drop_always_applies_even_if_degraded(self):
        """La Regla 2 (caída anual estructural) no depende del predictor
        -- nunca se omite, ni con DEGRADED."""
        signals = [_signal(pred_21d=None, ret_anual=-50.0)]
        out = apply_prediction_override(signals, predictor_health={"health": "DEGRADED"})
        assert out[0]["signal"] == "🟡 NEUTRAL/ESPERAR"
        assert "signal_override" in out[0]

    def test_signal_v2_synced_when_also_buy(self):
        signals = [_signal(pred_21d=-12.0, signal_v2="⭐ COMPRA FUERTE")]
        out = apply_prediction_override(signals, predictor_health={"health": "OK"})
        assert out[0]["signal_v2"] == "🔴 VENTA"

    def test_sell_signals_untouched_by_rule1(self):
        """Regla 1 solo aplica si is_buy -- una señal ya en VENTA no se
        toca por esta regla aunque pred_21d sea negativo."""
        signals = [_signal(signal="🔴 VENTA", signal_v2="🔴 VENTA", pred_21d=-3.0)]
        out = apply_prediction_override(signals, predictor_health={"health": "OK"})
        assert out[0]["signal"] == "🔴 VENTA"
        assert "signal_override" not in out[0]

    def test_no_predictor_data_leaves_signal_untouched(self):
        signals = [_signal(pred_21d=None, ret_anual=5.0)]
        out = apply_prediction_override(signals, predictor_health={"health": "OK"})
        assert out[0]["signal"] == "🟢 COMPRA"

    def test_missing_predictor_health_dict_defaults_to_not_skipping(self):
        """predictor_health=None (ej. si compute_predictor_health() falló
        y el except de pipeline.py dejó predictor_health={}) no debe
        tratarse como DEGRADED -- la Regla 1 debe seguir aplicando, no
        quedar bloqueada por un health vacío."""
        signals = [_signal(pred_21d=-12.0)]
        out = apply_prediction_override(signals, predictor_health={})
        assert out[0]["signal"] == "🔴 VENTA"
