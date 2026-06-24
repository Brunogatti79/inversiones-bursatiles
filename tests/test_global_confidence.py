"""
tests/test_global_confidence.py

Tests para compute_global_confidence() y apply_kill_switch() en
src/confidence_score.py (mejora 3.1 + 3.5).

Cubre: el score ponderado en escenarios límpios y degradados, los dos
triggers duros del kill switch (data_validator ERROR / exceso de alertas
críticas de quality_check), y que apply_kill_switch frene kelly_half en
TODAS las señales sin distinguir por confianza individual cuando está
activo -- es un circuit breaker de sistema, no un filtro por ticker.
"""
import pytest

from src.confidence_score import (
    compute_global_confidence,
    apply_kill_switch,
    KILL_SWITCH_THRESHOLD,
    MIN_CRITICAL_FOR_KILL,
)


def _healthy_inputs(n_signals=60):
    """Inputs de un run sano: todo OK, sin alertas, predictor y macro bien."""
    return dict(
        n_signals=n_signals,
        quality_resumen={"ok": n_signals, "criticas": 0, "advertencias": 0,
                          "total_alertas": 0, "nivel_global": "✅ OK"},
        predictor_health={"health": "OK", "rolling_accuracy": 0.61},
        macro_confidence={
            "MERVAL":  {"score": 80.0, "label": "ALTA"},
            "BOVESPA": {"score": 75.0, "label": "ALTA"},
            "SP500":   {"score": 90.0, "label": "ALTA"},
        },
        validacion_nivel="OK",
        sla_status="OK",
    )


class TestHealthyRun:

    def test_all_green_yields_high_score_no_kill_switch(self):
        result = compute_global_confidence(**_healthy_inputs())
        assert result["global_score"] >= 70
        assert result["label"] == "🟢 Confiable"
        assert result["kill_switch_active"] is False
        assert result["kill_switch_reasons"] == []

    def test_components_sum_to_global_score_via_weights(self):
        result = compute_global_confidence(**_healthy_inputs())
        weights = result["weights"]
        expected = round(sum(result["components"][k] * w for k, w in weights.items()), 1)
        assert result["global_score"] == expected


class TestMissingDataDefaultsToNeutral:
    """Sin datos de un componente (ej. corrida vieja antes de wirear
    quality_check, o macro_confidence vacío) no debe penalizar como si
    fuera un problema real -- debe quedar neutral."""

    def test_no_quality_resumen_does_not_tank_score(self):
        result = compute_global_confidence(
            n_signals=0, quality_resumen={}, predictor_health={"health": "OK"},
            macro_confidence={}, validacion_nivel="OK", sla_status="OK",
        )
        # calidad_datos cae al neutral (clean_ratio=1.0 sin penalización) y
        # macro cae a 50 (neutral) -- el resto en verde. Score debe ser
        # razonablemente alto, no colapsar a 0 por falta de datos.
        assert result["global_score"] > 60

    def test_unknown_predictor_health_is_neutral_not_punitive(self):
        result = compute_global_confidence(**{**_healthy_inputs(), "predictor_health": {}})
        assert result["components"]["predictor"] == 65.0  # neutral, no 0 ni 100


class TestSoftDegradation:

    def test_degraded_predictor_lowers_score_but_not_necessarily_kill_switch(self):
        inputs = _healthy_inputs()
        inputs["predictor_health"] = {"health": "DEGRADED"}
        result = compute_global_confidence(**inputs)
        baseline = compute_global_confidence(**_healthy_inputs())["global_score"]
        assert result["global_score"] < baseline

    def test_low_macro_confidence_lowers_score(self):
        inputs = _healthy_inputs()
        inputs["macro_confidence"] = {
            "MERVAL": {"score": 20.0, "label": "BAJA"},
            "BOVESPA": {"score": 20.0, "label": "BAJA"},
            "SP500": {"score": 20.0, "label": "BAJA"},
        }
        result = compute_global_confidence(**inputs)
        baseline = compute_global_confidence(**_healthy_inputs())["global_score"]
        assert result["global_score"] < baseline

    def test_warnings_accumulate_small_penalty(self):
        inputs = _healthy_inputs()
        inputs["quality_resumen"] = {"ok": 50, "criticas": 0, "advertencias": 10}
        result = compute_global_confidence(**inputs)
        baseline = compute_global_confidence(**_healthy_inputs())["global_score"]
        assert result["global_score"] < baseline
        # Las advertencias solas (sin críticas) no deben activar el kill switch
        assert result["kill_switch_active"] is False


class TestHardTriggers:
    """Los 2 triggers duros activan el kill switch sin importar qué tan
    alto sea el resto de los componentes -- son señales de que algo está
    estructuralmente roto, no solo "bajo en confianza"."""

    def test_data_validator_error_forces_kill_switch_even_with_perfect_score(self):
        inputs = _healthy_inputs()
        inputs["validacion_nivel"] = "ERROR"
        result = compute_global_confidence(**inputs)
        assert result["kill_switch_active"] is True
        assert any("data_validator" in r for r in result["kill_switch_reasons"])

    def test_excess_critical_quality_alerts_forces_kill_switch(self):
        inputs = _healthy_inputs()
        inputs["quality_resumen"] = {
            "ok": 55, "criticas": MIN_CRITICAL_FOR_KILL, "advertencias": 0,
        }
        result = compute_global_confidence(**inputs)
        assert result["kill_switch_active"] is True
        assert any("quality_check" in r for r in result["kill_switch_reasons"])

    def test_below_threshold_critical_count_does_not_force_kill_switch_alone(self):
        inputs = _healthy_inputs()
        inputs["quality_resumen"] = {
            "ok": 58, "criticas": MIN_CRITICAL_FOR_KILL - 1, "advertencias": 0,
        }
        result = compute_global_confidence(**inputs)
        # Por debajo del umbral duro -- si el kill switch se activa acá,
        # tiene que ser porque el score ponderado cayó, no por el trigger duro.
        assert not any("quality_check encontró" in r for r in result["kill_switch_reasons"])


class TestWeightedThresholdTrigger:

    def test_global_score_below_threshold_activates_kill_switch(self):
        inputs = dict(
            n_signals=60,
            quality_resumen={"ok": 20, "criticas": 2, "advertencias": 5},
            predictor_health={"health": "DEGRADED"},
            macro_confidence={"MERVAL": {"score": 10.0}, "BOVESPA": {"score": 10.0}, "SP500": {"score": 10.0}},
            validacion_nivel="WARNING",
            sla_status="CRITICAL",
        )
        result = compute_global_confidence(**inputs)
        assert result["global_score"] < KILL_SWITCH_THRESHOLD
        assert result["kill_switch_active"] is True
        assert any(str(result["global_score"]) in r for r in result["kill_switch_reasons"])


class TestApplyKillSwitch:

    def test_inactive_kill_switch_leaves_kelly_untouched(self):
        signals = [{"ticker": "AAPL", "kelly_half": 2.5, "kelly_half_adj": 1.8}]
        global_conf = {"kill_switch_active": False, "kill_switch_reasons": []}
        result = apply_kill_switch(signals, global_conf)
        assert result[0]["kill_switch_active"] is False
        assert result[0]["kelly_half"] == 2.5
        assert result[0]["kelly_half_adj"] == 1.8

    def test_active_kill_switch_zeroes_kelly_on_all_signals(self):
        signals = [
            {"ticker": "AAPL", "kelly_half": 2.5, "kelly_half_adj": 1.8},
            {"ticker": "GGAL.BA", "kelly_half": 5.0, "kelly_half_adj": 4.2},
        ]
        global_conf = {
            "kill_switch_active": True,
            "kill_switch_reasons": ["score global=20.0 por debajo del umbral=35"],
        }
        result = apply_kill_switch(signals, global_conf)
        for sig in result:
            assert sig["kill_switch_active"] is True
            assert sig["kelly_half"] == 0.0
            assert sig["kelly_half_adj"] == 0.0
            assert sig["kill_switch_reasons"] == global_conf["kill_switch_reasons"]

    def test_signals_without_kelly_fields_are_not_crashed_or_invented(self):
        """Señales NEUTRAL/VENTA no tienen kelly_half -- no debe agregarse
        de la nada, solo marcar el flag."""
        signals = [{"ticker": "KO", "signal": "VENTA"}]
        global_conf = {"kill_switch_active": True, "kill_switch_reasons": ["x"]}
        result = apply_kill_switch(signals, global_conf)
        assert result[0]["kill_switch_active"] is True
        assert "kelly_half" not in result[0]

    def test_empty_signals_list_returns_empty(self):
        assert apply_kill_switch([], {"kill_switch_active": True}) == []
