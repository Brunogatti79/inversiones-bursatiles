"""
tests/test_exposure_total.py

Diseño de "Exposure Total" (devolución externa, 25/06/2026) -- generaliza
el escalón binario del kill switch (score<35 -> 0, score>=35 -> full) en
una rampa continua entre 35 y 70, combinada con regime_factor. Reusa
exactamente los cortes ya existentes del label de confidence global (70/
50/35) -- cero números nuevos inventados sin datos.

MODO SOMBRA: compute_exposure_factor() se calcula y se expone (Telegram +
health_metrics) pero NO multiplica kelly_f/suggested_pct en
portfolio_optimizer.py todavía. Estos tests cubren el cálculo y la
sección de Telegram, no una integración con el optimizer porque
deliberadamente no existe todavía.
"""
import sys
import os
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

import pytest

from src.confidence_score import compute_exposure_factor, apply_exposure_factor, KILL_SWITCH_THRESHOLD
from src.notifier import _exposure_shadow_section


def _gc(score, kill_switch_active=False):
    return {"global_score": score, "kill_switch_active": kill_switch_active}


def _buy_signal(**overrides):
    base = {"ticker": "GGAL.BA", "kelly_f": 0.10, "kelly_half": 5.0, "suggested_pct": 8.0}
    base.update(overrides)
    return base


class TestApplyExposureFactor:

    def test_scales_all_three_fields(self):
        exposure = compute_exposure_factor(_gc(62))  # confidence_component=0.84 exacto
        factor = exposure["exposure_factor"]
        signals = [_buy_signal()]
        out = apply_exposure_factor(signals, exposure)
        assert out[0]["kelly_f"] == round(0.10 * factor, 4)
        assert out[0]["kelly_half"] == round(5.0 * factor, 1)
        assert out[0]["suggested_pct"] == round(8.0 * factor, 1)
        assert out[0]["exposure_factor_applied"] == factor

    def test_full_exposure_does_not_touch_values(self):
        exposure = compute_exposure_factor(_gc(85))  # factor 1.0
        signals = [_buy_signal()]
        out = apply_exposure_factor(signals, exposure)
        assert out[0]["kelly_f"] == 0.10
        assert out[0]["kelly_half"] == 5.0
        assert out[0]["suggested_pct"] == 8.0
        assert out[0]["exposure_factor_applied"] == 1.0

    def test_kill_switch_zero_factor_zeroes_everything(self):
        exposure = compute_exposure_factor(_gc(85, kill_switch_active=True))
        signals = [_buy_signal()]
        out = apply_exposure_factor(signals, exposure)
        assert out[0]["kelly_f"] == 0.0
        assert out[0]["kelly_half"] == 0.0
        assert out[0]["suggested_pct"] == 0.0

    def test_signals_without_kelly_fields_untouched(self):
        """Señales que no son COMPRA (portfolio_optimizer no las toca) no
        tienen kelly_f/kelly_half/suggested_pct -- no debe crashear ni
        agregarles esos campos de la nada."""
        exposure = compute_exposure_factor(_gc(62))
        signals = [{"ticker": "X", "signal": "🔴 VENTA"}]
        out = apply_exposure_factor(signals, exposure)
        assert "kelly_f" not in out[0]
        assert out[0]["exposure_factor_applied"] == exposure["exposure_factor"]

    def test_empty_signals_or_exposure_returns_unchanged(self):
        assert apply_exposure_factor([], {}) == []
        signals = [_buy_signal()]
        assert apply_exposure_factor(signals, {}) == signals
        assert apply_exposure_factor(None, {}) is None

    def test_relative_proportions_preserved_across_signals(self):
        """El factor es uniforme -- la proporción RELATIVA entre dos
        señales del mismo día no debe cambiar, solo el tamaño absoluto."""
        exposure = compute_exposure_factor(_gc(62))
        signals = [_buy_signal(ticker="A", suggested_pct=10.0),
                   _buy_signal(ticker="B", suggested_pct=5.0)]
        out = apply_exposure_factor(signals, exposure)
        ratio_before = 10.0 / 5.0
        ratio_after = out[0]["suggested_pct"] / out[1]["suggested_pct"]
        assert ratio_after == pytest.approx(ratio_before, rel=1e-3)

    def test_appends_note_when_factor_reduces_position(self):
        exposure = compute_exposure_factor(_gc(62))
        signals = [_buy_signal(allocation_notes="Allocar 8.0% del capital.")]
        out = apply_exposure_factor(signals, exposure)
        assert "Exposure Total recortó" in out[0]["allocation_notes"]
        assert "Allocar 8.0% del capital." in out[0]["allocation_notes"]

    def test_no_note_appended_when_factor_is_full(self):
        exposure = compute_exposure_factor(_gc(85))
        signals = [_buy_signal(allocation_notes="Allocar 8.0% del capital.")]
        out = apply_exposure_factor(signals, exposure)
        assert out[0]["allocation_notes"] == "Allocar 8.0% del capital."


class TestComputeExposureFactor:

    def test_score_70_or_above_is_full_exposure(self):
        r = compute_exposure_factor(_gc(70))
        assert r["exposure_factor"] == 1.0
        r2 = compute_exposure_factor(_gc(95))
        assert r2["exposure_factor"] == 1.0

    def test_score_50_gives_060(self):
        r = compute_exposure_factor(_gc(50))
        assert r["confidence_component"] == 0.60

    def test_score_35_gives_minimum_nonzero_030(self):
        """35 es KILL_SWITCH_THRESHOLD -- justo en el borde, todavía no
        está activo el kill switch (la condición es score < 35, no <=),
        así que debe dar el piso de la rampa, 0.30, no cero."""
        r = compute_exposure_factor(_gc(KILL_SWITCH_THRESHOLD))
        assert r["confidence_component"] == 0.30

    def test_score_just_below_threshold_is_zero(self):
        r = compute_exposure_factor(_gc(KILL_SWITCH_THRESHOLD - 0.1))
        assert r["confidence_component"] == 0.0
        assert r["exposure_factor"] == 0.0

    def test_kill_switch_active_overrides_high_score_to_zero(self):
        """Un hard trigger (ej. precio inválido) puede activar el kill
        switch aunque el score ponderado siga alto -- compute_exposure_factor
        debe respetar eso, nunca dar más exposición que el kill switch."""
        r = compute_exposure_factor(_gc(85, kill_switch_active=True))
        assert r["exposure_factor"] == 0.0

    def test_monotonic_increasing_with_score(self):
        scores = [35, 40, 45, 50, 55, 60, 65, 70]
        factors = [compute_exposure_factor(_gc(s))["confidence_component"] for s in scores]
        assert factors == sorted(factors)

    def test_regime_factor_multiplies_linearly(self):
        base = compute_exposure_factor(_gc(80), regime_factor=1.0)
        high_vol = compute_exposure_factor(_gc(80), regime_factor=0.75)
        low_vol = compute_exposure_factor(_gc(80), regime_factor=1.10)
        assert high_vol["exposure_factor"] == pytest.approx(0.75, rel=1e-3)
        assert low_vol["exposure_factor"] == pytest.approx(1.10, rel=1e-3)
        assert high_vol["exposure_factor"] < base["exposure_factor"] < low_vol["exposure_factor"]

    def test_default_regime_factor_is_neutral(self):
        r = compute_exposure_factor(_gc(80))
        assert r["regime_component"] == 1.0

    def test_real_snapshot_today_gives_084(self):
        """Snapshot real aproximado del confidence global de hoy (~62) --
        regresión directa para que un cambio futuro en la fórmula se note."""
        r = compute_exposure_factor(_gc(62))
        assert r["exposure_factor"] == pytest.approx(0.84, abs=0.01)

    def test_marked_as_active_in_production(self):
        """Activado el 25/06/2026 a pedido explícito de Bruno -- si esto
        se revierte a modo sombra alguna vez, debe ser un cambio
        deliberado que actualice este test, no un accidente."""
        r = compute_exposure_factor(_gc(80))
        assert r["active_in_production"] is True


class TestExposureShadowSection:

    def test_none_or_empty_returns_empty_string(self):
        assert _exposure_shadow_section(None) == ""
        assert _exposure_shadow_section({}) == ""

    def test_full_exposure_returns_empty_string(self):
        """Si el factor es 1.0, no hay nada interesante que comunicar."""
        exposure = compute_exposure_factor(_gc(90))
        assert _exposure_shadow_section(exposure) == ""

    def test_reduced_exposure_shows_percentage(self):
        exposure = compute_exposure_factor(_gc(62))
        section = _exposure_shadow_section(exposure)
        assert "84%" in section
        assert "aplicado" in section

    def test_shows_even_when_active_in_production_true(self):
        """Activo desde el 25/06/2026 -- a diferencia del período en modo
        sombra, ahora la sección debe mostrarse SIEMPRE que el factor sea
        <1.0, sin importar el flag active_in_production (ya no condiciona
        nada, el factor aplicado es real)."""
        exposure = compute_exposure_factor(_gc(62))
        section_con_flag_true = _exposure_shadow_section(exposure)
        exposure_sin_flag = dict(exposure)
        del exposure_sin_flag["active_in_production"]
        section_sin_flag = _exposure_shadow_section(exposure_sin_flag)
        assert section_con_flag_true == section_sin_flag != ""

    def test_zero_exposure_from_kill_switch_shown(self):
        exposure = compute_exposure_factor(_gc(80, kill_switch_active=True))
        section = _exposure_shadow_section(exposure)
        assert "0%" in section
