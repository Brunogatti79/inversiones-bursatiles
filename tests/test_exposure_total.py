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

from src.confidence_score import compute_exposure_factor, KILL_SWITCH_THRESHOLD
from src.notifier import _exposure_shadow_section


def _gc(score, kill_switch_active=False):
    return {"global_score": score, "kill_switch_active": kill_switch_active}


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

    def test_marked_as_shadow_not_active(self):
        """Flag explícito de que esto NO está activo en producción --
        si algún día se activa, este test debe actualizarse a mano (no
        por accidente)."""
        r = compute_exposure_factor(_gc(80))
        assert r["active_in_production"] is False


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
        assert "no activo" in section

    def test_active_in_production_true_suppresses_section(self):
        """Si en el futuro esto se activa de verdad, esta sección de
        'diseño, no activo' debe dejar de mostrarse -- cubre ese caso
        aunque hoy active_in_production siempre sea False."""
        exposure = compute_exposure_factor(_gc(62))
        exposure["active_in_production"] = True
        assert _exposure_shadow_section(exposure) == ""

    def test_zero_exposure_from_kill_switch_shown(self):
        exposure = compute_exposure_factor(_gc(80, kill_switch_active=True))
        section = _exposure_shadow_section(exposure)
        assert "0%" in section
