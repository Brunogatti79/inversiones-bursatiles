"""
tests/test_weights_provenance.py

Prioridad 1 (roadmap externo, 25/06/2026) — "conciencia operativa" sobre
los pesos V1: cuando optimized_weights.json viene 100% de replay
sintético (historical_replay.py, n_real_entries=0), eso tiene que ser
visible en 3 lugares, no solo calculable a mano leyendo el JSON:
  1. weight_optimizer.weights_provenance() — la fuente de verdad
  2. monitor._read_weights_metrics() — la reusa para health_metrics.json / /api/health
  3. notifier._synthetic_weights_section() — la reusa para el mensaje de Telegram

Los tres puntos comparten el mismo archivo real ("data/optimized_weights.json",
ruta relativa) -- se usa chdir a un tmp_path en vez de monkeypatchear cada
constante WEIGHTS_PATH por separado (cada módulo define la suya).
"""
import sys
import os
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

import json
import pytest

from src.weight_optimizer import weights_provenance, MARKETS
import src.monitor as monitor
from src.notifier import _synthetic_weights_section


@pytest.fixture
def _isolated_cwd(tmp_path, monkeypatch):
    monkeypatch.chdir(tmp_path)
    os.makedirs("data", exist_ok=True)
    return tmp_path


def _write_weights(mode="sensitivity", days=3, **market_overrides):
    data = {"generated": "2026-06-24T16:58:03.050031", "mode": mode, "days_history": days}
    for market in MARKETS:
        w = {"macro": 0.30, "tecnico": 0.30, "sector": 0.10, "fundamental": 0.30,
             "n_real_entries": 0, "n_replay_entries": 600}
        w.update(market_overrides.get(market, {}))
        data[market] = w
    with open("data/optimized_weights.json", "w") as f:
        json.dump(data, f)


# ── weight_optimizer.weights_provenance() ───────────────────────────────

class TestWeightsProvenance:

    def test_no_file_returns_unavailable(self, _isolated_cwd):
        prov = weights_provenance()
        assert prov["available"] is False
        assert prov["is_synthetic"] is False

    def test_all_markets_synthetic_real_snapshot(self, _isolated_cwd):
        """Reproduce el snapshot real del repo al momento de este fix:
        modo sensitivity, 0 entradas reales en los 3 mercados."""
        _write_weights(mode="sensitivity", days=3)
        prov = weights_provenance()
        assert prov["available"] is True
        assert prov["is_synthetic"] is True
        assert all(v["is_synthetic"] for v in prov["markets"].values())

    def test_walk_forward_with_real_entries_not_synthetic(self, _isolated_cwd):
        overrides = {m: {"n_real_entries": 40, "n_replay_entries": 0} for m in MARKETS}
        _write_weights(mode="walk_forward", days=20, **overrides)
        prov = weights_provenance()
        assert prov["is_synthetic"] is False
        assert all(not v["is_synthetic"] for v in prov["markets"].values())

    def test_one_market_synthetic_others_real_still_flagged(self, _isolated_cwd):
        """Caso intermedio: si SOLO un mercado sigue en 0 reales, el flag
        global is_synthetic debe seguir en True -- no alcanza con que la
        mayoría ya tenga historia real."""
        overrides = {
            "MERVAL":  {"n_real_entries": 40, "n_replay_entries": 0},
            "BOVESPA": {"n_real_entries": 35, "n_replay_entries": 0},
            "SP500":   {"n_real_entries": 0,  "n_replay_entries": 900},
        }
        _write_weights(mode="walk_forward", days=20, **overrides)
        prov = weights_provenance()
        assert prov["is_synthetic"] is True
        assert prov["markets"]["SP500"]["is_synthetic"] is True
        assert prov["markets"]["MERVAL"]["is_synthetic"] is False

    def test_sensitivity_mode_flags_synthetic_even_with_real_entries_present(self, _isolated_cwd):
        """mode != 'walk_forward' por sí solo ya marca is_synthetic=True a
        nivel global, aunque cada mercado individual ya tenga algunas
        entradas reales (>0) -- sensitivity mode significa que TODAVÍA no
        hay los 15 días mínimos para confiar en walk-forward, sin importar
        que ya haya empezado a acumularse historia real."""
        overrides = {m: {"n_real_entries": 8, "n_replay_entries": 0} for m in MARKETS}
        _write_weights(mode="sensitivity", days=8, **overrides)
        prov = weights_provenance()
        assert prov["is_synthetic"] is True
        # a nivel de mercado individual, ninguno está en 0 reales puro
        assert all(not v["is_synthetic"] for v in prov["markets"].values())


# ── monitor._read_weights_metrics() ──────────────────────────────────────

class TestReadWeightsMetrics:

    def test_no_file_returns_none_synthetic(self, _isolated_cwd):
        result = monitor._read_weights_metrics()
        assert result["optimized_weights_is_synthetic"] is None

    def test_synthetic_weights_reflected_in_health_metrics(self, _isolated_cwd):
        _write_weights(mode="sensitivity", days=3)
        result = monitor._read_weights_metrics()
        assert result["optimized_weights_is_synthetic"] is True
        assert set(result["optimized_weights_synthetic_markets"]) == set(MARKETS)
        assert result["optimized_weights_mode"] == "sensitivity"

    def test_real_weights_not_flagged(self, _isolated_cwd):
        overrides = {m: {"n_real_entries": 50, "n_replay_entries": 0} for m in MARKETS}
        _write_weights(mode="walk_forward", days=20, **overrides)
        result = monitor._read_weights_metrics()
        assert result["optimized_weights_is_synthetic"] is False
        assert result["optimized_weights_synthetic_markets"] == []


# ── notifier._synthetic_weights_section() ───────────────────────────────

class TestSyntheticWeightsSection:

    def test_empty_provenance_returns_empty_string(self):
        assert _synthetic_weights_section(None) == ""
        assert _synthetic_weights_section({}) == ""

    def test_not_synthetic_returns_empty_string(self):
        prov = {"available": True, "is_synthetic": False, "markets": {}}
        assert _synthetic_weights_section(prov) == ""

    def test_synthetic_mentions_mode_and_markets(self):
        prov = {
            "available": True, "is_synthetic": True, "mode": "sensitivity", "days_history": 3,
            "markets": {
                "MERVAL":  {"is_synthetic": True},
                "BOVESPA": {"is_synthetic": True},
                "SP500":   {"is_synthetic": True},
            },
        }
        section = _synthetic_weights_section(prov)
        assert "sensitivity" in section
        assert "MERVAL" in section and "BOVESPA" in section and "SP500" in section
        assert "100% sintéticos" in section

    def test_partial_synthetic_lists_only_affected_markets(self):
        prov = {
            "available": True, "is_synthetic": True, "mode": "walk_forward", "days_history": 20,
            "markets": {
                "MERVAL":  {"is_synthetic": False},
                "BOVESPA": {"is_synthetic": False},
                "SP500":   {"is_synthetic": True},
            },
        }
        section = _synthetic_weights_section(prov)
        assert "SP500" in section
        assert "MERVAL" not in section
        assert "BOVESPA" not in section
