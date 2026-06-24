"""
tests/test_weight_optimizer_replay_integration.py

Tests para la conexión de historical_replay.json con weight_optimizer.py
(24/06/2026, punto 2 del roadmap). Antes de este fix, las ~3.500
observaciones que historical_replay.py genera no tenían ningún consumidor
-- load_replay_observations() existía pero nada la llamaba. Estos tests
cubren:
  1. _replay_obs_to_entries(): traducción y validación de observaciones.
  2. _optimize_market(): el replay aumenta el pool SOLO cuando el modo no
     es walk_forward, y la cantidad real/replay queda registrada para
     auditoría.
  3. run_weight_optimization(): el gate inicial se relaja cuando hay datos
     de replay disponibles, aunque haya pocos días reales.
"""
import pytest

import src.weight_optimizer as wo


def _replay_obs(market="MERVAL", n=20, ticker="GGAL.BA", ret_21d=8.0,
                 s_macro=60.0, s_tec=55.0, s_fund=70.0, precio=150.0):
    """Genera N observaciones de replay sintéticas pero válidas."""
    return [
        {
            "ticker": ticker, "mercado": market, "date_idx": i,
            "precio": precio, "s_macro": s_macro, "s_tec": s_tec,
            "s_fund": s_fund, "ret_5d": 1.0, "ret_10d": 4.0, "ret_21d": ret_21d,
        }
        for i in range(n)
    ]


class TestReplayObsToEntries:

    def test_filters_by_market(self):
        obs = _replay_obs(market="MERVAL", n=3) + _replay_obs(market="BOVESPA", n=2)
        entries = wo._replay_obs_to_entries("MERVAL", obs)
        assert len(entries) == 3
        assert all(e["ticker"] == "GGAL.BA" for e in entries)

    def test_maps_field_names_correctly(self):
        obs = _replay_obs(n=1, s_macro=61.0, s_tec=44.0, s_fund=72.0, ret_21d=5.5, precio=200.0)
        entries = wo._replay_obs_to_entries("MERVAL", obs)
        e = entries[0]
        assert e["s_macro"] == 61.0
        assert e["s_tec"] == 44.0
        assert e["s_fund"] == 72.0
        assert e["ret_21d"] == 5.5
        assert e["precio"] == 200.0
        assert e["ticker"] == "GGAL.BA"

    def test_skips_entries_with_invalid_price(self):
        obs = _replay_obs(n=1, precio=0.0)
        assert wo._replay_obs_to_entries("MERVAL", obs) == []

    def test_skips_entries_with_missing_ret_21d(self):
        obs = _replay_obs(n=1)
        del obs[0]["ret_21d"]
        assert wo._replay_obs_to_entries("MERVAL", obs) == []

    def test_skips_entries_with_all_zero_scores(self):
        obs = _replay_obs(n=1, s_macro=0.0, s_tec=0.0, s_fund=0.0)
        assert wo._replay_obs_to_entries("MERVAL", obs) == []

    def test_empty_replay_obs_returns_empty(self):
        assert wo._replay_obs_to_entries("MERVAL", []) == []


class TestOptimizeMarketAugmentsWithReplay:

    def test_sensitivity_mode_uses_replay_when_real_entries_scarce(self, monkeypatch):
        """Caso real de hoy: pocos entries reales, pero historical_replay sí
        tiene datos -- debe usarlos en vez de devolver W_CURRENT por falta
        de muestra."""
        monkeypatch.setattr(wo, "_collect_market_entries", lambda *a, **k: [])
        replay_obs = _replay_obs(market="MERVAL", n=60, ret_21d=10.0, s_macro=70, s_tec=70, s_fund=70)

        result = wo._optimize_market(
            "MERVAL", history={}, sorted_dates=[], price_index={},
            mode="sensitivity", replay_obs=replay_obs,
        )

        assert result.get("n_real_entries") == 0
        assert result.get("n_replay_entries") == 60
        assert "ev_21d" in result  # llegó al grid search, no cayó a W_CURRENT

    def test_walk_forward_mode_ignores_replay(self, monkeypatch):
        """Con suficiente historia real (walk_forward), el replay NO debe
        usarse -- no tiene orden cronológico confiable entre tickers y
        rompería la separación train/eval."""
        real_entries = [
            {"ticker": "GGAL.BA", "signal_date": f"2026-06-{d:02d}", "precio": 150.0,
             "s_macro": 60.0, "s_tec": 55.0, "s_fund": 70.0, "ret_21d": 6.0}
            for d in range(1, 26)
        ]
        monkeypatch.setattr(wo, "_collect_market_entries", lambda *a, **k: real_entries)
        replay_obs = _replay_obs(market="MERVAL", n=500)  # mucho replay disponible

        result = wo._optimize_market(
            "MERVAL", history={}, sorted_dates=[], price_index={},
            mode="walk_forward", replay_obs=replay_obs,
        )

        assert result.get("n_real_entries") == 25
        assert result.get("n_replay_entries") == 0  # ignorado por estar en walk_forward

    def test_no_replay_obs_behaves_like_before_the_fix(self, monkeypatch):
        """Si replay_obs es None o [] (ej. historical_replay.json todavía no
        existe en este Railway), el comportamiento debe ser idéntico al de
        antes de este fix."""
        monkeypatch.setattr(wo, "_collect_market_entries", lambda *a, **k: [])

        result_none = wo._optimize_market(
            "MERVAL", history={}, sorted_dates=[], price_index={}, mode="sensitivity", replay_obs=None,
        )
        result_empty = wo._optimize_market(
            "MERVAL", history={}, sorted_dates=[], price_index={}, mode="sensitivity", replay_obs=[],
        )
        assert result_none == wo.W_CURRENT["MERVAL"]
        assert result_empty == wo.W_CURRENT["MERVAL"]

    def test_replay_filtered_by_market_even_when_mixed(self, monkeypatch):
        monkeypatch.setattr(wo, "_collect_market_entries", lambda *a, **k: [])
        mixed = _replay_obs(market="MERVAL", n=30) + _replay_obs(market="BOVESPA", n=40)

        result = wo._optimize_market(
            "MERVAL", history={}, sorted_dates=[], price_index={},
            mode="sensitivity", replay_obs=mixed,
        )
        assert result.get("n_replay_entries") == 30


class TestRunWeightOptimizationGate:

    def test_proceeds_with_few_real_days_if_replay_available(self, monkeypatch, tmp_path):
        """El gate original ('< 6 días reales -> saltar') se relaja cuando
        hay datos de historical_replay -- es exactamente el escenario de
        hoy (2-3 días reales de signals_history.json)."""
        monkeypatch.setattr(wo, "HISTORY_PATH", str(tmp_path / "signals_history.json"))
        monkeypatch.setattr(wo, "WEIGHTS_PATH", str(tmp_path / "optimized_weights.json"))
        # No existe signals_history.json -> history={}, n_days=0

        import src.historical_replay as hr
        replay_obs = _replay_obs(market="MERVAL", n=60, s_macro=70, s_tec=70, s_fund=70, ret_21d=9.0) \
            + _replay_obs(market="BOVESPA", n=60, s_macro=70, s_tec=70, s_fund=70, ret_21d=9.0) \
            + _replay_obs(market="SP500", n=60, s_macro=70, s_tec=70, s_fund=70, ret_21d=9.0)
        monkeypatch.setattr(hr, "load_replay_observations", lambda: replay_obs)

        import src.github_persistence as gp
        monkeypatch.setattr(gp, "save_json", lambda *a, **k: True)

        result = wo.run_weight_optimization(price_data={}, ticker_cols={})

        assert result != {}
        assert result.get("days_history") == 0
        assert result.get("mode") == "sensitivity"
        for market in wo.MARKETS:
            assert result[market].get("n_replay_entries", 0) > 0

    def test_skips_entirely_with_no_real_data_and_no_replay(self, monkeypatch, tmp_path):
        monkeypatch.setattr(wo, "HISTORY_PATH", str(tmp_path / "signals_history.json"))
        import src.historical_replay as hr
        monkeypatch.setattr(hr, "load_replay_observations", lambda: [])

        result = wo.run_weight_optimization(price_data={}, ticker_cols={})
        assert result == {}
