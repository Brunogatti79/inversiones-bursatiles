"""
tests/test_historical_replay_persistence.py

Tests para el fix de persistencia de src/historical_replay.py (24/06/2026).

Bug original: el resultado se escribía SOLO en el filesystem local de
Railway (efímero, se resetea en cada redeploy) y nunca se pusheaba a
GitHub. El chequeo de "correr solo 1x/semana" comparaba contra el mtime
del archivo local -- inexistente tras un redeploy -- por lo que en la
práctica se regeneraba desde cero en cada corrida sin que ese límite
tuviera ningún efecto real. Mismo patrón que ya rompió signals_history.json
hasta junio 2026 (ver arquitectura v4/v7 §3), en un módulo distinto que
nadie había revisado.

Estos tests no validan el cálculo del replay en sí (eso no cambió) --
se enfocan exclusivamente en el ciclo de persistencia: que el push
realmente se llame, que el chequeo de staleness lea el timestamp del
CONTENIDO y no del filesystem, y que la carga sea resiliente a JSON
corrupto.
"""
from datetime import datetime, timedelta

import pandas as pd
import pytest

import src.historical_replay as hr


@pytest.fixture(autouse=True)
def _isolate_replay_path(tmp_path, monkeypatch):
    monkeypatch.setattr(hr, "REPLAY_PATH", str(tmp_path / "historical_replay.json"))


def _long_enough_price_series(n=200, seed=0):
    """Serie sintética suficientemente larga para producir al menos una
    observación: MIN_TRAIN(60) + max(HORIZONS)(21) + margen."""
    import numpy as np
    rng = np.random.RandomState(seed)
    dates = pd.date_range("2025-01-01", periods=n, freq="B")
    return pd.Series(100 + rng.randn(n).cumsum(), index=dates)


def _price_data_with_one_ticker(ticker="GGAL.BA", market_key="merval"):
    serie = _long_enough_price_series()
    df = pd.DataFrame({ticker: serie})
    return {market_key: df}, {ticker: ticker}


class TestStalenessChecksContentNotMtime:

    def test_skips_regeneration_when_content_is_recent(self, monkeypatch):
        """Si el JSON existente tiene un 'generated' de hace menos de 6
        días, no debe recalcular nada -- ni siquiera debería necesitar
        price_data válido, porque retorna antes de tocarlo."""
        recent = (datetime.now() - timedelta(days=2)).isoformat()
        monkeypatch.setattr(hr, "_load_replay", lambda: {"generated": recent, "observations": ["x"]})

        push_calls = []
        monkeypatch.setattr(hr, "_replay_ticker", lambda *a, **k: pytest.fail("no debería recalcular"))

        result = hr.run_historical_replay(
            price_data={}, ticker_cols={}, macro_scores={}, fund_scores={},
        )
        assert result == {"generated": recent, "observations": ["x"]}

    def test_regenerates_when_content_is_stale(self, monkeypatch):
        """Con 'generated' de hace más de 6 días, debe recalcular --
        incluso si el archivo local tiene mtime reciente (ej. porque se
        pulleó hace 1 minuto al arrancar Railway). Este es el caso que el
        bug original rompía: antes se comparaba el mtime del archivo, que
        tras un pull siempre es 'reciente' sin importar la edad real del
        contenido."""
        old = (datetime.now() - timedelta(days=10)).isoformat()
        monkeypatch.setattr(hr, "_load_replay", lambda: {"generated": old, "observations": []})

        pushed = []
        import src.github_persistence as gp
        monkeypatch.setattr(gp, "push_file", lambda *a, **k: pushed.append(1) or True)

        price_data, ticker_cols = _price_data_with_one_ticker()
        result = hr.run_historical_replay(
            price_data=price_data, ticker_cols=ticker_cols,
            macro_scores={"MERVAL": 60.0}, fund_scores={"GGAL.BA": 70.0},
        )
        assert result != {}
        assert pushed, "debería haber recalculado y pusheado el resultado nuevo"

    def test_malformed_generated_field_does_not_crash_and_regenerates(self, monkeypatch):
        """Un 'generated' corrupto o con formato viejo no debe tirar abajo
        el pipeline -- debe tratarse como 'sin dato confiable' y regenerar."""
        monkeypatch.setattr(hr, "_load_replay", lambda: {"generated": "no-es-una-fecha", "observations": []})
        import src.github_persistence as gp
        monkeypatch.setattr(gp, "push_file", lambda *a, **k: True)

        price_data, ticker_cols = _price_data_with_one_ticker()
        result = hr.run_historical_replay(  # no debe lanzar excepción
            price_data=price_data, ticker_cols=ticker_cols,
            macro_scores={"MERVAL": 60.0}, fund_scores={"GGAL.BA": 70.0},
        )
        assert isinstance(result, dict)

    def test_no_prior_data_regenerates(self, monkeypatch):
        """Primera corrida (nunca se generó antes, _load_replay devuelve
        {}): debe regenerar sin necesitar ningún campo 'generated'."""
        monkeypatch.setattr(hr, "_load_replay", lambda: {})
        import src.github_persistence as gp
        pushed = []
        monkeypatch.setattr(gp, "push_file", lambda *a, **k: pushed.append(1) or True)

        price_data, ticker_cols = _price_data_with_one_ticker()
        result = hr.run_historical_replay(
            price_data=price_data, ticker_cols=ticker_cols,
            macro_scores={"MERVAL": 60.0}, fund_scores={"GGAL.BA": 70.0},
        )
        assert result.get("total_obs", 0) > 0
        assert pushed


class TestPushOnRegeneration:

    def test_push_file_called_with_replay_path_after_regeneration(self, monkeypatch, tmp_path):
        monkeypatch.setattr(hr, "_load_replay", lambda: {})
        import src.github_persistence as gp
        captured = {}

        def fake_push(path, message=None):
            captured["path"] = path
            captured["message"] = message
            return True

        monkeypatch.setattr(gp, "push_file", fake_push)

        price_data, ticker_cols = _price_data_with_one_ticker()
        hr.run_historical_replay(
            price_data=price_data, ticker_cols=ticker_cols,
            macro_scores={"MERVAL": 60.0}, fund_scores={"GGAL.BA": 70.0},
        )

        assert captured.get("path") == hr.REPLAY_PATH
        assert "historical_replay" in (captured.get("message") or "")

    def test_no_observations_does_not_push(self, monkeypatch):
        """Si no se generó ninguna observación (ej. todas las series son
        demasiado cortas), no tiene sentido pushear un archivo vacío --
        debe devolver {} sin llamar a push_file."""
        monkeypatch.setattr(hr, "_load_replay", lambda: {})
        import src.github_persistence as gp
        pushed = []
        monkeypatch.setattr(gp, "push_file", lambda *a, **k: pushed.append(1) or True)

        short_serie = pd.Series([100.0, 101.0, 99.0])
        price_data = {"merval": pd.DataFrame({"GGAL.BA": short_serie})}

        result = hr.run_historical_replay(
            price_data=price_data, ticker_cols={"GGAL.BA": "GGAL.BA"},
            macro_scores={"MERVAL": 60.0}, fund_scores={},
        )
        assert result == {}
        assert pushed == []


class TestLoadReplayResilience:

    def test_load_replay_missing_file_returns_empty_dict(self):
        assert hr._load_replay() == {}

    def test_load_replay_corrupt_json_returns_empty_dict_without_raising(self):
        with open(hr.REPLAY_PATH, "w") as f:
            f.write("{ esto no es json valido ::: ")
        assert hr._load_replay() == {}

    def test_load_replay_valid_file_returns_content(self):
        import json
        with open(hr.REPLAY_PATH, "w") as f:
            json.dump({"generated": "2026-06-24T00:00:00", "observations": [{"a": 1}]}, f)
        result = hr._load_replay()
        assert result["observations"] == [{"a": 1}]


class TestPublicConsumerHelpers:
    """load_replay_observations() y get_replay_summary() ya existían para
    que weight_optimizer.py los consuma -- a la fecha de este fix, nada los
    llama todavía (eso queda para una sesión aparte), pero deben seguir
    funcionando con el nuevo _load_replay() resiliente."""

    def test_load_replay_observations_empty_when_no_data(self):
        assert hr.load_replay_observations() == []

    def test_get_replay_summary_unavailable_when_no_data(self):
        summary = hr.get_replay_summary()
        assert summary == {"available": False}

    def test_get_replay_summary_reports_real_data(self):
        import json
        with open(hr.REPLAY_PATH, "w") as f:
            json.dump({
                "generated": "2026-06-24T10:00:00", "total_obs": 3500,
                "total_tickers": 67, "observations": [],
            }, f)
        summary = hr.get_replay_summary()
        assert summary["available"] is True
        assert summary["total_obs"] == 3500
        assert summary["total_tickers"] == 67
