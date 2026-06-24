"""
tests/test_predictor_validation.py

Tests para src/predictor_validation.py (Prioridad 3, roadmap externo:
predictor vs baselines simples).

Cubre, en orden de importancia:
  1. Aislamiento de cache: la validación NO debe tocar ni inflar
     data/pred_cache.json de producción (es el riesgo más concreto de este
     módulo -- predictor._CACHE está keyeado solo por ticker, no por fecha).
  2. Persistencia: mismo patrón que historical_replay.py -- staleness por
     contenido, no por mtime; push real.
  3. Correctitud de los 3 baselines (zero / momentum / promedio histórico).
  4. Agregación de métricas (directional accuracy / MAE / correlación).
"""
from datetime import datetime, timedelta

import numpy as np
import pandas as pd
import pytest

import src.predictor_validation as pv


@pytest.fixture(autouse=True)
def _isolate_validation_path(tmp_path, monkeypatch):
    monkeypatch.setattr(pv, "VALIDATION_PATH", str(tmp_path / "predictor_validation.json"))


def _trending_series(n=300, drift=0.3, noise=1.0, seed=0):
    rng = np.random.RandomState(seed)
    return pd.Series(100 + np.cumsum(drift + rng.randn(n) * noise))


# ── Aislamiento de cache (lo más importante de este módulo) ────────────

class TestCacheIsolation:

    def test_does_not_modify_production_cache_state(self, monkeypatch):
        import src.predictor as predictor
        predictor._CACHE = {"AAPL": {"pred_21d": 99.0}}
        predictor._CACHE_DATE = "2026-06-01"
        original_path = predictor.CACHE_PATH

        with pv._isolated_predictor_cache():
            assert predictor._CACHE == {}
            assert predictor.CACHE_PATH != original_path
            predictor._CACHE["__VALID__GGAL.BA__0"] = {"fake": True}

        # Tras salir del context manager, el estado de producción vuelve
        # exactamente como estaba -- la entrada sintética no sobrevive.
        assert predictor._CACHE == {"AAPL": {"pred_21d": 99.0}}
        assert predictor._CACHE_DATE == "2026-06-01"
        assert predictor.CACHE_PATH == original_path

    def test_restores_state_even_if_exception_raised_inside(self, monkeypatch):
        import src.predictor as predictor
        predictor._CACHE = {"REAL_TICKER": {"x": 1}}

        with pytest.raises(RuntimeError):
            with pv._isolated_predictor_cache():
                predictor._CACHE["junk"] = {}
                raise RuntimeError("boom")

        assert predictor._CACHE == {"REAL_TICKER": {"x": 1}}

    def test_validate_ticker_uses_synthetic_cache_keys_not_real_ticker(self, monkeypatch):
        """Cada snapshot debe llamar a predict_ticker con una cache_key
        sintética (__VALID__...) -- nunca con el nombre real del ticker,
        que rompería el cache de producción si coincidiera."""
        calls = []

        class FakePredictor:
            _CACHE = {}
            _CACHE_DATE = ""
            CACHE_PATH = "/tmp/x"

            @staticmethod
            def predict_ticker(ticker_key, serie, context=None, include_submodels=False):
                calls.append(ticker_key)
                return {"pred_5d": 1.0, "pred_10d": 2.0, "pred_21d": 3.0}

        monkeypatch.setattr(pv, "_predict_ticker_module", lambda: FakePredictor)

        serie = _trending_series(200)
        pv._validate_ticker("GGAL.BA", "MERVAL", serie)

        assert len(calls) > 0
        assert all(c.startswith("__VALID__GGAL.BA__") for c in calls)
        assert "GGAL.BA" not in calls  # nunca la cache_key real

    def test_repeated_isolation_same_day_does_not_leak_between_runs(self):
        """Regresión de un bug real encontrado durante el desarrollo: con
        un CACHE_PATH temporal FIJO, dos entradas al context manager el
        mismo día calendario (ej. dos llamadas a run_predictor_validation
        dentro del mismo proceso largo) podían leer el resultado cacheado
        de la entrada anterior -- predictor._load_cache() solo chequea que
        la fecha adentro del archivo sea 'hoy', no de qué corrida vino.
        Cada entrada al context manager debe usar un archivo temporal
        distinto y no dejarlo para la próxima."""
        import src.predictor as predictor
        serie = _trending_series(200)

        with pv._isolated_predictor_cache() as ctx1:
            path1 = ctx1._tmp_path
            r1 = predictor.predict_ticker("__VALID__SAME_KEY__0", serie, include_submodels=True)
        assert not __import__("os").path.exists(path1)  # se limpia solo al salir

        with pv._isolated_predictor_cache() as ctx2:
            path2 = ctx2._tmp_path
            assert path2 != path1  # nunca el mismo archivo temporal
            r2 = predictor.predict_ticker("__VALID__SAME_KEY__0", serie, include_submodels=True)

        # Ambas corridas deben haber calculado de cero (no servido un cache
        # ajeno) -- con la misma serie y misma cache_key, los resultados
        # deberían coincidir, pero por haber sido recalculados, no por
        # haber leído el archivo de la corrida anterior.
        assert r1.get("submodels") is not None
        assert r2.get("submodels") is not None


# ── Baselines ────────────────────────────────────────────────────────────

class TestBaselines:

    def test_zero_baseline_is_always_zero(self):
        result = pv._zero_baseline()
        assert all(v == 0.0 for v in result.values())
        assert set(result.keys()) == set(pv.HORIZONS)

    def test_momentum_baseline_matches_trailing_return(self):
        serie = pd.Series([100.0] * 80 + [110.0])  # +10% en el último salto
        result = pv._momentum_baseline(serie)
        assert result[5] == pytest.approx(10.0, abs=0.01)

    def test_momentum_baseline_none_when_insufficient_history(self):
        serie = pd.Series([100.0, 101.0, 102.0])
        result = pv._momentum_baseline(serie)
        assert result[21] is None

    def test_historical_avg_baseline_matches_manual_mean(self):
        serie = _trending_series(150, drift=0.5, noise=0.1, seed=1)
        result = pv._historical_avg_baseline(serie)
        manual_5d = serie.pct_change(5, fill_method=None).dropna().mean() * 100
        assert result[5] == pytest.approx(round(manual_5d, 2), abs=0.01)

    def test_historical_avg_baseline_none_when_no_data(self):
        serie = pd.Series([100.0])
        result = pv._historical_avg_baseline(serie)
        assert result[21] is None


# ── Agregación de métricas ──────────────────────────────────────────────

class TestAggregation:

    def _record(self, pred_5d, actual_5d, mercado="MERVAL"):
        return {
            "ticker": "X", "mercado": mercado, "snapshot": 0,
            "predictor":  {5: pred_5d, 10: pred_5d, 21: pred_5d},
            "baselines":  {"zero": {5: 0.0, 10: 0.0, 21: 0.0},
                            "momentum": {5: pred_5d, 10: pred_5d, 21: pred_5d},
                            "historical_avg": {5: 0.0, 10: 0.0, 21: 0.0}},
            "actual": {5: actual_5d, 10: actual_5d, 21: actual_5d},
        }

    def test_perfect_directional_accuracy(self):
        records = [self._record(p, p) for p in [3.0, -2.0, 5.0, -1.0, 4.0, -3.0]]
        agg = pv._aggregate(records, only_horizon=5)
        assert agg["predictor"]["directional_accuracy"] == 1.0
        assert agg["predictor"]["mae"] == 0.0

    def test_zero_baseline_never_matches_nonzero_actual_direction(self):
        records = [self._record(1.0, 3.0), self._record(1.0, -2.0), self._record(1.0, 1.0),
                   self._record(1.0, -1.0), self._record(1.0, 2.0)]
        agg = pv._aggregate(records, only_horizon=5)
        # sign(0) nunca es igual a sign(no-cero) -> directional_accuracy debe ser 0
        assert agg["zero"]["directional_accuracy"] == 0.0

    def test_insufficient_samples_returns_none_metrics(self):
        records = [self._record(1.0, 2.0)] * 2  # solo 2, bajo el umbral de 5
        agg = pv._aggregate(records, only_horizon=5)
        assert agg["predictor"]["directional_accuracy"] is None
        assert agg["predictor"]["n"] == 2

    def test_correlation_none_when_predictions_constant(self):
        """Si todas las predicciones son idénticas (ej. el zero baseline),
        no se puede calcular correlación -- debe devolver None, no NaN ni
        explotar."""
        records = [self._record(1.0, v) for v in [3.0, -2.0, 5.0, -1.0, 4.0]]
        agg = pv._aggregate(records, only_horizon=5)
        assert agg["zero"]["correlation"] is None  # std(zero predictions) == 0

    def test_submodel_keys_present_in_aggregate_output(self):
        records = [self._record(1.0, v) for v in [3.0, -2.0, 5.0, -1.0, 4.0]]
        agg = pv._aggregate(records, only_horizon=5)
        for model in pv.SUBMODEL_METHODS:
            assert model in agg

    def test_submodel_metrics_computed_from_real_submodel_values(self):
        """Si holt_winters predice perfecto y linear_baseline predice lo
        opuesto, sus directional_accuracy deben diferenciarse claramente --
        confirma que cada sub-modelo se mide con sus propios valores, no
        con el del ensemble final."""
        records = []
        for actual in [3.0, -2.0, 5.0, -1.0, 4.0, -3.0]:
            r = self._record(pred_5d=actual, actual_5d=actual)  # ensemble "perfecto" como placeholder
            r["submodels"] = {
                "holt_winters":      {5: actual, 10: actual, 21: actual},        # siempre acierta
                "gradient_boosting": {5: 0.0, 10: 0.0, 21: 0.0},
                "random_forest":     {5: 0.0, 10: 0.0, 21: 0.0},                  # RF desactivado
                "linear_baseline":   {5: -actual, 10: -actual, 21: -actual},     # siempre opuesto
            }
            records.append(r)

        agg = pv._aggregate(records, only_horizon=5)
        assert agg["holt_winters"]["directional_accuracy"] == 1.0
        assert agg["linear_baseline"]["directional_accuracy"] == 0.0

    def test_missing_submodels_key_does_not_crash(self):
        """Records de antes de este fix (sin 'submodels') deben seguir
        agregándose sin romper -- los métodos de sub-modelo simplemente
        quedan con n=0 / métricas None."""
        records = [self._record(1.0, v) for v in [3.0, -2.0, 5.0, -1.0, 4.0]]
        for r in records:
            r.pop("submodels", None)
        agg = pv._aggregate(records, only_horizon=5)
        assert agg["holt_winters"]["n"] == 0
        assert agg["holt_winters"]["directional_accuracy"] is None


# ── Persistencia (mismo patrón que historical_replay.py) ────────────────

class TestPersistence:

    def test_skips_regeneration_when_recent(self, monkeypatch):
        recent = (datetime.now() - timedelta(days=2)).isoformat()
        monkeypatch.setattr(pv, "_load_validation", lambda: {"generated": recent, "n_snapshots": 10})
        monkeypatch.setattr(pv, "_validate_ticker", lambda *a, **k: pytest.fail("no debería recalcular"))

        result = pv.run_predictor_validation(price_data={}, ticker_cols={})
        assert result == {"generated": recent, "n_snapshots": 10}

    def test_pushes_after_regeneration(self, monkeypatch):
        monkeypatch.setattr(pv, "_load_validation", lambda: {})
        pushed = []
        import src.github_persistence as gp
        monkeypatch.setattr(gp, "push_file", lambda *a, **k: pushed.append(1) or True)

        serie = _trending_series(200, drift=0.3, seed=2)
        df = pd.DataFrame({"GGAL.BA": serie.values})
        result = pv.run_predictor_validation(
            price_data={"merval": df}, ticker_cols={"GGAL.BA": "GGAL.BA"},
        )
        assert result.get("n_snapshots", 0) > 0
        assert pushed

    def test_no_records_returns_empty_without_pushing(self, monkeypatch):
        monkeypatch.setattr(pv, "_load_validation", lambda: {})
        pushed = []
        import src.github_persistence as gp
        monkeypatch.setattr(gp, "push_file", lambda *a, **k: pushed.append(1) or True)

        short_serie = pd.Series([100.0, 101.0, 99.0])
        df = pd.DataFrame({"GGAL.BA": short_serie.values})
        result = pv.run_predictor_validation(
            price_data={"merval": df}, ticker_cols={"GGAL.BA": "GGAL.BA"},
        )
        assert result == {}
        assert pushed == []


class TestGetValidationSummary:

    def test_unavailable_when_no_data(self):
        assert pv.get_validation_summary() == {"available": False}

    def test_reports_real_data(self, monkeypatch):
        monkeypatch.setattr(pv, "_load_validation", lambda: {
            "generated": "2026-06-24T10:00:00", "n_snapshots": 400,
            "global": {"predictor": {"directional_accuracy": 0.58}},
        })
        summary = pv.get_validation_summary()
        assert summary["available"] is True
        assert summary["n_snapshots"] == 400
        assert summary["global"]["predictor"]["directional_accuracy"] == 0.58
