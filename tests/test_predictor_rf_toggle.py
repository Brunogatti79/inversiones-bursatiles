"""
tests/test_predictor_rf_toggle.py

Tests para el toggle ENABLE_RF_PREDICTOR en src/predictor.py.

Contexto (incidente 23/06/2026): Random Forest fue desactivado temporalmente
por ser el modelo más pesado en CPU/memoria de los 4 del ensemble,
sospechoso de tumbar el contenedor de Railway a mitad de corrida. La causa
real del incidente terminó siendo `watchPatterns` (ver test_railway_config.py)
y no Random Forest, pero el toggle quedó como mecanismo de control de riesgo
explícito y debe seguir comportándose exactamente como se espera en ambas
posiciones antes de reactivarlo en producción.

Además se cubre el fallback anti-división-por-cero del `ensemble()` interno
de predict_ticker, que es la pieza de aritmética más frágil de este módulo:
si algún día todos los modelos devuelven confianza 0 (ej. por datos
insuficientes en varios a la vez), no debe romper el pipeline completo de
predicciones.
"""
import numpy as np
import pandas as pd
import pytest

import src.predictor as predictor


@pytest.fixture(autouse=True)
def _isolate_predictor_state(tmp_path, monkeypatch):
    """Evita pisar el cache real (data/pred_cache.json) y resetea el cache
    en memoria entre tests. predict_ticker cachea por ticker sin tener en
    cuenta el estado de ENABLE_RF_PREDICTOR, así que dos tests que usen el
    mismo nombre de ticker con distinto toggle se contaminarían entre sí
    sin este reset -- por eso además cada test usa un ticker propio."""
    monkeypatch.setattr(predictor, "CACHE_PATH", str(tmp_path / "pred_cache.json"))
    predictor._CACHE = {}
    predictor._CACHE_DATE = ""
    yield
    predictor._CACHE = {}
    predictor._CACHE_DATE = ""


def _series(n=90, seed=0):
    rng = np.random.RandomState(seed)
    return pd.Series(100 + rng.randn(n).cumsum())


class _CallRecorder:
    """Reemplaza una función del módulo y registra con qué horizonte la
    llamaron, devolviendo un valor fijo configurable."""

    def __init__(self, value=99.0, confidence=0.99):
        self.calls = []
        self.value = value
        self.confidence = confidence

    def __call__(self, serie, horizon, context=None):
        self.calls.append(horizon)
        return self.value, self.confidence


class TestRFToggleDefault:

    def test_disabled_when_env_unset(self, monkeypatch):
        """Default seguro: sin la env var presente, RF no debe ejecutarse."""
        monkeypatch.delenv("ENABLE_RF_PREDICTOR", raising=False)
        rf_spy = _CallRecorder()
        monkeypatch.setattr(predictor, "_random_forest", rf_spy)

        result = predictor.predict_ticker("TEST_RF_UNSET", _series(seed=1))

        assert rf_spy.calls == []
        assert result["pred_21d"] is not None

    def test_disabled_when_env_explicitly_false(self, monkeypatch):
        monkeypatch.setenv("ENABLE_RF_PREDICTOR", "false")
        rf_spy = _CallRecorder()
        monkeypatch.setattr(predictor, "_random_forest", rf_spy)

        predictor.predict_ticker("TEST_RF_FALSE", _series(seed=2))

        assert rf_spy.calls == []

    def test_disabled_rf_does_not_distort_ensemble_output(self, monkeypatch):
        """RF desactivado debe aportar confianza 0 y quedar excluido del
        promedio ponderado -- el resultado no debe arrastrarse hacia el
        valor que RF hubiera devuelto si corriera."""
        monkeypatch.delenv("ENABLE_RF_PREDICTOR", raising=False)
        monkeypatch.setattr(predictor, "_holt_winters", lambda serie, horizon: (8.0, 0.6))
        monkeypatch.setattr(predictor, "_gradient_boosting", lambda serie, horizon, context=None: (8.0, 0.6))
        monkeypatch.setattr(predictor, "_linear_baseline", lambda serie, horizon, context=None: (8.0, 0.6))
        monkeypatch.setattr(predictor, "_random_forest", _CallRecorder(value=99.0, confidence=0.99))

        result = predictor.predict_ticker("TEST_RF_NO_DISTORT", _series(seed=3))

        assert result["pred_21d"] == pytest.approx(8.0, abs=0.15)


class TestRFToggleEnabled:

    def test_enabled_when_env_true_calls_all_three_horizons(self, monkeypatch):
        monkeypatch.setenv("ENABLE_RF_PREDICTOR", "true")
        rf_spy = _CallRecorder(value=3.0, confidence=0.5)
        monkeypatch.setattr(predictor, "_random_forest", rf_spy)

        predictor.predict_ticker("TEST_RF_TRUE", _series(seed=4))

        # Se invoca para el bloque principal (5d, 21d) y el bloque separado
        # de 10d -- ver predictor.py: pred_10d se calcula aparte, no se
        # interpola entre 5d y 21d.
        assert sorted(rf_spy.calls) == [5, 10, 21]

    def test_enabled_value_is_case_insensitive(self, monkeypatch):
        monkeypatch.setenv("ENABLE_RF_PREDICTOR", "TRUE")
        rf_spy = _CallRecorder(value=3.0, confidence=0.5)
        monkeypatch.setattr(predictor, "_random_forest", rf_spy)

        predictor.predict_ticker("TEST_RF_CASE_INSENSITIVE", _series(seed=5))

        assert len(rf_spy.calls) == 3

    def test_other_truthy_strings_stay_disabled(self, monkeypatch):
        """El check del código es `.lower() == 'true'` exacto -- valores
        como '1' o 'yes' NO activan RF. Vale la pena dejarlo explícito: si
        alguien configura ENABLE_RF_PREDICTOR=1 en Railway pensando que
        activa RF, en realidad sigue desactivado sin ningún aviso."""
        monkeypatch.setenv("ENABLE_RF_PREDICTOR", "1")
        rf_spy = _CallRecorder(value=3.0, confidence=0.5)
        monkeypatch.setattr(predictor, "_random_forest", rf_spy)

        predictor.predict_ticker("TEST_RF_TRUTHY_STRING", _series(seed=6))

        assert rf_spy.calls == []


class TestEnsembleRobustness:
    """El cierre `ensemble()` dentro de predict_ticker tiene un fallback
    explícito para evitar división por cero cuando todas las confianzas son
    0 (ej. todos los modelos fallan a la vez, o RF desactivado coincide con
    que el resto también devuelve 0 por datos insuficientes). Es aritmética
    frágil: si regresiona, se cae el pipeline de predicciones completo, no
    solo un ticker."""

    def test_all_zero_confidence_does_not_raise_and_returns_finite_value(self, monkeypatch):
        monkeypatch.delenv("ENABLE_RF_PREDICTOR", raising=False)
        monkeypatch.setattr(predictor, "_holt_winters", lambda serie, horizon: (0.0, 0.0))
        monkeypatch.setattr(predictor, "_gradient_boosting", lambda serie, horizon, context=None: (0.0, 0.0))
        monkeypatch.setattr(predictor, "_linear_baseline", lambda serie, horizon, context=None: (0.0, 0.0))

        result = predictor.predict_ticker("TEST_ALL_ZERO_CONFIDENCE", _series(seed=7))

        assert result["pred_21d"] is not None
        assert np.isfinite(result["pred_21d"])

    def test_short_series_returns_empty_result_without_raising(self):
        """Menos de 30 observaciones -> debe devolver el dict default, no
        lanzar excepción (tickers nuevos o con muchos feriados acumulados
        pueden tener historiales cortos)."""
        short_serie = pd.Series([100.0, 101.0, 99.0])

        result = predictor.predict_ticker("TEST_SHORT_SERIES", short_serie)

        assert result["pred_21d"] is None
        assert result["pred_signal"] == ""
        assert result["pred_method"] == "ensemble"


class TestIncludeSubmodels:
    """include_submodels=True (default False) expone el desglose por
    sub-modelo -- agregado para predictor_validation.py (Prioridad 3
    ampliada: '¿hay un modelo arrastrando al resto?'). Default False no
    debe cambiar nada del comportamiento de producción."""

    def test_default_false_does_not_add_submodels_key(self):
        result = predictor.predict_ticker("TEST_NO_SUBMODELS", _series(seed=10))
        assert "submodels" not in result

    def test_true_adds_submodels_with_all_four_models(self):
        result = predictor.predict_ticker("TEST_WITH_SUBMODELS", _series(seed=11), include_submodels=True)
        assert "submodels" in result
        for model in ("holt_winters", "gradient_boosting", "random_forest", "linear_baseline"):
            assert model in result["submodels"]
            assert set(result["submodels"][model].keys()) == {5, 10, 21}

    def test_random_forest_submodel_reports_zero_when_disabled(self, monkeypatch):
        monkeypatch.delenv("ENABLE_RF_PREDICTOR", raising=False)
        result = predictor.predict_ticker("TEST_SUBMODELS_RF_OFF", _series(seed=12), include_submodels=True)
        assert result["submodels"]["random_forest"] == {5: 0.0, 10: 0.0, 21: 0.0}
        assert result["submodels"]["rf_enabled"] is False

    def test_random_forest_submodel_populated_when_enabled(self, monkeypatch):
        monkeypatch.setenv("ENABLE_RF_PREDICTOR", "true")
        result = predictor.predict_ticker("TEST_SUBMODELS_RF_ON", _series(seed=13), include_submodels=True)
        assert result["submodels"]["rf_enabled"] is True
        # No necesariamente != 0 siempre, pero el flag de que corrió debe
        # quedar explícito y separado del valor en sí.

    def test_submodel_values_feed_the_same_ensemble_output(self):
        """El ensemble final sigue siendo el promedio ponderado de los
        submodels -- include_submodels solo expone lo que ya se calculaba,
        no agrega un cálculo paralelo distinto."""
        result = predictor.predict_ticker("TEST_SUBMODELS_CONSISTENCY", _series(seed=14), include_submodels=True)
        sub = result["submodels"]
        # Con todos los pesos iguales (RF en 0 por desactivado), el promedio
        # de HW/GBR/Linear con peso similar debe quedar en el mismo rango
        # general que el pred_21d final -- no una comprobación exacta (los
        # pesos son por confianza, no iguales), solo que no estén en
        # universos completamente distintos.
        non_rf_avg = (sub["holt_winters"][21] + sub["gradient_boosting"][21] + sub["linear_baseline"][21]) / 3
        assert abs(result["pred_21d"] - non_rf_avg) < 50  # cota laxa, solo control de cordura
