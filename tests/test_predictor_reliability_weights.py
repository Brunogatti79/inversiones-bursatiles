"""
tests/test_predictor_reliability_weights.py

Tests para el reponderado del ensemble del predictor por precisión REAL
validada (mejora 27/07/2026, roadmap externo P3 -- ver model_version.py v4.10).

Cubre:
  - _load_reliability_weights() calcula bien la normalización contra el mejor
  - linear_baseline SIEMPRE queda en 0 (decisión explícita de Bruno, no
    depende de qué tan negativa sea su correlación)
  - Fallback a 1.0 en los 3 si el archivo no existe, está corrupto, o le
    faltan campos -- nunca debe romper una corrida
  - predict_ticker() aplica los pesos al ensemble (efecto end-to-end) sin
    tocar los valores crudos en submodels (que alimentan a
    predictor_validation.py -- no se debe contaminar la propia validación)
"""

import sys
import os
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

import json
import numpy as np
import pandas as pd
import pytest

import src.predictor as predictor


VALIDATION_PATH = "data/predictor_validation.json"


@pytest.fixture(autouse=True)
def _reset_reliability_cache_y_archivo(tmp_path, monkeypatch):
    """Cada test arranca sin cache y sin archivo real -- evita que un test
    contamine al siguiente, y evita tocar el data/predictor_validation.json
    ni el data/pred_cache.json reales del repo durante la suite (predict_ticker
    cachea por ticker+día -- sin aislar esto, una corrida anterior con el
    mismo ticker "TEST" ensucia el resultado)."""
    predictor._RELIABILITY_CACHE = None
    predictor._CACHE = {}
    predictor._CACHE_DATE = ""
    monkeypatch.setattr(predictor, "VALIDATION_PATH", str(tmp_path / "predictor_validation.json"))
    monkeypatch.setattr(predictor, "CACHE_PATH", str(tmp_path / "pred_cache.json"))
    yield
    predictor._RELIABILITY_CACHE = None
    predictor._CACHE = {}
    predictor._CACHE_DATE = ""


def _write_validation(path, holt_corr, gbr_corr):
    with open(path, "w") as f:
        json.dump({
            "generated": "2026-07-27T00:00:00",
            "global": {
                "holt_winters":      {"directional_accuracy": 0.52,  "correlation": holt_corr},
                "gradient_boosting": {"directional_accuracy": 0.476, "correlation": gbr_corr},
                "linear_baseline":   {"directional_accuracy": 0.501, "correlation": -0.096},
            },
        }, f)


def test_reliability_weights_normaliza_contra_el_mejor():
    _write_validation(predictor.VALIDATION_PATH, holt_corr=0.156, gbr_corr=0.012)
    rel = predictor._load_reliability_weights()

    assert rel["holt_winters"] == 1.0  # el mejor no pierde confianza
    assert rel["gradient_boosting"] == round(0.012 / 0.156, 3)
    assert rel["linear_baseline"] == 0.0


def test_linear_baseline_siempre_cero_aunque_su_correlacion_no_sea_la_peor():
    """Decisión explícita de Bruno: linear_baseline se excluye del ensemble
    SIEMPRE, no es un cálculo dinámico -- aunque hipotéticamente tuviera
    mejor correlación que hoy, debe seguir en 0."""
    _write_validation(predictor.VALIDATION_PATH, holt_corr=0.05, gbr_corr=0.03)
    rel = predictor._load_reliability_weights()
    assert rel["linear_baseline"] == 0.0


def test_fallback_a_1_si_no_existe_el_archivo():
    # no se escribe el archivo -- VALIDATION_PATH apunta a un tmp_path vacío
    rel = predictor._load_reliability_weights()
    assert rel == {"holt_winters": 1.0, "gradient_boosting": 1.0, "linear_baseline": 1.0}


def test_fallback_a_1_si_el_archivo_esta_corrupto(tmp_path):
    with open(predictor.VALIDATION_PATH, "w") as f:
        f.write("{ esto no es json valido")
    rel = predictor._load_reliability_weights()
    assert rel == {"holt_winters": 1.0, "gradient_boosting": 1.0, "linear_baseline": 1.0}


def test_fallback_a_1_si_faltan_campos_esperados():
    with open(predictor.VALIDATION_PATH, "w") as f:
        json.dump({"generated": "2026-07-27", "global": {}}, f)
    rel = predictor._load_reliability_weights()
    assert rel == {"holt_winters": 1.0, "gradient_boosting": 1.0, "linear_baseline": 1.0}


def test_get_reliability_weights_cachea_en_memoria():
    _write_validation(predictor.VALIDATION_PATH, holt_corr=0.156, gbr_corr=0.012)
    rel1 = predictor._get_reliability_weights()
    # Cambiar el archivo después de la primera lectura no debería afectar
    # dentro del mismo proceso -- el cache es a nivel de módulo/corrida.
    _write_validation(predictor.VALIDATION_PATH, holt_corr=0.99, gbr_corr=0.99)
    rel2 = predictor._get_reliability_weights()
    assert rel1 == rel2


def test_predict_ticker_incluye_reliability_weights_en_el_resultado():
    _write_validation(predictor.VALIDATION_PATH, holt_corr=0.156, gbr_corr=0.012)
    np.random.seed(7)
    n = 200
    precios = 100 * np.cumprod(1 + np.random.normal(0.0003, 0.02, n))
    serie = pd.Series(precios, index=pd.date_range("2025-01-01", periods=n, freq="B"))

    os.environ["ENABLE_RF_PREDICTOR"] = "false"
    res = predictor.predict_ticker("TEST", serie, include_submodels=True)

    assert res["reliability_weights"]["holt_winters"] == 1.0
    assert res["reliability_weights"]["linear_baseline"] == 0.0
    # Los valores crudos de submodels NO deben verse afectados por el peso --
    # alimentan a predictor_validation.py, que necesita el valor SIN ponderar
    # para poder medir la precisión real de cada modelo de forma independiente.
    assert "linear_baseline" in res["submodels"]
    lb = res["submodels"]["linear_baseline"]
    valor_21d = lb.get(21, lb.get("21"))
    assert isinstance(valor_21d, float)
