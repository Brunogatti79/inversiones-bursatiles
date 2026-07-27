"""
tests/test_v2_regime_adjustment.py

Tests para el ajuste de régimen de volatilidad DE MERCADO sobre Score V2
(mejora 27/07/2026, roadmap externo P4 -- ver model_version.py v4.10).

Cubre:
  - V2_REGIME_MULT tiene los 3 valores acordados con Bruno (HIGH/NORMAL/LOW)
  - analyze_market aplica el multiplicador correcto según el régimen recibido
  - Fallback a NORMAL (x1.00) si vol_regime es None o no trae el mercado
  - El campo se persiste en cada señal (vol_regime_mercado, v2_regime_mult)
"""

import sys
import os
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

import numpy as np
import pandas as pd
import pytest

from src.analyzer import analyze_market, V2_REGIME_MULT


def _mock_df(n=300, seed=1):
    """DataFrame en el MISMO formato que los CSV reales: una columna por
    ticker (nombre de empresa) + una columna de índice, solo Close -- sin
    High/Low, igual que merval/bovespa/sp500_cierres.csv."""
    np.random.seed(seed)
    dates = pd.date_range("2025-01-01", periods=n, freq="B")
    precio_ticker = 100 * np.cumprod(1 + np.random.normal(0.0005, 0.015, n))
    precio_indice = 1000 * np.cumprod(1 + np.random.normal(0.0003, 0.01, n))
    df = pd.DataFrame(
        {"Empresa Test": precio_ticker, "INDICE MERVAL": precio_indice},
        index=dates,
    )
    df.index.name = "Fecha"
    return df


TICKER_NAMES = {"TEST.BA": "Empresa Test"}


def test_v2_regime_mult_valores_acordados():
    """Magnitud decidida por Bruno 27/07/2026: suave, mismo espíritu que la
    penalización multi-timeframe ya existente."""
    assert V2_REGIME_MULT == {"HIGH": 0.90, "NORMAL": 1.00, "LOW": 1.05}


@pytest.mark.parametrize("regime,expected_mult", [
    ("HIGH", 0.90),
    ("NORMAL", 1.00),
    ("LOW", 1.05),
])
def test_analyze_market_aplica_multiplicador_por_regimen(regime, expected_mult):
    df = _mock_df()
    vol_regime = {"MERVAL": {"regime": regime}}
    signals = analyze_market(df, "MERVAL", TICKER_NAMES, vol_regime=vol_regime)

    assert len(signals) == 1
    s = signals[0]
    assert s["vol_regime_mercado"] == regime
    assert s["v2_regime_mult"] == expected_mult


def test_analyze_market_sin_vol_regime_cae_a_normal():
    """No debe romper una corrida si vol_regime no llegó (ej. falló
    compute_volatility_regime ese día en el pipeline)."""
    df = _mock_df()
    signals = analyze_market(df, "MERVAL", TICKER_NAMES, vol_regime=None)

    assert len(signals) == 1
    s = signals[0]
    assert s["vol_regime_mercado"] == "NORMAL"
    assert s["v2_regime_mult"] == 1.00


def test_analyze_market_vol_regime_sin_este_mercado_cae_a_normal():
    """Si vol_regime llegó pero no tiene entrada para ESTE mercado (ej. solo
    tiene BOVESPA/SP500), también cae a NORMAL en vez de romper."""
    df = _mock_df()
    vol_regime = {"BOVESPA": {"regime": "HIGH"}}  # sin MERVAL
    signals = analyze_market(df, "MERVAL", TICKER_NAMES, vol_regime=vol_regime)

    assert len(signals) == 1
    s = signals[0]
    assert s["vol_regime_mercado"] == "NORMAL"
    assert s["v2_regime_mult"] == 1.00


def test_regimen_high_da_score_v2_menor_que_low_para_misma_senal():
    """Consistencia direccional: para la MISMA señal subyacente, HIGH debe
    dar un score_final_v2 menor que NORMAL, y NORMAL menor que LOW."""
    df = _mock_df()
    scores = {}
    for regime in ("HIGH", "NORMAL", "LOW"):
        vol_regime = {"MERVAL": {"regime": regime}}
        signals = analyze_market(df, "MERVAL", TICKER_NAMES, vol_regime=vol_regime)
        scores[regime] = signals[0]["score_final_v2"]

    assert scores["HIGH"] < scores["NORMAL"] < scores["LOW"]
