"""
tests/test_volatility_regime.py

Primera suite de tests de src/volatility_regime.py (Prioridad 1, roadmap
externo, 25/06/2026). Antes de este fix solo tenía un smoke test
incidental en test_pct_change_deprecation.py (confirma que no tira
FutureWarning, no que el cálculo de régimen sea correcto).

Este módulo subió de criticidad en la misma sesión: regime_factor ahora
escala kelly_f/kelly_half directamente en portfolio_optimizer.py (Prioridad
5) -- un bug silencioso acá afecta el tamaño de TODAS las posiciones
sugeridas el día que se dispare.
"""
import sys
import os
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

import numpy as np
import pandas as pd
import pytest

import src.volatility_regime as vr
from src.volatility_regime import (
    compute_volatility_regime,
    load_vol_regime,
    _calc_regime,
    _pct_to_regime,
    _find_index_col,
)


@pytest.fixture(autouse=True)
def _isolate_path(tmp_path, monkeypatch):
    monkeypatch.setattr(vr, "VOL_REGIME_PATH", str(tmp_path / "volatility_regime.json"))


def _serie(n=280, daily_vol=0.01, seed=0, name="INDICE"):
    rng = np.random.default_rng(seed)
    dates = pd.date_range("2025-01-01", periods=n, freq="B")
    prices = 100 * np.cumprod(1 + rng.normal(0, daily_vol, n))
    return pd.Series(prices, index=dates, name=name)


def _calm_then_volatile(n_calm=220, n_vol=60, seed=0):
    """Serie con tramo calmo seguido de tramo muy volátil -- la ventana de
    20d más reciente queda en régimen HIGH respecto a la historia rolling."""
    rng = np.random.default_rng(seed)
    calm = 100 * np.cumprod(1 + rng.normal(0, 0.003, n_calm))
    volatile = calm[-1] * np.cumprod(1 + rng.normal(0, 0.05, n_vol))
    dates = pd.date_range("2025-01-01", periods=n_calm + n_vol, freq="B")
    return pd.Series(np.concatenate([calm, volatile]), index=dates, name="INDICE")


def _volatile_then_calm(n_vol=220, n_calm=60, seed=0):
    """Inverso: tramo volátil seguido de uno muy calmo -- la ventana
    reciente queda en régimen LOW respecto a la historia."""
    rng = np.random.default_rng(seed)
    volatile = 100 * np.cumprod(1 + rng.normal(0, 0.05, n_vol))
    calm = volatile[-1] * np.cumprod(1 + rng.normal(0, 0.002, n_calm))
    dates = pd.date_range("2025-01-01", periods=n_vol + n_calm, freq="B")
    return pd.Series(np.concatenate([volatile, calm]), index=dates, name="INDICE")


# ── _pct_to_regime: límites exactos ──────────────────────────────────────

class TestPctToRegime:

    def test_below_30_is_low(self):
        assert _pct_to_regime(0.0) == "LOW"
        assert _pct_to_regime(29.9) == "LOW"

    def test_30_is_normal_not_low(self):
        assert _pct_to_regime(30.0) == "NORMAL"

    def test_between_30_and_70_is_normal(self):
        assert _pct_to_regime(50.0) == "NORMAL"
        assert _pct_to_regime(70.0) == "NORMAL"  # límite inclusive del lado normal

    def test_above_70_is_high(self):
        assert _pct_to_regime(70.1) == "HIGH"
        assert _pct_to_regime(100.0) == "HIGH"


# ── _find_index_col ───────────────────────────────────────────────────────

class TestFindIndexCol:

    def test_finds_merval(self):
        df = pd.DataFrame({"AAPL": [1], "INDICE MERVAL": [2]})
        assert _find_index_col(df) == "INDICE MERVAL"

    def test_finds_caret_notation(self):
        df = pd.DataFrame({"AAPL": [1], "^BVSP": [2]})
        assert _find_index_col(df) == "^BVSP"

    def test_finds_sp_variant(self):
        df = pd.DataFrame({"AAPL": [1], "S&P 500": [2]})
        assert _find_index_col(df) == "S&P 500"

    def test_no_match_returns_none(self):
        df = pd.DataFrame({"AAPL": [1], "MSFT": [2]})
        assert _find_index_col(df) is None

    def test_none_or_empty_df_returns_none(self):
        assert _find_index_col(None) is None
        assert _find_index_col(pd.DataFrame()) is None


# ── _calc_regime ──────────────────────────────────────────────────────────

class TestCalcRegime:

    def test_calm_then_volatile_detects_high(self):
        serie = _calm_then_volatile()
        result = _calc_regime(serie)
        assert result["regime"] == "HIGH"
        assert result["percentile"] > 70

    def test_volatile_then_calm_detects_low(self):
        serie = _volatile_then_calm()
        result = _calc_regime(serie)
        assert result["regime"] == "LOW"
        assert result["percentile"] < 30

    def test_constant_volatility_has_no_systematic_bias(self):
        """Volatilidad uniforme en toda la serie -> con una sola ventana de
        20 días el ruido de muestreo puede dar LOW/HIGH por azar incluso
        sin cambio real de régimen. Por construcción, el percentil compara
        la ventana reciente contra una historia rolling que la incluye a
        ella misma, así que bajo vol constante la distribución esperada es
        ~uniforme entre LOW/NORMAL/HIGH (no mayoría NORMAL) -- lo que este
        test verifica es la ausencia de un SESGO SISTEMÁTICO hacia un solo
        régimen (que sí sería síntoma de un bug real, ej. un desplazamiento
        en el cálculo del percentil)."""
        regimes = []
        for seed in range(30):
            serie = _serie(n=280, daily_vol=0.012, seed=seed)
            regimes.append(_calc_regime(serie)["regime"])
        counts = {r: regimes.count(r) for r in ("LOW", "NORMAL", "HIGH")}
        # Ningún régimen debería dominar de forma aplastante (>80%) bajo
        # volatilidad constante sin cambio de régimen real.
        assert max(counts.values()) < 24, f"Sesgo sistemático detectado: {counts}"
        # Los 3 regímenes deberían aparecer al menos alguna vez en 30 corridas.
        assert all(c > 0 for c in counts.values()), f"Algún régimen nunca apareció: {counts}"

    def test_vol_ann_is_positive_and_reasonable(self):
        serie = _serie(n=280, daily_vol=0.01, seed=1)
        result = _calc_regime(serie)
        # vol diaria 1% anualizada (~sqrt(252)) da ~15-16%, con margen amplio
        assert 5 < result["vol_ann"] < 40

    def test_short_history_defaults_to_50_percentile(self):
        """Con menos de 2×VOL_WINDOW de historia, no hay rolling suficiente
        -> percentile cae al default 50 (NORMAL), no debe crashear."""
        serie = _serie(n=35, daily_vol=0.01, seed=2)  # > VOL_WINDOW+10 pero < 2×VOL_WINDOW
        result = _calc_regime(serie)
        assert result["percentile"] == 50.0
        assert result["regime"] == "NORMAL"


# ── compute_volatility_regime: orquestación completa ────────────────────

class TestComputeVolatilityRegime:

    def test_empty_price_data_returns_fallback(self):
        result = compute_volatility_regime(price_data={}, index_cols={})
        assert result["global_regime"] == "NORMAL"
        assert result["regime_factor"] == 1.00

    def test_missing_column_falls_back_to_normal_for_that_market(self):
        df = pd.DataFrame({"OTRA_COL": [1, 2, 3]})
        result = compute_volatility_regime(
            price_data={"merval": df}, index_cols={"merval": "INDICE MERVAL"},
        )
        assert result["MERVAL"]["regime"] == "NORMAL"
        assert result["MERVAL"]["vol_ann"] == 0

    def test_finds_index_col_by_name_when_index_cols_wrong(self):
        """Si index_cols no resuelve la columna pero el nombre real
        contiene 'MERVAL', _find_index_col() debe rescatarla."""
        serie = _serie(n=280, seed=3)
        df = pd.DataFrame({"INDICE MERVAL": serie})
        result = compute_volatility_regime(
            price_data={"merval": df}, index_cols={"merval": ""},
        )
        assert result["MERVAL"]["vol_ann"] > 0  # se calculó, no quedó en el default 0

    def test_regime_factor_mapping_high(self):
        """3 mercados en HIGH -> global HIGH -> factor 0.75."""
        dfs = {m: pd.DataFrame({f"INDICE_{m}": _calm_then_volatile(seed=i)})
               for i, m in enumerate(["merval", "bovespa", "sp500"])}
        cols = {m: f"INDICE_{m}" for m in dfs}
        result = compute_volatility_regime(price_data=dfs, index_cols=cols)
        assert result["global_regime"] == "HIGH"
        assert result["regime_factor"] == 0.75

    def test_regime_factor_mapping_low(self):
        dfs = {m: pd.DataFrame({f"INDICE_{m}": _volatile_then_calm(seed=i)})
               for i, m in enumerate(["merval", "bovespa", "sp500"])}
        cols = {m: f"INDICE_{m}" for m in dfs}
        result = compute_volatility_regime(price_data=dfs, index_cols=cols)
        assert result["global_regime"] == "LOW"
        assert result["regime_factor"] == 1.10

    def test_persists_to_json_file(self, tmp_path):
        serie = _serie(n=280, seed=5)
        df = pd.DataFrame({"INDICE MERVAL": serie})
        compute_volatility_regime(price_data={"merval": df}, index_cols={"merval": "INDICE MERVAL"})
        assert os.path.exists(vr.VOL_REGIME_PATH)

    def test_malformed_input_does_not_crash_returns_fallback(self):
        """price_data con un valor que no es DataFrame (ej. None explícito)
        no debe tirar excepción no controlada -- debe caer al fallback."""
        result = compute_volatility_regime(price_data={"merval": None}, index_cols={"merval": "X"})
        assert result["global_regime"] in ("NORMAL", "LOW", "HIGH")  # no crashea


# ── load_vol_regime ───────────────────────────────────────────────────────

class TestLoadVolRegime:

    def test_missing_file_returns_default(self):
        result = load_vol_regime()
        assert result["global_regime"] == "NORMAL"
        assert result["regime_factor"] == 1.00

    def test_loads_persisted_file(self):
        serie = _serie(n=280, seed=7)
        df = pd.DataFrame({"INDICE MERVAL": serie})
        computed = compute_volatility_regime(price_data={"merval": df}, index_cols={"merval": "INDICE MERVAL"})
        loaded = load_vol_regime()
        assert loaded["global_regime"] == computed["global_regime"]
        assert loaded["regime_factor"] == computed["regime_factor"]

    def test_corrupt_file_returns_default(self):
        with open(vr.VOL_REGIME_PATH, "w") as f:
            f.write("{esto no es json valido")
        result = load_vol_regime()
        assert result["global_regime"] == "NORMAL"
