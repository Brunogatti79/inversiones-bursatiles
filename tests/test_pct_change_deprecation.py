"""
tests/test_pct_change_deprecation.py

Regresión para el FutureWarning reportado por Bruno en el log de
producción (24/06/2026):

    /app/src/portfolio_optimizer.py:248: FutureWarning: The default
    fill_method='pad' in Series.pct_change is deprecated...

Al revisar, el mismo patrón (`.pct_change()` sin `fill_method` explícito)
aparecía en 6 módulos, no solo en el que mostró el log: analyzer.py (5),
cross_market.py (3), data_validator.py (1), portfolio_optimizer.py (1),
predictor.py (9), volatility_regime.py (1) — 20 call sites en total.
Se agregó `fill_method=None` explícito a todos.

Nota sobre el cambio de comportamiento real (no solo silenciar el
warning): para series sin gaps internos (el caso normal de estos CSVs,
verificado manualmente) el resultado es idéntico byte a byte. Para una
serie CON un gap interno, `fill_method=None` deja ese día y el siguiente
como NaN en vez de fabricar un retorno de 0% rellenando hacia adelante --
es el comportamiento más correcto, pero vale la pena dejarlo testeado
explícitamente para que si algún día importa, no sea una sorpresa.

Estos tests no re-verifican toda la lógica de cada función (eso ya lo
cubren test_analyzer.py / test_weight_optimizer.py / etc.) -- se enfocan
en confirmar que el warning específico no se dispara y que el
comportamiento sobre datos limpios no cambió.
"""
import warnings

import numpy as np
import pandas as pd
import pytest


def _clean_series(n=120, seed=0):
    rng = np.random.RandomState(seed)
    return pd.Series(100 + rng.randn(n).cumsum())


@pytest.fixture
def _raise_on_future_warning():
    """Convierte cualquier FutureWarning en excepción dentro del test --
    si algún pct_change() sin fill_method se reintrodujera, esto lo
    atrapa inmediatamente en CI en vez de aparecer como una línea
    silenciosa en el log de Railway."""
    with warnings.catch_warnings():
        warnings.simplefilter("error", FutureWarning)
        yield


class TestNoFutureWarningRaised:

    def test_analyzer_volatility_score(self, _raise_on_future_warning):
        from src.analyzer import _volatility_score
        _volatility_score(_clean_series())

    def test_data_validator_consistencia(self, _raise_on_future_warning):
        from src.data_validator import validar_consistencia
        df = pd.DataFrame({"INDICE MERVAL": _clean_series(10).values})
        validar_consistencia(df, market="MERVAL", index_col="INDICE MERVAL")

    def test_cross_market_context(self, _raise_on_future_warning):
        from src.cross_market import compute_cross_market_context
        col = "INDICE"
        merval_df  = pd.DataFrame({col: _clean_series(60, seed=4).values})
        bovespa_df = pd.DataFrame({col: _clean_series(60, seed=5).values})
        sp500_df   = pd.DataFrame({col: _clean_series(60, seed=6).values})
        compute_cross_market_context(merval_df, bovespa_df, sp500_df, index_cols={
            "merval": col, "bovespa": col, "sp500": col,
        })

    def test_portfolio_optimizer_covariance_adjustment(self, _raise_on_future_warning):
        from src.portfolio_optimizer import _covariance_adjustment

        ticker = "GGAL.BA"
        buy_signals = [{"ticker": ticker, "kelly_half": 2.0}, {"ticker": "BMA.BA", "kelly_half": 1.5}]
        df = pd.DataFrame({
            ticker: _clean_series(80, seed=1).values,
            "BMA.BA": _clean_series(80, seed=2).values,
        })
        price_data = {"merval": df}
        ticker_cols = {ticker: ticker, "BMA.BA": "BMA.BA"}

        weights = _covariance_adjustment(buy_signals, price_data, ticker_cols)
        assert len(weights) == 2

    def test_predictor_gradient_boosting_features(self, _raise_on_future_warning):
        import src.predictor as predictor
        predictor.predict_ticker("TEST_PCT_CHANGE", _clean_series(90, seed=3))

    def test_volatility_regime(self, _raise_on_future_warning):
        from src.volatility_regime import compute_volatility_regime
        col = "INDICE"
        price_data = {"merval": pd.DataFrame({col: _clean_series(90).values})}
        compute_volatility_regime(price_data, index_cols={"merval": col})


class TestBehaviorUnchangedForCleanSeries:
    """fill_method=None solo cambia algo si hay gaps internos -- para
    series limpias (el caso normal de estos CSVs) el resultado debe ser
    idéntico al de antes del fix."""

    def test_clean_series_identical_with_and_without_fill_method(self):
        s = _clean_series(50)
        with warnings.catch_warnings():
            warnings.simplefilter("ignore", FutureWarning)
            old_behavior = s.pct_change()
        new_behavior = s.pct_change(fill_method=None)
        pd.testing.assert_series_equal(old_behavior, new_behavior)

    def test_series_with_internal_gap_drops_two_points_instead_of_fabricating_zero(self):
        """Documenta el único caso donde el comportamiento SÍ cambia: un
        gap interno ya no se rellena hacia adelante con un 0% artificial,
        sino que ese día y el siguiente quedan NaN (y se descartan con
        dropna() en el código real)."""
        s = pd.Series([100.0, np.nan, 105.0, 103.0])
        new_behavior = s.pct_change(fill_method=None)
        assert new_behavior.isna().sum() == 3  # primero (siempre NaN) + el gap + el día siguiente al gap
