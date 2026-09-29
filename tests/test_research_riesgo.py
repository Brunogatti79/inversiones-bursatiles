"""Tests de scripts/research_riesgo.py (sin tocar data/ real)."""
import numpy as np
import pandas as pd
import pytest

from scripts import research_asignacion as ra
from scripts import research_riesgo as rr


def _P(n=40, seed=5):
    rng = np.random.default_rng(seed)
    idx = pd.date_range("2018-01-31", periods=n, freq="ME")
    r = rng.normal(0.01, 0.06, (n, 3))
    return pd.DataFrame(100 * np.cumprod(1 + r, axis=0), index=idx, columns=ra.MERCADOS)


def test_banda_cero_igual_al_simulador_base():
    P = _P()
    W = pd.DataFrame(1 / 3, index=P.index, columns=P.columns)
    a, _ = rr.simular_bandas(P, W, P.index[5], banda=0.0)
    b = ra.simular(P, W, P.index[5])
    pd.testing.assert_series_equal(a, b, check_names=False)


def test_banda_reduce_rotacion_y_no_cambia_si_no_hay_desvio():
    P = _P()
    W = pd.DataFrame(1 / 3, index=P.index, columns=P.columns)
    _, rot0 = rr.simular_bandas(P, W, P.index[5], banda=0.0)
    _, rot5 = rr.simular_bandas(P, W, P.index[5], banda=0.05)
    assert rot5 < rot0


def test_exposicion_propia_sin_mirar_el_futuro():
    v = pd.Series(np.r_[np.full(20, 0.2), np.full(20, 0.4)],
                  index=pd.date_range("2018-01-31", periods=40, freq="ME"))
    e1 = rr.exposicion_propia(v)
    v2 = v.copy(); v2.iloc[30:] = 5.0
    e2 = rr.exposicion_propia(v2)
    pd.testing.assert_series_equal(e1.iloc[:30], e2.iloc[:30])
    assert e1.iloc[19] == pytest.approx(1.0) and e1.iloc[20] < 1.0
    assert (e1.dropna() <= 1.0).all()
