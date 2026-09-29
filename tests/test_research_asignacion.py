"""Tests de scripts/research_asignacion.py (sin tocar data/ real)."""
import numpy as np
import pandas as pd
import pytest

from scripts import research_asignacion as ra


def _P(n=40, seed=3):
    rng = np.random.default_rng(seed)
    idx = pd.date_range("2018-01-31", periods=n, freq="ME")
    r = rng.normal(0.01, 0.05, (n, 3))
    return pd.DataFrame(100 * np.cumprod(1 + r, axis=0), index=idx, columns=ra.MERCADOS)


def test_pesos_no_miran_el_futuro():
    P = _P()
    W1 = ra.pesos_reglas(P)
    P2 = P.copy()
    P2.iloc[25:] *= np.linspace(1, 50, len(P2) - 25)[:, None]  # cambia solo el futuro
    W2 = ra.pesos_reglas(P2)
    for k in W1:
        pd.testing.assert_frame_equal(W1[k].iloc[:25], W2[k].iloc[:25])


def test_costo_y_liquidez():
    P = _P()
    W = pd.DataFrame(0.0, index=P.index, columns=P.columns)  # siempre liquidez
    r = ra.simular(P, W, P.index[12])
    assert (r == 0).all()
    W["SP500"] = 1.0
    r = ra.simular(P, W, P.index[12])
    ret = P["SP500"].pct_change()
    assert r.iloc[0] == pytest.approx(ret.iloc[13] - ra.COSTO_PATA)  # entrada paga una pata
    assert r.iloc[1] == pytest.approx(ret.iloc[14])                  # mantener no paga


def test_one_hot_elige_el_mejor():
    idx = pd.date_range("2018-01-31", periods=14, freq="ME")
    P = pd.DataFrame({"MERVAL": 100.0, "BOVESPA": 100.0,
                      "SP500": np.linspace(100, 200, 14)}, index=idx)
    W = ra.pesos_reglas(P)["R1_momentum_12_1"]
    assert W.iloc[-1]["SP500"] == 1.0 and W.iloc[-1].sum() == 1.0
