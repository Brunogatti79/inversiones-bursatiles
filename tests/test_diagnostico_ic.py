"""Tests de scripts/diagnostico_ic.py — nunca escriben en data/ real."""
import json

import numpy as np
import pandas as pd
import pytest

from scripts import diagnostico_ic as dg


@pytest.fixture(autouse=True)
def _aislar_paths(tmp_path, monkeypatch):
    monkeypatch.setattr(dg, "OUT_PATH", str(tmp_path / "diagnostico_ic.json"))
    monkeypatch.setattr(dg, "DATA_DIR", str(tmp_path))
    monkeypatch.setattr(dg, "_costo_pata_pct", lambda mk: 0.605)
    yield


def _panel_reversion(n_dias=280, n_tickers=20, seed=7, fuerza=0.08):
    """Precios donde el retorno acumulado de 21 días se revierte parcialmente."""
    rng = np.random.default_rng(seed)
    idx = pd.bdate_range("2025-08-01", periods=n_dias)
    R = np.zeros((n_dias, n_tickers))
    for t in range(n_dias):
        ruido = rng.normal(0, 0.02, n_tickers)
        rev = -fuerza * R[t - 21:t].sum(axis=0) if t >= 21 else 0
        R[t] = ruido + rev
    P = pd.DataFrame(100 * np.cumprod(1 + R, axis=0), index=idx,
                     columns=[f"T{i}" for i in range(n_tickers)])
    I = P.mean(axis=1)
    return P, I


def _panel_ruido(n_dias=280, n_tickers=20, seed=11):
    rng = np.random.default_rng(seed)
    idx = pd.bdate_range("2025-08-01", periods=n_dias)
    R = rng.normal(0, 0.02, (n_dias, n_tickers))
    P = pd.DataFrame(100 * np.cumprod(1 + R, axis=0), index=idx,
                     columns=[f"T{i}" for i in range(n_tickers)])
    return P, P.mean(axis=1)


def test_ic_series_perfecto():
    idx = pd.bdate_range("2026-01-01", periods=3)
    X = pd.DataFrame(np.tile(np.arange(10.0), (3, 1)), index=idx)
    ic = dg.ic_series(X, X * 2)
    assert np.allclose(ic.values, 1.0)


def test_ic_series_minimo_de_acciones():
    idx = pd.bdate_range("2026-01-01", periods=2)
    X = pd.DataFrame(np.tile(np.arange(5.0), (2, 1)), index=idx)
    assert dg.ic_series(X, X).empty  # menos de MIN_CS acciones


def test_clean_panel_neutraliza_split():
    idx = pd.bdate_range("2026-01-01", periods=5)
    P = pd.DataFrame({"A": [100, 101, 10.1, 10.2, 10.3], "B": [50, 51, 52, 53, 54]}, index=idx, dtype=float)
    adj, R, n_bad = dg.clean_panel(P)
    assert n_bad == 1
    assert adj["A"].iloc[-1] / adj["A"].iloc[0] == pytest.approx(1.03, abs=0.01)  # continuidad tras el split


def test_bloques_y_t():
    s = pd.Series([0.1] * 42 + [0.05] * 10, index=pd.bdate_range("2026-01-01", periods=52))
    completos, parcial = dg._bloques(s, 21)
    assert completos == [pytest.approx(0.1), pytest.approx(0.1)]
    assert parcial == pytest.approx(0.05)


def test_motor_detecta_reversion_fuera_de_muestra():
    P, I = _panel_reversion()
    r = dg.motor_mercado("TEST", P, I)
    res = r["resumen_oos"]
    assert res["n_periodos"] >= 3
    assert res["alpha_neto_medio_pp"] > 0
    # el motor tiene que haber elegido reversión con signo positivo
    factores = r["hoy"]["factores"]
    assert any(f in factores and factores[f]["ic"] > 0 for f in ("rev5", "rev21"))


def test_motor_no_mira_el_futuro():
    """Los factores elegidos en el rebalanceo k solo pueden usar IC con fecha <= k-H."""
    P, I = _panel_reversion()
    Pc, R, _ = dg.clean_panel(P)
    F = dg.compute_factors(Pc, R, I)
    fwd = Pc.shift(-dg.H) / Pc - 1
    ic = {f: dg.ic_series(X, fwd) for f, X in F.items()}
    fechas = list(Pc.index)
    pos = {d: i for i, d in enumerate(fechas)}
    k = 200
    # contaminamos el IC futuro: si se usara, cambiaría la decisión
    ic_cont = {f: s.copy() for f, s in ic.items()}
    for f in ic_cont:
        fut = [d for d in ic_cont[f].index if pos[d] > k - dg.H]
        ic_cont[f].loc[fut] = -99.0
    p1, _ = dg._elegir_factores(ic, pos, k, None)
    p2, _ = dg._elegir_factores(ic_cont, pos, k, None)
    assert p1 == p2


def test_motor_en_ruido_no_inventa_alpha():
    P, I = _panel_ruido()
    res = dg.motor_mercado("TEST", P, I)["resumen_oos"]
    assert abs(res["alpha_neto_medio_pp"]) < 3.0


def test_main_escribe_solo_en_tmp(monkeypatch, tmp_path):
    P, I = _panel_reversion()
    monkeypatch.setattr(dg, "load_prices", lambda pull=True: {"MERVAL": (P, I)})
    monkeypatch.setattr(dg, "load_archive", lambda: pd.DataFrame())
    out = dg.main(push=False)
    with open(tmp_path / "diagnostico_ic.json", encoding="utf-8") as f:
        data = json.load(f)
    assert data["shadow"] is True
    assert "MERVAL" in data["B_motor"]
    assert out["telegram_lines"]
