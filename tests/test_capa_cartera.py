"""Tests de src/capa_cartera.py — nunca escriben en data/ real ni pegan a GitHub."""
import json
import os
from datetime import datetime

import numpy as np
import pandas as pd
import pytest

from src import capa_cartera as cc


def _csv(df, path):
    os.makedirs(os.path.dirname(path), exist_ok=True)
    df.index.name = "Fecha"
    df.to_csv(path, sep=";", decimal=",", encoding="utf-8-sig", date_format="%Y-%m-%d")


@pytest.fixture(autouse=True)
def _aislar(tmp_path, monkeypatch):
    monkeypatch.chdir(tmp_path)
    monkeypatch.setattr("src.github_persistence.pull_file", lambda *a, **k: False)
    monkeypatch.setattr("src.github_persistence.push_file", lambda *a, **k: True)
    rng = np.random.default_rng(1)
    idx = pd.bdate_range("2026-01-01", "2026-11-30")
    vols = {"merval": 0.03, "bovespa": 0.02, "sp500": 0.01}
    for k, s in vols.items():
        p = 100 * np.cumprod(1 + rng.normal(0, s, len(idx)))
        _csv(pd.DataFrame({"INDICE": p}, index=idx), f"data/research/{k}_10y.csv")
    _csv(pd.DataFrame({"CCL": 1500.0, "BRL=X": 5.0}, index=idx), "data/research/fx_10y.csv")
    yield


def _portfolio(merval, bovespa, sp):
    os.makedirs("data", exist_ok=True)
    pos = [{"mercado": "MERVAL", "valor_actual_usd": merval},
           {"mercado": "BOVESPA", "valor_actual_usd": bovespa},
           {"mercado": "SP500", "valor_actual_usd": sp}]
    json.dump({"positions": pos}, open("data/portfolio.json", "w"))


def test_pesos_objetivo_inversa_vol():
    _portfolio(1, 1, 1)
    o = cc.run_capa_cartera(push=False, pull=False, ahora=datetime(2026, 12, 1))
    w = o["pesos_objetivo"]
    assert sum(w.values()) == pytest.approx(1.0)
    assert w["SP500"] > w["BOVESPA"] > w["MERVAL"]  # menos vol → más peso


def test_banda_y_montos():
    _portfolio(8000, 1000, 1000)  # muy cargado en MERVAL
    o = cc.run_capa_cartera(push=False, pull=False, ahora=datetime(2026, 12, 1))
    assert o["rebalancear"] is True
    assert o["montos_sugeridos_usd"]["MERVAL"] < 0 < o["montos_sugeridos_usd"]["SP500"]
    assert sum(o["montos_sugeridos_usd"].values()) == pytest.approx(0, abs=3)


def test_dentro_de_banda_no_rebalancea():
    _portfolio(1, 1, 1)
    o = cc.run_capa_cartera(push=False, pull=False, ahora=datetime(2026, 12, 1))
    w = o["pesos_objetivo"]
    _portfolio(w["MERVAL"] * 1000, w["BOVESPA"] * 1000, w["SP500"] * 1000)
    o = cc.run_capa_cartera(push=False, pull=False, ahora=datetime(2026, 12, 1))
    assert o["rebalancear"] is False and not o["montos_sugeridos_usd"]


def test_aviso_una_vez_por_dia():
    _portfolio(8000, 1000, 1000)
    a = cc.run_capa_cartera(push=False, pull=False, ahora=datetime(2026, 12, 1, 10))
    b = cc.run_capa_cartera(push=False, pull=False, ahora=datetime(2026, 12, 1, 18))
    c = cc.run_capa_cartera(push=False, pull=False, ahora=datetime(2026, 12, 2, 10))
    assert a["avisar_ahora"] and not b["avisar_ahora"] and c["avisar_ahora"]


def test_cartera_vacia_no_rompe():
    os.makedirs("data", exist_ok=True)
    json.dump({"positions": []}, open("data/portfolio.json", "w"))
    o = cc.run_capa_cartera(push=False, pull=False, ahora=datetime(2026, 12, 1))
    assert o["rebalancear"] is False and o["cartera_real"]["total_usd"] == 0


def test_seguimiento_y_datos_viejos():
    _portfolio(1, 1, 1)
    o = cc.run_capa_cartera(push=False, pull=False, ahora=datetime(2026, 12, 20))
    s = o["seguimiento"]
    assert s["inicio"] == cc.INICIO_SEGUIMIENTO_DEFAULT and s["sp500"]["dias"] > 0
    assert s["tercios"]["costos_pct"] > 0  # la entrada paga una pata
    assert o["datos_viejos"] is True       # datos al 30/11, consulta el 20/12


def test_inicio_seguimiento_se_preserva():
    _portfolio(1, 1, 1)
    json.dump({"inicio_seguimiento": "2026-10-15"}, open(cc.OUT_PATH, "w"))
    o = cc.run_capa_cartera(push=False, pull=False, ahora=datetime(2026, 12, 1))
    assert o["inicio_seguimiento"] == "2026-10-15"


# ── FIX 11/11/2026: índice MERVAL de Yahoo inutilizable → canasta ──────────
def _csv_con_acciones(indice, path, seed=7):
    """CSV de research con 4 acciones (camino aleatorio) + columna INDICE."""
    rng = np.random.default_rng(seed)
    idx = indice.index
    data = {f"T{i}.BA": 100 * np.exp(np.cumsum(rng.normal(0, 0.02, len(idx)))) for i in range(4)}
    data["INDICE"] = indice.values
    _csv(pd.DataFrame(data, index=idx), path)


def test_indice_completo_se_usa_tal_cual():
    idx = pd.bdate_range("2026-01-01", periods=200)
    df = pd.DataFrame({"A.BA": np.linspace(10, 20, 200), "INDICE": np.linspace(1000, 2000, 200)}, index=idx)
    serie, fuente = cc.indice_o_canasta(df)
    assert fuente == "indice"
    assert serie.equals(df["INDICE"])


def test_indice_con_poca_cobertura_usa_canasta():
    """Caso real: 64 valores de INDICE en 2.437 ruedas."""
    idx = pd.bdate_range("2025-01-01", periods=400)
    indice = pd.Series(np.nan, index=idx)
    indice.iloc[300:364] = 2_800_000.0
    df = pd.DataFrame({"A.BA": 100.0, "B.BA": 50.0, "INDICE": indice}, index=idx)
    df.loc[idx[1:], "A.BA"] = 100.0 * 1.01 ** np.arange(1, 400)
    serie, fuente = cc.indice_o_canasta(df)
    assert fuente == "canasta_ew"
    assert serie.index[-1] == idx[-1]
    # A sube 1% por rueda, B plana → la canasta sube 0,5% por rueda
    assert serie.pct_change().dropna().round(6).eq(0.005).all()


def test_indice_atrasado_usa_canasta():
    """Índice completo pero frenado 15 ruedas antes que las acciones."""
    idx = pd.bdate_range("2026-01-01", periods=200)
    indice = pd.Series(np.linspace(1000, 2000, 200), index=idx)
    indice.iloc[-15:] = np.nan
    df = pd.DataFrame({"A.BA": np.linspace(10, 20, 200), "INDICE": indice}, index=idx)
    assert cc.indice_o_canasta(df)[1] == "canasta_ew"


def test_canasta_neutraliza_saltos():
    idx = pd.bdate_range("2026-01-01", periods=10)
    df = pd.DataFrame({"A.BA": 100.0, "B.BA": 100.0}, index=idx)
    df.loc[idx[5]:, "A.BA"] = 10.0          # −90% en un día: split sin ajustar
    serie = cc.canasta_ew(df)
    assert serie.iloc[-1] == pytest.approx(100.0)


def test_run_con_merval_sin_indice_no_queda_con_datos_viejos(tmp_path, monkeypatch):
    idx = pd.bdate_range(end="2026-11-10", periods=400)
    rng = np.random.default_rng(1)
    for k, vol in (("bovespa", 0.02), ("sp500", 0.01)):
        p = 100 * np.exp(np.cumsum(rng.normal(0, vol, len(idx))))
        _csv(pd.DataFrame({"INDICE": p}, index=idx), f"data/research/{k}_10y.csv")
    roto = pd.Series(np.nan, index=idx)
    roto.iloc[300:364] = 2_800_000.0         # termina 36 ruedas antes del final
    _csv_con_acciones(roto, "data/research/merval_10y.csv")

    o = cc.run_capa_cartera(push=False, pull=False, ahora=datetime(2026, 11, 11, 12, 0))
    assert o["fuente_serie"] == {"MERVAL": "canasta_ew", "BOVESPA": "indice", "SP500": "indice"}
    assert o["fecha_datos"] == "2026-11-10"
    assert o["datos_viejos"] is False
    assert any("canasta equiponderada" in l for l in o["telegram_lines"])
