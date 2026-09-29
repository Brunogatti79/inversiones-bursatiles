#!/usr/bin/env python3
"""
scripts/research_asignacion.py — Capa de asignación ENTRE mercados (investigación)
=================================================================================

SHADOW / INVESTIGACIÓN. No toca señales, Kelly ni la cartera. Lee
data/research/ (backfill de 10 años) y escribe data/research/asignacion_resultados.json.

Pregunta: ¿alguna regla simple para decidir en qué mercado estar (MERVAL /
BOVESPA / S&P, medido en USD) le gana a no decidir nada?

Todo se mide en USD a fin de mes:
  MERVAL  = índice / CCL (GGAL)      → lo que obtiene un inversor en dólares
  BOVESPA = índice / BRL=X
  S&P 500 = índice
  (^MERV y ^GSPC son índices de precio; ^BVSP incluye dividendos. Se usa
   además la canasta de nuestro universo — cierres ajustados — como control.)

Reglas FIJADAS ANTES de mirar resultados (sin parámetros optimizados):
  R1 momentum_12_1   100% en el mercado con mejor retorno 12 meses salteando el último
  R2 momentum_6      100% en el mejor retorno de 6 meses
  R3 dual_momentum   R1, pero a liquidez si ese mercado tiene retorno 12m negativo
  R4 tendencia_10m   1/3 por mercado solo si su precio USD > promedio 10 meses; si no, ese tercio a liquidez
  R5 top2_momentum   50/50 en los dos mejores por R1
  R6 inversa_vol     pesos ∝ 1/volatilidad 6 meses (diversificación pura, sin predicción)
Referencias:
  B1 100% S&P · B2 1/3 cada uno rebalanceado mensual · B3 1/3 comprado y mantenido

Costos: 0,605% por pata (Balanz) sobre lo que se rota. Liquidez rinde 0%
(conservador: penaliza a las reglas que salen a liquidez).

Criterio de adopción (fijado antes): una regla "sirve" solo si le gana a B2
en Sharpe, en al menos 60% de los años, y con drawdown máximo no peor que
B2 + 5 pp; y sostiene el signo en las dos mitades de la muestra.
Con 6 reglas probadas, 1 que pase por azar es esperable: se reporta el t del
exceso mensual y se exige t >= 2 para considerarlo evidencia.
"""

from __future__ import annotations

import json
import logging
import math
import os
import sys
from datetime import datetime

import numpy as np
import pandas as pd

_ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
if _ROOT not in sys.path:
    sys.path.insert(0, _ROOT)

logger = logging.getLogger(__name__)

RESEARCH_DIR = "data/research"
OUT_PATH = os.path.join(RESEARCH_DIR, "asignacion_resultados.json")
COSTO_PATA = 0.00605
MERCADOS = ["MERVAL", "BOVESPA", "SP500"]


# ─────────────────────────────────────────────────────────────────────────────
# Datos
# ─────────────────────────────────────────────────────────────────────────────
def _leer(nombre: str) -> pd.DataFrame:
    path = os.path.join(RESEARCH_DIR, nombre)
    if not os.path.exists(path):
        try:
            from src.github_persistence import pull_file
            pull_file(path)
        except Exception as e:
            logger.warning(f"[asignacion] no pude traer {path}: {e}")
    df = pd.read_csv(path, sep=";", decimal=",", encoding="utf-8-sig", index_col=0)
    df.index = pd.to_datetime(df.index, errors="coerce")
    return df[df.index.notna()].sort_index().apply(pd.to_numeric, errors="coerce")


def _canasta(df: pd.DataFrame) -> pd.Series:
    """Índice equiponderado diario del universo (cierres ajustados), con los
    saltos > 45% neutralizados igual que en el motor."""
    P = df.drop(columns=["INDICE"], errors="ignore")
    R = P.pct_change(fill_method=None)
    R = R.mask(R.abs() > 0.45, 0.0)
    r = R.mean(axis=1, skipna=True).fillna(0.0)
    return (1 + r).cumprod()


def precios_usd_mensuales(fuente: str = "indice") -> pd.DataFrame:
    """Precio en USD a fin de mes por mercado. fuente: "indice" o "canasta"."""
    fx = _leer("fx_10y.csv")
    serie = {}
    for key, mk in (("merval", "MERVAL"), ("bovespa", "BOVESPA"), ("sp500", "SP500")):
        df = _leer(f"{key}_10y.csv")
        serie[mk] = df["INDICE"] if fuente == "indice" else _canasta(df)
    d = pd.DataFrame(serie)
    d = d[d.index.dayofweek < 5].ffill(limit=5)
    ccl = fx["CCL"].reindex(d.index).ffill(limit=5)
    brl = fx["BRL=X"].reindex(d.index).ffill(limit=5)
    usd = pd.DataFrame({
        "MERVAL": d["MERVAL"] / ccl,
        "BOVESPA": d["BOVESPA"] / brl,
        "SP500": d["SP500"],
    })
    m = usd.resample("ME").last().dropna()
    return m


# ─────────────────────────────────────────────────────────────────────────────
# Reglas: cada una devuelve pesos (fila = fin de mes t, se aplican al mes t+1)
# ─────────────────────────────────────────────────────────────────────────────
def pesos_reglas(P: pd.DataFrame) -> dict:
    ret1 = P.pct_change(fill_method=None)
    mom12_1 = P.shift(1) / P.shift(12) - 1
    mom6 = P / P.shift(6) - 1
    mom12 = P / P.shift(12) - 1
    sma10 = P.rolling(10).mean()
    vol6 = ret1.rolling(6).std()

    def one_hot(score: pd.DataFrame) -> pd.DataFrame:
        w = pd.DataFrame(0.0, index=score.index, columns=score.columns)
        ok = score.notna().all(axis=1)
        best = score[ok].idxmax(axis=1)
        for d, c in best.items():
            w.loc[d, c] = 1.0
        w[~ok] = np.nan
        return w

    R = {}
    R["R1_momentum_12_1"] = one_hot(mom12_1)
    R["R2_momentum_6"] = one_hot(mom6)
    dual = one_hot(mom12_1)
    for d in dual.index:
        if dual.loc[d].notna().all():
            c = dual.loc[d].idxmax()
            if mom12.loc[d, c] <= 0:
                dual.loc[d] = 0.0  # a liquidez
    R["R3_dual_momentum"] = dual
    tend = (P > sma10).astype(float) / 3
    tend[sma10.isna().any(axis=1)] = np.nan
    R["R4_tendencia_10m"] = tend
    top2 = pd.DataFrame(np.nan, index=P.index, columns=P.columns)
    for d in P.index:
        s = mom12_1.loc[d]
        if s.notna().all():
            top2.loc[d] = 0.0
            top2.loc[d, s.nlargest(2).index] = 0.5
    R["R5_top2_momentum"] = top2
    inv = 1 / vol6
    R["R6_inversa_vol"] = inv.div(inv.sum(axis=1), axis=0)
    R["B1_100_SP500"] = pd.DataFrame({"MERVAL": 0.0, "BOVESPA": 0.0, "SP500": 1.0}, index=P.index)
    R["B2_tercios_rebal"] = pd.DataFrame(1 / 3, index=P.index, columns=P.columns)
    return R


def simular(P: pd.DataFrame, W: pd.DataFrame, inicio, costo: float = COSTO_PATA,
            buy_and_hold: bool = False) -> pd.Series:
    """Retorno mensual neto. Pesos decididos a fin de mes t, aplicados en t+1.
    Costo = costo × (compras + ventas) sobre los pesos que cambian, tomando
    como punto de partida los pesos que dejó la deriva del mes anterior."""
    r = P.pct_change(fill_method=None)
    fechas = [d for d in P.index if d >= inicio]
    out = {}
    w_prev = pd.Series(0.0, index=P.columns)  # arranca 100% liquidez
    for i in range(len(fechas) - 1):
        d, d1 = fechas[i], fechas[i + 1]
        if buy_and_hold and i > 0:
            w = w_prev
        else:
            w = W.loc[d].fillna(0.0)
        turnover = float((w - w_prev).abs().sum())
        ret_bruto = float((w * r.loc[d1]).sum())
        out[d1] = ret_bruto - costo * turnover
        # deriva de pesos al cierre del mes
        valor = w * (1 + r.loc[d1])
        total = float(valor.sum()) + (1 - float(w.sum()))
        w_prev = (valor / total) if total > 0 else w * 0
    return pd.Series(out)


# ─────────────────────────────────────────────────────────────────────────────
# Métricas
# ─────────────────────────────────────────────────────────────────────────────
def metricas(r: pd.Series) -> dict:
    r = r.dropna()
    eq = (1 + r).cumprod()
    anios = len(r) / 12
    cagr = eq.iloc[-1] ** (1 / anios) - 1 if anios > 0 else np.nan
    vol = r.std(ddof=1) * math.sqrt(12)
    dd = (eq / eq.cummax() - 1).min()
    por_anio = ((1 + r).groupby(r.index.year).prod() - 1) * 100
    return {
        "cagr_pct": round(cagr * 100, 1),
        "vol_pct": round(vol * 100, 1),
        "sharpe": round(cagr / vol, 2) if vol > 0 else None,
        "max_dd_pct": round(dd * 100, 1),
        "meses": int(len(r)),
        "por_anio_pct": {int(k): round(float(v), 1) for k, v in por_anio.items()},
    }


def comparar(r: pd.Series, ref: pd.Series) -> dict:
    ex = (r - ref).dropna()
    t = ex.mean() / ex.std(ddof=1) * math.sqrt(len(ex)) if ex.std(ddof=1) > 0 else None
    ya = ((1 + r).groupby(r.index.year).prod() - (1 + ref).groupby(ref.index.year).prod())
    mitad = len(ex) // 2
    return {
        "exceso_mensual_medio_pp": round(float(ex.mean() * 100), 2),
        "t_exceso": round(float(t), 2) if t is not None else None,
        "pct_anios_gana": round(float((ya > 0).mean()), 2),
        "exceso_1ra_mitad_pp": round(float(ex.iloc[:mitad].mean() * 100), 2),
        "exceso_2da_mitad_pp": round(float(ex.iloc[mitad:].mean() * 100), 2),
    }


def evaluar(fuente: str = "indice") -> dict:
    P = precios_usd_mensuales(fuente)
    W = pesos_reglas(P)
    # arranque común: primer mes con todas las reglas definidas
    inicio = max(w.dropna(how="any").index.min() for w in W.values())
    series = {k: simular(P, w, inicio) for k, w in W.items()}
    series["B3_tercios_buy_hold"] = simular(P, W["B2_tercios_rebal"], inicio, buy_and_hold=True)
    ref = series["B2_tercios_rebal"]
    m_ref = metricas(ref)
    res = {}
    for k, s in series.items():
        m = metricas(s)
        c = comparar(s, ref) if not k.startswith("B2") else None
        pasa = None
        if k.startswith("R") and c:
            pasa = bool(
                m["sharpe"] is not None and m_ref["sharpe"] is not None
                and m["sharpe"] > m_ref["sharpe"]
                and c["pct_anios_gana"] >= 0.60
                and m["max_dd_pct"] >= m_ref["max_dd_pct"] - 5
                and c["exceso_1ra_mitad_pp"] > 0 and c["exceso_2da_mitad_pp"] > 0
            )
        rot = None
        if k in W:
            w = W[k].loc[W[k].index >= inicio].fillna(0.0)
            rot = round(float(w.diff().abs().sum(axis=1).mean()), 2)
        res[k] = {"metricas": m, "vs_B2": c, "pasa_criterio": pasa,
                  "evidencia_t2": bool(c and c["t_exceso"] is not None and c["t_exceso"] >= 2),
                  "rotacion_media_mensual": rot}
    # posición de hoy según cada regla (informativo)
    hoy = {k: {c: round(float(v), 2) for c, v in w.iloc[-1].fillna(0).items()} for k, w in W.items()}
    return {"desde": str(inicio.date()), "hasta": str(P.index[-1].date()),
            "reglas": res, "posicion_hoy": hoy,
            "mercados_usd_por_anio_pct": {
                mk: {int(k): round(float(v), 1) for k, v in
                     (((1 + P[mk].pct_change()).groupby(P.index.year).prod() - 1) * 100).items()}
                for mk in MERCADOS}}


def main(push: bool = True) -> dict:
    out = {
        "generated": datetime.now().isoformat(timespec="seconds"),
        "shadow": True,
        "costo_pata_pct": COSTO_PATA * 100,
        "indice": evaluar("indice"),
        "canasta": evaluar("canasta"),
        "advertencias": [
            "^MERV y ^GSPC son índices de precio (sin dividendos); ^BVSP incluye dividendos.",
            "La canasta usa tickers actuales: sesgo de supervivencia (más fuerte en S&P).",
            "Liquidez rinde 0% (conservador). Costos: Balanz 0,605% por pata, sin spread de CCL.",
            "6 reglas probadas: una que pase por azar es esperable; exigir t >= 2 y ambas fuentes.",
        ],
    }
    try:
        from src.github_persistence import save_json
        save_json(OUT_PATH, out, message="research: asignacion entre mercados", push=push)
    except Exception:
        os.makedirs(RESEARCH_DIR, exist_ok=True)
        with open(OUT_PATH, "w", encoding="utf-8") as f:
            json.dump(out, f, ensure_ascii=False, indent=2)
    return out


if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO)
    o = main(push="--no-push" not in sys.argv)
    for fuente in ("indice", "canasta"):
        print(f"\n== {fuente} {o[fuente]['desde']} → {o[fuente]['hasta']} ==")
        for k, v in o[fuente]["reglas"].items():
            m, c = v["metricas"], v["vs_B2"] or {}
            print(f"{k:22s} CAGR {m['cagr_pct']:6.1f}%  vol {m['vol_pct']:5.1f}%  Sharpe {m['sharpe']}  "
                  f"DD {m['max_dd_pct']:6.1f}%  exc {c.get('exceso_mensual_medio_pp')} t {c.get('t_exceso')} "
                  f"años {c.get('pct_anios_gana')} mitades {c.get('exceso_1ra_mitad_pp')}/{c.get('exceso_2da_mitad_pp')} "
                  f"pasa {v['pasa_criterio']} rot {v['rotacion_media_mensual']}")
