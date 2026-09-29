#!/usr/bin/env python3
"""
scripts/research_riesgo.py — Camino A: gestión de riesgo y costos (investigación)
================================================================================

SHADOW / INVESTIGACIÓN. No toca producción. Lee data/research/ y escribe
data/research/riesgo_resultados.json.

Objetivo de esta capa: NO es ganarle al mercado (eso ya se probó que no sale),
es obtener el mismo o casi el mismo retorno con menos caída y menos costos.

Todas las reglas y parámetros se fijaron ANTES de mirar resultados:

1) POR MERCADO (cada mercado por separado, en USD): control de volatilidad propio
   exposición_t = min(1, vol_mediana_historica_t / vol_actual_t)
     vol_actual = volatilidad diaria anualizada de las últimas 63 ruedas
     vol_mediana_historica = mediana de vol_actual desde el inicio HASTA t
     (sin mirar el futuro; cada mercado se compara contra su propia normalidad)
   El resto queda en liquidez. Banda: solo se opera si la exposición cambia > 10 pp.

2) CARTERA de los 3 mercados (USD, mensual):
   B1  100% S&P                              (referencia)
   B2  tercios rebalanceados todos los meses (referencia)
   C1  tercios con banda de 5 pp             (mismo riesgo, menos costo)
   C2  pesos por inversa de volatilidad 63d, banda 5 pp
   C3  C2 + control de vol de cartera a 15% anual, banda 10 pp
   C4  B1 con control de vol propio (regla 1 aplicada al S&P)

3) DENTRO de cada mercado (tamaño de posición por acción): canasta
   equiponderada vs canasta por inversa de volatilidad 63d, rebalanceo mensual,
   costos sobre lo rotado. Es lo que haría el sistema al dimensionar posiciones.

Costos: Balanz 0,605% por pata sobre lo que se rota. Liquidez rinde 0%
(conservador: con tasa en USD positiva el control de vol quedaría mejor).

Criterio (fijado antes): una regla de riesgo "sirve" si, contra su versión sin
gestionar, (a) mejora el Sharpe, (b) reduce la caída máxima en >= 5 pp y
(c) mejora el Sharpe en las dos mitades de la muestra.
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

from scripts import research_asignacion as ra  # noqa: E402

logger = logging.getLogger(__name__)

OUT_PATH = os.path.join(ra.RESEARCH_DIR, "riesgo_resultados.json")
COSTO = ra.COSTO_PATA
VENTANA_VOL = 63
TARGET_CARTERA = 0.15
BANDA_PESOS = 0.05
BANDA_EXPOSICION = 0.10
SALTO = 0.45


# ─────────────────────────────────────────────────────────────────────────────
# Datos diarios en USD
# ─────────────────────────────────────────────────────────────────────────────
def usd_diario(fuente: str = "indice") -> pd.DataFrame:
    fx = ra._leer("fx_10y.csv")
    serie = {}
    for key, mk in (("merval", "MERVAL"), ("bovespa", "BOVESPA"), ("sp500", "SP500")):
        df = ra._leer(f"{key}_10y.csv")
        serie[mk] = df["INDICE"] if fuente == "indice" else ra._canasta(df)
    d = pd.DataFrame(serie)
    d = d[d.index.dayofweek < 5].ffill(limit=5)
    ccl = fx["CCL"].reindex(d.index).ffill(limit=5)
    brl = fx["BRL=X"].reindex(d.index).ffill(limit=5)
    return pd.DataFrame({"MERVAL": d["MERVAL"] / ccl, "BOVESPA": d["BOVESPA"] / brl,
                         "SP500": d["SP500"]}).dropna()


def vol_anual(daily: pd.DataFrame, ventana: int = VENTANA_VOL) -> pd.DataFrame:
    r = daily.pct_change(fill_method=None)
    r = r.mask(r.abs() > SALTO, 0.0)
    return r.rolling(ventana, min_periods=int(ventana * 0.8)).std() * math.sqrt(252)


# ─────────────────────────────────────────────────────────────────────────────
# Simulador mensual con banda de no-operación
# ─────────────────────────────────────────────────────────────────────────────
def simular_bandas(P: pd.DataFrame, W: pd.DataFrame, inicio, banda: float = 0.0,
                   costo: float = COSTO) -> tuple[pd.Series, float]:
    """Pesos objetivo W decididos a fin de mes t y aplicados en t+1. Si el
    desvío máximo entre el objetivo y los pesos que dejó la deriva es <= banda,
    no se opera. Devuelve (retornos netos mensuales, rotación media mensual)."""
    r = P.pct_change(fill_method=None)
    fechas = [d for d in P.index if d >= inicio]
    out, rot = {}, []
    w_prev = pd.Series(0.0, index=P.columns)
    for i in range(len(fechas) - 1):
        d, d1 = fechas[i], fechas[i + 1]
        obj = W.loc[d].reindex(P.columns).fillna(0.0)
        dev = float((obj - w_prev).abs().max())
        w = obj if (i == 0 or dev > banda) else w_prev
        t = float((w - w_prev).abs().sum())
        rot.append(t)
        rr = r.loc[d1].fillna(0.0)
        out[d1] = float((w * rr).sum()) - costo * t
        valor = w * (1 + rr)
        total = float(valor.sum()) + (1 - float(w.sum()))
        w_prev = valor / total if total > 0 else w * 0
    return pd.Series(out), (float(np.mean(rot)) if rot else 0.0)


def _fin_de_mes(df: pd.DataFrame) -> pd.DataFrame:
    return df.resample("ME").last()


# ─────────────────────────────────────────────────────────────────────────────
# Reglas
# ─────────────────────────────────────────────────────────────────────────────
def exposicion_propia(vol_m: pd.Series) -> pd.Series:
    """min(1, mediana histórica (expandida, hasta t) / vol actual)."""
    med = vol_m.expanding(min_periods=12).median()
    return (med / vol_m).clip(upper=1.0)


def vol_cartera(daily: pd.DataFrame, pesos_m: pd.DataFrame) -> pd.Series:
    """Vol anualizada de los últimos 63 días de la cartera con los pesos de t
    (estimación ex-ante: pesos de hoy sobre retornos pasados)."""
    r = daily.pct_change(fill_method=None)
    r = r.mask(r.abs() > SALTO, 0.0)
    out = {}
    for d, w in pesos_m.iterrows():
        hist = r.loc[:d].tail(VENTANA_VOL)
        if len(hist) < int(VENTANA_VOL * 0.8) or w.isna().any():
            continue
        out[d] = float((hist * w).sum(axis=1).std() * math.sqrt(252))
    return pd.Series(out)


def evaluar(fuente: str = "indice") -> dict:
    daily = usd_diario(fuente)
    P = _fin_de_mes(daily)
    V = _fin_de_mes(vol_anual(daily))
    inicio = V.dropna().index[12]  # necesita 12 meses para la mediana histórica

    res = {"desde": str(inicio.date()), "hasta": str(P.index[-1].date())}

    # 1) por mercado
    por_mercado = {}
    for mk in P.columns:
        bh = pd.DataFrame({mk: 1.0}, index=P.index)
        vt = pd.DataFrame({mk: exposicion_propia(V[mk])}, index=P.index)
        r_bh, rot_bh = simular_bandas(P[[mk]], bh, inicio)
        r_vt, rot_vt = simular_bandas(P[[mk]], vt, inicio, banda=BANDA_EXPOSICION)
        por_mercado[mk] = {
            "sin_gestion": {**ra.metricas(r_bh), "rotacion": round(rot_bh, 3)},
            "control_vol_propio": {**ra.metricas(r_vt), "rotacion": round(rot_vt, 3),
                                    "exposicion_media": round(float(vt[mk].loc[inicio:].mean()), 2),
                                    "exposicion_hoy": round(float(vt[mk].iloc[-1]), 2)},
            "veredicto": veredicto(r_vt, r_bh),
        }
    res["por_mercado"] = por_mercado

    # 2) cartera
    tercios = pd.DataFrame(1 / 3, index=P.index, columns=P.columns)
    inv = 1 / V
    invw = inv.div(inv.sum(axis=1), axis=0)
    vc = vol_cartera(daily, invw)
    escala = (TARGET_CARTERA / vc).clip(upper=1.0).reindex(P.index)
    c3 = invw.mul(escala, axis=0)
    sp = pd.DataFrame({"MERVAL": 0.0, "BOVESPA": 0.0, "SP500": 1.0}, index=P.index)
    c4 = sp.copy()
    c4["SP500"] = exposicion_propia(V["SP500"])

    carteras = {
        "B1_100_SP500": simular_bandas(P, sp, inicio),
        "B2_tercios_mensual": simular_bandas(P, tercios, inicio),
        "C1_tercios_banda5": simular_bandas(P, tercios, inicio, banda=BANDA_PESOS),
        "C2_inversa_vol_banda5": simular_bandas(P, invw, inicio, banda=BANDA_PESOS),
        "C3_inv_vol_target15": simular_bandas(P, c3, inicio, banda=BANDA_EXPOSICION),
        "C4_SP500_control_vol": simular_bandas(P, c4, inicio, banda=BANDA_EXPOSICION),
    }
    ref_de = {"C1_tercios_banda5": "B2_tercios_mensual", "C2_inversa_vol_banda5": "B2_tercios_mensual",
              "C3_inv_vol_target15": "B2_tercios_mensual", "C4_SP500_control_vol": "B1_100_SP500"}
    cart = {}
    for k, (r, rot) in carteras.items():
        cart[k] = {**ra.metricas(r), "rotacion": round(rot, 3)}
        if k in ref_de:
            cart[k]["vs"] = ref_de[k]
            cart[k]["veredicto"] = veredicto(r, carteras[ref_de[k]][0])
    res["carteras"] = cart
    res["pesos_hoy"] = {
        "C2_inversa_vol": {c: round(float(v), 2) for c, v in invw.iloc[-1].items()},
        "C3_inv_vol_target15": {c: round(float(v), 2) for c, v in c3.iloc[-1].fillna(0).items()},
        "vol_63d_hoy_pct": {c: round(float(v) * 100, 1) for c, v in V.iloc[-1].items()},
    }
    return res


def veredicto(r: pd.Series, ref: pd.Series) -> dict:
    m, mr = ra.metricas(r), ra.metricas(ref)
    mitad = len(r) // 2

    def sh(x):
        x = x.dropna()
        s = x.std(ddof=1)
        return (x.mean() / s * math.sqrt(12)) if s > 0 else 0.0

    s1, s1r = sh(r.iloc[:mitad]), sh(ref.iloc[:mitad])
    s2, s2r = sh(r.iloc[mitad:]), sh(ref.iloc[mitad:])
    mejora_dd = m["max_dd_pct"] - mr["max_dd_pct"]
    return {
        "delta_sharpe": round((m["sharpe"] or 0) - (mr["sharpe"] or 0), 2),
        "delta_cagr_pp": round(m["cagr_pct"] - mr["cagr_pct"], 1),
        "mejora_caida_max_pp": round(mejora_dd, 1),
        "sharpe_mitades": [round(s1 - s1r, 2), round(s2 - s2r, 2)],
        "sirve": bool((m["sharpe"] or 0) > (mr["sharpe"] or 0) and mejora_dd >= 5
                      and s1 > s1r and s2 > s2r),
    }


# 3) dentro de cada mercado: equiponderado vs inversa de vol por acción
def tamanio_por_accion() -> dict:
    fx = ra._leer("fx_10y.csv")
    out = {}
    for key, mk in (("merval", "MERVAL"), ("bovespa", "BOVESPA"), ("sp500", "SP500")):
        df = ra._leer(f"{key}_10y.csv").drop(columns=["INDICE"], errors="ignore")
        df = df[df.index.dayofweek < 5].ffill(limit=5)
        R = df.pct_change(fill_method=None)
        R = R.mask(R.abs() > SALTO, 0.0)
        adj = (1 + R.fillna(0)).cumprod().where(df.notna())
        if mk == "MERVAL":
            adj = adj.div(fx["CCL"].reindex(adj.index).ffill(limit=5), axis=0)
        elif mk == "BOVESPA":
            adj = adj.div(fx["BRL=X"].reindex(adj.index).ffill(limit=5), axis=0)
        Pm = adj.resample("ME").last()
        vol = (R.rolling(VENTANA_VOL, min_periods=50).std() * math.sqrt(252)).resample("ME").last()
        ok = Pm.notna() & vol.notna()
        ew = ok.astype(float).div(ok.sum(axis=1), axis=0)
        inv = (1 / vol).where(ok)
        iw = inv.div(inv.sum(axis=1), axis=0)
        inicio = ew.dropna(how="all").index[12]
        r_ew, rot_ew = simular_bandas(Pm, ew, inicio)
        r_iw, rot_iw = simular_bandas(Pm, iw, inicio)
        out[mk] = {"equiponderado": {**ra.metricas(r_ew), "rotacion": round(rot_ew, 3)},
                   "inversa_vol": {**ra.metricas(r_iw), "rotacion": round(rot_iw, 3)},
                   "veredicto": veredicto(r_iw, r_ew)}
    return out


def main(push: bool = True) -> dict:
    out = {
        "generated": datetime.now().isoformat(timespec="seconds"),
        "shadow": True,
        "parametros": {"ventana_vol": VENTANA_VOL, "target_cartera": TARGET_CARTERA,
                       "banda_pesos": BANDA_PESOS, "banda_exposicion": BANDA_EXPOSICION,
                       "costo_pata_pct": COSTO * 100},
        "indice": evaluar("indice"),
        "canasta": evaluar("canasta"),
        "tamanio_por_accion": tamanio_por_accion(),
        "advertencias": [
            "Liquidez rinde 0%: con tasa USD real el control de vol mejora algo más.",
            "Canasta y tamaño por acción: tickers actuales (sesgo de supervivencia).",
            "Un solo período histórico (2017-2026); ninguna regla se optimizó sobre él.",
        ],
    }
    try:
        from src.github_persistence import save_json
        save_json(OUT_PATH, out, message="research: capa de riesgo", push=push)
    except Exception:
        os.makedirs(ra.RESEARCH_DIR, exist_ok=True)
        with open(OUT_PATH, "w", encoding="utf-8") as f:
            json.dump(out, f, ensure_ascii=False, indent=2)
    return out


if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO)
    o = main(push="--no-push" not in sys.argv)
    for fuente in ("indice", "canasta"):
        e = o[fuente]
        print(f"\n== {fuente} {e['desde']} → {e['hasta']} ==")
        for mk, v in e["por_mercado"].items():
            a, b = v["sin_gestion"], v["control_vol_propio"]
            print(f"{mk:8s} sin: CAGR {a['cagr_pct']:5.1f} vol {a['vol_pct']:5.1f} Sh {a['sharpe']} DD {a['max_dd_pct']:6.1f} | "
                  f"ctrl: CAGR {b['cagr_pct']:5.1f} vol {b['vol_pct']:5.1f} Sh {b['sharpe']} DD {b['max_dd_pct']:6.1f} "
                  f"expo {b['exposicion_media']} rot {b['rotacion']} → {v['veredicto']}")
        for k, v in e["carteras"].items():
            print(f"{k:24s} CAGR {v['cagr_pct']:5.1f} vol {v['vol_pct']:5.1f} Sh {v['sharpe']} DD {v['max_dd_pct']:6.1f} "
                  f"rot {v['rotacion']} {v.get('veredicto', '')}")
    print("\n== tamaño por acción ==")
    for mk, v in o["tamanio_por_accion"].items():
        a, b = v["equiponderado"], v["inversa_vol"]
        print(f"{mk:8s} EW: CAGR {a['cagr_pct']} vol {a['vol_pct']} Sh {a['sharpe']} DD {a['max_dd_pct']} | "
              f"IV: CAGR {b['cagr_pct']} vol {b['vol_pct']} Sh {b['sharpe']} DD {b['max_dd_pct']} → {v['veredicto']}")
