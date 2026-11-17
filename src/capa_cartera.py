"""
src/capa_cartera.py — Capa de cartera por riesgo (camino A) — SHADOW
====================================================================

Aprobado 28/09/2026. SHADOW: no toca señales, signal_v2, Kelly,
portfolio_optimizer ni portfolio.json. Solo lee y reporta.

Evidencia (scripts/research_riesgo.py, 10 años en USD, costos Balanz):
  - Pesos entre mercados por inversa de volatilidad 63d con banda de 5 pp:
    única regla que mejoró a los tercios (Sharpe 0,33 → 0,41, caída máxima
    −52% → −44%, mejor en las dos mitades de la muestra).
  - Bandas de rebalanceo: −40% de rotación sin costo.

Qué calcula en cada corrida:
  1. Volatilidad 63d anualizada de cada mercado en USD
     (MERVAL / CCL, BOVESPA / BRL, S&P), desde data/research/ (backfill).
  2. Pesos objetivo por mercado ∝ 1 / volatilidad.
  3. Pesos actuales de la cartera real por mercado (portfolio.json, solo lectura).
  4. ¿Rebalancear? Solo si algún mercado se desvía más de BANDA del objetivo;
     en ese caso sugiere montos en USD por mercado.
  5. Seguimiento en vivo desde INICIO_SEGUIMIENTO: cartera por inversa de vol
     (con banda) vs tercios vs 100% S&P, con costos, rebalanceo mensual.

Salida: data/capa_cartera.json (+ push). Aviso por Telegram como máximo una
vez por día y solo cuando la cartera real sale de banda.
"""

from __future__ import annotations

import logging
import math
import os
from datetime import datetime

import numpy as np
import pandas as pd

logger = logging.getLogger(__name__)

RESEARCH_DIR = "data/research"
OUT_PATH = "data/capa_cartera.json"
PORTFOLIO_PATH = "data/portfolio.json"
MERCADOS = ["MERVAL", "BOVESPA", "SP500"]
VENTANA_VOL = 63
BANDA = 0.05
COSTO_PATA = 0.00605
SALTO = 0.45
DATOS_VIEJOS_DIAS = 4
INICIO_SEGUIMIENTO_DEFAULT = "2026-09-30"
_FUENTE_SERIE: dict = {}        # {mercado: "indice" | "canasta_ew"} de la última corrida


# ─────────────────────────────────────────────────────────────────────────────
# Datos
# ─────────────────────────────────────────────────────────────────────────────
def _leer_csv(nombre: str, pull: bool) -> pd.DataFrame:
    path = os.path.join(RESEARCH_DIR, nombre)
    if pull or not os.path.exists(path):
        try:
            from src.github_persistence import pull_file
            pull_file(path)
        except Exception as e:
            logger.warning(f"[capa_cartera] pull {path} falló: {e}")
    df = pd.read_csv(path, sep=";", decimal=",", encoding="utf-8-sig", index_col=0)
    df.index = pd.to_datetime(df.index, errors="coerce")
    return df[df.index.notna()].sort_index().apply(pd.to_numeric, errors="coerce")


COBERTURA_MIN_INDICE = 0.90     # proporción mínima de ruedas con dato del índice
VENTANA_COBERTURA_INDICE = 252  # se mide sobre el último año: la serie real del índice
                                # (Rava) empieza en 2021-10, no cubre los 10 años
ATRASO_MAX_INDICE = 5           # ruedas que el índice puede quedar detrás de las acciones


def canasta_ew(df: pd.DataFrame) -> pd.Series:
    """Índice base 100 de la canasta equiponderada de las acciones del CSV
    (retorno diario promedio, saltos > SALTO neutralizados)."""
    acciones = df.drop(columns=["INDICE"], errors="ignore")
    r = acciones.pct_change(fill_method=None)
    r = r.where(r.abs() <= SALTO)
    minimo = max(1, acciones.shape[1] // 2)
    rb = r.mean(axis=1).where(r.notna().sum(axis=1) >= minimo)
    rb = rb.dropna()
    return (1 + rb).cumprod() * 100


def indice_o_canasta(df: pd.DataFrame) -> tuple[pd.Series, str]:
    """
    FIX 11/11/2026: el índice MERVAL de Yahoo (^MERV) quedó inutilizable —
    merval_10y.csv tenía 64 valores de INDICE en 2.437 ruedas y el último era
    del 16/10/2026. Con ffill(limit=5) la capa quedaba clavada en el 23/10
    ("datos_viejos") y medía la volatilidad de MERVAL sobre una serie rota.

    La solución de fondo es la fuente real del índice (src/indice_merval.py,
    que llena INDICE en el backfill). Esto es solo la red de seguridad: si el
    índice del CSV tiene buena cobertura en el último año y está al día, se
    usa. Si no, se usa la canasta equiponderada de las acciones del mismo CSV
    y se avisa en el reporte. No se empalman las dos series: es una u otra.
    """
    idx = df["INDICE"] if "INDICE" in df.columns else pd.Series(dtype=float)
    acciones = df.drop(columns=["INDICE"], errors="ignore").dropna(how="all")
    if acciones.empty:
        return idx, "indice"
    recientes = idx.reindex(acciones.index[-VENTANA_COBERTURA_INDICE:])
    cobertura = float(recientes.notna().mean()) if len(recientes) else 0.0
    ultimo = idx.last_valid_index()
    atraso = int((acciones.index > ultimo).sum()) if ultimo is not None else len(acciones)
    if cobertura >= COBERTURA_MIN_INDICE and atraso <= ATRASO_MAX_INDICE:
        return idx, "indice"
    logger.warning(f"[capa_cartera] índice no utilizable (cobertura {cobertura:.0%}, "
                   f"{atraso} ruedas de atraso) → canasta equiponderada")
    return canasta_ew(df), "canasta_ew"


def usd_diario(pull: bool = True) -> pd.DataFrame:
    fx = _leer_csv("fx_10y.csv", pull)
    idx, fuente = {}, {}
    for key, mk in (("merval", "MERVAL"), ("bovespa", "BOVESPA"), ("sp500", "SP500")):
        idx[mk], fuente[mk] = indice_o_canasta(_leer_csv(f"{key}_10y.csv", pull))
    _FUENTE_SERIE.clear()
    _FUENTE_SERIE.update(fuente)
    d = pd.DataFrame(idx)
    d = d[d.index.dayofweek < 5].ffill(limit=5)
    ccl = fx["CCL"].reindex(d.index).ffill(limit=5)
    brl = fx["BRL=X"].reindex(d.index).ffill(limit=5)
    return pd.DataFrame({"MERVAL": d["MERVAL"] / ccl, "BOVESPA": d["BOVESPA"] / brl,
                         "SP500": d["SP500"]}).dropna()


def retornos(daily: pd.DataFrame) -> pd.DataFrame:
    r = daily.pct_change(fill_method=None)
    return r.mask(r.abs() > SALTO, 0.0)


def vol_anual(daily: pd.DataFrame) -> pd.DataFrame:
    return retornos(daily).rolling(VENTANA_VOL, min_periods=int(VENTANA_VOL * 0.8)).std() * math.sqrt(252)


def pesos_inversa_vol(vol_row: pd.Series) -> pd.Series:
    inv = 1 / vol_row
    return inv / inv.sum()


def pesos_cartera_real(path: str = PORTFOLIO_PATH) -> dict:
    try:
        from src.github_persistence import load_json
        pf = load_json(path, default={}) or {}
    except Exception:
        pf = {}
    valor = {mk: 0.0 for mk in MERCADOS}
    for p in pf.get("positions", []) or []:
        mk = str(p.get("mercado", "")).upper().replace("S&P 500", "SP500").replace("S&P500", "SP500")
        v = p.get("valor_actual_usd")
        if mk in valor and isinstance(v, (int, float)) and v > 0:
            valor[mk] += float(v)
    total = sum(valor.values())
    return {"valor_usd": {k: round(v, 2) for k, v in valor.items()},
            "total_usd": round(total, 2),
            "pesos": {k: (round(v / total, 4) if total > 0 else None) for k, v in valor.items()}}


# ─────────────────────────────────────────────────────────────────────────────
# Seguimiento en vivo (diario, rebalanceo al primer día hábil de cada mes)
# ─────────────────────────────────────────────────────────────────────────────
def seguir(daily: pd.DataFrame, inicio: str, regla: str) -> dict:
    """NAV diario desde `inicio` (base 100) con costos. regla:
    "inversa_vol" (banda BANDA), "tercios" (mensual) o "sp500"."""
    r = retornos(daily)
    vol = vol_anual(daily)
    fechas = r.index[r.index > pd.Timestamp(inicio)]
    if len(fechas) == 0:
        return {"nav": 100.0, "dias": 0, "costos_pct": 0.0}
    w = pd.Series(0.0, index=daily.columns)
    nav, costos, mes_prev = 100.0, 0.0, None
    for d in fechas:
        prev = r.index[r.index.get_loc(d) - 1]
        if mes_prev is None or d.month != mes_prev:
            if regla == "sp500":
                obj = pd.Series({"MERVAL": 0.0, "BOVESPA": 0.0, "SP500": 1.0})
            elif regla == "tercios":
                obj = pd.Series(1 / 3, index=daily.columns)
            else:
                obj = pesos_inversa_vol(vol.loc[prev]) if vol.loc[prev].notna().all() else pd.Series(1 / 3, index=daily.columns)
            primera = mes_prev is None
            dev = float((obj - w).abs().max())
            banda = BANDA if regla == "inversa_vol" else 0.0
            if primera or dev > banda:
                t = float((obj - w).abs().sum())
                c = COSTO_PATA * t
                nav *= (1 - c)
                costos += c * 100
                w = obj.copy()
            mes_prev = d.month
        rr = r.loc[d].fillna(0.0)
        g = float((w * rr).sum())
        nav *= (1 + g)
        v = w * (1 + rr)
        tot = float(v.sum()) + (1 - float(w.sum()))
        w = v / tot if tot > 0 else w
    return {"nav": round(nav, 2), "dias": int(len(fechas)), "costos_pct": round(costos, 2),
            "pesos_actuales": {k: round(float(x), 3) for k, x in w.items()}}


# ─────────────────────────────────────────────────────────────────────────────
# Run
# ─────────────────────────────────────────────────────────────────────────────
def run_capa_cartera(push: bool = True, pull: bool = True, ahora: datetime | None = None) -> dict:
    ahora = ahora or datetime.now()
    try:
        from src.github_persistence import load_json, pull_file
        if not os.path.exists(OUT_PATH) and pull:
            pull_file(OUT_PATH)
        previo = load_json(OUT_PATH, default={}) or {}
    except Exception:
        previo = {}

    daily = usd_diario(pull=pull)
    vol = vol_anual(daily)
    fecha_datos = daily.index[-1]
    vol_hoy = vol.iloc[-1]
    obj = pesos_inversa_vol(vol_hoy)
    real = pesos_cartera_real()

    desvios, rebalancear, montos = {}, False, {}
    if real["total_usd"] > 0:
        for mk in MERCADOS:
            dv = (real["pesos"][mk] or 0.0) - float(obj[mk])
            desvios[mk] = round(dv * 100, 1)
        rebalancear = max(abs(v) for v in desvios.values()) > BANDA * 100
        if rebalancear:
            for mk in MERCADOS:
                montos[mk] = round(float(obj[mk]) * real["total_usd"] - real["valor_usd"][mk], 0)

    dias_viejos = (pd.Timestamp(ahora.date()) - fecha_datos.normalize()).days
    inicio = previo.get("inicio_seguimiento") or INICIO_SEGUIMIENTO_DEFAULT
    seguimiento = {
        "inicio": inicio,
        "inversa_vol_banda": seguir(daily, inicio, "inversa_vol"),
        "tercios": seguir(daily, inicio, "tercios"),
        "sp500": seguir(daily, inicio, "sp500"),
    }

    hoy_str = ahora.strftime("%Y-%m-%d")
    avisar = bool(rebalancear and previo.get("ultimo_aviso") != hoy_str)
    out = {
        "generated": ahora.isoformat(timespec="seconds"),
        "shadow": True,
        "fecha_datos": str(fecha_datos.date()),
        "datos_viejos": dias_viejos > DATOS_VIEJOS_DIAS,
        "fuente_serie": {mk: _FUENTE_SERIE.get(mk, "indice") for mk in MERCADOS},
        "vol_63d_pct": {mk: round(float(vol_hoy[mk]) * 100, 1) for mk in MERCADOS},
        "pesos_objetivo": {mk: round(float(obj[mk]), 4) for mk in MERCADOS},
        "cartera_real": real,
        "desvio_pp": desvios,
        "banda_pp": BANDA * 100,
        "rebalancear": rebalancear,
        "montos_sugeridos_usd": montos,
        "inicio_seguimiento": inicio,
        "seguimiento": seguimiento,
        "ultimo_aviso": hoy_str if avisar else previo.get("ultimo_aviso"),
        "avisar_ahora": avisar,
    }
    out["telegram_lines"] = telegram_lines(out)
    try:
        from src.github_persistence import save_json
        save_json(OUT_PATH, out, message="auto: capa_cartera (shadow)", push=push)
    except Exception as e:
        logger.warning(f"[capa_cartera] no se pudo guardar: {e}")
    return out


def telegram_lines(o: dict) -> list:
    nom = {"MERVAL": "🇦🇷 MERVAL", "BOVESPA": "🇧🇷 BOVESPA", "SP500": "🇺🇸 S&amp;P"}
    L = [f"<b>⚖️ Capa de cartera por riesgo (shadow)</b> — datos al {o['fecha_datos']}"]
    if o["datos_viejos"]:
        L.append("⚠️ Datos de research con más de 4 días: revisar el workflow Backfill research 10y")
    _canasta = [mk for mk, f in (o.get("fuente_serie") or {}).items() if f == "canasta_ew"]
    if _canasta:
        L.append(f"ℹ️ {', '.join(_canasta)}: sin índice utilizable, se usa la canasta equiponderada de sus acciones")
    for mk in MERCADOS:
        real = o["cartera_real"]["pesos"].get(mk)
        L.append(f"{nom[mk]}: objetivo {o['pesos_objetivo'][mk]:.0%} · real "
                 f"{(f'{real:.0%}' if real is not None else '—')} · vol {o['vol_63d_pct'][mk]}%")
    if o["cartera_real"]["total_usd"] <= 0:
        L.append("Cartera real vacía: sin comparación.")
    elif o["rebalancear"]:
        L.append(f"🔁 <b>Fuera de banda (±{o['banda_pp']:.0f} pp)</b>. Montos sugeridos (USD):")
        for mk, v in o["montos_sugeridos_usd"].items():
            if abs(v) >= 1:
                L.append(f"  {nom[mk]}: {'comprar' if v > 0 else 'vender'} {abs(v):,.0f}")
        L.append("Costo estimado de rebalancear: "
                 f"{sum(abs(v) for v in o['montos_sugeridos_usd'].values()) * COSTO_PATA:,.0f} USD")
    else:
        L.append(f"✅ Dentro de banda (±{o['banda_pp']:.0f} pp): no rebalancear.")
    s = o["seguimiento"]
    if s["inversa_vol_banda"]["dias"] > 0:
        L.append(f"Seguimiento desde {s['inicio']} (base 100): inversa vol {s['inversa_vol_banda']['nav']} · "
                 f"tercios {s['tercios']['nav']} · 100% S&amp;P {s['sp500']['nav']}")
    L.append("<i>Análisis de datos, no asesoramiento. No toca señales ni Kelly.</i>")
    return L
