#!/usr/bin/env python3
"""
scripts/diagnostico_ic.py — Motor de investigación POR MERCADO (SHADOW)
=======================================================================

SHADOW: no toca señales, signal_v2, Kelly, portfolio_optimizer ni
signals_history.json. Solo lee datos y escribe data/diagnostico_ic.json.

Principio: cada mercado es un caso distinto. Nada se calibra "global": cada
mercado elige sus propios factores y sus propios signos, usando solo su
propio pasado.

A) Ordenamiento de los scores ACTUALES del modelo (signals_archive_*.jsonl)
   IC de Spearman por mercado y por fecha entre cada score y el retorno a
   H ruedas, más el alpha del Top 5 contra la canasta equiponderada del mismo
   mercado. Resumido en bloques no superpuestos de H ruedas: si una señal no
   sostiene el signo bloque a bloque, no cuenta.

B) Motor de factores de precio, walk-forward, por mercado
   El archive arranca el 22/06 (≈3 bloques de 21d). Los CSV de cierres tienen
   ~13 meses (≈10 bloques). Los factores de precio se pueden medir sobre toda
   esa historia; macro y fundamentales no (no hay histórico point-in-time).
   En cada rebalanceo (cada H ruedas):
     1. Para cada factor se mira SOLO el IC ya realizado (fechas cuyo retorno
        a H ruedas terminó antes del rebalanceo): sin mirar el futuro.
     2. Un factor entra si su IC por bloque tiene |t| >= T_MIN, signo
        consistente en >= SHARE_MIN de los bloques y |IC| >= IC_MIN. Entra
        con SU signo en ESE mercado (un factor puede ser momentum en un
        mercado y reversión en otro).
     3. Score compuesto = promedio ponderado por IC de los rangos
        cross-section. Top N con buffer (se mantiene un nombre mientras siga
        dentro del Top BUFFER_N) para bajar rotación y costos.
     4. Se mide el período siguiente fuera de muestra, NETO de costos Balanz
        (misma función que backtester.py), contra la canasta equiponderada
        del mercado y contra el índice.
   Si ningún factor pasa el filtro, el motor no apuesta (queda en canasta):
   "si no está confirmado, no informa acción".

C) Picks de hoy del motor por mercado (shadow, solo informativo).

Uso:
    python -m scripts.diagnostico_ic            # trae CSV frescos de GitHub
    python -m scripts.diagnostico_ic --10y      # motor sobre data/research/ (10 años)
    DIAG_NO_PULL=1 python -m scripts.diagnostico_ic   # usa data/ local
    Telegram: /diagnostico_ic   ·   /diagnostico_ic 10y

Salida: data/diagnostico_ic.json (o data/diagnostico_ic_10y.json) + push a GitHub.

Métrica que decide (acordar ANTES de mirar resultados nuevos):
    alpha_neto medio por período del motor vs canasta, positivo en >= 70% de
    los períodos fuera de muestra y t >= 2, sostenido al sumar ventanas.
"""

from __future__ import annotations

import glob
import json
import logging
import math
import os
import sys
from datetime import datetime

import numpy as np
import pandas as pd

# Permite correr como `python scripts/diagnostico_ic.py` además de `-m`
_ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
if _ROOT not in sys.path:
    sys.path.insert(0, _ROOT)

logger = logging.getLogger(__name__)

# ── Parámetros (fijados a priori, NO optimizados sobre los resultados) ──────
H = 21                  # horizonte y frecuencia de rebalanceo (ruedas)
TOP_N = 5               # posiciones por mercado
BUFFER_N = 8            # un nombre en cartera se mantiene si sigue en el Top 8
MIN_TRAIN_BLOCKS = 4    # bloques de IC realizados mínimos para decidir
T_MIN = 1.0             # |t| mínimo del IC por bloque para usar un factor (< 20 bloques)
T_MIN_LARGO = 2.0       # con >= 20 bloques (dataset 10y) se exige t >= 2
BLOQUES_LARGO = 20
SHARE_MIN = 0.60        # % de bloques con el mismo signo que el promedio
IC_MIN = 0.03           # IC medio mínimo en valor absoluto
MIN_CS = 8              # mínimo de acciones con dato para calcular un IC
MAX_DAILY_MOVE = 0.45   # |ret diario| > 45% se trata como split / error de dato
TREND_MA = 100          # filtro de tendencia del índice (fijo, no optimizado)
OUT_PATH = "data/diagnostico_ic.json"
OUT_PATH_10Y = "data/diagnostico_ic_10y.json"
DATA_DIR = "data"
RESEARCH_DIR = "data/research"

MERCADOS = (("merval", "MERVAL"), ("bovespa", "BOVESPA"), ("sp500", "SP500"))

# Scores del modelo actual que se evalúan en la parte A
SCORES_MODELO = [
    "ranking", "score_v1", "score_v2", "confidence_score", "asset_quality",
    "entry_score", "score_tecnico", "score_fund", "score_sectorial",
    "rr_ratio", "pred_21d",
]


# ─────────────────────────────────────────────────────────────────────────────
# Costos (mismos que backtester.py)
# ─────────────────────────────────────────────────────────────────────────────
def _costo_pata_pct(mercado: str) -> float:
    try:
        from src.backtester import _costo_pata_pct as _c
        return float(_c(mercado))
    except Exception:
        return 0.605


# ─────────────────────────────────────────────────────────────────────────────
# Carga de datos
# ─────────────────────────────────────────────────────────────────────────────
def load_prices(pull: bool = True) -> dict:
    """Devuelve {MERCADO: (precios_df[fecha x ticker], indice_series)}.
    Mapea columnas por el diccionario oficial de src/downloader.py."""
    from src import downloader as dl

    dfs = None
    if pull:
        try:
            dfs = dl.reload_price_csvs_fresh(DATA_DIR)
        except Exception as e:
            logger.warning(f"[diagnostico_ic] reload_price_csvs_fresh falló: {e}")
    if dfs is None:
        dfs = {k: dl._load_csv(k, DATA_DIR) for k, _ in MERCADOS}

    mapas = {"MERVAL": dl.MERVAL_TICKERS, "BOVESPA": dl.BOVESPA_TICKERS,
             "SP500": dl.SP500_TICKERS}
    out = {}
    for key, mk in MERCADOS:
        df = dfs.get(key)
        if df is None or df.empty:
            logger.warning(f"[diagnostico_ic] sin precios para {mk}")
            continue
        nombre_a_ticker = {v: k for k, v in mapas[mk].items()}
        cols = [c for c in df.columns if c in nombre_a_ticker]
        idx_col = next((c for c in df.columns
                        if str(c).upper().replace("Í", "I").startswith("INDICE")), None)
        P = df[cols].rename(columns=nombre_a_ticker)
        I = df[idx_col] if idx_col else None
        out[mk] = (P, I)
    return out


def load_prices_research() -> dict:
    """Precios de 10 años de data/research/ (scripts/backfill_precios_10y.py).
    Columnas ya son tickers; el índice viene como columna INDICE. Si el archivo
    no está local (Railway recién redeployado) lo trae de GitHub."""
    out = {}
    for key, mk in MERCADOS:
        path = os.path.join(RESEARCH_DIR, f"{key}_10y.csv")
        if not os.path.exists(path):
            try:
                from src.github_persistence import pull_file
                pull_file(path)
            except Exception as e:
                logger.warning(f"[diagnostico_ic] no pude traer {path}: {e}")
        if not os.path.exists(path):
            logger.warning(f"[diagnostico_ic] falta {path} — correr /backfill_precios aplicar")
            continue
        df = pd.read_csv(path, sep=";", decimal=",", encoding="utf-8-sig", index_col=0)
        df.index = pd.to_datetime(df.index, errors="coerce")
        df = df[df.index.notna()].sort_index().apply(pd.to_numeric, errors="coerce")
        I = df["INDICE"] if "INDICE" in df.columns else None
        P = df.drop(columns=["INDICE"], errors="ignore")
        out[mk] = (P, I)
    return out


def load_archive() -> pd.DataFrame:
    """Lee data/signals_archive_YYYY-MM.jsonl (local; el pipeline los mantiene
    sincronizados). Si no hay locales, intenta traerlos de GitHub."""
    paths = sorted(glob.glob(os.path.join(DATA_DIR, "signals_archive_20*.jsonl")))
    textos = []
    for p in paths:
        with open(p, encoding="utf-8") as f:
            textos.append(f.read())
    if not textos:
        try:
            from src.signals_archive import fetch_remote_text
            hoy = datetime.now()
            for y, m in _meses_hacia_atras(hoy.year, hoy.month, 6):
                t = fetch_remote_text(f"{DATA_DIR}/signals_archive_{y:04d}-{m:02d}.jsonl")
                if t:
                    textos.append(t)
        except Exception as e:
            logger.warning(f"[diagnostico_ic] no pude traer el archive remoto: {e}")
    rows = []
    for t in textos:
        for line in t.splitlines():
            line = line.strip()
            if line:
                try:
                    rows.append(json.loads(line))
                except json.JSONDecodeError:
                    continue
    if not rows:
        return pd.DataFrame()
    df = pd.DataFrame(rows)
    df["fecha"] = pd.to_datetime(df["fecha"], errors="coerce")
    df = df.dropna(subset=["fecha"])
    df = df[df["fecha"].dt.dayofweek < 5]
    orden = "archivado_en" if "archivado_en" in df.columns else "fecha"
    df = df.sort_values(["fecha", orden]).drop_duplicates(["fecha", "ticker"], keep="last")
    return df


def _meses_hacia_atras(y, m, n):
    for _ in range(n):
        yield y, m
        m -= 1
        if m == 0:
            y, m = y - 1, 12


# ─────────────────────────────────────────────────────────────────────────────
# Limpieza y factores
# ─────────────────────────────────────────────────────────────────────────────
def clean_panel(P: pd.DataFrame):
    """Precio ajustado por saltos: un movimiento diario > MAX_DAILY_MOVE se
    toma como split / error de dato y se neutraliza (retorno 0 ese día)."""
    P = P.sort_index()
    P = P[P.index.dayofweek < 5]
    P = P.loc[:, P.notna().mean() >= 0.6]
    P = P.ffill(limit=3)
    R = P.pct_change(fill_method=None)
    bad = R.abs() > MAX_DAILY_MOVE
    n_bad = int(bad.sum().sum())
    R = R.mask(bad, 0.0)
    adj = (1 + R.fillna(0)).cumprod().where(P.notna())
    return adj, R.where(P.notna()), n_bad


def compute_factors(P: pd.DataFrame, R: pd.DataFrame, I: pd.Series | None) -> dict:
    F = {}
    F["rev5"] = -(P / P.shift(5) - 1)
    F["rev21"] = -(P / P.shift(21) - 1)
    F["mom63"] = P.shift(5) / P.shift(63) - 1
    F["mom126"] = P.shift(21) / P.shift(126) - 1
    hi = P.rolling(252, min_periods=63).max()
    lo = P.rolling(252, min_periods=63).min()
    F["dist_max"] = P / hi - 1
    F["dist_min"] = P / lo - 1
    ma50 = P.rolling(50, min_periods=40).mean()
    F["dist_ma50"] = P / ma50 - 1
    F["ma50_slope"] = ma50 / ma50.shift(10) - 1
    F["vol21"] = R.rolling(21, min_periods=15).std()
    F["vol63"] = R.rolling(63, min_periods=40).std()
    F["max_ret21"] = R.rolling(21, min_periods=15).max()
    d = P.diff()
    up = d.clip(lower=0).rolling(14, min_periods=10).mean()
    dn = (-d.clip(upper=0)).rolling(14, min_periods=10).mean()
    F["rsi14"] = 100 - 100 / (1 + up / dn.replace(0, np.nan))
    if I is not None:
        ri = I.reindex(P.index).ffill(limit=3).pct_change(fill_method=None)
        ri = ri.mask(ri.abs() > MAX_DAILY_MOVE, 0.0)
        var = ri.rolling(63, min_periods=40).var()
        beta = pd.DataFrame({c: R[c].rolling(63, min_periods=40).cov(ri) for c in R.columns})
        F["beta63"] = beta.div(var, axis=0)
    return F


# ─────────────────────────────────────────────────────────────────────────────
# IC y resúmenes por bloque
# ─────────────────────────────────────────────────────────────────────────────
def ic_series(X: pd.DataFrame, Y: pd.DataFrame, min_n: int = MIN_CS) -> pd.Series:
    """IC de Spearman cross-section, una observación por fecha (vectorizado:
    rangos promedio por fila sobre los pares con dato, igual que scipy)."""
    X, Y = X.align(Y, join="inner")
    X = X.apply(pd.to_numeric, errors="coerce")
    Y = Y.apply(pd.to_numeric, errors="coerce")
    m = X.notna() & Y.notna()
    Xr = X.where(m).rank(axis=1)
    Yr = Y.where(m).rank(axis=1)
    n = m.sum(axis=1)
    xc = Xr.sub(Xr.mean(axis=1), axis=0)
    yc = Yr.sub(Yr.mean(axis=1), axis=0)
    num = (xc * yc).sum(axis=1)
    den = np.sqrt((xc ** 2).sum(axis=1) * (yc ** 2).sum(axis=1))
    ok = (n >= min_n) & (den > 0) & (Xr.nunique(axis=1) >= 3) & (Yr.nunique(axis=1) >= 3)
    return (num[ok] / den[ok]).astype(float)


def _bloques(s: pd.Series, h: int = H):
    """Promedios por tramos contiguos de h fechas. Devuelve (completos, parcial)."""
    s = s.dropna()
    completos, parcial = [], None
    for i in range(0, len(s), h):
        tramo = s.iloc[i:i + h]
        if len(tramo) == h:
            completos.append(float(tramo.mean()))
        elif len(tramo) >= max(3, h // 3):
            parcial = float(tramo.mean())
    return completos, parcial


def _t_stat(vals):
    vals = [v for v in vals if v is not None and not math.isnan(v)]
    if len(vals) < 2:
        return None
    sd = float(np.std(vals, ddof=1))
    if sd == 0:
        return None
    return float(np.mean(vals) / sd * math.sqrt(len(vals)))


def resumen_ic(s: pd.Series, h: int = H) -> dict | None:
    s = s.dropna()
    if s.empty:
        return None
    bl, parcial = _bloques(s, h)
    media = float(s.mean())
    signo = np.sign(media)
    return {
        "ic_medio": round(media, 3),
        "pct_fechas_pos": round(float((s > 0).mean()), 2),
        "bloques": [round(b, 3) for b in bl],
        "bloque_parcial": round(parcial, 3) if parcial is not None else None,
        "n_bloques": len(bl),
        "pct_bloques_mismo_signo": round(float(np.mean([np.sign(b) == signo for b in bl])), 2) if bl else None,
        "t_bloques": round(_t_stat(bl), 2) if _t_stat(bl) is not None else None,
        "n_fechas": int(len(s)),
    }


# ─────────────────────────────────────────────────────────────────────────────
# A) Scores del modelo actual
# ─────────────────────────────────────────────────────────────────────────────
def parte_a(archive: pd.DataFrame, precios: dict) -> dict:
    res = {}
    if archive.empty:
        return {"error": "archive vacío"}
    for mk, (P_raw, _) in precios.items():
        P, _, _ = clean_panel(P_raw)
        fwd = P.shift(-H) / P - 1
        a = archive[archive["mercado"] == mk]
        if a.empty:
            continue
        out = {"scores": {}}
        for sc in SCORES_MODELO:
            if sc not in a.columns:
                continue
            X = a.pivot_table(index="fecha", columns="ticker", values=sc, aggfunc="last")
            X = X.apply(pd.to_numeric, errors="coerce")
            r = resumen_ic(ic_series(X, fwd))
            if r:
                out["scores"][sc] = r
        # Alpha de las COMPRA V2 emitidas vs canasta (mismo mercado y fecha)
        if "signal_v2" in a.columns:
            es_compra = a.assign(c=(a["signal_v2"].astype(str).str.contains("COMPRA")
                                  & ~a["signal_v2"].astype(str).str.contains("SIN CONFIRMAR")).astype(float))
            C = es_compra.pivot_table(index="fecha", columns="ticker", values="c", aggfunc="last")
            C, F_ = C.align(fwd, join="inner")
            canasta = F_.mean(axis=1)
            alpha = {}
            for d in C.index:
                sel = C.loc[d] == 1.0
                v = F_.loc[d][sel].dropna()
                if len(v) and not math.isnan(canasta.loc[d]):
                    alpha[d] = float((v.mean() - canasta.loc[d]) * 100)
            al = pd.Series(alpha, dtype=float)
            if not al.empty:
                bl, parcial = _bloques(al)
                out["compra_v2_alpha_bruto_pp"] = {
                    "medio": round(float(al.mean()), 2),
                    "pct_fechas_pos": round(float((al > 0).mean()), 2),
                    "bloques": [round(b, 2) for b in bl],
                    "bloque_parcial": round(parcial, 2) if parcial is not None else None,
                }
        res[mk] = out
    return res


# ─────────────────────────────────────────────────────────────────────────────
# B) Motor walk-forward por mercado
# ─────────────────────────────────────────────────────────────────────────────
def _rank_centrado(df_row: pd.Series) -> pd.Series:
    r = df_row.rank(pct=True)
    return r - 0.5


def _elegir_factores(ic_dict: dict, pos_de: dict, k: int, max_bloques: int | None):
    """Pesos por factor usando SOLO IC realizados (fecha <= k - H)."""
    pesos, detalle = {}, {}
    for f, s in ic_dict.items():
        s = s[[pos_de[d] <= k - H for d in s.index]] if len(s) else s
        bl, _ = _bloques(s)
        if max_bloques:
            bl = bl[-max_bloques:]
        if len(bl) < MIN_TRAIN_BLOCKS:
            continue
        m = float(np.mean(bl))
        t = _t_stat(bl)
        share = float(np.mean([np.sign(b) == np.sign(m) for b in bl]))
        t_min = T_MIN_LARGO if len(bl) >= BLOQUES_LARGO else T_MIN
        if t is not None and abs(t) >= t_min and share >= SHARE_MIN and abs(m) >= IC_MIN:
            pesos[f] = m
            detalle[f] = {"ic": round(m, 3), "t": round(t, 2), "bloques": len(bl)}
    return pesos, detalle


def _composite(F: dict, pesos: dict, d) -> pd.Series | None:
    if not pesos:
        return None
    tot, acc = 0.0, None
    for f, w in pesos.items():
        row = F[f].loc[d]
        if row.notna().sum() < MIN_CS:
            continue
        rc = _rank_centrado(row).fillna(0.0) * w
        acc = rc if acc is None else acc.add(rc, fill_value=0.0)
        tot += abs(w)
    if acc is None or tot == 0:
        return None
    return acc / tot


def _seleccion(score: pd.Series, disponibles: pd.Index, actuales: list) -> list:
    score = score.reindex(disponibles).dropna().sort_values(ascending=False)
    rank = {t: i + 1 for i, t in enumerate(score.index)}
    keep = [t for t in actuales if rank.get(t, 10**9) <= BUFFER_N]
    for t in score.index:
        if len(keep) >= TOP_N:
            break
        if t not in keep:
            keep.append(t)
    return keep[:TOP_N]


def motor_mercado(mk: str, P_raw: pd.DataFrame, I_raw: pd.Series | None,
                  max_bloques: int | None = None, filtro_tendencia: bool = False) -> dict:
    P, R, n_bad = clean_panel(P_raw)
    I = None
    if I_raw is not None:
        I = I_raw.reindex(P.index).ffill(limit=3)
    F = compute_factors(P, R, I)
    fwd = P.shift(-H) / P - 1
    ic_dict = {f: ic_series(X, fwd) for f, X in F.items()}
    fechas = list(P.index)
    pos_de = {d: i for i, d in enumerate(fechas)}
    c = _costo_pata_pct(mk) / 100

    # primer rebalanceo: cuando haya MIN_TRAIN_BLOCKS bloques de IC realizados
    ic_fechas = sorted(set().union(*[set(s.index) for s in ic_dict.values()]))
    if len(ic_fechas) < MIN_TRAIN_BLOCKS * H:
        return {"error": "historia insuficiente", "n_fechas_ic": len(ic_fechas)}
    k0 = pos_de[ic_fechas[MIN_TRAIN_BLOCKS * H - 1]] + H

    periodos, actuales = [], []
    k = k0
    while k + H <= len(fechas) - 1:
        d_in, d_out = fechas[k], fechas[k + H]
        pesos, detalle = _elegir_factores(ic_dict, pos_de, k, max_bloques)
        disponibles = P.columns[P.loc[d_in].notna() & P.loc[d_out].notna()]
        ret_all = (P.loc[d_out, disponibles] / P.loc[d_in, disponibles] - 1)
        canasta = float(ret_all.mean())
        idx_ret = None
        if I is not None and pd.notna(I.loc[d_in]) and pd.notna(I.loc[d_out]):
            idx_ret = float(I.loc[d_out] / I.loc[d_in] - 1)
        en_tendencia = True
        if filtro_tendencia and I is not None:
            ma = I.rolling(TREND_MA, min_periods=int(TREND_MA * 0.8)).mean().loc[d_in]
            en_tendencia = bool(pd.notna(ma) and I.loc[d_in] >= ma)

        score = _composite(F, pesos, d_in)
        if score is None:
            picks = []  # sin factor confirmado: no apuesta, queda en canasta
        else:
            picks = _seleccion(score, disponibles, actuales)
        if picks:
            nuevos = [t for t in picks if t not in actuales]
            q = len(nuevos) / TOP_N
            bruto = float(ret_all[picks].mean())
            neto = (1 + bruto) * (1 - c) ** q / (1 + c) ** q - 1  # compra+venta de lo rotado
        else:
            q, bruto, neto = 0.0, canasta, canasta
        if filtro_tendencia and not en_tendencia:
            neto_final = 0.0  # a liquidez; ignora el costo de salir (optimista, es solo informativo)
        else:
            neto_final = neto
        periodos.append({
            "entrada": str(d_in.date()), "salida": str(d_out.date()),
            "picks": picks, "factores": detalle,
            "rotacion": round(q, 2),
            "bruto_pct": round(bruto * 100, 2),
            "neto_pct": round(neto_final * 100, 2),
            "canasta_pct": round(canasta * 100, 2),
            "indice_pct": round(idx_ret * 100, 2) if idx_ret is not None else None,
            "alpha_neto_pp": round((neto_final - canasta) * 100, 2),
            "invertido": en_tendencia,
            "apuesta": bool(picks),
        })
        actuales = picks
        k += H

    # picks de hoy (último dato), sin evaluación
    k_hoy = len(fechas) - 1
    pesos_hoy, detalle_hoy = _elegir_factores(ic_dict, pos_de, k_hoy, max_bloques)
    score_hoy = _composite(F, pesos_hoy, fechas[k_hoy])
    picks_hoy = []
    if score_hoy is not None:
        disp = P.columns[P.loc[fechas[k_hoy]].notna()]
        picks_hoy = _seleccion(score_hoy, disp, actuales)

    res = {
        "n_saltos_neutralizados": n_bad,
        "universo": int(P.shape[1]),
        "ic_factores_muestra_completa": {f: resumen_ic(s) for f, s in ic_dict.items()},
        "periodos": periodos,
        "resumen_oos": _resumen_periodos(periodos),
        "hoy": {
            "fecha": str(fechas[k_hoy].date()),
            "factores": detalle_hoy,
            "picks": picks_hoy,
            "scores": {t: round(float(score_hoy[t]), 3) for t in picks_hoy} if picks_hoy else {},
        },
    }
    return res


def _resumen_periodos(periodos: list) -> dict:
    if not periodos:
        return {"n_periodos": 0}
    al = [p["alpha_neto_pp"] for p in periodos]
    apuesta = [p for p in periodos if p["apuesta"]]
    comp = lambda key: round((np.prod([1 + (p[key] or 0) / 100 for p in periodos]) - 1) * 100, 2)
    return {
        "n_periodos": len(periodos),
        "n_con_apuesta": len(apuesta),
        "alpha_neto_medio_pp": round(float(np.mean(al)), 2),
        "pct_periodos_alpha_pos": round(float(np.mean([a > 0 for a in al])), 2),
        "t_alpha": round(_t_stat(al), 2) if _t_stat(al) is not None else None,
        "acumulado_motor_neto_pct": comp("neto_pct"),
        "acumulado_canasta_pct": comp("canasta_pct"),
        "acumulado_indice_pct": comp("indice_pct") if all(p["indice_pct"] is not None for p in periodos) else None,
        "rotacion_media": round(float(np.mean([p["rotacion"] for p in periodos])), 2),
        "desde": periodos[0]["entrada"], "hasta": periodos[-1]["salida"],
        "por_anio": _por_anio(periodos),
    }


def _por_anio(periodos: list) -> dict:
    """Alpha neto medio y % positivo por año de entrada: muestra si el efecto
    sobrevive regímenes distintos o vive de un solo año."""
    out = {}
    for p in periodos:
        out.setdefault(p["entrada"][:4], []).append(p["alpha_neto_pp"])
    return {a: {"n": len(v), "alpha_medio_pp": round(float(np.mean(v)), 2),
                "pct_pos": round(float(np.mean([x > 0 for x in v])), 2)}
            for a, v in sorted(out.items())}


# ─────────────────────────────────────────────────────────────────────────────
# Main
# ─────────────────────────────────────────────────────────────────────────────
def main(push: bool = True, fuente: str = "csv") -> dict:
    """fuente="csv": CSV de producción (~13 meses) — parte A + motor.
    fuente="10y": dataset de investigación de 10 años — motor sobre 10 años;
    la parte A (scores del modelo) sigue usando los CSV de producción porque
    el archive recién empieza el 22/06/2026."""
    pull = os.environ.get("DIAG_NO_PULL") != "1"
    precios_prod = load_prices(pull=pull)
    precios_motor = load_prices_research() if fuente == "10y" else precios_prod
    archive = load_archive()
    out_path = OUT_PATH_10Y if fuente == "10y" else OUT_PATH

    resultado = {
        "generated": datetime.now().isoformat(timespec="seconds"),
        "shadow": True,
        "fuente_motor": fuente,
        "parametros": {
            "H": H, "TOP_N": TOP_N, "BUFFER_N": BUFFER_N,
            "MIN_TRAIN_BLOCKS": MIN_TRAIN_BLOCKS, "T_MIN": T_MIN,
            "T_MIN_LARGO": T_MIN_LARGO, "BLOQUES_LARGO": BLOQUES_LARGO,
            "SHARE_MIN": SHARE_MIN, "IC_MIN": IC_MIN, "TREND_MA": TREND_MA,
            "costo_pata_pct": {mk: _costo_pata_pct(mk) for _, mk in MERCADOS},
        },
        "advertencias": [
            "Universo = tickers actuales (sesgo de supervivencia).",
            "MERVAL en ARS: el alpha vs canasta es invariante a la moneda, "
            "los acumulados absolutos NO son retornos en USD.",
            "Factores de precio solamente: macro y fundamentales no tienen "
            "histórico point-in-time para testearse igual.",
        ],
        "A_scores_modelo": parte_a(archive, precios_prod),
        "B_motor": {},
    }
    if fuente == "10y" and not precios_motor:
        resultado["B_motor"] = {"error": "sin dataset 10y — correr /backfill_precios aplicar"}
    for mk, (P, I) in precios_motor.items():
        try:
            resultado["B_motor"][mk] = {
                "expandido": motor_mercado(mk, P, I),
                "ventana_6_bloques": motor_mercado(mk, P, I, max_bloques=6),
                "expandido_con_filtro_tendencia": motor_mercado(mk, P, I, filtro_tendencia=True),
            }
        except Exception as e:
            logger.exception(f"[diagnostico_ic] motor {mk} falló")
            resultado["B_motor"][mk] = {"error": str(e)[:300]}

    resultado["telegram_lines"] = _telegram_lines(resultado)

    try:
        from src.github_persistence import save_json
        save_json(out_path, resultado, message=f"auto: diagnostico_ic {fuente} (shadow)", push=push)
    except Exception as e:
        logger.warning(f"[diagnostico_ic] no se pudo guardar {out_path}: {e}")
        os.makedirs(os.path.dirname(out_path), exist_ok=True)
        with open(out_path, "w", encoding="utf-8") as f:
            json.dump(resultado, f, ensure_ascii=False, indent=2, default=str)
    return resultado


def _telegram_lines(res: dict) -> list:
    L = [f"<b>🔬 Diagnóstico IC + motor por mercado (shadow · {res.get('fuente_motor', 'csv')})</b>"]
    motor = res.get("B_motor", {})
    if "error" in motor:
        L.append(f"❌ {motor['error']}")
        motor = {}
    for mk, d in motor.items():
        e = d.get("expandido", {}) if isinstance(d, dict) else {}
        r = e.get("resumen_oos", {})
        if not r or not r.get("n_periodos"):
            L.append(f"\n<b>{mk}</b>: sin períodos evaluables")
            continue
        hoy = e.get("hoy", {})
        facts = ", ".join(f"{f}{'+' if v['ic'] > 0 else '−'}" for f, v in hoy.get("factores", {}).items()) or "ninguno confirmado"
        L.append(
            f"\n<b>{mk}</b> ({r['n_periodos']} períodos fuera de muestra)\n"
            f"Alpha neto medio: {r['alpha_neto_medio_pp']:+.2f} pp · positivo {r['pct_periodos_alpha_pos']:.0%} · t={r['t_alpha']}\n"
            f"Acumulado motor {r['acumulado_motor_neto_pct']:+.1f}% vs canasta {r['acumulado_canasta_pct']:+.1f}%\n"
            f"Factores hoy: {facts}\n"
            f"Picks hoy: {', '.join(hoy.get('picks', [])) or '— (no apuesta)'}"
        )
        pa = r.get("por_anio", {})
        if len(pa) > 1:
            L.append("Por año: " + " · ".join(f"{a} {v['alpha_medio_pp']:+.1f}pp ({v['pct_pos']:.0%})" for a, v in pa.items()))
        a = res.get("A_scores_modelo", {}).get(mk, {}).get("scores", {})
        if a:
            top = sorted(((k, v["ic_medio"]) for k, v in a.items()), key=lambda x: -x[1])
            L.append("Scores modelo IC: " + " · ".join(f"{k} {v:+.2f}" for k, v in top[:3])
                     + " … " + " · ".join(f"{k} {v:+.2f}" for k, v in top[-2:]))
    return L


if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO, format="%(levelname)s %(message)s")
    out = main(push=os.environ.get("DIAG_NO_PUSH") != "1",
               fuente="10y" if "--10y" in sys.argv else "csv")
    print("\n".join(out["telegram_lines"]).replace("<b>", "").replace("</b>", ""))
