"""
scripts/diagnostico_earnings_blackout.py

Evaluación del shadow de blackout pre-earnings (src/earnings_calendar.py).

Dos lecturas, SIEMPRE segmentadas por mercado (lección Simpson, v21.2/09-09):

  A) RETROACTIVA, universo completo, ~13 meses de precios (data/*_cierres.csv)
     con las fechas de resultados acumuladas en data/earnings_calendar.json.
     Caveat explícito: usa la fecha REAL publicada, no la estimada que se
     conocía en cada día (en la práctica la fecha se anuncia semanas antes,
     así que el sesgo es chico, pero existe). Por eso esta lectura informa,
     y la decisión final se valida con B.

  B) SEÑALES REALES (data/signals_history.json), COMPRA/COMPRA FUERTE, con
     los campos earnings_* grabados point-in-time desde el 25/09/2026.
     Mientras no haya ret_21d materializado para esas fechas, B cae a
     recalcular days_to desde el calendario (mismo caveat que A).

Métricas: exceso a 21d contra el universo equiponderado del mismo mercado
(alpha, no retorno bruto -- separa mercado de selección). Umbral primario
pre-registrado: PRE_BLACKOUT_BDAYS (10 ruedas). Controles de
pseudoreplicación: clustering por ticker (Wilcoxon sobre diferencias por
ticker) y 21 offsets no solapados. Se reporta también el percentil 5
(la hipótesis es de riesgo de cola, no solo de media).

Materialidad: diferencias < 0.5pp se tratan como ruido (learnings.md).

Uso:  python -m scripts.diagnostico_earnings_blackout
"""

from __future__ import annotations

import json
import os
import sys
import unicodedata

import numpy as np
import pandas as pd

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from src.earnings_calendar import PRE_BLACKOUT_BDAYS, load_calendar  # noqa: E402

DATA = "data"
MARKETS = {"MERVAL": "merval_cierres.csv", "BOVESPA": "bovespa_cierres.csv",
           "SP500": "sp500_cierres.csv"}
MATERIALIDAD_PP = 0.5


def _norm(s: str) -> str:
    s = unicodedata.normalize("NFKD", str(s)).encode("ascii", "ignore").decode()
    return "".join(ch for ch in s.lower() if ch.isalnum())


def _ticker_names() -> dict:
    from src import downloader
    out = {}
    for mk, attr in [("MERVAL", "MERVAL_TICKERS"), ("BOVESPA", "BOVESPA_TICKERS"),
                     ("SP500", "SP500_TICKERS")]:
        out[mk] = dict(getattr(downloader, attr, {}))
    return out


def load_prices(data_dir: str = DATA) -> dict:
    px = {}
    for mk, f in MARKETS.items():
        p = os.path.join(data_dir, f)
        if not os.path.exists(p):
            continue
        df = pd.read_csv(p, sep=";", decimal=",", index_col=0,
                         encoding="utf-8-sig", thousands=" ")
        df.index = pd.to_datetime(df.index, dayfirst=False, errors="coerce")
        df = df[df.index.notna()].sort_index()
        df = df.apply(pd.to_numeric, errors="coerce")
        px[mk] = df
    return px


def map_columns(px: dict, names: dict) -> dict:
    """ticker -> (mercado, columna) por nombre normalizado (sin tildes)."""
    out = {}
    for mk, df in px.items():
        cols = {_norm(c): c for c in df.columns if "indice" not in _norm(c)}
        for tk, nm in names.get(mk, {}).items():
            c = cols.get(_norm(nm))
            if c:
                out[tk] = (mk, c)
    return out


def days_to_next(idx: pd.DatetimeIndex, earnings: list[str]) -> np.ndarray:
    """Ruedas hábiles desde cada fecha de idx al próximo resultado (NaN si no hay)."""
    if not earnings:
        return np.full(len(idx), np.nan)
    e = np.array(sorted(earnings), dtype="datetime64[D]")
    d = idx.values.astype("datetime64[D]")
    pos = np.searchsorted(e, d, side="left")
    out = np.full(len(d), np.nan)
    ok = pos < len(e)
    out[ok] = np.busday_count(d[ok], e[pos[ok]])
    return out


def build_panel(px: dict, colmap: dict, cal: dict, horizon: int = 21) -> pd.DataFrame:
    rows = []
    for mk, df in px.items():
        tks = [t for t, (m, _) in colmap.items() if m == mk]
        if not tks:
            continue
        P = df[[colmap[t][1] for t in tks]].ffill(limit=3)
        P.columns = tks
        f = P.shift(-horizon) / P - 1
        ex = f.sub(f.mean(axis=1), axis=0)
        for t in tks:
            dates = cal.get("tickers", {}).get(t, {}).get("dates", [])
            if not dates:
                continue
            dt = days_to_next(P.index, dates)
            part = pd.DataFrame({"d": P.index, "tk": t, "m": mk,
                                 "ret": f[t].values * 100, "ex": ex[t].values * 100,
                                 "days_to": dt, "i": np.arange(len(P))})
            rows.append(part.dropna(subset=["ex", "days_to"]))
    return pd.concat(rows, ignore_index=True) if rows else pd.DataFrame()


def evaluate(panel: pd.DataFrame, threshold: int = PRE_BLACKOUT_BDAYS) -> dict:
    from scipy import stats
    res = {}
    if panel.empty:
        return res
    for mk, g in panel.groupby("m"):
        g = g.assign(bo=g.days_to <= threshold)
        a, b = g[g.bo], g[~g.bo]
        r = {"n_blackout": int(len(a)), "n_resto": int(len(b)),
             "tickers_con_fechas": int(g.tk.nunique()),
             "exc_blackout": round(float(a.ex.mean()), 2) if len(a) else None,
             "exc_resto": round(float(b.ex.mean()), 2) if len(b) else None,
             "p5_blackout": round(float(a.ex.quantile(.05)), 2) if len(a) else None,
             "p5_resto": round(float(b.ex.quantile(.05)), 2) if len(b) else None,
             "std_blackout": round(float(a.ex.std()), 2) if len(a) > 1 else None,
             "std_resto": round(float(b.ex.std()), 2) if len(b) > 1 else None}
        if len(a) and len(b):
            r["diff_pp"] = round(r["exc_blackout"] - r["exc_resto"], 2)
            pt = g.groupby(["tk", "bo"]).ex.mean().unstack().dropna()
            if len(pt) >= 5 and True in pt and False in pt:
                d = pt[True] - pt[False]
                r["tickers_peor_en_blackout"] = f"{int((d < 0).sum())}/{len(d)}"
                try:
                    r["wilcoxon_p"] = round(float(stats.wilcoxon(d).pvalue), 4)
                except ValueError:
                    r["wilcoxon_p"] = None
            offs = []
            for o in range(21):
                h = g[(g.i % 21) == o]
                if h.bo.any() and (~h.bo).any():
                    offs.append(h[h.bo].ex.mean() - h[~h.bo].ex.mean())
            if offs:
                r["offsets_no_solapados_negativos"] = f"{int(np.sum(np.array(offs) < 0))}/{len(offs)}"
            r["material"] = abs(r["diff_pp"]) >= MATERIALIDAD_PP
        res[mk] = r
    return res


def compra_real(history: dict, px: dict, colmap: dict, cal: dict,
                threshold: int = PRE_BLACKOUT_BDAYS) -> dict:
    """Lectura B sobre señales COMPRA reales con ret_21d materializado."""
    panel = build_panel(px, colmap, cal)
    if panel.empty:
        return {}
    key = panel.set_index(["tk", panel.d.dt.strftime("%Y-%m-%d")])
    recs = []
    for day, sigs in history.items():
        for s in sigs:
            sig = s.get("signal_v2_original") or s.get("signal_v2") or ""
            if "COMPRA" not in sig:
                continue
            k = (s.get("ticker"), day)
            if k not in key.index:
                continue
            row = key.loc[k]
            row = row.iloc[0] if isinstance(row, pd.DataFrame) else row
            dt = s.get("earnings_days_to")
            recs.append({"m": s.get("mercado"), "tk": s.get("ticker"), "d": day,
                         "ex": row.ex, "ret": row.ret, "i": row.i,
                         "days_to": dt if dt is not None else row.days_to,
                         "point_in_time": dt is not None})
    if not recs:
        return {}
    df = pd.DataFrame(recs)
    out = evaluate(df, threshold)
    for mk, g in df.groupby("m"):
        if mk in out:
            out[mk]["pct_point_in_time"] = round(100 * g.point_in_time.mean(), 1)
    return out


def run(data_dir: str = DATA) -> dict:
    cal = load_calendar(os.path.join(data_dir, "earnings_calendar.json"))
    px = load_prices(data_dir)
    colmap = map_columns(px, _ticker_names())
    panel = build_panel(px, colmap, cal)
    hist_path = os.path.join(data_dir, "signals_history.json")
    history = json.load(open(hist_path, encoding="utf-8")) if os.path.exists(hist_path) else {}
    out = {
        "umbral_primario_bdays": PRE_BLACKOUT_BDAYS,
        "cobertura_calendario": {
            mk: f"{sum(1 for t, (m, _) in colmap.items() if m == mk and cal.get('tickers', {}).get(t, {}).get('dates'))}"
                f"/{sum(1 for _, (m, _) in colmap.items() if m == mk)}"
            for mk in px},
        "A_retro_universo": evaluate(panel),
        "A_secundario_umbral_5": evaluate(panel, 5),
        "A_secundario_umbral_21": evaluate(panel, 21),
        "B_compra_reales": compra_real(history, px, colmap, cal),
    }
    return out


if __name__ == "__main__":
    print(json.dumps(run(), ensure_ascii=False, indent=2))
