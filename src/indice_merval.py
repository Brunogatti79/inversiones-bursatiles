"""
src/indice_merval.py — Fuente real del índice Merval
====================================================

11/11/2026. Qué pasó: el S&P Merval dejó de calcularse el viernes
16/10/2026. Desde el lunes 19/10/2026 el índice de referencia es el MSCI
Merval (ticker MMSCI), que arrancó con base 100.000. Yahoo (^MERV) seguía
al índice viejo: la columna "INDICE MERVAL" de merval_cierres.csv quedó
sin datos desde el 16/10 (y ya venía con huecos), y con ella la tarjeta de
Panorama, la correlación entre mercados, el alpha contra índice del
backtester y la capa de cartera.

Fuente: descarga pública de históricos de Rava Bursátil
(https://www.rava.com/perfil/descarga-historicos/MERVAL). Es un CSV diario
desde 2021-10-26 con la serie completa en una sola escala: S&P Merval
reexpresado a la base nueva hasta el 16/10/2026 (cierre = 100.000) y MSCI
Merval desde el 19/10/2026.

Verificado el 11/11/2026 contra los valores de Yahoo que el repo ya tenía:
    Yahoo ^MERV / Rava = 28,489 constante
    (21/09: 2.998.956 / 105.265 · 05/10: 2.869.488 / 100.721 ·
     14/10: 2.835.843 / 99.540,1 · 15/10: 2.859.051 / 100.355 ·
     16/10: 2.848.945 / 100.000)
Es el mismo índice, en otra base. No es una aproximación ni una canasta.

Consecuencia visible: el NIVEL del índice pasa de ~2,8 millones a ~100 mil
en todo el CSV (toda la columna queda en la base nueva). Los retornos, que
es lo único que usan los cálculos, no cambian.

Protecciones (ante cualquiera, la columna queda como vino de Yahoo):
  1. Formato: tienen que estar las columnas 'fecha' y 'cierre' y al menos
     MIN_FILAS ruedas.
  2. Misma serie: donde hay valores de Yahoo para comparar, el cociente
     Yahoo/Rava tiene que ser constante (±TOL_COCIENTE en al menos el 95%
     de las ruedas). Si no lo es, no es la misma serie y no se usa.
  3. Saltos: ningún movimiento diario mayor a ±SALTO_MAX.
  4. Cobertura: de las últimas VENTANA_COBERTURA ruedas del CSV de acciones,
     Rava tiene que tener al menos COBERTURA_MIN.

No es una API oficial ni tiene SLA: es el link de descarga de la página de
Rava. Si cambia de formato o deja de responder, el índice queda sin
actualizar y download_status.json lo dice en "indice_merval".

Sin dependencias fuera de pandas y la librería estándar.
"""

from __future__ import annotations

import io
import logging
import urllib.request

import pandas as pd

logger = logging.getLogger(__name__)

RAVA_URL = "https://www.rava.com/perfil/descarga-historicos/MERVAL"
USER_AGENT = ("Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) "
              "Chrome/126.0 Safari/537.36")
TIMEOUT = 20
MIN_FILAS = 200
TOL_COCIENTE = 0.005
MIN_SOLAPE = 10
SALTO_MAX = 0.45      # el Merval ya tuvo ruedas de más de ±20% (21/11/2023)
VENTANA_COBERTURA = 60
COBERTURA_MIN = 0.90
FUENTE = ("rava.com — S&P Merval reexpresado hasta 2026-10-16, "
          "MSCI Merval desde 2026-10-19 (base 100.000)")


def _http_get_text(url: str) -> str:
    req = urllib.request.Request(url, headers={"User-Agent": USER_AGENT,
                                               "Accept": "text/csv,*/*"})
    with urllib.request.urlopen(req, timeout=TIMEOUT) as resp:
        return resp.read().decode("utf-8", errors="replace")


def parsear_rava(texto: str) -> pd.DataFrame | None:
    """CSV de Rava → DataFrame con columnas cierre/maximo/minimo e índice de
    fechas. None si el formato no es el esperado."""
    try:
        raw = pd.read_csv(io.StringIO(texto))
    except Exception as e:
        logger.warning(f"[indice_merval] CSV ilegible: {e}")
        return None
    raw.columns = [str(c).strip().lower() for c in raw.columns]
    if not {"fecha", "cierre"} <= set(raw.columns):
        logger.warning(f"[indice_merval] faltan columnas fecha/cierre: {list(raw.columns)[:8]}")
        return None
    fechas = pd.to_datetime(raw["fecha"], errors="coerce", format="%Y-%m-%d")
    out = pd.DataFrame(index=pd.DatetimeIndex(fechas))
    for col in ("cierre", "maximo", "minimo"):
        vals = pd.to_numeric(raw[col], errors="coerce").values if col in raw.columns else float("nan")
        out[col] = vals
    out = out[out.index.notna() & (out["cierre"] > 0)]
    # 0 en máximo/mínimo = dato faltante en Rava, no un precio
    out["maximo"] = out["maximo"].where(out["maximo"] > 0)
    out["minimo"] = out["minimo"].where(out["minimo"] > 0)
    out = out[~out.index.duplicated(keep="last")].sort_index()
    if len(out) < MIN_FILAS:
        logger.warning(f"[indice_merval] solo {len(out)} ruedas (mínimo {MIN_FILAS})")
        return None
    return out


def _validar(rava: pd.DataFrame, yahoo: pd.Series | None, fechas: pd.DatetimeIndex) -> tuple[bool, dict]:
    info = {}
    saltos = rava["cierre"].pct_change().abs()
    if (saltos > SALTO_MAX).any():
        return False, {"motivo": f"salto diario mayor a {SALTO_MAX:.0%} el "
                                 f"{saltos.idxmax().date()}"}

    recientes = fechas[-VENTANA_COBERTURA:]
    cobertura = float(rava["cierre"].reindex(recientes).notna().mean()) if len(recientes) else 0.0
    info["cobertura_ultimas_ruedas"] = round(cobertura, 3)
    if cobertura < COBERTURA_MIN:
        return False, {**info, "motivo": f"cobertura {cobertura:.0%} en las últimas "
                                         f"{len(recientes)} ruedas (mínimo {COBERTURA_MIN:.0%})"}

    if yahoo is not None:
        comun = pd.concat([yahoo.rename("y"), rava["cierre"].rename("r")], axis=1).dropna()
        comun = comun[(comun["y"] > 0) & (comun["r"] > 0)]
        if len(comun) >= MIN_SOLAPE:
            cociente = comun["y"] / comun["r"]
            mediana = float(cociente.median())
            dentro = float(((cociente / mediana - 1).abs() <= TOL_COCIENTE).mean())
            info["cociente_yahoo_rava"] = round(mediana, 4)
            info["ruedas_comparadas"] = int(len(comun))
            if dentro < 0.95:
                return False, {**info, "motivo": f"no es la misma serie que Yahoo: solo "
                                                 f"{dentro:.0%} de las ruedas con cociente constante"}
    return True, info


def aplicar_indice_merval(
    df: pd.DataFrame,
    col_indice: str,
    df_high: pd.DataFrame | None = None,
    df_low: pd.DataFrame | None = None,
    http_get=_http_get_text,
):
    """
    Reemplaza la columna del índice de `df` (cierres, índice de fechas) por la
    serie de Rava, en todas las fechas de `df` que Rava tenga. Las fechas
    anteriores al inicio de Rava (2021-10-26) quedan vacías: no se mezclan
    dos bases en la misma columna.

    Devuelve (df, df_high, df_low, info). Si algo falla o no valida, devuelve
    los mismos objetos de entrada e info["aplicado"] = False con el motivo.
    """
    info = {"aplicado": False, "fuente": FUENTE}
    if df is None or df.empty:
        return df, df_high, df_low, {**info, "motivo": "sin datos de acciones"}
    try:
        texto = http_get(RAVA_URL)
    except Exception as e:
        logger.warning(f"[indice_merval] Rava no responde: {e}")
        return df, df_high, df_low, {**info, "motivo": f"Rava no responde: {e}"}

    rava = parsear_rava(texto)
    if rava is None:
        return df, df_high, df_low, {**info, "motivo": "formato inesperado"}

    fechas = pd.DatetimeIndex(df.index)
    yahoo = df[col_indice] if col_indice in df.columns else None
    ok, detalle = _validar(rava, yahoo, fechas)
    info.update(detalle)
    if not ok:
        logger.warning(f"[indice_merval] no se aplica: {detalle.get('motivo')}")
        return df, df_high, df_low, info

    def _reemplazar(base, serie):
        if base is None:
            return None
        out = base.copy()
        out[col_indice] = serie.reindex(pd.DatetimeIndex(out.index)).values
        return out

    df_out = _reemplazar(df, rava["cierre"])
    hi_out = _reemplazar(df_high, rava["maximo"])
    lo_out = _reemplazar(df_low, rava["minimo"])

    serie = df_out[col_indice].dropna()
    info.update({
        "aplicado": True,
        "ultima_fecha": str(pd.Timestamp(serie.index[-1]).date()) if len(serie) else None,
        "ultimo_valor": round(float(serie.iloc[-1]), 2) if len(serie) else None,
        "ruedas_con_dato": int(len(serie)),
    })
    logger.info(f"[indice_merval] índice aplicado desde Rava: {info['ruedas_con_dato']} ruedas, "
                f"último {info['ultimo_valor']} ({info['ultima_fecha']})")
    return df_out, hi_out, lo_out, info
