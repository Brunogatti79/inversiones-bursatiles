"""
src/downloader.py  ── versión v4: lee CSVs pre-descargados por GitHub Actions
Los datos son descargados diariamente por el workflow "Descargar datos de mercado"
(.github/workflows/download_data.yml) que corre a las 14:50 UTC (10 min antes que Railway).
Railway simplemente lee esos CSVs desde data/.
 
Si los CSVs no existen o son muy viejos (>3 días), intenta descarga directa como fallback.
"""
 
import yfinance as yf
import pandas as pd
import numpy as np
from datetime import datetime, timedelta
import logging
import time
import random
import os
import json
import pytz
 
logger = logging.getLogger(__name__)
 
# ─────────────────────────────────────────────
# Tickers por mercado
# ─────────────────────────────────────────────
 
MERVAL_TICKERS = {
    "GGAL.BA":  "Grupo Financiero Galicia",
    "BMA.BA":   "Banco Macro",
    "PAMP.BA":  "Pampa Energía",
    "TXAR.BA":  "Ternium Argentina",
    "ALUA.BA":  "Aluar",
    "CRES.BA":  "Cresud",
    "SUPV.BA":  "Supervielle",
    "CEPU.BA":  "Central Puerto",
    "LOMA.BA":  "Loma Negra",
    "MIRG.BA":  "Mirgor",
    "TECO2.BA": "Telecom Argentina",
    "TGSU2.BA": "Transportadora Gas del Sur",
    "VALO.BA":  "Grupo Supervielle (VALO)",
    "COME.BA":  "Soc. Comercial del Plata",
    "EDN.BA":   "Edenor",
    "HARG.BA":  "Holcim Argentina",
    "TRAN.BA":  "Transener",
    "MOLI.BA":  "Molinos Río de la Plata",
    "BYMA.BA":  "BYMA",
    "IRSA.BA":  "IRSA",
    "YPFD.BA":  "YPF",
    "BBAR.BA":  "BBVA Argentina",
}
MERVAL_INDEX = "^MERV"
 
BOVESPA_TICKERS = {
    "PETR4.SA":  "Petrobras PN",
    "VALE3.SA":  "Vale",
    "ITUB4.SA":  "Itaú Unibanco",
    "BBDC4.SA":  "Bradesco",
    "ABEV3.SA":  "Ambev",
    "WEGE3.SA":  "WEG",
    "RENT3.SA":  "Localiza",
    "RDOR3.SA":  "Rede D'Or",
    "BBAS3.SA":  "Banco do Brasil",
    "MGLU3.SA":  "Magazine Luiza",
    "SUZB3.SA":  "Suzano",
    "EQTL3.SA":  "Equatorial",
    "RAIZ4.SA":  "Raízen",
    "HAPV3.SA":  "Hapvida",
    "LREN3.SA":  "Lojas Renner",
    "CSNA3.SA":  "CSN",
    "CYRE3.SA":  "Cyrela",
    "EGIE3.SA":  "Engie Brasil",
    "BPAC11.SA": "BTG Pactual",
    "B3SA3.SA":  "B3 - Brasil Bolsa Balcão",
    "EMBR3.SA":  "Embraer",
    "JBSS3.SA":  "JBS",
    "ITSA4.SA":  "Itaúsa",
    "SANB11.SA": "Santander Brasil",
    "VIVT3.SA":  "Vivo Telefônica Brasil",
}
BOVESPA_INDEX = "^BVSP"
 
SP500_TICKERS = {
    "AAPL":  "Apple",
    "MSFT":  "Microsoft",
    "NVDA":  "NVIDIA",
    "GOOGL": "Alphabet (Google)",
    "META":  "Meta Platforms",
    "AMZN":  "Amazon",
    "JPM":   "JPMorgan Chase",
    "BAC":   "Bank of America",
    "GS":    "Goldman Sachs",
    "V":     "Visa",
    "XOM":   "ExxonMobil",
    "CVX":   "Chevron",
    "JNJ":   "Johnson & Johnson",
    "UNH":   "UnitedHealth",
    "LLY":   "Eli Lilly",
    "WMT":   "Walmart",
    "PG":    "Procter & Gamble",
    "KO":    "Coca-Cola",
    "MCD":   "McDonald's",
    "CAT":   "Caterpillar",
    "BA":    "Boeing",
    "GE":    "GE Aerospace",
    "TSLA":  "Tesla",
    "COPX":  "Global X Copper Miners ETF",
    "GLOB":  "Globant",
    "IBB":   "iShares Biotechnology ETF",
    "MELI":  "MercadoLibre",
    "RIO":   "Rio Tinto",
    "PBR":   "Petrobras ADR",
    "QCOM":  "Qualcomm",
    "EWZ":   "iShares MSCI Brazil ETF",
}
SP500_INDEX = "^GSPC" 
MIN_ROWS      = 10
MAX_CSV_AGE_DAYS = 3   # si el CSV tiene más de 3 días, intentar descarga directa

# FIX 03/09/2026 (auditoría de log real, 2026-09-02/03): _download_direct()
# solo validaba len(closes) >= MIN_ROWS -- eso cuenta DÍAS, no TICKERS. Un
# batch de yf.download() rate-limiteado por Yahoo puede devolver un
# DataFrame con 39/40 columnas vacías y aun así tener >=10 filas de fechas,
# pasando el check como si fuera un éxito. Confirmado en producción: la fila
# 2026-09-02 de sp500_cierres.csv quedó con 1/40 columnas con dato, porque
# save_csvs() (ver pipeline.py) escribe lo que sea que download_all()
# devuelva sin volver a chequear cobertura. Mismo umbral que ya usa
# scripts/download_data.py (MIN_SUCCESS_RATE=0.5) para quedar consistentes
# entre el script de GitHub Actions y el fallback de Railway.
MIN_SUCCESS_RATE = 0.5
DIRECT_DOWNLOAD_ATTEMPTS = 2   # mismo patrón de retry que scripts/download_data.py
DIRECT_DOWNLOAD_BACKOFF  = (4, 8)  # segundos, rango aleatorio entre intentos
 
 
# ─────────────────────────────────────────────
# Helpers
# ─────────────────────────────────────────────
 
def _csv_path(market: str, data_dir: str) -> str:
    return os.path.join(data_dir, f"{market.lower()}_cierres.csv")


def reload_price_csvs_fresh(data_dir: str = "data") -> dict | None:
    """
    FIX 10/08/2026 (auditoría externa v20, prioridad #1): el backtester
    corría siempre sobre merval_df/bovespa_df/sp500_df cargados en la
    FASE 1/8 del pipeline (el arranque de la corrida) y nunca se volvían
    a leer -- si GitHub Actions (download_data.yml, proceso independiente
    del pipeline de Railway, sin sincronización entre ambos) pusheaba
    CSVs de precio más nuevos MIENTRAS el pipeline de Railway ya estaba
    corriendo, el backtest terminaba evaluando retornos con precios de
    horas antes, sin que nada lo reflejara. Confirmado en producción el
    10/08/2026: recalcular el backtest con los CSVs más frescos del
    mismo commit dio ranking_top_vs_rest.samples=1876 vs 1742 persistido,
    y el EV del top 20% cambió de signo completo (-3.52% -> +3.48%).

    Se llama justo antes de correr el backtester (no en la Fase 1, que
    sigue usando lo que cargó al arrancar -- las señales de HOY deben
    ser consistentes con el momento del análisis, no hace falta que sean
    del segundo exacto; el backtest en cambio evalúa retornos HISTÓRICOS
    de señales pasadas, y ahí sí importa usar el precio más fresco
    posible para que más trades alcancen a completar su horizonte).

    Trae los 3 CSVs directo de GitHub (raw.githubusercontent.com, misma
    fuente de verdad que usa GitHub Actions para pushear) vía
    github_persistence.pull_file() -- no vuelve a pegarle a Yahoo
    Finance, es más rápido y evita otra fuente de inconsistencia.

    Devuelve {"merval": df, "bovespa": df, "sp500": df} si los 3 se
    pudieron traer y parsear, o None si algo falló -- el caller debe
    caer a los DataFrames ya cargados al arrancar el pipeline en ese
    caso, nunca romper la corrida por esto (backtester es un paso "no
    crítico" del pipeline, este refresh tampoco debería serlo).
    """
    from src.github_persistence import pull_file

    result = {}
    for market in ("merval", "bovespa", "sp500"):
        path = _csv_path(market, data_dir)
        try:
            if not pull_file(path):
                logger.warning(f"[reload_price_csvs_fresh] pull_file falló para {path}, "
                               f"backtester va a usar los CSVs cargados al arrancar el pipeline")
                return None
        except Exception as e:
            logger.warning(f"[reload_price_csvs_fresh] Error pull_file({path}): {e}")
            return None

        df = _load_csv(market, data_dir)
        if df is None or df.empty:
            logger.warning(f"[reload_price_csvs_fresh] {path} se trajo pero no se pudo parsear/vacío")
            return None
        result[market] = df

    logger.info("[reload_price_csvs_fresh] 3 CSVs de precio refrescados desde GitHub para el backtester")
    return result
 
 
def _get_period():
    end   = datetime.now(pytz.UTC)
    start = end - timedelta(days=400)
    return start.strftime("%Y-%m-%d"), end.strftime("%Y-%m-%d")
 
 
def _index_display_name(market: str) -> str:
    return {
        "MERVAL":  "ÍNDICE MERVAL",
        "BOVESPA": "ÍNDICE BOVESPA",
        "SP500":   "ÍNDICE S&P 500",
    }.get(market, market)
 
 
def _load_csv(market: str, data_dir: str) -> pd.DataFrame | None:
    """Carga el CSV pre-descargado por GitHub Actions."""
    path = _csv_path(market, data_dir)
    if not os.path.exists(path):
        logger.warning(f"[{market}] CSV no encontrado: {path}")
        return None
    try:
        df = pd.read_csv(path, sep=";", decimal=",", index_col=0,
                         encoding="utf-8-sig", thousands=" ")
        df.index = pd.to_datetime(df.index)
        df.index.name = "Fecha"
        for col in df.columns:
            df[col] = (
                df[col].astype(str)
                .str.replace(" ", "", regex=False)
                .str.replace(",", ".", regex=False)
            )
            df[col] = pd.to_numeric(df[col], errors="coerce")
        df = df.sort_index().dropna(how="all")
 
        if len(df) < MIN_ROWS:
            logger.warning(f"[{market}] CSV muy pequeño: {len(df)} filas")
            return None
 
        # Verificar antigüedad
        last_date = df.index[-1]
        age_days  = (datetime.now(pytz.UTC) - last_date.tz_localize(pytz.UTC)).days
        logger.info(f"[{market}] CSV OK — {len(df)} filas, último: {last_date.date()}, antigüedad: {age_days}d")
        return df
 
    except Exception as e:
        logger.error(f"[{market}] Error leyendo CSV: {e}")
        return None
 
 
def _check_status(data_dir: str) -> str:
    """Lee el archivo de estado de GitHub Actions."""
    path = os.path.join(data_dir, "download_status.json")
    if not os.path.exists(path):
        return "Sin información de descarga"
    try:
        with open(path) as f:
            status = json.load(f)
        return f"Datos descargados por GitHub Actions el {status.get('timestamp_utc', '?')}"
    except Exception:
        return "Error leyendo status"
 
 
def _download_direct(tickers: dict, index_ticker: str, market_name: str) -> pd.DataFrame | None:
    """
    Intento de descarga directa desde Yahoo Finance como último recurso.
    Puede fallar por rate limit desde Railway.

    FIX 03/09/2026: antes era un único intento sin retry (a diferencia de
    scripts/download_data.py, que sí reintenta) y solo validaba cantidad de
    FILAS, no cobertura de TICKERS -- un batch parcialmente rate-limiteado
    (ej. solo el índice con dato, todos los tickers individuales en NaN)
    pasaba como "éxito" con >=10 filas igual. Ahora reintenta hasta
    DIRECT_DOWNLOAD_ATTEMPTS veces con backoff, y exige que al menos
    MIN_SUCCESS_RATE de las columnas tengan dato en la última fila -- si
    no, se descarta el intento entero y NO se devuelve un DataFrame a medio
    llenar (ver save_csvs() en este mismo módulo, que ya no confía
    ciegamente en lo que le llega de acá tampoco, como defensa en
    profundidad).
    """
    start, end = _get_period()
    all_tickers = list(tickers.keys()) + [index_ticker]

    for intento in range(1, DIRECT_DOWNLOAD_ATTEMPTS + 1):
        logger.info(f"[{market_name}] Intentando descarga directa Yahoo Finance "
                    f"({len(all_tickers)} tickers, intento {intento}/{DIRECT_DOWNLOAD_ATTEMPTS})...")
        try:
            raw = yf.download(
                tickers=all_tickers,
                start=start,
                end=end,
                auto_adjust=True,
                progress=False,
                threads=False,
            )
            if isinstance(raw.columns, pd.MultiIndex):
                closes = raw["Close"].copy()
            else:
                closes = raw[["Close"]].rename(columns={"Close": all_tickers[0]}).copy()

            rename_map = {t: n for t, n in tickers.items() if t in closes.columns}
            idx_name = _index_display_name(market_name)
            if index_ticker in closes.columns:
                rename_map[index_ticker] = idx_name
            closes = closes.rename(columns=rename_map)
            closes.index = pd.to_datetime(closes.index)
            closes.index.name = "Fecha"
            closes = closes.sort_index().dropna(how="all")

            if len(closes) < MIN_ROWS:
                logger.warning(f"[{market_name}] Descarga directa vacía ({len(closes)} filas)")
            else:
                ultima_fila = closes.iloc[-1]
                total_cols = len(closes.columns)
                tickers_con_dato = int(ultima_fila.notna().sum())
                tasa = tickers_con_dato / total_cols if total_cols else 0

                if tasa >= MIN_SUCCESS_RATE:
                    logger.info(f"[{market_name}] Descarga directa OK — {len(closes)} filas, "
                                f"{tickers_con_dato}/{total_cols} tickers con dato ({tasa:.0%})")
                    return closes
                logger.warning(
                    f"[{market_name}] Descarga directa con cobertura insuficiente: "
                    f"{tickers_con_dato}/{total_cols} tickers ({tasa:.0%}) en la última fila "
                    f"-- se descarta este intento (probable rate limit parcial de Yahoo)"
                )
        except Exception as e:
            logger.warning(f"[{market_name}] Descarga directa falló (intento {intento}): {e}")

        if intento < DIRECT_DOWNLOAD_ATTEMPTS:
            espera = random.uniform(*DIRECT_DOWNLOAD_BACKOFF)
            logger.info(f"[{market_name}] Reintentando descarga directa en {espera:.1f}s...")
            time.sleep(espera)

    logger.warning(f"[{market_name}] Descarga directa agotó los {DIRECT_DOWNLOAD_ATTEMPTS} intentos sin éxito")
    return None
 
 
# ─────────────────────────────────────────────
# Descarga principal — prioriza CSV de GitHub Actions
# ─────────────────────────────────────────────
 
def _load_market(tickers: dict, index_ticker: str, market_name: str,
                 data_dir: str = "data") -> pd.DataFrame:
    """
    Estrategia de carga en orden de prioridad:
    1. CSV pre-descargado por GitHub Actions (data/xxx_cierres.csv)
    2. Descarga directa Yahoo Finance (fallback si CSV muy viejo o inexistente)
    3. Error explícito si todo falla
    """
    # 1. Intentar CSV de GitHub Actions
    df = _load_csv(market_name, data_dir)
    if df is not None:
        last_date = df.index[-1]
        age_days = (datetime.now(pytz.UTC) - last_date.tz_localize(pytz.UTC)).days
        if age_days <= MAX_CSV_AGE_DAYS:
            logger.info(f"[{market_name}] ✓ Usando CSV de GitHub Actions ({age_days}d de antigüedad)")
            return df
        else:
            logger.warning(f"[{market_name}] CSV desactualizado ({age_days}d) — intentando descarga directa")
 
    # 2. Fallback: descarga directa Yahoo Finance
    df_direct = _download_direct(tickers, index_ticker, market_name)
    if df_direct is not None:
        return df_direct
 
    # 3. Si la descarga directa falló pero tenemos CSV (aunque viejo), usarlo igual
    df_old = _load_csv(market_name, data_dir)
    if df_old is not None:
        logger.warning(f"[{market_name}] Usando CSV desactualizado como último recurso")
        return df_old
 
    raise RuntimeError(
        f"[{market_name}] Sin datos disponibles. "
        f"El CSV de GitHub Actions no existe y la descarga directa falló. "
        f"Verificar workflow 'Descargar datos de mercado' en GitHub Actions."
    )
 
 
# ─────────────────────────────────────────────
# Punto de entrada público
# ─────────────────────────────────────────────
 
def download_all(data_dir: str = "data") -> dict:
    """
    Carga datos de los 3 mercados.
    Prioriza CSVs de GitHub Actions, cae a descarga directa si es necesario.
    """
    status_msg = _check_status(data_dir)
    logger.info(status_msg)
 
    results = {}
    results["merval"]  = _load_market(MERVAL_TICKERS,  MERVAL_INDEX,  "MERVAL",  data_dir)
    results["bovespa"] = _load_market(BOVESPA_TICKERS, BOVESPA_INDEX, "BOVESPA", data_dir)
    results["sp500"]   = _load_market(SP500_TICKERS,   SP500_INDEX,   "SP500",   data_dir)
    return results
 
 
def save_csvs(data: dict, output_dir: str = "data") -> dict:
    """
    Guarda los DataFrames como CSV.

    FIX 03/09/2026: el docstring de esta función siempre dijo "solo si
    vinieron de descarga directa", pero el código escribía SIEMPRE, sin
    excepción -- pipeline.py la llama incondicionalmente después de
    download_all() (que puede devolver el DataFrame bueno del CSV de
    GitHub Actions, o el de _download_direct() como fallback). Con
    _download_direct() ahora validando cobertura antes de devolver algo
    (ver ese docstring), este caso ya no debería darse en la práctica --
    pero se deja este chequeo igual como defensa en profundidad: si algún
    DataFrame con cobertura insuficiente llega hasta acá por cualquier otro
    camino futuro, NO se pisa el CSV persistido (se preserva el que ya
    había, típicamente el bueno de GitHub Actions) en vez de contaminarlo.
    """
    os.makedirs(output_dir, exist_ok=True)
    paths = {}
    for market, df in data.items():
        path = _csv_path(market, output_dir)

        if df is None or df.empty:
            logger.warning(f"[{market}] save_csvs: DataFrame vacío/None -- no se escribe {path}")
            continue

        ultima_fila = df.iloc[-1]
        total_cols = len(df.columns)
        tickers_con_dato = int(ultima_fila.notna().sum())
        tasa = tickers_con_dato / total_cols if total_cols else 0

        if tasa < MIN_SUCCESS_RATE:
            logger.warning(
                f"[{market}] save_csvs: cobertura insuficiente en la última fila "
                f"({tickers_con_dato}/{total_cols}, {tasa:.0%}) -- NO se pisa {path}, "
                f"se preserva el CSV existente"
            )
            continue

        df.to_csv(path, sep=";", decimal=",", encoding="utf-8-sig")
        paths[market] = path
        logger.info(f"Guardado: {path} ({len(df)} filas, {tickers_con_dato}/{total_cols} "
                    f"tickers con dato en la última fila, {tasa:.0%})")
    return paths


# ─────────────────────────────────────────────
# High/Low reales (Fix 27/07/2026, roadmap externo P6)
# ─────────────────────────────────────────────

def _load_ohlc_csv(market: str, kind: str, data_dir: str) -> pd.DataFrame | None:
    """
    Carga data/{market}_{kind}.csv (kind ∈ {"high","low"}) generado por
    scripts/download_data.py. Devuelve None si el archivo no existe todavía
    (ej. GitHub Actions no corrió aún después de este cambio, o el workflow
    .yml no fue actualizado manualmente para commitear estos archivos nuevos
    -- requiere scope 'workflow' que el GH_TOKEN actual no tiene) -- nunca
    levanta excepción hacia el caller, para que analyze_market() pueda seguir
    funcionando con el proxy close-only de siempre si esto falta.
    """
    path = os.path.join(data_dir, f"{market.lower()}_{kind}.csv")
    if not os.path.exists(path):
        return None
    try:
        df = pd.read_csv(path, sep=";", decimal=",", index_col=0,
                         encoding="utf-8-sig", thousands=" ")
        df.index = pd.to_datetime(df.index)
        df.index.name = "Fecha"
        for col in df.columns:
            df[col] = (
                df[col].astype(str)
                .str.replace(" ", "", regex=False)
                .str.replace(",", ".", regex=False)
            )
            df[col] = pd.to_numeric(df[col], errors="coerce")
        df = df.sort_index().dropna(how="all")
        if df.empty:
            return None
        return df
    except Exception as e:
        logger.warning(f"[{market}] Error leyendo {kind}.csv (no crítico, se usa proxy close-only): {e}")
        return None


def load_ohlc_extra(data_dir: str = "data") -> dict:
    """
    Carga High/Low reales para los 3 mercados si están disponibles, SEPARADO
    del dict que devuelve download_all() a propósito -- ver docstring de
    _load_ohlc_csv(). validar_todos()/compute_volatility_regime()/
    _build_price_index() siguen recibiendo exactamente el dict de siempre
    ({"merval": df, "bovespa": df, "sp500": df}, solo Close); esto es un
    canal aparte que solo consume analyze_market() para ATR/ADX reales.

    Returns:
        {"MERVAL": {"high": df|None, "low": df|None}, "BOVESPA": {...}, "SP500": {...}}
    """
    result = {}
    for market in ("MERVAL", "BOVESPA", "SP500"):
        result[market] = {
            "high": _load_ohlc_csv(market, "high", data_dir),
            "low":  _load_ohlc_csv(market, "low", data_dir),
        }
    return result


def load_cedear_close_extra(data_dir: str = "data") -> pd.DataFrame | None:
    """
    FIX 10/08/2026 (auditoría externa v20, prioridad #2): data/cedear_cierres.csv
    se pushea desde el 29/06/2026 (snapshot diario del precio CEDEAR real vía
    data912.com, ver model_version.py) pero hasta hoy ningún módulo lo leía --
    _atr() para todo el universo SP500/CEDEARs se calculaba sobre el precio del
    subyacente NYSE (sp500_cierres.csv), correcto para el negocio pero en la
    escala/mercado equivocado para dimensionar stops: un CEDEAR se opera en
    pesos vía BYMA, con su propia liquidez, gaps y ruido de CCL -- la
    volatilidad efectiva que importa para el sizing del stop no es la de NYSE.

    Devuelve un DataFrame indexado por fecha, columnas = ticker (mismo
    formato que _load_ohlc_csv), o None si el archivo no existe o está vacío
    -- nunca levanta excepción hacia el caller. analyze_market() cae al
    comportamiento actual (ATR sobre NYSE) para cualquier ticker sin
    cobertura acá, no solo para cuando el archivo entero falta.
    """
    path = os.path.join(data_dir, "cedear_cierres.csv")
    if not os.path.exists(path):
        return None
    try:
        df = pd.read_csv(path, sep=";", decimal=",", index_col=0,
                         encoding="utf-8-sig", thousands=" ")
        df.index = pd.to_datetime(df.index)
        df.index.name = "Fecha"
        for col in df.columns:
            df[col] = (
                df[col].astype(str)
                .str.replace(" ", "", regex=False)
                .str.replace(",", ".", regex=False)
            )
            df[col] = pd.to_numeric(df[col], errors="coerce")
        df = df.sort_index().dropna(how="all")
        if df.empty:
            return None
        return df
    except Exception as e:
        logger.warning(f"[CEDEAR] Error leyendo cedear_cierres.csv (no crítico, se usa ATR sobre NYSE): {e}")
        return None

 
