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
}
SP500_INDEX = "^GSPC" 
MIN_ROWS      = 10
MAX_CSV_AGE_DAYS = 3   # si el CSV tiene más de 3 días, intentar descarga directa
 
 
# ─────────────────────────────────────────────
# Helpers
# ─────────────────────────────────────────────
 
def _csv_path(market: str, data_dir: str) -> str:
    return os.path.join(data_dir, f"{market.lower()}_cierres.csv")
 
 
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
    """
    start, end = _get_period()
    all_tickers = list(tickers.keys()) + [index_ticker]
    logger.info(f"[{market_name}] Intentando descarga directa Yahoo Finance ({len(all_tickers)} tickers)...")
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
 
        if len(closes) >= MIN_ROWS:
            logger.info(f"[{market_name}] Descarga directa OK — {len(closes)} filas")
            return closes
        else:
            logger.warning(f"[{market_name}] Descarga directa vacía ({len(closes)} filas)")
            return None
    except Exception as e:
        logger.warning(f"[{market_name}] Descarga directa falló: {e}")
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
    """Guarda los DataFrames como CSV (solo si vinieron de descarga directa)."""
    os.makedirs(output_dir, exist_ok=True)
    paths = {}
    for market, df in data.items():
        path = _csv_path(market, output_dir)
        df.to_csv(path, sep=";", decimal=",", encoding="utf-8-sig")
        paths[market] = path
        logger.info(f"Guardado: {path} ({len(df)} filas)")
    return paths
 
