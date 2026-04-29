"""
src/downloader.py
Descarga automática de precios desde Yahoo Finance.
"""

import yfinance as yf
import pandas as pd
import numpy as np
from datetime import datetime, timedelta
import logging
import pytz

logger = logging.getLogger(__name__)

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
}
SP500_INDEX = "^GSPC"


def _get_period():
    end = datetime.now(pytz.UTC)
    start = end - timedelta(days=400)
    return start.strftime("%Y-%m-%d"), end.strftime("%Y-%m-%d")


def _download_batch(tickers: dict, index_ticker: str, market_name: str) -> pd.DataFrame:
    start, end = _get_period()
    all_tickers = list(tickers.keys()) + [index_ticker]
    logger.info(f"[{market_name}] Descargando {len(all_tickers)} tickers desde {start}...")

    try:
        raw = yf.download(
            tickers=all_tickers,
            start=start,
            end=end,
            auto_adjust=True,
            progress=False,
            threads=True,
        )
    except Exception as e:
        logger.error(f"[{market_name}] Error en descarga: {e}")
        return pd.DataFrame()

    if raw is None or raw.empty:
        logger.warning(f"[{market_name}] DataFrame vacío — sin datos disponibles")
        return pd.DataFrame()

    if isinstance(raw.columns, pd.MultiIndex):
        closes = raw["Close"].copy()
    else:
        closes = raw[["Close"]].copy().rename(columns={"Close": all_tickers[0]})

    rename_map = {t: n for t, n in tickers.items() if t in closes.columns}
    index_col_name = _index_display_name(market_name)
    if index_ticker in closes.columns:
        rename_map[index_ticker] = index_col_name

    closes = closes.rename(columns=rename_map)
    closes.index = pd.to_datetime(closes.index)
    closes.index.name = "Fecha"
    closes = closes.sort_index().dropna(how="all")

    logger.info(f"[{market_name}] OK — {len(closes)} días, {len(closes.columns)} columnas")
    return closes


def _index_display_name(market: str) -> str:
    return {
        "MERVAL":  "ÍNDICE MERVAL",
        "BOVESPA": "ÍNDICE BOVESPA",
        "SP500":   "ÍNDICE S&P 500",
    }.get(market, market)


def download_all() -> dict:
    import time
    results = {}
    for key, tickers, index in [
        ("merval",  MERVAL_TICKERS,  MERVAL_INDEX),
        ("bovespa", BOVESPA_TICKERS, BOVESPA_INDEX),
        ("sp500",   SP500_TICKERS,   SP500_INDEX),
    ]:
        results[key] = _download_batch(tickers, index, key.upper())
        time.sleep(5)
    return results


def save_csvs(data: dict, output_dir: str = "data") -> dict:
    import os
    os.makedirs(output_dir, exist_ok=True)
    paths = {}
    for market, df in data.items():
        if df.empty:
            continue
        path = f"{output_dir}/{market}_cierres.csv"
        df.to_csv(path, sep=";", decimal=",", encoding="utf-8-sig")
        paths[market] = path
        logger.info(f"Guardado: {path} ({len(df)} filas)")
    return paths
