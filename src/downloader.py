"""
src/downloader.py  ── versión v3 con descarga individual + anti-rate-limit
Descarga automática de precios desde Yahoo Finance.
Cubre MERVAL (Argentina), BOVESPA (Brasil) y S&P 500 (EE.UU.).
 
CAMBIOS v3:
  - Descarga ticker por ticker (no en batch) para evitar rate limit
  - Delay aleatorio entre requests (2-5s)
  - User-agent rotation para simular navegador real
  - Session reutilizable con headers HTTP reales
  - Retry por ticker individual antes de descartar
  - Fallback CSV si falla más del 50% de los tickers
"""
 
import yfinance as yf
import pandas as pd
import numpy as np
from datetime import datetime, timedelta
import logging
import time
import os
import random
import requests
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
}
SP500_INDEX = "^GSPC"
 
# ─────────────────────────────────────────────
# Configuración anti-rate-limit
# ─────────────────────────────────────────────
 
MIN_ROWS       = 10
DELAY_MIN      = 2.0   # segundos mínimos entre tickers
DELAY_MAX      = 5.0   # segundos máximos entre tickers
TICKER_RETRIES = 2     # reintentos por ticker individual
MIN_SUCCESS_RATE = 0.5 # mínimo 50% de tickers exitosos para considerar válido
 
# User agents reales para rotar
USER_AGENTS = [
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/124.0.0.0 Safari/537.36",
    "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/123.0.0.0 Safari/537.36",
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64; rv:124.0) Gecko/20100101 Firefox/124.0",
    "Mozilla/5.0 (Macintosh; Intel Mac OS X 14_4) AppleWebKit/605.1.15 (KHTML, like Gecko) Version/17.4 Safari/605.1.15",
    "Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/122.0.0.0 Safari/537.36",
]
 
 
# ─────────────────────────────────────────────
# Session con headers reales
# ─────────────────────────────────────────────
 
def _make_session() -> requests.Session:
    """Crea una session HTTP con headers de navegador real."""
    session = requests.Session()
    session.headers.update({
        "User-Agent": random.choice(USER_AGENTS),
        "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
        "Accept-Language": "en-US,en;q=0.5",
        "Accept-Encoding": "gzip, deflate, br",
        "DNT": "1",
        "Connection": "keep-alive",
        "Upgrade-Insecure-Requests": "1",
    })
    return session
 
 
# ─────────────────────────────────────────────
# Helpers
# ─────────────────────────────────────────────
 
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
 
 
def _csv_path(market: str, data_dir: str) -> str:
    return os.path.join(data_dir, f"{market.lower()}_cierres.csv")
 
 
def _load_fallback(market: str, data_dir: str) -> pd.DataFrame | None:
    path = _csv_path(market, data_dir)
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
        logger.warning(f"[{market}] Usando fallback CSV: {path} ({len(df)} filas)")
        return df
    except Exception as e:
        logger.error(f"[{market}] Fallback CSV inválido: {e}")
        return None
 
 
# ─────────────────────────────────────────────
# Descarga individual por ticker
# ─────────────────────────────────────────────
 
def _download_single_ticker(ticker: str, start: str, end: str,
                             session: requests.Session,
                             market_name: str) -> pd.Series | None:
    """
    Descarga un solo ticker con reintentos y delay.
    Retorna una Series con el cierre ajustado, o None si falla.
    """
    for attempt in range(1, TICKER_RETRIES + 1):
        try:
            t = yf.Ticker(ticker, session=session)
            hist = t.history(start=start, end=end, auto_adjust=True)
 
            if hist.empty or len(hist) < MIN_ROWS:
                logger.debug(f"[{market_name}] {ticker}: sin datos (intento {attempt})")
                if attempt < TICKER_RETRIES:
                    time.sleep(random.uniform(DELAY_MIN, DELAY_MAX))
                    # Rotar user agent en el reintento
                    session.headers["User-Agent"] = random.choice(USER_AGENTS)
                continue
 
            serie = hist["Close"].copy()
            serie.index = pd.to_datetime(serie.index).tz_localize(None)
            serie.name = ticker
            return serie
 
        except Exception as e:
            err_str = str(e).lower()
            if "rate limit" in err_str or "429" in err_str or "too many" in err_str:
                wait = random.uniform(10, 20)
                logger.warning(f"[{market_name}] {ticker}: rate limit, esperando {wait:.0f}s...")
                time.sleep(wait)
                session.headers["User-Agent"] = random.choice(USER_AGENTS)
            else:
                logger.debug(f"[{market_name}] {ticker}: error {e}")
                if attempt < TICKER_RETRIES:
                    time.sleep(random.uniform(DELAY_MIN, DELAY_MAX))
 
    return None
 
 
# ─────────────────────────────────────────────
# Descarga por mercado
# ─────────────────────────────────────────────
 
def _download_batch(tickers: dict, index_ticker: str, market_name: str,
                    data_dir: str = "data") -> pd.DataFrame:
    """
    Descarga ticker por ticker con delays anti-rate-limit.
    Combina todo en un DataFrame de cierres.
    Si más del 50% falla → usa fallback CSV.
    """
    start, end = _get_period()
    session = _make_session()
    all_tickers = {**tickers, index_ticker: _index_display_name(market_name)}
 
    series_list = []
    total = len(all_tickers)
    ok = 0
 
    logger.info(f"[{market_name}] Descargando {total} tickers individualmente...")
 
    for i, (ticker, name) in enumerate(all_tickers.items()):
        serie = _download_single_ticker(ticker, start, end, session, market_name)
 
        if serie is not None:
            # Renombrar con nombre legible
            serie.name = name
            series_list.append(serie)
            ok += 1
            logger.info(f"[{market_name}] ✓ {ticker} ({ok}/{total})")
        else:
            logger.warning(f"[{market_name}] ✗ {ticker} sin datos")
 
        # Delay entre tickers (excepto el último)
        if i < total - 1:
            delay = random.uniform(DELAY_MIN, DELAY_MAX)
            time.sleep(delay)
 
        # Rotar user agent cada 5 tickers
        if (i + 1) % 5 == 0:
            session.headers["User-Agent"] = random.choice(USER_AGENTS)
 
    success_rate = ok / total if total > 0 else 0
    logger.info(f"[{market_name}] Descarga completada: {ok}/{total} tickers ({success_rate:.0%})")
 
    if success_rate >= MIN_SUCCESS_RATE and series_list:
        df = pd.concat(series_list, axis=1)
        df.index.name = "Fecha"
        df = df.sort_index().dropna(how="all")
        logger.info(f"[{market_name}] ✓ DataFrame: {len(df)} días, {len(df.columns)} columnas")
        return df
 
    # Tasa de éxito insuficiente → fallback
    logger.error(f"[{market_name}] Tasa de éxito {success_rate:.0%} < {MIN_SUCCESS_RATE:.0%} — usando fallback")
    fallback = _load_fallback(market_name, data_dir)
    if fallback is not None and len(fallback) >= MIN_ROWS:
        return fallback
 
    raise RuntimeError(
        f"[{market_name}] Descarga fallida ({ok}/{total} tickers) y sin CSV de respaldo. "
        f"El pipeline se detiene para evitar publicar un informe vacío."
    )
 
 
# ─────────────────────────────────────────────
# Punto de entrada público
# ─────────────────────────────────────────────
 
def download_all(data_dir: str = "data") -> dict:
    results = {}
    results["merval"]  = _download_batch(MERVAL_TICKERS,  MERVAL_INDEX,  "MERVAL",  data_dir)
    results["bovespa"] = _download_batch(BOVESPA_TICKERS, BOVESPA_INDEX, "BOVESPA", data_dir)
    results["sp500"]   = _download_batch(SP500_TICKERS,   SP500_INDEX,   "SP500",   data_dir)
    return results
 
 
def save_csvs(data: dict, output_dir: str = "data") -> dict:
    os.makedirs(output_dir, exist_ok=True)
    paths = {}
    for market, df in data.items():
        path = _csv_path(market, output_dir)
        df.to_csv(path, sep=";", decimal=",", encoding="utf-8-sig")
        paths[market] = path
        logger.info(f"Guardado: {path} ({len(df)} filas)")
    return paths
 
