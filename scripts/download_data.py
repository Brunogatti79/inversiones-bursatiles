"""
scripts/download_data.py
Descarga precios desde Yahoo Finance para GitHub Actions.
Usa descarga individual por ticker con delays para evitar rate limit.
"""
 
import yfinance as yf
import pandas as pd
from datetime import datetime, timedelta
import logging
import json
import os
import sys
import time
import random
import pytz
 
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    handlers=[logging.StreamHandler(sys.stdout)]
)
logger = logging.getLogger(__name__)
 
MERVAL_TICKERS = {
    "GGAL.BA": "Grupo Financiero Galicia",
    "BMA.BA": "Banco Macro",
    "PAMP.BA": "Pampa Energia",
    "TXAR.BA": "Ternium Argentina",
    "ALUA.BA": "Aluar",
    "CRES.BA": "Cresud",
    "SUPV.BA": "Supervielle",
    "CEPU.BA": "Central Puerto",
    "LOMA.BA": "Loma Negra",
    "MIRG.BA": "Mirgor",
    "TECO2.BA": "Telecom Argentina",
    "TGSU2.BA": "Transportadora Gas del Sur",
    "VALO.BA": "Grupo Supervielle (VALO)",
    "COME.BA": "Soc. Comercial del Plata",
    "EDN.BA": "Edenor",
    "HARG.BA": "Holcim Argentina",
    "TRAN.BA": "Transener",
    "MOLI.BA": "Molinos Rio de la Plata",
    "BYMA.BA": "BYMA",
    "IRSA.BA": "IRSA",
}
MERVAL_INDEX = "^MERV"
 
BOVESPA_TICKERS = {
    "PETR4.SA": "Petrobras PN",
    "VALE3.SA": "Vale",
    "ITUB4.SA": "Itau Unibanco",
    "BBDC4.SA": "Bradesco",
    "ABEV3.SA": "Ambev",
    "WEGE3.SA": "WEG",
    "RENT3.SA": "Localiza",
    "RDOR3.SA": "Rede D Or",
    "BBAS3.SA": "Banco do Brasil",
    "MGLU3.SA": "Magazine Luiza",
    "SUZB3.SA": "Suzano",
    "EQTL3.SA": "Equatorial",
    "RAIZ4.SA": "Raizen",
    "HAPV3.SA": "Hapvida",
    "LREN3.SA": "Lojas Renner",
    "CSNA3.SA": "CSN",
    "CYRE3.SA": "Cyrela",
    "EGIE3.SA": "Engie Brasil",
    "BPAC11.SA": "BTG Pactual",
}
BOVESPA_INDEX = "^BVSP"
 
SP500_TICKERS = {
    "AAPL": "Apple",
    "MSFT": "Microsoft",
    "NVDA": "NVIDIA",
    "GOOGL": "Alphabet (Google)",
    "META": "Meta Platforms",
    "AMZN": "Amazon",
    "JPM": "JPMorgan Chase",
    "BAC": "Bank of America",
    "GS": "Goldman Sachs",
    "V": "Visa",
    "XOM": "ExxonMobil",
    "CVX": "Chevron",
    "JNJ": "Johnson & Johnson",
    "UNH": "UnitedHealth",
    "LLY": "Eli Lilly",
    "WMT": "Walmart",
    "PG": "Procter & Gamble",
    "KO": "Coca-Cola",
    "MCD": "McDonalds",
    "CAT": "Caterpillar",
    "BA": "Boeing",
    "GE": "GE Aerospace",
    "TSLA": "Tesla",
}
SP500_INDEX = "^GSPC"
 
MIN_ROWS         = 10
MIN_SUCCESS_RATE = 0.5
DELAY_MIN        = 1.5
DELAY_MAX        = 3.5
DATA_DIR         = "data"
 
 
def get_period():
    end   = datetime.now(pytz.UTC)
    start = end - timedelta(days=400)
    return start.strftime("%Y-%m-%d"), end.strftime("%Y-%m-%d")
 
 
def index_display_name(market):
    return {
        "MERVAL":  "INDICE MERVAL",
        "BOVESPA": "INDICE BOVESPA",
        "SP500":   "INDICE S&P 500",
    }.get(market, market)
 
 
def download_single(ticker, start, end, market_name):
    """Descarga un ticker individual con retry."""
    for attempt in range(2):
        try:
            t    = yf.Ticker(ticker)
            hist = t.history(start=start, end=end, auto_adjust=True)
            if not hist.empty and len(hist) >= MIN_ROWS:
                serie = hist["Close"].copy()
                serie.index = pd.to_datetime(serie.index).tz_localize(None)
                serie.name  = ticker
                return serie
            if attempt == 0:
                time.sleep(random.uniform(3, 6))
        except Exception as e:
            logger.warning(f"[{market_name}] {ticker} error: {e}")
            if attempt == 0:
                time.sleep(random.uniform(5, 10))
    return None
 
 
def download_market(tickers, index_ticker, market_name):
    start, end   = get_period()
    all_tickers  = {**tickers, index_ticker: index_display_name(market_name)}
    series_list  = []
    ok           = 0
    total        = len(all_tickers)
 
    logger.info(f"[{market_name}] Descargando {total} tickers individualmente...")
 
    for i, (ticker, name) in enumerate(all_tickers.items()):
        serie = download_single(ticker, start, end, market_name)
        if serie is not None:
            serie.name = name
            series_list.append(serie)
            ok += 1
            logger.info(f"[{market_name}] ✓ {ticker} ({ok}/{total})")
        else:
            logger.warning(f"[{market_name}] ✗ {ticker} sin datos")
 
        if i < total - 1:
            time.sleep(random.uniform(DELAY_MIN, DELAY_MAX))
 
    rate = ok / total if total > 0 else 0
    logger.info(f"[{market_name}] Completado: {ok}/{total} ({rate:.0%})")
 
    if rate < MIN_SUCCESS_RATE or not series_list:
        raise RuntimeError(f"[{market_name}] Tasa de éxito insuficiente: {ok}/{total}")
 
    df = pd.concat(series_list, axis=1)
    df.index.name = "Fecha"
    df = df.sort_index().dropna(how="all")
    logger.info(f"[{market_name}] DataFrame: {len(df)} días, {len(df.columns)} columnas")
    return df
 
 
def save_csv(df, market_name):
    os.makedirs(DATA_DIR, exist_ok=True)
    path = f"{DATA_DIR}/{market_name.lower()}_cierres.csv"
    df.to_csv(path, sep=";", decimal=",", encoding="utf-8-sig")
    logger.info(f"Guardado: {path}")
    return path
 
 
def main():
    logger.info("=== GitHub Actions: Descarga de datos (ticker individual) ===")
    status = {
        "timestamp_utc": datetime.now(pytz.UTC).strftime("%Y-%m-%d %H:%M UTC"),
        "markets": {},
        "success": False,
    }
    errors = []
 
    for market, tickers, index in [
        ("MERVAL",  MERVAL_TICKERS,  MERVAL_INDEX),
        ("BOVESPA", BOVESPA_TICKERS, BOVESPA_INDEX),
        ("SP500",   SP500_TICKERS,   SP500_INDEX),
    ]:
        try:
            df = download_market(tickers, index, market)
            save_csv(df, market)
            status["markets"][market] = {
                "rows":      len(df),
                "last_date": str(df.index[-1].date()),
                "ok":        True,
            }
        except Exception as e:
            logger.error(f"[{market}] ERROR: {e}")
            errors.append(str(e))
            status["markets"][market] = {"ok": False, "error": str(e)}
 
    status["success"] = len(errors) == 0
    os.makedirs(DATA_DIR, exist_ok=True)
    with open(f"{DATA_DIR}/download_status.json", "w") as f:
        json.dump(status, f, ensure_ascii=False, indent=2)
 
    if errors:
        logger.warning(f"=== Descarga con errores parciales: {errors} ===")
        # No hacer sys.exit(1) para que el workflow haga commit de lo que se pudo bajar
    else:
        logger.info("=== Descarga completada exitosamente ===")
 
 
if __name__ == "__main__":
    main()
 
