"""
scripts/download_data.py
Corre en GitHub Actions a las 14:50 UTC.
Descarga precios de Yahoo Finance y guarda CSVs en data/.
Railway los lee a las 15:00 UTC sin necesidad de conectarse a Yahoo.
"""
 
import yfinance as yf
import pandas as pd
import numpy as np
from datetime import datetime, timedelta
import logging
import json
import os
import sys
import pytz
 
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    handlers=[logging.StreamHandler(sys.stdout)]
)
logger = logging.getLogger(__name__)
 
# ── Tickers (deben coincidir exactamente con downloader.py) ──────────
 
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
 
MIN_ROWS = 10
DATA_DIR = "data"
 
 
def get_period():
    end   = datetime.now(pytz.UTC)
    start = end - timedelta(days=400)
    return start.strftime("%Y-%m-%d"), end.strftime("%Y-%m-%d")
 
 
def index_display_name(market):
    return {
        "MERVAL":  "ÍNDICE MERVAL",
        "BOVESPA": "ÍNDICE BOVESPA",
        "SP500":   "ÍNDICE S&P 500",
    }.get(market, market)
 
 
def download_market(tickers, index_ticker, market_name):
    start, end = get_period()
    all_tickers = list(tickers.keys()) + [index_ticker]
 
    logger.info(f"[{market_name}] Descargando {len(all_tickers)} tickers desde {start}...")
 
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
 
    # Renombrar a nombres legibles
    rename_map = {t: n for t, n in tickers.items() if t in closes.columns}
    idx_name = index_display_name(market_name)
    if index_ticker in closes.columns:
        rename_map[index_ticker] = idx_name
 
    closes = closes.rename(columns=rename_map)
    closes.index = pd.to_datetime(closes.index)
    closes.index.name = "Fecha"
    closes = closes.sort_index().dropna(how="all")
 
    if len(closes) < MIN_ROWS:
        raise RuntimeError(
            f"[{market_name}] DataFrame vacío ({len(closes)} filas). "
            f"Yahoo Finance no respondió correctamente."
        )
 
    logger.info(f"[{market_name}] ✓ {len(closes)} días, {len(closes.columns)} columnas")
    return closes
 
 
def save_csv(df, market_name):
    os.makedirs(DATA_DIR, exist_ok=True)
    path = f"{DATA_DIR}/{market_name.lower()}_cierres.csv"
    df.to_csv(path, sep=";", decimal=",", encoding="utf-8-sig")
    logger.info(f"Guardado: {path}")
    return path
 
 
def main():
    logger.info("=== GitHub Actions: Descarga de datos ===")
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
                "rows": len(df),
                "last_date": str(df.index[-1].date()),
                "ok": True,
            }
        except Exception as e:
            logger.error(f"[{market}] ERROR: {e}")
            errors.append(str(e))
            status["markets"][market] = {"ok": False, "error": str(e)}
 
    status["success"] = len(errors) == 0
 
    # Guardar estado para que Railway pueda verificarlo
    os.makedirs(DATA_DIR, exist_ok=True)
    with open(f"{DATA_DIR}/download_status.json", "w") as f:
        json.dump(status, f, ensure_ascii=False, indent=2)
 
    if errors:
        logger.error(f"=== Descarga con errores: {errors} ===")
        sys.exit(1)  # Hace que el workflow de GitHub Actions marque FAILED
    else:
        logger.info("=== Descarga completada exitosamente ===")
 
 
if __name__ == "__main__":
    main()
 
