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
    "YPFD.BA": "YPF",
    "BBAR.BA": "BBVA Argentina",
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
    "B3SA3.SA":  "B3 - Brasil Bolsa Balcao",
    "EMBR3.SA":  "Embraer",
    "JBSS3.SA":  "JBS",
    "ITSA4.SA":  "Itausa",
    "SANB11.SA": "Santander Brasil",
    "VIVT3.SA":  "Vivo Telefonica Brasil",
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
    "COPX": "Global X Copper Miners ETF",
    "GLOB": "Globant",
    "IBB":  "iShares Biotechnology ETF",
    "MELI": "MercadoLibre",
    "RIO":  "Rio Tinto",
    "PBR":  "Petrobras ADR",
    "QCOM": "Qualcomm",
    "EWZ":  "iShares MSCI Brazil ETF",
    "NFLX": "Netflix",
    "MA":   "Mastercard",
    "AMD":  "Advanced Micro Devices",
    "PLTR": "Palantir",
    "HD":   "Home Depot",
    "CRM":  "Salesforce",
    "BRK-B":"Berkshire Hathaway",
    "ORCL": "Oracle",
}
SP500_INDEX = "^GSPC"

# CEDEARs del portfolio — precios en ARS desde BYMA via Yahoo Finance
CEDEAR_TICKERS = {
    "MELI.BA":  "MercadoLibre",
    "MSFT.BA":  "Microsoft",
    "COPX.BA":  "Global X Copper Miners ETF",
    "IBB.BA":   "iShares Biotechnology ETF",
    "GLOB.BA":  "Globant",
    "PBR.BA":   "Petrobras ADR",
    "QCOM.BA":  "Qualcomm",
    "RIO.BA":   "Rio Tinto",
    "EWZ.BA":   "iShares MSCI Brazil ETF",
    "HAPV3.BA": "Hapvida",
}
 
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
    """
    Descarga un ticker individual con retry.

    Fix 27/07/2026 (roadmap externo P6, revisión de otra IA sobre v17):
    yf.Ticker(...).history() YA trae High/Low junto con Close en la misma
    llamada (no hace falta pedirlos aparte, cero requests extra a Yahoo /
    cero riesgo adicional de rate limit) -- el código anterior los descartaba
    sin usarlos. Devuelve las 3 series para que download_market() pueda armar
    3 CSVs (Close/High/Low) en vez de solo uno. Devuelve (None, None, None)
    si falla, para que el caller pueda seguir iterando sin romperse.
    """
    for attempt in range(2):
        try:
            t    = yf.Ticker(ticker)
            hist = t.history(start=start, end=end, auto_adjust=True)
            if not hist.empty and len(hist) >= MIN_ROWS:
                idx = pd.to_datetime(hist.index).tz_localize(None)

                serie = hist["Close"].copy()
                serie.index = idx
                serie.name  = ticker

                serie_high = hist["High"].copy()
                serie_high.index = idx
                serie_high.name  = ticker

                serie_low = hist["Low"].copy()
                serie_low.index = idx
                serie_low.name  = ticker

                return serie, serie_high, serie_low
            if attempt == 0:
                time.sleep(random.uniform(3, 6))
        except Exception as e:
            logger.warning(f"[{market_name}] {ticker} error: {e}")
            if attempt == 0:
                time.sleep(random.uniform(5, 10))
    return None, None, None
 
 
def download_market(tickers, index_ticker, market_name):
    start, end   = get_period()
    all_tickers  = {**tickers, index_ticker: index_display_name(market_name)}
    series_list      = []
    series_high_list = []
    series_low_list  = []
    ok           = 0
    total        = len(all_tickers)

    logger.info(f"[{market_name}] Descargando {total} tickers individualmente...")

    for i, (ticker, name) in enumerate(all_tickers.items()):
        serie, serie_high, serie_low = download_single(ticker, start, end, market_name)
        if serie is not None:
            serie.name = name
            series_list.append(serie)
            serie_high.name = name
            series_high_list.append(serie_high)
            serie_low.name = name
            series_low_list.append(serie_low)
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

    # Fix 27/07/2026 (roadmap externo P6): High/Low reales, mismo índice de
    # fechas que Close. Si por algún motivo alguna lista quedó vacía (no
    # debería pasar -- viene del mismo hist que Close), se devuelve None en
    # vez de un DataFrame vacío, para que el caller lo trate como "no
    # disponible" en vez de un CSV corrupto de 0 columnas.
    df_high = None
    if series_high_list:
        df_high = pd.concat(series_high_list, axis=1)
        df_high.index.name = "Fecha"
        df_high = df_high.sort_index().dropna(how="all")

    df_low = None
    if series_low_list:
        df_low = pd.concat(series_low_list, axis=1)
        df_low.index.name = "Fecha"
        df_low = df_low.sort_index().dropna(how="all")

    return df, df_high, df_low
 
 
def download_market_no_index(tickers, market_name):
    """Descarga tickers sin índice (CEDEARs)."""
    start, end  = get_period()
    series_list = []
    ok = 0
    total = len(tickers)
    logger.info(f"[{market_name}] Descargando {total} tickers...")
    for i, (ticker, name) in enumerate(tickers.items()):
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
    if not series_list:
        raise RuntimeError(f"[{market_name}] Sin datos")
    df = pd.concat(series_list, axis=1)
    df.index.name = "Fecha"
    df = df.sort_index().dropna(how="all")
    logger.info(f"[{market_name}] DataFrame: {len(df)} días, {len(df.columns)} columnas")
    return df


def save_csv(df, market_name, suffix="cierres"):
    os.makedirs(DATA_DIR, exist_ok=True)
    path = f"{DATA_DIR}/{market_name.lower()}_{suffix}.csv"
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
        ("CEDEAR",  CEDEAR_TICKERS,  None),
    ]:
        try:
            if index is None:
                # CEDEARs: sin índice separado, sin cambios (pricing real vía
                # data912.com aparte, no necesita High/Low de Yahoo acá)
                df = download_market_no_index(tickers, market)
                save_csv(df, market)
                status["markets"][market] = {
                    "rows":      len(df),
                    "last_date": str(df.index[-1].date()),
                    "ok":        True,
                }
            else:
                df, df_high, df_low = download_market(tickers, index, market)
                save_csv(df, market)  # merval/bovespa/sp500_cierres.csv — SIN CAMBIOS de formato

                # Fix 27/07/2026 (roadmap externo P6): High/Low reales además
                # del Close de siempre. Archivos NUEVOS y SEPARADOS
                # (merval_high.csv, merval_low.csv, etc.) — el CSV de cierres
                # que ya consumen downloader.py/analyzer.py/data_validator.py/
                # generator.py queda exactamente igual, cero riesgo de romper
                # el parsing "1 columna = 1 ticker" que esos 4 módulos asumen.
                # No son críticos: si por algún motivo faltaran, analyzer.py
                # sigue funcionando con el proxy close-only de siempre.
                if df_high is not None:
                    save_csv(df_high, market, suffix="high")
                if df_low is not None:
                    save_csv(df_low, market, suffix="low")

                status["markets"][market] = {
                    "rows":      len(df),
                    "last_date": str(df.index[-1].date()),
                    "ok":        True,
                    "ohlc":      df_high is not None and df_low is not None,
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
 
