"""
src/downloader.py  ── versión con retry + fallback a CSV guardado
Descarga automática de precios desde Yahoo Finance.
Cubre MERVAL (Argentina), BOVESPA (Brasil) y S&P 500 (EE.UU.).
 
CAMBIOS v2:
  - retry_with_backoff: 3 intentos con espera 30s entre cada uno
  - Validación explícita: si el DataFrame queda vacío → lanza RuntimeError
  - Fallback: si la descarga falla y existe un CSV del día anterior, lo levanta
  - Log detallado de errores HTTP para Railway
"""
 
import yfinance as yf
import pandas as pd
import numpy as np
from datetime import datetime, timedelta
import logging
import time
import os
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
# Configuración de retry
# ─────────────────────────────────────────────
 
MAX_RETRIES   = 3       # intentos totales
RETRY_WAIT_S  = 30      # segundos entre intentos
MIN_ROWS      = 10      # mínimo de filas para considerar descarga válida
 
 
# ─────────────────────────────────────────────
# Helpers
# ─────────────────────────────────────────────
 
def _get_period():
    """Retorna start/end para los últimos 13 meses (buffer de 1 mes)."""
    end   = datetime.now(pytz.UTC)
    start = end - timedelta(days=400)
    return start.strftime("%Y-%m-%d"), end.strftime("%Y-%m-%d")
 
 
def _index_display_name(market: str) -> str:
    mapping = {
        "MERVAL":  "ÍNDICE MERVAL",
        "BOVESPA": "ÍNDICE BOVESPA",
        "SP500":   "ÍNDICE S&P 500",
    }
    return mapping.get(market, market)
 
 
def _csv_path(market: str, data_dir: str) -> str:
    return os.path.join(data_dir, f"{market.lower()}_cierres.csv")
 
 
def _load_fallback(market: str, data_dir: str) -> pd.DataFrame | None:
    """Carga el CSV guardado del último run exitoso como fallback."""
    path = _csv_path(market, data_dir)
    if not os.path.exists(path):
        return None
    try:
        df = pd.read_csv(path, sep=";", decimal=",", index_col=0,
                         encoding="utf-8-sig", thousands=" ")
        df.index = pd.to_datetime(df.index)
        df.index.name = "Fecha"
        # Limpiar columnas numéricas
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
# Descarga con retry
# ─────────────────────────────────────────────
 
def _download_batch(tickers: dict, index_ticker: str, market_name: str,
                    data_dir: str = "data") -> pd.DataFrame:
    """
    Descarga precios de cierre ajustados para un conjunto de tickers + índice.
    Reintenta hasta MAX_RETRIES veces con espera RETRY_WAIT_S entre intentos.
    Si todos los intentos fallan → intenta levantar CSV guardado.
    Si tampoco existe → lanza RuntimeError.
    """
    start, end = _get_period()
    all_tickers = list(tickers.keys()) + [index_ticker]
    last_error  = None
 
    for attempt in range(1, MAX_RETRIES + 1):
        logger.info(f"[{market_name}] Intento {attempt}/{MAX_RETRIES} — "
                    f"descargando {len(all_tickers)} tickers desde {start}...")
        try:
            raw = yf.download(
                tickers=all_tickers,
                start=start,
                end=end,
                auto_adjust=True,
                progress=False,
                threads=False,   # threads=False evita condición de carrera en Railway
            )
 
            # Extraer columna Close
            if isinstance(raw.columns, pd.MultiIndex):
                closes = raw["Close"].copy()
            else:
                closes = raw[["Close"]].rename(columns={"Close": all_tickers[0]}).copy()
 
            # Renombrar columnas
            rename_map = {t: n for t, n in tickers.items() if t in closes.columns}
            index_col_name = _index_display_name(market_name)
            if index_ticker in closes.columns:
                rename_map[index_ticker] = index_col_name
            closes = closes.rename(columns=rename_map)
 
            closes.index = pd.to_datetime(closes.index)
            closes.index.name = "Fecha"
            closes = closes.sort_index().dropna(how="all")
 
            # ── VALIDACIÓN CRÍTICA ──────────────────────────────────
            if len(closes) < MIN_ROWS:
                msg = (f"[{market_name}] DataFrame vacío tras descarga "
                       f"(filas={len(closes)}). "
                       f"Posible bloqueo 403 de Yahoo Finance.")
                logger.warning(msg)
                last_error = RuntimeError(msg)
                if attempt < MAX_RETRIES:
                    logger.info(f"[{market_name}] Esperando {RETRY_WAIT_S}s antes del próximo intento...")
                    time.sleep(RETRY_WAIT_S)
                continue
            # ───────────────────────────────────────────────────────
 
            logger.info(f"[{market_name}] ✓ Descarga OK — "
                        f"{len(closes)} días, {len(closes.columns)} columnas")
            return closes
 
        except Exception as e:
            last_error = e
            logger.warning(f"[{market_name}] Error en intento {attempt}: {e}")
            if attempt < MAX_RETRIES:
                logger.info(f"[{market_name}] Esperando {RETRY_WAIT_S}s...")
                time.sleep(RETRY_WAIT_S)
 
    # Todos los intentos fallaron → intentar fallback
    logger.error(f"[{market_name}] ✗ Todos los intentos fallaron. Último error: {last_error}")
    fallback = _load_fallback(market_name, data_dir)
    if fallback is not None and len(fallback) >= MIN_ROWS:
        return fallback
 
    # Sin fallback disponible → error explícito (nunca genera HTML en blanco)
    raise RuntimeError(
        f"[{market_name}] Descarga fallida y sin CSV de respaldo disponible. "
        f"El pipeline se detiene para evitar publicar un informe vacío. "
        f"Error original: {last_error}"
    )
 
 
# ─────────────────────────────────────────────
# Punto de entrada público
# ─────────────────────────────────────────────
 
def download_all(data_dir: str = "data") -> dict:
    """
    Punto de entrada principal.
    Retorna dict con keys 'merval', 'bovespa', 'sp500' → DataFrames de cierres.
    Propaga RuntimeError si algún mercado no pudo descargarse ni tiene fallback.
    """
    results = {}
    results["merval"]  = _download_batch(MERVAL_TICKERS,  MERVAL_INDEX,  "MERVAL",  data_dir)
    results["bovespa"] = _download_batch(BOVESPA_TICKERS, BOVESPA_INDEX, "BOVESPA", data_dir)
    results["sp500"]   = _download_batch(SP500_TICKERS,   SP500_INDEX,   "SP500",   data_dir)
    return results
 
 
def save_csvs(data: dict, output_dir: str = "data") -> dict:
    """Guarda los DataFrames como CSV compatibles con el formato del proyecto."""
    os.makedirs(output_dir, exist_ok=True)
    paths = {}
    for market, df in data.items():
        path = _csv_path(market, output_dir)
        df.to_csv(path, sep=";", decimal=",", encoding="utf-8-sig")
        paths[market] = path
        logger.info(f"Guardado: {path} ({len(df)} filas)")
    return paths
