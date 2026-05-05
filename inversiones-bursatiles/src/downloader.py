import pandas as pd
import numpy as np
from datetime import datetime, timedelta
import logging
import json
import os
import pytz
 
logger = logging.getLogger(__name__)
 
# ─────────────────────────────────────────────
# Tickers por mercado (se usan en analyzer.py)
# ─────────────────────────────────────────────
 
MERVAL_TICKERS = {
    "GGAL.BA":  "Grupo Financiero Galicia",
    "BMA.BA":   "Banco Macro",
    "PAMP.BA":  "Pampa Energia",
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
    "MOLI.BA":  "Molinos Rio de la Plata",
    "BYMA.BA":  "BYMA",
    "IRSA.BA":  "IRSA",
}
MERVAL_INDEX = "^MERV"
 
BOVESPA_TICKERS = {
    "PETR4.SA":  "Petrobras PN",
    "VALE3.SA":  "Vale",
    "ITUB4.SA": "Itau Unibanco",
    "BBDC4.SA":  "Bradesco",
    "ABEV3.SA":  "Ambev",
    "WEGE3.SA":  "WEG",
    "RENT3.SA":  "Localiza",
    "RDOR3.SA": "Rede D Or",
    "BBAS3.SA":  "Banco do Brasil",
    "MGLU3.SA":  "Magazine Luiza",
    "SUZB3.SA":  "Suzano",
    "EQTL3.SA":  "Equatorial",
    "RAIZ4.SA": "Raizen",
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
 
MIN_ROWS    = 10
MAX_AGE_DAYS = 3   # Si el CSV tiene más de 3 días → advertencia (puede ser finde/feriado)
 
 
# ─────────────────────────────────────────────
# Lectura de CSVs
# ─────────────────────────────────────────────
 
def _load_csv(market: str, data_dir: str) -> pd.DataFrame:
    """
    Lee el CSV generado por GitHub Actions.
    Valida existencia, tamaño mínimo y frescura.
    """
    path = os.path.join(data_dir, f"{market.lower()}_cierres.csv")
 
    # ── Existencia ───────────────────────────────────────────────────
    if not os.path.exists(path):
        raise RuntimeError(
            f"[{market}] CSV no encontrado: {path}\n"
            f"GitHub Actions aún no corrió o falló. "
            f"Verificá en https://github.com/Brunogatti79/inversiones-bursatiles/actions"
        )
 
    # ── Lectura ──────────────────────────────────────────────────────
    try:
        df = pd.read_csv(
            path,
            sep=";",
            decimal=",",
            index_col=0,
            encoding="utf-8-sig",
        )
    except Exception as e:
        raise RuntimeError(f"[{market}] Error leyendo CSV {path}: {e}")
 
    # Limpiar columnas numéricas (miles con espacio, decimal con coma)
    for col in df.columns:
        df[col] = (
            df[col].astype(str)
            .str.replace(" ", "", regex=False)
            .str.replace(",", ".", regex=False)
        )
        df[col] = pd.to_numeric(df[col], errors="coerce")
 
    df.index = pd.to_datetime(df.index, errors="coerce")
    df.index.name = "Fecha"
    df = df.sort_index().dropna(how="all")
 
    # ── Validación de tamaño ─────────────────────────────────────────
    if len(df) < MIN_ROWS:
        raise RuntimeError(
            f"[{market}] CSV con solo {len(df)} filas — datos insuficientes."
        )
 
    # ── Validación de frescura ───────────────────────────────────────
    last_date  = df.index[-1].date()
    today      = datetime.now(pytz.UTC).date()
    age_days   = (today - last_date).days
    if age_days > MAX_AGE_DAYS:
        logger.warning(
            f"[{market}] CSV desactualizado: último dato {last_date} "
            f"({age_days} días atrás). GitHub Actions puede haber fallado."
        )
    else:
        logger.info(f"[{market}] ✓ CSV OK — {len(df)} filas, último: {last_date}")
 
    return df
 
 
def download_all(data_dir: str = "data") -> dict:
    """
    Lee los CSVs descargados por GitHub Actions.
    Retorna dict con keys 'merval', 'bovespa', 'sp500'.
    """
    # Verificar status de GitHub Actions si existe
    status_path = os.path.join(data_dir, "download_status.json")
    if os.path.exists(status_path):
        with open(status_path) as f:
            st = json.load(f)
        ts = st.get("timestamp_utc", "desconocido")
        ok = st.get("success", False)
        if not ok:
            logger.warning(f"download_status.json indica fallo en descarga de {ts}")
        else:
            logger.info(f"Datos descargados por GitHub Actions el {ts}")
 
    results = {}
    results["merval"]  = _load_csv("merval",  data_dir)
    results["bovespa"] = _load_csv("bovespa", data_dir)
    results["sp500"]   = _load_csv("sp500",   data_dir)
    return results
 
 
def save_csvs(data: dict, output_dir: str = "data") -> dict:
    """
    En esta arquitectura los CSVs ya existen (los escribió GitHub Actions).
    Esta función es un no-op que retorna las rutas para compatibilidad
    con pipeline.py.
    """
    paths = {}
    for market in data:
        path = os.path.join(output_dir, f"{market.lower()}_cierres.csv")
        paths[market] = path
        logger.debug(f"CSV ya existe en {path} (escrito por GitHub Actions)")
    return paths
 
