"""
src/macro_auto.py — Carga automática de datos macro desde APIs públicas

APIs utilizadas:
- FRED (USA): Fed Funds, CPI, desempleo, GDP, confianza, PCE Core, ISM
- BCRA (Argentina): tasa, reservas, tipo de cambio
- BCB/SGS (Brasil): SELIC, IPCA, desempleo, reservas, BRL/USD
- Yahoo Finance: DXY, HY spread (via tickers)
- Ámbito/web: riesgo país ARG/BRA (EMBI)

Retorna dict compatible con macro_loader.py y analyzer.py
"""

import os
import logging
import json
import requests
import numpy as np
from datetime import datetime, timedelta

logger = logging.getLogger(__name__)

FRED_API_KEY = os.getenv("FRED_API_KEY", "")
CACHE_PATH = "data/macro_auto_cache.json"

# ─────────────────────────────────────────────
# FRED (USA) — Federal Reserve Economic Data
# ─────────────────────────────────────────────

FRED_SERIES = {
    "fed_funds":    "FEDFUNDS",       # Federal Funds Rate
    "cpi":          "CPIAUCSL",       # CPI (YoY se calcula)
    "unemployment": "UNRATE",         # Unemployment Rate
    "gdp_growth":   "A191RL1Q225SBEA",# Real GDP Growth (quarterly)
    "consumer_conf":"UMCSENT",        # U. Michigan Consumer Sentiment
    "pce_core":     "PCEPILFE",       # PCE Core (YoY se calcula)
    "ism_mfg":      "MANEMP",         # ISM Manufacturing (proxy via NAPM)
}

# Series adicionales de FRED para DXY y HY spread
FRED_EXTRA = {
    "dxy":          "DTWEXBGS",       # Trade Weighted USD Index
    "hy_spread":    "BAMLH0A0HYM2",  # ICE BofA HY Spread
}


def _fred_latest(series_id, api_key=None):
    """Obtiene el último valor de una serie FRED."""
    key = api_key or FRED_API_KEY
    if not key:
        return None, None
    try:
        url = f"https://api.stlouisfed.org/fred/series/observations"
        params = {
            "series_id": series_id,
            "api_key": key,
            "file_type": "json",
            "sort_order": "desc",
            "limit": 5,
        }
        r = requests.get(url, params=params, timeout=15)
        r.raise_for_status()
        obs = r.json().get("observations", [])
        for o in obs:
            val = o.get("value", ".")
            if val != ".":
                return float(val), o.get("date", "")
        return None, None
    except Exception as e:
        logger.warning(f"FRED error [{series_id}]: {e}")
        return None, None


def _fred_yoy(series_id, api_key=None):
    """Calcula YoY % change para series como CPI, PCE."""
    key = api_key or FRED_API_KEY
    if not key:
        return None
    try:
        url = f"https://api.stlouisfed.org/fred/series/observations"
        params = {
            "series_id": series_id,
            "api_key": key,
            "file_type": "json",
            "sort_order": "desc",
            "limit": 15,
            "units": "pc1",  # percent change from year ago
        }
        r = requests.get(url, params=params, timeout=15)
        r.raise_for_status()
        obs = r.json().get("observations", [])
        for o in obs:
            val = o.get("value", ".")
            if val != ".":
                return round(float(val), 2)
        return None
    except Exception as e:
        logger.warning(f"FRED YoY error [{series_id}]: {e}")
        return None


def fetch_usa_macro():
    """Obtiene todas las variables macro de USA desde FRED."""
    data = {}

    # Fed Funds Rate
    val, dt = _fred_latest("FEDFUNDS")
    data["fed_funds"] = {"valor": val, "fecha": dt}

    # CPI YoY
    cpi_yoy = _fred_yoy("CPIAUCSL")
    data["cpi"] = {"valor": cpi_yoy, "fecha": ""}

    # Unemployment
    val, dt = _fred_latest("UNRATE")
    data["unemployment"] = {"valor": val, "fecha": dt}

    # GDP Growth
    val, dt = _fred_latest("A191RL1Q225SBEA")
    data["gdp_growth"] = {"valor": val, "fecha": dt}

    # Consumer Confidence (U. Michigan)
    val, dt = _fred_latest("UMCSENT")
    data["consumer_conf"] = {"valor": val, "fecha": dt}

    # PCE Core YoY
    pce_yoy = _fred_yoy("PCEPILFE")
    data["pce_core"] = {"valor": pce_yoy, "fecha": ""}

    # ISM Manufacturing
    val, dt = _fred_latest("NAPMPI")
    if val is None:
        val, dt = _fred_latest("MANEMP")
    data["ism_mfg"] = {"valor": val, "fecha": dt}

    # DXY
    val, dt = _fred_latest("DTWEXBGS")
    data["dxy"] = {"valor": val, "fecha": dt}

    # HY Spread
    val, dt = _fred_latest("BAMLH0A0HYM2")
    data["hy_spread"] = {"valor": val, "fecha": dt}

    logger.info(f"USA macro: {sum(1 for v in data.values() if v['valor'] is not None)}/{len(data)} variables obtenidas")
    return data


# ─────────────────────────────────────────────
# BCRA (Argentina) — API pública
# ─────────────────────────────────────────────

BCRA_ENDPOINTS = {
    "tasa_plazo_fijo": "https://api.bcra.gob.ar/estadisticas/v2.0/DatosVariable/6",    # TNA plazo fijo 30d
    "reservas":        "https://api.bcra.gob.ar/estadisticas/v2.0/DatosVariable/1",    # Reservas internacionales
    "tipo_cambio":     "https://api.bcra.gob.ar/estadisticas/v2.0/DatosVariable/4",    # TC minorista
}


def _bcra_latest(url):
    """Obtiene el último valor de una variable BCRA."""
    try:
        today = datetime.now().strftime("%Y-%m-%d")
        start = (datetime.now() - timedelta(days=30)).strftime("%Y-%m-%d")
        full_url = f"{url}/{start}/{today}"
        r = requests.get(full_url, timeout=15, verify=False)
        r.raise_for_status()
        results = r.json().get("results", [])
        if results:
            latest = results[-1]
            return float(latest.get("valor", 0)), latest.get("fecha", "")
        return None, None
    except Exception as e:
        logger.warning(f"BCRA error: {e}")
        return None, None


def fetch_argentina_macro():
    """Obtiene variables macro de Argentina."""
    data = {}

    # Tasa plazo fijo
    val, dt = _bcra_latest(BCRA_ENDPOINTS["tasa_plazo_fijo"])
    data["tasa_tamar"] = {"valor": val, "fecha": dt}

    # Reservas
    val, dt = _bcra_latest(BCRA_ENDPOINTS["reservas"])
    data["reservas"] = {"valor": val, "fecha": dt}

    # Tipo de cambio
    val, dt = _bcra_latest(BCRA_ENDPOINTS["tipo_cambio"])
    data["tipo_cambio"] = {"valor": val, "fecha": dt}

    # Riesgo país (EMBI) — scraping Ámbito
    try:
        r = requests.get("https://mercados.ambito.com/riesgo-pais/datos", timeout=10)
        if r.status_code == 200:
            rp_data = r.json()
            if isinstance(rp_data, dict):
                val = float(rp_data.get("ultimo", "0").replace(".", "").replace(",", "."))
                data["riesgo_pais"] = {"valor": val, "fecha": datetime.now().strftime("%Y-%m-%d")}
            else:
                data["riesgo_pais"] = {"valor": None, "fecha": ""}
        else:
            data["riesgo_pais"] = {"valor": None, "fecha": ""}
    except Exception as e:
        logger.warning(f"Riesgo país ARG error: {e}")
        data["riesgo_pais"] = {"valor": None, "fecha": ""}

    # Brecha cambiaria (CCL vs oficial)
    try:
        r = requests.get("https://mercados.ambito.com/dolar/cl/variacion", timeout=10)
        if r.status_code == 200:
            ccl_data = r.json()
            ccl = float(ccl_data.get("compra", "0").replace(".", "").replace(",", "."))
            oficial = data["tipo_cambio"]["valor"] or 1
            if oficial > 0 and ccl > 0:
                brecha = round(((ccl / oficial) - 1) * 100, 1)
                data["brecha"] = {"valor": brecha, "fecha": datetime.now().strftime("%Y-%m-%d")}
            else:
                data["brecha"] = {"valor": None, "fecha": ""}
        else:
            data["brecha"] = {"valor": None, "fecha": ""}
    except Exception as e:
        logger.warning(f"Brecha cambiaria error: {e}")
        data["brecha"] = {"valor": None, "fecha": ""}

    logger.info(f"ARG macro: {sum(1 for v in data.values() if v['valor'] is not None)}/{len(data)} variables obtenidas")
    return data


# ─────────────────────────────────────────────
# BCB/SGS (Brasil) — API pública
# ─────────────────────────────────────────────

BCB_SERIES = {
    "selic":        432,    # Meta SELIC
    "ipca":         433,    # IPCA acumulado 12m
    "desempleo":    24369,  # Taxa de desocupação
    "reservas":     13621,  # Reservas internacionales
    "brl_usd":      1,      # Dólar comercial (venda)
}


def _bcb_latest(series_id):
    """Obtiene el último valor de una serie BCB/SGS."""
    try:
        end = datetime.now().strftime("%d/%m/%Y")
        start = (datetime.now() - timedelta(days=60)).strftime("%d/%m/%Y")
        url = f"https://api.bcb.gov.br/dados/serie/bcdata.sgs.{series_id}/dados?formato=json&dataInicial={start}&dataFinal={end}"
        r = requests.get(url, timeout=15)
        r.raise_for_status()
        data = r.json()
        if data:
            latest = data[-1]
            return float(latest.get("valor", "0").replace(",", ".")), latest.get("data", "")
        return None, None
    except Exception as e:
        logger.warning(f"BCB error [serie {series_id}]: {e}")
        return None, None


def fetch_brasil_macro():
    """Obtiene variables macro de Brasil."""
    data = {}

    for name, series_id in BCB_SERIES.items():
        val, dt = _bcb_latest(series_id)
        data[name] = {"valor": val, "fecha": dt}

    # Riesgo país Brasil (EMBI) — scraping
    try:
        r = requests.get("https://mercados.ambito.com/riesgo-pais-historico/brasil", timeout=10)
        if r.status_code == 200:
            rp_data = r.json()
            if isinstance(rp_data, list) and len(rp_data) > 0:
                val = float(str(rp_data[-1].get("valor", "0")).replace(".", "").replace(",", "."))
                data["riesgo_pais"] = {"valor": val, "fecha": ""}
            else:
                data["riesgo_pais"] = {"valor": None, "fecha": ""}
        else:
            data["riesgo_pais"] = {"valor": None, "fecha": ""}
    except Exception as e:
        logger.warning(f"Riesgo país BRA error: {e}")
        data["riesgo_pais"] = {"valor": None, "fecha": ""}

    logger.info(f"BRA macro: {sum(1 for v in data.values() if v['valor'] is not None)}/{len(data)} variables obtenidas")
    return data


# ─────────────────────────────────────────────
# Normalización y cálculo de scores
# ─────────────────────────────────────────────

# Rangos (peor, mejor) para normalización
RANGES = {
    # Argentina
    "arg_tasa":     (60.0,   5.0),
    "arg_riesgo":   (2000,   100),
    "arg_reservas": (5000,   60000),
    "arg_tc":       (150.0,  70.0),
    "arg_brecha":   (80.0,   0.0),
    # Brasil
    "bra_selic":    (20.0,   5.0),
    "bra_riesgo":   (800,    80),
    "bra_ipca":     (10.0,   0.0),
    "bra_desempleo":(15.0,   3.0),
    "bra_reservas": (100000, 500000),
    "bra_brl":      (7.0,    4.0),
    # USA
    "usa_fed":      (8.0,    0.5),
    "usa_cpi":      (9.0,    0.0),
    "usa_unemp":    (10.0,   2.0),
    "usa_gdp":      (-3.0,   5.0),
    "usa_conf":     (40.0,   140.0),
    "usa_pce":      (6.0,    1.0),
    "usa_hy":       (800,    200),
    "usa_dxy":      (115.0,  90.0),
    "usa_ism":      (40.0,   62.0),
}


def _normalize(valor, peor, mejor):
    """Normaliza a 0-100. Menor es mejor para tasas, mayor es mejor para reservas."""
    if mejor == peor or valor is None:
        return 50.0
    return max(0.0, min(100.0, (valor - peor) / (mejor - peor) * 100.0))


def compute_macro_scores(arg_data, bra_data, usa_data):
    """
    Calcula scores macro normalizados por país.
    Retorna: {"MERVAL": score, "BOVESPA": score, "SP500": score, "timestamp": ..., "detalles": ...}
    """
    timestamp = datetime.now().isoformat()
    detalles = {"ARG": {}, "BRA": {}, "USA": {}}

    # ── Argentina ──
    arg_scores = []
    vars_arg = [
        ("tasa_tamar",  "arg_tasa",     True),   # menor es mejor
        ("riesgo_pais", "arg_riesgo",   True),
        ("reservas",    "arg_reservas", False),  # mayor es mejor
        ("tipo_cambio", "arg_tc",       True),
        ("brecha",      "arg_brecha",   True),
    ]
    for var_name, range_key, invert in vars_arg:
        val = arg_data.get(var_name, {}).get("valor")
        if val is not None:
            peor, mejor = RANGES[range_key]
            s = _normalize(val, peor, mejor)
            if invert:
                s = 100.0 - s
            arg_scores.append(s)
            detalles["ARG"][var_name] = {"valor": val, "score": round(s, 1)}

    arg_macro = round(np.mean(arg_scores), 1) if arg_scores else 42.0

    # ── Brasil ──
    bra_scores = []
    vars_bra = [
        ("selic",       "bra_selic",     True),
        ("riesgo_pais", "bra_riesgo",    True),
        ("ipca",        "bra_ipca",      True),
        ("desempleo",   "bra_desempleo", True),
        ("reservas",    "bra_reservas",  False),
        ("brl_usd",     "bra_brl",       True),
    ]
    for var_name, range_key, invert in vars_bra:
        val = bra_data.get(var_name, {}).get("valor")
        if val is not None:
            peor, mejor = RANGES[range_key]
            s = _normalize(val, peor, mejor)
            if invert:
                s = 100.0 - s
            bra_scores.append(s)
            detalles["BRA"][var_name] = {"valor": val, "score": round(s, 1)}

    bra_macro = round(np.mean(bra_scores), 1) if bra_scores else 52.0

    # ── USA ──
    usa_scores = []
    vars_usa = [
        ("fed_funds",    "usa_fed",    True),
        ("cpi",          "usa_cpi",    True),
        ("unemployment", "usa_unemp",  True),
        ("gdp_growth",   "usa_gdp",    False),
        ("consumer_conf","usa_conf",   False),
        ("pce_core",     "usa_pce",    True),
        ("hy_spread",    "usa_hy",     True),
        ("dxy",          "usa_dxy",    True),
        ("ism_mfg",      "usa_ism",    False),
    ]
    for var_name, range_key, invert in vars_usa:
        val = usa_data.get(var_name, {}).get("valor")
        if val is not None:
            peor, mejor = RANGES[range_key]
            s = _normalize(val, peor, mejor)
            if invert:
                s = 100.0 - s
            usa_scores.append(s)
            detalles["USA"][var_name] = {"valor": val, "score": round(s, 1)}

    usa_macro = round(np.mean(usa_scores), 1) if usa_scores else 45.0

    result = {
        "macro_scores": {
            "MERVAL": arg_macro,
            "BOVESPA": bra_macro,
            "SP500": usa_macro,
        },
        "timestamp": timestamp,
        "variables_obtenidas": {
            "ARG": len(arg_scores),
            "BRA": len(bra_scores),
            "USA": len(usa_scores),
        },
        "detalles": detalles,
    }

    # Cache para debugging y fallback
    try:
        os.makedirs(os.path.dirname(CACHE_PATH), exist_ok=True)
        with open(CACHE_PATH, "w") as f:
            json.dump(result, f, ensure_ascii=False, indent=2, default=str)
    except Exception:
        pass

    logger.info(f"Macro scores auto: ARG={arg_macro} BRA={bra_macro} USA={usa_macro}")
    return result


# ─────────────────────────────────────────────
# Función principal — llamada desde pipeline
# ─────────────────────────────────────────────

def fetch_all_macro():
    """
    Descarga datos macro de los 3 países y calcula scores.
    Si falla alguna API, usa fallback del cache o hardcoded.
    Retorna dict compatible con xlsx_signals format.
    """
    logger.info("Descargando datos macro automáticos...")

    arg_data = fetch_argentina_macro()
    bra_data = fetch_brasil_macro()
    usa_data = fetch_usa_macro()

    result = compute_macro_scores(arg_data, bra_data, usa_data)

    # Formato compatible con lo que espera pipeline/analyzer
    return {
        "macro_scores": result["macro_scores"],
        "macro_timestamp": result["timestamp"],
        "macro_auto": True,
        "detalles": result["detalles"],
    }


def get_cached_macro():
    """Lee el último cache de macro como fallback."""
    try:
        if os.path.exists(CACHE_PATH):
            with open(CACHE_PATH) as f:
                cached = json.load(f)
            return {
                "macro_scores": cached["macro_scores"],
                "macro_timestamp": cached["timestamp"],
                "macro_auto": True,
            }
    except Exception:
        pass
    return None
