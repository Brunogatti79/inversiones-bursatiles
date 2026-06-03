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
import pandas as pd
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
 
# API alternativa estadisticasbcra.com (no requiere auth, API oficial v2 deprecada)
BCRA_ENDPOINTS = {
    "tasa_plazo_fijo": "https://api.estadisticasbcra.com/tasa_depositos_30_dias",
    "reservas":        "https://api.estadisticasbcra.com/reservas",
    "tipo_cambio":     "https://api.estadisticasbcra.com/usd_of",
}
 
 
def _bcra_latest(url):
    """Obtiene el último valor desde api.estadisticasbcra.com."""
    try:
        headers = {"Authorization": "BEARER eyJhbGciOiJIUzUxMiIsInR5cCI6IkpXVCJ9"}
        r = requests.get(url, timeout=15, headers=headers)
        r.raise_for_status()
        data = r.json()
        if data and len(data) > 0:
            latest = data[-1]
            return float(latest.get("v", 0)), latest.get("d", "")
        return None, None
    except Exception as e:
        logger.warning(f"BCRA error: {e}")
        return None, None
 
 
def fetch_argentina_macro():
    """Obtiene variables macro de Argentina via Yahoo Finance + fallback."""
    data = {}
 
    # Tipo de cambio USD/ARS desde Yahoo Finance
    try:
        import yfinance as yf
        ars = yf.download("ARS=X", period="5d", progress=False)
        if not ars.empty:
            if isinstance(ars.columns, pd.MultiIndex):
                ars.columns = ars.columns.get_level_values(0)
            tc = float(ars['Close'].iloc[-1])
            data["tipo_cambio"] = {"valor": tc, "fecha": datetime.now().strftime("%Y-%m-%d")}
        else:
            data["tipo_cambio"] = {"valor": None, "fecha": ""}
    except Exception as e:
        logger.warning(f"Yahoo ARS error: {e}")
        data["tipo_cambio"] = {"valor": None, "fecha": ""}
 
    # Tasa plazo fijo — fallback a último conocido (se actualiza mensualmente)
    data["tasa_tamar"] = {"valor": 29.0, "fecha": "2026-05-01"}
 
    # Reservas — intentar BCRA, fallback
    try:
        r = requests.get("https://api.estadisticasbcra.com/reservas", timeout=10, headers={"User-Agent": "InversionesBursatiles/1.0"})
        if r.status_code == 200:
            d = r.json()
            if d:
                data["reservas"] = {"valor": float(d[-1].get("v", 0)), "fecha": d[-1].get("d", "")}
            else:
                data["reservas"] = {"valor": 26500, "fecha": "fallback"}
        else:
            data["reservas"] = {"valor": 26500, "fecha": "fallback"}
    except Exception:
        data["reservas"] = {"valor": 26500, "fecha": "fallback"}
 
    # Riesgo país — Ámbito
    try:
        r = requests.get("https://mercados.ambito.com/riesgo-pais/datos", timeout=10, headers={"User-Agent": "InversionesBursatiles/1.0"})
        if r.status_code == 200:
            rp_data = r.json()
            if isinstance(rp_data, dict):
                val = float(str(rp_data.get("ultimo", "0")).replace(".", "").replace(",", "."))
                data["riesgo_pais"] = {"valor": val, "fecha": datetime.now().strftime("%Y-%m-%d")}
            else:
                data["riesgo_pais"] = {"valor": None, "fecha": ""}
        else:
            data["riesgo_pais"] = {"valor": None, "fecha": ""}
    except Exception as e:
        logger.warning(f"Riesgo país ARG error: {e}")
        data["riesgo_pais"] = {"valor": None, "fecha": ""}
 
    # Brecha cambiaria
    try:
        r = requests.get("https://mercados.ambito.com/dolar/cl/variacion", timeout=10, headers={"User-Agent": "InversionesBursatiles/1.0"})
        if r.status_code == 200:
            ccl_data = r.json()
            ccl = float(str(ccl_data.get("compra", "0")).replace(".", "").replace(",", "."))
            oficial = data.get("tipo_cambio", {}).get("valor") or 1
            if oficial > 0 and ccl > 0:
                brecha = round(((ccl / oficial) - 1) * 100, 1)
                data["brecha"] = {"valor": brecha, "fecha": datetime.now().strftime("%Y-%m-%d")}
            else:
                data["brecha"] = {"valor": None, "fecha": ""}
        else:
            data["brecha"] = {"valor": None, "fecha": ""}
    except Exception as e:
        logger.warning(f"Brecha error: {e}")
        data["brecha"] = {"valor": None, "fecha": ""}
 
    # IPC mensual — datos.gob.ar (INDEC)
    try:
        url_ipc = (
            "https://apis.datos.gob.ar/series/api/series/"
            "?ids=" + DATOS_GOB_SERIES["ipc"] + "&last=3&format=json"
        )
        r_ipc = requests.get(url_ipc, timeout=15, headers={"User-Agent": "InversionesBursatiles/1.0"})
        r_ipc.raise_for_status()
        pts = r_ipc.json().get("data", [])
        if len(pts) >= 2:
            ipc_var = round((float(pts[-1][1]) / float(pts[-2][1]) - 1) * 100, 2)
            data["ipc"] = {"valor": ipc_var, "fecha": str(pts[-1][0])}
        else:
            data["ipc"] = {"valor": None, "fecha": ""}
    except Exception as e:
        logger.warning("IPC ARG error: " + str(e))
        data["ipc"] = {"valor": None, "fecha": ""}
 
    # Desempleo — datos.gob.ar (INDEC/EPH)
    val, dt = _datos_gob_latest(DATOS_GOB_SERIES["desempleo"])
    data["desempleo"] = {"valor": val, "fecha": dt or ""}
 
    # Balanza comercial — datos.gob.ar (INDEC)
    val, dt = _datos_gob_latest(DATOS_GOB_SERIES["balanza"])
    data["balanza_comercial"] = {"valor": val, "fecha": dt or ""}
 
    # Resultado fiscal primario — datos.gob.ar (Mecon)
    val, dt = _datos_gob_latest(DATOS_GOB_SERIES["fiscal"])
    data["resultado_fiscal"] = {"valor": val, "fecha": dt or ""}
 
    logger.info(f"ARG macro: {sum(1 for v in data.values() if v['valor'] is not None)}/{len(data)} variables obtenidas")
    return data
 
 
# ─────────────────────────────────────────────
# datos.gob.ar (Argentina) — API pública sin key
# ─────────────────────────────────────────────

DATOS_GOB_SERIES = {
    "ipc":       "148.3_INIVELNAL_DICI_M_26",
    "desempleo": "41.1_DESO_TOTAL_D_L_29",
    "balanza":   "185.1_EXPOIM_TOTAL_D_M_26",
    "fiscal":    "28.3_RFPFSPN_D_0_M_36",
}


def _datos_gob_latest(series_id, n=3):
    """Obtiene el ultimo valor de una serie de datos.gob.ar (INDEC/Mecon)."""
    try:
        url = (
            "https://apis.datos.gob.ar/series/api/series/"
            "?ids=" + series_id + "&last=" + str(n) + "&format=json"
        )
        r = requests.get(url, timeout=15, headers={"User-Agent": "InversionesBursatiles/1.0"})
        r.raise_for_status()
        pts = r.json().get("data", [])
        if pts:
            return float(pts[-1][1]), str(pts[-1][0])
        return None, None
    except Exception as e:
        logger.warning("datos.gob.ar error [" + series_id + "]: " + str(e))
        return None, None


# ─────────────────────────────────────────────
# BCB/SGS (Brasil) — API pública
# ─────────────────────────────────────────────
 
BCB_SERIES = {
    "selic":        432,    # Meta SELIC
    "ipca":         433,    # IPCA acumulado 12m
    "desempleo":    24369,  # Taxa de desocupação
    "reservas":     13621,  # Reservas internacionales
    "brl_usd":    1,      # Dólar comercial (venda)
    "deuda_pib":  4503,   # Divida Liquida Setor Publico % PIB
}
 
 
def _bcb_latest(series_id):
    """Obtiene el último valor de una serie BCB/SGS."""
    try:
        end = datetime.now().strftime("%d/%m/%Y")
        start = (datetime.now() - timedelta(days=60)).strftime("%d/%m/%Y")
        url = f"https://api.bcb.gov.br/dados/serie/bcdata.sgs.{series_id}/dados?formato=json&dataInicial={start}&dataFinal={end}"
        headers = {"User-Agent": "InversionesBursatiles/1.0"}
        r = requests.get(url, timeout=15, headers=headers)
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
        r = requests.get("https://mercados.ambito.com/riesgo-pais-historico/brasil", timeout=10, headers={"User-Agent": "InversionesBursatiles/1.0"})
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
 
    # PMI Manufacturero Brasil — FRED (BRAPMIMANMISMEI)
    try:
        fred_key = os.environ.get("FRED_API_KEY", "")
        if fred_key:
            url_pmi = (
                "https://api.stlouisfed.org/fred/series/observations"
                "?series_id=BRAPMIMANMISMEI&api_key=" + fred_key +
                "&file_type=json&limit=3&sort_order=desc"
            )
            r_pmi = requests.get(url_pmi, timeout=15, headers={"User-Agent": "InversionesBursatiles/1.0"})
            r_pmi.raise_for_status()
            obs = [o for o in r_pmi.json().get("observations", []) if o.get("value", ".") != "."]
            if obs:
                data["pmi"] = {"valor": float(obs[0]["value"]), "fecha": obs[0]["date"]}
            else:
                data["pmi"] = {"valor": None, "fecha": ""}
        else:
            data["pmi"] = {"valor": None, "fecha": "sin_fred_key"}
    except Exception as e:
        logger.warning("PMI BRA FRED error: " + str(e))
        data["pmi"] = {"valor": None, "fecha": ""}

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
    # Argentina — ahora auto via datos.gob.ar
    "arg_ipc":       (6.0,    0.3),   # var% mensual
    "arg_desempleo": (15.0,   4.0),   # % desocupacion
    "arg_balanza":   (-2000,  4000),  # M USD
    "arg_fiscal":    (-3.0,   2.0),   # % PBI
    # Brasil — ahora auto via BCB/FRED
    "bra_deuda_pib": (100.0,  30.0),  # % PIB
    "bra_pmi":       (40.0,   58.0),  # PMI < 50 contraccion
    # USA
    "usa_fed":       (8.0,    0.5),
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
        ("brecha",           "arg_brecha",    True),
        ("ipc",              "arg_ipc",       True),
        ("desempleo",        "arg_desempleo", True),
        ("balanza_comercial","arg_balanza",   False),
        ("resultado_fiscal", "arg_fiscal",    False),
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
        ("brl_usd",    "bra_brl",      True),
        ("deuda_pib",  "bra_deuda_pib", True),
        ("pmi",        "bra_pmi",       False),
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
 
def update_xlsx_macro(detalles, xlsx_path="data/modelo_macro_micro_señales.xlsx"):
    """
    Actualiza la hoja 'Macro Variables' del xlsx con los valores frescos
    obtenidos por macro_auto, para que queden persistentes.
    """
    import openpyxl
 
    if not os.path.exists(xlsx_path):
        logger.warning(f"[macro_auto] xlsx no encontrado para update: {xlsx_path}")
        return False
 
    try:
        wb = openpyxl.load_workbook(xlsx_path)
        ws = wb["Macro Variables"]
 
        # Mapeo: variable_key de detalles → substring que aparece en columna Variable del xlsx
        MAPPING_ARG = {
            "tasa_tamar":  "Tasa de interés TAMAR",
            "riesgo_pais": "Riesgo País",
            "reservas":    "Reservas BCRA",
            "tipo_cambio": "Tipo de cambio real",
            "brecha":      "Brecha cambiaria",
        }
        MAPPING_BRA = {
            "selic":       "Tasa SELIC",
            "riesgo_pais": "Riesgo País",
            "ipca":        "Inflación IPCA",
            "desempleo":   "Desempleo",
            "reservas":    "Reservas BCB",
            "brl_usd":     "Tipo de cambio BRL",
        }
        MAPPING_USA = {
            "fed_funds":     "Fed Funds Rate",
            "cpi":           "Inflación CPI",
            "unemployment":  "Desempleo",
            "gdp_growth":    "GDP Growth",
            "consumer_conf": "Confianza consumidor",
            "pce_core":      "PCE Core",
            "hy_spread":     "Spread High Yield",
            "dxy":           "Índice DXY",
            "ism_mfg":       "ISM Manufacturero",
        }
 
        pais_map = {
            "Argentina": ("ARG", MAPPING_ARG),
            "Brasil":    ("BRA", MAPPING_BRA),
            "EE.UU.":    ("USA", MAPPING_USA),
        }
 
        updated_count = 0
        for row in range(2, ws.max_row + 1):
            pais_cell = str(ws.cell(row, 1).value or "")
            var_cell  = str(ws.cell(row, 2).value or "")
 
            for pais_nombre, (pais_key, mapping) in pais_map.items():
                if pais_cell != pais_nombre:
                    continue
                det_pais = detalles.get(pais_key, {})
                for var_key, var_xlsx_name in mapping.items():
                    if var_xlsx_name in var_cell and var_key in det_pais:
                        new_val = det_pais[var_key].get("valor")
                        new_score = det_pais[var_key].get("score")
                        if new_val is not None:
                            ws.cell(row, 3).value = new_val  # Columna Valor
                            updated_count += 1
                        if new_score is not None:
                            ws.cell(row, 4).value = new_score  # Columna Score
                        # Actualizar fuente con timestamp
                        old_fuente = str(ws.cell(row, 6).value or "")
                        ws.cell(row, 6).value = f"Auto {datetime.now().strftime('%d/%m/%Y')} — {old_fuente[:60]}"
                        break
 
        wb.save(xlsx_path)
        logger.info(f"[macro_auto] xlsx actualizado: {updated_count} variables en {xlsx_path}")
        return updated_count > 0
 
    except Exception as e:
        logger.error(f"[macro_auto] Error actualizando xlsx: {e}")
        return False
 
 
def fetch_all_macro():
    """
    Descarga datos macro de los 3 países y calcula scores.
    Si falla alguna API, usa fallback del cache o hardcoded.
    Actualiza el xlsx para persistencia.
    Retorna dict compatible con xlsx_signals format.
    """
    logger.info("Descargando datos macro automáticos...")
 
    arg_data = fetch_argentina_macro()
    bra_data = fetch_brasil_macro()
    usa_data = fetch_usa_macro()
 
    result = compute_macro_scores(arg_data, bra_data, usa_data)
 
    # Persistir en xlsx para que la próxima ejecución tenga datos frescos
    vars_ok = result.get("variables_obtenidas", {})
    total_ok = sum(vars_ok.values())
    if total_ok >= 8:  # Al menos 8 de 27 variables obtenidas
        try:
            update_xlsx_macro(result.get("detalles", {}))
        except Exception as e:
            logger.warning(f"[macro_auto] No se pudo actualizar xlsx: {e}")
 
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
 
