"""
src/fundamental_auto_av.py

Fuente de respaldo para los tickers SP500/CEDEARs que FMP free tier
bloquea como "Special Endpoint" premium (confirmado en vivo 07/09/2026 --
ver src/fundamental_auto.py y el log de esa corrida). Usa Alpha Vantage
en su lugar, endpoint OVERVIEW.

⚠️ CUOTA MUY AJUSTADA: Alpha Vantage free tier es 25 requests/día (bajó de
500 -> 100 -> 25 en el último año, verificado 07/09/2026). Este módulo usa
UN SOLO call por ticker (OVERVIEW trae ratios + sector + empresa juntos),
así que los 14 tickers bloqueados por FMP entran en una corrida con
margen -- pero NO hay margen para reintentos agresivos ni para correrlo
dos veces el mismo día. Además respeta 5 calls/min (delay de 13s entre
tickers).

LIMITACIÓN CONOCIDA: OVERVIEW no incluye Debt/Equity ni Current Ratio
(viven en BALANCE_SHEET, que consumiría el doble de cuota). Esos dos
campos quedan vacíos para tickers de esta fuente -- fundamental.py ya
tolera campos faltantes sin romper el cálculo del score.

TRAMPA DE ALPHA VANTAGE A TENER EN CUENTA: cuando se agota la cuota, NO
devuelve un error HTTP -- devuelve 200 OK con {"Information": "..."} en
vez del objeto de datos esperado. Si no se detecta explícitamente, un
ticker sin cuota se confunde con un ticker "sin datos" real. Ver _get().

NO PROBADO EN VIVO (mismo motivo que fundamental_auto.py: este sandbox
no tiene salida de red a alphavantage.co). Probar primero con --tickers
en 1-2 símbolos.

Uso manual:
    python -m src.fundamental_auto_av --tickers LLY,PG
    python -m src.fundamental_auto_av              # batch completo (14 tickers, ~3 min)
"""

import argparse
import logging
import os
import time

import pandas as pd
import requests

from src.fundamental_auto import CSV_COLUMNS, CSV_PATH_DEFAULT, _num, _pct

logger = logging.getLogger(__name__)

ALPHA_VANTAGE_API_KEY = os.getenv("ALPHA_VANTAGE_API_KEY", "")
AV_BASE = "https://www.alphavantage.co/query"

# Tickers confirmados bloqueados por FMP free tier (log real, 07/09/2026).
# No es una lista "para siempre" -- FMP puede cambiar su política. Bruno
# puede pasar --tickers para override si la lista cambia.
FMP_BLOCKED_DEFAULT = [
    "LLY", "PG", "MCD", "CAT", "GLOB", "MELI", "RIO", "PBR",
    "QCOM", "MA", "HD", "CRM", "BRK-B", "ORCL",
]

REQUEST_TIMEOUT = 15
REQUEST_DELAY_SECONDS = 13  # 5 calls/min -> 12s mínimo, dejamos 1s de margen


def _get_overview(ticker: str) -> dict | None:
    """GET a OVERVIEW para un ticker. Devuelve el dict de datos, o None ante
    cualquier problema (vacío, error, cuota agotada, timeout). Nunca levanta
    excepción -- el llamador decide qué hacer con None."""
    if not ALPHA_VANTAGE_API_KEY:
        logger.warning("[fundamental_auto_av] ALPHA_VANTAGE_API_KEY no seteada")
        return None
    params = {"function": "OVERVIEW", "symbol": ticker, "apikey": ALPHA_VANTAGE_API_KEY}
    try:
        r = requests.get(AV_BASE, params=params, timeout=REQUEST_TIMEOUT)
        if r.status_code != 200:
            logger.warning(f"[fundamental_auto_av] {ticker}: HTTP {r.status_code}")
            return None
        data = r.json()
        # Trampa de AV: cuota agotada -> 200 OK con "Note"/"Information" en
        # vez del objeto esperado. Si no se detecta esto explícitamente, un
        # ticker sin cuota se confunde con "sin datos" real.
        if "Note" in data or "Information" in data:
            msg = data.get("Note") or data.get("Information")
            logger.warning(f"[fundamental_auto_av] {ticker}: cuota/límite de AV — {msg[:200]}")
            return "QUOTA_EXCEEDED"
        if not data or "Symbol" not in data:
            logger.info(f"[fundamental_auto_av] {ticker}: respuesta vacía o sin 'Symbol'")
            return None
        return data
    except Exception as e:
        logger.warning(f"[fundamental_auto_av] {ticker}: error de red — {e}")
        return None


def build_ratio_updates(overview: dict) -> dict:
    """Mapea OVERVIEW a las columnas de ratios crudos del CSV. No incluye
    Deuda/Equity ni Current Ratio -- no vienen en este endpoint."""
    updates = {}
    updates["P/E (trailing)"] = _num(overview.get("PERatio"))
    updates["PEG Ratio"] = _num(overview.get("PEGRatio"))
    updates["P/B"] = _num(overview.get("PriceToBookRatio"))
    updates["P/S"] = _num(overview.get("PriceToSalesRatioTTM"))
    updates["EV/EBITDA"] = _num(overview.get("EVToEBITDA"))
    updates["Margen Operativo (%)"] = _pct(overview.get("OperatingMarginTTM"))
    updates["Margen Neto (%)"] = _pct(overview.get("ProfitMargin"))
    updates["ROE (%)"] = _pct(overview.get("ReturnOnEquityTTM"))
    updates["ROA (%)"] = _pct(overview.get("ReturnOnAssetsTTM"))
    updates["Crec. Ingresos YoY (%)"] = _pct(overview.get("QuarterlyRevenueGrowthYOY"))
    updates["Crec. Ganancias YoY (%)"] = _pct(overview.get("QuarterlyEarningsGrowthYOY"))
    updates["Div. Yield (%)"] = _pct(overview.get("DividendYield"))
    updates["Beta"] = _num(overview.get("Beta"))
    updates["Máximo 52s"] = _num(overview.get("52WeekHigh"))
    updates["Mínimo 52s"] = _num(overview.get("52WeekLow"))
    cap = overview.get("MarketCapitalization")
    if cap:
        updates["Cap. Mercado"] = _num(cap, decimals=0)
    return {k: v for k, v in updates.items() if v is not None}


def build_new_row(ticker: str, overview: dict, ratio_updates: dict) -> dict:
    row = {col: "" for col in CSV_COLUMNS}
    row["Ticker"] = ticker
    row["Empresa"] = overview.get("Name") or ticker
    row["Sector"] = overview.get("Sector") or ""
    row["Industria"] = overview.get("Industry") or ""
    row["País"] = overview.get("Country") or "USA"
    row["Moneda"] = overview.get("Currency") or "USD"
    row.update(ratio_updates)
    # Score Cuantitativo vacío a propósito -- fundamental.py calcula desde
    # ratios individuales cuando no hay score propio.
    return row


def update_fundamentals_csv_av(csv_path: str = CSV_PATH_DEFAULT,
                                tickers: list[str] | None = None,
                                dry_run: bool = False,
                                push: bool = True) -> dict:
    """
    Actualiza el CSV para tickers vía Alpha Vantage (por defecto, los
    bloqueados por FMP -- FMP_BLOCKED_DEFAULT). Devuelve un resumen
    {"updated": [...], "added": [...], "skipped": [...], "errors": [...],
    "quota_exceeded": [...]}.
    """
    if not ALPHA_VANTAGE_API_KEY:
        raise RuntimeError("ALPHA_VANTAGE_API_KEY no está seteada en el entorno")

    target_tickers = tickers or list(FMP_BLOCKED_DEFAULT)

    summary = {"updated": [], "added": [], "skipped": [], "errors": [], "quota_exceeded": []}

    if os.path.exists(csv_path):
        df = pd.read_csv(csv_path, sep=";", encoding="utf-8-sig", decimal=",")
    else:
        df = pd.DataFrame(columns=CSV_COLUMNS)

    df["Ticker"] = df["Ticker"].astype(str).str.strip()
    existing_tickers = set(df["Ticker"])

    new_rows = []
    quota_hit = False

    for ticker in target_tickers:
        if quota_hit:
            # Ya sabemos que se acabó la cuota del día -- no tiene sentido
            # seguir gastando requests contra un límite ya superado.
            summary["quota_exceeded"].append(ticker)
            continue
        try:
            logger.info(f"[fundamental_auto_av] {ticker}: consultando Alpha Vantage...")
            overview = _get_overview(ticker)

            if overview == "QUOTA_EXCEEDED":
                quota_hit = True
                summary["quota_exceeded"].append(ticker)
                continue

            if overview is None:
                summary["skipped"].append(ticker)
                time.sleep(REQUEST_DELAY_SECONDS)
                continue

            ratio_updates = build_ratio_updates(overview)

            if ticker in existing_tickers:
                idx = df.index[df["Ticker"] == ticker][0]
                for col, val in ratio_updates.items():
                    df.at[idx, col] = val
                summary["updated"].append(ticker)
            else:
                new_row = build_new_row(ticker, overview, ratio_updates)
                new_rows.append(new_row)
                summary["added"].append(ticker)

            time.sleep(REQUEST_DELAY_SECONDS)

        except Exception as e:
            logger.error(f"[fundamental_auto_av] {ticker}: error inesperado — {e}")
            summary["errors"].append(ticker)
            time.sleep(REQUEST_DELAY_SECONDS)

    if new_rows:
        df = pd.concat([df, pd.DataFrame(new_rows)], ignore_index=True)

    if dry_run:
        logger.info(f"[fundamental_auto_av] DRY RUN — no se escribió ni pusheó. Resumen: {summary}")
        return summary

    if summary["updated"] or summary["added"]:
        os.makedirs(os.path.dirname(csv_path) or ".", exist_ok=True)
        df.to_csv(csv_path, sep=";", encoding="utf-8-sig", decimal=",", index=False)
        logger.info(f"[fundamental_auto_av] CSV escrito localmente: {csv_path}")

        if push:
            from src.github_persistence import push_file
            ok = push_file(
                csv_path,
                f"auto: fundamentals Alpha Vantage (fallback FMP) "
                f"{len(summary['updated'])} act. {len(summary['added'])} nuevos",
            )
            if not ok:
                logger.warning("[fundamental_auto_av] CSV actualizado local pero el push a GitHub falló")

    logger.info(f"[fundamental_auto_av] Resumen: {summary}")
    return summary


def _main():
    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
    parser = argparse.ArgumentParser(description="Actualiza ratios (fallback FMP) vía Alpha Vantage")
    parser.add_argument("--tickers", type=str, default=None,
                         help="Lista separada por comas (default: los 14 bloqueados por FMP)")
    parser.add_argument("--csv-path", type=str, default=CSV_PATH_DEFAULT)
    parser.add_argument("--dry-run", action="store_true")
    parser.add_argument("--no-push", action="store_true")
    args = parser.parse_args()

    tickers = [t.strip().upper() for t in args.tickers.split(",")] if args.tickers else None

    summary = update_fundamentals_csv_av(
        csv_path=args.csv_path,
        tickers=tickers,
        dry_run=args.dry_run,
        push=not args.no_push,
    )
    print(summary)


if __name__ == "__main__":
    _main()
