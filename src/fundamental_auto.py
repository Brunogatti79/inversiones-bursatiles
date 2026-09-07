"""
src/fundamental_auto.py

Actualiza automáticamente las columnas de ratios crudos en
data/ratios_consolidado_quant.csv para el segmento S&P 500 / CEDEARs,
usando la API de Financial Modeling Prep (FMP).

ALCANCE — LEER ANTES DE TOCAR:
  - Solo toca tickers de SP500_TICKERS (src/downloader.py). NUNCA toca
    filas de MERVAL o BOVESPA (FMP free tier las bloquea como premium
    para tickers .BA/.SA -- confirmado en vivo 07/09/2026, ver auditoría
    en el chat de esa fecha).
  - Para tickers que YA tienen fila en el CSV: refresca solo los ratios
    crudos (P/E, EV/EBITDA, ROE, márgenes, Deuda/Equity, Current Ratio,
    crecimiento YoY). NUNCA sobreescribe "Score Cuantitativo" -- ese es
    el modelo propietario de Bruno, no algo que FMP pueda calcular.
  - Para tickers que NO tienen fila: crea una nueva con datos de FMP
    (profile + ratios + growth) y deja "Score Cuantitativo" vacío, para
    que fundamental.py calcule el score desde los ratios individuales
    (_score_fundamental_from_ratios), tal como ya hace con cualquier
    ticker sin score cuantitativo propio.
  - ETFs (COPX, IBB, EWZ) se excluyen explícitamente: no tienen estados
    financieros, /ratios les devuelve vacío.

NO PROBADO EN VIVO: este sandbox no tiene salida de red a
financialmodelingprep.com (solo Railway la va a tener). El manejo de
errores es defensivo (try/except por ticker, nunca rompe el batch
completo), pero hay que correr primero con --tickers en 1-2 símbolos
antes del batch completo.

Uso manual (Railway shell / consola local con FMP_API_KEY seteada):
    python -m src.fundamental_auto --tickers AAPL,GLOB   # prueba chica
    python -m src.fundamental_auto                        # batch completo
    python -m src.fundamental_auto --dry-run               # no pushea a GitHub
"""

import argparse
import logging
import os
import time

import pandas as pd
import requests

logger = logging.getLogger(__name__)

FMP_API_KEY = os.getenv("FMP_API_KEY", "")
FMP_BASE = "https://financialmodelingprep.com/stable"

CSV_PATH_DEFAULT = "data/ratios_consolidado_quant.csv"

# ETFs dentro de SP500_TICKERS que no tienen estados contables propios --
# /ratios y /financial-growth les devuelven vacío por diseño de FMP.
ETF_SKIP = {"COPX", "IBB", "EWZ"}

REQUEST_TIMEOUT = 15
REQUEST_DELAY_SECONDS = 0.3  # cortesía con el rate limit del free tier


def _get(endpoint: str, ticker: str) -> dict | None:
    """GET genérico a un endpoint /stable/{endpoint}?symbol=X. Devuelve el
    primer elemento del array de respuesta, o None ante cualquier problema
    (vacío, error, timeout, JSON inválido). Nunca levanta excepción -- el
    llamador decide qué hacer con None."""
    if not FMP_API_KEY:
        logger.warning("[fundamental_auto] FMP_API_KEY no seteada")
        return None
    url = f"{FMP_BASE}/{endpoint}"
    params = {"symbol": ticker, "apikey": FMP_API_KEY, "limit": 1}
    try:
        r = requests.get(url, params=params, timeout=REQUEST_TIMEOUT)
        if r.status_code != 200:
            logger.warning(f"[fundamental_auto] {ticker} {endpoint}: HTTP {r.status_code} {r.text[:150]}")
            return None
        data = r.json()
        if isinstance(data, dict) and "Error Message" in data:
            logger.warning(f"[fundamental_auto] {ticker} {endpoint}: {data['Error Message'][:150]}")
            return None
        if not isinstance(data, list) or len(data) == 0:
            logger.info(f"[fundamental_auto] {ticker} {endpoint}: respuesta vacía")
            return None
        return data[0]
    except Exception as e:
        logger.warning(f"[fundamental_auto] {ticker} {endpoint}: error de red — {e}")
        return None


def fetch_fmp_ratios(ticker: str) -> dict | None:
    return _get("ratios", ticker)


def fetch_fmp_growth(ticker: str) -> dict | None:
    return _get("financial-growth", ticker)


def fetch_fmp_profile(ticker: str) -> dict | None:
    return _get("profile", ticker)


def fetch_fmp_quote(ticker: str) -> dict | None:
    return _get("quote", ticker)


def _pct(val) -> float | None:
    """FMP devuelve márgenes/growth como fracción (0.469 = 46.9%) salvo
    dividendYieldPercentage que ya viene en %. Normalizamos a % (×100)."""
    if val is None:
        return None
    try:
        return round(float(val) * 100, 2)
    except (TypeError, ValueError):
        return None


def _num(val, decimals=2) -> float | None:
    if val is None:
        return None
    try:
        return round(float(val), decimals)
    except (TypeError, ValueError):
        return None


def build_ratio_updates(ticker: str, ratios: dict | None, growth: dict | None) -> dict:
    """Mapea la respuesta de FMP a las columnas de ratios CRUDOS del CSV
    (las que alimentan _score_fundamental_from_ratios en fundamental.py).
    No incluye Empresa/Sector/Score Cuantitativo -- eso se maneja aparte
    según si la fila ya existe o es nueva."""
    updates = {}
    if ratios:
        updates["P/E (trailing)"] = _num(ratios.get("priceToEarningsRatio"))
        updates["P/B"] = _num(ratios.get("priceToBookRatio"))
        updates["P/S"] = _num(ratios.get("priceToSalesRatio"))
        updates["EV/EBITDA"] = _num(ratios.get("enterpriseValueMultiple"))
        updates["Margen Bruto (%)"] = _pct(ratios.get("grossProfitMargin"))
        updates["Margen Operativo (%)"] = _pct(ratios.get("operatingProfitMargin"))
        updates["Margen Neto (%)"] = _pct(ratios.get("netProfitMargin"))
        updates["Margen EBITDA (%)"] = _pct(ratios.get("ebitdaMargin"))
        updates["Deuda/Equity"] = _num(ratios.get("debtToEquityRatio"))
        updates["Current Ratio"] = _num(ratios.get("currentRatio"))
        updates["Quick Ratio"] = _num(ratios.get("quickRatio"))
        updates["Div. Yield (%)"] = _num(ratios.get("dividendYieldPercentage"))
        updates["Payout Ratio (%)"] = _pct(ratios.get("dividendPayoutRatio"))

        # ROE no viene como campo directo en /stable/ratios -- se deriva de
        # netIncomePerShare / shareholdersEquityPerShare (el "por acción"
        # cancela y queda el ROE real). Si falta cualquiera de los dos, o
        # el equity es 0/negativo, se deja sin dato en vez de forzar un
        # número engañoso.
        ni_ps = ratios.get("netIncomePerShare")
        eq_ps = ratios.get("shareholdersEquityPerShare")
        try:
            if ni_ps is not None and eq_ps is not None and float(eq_ps) != 0:
                updates["ROE (%)"] = round(float(ni_ps) / float(eq_ps) * 100, 2)
        except (TypeError, ValueError):
            pass

    if growth:
        updates["Crec. Ingresos YoY (%)"] = _pct(growth.get("revenueGrowth"))
        # netIncomeGrowth es más estable que epsgrowth (no lo distorsiona
        # el conteo de acciones por buybacks) -- usamos ese como proxy de
        # "Crec. Ganancias".
        updates["Crec. Ganancias YoY (%)"] = _pct(growth.get("netIncomeGrowth"))

    # Filtrar None para no pisar un valor bueno existente con vacío si
    # FMP no devolvió ese campo puntual.
    return {k: v for k, v in updates.items() if v is not None}


def build_new_row(ticker: str, empresa: str, profile: dict | None,
                   quote: dict | None, ratio_updates: dict) -> dict:
    """Arma una fila completa nueva para un ticker que no está en el CSV."""
    row = {col: "" for col in CSV_COLUMNS}
    row["Ticker"] = ticker
    row["Empresa"] = (profile or {}).get("companyName") or empresa
    row["Sector"] = (profile or {}).get("sector") or ""
    row["Industria"] = (profile or {}).get("industry") or ""
    row["País"] = (profile or {}).get("country") or "USA"
    row["Moneda"] = (profile or {}).get("currency") or "USD"
    if quote:
        row["Precio actual"] = _num(quote.get("price"))
        row["Cap. Mercado"] = _num(quote.get("marketCap"), decimals=0)
        row["Máximo 52s"] = _num(quote.get("yearHigh"))
        row["Mínimo 52s"] = _num(quote.get("yearLow"))
        price = quote.get("price")
        yhigh = quote.get("yearHigh")
        ylow = quote.get("yearLow")
        try:
            if price and yhigh:
                row["Dist. Máx 52s (%)"] = round((float(price) / float(yhigh) - 1) * 100, 2)
            if price and ylow:
                row["Dist. Mín 52s (%)"] = round((float(price) / float(ylow) - 1) * 100, 2)
        except (TypeError, ValueError, ZeroDivisionError):
            pass
    if profile:
        row["Beta"] = _num(profile.get("beta"))
    row.update(ratio_updates)
    # Score Cuantitativo queda vacío a propósito -- fundamental.py cae a
    # _score_fundamental_from_ratios() automáticamente cuando no hay valor.
    return row


CSV_COLUMNS = [
    "Ticker", "Empresa", "Sector", "Industria", "País", "Moneda",
    "Precio actual", "Cap. Mercado", "Beta", "P/E (trailing)",
    "P/E (forward)", "P/B", "P/S", "EV/EBITDA", "PEG Ratio",
    "Valor Graham", "Upside vs Graham (%)", "ROE (%)", "ROA (%)",
    "Margen Bruto (%)", "Margen Operativo (%)", "Margen Neto (%)",
    "Margen EBITDA (%)", "Deuda/Equity", "Current Ratio", "Quick Ratio",
    "Crec. Ingresos YoY (%)", "Crec. Ganancias YoY (%)", "Div. Yield (%)",
    "Payout Ratio (%)", "Máximo 52s", "Mínimo 52s", "Dist. Máx 52s (%)",
    "Dist. Mín 52s (%)", "Score Cuantitativo",
]


def update_fundamentals_csv(csv_path: str = CSV_PATH_DEFAULT,
                             tickers: list[str] | None = None,
                             dry_run: bool = False,
                             push: bool = True) -> dict:
    """
    Actualiza el CSV para los tickers de SP500_TICKERS (o el subset pasado
    en `tickers`). Devuelve un resumen {"updated": [...], "added": [...],
    "skipped": [...], "errors": [...]}.
    """
    from src.downloader import SP500_TICKERS

    if not FMP_API_KEY:
        raise RuntimeError("FMP_API_KEY no está seteada en el entorno")

    target_tickers = tickers or list(SP500_TICKERS.keys())
    target_tickers = [t for t in target_tickers if t not in ETF_SKIP]

    summary = {"updated": [], "added": [], "skipped": [], "errors": []}

    if os.path.exists(csv_path):
        df = pd.read_csv(csv_path, sep=";", encoding="utf-8-sig", decimal=",")
    else:
        df = pd.DataFrame(columns=CSV_COLUMNS)

    df["Ticker"] = df["Ticker"].astype(str).str.strip()
    existing_tickers = set(df["Ticker"])

    new_rows = []

    for ticker in target_tickers:
        try:
            logger.info(f"[fundamental_auto] {ticker}: consultando FMP...")
            ratios = fetch_fmp_ratios(ticker)
            time.sleep(REQUEST_DELAY_SECONDS)
            growth = fetch_fmp_growth(ticker)
            time.sleep(REQUEST_DELAY_SECONDS)

            if ratios is None and growth is None:
                summary["skipped"].append(ticker)
                logger.info(f"[fundamental_auto] {ticker}: sin datos de FMP, se omite")
                continue

            ratio_updates = build_ratio_updates(ticker, ratios, growth)

            if ticker in existing_tickers:
                idx = df.index[df["Ticker"] == ticker][0]
                for col, val in ratio_updates.items():
                    df.at[idx, col] = val
                summary["updated"].append(ticker)
            else:
                profile = fetch_fmp_profile(ticker)
                time.sleep(REQUEST_DELAY_SECONDS)
                quote = fetch_fmp_quote(ticker)
                time.sleep(REQUEST_DELAY_SECONDS)
                empresa = SP500_TICKERS.get(ticker, ticker)
                new_row = build_new_row(ticker, empresa, profile, quote, ratio_updates)
                new_rows.append(new_row)
                summary["added"].append(ticker)

        except Exception as e:
            logger.error(f"[fundamental_auto] {ticker}: error inesperado — {e}")
            summary["errors"].append(ticker)

    if new_rows:
        df = pd.concat([df, pd.DataFrame(new_rows)], ignore_index=True)

    if dry_run:
        logger.info(f"[fundamental_auto] DRY RUN — no se escribió ni pusheó el CSV. Resumen: {summary}")
        return summary

    os.makedirs(os.path.dirname(csv_path) or ".", exist_ok=True)
    df.to_csv(csv_path, sep=";", encoding="utf-8-sig", decimal=",", index=False)
    logger.info(f"[fundamental_auto] CSV escrito localmente: {csv_path}")

    if push:
        from src.github_persistence import push_file
        ok = push_file(
            csv_path,
            f"auto: fundamentals FMP (SP500/CEDEARs) {len(summary['updated'])} act. "
            f"{len(summary['added'])} nuevos",
        )
        if not ok:
            logger.warning("[fundamental_auto] el CSV se actualizó local pero el push a GitHub falló")

    logger.info(f"[fundamental_auto] Resumen: {summary}")
    return summary


def _main():
    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
    parser = argparse.ArgumentParser(description="Actualiza ratios fundamentales SP500/CEDEARs vía FMP")
    parser.add_argument("--tickers", type=str, default=None,
                         help="Lista separada por comas para probar en un subset (ej: AAPL,GLOB)")
    parser.add_argument("--csv-path", type=str, default=CSV_PATH_DEFAULT)
    parser.add_argument("--dry-run", action="store_true", help="No escribe ni pushea, solo loguea")
    parser.add_argument("--no-push", action="store_true", help="Escribe local pero no pushea a GitHub")
    args = parser.parse_args()

    tickers = [t.strip().upper() for t in args.tickers.split(",")] if args.tickers else None

    summary = update_fundamentals_csv(
        csv_path=args.csv_path,
        tickers=tickers,
        dry_run=args.dry_run,
        push=not args.no_push,
    )
    print(summary)


if __name__ == "__main__":
    _main()
