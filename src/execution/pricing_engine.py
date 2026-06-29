"""
src/execution/pricing_engine.py

Única fuente de verdad para el precio de una posición del portfolio.

FIX 26/06/2026 (auditoría): antes de este archivo existían DOS implementaciones
de pricing, ambas parciales:
  1. tracker.update_portfolio_usd() — usaba 3 diccionarios hardcodeados
     (MERVAL_MAP: 4 tickers, BOVESPA_MAP: 1 ticker, CEDEAR_MAP: 9 tickers)
     que cubrían 14 de los 78 tickers del universo. Cualquier posición con
     un ticker fuera de esos diccionarios quedaba congelada para siempre
     en el precio de compra, sin aviso visible. Confirmado en datos reales:
     GGAL.BA, BMA.BA, EDN.BA, GOOGL mostraban 0,00% de rendimiento durante
     semanas pese a que sus precios reales sí se movieron.
  2. start_server.py _handle_get_portfolio() — ya generalizaba MERVAL_CSV
     y BOVESPA_CSV correctamente vía precio_fuente + _get_latest_prices()
     (sin diccionario manual), pero el camino SP500_CSV/CEDEAR dependía de
     data/cedear_cierres.csv, que NUNCA existió en el repo (0 commits en
     toda la historia de git) — así que ese camino también caía siempre al
     fallback "mantener valor guardado".

Este módulo generaliza los TRES caminos de conversión de moneda (que SÍ son
reales y necesarios — no se pueden colapsar en una sola fórmula, cada
mercado cotiza en una moneda distinta) para que cubran automáticamente
cualquier ticker de los 78, usando como única fuente de verdad los mismos
diccionarios ticker↔nombre que ya usa downloader.py para análisis
(MERVAL_TICKERS / BOVESPA_TICKERS / SP500_TICKERS) — en vez de mantener una
lista manual aparte que hay que recordar actualizar cada vez que se agrega
un ticker nuevo al universo.

Los tres caminos:
  MERVAL_CSV  (.BA real, ej. GGAL.BA):  precio_ars (signal) / CCL
  BOVESPA_CSV (.SA, ej. PETR4.SA):      precio_brl (signal) / BRL_USD
  SP500_CSV   (CEDEAR, ej. GLOB, AAPL): precio real de BYMA vía data912.com.

✅ ACTUALIZACIÓN 26/06/2026 (sesión 2) — CEDEAR resuelto con fuente real:

Antes (misma sesión, más temprano): se intentó la aproximación
`nyse_usd × ratio_cedear` y se descartó — dio rendimientos absurdos (MELI
+3493%, IBB +14948%, etc.) porque ni la fórmula ni el ratio_cedear guardado
eran confiables (verificado contra la tabla real de BYMA/COMAFI).

Ahora: `fetch_live_cedear_usd_prices()` consulta data912.com
(https://data912.com/live/arg_cedears), una API pública sin autenticación
que ya usan herramientas argentinas reales (rendimientos.co/cedears, entre
otras). Para cada ticker, prioriza la línea "{ticker}D" — la variante
dólar-MEP del mismo CEDEAR, que es un instrumento que cotiza DIRECTO en
dólares en BYMA. Esto elimina por completo la necesidad de ratio_cedear o
de CCL para estos tickers: es un precio de mercado real, ya en USD, no una
aproximación teórica. Confirmado funcionando en vivo (26/06/2026): GLOB
(c=2376 ARS, GLOBD c=1.61 USD), IBB, EWZ, COPX con sus variantes D.

Si un ticker no tiene línea "D" (CEDEAR poco operado), cae a "{ticker}"
(ARS) / CCL como red de respaldo — ahí sí queda sujeto al spread CCL
implícito de ese CEDEAR específico, pero sigue siendo un precio real de
mercado, no una fórmula inventada.

data912 es código abierto/educativo, sin SLA garantizado (cache de ~2hs en
Cloudflare, rate limit ~120 req/min) — mismo nivel de garantía que el
scraping de Ámbito que ya usa macro_auto.py para riesgo país. Si en algún
momento deja de responder, el sistema cae al último snapshot persistido en
data/cedear_cierres.csv (mismo patrón que el resto de los datos del
proyecto: vive en GitHub porque el filesystem de Railway es efímero).
"""

import os
import io
import json
import logging
from datetime import datetime

import pandas as pd

logger = logging.getLogger(__name__)

PORTFOLIO_PATH = "data/portfolio.json"
CCL_CACHE_PATH = "data/ccl_cache.json"
CEDEAR_CSV_PATH = "data/cedear_cierres.csv"  # no existe hoy — ver advertencia arriba

FALLBACK_CCL = 1487.0
FALLBACK_BRL_USD = 5.70


def get_latest_prices_by_ticker(gh_token: str = None) -> dict:
    """
    Devuelve {ticker: precio_en_moneda_local} para TODOS los tickers de los
    3 mercados (78 en total), leyendo directamente los 3 CSV de cierres.
    Generaliza automáticamente vía MERVAL_TICKERS/BOVESPA_TICKERS/SP500_TICKERS
    (downloader.py) — no requiere mantener una lista manual aparte.

    Relocado desde start_server.py (antes "_get_latest_prices", privada y
    usada solo por el handler GET) — misma lógica, ahora reusable también
    desde el pipeline en vez de tener dos copias del mismo parseo de CSV.
    """
    from src.downloader import MERVAL_TICKERS, BOVESPA_TICKERS, SP500_TICKERS

    prices = {}
    gh_token = gh_token or os.environ.get("GH_TOKEN", "")
    csv_configs = [
        ("data/merval_cierres.csv",  "data/merval_cierres.csv",  MERVAL_TICKERS),
        ("data/bovespa_cierres.csv", "data/bovespa_cierres.csv", BOVESPA_TICKERS),
        ("data/sp500_cierres.csv",   "data/sp500_cierres.csv",   SP500_TICKERS),
    ]
    for local_path, repo_path, ticker_map in csv_configs:
        csv_content = None
        if os.path.exists(local_path):
            try:
                with open(local_path, encoding="utf-8-sig") as f:
                    csv_content = f.read()
            except Exception:
                pass
        if not csv_content and gh_token:
            try:
                import base64
                import requests
                gh_url = f"https://api.github.com/repos/Brunogatti79/inversiones-bursatiles/contents/{repo_path}"
                gh_r = requests.get(gh_url, headers={"Authorization": f"token {gh_token}"}, timeout=10)
                if gh_r.ok:
                    csv_content = base64.b64decode(gh_r.json()["content"]).decode("utf-8-sig")
                    os.makedirs(os.path.dirname(local_path) or ".", exist_ok=True)
                    with open(local_path, "w", encoding="utf-8-sig") as f:
                        f.write(csv_content)
            except Exception as e:
                logger.warning(f"[pricing_engine] GitHub fallback falló para {repo_path}: {e}")
        if not csv_content:
            continue
        try:
            df = pd.read_csv(io.StringIO(csv_content), sep=";", decimal=",", thousands=" ")
            nombre_to_ticker = {v: k for k, v in ticker_map.items()}
            for col in df.columns:
                if col == "Fecha":
                    continue
                ticker = nombre_to_ticker.get(col)
                if not ticker:
                    continue
                try:
                    vals = pd.to_numeric(
                        df[col].astype(str).str.replace(" ", "").str.replace(",", "."),
                        errors="coerce",
                    )
                    last_val = vals.dropna().iloc[-1] if not vals.dropna().empty else None
                    if last_val and last_val > 0:
                        prices[ticker] = round(float(last_val), 2)
                except Exception:
                    pass
        except Exception as e:
            logger.warning(f"[pricing_engine] Error parseando {repo_path}: {e}")
    return prices


def get_ccl(signals: list = None) -> float:
    """CCL actual. Prioridad: cache → señales GGAL.BA/GGAL → fallback fijo."""
    try:
        if os.path.exists(CCL_CACHE_PATH):
            with open(CCL_CACHE_PATH) as f:
                ccl = float(json.load(f).get("compra", 0) or 0)
                if ccl > 0:
                    return ccl
    except Exception:
        pass
    if signals:
        try:
            g_ba = next((s for s in signals if s.get("ticker") == "GGAL.BA"), None)
            g_us = next((s for s in signals if s.get("ticker") == "GGAL"), None)
            if g_ba and g_us and g_ba.get("precio_actual", 0) > 0 and g_us.get("precio_actual", 0) > 0:
                return round(g_ba["precio_actual"] / g_us["precio_actual"] * 10, 2)
        except Exception:
            pass
    return FALLBACK_CCL


def get_brl_usd(signals: list = None) -> float:
    """BRL/USD actual. Prioridad: señales PETR4.SA/PBR → Yahoo en vivo → fallback fijo."""
    if signals:
        try:
            petr4 = next((s for s in signals if s.get("ticker") == "PETR4.SA"), None)
            pbr   = next((s for s in signals if s.get("ticker") == "PBR"), None)
            if petr4 and pbr:
                p_brl = petr4.get("precio_actual", 0)
                p_usd = pbr.get("precio_actual", 0)
                if p_brl > 0 and p_usd > 0:
                    brl_usd = round(p_brl / (p_usd * 2.6875), 4)
                    if 3 <= brl_usd <= 10:
                        return brl_usd
        except Exception:
            pass
    try:
        import yfinance as yf
        hist = yf.Ticker("BRL=X").history(period="2d")
        if not hist.empty:
            return round(float(hist["Close"].iloc[-1]), 4)
    except Exception:
        pass
    return FALLBACK_BRL_USD


def resolve_position_price(
    ticker: str,
    mercado: str,
    precio_fuente: str,
    ratio_cedear: float,
    local_prices: dict,
    ccl: float,
    brl_usd: float,
    cedear_prices: dict = None,
) -> tuple:
    """
    Resuelve (precio_usd, precio_ars, metodo) para UN ticker, sin diccionarios
    manuales — generaliza a cualquiera de los 78 tickers del universo según
    su precio_fuente (MERVAL_CSV / BOVESPA_CSV / SP500_CSV).

    local_prices:  {ticker: precio_en_moneda_local} — de get_latest_prices_by_ticker()
    cedear_prices: {ticker: precio_usd} — de fetch_live_cedear_usd_prices()
                   (data912.com, en vivo) o del último snapshot persistido en
                   data/cedear_cierres.csv. Ya viene en USD real de mercado —
                   no requiere ratio_cedear ni CCL (ver docstring del módulo).
    """
    precio_usd, precio_ars, metodo = 0.0, 0.0, "sin_precio"

    if precio_fuente == "MERVAL_CSV":
        p_ars = local_prices.get(ticker, 0)
        if p_ars > 0 and ccl > 0:
            precio_ars = round(p_ars, 2)
            precio_usd = round(p_ars / ccl, 6)
            metodo = "merval_signal_ccl"

    elif precio_fuente == "BOVESPA_CSV":
        p_brl = local_prices.get(ticker, 0)
        if p_brl > 0 and brl_usd > 0:
            precio_usd = round(p_brl / brl_usd, 6)
            precio_ars = round(precio_usd * ccl, 2) if ccl > 0 else 0.0
            metodo = "bovespa_signal_brlusd"

    elif precio_fuente == "SP500_CSV":
        p_usd_cedear = (cedear_prices or {}).get(ticker, 0)
        if p_usd_cedear > 0:
            precio_usd = round(p_usd_cedear, 6)
            precio_ars = round(precio_usd * ccl, 2) if ccl > 0 else 0.0
            metodo = "cedear_real_data912"
        else:
            # data912 caído y sin snapshot previo persistido — mejor no
            # resolver que inventar un número (ver docstring del módulo:
            # la aproximación nyse_usd × ratio_cedear ya se descartó por
            # dar resultados absurdos).
            metodo = "cedear_sin_fuente_confiable"

    return precio_usd, precio_ars, metodo


CEDEAR_LIVE_URL = "https://data912.com/live/arg_cedears"


def fetch_live_cedear_usd_prices(timeout: float = 10.0) -> dict:
    """
    Trae precios reales de CEDEARs desde data912.com (API pública, sin
    autenticación, sin costo — usada también por herramientas reales como
    rendimientos.co/cedears). Confirmado funcionando en vivo el 26/06/2026.

    Prioriza la línea "{ticker}D" (variante dólar-MEP del mismo CEDEAR):
    es un instrumento que cotiza DIRECTO en dólares en BYMA, así que da el
    precio USD real de mercado sin necesitar ratio_cedear ni CCL — elimina
    de raíz la ambigüedad de ratio que hizo descartar la aproximación
    nyse_usd × ratio_cedear más temprano en esta sesión. Si no existe línea
    D para un ticker (CEDEAR poco operado), cae a "{ticker}" (ARS) / CCL.

    Devuelve {ticker: precio_usd}. Vacío si data912 no responde — el
    llamador cae al último snapshot persistido en cedear_cierres.csv.
    """
    import requests
    from src.downloader import SP500_TICKERS

    try:
        resp = requests.get(CEDEAR_LIVE_URL, timeout=timeout)
        resp.raise_for_status()
        data = resp.json()
    except Exception as e:
        logger.warning(f"[pricing_engine] data912 arg_cedears no disponible: {e}")
        return {}

    if not isinstance(data, list):
        logger.warning("[pricing_engine] data912 arg_cedears: formato inesperado")
        return {}

    by_symbol = {item.get("symbol"): item for item in data if isinstance(item, dict)}
    ccl_local = get_ccl()

    prices = {}
    for ticker in SP500_TICKERS:
        d_line = by_symbol.get(f"{ticker}D")
        try:
            if d_line and float(d_line.get("c") or 0) > 0:
                prices[ticker] = round(float(d_line["c"]), 6)
                continue
        except (TypeError, ValueError):
            pass
        ars_line = by_symbol.get(ticker)
        try:
            if ars_line and float(ars_line.get("c") or 0) > 0 and ccl_local > 0:
                prices[ticker] = round(float(ars_line["c"]) / ccl_local, 6)
        except (TypeError, ValueError):
            pass

    return prices


def _persist_cedear_snapshot(prices: dict):
    """Guarda el snapshot de hoy en data/cedear_cierres.csv y lo pushea a
    GitHub — mismo motivo que el resto de los datos del proyecto: el
    filesystem de Railway es efímero y se resetea en cada redeploy.

    FIX 29/06/2026: antes esto sobreescribía el archivo completo en cada
    corrida (df = pd.DataFrame([prices]); to_csv sin leer lo existente) —
    nunca acumulaba más que la fila del día en curso. Confirmado en
    producción: el archivo lleva desde el 26/06 (introducido en 4.8) y
    seguía teniendo una sola fila el 29/06. Esto invalidaba el propósito
    del archivo (construir un histórico real de precio CEDEAR en USD para
    eventualmente calcular un ATR propio en vez de usar la serie NYSE como
    proxy). Ahora lee el histórico existente, reemplaza solo la fila de HOY
    si ya corrió antes en el mismo día (varias corridas/día → última gana),
    y conserva las filas de días anteriores — recién así se puede acumular
    una serie real con el tiempo.
    """
    if not prices:
        return
    hoy = datetime.now().strftime("%Y-%m-%d")
    try:
        hist = pd.DataFrame()
        if os.path.exists(CEDEAR_CSV_PATH):
            try:
                hist = pd.read_csv(CEDEAR_CSV_PATH, sep=";", decimal=",", encoding="utf-8-sig")
            except Exception as e:
                logger.warning(f"[pricing_engine] No se pudo leer histórico previo de cedear_cierres.csv, se empieza de cero: {e}")
                hist = pd.DataFrame()

        if not hist.empty and "Fecha" in hist.columns:
            hist = hist[hist["Fecha"].astype(str) != hoy]  # sacar la fila de hoy si ya existía (re-run del mismo día)

        fila_nueva = dict(prices)
        fila_nueva["Fecha"] = hoy
        df = pd.concat([hist, pd.DataFrame([fila_nueva])], ignore_index=True, sort=False)
        cols = ["Fecha"] + [c for c in df.columns if c != "Fecha"]
        df = df[cols]

        df.to_csv(CEDEAR_CSV_PATH, sep=";", decimal=",", index=False, encoding="utf-8-sig")
        from src.github_persistence import push_file
        push_file(CEDEAR_CSV_PATH, f"auto: cedear_cierres {datetime.now().strftime('%Y-%m-%d %H:%M')}")
    except Exception as e:
        logger.warning(f"[pricing_engine] Error persistiendo cedear_cierres.csv: {e}")


def _load_cedear_prices() -> dict:
    """
    Precios CEDEAR en USD. Intenta en vivo contra data912.com primero
    (fuente real de BYMA); si falla (red caída, rate limit, etc.) cae al
    último snapshot persistido en data/cedear_cierres.csv.
    """
    live = fetch_live_cedear_usd_prices()
    if live:
        _persist_cedear_snapshot(live)
        return live

    if not os.path.exists(CEDEAR_CSV_PATH):
        return {}
    try:
        df = pd.read_csv(CEDEAR_CSV_PATH, sep=";", decimal=",", encoding="utf-8-sig")
        if df.empty:
            return {}
        last = df.iloc[-1]
        out = {}
        for col in df.columns:
            if col == "Fecha":
                continue
            try:
                val = float(last[col])
                if val > 0:
                    out[col] = val
            except (TypeError, ValueError):
                pass
        return out
    except Exception as e:
        logger.warning(f"[pricing_engine] Error leyendo cedear_cierres.csv: {e}")
        return {}


def refresh_portfolio_prices(signals: list = None, persist: bool = True, brl_usd_override: float = None) -> dict:
    """
    Punto de entrada único de pricing. Reemplaza:
      - tracker.update_portfolio_usd()        (llamado desde pipeline.py, persist=True)
      - start_server._handle_get_portfolio()  (llamado en vivo, persist=False)

    Cobertura: los 78 tickers del universo, automáticamente — sin
    diccionario manual que haya que recordar extender cuando se agrega
    un ticker nuevo.

    brl_usd_override: si se pasa (>0), tiene prioridad sobre el cálculo
    interno de get_brl_usd(). pipeline.py lo usa para pasar el BRL/USD que
    ya calculó macro_auto.py — una fuente que este módulo no consulta por
    su cuenta.
    """
    if not os.path.exists(PORTFOLIO_PATH):
        return {}
    try:
        with open(PORTFOLIO_PATH) as f:
            portfolio = json.load(f)
    except Exception as e:
        logger.warning(f"[pricing_engine] Error leyendo portfolio: {e}")
        return {}

    positions = portfolio.get("positions", [])
    if not positions:
        return portfolio

    ccl = get_ccl(signals)
    brl_usd = brl_usd_override if brl_usd_override and brl_usd_override > 0 else get_brl_usd(signals)
    local_prices = get_latest_prices_by_ticker()
    cedear_prices = _load_cedear_prices()

    updated, frozen = 0, []
    for pos in positions:
        ticker  = pos.get("ticker", "")
        cant    = pos.get("cantidad", 0)
        ini_usd = pos.get("valor_inicial_usd", 0)
        if not ticker or not cant or not ini_usd:
            continue

        mercado       = pos.get("mercado", "")
        precio_fuente = pos.get("precio_fuente") or (
            "MERVAL_CSV" if ticker.endswith(".BA") else
            "BOVESPA_CSV" if ticker.endswith(".SA") else
            "SP500_CSV"
        )
        ratio_cedear = float(pos.get("ratio_cedear", 1.0) or 1.0)

        precio_usd, precio_ars, metodo = resolve_position_price(
            ticker, mercado, precio_fuente, ratio_cedear,
            local_prices, ccl, brl_usd, cedear_prices,
        )

        if precio_usd <= 0:
            pos["precio_metodo"] = metodo
            frozen.append(ticker)
            continue

        val_usd = round(precio_usd * cant, 2)
        pos["precio_actual_usd"] = precio_usd
        pos["precio_actual_ars"] = precio_ars
        pos["valor_actual_usd"]  = val_usd
        pos["valor_actual_ars"]  = round(precio_ars * cant, 2)
        pos["rend_usd"]          = round(val_usd - ini_usd, 2)
        pos["rend_pct"]          = round((val_usd / ini_usd - 1) * 100, 2) if ini_usd > 0 else 0.0
        pos["precio_metodo"]     = metodo
        updated += 1

    total_usd = round(
        sum(p.get("valor_actual_usd", p.get("valor_inicial_usd", 0)) for p in positions), 2
    )
    capital_ref = portfolio.get("capital_usd_ref", 0)
    portfolio["capital_usd"]   = total_usd
    portfolio["pl_total_usd"]  = round(total_usd - capital_ref, 2)
    portfolio["pl_total_pct"]  = round((total_usd / capital_ref - 1) * 100, 2) if capital_ref > 0 else 0.0
    portfolio["ccl_usado"]     = ccl
    portfolio["brl_usd_usado"] = brl_usd
    portfolio["last_updated"]  = datetime.now().strftime("%Y-%m-%d %H:%M")

    if frozen:
        logger.warning(f"[pricing_engine] {len(frozen)} posiciones sin precio resoluble: {frozen}")

    if persist:
        try:
            with open(PORTFOLIO_PATH, "w") as f:
                json.dump(portfolio, f, ensure_ascii=False, indent=2)
            from src.github_persistence import push_file
            push_file(PORTFOLIO_PATH, f"auto: portfolio USD actualizado {datetime.now().strftime('%Y-%m-%d %H:%M')}")
        except Exception as e:
            logger.warning(f"[pricing_engine] Error guardando/pusheando portfolio: {e}")

    logger.info(
        f"[pricing_engine] Portfolio actualizado: {updated}/{len(positions)} posiciones | "
        f"Total=${total_usd:,.2f} | CCL={ccl:.1f} | BRL/USD={brl_usd:.4f}"
    )
    return portfolio
