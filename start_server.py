"""
start_server.py
Punto de entrada para Railway.
Arranca el servidor HTTP PRIMERO (Railway necesita respuesta en PORT),
luego lanza el bot y scheduler en background.
Incluye endpoint /webhook/run para que GitHub Actions dispare el pipeline.
"""
import os
import sys
import subprocess
import threading
import json
import logging
import pandas as pd
from http.server import HTTPServer, BaseHTTPRequestHandler, SimpleHTTPRequestHandler

logger = logging.getLogger(__name__)

# ── Versión del código — para verificar qué está corriendo en Railway ──
import hashlib as _hashlib
_CODE_VERSION = _hashlib.md5(open(__file__, "rb").read()).hexdigest()[:8]
logger.info(f"[START] start_server.py version={_CODE_VERSION}")
 
# ── Actualizar código desde GitHub en cada arranque ──────────────
try:
    result = subprocess.run(
        ["git", "pull", "origin", "main"],
        capture_output=True, text=True, timeout=30
    )
    print(f"[start_server] git pull: {result.stdout.strip()}", flush=True)
    if result.returncode != 0:
        print(f"[start_server] git pull error: {result.stderr.strip()}", flush=True)
except Exception as e:
    print(f"[start_server] git pull excepción: {e}", flush=True)
# ─────────────────────────────────────────────────────────────────
 
OUTPUT_DIR = "outputs"
os.makedirs(OUTPUT_DIR, exist_ok=True)
PORT = int(os.environ.get("PORT", 8080))
WEBHOOK_SECRET = os.environ.get("WEBHOOK_SECRET", "")
 
# Referencia global al hilo del pipeline para evitar ejecuciones simultáneas
_pipeline_lock = threading.Lock()
_pipeline_running = False

# ── Ticker registry y precios desde CSVs ──────────────────────────
def _build_ticker_registry():
    """Construye lista de tickers válidos desde downloader.py."""
    try:
        from src.downloader import MERVAL_TICKERS, BOVESPA_TICKERS, SP500_TICKERS
        registry = []
        for ticker, nombre in MERVAL_TICKERS.items():
            registry.append({"ticker": ticker, "nombre": nombre, "mercado": "MERVAL"})
        for ticker, nombre in BOVESPA_TICKERS.items():
            registry.append({"ticker": ticker, "nombre": nombre, "mercado": "BOVESPA"})
        for ticker, nombre in SP500_TICKERS.items():
            registry.append({"ticker": ticker, "nombre": nombre, "mercado": "SP500"})
        return registry
    except Exception as e:
        logger.warning(f"Error construyendo registry de tickers: {e}")
        return []

def _get_latest_prices():
    """
    Lee último precio de cierre de cada ticker.
    Prioridad: CSV local → GitHub (fallback cuando Railway reinicia y borra el filesystem).
    """
    prices = {}
    try:
        from src.downloader import MERVAL_TICKERS, BOVESPA_TICKERS, SP500_TICKERS
        csv_configs = [
            ("data/merval_cierres.csv",  "data/merval_cierres.csv",  MERVAL_TICKERS),
            ("data/bovespa_cierres.csv", "data/bovespa_cierres.csv", BOVESPA_TICKERS),
            ("data/sp500_cierres.csv",   "data/sp500_cierres.csv",   SP500_TICKERS),
        ]
        gh_token = os.environ.get("GH_TOKEN", "")
        for local_path, repo_path, ticker_map in csv_configs:
            # 1. Intentar leer local
            csv_content = None
            if os.path.exists(local_path):
                try:
                    with open(local_path, encoding="utf-8-sig") as f:
                        csv_content = f.read()
                except Exception:
                    pass
            # 2. Fallback: descargar desde GitHub si no está local
            if not csv_content and gh_token:
                try:
                    import base64 as _b64
                    gh_url = f"https://api.github.com/repos/Brunogatti79/inversiones-bursatiles/contents/{repo_path}"
                    gh_r   = requests.get(gh_url,
                                          headers={"Authorization": f"token {gh_token}"},
                                          timeout=10)
                    if gh_r.ok:
                        csv_content = _b64.b64decode(gh_r.json()["content"]).decode("utf-8-sig")
                        # Guardar localmente para próximas llamadas
                        os.makedirs("data", exist_ok=True)
                        with open(local_path, "w", encoding="utf-8-sig") as f:
                            f.write(csv_content)
                        logger.info(f"[portfolio] CSV descargado desde GitHub: {repo_path}")
                except Exception as e_gh:
                    logger.warning(f"[portfolio] GitHub fallback falló para {repo_path}: {e_gh}")
            if not csv_content:
                continue
            # 3. Parsear CSV
            try:
                import io
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
                            errors="coerce"
                        )
                        last_val = vals.dropna().iloc[-1] if not vals.dropna().empty else None
                        if last_val and last_val > 0:
                            prices[ticker] = round(float(last_val), 2)
                    except Exception:
                        pass
            except Exception as e_parse:
                logger.warning(f"[portfolio] Error parseando {repo_path}: {e_parse}")
    except Exception as e:
        logger.warning(f"Error leyendo precios de CSVs: {e}")
    return prices
# ──────────────────────────────────────────────────────────────────
 
 
def trigger_pipeline():
    """Ejecuta git pull + pipeline en un hilo separado."""
    global _pipeline_running
    with _pipeline_lock:
        if _pipeline_running:
            print("[webhook] Pipeline ya en ejecución, ignorando.", flush=True)
            return False
        _pipeline_running = True
 
    def _run():
        global _pipeline_running
        try:
            # Primero actualizar el código/datos desde GitHub
            print("[webhook] git pull para obtener datos frescos...", flush=True)
            pull = subprocess.run(
                ["git", "pull", "origin", "main"],
                capture_output=True, text=True, timeout=30
            )
            print(f"[webhook] git pull: {pull.stdout.strip()}", flush=True)
 
            # Ahora correr el pipeline
            from src.pipeline import run_pipeline
            print("[webhook] Ejecutando pipeline...", flush=True)
            run_pipeline()
            print("[webhook] Pipeline completado.", flush=True)
        except Exception as e:
            print(f"[webhook] Error en pipeline: {e}", flush=True)
        finally:
            _pipeline_running = False
 
    t = threading.Thread(target=_run, daemon=True)
    t.start()
    return True
 
 
class Handler(SimpleHTTPRequestHandler):
    """Sirve archivos estáticos de OUTPUT_DIR + maneja /webhook/run y /api/portfolio."""
 
    def __init__(self, *args, **kwargs):
        super().__init__(*args, directory=OUTPUT_DIR, **kwargs)
 
    def do_OPTIONS(self):
        """Handle CORS preflight."""
        self.send_response(200)
        self.send_header("Access-Control-Allow-Origin", "*")
        self.send_header("Access-Control-Allow-Methods", "GET, POST, OPTIONS")
        self.send_header("Access-Control-Allow-Headers", "Content-Type, X-Webhook-Secret")
        self.end_headers()
 
    def do_POST(self):
        if self.path == "/webhook/run":
            self._handle_webhook()
        elif self.path in ("/api/compra", "/api/venta"):
            self._handle_portfolio_op()
        else:
            self.send_error(404)
 
    def do_GET(self):
        if self.path == "/webhook/status":
            self._handle_status()
        elif self.path == "/api/health":
            self._handle_health()
        elif self.path == "/api/portfolio":
            self._handle_get_portfolio()
        elif self.path == "/api/ccl":
            self._handle_get_ccl()
        elif self.path == "/api/tickers":
            self._handle_get_tickers()
        else:
            super().do_GET()
 
    def _send_json(self, code, obj):
        self.send_response(code)
        self.send_header("Content-Type", "application/json")
        self.send_header("Access-Control-Allow-Origin", "*")
        self.end_headers()
        self.wfile.write(json.dumps(obj, ensure_ascii=False).encode())
 
    def _read_body(self):
        length = int(self.headers.get("Content-Length", 0))
        body = self.rfile.read(length) if length > 0 else b""
        return json.loads(body) if body else {}
 
    def _handle_get_portfolio(self):
        """GET /api/portfolio — devuelve portfolio.json enriquecido con precios en tiempo real.

        Lógica de precio por fuente (campo precio_fuente en cada posición):
          MERVAL_CSV  (.BA):  precio_ars (CSV) / CCL = precio_usd
          BOVESPA_CSV (.SA):  precio_brl (CSV) / BRL_USD = precio_usd
          SP500_CSV:          precio_usd_nyse (CSV) × ratio_cedear = precio_usd_cedear
        Todos los tickers también devuelven precio_actual_ars = precio_usd × CCL.
        """
        try:
            portfolio_path = "data/portfolio.json"
            if os.path.exists(portfolio_path):
                with open(portfolio_path) as f:
                    portfolio = json.load(f)
            else:
                portfolio = {"positions": [], "last_updated": ""}

            # CCL actual
            ccl_val = 0.0
            ccl_path = "data/ccl_cache.json"
            try:
                if os.path.exists(ccl_path):
                    with open(ccl_path) as fc:
                        ccl_data = json.load(fc)
                        ccl_val = float(ccl_data.get("compra", 0) or 0)
                    portfolio["ccl"] = ccl_data
            except Exception:
                pass
            if ccl_val <= 0:
                ccl_val = 1487.0

            # BRL/USD desde Yahoo Finance
            brl_usd = 5.70
            try:
                import yfinance as yf
                brl_hist = yf.Ticker("BRL=X").history(period="2d")
                if not brl_hist.empty:
                    brl_usd = round(float(brl_hist["Close"].iloc[-1]), 4)
            except Exception:
                pass

            if portfolio.get("positions"):
                try:
                    prices_cache = _get_latest_prices()  # {ticker: precio_en_moneda_local}

                    for p in portfolio["positions"]:
                        t       = p.get("ticker", "")
                        cant    = p.get("cantidad", 1)
                        ini_usd = p.get("valor_inicial_usd", 0)
                        fuente  = p.get("precio_fuente", "")
                        ratio   = p.get("ratio_cedear", 1.0)

                        precio_usd = 0.0
                        precio_ars = 0.0

                        if fuente == "MERVAL_CSV":
                            p_ars = prices_cache.get(t, 0)
                            if p_ars > 0 and ccl_val > 0:
                                precio_ars = round(p_ars, 2)
                                precio_usd = round(p_ars / ccl_val, 4)

                        elif fuente == "BOVESPA_CSV":
                            p_brl = prices_cache.get(t, 0)
                            if p_brl > 0 and brl_usd > 0:
                                precio_usd = round(p_brl / brl_usd, 4)
                                precio_ars = round(precio_usd * ccl_val, 2)

                        elif fuente == "SP500_CSV":
                            # Los CEDEARs cotizan en BYMA con precio propio (distinto del NYSE).
                            # No se puede calcular el precio CEDEAR desde NYSE × ratio_cedear.
                            # Usamos el precio guardado del broker (fallback a continuación).
                            # Cuando exista cedear_cierres.csv, leer de ahí directamente.
                            cedear_csv = "data/cedear_cierres.csv"
                            if os.path.exists(cedear_csv):
                                try:
                                    import io as _io
                                    with open(cedear_csv, encoding="utf-8-sig") as _fc:
                                        _cc = _fc.read()
                                    _dfc = pd.read_csv(_io.StringIO(_cc), sep=";", decimal=",", thousands=" ")
                                    from src.downloader import SP500_TICKERS as _sp_map
                                    _n2t = {v: k for k, v in _sp_map.items()}
                                    for _col in _dfc.columns:
                                        if _col == "Fecha":
                                            continue
                                        _tk = _n2t.get(_col)
                                        if _tk and _tk == t:
                                            _vals = pd.to_numeric(
                                                _dfc[_col].astype(str).str.replace(" ","").str.replace(",","."),
                                                errors="coerce"
                                            ).dropna()
                                            if not _vals.empty and _vals.iloc[-1] > 0:
                                                p_ars_cedear = round(float(_vals.iloc[-1]), 2)
                                                precio_ars = p_ars_cedear
                                                precio_usd = round(p_ars_cedear / ccl_val, 4) if ccl_val > 0 else 0
                                except Exception as _e_ced:
                                    logger.warning(f"[portfolio] cedear_cierres.csv error para {t}: {_e_ced}")

                        if precio_usd <= 0:
                            # Usar valor guardado como fallback
                            precio_usd = p.get("precio_actual_usd", 0)
                            precio_ars = round(precio_usd * ccl_val, 2) if precio_usd > 0 else 0

                        if precio_usd > 0:
                            val_usd = round(precio_usd * cant, 2)
                            val_ars = round(precio_ars * cant, 2)
                            p["precio_actual_usd"] = precio_usd
                            p["precio_actual_ars"] = precio_ars
                            p["valor_actual_usd"]  = val_usd
                            p["valor_actual_ars"]  = val_ars
                            if ini_usd > 0:
                                p["rend_usd"] = round(val_usd - ini_usd, 2)
                                p["rend_pct"] = round((val_usd / ini_usd - 1) * 100, 2)

                    # Totales
                    total_usd  = round(sum(p.get("valor_actual_usd", p.get("valor_inicial_usd", 0)) for p in portfolio["positions"]), 2)
                    total_ars  = round(sum(p.get("valor_actual_ars", 0) for p in portfolio["positions"]), 2)
                    capital_ref = portfolio.get("capital_usd_ref", 0)
                    portfolio["capital_usd"]  = total_usd
                    portfolio["capital_ars"]  = total_ars
                    portfolio["pl_total_usd"] = round(total_usd - capital_ref, 2)
                    portfolio["pl_total_pct"] = round((total_usd / capital_ref - 1) * 100, 2) if capital_ref > 0 else 0.0
                    portfolio["ccl_usado"]    = ccl_val

                except Exception as e:
                    logger.warning(f"Error enriqueciendo portfolio: {e}")

            self.send_response(200)
            self.send_header("Content-Type", "application/json")
            self.send_header("Access-Control-Allow-Origin", "*")
            self.end_headers()
            import json as _j
            self.wfile.write(_j.dumps(portfolio, ensure_ascii=False, default=str).encode())
            return
        except Exception as e:
            self._send_json(500, {"error": str(e)})
 
    def _handle_get_ccl(self):
        """GET /api/ccl — devuelve CCL actual."""
        try:
            from src.macro_auto import get_ccl_data
            ccl = get_ccl_data()
            self._send_json(200, ccl)
        except Exception as e:
            self._send_json(500, {"error": str(e)})

    def _handle_get_tickers(self):
        """GET /api/tickers — devuelve lista de tickers válidos con precios."""
        try:
            registry = _build_ticker_registry()
            prices = _get_latest_prices()
            for item in registry:
                t = item["ticker"]
                if t in prices:
                    item["precio_actual"] = prices[t]
            self._send_json(200, {"tickers": registry})
        except Exception as e:
            self._send_json(500, {"error": str(e)})
 
    def _handle_portfolio_op(self):
        """POST /api/compra o /api/venta — registrar operación (sin auth, acceso público desde dashboard)."""
        try:
            body = self._read_body()
            ticker = body.get("ticker", "").upper()
            precio_raw     = float(body.get("precio", 0))   # precio unitario USD (frontend)
            total_usd_raw  = float(body.get("total_usd", 0))# total USD (frontend)
            cantidad = int(body.get("cantidad", 0))
            nombre = body.get("nombre", ticker)
            op_type_pre = "compra" if self.path == "/api/compra" else "venta"
            if not ticker or precio_raw <= 0 or cantidad <= 0:
                self._send_json(400, {"error": "ticker, precio y nominales requeridos"})
                return
            # Para COMPRA: total_invertido = precio_unitario * cantidad (precio ya es unitario)
            # Para VENTA:  precio_unitario = precio_raw directamente
            precio_unitario  = round(precio_raw, 6)
            total_invertido  = round(precio_unitario * cantidad, 2) if total_usd_raw <= 0 else round(total_usd_raw, 2)
            portfolio_path = "data/portfolio.json"
            if os.path.exists(portfolio_path):
                with open(portfolio_path) as f:
                    portfolio = json.load(f)
            else:
                portfolio = {"positions": [], "reglas": {}}
            op_type = op_type_pre  # ya calculado arriba
            from datetime import datetime as dt
            fecha = dt.now().strftime("%Y-%m-%d")
            if op_type == "compra":
                existing = None
                for p in portfolio.get("positions", []):
                    if p["ticker"] == ticker:
                        existing = p
                        break
                if existing:
                    old_total = existing.get("total_invertido", existing["precio_compra"] * existing["cantidad"])
                    new_total_inv = old_total + total_invertido
                    new_cant = existing["cantidad"] + cantidad
                    new_precio_usd = round(new_total_inv / new_cant, 6)
                    existing["precio_compra"]     = new_precio_usd
                    existing["precio_compra_usd"] = new_precio_usd
                    existing["cantidad"]          = new_cant
                    existing["valor_inicial_usd"] = round(new_total_inv, 2)
                    existing["valor_actual_usd"]  = round(new_total_inv, 2)
                    existing["rend_usd"]          = 0
                    existing["fecha_compra"] = fecha
                    existing["notas"] = f"Promediado {fecha}: +{cantidad} nom, +U$D {total_invertido}"
                    msg = f"Compra agregada a {ticker}, total: U$D {new_total_inv:.0f}, {new_cant} nom"
                else:
                    mercado_form = body.get("mercado", "")
                    if mercado_form:
                        mercado = mercado_form
                    else:
                        mercado = "MERVAL" if ".BA" in ticker else "BOVESPA" if ".SA" in ticker else "SP500"
                    # Tipo instrumento y ratio CEDEAR — enviados desde el formulario
                    precio_fuente = body.get("precio_fuente", "")
                    ratio_cedear  = float(body.get("ratio_cedear", 1.0) or 1.0)
                    # Inferir precio_fuente si no viene del formulario (compatibilidad)
                    if not precio_fuente:
                        if ticker.endswith(".BA"):
                            precio_fuente = "MERVAL_CSV"
                        elif ticker.endswith(".SA"):
                            precio_fuente = "BOVESPA_CSV"
                        else:
                            precio_fuente = "SP500_CSV"
                    new_pos = {
                        "ticker": ticker, "nombre": nombre,
                        "mercado": mercado, "moneda": "USD",
                        "precio_compra":     precio_unitario,
                        "precio_compra_usd": precio_unitario,
                        "precio_actual_usd": precio_unitario,
                        "cantidad": cantidad,
                        "valor_inicial_usd": round(total_invertido, 2),
                        "valor_actual_usd":  round(total_invertido, 2),
                        "rend_usd": 0,
                        "fecha_compra": fecha,
                        "stop_loss": None, "target": None,
                        "precio_fuente": precio_fuente,
                        "ratio_cedear":  ratio_cedear if precio_fuente == "SP500_CSV" else 1.0,
                        "notas": f"Compra {fecha} via Dashboard — {precio_fuente}",
                    }
                    portfolio.setdefault("positions", []).append(new_pos)
                    msg = f"Nueva posición: {ticker} {cantidad} nom @ U$D {total_invertido:.0f} total [{precio_fuente}]"
            else:
                existing = None
                for p in portfolio.get("positions", []):
                    if p["ticker"] == ticker:
                        existing = p
                        break
                if not existing:
                    self._send_json(404, {"error": f"No hay posición en {ticker}"})
                    return
                if cantidad > existing["cantidad"]:
                    self._send_json(400, {"error": f"Solo tenés {existing['cantidad']} de {ticker}"})
                    return
                pc = existing.get("precio_compra_usd") or existing.get("precio_compra", 0)
                pnl_pct = round((precio_unitario / pc - 1) * 100, 2) if pc > 0 else 0
                pnl_abs = round((precio_unitario - pc) * cantidad, 2)
                if cantidad == existing["cantidad"]:
                    portfolio["positions"] = [p for p in portfolio["positions"] if p["ticker"] != ticker]
                    msg = f"Venta total {ticker}. P&L: {pnl_abs} ({pnl_pct}%)"
                else:
                    restantes = existing["cantidad"] - cantidad
                    existing["cantidad"] = restantes
                    existing["valor_inicial_usd"] = round((existing.get("precio_compra_usd") or existing.get("precio_compra",0)) * restantes, 2)
                    existing["valor_actual_usd"]  = round(precio_unitario * restantes, 2)
                    existing["rend_usd"] = round((precio_unitario - (existing.get("precio_compra_usd") or existing.get("precio_compra",0))) * restantes, 2)
                    existing["notas"] = f"Venta parcial {fecha}: -{cantidad} @ USD {precio_unitario}"
                    msg = f"Venta parcial {ticker}. P&L: {pnl_abs} ({pnl_pct}%)"
            portfolio["last_updated"] = dt.now().strftime("%Y-%m-%d %H:%M")
            with open(portfolio_path, "w") as f:
                json.dump(portfolio, f, ensure_ascii=False, indent=2)
            # Push a GitHub
            pushed = False
            try:
                import requests as req
                import base64
                gh_token = os.environ.get("GH_TOKEN", "")
                if gh_token:
                    repo = "Brunogatti79/inversiones-bursatiles"
                    path = "data/portfolio.json"
                    url = f"https://api.github.com/repos/{repo}/contents/{path}"
                    headers = {"Authorization": f"token {gh_token}", "Accept": "application/vnd.github.v3+json"}
                    r = req.get(url, headers=headers)
                    sha = r.json().get("sha", "") if r.status_code == 200 else ""
                    with open(portfolio_path) as f:
                        content = f.read()
                    encoded = base64.b64encode(content.encode()).decode()
                    data_gh = {"message": f"api: {op_type} {ticker} {fecha}", "content": encoded}
                    if sha:
                        data_gh["sha"] = sha
                    r = req.put(url, headers=headers, json=data_gh)
                    pushed = r.status_code in (200, 201)
            except Exception as e:
                print(f"[api] GitHub push error: {e}", flush=True)
            # Notificar por Telegram
            try:
                bot_token = os.environ.get("TELEGRAM_BOT_TOKEN", "")
                chat_id = os.environ.get("TELEGRAM_CHAT_ID", "")
                if bot_token and chat_id:
                    icon = "\U0001f7e2" if op_type == "compra" else "\U0001f534"
                    text = f"{icon} <b>{op_type.upper()} via Dashboard</b>\n{ticker} \u2014 {cantidad} @ ${precio:,.2f}"
                    import requests as req2
                    req2.post(f"https://api.telegram.org/bot{bot_token}/sendMessage",
                        json={"chat_id": chat_id, "text": text, "parse_mode": "HTML"}, timeout=10)
            except Exception:
                pass
            self._send_json(200, {"status": "ok", "msg": msg, "pushed": pushed})
            print(f"[api] {op_type}: {ticker} {cantidad} @ {precio} | pushed={pushed}", flush=True)
        except Exception as e:
            self._send_json(500, {"error": str(e)})
            print(f"[api] Error: {e}", flush=True)
 

    def _handle_health(self):
        """GET /api/health — devuelve métricas de salud del sistema."""
        try:
            health_path = "data/health_metrics.json"
            if os.path.exists(health_path):
                with open(health_path) as f:
                    health = json.load(f)
            else:
                health = {"sla_status": "UNKNOWN", "error": "No health data yet"}

            # SLA check en tiempo real
            last_success = health.get("last_success")
            if last_success:
                from datetime import datetime as _dt
                try:
                    last_dt   = _dt.fromisoformat(last_success)
                    hours_ago = (_dt.now() - last_dt).total_seconds() / 3600
                    if hours_ago > 14:
                        health["sla_status"] = "CRITICAL"
                    elif hours_ago > 8:
                        health["sla_status"] = "WARNING"
                    else:
                        health["sla_status"] = "OK"
                    health["sla_hours_since_success"] = round(hours_ago, 1)
                except Exception:
                    pass

            health["pipeline_running"] = _pipeline_running
            self._send_json(200, health)
        except Exception as e:
            self._send_json(500, {"error": str(e), "sla_status": "ERROR"})

    def _handle_webhook(self):
        # Verificar secret si está configurado
        if WEBHOOK_SECRET:
            auth = self.headers.get("X-Webhook-Secret", "")
            if auth != WEBHOOK_SECRET:
                self.send_response(403)
                self.send_header("Content-Type", "application/json")
                self.end_headers()
                self.wfile.write(json.dumps({"error": "forbidden"}).encode())
                print("[webhook] Intento rechazado: secret inválido", flush=True)
                return
 
        started = trigger_pipeline()
        status_code = 200 if started else 409
        msg = "pipeline iniciado" if started else "pipeline ya en ejecución"
 
        self.send_response(status_code)
        self.send_header("Content-Type", "application/json")
        self.end_headers()
        self.wfile.write(json.dumps({"status": msg}).encode())
        print(f"[webhook] {msg}", flush=True)
 
    def _handle_status(self):
        self.send_response(200)
        self.send_header("Content-Type", "application/json")
        self.end_headers()
        self.wfile.write(json.dumps({
            "pipeline_running": _pipeline_running,
            "status": "ok"
        }).encode())
 
    def log_message(self, format, *args):
        pass
 
 
def _sync_portfolio_from_github():
    """Descarga portfolio.json fresco de GitHub al arrancar Railway."""
    gh_token = os.environ.get("GH_TOKEN", "")
    if not gh_token:
        return
    try:
        import urllib.request as _ur, json as _j, base64 as _b64
        url = "https://api.github.com/repos/Brunogatti79/inversiones-bursatiles/contents/data/portfolio.json"
        req = _ur.Request(url, headers={"Authorization": f"token {gh_token}"})
        with _ur.urlopen(req, timeout=10) as r:
            d = _j.loads(r.read())
            content = _b64.b64decode(d["content"]).decode("utf-8")
            os.makedirs("data", exist_ok=True)
            with open("data/portfolio.json", "w") as f:
                f.write(content)
        print("[start_server] portfolio.json sincronizado desde GitHub", flush=True)
    except Exception as e:
        print(f"[start_server] No se pudo sincronizar portfolio.json: {e}", flush=True)


def launch_main():
    import time
    time.sleep(2)
    subprocess.run([sys.executable, "main.py"])
 
 
t = threading.Thread(target=launch_main, daemon=True)
t.start()
 
_sync_portfolio_from_github()
print(f"[start_server] Servidor HTTP en puerto {PORT}", flush=True)
print(f"[start_server] Webhook activo en POST /webhook/run", flush=True)
httpd = HTTPServer(("0.0.0.0", PORT), Handler)
httpd.serve_forever()
 
 
