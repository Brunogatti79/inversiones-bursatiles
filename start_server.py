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
    """Lee último precio de cierre de cada ticker desde los CSVs del sistema."""
    prices = {}
    try:
        from src.downloader import MERVAL_TICKERS, BOVESPA_TICKERS, SP500_TICKERS
        csv_configs = [
            ("data/merval_cierres.csv", MERVAL_TICKERS),
            ("data/bovespa_cierres.csv", BOVESPA_TICKERS),
            ("data/sp500_cierres.csv", SP500_TICKERS),
        ]
        for csv_path, ticker_map in csv_configs:
            if not os.path.exists(csv_path):
                continue
            df = pd.read_csv(csv_path, sep=";", decimal=",", encoding="utf-8-sig", thousands=" ")
            # Mapear nombre columna → ticker
            nombre_to_ticker = {v: k for k, v in ticker_map.items()}
            for col in df.columns:
                if col == "Fecha":
                    continue
                ticker = nombre_to_ticker.get(col)
                if not ticker:
                    continue
                try:
                    vals = pd.to_numeric(df[col].astype(str).str.replace(" ", "").str.replace(",", "."), errors="coerce")
                    last_val = vals.dropna().iloc[-1] if not vals.dropna().empty else None
                    if last_val and last_val > 0:
                        prices[ticker] = round(float(last_val), 2)
                except Exception:
                    pass
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
        """GET /api/portfolio — devuelve portfolio.json + CCL."""
        try:
            portfolio_path = "data/portfolio.json"
            if os.path.exists(portfolio_path):
                with open(portfolio_path) as f:
                    portfolio = json.load(f)
            else:
                portfolio = {"positions": [], "last_updated": ""}
            ccl_path = "data/ccl_cache.json"
            if os.path.exists(ccl_path):
                with open(ccl_path) as f:
                    portfolio["ccl"] = json.load(f)
            # Enriquecer posiciones con precios actuales desde CSVs del sistema
            if portfolio.get("positions"):
                try:
                    prices_cache = _get_latest_prices()
                    # Obtener CCL para conversión ARS→USD
                    ccl_val = 0.0
                    try:
                        ccl_path = "data/ccl_cache.json"
                        if os.path.exists(ccl_path):
                            with open(ccl_path) as fc:
                                ccl_data = json.load(fc)
                                ccl_val = float(ccl_data.get("compra", 0) or 0)
                    except Exception:
                        pass
                    for p in portfolio["positions"]:
                        t = p.get("ticker", "")
                        if t in prices_cache and prices_cache[t] > 0:
                            precio_ars = prices_cache[t]
                            p["precio_actual"] = precio_ars
                            # Auto-calcular USD: precio_ARS / CCL (igual que broker)
                            if ccl_val > 0:
                                p["precio_actual_usd"] = round(precio_ars / ccl_val, 4)
                                p["valor_actual_usd"]  = round(precio_ars / ccl_val * p.get("cantidad", 1), 2)
                except Exception as e:
                    logger.warning(f"No se pudieron obtener precios de CSVs: {e}")
            self._send_json(200, portfolio)
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
            total_invertido = float(body.get("precio", 0))  # campo "precio" = total invertido en USD
            cantidad = int(body.get("cantidad", 0))
            nombre = body.get("nombre", ticker)
            if not ticker or total_invertido <= 0 or cantidad <= 0:
                self._send_json(400, {"error": "ticker, total invertido y nominales requeridos"})
                return
            precio_unitario = round(total_invertido / cantidad, 6)
            portfolio_path = "data/portfolio.json"
            if os.path.exists(portfolio_path):
                with open(portfolio_path) as f:
                    portfolio = json.load(f)
            else:
                portfolio = {"positions": [], "reglas": {}}
            op_type = "compra" if self.path == "/api/compra" else "venta"
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
                    existing["precio_compra"] = round(new_total_inv / new_cant, 6)
                    existing["cantidad"] = new_cant
                    existing["total_invertido"] = round(new_total_inv, 2)
                    existing["fecha_compra"] = fecha
                    existing["notas"] = f"Promediado {fecha}: +{cantidad} nom, +U$D {total_invertido}"
                    msg = f"Compra agregada a {ticker}, total: U$D {new_total_inv:.0f}, {new_cant} nom"
                else:
                    mercado_form = body.get("mercado", "")
                    if mercado_form:
                        mercado = mercado_form
                    else:
                        mercado = "MERVAL" if ".BA" in ticker else "BOVESPA" if ".SA" in ticker else "SP500"
                    new_pos = {
                        "ticker": ticker, "nombre": nombre,
                        "mercado": mercado, "moneda": "USD",
                        "precio_compra": precio_unitario, "cantidad": cantidad,
                        "total_invertido": round(total_invertido, 2),
                        "fecha_compra": fecha, "stop_loss": None, "target": None,
                        "notas": f"Compra {fecha} via Dashboard",
                    }
                    portfolio.setdefault("positions", []).append(new_pos)
                    msg = f"Nueva posición: {ticker} {cantidad} nom @ U$D {total_invertido:.0f} total"
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
                pnl_pct = round((precio / existing["precio_compra"] - 1) * 100, 2)
                pnl_abs = round((precio - existing["precio_compra"]) * cantidad, 2)
                if cantidad == existing["cantidad"]:
                    portfolio["positions"] = [p for p in portfolio["positions"] if p["ticker"] != ticker]
                    msg = f"Venta total {ticker}. P&L: {pnl_abs} ({pnl_pct}%)"
                else:
                    existing["cantidad"] -= cantidad
                    existing["notas"] = f"Venta parcial {fecha}: -{cantidad} @ {precio}"
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
 
 
def launch_main():
    import time
    time.sleep(2)
    subprocess.run([sys.executable, "main.py"])
 
 
t = threading.Thread(target=launch_main, daemon=True)
t.start()
 
print(f"[start_server] Servidor HTTP en puerto {PORT}", flush=True)
print(f"[start_server] Webhook activo en POST /webhook/run", flush=True)
httpd = HTTPServer(("0.0.0.0", PORT), Handler)
httpd.serve_forever()
 
 
