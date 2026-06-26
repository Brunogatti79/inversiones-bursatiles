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

        FIX 26/06/2026: esta lógica de pricing (que ya generalizaba bien
        MERVAL_CSV/BOVESPA_CSV pero nunca pudo resolver SP500_CSV/CEDEAR
        porque data/cedear_cierres.csv nunca existió) se consolidó en
        src/execution/pricing_engine.py — es la MISMA función que usa el
        pipeline (tracker.update_portfolio_usd), para que no vuelvan a
        existir dos implementaciones de pricing desincronizadas. Acá se
        llama con persist=False: esto se sirve en cada GET (potencialmente
        cada 60s por el auto-refresh del dashboard) y no debe escribir a
        disco ni pushear a GitHub en cada request — eso lo sigue haciendo
        solo el pipeline, 4 veces por día.
        """
        try:
            from src.execution.pricing_engine import refresh_portfolio_prices
            portfolio = refresh_portfolio_prices(signals=None, persist=False)
            if not portfolio:
                portfolio = {"positions": [], "last_updated": ""}
            self._send_json(200, portfolio)
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
        """POST /api/compra o /api/venta — registrar operación (sin auth, acceso público desde dashboard).

        FIX 26/06/2026: la lógica de negocio (promediado, venta parcial/total,
        push a GitHub, notificación Telegram) se extrajo a
        src/execution/order_engine.py. Este handler ahora solo parsea el
        request HTTP y traduce la respuesta — ver order_engine.py para el
        detalle de qué hace cada operación.
        """
        try:
            body = self._read_body()
            ticker = body.get("ticker", "").upper()
            precio_raw    = float(body.get("precio", 0))
            total_usd_raw = float(body.get("total_usd", 0))
            cantidad = int(body.get("cantidad", 0))
            nombre = body.get("nombre", ticker)
            op_type = "compra" if self.path == "/api/compra" else "venta"

            from src.execution import order_engine
            if op_type == "compra":
                result = order_engine.execute_compra(
                    ticker=ticker,
                    precio_raw=precio_raw,
                    total_usd_raw=total_usd_raw,
                    cantidad=cantidad,
                    nombre=nombre,
                    mercado_form=body.get("mercado", ""),
                    precio_fuente_form=body.get("precio_fuente", ""),
                    ratio_cedear_form=float(body.get("ratio_cedear", 1.0) or 1.0),
                )
            else:
                result = order_engine.execute_venta(
                    ticker=ticker, precio_raw=precio_raw, cantidad=cantidad,
                )

            http_code = result.pop("http_code", 200 if result.get("status") == "ok" else 500)
            self._send_json(http_code, result)
            print(f"[api] {op_type}: {ticker} {cantidad} @ {precio_raw:.4f} | "
                  f"status={result.get('status')} pushed={result.get('pushed')}", flush=True)
        except Exception as e:
            self._send_json(500, {"error": str(e)})
            print(f"[api] Error: {e}", flush=True)
 

    def _push_portfolio_to_github(self, portfolio_path: str, commit_msg: str, max_retries: int = 3):
        """
        Push portfolio.json a GitHub con reintento automático.
        Devuelve (pushed: bool, error_msg: str | None).
        """
        import base64 as _b64
        try:
            import requests as _req
        except ImportError:
            return False, "requests no disponible en el entorno"

        gh_token = os.environ.get("GH_TOKEN", "")
        if not gh_token:
            return False, "GH_TOKEN no configurado en Railway"

        repo    = "Brunogatti79/inversiones-bursatiles"
        path    = "data/portfolio.json"
        url     = f"https://api.github.com/repos/{repo}/contents/{path}"
        headers = {
            "Authorization": f"token {gh_token}",
            "Accept":        "application/vnd.github.v3+json",
        }

        last_error = None
        for attempt in range(1, max_retries + 1):
            try:
                # 1. Obtener SHA actual
                r_get = _req.get(url, headers=headers, timeout=10)
                if r_get.status_code not in (200, 404):
                    last_error = f"GET SHA status {r_get.status_code}: {r_get.text[:120]}"
                    print(f"[push] intento {attempt}/{max_retries} — {last_error}", flush=True)
                    continue
                sha = r_get.json().get("sha", "") if r_get.status_code == 200 else ""

                # 2. Leer archivo local
                with open(portfolio_path, "r", encoding="utf-8") as f:
                    raw = f.read()
                encoded = _b64.b64encode(raw.encode("utf-8")).decode()

                # 3. PUT
                payload = {"message": commit_msg, "content": encoded}
                if sha:
                    payload["sha"] = sha
                r_put = _req.put(url, headers=headers, json=payload, timeout=15)

                if r_put.status_code in (200, 201):
                    print(f"[push] ✅ portfolio.json pushed (intento {attempt})", flush=True)
                    return True, None
                else:
                    last_error = f"PUT status {r_put.status_code}: {r_put.text[:120]}"
                    print(f"[push] intento {attempt}/{max_retries} — {last_error}", flush=True)

            except Exception as e:
                last_error = str(e)
                print(f"[push] intento {attempt}/{max_retries} — excepción: {last_error}", flush=True)

        # Todos los intentos fallaron
        print(f"[push] ❌ push falló después de {max_retries} intentos: {last_error}", flush=True)
        return False, last_error

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
 
 
def _sync_all_data_from_github():
    """Repuebla data/ con todo lo que necesita sobrevivir entre redeploys de Railway.
    Antes eran 3 funciones casi idénticas — unificado en github_persistence.py."""
    try:
        from src.github_persistence import sync_all_at_startup
        sync_all_at_startup([
            "portfolio.json",
            "macro_score_history.json",
            "macro_raw_history.json",
            "signals_history.json",
            "health_metrics.json",
            "backtest_results.json",
            "opportunities_log.json",
            "opportunities_effectiveness.json",
            "model_performance_history.json",
            "optimized_weights.json",
            "historical_replay.json",     # fix 24/06: antes se perdía en cada redeploy
            "system_confidence.json",     # mejora 4.3: confidence global + kill switch
            "predictor_validation.json",  # Prioridad 3: predictor vs baselines (desde el día 1)
            "kill_switch_history.json",     # Prioridad 2: bitácora append-only del kill switch
            "kill_switch_validation.json",  # Prioridad 2: validación retroactiva vs precios
            "download_status.json",         # gap heredado de v4.0 (§8.1/#8 lista v9) -- se
                                             # commiteaba pero no se restauraba al arrancar;
                                             # impacto cosmético (solo un string de log), se
                                             # cierra de paso por ser de una línea.
            "cedear_cierres.csv",           # FIX 26/06/2026 (sesión 2): snapshot de respaldo
                                             # de precios CEDEAR (data912.com) -- por si la API
                                             # está caída justo al arrancar Railway.
        ])
        print("[start_server] data/ sincronizado desde GitHub", flush=True)
    except Exception as e:
        print(f"[start_server] No se pudo sincronizar data/ desde GitHub: {e}", flush=True)


def launch_main():
    import time
    time.sleep(2)
    subprocess.run([sys.executable, "main.py"])
 
 
t = threading.Thread(target=launch_main, daemon=True)
t.start()
 
_sync_all_data_from_github()
print(f"[start_server] Servidor HTTP en puerto {PORT}", flush=True)
print(f"[start_server] Webhook activo en POST /webhook/run", flush=True)
httpd = HTTPServer(("0.0.0.0", PORT), Handler)
httpd.serve_forever()
 
 
