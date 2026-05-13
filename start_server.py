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
from http.server import HTTPServer, BaseHTTPRequestHandler, SimpleHTTPRequestHandler
 
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
    """Sirve archivos estáticos de OUTPUT_DIR + maneja /webhook/run."""
 
    def __init__(self, *args, **kwargs):
        super().__init__(*args, directory=OUTPUT_DIR, **kwargs)
 
    def do_POST(self):
        if self.path == "/webhook/run":
            self._handle_webhook()
        else:
            self.send_error(404)
 
    def do_GET(self):
        if self.path == "/webhook/status":
            self._handle_status()
        else:
            super().do_GET()
 
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
 
