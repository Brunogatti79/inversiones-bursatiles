"""
start_server.py
Punto de entrada para Railway.
Arranca el servidor HTTP PRIMERO (Railway necesita respuesta en PORT),
luego lanza el bot y scheduler en background.
"""
import os
import sys
import subprocess
import threading
from http.server import HTTPServer, SimpleHTTPRequestHandler

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


class Handler(SimpleHTTPRequestHandler):
    def __init__(self, *args, **kwargs):
        super().__init__(*args, directory=OUTPUT_DIR, **kwargs)

    def log_message(self, format, *args):
        pass


def launch_main():
    import time
    time.sleep(2)
    subprocess.run([sys.executable, "main.py"])


t = threading.Thread(target=launch_main, daemon=True)
t.start()

print(f"[start_server] Servidor HTTP en puerto {PORT}", flush=True)
httpd = HTTPServer(("0.0.0.0", PORT), Handler)
httpd.serve_forever()
