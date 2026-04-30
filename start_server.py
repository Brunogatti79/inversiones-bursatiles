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

OUTPUT_DIR = "outputs"
os.makedirs(OUTPUT_DIR, exist_ok=True)
PORT = int(os.environ.get("PORT", 8080))


class Handler(SimpleHTTPRequestHandler):
    def __init__(self, *args, **kwargs):
        super().__init__(*args, directory=OUTPUT_DIR, **kwargs)

    def log_message(self, format, *args):
        pass  # silenciar logs HTTP


def launch_main():
    """Lanza main.py en un proceso separado."""
    import time
    time.sleep(2)  # pequeña pausa para que el servidor HTTP arranque
    subprocess.run([sys.executable, "main.py"])


# Lanzar main.py en hilo daemon
t = threading.Thread(target=launch_main, daemon=True)
t.start()

# Servidor HTTP en hilo principal (Railway hace health check aqui)
print(f"[start_server] Servidor HTTP en puerto {PORT}", flush=True)
httpd = HTTPServer(("0.0.0.0", PORT), Handler)
httpd.serve_forever()
