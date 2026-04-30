"""
server.py
Servidor HTTP simple que sirve los dashboards HTML generados.
Corre en paralelo al bot y al scheduler.
"""
import os
import threading
from http.server import HTTPServer, SimpleHTTPRequestHandler
import logging

logger = logging.getLogger(__name__)

PORT = int(os.getenv("PORT", 8080))
OUTPUT_DIR = "outputs"


class DashboardHandler(SimpleHTTPRequestHandler):
    def __init__(self, *args, **kwargs):
        super().__init__(*args, directory=OUTPUT_DIR, **kwargs)

    def log_message(self, format, *args):
        pass  # silenciar logs HTTP


def start_server():
    os.makedirs(OUTPUT_DIR, exist_ok=True)
    httpd = HTTPServer(("0.0.0.0", PORT), DashboardHandler)
    logger.info(f"Servidor HTTP activo en puerto {PORT} → sirviendo carpeta '{OUTPUT_DIR}/'")
    httpd.serve_forever()


def start_server_thread():
    t = threading.Thread(target=start_server, daemon=True)
    t.start()
    return t
