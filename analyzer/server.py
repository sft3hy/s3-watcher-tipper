import json
from http.server import BaseHTTPRequestHandler
from urllib.parse import urlparse
from .dashboard import build_html

DATA_CACHE = {}


class Handler(BaseHTTPRequestHandler):
    def log_message(self, *_):
        pass

    def do_GET(self):
        parsed = urlparse(self.path)
        if parsed.path == "/s3-watcher/data":
            body = json.dumps(DATA_CACHE).encode()
            self.send_response(200)
            self.send_header("Content-Type", "application/json")
            self.send_header("Access-Control-Allow-Origin", "*")
            self.end_headers()
            self.wfile.write(body)
        elif parsed.path in ("/s3-watcher", "/s3-watcher/", "/s3-watcher/index.html"):
            html = build_html()
            self.send_response(200)
            self.send_header("Content-Type", "text/html")
            self.end_headers()
            self.wfile.write(html.encode())
        else:
            self.send_response(404)
            self.end_headers()
