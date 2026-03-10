import json
import os
from http.server import BaseHTTPRequestHandler
from urllib.parse import urlparse, parse_qs
from .dashboard import build_html

DATA_CACHE = {}

# API key auth — if API_KEY env var is set, require it in X-API-Key header
_API_KEY = os.environ.get("API_KEY", "")


class Handler(BaseHTTPRequestHandler):
    def log_message(self, *_):
        pass

    def _json_response(self, data, status=200):
        body = json.dumps(data, default=str).encode()
        self.send_response(status)
        self.send_header("Content-Type", "application/json")
        self.send_header("Access-Control-Allow-Origin", "*")
        self.send_header("Access-Control-Allow-Headers", "X-API-Key, Content-Type")
        self.end_headers()
        self.wfile.write(body)

    def _check_api_key(self):
        """Return True if auth passes, False if rejected."""
        if not _API_KEY:
            return True
        key = self.headers.get("X-API-Key", "")
        if key == _API_KEY:
            return True
        self._json_response(
            {"error": "Unauthorized. Provide valid X-API-Key header."}, 401
        )
        return False

    def do_OPTIONS(self):
        """Handle CORS preflight."""
        self.send_response(200)
        self.send_header("Access-Control-Allow-Origin", "*")
        self.send_header("Access-Control-Allow-Methods", "GET, POST, OPTIONS")
        self.send_header("Access-Control-Allow-Headers", "X-API-Key, Content-Type")
        self.end_headers()

    def do_GET(self):
        parsed = urlparse(self.path)
        path = parsed.path.rstrip("/")

        # ── Dashboard routes ──────────────────────────────────────────
        if path == "/s3-watcher/data":
            body = json.dumps(DATA_CACHE, default=str).encode()
            self.send_response(200)
            self.send_header("Content-Type", "application/json")
            self.send_header("Access-Control-Allow-Origin", "*")
            self.end_headers()
            self.wfile.write(body)
            return

        if path in ("/s3-watcher", "/s3-watcher/index.html"):
            html = build_html()
            self.send_response(200)
            self.send_header("Content-Type", "text/html")
            self.end_headers()
            self.wfile.write(html.encode())
            return

        # ── API v1 routes ─────────────────────────────────────────────
        if path.startswith("/s3-watcher/api/v1"):
            if not self._check_api_key():
                return
            self._handle_api_v1(path, parse_qs(parsed.query))
            return

        self.send_response(404)
        self.end_headers()

    def do_POST(self):
        parsed = urlparse(self.path)
        path = parsed.path.rstrip("/")

        if path == "/s3-watcher/api/v1/s3/access":
            if not self._check_api_key():
                return
            self._handle_s3_access()
            return

        self.send_response(404)
        self.end_headers()

    def _handle_api_v1(self, path, params):
        """Route API v1 GET requests."""
        route = path.replace("/s3-watcher/api/v1", "")

        # Direct cache key mappings
        key_map = {
            "/summary": "summary",
            "/geo": "geo",
            "/timeline": "timeline",
            "/anomalies": "anomalies",
            "/units": "units",
            "/devices": "devices",
            "/movement": "movement",
            "/heatmap": "heatmap",
            "/associations": "associations",
            "/network": "network",
            "/behavioral": None,  # composite
            "/behavioral/colocation": "colocation",
            "/behavioral/cotravel": "cotravel",
            "/behavioral/conetwork": "conetwork",
            "/geofence": None,  # composite
            "/geofence/zones": None,
            "/geofence/dwell": "dwell",
            "/skytrace": "skytrace",
            "/geocode": "geocode",
            "/translation": "translation",
            "/domains/maritime": "maritime",
            "/domains/aviation": "aviation",
            "/domains/cyber": "cyber",
            "/domains/rf": "rf",
            "/domains/osint": "osint",
        }

        # Entities endpoint — extract unique entity list from geo data
        if route == "/entities":
            geo = DATA_CACHE.get("geo", [])
            entities = {}
            for pt in geo:
                eid = pt.get("entity_id")
                if eid and eid not in entities:
                    entities[eid] = {
                        "entity_id": eid,
                        "unit_name": pt.get("unit_name"),
                        "last_lat": pt.get("latitude"),
                        "last_lon": pt.get("longitude"),
                    }
            self._json_response(list(entities.values())[:500])
            return

        # Behavioral composite
        if route == "/behavioral":
            self._json_response(
                {
                    "colocation": DATA_CACHE.get("colocation", []),
                    "cotravel": DATA_CACHE.get("cotravel", []),
                    "conetwork": DATA_CACHE.get("conetwork", []),
                }
            )
            return

        # Geofence composite
        if route == "/geofence":
            self._json_response(
                {
                    "geofence": DATA_CACHE.get("geofence", {"zones": [], "events": []}),
                    "dwell": DATA_CACHE.get("dwell", []),
                }
            )
            return

        if route == "/geofence/zones":
            gf = DATA_CACHE.get("geofence", {"zones": [], "events": []})
            self._json_response(gf)
            return

        # API docs endpoint
        if route in ("", "/"):
            self._json_response(
                {
                    "service": "SIGINT Intelligence Dashboard API",
                    "version": "1.0",
                    "endpoints": sorted(key_map.keys())
                    + ["/entities", "/s3/access (POST)"],
                    "auth": (
                        "X-API-Key header"
                        if _API_KEY
                        else "open (no API_KEY configured)"
                    ),
                }
            )
            return

        # Direct key lookup
        if route in key_map and key_map[route] is not None:
            self._json_response(DATA_CACHE.get(key_map[route], {}))
            return

        self._json_response({"error": f"Unknown API route: {route}"}, 404)

    def _handle_s3_access(self):
        """Secure proxy to read S3 objects."""
        import boto3

        content_length = int(self.headers.get("Content-Length", 0))
        body = self.rfile.read(content_length) if content_length else b"{}"
        try:
            req = json.loads(body)
        except Exception:
            self._json_response({"error": "Invalid JSON body"}, 400)
            return

        bucket = req.get("bucket", "")
        key = req.get("key", "")
        if not bucket or not key:
            self._json_response(
                {"error": "Must provide 'bucket' and 'key' in body"}, 400
            )
            return

        try:
            aws_key = os.environ.get("AWS_ACCESS_KEY_ID", "").strip()
            aws_secret = os.environ.get("AWS_SECRET_ACCESS_KEY", "").strip()
            region = os.environ.get("AWS_REGION", "us-east-1")

            s3 = boto3.client(
                "s3",
                aws_access_key_id=aws_key,
                aws_secret_access_key=aws_secret,
                region_name=region,
            )
            response = s3.get_object(Bucket=bucket, Key=key)
            obj_bytes = response["Body"].read()

            # Return metadata + base64 content for binary, or text
            content_type = response.get("ContentType", "application/octet-stream")
            if (
                "text" in content_type
                or "json" in content_type
                or "csv" in content_type
            ):
                self._json_response(
                    {
                        "bucket": bucket,
                        "key": key,
                        "content_type": content_type,
                        "size_bytes": len(obj_bytes),
                        "content": obj_bytes.decode("utf-8", errors="replace"),
                    }
                )
            else:
                import base64

                self._json_response(
                    {
                        "bucket": bucket,
                        "key": key,
                        "content_type": content_type,
                        "size_bytes": len(obj_bytes),
                        "content_base64": base64.b64encode(obj_bytes).decode("ascii"),
                    }
                )
        except Exception as e:
            self._json_response({"error": f"S3 access error: {str(e)}"}, 500)
