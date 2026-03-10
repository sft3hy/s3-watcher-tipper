import os
import sys
import json
import threading
import webbrowser
from pathlib import Path
from http.server import HTTPServer
from .fusion import load_and_fuse
from .database import load_from_clickhouse
from .server import Handler, DATA_CACHE


def main():
    host = os.environ.get("CLICKHOUSE_HOST")
    if host:
        print("[+] ClickHouse backend detected. Pulling live data...")
        fused = load_from_clickhouse(host)
    else:
        if len(sys.argv) < 2:
            print(
                "Usage: python -m analyzer.main <path_to_csv_or_dir>   (OR set CLICKHOUSE_HOST)"
            )
            sys.exit(1)

        csv_path = sys.argv[1]
        if not Path(csv_path).exists():
            print(f"Error: path not found — {csv_path}")
            sys.exit(1)

        print("[+] Fusing data from local CSV...")
        fused = load_and_fuse(csv_path)

    DATA_CACHE.update(fused)
    print(f"[+] Summary: {json.dumps(fused['summary'], indent=2)}")

    port = 8000
    server = HTTPServer(("0.0.0.0", port), Handler)
    print(f"\n[+] Dashboard API and Client running on http://0.0.0.0:{port}")
    print("[+] Press Ctrl+C to stop\n")

    if not host:
        threading.Timer(
            1.0, lambda: webbrowser.open(f"http://localhost:{port}")
        ).start()

    try:
        server.serve_forever()
    except KeyboardInterrupt:
        print("\n[+] Server stopped.")


if __name__ == "__main__":
    main()
