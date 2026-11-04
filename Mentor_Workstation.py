"""Mentor Workstation - Windows desktop app.

A small window (Microsoft Edge WebView2 via pywebview) that shows the bundled index.html.
The page is served from 127.0.0.1 on a FIXED port, so saved data (projects, shortcut, etc.) stays
in the app between launches. Build: see .github/workflows/build-workstation-exe.yml
"""
import os
import sys
import threading
import urllib.request
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from pathlib import Path

APP_NAME = "Mentor Workstation"
HOST = "127.0.0.1"
PORTS = [47831, 47832, 47833, 47834, 47835, 0]   # first free one; the fixed port keeps saved data in place


def resource_path(name):
    base = Path(getattr(sys, "_MEIPASS", Path(__file__).resolve().parent))
    return base / name


def data_dir():
    root = Path(os.environ.get("APPDATA") or Path.home()) / "MentorWorkstation"
    root.mkdir(parents=True, exist_ok=True)
    return root


def page_server():
    page = resource_path("index.html")
    if not page.is_file():
        raise FileNotFoundError(f"Mentor Workstation page was not found: {page}")
    contents = page.read_bytes()

    class PageHandler(BaseHTTPRequestHandler):
        def log_message(self, *args):
            pass

        def _serve(self, include_body):
            path = self.path.split("?", 1)[0]
            if path not in ("/", "/index.html"):
                self.send_error(404)
                return
            self.send_response(200)
            self.send_header("Content-Type", "text/html; charset=utf-8")
            self.send_header("Content-Length", str(len(contents)))
            self.send_header("Cache-Control", "no-store")
            self.end_headers()
            if include_body:
                self.wfile.write(contents)

        def do_GET(self):
            self._serve(True)

        def do_HEAD(self):
            self._serve(False)

    last = None
    for port in PORTS:
        try:
            return ThreadingHTTPServer((HOST, port), PageHandler)
        except OSError as exc:
            last = exc
    raise last


def main():
    server = page_server()
    threading.Thread(target=server.serve_forever, daemon=True).start()
    url = f"http://{HOST}:{server.server_port}/?client=desktop"

    if "--check" in sys.argv:          # used by the build to prove the app starts (no window)
        try:
            with urllib.request.urlopen(url, timeout=5) as r:
                ok = r.status == 200 and b"Mentor Workstation" in r.read()
        except Exception as exc:       # noqa: BLE001
            ok = False
            (data_dir() / "check.txt").write_text(f"FAIL {exc}", encoding="utf-8")
        else:
            (data_dir() / "check.txt").write_text("OK" if ok else "FAIL page", encoding="utf-8")
        server.shutdown()
        return 0 if ok else 1

    import webview
    try:
        webview.settings["ALLOW_DOWNLOADS"] = True       # Excel / CSV exports
    except Exception:                                    # noqa: BLE001
        pass
    webview.create_window(APP_NAME, url, width=1360, height=860, min_size=(1000, 640))
    try:
        webview.start(private_mode=False, storage_path=str(data_dir() / "webview"))
    finally:
        server.shutdown()
        server.server_close()
    return 0


if __name__ == "__main__":
    sys.exit(main())
