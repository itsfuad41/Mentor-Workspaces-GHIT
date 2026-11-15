#!/usr/bin/env python3
"""
Fuad's Server (v3) - desktop app
================================
A modern control panel for Fuad's Project Server.

* The project server itself lives in fuads_server_core.py (same database, same website connection code).
* The control panel is a small web page (server_panel.html) shown in a native window (WebView2 via pywebview).
  It is served on 127.0.0.1 only, so nothing outside this PC can reach it.
* Closing the window keeps the server running (tray icon). "Stop & exit app" really stops it.
* Start with Windows launches this program with --autostart: tray icon only, no window, server starts by itself.

Run:  python Fuads_Server_App.py [--autostart] [--no-window] [--panel-port N]
"""
import json
import os
import secrets
import shutil
import subprocess
import sys
import threading
import time
import urllib.parse
import urllib.request
import http.server

import fuads_server_core as core
from fuads_server_core import log

APP_TITLE = "Fuad's Server"
PANEL_PORTS = (47820, 47821, 47822, 47823)
HERE = os.path.dirname(os.path.abspath(sys.executable if getattr(sys, "frozen", False) else __file__))


def resource(name):
    for base in (getattr(sys, "_MEIPASS", None), HERE, os.getcwd()):
        if base and os.path.isfile(os.path.join(base, name)):
            return os.path.join(base, name)
    return None


def open_in_os(path):
    if core.IS_WINDOWS:
        os.startfile(path)  # noqa: S606 - a folder or log file of this program
    elif sys.platform == "darwin":
        subprocess.Popen(["open", path])
    else:
        subprocess.Popen(["xdg-open", path], stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL)


class Panel:
    """Everything the control panel page can do. One instance per running app."""

    def __init__(self, runtime, port):
        self.rt = runtime
        self.port = port
        self.sessions = {}          # token -> admin username
        self.lock = threading.Lock()
        self.on_show = lambda: None
        self.on_quit = lambda: None
        self.started = time.time()
        self.httpd = None

    # ---- auth ----
    def login(self, username, password):
        ok, message = core.verify_admin_login(username, password)
        if not ok:
            raise ApiError(message, 401)
        token = secrets.token_urlsafe(24)
        with self.lock:
            self.sessions[token] = (username or "").strip().lower()
        return {"token": token, "user": (username or "").strip().lower()}

    def user_of(self, token):
        with self.lock:
            return self.sessions.get(token or "")

    def confirm_password(self, token, password):
        user = self.user_of(token)
        ok, message = core.verify_admin_login(user or "", password or "")
        if not ok:
            raise ApiError("That password was not accepted: " + message, 403)

    # ---- state ----
    def backups(self):
        out = []
        try:
            for name in sorted(os.listdir(core.BACKUP_DIR), reverse=True)[:30]:
                if name.endswith(".db"):
                    path = os.path.join(core.BACKUP_DIR, name)
                    out.append({"name": name, "size": os.path.getsize(path), "time": time.strftime("%Y-%m-%d %H:%M", time.localtime(os.path.getmtime(path)))})
        except OSError:
            pass
        return out

    def state(self):
        s = self.rt.status()
        projects = days = 0
        try:
            conn = core._db()
            try:
                projects = conn.execute("SELECT COUNT(*) FROM projects").fetchone()[0]
                days = conn.execute("SELECT COUNT(DISTINCT day) FROM daily_snapshots").fetchone()[0]
            finally:
                conn.close()
        except Exception:  # noqa: BLE001
            pass
        s["counts"] = {"projects": projects, "days": days, "clients": len(core.ACTIVITY.get_data(0)["clients"])}
        s["startup"] = core.startup_status()
        s["backups"] = self.backups()
        s["uptime_app"] = int(time.time() - self.started)
        return s

    # ---- actions (each returns a JSON-able dict, or raises ApiError) ----
    def server_start(self, body):
        port = body.get("port")
        if body.get("restart") and self.rt.server.running:
            self.rt.stop_server()
        ok, error = self.rt.start_server(port)
        if not ok:
            raise ApiError("The server could not start: " + str(error) + (". Another program may be using that port." if "in use" in str(error).lower() or "10048" in str(error) else ""))
        return {"ok": True}

    def server_stop(self, token, body):
        self.confirm_password(token, body.get("password"))
        self.rt.stop_server()
        return {"ok": True}

    def temp(self, start):
        if start:
            self.rt.start_temp()
        else:
            self.rt.temp.stop()
        return {"ok": True}

    def named_save(self, body):
        cfg = core.load_config()
        token, url = body.get("token"), str(body.get("url") or "").strip().rstrip("/")
        if token is not None:
            token = str(token).strip()
            if token.lower().startswith("cloudflared"):
                token = token.split()[-1]
        else:
            token = cfg.get("cloudflare_token", "")
        if url and "://" not in url:
            url = "https://" + url
        if token and not url:
            raise ApiError("Enter your public address too, so the website knows where to connect.")
        if url:
            parsed = urllib.parse.urlparse(url)
            if parsed.scheme != "https" or not parsed.netloc:
                raise ApiError("The public address must be a valid https address, like https://mentor.yourdomain.com")
        cfg["cloudflare_token"], cfg["cloudflare_url"] = token, url
        if not core.save_config(cfg):
            raise ApiError("Could not save the settings to " + core.CONFIG_PATH)
        if self.rt.named.running:
            self.rt.named.stop()
        return {"ok": True}

    def settings(self, body):
        cfg = core.load_config()
        for key in ("temp_link_auto", "cloudflare_auto", "auto_tunnel"):
            if key in body:
                cfg[key] = bool(body[key])
        if "custom_url" in body:
            url = str(body["custom_url"] or "").strip().rstrip("/")
            if url:
                parsed = urllib.parse.urlparse(url)
                if parsed.scheme != "https" or not parsed.netloc:
                    raise ApiError("Enter a valid https address, like https://your-device.your-tailnet.ts.net")
            cfg["custom_url"] = url
        if "snapshot_keep_days" in body:
            days = int(body["snapshot_keep_days"] or 0)
            if days < 0:
                raise ApiError("Keep-days cannot be negative.")
            cfg["snapshot_keep_days"] = days
        if not core.save_config(cfg):
            raise ApiError("Could not save the settings to " + core.CONFIG_PATH)
        return {"ok": True}

    def startup(self, body):
        try:
            note = core.set_background_startup(bool(body.get("enabled")))
        except (OSError, RuntimeError) as error:
            raise ApiError(f"Could not change Windows startup: {error}") from error
        return {"ok": True, "note": note}

    def open_path(self, what):
        target = {"log": core.BACKGROUND_LOG_PATH, "data": core.DATA_DIR, "backups": core.BACKUP_DIR}.get(what)
        if not target:
            raise ApiError("Unknown item.")
        if what == "backups":
            os.makedirs(target, exist_ok=True)
        if not os.path.exists(target):
            raise ApiError("Nothing there yet.")
        open_in_os(target)
        return {"ok": True}

    def peers_add(self, body):
        try:
            d = core.parse_code(body.get("code") or "")
            if not d.get("u"):
                raise ValueError("no address in that code")
        except Exception as error:  # noqa: BLE001
            raise ApiError(f"That is not a valid connection code ({error}).") from error
        cfg = core.load_config()
        peers = cfg.setdefault("peers", [])
        if any(p["url"].rstrip("/") == d["u"].rstrip("/") for p in peers):
            raise ApiError("That server is already in the list.")
        peers.append({"url": d["u"], "admin_key": d.get("a", ""), "view_key": d.get("v", "")})
        core.save_config(cfg)
        threading.Thread(target=core.REPLICATOR.tick, daemon=True).start()
        return {"ok": True}

    def peers_remove(self, body):
        cfg = core.load_config()
        cfg["peers"] = [p for p in cfg.get("peers", []) if p["url"].rstrip("/") != str(body.get("url", "")).rstrip("/")]
        core.save_config(cfg)
        return {"ok": True}

    def peers_backup(self, token, body):
        self.confirm_password(token, body.get("password"))
        try:
            d = core.parse_code(body.get("code") or "")
            if not d.get("u"):
                raise ValueError("no address in that code")
        except Exception as error:  # noqa: BLE001
            raise ApiError(f"That is not a valid connection code ({error}).") from error
        cfg = core.load_config()
        cfg["admin_key"], cfg["view_key"] = d.get("a", cfg["admin_key"]), d.get("v", cfg["view_key"])
        peers = cfg.setdefault("peers", [])
        if not any(p["url"].rstrip("/") == d["u"].rstrip("/") for p in peers):
            peers.append({"url": d["u"], "admin_key": d.get("a", ""), "view_key": d.get("v", "")})
        core.save_config(cfg)
        core.step_down_to_replica(d["u"], d.get("t", 1))
        threading.Thread(target=core.REPLICATOR.tick, daemon=True).start()
        return {"ok": True}

    def peers_promote(self, token, body):
        self.confirm_password(token, body.get("password"))
        core.promote_to_primary()
        return {"ok": True}

    def keys_new(self, token, body):
        self.confirm_password(token, body.get("password"))
        cfg = core.load_config()
        cfg["admin_key"], cfg["view_key"] = secrets.token_urlsafe(12), secrets.token_urlsafe(12)
        if not core.save_config(cfg):
            raise ApiError("Could not save the new keys to " + core.CONFIG_PATH)
        return {"ok": True}


class ApiError(Exception):
    def __init__(self, message, status=400):
        super().__init__(message)
        self.status = status


def make_handler(panel):
    page_path = resource("server_panel.html")
    allowed_hosts = {f"127.0.0.1:{panel.port}", f"localhost:{panel.port}"}

    class H(http.server.BaseHTTPRequestHandler):
        server_version = "FuadsPanel/3.0"

        def log_message(self, *a):
            pass

        def _send(self, code, obj=None, raw=None, ctype="application/json; charset=utf-8"):
            body = raw if raw is not None else json.dumps(obj).encode("utf-8")
            self.send_response(code)
            self.send_header("Content-Type", ctype)
            self.send_header("Content-Length", str(len(body)))
            self.send_header("Cache-Control", "no-store")
            self.send_header("X-Content-Type-Options", "nosniff")
            self.send_header("Content-Security-Policy", "default-src 'self' 'unsafe-inline'; connect-src 'self'; img-src 'self' data:")
            self.end_headers()
            self.wfile.write(body)

        def _json(self):
            n = int(self.headers.get("Content-Length") or 0)
            if n > 1_000_000:
                raise ApiError("Request too large.", 413)
            try:
                return json.loads(self.rfile.read(n) or b"{}")
            except ValueError as error:
                raise ApiError("Invalid request.") from error

        def _route(self, method):
            try:
                # only this PC, only our own page: blocks other websites (DNS rebinding / cross-site requests)
                if (self.headers.get("Host") or "").lower() not in allowed_hosts:
                    return self._send(403, {"error": "Not allowed."})
                origin = self.headers.get("Origin")
                if origin and origin.lower().replace("https://", "http://") not in {"http://" + h for h in allowed_hosts}:
                    return self._send(403, {"error": "Not allowed."})
                path = urllib.parse.urlparse(self.path).path
                query = urllib.parse.parse_qs(urllib.parse.urlparse(self.path).query)
                if method == "GET" and path in ("/", "/index.html"):
                    if not page_path:
                        return self._send(500, {"error": "server_panel.html is missing next to the program."})
                    with open(page_path, "rb") as fh:
                        return self._send(200, raw=fh.read(), ctype="text/html; charset=utf-8")
                if path == "/api/ping":
                    return self._send(200, {"app": APP_TITLE})
                if path == "/api/show" and method == "POST":
                    panel.on_show()
                    return self._send(200, {"ok": True})
                if path == "/api/login" and method == "POST":
                    b = self._json()
                    return self._send(200, panel.login(b.get("username"), b.get("password")))
                token = self.headers.get("X-Panel") or ""
                if not panel.user_of(token):
                    return self._send(401, {"error": "Please sign in."})
                if path == "/api/logout":
                    panel.sessions.pop(token, None)
                    return self._send(200, {"ok": True})
                if method == "GET":
                    if path == "/api/state":
                        return self._send(200, panel.state())
                    if path == "/api/snapshots":
                        return self._send(200, {"days": core.snapshot_days()})
                    if path == "/api/activity":
                        since = float((query.get("since") or ["0"])[0])
                        return self._send(200, core.ACTIVITY.get_data(since))
                    if path == "/api/log":
                        return self._send(200, {"lines": list(core._log)[-200:]})
                    return self._send(404, {"error": "Not found."})
                b = self._json()
                routes = {
                    "/api/server/start": lambda: panel.server_start(b),
                    "/api/server/stop": lambda: panel.server_stop(token, b),
                    "/api/temp/start": lambda: panel.temp(True),
                    "/api/temp/stop": lambda: panel.temp(False),
                    "/api/named/save": lambda: panel.named_save(b),
                    "/api/named/start": lambda: (panel.rt.start_named(), {"ok": True})[1],
                    "/api/named/stop": lambda: (panel.rt.named.stop(), {"ok": True})[1],
                    "/api/funnel/on": lambda: {"ok": True, "url": panel.rt.funnel_on()},
                    "/api/funnel/off": lambda: (panel.rt.funnel_off(), {"ok": True})[1],
                    "/api/settings": lambda: panel.settings(b),
                    "/api/startup": lambda: panel.startup(b),
                    "/api/open": lambda: panel.open_path(b.get("what")),
                    "/api/snapshots/now": lambda: {"ok": True, "projects": core.snapshot_all(force=True), "day": core._today()},
                    "/api/backups/now": lambda: (core.backup_db(), {"ok": True})[1],
                    "/api/peers/add": lambda: panel.peers_add(b),
                    "/api/peers/remove": lambda: panel.peers_remove(b),
                    "/api/peers/backup": lambda: panel.peers_backup(token, b),
                    "/api/peers/promote": lambda: panel.peers_promote(token, b),
                    "/api/keys/new": lambda: panel.keys_new(token, b),
                }
                if path == "/api/quit":
                    panel.confirm_password(token, b.get("password"))
                    self._send(200, {"ok": True})
                    threading.Thread(target=panel.on_quit, daemon=True).start()
                    return
                fn = routes.get(path)
                if not fn or method != "POST":
                    return self._send(404, {"error": "Not found."})
                return self._send(200, fn())
            except ApiError as error:
                self._send(error.status, {"error": str(error)})
            except (RuntimeError, OSError, subprocess.TimeoutExpired) as error:
                self._send(400, {"error": str(error)})
            except Exception as error:  # noqa: BLE001
                log(f"Panel error on {self.path}: {error}")
                try:
                    self._send(500, {"error": "Something went wrong: " + str(error)})
                except OSError:
                    pass

        def do_GET(self):
            self._route("GET")

        def do_POST(self):
            self._route("POST")

    return H


def bind_panel(panel_port_hint=None):
    """Try the panel ports in turn. Returns (httpd, port) or (None, port_in_use_by_other_copy)."""
    ports = (panel_port_hint,) if panel_port_hint else PANEL_PORTS
    for port in ports:
        panel = Panel(None, port)
        try:
            httpd = http.server.ThreadingHTTPServer(("127.0.0.1", port), make_handler(panel))
            httpd.daemon_threads = True
            return httpd, port
        except OSError:
            continue
    return None, ports[0]


def another_copy_running(port_hint=None):
    """If another copy of this app already owns a panel port, ask it to show its window and report True."""
    for port in ((port_hint,) if port_hint else PANEL_PORTS):
        try:
            with urllib.request.urlopen(f"http://127.0.0.1:{port}/api/ping", timeout=1.5) as r:
                if json.loads(r.read().decode()).get("app") == APP_TITLE:
                    try:
                        urllib.request.urlopen(urllib.request.Request(f"http://127.0.0.1:{port}/api/show", data=b"{}", method="POST"), timeout=2)
                    except OSError:
                        pass
                    return True
        except (OSError, ValueError):
            continue
    return False


def main(argv=None):
    argv = list(sys.argv[1:] if argv is None else argv)
    autostart = "--autostart" in argv
    no_window = "--no-window" in argv or autostart
    port_hint = None
    if "--panel-port" in argv:
        port_hint = int(argv[argv.index("--panel-port") + 1])

    if another_copy_running(port_hint):
        return 0  # already running: it has been asked to show its window

    if "--install-startup" in argv:
        print(core.set_background_startup(True) or "ok")
        return 0
    if "--remove-startup" in argv:
        print(core.set_background_startup(False) or "ok")
        return 0

    if core.IS_WINDOWS:
        try:
            import ctypes
            ctypes.windll.shell32.SetCurrentProcessExplicitAppUserModelID("Fuad.ProjectServer.3")
        except (AttributeError, OSError):
            pass

    log(f"{APP_TITLE} starting ({'start with Windows' if autostart else 'manual start'}).")
    rt = core.Runtime()
    try:
        ok, error = rt.boot()
    except Exception as err:  # noqa: BLE001
        log(f"Could not start: {err}")
        raise
    if not ok:
        log(f"Server did not start (the app stays open so you can see why): {error}")
    if core.IS_WINDOWS:
        threading.Thread(target=_startup_housekeeping, daemon=True).start()

    httpd, port = bind_panel(port_hint)
    if httpd is None:
        log("No free port for the control panel.")
        return 1
    panel = Panel(rt, port)
    httpd.RequestHandlerClass = make_handler(panel)
    threading.Thread(target=httpd.serve_forever, daemon=True).start()
    log(f"Control panel on http://127.0.0.1:{port}/")

    url = f"http://127.0.0.1:{port}/"
    state = {"window": None, "tray": None, "quit": threading.Event(), "webview": None}

    def show():
        w = state["window"]
        if w is not None:
            try:
                w.show()
                w.restore()
                return
            except Exception:  # noqa: BLE001
                pass
        if state["webview"] is None:
            import webbrowser
            webbrowser.open(url)

    def quit_all():
        log("Exit requested.")
        try:
            rt.shutdown()
        finally:
            try:
                if state["tray"]:
                    state["tray"].stop()
            except Exception:  # noqa: BLE001
                pass
            try:
                if state["window"] is not None:
                    state["window"].destroy()
            except Exception:  # noqa: BLE001
                pass
            state["quit"].set()
            httpd.shutdown()

    panel.on_show, panel.on_quit = show, quit_all

    # tray icon (optional: the app still works without it)
    try:
        import pystray
        from PIL import Image, ImageDraw

        def tray_image(running):
            img = Image.new("RGBA", (64, 64), (0, 0, 0, 0))
            d = ImageDraw.Draw(img)
            d.rounded_rectangle((4, 4, 60, 60), 14, fill=(20, 40, 58, 255))
            d.ellipse((36, 36, 56, 56), fill=(53, 208, 138, 255) if running else (192, 57, 43, 255))
            d.text((16, 12), "F", fill=(255, 255, 255, 255))
            return img

        icon = pystray.Icon("fuads_server", tray_image(rt.server.running), APP_TITLE,
                            menu=pystray.Menu(pystray.MenuItem("Open control panel", lambda *_: show(), default=True),
                                              pystray.MenuItem("Copy connection code", lambda *_: _copy_code(rt)),
                                              pystray.MenuItem("Open panel in browser", lambda *_: __import__("webbrowser").open(url))))
        state["tray"] = icon
        icon.run_detached()

        def tray_updater():
            last = None
            while not state["quit"].is_set():
                running = rt.server.running
                if running != last:
                    last = running
                    try:
                        icon.icon = tray_image(running)
                        icon.title = f"{APP_TITLE} - {'running' if running else 'stopped'}"
                    except Exception:  # noqa: BLE001
                        pass
                time.sleep(2)
        threading.Thread(target=tray_updater, daemon=True).start()
    except Exception as err:  # noqa: BLE001
        log(f"Tray icon not available ({err}); the app still works.")

    if no_window:
        if state["tray"] is None and not autostart:
            import webbrowser
            webbrowser.open(url)
        state["quit"].wait()
        return 0

    try:
        import webview
        state["webview"] = webview
        icon_file = resource("fuads_project_server.ico")
        win = webview.create_window(APP_TITLE, url, width=1220, height=800, min_size=(900, 620), background_color="#14283a")
        state["window"] = win
        # closing the window hides it (the server keeps running in the tray); the tray / panel "Stop & exit" really exits
        if state["tray"] is not None:
            def on_closing():
                try:
                    win.hide()
                except Exception:  # noqa: BLE001
                    pass
                return False
            win.events.closing += on_closing
        webview.start(icon=icon_file if core.IS_WINDOWS and icon_file else None, private_mode=False,
                      storage_path=os.path.join(core.DATA_DIR, "webview"))
        if state["tray"] is None:
            quit_all()
    except ImportError:
        import webbrowser
        log("pywebview is not installed: opening the control panel in your browser instead.")
        webbrowser.open(url)
        state["quit"].wait()
    return 0


def _startup_housekeeping():
    """First run on a PC: turn 'Start with Windows' on by itself (it stays switchable in the panel).
    Every run: repair the entry if it points at an old or moved copy of this program."""
    try:
        cfg = core.load_config()
        if not cfg.get("startup_default_applied"):
            cfg["startup_default_applied"] = True
            core.save_config(cfg)
            if not core.startup_status()["enabled"]:
                note = core.set_background_startup(True)
                log("Start with Windows switched on for the first run." + (" " + note if note else ""))
        else:
            core.heal_startup_entry()
    except Exception as err:  # noqa: BLE001
        log(f"Start-with-Windows setup problem: {err}")


def _copy_code(rt):
    try:
        code = rt.connection_code()
        if core.IS_WINDOWS:
            subprocess.run(["clip"], input=code.encode("utf-16le"), creationflags=0x08000000, timeout=5)
    except Exception as err:  # noqa: BLE001
        log(f"Could not copy the code: {err}")


if __name__ == "__main__":
    raise SystemExit(main() or 0)
