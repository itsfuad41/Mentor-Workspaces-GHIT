"""Self-update for Fuad's Server: checks the project's GitHub releases, downloads the new Fuads_Server.exe and swaps it in.

The swap is done by a tiny script that waits for this program to exit, replaces the exe and starts it again with the
same arguments, so the user never has to open the website and download anything."""
import hashlib
import json
import os
import subprocess
import sys
import threading
import time
import urllib.request

REPO = "itsfuad41/Mentor-Workspaces-GHIT"
API = os.environ.get("FUAD_UPDATE_API") or f"https://api.github.com/repos/{REPO}/releases?per_page=30"
TAG_PREFIX = "server-v"
ASSET = "Fuads_Server.exe"
SUMS = "SHA256SUMS.txt"
CHECK_EVERY = 6 * 3600
UA = {"User-Agent": "FuadsServer-Updater", "Accept": "application/vnd.github+json"}


def parse_version(text):
    out = []
    for piece in str(text).strip().lstrip("vV").split("."):
        digits = "".join(ch for ch in piece if ch.isdigit())
        out.append(int(digits) if digits else 0)
    return tuple(out + [0] * (3 - len(out)))


def exe_path():
    return os.environ.get("FUAD_TEST_EXE") or (sys.executable if getattr(sys, "frozen", False) else "")


def latest_release():
    """Newest published server release that carries the exe, or None."""
    req = urllib.request.Request(API, headers=UA)
    with urllib.request.urlopen(req, timeout=15) as resp:
        releases = json.loads(resp.read().decode("utf-8"))
    best = None
    for rel in releases:
        tag = str(rel.get("tag_name", ""))
        if rel.get("draft") or rel.get("prerelease") or not tag.startswith(TAG_PREFIX):
            continue
        asset = next((a for a in rel.get("assets", []) if a.get("name") == ASSET), None)
        if not asset:
            continue
        version = tag[len(TAG_PREFIX):]
        if best is None or parse_version(version) > parse_version(best["version"]):
            sums = next((a for a in rel.get("assets", []) if a.get("name") == SUMS), None)
            best = {"version": version, "tag": tag, "url": asset["browser_download_url"], "size": int(asset.get("size") or 0),
                    "sums_url": sums["browser_download_url"] if sums else "",
                    "page": rel.get("html_url", ""), "published": str(rel.get("published_at", ""))[:10]}
    return best


class Updater:
    def __init__(self, current_version, get_cfg, save_cfg, last_write_ts, quit_app, log=print):
        self.current = current_version
        self.get_cfg, self.save_cfg = get_cfg, save_cfg
        self.last_write_ts = last_write_ts      # callable -> epoch seconds of the last client write (0 if none)
        self.quit_app = quit_app
        self.log = log
        self.latest = None
        self.error = ""
        self.checked_at = 0.0
        self.busy = ""                           # "", "checking", "downloading", "installing"
        self.progress = 0.0
        self.lock = threading.Lock()
        self.supported = bool(exe_path())

    # ---- status for the panel ----
    def status(self):
        cfg = self.get_cfg()
        has_new = bool(self.latest and parse_version(self.latest["version"]) > parse_version(self.current))
        return {"current": self.current, "supported": self.supported, "auto": bool(cfg.get("auto_update", True)),
                "available": has_new, "latest": self.latest, "error": self.error, "busy": self.busy,
                "progress": round(self.progress, 2), "checked_at": self.checked_at}

    def check(self):
        with self.lock:
            if self.busy:
                return self.status()
            self.busy = "checking"
        try:
            self.latest = latest_release()
            self.error = ""
            self.checked_at = time.time()
        except Exception as exc:  # noqa: BLE001
            self.error = "Could not check for updates: " + str(exc)[:140]
        finally:
            self.busy = ""
        return self.status()

    # ---- download + swap ----
    def install(self):
        """Download the newest exe next to the running one, start the swap script, then close the app."""
        if not self.supported:
            self.error = "Updates work only in the installed Fuads_Server.exe (not when run from source)."
            return self.status()
        with self.lock:
            if self.busy in ("downloading", "installing"):
                return self.status()
            self.busy = "downloading"
        try:
            if not self.latest or parse_version(self.latest["version"]) <= parse_version(self.current):
                self.check()
                if not self.status()["available"]:
                    return self.status()
                self.busy = "downloading"
            rel = self.latest
            exe = exe_path()
            new = os.path.join(os.path.dirname(exe), "Fuads_Server.update.exe")
            self.progress = 0.0
            req = urllib.request.Request(rel["url"], headers={"User-Agent": UA["User-Agent"]})
            with urllib.request.urlopen(req, timeout=60) as resp, open(new, "wb") as out:
                total = int(resp.headers.get("Content-Length") or rel["size"] or 0)
                done = 0
                while True:
                    chunk = resp.read(256 * 1024)
                    if not chunk:
                        break
                    out.write(chunk)
                    done += len(chunk)
                    self.progress = done / total if total else 0.0
            size = os.path.getsize(new)
            with open(new, "rb") as fh:
                head = fh.read(2)
            if (rel["size"] and size != rel["size"]) or (os.name == "nt" and head != b"MZ"):
                os.remove(new)
                raise RuntimeError("The downloaded file is incomplete. Nothing was changed.")
            if rel.get("sums_url"):                  # the release publishes a SHA-256 for the exe: it must match
                with urllib.request.urlopen(urllib.request.Request(rel["sums_url"], headers={"User-Agent": UA["User-Agent"]}), timeout=30) as r:
                    listed = {ln.split()[-1].lstrip("*"): ln.split()[0].lower() for ln in r.read().decode("utf-8", "replace").splitlines() if len(ln.split()) >= 2}
                digest = hashlib.sha256(open(new, "rb").read()).hexdigest()
                if listed.get(ASSET) and listed[ASSET] != digest:
                    os.remove(new)
                    raise RuntimeError("The downloaded file does not match its published SHA-256. Nothing was changed.")
            self.busy = "installing"
            self.log(f"Update {self.current} -> {rel['version']} downloaded and verified ({size} bytes). Restarting to apply it.")
            if os.name != "nt":
                os.chmod(new, 0o755)
            args = [a for a in sys.argv[1:] if a != "--after-update"] if getattr(sys, "frozen", False) else []

            def go():
                time.sleep(1.5)            # let the panel receive its answer first
                try:
                    _swap_and_restart(exe, new, args, self.quit_app)
                except Exception as exc:  # noqa: BLE001
                    self.error, self.busy = "Update failed: " + str(exc)[:160], ""
            threading.Thread(target=go, daemon=True).start()
        except PermissionError:
            self.error = "No permission to write next to the app. Move Fuads_Server.exe to a normal folder (like Documents) and try again."
            self.busy = ""
        except Exception as exc:  # noqa: BLE001
            self.error = "Update failed: " + str(exc)[:160]
            self.busy = ""
        return self.status()

    # ---- background loop ----
    def start(self):
        threading.Thread(target=self._loop, daemon=True).start()

    def _loop(self):
        time.sleep(20)
        while True:
            try:
                cfg = self.get_cfg()
                if self.supported and (time.time() - self.checked_at > CHECK_EVERY - 60 or not self.checked_at):
                    self.check()
                if cfg.get("auto_update", True) and self.supported and self.status()["available"] and not self.busy:
                    idle = time.time() - float(self.last_write_ts() or 0)
                    if idle >= 180:      # wait until nobody has saved anything for 3 minutes
                        self.install()
            except Exception as exc:  # noqa: BLE001
                self.log(f"Updater error: {exc}")
            time.sleep(60)


def _swap_and_restart(exe, new, args, quit_app):
    """Windows lets a running exe be RENAMED (not overwritten): move the old one aside, put the new one in its place,
    close this copy, then start the new one. No batch file or hidden helper process is involved."""
    old = exe + ".old"
    try:
        if os.path.exists(old):
            os.remove(old)
    except OSError:
        pass
    os.replace(exe, old)
    try:
        os.replace(new, exe)
    except OSError:
        os.replace(old, exe)       # put things back; the app keeps running the version it had
        raise
    # start the new copy FIRST (it waits for this one to finish closing), then close this one
    flags = (0x00000008 | 0x00000200) if os.name == "nt" else 0     # DETACHED_PROCESS | NEW_PROCESS_GROUP
    subprocess.Popen([exe, "--after-update"] + list(args), creationflags=flags, close_fds=True, start_new_session=(os.name != "nt"))
    try:
        quit_app()
    finally:
        os._exit(0)


def cleanup_old(exe=None):
    """Remove the previous version left beside the exe by the last update."""
    exe = exe or exe_path()
    if exe:
        try:
            os.remove(exe + ".old")
        except OSError:
            pass
