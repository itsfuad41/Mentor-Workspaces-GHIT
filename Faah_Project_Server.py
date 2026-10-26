#!/usr/bin/env python3
"""
Faah Project Server
-------------------
Multi-PC High-Availability Sync Server for FAAH Mentor Workstation.
Supports 1 Primary + Multiple Backup Replicas, rolling backups,
failover term election, and embedded workflow telemetry.
"""

import base64
import collections
import hashlib
import http.server
import json
import os
import queue
import re
import secrets
import shutil
import socket
import sqlite3
import subprocess
import sys
import threading
import time
import urllib.error
import urllib.parse
import urllib.request
import uuid
import webbrowser
from collections import deque
from datetime import datetime

APP_NAME = "Faah Project Server"
DEFAULT_PORT = 8765
MAX_BODY = 200 * 1024 * 1024  # 200 MB
DEFAULT_SUPABASE_URL = "https://xupzriltfvznmaelixft.supabase.co"
DEFAULT_SUPABASE_KEY = "sb_publishable_rS81a0VMt3tIsqJU9UYKqA_niXFN-gS"
KV_PREFIXES = ("draft:", "qc:")
SERVER_START_TIME = time.time()

WORKFLOW_HTML = r"""<!doctype html>
<html lang="en">
<head>
<meta charset="utf-8">
<meta name="viewport" content="width=device-width,initial-scale=1">
<title>Faah DB Workflows</title>
<style>
:root{--bg:#f3f5f8;--card:#fff;--ink:#16202a;--mut:#5b6976;--line:#d6dde5;--ok:#1f8a5b;--bad:#c0392b;--warn:#b7791f;--acc:#1769aa;--soft:#eef3f8}
@media(prefers-color-scheme:dark){:root{--bg:#0f151b;--card:#18212a;--ink:#e6edf3;--mut:#93a1ae;--line:#2a3641;--soft:#1f2b36}}
*{box-sizing:border-box}
body{margin:0;background:var(--bg);color:var(--ink);font:14px/1.45 system-ui,Segoe UI,Roboto,sans-serif}
header{display:flex;align-items:center;gap:12px;flex-wrap:wrap;padding:10px 18px;background:var(--card);border-bottom:1px solid var(--line);position:sticky;top:0;z-index:5}
header h1{font-size:17px;margin:0 8px 0 0}
.pill{padding:3px 10px;border-radius:99px;font-size:12px;font-weight:600;background:var(--soft);border:1px solid var(--line)}
.pill.ok{color:var(--ok);border-color:var(--ok)}.pill.bad{color:var(--bad);border-color:var(--bad)}.pill.warn{color:var(--warn);border-color:var(--warn)}
.sp{flex:1}
button{font:inherit;cursor:pointer;border:1px solid var(--line);background:var(--soft);color:var(--ink);padding:5px 12px;border-radius:7px}
button.primary{background:var(--acc);color:#fff;border-color:var(--acc)}
nav{display:flex;gap:4px;padding:10px 18px 0}
nav button{border-radius:8px 8px 0 0;border-bottom:none}
nav button.on{background:var(--card);font-weight:700;border-color:var(--line)}
main{padding:0 18px 30px}
.panel{display:none;background:var(--card);border:1px solid var(--line);border-radius:0 10px 10px 10px;padding:16px}
.panel.on{display:block}
.grid{display:grid;grid-template-columns:repeat(auto-fit,minmax(420px,1fr));gap:14px}
.card{border:1px solid var(--line);border-radius:10px;padding:12px 14px;background:var(--card)}
.card h3{margin:0 0 8px;font-size:15px;display:flex;align-items:center;gap:8px}
table{border-collapse:collapse;width:100%;font-size:13px}
th,td{text-align:left;padding:5px 8px;border-bottom:1px solid var(--line);vertical-align:top}
th{color:var(--mut);font-weight:600;position:sticky;top:0;background:var(--card)}
.scroll{max-height:320px;overflow:auto}
.mut{color:var(--mut)}.ok{color:var(--ok)}.bad{color:var(--bad)}.warn{color:var(--warn)}
.mono{font-family:ui-monospace,Consolas,monospace;font-size:12px}
#login{position:fixed;inset:0;background:rgba(10,16,22,.72);display:flex;align-items:center;justify-content:center;z-index:20}
#login .box{background:var(--card);padding:22px 24px;border-radius:12px;width:380px;max-width:94vw}
#login input{width:100%;padding:7px 9px;border:1px solid var(--line);border-radius:7px;background:var(--bg);color:var(--ink);margin:6px 0 12px}
#err{color:var(--bad);min-height:20px;margin-top:8px;font-weight:600}
</style>
</head>
<body>
<header>
  <h1>Faah DB Workflows (Multi-Replica Cluster)</h1>
  <span class="pill" id="pMode">server: ...</span>
  <span class="pill" id="pRepl">cluster: ...</span>
  <span class="sp"></span>
  <span class="mut" id="stamp"></span>
  <button id="btnRefresh">Refresh</button>
  <button id="btnOut">Log out</button>
</header>
<nav id="tabs">
  <button data-p="dbs" class="on">Cluster & Databases</button>
  <button data-p="act">Live Activity</button>
</nav>
<main>
  <section class="panel on" id="dbs"><div class="grid" id="dbCards"></div></section>
  <section class="panel" id="act">
    <div style="display:flex;gap:10px;align-items:center;margin-bottom:8px;">
      <input id="actFilter" placeholder="Filter activity..." style="padding:6px 9px;border:1px solid var(--line);border-radius:7px;background:var(--bg);color:var(--ink);min-width:260px">
      <span class="mut" id="actStats"></span>
    </div>
    <div class="scroll" style="max-height:560px"><table id="actTable"><thead><tr><th>Time</th><th>User</th><th>Method</th><th>Target</th><th>Result</th><th>Detail</th></tr></thead><tbody></tbody></table></div>
  </section>
</main>
<div id="login"><div class="box">
  <h2>Admin Sign-In</h2>
  <label>Server Address</label><input id="srv" placeholder="http://127.0.0.1:8765">
  <label>Admin Key / Code</label><input id="k" placeholder="Admin Key or FAAH1:...">
  <div id="err"></div>
  <button class="primary" id="go" style="width:100%">Sign in</button>
</div></div>
<script>
const $=id=>document.getElementById(id);
const esc=v=>String(v??"").replace(/[&<>"']/g,c=>({"&":"&amp;","<":"&lt;",">":"&gt;",'"':"&quot;","'":"&#39;"}[c]));
const S={base:"",auth:"",info:null,repl:null,act:[],actLast:0,timer:null};

function decodeCode(v){try{v=v.trim().replace(/^FAAH1:/,"");const j=JSON.parse(atob(v.replace(/-/g,"+").replace(/_/g,"/")+"=".repeat((4-v.length%4)%4)));return j.a&&j.u?j:null}catch{return null}}
async function api(path,opt={}){const r=await fetch(S.base+path,{...opt,headers:{"X-Key":S.auth,...(opt.body?{"Content-Type":"application/json"}:{}),...(opt.headers||{})}});let j=null;try{j=await r.json()}catch{}if(!r.ok)throw Error((j&&j.error)||("HTTP "+r.status));return j;}

function showLogin(msg){$("login").style.display="flex";$("err").textContent=msg||"";}
async function signIn(){
  let base=$("srv").value.trim().replace(/\/+$/,"")||window.location.origin;
  const key=$("k").value.trim(),code=key&&decodeCode(key);
  if(code)base=code.u.replace(/\/+$/,"");
  S.base=base;S.auth=code?code.a:key;
  try{const r=await api("/ping");if(r.role!=="admin")throw Error("Not admin key.");$("login").style.display="none";start();}
  catch(e){$("err").textContent=e.message;}
}
function signOut(){S.auth="";clearInterval(S.timer);showLogin("");}
async function refresh(){
  try{
    S.info=await api("/admin/dbinfo?detail=1");
    S.repl=await api("/admin/replication");
    const a=await api("/admin/activity?since="+S.actLast);
    if(a.items.length){S.act=[...a.items.reverse(),...S.act].slice(0,300);S.actLast=a.last;}
    S.stats=a.stats;
    paint();
  }catch(e){$("pMode").className="pill bad";$("pMode").textContent="Unreachable";}
}
function start(){refresh();clearInterval(S.timer);S.timer=setInterval(refresh,4000);}

function paint(){
  const i=S.info,r=S.repl;
  if(i){$("pMode").className="pill "+(i.server.mode==="replica"?"warn":"ok");$("pMode").textContent=`Node: ${i.server.mode.toUpperCase()} (Term ${i.server.term})`;}
  if(r){$("pRepl").className="pill "+(r.in_sync?"ok":"warn");$("pRepl").textContent=`Cluster: ${r.peer_count||0} Peers`;}
  $("stamp").textContent="Updated "+new Date().toLocaleTimeString();

  let html=`<div class="card"><h3>Cluster Topology</h3><table><tbody>
    <tr><td>Node Role</td><td><b>${esc(r.mode.toUpperCase())}</b> (Term ${r.term})</td></tr>
    <tr><td>Status</td><td>${esc(r.status)}</td></tr>
    <tr><td>Connected Peers</td><td>${(r.peers||[]).map(p=>`<div class="mono">${esc(p.url)} - ${p.reachable?"Online":"Unreachable"} (${p.mode||"?"})</div>`).join("")||"None (Stand-alone)"}</td></tr>
  </tbody></table></div>`;
  
  if(i){
    html+=`<div class="card"><h3>Local Projects (${i.projects.length})</h3><div class="scroll"><table><thead><tr><th>Project</th><th>Parts</th><th>Version</th></tr></thead><tbody>
      ${i.projects.map(p=>`<tr><td><b>${esc(p.name)}</b></td><td>${p.parts}</td><td class="mono">${esc(p.version.slice(0,8))}</td></tr>`).join("")||"<tr><td colspan='3'>No projects</td></tr>"}
    </tbody></table></div></div>`;
  }
  $("dbCards").innerHTML=html;

  const f=$("actFilter").value.trim().toLowerCase();
  $("actTable").querySelector("tbody").innerHTML=S.act.filter(a=>!f||[a.who,a.method,a.path,a.detail].join(" ").toLowerCase().includes(f)).map(a=>`<tr>
    <td>${new Date(a.ts*1000).toLocaleTimeString()}</td><td>${esc(a.who)}</td><td><b>${esc(a.method)}</b></td><td class="mono">${esc(a.path)}</td>
    <td class="${a.status>=400?"bad":"ok"}">${a.status}</td><td>${esc(a.detail||"")}</td>
  </tr>`).join("")||"<tr><td colspan='6'>No activity recorded</td></tr>";
}
document.querySelectorAll("#tabs button").forEach(b=>b.onclick=()=>{document.querySelectorAll("#tabs button").forEach(x=>x.classList.toggle("on",x===b));document.querySelectorAll(".panel").forEach(p=>p.classList.toggle("on",p.id===b.dataset.p));});
$("go").onclick=signIn;
$("btnRefresh").onclick=refresh;
$("btnOut").onclick=signOut;
if(window.location.pathname.includes("workflow")){$("srv").value=window.location.origin;}
</script>
</body>
</html>
"""

def _data_dir():
    forced = os.environ.get("FAAH_SERVER_DIR")
    if forced:
        path = forced
    elif sys.platform.startswith("win"):
        path = r"C:\Data\FaahProjectServer"
    else:
        path = os.path.join(os.path.expanduser("~"), ".faah_project_server")
    os.makedirs(path, exist_ok=True)
    return path

DATA_DIR = _data_dir()
DB_PATH = os.path.join(DATA_DIR, "projects.db")
CONFIG_PATH = os.path.join(DATA_DIR, "server_config.json")
BACKUP_DIR = os.path.join(DATA_DIR, "backups")

_write_lock = threading.Lock()
_log = deque(maxlen=300)
_CFG = None

def log(msg):
    ts = datetime.now().strftime("%H:%M:%S")
    _log.append(f"{ts}  {msg}")
    print(f"[{ts}] {msg}", flush=True)

def save_config(cfg):
    try:
        with open(CONFIG_PATH, "w", encoding="utf-8") as fh:
            json.dump(cfg, fh, indent=2)
    except OSError as e:
        log(f"Config write error: {e}")

def load_config():
    global _CFG
    if _CFG is not None:
        return _CFG
    try:
        with open(CONFIG_PATH, "r", encoding="utf-8") as fh:
            cfg = json.load(fh)
    except (OSError, ValueError):
        cfg = {}
    changed = False
    for key in ("admin_key", "view_key"):
        if not cfg.get(key):
            cfg[key] = secrets.token_urlsafe(12)
            changed = True
    defaults = (
        ("port", DEFAULT_PORT),
        ("auto_tunnel", False),
        ("admin_cache", {}),
        ("mode", "primary"),
        ("term", 1),
        ("peers", []),
        ("custom_url", ""),
        ("auto_promote_after", 60),
        ("backup_hours", 24),
        ("backup_keep", 30),
        ("supabase_url", DEFAULT_SUPABASE_URL),
        ("supabase_key", DEFAULT_SUPABASE_KEY),
    )
    for key, default in defaults:
        if key not in cfg or (key == "port" and not cfg[key]):
            cfg[key] = default
            changed = True
    if changed:
        save_config(cfg)
    _CFG = cfg
    return cfg

def make_code(url, view_key, admin_key):
    cfg = load_config()
    final_url = cfg.get("custom_url") or url
    raw = json.dumps({
        "u": final_url,
        "v": view_key,
        "a": admin_key,
        "t": cfg.get("term", 1),
        "m": cfg.get("mode", "primary")
    }, separators=(",", ":")).encode("utf-8")
    return "FAAH1:" + base64.urlsafe_b64encode(raw).decode("ascii").rstrip("=")

def parse_code(code_str):
    raw = code_str.strip().replace("FAAH1:", "")
    padded = raw + "=" * ((4 - len(raw) % 4) % 4)
    return json.loads(base64.urlsafe_b64decode(padded.encode()).decode("utf-8"))

def init_and_migrate_db():
    with _write_lock:
        conn = sqlite3.connect(DB_PATH, timeout=15)
        try:
            conn.execute("PRAGMA journal_mode=WAL;")
            conn.execute("PRAGMA synchronous=NORMAL;")
            tbl = conn.execute("SELECT name FROM sqlite_master WHERE type='table' AND name='projects'").fetchone()
            if not tbl:
                conn.execute("""
                    CREATE TABLE projects (
                        name TEXT PRIMARY KEY,
                        parts TEXT NOT NULL DEFAULT '[]',
                        part_count INTEGER DEFAULT 0,
                        version TEXT DEFAULT '',
                        updated_at TEXT DEFAULT '',
                        updated_by TEXT DEFAULT '',
                        epoch TEXT DEFAULT ''
                    )
                """)
            else:
                cols = {r[1] for r in conn.execute("PRAGMA table_info(projects)").fetchall()}
                if "parts" not in cols:
                    if "data" in cols:
                        try:
                            conn.execute("ALTER TABLE projects RENAME COLUMN data TO parts")
                        except Exception:
                            conn.execute("ALTER TABLE projects ADD COLUMN parts TEXT NOT NULL DEFAULT '[]'")
                            conn.execute("UPDATE projects SET parts = data WHERE parts = '[]' OR parts IS NULL")
                    else:
                        conn.execute("ALTER TABLE projects ADD COLUMN parts TEXT NOT NULL DEFAULT '[]'")
                for c, d in (("part_count", "INTEGER DEFAULT 0"), ("version", "TEXT DEFAULT ''"),
                            ("updated_at", "TEXT DEFAULT ''"), ("updated_by", "TEXT DEFAULT ''"),
                            ("epoch", "TEXT DEFAULT ''")):
                    if c not in cols:
                        try:
                            conn.execute(f"ALTER TABLE projects ADD COLUMN {c} {d}")
                        except Exception:
                            pass
            conn.execute("""
                CREATE TABLE IF NOT EXISTS kv (
                    key TEXT PRIMARY KEY,
                    value TEXT NOT NULL DEFAULT '',
                    version TEXT DEFAULT '',
                    updated_at TEXT DEFAULT '',
                    updated_by TEXT DEFAULT ''
                )
            """)
            conn.commit()
            log("Database schema verified (parts column confirmed).")
        finally:
            conn.close()

def _db():
    conn = sqlite3.connect(DB_PATH, timeout=15)
    conn.row_factory = sqlite3.Row
    conn.execute("PRAGMA journal_mode=WAL;")
    conn.execute("PRAGMA synchronous=NORMAL;")
    return conn

def _now():
    return datetime.now().strftime("%Y-%m-%d %H:%M:%S")

def _ensure_ids(parts):
    for p in parts:
        if isinstance(p, dict) and not p.get("_id"):
            p["_id"] = uuid.uuid4().hex

def list_projects():
    conn = _db()
    try:
        rows = conn.execute("SELECT name, part_count, version, updated_at, updated_by, epoch FROM projects ORDER BY name").fetchall()
        return [{"project_name": r["name"], "part_count": r["part_count"] or 0, "version": r["version"] or "",
                 "updated_at": r["updated_at"] or "", "updated_by": r["updated_by"] or "", "epoch": r["epoch"] or ""} for r in rows]
    finally:
        conn.close()

def get_project(name):
    conn = _db()
    try:
        r = conn.execute("SELECT parts, version, epoch FROM projects WHERE name = ?", (name,)).fetchone()
        return None if not r else {"project_name": name, "parts": json.loads(r["parts"]), "version": r["version"] or "", "epoch": r["epoch"] or ""}
    finally:
        conn.close()

def put_project(name, parts, by=""):
    _ensure_ids(parts)
    text = json.dumps(parts, separators=(",", ":"), ensure_ascii=False)
    version = hashlib.md5(text.encode("utf-8")).hexdigest()
    epoch = uuid.uuid4().hex
    with _write_lock:
        conn = _db()
        try:
            conn.execute("INSERT OR REPLACE INTO projects (name, parts, part_count, version, updated_at, updated_by, epoch) VALUES (?,?,?,?,?,?,?)",
                         (name, text, len(parts), version, _now(), by or "", epoch))
            conn.commit()
        finally:
            conn.close()
    return version, epoch

def patch_project(name, upserts, deletes, base_version, by="", epoch=""):
    with _write_lock:
        conn = _db()
        try:
            row = conn.execute("SELECT parts, version, epoch FROM projects WHERE name = ?", (name,)).fetchone()
            if not row:
                return None
            if epoch and row["epoch"] and epoch != row["epoch"]:
                return {"replaced": True}
            parts = json.loads(row["parts"])
            current = row["version"] or ""
            dels = set(deletes or [])
            if dels:
                parts = [p for p in parts if p.get("_id") not in dels]
            idx = {p.get("_id"): i for i, p in enumerate(parts)}
            for u in upserts or []:
                if not isinstance(u, dict):
                    continue
                if not u.get("_id"):
                    u["_id"] = uuid.uuid4().hex
                if u["_id"] in idx:
                    parts[idx[u["_id"]]] = u
                else:
                    parts.append(u)
                    idx[u["_id"]] = len(parts) - 1
            text = json.dumps(parts, separators=(",", ":"), ensure_ascii=False)
            version = hashlib.md5(text.encode("utf-8")).hexdigest()
            conn.execute("UPDATE projects SET parts=?, part_count=?, version=?, updated_at=?, updated_by=? WHERE name=?",
                         (text, len(parts), version, _now(), by or "", name))
            conn.commit()
        finally:
            conn.close()
    return {"version": version, "merged": (not base_version) or base_version != current, "epoch": row["epoch"] or ""}

def delete_project(name):
    with _write_lock:
        conn = _db()
        try:
            conn.execute("DELETE FROM projects WHERE name = ?", (name,))
            conn.commit()
        finally:
            conn.close()

def kv_get(key):
    conn = _db()
    try:
        r = conn.execute("SELECT value, version, updated_at, updated_by FROM kv WHERE key = ?", (key,)).fetchone()
        return None if not r else {"key": key, "value": r["value"], "version": r["version"], "updated_at": r["updated_at"]}
    finally:
        conn.close()

def kv_put(key, value, by=""):
    value = value if isinstance(value, str) else json.dumps(value)
    version = hashlib.md5(value.encode("utf-8")).hexdigest()
    with _write_lock:
        conn = _db()
        try:
            conn.execute("INSERT OR REPLACE INTO kv (key, value, version, updated_at, updated_by) VALUES (?,?,?,?,?)",
                         (key, value, version, _now(), by or ""))
            conn.commit()
        finally:
            conn.close()
    return version

def kv_delete(key):
    with _write_lock:
        conn = _db()
        try:
            conn.execute("DELETE FROM kv WHERE key = ?", (key,))
            conn.commit()
        finally:
            conn.close()

def kv_versions(keys):
    out = {}
    conn = _db()
    try:
        for k in keys:
            r = conn.execute("SELECT version FROM kv WHERE key = ?", (k,)).fetchone() if (k and k.startswith(KV_PREFIXES)) else None
            out[k] = {"version": r["version"]} if r else None
    finally:
        conn.close()
    return out

def backup_db():
    os.makedirs(BACKUP_DIR, exist_ok=True)
    dest = os.path.join(BACKUP_DIR, f"projects_{datetime.now().strftime('%Y-%m-%d_%H-%M-%S')}.db")
    with _write_lock:
        src = _db()
        try:
            dst = sqlite3.connect(dest)
            try:
                src.backup(dst)
            finally:
                dst.close()
        finally:
            src.close()
    keep = int(load_config().get("backup_keep") or 30)
    files = sorted(f for f in os.listdir(BACKUP_DIR) if f.startswith("projects_") and f.endswith(".db"))
    if keep > 0 and len(files) > keep:
        for old in files[:-keep]:
            try:
                os.remove(os.path.join(BACKUP_DIR, old))
            except OSError:
                pass
    log(f"Backup saved: {os.path.basename(dest)}")
    return dest

def _backup_loop():
    while True:
        try:
            hours = float(load_config().get("backup_hours") or 24)
            files = [os.path.join(BACKUP_DIR, f) for f in os.listdir(BACKUP_DIR)] if os.path.isdir(BACKUP_DIR) else []
            last = max((os.path.getmtime(f) for f in files), default=0)
            if time.time() - last >= hours * 3600:
                backup_db()
        except Exception:
            pass
        time.sleep(600)

def start_backups():
    threading.Thread(target=_backup_loop, daemon=True).start()

def promote_to_primary():
    cfg = load_config()
    cfg["mode"] = "primary"
    cfg["term"] = int(cfg.get("term") or 0) + 1
    save_config(cfg)
    log(f"Server promoted to PRIMARY (Term {cfg['term']})")

def step_down_to_replica(peer_url, peer_term):
    cfg = load_config()
    cfg["mode"] = "replica"
    cfg["term"] = peer_term
    save_config(cfg)
    log(f"Server stepped down to REPLICA (Term {peer_term}) following {peer_url}")

def pull_sync_from(peer):
    url = peer["url"].rstrip("/") + "/api/sync"
    req = urllib.request.Request(
        url,
        headers={"User-Agent": "FaahReplicator/2.5", "X-Key": peer.get("admin_key") or load_config().get("admin_key")},
        method="GET"
    )
    with urllib.request.urlopen(req, timeout=8) as resp:
        data = json.loads(resp.read().decode("utf-8"))

    with _write_lock:
        conn = _db()
        try:
            for item in data.get("projects", []):
                p_text = json.dumps(item["parts"], separators=(",", ":"), ensure_ascii=False) if isinstance(item["parts"], list) else str(item["parts"])
                conn.execute(
                    "INSERT OR REPLACE INTO projects (name, parts, part_count, version, updated_at, updated_by, epoch) "
                    "VALUES (?, ?, ?, ?, ?, ?, ?)",
                    (item["name"], p_text, item.get("part_count", 0), item.get("version", ""), item.get("updated_at", ""), item.get("updated_by", ""), item.get("epoch", ""))
                )
            for kitem in data.get("kv", []):
                conn.execute(
                    "INSERT OR REPLACE INTO kv (key, value, version, updated_at, updated_by) VALUES (?, ?, ?, ?, ?)",
                    (kitem["key"], kitem["value"], kitem.get("version", ""), kitem.get("updated_at", ""), kitem.get("updated_by", ""))
                )
            conn.commit()
        finally:
            conn.close()
    return data

class MultiPeerReplicator:
    def __init__(self):
        self.running = False
        self.down_since = None
        self.status = "Idle"
        self.peer_states = {}
        self.in_sync = True

    def start(self):
        if not self.running:
            self.running = True
            threading.Thread(target=self._loop, daemon=True).start()

    def _loop(self):
        while self.running:
            self.tick()
            time.sleep(float(os.environ.get("FAAH_REPL_SECONDS", 10)))

    def tick(self):
        cfg = load_config()
        peers = cfg.get("peers", [])
        if not peers:
            self.status = "Stand-alone (No peers)"
            return

        active_primary = None
        highest_peer_term = 0

        for p in peers:
            p_url = p["url"].rstrip("/")
            try:
                req = urllib.request.Request(p_url + "/health", headers={"User-Agent": "FaahHealth/2.5"}, method="GET")
                with urllib.request.urlopen(req, timeout=4) as resp:
                    h = json.loads(resp.read().decode())
                    p_mode = h.get("mode", "replica")
                    p_term = int(h.get("term", 0))
                    self.peer_states[p_url] = {"reachable": True, "mode": p_mode, "term": p_term}
                    if p_term > highest_peer_term:
                        highest_peer_term = p_term
                    if p_mode == "primary" and (not active_primary or p_term > active_primary["term"]):
                        active_primary = {"peer": p, "term": p_term}
            except Exception:
                self.peer_states[p_url] = {"reachable": False, "mode": "unknown", "term": 0}

        my_mode = cfg.get("mode", "primary")
        my_term = int(cfg.get("term", 1))

        if my_mode == "primary" and active_primary and active_primary["term"] > my_term:
            step_down_to_replica(active_primary["peer"]["url"], active_primary["term"])
            my_mode = "replica"

        if my_mode == "replica":
            if active_primary:
                try:
                    pull_sync_from(active_primary["peer"])
                    self.down_since = None
                    self.in_sync = True
                    self.status = f"In sync with Primary ({active_primary['peer']['url'].split('//')[-1][:25]})"
                except Exception as e:
                    self.in_sync = False
                    self.status = f"Sync failed: {str(e)[:40]}"
            else:
                self.in_sync = False
                self.status = "Primary unreachable"
                after = int(cfg.get("auto_promote_after") or 60)
                self.down_since = self.down_since or time.time()
                if after and (time.time() - self.down_since >= after):
                    log(f"All primaries down for {after}s: Auto-promoting this node to PRIMARY")
                    promote_to_primary()
                    self.down_since = None
                    self.status = "Auto-promoted to Primary"
        else:
            self.status = f"Primary (Term {my_term}) serving {len(peers)} replica(s)"

REPLICATOR = MultiPeerReplicator()

class ActivityTracker:
    def __init__(self):
        self.lock = threading.Lock()
        self.items = deque(maxlen=600)
        self.writes = 0
        self.polls = 0
        self.errors = 0

    def record(self, who, method, path, status, detail=""):
        with self.lock:
            if status >= 400:
                self.errors += 1
            if method in ("PUT", "POST", "PATCH", "DELETE"):
                self.writes += 1
            if path in ("/state", "/ping"):
                self.polls += 1
            self.items.append({"ts": time.time(), "who": who or "anon", "method": method, "path": path, "status": status, "detail": detail})

    def get_data(self, since=0):
        with self.lock:
            return {
                "items": [it for it in self.items if it["ts"] > since],
                "last": max((it["ts"] for it in self.items), default=0),
                "stats": {"writes": self.writes, "polls": self.polls, "errors": self.errors}
            }

ACTIVITY = ActivityTracker()

class Handler(http.server.BaseHTTPRequestHandler):
    server_version = "FaahProjectServer/2.6"

    def log_message(self, *args):
        pass

    def _cors(self):
        self.send_header("Access-Control-Allow-Origin", "*")
        self.send_header("Access-Control-Allow-Headers", "X-Key, Content-Type, Authorization")
        self.send_header("Access-Control-Allow-Methods", "GET, PUT, PATCH, DELETE, OPTIONS, POST")

    def _send(self, code, obj, raw=None):
        body = raw if raw is not None else json.dumps(obj).encode("utf-8")
        self.send_response(code)
        self._cors()
        self.send_header("Content-Type", "text/plain; charset=utf-8" if raw is not None else "application/json; charset=utf-8")
        self.send_header("Content-Length", str(len(body)))
        self.end_headers()
        self.wfile.write(body)

    def _role(self):
        k = (self.headers.get("X-Key") or "").strip()
        auth = (self.headers.get("Authorization") or "").strip().replace("Bearer ", "")
        token = auth or k
        cfg = load_config()
        if token and (secrets.compare_digest(token, cfg.get("admin_key", "")) or token in (cfg.get("admin_tokens") or [])):
            return "admin"
        if token and secrets.compare_digest(token, cfg.get("view_key", "")):
            return "user"
        return None

    def _body(self):
        l = int(self.headers.get("Content-Length") or 0)
        return json.loads(self.rfile.read(l) or b"{}")

    def do_OPTIONS(self):
        self.send_response(204)
        self._cors()
        self.end_headers()

    def _route(self, method):
        try:
            path = urllib.parse.urlparse(self.path).path
            cfg = load_config()

            if path in ("/workflows", "/workflow.html") and method == "GET":
                b = WORKFLOW_HTML.encode("utf-8")
                self.send_response(200)
                self._cors()
                self.send_header("Content-Type", "text/html; charset=utf-8")
                self.send_header("Content-Length", str(len(b)))
                self.end_headers()
                return self.wfile.write(b)

            if path == "/" and method == "GET":
                return self._send(200, None, raw=f"{APP_NAME} cluster running.".encode())

            if path in ("", "/health") and method == "GET":
                return self._send(200, {"status": "ok", "mode": cfg.get("mode", "primary"), "term": cfg.get("term", 1), "time": datetime.now().isoformat()})

            role = self._role()
            who = self.headers.get("X-Who") or role or "guest"

            if path == "/admin/login" and method == "POST":
                b = self._body()
                u, p = str(b.get("username", "")).strip().lower(), str(b.get("password", "")).strip()
                ok, msg = verify_admin_login(u, p)
                if not ok:
                    ACTIVITY.record(u, "POST", path, 401, msg)
                    return self._send(401, {"error": msg})
                t = secrets.token_hex(16)
                cfg.setdefault("admin_tokens", []).append(t)
                save_config(cfg)
                ACTIVITY.record(u, "POST", path, 200, "Signed in")
                return self._send(200, {"token": t})

            if not role:
                ACTIVITY.record(who, method, path, 401, "Wrong key")
                return self._send(401, {"error": "Wrong server key."})

            if path == "/ping":
                return self._send(200, {"ok": True, "role": role})

            if path == "/admin/dbinfo" and method == "GET":
                if role != "admin":
                    return self._send(403, {"error": "Admin required."})
                conn = _db()
                try:
                    p_rows = conn.execute("SELECT name, parts, version, updated_at, updated_by FROM projects").fetchall()
                finally:
                    conn.close()
                projs = []
                for r in p_rows:
                    try:
                        pts = json.loads(r["parts"]) if r["parts"] else []
                    except Exception:
                        pts = []
                    projs.append({"name": r["name"], "parts": len(pts), "updated_by": r["updated_by"] or "Admin", "updated_at": r["updated_at"] or "", "version": r["version"] or "1"})
                return self._send(200, {
                    "server": {"mode": cfg.get("mode", "primary"), "term": cfg.get("term", 1), "db_size": os.path.getsize(DB_PATH) if os.path.exists(DB_PATH) else 0, "db_path": DB_PATH},
                    "projects": projs,
                    "tables": [{"name": "projects", "rows": len(p_rows)}, {"name": "kv", "rows": 0}],
                    "supabase": {"url": cfg.get("supabase_url"), "key": cfg.get("supabase_key")}
                })

            if path == "/admin/activity" and method == "GET":
                if role != "admin":
                    return self._send(403, {"error": "Admin required."})
                return self._send(200, ACTIVITY.get_data())

            if path == "/admin/replication" and method == "GET":
                if role != "admin":
                    return self._send(403, {"error": "Admin required."})
                p_list = [{"url": p["url"], "reachable": REPLICATOR.peer_states.get(p["url"].rstrip("/"), {}).get("reachable", False), "mode": REPLICATOR.peer_states.get(p["url"].rstrip("/"), {}).get("mode", "?")} for p in cfg.get("peers", [])]
                return self._send(200, {"mode": cfg.get("mode", "primary"), "term": cfg.get("term", 1), "peer_count": len(p_list), "peers": p_list, "in_sync": REPLICATOR.in_sync, "status": REPLICATOR.status})

            if path == "/api/sync" and method == "GET":
                conn = _db()
                try:
                    p_cur = conn.execute("SELECT name, parts, part_count, version, updated_at, updated_by, epoch FROM projects").fetchall()
                    p_rows = [{"name": r["name"], "parts": json.loads(r["parts"]), "part_count": r["part_count"], "version": r["version"], "updated_at": r["updated_at"], "updated_by": r["updated_by"], "epoch": r["epoch"]} for r in p_cur]
                    k_cur = conn.execute("SELECT key, value, version, updated_at, updated_by FROM kv").fetchall()
                    k_rows = [{"key": r["key"], "value": r["value"], "version": r["version"], "updated_at": r["updated_at"], "updated_by": r["updated_by"]} for r in k_cur]
                finally:
                    conn.close()
                return self._send(200, {"mode": cfg.get("mode", "primary"), "term": cfg.get("term", 1), "projects": p_rows, "kv": k_rows})

            if path == "/projects" and method == "GET":
                return self._send(200, list_projects())

            if path == "/state" and method == "GET":
                keys = [k for k in (urllib.parse.parse_qs(urllib.parse.urlparse(self.path).query).get("keys") or [""])[0].split(",") if k]
                return self._send(200, {"projects": list_projects(), "kv": kv_versions(keys)})

            if path.startswith("/kv/"):
                key = urllib.parse.unquote(path[len("/kv/"):])
                if method == "GET":
                    it = kv_get(key)
                    return self._send(200 if it else 404, it or {"error": "Not found."})
                if method == "PUT":
                    if cfg.get("mode") == "replica":
                        return self._send(403, {"error": "Server is read-only BACKUP replica."})
                    b = self._body()
                    v = kv_put(key, b.get("value", ""), str(b.get("updated_by") or ""))
                    ACTIVITY.record(who, "PUT", path, 200)
                    return self._send(200, {"ok": True, "version": v})
                if method == "DELETE":
                    if cfg.get("mode") == "replica":
                        return self._send(403, {"error": "Server is read-only BACKUP replica."})
                    kv_delete(key)
                    ACTIVITY.record(who, "DELETE", path, 200)
                    return self._send(200, {"ok": True})

            if path.startswith("/projects/"):
                name = urllib.parse.unquote(path[len("/projects/"):])
                if method == "GET":
                    d = get_project(name)
                    return self._send(200 if d else 404, d or {"error": "Not found."})
                if method == "PATCH":
                    if cfg.get("mode") == "replica":
                        return self._send(403, {"error": "Server is read-only BACKUP replica."})
                    b = self._body()
                    res = patch_project(name, b.get("upserts"), b.get("deletes"), str(b.get("base_version") or ""), str(b.get("updated_by") or ""), str(b.get("epoch") or ""))
                    if not res:
                        return self._send(404, {"error": "Not found."})
                    if res.get("replaced"):
                        return self._send(409, {"error": "Project replaced.", "replaced": True})
                    ACTIVITY.record(who, "PATCH", path, 200)
                    return self._send(200, {"ok": True, **res})
                if role != "admin":
                    return self._send(403, {"error": "Admin required."})
                if method == "PUT":
                    if cfg.get("mode") == "replica":
                        return self._send(403, {"error": "Server is read-only BACKUP replica."})
                    b = self._body()
                    v, ep = put_project(name, b.get("parts", []), str(b.get("updated_by") or ""))
                    log(f"Project '{name}' updated by admin.")
                    ACTIVITY.record(who, "PUT", path, 200)
                    return self._send(200, {"ok": True, "version": v, "epoch": ep})
                if method == "DELETE":
                    if cfg.get("mode") == "replica":
                        return self._send(403, {"error": "Server is read-only BACKUP replica."})
                    delete_project(name)
                    log(f"Project '{name}' deleted.")
                    ACTIVITY.record(who, "DELETE", path, 200)
                    return self._send(200, {"ok": True})

            return self._send(404, {"error": "Not found."})
        except Exception as e:
            log(f"HTTP handler error: {e}")
            try:
                self._send(500, {"error": str(e)})
            except Exception:
                pass

    def do_GET(self): self._route("GET")
    def do_POST(self): self._route("POST")
    def do_PUT(self): self._route("PUT")
    def do_PATCH(self): self._route("PATCH")
    def do_DELETE(self): self._route("DELETE")

def _pw_hash(salt, username, password):
    return hashlib.sha256(f"{salt}|{username}|{password}".encode("utf-8")).hexdigest()

def verify_admin_login(username, password):
    username = (username or "").strip().lower()
    password = (password or "").strip()
    if not username or not password:
        return False, "Enter username and password."
    cfg = load_config()
    base = (cfg.get("supabase_url") or DEFAULT_SUPABASE_URL).rstrip("/")
    key = cfg.get("supabase_key") or DEFAULT_SUPABASE_KEY
    try:
        q = urllib.parse.quote(username, safe="")
        req = urllib.request.Request(
            f"{base}/rest/v1/system_users?select=username,password,role,is_active&username=ilike.{q}&limit=5",
            headers={"apikey": key, "Authorization": f"Bearer {key}"},
        )
        with urllib.request.urlopen(req, timeout=7) as resp:
            rows = json.loads(resp.read().decode())
    except Exception:
        cached = (cfg.get("admin_cache") or {}).get(username)
        if cached and secrets.compare_digest(cached.get("hash", ""), _pw_hash(cached.get("salt", ""), username, password)):
            return True, "Signed in (offline)."
        return False, "Online user database unreachable."
    row = next((r for r in rows if str(r.get("username", "")).strip().lower() == username), None)
    if not row or row.get("is_active") is False:
        return False, "Account invalid or inactive."
    if str(row.get("password") or "").strip() != password:
        return False, "Wrong password."
    if str(row.get("role") or "").strip().lower() != "admin":
        return False, "Admin role required."
    salt = secrets.token_hex(8)
    cfg.setdefault("admin_cache", {})[username] = {"salt": salt, "hash": _pw_hash(salt, username, password)}
    save_config(cfg)
    return True, "Signed in."

class ServerCore:
    def __init__(self):
        self.httpd = None
        self.thread = None
        self.port = None

    @property
    def running(self):
        return self.httpd is not None

    def start(self, port):
        if self.httpd:
            return True, None
        try:
            httpd = http.server.ThreadingHTTPServer(("0.0.0.0", int(port)), Handler)
        except Exception as e:
            return False, str(e)
        httpd.daemon_threads = True
        self.thread = threading.Thread(target=httpd.serve_forever, daemon=True)
        self.thread.start()
        self.httpd, self.port = httpd, int(port)
        log(f"Server listening on 0.0.0.0:{self.port}")
        return True, None

    def stop(self):
        if self.httpd:
            try:
                self.httpd.shutdown()
                self.httpd.server_close()
            except Exception:
                pass
        self.httpd = self.thread = self.port = None
        log("Server stopped.")

def lan_addresses():
    found = []
    try:
        s = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
        s.connect(("8.8.8.8", 80))
        found.append(s.getsockname()[0])
        s.close()
    except Exception:
        pass
    return found or ["127.0.0.1"]

class Tunnel:
    def __init__(self):
        self.proc = None
        self.url = None

    @property
    def running(self):
        return self.proc is not None and self.proc.poll() is None

    def start(self, port):
        exe = shutil.which("cloudflared") or os.path.join(DATA_DIR, "cloudflared.exe" if sys.platform.startswith("win") else "cloudflared")
        if not os.path.exists(exe) and not shutil.which("cloudflared"):
            raise RuntimeError("cloudflared helper not found.")
        self.url = None
        flags = 0x08000000 if sys.platform.startswith("win") else 0
        self.proc = subprocess.Popen([exe, "tunnel", "--url", f"http://localhost:{port}", "--no-autoupdate"],
                                     stdout=subprocess.PIPE, stderr=subprocess.STDOUT, text=True, creationflags=flags)
        threading.Thread(target=self._read, args=(self.proc,), daemon=True).start()

    def _read(self, proc):
        r = re.compile(r"https://[a-z0-9-]+\.trycloudflare\.com")
        for line in proc.stdout:
            m = r.search(line)
            if m and proc is self.proc and not self.url:
                self.url = m.group(0)
                log(f"Public link active: {self.url}")

    def stop(self):
        if self.proc:
            try:
                self.proc.terminate()
            except Exception:
                pass
        self.proc, self.url = None, None

def run_app(server, tunnel, cfg):
    import tkinter as tk
    from tkinter import messagebox

    init_and_migrate_db()
    REPLICATOR.start()
    start_backups()

    root = tk.Tk()
    root.title(APP_NAME)
    root.geometry("730x780")
    root.minsize(680, 720)

    v_status = tk.StringVar(value="Server running" if server.running else "Server stopped")
    v_port = tk.StringVar(value=str(cfg.get("port", DEFAULT_PORT)))
    v_local = tk.StringVar()
    v_public = tk.StringVar()
    v_custom = tk.StringVar(value=cfg.get("custom_url", ""))
    v_code = tk.StringVar()
    v_view = tk.StringVar(value=cfg["view_key"])
    v_admin = tk.StringVar(value=cfg["admin_key"])
    v_role = tk.StringVar()
    v_peer_input = tk.StringVar()

    def copy(text):
        root.clipboard_clear()
        root.clipboard_append(text)

    tk.Label(root, text=APP_NAME, font=("Segoe UI", 15, "bold")).pack(anchor="w", padx=12, pady=(10, 0))
    tk.Label(root, text="Synchronizes databases across multiple backup replica PCs.", fg="#475569").pack(anchor="w", padx=12, pady=(0, 6))

    f_top = tk.Frame(root)
    f_top.pack(fill="x", padx=12, pady=3)
    lbl_status = tk.Label(f_top, textvariable=v_status, font=("Segoe UI", 10, "bold"), fg="#218c5a")
    lbl_status.pack(side="left")
    tk.Label(f_top, text="Port:").pack(side="left", padx=(10, 2))
    tk.Entry(f_top, textvariable=v_port, width=6).pack(side="left")
    btn_srv = tk.Button(f_top, text="Stop Server" if server.running else "Start Server", width=12)
    btn_srv.pack(side="left", padx=8)
    lbl_role = tk.Label(f_top, textvariable=v_role, font=("Segoe UI", 9, "bold"), fg="#1769aa")
    lbl_role.pack(side="left", padx=6)

    def row(label, var, btn_label="Copy", width=46, editable=False):
        f = tk.Frame(root)
        f.pack(fill="x", padx=12, pady=2)
        tk.Label(f, text=label, width=22, anchor="w").pack(side="left")
        state_val = "normal" if editable else "readonly"
        tk.Entry(f, textvariable=var, width=width, state=state_val).pack(side="left", fill="x", expand=True)
        if btn_label:
            tk.Button(f, text=btn_label, command=lambda: copy(var.get()), width=10).pack(side="left", padx=(6, 0))

    row("Local address:", v_local)
    row("Temporary Public link:", v_public)
    row("Permanent Tunnel URL:", v_custom, "Save", editable=True)
    
    btn_tun = tk.Button(root, text="Start Public Link", width=22)
    btn_tun.pack(anchor="w", padx=12, pady=2)

    tk.Label(root, text="Connection details for website:", font=("Segoe UI", 9, "bold")).pack(anchor="w", padx=12, pady=(8, 2))
    row("Connection code:", v_code, "Copy Code")
    row("User key (mentors):", v_view)
    row("Admin key (private):", v_admin)

    tk.Label(root, text="Cluster / Multi-Peer Connections (Add 1 or more Backup PCs):", font=("Segoe UI", 9, "bold")).pack(anchor="w", padx=12, pady=(10, 2))
    
    f_add = tk.Frame(root)
    f_add.pack(fill="x", padx=12, pady=2)
    tk.Label(f_add, text="Peer connection code:", width=20, anchor="w").pack(side="left")
    ent_code = tk.Entry(f_add, textvariable=v_peer_input)
    ent_code.pack(side="left", fill="x", expand=True)
    
    def paste_code():
        try: v_peer_input.set(root.clipboard_get().strip())
        except Exception: pass
    tk.Button(f_add, text="Paste", width=7, command=paste_code).pack(side="left", padx=4)

    f_list = tk.Frame(root)
    f_list.pack(fill="x", padx=12, pady=4)
    peer_box = tk.Listbox(f_list, height=3, font=("Consolas", 9))
    peer_box.pack(side="left", fill="x", expand=True)

    def refresh_peer_box():
        peer_box.delete(0, tk.END)
        for p in cfg.get("peers", []):
            st = REPLICATOR.peer_states.get(p["url"].rstrip("/"), {})
            status_txt = "Online" if st.get("reachable") else "Offline"
            peer_box.insert(tk.END, f"{p['url']}  [{status_txt} - {st.get('mode', 'replica')}]")

    def add_peer():
        try:
            d = parse_code(v_peer_input.get())
            if not d.get("u"): raise ValueError("No URL found in code")
            peers = cfg.setdefault("peers", [])
            if not any(p["url"].rstrip("/") == d["u"].rstrip("/") for p in peers):
                peers.append({"url": d["u"], "admin_key": d.get("a", ""), "view_key": d.get("v", "")})
                save_config(cfg)
                refresh_peer_box()
                v_peer_input.set("")
                threading.Thread(target=REPLICATOR.tick, daemon=True).start()
                messagebox.showinfo(APP_NAME, f"Added peer: {d['u']}")
        except Exception as e:
            messagebox.showerror(APP_NAME, f"Invalid code: {e}")

    def make_backup_of_code():
        try:
            d = parse_code(v_peer_input.get())
            if not d.get("u"): raise ValueError("No URL in code")
            cfg["admin_key"] = d.get("a", cfg["admin_key"])
            cfg["view_key"] = d.get("v", cfg["view_key"])
            peers = cfg.setdefault("peers", [])
            if not any(p["url"].rstrip("/") == d["u"].rstrip("/") for p in peers):
                peers.append({"url": d["u"], "admin_key": d.get("a", ""), "view_key": d.get("v", "")})
            step_down_to_replica(d["u"], d.get("t", 1))
            save_config(cfg)
            v_admin.set(cfg["admin_key"])
            v_view.set(cfg["view_key"])
            refresh_peer_box()
            threading.Thread(target=REPLICATOR.tick, daemon=True).start()
            messagebox.showinfo(APP_NAME, "Configured as BACKUP replica.")
        except Exception as e:
            messagebox.showerror(APP_NAME, f"Could not configure backup: {e}")

    def remove_peer():
        sel = peer_box.curselection()
        if not sel: return
        del cfg["peers"][sel[0]]
        save_config(cfg)
        refresh_peer_box()

    f_pbtns = tk.Frame(root)
    f_pbtns.pack(anchor="w", padx=12, pady=2)
    tk.Button(f_pbtns, text="Add Peer", command=add_peer).pack(side="left")
    tk.Button(f_pbtns, text="Make this a Backup of it", command=make_backup_of_code).pack(side="left", padx=6)
    tk.Button(f_pbtns, text="Promote this to Primary", command=lambda: promote_to_primary()).pack(side="left", padx=6)
    tk.Button(f_pbtns, text="Remove Selected", command=remove_peer).pack(side="left", padx=6)

    bar = tk.Frame(root)
    bar.pack(anchor="w", padx=12, pady=8)
    tk.Button(bar, text="Backup Now", command=lambda: threading.Thread(target=backup_db, daemon=True).start()).pack(side="left")
    tk.Button(bar, text="Workflows Monitor", bg="#1769aa", fg="white", command=lambda: webbrowser.open(f"http://localhost:{server.port}/workflows")).pack(side="left", padx=6)
    tk.Button(bar, text="Exit", width=10, command=root.destroy).pack(side="left", padx=6)

    tk.Label(root, text="Activity Log:", font=("Segoe UI", 9, "bold")).pack(anchor="w", padx=12, pady=(6, 0))
    txt = tk.Text(root, height=7, state="disabled", wrap="word", font=("Consolas", 9))
    txt.pack(fill="both", expand=True, padx=12, pady=(0, 10))

    def toggle_server():
        if server.running:
            tunnel.stop()
            server.stop()
        else:
            try:
                p = int(v_port.get().strip())
                server.start(p)
                cfg["port"] = p
                save_config(cfg)
            except Exception as e:
                messagebox.showerror(APP_NAME, str(e))

    def toggle_tunnel():
        if tunnel.running:
            tunnel.stop()
        else:
            if not server.running:
                return messagebox.showwarning(APP_NAME, "Start server first.")
            tunnel.start(server.port)

    btn_srv.configure(command=toggle_server)
    btn_tun.configure(command=toggle_tunnel)

    last_log_len = 0
    def refresh_ui():
        nonlocal last_log_len
        running = server.running
        v_status.set("Server running" if running else "Server stopped")
        lbl_status.configure(fg="#218c5a" if running else "#bd3b3b")
        btn_srv.configure(text="Stop Server" if running else "Start Server")
        p = server.port or v_port.get()
        v_local.set(f"http://{lan_addresses()[0]}:{p}" if running else "")
        v_public.set(tunnel.url or ("Starting..." if tunnel.running else ""))
        btn_tun.configure(text="Stop Public Link" if tunnel.running else "Start Public Link")
        
        base_url = v_custom.get().strip() or tunnel.url or v_local.get()
        v_code.set(make_code(base_url, cfg["view_key"], cfg["admin_key"]) if base_url else "")

        mode = cfg.get("mode", "primary").upper()
        term = cfg.get("term", 1)
        v_role.set(f"[{mode} T{term}] - {REPLICATOR.status}")
        lbl_role.configure(fg="#16a34a" if mode == "PRIMARY" else "#d97706")

        lines = list(_log)
        if len(lines) != last_log_len:
            last_log_len = len(lines)
            txt.configure(state="normal")
            txt.delete("1.0", "end")
            txt.insert("end", "\n".join(lines[-70:]))
            txt.see("end")
            txt.configure(state="disabled")

        root.after(1000, refresh_ui)

    refresh_peer_box()
    refresh_ui()
    root.mainloop()

def main():
    cfg = load_config()
    server = ServerCore()
    tunnel = Tunnel()
    server.start(cfg["port"])
    run_app(server, tunnel, cfg)

if __name__ == "__main__":
    main()