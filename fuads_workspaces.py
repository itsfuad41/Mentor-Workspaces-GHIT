"""Several projects on ONE Fuad's Server: one app, one tunnel address, one SQL console.

Each project ("workspace") is either
  * a NEW DATABASE: its own SQLite file in <data>/workspaces/<name>.db with its own admin/view keys and a small
    table API at /w/<name>/..., or
  * an EXISTING APP already running on this PC (a port): /w/<name>/... is forwarded to it unchanged.
The original Mentor Workstation database stays exactly where it was and is also available in the console as "mentor"."""
import http.client
import json
import os
import re
import secrets
import shutil
import sqlite3
import time
import urllib.parse
from datetime import datetime

import fuads_server_core as core

WS_DIR = os.path.join(core.DATA_DIR, "workspaces")
BACKUP_DIR = os.path.join(WS_DIR, "_backups")
RESERVED = {"mentor", "w", "admin", "api", "health", "workflows", "ping", "projects", "snapshots", "index"}
SLUG_RE = re.compile(r"^[a-z0-9][a-z0-9-]{1,30}[a-z0-9]$")
MAX_ROWS = 500
SQL_SECONDS = 8
HOP = {"connection", "keep-alive", "proxy-authenticate", "proxy-authorization", "te", "trailers", "transfer-encoding", "upgrade", "host", "content-length"}


class WsError(Exception):
    def __init__(self, message, status=400):
        super().__init__(message)
        self.status = status


def slugify(name):
    s = re.sub(r"[^a-z0-9]+", "-", str(name or "").lower()).strip("-")[:32].strip("-")
    return s


def _registry(cfg=None):
    cfg = cfg or core.load_config()
    return cfg.get("workspaces") or []


def get(slug, cfg=None):
    for w in _registry(cfg):
        if w["slug"] == slug:
            return w
    return None


def db_path(slug):
    if slug == "mentor":
        return core.DB_PATH
    w = get(slug)
    if not w or w.get("type") != "database":
        raise WsError("That project has no database.", 404)
    return os.path.join(WS_DIR, slug + ".db")


def list_all(cfg=None):
    cfg = cfg or core.load_config()
    out = [{"slug": "mentor", "name": "Mentor Workstation", "type": "database", "builtin": True,
            "db_size": os.path.getsize(core.DB_PATH) if os.path.exists(core.DB_PATH) else 0}]
    for w in _registry(cfg):
        item = dict(w, builtin=False)
        if w.get("type") == "database":
            p = os.path.join(WS_DIR, w["slug"] + ".db")
            item["db_size"] = os.path.getsize(p) if os.path.exists(p) else 0
        out.append(item)
    return out


def create(name, kind, port=None):
    name = str(name or "").strip()
    slug = slugify(name)
    if not name or not SLUG_RE.match(slug) or slug in RESERVED:
        raise WsError("Give the project a name of 3-32 letters or numbers (for example: couple-secrets).")
    cfg = core.load_config()
    if get(slug, cfg):
        raise WsError("There is already a project with that name.")
    entry = {"slug": slug, "name": name, "type": kind, "created": datetime.now().strftime("%Y-%m-%d %H:%M")}
    if kind == "database":
        os.makedirs(WS_DIR, exist_ok=True)
        conn = sqlite3.connect(os.path.join(WS_DIR, slug + ".db"))
        conn.execute("PRAGMA journal_mode=WAL")
        conn.commit()
        conn.close()
        entry.update(admin_key=secrets.token_urlsafe(14), view_key=secrets.token_urlsafe(14))
    elif kind == "app":
        try:
            port = int(port)
        except (TypeError, ValueError):
            raise WsError("Enter the port the app runs on, like 3000.")
        if not 1 <= port <= 65535 or port == int(cfg.get("port") or core.DEFAULT_PORT):
            raise WsError("That port is not usable (it must differ from this server's own port).")
        entry.update(type="app", target_port=port)
    else:
        raise WsError("Unknown project type.")
    cfg.setdefault("workspaces", []).append(entry)
    if not core.save_config(cfg):
        raise WsError("Could not save the settings.")
    return entry


def delete(slug):
    cfg = core.load_config()
    w = get(slug, cfg)
    if not w:
        raise WsError("No such project.", 404)
    if w.get("type") == "database":
        src = os.path.join(WS_DIR, slug + ".db")
        if os.path.exists(src):
            gone = os.path.join(WS_DIR, "_deleted")
            os.makedirs(gone, exist_ok=True)
            shutil.move(src, os.path.join(gone, f"{slug}_{datetime.now().strftime('%Y%m%d_%H%M%S')}.db"))
    cfg["workspaces"] = [x for x in _registry(cfg) if x["slug"] != slug]
    core.save_config(cfg)
    return {"ok": True}


# ---------------- SQL console ----------------
def backup(slug):
    """Copy of the database taken before any change from the console. Keeps the newest 15 per project."""
    os.makedirs(BACKUP_DIR, exist_ok=True)
    dest = os.path.join(BACKUP_DIR, f"{slug}_{datetime.now().strftime('%Y-%m-%d_%H-%M-%S')}.db")
    src = sqlite3.connect(db_path(slug))
    dst = sqlite3.connect(dest)
    try:
        src.backup(dst)
    finally:
        dst.close()
        src.close()
    old = sorted(f for f in os.listdir(BACKUP_DIR) if f.startswith(slug + "_"))
    for f in old[:-15]:
        try:
            os.remove(os.path.join(BACKUP_DIR, f))
        except OSError:
            pass
    return dest


def _cell(v):
    if isinstance(v, (bytes, bytearray)):
        return f"<binary {len(v)} bytes>"
    if isinstance(v, str) and len(v) > 2000:
        return v[:2000] + "…"
    return v


def _statements(sql):
    """Split a script into single statements (semicolons inside quotes/triggers are handled by sqlite3)."""
    out, buf = [], ""
    for ch in sql:
        buf += ch
        if ch == ";" and sqlite3.complete_statement(buf):
            out.append(buf.strip())
            buf = ""
    if buf.strip():
        out.append(buf.strip())
    return [x for x in out if x.strip(";").strip()]


def run_sql(slug, sql, write=False):
    sql = str(sql or "").strip()
    if not sql:
        raise WsError("Type an SQL command first.")
    path = db_path(slug)
    backed = ""
    if write:
        backed = os.path.basename(backup(slug))
        conn = sqlite3.connect(path, timeout=10)
    else:
        conn = sqlite3.connect(f"file:{path}?mode=ro", uri=True, timeout=10)
    started = time.time()
    conn.set_progress_handler(lambda: time.time() - started > SQL_SECONDS, 20000)
    try:
        columns, rows, shown = [], [], 0
        if write:
            for stmt in _statements(sql):
                cur = conn.execute(stmt)
                if cur.description:
                    columns = [d[0] for d in cur.description]
                    rows = [[_cell(c) for c in r] for r in cur.fetchmany(MAX_ROWS + 1)]
            conn.commit()
        else:
            cur = conn.execute(sql)
            if cur.description:
                columns = [d[0] for d in cur.description]
                rows = [[_cell(c) for c in r] for r in cur.fetchmany(MAX_ROWS + 1)]
        truncated = len(rows) > MAX_ROWS
        rows = rows[:MAX_ROWS]
        return {"columns": columns, "rows": rows, "truncated": truncated, "changes": conn.total_changes if write else 0,
                "ms": int((time.time() - started) * 1000), "backup": backed}
    except sqlite3.OperationalError as e:
        msg = str(e)
        if "readonly" in msg.lower():
            msg = "This command changes data. Tick \"Allow changes\" (a backup copy is made first) and run it again."
        elif "interrupted" in msg.lower():
            msg = f"The command took longer than {SQL_SECONDS} seconds and was stopped."
        if write:
            conn.rollback()
        raise WsError(msg)
    except (sqlite3.Error, sqlite3.Warning) as e:
        if write:
            conn.rollback()
        raise WsError(str(e))
    finally:
        conn.close()


def tables(slug):
    path = db_path(slug)
    conn = sqlite3.connect(f"file:{path}?mode=ro", uri=True, timeout=10)
    try:
        out = []
        for name, typ in conn.execute("SELECT name, type FROM sqlite_master WHERE type IN ('table','view') AND name NOT LIKE 'sqlite_%' ORDER BY name"):
            q = '"' + name.replace('"', '""') + '"'
            try:
                n = conn.execute(f"SELECT COUNT(*) FROM {q}").fetchone()[0] if typ == "table" else None
            except sqlite3.Error:
                n = None
            cols = [r[1] for r in conn.execute(f"PRAGMA table_info({q})")]
            out.append({"name": name, "type": typ, "rows": n, "columns": cols})
        return out
    finally:
        conn.close()


# ---------------- public API used by the websites: /w/<project>/... ----------------
def _auth(handler, w):
    k = (handler.headers.get("X-Key") or "").strip()
    a = (handler.headers.get("Authorization") or "").strip().replace("Bearer ", "")
    token = a or k
    if token and secrets.compare_digest(token, w.get("admin_key", "")):
        return "admin"
    if token and secrets.compare_digest(token, w.get("view_key", "")):
        return "user"
    return None


def _q(name):
    return '"' + name.replace('"', '""') + '"'


def _read_json(handler):
    n = int(handler.headers.get("Content-Length") or 0)
    if n > 5_000_000:
        raise WsError("Request too large.", 413)
    try:
        return json.loads(handler.rfile.read(n) or b"{}")
    except ValueError:
        raise WsError("The request body must be JSON.")


def handle(handler, method, path, query):
    """Entry point from the server for every /w/... request."""
    parts = [urllib.parse.unquote(p) for p in path.split("/") if p]
    try:
        if len(parts) < 2:
            return handler._send(200, {"projects": [{"name": w["name"], "path": "/w/" + w["slug"]} for w in _registry()]})
        slug = parts[1]
        w = get(slug)
        if not w:
            return handler._send(404, {"error": "No such project."})
        if w.get("type") == "app":
            return _proxy(handler, method, w, path[len("/w/" + slug):] or "/", query)
        rest = parts[2:]
        if not rest or rest == ["health"]:
            return handler._send(200, {"status": "ok", "project": w["name"], "type": "database"})
        role = _auth(handler, w)
        if not role:
            return handler._send(401, {"error": "Unauthorized: send this project's key in the X-Key header."})
        who = (handler.headers.get("X-Who") or role).strip()[:80]
        if rest == ["tables"] and method == "GET":
            return handler._send(200, {"tables": tables(slug)})
        if rest[0] == "t" and len(rest) >= 2:
            return _table_api(handler, method, w, role, who, rest[1], rest[2] if len(rest) > 2 else None, query)
        return handler._send(404, {"error": "Unknown path."})
    except WsError as e:
        return handler._send(e.status, {"error": str(e)})
    except sqlite3.Error as e:
        return handler._send(400, {"error": str(e)})


def _table_api(handler, method, w, role, who, table, rowid, query):
    slug = w["slug"]
    conn = sqlite3.connect(os.path.join(WS_DIR, slug + ".db"), timeout=10)
    conn.row_factory = sqlite3.Row
    try:
        known = {r[0] for r in conn.execute("SELECT name FROM sqlite_master WHERE type='table' AND name NOT LIKE 'sqlite_%'")}
        if table not in known:
            raise WsError("No such table. Create it in the SQL console first.", 404)
        cols = [r[1] for r in conn.execute(f"PRAGMA table_info({_q(table)})")]
        t = _q(table)
        if method == "GET":
            qs = urllib.parse.parse_qs(query)
            where, args = [], []
            for key, vals in qs.items():
                if key in ("limit", "offset", "order"):
                    continue
                if key not in cols:
                    raise WsError(f"Unknown column: {key}")
                where.append(f"{_q(key)} = ?")
                args.append(vals[0])
            if rowid is not None:
                where.append("rowid = ?")
                args.append(rowid)
            order = ""
            if qs.get("order"):
                col = qs["order"][0]
                desc = col.startswith("-")
                col = col.lstrip("-")
                if col not in cols and col != "rowid":
                    raise WsError(f"Unknown column: {col}")
                order = f" ORDER BY {_q(col) if col != 'rowid' else 'rowid'}{' DESC' if desc else ''}"
            limit = max(1, min(1000, int((qs.get("limit") or ["100"])[0])))
            offset = max(0, int((qs.get("offset") or ["0"])[0]))
            sql = f"SELECT rowid AS _rowid, * FROM {t}" + (" WHERE " + " AND ".join(where) if where else "") + order + " LIMIT ? OFFSET ?"
            rows = [dict(r) for r in conn.execute(sql, args + [limit, offset])]
            return handler._send(200, {"rows": rows, "count": len(rows)})
        if role != "admin":
            raise WsError("This key can only read. Use the admin key to change data.", 403)
        if method == "POST":
            body = _read_json(handler)
            items = body.get("rows") if isinstance(body, dict) and isinstance(body.get("rows"), list) else [body]
            last = None
            for item in items:
                if not isinstance(item, dict) or not item:
                    raise WsError("Send a JSON object (or {\"rows\": [...]}) with column values.")
                bad = [k for k in item if k not in cols]
                if bad:
                    raise WsError("Unknown column: " + bad[0])
                last = conn.execute(f"INSERT INTO {t} ({', '.join(_q(k) for k in item)}) VALUES ({', '.join('?' for _ in item)})",
                                    [json.dumps(v) if isinstance(v, (dict, list)) else v for v in item.values()]).lastrowid
            conn.commit()
            core.ACTIVITY.record(who, "POST", f"/w/{slug}/t/{table}", 200, f"{len(items)} row(s) added")
            return handler._send(200, {"inserted": len(items), "last_rowid": last})
        if method in ("PATCH", "PUT") and rowid is not None:
            body = _read_json(handler)
            bad = [k for k in body if k not in cols]
            if not body or bad:
                raise WsError("Unknown column: " + bad[0] if bad else "Send the columns to change.")
            cur = conn.execute(f"UPDATE {t} SET {', '.join(_q(k) + ' = ?' for k in body)} WHERE rowid = ?",
                               [json.dumps(v) if isinstance(v, (dict, list)) else v for v in body.values()] + [rowid])
            conn.commit()
            core.ACTIVITY.record(who, "PATCH", f"/w/{slug}/t/{table}/{rowid}", 200, "row updated")
            return handler._send(200, {"updated": cur.rowcount})
        if method == "DELETE" and rowid is not None:
            cur = conn.execute(f"DELETE FROM {t} WHERE rowid = ?", (rowid,))
            conn.commit()
            core.ACTIVITY.record(who, "DELETE", f"/w/{slug}/t/{table}/{rowid}", 200, "row deleted")
            return handler._send(200, {"deleted": cur.rowcount})
        raise WsError("Unsupported request.", 405)
    finally:
        conn.close()


def _proxy(handler, method, w, rest, query):
    """Forward to an app already running on this PC; the app keeps its own login and rules."""
    port = int(w["target_port"])
    n = int(handler.headers.get("Content-Length") or 0)
    body = handler.rfile.read(n) if n else None
    headers = {k: v for k, v in handler.headers.items() if k.lower() not in HOP}
    headers["X-Forwarded-Prefix"] = "/w/" + w["slug"]
    try:
        conn = http.client.HTTPConnection("127.0.0.1", port, timeout=60)
        conn.request(method, rest + ("?" + query if query else ""), body=body, headers=headers)
        resp = conn.getresponse()
        data = resp.read()
    except (OSError, http.client.HTTPException):
        return handler._send(502, {"error": f"The app for \"{w['name']}\" is not running on port {port}. Start it on this PC and try again."})
    handler.send_response(resp.status)
    seen_cors = False
    for k, v in resp.getheaders():
        if k.lower() in HOP:
            continue
        seen_cors |= k.lower() == "access-control-allow-origin"
        handler.send_header(k, v)
    if not seen_cors:
        handler._cors()
    handler.send_header("Content-Length", str(len(data)))
    handler.end_headers()
    handler.wfile.write(data)
    conn.close()


# ---------------- table editor (Supabase-style grid) ----------------
_last_edit_backup = {}


def _edit_backup(slug):
    """One safety copy per project at most every 5 minutes of editing."""
    if time.time() - _last_edit_backup.get(slug, 0) > 300:
        backup(slug)
        _last_edit_backup[slug] = time.time()


def _open(slug, write=False):
    path = db_path(slug)
    if write:
        _edit_backup(slug)
        return sqlite3.connect(path, timeout=10)
    return sqlite3.connect(f"file:{path}?mode=ro", uri=True, timeout=10)


def _check_table(conn, table):
    known = {r[0] for r in conn.execute("SELECT name FROM sqlite_master WHERE type='table' AND name NOT LIKE 'sqlite_%'")}
    if table not in known:
        raise WsError("No such table.", 404)
    return [dict(zip(("cid", "name", "type", "notnull", "default", "pk"), r)) for r in conn.execute(f"PRAGMA table_info({_q(table)})")]


def rows(slug, table, limit=50, offset=0, search=""):
    conn = _open(slug)
    try:
        cols = _check_table(conn, table)
        where, args = "", []
        if search:
            where = " WHERE " + " OR ".join(f"CAST({_q(c['name'])} AS TEXT) LIKE ?" for c in cols)
            args = [f"%{search}%"] * len(cols)
        total = conn.execute(f"SELECT COUNT(*) FROM {_q(table)}{where}", args).fetchone()[0]
        data = [[_cell(v) for v in r] for r in conn.execute(
            f"SELECT rowid, * FROM {_q(table)}{where} ORDER BY rowid LIMIT ? OFFSET ?", args + [max(1, min(500, int(limit))), max(0, int(offset))])]
        return {"columns": cols, "rows": data, "total": total}
    finally:
        conn.close()


def _val(v):
    return json.dumps(v) if isinstance(v, (dict, list)) else v


def row_save(slug, table, rowid, values):
    """rowid None = insert, otherwise update that row."""
    conn = _open(slug, True)
    try:
        cols = [c["name"] for c in _check_table(conn, table)]
        bad = [k for k in values if k not in cols]
        if bad:
            raise WsError("Unknown column: " + bad[0])
        if rowid is None:
            if values:
                conn.execute(f"INSERT INTO {_q(table)} ({', '.join(_q(k) for k in values)}) VALUES ({', '.join('?' for _ in values)})", [_val(v) for v in values.values()])
            else:
                conn.execute(f"INSERT INTO {_q(table)} DEFAULT VALUES")
        else:
            if not values:
                raise WsError("Nothing to change.")
            conn.execute(f"UPDATE {_q(table)} SET {', '.join(_q(k) + ' = ?' for k in values)} WHERE rowid = ?", [_val(v) for v in values.values()] + [int(rowid)])
        conn.commit()
        return {"ok": True}
    except sqlite3.Error as e:
        raise WsError(str(e))
    finally:
        conn.close()


def row_delete(slug, table, rowid):
    conn = _open(slug, True)
    try:
        _check_table(conn, table)
        conn.execute(f"DELETE FROM {_q(table)} WHERE rowid = ?", (int(rowid),))
        conn.commit()
        return {"ok": True}
    finally:
        conn.close()


TYPES = {"text": "TEXT", "number": "REAL", "integer": "INTEGER", "true/false": "INTEGER", "date/time": "TEXT", "json": "TEXT"}


def table_create(slug, table, columns):
    """columns: [{name, type}] - an auto-numbered id and a created_at column are added for you."""
    table = str(table or "").strip()
    if not re.match(r"^[A-Za-z_][A-Za-z0-9_]{0,40}$", table) or table.lower().startswith("sqlite_"):
        raise WsError("Table name: letters, numbers and _ only (for example: messages).")
    defs = ["id INTEGER PRIMARY KEY AUTOINCREMENT"]
    for c in columns or []:
        n = str(c.get("name") or "").strip()
        if not n:
            continue
        if not re.match(r"^[A-Za-z_][A-Za-z0-9_]{0,40}$", n) or n.lower() in ("id", "created_at"):
            raise WsError(f"Column name not allowed: {n}")
        defs.append(f"{_q(n)} {TYPES.get(str(c.get('type')).lower(), 'TEXT')}")
    defs.append("created_at TEXT DEFAULT (datetime('now'))")
    conn = _open(slug, True)
    try:
        conn.execute(f"CREATE TABLE {_q(table)} ({', '.join(defs)})")
        conn.commit()
        return {"ok": True}
    except sqlite3.Error as e:
        raise WsError(str(e))
    finally:
        conn.close()


def table_drop(slug, table):
    backup(slug)
    conn = sqlite3.connect(db_path(slug), timeout=10)
    try:
        _check_table(conn, table)
        conn.execute(f"DROP TABLE {_q(table)}")
        conn.commit()
        return {"ok": True}
    finally:
        conn.close()
