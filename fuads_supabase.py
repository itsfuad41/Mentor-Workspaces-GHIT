"""Manage the online Supabase database from Fuad's Server: every table as an editable sheet, Excel/CSV import, backups.
Talks to Supabase's REST API with the project's SERVICE key. The key is stored only in this PC's config file and is
never sent to a browser or website."""
import base64
import csv
import io
import json
import os
import re
import time
import urllib.error
import urllib.parse
import urllib.request
import zipfile
from datetime import datetime
from xml.etree import ElementTree as ET

import fuads_server_core as core

SB_DIR = os.path.join(core.DATA_DIR, "supabase")
HIDDEN = {"password"}                       # never sent to the panel; typing a new value replaces it
MASK = "••••••"
BATCH = 500


class SbError(Exception):
    def __init__(self, message, status=400):
        super().__init__(message)
        self.status = status


def _cfg():
    c = core.load_config()
    url = str(c.get("sb_url") or "").strip().rstrip("/")
    key = str(c.get("sb_key") or "").strip()
    if not url or not key:
        raise SbError("Connect Supabase first: paste the project URL and the service key.", 409)
    return url, key


def save_connection(url, key):
    cfg = core.load_config()
    url = str(url or "").strip().rstrip("/")
    key = cfg.get("sb_key", "") if key is None or str(key).strip() == "" else str(key).strip()
    if url and not re.match(r"^(https://[a-z0-9-]+\.supabase\.(co|in)|http://127\.0\.0\.1:\d+)$", url):
        raise SbError("The URL should look like https://xxxxxxxx.supabase.co")
    cfg["sb_url"], cfg["sb_key"] = url, key
    if not core.save_config(cfg):
        raise SbError("Could not save the settings.")
    if url and key:
        tables()                                   # proves the key works
    return {"ok": True}


def disconnect():
    cfg = core.load_config()
    cfg["sb_url"] = cfg["sb_key"] = ""
    core.save_config(cfg)
    return {"ok": True}


def connection():
    c = core.load_config()
    return {"url": c.get("sb_url", ""), "key_saved": bool(c.get("sb_key"))}


def _req(method, path, body=None, headers=None, raw=False):
    url, key = _cfg()
    h = {"apikey": key, "Authorization": "Bearer " + key, "Content-Type": "application/json", "Accept": "application/json"}
    h.update(headers or {})
    data = None if body is None else (body if isinstance(body, bytes) else json.dumps(body).encode())
    req = urllib.request.Request(url + "/rest/v1" + path, data=data, headers=h, method=method)
    try:
        with urllib.request.urlopen(req, timeout=40) as r:
            payload = r.read()
            return (r.status, dict(r.headers), payload) if raw else (json.loads(payload) if payload else None)
    except urllib.error.HTTPError as e:
        text = e.read().decode("utf-8", "replace")
        try:
            msg = json.loads(text).get("message") or text
        except ValueError:
            msg = text
        if e.code in (401, 403):
            msg = "Supabase refused the key. Use the service_role key (Project Settings > API). " + msg[:120]
        raise SbError(msg[:400], 400)
    except (urllib.error.URLError, OSError) as e:
        raise SbError("Could not reach Supabase: " + str(getattr(e, "reason", e)), 502)


def _q(v):
    return urllib.parse.quote(str(v), safe="")


# ---------------- schema ----------------
_schema = {"t": 0, "v": None}


def schema(force=False):
    if not force and _schema["v"] and time.time() - _schema["t"] < 60:
        return _schema["v"]
    spec = _req("GET", "/", headers={"Accept": "application/openapi+json"})
    out = {}
    for name, d in (spec.get("definitions") or {}).items():
        cols = []
        for cname, c in (d.get("properties") or {}).items():
            cols.append({"name": cname, "type": c.get("format") or c.get("type") or "text",
                         "pk": "<pk" in str(c.get("description") or ""), "default": c.get("default")})
        out[name] = cols
    _schema.update(t=time.time(), v=out)
    return out


def _table(table):
    s = schema()
    if table not in s:
        s = schema(True)
    if table not in s:
        raise SbError("No such table.", 404)
    cols = s[table]
    pks = [c["name"] for c in cols if c["pk"]]
    if not pks:
        raise SbError("This table has no primary key, so it can only be viewed in the SQL editor.")
    return cols, pks


def tables():
    out = []
    for name, cols in sorted(schema(True).items()):
        try:
            _, hdr, _ = _req("GET", f"/{_q(name)}?select=*&limit=1", headers={"Prefer": "count=exact"}, raw=True)
            n = int(str(hdr.get("Content-Range") or hdr.get("content-range") or "*/0").split("/")[-1])
        except (SbError, ValueError):
            n = None
        out.append({"name": name, "rows": n, "columns": [c["name"] for c in cols]})
    return out


# ---------------- rows ----------------
def rows(table, limit=100, offset=0, search=""):
    cols, pks = _table(table)
    limit, offset = max(1, min(500, int(limit))), max(0, int(offset))
    path = f"/{_q(table)}?select=*&order={','.join(_q(p) + '.asc' for p in pks)}&limit={limit}&offset={offset}"
    search = str(search or "").strip()
    if search:
        likes = ",".join(f"{c['name']}.ilike.*{re.sub(r'[,()*]', ' ', search)}*" for c in cols
                         if c["type"] in ("text", "character varying") and c["name"] not in HIDDEN)
        if likes:
            path += "&or=(" + _q(likes).replace("%2C", ",").replace("%2A", "*").replace("%2E", ".") + ")"
    status, hdr, payload = _req("GET", path, headers={"Prefer": "count=exact"}, raw=True)
    data = json.loads(payload or b"[]")
    total = int(str(hdr.get("Content-Range") or hdr.get("content-range") or "*/0").split("/")[-1] or 0)
    for r in data:
        for h in HIDDEN:
            if h in r and r[h] not in (None, ""):
                r[h] = MASK
    return {"columns": cols, "pks": pks, "rows": data, "total": total}


def _pk_filter(pks, key):
    if not isinstance(key, dict) or set(key) != set(pks):
        raise SbError("Row identity is missing.")
    return "&".join(f"{_q(p)}=eq.{_q(key[p])}" for p in pks)


def _audit(op, table, before, after):
    try:
        os.makedirs(SB_DIR, exist_ok=True)
        with open(os.path.join(SB_DIR, "changes.log"), "a", encoding="utf-8") as f:
            f.write(json.dumps({"t": datetime.now().strftime("%Y-%m-%d %H:%M:%S"), "op": op, "table": table, "before": before, "after": after}, ensure_ascii=False) + "\n")
    except OSError:
        pass


def _clean(cols, values):
    names = {c["name"]: c for c in cols}
    out = {}
    for k, v in values.items():
        if k not in names:
            raise SbError("Unknown column: " + k)
        if k in HIDDEN and v in (MASK, None):
            continue
        t = names[k]["type"]
        if v == "" and t not in ("text", "character varying"):
            v = None
        elif v is not None and t in ("integer", "bigint", "smallint", "numeric", "double precision", "real"):
            try:
                v = float(v) if t in ("numeric", "double precision", "real") else int(float(v))
            except (TypeError, ValueError):
                raise SbError(f"{k} must be a number.")
        elif v is not None and t == "boolean" and isinstance(v, str):
            v = v.strip().lower() in ("true", "t", "1", "yes", "y")
        out[k] = v
    return out


def row_save(table, key, values):
    cols, pks = _table(table)
    vals = _clean(cols, values or {})
    if key is None:
        got = _req("POST", f"/{_q(table)}", vals or {}, {"Prefer": "return=representation"})
        _audit("insert", table, None, got)
        return {"ok": True, "row": got[0] if got else None}
    if not vals:
        return {"ok": True}
    f = _pk_filter(pks, key)
    before = _req("GET", f"/{_q(table)}?select=*&{f}")
    got = _req("PATCH", f"/{_q(table)}?{f}", vals, {"Prefer": "return=representation"})
    _audit("update", table, before, got)
    return {"ok": True, "row": got[0] if got else None}


def row_delete(table, key):
    _, pks = _table(table)
    f = _pk_filter(pks, key)
    before = _req("GET", f"/{_q(table)}?select=*&{f}")
    _req("DELETE", f"/{_q(table)}?{f}")
    _audit("delete", table, before, None)
    return {"ok": True}


def _all(table, pks=None):
    cols, pk = _table(table)
    out, off = [], 0
    while True:
        part = _req("GET", f"/{_q(table)}?select=*&order={','.join(_q(p) + '.asc' for p in pk)}&limit=1000&offset={off}")
        out += part
        if len(part) < 1000:
            return out
        off += 1000


def backup(tables_=None):
    """Full JSON copy of every table on this PC."""
    stamp = datetime.now().strftime("%Y-%m-%d_%H-%M-%S")
    d = os.path.join(SB_DIR, "backups", stamp)
    os.makedirs(d, exist_ok=True)
    total = 0
    for t in tables_ or list(schema(True)):
        data = _all(t)
        with open(os.path.join(d, t + ".json"), "w", encoding="utf-8") as f:
            json.dump(data, f, ensure_ascii=False)
        total += len(data)
    keep = sorted(os.listdir(os.path.dirname(d)))
    for old in keep[:-10]:
        import shutil
        shutil.rmtree(os.path.join(os.path.dirname(d), old), ignore_errors=True)
    return {"ok": True, "folder": d, "rows": total}


def export_csv(table):
    cols, pks = _table(table)
    names = [c["name"] for c in cols if c["name"] not in HIDDEN]
    buf = io.StringIO()
    w = csv.writer(buf)
    w.writerow(names)
    for r in _all(table):
        w.writerow(["" if r.get(n) is None else (json.dumps(r[n]) if isinstance(r[n], (dict, list)) else r[n]) for n in names])
    return "﻿" + buf.getvalue()


# ---------------- Excel / CSV import ----------------
def _col_index(ref):
    n = 0
    for ch in re.match(r"[A-Z]+", ref).group(0):
        n = n * 26 + ord(ch) - 64
    return n - 1


def read_xlsx(data):
    """First sheet of an .xlsx as a list of rows (no extra libraries needed)."""
    try:
        z = zipfile.ZipFile(io.BytesIO(data))
    except zipfile.BadZipFile:
        raise SbError("That is not a valid .xlsx file.")
    ns = {"m": "http://schemas.openxmlformats.org/spreadsheetml/2006/main"}
    shared = []
    if "xl/sharedStrings.xml" in z.namelist():
        for si in ET.fromstring(z.read("xl/sharedStrings.xml")).findall("m:si", ns):
            shared.append("".join(t.text or "" for t in si.iter("{%s}t" % ns["m"])))
    sheets = sorted(n for n in z.namelist() if re.match(r"xl/worksheets/sheet\d+\.xml$", n))
    if not sheets:
        raise SbError("The workbook has no sheets.")
    out = []
    for row in ET.fromstring(z.read(sheets[0])).iter("{%s}row" % ns["m"]):
        cells = {}
        for c in row.findall("m:c", ns):
            t, v = c.get("t"), c.find("m:v", ns)
            if t == "inlineStr":
                val = "".join(x.text or "" for x in c.iter("{%s}t" % ns["m"]))
            elif v is None or v.text is None:
                continue
            elif t == "s":
                val = shared[int(v.text)]
            elif t == "b":
                val = v.text == "1"
            elif t in ("str", "e"):
                val = v.text
            else:
                try:
                    f = float(v.text)
                    val = int(f) if f == int(f) and abs(f) < 1e15 else f
                except ValueError:
                    val = v.text
            cells[_col_index(c.get("r"))] = val
        if cells:
            line = [cells.get(i) for i in range(max(cells) + 1)]
            out.append(line)
    return out


def _norm(s):
    return re.sub(r"[^a-z0-9]", "", str(s or "").lower())


def _parse(table, filename, b64):
    cols, pks = _table(table)
    try:
        data = base64.b64decode(b64 or "")
    except ValueError:
        raise SbError("The file could not be read.")
    if str(filename).lower().endswith((".csv", ".txt")):
        text = data.decode("utf-8-sig", "replace")
        grid = [r for r in csv.reader(io.StringIO(text)) if any(c.strip() for c in r)]
    else:
        grid = read_xlsx(data)
    if len(grid) < 2:
        raise SbError("The sheet needs a header row and at least one data row.")
    header = [str(h or "").strip() for h in grid[0]]
    by_norm = {_norm(c["name"]): c["name"] for c in cols}
    aliases = {"id": "op_id", "operatorid": "op_id", "opid": "op_id", "name": "full_name", "operatorname": "full_name", "mentor": "mentor_name"}
    mapping = {}
    for i, h in enumerate(header):
        n = _norm(h)
        target = by_norm.get(n) or (aliases.get(n) if aliases.get(n) in by_norm.values() else None)
        if target and target not in mapping.values():
            mapping[i] = target
    missing = [p for p in pks if p not in mapping.values()]
    if missing:
        raise SbError("The sheet needs a column for: " + ", ".join(missing) + ". Columns found: " + ", ".join(h for h in header if h))
    recs, skipped = [], 0
    for line in grid[1:]:
        rec = {}
        for i, name in mapping.items():
            v = line[i] if i < len(line) else None
            if isinstance(v, str):
                v = v.strip()
            rec[name] = None if v in ("", None) else v
        if all(rec.get(p) in (None, "") for p in pks):
            skipped += 1
            continue
        # keep to plain text for text columns (Excel turns "00123" into a number)
        for c in cols:
            if c["name"] in rec and rec[c["name"]] is not None and c["type"] in ("text", "character varying") and not isinstance(rec[c["name"]], str):
                rec[c["name"]] = str(rec[c["name"]])
        recs.append(rec)
    seen, dup = {}, 0
    for r in recs:
        k = tuple(str(r[p]) for p in pks)
        dup += k in seen
        seen[k] = r
    return cols, pks, header, mapping, list(seen.values()), skipped, dup


def import_preview(table, filename, b64):
    cols, pks, header, mapping, recs, skipped, dup = _parse(table, filename, b64)
    existing = {tuple(str(r[p]) for p in pks) for r in _all(table)}
    new = sum(1 for r in recs if tuple(str(r[p]) for p in pks) not in existing)
    return {"rows": len(recs), "new": new, "update": len(recs) - new, "skipped": skipped, "duplicates": dup,
            "mapped": [{"sheet": header[i], "column": c} for i, c in mapping.items()],
            "ignored": [h for i, h in enumerate(header) if i not in mapping and h],
            "sample": recs[:5]}


def import_apply(table, filename, b64, mode):
    cols, pks, header, mapping, recs, skipped, dup = _parse(table, filename, b64)
    backup([table])                                            # safety copy first
    path = f"/{_q(table)}?on_conflict={','.join(_q(p) for p in pks)}"
    pref = "resolution=ignore-duplicates" if mode == "new" else "resolution=merge-duplicates"
    # blank cells are left out so they never erase existing data; rows with the same filled-in columns go together
    groups = {}
    for r in recs:
        row = {k: v for k, v in r.items() if v is not None}
        groups.setdefault(tuple(sorted(row)), []).append(row)
    done = 0
    for rows_ in groups.values():
        for i in range(0, len(rows_), BATCH):
            chunk = rows_[i:i + BATCH]
            _req("POST", path, chunk, {"Prefer": pref + ",return=minimal"})
            done += len(chunk)
    _audit("import", table, None, {"file": filename, "rows": done, "mode": mode})
    return {"ok": True, "rows": done}
