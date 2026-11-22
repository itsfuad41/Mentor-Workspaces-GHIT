"""Fuad's SQL console for the Windows command prompt (cmd). Works on the same project databases as the server app,
directly on this PC - no tunnel, no login. Run:  Fuads_SQL.bat   (or  python fuads_sql.py [project])"""
import sys

import fuads_workspaces as ws

HELP = """Commands:  .projects   list projects        .use NAME   choose a project
           .tables     tables in it         .write on|off  allow changes (backup first)
           .help       this text            .quit       leave
Type SQL and finish it with ;  (it can run over several lines)."""


def show(res):
    cols, rows = res["columns"], res["rows"]
    if cols:
        text = [[("NULL" if v is None else str(v)).replace("\n", " ")[:60] for v in r] for r in rows]
        w = [max([len(c)] + [len(r[i]) for r in text]) for i, c in enumerate(cols)]
        print(" | ".join(c.ljust(w[i]) for i, c in enumerate(cols)))
        print("-+-".join("-" * x for x in w))
        for r in text:
            print(" | ".join(r[i].ljust(w[i]) for i in range(len(cols))))
        print(f"({len(rows)} row(s){', first 500 shown' if res['truncated'] else ''})")
    else:
        print(f"Done. {res['changes']} row(s) changed.")
    if res["backup"]:
        print("Backup saved:", res["backup"])


def main(argv):
    names = [p["slug"] for p in ws.list_all() if p["type"] == "database"]
    cur = argv[0] if argv and argv[0] in names else ("mentor" if "mentor" in names else names[0])
    write = False
    print("Fuad's SQL console.  Type .help for help.  Projects:", ", ".join(names))
    buf = ""
    while True:
        try:
            line = input(f"{cur}{'*' if write else ''}> " if not buf else "   ...> ")
        except (EOFError, KeyboardInterrupt):
            print()
            return 0
        s = line.strip()
        if not buf and s.startswith("."):
            cmd, _, arg = s.partition(" ")
            arg = arg.strip()
            if cmd in (".quit", ".exit", ".q"):
                return 0
            elif cmd == ".help":
                print(HELP)
            elif cmd == ".projects":
                for p in ws.list_all():
                    print(" ", p["slug"], "-", p["type"] if p["type"] == "app" else "database")
            elif cmd == ".use":
                if arg in names:
                    cur, write = arg, False
                else:
                    print("No database project with that name.")
            elif cmd == ".tables":
                try:
                    for t in ws.tables(cur):
                        print(f"  {t['name']}  ({t['rows']} rows)  {', '.join(t['columns'])}")
                except ws.WsError as e:
                    print("Error:", e)
            elif cmd == ".write":
                if arg == "on":
                    if cur == "mentor" and input("This is the LIVE Mentor database. Type YES to allow changes: ").strip() != "YES":
                        print("Staying read-only.")
                    else:
                        write = True
                        print("Changes allowed (a backup copy is made before each change).")
                else:
                    write = False
                    print("Read-only.")
            else:
                print("Unknown command. Type .help")
            continue
        buf += line + "\n"
        if s.endswith(";") or (not s and buf.strip()):
            try:
                show(ws.run_sql(cur, buf, write))
            except ws.WsError as e:
                print("Error:", str(e).replace('Tick "Allow changes"', "Type .write on"))
            buf = ""


if __name__ == "__main__":
    sys.exit(main(sys.argv[1:]))
