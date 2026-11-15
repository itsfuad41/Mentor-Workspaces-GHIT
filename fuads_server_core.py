#!/usr/bin/env python3
"""
Fuad's Project Server - core (v3)
---------------------------------
Multi-PC High-Availability Sync Server for FAAH Mentor Workstation.
Supports 1 Primary + Multiple Backup Replicas, rolling backups,
failover term election, embedded workflow telemetry and (new in v3)
DAILY SNAPSHOTS: every project is stored on the server once per day,
so older dates can be opened and downloaded as Excel from the website.
This module has no GUI; Fuads_Server_App.py is the desktop app on top of it.
"""

import base64
import collections
from concurrent.futures import ThreadPoolExecutor
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
import traceback
import urllib.error
import urllib.parse
import urllib.request
import uuid
import webbrowser
import zlib
from collections import deque
from datetime import datetime, timedelta

APP_NAME = "Fuad's Project Server"
DEFAULT_PORT = 8765
MAX_BODY = 200 * 1024 * 1024  # 200 MB
DEFAULT_SUPABASE_URL = "https://xupzriltfvznmaelixft.supabase.co"
DEFAULT_SUPABASE_KEY = "sb_publishable_rS81a0VMt3tIsqJU9UYKqA_niXFN-gS"
TAILSCALE_DOWNLOAD_URL = "https://tailscale.com/download/windows"
KV_PREFIXES = ("draft:", "qc:")
SERVER_START_TIME = time.time()

WORKFLOW_HTML = r"""<!doctype html>
<html lang="en">
<head>
<meta charset="utf-8">
<meta name="viewport" content="width=device-width,initial-scale=1">
<title>Fuad Project Server · Live Workflows</title>
<style>
:root{--bg:#f3f5f8;--card:#fff;--ink:#16202a;--mut:#5b6976;--line:#d6dde5;--ok:#1f8a5b;--bad:#c0392b;--warn:#b7791f;--acc:#1769aa;--soft:#eef3f8;--navy:#17324b}
@media(prefers-color-scheme:dark){:root{--bg:#0f151b;--card:#18212a;--ink:#e6edf3;--mut:#93a1ae;--line:#2a3641;--soft:#1f2b36}}
*{box-sizing:border-box}
body{margin:0;background:var(--bg);color:var(--ink);font:14px/1.45 system-ui,Segoe UI,Roboto,sans-serif}
header{display:flex;align-items:center;gap:10px;flex-wrap:wrap;padding:12px 20px;background:var(--card);border-bottom:1px solid var(--line);position:sticky;top:0;z-index:5;box-shadow:0 2px 8px #16202a0c}
header h1{font-size:17px;margin:0 8px 0 0}
.pill{padding:3px 10px;border-radius:99px;font-size:12px;font-weight:600;background:var(--soft);border:1px solid var(--line)}
.pill.ok{color:var(--ok);border-color:var(--ok)}.pill.bad{color:var(--bad);border-color:var(--bad)}.pill.warn{color:var(--warn);border-color:var(--warn)}
.sp{flex:1}
button{font:inherit;cursor:pointer;border:1px solid var(--line);background:var(--soft);color:var(--ink);padding:6px 12px;border-radius:7px}
button.primary{background:var(--acc);color:#fff;border-color:var(--acc)}
nav{display:flex;gap:4px;padding:10px 18px 0}
nav button{border-radius:8px 8px 0 0;border-bottom:none}
nav button.on{background:var(--card);font-weight:700;border-color:var(--line)}
main{padding:0 18px 30px}
.panel{display:none;background:var(--card);border:1px solid var(--line);border-radius:0 10px 10px 10px;padding:16px}
.panel.on{display:block}
.grid{display:grid;grid-template-columns:repeat(auto-fit,minmax(420px,1fr));gap:14px}
.card{border:1px solid var(--line);border-radius:10px;padding:12px 14px;background:var(--card);box-shadow:0 2px 8px #16202a08}
.card h3{margin:0 0 8px;font-size:15px;display:flex;align-items:center;gap:8px}
table{border-collapse:collapse;width:100%;font-size:13px}
th,td{text-align:left;padding:5px 8px;border-bottom:1px solid var(--line);vertical-align:top}
th{color:var(--mut);font-weight:600;position:sticky;top:0;background:var(--card)}
.scroll{max-height:320px;overflow:auto}
.mut{color:var(--mut)}.ok{color:var(--ok)}.bad{color:var(--bad)}.warn{color:var(--warn)}
.mono{font-family:ui-monospace,Consolas,monospace;font-size:12px}
#login{position:fixed;inset:0;background:rgba(10,22,34,.68);display:flex;align-items:center;justify-content:center;z-index:20;backdrop-filter:blur(3px)}
#login .box{background:var(--card);padding:22px 24px;border-radius:12px;width:360px;max-width:94vw;box-shadow:0 20px 60px #0004;border:1px solid var(--line)}
#login h2{margin:0 0 5px;color:var(--navy)}
#login .hint{margin:0 0 16px;color:var(--mut)}
#login label{display:block;font-weight:600;margin:10px 0 4px}
#login input{width:100%;padding:8px 10px;border:1px solid var(--line);border-radius:7px;background:var(--bg);color:var(--ink);margin:0 0 7px}
#err{color:var(--bad);min-height:20px;margin-top:8px;font-weight:600}
#login .actions{display:flex;justify-content:flex-end;gap:8px;margin-top:8px}
#login .actions button{min-width:82px}
#metrics{display:grid;grid-template-columns:repeat(auto-fit,minmax(150px,1fr));gap:10px;margin:14px 0}
.metric{padding:12px 14px;border:1px solid var(--line);border-radius:10px;background:var(--card)}
.metric b{display:block;font-size:20px}.metric span{color:var(--mut);font-size:12px}
.peer{padding:7px 9px;margin-top:6px;border:1px solid var(--line);border-radius:8px;font-size:12px;overflow-wrap:anywhere}
.database-table{font-size:12px}.database-table td:last-child{text-align:right;font-variant-numeric:tabular-nums}
.dashboard{position:relative;min-height:580px;margin:4px 0 8px;padding:18px;border:1px solid var(--line);border-radius:12px;background:linear-gradient(145deg,var(--card),var(--soft));overflow:hidden}
.map-lines{position:absolute;inset:0;width:100%;height:100%;pointer-events:none}
.map-lines path{fill:none;stroke:#647789;stroke-width:2;stroke-dasharray:7 6}
.map-lines marker path{fill:#1769aa;stroke:none}
.map-lines path.live{stroke:#188354;stroke-dasharray:none}
.node{position:absolute;z-index:1;width:30%;min-height:138px;padding:14px 16px;border:2px solid var(--ok);border-radius:14px;background:var(--card);box-shadow:0 6px 20px #10223512}
.node.offline{border-color:var(--bad)}
.node.pending{border-color:var(--warn)}
.node h3{margin:0 0 9px;font-size:15px;color:var(--navy)}
.node p{margin:4px 0;color:var(--mut);font-size:13px;overflow-wrap:anywhere}
.node .node-status{display:inline-flex;align-items:center;gap:6px;margin-bottom:6px;padding:2px 8px;border-radius:20px;background:var(--soft);font-size:12px;font-weight:700}
.node-status:before{content:"";width:8px;height:8px;border-radius:50%;background:currentColor}
.node-status.ok{color:var(--ok)}.node-status.bad{color:var(--bad)}.node-status.warn{color:var(--warn)}
#nodeClients{left:3%;top:9%;border-style:dashed;border-color:#647789}
#nodeCloud{left:39%;top:5%;width:30%;min-height:180px}
#nodeDesktop{left:3%;top:67%;border-style:dashed;border-color:#647789}
#nodePrimary{left:39%;top:67%;width:30%;min-height:160px}
#nodeBackup{right:3%;top:67%;width:27%;min-height:160px}
.connection-label{position:absolute;z-index:2;padding:3px 10px;border:1px solid var(--line);border-radius:99px;background:var(--card);font-size:12px;font-weight:600}
#labelClientCloud{left:27%;top:24%}#labelClientsPrimary{left:25%;top:54%}#labelCloudPrimary{left:47%;top:52%}#labelPrimaryBackup{left:69%;top:78%}
.empty{padding:18px;text-align:center;color:var(--mut)}
#actTable tbody tr:nth-child(even){background:var(--soft)}
@media(max-width:900px){.dashboard{display:grid;gap:10px;min-height:0;padding:12px}.node{position:relative;inset:auto!important;width:auto!important;min-height:0!important}.map-lines,.connection-label{display:none}.grid{grid-template-columns:1fr}header{padding:10px 12px}main{padding:0 10px 20px}}
</style>
</head>
<body>
<header>
  <h1>Fuad Project Server · Live Workflows</h1>
  <span class="pill" id="pMode">server: ...</span>
  <span class="pill" id="pCloud">online DB: checking</span>
  <span class="pill" id="pRepl">backup: checking</span>
  <span class="sp"></span>
  <span class="mut" id="stamp"></span>
  <label class="mut" style="display:flex;align-items:center;gap:5px"><input id="autoRefresh" type="checkbox" checked> auto-refresh</label>
  <button id="btnRefresh">Refresh now</button>
  <button id="btnOut">Log out</button>
</header>
<nav id="tabs">
  <button data-p="dbs" class="on">Cluster & Databases</button>
  <button data-p="act">Live Activity</button>
</nav>
<main>
  <section class="panel on" id="dbs">
    <div id="clusterCoverage" class="mut" style="margin:0 0 10px;padding:8px 10px;border:1px solid var(--line);border-radius:8px;background:var(--card)">Loading configured server connections…</div>
    <div class="dashboard" id="dashboard">
      <svg class="map-lines" viewBox="0 0 1000 580" preserveAspectRatio="none" aria-hidden="true">
        <defs><marker id="arrow" markerWidth="8" markerHeight="8" refX="7" refY="4" orient="auto"><path d="M0,0 L8,4 L0,8 z" fill="#1769aa"></path></marker></defs>
        <path id="linkClientsCloud" d="M330 150 C390 150 380 100 440 100" marker-end="url(#arrow)"></path>
        <path id="linkClientsPrimary" d="M300 175 C360 190 360 430 440 455" marker-end="url(#arrow)"></path>
        <path id="linkCloudPrimary" d="M540 240 L540 390" marker-end="url(#arrow)"></path>
        <path id="linkDesktopPrimary" d="M330 450 C390 450 390 480 440 480" marker-end="url(#arrow)"></path>
        <path id="linkPrimaryBackup" d="M690 480 L735 480" marker-end="url(#arrow)"></path>
      </svg>
      <div class="node" id="nodeClients"><h3>Browsers — mentors &amp; admin</h3><span class="node-status warn" id="clientStatus">Waiting for clients</span><div id="clientList"></div></div>
      <div class="node" id="nodeCloud"><h3>Online database</h3><span class="node-status warn" id="cloudStatus">Checking connection</span><div id="cloudDetails"></div></div>
      <div class="node pending" id="nodeDesktop"><h3>Desktop app (Python)</h3><span class="node-status warn" id="desktopStatus">Waiting for desktop app</span><div id="desktopDetails"><p>QC workstation and summaries sync through this PC server.</p></div></div>
      <div class="node" id="nodePrimary"><h3 id="primaryTitle">PC Project Server</h3><span class="node-status warn" id="primaryStatus">Checking server</span><div id="primaryDetails"></div></div>
      <div class="node" id="nodeBackup"><h3>Backup server(s)</h3><span class="node-status warn" id="backupStatus">Checking replicas</span><div id="backupDetails"></div></div>
      <span class="connection-label" id="labelClientCloud">Browser ↔ online tables</span>
      <span class="connection-label" id="labelClientsPrimary">Browser ↔ PC server</span>
      <span class="connection-label" id="labelCloudPrimary">Server admin check</span>
      <span class="connection-label" id="labelPrimaryBackup">Replica sync</span>
    </div>
    <div id="metrics"></div>
    <div class="grid" id="dbCards"></div>
  </section>
  <section class="panel" id="act">
    <div style="display:flex;gap:10px;align-items:center;margin-bottom:8px;">
      <input id="actFilter" placeholder="Filter activity..." style="padding:6px 9px;border:1px solid var(--line);border-radius:7px;background:var(--bg);color:var(--ink);min-width:260px">
      <span class="mut" id="actStats"></span>
    </div>
    <div class="scroll" style="max-height:560px"><table id="actTable"><thead><tr><th>Time</th><th>User</th><th>Server</th><th>Method</th><th>Target</th><th>Result</th><th>Detail</th></tr></thead><tbody></tbody></table></div>
  </section>
</main>
<div id="login"><form class="box" id="loginForm">
  <h2>Admin Login</h2>
  <p class="hint">Use your website administrator account.</p>
  <label for="u">Username</label><input id="u" autocomplete="username" required>
  <label for="pw">Password</label><input id="pw" type="password" autocomplete="current-password" required>
  <div id="err"></div>
  <div class="actions"><button type="button" id="cancelLogin">Cancel</button><button class="primary" id="go" type="submit">Sign in</button></div>
</form></div>
<script>
const $=id=>document.getElementById(id);
const esc=v=>String(v??"").replace(/[&<>"']/g,c=>({"&":"&amp;","<":"&lt;",">":"&gt;",'"':"&quot;","'":"&#39;"}[c]));
const S={base:window.location.origin,auth:"",info:null,repl:null,act:[],clients:[],nodes:[],stats:{writes:0,polls:0,errors:0},actLast:0,timer:null};

async function api(path,opt={}){const r=await fetch(S.base+path,{...opt,headers:{"X-Key":S.auth,...(opt.body?{"Content-Type":"application/json"}:{}),...(opt.headers||{})}});let j=null;try{j=await r.json()}catch{}if(!r.ok)throw Error((j&&j.error)||("HTTP "+r.status));return j;}

function showLogin(msg){$("login").style.display="flex";$("err").textContent=msg||"";}
async function signIn(){
  const username=$("u").value.trim(),password=$("pw").value;
  try{
    if(!username||!password)throw Error("Enter your website admin username and password.");
    const r=await api("/admin/login",{method:"POST",body:JSON.stringify({username,password})});S.auth=r.token;
    $("pw").value="";$("login").style.display="none";start();
  }
  catch(e){$("err").textContent=e.message;}
}
function cancelLogin(){clearInterval(S.timer);window.location.replace("/");}
async function signOut(){clearInterval(S.timer);let warning="";try{if(S.auth)await api("/admin/logout",{method:"POST"});}catch(e){warning="Signed out locally, but the server could not revoke this session: "+e.message;}S.auth="";S.info=null;S.repl=null;S.act=[];S.clients=[];S.nodes=[];S.actLast=0;showLogin(warning);}
async function refresh(){
  try{
    S.info=await api("/admin/dbinfo?detail=1");
    S.repl=await api("/admin/replication");
    const a=await api("/admin/activity?since="+S.actLast);
    if(a.items.length){S.act=[...a.items.reverse(),...S.act].slice(0,300);S.actLast=a.last;}
    S.clients=a.clients||[];
    S.nodes=a.nodes||[];
    S.stats=a.stats||{writes:0,polls:0,errors:0};
    paint();
  }catch(e){$("pMode").className="pill bad";$("pMode").textContent="Server unreachable";$("primaryStatus").textContent=e.message;$("nodePrimary").className="node offline";$("stamp").textContent="Refresh failed · "+new Date().toLocaleTimeString();}
}
function start(){refresh();clearInterval(S.timer);S.timer=setInterval(()=>{if($("autoRefresh").checked)refresh();},4000);}

function paint(){
  const i=S.info,r=S.repl;
  if(i){$("pMode").className="pill "+(i.server.mode==="replica"?"warn":"ok");$("pMode").textContent=`server: ${i.server.mode.toUpperCase()} · term ${i.server.term}`;
    $("nodePrimary").className="node";$("primaryTitle").textContent=`PC Project Server — ${i.server.mode.toUpperCase()}`;
    $("primaryStatus").className="node-status ok";$("primaryStatus").textContent="Online";
    $("primaryDetails").innerHTML=`<p>${esc(i.server.db_path.split(/[\\\\/]/).pop())} · ${formatBytes(i.server.db_size)}</p><p>${i.projects.length} project(s) · term ${esc(i.server.term)}</p><p>Port ${esc(i.server.port)} · uptime ${formatDuration(i.server.uptime)}</p>`;
    $("nodeCloud").className="node "+(i.online_db?.connected?"":"offline");
    $("cloudStatus").className="node-status "+(i.online_db?.connected?"ok":"bad");$("cloudStatus").textContent=i.online_db?.connected?"Connected":"Disconnected";
    $("pCloud").className="pill "+(i.online_db?.connected?"ok":"bad");$("pCloud").textContent=i.online_db?.connected?"online DB: connected":"online DB: offline";
    $("cloudDetails").innerHTML=`<p>${esc(i.online_db?.message||"Connection status unavailable")}</p><p>${esc(i.online_db?.url||"")}</p><p>${i.online_db?.tables?.filter(t=>t.available).length||0}/${i.online_db?.tables?.length||0} tables accessible</p>`;
  }
  if(r){
    const peers=r.peers||[],online=peers.filter(p=>p.reachable).length;
    $("pRepl").className="pill "+(peers.length&&online===peers.length&&r.in_sync?"ok":"warn");
    $("pRepl").textContent=peers.length?`replicas: ${online}/${peers.length} reachable`:"replicas: not configured";
    $("nodeBackup").className="node "+(peers.length&&online===peers.length?"":"pending");
    $("backupStatus").className="node-status "+(peers.length&&online===peers.length?"ok":"warn");
    $("backupStatus").textContent=peers.length?`${online}/${peers.length} reachable · ${r.in_sync?"in sync":"sync pending"}`:"No replicas configured";
    $("backupDetails").innerHTML=peers.map(p=>`<div class="peer"><b>${esc(p.mode||"unknown")} · ${p.reachable?"reachable":"offline"}</b><br>${esc(p.url)} · term ${esc(p.term??"—")}</div>`).join("")||"<p>Add replica URLs in server settings to enable replication.</p>";
    $("primaryDetails").innerHTML+=`<p>${esc(r.status)}</p>`;
    $("linkPrimaryBackup").classList.toggle("live",online>0&&r.in_sync);
  }
  if(i){
    const clients=S.clients||[],browsers=clients.filter(c=>c.kind!=="desktop"),desktop=clients.filter(c=>c.kind==="desktop");
        $("clientStatus").className="node-status "+(browsers.length?"ok":"warn");
        $("clientStatus").textContent=browsers.length?`${browsers.length} active browser${browsers.length===1?"":"s"}`:"Waiting for browser clients";
        $("clientList").innerHTML=browsers.map(c=>`<p>${esc(c.name)} · ${esc(c.server||"this server")} · active ${esc(c.seconds_ago)}s ago</p>`).join("")||"<p>No browser sync requests in the last 45 seconds.</p>";
        $("nodeDesktop").className="node "+(desktop.length?"":"pending");
        $("desktopStatus").className="node-status "+(desktop.length?"ok":"warn");
        $("desktopStatus").textContent=desktop.length?"Connected":"Waiting for desktop app";
        $("desktopDetails").innerHTML=desktop.map(c=>`<p>${esc(c.name)} · ${esc(c.server||"this server")} · active ${esc(c.seconds_ago)}s ago</p>`).join("")||"<p>QC workstation and summaries sync through this PC server.</p>";
        $("linkDesktopPrimary").classList.toggle("live",desktop.length>0);
        $("linkClientsCloud").classList.toggle("live",browsers.length>0&&Boolean(i.online_db?.connected));
        $("linkClientsPrimary").classList.toggle("live",browsers.length>0);
    $("linkCloudPrimary").classList.toggle("live",Boolean(i.online_db?.connected));
  }
  $("stamp").textContent="Updated "+new Date().toLocaleTimeString();
  const onlineNodes=S.nodes.filter(n=>n.reachable).length,failedNodes=S.nodes.length-onlineNodes;
  const nodeSummary=S.nodes.map(n=>`${n.name}: ${n.reachable?"online":`offline (${n.error||"unreachable"})`}`).join(" · ");
  $("clusterCoverage").textContent=S.nodes.length
    ? `Connection activity: ${onlineNodes}/${S.nodes.length} configured server(s) reachable · ${nodeSummary}`
    : "Connection activity is local to this server. Configure its peers to include their connections.";
  const browserCount=S.clients.filter(c=>c.kind!=="desktop").length,desktopCount=S.clients.filter(c=>c.kind==="desktop").length;
  $("metrics").innerHTML=`<div class="metric"><b>${browserCount}</b><span>Active browser clients</span></div><div class="metric"><b>${desktopCount}</b><span>Active desktop clients</span></div><div class="metric"><b>${S.stats.writes}</b><span>Recorded writes</span></div><div class="metric"><b>${S.stats.polls}</b><span>State polls</span></div><div class="metric"><b>${S.stats.errors}</b><span>Request errors</span></div>`;
  const localTables=i?.tables||[],cloudTables=i?.online_db?.tables||[];
  $("dbCards").innerHTML=i?`<div class="card"><h3>Local SQLite · ${esc(i.server.db_path)}</h3><div class="scroll"><table class="database-table"><thead><tr><th>Table</th><th>Rows</th></tr></thead><tbody>${localTables.map(t=>`<tr><td>${esc(t.name)}</td><td>${esc(t.rows)}</td></tr>`).join("")||"<tr><td colspan='2'>No application tables found</td></tr>"}</tbody></table></div></div><div class="card"><h3>Online database tables</h3><div class="scroll"><table class="database-table"><thead><tr><th>Table</th><th>Status / rows</th></tr></thead><tbody>${cloudTables.map(t=>`<tr><td>${esc(t.name)}</td><td class="${t.available?"ok":"bad"}">${t.available?(t.rows==null?"Accessible · count unavailable":esc(t.rows)+" rows"):esc(t.error||"Unavailable")}</td></tr>`).join("")||"<tr><td colspan='2'>No cloud table checks returned</td></tr>"}</tbody></table></div></div><div class="card"><h3>Local projects (${i.projects.length})</h3><div class="scroll"><table><thead><tr><th>Project</th><th>Parts</th><th>Version</th></tr></thead><tbody>${i.projects.map(p=>`<tr><td><b>${esc(p.name)}</b></td><td>${p.parts}</td><td class="mono">${esc(p.version.slice(0,8))}</td></tr>`).join("")||"<tr><td colspan='3'>No projects</td></tr>"}</tbody></table></div></div>`:"";

  const f=$("actFilter").value.trim().toLowerCase();
  $("actTable").querySelector("tbody").innerHTML=S.act.filter(a=>!f||[a.who,a.server,a.method,a.path,a.detail].join(" ").toLowerCase().includes(f)).map(a=>`<tr>
    <td>${new Date(a.ts*1000).toLocaleTimeString()}</td><td>${esc(a.who)}</td><td>${esc(a.server||"this server")}</td><td><b>${esc(a.method)}</b></td><td class="mono">${esc(a.path)}</td>
    <td class="${a.status>=400?"bad":"ok"}">${a.status}</td><td>${esc(a.detail||"")}</td>
  </tr>`).join("")||"<tr><td colspan='7'>No activity recorded</td></tr>";
}
document.querySelectorAll("#tabs button").forEach(b=>b.onclick=()=>{document.querySelectorAll("#tabs button").forEach(x=>x.classList.toggle("on",x===b));document.querySelectorAll(".panel").forEach(p=>p.classList.toggle("on",p.id===b.dataset.p));});
function formatBytes(n){if(n<1024)return n+" B";if(n<1024*1024)return (n/1024).toFixed(1)+" KB";return (n/1024/1024).toFixed(1)+" MB";}
function formatDuration(s){s=Math.max(0,Math.floor(Number(s)||0));const h=Math.floor(s/3600),m=Math.floor((s%3600)/60);return h?`${h}h ${m}m`:`${m}m ${s%60}s`;}
$("loginForm").addEventListener("submit",e=>{e.preventDefault();signIn();});
$("cancelLogin").onclick=cancelLogin;
$("btnRefresh").onclick=refresh;
$("btnOut").onclick=signOut;
$("actFilter").addEventListener("input",()=>{if(S.info)paint();});
$("u").focus();
</script>
</body>
</html>
"""

def _data_dir():
    forced = os.environ.get("FAAH_SERVER_DIR")
    if forced:
        path = forced
    elif sys.platform.startswith("win"):
        path = os.path.join(os.environ.get("LOCALAPPDATA", os.path.expanduser("~")), "FuadProjectServer")
    else:
        path = os.path.join(os.path.expanduser("~"), ".faah_project_server")
    os.makedirs(path, exist_ok=True)
    if not forced and sys.platform.startswith("win"):
        legacy = r"C:\Data\FaahProjectServer"
        if os.path.isdir(legacy) and os.path.normcase(legacy) != os.path.normcase(path):
            for filename in ("projects.db", "server_config.json"):
                source, destination = os.path.join(legacy, filename), os.path.join(path, filename)
                if os.path.isfile(source) and not os.path.exists(destination):
                    shutil.copy2(source, destination)
            old_backups, new_backups = os.path.join(legacy, "backups"), os.path.join(path, "backups")
            if os.path.isdir(old_backups) and not os.path.exists(new_backups):
                shutil.copytree(old_backups, new_backups)
    return path

DATA_DIR = _data_dir()
DB_PATH = os.path.join(DATA_DIR, "projects.db")
CONFIG_PATH = os.path.join(DATA_DIR, "server_config.json")
BACKUP_DIR = os.path.join(DATA_DIR, "backups")
BACKGROUND_STOP_PATH = os.path.join(DATA_DIR, "background_server.stop")
BACKGROUND_LOG_PATH = os.path.join(DATA_DIR, "background_server.log")

_write_lock = threading.Lock()
_log = deque(maxlen=300)
_CFG = None
_BACKGROUND_LOG = None

def _append_log_file(line):
    """Keep a log file on disk (needed to find out why a silent start at sign-in did not work)."""
    try:
        if os.path.exists(BACKGROUND_LOG_PATH) and os.path.getsize(BACKGROUND_LOG_PATH) > 2 * 1024 * 1024:
            old_path = BACKGROUND_LOG_PATH + ".1"
            if os.path.exists(old_path):
                os.remove(old_path)
            os.replace(BACKGROUND_LOG_PATH, old_path)
        with open(BACKGROUND_LOG_PATH, "a", encoding="utf-8") as fh:
            fh.write(datetime.now().strftime("%Y-%m-%d ") + line + "\n")
    except OSError:
        pass

def log(msg):
    ts = datetime.now().strftime("%H:%M:%S")
    line = f"{ts}  {msg}"
    _log.append(line)
    _append_log_file(line)
    if sys.stdout is not None and not getattr(sys, "frozen", False):
        print(f"[{ts}] {msg}", flush=True)

def save_config(cfg):
    try:
        with open(CONFIG_PATH, "w", encoding="utf-8") as fh:
            json.dump(cfg, fh, indent=2)
        return True
    except OSError as e:
        log(f"Config write error: {e}")
        return False

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
        ("cloudflare_token", ""),
        ("cloudflare_url", ""),
        ("cloudflare_auto", False),
        ("temp_link_auto", False),
        ("snapshot_keep_days", 0),
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

def public_base_url(cfg):
    """Stable public address: Tailscale/custom URL first, else the user's own Cloudflare tunnel address."""
    custom = str(cfg.get("custom_url") or "").strip().rstrip("/")
    if custom:
        return custom
    if cfg.get("cloudflare_token") and cfg.get("cloudflare_url"):
        return str(cfg["cloudflare_url"]).strip().rstrip("/")
    return ""

def make_code(url, view_key, admin_key):
    cfg = load_config()
    final_url = public_base_url(cfg) or url
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
            conn.execute("""
                CREATE TABLE IF NOT EXISTS uploads (
                    id TEXT PRIMARY KEY,
                    project_name TEXT NOT NULL,
                    file_name TEXT DEFAULT '',
                    uploaded_by TEXT DEFAULT '',
                    uploaded_at TEXT DEFAULT '',
                    part_count INTEGER DEFAULT 0,
                    parts TEXT NOT NULL DEFAULT '[]',
                    status TEXT DEFAULT 'pending',
                    published_by TEXT DEFAULT '',
                    published_at TEXT DEFAULT ''
                )
            """)
            conn.execute("""
                CREATE TABLE IF NOT EXISTS project_activity (
                    project TEXT NOT NULL,
                    user TEXT NOT NULL,
                    last_at TEXT DEFAULT '',
                    last_ts REAL DEFAULT 0,
                    edits INTEGER DEFAULT 0,
                    PRIMARY KEY (project, user)
                )
            """)
            conn.execute("""
                CREATE TABLE IF NOT EXISTS daily_snapshots (
                    day TEXT NOT NULL,
                    project TEXT NOT NULL,
                    parts BLOB NOT NULL,
                    part_count INTEGER DEFAULT 0,
                    updated_at TEXT DEFAULT '',
                    updated_by TEXT DEFAULT '',
                    edited INTEGER DEFAULT 0,
                    PRIMARY KEY (day, project)
                )
            """)
            conn.execute("CREATE INDEX IF NOT EXISTS idx_snap_updated ON daily_snapshots(updated_at)")
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
            _snapshot_write(conn, name, text, len(parts), by, edited=True)
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
            _snapshot_write(conn, name, text, len(parts), by, edited=True)
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

# ---- Daily snapshots: the server keeps one copy of every project for every day ----
DAY_RE = re.compile(r"^\d{4}-\d{2}-\d{2}$")
SNAP_SYNC_BATCH = 40

def _today():
    return datetime.now().strftime("%Y-%m-%d")

def _snapshot_write(conn, name, text, count, by="", edited=True):
    """Store (or refresh) today's copy of a project. Runs inside the caller's write transaction.
    A failure here must never block the real project save, so errors are only logged."""
    try:
        conn.execute(
            "INSERT INTO daily_snapshots (day, project, parts, part_count, updated_at, updated_by, edited) VALUES (?,?,?,?,?,?,?) "
            "ON CONFLICT(day, project) DO UPDATE SET parts=excluded.parts, part_count=excluded.part_count, "
            "updated_at=excluded.updated_at, updated_by=excluded.updated_by, edited=MAX(edited, excluded.edited)",
            (_today(), name, zlib.compress(text.encode("utf-8"), 6), count, _now(), by or "", 1 if edited else 0))
    except Exception as error:
        log(f"Daily snapshot of '{name}' failed: {error}")

def snapshot_put(day, project, parts, by=""):
    """Save an edited copy for a PAST day (the website allows this while that day's Mentor Summary is not submitted).
    The live project is never touched, and today's copy only changes through the normal project save."""
    if not DAY_RE.match(day or ""):
        raise ValueError("Use a date like 2026-11-15.")
    if day >= _today():
        raise ValueError("Today's work is saved with the live project, not as a back-dated copy.")
    if not project or len(project) > 200:
        raise ValueError("Missing project name.")
    if not isinstance(parts, list):
        raise ValueError("parts must be a list.")
    text = json.dumps(parts, ensure_ascii=False)
    with _write_lock:
        conn = _db()
        try:
            conn.execute(
                "INSERT INTO daily_snapshots (day, project, parts, part_count, updated_at, updated_by, edited) VALUES (?,?,?,?,?,?,1) "
                "ON CONFLICT(day, project) DO UPDATE SET parts=excluded.parts, part_count=excluded.part_count, "
                "updated_at=excluded.updated_at, updated_by=excluded.updated_by, edited=1",
                (day, project, zlib.compress(text.encode("utf-8"), 6), len(parts), _now(), by or ""))
            conn.commit()
        finally:
            conn.close()
    return len(parts)

def snapshot_all(force=False):
    """Make sure every project has a copy for today (days nobody edited still get a copy).
    force=True also refreshes copies that already exist. Returns how many were written."""
    if load_config().get("mode") == "replica":
        return 0  # a backup server receives its snapshots from the primary
    written = 0
    with _write_lock:
        conn = _db()
        try:
            have = {r["project"] for r in conn.execute("SELECT project FROM daily_snapshots WHERE day=?", (_today(),)).fetchall()}
            for r in conn.execute("SELECT name, parts, part_count, updated_by FROM projects").fetchall():
                if force or r["name"] not in have:
                    _snapshot_write(conn, r["name"], r["parts"], r["part_count"] or 0, r["updated_by"], edited=False)
                    written += 1
            conn.commit()
        finally:
            conn.close()
    return written

def prune_snapshots():
    keep = int(load_config().get("snapshot_keep_days") or 0)
    if keep <= 0:
        return 0
    cutoff = (datetime.now() - timedelta(days=keep)).strftime("%Y-%m-%d")
    with _write_lock:
        conn = _db()
        try:
            cur = conn.execute("DELETE FROM daily_snapshots WHERE day < ?", (cutoff,))
            conn.commit()
            return cur.rowcount
        finally:
            conn.close()

def snapshot_days():
    conn = _db()
    try:
        rows = conn.execute("SELECT day, COUNT(*) AS projects, SUM(edited) AS edited, SUM(part_count) AS parts "
                            "FROM daily_snapshots GROUP BY day ORDER BY day DESC").fetchall()
        return [{"day": r["day"], "projects": r["projects"], "edited": r["edited"] or 0, "parts": r["parts"] or 0} for r in rows]
    finally:
        conn.close()

def snapshot_day(day):
    conn = _db()
    try:
        live = {r["name"] for r in conn.execute("SELECT name FROM projects").fetchall()}
        rows = conn.execute("SELECT project, part_count, updated_at, updated_by, edited FROM daily_snapshots WHERE day=? ORDER BY project", (day,)).fetchall()
        ups = conn.execute("SELECT id, project_name, file_name, uploaded_by, uploaded_at, part_count FROM uploads "
                           "WHERE substr(uploaded_at,1,10)=? ORDER BY uploaded_at", (day,)).fetchall()
        return {
            "day": day,
            "projects": [{"project_name": r["project"], "part_count": r["part_count"] or 0, "updated_at": r["updated_at"] or "",
                          "updated_by": r["updated_by"] or "", "edited": bool(r["edited"]), "deleted": r["project"] not in live} for r in rows],
            "uploads": [{k: u[k] for k in u.keys()} for u in ups],
        }
    finally:
        conn.close()

def snapshot_get(day, project):
    conn = _db()
    try:
        r = conn.execute("SELECT parts, part_count, updated_at, updated_by, edited FROM daily_snapshots WHERE day=? AND project=?", (day, project)).fetchone()
        if not r:
            return None
        return {"day": day, "project_name": project, "parts": json.loads(zlib.decompress(r["parts"]).decode("utf-8")),
                "part_count": r["part_count"] or 0, "updated_at": r["updated_at"] or "", "updated_by": r["updated_by"] or "", "edited": bool(r["edited"])}
    finally:
        conn.close()

def snapshots_since(since):
    conn = _db()
    try:
        rows = conn.execute("SELECT day, project, parts, part_count, updated_at, updated_by, edited FROM daily_snapshots "
                            "WHERE updated_at >= ? ORDER BY updated_at, day, project LIMIT ?", (since or "", SNAP_SYNC_BATCH)).fetchall()
        return [{"day": r["day"], "project": r["project"], "parts": base64.b64encode(r["parts"]).decode("ascii"), "part_count": r["part_count"],
                 "updated_at": r["updated_at"], "updated_by": r["updated_by"], "edited": r["edited"]} for r in rows]
    finally:
        conn.close()

def pull_snapshots_from(peer):
    """Replica side: copy the primary's daily snapshots that this server does not have yet."""
    base = peer["url"].rstrip("/")
    key = peer.get("admin_key") or load_config().get("admin_key")
    for _ in range(200):
        conn = _db()
        try:
            since = conn.execute("SELECT COALESCE(MAX(updated_at), '') FROM daily_snapshots").fetchone()[0]
        finally:
            conn.close()
        req = urllib.request.Request(base + "/api/snapshots-sync?since=" + urllib.parse.quote(since), headers={"User-Agent": "FaahReplicator/3.0", "X-Key": key})
        with urllib.request.urlopen(req, timeout=15) as resp:
            rows = json.loads(resp.read().decode("utf-8")).get("snapshots", [])
        if not rows:
            return
        fresh = 0
        with _write_lock:
            conn = _db()
            try:
                for r in rows:
                    cur = conn.execute("SELECT updated_at FROM daily_snapshots WHERE day=? AND project=?", (r["day"], r["project"])).fetchone()
                    if cur and cur[0] == r["updated_at"]:
                        continue
                    conn.execute("INSERT OR REPLACE INTO daily_snapshots (day, project, parts, part_count, updated_at, updated_by, edited) VALUES (?,?,?,?,?,?,?)",
                                 (r["day"], r["project"], base64.b64decode(r["parts"]), r.get("part_count", 0), r.get("updated_at", ""), r.get("updated_by", ""), r.get("edited", 0)))
                    fresh += 1
                conn.commit()
            finally:
                conn.close()
        if not fresh:
            return  # everything in this batch was already here

def _snapshot_loop():
    last_prune = ""
    while True:
        try:
            snapshot_all()
            if last_prune != _today():
                prune_snapshots()
                last_prune = _today()
        except Exception as error:
            log(f"Snapshot loop error: {error}")
        time.sleep(300)

# ---- Project Excel uploads (who uploaded what) + who is working on which project ----
ACTIVE_WINDOW_SECONDS = 600  # a user counts as "working now" if they saved within 10 minutes

def record_project_activity(project, user):
    user = (user or "").strip()[:80]
    if not project or not user or user.lower() == "guest":
        return
    now = time.time()
    with _write_lock:
        conn = _db()
        try:
            conn.execute(
                "INSERT INTO project_activity (project, user, last_at, last_ts, edits) VALUES (?,?,?,?,1) "
                "ON CONFLICT(project, user) DO UPDATE SET last_at=excluded.last_at, last_ts=excluded.last_ts, edits=edits+1",
                (project, user, _now(), now))
            conn.commit()
        finally:
            conn.close()

def add_upload(name, file_name, by, parts, already_live=False):
    if not isinstance(parts, list):
        raise ValueError("No parts to upload.")
    parts = [p for p in parts if isinstance(p, dict)]
    if not parts:
        raise ValueError("No parts to upload.")
    _ensure_ids(parts)
    text = json.dumps(parts, separators=(",", ":"), ensure_ascii=False)
    uid = uuid.uuid4().hex
    now = _now()
    status = "published" if already_live else "pending"
    with _write_lock:
        conn = _db()
        try:
            # a new upload from the same user for the same project replaces their older waiting one
            conn.execute("DELETE FROM uploads WHERE project_name=? AND uploaded_by=? AND status='pending'", (name, by or ""))
            conn.execute(
                "INSERT INTO uploads (id, project_name, file_name, uploaded_by, uploaded_at, part_count, parts, status, published_by, published_at) "
                "VALUES (?,?,?,?,?,?,?,?,?,?)",
                (uid, name, (file_name or "")[:200], by or "", now, len(parts), text, status,
                 (by or "") if already_live else "", now if already_live else ""))
            conn.commit()
        finally:
            conn.close()
    return uid

def list_uploads():
    now = time.time()
    conn = _db()
    try:
        rows = conn.execute(
            "SELECT id, project_name, file_name, uploaded_by, uploaded_at, part_count, status, published_by, published_at "
            "FROM uploads ORDER BY uploaded_at DESC, rowid DESC LIMIT 500").fetchall()
        act = conn.execute("SELECT project, user, last_at, last_ts, edits FROM project_activity ORDER BY last_ts DESC").fetchall()
        live = {r["name"] for r in conn.execute("SELECT name FROM projects").fetchall()}
    finally:
        conn.close()
    by_project = {}
    for a in act:
        age = max(0, int(now - (a["last_ts"] or 0)))
        by_project.setdefault(a["project"], []).append(
            {"user": a["user"], "last_at": a["last_at"] or "", "edits": a["edits"] or 0,
             "seconds_ago": age, "active": age <= ACTIVE_WINDOW_SECONDS})
    out = []
    for r in rows:
        d = {k: r[k] for k in r.keys()}
        d["activity"] = by_project.get(r["project_name"], [])
        d["on_server"] = r["project_name"] in live
        out.append(d)
    return out

def publish_upload(uid, by):
    conn = _db()
    try:
        r = conn.execute("SELECT project_name, parts FROM uploads WHERE id=?", (uid,)).fetchone()
    finally:
        conn.close()
    if not r:
        return None
    parts = json.loads(r["parts"])
    version, epoch = put_project(r["project_name"], parts, by)
    with _write_lock:
        conn = _db()
        try:
            conn.execute("UPDATE uploads SET status='published', published_by=?, published_at=? WHERE id=?", (by or "", _now(), uid))
            conn.commit()
        finally:
            conn.close()
    return {"project_name": r["project_name"], "version": version, "epoch": epoch, "part_count": len(parts)}

def delete_upload(uid):
    with _write_lock:
        conn = _db()
        try:
            cur = conn.execute("DELETE FROM uploads WHERE id=?", (uid,))
            conn.commit()
            return cur.rowcount > 0
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

_BACKUPS_STARTED = False

def start_backups():
    """Starts the rolling database backups and the daily-snapshot loop (safe to call twice)."""
    global _BACKUPS_STARTED
    if _BACKUPS_STARTED:
        return
    _BACKUPS_STARTED = True
    threading.Thread(target=_backup_loop, daemon=True).start()
    threading.Thread(target=_snapshot_loop, daemon=True).start()

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
            for u in data.get("uploads", []):
                conn.execute(
                    "INSERT OR REPLACE INTO uploads (id, project_name, file_name, uploaded_by, uploaded_at, part_count, parts, status, published_by, published_at) "
                    "VALUES (?,?,?,?,?,?,?,?,?,?)",
                    (u["id"], u["project_name"], u.get("file_name", ""), u.get("uploaded_by", ""), u.get("uploaded_at", ""),
                     u.get("part_count", 0), u.get("parts", "[]"), u.get("status", "pending"), u.get("published_by", ""), u.get("published_at", "")))
            for a in data.get("activity", []):
                conn.execute(
                    "INSERT OR REPLACE INTO project_activity (project, user, last_at, last_ts, edits) VALUES (?,?,?,?,?)",
                    (a["project"], a["user"], a.get("last_at", ""), a.get("last_ts", 0), a.get("edits", 0)))
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
                    try:
                        pull_snapshots_from(active_primary["peer"])
                    except Exception as error:
                        log(f"Snapshot sync from primary failed (will retry): {error}")
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

    def record(self, who, method, path, status, detail="", client_type="client"):
        with self.lock:
            if status >= 400:
                self.errors += 1
            if method in ("PUT", "POST", "PATCH", "DELETE"):
                self.writes += 1
            if path in ("/state", "/ping"):
                self.polls += 1
            self.items.append({"ts": time.time(), "who": who or "anon", "method": method, "path": path, "status": status, "detail": detail, "client_type": client_type})

    def get_data(self, since=0):
        with self.lock:
            now = time.time()
            active = {}
            for item in reversed(self.items):
                if now - item["ts"] > 45:
                    break
                if item["path"] not in ("/state", "/projects", "/api/sync") or item["status"] >= 400:
                    continue
                name = item["who"] or "Unknown client"
                client_type = item.get("client_type", "client")
                client_key = (client_type, name)
                if client_key not in active:
                    active[client_key] = {
                        "name": name,
                        "kind": client_type,
                        "last_seen": item["ts"],
                        "seconds_ago": max(0, int(now - item["ts"])),
                    }
            return {
                "items": [it for it in self.items if it["ts"] > since],
                "last": max((it["ts"] for it in self.items), default=0),
                "stats": {"writes": self.writes, "polls": self.polls, "errors": self.errors},
                "clients": list(active.values()),
            }

ACTIVITY = ActivityTracker()

def _activity_server_name(url, fallback):
    try:
        hostname = urllib.parse.urlsplit(url).hostname if url else None
    except ValueError:
        hostname = None
    return hostname or fallback

def cluster_activity_data(since, cfg):
    local_url = public_base_url(cfg)
    local_name = _activity_server_name(local_url, socket.gethostname())
    local_data = ACTIVITY.get_data(since)
    items = [{**item, "server": local_name} for item in local_data["items"]]
    clients = [{**client, "server": local_name} for client in local_data["clients"]]
    stats = dict(local_data["stats"])
    nodes = [{
        "name": local_name,
        "url": local_url,
        "mode": cfg.get("mode", "primary"),
        "reachable": True,
    }]
    last = local_data["last"]
    peers = cfg.get("peers", [])
    if not peers:
        return {"items": items, "last": last, "stats": stats, "clients": clients, "nodes": nodes}

    def fetch_peer(peer):
        url = str(peer.get("url") or "").rstrip("/")
        name = _activity_server_name(url, "Unnamed server")
        node = {"name": name, "url": url, "reachable": False}
        key = str(peer.get("admin_key") or "")
        try:
            parsed_url = urllib.parse.urlsplit(url)
        except ValueError:
            parsed_url = None
        if not parsed_url or parsed_url.scheme not in ("http", "https") or not parsed_url.netloc:
            node["error"] = "Invalid server URL."
            return node, None
        if not key:
            node["error"] = "No server admin key is saved for this peer."
            return node, None
        query = urllib.parse.urlencode({"since": since, "local": "1"})
        request = urllib.request.Request(
            f"{url}/admin/activity?{query}",
            headers={"X-Key": key, "User-Agent": "FuadProjectServer/2.6"},
        )
        try:
            with urllib.request.urlopen(request, timeout=4) as response:
                data = json.loads(response.read().decode("utf-8"))
            node["reachable"] = True
            node["mode"] = str(data.get("mode") or "unknown")
            return node, data
        except (OSError, urllib.error.URLError, TimeoutError, ValueError, TypeError) as error:
            node["error"] = str(error)[:160]
            return node, None

    with ThreadPoolExecutor(max_workers=min(8, len(peers))) as executor:
        results = list(executor.map(fetch_peer, peers))
    for node, data in results:
        nodes.append(node)
        if data is None:
            continue
        peer_name = node["name"]
        items.extend({**item, "server": peer_name} for item in data.get("items", []))
        clients.extend({**client, "server": peer_name} for client in data.get("clients", []))
        peer_stats = data.get("stats") or {}
        for key in stats:
            stats[key] += int(peer_stats.get(key, 0) or 0)
        last = max(last, float(data.get("last") or 0))

    items.sort(key=lambda item: item.get("ts", 0))
    return {"items": items, "last": last, "stats": stats, "clients": clients, "nodes": nodes}

_cloud_health_lock = threading.Lock()
_cloud_health_cache = {"checked_at": 0, "connected": False, "message": "Not checked", "tables": []}
ONLINE_TABLES = ("system_users", "daily_production_summaries", "operators", "pc_server_config")

def online_database_status():
    global _cloud_health_cache
    now = time.time()
    with _cloud_health_lock:
        if now - _cloud_health_cache["checked_at"] < 30:
            return dict(_cloud_health_cache)
        cfg = load_config()
        base = (cfg.get("supabase_url") or DEFAULT_SUPABASE_URL).rstrip("/")
        key = cfg.get("supabase_key") or DEFAULT_SUPABASE_KEY

        def check_table(name):
            request = urllib.request.Request(
                f"{base}/rest/v1/{name}?select=*&limit=0",
                headers={
                    "apikey": key,
                    "Authorization": f"Bearer {key}",
                    "Prefer": "count=exact",
                    "Range": "0-0",
                },
            )
            try:
                with urllib.request.urlopen(request, timeout=2) as response:
                    match = re.search(r"/(\d+)$", response.headers.get("Content-Range", ""))
                    return {"name": name, "available": True, "rows": int(match.group(1)) if match else None}
            except urllib.error.HTTPError as error:
                return {"name": name, "available": False, "rows": None, "error": f"HTTP {error.code}"}
            except (OSError, urllib.error.URLError, TimeoutError) as error:
                return {"name": name, "available": False, "rows": None, "error": f"Connection failed: {str(error)[:100]}"}

        with ThreadPoolExecutor(max_workers=len(ONLINE_TABLES)) as executor:
            tables = list(executor.map(check_table, ONLINE_TABLES))
        connected = bool(tables and tables[0]["available"])
        available = sum(1 for table in tables if table["available"])
        message = f"{available}/{len(tables)} checked tables accessible" if connected else (tables[0].get("error", "Connection failed") if tables else "No tables checked")
        result = {"checked_at": now, "connected": connected, "message": message, "url": base, "tables": tables}
        _cloud_health_cache = result
        return dict(result)

class Handler(http.server.BaseHTTPRequestHandler):
    server_version = "FaahProjectServer/3.0"

    def log_message(self, *args):
        pass

    def _cors(self):
        self.send_header("Access-Control-Allow-Origin", "*")
        self.send_header("Access-Control-Allow-Headers", "X-Key, X-Who, X-Client, Content-Type, Authorization")
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

            if path == "/favicon.ico" and method == "GET":
                self.send_response(204)
                self.end_headers()
                return

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
            who = (self.headers.get("X-Who") or role or "guest").strip()[:80]
            client_type = (self.headers.get("X-Client") or "client").strip().lower()
            if client_type not in ("browser", "desktop"):
                client_type = "client"

            if path == "/admin/login" and method == "POST":
                b = self._body()
                u, p = str(b.get("username", "")).strip().lower(), str(b.get("password", "")).strip()
                ok, msg = verify_admin_login(u, p)
                if not ok:
                    ACTIVITY.record(u, "POST", path, 401, msg)
                    return self._send(401, {"error": msg})
                t = secrets.token_hex(16)
                cfg.setdefault("admin_tokens", []).append(t)
                if not save_config(cfg):
                    cfg["admin_tokens"].remove(t)
                    return self._send(500, {"error": "Could not save the admin session."})
                ACTIVITY.record(u, "POST", path, 200, "Signed in")
                return self._send(200, {"token": t})

            if path == "/admin/logout" and method == "POST":
                token = (self.headers.get("Authorization") or "").strip().replace("Bearer ", "") or (self.headers.get("X-Key") or "").strip()
                tokens = cfg.get("admin_tokens") or []
                if token in tokens:
                    tokens.remove(token)
                    cfg["admin_tokens"] = tokens
                    if not save_config(cfg):
                        tokens.append(token)
                        return self._send(500, {"error": "Could not revoke the admin session."})
                return self._send(200, {"ok": True})

            if not role:
                ACTIVITY.record(who, method, path, 401, "Wrong key")
                token = (self.headers.get("Authorization") or "").strip().replace("Bearer ", "") or (self.headers.get("X-Key") or "").strip()
                message = (
                    "This endpoint requires the latest server key. Paste the current connection code from the server app into Website → Admin → Server settings, then click Save & Test."
                    if token else
                    "This endpoint requires a server key. Do not open /ping directly; use the current connection code in Website → Admin → Server settings."
                )
                return self._send(401, {"error": message})

            if path == "/ping":
                return self._send(200, {"ok": True, "role": role})

            if path == "/admin/dbinfo" and method == "GET":
                if role != "admin":
                    return self._send(403, {"error": "Admin required."})
                conn = _db()
                try:
                    p_rows = conn.execute("SELECT name, parts, version, updated_at, updated_by FROM projects").fetchall()
                    table_names = [
                        row["name"] for row in conn.execute(
                            "SELECT name FROM sqlite_master WHERE type='table' AND name NOT LIKE 'sqlite_%' ORDER BY name"
                        ).fetchall()
                    ]
                    tables = []
                    for table_name in table_names:
                        safe_name = table_name.replace('"', '""')
                        row_count = conn.execute(f'SELECT COUNT(*) FROM "{safe_name}"').fetchone()[0]
                        tables.append({"name": table_name, "rows": row_count})
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
                    "server": {
                        "mode": cfg.get("mode", "primary"),
                        "term": cfg.get("term", 1),
                        "port": cfg.get("port", DEFAULT_PORT),
                        "uptime": max(0, int(time.time() - SERVER_START_TIME)),
                        "db_size": os.path.getsize(DB_PATH) if os.path.exists(DB_PATH) else 0,
                        "db_path": DB_PATH,
                    },
                    "projects": projs,
                    "tables": tables,
                    "online_db": online_database_status(),
                })

            if path == "/admin/activity" and method == "GET":
                if role != "admin":
                    return self._send(403, {"error": "Admin required."})
                query = urllib.parse.parse_qs(urllib.parse.urlparse(self.path).query)
                try:
                    since = float((query.get("since") or ["0"])[0])
                except ValueError:
                    return self._send(400, {"error": "Invalid activity timestamp."})
                if (query.get("local") or ["0"])[0] == "1":
                    data = ACTIVITY.get_data(since)
                    data["mode"] = cfg.get("mode", "primary")
                    return self._send(200, data)
                return self._send(200, cluster_activity_data(since, cfg))

            if path == "/admin/replication" and method == "GET":
                if role != "admin":
                    return self._send(403, {"error": "Admin required."})
                p_list = [
                    {
                        "url": p["url"],
                        "reachable": REPLICATOR.peer_states.get(p["url"].rstrip("/"), {}).get("reachable", False),
                        "mode": REPLICATOR.peer_states.get(p["url"].rstrip("/"), {}).get("mode", "?"),
                        "term": REPLICATOR.peer_states.get(p["url"].rstrip("/"), {}).get("term", "?"),
                    }
                    for p in cfg.get("peers", [])
                ]
                return self._send(200, {"mode": cfg.get("mode", "primary"), "term": cfg.get("term", 1), "peer_count": len(p_list), "peers": p_list, "in_sync": REPLICATOR.in_sync, "status": REPLICATOR.status})

            if path == "/snapshots" and method == "GET":
                ACTIVITY.record(who, method, path, 200, client_type=client_type)
                return self._send(200, {"today": _today(), "days": snapshot_days(), "keep_days": int(cfg.get("snapshot_keep_days") or 0)})

            if path.startswith("/snapshots/") and method == "GET":
                ACTIVITY.record(who, method, path, 200, client_type=client_type)
                day_raw, _, project_raw = path[len("/snapshots/"):].partition("/")
                day, project = urllib.parse.unquote(day_raw), urllib.parse.unquote(project_raw)
                if not DAY_RE.match(day):
                    return self._send(400, {"error": "Use a date like 2026-11-15."})
                if not project:
                    return self._send(200, snapshot_day(day))
                item = snapshot_get(day, project)
                return self._send(200 if item else 404, item or {"error": "No copy of that project was stored on that day."})

            if path.startswith("/snapshots/") and method == "PUT":
                if cfg.get("mode") == "replica":
                    return self._send(403, {"error": "Server is read-only BACKUP replica."})
                day_raw, _, project_raw = path[len("/snapshots/"):].partition("/")
                day, project = urllib.parse.unquote(day_raw), urllib.parse.unquote(project_raw)
                b = self._body()
                try:
                    n = snapshot_put(day, project, b.get("parts"), str(b.get("updated_by") or who))
                except ValueError as error:
                    ACTIVITY.record(who, method, path, 400)
                    return self._send(400, {"error": str(error)})
                log(f"Back-dated copy {day} / '{project}' saved by {who} ({n} parts).")
                ACTIVITY.record(who, "PUT", path, 200, f"{day} {project}")
                return self._send(200, {"ok": True, "day": day, "project_name": project, "part_count": n})

            if path == "/admin/snapshots/now" and method == "POST":
                if role != "admin":
                    return self._send(403, {"error": "Admin required."})
                n = snapshot_all(force=True)
                ACTIVITY.record(who, "POST", path, 200, f"Snapshot of {n} project(s)")
                return self._send(200, {"ok": True, "projects": n, "day": _today()})

            if path == "/api/snapshots-sync" and method == "GET":
                if role != "admin":
                    return self._send(403, {"error": "Admin required."})
                since = (urllib.parse.parse_qs(urllib.parse.urlparse(self.path).query).get("since") or [""])[0]
                return self._send(200, {"snapshots": snapshots_since(since)})

            if path == "/uploads" and method == "POST":
                if cfg.get("mode") == "replica":
                    return self._send(403, {"error": "Server is read-only BACKUP replica."})
                b = self._body()
                name = str(b.get("project_name") or "").strip()
                if not name:
                    return self._send(400, {"error": "Project name is required."})
                try:
                    uid = add_upload(name, str(b.get("file_name") or ""), who, b.get("parts"),
                                     already_live=(role == "admin" and bool(b.get("already_live"))))
                except ValueError as e:
                    return self._send(400, {"error": str(e)})
                log(f"Project Excel '{name}' uploaded by {who}.")
                ACTIVITY.record(who, "POST", path, 200, f"Uploaded Excel for {name}")
                return self._send(200, {"ok": True, "id": uid})

            if path == "/admin/uploads" and method == "GET":
                if role != "admin":
                    return self._send(403, {"error": "Admin required."})
                return self._send(200, {"uploads": list_uploads(), "active_window": ACTIVE_WINDOW_SECONDS})

            if path.startswith("/admin/uploads/"):
                if role != "admin":
                    return self._send(403, {"error": "Admin required."})
                rest = urllib.parse.unquote(path[len("/admin/uploads/"):]).strip("/")
                if method == "POST" and rest.endswith("/publish"):
                    if cfg.get("mode") == "replica":
                        return self._send(403, {"error": "Server is read-only BACKUP replica."})
                    res = publish_upload(rest[:-len("/publish")], who)
                    if not res:
                        return self._send(404, {"error": "Upload not found."})
                    log(f"Project Excel '{res['project_name']}' sent to all users by {who}.")
                    ACTIVITY.record(who, "POST", path, 200, f"Sent {res['project_name']} to all users")
                    return self._send(200, {"ok": True, **res})
                if method == "DELETE" and rest and "/" not in rest:
                    if cfg.get("mode") == "replica":
                        return self._send(403, {"error": "Server is read-only BACKUP replica."})
                    ok = delete_upload(rest)
                    ACTIVITY.record(who, "DELETE", path, 200 if ok else 404)
                    return self._send(200 if ok else 404, {"ok": True} if ok else {"error": "Upload not found."})

            if path == "/api/sync" and method == "GET":
                ACTIVITY.record(who, method, path, 200, client_type=client_type)
                conn = _db()
                try:
                    p_cur = conn.execute("SELECT name, parts, part_count, version, updated_at, updated_by, epoch FROM projects").fetchall()
                    p_rows = [{"name": r["name"], "parts": json.loads(r["parts"]), "part_count": r["part_count"], "version": r["version"], "updated_at": r["updated_at"], "updated_by": r["updated_by"], "epoch": r["epoch"]} for r in p_cur]
                    k_cur = conn.execute("SELECT key, value, version, updated_at, updated_by FROM kv").fetchall()
                    k_rows = [{"key": r["key"], "value": r["value"], "version": r["version"], "updated_at": r["updated_at"], "updated_by": r["updated_by"]} for r in k_cur]
                    u_rows = [{k: r[k] for k in r.keys()} for r in conn.execute("SELECT id, project_name, file_name, uploaded_by, uploaded_at, part_count, parts, status, published_by, published_at FROM uploads").fetchall()]
                    a_rows = [{k: r[k] for k in r.keys()} for r in conn.execute("SELECT project, user, last_at, last_ts, edits FROM project_activity").fetchall()]
                finally:
                    conn.close()
                return self._send(200, {"mode": cfg.get("mode", "primary"), "term": cfg.get("term", 1), "projects": p_rows, "kv": k_rows, "uploads": u_rows, "activity": a_rows})

            if path == "/projects" and method == "GET":
                ACTIVITY.record(who, method, path, 200, client_type=client_type)
                return self._send(200, list_projects())

            if path == "/state" and method == "GET":
                ACTIVITY.record(who, method, path, 200, client_type=client_type)
                keys = [k for k in (urllib.parse.parse_qs(urllib.parse.urlparse(self.path).query).get("keys") or [""])[0].split(",") if k]
                return self._send(200, {"projects": list_projects(), "kv": kv_versions(keys)})

            if path.startswith("/kv/"):
                key = urllib.parse.unquote(path[len("/kv/"):])
                if method == "GET":
                    ACTIVITY.record(who, method, path, 200, client_type=client_type)
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
                    ACTIVITY.record(who, method, path, 200, client_type=client_type)
                    d = get_project(name)
                    return self._send(200 if d else 404, d or {"error": "Not found."})
                if method == "PATCH":
                    if cfg.get("mode") == "replica":
                        return self._send(403, {"error": "Server is read-only BACKUP replica."})
                    b = self._body()
                    record_project_activity(name, who)
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
                    record_project_activity(name, who)
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
        req = urllib.request.Request(
            f"{base}/rest/v1/rpc/app_login",
            data=json.dumps({"p_username": username, "p_password": password}).encode("utf-8"),
            headers={"apikey": key, "Authorization": f"Bearer {key}", "Content-Type": "application/json"},
            method="POST",
        )
        with urllib.request.urlopen(req, timeout=7) as resp:
            res = json.loads(resp.read().decode())
    except Exception:
        cached = (cfg.get("admin_cache") or {}).get(username)
        if cached and secrets.compare_digest(cached.get("hash", ""), _pw_hash(cached.get("salt", ""), username, password)):
            return True, "Signed in (offline)."
        return False, "Online user database unreachable."
    if not isinstance(res, dict) or not res.get("ok"):
        err = (res or {}).get("error") if isinstance(res, dict) else ""
        if err == "locked":
            return False, "Too many wrong attempts. Try again in 10 minutes."
        if err == "inactive":
            return False, "Account invalid or inactive."
        return False, "Wrong username or password."
    if str((res.get("user") or {}).get("role") or "").strip().lower() != "admin":
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

CLOUDFLARED_URLS = {
    "win": "https://github.com/cloudflare/cloudflared/releases/latest/download/cloudflared-windows-amd64.exe",
    "linux": "https://github.com/cloudflare/cloudflared/releases/latest/download/cloudflared-linux-amd64",
}
IS_WINDOWS = sys.platform.startswith("win")

def _no_window_flags():
    return 0x08000000 if IS_WINDOWS else 0  # CREATE_NO_WINDOW

def cloudflared_executable():
    found = shutil.which("cloudflared")
    if found:
        return found
    name = "cloudflared.exe" if IS_WINDOWS else "cloudflared"
    # the data folder (where the app downloads it), the folder holding this program, then the bundle
    for base in (DATA_DIR, os.path.dirname(os.path.abspath(sys.executable if getattr(sys, "frozen", False) else __file__)), getattr(sys, "_MEIPASS", None)):
        if base:
            local = os.path.join(base, name)
            if os.path.isfile(local):
                return local
    return None

def download_cloudflared(progress=None):
    """Download Cloudflare's helper program (cloudflared) into the data folder. Raises RuntimeError with a plain message."""
    name = "cloudflared.exe" if IS_WINDOWS else "cloudflared"
    url = CLOUDFLARED_URLS["win" if IS_WINDOWS else "linux"]
    dest = os.path.join(DATA_DIR, name)
    tmp = dest + ".part"
    try:
        request = urllib.request.Request(url, headers={"User-Agent": "FuadsServer/3.0"})
        with urllib.request.urlopen(request, timeout=60) as response, open(tmp, "wb") as out:
            total, got = int(response.headers.get("Content-Length") or 0), 0
            while True:
                chunk = response.read(262144)
                if not chunk:
                    break
                out.write(chunk)
                got += len(chunk)
                if progress:
                    progress(got, total)
        if os.path.getsize(tmp) < 5_000_000:
            raise RuntimeError("the downloaded file is too small to be the helper")
        with open(tmp, "rb") as fh:
            head = fh.read(4)
        if IS_WINDOWS and head[:2] != b"MZ":
            raise RuntimeError("the downloaded file is not a Windows program")
        os.replace(tmp, dest)
        if not IS_WINDOWS:
            os.chmod(dest, 0o755)
        log(f"Downloaded cloudflared to {dest}")
        return dest
    except Exception as error:  # noqa: BLE001
        try:
            os.remove(tmp)
        except OSError:
            pass
        raise RuntimeError(
            f"Could not download Cloudflare's helper (cloudflared): {error}. Check the internet connection, or download "
            f"{os.path.basename(url)} from github.com/cloudflare/cloudflared/releases, rename it to {name} and put it in {DATA_DIR}."
        ) from error

class Tunnel:
    """Cloudflare tunnel. Quick mode = temporary trycloudflare.com link.
    Named mode = the user's own tunnel (token from the Cloudflare dashboard) on their own address.
    start() never blocks or raises: look at .state ('idle','downloading','starting','connected','error'),
    .url, .error and .detail instead."""
    QUICK_TIMEOUT = 60

    def __init__(self):
        self.proc = None
        self.url = None
        self.named = False
        self.error = ""
        self.detail = ""
        self.state = "idle"
        self.tail = deque(maxlen=14)
        self._gen = 0
        self._attempt = 0
        self._args = None
        self._lock = threading.Lock()

    @property
    def running(self):
        return self.state in ("downloading", "starting", "connected")

    def start(self, port, token=None, public_url=None):
        with self._lock:
            if self.running:
                return
            self._gen += 1
            gen = self._gen
            self.url, self.error, self.detail = None, "", "Starting..."
            self.state = "starting"
            self.named = bool(token)
            self._attempt, self._args = 0, (port, token, public_url)
            self.tail.clear()
        threading.Thread(target=self._boot, args=(gen, port, token, public_url), daemon=True).start()

    @staticmethod
    def explain(line):
        """Turn cloudflared's raw error into something a person can act on."""
        low = (line or "").lower()
        if "no such host" in low or "lookup" in low:
            return ("This PC could not look up Cloudflare (api.trycloudflare.com) - a DNS or internet problem. "
                    "Check the internet connection, try DNS 1.1.1.1 or 8.8.8.8, or use your own Cloudflare tunnel / Tailscale instead.")
        if "timeout" in low or "timed out" in low or "refused" in low or "unreachable" in low:
            return ("Cloudflare could not be reached (blocked or very slow network). Try again, use another network, "
                    "or use your own Cloudflare tunnel / Tailscale instead.")
        if "429" in low or "too many" in low:
            return "Cloudflare is limiting new temporary links right now. Wait a minute and try again."
        return "Cloudflare would not create a temporary link: " + (line or "")[-160:]

    def _fail(self, gen, message):
        if gen != self._gen:
            return
        self.error, self.state, self.url = message, "error", None
        log(f"Cloudflare tunnel problem: {message}")

    def _boot(self, gen, port, token, public_url):
        exe = cloudflared_executable()
        if not exe:
            self.state, self.detail = "downloading", "Downloading Cloudflare's helper (one time, ~30 MB)..."
            try:
                exe = download_cloudflared(lambda got, total: setattr(self, "detail", f"Downloading Cloudflare's helper... {got // 1048576} MB" + (f" of {total // 1048576} MB" if total else "")))
            except RuntimeError as error:
                return self._fail(gen, str(error))
            if gen != self._gen:
                return
            self.state, self.detail = "starting", "Starting..."
        env = None
        if token:
            # named tunnel: the token goes in the environment so it never shows in the process list
            token = str(token).strip()
            if token.lower().startswith("cloudflared"):
                token = token.split()[-1]  # user pasted the whole "cloudflared service install <token>" command
            env = dict(os.environ, TUNNEL_TOKEN=token)
            cmd = [exe, "tunnel", "--no-autoupdate", "run"]
        else:
            # 127.0.0.1 (not "localhost": that can resolve to IPv6 while the server listens on IPv4 only -> blank 502 page),
            # http2 over TCP (many networks block the default QUIC/UDP), IPv4 edge, and an empty home folder so an old
            # ~/.cloudflared/config.yml cannot stop a quick tunnel from starting.
            cf_home = os.path.join(DATA_DIR, "cf_home")
            os.makedirs(cf_home, exist_ok=True)
            env = dict(os.environ, HOME=cf_home, USERPROFILE=cf_home)
            cmd = [exe, "tunnel", "--url", f"http://127.0.0.1:{port}", "--no-autoupdate", "--protocol", "http2", "--edge-ip-version", "4"]
        try:
            proc = subprocess.Popen(cmd, stdout=subprocess.PIPE, stderr=subprocess.STDOUT, text=True, errors="replace",
                                    creationflags=_no_window_flags(), env=env, stdin=subprocess.DEVNULL)
        except OSError as error:
            return self._fail(gen, f"Could not start cloudflared: {error}")
        if gen != self._gen:
            proc.terminate()
            return
        self.proc = proc
        threading.Thread(target=self._read, args=(gen, proc, public_url), daemon=True).start()
        if not token:
            threading.Thread(target=self._watch_timeout, args=(gen, proc), daemon=True).start()

    def _watch_timeout(self, gen, proc):
        deadline = time.time() + self.QUICK_TIMEOUT
        while time.time() < deadline:
            if gen != self._gen or self.state != "starting":
                return
            time.sleep(1)
        if gen == self._gen and self.state == "starting":
            last = self.tail[-1] if self.tail else ""
            self._fail(gen, "No link after 60 seconds. " + self.explain(last or "timeout"))
            try:
                proc.terminate()
            except OSError:
                pass

    def _read(self, gen, proc, public_url=None):
        r = re.compile(r"https://(?!api\.)[a-z0-9-]+\.trycloudflare\.com")  # api.trycloudflare.com is Cloudflare's own address, shown in error messages
        for line in proc.stdout:
            if gen != self._gen:
                return
            line = line.rstrip()
            if line:
                self.tail.append(line)
            low = line.lower()
            if self.named:
                if "registered tunnel connection" in low and self.state != "connected":
                    self.url = (public_url or "").rstrip("/") or "connected"
                    self.state, self.detail = "connected", "Connected"
                    log(f"Cloudflare tunnel connected: {self.url}")
                elif any(k in low for k in ("unauthorized", "invalid tunnel secret", "token", "failed to unmarshal")) and ("err" in low or "fail" in low or "invalid" in low):
                    self.error = line[-200:]
                    log(f"Cloudflare tunnel problem: {self.error}")
                continue
            m = r.search(line)
            if m and not self.url:
                self.url = m.group(0)
                self.state, self.detail = "connected", "Connected"
                log(f"Public link active: {self.url}")
            elif " err " in low or "failed to" in low:
                self.detail = line[-160:]
        # cloudflared ended
        if gen == self._gen and self.state != "idle":
            if self.url and self.state == "connected":
                self._fail(gen, "The link dropped (cloudflared stopped). Start it again for a new link.")
                return
            quick_fail = next((l for l in reversed(self.tail) if "failed to request quick tunnel" in l.lower()), "")
            if not self.named and self._attempt < 3 and self._args:
                # quick links fail now and then (DNS hiccup, rate limit): try again by itself
                self._attempt += 1
                self.detail = f"Cloudflare did not answer, trying again ({self._attempt}/3)..."
                time.sleep(3)
                if gen == self._gen:
                    self.state = "starting"
                    self._boot(gen, *self._args)
                return
            last = quick_fail or (self.tail[-1] if self.tail else "")
            self._fail(gen, self.explain(last) if quick_fail or not self.named else "cloudflared stopped before connecting. " + last[-200:])

    def stop(self):
        with self._lock:
            self._gen += 1
            proc, self.proc = self.proc, None
            self.url, self.state, self.error, self.detail = None, "idle", "", ""
        if proc:
            try:
                proc.terminate()
            except OSError:
                pass

def tailscale_executable():
    found = shutil.which("tailscale.exe") or shutil.which("tailscale")
    if found:
        return found
    if sys.platform.startswith("win"):
        roots = (
            os.environ.get("ProgramFiles", r"C:\Program Files"),
            os.environ.get("ProgramFiles(x86)", r"C:\Program Files (x86)"),
            os.environ.get("LOCALAPPDATA", ""),
        )
        for root in roots:
            if not root:
                continue
            for directory in ("Tailscale", "Tailscale IPN", os.path.join("Programs", "Tailscale")):
                candidate = os.path.join(root, directory, "tailscale.exe")
                if os.path.isfile(candidate):
                    return candidate

        import winreg

        service_key = r"SYSTEM\CurrentControlSet\Services\Tailscale"
        try:
            with winreg.OpenKey(winreg.HKEY_LOCAL_MACHINE, service_key) as key:
                image_path, _ = winreg.QueryValueEx(key, "ImagePath")
        except FileNotFoundError:
            image_path = ""
        if image_path:
            match = re.match(r'\s*"([^"]+)"|(\S+)', image_path)
            if match:
                service_executable = match.group(1) or match.group(2)
                candidate = os.path.join(os.path.dirname(service_executable), "tailscale.exe")
                if os.path.isfile(candidate):
                    return candidate
    return None

def tailscale_run(*args):
    executable = tailscale_executable()
    if not executable:
        raise RuntimeError(
            "Tailscale is not installed. Install the official Windows app, sign in, "
            f"and enable Funnel for your tailnet: {TAILSCALE_DOWNLOAD_URL}"
        )
    try:
        result = subprocess.run(
            [executable, *args],
            capture_output=True,
            text=True,
            stdin=subprocess.DEVNULL,
            timeout=30,
        )
    except subprocess.TimeoutExpired as error:
        output = error.stderr or error.stdout or ""
        if isinstance(output, bytes):
            output = output.decode(errors="replace")
        detail = output.strip()
        message = "Tailscale did not respond within 30 seconds."
        if detail:
            message = f"{message} {detail}"
        raise RuntimeError(message) from error
    if result.returncode:
        detail = (result.stderr or result.stdout).strip()
        raise RuntimeError(detail or f"Tailscale exited with code {result.returncode}.")
    return result.stdout.strip()

def enable_tailscale_funnel(port):
    status = tailscale_run("status", "--json")
    try:
        status_data = json.loads(status)
        self_status = status_data.get("Self") or {}
        dns_name = self_status.get("DNSName", "").rstrip(".")
        backend_state = str(status_data.get("BackendState", "")).lower()
    except (ValueError, AttributeError, TypeError) as error:
        raise RuntimeError(f"Tailscale returned an invalid status response: {error}") from error
    if backend_state and backend_state != "running":
        raise RuntimeError("Tailscale is installed but not connected. Open Tailscale, sign in, and try again.")
    output = tailscale_run("funnel", "--bg", "--yes", "--https=443", f"http://127.0.0.1:{port}")
    if not dns_name:
        match = re.search(r"https://[a-z0-9-]+(?:\.[a-z0-9-]+)*\.ts\.net", output, re.I)
        dns_name = match.group(0).removeprefix("https://") if match else ""
    if not dns_name:
        raise RuntimeError("Funnel was enabled, but Tailscale did not report its stable DNS name. Check `tailscale status --json`.")
    return f"https://{dns_name}"

def disable_tailscale_funnel():
    tailscale_run("funnel", "reset")

# ---------------------------------------------------------------------------------------------
# Start with Windows
#   Three layers so it still starts when one of them is blocked or stale:
#   1. Windows Task Scheduler "at sign-in" task (restarts the app if it crashes, no battery limits)
#   2. the classic HKCU "Run" entry
#   3. the StartupApproved flag (Task Manager > Startup apps) is cleared when enabling
#   A second copy that starts while the first is running exits at once (single-instance guard in the app).
# ---------------------------------------------------------------------------------------------
STARTUP_NAME = "FuadsServer"
LEGACY_RUN_VALUES = ("FuadProjectServerBackground",)
RUN_KEY = r"Software\Microsoft\Windows\CurrentVersion\Run"
APPROVED_KEY = r"Software\Microsoft\Windows\CurrentVersion\Explorer\StartupApproved\Run"

def startup_command():
    """The command Windows should run at sign-in."""
    if getattr(sys, "frozen", False):
        return [sys.executable, "--autostart"]
    exe = sys.executable
    pythonw = os.path.join(os.path.dirname(exe), "pythonw.exe")
    if IS_WINDOWS and os.path.isfile(pythonw):
        exe = pythonw  # no console window
    return [exe, os.path.abspath(sys.argv[0]), "--autostart"]

def _first_path(command):
    command = (command or "").strip()
    if command.startswith('"'):
        return command[1:].split('"', 1)[0]
    return command.split(" ", 1)[0]

def _xml_escape(text):
    return str(text).replace("&", "&amp;").replace("<", "&lt;").replace(">", "&gt;").replace('"', "&quot;")

def startup_task_xml(command, user=""):
    exe, args = command[0], subprocess.list2cmdline(command[1:])
    user_xml = f"<UserId>{_xml_escape(user)}</UserId>" if user else ""
    return f"""<?xml version="1.0" encoding="UTF-16"?>
<Task version="1.4" xmlns="http://schemas.microsoft.com/windows/2004/02/mit/task">
  <RegistrationInfo><Description>Starts Fuad's Server when you sign in to Windows.</Description></RegistrationInfo>
  <Triggers><LogonTrigger><Enabled>true</Enabled>{user_xml}<Delay>PT10S</Delay></LogonTrigger></Triggers>
  <Principals><Principal id="Author">{user_xml}<LogonType>InteractiveToken</LogonType><RunLevel>LeastPrivilege</RunLevel></Principal></Principals>
  <Settings>
    <MultipleInstancesPolicy>IgnoreNew</MultipleInstancesPolicy>
    <DisallowStartIfOnBatteries>false</DisallowStartIfOnBatteries>
    <StopIfGoingOnBatteries>false</StopIfGoingOnBatteries>
    <AllowHardTerminate>true</AllowHardTerminate>
    <StartWhenAvailable>true</StartWhenAvailable>
    <RunOnlyIfNetworkAvailable>false</RunOnlyIfNetworkAvailable>
    <AllowStartOnDemand>true</AllowStartOnDemand>
    <Enabled>true</Enabled>
    <Hidden>false</Hidden>
    <ExecutionTimeLimit>PT0S</ExecutionTimeLimit>
    <Priority>7</Priority>
    <RestartOnFailure><Interval>PT1M</Interval><Count>5</Count></RestartOnFailure>
  </Settings>
  <Actions Context="Author"><Exec><Command>{_xml_escape(exe)}</Command><Arguments>{_xml_escape(args)}</Arguments><WorkingDirectory>{_xml_escape(os.path.dirname(exe))}</WorkingDirectory></Exec></Actions>
</Task>
"""

def _schtasks(*args):
    return subprocess.run(["schtasks", *args], capture_output=True, text=True, timeout=20, creationflags=_no_window_flags(), stdin=subprocess.DEVNULL)

def _task_exists():
    try:
        return _schtasks("/Query", "/TN", STARTUP_NAME).returncode == 0
    except (OSError, subprocess.TimeoutExpired):
        return False

def startup_status():
    """What Windows will really do at sign-in, in plain words."""
    info = {"supported": IS_WINDOWS, "enabled": False, "run_entry": False, "task": False, "blocked": False,
            "path_ok": True, "command": "", "problem": "", "log": os.path.join(DATA_DIR, "background_server.log")}
    if not IS_WINDOWS:
        info["problem"] = "Starting with Windows is only available on Windows."
        return info
    import winreg
    command = ""
    try:
        with winreg.OpenKey(winreg.HKEY_CURRENT_USER, RUN_KEY) as key:
            command = str(winreg.QueryValueEx(key, STARTUP_NAME)[0])
            info["run_entry"] = True
    except FileNotFoundError:
        pass
    try:
        with winreg.OpenKey(winreg.HKEY_CURRENT_USER, APPROVED_KEY) as key:
            flag = winreg.QueryValueEx(key, STARTUP_NAME)[0]
            if info["run_entry"] and isinstance(flag, (bytes, bytearray)) and flag and (flag[0] & 1):
                info["blocked"] = True
    except FileNotFoundError:
        pass
    info["task"] = _task_exists()
    info["command"] = command
    info["enabled"] = info["run_entry"] or info["task"]
    if command:
        info["path_ok"] = os.path.isfile(_first_path(command))
    if info["enabled"]:
        if info["run_entry"] and not info["path_ok"]:
            info["problem"] = "The saved program path no longer exists (the app was moved or deleted). Switch it off and on again to repair."
        elif info["blocked"] and not info["task"]:
            info["problem"] = "Windows has this startup entry switched off (Task Manager > Startup apps). Switch it off and on again here to turn it back on."
    return info

def set_background_startup(enabled):
    """Turn 'start with Windows' on or off. Returns a short note about anything that only partly worked."""
    if not IS_WINDOWS:
        raise RuntimeError("Starting with Windows is only available on Windows.")
    import winreg
    command = startup_command()
    notes = []
    with winreg.CreateKey(winreg.HKEY_CURRENT_USER, RUN_KEY) as key:
        for legacy in LEGACY_RUN_VALUES:
            try:
                winreg.DeleteValue(key, legacy)
            except FileNotFoundError:
                pass
        if enabled:
            winreg.SetValueEx(key, STARTUP_NAME, 0, winreg.REG_SZ, subprocess.list2cmdline(command))
        else:
            try:
                winreg.DeleteValue(key, STARTUP_NAME)
            except FileNotFoundError:
                pass
    with winreg.CreateKey(winreg.HKEY_CURRENT_USER, APPROVED_KEY) as key:
        if enabled:
            winreg.SetValueEx(key, STARTUP_NAME, 0, winreg.REG_BINARY, bytes([2]) + bytes(11))  # 02 = enabled
        else:
            try:
                winreg.DeleteValue(key, STARTUP_NAME)
            except FileNotFoundError:
                pass
    if enabled:
        xml_path = os.path.join(DATA_DIR, "startup_task.xml")
        try:
            user = (os.environ.get("USERDOMAIN", "") + "\\" + os.environ.get("USERNAME", "")).strip("\\")
            with open(xml_path, "w", encoding="utf-16") as fh:
                fh.write(startup_task_xml(command, user))
            result = _schtasks("/Create", "/TN", STARTUP_NAME, "/XML", xml_path, "/F")
            if result.returncode != 0:
                notes.append("Task Scheduler refused the task (" + (result.stderr or result.stdout).strip()[:120] + "); the Run entry is still in place.")
        except (OSError, subprocess.TimeoutExpired) as error:
            notes.append(f"Could not create the Task Scheduler task ({error}); the Run entry is still in place.")
    else:
        try:
            _schtasks("/Delete", "/TN", STARTUP_NAME, "/F")
        except (OSError, subprocess.TimeoutExpired):
            pass
    log(f"Start with Windows {'enabled' if enabled else 'disabled'}: {subprocess.list2cmdline(command)}")
    return " ".join(notes)

def heal_startup_entry():
    """If 'start with Windows' is on but points at an old copy of this program, point it at this one."""
    try:
        status = startup_status()
        if status["enabled"] and (status["problem"] or (status["command"] and os.path.normcase(_first_path(status["command"])) != os.path.normcase(startup_command()[0]))):
            set_background_startup(True)
            log("Start-with-Windows entry repaired to point at this program.")
    except Exception as error:  # noqa: BLE001
        log(f"Could not check the start-with-Windows entry: {error}")

# ---------------------------------------------------------------------------------------------
# Runtime: everything that keeps the server reachable (server + temporary link + own Cloudflare tunnel + Funnel)
# Used by the desktop app and by the silent start at sign-in.
# ---------------------------------------------------------------------------------------------
class Runtime:
    def __init__(self):
        self.server = ServerCore()
        self.temp = Tunnel()
        self.named = Tunnel()
        self.funnel_active = False
        self.funnel_error = ""
        self.want_server = True
        self.start_error = ""
        self.booted = False
        self._stop = threading.Event()
        self._next = {"server": 0.0, "named": 0.0, "temp": 0.0, "funnel": 0.0}

    def boot(self):
        cfg = load_config()
        init_and_migrate_db()
        REPLICATOR.start()
        start_backups()
        self.booted = True
        ok, error = self.start_server(cfg.get("port"))
        threading.Thread(target=self._watchdog, daemon=True).start()
        return ok, error

    def start_server(self, port=None):
        cfg = load_config()
        port = int(port or cfg.get("port") or DEFAULT_PORT)
        if not 1024 <= port <= 65535:
            return False, "Choose a port between 1024 and 65535."
        self.want_server = True
        ok, error = self.server.start(port)
        self.start_error = "" if ok else str(error)
        if ok and cfg.get("port") != port:
            cfg["port"] = port
            save_config(cfg)
        return ok, error

    def stop_server(self):
        self.want_server = False
        self.temp.stop()
        self.named.stop()
        self._funnel_off()
        self.server.stop()

    def start_temp(self):
        if not self.server.running:
            raise RuntimeError("Start the server first.")
        self.temp.start(self.server.port)

    def start_named(self):
        cfg = load_config()
        if not cfg.get("cloudflare_token"):
            raise RuntimeError("Save your Cloudflare tunnel token first.")
        if not self.server.running:
            raise RuntimeError("Start the server first.")
        self.named.start(self.server.port, cfg["cloudflare_token"], cfg.get("cloudflare_url"))

    def funnel_on(self):
        if not self.server.running:
            raise RuntimeError("Start the server first.")
        cfg = load_config()
        url = enable_tailscale_funnel(self.server.port)
        self.funnel_active, self.funnel_error = True, ""
        cfg["custom_url"], cfg["tailscale_funnel"] = url, True
        save_config(cfg)
        return url

    def _funnel_off(self):
        if self.funnel_active:
            try:
                disable_tailscale_funnel()
            except (OSError, RuntimeError, subprocess.TimeoutExpired) as error:
                log(f"Could not stop Tailscale Funnel: {error}")
            self.funnel_active = False

    def funnel_off(self):
        cfg = load_config()
        disable_tailscale_funnel()
        self.funnel_active = False
        cfg["tailscale_funnel"], cfg["auto_tunnel"] = False, False
        if ".ts.net" in str(cfg.get("custom_url", "")):
            cfg["custom_url"] = ""
        save_config(cfg)

    def _due(self, name, wait):
        now = time.monotonic()
        if now >= self._next[name]:
            self._next[name] = now + wait
            return True
        return False

    def _watchdog(self):
        while not self._stop.is_set():
            try:
                cfg = load_config()
                if self.want_server and not self.server.running and self._due("server", 15):
                    ok, error = self.start_server()
                    log("Server restarted by the watchdog." if ok else f"Server could not start; will retry: {error}")
                if self.server.running:
                    if cfg.get("cloudflare_token") and cfg.get("cloudflare_auto") and not self.named.running and self._due("named", 30):
                        self.start_named()
                    if cfg.get("temp_link_auto") and not cfg.get("cloudflare_token") and not self.temp.running and self._due("temp", 60):
                        self.start_temp()
                    if cfg.get("auto_tunnel") and tailscale_executable() and self._due("funnel", 60):
                        try:
                            self.funnel_on()
                            log(f"Stable Tailscale Funnel active at {cfg.get('custom_url')}")
                        except (OSError, RuntimeError, subprocess.TimeoutExpired) as error:
                            self.funnel_active, self.funnel_error = False, str(error)
            except Exception as error:  # noqa: BLE001
                log(f"Watchdog error: {error}")
            self._stop.wait(2)

    def shutdown(self):
        self._stop.set()
        self.stop_server()

    # ---- what the control panel shows ----
    def best_url(self, cfg=None):
        cfg = cfg or load_config()
        return public_base_url(cfg) or (self.temp.url if self.temp.state == "connected" else "") or \
            (f"http://{lan_addresses()[0]}:{self.server.port or cfg.get('port', DEFAULT_PORT)}")

    def connection_code(self):
        cfg = load_config()
        return make_code(self.best_url(cfg), cfg["view_key"], cfg["admin_key"])

    def status(self):
        cfg = load_config()
        port = self.server.port or int(cfg.get("port") or DEFAULT_PORT)
        def tun(t):
            return {"state": t.state, "url": t.url or "", "error": t.error, "detail": t.detail, "running": t.running}
        db_size = os.path.getsize(DB_PATH) if os.path.exists(DB_PATH) else 0
        return {
            "server": {"running": self.server.running, "port": port, "mode": cfg.get("mode", "primary"), "term": cfg.get("term", 1),
                       "uptime": max(0, int(time.time() - SERVER_START_TIME)), "error": self.start_error, "db_size": db_size, "db_path": DB_PATH, "data_dir": DATA_DIR},
            "links": {"local": f"http://127.0.0.1:{port}", "lan": [f"http://{a}:{port}" for a in lan_addresses()],
                      "custom": cfg.get("custom_url", ""), "best": self.best_url(cfg)},
            "temp": tun(self.temp),
            "named": {**tun(self.named), "token_saved": bool(cfg.get("cloudflare_token")), "public_url": cfg.get("cloudflare_url", ""), "auto": bool(cfg.get("cloudflare_auto"))},
            "funnel": {"active": self.funnel_active or bool(cfg.get("tailscale_funnel")), "error": self.funnel_error, "installed": tailscale_executable() is not None, "auto": bool(cfg.get("auto_tunnel"))},
            "temp_auto": bool(cfg.get("temp_link_auto")),
            "keys": {"admin": cfg["admin_key"], "view": cfg["view_key"]},
            "code": self.connection_code(),
            "peers": [{"url": p["url"], "reachable": REPLICATOR.peer_states.get(p["url"].rstrip("/"), {}).get("reachable", False),
                       "mode": REPLICATOR.peer_states.get(p["url"].rstrip("/"), {}).get("mode", "?")} for p in cfg.get("peers", [])],
            "replication": {"status": REPLICATOR.status, "in_sync": REPLICATOR.in_sync},
            "snapshot_keep_days": int(cfg.get("snapshot_keep_days") or 0),
        }
