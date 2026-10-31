#!/usr/bin/env python3
"""
Fuad's Project Server
-------------------
Multi-PC High-Availability Sync Server for FAAH Mentor Workstation.
Supports 1 Primary + Multiple Backup Replicas, rolling backups,
failover term election, and embedded workflow telemetry.
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
from collections import deque
from datetime import datetime

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

def log(msg):
    ts = datetime.now().strftime("%H:%M:%S")
    line = f"{ts}  {msg}"
    _log.append(line)
    if _BACKGROUND_LOG is not None:
        _BACKGROUND_LOG.write(line + "\n")
        _BACKGROUND_LOG.flush()
    if _BACKGROUND_LOG is None and sys.stdout is not None and not getattr(sys, "frozen", False):
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
    server_version = "FaahProjectServer/2.6"

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

def cloudflared_executable():
    found = shutil.which("cloudflared")
    if found:
        return found
    name = "cloudflared.exe" if sys.platform.startswith("win") else "cloudflared"
    # the data folder, the folder holding this program (the release zip ships cloudflared.exe next to it), then the bundle
    for base in (DATA_DIR, os.path.dirname(os.path.abspath(sys.executable if getattr(sys, "frozen", False) else __file__)), getattr(sys, "_MEIPASS", None)):
        if base:
            local = os.path.join(base, name)
            if os.path.isfile(local):
                return local
    return None

class Tunnel:
    """Cloudflare tunnel. Quick mode = temporary trycloudflare.com link.
    Named mode = the user's own tunnel (token from the Cloudflare dashboard) on their own address."""
    def __init__(self):
        self.proc = None
        self.url = None
        self.named = False
        self.error = ""

    @property
    def running(self):
        return self.proc is not None and self.proc.poll() is None

    def start(self, port, token=None, public_url=None):
        exe = cloudflared_executable()
        if not exe:
            raise RuntimeError("cloudflared helper not found. Put cloudflared.exe in the same folder as this program (the release zip includes it) or install Cloudflare's 'cloudflared'.")
        self.url, self.error = None, ""
        flags = 0x08000000 if sys.platform.startswith("win") else 0
        env = None
        if token:
            # named tunnel: the token goes in the environment so it never shows in the process list
            token = str(token).strip()
            if token.lower().startswith("cloudflared"):
                token = token.split()[-1]  # user pasted the whole "cloudflared service install <token>" command
            env = dict(os.environ, TUNNEL_TOKEN=token)
            cmd = [exe, "tunnel", "--no-autoupdate", "run"]
            self.named = True
        else:
            cmd = [exe, "tunnel", "--url", f"http://localhost:{port}", "--no-autoupdate"]
            self.named = False
        self.proc = subprocess.Popen(cmd, stdout=subprocess.PIPE, stderr=subprocess.STDOUT, text=True, creationflags=flags, env=env)
        threading.Thread(target=self._read, args=(self.proc, public_url), daemon=True).start()

    def _read(self, proc, public_url=None):
        r = re.compile(r"https://[a-z0-9-]+\.trycloudflare\.com")
        for line in proc.stdout:
            if proc is not self.proc:
                continue
            if self.named:
                low = line.lower()
                if "registered tunnel connection" in low and not self.url:
                    self.url = (public_url or "").rstrip("/") or "connected"
                    log(f"Cloudflare tunnel connected: {self.url}")
                elif any(k in low for k in ("unauthorized", "invalid tunnel secret", "token", "failed to unmarshal")) and ("err" in low or "fail" in low or "invalid" in low):
                    self.error = line.strip()[-200:]
                    log(f"Cloudflare tunnel problem: {self.error}")
                continue
            m = r.search(line)
            if m and not self.url:
                self.url = m.group(0)
                log(f"Public link active: {self.url}")

    def stop(self):
        if self.proc:
            try:
                self.proc.terminate()
            except Exception:
                pass
        self.proc, self.url = None, None

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

def background_startup_enabled():
    if not sys.platform.startswith("win"):
        return False
    import winreg

    run_key = r"Software\Microsoft\Windows\CurrentVersion\Run"
    try:
        with winreg.OpenKey(winreg.HKEY_CURRENT_USER, run_key) as key:
            winreg.QueryValueEx(key, "FuadProjectServerBackground")
    except FileNotFoundError:
        return False
    return True

def set_background_startup(enabled):
    if not sys.platform.startswith("win"):
        raise RuntimeError("Background startup is supported only on Windows.")
    import winreg

    run_key = r"Software\Microsoft\Windows\CurrentVersion\Run"
    with winreg.CreateKey(winreg.HKEY_CURRENT_USER, run_key) as key:
        if enabled:
            command = [sys.executable]
            if not getattr(sys, "frozen", False):
                command.append(os.path.abspath(__file__))
            command.append("--background-server")
            winreg.SetValueEx(
                key,
                "FuadProjectServerBackground",
                0,
                winreg.REG_SZ,
                subprocess.list2cmdline(command),
            )
        else:
            try:
                winreg.DeleteValue(key, "FuadProjectServerBackground")
            except FileNotFoundError:
                pass

def request_background_server_stop():
    with open(BACKGROUND_STOP_PATH, "w", encoding="utf-8") as stop_file:
        stop_file.write("stop\n")

def run_background_server():
    global _BACKGROUND_LOG
    with open(BACKGROUND_LOG_PATH, "a", encoding="utf-8") as log_file:
        _BACKGROUND_LOG = log_file
        server = ServerCore()
        cf_tunnel = Tunnel()
        funnel_active = False
        cfg = None
        try:
            try:
                os.remove(BACKGROUND_STOP_PATH)
            except FileNotFoundError:
                pass
            cfg = load_config()
            init_and_migrate_db()
            REPLICATOR.start()
            start_backups()
            port = int(cfg.get("port", DEFAULT_PORT))
            ok, error = server.start(port)
            if not ok:
                raise RuntimeError(f"Could not start the background server on port {port}: {error}")

            next_funnel_refresh = 0.0
            next_cf_check = 0.0
            log(f"Background server started on port {port}.")
            while not os.path.exists(BACKGROUND_STOP_PATH):
                if cfg.get("cloudflare_token") and cfg.get("cloudflare_auto") and time.monotonic() >= next_cf_check:
                    next_cf_check = time.monotonic() + 30
                    if not cf_tunnel.running:
                        try:
                            cf_tunnel.start(port, cfg["cloudflare_token"], cfg.get("cloudflare_url"))
                            log("Starting your Cloudflare tunnel in the background.")
                        except (OSError, RuntimeError) as error:
                            log(f"Cloudflare tunnel could not start; will retry: {error}")
                if time.monotonic() >= next_funnel_refresh:
                    try:
                        url = enable_tailscale_funnel(port)
                        funnel_active = True
                        if not cfg.get("custom_url") or ".ts.net" in cfg["custom_url"]:
                            cfg["custom_url"] = url
                        cfg["tailscale_funnel"] = True
                        cfg["auto_tunnel"] = True
                        if not save_config(cfg):
                            raise RuntimeError(f"Could not save the Funnel settings to {CONFIG_PATH}.")
                        log(f"Stable Tailscale Funnel active at {url}")
                        next_funnel_refresh = time.monotonic() + 60
                    except (OSError, RuntimeError, subprocess.TimeoutExpired) as error:
                        funnel_active = False
                        log(f"Tailscale Funnel is not ready; will retry in 60 seconds: {error}")
                        next_funnel_refresh = time.monotonic() + 60
                time.sleep(2)
            log("Background server stop requested.")
        except Exception as error:
            log(f"Background server failed: {error}\n{traceback.format_exc().rstrip()}")
            return 1
        finally:
            if funnel_active:
                try:
                    disable_tailscale_funnel()
                    if cfg is not None:
                        cfg["tailscale_funnel"] = False
                        if not save_config(cfg):
                            log(f"Could not save the stopped Funnel state to {CONFIG_PATH}.")
                except (OSError, RuntimeError, subprocess.TimeoutExpired) as error:
                    log(f"Could not stop Tailscale Funnel: {error}")
            cf_tunnel.stop()
            server.stop()
            _BACKGROUND_LOG = None
    try:
        os.remove(BACKGROUND_STOP_PATH)
    except FileNotFoundError:
        pass
    return 0

def server_icon_path():
    for base in (getattr(sys, "_MEIPASS", None), os.path.dirname(os.path.abspath(sys.executable)),
                 os.path.dirname(os.path.abspath(__file__)), os.getcwd()):
        if base:
            for icon_name in ("fuads_project_server.ico", "fuad's_project_server.ico"):
                path = os.path.join(base, icon_name)
                if os.path.isfile(path):
                    return path
    return None

def run_app(server, tunnel, cfg):
    import tkinter as tk
    from tkinter import ttk
    from tkinter import messagebox
    import pystray
    from PIL import Image, ImageDraw

    root = tk.Tk()
    root.title("Admin Login")
    root.geometry("390x350")
    root.minsize(360, 300)
    if sys.platform.startswith("win"):
        try:
            import ctypes
            ctypes.windll.shell32.SetCurrentProcessExplicitAppUserModelID("Fuad.ProjectServer.1")
        except (AttributeError, OSError):
            pass
    icon_path = server_icon_path()
    if icon_path:
        try:
            root.iconbitmap(default=icon_path)
        except tk.TclError:
            pass
    style = ttk.Style(root)
    if "clam" in style.theme_names():
        style.theme_use("clam")
    root.configure(bg="#0f172a")
    root.option_add("*Font", ("Segoe UI", 9))
    root.option_add("*Button.Background", "#e7edf4")
    root.option_add("*Button.ActiveBackground", "#d6e2ee")
    root.option_add("*Button.Relief", "flat")
    root.option_add("*Button.BorderWidth", 0)
    root.option_add("*Button.Padding", 8)
    root.option_add("*Entry.Background", "#ffffff")
    root.option_add("*Text.Background", "#ffffff")
    style.configure("TNotebook", padding=6, background="#eef3f8", borderwidth=0)
    style.configure("TNotebook.Tab", padding=(16, 8), font=("Segoe UI", 10, "bold"), background="#dce6f0")
    style.configure("Header.TLabel", font=("Segoe UI", 18, "bold"), foreground="#1a2f42", background="#eef3f8")
    style.configure("Subheader.TLabel", font=("Segoe UI", 10), foreground="#64748b")
    style.configure("Login.TFrame", background="#0f172a")
    style.configure("LoginHeader.TLabel", font=("Segoe UI", 18, "bold"), foreground="#f8fafc", background="#0f172a")
    style.configure("LoginSubheader.TLabel", font=("Segoe UI", 10), foreground="#a6b3c4", background="#0f172a")
    style.configure("Login.TLabel", foreground="#e2e8f0", background="#0f172a")
    style.configure("Section.TLabelframe.Label", font=("Segoe UI", 10, "bold"), foreground="#1a2f42")
    style.configure("App.TFrame", background="#eef3f8")
    style.configure("TLabel", background="#eef3f8", foreground="#1f2937", font=("Segoe UI", 9))
    style.configure("TButton", padding=(10, 6), font=("Segoe UI", 9, "bold"))
    root.minsize(360, 300)

    main_frame = ttk.Frame(root, style="App.TFrame")
    login_frame = ttk.Frame(root, padding=24, style="Login.TFrame")
    login_frame.pack(fill="both", expand=True)
    root.resizable(False, False)
    root.grab_set()
    ttk.Label(login_frame, text="Admin Login", style="LoginHeader.TLabel").pack(anchor="w")
    ttk.Label(login_frame, text="Sign in with your website administrator account.", style="LoginSubheader.TLabel").pack(anchor="w", pady=(4, 18))
    ttk.Label(login_frame, text="Username", style="Login.TLabel").pack(anchor="w")
    login_user = ttk.Entry(login_frame, width=38)
    login_user.pack(fill="x", pady=(3, 12))
    ttk.Label(login_frame, text="Password", style="Login.TLabel").pack(anchor="w")
    login_password = ttk.Entry(login_frame, width=38, show="•")
    login_password.pack(fill="x", pady=(3, 8))
    login_error = tk.StringVar()
    ttk.Label(login_frame, textvariable=login_error, foreground="#bd3b3b", wraplength=330).pack(anchor="w", pady=(0, 8))
    auth_finished = tk.BooleanVar(value=False)
    admin_username = ""

    def authenticate_server_admin(_event=None):
        nonlocal admin_username
        username = login_user.get().strip().lower()
        accepted, message = verify_admin_login(username, login_password.get())
        if not accepted:
            login_error.set(message)
            login_password.focus_set()
            return
        root.grab_release()
        root.unbind("<Return>")
        login_frame.destroy()
        admin_username = username
        root.configure(bg="#eef3f8")
        root.title(APP_NAME)
        root.geometry("950x760")
        root.minsize(800, 650)
        root.resizable(True, True)
        if icon_path:
            try:
                root.iconbitmap(default=icon_path)
            except tk.TclError:
                pass
        main_frame.pack(fill="both", expand=True)
        auth_finished.set(True)

    def cancel_login():
        auth_finished.set(True)
        root.destroy()

    login_buttons = ttk.Frame(login_frame)
    login_buttons.pack(fill="x", pady=(4, 0))
    ttk.Button(login_buttons, text="Cancel", command=cancel_login).pack(side="right")
    ttk.Button(login_buttons, text="Sign in", command=authenticate_server_admin).pack(side="right", padx=(0, 8))
    root.bind("<Return>", authenticate_server_admin)
    root.protocol("WM_DELETE_WINDOW", cancel_login)
    login_user.focus_set()
    root.update_idletasks()
    root.lift()
    root.focus_force()
    root.wait_variable(auth_finished)
    if not root.winfo_exists():
        return
    try:
        init_and_migrate_db()
        REPLICATOR.start()
        start_backups()
    except Exception as error:
        log(f"Server initialization failed: {error}")
        messagebox.showerror(APP_NAME, f"Could not initialize the server database:\n{error}")
        root.destroy()
        return
    root.protocol("WM_DELETE_WINDOW", lambda: None)

    canvas = tk.Canvas(main_frame, bg="#eef3f8", highlightthickness=0)
    scrollbar = ttk.Scrollbar(main_frame, orient="vertical", command=canvas.yview)
    canvas.configure(yscrollcommand=scrollbar.set)
    scrollbar.pack(side="right", fill="y")
    canvas.pack(side="left", fill="both", expand=True)
    content_frame = tk.Frame(canvas, bg="#eef3f8")
    content_window = canvas.create_window((0, 0), window=content_frame, anchor="nw")
    content_frame.bind("<Configure>", lambda _event: canvas.configure(scrollregion=canvas.bbox("all")))
    canvas.bind("<Configure>", lambda event: canvas.itemconfigure(content_window, width=event.width))
    def scroll_main(event):
        if event.widget.winfo_class() in ("Text", "Entry", "TEntry", "Listbox"):
            return
        canvas.yview_scroll(int(-event.delta / 120), "units")
        return "break"
    canvas.bind_all("<MouseWheel>", scroll_main)

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
    v_auto_funnel = tk.BooleanVar(value=bool(cfg.get("auto_tunnel", cfg.get("tailscale_funnel", False))))
    v_background_startup = tk.BooleanVar(value=background_startup_enabled())
    v_cf_auto = tk.BooleanVar(value=bool(cfg.get("cloudflare_auto", False)))
    v_cf_status = tk.StringVar()
    cf_tunnel = Tunnel()
    tailscale_installed = tailscale_executable() is not None
    peer_box = None

    def copy(text):
        root.clipboard_clear()
        root.clipboard_append(text)

    def open_workflows_monitor():
        if not server.running:
            messagebox.showwarning(APP_NAME, "Start the server before opening its live workflow monitor.")
            return
        webbrowser.open(f"http://127.0.0.1:{server.port}/workflows")

    root.title(APP_NAME)
    root.geometry("1040x820")
    root.minsize(860, 680)
    pad = {"padx": 6, "pady": 2}
    white = "#ffffff"
    navy = "#17324b"

    def section(title):
        card = tk.Frame(content_frame, bg=white, bd=0, highlightbackground="#dbe4ef", highlightthickness=1)
        card.pack(fill="x", padx=14, pady=7)
        tk.Label(card, text=title, bg=white, fg=navy, font=("Segoe UI Semibold", 10)).pack(
            anchor="w", padx=14, pady=(10, 4)
        )
        body = tk.Frame(card, bg=white, padx=10, pady=5)
        body.pack(fill="x", padx=4, pady=(0, 7))
        return body

    def row(label, var, button=None, width=52, parent=content_frame, editable=False):
        frame = tk.Frame(parent, bg=white)
        frame.pack(fill="x", **pad)
        tk.Label(frame, text=label, width=24, anchor="w", bg=white, fg="#334155").pack(side="left")
        entry = tk.Entry(frame, textvariable=var, width=width, state="normal" if editable else "readonly",
                         readonlybackground="#f8fafc", relief="solid", bd=1)
        entry.pack(side="left", fill="x", expand=True, ipady=4)
        if button:
            tk.Button(frame, text=button[0], command=button[1], width=12, relief="flat",
                      bg="#e7edf4", activebackground="#d6e2ee").pack(side="left", padx=(6, 0))
        return frame

    banner = tk.Frame(content_frame, bg=navy, padx=16, pady=12)
    banner.pack(fill="x", padx=12, pady=(12, 6))
    tk.Label(banner, text=APP_NAME, font=("Segoe UI", 17, "bold"), bg=navy, fg=white).pack(anchor="w")
    tk.Label(banner, text="Live sync and replicas · close to minimize to tray · stop/exit requires admin password",
             font=("Segoe UI", 9), bg=navy, fg="#d5e4f1").pack(anchor="w", pady=(3, 0))

    status_card = section("Server status")
    top = tk.Frame(status_card, bg=white)
    top.pack(fill="x", **pad)
    lbl_status = tk.Label(top, textvariable=v_status, font=("Segoe UI", 10, "bold"), width=22, anchor="w", bg=white)
    lbl_status.pack(side="left")
    tk.Label(top, text="Port:").pack(side="left")
    ent_port = tk.Entry(top, textvariable=v_port, width=7)
    ent_port.pack(side="left", padx=4)
    btn_srv = tk.Button(top, width=14)
    btn_srv.pack(side="left", padx=6, ipady=3)
    lbl_role = tk.Label(top, textvariable=v_role, font=("Segoe UI", 9, "bold"), bg=white)
    lbl_role.pack(side="right", padx=8)

    network_card = section("Network access")
    row("Address (this network):", v_local, ("Copy", lambda: copy(v_local.get())), parent=network_card)
    row("Stable link (Tailscale):", v_custom, ("Copy", lambda: copy(v_custom.get())), parent=network_card)
    tunnel_controls = tk.Frame(network_card, bg=white)
    tunnel_controls.pack(fill="x", padx=6, pady=3)
    btn_funnel = tk.Button(tunnel_controls, width=30, relief="flat")
    btn_funnel.pack(side="left")
    tk.Checkbutton(tunnel_controls, text="Start stable public link automatically", variable=v_auto_funnel,
                   bg=white, activebackground=white).pack(side="left", padx=8)
    startup_controls = tk.Frame(network_card, bg=white)
    startup_controls.pack(fill="x", padx=6, pady=(0, 3))
    tk.Checkbutton(
        startup_controls,
        text="Start server and stable Funnel in the background when I sign in to Windows",
        variable=v_background_startup,
        bg=white,
        activebackground=white,
    ).pack(side="left")
    row("Temporary public link:", v_public, ("Copy", lambda: copy(v_public.get())), parent=network_card)
    btn_tun = tk.Button(network_card, width=26, relief="flat")
    btn_tun.pack(anchor="w", padx=6, pady=3)

    tk.Label(network_card, text="My own Cloudflare tunnel (permanent address)", font=("Segoe UI", 9, "bold"),
             bg=white, fg=navy).pack(anchor="w", padx=6, pady=(8, 0))
    row("Cloudflare status:", v_cf_status, ("Copy address", lambda: copy(str(cfg.get("cloudflare_url") or ""))), parent=network_card)
    cf_controls = tk.Frame(network_card, bg=white)
    cf_controls.pack(fill="x", padx=6, pady=3)
    btn_cf_setup = tk.Button(cf_controls, text="Set up my tunnel (token + address)", width=30, relief="flat")
    btn_cf_setup.pack(side="left")
    btn_cf = tk.Button(cf_controls, width=26, relief="flat")
    btn_cf.pack(side="left", padx=6)
    tk.Checkbutton(cf_controls, text="Start my tunnel automatically", variable=v_cf_auto,
                   bg=white, activebackground=white).pack(side="left", padx=8)

    connection_card = section("Website connection")
    tk.Label(connection_card, justify="left", wraplength=850, bg=white, fg="#475569",
             text="Copy this single connection code into Website → Admin → Server settings. Keep the admin key private.").pack(anchor="w", padx=6, pady=(0, 4))
    row("Connection code:", v_code, ("Copy Code", lambda: copy(v_code.get())), parent=connection_card)
    row("View key (users):", v_view, parent=connection_card)
    row("Admin key (private):", v_admin, parent=connection_card)
    key_buttons = tk.Frame(connection_card, bg=white)
    key_buttons.pack(fill="x", padx=6, pady=3)
    tk.Button(key_buttons, text="Create New Keys", width=18, command=lambda: new_keys(), relief="flat").pack(side="left")
    tk.Button(key_buttons, text="Edit stable URL", width=18, command=lambda: edit_stable_url(), relief="flat").pack(side="left", padx=6)

    replica_card = section("Backups and activity")
    replica_controls = tk.Frame(replica_card, bg=white)
    replica_controls.pack(fill="x", padx=6, pady=(2, 4))
    tk.Label(replica_controls, text="Replica servers:", font=("Segoe UI", 9, "bold"), bg=white, fg=navy).pack(side="left")
    tk.Button(replica_controls, text="Manage Replicas", command=lambda: open_replica_manager()).pack(side="left", padx=8)
    tk.Button(replica_controls, text="Backup Now", command=lambda: threading.Thread(target=backup_db, daemon=True).start()).pack(side="left")
    tk.Button(replica_controls, text="Workflows Monitor", command=open_workflows_monitor).pack(side="left", padx=6)

    tk.Label(replica_card, text="Recent server activity", anchor="w", bg=white, fg=navy,
             font=("Segoe UI", 9, "bold")).pack(anchor="w", padx=6, pady=(5, 3))
    txt = tk.Text(replica_card, height=8, state="disabled", wrap="word", font=("Consolas", 9),
                  bg="#f8fafc", fg="#1e293b", relief="solid", bd=1)
    txt.pack(fill="both", expand=True, padx=6, pady=(0, 4))

    def save_custom_url(url):
        url = url.strip().rstrip("/")
        if url:
            parsed = urllib.parse.urlparse(url)
            if parsed.scheme != "https" or not parsed.netloc:
                messagebox.showerror(APP_NAME, "Enter a valid HTTPS URL, such as https://your-device.your-tailnet.ts.net.")
                return False
        previous = cfg.get("custom_url", "")
        cfg["custom_url"] = url
        if not save_config(cfg):
            cfg["custom_url"] = previous
            messagebox.showerror(APP_NAME, f"Could not save the permanent URL to {CONFIG_PATH}.")
            return False
        v_custom.set(url)
        return True

    def edit_cloudflare():
        dialog = tk.Toplevel(root)
        dialog.title("My Cloudflare Tunnel")
        dialog.transient(root)
        dialog.grab_set()
        tk.Label(dialog, justify="left", wraplength=520,
                 text="In Cloudflare Zero Trust → Networks → Tunnels, open your tunnel → Public Hostname and point your address to "
                      f"http://localhost:{server.port or cfg.get('port', DEFAULT_PORT)}. Then copy the tunnel token (the long text after 'cloudflared service install').").pack(anchor="w", padx=12, pady=(12, 6))
        tk.Label(dialog, text="Tunnel token", font=("Segoe UI", 10, "bold")).pack(anchor="w", padx=12)
        token_entry = tk.Entry(dialog, width=62, show="•")
        token_entry.pack(fill="x", padx=12, pady=(2, 6))
        token_entry.insert(0, cfg.get("cloudflare_token", ""))
        tk.Label(dialog, text="Public address (HTTPS), e.g. https://mentor.yourdomain.com", font=("Segoe UI", 10, "bold")).pack(anchor="w", padx=12)
        url_entry = tk.Entry(dialog, width=62)
        url_entry.pack(fill="x", padx=12, pady=(2, 6))
        url_entry.insert(0, cfg.get("cloudflare_url", ""))
        buttons = tk.Frame(dialog)
        buttons.pack(fill="x", padx=12, pady=10)
        def save_cf():
            token = token_entry.get().strip()
            if token.lower().startswith("cloudflared"):
                token = token.split()[-1]
            url = url_entry.get().strip().rstrip("/")
            if url and "://" not in url:
                url = "https://" + url
            if token and not url:
                return messagebox.showerror(APP_NAME, "Enter your public address too, so users and the website know where to connect.", parent=dialog)
            if url:
                parsed = urllib.parse.urlparse(url)
                if parsed.scheme != "https" or not parsed.netloc:
                    return messagebox.showerror(APP_NAME, "The public address must be a valid HTTPS URL.", parent=dialog)
            previous = cfg.get("cloudflare_token", ""), cfg.get("cloudflare_url", "")
            cfg["cloudflare_token"], cfg["cloudflare_url"] = token, url
            if not save_config(cfg):
                cfg["cloudflare_token"], cfg["cloudflare_url"] = previous
                return messagebox.showerror(APP_NAME, f"Could not save the Cloudflare settings to {CONFIG_PATH}.", parent=dialog)
            if cf_tunnel.running:
                cf_tunnel.stop()
            dialog.destroy()
            refresh_ui()
        tk.Button(buttons, text="Cancel", command=dialog.destroy).pack(side="right")
        tk.Button(buttons, text="Save", command=save_cf).pack(side="right", padx=6)
        token_entry.focus_set()

    def toggle_cf_tunnel():
        if cf_tunnel.running:
            cf_tunnel.stop()
            return refresh_ui()
        if not cfg.get("cloudflare_token"):
            return edit_cloudflare()
        if not server.running:
            return messagebox.showwarning(APP_NAME, "Start the server first.")
        try:
            cf_tunnel.start(server.port, cfg["cloudflare_token"], cfg.get("cloudflare_url"))
        except (OSError, RuntimeError) as error:
            messagebox.showerror(APP_NAME, str(error))
        refresh_ui()

    def on_cf_auto(*_):
        previous = cfg.get("cloudflare_auto", False)
        cfg["cloudflare_auto"] = v_cf_auto.get()
        if not save_config(cfg):
            cfg["cloudflare_auto"] = previous
            v_cf_auto.set(previous)
            messagebox.showerror(APP_NAME, f"Could not save the setting to {CONFIG_PATH}.")

    def edit_stable_url():
        dialog = tk.Toplevel(root)
        dialog.title("Stable Connection URL")
        dialog.transient(root)
        dialog.grab_set()
        tk.Label(dialog, text="Permanent HTTPS URL (Tailscale Funnel)", font=("Segoe UI", 10, "bold")).pack(anchor="w", padx=12, pady=(12, 4))
        url_entry = tk.Entry(dialog, width=62)
        url_entry.pack(fill="x", padx=12, pady=4)
        url_entry.insert(0, v_custom.get())
        buttons = tk.Frame(dialog)
        buttons.pack(fill="x", padx=12, pady=10)
        tk.Button(buttons, text="Cancel", command=dialog.destroy).pack(side="right")
        def save_url():
            if save_custom_url(url_entry.get()) is not False:
                dialog.destroy()
        tk.Button(buttons, text="Save URL", command=save_url).pack(side="right", padx=6)
        url_entry.focus_set()

    def new_keys():
        if not messagebox.askyesno(
            APP_NAME,
            "Create new server keys?\n\n"
            "This will invalidate the keys currently saved in the website and disconnect sync "
            "until an administrator pastes the updated connection code into Website → Admin → "
            "Server settings and clicks Save & Test.",
        ):
            return
        previous = cfg["admin_key"], cfg["view_key"]
        cfg["admin_key"], cfg["view_key"] = secrets.token_urlsafe(12), secrets.token_urlsafe(12)
        if not save_config(cfg):
            cfg["admin_key"], cfg["view_key"] = previous
            return messagebox.showerror(APP_NAME, f"Could not save new keys to {CONFIG_PATH}.")
        v_admin.set(cfg["admin_key"])
        v_view.set(cfg["view_key"])

    replica_window = None
    def open_replica_manager():
        nonlocal peer_box, replica_window
        if replica_window and replica_window.winfo_exists():
            replica_window.lift()
            return
        replica_window = tk.Toplevel(root)
        replica_window.title("Manage Replica Servers")
        replica_window.geometry("700x360")
        replica_window.transient(root)
        tk.Label(replica_window, text="Pair another server with the connection code from its window.", anchor="w").pack(fill="x", padx=12, pady=(12, 4))
        entry_row = tk.Frame(replica_window)
        entry_row.pack(fill="x", padx=12, pady=4)
        ttk.Entry(entry_row, textvariable=v_peer_input).pack(side="left", fill="x", expand=True)
        tk.Button(entry_row, text="Paste", command=paste_code).pack(side="left", padx=5)
        peer_frame = tk.Frame(replica_window)
        peer_frame.pack(fill="both", expand=True, padx=12, pady=6)
        peer_box = tk.Listbox(peer_frame, height=8, font=("Consolas", 9))
        peer_box.pack(side="left", fill="both", expand=True)
        scrollbar = ttk.Scrollbar(peer_frame, orient="vertical", command=peer_box.yview)
        scrollbar.pack(side="right", fill="y")
        peer_box.configure(yscrollcommand=scrollbar.set)
        buttons = tk.Frame(replica_window)
        buttons.pack(anchor="w", padx=12, pady=(0, 12))
        tk.Button(buttons, text="Add Peer", command=add_peer).pack(side="left")
        tk.Button(buttons, text="Make this a Backup", command=make_backup_of_code).pack(side="left", padx=6)
        tk.Button(buttons, text="Promote to Primary", command=lambda: promote_to_primary()).pack(side="left")
        tk.Button(buttons, text="Remove Selected", command=remove_peer).pack(side="left", padx=6)
        replica_window.protocol("WM_DELETE_WINDOW", lambda: close_replica_manager())
        refresh_peer_box()

    def close_replica_manager():
        nonlocal peer_box, replica_window
        peer_box = None
        if replica_window and replica_window.winfo_exists():
            replica_window.destroy()
        replica_window = None

    def paste_code():
        try:
            v_peer_input.set(root.clipboard_get().strip())
        except tk.TclError as error:
            messagebox.showerror(APP_NAME, f"Could not read the clipboard: {error}")

    def refresh_peer_box():
        if not peer_box or not peer_box.winfo_exists():
            return
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
        if not peer_box:
            return
        sel = peer_box.curselection()
        if not sel:
            return messagebox.showinfo(APP_NAME, "Select a replica to remove.")
        del cfg["peers"][sel[0]]
        save_config(cfg)
        refresh_peer_box()

    def toggle_server():
        if server.running:
            confirm_admin_action("stop the server", stop_server_authorized)
        else:
            try:
                p = int(v_port.get().strip())
                if not 1024 <= p <= 65535:
                    raise ValueError("Enter a port between 1024 and 65535.")
                ok, error = server.start(p)
                if not ok:
                    return messagebox.showwarning(APP_NAME, f"The server could not start:\n{error}")
                cfg["port"] = p
                if not save_config(cfg):
                    messagebox.showerror(APP_NAME, f"Server started, but settings could not be saved to {CONFIG_PATH}.")
            except Exception as e:
                messagebox.showerror(APP_NAME, str(e))

    def toggle_tunnel():
        if tunnel.running:
            tunnel.stop()
        else:
            if not server.running:
                return messagebox.showwarning(APP_NAME, "Start server first.")
            tunnel.start(server.port)

    def start_funnel(show_error=True):
        if not server.running:
            if show_error:
                messagebox.showwarning(APP_NAME, "Start the local server before enabling Funnel.")
            return False
        try:
            url = enable_tailscale_funnel(server.port)
            previous_url, previous_enabled = cfg.get("custom_url", ""), cfg.get("tailscale_funnel", False)
            cfg["custom_url"] = url
            cfg["tailscale_funnel"] = True
            if not save_config(cfg):
                cfg["custom_url"], cfg["tailscale_funnel"] = previous_url, previous_enabled
                message = f"Funnel is active at {url}, but its URL could not be saved to {CONFIG_PATH}."
                log(message)
                if show_error:
                    messagebox.showerror(APP_NAME, message)
                return False
            v_custom.set(url)
            return True
        except (OSError, RuntimeError, subprocess.TimeoutExpired) as error:
            log(f"Could not start Tailscale Funnel: {error}")
            if show_error:
                messagebox.showerror(APP_NAME, str(error))
            return False

    def toggle_funnel():
        nonlocal tailscale_installed
        tailscale_installed = tailscale_executable() is not None
        if not cfg.get("tailscale_funnel") and not tailscale_installed:
            if messagebox.askyesno(
                APP_NAME,
                "Tailscale is not installed on this PC. Install Tailscale for Windows, sign in, "
                "and enable Funnel for your tailnet before trying again.\n\nOpen the official download page?",
            ):
                webbrowser.open(TAILSCALE_DOWNLOAD_URL)
            return
        if cfg.get("tailscale_funnel"):
            try:
                disable_tailscale_funnel()
            except (OSError, RuntimeError, subprocess.TimeoutExpired) as error:
                return messagebox.showerror(APP_NAME, str(error))
            old_url, old_enabled = cfg.get("custom_url", ""), cfg.get("tailscale_funnel")
            cfg["custom_url"] = ""
            cfg["tailscale_funnel"] = False
            if not save_config(cfg):
                cfg["custom_url"], cfg["tailscale_funnel"] = old_url, old_enabled
                return messagebox.showerror(APP_NAME, f"Funnel stopped, but its setting could not be saved to {CONFIG_PATH}.")
            v_custom.set("")
        else:
            start_funnel()

    def on_auto_funnel(*_args):
        previous = cfg.get("auto_tunnel", False)
        cfg["auto_tunnel"] = v_auto_funnel.get()
        if not save_config(cfg):
            cfg["auto_tunnel"] = previous
            v_auto_funnel.set(previous)
            messagebox.showerror(APP_NAME, f"Could not save startup settings to {CONFIG_PATH}.")

    def on_background_startup(*_args):
        previous = not v_background_startup.get()
        try:
            set_background_startup(v_background_startup.get())
        except (OSError, RuntimeError) as error:
            v_background_startup.set(previous)
            messagebox.showerror(APP_NAME, f"Could not update Windows startup settings: {error}")

    v_auto_funnel.trace_add("write", on_auto_funnel)
    v_cf_auto.trace_add("write", on_cf_auto)
    v_background_startup.trace_add("write", on_background_startup)

    def stop_server_authorized():
        tunnel.stop()
        cf_tunnel.stop()
        if cfg.get("tailscale_funnel"):
            try:
                disable_tailscale_funnel()
                cfg["tailscale_funnel"] = False
                cfg["custom_url"] = ""
                if not save_config(cfg):
                    messagebox.showerror(APP_NAME, f"Funnel stopped, but its state could not be saved to {CONFIG_PATH}.")
                v_custom.set("")
            except (OSError, RuntimeError, subprocess.TimeoutExpired) as error:
                messagebox.showerror(APP_NAME, f"Could not stop Tailscale Funnel: {error}")
        server.stop()
        refresh_ui()

    closing = False
    tray_icon = None
    tray_image_states = {}

    def restore_window():
        if root.winfo_exists():
            root.deiconify()
            root.lift()
            root.focus_force()

    def hide_to_tray():
        root.withdraw()
        if tray_icon:
            tray_icon.title = f"{APP_NAME} — {'Running' if server.running else 'Stopped'}"

    def confirm_admin_action(action, callback):
        restore_window()
        dialog = tk.Toplevel(root)
        dialog.title("Administrator confirmation")
        dialog.geometry("400x260")
        dialog.resizable(False, False)
        dialog.transient(root)
        dialog.grab_set()
        dialog.configure(bg="#f4f7fb")
        tk.Label(dialog, text="Administrator confirmation", font=("Segoe UI", 13, "bold"),
                 bg="#f4f7fb", fg=navy).pack(anchor="w", padx=20, pady=(18, 3))
        tk.Label(dialog, text=f"Enter your website admin password to {action}.",
                 wraplength=350, bg="#f4f7fb", fg="#475569").pack(anchor="w", padx=20, pady=(0, 12))
        tk.Label(dialog, text="Username", bg="#f4f7fb").pack(anchor="w", padx=20)
        username = tk.Entry(dialog)
        username.pack(fill="x", padx=20, pady=(3, 9), ipady=4)
        username.insert(0, admin_username)
        tk.Label(dialog, text="Password", bg="#f4f7fb").pack(anchor="w", padx=20)
        password = tk.Entry(dialog, show="*")
        password.pack(fill="x", padx=20, pady=(3, 4), ipady=4)
        error_text = tk.StringVar()
        tk.Label(dialog, textvariable=error_text, fg="#bd3b3b", bg="#f4f7fb").pack(anchor="w", padx=20)

        def authorize(_event=None):
            nonlocal admin_username
            user = username.get().strip().lower()
            accepted, message = verify_admin_login(user, password.get())
            if not accepted:
                error_text.set(message)
                password.focus_set()
                return
            admin_username = user
            dialog.grab_release()
            dialog.destroy()
            callback()

        actions = tk.Frame(dialog, bg="#f4f7fb")
        actions.pack(fill="x", padx=20, pady=(7, 14))
        tk.Button(actions, text="Cancel", command=dialog.destroy, relief="flat").pack(side="right")
        tk.Button(actions, text="Confirm", command=authorize, relief="flat",
                  bg="#1769aa", fg="white", activebackground="#12558b").pack(side="right", padx=7)
        dialog.bind("<Return>", authorize)
        dialog.protocol("WM_DELETE_WINDOW", dialog.destroy)
        password.focus_set()

    def shutdown_app():
        nonlocal closing
        if closing:
            return
        closing = True
        tunnel.stop()
        cf_tunnel.stop()
        if cfg.get("tailscale_funnel"):
            try:
                disable_tailscale_funnel()
                cfg["tailscale_funnel"] = False
                cfg["custom_url"] = ""
                if not save_config(cfg):
                    log(f"Could not save Funnel state to {CONFIG_PATH}.")
                v_custom.set("")
            except (OSError, RuntimeError, subprocess.TimeoutExpired) as error:
                messagebox.showerror(APP_NAME, f"Could not stop Tailscale Funnel: {error}")
        server.stop()
        if tray_icon:
            tray_icon.stop()
        root.destroy()

    def request_exit():
        confirm_admin_action("exit and stop the server", shutdown_app)

    def tray_open(_icon, _item):
        root.after(0, restore_window)

    def tray_stop(_icon, _item):
        root.after(0, lambda: confirm_admin_action("stop the server", stop_server_authorized))

    def tray_exit(_icon, _item):
        root.after(0, request_exit)

    def make_tray_image(running):
        image = Image.new("RGBA", (64, 64), (255, 255, 255, 0))
        if icon_path:
            with Image.open(icon_path) as source:
                source.seek(0)
                image.alpha_composite(source.convert("RGBA").resize((64, 64)))
        else:
            draw = ImageDraw.Draw(image)
            draw.rounded_rectangle((2, 2, 62, 62), radius=12, fill=navy)
            draw.text((22, 13), "F", fill="white")
        ImageDraw.Draw(image).ellipse((45, 45, 62, 62), fill="#22c55e" if running else "#ef4444",
                                     outline="white", width=2)
        return image

    def run_tray():
        try:
            tray_icon.run()
        except Exception as error:
            log(f"System tray failed: {error}")
            if not closing:
                root.after(0, lambda: messagebox.showerror(APP_NAME, f"System tray could not start:\n{error}"))

    tray_icon = pystray.Icon(
        "FuadProjectServer",
        make_tray_image(server.running),
        f"{APP_NAME} — {'Running' if server.running else 'Stopped'}",
        menu=pystray.Menu(
            pystray.MenuItem("Open control panel", tray_open, default=True),
            pystray.MenuItem("Stop server (admin required)", tray_stop),
            pystray.MenuItem("Exit (admin required)", tray_exit),
        ),
    )
    tray_thread = threading.Thread(target=run_tray, name="server-tray", daemon=True)
    tray_thread.start()

    btn_srv.configure(command=toggle_server)
    btn_tun.configure(command=toggle_tunnel)
    btn_cf.configure(command=toggle_cf_tunnel)
    btn_cf_setup.configure(command=edit_cloudflare)
    btn_funnel.configure(command=toggle_funnel)

    last_log_len = 0
    last_tray_running = None
    def refresh_ui():
        nonlocal last_log_len, last_tray_running
        running = server.running
        v_status.set("Server running" if running else "Server stopped")
        lbl_status.configure(foreground="#218c5a" if running else "#bd3b3b")
        btn_srv.configure(text="Stop Server" if running else "Start Server",
                          bg="#bd3b3b" if running else "#218c5a", fg="white", activebackground="#a72e2e" if running else "#176b43")
        ent_port.configure(state="disabled" if running else "normal")
        if tray_icon:
            tray_icon.title = f"{APP_NAME} — {'Running' if running else 'Stopped'}"
            if running != last_tray_running:
                tray_icon.icon = make_tray_image(running)
                last_tray_running = running
        p = server.port or v_port.get()
        v_local.set(f"http://{lan_addresses()[0]}:{p}" if running else "")
        v_public.set(tunnel.url or ("Starting..." if tunnel.running else ""))
        btn_tun.configure(text="Stop Temporary Cloudflare Link" if tunnel.running else "Start Temporary Cloudflare Link")
        if not cfg.get("cloudflare_token"):
            v_cf_status.set("Not set up")
        elif cf_tunnel.running and cf_tunnel.url:
            v_cf_status.set("Connected: " + str(cfg.get("cloudflare_url") or cf_tunnel.url))
        elif cf_tunnel.running:
            v_cf_status.set("Connecting…" + (f"  ({cf_tunnel.error})" if cf_tunnel.error else ""))
        else:
            v_cf_status.set("Stopped — " + str(cfg.get("cloudflare_url") or "no address saved"))
        btn_cf.configure(text="Stop my Cloudflare tunnel" if cf_tunnel.running else "Start my Cloudflare tunnel")
        btn_funnel.configure(
            text="Stop Stable Funnel" if cfg.get("tailscale_funnel") else
            "Enable / Refresh Stable Funnel" if tailscale_installed else
            "Download Tailscale to Enable Funnel"
        )
        
        base_url = v_custom.get().strip() or public_base_url(cfg) or tunnel.url or v_local.get()
        v_code.set(make_code(base_url, cfg["view_key"], cfg["admin_key"]) if base_url else "")

        mode = cfg.get("mode", "primary").upper()
        term = cfg.get("term", 1)
        v_role.set(f"[{mode} T{term}] - {REPLICATOR.status}")
        lbl_role.configure(foreground="#16a34a" if mode == "PRIMARY" else "#d97706")

        lines = list(_log)
        if len(lines) != last_log_len:
            last_log_len = len(lines)
            txt.configure(state="normal")
            txt.delete("1.0", "end")
            txt.insert("end", "\n".join(lines[-70:]))
            txt.see("end")
            txt.configure(state="disabled")

        root.after(1000, refresh_ui)

    try:
        port = int(v_port.get().strip())
        if 1024 <= port <= 65535:
            ok, error = server.start(port)
            if not ok:
                messagebox.showwarning(APP_NAME, f"The server could not start on port {port}:\n{error}\n\nChange the port and click Start Server.")
        else:
            messagebox.showwarning(APP_NAME, "Enter a port between 1024 and 65535, then click Start Server.")
    except (ValueError, OSError) as error:
        messagebox.showwarning(APP_NAME, f"The server could not start:\n{error}\n\nChange the port and click Start Server.")
    refresh_peer_box()
    refresh_ui()
    if cfg.get("auto_tunnel"):
        root.after(800, lambda: start_funnel(show_error=False))
    if cfg.get("cloudflare_auto") and cfg.get("cloudflare_token"):
        def _auto_cf():
            if server.running and not cf_tunnel.running:
                try:
                    cf_tunnel.start(server.port, cfg["cloudflare_token"], cfg.get("cloudflare_url"))
                except (OSError, RuntimeError) as error:
                    log(f"Cloudflare tunnel could not start: {error}")
        root.after(1200, _auto_cf)
    root.protocol("WM_DELETE_WINDOW", hide_to_tray)
    root.mainloop()

def main():
    command = sys.argv[1:]
    if command == ["--background-server"]:
        return run_background_server()
    if command == ["--stop-background-server"]:
        request_background_server_stop()
        return 0
    if command == ["--install-background-startup"]:
        set_background_startup(True)
        return 0
    if command == ["--remove-background-startup"]:
        set_background_startup(False)
        return 0
    cfg = load_config()
    server = ServerCore()
    tunnel = Tunnel()
    run_app(server, tunnel, cfg)

if __name__ == "__main__":
    raise SystemExit(main() or 0)