"""Standalone Admin UI for monitoring the bootstrap server."""

from __future__ import annotations

import argparse
import json
import urllib.error
import urllib.request
from typing import Any

from flask import Flask, jsonify, render_template_string, request

from common.constants import DEFAULT_BOOTSTRAP_HOST, DEFAULT_BOOTSTRAP_PORT


ADMIN_PAGE = """
<!doctype html>
<html lang="en">
<head>
  <meta charset="utf-8">
  <meta name="viewport" content="width=device-width, initial-scale=1">
  <title>Bootstrap Admin</title>
  <style>
    :root { --bg:#f7f8fb; --panel:#fff; --line:#d7deea; --text:#172033; --muted:#64748b; --primary:#1d4ed8; --danger:#b42318; }
    * { box-sizing: border-box; }
    body { margin:0; font-family:Arial, Helvetica, sans-serif; color:var(--text); background:var(--bg); }
    header { display:flex; align-items:center; justify-content:space-between; gap:16px; padding:18px 22px; background:#111827; color:white; }
    button { border:0; border-radius:6px; padding:10px 14px; background:var(--primary); color:white; font-weight:700; cursor:pointer; }
    main { display:grid; gap:16px; padding:16px; }
    section { background:var(--panel); border:1px solid var(--line); border-radius:8px; padding:16px; }
    h1, h2 { margin:0 0 12px; }
    .offline { color:#fecaca; font-weight:700; }
    .stats { display:grid; grid-template-columns:repeat(auto-fit, minmax(160px, 1fr)); gap:10px; }
    .metric { border:1px solid var(--line); border-radius:6px; padding:12px; background:#fbfcff; }
    .metric strong { display:block; font-size:24px; }
    .metric span { color:var(--muted); font-size:13px; }
    table { width:100%; border-collapse:collapse; font-size:14px; }
    th, td { text-align:left; border-bottom:1px solid var(--line); padding:9px; vertical-align:top; }
    th { color:var(--muted); font-size:12px; text-transform:uppercase; }
    .empty { color:var(--muted); }
  </style>
</head>
<body>
  <header>
    <div>
      <h1>Bootstrap Server Status</h1>
      <div id="subtitle"></div>
    </div>
    <button type="button" onclick="refresh()">Refresh</button>
  </header>
  <main>
    <section><h2>Runtime Stats</h2><div id="stats" class="stats"></div></section>
    <section><h2>Known Peers</h2><div id="peers"></div></section>
    <section><h2>Groups</h2><div id="groups"></div></section>
    <section><h2>Offline Message Queue</h2><div id="offline"></div></section>
  </main>
  <script>
    async function getJson(path) {
      const response = await fetch(path, { cache: "no-store" });
      return response.json();
    }
    function table(rows, columns) {
      if (!rows.length) return "<p class='empty'>No records.</p>";
      const head = columns.map(column => `<th>${column.label}</th>`).join("");
      const body = rows.map(row => `<tr>${columns.map(column => `<td>${column.render(row)}</td>`).join("")}</tr>`).join("");
      return `<table><thead><tr>${head}</tr></thead><tbody>${body}</tbody></table>`;
    }
    function setOffline(message) {
      document.getElementById("subtitle").innerHTML = `<span class="offline">Bootstrap offline: ${message}</span>`;
      document.getElementById("stats").innerHTML = "";
      document.getElementById("peers").innerHTML = "<p class='empty'>Unavailable.</p>";
      document.getElementById("groups").innerHTML = "<p class='empty'>Unavailable.</p>";
      document.getElementById("offline").innerHTML = "<p class='empty'>Unavailable.</p>";
    }
    async function refresh() {
      try {
        const [stats, peers, groups, offline] = await Promise.all([
          getJson("/admin/api/stats"),
          getJson("/admin/api/peers"),
          getJson("/admin/api/groups"),
          getJson("/admin/api/offline-messages")
        ]);
        if (!stats.ok) return setOffline(stats.error || "unreachable");
        document.getElementById("subtitle").textContent = `Session ${stats.session_id} | Started ${stats.started_at} | uptime ${stats.uptime_seconds}s`;
        document.getElementById("stats").innerHTML = [
          ["Peers", stats.peer_count],
          ["Online", stats.status_counts.online || 0],
          ["Offline", stats.status_counts.offline || 0],
          ["Disconnected", stats.status_counts.disconnected || 0],
          ["Groups", stats.group_count],
          ["Queued messages", stats.queued_offline_messages]
        ].map(([label, value]) => `<div class="metric"><strong>${value}</strong><span>${label}</span></div>`).join("");
        document.getElementById("peers").innerHTML = table(peers.peers || [], [
          { label:"Username", render: row => row.username || row.peer_id },
          { label:"Address", render: row => `${row.host}:${row.port}` },
          { label:"Reserved port owner", render: row => row.reserved_port_owner || row.username || row.peer_id },
          { label:"Status", render: row => row.status },
          { label:"Last seen", render: row => row.last_seen }
        ]);
        document.getElementById("groups").innerHTML = table(groups.groups || [], [
          { label:"Name", render: row => row.group_name },
          { label:"Owner", render: row => row.owner },
          { label:"Members", render: row => (row.members || []).join(", ") },
          { label:"Member count", render: row => row.member_count || (row.members || []).length },
          { label:"Created", render: row => row.created_at }
        ]);
        document.getElementById("offline").innerHTML = table(offline.messages || [], [
          { label:"Status", render: row => row.delivery_status },
          { label:"Type", render: row => row.type },
          { label:"From", render: row => row.from },
          { label:"To", render: row => row.to },
          { label:"Group", render: row => row.group_name || row.group_id || "" },
          { label:"Message", render: row => row.message },
          { label:"Created", render: row => row.created_at }
        ]);
      } catch (error) {
        setOffline(error.message);
      }
    }
    refresh();
    setInterval(refresh, 2000);
  </script>
</body>
</html>
"""


def create_app(bootstrap_host: str, bootstrap_port: int) -> Flask:
    app = Flask(__name__)
    base_url = f"http://{bootstrap_host}:{bootstrap_port}"

    @app.after_request
    def no_cache(response):
        if request.path.startswith("/admin/api/"):
            response.headers["Cache-Control"] = "no-store, no-cache, must-revalidate, max-age=0"
            response.headers["Pragma"] = "no-cache"
            response.headers["Expires"] = "0"
        return response

    @app.get("/")
    def index():
        return render_template_string(ADMIN_PAGE)

    @app.get("/admin/api/<path:name>")
    def admin_proxy(name: str):
        mapping = {
            "peers": "/api/admin/peers",
            "groups": "/api/admin/groups",
            "offline-messages": "/api/admin/offline-messages",
            "stats": "/api/admin/stats",
        }
        path = mapping.get(name)
        if path is None:
            return jsonify({"ok": False, "error": "Unknown admin endpoint"}), 404
        payload, status = _fetch_json(f"{base_url}{path}")
        return jsonify(payload), status

    return app


def _fetch_json(url: str) -> tuple[dict[str, Any], int]:
    req = urllib.request.Request(url, headers={"Cache-Control": "no-store"})
    try:
        with urllib.request.urlopen(req, timeout=1.5) as response:
            data = json.loads(response.read().decode("utf-8"))
            return data if isinstance(data, dict) else {"ok": False, "error": "Invalid bootstrap response"}, response.status
    except (OSError, urllib.error.URLError, json.JSONDecodeError) as exc:
        return {"ok": False, "offline": True, "error": str(exc)}, 503


def main() -> None:
    parser = argparse.ArgumentParser(description="Run Bootstrap Admin UI")
    parser.add_argument("--bootstrap-host", default=DEFAULT_BOOTSTRAP_HOST)
    parser.add_argument("--bootstrap-port", type=int, default=DEFAULT_BOOTSTRAP_PORT)
    parser.add_argument("--admin-host", default="127.0.0.1")
    parser.add_argument("--admin-port", type=int, default=9100)
    args = parser.parse_args()

    app = create_app(args.bootstrap_host, args.bootstrap_port)
    app.run(host=args.admin_host, port=args.admin_port, debug=False, use_reloader=False)


if __name__ == "__main__":
    main()
