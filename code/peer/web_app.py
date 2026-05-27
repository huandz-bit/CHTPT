"""Flask Web UI where a browser session registers one peer before chatting."""

from __future__ import annotations

import argparse
import socket
import tempfile
import threading
from pathlib import Path
from typing import Any
from urllib.parse import quote

from flask import Flask, jsonify, render_template_string, request, send_from_directory

from common.constants import DEFAULT_BOOTSTRAP_HOST, DEFAULT_BOOTSTRAP_PORT, DEFAULT_PEER_HOST, MAX_FILE_SIZE_MB
from peer.file_transfer import FileTransferError
from peer.peer import PeerNode
from peer.peer_discovery import PeerDiscovery


PAGE = """
<!doctype html>
<html lang="en">
<head>
  <meta charset="utf-8">
  <meta name="viewport" content="width=device-width, initial-scale=1">
  <title>P2P Chat Network</title>
  <style>
    :root {
      color-scheme: light;
      --bg: #f5f7fb;
      --panel: #ffffff;
      --line: #d8dee8;
      --text: #1c2430;
      --muted: #647082;
      --primary: #1d4ed8;
      --danger: #b42318;
      --ok: #16794c;
    }
    * { box-sizing: border-box; }
    body {
      margin: 0;
      min-height: 100vh;
      font-family: Arial, Helvetica, sans-serif;
      background: var(--bg);
      color: var(--text);
    }
    button, input, textarea, select { font: inherit; }
    button {
      border: 0;
      border-radius: 6px;
      padding: 10px 14px;
      background: var(--primary);
      color: #fff;
      cursor: pointer;
      font-weight: 700;
    }
    button.secondary { background: #e9edf5; color: var(--text); }
    button.danger { background: var(--danger); }
    button:disabled { opacity: .55; cursor: not-allowed; }
    input, textarea, select {
      width: 100%;
      border: 1px solid var(--line);
      border-radius: 6px;
      padding: 10px 11px;
      background: #fff;
      color: var(--text);
    }
    textarea { min-height: 84px; resize: vertical; }
    label { display: grid; gap: 6px; color: var(--muted); font-size: 13px; font-weight: 700; }
    h1, h2, h3, p { margin-top: 0; }
    .hidden { display: none !important; }
    .center {
      min-height: 100vh;
      display: grid;
      place-items: center;
      padding: 24px;
    }
    .card, .panel {
      background: var(--panel);
      border: 1px solid var(--line);
      border-radius: 8px;
      box-shadow: 0 10px 30px rgba(31, 42, 68, .08);
    }
    .join-card { width: min(560px, 100%); padding: 26px; }
    .form-grid { display: grid; grid-template-columns: 1fr 1fr; gap: 14px; }
    .span-2 { grid-column: 1 / -1; }
    .note { color: var(--muted); font-size: 13px; margin-bottom: 0; }
    .error { color: var(--danger); font-weight: 700; min-height: 20px; }
    .notice {
      min-height: 20px;
      color: var(--muted);
      font-size: 13px;
      font-weight: 700;
    }
    .notice.error { color: var(--danger); }
    .notice.ok { color: var(--ok); }
    .status {
      display: flex;
      align-items: center;
      justify-content: space-between;
      gap: 16px;
      padding: 14px 18px;
      background: #111827;
      color: #fff;
    }
    .status small { color: #cbd5e1; display: block; margin-top: 3px; }
    .app {
      display: grid;
      grid-template-columns: 260px minmax(360px, 1fr) minmax(320px, 420px);
      gap: 16px;
      padding: 16px;
      min-height: calc(100vh - 68px);
    }
    .panel { padding: 16px; min-width: 0; }
    .panel h2 { font-size: 18px; margin-bottom: 12px; }
    .peer-list { display: grid; gap: 8px; }
    .peer-item {
      display: flex;
      align-items: center;
      justify-content: space-between;
      gap: 8px;
      border: 1px solid var(--line);
      border-radius: 6px;
      padding: 10px;
      background: #fbfcff;
    }
    .peer-item strong { display: block; }
    .peer-item span { color: var(--muted); font-size: 12px; }
    .badge {
      display: inline-flex;
      align-items: center;
      border-radius: 999px;
      padding: 2px 8px;
      background: #e9edf5;
      color: var(--muted);
      font-size: 12px;
      font-weight: 700;
    }
    .badge.online { background: #dcfce7; color: #166534; }
    .badge.offline { background: #fef3c7; color: #92400e; }
    .badge.disconnected { background: #fee2e2; color: #991b1b; }
    .stack { display: grid; gap: 14px; }
    .row { display: flex; gap: 10px; align-items: center; }
    .row > * { flex: 1; }
    .group-list { display: grid; gap: 8px; max-height: 220px; overflow: auto; }
    .group-item {
      border: 1px solid var(--line);
      border-radius: 6px;
      padding: 10px;
      background: #fbfcff;
    }
    .group-item strong, .group-item span { display: block; }
    .group-item span { color: var(--muted); font-size: 12px; margin-top: 3px; }
    .logs {
      height: calc(58vh - 120px);
      min-height: 260px;
      overflow: auto;
      border: 1px solid var(--line);
      border-radius: 6px;
      background: #0f172a;
      color: #dbeafe;
      padding: 12px;
      font-family: Consolas, monospace;
      font-size: 12px;
      line-height: 1.45;
      white-space: pre-wrap;
    }
    .log-line { margin-bottom: 8px; }
    .file-actions, .file-row-actions {
      display: inline-flex;
      gap: 8px;
      margin-left: 8px;
      white-space: normal;
    }
    .file-link {
      display: inline-flex;
      align-items: center;
      border-radius: 6px;
      padding: 3px 8px;
      background: #dbeafe;
      color: #1e3a8a;
      font-family: Arial, Helvetica, sans-serif;
      font-size: 12px;
      font-weight: 700;
      text-decoration: none;
    }
    .received-files {
      max-height: calc(42vh - 112px);
      min-height: 160px;
      overflow: auto;
      display: grid;
      gap: 8px;
    }
    .file-row {
      border: 1px solid var(--line);
      border-radius: 6px;
      padding: 10px;
      background: #fbfcff;
      display: grid;
      gap: 6px;
    }
    .file-row strong { overflow-wrap: anywhere; }
    .file-row span { color: var(--muted); font-size: 12px; }
    @media (max-width: 1050px) {
      .app { grid-template-columns: 1fr; }
      .logs { height: 340px; }
    }
    @media (max-width: 640px) {
      .form-grid { grid-template-columns: 1fr; }
      .span-2 { grid-column: auto; }
      .status { align-items: flex-start; flex-direction: column; }
    }
  </style>
</head>
<body>
  <section id="registration" class="center hidden">
    <form id="register-form" class="card join-card">
      <h1>Join P2P Chat Network</h1>
      <p class="note">Each peer must use a unique TCP port.</p>
      <div class="form-grid" style="margin-top: 18px;">
        <label class="span-2">Username
          <input name="username" autocomplete="off" required placeholder="alice">
        </label>
        <label>Peer host
          <input name="peer_host" value="127.0.0.1" required>
        </label>
        <label>Peer TCP port
          <input name="peer_port" type="number" min="1" max="65535" value="{{ suggested_peer_port }}" required>
        </label>
        <label>Bootstrap host
          <input name="bootstrap_host" value="127.0.0.1" required>
        </label>
        <label>Bootstrap port
          <input name="bootstrap_port" type="number" min="1" max="65535" value="9000" required>
        </label>
        <button class="span-2" type="submit">Register / Join Network</button>
        <div id="register-error" class="error span-2"></div>
      </div>
    </form>
  </section>

  <section id="dashboard" class="hidden">
    <header class="status">
      <div>
        <strong id="status-title">Not registered</strong>
        <small id="status-subtitle"></small>
      </div>
      <button id="disconnect" class="danger" type="button">Disconnect</button>
    </header>
    <main class="app">
      <aside class="panel">
        <div class="row">
          <h2 style="margin-bottom: 0;">Known Peers</h2>
          <button id="refresh-peers" class="secondary" type="button">Refresh</button>
        </div>
        <div id="peers" class="peer-list" style="margin-top: 14px;"></div>
      </aside>
      <section class="panel stack">
        <div>
          <h2>Direct Chat</h2>
          <form id="direct-form" class="stack">
            <label>Target peer
              <select name="target" required></select>
            </label>
            <label>Message
              <textarea name="message" required placeholder="Message for one peer"></textarea>
            </label>
            <button type="submit">Send Direct Message</button>
          </form>
          <form id="direct-file-form" class="stack" style="margin-top: 14px;">
            <label>Target peer
              <select id="direct-file-target" name="target" required></select>
            </label>
            <label>File
              <input name="file" type="file" required>
            </label>
            <button type="submit">Send File</button>
          </form>
        </div>
        <div>
          <h2>Create Group</h2>
          <form id="create-group-form" class="stack">
            <label>Group name
              <input name="group_name" required placeholder="number">
            </label>
            <button type="submit">Create Group</button>
          </form>
        </div>
        <div>
          <div class="row">
            <h2 style="margin-bottom: 0;">Join / Leave Group</h2>
            <button id="refresh-groups" class="secondary" type="button">Refresh</button>
          </div>
          <div id="groups" class="group-list" style="margin-top: 14px;"></div>
        </div>
        <div>
          <h2>Send Group Message</h2>
          <form id="send-group-form" class="stack">
            <label>Group
              <select id="group-send-select" name="group_id"></select>
            </label>
            <label>Message
              <textarea name="message" required placeholder="Message for selected group"></textarea>
            </label>
            <button type="submit">Send to Group</button>
          </form>
          <form id="group-file-form" class="stack" style="margin-top: 14px;">
            <label>Group
              <select id="group-file-select" name="group_id"></select>
            </label>
            <label>File
              <input name="file" type="file" required>
            </label>
            <button type="submit">Send File to Group</button>
          </form>
        </div>
        <div>
          <h2>Broadcast</h2>
          <form id="broadcast-form" class="stack">
            <label>Message
              <textarea name="message" required placeholder="Message for all online peers"></textarea>
            </label>
            <button type="submit">Broadcast</button>
          </form>
          <form id="broadcast-file-form" class="stack" style="margin-top: 14px;">
            <label>File
              <input name="file" type="file" required>
            </label>
            <button type="submit">Send File to All</button>
          </form>
        </div>
        <div id="send-status" class="notice" role="status" aria-live="polite"></div>
      </section>
      <section class="panel">
        <h2>Message Log</h2>
        <div id="logs" class="logs"></div>
        <h2 style="margin-top: 16px;">Received Files</h2>
        <div id="received-files" class="received-files"></div>
      </section>
    </main>
  </section>

  <script>
    const registration = document.getElementById("registration");
    const dashboard = document.getElementById("dashboard");
    const registerError = document.getElementById("register-error");
    const peersBox = document.getElementById("peers");
    const groupsBox = document.getElementById("groups");
    const logsBox = document.getElementById("logs");
    const receivedFilesBox = document.getElementById("received-files");
    const sendStatus = document.getElementById("send-status");
    const targetSelect = document.querySelector("#direct-form select[name='target']");
    const directFileTargetSelect = document.getElementById("direct-file-target");
    const groupSendSelect = document.getElementById("group-send-select");
    const groupFileSelect = document.getElementById("group-file-select");
    let peersCache = [];
    let allGroupsCache = [];
    let myGroupsCache = [];
    let pollTimer = null;

    async function api(path, options = {}, timeoutMs = 3000) {
      const controller = new AbortController();
      const timeout = setTimeout(() => controller.abort(), timeoutMs);
      try {
        const response = await fetch(path, {
          headers: { "Content-Type": "application/json" },
          cache: "no-store",
          signal: controller.signal,
          ...options
        });
        const data = await response.json();
        if (!response.ok || data.ok === false) {
          throw new Error(data.error || "Request failed");
        }
        return data;
      } finally {
        clearTimeout(timeout);
      }
    }

    async function apiForm(path, form, timeoutMs = 30000) {
      const controller = new AbortController();
      const timeout = setTimeout(() => controller.abort(), timeoutMs);
      try {
        const response = await fetch(path, {
          method: "POST",
          body: new FormData(form),
          cache: "no-store",
          signal: controller.signal
        });
        const data = await response.json();
        if (!response.ok || data.ok === false) {
          throw new Error(data.error || "Request failed");
        }
        return data;
      } finally {
        clearTimeout(timeout);
      }
    }

    function formPayload(form) {
      const data = new FormData(form);
      return Object.fromEntries(data.entries());
    }

    function setSendStatus(message, level = "info") {
      if (!sendStatus) return;
      sendStatus.textContent = message;
      sendStatus.className = `notice ${level === "error" ? "error" : (level === "ok" ? "ok" : "")}`;
    }

    function appendPendingLog(kind, message) {
      return null;
    }

    function updatePendingLog(line, kind, message, level = "ok") {
      return;
    }

    function fileResultMessage(data, prefix) {
      const sent = data.sent_to || [];
      const skipped = data.skipped || [];
      const parts = [`${prefix}: ${data.filename}`];
      if (sent.length) parts.push(`sent_to: ${sent.join(", ")}`);
      if (skipped.length) parts.push(`skipped: ${skipped.join(", ")}`);
      return parts.join(" | ");
    }

    async function handleFileSend(event, config) {
      event.preventDefault();
      const form = document.getElementById(config.formId);
      const button = form.querySelector("button[type='submit']");
      const fileInput = form.querySelector("input[type='file']");
      if (config.validate) {
        const validationError = config.validate(form);
        if (validationError) {
          setSendStatus(validationError, "error");
          return;
        }
      }
      if (!fileInput || !fileInput.files || !fileInput.files.length) {
        setSendStatus("Please choose a file.", "error");
        return;
      }
      if (button) button.disabled = true;
      setSendStatus("Sending file...", "info");
      try {
        const data = await apiForm(config.path(form), form);
        form.reset();
        setSendStatus(fileResultMessage(data, config.successPrefix(data)), "ok");
        await refreshLogs();
      } catch (error) {
        setSendStatus(error.name === "AbortError" ? "File transfer timed out." : error.message, "error");
        await refreshLogs().catch(() => {});
      } finally {
        if (button) button.disabled = false;
      }
    }

    async function handleSend(event, config) {
      event.preventDefault();
      const form = document.getElementById(config.formId);
      if (!form) {
        setSendStatus("Message form is not available.", "error");
        return;
      }
      const button = form.querySelector("button[type='submit']");
      if (button && button.disabled) return;
      const payload = config.payload(form);
      if (config.validate) {
        const validationError = config.validate(payload);
        if (validationError) {
          setSendStatus(validationError, "error");
          return;
        }
      }
      const pendingLine = appendPendingLog(config.pendingType, config.pendingText(payload));
      if (button) button.disabled = true;
      setSendStatus("Sending...", "info");

      try {
        const data = await api(config.path, { method: "POST", body: JSON.stringify(payload) }, 3000);
        const status = data.status || "ack";
        const label = status === "queued_offline"
          ? (data.message || `${data.target || "Target peer"} is offline. Message queued.`)
          : (status === "queued"
          ? (data.message || "Message queued. It will be delivered when target peer is online.")
          : (status === "sent" ? "Message sent." : (status === "sent_no_ack" ? "sent, no ACK yet" : "ACK received")));
        if (form.elements.message) form.elements.message.value = "";
        updatePendingLog(pendingLine, config.doneType, `${config.doneText(payload)} (${label})`, "ok");
        setSendStatus(label, "ok");
        refreshGroups().catch(() => {});
        refreshLogs().catch(() => {});
      } catch (error) {
        updatePendingLog(pendingLine, "ERROR", error.name === "AbortError" ? "Request timed out. Message may still be in progress." : error.message, "error");
        setSendStatus(error.name === "AbortError" ? "Request timed out. Message may still be in progress." : error.message, "error");
        refreshLogs().catch(() => {});
      } finally {
        if (button) button.disabled = false;
      }
    }

    function showRegistered(me) {
      registration.classList.add("hidden");
      dashboard.classList.remove("hidden");
      document.getElementById("status-title").textContent = `${me.username} is online`;
      document.getElementById("status-subtitle").textContent =
        `Peer ${me.peer_host}:${me.peer_port} | Bootstrap ${me.bootstrap_host}:${me.bootstrap_port}`;
      if (!pollTimer) {
        pollTimer = setInterval(() => {
          refreshPeers().catch(() => {});
          refreshGroups().catch(() => {});
          refreshLogs().catch(() => {});
          refreshReceivedFiles().catch(() => {});
        }, 2500);
      }
    }

    function showRegistration() {
      dashboard.classList.add("hidden");
      registration.classList.remove("hidden");
      if (pollTimer) {
        clearInterval(pollTimer);
        pollTimer = null;
      }
      peersBox.innerHTML = "";
      groupsBox.innerHTML = "";
      logsBox.innerHTML = "";
      receivedFilesBox.innerHTML = "";
      targetSelect.innerHTML = "";
      directFileTargetSelect.innerHTML = "";
      groupSendSelect.innerHTML = "";
      groupFileSelect.innerHTML = "";
      peersCache = [];
      allGroupsCache = [];
      myGroupsCache = [];
      setSendStatus("", "info");
    }

    async function refreshMe() {
      const data = await api("/api/me");
      if (data.registered) {
        showRegistered(data.peer);
        await refreshPeers();
        await refreshGroups();
        await refreshLogs();
        await refreshReceivedFiles();
      } else {
        showRegistration();
      }
    }

    async function refreshPeers() {
      const data = await api("/api/peers");
      const peers = data.peers || [];
      if (data.bootstrap_offline) {
        setSendStatus(data.warning || "Bootstrap offline: discovery/status/offline queue unavailable.", "error");
      }
      peersCache = peers;
      peersBox.innerHTML = peers.length ? "" : "<p class='note'>No other peers registered in this bootstrap session.</p>";
      const selectedDirect = targetSelect.value;
      const selectedDirectFile = directFileTargetSelect.value;
      targetSelect.innerHTML = "";
      directFileTargetSelect.innerHTML = "";
      targetSelect.appendChild(new Option("Select a target peer", ""));
      directFileTargetSelect.appendChild(new Option("Select a target peer", ""));
      for (const peer of peers) {
        const item = document.createElement("div");
        item.className = "peer-item";
        const status = peer.status || "online";
        item.innerHTML = `<div><strong>${peer.peer_id}</strong><span>${peer.host}:${peer.port}<br>${peer.last_seen || ""}</span></div><span class="badge ${status}">${status}</span>`;
        peersBox.appendChild(item);

        const direct = document.createElement("option");
        direct.value = peer.peer_id;
        direct.textContent = `${peer.peer_id} (${status})`;
        targetSelect.appendChild(direct);
        if (status === "online") {
          const directFile = document.createElement("option");
          directFile.value = peer.peer_id;
          directFile.textContent = `${peer.peer_id} (${status})`;
          directFileTargetSelect.appendChild(directFile);
        }
      }
      if (selectedDirect && Array.from(targetSelect.options).some(option => option.value === selectedDirect)) {
        targetSelect.value = selectedDirect;
      } else {
        targetSelect.value = "";
      }
      if (selectedDirectFile && Array.from(directFileTargetSelect.options).some(option => option.value === selectedDirectFile)) {
        directFileTargetSelect.value = selectedDirectFile;
      } else {
        directFileTargetSelect.value = "";
      }
    }

    async function refreshGroups() {
      const data = await api("/api/groups");
      if (data.bootstrap_offline) {
        setSendStatus(data.warning || "Bootstrap offline: discovery/status/offline queue unavailable.", "error");
      }
      allGroupsCache = data.all_groups || data.groups || [];
      myGroupsCache = data.my_groups || data.groups || [];
      groupsBox.innerHTML = allGroupsCache.length ? "" : "<p class='note'>No groups yet.</p>";
      const selected = groupSendSelect.value;
      const selectedFileGroup = groupFileSelect.value;
      groupSendSelect.innerHTML = "";
      groupFileSelect.innerHTML = "";
      groupSendSelect.appendChild(new Option("Select a group", ""));
      groupFileSelect.appendChild(new Option("Select a group", ""));
      for (const group of allGroupsCache) {
        const item = document.createElement("div");
        item.className = "group-item";
        const title = document.createElement("strong");
        title.textContent = group.group_name;
        const meta = document.createElement("span");
        const status = group.is_member ? "joined" : "not joined";
        meta.textContent = `Owner: ${group.owner} | Members: ${(group.members || []).join(", ") || "none"} | Count: ${group.member_count || 0} | Status: ${status}`;
        const button = document.createElement("button");
        button.type = "button";
        button.dataset.groupId = group.group_id;
        if (group.is_member) {
          button.className = "danger";
          button.textContent = "Leave Group";
          button.addEventListener("click", () => leaveGroup(group.group_id));
        } else {
          button.textContent = "Join Group";
          button.addEventListener("click", () => joinGroup(group.group_id));
        }
        item.append(title, meta, button);
        groupsBox.appendChild(item);
      }
      for (const group of myGroupsCache) {
        const option = document.createElement("option");
        option.value = group.group_id;
        option.textContent = `${group.group_name} (${group.member_count || (group.members || []).length})`;
        groupSendSelect.appendChild(option);
        const fileOption = document.createElement("option");
        fileOption.value = group.group_id;
        fileOption.textContent = option.textContent;
        groupFileSelect.appendChild(fileOption);
      }
      if (selected && Array.from(groupSendSelect.options).some(option => option.value === selected)) {
        groupSendSelect.value = selected;
      }
      if (selectedFileGroup && Array.from(groupFileSelect.options).some(option => option.value === selectedFileGroup)) {
        groupFileSelect.value = selectedFileGroup;
      }
    }

    async function joinGroup(groupId) {
      if (!groupId) return setSendStatus("Please select a group.", "error");
      try {
        const data = await api(`/api/groups/${groupId}/join`, { method: "POST", body: "{}" });
        const groupName = (data.group && data.group.group_name) || groupId;
        setSendStatus(`Joined group ${groupName}`, "ok");
        await refreshGroups();
      } catch (error) {
        setSendStatus(error.message, "error");
      }
    }

    async function leaveGroup(groupId) {
      if (!groupId) return setSendStatus("Please select a group.", "error");
      try {
        const data = await api(`/api/groups/${groupId}/leave`, { method: "POST", body: "{}" });
        const groupName = data.group_name || groupId;
        setSendStatus(`Left group ${groupName}`, "ok");
        await refreshGroups();
        await refreshLogs();
      } catch (error) {
        setSendStatus(error.message, "error");
      }
    }

    async function refreshLogs() {
      const data = await api("/api/logs");
      logsBox.innerHTML = "";
      for (const log of data.messages || []) {
        const line = document.createElement("div");
        line.className = "log-line";
        const text = document.createElement("span");
        text.textContent = `[${log.timestamp}] ${log.message}`;
        line.appendChild(text);
        if (log.type === "file" && log.direction === "received") {
          line.appendChild(fileActions(log.download_url, log.open_url));
        }
        logsBox.appendChild(line);
      }
      logsBox.scrollTop = logsBox.scrollHeight;
    }

    function fileActions(downloadUrl, openUrl) {
      const actions = document.createElement("span");
      actions.className = "file-actions";
      const download = document.createElement("a");
      download.className = "file-link";
      download.href = downloadUrl;
      download.textContent = "Download";
      const open = document.createElement("a");
      open.className = "file-link";
      open.href = openUrl;
      open.target = "_blank";
      open.rel = "noopener";
      open.textContent = "Open";
      actions.append(download, open);
      return actions;
    }

    function formatBytes(size) {
      const value = Number(size || 0);
      if (value < 1024) return `${value} B`;
      if (value < 1024 * 1024) return `${(value / 1024).toFixed(1)} KB`;
      return `${(value / (1024 * 1024)).toFixed(1)} MB`;
    }

    async function refreshReceivedFiles() {
      const data = await api("/api/files/received");
      const files = data.files || [];
      receivedFilesBox.innerHTML = "";
      if (!files.length) {
        receivedFilesBox.innerHTML = "<p class='note'>No received files yet.</p>";
        return;
      }
      for (const file of files) {
        const row = document.createElement("div");
        row.className = "file-row";
        const name = document.createElement("strong");
        name.textContent = file.filename || file.saved_filename || "file";
        const details = document.createElement("span");
        details.textContent = `${file.from || "unknown"} | ${formatBytes(file.size)} | ${file.time || ""}`;
        const actions = fileActions(file.download_url, file.open_url);
        actions.className = "file-row-actions";
        row.append(name, details, actions);
        receivedFilesBox.appendChild(row);
      }
    }

    document.getElementById("register-form").addEventListener("submit", async (event) => {
      event.preventDefault();
      registerError.textContent = "";
      const payload = formPayload(event.currentTarget);
      payload.peer_port = Number(payload.peer_port);
      payload.bootstrap_port = Number(payload.bootstrap_port);
      try {
        const data = await api("/api/register", { method: "POST", body: JSON.stringify(payload) });
        showRegistered(data.peer);
        await refreshPeers();
        await refreshGroups();
        await refreshLogs();
        await refreshReceivedFiles();
      } catch (error) {
        registerError.textContent = error.message;
      }
    });

    document.getElementById("refresh-peers").addEventListener("click", () => {
      refreshPeers().then(refreshGroups).then(refreshLogs).then(refreshReceivedFiles).catch(error => setSendStatus(error.message, "error"));
    });

    document.getElementById("refresh-groups").addEventListener("click", () => {
      refreshGroups().catch(error => setSendStatus(error.message, "error"));
    });

    document.getElementById("create-group-form").addEventListener("submit", async (event) => {
      event.preventDefault();
      const payload = formPayload(event.currentTarget);
      try {
        await api("/api/groups/create", { method: "POST", body: JSON.stringify(payload) });
        event.currentTarget.reset();
        setSendStatus("Group created.", "ok");
        await refreshGroups();
      } catch (error) {
        setSendStatus(error.message, "error");
      }
    });

    document.getElementById("send-group-form").addEventListener("submit", (event) => {
      const groupId = groupSendSelect.value;
      if (!groupId) {
        event.preventDefault();
        setSendStatus("Please select a group.", "error");
        return;
      }
      handleSend(event, {
        formId: "send-group-form",
        path: `/api/groups/${groupId}/send`,
        payload: formPayload,
        validate: payload => !String(payload.message || "").trim() ? "Please enter a message." : "",
        pendingType: "SENT",
        pendingText: payload => `Group message: ${payload.message}`,
        doneType: "SENT",
        doneText: payload => `Group message: ${payload.message}`
      });
    });

    document.getElementById("direct-form").addEventListener("submit", (event) => {
      handleSend(event, {
        formId: "direct-form",
        path: "/api/chat",
        payload: formPayload,
        pendingType: "SENT",
        pendingText: payload => `Direct message to ${payload.target}: ${payload.message}`,
        doneType: "SENT",
        doneText: payload => `Direct message to ${payload.target}: ${payload.message}`
      });
    });

    document.getElementById("direct-file-form").addEventListener("submit", (event) => {
      handleFileSend(event, {
        formId: "direct-file-form",
        path: () => "/api/files/direct",
        validate: form => !String(form.elements.target.value || "").trim() ? "Please select a target peer." : "",
        successPrefix: data => `File sent to ${(data.sent_to || []).join(", ") || "peer"}`
      });
    });

    document.getElementById("broadcast-form").addEventListener("submit", (event) => {
      handleSend(event, {
        formId: "broadcast-form",
        path: "/api/broadcast",
        payload: formPayload,
        pendingType: "SENT",
        pendingText: payload => `Broadcast: ${payload.message}`,
        doneType: "SENT",
        doneText: payload => `Broadcast: ${payload.message}`
      });
    });

    document.getElementById("group-file-form").addEventListener("submit", (event) => {
      handleFileSend(event, {
        formId: "group-file-form",
        path: () => "/api/files/group",
        validate: form => !String(form.elements.group_id.value || "").trim() ? "Please select a group." : "",
        successPrefix: () => "Group file transfer"
      });
    });

    document.getElementById("broadcast-file-form").addEventListener("submit", (event) => {
      handleFileSend(event, {
        formId: "broadcast-file-form",
        path: () => "/api/files/broadcast",
        successPrefix: () => "Broadcast file transfer"
      });
    });

    document.getElementById("disconnect").addEventListener("click", async () => {
      try {
        await api("/api/disconnect", { method: "POST", body: "{}" });
      } finally {
        showRegistration();
      }
    });

    refreshMe().catch(() => showRegistration());
  </script>
</body>
</html>
"""


CHAT_LOG_TYPES = {"CHAT", "GROUP_CHAT", "BROADCAST", "QUEUED", "DELIVERED", "SYSTEM", "FILE_SENT", "FILE_RECEIVED"}


class WebPeerState:
    def __init__(self) -> None:
        self.node: PeerNode | None = None
        self.bootstrap_host = DEFAULT_BOOTSTRAP_HOST
        self.bootstrap_port = DEFAULT_BOOTSTRAP_PORT
        self.chat_logs: list[dict[str, Any]] = []
        self.received_files: list[dict[str, Any]] = []
        self.lock = threading.RLock()

    def add_chat_log(self, item: dict[str, Any]) -> None:
        from common.utils import utc_now_iso

        if str(item.get("type") or "CHAT") not in CHAT_LOG_TYPES:
            return

        with_timestamp = dict(item)
        with_timestamp.setdefault("timestamp", utc_now_iso())
        with self.lock:
            peer_id = self.node.peer_id if self.node is not None else str(with_timestamp.get("to") or "")
            timestamp = _format_log_time(str(with_timestamp.get("timestamp", "")))
            log_entry: dict[str, Any] = {
                "timestamp": timestamp,
                "message": _format_chat_message(with_timestamp, peer_id),
            }
            if str(with_timestamp.get("type") or "") == "FILE_RECEIVED":
                file_entry = _received_file_entry(with_timestamp, timestamp)
                self.received_files.append(file_entry)
                self.received_files = self.received_files[-200:]
                log_entry.update(
                    {
                        "type": "file",
                        "direction": "received",
                        "from": file_entry["from"],
                        "to": file_entry["to"],
                        "filename": file_entry["filename"],
                        "saved_filename": file_entry["saved_filename"],
                        "file_path": file_entry["file_path"],
                        "size": file_entry["size"],
                        "time": file_entry["time"],
                        "download_url": file_entry["download_url"],
                        "open_url": file_entry["open_url"],
                    }
                )
            self.chat_logs = self.chat_logs[-200:]
            self.chat_logs.append(log_entry)
            self.chat_logs = self.chat_logs[-200:]

    def set_node(self, node: PeerNode, bootstrap_host: str, bootstrap_port: int) -> None:
        with self.lock:
            self.node = node
            self.bootstrap_host = bootstrap_host
            self.bootstrap_port = bootstrap_port

    def clear_node(self) -> None:
        with self.lock:
            self.node = None

    def clear_runtime_state(self) -> None:
        with self.lock:
            self.node = None
            self.chat_logs = []
            self.received_files = []


def create_app(web_port: int = 8001) -> Flask:
    app = Flask(__name__)
    state = WebPeerState()
    suggested_peer_port = _suggest_peer_port(web_port)

    @app.after_request
    def no_cache(response):
        if request.path.startswith("/api/"):
            response.headers["Cache-Control"] = "no-store, no-cache, must-revalidate, max-age=0"
            response.headers["Pragma"] = "no-cache"
            response.headers["Expires"] = "0"
        return response

    @app.get("/")
    def index():
        return render_template_string(PAGE, suggested_peer_port=suggested_peer_port)

    @app.get("/api/defaults")
    def api_defaults():
        return jsonify(
            {
                "ok": True,
                "peer_host": DEFAULT_PEER_HOST,
                "peer_port": suggested_peer_port,
                "bootstrap_host": DEFAULT_BOOTSTRAP_HOST,
                "bootstrap_port": DEFAULT_BOOTSTRAP_PORT,
            }
        )

    @app.post("/api/register")
    def api_register():
        data = _json_body()
        try:
            username = _clean_username(data.get("username"))
            peer_host = str(data.get("peer_host") or DEFAULT_PEER_HOST).strip()
            peer_port = _parse_port(data.get("peer_port"), "peer_port")
            bootstrap_host = str(data.get("bootstrap_host") or DEFAULT_BOOTSTRAP_HOST).strip()
            bootstrap_port = _parse_port(data.get("bootstrap_port") or DEFAULT_BOOTSTRAP_PORT, "bootstrap_port")
        except ValueError as exc:
            return _error(str(exc), 400)

        with state.lock:
            if state.node is not None:
                return _error("This Web UI instance is already registered. Disconnect first.", 409)

        try:
            discovery = PeerDiscovery(bootstrap_host, bootstrap_port)
            peers = discovery.list_peers(None)
        except (OSError, socket.timeout, RuntimeError) as exc:
            message = f"Bootstrap server offline or unreachable: {exc}"
            return _error(message, 503)

        for peer in peers:
            if str(peer.get("peer_id")) != username or str(peer.get("status") or "online") != "online":
                continue
            if str(peer.get("host")) != peer_host or int(peer.get("port") or 0) != peer_port:
                message = f"Username {username} is already online at {peer.get('host')}:{peer.get('port')}."
                return _error(message, 409)

        node = PeerNode(
            peer_id=username,
            host=peer_host,
            port=peer_port,
            bootstrap_host=bootstrap_host,
            bootstrap_port=bootstrap_port,
        )
        state.clear_runtime_state()
        state.set_node(node, bootstrap_host, bootstrap_port)
        node.handler.on_chat = lambda message: state.add_chat_log(message)

        try:
            node.start()
        except OSError as exc:
            message = f"Peer port is already in use on this machine: {peer_host}:{peer_port} ({exc})"
            node.stop()
            state.clear_runtime_state()
            return _error(message, 409)
        except (RuntimeError, socket.timeout, OSError) as exc:
            message = f"Could not register with bootstrap server: {exc}"
            node.stop()
            state.clear_runtime_state()
            return _error(message, 503)
        except Exception as exc:
            message = f"Registration failed: {exc}"
            node.stop()
            state.clear_runtime_state()
            return _error(message, 500)

        return jsonify({"ok": True, "peer": _peer_info(state)})

    @app.get("/api/me")
    def api_me():
        return jsonify({"ok": True, "registered": state.node is not None, "peer": _peer_info(state)})

    @app.get("/api/peers")
    def api_peers():
        node = _require_node(state)
        if node is None:
            return _error("Peer is not registered.", 400)
        try:
            peers = node.refresh_peers()
            return jsonify({"ok": True, "peers": peers, "bootstrap_online": True})
        except Exception as exc:
            return jsonify(
                {
                    "ok": True,
                    "peers": node.cached_peers(),
                    "bootstrap_online": False,
                    "bootstrap_offline": True,
                    "warning": "Bootstrap offline: discovery/status/offline queue unavailable.",
                    "details": str(exc),
                }
            )

    @app.post("/api/chat")
    def api_chat():
        node = _require_node(state)
        if node is None:
            return _error("Peer is not registered.", 400)
        data = _json_body()
        target = str(data.get("target") or "").strip()
        message = str(data.get("message") or "").strip()
        if not target:
            return _error("Please select a target peer.", 400)
        if target == node.peer_id:
            return _error("Cannot send a direct message to yourself.", 400)
        if not message:
            return _error("message is required.", 400)
        target_peer = node.get_peer_or_refresh(target)
        if target_peer is None:
            if not node.bootstrap_online:
                return _error("Target not found in local cache and bootstrap is offline.", 404)
            return _error(f"Unknown target peer: {target}", 404)
        status = node.send_chat_status(target, message, queue_on_fail=True)
        if status in {"ack", "sent_no_ack"}:
            state.add_chat_log({"type": "CHAT", "from": node.peer_id, "to": target, "message": message})
        if status == "queued":
            notice = f"{target} is offline. Message queued."
            state.add_chat_log({"type": "QUEUED", "to": target, "message": f"{target} is offline. Message will be delivered when they reconnect."})
            return jsonify({"ok": True, "status": "queued_offline", "target": target, "message": notice})
        if status == "ack":
            return jsonify({"ok": True, "status": "sent", "target": target, "message": message})
        if status == "sent_no_ack":
            return jsonify({"ok": True, "status": "sent_no_ack", "target": target, "message": message})
        if not node.bootstrap_online:
            return _error(f"Could not deliver to {target}; bootstrap offline so message cannot be queued.", 504)
        text = f"Connection timeout or delivery failure for peer {target}."
        return _error(text, 504)

    @app.post("/api/broadcast")
    def api_broadcast():
        node = _require_node(state)
        if node is None:
            return _error("Peer is not registered.", 400)
        data = _json_body()
        message = str(data.get("message") or "").strip()
        if not message:
            return _error("message is required.", 400)
        results = node.broadcast(message)
        failed = [peer_id for peer_id, ok in results.items() if not ok]
        if failed:
            text = f"Broadcast delivery failed for: {', '.join(failed)}"
            if not node.bootstrap_online:
                text = f"{text}; bootstrap offline so messages cannot be queued."
            return _error(text, 504, {"results": results})
        state.add_chat_log({"type": "BROADCAST", "from": node.peer_id, "message": message})
        return jsonify({"ok": True, "status": "ack", "results": results})

    @app.post("/api/files/direct")
    def api_file_direct():
        node = _require_node(state)
        if node is None:
            return _error("Peer is not registered.", 400)
        target = str(request.form.get("target") or "").strip()
        if not target:
            return _error("Please select a target peer.", 400)
        try:
            filename, temp_path = _save_uploaded_file()
            result = node.send_file_direct(target, temp_path)
        except FileTransferError as exc:
            return _error(str(exc), 400)
        finally:
            _cleanup_temp_file(locals().get("temp_path"))
        state.add_chat_log({"type": "FILE_SENT", "from": node.peer_id, "to": target, "mode": "direct", "filename": filename})
        return jsonify({"ok": True, "filename": filename, **result})

    @app.post("/api/files/group")
    def api_file_group():
        node = _require_node(state)
        if node is None:
            return _error("Peer is not registered.", 400)
        group_id = str(request.form.get("group_id") or "").strip()
        if not group_id:
            return _error("Please select a group.", 400)
        try:
            group = node.get_group(group_id)
            filename, temp_path = _save_uploaded_file()
            result = node.send_file_group(group_id, temp_path)
        except FileTransferError as exc:
            return _error(str(exc), 400)
        except Exception as exc:
            return _error(str(exc), 400)
        finally:
            _cleanup_temp_file(locals().get("temp_path"))
        state.add_chat_log(
            {
                "type": "FILE_SENT",
                "from": node.peer_id,
                "mode": "group",
                "group_id": group_id,
                "group_name": group.get("group_name"),
                "filename": filename,
            }
        )
        if result.get("skipped"):
            state.add_chat_log({"type": "SYSTEM", "message": f"Skipped offline members: {', '.join(result['skipped'])}"})
        return jsonify({"ok": True, "filename": filename, **result})

    @app.post("/api/files/broadcast")
    def api_file_broadcast():
        node = _require_node(state)
        if node is None:
            return _error("Peer is not registered.", 400)
        try:
            filename, temp_path = _save_uploaded_file()
            result = node.send_file_broadcast(temp_path)
        except FileTransferError as exc:
            return _error(str(exc), 400)
        finally:
            _cleanup_temp_file(locals().get("temp_path"))
        state.add_chat_log({"type": "FILE_SENT", "from": node.peer_id, "mode": "broadcast", "filename": filename})
        return jsonify({"ok": True, "filename": filename, **result})

    @app.post("/api/groups/create")
    def api_group_create():
        node = _require_node(state)
        if node is None:
            return _error("Peer is not registered.", 400)
        data = _json_body()
        group_name = str(data.get("group_name") or "").strip()
        members = data.get("members") or []
        if isinstance(members, str):
            members = [item.strip() for item in members.split(",") if item.strip()]
        if not group_name:
            return _error("group_name is required.", 400)
        try:
            group = node.create_group(group_name, [str(member) for member in members])
            return jsonify({"ok": True, "group": group})
        except Exception as exc:
            return _error(str(exc), 400)

    @app.get("/api/groups")
    def api_groups():
        node = _require_node(state)
        if node is None:
            return _error("Peer is not registered.", 400)
        try:
            views = node.list_group_views()
            payload = {
                "ok": True,
                "all_groups": views.get("all_groups", []),
                "my_groups": views.get("my_groups", []),
            }
            if not node.bootstrap_online:
                payload.update(
                    {
                        "bootstrap_online": False,
                        "bootstrap_offline": True,
                        "warning": "Bootstrap offline: discovery/status/offline queue unavailable.",
                    }
                )
            return jsonify(payload)
        except Exception as exc:
            views = node.cached_group_views()
            return jsonify(
                {
                    "ok": True,
                    "all_groups": views.get("all_groups", []),
                    "my_groups": views.get("my_groups", []),
                    "bootstrap_online": False,
                    "bootstrap_offline": True,
                    "warning": "Bootstrap offline: discovery/status/offline queue unavailable.",
                    "details": str(exc),
                }
            )

    @app.get("/api/groups/<group_id>")
    def api_group_get(group_id: str):
        node = _require_node(state)
        if node is None:
            return _error("Peer is not registered.", 400)
        try:
            return jsonify({"ok": True, "group": node.get_group(group_id)})
        except Exception as exc:
            return _error(str(exc), 404)

    @app.post("/api/groups/<group_id>/add-member")
    def api_group_add_member(group_id: str):
        node = _require_node(state)
        if node is None:
            return _error("Peer is not registered.", 400)
        member = str(_json_body().get("member") or "").strip()
        if not member:
            return _error("member is required.", 400)
        try:
            return jsonify({"ok": True, "group": node.add_group_member(group_id, member)})
        except PermissionError as exc:
            return _error(str(exc), 403)
        except Exception as exc:
            return _error(str(exc), 400)

    @app.post("/api/groups/<group_id>/join")
    def api_group_join(group_id: str):
        node = _require_node(state)
        if node is None:
            return _error("Peer is not registered.", 400)
        if not str(group_id or "").strip():
            return _error("group_id is required.", 400)
        try:
            group = node.join_group(group_id)
            return jsonify({"ok": True, "group": group})
        except Exception as exc:
            return _error(str(exc), 400)

    @app.post("/api/groups/<group_id>/remove-member")
    def api_group_remove_member(group_id: str):
        node = _require_node(state)
        if node is None:
            return _error("Peer is not registered.", 400)
        member = str(_json_body().get("member") or "").strip()
        if not member:
            return _error("member is required.", 400)
        try:
            return jsonify({"ok": True, "group": node.remove_group_member(group_id, member)})
        except PermissionError as exc:
            return _error(str(exc), 403)
        except Exception as exc:
            return _error(str(exc), 400)

    @app.post("/api/groups/<group_id>/leave")
    def api_group_leave(group_id: str):
        node = _require_node(state)
        if node is None:
            return _error("Peer is not registered.", 400)
        try:
            group = node.get_group(group_id)
            result = node.leave_group(group_id)
            group_name = str(group.get("group_name") or group_id)
            state.add_chat_log({"type": "SYSTEM", "message": f"{node.peer_id} left Group({group_name})"})
            return jsonify({"ok": True, "group": result, "group_id": group_id, "group_name": group_name})
        except Exception as exc:
            return _error(str(exc), 400)

    @app.post("/api/groups/<group_id>/send")
    def api_group_send(group_id: str):
        node = _require_node(state)
        if node is None:
            return _error("Peer is not registered.", 400)
        if not str(group_id or "").strip():
            return _error("group_id is required.", 400)
        message = str(_json_body().get("message") or "").strip()
        if not message:
            return _error("Please enter a message.", 400)
        try:
            group = node.get_group(group_id)
            results = node.send_managed_group(group_id, message)
        except Exception as exc:
            return _error(str(exc), 400)
        state.add_chat_log(
            {
                "type": "GROUP_CHAT",
                "from": node.peer_id,
                "to": group_id,
                "group_id": group_id,
                "group_name": group.get("group_name"),
                "recipients": group.get("members", []),
                "message": message,
            }
        )
        for peer_id, status in results.items():
            if status == "queued":
                state.add_chat_log(
                    {
                        "type": "QUEUED",
                        "to": peer_id,
                        "group_id": group_id,
                        "group_name": group.get("group_name"),
                        "message": f"{peer_id} is offline",
                    }
                )
            if status == "failed" and not node.bootstrap_online:
                state.add_chat_log(
                    {
                        "type": "SYSTEM",
                        "message": f"Could not deliver to {peer_id}; bootstrap offline so message cannot be queued.",
                    }
                )
        sent_to = [peer_id for peer_id, value in results.items() if value in {"ack", "sent_no_ack"}]
        queued_for = [peer_id for peer_id, value in results.items() if value == "queued"]
        failed_for = [peer_id for peer_id, value in results.items() if value == "failed"]
        status = "queued_offline" if queued_for else "sent"
        message_text = "One or more group members are offline. Message queued." if queued_for else message
        if failed_for and not node.bootstrap_online:
            message_text = "; ".join(
                f"Could not deliver to {peer_id}; bootstrap offline so message cannot be queued."
                for peer_id in failed_for
            )
        return jsonify(
            {
                "ok": True,
                "status": status,
                "message": message_text,
                "group_id": group_id,
                "group_name": group.get("group_name"),
                "sent_to": sent_to,
                "queued_for": queued_for,
                "failed_for": failed_for,
                "results": results,
            }
        )

    @app.post("/api/messages/offline/queue")
    def api_offline_queue():
        node = _require_node(state)
        if node is None:
            return _error("Peer is not registered.", 400)
        data = _json_body()
        target = str(data.get("to") or data.get("target") or "").strip()
        message = str(data.get("message") or "").strip()
        offline_type = str(data.get("type") or "direct").strip()
        if not target or not message:
            return _error("to and message are required.", 400)
        try:
            item = node.discovery.queue_offline_message(
                node.peer_id,
                target,
                message,
                offline_type=offline_type,
                group_id=str(data.get("group_id")) if data.get("group_id") else None,
                group_name=str(data.get("group_name")) if data.get("group_name") else None,
            )
        except Exception as exc:
            return _error(str(exc), 503)
        state.add_chat_log({"type": "QUEUED", "to": target, "message": message, "group_name": data.get("group_name")})
        return jsonify({"ok": True, "offline_message": item, "status": "queued"})

    @app.get("/api/messages/offline/pending")
    def api_offline_pending():
        node = _require_node(state)
        if node is None:
            return _error("Peer is not registered.", 400)
        messages = node.fetch_pending_offline_messages()
        return jsonify({"ok": True, "messages": messages})

    @app.post("/api/messages/offline/mark-delivered")
    def api_offline_mark_delivered():
        node = _require_node(state)
        if node is None:
            return _error("Peer is not registered.", 400)
        data = _json_body()
        message_ids = [str(value) for value in data.get("message_ids", [])]
        try:
            delivered = node.discovery.mark_offline_delivered(node.peer_id, message_ids)
        except Exception as exc:
            return _error(str(exc), 503)
        return jsonify({"ok": True, "delivered": delivered})

    @app.get("/api/logs")
    def api_logs():
        with state.lock:
            messages = list(state.chat_logs)
        return jsonify({"ok": True, "messages": messages[-200:]})

    @app.get("/api/files/received")
    def api_files_received():
        node = _require_node(state)
        if node is None:
            return _error("Peer is not registered.", 403)
        with state.lock:
            files = list(state.received_files)
        return jsonify({"ok": True, "files": files[-200:]})

    @app.get("/api/files/download/<path:filename>")
    def api_file_download(filename: str):
        result = _received_file_for_request(state, filename)
        if not isinstance(result, dict):
            return result
        directory = result["directory"]
        saved_filename = result["saved_filename"]
        return send_from_directory(directory, saved_filename, as_attachment=True)

    @app.get("/api/files/open/<path:filename>")
    def api_file_open(filename: str):
        result = _received_file_for_request(state, filename)
        if not isinstance(result, dict):
            return result
        directory = result["directory"]
        saved_filename = result["saved_filename"]
        return send_from_directory(directory, saved_filename, as_attachment=False)

    @app.post("/api/disconnect")
    def api_disconnect():
        error = _stop_current_node(state)
        if error:
            return _error(f"Disconnect error: {error}", 500)
        return jsonify({"ok": True})

    @app.post("/api/reset-local-state")
    def api_reset_local_state():
        error = _stop_current_node(state)
        if error:
            return _error(f"Reset error: {error}", 500)
        return jsonify({"ok": True})

    return app


def _json_body() -> dict[str, Any]:
    data = request.get_json(silent=True)
    return data if isinstance(data, dict) else {}


def _save_uploaded_file() -> tuple[str, Path]:
    upload = request.files.get("file")
    if upload is None or not upload.filename:
        raise FileTransferError("Please choose a file.")
    filename = Path(upload.filename).name
    temp_dir = Path(tempfile.mkdtemp(prefix="p2p-file-"))
    temp_path = temp_dir / filename
    try:
        upload.save(str(temp_path))
        size = temp_path.stat().st_size
        if size <= 0:
            raise FileTransferError("File is empty.")
        if size > MAX_FILE_SIZE_MB * 1024 * 1024:
            raise FileTransferError(f"File is too large. Max size is {MAX_FILE_SIZE_MB} MB.")
    except Exception:
        _cleanup_temp_file(temp_path)
        raise
    return filename, temp_path


def _cleanup_temp_file(path: Any) -> None:
    if not path:
        return
    try:
        target = Path(path)
        parent = target.parent
        target.unlink(missing_ok=True)
        if parent.name.startswith("p2p-file-"):
            parent.rmdir()
    except OSError:
        pass


def _clean_username(value: Any) -> str:
    username = str(value or "").strip()
    if not username:
        raise ValueError("username is required.")
    if any(char.isspace() for char in username):
        raise ValueError("username cannot contain spaces.")
    return username


def _parse_port(value: Any, field: str) -> int:
    try:
        port = int(value)
    except (TypeError, ValueError) as exc:
        raise ValueError(f"{field} must be a valid port number.") from exc
    if port < 1 or port > 65535:
        raise ValueError(f"{field} must be between 1 and 65535.")
    return port


def _require_node(state: WebPeerState) -> PeerNode | None:
    with state.lock:
        return state.node


def _stop_current_node(state: WebPeerState) -> str | None:
    with state.lock:
        node = state.node
        state.node = None
        state.chat_logs = []
        state.received_files = []
    if node is None:
        return None
    try:
        node.stop()
    except Exception as exc:
        return str(exc)
    finally:
        state.clear_runtime_state()
    return None


def _peer_info(state: WebPeerState) -> dict[str, Any] | None:
    node = state.node
    if node is None:
        return None
    return {
        "username": node.peer_id,
        "peer_host": node.host,
        "peer_port": node.port,
        "bootstrap_host": state.bootstrap_host,
        "bootstrap_port": state.bootstrap_port,
    }


def _recipients(item: dict[str, Any]) -> list[str]:
    raw = item.get("recipients")
    if isinstance(raw, list):
        return [str(value) for value in raw]
    if raw:
        return [str(raw)]
    target = item.get("to")
    return [str(target)] if target else []


def _received_file_entry(item: dict[str, Any], timestamp: str) -> dict[str, Any]:
    saved_filename = Path(str(item.get("saved_filename") or Path(str(item.get("saved_path") or "")).name or item.get("filename") or "file")).name
    filename = Path(str(item.get("filename") or saved_filename)).name
    encoded = quote(saved_filename, safe="")
    return {
        "type": "file",
        "direction": "received",
        "from": str(item.get("from") or "unknown"),
        "to": str(item.get("to") or ""),
        "filename": filename,
        "saved_filename": saved_filename,
        "file_path": str(item.get("file_path") or item.get("saved_path") or ""),
        "size": int(item.get("size") or item.get("filesize") or 0),
        "time": timestamp,
        "download_url": f"/api/files/download/{encoded}",
        "open_url": f"/api/files/open/{encoded}",
    }


def _received_file_for_request(state: WebPeerState, filename: str):
    node = _require_node(state)
    if node is None:
        return _error("Peer is not registered.", 403)

    requested = str(filename or "")
    requested_path = Path(requested)
    if requested_path.is_absolute() or requested_path.name != requested or requested in {"", ".", ".."}:
        return _error("Invalid filename.", 403)

    with state.lock:
        allowed = {
            str(item.get("saved_filename") or "")
            for item in state.received_files
            if str(item.get("saved_filename") or "") == requested
        }
    if requested not in allowed:
        return _error("File not found.", 404)

    received_dir = (node.storage_root / "received_files" / node.peer_id).resolve()
    target = (received_dir / requested).resolve()
    try:
        target.relative_to(received_dir)
    except ValueError:
        return _error("Invalid filename.", 403)
    if not target.is_file():
        return _error("File not found.", 404)
    return {"directory": received_dir, "saved_filename": requested}


def _format_chat_message(item: dict[str, Any], peer_id: str) -> str:
    sender = str(item.get("from") or "unknown")
    sender_label = "Me" if sender == peer_id else sender
    message_type = str(item.get("type") or "CHAT")
    text = str(item.get("message") or "")

    if message_type == "QUEUED":
        target = str(item.get("to") or "")
        group_name = item.get("group_name")
        if group_name:
            return f"queued for {target} in Group({group_name}): {text}"
        return f"queued for {target}: {text}"

    if message_type == "DELIVERED":
        target = str(item.get("to") or "")
        return f"delivered to {target}: {text}"

    if message_type == "SYSTEM":
        return text

    if message_type in {"FILE_SENT", "FILE_RECEIVED"}:
        filename = str(item.get("filename") or "file")
        mode = str(item.get("mode") or "direct")
        group_name = str(item.get("group_name") or item.get("group_id") or "").strip()
        if message_type == "FILE_RECEIVED":
            if mode == "group":
                return f"{sender_label} sent file to Group({group_name}): {filename}"
            if mode == "broadcast":
                return f"{sender_label} broadcasted file: {filename}"
            return f"{sender} sent file: {filename}"
        if mode == "group":
            return f"{sender_label} sent file to Group({group_name}): {filename}"
        if mode == "broadcast":
            return f"{sender_label} broadcasted file: {filename}"
        target = str(item.get("to") or "")
        return f"{sender_label} sent file to {target}: {filename}"

    if message_type == "BROADCAST":
        target_label = "Everyone" if sender == peer_id else "Me"
    elif message_type == "GROUP_CHAT":
        recipients = _recipients(item)
        group_name = str(item.get("group_name") or "").strip()
        if sender == peer_id:
            target_label = f"Group({group_name})" if group_name else f"Group({', '.join(recipients)})"
        else:
            others = [recipient for recipient in recipients if recipient != peer_id]
            target_label = f"Group({group_name})" if group_name else (f"Group({', '.join(others)})" if others else "Me")
    else:
        target = str(item.get("to") or peer_id)
        target_label = "Me" if target == peer_id else target

    return f"{sender_label} \u2192 {target_label}: {text}"


def _format_log_time(value: str) -> str:
    if "T" in value:
        return value.split("T", 1)[1].split(".", 1)[0].replace("Z", "")[:5]
    if len(value) >= 5 and value[2:3] == ":":
        return value[:5]
    return value


def _suggest_peer_port(web_port: int) -> int:
    if web_port >= 8001:
        return 5000 + (web_port - 8000)
    return 5001


def _error(message: str, status: int, extra: dict[str, Any] | None = None):
    payload: dict[str, Any] = {"ok": False, "error": message}
    if extra:
        payload.update(extra)
    return jsonify(payload), status


def main() -> None:
    parser = argparse.ArgumentParser(description="Run Flask Web UI for browser-registered P2P peers")
    parser.add_argument("--web-host", default="127.0.0.1")
    parser.add_argument("--web-port", type=int, default=8001)
    args = parser.parse_args()

    app = create_app(args.web_port)
    app.run(host=args.web_host, port=args.web_port, debug=False, use_reloader=False)


if __name__ == "__main__":
    main()
