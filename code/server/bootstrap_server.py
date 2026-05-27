"""Bootstrap server for peer registration, discovery, groups, and offline queues."""

from __future__ import annotations

import argparse
import json
import socket
import threading
from urllib.parse import parse_qs, unquote

from common.constants import SOCKET_BACKLOG, SOCKET_TIMEOUT_SECONDS
from common.logger import get_logger
from common.protocol import ProtocolError, build_error, build_message, recv_message, send_message
from server.config import BOOTSTRAP_HOST, BOOTSTRAP_PORT, REGISTRY_FILE
from server.peer_registry import PeerRegistry

logger = get_logger("bootstrap")


class BootstrapServer:
    def __init__(self, host: str = BOOTSTRAP_HOST, port: int = BOOTSTRAP_PORT) -> None:
        self.host = host
        self.port = port
        self.registry = PeerRegistry(REGISTRY_FILE)
        self._shutdown = threading.Event()

    def serve_forever(self) -> None:
        with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as server_socket:
            server_socket.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
            server_socket.bind((self.host, self.port))
            server_socket.listen(SOCKET_BACKLOG)
            server_socket.settimeout(1.0)
            logger.info("Bootstrap server listening on %s:%s", self.host, self.port)

            while not self._shutdown.is_set():
                try:
                    client_socket, address = server_socket.accept()
                except socket.timeout:
                    continue
                thread = threading.Thread(
                    target=self._handle_client,
                    args=(client_socket, address),
                    daemon=True,
                )
                thread.start()

    def shutdown(self) -> None:
        self._shutdown.set()

    def _handle_client(self, client_socket: socket.socket, address: tuple[str, int]) -> None:
        with client_socket:
            client_socket.settimeout(SOCKET_TIMEOUT_SECONDS)
            if self._looks_like_http(client_socket):
                self._handle_http(client_socket)
                return
            try:
                request = recv_message(client_socket)
                response = self._dispatch(request, address)
            except (ProtocolError, OSError) as exc:
                logger.warning("Invalid request from %s: %s", address, exc)
                response = build_error("BAD_REQUEST", str(exc))
            send_message(client_socket, response)

    def _dispatch(self, request: dict, address: tuple[str, int]) -> dict:
        message_type = request["type"]
        peer_id = request.get("peer_id") or request.get("from")

        if message_type == "REGISTER":
            if not peer_id or not request.get("port"):
                return build_error("INVALID_REGISTER", "REGISTER requires peer_id and port", request["message_id"])
            host = request.get("host") or address[0]
            try:
                self.registry.register(str(peer_id), str(host), int(request["port"]))
            except ValueError as exc:
                return build_error("REGISTER_FAILED", str(exc), request["message_id"])
            logger.info("Registered peer %s at %s:%s", peer_id, host, request["port"])
            return build_message(
                "PEER_LIST",
                peers=self.registry.list_peers(exclude_peer_id=str(peer_id)),
                pending_count=len(self.registry.pending_messages(str(peer_id))),
                registered=True,
                correlation_id=request["message_id"],
            )

        if message_type == "PEER_LIST":
            return build_message(
                "PEER_LIST",
                peers=self.registry.list_peers(exclude_peer_id=str(peer_id) if peer_id else None),
                correlation_id=request["message_id"],
            )

        if message_type == "HEARTBEAT":
            if not peer_id:
                return build_error("INVALID_HEARTBEAT", "HEARTBEAT requires peer_id", request["message_id"])
            ok = self.registry.heartbeat(str(peer_id))
            if not ok:
                return build_error("UNKNOWN_PEER", f"Peer {peer_id} is not registered", request["message_id"])
            return build_message("ACK", ack_for=request["message_id"], from_peer="bootstrap")

        if message_type == "DISCONNECT":
            if peer_id:
                self.registry.mark_offline(str(peer_id), "offline")
                logger.info("Marked peer %s offline", peer_id)
            return build_message("ACK", ack_for=request["message_id"], from_peer="bootstrap")

        if message_type == "GROUP_CREATE":
            if not peer_id or not request.get("group_name"):
                return build_error("INVALID_GROUP_CREATE", "GROUP_CREATE requires peer_id and group_name", request["message_id"])
            try:
                group = self.registry.create_group(
                    str(peer_id),
                    str(request["group_name"]).strip(),
                    [str(member) for member in request.get("members", [])],
                )
            except ValueError as exc:
                return build_error("GROUP_CREATE_FAILED", str(exc), request["message_id"])
            return build_message("GROUP_GET", group=group, correlation_id=request["message_id"])

        if message_type == "GROUP_LIST":
            peer_text = str(peer_id) if peer_id else None
            views = self.registry.list_group_views(peer_text) if peer_text else {"all_groups": self.registry.list_groups(), "my_groups": []}
            return build_message(
                "GROUP_LIST",
                groups=views["my_groups"],
                all_groups=views["all_groups"],
                my_groups=views["my_groups"],
                correlation_id=request["message_id"],
            )

        if message_type == "GROUP_GET":
            group_id = str(request.get("group_id") or "")
            group = self.registry.get_group(group_id, str(peer_id) if peer_id else None)
            if group is None:
                return build_error("GROUP_NOT_FOUND", f"Group not found: {group_id}", request["message_id"])
            return build_message("GROUP_GET", group=group, correlation_id=request["message_id"])

        if message_type in {"GROUP_JOIN", "GROUP_ADD_MEMBER", "GROUP_REMOVE_MEMBER", "GROUP_LEAVE"}:
            if not peer_id or not request.get("group_id"):
                return build_error("INVALID_GROUP_REQUEST", f"{message_type} requires peer_id and group_id", request["message_id"])
            try:
                if message_type == "GROUP_JOIN":
                    group = self.registry.join_group(str(request["group_id"]), str(peer_id))
                elif message_type == "GROUP_ADD_MEMBER":
                    group = self.registry.add_group_member(str(request["group_id"]), str(peer_id), str(request.get("member") or ""))
                elif message_type == "GROUP_REMOVE_MEMBER":
                    group = self.registry.remove_group_member(str(request["group_id"]), str(peer_id), str(request.get("member") or ""))
                else:
                    group = self.registry.leave_group(str(request["group_id"]), str(peer_id))
            except PermissionError as exc:
                return build_error("GROUP_FORBIDDEN", str(exc), request["message_id"])
            except ValueError as exc:
                return build_error("GROUP_UPDATE_FAILED", str(exc), request["message_id"])
            return build_message("GROUP_GET", group=group, correlation_id=request["message_id"])

        if message_type == "OFFLINE_QUEUE":
            if not request.get("from") or not request.get("to") or not request.get("message"):
                return build_error("INVALID_OFFLINE_QUEUE", "OFFLINE_QUEUE requires from, to, and message", request["message_id"])
            item = self.registry.queue_message(
                str(request.get("offline_type") or "direct"),
                str(request["from"]),
                str(request["to"]),
                str(request["message"]),
                str(request["group_id"]) if request.get("group_id") else None,
                str(request["group_name"]) if request.get("group_name") else None,
            )
            return build_message("OFFLINE_QUEUE", offline_message=item, correlation_id=request["message_id"])

        if message_type == "OFFLINE_PENDING":
            if not peer_id:
                return build_error("INVALID_OFFLINE_PENDING", "OFFLINE_PENDING requires peer_id", request["message_id"])
            return build_message(
                "OFFLINE_PENDING",
                messages=self.registry.pending_messages(str(peer_id)),
                correlation_id=request["message_id"],
            )

        if message_type == "OFFLINE_MARK_DELIVERED":
            if not peer_id:
                return build_error("INVALID_OFFLINE_MARK_DELIVERED", "OFFLINE_MARK_DELIVERED requires peer_id", request["message_id"])
            delivered = self.registry.mark_messages_delivered(
                str(peer_id),
                [str(message_id) for message_id in request.get("message_ids", [])],
            )
            return build_message(
                "OFFLINE_MARK_DELIVERED",
                delivered=delivered,
                correlation_id=request["message_id"],
            )

        return build_error("UNSUPPORTED_MESSAGE", f"Bootstrap cannot handle {message_type}", request["message_id"])

    @staticmethod
    def _looks_like_http(client_socket: socket.socket) -> bool:
        try:
            first = client_socket.recv(1, socket.MSG_PEEK)
        except OSError:
            return False
        return first in {b"G", b"P", b"D", b"H", b"O"}

    def _handle_http(self, client_socket: socket.socket) -> None:
        try:
            method, path, body = self._read_http_request(client_socket)
            status, payload = self._dispatch_http(method, path, body)
        except Exception as exc:
            logger.warning("HTTP API request failed: %s", exc)
            status, payload = 500, {"ok": False, "error": str(exc)}
        self._send_http_json(client_socket, status, payload)

    def _read_http_request(self, client_socket: socket.socket) -> tuple[str, str, dict]:
        data = b""
        while b"\r\n\r\n" not in data:
            chunk = client_socket.recv(4096)
            if not chunk:
                break
            data += chunk
            if len(data) > 1024 * 1024:
                raise ValueError("HTTP request too large")
        header_bytes, _, body_bytes = data.partition(b"\r\n\r\n")
        lines = header_bytes.decode("iso-8859-1").split("\r\n")
        method, raw_path, _version = lines[0].split(" ", 2)
        headers: dict[str, str] = {}
        for line in lines[1:]:
            name, _, value = line.partition(":")
            headers[name.lower()] = value.strip()
        content_length = int(headers.get("content-length") or 0)
        while len(body_bytes) < content_length:
            body_bytes += client_socket.recv(content_length - len(body_bytes))
        parsed_body = json.loads(body_bytes.decode("utf-8")) if body_bytes.strip() else {}
        body = parsed_body if isinstance(parsed_body, dict) else {}
        raw_path_only, _, query_string = raw_path.partition("?")
        path = unquote(raw_path_only)
        query = {key: values[-1] for key, values in parse_qs(query_string).items() if values}
        body.update(query)
        return method.upper(), path, body

    def _dispatch_http(self, method: str, path: str, body: dict) -> tuple[int, dict]:
        try:
            if method == "POST" and path == "/api/register":
                peer_id = str(body.get("username") or body.get("peer_id") or "").strip()
                host = str(body.get("host") or body.get("peer_host") or "").strip()
                port = int(body.get("port") or body.get("peer_port") or 0)
                if not peer_id or not host or not port:
                    return 400, {"ok": False, "error": "username, host, and port are required"}
                self.registry.register(peer_id, host, port)
                return 200, {"ok": True, "peers": self.registry.list_peers(exclude_peer_id=peer_id)}
            if method == "POST" and path == "/api/disconnect":
                peer_id = str(body.get("username") or body.get("peer_id") or "").strip()
                if peer_id:
                    self.registry.mark_offline(peer_id, "offline")
                return 200, {"ok": True}
            if method == "GET" and path == "/api/peers":
                return 200, {"ok": True, "peers": self.registry.list_peers()}
            if method == "GET" and path == "/api/groups":
                return 200, {"ok": True, "groups": self.registry.list_groups(), "all_groups": self.registry.list_groups()}
            if method == "POST" and path == "/api/groups/create":
                group = self.registry.create_group(
                    str(body.get("owner") or body.get("peer_id") or ""),
                    str(body.get("group_name") or "").strip(),
                    [str(member) for member in body.get("members", [])],
                )
                return 200, {"ok": True, "group": group}
            if method == "POST" and path.startswith("/api/groups/"):
                parts = path.strip("/").split("/")
                if len(parts) == 4 and parts[0] == "api" and parts[1] == "groups":
                    group_id, action = parts[2], parts[3]
                    peer_id = str(body.get("username") or body.get("peer_id") or "").strip()
                    if action == "join":
                        return 200, {"ok": True, "group": self.registry.join_group(group_id, peer_id)}
                    if action == "leave":
                        return 200, {"ok": True, "group": self.registry.leave_group(group_id, peer_id)}
            if method == "POST" and path == "/api/messages/offline/queue":
                item = self.registry.queue_message(
                    str(body.get("type") or body.get("offline_type") or "direct"),
                    str(body.get("from") or ""),
                    str(body.get("to") or ""),
                    str(body.get("message") or ""),
                    str(body.get("group_id")) if body.get("group_id") else None,
                    str(body.get("group_name")) if body.get("group_name") else None,
                )
                return 200, {"ok": True, "offline_message": item}
            if method == "GET" and path == "/api/messages/offline/pending":
                peer_id = str(body.get("username") or body.get("peer_id") or "").strip()
                if not peer_id:
                    return 400, {"ok": False, "error": "peer_id is required"}
                return 200, {"ok": True, "messages": self.registry.pending_messages(peer_id)}
            if method == "POST" and path == "/api/messages/offline/mark-delivered":
                peer_id = str(body.get("username") or body.get("peer_id") or "").strip()
                message_ids = [str(message_id) for message_id in body.get("message_ids", [])]
                if not peer_id:
                    return 400, {"ok": False, "error": "peer_id is required"}
                return 200, {"ok": True, "delivered": self.registry.mark_messages_delivered(peer_id, message_ids)}
            if method == "GET" and path == "/api/admin/peers":
                return 200, {"ok": True, "peers": self.registry.list_peers()}
            if method == "GET" and path == "/api/admin/groups":
                return 200, {"ok": True, "groups": self.registry.list_groups()}
            if method == "GET" and path == "/api/admin/offline-messages":
                return 200, {"ok": True, "messages": self.registry.list_offline_messages()}
            if method == "GET" and path == "/api/admin/stats":
                payload = {"ok": True}
                payload.update(self.registry.stats())
                return 200, payload
        except (ValueError, PermissionError) as exc:
            return 400, {"ok": False, "error": str(exc)}
        return 404, {"ok": False, "error": f"Unknown endpoint: {method} {path}"}

    @staticmethod
    def _send_http_json(client_socket: socket.socket, status: int, payload: dict) -> None:
        reason = {200: "OK", 400: "Bad Request", 404: "Not Found", 500: "Internal Server Error"}.get(status, "Error")
        body = json.dumps(payload, separators=(",", ":")).encode("utf-8")
        headers = (
            f"HTTP/1.1 {status} {reason}\r\n"
            "Content-Type: application/json\r\n"
            "Cache-Control: no-store\r\n"
            f"Content-Length: {len(body)}\r\n"
            "Connection: close\r\n\r\n"
        ).encode("ascii")
        client_socket.sendall(headers + body)


def main() -> None:
    parser = argparse.ArgumentParser(description="Run the P2P bootstrap server")
    parser.add_argument("--host", default=BOOTSTRAP_HOST)
    parser.add_argument("--port", type=int, default=BOOTSTRAP_PORT)
    args = parser.parse_args()
    server = BootstrapServer(args.host, args.port)
    server.serve_forever()


if __name__ == "__main__":
    main()
