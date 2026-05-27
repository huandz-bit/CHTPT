"""Interactive peer node that acts as both client and server."""

from __future__ import annotations

import argparse
import threading
import time
from pathlib import Path

from common.constants import (
    DEFAULT_BOOTSTRAP_HOST,
    DEFAULT_BOOTSTRAP_PORT,
    DEFAULT_PEER_HOST,
    OFFLINE_RETRY_INTERVAL_SECONDS,
)
from common.logger import get_logger
from common.protocol import build_message
from common.utils import FernetCipher, append_jsonl, read_json_file, write_json_file
from peer.file_transfer import FileTransferError, send_file_to_peer
from peer.group_chat import send_group
from peer.heartbeat import HeartbeatLoop
from peer.message_handler import MessageHandler
from peer.peer_client import AckTimeoutError, DeliveryError, PeerClient
from peer.peer_discovery import PeerDiscovery
from peer.peer_server import PeerServer

logger = get_logger("peer")


class PeerNode:
    def __init__(
        self,
        peer_id: str,
        host: str = DEFAULT_PEER_HOST,
        port: int = 0,
        bootstrap_host: str = DEFAULT_BOOTSTRAP_HOST,
        bootstrap_port: int = DEFAULT_BOOTSTRAP_PORT,
        storage_root: str | Path = "storage",
    ) -> None:
        self.peer_id = peer_id
        self.host = host
        self.port = port
        self.storage_root = Path(storage_root)
        self.cipher = FernetCipher.from_env()
        self.client = PeerClient()
        self.discovery = PeerDiscovery(bootstrap_host, bootstrap_port)
        self.known_peers: dict[str, dict] = {}
        self.groups_cache: dict[str, dict] = {}
        self.bootstrap_online = False
        self._peer_lock = threading.RLock()
        self._stop_retry = threading.Event()
        self._retry_thread: threading.Thread | None = None

        self.handler = MessageHandler(peer_id, self.storage_root, self.cipher, on_broadcast=self._forward_broadcast)
        self.server = PeerServer(host, port, self.handler)
        self.heartbeat = HeartbeatLoop(peer_id, self.discovery)

    def start(self) -> None:
        self.server.start()
        while self.server.bound_port == 0:
            time.sleep(0.01)
        self.port = self.server.bound_port
        self.register()
        self.fetch_pending_offline_messages()
        self.heartbeat.start()
        self.retry_offline_messages()
        self._start_offline_retry_loop()

    def stop(self) -> None:
        self.heartbeat.stop()
        self._stop_retry.set()
        if self._retry_thread:
            self._retry_thread.join(timeout=2.0)
        self.server.stop()
        try:
            self.discovery.disconnect(self.peer_id)
        except Exception as exc:
            logger.warning("Bootstrap disconnect failed: %s", exc)

    def register(self) -> None:
        peers = self.discovery.register(self.peer_id, self.host, self.port)
        self.bootstrap_online = True
        self._replace_peers(peers)
        self.refresh_groups_cache()
        logger.info("Registered as %s at %s:%s", self.peer_id, self.host, self.port)

    def refresh_peers(self) -> list[dict]:
        try:
            peers = self.discovery.list_peers(self.peer_id)
        except Exception:
            self.bootstrap_online = False
            raise
        self.bootstrap_online = True
        self._replace_peers(peers)
        return self.cached_peers()

    def cached_peers(self) -> list[dict]:
        with self._peer_lock:
            return [dict(peer) for peer in sorted(self.known_peers.values(), key=lambda item: str(item.get("peer_id")))]

    def send_chat(
        self,
        target_peer_id: str,
        text: str,
        group: bool = False,
        recipients: list[str] | None = None,
        queue_on_fail: bool = True,
    ) -> bool:
        return self.send_chat_status(target_peer_id, text, group, recipients, queue_on_fail) in {"ack", "sent_no_ack"}

    def send_chat_status(
        self,
        target_peer_id: str,
        text: str,
        group: bool = False,
        recipients: list[str] | None = None,
        queue_on_fail: bool = True,
        group_id: str | None = None,
        group_name: str | None = None,
    ) -> str:
        target = self.get_peer_or_refresh(target_peer_id)
        if target is None:
            print(f"Unknown peer: {target_peer_id}")
            if queue_on_fail:
                self._queue_offline(target_peer_id, text, "group" if group else "direct", group_id, group_name)
                return "queued"
            return "failed"

        if str(target.get("status") or "online") != "online":
            if queue_on_fail and self.bootstrap_online:
                self._queue_offline(target_peer_id, text, "group" if group else "direct", group_id, group_name)
                return "queued"
            logger.info("Using cached address for %s despite cached status %s", target_peer_id, target.get("status"))

        encrypted_text = self.cipher.encrypt(text)
        message = build_message(
            "GROUP_CHAT" if group else "CHAT",
            **{
                "from": self.peer_id,
                "to": target_peer_id,
                "recipients": recipients or [target_peer_id],
                "group_id": group_id,
                "group_name": group_name,
                "message": encrypted_text,
                "encrypted": self.cipher.enabled,
            },
        )
        try:
            self.client.send_with_ack(str(target["host"]), int(target["port"]), message)
            self._store_outgoing(message, text)
            return "ack"
        except AckTimeoutError as exc:
            logger.warning("%s", exc)
            self._store_outgoing(message, text)
            return "sent_no_ack"
        except DeliveryError as exc:
            logger.warning("%s", exc)
            if queue_on_fail and self.bootstrap_online:
                self._queue_offline(target_peer_id, text, "group" if group else "direct", group_id, group_name)
                return "queued"
            return "failed"

    def group_chat(self, peer_ids: list[str], text: str) -> dict[str, bool]:
        return send_group(self, peer_ids, text)

    def create_group(self, group_name: str, members: list[str] | None = None) -> dict:
        group = self.discovery.create_group(self.peer_id, group_name, members or [])
        self.bootstrap_online = True
        self._cache_group(group)
        return group

    def list_groups(self) -> list[dict]:
        return self.list_group_views()["my_groups"]

    def list_group_views(self) -> dict[str, list[dict]]:
        try:
            views = self.discovery.list_group_views(self.peer_id)
        except Exception:
            self.bootstrap_online = False
            return self.cached_group_views()
        self.bootstrap_online = True
        self._replace_groups(views.get("all_groups", []))
        return self.cached_group_views()

    def refresh_groups_cache(self) -> dict[str, list[dict]]:
        return self.list_group_views()

    def cached_group_views(self) -> dict[str, list[dict]]:
        with self._peer_lock:
            groups = [self._group_for_current_user(group) for group in self.groups_cache.values()]
        groups.sort(key=lambda item: str(item.get("group_name")))
        return {
            "all_groups": groups,
            "my_groups": [group for group in groups if group.get("is_member")],
        }

    def get_group(self, group_id: str) -> dict:
        with self._peer_lock:
            cached = self.groups_cache.get(group_id)
        if cached is not None:
            return self._group_for_current_user(cached)
        group = self.discovery.get_group(self.peer_id, group_id)
        self.bootstrap_online = True
        self._cache_group(group)
        return self._group_for_current_user(group)

    def add_group_member(self, group_id: str, member: str) -> dict:
        group = self.discovery.add_group_member(self.peer_id, group_id, member)
        self.bootstrap_online = True
        self._cache_group(group)
        return group

    def join_group(self, group_id: str) -> dict:
        group = self.discovery.join_group(self.peer_id, group_id)
        self.bootstrap_online = True
        self._cache_group(group)
        return group

    def remove_group_member(self, group_id: str, member: str) -> dict:
        group = self.discovery.remove_group_member(self.peer_id, group_id, member)
        self.bootstrap_online = True
        self._cache_group(group)
        return group

    def leave_group(self, group_id: str) -> dict:
        group = self.get_group(group_id)
        remaining = [str(member) for member in group.get("members", []) if str(member) != self.peer_id]
        updated = self.discovery.leave_group(self.peer_id, group_id)
        self.bootstrap_online = True
        if updated:
            self._cache_group(updated)
        else:
            with self._peer_lock:
                self.groups_cache.pop(group_id, None)
        group_name = str(group.get("group_name") or group_id)
        notice = f"{self.peer_id} left Group({group_name})"
        for target in remaining:
            self.send_system_notification(target, notice, str(group.get("group_id") or group_id), group_name)
        return updated

    def send_managed_group(self, group_id: str, text: str) -> dict[str, str]:
        group = self.get_group(group_id)
        members = [str(member) for member in group.get("members", [])]
        if self.peer_id not in members:
            raise RuntimeError("You are not a member of this group.")
        targets = [member for member in members if member != self.peer_id]
        results: dict[str, str] = {}
        for target in targets:
            results[target] = self.send_chat_status(
                target,
                text,
                group=True,
                recipients=members,
                group_id=str(group["group_id"]),
                group_name=str(group["group_name"]),
            )
        return results

    def send_system_notification(
        self,
        target_peer_id: str,
        text: str,
        group_id: str | None = None,
        group_name: str | None = None,
    ) -> str:
        target = self.get_peer_or_refresh(target_peer_id)
        if target is None or str(target.get("status") or "online") != "online":
            self._queue_offline(target_peer_id, text, "system", group_id, group_name)
            return "queued"
        message = build_message(
            "SYSTEM",
            **{
                "from": self.peer_id,
                "to": target_peer_id,
                "group_id": group_id,
                "group_name": group_name,
                "message": text,
                "encrypted": False,
            },
        )
        try:
            self.client.send_with_ack(str(target["host"]), int(target["port"]), message)
            return "ack"
        except AckTimeoutError as exc:
            logger.warning("%s", exc)
            return "sent_no_ack"
        except DeliveryError as exc:
            logger.warning("%s", exc)
            self._queue_offline(target_peer_id, text, "system", group_id, group_name)
            return "queued"

    def broadcast(self, text: str) -> dict[str, bool]:
        peers = self.cached_peers()
        encrypted_text = self.cipher.encrypt(text)
        message = build_message(
            "BROADCAST",
            **{
                "from": self.peer_id,
                "origin": self.peer_id,
                "previous_hop": self.peer_id,
                "message": encrypted_text,
                "encrypted": self.cipher.enabled,
                "hop_count": 0,
            },
        )
        self.handler.seen_message_ids.add(message["message_id"])
        self._store_outgoing(message, text)
        return self._flood_message(
            message,
            [str(peer["peer_id"]) for peer in peers if str(peer.get("status") or "online") == "online"],
        )

    def send_file(self, target_peer_id: str, file_path: str | Path) -> bool:
        return bool(self.send_file_direct(target_peer_id, file_path)["sent_to"])

    def send_file_direct(self, target_peer_id: str, file_path: str | Path) -> dict:
        if not target_peer_id:
            raise FileTransferError("Please select a target peer.")
        if target_peer_id == self.peer_id:
            raise FileTransferError("Cannot send a file to yourself.")
        target = self.get_peer_or_refresh(target_peer_id)
        if target is None:
            if not self.bootstrap_online:
                raise FileTransferError("Target not found in local cache and bootstrap is offline.")
            raise FileTransferError(f"Unknown target peer: {target_peer_id}")
        if str(target.get("status") or "online") != "online":
            raise FileTransferError(f"{target_peer_id} is offline. File transfer requires online peer.")
        send_file_to_peer(self, target, file_path, mode="direct")
        return {"sent_to": [target_peer_id], "skipped": [], "errors": []}

    def send_file_group(self, group_id: str, file_path: str | Path) -> dict:
        if not group_id:
            raise FileTransferError("Please select a group.")
        group = self.get_group(group_id)
        members = [str(member) for member in group.get("members", [])]
        if self.peer_id not in members:
            raise FileTransferError("You are not a member of this group.")
        return self._send_file_many(
            [member for member in members if member != self.peer_id],
            file_path,
            "group",
            {"group_id": str(group.get("group_id") or group_id), "group_name": str(group.get("group_name") or group_id)},
        )

    def send_file_broadcast(self, file_path: str | Path) -> dict:
        peers = [str(peer["peer_id"]) for peer in self.cached_peers() if str(peer.get("peer_id")) != self.peer_id]
        return self._send_file_many(peers, file_path, "broadcast", None)

    def _send_file_many(self, peer_ids: list[str], file_path: str | Path, mode: str, group_info: dict | None) -> dict:
        sent_to: list[str] = []
        skipped: list[str] = []
        errors: list[str] = []
        for peer_id in peer_ids:
            target = self.get_peer_or_refresh(peer_id)
            if target is None or str(target.get("status") or "online") != "online":
                skipped.append(peer_id)
                continue
            try:
                send_file_to_peer(self, target, file_path, mode=mode, group_info=group_info)
                sent_to.append(peer_id)
            except FileTransferError as exc:
                skipped.append(peer_id)
                errors.append(f"{peer_id}: {exc}")
        return {"sent_to": sent_to, "skipped": skipped, "errors": errors}

    def retry_offline_messages(self) -> None:
        queue_file = self._offline_file()
        queued = read_json_file(queue_file, default=[])
        if not isinstance(queued, list) or not queued:
            return
        remaining = []
        for item in queued:
            ok = self.send_chat(
                str(item["to"]),
                str(item["message"]),
                bool(item.get("group", False)),
                list(item.get("recipients") or [item["to"]]),
                queue_on_fail=False,
            )
            if not ok:
                remaining.append(item)
        write_json_file(queue_file, remaining)

    def fetch_pending_offline_messages(self) -> list[dict]:
        try:
            pending = self.discovery.pending_offline_messages(self.peer_id)
        except Exception as exc:
            logger.warning("Could not fetch pending offline messages: %s", exc)
            return []
        delivered_ids: list[str] = []
        for item in pending:
            stored = {
                "type": {
                    "direct": "CHAT",
                    "group": "GROUP_CHAT",
                    "broadcast": "BROADCAST",
                    "system": "SYSTEM",
                }.get(str(item.get("type") or "direct"), "CHAT"),
                "message_id": str(item["message_id"]),
                "timestamp": str(item.get("created_at") or ""),
                "from": str(item.get("from") or "unknown"),
                "to": self.peer_id,
                "group_id": item.get("group_id"),
                "group_name": item.get("group_name"),
                "recipients": [self.peer_id],
                "message": str(item.get("message") or ""),
                "offline": True,
                "delivery_status": "delivered",
            }
            self.handler.handle(stored)
            delivered_ids.append(str(item["message_id"]))
        if delivered_ids:
            try:
                self.discovery.mark_offline_delivered(self.peer_id, delivered_ids)
            except Exception as exc:
                logger.warning("Could not mark offline messages delivered: %s", exc)
        return pending

    def get_peer_or_refresh(self, peer_id: str) -> dict | None:
        target = self._get_peer(peer_id)
        if target is None:
            try:
                self.refresh_peers()
            except Exception as exc:
                logger.warning("Could not refresh peers: %s", exc)
            target = self._get_peer(peer_id)
        return target

    def _start_offline_retry_loop(self) -> None:
        self._stop_retry.clear()
        self._retry_thread = threading.Thread(target=self._offline_retry_loop, daemon=True)
        self._retry_thread.start()

    def _offline_retry_loop(self) -> None:
        while not self._stop_retry.wait(OFFLINE_RETRY_INTERVAL_SECONDS):
            try:
                self.refresh_peers()
                self.fetch_pending_offline_messages()
                self.retry_offline_messages()
            except Exception as exc:
                logger.warning("Offline retry failed: %s", exc)

    def _forward_broadcast(self, message: dict) -> None:
        forwarded = dict(message)
        forwarded["previous_hop"] = self.peer_id
        forwarded["hop_count"] = int(forwarded.get("hop_count", 0)) + 1
        peers = self.cached_peers()
        blocked = {str(message.get("from")), str(message.get("previous_hop")), self.peer_id}
        targets = [
            str(peer["peer_id"])
            for peer in peers
            if str(peer["peer_id"]) not in blocked and str(peer.get("status") or "online") == "online"
        ]
        if targets:
            self._flood_message(forwarded, targets)

    def _flood_message(self, message: dict, peer_ids: list[str]) -> dict[str, bool]:
        results: dict[str, bool] = {}
        lock = threading.Lock()

        def worker(target_peer_id: str) -> None:
            target = self.get_peer_or_refresh(target_peer_id)
            ok = False
            if target is not None:
                outbound = dict(message)
                outbound["from"] = self.peer_id
                try:
                    self.client.send_with_ack(str(target["host"]), int(target["port"]), outbound)
                    ok = True
                except AckTimeoutError as exc:
                    logger.warning("Broadcast sent to %s without ACK: %s", target_peer_id, exc)
                    ok = True
                except DeliveryError as exc:
                    logger.warning("Broadcast delivery to %s failed: %s", target_peer_id, exc)
            with lock:
                results[target_peer_id] = ok

        threads = [threading.Thread(target=worker, args=(peer_id,), daemon=True) for peer_id in peer_ids]
        for thread in threads:
            thread.start()
        for thread in threads:
            thread.join()
        return results

    def _replace_peers(self, peers: list[dict]) -> None:
        with self._peer_lock:
            self.known_peers = {str(peer["peer_id"]): peer for peer in peers}

    def _replace_groups(self, groups: list[dict]) -> None:
        with self._peer_lock:
            self.groups_cache = {str(group["group_id"]): dict(group) for group in groups}

    def _cache_group(self, group: dict | None) -> None:
        if not group:
            return
        with self._peer_lock:
            self.groups_cache[str(group["group_id"])] = dict(group)

    def _group_for_current_user(self, group: dict) -> dict:
        members = [str(member) for member in group.get("members", [])]
        return {
            **dict(group),
            "members": members,
            "member_count": len(members),
            "is_member": self.peer_id in members,
            "is_owner": str(group.get("owner")) == self.peer_id,
        }

    def _get_peer(self, peer_id: str) -> dict | None:
        with self._peer_lock:
            return self.known_peers.get(peer_id)

    def _store_outgoing(self, message: dict, plaintext: str) -> None:
        stored = dict(message)
        stored["message"] = plaintext
        append_jsonl(self.storage_root / "chat_history" / f"{self.peer_id}.jsonl", stored)

    def _queue_offline(
        self,
        target_peer_id: str,
        text: str,
        offline_type: str = "direct",
        group_id: str | None = None,
        group_name: str | None = None,
    ) -> None:
        try:
            self.discovery.queue_offline_message(
                self.peer_id,
                target_peer_id,
                text,
                offline_type=offline_type,
                group_id=group_id,
                group_name=group_name,
            )
            self.bootstrap_online = True
            return
        except Exception as exc:
            self.bootstrap_online = False
            logger.warning("Bootstrap offline queue unavailable: %s", exc)
        queued = read_json_file(self._offline_file(), default=[])
        if not isinstance(queued, list):
            queued = []
        queued.append(
            {
                "to": target_peer_id,
                "message": text,
                "group": offline_type == "group",
                "offline_type": offline_type,
                "group_id": group_id,
                "group_name": group_name,
            }
        )
        write_json_file(self._offline_file(), queued)

    def _offline_file(self) -> Path:
        return self.storage_root / "offline_messages" / f"{self.peer_id}.json"


def print_help() -> None:
    print(
        "Commands:\n"
        "  register                         refresh registration\n"
        "  list                             show known peers and status\n"
        "  chat <peer> <message>            send direct message\n"
        "  groupchat <p1,p2,...> <message>  send group message\n"
        "  creategroup <name>               create a managed group\n"
        "  groups                           list groups you belong to\n"
        "  groupinfo <group_id>             show group members\n"
        "  addmember <group_id> <peer>       add a member as group owner\n"
        "  removemember <group_id> <peer>    remove a member as group owner\n"
        "  leavegroup <group_id>             leave a managed group\n"
        "  sendgroup <group_id> <message>    send to current group members\n"
        "  broadcast <message>              send to all known online peers\n"
        "  sendfile <peer> <path>            send a file to a peer\n"
        "  retry                            retry queued offline messages\n"
        "  exit                             disconnect and quit\n"
    )


def run_cli(node: PeerNode) -> None:
    node.start()
    print_help()
    try:
        while True:
            raw = input(f"{node.peer_id}> ").strip()
            if not raw:
                continue
            command, _, rest = raw.partition(" ")
            if command == "register":
                node.register()
            elif command == "list":
                peers = node.refresh_peers()
                if not peers:
                    print("No other peers registered.")
                for peer in peers:
                    print(f"{peer['peer_id']} {peer['host']}:{peer['port']} {peer.get('status', 'online')}")
            elif command == "chat":
                target, _, text = rest.partition(" ")
                if not target or not text:
                    print("Usage: chat <peer> <message>")
                    continue
                ok = node.send_chat(target, text)
                print("delivered" if ok else "queued/offline")
            elif command == "groupchat":
                targets, _, text = rest.partition(" ")
                if not targets or not text:
                    print("Usage: groupchat <peer1,peer2,...> <message>")
                    continue
                results = node.group_chat([item.strip() for item in targets.split(",") if item.strip()], text)
                print(results)
            elif command == "creategroup":
                if not rest:
                    print("Usage: creategroup <name>")
                    continue
                print(node.create_group(rest))
            elif command == "groups":
                for group in node.list_groups():
                    print(f"{group['group_id']} {group['group_name']} owner={group['owner']} members={','.join(group['members'])}")
            elif command == "groupinfo":
                if not rest:
                    print("Usage: groupinfo <group_id>")
                    continue
                print(node.get_group(rest))
            elif command == "addmember":
                group_id, _, member = rest.partition(" ")
                if not group_id or not member:
                    print("Usage: addmember <group_id> <peer>")
                    continue
                print(node.add_group_member(group_id, member))
            elif command == "removemember":
                group_id, _, member = rest.partition(" ")
                if not group_id or not member:
                    print("Usage: removemember <group_id> <peer>")
                    continue
                print(node.remove_group_member(group_id, member))
            elif command == "leavegroup":
                if not rest:
                    print("Usage: leavegroup <group_id>")
                    continue
                print(node.leave_group(rest))
            elif command == "sendgroup":
                group_id, _, text = rest.partition(" ")
                if not group_id or not text:
                    print("Usage: sendgroup <group_id> <message>")
                    continue
                print(node.send_managed_group(group_id, text))
            elif command == "broadcast":
                if not rest:
                    print("Usage: broadcast <message>")
                    continue
                print(node.broadcast(rest))
            elif command == "sendfile":
                target, _, path = rest.partition(" ")
                if not target or not path:
                    print("Usage: sendfile <peer> <path>")
                    continue
                print("sent" if node.send_file(target, path) else "failed/offline")
            elif command == "retry":
                node.retry_offline_messages()
            elif command == "help":
                print_help()
            elif command == "exit":
                break
            else:
                print("Unknown command. Type 'help'.")
    finally:
        node.stop()


def main() -> None:
    parser = argparse.ArgumentParser(description="Run a P2P chat peer")
    parser.add_argument("--id", required=True, help="Unique peer ID, e.g. alice")
    parser.add_argument("--host", default=DEFAULT_PEER_HOST)
    parser.add_argument("--port", type=int, default=0)
    parser.add_argument("--bootstrap-host", default=DEFAULT_BOOTSTRAP_HOST)
    parser.add_argument("--bootstrap-port", type=int, default=DEFAULT_BOOTSTRAP_PORT)
    args = parser.parse_args()

    node = PeerNode(
        peer_id=args.id,
        host=args.host,
        port=args.port,
        bootstrap_host=args.bootstrap_host,
        bootstrap_port=args.bootstrap_port,
    )
    run_cli(node)


if __name__ == "__main__":
    main()
