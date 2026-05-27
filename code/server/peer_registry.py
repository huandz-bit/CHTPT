"""Thread-safe bootstrap session state for peers, groups, and offline queues."""

from __future__ import annotations

import threading
import time
from dataclasses import dataclass
from pathlib import Path
from typing import Any

from common.constants import PEER_STALE_AFTER_SECONDS
from common.utils import new_message_id, utc_now_iso, write_json_file


@dataclass
class PeerRecord:
    peer_id: str
    host: str
    port: int
    last_seen: float
    registered_at: float
    status: str = "online"

    def public_dict(self) -> dict[str, str | int]:
        return {
            "peer_id": self.peer_id,
            "username": self.peer_id,
            "host": self.host,
            "port": self.port,
            "reserved_port_owner": self.peer_id,
            "status": self.status,
            "last_seen": _iso_from_epoch(self.last_seen),
            "registered_at": _iso_from_epoch(self.registered_at),
        }


class PeerRegistry:
    """In-memory registry scoped to one bootstrap server runtime session."""

    def __init__(self, storage_file: str | Path) -> None:
        self._storage_file = Path(storage_file)
        self._peers: dict[str, PeerRecord] = {}
        self._port_owners: dict[int, str] = {}
        self._groups: dict[str, dict[str, Any]] = {}
        self._offline_messages: list[dict[str, Any]] = []
        self._lock = threading.RLock()
        self._started_at = time.time()
        self._session_id = new_message_id()
        self._clear_storage_snapshot()

    def register(self, peer_id: str, host: str, port: int) -> PeerRecord:
        with self._lock:
            self._mark_stale_locked()
            port = int(port)
            owner = self._port_owners.get(port)
            if owner is not None and owner != peer_id:
                raise ValueError(f"Port {port} is reserved by {owner} in this bootstrap session.")
            record = self._peers.get(peer_id)
            if record is None:
                now = time.time()
                record = PeerRecord(
                    peer_id=peer_id,
                    host=host,
                    port=port,
                    last_seen=now,
                    registered_at=now,
                )
                self._peers[peer_id] = record
                self._port_owners[port] = peer_id
            else:
                if int(record.port) != port:
                    raise ValueError(
                        f"Peer {peer_id} must reuse reserved port {record.port} during this bootstrap session."
                    )
                if record.status == "online" and record.host != host:
                    raise ValueError(
                        f"Username {peer_id} is already online at {record.host}:{record.port}."
                    )
                record.host = host
                record.port = port
                record.last_seen = time.time()
                record.status = "online"
                self._port_owners[port] = peer_id
            self._persist()
            return record

    def heartbeat(self, peer_id: str) -> bool:
        with self._lock:
            record = self._peers.get(peer_id)
            if record is None:
                return False
            record.last_seen = time.time()
            record.status = "online"
            self._persist()
            return True

    def mark_offline(self, peer_id: str, status: str = "offline") -> bool:
        with self._lock:
            record = self._peers.get(peer_id)
            if record is None:
                return False
            record.last_seen = time.time()
            record.status = status if status in {"offline", "disconnected"} else "offline"
            self._persist()
            return True

    def remove(self, peer_id: str) -> bool:
        """Backward-compatible alias: graceful disconnect marks the peer offline."""
        return self.mark_offline(peer_id, "offline")

    def get_peer(self, peer_id: str) -> dict[str, str | int] | None:
        with self._lock:
            self._mark_stale_locked()
            record = self._peers.get(peer_id)
            return record.public_dict() if record else None

    def list_peers(self, exclude_peer_id: str | None = None) -> list[dict[str, str | int]]:
        with self._lock:
            self._mark_stale_locked()
            return [
                record.public_dict()
                for peer_id, record in sorted(self._peers.items())
                if peer_id != exclude_peer_id
            ]

    def create_group(self, owner: str, group_name: str, members: list[str] | None = None) -> dict[str, Any]:
        with self._lock:
            if owner not in self._peers:
                raise ValueError(f"Unknown owner: {owner}")
            member_set = {owner, *(members or [])}
            missing = sorted(peer_id for peer_id in member_set if peer_id not in self._peers)
            if missing:
                raise ValueError(f"Unknown peer(s): {', '.join(missing)}")
            group_id = new_message_id()
            group = {
                "group_id": group_id,
                "group_name": group_name,
                "owner": owner,
                "members": sorted(member_set),
                "created_at": utc_now_iso(),
            }
            self._groups[group_id] = group
            self._persist()
            return self._group_snapshot(group, owner)

    def list_groups(self, peer_id: str | None = None) -> list[dict[str, Any]]:
        with self._lock:
            groups = list(self._groups.values())
            if peer_id is not None:
                groups = [group for group in groups if peer_id in group["members"]]
            return [self._group_snapshot(group, peer_id) for group in sorted(groups, key=lambda item: item["group_name"])]

    def list_group_views(self, peer_id: str) -> dict[str, list[dict[str, Any]]]:
        with self._lock:
            groups = [self._group_snapshot(group, peer_id) for group in sorted(self._groups.values(), key=lambda item: item["group_name"])]
            return {
                "all_groups": groups,
                "my_groups": [group for group in groups if group["is_member"]],
            }

    def get_group(self, group_id: str, peer_id: str | None = None) -> dict[str, Any] | None:
        with self._lock:
            group = self._groups.get(group_id)
            if group is None:
                return None
            if peer_id is not None and peer_id not in group["members"]:
                return None
            return self._group_snapshot(group, peer_id)

    def join_group(self, group_id: str, peer_id: str) -> dict[str, Any]:
        with self._lock:
            group = self._require_group(group_id)
            if peer_id not in self._peers:
                raise ValueError(f"Unknown peer: {peer_id}")
            members = set(group["members"])
            members.add(peer_id)
            group["members"] = sorted(members)
            self._persist()
            return self._group_snapshot(group, peer_id)

    def add_group_member(self, group_id: str, actor: str, member: str) -> dict[str, Any]:
        with self._lock:
            group = self._require_group(group_id)
            self._require_owner(group, actor)
            if member not in self._peers:
                raise ValueError(f"Unknown peer: {member}")
            members = set(group["members"])
            members.add(member)
            group["members"] = sorted(members)
            self._persist()
            return self._group_snapshot(group, actor)

    def remove_group_member(self, group_id: str, actor: str, member: str) -> dict[str, Any]:
        with self._lock:
            group = self._require_group(group_id)
            self._require_owner(group, actor)
            if member == group["owner"]:
                raise ValueError("Owner must use leave group instead.")
            members = set(group["members"])
            members.discard(member)
            group["members"] = sorted(members)
            self._persist()
            return self._group_snapshot(group, actor)

    def leave_group(self, group_id: str, peer_id: str) -> dict[str, Any] | None:
        with self._lock:
            group = self._require_group(group_id)
            members = [member for member in group["members"] if member != peer_id]
            if len(members) == len(group["members"]):
                raise ValueError(f"{peer_id} is not a member of this group.")
            if not members:
                self._groups.pop(group_id, None)
                self._persist()
                return None
            group["members"] = sorted(members)
            if group["owner"] == peer_id:
                group["owner"] = group["members"][0]
            self._persist()
            return self._group_snapshot(group, peer_id)

    def queue_message(
        self,
        message_type: str,
        from_peer: str,
        to_peer: str,
        message: str,
        group_id: str | None = None,
        group_name: str | None = None,
        message_id: str | None = None,
    ) -> dict[str, Any]:
        with self._lock:
            item = {
                "message_id": message_id or new_message_id(),
                "type": message_type,
                "from": from_peer,
                "to": to_peer,
                "group_id": group_id,
                "group_name": group_name,
                "message": message,
                "created_at": utc_now_iso(),
                "delivery_status": "queued",
                "delivered_at": None,
            }
            self._offline_messages.append(item)
            self._persist()
            return dict(item)

    def pending_messages(self, peer_id: str) -> list[dict[str, Any]]:
        with self._lock:
            return [
                dict(item)
                for item in self._offline_messages
                if item["to"] == peer_id and item["delivery_status"] == "queued"
            ]

    def mark_messages_delivered(self, peer_id: str, message_ids: list[str]) -> list[dict[str, Any]]:
        ids = set(message_ids)
        delivered: list[dict[str, Any]] = []
        with self._lock:
            for item in self._offline_messages:
                if item["to"] == peer_id and item["message_id"] in ids and item["delivery_status"] == "queued":
                    item["delivery_status"] = "delivered"
                    item["delivered_at"] = utc_now_iso()
                    delivered.append(dict(item))
            self._persist()
            return delivered

    def list_offline_messages(self, include_delivered: bool = True) -> list[dict[str, Any]]:
        with self._lock:
            messages = self._offline_messages
            if not include_delivered:
                messages = [item for item in messages if item["delivery_status"] == "queued"]
            return [dict(item) for item in messages]

    def clear_offline_messages(self) -> int:
        with self._lock:
            count = len(self._offline_messages)
            self._offline_messages = []
            self._persist()
            return count

    def stats(self) -> dict[str, Any]:
        with self._lock:
            self._mark_stale_locked()
            status_counts: dict[str, int] = {"online": 0, "offline": 0, "disconnected": 0}
            for record in self._peers.values():
                status_counts[record.status] = status_counts.get(record.status, 0) + 1
            queued = [item for item in self._offline_messages if item["delivery_status"] == "queued"]
            return {
                "started_at": _iso_from_epoch(self._started_at),
                "session_id": self._session_id,
                "uptime_seconds": int(time.time() - self._started_at),
                "peer_count": len(self._peers),
                "status_counts": status_counts,
                "group_count": len(self._groups),
                "queued_offline_messages": len(queued),
                "offline_message_count": len(self._offline_messages),
            }

    def _mark_stale_locked(self) -> None:
        now = time.time()
        changed = False
        for record in self._peers.values():
            if record.status == "online" and now - record.last_seen > PEER_STALE_AFTER_SECONDS:
                record.status = "disconnected"
                changed = True
        if changed:
            self._persist()

    def _require_group(self, group_id: str) -> dict[str, Any]:
        group = self._groups.get(group_id)
        if group is None:
            raise ValueError(f"Unknown group: {group_id}")
        return group

    @staticmethod
    def _group_snapshot(group: dict[str, Any], peer_id: str | None = None) -> dict[str, Any]:
        members = list(group["members"])
        return {
            **dict(group),
            "members": members,
            "member_count": len(members),
            "is_member": bool(peer_id and peer_id in members),
            "is_owner": bool(peer_id and group["owner"] == peer_id),
        }

    @staticmethod
    def _require_owner(group: dict[str, Any], actor: str) -> None:
        if group["owner"] != actor:
            raise PermissionError("Only the group owner can change members.")

    def _persist(self) -> None:
        return

    def _clear_storage_snapshot(self) -> None:
        data = {
            "peers": {},
            "groups": {},
            "offline_messages": [],
            "session_id": self._session_id,
            "started_at": _iso_from_epoch(self._started_at),
        }
        write_json_file(self._storage_file, data)


def _iso_from_epoch(value: float) -> str:
    import datetime as _dt

    return _dt.datetime.fromtimestamp(value, tz=_dt.timezone.utc).replace(microsecond=0).isoformat()
