"""Bootstrap-server client used by peers for discovery."""

from __future__ import annotations

import socket

from common.constants import SOCKET_TIMEOUT_SECONDS
from common.protocol import build_message, recv_message, send_message


class PeerDiscovery:
    def __init__(self, bootstrap_host: str, bootstrap_port: int) -> None:
        self.bootstrap_host = bootstrap_host
        self.bootstrap_port = bootstrap_port

    def request(self, message: dict) -> dict:
        with socket.create_connection((self.bootstrap_host, self.bootstrap_port), timeout=SOCKET_TIMEOUT_SECONDS) as sock:
            sock.settimeout(SOCKET_TIMEOUT_SECONDS)
            send_message(sock, message)
            return recv_message(sock)

    def register(self, peer_id: str, host: str, port: int) -> list[dict]:
        response = self.request(build_message("REGISTER", peer_id=peer_id, host=host, port=port))
        self._raise_for_error(response)
        return list(response.get("peers", []))

    def list_peers(self, peer_id: str | None = None) -> list[dict]:
        response = self.request(build_message("PEER_LIST", peer_id=peer_id))
        self._raise_for_error(response)
        return list(response.get("peers", []))

    def heartbeat(self, peer_id: str) -> bool:
        response = self.request(build_message("HEARTBEAT", peer_id=peer_id))
        return response["type"] == "ACK"

    def disconnect(self, peer_id: str) -> None:
        self.request(build_message("DISCONNECT", peer_id=peer_id))

    def create_group(self, peer_id: str, group_name: str, members: list[str] | None = None) -> dict:
        response = self.request(
            build_message("GROUP_CREATE", peer_id=peer_id, group_name=group_name, members=members or [])
        )
        self._raise_for_error(response)
        return dict(response.get("group") or {})

    def list_groups(self, peer_id: str) -> list[dict]:
        response = self.request(build_message("GROUP_LIST", peer_id=peer_id))
        self._raise_for_error(response)
        return list(response.get("groups", []))

    def list_group_views(self, peer_id: str) -> dict[str, list[dict]]:
        response = self.request(build_message("GROUP_LIST", peer_id=peer_id))
        self._raise_for_error(response)
        return {
            "all_groups": list(response.get("all_groups", response.get("groups", []))),
            "my_groups": list(response.get("my_groups", response.get("groups", []))),
        }

    def get_group(self, peer_id: str, group_id: str) -> dict:
        response = self.request(build_message("GROUP_GET", peer_id=peer_id, group_id=group_id))
        self._raise_for_error(response)
        return dict(response.get("group") or {})

    def add_group_member(self, peer_id: str, group_id: str, member: str) -> dict:
        response = self.request(
            build_message("GROUP_ADD_MEMBER", peer_id=peer_id, group_id=group_id, member=member)
        )
        self._raise_for_error(response)
        return dict(response.get("group") or {})

    def join_group(self, peer_id: str, group_id: str) -> dict:
        response = self.request(build_message("GROUP_JOIN", peer_id=peer_id, group_id=group_id))
        self._raise_for_error(response)
        return dict(response.get("group") or {})

    def remove_group_member(self, peer_id: str, group_id: str, member: str) -> dict:
        response = self.request(
            build_message("GROUP_REMOVE_MEMBER", peer_id=peer_id, group_id=group_id, member=member)
        )
        self._raise_for_error(response)
        return dict(response.get("group") or {})

    def leave_group(self, peer_id: str, group_id: str) -> dict:
        response = self.request(build_message("GROUP_LEAVE", peer_id=peer_id, group_id=group_id))
        self._raise_for_error(response)
        return dict(response.get("group") or {})

    def queue_offline_message(
        self,
        from_peer: str,
        to_peer: str,
        message: str,
        offline_type: str = "direct",
        group_id: str | None = None,
        group_name: str | None = None,
    ) -> dict:
        response = self.request(
            build_message(
                "OFFLINE_QUEUE",
                **{
                    "from": from_peer,
                    "to": to_peer,
                    "message": message,
                    "offline_type": offline_type,
                    "group_id": group_id,
                    "group_name": group_name,
                },
            )
        )
        self._raise_for_error(response)
        return dict(response.get("offline_message") or {})

    def pending_offline_messages(self, peer_id: str) -> list[dict]:
        response = self.request(build_message("OFFLINE_PENDING", peer_id=peer_id))
        self._raise_for_error(response)
        return list(response.get("messages", []))

    def mark_offline_delivered(self, peer_id: str, message_ids: list[str]) -> list[dict]:
        response = self.request(
            build_message("OFFLINE_MARK_DELIVERED", peer_id=peer_id, message_ids=message_ids)
        )
        self._raise_for_error(response)
        return list(response.get("delivered", []))

    @staticmethod
    def _raise_for_error(response: dict) -> None:
        if response["type"] == "ERROR":
            raise RuntimeError(response.get("details") or response.get("error"))
