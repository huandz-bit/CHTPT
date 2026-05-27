"""Inbound peer message handling and persistence."""

from __future__ import annotations

from pathlib import Path
from typing import Callable

from common.logger import get_logger
from common.protocol import build_ack, build_error
from common.utils import FernetCipher, append_jsonl

logger = get_logger("peer.handler")


class MessageHandler:
    def __init__(
        self,
        peer_id: str,
        storage_root: str | Path = "storage",
        cipher: FernetCipher | None = None,
        on_chat: Callable[[dict], None] | None = None,
        on_broadcast: Callable[[dict], None] | None = None,
    ) -> None:
        self.peer_id = peer_id
        self.storage_root = Path(storage_root)
        self.cipher = cipher or FernetCipher.from_env()
        self.on_chat = on_chat
        self.on_broadcast = on_broadcast
        self.seen_message_ids: set[str] = set()

    def handle(self, message: dict) -> dict:
        message_type = message["type"]
        if message_type in {"CHAT", "GROUP_CHAT", "BROADCAST", "SYSTEM"}:
            return self._handle_chat(message)
        if message_type == "FILE_META":
            return build_error("FILE_TRANSFER_REQUIRES_STREAM", "FILE_META must be handled by the peer TCP server.", message["message_id"])
        if message_type == "FILE_CHUNK":
            return self._handle_file_chunk(message)
        if message_type == "HEARTBEAT":
            return build_ack(message["message_id"], sender=self.peer_id)
        if message_type == "DISCONNECT":
            return build_ack(message["message_id"], sender=self.peer_id)
        return build_error("UNSUPPORTED_MESSAGE", f"Peer cannot handle {message_type}", message["message_id"])

    def _handle_chat(self, message: dict) -> dict:
        if message["message_id"] in self.seen_message_ids:
            return build_ack(message["message_id"], sender=self.peer_id)
        self.seen_message_ids.add(message["message_id"])

        stored = dict(message)
        if stored.get("encrypted"):
            try:
                stored["message"] = self.cipher.decrypt(str(stored["message"]))
            except Exception as exc:
                logger.warning("Could not decrypt message %s: %s", stored["message_id"], exc)
                return build_error("DECRYPT_FAILED", str(exc), stored["message_id"])

        append_jsonl(self.storage_root / "chat_history" / f"{self.peer_id}.jsonl", stored)
        sender = stored.get("from", "unknown")
        text = stored.get("message", "")
        prefix = {
            "GROUP_CHAT": "group",
            "BROADCAST": "broadcast",
            "SYSTEM": "system",
        }.get(stored["type"], "direct")
        print(f"\n[{prefix} from {sender}] {text}")
        if self.on_chat:
            self.on_chat(stored)
        if stored["type"] == "BROADCAST" and self.on_broadcast:
            self.on_broadcast(message)
        return build_ack(stored["message_id"], sender=self.peer_id)

    def _handle_file_chunk(self, message: dict) -> dict:
        from peer.file_transfer import receive_file_chunk

        receive_file_chunk(message, self.storage_root)
        return build_ack(message["message_id"], sender=self.peer_id)

    def handle_file_connection(self, sock, file_meta: dict) -> None:
        from peer.file_transfer import receive_file_connection

        receive_file_connection(sock, file_meta, self.storage_root, self.peer_id, self.on_chat)
