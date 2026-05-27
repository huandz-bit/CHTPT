"""JSON message protocol and newline-delimited socket framing."""

from __future__ import annotations

import json
import socket
from typing import Any

from common.constants import ENCODING, MAX_MESSAGE_BYTES, MESSAGE_TYPES
from common.utils import new_message_id, utc_now_iso


class ProtocolError(Exception):
    """Raised when a socket frame or protocol message is invalid."""


def build_message(message_type: str, **fields: Any) -> dict[str, Any]:
    if message_type not in MESSAGE_TYPES:
        raise ProtocolError(f"Unsupported message type: {message_type}")
    message = {
        "type": message_type,
        "message_id": fields.pop("message_id", new_message_id()),
        "timestamp": fields.pop("timestamp", utc_now_iso()),
    }
    message.update(fields)
    return message


def build_error(error: str, details: str | None = None, correlation_id: str | None = None) -> dict[str, Any]:
    return build_message("ERROR", error=error, details=details, correlation_id=correlation_id)


def build_ack(message_id: str, sender: str | None = None) -> dict[str, Any]:
    return build_message("ACK", ack_for=message_id, from_peer=sender)


def validate_message(message: dict[str, Any]) -> None:
    message_type = message.get("type")
    if message_type not in MESSAGE_TYPES:
        raise ProtocolError(f"Invalid message type: {message_type}")
    if not message.get("message_id"):
        raise ProtocolError("Missing message_id")
    if not message.get("timestamp"):
        raise ProtocolError("Missing timestamp")


def encode_message(message: dict[str, Any]) -> bytes:
    validate_message(message)
    payload = json.dumps(message, separators=(",", ":"), sort_keys=True).encode(ENCODING)
    if len(payload) > MAX_MESSAGE_BYTES:
        raise ProtocolError("Message exceeds maximum frame size")
    return payload + b"\n"


def send_message(sock: socket.socket, message: dict[str, Any]) -> None:
    sock.sendall(encode_message(message))


def recv_message(sock: socket.socket) -> dict[str, Any]:
    """Receive one newline-delimited JSON message from a socket."""
    chunks: list[bytes] = []
    total = 0
    while True:
        chunk = sock.recv(4096)
        if not chunk:
            raise ProtocolError("Connection closed before a complete message arrived")
        total += len(chunk)
        if total > MAX_MESSAGE_BYTES:
            raise ProtocolError("Incoming message exceeds maximum frame size")
        if b"\n" in chunk:
            before_newline, _sep, _after = chunk.partition(b"\n")
            chunks.append(before_newline)
            break
        chunks.append(chunk)

    try:
        message = json.loads(b"".join(chunks).decode(ENCODING))
    except json.JSONDecodeError as exc:
        raise ProtocolError(f"Invalid JSON frame: {exc}") from exc
    if not isinstance(message, dict):
        raise ProtocolError("Protocol frame must be a JSON object")
    validate_message(message)
    return message

