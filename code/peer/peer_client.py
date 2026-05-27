"""Outbound client operations for direct peer-to-peer messages."""

from __future__ import annotations

import socket

from common.constants import ACK_TIMEOUT_SECONDS, SOCKET_TIMEOUT_SECONDS
from common.logger import get_logger
from common.protocol import ProtocolError, recv_message, send_message

logger = get_logger("peer.client")


class DeliveryError(Exception):
    """Raised when a peer message cannot be delivered or acknowledged."""


class AckTimeoutError(DeliveryError):
    """Raised after a message is sent but no ACK arrives before the deadline."""


class PeerClient:
    def __init__(self, timeout: float = SOCKET_TIMEOUT_SECONDS) -> None:
        self.timeout = timeout

    def send_with_ack(self, host: str, port: int, message: dict) -> dict:
        """Send a message and require an ACK for its message_id."""
        try:
            with socket.create_connection((host, int(port)), timeout=self.timeout) as sock:
                sock.settimeout(ACK_TIMEOUT_SECONDS)
                send_message(sock, message)
                try:
                    response = recv_message(sock)
                except socket.timeout as exc:
                    raise AckTimeoutError(f"Message sent to {host}:{port}, but ACK timed out") from exc
        except AckTimeoutError:
            raise
        except (OSError, socket.timeout, ProtocolError) as exc:
            raise DeliveryError(f"Could not deliver to {host}:{port}: {exc}") from exc

        try:
            ack_for = message["message_id"]
        except KeyError as exc:
            raise DeliveryError("Message is missing message_id") from exc

        if response.get("type") != "ACK" or response.get("ack_for") != ack_for:
            raise DeliveryError(f"Invalid ACK response from {host}:{port}: {response}")
        logger.info("Delivered %s to %s:%s", ack_for, host, port)
        return response
