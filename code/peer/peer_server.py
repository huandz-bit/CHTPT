"""Peer-side TCP server for receiving direct messages."""

from __future__ import annotations

import socket
import threading
import time

from common.constants import SOCKET_BACKLOG, SOCKET_TIMEOUT_SECONDS
from common.logger import get_logger
from common.protocol import ProtocolError, build_error, recv_message, send_message
from peer.message_handler import MessageHandler

logger = get_logger("peer.server")


class PeerServer:
    def __init__(self, host: str, port: int, handler: MessageHandler) -> None:
        self.host = host
        self.port = port
        self.handler = handler
        self._shutdown = threading.Event()
        self._started = threading.Event()
        self._thread: threading.Thread | None = None
        self.bound_port = 0
        self.startup_error: OSError | None = None

    def start(self) -> None:
        self._shutdown.clear()
        self._started.clear()
        self.bound_port = 0
        self.startup_error = None
        self._thread = threading.Thread(target=self.serve_forever, daemon=True)
        self._thread.start()
        deadline = time.monotonic() + 2.0
        while not self._started.is_set() and self._thread.is_alive() and time.monotonic() < deadline:
            time.sleep(0.01)
        if self.startup_error is not None:
            raise self.startup_error

    def serve_forever(self) -> None:
        with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as server_socket:
            try:
                server_socket.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
                server_socket.bind((self.host, self.port))
                self.bound_port = int(server_socket.getsockname()[1])
                server_socket.listen(SOCKET_BACKLOG)
                server_socket.settimeout(1.0)
            except OSError as exc:
                self.startup_error = exc
                self._started.set()
                logger.warning("Peer server could not bind %s:%s: %s", self.host, self.port, exc)
                return
            self._started.set()
            logger.info("Peer server listening on %s:%s", self.host, self.bound_port)

            while not self._shutdown.is_set():
                try:
                    client_socket, address = server_socket.accept()
                except socket.timeout:
                    continue
                thread = threading.Thread(
                    target=self._handle_connection,
                    args=(client_socket, address),
                    daemon=True,
                )
                thread.start()

    def stop(self) -> None:
        self._shutdown.set()
        if self.bound_port:
            try:
                with socket.create_connection((self.host, self.bound_port), timeout=0.2):
                    pass
            except OSError:
                pass
        if self._thread:
            self._thread.join(timeout=2.0)

    def _handle_connection(self, client_socket: socket.socket, address: tuple[str, int]) -> None:
        with client_socket:
            client_socket.settimeout(SOCKET_TIMEOUT_SECONDS)
            try:
                message = recv_message(client_socket)
                if message.get("type") == "FILE_META":
                    self.handler.handle_file_connection(client_socket, message)
                    return
                response = self.handler.handle(message)
            except (ProtocolError, OSError) as exc:
                if self._shutdown.is_set():
                    return
                logger.warning("Bad peer request from %s: %s", address, exc)
                response = build_error("BAD_REQUEST", str(exc))
            send_message(client_socket, response)
