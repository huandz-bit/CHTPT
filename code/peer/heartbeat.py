"""Background heartbeat loop for keeping bootstrap registry fresh."""

from __future__ import annotations

import threading
import time

from common.constants import HEARTBEAT_INTERVAL_SECONDS
from common.logger import get_logger

logger = get_logger("peer.heartbeat")


class HeartbeatLoop:
    def __init__(self, peer_id: str, discovery, interval: float = HEARTBEAT_INTERVAL_SECONDS) -> None:
        self.peer_id = peer_id
        self.discovery = discovery
        self.interval = interval
        self._stop = threading.Event()
        self._thread: threading.Thread | None = None

    def start(self) -> None:
        self._stop.clear()
        self._thread = threading.Thread(target=self._run, daemon=True)
        self._thread.start()

    def stop(self) -> None:
        self._stop.set()
        if self._thread:
            self._thread.join(timeout=2.0)

    def _run(self) -> None:
        while not self._stop.wait(self.interval):
            try:
                self.discovery.heartbeat(self.peer_id)
            except Exception as exc:
                logger.warning("Heartbeat failed: %s", exc)
