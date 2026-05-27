"""Continuously simulate peer joins, leaves, and message traffic."""

from __future__ import annotations

import argparse
import random
import threading
import time

from common.logger import get_logger
from peer.peer import PeerNode
from server.bootstrap_server import BootstrapServer

logger = get_logger("churn")


def run_churn(peer_count: int, duration: int, bootstrap_port: int) -> None:
    bootstrap = BootstrapServer(port=bootstrap_port)
    bootstrap_thread = threading.Thread(target=bootstrap.serve_forever, daemon=True)
    bootstrap_thread.start()
    time.sleep(0.5)

    peers = [
        PeerNode(f"peer{i}", port=0, bootstrap_port=bootstrap_port)
        for i in range(peer_count)
    ]
    online: dict[str, PeerNode] = {}
    deadline = time.time() + duration

    try:
        while time.time() < deadline:
            node = random.choice(peers)
            if node.peer_id in online and random.random() < 0.45:
                logger.info("leave: %s", node.peer_id)
                node.stop()
                online.pop(node.peer_id, None)
            elif node.peer_id not in online:
                logger.info("join: %s", node.peer_id)
                node.start()
                online[node.peer_id] = node

            active = list(online.values())
            if len(active) >= 2:
                sender = random.choice(active)
                target = random.choice([item for item in active if item.peer_id != sender.peer_id])
                if random.random() < 0.25:
                    logger.info("broadcast from %s", sender.peer_id)
                    sender.broadcast(f"churn broadcast from {sender.peer_id}")
                else:
                    logger.info("chat %s -> %s", sender.peer_id, target.peer_id)
                    sender.send_chat(target.peer_id, f"hello during churn from {sender.peer_id}")

            time.sleep(random.uniform(0.5, 1.5))
    finally:
        for node in list(online.values()):
            node.stop()
        bootstrap.shutdown()


def main() -> None:
    parser = argparse.ArgumentParser(description="Run churn simulation")
    parser.add_argument("--peers", type=int, default=5)
    parser.add_argument("--duration", type=int, default=30)
    parser.add_argument("--bootstrap-port", type=int, default=9010)
    args = parser.parse_args()
    run_churn(args.peers, args.duration, args.bootstrap_port)


if __name__ == "__main__":
    main()
