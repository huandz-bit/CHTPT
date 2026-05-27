"""Group chat fan-out helper."""

from __future__ import annotations

import threading


def send_group(peer_node, peer_ids: list[str], text: str) -> dict[str, bool]:
    """Send a group message concurrently to selected peers."""
    results: dict[str, bool] = {}
    lock = threading.Lock()

    def worker(target: str) -> None:
        ok = peer_node.send_chat(target, text, group=True, recipients=peer_ids)
        with lock:
            results[target] = ok

    threads = [threading.Thread(target=worker, args=(peer_id,), daemon=True) for peer_id in peer_ids]
    for thread in threads:
        thread.start()
    for thread in threads:
        thread.join()
    return results

