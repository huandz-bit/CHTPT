"""Convenience launcher for bootstrap server and peer nodes."""

from __future__ import annotations

import argparse

from peer.peer import main as peer_main
from server.bootstrap_server import main as server_main
from peer.web_app import main as web_main


def main() -> None:
    parser = argparse.ArgumentParser(description="P2P Chat launcher")
    parser.add_argument("mode", choices=["server", "peer", "web"])
    args, remaining = parser.parse_known_args()

    import sys

    sys.argv = [sys.argv[0], *remaining]
    if args.mode == "server":
        server_main()
    elif args.mode == "peer":
        peer_main()
    else:
        web_main()


if __name__ == "__main__":
    main()
