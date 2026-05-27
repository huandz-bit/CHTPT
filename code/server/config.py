"""Bootstrap server configuration."""

from __future__ import annotations

from pathlib import Path

from common.constants import DEFAULT_BOOTSTRAP_HOST, DEFAULT_BOOTSTRAP_PORT

BOOTSTRAP_HOST = DEFAULT_BOOTSTRAP_HOST
BOOTSTRAP_PORT = DEFAULT_BOOTSTRAP_PORT
REGISTRY_FILE = Path("storage/peers.json")

