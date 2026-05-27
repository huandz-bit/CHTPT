"""General-purpose utilities for IDs, timestamps, files, and encryption."""

from __future__ import annotations

import json
import os
import uuid
from datetime import datetime, timezone
from pathlib import Path
from typing import Any


def utc_now_iso() -> str:
    return datetime.now(timezone.utc).replace(microsecond=0).isoformat()


def new_message_id() -> str:
    return str(uuid.uuid4())


def ensure_dir(path: str | Path) -> Path:
    target = Path(path)
    target.mkdir(parents=True, exist_ok=True)
    return target


def read_json_file(path: str | Path, default: Any) -> Any:
    target = Path(path)
    if not target.exists():
        return default
    try:
        return json.loads(target.read_text(encoding="utf-8"))
    except (json.JSONDecodeError, OSError):
        return default


def write_json_file(path: str | Path, data: Any) -> None:
    target = Path(path)
    ensure_dir(target.parent)
    target.write_text(json.dumps(data, indent=2, sort_keys=True), encoding="utf-8")


def append_jsonl(path: str | Path, data: dict[str, Any]) -> None:
    target = Path(path)
    ensure_dir(target.parent)
    with target.open("a", encoding="utf-8") as file:
        file.write(json.dumps(data, sort_keys=True) + "\n")


class FernetCipher:
    """Small wrapper around cryptography.Fernet.

    Peers use the same key so they can decrypt each other's payloads. The key
    can be provided with P2P_FERNET_KEY or generated once in storage/fernet.key.
    """

    def __init__(self, key: str | None = None) -> None:
        self.enabled = False
        self._fernet = None
        if not key:
            return
        try:
            from cryptography.fernet import Fernet

            self._fernet = Fernet(key.encode("utf-8"))
            self.enabled = True
        except Exception:
            self.enabled = False

    @staticmethod
    def generate_key() -> str:
        from cryptography.fernet import Fernet

        return Fernet.generate_key().decode("utf-8")

    @classmethod
    def from_env(cls) -> "FernetCipher":
        key = os.getenv("P2P_FERNET_KEY")
        if key:
            return cls(key)
        return cls.from_key_file(Path("storage") / "fernet.key")

    @classmethod
    def from_key_file(cls, path: str | Path) -> "FernetCipher":
        target = Path(path)
        if target.exists():
            return cls(target.read_text(encoding="utf-8").strip())
        key = cls.generate_key()
        ensure_dir(target.parent)
        target.write_text(key, encoding="utf-8")
        return cls(key)

    def encrypt(self, plaintext: str) -> str:
        if not self.enabled or self._fernet is None:
            return plaintext
        return self._fernet.encrypt(plaintext.encode("utf-8")).decode("utf-8")

    def decrypt(self, ciphertext: str) -> str:
        if not self.enabled or self._fernet is None:
            return ciphertext
        return self._fernet.decrypt(ciphertext.encode("utf-8")).decode("utf-8")
