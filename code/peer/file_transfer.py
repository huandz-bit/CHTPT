"""Streaming peer-to-peer file transfer helpers."""

from __future__ import annotations

import base64
import hashlib
import mimetypes
import socket
import uuid
from pathlib import Path
from typing import BinaryIO, Callable, Any

from common.constants import FILE_CHUNK_BYTES, MAX_FILE_SIZE_MB
from common.logger import get_logger
from common.protocol import ProtocolError, build_message, recv_message, send_message
from common.utils import ensure_dir, utc_now_iso

logger = get_logger("peer.file_transfer")

MAX_FILE_BYTES = MAX_FILE_SIZE_MB * 1024 * 1024


class FileTransferError(Exception):
    """Raised when a file transfer cannot complete."""


def calculate_sha256(file_path: str | Path) -> str:
    digest = hashlib.sha256()
    with Path(file_path).open("rb") as file:
        for chunk in iter(lambda: file.read(FILE_CHUNK_BYTES), b""):
            digest.update(chunk)
    return digest.hexdigest()


def unique_filename(directory: str | Path, filename: str) -> Path:
    target_dir = ensure_dir(Path(directory))
    safe_name = Path(filename).name or "received_file"
    candidate = target_dir / safe_name
    if not candidate.exists():
        return candidate
    stem = candidate.stem
    suffix = candidate.suffix
    index = 1
    while True:
        candidate = target_dir / f"{stem}_{index}{suffix}"
        if not candidate.exists():
            return candidate
        index += 1


def send_file_to_peer(
    peer_node: Any,
    target_peer: dict,
    file_path: str | Path,
    mode: str = "direct",
    group_info: dict | None = None,
) -> dict:
    source = Path(file_path)
    if not source.exists() or not source.is_file():
        raise FileTransferError("Please choose a file.")
    filesize = source.stat().st_size
    if filesize <= 0:
        raise FileTransferError("File is empty.")
    if filesize > MAX_FILE_BYTES:
        raise FileTransferError(f"File is too large. Max size is {MAX_FILE_SIZE_MB} MB.")

    target_peer_id = str(target_peer.get("peer_id") or "")
    transfer_id = str(uuid.uuid4())
    checksum = hashlib.sha256()
    mime_type = mimetypes.guess_type(source.name)[0] or "application/octet-stream"
    meta = build_message(
        "FILE_META",
        **{
            "transfer_id": transfer_id,
            "from": peer_node.peer_id,
            "to": target_peer_id,
            "mode": mode,
            "group_id": group_info.get("group_id") if group_info else None,
            "group_name": group_info.get("group_name") if group_info else None,
            "filename": source.name,
            "filesize": filesize,
            "mime_type": mime_type,
            "chunk_size": FILE_CHUNK_BYTES,
            "timestamp": utc_now_iso(),
        },
    )

    try:
        with socket.create_connection((str(target_peer["host"]), int(target_peer["port"])), timeout=5.0) as sock:
            sock.settimeout(15.0)
            send_message(sock, meta)
            ready = recv_message(sock)
            if ready.get("type") != "FILE_ACK" or ready.get("transfer_id") != transfer_id or ready.get("status") != "ready":
                raise FileTransferError(str(ready.get("error") or f"Invalid file ACK from {target_peer_id}"))

            with source.open("rb") as file:
                for chunk in iter(lambda: file.read(FILE_CHUNK_BYTES), b""):
                    checksum.update(chunk)
                    sock.sendall(chunk)

            send_message(sock, build_message("FILE_END", transfer_id=transfer_id, checksum=checksum.hexdigest()))
            received = recv_message(sock)
    except (OSError, socket.timeout, ProtocolError) as exc:
        raise FileTransferError(f"{target_peer_id} is offline. File transfer requires online peer.") from exc

    if received.get("type") != "FILE_ACK" or received.get("transfer_id") != transfer_id or received.get("status") != "received":
        raise FileTransferError(str(received.get("error") or f"File transfer to {target_peer_id} failed."))
    return {
        "transfer_id": transfer_id,
        "filename": source.name,
        "filesize": filesize,
        "checksum": checksum.hexdigest(),
        "target": target_peer_id,
    }


def receive_file_connection(
    sock: socket.socket,
    file_meta: dict,
    storage_root: str | Path,
    username: str,
    on_received: Callable[[dict], None] | None = None,
) -> None:
    transfer_id = str(file_meta.get("transfer_id") or "")
    filename = Path(str(file_meta.get("filename") or "received_file")).name
    filesize = int(file_meta.get("filesize") or 0)
    if not transfer_id or filesize <= 0 or filesize > MAX_FILE_BYTES:
        send_message(sock, build_message("FILE_ERROR", transfer_id=transfer_id, error="Invalid file metadata."))
        return

    destination = unique_filename(Path(storage_root) / "received_files" / username, filename)
    digest = hashlib.sha256()
    remaining = filesize
    send_message(sock, build_message("FILE_ACK", transfer_id=transfer_id, status="ready"))

    try:
        with destination.open("wb") as output:
            while remaining > 0:
                chunk = sock.recv(min(FILE_CHUNK_BYTES, remaining))
                if not chunk:
                    raise FileTransferError("Connection closed before the full file arrived.")
                output.write(chunk)
                digest.update(chunk)
                remaining -= len(chunk)
        end = recv_message(sock)
        if end.get("type") != "FILE_END" or end.get("transfer_id") != transfer_id:
            raise FileTransferError("Missing FILE_END.")
        checksum = digest.hexdigest()
        if str(end.get("checksum") or "") != checksum:
            destination.unlink(missing_ok=True)
            raise FileTransferError("Checksum verification failed.")
    except Exception as exc:
        destination.unlink(missing_ok=True)
        send_message(sock, build_message("FILE_ERROR", transfer_id=transfer_id, error=str(exc)))
        return

    send_message(sock, build_message("FILE_ACK", transfer_id=transfer_id, status="received"))
    event = {
        "type": "FILE_RECEIVED",
        "from": file_meta.get("from"),
        "to": username,
        "mode": file_meta.get("mode"),
        "group_id": file_meta.get("group_id"),
        "group_name": file_meta.get("group_name"),
        "filename": filename,
        "saved_filename": destination.name,
        "file_path": destination.as_posix(),
        "saved_path": destination.as_posix(),
        "size": filesize,
        "transfer_id": transfer_id,
    }
    if on_received:
        on_received(event)
    logger.info("Received file %s from %s into %s", filename, file_meta.get("from"), destination)


def save_received_file(username: str, filename: str, stream: BinaryIO, storage_root: str | Path = "storage") -> Path:
    destination = unique_filename(Path(storage_root) / "received_files" / username, filename)
    with destination.open("wb") as output:
        for chunk in iter(lambda: stream.read(FILE_CHUNK_BYTES), b""):
            output.write(chunk)
    return destination


def send_file_chunks(peer_node, target_peer_id: str, file_path: str | Path) -> bool:
    target = peer_node.get_peer_or_refresh(target_peer_id)
    if target is None:
        return False
    send_file_to_peer(peer_node, target, file_path, mode="direct")
    return True


def receive_file_chunk(message: dict, storage_root: str | Path = "storage") -> Path:
    storage = Path(storage_root)
    incoming_root = ensure_dir(storage / "incoming_files" / str(message["file_id"]))
    shared_root = ensure_dir(storage.parent / "shared_files")

    chunk_index = int(message["chunk_index"])
    total_chunks = int(message["total_chunks"])
    chunk_path = incoming_root / f"{chunk_index:06d}.part"
    chunk_path.write_bytes(base64.b64decode(str(message["data"]).encode("ascii")))

    if len(list(incoming_root.glob("*.part"))) < total_chunks:
        return chunk_path

    destination = unique_filename(shared_root, str(message["filename"]))
    with destination.open("wb") as output:
        for index in range(total_chunks):
            output.write((incoming_root / f"{index:06d}.part").read_bytes())
    return destination
