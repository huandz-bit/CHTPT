from pathlib import Path
import tempfile
import unittest

from common.protocol import build_message
from common.utils import FernetCipher, read_json_file
from peer.file_transfer import receive_file_chunk
from peer.message_handler import MessageHandler
from peer.peer import PeerNode


class AdvancedFeatureTests(unittest.TestCase):
    def test_encrypted_chat_is_decrypted_before_storage(self):
        with tempfile.TemporaryDirectory() as temp_dir:
            key = FernetCipher.generate_key()
            cipher = FernetCipher(key)
            handler = MessageHandler("bob", storage_root=temp_dir, cipher=cipher)
            encrypted = cipher.encrypt("secret")
            message = build_message(
                "CHAT",
                **{"from": "alice", "to": "bob", "message": encrypted, "encrypted": True},
            )

            response = handler.handle(message)

            self.assertEqual(response["type"], "ACK")
            history = Path(temp_dir) / "chat_history" / "bob.jsonl"
            self.assertIn("secret", history.read_text(encoding="utf-8"))
            self.assertNotIn(encrypted, history.read_text(encoding="utf-8"))

    def test_duplicate_broadcast_is_not_forwarded_twice(self):
        forwarded = []
        with tempfile.TemporaryDirectory() as temp_dir:
            handler = MessageHandler("bob", storage_root=temp_dir, on_broadcast=forwarded.append)
            message = build_message(
                "BROADCAST",
                **{"from": "alice", "origin": "alice", "message": "hello", "encrypted": False},
            )

            handler.handle(message)
            handler.handle(message)

            self.assertEqual(len(forwarded), 1)

    def test_offline_queue_is_persisted(self):
        with tempfile.TemporaryDirectory() as temp_dir:
            node = PeerNode("alice", storage_root=temp_dir)
            ok = node.send_chat("missing", "queued message")

            self.assertFalse(ok)
            queue = read_json_file(Path(temp_dir) / "offline_messages" / "alice.json", [])
            self.assertEqual(queue[0]["to"], "missing")
            self.assertEqual(queue[0]["message"], "queued message")

    def test_file_chunks_are_reassembled(self):
        with tempfile.TemporaryDirectory() as temp_dir:
            first = build_message(
                "FILE_CHUNK",
                **{
                    "from": "alice",
                    "to": "bob",
                    "file_id": "file-1",
                    "filename": "sample.txt",
                    "chunk_index": 0,
                    "total_chunks": 2,
                    "total_size": 11,
                    "data": "aGVsbG8g",
                },
            )
            second = dict(first)
            second["message_id"] = "second"
            second["chunk_index"] = 1
            second["data"] = "d29ybGQ="

            receive_file_chunk(first, temp_dir)
            output = receive_file_chunk(second, temp_dir)

            self.assertEqual(output.read_text(encoding="utf-8"), "hello world")

    def test_streaming_file_transfer_saves_received_file_and_keeps_duplicates(self):
        with tempfile.TemporaryDirectory() as temp_dir:
            root = Path(temp_dir)
            received_events = []
            bob = PeerNode("bob", port=0, storage_root=root / "bob")
            bob.handler.on_chat = received_events.append
            bob.server.start()
            try:
                alice = PeerNode("alice", storage_root=root / "alice")
                alice.known_peers["bob"] = {
                    "peer_id": "bob",
                    "host": bob.host,
                    "port": bob.server.bound_port,
                    "status": "online",
                }
                source = root / "sample.txt"
                source.write_text("hello file", encoding="utf-8")

                first = alice.send_file_direct("bob", source)
                second = alice.send_file_direct("bob", source)

                received_root = root / "bob" / "received_files" / "bob"
                self.assertEqual(first["sent_to"], ["bob"])
                self.assertEqual(second["sent_to"], ["bob"])
                self.assertEqual((received_root / "sample.txt").read_text(encoding="utf-8"), "hello file")
                self.assertEqual((received_root / "sample_1.txt").read_text(encoding="utf-8"), "hello file")
                self.assertEqual([event["filename"] for event in received_events], ["sample.txt", "sample.txt"])
            finally:
                bob.server.stop()


if __name__ == "__main__":
    unittest.main()
