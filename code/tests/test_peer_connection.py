from pathlib import Path
import tempfile
import unittest

from peer.message_handler import MessageHandler
from peer.peer_client import PeerClient
from peer.peer_server import PeerServer
from common.protocol import build_message


class PeerConnectionTests(unittest.TestCase):
    def test_direct_peer_connection_ack(self):
        with tempfile.TemporaryDirectory() as temp_dir:
            tmp_path = Path(temp_dir)
            handler = MessageHandler("bob", storage_root=tmp_path)
            server = PeerServer("127.0.0.1", 0, handler)
            server.start()
            while server.bound_port == 0:
                pass

            message = build_message("CHAT", **{"from": "alice", "to": "bob", "message": "hello"})
            response = PeerClient().send_with_ack("127.0.0.1", server.bound_port, message)

            server.stop()
            self.assertEqual(response["type"], "ACK")
            self.assertEqual(response["ack_for"], message["message_id"])
            history = tmp_path / "chat_history" / "bob.jsonl"
            self.assertTrue(history.exists())


if __name__ == "__main__":
    unittest.main()
