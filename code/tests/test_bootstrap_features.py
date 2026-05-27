import tempfile
import socket
import threading
import time
import unittest
from pathlib import Path
from unittest import mock

import server.bootstrap_server as bootstrap_module
from peer.peer_discovery import PeerDiscovery
from server.bootstrap_server import BootstrapServer
from server.peer_registry import PeerRegistry


class BootstrapFeatureTests(unittest.TestCase):
    def test_peer_disconnect_keeps_status_and_reconnects_same_port(self):
        with tempfile.TemporaryDirectory() as temp_dir:
            registry = PeerRegistry(Path(temp_dir) / "peers.json")
            registry.register("bob", "127.0.0.1", 5002)

            self.assertTrue(registry.mark_offline("bob"))
            offline = registry.get_peer("bob")
            self.assertEqual(offline["status"], "offline")

            registry.register("bob", "127.0.0.1", 5002)
            online = registry.get_peer("bob")
            self.assertEqual(online["status"], "online")
            self.assertEqual(online["port"], 5002)

    def test_port_remains_reserved_after_disconnect(self):
        with tempfile.TemporaryDirectory() as temp_dir:
            registry = PeerRegistry(Path(temp_dir) / "peers.json")
            registry.register("alice", "127.0.0.1", 5001)
            registry.mark_offline("alice")

            with self.assertRaisesRegex(ValueError, "Port 5001 is reserved by alice"):
                registry.register("bob", "127.0.0.1", 5001)

            registry.register("alice", "127.0.0.1", 5001)
            self.assertEqual(registry.get_peer("alice")["status"], "online")

    def test_same_username_must_reuse_reserved_port(self):
        with tempfile.TemporaryDirectory() as temp_dir:
            registry = PeerRegistry(Path(temp_dir) / "peers.json")
            registry.register("alice", "127.0.0.1", 5001)
            registry.mark_offline("alice")

            with self.assertRaisesRegex(ValueError, "must reuse reserved port 5001"):
                registry.register("alice", "127.0.0.1", 6001)

    def test_online_username_can_only_reregister_from_same_address(self):
        with tempfile.TemporaryDirectory() as temp_dir:
            registry = PeerRegistry(Path(temp_dir) / "peers.json")
            registry.register("alice", "127.0.0.1", 5001)

            registry.register("alice", "127.0.0.1", 5001)
            self.assertEqual(registry.get_peer("alice")["status"], "online")

            with self.assertRaises(ValueError):
                registry.register("alice", "127.0.0.1", 6001)

    def test_group_member_leave_hides_group_from_that_member(self):
        with tempfile.TemporaryDirectory() as temp_dir:
            registry = PeerRegistry(Path(temp_dir) / "peers.json")
            registry.register("alice", "127.0.0.1", 5001)
            registry.register("bob", "127.0.0.1", 5002)
            group = registry.create_group("alice", "team", ["bob"])

            self.assertEqual(len(registry.list_groups("bob")), 1)

            registry.leave_group(group["group_id"], "bob")

            self.assertEqual(registry.list_groups("bob"), [])
            self.assertEqual(registry.get_group(group["group_id"])["members"], ["alice"])

    def test_offline_messages_are_queued_and_marked_delivered(self):
        with tempfile.TemporaryDirectory() as temp_dir:
            registry = PeerRegistry(Path(temp_dir) / "peers.json")
            registry.register("alice", "127.0.0.1", 5001)
            registry.register("bob", "127.0.0.1", 5002)
            registry.mark_offline("bob")
            queued = registry.queue_message("direct", "alice", "bob", "are you there?")

            pending = registry.pending_messages("bob")
            self.assertEqual([item["message"] for item in pending], ["are you there?"])

            delivered = registry.mark_messages_delivered("bob", [queued["message_id"]])
            self.assertEqual(delivered[0]["delivery_status"], "delivered")
            self.assertEqual(registry.pending_messages("bob"), [])

    def test_bootstrap_protocol_supports_groups_and_offline_queue(self):
        with tempfile.TemporaryDirectory() as temp_dir:
            registry_file = Path(temp_dir) / "peers.json"
            port = _free_port()
            with mock.patch.object(bootstrap_module, "REGISTRY_FILE", registry_file):
                bootstrap = BootstrapServer("127.0.0.1", port)
            thread = threading.Thread(target=bootstrap.serve_forever, daemon=True)
            thread.start()
            time.sleep(0.1)

            discovery = PeerDiscovery("127.0.0.1", port)
            try:
                discovery.register("alice", "127.0.0.1", 5001)
                discovery.register("bob", "127.0.0.1", 5002)
                group = discovery.create_group("alice", "team", ["bob"])
                self.assertIn("bob", group["members"])

                queued = discovery.queue_offline_message("alice", "bob", "hello later")
                self.assertEqual(queued["delivery_status"], "queued")

                pending = discovery.pending_offline_messages("bob")
                self.assertEqual([item["message"] for item in pending], ["hello later"])
            finally:
                bootstrap.shutdown()
                thread.join(timeout=2.0)


def _free_port() -> int:
    with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as sock:
        sock.bind(("127.0.0.1", 0))
        return int(sock.getsockname()[1])


if __name__ == "__main__":
    unittest.main()
