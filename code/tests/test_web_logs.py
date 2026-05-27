import io
import socket
import tempfile
import threading
import time
import unittest
from pathlib import Path
from types import SimpleNamespace
from unittest import mock

import server.bootstrap_server as bootstrap_module
from peer.peer import PeerNode
from peer.web_app import WebPeerState, create_app
from server.bootstrap_server import BootstrapServer


class WebLogTests(unittest.TestCase):
    def test_web_port_suggests_peer_tcp_port(self):
        client = create_app(web_port=8003).test_client()
        defaults = client.get("/api/defaults").get_json()

        self.assertEqual(defaults["peer_port"], 5003)
        self.assertEqual(defaults["bootstrap_port"], 9000)

    def test_runtime_logs_are_memory_only_and_clearable(self):
        state = WebPeerState()
        state.node = SimpleNamespace(peer_id="alice")

        state.add_chat_log(
            {
                "type": "CHAT",
                "timestamp": "2026-05-26T09:25:10+00:00",
                "from": "alice",
                "to": "bob",
                "message": "xin chao",
            }
        )

        self.assertEqual([log["message"] for log in state.chat_logs], ["Me \u2192 bob: xin chao"])
        self.assertEqual([log["timestamp"] for log in state.chat_logs], ["09:25"])
        self.assertEqual(set(state.chat_logs[0]), {"timestamp", "message"})

        for message_type in ["REGISTER", "DISCONNECT", "HEARTBEAT", "DEBUG"]:
            state.add_chat_log({"type": message_type, "message": message_type})
        self.assertEqual([log["message"] for log in state.chat_logs], ["Me \u2192 bob: xin chao"])

        state.add_chat_log({"type": "SYSTEM", "timestamp": "2026-05-26T09:26:10+00:00", "message": "bob left group team"})
        self.assertEqual(state.chat_logs[-1]["message"], "bob left group team")

        state.clear_runtime_state()

        self.assertEqual(state.chat_logs, [])

    def test_api_logs_keep_refresh_and_register_out_of_chat_logs(self):
        with tempfile.TemporaryDirectory() as tmpdir:
            root = Path(tmpdir)
            original_registry_file = bootstrap_module.REGISTRY_FILE
            bootstrap_module.REGISTRY_FILE = root / "peers.json"
            bootstrap = BootstrapServer("127.0.0.1", _free_port())
            thread = threading.Thread(target=bootstrap.serve_forever, daemon=True)
            thread.start()
            time.sleep(0.1)

            class TempPeerNode(PeerNode):
                def __init__(self, *args, **kwargs):
                    kwargs["storage_root"] = root / "storage"
                    super().__init__(*args, **kwargs)

            with mock.patch("peer.web_app.PeerNode", TempPeerNode):
                alice_app = create_app()
                bob_app = create_app()
                alice = alice_app.test_client()
                bob = bob_app.test_client()
                alice_port = _free_port()
                bob_port = _free_port()

                try:
                    self.assertTrue(
                        alice.post(
                            "/api/register",
                            json={
                                "username": "alice",
                                "peer_host": "127.0.0.1",
                                "peer_port": alice_port,
                                "bootstrap_host": "127.0.0.1",
                                "bootstrap_port": bootstrap.port,
                            },
                        ).get_json()["ok"]
                    )
                    self.assertTrue(
                        bob.post(
                            "/api/register",
                            json={
                                "username": "bob",
                                "peer_host": "127.0.0.1",
                                "peer_port": bob_port,
                                "bootstrap_host": "127.0.0.1",
                                "bootstrap_port": bootstrap.port,
                            },
                        ).get_json()["ok"]
                    )

                    history = root / "storage" / "chat_history" / "alice.jsonl"
                    history.parent.mkdir(parents=True)
                    history.write_text(
                        '{"type":"CHAT","timestamp":"2026-05-26T08:00:00+00:00","from":"alice","to":"bob","message":"old"}\n',
                        encoding="utf-8",
                    )

                    before = alice.get("/api/logs").get_json()
                    self.assertEqual(before["messages"], [])
                    self.assertEqual(set(before), {"ok", "messages"})
                    self.assertNotIn("system_logs", before)
                    self.assertNotIn("status", before)

                    for _ in range(3):
                        self.assertTrue(alice.get("/api/peers").get_json()["ok"])
                    after_refresh = alice.get("/api/logs").get_json()
                    self.assertEqual(after_refresh["messages"], [])
                    self.assertEqual(set(after_refresh), {"ok", "messages"})
                    self.assertNotIn("system_logs", after_refresh)
                    self.assertNotIn("status", after_refresh)

                    missing_target = alice.post("/api/chat", json={"message": "xin chao"})
                    self.assertEqual(missing_target.status_code, 400)
                    self.assertEqual(missing_target.get_json()["error"], "Please select a target peer.")

                    self_target = alice.post("/api/chat", json={"target": "alice", "message": "xin chao"})
                    self.assertEqual(self_target.status_code, 400)
                    self.assertEqual(self_target.get_json()["error"], "Cannot send a direct message to yourself.")

                    send = alice.post("/api/chat", json={"target": "bob", "message": "xin chao"}).get_json()
                    self.assertTrue(send["ok"])
                    self.assertEqual(send["status"], "sent")
                    self.assertEqual(send["target"], "bob")
                    alice_logs = alice.get("/api/logs").get_json()["messages"]
                    bob_logs = bob.get("/api/logs").get_json()["messages"]

                    self.assertEqual([log["message"] for log in alice_logs], ["Me \u2192 bob: xin chao"])
                    self.assertEqual([log["message"] for log in bob_logs], ["alice \u2192 Me: xin chao"])
                    self.assertTrue(all(len(log["timestamp"]) == 5 for log in [*alice_logs, *bob_logs]))
                    self.assertTrue(all(set(log) == {"timestamp", "message"} for log in [*alice_logs, *bob_logs]))

                    first_file = alice.post(
                        "/api/files/direct",
                        data={"target": "bob", "file": (io.BytesIO(b"report contents"), "report.pdf")},
                        content_type="multipart/form-data",
                    ).get_json()
                    second_file = alice.post(
                        "/api/files/direct",
                        data={"target": "bob", "file": (io.BytesIO(b"second report"), "report.pdf")},
                        content_type="multipart/form-data",
                    ).get_json()
                    self.assertTrue(first_file["ok"], first_file)
                    self.assertTrue(second_file["ok"], second_file)

                    bob_file_logs = bob.get("/api/logs").get_json()["messages"]
                    received_file_logs = [log for log in bob_file_logs if log.get("type") == "file"]
                    self.assertEqual([log["message"] for log in received_file_logs], ["alice sent file: report.pdf", "alice sent file: report.pdf"])
                    self.assertEqual([log["saved_filename"] for log in received_file_logs], ["report.pdf", "report_1.pdf"])
                    self.assertEqual(received_file_logs[0]["download_url"], "/api/files/download/report.pdf")
                    self.assertEqual(received_file_logs[1]["open_url"], "/api/files/open/report_1.pdf")

                    received_files = bob.get("/api/files/received").get_json()["files"]
                    self.assertEqual([item["saved_filename"] for item in received_files], ["report.pdf", "report_1.pdf"])
                    self.assertEqual(received_files[0]["from"], "alice")
                    self.assertEqual(received_files[0]["size"], len(b"report contents"))
                    download_response = bob.get("/api/files/download/report.pdf")
                    open_response = bob.get("/api/files/open/report_1.pdf")
                    try:
                        self.assertEqual(download_response.data, b"report contents")
                        self.assertEqual(open_response.data, b"second report")
                    finally:
                        download_response.close()
                        open_response.close()
                    self.assertIn(bob.get("/api/files/download/../../README.md").status_code, {403, 404})

                    created = alice.post("/api/groups/create", json={"group_name": "number"}).get_json()
                    self.assertTrue(created["ok"])
                    group_id = created["group"]["group_id"]
                    self.assertTrue(created["group"]["is_member"])
                    self.assertTrue(created["group"]["is_owner"])

                    bob_groups = bob.get("/api/groups").get_json()
                    self.assertEqual(set(bob_groups), {"ok", "all_groups", "my_groups"})
                    self.assertEqual(bob_groups["all_groups"][0]["group_name"], "number")
                    self.assertFalse(bob_groups["all_groups"][0]["is_member"])
                    self.assertEqual(bob_groups["my_groups"], [])

                    joined = bob.post(f"/api/groups/{group_id}/join", json={}).get_json()
                    self.assertTrue(joined["ok"], joined)
                    self.assertIn("bob", joined["group"]["members"])

                    bob_groups = bob.get("/api/groups").get_json()
                    self.assertEqual([group["group_id"] for group in bob_groups["my_groups"]], [group_id])

                    member_send = bob.post(f"/api/groups/{group_id}/send", json={"message": "hello group"}).get_json()
                    self.assertTrue(member_send["ok"])
                    self.assertEqual(member_send["group_id"], group_id)
                    self.assertEqual(member_send["group_name"], "number")
                    self.assertEqual(member_send["sent_to"], ["alice"])
                    self.assertEqual(member_send["queued_for"], [])

                    alice_group_logs = alice.get("/api/logs").get_json()["messages"]
                    bob_group_logs = bob.get("/api/logs").get_json()["messages"]
                    self.assertIn("bob \u2192 Group(number): hello group", [log["message"] for log in alice_group_logs])
                    self.assertIn("Me \u2192 Group(number): hello group", [log["message"] for log in bob_group_logs])

                    left = bob.post(f"/api/groups/{group_id}/leave", json={}).get_json()
                    self.assertTrue(left["ok"], left)
                    self.assertEqual(left["group_name"], "number")
                    self.assertEqual(bob.get("/api/groups").get_json()["my_groups"], [])

                    alice_leave_logs = alice.get("/api/logs").get_json()["messages"]
                    self.assertIn("bob left Group(number)", [log["message"] for log in alice_leave_logs])

                    self.assertTrue(alice.post("/api/disconnect", json={}).get_json()["ok"])
                    self.assertEqual(alice.get("/api/logs").get_json()["messages"], [])
                finally:
                    alice.post("/api/disconnect", json={})
                    bob.post("/api/disconnect", json={})
                    bootstrap.shutdown()
                    thread.join(timeout=2.0)
                    bootstrap_module.REGISTRY_FILE = original_registry_file


def _free_port() -> int:
    with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as sock:
        sock.bind(("127.0.0.1", 0))
        return int(sock.getsockname()[1])


if __name__ == "__main__":
    unittest.main()
