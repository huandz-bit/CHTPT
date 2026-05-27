import unittest

from common.protocol import build_message
from peer.peer_client import DeliveryError, PeerClient


class MessageDeliveryTests(unittest.TestCase):
    def test_delivery_error_for_unreachable_peer(self):
        message = build_message("CHAT", **{"from": "alice", "to": "nobody", "message": "test"})
        with self.assertRaises(DeliveryError):
            PeerClient(timeout=0.2).send_with_ack("127.0.0.1", 9, message)


if __name__ == "__main__":
    unittest.main()
