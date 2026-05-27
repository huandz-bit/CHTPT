import unittest

from peer.group_chat import send_group


class FakePeer:
    def __init__(self):
        self.sent = []

    def send_chat(self, target, text, group=False, recipients=None):
        self.sent.append((target, text, group, recipients))
        return True


class GroupChatTests(unittest.TestCase):
    def test_group_chat_sends_to_all_targets(self):
        peer = FakePeer()
        results = send_group(peer, ["bob", "carol"], "hello team")

        self.assertEqual(results, {"bob": True, "carol": True})
        self.assertEqual(len(peer.sent), 2)
        self.assertTrue(all(item[2] is True for item in peer.sent))


if __name__ == "__main__":
    unittest.main()
