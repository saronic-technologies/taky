import unittest as ut
from unittest import mock

from taky.cot.mgmt import MgmtClient


class MgmtClientTest(ut.TestCase):
    def setUp(self):
        self.sock = mock.Mock()
        self.sock.getpeername.return_value = ("127.0.0.1", 12345)
        self.client = MgmtClient(
            sock=self.sock,
            use_ssl=False,
            server=mock.Mock(),
        )

    def test_responses_are_transmitted_in_order(self):
        self.client.feed(b'{"cmd": "ping"}\0{"cmd": "invalid"}\0')

        transmitted = bytearray()

        def send(data):
            data = bytes(data)
            transmitted.extend(data)
            return len(data)

        self.sock.send.side_effect = send
        for _ in range(3):
            if not self.client.has_data:
                break
            self.client.socket_tx()
        else:
            self.fail("Management responses were not fully transmitted")

        self.assertEqual(
            bytes(transmitted),
            b'{"pong": "taky"}\0{"error": "Invalid cmd: invalid"}\0',
        )
