import os
import unittest as ut
import mock

from lxml import etree

from taky import cot
from taky.config import load_config, app_config
from taky.cot import models

from .test_cot_event import XML_S


class TAKClientTest(ut.TestCase):
    def setUp(self):
        load_config(os.devnull)
        app_config.set("taky", "redis", "false")
        router = cot.COTRouter()
        self.tk = cot.TAKClient(cbs={"route": router.route})

    def test_ident(self):
        self.tk.feed(XML_S)

        self.assertEqual(self.tk.user.callsign, "JENNY")
        self.assertEqual(self.tk.user.uid, "ANDROID-deadbeef")
        self.assertEqual(self.tk.user.device.os, "29")
        self.assertEqual(self.tk.user.device.device, "Some Android Device")
        self.assertEqual(self.tk.user.group, cot.Teams.CYAN)
        self.assertEqual(self.tk.user.battery, "78")
        self.assertEqual(self.tk.user.role, "Team Member")


class SocketTAKClientTest(ut.TestCase):
    def setUp(self):
        load_config(os.devnull)
        app_config.set("taky", "redis", "false")
        router = cot.COTRouter()

        self.mock_sock = mock.patch("socket.socket")
        self.sock = self.mock_sock.start()
        self.sock.recv.return_value = b"</invalid>"
        self.sock.getpeername.return_value = (
            "127.0.0.1",
            12345,
        )

        self.tk = cot.SocketTAKClient(sock=self.sock, use_ssl=False, router=router)

    def test_invalid_xml(self):
        self.tk.socket_rx()
        self.sock.close.assert_called()

    def test_socket_tx_sends_events_in_order_with_partial_writes(self):
        first_elm = etree.fromstring(XML_S)
        remarks = etree.SubElement(first_elm.find("detail"), "remarks")
        remarks.text = "x" * 5000
        first_event = models.Event.from_elm(first_elm)

        second_elm = etree.fromstring(XML_S)
        second_elm.set("uid", "ANDROID-cafebabe")
        second_event = models.Event.from_elm(second_elm)

        expected = etree.tostring(first_event.as_element)
        expected += etree.tostring(second_event.as_element)
        self.tk.send_event(first_event)
        self.tk.send_event(second_event)

        transmitted = bytearray()
        offered = []

        def send(data):
            data = bytes(data)
            offered.append(data)
            sent = min(1024, len(data))
            transmitted.extend(data[:sent])
            return sent

        self.sock.send.side_effect = send
        for _ in range(20):
            if not self.tk.has_data:
                break
            self.tk.socket_tx()
        else:
            self.fail("Queued events were not fully transmitted")

        self.assertEqual(bytes(transmitted), expected)
        self.assertEqual(offered[0], expected[:4096])
        self.assertTrue(all(len(data) <= 4096 for data in offered))

    def test_socket_tx_preserves_event_when_write_blocks(self):
        event = models.Event.from_elm(etree.fromstring(XML_S))
        expected = etree.tostring(event.as_element)
        self.tk.send_event(event)

        self.sock.send.side_effect = BlockingIOError
        self.tk.socket_tx()

        self.assertTrue(self.tk.has_data)

        transmitted = bytearray()

        def send(data):
            data = bytes(data)
            transmitted.extend(data)
            return len(data)

        self.sock.send.side_effect = send
        self.tk.socket_tx()

        self.assertEqual(bytes(transmitted), expected)
        self.assertFalse(self.tk.has_data)

    def tearDown(self):
        self.mock_sock.stop()
