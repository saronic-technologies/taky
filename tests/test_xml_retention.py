import gc
import os
import unittest as ut
import weakref
from datetime import datetime, timedelta
from unittest import mock

from lxml import etree

from taky.config import app_config, load_config
from taky.cot import COTRouter, TAKClient

from . import XML_S, elements_equal


class XMLRetentionTest(ut.TestCase):
    def setUp(self):
        load_config(os.devnull)
        app_config.set("taky", "redis", "false")
        app_config.set("cot_server", "log_cot", None)

    def packet(self, uid, **attributes):
        elm = etree.fromstring(XML_S)
        elm.set("uid", uid)
        elm.set("stale", (datetime.utcnow() + timedelta(hours=1)).isoformat())
        for name, value in attributes.items():
            elm.set(name, value)
        return etree.tostring(elm, xml_declaration=True)

    def test_completed_events_are_removed_for_all_input_layouts(self):
        packets = [self.packet(str(index)) for index in range(200)]
        combined = b"".join(packets)
        layouts = {
            "separate": packets,
            "coalesced": [combined],
            "fragmented": [
                combined[index : index + 37] for index in range(0, len(combined), 37)
            ],
        }
        for layout, chunks in layouts.items():
            with self.subTest(layout=layout):
                received = []
                client = TAKClient(
                    cbs={"route": lambda _, event: received.append(event)}
                )
                for chunk in chunks:
                    client.feed(chunk)

                self.assertEqual(
                    [event.uid for event in received], [str(i) for i in range(200)]
                )
                self.assertEqual(client.num_rx, 200)
                self.assertTrue(
                    all(event.detail.callsign == "JENNY" for event in received)
                )
                root = received[0].detail.elm.getroottree().getroot()
                self.assertEqual(root.tag, "root")
                self.assertEqual(len(root), 1)
                self.assertEqual(len(root[0]), 0)
                self.assertEqual(dict(root[0].attrib), {})

    def test_cleanup_runs_on_early_returns_and_callback_errors(self):
        for outcome in ["invalid_fields", "ping", "callback_error"]:
            with self.subTest(outcome=outcome):
                route = mock.Mock()
                client = TAKClient(cbs={"route": route})
                client.feed(self.packet("initial"))
                root = route.call_args[0][1].detail.elm.getroottree().getroot()
                client.pong = mock.Mock()
                if outcome == "invalid_fields":
                    packet = self.packet("invalid", stale="invalid")
                elif outcome == "ping":
                    packet = self.packet("ping", type="t-x-c-t")
                else:
                    packet = self.packet("callback")
                    route.side_effect = RuntimeError("Route callback failed")

                with mock.patch.object(client.lgr, "error"):
                    for _ in range(10):
                        client.feed(packet)
                        self.assertEqual(len(root), 1)
                if outcome == "ping":
                    self.assertEqual(client.pong.call_count, 10)
                if outcome != "callback_error":
                    self.assertEqual(route.call_count, 1)

                route.side_effect = None
                client.feed(self.packet("following"))
                self.assertEqual(route.call_args[0][1].uid, "following")
                self.assertEqual(len(root), 1)

    def test_persisted_detail_survives_client_collection_without_history(self):
        router = COTRouter()
        client = TAKClient(cbs={"route": router.route})
        router.client_connect(client)
        packet = self.packet("marker")
        for _ in range(200):
            client.feed(packet)

        reference = weakref.ref(client)
        router.client_disconnect(client)
        del client
        gc.collect()
        self.assertIsNone(reference())

        persisted = list(router.persist.get_all())
        self.assertEqual(len(persisted), 1)
        event = persisted[0]
        root = event.detail.elm.getroottree().getroot()
        self.assertEqual(root.tag, "root")
        self.assertEqual(len(root), 1)
        self.assertIsNone(event.detail.elm.getparent())
        self.assertEqual(event.uid, "marker")
        self.assertTrue(
            elements_equal(
                etree.fromstring(packet).find("detail"), event.as_element.find("detail")
            )
        )
