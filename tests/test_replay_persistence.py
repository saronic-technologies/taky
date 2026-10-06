"""Redis replay snapshots contain identifiers and fetch current values."""

import unittest
from unittest import mock

from lxml import etree

from taky.cot.persistence import RedisPersistence

from . import XML_S


class RedisReplayTest(unittest.TestCase):
    def setUp(self):
        patcher = mock.patch("taky.cot.persistence.redis.StrictRedis")
        self.addCleanup(patcher.stop)
        self.redis = patcher.start().return_value
        self.redis.keys.return_value = []
        self.persistence = RedisPersistence("test")

    def test_snapshot_decodes_unicode_uids_without_fetching_events(self):
        self.redis.keys.return_value = [
            "taky:test:persist:unit:café".encode("utf8"),
            "taky:test:persist:plain",
        ]

        uids = self.persistence.get_uids()

        self.assertEqual(uids, ["unit:café", "plain"])
        self.redis.get.assert_not_called()

    def test_lookup_handles_expired_key_and_fetches_current_value(self):
        old = etree.fromstring(XML_S)
        new = etree.fromstring(XML_S)
        new.find("point").set("lat", "2")
        self.redis.get.side_effect = [
            None,
            etree.tostring(old),
            etree.tostring(new),
        ]

        self.assertIsNone(self.persistence.get_event("expired"))
        self.assertAlmostEqual(
            self.persistence.get_event("ANDROID-deadbeef").point.lat, 1.234567
        )
        self.assertEqual(self.persistence.get_event("ANDROID-deadbeef").point.lat, 2)
        self.redis.get.assert_has_calls(
            [
                mock.call("taky:test:persist:expired"),
                mock.call("taky:test:persist:ANDROID-deadbeef"),
                mock.call("taky:test:persist:ANDROID-deadbeef"),
            ]
        )
