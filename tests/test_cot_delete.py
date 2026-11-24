"""Delete tasking removes saved events without changing message delivery."""

import fnmatch
import os
import queue
import unittest
from datetime import datetime, timedelta
from unittest import mock

from lxml import etree
import redis

from taky.config import app_config, load_config
from taky.cot import COTRouter, models
from taky.cot.persistence import RedisPersistence

from . import XML_S, UnittestTAKClient
from .test_geo_chat import XML_S as CHAT_XML


class CacheDeleteTest(unittest.TestCase):
    def setUp(self):
        load_config(os.devnull)
        app_config.set("taky", "redis", "false")
        app_config.set("cot_server", "log_cot", None)
        self.router = COTRouter()
        self.sender = UnittestTAKClient()
        self.recipient = UnittestTAKClient()
        self.other = UnittestTAKClient()
        self.recipient.user = models.TAKUser.from_elm(
            etree.fromstring(XML_S).find("detail"), uid="recipient"
        )
        for client in (self.sender, self.recipient, self.other):
            self.router.client_connect(client)

    def event(self, uid, etype="a-u-G", detail=None):
        now = datetime.utcnow()
        event = models.Event(
            uid=uid,
            etype=etype,
            how="m-g",
            time=now,
            start=now,
            stale=now + timedelta(hours=1),
        )
        if detail is not None:
            event.detail = models.Detail.from_elm(etree.fromstring(detail))
        return models.Event.from_elm(event.as_element)

    def save(self, *uids):
        for uid in uids:
            self.router.persist.track(self.event(uid))

    def route_delete(self, detail=None, uid="delete-request", etype="t-x-d-d"):
        event = self.event(uid, etype, detail)
        self.router.route(self.sender, event)
        return event

    def test_broadcast_delete_removes_target_from_replay_with_or_without_force(self):
        for force in ("", "<__forcedelete/>"):
            with self.subTest(force=bool(force)):
                target_type = "u-d-f" if force else "a-u-G"
                target = self.event("target", etype=target_type)
                self.router.route(self.sender, target)
                self.save("unrelated")
                self.assertIs(self.recipient.queue.get_nowait(), target)
                self.assertIs(self.other.queue.get_nowait(), target)

                detail = (
                    '<detail><link uid="target" type="'
                    + target_type
                    + '" relation="p-p"/>'
                    + force
                    + "</detail>"
                )
                event = self.route_delete(detail)

                self.assertIsNone(self.router.persist.get_event("target"))
                self.assertIs(self.recipient.queue.get_nowait(), event)
                self.assertIs(self.other.queue.get_nowait(), event)
                self.assertTrue(self.sender.queue.empty())
                self.assertEqual(
                    etree.tostring(event.detail.as_element),
                    etree.tostring(etree.fromstring(detail)),
                )

                newcomer = UnittestTAKClient()
                self.router.send_persist(newcomer)
                self.assertEqual(newcomer.queue.get_nowait().uid, "unrelated")
                self.assertRaises(queue.Empty, newcomer.queue.get_nowait)

    def test_directed_delete_removes_shared_cache_and_only_reaches_recipient(self):
        self.save("target")
        event = self.route_delete(
            '<detail><link uid="target"/>'
            '<marti><dest callsign="JENNY"/></marti></detail>'
        )

        self.assertIsNone(self.router.persist.get_event("target"))
        self.assertIs(self.recipient.queue.get_nowait(), event)
        self.assertTrue(self.other.queue.empty())
        self.assertTrue(self.sender.queue.empty())

    def test_absent_recipient_does_not_prevent_cache_deletion(self):
        self.save("target", "unrelated")
        self.route_delete(
            '<detail><link uid="target"/>'
            '<marti><dest callsign="nobody"/></marti></detail>'
        )

        self.assertIsNone(self.router.persist.get_event("target"))
        for client in (self.sender, self.recipient, self.other):
            self.assertTrue(client.queue.empty())

        newcomer = UnittestTAKClient()
        self.router.send_persist(newcomer)
        self.assertEqual(newcomer.queue.get_nowait().uid, "unrelated")
        self.assertRaises(queue.Empty, newcomer.queue.get_nowait)

    def test_chat_shaped_delete_removes_cache_before_routing(self):
        self.recipient.user.uid = "recipient-uid"
        self.recipient.user.callsign = "RECIPIENT"
        cases = (
            ("RECIPIENT", "recipient-uid", "RootContactGroup", True),
            ("NOBODY", "nobody", "RootContactGroup", False),
            ("Cyan", "Cyan", "TeamGroups", True),
            ("All Chat Rooms", "All Chat Rooms", "RootContactGroup", True),
        )
        for room, recipient, parent, delivered in cases:
            for with_marti in (False, True):
                with self.subTest(room=room, with_marti=with_marti):
                    elm = etree.fromstring(CHAT_XML)
                    elm.attrib.update(
                        self.event("delete-request", "t-x-d-d").as_element.attrib
                    )
                    detail = elm.find("detail")
                    detail.find("link").set("uid", "target")
                    chat = detail.find("__chat")
                    chat.set("chatroom", room)
                    chat.set("id", recipient)
                    chat.set("parent", parent)
                    marti = detail.find("marti")
                    if with_marti:
                        marti.find("dest").set("callsign", "ignored-callsign")
                    else:
                        detail.remove(marti)
                    event = models.Event.from_elm(elm)
                    self.save("target", "unrelated")

                    self.router.route(self.sender, event)

                    self.assertIsNone(self.router.persist.get_event("target"))
                    self.assertEqual(self.router.persist.get_uids(), ["unrelated"])
                    if delivered:
                        self.assertIs(self.recipient.queue.get_nowait(), event)
                    self.assertTrue(self.recipient.queue.empty())
                    if room == "All Chat Rooms":
                        self.assertIs(self.other.queue.get_nowait(), event)
                    self.assertTrue(self.other.queue.empty())
                    self.assertTrue(self.sender.queue.empty())

    def test_multiple_unknown_and_repeated_targets_are_harmless(self):
        self.save("first", "second", "unrelated")
        detail = (
            '<detail><link uid="first"/><link uid="missing"/>'
            '<link uid="second"/><link uid="first"/></detail>'
        )
        self.route_delete(detail)
        self.route_delete(detail)

        self.assertEqual(self.router.persist.get_uids(), ["unrelated"])

    def test_missing_targets_do_not_use_event_uid_or_nested_links(self):
        for detail in (
            None,
            "<detail/>",
            '<detail><link/><link uid=""/>'
            '<shape><link uid="nested"/></shape><__forcedelete/></detail>',
        ):
            with self.subTest(detail=detail):
                self.save("delete-request", "nested")
                self.route_delete(detail)
                self.assertEqual(
                    set(self.router.persist.get_uids()), {"delete-request", "nested"}
                )

    def test_other_event_types_do_not_delete_linked_items(self):
        self.save("target")
        self.route_delete(
            '<detail><link uid="target"/><__forcedelete/></detail>',
            etype="t-x-x-d",
        )

        self.assertIsNotNone(self.router.persist.get_event("target"))

    def test_later_update_can_save_deleted_uid_again(self):
        self.save("target")
        self.route_delete('<detail><link uid="target"/></detail>')
        update = self.event("target")
        self.router.route(self.sender, update)

        self.assertEqual(self.router.persist.get_event("target").uid, "target")
        self.assertEqual(self.router.persist.get_uids(), ["target"])


class RedisCacheDeleteTest(CacheDeleteTest):
    def setUp(self):
        super().setUp()
        patcher = mock.patch("taky.cot.persistence.redis.StrictRedis")
        self.addCleanup(patcher.stop)
        self.redis = patcher.start().return_value
        self.values = {}
        self.redis.keys.side_effect = lambda pattern: [
            key for key in self.values if fnmatch.fnmatchcase(key, pattern)
        ]
        self.redis.set.side_effect = self.values.__setitem__
        self.redis.get.side_effect = self.values.get
        self.redis.exists.side_effect = lambda key: int(key in self.values)
        self.redis.delete.side_effect = self.delete_keys
        self.router.persist = RedisPersistence("test")

    def delete_keys(self, *keys):
        return sum(self.values.pop(key, None) is not None for key in keys)

    def test_removal_only_deletes_exact_key_in_configured_namespace(self):
        self.save("target:*")
        self.values["taky:test:persist:target:other"] = b"keep"
        self.values["taky:other:persist:target:*"] = b"keep"

        self.router.persist.remove_events(["target:*"])

        self.redis.delete.assert_called_once_with("taky:test:persist:target:*")
        self.assertEqual(
            set(self.values),
            {"taky:test:persist:target:other", "taky:other:persist:target:*"},
        )

    def test_missing_targets_and_empty_batches_do_not_call_redis(self):
        for detail in (
            None,
            "<detail/>",
            '<detail><link/><link uid=""/><shape><link uid="nested"/></shape></detail>',
        ):
            with self.subTest(detail=detail):
                self.route_delete(detail)
        self.router.persist.remove_events([])

        self.redis.delete.assert_not_called()

    def test_large_batch_uses_one_redis_command_with_distinct_targets(self):
        detail = (
            "<detail>"
            + "".join(
                '<link uid="target-%d"/>' % (index % 5000) for index in range(10000)
            )
            + "</detail>"
        )
        expected_keys = {"taky:test:persist:target-%d" % index for index in range(5000)}
        for error in (None, redis.ConnectionError("unavailable")):
            with self.subTest(error=error):
                self.redis.delete.reset_mock()
                self.redis.delete.side_effect = error or self.delete_keys
                event = self.route_delete(detail)

                self.redis.delete.assert_called_once()
                keys = self.redis.delete.call_args[0]
                self.assertEqual(len(keys), 5000)
                self.assertEqual(set(keys), expected_keys)
                self.assertIs(self.recipient.queue.get_nowait(), event)
                self.assertIs(self.other.queue.get_nowait(), event)

    def test_redis_failures_still_forward_and_later_removal_recovers(self):
        for error_type in (
            redis.ConnectionError,
            redis.TimeoutError,
            redis.ReadOnlyError,
            redis.ResponseError,
        ):
            with self.subTest(error=error_type.__name__):
                self.save("target")
                self.redis.delete.reset_mock()
                self.redis.delete.side_effect = error_type("unavailable")
                with mock.patch.object(self.router.persist.lgr, "warning"):
                    event = self.route_delete('<detail><link uid="target"/></detail>')

                self.assertFalse(self.router.persist.rds_ok)
                self.assertIn("taky:test:persist:target", self.values)
                self.redis.delete.assert_called_once_with("taky:test:persist:target")
                self.assertIs(self.recipient.queue.get_nowait(), event)
                self.assertIs(self.other.queue.get_nowait(), event)
                self.redis.delete.side_effect = self.delete_keys
                with mock.patch.object(self.router.persist.lgr, "warning"):
                    self.router.persist.remove_events(["target"])

                self.assertTrue(self.router.persist.rds_ok)
                self.assertIsNone(self.router.persist.get_event("target"))
