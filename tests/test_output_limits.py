"""Regression tests for bounded output and paced persistence replay."""

import os
import ssl
import unittest
from datetime import datetime, timedelta
from unittest import mock

from lxml import etree

from taky.config import app_config, load_config
from taky.cot import models
from taky.cot.client import OutputBudget, SocketClient, SocketTAKClient
from taky.cot.persistence import Persistence

from . import XML_S


class FakeSocket:
    def __init__(self):
        self.closed = False
        self.transmitted = bytearray()
        self.send_limit = 4096
        self.blocked = False

    def fileno(self):
        return -1 if self.closed else 1

    def getpeername(self):
        return ("127.0.0.1", 12345)

    def shutdown(self, _how):
        pass

    def close(self):
        self.closed = True

    def send(self, data):
        if self.blocked:
            raise BlockingIOError()
        sent = min(self.send_limit, len(data))
        self.transmitted.extend(data[:sent])
        return sent


class OutputLimitsTest(unittest.TestCase):
    def setUp(self):
        load_config(os.devnull)
        app_config.set("taky", "redis", "false")
        self.clients = []

    def tearDown(self):
        for client in self.clients:
            client.disconnect()

    def client(self, budget=None, tak=False, **kwargs):
        cls = SocketTAKClient if tak else SocketClient
        client = cls(sock=FakeSocket(), output_budget=budget, **kwargs)
        self.clients.append(client)
        return client

    def limit(self, size):
        app_config.set("cot_server", "output_client_bytes", str(size))

    def event(self, uid="position", offset=0, lat=1, extra=None, etype=None):
        elm = etree.fromstring(XML_S)
        elm.set("uid", uid)
        if etype is not None:
            elm.set("type", etype)
        now = datetime.utcnow() + timedelta(seconds=offset)
        for field in ["time", "start"]:
            elm.set(field, now.isoformat(timespec="milliseconds") + "Z")
        elm.set("stale", (now + timedelta(hours=1)).isoformat() + "Z")
        elm.find("point").set("lat", str(lat))
        if extra is not None:
            elm.find("detail").append(etree.fromstring(extra))
        return models.Event.from_elm(elm)

    def drain(self, client):
        for _ in range(1000):
            if not client.out_buff:
                return
            client.socket_tx()
        self.fail("Output did not drain within 1,000 writes")

    def transmitted_events(self, client):
        return list(etree.fromstring(b"<root>" + client.sock.transmitted + b"</root>"))

    def test_shared_total_tracks_buffer_storage_through_its_lifecycle(self):
        budget = OutputBudget(1000)
        first = self.client(budget)
        second = self.client(budget)
        first.enqueue(b"abcd", key="position", timestamp=2)
        second.enqueue(b"abcdef")
        self.assertEqual(budget.retained_bytes, 10)

        self.assertFalse(first.enqueue(b"old", key="position", timestamp=1))
        self.assertEqual(budget.retained_bytes, 10)
        first.enqueue(b"12345", key="position", timestamp=3)
        first.out_buff.append(b"zz")
        self.assertEqual(budget.retained_bytes, 13)

        first.sock.send_limit = 3
        first.socket_tx()
        self.assertEqual(budget.retained_bytes, 13)
        first.sock.send_limit = 4
        first.socket_tx()
        self.assertEqual(budget.retained_bytes, 6)

        second.sock.blocked = True
        second.socket_tx()
        self.assertEqual(budget.retained_bytes, 6)
        second.sock.send = mock.Mock(side_effect=ssl.SSLWantReadError)
        second.socket_tx()
        self.assertEqual(budget.retained_bytes, 6)
        second.disconnect()
        second.disconnect()
        second.out_buff.clear()
        self.assertEqual(budget.retained_bytes, 0)

        first.enqueue(b"again")
        self.assertEqual(budget.retained_bytes, 5)
        first.out_buff.clear()
        self.assertEqual(budget.retained_bytes, 0)

    def test_admission_and_total_reads_do_not_scan_connected_clients(self):
        class NoIterationSet(set):
            def __iter__(self):
                raise AssertionError("Normal queue admission scanned clients")

        budget = OutputBudget(1000)
        clients = [self.client(budget) for _ in range(10)]
        budget.clients = NoIterationSet(budget.clients)
        for client in clients:
            self.assertTrue(client.enqueue(b"payload"))
        self.assertEqual(budget.retained_bytes, 70)
        for client in clients:
            client.socket_tx()
        self.assertEqual(budget.retained_bytes, 0)

    def test_client_limit_counts_partially_sent_backing_bytes(self):
        self.limit(100)
        budget = OutputBudget(1000)
        client = self.client(budget)
        self.assertTrue(client.enqueue(b"x" * 100))
        client.sock.send_limit = 99
        client.socket_tx()
        self.assertEqual(len(client.out_buff), 1)
        self.assertEqual(budget.retained_bytes, 100)

        self.assertFalse(client.enqueue(b"y"))

        self.assertTrue(client.is_closed)
        self.assertEqual(budget.retained_bytes, 0)
        self.assertFalse(client.out_buff)

    def test_aggregate_pressure_disconnects_largest_retained_queue(self):
        self.limit(100)
        budget = OutputBudget(100)
        largest = self.client(budget)
        smaller = self.client(budget)
        recipient = self.client(budget)
        largest.enqueue(b"x" * 60)
        smaller.enqueue(b"y" * 20)
        largest.sock.send_limit = 59
        largest.socket_tx()

        self.assertTrue(recipient.enqueue(b"z" * 30))

        self.assertTrue(largest.is_closed)
        self.assertFalse(smaller.is_closed)
        self.assertFalse(recipient.is_closed)
        self.assertEqual(budget.retained_bytes, 50)

    def test_budget_registration_precedes_connect_callback(self):
        budget = OutputBudget(100)
        observed = []

        def connected(client):
            observed.append(client in budget.clients)
            client.enqueue(b"hello")
            observed.append(budget.retained_bytes)

        client = self.client(budget, cbs={"connect": connected})

        self.assertEqual(observed, [True, 5])
        client.disconnect()
        client.disconnect()
        self.assertEqual(budget.retained_bytes, 0)
        self.assertNotIn(client, budget.clients)

    def test_replacement_does_not_extend_stall_deadline(self):
        client = self.client()
        timestamp = datetime.utcnow()
        with mock.patch("time.monotonic", return_value=100):
            client.enqueue(b"old", key="position", timestamp=timestamp)
        with mock.patch("time.monotonic", return_value=130):
            client.enqueue(
                b"new", key="position", timestamp=timestamp + timedelta(seconds=1)
            )

        client.check_output_timeout(now=161)

        self.assertTrue(client.is_closed)

    def test_incoming_positions_do_not_extend_stall_deadline(self):
        client = self.client(tak=True)
        with mock.patch("time.monotonic", return_value=100):
            client.enqueue(b"waiting")
        with mock.patch("time.monotonic", return_value=155):
            client.feed(etree.tostring(self.event().as_element))

        client.check_output_timeout(now=161)

        self.assertEqual(client.num_rx, 1)
        self.assertTrue(client.is_closed)

    def test_only_successful_send_resets_stall_deadline(self):
        client = self.client()
        with mock.patch("time.monotonic", return_value=100):
            client.enqueue(b"waiting")
        client.sock.send_limit = 1
        with mock.patch("time.monotonic", return_value=150):
            client.socket_tx()
        client.check_output_timeout(now=180)
        self.assertFalse(client.is_closed)

        client.sock.blocked = True
        with mock.patch("time.monotonic", return_value=200):
            client.socket_tx()
        client.check_output_timeout(now=211)
        self.assertTrue(client.is_closed)

    def test_empty_queue_has_no_stall_deadline(self):
        client = self.client()
        with mock.patch("time.monotonic", return_value=100):
            client.enqueue(b"complete")
            self.drain(client)

        client.check_output_timeout(now=10000)

        self.assertFalse(client.is_closed)

    def test_tls_write_waiting_for_read_retries_unchanged_payload(self):
        client = self.client()
        client.sock.recv = mock.Mock()
        client.sock.send = mock.Mock(side_effect=[ssl.SSLWantReadError(), 3])
        with mock.patch("time.monotonic", return_value=100):
            client.enqueue(b"old")
            client.socket_tx()
        self.assertFalse(client.has_data)
        self.assertTrue(client.wants_read)
        self.assertFalse(client.is_closed)
        client.enqueue(b"new")

        with mock.patch("time.monotonic", return_value=150):
            client.socket_rx()

        client.sock.recv.assert_not_called()
        offered = [bytes(call[0][0]) for call in client.sock.send.call_args_list]
        self.assertEqual(offered, [b"old", b"old"])
        self.assertTrue(client.has_data)
        self.assertTrue(client.wants_read)
        self.assertEqual(client.out_buff.progress_at, 150)
        self.assertEqual(len(client.out_buff), 3)

    def test_tls_write_waiting_for_write_requests_writable_socket(self):
        client = self.client()
        client.sock.send = mock.Mock(side_effect=[ssl.SSLWantWriteError(), 3])
        client.enqueue(b"old")
        client.socket_tx()
        self.assertTrue(client.has_data)
        self.assertFalse(client.wants_read)
        self.assertFalse(client.is_closed)
        client.enqueue(b"new")

        client.socket_tx()

        offered = [bytes(call[0][0]) for call in client.sock.send.call_args_list]
        self.assertEqual(offered, [b"old", b"old"])
        self.assertTrue(client.has_data)
        self.assertTrue(client.wants_read)

    def test_tls_read_waiting_for_read_keeps_connection_open(self):
        client = self.client()
        client.feed = mock.Mock()
        client.sock.recv = mock.Mock(side_effect=[ssl.SSLWantReadError(), b"received"])

        client.socket_rx()

        self.assertTrue(client.wants_read)
        self.assertFalse(client.has_data)
        self.assertFalse(client.is_closed)
        client.socket_rx()
        client.feed.assert_called_once_with(b"received")

    def test_tls_read_waiting_for_write_retries_read_before_output(self):
        client = self.client()
        client.feed = mock.Mock()
        client.sock.recv = mock.Mock(side_effect=[ssl.SSLWantWriteError(), b"received"])
        client.sock.send = mock.Mock()
        client.enqueue(b"queued")

        client.socket_rx()

        self.assertFalse(client.wants_read)
        self.assertTrue(client.has_data)
        self.assertFalse(client.is_closed)
        client.socket_tx()
        client.feed.assert_called_once_with(b"received")
        client.sock.send.assert_not_called()
        self.assertEqual(len(client.out_buff), len(b"queued"))
        self.assertTrue(client.wants_read)
        self.assertTrue(client.has_data)

    def test_superseded_position_moves_after_intervening_message(self):
        client = self.client(tak=True)
        client.send_event(self.event(lat=1))
        client.send_event(self.event(uid="command", etype="b-a-o-tbl"))
        client.send_event(self.event(offset=1, lat=2))
        self.drain(client)

        events = self.transmitted_events(client)
        self.assertEqual(
            [event.get("uid") for event in events], ["command", "position"]
        )
        self.assertEqual(events[-1].find("point").get("lat"), "2.000000")

    def test_partially_sent_position_is_finished_before_new_position(self):
        client = self.client(tak=True)
        client.send_event(self.event(lat=1))
        client.sock.send_limit = 1
        client.socket_tx()
        client.send_event(self.event(offset=1, lat=2))
        client.sock.send_limit = 4096
        self.drain(client)

        events = self.transmitted_events(client)
        self.assertEqual(len(events), 2)
        self.assertEqual(
            [float(event.find("point").get("lat")) for event in events], [1, 2]
        )

    def test_replacement_reclaims_space_before_client_limit_check(self):
        first = self.event(lat=1)
        latest = self.event(offset=1, lat=2)
        size = max(len(etree.tostring(event.as_element)) for event in [first, latest])
        self.limit(size)
        budget = OutputBudget(size)
        client = self.client(budget, tak=True)

        client.send_event(first)
        client.send_event(latest)

        self.assertFalse(client.is_closed)
        self.assertLessEqual(budget.retained_bytes, size)
        self.drain(client)
        events = self.transmitted_events(client)
        self.assertEqual(len(events), 1)
        self.assertEqual(float(events[0].find("point").get("lat")), 2)

    def test_critical_or_unknown_details_remain_in_order(self):
        for detail in [
            b"<emergency type='911 Alert'/>",
            b"<marti/>",
            b"<extension value='unknown'/>",
            b"<status><extension/></status>",
        ]:
            with self.subTest(detail=detail):
                client = self.client(tak=True)
                client.send_event(self.event(lat=1, extra=detail))
                client.send_event(self.event(offset=1, lat=2))
                self.drain(client)
                events = self.transmitted_events(client)
                self.assertEqual(len(events), 2)
                self.assertEqual(
                    [float(event.find("point").get("lat")) for event in events],
                    [1, 2],
                )

    def test_programmatically_built_user_can_be_sent(self):
        event = self.event()
        event.detail.elm = None
        client = self.client(tak=True)

        client.send_event(event)
        self.drain(client)

        events = self.transmitted_events(client)
        self.assertEqual(len(events), 1)
        self.assertEqual(events[0].find("detail/contact").get("callsign"), "JENNY")

    def test_persistence_lookup_uses_requested_uid_and_excludes_expired(self):
        persistence = Persistence()
        event = self.event()
        persistence.track_event(event, 3600)
        self.assertIs(persistence.get_event(event.uid), event)
        self.assertEqual(persistence.get_uids(), [event.uid])
        event.stale = datetime.utcnow() - timedelta(seconds=1)
        self.assertIsNone(persistence.get_event(event.uid))
        self.assertEqual(persistence.get_uids(), [])

    def test_replay_larger_than_queue_limit_is_paced(self):
        persistence = Persistence()
        events = [self.event(uid=str(index)) for index in range(5)]
        for event in events:
            persistence.track_event(event, 3600)
        largest = max(len(etree.tostring(event.as_element)) for event in events)
        self.limit(largest)
        budget = OutputBudget(largest * 2)
        client = self.client(budget, tak=True)
        client.start_replay(persistence)

        for _ in events:
            client.pump_replay()
            self.assertLessEqual(budget.retained_bytes, largest)
            self.assertFalse(client.is_closed)
            self.drain(client)

        self.assertFalse(client.replay_pending)
        self.assertEqual(
            [event.get("uid") for event in self.transmitted_events(client)],
            [str(index) for index in range(5)],
        )

    def test_replay_looks_up_latest_value_after_snapshot_changes(self):
        persistence = Persistence()
        persistence.track_event(self.event(uid="updated", lat=1), 3600)
        persistence.track_event(self.event(uid="removed"), 3600)
        client = self.client(tak=True)
        client.enqueue(b"held")
        client.start_replay(persistence)
        persistence.track_event(self.event(uid="updated", offset=1, lat=2), 3600)
        persistence.events.pop("removed")
        persistence.track_event(self.event(uid="added"), 3600)
        self.drain(client)
        client.sock.transmitted.clear()

        for _ in range(3):
            client.pump_replay()
            self.drain(client)

        events = self.transmitted_events(client)
        self.assertEqual([event.get("uid") for event in events], ["updated"])
        self.assertEqual(float(events[0].find("point").get("lat")), 2)
        self.assertFalse(client.replay_pending)

    def test_live_event_prevents_older_replay_for_same_uid(self):
        persistence = Persistence()
        persistence.track_event(self.event(lat=1), 3600)
        client = self.client(tak=True)
        client.enqueue(b"held")
        client.start_replay(persistence)
        self.drain(client)
        client.sock.transmitted.clear()

        client.send_event(self.event(offset=1, lat=2))
        self.drain(client)
        client.pump_replay()
        self.drain(client)

        events = self.transmitted_events(client)
        self.assertEqual(len(events), 1)
        self.assertEqual(float(events[0].find("point").get("lat")), 2)

    def test_replay_defers_aggregate_pressure_without_disconnect(self):
        event = self.event()
        size = len(etree.tostring(event.as_element))
        self.limit(size)
        budget = OutputBudget(size)
        other = self.client(budget)
        other.enqueue(b"occupied")
        persistence = Persistence()
        persistence.track_event(event, 3600)
        client = self.client(budget, tak=True)
        client.start_replay(persistence)

        client.pump_replay()

        self.assertFalse(other.is_closed)
        self.assertFalse(client.is_closed)
        self.assertFalse(client.out_buff)
        self.assertIn(event.uid, client.replay_pending)
        self.assertEqual(budget.retained_bytes, len(b"occupied"))

        other.disconnect()
        client.pump_replay()
        self.drain(client)
        self.assertEqual(len(self.transmitted_events(client)), 1)
        self.assertFalse(client.replay_pending)

    def test_replay_skips_oversized_event_and_continues(self):
        oversized = self.event(uid="large")
        small = self.event(uid="small")
        small.detail = None
        size = len(etree.tostring(small.as_element))
        self.limit(size)
        persistence = Persistence()
        persistence.track_event(oversized, 3600)
        persistence.track_event(small, 3600)
        client = self.client(OutputBudget(size), tak=True)
        client.start_replay(persistence)

        for _ in range(3):
            client.pump_replay()
            self.drain(client)

        self.assertFalse(client.is_closed)
        self.assertFalse(client.replay_pending)
        self.assertEqual(
            [event.get("uid") for event in self.transmitted_events(client)], ["small"]
        )

    def test_disconnect_releases_pending_replay_and_output(self):
        persistence = Persistence()
        persistence.track_event(self.event(), 3600)
        budget = OutputBudget(10000)
        client = self.client(budget, tak=True)
        client.enqueue(b"held")
        client.start_replay(persistence)
        self.assertTrue(client.replay_pending)

        client.disconnect()
        client.pump_replay()

        self.assertFalse(client.out_buff)
        self.assertFalse(client.replay_pending)
        self.assertEqual(budget.retained_bytes, 0)
        self.assertNotIn(client, budget.clients)
