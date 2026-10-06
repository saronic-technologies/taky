import ssl
import unittest
from unittest import mock

from taky.cot.output import OutputBuffer


class OutputBufferTest(unittest.TestCase):
    def setUp(self):
        self.clock = mock.patch("taky.cot.output.time.monotonic", return_value=10)
        self.now = self.clock.start()
        self.addCleanup(self.clock.stop)
        self.buffer = OutputBuffer()
        self.sock = mock.Mock()
        self.sock.send.side_effect = lambda data: len(data)

    def enqueue(self, data, key=None, timestamp=None):
        if self.buffer.remove_superseded(key, timestamp):
            self.buffer.append(data, key, timestamp)
            return True
        return False

    def pending(self):
        return bytes(self.buffer._peek(max(1, len(self.buffer))))

    def test_empty_and_immutable_input(self):
        self.assertFalse(self.buffer)
        self.assertEqual(self.buffer.send(self.sock), 0)
        self.sock.send.assert_not_called()
        self.buffer.append(b"")
        self.assertIsNone(self.buffer.progress_at)
        with self.assertRaises(TypeError):
            self.buffer.append(bytearray(b"mutable"))

    def test_partial_head_keeps_full_backing_storage(self):
        self.buffer.append(b"a" * 100)
        self.buffer.append(b"b" * 20)
        self.sock.send.return_value = 90
        self.sock.send.side_effect = None
        self.assertEqual(self.buffer.send(self.sock), 90)
        self.assertEqual(len(self.buffer), 30)
        self.assertEqual(self.buffer.retained_bytes, 120)

        self.sock.send.return_value = 10
        self.buffer.send(self.sock)
        self.assertEqual(len(self.buffer), 20)
        self.assertEqual(self.buffer.retained_bytes, 20)
        self.sock.send.return_value = 20
        self.buffer.send(self.sock)
        self.assertEqual(self.buffer.retained_bytes, 0)
        self.assertIsNone(self.buffer.progress_at)

    def test_batches_partial_writes_across_chunk_boundaries(self):
        self.buffer.append(b"abc")
        self.buffer.append(b"defg")
        offered = []

        def send(data):
            offered.append(bytes(data))
            return min(5, len(data))

        self.sock.send.side_effect = send
        self.buffer.send(self.sock)
        self.buffer.append(b"hij")
        self.buffer.send(self.sock)
        self.assertEqual(offered, [b"abcdefg", b"fghij"])
        self.assertFalse(self.buffer)
        self.assertEqual(self.buffer.retained_bytes, 0)

    def test_limits_each_new_write_to_requested_size(self):
        self.buffer.append(b"a" * 3000)
        self.buffer.append(b"b" * 3000)
        self.buffer.send(self.sock)
        self.assertEqual(len(self.sock.send.call_args[0][0]), 4096)
        self.assertEqual(self.pending(), b"b" * 1904)
        self.assertEqual(self.buffer.retained_bytes, 3000)

    def test_replacement_moves_to_tail_and_preserves_ordered_events(self):
        self.enqueue(b"old-position", "position", 1)
        self.enqueue(b"chat-one")
        self.enqueue(b"chat-two")
        self.enqueue(b"new-position", "position", 2)
        self.assertEqual(self.pending(), b"chat-onechat-twonew-position")
        self.assertEqual(len(self.buffer._chunks), 3)
        self.assertEqual(len(self.buffer._by_key), 1)
        self.assertEqual(self.buffer.retained_bytes, len(self.buffer))

    def test_replacements_release_entries_and_index_metadata(self):
        for timestamp in range(5000):
            self.enqueue(str(timestamp).encode(), "position", timestamp)
        self.assertEqual(self.pending(), b"4999")
        self.assertEqual(len(self.buffer._chunks), 1)
        self.assertEqual(len(self.buffer._by_key), 1)
        self.assertEqual(self.buffer.retained_bytes, 4)
        self.buffer.send(self.sock)
        self.assertEqual(len(self.buffer._chunks), 0)
        self.assertEqual(len(self.buffer._by_key), 0)

    def test_older_replacement_does_not_remove_newer_update(self):
        self.enqueue(b"current", "position", 20)
        self.assertFalse(self.enqueue(b"old", "position", 10))
        self.assertEqual(self.pending(), b"current")
        self.assertTrue(self.enqueue(b"same-time", "position", 20))
        self.assertEqual(self.pending(), b"same-time")

    def test_different_keys_and_unkeyed_data_are_not_replaced(self):
        self.enqueue(b"one", ("uid-one", "type"), 1)
        self.enqueue(b"two", ("uid-two", "type"), 2)
        self.enqueue(b"chat")
        self.enqueue(b"chat")
        self.assertEqual(self.pending(), b"onetwochatchat")
        self.assertEqual(len(self.buffer._chunks), 4)

    def test_partially_sent_event_cannot_be_replaced(self):
        self.enqueue(b"old-position", "position", 1)
        self.sock.send.side_effect = lambda data: 4
        self.buffer.send(self.sock)
        self.enqueue(b"next-position", "position", 2)
        self.enqueue(b"chat")
        self.enqueue(b"new-position", "position", 3)
        self.assertEqual(self.pending(), b"positionchatnew-position")
        self.assertEqual(self.buffer.retained_bytes, 12 + 4 + 12)
        self.assertEqual(len(self.buffer._by_key), 1)

    def test_older_update_does_not_follow_newer_partial_head(self):
        self.enqueue(b"current", "position", 20)
        self.sock.send.side_effect = lambda data: 1
        self.buffer.send(self.sock)
        self.assertFalse(self.enqueue(b"old", "position", 10))
        self.assertEqual(self.pending(), b"urrent")
        self.assertEqual(len(self.buffer._by_key), 0)

    def test_progress_updates_on_success_not_enqueue_or_blocking(self):
        self.enqueue(b"old", "position", 1)
        self.assertEqual(self.buffer.progress_at, 10)
        self.now.return_value = 20
        self.enqueue(b"new", "position", 2)
        self.assertEqual(self.buffer.progress_at, 10)
        self.sock.send.side_effect = BlockingIOError
        with self.assertRaises(BlockingIOError):
            self.buffer.send(self.sock)
        self.assertEqual(self.buffer.progress_at, 10)

        self.now.return_value = 30
        self.sock.send.side_effect = lambda data: 1
        self.buffer.send(self.sock)
        self.assertEqual(self.buffer.progress_at, 30)
        self.sock.send.side_effect = lambda data: len(data)
        self.buffer.send(self.sock)
        self.assertIsNone(self.buffer.progress_at)
        self.now.return_value = 40
        self.enqueue(b"again", "position", 3)
        self.assertEqual(self.buffer.progress_at, 40)

    def test_ssl_retry_protects_every_chunk_in_offered_prefix(self):
        self.enqueue(b"aaa", "a", 1)
        self.enqueue(b"bbb", "b", 1)
        self.enqueue(b"ccc", "c", 1)
        offered = []
        outcomes = iter([ssl.SSLWantReadError(), ssl.SSLWantWriteError(), 4])

        def send(data):
            offered.append(bytes(data))
            result = next(outcomes)
            if isinstance(result, Exception):
                raise result
            return result

        self.sock.send.side_effect = send
        with self.assertRaises(ssl.SSLWantReadError):
            self.buffer.send(self.sock, max_bytes=4)
        self.assertEqual(self.buffer.write_wait, "read")
        self.assertEqual(self.buffer.progress_at, 10)
        self.assertEqual(len(self.buffer._protected), 2)
        self.enqueue(b"BBB", "b", 2)
        self.enqueue(b"CCC", "c", 2)
        self.assertEqual(self.pending(), b"aaabbbBBBCCC")

        with self.assertRaises(ssl.SSLWantWriteError):
            self.buffer.send(self.sock, max_bytes=20)
        self.assertEqual(self.buffer.write_wait, "write")
        self.now.return_value = 30
        self.buffer.send(self.sock, max_bytes=1)
        self.assertEqual(offered, [b"aaab", b"aaab", b"aaab"])
        self.assertEqual(self.pending(), b"bbBBBCCC")
        self.assertEqual(self.buffer.retained_bytes, 9)
        self.assertIsNone(self.buffer.write_wait)
        self.assertIsNone(self.buffer._retry_data)
        self.assertEqual(len(self.buffer._protected), 0)
        self.assertEqual(self.buffer.progress_at, 30)
        self.assertEqual(len(self.buffer._by_key), 2)

    def test_blocking_after_ssl_want_preserves_retry_payload(self):
        self.enqueue(b"old", "position", 1)
        self.sock.send.side_effect = ssl.SSLWantReadError
        with self.assertRaises(ssl.SSLWantReadError):
            self.buffer.send(self.sock)
        self.enqueue(b"new", "position", 2)
        self.sock.send.side_effect = BlockingIOError
        with self.assertRaises(BlockingIOError):
            self.buffer.send(self.sock)
        self.assertEqual(bytes(self.buffer._retry_data), b"old")
        self.assertEqual(self.buffer.write_wait, "write")
        self.sock.send.side_effect = lambda data: len(data)
        self.buffer.send(self.sock)
        self.assertEqual(bytes(self.sock.send.call_args[0][0]), b"old")
        self.assertEqual(self.pending(), b"new")

    def test_plain_blocking_does_not_protect_unsent_chunks(self):
        self.enqueue(b"old", "position", 1)
        self.sock.send.side_effect = BlockingIOError
        with self.assertRaises(BlockingIOError):
            self.buffer.send(self.sock)
        self.enqueue(b"new", "position", 2)
        self.assertEqual(self.pending(), b"new")
        self.assertEqual(len(self.buffer._protected), 0)

    def test_clear_releases_retry_and_replacement_state(self):
        self.enqueue(b"one", "one", 1)
        self.enqueue(b"two", "two", 1)
        self.sock.send.side_effect = ssl.SSLWantReadError
        with self.assertRaises(ssl.SSLWantReadError):
            self.buffer.send(self.sock)
        self.buffer.clear()
        self.buffer.clear()
        self.assertFalse(self.buffer)
        self.assertEqual(self.buffer.retained_bytes, 0)
        self.assertEqual(len(self.buffer._chunks), 0)
        self.assertEqual(len(self.buffer._by_key), 0)
        self.assertEqual(len(self.buffer._protected), 0)
        self.assertIsNone(self.buffer._retry_data)
        self.assertIsNone(self.buffer.write_wait)
        self.assertIsNone(self.buffer.progress_at)

    def test_invalid_removal_and_zero_progress_leave_data_unchanged(self):
        self.enqueue(b"data", "position", 1)
        for count in [-1, 5]:
            with self.assertRaises(ValueError):
                self.buffer._remove(count)
        for count in [0, -1, 5]:
            self.sock.send.side_effect = lambda data, value=count: value
            with self.assertRaises(ConnectionError):
                self.buffer.send(self.sock)
            self.assertEqual(self.pending(), b"data")
            self.assertEqual(self.buffer.retained_bytes, 4)
        for limit in [0, -1]:
            with self.assertRaises(ValueError):
                self.buffer.send(self.sock, limit)
