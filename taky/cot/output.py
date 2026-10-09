"""Outgoing socket data with replaceable, entirely unsent updates."""

import ssl
import time
from collections import OrderedDict


class OutputBuffer:
    """Keep ordered immutable chunks and account for their retained storage."""

    def __init__(self, retained_change=None):
        self._chunks = OrderedDict()
        self._by_key = {}
        self._next_id = 0
        self._head_offset = 0
        self._size = 0
        self._retained_bytes = 0
        self._retained_change = retained_change
        self._retry_data = None
        self._protected = set()
        self.progress_at = None
        self.write_wait = None

    def __bool__(self):
        return self._size > 0

    def __len__(self):
        return self._size

    @property
    def retained_bytes(self):
        """Bytes owned by queued chunks, including the sent part of the head."""
        return self._retained_bytes

    def remove_superseded(self, key, timestamp=None):
        """Prepare a replacement, returning False for an older update.

        The caller must append the replacement or clear the buffer. Keep the
        progress deadline when replacing the only remaining chunk.
        """
        if key is None:
            return True

        if self._head_offset and timestamp is not None:
            _, head_key, head_time = next(iter(self._chunks.values()))
            if head_key == key and head_time is not None and timestamp < head_time:
                return False
        if key not in self._by_key:
            return True

        chunk_id = self._by_key[key]
        data, _, previous_time = self._chunks[chunk_id]
        if timestamp is not None and previous_time is not None:
            if timestamp < previous_time:
                return False

        if chunk_id in self._protected:
            return True

        self._size -= len(data)
        self._discard(chunk_id)
        return True

    def append(self, data, key=None, timestamp=None):
        """Append bytes after replacement and capacity checks have succeeded."""
        if not isinstance(data, bytes):
            raise TypeError("OutputBuffer accepts bytes")
        if not data:
            return

        chunk_id = self._next_id
        self._next_id += 1
        self._chunks[chunk_id] = (data, key, timestamp)
        if key is not None:
            self._by_key[key] = chunk_id
        self._size += len(data)
        self._change_retained_bytes(len(data))
        if self.progress_at is None:
            self.progress_at = time.monotonic()

    def clear(self):
        self._chunks.clear()
        self._by_key.clear()
        self._head_offset = 0
        self._size = 0
        self._change_retained_bytes(-self._retained_bytes)
        self._retry_data = None
        self._protected.clear()
        self.progress_at = None
        self.write_wait = None

    def send(self, sock, max_bytes=4096):
        """Attempt one write, retaining an unchanged payload for SSL retries."""
        if max_bytes <= 0:
            raise ValueError("max_bytes must be positive")
        data = self._retry_data
        if data is None:
            data = self._peek(max_bytes)
        if not data:
            return 0

        try:
            sent = sock.send(data)
        except (ssl.SSLWantReadError, ssl.SSLWantWriteError) as exc:
            if self._retry_data is None:
                self._retry_data = data
                self._protected = self._prefix_ids(len(data))
            self.write_wait = (
                "read" if isinstance(exc, ssl.SSLWantReadError) else "write"
            )
            raise
        except BlockingIOError:
            if self._retry_data is not None:
                self.write_wait = "write"
            raise

        if sent <= 0 or sent > len(data):
            raise ConnectionError("Socket send made no valid progress")

        self._retry_data = None
        self._protected.clear()
        self.write_wait = None
        self._remove(sent)
        self.progress_at = time.monotonic() if self else None
        return sent

    def _peek(self, max_bytes):
        if max_bytes <= 0:
            raise ValueError("max_bytes must be positive")
        if not self._chunks:
            return b""

        chunks = iter(self._chunks.values())
        head = memoryview(next(chunks)[0])[self._head_offset :]
        if len(head) >= max_bytes or len(self._chunks) == 1:
            return head[:max_bytes]

        parts = [head]
        remaining = max_bytes - len(head)
        for data, _, _ in chunks:
            part = memoryview(data)[:remaining]
            parts.append(part)
            remaining -= len(part)
            if remaining == 0:
                break
        return b"".join(parts)

    def _prefix_ids(self, count):
        protected = set()
        offset = self._head_offset
        for chunk_id, (data, _, _) in self._chunks.items():
            protected.add(chunk_id)
            count -= len(data) - offset
            if count <= 0:
                break
            offset = 0
        return protected

    def _change_retained_bytes(self, delta):
        self._retained_bytes += delta
        if delta and self._retained_change is not None:
            self._retained_change(delta)

    def _discard(self, chunk_id):
        data, key, _ = self._chunks.pop(chunk_id)
        self._change_retained_bytes(-len(data))
        if key is not None and self._by_key.get(key) == chunk_id:
            del self._by_key[key]

    def _remove(self, count):
        if count < 0 or count > self._size:
            raise ValueError("Cannot remove beyond buffered data")

        self._size -= count
        while count:
            chunk_id = next(iter(self._chunks))
            data, key, _ = self._chunks[chunk_id]
            available = len(data) - self._head_offset
            if count < available:
                self._head_offset += count
                if key is not None and self._by_key.get(key) == chunk_id:
                    del self._by_key[key]
                break

            count -= available
            self._discard(chunk_id)
            self._head_offset = 0

        if not self:
            self.progress_at = None
