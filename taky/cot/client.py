# pylint: disable=missing-module-docstring
import os
import time
import enum
from collections import deque
from datetime import datetime as dt
from datetime import timedelta
from itertools import islice
import socket
import ssl
import logging
import traceback

from lxml import etree

from taky.config import app_config
from taky.util import XMLDeclStrip
from . import models


class OutputBuffer:
    """Buffers immutable bytes for non-blocking socket writes."""

    def __init__(self):
        self._chunks = deque()
        self._head_offset = 0
        self._size = 0

    def __bool__(self):
        return self._size > 0

    def __len__(self):
        return self._size

    def append(self, data):
        """Append immutable bytes to the buffer."""
        if not isinstance(data, bytes):
            raise TypeError("OutputBuffer accepts bytes")

        if data:
            self._chunks.append(data)
            self._size += len(data)

    def send(self, sock, max_bytes=4096):
        """Send queued bytes once and remove the bytes written."""
        data = self._peek(max_bytes)
        if not data:
            return 0

        sent = sock.send(data)
        if sent == 0:
            raise ConnectionError("Socket send made no progress")

        self._remove(sent)
        return sent

    def _peek(self, max_bytes):
        if max_bytes <= 0:
            raise ValueError("max_bytes must be positive")

        if not self._chunks:
            return b""

        head = memoryview(self._chunks[0])[self._head_offset :]
        if len(head) >= max_bytes or len(self._chunks) == 1:
            return head[:max_bytes]

        parts = [head]
        remaining = max_bytes - len(head)
        for chunk in islice(self._chunks, 1, None):
            part = memoryview(chunk)[:remaining]
            parts.append(part)
            remaining -= len(part)
            if remaining == 0:
                break

        return b"".join(parts)

    def _remove(self, count):
        if count < 0 or count > self._size:
            raise ValueError("Cannot remove beyond buffered data")

        self._size -= count
        while count:
            available = len(self._chunks[0]) - self._head_offset
            if count < available:
                self._head_offset += count
                return

            count -= available
            self._chunks.popleft()
            self._head_offset = 0


class SSLState(enum.Enum):
    """Tracks SSL state"""

    NO_SSL = 0
    SSL_WAIT = 1
    SSL_WAIT_TX = 2
    SSL_ESTAB = 4


class SocketClient:
    """
    A class to simplify tracking connection details for a select() based
    server, such as SSL handshake state, and an outgoing data buffer.
    """

    def __init__(self, sock, use_ssl=False, **kwargs):
        self.sock = sock
        self.ssl = use_ssl
        self.peer_cert = None
        self.ssl_hs = SSLState.SSL_WAIT if use_ssl else SSLState.NO_SSL
        self.out_buff = OutputBuffer()
        self.connect_cb = kwargs.get("cbs", {}).get("connect", lambda client: None)

        (ip, port) = self.addr
        lgr_name = f"{self.__class__.__name__}@{ip}:{port}"
        self.lgr = logging.getLogger(lgr_name)

        if self.ready:
            self.connect_cb(self)

    @property
    def addr(self):
        try:
            addr = self.sock.getpeername()
            if addr == "":
                return ("unix", "")
            return addr
        except:  # pylint: disable=bare-except
            return (None, None)

    @property
    def ready(self):
        if not self.ssl:
            return True

        return self.ssl_hs in [SSLState.NO_SSL, SSLState.SSL_ESTAB]

    @property
    def is_closed(self):
        """Returns true if the socket is closed"""
        return self.sock.fileno() == -1

    @property
    def has_data(self):
        """
        Returns true if the socket wants to be considered for transmitting
        """
        return len(self.out_buff) > 0 or self.ssl_hs == SSLState.SSL_WAIT_TX

    def __repr__(self):
        (ip, port) = self.addr[0:2]
        return f"<{self.__class__.__name__} addr={ip}:{port} ssl={self.ssl}>"

    def feed(self, data):
        """
        Implemented in a subclass to handle reception of data
        """
        raise NotImplementedError()

    def ssl_handshake(self):
        """Preform the SSL handshake on the socket"""
        if self.ready:
            return

        try:
            self.sock.do_handshake()
            self.ssl_hs = SSLState.SSL_ESTAB
            self.peer_cert = self.sock.getpeercert()
            self.connect_cb(self)
        except ssl.SSLWantReadError:
            self.ssl_hs = SSLState.SSL_WAIT
        except ssl.SSLWantWriteError:
            self.ssl_hs = SSLState.SSL_WAIT_TX
        except (ssl.SSLError, socket.error, IOError, OSError) as exc:
            self.disconnect(str(exc))

    def socket_rx(self):
        """
        Call this whenever a socket indicates it has data to receive.

        If the socket is SSL based, this may be part of the handshake.
        """
        if self.ssl and not self.ready:
            self.ssl_handshake()
            return

        try:
            data = self.sock.recv(4096)

            if len(data) == 0:
                self.disconnect("Client disconnected")
                return

            self.feed(data)
        except etree.XMLSyntaxError as exc:
            self.disconnect("XML Syntax Error")
            self.lgr.debug("XML Syntax Error: %s", self, exc_info=exc)
        except BlockingIOError:
            self.lgr.debug("Client blocked RX: %s", self)
        except (ssl.SSLError, socket.error, IOError, OSError) as exc:
            self.disconnect(str(exc))

    def socket_tx(self):
        """
        Transmit data to client socket. (Check has_data to see if this needs
        to be called.)

        If the client is SSL enabled, and the handshake has not yet taken
        place, we fail silently.
        """
        if self.ssl and not self.ready:
            self.ssl_handshake()
            return

        if not self.out_buff:
            return

        try:
            self.out_buff.send(self.sock)
        except BlockingIOError:
            self.lgr.debug("Client blocked TX: %s", self)
        except (ssl.SSLError, socket.error, IOError, OSError) as exc:
            self.disconnect(str(exc))

    def disconnect(self, reason=None):
        if not self.is_closed:
            self.lgr.info("Socket disconnect: %s", reason)

        try:
            self.sock.shutdown(socket.SHUT_RDWR)
        except:  # pylint: disable=bare-except
            pass
        finally:
            self.sock.close()


class TAKClient:
    """
    Holds state and information regarding a client connected to the TAK server.
    This object is designed to be somewhat agnostic as to HOW the client
    connected, and instead only focuses on what the server needs to know about
    the client.
    """

    def __init__(self, monitor=False, **kwargs):
        self.monitor = monitor
        self.user = None
        self.connected = time.time()
        self.num_rx = 0
        self.last_rx = 0

        cbs = kwargs.get("cbs", {})
        self.route = cbs.get("route", lambda client, pkt: None)
        self.packet_rx = cbs.get("packet_rx", lambda pkt: None)
        self.client_ident = cbs.get("client_ident", lambda pkt: None)

        self.log_cot_dir = app_config.get("cot_server", "log_cot")
        self.cot_fp = None

        parser = etree.XMLPullParser(tag="event", resolve_entities=False)
        parser.feed(b"<root>")
        self.xdc = XMLDeclStrip(parser)

        self.lgr = logging.getLogger(self.__class__.__name__)

    def __repr__(self):
        if self.user:
            return f"<TAKClient uid={self.user.uid} callsign={self.user.callsign}>"

        return "<TAKClient uid=None callsign=None>"

    def send_event(self, event):
        """
        Send a CoT event to the client.

        @param event A CoT Event object
        """
        raise NotImplementedError()

    def close(self):
        self.close_cot()

    def close_cot(self):
        """Close the COT log"""
        if self.cot_fp:
            try:
                self.cot_fp.close()
            except:  # pylint: disable=bare-except
                pass
            self.cot_fp = None

    def log_event(self, evt=None, elm=None, _exc=None):
        """
        Writes the COT XML to the logfile, if configured.

        @param evt The COT Event to log
        """
        # Skip if we're not configured to log
        if not self.log_cot_dir:
            return
        if evt is None and elm is None:
            return
        # Skip logging of pings
        if evt and evt.uid and evt.uid.endswith("-ping"):
            return

        # Open the COT file if it's the first run
        if not self.cot_fp:
            # Don't log if we don't have a user yet
            if self.user and self.user.uid:
                name = os.path.join(
                    self.log_cot_dir, f"{self.user.uid}-{self.user.callsign}.cot"
                )
            elif hasattr(self, "addr"):
                name = "monitor" if self.monitor else "anonymous"
                name = os.path.join(self.log_cot_dir, f"{name}-{self.addr[0]}.cot")
            else:
                # Don't have a way to determine log file name!
                return

            try:
                self.lgr.debug("Opening logfile %s", name)
                self.cot_fp = open(name, "a+", encoding="utf8")
            except OSError as exc:
                self.lgr.warning("Unable to open COT log: %s", exc)
                self.cot_fp = None
                self.log_cot_dir = None
                return

        try:
            if elm is None:
                elm = evt.as_element

            if _exc:
                taky_err = etree.Element("__taky_err")
                taky_err.append(etree.Comment(_exc))
                elm.append(taky_err)

            doc = etree.tostring(elm, pretty_print=True).decode()
        except Exception as exc:  # pylint: disable=broad-except
            self.lgr.warning("Unable to build packet string for logfile", exc_info=exc)
            return

        try:
            self.cot_fp.write(doc)
            self.cot_fp.flush()
        except (IOError, OSError) as exc:
            self.lgr.warning("Unable to write to COT log: %s", exc)
            self.close()
            self.log_cot_dir = None

    def feed(self, data):
        """
        Feed the XML data parser with COT data
        """
        # TODO: Specify maximum element size
        self.xdc.feed(data)

        for (_, elm) in self.xdc.read_events():
            self.num_rx += 1
            self.last_rx = time.time()
            try:
                evt = models.Event.from_elm(elm)
                self.packet_rx(evt)

                if not evt.etype:
                    continue

                if evt.etype == "t-x-c-t":
                    self.pong()
                    continue

                if evt.etype.startswith("a"):
                    self.handle_atom(evt)

                self.route(self, evt)
                self.log_event(evt)
            except models.UnmarshalError as exc:
                self.lgr.debug("Unable to parse Event: %s", exc, exc_info=exc)
                self.lgr.debug(etree.tostring(elm, pretty_print=True))
                self.log_event(elm=elm, _exc=traceback.format_exc())
                continue
            except Exception as exc:  # pylint: disable=broad-except
                self.lgr.error(
                    "Unhandled exception parsing Event: %s", exc, exc_info=exc
                )
                self.lgr.error(etree.tostring(elm, pretty_print=True))
                self.log_event(elm=elm, _exc=traceback.format_exc())
                continue
            finally:
                elm.clear(keep_tail=True)
                while elm.getprevious() is not None:
                    del elm.getparent()[0]

    def handle_atom(self, evt):
        """
        Process a COT atom.

        Inspects the Event to see if it is a self description, and if so,
        informs the router a client has identified itself.
        """
        if self.monitor:
            return

        if evt.detail is None:
            return

        if isinstance(evt.detail, models.TAKUser):
            if self.user is None:
                self.user = evt.detail
                # Try to close the COT (ie: anonymous log)
                self.close_cot()
                self.client_ident(self)
            else:
                self.user = evt.detail

    def pong(self):
        """
        Generate and send a TAK pong. Clients that do not receive a pong in
        an appropriate amount of time will disconnect.
        """
        now = dt.utcnow()
        pong = models.Event(
            uid="takPong",
            etype="t-x-c-t-r",
            how="h-g-i-g-o",
            time=now,
            start=now,
            stale=now + timedelta(seconds=20),
        )
        self.send_event(pong)


class SocketTAKClient(TAKClient, SocketClient):
    """
    A TAK client based on sockets
    """

    def __init__(self, **kwargs):
        TAKClient.__init__(self, **kwargs)
        SocketClient.__init__(self, **kwargs)

    def __repr__(self):
        if self.user:
            return (
                f"<SocketTAKClient uid={self.user.uid} "
                f"callsign={self.user.callsign} "
                f"addr={self.addr[0]}:{self.addr[1]}>"
            )

        return (
            f"<SocketTAKClient uid=None "
            f"callsign=None "
            f"addr={self.addr[0]}:{self.addr[1]}>"
        )

    def send_event(self, event):
        """
        Send a CoT event to the client.

        @param event A CoT Event object
        """
        if not isinstance(event, models.Event):
            raise TypeError("Must send a COTEvent")

        # Silently drop data if the SSL handshake is not ready yet
        if not self.ready:
            return

        self.out_buff.append(etree.tostring(event.as_element))
