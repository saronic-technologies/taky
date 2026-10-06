# pylint: disable=missing-module-docstring
import os
import time
import enum
from collections import OrderedDict
from datetime import datetime as dt
from datetime import timedelta
import socket
import ssl
import logging
import traceback

from lxml import etree

from taky.config import app_config
from taky.util import XMLDeclStrip
from . import models
from .output import OutputBuffer


class OutputBudget:
    """Limits retained output payloads across the server's socket clients."""

    def __init__(self, limit):
        self.limit = limit
        self.clients = set()
        self._retained_bytes = 0

    @property
    def retained_bytes(self):
        return self._retained_bytes

    def adjust_retained_bytes(self, delta):
        """Account for backing storage added or released by an output buffer."""
        self._retained_bytes += delta

    def make_room(self, client, size):
        if size > self.limit:
            client.disconnect("Event exceeds total output byte limit")
            return False
        while self.retained_bytes + size > self.limit:
            victim = max(self.clients, key=lambda item: item.out_buff.retained_bytes)
            victim.disconnect("Total output byte limit exceeded")
            if victim is client:
                return False
        return True


POSITION_DETAIL_TAGS = {
    "takv",
    "contact",
    "uid",
    "precisionlocation",
    "__group",
    "status",
    "track",
}


def position_key(event):
    """Recognize ordinary participant updates without command extensions."""
    if (
        event.uid
        and event.etype
        and event.etype.startswith("a-")
        and isinstance(event.detail, models.TAKUser)
        and event.detail.elm is not None
        and all(
            child.tag in POSITION_DETAIL_TAGS and len(child) == 0
            for child in event.detail.elm
        )
    ):
        return (event.uid, event.etype)
    return None


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
        self.output_limit = app_config.getint(
            "cot_server", "output_client_bytes", fallback=4 * 1024 * 1024
        )
        self.output_stall_seconds = app_config.getint(
            "cot_server", "output_stall_seconds", fallback=60
        )
        self.output_budget = kwargs.get("output_budget")
        if self.output_budget is None:
            self.output_budget = OutputBudget(
                app_config.getint(
                    "cot_server", "output_total_bytes", fallback=64 * 1024 * 1024
                )
            )
        self.out_buff = OutputBuffer(
            retained_change=self.output_budget.adjust_retained_bytes
        )
        self.output_budget.clients.add(self)
        self._closed = False
        self._rx_wait_write = False
        self.connect_cb = kwargs.get("cbs", {}).get("connect", lambda client: None)

        ip, port = self.addr
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
        return self._closed or self.sock.fileno() == -1

    @property
    def has_data(self):
        """
        Returns true if the socket wants to be considered for transmitting
        """
        return (
            (bool(self.out_buff) and self.out_buff.write_wait != "read")
            or self.ssl_hs == SSLState.SSL_WAIT_TX
            or self._rx_wait_write
        )

    @property
    def wants_read(self):
        return not self._rx_wait_write and self.out_buff.write_wait != "write"

    @property
    def output_stalled_seconds(self):
        if not self.out_buff or self.out_buff.progress_at is None:
            return 0
        return max(0, time.monotonic() - self.out_buff.progress_at)

    def enqueue(self, data, key=None, timestamp=None, replay=False):
        """Queue bytes, replacing only recognized, entirely unsent updates."""
        if self.is_closed:
            return False
        if not self.out_buff.remove_superseded(key, timestamp):
            return False
        if self.out_buff.retained_bytes + len(data) > self.output_limit:
            if not replay:
                self.disconnect("Client output byte limit exceeded")
            return False
        if replay:
            if self.output_budget.retained_bytes + len(data) > self.output_budget.limit:
                return False
        elif not self.output_budget.make_room(self, len(data)):
            return False
        self.out_buff.append(data, key=key, timestamp=timestamp)
        return True

    def check_output_timeout(self, now=None):
        if not self.out_buff or self.out_buff.progress_at is None:
            return
        if now is None:
            now = time.monotonic()
        if now - self.out_buff.progress_at >= self.output_stall_seconds:
            self.disconnect("Output made no progress before timeout")

    def __repr__(self):
        ip, port = self.addr[0:2]
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

        if self.out_buff.write_wait == "read":
            self.socket_tx()
            return

        try:
            data = self.sock.recv(4096)
            self._rx_wait_write = False

            if len(data) == 0:
                self.disconnect("Client disconnected")
                return

            self.feed(data)
        except etree.XMLSyntaxError as exc:
            self.disconnect("XML Syntax Error")
            self.lgr.debug("XML Syntax Error: %s", self, exc_info=exc)
        except ssl.SSLWantReadError:
            self._rx_wait_write = False
        except ssl.SSLWantWriteError:
            self._rx_wait_write = True
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

        if self._rx_wait_write:
            self.socket_rx()
            return
        if not self.out_buff:
            return

        try:
            self.out_buff.send(self.sock)
        except (ssl.SSLWantReadError, ssl.SSLWantWriteError):
            pass
        except BlockingIOError:
            self.lgr.debug("Client blocked TX: %s", self)
        except (ssl.SSLError, socket.error, IOError, OSError) as exc:
            self.disconnect(str(exc))

    def disconnect(self, reason=None):
        if self._closed:
            return
        self.lgr.info(
            "Socket disconnect: %s (pending=%d retained=%d stalled=%.1fs)",
            reason,
            len(self.out_buff),
            self.out_buff.retained_bytes,
            self.output_stalled_seconds,
        )
        self._closed = True
        self.out_buff.clear()
        self.output_budget.clients.discard(self)
        self._rx_wait_write = False
        self.close()

        try:
            self.sock.shutdown(socket.SHUT_RDWR)
        except:  # pylint: disable=bare-except
            pass
        finally:
            self.sock.close()

    def close(self):
        """Release subclass state when the socket closes."""


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
            self.close_cot()
            self.log_cot_dir = None

    def feed(self, data):
        """
        Feed the XML data parser with COT data
        """
        # TODO: Specify maximum element size
        self.xdc.feed(data)

        for _, elm in self.xdc.read_events():
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
        self.replay_pending = OrderedDict()
        self.replay_store = None
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

        if not self.ready or self.is_closed:
            return False

        # A live update supersedes the saved event still waiting for replay.
        self.replay_pending.pop(event.uid, None)
        return self.enqueue(
            etree.tostring(event.as_element), position_key(event), event.time
        )

    def start_replay(self, persistence):
        self.replay_store = persistence
        self.replay_pending = OrderedDict.fromkeys(persistence.get_uids())
        self.pump_replay()

    def pump_replay(self):
        """Queue one saved event when the preceding output has drained."""
        if self.is_closed or not self.ready or self.out_buff:
            return
        for _ in range(64):
            if not self.replay_pending:
                self.replay_store = None
                return
            uid = next(iter(self.replay_pending))
            event = self.replay_store.get_event(uid)
            if event is None or (self.user and uid == self.user.uid):
                self.replay_pending.pop(uid)
                continue
            data = etree.tostring(event.as_element)
            if len(data) > min(self.output_limit, self.output_budget.limit):
                self.lgr.warning(
                    "Skipping saved event %s: exceeds output byte limit", uid
                )
                self.replay_pending.pop(uid)
                continue
            if self.enqueue(data, position_key(event), event.time, replay=True):
                self.replay_pending.pop(uid)
            return

    def close(self):
        self.replay_pending.clear()
        self.replay_store = None
        TAKClient.close(self)
