# pylint: disable=missing-module-docstring
import time
import enum
import logging
from datetime import datetime as dt
from datetime import timedelta
from itertools import islice

from taky.config import app_config
from . import models
from .client import TAKClient, SocketTAKClient
from .persistence import build_persistence


def _audit_identifier(value):
    if value is None or len(value) <= 128:
        return value
    return value[:125] + "..."


class Destination(enum.Enum):
    """
    Indicate where this packet is routed
    """

    BROADCAST = 1
    GROUP = 2


class COTRouter:
    """
    A class to keep track of clients, and ensure packets get routed properly.
    """

    def __init__(self):
        # TODO: self.clients as dictionary, with UID as keys?
        #     : should prohibit multiple sockets sharing a client
        self.clients = set()
        self.persist = build_persistence()
        self.last_prune = 0
        self.max_ttl = app_config.getint("cot_server", "max_persist_ttl")
        self.lgr = logging.getLogger(self.__class__.__name__)

    def prune(self):
        now = time.time()
        if (now - self.last_prune) > 10:
            self.last_prune = now
            self.persist.prune()

    def client_connect(self, client):
        """
        Add a client to the router
        """
        self.clients.add(client)

    def client_disconnect(self, client):
        """
        Remove a client from the router
        """
        self.clients.discard(client)

    def send_persist(self, client):
        """
        Called by TAKClient when the client first identifies to the server
        """
        self.lgr.debug("Sending persistence objects to %s", client)
        if isinstance(client, SocketTAKClient):
            client.start_replay(self.persist)
            return
        for event in self.persist.get_all():
            if client.user and event.uid == client.user.uid:
                continue

            client.send_event(event)

    def find_clients(self, uid=None, callsign=None):
        """
        Returns an iterator of objects matching the criteria
        """
        for client in self.clients:
            if not client.user:
                continue

            if uid and client.user.uid == uid:
                yield client
            if callsign and client.user.callsign == callsign:
                yield client

    def broadcast(self, src, msg):
        """
        Broadcast a message from source to all clients
        """
        if src.user:
            self.lgr.debug("%s -> Broadcast: %s", src.user.callsign, msg)
        else:
            self.lgr.debug("Anonymous Broadcast: %s", msg)

        self.persist.track(msg)
        for client in self.clients:
            if client is src:
                continue

            client.send_event(msg)

    def group_broadcast(self, src, msg, group=None):
        """
        Broadcast a message from source to all members to a group.

        If group is not specified, the source's group is used.
        """
        if isinstance(src, TAKClient):
            src = src.user

        if group is None:
            if src is None:
                raise ValueError("Unable to determine group to send to")
            group = src.group

        if not isinstance(group, models.Teams):
            raise ValueError("group must be models.Teams")

        if src:
            self.lgr.debug("%s -> %s: %s", src.callsign, group, msg)
        else:
            self.lgr.debug("Anonymous -> %s: %s", group, msg)

        for client in self.clients:
            if not client.user or (client.user is src):
                continue

            if client.user.group == group:
                client.send_event(msg)

    def send_user(self, src, msg, dst_cs=None, dst_uid=None):
        """
        Send a message to a destination by callsign or UID
        """
        for client in self.find_clients(uid=dst_uid, callsign=dst_cs):
            client.send_event(msg)

    def _log_delete_request(self, src, evt, uids):
        if not self.lgr.isEnabledFor(logging.INFO):
            return

        user = src.user if src else None
        detail = evt.detail
        recipients = "broadcast=True"
        if isinstance(detail, models.GeoChat):
            if not detail.broadcast:
                if detail.dst_team:
                    recipients = "recipient_team=%r" % _audit_identifier(
                        detail.dst_team.value
                    )
                else:
                    recipients = "recipient_uids=%r" % [
                        _audit_identifier(detail.dst_uid)
                    ]
        elif detail is not None and detail.has_marti:
            callsigns = list(detail.marti_cs)
            recipients = "recipient_callsign_count=%d recipient_callsigns=%r" % (
                len(callsigns),
                [_audit_identifier(callsign) for callsign in callsigns[:10]],
            )
        self.lgr.info(
            "CoT delete request: uid=%r source_addr=%r source_uid=%r "
            "source_callsign=%r target_count=%d target_uids=%r %s",
            _audit_identifier(evt.uid),
            getattr(src, "addr", None),
            _audit_identifier(user.uid) if user else None,
            _audit_identifier(user.callsign) if user else None,
            len(uids),
            [_audit_identifier(uid) for uid in islice(uids, 10)],
            recipients,
        )

    def route(self, src, evt):
        """
        Push an event to the router
        """
        if not isinstance(evt, models.Event):
            raise ValueError(f"Unable to route {type(evt)}")

        # If configured, constrain events to a max TTL
        if self.max_ttl >= 0:
            if evt.persist_ttl > self.max_ttl:
                evt.stale = dt.utcnow() + timedelta(seconds=self.max_ttl)

        if evt.etype == "t-x-d-d":
            uids = set()
            detail = evt.detail.as_element if evt.detail is not None else None
            if detail is not None:
                uids = {
                    link.get("uid")
                    for link in detail.iterfind("link")
                    if link.get("uid")
                }
            self._log_delete_request(src, evt, uids)
            if uids:
                self.persist.remove_events(uids)

        # Special handling for chat messages
        if isinstance(evt.detail, models.GeoChat):
            chat = evt.detail
            if chat.broadcast:
                self.broadcast(src, evt)
            elif chat.dst_team:
                self.group_broadcast(src, evt, group=chat.dst_team)
            else:
                self.send_user(src, evt, dst_uid=chat.dst_uid)
            return

        # Check for Marti, use first
        if evt.detail and evt.detail.has_marti:
            destinations = list(evt.detail.marti_cs)
            source_callsign = src.user.callsign if src and src.user else None
            self.lgr.debug(
                "Handling marti: uid=%s source_callsign=%s destinations=%s",
                evt.uid,
                source_callsign,
                destinations,
            )
            for callsign in destinations:
                self.send_user(src, evt, dst_cs=callsign)
            return

        # Assume broadcast
        self.broadcast(src, evt)
