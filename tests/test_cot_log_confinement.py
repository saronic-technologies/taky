"""Native CoT routing and append regressions for untrusted identities."""

import os
import tempfile
import unittest
from pathlib import Path
from urllib.parse import quote

from lxml import etree

from taky.config import app_config, load_config
from taky.cot import TAKClient

from . import XML_S


class CoTLogConfinementTest(unittest.TestCase):
    def setUp(self):
        self.temp = tempfile.TemporaryDirectory()
        self.addCleanup(self.temp.cleanup)
        self.root = Path(self.temp.name)
        self.logs = self.root / "logs"
        self.logs.mkdir()
        self.outside = self.root / "outside-harmless-fixture.cot"
        self.before = b"CLAB031_INERT_PREFIX\n"
        self.outside.write_bytes(self.before)
        load_config(os.devnull)
        app_config.set("taky", "redis", "false")
        app_config.set("cot_server", "log_cot", str(self.logs))

    def submit(self, uid, callsign="fixture"):
        received = []
        client = TAKClient(cbs={"route": lambda _, event: received.append(event.uid)})
        elm = etree.fromstring(XML_S)
        elm.set("uid", uid)
        elm.find("detail/contact").set("callsign", callsign)
        etree.SubElement(elm.find("detail"), "remarks").text = "CLAB031_NATIVE_APPEND"
        try:
            client.feed(etree.tostring(elm))
        finally:
            client.close()
        self.assertEqual(received, [uid])

    def test_normal_logging_keeps_its_existing_name(self):
        self.submit("clab031-legitimate")
        self.assertIn(
            b"CLAB031_NATIVE_APPEND",
            (self.logs / "clab031-legitimate-fixture.cot").read_bytes(),
        )
        self.assertEqual(self.outside.read_bytes(), self.before)

    def test_absolute_and_relative_identifiers_stay_in_log_directory(self):
        for uid, callsign in [
            (str(self.root / "outside-harmless"), "fixture"),
            ("../outside-harmless", "fixture"),
            ("ordinary", "../../outside-harmless"),
        ]:
            with self.subTest(uid=uid, callsign=callsign):
                self.submit(uid, callsign)
                name = quote("{}-{}.cot".format(uid, callsign), safe="-_.")
                self.assertIn(b"CLAB031_NATIVE_APPEND", (self.logs / name).read_bytes())
                self.assertEqual(self.outside.read_bytes(), self.before)

    def test_existing_symlink_cannot_redirect_an_append(self):
        (self.logs / "symlink-fixture.cot").symlink_to(self.outside)
        self.submit("symlink")
        self.assertEqual(self.outside.read_bytes(), self.before)

    def test_disabled_logging_still_routes_without_appending(self):
        app_config.set("cot_server", "log_cot", None)
        self.submit(str(self.root / "outside-harmless"))
        self.assertEqual(self.outside.read_bytes(), self.before)
        self.assertEqual(list(self.logs.iterdir()), [])
