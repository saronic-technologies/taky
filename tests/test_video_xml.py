"""Native DPS regressions for video-feed XML processing."""

import tempfile
import unittest
from pathlib import Path

from taky.dps import app


class VideoXMLTest(unittest.TestCase):
    def setUp(self):
        self.temp = tempfile.TemporaryDirectory()
        self.addCleanup(self.temp.cleanup)
        self.root = Path(self.temp.name)
        self.sentinel = self.root / "sentinel.txt"
        self.sentinel.write_text("CLAB030_HARMLESS_SENTINEL", encoding="utf8")
        original_path = app.config["UPLOAD_PATH"]
        app.config["UPLOAD_PATH"] = str(self.root)
        self.addCleanup(app.config.__setitem__, "UPLOAD_PATH", original_path)
        self.client = app.test_client()
        self.headers = {"X-USER": "admitted-test-client"}

    def post(self, value):
        return self.client.post(
            "/Marti/vcm",
            data=value,
            headers=self.headers,
            content_type="application/xml",
        )

    def benign(self):
        value = (
            "<videoConnections><feed><uid>legitimate</uid>"
            "<notes>normal &amp; useful</notes></feed></videoConnections>"
        )
        self.assertEqual(self.post(value).status_code, 200)

    def test_video_upload_rejects_external_entities_and_preserves_normal_feeds(self):
        self.assertEqual(self.client.get("/Marti/vcm").status_code, 401)
        self.benign()
        attack = (
            '<?xml version="1.0"?><!DOCTYPE videoConnections ['
            '<!ENTITY proof SYSTEM "' + self.sentinel.as_uri() + '">]>'
            "<videoConnections><feed><uid>clab030-candidate</uid>"
            "<notes>&proof;</notes></feed></videoConnections>"
        )
        self.assertEqual(self.post(attack).status_code, 400)
        self.assertFalse((self.root / "video_feed/clab030-candidate.xml").exists())
        response = self.client.get("/Marti/vcm", headers=self.headers)
        self.assertEqual(response.status_code, 200)
        self.assertIn(b"normal &amp; useful", response.data)
        self.assertNotIn(self.sentinel.read_bytes(), response.data)

    def test_video_index_ignores_legacy_files_with_external_entities(self):
        self.benign()
        stored = (
            '<!DOCTYPE feed [<!ENTITY proof SYSTEM "'
            + self.sentinel.as_uri()
            + '">]><feed><uid>legacy</uid><notes>&proof;</notes></feed>'
        )
        (self.root / "video_feed/legacy.xml").write_text(stored, encoding="utf8")
        response = self.client.get("/Marti/vcm", headers=self.headers)
        self.assertEqual(response.status_code, 200)
        self.assertIn(b"normal &amp; useful", response.data)
        self.assertNotIn(self.sentinel.read_bytes(), response.data)
