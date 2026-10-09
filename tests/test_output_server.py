import os
import tempfile
import unittest as ut
from unittest import mock

from taky.config import app_config, load_config
from taky.cot.client import SocketTAKClient
from taky.cot.mgmt import MgmtClient
from taky.cot.server import COTServer


class OutputServerTest(ut.TestCase):
    def setUp(self):
        load_config(os.devnull)
        app_config.set("taky", "redis", "false")
        with mock.patch("taky.cot.server.anc.CertificateDatabase"):
            self.server = COTServer()
        self.server.srv = mock.Mock()
        self.addCleanup(self.server.shutdown)

    def client(self):
        sock = mock.Mock()
        sock.getpeername.return_value = ("127.0.0.1", 12345)
        sock.fileno.return_value = 1
        client = SocketTAKClient(sock=sock, output_budget=self.server.output_budget)
        self.server.clients[sock] = client
        self.server.router.client_connect(client)
        return client

    def test_timeout_is_checked_before_select_without_writable_notification(self):
        client = self.client()
        client.enqueue(b"pending")
        expired = client.out_buff.progress_at + client.output_stall_seconds
        with mock.patch("time.monotonic", return_value=expired), mock.patch(
            "taky.cot.server.select.select", return_value=([], [], [])
        ) as select:
            self.server.loop()
        self.assertTrue(client.is_closed)
        self.assertNotIn(client.sock, self.server.clients)
        self.assertNotIn(client, self.server.router.clients)
        for sockets in select.call_args[0][:3]:
            self.assertNotIn(client.sock, sockets)
        self.assertEqual(self.server.output_budget.retained_bytes, 0)

    def test_pressure_does_not_change_collections_during_iteration(self):
        clients = [self.client() for _ in range(3)]
        self.server.output_budget.limit = 9
        clients[0].enqueue(b"123456")
        for client in self.server.router.clients:
            client.enqueue(b"abcd")
        self.assertEqual(len(self.server.router.clients), 3)
        self.assertTrue(any(client.is_closed for client in clients))
        self.assertLessEqual(self.server.output_budget.retained_bytes, 9)
        self.server.service_clients()
        self.assertTrue(
            all(not client.is_closed for client in self.server.clients.values())
        )

    def test_management_socket_is_nonblocking_and_shares_budget(self):
        sock = mock.Mock()
        sock.getpeername.return_value = ""
        self.server.mgmt = mock.Mock()
        self.server.mgmt.accept.return_value = (sock, "")
        self.server.mgmt_accept()
        sock.setblocking.assert_called_once_with(False)
        client = self.server.clients[sock]
        self.assertIs(client.output_budget, self.server.output_budget)
        self.server.mgmt = None

    def test_management_readiness_has_no_parsing_side_effect(self):
        sock = mock.Mock()
        sock.getpeername.return_value = ""
        client = MgmtClient(sock=sock, server=self.server)
        self.addCleanup(client.disconnect)
        client.output_limit = 20
        client.feed(b'{"cmd":"ping"}\0{"cmd":"ping"}\0{"cmd":"ping"}\0')
        self.assertTrue(client.is_closed)
        self.assertEqual(client.buff, b"")
        self.assertFalse(client.has_data)
        self.assertEqual(client.out_buff.retained_bytes, 0)

    def test_management_status_includes_anonymous_output_and_shared_usage(self):
        client = self.client()
        client.enqueue(b"queued")
        client.sock.send.return_value = 2
        client.socket_tx()
        sock = mock.Mock()
        sock.getpeername.return_value = ""
        mgmt = MgmtClient(sock=sock, server=self.server)
        self.addCleanup(mgmt.disconnect)
        result = mgmt.status()
        self.assertEqual(result["output_retained_bytes"], 6)
        info = result["clients"][0]
        self.assertTrue(info["anonymous"])
        self.assertEqual(info["ip"], "127.0.0.1")
        self.assertEqual(info["port"], 12345)
        self.assertEqual(info["output_pending_bytes"], 4)
        self.assertEqual(info["output_retained_bytes"], 6)
        self.assertGreaterEqual(info["output_stalled_seconds"], 0)

    def test_log_failure_does_not_cancel_pending_replay(self):
        client = self.client()
        client.replay_pending["saved"] = None
        client.log_cot_dir = "/unused"
        client.cot_fp = mock.Mock()
        client.cot_fp.write.side_effect = OSError("log full")
        from lxml import etree

        with mock.patch.object(client.lgr, "warning"):
            client.log_event(elm=etree.Element("event"))
        self.assertIn("saved", client.replay_pending)
        self.assertFalse(client.is_closed)
        self.assertIsNone(client.cot_fp)


class OutputConfigTest(ut.TestCase):
    def test_existing_configs_get_defaults(self):
        load_config(os.devnull)
        self.assertEqual(
            app_config.getint("cot_server", "output_client_bytes"), 4194304
        )
        self.assertEqual(
            app_config.getint("cot_server", "output_total_bytes"), 67108864
        )
        self.assertEqual(app_config.getint("cot_server", "output_stall_seconds"), 60)

    def test_limits_require_positive_integers(self):
        for name in (
            "output_client_bytes",
            "output_total_bytes",
            "output_stall_seconds",
        ):
            for value in ("0", "-1", "1.5", "none", ""):
                with self.subTest(name=name, value=value):
                    with tempfile.NamedTemporaryFile(
                        mode="w", suffix=".conf"
                    ) as config:
                        config.write("[cot_server]\n{}={}\n".format(name, value))
                        config.flush()
                        with self.assertRaisesRegex(ValueError, name):
                            load_config(config.name)
