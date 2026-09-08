"""Tests for hivemind_bus_client.client — BinaryDataCallbacks, Waiters, HiveMessageBusClient."""
import json
import ssl
import unittest
from threading import Event
from unittest.mock import MagicMock, patch, PropertyMock

from ovos_bus_client.message import Message

from hivemind_bus_client.client import (
    BinaryDataCallbacks,
    HiveMessageWaiter,
    HivePayloadWaiter,
    HiveMessageBusClient,
)
from hivemind_bus_client.message import HiveMessage, HiveMessageType, HiveMindBinaryPayloadType


class TestBinaryDataCallbacks(unittest.TestCase):
    def test_handle_receive_tts_does_not_raise(self):
        cb = BinaryDataCallbacks()
        cb.handle_receive_tts(b"\x00\x01", "hello", "en", "test.wav")

    def test_handle_receive_file_does_not_raise(self):
        cb = BinaryDataCallbacks()
        cb.handle_receive_file(b"\x00\x01", "test.bin")


class TestHiveMessageWaiter(unittest.TestCase):
    def test_init_registers_handler(self):
        bus = MagicMock()
        waiter = HiveMessageWaiter(bus, HiveMessageType.PING)
        bus.on.assert_called_once_with(HiveMessageType.PING, waiter._handler)

    def test_handler_sets_event(self):
        bus = MagicMock()
        waiter = HiveMessageWaiter(bus, HiveMessageType.PING)
        msg = HiveMessage(HiveMessageType.PING, {"flood_id": "x"})
        waiter._handler(msg)
        self.assertIs(waiter.received_msg, msg)
        self.assertTrue(waiter.response_event.is_set())

    def test_wait_returns_message_and_removes_handler(self):
        bus = MagicMock()
        waiter = HiveMessageWaiter(bus, HiveMessageType.PING)
        msg = HiveMessage(HiveMessageType.PING, {"flood_id": "x"})
        waiter._handler(msg)
        result = waiter.wait(timeout=0.1)
        self.assertIs(result, msg)
        bus.remove.assert_called_once_with(HiveMessageType.PING, waiter._handler)

    def test_wait_returns_none_on_timeout(self):
        bus = MagicMock()
        waiter = HiveMessageWaiter(bus, HiveMessageType.PING)
        result = waiter.wait(timeout=0.01)
        self.assertIsNone(result)


class TestHivePayloadWaiter(unittest.TestCase):
    def test_filters_by_payload_type(self):
        bus = MagicMock()
        waiter = HivePayloadWaiter(bus, payload_type="speak", message_type=HiveMessageType.BUS)

        # Matching payload
        inner_msg = Message("speak", {"utterance": "hi"})
        outer = HiveMessage(HiveMessageType.BUS, inner_msg)
        waiter._handler(outer)
        self.assertIsNotNone(waiter.received_msg)

    def test_ignores_non_matching_payload(self):
        bus = MagicMock()
        waiter = HivePayloadWaiter(bus, payload_type="speak", message_type=HiveMessageType.BUS)

        inner_msg = Message("recognizer_loop:utterance", {"utterances": ["hi"]})
        outer = HiveMessage(HiveMessageType.BUS, inner_msg)
        waiter._handler(outer)
        self.assertIsNone(waiter.received_msg)


def _make_client(**kwargs):
    """Create HiveMessageBusClient without actually connecting or starting threads."""
    from pyee import EventEmitter
    from ovos_utils.fakebus import FakeBus

    defaults = {
        "key": "test-key",
        "password": "test-pass",
        "port": 5678,
        "host": "ws://localhost",
        "internal_bus": FakeBus(),
        "websocket_ping_interval": None,
        "websocket_ping_timeout": None,
    }
    defaults.update(kwargs)

    # Bypass OVOSBusClient.__init__ which starts websocket threads
    client = object.__new__(HiveMessageBusClient)
    client.bin_callbacks = defaults.pop("bin_callbacks", BinaryDataCallbacks())
    from hivemind_bus_client.encryption import SupportedEncodings, SupportedCiphers
    client.json_encoding = SupportedEncodings.JSON_HEX
    client.cipher = SupportedCiphers.AES_GCM
    client.identity = None
    client._password = defaults["password"]
    client._access_key = defaults["key"]
    client._name = defaults.get("useragent", "")
    client._port = defaults["port"]
    client._host = defaults["host"]
    client.init_identity()
    client.crypto_key = defaults.get("crypto_key")
    client.allow_self_signed = True
    client.share_bus = False
    client.handshake_event = Event()
    client.websocket_ping_interval = defaults["websocket_ping_interval"]
    client.websocket_ping_timeout = defaults["websocket_ping_timeout"]
    client.compress = True
    client.binarize = True
    client.internal_bus = defaults["internal_bus"]
    client.emitter = MagicMock(spec=EventEmitter)
    client.connected_event = Event()
    client.started_running = False
    client.session_id = "test-session-id"
    client.client = MagicMock()
    client.retry = 5
    return client


class TestHiveMessageBusClientProperties(unittest.TestCase):
    def test_useragent(self):
        client = _make_client(useragent="my-agent")
        self.assertEqual(client.useragent, "my-agent")

    def test_useragent_setter(self):
        client = _make_client()
        client.useragent = "new-agent"
        self.assertEqual(client.useragent, "new-agent")

    def test_password_property(self):
        client = _make_client()
        self.assertEqual(client.password, "test-pass")

    def test_key_property(self):
        client = _make_client()
        self.assertEqual(client.key, "test-key")

    def test_site_id_property(self):
        client = _make_client()
        client.site_id = "kitchen"
        self.assertEqual(client.site_id, "kitchen")

    def test_password_setter(self):
        client = _make_client()
        client.password = "new-pass"
        self.assertEqual(client.password, "new-pass")

    def test_key_setter(self):
        client = _make_client()
        client.key = "new-key"
        self.assertEqual(client.key, "new-key")


class TestHiveMessageBusClientOnError(unittest.TestCase):
    @patch("ovos_bus_client.client.client.MessageBusClient.on_error")
    def test_on_error_clears_handshake(self, mock_super_error):
        client = _make_client()
        client.connected_event.set()
        client.handshake_event.set()
        client.crypto_key = "some-key"
        client.on_error(Exception("test"))
        self.assertFalse(client.connected_event.is_set())
        self.assertFalse(client.handshake_event.is_set())
        self.assertIsNone(client.crypto_key)

    def test_on_close_clears_handshake(self):
        client = _make_client()
        client.connected_event.set()
        client.handshake_event.set()
        client.crypto_key = "some-key"
        client.on_close()
        self.assertFalse(client.connected_event.is_set())
        self.assertFalse(client.handshake_event.is_set())
        self.assertIsNone(client.crypto_key)


class TestHiveMessageBusClientKeepalive(unittest.TestCase):
    def test_keepalive_options_default(self):
        client = _make_client()
        self.assertEqual(
            client._websocket_keepalive_options(),
            {"ping_interval": 25.0, "ping_timeout": 10.0},
        )

    def test_keepalive_options_explicit(self):
        client = _make_client(websocket_ping_interval=15, websocket_ping_timeout=5)
        self.assertEqual(
            client._websocket_keepalive_options(),
            {"ping_interval": 15.0, "ping_timeout": 5.0},
        )

    def test_keepalive_options_from_env(self):
        client = _make_client()
        with patch.dict(
            "os.environ",
            {
                "HIVEMIND_WEBSOCKET_CLIENT_PING_INTERVAL": "30",
                "HIVEMIND_WEBSOCKET_CLIENT_PING_TIMEOUT": "12",
            },
        ):
            self.assertEqual(
                client._websocket_keepalive_options(),
                {"ping_interval": 30.0, "ping_timeout": 12.0},
            )

    def test_keepalive_options_disable_interval(self):
        client = _make_client(websocket_ping_interval=0)
        self.assertEqual(client._websocket_keepalive_options(), {"ping_interval": 0})

    def test_keepalive_options_adjusts_timeout(self):
        client = _make_client(websocket_ping_interval=10, websocket_ping_timeout=10)
        self.assertEqual(
            client._websocket_keepalive_options(),
            {"ping_interval": 10.0, "ping_timeout": 5.0},
        )

    def test_run_forever_passes_keepalive(self):
        client = _make_client()
        client.allow_self_signed = False
        client.run_forever()
        client.client.run_forever.assert_called_once_with(
            ping_interval=25.0, ping_timeout=10.0
        )

    def test_run_forever_passes_keepalive_and_ssl_options(self):
        client = _make_client()
        client.run_forever()
        kwargs = client.client.run_forever.call_args.kwargs
        self.assertEqual(kwargs["ping_interval"], 25.0)
        self.assertEqual(kwargs["ping_timeout"], 10.0)
        self.assertEqual(kwargs["sslopt"]["cert_reqs"], ssl.CERT_NONE)


class TestHiveMessageBusClientOn(unittest.TestCase):
    def test_on_mycroft_event_registers_on_internal_bus(self):
        client = _make_client()
        client.internal_bus = MagicMock()
        fn = MagicMock()
        client.on("speak", fn)
        client.internal_bus.on.assert_called_with("speak", fn)

    def test_on_hive_event_registers_on_emitter(self):
        client = _make_client()
        fn = MagicMock()
        client.on(HiveMessageType.PING, fn)
        client.emitter.on.assert_called_with(HiveMessageType.PING, fn)

    def test_remove_mycroft_event(self):
        client = _make_client()
        client.internal_bus = MagicMock()
        fn = MagicMock()
        client.remove("speak", fn)
        client.internal_bus.remove.assert_called_with("speak", fn)

    def test_remove_hive_event(self):
        client = _make_client()
        fn = MagicMock()
        client.remove(HiveMessageType.PING, fn)
        client.emitter.remove_listener.assert_called_with(HiveMessageType.PING, fn)


class TestHandleHiveProtocol(unittest.TestCase):
    def test_bus_message_emitted_to_internal_bus(self):
        client = _make_client()
        client.internal_bus = MagicMock()
        msg = HiveMessage(HiveMessageType.BUS, Message("speak", {"utterance": "hi"}))
        client._handle_hive_protocol(msg)
        self.assertTrue(client.internal_bus.emit.called)

    def test_non_bus_message_emitted_to_emitter(self):
        client = _make_client()
        msg = HiveMessage(HiveMessageType.PING, {"flood_id": "x"})
        client._handle_hive_protocol(msg)
        client.emitter.emit.assert_called()


class TestHandleBinary(unittest.TestCase):
    def test_tts_audio_callback(self):
        cb = MagicMock()
        client = _make_client(bin_callbacks=cb)
        msg = HiveMessage(HiveMessageType.BINARY, b"\x00\x01",
                          bin_type=HiveMindBinaryPayloadType.TTS_AUDIO,
                          metadata={"lang": "en", "utterance": "hi", "file_name": "tts.wav"})
        client._handle_binary(msg)
        cb.handle_receive_tts.assert_called_once_with(b"\x00\x01", "hi", "en", "tts.wav")

    def test_file_callback(self):
        cb = MagicMock()
        client = _make_client(bin_callbacks=cb)
        msg = HiveMessage(HiveMessageType.BINARY, b"\x00\x01",
                          bin_type=HiveMindBinaryPayloadType.FILE,
                          metadata={"file_name": "data.bin"})
        client._handle_binary(msg)
        cb.handle_receive_file.assert_called_once_with(b"\x00\x01", "data.bin")

    def test_undefined_binary_no_callback(self):
        cb = MagicMock()
        client = _make_client(bin_callbacks=cb)
        msg = HiveMessage(HiveMessageType.BINARY, b"\x00\x01",
                          bin_type=HiveMindBinaryPayloadType.UNDEFINED)
        client._handle_binary(msg)
        cb.handle_receive_tts.assert_not_called()
        cb.handle_receive_file.assert_not_called()


class TestBuildUrl(unittest.TestCase):
    def test_ssl_url(self):
        url = HiveMessageBusClient.build_url("mykey", host="example.com", port=5678, ssl=True)
        self.assertTrue(url.startswith("wss://"))
        self.assertIn("example.com:5678", url)
        self.assertIn("authorization=", url)

    def test_no_ssl_url(self):
        url = HiveMessageBusClient.build_url("mykey", ssl=False)
        self.assertTrue(url.startswith("ws://"))


if __name__ == "__main__":
    unittest.main()
