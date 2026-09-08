"""HiveMindHTTPClient must speak the protocol v3 Noise transport.

The websocket client did; the HTTP client had no send or receive path for
it. Against a HiveMind-core 5.x listener the handshake completed and then
the first message -- the HELLO -- went out in the clear, the listener closed
the session with 1008, and nothing was ever deliverable.
"""
import json
import threading
from unittest.mock import MagicMock, patch

import pybase64
import pytest

from hivemind_bus_client.http_client import HiveMindHTTPClient
from hivemind_bus_client.message import HiveMessage, HiveMessageType
from hivemind_bus_client.noise import NoiseTransportFailed


def _client(transport=None):
    c = HiveMindHTTPClient.__new__(HiveMindHTTPClient)
    c.noise_transport = transport
    c.crypto_key = None
    c.protocol = MagicMock(binarize=False)
    c.binarize = False
    c.compress = False
    c.connected = threading.Event(); c.connected.set()
    c.handshake_event = threading.Event()
    c._host, c._port = "http://127.0.0.1", 5679
    c._name, c._access_key, c._site_id = "ua", "k", "site"
    c.session_id = "sess"
    c.http_timeout = 5
    c._handle_hive_protocol = MagicMock()
    return c


def test_emit_on_a_v3_session_goes_through_the_noise_transport():
    transport = MagicMock()
    c = _client(transport)
    c.emit(HiveMessage(HiveMessageType.HELLO, {"pubkey": "x"}))
    transport.send_message.assert_called_once()
    plaintext, raw_send = transport.send_message.call_args.args
    assert json.loads(plaintext)["msg_type"] == "hello"
    with patch("hivemind_bus_client.http_client.requests.post") as post:
        post.return_value.ok = True
        raw_send(b"\x00frame\xff")
    data = post.call_args.kwargs["data"]
    assert data["binary"] == "1"
    assert pybase64.b64decode(data["message"]) == b"\x00frame\xff"


def test_a_rejected_frame_raises_instead_of_desyncing_silently():
    c = _client(MagicMock())
    with patch("hivemind_bus_client.http_client.requests.post") as post:
        post.return_value.ok = False
        post.return_value.status_code = 500
        with pytest.raises(ConnectionError):
            c._send_noise_frame(b"frame")


def test_received_frames_are_decrypted_then_dispatched():
    transport = MagicMock()
    transport.decrypt_frame.return_value = json.dumps(
        {"msg_type": "hello", "payload": {"pubkey": "srv"}})
    c = _client(transport)
    c.on_message(b"ciphertext")
    transport.decrypt_frame.assert_called_once_with(b"ciphertext")
    (msg,), _ = c._handle_hive_protocol.call_args
    assert msg.msg_type == HiveMessageType.HELLO


def test_a_cleartext_message_on_a_v3_session_is_dropped():
    transport = MagicMock()
    c = _client(transport)
    c.on_message('{"msg_type": "hello", "payload": {}}')
    transport.decrypt_frame.assert_not_called()
    c._handle_hive_protocol.assert_not_called()


def test_a_buffered_chunk_dispatches_nothing():
    transport = MagicMock()
    transport.decrypt_frame.return_value = None
    c = _client(transport)
    c.on_message(b"chunk")
    c._handle_hive_protocol.assert_not_called()


def test_a_tampered_frame_closes_the_session():
    transport = MagicMock()
    transport.decrypt_frame.side_effect = NoiseTransportFailed("bad tag")
    c = _client(transport)
    c.close_connection = MagicMock()
    c.on_message(b"tampered")
    c.close_connection.assert_called_once()
    c._handle_hive_protocol.assert_not_called()


def test_disconnect_forgets_the_transport_and_resets_the_protocol():
    c = _client(object())
    with patch("hivemind_bus_client.http_client.requests.post") as post:
        post.return_value.json.return_value = {"status": "Disconnected"}
        c.disconnect()
    assert c.noise_transport is None
    c.protocol.reset_connection_state.assert_called_once()


def test_close_connection_exists_for_abort_noise():
    c = _client(object())
    c.disconnect = MagicMock()
    c.close_connection()
    c.disconnect.assert_called_once()
