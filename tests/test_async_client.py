"""Tests for hivemind_bus_client.async_client.

Covers the public surface of AsyncHiveMessageBusClient, AsyncHiveMessageWaiter,
and AsyncHivePayloadWaiter without standing up a real WebSocket server. The
client's websocket attribute and protocol are mocked so we exercise emit,
on_message dispatch, handler registration, and the asyncio.Event-based
handshake/connect lifecycle.
"""
from __future__ import annotations

import asyncio
import json
import unittest
from unittest.mock import AsyncMock, MagicMock, patch

import pytest
from ovos_bus_client.message import Message as MycroftMessage

from hivemind_bus_client.async_client import (AsyncHiveMessageBusClient,
                                              AsyncHiveMessageWaiter,
                                              AsyncHivePayloadWaiter,
                                              BinaryDataCallbacks)
from hivemind_bus_client.message import HiveMessage, HiveMessageType


def _bare_client(**overrides) -> AsyncHiveMessageBusClient:
    """Construct a client without going through identity validation.

    We bypass __init__ — the identity helpers require either a real
    NodeIdentity or a fully-configured one. Tests only need to exercise
    the post-connect machinery.
    """
    bus = AsyncHiveMessageBusClient.__new__(AsyncHiveMessageBusClient)
    bus.bin_callbacks = BinaryDataCallbacks()
    bus.json_encoding = "JSON_HEX"
    bus.cipher = "AES_GCM"
    bus.crypto_key = None
    bus.noise_transport = None
    bus.max_protocol_version = 3
    bus.compress = False
    bus.binarize = False
    bus.websocket_ping_interval = None
    bus.websocket_ping_timeout = None
    bus.allow_self_signed = True
    bus.share_bus = False
    # MagicMock(name=...) sets the mock's repr, NOT a .name attribute. Assign explicitly.
    ident = MagicMock()
    ident.access_key = "k"
    ident.password = "p"
    ident.default_master = "ws://127.0.0.1"
    ident.default_port = 5678
    ident.name = "test-useragent"
    ident.site_id = "testsite"
    ident.private_key = ""
    bus.identity = ident
    bus.session_id = "test-session"
    bus.connected_event = asyncio.Event()
    bus.handshake_event = asyncio.Event()
    from pyee import EventEmitter
    bus.emitter = EventEmitter()
    from ovos_utils.fakebus import FakeBus
    bus.internal_bus = FakeBus()
    bus._ws = None
    bus._receive_task = None
    bus.protocol = MagicMock(binarize=False, start_handshake=MagicMock())
    for k, v in overrides.items():
        setattr(bus, k, v)
    return bus


# ---------------------------------------------------------------------------
# BinaryDataCallbacks
# ---------------------------------------------------------------------------

class TestBinaryDataCallbacks(unittest.TestCase):
    def test_handle_receive_tts_warns_but_does_not_raise(self):
        BinaryDataCallbacks().handle_receive_tts(b"\x00", "u", "en", "f.wav")

    def test_handle_receive_file_warns_but_does_not_raise(self):
        BinaryDataCallbacks().handle_receive_file(b"\x00", "f.bin")


# ---------------------------------------------------------------------------
# Handler registration
# ---------------------------------------------------------------------------

class TestHandlerRegistration(unittest.TestCase):
    def test_on_hivemessage_type_goes_to_emitter(self):
        bus = _bare_client()
        seen = []
        bus.on(HiveMessageType.PING, lambda m: seen.append(m))
        bus.emitter.emit(HiveMessageType.PING, HiveMessage(HiveMessageType.PING))
        self.assertEqual(len(seen), 1)

    def test_on_unknown_routes_to_mycroft_internal_bus(self):
        bus = _bare_client()
        # internal_bus is a real FakeBus
        seen = []
        bus.on("some.mycroft.event", lambda m: seen.append(m))
        bus.internal_bus.emit(MycroftMessage("some.mycroft.event", {"v": 1}))
        self.assertEqual(len(seen), 1)

    def test_remove_hive_event(self):
        bus = _bare_client()
        cb = lambda m: None
        bus.on(HiveMessageType.PING, cb)
        bus.remove(HiveMessageType.PING, cb)
        self.assertEqual(bus.emitter.listeners(HiveMessageType.PING), [])

    def test_remove_mycroft_event(self):
        bus = _bare_client()
        cb = lambda m: None
        bus.on("custom", cb)
        bus.remove("custom", cb)


# ---------------------------------------------------------------------------
# AsyncHiveMessageWaiter / AsyncHivePayloadWaiter
# ---------------------------------------------------------------------------

@pytest.mark.asyncio
async def test_waiter_returns_matched_message():
    bus = _bare_client()
    waiter = AsyncHiveMessageWaiter(bus, HiveMessageType.PING)
    msg = HiveMessage(HiveMessageType.PING, {"flood_id": "x"})
    bus.emitter.emit(HiveMessageType.PING, msg)
    out = await waiter.wait(timeout=0.2)
    assert out is msg


@pytest.mark.asyncio
async def test_waiter_returns_none_on_timeout():
    bus = _bare_client()
    waiter = AsyncHiveMessageWaiter(bus, HiveMessageType.PING)
    out = await waiter.wait(timeout=0.05)
    assert out is None


@pytest.mark.asyncio
async def test_payload_waiter_filters_by_inner_msg_type():
    bus = _bare_client()
    waiter = AsyncHivePayloadWaiter(
        bus, payload_type="speak", message_type=HiveMessageType.BUS,
    )
    # wrong inner type — ignored
    wrong = HiveMessage(HiveMessageType.BUS, payload=MycroftMessage("not-speak", {}))
    bus.emitter.emit(HiveMessageType.BUS, wrong)
    out_first = await waiter.wait(timeout=0.05)
    assert out_first is None

    # right inner type — matched
    waiter = AsyncHivePayloadWaiter(
        bus, payload_type="speak", message_type=HiveMessageType.BUS,
    )
    right = HiveMessage(HiveMessageType.BUS,
                        payload=MycroftMessage("speak", {"utterance": "hi"}))
    bus.emitter.emit(HiveMessageType.BUS, right)
    out = await waiter.wait(timeout=0.2)
    assert out is right


# ---------------------------------------------------------------------------
# on_message dispatch (the wire-decode path)
# ---------------------------------------------------------------------------

class TestOnMessageDispatch(unittest.TestCase):
    def test_dispatches_bus_payload_to_internal_bus_and_emitter(self):
        bus = _bare_client()
        seen_bus = []
        bus.internal_bus.on("speak", lambda m: seen_bus.append(m))
        seen_hive = []
        bus.emitter.on(HiveMessageType.BUS, lambda m: seen_hive.append(m))

        # send wire-style JSON: a HiveMessage(BUS, payload=Mycroft speak)
        wire = json.dumps({
            "msg_type": HiveMessageType.BUS,
            "payload": {"type": "speak", "data": {"utterance": "hi"}, "context": {}},
        }, ensure_ascii=False)
        bus.on_message(wire)

        self.assertEqual(len(seen_bus), 1)
        self.assertEqual(seen_bus[0].msg_type, "speak")
        self.assertEqual(len(seen_hive), 1)

    def test_dispatches_dict_message_directly(self):
        bus = _bare_client()
        seen = []
        bus.emitter.on(HiveMessageType.PING, lambda m: seen.append(m))
        bus.on_message({"msg_type": HiveMessageType.PING})
        self.assertEqual(len(seen), 1)

    def test_unencryptable_ciphertext_is_ignored(self):
        bus = _bare_client()
        # crypto_key None, but ciphertext present → no crash, no dispatch
        bus.on_message({"ciphertext": "deadbeef", "tag": "x", "nonce": "y"})


# ---------------------------------------------------------------------------
# emit (with mocked websocket)
# ---------------------------------------------------------------------------

@pytest.mark.asyncio
async def test_emit_wraps_mycroft_into_bus():
    bus = _bare_client()
    bus._ws = AsyncMock()
    bus.connected_event.set()
    await bus.emit(MycroftMessage("speak", {"utterance": "hi"}))
    bus._ws.send.assert_awaited_once()


@pytest.mark.asyncio
async def test_emit_injects_routing_context():
    bus = _bare_client()
    bus._ws = AsyncMock()
    bus.connected_event.set()

    sent_calls = []
    async def _capture(payload):
        sent_calls.append(payload)
    bus._ws.send = _capture

    await bus.emit(MycroftMessage("speak", {"utterance": "hi"}))
    self_assertions = []  # use plain asserts below
    # the wire payload is a JSON string by default (binarize=False here)
    assert sent_calls, "no message sent"
    decoded = json.loads(sent_calls[0])
    ctx = decoded["payload"]["context"]
    assert ctx["source"] == "test-useragent"
    assert ctx["destination"] == "HiveMind"
    assert ctx["session"]["session_id"] == "test-session"


@pytest.mark.asyncio
async def test_emit_raises_when_disconnected_and_never_started():
    bus = _bare_client()
    bus._ws = AsyncMock()
    # connected_event is NOT set; emit should bail after the 10s wait.
    # Patch wait_for to surface the timeout immediately.
    async def _timeout(awaitable, timeout=None):
        awaitable.close()
        raise asyncio.TimeoutError()

    with patch("hivemind_bus_client.async_client.asyncio.wait_for",
               side_effect=_timeout):
        with pytest.raises(RuntimeError):
            await bus.emit(HiveMessage(HiveMessageType.PING))


# ---------------------------------------------------------------------------
# Connect / handshake lifecycle (mocked websockets)
# ---------------------------------------------------------------------------

@pytest.mark.asyncio
async def test_wait_for_handshake_returns_when_event_set_in_time():
    bus = _bare_client()
    bus.handshake_event.set()
    # already set → returns immediately
    await bus.wait_for_handshake(timeout=0.1)


@pytest.mark.asyncio
async def test_wait_for_handshake_raises_after_retries_exhausted():
    bus = _bare_client()
    # connected but no handshake; retries exhaust → RuntimeError
    bus.connected_event.set()
    bus.handshake_event.clear()
    with pytest.raises(RuntimeError):
        await bus.wait_for_handshake(timeout=0.01, max_retries=1)


def test_async_keepalive_options_default():
    bus = _bare_client()
    assert bus._websocket_keepalive_options() == {
        "ping_interval": 25.0,
        "ping_timeout": 10.0,
    }


def test_async_keepalive_options_can_disable_interval():
    bus = _bare_client(websocket_ping_interval=0)
    assert bus._websocket_keepalive_options() == {"ping_interval": None}


def test_async_keepalive_options_adjusts_timeout():
    bus = _bare_client(websocket_ping_interval=12, websocket_ping_timeout=12)
    assert bus._websocket_keepalive_options() == {
        "ping_interval": 12.0,
        "ping_timeout": 6.0,
    }


@pytest.mark.asyncio
async def test_close_cancels_receive_task_and_resets_state():
    bus = _bare_client()

    async def _idle():
        await asyncio.sleep(10)

    bus._ws = AsyncMock()
    bus._ws.close = AsyncMock()
    bus._receive_task = asyncio.create_task(_idle())
    bus.connected_event.set()
    bus.handshake_event.set()
    bus.crypto_key = "deadbeef"

    await bus.close()
    assert not bus.connected_event.is_set()
    assert not bus.handshake_event.is_set()
    assert bus.crypto_key is None
    bus._ws.close.assert_awaited_once()


# ---------------------------------------------------------------------------
# wait_for_response and wait_for_message
# ---------------------------------------------------------------------------

@pytest.mark.asyncio
async def test_wait_for_message_via_emitter():
    bus = _bare_client()
    bus._ws = AsyncMock()
    bus.connected_event.set()

    async def feed():
        await asyncio.sleep(0.02)
        bus.emitter.emit(HiveMessageType.PING,
                         HiveMessage(HiveMessageType.PING, {"flood_id": "x"}))

    asyncio.create_task(feed())
    got = await bus.wait_for_message(HiveMessageType.PING, timeout=0.5)
    assert got is not None
    assert got.msg_type == HiveMessageType.PING


@pytest.mark.asyncio
async def test_wait_for_response_emits_and_returns_matching_reply():
    bus = _bare_client()
    bus._ws = AsyncMock()
    bus.connected_event.set()

    query = HiveMessage(HiveMessageType.PING, {"flood_id": "q1"})

    async def reply():
        await asyncio.sleep(0.02)
        bus.emitter.emit(HiveMessageType.PING,
                         HiveMessage(HiveMessageType.PING, {"flood_id": "r1"}))

    asyncio.create_task(reply())
    reply_msg = await bus.wait_for_response(query, timeout=0.5)
    assert reply_msg is not None
    bus._ws.send.assert_awaited()


# ---------------------------------------------------------------------------
# build_url
# ---------------------------------------------------------------------------

class TestBuildUrl(unittest.TestCase):
    def test_ws_scheme(self):
        url = AsyncHiveMessageBusClient.build_url(
            key="k", host="h", port=1, useragent="u", ssl=False,
        )
        self.assertTrue(url.startswith("ws://h:1?authorization="))

    def test_wss_scheme(self):
        url = AsyncHiveMessageBusClient.build_url(
            key="k", host="h", port=1, useragent="u", ssl=True,
        )
        self.assertTrue(url.startswith("wss://h:1?authorization="))
