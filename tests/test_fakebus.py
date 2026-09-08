"""Tests for AsyncFakeHiveMessageBus.

Mirrors the test shape of ovos_utils.fakebus.AsyncFakeBus and exercises
the surface of AsyncHiveMessageBusClient without standing up a real
WebSocket.
"""
from __future__ import annotations

import asyncio
import unittest

from ovos_bus_client.message import Message as MycroftMessage

from hivemind_bus_client.fakebus import AsyncFakeHiveMessageBus
from hivemind_bus_client.message import HiveMessage, HiveMessageType


def _run(coro):
    return asyncio.run(coro)


class TestLifecycle(unittest.TestCase):
    def test_constructs_idle(self):
        bus = AsyncFakeHiveMessageBus()
        self.assertFalse(bus.connected_event.is_set())
        self.assertFalse(bus.handshake_event.is_set())

    def test_connect_sets_both_events(self):
        bus = AsyncFakeHiveMessageBus()
        _run(bus.connect())
        self.assertTrue(bus.connected_event.is_set())
        self.assertTrue(bus.handshake_event.is_set())

    def test_connect_site_id_override(self):
        bus = AsyncFakeHiveMessageBus()
        _run(bus.connect(site_id="kitchen"))
        self.assertEqual(bus.site_id, "kitchen")

    def test_close_clears_events(self):
        bus = AsyncFakeHiveMessageBus()
        _run(bus.connect())
        _run(bus.close())
        self.assertFalse(bus.connected_event.is_set())
        self.assertFalse(bus.handshake_event.is_set())

    def test_wait_for_handshake_returns_when_set(self):
        bus = AsyncFakeHiveMessageBus()
        _run(bus.connect())
        _run(bus.wait_for_handshake(timeout=0.1))

    def test_wait_for_handshake_raises_when_never_set(self):
        bus = AsyncFakeHiveMessageBus()
        with self.assertRaises(RuntimeError):
            _run(bus.wait_for_handshake(timeout=0.05))


class TestHandlerRegistration(unittest.TestCase):
    def setUp(self):
        self.bus = AsyncFakeHiveMessageBus()
        _run(self.bus.connect())

    def test_on_hive_event_dispatches(self):
        seen = []
        self.bus.on(HiveMessageType.PING, lambda m: seen.append(m))
        _run(self.bus.emit(HiveMessage(HiveMessageType.PING, {"flood_id": "x"})))
        self.assertEqual(len(seen), 1)
        self.assertEqual(seen[0].msg_type, HiveMessageType.PING)

    def test_on_mycroft_event_dispatches_via_internal_bus(self):
        seen = []
        self.bus.on("speak", lambda m: seen.append(m))
        _run(self.bus.emit(MycroftMessage("speak", {"utterance": "hi"})))
        self.assertEqual(len(seen), 1)
        self.assertEqual(seen[0].msg_type, "speak")

    def test_once_fires_only_once(self):
        seen = []
        self.bus.once(HiveMessageType.PING, lambda m: seen.append(m))
        _run(self.bus.emit(HiveMessage(HiveMessageType.PING)))
        _run(self.bus.emit(HiveMessage(HiveMessageType.PING)))
        self.assertEqual(len(seen), 1)

    def test_remove_hive_handler(self):
        cb = lambda m: None
        self.bus.on(HiveMessageType.PING, cb)
        self.bus.remove(HiveMessageType.PING, cb)
        self.assertEqual(self.bus.emitter.listeners(HiveMessageType.PING), [])

    def test_remove_mycroft_handler(self):
        cb = lambda m: None
        self.bus.on("speak", cb)
        self.bus.remove("speak", cb)

    def test_on_mycroft_alias(self):
        seen = []
        self.bus.on_mycroft("recognizer_loop:utterance",
                            lambda m: seen.append(m))
        _run(self.bus.emit(MycroftMessage("recognizer_loop:utterance",
                                          {"utterances": ["hi"]})))
        self.assertEqual(len(seen), 1)


class TestEmitRoutingContext(unittest.TestCase):
    def test_emit_mycroft_wrapped_into_bus(self):
        bus = AsyncFakeHiveMessageBus()
        _run(bus.connect())
        _run(bus.emit(MycroftMessage("speak", {"utterance": "hi"})))
        self.assertEqual(len(bus.emitted), 1)
        self.assertEqual(bus.emitted[0].msg_type, HiveMessageType.BUS)
        self.assertEqual(bus.emitted[0].payload.msg_type, "speak")

    def test_emit_injects_routing_context(self):
        bus = AsyncFakeHiveMessageBus(
            session_id="sess-1", site_id="kitchen", useragent="bridge-X",
        )
        _run(bus.connect())
        _run(bus.emit(MycroftMessage("speak", {"u": "hi"})))
        payload = bus.emitted[0].payload
        self.assertEqual(payload.context["source"], "bridge-X")
        self.assertEqual(payload.context["destination"], "HiveMind")
        self.assertEqual(payload.context["session"]["session_id"], "sess-1")
        self.assertEqual(payload.context["session"]["site_id"], "kitchen")

    def test_emit_records_in_emitted_list(self):
        bus = AsyncFakeHiveMessageBus()
        _run(bus.connect())
        _run(bus.emit(HiveMessage(HiveMessageType.PING, {"flood_id": "a"})))
        _run(bus.emit(HiveMessage(HiveMessageType.PING, {"flood_id": "b"})))
        self.assertEqual(len(bus.emitted), 2)


class TestWaitForMessage(unittest.TestCase):
    def test_returns_matched(self):
        bus = AsyncFakeHiveMessageBus()
        _run(bus.connect())

        async def scenario():
            async def feed():
                await asyncio.sleep(0.02)
                await bus.emit(HiveMessage(HiveMessageType.PING,
                                           {"flood_id": "x"}))
            asyncio.create_task(feed())
            return await bus.wait_for_message(HiveMessageType.PING, timeout=1.0)

        got = _run(scenario())
        self.assertIsNotNone(got)
        self.assertEqual(got.msg_type, HiveMessageType.PING)

    def test_returns_none_on_timeout(self):
        bus = AsyncFakeHiveMessageBus()
        _run(bus.connect())

        async def scenario():
            return await bus.wait_for_message(HiveMessageType.PING, timeout=0.05)

        self.assertIsNone(_run(scenario()))


class TestWaitForResponse(unittest.TestCase):
    def test_mycroft_response_matches_inner_msg_type(self):
        bus = AsyncFakeHiveMessageBus()
        _run(bus.connect())

        async def scenario():
            def echo(m: HiveMessage):
                # respond as a BUS HiveMessage carrying a Mycroft reply
                asyncio.create_task(
                    bus.emit(MycroftMessage(
                        "speak",
                        {"utterance": "ok"},
                        m.payload.context,
                    ))
                )
            bus.on(HiveMessageType.BUS, echo)
            return await bus.wait_for_response(
                MycroftMessage("speak", {"utterance": "hello"}),
                reply_type="speak",
                timeout=1.0,
            )

        reply = _run(scenario())
        self.assertIsNotNone(reply)
        self.assertEqual(reply.msg_type, HiveMessageType.BUS)
        self.assertEqual(reply.payload.msg_type, "speak")

    def test_hive_response_returns_first_match(self):
        bus = AsyncFakeHiveMessageBus()
        _run(bus.connect())

        async def scenario():
            def echo(_m):
                asyncio.create_task(
                    bus.emit(HiveMessage(HiveMessageType.PING, {"flood_id": "r"}))
                )
            bus.on(HiveMessageType.PING, echo)
            return await bus.wait_for_response(
                HiveMessage(HiveMessageType.PING, {"flood_id": "q"}),
                timeout=1.0,
            )

        reply = _run(scenario())
        self.assertIsNotNone(reply)
        self.assertEqual(reply.msg_type, HiveMessageType.PING)

    def test_returns_none_on_timeout(self):
        bus = AsyncFakeHiveMessageBus()
        _run(bus.connect())

        async def scenario():
            # Distinct reply_type so the emit doesn't trigger its own waiter.
            return await bus.wait_for_response(
                HiveMessage(HiveMessageType.PING),
                reply_type=HiveMessageType.CASCADE,
                timeout=0.05,
            )

        self.assertIsNone(_run(scenario()))


class TestWaitForPayloadAndMycroft(unittest.TestCase):
    def test_wait_for_payload_filters_inner(self):
        bus = AsyncFakeHiveMessageBus()
        _run(bus.connect())

        async def scenario():
            async def feed():
                await asyncio.sleep(0.02)
                # wrong inner type — ignored
                await bus.emit(
                    HiveMessage(HiveMessageType.BUS,
                                payload=MycroftMessage("not-speak"))
                )
                await asyncio.sleep(0.02)
                # right inner type — matched
                await bus.emit(
                    HiveMessage(HiveMessageType.BUS,
                                payload=MycroftMessage("speak",
                                                       {"utterance": "ok"}))
                )
            asyncio.create_task(feed())
            return await bus.wait_for_payload(
                "speak",
                message_type=HiveMessageType.BUS,
                timeout=1.0,
            )

        got = _run(scenario())
        self.assertIsNotNone(got)
        self.assertEqual(got.payload.msg_type, "speak")

    def test_wait_for_mycroft_is_payload_with_bus_default(self):
        bus = AsyncFakeHiveMessageBus()
        _run(bus.connect())

        async def scenario():
            async def feed():
                await asyncio.sleep(0.02)
                await bus.emit(MycroftMessage("speak", {"u": "ok"}))
            asyncio.create_task(feed())
            return await bus.wait_for_mycroft("speak", timeout=1.0)

        got = _run(scenario())
        self.assertIsNotNone(got)
        self.assertEqual(got.payload.msg_type, "speak")


class TestEmitIntercom(unittest.TestCase):
    def test_intercom_passes_through(self):
        bus = AsyncFakeHiveMessageBus()
        _run(bus.connect())
        _run(bus.emit_intercom({"payload": "anything"}, pubkey="ignored"))
        self.assertEqual(bus.emitted[0].msg_type, HiveMessageType.INTERCOM)


if __name__ == "__main__":
    unittest.main()
