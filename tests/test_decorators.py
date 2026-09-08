"""Tests for hivemind_bus_client.decorators — listener classes and decorator functions."""
import unittest
from unittest.mock import MagicMock, call

from hivemind_bus_client.message import HiveMessage, HiveMessageType
from hivemind_bus_client.decorators import (
    HiveMessageListener,
    HivePayloadListener,
    on_hive_message,
    on_mycroft_message,
    on_third_party,
    on_broadcast,
    on_ping,
    on_propagate,
    on_escalate,
    on_handshake,
    on_hello,
    on_cascade,
    on_query,
    on_rendezvous,
    on_shared_bus,
    on_payload,
)


class TestHiveMessageListener(unittest.TestCase):
    def _make_bus(self):
        bus = MagicMock()
        return bus

    def test_init_stores_attrs(self):
        bus = self._make_bus()
        listener = HiveMessageListener(bus, HiveMessageType.BUS)
        self.assertIs(listener.bus, bus)
        self.assertEqual(listener.message_type, HiveMessageType.BUS)
        self.assertEqual(listener._handlers, [])

    def test_add_handler(self):
        bus = self._make_bus()
        listener = HiveMessageListener(bus, HiveMessageType.BUS)
        fn = MagicMock()
        listener.add_handler(fn)
        self.assertIn(fn, listener._handlers)

    def test_clear_handlers(self):
        bus = self._make_bus()
        listener = HiveMessageListener(bus, HiveMessageType.BUS)
        listener.add_handler(MagicMock())
        listener.add_handler(MagicMock())
        listener.clear_handlers()
        self.assertEqual(listener._handlers, [])

    def test_listen_registers_and_returns_self(self):
        bus = self._make_bus()
        listener = HiveMessageListener(bus, HiveMessageType.BUS)
        result = listener.listen()
        self.assertIs(result, listener)
        bus.once.assert_called_once_with(HiveMessageType.BUS, listener._handler)

    def test_shutdown_removes_handler(self):
        bus = self._make_bus()
        listener = HiveMessageListener(bus, HiveMessageType.BUS)
        listener.shutdown()
        bus.remove.assert_called_once_with(HiveMessageType.BUS, listener._handler)

    def test_handler_dispatches_and_rearms(self):
        bus = self._make_bus()
        listener = HiveMessageListener(bus, HiveMessageType.PING)
        fn1 = MagicMock()
        fn2 = MagicMock()
        listener.add_handler(fn1)
        listener.add_handler(fn2)

        msg = HiveMessage(HiveMessageType.PING, {"flood_id": "x"})
        listener._handler(msg)

        fn1.assert_called_once_with(msg)
        fn2.assert_called_once_with(msg)
        bus.once.assert_called_with(HiveMessageType.PING, listener._handler)


class TestHivePayloadListener(unittest.TestCase):
    def test_filters_on_payload_type(self):
        bus = MagicMock()
        listener = HivePayloadListener(
            payload_type=HiveMessageType.BUS,
            bus=bus,
            message_type=HiveMessageType.PROPAGATE,
        )
        fn = MagicMock()
        listener.add_handler(fn)

        # Inner payload matches
        inner = HiveMessage(HiveMessageType.BUS, {"type": "test", "data": {}, "context": {}})
        outer = HiveMessage(HiveMessageType.PROPAGATE, inner)
        listener._handler(outer)
        fn.assert_called_once()

    def test_skips_non_matching_payload_type(self):
        bus = MagicMock()
        listener = HivePayloadListener(
            payload_type=HiveMessageType.PING,
            bus=bus,
            message_type=HiveMessageType.PROPAGATE,
        )
        fn = MagicMock()
        listener.add_handler(fn)

        inner = HiveMessage(HiveMessageType.BUS, {"type": "test", "data": {}, "context": {}})
        outer = HiveMessage(HiveMessageType.PROPAGATE, inner)
        listener._handler(outer)
        fn.assert_not_called()


class TestOnHiveMessageDecorator(unittest.TestCase):
    def test_registers_handler(self):
        bus = MagicMock()

        @on_hive_message(HiveMessageType.PING, bus)
        def my_handler(msg):
            pass

        bus.on.assert_called_once_with(HiveMessageType.PING, my_handler)

    def test_returns_original_function(self):
        bus = MagicMock()

        @on_hive_message(HiveMessageType.BUS, bus)
        def my_handler(msg):
            pass

        self.assertEqual(my_handler.__name__, "my_handler")


class TestPayloadDecorators(unittest.TestCase):
    """All payload-listener decorators follow the same pattern."""

    def _test_decorator(self, decorator, msg_type, *args):
        bus = MagicMock()
        all_args = list(args) + [bus]

        @decorator(*all_args)
        def handler(msg):
            pass

        # Should register on the bus
        bus.once.assert_called_once()
        # Should attach shutdown
        self.assertTrue(hasattr(handler, "shutdown"))

    def test_on_mycroft_message(self):
        self._test_decorator(on_mycroft_message, HiveMessageType.BUS, "speak")

    def test_on_shared_bus(self):
        self._test_decorator(on_shared_bus, HiveMessageType.SHARED_BUS, "speak")

    def test_on_broadcast(self):
        self._test_decorator(on_broadcast, HiveMessageType.BROADCAST, "speak")

    def test_on_ping(self):
        self._test_decorator(on_ping, HiveMessageType.PING, "speak")

    def test_on_propagate(self):
        self._test_decorator(on_propagate, HiveMessageType.PROPAGATE, "speak")

    def test_on_escalate(self):
        self._test_decorator(on_escalate, HiveMessageType.ESCALATE, "speak")

    def test_on_handshake(self):
        self._test_decorator(on_handshake, HiveMessageType.HANDSHAKE, "speak")

    def test_on_hello(self):
        self._test_decorator(on_hello, HiveMessageType.HELLO, "speak")

    def test_on_query(self):
        self._test_decorator(on_query, HiveMessageType.QUERY, "speak")

    def test_on_cascade(self):
        self._test_decorator(on_cascade, HiveMessageType.CASCADE, "speak")

    def test_on_rendezvous(self):
        self._test_decorator(on_rendezvous, HiveMessageType.RENDEZVOUS, "speak")

    def test_on_third_party(self):
        bus = MagicMock()

        @on_third_party(bus)
        def handler(msg):
            pass

        bus.once.assert_called_once()
        self.assertTrue(hasattr(handler, "shutdown"))

    def test_on_payload(self):
        bus = MagicMock()

        @on_payload(HiveMessageType.PROPAGATE, "speak", bus)
        def handler(msg):
            pass

        bus.once.assert_called_once()
        self.assertTrue(hasattr(handler, "shutdown"))


if __name__ == "__main__":
    unittest.main()
