"""Tests for hivemind_bus_client.protocol — HiveMindSlaveProtocol handlers."""
import time
from unittest.mock import MagicMock, patch
import pytest

from hivemind_bus_client.message import HiveMessage, HiveMessageType
from hivemind_bus_client.protocol import CascadeAggregator, HiveMindSlaveProtocol
from hivemind_bus_client.hive_map import HiveMapper, NodeInfo
from hivemind_bus_client.identity import NodeIdentity


def _make_protocol() -> HiveMindSlaveProtocol:
    """Create a minimal HiveMindSlaveProtocol with mocked dependencies."""
    hm = MagicMock()
    hm.session_id = "test-session"
    identity = MagicMock(spec=NodeIdentity)
    identity.name = "test-node"
    identity.password = "test-node-horse-battery-staple-92"
    proto = HiveMindSlaveProtocol(hm=hm, identity=identity, site_id="living-room")
    return proto


class TestHandlePing:
    def test_sends_responsive_ping(self):
        proto = _make_protocol()
        inner = HiveMessage(HiveMessageType.PING, {"flood_id": "abc", "peer": "other"})
        proto.handle_ping(inner)

        proto.hm.emit.assert_called_once()
        sent = proto.hm.emit.call_args[0][0]
        assert sent.msg_type == HiveMessageType.PROPAGATE
        inner_sent = sent.payload
        assert isinstance(inner_sent, HiveMessage)
        assert inner_sent.msg_type == HiveMessageType.PING
        payload = inner_sent.payload
        assert payload["flood_id"] == "abc"
        assert payload["peer"] == "test-node::test-session"
        assert payload["site_id"] == "living-room"

    def test_duplicate_flood_id_ignored(self):
        proto = _make_protocol()
        inner = HiveMessage(HiveMessageType.PING, {"flood_id": "abc", "peer": "other"})
        proto.handle_ping(inner)
        proto.hm.emit.reset_mock()

        # Second call with same flood_id
        proto.handle_ping(inner)
        proto.hm.emit.assert_not_called()

    def test_empty_flood_id_ignored(self):
        proto = _make_protocol()
        inner = HiveMessage(HiveMessageType.PING, {"flood_id": "", "peer": "other"})
        proto.handle_ping(inner)
        proto.hm.emit.assert_not_called()

    def test_flood_id_set_capped(self):
        """When seen flood IDs reach max_size, oldest entries are evicted."""
        proto = _make_protocol()
        mapper = proto.hive_mapper or HiveMapper()
        proto.hive_mapper = mapper
        # Pre-fill with 1000 flood IDs
        for i in range(1000):
            mapper.check_flood_id(str(i))
        inner = HiveMessage(HiveMessageType.PING, {"flood_id": "new", "peer": "other"})
        proto.handle_ping(inner)
        proto.hm.emit.assert_called_once()  # "new" was not seen, so ping sent

    def test_different_flood_ids_both_processed(self):
        proto = _make_protocol()
        for fid in ["flood1", "flood2"]:
            inner = HiveMessage(HiveMessageType.PING, {"flood_id": fid, "peer": "other"})
            proto.handle_ping(inner)
        assert proto.hm.emit.call_count == 2


class TestHandleQuery:
    def _make_protocol_with_bus(self):
        proto = _make_protocol()
        proto.internal_protocol = MagicMock()
        proto.internal_protocol.node_id = "master-1"
        proto.internal_protocol.bus = MagicMock()
        return proto

    def test_bus_payload_dispatches_to_handle_bus(self):
        """QUERY with inner BUS payload should call handle_bus."""
        proto = self._make_protocol_with_bus()
        inner_bus = HiveMessage(HiveMessageType.BUS,
                                {"type": "speak", "data": {"utterance": "hello"}, "context": {}})
        query_msg = HiveMessage(HiveMessageType.QUERY, payload=inner_bus)

        with patch.object(proto, 'handle_bus') as mock_bus:
            proto.handle_query(query_msg)
            mock_bus.assert_called_once()
            # the QUERY wraps an inner BUS HiveMessage; handle_bus gets that, not the wrapper
            called = mock_bus.call_args[0][0]
            assert called.msg_type == HiveMessageType.BUS

    def test_intercom_payload_dispatches_to_handle_intercom(self):
        """QUERY with inner INTERCOM payload should call handle_intercom."""
        proto = self._make_protocol_with_bus()
        inner_bus = HiveMessage(HiveMessageType.BUS,
                                {"type": "speak", "data": {}, "context": {}})
        inner_intercom = HiveMessage(HiveMessageType.INTERCOM, payload=inner_bus)
        query_msg = HiveMessage(HiveMessageType.QUERY, payload=inner_intercom)

        with patch.object(proto, 'handle_intercom') as mock_intercom:
            proto.handle_query(query_msg)
            mock_intercom.assert_called_once_with(query_msg)

    def test_invalid_inner_type_raises(self):
        """QUERY with non-BUS/INTERCOM inner payload should raise AssertionError."""
        proto = self._make_protocol_with_bus()
        inner = HiveMessage(HiveMessageType.PING, {"flood_id": "x"})
        query_msg = HiveMessage(HiveMessageType.QUERY, payload=inner)

        with pytest.raises(AssertionError):
            proto.handle_query(query_msg)


def _make_cascade_msg(utterance: str = "hello") -> HiveMessage:
    """Helper to build a CASCADE(BUS) message."""
    inner = HiveMessage(HiveMessageType.BUS,
                        {"type": "speak", "data": {"utterance": utterance}, "context": {}})
    return HiveMessage(HiveMessageType.CASCADE, payload=inner)


class TestCascadeAggregator:
    def test_single_response_emitted_on_timeout(self):
        """A single response should be emitted after timeout."""
        emitted = []
        agg = CascadeAggregator(
            timeout=0.05,
            select_callback=lambda rs: rs[0],
            emit_callback=emitted.append,
        )
        msg = _make_cascade_msg("one")
        agg.add_response(msg)
        assert emitted == []
        time.sleep(0.15)
        assert len(emitted) == 1
        assert emitted[0] is msg

    def test_multiple_responses_passed_to_select(self):
        """All buffered responses should be passed to the select callback."""
        collected = []

        def select_cb(responses):
            collected.extend(responses)
            return responses[-1]  # pick last

        emitted = []
        agg = CascadeAggregator(
            timeout=0.05,
            select_callback=select_cb,
            emit_callback=emitted.append,
        )
        msg1 = _make_cascade_msg("one")
        msg2 = _make_cascade_msg("two")
        agg.add_response(msg1)
        agg.add_response(msg2)
        time.sleep(0.15)
        assert len(collected) == 2
        assert emitted[0] is msg2

    def test_early_resolution_with_expected_responses(self):
        """Resolves immediately when expected_responses count is reached."""
        emitted = []
        agg = CascadeAggregator(
            timeout=10.0,  # very long — should NOT wait this long
            select_callback=lambda rs: rs[0],
            emit_callback=emitted.append,
            expected_responses=2,
        )
        agg.add_response(_make_cascade_msg("one"))
        assert emitted == []
        agg.add_response(_make_cascade_msg("two"))
        # should resolve synchronously, no need to wait
        assert len(emitted) == 1

    def test_late_responses_ignored_after_resolve(self):
        """Responses arriving after resolution are discarded."""
        emitted = []
        agg = CascadeAggregator(
            timeout=10.0,
            select_callback=lambda rs: rs[0],
            emit_callback=emitted.append,
            expected_responses=1,
        )
        agg.add_response(_make_cascade_msg("one"))
        assert len(emitted) == 1
        agg.add_response(_make_cascade_msg("late"))
        assert len(emitted) == 1  # still 1

    def test_cancel_prevents_emission(self):
        """Cancelling the aggregator should prevent any emission."""
        emitted = []
        agg = CascadeAggregator(
            timeout=0.05,
            select_callback=lambda rs: rs[0],
            emit_callback=emitted.append,
        )
        agg.add_response(_make_cascade_msg("one"))
        agg.cancel()
        time.sleep(0.15)
        assert emitted == []

    def test_select_returning_none_skips_emit(self):
        """If select_callback returns None, nothing is emitted."""
        emitted = []
        agg = CascadeAggregator(
            timeout=10.0,
            select_callback=lambda rs: None,
            emit_callback=emitted.append,
            expected_responses=1,
        )
        agg.add_response(_make_cascade_msg("one"))
        assert emitted == []


class TestHandleCascade:
    def _make_protocol_with_bus(self):
        proto = _make_protocol()
        proto.internal_protocol = MagicMock()
        proto.internal_protocol.node_id = "master-1"
        proto.internal_protocol.bus = MagicMock()
        proto.cascade_timeout = 0.05  # fast for tests
        return proto

    def test_buffers_and_emits_after_timeout(self):
        """handle_cascade should buffer responses and emit via aggregator."""
        proto = self._make_protocol_with_bus()
        msg = _make_cascade_msg("hi")

        with patch.object(proto, 'handle_bus') as mock_bus:
            proto.handle_cascade(msg)
            mock_bus.assert_not_called()  # not yet — buffered
            time.sleep(0.15)
            mock_bus.assert_called_once()

    def test_custom_select_callback(self):
        """User-provided cascade_select_callback should be used."""
        proto = self._make_protocol_with_bus()
        proto.cascade_select_callback = lambda rs: rs[-1]  # pick last

        msg1 = _make_cascade_msg("first")
        msg2 = _make_cascade_msg("second")

        with patch.object(proto, 'handle_bus') as mock_bus:
            proto.handle_cascade(msg1)
            proto.handle_cascade(msg2)
            time.sleep(0.15)
            mock_bus.assert_called_once()
            selected = mock_bus.call_args[0][0]
            # the aggregator emits the selected (last) response's inner BUS, not the wrapper
            assert selected.msg_type == HiveMessageType.BUS
            assert selected.payload.data["utterance"] == "second"

    def test_early_resolve_with_hive_mapper(self):
        """With hive_mapper set, resolves early when all nodes responded."""
        proto = self._make_protocol_with_bus()
        proto.cascade_timeout = 10.0  # long timeout — should resolve early
        mapper = HiveMapper()
        mapper.nodes = {"peer1": NodeInfo(peer="peer1"), "peer2": NodeInfo(peer="peer2")}
        proto.hive_mapper = mapper

        with patch.object(proto, 'handle_bus') as mock_bus:
            proto.handle_cascade(_make_cascade_msg("one"))
            mock_bus.assert_not_called()
            proto.handle_cascade(_make_cascade_msg("two"))
            mock_bus.assert_called_once()  # resolved immediately

    def test_invalid_inner_type_raises(self):
        """CASCADE with non-BUS/INTERCOM inner payload should raise AssertionError."""
        proto = self._make_protocol_with_bus()
        inner = HiveMessage(HiveMessageType.PING, {"flood_id": "x"})
        cascade_msg = HiveMessage(HiveMessageType.CASCADE, payload=inner)

        with pytest.raises(AssertionError):
            proto.handle_cascade(cascade_msg)


class TestHandlePropagate:
    def test_ping_dispatched(self):
        proto = _make_protocol()
        inner = HiveMessage(HiveMessageType.PING, {"flood_id": "abc", "peer": "x"})
        outer = HiveMessage(HiveMessageType.PROPAGATE, inner)

        with patch.object(proto, 'handle_ping') as mock_ping:
            proto.handle_propagate(outer)
            mock_ping.assert_called_once()
            called_with = mock_ping.call_args[0][0]
            assert called_with.msg_type == HiveMessageType.PING
            assert called_with.payload == {"flood_id": "abc", "peer": "x"}
