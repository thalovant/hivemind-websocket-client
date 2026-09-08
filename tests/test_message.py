"""Tests for hivemind_bus_client.message — HiveMessage, HiveMessageType, HiveMindBinaryPayloadType."""
import json
import pytest
from ovos_bus_client.message import Message
from hivemind_bus_client.message import HiveMessage, HiveMessageType, HiveMindBinaryPayloadType


class TestHiveMessageType:
    """Verify enum values match the wire protocol."""

    def test_all_types_are_strings(self):
        for member in HiveMessageType:
            assert isinstance(member.value, str)

    def test_known_values(self):
        assert HiveMessageType.BUS == "bus"
        assert HiveMessageType.PROPAGATE == "propagate"
        assert HiveMessageType.PING == "ping"
        assert HiveMessageType.BINARY == "bin"
        assert HiveMessageType.HANDSHAKE == "shake"

    def test_invalid_type_raises(self):
        with pytest.raises(ValueError, match="Unknown HiveMessage.msg_type"):
            HiveMessage("nonexistent", {})


class TestHiveMindBinaryPayloadType:
    def test_values_are_ints(self):
        for member in HiveMindBinaryPayloadType:
            assert isinstance(member.value, int)

    def test_bin_type_only_for_binary(self):
        with pytest.raises(ValueError, match="bin_type can only be set"):
            HiveMessage(HiveMessageType.BUS, Message("test", {}),
                        bin_type=HiveMindBinaryPayloadType.TTS_AUDIO)


class TestHiveMessageInit:
    """Constructor and payload normalization."""

    def test_from_mycroft_message(self):
        msg = Message("speak", {"utterance": "hi"})
        hm = HiveMessage(HiveMessageType.BUS, msg)
        assert hm._payload["type"] == "speak"
        assert hm._payload["data"]["utterance"] == "hi"

    def test_from_json_string(self):
        payload_str = json.dumps({"type": "speak", "data": {"utterance": "hi"}, "context": {}})
        hm = HiveMessage(HiveMessageType.BUS, payload_str)
        assert hm._payload["type"] == "speak"

    def test_from_dict(self):
        hm = HiveMessage(HiveMessageType.THIRDPRTY, {"custom": "data"})
        assert hm._payload["custom"] == "data"

    def test_none_payload_becomes_empty_dict(self):
        hm = HiveMessage(HiveMessageType.THIRDPRTY)
        assert hm._payload == {}

    def test_binary_requires_bytes(self):
        with pytest.raises(ValueError, match="expected 'bytes'"):
            HiveMessage(HiveMessageType.BINARY, "not bytes")

    def test_binary_with_bytes(self):
        data = b"\x00\x01\x02"
        hm = HiveMessage(HiveMessageType.BINARY, data,
                          bin_type=HiveMindBinaryPayloadType.RAW_AUDIO)
        assert hm.payload == data
        assert hm.bin_type == HiveMindBinaryPayloadType.RAW_AUDIO


class TestHiveMessagePayloadProperty:
    """Test the payload getter reconstructs correct types."""

    def test_bus_returns_mycroft_message(self):
        hm = HiveMessage(HiveMessageType.BUS,
                          Message("speak", {"utterance": "hello"}))
        payload = hm.payload
        assert isinstance(payload, Message)
        assert payload.msg_type == "speak"

    def test_propagate_returns_hive_message(self):
        inner = HiveMessage(HiveMessageType.PING, {"flood_id": "x"})
        outer = HiveMessage(HiveMessageType.PROPAGATE, inner)
        payload = outer.payload
        assert isinstance(payload, HiveMessage)
        assert payload.msg_type == HiveMessageType.PING

    def test_thirdparty_returns_dict(self):
        hm = HiveMessage(HiveMessageType.THIRDPRTY, {"foo": "bar"})
        assert isinstance(hm.payload, dict)
        assert hm.payload["foo"] == "bar"


class TestHiveMessageSerialization:
    def test_serialize_deserialize_roundtrip(self):
        msg = Message("speak", {"utterance": "hi"}, {"source": "test"})
        hm = HiveMessage(HiveMessageType.BUS, msg,
                          target_site_id="kitchen")
        serialized = hm.serialize()
        restored = HiveMessage.deserialize(serialized)
        assert restored.msg_type == HiveMessageType.BUS
        assert restored.target_site_id == "kitchen"

    def test_deserialize_from_dict(self):
        d = {"msg_type": "ping", "payload": {"flood_id": "abc"},
             "metadata": {}}
        restored = HiveMessage.deserialize(d)
        assert restored.msg_type == HiveMessageType.PING
        assert restored.payload["flood_id"] == "abc"

    def test_deserialize_mycroft_message(self):
        d = {"type": "speak", "data": {"utterance": "hi"}, "context": {}}
        restored = HiveMessage.deserialize(d)
        assert restored.msg_type == HiveMessageType.BUS

    def test_deserialize_unknown_falls_to_thirdparty(self):
        d = {"random_key": "value"}
        restored = HiveMessage.deserialize(d)
        assert restored.msg_type == HiveMessageType.THIRDPRTY

    def test_as_dict(self):
        hm = HiveMessage(HiveMessageType.THIRDPRTY, {"k": "v"},
                          node="node1", source_peer="peer1")
        d = hm.as_dict
        assert d["msg_type"] == HiveMessageType.THIRDPRTY
        assert d["payload"] == {"k": "v"}
        assert d["node"] == "node1"
        assert d["source_peer"] == "peer1"

    def test_binary_as_dict_raises(self):
        hm = HiveMessage(HiveMessageType.BINARY, b"\x00")
        with pytest.raises(ValueError, match="BINARY"):
            hm.as_dict


class TestHiveMessageRouting:
    def test_route_filters_incomplete_hops(self):
        hm = HiveMessage(HiveMessageType.THIRDPRTY, {},
                          route=[{"source": "a", "targets": ["b"]},
                                 {"source": "", "targets": []},
                                 {"source": "c", "targets": []}])
        # only hops with both source and targets are kept
        assert len(hm.route) == 1
        assert hm.route[0]["source"] == "a"

    def test_target_peers_defaults_to_source(self):
        hm = HiveMessage(HiveMessageType.THIRDPRTY, {},
                          source_peer="peer1")
        assert hm.target_peers == ["peer1"]

    def test_update_hop_data(self):
        hm = HiveMessage(HiveMessageType.THIRDPRTY, {},
                          source_peer="a", target_peers=["b"])
        hm.update_hop_data()
        assert len(hm._route) == 1
        assert hm._route[0]["source"] == "a"

    def test_replace_route(self):
        hm = HiveMessage(HiveMessageType.THIRDPRTY, {})
        new_route = [{"source": "x", "targets": ["y"]}]
        hm.replace_route(new_route)
        assert hm.route == new_route

    def test_add_remove_target_peer(self):
        hm = HiveMessage(HiveMessageType.THIRDPRTY, {})
        hm.add_target_peer("peer1")
        assert "peer1" in hm._targets
        hm.remove_target_peer("peer1")
        assert "peer1" not in hm._targets

    def test_update_source_peer(self):
        hm = HiveMessage(HiveMessageType.THIRDPRTY, {})
        result = hm.update_source_peer("new_peer")
        assert hm.source_peer == "new_peer"
        assert result is hm  # returns self for chaining


class TestHiveMessageItemAccess:
    def test_getitem(self):
        hm = HiveMessage(HiveMessageType.THIRDPRTY, {"key": "value"})
        assert hm["key"] == "value"
        assert hm["missing"] is None

    def test_setitem(self):
        hm = HiveMessage(HiveMessageType.THIRDPRTY, {"key": "old"})
        hm["key"] = "new"
        assert hm["key"] == "new"

    def test_getitem_non_dict_raises(self):
        hm = HiveMessage(HiveMessageType.BINARY, b"\x00")
        with pytest.raises(TypeError):
            _ = hm["key"]

    def test_setitem_non_dict_raises(self):
        hm = HiveMessage(HiveMessageType.BINARY, b"\x00")
        with pytest.raises(TypeError):
            hm["key"] = "value"


class TestHiveMessageStr:
    def test_str_normal(self):
        hm = HiveMessage(HiveMessageType.THIRDPRTY, {"k": "v"})
        s = str(hm)
        parsed = json.loads(s)
        assert parsed["msg_type"] == "3rdparty"

    def test_str_binary(self):
        hm = HiveMessage(HiveMessageType.BINARY, b"\x00\x01\x02")
        assert "BINARY" in str(hm)
        assert "3" in str(hm)  # length


class TestHiveMessagePayloadSetter:
    def test_set_message_payload(self):
        hm = HiveMessage(HiveMessageType.THIRDPRTY, {"old": "data"})
        new_msg = Message("speak", {"utterance": "hi"})
        hm.payload = new_msg
        # Stored as dict internally
        assert isinstance(hm._payload, dict)

    def test_set_hive_message_payload(self):
        hm = HiveMessage(HiveMessageType.THIRDPRTY, {"old": "data"})
        inner = HiveMessage(HiveMessageType.PING, {"flood_id": "x"})
        hm.payload = inner
        assert isinstance(hm._payload, dict)

    def test_set_dict_payload(self):
        hm = HiveMessage(HiveMessageType.THIRDPRTY, {"old": "data"})
        hm.payload = {"new": "data"}
        assert hm._payload == {"new": "data"}

    def test_set_bytes_payload(self):
        hm = HiveMessage(HiveMessageType.BINARY, b"\x00")
        hm.payload = b"\x01\x02"
        assert hm._payload == b"\x01\x02"


class TestHiveMessageTargetPeers:
    def test_target_peers_with_source_peer_fallback(self):
        hm = HiveMessage(HiveMessageType.THIRDPRTY, {},
                          source_peer="peer1")
        assert hm.target_peers == ["peer1"]

    def test_target_peers_explicit(self):
        hm = HiveMessage(HiveMessageType.THIRDPRTY, {},
                          source_peer="peer1", target_peers=["peer2"])
        assert hm.target_peers == ["peer2"]

    def test_target_peers_no_source(self):
        hm = HiveMessage(HiveMessageType.THIRDPRTY, {})
        assert hm.target_peers == []


class TestHiveMessageUpdateHopData:
    def test_update_hop_with_data_merge(self):
        hm = HiveMessage(HiveMessageType.THIRDPRTY, {},
                          source_peer="a", target_peers=["b"])
        hm.update_hop_data(data={"extra": "info"})
        assert len(hm._route) == 1
        assert hm._route[0]["extra"] == "info"

    def test_update_hop_same_source_no_duplicate(self):
        hm = HiveMessage(HiveMessageType.THIRDPRTY, {},
                          source_peer="a", target_peers=["b"])
        hm.update_hop_data()
        hm.update_hop_data()  # same source, should not duplicate
        assert len(hm._route) == 1


class TestDeserializePreservesFields:
    """Regression tests for deserialize() restoring serialized fields."""

    def test_serialize_deserialize_preserves_route(self):
        route = [{"source": "peer-A", "targets": ["peer-B", "peer-C"]}]
        msg = HiveMessage(HiveMessageType.BUS,
                          payload=Message("test", {}, {}),
                          source_peer="peer-A", target_peers=["peer-B", "peer-C"])
        msg.replace_route(route)
        restored = HiveMessage.deserialize(msg.as_dict)
        assert restored.route == route

    def test_source_peer_not_preserved_through_deserialize(self):
        """source_peer is per-hop transient — intentionally NOT restored."""
        msg = HiveMessage(HiveMessageType.BUS,
                          payload=Message("test", {}, {}),
                          source_peer="peer-X")
        restored = HiveMessage.deserialize(msg.serialize())
        assert restored.source_peer is None

    def test_node_not_preserved_through_deserialize(self):
        """node is per-hop transient — intentionally NOT restored."""
        msg = HiveMessage(HiveMessageType.BUS,
                          payload=Message("test", {}, {}),
                          node="node-42")
        restored = HiveMessage.deserialize(msg.serialize())
        assert restored.node_id is None

    def test_route_entries_are_dicts(self):
        route = [{"source": "p1", "targets": ["p2"]}, {"source": "p2", "targets": ["p3"]}]
        msg = HiveMessage(HiveMessageType.BUS, payload=Message("t", {}, {}))
        msg.replace_route(route)
        for hop in msg.route:
            assert isinstance(hop, dict)
            assert "source" in hop
            assert "targets" in hop
