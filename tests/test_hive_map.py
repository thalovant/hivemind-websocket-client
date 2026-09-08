"""Tests for hivemind_bus_client.hive_map — HiveMapper and NodeInfo."""
import json
import pytest
from hivemind_bus_client.hive_map import HiveMapper, NodeInfo
from hivemind_bus_client.message import HiveMessage, HiveMessageType


class TestNodeInfo:
    def test_latency_ms_both_timestamps(self):
        node = NodeInfo(peer="a", timestamp=100.0, received_at=100.05)
        assert node.latency_ms == pytest.approx(50.0)

    def test_latency_ms_missing_timestamp(self):
        node = NodeInfo(peer="a", received_at=100.0)
        assert node.latency_ms is None

    def test_latency_ms_missing_received(self):
        node = NodeInfo(peer="a", timestamp=100.0)
        assert node.latency_ms is None

    def test_latency_ms_both_none(self):
        node = NodeInfo(peer="a")
        assert node.latency_ms is None


def _make_ping(flood_id: str, peer: str, site_id: str = "",
               route: list = None, timestamp: float = None) -> HiveMessage:
    """Helper: build a PING HiveMessage with optional route."""
    payload = {"flood_id": flood_id, "peer": peer, "site_id": site_id}
    if timestamp is not None:
        payload["timestamp"] = timestamp
    msg = HiveMessage(HiveMessageType.PING, payload, route=route or [])
    return msg


class TestHiveMapperOnPing:
    def test_new_ping_returns_true(self):
        mapper = HiveMapper()
        mapper.start_ping("flood1")
        msg = _make_ping("flood1", "nodeA")
        assert mapper.on_ping(msg) is True
        assert "nodeA" in mapper.nodes

    def test_duplicate_ping_returns_false(self):
        mapper = HiveMapper()
        mapper.start_ping("flood1")
        msg = _make_ping("flood1", "nodeA")
        mapper.on_ping(msg)
        assert mapper.on_ping(msg) is False

    def test_empty_peer_ignored(self):
        mapper = HiveMapper()
        mapper.start_ping("flood1")
        msg = _make_ping("flood1", "")
        assert mapper.on_ping(msg) is False

    def test_empty_flood_id_ignored(self):
        mapper = HiveMapper()
        msg = _make_ping("", "nodeA")
        assert mapper.on_ping(msg) is False

    def test_malformed_route_hop_skipped(self):
        mapper = HiveMapper()
        mapper.start_ping("f1")
        route = [{"source": "hub", "targets": ["nodeA"]}, "malformed"]
        msg = _make_ping("f1", "nodeA", route=route)
        mapper.on_ping(msg)
        assert "hub" in mapper.edges

    def test_non_dict_payload_ignored(self):
        mapper = HiveMapper()
        # HiveMessageType.PING with a non-dict payload (raw string won't happen
        # in practice but tests the guard)
        msg = HiveMessage(HiveMessageType.THIRDPRTY, {"flood_id": "x", "peer": "y"})
        # msg_type is THIRDPRTY so payload returns dict — but for PING type,
        # payload is returned as-is (dict). Let's test with a proper PING.
        msg2 = HiveMessage(HiveMessageType.PING, {})
        assert mapper.on_ping(msg2) is False

    def test_route_edges_extracted(self):
        mapper = HiveMapper()
        mapper.start_ping("f1")
        route = [{"source": "hub", "targets": ["relay1"]},
                 {"source": "relay1", "targets": ["nodeA"]}]
        msg = _make_ping("f1", "nodeA", route=route)
        mapper.on_ping(msg)
        assert "hub" in mapper.edges
        assert "relay1" in mapper.edges["hub"]
        assert "relay1" in mapper.edges
        assert "nodeA" in mapper.edges["relay1"]

    def test_node_info_populated(self):
        mapper = HiveMapper()
        mapper.start_ping("f1")
        msg = _make_ping("f1", "nodeA", site_id="kitchen", timestamp=1000.0)
        mapper.on_ping(msg, received_at=1000.05)
        node = mapper.nodes["nodeA"]
        assert node.site_id == "kitchen"
        assert node.latency_ms == pytest.approx(50.0)

    def test_auto_creates_seen_set_for_unknown_flood_id(self):
        mapper = HiveMapper()
        # Don't call start_ping — on_ping should auto-create the set
        msg = _make_ping("unknown_flood", "nodeX")
        assert mapper.on_ping(msg) is True

    def test_multiple_floods_independent(self):
        mapper = HiveMapper()
        mapper.start_ping("f1")
        mapper.start_ping("f2")
        msg1 = _make_ping("f1", "nodeA")
        msg2 = _make_ping("f2", "nodeA")
        assert mapper.on_ping(msg1) is True
        assert mapper.on_ping(msg2) is True
        # Same peer, different flood_id — both accepted


class TestHiveMapperToDict:
    def test_empty_mapper(self):
        mapper = HiveMapper()
        d = mapper.to_dict()
        assert d == {"nodes": [], "edges": []}

    def test_nodes_and_edges(self):
        mapper = HiveMapper()
        mapper.start_ping("f1")
        route = [{"source": "hub", "targets": ["nodeA"]}]
        mapper.on_ping(_make_ping("f1", "nodeA", site_id="lr", route=route,
                                   timestamp=10.0), received_at=10.1)
        d = mapper.to_dict()
        assert len(d["nodes"]) == 1
        assert d["nodes"][0]["peer"] == "nodeA"
        assert d["nodes"][0]["site_id"] == "lr"
        assert len(d["edges"]) == 1
        assert d["edges"][0] == {"source": "hub", "target": "nodeA"}


class TestHiveMapperToJson:
    def test_valid_json(self):
        mapper = HiveMapper()
        mapper.start_ping("f1")
        mapper.on_ping(_make_ping("f1", "nodeA"))
        result = json.loads(mapper.to_json())
        assert "nodes" in result
        assert "edges" in result


class TestHiveMapperToAscii:
    def test_empty_returns_no_nodes(self):
        mapper = HiveMapper()
        assert mapper.to_ascii() == "[No nodes discovered]"

    def test_single_node_with_root(self):
        mapper = HiveMapper()
        mapper.start_ping("f1")
        route = [{"source": "nodeA", "targets": ["root"]}]
        mapper.on_ping(_make_ping("f1", "nodeA", site_id="lr", route=route))
        tree = mapper.to_ascii(root_peer="root")
        assert "[self] root" in tree
        assert "nodeA" in tree

    def test_without_root_peer(self):
        mapper = HiveMapper()
        mapper.start_ping("f1")
        route = [{"source": "hub", "targets": ["nodeA"]}]
        mapper.on_ping(_make_ping("f1", "nodeA", route=route))
        tree = mapper.to_ascii()
        assert "hub" in tree
        assert "nodeA" in tree


class TestCheckFloodId:
    def test_first_call_returns_false(self):
        mapper = HiveMapper()
        assert mapper.check_flood_id("abc") is False

    def test_second_call_returns_true(self):
        mapper = HiveMapper()
        mapper.check_flood_id("abc")
        assert mapper.check_flood_id("abc") is True

    def test_empty_flood_id_always_seen(self):
        mapper = HiveMapper()
        assert mapper.check_flood_id("") is True

    def test_different_ids_both_unseen(self):
        mapper = HiveMapper()
        assert mapper.check_flood_id("a") is False
        assert mapper.check_flood_id("b") is False

    def test_fifo_eviction(self):
        mapper = HiveMapper()
        for i in range(5):
            mapper.check_flood_id(str(i), max_size=5)
        # cache full, "0" is oldest
        mapper.check_flood_id("new", max_size=5)
        assert len(mapper._seen_flood_ids) == 5
        # "0" was evicted, should be unseen now
        assert mapper.check_flood_id("0", max_size=5) is False

    def test_timestamps_recorded(self):
        mapper = HiveMapper()
        mapper.check_flood_id("abc")
        assert isinstance(mapper._seen_flood_ids["abc"], float)

    def test_clear_resets_flood_ids(self):
        mapper = HiveMapper()
        mapper.check_flood_id("abc")
        mapper.clear()
        assert mapper.check_flood_id("abc") is False


class TestTrustMarking:
    def test_mark_trusted_nodes(self):
        mapper = HiveMapper()
        mapper.nodes["peerA"] = NodeInfo(peer="peerA", public_key="KEY_A")
        mapper.nodes["peerB"] = NodeInfo(peer="peerB", public_key="KEY_B")
        mapper.nodes["peerC"] = NodeInfo(peer="peerC")  # no pubkey
        mapper.mark_trusted_nodes({"hub": "KEY_A"})
        assert mapper.nodes["peerA"].trusted is True
        assert mapper.nodes["peerB"].trusted is False
        assert mapper.nodes["peerC"].trusted is False

    def test_is_peer_trusted(self):
        mapper = HiveMapper()
        mapper.nodes["peerA"] = NodeInfo(peer="peerA", public_key="KEY_A", trusted=True)
        mapper.nodes["peerB"] = NodeInfo(peer="peerB", public_key="KEY_B")
        assert mapper.is_peer_trusted("peerA") is True
        assert mapper.is_peer_trusted("peerB") is False
        assert mapper.is_peer_trusted("unknown") is False


class TestHiveMapperClear:
    def test_clear_resets_state(self):
        mapper = HiveMapper()
        mapper.start_ping("f1")
        mapper.on_ping(_make_ping("f1", "nodeA"))
        mapper.clear()
        assert mapper.nodes == {}
        assert mapper.edges == {}
        assert mapper._seen_pings == {}
        assert mapper._seen_flood_ids == {}
