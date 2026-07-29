"""HiveMessageBusClient end-to-end: messages round-trip through a real WebSocket."""

import time

from hivemind_bus_client.client import HiveMessageBusClient
from hivemind_bus_client.identity import NodeIdentity
from hivemind_bus_client.message import HiveMessage, HiveMessageType
from ovos_bus_client.message import Message

from hivescope import TopologyBuilder

ROUND_TRIP_PASSWORD = "round-trip-horse-battery-staple-92"


def _wait_for(condition, timeout: float = 5.0, interval: float = 0.05) -> bool:
    deadline = time.monotonic() + timeout
    while time.monotonic() < deadline:
        if condition():
            return True
        time.sleep(interval)
    return False


def _make_client(url, key, password, name="round-trip-client"):
    host, port = url.replace("ws://", "").rstrip("/").split(":")
    port = int(port)
    identity = NodeIdentity()
    identity.access_key = key
    identity.password = password
    identity.default_master = f"ws://{host}"
    identity.default_port = port
    identity.name = name
    identity.site_id = f"{name}-site"
    return HiveMessageBusClient(
        key=key, password=password,
        host=f"ws://{host}", port=port,
        useragent=name, self_signed=False,
        identity=identity,
    )


def test_client_emit_reaches_master_agent_bus():
    """A client.emit(BUS) is delivered to the master's agent bus."""
    b = TopologyBuilder()
    m = b.add_master("M0", use_loopback=True)
    m.register_satellite("rt-key", password=ROUND_TRIP_PASSWORD,
                         allowed_types=["recognizer_loop:utterance"])
    try:
        b.start_all()
        client = _make_client(m.network_protocol.url, "rt-key", ROUND_TRIP_PASSWORD)
        client.connect(site_id="loopback-site")
        client.wait_for_handshake(timeout=10)

        seen = []
        m.agent_protocol.bus.on("recognizer_loop:utterance", seen.append)

        client.emit(HiveMessage(
            HiveMessageType.BUS,
            payload=Message("recognizer_loop:utterance",
                            {"utterances": ["hello over the wire"]}),
        ))

        assert _wait_for(lambda: len(seen) >= 1), "BUS message did not reach master"
        assert seen[0].data["utterances"] == ["hello over the wire"]

        client.close()
    finally:
        b.stop_all()


def test_modern_ovos_topic_uses_legacy_acl_once():
    """A modern local topic crosses the legacy HiveMind BUS contract once."""
    b = TopologyBuilder()
    m = b.add_master("M0", use_loopback=True)
    m.register_satellite(
        "rt-key",
        password=ROUND_TRIP_PASSWORD,
        allowed_types=["recognizer_loop:utterance"],
    )
    try:
        b.start_all()
        client = _make_client(
            m.network_protocol.url,
            "rt-key",
            ROUND_TRIP_PASSWORD,
        )
        client.connect(site_id="loopback-site")
        client.wait_for_handshake(timeout=10)

        local = []
        remote = []
        client.on_mycroft("ovos.utterance.handle", local.append)
        m.agent_protocol.bus.on("recognizer_loop:utterance", remote.append)

        client.emit(Message(
            "ovos.utterance.handle",
            {"utterances": ["Quelle heure est-il?"], "lang": "fr-fr"},
        ))

        assert _wait_for(lambda: len(remote) == 1), (
            "modern OVOS topic did not cross the legacy HiveMind ACL"
        )
        time.sleep(0.25)
        assert len(local) == 1
        assert len(remote) == 1
        assert remote[0].data["utterances"] == ["Quelle heure est-il?"]

        client.close()
    finally:
        b.stop_all()


def test_master_to_client_bus_message():
    """A master-side send_to_satellite reaches the client's hive bus."""
    b = TopologyBuilder()
    m = b.add_master("M0", use_loopback=True)
    m.register_satellite("rt-key", password=ROUND_TRIP_PASSWORD)
    try:
        b.start_all()
        client = _make_client(m.network_protocol.url, "rt-key", ROUND_TRIP_PASSWORD,
                              name="downstream")
        client.connect(site_id="loopback-site")
        client.wait_for_handshake(timeout=10)
        time.sleep(0.5)

        received = []
        received_modern = []
        client.on_mycroft("speak", received.append)
        client.on_mycroft("ovos.utterance.speak", received_modern.append)

        peer = next(p for p in m.connected_peers() if p.startswith("downstream"))
        m.send_to_satellite(peer, HiveMessage(
            HiveMessageType.BUS,
            payload=Message("speak", {"utterance": "hi from master"}),
        ))

        assert _wait_for(lambda: len(received) >= 1), "speak never reached client"
        assert received[0].data["utterance"] == "hi from master"
        assert _wait_for(lambda: len(received_modern) >= 1), (
            "legacy HiveMind speak was not modernized for OVOS"
        )
        assert len(received_modern) == 1
        assert received_modern[0].data["utterance"] == "hi from master"
        client.close()
    finally:
        b.stop_all()
