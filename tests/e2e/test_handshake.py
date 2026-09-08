"""E2E handshake tests for HiveMessageBusClient via real loopback WebSocket."""

import time

from hivemind_bus_client.client import HiveMessageBusClient
from hivemind_bus_client.identity import NodeIdentity
from hivescope import TopologyBuilder

STRONG_PASSWORD = "correct-horse-battery-staple-92"


def _make_client(url: str, key: str, password: str,
                 name: str = "test-client") -> HiveMessageBusClient:
    host, port = url.replace("ws://", "").replace("wss://", "").rstrip("/").split(":")
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


def test_client_completes_handshake_via_websocket():
    b = TopologyBuilder()
    m = b.add_master("M0", use_loopback=True)
    m.register_satellite("test-key", password=STRONG_PASSWORD)
    try:
        b.start_all()
        client = _make_client(m.network_protocol.url, "test-key", STRONG_PASSWORD)
        client.connect(site_id="loopback-site")
        client.wait_for_handshake(timeout=10)
        assert client.handshake_event.is_set()
        # encrypted session established: v2 crypto_key or v3 Noise transport
        assert client.crypto_key is not None or client.noise_transport is not None

        time.sleep(1)  # let encrypted HELLO register the peer
        peers = m.connected_peers()
        assert any("test-client" in p for p in peers), peers
        client.close()
    finally:
        b.stop_all()


def test_client_crypto_key_set_after_handshake():
    b = TopologyBuilder()
    m = b.add_master("M0", use_loopback=True)
    m.register_satellite("test-key", password=STRONG_PASSWORD)
    try:
        b.start_all()
        client = _make_client(m.network_protocol.url, "test-key", STRONG_PASSWORD)
        client.connect(site_id="loopback-site")
        client.wait_for_handshake(timeout=10)
        # encrypted session established: v2 crypto_key or v3 Noise transport
        assert client.crypto_key or client.noise_transport
        client.close()
    finally:
        b.stop_all()
