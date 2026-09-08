"""E2E encryption negotiation tests for HiveMessageBusClient."""

from hivemind_bus_client.client import HiveMessageBusClient
from hivemind_bus_client.identity import NodeIdentity
from hivescope import TopologyBuilder

STRONG_PASSWORD = "encryption-horse-battery-staple-92"


def _make_client(url, key, password, name="test-client"):
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


def test_client_and_master_agree_on_encryption():
    b = TopologyBuilder()
    m = b.add_master("M0", use_loopback=True)
    m.register_satellite("test-key", password=STRONG_PASSWORD)
    try:
        b.start_all()
        client = _make_client(m.network_protocol.url, "test-key", STRONG_PASSWORD)
        client.connect(site_id="loopback-site")
        client.wait_for_handshake(timeout=10)
        # the session must be encrypted: v2 AES session key or the negotiated
        # protocol v3 Noise transport (whichever version was agreed on)
        assert client.crypto_key is not None or client.noise_transport is not None
        client.close()
    finally:
        b.stop_all()
