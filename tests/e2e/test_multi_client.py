"""Multiple HiveMessageBusClient instances against one loopback master."""

import time

from hivescope import TopologyBuilder

from hivemind_bus_client.client import HiveMessageBusClient
from hivemind_bus_client.identity import NodeIdentity

PASSWORD_A = "client-a-horse-battery-staple-92"
PASSWORD_B = "client-b-horse-battery-staple-92"
RECONNECT_PASSWORD = "reconnect-horse-battery-staple-92"


def _wait_for(predicate, timeout=15, interval=0.05):
    deadline = time.time() + timeout
    while time.time() < deadline:
        if predicate():
            return True
        time.sleep(interval)
    return False


def _make_client(url, key, password, name):
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


def test_two_clients_register_independently():
    b = TopologyBuilder()
    m = b.add_master("M0", use_loopback=True)
    m.register_satellite("key-a", password=PASSWORD_A)
    m.register_satellite("key-b", password=PASSWORD_B)
    try:
        b.start_all()
        c1 = _make_client(m.network_protocol.url, "key-a", PASSWORD_A, "client-a")
        c2 = _make_client(m.network_protocol.url, "key-b", PASSWORD_B, "client-b")
        c1.connect(site_id="site-a")
        c2.connect(site_id="site-b")
        c1.wait_for_handshake(timeout=10)
        c2.wait_for_handshake(timeout=10)
        time.sleep(1)
        peers = m.connected_peers()
        assert any("client-a" in p for p in peers), peers
        assert any("client-b" in p for p in peers), peers
        c1.close()
        c2.close()
    finally:
        b.stop_all()


def test_client_can_close_and_reconnect():
    b = TopologyBuilder()
    m = b.add_master("M0", use_loopback=True)
    m.register_satellite("k", password=RECONNECT_PASSWORD)
    try:
        b.start_all()
        c = _make_client(m.network_protocol.url, "k", RECONNECT_PASSWORD, "reconnect")
        c.connect(site_id="s1")
        c.wait_for_handshake(timeout=10)
        c.close()

        c2 = _make_client(m.network_protocol.url, "k", RECONNECT_PASSWORD, "reconnect")
        c2.connect(site_id="s2")
        c2.wait_for_handshake(timeout=10)
        # encrypted session established: v2 crypto_key or v3 Noise transport
        assert c2.crypto_key or c2.noise_transport
        c2.close()
    finally:
        b.stop_all()


def test_same_client_reconnects_after_clean_server_close():
    """A normal server close must not leave the websocket worker stopped."""
    b = TopologyBuilder()
    m = b.add_master("M0", use_loopback=True)
    m.register_satellite("k", password=RECONNECT_PASSWORD)
    client = None
    try:
        b.start_all()
        client = _make_client(
            m.network_protocol.url, "k", RECONNECT_PASSWORD, "reconnect"
        )
        client.connect(site_id="s1")
        client.wait_for_handshake(timeout=10)
        old_transport = client.noise_transport
        assert old_transport is not None

        # on_open resets the production delay to five seconds; shorten only
        # this test's next reconnect.
        client.retry = 0.01
        assert _wait_for(lambda: len(m.network_protocol.clients) == 1)
        server_connection = next(iter(m.network_protocol.clients.values()))
        server_connection.disconnect()

        assert _wait_for(
            lambda: (
                client.handshake_event.is_set()
                and client.noise_transport is not None
                and client.noise_transport is not old_transport
            )
        ), "same client did not establish a fresh session after server close"
    finally:
        if client is not None:
            client.close()
        b.stop_all()
