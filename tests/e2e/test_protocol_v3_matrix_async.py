"""Protocol v2/v3 handshake matrix for the asyncio client.

Async analogue of ``test_protocol_v3_matrix.py``, driving a REAL
websocket against a REAL hivemind-core master (via hivescope's
``TopologyBuilder``) with :class:`AsyncHiveMessageBusClient`:

- v3 async client ↔ v3 server: Noise session established, BUS messages
  round-trip both ways over the Noise transport.
- v3 async client ↔ v2 server (Noise disabled): negotiates down to the
  legacy AES handshake, BUS messages round-trip.

These reproduce the two shipped bugs that made the ``[async]`` extra
unusable against a protocol-v3 hub:

1. ``AsyncHiveMessageBusClient`` did not define ``max_protocol_version``,
   so ``HiveMindSlaveProtocol._should_use_noise`` read the ``2`` default
   and Noise was never negotiated.
2. The handshake state machine emitted its frames with a bare
   ``self.hm.emit(...)`` — a never-awaited coroutine on the async client —
   so the handshake frame never reached the wire and ``connect()`` hung
   forever.
"""

import asyncio

import pytest
from ovos_bus_client.message import Message

import hivemind_core.protocol as server_protocol
from hivemind_bus_client.async_client import AsyncHiveMessageBusClient
from hivemind_bus_client.identity import NodeIdentity
from hivemind_bus_client.message import HiveMessage, HiveMessageType
from hivescope import TopologyBuilder


async def _wait_for(condition, timeout: float = 10.0, interval: float = 0.05) -> bool:
    loop = asyncio.get_event_loop()
    deadline = loop.time() + timeout
    while loop.time() < deadline:
        if condition():
            return True
        await asyncio.sleep(interval)
    return False


def _make_async_client(url, key, password, name="v3-async-client",
                       max_protocol_version=3):
    host, port = url.replace("ws://", "").rstrip("/").split(":")
    port = int(port)
    identity = NodeIdentity()
    identity.access_key = key
    identity.password = password
    identity.default_master = f"ws://{host}"
    identity.default_port = port
    identity.name = name
    identity.site_id = f"{name}-site"
    return AsyncHiveMessageBusClient(
        key=key, password=password,
        host=f"ws://{host}", port=port,
        useragent=name, self_signed=False,
        identity=identity,
        max_protocol_version=max_protocol_version,
    )


def _master(key="async-matrix-key", password="async-matrix-pwd",
            allowed_types=("recognizer_loop:utterance",)):
    b = TopologyBuilder()
    m = b.add_master("M0", use_loopback=True)
    m.register_satellite(key, password=password,
                         allowed_types=list(allowed_types))
    return b, m


async def _assert_round_trip(client, m):
    """BUS messages cross the session in both directions."""
    seen = []
    m.agent_protocol.bus.on("recognizer_loop:utterance", seen.append)
    await client.emit(HiveMessage(
        HiveMessageType.BUS,
        payload=Message("recognizer_loop:utterance",
                        {"utterances": ["async v3 ping"]}),
    ))
    assert await _wait_for(lambda: len(seen) >= 1), "client->master BUS never arrived"
    assert seen[0].data["utterances"] == ["async v3 ping"]

    received = []
    client.on_mycroft("speak", received.append)
    peer = next(p for p in m.connected_peers())
    m.send_to_satellite(peer, HiveMessage(
        HiveMessageType.BUS,
        payload=Message("speak", {"utterance": "async pong"}),
    ))
    assert await _wait_for(lambda: len(received) >= 1), "master->client BUS never arrived"
    assert received[0].data["utterance"] == "async pong"


@pytest.mark.asyncio
async def test_async_v3_client_v3_server_noise_session_round_trip():
    b, m = _master()
    try:
        b.start_all()
        client = _make_async_client(m.network_protocol.url,
                                    "async-matrix-key", "async-matrix-pwd")
        # would hang forever before the fix
        await asyncio.wait_for(client.connect(site_id="matrix-site"), timeout=15)
        # protocol v3 negotiated: Noise transport replaces the v2 AES session
        assert client.noise_transport is not None
        assert client.crypto_key is None
        await _assert_round_trip(client, m)
        await client.close()
    finally:
        b.stop_all()


@pytest.mark.asyncio
async def test_async_v3_client_v2_server_negotiates_down_to_legacy(monkeypatch):
    # a pre-v3 server never advertises Noise support
    monkeypatch.setattr(server_protocol, "NOISE_SUPPORTED", False)
    b, m = _master()
    try:
        b.start_all()
        client = _make_async_client(m.network_protocol.url,
                                    "async-matrix-key", "async-matrix-pwd")
        await asyncio.wait_for(client.connect(site_id="matrix-site"), timeout=15)
        # legacy (v2) handshake: AES session key, no Noise transport
        assert client.crypto_key is not None
        assert client.noise_transport is None
        await _assert_round_trip(client, m)
        await client.close()
    finally:
        b.stop_all()
