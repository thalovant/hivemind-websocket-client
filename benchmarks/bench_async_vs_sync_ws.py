#!/usr/bin/env python3
"""End-to-end benchmark vs a real WebSocket echo server.

Loopback WebSocket only, no HiveMind handshake — we want to compare
raw transport latency between sync and async clients on the same wire.
Both clients have ``crypto_key`` left unset (no encryption) so the
benchmark measures: serialization, framing, websocket send/recv, and
emitter dispatch.

Use it to validate that the async client is at least competitive on
real I/O, where the GIL/threads costs of the sync client start to matter.

Usage:
    python benchmarks/bench_async_vs_sync_ws.py
    python benchmarks/bench_async_vs_sync_ws.py --n 500
"""
import argparse
import asyncio
import json
import socket
import statistics
import threading
import time
from unittest.mock import MagicMock

import websockets

from hivemind_bus_client.async_client import (AsyncHiveMessageBusClient,
                                              AsyncHiveMessageWaiter)
from hivemind_bus_client.client import (HiveMessageBusClient,
                                        HiveMessageWaiter)
from hivemind_bus_client.message import HiveMessage, HiveMessageType


def _free_port() -> int:
    with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as s:
        s.bind(("127.0.0.1", 0))
        return s.getsockname()[1]


# ---------------------------------------------------------------------------
# Echo server: receive a HiveMessage JSON, return one with `flood_id` set so
# the receiver's emitter sees a `PING` and waiters fire.
# ---------------------------------------------------------------------------

class EchoServer:
    def __init__(self, port: int):
        self.port = port
        self._loop = None
        self._thread = None
        self._stop = None
        self._ready = threading.Event()

    async def _handle(self, ws):
        try:
            async for raw in ws:
                try:
                    if isinstance(raw, bytes):
                        # echo binary verbatim
                        await ws.send(raw)
                        continue
                    obj = json.loads(raw)
                    obj["msg_type"] = HiveMessageType.PING
                    await ws.send(json.dumps(obj))
                except Exception:
                    pass
        except websockets.ConnectionClosed:
            pass

    async def _serve(self):
        async with websockets.serve(self._handle, "127.0.0.1", self.port):
            self._stop = asyncio.Event()
            self._ready.set()
            await self._stop.wait()

    def start(self):
        def _run():
            self._loop = asyncio.new_event_loop()
            asyncio.set_event_loop(self._loop)
            self._loop.run_until_complete(self._serve())
        self._thread = threading.Thread(target=_run, daemon=True)
        self._thread.start()
        self._ready.wait(timeout=5)

    def stop(self):
        if self._stop and self._loop:
            self._loop.call_soon_threadsafe(self._stop.set)
        if self._thread:
            self._thread.join(timeout=5)


# ---------------------------------------------------------------------------
# Pre-connected client factories (bypass handshake; identity is mocked)
# ---------------------------------------------------------------------------

def _stub_identity(port):
    ident = MagicMock()
    ident.access_key = "bench"
    ident.password = "bench"
    ident.default_master = f"ws://127.0.0.1"
    ident.default_port = port
    ident.name = "bench"
    ident.site_id = "bench-site"
    ident.private_key = ""
    return ident


def _make_sync(port: int) -> HiveMessageBusClient:
    from threading import Event
    from pyee import EventEmitter
    from ovos_utils.fakebus import FakeBus
    bus = HiveMessageBusClient.__new__(HiveMessageBusClient)
    bus.identity = _stub_identity(port)
    bus.bin_callbacks = MagicMock()
    bus.json_encoding = "JSON_HEX"
    bus.cipher = "AES_GCM"
    bus.crypto_key = None
    bus.allow_self_signed = True
    bus.share_bus = False
    bus.compress = False
    bus.binarize = False
    bus.emitter = EventEmitter()
    bus.internal_bus = FakeBus()
    bus.session_id = "bench"
    bus.connected_event = Event()
    bus.handshake_event = Event()
    bus.handshake_event.set()
    bus.protocol = MagicMock(binarize=False)
    bus.started_running = True
    # build a real WebSocketApp targeting our echo server
    from websocket import WebSocketApp
    url = f"ws://127.0.0.1:{port}?bench=1"
    bus.client = WebSocketApp(
        url,
        on_open=bus.on_open, on_close=bus.on_close,
        on_error=bus.on_error, on_message=bus.on_message,
    )

    # spin up run_forever in a background thread
    def _run():
        bus.client.run_forever()
    threading.Thread(target=_run, daemon=True).start()
    # wait for open
    if not bus.connected_event.wait(timeout=2):
        raise RuntimeError("sync client did not connect")
    return bus


async def _make_async(port: int) -> AsyncHiveMessageBusClient:
    from pyee import EventEmitter
    from ovos_utils.fakebus import FakeBus
    bus = AsyncHiveMessageBusClient.__new__(AsyncHiveMessageBusClient)
    bus.identity = _stub_identity(port)
    bus.bin_callbacks = MagicMock()
    bus.json_encoding = "JSON_HEX"
    bus.cipher = "AES_GCM"
    bus.crypto_key = None
    bus.allow_self_signed = True
    bus.share_bus = False
    bus.compress = False
    bus.binarize = False
    bus.emitter = EventEmitter()
    bus.internal_bus = FakeBus()
    bus.session_id = "bench"
    bus.connected_event = asyncio.Event()
    bus.handshake_event = asyncio.Event()
    bus.handshake_event.set()
    bus.protocol = MagicMock(binarize=False)
    bus._ws = await websockets.connect(f"ws://127.0.0.1:{port}?bench=1")
    bus.connected_event.set()
    bus._receive_task = asyncio.create_task(bus._receive_loop())
    return bus


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def _stats(times, label):
    ms = [t * 1000 for t in times]
    p95 = sorted(ms)[int(len(ms) * 0.95)]
    print(
        f"  {label:50s}  min={min(ms):.2f}ms  "
        f"mean={statistics.mean(ms):.2f}ms  "
        f"median={statistics.median(ms):.2f}ms  "
        f"p95={p95:.2f}ms  "
        f"max={max(ms):.2f}ms"
    )


# ---------------------------------------------------------------------------
# Benchmarks
# ---------------------------------------------------------------------------

def bench_sync(port, n):
    bus = _make_sync(port)

    # round-trip: emit a PING, wait for echo
    times = []
    for i in range(n):
        waiter = HiveMessageWaiter(bus, HiveMessageType.PING)
        t0 = time.perf_counter()
        bus.emit(HiveMessage(HiveMessageType.PING, {"flood_id": str(i)}))
        waiter.wait(timeout=2.0)
        times.append(time.perf_counter() - t0)
    _stats(times, f"sync emit + round-trip ×{n}")

    bus.client.close()


async def bench_async(port, n):
    bus = await _make_async(port)

    # round-trip: emit a PING, wait for echo
    times = []
    for i in range(n):
        waiter = AsyncHiveMessageWaiter(bus, HiveMessageType.PING)
        t0 = time.perf_counter()
        await bus.emit(HiveMessage(HiveMessageType.PING, {"flood_id": str(i)}))
        await waiter.wait(timeout=2.0)
        times.append(time.perf_counter() - t0)
    _stats(times, f"async emit + round-trip ×{n}")

    # fan-out: async-only — N round-trips in parallel
    async def one_rt(i):
        waiter = AsyncHiveMessageWaiter(bus, HiveMessageType.PING)
        await bus.emit(HiveMessage(HiveMessageType.PING, {"flood_id": f"fan-{i}"}))
        await waiter.wait(timeout=5.0)

    t0 = time.perf_counter()
    await asyncio.gather(*[one_rt(i) for i in range(n)])
    total = time.perf_counter() - t0
    print(f"  async fan-out × {n:<5}                            "
          f"total={total * 1000:.0f}ms  throughput={n / total:.0f} req/s")

    await bus.close()


def main():
    ap = argparse.ArgumentParser(description=__doc__)
    ap.add_argument("--n", type=int, default=500)
    args = ap.parse_args()

    port = _free_port()
    server = EchoServer(port)
    server.start()
    print(f"\nWebSocket echo server: ws://127.0.0.1:{port}/\n")

    try:
        print("=" * 70)
        print(f"  Real-transport benchmark (n={args.n}, port={port})")
        print("=" * 70 + "\n")

        print("── sync HiveMessageBusClient ───────────────────────────────────")
        bench_sync(port, args.n)
        print()

        print("── async AsyncHiveMessageBusClient ─────────────────────────────")
        asyncio.run(bench_async(port, args.n))
        print()
    finally:
        server.stop()


if __name__ == "__main__":
    main()
