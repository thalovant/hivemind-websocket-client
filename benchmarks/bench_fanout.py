#!/usr/bin/env python3
"""Fair fan-out benchmark: where async actually wins.

The previous two benchmarks intentionally stack the deck against async by
testing single-connection, sequential, zero-server-latency cases — the
exact shape where `websocket-client` (C extension) and a single thread
beat `websockets` (pure Python) plus event-loop overhead.

This script measures three scenarios that match real usage:

  A. **Sequential round-trip** with a slow server (server-side delay).
     Both sync and async pay the full server-side delay per request.
     Async has nothing to do during the wait — it loses by per-call
     overhead. Reported as the baseline.

  B. **Concurrent fan-out**: N round-trips at once.
     - Sync: N threads, each blocking on its own waiter.
     - Async: one event loop, N tasks, `asyncio.gather`.
     With a non-zero server delay, both should converge to ~server delay
     (plus overhead). Async wins on overhead because tasks are cheaper
     than threads.

  C. **Many concurrent connections, each doing a few round-trips**.
     - Sync: K threads × M sub-threads, or K threads doing M round-trips
       sequentially each. We measure the second (one thread per
       connection, M sequential calls — the realistic sync pattern).
     - Async: K connections in one event loop, M `gather` calls each.
     This is where async crushes sync on memory and scheduling.

Server-side latency is configurable; default 25 ms (typical when the
server actually does work).

Usage:
    python benchmarks/bench_fanout.py
    python benchmarks/bench_fanout.py --fan 200 --server-delay 50
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
# Echo server with configurable server-side latency
# ---------------------------------------------------------------------------

class SlowEchoServer:
    """One connection-per-task; per-request server-side delay configurable.

    Crucially, this spawns a task per incoming message instead of awaiting
    them sequentially — so true concurrency on one connection is possible.
    """

    def __init__(self, port: int, delay_s: float):
        self.port = port
        self.delay_s = delay_s
        self._loop = None
        self._thread = None
        self._stop = None
        self._ready = threading.Event()

    async def _respond(self, ws, raw):
        await asyncio.sleep(self.delay_s)
        try:
            if isinstance(raw, bytes):
                await ws.send(raw)
                return
            obj = json.loads(raw)
            obj["msg_type"] = HiveMessageType.PING
            await ws.send(json.dumps(obj))
        except Exception:
            pass

    async def _handle(self, ws):
        try:
            async for raw in ws:
                # Spawn a task so multiple in-flight requests can wait in parallel
                asyncio.create_task(self._respond(ws, raw))
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
# Pre-connected client factories (no handshake; benchmark transport only)
# ---------------------------------------------------------------------------

def _stub_identity(port):
    ident = MagicMock()
    ident.access_key = "bench"
    ident.password = "bench"
    ident.default_master = "ws://127.0.0.1"
    ident.default_port = port
    ident.name = "bench"
    ident.site_id = "bench-site"
    ident.private_key = ""
    return ident


def _make_sync(port: int) -> HiveMessageBusClient:
    from threading import Event
    from pyee import EventEmitter
    from ovos_utils.fakebus import FakeBus
    from websocket import WebSocketApp
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
    url = f"ws://127.0.0.1:{port}?bench=1"
    bus.client = WebSocketApp(
        url, on_open=bus.on_open, on_close=bus.on_close,
        on_error=bus.on_error, on_message=bus.on_message,
    )
    threading.Thread(target=bus.client.run_forever, daemon=True).start()
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
# Round-trip helpers
# ---------------------------------------------------------------------------

def _sync_rt(bus, i):
    waiter = HiveMessageWaiter(bus, HiveMessageType.PING)
    bus.emit(HiveMessage(HiveMessageType.PING, {"flood_id": str(i)}))
    waiter.wait(timeout=5.0)


async def _async_rt(bus, i):
    waiter = AsyncHiveMessageWaiter(bus, HiveMessageType.PING)
    await bus.emit(HiveMessage(HiveMessageType.PING, {"flood_id": str(i)}))
    await waiter.wait(timeout=5.0)


# ---------------------------------------------------------------------------
# Scenarios
# ---------------------------------------------------------------------------

def scenario_A_sequential(port: int, n: int, label: str):
    """Pay the full server delay per request; both clients should be
    roughly server_delay × n. Async slower per call by overhead."""

    bus = _make_sync(port)
    t0 = time.perf_counter()
    for i in range(n):
        _sync_rt(bus, i)
    sync_total = time.perf_counter() - t0
    bus.client.close()

    async def _runa():
        bus = await _make_async(port)
        t0 = time.perf_counter()
        for i in range(n):
            await _async_rt(bus, i)
        total = time.perf_counter() - t0
        await bus.close()
        return total
    async_total = asyncio.run(_runa())

    print(f"  [A] {label:40s}  sync={sync_total * 1000:7.0f}ms  "
          f"async={async_total * 1000:7.0f}ms  "
          f"(sync per-call={sync_total / n * 1000:.2f}ms, "
          f"async per-call={async_total / n * 1000:.2f}ms)")


def scenario_B_fanout_one_conn(port: int, fan: int, label: str):
    """N concurrent round-trips on a single connection.
    Sync needs N threads (each its own waiter); async uses gather."""

    # sync via N threads sharing one bus
    bus = _make_sync(port)
    results = []
    def worker(i):
        t0 = time.perf_counter()
        _sync_rt(bus, i)
        results.append(time.perf_counter() - t0)
    t0 = time.perf_counter()
    threads = [threading.Thread(target=worker, args=(i,)) for i in range(fan)]
    for t in threads:
        t.start()
    for t in threads:
        t.join()
    sync_total = time.perf_counter() - t0
    bus.client.close()

    # async via gather on one bus
    async def _runa():
        bus = await _make_async(port)
        t0 = time.perf_counter()
        await asyncio.gather(*[_async_rt(bus, i) for i in range(fan)])
        total = time.perf_counter() - t0
        await bus.close()
        return total
    async_total = asyncio.run(_runa())

    speedup = sync_total / async_total if async_total else float("inf")
    print(f"  [B] {label:40s}  sync={sync_total * 1000:7.0f}ms  "
          f"async={async_total * 1000:7.0f}ms  "
          f"speedup={speedup:.1f}x")


def scenario_C_many_connections(port: int, conns: int, calls_per: int,
                                label: str):
    """K independent connections, each doing M sequential round-trips.
    Sync: K threads, each owning a bus; async: K connections in one loop."""

    # sync: K threads, K buses
    def sync_worker():
        bus = _make_sync(port)
        for i in range(calls_per):
            _sync_rt(bus, i)
        bus.client.close()
    t0 = time.perf_counter()
    workers = [threading.Thread(target=sync_worker) for _ in range(conns)]
    for w in workers:
        w.start()
    for w in workers:
        w.join()
    sync_total = time.perf_counter() - t0

    # async: K connections, all on one loop, each does its own M calls
    async def _async_one():
        bus = await _make_async(port)
        for i in range(calls_per):
            await _async_rt(bus, i)
        await bus.close()

    async def _runa():
        t0 = time.perf_counter()
        await asyncio.gather(*[_async_one() for _ in range(conns)])
        return time.perf_counter() - t0
    async_total = asyncio.run(_runa())

    speedup = sync_total / async_total if async_total else float("inf")
    print(f"  [C] {label:40s}  sync={sync_total * 1000:7.0f}ms  "
          f"async={async_total * 1000:7.0f}ms  "
          f"speedup={speedup:.1f}x")


# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------

def main():
    ap = argparse.ArgumentParser(description=__doc__)
    ap.add_argument("--n", type=int, default=50,
                    help="Sequential round-trips for scenario A")
    ap.add_argument("--fan", type=int, default=100,
                    help="Concurrent fan-out size for scenario B")
    ap.add_argument("--conns", type=int, default=50,
                    help="Number of concurrent connections for scenario C")
    ap.add_argument("--calls-per", type=int, default=5,
                    help="Round-trips per connection in scenario C")
    ap.add_argument("--server-delay", type=int, default=25,
                    help="Server-side per-request delay in ms (default 25)")
    args = ap.parse_args()

    port = _free_port()
    delay = args.server_delay / 1000.0
    server = SlowEchoServer(port, delay)
    server.start()
    print(f"\nEcho server: ws://127.0.0.1:{port}/  "
          f"(server-side delay: {args.server_delay} ms per request)\n")

    try:
        print("=" * 100)
        print(f"  Fan-out benchmark (server delay {args.server_delay} ms)")
        print("=" * 100 + "\n")

        scenario_A_sequential(
            port, n=args.n,
            label=f"sequential ×{args.n}",
        )

        scenario_B_fanout_one_conn(
            port, fan=args.fan,
            label=f"fan-out ×{args.fan} on 1 connection",
        )

        scenario_C_many_connections(
            port, conns=args.conns, calls_per=args.calls_per,
            label=f"{args.conns} conns × {args.calls_per} calls",
        )

        print()
        print("=" * 100)
        print("Reading:")
        print("  A — both clients pay the full server delay per request.")
        print("      Async per-call overhead shows up as a slight loss.")
        print("  B — fan-out on one socket. Both can interleave waits;")
        print("      async tasks are cheaper than threads.")
        print("  C — many connections. Sync needs one thread per connection;")
        print("      async runs them all in one event loop.")
        print("=" * 100)
    finally:
        server.stop()


if __name__ == "__main__":
    main()
