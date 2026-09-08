#!/usr/bin/env python3
"""Memory footprint: N idle HiveMind connections, sync vs async.

The wall-time benchmarks show that for I/O-bound workloads the Python
GIL hides most of the difference between threads and asyncio. The case
where async wins decisively is **resource cost at scale** — sync needs
one OS thread per connection (each carrying its own stack); async runs
N connections in a single thread, with task overhead that is orders of
magnitude lower.

This benchmark opens N idle connections, holds them open, and reports
RSS and thread count.

Usage:
    python benchmarks/bench_memory.py --conns 200
"""
import argparse
import asyncio
import os
import resource
import socket
import threading
import time
from unittest.mock import MagicMock

import psutil
import websockets

from hivemind_bus_client.async_client import AsyncHiveMessageBusClient
from hivemind_bus_client.client import HiveMessageBusClient


def _free_port() -> int:
    with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as s:
        s.bind(("127.0.0.1", 0))
        return s.getsockname()[1]


class IdleServer:
    """Accepts and holds connections open without doing anything."""

    def __init__(self, port: int):
        self.port = port
        self._loop = None
        self._thread = None
        self._stop = None
        self._ready = threading.Event()

    async def _handle(self, ws):
        try:
            async for _ in ws:
                pass
        except websockets.ConnectionClosed:
            pass

    async def _serve(self):
        async with websockets.serve(self._handle, "127.0.0.1", self.port,
                                    max_size=2**20):
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


def _rss_mb() -> float:
    return psutil.Process(os.getpid()).memory_info().rss / 1024 / 1024


def _threads() -> int:
    return threading.active_count()


def bench_sync(port: int, n: int):
    rss_before = _rss_mb()
    threads_before = _threads()
    t0 = time.perf_counter()
    buses = [_make_sync(port) for _ in range(n)]
    elapsed = time.perf_counter() - t0
    time.sleep(0.5)  # let things settle
    rss_after = _rss_mb()
    threads_after = _threads()
    for bus in buses:
        bus.client.close()
    return rss_before, rss_after, threads_before, threads_after, elapsed


async def bench_async(port: int, n: int):
    rss_before = _rss_mb()
    threads_before = _threads()
    t0 = time.perf_counter()
    buses = await asyncio.gather(*[_make_async(port) for _ in range(n)])
    elapsed = time.perf_counter() - t0
    await asyncio.sleep(0.5)
    rss_after = _rss_mb()
    threads_after = _threads()
    for bus in buses:
        await bus.close()
    return rss_before, rss_after, threads_before, threads_after, elapsed


def main():
    ap = argparse.ArgumentParser(description=__doc__)
    ap.add_argument("--conns", type=int, default=200)
    args = ap.parse_args()

    port = _free_port()
    server = IdleServer(port)
    server.start()
    print(f"\nIdle server: ws://127.0.0.1:{port}/\n")
    print("=" * 80)
    print(f"  Resource footprint at {args.conns} idle connections")
    print("=" * 80 + "\n")

    try:
        # Sync first (clean slate)
        rb, ra, tb, ta, el = bench_sync(port, args.conns)
        print(f"  sync   {args.conns} conns: "
              f"RSS {rb:6.1f} → {ra:6.1f} MB "
              f"(+{ra - rb:5.1f}, ~{(ra - rb) * 1024 / args.conns:.0f} KB/conn)  "
              f"threads {tb} → {ta} (+{ta - tb})  "
              f"setup={el * 1000:.0f} ms")
        time.sleep(1)

        rb, ra, tb, ta, el = asyncio.run(bench_async(port, args.conns))
        print(f"  async  {args.conns} conns: "
              f"RSS {rb:6.1f} → {ra:6.1f} MB "
              f"(+{ra - rb:5.1f}, ~{(ra - rb) * 1024 / args.conns:.0f} KB/conn)  "
              f"threads {tb} → {ta} (+{ta - tb})  "
              f"setup={el * 1000:.0f} ms")

        print()
        print("=" * 80)
        print("Reading: lower RSS-per-conn and fewer added threads = better")
        print("scaling. The difference is structural; thread stacks (~8 KB")
        print("minimum) and per-thread bookkeeping vs asyncio task overhead.")
        print("=" * 80)
    finally:
        server.stop()


if __name__ == "__main__":
    main()
