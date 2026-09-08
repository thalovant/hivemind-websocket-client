#!/usr/bin/env python3
"""In-process benchmark: sync HiveMessageBusClient vs async AsyncHiveMessageBusClient.

Measures pure library overhead (no real WebSocket): emit path, on_message
dispatch, waiter/event coordination, message + HiveMessage serialization.
The websocket attribute is stubbed out so emit() exercises the framing and
emitter code but doesn't actually transmit.

Use it to catch library-level regressions. For end-to-end latency see
`bench_async_vs_sync_ws.py`.

Usage:
    python benchmarks/bench_async_vs_sync.py
    python benchmarks/bench_async_vs_sync.py --n 5000
"""
import argparse
import asyncio
import statistics
import time
from unittest.mock import AsyncMock, MagicMock

from hivemind_bus_client.async_client import AsyncHiveMessageBusClient
from hivemind_bus_client.message import HiveMessage, HiveMessageType


def _stats(times, label):
    times_ms = [t * 1000 for t in times]
    print(
        f"  {label:50s}  min={min(times_ms):.3f}ms  "
        f"mean={statistics.mean(times_ms):.3f}ms  "
        f"median={statistics.median(times_ms):.3f}ms  "
        f"max={max(times_ms):.3f}ms"
    )
    return statistics.mean(times)


# ---------------------------------------------------------------------------
# Stub-bus factories
# ---------------------------------------------------------------------------

def _async_bus():
    bus = AsyncHiveMessageBusClient.__new__(AsyncHiveMessageBusClient)
    from pyee import EventEmitter
    from ovos_utils.fakebus import FakeBus
    ident = MagicMock()
    ident.access_key = "k"
    ident.password = "p"
    ident.name = "bench"
    ident.site_id = "bench-site"
    bus.identity = ident
    bus.session_id = "bench"
    bus.connected_event = asyncio.Event()
    bus.connected_event.set()
    bus.handshake_event = asyncio.Event()
    bus.handshake_event.set()
    bus.emitter = EventEmitter()
    bus.internal_bus = FakeBus()
    bus.crypto_key = None
    bus.compress = False
    bus.binarize = False
    bus.cipher = "AES_GCM"
    bus.json_encoding = "JSON_HEX"
    bus.protocol = MagicMock(binarize=False)
    bus._ws = AsyncMock()
    return bus


def _sync_bus():
    """Construct a sync HiveMessageBusClient by-passing __init__."""
    from hivemind_bus_client.client import HiveMessageBusClient
    bus = HiveMessageBusClient.__new__(HiveMessageBusClient)
    from pyee import EventEmitter
    from ovos_utils.fakebus import FakeBus
    from threading import Event
    ident = MagicMock()
    ident.access_key = "k"
    ident.password = "p"
    ident.name = "bench"
    ident.site_id = "bench-site"
    bus.identity = ident
    bus.bin_callbacks = MagicMock()
    bus.emitter = EventEmitter()
    bus.internal_bus = FakeBus()
    bus.session_id = "bench"
    bus.crypto_key = None
    bus.compress = False
    bus.binarize = False
    bus.cipher = "AES_GCM"
    bus.json_encoding = "JSON_HEX"
    bus.protocol = MagicMock(binarize=False)
    bus.connected_event = Event()
    bus.connected_event.set()
    bus.handshake_event = Event()
    bus.handshake_event.set()
    bus.started_running = True
    bus.client = MagicMock()
    return bus


# ---------------------------------------------------------------------------
# Benchmarks
# ---------------------------------------------------------------------------

def bench_sync_emit(n):
    from ovos_bus_client.message import Message as MycroftMessage
    bus = _sync_bus()
    times = []
    for i in range(n):
        msg = MycroftMessage("bench.emit", {"i": i})
        t0 = time.perf_counter()
        bus.emit(msg)
        times.append(time.perf_counter() - t0)
    return _stats(times, f"sync HiveMessageBusClient.emit ×{n}")


async def bench_async_emit(n):
    from ovos_bus_client.message import Message as MycroftMessage
    bus = _async_bus()
    times = []
    for i in range(n):
        msg = MycroftMessage("bench.emit", {"i": i})
        t0 = time.perf_counter()
        await bus.emit(msg)
        times.append(time.perf_counter() - t0)
    return _stats(times, f"async AsyncHiveMessageBusClient.emit ×{n}")


def bench_sync_wait_for_message(n):
    from hivemind_bus_client.client import HiveMessageWaiter
    bus = _sync_bus()
    times = []
    for _ in range(n):
        waiter = HiveMessageWaiter(bus, HiveMessageType.PING)
        msg = HiveMessage(HiveMessageType.PING, {"flood_id": "x"})
        bus.emitter.emit(HiveMessageType.PING, msg)
        t0 = time.perf_counter()
        waiter.wait(timeout=1.0)
        times.append(time.perf_counter() - t0)
    return _stats(times, f"sync wait_for_message ×{n}")


async def bench_async_wait_for_message(n):
    from hivemind_bus_client.async_client import AsyncHiveMessageWaiter
    bus = _async_bus()
    times = []
    for _ in range(n):
        waiter = AsyncHiveMessageWaiter(bus, HiveMessageType.PING)
        msg = HiveMessage(HiveMessageType.PING, {"flood_id": "x"})
        bus.emitter.emit(HiveMessageType.PING, msg)
        t0 = time.perf_counter()
        await waiter.wait(timeout=1.0)
        times.append(time.perf_counter() - t0)
    return _stats(times, f"async wait_for_message ×{n}")


async def bench_async_concurrent_emit(rounds, concurrency):
    from ovos_bus_client.message import Message as MycroftMessage
    bus = _async_bus()
    times = []
    for _ in range(rounds):
        msgs = [MycroftMessage(f"bench.fan.{i}", {"i": i})
                for i in range(concurrency)]
        t0 = time.perf_counter()
        await asyncio.gather(*[bus.emit(m) for m in msgs])
        times.append(time.perf_counter() - t0)
    _stats(times, f"async concurrent emit ×{concurrency} (×{rounds} rounds)")
    total = sum(times)
    total_msgs = rounds * concurrency
    print(f"    => throughput: {total_msgs / total:.0f} msg/s")


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--n", type=int, default=2000)
    ap.add_argument("--concurrency", type=int, default=200)
    args = ap.parse_args()

    print(f"\n{'=' * 70}")
    print(f"  hivemind-bus-client sync vs async (in-process, n={args.n})")
    print(f"{'=' * 70}\n")

    print("── 1. emit() ──────────────────────────────────────────────────────")
    sync_emit = bench_sync_emit(args.n)
    async_emit = asyncio.run(bench_async_emit(args.n))
    print(f"  ratio async/sync: {async_emit / sync_emit:.2f}x\n")

    print("── 2. wait_for_message() ──────────────────────────────────────────")
    sync_wait = bench_sync_wait_for_message(args.n)
    async_wait = asyncio.run(bench_async_wait_for_message(args.n))
    print(f"  ratio async/sync: {async_wait / sync_wait:.2f}x\n")

    print("── 3. concurrent emit (fan-out) ───────────────────────────────────")
    asyncio.run(bench_async_concurrent_emit(
        rounds=max(args.n // 10, 10), concurrency=args.concurrency,
    ))
    print()
    print("=" * 70)


if __name__ == "__main__":
    main()
