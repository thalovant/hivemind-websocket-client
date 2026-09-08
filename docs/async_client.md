# AsyncHiveMessageBusClient

An asyncio-native sibling of [`HiveMessageBusClient`](client_api.md), built
on `websockets` instead of `websocket-client`. Same protocol on the wire,
same `HiveMessage` envelopes, same handshake state machine — just
`await` everywhere.

## When to use it

Use `AsyncHiveMessageBusClient` when your application's main loop is
already asyncio: FastAPI, aiohttp, Matrix `nio`, `discord.py`,
DeltaChat-RPC, Telegram async clients, etc. The async client removes the
need to spawn a daemon thread for the HiveMind connection and bridge
back to your event loop with `asyncio.run_coroutine_threadsafe`.

Keep the sync [`HiveMessageBusClient`](client_api.md) for:

- one-shot scripts and CLIs
- any program whose main thread is already blocking somewhere
- environments without `websockets` installed (the async client is an
  optional extra)

Both clients can talk to the same HiveMind server, send the same
`HiveMessage` types, and use the same `NodeIdentity`. Pick one per
process; do not mix them on the same connection.

## Install

The async path is an optional extra so the base install stays
threading-based and small:

```bash
pip install hivemind-bus-client[async]
```

Importing `AsyncHiveMessageBusClient` from a bare install (no
`websockets`) raises a descriptive `ImportError` at instantiation. Bare
installs that never reference the async surface keep working unchanged.

## Quick start

```python
import asyncio
from ovos_bus_client.message import Message
from hivemind_bus_client import (
    AsyncHiveMessageBusClient, HiveMessage, HiveMessageType,
)


async def main():
    bus = AsyncHiveMessageBusClient(
        key="my-api-key",
        password="my-password",
        host="ws://hivemind.local",
        port=5678,
    )
    await bus.connect()       # handshake completes before this returns

    # send a Mycroft Message — automatically wrapped as a BUS HiveMessage
    await bus.emit(Message("speak", {"utterance": "hello hive"}))

    # request/reply
    reply = await bus.wait_for_response(
        HiveMessage(HiveMessageType.PING, {"flood_id": "q1"}),
        timeout=3.0,
    )
    print(reply)

    await bus.close()


asyncio.run(main())
```

## API surface

| Method | Sync client equivalent | Notes |
|---|---|---|
| `await bus.connect(bus, protocol, site_id)` | `bus.connect(...)` | Awaits handshake completion. |
| `await bus.close()` | n/a (close happens via `client.close()`) | Cancels the receive task, clears state. |
| `await bus.emit(msg, binary_type=...)` | `bus.emit(...)` | Awaits send. Accepts `MycroftMessage` or `HiveMessage`. |
| `await bus.emit_mycroft(msg)` | `bus.emit_mycroft(...)` | Convenience for BUS-wrapped Mycroft messages. |
| `await bus.wait_for_message(type, timeout)` | `bus.wait_for_message(...)` | Returns `HiveMessage` or `None`. |
| `await bus.wait_for_payload(payload_type, message_type, timeout)` | `bus.wait_for_payload(...)` | Filters by inner payload type. |
| `await bus.wait_for_mycroft(msg_type, timeout)` | `bus.wait_for_mycroft(...)` | Sugar for `wait_for_payload(message_type=BUS)`. |
| `await bus.wait_for_response(msg, reply_type, timeout)` | `bus.wait_for_response(...)` | Emits then waits. |
| `await bus.wait_for_payload_response(...)` | `bus.wait_for_payload_response(...)` | Emits then payload-filtered wait. |
| `await bus.wait_for_handshake(timeout, max_retries)` | `bus.wait_for_handshake(...)` | Async retry loop; `max_retries=None` keeps waiting through reconnects. |
| `await bus.emit_intercom(msg, pubkey)` | `bus.emit_intercom(...)` | Hybrid-encrypted INTERCOM send. |
| `bus.on(event, fn)` / `bus.once(event, fn)` / `bus.remove(event, fn)` | same | **Synchronous**, like the sync client. Keeps `HiveMindSlaveProtocol` drop-in compatible. |
| `bus.on_mycroft(msg_type, fn)` | same | Internal-bus handler registration. |

`AsyncHiveMessageWaiter` and `AsyncHivePayloadWaiter` are the
asyncio.Event-based equivalents of `HiveMessageWaiter` and
`HivePayloadWaiter`; use them when you want to set up the waiter before
emitting and then `await waiter.wait(timeout)`.

## Handshake

`await bus.connect()` performs the full HiveMind handshake (asymmetric
RSA + symmetric AES, or password-handshake fallback) before returning.
If the server is reachable but the handshake stalls, the call retries up
to `max_retries` times before raising `RuntimeError`. Reconnect after a
forced disconnect re-runs the handshake automatically on the next
`emit`.

## Binary payloads, encryption, compression

All transport-side decoding/encoding (AES-GCM, ChaCha20, bitstring framing,
zlib compression) is reused from the shared
`hivemind_bus_client.encryption` and `serialization` modules — the async
client is just a different transport, not a different protocol. The
`compress`, `binarize`, `cipher`, and `json_encoding` knobs work the
same way on both clients.

## Benchmarks

Four scripts ship under `benchmarks/`. They each answer a different
question — picking only one tells a misleading story.

### Honest summary first

**The async client is _not_ faster per call on a single connection.**
`websocket-client` is a C extension well-tuned for blocking I/O; the
pure-Python `websockets` library plus event-loop dispatch is slightly
slower per round-trip (~0.3 ms sync vs ~0.5 ms async, loopback).

**Python threads are also surprisingly competitive for I/O-bound
workloads.** The GIL releases on socket reads, so hundreds of threads
genuinely block on independent sockets in parallel. The wall-time gap
between threads-and-sync vs asyncio-and-async is small for HiveMind's
workload shape.

**Where async wins is structural, not microseconds**: setup time for
many connections, thread-count footprint, and integration with existing
asyncio code. Pick async when those matter; pick sync when your code is
already threading-based or single-shot.

### 1. In-process — library overhead only

`benchmarks/bench_async_vs_sync.py` stubs out the WebSocket. Measures
serialization, emitter dispatch, and waiter coordination only —
regression-detector, not runtime predictor.

Python 3.11, n=1500:

| Benchmark | Sync | Async | Reading |
|---|---|---|---|
| `emit()` mean | 0.42 ms | 0.58 ms | async pays coroutine-dispatch cost |
| `wait_for_message()` mean (already-set Event) | 0.004 ms | 0.020 ms | C-level `Event.is_set` beats asyncio scheduling on a degenerate case |
| async-only concurrent emit ×200 | — | ≈1 660 msg/s | path that sync structurally can't take on one client |

### 2. Real transport — single connection round-trip

`benchmarks/bench_async_vs_sync_ws.py` against a loopback `websockets`
echo server, sequential, no server-side latency. The worst case for
async — there's nothing to overlap.

Python 3.11, n=300:

| Benchmark | Sync | Async |
|---|---|---|
| Emit + round-trip mean | 0.33 ms | 0.55 ms |
| Emit + round-trip p95 | 0.44 ms | 0.74 ms |

Both well under 1 ms. Sync wins per call by ~220 µs of event-loop
overhead.

### 3. Fan-out with realistic server latency

`benchmarks/bench_fanout.py` adds a configurable server-side delay
(25 – 50 ms — what you'd see when the server actually does work) and
runs three scenarios:

- **A — sequential** (30 calls, 25 ms server delay): tied. Both pay the
  full server delay; per-call overhead is negligible vs that.
- **B — fan-out on one connection** (100 concurrent round-trips on the
  same socket): tied. Threads release the GIL on I/O so they
  interleave fine.
- **C — many independent connections** (200 connections × 10 calls):
  tied at the wall-clock level.

The honest reading: don't pick the async client expecting a wall-time
win, because for most realistic shapes there isn't one.

### 4. Resource footprint at scale

`benchmarks/bench_memory.py` — open N idle connections, hold them open,
report RSS and thread count.

Python 3.11, 1 000 idle connections:

| Metric | Sync | Async |
|---|---|---|
| RSS added | +83.7 MB (~86 KB / conn) | +79.2 MB (~81 KB / conn) |
| Threads added | **+1 000** | **+0** |
| Setup time | 1 515 ms | 1 467 ms |

This is the most honest answer to "why use async":

- **Thread count.** 1 000 connections = 1 000 threads under sync.
  ulimit pressure, scheduler overhead, debugger noise, profiler noise.
  Async runs the same 1 000 connections in a single OS thread. At
  10 000+ connections, this becomes a hard wall for sync, not a
  tradeoff.
- **Cleaner shutdown / cancellation.** `asyncio.CancelledError`
  propagates through awaits; cancelling a thread is messy. Matters for
  graceful FastAPI/aiohttp shutdown and signal handlers.
- **RSS roughly ties.** Python thread stacks are smaller than the
  textbook suggests on Linux for this workload, so memory isn't the
  differentiator. Thread count is.

### When to pick which

| Situation | Pick |
|---|---|
| Single-shot script, CLI, batch job | sync |
| Existing threading-based app | sync (don't mix concurrency models) |
| Single long-lived connection, per-call latency matters | sync (~30 % faster per round-trip) |
| FastAPI / aiohttp / Matrix `nio` / async chat bots | **async** (no thread bridge) |
| 100s – 1000s of HiveMind connections from one process | **async** (thread count) |
| Need clean cancellation across many in-flight requests | **async** (`asyncio.CancelledError`) |

### Run the benchmarks yourself

```bash
# Library overhead only — fast, no transport
python benchmarks/bench_async_vs_sync.py --n 1500

# End-to-end round-trip via a real loopback WebSocket echo server
python benchmarks/bench_async_vs_sync_ws.py --n 300

# Fan-out with realistic server-side latency
python benchmarks/bench_fanout.py --fan 100 --conns 100 --server-delay 25

# Resource footprint at scale (RSS + thread count)
python benchmarks/bench_memory.py --conns 1000
```

## Reusing existing components

The async client deliberately reuses the transport-independent pieces of
the package:

- `HiveMessage`, `HiveMessageType` ([`hivemind_bus_client/message.py`](../hivemind_bus_client/message.py))
- `NodeIdentity` ([`hivemind_bus_client/identity.py`](../hivemind_bus_client/identity.py))
- All of [`encryption.py`](../hivemind_bus_client/encryption.py) (AES-GCM / ChaCha20 / hybrid)
- All of [`serialization.py`](../hivemind_bus_client/serialization.py) (binary framing, zlib)
- `HiveMindSlaveProtocol` ([`hivemind_bus_client/protocol.py`](../hivemind_bus_client/protocol.py)) — works with both sync and async emitters via `pyee`

That keeps async-vs-sync a transport question, not a protocol fork.

## Caveats

- **Reconnect**: the async client does not auto-reconnect today. The
  receive task exits on close; supervise it from your application
  (the same shape as the sync client's responsibility-of-the-caller
  pattern).
- **Mixed clients in one process**: don't share the same emitter or
  internal bus between sync and async clients. Each owns its own.
- **`websockets` version**: the package floor is `websockets>=10`.
  Newer versions are fine; the public API used here (`websockets.connect`,
  `WebSocketClientProtocol.send/recv`, `ConnectionClosed*`) is stable.
