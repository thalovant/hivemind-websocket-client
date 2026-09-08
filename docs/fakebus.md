# Fakes

`hivemind_bus_client.fakebus.AsyncFakeHiveMessageBus` is an in-process
stand-in for [`AsyncHiveMessageBusClient`](async_client.md). It mirrors
the real client's surface but does no WebSocket I/O and skips the
HiveMind handshake.

## When to use it

- **Unit tests** for code that talks to the HiveMind bus via the async
  client. No need to stand up `HiveMind-core` or stub the handshake by
  hand.
- **Embedded / runtime fallback** wherever a HiveMind client is
  expected but no server is configured. Mirrors how
  `ovos_utils.fakebus.FakeBus` is sometimes used as a default-bus
  today.

For **protocol-level end-to-end testing** (full master + satellite
topology, real handshake, real serialization), use
[hivescope](https://github.com/JarbasHiveMind/hivescope) instead.
`AsyncFakeHiveMessageBus` is the low-level primitive; hivescope is the
heavier harness built on top.

## Quick start

```python
import asyncio
from ovos_bus_client.message import Message as MycroftMessage

from hivemind_bus_client.fakebus import AsyncFakeHiveMessageBus
from hivemind_bus_client.message import HiveMessage, HiveMessageType


async def main():
    bus = AsyncFakeHiveMessageBus(
        session_id="kitchen",
        site_id="kitchen",
        useragent="test-bridge",
    )
    await bus.connect()  # no-op handshake; flips connected_event + handshake_event

    captured = []
    bus.on("speak", captured.append)            # mycroft event -> internal bus
    bus.on(HiveMessageType.BUS, lambda m: ...)  # hive event   -> emitter

    await bus.emit(MycroftMessage("speak", {"utterance": "hi"}))

    # request/reply
    reply = await bus.wait_for_response(
        HiveMessage(HiveMessageType.PING, {"flood_id": "q"}),
        timeout=1.0,
    )

    await bus.close()


asyncio.run(main())
```

## What's faked vs. what's real

| Concern | Behaviour |
|---|---|
| WebSocket | None. `connect()` is a no-op that flips `connected_event` and `handshake_event`. |
| Handshake | Skipped. `handshake_event` is set during `connect()`. |
| Encryption / binary framing | Not exercised. `emit()` dispatches the message object directly through `pyee` rather than serializing it onto a wire. |
| `HiveMessageType.BUS` routing-context injection | **Real** — `source`, `platform`, `destination`, and `session` are injected exactly as the real client does (`fakebus.py:175`), so session-aware downstream tests behave identically. |
| Internal-bus dispatch (`on("speak", ...)`) | **Real** — attaches to a real `ovos_utils.fakebus.FakeBus` (`fakebus.py:130`), just like the real client. |
| `emitted` list | Test affordance — every `HiveMessage` passed to `emit()` is appended for assertions (`fakebus.py:189`). |

## API surface

| Method | Sync/async | Behaviour |
|---|---|---|
| `connect(bus=None, protocol=None, site_id=None)` | async | No-op; sets `connected_event` and `handshake_event`. |
| `close()` | async | Clears both events. |
| `wait_for_handshake(timeout, max_retries)` | async | Returns immediately if `handshake_event` is set; otherwise raises after `timeout`. |
| `emit(message, binary_type=None)` | async | Wraps `MycroftMessage` into `HiveMessageType.BUS`, injects routing context, records in `emitted`, dispatches. |
| `emit_mycroft(message)` | async | Convenience for the `BUS`-wrap path. |
| `emit_intercom(message, pubkey)` | async | No encryption; dispatches as `HiveMessageType.INTERCOM`. |
| `wait_for_message(type, timeout)` | async | `asyncio.Event`-based wait on `emitter`. |
| `wait_for_payload(payload_type, message_type, timeout)` | async | Same wait, filtered by inner `payload.msg_type`. |
| `wait_for_mycroft(msg_type, timeout)` | async | Sugar for `wait_for_payload(message_type=BUS)`. |
| `wait_for_response(message, reply_type=None, timeout)` | async | Emit, then wait. For `MycroftMessage` queries, matches the inner payload type via `HivePayloadWaiter` semantics. |
| `on`/`once`/`remove`/`on_mycroft` | sync | Routes `HiveMessageType.*` to `emitter`, everything else to the internal Mycroft bus — matches the real `AsyncHiveMessageBusClient` contract. |

## Test affordances

Two attributes make assertions easy:

- **`bus.emitted: list[HiveMessage]`** — every message passed to
  `emit()`. Assert on counts, types, payloads, and injected routing
  context.
- **`bus.emitter`** — the `pyee.EventEmitter`. Inspect listeners,
  fire synthetic events, drive corner cases.

```python
await bus.emit(MycroftMessage("speak", {"utterance": "hi"}))

assert len(bus.emitted) == 1
assert bus.emitted[0].msg_type == HiveMessageType.BUS
assert bus.emitted[0].payload.context["session"]["session_id"] == "kitchen"
```

## When **not** to use it

- **Protocol regressions, handshake bugs, encryption changes** — the
  fake skips all of that. Use `hivescope` (real handshake) or run an
  integration test against `HiveMind-core`.
- **Binary payload framing tests** — same reason; `serialization.py`
  paths are not exercised.
- **Reconnect semantics** — the fake has no transport to drop.

## Source

- Class: `hivemind_bus_client/fakebus.py`
- Tests: `tests/test_fakebus.py`
