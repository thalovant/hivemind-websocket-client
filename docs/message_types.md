
HiveMind communication is based on the `HiveMessage` class (defined in `hivemind_bus_client.message.HiveMessage`), which wraps standard AI bus messages with HiveMind-specific metadata and routing instructions.

## `HiveMessage` Fields

- **`msg_type`**: A value from the `HiveMessageType` enum.
- **`payload`**: The actual message (an OVOS `Message` for BUS types, `bytes` for BIN types).
- **`context`**: Metadata used for routing and tracking (optional).

## `HiveMessageType` (The Routing Modes)

The `msg_type` (defined in `hivemind_bus_client.message.HiveMessageType`) dictates how the Mind should handle the message.

| Type | Purpose | Use Case |
|---|---|---|
| **`BUS`** | Standard message for the AI | Utterances, intent triggers, speak events. |
| **`BIN`** | Raw binary data | Audio streams for STT/TTS, file transfers. |
| **`ESCALATE`** | Upstream request | Used by a Slave Mind to ask a Master Mind for help. |
| **`BROADCAST`** | Downstream flood (admin only) | Master pushes a message to all connected satellites. |
| **`PROPAGATE`** | Bidirectional flood | Forwards to all peers in both directions. |
| **`INTERCOM`** | End-to-end hybrid-encrypted | AES-GCM payload + RSA-encrypted ephemeral key. Only trusted peers or explicit targets are injected. |
| **`QUERY`** | Request-response upstream | Like ESCALATE, but first answering node sends a response back. Stops propagation on answer. |
| **`CASCADE`** | Request-response flood | Like PROPAGATE, but expects responses from ALL nodes. Supports disambiguation. |
| **`PING`** | Network discovery flood | Each node responds with its own PING (same `flood_id`). Carried inside PROPAGATE. Route metadata = hive path. |
| **`HELLO`** | Node announcement | Session sync at connection time. |
| **`HANDSHAKE`** | Crypto negotiation | Key exchange at connection time. |
| **`THIRDPRTY`** | User-land custom | Application-defined payload; HiveMind relays without interpretation. |

## QUERY — First-Match Request-Response

QUERY propagates upstream like ESCALATE, but stops as soon as one node can respond.

**Satellite behaviour** (`HiveMindSlaveProtocol.handle_query` — `protocol.py:311`):
- Inner payload must be `BUS` or `INTERCOM`.
- `BUS` payloads are dispatched to `handle_bus`.
- `INTERCOM` payloads are dispatched to `handle_intercom`.

### Sending a QUERY (from satellite)

```python
from hivemind_bus_client.message import HiveMessage, HiveMessageType
from ovos_bus_client.message import Message

inner = HiveMessage(HiveMessageType.BUS,
                    Message("intent.request", {"utterance": "what time is it"}))
query = HiveMessage(HiveMessageType.QUERY, payload=inner)
client.emit(query)
```

### Listening for QUERY responses (decorator)

```python
from hivemind_bus_client.decorators import on_query

@on_query("speak", bus)
def on_speak(msg):
    print(msg.data["utterance"])
```

## CASCADE — Collect-All Request-Response

CASCADE propagates like PROPAGATE (bidirectional flood) but expects responses from all reachable nodes. Responses are optional — nodes that cannot answer simply stay silent.

**Satellite behaviour** (`HiveMindSlaveProtocol.handle_cascade` — `protocol.py:436`):

Responses are buffered in a `CascadeAggregator` (`protocol.py:21`). After `cascade_timeout` seconds (default 5.0) **or** when the number of responses reaches the known node count from `hive_mapper`, the `cascade_select_callback` picks the best response and emits it on the internal bus.

- Inner payload must be `BUS` or `INTERCOM`.
- Default select callback returns the first response.
- Set `cascade_select_callback` on the protocol to provide custom disambiguation.
- Set `hive_mapper` to enable early resolution when all nodes have responded.

### Sending a CASCADE (from satellite)

```python
inner = HiveMessage(HiveMessageType.BUS,
                    Message("skill.list.request", {}))
cascade = HiveMessage(HiveMessageType.CASCADE, payload=inner)
client.emit(cascade)
```

### Custom disambiguation

```python
from hivemind_bus_client.protocol import HiveMindSlaveProtocol
from hivemind_bus_client.hive_map import HiveMapper

def pick_best(responses):
    # custom logic — e.g. highest confidence, specific node, etc.
    return responses[0]

proto.cascade_select_callback = pick_best
proto.hive_mapper = mapper  # enables early resolution
```

### Listening for CASCADE responses (decorator)

```python
from hivemind_bus_client.decorators import on_cascade

@on_cascade("skill.list.response", bus)
def on_skills(msg):
    print(msg.data["skills"])
```

## PING Flood — Network Discovery

PING messages are **always the inner payload of a PROPAGATE message**. They are never sent bare. Each node that receives a PING responds with its own PING carrying the same `flood_id`. The `flood_id` prevents infinite loops (tracked via `HiveMapper.check_flood_id`). PONG is no longer used.

### PING Payload Fields

| Field | Type | Description |
|-------|------|-------------|
| `flood_id` | `str` | UUID preventing infinite loops |
| `peer` | `str` | `{name}::{session_id}` identifier |
| `site_id` | `str` | Location identifier |
| `timestamp` | `float` | Sender's clock for RTT estimation |
| `public_key` | `str?` | RSA public key — enables trust verification via `HiveMapper.mark_trusted_nodes` |
| `lang` | `str?` | Node's locale (e.g. `"en-us"`) — enables localized INTERCOM communication |

### Sending a PING

```python
import time, uuid
from hivemind_bus_client.message import HiveMessage, HiveMessageType

flood_id = str(uuid.uuid4())
ping_inner = HiveMessage(
    HiveMessageType.PING,
    payload={
        "flood_id":  flood_id,
        "timestamp": time.time(),
        "peer":      client.peer,
        "site_id":   client.site_id,
        "public_key": identity.public_key,
        "lang":      "en-us",
    }
)
ping_outer = HiveMessage(HiveMessageType.PROPAGATE, payload=ping_inner)
client.emit(ping_outer)
```

### Receiving responsive PINGs

```python
def on_ping(message: HiveMessage) -> None:
    payload = message.payload          # inner PING dict
    route   = message.route            # List[{source, targets}] — the hive path
    print(f"PING from {payload['peer']} via {len(route)} hops")

client.on(HiveMessageType.PING, on_ping)
```

For automated topology collection use `HiveMapper` from `hivemind_core.hive_map`.

---

## Route Metadata

Every `HiveMessage` has a `route` field: `List[Dict[str, Any]]` — an ordered list of hops tracking the network path.

### Hop Structure

Each hop is a dict: `{"source": "peer_id", "targets": ["peer_id1", "peer_id2"]}`.

- `source`: the peer that forwarded the message at this hop
- `targets`: the peers the message was sent to from this hop

### Multi-Hop Example

After S0 → R1 → M0 traversal:

```python
message.route == [
    {"source": "S0_peer_id", "targets": ["R1_peer_id"]},
    {"source": "R1_peer_id", "targets": ["M0_peer_id"]},
]
```

### Route API

```python
# Record a hop (called automatically by protocol layer)
message.update_hop_data()

# Replace route (used when transferring between wrapper/inner messages)
message.replace_route(other_message.route)

# Read route (filters incomplete hops)
for hop in message.route:
    print(f"{hop['source']} → {hop['targets']}")
```

### Serialization

Route survives `as_dict()` → `deserialize()` roundtrips. The `route` field is included in JSON serialization and restored on deserialization (`message.py:208-231`).

---

## Serialization and Encryption

Before being sent over the network, `HiveMessage` objects are:
1. **Serialized**: Using functions in `hivemind_bus_client.serialization` (`get_bitstring`, `decode_bitstring`).
2. **Encrypted**: Using AES-256-GCM via functions in `hivemind_bus_client.encryption`.
3. **Encoded**: Frequently using Z85+Base91 for safe text transport via `hivemind_bus_client.encodings`.

### Examples

#### Sending a Standard Bus Message
```python
from hivemind_bus_client.message import HiveMessage, HiveMessageType
from ovos_bus_client.message import Message

hive_msg = HiveMessage(HiveMessageType.BUS, 
                       Message("mycroft.stop"))
```

#### Sending Binary Audio Data
```python
audio_bytes = b"..." # Raw PCM audio
hive_msg = HiveMessage(HiveMessageType.BIN, audio_bytes)
```
