[![Ask DeepWiki](https://deepwiki.com/badge.svg)](https://deepwiki.com/JarbasHiveMind/hivemind-websocket-client)

# HiveMind Bus Client

`hivemind-websocket-client` (package `hivemind_bus_client`) is the **foundation library** for every HiveMind satellite. It provides an authenticated, encrypted WebSocket client that extends the standard OVOS bus client, enabling secure and routed communication between a satellite and a [hivemind-core](https://github.com/JarbasHiveMind/HiveMind-core) hub.

All satellite packages — `hivemind-mic-satellite`, `HiveMind-voice-relay`, `HiveMind-voice-sat`, and `HiveMind-cli` — build on this library. If you are building a custom satellite or integration, this is your starting point.

## Where it fits in the satellite spectrum

HiveMind satellites are differentiated by how much audio and language processing happens locally. `hivemind-websocket-client` sits below all of them:

| Satellite | Local processing | Remote processing |
|---|---|---|
| `HiveMind-cli` | nothing (text input) | everything |
| `hivemind-mic-satellite` | mic + VAD | STT, TTS, intent, skills |
| `HiveMind-voice-relay` | mic + VAD + wakeword | STT, TTS |
| `HiveMind-voice-sat` | mic + VAD + wakeword + STT + TTS | skills only |
| **This library** | WebSocket transport + encryption | — |

Every satellite in that table uses `HiveMessageBusClient` from this library to open the connection, complete the handshake, and exchange `HiveMessage` packets with the hub. Server-side STT and TTS are provided by the hub's `hivemind-audio-binary-protocol` plugin; the satellite cannot choose the engine — the hub operator configures it.

See the [whitepaper](https://github.com/JarbasHiveMind/HiveMind-core/blob/dev/docs/whitepaper.md) for protocol details.

## Hardware and OS requirements

- Python 3.9 or later
- No special hardware required — runs on any machine with network access to the hub
- The async client (`hivemind-bus-client[async]`) requires Python 3.10+

## Install

```bash
pip install hivemind_bus_client
```

Async client (asyncio-native applications):

```bash
pip install "hivemind_bus_client[async]"
```

Verify:

```bash
hivemind-client --help
```

## Quickstart: pair with a hub and send your first message

### 1. Generate an access key on the hub

On the machine running `hivemind-core`:

```bash
hivemind-core add-client --name "my-satellite" --access-key KEY --password PASS
```

`add-client` prints an **access key** and a **password**. Keep both — you need them on every satellite you pair.

### 2. Configure the satellite

On the satellite machine:

```bash
hivemind-client set-identity \
  --key   "your-access-key" \
  --password "your-password" \
  --host  ws://192.168.1.10 \
  --port  5678 \
  --siteid living-room
```

Credentials are saved to `~/.config/hivemind/_identity.json` and are read automatically by the client.

### 3. Verify connectivity

```bash
hivemind-client ping --host ws://192.168.1.10 --port 5678
```

### 4. Open a terminal session

```bash
hivemind-client terminal
```

Type an utterance, and the hub processes it. Spoken responses are printed to stdout.

### 5. Use the library

```python
from hivemind_bus_client import HiveMessageBusClient
from hivemind_bus_client.message import HiveMessage, HiveMessageType
from ovos_bus_client.message import Message

# Credentials are loaded from the identity file set in step 2.
# Pass them explicitly if you prefer:
#   client = HiveMessageBusClient(key="...", password="...", host="ws://192.168.1.10", port=5678)
client = HiveMessageBusClient()
client.connect()

# React to spoken responses from the hub
client.on_mycroft("speak", lambda msg: print("Hub says:", msg.data["utterance"]))

# Send an utterance
client.emit(HiveMessage(
    HiveMessageType.BUS,
    Message("recognizer_loop:utterance", {"utterances": ["hello world"]}),
))

# Block until you're done
input("Press Enter to disconnect...\n")
client.close()
```

## Library guide

### Connecting

`HiveMessageBusClient` extends `ovos_bus_client.MessageBusClient`. The constructor accepts credentials either directly or through a saved `NodeIdentity`.

```python
from hivemind_bus_client import HiveMessageBusClient

# All parameters optional if identity file is populated
client = HiveMessageBusClient(
    key="access-key",           # access key from hivemind-core add-client
    password="password",        # password from hivemind-core add-client
    host="ws://192.168.1.10",   # hub address, ws:// or wss://
    port=5678,                  # default 5678
    useragent="my-satellite",   # shown in hub logs
    self_signed=True,           # accept self-signed TLS certs
    share_bus=False,            # share the local OVOS bus with the hub (trusted satellites only)
    compress=True,              # zlib-compress binary frames
    binarize=True,              # use binary wire format instead of JSON
    websocket_ping_interval=25,  # keep long-lived proxy connections warm
    websocket_ping_timeout=10,   # fail fast enough for reconnect to take over
)
client.connect()                # blocks until the handshake completes
```

`connect()` runs the WebSocket in a background thread and calls `wait_for_handshake()`. After it returns the connection is live.

Ping settings can also be supplied with environment variables. Client-specific
variables (`HIVEMIND_WEBSOCKET_CLIENT_PING_INTERVAL`,
`HIVEMIND_WEBSOCKET_CLIENT_PING_TIMEOUT`) win over shared hub defaults
(`HIVEMIND_WEBSOCKET_PING_INTERVAL`, `HIVEMIND_WEBSOCKET_PING_TIMEOUT`).
Set the interval to `0` to disable client pings.

### Sending messages

Any OVOS `Message` can be sent directly — the client wraps it in a `HiveMessage(BUS, ...)` automatically:

```python
from ovos_bus_client.message import Message

client.emit(Message("recognizer_loop:utterance", {"utterances": ["what time is it"]}))
```

For explicit HiveMessage control:

```python
from hivemind_bus_client.message import HiveMessage, HiveMessageType

client.emit(HiveMessage(HiveMessageType.BUS,
                        Message("recognizer_loop:utterance", {"utterances": ["what time is it"]})))
```

### Receiving messages

Register handlers for OVOS (inner) message types:

```python
client.on_mycroft("speak", lambda msg: print(msg.data["utterance"]))
client.on_mycroft("ovos.common_play.play", handle_play)
```

Register handlers for HiveMind protocol messages:

```python
from hivemind_bus_client.message import HiveMessageType

client.on(HiveMessageType.BROADCAST, lambda hm: print("Broadcast:", hm.payload))
client.on(HiveMessageType.PING, lambda hm: print("Ping from", hm.metadata))
```

### Wait helpers

```python
# Send and block until a reply arrives
response = client.wait_for_response(
    Message("recognizer_loop:utterance", {"utterances": ["what time is it"]}),
    reply_type="speak",
    timeout=10,
)
if response:
    print(response.payload.data["utterance"])

# Wait for the next message of a given HiveMind type
hive_msg = client.wait_for_message(HiveMessageType.BROADCAST, timeout=30)

# Wait for the next inner OVOS message of a given type wrapped in BUS
bus_msg = client.wait_for_mycroft("speak", timeout=10)
```

### ESCALATE, QUERY, CASCADE, BROADCAST, PROPAGATE

```python
# ESCALATE — forward up the authority chain (supervisor hubs)
client.emit(HiveMessage(HiveMessageType.ESCALATE,
                        Message("recognizer_loop:utterance", {"utterances": ["call admin"]})))

# QUERY — first answering node wins
inner = HiveMessage(HiveMessageType.BUS,
                    Message("intent.request", {"utterance": "what time is it"}))
client.emit(HiveMessage(HiveMessageType.QUERY, payload=inner))

# BROADCAST — admin pushes to all connected satellites (requires admin access key)
client.emit(HiveMessage(
    HiveMessageType.BROADCAST,
    payload=HiveMessage(HiveMessageType.BUS,
                        Message("speak", {"utterance": "System update in 5 minutes"}))
))
```

### Peer-to-peer encrypted messages (INTERCOM)

INTERCOM uses hybrid encryption (random AES-256-GCM key per message, RSA-encrypted key exchange):

```python
target_pubkey = "-----BEGIN PUBLIC KEY-----\n..."
client.emit_intercom(
    HiveMessage(HiveMessageType.BUS, Message("speak", {"utterance": "private message"})),
    pubkey=target_pubkey,
)
```

Incoming INTERCOM messages are only injected into the internal bus if the sender's public key is in `NodeIdentity.trusted_keys`.

### Async client

For asyncio-native applications (FastAPI, aiohttp, async chat bots):

```python
from hivemind_bus_client import AsyncHiveMessageBusClient

async def main():
    client = AsyncHiveMessageBusClient(key="...", password="...", host="ws://192.168.1.10")
    await client.connect()
    await client.emit(Message("recognizer_loop:utterance", {"utterances": ["hello"]}))
```

Requires: `pip install "hivemind_bus_client[async]"`

### Binary payloads (TTS audio, files)

Override `BinaryDataCallbacks` to handle incoming binary data:

```python
from hivemind_bus_client.client import BinaryDataCallbacks, HiveMessageBusClient

class MyCallbacks(BinaryDataCallbacks):
    def handle_receive_tts(self, bin_data: bytes, utterance: str, lang: str, file_name: str):
        with open(file_name, "wb") as f:
            f.write(bin_data)

client = HiveMessageBusClient(bin_callbacks=MyCallbacks())
client.connect()
```

## Message types

| Type | Direction | Description |
|---|---|---|
| `BUS` | satellite ↔ hub | Standard OVOS bus message forwarded to the hub's skill engine |
| `ESCALATE` | upstream | Forward up the authority chain (supervisor hubs) |
| `QUERY` | upstream + response | First answering node wins |
| `BROADCAST` | downstream | Admin pushes to all connected satellites |
| `PROPAGATE` | flood | Forward to all peers in all directions |
| `CASCADE` | flood + responses | Collect answers from all nodes |
| `INTERCOM` | any → any | End-to-end hybrid-encrypted (AES-GCM + RSA) |
| `PING` | inside PROPAGATE | Flood-based network topology discovery |
| `BINARY` | hub → satellite | Raw binary payload (TTS audio, file transfer) |

## Security

- **Per-link encryption**: AES-GCM or ChaCha20-Poly1305, negotiated at handshake via `poorman_handshake`
- **Hybrid INTERCOM encryption**: Random AES-256 key per message, RSA-encrypted key exchange — no payload size limit
- **Self-signed TLS**: `self_signed=True` (default) accepts self-signed certificates; set `False` in production
- **Trusted peers**: Only peers with a public key in `NodeIdentity.trusted_keys` can inject BUS messages via PROPAGATE and INTERCOM; untrusted messages are silently dropped

## Identity and credentials

```python
from hivemind_bus_client.identity import NodeIdentity

identity = NodeIdentity()            # loads from ~/.config/hivemind/_identity.json
print(identity.access_key)
print(identity.default_master)

identity.add_trusted_key("home-hub", "-----BEGIN PUBLIC KEY-----\n...")
identity.save()
```

See [Identity & Credentials](docs/identity.md) for the full field reference.

## CLI

```bash
# Persist credentials
hivemind-client set-identity --key KEY --password PASS --host ws://hub.local --port 5678 --siteid home

# Interactive terminal (type utterances, see spoken responses)
hivemind-client terminal

# Ping the hub and print round-trip info
hivemind-client ping --host ws://hub.local --port 5678

# Send a single OVOS message
hivemind-client send-mycroft \
  --msg "recognizer_loop:utterance" \
  --payload '{"utterances": ["hello world"]}'

# Send as ESCALATE or PROPAGATE
hivemind-client escalate --msg "recognizer_loop:utterance" --payload '{"utterances": ["hello"]}'
hivemind-client propagate --msg "recognizer_loop:utterance" --payload '{"utterances": ["hello"]}'
```

## Troubleshooting

**`RuntimeError: NodeIdentity not set`** — Run `hivemind-client set-identity` or pass `key`, `password`, and `host` to the constructor.

**`RuntimeError: timed out waiting for handshake`** — The hub is unreachable or the port is wrong. Verify the hub is running (`hivemind-core listen`) and the firewall allows port 5678. Try `hivemind-client ping` first.

**`got encrypted message, but could not decrypt!`** — The access key or password does not match what was registered on the hub. Re-run `hivemind-core add-client` and update the satellite identity.

**Connection drops immediately** — The hub may have rejected the access key (wrong key, key revoked, or blacklisted). Check hub logs: `journalctl -u hivemind-core -f`.

## Documentation

Full reference in [`/docs`](docs/index.md):

- [Installation](docs/installation.md) — PyPI, optional extras, dependencies
- [API Reference](docs/api.md) — `HiveMessage`, `HiveMessageBusClient`, `NodeIdentity`, `HiveMapper`
- [Client API](docs/client_api.md) — WebSocket and HTTP client usage
- [Async Client](docs/async_client.md) — asyncio-native `AsyncHiveMessageBusClient`
- [Message Types](docs/message_types.md) — Routing modes, QUERY, CASCADE, PING
- [Identity & Credentials](docs/identity.md) — Credentials, RSA keys, trusted peers
- [Binary Handlers](docs/binary_handlers.md) — TTS audio and file transfer callbacks
- [Serialization](docs/serialization.md) — Binary wire format
- [CLI Reference](docs/cli.md) — All `hivemind-client` commands
- [Examples](docs/examples.md) — Chat, TTS, INTERCOM, QUERY, CASCADE, trust management
