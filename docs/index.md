# HiveMind WebSocket Client

`hivemind-websocket-client` (package: `hivemind_bus_client`) is the primary library for building and running HiveMind satellites. It provides a robust WebSocket client that extends the standard OpenVoiceOS (OVOS) bus client, enabling secure, encrypted, and routed communication across the HiveMind network.

## Key Features

- **Transparent Routing**: Automatically handles message routing between satellites and hubs.
- **Per-link Encryption**: AES-GCM or ChaCha20-Poly1305, negotiated at handshake.
- **Hybrid INTERCOM Encryption**: AES-256-GCM payload + RSA-encrypted ephemeral key per message — no size limits.
- **Trusted Peers**: `NodeIdentity.trusted_keys` gates BUS injection for PROPAGATE and INTERCOM from untrusted sources.
- **CASCADE Aggregation**: Collects responses from all nodes with timeout and early resolution via `HiveMapper`.
- **PING Discovery**: Flood-based topology mapping with public key and locale announcement.
- **Binary Support**: Optimized handling for binary payloads such as TTS audio and file transfers.
- **Drop-in Replacement**: Designed to be mostly compatible with `ovos-bus-client`, allowing easy migration of existing OVOS skills or services to HiveMind.

## Primary Components

- **`HiveMessageBusClient`**: The main WebSocket client class (`hivemind_bus_client/client.py:93`).
- **`HiveMessage`**: The fundamental message unit of the HiveMind protocol (`hivemind_bus_client/message.py:45`).
- **`NodeIdentity`**: Manages device credentials, keys, and identity settings (`hivemind_bus_client/identity.py:7`).

## Quick Start

```python
from hivemind_bus_client import HiveMessageBusClient
from ovos_bus_client.message import Message

# Initialize the client
client = HiveMessageBusClient(key="my_access_key", password="my_password", host="ws://127.0.0.1")

# Connect and block until handshake is complete
client.connect()

# Listen for a 'speak' event from the hub
client.on_mycroft("speak", lambda msg: print(f"Hub says: {msg.data['utterance']}"))

# Send an utterance to the hub
client.emit(Message("recognizer_loop:utterance", {"utterances": ["hello world"]}))
```

## Documentation Guides

- [Setup walkthrough](setup.md) - Pair a satellite with a hub step by step.
- [Installation](installation.md) - Getting started.
- [API Reference](api.md) - `HiveMessage`, `HiveMessageType`, `HiveMessageBusClient`, `BinaryDataCallbacks`, `NodeIdentity`.
- [Client API](client_api.md) - `HiveMessageBusClient` and `HiveMindHTTPClient` usage.
- [Async Client](async_client.md) - `AsyncHiveMessageBusClient` for asyncio-native applications.
- [Fakes](fakebus.md) - `AsyncFakeHiveMessageBus` in-process stand-in for testing.
- [Message Types](message_types.md) - `HiveMessage`, `HiveMessageType` enum, routing, serialization.
- [Binary Serialization](serialization.md) - Bitstring wire format, `get_bitstring`, `decode_bitstring` (reference implementation).
- [Binary Handlers](binary_handlers.md) - `BinaryDataCallbacks` for TTS audio and file transfers.
- [Identity & Credentials](identity.md) - Managing node identity and settings.
- [CLI Reference](cli.md) - `hivemind-client` commands (`terminal`, `ping`, `send-mycroft`, etc.).
- [CLI Guide](cli_guide.md) - Detailed CLI usage with `HiveMapper` network discovery.
- [Examples](examples.md) - Practical code snippets and walkthroughs.
