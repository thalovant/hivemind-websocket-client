# API Reference

## `HiveMessage` (`hivemind_bus_client/message.py:45`)

The fundamental message type exchanged across the HiveMind network.

### Properties

| Property | Type | Description | Source |
|---|---|---|---|
| `msg_type` | `str` | One of the `HiveMessageType` values | `message.py:105` |
| `payload` | `Message \| HiveMessage \| dict \| bytes` | Message content; automatically cast to the appropriate type | `message.py:128` |
| `bin_type` | `HiveMindBinaryPayloadType` | Binary payload subtype (only for `BINARY` messages) | `message.py:164` |
| `metadata` | `dict` | Auxiliary metadata (e.g. `sample_rate`, `lang`, `file_name`) | `message.py:94` |
| `node_id` | `str` | Node semi-unique identifier | `message.py:109` |
| `source_peer` | `str` | Peer ID of the sender | `message.py:114` |
| `target_peers` | `list[str]` | Intended recipients | `message.py:118` |
| `target_site_id` | `str` | Route to a specific site ID | `message.py:98` |
| `target_public_key` | `str` | Route to a specific RSA public key (used with INTERCOM) | `message.py:102` |
| `route` | `list[dict]` | Hop history | `message.py:123` |

### Serialization

- `serialize()` (`message.py:200`): Returns the JSON string representation of the message.
- `deserialize(payload)` (`message.py:204`): Reconstructs a `HiveMessage` from a JSON string or dictionary.

---

## `HiveMessageType` (`hivemind_bus_client/message.py:9`)

Enumeration of HiveMind message types.

| Value | String | Description |
|---|---|---|
| `HANDSHAKE` | `"shake"` | Connection negotiation and key exchange |
| `BUS` | `"bus"` | OVOS/Mycroft bus message destined for the hub agent |
| `SHARED_BUS` | `"shared_bus"` | Passive bus share from a satellite (monitoring only) |
| `INTERCOM` | `"intercom"` | Peer-to-peer encrypted message |
| `BROADCAST` | `"broadcast"` | Forward message to all slaves |
| `PROPAGATE` | `"propagate"` | Forward message to all slaves and masters |
| `ESCALATE` | `"escalate"` | Forward message up the authority chain |
| `HELLO` | `"hello"` | Node announcement and session setup |
| `QUERY` | `"query"` | Request-response upstream; first answering node wins |
| `CASCADE` | `"cascade"` | Request-response flood; collects responses from all nodes |
| `PING` | `"ping"` | Network topology discovery (flood-based) |
| `RENDEZVOUS` | `"rendezvous"` | Reserved for rendezvous nodes |
| `BINARY` | `"bin"` | Raw binary data container |
| `THIRDPRTY` | `"3rdparty"` | User-defined message type |

---

## `HiveMindBinaryPayloadType` (`hivemind_bus_client/message.py:33`)

Describes the content of a `BINARY` message.

| Value | Int | Description |
|---|---|---|
| `UNDEFINED` | `0` | Unknown binary content |
| `RAW_AUDIO` | `1` | Raw PCM audio |
| `NUMPY_IMAGE` | `2` | NumPy image array |
| `FILE` | `3` | Generic file transfer |
| `STT_AUDIO_TRANSCRIBE` | `4` | Audio for STT — return transcripts |
| `STT_AUDIO_HANDLE` | `5` | Audio for STT — transcribe and handle immediately |
| `TTS_AUDIO` | `6` | Synthesized TTS audio to be played back |

---

## `HiveMessageBusClient` (`hivemind_bus_client/client.py:93`)

WebSocket client that extends `ovos_bus_client.MessageBusClient`.

### Core Methods

- `connect(bus=FakeBus(), protocol=None, site_id=None)` (`client.py:193`): Connects to the HiveMind hub, starts the background thread, and waits for the handshake to complete.
- `emit(message, binary_type=UNDEFINED)` (`client.py:348`): Sends a `HiveMessage` or `MycroftMessage`. Automatically injects routing context for `BUS` messages (`client.py:379`).
- `on(event_name, func)` (`client.py:434`): Registers a handler. Automatically detects if the event name is a `HiveMessageType` or a standard OVOS message type (`client.py:435`).
- `on_mycroft(mycroft_msg_type, func)` (`client.py:429`): Explicitly registers a handler for an OVOS internal bus message.
- `remove(event_name, func)` (`client.py:447`): Removes a registered handler.
- `wait_for_handshake(timeout=5, max_retries=None)` (`client.py:236`): Blocks until the cryptographic handshake with the hub is finished. By default it waits through reconnects forever; pass `max_retries` for a hard timeout.
- `emit_intercom(message, pubkey)` (`client.py:538`): Sends a hybrid-encrypted (AES-GCM + RSA) message targeted at a specific node's public key.

### Waiting for Messages

- `wait_for_message(message_type, timeout=3.0)` (`client.py:454`): Blocks until a specific `HiveMessageType` is received.
- `wait_for_payload(payload_type, message_type=THIRDPRTY, timeout=3.0)` (`client.py:467`): Blocks until a message of a specific type with a specific payload type is received.
- `wait_for_mycroft(mycroft_msg_type, timeout=3.0)` (`client.py:484`): Blocks until a specific OVOS message is received over the `BUS` message type.
- `wait_for_response(message, reply_type=None, timeout=3.0)` (`client.py:488`): Sends a message and waits for a response.
- `wait_for_payload_response(message, payload_type, reply_type=None, timeout=3.0)` (`client.py:511`): Sends a message and waits for a specific payload in the response.

---

## `BinaryDataCallbacks` (`hivemind_bus_client/client.py:26`)

Base class for handling incoming binary data.

- `handle_receive_tts(bin_data, utterance, lang, file_name)` (`client.py:27`): Called when TTS audio is received.
- `handle_receive_file(bin_data, file_name)` (`client.py:33`): Called when a file is received.

---

## `NodeIdentity` (`hivemind_bus_client/identity.py:7`)

Manages the node's identity, including credentials and RSA keys.

### Persistence

- `save()` (`identity.py:219`): Persists identity settings to disk.
- `reload()` (`identity.py:225`): Reloads settings from the identity file.
- `create_keys()` (`identity.py:231`): Generates a new RSA key pair.

### Trusted Keys

Trusted keys are stored as an alias → public key mapping in the identity file. Used by protocol handlers to gate BUS injection from PROPAGATE and INTERCOM messages.

- `trusted_keys` (`identity.py:153`): `Dict[str, str]` — alias → public key mapping.
- `add_trusted_key(alias, pubkey)` (`identity.py:176`): Add a peer. Returns `False` if alias already exists.
- `remove_trusted_key(alias)` (`identity.py:195`): Remove by alias. Returns `False` if not found.
- `is_trusted_key(pubkey)` (`identity.py:210`): Check if a public key is trusted.
- `get_trusted_alias(pubkey)` (`identity.py:222`): Look up alias for a public key. Returns `None` if not found.

---

## `HiveMapper` (`hivemind_bus_client/hive_map.py:38`)

Collects responsive PINGs from a flood and builds a directed hive topology graph.

### Key Methods

- `on_ping(message, received_at=None)` (`hive_map.py:66`): Ingest a PING and update topology.
- `check_flood_id(flood_id, max_size=1000)` (`hive_map.py:119`): Flood-loop prevention with FIFO eviction.
- `mark_trusted_nodes(trusted_keys)` (`hive_map.py:145`): Set `NodeInfo.trusted` based on `NodeIdentity.trusted_keys`.
- `is_peer_trusted(peer)` (`hive_map.py:158`): Check if a discovered peer is trusted.
- `to_ascii(root_peer=None)` (`hive_map.py:173`): Render topology as ASCII tree.
- `to_dict()` / `to_json()` (`hive_map.py:147`): JSON-serialisable topology snapshot.

### `NodeInfo` (`hive_map.py:15`)

| Field | Type | Description |
|-------|------|-------------|
| `peer` | `str` | Peer identifier |
| `site_id` | `str?` | Location identifier |
| `public_key` | `str?` | RSA public key announced in PING |
| `lang` | `str?` | Locale announced by the node |
| `trusted` | `bool` | Whether this peer's key is in the trusted list |
| `timestamp` | `float?` | Sender's clock when PING was created |
| `latency_ms` | `float?` | Estimated one-way latency (computed property) |

---

## Decorators (`hivemind_bus_client/decorators.py`)

Convenience decorators for registering handlers on specific message types.

| Decorator | Message Type | Source |
|-----------|-------------|--------|
| `on_query(payload_type, bus)` | `QUERY` | `decorators.py:236` |
| `on_cascade(payload_type, bus)` | `CASCADE` | `decorators.py:253` |
| `on_propagate(payload_type, bus)` | `PROPAGATE` | `decorators.py:164` |
| `on_broadcast(payload_type, bus)` | `BROADCAST` | `decorators.py:128` |
| `on_escalate(payload_type, bus)` | `ESCALATE` | `decorators.py:182` |
| `on_ping(payload_type, bus)` | `PING` | `decorators.py:146` |
| `on_mycroft_message(payload_type, bus)` | `BUS` | `decorators.py:92` |
| `on_shared_bus(payload_type, bus)` | `SHARED_BUS` | `decorators.py:110` |
| `on_hive_message(message_type, bus)` | any | `decorators.py:78` |

---

## Encryption (`hivemind_bus_client/encryption.py`)

### Hybrid Encryption (INTERCOM)

- `hybrid_encrypt(public_key, plaintext, sign_key=None)` (`encryption.py:502`): AES-256-GCM payload + RSA-encrypted ephemeral key. Returns a JSON-serialisable dict.
- `hybrid_decrypt(private_key, envelope)` (`encryption.py:547`): Decrypts a hybrid-encrypted envelope.

### Symmetric Encryption (per-link)

- `encrypt_AES(key, text)` / `decrypt_AES_128(key, ciphertext, tag, nonce)` — AES-GCM
- `encrypt_ChaCha20_Poly1305(key, text)` / `decrypt_ChaCha20_Poly1305(key, ciphertext, tag, nonce)` — ChaCha20-Poly1305

---

## `HiveMessageWaiter` / `HivePayloadWaiter` (`hivemind_bus_client/client.py:38`, `78`)

Utility classes used for one-shot message waiting. These are used internally by the `wait_for_*` methods of `HiveMessageBusClient`.
