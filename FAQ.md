# Hivemind Websocket Client — FAQ

## Installation

### How do I install the client?
```bash
uv pip install hivemind_bus_client
```
Verify with `hivemind-client --help`.

### What are the key dependencies?
`websocket-client`, `ovos-bus-client`, `poorman-handshake` (RSA + PBKDF2), `pycryptodome` (AES-GCM, ChaCha20), `pybase64`, `bitstring` (binary protocol).

## Connection & Authentication

### How is the connection secured?
Cryptographic handshake negotiates a session key via RSA or shared password (PBKDF2). All subsequent traffic is encrypted using AES-GCM or ChaCha20-Poly1305. The cipher is negotiated during handshake based on CPU capabilities (`optimal_ciphers` — `encryption.py:109`). Supports `wss://` for additional transport-layer security.

### What happens during the handshake?
1. Client sends `HANDSHAKE` with supported ciphers/encodings and either a password envelope or RSA pubkey.
2. Server responds with its own envelope.
3. `HiveMindSlaveProtocol.receive_handshake` (`protocol.py:158`) derives the shared secret and sets `crypto_key`.
4. Client sends `HELLO` with its public key, session, and site_id.
Source: `handle_handshake` — `protocol.py:183`

### How do I set credentials?
Either via CLI (`hivemind-client set-identity --key X --password Y --siteid Z`) or programmatically via `NodeIdentity` (`identity.py:7`). Credentials are stored in `~/.config/hivemind/_identity.json`.

### What if I don't set credentials?
`HiveMessageBusClient.__init__` raises `RuntimeError` if `access_key`, `password`, or `default_master` are missing. Source: `client.py:154`

## Messages

### What is `HiveMessage`?
The fundamental message unit. Wraps a payload with routing metadata (`msg_type`, `route`, `source_peer`, `target_peers`, `target_site_id`). Source: `HiveMessage` — `message.py:44`

### What message types exist?
`HiveMessageType` enum (`message.py:9`): `HANDSHAKE`, `BUS`, `SHARED_BUS`, `INTERCOM`, `BROADCAST`, `PROPAGATE`, `ESCALATE`, `HELLO`, `QUERY`, `CASCADE`, `PING`, `RENDEZVOUS`, `THIRDPRTY`, `BINARY`.

### How does `BUS` vs `PROPAGATE` vs `BROADCAST` differ?
- `BUS`: Standard message sent to the hub for processing (like a regular OVOS bus message).
- `BROADCAST`: Hub forwards to all connected satellites (admin-only, downstream only).
- `PROPAGATE`: Forwards to all peers — both upstream and downstream.
- `ESCALATE`: Forwards upstream only through the authority chain.

### How does the payload getter work?
`HiveMessage.payload` (`message.py:127`) reconstructs the appropriate type: `Message` for `BUS`/`SHARED_BUS`, nested `HiveMessage` for `BROADCAST`/`PROPAGATE`/`CASCADE`/`ESCALATE`, raw dict otherwise.

### Can I nest HiveMessages?
Yes. `HiveMessage.__init__` normalizes nested `HiveMessage` payloads to dict via `as_dict`. The `payload` property reconstructs them on access.

### How does CASCADE response aggregation work?
`handle_cascade` (`protocol.py:436`) buffers responses in a `CascadeAggregator` (`protocol.py:21`). After `cascade_timeout` seconds (default 5.0) or when `expected_responses` are collected, the `cascade_select_callback` picks the best response. Default callback returns the first response. Set `HiveMindSlaveProtocol.cascade_select_callback` for custom disambiguation.

### How does the satellite know how many CASCADE responses to expect?
If `HiveMindSlaveProtocol.hive_mapper` is set (a `HiveMapper` instance from prior PING discovery), the aggregator uses `len(hive_mapper.nodes)` as the expected count and resolves early when all nodes respond. Without a mapper, it falls back to the timeout.

### How does trust verification work for PROPAGATE and INTERCOM?
PROPAGATE(BUS) and INTERCOM messages are only injected into the internal bus if the source peer's public key is in `NodeIdentity.trusted_keys`. Messages from untrusted peers are dropped with a warning. INTERCOM messages explicitly targeted at this node (via `target_public_key`) are always accepted. Source: `_is_source_trusted` — `protocol.py:310`, `handle_propagate` — `protocol.py:375`, `handle_intercom` — `protocol.py:478`.

### How do I add a trusted peer?
```python
identity.add_trusted_key("hub-alias", "<peer_public_key>")
identity.save()
```
Source: `NodeIdentity.add_trusted_key` — `identity.py:176`.

### How does HiveMapper mark peers as trusted?
After PING discovery, call `mapper.mark_trusted_nodes(identity.trusted_keys)` to set `NodeInfo.trusted` for each discovered node whose public key matches. Source: `HiveMapper.mark_trusted_nodes` — `hive_map.py:119`.

## Binary Protocol

### What is the binary serialization format?
`get_bitstring` / `decode_bitstring` (`serialization.py`) encode messages as compact bitstrings: 1-bit padding marker, 1-bit versioned flag, optional 8-bit protocol version, 5-bit message type, 1-bit compression flag, 8-bit metadata length, metadata bytes, then payload. Binary messages get an additional 4-bit `HiveMindBinaryPayloadType` field.

### When is binary vs JSON used?
Binary (bitstring) is used when both client and server support `binarize` (negotiated during handshake). JSON is the fallback. `HELLO` and `HANDSHAKE` messages are always sent as JSON. Source: `client.py:400`

### How do I handle binary data like audio?
Subclass `BinaryDataCallbacks` (`client.py:26`) and pass an instance to the client constructor. Override `handle_receive_tts` for TTS audio and `handle_receive_file` for file transfers.

## Encryption

### What ciphers are supported?
AES-GCM (128/192/256-bit keys) and ChaCha20-Poly1305 (256-bit keys). `optimal_ciphers()` (`encryption.py:109`) prefers AES-GCM on CPUs with AES-NI, ChaCha20 otherwise.

### What encodings are supported for JSON-encrypted messages?
`SupportedEncodings` (`encryption.py:38`): `JSON-B64`, `JSON-URLSAFE-B64`, `JSON-B32`, `JSON-HEX`, `JSON-Z85B`, `JSON-Z85P`, `JSON-B91`.

### How does `encrypt_as_json` / `decrypt_from_json` work?
Encrypts plaintext, splits into nonce/ciphertext/tag, encodes each with the chosen encoding, and returns a JSON object with those three fields. Decoding reverses the process. Source: `encryption.py:182`, `encryption.py:246`

## Client API

### How is `HiveMessageBusClient` different from `ovos-bus-client`?
It extends `MessageBusClient` with: HiveMind handshake, automatic encryption/decryption, binary protocol support, routing context injection, and `HiveMessage` event system. It's designed as a near drop-in replacement. Source: `client.py:93`

### What does `on()` do?
`on(event_name, func)` (`client.py:434`) auto-detects whether `event_name` is a `HiveMessageType` (registers on the HiveMind emitter) or an OVOS message type (registers on the internal bus). This makes migration from `ovos-bus-client` seamless.

### How does `emit()` inject routing context?
For `BUS` messages, `emit()` (`client.py:348`) auto-injects `source`, `platform`, `destination`, and `session` into `message.context` if not already present.

## INTERCOM (Peer-to-Peer)

### How does INTERCOM work?
`emit_intercom` (`client.py:538`) RSA-encrypts the message with the target node's public key and signs it with the sender's private key. Only the target node can decrypt it. The message is wrapped in a `HiveMessageType.INTERCOM` envelope.

## CLI

### What CLI commands are available?
`set-identity`, `terminal`, `send-mycroft`, `escalate`, `propagate`, `reset-keys`. Run `hivemind-client --help` for the full list. Source: `scripts.py`
