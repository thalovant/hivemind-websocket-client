# Binary Serialization Protocol

The HiveMind binary serialization protocol (`hivemind_bus_client/serialization.py`) encodes `HiveMessage` objects into compact bitstrings for efficient WebSocket transport. This is the **reference implementation** — other language clients must produce identical wire bytes.

## Protocol Version

Current version: **1** (`PROTOCOL_VERSION` — `serialization.py:12`).

Decoding a bitstring with an unsupported version raises `UnsupportedProtocolVersion` (`exceptions.py`).

## Wire Format (v1)

All fields are packed MSB-first. The bitstring is left-padded with `0` bits to reach a byte boundary; the first `1` bit marks the start of meaningful data.

```
┌─────────────┬───────────┬─────────────────────┬──────────┬──────────┬──────────────────┬────────────────┬──────────┐
│ Padding 0s  │ Start (1) │ Versioned (1 bit)   │ [Proto   │ Type     │ Compressed       │ Meta-length    │ Metadata │
│ (0+ bits)   │           │ 0=unversioned       │  Version │ (5 bits) │ (1 bit)          │ (8 bits = N    │ (N bytes)│
│             │           │ 1=versioned         │  8 bits] │          │ 0=raw, 1=zlib    │  bytes)        │          │
└─────────────┴───────────┴─────────────────────┴──────────┴──────────┴──────────────────┴────────────────┴──────────┘
                                                                       ┌────────────────────────────────────────────┐
                                                                       │ If Type == BINARY:                         │
                                                                       │   BinarySubtype (4 bits)                   │
                                                                       │ Payload (remaining bits)                   │
                                                                       └────────────────────────────────────────────┘
```

### Field Details

| Field | Bits | Description | Source |
|-------|------|-------------|--------|
| Padding | 0+ | Leading `0` bits for byte alignment | `_get_bitstring_v1` — `serialization.py:78-80` |
| Start marker | 1 | Always `1`; decoder skips `0`s until it finds this | `serialization.py:54` |
| Versioned flag | 1 | `1` = next 8 bits are a protocol version; `0` = assume current version | `serialization.py:55` |
| Protocol version | 8 (optional) | Only present when versioned=1. Integer protocol version | `serialization.py:57` |
| Message type | 5 | Index into `_INT2TYPE` mapping (13 types, 0–12) | `serialization.py:58`, `_INT2TYPE` — `serialization.py:16-28` |
| Compressed | 1 | `1` = payload is zlib-compressed | `serialization.py:59` |
| Metadata length | 8 | Number of **bytes** of metadata that follow | `serialization.py:63` |
| Metadata | N×8 | JSON-encoded dict, optionally zlib-compressed | `serialization.py:64` |
| Binary subtype | 4 (conditional) | Only for `BINARY` type. Maps to `HiveMindBinaryPayloadType` | `serialization.py:68-69` |
| Payload | remaining | JSON string (text types) or raw bytes (BINARY type) | `serialization.py:71-76` |

### Type Map (`_INT2TYPE` — `serialization.py:16-28`)

| Integer | HiveMessageType |
|---------|-----------------|
| 0 | HANDSHAKE |
| 1 | BUS |
| 2 | SHARED_BUS |
| 3 | BROADCAST |
| 4 | PROPAGATE |
| 5 | ESCALATE |
| 6 | HELLO |
| 7 | QUERY |
| 8 | CASCADE |
| 9 | PING |
| 10 | RENDEZVOUS |
| 11 | THIRDPRTY |
| 12 | BINARY |

### Binary Payload Subtypes (`HiveMindBinaryPayloadType` — `message.py:32-41`)

| Integer | Subtype | Description |
|---------|---------|-------------|
| 0 | UNDEFINED | No info about binary contents |
| 1 | RAW_AUDIO | Raw audio data |
| 2 | NUMPY_IMAGE | Image as numpy array |
| 3 | FILE | Generic file transfer |
| 4 | STT_AUDIO_TRANSCRIBE | Audio for STT transcription |
| 5 | STT_AUDIO_HANDLE | Audio for STT + immediate handling |
| 6 | TTS_AUDIO | Synthesized TTS audio |

## Public API

### `get_bitstring()` — `serialization.py:31`

Encode a `HiveMessage` to a `BitArray`.

```python
from hivemind_bus_client.serialization import get_bitstring
from hivemind_bus_client.message import HiveMessageType

bitstr = get_bitstring(
    hive_type=HiveMessageType.BUS,
    payload=message,          # Message, HiveMessage, str, dict, or bytes
    compressed=None,          # None=auto (pick smaller), True/False=force
    hivemeta=None,            # optional dict of metadata
    binary_type=...,          # HiveMindBinaryPayloadType (for BINARY only)
    proto_version=1,          # protocol version
    versioned=False,          # embed version in bitstring
)
# Returns: BitArray — call .bytes for raw bytes
```

When `compressed=None`, both compressed and uncompressed encodings are generated and the smaller one is returned (`serialization.py:36-41`).

### `decode_bitstring()` — `serialization.py:85`

Decode raw bytes back to a `HiveMessage`.

```python
from hivemind_bus_client.serialization import decode_bitstring

hive_msg = decode_bitstring(raw_bytes)  # returns HiveMessage
```

### `mycroft2bitstring()` — `serialization.py:129`

Convenience wrapper: encode an OVOS `Message` as a `BUS`-type bitstring, using `msg.context` as metadata.

```python
from hivemind_bus_client.serialization import mycroft2bitstring

bitstr = mycroft2bitstring(msg, compressed=False)
```

## Auto-Compression

`get_bitstring(compressed=None)` encodes the payload both ways and returns whichever is smaller. The `compressed` bit in the wire format tells the decoder which path to take. For small payloads, uncompressed is often smaller; for payloads > ~200 bytes, zlib compression typically wins.

## Cross-Language Implementors

To implement a compatible encoder/decoder:

1. Pack fields in the exact order and bit widths above
2. Use zlib (RFC 1950) for compression
3. JSON-encode metadata and non-binary payloads as UTF-8 before compression
4. Left-pad with `0` bits to byte-align; prepend a `1` start marker
5. For `BINARY` messages, the 4-bit subtype appears before the raw payload bytes
6. Test against the Python reference implementation using cross-platform test vectors (`test/unittests/vectors.json`)
