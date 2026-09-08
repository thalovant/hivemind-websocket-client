# Installation

## Install from PyPI

```bash
pip install hivemind_bus_client
```

## Verify

```bash
hivemind-client --help
```

## Dependencies

The library requires:

- `websocket-client` — WebSocket transport
- `ovos-bus-client` — `Message` type compatibility with OVOS
- `poorman-handshake` — RSA-based handshake and AES session key derivation
- `pycryptodome` — AES-GCM encryption
- `pybase64` — base64 encoding helpers

All are installed automatically as package dependencies.
