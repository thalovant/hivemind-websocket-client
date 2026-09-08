# Setup walkthrough: pairing a satellite with a hub

This guide walks through installing the library, registering a satellite on a hub, and verifying the connection.

## Prerequisites

- A running [hivemind-core](https://github.com/JarbasHiveMind/HiveMind-core) hub (the machine that hosts the OVOS skill engine)
- Python 3.9+ on the satellite machine
- Network access from the satellite to the hub on port 5678 (WebSocket) or 5679 (HTTP)

## 1. Install the library

```bash
pip install hivemind_bus_client
```

## 2. Register this satellite on the hub

On the **hub machine**, create an access key for this satellite:

```bash
hivemind-core add-client --name "living-room-satellite"
```

Output:

```
Added satellite: living-room-satellite
  access-key : 42caf3d2405075fb9e7a4e1ff44e4c4f
  password   : 5ae486f7f1c26bd4645bd052e4af3ea3
```

Keep the access key and password — you need them on the satellite.

To give the satellite admin rights (able to send BROADCAST messages):

```bash
hivemind-core make-admin --name "living-room-satellite"
```

## 3. Save credentials on the satellite

On the **satellite machine**:

```bash
hivemind-client set-identity \
  --key      "42caf3d2405075fb9e7a4e1ff44e4c4f" \
  --password "5ae486f7f1c26bd4645bd052e4af3ea3" \
  --host     ws://192.168.1.10 \
  --port     5678 \
  --siteid   living-room
```

Credentials are written to `~/.config/hivemind/_identity.json`. The `--siteid` value is injected into every outgoing OVOS message context so the hub can route responses back to the right room.

For an encrypted (TLS) connection use `wss://` instead of `ws://`. The library accepts self-signed certificates by default (`self_signed=True`).

## 4. Verify connectivity

```bash
hivemind-client ping --host ws://192.168.1.10 --port 5678
```

A successful response looks like:

```
Pong from hub (192.168.1.10:5678) — RTT 12 ms
```

## 5. Open an interactive terminal

```bash
hivemind-client terminal
```

Type a sentence and press Enter. The hub processes it and the spoken response is printed:

```
== connected to HiveMind
Utterance: what time is it
> It is 3:45 PM.
```

Press Ctrl-C to disconnect.

## 6. Use the library in code

```python
from hivemind_bus_client import HiveMessageBusClient
from hivemind_bus_client.message import HiveMessage, HiveMessageType
from ovos_bus_client.message import Message

# Loads credentials from ~/.config/hivemind/_identity.json
client = HiveMessageBusClient()
client.connect()

client.on_mycroft("speak", lambda msg: print("Hub says:", msg.data["utterance"]))

client.emit(HiveMessage(
    HiveMessageType.BUS,
    Message("recognizer_loop:utterance", {"utterances": ["hello world"]}),
))

input("Press Enter to disconnect...\n")
client.close()
```

Or pass credentials directly without saving them:

```python
client = HiveMessageBusClient(
    key="42caf3d2405075fb9e7a4e1ff44e4c4f",
    password="5ae486f7f1c26bd4645bd052e4af3ea3",
    host="ws://192.168.1.10",
    port=5678,
)
client.connect()
```

## SSL / TLS

For production deployments where the hub uses a signed certificate:

```python
client = HiveMessageBusClient(
    host="wss://hub.example.com",
    port=5678,
    self_signed=False,   # enforce certificate verification
)
```

For self-hosted hubs with a self-signed certificate keep `self_signed=True` (the default).

## Troubleshooting

### Connection refused or timeout

- Confirm the hub is running: `systemctl status hivemind-core`
- Check the firewall allows port 5678: `sudo ufw allow 5678/tcp`
- Use `ws://` (not `http://`) for the host URL

### Handshake timeout

```
RuntimeError: timed out waiting for handshake
```

The TCP connection opened but the hub rejected or did not complete the handshake. Common causes:

- Wrong access key or password — re-run `hivemind-core add-client` and update `set-identity`
- The satellite's IP is blocked — check `hivemind-core blacklist-client`
- Firewall is stateful and drops the upgrade: ensure WebSocket upgrades are not filtered

### Decryption error

```
got encrypted message, but could not decrypt!
```

The `password` does not match what the hub stored for this access key. The password is used to derive the AES session key during the handshake — a mismatch means every message is unreadable.

### Multiple satellites on the same machine

Each satellite needs its own `NodeIdentity`. Pass a custom identity file path:

```python
from hivemind_bus_client.identity import NodeIdentity
from hivemind_bus_client import HiveMessageBusClient

identity = NodeIdentity(identity_file="/etc/hivemind/kitchen-satellite.json")
identity.access_key     = "..."
identity.password       = "..."
identity.default_master = "ws://192.168.1.10"
identity.default_port   = 5678
identity.site_id        = "kitchen"
identity.save()

client = HiveMessageBusClient(identity=identity)
client.connect()
```
