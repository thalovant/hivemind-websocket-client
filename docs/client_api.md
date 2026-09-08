# Client API Guide

The `hivemind-websocket-client` library (package `hivemind-bus-client`) provides two primary classes for interacting with a HiveMind.

## 1. `HiveMessageBusClient`
Use this for real-time, bidirectional communication over WebSockets.
- **Source**: `hivemind_bus_client.client.HiveMessageBusClient`

### Basic Connection
The client handles the encrypted handshake (via `poorman_handshake`) and transport layer security (AES-256-GCM via `hivemind_bus_client.encryption`).

```python
from hivemind_bus_client.client import HiveMessageBusClient

# Uses identity from ~/.config/hivemind-core/identity.json by default
client = HiveMessageBusClient(host="192.168.1.10", port=5678)
client.connect() # Executes poorman_handshake.PasswordHandShake
```

### Handling AI Events
Listen for events using `on_mycroft` (inherited from `ovos_bus_client.client.MessageBusClient` but wrapped to handle encrypted HiveMessages).

```python
def handle_speak(message):
    print(f"AI says: {message.data['utterance']}")

client.on_mycroft("speak", handle_speak)
```

### Sending Utterances
To send a command, wrap an OVOS `Message` in a `HiveMessage` and use `emit`.

```python
from ovos_bus_client.message import Message
from hivemind_bus_client.message import HiveMessage, HiveMessageType

utt = "What time is it?"
msg = HiveMessage(HiveMessageType.BUS, 
                  Message("recognizer_loop:utterance", {"utterances": [utt]}))
client.emit(msg) # Encrypts and sends via the WebSocket
```

## 2. `HiveMindHTTPClient`
Use this for scenarios where a persistent WebSocket is not desired or possible.
- **Source**: `hivemind_bus_client.http_client.HiveMindHTTPClient`

```python
from hivemind_bus_client.http_client import HiveMindHTTPClient

client = HiveMindHTTPClient(host="http://192.168.1.10", port=5679)
client.connect() # Performs the encrypted handshake via HTTP POST
```

## 3. Decorators
The library includes useful decorators in `hivemind_bus_client.decorators` for wrapping functions as HiveMind services.
- **`@with_hivemind`**: Automatically manages client connection and cleanup.
