# Handling Binary Data

The `hivemind-websocket-client` provides a flexible way to handle binary data (audio, images, files) via the `BinaryDataCallbacks` class (defined in `hivemind_bus_client.client.BinaryDataCallbacks`).

## Subclassing `BinaryDataCallbacks`

To process binary data, subclass `BinaryDataCallbacks` and implement the methods you need. These methods are invoked by the client when it receives a `HiveMessage` of type `BIN`.

```python
from hivemind_bus_client.client import BinaryDataCallbacks

class MyAudioHandler(BinaryDataCallbacks):
    def handle_receive_tts(self, bin_data: bytes, utterance: str, lang: str, file_name: str):
        # bin_data is the raw audio bytes (usually WAV)
        # Source: hivemind_bus_client.client.BinaryDataCallbacks.handle_receive_tts
        with open(file_name, "wb") as f:
            f.write(bin_data)
        print(f"TTS audio saved as {file_name}")

    def handle_receive_file(self, bin_data: bytes, file_name: str):
        # Source: hivemind_bus_client.client.BinaryDataCallbacks.handle_receive_file
        pass
```

## Using with `HiveMessageBusClient`

Pass your custom handler to the client during initialization:

```python
from hivemind_bus_client.client import HiveMessageBusClient

my_handler = MyAudioHandler()
client = HiveMessageBusClient(host="...", 
                              bin_callbacks=my_handler)
client.connect()
```

## Available Callback Methods

- **`handle_receive_tts`**: Called when the AI generates audio from text (`client.py:27`).
- **`handle_receive_file`**: Called for generic file transfers (`client.py:33`).
