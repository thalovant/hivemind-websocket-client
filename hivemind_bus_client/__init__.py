from .client import HiveMessageBusClient
from .message import HiveMessage, HiveMessageType


def __getattr__(name):
    # Lazy-import async surface so bare installs (no `websockets`) keep working.
    if name in ("AsyncHiveMessageBusClient",
                "AsyncHiveMessageWaiter",
                "AsyncHivePayloadWaiter"):
        from . import async_client
        return getattr(async_client, name)
    raise AttributeError(f"module 'hivemind_bus_client' has no attribute {name!r}")
