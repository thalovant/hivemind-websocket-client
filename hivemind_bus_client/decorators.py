"""Decorator-based event handlers for HiveMind message types.

Provides convenience decorators and listener classes for registering
handlers on specific ``HiveMessageType`` events without manual bus
wiring.
"""
from typing import Callable, List, Optional, Union

from hivemind_bus_client.message import HiveMessage, HiveMessageType


class HiveMessageListener:
    """Persistent listener for a single HiveMind message type.

    Re-registers itself after each invocation so handlers fire on every
    matching message, not just the first.

    Args:
        bus: A ``HiveMessageBusClient`` (or any object with ``once`` / ``remove``).
        message_type: The ``HiveMessageType`` to listen for.
    """

    def __init__(self, bus: 'HiveMessageBusClient',
                 message_type: Union[HiveMessageType, str]) -> None:
        self.bus = bus
        self.message_type = message_type
        self._handlers: List[Callable] = []

    def _handler(self, message: HiveMessage) -> None:
        """Dispatch *message* to all registered handlers and re-arm."""
        for handler in self._handlers:
            handler(message)
        self.bus.once(self.message_type, self._handler)

    def listen(self) -> 'HiveMessageListener':
        """Start listening. Returns *self* for chaining."""
        self.bus.once(self.message_type, self._handler)
        return self

    def add_handler(self, handler: Callable) -> None:
        """Append *handler* to the callback list."""
        self._handlers.append(handler)

    def clear_handlers(self) -> None:
        """Remove all registered handlers."""
        self._handlers = []

    def shutdown(self) -> None:
        """Stop listening and remove the internal callback."""
        self.bus.remove(self.message_type, self._handler)


class HivePayloadListener(HiveMessageListener):
    """Listener that filters on an inner payload message type.

    Only dispatches when the outer message matches *message_type* **and**
    the inner ``payload.msg_type`` matches *payload_type*.

    Args:
        payload_type: Required inner ``msg_type`` to match.
        *args: Forwarded to ``HiveMessageListener``.
        **kwargs: Forwarded to ``HiveMessageListener``.
    """

    def __init__(self, payload_type: Union[HiveMessageType, str] = HiveMessageType.THIRDPRTY,
                 *args, **kwargs) -> None:
        super().__init__(*args, **kwargs)
        self.payload_type = payload_type

    def _handler(self, message: HiveMessage) -> None:
        """Dispatch only if inner payload type matches, then re-arm."""
        if message.payload.msg_type == self.payload_type:
            for handler in self._handlers:
                handler(message.payload)
        self.bus.once(self.message_type, self._handler)


def on_hive_message(message_type: Union[HiveMessageType, str],
                    bus: 'HiveMessageBusClient') -> Callable:
    """Decorator: register the wrapped function as a handler for *message_type*.

    Args:
        message_type: The ``HiveMessageType`` to listen for.
        bus: The bus to register on.
    """
    def wrapped_handler(func: Callable) -> Callable:
        bus.on(message_type, func)
        return func
    return wrapped_handler


def on_mycroft_message(payload_type: str,
                       bus: 'HiveMessageBusClient') -> Callable:
    """Decorator: register the wrapped function for OVOS bus messages arriving via ``BUS``.

    Args:
        payload_type: The inner OVOS message type (e.g. ``"speak"``).
        bus: The bus to register on.
    """
    def wrapped_handler(func: Callable) -> Callable:
        waiter = HivePayloadListener(bus=bus, payload_type=payload_type,
                                     message_type=HiveMessageType.BUS)
        waiter.add_handler(func)
        waiter.listen()
        func.shutdown = waiter.shutdown
        return func
    return wrapped_handler


def on_shared_bus(payload_type: str,
                  bus: 'HiveMessageBusClient') -> Callable:
    """Decorator: register the wrapped function for ``SHARED_BUS`` messages.

    Args:
        payload_type: The inner OVOS message type to match.
        bus: The bus to register on.
    """
    def wrapped_handler(func: Callable) -> Callable:
        waiter = HivePayloadListener(bus=bus, payload_type=payload_type,
                                     message_type=HiveMessageType.SHARED_BUS)
        waiter.add_handler(func)
        waiter.listen()
        func.shutdown = waiter.shutdown
        return func
    return wrapped_handler


def on_broadcast(payload_type: str,
                 bus: 'HiveMessageBusClient') -> Callable:
    """Decorator: register the wrapped function for ``BROADCAST`` messages.

    Args:
        payload_type: The inner message type to match.
        bus: The bus to register on.
    """
    def wrapped_handler(func: Callable) -> Callable:
        waiter = HivePayloadListener(bus=bus, payload_type=payload_type,
                                     message_type=HiveMessageType.BROADCAST)
        waiter.add_handler(func)
        waiter.listen()
        func.shutdown = waiter.shutdown
        return func
    return wrapped_handler


def on_ping(payload_type: str,
            bus: 'HiveMessageBusClient') -> Callable:
    """Decorator: register the wrapped function for ``PING`` messages.

    Args:
        payload_type: The inner message type to match.
        bus: The bus to register on.
    """
    def wrapped_handler(func: Callable) -> Callable:
        waiter = HivePayloadListener(bus=bus, payload_type=payload_type,
                                     message_type=HiveMessageType.PING)
        waiter.add_handler(func)
        waiter.listen()
        func.shutdown = waiter.shutdown
        return func
    return wrapped_handler


def on_propagate(payload_type: str,
                 bus: 'HiveMessageBusClient') -> Callable:
    """Decorator: register the wrapped function for ``PROPAGATE`` messages.

    Args:
        payload_type: The inner message type to match.
        bus: The bus to register on.
    """
    def wrapped_handler(func: Callable) -> Callable:
        waiter = HivePayloadListener(bus=bus, payload_type=payload_type,
                                     message_type=HiveMessageType.PROPAGATE)
        waiter.add_handler(func)
        waiter.listen()
        func.shutdown = waiter.shutdown
        return func
    return wrapped_handler


def on_escalate(payload_type: str,
                bus: 'HiveMessageBusClient') -> Callable:
    """Decorator: register the wrapped function for ``ESCALATE`` messages.

    Args:
        payload_type: The inner message type to match.
        bus: The bus to register on.
    """
    def wrapped_handler(func: Callable) -> Callable:
        waiter = HivePayloadListener(bus=bus, payload_type=payload_type,
                                     message_type=HiveMessageType.ESCALATE)
        waiter.add_handler(func)
        waiter.listen()
        func.shutdown = waiter.shutdown
        return func
    return wrapped_handler


def on_handshake(payload_type: str,
                 bus: 'HiveMessageBusClient') -> Callable:
    """Decorator: register the wrapped function for ``HANDSHAKE`` messages.

    Args:
        payload_type: The inner message type to match.
        bus: The bus to register on.
    """
    def wrapped_handler(func: Callable) -> Callable:
        waiter = HivePayloadListener(bus=bus, payload_type=payload_type,
                                     message_type=HiveMessageType.HANDSHAKE)
        waiter.add_handler(func)
        waiter.listen()
        func.shutdown = waiter.shutdown
        return func
    return wrapped_handler


def on_hello(payload_type: str,
             bus: 'HiveMessageBusClient') -> Callable:
    """Decorator: register the wrapped function for ``HELLO`` messages.

    Args:
        payload_type: The inner message type to match.
        bus: The bus to register on.
    """
    def wrapped_handler(func: Callable) -> Callable:
        waiter = HivePayloadListener(bus=bus, payload_type=payload_type,
                                     message_type=HiveMessageType.HELLO)
        waiter.add_handler(func)
        waiter.listen()
        func.shutdown = waiter.shutdown
        return func
    return wrapped_handler


def on_query(payload_type: str,
             bus: 'HiveMessageBusClient') -> Callable:
    """Decorator: register the wrapped function for ``QUERY`` messages.

    Args:
        payload_type: The inner message type to match.
        bus: The bus to register on.
    """
    def wrapped_handler(func: Callable) -> Callable:
        waiter = HivePayloadListener(bus=bus, payload_type=payload_type,
                                     message_type=HiveMessageType.QUERY)
        waiter.add_handler(func)
        waiter.listen()
        func.shutdown = waiter.shutdown
        return func
    return wrapped_handler


def on_cascade(payload_type: str,
               bus: 'HiveMessageBusClient') -> Callable:
    """Decorator: register the wrapped function for ``CASCADE`` messages.

    Args:
        payload_type: The inner message type to match.
        bus: The bus to register on.
    """
    def wrapped_handler(func: Callable) -> Callable:
        waiter = HivePayloadListener(bus=bus, payload_type=payload_type,
                                     message_type=HiveMessageType.CASCADE)
        waiter.add_handler(func)
        waiter.listen()
        func.shutdown = waiter.shutdown
        return func
    return wrapped_handler


def on_rendezvous(payload_type: str,
                  bus: 'HiveMessageBusClient') -> Callable:
    """Decorator: register the wrapped function for ``RENDEZVOUS`` messages.

    Args:
        payload_type: The inner message type to match.
        bus: The bus to register on.
    """
    def wrapped_handler(func: Callable) -> Callable:
        waiter = HivePayloadListener(bus=bus, payload_type=payload_type,
                                     message_type=HiveMessageType.RENDEZVOUS)
        waiter.add_handler(func)
        waiter.listen()
        func.shutdown = waiter.shutdown
        return func
    return wrapped_handler


def on_third_party(bus: 'HiveMessageBusClient') -> Callable:
    """Decorator: register the wrapped function for ``THIRDPRTY`` messages.

    Args:
        bus: The bus to register on.
    """
    def wrapped_handler(func: Callable) -> Callable:
        waiter = HiveMessageListener(bus=bus,
                                     message_type=HiveMessageType.THIRDPRTY)
        waiter.add_handler(func)
        waiter.listen()
        func.shutdown = waiter.shutdown
        return func
    return wrapped_handler


def on_payload(message_type: Union[HiveMessageType, str],
               payload_type: str,
               bus: 'HiveMessageBusClient') -> Callable:
    """Decorator: register the wrapped function for a specific outer + inner type pair.

    Args:
        message_type: The outer ``HiveMessageType`` to listen for.
        payload_type: The inner payload message type to match.
        bus: The bus to register on.
    """
    def wrapped_handler(func: Callable) -> Callable:
        waiter = HivePayloadListener(bus=bus, payload_type=payload_type,
                                     message_type=message_type)
        waiter.add_handler(func)
        waiter.listen()
        func.shutdown = waiter.shutdown
        return func
    return wrapped_handler
