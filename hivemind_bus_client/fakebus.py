"""In-process stand-ins for the HiveMind bus clients.

Currently exposes:

- :class:`AsyncFakeHiveMessageBus` — an asyncio-native fake that mirrors
  :class:`hivemind_bus_client.async_client.AsyncHiveMessageBusClient`'s
  public surface without opening a WebSocket or completing a handshake.

The intended consumers are:

1. **Tests** for code that talks to the HiveMind bus via the async client.
   No need to stand up a HiveMind-core instance or stub the handshake by
   hand.
2. **Runtime fallback** wherever a HiveMind client is expected but no
   server is configured (mirroring how ``ovos_utils.fakebus.FakeBus`` is
   used as a default-bus today).

A sync ``FakeHiveMessageBus`` could be added here in the future if a
consumer needs one; today, none does.
"""
from __future__ import annotations

import asyncio
import json
from typing import Awaitable, Callable, Optional, Union

from ovos_bus_client.message import Message as MycroftMessage
from ovos_utils.fakebus import FakeBus
from ovos_utils.log import LOG
from pyee import EventEmitter

from hivemind_bus_client.message import HiveMessage, HiveMessageType


class AsyncFakeHiveMessageBus:
    """asyncio-native stand-in for ``AsyncHiveMessageBusClient``.

    Mirrors the real client's surface so consumer code can be tested or
    embedded without an actual WebSocket connection or a HiveMind-core
    instance:

    - ``connect``, ``close``, ``emit``, ``wait_for_message``,
      ``wait_for_payload``, ``wait_for_response``,
      ``wait_for_handshake``, ``emit_mycroft``, ``emit_intercom`` are
      coroutines.
    - ``on``, ``once``, ``remove``, ``on_mycroft`` are synchronous —
      matches the real client, which uses ``pyee.EventEmitter`` for
      dispatch and never required awaitable callbacks.

    What's faked vs. what's real:

    - **No WebSocket.** ``connect()`` is a no-op that flips
      ``connected_event`` and ``handshake_event``. No transport, no
      retries, no handshake state machine.
    - **No encryption / serialization.** ``emit()`` dispatches the
      ``HiveMessage`` (or wrapped Mycroft ``Message``) directly through
      the emitter; payload framing, AES-GCM, and bitstring serialization
      are skipped because no wire bytes are produced.
    - **Real routing-context injection.** For ``HiveMessageType.BUS``
      messages, ``emit()`` injects the same ``context["source"]``,
      ``platform``, ``destination``, and ``session`` keys the real
      client would, so downstream session-aware tests behave identically.
    - **Real internal-bus dispatch.** Mycroft handlers registered via
      ``on()`` (when the event name is not a ``HiveMessageType``) attach
      to a real :class:`ovos_utils.fakebus.FakeBus`, just like the real
      client.

    Lifecycle::

        bus = AsyncFakeHiveMessageBus()
        await bus.connect()
        bus.on(HiveMessageType.BUS, my_handler)
        await bus.emit(MycroftMessage("speak", {"utterance": "hi"}))
        reply = await bus.wait_for_response(query, timeout=1.0)
        await bus.close()
    """

    def __init__(self,
                 *,
                 session_id: str = "fake-hivemind-session",
                 site_id: str = "fake-site",
                 useragent: str = "AsyncFakeHiveMessageBus",
                 internal_bus: Optional[FakeBus] = None):
        self.session_id = session_id
        self._site_id = site_id
        self._useragent = useragent
        self.emitter = EventEmitter()
        self.internal_bus = internal_bus or FakeBus()
        self.connected_event = asyncio.Event()
        self.handshake_event = asyncio.Event()
        self.emitted: list[HiveMessage] = []
        self.crypto_key: Optional[str] = None  # parity attribute; never set
        # Backwards-compat parity attributes that real client exposes
        self.compress = False
        self.binarize = False

    # ------------------------------------------------------------------
    # identity-shaped properties
    # ------------------------------------------------------------------

    @property
    def useragent(self) -> str:
        return self._useragent

    @useragent.setter
    def useragent(self, val: str):
        self._useragent = val

    @property
    def site_id(self) -> str:
        return self._site_id

    @site_id.setter
    def site_id(self, val: str):
        self._site_id = val

    # ------------------------------------------------------------------
    # lifecycle (async)
    # ------------------------------------------------------------------

    async def connect(self, bus=None, protocol=None, site_id: Optional[str] = None):
        """No-op connect: flips connected_event + handshake_event, registers
        no transport. ``bus``, ``protocol``, ``site_id`` mirror the real
        client's signature so call sites are drop-in."""
        if site_id is not None:
            self._site_id = site_id
        if bus is not None:
            # If caller hands us a "core" bus to bridge into, use it.
            self.internal_bus = bus
        self.connected_event.set()
        self.handshake_event.set()
        return self

    async def close(self):
        self.connected_event.clear()
        self.handshake_event.clear()
        self.crypto_key = None

    async def wait_for_handshake(self, timeout: float = 5.0, max_retries: int = 15):
        # Always set in connect(); return immediately.
        if not self.handshake_event.is_set():
            try:
                await asyncio.wait_for(self.handshake_event.wait(), timeout=timeout)
            except asyncio.TimeoutError:
                raise RuntimeError("timed out waiting for handshake")

    # ------------------------------------------------------------------
    # handler registration (sync — matches real async client)
    # ------------------------------------------------------------------

    def on(self, event_name: Union[HiveMessageType, str], func: Callable):
        if event_name not in list(HiveMessageType):
            # mycroft event → internal bus
            self.internal_bus.on(event_name, func)
        else:
            self.emitter.on(event_name, func)

    def once(self, event_name: Union[HiveMessageType, str], func: Callable):
        if event_name not in list(HiveMessageType):
            self.internal_bus.once(event_name, func)
        else:
            self.emitter.once(event_name, func)

    def remove(self, event_name: Union[HiveMessageType, str], func: Callable):
        try:
            if event_name not in list(HiveMessageType):
                self.internal_bus.remove(event_name, func)
            else:
                self.emitter.remove_listener(event_name, func)
        except Exception:
            pass

    def on_mycroft(self, mycroft_msg_type: str, func: Callable):
        self.internal_bus.on(mycroft_msg_type, func)

    # ------------------------------------------------------------------
    # emit (async)
    # ------------------------------------------------------------------

    async def emit(self,
                   message: Union[MycroftMessage, HiveMessage],
                   binary_type=None):
        """Same routing-context injection and dispatch shape as the real
        client. Records the message in ``self.emitted`` for test assertions.
        """
        if isinstance(message, MycroftMessage):
            message = HiveMessage(msg_type=HiveMessageType.BUS, payload=message)

        # Routing context for BUS messages (same as real client)
        if message.msg_type == HiveMessageType.BUS:
            payload = message.payload
            ctx = payload.context
            ctx.setdefault("source", self._useragent)
            ctx.setdefault("platform", self._useragent)
            ctx.setdefault("destination", "HiveMind")
            ctx.setdefault("session", {})
            ctx["session"]["session_id"] = self.session_id
            ctx["session"]["site_id"] = self._site_id
            message.payload = payload
            # surface to internal-bus subscribers
            self.internal_bus.emit(message.payload)

        self.emitted.append(message)
        # raw 'message' event (mirror real client)
        try:
            self.emitter.emit("message", message.serialize())
        except Exception:
            LOG.debug("could not serialize message in fake emit; skipping raw event")
        # hive-type event
        self.emitter.emit(message.msg_type, message)

    async def emit_mycroft(self, message: MycroftMessage):
        await self.emit(
            HiveMessage(msg_type=HiveMessageType.BUS, payload=message)
        )

    async def emit_intercom(self, message, pubkey):
        """Pass-through for INTERCOM emit — no encryption in the fake."""
        await self.emit(HiveMessage(HiveMessageType.INTERCOM, payload=message))

    # ------------------------------------------------------------------
    # waiters
    # ------------------------------------------------------------------

    async def wait_for_message(self,
                               message_type: Union[HiveMessageType, str],
                               timeout: float = 3.0) -> Optional[HiveMessage]:
        evt = asyncio.Event()
        captured = {"msg": None}

        def _rcv(m):
            captured["msg"] = m
            evt.set()

        self.emitter.once(message_type, _rcv)
        try:
            await asyncio.wait_for(evt.wait(), timeout=timeout)
        except asyncio.TimeoutError:
            pass
        return captured["msg"]

    async def wait_for_payload(self,
                               payload_type: Union[HiveMessageType, str],
                               message_type: Union[HiveMessageType, str] = HiveMessageType.THIRDPRTY,
                               timeout: float = 3.0) -> Optional[HiveMessage]:
        evt = asyncio.Event()
        captured = {"msg": None}

        def _rcv(m):
            if getattr(m.payload, "msg_type", None) == payload_type:
                captured["msg"] = m
                evt.set()

        self.emitter.on(message_type, _rcv)
        try:
            await asyncio.wait_for(evt.wait(), timeout=timeout)
        except asyncio.TimeoutError:
            pass
        finally:
            self.emitter.remove_listener(message_type, _rcv)
        return captured["msg"]

    async def wait_for_mycroft(self, mycroft_msg_type: str,
                               timeout: float = 3.0) -> Optional[HiveMessage]:
        return await self.wait_for_payload(
            mycroft_msg_type, timeout=timeout,
            message_type=HiveMessageType.BUS,
        )

    async def wait_for_response(self,
                                message: Union[MycroftMessage, HiveMessage],
                                reply_type: Optional[Union[HiveMessageType, str]] = None,
                                timeout: float = 3.0) -> Optional[HiveMessage]:
        message_type = reply_type or message.msg_type
        evt = asyncio.Event()
        captured = {"msg": None}

        if isinstance(message, MycroftMessage):
            # match by inner payload type, like real client's HivePayloadWaiter
            def _rcv(m):
                if getattr(m.payload, "msg_type", None) == message_type:
                    captured["msg"] = m
                    evt.set()
            self.emitter.on(HiveMessageType.BUS, _rcv)
        else:
            def _rcv(m):
                captured["msg"] = m
                evt.set()
            self.emitter.once(message_type, _rcv)

        await self.emit(message)
        try:
            await asyncio.wait_for(evt.wait(), timeout=timeout)
        except asyncio.TimeoutError:
            pass
        finally:
            if isinstance(message, MycroftMessage):
                try:
                    self.emitter.remove_listener(HiveMessageType.BUS, _rcv)
                except Exception:
                    pass
        return captured["msg"]


__all__ = ["AsyncFakeHiveMessageBus"]
