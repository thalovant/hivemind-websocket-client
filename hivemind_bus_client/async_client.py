"""asyncio-native HiveMind bus client.

Mirrors :class:`hivemind_bus_client.client.HiveMessageBusClient` but uses
the :mod:`websockets` library and :mod:`asyncio` primitives. All
transport-independent pieces (handshake state machine, encryption,
binary serialization, ``HiveMessage`` envelopes, ``NodeIdentity``,
``HiveMindSlaveProtocol``) are reused without modification.

This is an optional path: bare ``pip install hivemind-bus-client`` keeps
the threading-based client and does not require ``websockets``. To use
this module, install with ``pip install hivemind-bus-client[async]``.

Public surface mirrors the sync client where it makes sense:

- :class:`AsyncHiveMessageBusClient` — connect/emit/wait via ``await``.
- :class:`AsyncHiveMessageWaiter` / :class:`AsyncHivePayloadWaiter` —
  asyncio.Event-based waiters.

Handler registration (``on`` / ``once`` / ``remove``) stays synchronous,
keeping :class:`hivemind_bus_client.protocol.HiveMindSlaveProtocol`
drop-in compatible with both clients.
"""
from __future__ import annotations

import asyncio
import json
import ssl
from typing import Awaitable, Callable, Optional, Union

import pybase64
from ovos_bus_client import Message as MycroftMessage
from ovos_bus_client.session import Session
from ovos_utils.fakebus import FakeBus
from ovos_utils.log import LOG
from pyee import EventEmitter

try:  # optional dep
    import websockets
    from websockets.exceptions import (ConnectionClosed,
                                       ConnectionClosedError,
                                       ConnectionClosedOK)
except ImportError:  # pragma: no cover - import-time guard
    websockets = None
    ConnectionClosed = ConnectionClosedError = ConnectionClosedOK = Exception

from hivemind_bus_client.encryption import (SupportedCiphers,
                                            SupportedEncodings, decrypt_bin,
                                            decrypt_from_json, encrypt_as_json,
                                            encrypt_bin, hybrid_encrypt)
from hivemind_bus_client.identity import NodeIdentity
from hivemind_bus_client.noise import NoiseTransportFailed
from hivemind_bus_client.keepalive import websocket_keepalive_options
from hivemind_bus_client.message import HiveMessage, HiveMessageType
from hivemind_bus_client.serialization import (HiveMindBinaryPayloadType,
                                               decode_bitstring, get_bitstring)
from hivemind_bus_client.util import serialize_message


_MISSING_WEBSOCKETS = (
    "The 'websockets' package is required for AsyncHiveMessageBusClient. "
    "Install it with: pip install hivemind-bus-client[async]"
)


class BinaryDataCallbacks:
    """Same shape as the sync client's callbacks; sync methods called from
    the receive task. Override if you need to handle TTS/file binaries."""

    def handle_receive_tts(self, bin_data: bytes, utterance: str,
                           lang: str, file_name: str):
        LOG.warning(f"Ignoring received binary TTS audio: {utterance} "
                    f"with {len(bin_data)} bytes")

    def handle_receive_file(self, bin_data: bytes, file_name: str):
        LOG.warning(f"Ignoring received binary file: {file_name} "
                    f"with {len(bin_data)} bytes")


class AsyncHiveMessageWaiter:
    """Wait for one HiveMessage of a given type. Same semantics as the
    sync ``HiveMessageWaiter`` but on ``asyncio.Event``."""

    def __init__(self, bus: "AsyncHiveMessageBusClient",
                 message_type: Union[HiveMessageType, str]):
        self.bus = bus
        self.msg_type = message_type
        self.received_msg: Optional[HiveMessage] = None
        self._event = asyncio.Event()
        self.bus.on(self.msg_type, self._handler)

    def _handler(self, message: HiveMessage):
        self.received_msg = message
        self._event.set()

    async def wait(self, timeout: float = 3.0) -> Optional[HiveMessage]:
        try:
            await asyncio.wait_for(self._event.wait(), timeout=timeout)
        except asyncio.TimeoutError:
            pass
        finally:
            self.bus.remove(self.msg_type, self._handler)
        return self.received_msg


class AsyncHivePayloadWaiter(AsyncHiveMessageWaiter):
    """Wait for a HiveMessage whose inner payload matches ``payload_type``."""

    def __init__(self, bus: "AsyncHiveMessageBusClient",
                 payload_type: Union[HiveMessageType, str],
                 message_type: Union[HiveMessageType, str] = HiveMessageType.BUS):
        super().__init__(bus=bus, message_type=message_type)
        self.payload_type = payload_type

    def _handler(self, message: HiveMessage):
        if getattr(message.payload, "msg_type", None) == self.payload_type:
            super()._handler(message)


class AsyncHiveMessageBusClient:
    """asyncio-native HiveMind bus client.

    Parameters mirror the sync :class:`HiveMessageBusClient`. The connect
    lifecycle is explicit: instantiate, then ``await connect()``. All I/O
    methods are coroutines; handler registration (``on``, ``once``,
    ``remove``) is synchronous so existing
    :class:`HiveMindSlaveProtocol` handlers work unchanged.

    Typical usage::

        bus = AsyncHiveMessageBusClient(key="...", password="...")
        await bus.connect()
        await bus.emit(HiveMessage(HiveMessageType.BUS, payload=mycroft_msg))
        reply = await bus.wait_for_response(query, timeout=3.0)
        await bus.close()
    """

    def __init__(self,
                 key: Optional[str] = None,
                 password: Optional[str] = None,
                 crypto_key: Optional[str] = None,
                 host: Optional[str] = None,
                 port: Optional[int] = None,
                 useragent: str = "",
                 self_signed: bool = True,
                 share_bus: bool = False,
                 compress: bool = True,
                 binarize: bool = True,
                 identity: Optional[NodeIdentity] = None,
                 internal_bus=None,
                 bin_callbacks: Optional[BinaryDataCallbacks] = None,
                 websocket_ping_interval: Optional[float] = None,
                 websocket_ping_timeout: Optional[float] = None,
                 max_protocol_version: int = 3):
        if websockets is None:
            raise ImportError(_MISSING_WEBSOCKETS)

        self.bin_callbacks = bin_callbacks or BinaryDataCallbacks()
        # highest HiveMind protocol version this client will negotiate
        # (HIVEMIND-WIRE-1 §2), mirroring the sync client. 3 -> Noise
        # handshake when the server also supports it; servers capped at 2 or
        # lower fall back to the legacy handshake transparently. The shared
        # HiveMindSlaveProtocol._should_use_noise reads this attribute, so it
        # MUST exist for v3 Noise to be negotiated. Set to 2 to force legacy.
        self.max_protocol_version = max_protocol_version
        # established protocol v3 Noise session (None on v2 and below)
        self.noise_transport = None
        self.json_encoding = SupportedEncodings.JSON_HEX
        self.cipher = SupportedCiphers.AES_GCM

        self.identity = identity
        self._password = password
        self._access_key = key
        self._name = useragent
        self._port = port
        self._host = host
        self.init_identity()

        self.crypto_key = crypto_key
        self.allow_self_signed = self_signed
        self.share_bus = share_bus
        self.compress = compress
        self.binarize = binarize
        self.websocket_ping_interval = websocket_ping_interval
        self.websocket_ping_timeout = websocket_ping_timeout

        # asyncio primitives
        self.connected_event = asyncio.Event()
        self.handshake_event = asyncio.Event()

        # internal OVOS bus (FakeBus in async mode unless caller provides one)
        if not internal_bus:
            sess = Session()
            self.internal_bus = FakeBus(session=sess)
        else:
            sess = Session(session_id=internal_bus.session_id)
            self.internal_bus = internal_bus
        self.session_id = sess.session_id
        LOG.info(f"Session ID: {sess.session_id}")

        self.emitter = EventEmitter()
        self._ws: Optional["websockets.WebSocketClientProtocol"] = None
        self._receive_task: Optional[asyncio.Task] = None
        self.protocol = None  # bound in connect()

    # ------------------------------------------------------------------
    # identity / config — copied from sync client (CPU-only, no I/O)
    # ------------------------------------------------------------------

    def init_identity(self, site_id: Optional[str] = None):
        self.identity = self.identity or NodeIdentity()
        self.identity.password = self._password or self.identity.password
        self.identity.access_key = self._access_key or self.identity.access_key
        self.identity.default_master = self._host = self._host or self.identity.default_master
        self.identity.default_port = self._port = self._port or self.identity.default_port
        self.identity.name = self._name or "AsyncHiveMessageBusClientV0.0.1"
        self.identity.site_id = site_id or self.identity.site_id

        if not self.identity.access_key or not self.identity.password:
            raise RuntimeError(
                "NodeIdentity not set, please pass key and password or "
                "call 'hivemind-client set-identity'")
        if not self.identity.default_master:
            raise RuntimeError(
                "host not set, please pass host and port or "
                "call 'hivemind-client set-identity'")

    @property
    def useragent(self) -> str:
        return self.identity.name

    @useragent.setter
    def useragent(self, val: str):
        self.identity.name = val

    @property
    def password(self) -> str:
        return self.identity.password

    @password.setter
    def password(self, val: str):
        self.identity.password = val

    @property
    def key(self) -> str:
        return self.identity.access_key

    @key.setter
    def key(self, val: str):
        self.identity.access_key = val

    @property
    def site_id(self) -> Optional[str]:
        return self.identity.site_id

    @site_id.setter
    def site_id(self, val: str):
        self.identity.site_id = val

    @staticmethod
    def build_url(key: str, host: str = "127.0.0.1", port: int = 5678,
                  useragent: str = "AsyncHiveMessageBusClientV0.0.1",
                  ssl: bool = True) -> str:
        scheme = "wss" if ssl else "ws"
        encoded = pybase64.b64encode(f"{useragent}:{key}".encode("utf-8")).decode("utf-8")
        return f"{scheme}://{host}:{port}?authorization={encoded}"

    # ------------------------------------------------------------------
    # connection lifecycle
    # ------------------------------------------------------------------

    async def connect(self, bus=None, protocol=None, site_id: Optional[str] = None):
        """Open the WebSocket, bind the slave protocol, complete handshake.

        Blocks (awaits) until the HiveMind handshake completes successfully
        or raises if it times out.
        """
        from hivemind_bus_client.protocol import HiveMindSlaveProtocol

        bus = bus if bus is not None else FakeBus()
        self.identity.site_id = site_id or self.identity.site_id

        if protocol is None:
            LOG.debug("Initializing HiveMindSlaveProtocol")
            self.protocol = HiveMindSlaveProtocol(
                self,
                shared_bus=self.share_bus,
                site_id=self.identity.site_id or "unknown",
                identity=self.identity,
            )
        else:
            self.protocol = protocol
            self.protocol.identity = self.identity
            if self.identity.site_id is not None:
                self.protocol.site_id = self.identity.site_id

        host = self._host
        use_ssl = host.startswith("wss://")
        host = host.replace("ws://", "").replace("wss://", "").strip()
        url = self.build_url(
            key=self.key, host=host, port=self._port,
            useragent=self.useragent, ssl=use_ssl,
        )
        LOG.info(f"Connecting to HiveMind: {url}")

        ssl_ctx = None
        if use_ssl and self.allow_self_signed:
            ssl_ctx = ssl.SSLContext(ssl.PROTOCOL_TLS_CLIENT)
            ssl_ctx.check_hostname = False
            ssl_ctx.verify_mode = ssl.CERT_NONE

        self._ws = await websockets.connect(
            url, ssl=ssl_ctx, **self._websocket_keepalive_options()
        )
        self.connected_event.set()
        self.emitter.emit("open")
        self._receive_task = asyncio.create_task(self._receive_loop())

        # Bind protocol — its bind() registers handlers on the emitter and
        # may emit the handshake start.
        self.protocol.bind(bus)
        await self.wait_for_handshake()

    async def close(self):
        """Cleanly close the WebSocket and stop the receive loop."""
        self.handshake_event.clear()
        self.crypto_key = None
        self.noise_transport = None
        self.connected_event.clear()
        if self._ws is not None:
            try:
                await self._ws.close()
            except Exception:
                pass
        if self._receive_task is not None:
            self._receive_task.cancel()
            try:
                await self._receive_task
            except (asyncio.CancelledError, Exception):
                pass
        self.emitter.emit("close")

    async def wait_for_handshake(self, timeout: float = 5.0, max_retries: Optional[int] = None):
        """Wait until the HiveMind handshake state machine reports success.

        Mirrors the sync client's retry loop, using asyncio sleeps. The
        default is to keep waiting through reconnects forever.
        """
        attempts = 0
        while not self.handshake_event.is_set():
            try:
                await asyncio.wait_for(self.handshake_event.wait(), timeout=timeout)
            except asyncio.TimeoutError:
                pass
            if self.handshake_event.is_set():
                return
            if max_retries is not None and attempts >= max_retries:
                raise RuntimeError("timed out waiting for handshake")
            attempts += 1

            if self.connected_event.is_set():
                self.protocol.start_handshake()
            else:
                LOG.warning("Can't start handshake — websocket not yet open")
                try:
                    await asyncio.wait_for(self.connected_event.wait(), timeout=timeout)
                except asyncio.TimeoutError:
                    pass

    def _websocket_keepalive_options(self):
        return websocket_keepalive_options(
            ping_interval=self.websocket_ping_interval,
            ping_timeout=self.websocket_ping_timeout,
            disabled_interval_value=None,
        )

    # ------------------------------------------------------------------
    # receive loop and message dispatch
    # ------------------------------------------------------------------

    async def _receive_loop(self):
        """Consume frames from the WebSocket and feed them to on_message."""
        try:
            async for raw in self._ws:
                try:
                    self.on_message(raw)
                except Exception:
                    LOG.exception("Error in on_message")
        except (ConnectionClosedOK, ConnectionClosedError, ConnectionClosed) as e:
            LOG.debug(f"WebSocket closed: {e!r}")
            self.handshake_event.clear()
            self.crypto_key = None
            self.noise_transport = None
            self.connected_event.clear()
            self.emitter.emit("close")
        except asyncio.CancelledError:
            raise
        except Exception:
            LOG.exception("Unexpected error in receive loop")
            self.emitter.emit("error")
            self.handshake_event.clear()
            self.crypto_key = None
            self.noise_transport = None
            self.connected_event.clear()

    def on_message(self, message):
        """Decode + dispatch a single WS frame. Mirrors the sync client."""
        if self.noise_transport is not None:
            # protocol v3: every post-handshake message is a Noise transport
            # message; there is no cleartext v3 session (CRYPTO-1 §3.4.5)
            if not isinstance(message, (bytes, bytearray)):
                LOG.error("dropping non-Noise message received on a "
                          "protocol v3 session")
                return
            try:
                message = self.noise_transport.decrypt_frame(bytes(message))
            except NoiseTransportFailed:
                # tampered / replayed / out-of-order — MUST reject; the
                # receive counter is now out of sync so the session is dead
                LOG.exception("rejecting invalid Noise transport message, "
                              "closing connection")
                asyncio.ensure_future(self.close())
                return
        elif self.crypto_key:
            if isinstance(message, (bytes, bytearray)):
                message = decrypt_bin(self.crypto_key, message, cipher=self.cipher)
            elif "ciphertext" in message:
                message = decrypt_from_json(self.crypto_key, message,
                                            cipher=self.cipher,
                                            encoding=self.json_encoding)
            else:
                LOG.debug("Message was unencrypted")

        if isinstance(message, (bytes, bytearray)):
            message = decode_bitstring(message)
        elif isinstance(message, str):
            message = json.loads(message)
        if isinstance(message, dict) and "ciphertext" in message:
            LOG.error("got encrypted message, but could not decrypt!")
            return

        if isinstance(message, HiveMessage) and message.msg_type == HiveMessageType.BINARY:
            self._handle_binary(message)
            return

        if isinstance(message, HiveMessage):
            self.emitter.emit("message", message.serialize())
            self._handle_hive_protocol(message)
        elif isinstance(message, str):
            self.emitter.emit("message", message)
            self._handle_hive_protocol(HiveMessage(**json.loads(message)))
        else:
            assert isinstance(message, dict)
            self.emitter.emit("message", json.dumps(message, ensure_ascii=False))
            self._handle_hive_protocol(HiveMessage(**message))

    def _handle_binary(self, message: HiveMessage):
        assert message.msg_type == HiveMessageType.BINARY
        bin_data = message.payload
        LOG.debug(f"Got binary data of type: {message.bin_type}")
        if message.bin_type == HiveMindBinaryPayloadType.TTS_AUDIO:
            lang = message.metadata.get("lang")
            utt = message.metadata.get("utterance")
            file_name = message.metadata.get("file_name")
            try:
                self.bin_callbacks.handle_receive_tts(bin_data, utt, lang, file_name)
            except Exception:
                LOG.exception("Error in binary callback: handle_receive_tts")
        elif message.bin_type == HiveMindBinaryPayloadType.FILE:
            file_name = message.metadata.get("file_name")
            try:
                self.bin_callbacks.handle_receive_file(bin_data, file_name)
            except Exception:
                LOG.exception("Error in binary callback: handle_receive_file")
        else:
            LOG.warning(f"Ignoring received untyped binary data: {len(bin_data)} bytes")

    def _handle_hive_protocol(self, message: HiveMessage):
        if message.msg_type == HiveMessageType.BUS:
            self.internal_bus.emit(message.payload)
        self.emitter.emit(message.msg_type, message)

    # ------------------------------------------------------------------
    # emit
    # ------------------------------------------------------------------

    async def emit(self, message: Union[MycroftMessage, HiveMessage],
                   binary_type: HiveMindBinaryPayloadType = HiveMindBinaryPayloadType.UNDEFINED):
        """Send a HiveMessage (or Mycroft Message wrapped as BUS) to the
        HiveMind. Awaits the connection if not yet open.

        Performs the same routing-context injection, optional binary
        framing, and optional encryption as the sync client.
        """
        if isinstance(message, MycroftMessage):
            message = HiveMessage(msg_type=HiveMessageType.BUS, payload=message)

        if not self.connected_event.is_set():
            LOG.warning("hivemind connection not ready!")
            try:
                await asyncio.wait_for(self.connected_event.wait(), timeout=10)
            except asyncio.TimeoutError:
                raise RuntimeError(
                    f"Can not send messages before opening the websocket "
                    f"connection. Failed to emit: {message.serialize()}")

        try:
            # Auto-inject routing context for BUS messages
            if message.msg_type == HiveMessageType.BUS:
                payload = message.payload
                ctx = payload.context
                ctx.setdefault("source", self.useragent)
                ctx.setdefault("platform", self.useragent)
                ctx.setdefault("destination", "HiveMind")
                ctx.setdefault("session", {})
                ctx["session"]["session_id"] = self.session_id
                ctx["session"]["site_id"] = self.site_id
                message.payload = payload
                # also surface to internal-bus subscribers
                self.internal_bus.emit(message.payload)

            LOG.debug(f"sending to HiveMind: {message.msg_type}")

            binarize = False
            if message.msg_type == HiveMessageType.BINARY:
                binarize = True
            elif message.msg_type not in (HiveMessageType.HELLO,
                                          HiveMessageType.HANDSHAKE):
                binarize = (getattr(self.protocol, "binarize", False)
                            and self.binarize)

            if binarize:
                bitstr = get_bitstring(hive_type=message.msg_type,
                                       payload=message.payload,
                                       compressed=self.compress,
                                       binary_type=binary_type,
                                       hivemeta=message.metadata)
                if self.noise_transport is not None:
                    # protocol v3: Noise transport CipherState (replay
                    # resistant sequential nonces) replaces the v2 AEAD
                    payload_bytes = self.noise_transport.encrypt_frame(bitstr.bytes)
                elif self.crypto_key:
                    payload_bytes = encrypt_bin(self.crypto_key, bitstr.bytes,
                                                cipher=self.cipher)
                else:
                    payload_bytes = bitstr.bytes
                await self._ws.send(payload_bytes)
            else:
                ws_payload = serialize_message(message)
                # HANDSHAKE frames carry the Noise handshake itself and are
                # ALWAYS cleartext — they must never be transport-encrypted.
                # The sync client relies on timing for this (noise_transport is
                # still None when the final handshake frame is sent); on the
                # async client emit is deferred onto the loop and runs after
                # noise_transport is assigned, so guard on the type explicitly.
                if (self.noise_transport is not None
                        and message.msg_type != HiveMessageType.HANDSHAKE):
                    # v3 sessions are always encrypted, HELLO included
                    await self._ws.send(self.noise_transport.encrypt_frame(ws_payload))
                    return
                if self.crypto_key:
                    ws_payload = encrypt_as_json(
                        self.crypto_key, ws_payload,
                        cipher=self.cipher, encoding=self.json_encoding,
                    )
                await self._ws.send(ws_payload)
        except (ConnectionClosedOK, ConnectionClosedError, ConnectionClosed):
            LOG.warning(f"Could not send {message.msg_type} message because "
                        "connection has been closed")

    async def emit_mycroft(self, message: MycroftMessage):
        await self.emit(HiveMessage(msg_type=HiveMessageType.BUS, payload=message))

    def on_mycroft(self, mycroft_msg_type: str, func: Callable):
        LOG.debug(f"registering mycroft event: {mycroft_msg_type}")
        self.internal_bus.on(mycroft_msg_type, func)

    # ------------------------------------------------------------------
    # handler registration (sync, matching sync client)
    # ------------------------------------------------------------------

    def on(self, event_name: Union[HiveMessageType, str], func: Callable):
        if event_name not in list(HiveMessageType):
            self.on_mycroft(event_name, func)
        else:
            LOG.debug(f"registering handler: {event_name}")
            self.emitter.on(event_name, func)

    def once(self, event_name: Union[HiveMessageType, str], func: Callable):
        if event_name not in list(HiveMessageType):
            # FakeBus exposes once() the same way pyee does
            self.internal_bus.once(event_name, func)
        else:
            self.emitter.once(event_name, func)

    def remove(self, event_name: Union[HiveMessageType, str], func: Callable):
        if event_name not in list(HiveMessageType):
            self.internal_bus.remove(event_name, func)
        else:
            self.emitter.remove_listener(event_name, func)

    # ------------------------------------------------------------------
    # waiters
    # ------------------------------------------------------------------

    async def wait_for_message(self,
                               message_type: Union[HiveMessageType, str],
                               timeout: float = 3.0) -> Optional[HiveMessage]:
        return await AsyncHiveMessageWaiter(self, message_type).wait(timeout)

    async def wait_for_payload(self,
                               payload_type: Union[HiveMessageType, str],
                               message_type: Union[HiveMessageType, str] = HiveMessageType.THIRDPRTY,
                               timeout: float = 3.0) -> Optional[HiveMessage]:
        return await AsyncHivePayloadWaiter(
            bus=self, payload_type=payload_type, message_type=message_type,
        ).wait(timeout)

    async def wait_for_mycroft(self, mycroft_msg_type: str,
                               timeout: float = 3.0) -> Optional[HiveMessage]:
        return await self.wait_for_payload(
            mycroft_msg_type, timeout=timeout, message_type=HiveMessageType.BUS,
        )

    async def wait_for_response(self,
                                message: Union[MycroftMessage, HiveMessage],
                                reply_type: Optional[Union[HiveMessageType, str]] = None,
                                timeout: float = 3.0) -> Optional[HiveMessage]:
        message_type = reply_type or message.msg_type
        if isinstance(message, MycroftMessage):
            waiter = AsyncHivePayloadWaiter(bus=self, payload_type=message_type)
        else:
            waiter = AsyncHiveMessageWaiter(bus=self, message_type=message_type)
        await self.emit(message)
        return await waiter.wait(timeout)

    async def wait_for_payload_response(
        self, message: Union[MycroftMessage, HiveMessage],
        payload_type: Union[HiveMessageType, str],
        reply_type: Optional[Union[HiveMessageType, str]] = None,
        timeout: float = 3.0,
    ) -> Optional[HiveMessage]:
        if isinstance(message, MycroftMessage):
            message = HiveMessage(msg_type=HiveMessageType.BUS, payload=message)
        message_type = reply_type or message.msg_type
        waiter = AsyncHivePayloadWaiter(
            bus=self, payload_type=payload_type, message_type=message_type,
        )
        await self.emit(message)
        return await waiter.wait(timeout)

    # ------------------------------------------------------------------
    # targeted (INTERCOM) helpers
    # ------------------------------------------------------------------

    async def emit_intercom(self,
                            message: Union[MycroftMessage, HiveMessage],
                            pubkey):
        """INTERCOM (hybrid-encrypted) send. Same shape as sync client."""
        from poorman_handshake.asymmetric.utils import load_RSA_key
        private_key = load_RSA_key(self.identity.private_key)
        envelope = hybrid_encrypt(pubkey, message.serialize(), sign_key=private_key)
        await self.emit(HiveMessage(HiveMessageType.INTERCOM, payload=envelope))


__all__ = [
    "AsyncHiveMessageBusClient",
    "AsyncHiveMessageWaiter",
    "AsyncHivePayloadWaiter",
    "BinaryDataCallbacks",
]
