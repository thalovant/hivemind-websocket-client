import base64
import json
import threading
import time
from typing import List, Dict, Callable, Union, Optional

import pybase64

from hivemind_bus_client.noise import NoiseTransportFailed
import requests
from Cryptodome.PublicKey import RSA
from ovos_bus_client import Message as MycroftMessage, MessageBusClient as OVOSBusClient
from ovos_bus_client.session import Session
from ovos_utils.fakebus import FakeBus
from ovos_utils.log import LOG

from hivemind_bus_client.client import BinaryDataCallbacks
from hivemind_bus_client.encryption import (encrypt_as_json, decrypt_from_json, encrypt_bin, decrypt_bin,
                                            SupportedEncodings, SupportedCiphers, hybrid_encrypt)
from hivemind_bus_client.exceptions import MetadataTooLarge
from hivemind_bus_client.identity import NodeIdentity
from hivemind_bus_client.message import HiveMessage, HiveMessageType, HiveMindBinaryPayloadType
from hivemind_bus_client.protocol import HiveMindSlaveProtocol
from hivemind_bus_client.serialization import (BINARY_ENCODABLE_TYPES,
                                               get_bitstring, decode_bitstring)
from hivemind_bus_client.util import serialize_message
from poorman_handshake.asymmetric.utils import load_RSA_key


#: Default per-request timeout (seconds) for every HTTP call to the hub. A dead
#: or slow hub without this hangs the receive loop forever.
HTTP_TIMEOUT = 30

#: Default bound on handshake retries in :meth:`HiveMindHTTPClient.connect`,
#: so a hub that never completes the handshake fails instead of recursing.
DEFAULT_HANDSHAKE_MAX_RETRIES = 5


class HiveMindHTTPClient(threading.Thread):
    """
    A client for the HiveMind HTTP server protocol.
    """

    def __init__(self, key: Optional[str] = None,
                 password: Optional[str] = None,
                 crypto_key: Optional[str] = None,
                 host: Optional[str] = None,
                 port: Optional[int] = None,
                 useragent: str = "HiveMindHTTPClientV1.0",
                 self_signed: bool = True,
                 share_bus: bool = False,
                 compress: bool = True,
                 binarize: bool = True,
                 identity: NodeIdentity = None,
                 internal_bus: Optional[OVOSBusClient] = None,
                 bin_callbacks: Optional[BinaryDataCallbacks] = None,
                 http_timeout: float = HTTP_TIMEOUT,
                 max_protocol_version: int = 3):
        super().__init__(daemon=True)
        # HiveMindSlaveProtocol._should_use_noise() reads this off the client;
        # without it the getattr default of 2 made every HTTP client decline
        # the v3 Noise handshake. Set to 2 to force the legacy handshake.
        self.max_protocol_version = max_protocol_version
        # A mutable default is created once at import and shared across every
        # instance; construct a fresh one per client instead.
        self.bin_callbacks = bin_callbacks or BinaryDataCallbacks()
        self.http_timeout = http_timeout
        self.json_encoding = SupportedEncodings.JSON_HEX  # server defaults before it was made configurable
        self.cipher = SupportedCiphers.AES_GCM  # server defaults before it was made configurable
        self.server_key: Optional[str] = None  # public RSA key
        self.identity = identity or None
        self._password = password
        self._access_key = key
        self._name = useragent
        self._port = port
        self._host = host
        self.init_identity()
        self.crypto_key = crypto_key
        # protocol v3: set by HiveMindSlaveProtocol.receive_noise_handshake()
        # once the Noise session is up, cleared by _abort_noise(); every
        # message after that point goes through it in both directions
        self.noise_transport = None
        self.allow_self_signed = self_signed
        self.share_bus = share_bus
        self.handshake_event = threading.Event()
        # if you want to reduce CPU usage in exchange for more bandwidth set below to False
        self.compress = compress  # None -> auto
        self.binarize = binarize  # only if hivemind reports also supporting it

        # connect to OVOS, if on a OVOS device
        if not internal_bus:
            # FakeBus needed to send emitted events to handlers registered within the client
            sess = Session()  # new session for this client
            self.internal_bus = FakeBus(session=sess)
        else:
            sess = Session(session_id=internal_bus.session_id)
            self.internal_bus = internal_bus
        LOG.info(f"Session ID: {sess.session_id}")
        self.session_id = sess.session_id
        self.stopped = threading.Event()
        self.connected = threading.Event()
        self._handlers: Dict[str, List[Callable[[HiveMessage], None]]] = {}
        self._agent_handlers: Dict[str, List[Callable[[MycroftMessage], None]]] = {}
        self.start()


    def wait_for_handshake(self, timeout=5, max_retries=DEFAULT_HANDSHAKE_MAX_RETRIES):
        """Wait for the handshake, retrying up to ``max_retries`` times.

        This used to recurse on every failed attempt, which turns a hub that
        never handshakes into an unbounded recursion ending in RecursionError.
        A bounded loop raises a clear error once the retries are exhausted.
        """
        attempts = 0
        while not self.handshake_event.is_set():
            self.handshake_event.wait(timeout=timeout)
            if self.handshake_event.is_set():
                break
            if attempts >= max_retries:
                raise ConnectionRefusedError(
                    "timed out waiting for HiveMind handshake")
            attempts += 1
            self.protocol.start_handshake()
        time.sleep(1) # let server process our "hello" response

    @property
    def base_url(self) -> str:
        url = f"{self._host}:{self._port}"
        if url.startswith("ws://"):
            url = url.replace("ws://", "http://")
        elif url.startswith("wss://"):
            url = url.replace("wss://", "https://")
        return url

    @property
    def auth(self) -> str:
        return base64.b64encode(f"{self.useragent}:{self.key}".encode("utf-8")).decode("utf-8")

    @property
    def useragent(self) -> str:
        return self._name

    @useragent.setter
    def useragent(self, val):
        self._name = val

    @property
    def password(self) -> str:
        return self._password

    @property
    def key(self) -> str:
        return self._access_key

    @property
    def site_id(self) -> str:
        return self._site_id

    @site_id.setter
    def site_id(self, val):
        self._site_id = val

    @password.setter
    def password(self, val):
        self._password = val

    @key.setter
    def key(self, val):
        self._access_key = val

    def init_identity(self, site_id=None):
        self.identity = self.identity or NodeIdentity()
        # Credentials say how to reach one master; they are not the node's
        # identity. Writing them back overwrote the node's own access key,
        # password and name on the first save — and pinning a peer key saves.
        self._password = self._password or self.identity.password
        self._access_key = self._access_key or self.identity.access_key
        self._host = self._host or self.identity.default_master
        self._port = self._port or self.identity.default_port
        self._name = self._name or "HiveMessageBusClientV0.0.1"
        self._site_id = site_id or self.identity.site_id

        if not self._access_key or not self._password:
            raise RuntimeError("NodeIdentity not set, please pass key and password or "
                               "call 'hivemind-client set-identity'")
        if not self._host:
            raise RuntimeError("host not set, please pass host and port or "
                               "call 'hivemind-client set-identity'")

    def on_message(self, message: Union[bytes, str]):
        # getattr: subclasses and tests build clients without __init__
        noise_transport = getattr(self, "noise_transport", None)
        if noise_transport is not None:
            # protocol v3: every post-handshake message is a Noise transport
            # frame -- over HTTP it arrives through the binary queue as
            # bytes; there is no cleartext v3 session (CRYPTO-1 §3.4.5)
            if not isinstance(message, bytes):
                LOG.error("dropping non-Noise message received on a "
                          "protocol v3 session")
                return
            try:
                message = noise_transport.decrypt_frame(message)
            except NoiseTransportFailed:
                # tampered / replayed / out-of-order: the receive counter
                # is out of sync, so the session is dead
                LOG.exception("rejecting invalid Noise transport message, "
                              "disconnecting")
                self.close_connection()
                return
            if message is None:
                # a chunk of a multi-frame message, buffered for reassembly
                return
        elif self.crypto_key:
            # handle binary encryption
            if isinstance(message, bytes):
                message = decrypt_bin(self.crypto_key, message, cipher=self.cipher)
            # handle json encryption
            elif "ciphertext" in message:
                # LOG.debug(f"got encrypted message: {len(message)}")
                message = decrypt_from_json(self.crypto_key, message,
                                            cipher=self.cipher, encoding=self.json_encoding)
            else:
                LOG.debug("Message was unencrypted")

        if isinstance(message, bytes):
            try:
                message = decode_bitstring(message)
            except Exception:
                # WIRE-1 §4.2: reject a malformed binary frame (e.g. an
                # unassigned/reserved message-type code) instead of
                # crashing the receive loop.
                LOG.exception("dropping malformed binary frame")
                return
        elif isinstance(message, str):
            message = json.loads(message)
        if isinstance(message, dict) and "ciphertext" in message:
            LOG.error("got encrypted message, but could not decrypt!")
            return

        if isinstance(message, HiveMessage) and message.msg_type == HiveMessageType.BINARY:
            self._handle_binary(message)
            return

        if isinstance(message, HiveMessage):
            self._handle_hive_protocol(message)
        elif isinstance(message, str):
            self._handle_hive_protocol(HiveMessage(**json.loads(message)))
        else:
            assert isinstance(message, dict)
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
            except:
                LOG.exception("Error in binary callback: handle_receive_tts")
        elif message.bin_type == HiveMindBinaryPayloadType.FILE:
            file_name = message.metadata.get("file_name")
            try:
                self.bin_callbacks.handle_receive_file(bin_data, file_name)
            except:
                LOG.exception("Error in binary callback: handle_receive_file")
        else:
            LOG.warning(f"Ignoring received untyped binary data: {len(bin_data)} bytes")

    def _handle_hive_protocol(self, message: HiveMessage):
        LOG.debug(f"received HiveMind message: {message}")
        if message.msg_type == HiveMessageType.HELLO:
            self.protocol.handle_hello(message)
        if message.msg_type == HiveMessageType.HANDSHAKE:
            self.protocol.handle_handshake(message)
        if message.msg_type == HiveMessageType.BUS:
            self.protocol.handle_bus(message)
        if message.msg_type == HiveMessageType.BROADCAST:
            self.protocol.handle_broadcast(message)
        if message.msg_type == HiveMessageType.PROPAGATE:
            self.protocol.handle_propagate(message)
        if message.msg_type == HiveMessageType.INTERCOM:
            self.protocol.handle_intercom(message)

        if message.msg_type in self._handlers:
            for handler in self._handlers[message.msg_type]:
                try:
                    handler(message)
                except Exception as e:
                    LOG.error(f"Error in message handler: {handler} - {e}")
        if message.msg_type == HiveMessageType.BUS and message.payload.msg_type in self._agent_handlers:
            for handler in self._agent_handlers[message.payload.msg_type]:
                try:
                    handler(message.payload)
                except Exception as e:
                    LOG.error(f"Error in agent message handler: {handler} - {e}")

        # these are not supposed to come from server -> client
        if message.msg_type == HiveMessageType.ESCALATE:
            self.protocol.handle_illegal_msg(message)
        if message.msg_type == HiveMessageType.SHARED_BUS:
            self.protocol.handle_illegal_msg(message)

    ###########
    # main loop
    def run(self):
        self.stopped.clear()

        # Connect to the server
        self.connected.wait()

        # Retrieve messages until stop
        while not self.stopped.is_set():
            try:
                messages = self.get_messages() + self.get_binary_messages()
            except RuntimeError as e:
                # A server-error payload used to propagate out of run(),
                # killing this daemon thread while self.connected stayed set —
                # callers then kept emit()-ing into a dead connection. Instead
                # tear the connection down cleanly so the caller can see it.
                LOG.warning(f"HiveMind server returned an error, "
                            f"disconnecting: {e}")
                self.disconnect()
                break
            for hm in messages:
                self.on_message(hm)

            self.stopped.wait(1)

        # Disconnect from the server
        self.disconnect()

    def shutdown(self):
        self.stopped.set()

    #################
    # user facing api
    def on(self, event_name: str, func: Callable):
        if event_name not in self._handlers:
            self._handlers[event_name] = []
        self._handlers[event_name].append(func)

    def on_mycroft(self, event_name: str, func: Callable):
        if event_name not in self._agent_handlers:
            self._agent_handlers[event_name] = []
        self._agent_handlers[event_name].append(func)

    def remove(self, event_name: str, func: Callable):
        if event_name in self._handlers:
            self._handlers[event_name] = [h for h in self._handlers[event_name]
                                          if h is not func]

    def remove_mycroft(self, event_name: str, func: Callable):
        if event_name in self._agent_handlers:
            self._agent_handlers[event_name] = [h for h in self._agent_handlers[event_name]
                                                if h is not func]

    def emit(self, message: Union[MycroftMessage, HiveMessage],
             binary_type: HiveMindBinaryPayloadType = HiveMindBinaryPayloadType.UNDEFINED):
        if not self.connected.is_set():
            raise ConnectionAbortedError("self.connect() needs to be called first!")
        if isinstance(message, MycroftMessage):
            message = HiveMessage(msg_type=HiveMessageType.BUS,
                                  payload=message)
        if message.msg_type == HiveMessageType.BUS:
            ctxt = dict(message.payload.context)
            if "source" not in ctxt:
                ctxt["source"] = self.useragent
            if "platform" not in message.payload.context:
                ctxt["platform"] = self.useragent
            if "destination" not in message.payload.context:
                ctxt["destination"] = "HiveMind"
            if "session" not in ctxt:
                ctxt["session"] = {}
            if not ctxt["session"].get("session_id"):
                ctxt["session"]["session_id"] = self.session_id
            if not ctxt["session"].get("site_id"):
                ctxt["session"]["site_id"] = self.site_id
            message.payload.context = ctxt

        LOG.debug(f"sending to HiveMind: {message.msg_type}")
        binarize = False
        if message.msg_type == HiveMessageType.BINARY:
            binarize = True
        elif (message.msg_type in BINARY_ENCODABLE_TYPES
              and message.msg_type not in [HiveMessageType.HELLO, HiveMessageType.HANDSHAKE]):
            binarize = self.protocol.binarize and self.binarize

        bitstr = None
        if binarize:
            try:
                bitstr = get_bitstring(hive_type=message.msg_type,
                                       payload=message.payload,
                                       compressed=self.compress,
                                       binary_type=binary_type,
                                       hivemeta=message.metadata)
            except MetadataTooLarge as e:
                # WIRE-1 §4.1: fall back to a text frame. A BINARY payload has
                # no text form, so that one has to be refused.
                if message.msg_type == HiveMessageType.BINARY:
                    raise
                LOG.warning(f"sending {message.msg_type} as a text frame: {e}")

        noise_transport = getattr(self, "noise_transport", None)
        if noise_transport is not None:
            # protocol v3: the Noise transport CipherState replaces the v2
            # AEAD, HELLO included; send_message chunks an oversize payload
            # transparently, one POST per frame. Without this branch every
            # message left in the clear and a 5.x listener closed the
            # session on the first one.
            noise_transport.send_message(
                bitstr.bytes if bitstr is not None else serialize_message(message),
                self._send_noise_frame)
            return

        if bitstr is not None:
            if self.crypto_key:
                payload = encrypt_bin(self.crypto_key, bitstr.bytes, cipher=self.cipher)
            else:
                payload = bitstr.bytes
        else:
            payload = serialize_message(message)
            if self.crypto_key:
                payload = encrypt_as_json(self.crypto_key, payload,
                                          cipher=self.cipher, encoding=self.json_encoding)

        url = f"{self.base_url}/send_message"
        return requests.post(url, data={"message": payload},
                             params={"authorization": self.auth},
                             timeout=self.http_timeout)

    def _send_noise_frame(self, frame: bytes) -> None:
        """POST one Noise transport frame.

        HTTP has no binary opcode, so the frame travels base64-encoded and
        flagged ``binary=1``; the listener decodes it back to the bytes that
        ``HiveMindClientConnection.decode`` requires on a v3 session.
        """
        response = requests.post(
            f"{self.base_url}/send_message",
            data={"message": pybase64.b64encode(frame).decode("utf-8"),
                  "binary": "1"},
            params={"authorization": self.auth},
            timeout=self.http_timeout)
        if not response.ok:
            # the send counter has already advanced for this frame; the
            # session cannot be resynchronised, so say so loudly
            raise ConnectionError(
                f"HiveMind rejected a Noise transport frame: HTTP {response.status_code}")

    def close_connection(self):
        """Release a session that has become unusable.

        ``HiveMindSlaveProtocol._abort_noise`` calls this on the bound client
        after a failed or tampered Noise exchange (it used to raise
        AttributeError here). The websocket client closes its socket; the
        HTTP session is released so the next ``connect()`` starts clean.
        """
        try:
            self.disconnect()
        except Exception:
            LOG.exception("failed to release the aborted HTTP session")

    # targeted messages for nodes, asymmetric encryption
    def emit_intercom(self, message: Union[MycroftMessage, HiveMessage],
                      pubkey: Union[str, bytes, RSA.RsaKey]):
        """INTERCOM (hybrid-encrypted) send. Same shape as sync/async clients.

        This used to build its own envelope: plain ``encrypt_RSA`` + ``sign_RSA``
        under keys ``{"ciphertext", "signature"}`` and no ``encrypted_key``.
        The receiving protocol only recognises the hybrid-encryption envelope
        (``hybrid_encrypt``'s ``"encrypted_key"`` shape) and drops anything
        else at the inner-type check, so that frame could never be accepted.
        Worse, ``pybase64.b64encode(...)`` returns ``bytes``, and a HiveMessage
        payload containing raw bytes cannot be JSON-serialized, so the frame
        raised before it could even be sent — this path was dead. Building the
        same hybrid envelope as ``client.py`` / ``async_client.py``, addressed
        via ``target_pubkey`` and wrapped in PROPAGATE, makes it match what the
        receiver actually verifies and decrypts.
        """
        private_key = load_RSA_key(self.identity.private_key)
        envelope = hybrid_encrypt(pubkey, message.serialize(), sign_key=private_key)
        inner = HiveMessage(HiveMessageType.INTERCOM, payload=envelope,
                            target_pubkey=pubkey if isinstance(pubkey, str) else None)
        self.emit(HiveMessage(HiveMessageType.PROPAGATE, payload=inner))

    ###############
    # HiveMind HTTP Api
    def connect(self, bus=None, protocol=None, site_id=None,
                handshake_max_retries=DEFAULT_HANDSHAKE_MAX_RETRIES):
        LOG.info("Connecting...")
        # A mutable default (FakeBus()) is created once at import and shared
        # across every call; construct a fresh one here instead.
        if bus is None:
            bus = FakeBus()
        # The site this connection reports is the client's, not the identity's
        # — writing it back would rewrite the node's own site, and reading it
        # back would silently ignore `client.site_id = ...` set before connect.
        self._site_id = site_id or self._site_id or self.identity.site_id
        if protocol is None:
            LOG.debug("Initializing HiveMindSlaveProtocol")
            self.protocol = HiveMindSlaveProtocol(self,
                                                  shared_bus=self.share_bus,
                                                  site_id=self._site_id or "unknown",
                                                  identity=self.identity)
        else:
            self.protocol = protocol
            self.protocol.identity = self.identity
            if self._site_id is not None:
                self.protocol.site_id = self._site_id

        LOG.info("Connecting to Hivemind")
        self.protocol.bind(bus)
        url = f"{self.base_url}/connect"
        response = requests.post(url, params={"authorization": self.auth},
                                 timeout=self.http_timeout)
        # An HTTP-level auth failure must surface here, not silently fall into
        # the handshake loop against a hub that already refused us.
        if not response.ok:
            raise ConnectionRefusedError(
                f"HiveMind refused the connection: HTTP {response.status_code}")
        payload = response.json()
        if isinstance(payload, dict) and payload.get("error"):
            raise ConnectionRefusedError(
                f"HiveMind refused the connection: {payload['error']}")
        self.connected.set()
        self.wait_for_handshake(max_retries=handshake_max_retries)
        return payload

    def disconnect(self) -> dict:
        """Disconnect from the HiveMind server."""
        LOG.info("Disconnecting...")
        url = f"{self.base_url}/disconnect"
        response = requests.post(url, params={"authorization": self.auth},
                                 timeout=self.http_timeout)
        self.connected.clear()
        self.handshake_event.clear()
        # the next session starts from a fresh handshake; a stale transport
        # would encrypt its HELLO against a CipherState the server dropped
        self.noise_transport = None
        protocol = getattr(self, "protocol", None)
        if protocol is not None and hasattr(protocol, "reset_connection_state"):
            protocol.reset_connection_state()
        return response.json()

    def get_messages(self) -> List[str]:
        """Retrieve messages from the HiveMind server."""
        if not self.connected.is_set():
            raise ConnectionAbortedError("self.connect() needs to be called first!")
        url = f"{self.base_url}/get_messages"
        response = requests.get(url, params={"authorization": self.auth},
                                timeout=self.http_timeout).json()
        if response.get("error"):
            raise RuntimeError(response["error"])
        return [m for m in response["messages"]]

    def get_binary_messages(self) -> List[bytes]:
        """Retrieve messages from the HiveMind server."""
        if not self.connected.is_set():
            raise ConnectionAbortedError("self.connect() needs to be called first!")
        url = f"{self.base_url}/get_binary_messages"
        response = requests.get(url, params={"authorization": self.auth},
                                timeout=self.http_timeout).json()
        if response.get("error"):
            raise RuntimeError(response["error"])
        return [pybase64.b64decode(m) for m in response["b64_messages"]]


# Example usage:
if __name__ == "__main__":
    from ovos_utils.log import init_service_logger

    init_service_logger("HiveMindHTTP")
    LOG.set_level("ERROR")


    got_tts = threading.Event()

    # To handle binary data subclass BinaryDataCallbacks
    class BinaryDataHandler(BinaryDataCallbacks):
        def handle_receive_tts(self, bin_data: bytes,
                               utterance: str,
                               lang: str,
                               file_name: str):
            # we can play it or save to file or whatever
            print(f"got {len(bin_data)} bytes of TTS audio")
            print(f"utterance: {utterance}", f"lang: {lang}", f"file_name: {file_name}")
            # got 33836 bytes of TTS audio
            # utterance: hello world lang: en-US file_name: 5eb63bbbe01eeed093cb22bb8f5acdc3.wav
            got_tts.set()



    # not passing key etc so it uses identity file
    client = HiveMindHTTPClient(host="http://localhost", port=5679,
                                bin_callbacks=BinaryDataHandler())
    client.connect()

    # send HiveMessages as usual
    client.emit(HiveMessage(HiveMessageType.BUS,
                            MycroftMessage("speak:synth",
                                           {"utterance": "hello world"})))

    got_tts.wait()

    # to handle agent responses, use client.on_mycroft("event", handler)
    answer = None
    answered = threading.Event()

    def handle_speak(message: MycroftMessage):
        global answer
        answer = message.data['utterance']

    def utt_handled(message: MycroftMessage):
        answered.set()

    client.on_mycroft("speak", handle_speak)
    client.on_mycroft("ovos.utterance.handled", utt_handled)


    while True:
        utt = input("> ")
        client.emit(HiveMessage(HiveMessageType.BUS,
                                MycroftMessage("recognizer_loop:utterance",
                                               {"utterances": [utt]})))
        answered.wait()
        print(answer)
        answered.clear()

