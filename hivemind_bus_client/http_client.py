import base64
import json
import threading
import time
from typing import List, Dict, Callable, Union, Optional

import pybase64
import requests
from Cryptodome.PublicKey import RSA
from ovos_bus_client import Message as MycroftMessage, MessageBusClient as OVOSBusClient
from ovos_bus_client.session import Session
from ovos_utils.fakebus import FakeBus
from ovos_utils.log import LOG

from hivemind_bus_client.client import BinaryDataCallbacks
from hivemind_bus_client.encryption import (encrypt_as_json, decrypt_from_json, encrypt_bin, decrypt_bin,
                                            SupportedEncodings, SupportedCiphers)
from hivemind_bus_client.identity import NodeIdentity
from hivemind_bus_client.message import HiveMessage, HiveMessageType, HiveMindBinaryPayloadType
from hivemind_bus_client.protocol import HiveMindSlaveProtocol
from hivemind_bus_client.serialization import get_bitstring, decode_bitstring
from hivemind_bus_client.util import (
    get_hivemind_wire_message,
    serialize_message,
)
from poorman_handshake.asymmetric.utils import encrypt_RSA, load_RSA_key, sign_RSA


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
                 bin_callbacks: BinaryDataCallbacks = BinaryDataCallbacks()):
        super().__init__(daemon=True)
        self.bin_callbacks = bin_callbacks
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


    def wait_for_handshake(self, timeout=5):
        self.handshake_event.wait(timeout=timeout)
        if not self.handshake_event.is_set():
            self.protocol.start_handshake()
            self.wait_for_handshake()
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
        return self.identity.name

    @useragent.setter
    def useragent(self, val):
        self.identity.name = val

    @property
    def password(self) -> str:
        return self.identity.password

    @property
    def key(self) -> str:
        return self.identity.access_key

    @property
    def site_id(self) -> str:
        return self.identity.site_id

    @site_id.setter
    def site_id(self, val):
        self.identity.site_id = val

    @password.setter
    def password(self, val):
        self.identity.password = val

    @key.setter
    def key(self, val):
        self.identity.access_key = val

    def init_identity(self, site_id=None):
        self.identity = self.identity or NodeIdentity()
        self.identity.password = self._password or self.identity.password
        self.identity.access_key = self._access_key or self.identity.access_key
        self.identity.default_master = self._host = self._host or self.identity.default_master
        self.identity.default_port = self._port = self._port or self.identity.default_port
        self.identity.name = self._name or "HiveMessageBusClientV0.0.1"
        self.identity.site_id = site_id or self.identity.site_id

        if not self.identity.access_key or not self.identity.password:
            raise RuntimeError("NodeIdentity not set, please pass key and password or "
                               "call 'hivemind-client set-identity'")
        if not self.identity.default_master:
            raise RuntimeError("host not set, please pass host and port or "
                               "call 'hivemind-client set-identity'")

    def on_message(self, message: Union[bytes, str]):
        if self.crypto_key:
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
            for hm in self.get_messages() + self.get_binary_messages():
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
            ctxt["session"]["session_id"] = self.session_id
            ctxt["session"]["site_id"] = self.site_id
            message.payload.context = ctxt

        message = get_hivemind_wire_message(message)
        LOG.debug(f"sending to HiveMind: {message.msg_type}")
        binarize = False
        if message.msg_type == HiveMessageType.BINARY:
            binarize = True
        elif message.msg_type not in [HiveMessageType.HELLO, HiveMessageType.HANDSHAKE]:
            binarize = self.protocol.binarize and self.binarize

        if binarize:
            bitstr = get_bitstring(hive_type=message.msg_type,
                                   payload=message.payload,
                                   compressed=self.compress,
                                   binary_type=binary_type,
                                   hivemeta=message.metadata)
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
        return requests.post(url, data={"message": payload}, params={"authorization": self.auth})

    # targeted messages for nodes, asymmetric encryption
    def emit_intercom(self, message: Union[MycroftMessage, HiveMessage],
                      pubkey: Union[str, bytes, RSA.RsaKey]):

        encrypted_message = encrypt_RSA(pubkey, message.serialize())

        # sign message
        private_key = load_RSA_key(self.identity.private_key)
        signature = sign_RSA(private_key, encrypted_message)

        self.emit(HiveMessage(HiveMessageType.INTERCOM, payload={"ciphertext": pybase64.b64encode(encrypted_message),
                                                                 "signature": pybase64.b64encode(signature)}))

    ###############
    # HiveMind HTTP Api
    def connect(self, bus=FakeBus(), protocol=None, site_id=None):
        LOG.info("Connecting...")
        self.identity.site_id = site_id or self.identity.site_id
        if protocol is None:
            LOG.debug("Initializing HiveMindSlaveProtocol")
            self.protocol = HiveMindSlaveProtocol(self,
                                                  shared_bus=self.share_bus,
                                                  site_id=self.identity.site_id or "unknown",
                                                  identity=self.identity)
        else:
            self.protocol = protocol
            self.protocol.identity = self.identity
            if self.identity.site_id is not None:
                self.protocol.site_id = self.identity.site_id

        LOG.info("Connecting to Hivemind")
        self.protocol.bind(bus)
        url = f"{self.base_url}/connect"
        response = requests.post(url, params={"authorization": self.auth})
        self.connected.set()
        self.wait_for_handshake()
        return response.json()

    def disconnect(self) -> dict:
        """Disconnect from the HiveMind server."""
        LOG.info("Disconnecting...")
        url = f"{self.base_url}/disconnect"
        response = requests.post(url, params={"authorization": self.auth})
        self.connected.clear()
        self.handshake_event.clear()
        return response.json()

    def get_messages(self) -> List[str]:
        """Retrieve messages from the HiveMind server."""
        if not self.connected.is_set():
            raise ConnectionAbortedError("self.connect() needs to be called first!")
        url = f"{self.base_url}/get_messages"
        response = requests.get(url, params={"authorization": self.auth}).json()
        if response.get("error"):
            raise RuntimeError(response["error"])
        return [m for m in response["messages"]]

    def get_binary_messages(self) -> List[bytes]:
        """Retrieve messages from the HiveMind server."""
        if not self.connected.is_set():
            raise ConnectionAbortedError("self.connect() needs to be called first!")
        url = f"{self.base_url}/get_binary_messages"
        response = requests.get(url, params={"authorization": self.auth}).json()
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
