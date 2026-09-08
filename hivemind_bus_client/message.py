import json
from enum import Enum, IntEnum

from ovos_bus_client import Message
from ovos_utils.json_helper import merge_dict
from typing import Union, List, Optional, Dict, Any


class HiveMessageType(str, Enum):
    HANDSHAKE = "shake"  # negotiate initial connection
    BUS = "bus"  # request meant for internal mycroft-bus in master
    SHARED_BUS = "shared_bus"  # passive sharing of message
    # from mycroft-bus in slave

    INTERCOM = "intercom"  # from satellite to satellite

    BROADCAST = "broadcast"  # forward message to all slaves
    PROPAGATE = "propagate"  # forward message to all slaves and masters
    ESCALATE = "escalate"  # forward message up the authority chain to all
    # masters
    HELLO = "hello"  # like escalate, used to announce the device
    QUERY = "query"  # like escalate, but stops once one of the nodes can
    # send a response
    CASCADE = "cascade"  # like propagate, but expects a response back from
    # all nodes in the hive (responses optional)
    PING = "ping"  # like cascade, but used to map the network
    RENDEZVOUS = "rendezvous"  # reserved for rendezvous-nodes
    THIRDPRTY = "3rdparty"  # user land message, do whatever you want
    BINARY = "bin"  # binary data container, payload for something else


class HiveMindBinaryPayloadType(IntEnum):
    """ Pseudo extension type for binary payloads
    it doesnt describe the payload but rather provides instruction to hivemind about how to handle it"""
    UNDEFINED = 0  # no info provided about binary contents
    RAW_AUDIO = 1  # binary content is raw audio  (TODO spec exactly what "raw audio" means)
    NUMPY_IMAGE = 2  # binary content is an image as a numpy array, eg. webcam picture
    FILE = 3  # binary is a file to be saved, additional metadata provided elsewhere
    STT_AUDIO_TRANSCRIBE = 4  # full audio sentence to perform STT and return transcripts
    STT_AUDIO_HANDLE = 5  # full audio sentence to perform STT and handle transcription immediately
    TTS_AUDIO = 6  # synthesized TTS audio to be played


class HiveMessage:
    def __init__(self, msg_type: Union[HiveMessageType, str],
                 payload: Optional[Union[Message, 'HiveMessage', str, dict, bytes]] =None,
                 node: Optional[str]=None,
                 source_peer: Optional[str]=None,
                 route: Optional[List[str]]=None,
                 target_peers: Optional[List[str]]=None,
                 target_site_id: Optional[str] =None,
                 target_pubkey: Optional[str] =None,
                 bin_type: HiveMindBinaryPayloadType = HiveMindBinaryPayloadType.UNDEFINED,
                 metadata: Optional[Dict[str, Any]] = None):
        #  except for the hivemind node classes receiving the message and
        #  creating the object nothing should be able to change these values
        #  node classes might change them a runtime by the private attribute
        #  but end-users should consider them read_only
        if msg_type not in [m.value for m in HiveMessageType]:
            raise ValueError("Unknown HiveMessage.msg_type")
        if msg_type != HiveMessageType.BINARY and bin_type != HiveMindBinaryPayloadType.UNDEFINED:
            raise ValueError("bin_type can only be set for BINARY message type")

        self._msg_type = msg_type
        self._bin_type = bin_type
        self._meta = metadata or {}

        # the payload is more or less a free for all
        # the msg_type determines what happens to the message, but the
        # payload can simply be ignored by the receiving module
        # we store things in dict/json format, json is always used at the
        # transport layer before converting into any of the other formats
        if not isinstance(payload, bytes) and msg_type == HiveMessageType.BINARY:
            raise ValueError(f"expected 'bytes' payload for HiveMessageType.BINARY, got {type(payload)}")
        elif isinstance(payload, Message):
            payload = {"type": payload.msg_type,
                       "data": payload.data,
                       "context": payload.context}
        elif isinstance(payload, HiveMessage):
            payload = payload.as_dict
        elif isinstance(payload, str):
            payload = json.loads(payload)
        self._payload = payload or {}

        self._site_id = target_site_id
        self._target_pubkey = target_pubkey
        self._node = node  # node semi-unique identifier
        self._source_peer = source_peer  # peer_id
        self._route = route or []  # where did this message come from
        self._targets = target_peers or []  # where will it be sent

    @property
    def metadata(self) -> Dict[str, Any]:
        return self._meta

    @property
    def target_site_id(self) -> str:
        return self._site_id

    @property
    def target_public_key(self) -> str:
        return self._target_pubkey

    @property
    def msg_type(self) -> str:
        return self._msg_type

    @property
    def node_id(self) -> str:
        return self._node

    @property
    def source_peer(self) -> str:
        return self._source_peer

    @property
    def target_peers(self) -> List[str]:
        if self.source_peer:
            return self._targets or [self._source_peer]
        return self._targets

    @property
    def route(self) -> List[str]:
        return [r for r in self._route if isinstance(r, dict) and r.get("targets") and r.get("source")]

    @property
    def payload(self) -> Union['HiveMessage', Message, dict, bytes]:
        """
        Return the public payload converted to the most appropriate message representation for this HiveMessage.
        
        Depending on this message's msg_type, the payload is returned as a reconstructed `Message`, a reconstructed `HiveMessage`, or the raw stored payload.
        
        Returns:
            Union[HiveMessage, Message, dict, bytes]: A `Message` when msg_type is BUS or SHARED_BUS; a `HiveMessage` when msg_type is BROADCAST, PROPAGATE, CASCADE, ESCALATE, or QUERY; otherwise the raw payload (typically a `dict` or `bytes`).
        """
        if self.msg_type in [HiveMessageType.BUS, HiveMessageType.SHARED_BUS]:
            return Message(self._payload["type"],
                           data=self._payload.get("data"),
                           context=self._payload.get("context"))
        if self.msg_type in [HiveMessageType.BROADCAST,
                             HiveMessageType.PROPAGATE,
                             HiveMessageType.CASCADE,
                             HiveMessageType.ESCALATE,
                             HiveMessageType.QUERY]:
            return HiveMessage(**self._payload)
        return self._payload

    @payload.setter
    def payload(self, payload: Union['HiveMessage', Message, dict, bytes]):
        """
        Set the message payload, normalizing Message or HiveMessage inputs to their dictionary representations.
        
        Parameters:
            payload (HiveMessage | Message | dict | bytes): New payload to assign. If a `Message` or `HiveMessage` is provided, its dict representation is stored; otherwise the value is stored as given.
        """
        if isinstance(payload, Message):
            self._payload = payload.as_dict
        elif isinstance(payload, HiveMessage):
            self._payload = payload.as_dict
        else:
            self._payload = payload

    @property
    def bin_type(self) -> HiveMindBinaryPayloadType:
        """
        Get the binary payload type for this message.
        
        Returns:
            HiveMindBinaryPayloadType: Indicator of how the message's binary payload should be interpreted.
        """
        return self._bin_type

    @property
    def as_dict(self) -> dict:
        pload = self._payload
        if self.msg_type == HiveMessageType.BINARY:
            raise ValueError("messages with type HiveMessageType.BINARY can not be cast to dict")
        if isinstance(pload, HiveMessage):
            pload = pload.as_dict
        elif isinstance(pload, Message):
            pload = pload.serialize()
        if isinstance(pload, str):
            pload = json.loads(pload)

        assert isinstance(pload, dict)

        return {"msg_type": self.msg_type,
                "payload": pload,
                "metadata": self.metadata,
                "route": self.route,
                "node": self.node_id,
                "target_site_id": self.target_site_id,
                "target_pubkey": self.target_public_key,
                "source_peer": self.source_peer}

    @property
    def as_json(self) -> str:
        return json.dumps(self.as_dict, ensure_ascii=False)

    def serialize(self) -> str:
        return self.as_json

    @staticmethod
    def deserialize(payload: Union[str, dict]) -> 'HiveMessage':
        if isinstance(payload, str):
            payload = json.loads(payload)

        if "msg_type" in payload:
            try:
                return HiveMessage(payload["msg_type"], payload["payload"],
                                   metadata=payload.get("metadata", {}),
                                   route=payload.get("route"),
                                   target_site_id=payload.get("target_site_id"),
                                   target_pubkey=payload.get("target_pubkey"))
            except Exception:
                pass  # not a hivemind message

        if "type" in payload:
            try:
                # NOTE: technically could also be SHARED_BUS or THIRDPRTY
                return HiveMessage(HiveMessageType.BUS,
                                   payload=Message.deserialize(payload),
                                   metadata=payload.get("metadata", {}),
                                   target_site_id=payload.get("target_site_id"),
                                   target_pubkey=payload.get("target_pubkey"))
            except Exception:
                pass  # not a mycroft message

        return HiveMessage(HiveMessageType.THIRDPRTY, payload,
                           metadata=payload.get("metadata", {}),
                           target_site_id=payload.get("target_site_id"),
                           target_pubkey=payload.get("target_pubkey"))

    def __getitem__(self, item):
        if not isinstance(self._payload, dict):
            raise TypeError(f"Item access not supported for payload type {type(self._payload)}")
        return self._payload.get(item)

    def __setitem__(self, key, value):
        if isinstance(self._payload, dict):
            self._payload[key] = value
        else:
            raise TypeError(f"Item assignment not supported for payload type {type(self._payload)}")

    def __str__(self):
        if self.msg_type == HiveMessageType.BINARY:
            return f"HiveMessage(BINARY:{len(self._payload)}])"
        return self.as_json

    def update_hop_data(self, data=None, **kwargs):
        if not self._route or self._route[-1]["source"] != self.source_peer:
            self._route += [{"source": self.source_peer,
                             "targets": self.target_peers}]
        if self._route and data:
            self._route[-1] = merge_dict(self._route[-1], data, **kwargs)

    def replace_route(self, route):
        self._route = route

    def update_source_peer(self, peer):
        self._source_peer = peer
        return self

    def add_target_peer(self, peer):
        self._targets.append(peer)

    def remove_target_peer(self, peer):
        if peer in self._targets:
            self._targets.remove(peer)