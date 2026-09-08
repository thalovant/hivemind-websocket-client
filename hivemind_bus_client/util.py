"""Serialization helpers, payload normalization, and compression utilities.

Also re-exports deprecated encryption wrappers that redirect to
``hivemind_bus_client.encryption``.
"""
import json
import warnings
import zlib
from typing import Dict, Union

from hivemind_bus_client.encryption import SupportedEncodings, SupportedCiphers
from hivemind_bus_client.message import HiveMessage, HiveMessageType, Message


def serialize_message(message: Union[HiveMessage, Message, Dict]) -> str:
    """Convert a message object to a JSON string suitable for WebSocket transport.

    Args:
        message: A ``HiveMessage``, ``Message``, dict, or any object with a
            ``serialize()`` method.

    Returns:
        JSON string representation of *message*.
    """
    if hasattr(message, 'serialize'):
        return message.serialize()
    elif isinstance(message, dict):
        message = {
            k: v if not hasattr(v, 'serialize') else serialize_message(v)
            for k, v in message.items()}
        return json.dumps(message)
    else:
        return json.dumps(message.__dict__)


def payload2dict(payload: Union[HiveMessage, Message, str]) -> Dict:
    """Recursively normalize *payload* to a JSON-safe dict.

    Ensures all nested ``HiveMessage`` and ``Message`` sub-objects are
    converted so the result can be safely sent over the OVOS bus.

    Args:
        payload: A ``HiveMessage``, ``Message``, JSON string, or dict.

    Returns:
        A fully-normalized dict.
    """
    if isinstance(payload, HiveMessage):
        payload = payload.as_dict
    if isinstance(payload, Message):
        payload = payload.serialize()
    if isinstance(payload, str):
        try:
            payload = json.loads(payload)
        except Exception:
            pass
    assert isinstance(payload, dict)

    def can_serialize(val: object) -> bool:
        return isinstance(val, (HiveMessage, Message, dict))

    for k, v in payload.items():
        if can_serialize(v):
            payload[k] = payload2dict(v)
        if isinstance(v, list):
            for idx, item in enumerate(v):
                if can_serialize(item):
                    payload[k][idx] = payload2dict(item)
    return payload


def get_payload(msg: Union[HiveMessage, Message, str, Dict]) -> Dict:
    """Extract a normalized dict payload from any supported message format.

    Args:
        msg: A ``HiveMessage``, ``Message``, JSON string, or dict.

    Returns:
        The payload as a plain dict.
    """
    if isinstance(msg, HiveMessage):
        msg = msg.payload
    if isinstance(msg, Message):
        msg = msg.serialize()
    if isinstance(msg, str):
        msg = json.loads(msg)
    return msg


def get_hivemsg(msg: Union[Message, str, Dict]) -> HiveMessage:
    """Create a normalized ``HiveMessage`` from any supported format.

    Args:
        msg: A ``Message``, JSON string, or dict with ``HiveMessage`` fields.

    Returns:
        A ``HiveMessage`` instance.
    """
    if isinstance(msg, str):
        msg = json.loads(msg)
    if isinstance(msg, dict):
        msg = HiveMessage(**msg)
    if isinstance(msg, Message):
        msg = HiveMessage(msg_type=HiveMessageType.BUS, payload=msg)
    assert isinstance(msg, HiveMessage)
    return msg


def get_mycroft_msg(pload: Union[HiveMessage, str, Dict]) -> Message:
    """Extract a ``Message`` (OVOS bus message) from a ``HiveMessage`` or raw data.

    Args:
        pload: A ``HiveMessage`` (must be ``BUS`` type), JSON string, or dict.

    Returns:
        An OVOS ``Message`` instance.
    """
    if isinstance(pload, HiveMessage):
        assert pload.msg_type == HiveMessageType.BUS
        pload = pload.payload

    if isinstance(pload, str):
        try:
            pload = Message.deserialize(pload)
        except Exception:
            pload = json.loads(pload)
    if isinstance(pload, dict):
        msg_type = pload.get("msg_type") or pload["type"]
        data = pload.get("data") or {}
        context = pload.get("context") or {}
        pload = Message(msg_type, data, context)

    assert isinstance(pload, Message)
    return pload


def compress_payload(text: Union[str, bytes]) -> bytes:
    """Compress *text* using zlib.

    Args:
        text: UTF-8 string or raw bytes to compress.

    Returns:
        zlib-compressed bytes.
    """
    if isinstance(text, str):
        decompressed = text.encode("utf-8")
    else:
        decompressed = text
    return zlib.compress(decompressed)


def decompress_payload(compressed: bytes) -> bytes:
    """Decompress zlib-compressed *compressed* bytes.

    Args:
        compressed: zlib-compressed bytes.

    Returns:
        Decompressed bytes.
    """
    return zlib.decompress(compressed)


def cast2bytes(payload: Union[Dict, str], compressed: bool = False) -> bytes:
    """Convert *payload* to bytes, optionally compressing.

    Args:
        payload: A dict (JSON-serialized first) or string.
        compressed: If True, zlib-compress the result.

    Returns:
        The payload as bytes.
    """
    if isinstance(payload, dict):
        payload = json.dumps(payload)
    if compressed:
        payload = compress_payload(payload)
    if isinstance(payload, str):
        payload = payload.encode("utf-8")
    assert isinstance(payload, bytes)
    return payload


def bytes2str(payload: bytes, compressed: bool = False) -> str:
    """Convert *payload* bytes to a UTF-8 string, optionally decompressing.

    Args:
        payload: Raw or zlib-compressed bytes.
        compressed: If True, decompress before decoding.

    Returns:
        Decoded UTF-8 string.
    """
    if compressed:
        return decompress_payload(payload).decode("utf-8")
    else:
        return payload.decode("utf-8")


###############
# deprecated

def encrypt_as_json(key: Union[str, bytes], data: Union[str, Dict],
                    b64: bool = False) -> str:
    """Deprecated: use ``hivemind_bus_client.encryption.encrypt_as_json``."""
    warnings.warn(
        "encrypt_as_json is deprecated and will be removed in future versions. "
        "Use 'from hivemind_bus_client.encryption import encrypt_as_json' instead",
        DeprecationWarning,
        stacklevel=2
    )
    from hivemind_bus_client.encryption import encrypt_as_json as _ej
    c = SupportedEncodings.JSON_B64 if b64 else SupportedEncodings.JSON_HEX
    return _ej(key, data, encoding=c, cipher=SupportedCiphers.AES_GCM)


def decrypt_from_json(key: Union[str, bytes],
                      data: Union[str, bytes]) -> str:
    """Deprecated: use ``hivemind_bus_client.encryption.decrypt_from_json``."""
    warnings.warn(
        "decrypt_from_json is deprecated and will be removed in future versions. "
        "Use 'from hivemind_bus_client.encryption import decrypt_from_json' instead",
        DeprecationWarning,
        stacklevel=2
    )
    from hivemind_bus_client.encryption import decrypt_from_json as _dj
    try:
        return _dj(key, data, encoding=SupportedEncodings.JSON_HEX, cipher=SupportedCiphers.AES_GCM)
    except Exception as e:
        try:
            return _dj(key, data, encoding=SupportedEncodings.JSON_B64, cipher=SupportedCiphers.AES_GCM)
        except Exception:
            raise e


def encrypt_bin(key: Union[str, bytes],
                data: Union[str, bytes]) -> bytes:
    """Deprecated: use ``hivemind_bus_client.encryption.encrypt_bin``."""
    warnings.warn(
        "encrypt_bin is deprecated and will be removed in future versions. "
        "Use 'from hivemind_bus_client.encryption import encrypt_bin' instead",
        DeprecationWarning,
        stacklevel=2
    )
    from hivemind_bus_client.encryption import encrypt_bin as _eb
    return _eb(key, data, cipher=SupportedCiphers.AES_GCM)


def decrypt_bin(key: Union[str, bytes],
                ciphertext: bytes) -> bytes:
    """Deprecated: use ``hivemind_bus_client.encryption.decrypt_bin``."""
    warnings.warn(
        "decrypt_bin is deprecated and will be removed in future versions. "
        "Use 'from hivemind_bus_client.encryption import decrypt_bin' instead",
        DeprecationWarning,
        stacklevel=2
    )
    from hivemind_bus_client.encryption import decrypt_bin as _db
    return _db(key, ciphertext, SupportedCiphers.AES_GCM)
