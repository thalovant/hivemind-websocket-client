import unittest
import warnings

from ovos_bus_client import Message

from hivemind_bus_client.message import HiveMessage, HiveMessageType
from hivemind_bus_client.util import (
    compress_payload, decompress_payload,
    cast2bytes, bytes2str,
    serialize_message, get_payload, get_hivemsg, get_mycroft_msg,
    payload2dict,
    encrypt_as_json, decrypt_from_json, encrypt_bin, decrypt_bin,
)


class TestCompressDecompress(unittest.TestCase):
    def test_compress_string(self):
        result = compress_payload("hello world")
        self.assertIsInstance(result, bytes)

    def test_compress_bytes(self):
        result = compress_payload(b"hello world")
        self.assertIsInstance(result, bytes)

    def test_roundtrip(self):
        text = "The quick brown fox jumps over the lazy dog."
        compressed = compress_payload(text)
        decompressed = decompress_payload(compressed)
        self.assertEqual(decompressed, text.encode("utf-8"))

    def test_roundtrip_bytes(self):
        data = b"\x00\x01\x02\xff"
        compressed = compress_payload(data)
        decompressed = decompress_payload(compressed)
        self.assertEqual(decompressed, data)

    def test_compressed_smaller_for_large_text(self):
        text = "a" * 10000
        compressed = compress_payload(text)
        self.assertLess(len(compressed), len(text.encode("utf-8")))


class TestCast2Bytes(unittest.TestCase):
    def test_dict_to_bytes(self):
        result = cast2bytes({"key": "val"})
        self.assertIsInstance(result, bytes)

    def test_string_to_bytes(self):
        result = cast2bytes("hello")
        self.assertIsInstance(result, bytes)

    def test_bytes_passthrough(self):
        result = cast2bytes("hello".encode())
        self.assertIsInstance(result, bytes)

    def test_compressed_dict(self):
        result = cast2bytes({"key": "val"}, compressed=True)
        self.assertIsInstance(result, bytes)


class TestBytes2Str(unittest.TestCase):
    def test_plain_bytes(self):
        result = bytes2str(b"hello")
        self.assertEqual(result, "hello")

    def test_compressed_bytes(self):
        text = "hello world"
        compressed = compress_payload(text)
        result = bytes2str(compressed, compressed=True)
        self.assertEqual(result, text)


class TestSerializeMessage(unittest.TestCase):
    def test_hivemessage(self):
        msg = HiveMessage(HiveMessageType.BUS, payload={"type": "test", "data": {}, "context": {}})
        s = serialize_message(msg)
        self.assertIsInstance(s, str)

    def test_ovos_message(self):
        msg = Message("speak", {"utterance": "hello"})
        s = serialize_message(msg)
        self.assertIsInstance(s, str)

    def test_dict(self):
        d = {"key": "value"}
        s = serialize_message(d)
        self.assertIsInstance(s, str)


class TestGetPayload(unittest.TestCase):
    def test_from_hivemessage_bus(self):
        m = Message("speak", {"utterance": "hello"})
        hm = HiveMessage(HiveMessageType.BUS, payload=m)
        payload = get_payload(hm)
        # payload of BUS type is a Message, get_payload then serializes it
        self.assertIsNotNone(payload)

    def test_from_string(self):
        d = {"type": "test", "data": {}, "context": {}}
        import json
        result = get_payload(json.dumps(d))
        self.assertEqual(result, d)

    def test_from_dict(self):
        d = {"type": "test", "data": {}, "context": {}}
        result = get_payload(d)
        self.assertEqual(result, d)


class TestGetHiveMsg(unittest.TestCase):
    def test_from_ovos_message(self):
        m = Message("speak", {"utterance": "hello"})
        hm = get_hivemsg(m)
        self.assertIsInstance(hm, HiveMessage)
        self.assertEqual(hm.msg_type, HiveMessageType.BUS)

    def test_from_dict(self):
        d = {"msg_type": "bus", "payload": {"type": "t", "data": {}, "context": {}},
             "metadata": {}, "route": [], "node": None, "target_site_id": None,
             "target_pubkey": None, "source_peer": None}
        hm = get_hivemsg(d)
        self.assertIsInstance(hm, HiveMessage)

    def test_from_json_string(self):
        import json
        d = {"msg_type": "bus", "payload": {"type": "t", "data": {}, "context": {}},
             "metadata": {}, "route": [], "node": None, "target_site_id": None,
             "target_pubkey": None, "source_peer": None}
        hm = get_hivemsg(json.dumps(d))
        self.assertIsInstance(hm, HiveMessage)


class TestGetMycroftMsg(unittest.TestCase):
    def test_from_hivemessage(self):
        m = Message("speak", {"utterance": "hello"})
        hm = HiveMessage(HiveMessageType.BUS, payload=m)
        result = get_mycroft_msg(hm)
        self.assertIsInstance(result, Message)
        self.assertEqual(result.msg_type, "speak")

    def test_from_dict(self):
        d = {"type": "speak", "data": {"utterance": "hello"}, "context": {}}
        result = get_mycroft_msg(d)
        self.assertIsInstance(result, Message)
        self.assertEqual(result.msg_type, "speak")

    def test_from_string(self):
        import json
        d = {"type": "speak", "data": {"utterance": "hello"}, "context": {}}
        result = get_mycroft_msg(json.dumps(d))
        self.assertIsInstance(result, Message)


class TestPayload2Dict(unittest.TestCase):
    def test_from_hive_message(self):
        inner = Message("speak", {"utterance": "hi"})
        hm = HiveMessage(HiveMessageType.BUS, inner)
        result = payload2dict(hm)
        self.assertIsInstance(result, dict)
        self.assertEqual(result["msg_type"], HiveMessageType.BUS)

    def test_from_ovos_message(self):
        msg = Message("speak", {"utterance": "hi"})
        result = payload2dict(msg)
        self.assertIsInstance(result, dict)
        self.assertEqual(result["type"], "speak")

    def test_from_dict(self):
        d = {"key": "val"}
        result = payload2dict(d)
        self.assertEqual(result, {"key": "val"})

    def test_nested_message_in_list(self):
        inner = Message("speak", {"utterance": "hi"})
        d = {"items": [inner]}
        result = payload2dict(d)
        self.assertIsInstance(result["items"][0], dict)
        self.assertEqual(result["items"][0]["type"], "speak")

    def test_nested_dict_value(self):
        d = {"inner": {"nested": "data"}}
        result = payload2dict(d)
        self.assertEqual(result["inner"]["nested"], "data")


class TestDeprecatedEncryptionWrappers(unittest.TestCase):
    """Test that deprecated wrappers emit warnings and delegate correctly."""

    def test_encrypt_as_json_warns(self):
        with warnings.catch_warnings(record=True) as w:
            warnings.simplefilter("always")
            result = encrypt_as_json("a" * 16, {"key": "val"})
            self.assertTrue(any("deprecated" in str(warning.message).lower() for warning in w))
            self.assertIsInstance(result, str)

    def test_decrypt_from_json_warns(self):
        with warnings.catch_warnings(record=True) as w:
            warnings.simplefilter("always")
            encrypted = encrypt_as_json("a" * 16, {"key": "val"})
            result = decrypt_from_json("a" * 16, encrypted)
            dep_warnings = [x for x in w if "deprecated" in str(x.message).lower()]
            self.assertTrue(len(dep_warnings) >= 1)

    def test_encrypt_decrypt_bin_warns(self):
        key = "a" * 16
        with warnings.catch_warnings(record=True) as w:
            warnings.simplefilter("always")
            encrypted = encrypt_bin(key, b"hello")
            self.assertIsInstance(encrypted, bytes)
            dep = [x for x in w if "deprecated" in str(x.message).lower()]
            self.assertTrue(len(dep) >= 1)

        with warnings.catch_warnings(record=True) as w:
            warnings.simplefilter("always")
            decrypted = decrypt_bin(key, encrypted)
            self.assertEqual(decrypted, b"hello")
            dep = [x for x in w if "deprecated" in str(x.message).lower()]
            self.assertTrue(len(dep) >= 1)

    def test_encrypt_decrypt_json_roundtrip(self):
        key = "b" * 16
        with warnings.catch_warnings(record=True):
            warnings.simplefilter("always")
            encrypted = encrypt_as_json(key, "hello world")
            decrypted = decrypt_from_json(key, encrypted)
            self.assertEqual(decrypted, "hello world")

    def test_encrypt_as_json_b64(self):
        key = "c" * 16
        with warnings.catch_warnings(record=True):
            warnings.simplefilter("always")
            result = encrypt_as_json(key, "test", b64=True)
            self.assertIsInstance(result, str)


if __name__ == "__main__":
    unittest.main()
