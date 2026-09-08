"""Tests for NodeIdentity and HiveMessageBusClient initialization."""
import os
import tempfile
import unittest
from unittest.mock import MagicMock, patch


class TestNodeIdentity(unittest.TestCase):

    def test_default_identity_name_fallback(self):
        """Fresh NodeIdentity with no stored name returns the default."""
        with tempfile.TemporaryDirectory() as tmpdir:
            from json_database import JsonConfigXDG
            from hivemind_bus_client.identity import NodeIdentity
            identity = NodeIdentity.__new__(NodeIdentity)
            identity.IDENTITY_FILE = JsonConfigXDG("_test_identity_blank", subfolder="hivemind_test_tmp")
            # Clear any persisted values
            identity.IDENTITY_FILE.clear()
            identity.IDENTITY_FILE.store()

            # Without a name set, should return the default fallback
            self.assertIsInstance(identity.name, str)
            self.assertTrue(len(identity.name) > 0)

    def test_access_key_initially_none_or_string(self):
        """access_key property should be None or str, never raise."""
        from hivemind_bus_client.identity import NodeIdentity
        identity = NodeIdentity.__new__(NodeIdentity)
        identity.IDENTITY_FILE = MagicMock()
        identity.IDENTITY_FILE.get.return_value = None
        result = identity.access_key
        self.assertIsNone(result)

    def test_password_initially_none_or_string(self):
        from hivemind_bus_client.identity import NodeIdentity
        identity = NodeIdentity.__new__(NodeIdentity)
        identity.IDENTITY_FILE = MagicMock()
        identity.IDENTITY_FILE.get.return_value = None
        result = identity.password
        self.assertIsNone(result)

    def test_site_id_returns_none_when_not_set(self):
        """site_id returns None when not stored in the identity file."""
        from hivemind_bus_client.identity import NodeIdentity
        identity = NodeIdentity.__new__(NodeIdentity)
        identity.IDENTITY_FILE = MagicMock()
        identity.IDENTITY_FILE.get.return_value = None
        result = identity.site_id
        self.assertIsNone(result)

    def test_site_id_returns_value_when_set(self):
        from hivemind_bus_client.identity import NodeIdentity
        identity = NodeIdentity.__new__(NodeIdentity)
        identity.IDENTITY_FILE = MagicMock()
        identity.IDENTITY_FILE.get.return_value = "my-site"
        result = identity.site_id
        self.assertEqual(result, "my-site")

    def test_name_setter_stores_value(self):
        from hivemind_bus_client.identity import NodeIdentity
        identity = NodeIdentity.__new__(NodeIdentity)
        identity.IDENTITY_FILE = MagicMock()
        identity.IDENTITY_FILE.get.return_value = None
        identity.name = "my-satellite"
        identity.IDENTITY_FILE.__setitem__.assert_called_with("name", "my-satellite")

    def test_access_key_setter(self):
        from hivemind_bus_client.identity import NodeIdentity
        identity = NodeIdentity.__new__(NodeIdentity)
        identity.IDENTITY_FILE = MagicMock()
        identity.access_key = "my-api-key"
        identity.IDENTITY_FILE.__setitem__.assert_called_with("access_key", "my-api-key")

    def test_password_setter(self):
        from hivemind_bus_client.identity import NodeIdentity
        identity = NodeIdentity.__new__(NodeIdentity)
        identity.IDENTITY_FILE = MagicMock()
        identity.password = "secret"
        identity.IDENTITY_FILE.__setitem__.assert_called_with("password", "secret")


class TestNodeIdentityProperties(unittest.TestCase):
    """Cover remaining property getters/setters and methods."""

    def _make_identity(self, data=None):
        from hivemind_bus_client.identity import NodeIdentity
        identity = NodeIdentity.__new__(NodeIdentity)
        identity.IDENTITY_FILE = MagicMock()
        store = data or {}
        identity.IDENTITY_FILE.get = lambda k, default=None: store.get(k, default)
        identity.IDENTITY_FILE.__getitem__ = lambda self_inner, k: store[k]
        identity.IDENTITY_FILE.__setitem__ = lambda self_inner, k, v: store.__setitem__(k, v)
        identity.IDENTITY_FILE.path = "/tmp/fake_identity.json"
        return identity, store

    def test_name_fallback_from_key(self):
        """When name is unset but key exists, name is derived from key basename."""
        identity, store = self._make_identity({"key": "/some/path/my-device"})
        self.assertEqual(identity.name, "my-device")

    def test_name_default_unnamed(self):
        identity, store = self._make_identity({})
        self.assertEqual(identity.name, "unnamed-node")

    def test_name_setter(self):
        identity, store = self._make_identity({})
        identity.name = "new-name"
        self.assertEqual(store["name"], "new-name")

    def test_public_key_get_set(self):
        identity, store = self._make_identity({})
        self.assertIsNone(identity.public_key)
        identity.public_key = "RSA_KEY_DATA"
        self.assertEqual(store["public_key"], "RSA_KEY_DATA")

    def test_private_key_fallback(self):
        identity, store = self._make_identity({})
        # No secret_key set → falls back to path-based default
        self.assertIn(".pem", identity.private_key)

    def test_private_key_explicit(self):
        identity, store = self._make_identity({"secret_key": "/my/key.pem"})
        self.assertEqual(identity.private_key, "/my/key.pem")

    def test_private_key_setter(self):
        identity, store = self._make_identity({})
        identity.private_key = "/new/key.pem"
        self.assertEqual(store["secret_key"], "/new/key.pem")

    def test_default_master_get_set(self):
        identity, store = self._make_identity({})
        self.assertIsNone(identity.default_master)
        identity.default_master = "ws://hub.local"
        self.assertEqual(store["default_master"], "ws://hub.local")

    def test_default_port_get_set(self):
        identity, store = self._make_identity({})
        self.assertIsNone(identity.default_port)
        identity.default_port = 5678
        self.assertEqual(store["default_port"], 5678)

    def test_save_calls_store(self):
        identity, store = self._make_identity({})
        identity.save()
        identity.IDENTITY_FILE.store.assert_called_once()

    def test_reload_calls_reload(self):
        identity, store = self._make_identity({})
        identity.reload()
        identity.IDENTITY_FILE.reload.assert_called_once()

    def test_create_keys(self):
        from hivemind_bus_client.identity import NodeIdentity
        identity, store = self._make_identity({})
        with patch("hivemind_bus_client.identity.create_RSA_key", return_value=("PUB", "SECRET")), \
             patch("hivemind_bus_client.identity.export_RSA_key") as mock_export:
            identity.create_keys()
            mock_export.assert_called_once()
            self.assertEqual(store["public_key"], "PUB")
            self.assertIn(".pem", store["secret_key"])


class TestTrustedKeys(unittest.TestCase):

    def _make_identity(self, data=None):
        from hivemind_bus_client.identity import NodeIdentity
        identity = NodeIdentity.__new__(NodeIdentity)
        identity.IDENTITY_FILE = MagicMock()
        store = data or {}
        identity.IDENTITY_FILE.get = lambda k, default=None: store.get(k, default)
        identity.IDENTITY_FILE.__getitem__ = lambda self_inner, k: store[k]
        identity.IDENTITY_FILE.__setitem__ = lambda self_inner, k, v: store.__setitem__(k, v)
        identity.IDENTITY_FILE.path = "/tmp/fake_identity.json"
        return identity, store

    def test_trusted_keys_empty_by_default(self):
        identity, _ = self._make_identity({})
        self.assertEqual(identity.trusted_keys, {})

    def test_add_trusted_key(self):
        identity, store = self._make_identity({})
        self.assertTrue(identity.add_trusted_key("hub", "KEY_A"))
        self.assertEqual(store["trusted_keys"]["hub"], "KEY_A")

    def test_add_duplicate_alias_returns_false(self):
        identity, _ = self._make_identity({"trusted_keys": {"hub": "KEY_A"}})
        self.assertFalse(identity.add_trusted_key("hub", "KEY_B"))

    def test_remove_trusted_key(self):
        identity, store = self._make_identity({"trusted_keys": {"hub": "KEY_A", "relay": "KEY_B"}})
        self.assertTrue(identity.remove_trusted_key("hub"))
        self.assertNotIn("hub", store["trusted_keys"])
        self.assertIn("relay", store["trusted_keys"])

    def test_remove_missing_alias_returns_false(self):
        identity, _ = self._make_identity({})
        self.assertFalse(identity.remove_trusted_key("nope"))

    def test_is_trusted_key_by_pubkey(self):
        identity, _ = self._make_identity({"trusted_keys": {"hub": "KEY_A"}})
        self.assertTrue(identity.is_trusted_key("KEY_A"))
        self.assertFalse(identity.is_trusted_key("KEY_B"))

    def test_get_trusted_alias(self):
        identity, _ = self._make_identity({"trusted_keys": {"hub": "KEY_A"}})
        self.assertEqual(identity.get_trusted_alias("KEY_A"), "hub")
        self.assertIsNone(identity.get_trusted_alias("KEY_B"))

    def test_trusted_keys_setter(self):
        identity, store = self._make_identity({})
        identity.trusted_keys = {"k1": "PUB1", "k2": "PUB2"}
        self.assertEqual(store["trusted_keys"], {"k1": "PUB1", "k2": "PUB2"})


class TestHiveMessageBusClientInit(unittest.TestCase):

    def _make_client(self, **kwargs):
        """Create HiveMessageBusClient without connecting."""
        from hivemind_bus_client.client import HiveMessageBusClient
        defaults = {
            "key": "test-api-key",
            "password": "test-password",
            "port": 5678,
            "host": "ws://localhost",
        }
        defaults.update(kwargs)
        # Bypass actual connection
        with patch.object(HiveMessageBusClient, "connect", return_value=None):
            client = HiveMessageBusClient(**defaults)
        return client

    def test_client_stores_key(self):
        client = self._make_client()
        self.assertEqual(client.key, "test-api-key")

    def test_client_stores_password(self):
        client = self._make_client()
        self.assertEqual(client.password, "test-password")

    def test_client_stores_key(self):
        client = self._make_client()
        self.assertEqual(client.key, "test-api-key")

    def test_client_session_id_is_string(self):
        """session_id should be a non-empty string (typically a UUID)."""
        client = self._make_client()
        self.assertIsInstance(client.session_id, str)
        self.assertTrue(len(client.session_id) > 0)

    def test_client_session_id_looks_like_uuid(self):
        """session_id should have the UUID-hex format (32 hex chars / UUID dashes)."""
        import re
        client = self._make_client()
        uuid_pattern = re.compile(
            r"^[0-9a-f]{8}-?[0-9a-f]{4}-?[0-9a-f]{4}-?[0-9a-f]{4}-?[0-9a-f]{12}$",
            re.IGNORECASE
        )
        self.assertRegex(client.session_id, uuid_pattern)


if __name__ == "__main__":
    unittest.main()
