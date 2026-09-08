"""Tests for hivemind_bus_client.encryption — encrypt/decrypt roundtrips and edge cases."""
import json
import pytest
from Cryptodome.Random import get_random_bytes

from hivemind_bus_client.encryption import (
    encrypt_as_json, decrypt_from_json,
    encrypt_bin, decrypt_bin,
    encrypt_AES, decrypt_AES_128,
    encrypt_ChaCha20_Poly1305, decrypt_ChaCha20_Poly1305,
    hybrid_encrypt, hybrid_decrypt,
    SupportedCiphers, SupportedEncodings,
    get_encoder, get_decoder,
    _norm_cipher, _norm_encoding,
    AES_KEY_SIZES, CHACHA20_KEY_SIZE,
)
from hivemind_bus_client.exceptions import (
    EncryptionKeyError, DecryptionKeyError, InvalidEncoding, InvalidCipher, InvalidKeySize,
)


class TestAESEncryption:
    def test_roundtrip_16_byte_key(self):
        key = get_random_bytes(16)
        ct, tag, nonce = encrypt_AES(key, "hello")
        result = decrypt_AES_128(key, ct, tag, nonce)
        assert result == b"hello"

    def test_roundtrip_32_byte_key(self):
        key = get_random_bytes(32)
        ct, tag, nonce = encrypt_AES(key, b"binary data")
        result = decrypt_AES_128(key, ct, tag, nonce)
        assert result == b"binary data"

    def test_invalid_key_size(self):
        with pytest.raises(InvalidKeySize):
            encrypt_AES(b"short", "test")

    def test_string_key(self):
        key = "a" * 16
        ct, tag, nonce = encrypt_AES(key, "test")
        result = decrypt_AES_128(key, ct, tag, nonce)
        assert result == b"test"


class TestChaCha20Encryption:
    def test_roundtrip(self):
        key = get_random_bytes(CHACHA20_KEY_SIZE)
        ct, tag, nonce = encrypt_ChaCha20_Poly1305(key, "hello")
        result = decrypt_ChaCha20_Poly1305(key, ct, tag, nonce)
        assert result == b"hello"

    def test_invalid_key_size(self):
        with pytest.raises(InvalidKeySize):
            encrypt_ChaCha20_Poly1305(b"short", "test")

    def test_invalid_nonce_size(self):
        key = get_random_bytes(CHACHA20_KEY_SIZE)
        with pytest.raises(InvalidKeySize):
            encrypt_ChaCha20_Poly1305(key, "test", nonce=b"short")


class TestBinEncryptDecrypt:
    @pytest.mark.parametrize("cipher", [SupportedCiphers.AES_GCM, SupportedCiphers.CHACHA20_POLY1305])
    def test_roundtrip(self, cipher):
        key = get_random_bytes(32)
        encrypted = encrypt_bin(key, "secret message", cipher)
        decrypted = decrypt_bin(key, encrypted, cipher)
        assert decrypted == b"secret message"

    def test_wrong_key_raises(self):
        key1 = get_random_bytes(32)
        key2 = get_random_bytes(32)
        encrypted = encrypt_bin(key1, "secret", SupportedCiphers.AES_GCM)
        with pytest.raises(DecryptionKeyError):
            decrypt_bin(key2, encrypted, SupportedCiphers.AES_GCM)


class TestJsonEncryptDecrypt:
    @pytest.mark.parametrize("cipher", [SupportedCiphers.AES_GCM, SupportedCiphers.CHACHA20_POLY1305])
    @pytest.mark.parametrize("encoding", [SupportedEncodings.JSON_B64, SupportedEncodings.JSON_HEX,
                                           SupportedEncodings.JSON_B32])
    def test_roundtrip(self, cipher, encoding):
        key = get_random_bytes(32)
        encrypted = encrypt_as_json(key, "hello world", cipher=cipher, encoding=encoding)
        decrypted = decrypt_from_json(key, encrypted, cipher=cipher, encoding=encoding)
        assert decrypted == "hello world"

    def test_dict_plaintext(self):
        key = get_random_bytes(32)
        data = {"utterance": "hello"}
        encrypted = encrypt_as_json(key, data)
        decrypted = decrypt_from_json(key, encrypted)
        assert json.loads(decrypted) == data

    def test_json_output_has_required_fields(self):
        key = get_random_bytes(32)
        encrypted = json.loads(encrypt_as_json(key, "test"))
        assert "ciphertext" in encrypted
        assert "tag" in encrypted
        assert "nonce" in encrypted


class TestNormHelpers:
    def test_norm_cipher_string(self):
        assert _norm_cipher("AES-GCM") == SupportedCiphers.AES_GCM

    def test_norm_cipher_enum(self):
        assert _norm_cipher(SupportedCiphers.AES_GCM) == SupportedCiphers.AES_GCM

    def test_norm_cipher_invalid(self):
        with pytest.raises(InvalidCipher):
            _norm_cipher("INVALID")

    def test_norm_encoding_string(self):
        assert _norm_encoding("JSON-B64") == SupportedEncodings.JSON_B64

    def test_norm_encoding_invalid(self):
        with pytest.raises(InvalidEncoding):
            _norm_encoding("INVALID")


class TestHybridEncryption:
    """Test hybrid RSA+AES-GCM encrypt/decrypt roundtrip."""

    @pytest.fixture(autouse=True)
    def _keys(self):
        from poorman_handshake.asymmetric.utils import create_RSA_key, load_RSA_key
        self.pub_pem, self.priv_key_obj = create_RSA_key()
        self.priv_key = self.priv_key_obj

    def test_roundtrip(self):
        plaintext = b"Hello from satellite!"
        envelope = hybrid_encrypt(self.pub_pem, plaintext, sign_key=self.priv_key)
        assert "encrypted_key" in envelope
        assert "ciphertext" in envelope
        assert "tag" in envelope
        assert "nonce" in envelope
        assert "signature" in envelope
        recovered = hybrid_decrypt(self.priv_key, envelope)
        assert recovered == plaintext

    def test_roundtrip_without_signature(self):
        plaintext = b"No signature"
        envelope = hybrid_encrypt(self.pub_pem, plaintext)
        assert "signature" not in envelope
        recovered = hybrid_decrypt(self.priv_key, envelope)
        assert recovered == plaintext

    def test_string_plaintext(self):
        plaintext = "unicode payload: café"
        envelope = hybrid_encrypt(self.pub_pem, plaintext)
        recovered = hybrid_decrypt(self.priv_key, envelope)
        assert recovered == plaintext.encode("utf-8")

    def test_wrong_key_fails(self):
        from poorman_handshake.asymmetric.utils import create_RSA_key
        other_pub, other_priv = create_RSA_key()
        envelope = hybrid_encrypt(other_pub, b"secret")
        with pytest.raises(Exception):
            hybrid_decrypt(self.priv_key, envelope)


class TestEncoderDecoder:
    @pytest.mark.parametrize("encoding", list(SupportedEncodings))
    def test_roundtrip(self, encoding):
        encoder = get_encoder(encoding)
        decoder = get_decoder(encoding)
        data = b"test data 12345"
        assert decoder(encoder(data)) == data
