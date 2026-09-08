import base64
import enum
import json
from binascii import hexlify, unhexlify
from typing import Union, Optional, Dict, Any, Literal, List, Callable

import pybase64
from Cryptodome.Cipher import AES, ChaCha20_Poly1305
from cpuinfo import get_cpu_info

from hivemind_bus_client.exceptions import EncryptionKeyError, DecryptionKeyError, InvalidEncoding, InvalidCipher, \
    InvalidKeySize
from z85base91 import Z85B, B91, Z85P

# Cipher-specific constants
AES_KEY_SIZES = [16, 24, 32]  # poorman_handshake generates 32 bit secrets
AES_NONCE_SIZE = 16
AES_TAG_SIZE = 16
CHACHA20_KEY_SIZE = 32
CHACHA20_NONCE_SIZE = 12
CHACHA20_TAG_SIZE = 16


def cpu_supports_AES() -> bool:
    """
    Check if the CPU supports AES encryption.

    This function checks the CPU flags to determine if the hardware supports
    AES encryption. It does so by querying the CPU information and checking
    if the 'aes' flag is present.

    Returns:
        bool: True if AES is supported by the CPU, False otherwise.
    """
    return "aes" in get_cpu_info().get("flags", [])


class SupportedEncodings(str, enum.Enum):
    """
    Enum representing JSON-based encryption encodings.

    Ciphers output binary data, and JSON needs to transmit that data as plaintext.
    The supported encodings include Base64 and Hex encoding.
    """
    JSON_B91 = "JSON-B91"  # JSON text output with Base91 encoding
    JSON_Z85B = "JSON-Z85B"  # JSON text output with Z85B encoding
    JSON_Z85P = "JSON-Z85P"  # JSON text output with Z85B encoding
    JSON_B64 = "JSON-B64"  # JSON text output with Base64 encoding
    JSON_URLSAFE_B64 = "JSON-URLSAFE-B64"  # JSON text output with url safe Base64 encoding
    JSON_B32 = "JSON-B32"  # JSON text output with Base32 encoding
    JSON_HEX = "JSON-HEX"  # JSON text output with Base16 (Hex) encoding


def get_encoder(encoding: SupportedEncodings) -> Callable[[bytes], bytes]:
    encoding = _norm_encoding(encoding)
    if encoding == SupportedEncodings.JSON_B64:
        return pybase64.b64encode
    if encoding == SupportedEncodings.JSON_URLSAFE_B64:
        return pybase64.urlsafe_b64encode
    if encoding == SupportedEncodings.JSON_B32:
        return base64.b32encode
    if encoding == SupportedEncodings.JSON_HEX:
        return hexlify
    if encoding == SupportedEncodings.JSON_Z85B:
        return Z85B.encode
    if encoding == SupportedEncodings.JSON_Z85P:
        return Z85P.encode
    if encoding == SupportedEncodings.JSON_B91:
        return B91.encode
    raise InvalidEncoding(f"Invalid encoding: {encoding}")


def get_decoder(encoding: SupportedEncodings) -> Callable[[bytes], bytes]:
    encoding = _norm_encoding(encoding)
    if encoding == SupportedEncodings.JSON_B64:
        return pybase64.b64decode
    if encoding == SupportedEncodings.JSON_URLSAFE_B64:
        return pybase64.urlsafe_b64decode
    if encoding == SupportedEncodings.JSON_B32:
        return base64.b32decode
    if encoding == SupportedEncodings.JSON_HEX:
        return unhexlify
    if encoding == SupportedEncodings.JSON_Z85B:
        return Z85B.decode
    if encoding == SupportedEncodings.JSON_Z85P:
        return Z85P.decode
    if encoding == SupportedEncodings.JSON_B91:
        return B91.decode
    raise InvalidEncoding(f"Invalid encoding: {encoding}")


class SupportedCiphers(str, enum.Enum):
    """
    Enum representing binary encryption ciphers.

    Specifications:
      - AES - http://csrc.nist.gov/publications/fips/fips197/fips-197.pdf
      - GCM - http://csrc.nist.gov/publications/nistpubs/800-38D/SP-800-38D.pdf
      - CHACHA20-POLY1305 - https://datatracker.ietf.org/doc/html/rfc7539
    """
    AES_GCM = "AES-GCM"
    CHACHA20_POLY1305 = "CHACHA20-POLY1305"  # specified in RFC7539.


AES_CIPHERS = {c for c in SupportedCiphers if "AES" in c}
BLOCK_CIPHERS = AES_CIPHERS  # Blowfish etc can be added in the future


def optimal_ciphers() -> List[SupportedCiphers]:
    """
    Determine the optimal ciphers based on CPU support.

    This function checks if the CPU supports AES encryption. If it does, it
    returns a list of ciphers with AES first, followed by other supported ciphers.
    If AES is not supported, the function returns a list of ciphers with
    ChaCha20-Poly1305 first.

    Returns:
        List[SupportedCiphers]: A list of optimal ciphers based on CPU support.
    """
    if not cpu_supports_AES():
        return [SupportedCiphers.CHACHA20_POLY1305, SupportedCiphers.AES_GCM]
    return [SupportedCiphers.AES_GCM, SupportedCiphers.CHACHA20_POLY1305]


def _norm_cipher(cipher: Union[SupportedCiphers, str]) -> SupportedCiphers:
    """
    Normalize a cipher to an enum member.

    This function takes either a cipher string or an enum member and ensures it
    is converted to the corresponding enum member of SupportedCiphers. If the input
    is invalid, an InvalidCipher exception is raised.

    Args:
        cipher (Union[SupportedCiphers, str]): The cipher to normalize, either a string or an enum member.

    Returns:
        SupportedCiphers: The corresponding enum member of SupportedCiphers.

    Raises:
        InvalidCipher: If the cipher is invalid.
    """
    if isinstance(cipher, SupportedCiphers):
        return cipher  # If already an enum member, just return it

    # Convert string to enum member by matching the value
    for member in SupportedCiphers:
        if member.value == cipher:
            return member

    raise InvalidCipher(f"Invalid cipher: {cipher}")


def _norm_encoding(encoding: Union[SupportedEncodings, str]) -> SupportedEncodings:
    """
    Normalize an encoding to an enum member.

    This function takes either an encoding string or an enum member and ensures it
    is converted to the corresponding enum member of SupportedEncodings. If the input
    is invalid, an InvalidEncoding exception is raised.

    Args:
        encoding (Union[SupportedEncodings, str]): The encoding to normalize, either a string or an enum member.

    Returns:
        SupportedEncodings: The corresponding enum member of SupportedEncodings.

    Raises:
        InvalidEncoding: If the encoding is invalid.
    """
    if isinstance(encoding, SupportedEncodings):
        return encoding  # If already an enum member, just return it

    # Convert string to enum member by matching the value
    for member in SupportedEncodings:
        if member.value == encoding:
            return member

    raise InvalidEncoding(f"Invalid JSON encoding: {encoding}")


def encrypt_as_json(
        key: Union[str, bytes],
        plaintext: Union[str, Dict[str, Any]],
        cipher: Union[SupportedCiphers, str] = SupportedCiphers.AES_GCM,
        encoding: Union[SupportedEncodings, str] = SupportedEncodings.JSON_B64
) -> str:
    """
    Encrypts the given data and outputs it as a JSON string.

    Args:
        key (Union[str, bytes]): The encryption key, up to 16 bytes. Longer keys will be truncated.
        plaintext (Union[str, Dict[str, Any]]): The data to encrypt. If a dictionary, it will be serialized to JSON.
        cipher (Union[SupportedCiphers, str]): The encryption cipher. Supported options:
            - AES-GCM (default)
        encoding (Union[SupportedEncodings, str]): The encoding type for JSON. Supported options:
            - JSON-B64 (default)

    Returns:
        str: A JSON string containing the encrypted data, nonce, and tag.

    Raises:
        InvalidCipher: If an unsupported cipher is provided.
        InvalidEncoding: If an unsupported encoding is provided.
    """

    cipher = _norm_cipher(cipher)
    encoding = _norm_encoding(encoding)

    # If plaintext is a dictionary, convert it to a JSON string
    if isinstance(plaintext, dict):
        plaintext = json.dumps(plaintext)

    try:
        ciphertext = encrypt_bin(key=key, plaintext=plaintext, cipher=cipher)
    except InvalidKeySize as e:
        raise e
    except Exception as e:
        raise EncryptionKeyError from e

    # Extract nonce/tag depending on cipher, sizes are different
    if cipher in AES_CIPHERS:
        nonce, ciphertext, tag = (
            ciphertext[:AES_NONCE_SIZE],
            ciphertext[AES_NONCE_SIZE:-AES_TAG_SIZE],
            ciphertext[-AES_TAG_SIZE:]
        )
    else:
        nonce, ciphertext, tag = (
            ciphertext[:CHACHA20_NONCE_SIZE],
            ciphertext[CHACHA20_NONCE_SIZE:-CHACHA20_TAG_SIZE],
            ciphertext[-CHACHA20_TAG_SIZE:]
        )

    # Choose encoder based on the encoding
    encoder = get_encoder(encoding)

    # Return the JSON-encoded ciphertext, tag, and nonce
    return json.dumps({
        "ciphertext": encoder(ciphertext).decode('utf-8'),
        "tag": encoder(tag).decode('utf-8'),
        "nonce": encoder(nonce).decode('utf-8')
    })


def decrypt_from_json(key: Union[str, bytes], ciphertext_json: Union[str, bytes],
                      cipher: Union[SupportedCiphers, str] = SupportedCiphers.AES_GCM,
                      encoding: Union[SupportedEncodings, str] = SupportedEncodings.JSON_B64) -> str:
    """
    Decrypts data from a JSON string.

    Args:
        key (Union[str, bytes]): The decryption key, up to 16 bytes. Longer keys will be truncated.
        ciphertext_json (Union[str, bytes]): The encrypted data as a JSON string or bytes.
        cipher (SupportedEncodings): The cipher used for encryption.

    Returns:
        str: The decrypted plaintext data.

    Raises:
        InvalidCipher: If an unsupported cipher is provided.
        InvalidEncoding: If an unsupported encoding is provided.
        DecryptionKeyError: If decryption fails due to an invalid key or corrupted data.
    """
    cipher = _norm_cipher(cipher)
    encoding = _norm_encoding(encoding)

    if isinstance(ciphertext_json, str):
        ciphertext_json = json.loads(ciphertext_json)

    decoder = get_decoder(encoding)

    # JSON stores encoded values as str; decoders expect bytes
    def _to_bytes(val):
        return val.encode("utf-8") if isinstance(val, str) else val

    ciphertext: bytes = decoder(_to_bytes(ciphertext_json["ciphertext"]))

    if "tag" not in ciphertext_json:  # web crypto compatibility
        if cipher in AES_CIPHERS:
            ciphertext, tag = ciphertext[:-AES_TAG_SIZE], ciphertext[-AES_TAG_SIZE:]
        else:
            ciphertext, tag = ciphertext[:-CHACHA20_TAG_SIZE], ciphertext[-CHACHA20_TAG_SIZE:]
    else:
        tag = decoder(_to_bytes(ciphertext_json["tag"]))
    nonce = decoder(_to_bytes(ciphertext_json["nonce"]))

    try:
        ciphertext = decrypt_bin(key=key,
                                 ciphertext=nonce + ciphertext + tag,
                                 cipher=cipher)
        return ciphertext.decode("utf-8")
    except InvalidKeySize as e:
        raise e
    except Exception as e:
        raise DecryptionKeyError from e


def encrypt_bin(key: Union[str, bytes], plaintext: Union[str, bytes], cipher: Union[SupportedCiphers, str]) -> bytes:
    """
    Encrypts the given data and returns it as binary.

    Args:
        key (Union[str, bytes]): The encryption key, up to 16 bytes. Longer keys will be truncated.
        plaintext (Union[str, bytes]): The data to encrypt. Strings will be encoded as UTF-8.
        cipher (SupportedCiphers): The encryption cipher. Supported options:
            - AES_GCM: AES-GCM with 128-bit/256-bit key
            - CHACHA20_POLY1305: ChaCha20-Poly1305 with 256-bit key

    Returns:
        bytes: The encrypted data, including the nonce and tag.

    Raises:
        InvalidCipher: If an unsupported cipher is provided.
        InvalidKeySize: If an invalid key size is provided.
    """
    cipher = _norm_cipher(cipher)

    encryptor = encrypt_AES if cipher in AES_CIPHERS else encrypt_ChaCha20_Poly1305

    try:
        if cipher in BLOCK_CIPHERS:
            if cipher == SupportedCiphers.AES_GCM:
                mode = AES.MODE_GCM
            else:
                raise ValueError("invalid block cipher mode")
            ciphertext, tag, nonce = encryptor(key, plaintext, mode=mode)
        else:
            ciphertext, tag, nonce = encryptor(key, plaintext)
    except InvalidKeySize as e:
        raise e
    except Exception as e:
        raise EncryptionKeyError from e

    return nonce + ciphertext + tag


def decrypt_bin(key: Union[str, bytes], ciphertext: Union[str, bytes], cipher: Union[SupportedCiphers, str]) -> bytes:
    """
    Decrypts the given binary data.

    Args:
        key (Union[str, bytes]): The decryption key, up to 16 bytes. Longer keys will be truncated.
        ciphertext (Union[str, bytes]): The data to decrypt, including the nonce and tag.
        cipher (SupportedCiphers): The cipher used for encryption.

    Returns:
        bytes: The decrypted data.

    Raises:
        InvalidCipher: If an unsupported cipher is provided.
        DecryptionKeyError: If decryption fails due to an invalid key or corrupted data.
    """
    cipher = _norm_cipher(cipher)

    # extract nonce/tag depending on cipher, sizes are different
    if cipher == SupportedCiphers.AES_GCM:
        nonce, ciphertext, tag = (ciphertext[:AES_NONCE_SIZE],
                                  ciphertext[AES_NONCE_SIZE:-AES_TAG_SIZE],
                                  ciphertext[-AES_TAG_SIZE:])
    else:
        nonce, ciphertext, tag = (ciphertext[:CHACHA20_NONCE_SIZE],
                                  ciphertext[CHACHA20_NONCE_SIZE:-CHACHA20_TAG_SIZE],
                                  ciphertext[-CHACHA20_TAG_SIZE:])

    decryptor = decrypt_AES_128 if cipher in AES_CIPHERS else decrypt_ChaCha20_Poly1305
    try:
        if cipher in BLOCK_CIPHERS:
            if cipher == SupportedCiphers.AES_GCM:
                mode = AES.MODE_GCM
            else:
                raise ValueError("invalid block cipher mode")
            return decryptor(key, ciphertext, tag, nonce, mode=mode)
        return decryptor(key, ciphertext, tag, nonce)
    except InvalidKeySize as e:
        raise e
    except Exception as e:
        raise DecryptionKeyError from e


#############################
# Cipher Implementations
def encrypt_AES(key: Union[str, bytes], text: Union[str, bytes],
                nonce: Optional[bytes] = None,
                mode: Literal[AES.MODE_GCM] = AES.MODE_GCM) -> tuple[bytes, bytes, bytes]:
    """
    Encrypts plaintext using AES-GCM-128.

    Args:
        key (Union[str, bytes]): The encryption key. Strings will be encoded as UTF-8.
        text (Union[str, bytes]): The plaintext to encrypt.
        nonce (Optional[bytes]): An optional nonce. If None, a new one is generated.

    Returns:
        tuple[bytes, bytes, bytes]: The ciphertext, authentication tag, and nonce.
    """
    if not isinstance(text, bytes):
        text = bytes(text, encoding="utf-8")
    if not isinstance(key, bytes):
        key = bytes(key, encoding="utf-8")
    # AES-128 uses 128 bit/16 byte keys
    # AES-256 uses 256 bit/32 byte keys
    if len(key) not in AES_KEY_SIZES:
        raise InvalidKeySize("AES-GCM requires a 16/24/32 bytes key")
    cipher = AES.new(key, mode, nonce=nonce)
    ciphertext, tag = cipher.encrypt_and_digest(text)
    return ciphertext, tag, cipher.nonce


def decrypt_AES_128(key: Union[str, bytes],
                    ciphertext: bytes,
                    tag: bytes,
                    nonce: bytes,
                    mode: Literal[AES.MODE_GCM] = AES.MODE_GCM) -> bytes:
    """
    Decrypts ciphertext encrypted using AES-GCM-128.

    Args:
        key (Union[str, bytes]): The decryption key. Strings will be encoded as UTF-8.
        ciphertext (bytes): The encrypted ciphertext.
        tag (bytes): The authentication tag.
        nonce (bytes): The nonce used during encryption.

    Returns:
        str: The decrypted plaintext.

    Raises:
        InvalidKeySize: If key size is not valid
        ValueError: If decryption or authentication fails.
    """
    if isinstance(key, str):
        key = key.encode("utf-8")
    # AES-128 uses 128 bit/16 byte keys
    # AES-256 uses 256 bit/32 byte keys
    if len(key) not in AES_KEY_SIZES:
        raise InvalidKeySize("AES-GCM requires a 16/24/32 bytes key")
    cipher = AES.new(key, mode, nonce)
    return cipher.decrypt_and_verify(ciphertext, tag)


def encrypt_ChaCha20_Poly1305(key: Union[str, bytes],
                              text: Union[str, bytes],
                              nonce: Optional[bytes] = None) -> tuple[bytes, bytes, bytes]:
    """
    Encrypts plaintext using ChaCha20-Poly1305.

    Args:
        key (Union[str, bytes]): The encryption key. Strings will be encoded as UTF-8.
        text (Union[str, bytes]): The plaintext to encrypt.
        nonce (Optional[bytes]): An optional nonce. If None, a new one is generated.

    Returns:
        tuple[bytes, bytes, bytes]: The ciphertext, authentication tag, and nonce.
    """
    if isinstance(text, str):
        text = text.encode("utf-8")
    if isinstance(key, str):
        key = key.encode("utf-8")

    if len(key) != CHACHA20_KEY_SIZE:  # ChaCha20 uses 256 bit/32 byte keys
        raise InvalidKeySize("CHACHA20-POLY1305 requires a 32-byte key")
    if nonce:
        if len(nonce) != CHACHA20_NONCE_SIZE:  # 92bits/12bytes per RFC7539
            raise InvalidKeySize("CHACHA20-POLY1305 requires a 12-byte nonce per RFC7539")
    cipher = ChaCha20_Poly1305.new(key=key, nonce=nonce)
    ciphertext, tag = cipher.encrypt_and_digest(text)
    return ciphertext, tag, cipher.nonce


def decrypt_ChaCha20_Poly1305(key: Union[str, bytes],
                              ciphertext: bytes,
                              tag: bytes,
                              nonce: bytes) -> bytes:
    """
    Decrypts ciphertext encrypted using AES-GCM-128.

    Args:
        key (Union[str, bytes]): The decryption key. Strings will be encoded as UTF-8.
        ciphertext (bytes): The encrypted ciphertext.
        tag (bytes): The authentication tag.
        nonce (bytes): The nonce used during encryption.

    Returns:
        str: The decrypted plaintext.

    Raises:
        InvalidKeySize:
        ValueError: If decryption or authentication fails.
    """
    if isinstance(key, str):
        key = key.encode("utf-8")

    if len(key) != CHACHA20_KEY_SIZE:  # ChaCha20 uses 256 bit/32 byte keys
        raise InvalidKeySize("CHACHA20-POLY1305 requires a 32-byte key")
    if nonce:
        if len(nonce) != CHACHA20_NONCE_SIZE:  # 92bits/12bytes per RFC7539
            raise InvalidKeySize("CHACHA20-POLY1305 requires a 12-byte nonce per RFC7539")
    cipher = ChaCha20_Poly1305.new(key=key, nonce=nonce)
    return cipher.decrypt_and_verify(ciphertext, tag)


def hybrid_encrypt(public_key: Union[str, bytes, 'RSA.RsaKey'],
                   plaintext: Union[str, bytes],
                   sign_key: Optional[Union[str, bytes, 'RSA.RsaKey']] = None) -> Dict[str, str]:
    """Hybrid-encrypt *plaintext* for the owner of *public_key*.

    Generates a random 32-byte AES-GCM key, encrypts the payload with it,
    then RSA-encrypts only the AES key with the target's public key.
    Optionally signs the ciphertext with *sign_key*.

    The returned dict is JSON-serialisable and safe to embed in a
    ``HiveMessage`` payload.

    Args:
        public_key: RSA public key of the target peer.
        plaintext: Data to encrypt.
        sign_key: Optional RSA private key to sign the ciphertext.

    Returns:
        Dict with keys ``encrypted_key``, ``ciphertext``, ``tag``, ``nonce``,
        and optionally ``signature`` — all base64-encoded strings.
    """
    from Cryptodome.Random import get_random_bytes
    from poorman_handshake.asymmetric.utils import encrypt_RSA, sign_RSA

    if isinstance(plaintext, str):
        plaintext = plaintext.encode("utf-8")

    # generate ephemeral symmetric key
    sym_key = get_random_bytes(32)  # AES-256

    # encrypt payload with AES-GCM
    ciphertext, tag, nonce = encrypt_AES(sym_key, plaintext)

    # RSA-encrypt the symmetric key
    encrypted_key = encrypt_RSA(public_key, sym_key)

    result = {
        "encrypted_key": pybase64.b64encode(encrypted_key).decode("ascii"),
        "ciphertext": pybase64.b64encode(ciphertext).decode("ascii"),
        "tag": pybase64.b64encode(tag).decode("ascii"),
        "nonce": pybase64.b64encode(nonce).decode("ascii"),
    }

    if sign_key is not None:
        signature = sign_RSA(sign_key, ciphertext)
        result["signature"] = pybase64.b64encode(signature).decode("ascii")

    return result


def hybrid_decrypt(private_key: Union[str, bytes, 'RSA.RsaKey'],
                   envelope: Dict[str, str]) -> bytes:
    """Decrypt a hybrid-encrypted envelope.

    The envelope must contain ``encrypted_key``, ``ciphertext``, ``tag``,
    and ``nonce`` — all base64-encoded.

    Args:
        private_key: RSA private key to decrypt the symmetric key.
        envelope: The dict produced by ``hybrid_encrypt``.

    Returns:
        The decrypted plaintext bytes.
    """
    from poorman_handshake.asymmetric.utils import decrypt_RSA

    encrypted_key = pybase64.b64decode(envelope["encrypted_key"])
    ciphertext = pybase64.b64decode(envelope["ciphertext"])
    tag = pybase64.b64decode(envelope["tag"])
    nonce = pybase64.b64decode(envelope["nonce"])

    # RSA-decrypt the symmetric key
    sym_key = decrypt_RSA(private_key, encrypted_key)

    # AES-GCM decrypt the payload
    return decrypt_AES_128(sym_key, ciphertext, tag, nonce)


if __name__ == "__main__":
    from Cryptodome.Random import get_random_bytes

    print("JSON-B64" == SupportedEncodings.JSON_B64)

    key = get_random_bytes(CHACHA20_KEY_SIZE)
    plaintext = b'Attack at dawn'
    ciphertext, tag, nonce = encrypt_ChaCha20_Poly1305(key, plaintext)
    recovered = decrypt_ChaCha20_Poly1305(key, ciphertext, tag, nonce)
    print(recovered)
    assert recovered == plaintext
