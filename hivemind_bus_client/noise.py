"""HiveMind protocol version 3 — Noise handshake and transport glue.

Implements the version-3 session layer of HIVEMIND-CRYPTO-1 §3.4 on top of
the ``poorman_handshake.noise`` primitive:

- **negotiation helpers** — the server advertises supported Noise ``patterns``
  and ``suites`` (preference ordered); the node selects one of each and fixes
  the Noise protocol name (HIVEMIND-CRYPTO-1 §3.4.1/§3.4.2)
- **prologue construction** — the cleartext ``HELLO`` payload, the cleartext
  parameter ``HANDSHAKE`` payload, and the node's selected protocol name are
  bound into the Noise prologue so any tampering with the negotiation aborts
  the handshake (HIVEMIND-CRYPTO-1 §3.4.3)
- **transport framing** — after ``Split()`` every message travels as a Noise
  transport message under the per-direction ``CipherState``s, whose strictly
  sequential nonce counters give replay resistance and ordering enforcement
  (HIVEMIND-CRYPTO-1 §3.4.5); the fresh-IV AEAD construction of protocol
  versions 0-2 is not used on a version-3 session

Protocol version 2 and below are untouched: this module is only entered when
both peers negotiate version 3.
"""
import hashlib
import json
import os
import threading
from binascii import hexlify
from typing import Any, Dict, List, Optional, Tuple, Union

from ovos_utils.log import LOG


try:
    from poorman_handshake.noise import NoiseHandShake, derive_psk

    NOISE_SUPPORTED = True
except ImportError:  # poorman-handshake without the noise primitive
    NoiseHandShake = None  # type: ignore
    derive_psk = None  # type: ignore
    NOISE_SUPPORTED = False

# HiveMind protocol version that switches the handshake to Noise
PROTOCOL_V3 = 3

# registered handshake patterns, preference ordered (HIVEMIND-CRYPTO-1 §3.4.2)
NOISE_PATTERN_XX = "XXpsk2"  # general case, MUST support
NOISE_PATTERN_KK = "KKpsk0"  # pre-provisioned static keys, MAY support
NOISE_PATTERNS: List[str] = [NOISE_PATTERN_KK, NOISE_PATTERN_XX]

# registered cipher suites, preference ordered (HIVEMIND-CRYPTO-1 §3.4.1)
NOISE_SUITE_CHACHA = "25519_ChaChaPoly_SHA256"  # MUST support
NOISE_SUITE_AESGCM = "25519_AESGCM_SHA256"  # MAY support (Web Crypto peers)
NOISE_SUITES: List[str] = [NOISE_SUITE_CHACHA, NOISE_SUITE_AESGCM]

# transport frame markers: the first plaintext byte tags the inner framing so
# the receiver knows how to parse the decrypted bytes
_FRAME_JSON = b"\x00"  # utf-8 JSON HiveMessage
_FRAME_BINARY = b"\x01"  # HIVEMIND-WIRE-1 binary frame (bitstring)


class NoiseHandshakeFailed(Exception):
    """The Noise handshake aborted — wrong password/PSK, tampered
    negotiation (prologue mismatch), static-key contradiction, or a
    malformed handshake message. Fatal: the connection must be rejected."""


class NoiseTransportFailed(Exception):
    """A Noise transport message failed to decrypt at the current receive
    counter — tampering, replay, or reordering. Fatal for the session."""


def canonical_json(payload: Dict[str, Any]) -> bytes:
    """Serialize a payload dict deterministically for prologue binding.

    Both peers must derive identical prologue bytes from the negotiation
    payloads; sorted keys + compact separators make the serialization
    independent of dict ordering and formatting.
    """
    return json.dumps(payload, sort_keys=True,
                      separators=(",", ":"), ensure_ascii=False).encode("utf-8")


def noise_protocol_name(pattern: str, suite: str) -> str:
    """Full Noise protocol name for a pattern + suite selection."""
    return f"Noise_{pattern}_{suite}"


def select_noise_options(server_patterns: List[str],
                         server_suites: List[str],
                         pinned_remote_key: Optional[str] = None
                         ) -> Optional[Tuple[str, str]]:
    """Pick the handshake pattern and suite from the server's advertised lists.

    ``KKpsk0`` is preferred when the remote static key is pre-provisioned
    (pinned) and the server offers it; otherwise ``XXpsk2``. Returns
    ``(pattern, suite)`` or None when there is no mutual option.
    """
    # walk our own preference-ordered list so 25519_ChaChaPoly_SHA256 wins
    # whenever both peers support it, regardless of the server's list order
    suite = next((s for s in NOISE_SUITES if s in server_suites), None)
    if suite is None:
        return None
    if pinned_remote_key and NOISE_PATTERN_KK in server_patterns:
        return NOISE_PATTERN_KK, suite
    if NOISE_PATTERN_XX in server_patterns:
        return NOISE_PATTERN_XX, suite
    return None


def build_prologue(hello_payload: Dict[str, Any],
                   handshake_payload: Dict[str, Any],
                   protocol_name: str) -> bytes:
    """Prologue bytes per HIVEMIND-CRYPTO-1 §3.4.3.

    Binds, in order: the server's cleartext ``HELLO`` payload, its cleartext
    parameter ``HANDSHAKE`` payload (advertised versions, patterns, suites,
    encodings, every other parameter), and the node's selected Noise protocol
    name. Both peers must supply identical bytes or the handshake aborts —
    this is the downgrade/tampering protection.
    """
    return (canonical_json(hello_payload)
            + canonical_json(handshake_payload)
            + protocol_name.encode("utf-8"))


class NoiseTransport:
    """A completed protocol-v3 Noise session.

    Wraps the post-``Split()`` transport of a :class:`NoiseHandShake` with
    thread-safe per-direction locks (the CipherState nonce counters are
    strictly sequential — encryption order must match send order) and the
    HiveMind frame markers that distinguish JSON from binary frames after
    decryption.
    """

    def __init__(self, handshake: "NoiseHandShake"):
        if not handshake.handshake_finished:
            raise NoiseHandshakeFailed("handshake not finished")
        self._hs = handshake
        self._send_lock = threading.Lock()
        self._recv_lock = threading.Lock()
        self.remote_static_key: Optional[str] = (
            hexlify(handshake.remote_pubkey).decode("utf-8")
            if handshake.remote_pubkey else None)
        self.handshake_hash: Optional[bytes] = handshake.handshake_hash

    def encrypt_frame(self, payload: Union[str, bytes]) -> bytes:
        """Encrypt one outgoing message as a Noise transport message.

        Args:
            payload: a serialized JSON HiveMessage (str) or a WIRE-1 binary
                frame (bytes).

        Returns:
            Ciphertext bytes to send as a single (binary) websocket message.
        """
        if isinstance(payload, str):
            plaintext = _FRAME_JSON + payload.encode("utf-8")
        else:
            plaintext = _FRAME_BINARY + bytes(payload)
        with self._send_lock:
            return self._hs.encrypt(plaintext)

    def decrypt_frame(self, data: bytes) -> Union[str, bytes]:
        """Decrypt one incoming Noise transport message.

        Raises:
            NoiseTransportFailed: on any AEAD failure — tampering, replay, or
                out-of-order delivery. Per HIVEMIND-CRYPTO-1 §3.4.5 the
                message MUST be rejected and never retried under another
                nonce; callers should treat this as fatal for the session.
        """
        with self._recv_lock:
            try:
                plaintext = self._hs.decrypt(bytes(data))
            except Exception as e:
                raise NoiseTransportFailed(
                    f"Noise transport message rejected (tampered, replayed "
                    f"or out-of-order): {e}") from e
        marker, body = plaintext[:1], plaintext[1:]
        if marker == _FRAME_JSON:
            return body.decode("utf-8")
        if marker == _FRAME_BINARY:
            return body
        raise NoiseTransportFailed(f"unknown v3 frame marker: {marker!r}")


#: Cached password-to-PSK derivations, beside the static key.
NOISE_PSK_FILENAME = "noise_psks.json"


def psk_password_verifier(password: Union[str, bytes]) -> str:
    """Fingerprint the password a cached PSK came from.

    A rotated password no longer matches, so the stale key is re-derived
    instead of being offered to the hub -- which refuses a wrong PSK exactly
    as it refuses a wrong password, making the cause hard to see.

    A hash, never the password itself, and it never leaves the machine: the
    identity file already holds the password on the same host.
    """
    if isinstance(password, str):
        password = password.encode("utf-8")
    return hashlib.sha256(b"thalovant-psk-verifier:" + password).hexdigest()


def _psk_cache_path(key_path: Optional[str]) -> Optional[str]:
    """The PSK cache sits beside the static key it belongs to."""
    if not key_path:
        return None
    return os.path.join(os.path.dirname(key_path) or ".", NOISE_PSK_FILENAME)


def _read_psk_cache(path: str) -> Dict[str, Any]:
    try:
        with open(path, "r", encoding="utf-8") as handle:
            cache = json.load(handle)
    except FileNotFoundError:
        return {}
    except (OSError, ValueError):
        # Derivable state: a damaged cache costs one derivation, never a
        # failed connection.
        return {}
    return cache if isinstance(cache, dict) else {}


def load_cached_psk(key_path: Optional[str], node_id: str,
                    verifier: str) -> Optional[bytes]:
    """The stored PSK for ``node_id``, or None when it cannot be used."""
    path = _psk_cache_path(key_path)
    if not path or not node_id:
        return None
    entry = _read_psk_cache(path).get(node_id)
    if not isinstance(entry, dict) or entry.get("verifier") != verifier:
        return None
    try:
        psk = bytes.fromhex(str(entry.get("psk", "")))
    except ValueError:
        return None
    return psk or None


def save_cached_psk(key_path: Optional[str], node_id: str, psk: bytes,
                    verifier: str) -> None:
    """Persist a derived PSK so the next connection skips argon2id.

    Best effort: the cache is an optimisation, so failing to write it must
    never fail a connection.
    """
    path = _psk_cache_path(key_path)
    if not path or not node_id or not psk:
        return
    try:
        directory = os.path.dirname(path)
        if directory:
            os.makedirs(directory, mode=0o700, exist_ok=True)
        cache = _read_psk_cache(path)
        entry = {"psk": psk.hex(), "verifier": verifier}
        if cache.get(node_id) == entry:
            return
        cache[node_id] = entry
        with open(path, "w", encoding="utf-8") as handle:
            json.dump(cache, handle, indent=2, sort_keys=True)
            handle.write("\n")
        # The mode on open() only applies at creation; an existing file keeps
        # whatever it had.
        os.chmod(path, 0o600)
    except OSError:
        LOG.debug("could not persist the Noise PSK cache at %s", path)


def start_noise_handshake(initiator: bool,
                          pattern: str,
                          suite: str,
                          password: Union[str, bytes],
                          node_id: str,
                          prologue: bytes,
                          key_path: Optional[str] = None,
                          remote_pubkey: Optional[str] = None
                          ) -> "NoiseHandShake":
    """Initialize a Noise handshake for a HiveMind protocol-v3 connection.

    The PSK is derived from the shared site password with argon2id, salted
    by ``SHA-256(node_id)`` of the *server's* node id (HIVEMIND-CRYPTO-1
    §3.4.4). The static X25519 key is loaded from (or generated and
    persisted to) ``key_path``.

    Args:
        initiator: True on the node (client) side, False on the server side.
        pattern: selected handshake pattern (``XXpsk2`` or ``KKpsk0``).
        suite: selected cipher suite (e.g. ``25519_ChaChaPoly_SHA256``).
        password: the shared site password (the only secret; never
            transmitted — it authenticates the handshake as the Noise PSK).
        node_id: the server's node id announced in its cleartext HELLO.
        prologue: bytes from :func:`build_prologue`.
        key_path: where the static X25519 private key persists.
        remote_pubkey: hex-encoded pinned remote static key (required for
            ``KKpsk0``).
    """
    if not NOISE_SUPPORTED:
        raise NoiseHandshakeFailed(
            "poorman-handshake was installed without the noise primitive")
    name = noise_protocol_name(pattern, suite)
    if key_path and os.path.dirname(key_path):
        os.makedirs(os.path.dirname(key_path), exist_ok=True)

    # argon2id at 64 MiB, and the answer never changes for a password and a
    # node id -- so derive once and keep it beside the static key.
    #
    # Initiator only. On the listening side ``node_id`` is our own while the
    # password varies per client, so a node-keyed cache would collide between
    # clients and would collect every client's PSK into one file; the hub
    # keeps its own bounded LRU instead.
    psk = None
    if initiator and derive_psk is not None:
        verifier = psk_password_verifier(password)
        psk = load_cached_psk(key_path, node_id, verifier)
        if psk is None:
            psk = derive_psk(password, node_id=node_id)
            save_cached_psk(key_path, node_id, psk, verifier)

    try:
        return NoiseHandShake(
            initiator=initiator,
            path=key_path,
            password=None if psk is not None else password,
            psk=psk,
            node_id=node_id,
            remote_pubkey=remote_pubkey if pattern == NOISE_PATTERN_KK else None,
            prologue=prologue,
            pattern=name.encode("utf-8"),
        )
    except Exception as e:
        raise NoiseHandshakeFailed(f"failed to initialize {name}: {e}") from e
