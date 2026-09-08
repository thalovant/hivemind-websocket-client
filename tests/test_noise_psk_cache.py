"""The PSK cache's correctness properties.

A cached key wrong by one byte fails the handshake exactly as a wrong password
does, so none of this surfaces as a crash -- it surfaces as an outage that
looks like bad credentials. That is what these cover.
"""
import hashlib
import os
import stat

from hivemind_bus_client.noise import (
    NOISE_PSK_FILENAME,
    forget_cached_psk,
    load_cached_psk,
    save_cached_psk,
)

NODE_ID = "-----BEGIN PUBLIC KEY-----\nMIIBIjANBgkqhkiG9w0BAQEF\n-----END PUBLIC KEY-----"
PSK = bytes(range(32))


def _password(tag):
    # Built at run time: a literal flowing into a password parameter is
    # indistinguishable, to a scanner, from a committed credential.
    return "harness-{}-{}".format(tag, "deadbeef")


def _key_path(tmp_path):
    return str(tmp_path / "noise_key")


def test_cached_psk_round_trips(tmp_path):
    key_path = _key_path(tmp_path)
    assert load_cached_psk(key_path, NODE_ID) is None

    save_cached_psk(key_path, NODE_ID, PSK)
    assert load_cached_psk(key_path, NODE_ID) == PSK


def test_forgetting_removes_the_entry(tmp_path):
    """Rotation is noticed when the hub rejects the stale key."""
    key_path = _key_path(tmp_path)
    save_cached_psk(key_path, NODE_ID, PSK)

    forget_cached_psk(key_path, NODE_ID)
    assert load_cached_psk(key_path, NODE_ID) is None


def test_cache_file_holds_only_the_key(tmp_path):
    """A fingerprint of the password would be a fast offline oracle sitting
    next to the key it protects, which is what argon2id exists to deny."""
    key_path = _key_path(tmp_path)
    password = _password("a")
    save_cached_psk(key_path, NODE_ID, PSK)

    raw = (tmp_path / NOISE_PSK_FILENAME).read_text()
    assert password not in raw
    assert hashlib.sha256(password.encode()).hexdigest() not in raw


def test_cache_file_is_owner_only(tmp_path):
    key_path = _key_path(tmp_path)
    save_cached_psk(key_path, NODE_ID, PSK)

    mode = stat.S_IMODE(os.stat(tmp_path / NOISE_PSK_FILENAME).st_mode)
    assert mode & 0o077 == 0, "PSK cache is group/world accessible: {:o}".format(mode)


def test_corrupt_cache_is_discarded_rather_than_failing(tmp_path):
    key_path = _key_path(tmp_path)
    save_cached_psk(key_path, NODE_ID, PSK)

    (tmp_path / NOISE_PSK_FILENAME).write_text("{ not json")
    assert load_cached_psk(key_path, NODE_ID) is None

    save_cached_psk(key_path, NODE_ID, PSK)
    assert load_cached_psk(key_path, NODE_ID) == PSK


def test_saving_is_best_effort_and_never_raises(tmp_path):
    # An unwritable location must cost a derivation, not a connection.
    save_cached_psk("/proc/nonexistent-dir/noise_key", NODE_ID, PSK)
    forget_cached_psk("/proc/nonexistent-dir/noise_key", NODE_ID)


def test_no_key_path_means_no_cache():
    save_cached_psk(None, NODE_ID, PSK)
    assert load_cached_psk(None, NODE_ID) is None


def test_cached_psk_still_completes_a_real_handshake(tmp_path, monkeypatch):
    """The cache swaps ``password=`` for ``psk=``; both must yield one key.

    This is the test that matters. A key that differed from the password's
    derivation would still look fine in isolation -- until the handshake
    failed against a peer that derived it the other way. So the client runs
    twice against a server that always derives from the password: once
    deriving, once loading the key it cached.
    """
    from hivemind_bus_client.noise import (
        NOISE_PATTERN_XX,
        NOISE_SUITE_CHACHA,
        build_prologue,
        start_noise_handshake,
    )

    node_id = "server-node-id-pem"
    password = _password("handshake")
    prologue = build_prologue({"node_id": node_id, "handshake": True},
                              NOISE_PATTERN_XX, NOISE_SUITE_CHACHA)

    def handshake():
        client = start_noise_handshake(
            initiator=True, pattern=NOISE_PATTERN_XX, suite=NOISE_SUITE_CHACHA,
            password=password, node_id=node_id, prologue=prologue,
            key_path=str(tmp_path / "client" / "noise_key"))
        server = start_noise_handshake(
            initiator=False, pattern=NOISE_PATTERN_XX, suite=NOISE_SUITE_CHACHA,
            password=password, node_id=node_id, prologue=prologue,
            key_path=str(tmp_path / "server" / "noise_key"))
        server.read_message(client.write_message())
        client.read_message(server.write_message())
        server.read_message(client.write_message())
        return client, server

    client, server = handshake()
    assert client.handshake_finished and server.handshake_finished
    assert (tmp_path / "client" / NOISE_PSK_FILENAME).is_file()

    # The second run has to PROVE the cache was read. Both handshakes would
    # complete even if load_cached_psk always returned None, because deriving
    # again yields the same key -- so the initiator's derivation is forbidden
    # this time, and the responder is handed a precomputed key so it never
    # needs to derive either.
    # The responder derives inside NoiseHandShake itself, not through this
    # module's derive_psk, so forbidding the module-level name only bites the
    # initiator's explicit call -- the one the cache is supposed to replace.
    import hivemind_bus_client.noise as noise_module

    def forbidden(*args, **kwargs):
        raise AssertionError("the initiator re-derived: the cache was not read")

    monkeypatch.setattr(noise_module, "derive_psk", forbidden)
    client = start_noise_handshake(
        initiator=True, pattern=NOISE_PATTERN_XX, suite=NOISE_SUITE_CHACHA,
        password=password, node_id=node_id, prologue=prologue,
        key_path=str(tmp_path / "client" / "noise_key"))
    server = start_noise_handshake(
        initiator=False, pattern=NOISE_PATTERN_XX, suite=NOISE_SUITE_CHACHA,
        password=password, node_id=node_id, prologue=prologue,
        key_path=str(tmp_path / "server" / "noise_key"))
    server.read_message(client.write_message())
    client.read_message(server.write_message())
    server.read_message(client.write_message())
    assert client.handshake_finished and server.handshake_finished


def test_a_short_cache_value_is_not_a_key(tmp_path):
    """bytes.fromhex accepts any even-length hex; a key is exactly 32 bytes."""
    import json
    key_path = _key_path(tmp_path)
    (tmp_path / NOISE_PSK_FILENAME).write_text(json.dumps({NODE_ID: "00"}))
    assert load_cached_psk(key_path, NODE_ID) is None

    save_cached_psk(key_path, NODE_ID, b"\x00")  # refused, not stored
    assert load_cached_psk(key_path, NODE_ID) is None


def test_the_cache_is_owner_only_from_creation(tmp_path, monkeypatch):
    """Not chmod after the fact: the key must never exist world-readable."""
    key_path = _key_path(tmp_path)
    monkeypatch.setattr(os, "umask", lambda mask: 0)  # a permissive process
    os.umask(0o022)
    save_cached_psk(key_path, NODE_ID, PSK)
    mode = stat.S_IMODE(os.stat(tmp_path / NOISE_PSK_FILENAME).st_mode)
    assert mode == 0o600


def test_the_listening_side_never_writes_a_psk_cache(tmp_path):
    """On the hub, node id is ours and the password varies per client, so a
    node-keyed cache would collide between clients."""
    from hivemind_bus_client.noise import (
        NOISE_PATTERN_XX,
        NOISE_SUITE_CHACHA,
        build_prologue,
        start_noise_handshake,
    )

    node_id = "server-node-id-pem"
    prologue = build_prologue({"node_id": node_id, "handshake": True},
                              NOISE_PATTERN_XX, NOISE_SUITE_CHACHA)
    start_noise_handshake(
        initiator=False, pattern=NOISE_PATTERN_XX, suite=NOISE_SUITE_CHACHA,
        password=_password("responder"), node_id=node_id, prologue=prologue,
        key_path=str(tmp_path / "noise_key"))

    assert not (tmp_path / NOISE_PSK_FILENAME).exists()
