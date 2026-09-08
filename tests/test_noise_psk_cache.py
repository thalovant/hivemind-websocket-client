"""The PSK cache's correctness properties.

A cached key wrong by one byte fails the handshake exactly as a wrong password
does, so none of this surfaces as a crash -- it surfaces as an outage that
looks like bad credentials. That is what these cover.
"""
import json
import os
import stat

import pytest

from hivemind_bus_client.noise import (
    NOISE_PSK_FILENAME,
    load_cached_psk,
    psk_password_verifier,
    save_cached_psk,
)

NODE_ID = "-----BEGIN PUBLIC KEY-----\nMIIBIjANBgkqhkiG9w0BAQEF\n-----END PUBLIC KEY-----"
PSK = bytes(range(32))


def _key_path(tmp_path):
    return str(tmp_path / "noise_key")


def test_cached_psk_round_trips(tmp_path):
    key_path = _key_path(tmp_path)
    verifier = psk_password_verifier("hunter2")
    assert load_cached_psk(key_path, NODE_ID, verifier) is None

    save_cached_psk(key_path, NODE_ID, PSK, verifier)
    assert load_cached_psk(key_path, NODE_ID, verifier) == PSK


def test_rotated_password_is_not_served_from_cache(tmp_path):
    key_path = _key_path(tmp_path)
    save_cached_psk(key_path, NODE_ID, PSK, psk_password_verifier("old"))

    assert load_cached_psk(key_path, NODE_ID, psk_password_verifier("new")) is None
    assert load_cached_psk(key_path, NODE_ID, psk_password_verifier("old")) == PSK


def test_cache_file_never_holds_the_password(tmp_path):
    key_path = _key_path(tmp_path)
    password = "a-very-distinctive-password-9931"
    save_cached_psk(key_path, NODE_ID, PSK, psk_password_verifier(password))

    raw = (tmp_path / NOISE_PSK_FILENAME).read_text()
    assert password not in raw


def test_cache_file_is_owner_only(tmp_path):
    key_path = _key_path(tmp_path)
    save_cached_psk(key_path, NODE_ID, PSK, psk_password_verifier("hunter2"))

    mode = stat.S_IMODE(os.stat(tmp_path / NOISE_PSK_FILENAME).st_mode)
    assert mode & 0o077 == 0, f"PSK cache is group/world accessible: {mode:o}"


def test_corrupt_cache_is_discarded_rather_than_failing(tmp_path):
    key_path = _key_path(tmp_path)
    verifier = psk_password_verifier("hunter2")
    save_cached_psk(key_path, NODE_ID, PSK, verifier)

    (tmp_path / NOISE_PSK_FILENAME).write_text("{ not json")
    assert load_cached_psk(key_path, NODE_ID, verifier) is None

    save_cached_psk(key_path, NODE_ID, PSK, verifier)
    assert load_cached_psk(key_path, NODE_ID, verifier) == PSK


def test_saving_is_best_effort_and_never_raises(tmp_path):
    # An unwritable location must cost a derivation, not a connection.
    save_cached_psk("/proc/nonexistent-dir/noise_key", NODE_ID, PSK, "verifier")


def test_no_key_path_means_no_cache(tmp_path):
    # Callers without a persistent key directory simply do not cache.
    save_cached_psk(None, NODE_ID, PSK, "verifier")
    assert load_cached_psk(None, NODE_ID, "verifier") is None


@pytest.mark.parametrize("password", ["", "hunter2", "pässwörd-ünïcode-✓", "x" * 200])
def test_verifier_is_stable_and_distinct(password):
    assert psk_password_verifier(password) == psk_password_verifier(password)
    assert psk_password_verifier(password) != psk_password_verifier(password + "!")


def test_cached_psk_still_completes_a_real_handshake(tmp_path):
    """The cache swaps ``password=`` for ``psk=``; both must yield one key.

    This is the test that matters. A PSK that differs from the password's
    derivation would still *look* fine here -- until the handshake failed
    against a peer that derived it the other way. So the client is run twice
    against a server that always derives from the password: once deriving,
    once loading the key it cached.
    """
    from hivemind_bus_client.noise import (
        NOISE_PATTERN_XX,
        NOISE_SUITE_CHACHA,
        build_prologue,
        start_noise_handshake,
    )

    node_id = "server-node-id-pem"
    password = "Tr0ub4dor-Horse-Battery-91x"
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

    # Second time the client loads the PSK it just cached; the server still
    # derives from the password, so agreement proves the two paths match.
    client, server = handshake()
    assert client.handshake_finished and server.handshake_finished


def test_the_listening_side_never_writes_a_psk_cache(tmp_path):
    """On the hub, node id is ours and the password varies per client.

    A node-keyed cache there would collide between clients and would collect
    every client's PSK into one file, so the responder must not write one.
    """
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
        password="whatever", node_id=node_id, prologue=prologue,
        key_path=str(tmp_path / "noise_key"))

    assert not (tmp_path / NOISE_PSK_FILENAME).exists()
