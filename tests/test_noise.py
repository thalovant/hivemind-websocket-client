"""Unit tests for the protocol v3 Noise glue (hivemind_bus_client.noise)."""

import unittest

from hivemind_bus_client.noise import (
    NOISE_PATTERN_KK,
    NOISE_PATTERN_XX,
    NOISE_SUITE_AESGCM,
    NOISE_SUITE_CHACHA,
    NOISE_SUITES,
    NOISE_SUPPORTED,
    NoiseHandshakeFailed,
    NoiseTransport,
    NoiseTransportFailed,
    build_prologue,
    canonical_json,
    noise_protocol_name,
    select_noise_options,
    start_noise_handshake,
)


class TestNegotiationHelpers(unittest.TestCase):
    def test_canonical_json_is_order_independent(self):
        a = canonical_json({"b": 1, "a": [2, 3]})
        b = canonical_json({"a": [2, 3], "b": 1})
        self.assertEqual(a, b)

    def test_noise_protocol_name(self):
        self.assertEqual(
            noise_protocol_name(NOISE_PATTERN_XX, NOISE_SUITE_CHACHA),
            "Noise_XXpsk2_25519_ChaChaPoly_SHA256",
        )

    def test_select_prefers_kk_only_when_pinned(self):
        patterns = [NOISE_PATTERN_KK, NOISE_PATTERN_XX]
        suites = [NOISE_SUITE_CHACHA]
        self.assertEqual(select_noise_options(patterns, suites),
                         (NOISE_PATTERN_XX, NOISE_SUITE_CHACHA))
        self.assertEqual(select_noise_options(patterns, suites, "aa" * 32),
                         (NOISE_PATTERN_KK, NOISE_SUITE_CHACHA))

    def test_select_returns_none_without_mutual_option(self):
        self.assertIsNone(select_noise_options([], [NOISE_SUITE_CHACHA]))
        self.assertIsNone(select_noise_options([NOISE_PATTERN_XX], []))
        self.assertIsNone(select_noise_options(["bogus"], ["bogus"]))

    def test_suite_registry_offers_both_chacha_first(self):
        # CRYPTO-1 §3.4.1: ChaChaPoly MUST (preferred), AES-GCM MAY (second)
        self.assertEqual(NOISE_SUITES,
                         [NOISE_SUITE_CHACHA, NOISE_SUITE_AESGCM])

    def test_select_prefers_chacha_regardless_of_server_order(self):
        patterns = [NOISE_PATTERN_XX]
        self.assertEqual(
            select_noise_options(patterns,
                                 [NOISE_SUITE_AESGCM, NOISE_SUITE_CHACHA]),
            (NOISE_PATTERN_XX, NOISE_SUITE_CHACHA))
        # an AES-GCM-only peer (e.g. Web Crypto) still negotiates
        self.assertEqual(
            select_noise_options(patterns, [NOISE_SUITE_AESGCM]),
            (NOISE_PATTERN_XX, NOISE_SUITE_AESGCM))

    def test_select_suite_mismatch(self):
        self.assertIsNone(select_noise_options([NOISE_PATTERN_XX],
                                               ["25519_Bogus_SHA512"]))

    def test_prologue_binds_negotiation(self):
        hello = {"node_id": "server", "pubkey": "abc"}
        handshake = {"max_protocol_version": 3, "noise": {"patterns": ["XXpsk2"]}}
        name = noise_protocol_name(NOISE_PATTERN_XX, NOISE_SUITE_CHACHA)
        p1 = build_prologue(hello, handshake, name)
        # any change to the advertised parameters changes the prologue
        tampered = dict(handshake, max_protocol_version=1)
        self.assertNotEqual(p1, build_prologue(hello, tampered, name))
        # both peers derive identical bytes from equal payloads
        self.assertEqual(p1, build_prologue(dict(hello), dict(handshake), name))


class TestHandshakeAndTransport(unittest.TestCase):
    """The noise primitive is a declared dependency: these must always run."""

    def test_noise_primitive_installed(self):
        self.assertTrue(NOISE_SUPPORTED)

    PROLOGUE = build_prologue({"node_id": "server"},
                              {"max_protocol_version": 3},
                              "Noise_XXpsk2_25519_ChaChaPoly_SHA256")

    def _handshake_pair(self, password_b="s3cr3t", prologue_b=None,
                        suite=NOISE_SUITE_CHACHA):
        prologue = build_prologue(
            {"node_id": "server"}, {"max_protocol_version": 3},
            noise_protocol_name(NOISE_PATTERN_XX, suite))
        common = dict(pattern=NOISE_PATTERN_XX, suite=suite,
                      node_id="server")
        alice = start_noise_handshake(initiator=True, password="s3cr3t",
                                      prologue=prologue, **common)
        bob = start_noise_handshake(initiator=False, password=password_b,
                                    prologue=prologue_b or prologue,
                                    **common)
        return alice, bob

    def _complete(self, alice, bob):
        m1 = alice.write_message(b"hi")
        bob.read_message(m1)
        m2 = bob.write_message(b"ho")
        alice.read_message(m2)
        m3 = alice.write_message(b"")
        bob.read_message(m3)
        return NoiseTransport(alice), NoiseTransport(bob)

    def test_xxpsk2_round_trip_and_static_key_exchange(self):
        alice, bob = self._handshake_pair()
        ta, tb = self._complete(alice, bob)
        # learned static keys are exposed for TOFU pinning
        self.assertTrue(ta.remote_static_key)
        self.assertTrue(tb.remote_static_key)
        # JSON frames round-trip as str, binary frames as bytes
        ct = ta.encrypt_frame('{"msg_type": "bus"}')
        self.assertEqual(tb.decrypt_frame(ct), '{"msg_type": "bus"}')
        ct = tb.encrypt_frame(b"\x00\x01binary")
        self.assertEqual(ta.decrypt_frame(ct), b"\x00\x01binary")

    def test_xxpsk2_aesgcm_round_trip(self):
        # the AES-GCM suite (Web Crypto / HiveMind-js peers) completes the
        # handshake and encrypts/decrypts transport frames both ways
        alice, bob = self._handshake_pair(suite=NOISE_SUITE_AESGCM)
        ta, tb = self._complete(alice, bob)
        self.assertTrue(ta.remote_static_key)
        self.assertTrue(tb.remote_static_key)
        ct = ta.encrypt_frame('{"msg_type": "bus"}')
        self.assertEqual(tb.decrypt_frame(ct), '{"msg_type": "bus"}')
        ct = tb.encrypt_frame(b"\x00\x01binary")
        self.assertEqual(ta.decrypt_frame(ct), b"\x00\x01binary")

    def test_suite_mismatch_between_peers_aborts(self):
        # peers that fixed different cipher suites cannot complete a handshake
        common = dict(pattern=NOISE_PATTERN_XX, node_id="server",
                      password="s3cr3t", prologue=self.PROLOGUE)
        alice = start_noise_handshake(initiator=True,
                                      suite=NOISE_SUITE_AESGCM, **common)
        bob = start_noise_handshake(initiator=False,
                                    suite=NOISE_SUITE_CHACHA, **common)
        with self.assertRaises(Exception):
            self._complete(alice, bob)

    def test_wrong_password_aborts(self):
        alice, bob = self._handshake_pair(password_b="wrong")
        with self.assertRaises(Exception):
            self._complete(alice, bob)

    def test_prologue_mismatch_aborts(self):
        tampered = build_prologue({"node_id": "server"},
                                  {"max_protocol_version": 1},  # downgraded
                                  "Noise_XXpsk2_25519_ChaChaPoly_SHA256")
        alice, bob = self._handshake_pair(prologue_b=tampered)
        with self.assertRaises(Exception):
            self._complete(alice, bob)

    def test_replay_is_rejected(self):
        ta, tb = self._complete(*self._handshake_pair())
        ct = ta.encrypt_frame("one")
        self.assertEqual(tb.decrypt_frame(ct), "one")
        # decrypting the same ciphertext again fails: the receive nonce
        # counter has advanced (CRYPTO-1 §3.4.5 replay resistance)
        with self.assertRaises(NoiseTransportFailed):
            tb.decrypt_frame(ct)

    def test_tampered_ciphertext_is_rejected(self):
        ta, tb = self._complete(*self._handshake_pair())
        ct = bytearray(ta.encrypt_frame("payload"))
        ct[-1] ^= 0x01
        with self.assertRaises(NoiseTransportFailed):
            tb.decrypt_frame(bytes(ct))

    def test_out_of_order_delivery_is_rejected(self):
        ta, tb = self._complete(*self._handshake_pair())
        first = ta.encrypt_frame("first")
        second = ta.encrypt_frame("second")
        with self.assertRaises(NoiseTransportFailed):
            tb.decrypt_frame(second)  # skipped ahead of `first`
        del first

    def test_transport_requires_finished_handshake(self):
        alice, _ = self._handshake_pair()
        with self.assertRaises(NoiseHandshakeFailed):
            NoiseTransport(alice)


if __name__ == "__main__":
    unittest.main()
