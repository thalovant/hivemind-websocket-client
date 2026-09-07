import asyncio
import pybase64
import json
import os
import random

import threading
import time
from dataclasses import dataclass, field
from typing import Callable, List, Optional

from ovos_bus_client import Message as MycroftMessage
from ovos_bus_client import MessageBusClient
from ovos_bus_client.message import Message
from ovos_bus_client.session import Session, SessionManager
from ovos_utils.log import LOG

from hivemind_bus_client.client import HiveMessageBusClient
from hivemind_bus_client.encryption import SupportedEncodings, SupportedCiphers, optimal_ciphers, hybrid_decrypt
from hivemind_bus_client.hive_map import FloodIdCache, HiveMapper
from hivemind_bus_client.identity import NodeIdentity
from hivemind_bus_client.message import HiveMessage, HiveMessageType
from hivemind_bus_client.noise import (NOISE_PATTERN_KK, NOISE_SUPPORTED, PROTOCOL_V3,
                                       NoiseTransport, NoiseHandshakeFailed,
                                       build_prologue, canonical_json,
                                       noise_protocol_name, select_noise_options,
                                       start_noise_handshake)
from poorman_handshake import HandShake, PasswordHandShake
from poorman_handshake.asymmetric.utils import load_RSA_key, verify_RSA


# QUERY/CASCADE answers stream as response chunks terminated by a response
# wrapping this control message (protocol contract with hivemind-core; the
# end-of-stream is content, not metadata).
QUERY_STREAM_END = "hive.query.complete"

# Context key an INTERCOM's verified signer is stamped under, on the inner
# bus message actually handed to ``bus.emit``. Namespaced so it cannot
# collide with ordinary bus context (``source``, ``destination``, session
# fields, ...); a consumer that wants the *authenticated* origin of a
# delivered INTERCOM, as opposed to the sender-supplied ``source_peer`` on
# the (discarded) outer envelope, reads this key.
VERIFIED_SOURCE_PEER_KEY = "hivemind_verified_source_peer"


def _pw_min_bits() -> float:
    """Minimum password entropy (bits) enforced when building the shared-password
    handshake.

    poorman-handshake refuses guessable passwords by default; deployments that
    knowingly use a weak shared secret (or tests) can disable the runtime check
    with the ``HIVEMIND_DISABLE_PASSWORD_STRENGTH_CHECK`` env var (mirrors the
    hivemind-core server-side backstop, without importing it).
    """
    disabled = os.environ.get(
        "HIVEMIND_DISABLE_PASSWORD_STRENGTH_CHECK", ""
    ).strip().lower() in ("1", "true", "yes", "on")
    return 0.0 if disabled else 40.0


class CascadeAggregator:
    """Collects CASCADE responses over a timeout window, then selects the best.

    When the first response arrives a timer starts. Subsequent responses are
    buffered until the timer expires **or** ``expected_responses`` have been
    collected (whichever comes first), at which point ``select_callback``
    picks the winner and ``emit_callback`` delivers it.

    Args:
        timeout: Seconds to wait after the first response before resolving.
        select_callback: ``(List[HiveMessage]) -> Optional[HiveMessage]`` chooser.
        emit_callback: Called with the selected message to deliver it.
        expected_responses: If set, resolve early once this many responses arrive.
    """

    def __init__(self, timeout: float,
                 select_callback: Callable[[List[HiveMessage]], Optional[HiveMessage]],
                 emit_callback: Callable[[HiveMessage], None],
                 expected_responses: Optional[int] = None) -> None:

        self.timeout = timeout
        self.select_callback = select_callback or random.choice
        self.emit_callback = emit_callback
        self.expected_responses = expected_responses
        self.responses: List[HiveMessage] = []
        self._timer: Optional[threading.Timer] = None
        self._lock = threading.Lock()
        self._resolved = False

    def add_response(self, message: HiveMessage) -> None:
        """Buffer a response and start the timer on the first one.

        Resolves early if ``expected_responses`` have been collected.

        Args:
            message: A CASCADE response HiveMessage.
        """
        with self._lock:
            if self._resolved:
                return
            self.responses.append(message)
            if self._timer is None:
                self._timer = threading.Timer(self.timeout, self._resolve)
                self._timer.daemon = True
                self._timer.start()
            # early resolution when all expected responses have arrived
            if (self.expected_responses is not None
                    and len(self.responses) >= self.expected_responses):
                if self._timer is not None:
                    self._timer.cancel()
                self._do_resolve()
                return

    def _resolve(self) -> None:
        """Timer callback — resolve under lock."""
        with self._lock:
            self._do_resolve()

    def _do_resolve(self) -> None:
        """Select the best response and emit it.  Caller must hold ``_lock``."""
        if self._resolved:
            return
        self._resolved = True
        responses = self.responses
        self.responses = []
        self._timer = None
        if responses:
            selected = self.select_callback(responses)
            if selected is not None:
                self.emit_callback(selected)

    def cancel(self) -> None:
        """Cancel the pending timer without emitting."""
        with self._lock:
            if self._timer is not None:
                self._timer.cancel()
                self._timer = None
            self.responses = []
            self._resolved = True


class QueryLivenessTimer:
    """Bounds how long a QUERY originator waits for the answer stream.

    HIVEMIND-NODE-1 §5.5 / HIVEMIND-AGENT-1 §5: a node whose backend
    declines a QUERY escalates it silently, sending nothing back down. The
    originator therefore cannot tell "still travelling" from "the node
    holding it went away", and without a bound of its own it waits forever.

    Every chunk restarts the clock, because a stream that is still
    producing is alive. The stream terminator and an explicit
    ``hive.query.timeout`` stop it. On expiry ``on_timeout`` is called with
    the last query id seen, or an empty string when no response ever
    arrived to name one.
    """

    def __init__(self, timeout: float, on_timeout: Callable[[str], None]) -> None:
        self.timeout = timeout
        self.on_timeout = on_timeout
        self.query_id = ""
        self._timer: Optional[threading.Timer] = None
        self._lock = threading.Lock()

    def arm(self, query_id: str = "") -> None:
        """Start or restart the interval. A known query id is remembered."""
        with self._lock:
            if self._timer is not None:
                self._timer.cancel()
            if query_id:
                self.query_id = query_id
            self._timer = threading.Timer(self.timeout, self._fire)
            self._timer.daemon = True
            self._timer.start()

    def cancel(self) -> None:
        """Stop waiting — the stream ended or failed explicitly."""
        with self._lock:
            if self._timer is not None:
                self._timer.cancel()
                self._timer = None
            self.query_id = ""

    def _fire(self) -> None:
        with self._lock:
            if self._timer is None:
                return
            self._timer = None
            query_id = self.query_id
            self.query_id = ""
        self.on_timeout(query_id)


@dataclass()
class HiveMindSlaveInternalProtocol:
    """ this class handles all interactions between a hivemind listener and a ovos-core messagebus"""
    hm_bus: HiveMessageBusClient
    share_bus: bool = False
    bus: Optional[MessageBusClient] = None
    node_id: str = ""  # this is how ovos-core bus refers to this slave's master

    def register_bus_handlers(self):
        self.bus.on("hive.send.upstream", self.handle_send) # meant for internal usage by agent protocol plugins
        self.bus.on("message", self.handle_outgoing_mycroft)  # catch all

    # mycroft handlers  - from slave -> master
    def handle_send(self, message: Message):
        """ ovos wants to send a HiveMessage

        a device can be both a master and a slave, upstream messages are handled here

        HiveMindListenerInternalProtocol will handle requests meant to go downstream
        """

        payload = message.data.get("payload")
        msg_type = message.data["msg_type"]

        hmessage = HiveMessage(msg_type, payload=payload)

        if msg_type == HiveMessageType.BROADCAST:
            # only masters can broadcast, ignore silently
            #   if this device is also a master to something,
            #   HiveMindListenerInternalProtocol will handle the request
            pass
        else:
            self.hm_bus.emit(hmessage)

    def handle_outgoing_mycroft(self, message: Message):
        """ forward internal messages to masters"""
        if isinstance(message, str):
            # "message" is a special case in ovos-bus-client that is not deserialized
            message = Message.deserialize(message)

        # this allows the master node to do passive monitoring of bus events
        if self.share_bus:
            msg = HiveMessage(HiveMessageType.SHARED_BUS,
                              payload=message.serialize())
            self.hm_bus.emit(msg)

        # this message is targeted at master
        # eg, a response to some bus event injected by master
        # note: master might completely ignore it
        peers = message.context.get("destination")
        if peers:
            if not isinstance(peers, list):
                peers = [peers]
            if self.node_id in peers:
                msg = HiveMessage(HiveMessageType.BUS,
                                  payload=message.serialize())
                self.hm_bus.emit(msg)


@dataclass()
class HiveMindSlaveProtocol:
    """
    Joins this instance ovos-core bus with master ovos-core bus
    Master becomes able to inject arbitrary bus messages
    """
    hm: HiveMessageBusClient
    identity: Optional[NodeIdentity] = None
    handshake: Optional[HandShake] = None
    pswd_handshake: Optional[PasswordHandShake] = None
    internal_protocol: HiveMindSlaveInternalProtocol = None
    mpubkey: str = ""  # asc public PGP key from master
    shared_bus: bool = False
    binarize: bool = False
    site_id: str = "unknown"
    cascade_timeout: float = 5.0
    # NODE-1 §5.5: the deployment-configured interval after which this node,
    # as originator, gives up on a QUERY. Set to 0 to wait forever.
    query_timeout: float = 30.0
    cascade_select_callback: Optional[Callable[[List[HiveMessage]], Optional[HiveMessage]]] = None
    hive_mapper: Optional[HiveMapper] = None
    cascade_aggregator: Optional[CascadeAggregator] = field(default=None, repr=False)
    query_liveness: Optional[QueryLivenessTimer] = field(default=None, repr=False)
    # protocol v3 (Noise handshake) state — HIVEMIND-CRYPTO-1 §3.4
    noise_handshake: Optional[object] = field(default=None, repr=False)
    _noise_pattern: Optional[str] = field(default=None, repr=False)
    # True once a Noise session actually established on this socket. Read on
    # disconnect to tell "the handshake worked and the peer went away" from
    # "the handshake never completed", which is what distinguishes a normal
    # reconnect from the KKpsk0 lockout.
    _noise_established: bool = field(default=False, repr=False)
    _server_hello_payload: Optional[dict] = field(default=None, repr=False)
    _server_handshake_payload: Optional[dict] = field(default=None, repr=False)

    def bind_flood_cache(self, cache: FloodIdCache) -> None:
        """Share this node's PING flood dedup store with a co-located listener.

        A node that has an upstream runs two protocol objects: this slave
        (its connection to the upstream) and a
        ``HiveMindListenerProtocol`` (serving its own downstream clients).
        They are two halves of **one** node, and HIVEMIND-NODE-1 §4 gives
        the node — not the connection — exactly one part in a PING flood.
        With one cache each, both halves answer the same flood, under two
        different identities, and the flood's originator maps one node as
        two.

        hivemind-core calls this from
        ``HiveMindListenerProtocol.bind_upstream``, passing the listener's
        cache, so whichever half sees a ``flood_id`` first suppresses the
        other. A node with no upstream is unaffected and still answers
        exactly once.
        """
        if self.hive_mapper is None:
            self.hive_mapper = HiveMapper()
        self.hive_mapper._seen_flood_ids = cache

    def bind(self, bus: Optional[MessageBusClient] = None):
        if self.identity is None:
            self.identity = self.hm.identity or NodeIdentity()
        self.handshake = HandShake(self.identity.private_key)
        # PasswordHandShake is a legacy (v2) mechanism. Build it only after
        # the server explicitly selects that fallback in handle_handshake();
        # validating it here would apply v2 policy before v3 Noise negotiation.

        if bus is None:
            bus = MessageBusClient()
            bus.run_in_thread()
            bus.connected_event.wait()
        LOG.info("Initializing HiveMindSlaveInternalProtocol")
        self.internal_protocol = HiveMindSlaveInternalProtocol(bus=bus, hm_bus=self.hm)
        self.internal_protocol.register_bus_handlers()
        LOG.info("registering protocol handlers")
        self.hm.on(HiveMessageType.HELLO, self.handle_hello)
        self.hm.on(HiveMessageType.BROADCAST, self.handle_broadcast)
        self.hm.on(HiveMessageType.PROPAGATE, self.handle_propagate)
        self.hm.on(HiveMessageType.INTERCOM, self.handle_intercom)
        self.hm.on(HiveMessageType.ESCALATE, self.handle_illegal_msg)
        self.hm.on(HiveMessageType.SHARED_BUS, self.handle_illegal_msg)
        self.hm.on(HiveMessageType.QUERY, self.handle_query)
        self.hm.on(HiveMessageType.CASCADE, self.handle_cascade)
        self.hm.on(HiveMessageType.BUS, self.handle_bus)
        self.hm.on(HiveMessageType.HANDSHAKE, self.handle_handshake)

    def reset_connection_state(self):
        """Discard handshake state owned by the closed websocket.

        A socket can close between Noise messages. Reusing that partial
        handshake on the replacement socket would make the next server
        negotiation envelope look like a malformed Noise response.
        """
        # A KKpsk0 that never reached a session is the lockout signal. The
        # server does not send a Noise error for a KK it cannot complete — it
        # closes the socket — so this, not receive_noise_handshake, is where
        # most failures are observable.
        if not self._noise_established:
            self._drop_stale_pin_after_kk_failure()
        self._noise_established = False
        self.handshake = HandShake(self.identity.private_key)
        self.pswd_handshake = None
        self.mpubkey = ""
        self.noise_handshake = None
        self._noise_pattern = None
        self._server_hello_payload = None
        self._server_handshake_payload = None
        if self.internal_protocol is not None:
            self.internal_protocol.node_id = ""

    @property
    def node_id(self):
        # this is how ovos-core bus refers to this slave's master
        return self.internal_protocol.node_id

    # hivemind events
    def handle_illegal_msg(self, message: HiveMessage):
        # this should not happen,
        # only sent from client -> server NOT server -> client
        # TODO log, kill connection (?)
        LOG.warning(f"illegal message {message}")

    def handle_hello(self, message: HiveMessage):
        # this check is because other nodes in the hive
        # may also send HELLO with their pubkey
        # only want this on the first connection
        LOG.info(f"HELLO: {message.payload}")
        assert message.msg_type == HiveMessageType.HELLO
        if not self.node_id:
            self.mpubkey = message.payload.get("pubkey")
            node_id = message.payload.get("node_id", "")
            self.internal_protocol.node_id = node_id
            # retained for the Noise prologue (HIVEMIND-CRYPTO-1 §3.4.3)
            self._server_hello_payload = dict(message.payload)
            LOG.info(f"Connected to HiveMind: {node_id}")

    # ------------------------------------------------------- protocol v3 (Noise)
    @property
    def _noise_pin_id(self) -> str:
        """Identifier the server's Noise static key is pinned against.

        Uses the connection endpoint (host:port) so that distinct servers
        that announce the same default node_id do not collide in the pin
        store.
        """
        cfg = getattr(self.hm, "config", None)
        if cfg is not None:
            return f"{cfg.host}:{cfg.port}"
        return self.internal_protocol.node_id or "unknown"

    def _emit(self, message: HiveMessage):
        """Send a protocol frame through the bound client.

        The handshake state machine is shared by both the threading client
        (:class:`~hivemind_bus_client.client.HiveMessageBusClient`, whose
        ``emit`` is synchronous) and the asyncio client
        (:class:`~hivemind_bus_client.async_client.AsyncHiveMessageBusClient`,
        whose ``emit`` is a coroutine). Calling ``self.hm.emit(...)`` directly
        would leave the coroutine un-awaited on the async client, so the frame
        would never reach the wire and the handshake would hang. Detect the
        coroutine and schedule it on the running receive loop; on the sync
        client ``emit`` returns ``None`` and nothing extra happens.
        """
        result = self.hm.emit(message)
        if asyncio.iscoroutine(result):
            asyncio.ensure_future(result)

    def _should_use_noise(self, payload: dict) -> bool:
        """True when both peers are v3-capable and can run the Noise handshake.

        Per HIVEMIND-WIRE-1 §2 both peers operate at the highest version both
        support: the server advertises ``max_protocol_version`` >= 3 together
        with its Noise ``patterns``/``suites``, and this client must support
        protocol v3 (noise primitive available + shared password set). Any
        other combination falls back to the legacy (v2 and below) handshake.
        """
        # The password belongs to this LINK, not to the node. A relay reaches
        # its master with credentials the master issued it, while its own
        # identity carries the keypair it is known by in both directions —
        # reading the password off the identity meant a node whose identity
        # holds no credentials silently skipped the encrypted handshake and
        # spoke plaintext to a server that requires crypto.
        if not NOISE_SUPPORTED or not self.identity or not self.hm.password:
            return False
        if getattr(self.hm, "max_protocol_version", 2) < PROTOCOL_V3:
            return False
        if payload.get("max_protocol_version", 1) < PROTOCOL_V3:
            return False
        noise_params = payload.get("noise")
        if not isinstance(noise_params, dict):
            return False
        pinned = self.identity.get_pinned_noise_key(self._noise_pin_id)
        return select_noise_options(noise_params.get("patterns") or [],
                                    noise_params.get("suites") or [],
                                    pinned) is not None

    def start_noise_handshake(self, server_payload: dict):
        """Send Noise handshake message 1 (HIVEMIND-CRYPTO-1 §3.4.3 step 3).

        Selects one pattern and one suite from the server's advertised lists,
        binds the negotiation into the prologue, and carries the Noise message
        bytes inside the regular HANDSHAKE envelope. The Noise payload carries
        this node's preference-ordered encodings and binarize capability.
        """
        node_id = self.internal_protocol.node_id
        noise_params = server_payload.get("noise") or {}
        pinned = self.identity.get_pinned_noise_key(self._noise_pin_id)
        selection = select_noise_options(noise_params.get("patterns") or [],
                                         noise_params.get("suites") or [],
                                         pinned)
        if selection is None:
            LOG.warning("no mutual Noise pattern/suite, falling back to legacy handshake")
            self._legacy_start_handshake(server_payload)
            return
        pattern, suite = selection
        name = noise_protocol_name(pattern, suite)
        prologue = build_prologue(self._server_hello_payload or {},
                                  server_payload, name)
        LOG.info(f"starting protocol v3 handshake: {name}")
        try:
            self.noise_handshake = start_noise_handshake(
                initiator=True, pattern=pattern, suite=suite,
                password=self.hm.password, node_id=node_id,
                prologue=prologue, key_path=self.identity.noise_key,
                remote_pubkey=pinned)
            self._noise_pattern = pattern
            noise_payload = canonical_json({
                "binarize": self.binarize,
                "encodings": [str(e.value) for e in SupportedEncodings]})
            msg1 = self.noise_handshake.write_message(noise_payload)
        except NoiseHandshakeFailed:
            LOG.exception("failed to start Noise handshake")
            self._abort_noise("failed to initialize Noise handshake")
            return
        self._emit(HiveMessage(HiveMessageType.HANDSHAKE, {
            "noise": {"pattern": pattern, "suite": suite,
                      "msg": msg1.hex()}}))

    def receive_noise_handshake(self, payload: dict):
        """Consume the server's Noise handshake message (§3.4.3 step 4).

        Any authentication failure — wrong password (PSK mismatch), tampered
        negotiation (prologue mismatch), or a static key contradicting the
        pinned key — is fatal: the handshake aborts and the connection is
        rejected. On success the two transport CipherStates take over all
        session traffic and the encrypted HELLO is sent as the first Noise
        transport message.
        """
        try:
            msg = bytes.fromhex(payload["noise"]["msg"])
        except (KeyError, TypeError, ValueError):
            self._abort_noise("malformed Noise handshake envelope")
            return
        try:
            noise_payload = self.noise_handshake.read_message(msg)
            if not self.noise_handshake.handshake_finished:
                # XXpsk2 message 3: our (encrypted) static key + final DH mix
                msg3 = self.noise_handshake.write_message(b"")
                self._emit(HiveMessage(HiveMessageType.HANDSHAKE,
                                         {"noise": {"msg": msg3.hex()}}))
            transport = NoiseTransport(self.noise_handshake)
        except Exception:
            # wrong password / tampered prologue / bad static key -> fatal,
            # fails cryptographically at handshake time (§3.4.3)
            LOG.exception("protocol v3 Noise handshake FAILED "
                          "(wrong password or tampered negotiation)")
            self._drop_stale_pin_after_kk_failure()
            self._abort_noise("Noise handshake authentication failure")
            return

        # TOFU-then-pin the server's static key (§3.4.5)
        pin_id = self._noise_pin_id
        pinned = self.identity.get_pinned_noise_key(pin_id)
        if pinned and transport.remote_static_key != pinned:
            LOG.error(
                f"server Noise static key CHANGED for {pin_id} — refusing "
                "to connect. If you did not reinstall or replace the "
                "master, another machine may be answering at this address. "
                "If you did, the pinned key is stale: run "
                "'hivemind-client forget-server' to drop it and reconnect "
                "to trust the new key.")
            self._abort_noise("pinned key mismatch")
            return
        if not pinned and transport.remote_static_key:
            self.identity.pin_noise_key(pin_id, transport.remote_static_key)

        try:
            server_selection = json.loads(noise_payload.decode("utf-8")) if noise_payload else {}
        except ValueError:
            server_selection = {}
        if server_selection.get("encoding"):
            self.hm.json_encoding = server_selection["encoding"]

        self.hm.noise_transport = transport  # session encryption from here on
        self.noise_handshake = None
        self._noise_established = True
        LOG.info("protocol v3 Noise session established "
                 f"(pattern={self._noise_pattern})")

        # first Noise transport message: session data + site id + pubkey
        sess = Session(self.hm.session_id)
        self._emit(HiveMessage(HiveMessageType.HELLO,
                                 {"pubkey": self.identity.public_key,
                                  "session": sess.serialize(),
                                  "site_id": self.site_id}))
        self.hm.handshake_event.set()

    def _drop_stale_pin_after_kk_failure(self) -> None:
        """Forget the pinned server key when a ``KKpsk0`` handshake fails.

        KK needs each side to hold the other's static key, but the client
        picks it on the strength of having pinned the *server's* key — which
        says nothing about whether the server still holds this node's. The two
        diverge whenever the identity is recreated (reinstall, fresh
        container, new machine on the same credentials) or when one access key
        is used from a second useragent, since the server stores one pin per
        access key.

        Without this the node is locked out for good: it retries KK, fails,
        and retries KK again, every few seconds, forever. Dropping the pin
        makes the next attempt an ``XXpsk2`` handshake, which re-establishes
        trust and re-pins.

        This is not a downgrade an attacker can profit from. Both patterns
        carry the password-derived PSK, so whoever answers still has to know
        the password; failing KK on purpose only moves them to a handshake
        they equally cannot complete. The genuine MITM signal is a *completed*
        handshake whose static key contradicts the pin, and that path is
        untouched below — it still refuses and keeps the pin.
        """
        if self._noise_pattern != NOISE_PATTERN_KK:
            return
        pin_id = self._noise_pin_id
        if not self.identity.get_pinned_noise_key(pin_id):
            return
        LOG.warning(
            f"KKpsk0 failed for {pin_id} and a pinned server key is present. "
            "The master no longer holds this node's static key — most likely "
            "this identity was recreated, or these credentials are in use "
            "from another node. Dropping the pin and retrying with XXpsk2.")
        self.identity.forget_noise_key(pin_id)

    def _abort_noise(self, reason: str):
        """Fatal handshake failure — reject the connection (§3.4.3).

        The connection is dropped, but the client keeps reconnecting. Every
        abort reason here can be transient or repairable from the other
        side: a rotated password, a truncated handshake envelope, a master
        that was reinstalled. A permanent stop would leave the node dead
        until somebody edits its identity file by hand, and the failure
        would be logged once and then never again. Retrying re-reports the
        problem on every attempt and recovers by itself once the cause is
        fixed. Refusing the connection is what protects the session, not
        giving up on it.
        """
        LOG.error(f"aborting protocol v3 connection: {reason}")
        self.noise_handshake = None
        self.hm.noise_transport = None
        try:
            result = self.hm.close_connection()
            if asyncio.iscoroutine(result):
                asyncio.ensure_future(result)
        except Exception:
            LOG.exception("failed to close the aborted connection")

    def start_handshake(self):
        # negotiated protocol v3 -> the Noise handshake replaces the legacy
        # password/pubkey handshake
        if self.noise_handshake is not None:
            return  # Noise handshake already in flight, keep waiting
        if self._server_handshake_payload and self._should_use_noise(self._server_handshake_payload):
            self.start_noise_handshake(self._server_handshake_payload)
            return
        self._legacy_start_handshake(self._server_handshake_payload or {})

    def _legacy_start_handshake(self, server_payload: dict):
        if self.binarize:
            LOG.info("hivemind supports binarization protocol")
        else:
            LOG.info("hivemind does not support binarization protocol")

        payload = {"binarize": self.binarize,
                   "encodings": list(SupportedEncodings),
                   "ciphers": optimal_ciphers()}
        if self.pswd_handshake is not None:
            payload["envelope"] = self.pswd_handshake.generate_handshake()
        else:
            payload["pubkey"] = self.handshake.pubkey

        self._emit(HiveMessage(HiveMessageType.HANDSHAKE, payload))

    def receive_handshake(self, envelope):
        if self.pswd_handshake is not None:
            LOG.info("Received password envelope")
            self.pswd_handshake.receive_and_verify(envelope)  # validate master password matched
            self.hm.crypto_key = self.pswd_handshake.secret  # update to new crypto key
        else:
            LOG.info("Received pubkey envelope")
            # if we have a pubkey let's verify the master node is who it claims to be
            # currently this is sent in HELLO, but advance use cases can read it from somewhere else
            if self.mpubkey:
                # authenticates the server to the client
                self.handshake.receive_and_verify(envelope, self.mpubkey)
            else:
                # implicitly trust the server
                self.handshake.receive_handshake(envelope)
            self.hm.crypto_key = self.handshake.secret  # update to new crypto key

        # now that communication is secure, send our Session data and other personal info
        sess = Session(self.hm.session_id)
        msg = HiveMessage(HiveMessageType.HELLO, {"pubkey": self.identity.public_key,
                                                  "session": sess.serialize(),
                                                  "site_id": self.site_id})
        self._emit(msg)
        self.hm.handshake_event.set()

    def handle_handshake(self, message: HiveMessage):
        LOG.info(f"HANDSHAKE: {message.payload}")
        assert message.msg_type == HiveMessageType.HANDSHAKE
        # protocol v3: server's Noise handshake message.
        # Only a Noise handshake MESSAGE carries noise.msg -- the server's
        # OFFER carries noise.patterns/noise.suites. Dispatching on the mere
        # presence of "noise" fed a repeated offer to
        # receive_noise_handshake(), which aborted the handshake as a
        # malformed envelope and discarded the genuine reply behind it.
        # A transport may legitimately redeliver the offer: the HTTP
        # protocol queues per access key and re-sends HELLO + offer from
        # handle_new_client() whenever /connect finds no cached connection,
        # so a failed attempt leaves an offer queued for the next one.
        noise = message.payload.get("noise")
        if isinstance(noise, dict) and self.noise_handshake is not None:
            if "msg" not in noise:
                LOG.debug("ignoring a repeated HANDSHAKE offer received "
                          "while the Noise handshake is in flight")
                return
            self.receive_noise_handshake(message.payload)
            return
        # master is performing the handshake
        if "envelope" in message.payload:
            envelope = message.payload["envelope"]
            self.hm.json_encoding = message.payload.get("encoding") or SupportedEncodings.JSON_HEX
            self.hm.cipher = message.payload.get("cipher") or SupportedCiphers.AES_GCM
            self.receive_handshake(envelope)
            LOG.debug(f"Encoding: {self.hm.json_encoding}")
            LOG.debug(f"Cipher: {self.hm.cipher}")
            active_handshake = self.pswd_handshake or self.handshake
            LOG.debug(f"Key size: {len(active_handshake.secret) * 8}bit")

        # master is requesting handshake start
        else:
            # required = message.payload.get("handshake")
            # if not required:
            #    self.hm.handshake_event.set()  # don't wait
            #    return

            encodings = message.payload.get("encodings") or [SupportedEncodings.JSON_HEX]
            ciphers = message.payload.get("ciphers") or [SupportedCiphers.AES_GCM]
            LOG.debug(f"Server supported encodings: {encodings}")
            LOG.debug(f"Server supported ciphers: {ciphers}")
            if message.payload.get("crypto_key") and self.hm.crypto_key:
                pass
                # we can use the pre-shared key instead of handshake
                # TODO - flag to give preference to pre-shared key over handshake

            self.binarize = message.payload.get("binarize", False)

            # retained for the Noise prologue + handshake retries
            self._server_handshake_payload = dict(message.payload)

            # protocol v3 negotiation (HIVEMIND-WIRE-1 §2): both peers
            # v3-capable -> Noise handshake; otherwise fall through to the
            # legacy (v2) handshake below, unchanged
            if self._should_use_noise(self._server_handshake_payload):
                self.start_noise_handshake(self._server_handshake_payload)
                return

            # TODO - flag to give preference to / require password or use RSA handshake
            # currently if password is set then it is always used
            if message.payload.get("password") and self.hm.password:
                self.pswd_handshake = PasswordHandShake(self.hm.password,
                                                        min_bits=_pw_min_bits())
                self._legacy_start_handshake(self._server_handshake_payload)

    def _is_source_trusted(self, message: HiveMessage) -> bool:
        """Check if the message originates from a trusted peer.

        Checks the message's source_peer against the HiveMapper's trust
        data (populated from ``NodeIdentity.trusted_keys`` via
        ``HiveMapper.mark_trusted_nodes``).  Also checks the identity's
        trusted keys directly against any public key in the route.

        Args:
            message: The HiveMessage to check.

        Returns:
            True if the source peer is trusted.
        """
        # check HiveMapper trust data first (fast path)
        if self.hive_mapper and message.source_peer:
            if self.hive_mapper.is_peer_trusted(message.source_peer):
                return True
        # fallback: check route for any known trusted public key
        if self.identity:
            for hop in message.route:
                source = hop.get("source", "")
                if self.hive_mapper:
                    node = self.hive_mapper.nodes.get(source)
                    if node and node.public_key and self.identity.is_trusted_key(node.public_key):
                        return True
        return False

    def _verified_origin(self, envelope: dict,
                         message_source: Optional[str] = None) -> Optional[str]:
        """Whether *envelope*'s signature verifies against a trusted key.

        CRYPTO-1 §5 requires the origin signature on an INTERCOM to verify.
        hivemind-core enforces this; this side generated the signature and
        never checked one, so knowing a node's public key — which is the
        addressing mechanism, published in every PING answer — was enough to
        forge a message to it. The trust store's own contract already says
        otherwise: "Only messages from peers whose public key is in this
        mapping will be accepted for bus injection."

        Being addressed to us is not evidence of origin: the sender chooses
        the address. Only the signature is.
        """
        signature = envelope.get("signature")
        ciphertext = envelope.get("ciphertext")
        if not signature or not ciphertext:
            LOG.warning("dropping INTERCOM with no origin signature")
            return None
        if not self.identity:
            return None
        try:
            raw_sig = pybase64.b64decode(signature)
            raw_ciphertext = pybase64.b64decode(ciphertext)
        except Exception:
            LOG.warning("dropping INTERCOM with an undecodable signature")
            return None
        # The master this node is attached to is an anchor by construction:
        # the connection authenticated to it and it announced this key in
        # HELLO. Without it, a default deployment — which has an empty
        # trusted_keys — could not receive mail from its own master, and
        # nothing in the library or the CLI populates that store.
        candidates = dict(self.identity.trusted_keys)
        if self.mpubkey:
            candidates.setdefault("__master__", self.mpubkey)

        # `source_peer` is deliberately NOT used to narrow this. Relays
        # rewrite it hop by hop, so on arrival it names the last relay rather
        # than the origin — binding to it rejects every relayed frame while
        # proving nothing. What the claim cannot do is mislead: the key that
        # actually verified is stamped into the delivered message's context
        # (see ``_deliver_verified`` / ``VERIFIED_SOURCE_PEER_KEY``), so a
        # consumer reading that field sees the authenticated origin, not the
        # asserted one.

        for pub in candidates.values():
            try:
                if verify_RSA(pub, raw_ciphertext, raw_sig):
                    return pub
            except Exception:
                continue
        LOG.warning("dropping INTERCOM whose signature matches no trusted key")
        return None

    def _deliver_verified(self, message: HiveMessage, verified_key: str) -> None:
        """Inject the payload, recording who actually signed it.

        The envelope binds no origin identity, so ``source_peer`` is whatever
        the sender wrote — one trusted peer can put another's label on a frame
        it signed itself. What actually reaches the consumer is not this
        outer INTERCOM object (it is discarded once unwrapped) but the inner
        BUS message's ``payload`` — a plain ``ovos_bus_client.Message`` handed
        to ``bus.emit``. Stamping ``source_peer`` here, on the object nobody
        reads, would be inert. The verified key is instead written into that
        inner message's context under ``VERIFIED_SOURCE_PEER_KEY``, a name
        chosen not to collide with ordinary bus context (``source``,
        ``destination``, session fields, ...), so a consumer that wants the
        authenticated origin has somewhere real to read it from.
        """
        message.update_source_peer(verified_key)
        self.handle_bus(message.payload, verified_source=verified_key)

    def handle_bus(self, message: HiveMessage, verified_source: Optional[str] = None):
        """Dispatch event to the agent protocol bus.

        Args:
            message: The BUS HiveMessage to inject.
            verified_source: The public key THIS node itself verified as the
                signer of this delivery, if any. Only ``_deliver_verified``
                (the INTERCOM path) passes this. Every other path into this
                method — plain master BUS, PROPAGATE-wrapped BUS — leaves it
                None, because nothing on those paths verifies a signature.

        ``handle_bus`` is the common sink for every bus-injection path, not
        just INTERCOM. ``VERIFIED_SOURCE_PEER_KEY`` in the outgoing context
        is only meaningful if it is impossible to forge, so it is handled
        here, once, for all callers: any inbound value is unconditionally
        stripped first, then set — if and only if ``verified_source`` was
        passed — from what THIS node verified this delivery. A sender that
        pre-sets the key on a plain BUS or PROPAGATE frame (paths with no
        signature check at all) must not be able to make it survive to the
        consumer.
        """
        assert message.msg_type == HiveMessageType.BUS
        assert isinstance(message.payload, MycroftMessage)
        LOG.info(f"BUS: {message.payload.msg_type}")
        # master wants to inject message into mycroft bus
        pload = message.payload

        # No inbound frame, on any path, may carry a pre-set verified-origin
        # claim - strip it unconditionally before possibly re-setting it below.
        pload.context.pop(VERIFIED_SOURCE_PEER_KEY, None)
        if verified_source is not None:
            pload.context[VERIFIED_SOURCE_PEER_KEY] = verified_source

        # update session sent from hivemind-core
        sess = Session.from_message(pload)
        if sess.session_id == self.hm.session_id:
            sess.site_id = self.site_id  # do not allow overwriting site_id
        SessionManager.update(sess)

        # from this point on, it should be a native source and execute audio
        if "destination" in pload.context:
            pload.context["source"] = pload.context.pop("destination")
        self.internal_protocol.bus.emit(pload)

    def handle_broadcast(self, message: HiveMessage):
        """Handle a BROADCAST message received from the master.

        By definition all messages from masters are trusted

        Args:
            message: The BROADCAST HiveMessage.
        """
        LOG.info(f"BROADCAST: {message.payload}")
        if message.msg_type != HiveMessageType.BROADCAST:
            LOG.error(f"not a BROADCAST message, dropping: {message.msg_type}")
            return

        inner = getattr(message.payload, "msg_type", None)
        if inner == HiveMessageType.INTERCOM:
            self.handle_intercom(message.payload)
        elif inner == HiveMessageType.BUS:
            # if the message targets our site_id, send it to internal bus
            site = message.target_site_id
            if site and site == self.site_id:
                # always trusted, comes from a master, never from other satellite
                self.handle_bus(message.payload)
        else:
            # MSG-1 §3: a node MUST forward or ignore a payload type it does
            # not handle. It MUST NOT reject the connection, and it must not
            # crash the handler either. An assert here also disappeared under
            # "python -O".
            LOG.debug(f"ignoring BROADCAST with unhandled payload type: {inner}")

    def handle_propagate(self, message: HiveMessage):
        """Handle a PROPAGATE message (bidirectional flood).

        BUS payloads are only injected if the source peer is trusted
        (via ``NodeIdentity.trusted_keys``).  Untrusted BUS payloads
        are silently dropped to prevent cross-satellite injection.

        Args:
            message: The PROPAGATE HiveMessage.
        """
        LOG.info(f"PROPAGATE: {message.payload}")
        if message.msg_type != HiveMessageType.PROPAGATE:
            LOG.error(f"not a PROPAGATE message, dropping: {message.msg_type}")
            return

        inner = getattr(message.payload, "msg_type", None)
        if inner == HiveMessageType.INTERCOM:
            self.handle_intercom(message.payload)
        elif inner == HiveMessageType.PING:
            self.handle_ping(message.payload)
        elif inner == HiveMessageType.BUS:
            site = message.target_site_id
            if site and site == self.site_id:
                if self._is_source_trusted(message):
                    self.handle_bus(message.payload)
                else:
                    LOG.warning(f"Dropping untrusted PROPAGATE(BUS) from {message.source_peer}")
        else:
            # MSG-1 §3: a node MUST forward or ignore a payload type it does
            # not handle. It MUST NOT reject the connection, and it must not
            # crash the handler either. An assert here also disappeared under
            # "python -O".
            LOG.debug(f"ignoring PROPAGATE with unhandled payload type: {inner}")

    def handle_ping(self, message: HiveMessage):
        """Handle a received PROPAGATE(PING) using flood-based discovery.

        If this ``flood_id`` has not been seen before, builds and sends
        this node's own responsive PING (same ``flood_id``) upstream.

        Args:
            message: The outer PROPAGATE HiveMessage whose inner payload is a PING.
        """
        assert message.msg_type == HiveMessageType.PING
        ping_payload = message.payload if isinstance(message.payload, dict) else {}

        flood_id = ping_payload.get("flood_id", "")

        # Lazy init for tests / callers that don't set hive_mapper
        if self.hive_mapper is None:
            self.hive_mapper = HiveMapper()

        # Flood-loop prevention — delegated to HiveMapper
        if self.hive_mapper.check_flood_id(flood_id):
            return

        # Build our own responsive PING with the same flood_id.
        #
        # Two fields, two jobs. ``peer`` is the connection-level label the
        # upstream master already knows this connection by, and is what the
        # HiveMapper keys its node table on. ``public_key`` is the node's
        # stable identity — it survives reconnects and is how the mesh
        # addresses a node end-to-end (INTERCOM ``target_public_key``, and
        # the loop-detection ``_node_id`` in hivemind-core). Every
        # responsive PING carries both, on this side and on the listener
        # side, so a consumer can always tell two nodes apart without
        # relying on the per-connection label.
        peer = f"{self.identity.name or 'satellite'}::{self.hm.session_id}"
        # announce lang from active session if available
        sess = SessionManager.get_default_session()
        lang = getattr(sess, "lang", None) if sess else None

        own_ping_payload = {
            "flood_id": flood_id,
            "peer": peer,
            "site_id": self.site_id,
            "timestamp": time.time(),
            "public_key": self.identity.public_key,
            "lang": lang,
        }
        own_ping_inner = HiveMessage(HiveMessageType.PING, own_ping_payload)
        own_ping_outer = HiveMessage(HiveMessageType.PROPAGATE, payload=own_ping_inner)

        LOG.debug(f"Sending responsive PING for flood_id={flood_id}")
        self._emit(own_ping_outer)

    @staticmethod
    def _is_stream_end(message: HiveMessage) -> bool:
        """True if this QUERY/CASCADE response is the end-of-stream control
        message (a wrapped BUS payload of type ``QUERY_STREAM_END``)."""
        inner = message.payload
        return (inner.msg_type == HiveMessageType.BUS
                and getattr(inner.payload, "msg_type", "") == QUERY_STREAM_END)

    def arm_query_timeout(self) -> None:
        """Start the originator's liveness bound for a QUERY just sent upstream.

        HIVEMIND-NODE-1 §5.5 requires the originator to carry this bound
        itself; intermediate declines are silent, so nothing else can tell it
        the query was lost.
        """
        if self.query_timeout <= 0:
            return
        if self.query_liveness is None:
            self.query_liveness = QueryLivenessTimer(self.query_timeout,
                                                     self.handle_query_timeout)
        self.query_liveness.arm()

    def handle_query_timeout(self, query_id: str) -> None:
        """Report a query that produced nothing within ``query_timeout``.

        The failure is announced with the same ``hive.query.timeout`` payload
        a top-of-chain node would have sent (HIVEMIND-AGENT-1 §5), so a
        consumer has one code path for "nobody answered" whether the mesh
        said so or the wait simply ran out.
        """
        LOG.warning(f"no QUERY response within {self.query_timeout}s, "
                    f"treating the query as failed: query_id={query_id}")
        self.internal_protocol.bus.emit(
            Message("hive.query.timeout",
                    {"query_id": query_id, "error": "no_answer"}))

    def handle_query(self, message: HiveMessage):
        """Handle a QUERY message received from the master.

        By definition all responses from masters are trusted

        Args:
            message: The QUERY HiveMessage.
        """
        LOG.info(f"QUERY: {message.payload}")
        assert message.msg_type == HiveMessageType.QUERY
        query_id = (message.metadata or {}).get("query_id", "")
        if self._is_stream_end(message):
            LOG.debug(f"QUERY stream complete: {query_id}")
            if self.query_liveness is not None:
                self.query_liveness.cancel()
            return
        assert message.payload.msg_type in [HiveMessageType.BUS, HiveMessageType.INTERCOM]
        if message.payload.msg_type == HiveMessageType.INTERCOM:
            # using INTERCOM allows end2end privacy, nodes along the chain can't read responses
            self.handle_intercom(message)
        elif message.payload.msg_type == HiveMessageType.BUS:
            if self.query_liveness is not None:
                if message.payload.payload.msg_type == "hive.query.timeout":
                    self.query_liveness.cancel()
                else:
                    # a chunk arrived, so the answer is still being produced
                    self.query_liveness.arm(query_id)
            # each streamed chunk wraps an inner BUS speak; emit that, not the wrapper
            self.handle_bus(message.payload)

    def handle_cascade(self, message: HiveMessage):
        """Handle a CASCADE response received from the master.

        Unlike QUERY we may receive untrusted responses from all across the hive

        Responses are buffered in a ``CascadeAggregator``. After
        ``cascade_timeout`` seconds the ``cascade_select_callback``
        picks the best response and it is emitted on the internal bus.

        Args:
            message: The CASCADE HiveMessage.
        """
        LOG.info(f"CASCADE: {message.payload}")
        assert message.msg_type == HiveMessageType.CASCADE
        if self._is_stream_end(message):
            # a responder finished streaming; not an answer to aggregate
            LOG.debug("CASCADE stream complete from a responder: "
                      f"{(message.metadata or {}).get('query_id')}")
            return
        assert message.payload.msg_type == HiveMessageType.BUS

        # using INTERCOM allows end2end privacy, nodes along the chain can't read responses
        if self.cascade_aggregator is None or self.cascade_aggregator._resolved:
            select_cb = self.cascade_select_callback or random.choice
            expected = len(self.hive_mapper.nodes) if self.hive_mapper else None
            self.cascade_aggregator = CascadeAggregator(
                timeout=self.cascade_timeout,
                select_callback=select_cb,
                emit_callback=lambda m: self.handle_bus(m.payload),
                expected_responses=expected,
            )
        self.cascade_aggregator.add_response(message)

    def handle_intercom(self, message: HiveMessage):
        """Handle an INTERCOM (end-to-end encrypted) message.

        Decrypts the payload if it contains a ciphertext envelope,
        then injects the inner BUS message only if:
        - The message is explicitly targeted at us (target_public_key matches), OR
        - The source peer is in the trusted keys list.

        Untrusted, non-targeted INTERCOM messages are silently dropped.

        Args:
            message: The INTERCOM HiveMessage.
        """
        LOG.info(f"INTERCOM: {message.payload}")
        assert message.msg_type == HiveMessageType.INTERCOM
        # NOT "assert message.payload.msg_type == BUS" here. The payload of an
        # encrypted INTERCOM is the encryption envelope — a plain dict with no
        # msg_type — and that is the normal case, the one INTERCOM exists for.
        # Asserting the decrypted shape before decrypting raised
        # AttributeError: 'dict' object has no attribute 'msg_type' and killed
        # the handler on every encrypted frame. The inner type is checked below,
        # after decryption, where the value actually exists. An assert would be
        # the wrong tool anyway: it disappears under `python -O`.

        k = message.target_public_key
        if k and k != self.identity.public_key:
            # explicitly targeted at someone else — not for us
            return False

        pload = message.payload
        envelope = pload if isinstance(pload, dict) else {}
        if isinstance(pload, dict) and "encrypted_key" in pload:
            # hybrid encryption envelope (AES key RSA-encrypted)
            try:
                private_key = load_RSA_key(self.identity.private_key)
                decrypted = hybrid_decrypt(private_key, pload).decode("utf-8")
                message._payload = HiveMessage.deserialize(decrypted)
            except Exception:
                if k:
                    LOG.error("failed to decrypt INTERCOM message!")
                else:
                    LOG.debug("failed to decrypt INTERCOM, not for us")
                return False

        # the check the premature assert was trying to make, now that the
        # value exists: after decryption the inner payload must be a BUS
        # envelope, because injecting anything else onto the internal bus is
        # not something this handler knows how to do.
        inner = getattr(message.payload, "msg_type", None)
        if inner != HiveMessageType.BUS:
            LOG.warning(f"dropping INTERCOM whose inner payload is {inner}, "
                        f"expected {HiveMessageType.BUS}")
            return False

        # Deliver the DECRYPTED inner BUS envelope, not the outer INTERCOM.
        # handle_bus asserts its argument is a BUS message, so passing the
        # outer frame raised AssertionError from inside the handler — after a
        # successful decrypt, on exactly the messages that were meant to work.
        # Addressed to us, and the origin signature verifies against a key we
        # trust. Both halves are required: the address is chosen by the sender.
        verified = self._verified_origin(envelope, message.source_peer)
        if k and k == self.identity.public_key and verified:
            self._deliver_verified(message, verified)
            return True

        # not targeted — only inject if source is trusted
        if self._is_source_trusted(message) and verified:
            self._deliver_verified(message, verified)
            return True

        LOG.warning(f"Dropping untrusted INTERCOM from {message.source_peer}")
        return False
