"""Hive topology mapper built from PING-only network discovery.

Every node responds to a PING by propagating its own PING (with the same
``flood_id``), so all nodes in the hive sync simultaneously.  An ephemeral
``flood_id`` prevents infinite loops.
"""
import json
import time
from collections import OrderedDict
from dataclasses import dataclass, field
from typing import Dict, List, Optional, Set

from hivemind_bus_client.message import HiveMessage


@dataclass
class NodeInfo:
    """Metadata about a node discovered via PING flood."""

    peer: str
    site_id: Optional[str] = None
    timestamp: Optional[float] = None       # sender's clock when they created the PING
    received_at: Optional[float] = None     # our local clock when we received it
    public_key: Optional[str] = None        # RSA public key if provided in PING
    lang: Optional[str] = None             # locale announced by the node (e.g. "en-us")
    trusted: bool = False                   # whether this peer's key is in the trusted list

    @property
    def latency_ms(self) -> Optional[float]:
        """Estimated one-way latency in milliseconds (receiver clock minus sender clock).

        This is a clock-difference estimate, **not** a true round-trip
        measurement.  On unsynchronised clocks it may be negative or
        inaccurate.  Returns None if either timestamp is unavailable.
        """
        if self.received_at is not None and self.timestamp is not None:
            return (self.received_at - self.timestamp) * 1000
        return None


class HiveMapper:
    """Collect responsive PINGs from a flood and build a directed hive topology graph.

    Usage::

        mapper = HiveMapper()
        mapper.start_ping(flood_id)
        # ... feed each received inner PING HiveMessage ...
        mapper.on_ping(ping_msg)
        print(mapper.to_ascii(root_peer="my-node::session1"))
    """

    def __init__(self) -> None:
        # peer → NodeInfo for every node that responded
        self.nodes: Dict[str, NodeInfo] = {}
        # source peer → set of target peers (directed edges from route records)
        self.edges: Dict[str, Set[str]] = {}
        # flood_id → set of peer IDs that already sent a PING (deduplication)
        self._seen_pings: Dict[str, Set[str]] = {}
        # flood_id → timestamp for flood-loop prevention (FIFO eviction by age)
        self._seen_flood_ids: OrderedDict[str, float] = OrderedDict()

    def start_ping(self, flood_id: str) -> None:
        """Register a new PING session, clearing stale deduplication state for that ID.

        Args:
            flood_id: UUID string from the PING payload.
        """
        self._seen_pings[flood_id] = set()

    def on_ping(self, message: HiveMessage, received_at: Optional[float] = None) -> bool:
        """Ingest a received PING HiveMessage and update the topology graph.

        The route on *message* must already contain the hop history transferred
        from the outer PROPAGATE wrapper (done by ``_unpack_message`` in the server
        protocol before this method is called).

        Args:
            message: Inner PING HiveMessage with ``msg_type == HiveMessageType.PING``.
            received_at: Local clock timestamp when the PING was received.

        Returns:
            True if the PING was new and the graph was updated; False if duplicate.
        """
        payload = message.payload
        if not isinstance(payload, dict):
            return False

        flood_id = payload.get("flood_id", "")
        peer = payload.get("peer", "")

        if not flood_id or not peer:
            return False

        seen = self._seen_pings.setdefault(flood_id, set())
        if peer in seen:
            return False
        seen.add(peer)

        self.nodes[peer] = NodeInfo(
            peer=peer,
            site_id=payload.get("site_id"),
            timestamp=payload.get("timestamp"),
            received_at=received_at,
            public_key=payload.get("public_key"),
            lang=payload.get("lang"),
        )

        for hop in message.route:
            if not isinstance(hop, dict):
                continue
            source = hop.get("source", "")
            targets = hop.get("targets") or []
            if source:
                edge_set = self.edges.setdefault(source, set())
                for t in targets:
                    if t:
                        edge_set.add(t)

        return True

    def mark_trusted_nodes(self, trusted_keys: Dict[str, str]) -> None:
        """Mark nodes whose public key is in the trusted keys mapping.

        Should be called after PING discovery completes to update each
        ``NodeInfo.trusted`` flag based on the identity's trusted keys.

        Args:
            trusted_keys: Alias → public key mapping from ``NodeIdentity.trusted_keys``.
        """
        trusted_values = set(trusted_keys.values())
        for node in self.nodes.values():
            node.trusted = node.public_key is not None and node.public_key in trusted_values

    def is_peer_trusted(self, peer: str) -> bool:
        """Check if a peer is trusted based on prior PING discovery.

        Args:
            peer: The peer identifier string.

        Returns:
            True if the peer was discovered and marked as trusted.
        """
        node = self.nodes.get(peer)
        return node is not None and node.trusted

    def to_dict(self) -> dict:
        """Return a JSON-serialisable snapshot of the current topology.

        Returns:
            dict with keys ``nodes`` (list of node dicts) and ``edges``
            (list of ``{source, target}`` dicts).
        """
        nodes = [
            {
                "peer": n.peer,
                "site_id": n.site_id,
                "timestamp": n.timestamp,
                "latency_ms": n.latency_ms,
                "public_key": n.public_key,
                "lang": n.lang,
                "trusted": n.trusted,
            }
            for n in self.nodes.values()
        ]
        edges = [
            {"source": src, "target": tgt}
            for src, targets in self.edges.items()
            for tgt in targets
        ]
        return {"nodes": nodes, "edges": edges}

    def to_json(self) -> str:
        """Return ``to_dict()`` as a formatted JSON string."""
        return json.dumps(self.to_dict(), indent=2)

    def to_ascii(self, root_peer: Optional[str] = None) -> str:
        """Render the hive topology as a human-readable ASCII tree.

        PING routes flow *toward* the originator, so edges are stored as
        ``relayer → originator``.  When ``root_peer`` (the local node / PING
        originator) is supplied the edge direction is inverted for display so
        that the tree reads top-down from the originator outward to leaf nodes.

        Args:
            root_peer: Peer ID of the local node, labeled ``[self]`` at the
                tree root.  Omit to display the raw edge directions.

        Returns:
            Multi-line string representing the topology.
        """
        if not self.nodes and not self.edges:
            return "[No nodes discovered]"

        lines: List[str] = []

        if root_peer is not None:
            # Build display children as the *inverse* of stored edges:
            # stored: relayer → originator  →  display: originator ← relayer
            display_children: Dict[str, List[str]] = {}
            for src, targets in self.edges.items():
                for t in targets:
                    display_children.setdefault(t, [])
                    if src not in display_children[t]:
                        display_children[t].append(src)

            def _render_inv(peer: str, prefix: str, is_last: bool) -> None:
                connector = "└── " if is_last else "├── "
                node = self.nodes.get(peer)
                site = f"  site={node.site_id}" if node and node.site_id else ""
                lat = (f"  latency={node.latency_ms:.0f}ms"
                       if node and node.latency_ms is not None else "")
                lines.append(f"{prefix}{connector}{peer}{site}{lat}")
                kids = display_children.get(peer, [])
                child_prefix = prefix + ("    " if is_last else "│   ")
                for i, kid in enumerate(kids):
                    _render_inv(kid, child_prefix, i == len(kids) - 1)

            node = self.nodes.get(root_peer)
            site = f"  site={node.site_id}" if node and node.site_id else ""
            lines.append(f"[self] {root_peer}{site}")
            kids = display_children.get(root_peer, [])
            for i, kid in enumerate(kids):
                _render_inv(kid, "", i == len(kids) - 1)

        else:
            # Raw display following stored edge direction (relayer → originator)
            children: Dict[str, List[str]] = {}
            all_targets: Set[str] = set()
            for src, targets in self.edges.items():
                children.setdefault(src, [])
                for t in targets:
                    children[src].append(t)
                    all_targets.add(t)

            candidate_roots = [p for p in self.edges if p not in all_targets]
            display_roots = candidate_roots or list(self.edges.keys())[:1]

            def _render(peer: str, prefix: str, is_last: bool) -> None:
                connector = "└── " if is_last else "├── "
                node = self.nodes.get(peer)
                site = f"  site={node.site_id}" if node and node.site_id else ""
                lat = (f"  latency={node.latency_ms:.0f}ms"
                       if node and node.latency_ms is not None else "")
                lines.append(f"{prefix}{connector}{peer}{site}{lat}")
                kids = children.get(peer, [])
                child_prefix = prefix + ("    " if is_last else "│   ")
                for i, kid in enumerate(kids):
                    _render(kid, child_prefix, i == len(kids) - 1)

            for root in display_roots:
                node = self.nodes.get(root)
                site = f"  site={node.site_id}" if node and node.site_id else ""
                lines.append(f"{root}{site}")
                kids = children.get(root, [])
                for i, kid in enumerate(kids):
                    _render(kid, "", i == len(kids) - 1)

        return "\n".join(lines) if lines else "[No topology data]"

    def check_flood_id(self, flood_id: str, max_size: int = 1000) -> bool:
        """Check whether *flood_id* has been seen before, and register it.

        Used for flood-loop prevention: the first call for a given
        ``flood_id`` returns ``False`` (not seen), subsequent calls
        return ``True``.  When the cache exceeds *max_size* the oldest
        entries (by insertion time) are evicted first (FIFO).

        Args:
            flood_id: The flood identifier to check.
            max_size: Maximum number of flood IDs to retain.

        Returns:
            ``True`` if the flood_id was already seen, ``False`` otherwise.
        """
        if not flood_id:
            return True  # empty flood_id is always "seen" (rejected)
        if flood_id in self._seen_flood_ids:
            return True
        # evict oldest entries when cache is full
        while len(self._seen_flood_ids) >= max_size:
            self._seen_flood_ids.popitem(last=False)  # FIFO — remove oldest
        self._seen_flood_ids[flood_id] = time.time()
        return False

    def clear(self) -> None:
        """Reset the mapper to an empty state."""
        self.nodes.clear()
        self.edges.clear()
        self._seen_pings.clear()
        self._seen_flood_ids.clear()
