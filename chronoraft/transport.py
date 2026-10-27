"""Simulated network bus with programmable Chaos Engineering harness."""
from __future__ import annotations

import collections
import random
import threading
import time
from typing import Any, Callable, Dict, List, Optional, Set, Tuple

from chronoraft.models import (
    AppendEntriesArgs,
    AppendEntriesReply,
    ClusterEvent,
    RequestVoteArgs,
    RequestVoteReply,
)


class ChaosTransport:
    """High-performance virtual network with partition matrix, drop rates, and real-time telemetry."""

    def __init__(self, max_event_history: int = 500) -> None:
        self._lock = threading.RLock()
        self._nodes: Dict[str, Any] = {}  # node_id -> RaftNode instance

        # Chaos conditions
        self._partitions: List[Set[str]] = []  # List of disjoint sets of reachable nodes
        self._isolated_nodes: Set[str] = set()
        self._drop_rate: float = 0.0
        self._min_latency_ms: float = 0.0
        self._max_latency_ms: float = 0.0

        # Telemetry event stream for HUD and TUI
        self._events: collections.deque[Dict[str, Any]] = collections.deque(maxlen=max_event_history)
        self._rpc_count: int = 0
        self._dropped_count: int = 0

    def register_node(self, node_id: str, node_instance: Any) -> None:
        with self._lock:
            self._nodes[node_id] = node_instance

    def unregister_node(self, node_id: str) -> None:
        with self._lock:
            self._nodes.pop(node_id, None)

    # --- Chaos Injection Controls ---

    def partition(self, groups: List[List[str]]) -> None:
        """Divides cluster into isolated network groups.
        Nodes within the same group can talk; across groups, all packets drop.
        """
        with self._lock:
            self._partitions = [set(g) for g in groups]
            self.emit_event("CHAOS_PARTITION", "NETWORK", {
                "groups": [list(g) for g in self._partitions]
            })

    def isolate(self, node_id: str) -> None:
        """Completely disconnects a single node from the entire network."""
        with self._lock:
            self._isolated_nodes.add(node_id)
            self.emit_event("CHAOS_ISOLATE", node_id, {"isolated": True})

    def unisolate(self, node_id: str) -> None:
        with self._lock:
            self._isolated_nodes.discard(node_id)
            self.emit_event("CHAOS_HEAL", node_id, {"isolated": False})

    def heal(self) -> None:
        """Restores complete full-mesh connectivity across all nodes."""
        with self._lock:
            self._partitions.clear()
            self._isolated_nodes.clear()
            self._drop_rate = 0.0
            self._min_latency_ms = 0.0
            self._max_latency_ms = 0.0
            self.emit_event("CHAOS_HEAL_ALL", "NETWORK", {"status": "fully_connected"})

    def set_drop_rate(self, rate: float) -> None:
        with self._lock:
            self._drop_rate = max(0.0, min(1.0, float(rate)))
            self.emit_event("CHAOS_DROP_RATE", "NETWORK", {"drop_rate": self._drop_rate})

    def set_latency(self, min_ms: float, max_ms: float) -> None:
        with self._lock:
            self._min_latency_ms = max(0.0, float(min_ms))
            self._max_latency_ms = max(self._min_latency_ms, float(max_ms))
            self.emit_event("CHAOS_LATENCY", "NETWORK", {
                "min_ms": self._min_latency_ms,
                "max_ms": self._max_latency_ms
            })

    def can_communicate(self, from_id: str, to_id: str) -> bool:
        """Evaluates partition and isolation rules."""
        with self._lock:
            if from_id == to_id:
                return True
            if from_id in self._isolated_nodes or to_id in self._isolated_nodes:
                return False

            if self._partitions:
                # Find group for from_id and to_id
                from_group = None
                to_group = None
                for g in self._partitions:
                    if from_id in g:
                        from_group = g
                    if to_id in g:
                        to_group = g

                # If either is in a defined group, they must be in the exact same group
                if from_group is not None or to_group is not None:
                    if from_group is not to_group:
                        return False

            return True

    # --- RPC Dispatch ---

    def send_request_vote(self, from_id: str, to_id: str, args: RequestVoteArgs) -> Optional[RequestVoteReply]:
        self._rpc_count += 1
        with self._lock:
            can_comm = self.can_communicate(from_id, to_id)
            drop = random.random() < self._drop_rate if can_comm else True
            delay_ms = random.uniform(self._min_latency_ms, self._max_latency_ms) if can_comm else 0.0
            target_node = self._nodes.get(to_id)

        if not can_comm or drop or target_node is None:
            self._dropped_count += 1
            self.emit_event("RPC_DROPPED", from_id, {
                "target": to_id,
                "rpc": "RequestVote",
                "reason": "partition_or_loss"
            })
            return None

        if delay_ms > 0:
            time.sleep(delay_ms / 1000.0)

        # Execute remote call
        try:
            reply = target_node.handle_request_vote(args)
            reply.from_id = to_id
            self.emit_event("RPC_VOTE", from_id, {
                "target": to_id,
                "term": args.term,
                "granted": reply.vote_granted
            })
            return reply
        except Exception as e:
            self._dropped_count += 1
            return None

    def send_append_entries(self, from_id: str, to_id: str, args: AppendEntriesArgs) -> Optional[AppendEntriesReply]:
        self._rpc_count += 1
        with self._lock:
            can_comm = self.can_communicate(from_id, to_id)
            drop = random.random() < self._drop_rate if can_comm else True
            delay_ms = random.uniform(self._min_latency_ms, self._max_latency_ms) if can_comm else 0.0
            target_node = self._nodes.get(to_id)

        if not can_comm or drop or target_node is None:
            self._dropped_count += 1
            self.emit_event("RPC_DROPPED", from_id, {
                "target": to_id,
                "rpc": "AppendEntries",
                "reason": "partition_or_loss"
            })
            return None

        if delay_ms > 0:
            time.sleep(delay_ms / 1000.0)

        # Execute remote call
        try:
            reply = target_node.handle_append_entries(args)
            reply.from_id = to_id
            is_heartbeat = len(args.entries) == 0
            self.emit_event("RPC_APPEND" if not is_heartbeat else "RPC_HEARTBEAT", from_id, {
                "target": to_id,
                "term": args.term,
                "entries_count": len(args.entries),
                "success": reply.success
            })
            return reply
        except Exception as e:
            self._dropped_count += 1
            return None

    # --- Telemetry & Events ---

    def emit_event(self, event_type: str, node_id: str, details: Dict[str, Any]) -> None:
        ev = ClusterEvent(
            timestamp=time.time(),
            event_type=event_type,
            node_id=node_id,
            details=details
        )
        with self._lock:
            self._events.append(ev.to_dict())

    def get_events(self, limit: int = 50) -> List[Dict[str, Any]]:
        with self._lock:
            items = list(self._events)
            return items[-limit:]

    def get_status(self) -> Dict[str, Any]:
        with self._lock:
            return {
                "total_rpcs": self._rpc_count,
                "dropped_rpcs": self._dropped_count,
                "drop_rate": self._drop_rate,
                "latency_range_ms": [self._min_latency_ms, self._max_latency_ms],
                "partitions": [list(g) for g in self._partitions],
                "isolated_nodes": list(self._isolated_nodes),
                "is_partitioned": len(self._partitions) > 0 or len(self._isolated_nodes) > 0
            }
