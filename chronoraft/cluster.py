"""Multi-node Raft Cluster orchestrator with dynamic Chaos injection and cluster-wide telemetry."""
from __future__ import annotations

import threading
import time
from typing import Any, Dict, List, Optional, Tuple

from chronoraft.models import Role
from chronoraft.node import RaftNode
from chronoraft.transport import ChaosTransport


class RaftCluster:
    """Manages an ensemble of N Raft nodes, virtual chaos interconnects, and client request routing."""

    def __init__(
        self,
        node_count: int = 5,
        node_prefix: str = "node",
        data_dir: Optional[str] = None,
        election_timeout_range: Tuple[float, float] = (0.15, 0.35),
        heartbeat_interval: float = 0.05,
    ) -> None:
        self.node_count = node_count
        self.node_ids = [f"{node_prefix}-{i+1}" for i in range(node_count)]
        self.transport = ChaosTransport()
        self.data_dir = data_dir

        self.nodes: Dict[str, RaftNode] = {}
        for nid in self.node_ids:
            self.nodes[nid] = RaftNode(
                node_id=nid,
                peer_ids=self.node_ids,
                transport=self.transport,
                data_dir=data_dir,
                election_timeout_range=election_timeout_range,
                heartbeat_interval=heartbeat_interval,
            )

        self._started = False
        self._lock = threading.RLock()

    def start(self) -> None:
        with self._lock:
            if self._started:
                return
            self._started = True
            for node in self.nodes.values():
                node.start()

    def stop(self) -> None:
        with self._lock:
            if not self._started:
                return
            self._started = False
            for node in self.nodes.values():
                node.stop()

    def wait_for_leader(self, timeout: float = 3.0) -> Optional[str]:
        """Blocks until a stable cluster leader is elected or timeout expires."""
        deadline = time.time() + timeout
        while time.time() < deadline:
            for node in self.nodes.values():
                if node.role == Role.LEADER:
                    return node.node_id
            time.sleep(0.02)
        return None

    def get_leader(self) -> Optional[RaftNode]:
        for node in self.nodes.values():
            if node.role == Role.LEADER:
                return node
        return None

    def get_leader_id(self) -> Optional[str]:
        leader = self.get_leader()
        return leader.node_id if leader else None

    # --- Client Operations ---

    def execute_command(self, command: Dict[str, Any], timeout: float = 2.0) -> Dict[str, Any]:
        """Routes command to current leader, with automatic retry/redirect."""
        start_time = time.time()
        deadline = start_time + timeout

        while time.time() < deadline:
            leader = self.get_leader()
            if not leader:
                time.sleep(0.05)
                continue

            remaining = max(0.2, deadline - time.time())
            success, result, leader_id = leader.propose(command, timeout=remaining)
            if success:
                return {
                    "success": True,
                    "result": result,
                    "leader": leader_id,
                    "term": leader.current_term,
                    "latency_ms": round((time.time() - start_time) * 1000, 2)
                }
            time.sleep(0.05)

        return {
            "success": False,
            "error": "Failed to commit command before timeout (cluster partitioned or no leader)",
            "leader": self.get_leader_id(),
            "latency_ms": round((time.time() - start_time) * 1000, 2)
        }

    def set(self, key: str, value: Any, timeout: float = 2.0) -> Dict[str, Any]:
        return self.execute_command({"op": "SET", "key": key, "value": value}, timeout=timeout)

    def get(self, key: str) -> Dict[str, Any]:
        leader = self.get_leader()
        if not leader:
            return {"success": False, "error": "No elected leader available"}
        val = leader.state_machine.get(key)
        return {"success": True, "key": key, "value": val, "leader": leader.node_id}

    # --- Chaos Injections ---

    def partition_cluster(self, groups: List[List[str]]) -> Dict[str, Any]:
        self.transport.partition(groups)
        return {"status": "partitioned", "groups": groups}

    def isolate_node(self, node_id: str) -> Dict[str, Any]:
        self.transport.isolate(node_id)
        return {"status": "isolated", "node": node_id}

    def heal_cluster(self) -> Dict[str, Any]:
        self.transport.heal()
        return {"status": "healed"}

    def kill_node(self, node_id: str) -> Dict[str, Any]:
        node = self.nodes.get(node_id)
        if node:
            node.pause()
            return {"status": "killed", "node": node_id}
        return {"status": "not_found", "node": node_id}

    def resume_node(self, node_id: str) -> Dict[str, Any]:
        node = self.nodes.get(node_id)
        if node:
            node.resume()
            return {"status": "resumed", "node": node_id}
        return {"status": "not_found", "node": node_id}

    # --- Telemetry & Diagnostics ---

    def get_cluster_status(self) -> Dict[str, Any]:
        leader = self.get_leader()
        nodes_telemetry = {nid: n.get_telemetry() for nid, n in self.nodes.items()}
        transport_status = self.transport.get_status()

        # Quorum consensus health
        active_nodes = sum(1 for n in self.nodes.values() if n.role != Role.DEAD)
        majority = (self.node_count // 2) + 1
        has_quorum = active_nodes >= majority

        # State machine sync check
        synced_kv = {}
        if leader:
            synced_kv = leader.state_machine.get_all()

        return {
            "node_count": self.node_count,
            "leader_id": leader.node_id if leader else None,
            "leader_term": leader.current_term if leader else None,
            "has_quorum": has_quorum,
            "active_nodes": active_nodes,
            "majority_threshold": majority,
            "transport": transport_status,
            "nodes": nodes_telemetry,
            "kv_store": synced_kv,
            "recent_events": self.transport.get_events(limit=30)
        }
