"""Deterministic Key-Value State Machine for ChronoRaft log execution."""
from __future__ import annotations

import threading
from typing import Any, Dict, Optional, Tuple


class KeyValueStateMachine:
    """Thread-safe, deterministic state machine applied sequentially from Raft log commits."""

    def __init__(self) -> None:
        self._lock = threading.RLock()
        self._kv: Dict[str, Any] = {}
        self._last_applied_index: int = 0
        self._mutation_count: int = 0

    @property
    def last_applied_index(self) -> int:
        with self._lock:
            return self._last_applied_index

    def apply(self, index: int, command: Dict[str, Any]) -> Tuple[bool, Any]:
        """Applies a single committed log entry sequentially.
        
        Returns:
            Tuple of (success: bool, result: Any)
        """
        with self._lock:
            if index <= self._last_applied_index and index != 0:
                # Idempotency guard: already applied
                return True, "Already applied"

            op = str(command.get("op", "")).upper()
            key = str(command.get("key", ""))
            val = command.get("value")

            result: Any = None
            success = True

            if op == "SET":
                self._kv[key] = val
                result = val
                self._mutation_count += 1
            elif op == "GET":
                result = self._kv.get(key)
            elif op == "DEL":
                existed = key in self._kv
                result = self._kv.pop(key, None)
                success = existed
                if existed:
                    self._mutation_count += 1
            elif op == "INCR":
                delta = int(command.get("delta", 1))
                curr = int(self._kv.get(key, 0))
                self._kv[key] = curr + delta
                result = self._kv[key]
                self._mutation_count += 1
            elif op == "CAS":
                old_val = command.get("expected")
                curr = self._kv.get(key)
                if curr == old_val:
                    self._kv[key] = val
                    result = val
                    self._mutation_count += 1
                else:
                    success = False
                    result = {"error": "CAS mismatch", "current": curr, "expected": old_val}
            elif op == "NOOP":
                result = "NOOP"
            else:
                success = False
                result = f"Unknown op: {op}"

            self._last_applied_index = index
            return success, result

    def get(self, key: str) -> Optional[Any]:
        with self._lock:
            return self._kv.get(key)

    def get_all(self) -> Dict[str, Any]:
        with self._lock:
            return dict(self._kv)

    def snapshot(self) -> Dict[str, Any]:
        with self._lock:
            return {
                "kv": dict(self._kv),
                "last_applied_index": self._last_applied_index,
                "mutation_count": self._mutation_count
            }

    def restore(self, snap: Dict[str, Any]) -> None:
        with self._lock:
            self._kv = dict(snap.get("kv", {}))
            self._last_applied_index = int(snap.get("last_applied_index", 0))
            self._mutation_count = int(snap.get("mutation_count", 0))
