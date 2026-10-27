"""Storage engine for persistent Raft metadata and write-ahead log entries."""
from __future__ import annotations

import json
import os
import threading
from typing import Any, Dict, List, Optional

from chronoraft.models import LogEntry


class RaftStorage:
    """Manages Raft persistent state (current_term, voted_for, log) with optional atomic disk sync."""

    def __init__(self, node_id: str, data_dir: Optional[str] = None) -> None:
        self.node_id = node_id
        self.data_dir = data_dir
        self._lock = threading.RLock()

        # In-memory state
        self._current_term: int = 0
        self._voted_for: Optional[str] = None
        # Log is 1-indexed; index 0 is a sentinel entry
        self._log: List[LogEntry] = [LogEntry(index=0, term=0, command={"op": "INIT"})]

        if self.data_dir:
            os.makedirs(self.data_dir, exist_ok=True)
            self._state_file = os.path.join(self.data_dir, f"{self.node_id}_state.json")
            self._load_from_disk()

    @property
    def current_term(self) -> int:
        with self._lock:
            return self._current_term

    @property
    def voted_for(self) -> Optional[str]:
        with self._lock:
            return self._voted_for

    @property
    def last_log_index(self) -> int:
        with self._lock:
            return self._log[-1].index

    @property
    def last_log_term(self) -> int:
        with self._lock:
            return self._log[-1].term

    def set_term_and_vote(self, term: int, voted_for: Optional[str]) -> None:
        with self._lock:
            self._current_term = term
            self._voted_for = voted_for
            self._persist_to_disk()

    def set_voted_for(self, candidate_id: Optional[str]) -> None:
        with self._lock:
            self._voted_for = candidate_id
            self._persist_to_disk()

    def get_entry(self, index: int) -> Optional[LogEntry]:
        with self._lock:
            if 0 <= index < len(self._log):
                return self._log[index]
            return None

    def get_entries_from(self, start_index: int) -> List[LogEntry]:
        with self._lock:
            if start_index < len(self._log):
                return [entry for entry in self._log[start_index:]]
            return []

    def append_entry(self, term: int, command: Dict[str, Any]) -> LogEntry:
        with self._lock:
            new_index = len(self._log)
            entry = LogEntry(index=new_index, term=term, command=command)
            self._log.append(entry)
            self._persist_to_disk()
            return entry

    def truncate_and_append(self, prev_index: int, new_entries: List[LogEntry]) -> None:
        """Truncates conflicting entries after prev_index and appends new entries."""
        with self._lock:
            # Keep log up to prev_index
            if prev_index + 1 < len(self._log):
                self._log = self._log[: prev_index + 1]

            for entry in new_entries:
                if entry.index < len(self._log):
                    if self._log[entry.index].term != entry.term:
                        self._log = self._log[: entry.index]
                        self._log.append(entry)
                else:
                    self._log.append(entry)

            self._persist_to_disk()

    def get_all_entries(self) -> List[Dict[str, Any]]:
        with self._lock:
            return [e.to_dict() for e in self._log[1:]]

    def _persist_to_disk(self) -> None:
        if not self.data_dir:
            return
        tmp_file = f"{self._state_file}.tmp"
        data = {
            "current_term": self._current_term,
            "voted_for": self._voted_for,
            "log": [e.to_dict() for e in self._log]
        }
        with open(tmp_file, "w") as f:
            json.dump(data, f)
        os.replace(tmp_file, self._state_file)

    def _load_from_disk(self) -> None:
        if not self.data_dir or not os.path.exists(self._state_file):
            return
        try:
            with open(self._state_file, "r") as f:
                data = json.load(f)
            self._current_term = int(data.get("current_term", 0))
            self._voted_for = data.get("voted_for")
            raw_log = data.get("log", [])
            if raw_log:
                self._log = [LogEntry.from_dict(d) for d in raw_log]
        except Exception:
            pass
