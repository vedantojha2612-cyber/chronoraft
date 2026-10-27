"""Data structures, protocol RPC schemas, and event definitions for ChronoRaft."""
from __future__ import annotations

import time
from dataclasses import dataclass, field, asdict
from enum import Enum
from typing import Any, Dict, List, Optional


class Role(str, Enum):
    FOLLOWER = "FOLLOWER"
    CANDIDATE = "CANDIDATE"
    LEADER = "LEADER"
    DEAD = "DEAD"


@dataclass
class LogEntry:
    index: int
    term: int
    command: Dict[str, Any]
    timestamp: float = field(default_factory=time.time)

    def to_dict(self) -> Dict[str, Any]:
        return asdict(self)

    @classmethod
    def from_dict(cls, data: Dict[str, Any]) -> LogEntry:
        return cls(
            index=int(data["index"]),
            term=int(data["term"]),
            command=dict(data.get("command", {})),
            timestamp=float(data.get("timestamp", time.time()))
        )


@dataclass
class RequestVoteArgs:
    term: int
    candidate_id: str
    last_log_index: int
    last_log_term: int

    def to_dict(self) -> Dict[str, Any]:
        return asdict(self)

    @classmethod
    def from_dict(cls, data: Dict[str, Any]) -> RequestVoteArgs:
        return cls(
            term=int(data["term"]),
            candidate_id=str(data["candidate_id"]),
            last_log_index=int(data["last_log_index"]),
            last_log_term=int(data["last_log_term"])
        )


@dataclass
class RequestVoteReply:
    term: int
    vote_granted: bool
    from_id: str = ""

    def to_dict(self) -> Dict[str, Any]:
        return asdict(self)

    @classmethod
    def from_dict(cls, data: Dict[str, Any]) -> RequestVoteReply:
        return cls(
            term=int(data["term"]),
            vote_granted=bool(data["vote_granted"]),
            from_id=str(data.get("from_id", ""))
        )


@dataclass
class AppendEntriesArgs:
    term: int
    leader_id: str
    prev_log_index: int
    prev_log_term: int
    entries: List[Dict[str, Any]] = field(default_factory=list)
    leader_commit: int = 0

    def to_dict(self) -> Dict[str, Any]:
        return asdict(self)

    @classmethod
    def from_dict(cls, data: Dict[str, Any]) -> AppendEntriesArgs:
        return cls(
            term=int(data["term"]),
            leader_id=str(data["leader_id"]),
            prev_log_index=int(data["prev_log_index"]),
            prev_log_term=int(data["prev_log_term"]),
            entries=list(data.get("entries", [])),
            leader_commit=int(data.get("leader_commit", 0))
        )


@dataclass
class AppendEntriesReply:
    term: int
    success: bool
    match_index: int = 0
    from_id: str = ""

    def to_dict(self) -> Dict[str, Any]:
        return asdict(self)

    @classmethod
    def from_dict(cls, data: Dict[str, Any]) -> AppendEntriesReply:
        return cls(
            term=int(data["term"]),
            success=bool(data["success"]),
            match_index=int(data.get("match_index", 0)),
            from_id=str(data.get("from_id", ""))
        )


@dataclass
class ClusterEvent:
    timestamp: float
    event_type: str
    node_id: str
    details: Dict[str, Any] = field(default_factory=dict)

    def to_dict(self) -> Dict[str, Any]:
        return asdict(self)
