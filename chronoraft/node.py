"""Raft Consensus Node with strict State Machine replication, election timers, and quorum commits."""
from __future__ import annotations

import random
import threading
import time
from concurrent.futures import ThreadPoolExecutor
from typing import Any, Dict, List, Optional, Tuple

from chronoraft.models import (
    AppendEntriesArgs,
    AppendEntriesReply,
    LogEntry,
    RequestVoteArgs,
    RequestVoteReply,
    Role,
)
from chronoraft.state_machine import KeyValueStateMachine
from chronoraft.storage import RaftStorage
from chronoraft.transport import ChaosTransport


class RaftNode:
    """Production-grade Raft consensus node implementing the Ongaro-Ousterhout consensus specification."""

    def __init__(
        self,
        node_id: str,
        peer_ids: List[str],
        transport: ChaosTransport,
        data_dir: Optional[str] = None,
        election_timeout_range: Tuple[float, float] = (0.15, 0.35),
        heartbeat_interval: float = 0.05,
    ) -> None:
        self.node_id = str(node_id)
        self.peer_ids = [str(p) for p in peer_ids if str(p) != self.node_id]
        self.transport = transport
        self.storage = RaftStorage(node_id=self.node_id, data_dir=data_dir)
        self.state_machine = KeyValueStateMachine()

        self._lock = threading.RLock()
        self._role: Role = Role.FOLLOWER
        self.leader_id: Optional[str] = None

        # Volatile state on all servers
        self.commit_index: int = 0
        self.last_applied: int = 0

        # Volatile state on leaders
        self.next_index: Dict[str, int] = {}
        self.match_index: Dict[str, int] = {}

        # Timing configurations (in seconds)
        self._timeout_min, self._timeout_max = election_timeout_range
        self.heartbeat_interval = heartbeat_interval
        self._election_timeout = self._random_election_timeout()
        self._last_heartbeat_time = time.time()

        # Threading & lifecycle
        self._running = False
        self._threads: List[threading.Thread] = []
        self._pool = ThreadPoolExecutor(max_workers=max(4, len(self.peer_ids) * 2), thread_name_prefix=f"Raft-{self.node_id}")
        self._pending_commits: Dict[int, threading.Event] = {}
        self._commit_results: Dict[int, Tuple[bool, Any]] = {}

        # Telemetry stats
        self.elections_won: int = 0
        self.elections_started: int = 0
        self.proposals_handled: int = 0

    @property
    def role(self) -> Role:
        with self._lock:
            return self._role

    @property
    def current_term(self) -> int:
        return self.storage.current_term

    @property
    def voted_for(self) -> Optional[str]:
        return self.storage.voted_for

    @property
    def last_log_index(self) -> int:
        return self.storage.last_log_index

    @property
    def last_log_term(self) -> int:
        return self.storage.last_log_term

    def _random_election_timeout(self) -> float:
        return random.uniform(self._timeout_min, self._timeout_max)

    def _reset_election_timer(self) -> None:
        self._last_heartbeat_time = time.time()
        self._election_timeout = self._random_election_timeout()

    # --- Lifecycle Management ---

    def start(self) -> None:
        with self._lock:
            if self._running:
                return
            self._running = True
            self.transport.register_node(self.node_id, self)
            self._reset_election_timer()

            t_timer = threading.Thread(target=self._run_timer_loop, daemon=True, name=f"timer-{self.node_id}")
            t_apply = threading.Thread(target=self._run_apply_loop, daemon=True, name=f"apply-{self.node_id}")
            self._threads = [t_timer, t_apply]
            for t in self._threads:
                t.start()

    def stop(self) -> None:
        with self._lock:
            self._running = False
            self._role = Role.DEAD
            self.transport.unregister_node(self.node_id)
        self._pool.shutdown(wait=False)

    def pause(self) -> None:
        with self._lock:
            self._role = Role.DEAD
            self.transport.unregister_node(self.node_id)

    def resume(self) -> None:
        with self._lock:
            self._role = Role.FOLLOWER
            self._reset_election_timer()
            self.transport.register_node(self.node_id, self)

    # --- Background Loops ---

    def _run_timer_loop(self) -> None:
        while self._running:
            time.sleep(0.015)
            with self._lock:
                if self._role == Role.DEAD:
                    continue

                now = time.time()
                elapsed = now - self._last_heartbeat_time

                if self._role == Role.LEADER:
                    if elapsed >= self.heartbeat_interval:
                        self._last_heartbeat_time = now
                        self._pool.submit(self._replicate_entries_to_all)
                else:
                    if elapsed >= self._election_timeout:
                        self._start_election()

    def _run_apply_loop(self) -> None:
        while self._running:
            time.sleep(0.01)
            entries_to_apply = []
            with self._lock:
                while self.commit_index > self.last_applied:
                    self.last_applied += 1
                    entry = self.storage.get_entry(self.last_applied)
                    if entry:
                        entries_to_apply.append(entry)

            for entry in entries_to_apply:
                success, res = self.state_machine.apply(entry.index, entry.command)
                with self._lock:
                    self._commit_results[entry.index] = (success, res)
                    if entry.index in self._pending_commits:
                        self._pending_commits[entry.index].set()

    # --- Leader Election ---

    def _start_election(self) -> None:
        self._role = Role.CANDIDATE
        new_term = self.storage.current_term + 1
        self.storage.set_term_and_vote(new_term, self.node_id)
        self._reset_election_timer()
        self.leader_id = None
        self.elections_started += 1

        term = new_term
        last_index = self.last_log_index
        last_term = self.last_log_term
        peers = list(self.peer_ids)

        self.transport.emit_event("ELECTION_START", self.node_id, {
            "term": term,
            "role": "CANDIDATE"
        })

        # Self-vote counts as 1
        votes_lock = threading.Lock()
        votes_granted = 1
        total_nodes = len(peers) + 1
        majority = (total_nodes // 2) + 1

        if votes_granted >= majority:
            self._become_leader()
            return

        def request_vote_task(peer: str) -> None:
            nonlocal votes_granted
            args = RequestVoteArgs(
                term=term,
                candidate_id=self.node_id,
                last_log_index=last_index,
                last_log_term=last_term
            )
            reply = self.transport.send_request_vote(self.node_id, peer, args)
            if not reply:
                return

            with self._lock:
                if self._role != Role.CANDIDATE or self.storage.current_term != term:
                    if reply.term > self.storage.current_term:
                        self._step_down(reply.term)
                    return

                if reply.term > self.storage.current_term:
                    self._step_down(reply.term)
                    return

                if reply.vote_granted:
                    with votes_lock:
                        votes_granted += 1
                        current_votes = votes_granted

                    if current_votes >= majority and self._role == Role.CANDIDATE:
                        self._become_leader()

        for peer in peers:
            self._pool.submit(request_vote_task, peer)

    def _become_leader(self) -> None:
        self._role = Role.LEADER
        self.leader_id = self.node_id
        self.elections_won += 1

        # Re-initialize leader state
        last_idx = self.last_log_index
        for peer in self.peer_ids:
            self.next_index[peer] = last_idx + 1
            self.match_index[peer] = 0

        self.transport.emit_event("LEADER_ELECTED", self.node_id, {
            "term": self.storage.current_term,
            "peers": self.peer_ids
        })

        # Send immediate empty AppendEntries (heartbeat)
        self._pool.submit(self._replicate_entries_to_all)

    def _step_down(self, new_term: int) -> None:
        self._role = Role.FOLLOWER
        self.storage.set_term_and_vote(new_term, None)
        self.leader_id = None
        self._reset_election_timer()
        self.transport.emit_event("ROLE_STEP_DOWN", self.node_id, {
            "term": new_term,
            "role": "FOLLOWER"
        })

    # --- RPC Handlers ---

    def handle_request_vote(self, args: RequestVoteArgs) -> RequestVoteReply:
        with self._lock:
            if self._role == Role.DEAD:
                return RequestVoteReply(term=self.storage.current_term, vote_granted=False)

            current_term = self.storage.current_term

            # Rule 1: Term check
            if args.term > current_term:
                self._step_down(args.term)
                current_term = args.term

            if args.term < current_term:
                return RequestVoteReply(term=current_term, vote_granted=False)

            # Rule 2: Vote check
            voted_for = self.storage.voted_for
            can_vote = (voted_for is None or voted_for == args.candidate_id)

            # Rule 3: Candidate log up-to-date check
            last_term = self.last_log_term
            last_idx = self.last_log_index
            log_up_to_date = (
                args.last_log_term > last_term
                or (args.last_log_term == last_term and args.last_log_index >= last_idx)
            )

            if can_vote and log_up_to_date:
                self.storage.set_voted_for(args.candidate_id)
                self._reset_election_timer()
                return RequestVoteReply(term=current_term, vote_granted=True)

            return RequestVoteReply(term=current_term, vote_granted=False)

    def handle_append_entries(self, args: AppendEntriesArgs) -> AppendEntriesReply:
        with self._lock:
            if self._role == Role.DEAD:
                return AppendEntriesReply(term=self.storage.current_term, success=False)

            current_term = self.storage.current_term

            # Rule 1: Reply false if term < currentTerm
            if args.term < current_term:
                return AppendEntriesReply(term=current_term, success=False, match_index=self.last_log_index)

            # If term > currentTerm or candidate discovered current leader
            if args.term > current_term or self._role == Role.CANDIDATE:
                self._role = Role.FOLLOWER
                self.storage.set_term_and_vote(args.term, None)
                current_term = args.term

            self.leader_id = args.leader_id
            self._reset_election_timer()

            # Rule 2: Reply false if log doesn't contain entry at prev_log_index matching prev_log_term
            if args.prev_log_index > self.last_log_index:
                return AppendEntriesReply(term=current_term, success=False, match_index=self.last_log_index)

            prev_entry = self.storage.get_entry(args.prev_log_index)
            if prev_entry is None or prev_entry.term != args.prev_log_term:
                # Hint for fast rollback: decrement match_index
                return AppendEntriesReply(
                    term=current_term,
                    success=False,
                    match_index=max(0, min(self.last_log_index, args.prev_log_index - 1))
                )

            # Rule 3 & 4: Append new entries, resolving conflicts
            new_log_entries = [LogEntry.from_dict(d) for d in args.entries]
            if new_log_entries:
                self.storage.truncate_and_append(args.prev_log_index, new_log_entries)

            # Rule 5: Update commitIndex
            if args.leader_commit > self.commit_index:
                self.commit_index = min(args.leader_commit, self.last_log_index)

            return AppendEntriesReply(term=current_term, success=True, match_index=self.last_log_index)

    # --- Log Replication (Leader Side) ---

    def _replicate_entries_to_all(self) -> None:
        with self._lock:
            if self._role != Role.LEADER:
                return
            peers = list(self.peer_ids)

        for peer in peers:
            self._pool.submit(self._replicate_to_peer, peer)

    def _replicate_to_peer(self, peer: str) -> None:
        with self._lock:
            if self._role != Role.LEADER:
                return
            term = self.storage.current_term
            prev_idx = self.next_index.get(peer, self.last_log_index + 1) - 1
            prev_entry = self.storage.get_entry(prev_idx)
            prev_term = prev_entry.term if prev_entry else 0

            entries = [e.to_dict() for e in self.storage.get_entries_from(prev_idx + 1)]
            leader_commit = self.commit_index

        args = AppendEntriesArgs(
            term=term,
            leader_id=self.node_id,
            prev_log_index=prev_idx,
            prev_log_term=prev_term,
            entries=entries,
            leader_commit=leader_commit
        )

        reply = self.transport.send_append_entries(self.node_id, peer, args)
        if not reply:
            return

        with self._lock:
            if self._role != Role.LEADER or self.storage.current_term != term:
                if reply.term > self.storage.current_term:
                    self._step_down(reply.term)
                return

            if reply.term > self.storage.current_term:
                self._step_down(reply.term)
                return

            if reply.success:
                match = reply.match_index
                self.match_index[peer] = max(self.match_index.get(peer, 0), match)
                self.next_index[peer] = self.match_index[peer] + 1
                self._check_and_update_commit_index()
            else:
                # Log inconsistency: decrement next_index and retry
                hint = reply.match_index
                current_next = self.next_index.get(peer, self.last_log_index + 1)
                new_next = min(current_next - 1, hint + 1)
                self.next_index[peer] = max(1, new_next)

    def _check_and_update_commit_index(self) -> None:
        """Finds highest N such that N > commitIndex, majority of match_index[i] >= N, and log[N].term == currentTerm."""
        if self._role != Role.LEADER:
            return

        total_nodes = len(self.peer_ids) + 1
        majority = (total_nodes // 2) + 1

        for N in range(self.last_log_index, self.commit_index, -1):
            entry = self.storage.get_entry(N)
            if not entry or entry.term != self.storage.current_term:
                continue

            matches = 1  # Leader itself has the entry
            for peer in self.peer_ids:
                if self.match_index.get(peer, 0) >= N:
                    matches += 1

            if matches >= majority:
                self.commit_index = N
                self.transport.emit_event("COMMIT_ADVANCED", self.node_id, {
                    "commit_index": N,
                    "term": self.storage.current_term,
                    "matches": matches
                })
                break

    # --- Client Interaction ---

    def propose(self, command: Dict[str, Any], timeout: float = 2.0) -> Tuple[bool, Any, Optional[str]]:
        """Proposes a client command to the Raft cluster.
        
        Returns:
            Tuple of (success: bool, result: Any, leader_id: Optional[str])
        """
        with self._lock:
            if self._role == Role.DEAD:
                return False, "Node is stopped", None
            if self._role != Role.LEADER:
                return False, "Not cluster leader", self.leader_id

            entry = self.storage.append_entry(self.storage.current_term, command)
            entry_index = entry.index
            commit_event = threading.Event()
            self._pending_commits[entry_index] = commit_event
            self.proposals_handled += 1

        # Trigger immediate replication
        self._pool.submit(self._replicate_entries_to_all)

        # Await quorum commit & state machine execution
        committed = commit_event.wait(timeout=timeout)

        with self._lock:
            self._pending_commits.pop(entry_index, None)
            if not committed:
                return False, f"Propose timeout ({timeout}s) - quorum unreached", self.node_id
            success, res = self._commit_results.pop(entry_index, (True, "Committed"))
            return success, res, self.node_id

    # --- Telemetry & Diagnostics ---

    def get_telemetry(self) -> Dict[str, Any]:
        with self._lock:
            return {
                "node_id": self.node_id,
                "role": self._role.value,
                "term": self.storage.current_term,
                "voted_for": self.storage.voted_for,
                "leader_id": self.leader_id,
                "commit_index": self.commit_index,
                "last_applied": self.last_applied,
                "last_log_index": self.last_log_index,
                "last_log_term": self.last_log_term,
                "election_timeout": round(self._election_timeout, 3),
                "elections_won": self.elections_won,
                "proposals_handled": self.proposals_handled,
                "next_index": dict(self.next_index) if self._role == Role.LEADER else {},
                "match_index": dict(self.match_index) if self._role == Role.LEADER else {},
                "log_entries_count": len(self.storage.get_all_entries()),
                "kv_store": self.state_machine.get_all()
            }
