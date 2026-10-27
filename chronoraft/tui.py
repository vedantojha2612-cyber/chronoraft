"""ANSI Terminal Radar and live cluster status dashboard for ChronoRaft."""
from __future__ import annotations

import os
import re
import sys
import time
from typing import Any, Dict

from chronoraft.cluster import RaftCluster
from chronoraft.models import Role


# ANSI Color Codes
RESET = "\033[0m"
BOLD = "\033[1m"
DIM = "\033[2m"
GOLD = "\033[38;5;220m"
AMBER = "\033[38;5;214m"
CYAN = "\033[38;5;51m"
GREEN = "\033[38;5;82m"
RED = "\033[38;5;196m"
GRAY = "\033[38;5;244m"
MAGENTA = "\033[38;5;201m"


def pad_ansi(text: str, width: int) -> str:
    """Pads a string containing ANSI escape codes to visible character width."""
    visible = re.sub(r"\033\[[0-9;]*m", "", text)
    pad = max(0, width - len(visible))
    return text + (" " * pad)


def role_badge(role_str: str) -> str:
    if role_str == Role.LEADER.value:
        return f"{BOLD}{GOLD}LEADER{RESET}"
    if role_str == Role.CANDIDATE.value:
        return f"{BOLD}{AMBER}CANDIDATE{RESET}"
    if role_str == Role.FOLLOWER.value:
        return f"{CYAN}FOLLOWER{RESET}"
    return f"{RED}DEAD{RESET}"


def format_cluster_status(status: Dict[str, Any]) -> str:
    lines = []
    lines.append(f"{BOLD}{MAGENTA}⚡ CHRONORAFT: DISTRIBUTED CONSENSUS ENGINE & CHAOS RADAR ⚡{RESET}")
    lines.append("=" * 86)

    leader_id = status.get("leader_id") or "NONE (ELECTION IN PROGRESS)"
    term = status.get("leader_term") if status.get("leader_term") is not None else "-"
    has_quorum = status.get("has_quorum", False)
    q_str = f"{GREEN}HEALTHY (QUORUM ACHIEVED){RESET}" if has_quorum else f"{RED}QUORUM LOST (SPLIT BRAIN / ISOLATION){RESET}"

    lines.append(f" Cluster Leader:    {BOLD}{GOLD}{leader_id}{RESET} (Term: {term})")
    lines.append(f" Quorum Status:     {q_str} [Majority: {status.get('majority_threshold')}/{status.get('node_count')}]")

    transport = status.get("transport", {})
    is_part = transport.get("is_partitioned", False)
    if is_part:
        part_info = f"{RED}ACTIVE PARTITION MATRIX: {transport.get('partitions')}{RESET}"
        if transport.get("isolated_nodes"):
            part_info += f" | {RED}Isolated: {transport.get('isolated_nodes')}{RESET}"
    else:
        part_info = f"{GREEN}FULL MESH (ALL NODES CONNECTED){RESET}"
    lines.append(f" Network Topology:  {part_info}")
    lines.append(f" RPC Statistics:    Total: {transport.get('total_rpcs', 0)} | Dropped: {transport.get('dropped_rpcs', 0)} (Loss: {transport.get('drop_rate', 0.0) * 100:.1f}%)")

    lines.append("-" * 86)
    hdr = f"{'NODE ID':<10} {'ROLE':<14} {'TERM':<8} {'COMMIT':<9} {'APPLIED':<9} {'LOG IDX':<9} {'VOTED FOR':<12} {'TIMEOUT':<8}"
    lines.append(f"{BOLD}{hdr}{RESET}")
    lines.append("-" * 86)

    nodes = status.get("nodes", {})
    for nid in sorted(nodes.keys()):
        n = nodes[nid]
        r_badge = pad_ansi(role_badge(n.get("role", "DEAD")), 14)
        t = str(n.get("term", 0))
        c = str(n.get("commit_index", 0))
        a = str(n.get("last_applied", 0))
        l = str(n.get("last_log_index", 0))
        v = str(n.get("voted_for") or "-")
        to = f"{n.get('election_timeout', 0.0):.2f}s"
        row = f"{nid:<10} {r_badge} {t:<8} {c:<9} {a:<9} {l:<9} {v:<12} {to:<8}"
        lines.append(row)

    lines.append("-" * 86)
    lines.append(f"{BOLD}💾 STATE MACHINE KEY-VALUE STORE:{RESET}")
    kv = status.get("kv_store", {})
    if kv:
        for k, v in list(kv.items())[:8]:
            lines.append(f"   • {CYAN}{k}{RESET} ➔ {BOLD}{v}{RESET}")
        if len(kv) > 8:
            lines.append(f"   ... ({len(kv) - 8} more keys)")
    else:
        lines.append(f"   {GRAY}(No committed keys yet){RESET}")

    lines.append("-" * 86)
    lines.append(f"{BOLD}📡 RECENT CLUSTER & RPC TELEMETRY EVENTS:{RESET}")
    events = status.get("recent_events", [])
    for ev in events[-6:]:
        etype = ev.get("event_type", "")
        nid = ev.get("node_id", "")
        details = ev.get("details", {})
        ts = time.strftime("%H:%M:%S", time.localtime(ev.get("timestamp", time.time())))
        lines.append(f"   [{GRAY}{ts}{RESET}] {BOLD}{etype:<18}{RESET} ({CYAN}{nid}{RESET}): {details}")

    lines.append("=" * 86)
    return "\n".join(lines)


def run_live_tui(cluster: RaftCluster, refresh_rate: float = 0.25) -> None:
    """Renders a continuous live terminal radar HUD."""
    try:
        while True:
            sys.stdout.write("\033[H\033[J")
            status = cluster.get_cluster_status()
            sys.stdout.write(format_cluster_status(status) + "\n")
            sys.stdout.write(f"{GRAY}Press Ctrl+C to exit TUI monitor...{RESET}\n")
            sys.stdout.flush()
            time.sleep(refresh_rate)
    except KeyboardInterrupt:
        sys.stdout.write(f"\n{GREEN}TUI monitor stopped.{RESET}\n")
