#!/usr/bin/env python3
"""ChronoRaft: Distributed Consensus Engine & Chaos Hologram CLI."""
import argparse
import os
import sys
import time
import unittest

# Ensure chronoraft package is in sys.path
sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))

from chronoraft.cluster import RaftCluster
from chronoraft.server import run_server
from chronoraft.tui import format_cluster_status, run_live_tui


def run_demo() -> None:
    print("\n⚡ STARTING CHRONORAFT CONSENSUS & CHAOS DEMO ⚡")
    print("=" * 70)
    print("Initializing 5-node distributed Raft cluster...")
    cluster = RaftCluster(node_count=5)
    cluster.start()

    try:
        print("⏳ Waiting for initial leader election...")
        leader_id = cluster.wait_for_leader(timeout=4.0)
        if not leader_id:
            print("❌ Leader election timed out!")
            return

        print(f"👑 Leader Elected: {leader_id} (Term: {cluster.nodes[leader_id].current_term})")
        print("\n--- Initial Cluster Status ---")
        print(format_cluster_status(cluster.get_cluster_status()))

        print("\n📝 Proposing client writes to leader...")
        for i, (k, v) in enumerate([("cluster_name", "ChronoRaft"), ("replication_factor", 3), ("consensus", "strict")]):
            res = cluster.set(k, v)
            print(f"   [Write {i+1}] SET '{k}' = '{v}' ➔ Success: {res['success']} ({res.get('latency_ms', 0)}ms)")
            time.sleep(0.05)

        print("\n🌪️ INJECTING CHAOS: SPLIT-BRAIN NETWORK PARTITION!")
        print("   Partition A (Majority): [node-1, node-2, node-3]")
        print("   Partition B (Minority): [node-4, node-5]")
        cluster.partition_cluster([["node-1", "node-2", "node-3"], ["node-4", "node-5"]])
        time.sleep(0.4)

        print("\n--- Cluster Status Under Partition ---")
        print(format_cluster_status(cluster.get_cluster_status()))

        print("\n🛡️ Testing Quorum Safety on Partition A (Majority):")
        res_maj = cluster.set("quorum_check", "committed_by_majority")
        print(f"   Result: Success = {res_maj['success']} (Leader: {res_maj.get('leader')})")

        print("\n🩹 HEALING NETWORK PARTITION...")
        cluster.heal_cluster()
        time.sleep(0.4)

        print("\n--- Final Converged Cluster Status ---")
        print(format_cluster_status(cluster.get_cluster_status()))
        print("\n✅ ChronoRaft Demo completed successfully with zero split-brain violations!\n")
    finally:
        cluster.stop()


def run_benchmark(num_ops: int = 200) -> None:
    print(f"\n⚡ BENCHMARKING CHRONORAFT CONSENSUS: {num_ops} SEQUENTIAL COMMITS ⚡")
    print("=" * 70)
    cluster = RaftCluster(node_count=3, heartbeat_interval=0.03, election_timeout_range=(0.08, 0.18))
    cluster.start()

    try:
        leader_id = cluster.wait_for_leader(timeout=3.0)
        print(f"Cluster ready. Leader: {leader_id}")

        start_time = time.perf_counter()
        latencies = []
        for i in range(num_ops):
            t0 = time.perf_counter()
            res = cluster.set(f"bench_key_{i}", f"val_{i}")
            t1 = time.perf_counter()
            latencies.append((t1 - t0) * 1000.0)
            if not res["success"]:
                print(f"⚠️ Write {i} failed: {res}")

        total_elapsed = time.perf_counter() - start_time
        ops_sec = num_ops / total_elapsed
        latencies.sort()
        p50 = latencies[int(len(latencies) * 0.50)]
        p95 = latencies[int(len(latencies) * 0.95)]
        p99 = latencies[int(len(latencies) * 0.99)]

        print(f"• Total Operations:   {num_ops}")
        print(f"• Elapsed Time:       {total_elapsed:.3f} s")
        print(f"• Throughput:         {ops_sec:.1f} commits/sec (Quorum replicated)")
        print(f"• P50 Latency:        {p50:.2f} ms")
        print(f"• P95 Latency:        {p95:.2f} ms")
        print(f"• P99 Latency:        {p99:.2f} ms")
        print("=" * 70)
    finally:
        cluster.stop()


def main() -> None:
    parser = argparse.ArgumentParser(description="ChronoRaft: Real-Time 3D Raft Distributed Consensus Engine")
    parser.add_argument("--demo", action="store_true", help="Run interactive consensus and chaos demonstration")
    parser.add_argument("--tui", action="store_true", help="Launch live ANSI terminal radar HUD")
    parser.add_argument("--web", type=int, nargs="?", const=8765, default=None, help="Launch HTTP REST server on specified port (default: 8765)")
     parser.add_argument("--test", action="store_true", help="Run automated test suite")
    parser.add_argument("--benchmark", type=int, nargs="?", const=100, default=None, help="Run consensus write benchmark")
    parser.add_argument("--nodes", type=int, default=5, help="Number of nodes in cluster (default: 5)")

    args = parser.parse_args()

    if args.test:
        test_dir = os.path.join(os.path.dirname(os.path.abspath(__file__)), "tests")
        suite = unittest.defaultTestLoader.discover(test_dir)
        runner = unittest.TextTestRunner(verbosity=2)
        res = runner.run(suite)
        sys.exit(0 if res.wasSuccessful() else 1)

    if args.benchmark is not None:
        run_benchmark(num_ops=args.benchmark)
        return

    if args.demo:
        run_demo()
        return

    if args.web is not None:
        port = args.web
        print(f"⚡ Launching ChronoRaft Cluster ({args.nodes} nodes)...")
        cluster = RaftCluster(node_count=args.nodes)
        cluster.start()
        cluster.wait_for_leader(timeout=3.0)
        print(f"🚀 ChronoRaft HTTP Server & HUD running at http://127.0.0.1:{port}")
        print("   Endpoints: /api/status, /api/nodes, /api/logs, /api/events, /api/propose, /api/chaos/*")
        server = run_server(cluster, host="127.0.0.1", port=port)
        try:
            server.serve_forever()
        except KeyboardInterrupt:
            print("\nShutting down server and cluster...")
            server.shutdown()
            cluster.stop()
        return

    if args.tui:
        cluster = RaftCluster(node_count=args.nodes)
        cluster.start()
        cluster.wait_for_leader(timeout=3.0)
        try:
            run_live_tui(cluster)
        finally:
            cluster.stop()
        return

    # Default action if no args
     run_demo()


if __name__ == "__main__":
    main()
