import unittest
import time
import concurrent.futures
import threading
from chronoraft.cluster import RaftCluster

class TestPerformanceBenchmarks(unittest.TestCase):
    def test_concurrent_throughput_and_latency(self):
        """Measures concurrent commit throughput and P95 latency under multi-threaded load."""
        cluster = RaftCluster(node_count=5, heartbeat_interval=0.03, election_timeout_range=(0.08, 0.18))
        cluster.start()
        try:
            leader = cluster.wait_for_leader(timeout=3.0)
            self.assertIsNotNone(leader)

            latencies = []
            lock = threading.Lock()

            def worker(i):
                t0 = time.perf_counter()
                res = cluster.set(f"load_k_{i}", f"v_{i}")
                t1 = time.perf_counter()
                with lock:
                    latencies.append((t1 - t0) * 1000.0)
                return res

            t_start = time.perf_counter()
            with concurrent.futures.ThreadPoolExecutor(max_workers=20) as executor:
                futures = [executor.submit(worker, i) for i in range(100)]
                results = [f.result() for f in concurrent.futures.as_completed(futures)]
            t_end = time.perf_counter()

            successful = sum(1 for r in results if r.get("success"))
            self.assertEqual(successful, 100)

            latencies.sort()
            p95 = latencies[int(len(latencies) * 0.95)]
            self.assertLess(p95, 300.0)  # P95 should be well under 300ms
        finally:
            cluster.stop()

    def test_election_failover_latency(self):
        """Measures cluster leader failover recovery time."""
        cluster = RaftCluster(node_count=5, heartbeat_interval=0.03, election_timeout_range=(0.08, 0.18))
        cluster.start()
        try:
            leader = cluster.wait_for_leader(timeout=3.0)
            self.assertIsNotNone(leader)

            t0 = time.perf_counter()
            cluster.kill_node(leader)
            new_leader = cluster.wait_for_leader(timeout=3.0)
            t1 = time.perf_counter()
            failover_ms = (t1 - t0) * 1000.0

            self.assertIsNotNone(new_leader)
            self.assertNotEqual(leader, new_leader)
            self.assertLess(failover_ms, 500.0)  # Failover should happen within 500ms
        finally:
            cluster.stop()

if __name__ == "__main__":
    unittest.main()
