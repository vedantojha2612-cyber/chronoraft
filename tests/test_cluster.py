"""Unit tests for Cluster lifecycle, Isolation, and Telemetry."""
import time
import unittest

from chronoraft.cluster import RaftCluster
from chronoraft.models import Role


class TestClusterOperations(unittest.TestCase):
    def setUp(self):
        self.cluster = RaftCluster(
            node_count=3,
            election_timeout_range=(0.10, 0.25),
            heartbeat_interval=0.04
        )
        self.cluster.start()
        self.leader_id = self.cluster.wait_for_leader(timeout=3.0)
        self.assertIsNotNone(self.leader_id)

    def tearDown(self):
        self.cluster.stop()

    def test_telemetry_schema(self):
        status = self.cluster.get_cluster_status()
        self.assertEqual(status["node_count"], 3)
        self.assertTrue(status["has_quorum"])
        self.assertEqual(status["majority_threshold"], 2)
        self.assertIn("nodes", status)
        self.assertIn("transport", status)
        self.assertIn("recent_events", status)

    def test_single_node_isolation_and_reconnection(self):
        # Pick a follower node to isolate
        follower_id = None
        for nid, node in self.cluster.nodes.items():
            if node.role == Role.FOLLOWER:
                follower_id = nid
                break
        self.assertIsNotNone(follower_id)

        # Isolate follower
        self.cluster.isolate_node(follower_id)

        # Cluster of 3 with 1 isolated still has 2 active nodes (majority = 2)
        res = self.cluster.set("survivor", "alive", timeout=1.5)
        self.assertTrue(res["success"])

        # Reconnect follower
        self.cluster.heal_cluster()
        time.sleep(0.3)

        # Follower must now catch up
        follower_node = self.cluster.nodes[follower_id]
        self.assertEqual(follower_node.state_machine.get("survivor"), "alive")


if __name__ == "__main__":
    unittest.main()
