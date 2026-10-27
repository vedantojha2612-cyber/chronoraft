"""Unit tests for Raft Log Replication, Quorum Commit, and Reconciliation."""
import time
import unittest

from chronoraft.cluster import RaftCluster


class TestLogReplication(unittest.TestCase):
    def setUp(self):
        self.cluster = RaftCluster(
            node_count=3,
            election_timeout_range=(0.10, 0.25),
            heartbeat_interval=0.04
        )
        self.cluster.start()
        self.leader_id = self.cluster.wait_for_leader(timeout=3.0)
        self.assertIsNotNone(self.leader_id, "Leader election failed during setUp")

    def tearDown(self):
        self.cluster.stop()

    def test_single_command_replication(self):
        res = self.cluster.set("alpha", 42, timeout=2.0)
        self.assertTrue(res["success"], f"Failed to commit alpha: {res}")

        # Verify key on leader state machine
        get_res = self.cluster.get("alpha")
        self.assertTrue(get_res["success"])
        self.assertEqual(get_res["value"], 42)

        # Give followers a moment to catch up
        time.sleep(0.1)

        # Verify all active nodes have committed the entry
        for nid, node in self.cluster.nodes.items():
            self.assertEqual(node.commit_index, 1, f"Node {nid} commit_index mismatch")
            self.assertEqual(node.state_machine.get("alpha"), 42, f"Node {nid} KV mismatch")

    def test_sequential_multi_command_commits(self):
        for i in range(5):
            res = self.cluster.set(f"key_{i}", i * 10, timeout=2.0)
            self.assertTrue(res["success"], f"Failed on key_{i}")

        time.sleep(0.1)

        leader = self.cluster.get_leader()
        self.assertIsNotNone(leader)
        self.assertEqual(leader.commit_index, 5)

        for i in range(5):
            val = leader.state_machine.get(f"key_{i}")
            self.assertEqual(val, i * 10)


if __name__ == "__main__":
    unittest.main()
