"""Unit tests for Raft Leader Election, Heartbeats, and Failover."""
import time
import unittest

from chronoraft.cluster import RaftCluster
from chronoraft.models import Role


class TestLeaderElection(unittest.TestCase):
    def setUp(self):
        self.cluster = RaftCluster(
            node_count=3,
            election_timeout_range=(0.10, 0.25),
            heartbeat_interval=0.04
        )
        self.cluster.start()

    def tearDown(self):
        self.cluster.stop()

    def test_initial_leader_election(self):
        leader_id = self.cluster.wait_for_leader(timeout=3.0)
        self.assertIsNotNone(leader_id, "Leader should be elected within timeout")

        leader = self.cluster.get_leader()
        self.assertIsNotNone(leader)
        self.assertEqual(leader.role, Role.LEADER)
        self.assertGreater(leader.current_term, 0)

        # Check other nodes are followers
        follower_count = sum(1 for n in self.cluster.nodes.values() if n.role == Role.FOLLOWER)
        self.assertEqual(follower_count, 2)

    def test_leader_failover_on_kill(self):
        leader_id = self.cluster.wait_for_leader(timeout=3.0)
        self.assertIsNotNone(leader_id)
        old_leader = self.cluster.nodes[leader_id]
        old_term = old_leader.current_term

        # Kill the current leader
        self.cluster.kill_node(leader_id)

        # Wait for remaining 2 nodes to elect a new leader
        deadline = time.time() + 3.0
        new_leader_id = None
        while time.time() < deadline:
            for nid, node in self.cluster.nodes.items():
                if nid != leader_id and node.role == Role.LEADER:
                    new_leader_id = nid
                    break
            if new_leader_id:
                break
            time.sleep(0.03)

        self.assertIsNotNone(new_leader_id, "New leader should be elected after old leader killed")
        self.assertNotEqual(new_leader_id, leader_id)
        new_leader = self.cluster.nodes[new_leader_id]
        self.assertGreater(new_leader.current_term, old_term)


if __name__ == "__main__":
    unittest.main()
