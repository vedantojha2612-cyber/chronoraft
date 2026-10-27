"""Unit tests for Network Partition Chaos, Split-Brain Protection, and Healing."""
import time
import unittest

from chronoraft.cluster import RaftCluster
from chronoraft.models import Role


class TestChaosPartitions(unittest.TestCase):
    def setUp(self):
        self.cluster = RaftCluster(
            node_count=5,
            election_timeout_range=(0.10, 0.25),
            heartbeat_interval=0.04
        )
        self.cluster.start()
        self.leader_id = self.cluster.wait_for_leader(timeout=3.0)
        self.assertIsNotNone(self.leader_id, "Cluster failed to elect initial leader")

    def tearDown(self):
        self.cluster.stop()

    def test_majority_minority_network_partition(self):
        """Tests that a 3-2 network partition preserves safety:
        Majority (3 nodes) can commit writes, while minority (2 nodes) cannot.
        """
        initial_leader = self.cluster.get_leader()
        self.assertIsNotNone(initial_leader)

        # Baseline write before partition
        w1 = self.cluster.set("epoch", "pre-partition", timeout=2.0)
        self.assertTrue(w1["success"])

        # Determine partition groups: ensure initial leader is in minority or majority
        all_ids = list(self.cluster.node_ids)
        majority_group = all_ids[:3]
        minority_group = all_ids[3:]

        self.cluster.partition_cluster([majority_group, minority_group])
        time.sleep(0.5)

        # 1. Test write on majority group
        majority_leader = None
        for nid in majority_group:
            node = self.cluster.nodes[nid]
            if node.role == Role.LEADER:
                majority_leader = node
                break

        if not majority_leader:
            deadline = time.time() + 2.0
            while time.time() < deadline:
                for nid in majority_group:
                    node = self.cluster.nodes[nid]
                    if node.role == Role.LEADER:
                        majority_leader = node
                        break
                if majority_leader:
                    break
                time.sleep(0.05)

        self.assertIsNotNone(majority_leader, "Majority group should elect/maintain a leader")

        # Propose to majority leader
        ok_maj, res_maj, _ = majority_leader.propose({"op": "SET", "key": "quorum_key", "value": "majority_win"}, timeout=1.5)
        self.assertTrue(ok_maj, "Majority group should successfully commit write")

        # 2. Test minority group cannot commit write
        minority_node = self.cluster.nodes[minority_group[0]]
        ok_min, res_min, _ = minority_node.propose({"op": "SET", "key": "ghost_key", "value": "split_brain_attempt"}, timeout=0.6)
        self.assertFalse(ok_min, "Minority group must NOT be able to commit writes")

        # 3. Heal network partition
        self.cluster.heal_cluster()
        time.sleep(0.5)

        # Re-test cluster-wide write after healing
        w_healed = self.cluster.set("post_heal", "all_nodes_synced", timeout=2.0)
        self.assertTrue(w_healed["success"], "Post-heal writes must succeed")

        time.sleep(0.2)
        # All 5 nodes must have the post-heal key
        for nid, node in self.cluster.nodes.items():
            self.assertEqual(node.state_machine.get("post_heal"), "all_nodes_synced", f"Node {nid} missing synced key")


if __name__ == "__main__":
    unittest.main()
