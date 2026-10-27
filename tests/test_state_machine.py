"""Unit tests for the Deterministic Key-Value State Machine."""
import unittest

from chronoraft.state_machine import KeyValueStateMachine


class TestStateMachine(unittest.TestCase):
    def setUp(self):
        self.sm = KeyValueStateMachine()

    def test_set_and_get(self):
        ok, res = self.sm.apply(1, {"op": "SET", "key": "user", "value": "obito"})
        self.assertTrue(ok)
        self.assertEqual(res, "obito")
        self.assertEqual(self.sm.get("user"), "obito")

    def test_incr_operation(self):
        self.sm.apply(1, {"op": "SET", "key": "counter", "value": 10})
        ok, res = self.sm.apply(2, {"op": "INCR", "key": "counter", "delta": 5})
        self.assertTrue(ok)
        self.assertEqual(res, 15)
        self.assertEqual(self.sm.get("counter"), 15)

    def test_del_operation(self):
        self.sm.apply(1, {"op": "SET", "key": "temp", "value": "xyz"})
        ok, res = self.sm.apply(2, {"op": "DEL", "key": "temp"})
        self.assertTrue(ok)
        self.assertIsNone(self.sm.get("temp"))

    def test_cas_operation(self):
        self.sm.apply(1, {"op": "SET", "key": "lock", "value": "free"})
        # Successful CAS
        ok, res = self.sm.apply(2, {"op": "CAS", "key": "lock", "expected": "free", "value": "acquired"})
        self.assertTrue(ok)
        self.assertEqual(self.sm.get("lock"), "acquired")
        # Failing CAS
        ok2, res2 = self.sm.apply(3, {"op": "CAS", "key": "lock", "expected": "free", "value": "failed"})
        self.assertFalse(ok2)
        self.assertEqual(self.sm.get("lock"), "acquired")

    def test_snapshot_and_restore(self):
        self.sm.apply(1, {"op": "SET", "key": "k1", "value": "v1"})
        self.sm.apply(2, {"op": "SET", "key": "k2", "value": "v2"})
        snap = self.sm.snapshot()

        new_sm = KeyValueStateMachine()
        new_sm.restore(snap)
        self.assertEqual(new_sm.get("k1"), "v1")
        self.assertEqual(new_sm.get("k2"), "v2")
        self.assertEqual(new_sm.last_applied_index, 2)


if __name__ == "__main__":
    unittest.main()
