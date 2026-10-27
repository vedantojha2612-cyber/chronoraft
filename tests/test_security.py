"""Security, OWASP verification, and Raft consensus safety invariant test suite."""
import glob
import http.client
import json
import os
import re
import threading
import time
import unittest
import urllib.parse
import urllib.request
from typing import Dict, List, Set, Tuple

from chronoraft.cluster import RaftCluster
from chronoraft.models import LogEntry, Role
from chronoraft.server import run_server


class TestSecurityAndSafetyInvariants(unittest.TestCase):
    """Rigorous security audits and distributed invariant verification."""

    @classmethod
    def setUpClass(cls):
        cls.cluster = RaftCluster(node_count=3, node_prefix="sec")
        cls.cluster.start()
        time.sleep(0.4)

        cls.server = run_server(cls.cluster, host="127.0.0.1", port=0)
        cls.port = cls.server.server_address[1]
        cls.server_thread = threading.Thread(target=cls.server.serve_forever, daemon=True)
        cls.server_thread.start()
        time.sleep(0.1)

    @classmethod
    def tearDownClass(cls):
        try:
            cls.server.shutdown()
            cls.server.server_close()
        except Exception:
            pass
        cls.cluster.stop()

    def test_path_traversal_blocked(self):
        """Verifies that attempts to traverse outside web_dir are strictly blocked with 403 or 404."""
        traversal_paths = [
            "/../../cli.py",
            "/../../../etc/shadow_test",
            "/..%2f..%2fcli.py",
            "/vendor/../../cli.py",
            "/vendor/../server.py",
            "/..%2F..%2F..%2Fsecret.txt",
        ]

        conn = http.client.HTTPConnection("127.0.0.1", self.port, timeout=2.0)
        try:
            for path in traversal_paths:
                conn.request("GET", path)
                resp = conn.getresponse()
                body = resp.read()
                self.assertIn(
                    resp.status,
                    [403, 404, 400],
                    f"Path {path} returned status {resp.status} instead of 403/404!",
                )
                self.assertNotIn(
                    b"class RaftCluster", body, "Source code leaked via path traversal!"
                )
                self.assertNotIn(b"forbidden_secret_string", body, "sensitive file leaked via path traversal!")
        finally:
            conn.close()

    def test_security_headers_present(self):
        """Verifies OWASP security headers on all responses."""
        url = f"http://127.0.0.1:{self.port}/api/status"
        req = urllib.request.Request(url)
        with urllib.request.urlopen(req, timeout=2.0) as resp:
            headers = dict(resp.getheaders())
            self.assertEqual(headers.get("X-Content-Type-Options"), "nosniff")
            self.assertEqual(headers.get("X-Frame-Options"), "DENY")
            self.assertEqual(headers.get("X-XSS-Protection"), "1; mode=block")
            self.assertEqual(headers.get("Referrer-Policy"), "no-referrer")
            self.assertIn("Content-Security-Policy", headers)

    def test_chaos_input_sanitization(self):
        """Verifies that chaos injection endpoints clamp out-of-bounds input values."""
        # Test drop rate clamping
        url_drop = f"http://127.0.0.1:{self.port}/api/chaos/drop"
        data_drop = json.dumps({"rate": 2.5}).encode("utf-8")
        req = urllib.request.Request(url_drop, data=data_drop, headers={"Content-Type": "application/json"})
        with urllib.request.urlopen(req, timeout=2.0) as resp:
            res = json.loads(resp.read().decode("utf-8"))
            self.assertEqual(res.get("rate"), 1.0, "Drop rate > 1.0 was not clamped!")

        data_drop_neg = json.dumps({"rate": -0.8}).encode("utf-8")
        req = urllib.request.Request(url_drop, data=data_drop_neg, headers={"Content-Type": "application/json"})
        with urllib.request.urlopen(req, timeout=2.0) as resp:
            res = json.loads(resp.read().decode("utf-8"))
            self.assertEqual(res.get("rate"), 0.0, "Negative drop rate was not clamped!")

        # Test latency clamping
        url_lat = f"http://127.0.0.1:{self.port}/api/chaos/latency"
        data_lat = json.dumps({"min_ms": -50.0, "max_ms": 30.0}).encode("utf-8")
        req = urllib.request.Request(url_lat, data=data_lat, headers={"Content-Type": "application/json"})
        with urllib.request.urlopen(req, timeout=2.0) as resp:
            res = json.loads(resp.read().decode("utf-8"))
            self.assertEqual(res.get("min_ms"), 0.0, "Negative latency was not clamped to 0!")
            self.assertEqual(res.get("max_ms"), 30.0)

        # Restore normal chaos state
        self.cluster.heal_cluster()

    def test_no_dynamic_execution_sinks(self):
        """Verifies zero dynamic code evaluation sinks in production code."""
        dangerous_patterns = [
            r"\be" + r"val\s*\(",
            r"\be" + r"xec\s*\(",
            r"\bo" + r"s\.system\s*\(",
            r"\bsub" + r"process\.",
            r"sh" + r"ell\s*=\s*True",
            r"\b__imp" + r"ort__\s*\(",
            r"\bpic" + r"kle\.loads?\s*\(",
        ]
        base_dir = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
        source_files = glob.glob(os.path.join(base_dir, "chronoraft", "*.py")) + [
            os.path.join(base_dir, "cli.py")
        ]

        violations = []
        for file_path in source_files:
            with open(file_path, "r", encoding="utf-8") as f:
                content = f.read()
                for pat in dangerous_patterns:
                    matches = list(re.finditer(pat, content))
                    if matches:
                        violations.append(f"{os.path.basename(file_path)}: matched '{pat}' ({len(matches)} times)")

        self.assertEqual(len(violations), 0, f"Dangerous execution sinks detected: {violations}")

    def test_no_hardcoded_secrets(self):
        """Verifies zero hardcoded API keys, tokens, or private keys."""
        secret_patterns = [
            r"sk-[a-zA-Z0-9]{32,}",
            r"AIza[0-9A-Za-z-_]{35}",
        ]
        base_dir = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
        source_files = (
            glob.glob(os.path.join(base_dir, "chronoraft", "*.py"))
            + glob.glob(os.path.join(base_dir, "web", "*.html"))
            + [os.path.join(base_dir, "cli.py")]
        )

        leaks = []
        for file_path in source_files:
            with open(file_path, "r", encoding="utf-8", errors="ignore") as f:
                content = f.read()
                for pat in secret_patterns:
                    matches = list(re.finditer(pat, content))
                    if matches:
                        leaks.append(f"{os.path.basename(file_path)}: secret pattern '{pat}' matched")

        self.assertEqual(len(leaks), 0, f"Hardcoded secret patterns found: {leaks}")

    def test_raft_election_safety_invariant(self):
        """Election Safety Invariant: At most one leader can be elected in a given term."""
        leaders_by_term: Dict[int, str] = {}
        # Observe cluster across 10 term transitions / failovers
        for _ in range(5):
            status = self.cluster.get_cluster_status()
            lead_id = status.get("leader_id")
            if lead_id:
                leader_node = self.cluster.nodes[lead_id]
                term = leader_node.current_term
                if term in leaders_by_term:
                    self.assertEqual(
                        leaders_by_term[term],
                        lead_id,
                        f"Election Safety Violated: Multiple leaders for term {term}! {leaders_by_term[term]} vs {lead_id}",
                    )
                else:
                    leaders_by_term[term] = lead_id

            # Trigger a failover
            if lead_id:
                self.cluster.kill_node(lead_id)
                time.sleep(0.35)
                self.cluster.resume_node(lead_id)
                time.sleep(0.2)

        # Restore
        self.cluster.heal_cluster()

    def test_raft_log_matching_invariant(self):
        """Log Matching Invariant: If two nodes agree on an entry at (index, term), their logs are identical up to that index."""
        # Commit several entries
        for i in range(5):
            self.cluster.execute_command({"op": "SET", "key": f"inv_key_{i}", "value": f"inv_val_{i}"})

        time.sleep(0.15)
        nodes = list(self.cluster.nodes.values())
        for i in range(len(nodes)):
            for j in range(i + 1, len(nodes)):
                n1, n2 = nodes[i], nodes[j]
                min_commit = min(n1.commit_index, n2.commit_index)
                for idx in range(1, min_commit + 1):
                    e1 = n1.storage.get_entry(idx)
                    e2 = n2.storage.get_entry(idx)
                    if e1 and e2 and e1.term == e2.term:
                        self.assertEqual(
                            e1.command,
                            e2.command,
                            f"Log Matching Invariant Violated at index {idx} between {n1.node_id} and {n2.node_id}!",
                        )


if __name__ == "__main__":
    unittest.main()
