"""Pure standard-library HTTP REST Server and Web visualizer backend for ChronoRaft."""
from __future__ import annotations

import json
import mimetypes
import os
import urllib.parse
from http import HTTPStatus
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from typing import Any, Dict, Optional

from chronoraft.cluster import RaftCluster


class ChronoRaftHTTPHandler(BaseHTTPRequestHandler):
    """Handles REST API queries and serves static WebGL HUD assets with strict OWASP security controls."""

    cluster: RaftCluster
    web_dir: str

    def log_message(self, format: str, *args: Any) -> None:
        # Suppress noisy HTTP request logging during benchmarks
        pass

    def _send_security_headers(self) -> None:
        """Injects OWASP-compliant security headers."""
        self.send_header("X-Content-Type-Options", "nosniff")
        self.send_header("X-Frame-Options", "DENY")
        self.send_header("X-XSS-Protection", "1; mode=block")
        self.send_header("Referrer-Policy", "no-referrer")
        self.send_header(
            "Content-Security-Policy",
            "default-src 'self' 'unsafe-inline' data:; connect-src 'self' *;"
        )

    def _send_json(self, data: Any, status: int = HTTPStatus.OK) -> None:
        payload = json.dumps(data, indent=2).encode("utf-8")
        self.send_response(status)
        self.send_header("Content-Type", "application/json; charset=utf-8")
        self.send_header("Content-Length", str(len(payload)))
        self.send_header("Access-Control-Allow-Origin", "*")
        self.send_header("Access-Control-Allow-Methods", "GET, POST, OPTIONS")
        self.send_header("Access-Control-Allow-Headers", "Content-Type")
        self._send_security_headers()
        self.end_headers()
        self.wfile.write(payload)

    def do_OPTIONS(self) -> None:
        self.send_response(HTTPStatus.NO_CONTENT)
        self.send_header("Access-Control-Allow-Origin", "*")
        self.send_header("Access-Control-Allow-Methods", "GET, POST, OPTIONS")
        self.send_header("Access-Control-Allow-Headers", "Content-Type")
        self._send_security_headers()
        self.end_headers()

    def do_GET(self) -> None:
        parsed = urllib.parse.urlparse(self.path)
        path = parsed.path
        query = urllib.parse.parse_qs(parsed.query)

        if path == "/api/status":
            self._send_json(self.cluster.get_cluster_status())
            return

        if path == "/api/nodes":
            telemetry = {nid: n.get_telemetry() for nid, n in self.cluster.nodes.items()}
            self._send_json(telemetry)
            return

        if path == "/api/logs":
            logs = {}
            for nid, node in self.cluster.nodes.items():
                logs[nid] = {
                    "role": node.role.value,
                    "term": node.current_term,
                    "commit_index": node.commit_index,
                    "last_applied": node.last_applied,
                    "entries": node.storage.get_all_entries()
                }
            self._send_json(logs)
            return

        if path == "/api/events":
            limit = int(query.get("limit", [50])[0])
            self._send_json(self.cluster.transport.get_events(limit=limit))
            return

        if path == "/api/get":
            key = query.get("key", [""])[0]
            self._send_json(self.cluster.get(key))
            return

        # Serve static files from web_dir with path-traversal prevention
        self._serve_static(path)

    def do_POST(self) -> None:
        parsed = urllib.parse.urlparse(self.path)
        path = parsed.path

        content_length = int(self.headers.get("Content-Length", 0))
        body = self.rfile.read(content_length) if content_length > 0 else b"{}"
        try:
            payload = json.loads(body.decode("utf-8")) if body else {}
        except Exception:
            payload = {}

        if path == "/api/propose":
            op = payload.get("op", "SET")
            key = payload.get("key", "test_key")
            val = payload.get("value", "test_val")
            res = self.cluster.execute_command({"op": op, "key": key, "value": val})
            self._send_json(res)
            return

        if path == "/api/chaos/partition":
            groups = payload.get("groups", [])
            res = self.cluster.partition_cluster(groups)
            self._send_json(res)
            return

        if path == "/api/chaos/isolate":
            node_id = payload.get("node_id", "")
            res = self.cluster.isolate_node(node_id)
            self._send_json(res)
            return

        if path == "/api/chaos/heal":
            res = self.cluster.heal_cluster()
            self._send_json(res)
            return

        if path == "/api/chaos/kill":
            node_id = payload.get("node_id", "")
            res = self.cluster.kill_node(node_id)
            self._send_json(res)
            return

        if path == "/api/chaos/resume":
            node_id = payload.get("node_id", "")
            res = self.cluster.resume_node(node_id)
            self._send_json(res)
            return

        if path == "/api/chaos/drop":
            raw_rate = float(payload.get("rate", 0.0))
            rate = max(0.0, min(1.0, raw_rate))
            self.cluster.transport.set_drop_rate(rate)
            self._send_json({"status": "drop_rate_updated", "rate": rate})
            return

        if path == "/api/chaos/latency":
            min_ms = max(0.0, float(payload.get("min_ms", 0.0)))
            max_ms = max(min_ms, float(payload.get("max_ms", 0.0)))
            self.cluster.transport.set_latency(min_ms, max_ms)
            self._send_json({"status": "latency_updated", "min_ms": min_ms, "max_ms": max_ms})
            return

        self._send_json({"error": f"Endpoint not found: {path}"}, status=HTTPStatus.NOT_FOUND)

    def _serve_static(self, path: str) -> None:
        if path in ("", "/"):
            path = "/index.html"

        # Canonicalize and prevent path traversal
        clean_path = urllib.parse.unquote(path).lstrip("/")
        base_dir = os.path.realpath(celf.web_dir)
        full_path = os.path.realpath(os.path.join(base_dir, clean_path))

        # Strict sandbox boundary enforcement
        if not (full_path == base_dir or full_path.startswith(base_dir + os.sep)):
            self.send_error(HTTPStatus.FORBIDDEN, "Access Denied: Path traversal detected")
            return

        if not os.path.exists(full_path) or os.path.isdir(full_path):
            self.send_error(HTTPStatus.NOT_FOUND, "File not found")
            return

        mime_type, _ = mimetypes.guess_type(full_path)
        mime_type = mime_type or "application/octet-stream"

        try:
            with open(full_path, "rb") as f:
                content = f.read()
            self.send_response(HTTPStatus.OK)
            self.send_header("Content-Type", mime_type)
            self.send_header("Content-Length", str(len(content)))
            self._send_security_headers()
            self.end_headers()
            self.wfile.write(content)
        except Exception as e:
            self.send_error(HTTPStatus.INTERNAL_SERVER_ERROR, str(e))


def run_server(cluster: RaftCluster, host: str = "127.0.0.1", port: int = 8765, web_dir: Optional[str] = None) -> ThreadingHTTPServer:
    """Starts the ChronoRaft HTTP server in a non-blocking background thread."""
    if web_dir is None:
        web_dir = os.path.abspath(os.path.join(os.path.dirname(__file__), "..", "web"))

    ChronoRaftHTTPHandler.cluster = cluster
    ChronoRaftHTTPHandler.web_dir = web_dir

    server = ThreadingHTTPServer((host, port), ChronoRaftHTTPHandler)
    return server
