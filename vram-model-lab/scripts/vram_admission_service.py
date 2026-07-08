#!/usr/bin/env python3
"""Minimal HTTP service exposing the VRAM resolution cascade + mutating-admission logic.

Endpoints:
  POST /predict  -> resolve(pod) result JSON (for a Rust/Go webhook to consume)
  POST /admit    -> AdmissionReview response with a base64 JSONPatch (a webhook itself)

This is the delivery layer for the VRAM->DRA wedge. The model math + cascade live in
vram_resolver; this only wires HTTP. For a production MutatingWebhookConfiguration, front it
with TLS (or relay through ksolver's existing Rust admission handler). Fails open on errors.

Run: KUBECONFIG=~/.kube/wsl /tmp/vram-venv/bin/python vram_admission_service.py --port 8091
"""
from __future__ import annotations

import argparse
import json
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer

import vram_admission as va
import vram_resolver as vr


def _make_handler(observations, config_docs_fn):
    def resolve_fn(pod):
        return vr.resolve(pod, observations=observations)

    class Handler(BaseHTTPRequestHandler):
        def log_message(self, *args):  # quiet
            pass

        def _send(self, obj, code=200):
            body = json.dumps(obj).encode()
            self.send_response(code)
            self.send_header("Content-Type", "application/json")
            self.send_header("Content-Length", str(len(body)))
            self.end_headers()
            self.wfile.write(body)

        def do_POST(self):
            length = int(self.headers.get("Content-Length", 0))
            raw = self.rfile.read(length) if length else b"{}"
            try:
                payload = json.loads(raw or b"{}")
            except json.JSONDecodeError:
                self._send({"error": "invalid json"}, 400)
                return
            if self.path.rstrip("/") == "/predict":
                self._send(resolve_fn(payload))
            elif self.path.rstrip("/") == "/admit":
                self._send(va.render_admission_response(payload, resolve_fn))
            else:
                self._send({"error": "not found"}, 404)

    return Handler


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("--port", type=int, default=8091)
    parser.add_argument("--observations", help="JSONL observation store for the fingerprint tier")
    args = parser.parse_args()

    observations = vr.load_observations(args.observations) if args.observations else None
    server = ThreadingHTTPServer(("127.0.0.1", args.port), _make_handler(observations, None))
    print(f"vram admission service on 127.0.0.1:{args.port} (/predict, /admit)")
    server.serve_forever()
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
