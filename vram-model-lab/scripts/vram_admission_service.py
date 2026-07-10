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


def route(path, payload, observations, store_path=None):
    """Pure request router -> (status_code, response_dict). Testable without HTTP.

    /predict {pod}          -> resolution
    /admit   {request:...}  -> AdmissionReview response (base64 JSONPatch)
    /observe {pod,peak_mib} -> index + persist an observation (tier-4 populate)
    """
    path = path.rstrip("/")
    if path == "/predict":
        return 200, vr.resolve(payload, observations=observations)
    if path == "/admit":
        return 200, va.render_admission_response(
            payload, lambda pod: vr.resolve(pod, observations=observations)
        )
    if path == "/claim":
        # Return the right-sized DRA ResourceClaimTemplate for a pod (the DRA-native artifact an
        # operator/GitOps flow applies alongside the pod). Null when there's no confident estimate.
        res = vr.resolve(payload, observations=observations)
        if res.get("vram_gib") is None:
            return 200, {"claim": None, "reason": f"{res['source']}/{res['confidence']}: no VRAM estimate", "resolution": res}
        meta = payload.get("metadata") or {}
        name = (meta.get("name") or "pod") + "-vram"
        namespace = meta.get("namespace") or "default"
        return 200, {"claim": va.build_resource_claim_template(namespace, name, res["vram_gib"]), "resolution": res}
    if path == "/observe":
        pod = payload.get("pod") or {}
        try:
            peak = float(payload.get("peak_mib"))
        except (TypeError, ValueError):
            return 400, {"error": "peak_mib must be a number"}
        key = vr.index_observation(observations, pod, peak)
        if store_path:
            vr.record_observation(store_path, pod, peak)
        return 200, {"recorded": True, "key": key, "samples": len(observations[key])}
    return 404, {"error": "not found"}


def _make_handler(observations, store_path):
    observations = observations if observations is not None else {}

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
            status, body = route(self.path, payload, observations, store_path)
            self._send(body, status)

    return Handler


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("--port", type=int, default=8091)
    parser.add_argument("--observations", help="JSONL observation store for the fingerprint tier")
    args = parser.parse_args()

    import os

    observations = {}
    if args.observations and os.path.exists(args.observations):
        observations = vr.load_observations(args.observations)
    server = ThreadingHTTPServer(
        ("127.0.0.1", args.port), _make_handler(observations, args.observations)
    )
    print(f"vram admission service on 127.0.0.1:{args.port} (/predict, /admit, /observe, /claim)")
    server.serve_forever()
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
