#!/usr/bin/env python3
"""Minimal HTTP service exposing the VRAM resolution cascade + mutating-admission logic.

Endpoints:
  POST /predict  -> resolve(pod) result JSON (for a Rust/Go webhook to consume)
  POST /admit    -> AdmissionReview response with a base64 JSONPatch (a webhook itself)
  POST /observe  -> record a completed run's measured peak (tier-4 populate)
  POST /claim    -> right-sized DRA ResourceClaimTemplate for a pod

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
    # Strip any query string — the k8s API server calls the webhook path with a "?timeout=<N>s" query,
    # so matching the raw path (with query) would 404 every real admission call. Then strip trailing /.
    path = path.split("?", 1)[0].rstrip("/")
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
        ann = meta.get("annotations") or {}
        # Target DRA API version for the cluster (k8s 1.31-1.35 serve different resource.k8s.io
        # versions). Default GA; an operator/relay can override via annotation.
        dra_version = ann.get("ksolver.ai/dra-api-version", va.GA_DRA_API_VERSION)
        name = (meta.get("name") or "pod") + "-vram"
        namespace = meta.get("namespace") or "default"
        claim = va.build_resource_claim_template(namespace, name, res["vram_gib"], api_version=dra_version)
        if claim is None:
            # Non-GA DRA has no consumable-capacity claim — the node-affinity feasibility patch
            # (/admit, /predict) still keeps the pod off too-small GPUs on this cluster.
            return 200, {
                "claim": None,
                "pod_patch": [],
                "reason": f"DRA {dra_version} has no consumable-capacity claim (GA-only, k8s 1.34+); use node-affinity feasibility instead",
                "resolution": res,
            }
        return 200, {
            "claim": claim,
            "pod_patch": va.build_resource_claim_pod_refs(payload, name),
            "resolution": res,
        }
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
    # In-cluster a MutatingWebhookConfiguration calls /admit over HTTPS from the API server, so bind
    # 0.0.0.0 and terminate TLS here. Localhost + plain HTTP stay the default for local dev/testing.
    parser.add_argument("--host", default="127.0.0.1", help="bind address (use 0.0.0.0 in-cluster)")
    parser.add_argument("--tls-cert", help="PEM cert; enables HTTPS (required for a k8s webhook)")
    parser.add_argument("--tls-key", help="PEM private key (paired with --tls-cert)")
    args = parser.parse_args()

    import os

    observations = {}
    if args.observations and os.path.exists(args.observations):
        observations = vr.load_observations(args.observations)
    server = ThreadingHTTPServer(
        (args.host, args.port), _make_handler(observations, args.observations)
    )
    scheme = "http"
    if args.tls_cert or args.tls_key:
        if not (args.tls_cert and args.tls_key):
            parser.error("--tls-cert and --tls-key must be given together")
        import ssl

        ctx = ssl.SSLContext(ssl.PROTOCOL_TLS_SERVER)
        ctx.load_cert_chain(certfile=args.tls_cert, keyfile=args.tls_key)
        server.socket = ctx.wrap_socket(server.socket, server_side=True)
        scheme = "https"
    print(
        f"vram admission service on {scheme}://{args.host}:{args.port} "
        f"(/predict, /admit, /observe, /claim)"
    )
    server.serve_forever()
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
