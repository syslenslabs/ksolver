"""Unit tests for the predictor service request routing (no HTTP needed)."""
from __future__ import annotations

import base64
import json
import unittest

import vram_admission_service as svc


def gpu_pod(annotations=None, args=None):
    return {
        "metadata": {"annotations": annotations or {}},
        "spec": {"containers": [{"name": "t", "image": "i", "args": args or [],
                                 "resources": {"limits": {"nvidia.com/gpu": "1"}}}]},
    }


class RouteTest(unittest.TestCase):
    def test_predict_returns_resolution(self):
        status, body = svc.route("/predict", gpu_pod({"ksolver.dev/predicted-peak-vram-gib": "20"}), {})
        self.assertEqual(status, 200)
        self.assertEqual(body["source"], "explicit-annotation")
        self.assertEqual(body["vram_gib"], 20.0)

    def test_admit_returns_admissionreview_with_patch(self):
        review = {"request": {"uid": "u", "operation": "CREATE",
                              "object": gpu_pod({"ksolver.dev/predicted-peak-vram-gib": "20"})}}
        status, body = svc.route("/admit", review, {})
        self.assertEqual(status, 200)
        self.assertTrue(body["response"]["allowed"])
        ops = json.loads(base64.b64decode(body["response"]["patch"]))
        self.assertTrue(any("predicted-peak-vram" in o["path"] for o in ops))

    def test_observe_indexes_and_reports_samples(self):
        obs: dict = {}
        pod = gpu_pod(args=["--x", "1"])
        for peak in (5000, 5100, 5200):
            status, body = svc.route("/observe", {"pod": pod, "peak_mib": peak}, obs)
            self.assertEqual(status, 200)
            self.assertTrue(body["recorded"])
        self.assertEqual(body["samples"], 3)
        # and now /predict resolves via tier 4
        _, pred = svc.route("/predict", pod, obs)
        self.assertEqual(pred["source"], "historical-fingerprint")

    def test_observe_rejects_bad_peak(self):
        status, body = svc.route("/observe", {"pod": gpu_pod(), "peak_mib": "nope"}, {})
        self.assertEqual(status, 400)
        self.assertIn("error", body)

    def test_claim_returns_sized_dra_template(self):
        pod = gpu_pod({"ksolver.dev/predicted-peak-vram-gib": "17.3"})
        pod["metadata"]["name"] = "job"
        pod["metadata"]["namespace"] = "team"
        status, body = svc.route("/claim", pod, {})
        self.assertEqual(status, 200)
        rct = body["claim"]
        self.assertEqual(rct["kind"], "ResourceClaimTemplate")
        self.assertEqual(rct["metadata"], {"name": "job-vram", "namespace": "team"})
        self.assertEqual(
            rct["spec"]["spec"]["devices"]["requests"][0]["exactly"]["capacity"]["requests"]["memory"],
            "18Gi",  # ceil(17.3)
        )

    def test_claim_null_when_no_estimate(self):
        status, body = svc.route("/claim", gpu_pod(args=["--epochs", "3"]), {})
        self.assertEqual(status, 200)
        self.assertIsNone(body["claim"])

    def test_claim_degrades_on_non_ga_dra_cluster(self):
        # On a k8s 1.31-1.33 cluster (pre-GA DRA), the operator/relay flags the served version;
        # no consumable-capacity claim is emitted, and the reason points to node-affinity feasibility.
        pod = gpu_pod({
            "ksolver.dev/predicted-peak-vram-gib": "17.3",
            "ksolver.ai/dra-api-version": "resource.k8s.io/v1beta1",
        })
        pod["metadata"]["name"] = "job"
        status, body = svc.route("/claim", pod, {})
        self.assertEqual(status, 200)
        self.assertIsNone(body["claim"])
        self.assertEqual(body["pod_patch"], [])
        self.assertIn("node-affinity", body["reason"])
        # the estimate itself still resolved (feasibility can still be enforced via affinity)
        self.assertEqual(body["resolution"]["vram_gib"], 17.3)

    def test_unknown_path_404(self):
        status, body = svc.route("/nope", {}, {})
        self.assertEqual(status, 404)


if __name__ == "__main__":
    unittest.main()
