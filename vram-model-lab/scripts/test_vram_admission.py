"""Unit tests for VRAM admission patch + DRA claim generation."""
from __future__ import annotations

import unittest

import vram_admission as va


def pod(annotations=None):
    return {"metadata": {"annotations": annotations or {}}, "spec": {"containers": []}}


class AdmissionPatchTest(unittest.TestCase):
    def _find_ann(self, patches, key):
        esc = va._escape(key)
        for p in patches:
            if p["path"] == f"/metadata/annotations/{esc}":
                return p["value"]
        return None

    def _affinity(self, patches):
        for p in patches:
            if p["path"] == "/spec/affinity":
                return p["value"]
        return None

    def test_high_confidence_sets_annotations_and_node_affinity(self):
        res = {"vram_gib": 18.0, "source": "static-sniff+model", "confidence": "high", "hard": True}
        patches = va.build_admission_patch(pod(), res)
        self.assertEqual(self._find_ann(patches, "ksolver.dev/predicted-peak-vram-gib"), "18.0")
        self.assertEqual(self._find_ann(patches, "ksolver.dev/predicted-peak-vram-source"), "static-sniff+model")
        aff = self._affinity(patches)
        self.assertIsNotNone(aff)
        terms = aff["nodeAffinity"]["requiredDuringSchedulingIgnoredDuringExecution"]["nodeSelectorTerms"]
        # OR'd: GiB label term + MiB label term
        gib = terms[0]["matchExpressions"][0]
        self.assertEqual(gib["key"], va.NODE_VRAM_LABEL)
        self.assertEqual(gib["operator"], "Gt")
        self.assertEqual(gib["values"], ["17"])  # floor(18)-1
        mib = terms[1]["matchExpressions"][0]
        self.assertEqual(mib["key"], va.NODE_VRAM_LABEL_MIB)
        self.assertEqual(mib["values"], ["17408"])  # 17 * 1024

    def test_explanation_annotation_emitted_when_present(self):
        res = {"vram_gib": 18.0, "source": "static-sniff+model", "confidence": "high", "hard": True,
               "explanation": "predicted 18 GiB from transformer"}
        patches = va.build_admission_patch(pod(), res)
        self.assertEqual(
            self._find_ann(patches, "ksolver.dev/predicted-peak-vram-explanation"),
            "predicted 18 GiB from transformer",
        )

    def test_advisory_annotates_but_no_affinity(self):
        res = {"vram_gib": None, "source": "unknown", "confidence": "advisory", "hard": False}
        patches = va.build_admission_patch(pod(), res)
        self.assertIsNone(self._affinity(patches))
        self.assertEqual(self._find_ann(patches, "ksolver.dev/predicted-peak-vram-advisory"), "true")

    def test_creates_annotations_object_when_absent(self):
        p = {"metadata": {}, "spec": {"containers": []}}
        patches = va.build_admission_patch(p, {"vram_gib": 10.0, "source": "x", "confidence": "high", "hard": True})
        self.assertEqual(patches[0], {"op": "add", "path": "/metadata/annotations", "value": {}})

    def test_does_not_clobber_existing_annotations(self):
        patches = va.build_admission_patch(
            pod(annotations={"existing": "keep"}),
            {"vram_gib": 10.0, "source": "x", "confidence": "high", "hard": True},
        )
        self.assertNotEqual(patches[0]["path"], "/metadata/annotations")  # no clobbering add

    def test_render_admission_response_encodes_patch(self):
        import base64 as _b64
        import json as _json

        review = {
            "request": {
                "uid": "abc-123",
                "object": {
                    "metadata": {"annotations": {}},
                    "spec": {"containers": [{"name": "t", "resources": {"limits": {"nvidia.com/gpu": "1"}}}]},
                },
            }
        }
        resp = va.render_admission_response(
            review, lambda pod: {"vram_gib": 12.0, "source": "x", "confidence": "high", "hard": True}
        )
        r = resp["response"]
        self.assertEqual(r["uid"], "abc-123")
        self.assertTrue(r["allowed"])
        self.assertEqual(r["patchType"], "JSONPatch")
        patches = _json.loads(_b64.b64decode(r["patch"]))
        self.assertTrue(any(p["path"] == "/spec/affinity" for p in patches))

    def test_render_admission_response_fails_open(self):
        review = {"request": {"uid": "u", "object": {"spec": {"containers": [{"name": "t"}]}}}}

        def boom(pod):
            raise RuntimeError("resolver down")

        resp = va.render_admission_response(review, boom)
        self.assertTrue(resp["response"]["allowed"])
        self.assertNotIn("patch", resp["response"])  # admitted unchanged

    def test_resource_claim_pod_refs_wire_claim_to_gpu_container(self):
        p = {"spec": {"containers": [
            {"name": "side", "resources": {}},
            {"name": "trainer", "resources": {"limits": {"nvidia.com/gpu": "1"}}},
        ]}}
        ops = va.build_resource_claim_pod_refs(p, "job-vram")
        rc = next(o for o in ops if o["path"] == "/spec/resourceClaims")
        self.assertEqual(rc["value"][0]["resourceClaimTemplateName"], "job-vram")
        # references the GPU container (index 1), not the sidecar
        self.assertTrue(any(o["path"] == "/spec/containers/1/resources/claims" for o in ops))

    def test_resource_claim_template_sized_to_estimate(self):
        rct = va.build_resource_claim_template("ns", "job-vram", 17.3)
        self.assertEqual(rct["kind"], "ResourceClaimTemplate")
        self.assertEqual(rct["metadata"]["namespace"], "ns")
        self.assertEqual(rct["apiVersion"], "resource.k8s.io/v1")
        req = rct["spec"]["spec"]["devices"]["requests"][0]["exactly"]
        self.assertEqual(req["capacity"]["requests"]["memory"], "18Gi")  # ceil(17.3)

    def test_resource_claim_template_none_for_non_ga_dra_versions(self):
        # Consumable-capacity claims are GA-only; pre-GA versions (1.31-1.33) get no claim so the
        # caller degrades to node-affinity feasibility.
        for v in ("resource.k8s.io/v1beta1", "resource.k8s.io/v1alpha3", "resource.k8s.io/v1beta2"):
            self.assertIsNone(
                va.build_resource_claim_template("ns", "j", 10.0, api_version=v),
                f"{v} should not emit a consumable-capacity claim",
            )
        # GA still emits.
        self.assertIsNotNone(va.build_resource_claim_template("ns", "j", 10.0))


if __name__ == "__main__":
    unittest.main()
