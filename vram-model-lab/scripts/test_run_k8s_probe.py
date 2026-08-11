"""Unit tests for the VRAM probe manifest builder — focus on the cross-SKU nodeSelector/toleration
plumbing (roadmap F1: run the same probe matrix on each SKU's node pool)."""
from __future__ import annotations

import unittest

import run_k8s_probe as probe


SCENARIO = {"name": "smoke-mlp", "family": "mlp", "precision": "fp32", "batch_size": 8}


def pod_spec(manifest):
    return manifest["spec"]["template"]["spec"]


class BuildManifestTest(unittest.TestCase):
    def test_default_has_no_node_selector_or_tolerations(self):
        spec = pod_spec(probe.build_manifest(SCENARIO, "img:1", "default"))
        self.assertNotIn("nodeSelector", spec)
        self.assertNotIn("tolerations", spec)

    def test_requests_one_gpu_by_default(self):
        spec = pod_spec(probe.build_manifest(SCENARIO, "img:1", "default"))
        resources = spec["containers"][0]["resources"]
        self.assertEqual(resources["requests"], {"nvidia.com/gpu": "1"})
        self.assertEqual(resources["limits"], {"nvidia.com/gpu": "1"})

    def test_declared_vram_fingerprint_is_copied_to_pod_annotations(self):
        scenario = {**SCENARIO, "hidden_size": 1024, "layers": 8, "seq_len": 2048,
                    "param_count": 166_337_792}
        manifest = probe.build_manifest(scenario, "img:1", "default")
        annotations = manifest["spec"]["template"]["metadata"]["annotations"]
        self.assertEqual(annotations["ksolver.ai/vram-param-count"], "166337792")
        self.assertEqual(annotations["ksolver.ai/vram-family"], "mlp")

    def test_cli_node_selector_and_gpu_toleration(self):
        spec = pod_spec(
            probe.build_manifest(
                SCENARIO,
                "img:1",
                "default",
                node_selector={"cloud.google.com/gke-accelerator": "nvidia-tesla-t4"},
                tolerate_gpu=True,
            )
        )
        self.assertEqual(
            spec["nodeSelector"], {"cloud.google.com/gke-accelerator": "nvidia-tesla-t4"}
        )
        self.assertEqual(
            spec["tolerations"],
            [{"key": "nvidia.com/gpu", "operator": "Exists", "effect": "NoSchedule"}],
        )

    def test_cli_selector_overrides_scenario_selector(self):
        scenario = {**SCENARIO, "node_selector": {"sku": "a100", "zone": "z1"}}
        spec = pod_spec(
            probe.build_manifest(scenario, "img:1", "default", node_selector={"sku": "t4"})
        )
        # CLI wins on conflict (sku), scenario-only keys survive (zone).
        self.assertEqual(spec["nodeSelector"], {"sku": "t4", "zone": "z1"})

    def test_scenario_selector_used_when_no_cli(self):
        scenario = {**SCENARIO, "node_selector": {"sku": "l4"}}
        spec = pod_spec(probe.build_manifest(scenario, "img:1", "default"))
        self.assertEqual(spec["nodeSelector"], {"sku": "l4"})


class ParseKeyValueTest(unittest.TestCase):
    def test_parses_pairs_last_wins(self):
        self.assertEqual(
            probe.parse_key_value_pairs(["a=1", "b=2", "a=3"]), {"a": "3", "b": "2"}
        )

    def test_empty_list_is_empty_dict(self):
        self.assertEqual(probe.parse_key_value_pairs([]), {})

    def test_rejects_missing_equals(self):
        with self.assertRaises(SystemExit):
            probe.parse_key_value_pairs(["nope"])

    def test_rejects_empty_key(self):
        with self.assertRaises(SystemExit):
            probe.parse_key_value_pairs(["=value"])


if __name__ == "__main__":
    unittest.main()
