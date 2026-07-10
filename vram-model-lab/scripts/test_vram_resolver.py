"""Unit tests for the VRAM resolution cascade (tiers A: explicit + static-sniff)."""
from __future__ import annotations

import os
import tempfile
import unittest

import vram_resolver as vr


def gpu_pod(annotations=None, command=None, args=None, env=None):
    return {
        "metadata": {"annotations": annotations or {}},
        "spec": {
            "containers": [
                {
                    "name": "trainer",
                    "image": "pytorch/pytorch:2.5.1-cuda12.4-cudnn9-runtime",
                    "command": command or [],
                    "args": args or [],
                    "env": env or [],
                    "resources": {"limits": {"nvidia.com/gpu": "1"}},
                }
            ]
        },
    }


class ResolveCascadeTest(unittest.TestCase):
    def test_tier1_explicit_gib_is_authoritative_and_hard(self):
        pod = gpu_pod(annotations={"ksolver.dev/predicted-peak-vram-gib": "40"})
        r = vr.resolve(pod)
        self.assertEqual(r["source"], "explicit-annotation")
        self.assertEqual(r["confidence"], "authoritative")
        self.assertTrue(r["hard"])
        self.assertAlmostEqual(r["vram_gib"], 40.0, places=2)

    def test_tier1_explicit_bytes(self):
        pod = gpu_pod(annotations={"ksolver.dev/predicted-peak-vram-bytes": str(20 * 1024 * 1024 * 1024)})
        r = vr.resolve(pod)
        self.assertEqual(r["confidence"], "authoritative")
        self.assertAlmostEqual(r["vram_gib"], 20.0, places=1)

    def test_tier2_static_sniff_predicts_high_and_hard(self):
        # family/hidden/layers via annotations; batch/seq via CLI args -> full feature set.
        pod = gpu_pod(
            annotations={
                "ksolver.ai/vram-family": "transformer",
                "ksolver.ai/vram-hidden-size": "1024",
                "ksolver.ai/vram-layers": "12",
            },
            args=["--batch-size", "8", "--seq-len", "512", "--precision", "fp16"],
        )
        r = vr.resolve(pod)
        self.assertEqual(r["source"], "static-sniff+model")
        self.assertEqual(r["confidence"], "high")
        self.assertTrue(r["hard"])
        self.assertIsNotNone(r["vram_gib"])
        self.assertGreater(r["vram_mib"], 0.0)
        self.assertEqual(r["missing"], [])

    def test_extrapolated_prediction_is_downgraded_to_advisory(self):
        # A very large transformer extrapolates far beyond training -> implausible single-GPU VRAM.
        pod = gpu_pod(
            annotations={
                "ksolver.ai/vram-family": "transformer",
                "ksolver.ai/vram-hidden-size": "8192",
                "ksolver.ai/vram-layers": "80",
            },
            args=["--batch-size", "64", "--seq-len", "8192"],
        )
        r = vr.resolve(pod)
        self.assertEqual(r["source"], "static-sniff+model")
        self.assertEqual(r["confidence"], "advisory")
        self.assertFalse(r["hard"])  # never a hard constraint on a wild extrapolation
        self.assertIn("guard", r)

    def test_tier_unknown_when_hints_missing_is_advisory_not_hard(self):
        pod = gpu_pod(args=["--epochs", "3"])  # nothing useful
        r = vr.resolve(pod)
        self.assertEqual(r["source"], "unknown")
        self.assertEqual(r["confidence"], "advisory")
        self.assertFalse(r["hard"])
        self.assertIsNone(r["vram_mib"])
        self.assertIn("family", r["missing"])

    def test_tier4_historical_fingerprint_hit_is_high_and_uses_p95(self):
        pod = gpu_pod(args=["--epochs", "3"])  # unsniffable, but seen before
        key = vr.fingerprint_key(vr.pod_fingerprint(pod))
        obs = {key: [10000.0, 11000.0, 12000.0, 13000.0]}
        r = vr.resolve(pod, observations=obs)
        self.assertEqual(r["source"], "historical-fingerprint")
        self.assertEqual(r["confidence"], "high")
        self.assertTrue(r["hard"])
        self.assertEqual(r["observation_samples"], 4)
        self.assertGreaterEqual(r["vram_mib"], 12000.0)  # ~p95 of the samples

    def test_tier4_below_min_samples_falls_through(self):
        pod = gpu_pod(args=["--epochs", "3"])
        key = vr.fingerprint_key(vr.pod_fingerprint(pod))
        obs = {key: [10000.0, 11000.0]}  # only 2 < min 3
        r = vr.resolve(pod, observations=obs)
        self.assertNotEqual(r["source"], "historical-fingerprint")
        self.assertEqual(r["source"], "unknown")  # nothing else resolvable

    def test_tier4_measured_beats_sniffed(self):
        # Even with full sniffable hints, a strong observation history wins.
        pod = gpu_pod(
            annotations={
                "ksolver.ai/vram-family": "transformer",
                "ksolver.ai/vram-hidden-size": "1024",
                "ksolver.ai/vram-layers": "12",
            },
            args=["--batch-size", "8", "--seq-len", "512"],
        )
        key = vr.fingerprint_key(vr.pod_fingerprint(pod))
        obs = {key: [9000.0, 9000.0, 9000.0]}
        r = vr.resolve(pod, observations=obs)
        self.assertEqual(r["source"], "historical-fingerprint")
        self.assertAlmostEqual(r["vram_mib"], 9000.0, places=1)

    def test_config_docs_extract_hints_deepspeed_and_hf(self):
        deepspeed = {"train_micro_batch_size_per_gpu": 8, "fp16": {"enabled": True}}
        hf = {"per_device_train_batch_size": 16, "max_seq_length": 512, "bf16": True}
        h1 = vr.hints_from_config_docs([deepspeed])
        self.assertEqual(str(h1["batch_size"]), "8")
        self.assertEqual(h1["precision"], "fp16")
        h2 = vr.hints_from_config_docs([hf])
        self.assertEqual(str(h2["batch_size"]), "16")
        self.assertEqual(str(h2["seq_len"]), "512")
        self.assertEqual(h2["precision"], "bf16")

    def test_tier3_config_fills_gaps_sniff_cannot(self):
        # annotations give family/hidden/layers; batch/seq are ONLY in the referenced config.
        pod = gpu_pod(
            annotations={
                "ksolver.ai/vram-family": "transformer",
                "ksolver.ai/vram-hidden-size": "1024",
                "ksolver.ai/vram-layers": "12",
            }
        )
        # Without config -> unknown (batch/seq missing).
        self.assertEqual(vr.resolve(pod)["source"], "unknown")
        # With config -> config+model, high, hard.
        docs = [{"train_micro_batch_size_per_gpu": 8, "max_seq_length": 512, "fp16": {"enabled": True}}]
        r = vr.resolve(pod, config_docs=docs)
        self.assertEqual(r["source"], "config+model")
        self.assertEqual(r["confidence"], "high")
        self.assertTrue(r["hard"])
        self.assertGreater(r["vram_mib"], 0.0)

    def test_record_then_resolve_round_trip_fires_tier4(self):
        pod = gpu_pod(args=["--custom-flag", "x"])  # unsniffable
        fd, path = tempfile.mkstemp(suffix=".jsonl")
        os.close(fd)
        try:
            for peak in (8000.0, 8200.0, 8400.0):
                vr.record_observation(path, pod, peak)
            obs = vr.load_observations(path)
            r = vr.resolve(pod, observations=obs)
            self.assertEqual(r["source"], "historical-fingerprint")
            self.assertEqual(r["observation_samples"], 3)
            self.assertGreaterEqual(r["vram_mib"], 8000.0)
        finally:
            os.unlink(path)

    def test_index_observation_makes_tier4_fire_in_memory(self):
        pod = gpu_pod(args=["--live", "x"])
        obs: dict = {}
        for peak in (7000.0, 7100.0, 7200.0):
            vr.index_observation(obs, pod, peak)
        r = vr.resolve(pod, observations=obs)
        self.assertEqual(r["source"], "historical-fingerprint")
        self.assertEqual(r["observation_samples"], 3)

    def test_fingerprint_is_stable_and_present(self):
        pod = gpu_pod(args=["--batch-size", "8"])
        a = vr.resolve(pod)["fingerprint"]
        b = vr.resolve(pod)["fingerprint"]
        self.assertEqual(a, b)
        self.assertIn("command_hash", a)
        self.assertTrue(a["image"].startswith("pytorch/"))


if __name__ == "__main__":
    unittest.main()
