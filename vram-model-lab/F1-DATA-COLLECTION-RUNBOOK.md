# F1 — VRAM data-collection runbook (close the credibility gap)

The peak-VRAM model is architecture-complete and honest (group-aware novel-config MAE ~1240 MiB now
surfaced), but training data is **~99% synthetic, 0% real CUDA-OOM, 100% single-SKU (RTX-4090)**. This
is the #1 lever for credible customer accuracy claims. All tooling exists — this is "run + refit +
promote," not "build." **Prerequisite (USER): a GPU cluster, ideally multi-SKU (T4/L4/A10/A100 +
4090).**

## Data flow (existing tooling)
`run_k8s_probe.py` runs a probe pod, samples `nvidia-smi` + torch peak, appends a row to
`data/results.jsonl` → `fit_peak_vram_model.py` reads rows (`ok` + `nvidia_smi_peak_used_mib`), fits +
writes `data/models/peak_vram_linear.json` → `evaluate_model.py` writes `evaluation.json` →
`test_model_quality_gate.py` gates (incl. the committed group-aware fields).

## Gaps to fill (each row's SKU comes from the NODE it lands on)
1. **Real-framework** (highest value): today ~5 verified_real_framework rows. Run real HF Trainer,
   torchvision/timm, DeepSpeed/FSDP/Accelerate jobs — not just synthetic probes.
2. **True CUDA-OOM**: today oom=False for all rows. Size some configs to actually OOM so the model +
   OOM-risk classifier learn from real overflow, not a VRAM-fraction proxy.
3. **Cross-SKU**: today 100% 4090. Run the same matrix on ≥2 more SKUs (schedule probe pods onto each
   SKU's node pool via nodeSelector) to validate the workload→VRAM mapping transfers.

## Steps
1. **Define the matrix** in `scenarios.yaml`: family × precision × size × (optionally) batch/seq, plus
   a few deliberately-oversized configs for OOM. `run_k8s_probe.py --print-manifest --scenario <name>`
   emits the job manifest (label `ksolver.ai/vram-scenario=<slug>`) to review before running.
2. **Run per SKU**: on each SKU's node pool, `python run_k8s_probe.py --all --namespace <ns>` (or
   `--scenario <name>`). Each successful run appends a row to `data/results.jsonl` with
   `nvidia_smi_peak_used_mib`, torch peaks, `oom`, and framework labels.
3. **Refit + evaluate**: `python fit_peak_vram_model.py && python evaluate_model.py`. Regen is
   deterministic given the data; check `git diff data/models/` — coefficients change only because new
   rows landed.
4. **Gate**: `python -m unittest test_model_quality_gate` — must pass, and the group-aware
   (novel-config) p95/max must stay within policy (5000 / 25000 MiB). The dashboard/estimator already
   read the group-aware fields, so honest accuracy propagates automatically.
5. **Promote**: commit the regenerated model + evaluation + the new rows.

## Acceptance (unblocks external accuracy claims)
- ≥50 `verified_real_framework=True` rows across ≥2 SKUs, including real `oom=True` labels.
- Group-aware novel-config MAE reported (and improved vs the current ~1240 as real diverse data lands).
- Quality gate green under the honest (group-aware) metric.
- Demo/estimator cite the group-aware number (already wired).

## Notes
- SKU is NOT a model feature (peak VRAM is workload-driven); cross-SKU mainly validates transfer + the
  small per-SKU driver/context overhead. Real-framework > true-OOM > cross-SKU in value.
- The OOM-risk classifier (`oom_risk_classifier.json`) is currently a VRAM-fraction proxy; true-OOM
  labels are what turn it into a learned predictor — until then, don't quote it as learned.
