# Frontier Roadmap — beyond the 8-phase roadmap + Land & Harden (2026-07-13)

The 8-phase roadmap and the Land & Harden consolidation are complete (all fixes/honesty landed,
CI-green). What remains blocks **external/customer** claims and is **infra/data-gated** — the code and
tooling largely exist; these phases are "run / deploy / collect," not "build." Ranked by how much each
unblocks credible external claims. Each notes: the gap, what's already built, the prerequisite the USER
must supply, acceptance criteria, and any autonomous prep an agent can do now.

## F1 — VRAM model data credibility  (rank 1: biggest lever)
**Gap:** training data is ~99% synthetic, 0% real CUDA-OOM, 100% single-SKU (RTX-4090). The model
architecture is complete and honest (group-aware error now surfaced: ~1240 MiB novel-config MAE), but
customer accuracy claims need real grounding.
**Already built:** `vram-model-lab/scripts/run_k8s_probe.py` (labels jobs, captures nvidia-smi peak),
the sidecar profiler (`ksolver_vram_sidecar.py` + `examples/sidecar-profile-job.yaml`), the fit/eval
pipeline, and the quality gate.
**USER must supply:** a GPU cluster (ideally >1 SKU: T4/L4/A10/A100 alongside 4090) and permission to
run real training probes (HF Trainer, torchvision/timm, DeepSpeed/FSDP/Accelerate) to OOM.
**Acceptance:** ≥50 verified_real_framework rows across ≥2 SKUs incl. real oom=True labels; refit +
promote; group-aware novel-config MAE reported; quality gate still passes; demo VRAM claims cite the
group-aware number.
**Autonomous prep now:** a runbook + manifest set for the probe matrix (families × precisions × sizes ×
SKUs) ready to `kubectl apply` on a GPU cluster.

## F2 — Live kube-scheduler-simulator  (rank 2: competitive provenance)
**Gap:** live KSS/gang-aware baselines need a working simulator; the self-built arm64 image's
`/api/v1/reset` never drains (verified), so only cached baselines exist — customer $ claims require
live provenance.
**Already built:** the whole comparison pipeline (`gpu-scenarios --simulator[-pool]`, cache grind,
Volcano baseline capture); it fails closed without live KSS.
**USER must supply:** an amd64 host running the official `registry.k8s.io/scheduler-simulator` images,
OR a fixed arm64 source build (debug the Go `/reset` drain).
**Acceptance:** `gpu-scenarios` reports `mode=live` for spread+binpack on the proof scenarios;
`conform` runs live; provenance flips cached→live in the honesty strip.
**Autonomous prep now:** none beyond what exists (upstream/infra fix required).

## F3 — In-cluster VRAM→DRA admission webhook  (rank 3: makes it actionable)
**Gap:** the VRAM→DRA wedge runs fail-open but has no deployed `MutatingWebhookConfiguration` + TLS.
**Already built:** `vram-model-lab/scripts/vram_admission_service.py` (AdmissionReview handler,
fail-open), the resolver cascade, and `admission.rs` patch rendering.
**USER must supply:** a cluster + TLS strategy (cert-manager or manual CA) and sign-off to register a
mutating webhook (it mutates pods; must stay fail-open + scoped by namespace/label).
**Acceptance:** webhook deployed with TLS; live-verified it injects DRA consumable-capacity claims on
opt-in pods and fails open on resolver error; RBAC-minimal; kill switch honored.
**Autonomous prep now:** scaffold the deploy manifests (Deployment + Service + Certificate +
MutatingWebhookConfiguration with namespace/label selectors + failurePolicy: Ignore) as
ready-to-apply YAML, gated behind an opt-in label — reviewable without a cluster.

## F4 — DCGM VRAM-metrics exporter  (rank 4: closes the prediction loop)
**Gap:** the tier-4 historical VRAM store isn't auto-populated from real usage.
**Already built:** the historical-fingerprint prediction tier + store reader.
**USER must supply:** DCGM exporter (or equivalent) scraping GPU memory + a Prometheus the collector
can read.
**Acceptance:** the store auto-fills from live metrics; tier-4 predictions cite real observations.
**Autonomous prep now:** a scrape-config + ingestion mapping (DCGM metric → store row) as a doc/config.

## F5 — Concrete DRA device identity + topology  (rank 5: high potential, high risk)
**Gap:** DRA is honest scalar approximation; NVLink/topology is label-filter only. Phase 5 depth.
**Already built:** version-adaptive DRA reads (1.31–1.35), scalar accounting, topology label filters,
the device-correctness honesty summary (exact/approx/unsupported).
**USER must supply:** a GPU DRA driver env + concrete device/topology inventory to validate against.
**Acceptance:** device-assignment variables produce concrete per-device claims (not scalar) with a
proof; NVLink-optimal placement backed by real topology inventory; honesty summary upgrades those
from "approximate/unsupported" to "exact".
**Autonomous prep now:** a design spec for device-identity variables (this is real code, but should be
brainstormed + validated against a driver env before implementing — high risk of a wrong model).

## Recommended order
F1 (data) → F2 (live sim) unlock external claims; F3 (webhook) makes it deployable; F4/F5 deepen it.
The fastest autonomous progress while waiting on infra: F3 manifest scaffolding, then F1 probe-matrix
runbook. Everything else needs the USER to supply the gated infra/data above.
