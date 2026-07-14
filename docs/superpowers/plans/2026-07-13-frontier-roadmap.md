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
**ROOT CAUSE FOUND (2026-07-13):** the `/reset` drain bug is an **etcd-prefix mismatch**, not a
fundamental arm64 issue. `reset.go` deletes prefix `/kube-scheduler-simulator`, but the self-built
setup points the simulator at a KWOK cluster whose apiserver stores objects under the default
`/registry` prefix → reset misses them. Fix: make the prefixes match — either configure KWOK's
apiserver `--etcd-prefix=/kube-scheduler-simulator` (committable in kss-pool.sh) or patch reset.go's
`EtcdPrefix` to `/registry` + rebuild (source is a gitignored build artifact, so patch in the build
step). Details in memory [[kss-simulator-reset-rootcause]]. Go 1.26 + the source + the official amd64
v0.4.0 images are all present locally.
**USER must supply / decide:** which fix (KWOK apiserver arg vs rebuild vs official amd64 under
emulation) — then it's implementable + testable locally.
**Acceptance:** import→reset→export drains empty; `gpu-scenarios` reports `mode=live`; `conform` runs
live; provenance flips cached→live.
**Autonomous prep now:** root cause diagnosed + fix path documented (above). Full fix needs the
KWOK-apiserver reconfig + full-stack re-test (a focused follow-up).

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
**Concrete approach (2026-07-13):** `historical_usage.rs` already has the PromQL client
(`query_prometheus_vector/matrix`) — it currently queries pod CPU/memory. Add a per-pod peak-VRAM
query against the standard dcgm-exporter metric: `max_over_time(DCGM_FI_DEV_FB_USED{pod!=""}[<window>])`
(framebuffer used, MiB; the kube-integrated dcgm-exporter adds `pod`/`namespace`/`exported_pod`
labels). The non-trivial glue is mapping DCGM's per-pod peak → the tier-4 store's key (workload
fingerprint = image+command_hash, `vram_store.rs`): join the DCGM pod to its spec, compute the
fingerprint, write the observed peak. **Can't be validated without a real dcgm-exporter** (the exact
label set varies by exporter version + kube-state-metrics join), so wire it behind the existing
Prometheus config and gate on real metrics rather than writing it blind.
**Autonomous prep now:** the query + mapping design above; a mock-Prometheus unit test could validate
the PromQL parsing + fingerprint mapping, but the real label join needs a live exporter.

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
