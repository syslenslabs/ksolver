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
**Landed 2026-07-13 (cross-SKU plumbing):** `run_k8s_probe.py` now supports `--node-selector KEY=VALUE`
(repeatable, targets a SKU's node pool; overrides a per-scenario `node_selector:`) and `--tolerate-gpu`
(adds the standard `nvidia.com/gpu:NoSchedule` toleration — required on tainted GPU pools). This makes
the roadmap's "run the same matrix per SKU via nodeSelector" acceptance actually schedulable. Preview
with `--print-manifest` (offline). Unit-tested (`test_run_k8s_probe.py`, wired into CI). The remaining
F1 gap is purely the USER-supplied multi-SKU GPU cluster + running the probes.

## F2 — Live kube-scheduler-simulator  (DONE 2026-07-13)
**Status:** ✅ COMPLETE + verified. `gpu-scenarios --simulator-pool ... --refresh-simulator-cache
--simulator-max-live-baselines all` now returns exit 0 with **66/66 live baselines** (mode=live,
0 cached, 0 errors), including the binpack variants that previously timed out.
**Already built:** the whole comparison pipeline (`gpu-scenarios --simulator[-pool]`, cache grind,
Volcano baseline capture); it fails closed without live KSS.
**ROOT CAUSE (corrected 2026-07-13) — there were TWO KWOK bugs + one convergence bug**, all fixed:
1. **Import 500 (first blocker):** the KWOK cluster image runs no kube-controller-manager, so no
   per-namespace `default` ServiceAccount exists; the ServiceAccount admission plugin then rejects
   every imported pod. Fix: `--kube-admission=false` on the cluster container (committed in
   `scripts/kss-pool.sh`).
2. **/reset never drains (second blocker):** KWOK's apiserver stores under etcd prefix `/registry`
   but `reset.go` deletes `/kube-scheduler-simulator`. Fix:
   `--extra-args kube-apiserver=etcd-prefix=/kube-scheduler-simulator` (committed in
   `scripts/kss-pool.sh`). No image rebuild needed.
   *Regression-guarded:* `test_kss_pool.py::test_start_passes_f2_apiserver_fixes_to_cluster_container`
   asserts both flags are on the cluster `docker run` (auto-run in CI via `test-operator-tools.py`), so
   they can't be silently removed.
3. **Unschedulable-pod convergence (ksolver-side):** the simulator only writes `filter-result`
   annotations on bind, so a genuinely-unschedulable pod stays Pending with no terminal marker and
   the batch timed out. Fix in `ksolver/src/verifier.rs`: treat a target with
   `PodScheduled=False/Unschedulable` as a settled "not placed" outcome (gated on batch stability).
   New `SimulatorBatchState::unschedulable_present_targets` + `visible_targets_settled()`; unit-tested.
Details in memory [[kss-simulator-reset-rootcause]].
**Acceptance:** ✅ import→reset→export drains empty; ✅ `gpu-scenarios` reports `mode=live`;
✅ provenance flips cached→live (66/66 live); ✅ `conform` runs live (reset+import+export path
verified against `solver-lab`: 41/41 imports 200, 41/41 resets 202, 456/458 exports 200 — the 2
export 500s are `context canceled` from ksolver's own client-side poll deadline on a slow export,
not a server bug). 589 Rust tests + clippy green.
**Note on conform completion:** `conform` is O(pods × nodes) — one full reset+import+poll per
(pending pod, candidate node), because it deliberately isolates ONE node + ONE pod so a successful
bind proves feasibility on *that specific* node (see `build_single_node_payload`). On a 113-node
cluster like `solver-lab` a full run is intractable under a normal timeout. This is a pre-existing
conform scaling cost, not an F2 issue.
**LANDED 2026-07-13 (node sampling):** `conform --max-nodes N` caps the probed candidate set so a
large-fleet run becomes a tractable, *honest* spot-check. The report records `nodes_evaluated` vs
`nodes_total` and `render()` prints a "node-sampled spot-check — probed N of M ... (not full-cluster
coverage)" note so it's never mistaken for full coverage. Default (0) is unchanged (all nodes).
Verified live: `conform --cluster solver-lab --sample 2 --max-nodes 3` completes (exit 0) with
strict agree=6/6, false_positive=0, gate=pass — real Phase-2 live conformance, now runnable. 594 Rust
tests + clippy green.
**Optimization investigation (2026-07-13):** I tested reading the simulator's per-node
`filter-result` annotation from one all-nodes import (would cut O(pods×nodes)→O(pods)). **Not
viable** — the annotation is written *inconsistently*: a scheduled pod had the full filter/score/bind
annotations in one case but ZERO annotations in another (a pod that bound to the only fitting node,
rechecked to 25s). Can't build conformance on an unreliable signal. Relying on *bind* with all nodes
is also wrong (the scheduler picks the best node, so a bind to X says nothing about Y). The
single-node isolation (one import per (pod,node), bind == feasible-on-that-node) remains the only
reliable per-node probe.
**LANDED 2026-07-14 (node-class dedup, exact full coverage, opt-in):** `conform --dedup-nodes` groups
candidate nodes into feasibility-equivalence classes and probes ONE representative per class, then
computes each node's verdict (own per-node `ours` + the class's shared simulator Filter verdict) —
exact full coverage at O(pods × node-classes) instead of O(pods × nodes). Default OFF (unchanged).
**Safety design (resolves the earlier "needs sign-off" concern):** the key
(`pod_filter_equivalence_key`) captures exactly the single-pod Filter determinants — allocatable +
the node labels the pod's nodeSelector/required node-affinity `matchExpressions` reference + taints —
and returns `None` (⇒ probe individually) on anything uncertain: `matchFields` (node-name dependent)
or PVC volumes (VolumeBinding topology). Inter-pod affinity/topology-spread are trivially satisfiable
with no other pods, so they don't affect the verdict and are safely ignored. A key bug can therefore
only *under*-merge (slower), never produce a wrong verdict; `ours` is always computed per-node so only
the expensive probe is shared. Report adds `simulator_probes` + a "node-dedup: N probes for M pairs"
note. Unit-tested (merge-identical / separate-referenced-diff / None-on-PVC+matchFields / grouping) and
**live-verified on solver-lab: full vs `--dedup-nodes` produced IDENTICAL verdicts + mismatches**
(agree 6/6), with conservative no-merge on heterogeneous nodes. 597 Rust tests + clippy + fmt green.
**Remaining (optional):** none outstanding for conform — sampling (`--max-nodes`) and exact dedup
(`--dedup-nodes`) both shipped.
**LANDED 2026-07-14 (demo pool wiring, opt-in):** `demo-gate.py --start-kss` starts a KSS pool via
`kss-pool.sh` before the gate and tears it down after (guaranteed via `finally`, even if the gate
raises), so a live demo gate is now a one-command run (`--start-kss --require-kss-ready`). Default OFF
— zero behavior change without the flag. Leverages the F2 single-pool fix. 3 new lifecycle tests in
`test_demo_gate.py` (start→stop ordering, default no-op, teardown-on-raise); 45 demo-gate tests pass.

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

## F4 — DCGM VRAM-metrics exporter  (DONE 2026-07-13, pending real-exporter validation)
**Status:** ✅ WIRED + verified end-to-end against a mock dcgm-exporter. The tier-4 store now
auto-fills from Prometheus DCGM metrics; full validation still needs a real dcgm-exporter (the exact
label set varies by version), and the code refuses to run without a live Prometheus rather than
fabricate.
**Already built:** the historical-fingerprint prediction tier + store reader/writer.
**What landed:**
- `historical_usage.rs`: `pod_peak_vram_query(window)` →
  `max by (namespace,pod,exported_pod)(max_over_time(DCGM_FI_DEV_FB_USED{pod!=""}[window]))`;
  `query_pod_peak_vram_mib()` + a pure `parse_pod_vram_peaks()` that takes the max across a pod's GPUs
  and falls back `pod`→`exported_pod`. Mock-Prometheus unit-tested.
- `vram_store.rs`: `observations_from_vram_metrics(peak_by_pod, pods)` joins DCGM (ns/pod) → tier-4
  (image, command_hash) via `pod_command_hash`, skipping unreported/non-positive pods (never invents a
  value). `collect_and_store_vram_observations()` orchestrates query→list pods→map→append. Unit-tested
  (fingerprint parity with the Python resolver).
- `main.rs`: `ksolver vram-observe --store <p> --prometheus-url <u> [--prometheus-username/-token]
  [--window <dur>] [--kubeconfig <p>]`; exits 2 without a store/prometheus-url.
**Verified:** mock DCGM Prometheus + live `desktop` kind cluster → 2 fingerprinted rows written
(max-across-GPUs + exported_pod fallback + drop-unlisted all exercised). 593 Rust tests + clippy green.
**USER must supply for full validation:** a real dcgm-exporter + Prometheus, then run `vram-observe`
on a cron and confirm tier-4 predictions cite the real observations.

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
**LANDED 2026-07-14 (design DRAFT):** `docs/superpowers/specs/2026-07-14-f5-concrete-dra-device-identity-design.md`
— device-assignment variables `a[r,d]` (selector-gated, node-linked to placement `x`, exclusivity/
capacity), consumable-capacity (F5b, ties to the VRAM wedge), NVLink topology as a two-phase soft
objective (F5c), scale mitigations (pre-filter by selector + feasible nodes, symmetry-break, flag-gated
default-off with scalar fallback), honesty-summary upgrade rules, and a testing plan whose live gate is
a real GPU DRA driver. Explicitly marked NOT-approved-for-implementation: the attribute/capacity/
topology model MUST be validated against a real driver first (open questions enumerated). Review +
driver-env validation is the USER's call before F5a code.

## Recommended order
F1 (data) → F2 (live sim) unlock external claims; F3 (webhook) makes it deployable; F4/F5 deepen it.
The fastest autonomous progress while waiting on infra: F3 manifest scaffolding, then F1 probe-matrix
runbook. Everything else needs the USER to supply the gated infra/data above.
