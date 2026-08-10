# Volcano gang-aware baseline — feasibility + integration plan

**Status:** COMPLETE end-to-end (2026-07-11). The full gang-aware baseline pipeline is built + wired:
1. `ksolver dump-scenarios` — export scenario topology + jobs.
2. `scripts/volcano-baseline-run.sh <scenario>` — run one scenario through Volcano on KWOK fake GPU
   nodes; **live-verified** on `colocated-gang-vs-large` (`volcano_safe_useful_gpu: 14`).
3. `ksolver score-gang-baseline` — VRAM-SAFE scoring (reuses `pending_input::vram_fits`); unit-tested.
4. `scripts/volcano-baseline-cache.sh` — batch step 2 over all gang scenarios → `{scenario: useful}` cache.
5. `ksolver gpu-scenarios --volcano-baseline <cache.json>` — loads the cache; `classify_win` then emits
   `beats-gang-aware` where ksolver beats BOTH kube and Volcano (`not-proven` for gang-only wins).

Every component is verified (live harness run + unit tests + build). The only remaining action is
OPERATIONAL: run the ~2h offline batch (`volcano-baseline-cache.sh`) to produce the cache, then pass
`--volcano-baseline` — turning today's honest `beats-kube-only` verdicts into provable
`beats-gang-aware` differentiator claims. No code work remains.
**Why:** ksolver's win classification only ever emits `beats-kube-only` today because there is no
gang-aware baseline (`classify_win(..., gang_aware = None)`). The roadmap's own no-strawman honesty
stance forbids a hand-rolled local gang scheduler (the greedy kube fallback was deliberately
disabled), so the honest baseline must be **real Volcano**. This is the single thing that turns
honest `beats-kube-only` wins into provable `beats-gang-aware` differentiator claims — the roadmap's
prerequisite for external competitive claims.

## Feasibility spike (verified 2026-07-10)

On a local `kind` cluster:
- Volcano installs cleanly from `installer/volcano-development.yaml` (scheduler + controllers +
  admission all Running).
- It gang-schedules **all-or-nothing**: a `batch.volcano.sh/v1alpha1` Job with `minAvailable: 3` and
  3×7 CPU requests (21 > 16 allocatable) left the **PodGroup Pending with zero members placed**.
- Contrast on the same node: a plain `ReplicaSet` (default scheduler, no gang) placed **2/3 Running,
  1 Pending** — the partial/broken-gang admission that ksolver treats as a kube liability.

So real gang-aware placement is reproducible here — a credible baseline, not a strawman.

### Substrate spike for GPU scenarios (2026-07-10)

kind has no real GPUs, so the baseline needs fake GPU nodes. Verified on kind:
- **KWOK fake GPU nodes work.** A KWOK node (`kwok.x-k8s.io/node: fake`, taint tolerated) advertising
  `nvidia.com/gpu: 4` goes Ready, and a GPU pod (`limits: nvidia.com/gpu`) runs on it (KWOK fakes the
  lifecycle). Note: extended resources require `limits`, and pods need the kwok taint toleration +
  `nodeSelector: {type: kwok}`.
- **Volcano gang-blocks over-capacity GPU gangs.** A 3×2-GPU gang (6 > 4 available) stayed
  `PodGroup: Inqueue`, 0/3 placed — all-or-nothing on the GPU resource, on fake nodes.
- **Harness note:** a *fitting* GPU gang was still 0/2 at ~18 s (torn down before it stabilized). The
  harness must **poll to steady state** (like the KSS baseline caches results), NOT use a fixed
  sleep — KWOK+Volcano need time to settle. Re-confirm the fitting-placement case when building it.

Stack for the harness: `kind` + KWOK (fake GPU nodes sized to the scenario) + Volcano.

### Capture tool (verified end-to-end, 2026-07-10)

`scripts/volcano-baseline-capture.sh` implements the "capture" half: stands up the stack, runs a gang
config through Volcano, polls to steady state, and emits placement metrics as JSON
(`{volcano_useful_gpu, placed, replicas, min_available, gang_complete}`). Verified both cases:
- fitting (2×2=4 GPU on a 4-GPU node) → `useful=4, placed=2, gang_complete=true`;
- over-capacity (3×2=6 > 4) → `useful=0, placed=0, gang_complete=false` (all-or-nothing).

Key implementation finding: **count placement by `spec.nodeName`, not pod phase** — KWOK's
`stage-fast` fast-forwards scheduled pods to `Succeeded`, so a "Running" count reads 0 even for a
placed gang (this was a real bug caught by running it; a blocked gang has no `nodeName`). Also set
`queue: default` on the vcjob and retry the apply until the admission webhook is ready.

Remaining for the full harness: translate the scenario LIBRARY's gangs (Rust
`jobs_to_volcano_baseline`), size KWOK nodes to each scenario's topology, run the capture per
scenario, and feed `volcano_useful_gpu` into `classify_win`'s `gang_aware` arg (the honesty layer
already consumes it). Scenario topology + jobs are now exportable via `ksolver dump-scenarios`
(`dump_scenario_library()`) — the harness reproduces each scenario from that JSON.

### CRITICAL fairness finding (2026-07-11): VRAM-safe scoring

**Volcano schedules on GPU *count* only — it ignores VRAM.** So on VRAM-constrained scenarios it
will PLACE gangs that ksolver correctly REFUSES (OOM risk). A naive `volcano_useful_gpu` (raw
placement) would therefore count VRAM-unsafe placements as "useful," making Volcano look better and
**understating `beats-gang-aware` dishonestly** — the same over/under-claim failure the honesty layer
exists to prevent, just in the other direction.

So the baseline MUST score Volcano's placements with the SAME safety criterion ksolver applies:
a placed pod counts as useful only if its predicted peak VRAM fits the node's per-GPU VRAM
(`pending_input::vram_fits_node` / `node_peak_vram_bytes`). The harness knows each placement's node
(from `spec.nodeName`) and the scenario's node VRAM (from `dump-scenarios`) and the pod's VRAM
(predicted), so it can recompute `volcano_useful_gpu` counting only VRAM-safe complete gangs.

Implication: the scoring/join step should **reuse ksolver's Rust VRAM-feasibility logic** rather than
reimplement it in bash — pointing to a Rust-native scoring step (read Volcano placements back, score
with `vram_fits_node`) even if cluster orchestration stays in a script. Getting this wrong produces
dishonest `beats-gang-aware` verdicts, so it must be exact.

## Integration plan (the large part)

The existing KSS baseline path (`run_kube_baseline` → `run_kube_simulator`) POSTs scenarios to the
kube-scheduler-simulator HTTP API. Volcano is a separate scheduler, so it needs its own harness:

1. **Cluster with Volcano** (kind + `volcano-development.yaml`, or a provided cluster). Gate the
   baseline on Volcano being present; absent ⇒ `gang_aware = None` (today's behavior) — never fake it.
2. **Scenario → Volcano translation:** render each `ScenarioSpec` job as a Volcano `Job`
   (`minAvailable` = gang size for co-located gangs; `schedulerName: volcano`), sized to the
   scenario's GPU/CPU demand. Reuse the node topology the KSS baseline uses so the comparison is apples-to-apples.
3. **Apply + observe:** submit, wait for steady state (bounded), read pod placements + PodGroup
   status. Compute the same `PlacementMetrics` (useful_gpu, partial_or_invalid_gangs) used for KSS.
4. **Wire into the benchmark:** add a `gang_aware: Option<EngineResult>` alongside `kube`/`kube_binpack`
   on `ScenarioResult`; pass `gang_aware.map(|e| e.metrics.useful_gpu)` into `classify_win`. Then a
   scenario that beats BOTH kube and Volcano earns `beats-gang-aware`; one that beats kube but not
   Volcano correctly drops to `not-proven` (table stakes) — the honesty machinery is already built
   to consume this.
5. **Provenance:** extend `ProofProvenance` / the summary so a Volcano-substantiated win is labeled,
   and update `does_not_prove` (drop the "no gang-aware baseline" disclaimer when one is present).

## Scope / risks

- Substantial: a new baseline engine + harness (cluster lifecycle, translation, placement capture),
  parallel to the KSS path. Not a single increment — a multi-step feature.
- Determinism: Volcano scheduling can vary; capture + cache results (like KSS) and disclose provenance.
- The honesty layer (`classify_win`, `does_not_prove`, provenance) is ALREADY built to consume a
  gang-aware baseline — this integration is what feeds it.

## Acceptance

- With Volcano present, at least one scenario earns `beats-gang-aware`, and gang-only wins that
  Volcano also achieves correctly classify `not-proven`.
- With Volcano absent, behavior is unchanged (`gang_aware = None`, honest `beats-kube-only`).
- A repeatable script (mirroring `scripts/dra-version-smoke.sh`) stands up Volcano and runs the
  comparison.

## ACHIEVED — full reproduction + results (validated 2026-07-12)

**Result:** the full real report shows **15 beats-kube-only, 12 not-proven, 6 beats-gang-aware**,
scored against a faithful Volcano baseline, produced entirely in-environment (kind + KWOK + Volcano +
a self-built arm64 kube-scheduler-simulator). The 6 wins are validated legitimate (pod→node placement
dumps + run-to-run variance): Volcano's non-gang fillers greedily grab the nodes high-value gangs
need, stranding the gangs; ksolver's global + priority-aware solve reserves them. ksolver never
genuinely loses (6 ties fill capacity; weekend-flex-rightsize's 2-vs-8 is intentional rightsizing —
`flexible_gpu_reduction=6`, a cost win).

**Three Volcano-harness fidelity fixes were required** (all now in `scripts/volcano-baseline-run.sh`):
1. **Pods must stay Running.** `stage-fast`'s `pod-complete` Stage fast-forwards pods to Succeeded,
   freeing reservations → Volcano piles more onto a node → cumulative useful-GPU over-count (e.g. 20
   on 19 GPU, physically impossible). Fix: `kubectl delete stage pod-complete`.
2. **Colocated gangs need single-node co-location.** `minAvailable` gives all-or-nothing but NOT
   single-node; without it colocated gangs SCATTER and fragment nodes, crippling Volcano. Fix: self
   `podAffinity` on `kubernetes.io/hostname` (Volcano schedules the gang atomically → no deadlock).
3. **Patient settle** before counting placements (pods now stay Running so the count stabilizes).
Validation: `colocated-gang-vs-large` then places exactly 14 = the feasible optimum.

**A scoring-side per-node GPU cap was tried and REJECTED** — Volcano's KWOK placement is
non-deterministic, so capping can under-count → false `beats-gang-aware` (over-claim). The three
harness fixes above are the correct approach; the scorer stays simple.

**Reproduction commands (this arm64 env):**
1. Build the simulator once: arm64 `simulator-server`/`simulator-scheduler` images (see
   [[gpu-scheduler-phase1-status]] — `docker buildx --platform=linux/arm64` from simulator source).
2. KSS baselines (66 = 33 scenarios × spread/binpack). As of 2026-07-13 the arm64 simulator's
   `/api/v1/reset` DRAINS correctly (the two KWOK apiserver bugs — ServiceAccount admission and an
   etcd-prefix mismatch — are fixed in `scripts/kss-pool.sh`), so a single fresh pool serves all 66
   in ONE pass: `kss-pool.sh start 1 12120 <cache-dir>` → `ksolver gpu-scenarios --simulator-pool
   http://127.0.0.1:12120 --simulator-cache-dir <cache-dir> --refresh-simulator-cache
   --simulator-max-live-baselines all` (verified: 66/66 live, 0 errors). `scripts/kss-cache-grind.sh`
   still works as a harmless multi-round fallback but now completes in round 1.
   (HISTORICAL: before that fix, reset never drained, so each container served ~1 baseline and the
   grind needed ~11 rounds.)
3. Volcano baseline: `scripts/volcano-baseline-cache.sh volcano-baseline-cache.json` (faithful
   harness; ~18 min for 13 gang scenarios).
4. Report: `ksolver gpu-scenarios --simulator-cache-dir <kss-cache-dir> --volcano-baseline
   volcano-baseline-cache.json --json`.
5. Dashboard / live server (verified end-to-end 2026-07-12): the shadow server needs a readable
   kubeconfig (it bails otherwise) but the demo report itself uses only the caches + scenario library,
   so any reachable cluster works (an empty `kind` cluster is fine). From the repo root:
   `KUBECONFIG=<kubeconfig> KSOLVER_GPU_SCENARIO_SIMULATOR_CACHE_DIR=<kss-cache-dir> ksolver shadow`
   (observe-only default; HTTP on `127.0.0.1:8090`, override with `KSOLVER_SHADOW_ADDR`). The
   `volcano-baseline-cache.json` at CWD auto-loads. `GET /api/scheduler/demo-report` then returns the
   report with `beats_gang_aware: 6, gang_aware_baseline_pending: false` (confirmed live: 200,
   ~305 KB), and the dashboard's honesty strip renders green "proven" with the 6 wins.
