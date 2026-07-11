# Volcano gang-aware baseline — feasibility + integration plan

**Status:** feasibility SPIKED + confirmed (2026-07-10); full integration proposed — needs a go-ahead.
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
