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
