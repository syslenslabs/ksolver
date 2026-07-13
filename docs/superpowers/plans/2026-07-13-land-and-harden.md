# Land & Harden — consolidation plan (2026-07-13)

The 8-phase GPU-scheduler roadmap is complete. Before opening any new frontier, land the validated
session work and harden the two known honesty gaps. Ordered by dependency; each phase is its own
reviewable commit(s) with before/after and rollback.

## Phase A — Commit the validated work  ✅ authorized (land+harden)

- **Commit 1 (semantic + demo + tooling):** the 12 tracked-modified files (3 real fixes —
  `--cluster` connection, stdout/stderr hygiene, misleading cost-%; + demo honesty/narrative; +
  clippy cleanups) **plus** the untracked session work: `volcano-baseline-cache.json` (powers the
  6 beats-gang-aware headline), `scripts/kss-cache-grind.sh` (referenced by the fixed build_hint),
  and `vram-model-lab/scripts/{group_aware_eval.py,test_model_quality_gate.py}` (VRAM quality-gate
  tooling), plus this plan doc.
- **Commit 2 (formatting):** `cargo fmt --all` alone — clears the 92 pre-existing fmt hunks so CI's
  `fmt --check` gate goes green; kept separate so the semantic diff stays reviewable.
- **Gate:** full suite (588 Rust + operator/JS + vram-lab) green + clippy `--all-targets` clean
  before committing. **Rollback:** plain commits on `scheduler`; `git revert` if needed.

## Phase B — Surface the honest VRAM accuracy number

The demo/estimator surface the optimistic row-LOO error (~1037 MiB MAE); honest novel-config
generalization is ~1240 MAE / 3365 p95 / 12200 max (group-aware CV). Decision: **show both** (most
honest), not replace.

- Add group-aware CV to `fit_peak_vram_model.py`'s eval as NEW fields (`group_loo_*`), keeping the
  existing row-level fields so `test_model_quality_gate.py`'s model↔evaluation agreement holds.
- Regenerate `evaluation.json`; extend the gate test to assert the group-aware fields + wire it into CI.
- Dashboard: show both row-level and group-aware LOO (the "row-level" label already shipped).
- Estimator: switch the `safety_margin` to the honest group-aware p95 — the one place it has
  functional impact (borderline fits/OOM decisions).
- **Gate:** model still passes its quality gate under the honest metric (p95 3365<5000, max 12200<25000).
  **Rollback:** revert the eval regen + wiring commit.

## Phase C — Fix the `best_kube` efficiency baseline

For 4 scenarios (`weekend-flex-rightsize`, `many-mediums-one-large`, `business-value-over-fifo`,
`queue-urgent-over-fifo`) `efficiency()` compares against a do-nothing (0-useful) baseline, inflating
the advantage.

- Change the efficiency comparator to prefer the strongest-admission baseline when the cheapest admits
  0 useful GPU; keep `win_classification` on max-useful (unchanged).
- Verify the 4 scenarios display honestly — especially `weekend-flex`'s rightsizing must not read as
  under-admission (dashboard `scGain` flexible-reduction handling covers this).
- Capture before/after numbers for the 4 scenarios in the commit message.
- **Gate:** 588+ tests green; the 6 beats-gang-aware wins unchanged. **Rollback:** revert the commit.

## Non-goals (deferred to a future frontier plan)
Real-framework/true-OOM/cross-SKU VRAM data; live kube-scheduler-simulator; in-cluster webhook+TLS;
DCGM exporter; concrete DRA device identity; NVLink/topology. All infra/data-gated.
