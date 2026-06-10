# Constraint Cost Attribution

## Problem

After a baseline solve, the user sees a total savings number but can't answer: "which specific constraint is costing me how much?" The inflation drivers section shows heuristic estimates per constraint type, but these aren't verified by the solver. The constraint simulator lets users toggle constraints one at a time, but that's manual exploration — there's no automatic, ranked, solver-verified view of per-constraint dollar costs.

## Solution

After every baseline solve, automatically run single-constraint-relaxation scenarios on the server, compute the savings delta for each, and return a ranked `ConstraintCostTable` as part of the `ExplainabilityReport`. The frontend renders this as a prominent table immediately after the hero cards.

## Data Model

New types in `model.rs`:

```rust
#[derive(Default, Clone, Serialize, Deserialize)]
pub struct ConstraintCostRow {
    pub constraint_key: String,
    pub display_name: String,
    pub baseline_savings: Money,
    pub relaxed_savings: Money,
    pub delta: Money,
    pub affected_workload_count: i32,
    pub affected_node_count: i32,
    pub nodes_removable_baseline: i32,
    pub nodes_removable_relaxed: i32,
    pub action: String,
}

#[derive(Default, Clone, Serialize, Deserialize)]
pub struct ConstraintCostTable {
    pub rows: Vec<ConstraintCostRow>,
    pub baseline_savings: Money,
    pub theoretical_max_savings: Money,
}
```

Add to `ExplainabilityReport`:
```rust
pub constraint_cost_table: ConstraintCostTable,
```

## Scenarios

Five single-variable scenarios, each relaxing one constraint dimension:

| Key | Display Name | ScenarioConfig Override |
|-----|-------------|----------------------|
| `taints` | Taints & Tolerations | `ignore_taints: true` |
| `anti_affinity` | Required Anti-Affinity | `relax_required_anti_affinity: true` |
| `affinity` | Preferred Affinity | `relax_preferred_affinity: true` |
| `rightsizing` | Request Rightsizing | `enable_joint_rightsizing: true` |
| `theoretical_max` | All Constraints Relaxed | all of the above |

The `theoretical_max` row is not shown in the main table — it's used for the total row and for scaling the bar widths.

## Backend Flow

In `service.rs`, the `analyze_with_progress` method changes:

1. **Baseline solve** runs as today (stages 1-7, progress 0-85%).
2. **Constraint cost phase** (progress 85-100%):
   - Reuses the cached `ClusterSnapshot` and `PricingCatalog` from the baseline run.
   - For each of the 5 scenarios:
     - Re-normalizes with the relaxed constraint config (normalizer options change).
     - Builds optimization input.
     - Solves via CP-SAT.
     - Extracts savings from `OptimizationPlan`.
     - Reports SSE progress: `"constraint-cost: {name} ({n}/5)"`.
   - Computes delta = `relaxed_savings - baseline_savings` for each.
   - Sorts rows by delta descending.
   - Attaches `ConstraintCostTable` to the `ExplainabilityReport`.
3. If any scenario solve fails, that row is omitted (not fatal to the overall report).

### Performance

Each scenario reuses the cached snapshot and pricing catalog. The normalization step is fast (~10-50ms). The solver is the bottleneck (~1-3s per scenario). Total added time: ~5-15s for 5 scenarios.

The scenarios run sequentially to avoid CPU contention with the CP-SAT solver (which already uses multiple workers internally). Progress updates keep the user informed.

### Extracting affected workload/node counts

For each scenario, compare the baseline plan's `active_nodes` against the relaxed plan's `active_nodes`. The difference gives `affected_node_count`. For `affected_workload_count`, count workloads that moved to different nodes in the relaxed solution vs baseline.

### Action text

Generated from the constraint type:
- taints → "Review taint policies — {N} workloads pinned to dedicated pools"
- anti_affinity → "Review anti-affinity rules — {N} workloads forced apart"
- affinity → "Review preferred affinity — {N} workloads preferring specific nodes"
- rightsizing → "Right-size requests — {N} workloads over-requesting resources"

## Frontend

### New section: "What Each Constraint Costs You"

Placed after the summary sentence, before action items. Visible whenever `constraint_cost_table.rows` is non-empty.

```
┌─────────────────────────────────────────────────────────────────────┐
│  WHAT EACH CONSTRAINT COSTS YOU                        4 constraints│
├─────────────────────────────────────────────────────────────────────┤
│  Constraint              Workloads   Nodes   Cost to Keep   Action │
│  ─────────────────────── ───────── ─────── ────────────── ──────── │
│  ████████████████         Request     291      $3,149/mo    review │
│  Rightsizing                                                       │
│                                                                    │
│  █████                    Taints &      9        $812/mo    review │
│                           Tolerations                              │
│                                                                    │
│  ███                      Anti-         5        $240/mo    review │
│                           Affinity                                 │
│                                                                    │
│  █                        Preferred    12         $90/mo    review │
│                           Affinity                                 │
│                                                                    │
│  Total potential if all constraints relaxed: $4,291/mo             │
└─────────────────────────────────────────────────────────────────────┘
```

### Styling

Follows existing design language:
- Panel with `var(--panel)` background, `var(--line)` border, `16px` border-radius.
- Section header with uppercase label and pill badge showing row count.
- Each row is a card-style element (like blocker cards) with:
  - Horizontal bar showing delta proportional to `theoretical_max_savings`.
  - Bar color: green gradient (same as savings stack segments).
  - Display name, workload count, node count, delta as money, and action text.
- "Total potential" footer line styled like summary sentence.

### Progress display

During constraint cost phase, the progress bar continues from 85% to 100%. Progress stage text shows: "Analyzing constraint: Taints (1/5)", "Analyzing constraint: Anti-Affinity (2/5)", etc.

## Testing

### Backend unit tests

In `service.rs` tests:
- Test that `ConstraintCostTable` is populated when baseline solve succeeds.
- Test that rows are sorted by delta descending.
- Test that theoretical_max >= any individual row delta.
- Test that a failing scenario solve doesn't crash the overall analysis.

### Frontend verification

- Start the server, open the dashboard, click Analyze Cluster.
- Verify the constraint cost table appears after the summary sentence.
- Verify rows are ordered by cost (highest first).
- Verify bar widths are proportional.
- Verify the total line shows theoretical max.
- Verify progress updates show per-scenario status.
