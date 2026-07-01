# GPU Scheduler — Fix: Required Node Affinity OR-of-Terms Semantics — Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: superpowers:subagent-driven-development or superpowers:executing-plans. Steps use checkbox (`- [ ]`) syntax.

**Goal:** Make required node affinity match Kubernetes semantics — `nodeSelectorTerms` are OR-of-terms (each term's `matchExpressions` ANDed) — instead of the current bug where all expressions across all terms are ANDed.

**Why:** Surfaced by the Phase 2 conformance design review. `collector::to_required_node_affinity` flattens every term's expressions into one list and `normalizer::matches_required_node_affinity` requires ALL to match. kube-scheduler treats `nodeSelectorTerms` as OR. So a pod with 2+ terms (e.g. "zone=a OR zone=b") is under-admitted: we mark nodes infeasible the real scheduler accepts. This affects both the offline planner (over-conservative consolidation) and shadow feasibility.

**Architecture:** Preserve the term grouping through the model. Change `Pod.required_node_affinity` from `Vec<NodeAffinityTerm>` (flat, ANDed) to `Vec<Vec<NodeAffinityTerm>>` — the outer Vec is OR-of-terms, each inner Vec is one term's `matchExpressions` (ANDed). Matching (codex-corrected): **skip empty modeled groups** (a term whose modeled expressions are empty — e.g. a `matchFields`-only term); if NO modeled groups remain, the pod is unconstrained (true) — exactly today's all-ignored behavior. Otherwise the pod matches a node iff ANY non-empty group matches (all its expressions match). Crucially, an empty group is NOT a match-all OR branch: for `(zone=a) OR (matchFields name=x)` → `[[zone=a],[]]`, we evaluate only `zone=a` (the `matchFields` branch is skipped, yielding a conservative false-negative for name=x-only nodes rather than an incorrect match-all). **`matchFields` stays ignored exactly as today** (rare; explicitly bucketed as expected divergence in Phase 2). This is the minimal fix: it corrects OR grouping for `matchExpressions` and never becomes more permissive than today.

**Tech Stack:** Rust; `model.rs`, `collector.rs`, `normalizer.rs`. Contained — no other consumers of `required_node_affinity`.

## Global Constraints

- **No regression to single-term or matchFields behavior:** a single `nodeSelectorTerm` with multiple `matchExpressions` still ANDs them (now one group); `matchFields`-only terms remain feasible-everywhere (empty group is vacuously true), identical to today.
- **Only OR grouping changes:** multi-term pods now match if ANY term matches. All other predicates untouched.
- **Empty `required_node_affinity` ⇒ unconstrained** (true), as today.
- `cargo fmt` + clean clippy; update the two existing node-affinity unit tests to the grouped shape; add an OR-semantics test proving the bug is fixed. Binds nothing.

## File Structure

- Modify `ksolver/src/model.rs` — `Pod.required_node_affinity: Vec<Vec<NodeAffinityTerm>>` (keep `NodeAffinityTerm` unchanged); update the doc comment.
- Modify `ksolver/src/collector.rs` — `to_required_node_affinity` returns `Vec<Vec<NodeAffinityTerm>>` (one inner Vec per `nodeSelectorTerm`).
- Modify `ksolver/src/normalizer.rs` — `matches_required_node_affinity` takes `&[Vec<NodeAffinityTerm>]`, OR-of-groups; keep the single-expression matcher factored out.

## Tasks

### Task 1: Model field shape
- [ ] In `model.rs`, change `pub required_node_affinity: Vec<NodeAffinityTerm>` to `pub required_node_affinity: Vec<Vec<NodeAffinityTerm>>` with a doc comment: "OR-of-terms: outer Vec is OR (nodeSelectorTerms), inner Vec is AND (a term's matchExpressions). Empty ⇒ unconstrained. matchFields are not modeled (a matchFields-only term is an empty inner Vec ⇒ vacuously matches)." Build; fix literals (compiler lists them). Commit.

### Task 2: Collector grouping
- [ ] In `collector.rs`, rewrite `to_required_node_affinity` to return `Vec<Vec<crate::model::NodeAffinityTerm>>`: for each `selector_term` in `node_selector_terms`, build ONE inner Vec from that term's `match_expressions` (map each expr to `NodeAffinityTerm`), and push it (even if empty — a matchFields-only or empty term becomes an empty group). Do NOT flatten across terms.
- [ ] Build. Commit.

### Task 3: Normalizer OR matching + tests
- [ ] In `normalizer.rs`, factor the per-expression check into `fn node_affinity_expr_matches(node_labels, term: &NodeAffinityTerm) -> bool` (the existing In/NotIn/Exists/DoesNotExist/Gt/Lt logic). Rewrite `matches_required_node_affinity(node_labels, groups: &[Vec<NodeAffinityTerm>]) -> bool`:
```rust
fn matches_required_node_affinity(
    node_labels: &BTreeMap<String, String>,
    groups: &[Vec<crate::model::NodeAffinityTerm>],
) -> bool {
    // Skip empty modeled groups (e.g. matchFields-only terms): they are NOT match-all
    // OR branches. If nothing modeled remains, the pod is unconstrained (today's behavior).
    let mut modeled = groups.iter().filter(|g| !g.is_empty()).peekable();
    if modeled.peek().is_none() {
        return true;
    }
    // OR across terms; AND across a term's expressions.
    modeled.any(|exprs| exprs.iter().all(|t| node_affinity_expr_matches(node_labels, t)))
}
```
- [ ] Update the two existing tests (`required_node_affinity_filters_infeasible_nodes`, `node_affinity_exists_operator`) to wrap their term lists in an outer group (`vec![vec![term...]]`).
- [ ] Add `node_affinity_or_of_terms_matches_either`: two SEPARATE groups `vec![vec![In zone=a], vec![In zone=b]]` → a node with zone=a matches, zone=b matches, zone=c does NOT. This is the regression test for the bug (pre-fix it would require BOTH and fail zone=a).
- [ ] Add `single_term_ands_expressions`: one group `vec![vec![In zone=a, Exists gpu]]` → matches only a node with BOTH; missing gpu ⇒ no match (proves intra-term AND preserved).
- [ ] Add `empty_group_is_not_match_all`: groups `vec![vec![In zone=a], vec![]]` (a modeled term OR a matchFields-only/empty term) → a node with zone=b does NOT match (the empty group must not make it match-all); a node with zone=a DOES match. And `vec![vec![]]` alone (all-empty) → matches any node (unconstrained fallback, no regression).
- [ ] Run `cargo test --features rust-cp-sat --lib node_affinity` → pass. fmt + clippy. Commit.

## Self-Review Notes
- OR-of-terms / AND-of-expressions now matches Kubernetes.
- Empty modeled groups are SKIPPED, not treated as match-all (codex fix): `(zone=a) OR (matchFields x)` evaluates only `zone=a` — never more permissive than today, no false-positive match-all.
- All-empty/matchFields-only ⇒ unconstrained fallback = today's behavior (no regression).
- Single-term AND preserved; only multi-term matchExpressions pods change (correctly).
- Contained to 3 files; no other consumers.
- Regression tests: `or_of_terms` fails pre-fix; `empty_group_is_not_match_all` guards the codex edge.
