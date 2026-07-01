# GPU Scheduler — Fix: Required Node Affinity OR-of-Terms Semantics — Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: superpowers:subagent-driven-development or superpowers:executing-plans. Steps use checkbox (`- [ ]`) syntax.

**Goal:** Make required node affinity match Kubernetes semantics — `nodeSelectorTerms` are OR-of-terms (each term's `matchExpressions` ANDed) — instead of the current bug where all expressions across all terms are ANDed.

**Why:** Surfaced by the Phase 2 conformance design review. `collector::to_required_node_affinity` flattens every term's expressions into one list and `normalizer::matches_required_node_affinity` requires ALL to match. kube-scheduler treats `nodeSelectorTerms` as OR. So a pod with 2+ terms (e.g. "zone=a OR zone=b") is under-admitted: we mark nodes infeasible the real scheduler accepts. This affects both the offline planner (over-conservative consolidation) and shadow feasibility.

**Architecture:** Preserve the term grouping through the model. Change `Pod.required_node_affinity` from `Vec<NodeAffinityTerm>` (flat, ANDed) to `Vec<Vec<NodeAffinityTerm>>` — the outer Vec is OR-of-terms, each inner Vec is one term's `matchExpressions` (ANDed). Matching: no groups ⇒ no constraint (true); otherwise the pod matches a node iff ANY group matches, where a group matches iff ALL its expressions match (an empty group — a term with no modeled expressions — is vacuously true, matching today's behavior). **`matchFields` stays ignored exactly as today** (rare; explicitly bucketed as expected divergence in Phase 2), so a term carrying only `matchFields` becomes an empty group → still feasible-everywhere, i.e. NO regression versus current behavior. This is the minimal fix: it only corrects the OR grouping for `matchExpressions` and changes nothing else.

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
    if groups.is_empty() {
        return true; // no required node affinity ⇒ unconstrained
    }
    // OR across terms; AND across a term's expressions (empty group ⇒ vacuously true).
    groups
        .iter()
        .any(|exprs| exprs.iter().all(|t| node_affinity_expr_matches(node_labels, t)))
}
```
- [ ] Update the two existing tests (`required_node_affinity_filters_infeasible_nodes`, `node_affinity_exists_operator`) to wrap their term lists in an outer group (`vec![vec![term...]]`).
- [ ] Add `node_affinity_or_of_terms_matches_either`: two SEPARATE groups `vec![vec![In zone=a]], vec![In zone=b]]` → a node with zone=a matches, zone=b matches, zone=c does NOT. This is the regression test for the bug (pre-fix it would require BOTH and fail zone=a).
- [ ] Add `single_term_ands_expressions`: one group `vec![vec![In zone=a, Exists gpu]]` → matches only a node with BOTH; missing gpu ⇒ no match (proves intra-term AND preserved).
- [ ] Run `cargo test --features rust-cp-sat --lib node_affinity` → pass. fmt + clippy. Commit.

## Self-Review Notes
- OR-of-terms / AND-of-expressions now matches Kubernetes.
- Single-term and matchFields-only behavior unchanged (no regression); only multi-term pods change (correctly).
- Contained to 3 files; no other consumers.
- Regression test (`or_of_terms`) fails pre-fix, passes post-fix.
