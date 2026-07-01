# GPU Scheduler — Model `matchFields` in Required Node Affinity — Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: superpowers:subagent-driven-development or superpowers:executing-plans. Steps use checkbox (`- [ ]`) syntax.

**Goal:** Model `matchFields` in required node affinity (Kubernetes supports only `metadata.name`), removing the last node-affinity divergence the Phase 2 conformance harness buckets as "expected".

**Why:** After the OR-of-terms fix, the only remaining node-affinity gap vs kube-scheduler is `matchFields` (dropped today). `metadata.name` maps directly to `NormalizedNode.name`, so we can evaluate it exactly. Closing this lets `matchFields` pods move from the expected-divergence bucket into the strict must-match bucket — tighter conformance and "account for everything" fidelity.

**Architecture:** Today `required_node_affinity: Vec<Vec<NodeAffinityTerm>>` carries only a term's `matchExpressions` (evaluated against node LABELS). Extend each term-group to also carry its `matchFields` (evaluated against node FIELDS — only `metadata.name` is valid in k8s). Change the inner group type from `Vec<NodeAffinityTerm>` to a struct `NodeAffinityGroup { match_expressions: Vec<NodeAffinityTerm>, match_fields: Vec<NodeAffinityTerm> }`. Matching (OR across groups, AND within): a group matches iff every `match_expressions` term matches the node's labels AND every `match_fields` term matches the node's fields.

Two kube-semantics rules (codex, with the k8s node-affinity helper as source):
- **`matchFields` operators are only `In` and `NotIn`, single value, key `metadata.name`.** `node_affinity_field_matches`: for key `metadata.name`, `In` ⇒ values contain `node.name`, `NotIn` ⇒ they don't; any other operator or key ⇒ non-match (kube rejects them). NOT the label operators.
- **Empty required-affinity vs empty terms differ.** If the pod has NO required node affinity (`groups` empty) ⇒ unconstrained (true). If it HAS required affinity but every term is empty (no expressions, no fields) ⇒ selects NOTHING (false) — a nil/empty `NodeSelectorTerm` matches no node. So: `groups.is_empty() → true`; else drop empty groups; if none remain → false; else OR over the non-empty groups.

Because a `matchFields`-only group is now evaluable (not empty), it is matched, not skipped — removing the prior conservative false-negative.

**Tech Stack:** Rust; `model.rs`, `collector.rs`, `normalizer.rs`, and `conformance.rs` (drop matchFields from the divergence bucket). Contained.

## Global Constraints

- **OR/AND semantics preserved** from the prior fix; only add the `matchFields` dimension within a group.
- **`matchFields`: `metadata.name` only, operators `In`/`NotIn` only, EXACTLY ONE value** (codex; kube parse rule `len(values)==1`). Unknown key, unsupported operator, or value count ≠ 1 ⇒ that field term does not match (group fails), never match-all.
- **Empty-affinity vs empty-terms** (codex): no required affinity ⇒ true; required affinity with only empty terms ⇒ false (selects nothing). This corrects the prior fix's all-empty→true for the has-affinity case.
- **No regression:** `matchExpressions`-only groups behave exactly as after the OR fix (a real pod always has ≥1 non-empty term).
- `cargo fmt` + clean clippy; update existing node-affinity tests to the new group struct; add matchFields tests; update the conformance divergence test. Binds nothing.

## File Structure

- Modify `ksolver/src/model.rs` — add `NodeAffinityGroup { match_expressions, match_fields }`; change `Pod.required_node_affinity: Vec<NodeAffinityGroup>`.
- Modify `ksolver/src/collector.rs` — `to_required_node_affinity` fills both `match_expressions` and `match_fields` per term.
- Modify `ksolver/src/normalizer.rs` — matching evaluates expressions vs labels and fields vs `node.name`; thread `node.name` into the matcher.
- Modify `ksolver/src/conformance.rs` — `pod_has_unmodeled_constructs` no longer flags `matchFields` (now modeled); update its test.

## Tasks

### Task 1: Model group struct
- [ ] In `model.rs` add:
```rust
#[derive(Debug, Clone, Default, Serialize, Deserialize)]
pub struct NodeAffinityGroup {
    #[serde(default)] pub match_expressions: Vec<NodeAffinityTerm>,
    #[serde(default)] pub match_fields: Vec<NodeAffinityTerm>,
}
```
Change `pub required_node_affinity: Vec<Vec<NodeAffinityTerm>>` → `Vec<NodeAffinityGroup>` (update the doc comment to describe both dimensions + metadata.name-only fields). Build; fix literals with `..Default::default()`/the struct. Commit.

### Task 2: Collector fills both dimensions
- [ ] In `collector.rs` `to_required_node_affinity` (returns `Vec<NodeAffinityGroup>`): for each `selector_term`, map `match_expressions` and `match_fields` each into `Vec<NodeAffinityTerm>` (key/operator/values), and push `NodeAffinityGroup { match_expressions, match_fields }` (even if both empty — matcher skips those). Build. Commit.

### Task 3: Normalizer matching + tests
- [ ] In `normalizer.rs`, keep `node_affinity_expr_matches(node_labels, term)` for label expressions. Add:
```rust
fn node_affinity_field_matches(node_name: &str, term: &crate::model::NodeAffinityTerm) -> bool {
    if term.key != "metadata.name" {
        return false; // only metadata.name is a valid node field selector
    }
    // matchFields In/NotIn require EXACTLY ONE value (kube parse rule); else invalid => non-match.
    if term.values.len() != 1 {
        return false;
    }
    match term.operator.as_str() {
        "In" => term.values[0] == node_name,
        "NotIn" => term.values[0] != node_name,
        _ => false, // fields support only In/NotIn
    }
}
```
- [ ] Rewrite `matches_required_node_affinity(node_labels, node_name, groups: &[NodeAffinityGroup]) -> bool`:
```rust
fn matches_required_node_affinity(
    node_labels: &BTreeMap<String, String>,
    node_name: &str,
    groups: &[crate::model::NodeAffinityGroup],
) -> bool {
    if groups.is_empty() {
        return true; // no required node affinity => unconstrained
    }
    let non_empty: Vec<&crate::model::NodeAffinityGroup> = groups
        .iter()
        .filter(|g| !g.match_expressions.is_empty() || !g.match_fields.is_empty())
        .collect();
    if non_empty.is_empty() {
        return false; // affinity present but all terms empty => selects nothing
    }
    non_empty.iter().any(|g| {
        g.match_expressions
            .iter()
            .all(|t| node_affinity_expr_matches(node_labels, t))
            && g.match_fields
                .iter()
                .all(|t| node_affinity_field_matches(node_name, t))
    })
}
```
- [ ] Update the `feasible_on_node` call site to pass `&node.name`.
- [ ] Update existing tests (`required_node_affinity_filters_infeasible_nodes`, `node_affinity_exists_operator`, `node_affinity_or_of_terms_matches_either`, `node_affinity_single_term_ands_expressions`, `node_affinity_empty_group_is_not_match_all`) to the `NodeAffinityGroup` shape + new `node_name` arg. In `node_affinity_empty_group_is_not_match_all`, change the all-empty (`vec![NodeAffinityGroup::default()]`) assertion from true to **false** (has-affinity-with-only-empty-terms selects nothing per kube), and keep the mixed `(zone=a) OR (empty)` case (zone=b ⇒ false, zone=a ⇒ true).
- [ ] Add `node_affinity_matchfields_metadata_name`: a group with `match_fields = [metadata.name In [node-a]]`, empty expressions → matches node "node-a", NOT "node-b". `NotIn` inverts. A mixed group `expressions=[zone In a], fields=[metadata.name In node-a]` matches only node-a with zone=a (node-a zone=b ⇒ false; node-b zone=a ⇒ false). An unsupported field operator (e.g. `Exists`) or non-`metadata.name` key ⇒ non-match. Run `cargo test --lib node_affinity`. fmt + clippy. Commit.

### Task 4: Conformance bucket update
- [ ] In `conformance.rs` `pod_has_unmodeled_constructs`, REMOVE the `matchFields` check (matchFields is now modeled). Update the doc comment. Update `unmodeled_constructs_detects_expected_divergence` so the matchFields pod is now NOT flagged (assert false) — leaving pod affinity/anti-affinity, spread, priority as the only expected-divergence constructs. Run conformance tests. fmt + clippy. Commit.

## Self-Review Notes
- `matchFields` (metadata.name, In/NotIn, exactly one value) now modeled exactly against `node.name` (codex); value-count ≠ 1 ⇒ non-match; removes the prior conservative false-negative.
- Empty-affinity ⇒ true; has-affinity-with-only-empty-terms ⇒ false (codex; corrects prior all-empty→true). Unknown field key/operator never match-all.
- OR/AND semantics preserved for `matchExpressions`.
- Conformance divergence bucket tightened (matchFields moves to strict).
- Contained to 4 files; tests updated + new matchFields tests.
