//! Feasibility conformance harness: compares our `feasible_on_node` verdict against real
//! kube-scheduler Filter decisions (via kube-scheduler-simulator), per (pod, node) pair.
//!
//! For each pair we get two verdicts — ours (`node_feasibility_reasons(...).is_empty()`)
//! and the scheduler's (present the simulator a snapshot with exactly that one node, empty
//! of other pods, plus the pod; the pod binds ⇒ Filter passed; unschedulable ⇒ Filter
//! failed). One node isolates Filter from Score. This module holds the pure classification
//! and reporting logic; the simulator round-trip reuses `verifier`'s client.

use crate::verifier::{
    clone_as_unscheduled_verification_pod, pod_assigned_node, pod_scope, SimulatorExportPayload,
    SimulatorImportPayload, SimulatorResources, FILTER_RESULT_ANNOTATION,
};
use k8s_openapi::api::core::v1 as corev1;
use serde::Serialize;

/// Build a simulator import payload isolating ONE node and ONE pod so a successful bind means
/// that node passed Filter (Score is moot with a single candidate). The node carries raw
/// allocatable and NO other pods. Includes the pod's referenced PVCs + their StorageClasses +
/// the pod's Namespace + all PriorityClasses, and ALL PVs (kube-scheduler's VolumeBinding
/// filter may need available PVs the pod does not directly reference to satisfy an unbound PVC).
pub(crate) fn build_single_node_payload(
    raw: &SimulatorResources,
    pod: &corev1::Pod,
    node: &corev1::Node,
) -> SimulatorImportPayload {
    let ns = pod.metadata.namespace.clone().unwrap_or_default();

    // PVCs the pod references by name (same namespace).
    let referenced_claims: std::collections::BTreeSet<String> = pod
        .spec
        .as_ref()
        .map(|s| {
            s.volumes
                .as_ref()
                .map(|vols| {
                    vols.iter()
                        .filter_map(|v| v.persistent_volume_claim.as_ref())
                        .map(|pvc| pvc.claim_name.clone())
                        .collect()
                })
                .unwrap_or_default()
        })
        .unwrap_or_default();

    let pvcs: Vec<corev1::PersistentVolumeClaim> = raw
        .pvcs
        .iter()
        .filter(|c| {
            c.metadata.namespace.as_deref() == Some(ns.as_str())
                && c.metadata
                    .name
                    .as_ref()
                    .map(|n| referenced_claims.contains(n))
                    .unwrap_or(false)
        })
        .cloned()
        .collect();

    // StorageClasses referenced by those PVCs.
    let referenced_scs: std::collections::BTreeSet<String> = pvcs
        .iter()
        .filter_map(|c| c.spec.as_ref().and_then(|s| s.storage_class_name.clone()))
        .collect();
    let storage_classes: Vec<_> = raw
        .storage_classes
        .iter()
        .filter(|sc| {
            sc.metadata
                .name
                .as_ref()
                .map(|n| referenced_scs.contains(n))
                .unwrap_or(false)
        })
        .cloned()
        .collect();

    let namespaces: Vec<_> = raw
        .namespaces
        .iter()
        .filter(|n| n.metadata.name.as_deref() == Some(ns.as_str()))
        .cloned()
        .collect();

    SimulatorImportPayload {
        pods: vec![clone_as_unscheduled_verification_pod(pod.clone())],
        nodes: vec![node.clone()],
        pvs: raw.pvs.clone(), // ALL PVs — VolumeBinding may need non-referenced candidates.
        pvcs,
        storage_classes,
        priority_classes: raw.priority_classes.clone(),
        namespaces,
        scheduler_config: crate::verifier::default_scheduler_config(),
    }
}

/// True when the pod carries a construct that `feasible_on_node` does not model (so a
/// divergence from kube-scheduler is EXPECTED, not a bug): required pod affinity/anti-affinity,
/// unsupported DoNotSchedule topology spread shapes, or non-empty priority/priorityClassName.
/// Required node affinity (both matchExpressions OR-of-terms and matchFields metadata.name) and
/// the supported hard topology-spread subset are now modeled, so they are NOT bucketed here.
pub(crate) fn pod_expected_divergence_reasons(pod: &corev1::Pod) -> Vec<String> {
    let mut reasons = Vec::new();
    let Some(spec) = pod.spec.as_ref() else {
        return reasons;
    };
    if spec.priority.unwrap_or(0) != 0 {
        reasons.push("priority".to_string());
    }
    if spec
        .priority_class_name
        .as_ref()
        .map(|p| !p.is_empty())
        .unwrap_or(false)
    {
        reasons.push("priorityClassName".to_string());
    }
    if let Some(tsc) = spec.topology_spread_constraints.as_ref() {
        if tsc
            .iter()
            .any(|c| c.when_unsatisfiable == "DoNotSchedule" && !modeled_hard_spread(c))
        {
            reasons.push("topologySpreadConstraints/DoNotSchedule".to_string());
        }
    }
    if let Some(aff) = spec.affinity.as_ref() {
        let required_terms = |t: &Option<Vec<corev1::PodAffinityTerm>>| {
            t.as_ref().map(|v| !v.is_empty()).unwrap_or(false)
        };
        if aff
            .pod_affinity
            .as_ref()
            .map(|a| required_terms(&a.required_during_scheduling_ignored_during_execution))
            .unwrap_or(false)
        {
            reasons.push("requiredPodAffinity".to_string());
        }
        if aff
            .pod_anti_affinity
            .as_ref()
            .map(|a| required_terms(&a.required_during_scheduling_ignored_during_execution))
            .unwrap_or(false)
        {
            reasons.push("requiredPodAntiAffinity".to_string());
        }
    }
    reasons
}

fn modeled_hard_spread(c: &corev1::TopologySpreadConstraint) -> bool {
    if c.when_unsatisfiable != "DoNotSchedule" || c.max_skew <= 0 || c.topology_key.is_empty() {
        return false;
    }
    if c.min_domains.is_some()
        || c.node_affinity_policy.is_some()
        || c.node_taints_policy.is_some()
        || c.match_label_keys
            .as_ref()
            .is_some_and(|keys| !keys.is_empty())
    {
        return false;
    }
    c.label_selector
        .as_ref()
        .and_then(crate::collector::label_selector_to_reqs)
        .map(|reqs| !reqs.is_empty())
        .unwrap_or(false)
}

pub(crate) fn pod_has_unmodeled_constructs(pod: &corev1::Pod) -> bool {
    !pod_expected_divergence_reasons(pod).is_empty()
}

/// A key that is EQUAL for two nodes iff the kube-scheduler **Filter** verdict for `pod` is
/// guaranteed identical on both (so one simulator probe can stand in for the whole class). Returns
/// `None` — meaning "do NOT dedup this pod; probe every node individually" — whenever the verdict
/// could depend on a node attribute this key does not capture. Correctness rule: a `None` or a
/// coarser key only ever *under*-merges (slower, never wrong); it must never merge two nodes that
/// could differ.
///
/// For conform's isolated probe (ONE node + ONE pod, no other pods), the Filter verdict depends on:
/// node allocatable (NodeResourcesFit), the values of the node labels the pod's nodeSelector /
/// required node-affinity `matchExpressions` reference (NodeAffinity), and node taints
/// (TaintToleration). Inter-pod affinity/anti-affinity and topology-spread are trivially satisfiable
/// with no other pods, so they don't affect the verdict and are safely ignored. Constructs whose
/// verdict CAN depend on un-keyed node attributes force `None`: `matchFields` (node name/fields) and
/// PVC volumes (VolumeBinding topology references node labels via the PV/PVC, not the pod).
pub(crate) fn pod_filter_equivalence_key(pod: &corev1::Pod, node: &corev1::Node) -> Option<String> {
    let spec = pod.spec.as_ref()?;

    // PVC volumes → VolumeBinding may filter on node topology labels we don't enumerate. Bail.
    if spec
        .volumes
        .iter()
        .flatten()
        .any(|v| v.persistent_volume_claim.is_some())
    {
        return None;
    }

    // Node-label keys the pod's Filter verdict depends on (nodeSelector + required node-affinity
    // matchExpressions). matchFields ⇒ node-name/field dependent ⇒ bail (no dedup).
    let mut label_keys: std::collections::BTreeSet<String> = spec
        .node_selector
        .iter()
        .flatten()
        .map(|(k, _)| k.clone())
        .collect();
    if let Some(node_affinity) = spec
        .affinity
        .as_ref()
        .and_then(|a| a.node_affinity.as_ref())
    {
        if let Some(required) = node_affinity
            .required_during_scheduling_ignored_during_execution
            .as_ref()
        {
            for term in &required.node_selector_terms {
                if term.match_fields.as_ref().is_some_and(|f| !f.is_empty()) {
                    return None;
                }
                for expr in term.match_expressions.iter().flatten() {
                    label_keys.insert(expr.key.clone());
                }
            }
        }
        // preferredDuringScheduling affects Score, not Filter — irrelevant to feasibility.
    }

    let allocatable: Vec<String> = node
        .status
        .as_ref()
        .and_then(|s| s.allocatable.as_ref())
        .map(|m| m.iter().map(|(k, v)| format!("{k}={}", v.0)).collect())
        .unwrap_or_default();

    let labels = node.metadata.labels.clone().unwrap_or_default();
    let referenced: Vec<String> = label_keys
        .iter()
        .map(|k| {
            let v = labels.get(k).map(String::as_str).unwrap_or("\u{0}absent");
            format!("{k}={v}")
        })
        .collect();

    let mut taints: Vec<String> = node
        .spec
        .as_ref()
        .and_then(|s| s.taints.as_ref())
        .map(|ts| {
            ts.iter()
                .map(|t| {
                    format!(
                        "{}={}:{}",
                        t.key,
                        t.value.as_deref().unwrap_or(""),
                        t.effect
                    )
                })
                .collect()
        })
        .unwrap_or_default();
    taints.sort();

    Some(format!(
        "alloc[{}]|lbl[{}]|taint[{}]",
        allocatable.join(","),
        referenced.join(","),
        taints.join(",")
    ))
}

/// Group candidate nodes into probe-classes for a pod. Each returned inner Vec is one class whose
/// representative (element 0) is probed once against the simulator; the verdict is replicated to the
/// rest. With `dedup=false`, or for nodes whose `pod_filter_equivalence_key` is `None`, each node is
/// its own singleton class (probed individually — the exact prior behavior). Node order is preserved.
pub(crate) fn group_nodes_for_probe<'a>(
    pod: &corev1::Pod,
    nodes: &[&'a crate::model::NormalizedNode],
    raw_nodes: &std::collections::BTreeMap<String, &corev1::Node>,
    dedup: bool,
) -> Vec<Vec<&'a crate::model::NormalizedNode>> {
    let mut groups: Vec<Vec<&crate::model::NormalizedNode>> = Vec::new();
    let mut index_by_key: std::collections::HashMap<String, usize> =
        std::collections::HashMap::new();
    for node in nodes {
        let key = if dedup {
            raw_nodes
                .get(&node.name)
                .and_then(|raw| pod_filter_equivalence_key(pod, raw))
        } else {
            None
        };
        match key {
            Some(k) => {
                if let Some(&i) = index_by_key.get(&k) {
                    groups[i].push(node);
                } else {
                    index_by_key.insert(k, groups.len());
                    groups.push(vec![node]);
                }
            }
            None => groups.push(vec![node]),
        }
    }
    groups
}

/// The scheduler's Filter verdict for `pod_scope_target` on `node_name`, read from the export:
/// feasible iff the pod bound to that node (selected-node annotation OR spec.nodeName), else
/// infeasible with the filter-result annotation (if any) as the reason.
pub(crate) fn scheduler_feasible(
    export: &SimulatorExportPayload,
    node_name: &str,
    pod_scope_target: &str,
) -> (bool, Option<String>) {
    let Some(pod) = export
        .pods
        .iter()
        .find(|p| pod_scope(p) == pod_scope_target)
    else {
        return (false, Some("pod absent from simulator export".to_string()));
    };
    if pod_assigned_node(pod).as_deref() == Some(node_name) {
        return (true, None);
    }
    let reason = pod
        .metadata
        .annotations
        .as_ref()
        .and_then(|a| a.get(FILTER_RESULT_ANNOTATION).cloned())
        .or_else(|| Some("unschedulable (no filter-result)".to_string()));
    (false, reason)
}

/// Outcome of comparing our feasibility verdict to the scheduler's for one (pod, node) pair.
/// `FalsePositive` (we say feasible, the scheduler rejects) is the dangerous case — it means
/// we would recommend a placement the real scheduler refuses.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum Verdict {
    Agree,
    FalsePositive,
    FalseNegative,
}

/// Classify a pair from the two boolean verdicts.
pub fn classify(ours_feasible: bool, scheduler_feasible: bool) -> Verdict {
    match (ours_feasible, scheduler_feasible) {
        (true, true) | (false, false) => Verdict::Agree,
        (true, false) => Verdict::FalsePositive,
        (false, true) => Verdict::FalseNegative,
    }
}

/// Tally of verdicts across all compared pairs.
#[derive(Debug, Default, Clone, Copy, Serialize)]
pub struct ConfusionMatrix {
    pub agree: usize,
    pub false_positive: usize,
    pub false_negative: usize,
}

impl ConfusionMatrix {
    pub fn record(&mut self, v: Verdict) {
        match v {
            Verdict::Agree => self.agree += 1,
            Verdict::FalsePositive => self.false_positive += 1,
            Verdict::FalseNegative => self.false_negative += 1,
        }
    }

    pub fn total(&self) -> usize {
        self.agree + self.false_positive + self.false_negative
    }

    /// Fraction of pairs where we agree with the scheduler. An empty matrix is vacuously 1.0.
    pub fn agreement_rate(&self) -> f64 {
        if self.total() == 0 {
            1.0
        } else {
            self.agree as f64 / self.total() as f64
        }
    }
}

/// A strict-bucket disagreement worth reporting.
#[derive(Debug, Clone, Serialize)]
pub struct Mismatch {
    pub pod: String,
    pub node: String,
    pub verdict: Verdict,
    pub ours_reasons: Vec<String>,
    pub scheduler_reason: Option<String>,
    pub expected_divergence_reasons: Vec<String>,
}

/// Full conformance result: strict (plain pods, must match) vs expected-divergence buckets.
#[derive(Debug, Default, Clone, Serialize)]
pub struct ConformanceReport {
    pub strict: ConfusionMatrix,
    pub expected_divergence: ConfusionMatrix,
    pub expected_divergence_reason_counts: std::collections::BTreeMap<String, usize>,
    pub pods_evaluated: usize,
    pub cordoned_nodes_skipped: usize,
    /// Total non-cordoned candidate nodes in the cluster.
    pub nodes_total: usize,
    /// Nodes actually probed per pod. Less than `nodes_total` when `--max-nodes` caps the set — the
    /// run is then a SAMPLED spot-check, not full-cluster coverage (surfaced in `render()`).
    pub nodes_evaluated: usize,
    /// Actual simulator round-trips made. With `--dedup-nodes`, feasibility-identical nodes share one
    /// probe, so this is < the number of (pod, node) pairs recorded — a pure speedup, same verdicts.
    pub simulator_probes: usize,
    pub mismatches: Vec<Mismatch>,
    pub expected_divergence_mismatches: Vec<Mismatch>,
}

impl ConformanceReport {
    /// True when ksolver says a strict-bucket pod/node pair is feasible but kube-scheduler rejects
    /// it. This is the CI-gate condition because expected-divergence mismatches are advisory.
    pub fn has_strict_false_positives(&self) -> bool {
        self.strict.false_positive > 0
    }

    pub fn strict_gate_status(&self) -> &'static str {
        if self.has_strict_false_positives() {
            "fail"
        } else {
            "pass"
        }
    }

    /// Human-readable summary. FalsePositives (we say feasible, scheduler rejects) are the
    /// dangerous ones and are listed first.
    pub fn render(&self) -> String {
        let mut out = String::new();
        out.push_str(&format!(
            "feasibility conformance: {} pods, {} strict pairs (agreement {:.1}%), {} expected-divergence pairs; {} cordoned nodes skipped\n",
            self.pods_evaluated,
            self.strict.total(),
            self.strict.agreement_rate() * 100.0,
            self.expected_divergence.total(),
            self.cordoned_nodes_skipped,
        ));
        if self.nodes_evaluated < self.nodes_total {
            out.push_str(&format!(
                "  NOTE: node-sampled spot-check — probed {} of {} candidate nodes per pod (not full-cluster coverage)\n",
                self.nodes_evaluated, self.nodes_total
            ));
        }
        let pairs = self.strict.total() + self.expected_divergence.total();
        if self.simulator_probes > 0 && self.simulator_probes < pairs {
            out.push_str(&format!(
                "  node-dedup: {} simulator probes for {} (pod,node) pairs (feasibility-identical nodes shared a probe; same verdicts)\n",
                self.simulator_probes, pairs
            ));
        }
        out.push_str(&format!(
            "  strict: agree={} false_positive={} false_negative={}\n",
            self.strict.agree, self.strict.false_positive, self.strict.false_negative
        ));
        out.push_str(&format!("  strict-gate: {}\n", self.strict_gate_status()));
        out.push_str(&format!(
            "  expected-divergence: agree={} false_positive={} false_negative={}\n",
            self.expected_divergence.agree,
            self.expected_divergence.false_positive,
            self.expected_divergence.false_negative
        ));
        if !self.expected_divergence_reason_counts.is_empty() {
            let counts = self
                .expected_divergence_reason_counts
                .iter()
                .map(|(reason, count)| format!("{reason}={count}"))
                .collect::<Vec<_>>()
                .join(", ");
            out.push_str(&format!("  expected-divergence reasons: {counts}\n"));
        }
        let mut sorted = self.mismatches.clone();
        // FalsePositive first (dangerous), then FalseNegative.
        sorted.sort_by_key(|m| match m.verdict {
            Verdict::FalsePositive => 0,
            Verdict::FalseNegative => 1,
            Verdict::Agree => 2,
        });
        for m in sorted.iter().take(50) {
            let kind = match m.verdict {
                Verdict::FalsePositive => "FALSE-POSITIVE (we feasible, scheduler rejects)",
                Verdict::FalseNegative => "false-negative (we infeasible, scheduler accepts)",
                Verdict::Agree => "agree",
            };
            out.push_str(&format!(
                "  {kind}: {}@{} ours=[{}] scheduler={}\n",
                m.pod,
                m.node,
                m.ours_reasons.join(","),
                m.scheduler_reason.as_deref().unwrap_or("-"),
            ));
        }
        if !self.expected_divergence_mismatches.is_empty() {
            out.push_str("  expected-divergence mismatches (bucketed separately):\n");
            let mut expected = self.expected_divergence_mismatches.clone();
            expected.sort_by_key(|m| match m.verdict {
                Verdict::FalsePositive => 0,
                Verdict::FalseNegative => 1,
                Verdict::Agree => 2,
            });
            for m in expected.iter().take(25) {
                let kind = match m.verdict {
                    Verdict::FalsePositive => "false-positive in expected-divergence bucket",
                    Verdict::FalseNegative => "false-negative in expected-divergence bucket",
                    Verdict::Agree => "agree",
                };
                out.push_str(&format!(
                    "    {kind}: {}@{} expected=[{}] ours=[{}] scheduler={}\n",
                    m.pod,
                    m.node,
                    m.expected_divergence_reasons.join(","),
                    m.ours_reasons.join(","),
                    m.scheduler_reason.as_deref().unwrap_or("-"),
                ));
            }
        }
        out
    }
}

/// Node used for OUR feasibility check: raw allocatable (effective_capacity=allocatable,
/// reserved=0) so both sides test the same capacity the empty simulator node exposes.
fn conformance_node(n: &crate::model::NormalizedNode) -> crate::model::NormalizedNode {
    let mut c = n.clone();
    c.effective_capacity = n.allocatable.clone();
    c.reserved = crate::model::ResourceList::default();
    c
}

/// Run the conformance harness against a live cluster + kube-scheduler-simulator. Compares our
/// `feasible_on_node` verdict to the scheduler's Filter verdict for each (pending pod, node)
/// pair. Read-only on the real cluster; only the simulator (a sandbox) is scheduled against.
pub async fn run_conformance(
    kubeconfig: &str,
    cluster_name: &str,
    simulator_url: &str,
    sample: usize,
    max_nodes: usize,
    dedup_nodes: bool,
) -> anyhow::Result<ConformanceReport> {
    use crate::normalizer::{
        build_volumes_by_claim, node_feasibility_reasons, Normalizer, Options,
    };

    let collector =
        crate::collector::KubeCollector::new(cluster_name.to_string(), kubeconfig.to_string())
            .await?;
    let snapshot = collector.collect().await?;
    let raw = crate::verifier::collect_simulator_resources(kubeconfig).await?;
    let pricing = crate::pricing::load_pricing_catalog("").unwrap_or_default();
    let normalized = Normalizer::new(pricing, Options::default()).normalize(&snapshot);
    let volumes = build_volumes_by_claim(&snapshot);
    let opts = Options::default();

    let raw_pods: std::collections::BTreeMap<String, &corev1::Pod> =
        raw.pods.iter().map(|p| (pod_scope(p), p)).collect();
    let raw_nodes: std::collections::BTreeMap<String, &corev1::Node> = raw
        .nodes
        .iter()
        .filter_map(|n| n.metadata.name.clone().map(|name| (name, n)))
        .collect();

    // Candidate nodes: exclude cordoned (spec.unschedulable) — we don't model NodeUnschedulable.
    let mut candidate_nodes: Vec<&crate::model::NormalizedNode> = Vec::new();
    let mut cordoned_nodes_skipped = 0;
    for node in &normalized.nodes {
        let cordoned = raw_nodes
            .get(&node.name)
            .and_then(|n| n.spec.as_ref())
            .and_then(|s| s.unschedulable)
            .unwrap_or(false);
        if cordoned {
            cordoned_nodes_skipped += 1;
        } else {
            candidate_nodes.push(node);
        }
    }

    // Optionally cap the candidate-node set. conform is O(pods x nodes) — one reset+import+poll per
    // (pod, node) — so on a large fleet a full run is intractable. `max_nodes > 0` truncates to a
    // sample, turning the run into an honest spot-check (recorded as nodes_evaluated < nodes_total
    // and flagged in the report). 0 means "all nodes" (unchanged default).
    let nodes_total = candidate_nodes.len();
    if max_nodes > 0 && candidate_nodes.len() > max_nodes {
        candidate_nodes.truncate(max_nodes);
    }
    let nodes_evaluated = candidate_nodes.len();

    // Pending pods: unscheduled (no node) and not terminal.
    let pending: Vec<&crate::model::Pod> = snapshot
        .pods
        .iter()
        .filter(|p| p.node_name.is_empty() && p.phase != "Succeeded" && p.phase != "Failed")
        .take(sample)
        .collect();

    let mut report = ConformanceReport {
        pods_evaluated: pending.len(),
        cordoned_nodes_skipped,
        nodes_total,
        nodes_evaluated,
        ..Default::default()
    };

    // Only nodes with a raw counterpart can be probed/recorded (matches the prior per-node `continue`
    // on a missing raw node). Pod-independent, so compute once; the per-pod grouping happens below.
    let probeable: Vec<&crate::model::NormalizedNode> = candidate_nodes
        .iter()
        .copied()
        .filter(|n| raw_nodes.contains_key(&n.name))
        .collect();

    for pod in &pending {
        let scope = format!("{}/{}", pod.namespace, pod.name);
        let Some(raw_pod) = raw_pods.get(&scope) else {
            continue;
        };
        let expected_divergence_reasons = pod_expected_divergence_reasons(raw_pod);
        let expected_divergence = pod_has_unmodeled_constructs(raw_pod);

        // Group feasibility-identical nodes so one simulator probe covers the whole class. With
        // dedup off (default), every class is a singleton == the exact prior per-node behavior.
        let groups = group_nodes_for_probe(raw_pod, &probeable, &raw_nodes, dedup_nodes);

        for group in &groups {
            // Probe the representative ONCE; its Filter verdict holds for the whole class (the
            // equivalence key captures every attribute that could change the verdict).
            let rep = group[0];
            let rep_raw = raw_nodes.get(&rep.name).expect("probeable node has raw");
            let payload = build_single_node_payload(&raw, raw_pod, rep_raw);
            let export =
                crate::verifier::schedule_snapshot(simulator_url, &payload, &scope).await?;
            let (sched_feasible, sched_reason) = scheduler_feasible(&export, &rep.name, &scope);
            report.simulator_probes += 1;

            for node in group {
                // `ours` is computed per-node (cheap, no probe) so replication only ever reuses the
                // simulator's Filter verdict — the sole class-invariant we depend on.
                let ours_reasons =
                    node_feasibility_reasons(pod, &conformance_node(node), &volumes, &opts);
                let ours_feasible = ours_reasons.is_empty();
                let verdict = classify(ours_feasible, sched_feasible);
                if expected_divergence {
                    report.expected_divergence.record(verdict);
                    for reason in &expected_divergence_reasons {
                        *report
                            .expected_divergence_reason_counts
                            .entry(reason.clone())
                            .or_insert(0) += 1;
                    }
                    if verdict != Verdict::Agree {
                        report.expected_divergence_mismatches.push(Mismatch {
                            pod: scope.clone(),
                            node: node.name.clone(),
                            verdict,
                            ours_reasons,
                            scheduler_reason: sched_reason.clone(),
                            expected_divergence_reasons: expected_divergence_reasons.clone(),
                        });
                    }
                } else {
                    report.strict.record(verdict);
                    if verdict != Verdict::Agree {
                        report.mismatches.push(Mismatch {
                            pod: scope.clone(),
                            node: node.name.clone(),
                            verdict,
                            ours_reasons,
                            scheduler_reason: sched_reason.clone(),
                            expected_divergence_reasons: Vec::new(),
                        });
                    }
                }
            }
        }
    }

    Ok(report)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn classify_covers_all_combinations() {
        assert_eq!(classify(true, true), Verdict::Agree);
        assert_eq!(classify(false, false), Verdict::Agree);
        assert_eq!(classify(true, false), Verdict::FalsePositive);
        assert_eq!(classify(false, true), Verdict::FalseNegative);
    }

    #[test]
    fn render_flags_node_sampling_as_spot_check() {
        // When --max-nodes caps the probed set, the report must NOT read as full-cluster coverage.
        let sampled = ConformanceReport {
            pods_evaluated: 3,
            nodes_total: 113,
            nodes_evaluated: 10,
            ..Default::default()
        };
        let rendered = sampled.render();
        assert!(
            rendered.contains("node-sampled spot-check") && rendered.contains("10 of 113"),
            "sampled run must disclose partial coverage; got:\n{rendered}"
        );

        // A full run (nodes_evaluated == nodes_total) must NOT print the sampling note.
        let full = ConformanceReport {
            pods_evaluated: 3,
            nodes_total: 4,
            nodes_evaluated: 4,
            ..Default::default()
        };
        assert!(!full.render().contains("node-sampled spot-check"));
    }

    use crate::verifier::{SimulatorExportPayload, SimulatorResources};
    use k8s_openapi::api::core::v1 as corev1;

    fn pod_named(ns: &str, name: &str) -> corev1::Pod {
        corev1::Pod {
            metadata: kube::api::ObjectMeta {
                name: Some(name.to_string()),
                namespace: Some(ns.to_string()),
                ..Default::default()
            },
            spec: Some(corev1::PodSpec::default()),
            ..Default::default()
        }
    }

    fn node_named(name: &str) -> corev1::Node {
        corev1::Node {
            metadata: kube::api::ObjectMeta {
                name: Some(name.to_string()),
                ..Default::default()
            },
            ..Default::default()
        }
    }

    #[test]
    fn single_node_payload_isolates_one_pod_and_node() {
        let raw = SimulatorResources {
            pvs: vec![corev1::PersistentVolume {
                metadata: kube::api::ObjectMeta {
                    name: Some("pv-1".to_string()),
                    ..Default::default()
                },
                ..Default::default()
            }],
            ..Default::default()
        };
        let payload = build_single_node_payload(&raw, &pod_named("ns", "p"), &node_named("n1"));
        assert_eq!(payload.nodes.len(), 1);
        assert_eq!(payload.nodes[0].metadata.name.as_deref(), Some("n1"));
        assert_eq!(payload.pods.len(), 1);
        // Cloned as unscheduled: node_name cleared.
        assert_eq!(
            payload.pods[0]
                .spec
                .as_ref()
                .and_then(|s| s.node_name.clone()),
            None
        );
        // ALL PVs included even though the pod references none.
        assert_eq!(payload.pvs.len(), 1);
    }

    #[test]
    fn conformance_node_uses_raw_allocatable_capacity() {
        let node = crate::model::NormalizedNode {
            name: "n1".to_string(),
            allocatable: crate::model::ResourceList {
                milli_cpu: 16_000,
                memory_bytes: 128,
                ephemeral_storage: 64,
                pods: 110,
            },
            effective_capacity: crate::model::ResourceList {
                milli_cpu: 12_000,
                memory_bytes: 96,
                ephemeral_storage: 32,
                pods: 100,
            },
            reserved: crate::model::ResourceList {
                milli_cpu: 4_000,
                memory_bytes: 32,
                ephemeral_storage: 32,
                pods: 10,
            },
            ..Default::default()
        };

        let conformance = conformance_node(&node);

        assert_eq!(
            conformance.effective_capacity.milli_cpu,
            node.allocatable.milli_cpu
        );
        assert_eq!(
            conformance.effective_capacity.memory_bytes,
            node.allocatable.memory_bytes
        );
        assert_eq!(
            conformance.effective_capacity.ephemeral_storage,
            node.allocatable.ephemeral_storage
        );
        assert_eq!(conformance.effective_capacity.pods, node.allocatable.pods);
        assert_eq!(conformance.reserved.milli_cpu, 0);
        assert_eq!(conformance.reserved.memory_bytes, 0);
        assert_eq!(conformance.reserved.ephemeral_storage, 0);
        assert_eq!(conformance.reserved.pods, 0);
    }

    #[test]
    fn unmodeled_constructs_detects_expected_divergence() {
        // Plain pod: modeled.
        assert!(!pod_has_unmodeled_constructs(&pod_named("ns", "plain")));

        // Required pod anti-affinity: unmodeled.
        let mut aa = pod_named("ns", "aa");
        aa.spec.as_mut().unwrap().affinity = Some(corev1::Affinity {
            pod_anti_affinity: Some(corev1::PodAntiAffinity {
                required_during_scheduling_ignored_during_execution: Some(vec![
                    corev1::PodAffinityTerm {
                        topology_key: "kubernetes.io/hostname".to_string(),
                        ..Default::default()
                    },
                ]),
                ..Default::default()
            }),
            ..Default::default()
        });
        let aa_reasons = pod_expected_divergence_reasons(&aa);
        assert!(pod_has_unmodeled_constructs(&aa));
        assert_eq!(aa_reasons, vec!["requiredPodAntiAffinity".to_string()]);

        // matchFields node affinity: now MODELED (metadata.name) => NOT bucketed.
        let mut mf = pod_named("ns", "mf");
        mf.spec.as_mut().unwrap().affinity = Some(corev1::Affinity {
            node_affinity: Some(corev1::NodeAffinity {
                required_during_scheduling_ignored_during_execution: Some(corev1::NodeSelector {
                    node_selector_terms: vec![corev1::NodeSelectorTerm {
                        match_fields: Some(vec![corev1::NodeSelectorRequirement {
                            key: "metadata.name".to_string(),
                            operator: "In".to_string(),
                            values: Some(vec!["node-x".to_string()]),
                        }]),
                        ..Default::default()
                    }],
                }),
                ..Default::default()
            }),
            ..Default::default()
        });
        assert!(!pod_has_unmodeled_constructs(&mf));

        // Supported hard topology spread: now modeled => NOT bucketed.
        let mut modeled_spread = pod_named("ns", "spread-modeled");
        modeled_spread
            .spec
            .as_mut()
            .unwrap()
            .topology_spread_constraints = Some(vec![corev1::TopologySpreadConstraint {
            max_skew: 1,
            topology_key: "topology.kubernetes.io/zone".to_string(),
            when_unsatisfiable: "DoNotSchedule".to_string(),
            label_selector: Some(
                k8s_openapi::apimachinery::pkg::apis::meta::v1::LabelSelector {
                    match_expressions: Some(vec![
                        k8s_openapi::apimachinery::pkg::apis::meta::v1::LabelSelectorRequirement {
                            key: "app".to_string(),
                            operator: "In".to_string(),
                            values: Some(vec!["trainer".to_string()]),
                        },
                    ]),
                    ..Default::default()
                },
            ),
            ..Default::default()
        }]);
        assert!(!pod_has_unmodeled_constructs(&modeled_spread));

        // Advanced hard topology spread fields are not modeled exactly => bucketed.
        let mut advanced_spread = pod_named("ns", "spread-advanced");
        advanced_spread
            .spec
            .as_mut()
            .unwrap()
            .topology_spread_constraints = Some(vec![corev1::TopologySpreadConstraint {
            max_skew: 1,
            topology_key: "topology.kubernetes.io/zone".to_string(),
            when_unsatisfiable: "DoNotSchedule".to_string(),
            min_domains: Some(2),
            label_selector: Some(
                k8s_openapi::apimachinery::pkg::apis::meta::v1::LabelSelector {
                    match_labels: Some(std::collections::BTreeMap::from([(
                        "app".to_string(),
                        "trainer".to_string(),
                    )])),
                    ..Default::default()
                },
            ),
            ..Default::default()
        }]);
        assert!(pod_has_unmodeled_constructs(&advanced_spread));

        // Unsupported hard topology spread: still expected divergence.
        let mut unsupported_spread = pod_named("ns", "spread-unsupported");
        unsupported_spread
            .spec
            .as_mut()
            .unwrap()
            .topology_spread_constraints = Some(vec![corev1::TopologySpreadConstraint {
            max_skew: 1,
            topology_key: "topology.kubernetes.io/zone".to_string(),
            when_unsatisfiable: "DoNotSchedule".to_string(),
            label_selector: Some(
                k8s_openapi::apimachinery::pkg::apis::meta::v1::LabelSelector {
                    match_expressions: Some(vec![
                        k8s_openapi::apimachinery::pkg::apis::meta::v1::LabelSelectorRequirement {
                            key: "app".to_string(),
                            operator: "Gt".to_string(),
                            values: Some(vec!["trainer".to_string()]),
                        },
                    ]),
                    ..Default::default()
                },
            ),
            ..Default::default()
        }]);
        assert_eq!(
            pod_expected_divergence_reasons(&unsupported_spread),
            vec!["topologySpreadConstraints/DoNotSchedule".to_string()]
        );

        // priorityClassName: unmodeled.
        let mut pr = pod_named("ns", "pr");
        pr.spec.as_mut().unwrap().priority_class_name = Some("high".to_string());
        assert!(pod_has_unmodeled_constructs(&pr));
        assert_eq!(
            pod_expected_divergence_reasons(&pr),
            vec!["priorityClassName".to_string()]
        );
    }

    #[test]
    fn scheduler_feasible_reads_bind_and_filter_result() {
        // Feasible: selected-node annotation == node.
        let mut ann = std::collections::BTreeMap::new();
        ann.insert(
            "kube-scheduler-simulator.sigs.k8s.io/selected-node".to_string(),
            "n1".to_string(),
        );
        let mut bound = pod_named("ns", "p");
        bound.metadata.annotations = Some(ann);
        let export = SimulatorExportPayload { pods: vec![bound] };
        let (feasible, reason) = scheduler_feasible(&export, "n1", "ns/p");
        assert!(feasible);
        assert!(reason.is_none());
        // Wrong node → infeasible.
        let (feasible2, _) = scheduler_feasible(&export, "n2", "ns/p");
        assert!(!feasible2);

        // Infeasible with filter-result reason.
        let mut ann2 = std::collections::BTreeMap::new();
        ann2.insert(
            "kube-scheduler-simulator.sigs.k8s.io/filter-result".to_string(),
            "Insufficient nvidia.com/gpu".to_string(),
        );
        let mut rejected = pod_named("ns", "p");
        rejected.metadata.annotations = Some(ann2);
        let export2 = SimulatorExportPayload {
            pods: vec![rejected],
        };
        let (feasible3, reason3) = scheduler_feasible(&export2, "n1", "ns/p");
        assert!(!feasible3);
        assert_eq!(reason3.as_deref(), Some("Insufficient nvidia.com/gpu"));

        // Pod absent → infeasible with absence reason.
        let (feasible4, reason4) =
            scheduler_feasible(&SimulatorExportPayload::default(), "n1", "ns/p");
        assert!(!feasible4);
        assert!(reason4.unwrap().contains("absent"));
    }

    #[test]
    fn confusion_matrix_records_and_rates() {
        let mut m = ConfusionMatrix::default();
        // empty is vacuously perfect agreement.
        assert_eq!(m.total(), 0);
        assert_eq!(m.agreement_rate(), 1.0);

        m.record(Verdict::Agree);
        m.record(Verdict::Agree);
        m.record(Verdict::Agree);
        m.record(Verdict::FalsePositive);
        assert_eq!(m.total(), 4);
        assert_eq!(m.agree, 3);
        assert_eq!(m.false_positive, 1);
        assert_eq!(m.false_negative, 0);
        assert!((m.agreement_rate() - 0.75).abs() < 1e-9);

        m.record(Verdict::FalseNegative);
        assert_eq!(m.false_negative, 1);
        assert_eq!(m.total(), 5);
    }

    #[test]
    fn render_separates_strict_and_expected_divergence_mismatches() {
        let mut report = ConformanceReport::default();
        assert!(!report.has_strict_false_positives());
        assert_eq!(report.strict_gate_status(), "pass");
        report.strict.record(Verdict::FalsePositive);
        report.expected_divergence.record(Verdict::FalseNegative);
        report
            .expected_divergence_reason_counts
            .insert("requiredPodAntiAffinity".to_string(), 3);
        report
            .expected_divergence_reason_counts
            .insert("topologySpreadConstraints/DoNotSchedule".to_string(), 2);
        report.mismatches.push(Mismatch {
            pod: "ns/strict".to_string(),
            node: "n1".to_string(),
            verdict: Verdict::FalsePositive,
            ours_reasons: Vec::new(),
            scheduler_reason: Some("scheduler rejected strict pod".to_string()),
            expected_divergence_reasons: Vec::new(),
        });
        report.expected_divergence_mismatches.push(Mismatch {
            pod: "ns/expected".to_string(),
            node: "n2".to_string(),
            verdict: Verdict::FalseNegative,
            ours_reasons: vec!["required pod anti-affinity not modeled".to_string()],
            scheduler_reason: None,
            expected_divergence_reasons: vec!["requiredPodAntiAffinity".to_string()],
        });

        let rendered = report.render();

        assert!(rendered.contains("strict: agree=0 false_positive=1 false_negative=0"));
        assert!(rendered.contains("strict-gate: fail"));
        assert!(rendered.contains("expected-divergence: agree=0 false_positive=0 false_negative=1"));
        assert!(rendered.contains(
            "expected-divergence reasons: requiredPodAntiAffinity=3, topologySpreadConstraints/DoNotSchedule=2"
        ));
        assert!(rendered.contains("FALSE-POSITIVE (we feasible, scheduler rejects): ns/strict@n1"));
        assert!(rendered.contains("expected-divergence mismatches"));
        assert!(rendered.contains("false-negative in expected-divergence bucket: ns/expected@n2"));
        assert!(rendered.contains("expected=[requiredPodAntiAffinity]"));
        assert!(report.has_strict_false_positives());
        assert_eq!(report.strict_gate_status(), "fail");
    }

    // ---- node-dedup optimization (opt-in --dedup-nodes) ----

    fn qty(v: &str) -> k8s_openapi::apimachinery::pkg::api::resource::Quantity {
        k8s_openapi::apimachinery::pkg::api::resource::Quantity(v.to_string())
    }

    fn raw_node(
        name: &str,
        alloc: &[(&str, &str)],
        labels: &[(&str, &str)],
        taints: &[(&str, &str, &str)],
    ) -> corev1::Node {
        corev1::Node {
            metadata: kube::api::ObjectMeta {
                name: Some(name.to_string()),
                labels: Some(
                    labels
                        .iter()
                        .map(|(k, v)| (k.to_string(), v.to_string()))
                        .collect(),
                ),
                ..Default::default()
            },
            spec: Some(corev1::NodeSpec {
                taints: Some(
                    taints
                        .iter()
                        .map(|(k, v, e)| corev1::Taint {
                            key: k.to_string(),
                            value: Some(v.to_string()),
                            effect: e.to_string(),
                            ..Default::default()
                        })
                        .collect(),
                ),
                ..Default::default()
            }),
            status: Some(corev1::NodeStatus {
                allocatable: Some(alloc.iter().map(|(k, v)| (k.to_string(), qty(v))).collect()),
                ..Default::default()
            }),
            ..Default::default()
        }
    }

    fn pod_with_node_selector(pairs: &[(&str, &str)]) -> corev1::Pod {
        corev1::Pod {
            spec: Some(corev1::PodSpec {
                node_selector: Some(
                    pairs
                        .iter()
                        .map(|(k, v)| (k.to_string(), v.to_string()))
                        .collect(),
                ),
                ..Default::default()
            }),
            ..Default::default()
        }
    }

    #[test]
    fn equivalence_key_merges_identical_and_ignores_unreferenced_labels() {
        // Pod selects only on `sku`. Two nodes identical on alloc/taints/sku but differing on an
        // UNREFERENCED label (hostname) must share a key (the whole point — enables dedup).
        let pod = pod_with_node_selector(&[("sku", "a100")]);
        let a = raw_node(
            "a",
            &[("cpu", "8"), ("nvidia.com/gpu", "8")],
            &[("sku", "a100"), ("kubernetes.io/hostname", "a")],
            &[("nvidia.com/gpu", "present", "NoSchedule")],
        );
        let b = raw_node(
            "b",
            &[("cpu", "8"), ("nvidia.com/gpu", "8")],
            &[("sku", "a100"), ("kubernetes.io/hostname", "b")],
            &[("nvidia.com/gpu", "present", "NoSchedule")],
        );
        let ka = pod_filter_equivalence_key(&pod, &a);
        assert!(ka.is_some());
        assert_eq!(
            ka,
            pod_filter_equivalence_key(&pod, &b),
            "differ only on unreferenced label"
        );

        // Differing REFERENCED label ⇒ different key (must NOT merge).
        let c = raw_node(
            "c",
            &[("cpu", "8"), ("nvidia.com/gpu", "8")],
            &[("sku", "l4"), ("kubernetes.io/hostname", "c")],
            &[("nvidia.com/gpu", "present", "NoSchedule")],
        );
        assert_ne!(
            ka,
            pod_filter_equivalence_key(&pod, &c),
            "differ on referenced label sku"
        );

        // Differing allocatable ⇒ different key. Differing taint ⇒ different key.
        let d = raw_node(
            "d",
            &[("cpu", "4"), ("nvidia.com/gpu", "8")],
            &[("sku", "a100")],
            &[],
        );
        assert_ne!(ka, pod_filter_equivalence_key(&pod, &d));
    }

    #[test]
    fn equivalence_key_extracts_node_affinity_matchexpression_labels() {
        // Safety-critical: the key must also fold in labels referenced by required node-affinity
        // matchExpressions (not just nodeSelector). If it didn't, two nodes differing on an
        // affinity-referenced label would false-merge and share a probe ⇒ a WRONG verdict.
        let pod = corev1::Pod {
            spec: Some(corev1::PodSpec {
                affinity: Some(corev1::Affinity {
                    node_affinity: Some(corev1::NodeAffinity {
                        required_during_scheduling_ignored_during_execution: Some(
                            corev1::NodeSelector {
                                node_selector_terms: vec![corev1::NodeSelectorTerm {
                                    match_expressions: Some(vec![
                                        corev1::NodeSelectorRequirement {
                                            key: "zone".to_string(),
                                            operator: "In".to_string(),
                                            values: Some(vec!["z1".to_string()]),
                                        },
                                    ]),
                                    ..Default::default()
                                }],
                            },
                        ),
                        ..Default::default()
                    }),
                    ..Default::default()
                }),
                ..Default::default()
            }),
            ..Default::default()
        };
        let a = raw_node("a", &[("cpu", "8")], &[("zone", "z1")], &[]);
        let b = raw_node("b", &[("cpu", "8")], &[("zone", "z2")], &[]);
        let same = raw_node("s", &[("cpu", "8")], &[("zone", "z1")], &[]);
        let ka = pod_filter_equivalence_key(&pod, &a);
        assert!(ka.is_some());
        // Nodes differing on the affinity-referenced label MUST NOT merge.
        assert_ne!(
            ka,
            pod_filter_equivalence_key(&pod, &b),
            "affinity-referenced label 'zone' differs ⇒ keys must differ"
        );
        // Nodes agreeing on it (and everything else keyed) DO merge.
        assert_eq!(ka, pod_filter_equivalence_key(&pod, &same));
    }

    #[test]
    fn equivalence_key_none_on_pvc_and_matchfields() {
        // PVC volume ⇒ VolumeBinding topology not keyed ⇒ None (probe individually).
        let mut pvc_pod = pod_with_node_selector(&[]);
        pvc_pod.spec.as_mut().unwrap().volumes = Some(vec![corev1::Volume {
            name: "data".to_string(),
            persistent_volume_claim: Some(corev1::PersistentVolumeClaimVolumeSource {
                claim_name: "c".to_string(),
                ..Default::default()
            }),
            ..Default::default()
        }]);
        let n = raw_node("n", &[("cpu", "8")], &[], &[]);
        assert_eq!(pod_filter_equivalence_key(&pvc_pod, &n), None);

        // matchFields (node-name dependent) ⇒ None.
        let mf_pod = corev1::Pod {
            spec: Some(corev1::PodSpec {
                affinity: Some(corev1::Affinity {
                    node_affinity: Some(corev1::NodeAffinity {
                        required_during_scheduling_ignored_during_execution: Some(
                            corev1::NodeSelector {
                                node_selector_terms: vec![corev1::NodeSelectorTerm {
                                    match_fields: Some(vec![corev1::NodeSelectorRequirement {
                                        key: "metadata.name".to_string(),
                                        operator: "In".to_string(),
                                        values: Some(vec!["n".to_string()]),
                                    }]),
                                    ..Default::default()
                                }],
                            },
                        ),
                        ..Default::default()
                    }),
                    ..Default::default()
                }),
                ..Default::default()
            }),
            ..Default::default()
        };
        assert_eq!(pod_filter_equivalence_key(&mf_pod, &n), None);
    }

    #[test]
    fn group_nodes_for_probe_dedups_only_when_enabled() {
        let pod = pod_with_node_selector(&[("sku", "a100")]);
        let raw: std::collections::BTreeMap<String, &corev1::Node> =
            std::collections::BTreeMap::new();
        let a = raw_node("a", &[("cpu", "8")], &[("sku", "a100")], &[]);
        let b = raw_node("b", &[("cpu", "8")], &[("sku", "a100")], &[]); // identical to a for this pod
        let c = raw_node("c", &[("cpu", "4")], &[("sku", "a100")], &[]); // different alloc
        let raw = {
            let mut m = raw;
            m.insert("a".into(), &a);
            m.insert("b".into(), &b);
            m.insert("c".into(), &c);
            m
        };
        let na = crate::model::NormalizedNode {
            name: "a".into(),
            ..Default::default()
        };
        let nb = crate::model::NormalizedNode {
            name: "b".into(),
            ..Default::default()
        };
        let nc = crate::model::NormalizedNode {
            name: "c".into(),
            ..Default::default()
        };
        let nodes = vec![&na, &nb, &nc];

        // dedup off ⇒ every node its own group (exact prior behavior).
        let off = group_nodes_for_probe(&pod, &nodes, &raw, false);
        assert_eq!(off.len(), 3);
        assert!(off.iter().all(|g| g.len() == 1));

        // dedup on ⇒ a and b merge (identical for this pod); c separate.
        let on = group_nodes_for_probe(&pod, &nodes, &raw, true);
        assert_eq!(on.len(), 2, "a+b merge, c separate");
        let sizes: std::collections::BTreeSet<usize> = on.iter().map(|g| g.len()).collect();
        assert_eq!(sizes, std::collections::BTreeSet::from([1, 2]));
        // every node still appears exactly once across groups (no dropped/duplicated coverage).
        let mut names: Vec<&str> = on.iter().flatten().map(|n| n.name.as_str()).collect();
        names.sort();
        assert_eq!(names, vec!["a", "b", "c"]);
    }
}
