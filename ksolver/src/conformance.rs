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
/// DoNotSchedule topology spread, or non-empty priority/priorityClassName. Required node
/// affinity (both matchExpressions OR-of-terms and matchFields metadata.name) is now modeled,
/// so it is NOT bucketed here.
pub(crate) fn pod_has_unmodeled_constructs(pod: &corev1::Pod) -> bool {
    let Some(spec) = pod.spec.as_ref() else {
        return false;
    };
    if spec.priority.unwrap_or(0) != 0
        || spec
            .priority_class_name
            .as_ref()
            .map(|p| !p.is_empty())
            .unwrap_or(false)
    {
        return true;
    }
    if let Some(tsc) = spec.topology_spread_constraints.as_ref() {
        if tsc.iter().any(|c| c.when_unsatisfiable == "DoNotSchedule") {
            return true;
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
            || aff
                .pod_anti_affinity
                .as_ref()
                .map(|a| required_terms(&a.required_during_scheduling_ignored_during_execution))
                .unwrap_or(false)
        {
            return true;
        }
    }
    false
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
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
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
#[derive(Debug, Default, Clone, Copy)]
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
#[derive(Debug, Clone)]
pub struct Mismatch {
    pub pod: String,
    pub node: String,
    pub verdict: Verdict,
    pub ours_reasons: Vec<String>,
    pub scheduler_reason: Option<String>,
}

/// Full conformance result: strict (plain pods, must match) vs expected-divergence buckets.
#[derive(Debug, Default, Clone)]
pub struct ConformanceReport {
    pub strict: ConfusionMatrix,
    pub expected_divergence: ConfusionMatrix,
    pub pods_evaluated: usize,
    pub cordoned_nodes_skipped: usize,
    pub mismatches: Vec<Mismatch>,
}

impl ConformanceReport {
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
        out.push_str(&format!(
            "  strict: agree={} false_positive={} false_negative={}\n",
            self.strict.agree, self.strict.false_positive, self.strict.false_negative
        ));
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
        ..Default::default()
    };

    for pod in &pending {
        let scope = format!("{}/{}", pod.namespace, pod.name);
        let Some(raw_pod) = raw_pods.get(&scope) else {
            continue;
        };
        let expected_divergence = pod_has_unmodeled_constructs(raw_pod);
        for node in &candidate_nodes {
            let Some(raw_node) = raw_nodes.get(&node.name) else {
                continue;
            };
            let ours_reasons =
                node_feasibility_reasons(pod, &conformance_node(node), &volumes, &opts);
            let ours_feasible = ours_reasons.is_empty();

            let payload = build_single_node_payload(&raw, raw_pod, raw_node);
            let export =
                crate::verifier::schedule_snapshot(simulator_url, &payload, &scope).await?;
            let (sched_feasible, sched_reason) = scheduler_feasible(&export, &node.name, &scope);

            let verdict = classify(ours_feasible, sched_feasible);
            if expected_divergence {
                report.expected_divergence.record(verdict);
            } else {
                report.strict.record(verdict);
                if verdict != Verdict::Agree {
                    report.mismatches.push(Mismatch {
                        pod: scope.clone(),
                        node: node.name.clone(),
                        verdict,
                        ours_reasons: ours_reasons.clone(),
                        scheduler_reason: sched_reason,
                    });
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
        assert!(pod_has_unmodeled_constructs(&aa));

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

        // priorityClassName: unmodeled.
        let mut pr = pod_named("ns", "pr");
        pr.spec.as_mut().unwrap().priority_class_name = Some("high".to_string());
        assert!(pod_has_unmodeled_constructs(&pr));
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
}
