//! Dry-run binding plan: renders the exact Kubernetes `Binding` subresource payloads that a real
//! binder WOULD POST for each placed pod. This module is PURE — it builds data only and never
//! contacts the API server (enforced by `no_mutation_guard`). Shadow mode stays read-only.

use crate::scheduler::trace::{DecisionTrace, PodPlacement};
use serde::{Deserialize, Serialize};

/// One rendered (but never sent) pod→node binding.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct BindingPlanEntry {
    pub namespace: String,
    pub pod_name: String,
    pub pod_uid: String,
    /// Solver workload id shared by all members of a gang. Empty for old/singleton traces.
    #[serde(default)]
    pub binding_group: String,
    /// Optional tenant/team owner hint copied from the decision trace for audit surfaces.
    #[serde(default)]
    pub team: String,
    pub node_name: String,
    /// GPU count the pod requests (a binder needs it to re-check capacity before applying).
    #[serde(default)]
    pub gpu_request: i64,
    /// The canonical `pods/binding` subresource POST body a real binder would send (dry-run only).
    pub binding_body: serde_json::Value,
}

/// Whether a rendered (dry-run) binding is still safe to apply against the current cluster snapshot.
/// A stale/conflict guard — NOT a full scheduler-predicate revalidation.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
#[serde(tag = "state", rename_all = "lowercase")]
pub enum BindReadiness {
    Ready,
    Stale { reason: String },
}

/// Re-validate a rendered binding against the LATEST cluster snapshot — the stale/conflict guard a
/// real binder must run before POSTing. Pure: reads only, mutates nothing. Covers a vanished target
/// node, a pod gone from the snapshot, missing pod identity, a pod recreated under the same
/// namespace/name (uid changed), a pod already bound, and a target node that is no longer in the
/// pod's latest feasible-node set. This is still not a fresh kube-scheduler Filter call, so do not
/// treat `Ready` as a guarantee that a bind will succeed; it means the plan is not obviously
/// stale/conflicting in ksolver's latest normalized snapshot.
pub fn assess_binding_readiness(
    entry: &BindingPlanEntry,
    cluster: &crate::model::NormalizedCluster,
) -> BindReadiness {
    if !cluster.nodes.iter().any(|n| n.name == entry.node_name) {
        return BindReadiness::Stale {
            reason: format!("target node {} no longer present", entry.node_name),
        };
    }
    match cluster
        .workloads
        .iter()
        .find(|w| w.namespace == entry.namespace && w.name == entry.pod_name)
    {
        None => BindReadiness::Stale {
            reason: "pod no longer present in latest snapshot".to_string(),
        },
        Some(w) if entry.pod_uid.is_empty() || w.uid.is_empty() => BindReadiness::Stale {
            reason: "missing pod uid in binding plan or latest snapshot".to_string(),
        },
        // Same name, different identity: the pod was deleted and recreated — this plan targets the
        // old pod.
        Some(w) if !entry.pod_uid.is_empty() && !w.uid.is_empty() && w.uid != entry.pod_uid => {
            BindReadiness::Stale {
                reason: "pod recreated (uid changed) since the plan was rendered".to_string(),
            }
        }
        Some(w) if !w.current_node.is_empty() => BindReadiness::Stale {
            reason: format!("pod already scheduled on {}", w.current_node),
        },
        Some(w)
            if !w.feasible_node_names.is_empty()
                && !w.feasible_node_names.iter().any(|n| n == &entry.node_name) =>
        {
            BindReadiness::Stale {
                reason: format!(
                    "target node {} no longer feasible for pod in latest snapshot",
                    entry.node_name
                ),
            }
        }
        Some(_) => BindReadiness::Ready,
    }
}

/// Render the pod→node bindings implied by a decision trace. Only `Placed` decisions produce an
/// entry; unplaced pods are skipped. No side effects, no API calls.
pub fn render_binding_plan(trace: &DecisionTrace) -> Vec<BindingPlanEntry> {
    trace
        .decisions
        .iter()
        .filter_map(|d| match &d.placement {
            PodPlacement::Placed { node } => Some(BindingPlanEntry {
                namespace: d.namespace.clone(),
                pod_name: d.name.clone(),
                pod_uid: d.uid.clone(),
                binding_group: d.binding_group.clone(),
                team: d.team.clone(),
                node_name: node.clone(),
                gpu_request: d.gpu_request,
                binding_body: serde_json::json!({
                    "apiVersion": "v1",
                    "kind": "Binding",
                    "metadata": { "name": d.name, "namespace": d.namespace },
                    "target": { "apiVersion": "v1", "kind": "Node", "name": node },
                }),
            }),
            PodPlacement::Unplaced { .. } => None,
        })
        .collect()
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::scheduler::trace::{
        DeadlineMetrics, DecisionTrace, PodDecision, PodPlacement, QuotaMetrics,
    };

    fn trace_with(decisions: Vec<PodDecision>) -> DecisionTrace {
        DecisionTrace {
            sequence: 1,
            observed_pods: decisions.len(),
            decisions,
            solver_status: "OPTIMAL".into(),
            objective_profile: Default::default(),
            objective_weights: Default::default(),
            solve_millis: 1,
            solve_core_millis: 1,
            snapshot_age_millis: 0,
            note: String::new(),
            repair_plans: Vec::new(),
            repair_notes: Vec::new(),
            repair_metrics: Default::default(),
            deadline_metrics: DeadlineMetrics::default(),
            quota_metrics: QuotaMetrics::default(),
            admission_metrics: Default::default(),
            queue_wait_metrics: Default::default(),
            tenant_fairness_metrics: Default::default(),
            gpu_utilization_metrics: Default::default(),
            outcome_summary: Default::default(),
            job_observation_metrics: Default::default(),
            prediction_audit_metrics: Default::default(),
            prediction_audit_details: Vec::new(),
            node_grouping_metrics: Default::default(),
            candidate_quality_metrics: Default::default(),
            binding_reservation_metrics: Default::default(),
            binding_outcome_metrics: Default::default(),
            candidate_node_limit: 0,
            retry_count: 0,
            unpruned_candidate_edges: 0,
            initial_candidate_edges: 0,
            final_candidate_edges: 0,
            candidate_pruned_workloads: 0,
            widening_reason: String::new(),
        }
    }

    fn placed(ns: &str, name: &str, uid: &str, node: &str) -> PodDecision {
        PodDecision {
            uid: uid.into(),
            namespace: ns.into(),
            name: name.into(),
            binding_group: String::new(),
            gpu_request: 1,
            priority: 0,
            priority_class_name: String::new(),
            team: String::new(),
            queue: String::new(),
            queue_score: 0,
            business_value: 0,
            queue_wait_seconds: 0,
            deadline_unix_seconds: 0,
            min_gpus: 0,
            max_gpus: 0,
            preferred_gpus: 0,
            flexible: false,
            predicted_runtime_seconds: 0,
            predicted_peak_vram_bytes: 0,
            deadline_slack_seconds: 0,
            predicted_finish_unix_seconds: 0,
            predicted_deadline_miss: false,
            placement: PodPlacement::Placed { node: node.into() },
            caveats: vec![],
        }
    }

    fn placed_in_group(ns: &str, name: &str, uid: &str, node: &str, group: &str) -> PodDecision {
        PodDecision {
            binding_group: group.into(),
            ..placed(ns, name, uid, node)
        }
    }

    #[test]
    fn renders_binding_for_each_placed_pod_only() {
        let mut placed = placed("team", "a", "uid-a", "node-1");
        placed.team = "research".to_string();
        let t = trace_with(vec![
            placed,
            PodDecision {
                uid: "uid-b".into(),
                namespace: "team".into(),
                name: "b".into(),
                binding_group: String::new(),
                gpu_request: 1,
                priority: 0,
                priority_class_name: String::new(),
                team: String::new(),
                queue: String::new(),
                queue_score: 0,
                business_value: 0,
                queue_wait_seconds: 0,
                deadline_unix_seconds: 0,
                min_gpus: 0,
                max_gpus: 0,
                preferred_gpus: 0,
                flexible: false,
                predicted_runtime_seconds: 0,
                predicted_peak_vram_bytes: 0,
                deadline_slack_seconds: 0,
                predicted_finish_unix_seconds: 0,
                predicted_deadline_miss: false,
                placement: PodPlacement::Unplaced {
                    reason: "no feasible node".into(),
                },
                caveats: vec![],
            },
        ]);
        let plan = render_binding_plan(&t);
        assert_eq!(plan.len(), 1, "only placed pods yield bindings");
        let e = &plan[0];
        assert_eq!(e.namespace, "team");
        assert_eq!(e.pod_name, "a");
        assert_eq!(e.pod_uid, "uid-a");
        assert_eq!(e.team, "research");
        assert_eq!(e.node_name, "node-1");
        assert_eq!(e.gpu_request, 1);
        assert_eq!(e.binding_body["kind"], "Binding");
        assert_eq!(e.binding_body["apiVersion"], "v1");
        assert_eq!(e.binding_body["metadata"]["name"], "a");
        assert_eq!(e.binding_body["metadata"]["namespace"], "team");
        assert_eq!(e.binding_body["target"]["kind"], "Node");
        assert_eq!(e.binding_body["target"]["name"], "node-1");
    }

    #[test]
    fn empty_when_nothing_placed() {
        let t = trace_with(vec![PodDecision {
            uid: "uid-b".into(),
            namespace: "team".into(),
            name: "b".into(),
            binding_group: String::new(),
            gpu_request: 1,
            priority: 0,
            priority_class_name: String::new(),
            team: String::new(),
            queue: String::new(),
            queue_score: 0,
            business_value: 0,
            queue_wait_seconds: 0,
            deadline_unix_seconds: 0,
            min_gpus: 0,
            max_gpus: 0,
            preferred_gpus: 0,
            flexible: false,
            predicted_runtime_seconds: 0,
            predicted_peak_vram_bytes: 0,
            deadline_slack_seconds: 0,
            predicted_finish_unix_seconds: 0,
            predicted_deadline_miss: false,
            placement: PodPlacement::Unplaced { reason: "x".into() },
            caveats: vec![],
        }]);
        assert!(render_binding_plan(&t).is_empty());
    }

    #[test]
    fn renders_binding_group_for_gang_members() {
        let t = trace_with(vec![
            placed_in_group("team", "m0", "uid-0", "node-1", "gang:team/train"),
            placed_in_group("team", "m1", "uid-1", "node-2", "gang:team/train"),
        ]);
        let plan = render_binding_plan(&t);
        assert_eq!(plan.len(), 2);
        assert!(plan
            .iter()
            .all(|entry| entry.binding_group == "gang:team/train"));
    }

    fn entry(node: &str, uid: &str) -> BindingPlanEntry {
        BindingPlanEntry {
            namespace: "team".into(),
            pod_name: "a".into(),
            pod_uid: uid.into(),
            binding_group: String::new(),
            team: String::new(),
            node_name: node.into(),
            gpu_request: 1,
            binding_body: serde_json::json!({}),
        }
    }

    fn workload(current_node: &str, uid: &str) -> crate::model::NormalizedWorkload {
        crate::model::NormalizedWorkload {
            namespace: "team".into(),
            name: "a".into(),
            uid: uid.into(),
            current_node: current_node.into(),
            feasible_node_names: vec!["n1".into()],
            ..Default::default()
        }
    }

    fn cluster(
        nodes: &[&str],
        workloads: Vec<crate::model::NormalizedWorkload>,
    ) -> crate::model::NormalizedCluster {
        crate::model::NormalizedCluster {
            nodes: nodes
                .iter()
                .map(|n| crate::model::NormalizedNode {
                    name: (*n).into(),
                    ..Default::default()
                })
                .collect(),
            workloads,
            ..Default::default()
        }
    }

    #[test]
    fn readiness_ready_when_node_present_and_pod_unbound() {
        let c = cluster(&["n1"], vec![workload("", "uid-a")]);
        assert!(matches!(
            assess_binding_readiness(&entry("n1", "uid-a"), &c),
            BindReadiness::Ready
        ));
    }

    #[test]
    fn readiness_stale_when_target_node_gone() {
        let c = cluster(&[], vec![workload("", "uid-a")]);
        match assess_binding_readiness(&entry("n1", "uid-a"), &c) {
            BindReadiness::Stale { reason } => assert!(reason.contains("node")),
            _ => panic!("expected stale"),
        }
    }

    #[test]
    fn readiness_stale_when_pod_absent() {
        let c = cluster(&["n1"], vec![]);
        match assess_binding_readiness(&entry("n1", "uid-a"), &c) {
            BindReadiness::Stale { reason } => assert!(reason.contains("no longer present")),
            _ => panic!("expected stale"),
        }
    }

    #[test]
    fn readiness_stale_when_pod_recreated_uid_changed() {
        let c = cluster(&["n1"], vec![workload("", "uid-NEW")]);
        match assess_binding_readiness(&entry("n1", "uid-OLD"), &c) {
            BindReadiness::Stale { reason } => assert!(reason.contains("uid")),
            _ => panic!("expected stale"),
        }
    }

    #[test]
    fn readiness_stale_when_binding_entry_uid_missing() {
        let c = cluster(&["n1"], vec![workload("", "uid-a")]);
        match assess_binding_readiness(&entry("n1", ""), &c) {
            BindReadiness::Stale { reason } => assert!(reason.contains("missing pod uid")),
            _ => panic!("expected stale"),
        }
    }

    #[test]
    fn readiness_stale_when_latest_snapshot_uid_missing() {
        let c = cluster(&["n1"], vec![workload("", "")]);
        match assess_binding_readiness(&entry("n1", "uid-a"), &c) {
            BindReadiness::Stale { reason } => assert!(reason.contains("missing pod uid")),
            _ => panic!("expected stale"),
        }
    }

    #[test]
    fn readiness_stale_when_pod_already_scheduled() {
        let c = cluster(&["n1"], vec![workload("n2", "uid-a")]);
        match assess_binding_readiness(&entry("n1", "uid-a"), &c) {
            BindReadiness::Stale { reason } => assert!(reason.contains("already")),
            _ => panic!("expected stale"),
        }
    }

    #[test]
    fn readiness_stale_when_target_no_longer_feasible_for_pod() {
        let mut w = workload("", "uid-a");
        w.feasible_node_names = vec!["n2".into()];
        let c = cluster(&["n1", "n2"], vec![w]);
        match assess_binding_readiness(&entry("n1", "uid-a"), &c) {
            BindReadiness::Stale { reason } => assert!(reason.contains("no longer feasible")),
            _ => panic!("expected stale"),
        }
    }

    #[test]
    fn readiness_ready_when_feasible_nodes_unknown_keeps_backward_compatibility() {
        let mut w = workload("", "uid-a");
        w.feasible_node_names.clear();
        let c = cluster(&["n1"], vec![w]);
        assert!(matches!(
            assess_binding_readiness(&entry("n1", "uid-a"), &c),
            BindReadiness::Ready
        ));
    }
}
