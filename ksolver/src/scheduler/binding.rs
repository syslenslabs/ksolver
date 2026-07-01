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
/// node, a pod gone from the snapshot, a pod recreated under the same namespace/name (uid changed),
/// and a pod already bound. Live GPU-capacity/taint/affinity rechecks are deferred (the decision
/// solve ensured fit; races are a binder-side optimistic-retry concern) — do not treat `Ready` as a
/// guarantee that a bind will succeed, only that it is not obviously stale/conflicting.
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
        // Same name, different identity: the pod was deleted and recreated — this plan targets the
        // old pod. (Empty uid on either side ⇒ skip the check rather than false-positive.)
        Some(w) if !entry.pod_uid.is_empty() && !w.uid.is_empty() && w.uid != entry.pod_uid => {
            BindReadiness::Stale {
                reason: "pod recreated (uid changed) since the plan was rendered".to_string(),
            }
        }
        Some(w) if !w.current_node.is_empty() => BindReadiness::Stale {
            reason: format!("pod already scheduled on {}", w.current_node),
        },
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
    use crate::scheduler::trace::{DecisionTrace, PodDecision, PodPlacement};

    fn trace_with(decisions: Vec<PodDecision>) -> DecisionTrace {
        DecisionTrace {
            sequence: 1,
            observed_pods: decisions.len(),
            decisions,
            solver_status: "OPTIMAL".into(),
            solve_millis: 1,
            solve_core_millis: 1,
            snapshot_age_millis: 0,
            note: String::new(),
        }
    }

    fn placed(ns: &str, name: &str, uid: &str, node: &str) -> PodDecision {
        PodDecision {
            uid: uid.into(),
            namespace: ns.into(),
            name: name.into(),
            gpu_request: 1,
            placement: PodPlacement::Placed { node: node.into() },
            caveats: vec![],
        }
    }

    #[test]
    fn renders_binding_for_each_placed_pod_only() {
        let t = trace_with(vec![
            placed("team", "a", "uid-a", "node-1"),
            PodDecision {
                uid: "uid-b".into(),
                namespace: "team".into(),
                name: "b".into(),
                gpu_request: 1,
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
            gpu_request: 1,
            placement: PodPlacement::Unplaced { reason: "x".into() },
            caveats: vec![],
        }]);
        assert!(render_binding_plan(&t).is_empty());
    }

    fn entry(node: &str, uid: &str) -> BindingPlanEntry {
        BindingPlanEntry {
            namespace: "team".into(),
            pod_name: "a".into(),
            pod_uid: uid.into(),
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
    fn readiness_stale_when_pod_already_scheduled() {
        let c = cluster(&["n1"], vec![workload("n2", "uid-a")]);
        match assess_binding_readiness(&entry("n1", "uid-a"), &c) {
            BindReadiness::Stale { reason } => assert!(reason.contains("already")),
            _ => panic!("expected stale"),
        }
    }
}
