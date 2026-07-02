//! PHASE 3 — real binding executor. This is the ONLY module that mutates cluster state, and only
//! when `ShadowConfig.enable_real_binding` is true (default false). It POSTs pod→node `Binding`
//! subresources for decisions whose dry-run readiness is `Ready`, after a final live re-check.
//! Everything here is gated, throttled, and logged; a per-pod error never aborts the pass.

use crate::scheduler::binding::{BindReadiness, BindingPlanEntry};
use crate::scheduler::config::ShadowConfig;

/// Minimal live view of a pod at apply time (fetched immediately before binding).
#[derive(Debug, Clone)]
pub struct LivePodView {
    pub uid: String,
    pub phase: String,
    pub node_name: String,
    pub deleting: bool,
    pub scheduler_name: String,
    /// True if the pod requests devices via `spec.resourceClaims` (DRA). ksolver models DRA only as
    /// a scalar shadow approximation and does NOT allocate ResourceClaims, so it must NOT real-bind
    /// such pods — a bound-but-unallocated DRA pod would hang (kubelet waits for claim allocation).
    pub uses_dra: bool,
}

/// Whether to apply a rendered binding, decided against the freshest live pod state.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum ApplyDecision {
    Apply,
    Skip { reason: String },
}

/// Pure final gate before a POST. Requires: readiness `Ready`; pod present; strict uid match (both
/// non-empty); not terminating; owned by our scheduler; unbound; Pending. Anything else ⇒ Skip.
pub fn should_apply(
    entry: &BindingPlanEntry,
    readiness: &BindReadiness,
    live: Option<&LivePodView>,
    expected_scheduler: &str,
) -> ApplyDecision {
    if !matches!(readiness, BindReadiness::Ready) {
        return ApplyDecision::Skip {
            reason: "not ready (stale plan)".into(),
        };
    }
    let Some(live) = live else {
        return ApplyDecision::Skip {
            reason: "pod not found at apply time".into(),
        };
    };
    // Strict identity: never bind blind. Missing uid on either side is unsafe for real mutation.
    if entry.pod_uid.is_empty() || live.uid.is_empty() {
        return ApplyDecision::Skip {
            reason: "missing pod uid at apply time".into(),
        };
    }
    if live.uid != entry.pod_uid {
        return ApplyDecision::Skip {
            reason: "pod uid changed at apply time".into(),
        };
    }
    if live.deleting {
        return ApplyDecision::Skip {
            reason: "pod is terminating".into(),
        };
    }
    if live.uses_dra {
        // ksolver does not allocate ResourceClaims; binding a DRA pod would leave it stuck waiting
        // for device allocation. Refuse (its shadow placement is advisory only).
        return ApplyDecision::Skip {
            reason: "DRA pod: ksolver does not allocate ResourceClaims (real binding unsafe)"
                .into(),
        };
    }
    if !expected_scheduler.is_empty() && live.scheduler_name != expected_scheduler {
        return ApplyDecision::Skip {
            reason: format!(
                "pod scheduler is {}, not {}",
                live.scheduler_name, expected_scheduler
            ),
        };
    }
    if !live.node_name.is_empty() {
        return ApplyDecision::Skip {
            reason: format!("pod already bound to {}", live.node_name),
        };
    }
    if live.phase != "Pending" {
        return ApplyDecision::Skip {
            reason: format!("pod not Pending (phase {})", live.phase),
        };
    }
    ApplyDecision::Apply
}

/// Outcome of one binding attempt.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum BindResult {
    Bound { dry_run: bool },
    Skipped { reason: String },
    Failed { error: String },
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct BindOutcome {
    pub namespace: String,
    pub pod: String,
    pub node: String,
    pub result: BindResult,
}

impl BindOutcome {
    fn skip(entry: &BindingPlanEntry, reason: String) -> Self {
        BindOutcome {
            namespace: entry.namespace.clone(),
            pod: entry.pod_name.clone(),
            node: entry.node_name.clone(),
            result: BindResult::Skipped { reason },
        }
    }
}

/// Apply the ready bindings in `plan` (effectful; the ONLY mutation path). Returns one outcome per
/// entry. No-op (all skipped) when `enable_real_binding` is false. Throttled by `max_binds_per_pass`.
/// With `real_binding_dry_run`, the POST carries server-side `dryRun=All` (validated, not persisted).
pub async fn apply_bindings(
    client: &kube::Client,
    plan: &[(BindingPlanEntry, BindReadiness)],
    cfg: &ShadowConfig,
) -> Vec<BindOutcome> {
    use k8s_openapi::api::core::v1 as corev1;
    use kube::api::{Api, PostParams};

    // Defense-in-depth: never mutate unless explicitly enabled, regardless of caller.
    if !cfg.enable_real_binding {
        return plan
            .iter()
            .map(|(e, _)| BindOutcome::skip(e, "real binding disabled".into()))
            .collect();
    }

    let pp = PostParams {
        dry_run: cfg.real_binding_dry_run,
        ..Default::default()
    };
    let mut outcomes = Vec::with_capacity(plan.len());
    let mut applied = 0usize;
    for (entry, readiness) in plan {
        if applied >= cfg.max_binds_per_pass {
            outcomes.push(BindOutcome::skip(
                entry,
                "max binds per pass reached".into(),
            ));
            continue;
        }
        let pods: Api<corev1::Pod> = Api::namespaced(client.clone(), &entry.namespace);
        // Final live re-check (optimistic concurrency): fetch the pod right before binding.
        let live = match pods.get_opt(&entry.pod_name).await {
            Ok(Some(p)) => Some(LivePodView {
                uid: p.metadata.uid.clone().unwrap_or_default(),
                phase: p
                    .status
                    .as_ref()
                    .and_then(|s| s.phase.clone())
                    .unwrap_or_default(),
                node_name: p
                    .spec
                    .as_ref()
                    .and_then(|s| s.node_name.clone())
                    .unwrap_or_default(),
                deleting: p.metadata.deletion_timestamp.is_some(),
                scheduler_name: p
                    .spec
                    .as_ref()
                    .and_then(|s| s.scheduler_name.clone())
                    .unwrap_or_default(),
                uses_dra: p
                    .spec
                    .as_ref()
                    .and_then(|s| s.resource_claims.as_ref())
                    .map(|c| !c.is_empty())
                    .unwrap_or(false),
            }),
            Ok(None) => None,
            Err(e) => {
                outcomes.push(BindOutcome {
                    namespace: entry.namespace.clone(),
                    pod: entry.pod_name.clone(),
                    node: entry.node_name.clone(),
                    result: BindResult::Failed {
                        error: format!("get: {e}"),
                    },
                });
                continue;
            }
        };
        match should_apply(entry, readiness, live.as_ref(), &cfg.scheduler_name) {
            ApplyDecision::Skip { reason } => outcomes.push(BindOutcome::skip(entry, reason)),
            ApplyDecision::Apply => {
                let binding = corev1::Binding {
                    metadata: k8s_openapi::apimachinery::pkg::apis::meta::v1::ObjectMeta {
                        name: Some(entry.pod_name.clone()),
                        namespace: Some(entry.namespace.clone()),
                        ..Default::default()
                    },
                    target: corev1::ObjectReference {
                        api_version: Some("v1".into()),
                        kind: Some("Node".into()),
                        name: Some(entry.node_name.clone()),
                        ..Default::default()
                    },
                };
                let data = match serde_json::to_vec(&binding) {
                    Ok(d) => d,
                    Err(e) => {
                        outcomes.push(BindOutcome {
                            namespace: entry.namespace.clone(),
                            pod: entry.pod_name.clone(),
                            node: entry.node_name.clone(),
                            result: BindResult::Failed {
                                error: format!("encode: {e}"),
                            },
                        });
                        continue;
                    }
                };
                let res = pods
                    .create_subresource::<serde_json::Value>("binding", &entry.pod_name, &pp, data)
                    .await;
                let result = match res {
                    Ok(_) => {
                        applied += 1;
                        tracing::info!(
                            ns = %entry.namespace, pod = %entry.pod_name, node = %entry.node_name,
                            dry_run = cfg.real_binding_dry_run, "REAL BIND applied"
                        );
                        BindResult::Bound {
                            dry_run: cfg.real_binding_dry_run,
                        }
                    }
                    Err(e) => {
                        // A 2xx whose body failed to deserialize would land here even though the
                        // bind SUCCEEDED. Reconcile against live state (skip for dry-run, which
                        // persists nothing): if the pod is now bound to the target, count it Bound.
                        if !cfg.real_binding_dry_run {
                            if let Ok(Some(p)) = pods.get_opt(&entry.pod_name).await {
                                let bound_to = p
                                    .spec
                                    .as_ref()
                                    .and_then(|s| s.node_name.clone())
                                    .unwrap_or_default();
                                // Only claim success if it's still OUR pod (uid match) bound to the
                                // target — a same-name recreated/other pod must not be counted Bound.
                                let same_pod = p.metadata.uid.clone().unwrap_or_default()
                                    == entry.pod_uid
                                    && !entry.pod_uid.is_empty();
                                if same_pod && bound_to == entry.node_name {
                                    applied += 1;
                                    tracing::info!(
                                        ns = %entry.namespace, pod = %entry.pod_name,
                                        node = %entry.node_name,
                                        "REAL BIND applied (confirmed after response parse error)"
                                    );
                                    outcomes.push(BindOutcome {
                                        namespace: entry.namespace.clone(),
                                        pod: entry.pod_name.clone(),
                                        node: entry.node_name.clone(),
                                        result: BindResult::Bound { dry_run: false },
                                    });
                                    continue;
                                }
                            }
                        }
                        tracing::warn!(ns = %entry.namespace, pod = %entry.pod_name, error = %e, "real bind failed");
                        BindResult::Failed {
                            error: e.to_string(),
                        }
                    }
                };
                outcomes.push(BindOutcome {
                    namespace: entry.namespace.clone(),
                    pod: entry.pod_name.clone(),
                    node: entry.node_name.clone(),
                    result,
                });
            }
        }
    }
    outcomes
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::scheduler::binding::{BindReadiness, BindingPlanEntry};

    fn entry(uid: &str) -> BindingPlanEntry {
        BindingPlanEntry {
            namespace: "team".into(),
            pod_name: "a".into(),
            pod_uid: uid.into(),
            node_name: "n1".into(),
            gpu_request: 1,
            binding_body: serde_json::json!({}),
        }
    }
    fn live(uid: &str, phase: &str, node: &str) -> LivePodView {
        LivePodView {
            uid: uid.into(),
            phase: phase.into(),
            node_name: node.into(),
            deleting: false,
            scheduler_name: "ksolver".into(),
            uses_dra: false,
        }
    }

    #[test]
    fn skips_dra_pod_never_real_binds() {
        let mut lv = live("u", "Pending", "");
        lv.uses_dra = true;
        let d = should_apply(&entry("u"), &BindReadiness::Ready, Some(&lv), "ksolver");
        match d {
            ApplyDecision::Skip { reason } => assert!(reason.contains("DRA")),
            _ => panic!("DRA pod must never be real-bound"),
        }
    }

    #[test]
    fn applies_when_ready_and_live_pending_unbound_uid_match() {
        let d = should_apply(
            &entry("u"),
            &BindReadiness::Ready,
            Some(&live("u", "Pending", "")),
            "ksolver",
        );
        assert_eq!(d, ApplyDecision::Apply);
    }

    #[test]
    fn skips_when_readiness_stale() {
        let d = should_apply(
            &entry("u"),
            &BindReadiness::Stale { reason: "x".into() },
            Some(&live("u", "Pending", "")),
            "ksolver",
        );
        assert!(matches!(d, ApplyDecision::Skip { .. }));
    }

    #[test]
    fn skips_when_pod_gone() {
        let d = should_apply(&entry("u"), &BindReadiness::Ready, None, "ksolver");
        assert!(matches!(d, ApplyDecision::Skip { .. }));
    }

    #[test]
    fn skips_when_uid_changed() {
        let d = should_apply(
            &entry("u-OLD"),
            &BindReadiness::Ready,
            Some(&live("u-NEW", "Pending", "")),
            "ksolver",
        );
        assert!(matches!(d, ApplyDecision::Skip { .. }));
    }

    #[test]
    fn skips_when_uid_missing() {
        let d = should_apply(
            &entry(""),
            &BindReadiness::Ready,
            Some(&live("u", "Pending", "")),
            "ksolver",
        );
        assert!(matches!(d, ApplyDecision::Skip { .. }));
    }

    #[test]
    fn skips_when_already_bound() {
        let d = should_apply(
            &entry("u"),
            &BindReadiness::Ready,
            Some(&live("u", "Running", "n2")),
            "ksolver",
        );
        assert!(matches!(d, ApplyDecision::Skip { .. }));
    }

    #[test]
    fn skips_when_terminating() {
        let mut lv = live("u", "Pending", "");
        lv.deleting = true;
        let d = should_apply(&entry("u"), &BindReadiness::Ready, Some(&lv), "ksolver");
        assert!(matches!(d, ApplyDecision::Skip { .. }));
    }

    #[test]
    fn skips_when_scheduler_mismatch() {
        let mut lv = live("u", "Pending", "");
        lv.scheduler_name = "default-scheduler".into();
        let d = should_apply(&entry("u"), &BindReadiness::Ready, Some(&lv), "ksolver");
        assert!(matches!(d, ApplyDecision::Skip { .. }));
    }
}
