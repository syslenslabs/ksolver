//! PHASE 3 — real binding executor. This is the ONLY module that mutates cluster state, and only
//! when real binding is explicitly enabled and the kill switch is off. It POSTs pod→node `Binding`
//! subresources for decisions whose dry-run readiness is `Ready`, after a final live re-check.
//! Everything here is gated, throttled, and logged; a per-pod error never aborts the pass.

use crate::scheduler::binding::{BindReadiness, BindingPlanEntry};
use crate::scheduler::config::{BindingCanaryMode, ShadowConfig};
use crate::scheduler::ledger::{ReservationError, ReservationLedger};
use std::collections::{BTreeMap, BTreeSet};
use std::time::{Duration, Instant};

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
    pub pod_uid: String,
    pub team: String,
    pub node: String,
    pub result: BindResult,
}

impl BindOutcome {
    fn skip(entry: &BindingPlanEntry, reason: String) -> Self {
        BindOutcome {
            namespace: entry.namespace.clone(),
            pod: entry.pod_name.clone(),
            pod_uid: entry.pod_uid.clone(),
            team: entry.team.clone(),
            node: entry.node_name.clone(),
            result: BindResult::Skipped { reason },
        }
    }
}

fn binding_disabled_reason(cfg: &ShadowConfig) -> Option<&'static str> {
    if cfg.real_binding_mutations_enabled() {
        None
    } else if cfg.binding_kill_switch {
        Some("real binding disabled by kill switch")
    } else {
        Some("real binding disabled")
    }
}

/// Phase 6 safety gate: refuse real binding for the pass when candidate pruning was active AND its
/// scheduling regret is unknown. In that state we cannot prove pruning didn't change the placement,
/// so a real bind would be a decision we can't stand behind. Conservative — it can only ever BLOCK
/// binding, never enable it. When pruning wasn't active there is no pruning regret to worry about.
fn unknown_regret_block_reason(regret_status: &str, pruning_active: bool) -> Option<String> {
    if pruning_active && regret_status.contains("unknown") {
        Some(format!(
            "real binding blocked: candidate-pruning regret is {regret_status} (rerun with candidate_node_limit=0 to prove no scheduling regret before binding)"
        ))
    } else {
        None
    }
}

fn canary_skip_reason(entry: &BindingPlanEntry, cfg: &ShadowConfig) -> Option<String> {
    match cfg.binding_canary_mode {
        BindingCanaryMode::All => None,
        BindingCanaryMode::LowRisk => {
            if entry.gpu_request.max(0) <= cfg.binding_low_risk_max_gpus {
                None
            } else {
                Some(format!(
                    "binding canary low-risk mode: pod requests {} GPUs, max allowed {}",
                    entry.gpu_request, cfg.binding_low_risk_max_gpus
                ))
            }
        }
    }
}

fn binding_group(entry: &BindingPlanEntry) -> Option<&str> {
    let group = entry.binding_group.trim();
    (!group.is_empty()).then_some(group)
}

fn static_group_skip_reasons(
    plan: &[(BindingPlanEntry, BindReadiness)],
    cfg: &ShadowConfig,
) -> BTreeMap<String, String> {
    let mut groups: BTreeMap<String, Vec<(&BindingPlanEntry, &BindReadiness)>> = BTreeMap::new();
    for (entry, readiness) in plan {
        if let Some(group) = binding_group(entry) {
            groups
                .entry(group.to_string())
                .or_default()
                .push((entry, readiness));
        }
    }

    let mut skipped = BTreeMap::new();
    for (group, entries) in groups {
        if entries.len() <= 1 {
            continue;
        }
        if entries
            .iter()
            .any(|(_, readiness)| !matches!(readiness, BindReadiness::Ready))
        {
            skipped.insert(
                group,
                "binding group skipped: at least one member is not ready".to_string(),
            );
            continue;
        }
        if let Some(reason) = entries
            .iter()
            .find_map(|(entry, _)| canary_skip_reason(entry, cfg))
        {
            skipped.insert(
                group,
                format!("binding group skipped: one member failed canary policy ({reason})"),
            );
        }
    }
    skipped
}

fn live_pod_view(p: &k8s_openapi::api::core::v1::Pod) -> LivePodView {
    LivePodView {
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
    }
}

async fn fetch_live_pod(
    client: &kube::Client,
    namespace: &str,
    pod_name: &str,
) -> Result<Option<LivePodView>, kube::Error> {
    use k8s_openapi::api::core::v1 as corev1;
    use kube::api::Api;

    let pods: Api<corev1::Pod> = Api::namespaced(client.clone(), namespace);
    pods.get_opt(pod_name)
        .await
        .map(|p| p.as_ref().map(live_pod_view))
}

fn live_group_skip_reason<'a, I>(
    group: &str,
    entries: I,
    expected_scheduler: &str,
) -> Option<String>
where
    I: IntoIterator<
        Item = (
            &'a BindingPlanEntry,
            &'a BindReadiness,
            Option<&'a LivePodView>,
        ),
    >,
{
    for (entry, readiness, live) in entries {
        if let ApplyDecision::Skip { reason } =
            should_apply(entry, readiness, live, expected_scheduler)
        {
            return Some(format!(
                "binding group skipped: member {}/{} failed live preflight for {group}: {reason}",
                entry.namespace, entry.pod_name
            ));
        }
    }
    None
}

fn reservation_error_message(error: ReservationError) -> String {
    match error {
        ReservationError::InvalidEntry {
            namespace,
            pod,
            reason,
        } => {
            format!("binding reservation rejected: invalid entry {namespace}/{pod}: {reason}")
        }
        ReservationError::UnknownNode { node } => {
            format!("binding reservation rejected: unknown target node {node}")
        }
        ReservationError::NodeCapacityExceeded {
            node,
            requested,
            available,
        } => format!(
            "binding reservation rejected: node {node} requested {requested} GPUs, available {available}"
        ),
        ReservationError::TenantQuotaExceeded {
            tenant,
            requested,
            available,
        } => format!(
            "binding reservation rejected: tenant {tenant} requested {requested} GPUs, available {available}"
        ),
    }
}

/// Reserve the ready subset of a binding plan before any real POSTs are attempted. This is pure
/// except for the in-memory ledger mutation; on rejection the caller should skip the whole pass.
pub fn reserve_ready_bindings(
    ledger: &mut ReservationLedger,
    cluster: &crate::model::NormalizedCluster,
    tenant_quotas: &BTreeMap<String, i64>,
    plan: &[(BindingPlanEntry, BindReadiness)],
    ttl: Duration,
    now: Instant,
) -> Result<Option<u64>, Vec<BindOutcome>> {
    let ready: Vec<_> = plan
        .iter()
        .filter(|(_, readiness)| matches!(readiness, BindReadiness::Ready))
        .map(|(entry, _)| entry.clone())
        .collect();
    if ready.is_empty() {
        ledger.expire(now);
        return Ok(None);
    }
    match ledger.reserve(cluster, tenant_quotas, ready, ttl, now) {
        Ok(id) => Ok(Some(id)),
        Err(e) => {
            let reason = reservation_error_message(e);
            Err(plan
                .iter()
                .map(|(entry, _)| BindOutcome::skip(entry, reason.clone()))
                .collect())
        }
    }
}

/// Apply the ready bindings in `plan` (effectful; the ONLY mutation path). Returns one outcome per
/// entry. No-op (all skipped) unless real binding is enabled and the kill switch is off. Throttled
/// by `max_binds_per_pass`. With `real_binding_dry_run`, the POST carries server-side `dryRun=All`
/// (validated, not persisted).
pub async fn apply_bindings(
    client: &kube::Client,
    plan: &[(BindingPlanEntry, BindReadiness)],
    cfg: &ShadowConfig,
    regret_status: &str,
    pruning_active: bool,
) -> Vec<BindOutcome> {
    use k8s_openapi::api::core::v1 as corev1;
    use kube::api::{Api, PostParams};

    // Defense-in-depth: never mutate unless explicitly enabled, regardless of caller.
    if let Some(reason) = binding_disabled_reason(cfg) {
        return plan
            .iter()
            .map(|(e, _)| BindOutcome::skip(e, reason.into()))
            .collect();
    }

    // Phase 6: don't mutate when candidate pruning may have changed the placement and we can't
    // prove it didn't (unknown regret). Blocks the whole pass; each entry reports the reason.
    if let Some(reason) = unknown_regret_block_reason(regret_status, pruning_active) {
        return plan
            .iter()
            .map(|(e, _)| BindOutcome::skip(e, reason.clone()))
            .collect();
    }

    let pp = PostParams {
        dry_run: cfg.real_binding_dry_run,
        ..Default::default()
    };
    let static_group_skips = static_group_skip_reasons(plan, cfg);
    let mut dynamic_group_skips: BTreeMap<String, String> = BTreeMap::new();
    let mut seen_groups = BTreeSet::new();
    let mut live_group_checked = BTreeSet::new();
    let mut live_group_skips: BTreeMap<String, String> = BTreeMap::new();
    let mut live_cache: BTreeMap<(String, String), Option<LivePodView>> = BTreeMap::new();
    let mut outcomes = Vec::with_capacity(plan.len());
    let mut applied = 0usize;
    for (entry, readiness) in plan {
        if let Some(group) = binding_group(entry) {
            if let Some(reason) = static_group_skips.get(group) {
                outcomes.push(BindOutcome::skip(entry, reason.clone()));
                continue;
            }
            if let Some(reason) = dynamic_group_skips.get(group) {
                outcomes.push(BindOutcome::skip(entry, reason.clone()));
                continue;
            }
            if let Some(reason) = live_group_skips.get(group) {
                outcomes.push(BindOutcome::skip(entry, reason.clone()));
                continue;
            }
            if seen_groups.insert(group.to_string()) {
                let group_size = plan
                    .iter()
                    .filter(|(candidate, _)| binding_group(candidate) == Some(group))
                    .count();
                if applied.saturating_add(group_size) > cfg.max_binds_per_pass {
                    let reason = format!(
                        "binding group skipped: {group_size} members would exceed max binds per pass {}",
                        cfg.max_binds_per_pass
                    );
                    dynamic_group_skips.insert(group.to_string(), reason.clone());
                    outcomes.push(BindOutcome::skip(entry, reason));
                    continue;
                }
            }
            let group_size = plan
                .iter()
                .filter(|(candidate, _)| binding_group(candidate) == Some(group))
                .count();
            if group_size > 1 && live_group_checked.insert(group.to_string()) {
                let mut fetch_failed = None;
                for (member, _) in plan
                    .iter()
                    .filter(|(candidate, _)| binding_group(candidate) == Some(group))
                {
                    let key = (member.namespace.clone(), member.pod_name.clone());
                    if let std::collections::btree_map::Entry::Vacant(entry) = live_cache.entry(key)
                    {
                        match fetch_live_pod(client, &member.namespace, &member.pod_name).await {
                            Ok(live) => {
                                entry.insert(live);
                            }
                            Err(e) => {
                                fetch_failed = Some(format!(
                                    "binding group skipped: member {}/{} live get failed for {group}: {e}",
                                    member.namespace, member.pod_name
                                ));
                                break;
                            }
                        }
                    }
                }
                if let Some(reason) = fetch_failed {
                    live_group_skips.insert(group.to_string(), reason.clone());
                    outcomes.push(BindOutcome::skip(entry, reason));
                    continue;
                }
                let reason = live_group_skip_reason(
                    group,
                    plan.iter()
                        .filter(|(candidate, _)| binding_group(candidate) == Some(group))
                        .map(|(member, member_readiness)| {
                            let key = (member.namespace.clone(), member.pod_name.clone());
                            let live = live_cache.get(&key).and_then(|v| v.as_ref());
                            (member, member_readiness, live)
                        }),
                    &cfg.scheduler_name,
                );
                if let Some(reason) = reason {
                    live_group_skips.insert(group.to_string(), reason.clone());
                    outcomes.push(BindOutcome::skip(entry, reason));
                    continue;
                }
            }
        }
        if applied >= cfg.max_binds_per_pass {
            outcomes.push(BindOutcome::skip(
                entry,
                "max binds per pass reached".into(),
            ));
            continue;
        }
        if let Some(reason) = canary_skip_reason(entry, cfg) {
            outcomes.push(BindOutcome::skip(entry, reason));
            continue;
        }
        let pods: Api<corev1::Pod> = Api::namespaced(client.clone(), &entry.namespace);
        // Final live re-check (optimistic concurrency): fetch the pod right before binding. Do not
        // reuse the group preflight cache here; another scheduler could have changed this pod after
        // the group preflight and before this member's POST.
        let live = match fetch_live_pod(client, &entry.namespace, &entry.pod_name).await {
            Ok(live) => live,
            Err(e) => {
                outcomes.push(BindOutcome {
                    namespace: entry.namespace.clone(),
                    pod: entry.pod_name.clone(),
                    pod_uid: entry.pod_uid.clone(),
                    team: entry.team.clone(),
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
                            pod_uid: entry.pod_uid.clone(),
                            team: entry.team.clone(),
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
                                        pod_uid: entry.pod_uid.clone(),
                                        team: entry.team.clone(),
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
                    pod_uid: entry.pod_uid.clone(),
                    team: entry.team.clone(),
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
            binding_group: String::new(),
            team: String::new(),
            node_name: "n1".into(),
            gpu_request: 1,
            binding_body: serde_json::json!({}),
        }
    }

    fn grouped_entry(uid: &str, pod: &str, group: &str, gpu: i64) -> BindingPlanEntry {
        BindingPlanEntry {
            pod_name: pod.into(),
            binding_group: group.into(),
            gpu_request: gpu,
            ..entry(uid)
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

    fn node(name: &str, gpu: i64) -> crate::model::NormalizedNode {
        crate::model::NormalizedNode {
            name: name.to_string(),
            extended_resources: BTreeMap::from([("nvidia.com/gpu".to_string(), gpu)]),
            ..Default::default()
        }
    }

    fn running(ns: &str, pod: &str, node: &str, gpu: i64) -> crate::model::NormalizedWorkload {
        crate::model::NormalizedWorkload {
            namespace: ns.to_string(),
            name: pod.to_string(),
            current_node: node.to_string(),
            extended_resource_requests: BTreeMap::from([("nvidia.com/gpu".to_string(), gpu)]),
            ..Default::default()
        }
    }

    fn cluster() -> crate::model::NormalizedCluster {
        crate::model::NormalizedCluster {
            nodes: vec![node("n1", 2)],
            workloads: vec![running("team", "used", "n1", 1)],
            ..Default::default()
        }
    }

    fn cfg() -> ShadowConfig {
        ShadowConfig {
            scheduler_name: "ksolver".to_string(),
            batch_window: Duration::from_secs(10),
            namespace_allowlist: vec![],
            gpu_resource_names: vec!["nvidia.com/gpu".to_string()],
            gpu_resource_prefixes: vec!["nvidia.com/mig-".to_string()],
            cluster_name: "default".to_string(),
            kubeconfig: String::new(),
            http_addr: "127.0.0.1:8090".to_string(),
            admission_opt_in_label: String::new(),
            gang_label_key: "scheduling.x-k8s.io/pod-group".to_string(),
            gang_colocate_label: "scheduling.x-k8s.io/gang-colocate".to_string(),
            solve_time_limit_secs: 10,
            namespace_gpu_quotas: BTreeMap::new(),
            tenant_share_weights: BTreeMap::new(),
            tenant_monthly_budgets_milli: BTreeMap::new(),
            queue_weights: BTreeMap::new(),
            enable_real_binding: false,
            binding_rollout_mode: crate::scheduler::config::BindingRolloutMode::ObserveOnly,
            binding_kill_switch: false,
            enable_kubernetes_events: false,
            real_binding_dry_run: false,
            binding_canary_mode: BindingCanaryMode::All,
            binding_low_risk_max_gpus: 1,
            max_binds_per_pass: 10,
            binding_reservation_ttl: Duration::from_secs(60),
            objective_profile: crate::model::ObjectiveProfile::CostBinpack,
            objective_weights: crate::model::ObjectiveWeights::default(),
            candidate_node_limit: 0,
            candidate_widen_min_admission_percent_milli: 50_000,
            enable_node_grouping: false,
            repair_candidate_limit: 8,
            enable_leader_election: false,
            leader_election_namespace: "ksolver".to_string(),
            leader_election_lease_name: "ksolver-scheduler".to_string(),
            leader_election_identity: "ksolver".to_string(),
        }
    }

    #[test]
    fn kill_switch_disables_binding_even_when_enable_flag_is_true() {
        let mut cfg = cfg();
        assert_eq!(binding_disabled_reason(&cfg), Some("real binding disabled"));
        cfg.enable_real_binding = true;
        assert_eq!(binding_disabled_reason(&cfg), None);
        cfg.binding_kill_switch = true;
        assert_eq!(
            binding_disabled_reason(&cfg),
            Some("real binding disabled by kill switch")
        );
    }

    #[test]
    fn unknown_regret_blocks_binding_only_when_pruning_active() {
        // Blocked: pruning changed the candidate set AND regret couldn't be measured.
        assert!(unknown_regret_block_reason("unknown", true).is_some());
        assert!(unknown_regret_block_reason("unknown-regret", true).is_some());
        // Not blocked: pruning wasn't active (no pruning regret to worry about).
        assert!(unknown_regret_block_reason("unknown", false).is_none());
        // Not blocked: regret is measured/bounded, even with pruning active.
        assert!(unknown_regret_block_reason("no_measured_regret", true).is_none());
        assert!(unknown_regret_block_reason("measured_regret", true).is_none());
        assert!(unknown_regret_block_reason("full_feasible_set", true).is_none());
    }

    #[test]
    fn canary_policy_allows_all_by_default() {
        let mut cfg = cfg();
        cfg.binding_canary_mode = BindingCanaryMode::All;
        let mut e = entry("u");
        e.gpu_request = 8;

        assert_eq!(canary_skip_reason(&e, &cfg), None);
    }

    #[test]
    fn canary_policy_skips_large_gpu_requests_in_low_risk_mode() {
        let mut cfg = cfg();
        cfg.binding_canary_mode = BindingCanaryMode::LowRisk;
        cfg.binding_low_risk_max_gpus = 1;
        let mut small = entry("small");
        small.gpu_request = 1;
        let mut large = entry("large");
        large.gpu_request = 2;

        assert_eq!(canary_skip_reason(&small, &cfg), None);
        let reason = canary_skip_reason(&large, &cfg).expect("large job should be skipped");
        assert!(reason.contains("low-risk mode"));
        assert!(reason.contains("2 GPUs"));
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

    #[test]
    fn reserves_ready_bindings_before_apply_path() {
        let now = Instant::now();
        let mut ledger = ReservationLedger::new();
        let plan = vec![(entry("u"), BindReadiness::Ready)];
        let id = reserve_ready_bindings(
            &mut ledger,
            &cluster(),
            &BTreeMap::from([("team".to_string(), 2)]),
            &plan,
            Duration::from_secs(60),
            now,
        )
        .expect("reservation should fit")
        .expect("ready plan should reserve");

        assert_eq!(ledger.committed_gpu_by_node().get("n1"), Some(&1));
        assert!(ledger.release(id));
    }

    #[test]
    fn reservation_failure_skips_entire_binding_pass() {
        let now = Instant::now();
        let mut ledger = ReservationLedger::new();
        let plan = vec![(entry("u"), BindReadiness::Ready)];
        let outcomes = reserve_ready_bindings(
            &mut ledger,
            &cluster(),
            &BTreeMap::from([("team".to_string(), 1)]),
            &plan,
            Duration::from_secs(60),
            now,
        )
        .expect_err("quota should reject the reservation");

        assert_eq!(outcomes.len(), 1);
        match &outcomes[0].result {
            BindResult::Skipped { reason } => assert!(reason.contains("tenant team")),
            other => panic!("expected reservation skip, got {other:?}"),
        }
        assert!(ledger.is_empty());
    }

    #[test]
    fn malformed_reservation_entry_skips_entire_binding_pass() {
        let now = Instant::now();
        let mut ledger = ReservationLedger::new();
        let mut invalid = entry("u");
        invalid.gpu_request = 0;
        let plan = vec![(invalid, BindReadiness::Ready)];

        let outcomes = reserve_ready_bindings(
            &mut ledger,
            &cluster(),
            &BTreeMap::new(),
            &plan,
            Duration::from_secs(60),
            now,
        )
        .expect_err("malformed reservation should reject the pass");

        assert_eq!(outcomes.len(), 1);
        match &outcomes[0].result {
            BindResult::Skipped { reason } => {
                assert!(reason.contains("invalid entry"));
                assert!(reason.contains("non-positive GPU request"));
            }
            other => panic!("expected reservation skip, got {other:?}"),
        }
        assert!(ledger.is_empty());
    }

    #[test]
    fn group_preflight_skips_entire_group_when_any_member_stale() {
        let cfg = cfg();
        let plan = vec![
            (
                grouped_entry("u0", "m0", "gang:team/train", 1),
                BindReadiness::Ready,
            ),
            (
                grouped_entry("u1", "m1", "gang:team/train", 1),
                BindReadiness::Stale {
                    reason: "pod recreated".to_string(),
                },
            ),
        ];

        let reasons = static_group_skip_reasons(&plan, &cfg);
        let reason = reasons
            .get("gang:team/train")
            .expect("group should be skipped");
        assert!(reason.contains("not ready"));
    }

    #[test]
    fn group_preflight_skips_entire_group_when_any_member_fails_canary() {
        let mut cfg = cfg();
        cfg.binding_canary_mode = BindingCanaryMode::LowRisk;
        cfg.binding_low_risk_max_gpus = 1;
        let plan = vec![
            (
                grouped_entry("u0", "m0", "gang:team/train", 1),
                BindReadiness::Ready,
            ),
            (
                grouped_entry("u1", "m1", "gang:team/train", 4),
                BindReadiness::Ready,
            ),
        ];

        let reasons = static_group_skip_reasons(&plan, &cfg);
        let reason = reasons
            .get("gang:team/train")
            .expect("group should be skipped");
        assert!(reason.contains("canary"));
    }

    #[test]
    fn live_group_preflight_skips_entire_group_when_any_member_changed() {
        let e0 = grouped_entry("u0", "m0", "gang:team/train", 1);
        let e1 = grouped_entry("u1", "m1", "gang:team/train", 1);
        let mut live1 = live("u1", "Pending", "");
        live1.scheduler_name = "default-scheduler".to_string();
        let reason = live_group_skip_reason(
            "gang:team/train",
            [
                (&e0, &BindReadiness::Ready, Some(&live("u0", "Pending", ""))),
                (&e1, &BindReadiness::Ready, Some(&live1)),
            ],
            "ksolver",
        )
        .expect("group should be skipped");

        assert!(reason.contains("m1"));
        assert!(reason.contains("default-scheduler"));
    }

    #[test]
    fn live_group_preflight_allows_group_when_all_members_apply() {
        let e0 = grouped_entry("u0", "m0", "gang:team/train", 1);
        let e1 = grouped_entry("u1", "m1", "gang:team/train", 1);
        let l0 = live("u0", "Pending", "");
        let l1 = live("u1", "Pending", "");

        let reason = live_group_skip_reason(
            "gang:team/train",
            [
                (&e0, &BindReadiness::Ready, Some(&l0)),
                (&e1, &BindReadiness::Ready, Some(&l1)),
            ],
            "ksolver",
        );

        assert_eq!(reason, None);
    }

    #[test]
    fn no_ready_bindings_only_expires_ledger() {
        let now = Instant::now();
        let mut ledger = ReservationLedger::new();
        ledger
            .reserve(
                &cluster(),
                &BTreeMap::new(),
                vec![entry("reserved")],
                Duration::from_secs(1),
                now,
            )
            .expect("initial reservation should fit");

        let plan = vec![(
            entry("u"),
            BindReadiness::Stale {
                reason: "stale".to_string(),
            },
        )];
        let got = reserve_ready_bindings(
            &mut ledger,
            &cluster(),
            &BTreeMap::new(),
            &plan,
            Duration::from_secs(60),
            now + Duration::from_secs(2),
        )
        .expect("stale-only plan should not fail");

        assert_eq!(got, None);
        assert!(ledger.is_empty());
    }
}
