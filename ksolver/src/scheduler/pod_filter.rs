use crate::scheduler::config::ShadowConfig;
use chrono::{DateTime, Utc};
use k8s_openapi::api::core::v1 as corev1;

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct PendingGpuPod {
    pub uid: String,
    pub namespace: String,
    pub name: String,
    pub gpu_request: i64,
    /// Normalized scheduling priority. Defaults to 0 when neither Kubernetes nor ksolver priority
    /// hints are set. `ksolver.dev/priority` overrides Kubernetes `spec.priority` when present.
    pub priority: i64,
    pub priority_class_name: Option<String>,
    pub team: Option<String>,
    pub queue: Option<String>,
    pub business_value: i64,
    /// Seconds since Kubernetes `metadata.creationTimestamp` at classification time.
    pub queue_wait_seconds: i64,
    pub deadline_unix_seconds: i64,
    pub min_gpus: i64,
    pub max_gpus: i64,
    pub preferred_gpus: i64,
    pub flexible: bool,
    pub predicted_runtime_seconds: i64,
    pub predicted_peak_vram_bytes: i64,
    /// Hard GPU-topology requirements lowered from explicit ksolver annotations.
    /// Each pair requires candidate nodes to carry the exact label value. This is used for
    /// NVLink/NVSwitch/NUMA island hints that Kubernetes does not expose as scalar GPU capacity.
    pub required_gpu_topology: Vec<(String, String)>,
    /// `Some("{namespace}/{label-value}")` when the configured gang label is present.
    pub gang_key: Option<String>,
    /// True when the configured co-location label is set to "true" (gang wants one node).
    pub colocate: bool,
    /// Scheduling constraints present on the pod that shadow mode does NOT model
    /// (e.g. "pod affinity", "pod anti-affinity", "topology spread"); surfaced as
    /// per-decision caveats so recommendations are not silently misleading.
    pub unmodeled_constraints: Vec<String>,
    /// *Modeled* hostname pod-anti-affinity selectors (reqs + namespace scope) for best-effort node
    /// exclusion against running pods. Empty when none/unmodeled. (The "pod anti-affinity" caveat is
    /// still raised.)
    pub anti_affinity_host_selectors: Vec<crate::model::AntiAffinitySelector>,
    /// `(topologyKey, selector)` of modeled required pod-affinity terms. Best-effort: used to keep
    /// pending pods near matching already-running pods, while the "pod affinity" caveat remains.
    pub affinity_topology_selectors: Vec<(String, crate::model::AntiAffinitySelector)>,
    /// `(topologyKey, selector)` of *modeled* NON-hostname pod-anti-affinity terms (zone/rack) for
    /// best-effort topology-domain exclusion (Phase 12).
    pub anti_affinity_topology_selectors: Vec<(String, crate::model::AntiAffinitySelector)>,
    /// Preferred (soft) node-affinity terms (weight + matchExpressions) for the soft tie-break pass.
    pub preferred_node_affinity: Vec<crate::model::PreferredNodeTerm>,
    /// Preferred (soft) pod affinity + anti-affinity terms for the soft tie-break pass.
    pub preferred_pod_affinity: Vec<crate::model::PreferredPodTerm>,
}

/// GPU counts are whole numbers; anything non-integer floors to 0 (fractional GPUs
/// are a later phase).
fn parse_gpu_quantity(raw: &str) -> i64 {
    raw.trim().parse::<i64>().unwrap_or(0)
}

fn normalize_priority(raw: i64) -> i64 {
    if raw <= 0 {
        return 0;
    }
    // Kubernetes priority values can be very large. Bucket to a bounded score so user workloads
    // can be compared without letting system-class magnitudes dominate the objective.
    ((raw + 999) / 1000).clamp(1, 1000)
}

fn pod_priority(pod: &corev1::Pod, spec: &corev1::PodSpec) -> i64 {
    let annotated = pod
        .metadata
        .annotations
        .as_ref()
        .and_then(|a| a.get("ksolver.dev/priority"))
        .and_then(|v| v.trim().parse::<i64>().ok());
    normalize_priority(annotated.or(spec.priority.map(i64::from)).unwrap_or(0))
}

fn annotation(pod: &corev1::Pod, key: &str) -> Option<String> {
    pod.metadata
        .annotations
        .as_ref()
        .and_then(|a| a.get(key))
        .map(|v| v.trim())
        .filter(|v| !v.is_empty())
        .map(|v| v.to_string())
}

fn business_value(pod: &corev1::Pod) -> i64 {
    annotation(pod, "ksolver.dev/business-value")
        .and_then(|v| v.parse::<i64>().ok())
        .unwrap_or(0)
        .max(0)
}

fn annotation_i64(pod: &corev1::Pod, key: &str) -> i64 {
    annotation(pod, key)
        .and_then(|v| v.parse::<i64>().ok())
        .unwrap_or(0)
        .max(0)
}

fn annotation_f64(pod: &corev1::Pod, key: &str) -> f64 {
    annotation(pod, key)
        .and_then(|v| v.parse::<f64>().ok())
        .filter(|v| v.is_finite() && *v > 0.0)
        .unwrap_or(0.0)
}

fn annotation_bool(pod: &corev1::Pod, key: &str) -> bool {
    matches!(
        annotation(pod, key)
            .map(|v| v.trim().to_ascii_lowercase())
            .as_deref(),
        Some("true") | Some("1") | Some("yes") | Some("on")
    )
}

fn required_gpu_topology(pod: &corev1::Pod) -> Vec<(String, String)> {
    let mut out = Vec::new();
    if let (Some(key), Some(value)) = (
        annotation(pod, "ksolver.dev/gpu-topology-key"),
        annotation(pod, "ksolver.dev/gpu-topology-value"),
    ) {
        out.push((key, value));
    }
    if let Some(value) = annotation(pod, "ksolver.dev/nvlink-domain") {
        out.push(("ksolver.dev/nvlink-domain".to_string(), value));
    }
    out.sort();
    out.dedup();
    out
}

fn queue_wait_seconds(pod: &corev1::Pod) -> i64 {
    pod.metadata
        .creation_timestamp
        .as_ref()
        .map(|t| Utc::now().timestamp().saturating_sub(t.0.timestamp()))
        .unwrap_or(0)
        .max(0)
}

fn deadline_unix_seconds(pod: &corev1::Pod) -> i64 {
    annotation(pod, "ksolver.dev/deadline")
        .and_then(|v| DateTime::parse_from_rfc3339(&v).ok())
        .map(|dt| dt.with_timezone(&Utc).timestamp())
        .unwrap_or(0)
        .max(0)
}

fn precision_multiplier(raw: Option<String>) -> f64 {
    match raw
        .as_deref()
        .map(str::trim)
        .map(str::to_ascii_lowercase)
        .as_deref()
    {
        Some("fp32") | Some("float32") => 1.8,
        Some("bf16") | Some("bfloat16") | Some("fp16") | Some("float16") | Some("half") => 1.0,
        Some("fp8") | Some("int8") => 0.7,
        _ => 1.2,
    }
}

fn precision_bytes_per_param(raw: Option<String>) -> f64 {
    match raw
        .as_deref()
        .map(str::trim)
        .map(str::to_ascii_lowercase)
        .as_deref()
    {
        Some("fp32") | Some("float32") => 4.0,
        Some("bf16") | Some("bfloat16") | Some("fp16") | Some("float16") | Some("half") => 2.0,
        Some("fp8") | Some("int8") => 1.0,
        _ => 2.0,
    }
}

fn predicted_runtime_seconds(pod: &corev1::Pod, gpu_request: i64) -> i64 {
    let explicit = annotation_i64(pod, "ksolver.dev/predicted-runtime-seconds");
    if explicit > 0 {
        return explicit;
    }

    let params_b = annotation_f64(pod, "ksolver.dev/model-parameters-billions");
    let batch_size = annotation_i64(pod, "ksolver.dev/batch-size");
    let sequence_length = annotation_i64(pod, "ksolver.dev/sequence-length");
    if params_b <= 0.0 || batch_size <= 0 {
        return 0;
    }

    let seq_factor = if sequence_length > 0 {
        (sequence_length as f64 / 2048.0).clamp(0.5, 8.0)
    } else {
        1.0
    };
    let gpu_factor = gpu_request.max(1) as f64;
    let precision = precision_multiplier(annotation(pod, "ksolver.dev/precision"));

    // Deliberately conservative bootstrap heuristic until historical predictors exist. It is
    // monotonic in model size, batch size, sequence length, and inverse GPU count, bounded to avoid
    // a bad hint overwhelming the solver.
    let seconds =
        600.0 * params_b * (batch_size as f64 / 32.0) * seq_factor * precision / gpu_factor.sqrt();
    seconds.round().clamp(60.0, 30.0 * 24.0 * 3600.0) as i64
}

fn predicted_peak_vram_bytes(pod: &corev1::Pod, gpu_request: i64) -> i64 {
    let explicit_bytes = annotation_i64(pod, "ksolver.dev/predicted-peak-vram-bytes");
    if explicit_bytes > 0 {
        return explicit_bytes;
    }
    let explicit_gib = annotation_f64(pod, "ksolver.dev/predicted-peak-vram-gib");
    if explicit_gib > 0.0 {
        return (explicit_gib * 1024.0 * 1024.0 * 1024.0).round() as i64;
    }

    let params_b = annotation_f64(pod, "ksolver.dev/model-parameters-billions");
    let batch_size = annotation_i64(pod, "ksolver.dev/batch-size");
    if params_b <= 0.0 || batch_size <= 0 {
        return 0;
    }
    let sequence_length = annotation_i64(pod, "ksolver.dev/sequence-length");
    let seq_factor = if sequence_length > 0 {
        (sequence_length as f64 / 2048.0).clamp(0.5, 8.0)
    } else {
        1.0
    };
    let precision = annotation(pod, "ksolver.dev/precision");
    let bytes_per_param = precision_bytes_per_param(precision);
    let gpu_factor = gpu_request.max(1) as f64;

    // Bootstrap estimate for peak per-GPU VRAM. Model weights dominate the base; batch/sequence
    // hints approximate activation/checkpoint pressure. This is intentionally conservative and
    // bounded until historical measurements replace it.
    let model_gib = params_b * bytes_per_param * 1.25;
    let activation_gib = params_b * (batch_size as f64 / 32.0) * seq_factor * 1.5;
    let per_gpu_gib = (model_gib + activation_gib) / gpu_factor.sqrt();
    (per_gpu_gib.clamp(1.0, 1920.0) * 1024.0 * 1024.0 * 1024.0).round() as i64
}

/// Sum of GPU resources (exact names or MIG prefixes, per `cfg.is_gpu_resource`) within one
/// container. The effective request is computed PER resource — `requests[r]` if set, otherwise
/// `limits[r]` (Kubernetes semantics) — NOT per map. Doing it per-map (take requests wholesale,
/// fall back to limits only when requests is entirely absent) drops a GPU declared only in
/// `limits` whenever cpu/mem sit in `requests`, reading such a pod as 0 GPUs.
fn container_gpu(container: &corev1::Container, cfg: &ShadowConfig) -> i64 {
    let Some(res) = container.resources.as_ref() else {
        return 0;
    };
    let requests = res.requests.as_ref();
    let mut total = 0i64;
    if let Some(requests) = requests {
        for (name, qty) in requests {
            if cfg.is_gpu_resource(name) {
                total += parse_gpu_quantity(&qty.0);
            }
        }
    }
    // Add GPU resources present only in limits (absent from requests): effective = requests[r]
    // else limits[r], applied per resource so requests wins where both are set.
    if let Some(limits) = res.limits.as_ref() {
        for (name, qty) in limits {
            if cfg.is_gpu_resource(name) && requests.map(|r| !r.contains_key(name)).unwrap_or(true)
            {
                total += parse_gpu_quantity(&qty.0);
            }
        }
    }
    total
}

/// Kubernetes effective resource request across all GPU resources (whole GPUs + MIG slices):
/// max(sum of normal containers, max over init containers).
pub fn effective_gpu_request(pod: &corev1::Pod, cfg: &ShadowConfig) -> i64 {
    let Some(spec) = pod.spec.as_ref() else {
        return 0;
    };
    let normal_sum: i64 = spec.containers.iter().map(|c| container_gpu(c, cfg)).sum();

    // Kubernetes effective request across init containers (KEP-753 sidecars). Plain init
    // containers run in sequence, so their demand is a running max — not additive. A restartable
    // init container (`restartPolicy: Always`, i.e. a native sidecar) runs CONCURRENTLY with the
    // app containers, so its demand ADDS. Walk init containers in order tracking accumulated
    // restartable demand; each init container peaks at its own demand plus everything restartable
    // that started before it. The app phase then runs all normal containers plus all sidecars.
    let mut restartable_sum: i64 = 0;
    let mut init_peak: i64 = 0;
    if let Some(inits) = spec.init_containers.as_ref() {
        for c in inits {
            let c_gpu = container_gpu(c, cfg);
            init_peak = init_peak.max(restartable_sum + c_gpu);
            if c.restart_policy.as_deref() == Some("Always") {
                restartable_sum += c_gpu;
            }
        }
    }
    (normal_sum + restartable_sum).max(init_peak)
}

pub fn classify(pod: &corev1::Pod, cfg: &ShadowConfig) -> Option<PendingGpuPod> {
    let namespace = pod.metadata.namespace.clone().unwrap_or_default();
    let name = pod.metadata.name.clone().unwrap_or_default();
    let uid = pod.metadata.uid.clone().unwrap_or_default();
    if !cfg.namespace_in_scope(&namespace) {
        return None;
    }
    if pod.metadata.deletion_timestamp.is_some() {
        return None;
    }
    let spec = pod.spec.as_ref()?;
    if spec.scheduler_name.as_deref() != Some(cfg.scheduler_name.as_str()) {
        return None;
    }
    if spec
        .node_name
        .as_deref()
        .map(|n| !n.is_empty())
        .unwrap_or(false)
    {
        return None;
    }
    if let Some(phase) = pod.status.as_ref().and_then(|s| s.phase.as_deref()) {
        if phase != "Pending" {
            return None;
        }
    }
    let gpu = effective_gpu_request(pod, cfg);
    // DRA pods request GPUs via `spec.resourceClaims` (not container limits); keep them in scope so
    // shadow schedules them. Their actual per-DeviceClass demand rides the NormalizedWorkload's
    // extended_resource_requests (injected by the collector's DRA augmentation), so `gpu` may be 0.
    let uses_dra = spec
        .resource_claims
        .as_ref()
        .map(|c| !c.is_empty())
        .unwrap_or(false);
    if gpu < 1 && !uses_dra {
        return None;
    }
    let gang_key = if cfg.gang_label_key.is_empty() {
        None
    } else {
        pod.metadata
            .labels
            .as_ref()
            .and_then(|l| l.get(&cfg.gang_label_key))
            .filter(|v| !v.is_empty())
            .map(|v| format!("{namespace}/{v}"))
    };
    let colocate = !cfg.gang_colocate_label.is_empty()
        && pod
            .metadata
            .labels
            .as_ref()
            .and_then(|l| l.get(&cfg.gang_colocate_label))
            .map(|v| v == "true")
            .unwrap_or(false);
    let mut unmodeled_constraints = unmodeled_constraints(spec);
    if uses_dra {
        // Disclose that DRA device matching is a scalar approximation (exact per-device assignment
        // and full CEL selectors are not modeled — see the `dra` module contract).
        unmodeled_constraints
            .push("DRA: device demand modeled as scalar approximation".to_string());
    }
    let affinity_topology_selectors = modeled_affinity_selectors(spec);
    let all = modeled_anti_affinity_selectors(spec);
    let anti_affinity_host_selectors = all
        .iter()
        .filter(|(k, _)| k == "kubernetes.io/hostname")
        .map(|(_, sel)| sel.clone())
        .collect();
    let anti_affinity_topology_selectors = all
        .into_iter()
        .filter(|(k, _)| k != "kubernetes.io/hostname")
        .collect();
    let preferred_node_affinity = modeled_preferred_node_affinity(spec);
    let preferred_pod_affinity = modeled_preferred_pod_affinity(spec);
    let priority = pod_priority(pod, spec);
    let team = annotation(pod, "ksolver.dev/team");
    let queue = annotation(pod, "ksolver.dev/queue");
    let business_value = business_value(pod);
    let queue_wait_seconds = queue_wait_seconds(pod);
    let deadline_unix_seconds = deadline_unix_seconds(pod);
    let min_gpus = annotation_i64(pod, "ksolver.dev/min-gpus");
    let max_gpus = annotation_i64(pod, "ksolver.dev/max-gpus");
    let preferred_gpus = annotation_i64(pod, "ksolver.dev/preferred-gpus");
    let flexible = annotation_bool(pod, "ksolver.dev/flexible");
    let predicted_runtime_seconds = predicted_runtime_seconds(pod, gpu);
    let predicted_peak_vram_bytes = predicted_peak_vram_bytes(pod, gpu);
    let required_gpu_topology = required_gpu_topology(pod);
    if !required_gpu_topology.is_empty() {
        unmodeled_constraints.push(format!(
            "GPU topology: requires {}",
            required_gpu_topology
                .iter()
                .map(|(k, v)| format!("{k}={v}"))
                .collect::<Vec<_>>()
                .join(",")
        ));
    }
    Some(PendingGpuPod {
        uid,
        namespace,
        name,
        gpu_request: gpu,
        priority,
        priority_class_name: spec.priority_class_name.clone(),
        team,
        queue,
        business_value,
        queue_wait_seconds,
        deadline_unix_seconds,
        min_gpus,
        max_gpus,
        preferred_gpus,
        flexible,
        predicted_runtime_seconds,
        predicted_peak_vram_bytes,
        required_gpu_topology,
        gang_key,
        colocate,
        unmodeled_constraints,
        anti_affinity_host_selectors,
        affinity_topology_selectors,
        anti_affinity_topology_selectors,
        preferred_node_affinity,
        preferred_pod_affinity,
    })
}

/// Preferred (soft) node-affinity terms from `nodeAffinity.preferredDuringScheduling…`.
/// matchExpressions are lowered to label requirements; matchFields are lowered to field
/// requirements and later evaluated with the same narrow `metadata.name` support as required node
/// affinity. Weight ≤ 0 or empty selector ⇒ term dropped.
fn modeled_preferred_node_affinity(spec: &corev1::PodSpec) -> Vec<crate::model::PreferredNodeTerm> {
    let mut out = Vec::new();
    let Some(terms) = spec
        .affinity
        .as_ref()
        .and_then(|a| a.node_affinity.as_ref())
        .and_then(|na| {
            na.preferred_during_scheduling_ignored_during_execution
                .as_ref()
        })
    else {
        return out;
    };
    for t in terms {
        if t.weight <= 0 {
            continue;
        }
        let exprs: Vec<crate::model::NodeAffinityTerm> = t
            .preference
            .match_expressions
            .as_ref()
            .map(|es| {
                es.iter()
                    .map(|e| crate::model::NodeAffinityTerm {
                        key: e.key.clone(),
                        operator: e.operator.clone(),
                        values: e.values.clone().unwrap_or_default(),
                    })
                    .collect()
            })
            .unwrap_or_default();
        let fields: Vec<crate::model::NodeAffinityTerm> = t
            .preference
            .match_fields
            .as_ref()
            .map(|fs| {
                fs.iter()
                    .map(|f| crate::model::NodeAffinityTerm {
                        key: f.key.clone(),
                        operator: f.operator.clone(),
                        values: f.values.clone().unwrap_or_default(),
                    })
                    .collect()
            })
            .unwrap_or_default();
        if exprs.is_empty() && fields.is_empty() {
            continue;
        }
        out.push(crate::model::PreferredNodeTerm {
            weight: i64::from(t.weight),
            exprs,
            fields,
        });
    }
    out
}

/// Preferred (soft) pod affinity + anti-affinity terms from `podAffinity`/`podAntiAffinity`
/// `preferredDuringScheduling…`. Each `WeightedPodAffinityTerm` lowers to a `PreferredPodTerm`
/// (weight>0, modelable labelSelector, modelable namespaceSelector); unmodelable selectors are
/// skipped (best-effort soft — no caveat). `anti=true` for anti-affinity. Selector lowering shared
/// with the collector; namespace scope reuses `AntiAffinitySelector` (empty `namespaces` ⇒ own ns).
fn modeled_preferred_pod_affinity(spec: &corev1::PodSpec) -> Vec<crate::model::PreferredPodTerm> {
    crate::collector::modeled_preferred_pod_terms(spec.affinity.as_ref())
}

fn modeled_affinity_selectors(
    spec: &corev1::PodSpec,
) -> Vec<(String, crate::model::AntiAffinitySelector)> {
    let Some(terms) = spec
        .affinity
        .as_ref()
        .and_then(|a| a.pod_affinity.as_ref())
        .and_then(|a| {
            a.required_during_scheduling_ignored_during_execution
                .as_ref()
        })
    else {
        return Vec::new();
    };
    let mut out = Vec::new();
    for term in terms {
        let namespace_selector = match term.namespace_selector.as_ref() {
            None => None,
            Some(ns_ls) => match crate::collector::namespace_selector_to_reqs(ns_ls) {
                Some(reqs) => Some(reqs),
                None => continue,
            },
        };
        let Some(ls) = term.label_selector.as_ref() else {
            continue;
        };
        if let Some(reqs) = crate::collector::label_selector_to_reqs(ls) {
            out.push((
                term.topology_key.clone(),
                crate::model::AntiAffinitySelector {
                    reqs,
                    namespaces: term.namespaces.clone().unwrap_or_default(),
                    namespace_selector,
                },
            ));
        }
    }
    out
}

/// `(topologyKey, selector)` of required pod-anti-affinity terms we can fully model for best-effort
/// exclusion: a modelable labelSelector (In/NotIn/Exists/DoesNotExist, non-empty). An explicit
/// `namespaces` list is captured (F-CNS-1; empty ⇒ own namespace); a `namespaceSelector` with
/// supported operators is captured too (F-CNS-2; empty `{}` = all namespaces), while an unmodelable
/// namespaceSelector leaves the whole term unmodeled (caveated). Callers split hostname vs
/// non-hostname. Selector lowering is shared with the collector.
fn modeled_anti_affinity_selectors(
    spec: &corev1::PodSpec,
) -> Vec<(String, crate::model::AntiAffinitySelector)> {
    let mut out = Vec::new();
    let Some(terms) = spec
        .affinity
        .as_ref()
        .and_then(|a| a.pod_anti_affinity.as_ref())
        .and_then(|a| {
            a.required_during_scheduling_ignored_during_execution
                .as_ref()
        })
    else {
        return out;
    };
    for term in terms {
        // namespaceSelector (F-CNS-2): None if absent; Some(reqs) if modelable (empty {} = all);
        // unmodelable ⇒ skip the whole term (stays caveated).
        let namespace_selector = match term.namespace_selector.as_ref() {
            None => None,
            Some(ns_ls) => match crate::collector::namespace_selector_to_reqs(ns_ls) {
                Some(reqs) => Some(reqs),
                None => continue,
            },
        };
        let Some(ls) = term.label_selector.as_ref() else {
            continue;
        };
        if let Some(reqs) = crate::collector::label_selector_to_reqs(ls) {
            let namespaces = term.namespaces.clone().unwrap_or_default();
            out.push((
                term.topology_key.clone(),
                crate::model::AntiAffinitySelector {
                    reqs,
                    namespaces,
                    namespace_selector,
                },
            ));
        }
    }
    out
}

/// Names of hard scheduling constraints present on the pod that shadow does not model:
/// required pod affinity, required pod anti-affinity, and unsupported DoNotSchedule topology spread.
/// (Node affinity IS enforced by feasibility, so it is not listed.)
fn unmodeled_constraints(spec: &corev1::PodSpec) -> Vec<String> {
    let mut out = Vec::new();
    let has_terms = |t: &Option<Vec<corev1::PodAffinityTerm>>| {
        t.as_ref().map(|v| !v.is_empty()).unwrap_or(false)
    };
    if let Some(aff) = spec.affinity.as_ref() {
        if aff
            .pod_affinity
            .as_ref()
            .map(|a| has_terms(&a.required_during_scheduling_ignored_during_execution))
            .unwrap_or(false)
        {
            out.push("pod affinity".to_string());
        }
        if aff
            .pod_anti_affinity
            .as_ref()
            .map(|a| has_terms(&a.required_during_scheduling_ignored_during_execution))
            .unwrap_or(false)
        {
            out.push("pod anti-affinity".to_string());
        }
    }
    // Only DoNotSchedule spread is a hard feasibility constraint; ScheduleAnyway is soft. The
    // pending-input builder models supported label selectors against already-running pods, so only
    // unsupported hard spread shapes remain caveats.
    let unmodeled_hard_spread = spec
        .topology_spread_constraints
        .as_ref()
        .map(|v| {
            v.iter()
                .any(|c| c.when_unsatisfiable == "DoNotSchedule" && !modeled_hard_spread(c))
        })
        .unwrap_or(false);
    if unmodeled_hard_spread {
        out.push("topology spread".to_string());
    }
    out
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
    let Some(selector) = c.label_selector.as_ref() else {
        return false;
    };
    crate::collector::label_selector_to_reqs(selector)
        .map(|reqs| !reqs.is_empty())
        .unwrap_or(false)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::scheduler::config::ShadowConfig;
    use k8s_openapi::api::core::v1 as corev1;
    use k8s_openapi::apimachinery::pkg::api::resource::Quantity;
    use k8s_openapi::apimachinery::pkg::apis::meta::v1::{ObjectMeta, Time};
    use std::collections::BTreeMap;
    use std::time::Duration;

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
            namespace_gpu_quotas: std::collections::BTreeMap::new(),
            tenant_share_weights: std::collections::BTreeMap::new(),
            tenant_monthly_budgets_milli: std::collections::BTreeMap::new(),
            queue_weights: std::collections::BTreeMap::new(),
            enable_real_binding: false,
            binding_rollout_mode: crate::scheduler::config::BindingRolloutMode::ObserveOnly,
            binding_kill_switch: false,
            enable_kubernetes_events: false,
            real_binding_dry_run: false,
            binding_canary_mode: crate::scheduler::config::BindingCanaryMode::All,
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

    fn q(map: &[(&str, &str)]) -> BTreeMap<String, Quantity> {
        map.iter()
            .map(|(k, v)| (k.to_string(), Quantity(v.to_string())))
            .collect()
    }

    fn container(
        name: &str,
        requests: Option<BTreeMap<String, Quantity>>,
        limits: Option<BTreeMap<String, Quantity>>,
    ) -> corev1::Container {
        corev1::Container {
            name: name.to_string(),
            resources: Some(corev1::ResourceRequirements {
                requests,
                limits,
                ..Default::default()
            }),
            ..Default::default()
        }
    }

    fn pod(
        scheduler: &str,
        node: Option<&str>,
        phase: Option<&str>,
        containers: Vec<corev1::Container>,
        init: Vec<corev1::Container>,
    ) -> corev1::Pod {
        corev1::Pod {
            metadata: ObjectMeta {
                name: Some("job-0".to_string()),
                namespace: Some("team-a".to_string()),
                uid: Some("uid-123".to_string()),
                ..Default::default()
            },
            spec: Some(corev1::PodSpec {
                scheduler_name: Some(scheduler.to_string()),
                node_name: node.map(|n| n.to_string()),
                containers,
                init_containers: if init.is_empty() { None } else { Some(init) },
                ..Default::default()
            }),
            status: Some(corev1::PodStatus {
                phase: phase.map(|p| p.to_string()),
                ..Default::default()
            }),
        }
    }

    #[test]
    fn classifies_pending_gpu_pod_with_uid() {
        let p = pod(
            "ksolver",
            None,
            Some("Pending"),
            vec![container("main", Some(q(&[("nvidia.com/gpu", "4")])), None)],
            vec![],
        );
        let got = classify(&p, &cfg()).expect("classify");
        assert_eq!(got.uid, "uid-123");
        assert_eq!(got.gpu_request, 4);
    }

    #[test]
    fn classifies_priority_and_annotation_override() {
        let mut p = pod(
            "ksolver",
            None,
            Some("Pending"),
            vec![container("main", Some(q(&[("nvidia.com/gpu", "1")])), None)],
            vec![],
        );
        p.spec.as_mut().unwrap().priority = Some(2500);
        p.spec.as_mut().unwrap().priority_class_name = Some("research-high".to_string());
        let got = classify(&p, &cfg()).expect("classify");
        assert_eq!(got.priority, 3);
        assert_eq!(got.priority_class_name.as_deref(), Some("research-high"));

        p.metadata
            .annotations
            .get_or_insert_with(BTreeMap::new)
            .insert("ksolver.dev/priority".to_string(), "9000".to_string());
        let got = classify(&p, &cfg()).expect("classify");
        assert_eq!(got.priority, 9);
    }

    #[test]
    fn classifies_policy_annotations() {
        let mut p = pod(
            "ksolver",
            None,
            Some("Pending"),
            vec![container("main", Some(q(&[("nvidia.com/gpu", "1")])), None)],
            vec![],
        );
        p.metadata
            .annotations
            .get_or_insert_with(BTreeMap::new)
            .extend([
                ("ksolver.dev/team".to_string(), "research".to_string()),
                ("ksolver.dev/queue".to_string(), "urgent".to_string()),
                ("ksolver.dev/business-value".to_string(), "42".to_string()),
                (
                    "ksolver.dev/deadline".to_string(),
                    "2026-07-06T12:00:00Z".to_string(),
                ),
                ("ksolver.dev/min-gpus".to_string(), "2".to_string()),
                ("ksolver.dev/max-gpus".to_string(), "8".to_string()),
                ("ksolver.dev/preferred-gpus".to_string(), "4".to_string()),
                ("ksolver.dev/flexible".to_string(), "true".to_string()),
                (
                    "ksolver.dev/predicted-runtime-seconds".to_string(),
                    "7200".to_string(),
                ),
                (
                    "ksolver.dev/predicted-peak-vram-gib".to_string(),
                    "40".to_string(),
                ),
            ]);

        let got = classify(&p, &cfg()).expect("classify");
        assert_eq!(got.team.as_deref(), Some("research"));
        assert_eq!(got.queue.as_deref(), Some("urgent"));
        assert_eq!(got.business_value, 42);
        assert_eq!(got.deadline_unix_seconds, 1783339200);
        assert_eq!(got.min_gpus, 2);
        assert_eq!(got.max_gpus, 8);
        assert_eq!(got.preferred_gpus, 4);
        assert!(got.flexible);
        assert_eq!(got.predicted_runtime_seconds, 7200);
        assert_eq!(got.predicted_peak_vram_bytes, 40 * 1024 * 1024 * 1024);
    }

    #[test]
    fn classifies_required_gpu_topology_annotations() {
        let mut p = pod(
            "ksolver",
            None,
            Some("Pending"),
            vec![container("main", Some(q(&[("nvidia.com/gpu", "1")])), None)],
            vec![],
        );
        p.metadata
            .annotations
            .get_or_insert_with(BTreeMap::new)
            .extend([
                (
                    "ksolver.dev/gpu-topology-key".to_string(),
                    "topology.gpu.ksolver.dev/island".to_string(),
                ),
                (
                    "ksolver.dev/gpu-topology-value".to_string(),
                    "island-a".to_string(),
                ),
                (
                    "ksolver.dev/nvlink-domain".to_string(),
                    "rack-7".to_string(),
                ),
            ]);

        let got = classify(&p, &cfg()).expect("classify");

        assert_eq!(
            got.required_gpu_topology,
            vec![
                (
                    "ksolver.dev/nvlink-domain".to_string(),
                    "rack-7".to_string()
                ),
                (
                    "topology.gpu.ksolver.dev/island".to_string(),
                    "island-a".to_string()
                ),
            ]
        );
        assert!(got
            .unmodeled_constraints
            .iter()
            .any(|c| c.contains("GPU topology: requires")));
    }

    #[test]
    fn classifies_queue_wait_from_creation_timestamp() {
        let mut p = pod(
            "ksolver",
            None,
            Some("Pending"),
            vec![container("main", Some(q(&[("nvidia.com/gpu", "1")])), None)],
            vec![],
        );
        p.metadata.creation_timestamp = Some(Time(Utc::now() - chrono::Duration::hours(1)));

        let got = classify(&p, &cfg()).expect("classify");

        assert!(
            (3590..=3610).contains(&got.queue_wait_seconds),
            "queue wait should be about one hour, got {}",
            got.queue_wait_seconds
        );
    }

    #[test]
    fn explicit_predicted_runtime_overrides_training_hint_heuristic() {
        let mut p = pod(
            "ksolver",
            None,
            Some("Pending"),
            vec![container("main", Some(q(&[("nvidia.com/gpu", "4")])), None)],
            vec![],
        );
        p.metadata
            .annotations
            .get_or_insert_with(BTreeMap::new)
            .extend([
                (
                    "ksolver.dev/predicted-runtime-seconds".to_string(),
                    "7200".to_string(),
                ),
                (
                    "ksolver.dev/model-parameters-billions".to_string(),
                    "70".to_string(),
                ),
                ("ksolver.dev/batch-size".to_string(), "256".to_string()),
                (
                    "ksolver.dev/sequence-length".to_string(),
                    "8192".to_string(),
                ),
                ("ksolver.dev/precision".to_string(), "fp32".to_string()),
            ]);

        let got = classify(&p, &cfg()).expect("classify");
        assert_eq!(got.predicted_runtime_seconds, 7200);
    }

    #[test]
    fn estimates_predicted_runtime_from_training_hints_when_explicit_absent() {
        let mut p = pod(
            "ksolver",
            None,
            Some("Pending"),
            vec![container("main", Some(q(&[("nvidia.com/gpu", "4")])), None)],
            vec![],
        );
        p.metadata
            .annotations
            .get_or_insert_with(BTreeMap::new)
            .extend([
                (
                    "ksolver.dev/model-parameters-billions".to_string(),
                    "7".to_string(),
                ),
                ("ksolver.dev/batch-size".to_string(), "64".to_string()),
                (
                    "ksolver.dev/sequence-length".to_string(),
                    "4096".to_string(),
                ),
                ("ksolver.dev/precision".to_string(), "bf16".to_string()),
            ]);

        let got = classify(&p, &cfg()).expect("classify");
        assert_eq!(got.predicted_runtime_seconds, 8400);
        assert!(got.predicted_peak_vram_bytes > 0);
    }

    #[test]
    fn explicit_predicted_peak_vram_bytes_overrides_training_hint_heuristic() {
        let mut p = pod(
            "ksolver",
            None,
            Some("Pending"),
            vec![container("main", Some(q(&[("nvidia.com/gpu", "4")])), None)],
            vec![],
        );
        p.metadata
            .annotations
            .get_or_insert_with(BTreeMap::new)
            .extend([
                (
                    "ksolver.dev/predicted-peak-vram-bytes".to_string(),
                    "123456789".to_string(),
                ),
                (
                    "ksolver.dev/model-parameters-billions".to_string(),
                    "70".to_string(),
                ),
                ("ksolver.dev/batch-size".to_string(), "256".to_string()),
                (
                    "ksolver.dev/sequence-length".to_string(),
                    "8192".to_string(),
                ),
            ]);

        let got = classify(&p, &cfg()).expect("classify");
        assert_eq!(got.predicted_peak_vram_bytes, 123456789);
    }

    #[test]
    fn classifies_dra_pod_without_container_gpu() {
        // A DRA pod requests GPUs via spec.resourceClaims, not container limits — it must still be
        // in scope (gpu=0) and carry the scalar-approximation caveat.
        let mut p = pod(
            "ksolver",
            None,
            Some("Pending"),
            vec![container("m", None, None)],
            vec![],
        );
        p.spec.as_mut().unwrap().resource_claims = Some(vec![corev1::PodResourceClaim {
            name: "gpu".to_string(),
            resource_claim_template_name: Some("gpu-template".to_string()),
            ..Default::default()
        }]);
        let got = classify(&p, &cfg()).expect("DRA pod must be classified");
        assert_eq!(got.gpu_request, 0);
        assert!(got.unmodeled_constraints.iter().any(|c| c.contains("DRA")));
    }

    #[test]
    fn non_gpu_non_dra_pod_is_skipped() {
        let p = pod(
            "ksolver",
            None,
            Some("Pending"),
            vec![container("m", None, None)],
            vec![],
        );
        assert!(classify(&p, &cfg()).is_none());
    }

    #[test]
    fn rejects_other_scheduler() {
        let p = pod(
            "default-scheduler",
            None,
            Some("Pending"),
            vec![container("main", Some(q(&[("nvidia.com/gpu", "4")])), None)],
            vec![],
        );
        assert!(classify(&p, &cfg()).is_none());
    }

    #[test]
    fn rejects_bound_pod() {
        let p = pod(
            "ksolver",
            Some("node-1"),
            Some("Running"),
            vec![container("main", Some(q(&[("nvidia.com/gpu", "4")])), None)],
            vec![],
        );
        assert!(classify(&p, &cfg()).is_none());
    }

    #[test]
    fn rejects_zero_gpu() {
        let p = pod(
            "ksolver",
            None,
            Some("Pending"),
            vec![container("main", Some(q(&[("cpu", "2")])), None)],
            vec![],
        );
        assert!(classify(&p, &cfg()).is_none());
    }

    #[test]
    fn sums_normal_containers() {
        let p = pod(
            "ksolver",
            None,
            Some("Pending"),
            vec![
                container("a", Some(q(&[("nvidia.com/gpu", "1")])), None),
                container("b", Some(q(&[("nvidia.com/gpu", "2")])), None),
            ],
            vec![],
        );
        assert_eq!(effective_gpu_request(&p, &cfg()), 3);
    }

    #[test]
    fn init_container_is_max_not_added() {
        let p = pod(
            "ksolver",
            None,
            Some("Pending"),
            vec![
                container("a", Some(q(&[("nvidia.com/gpu", "1")])), None),
                container("b", Some(q(&[("nvidia.com/gpu", "1")])), None),
            ],
            vec![container("init", Some(q(&[("nvidia.com/gpu", "5")])), None)],
        );
        assert_eq!(effective_gpu_request(&p, &cfg()), 5);
    }

    #[test]
    fn restartable_init_sidecar_gpu_adds_to_app_sum() {
        // A restartable init container (restartPolicy: Always — a native sidecar) runs concurrently
        // with app containers, so its GPU demand ADDS. A plain init-max would undercount it and let
        // the pod overcommit the GPU.
        let mut sidecar = container("gpu-sidecar", Some(q(&[("nvidia.com/gpu", "1")])), None);
        sidecar.restart_policy = Some("Always".to_string());
        let p = pod(
            "ksolver",
            None,
            Some("Pending"),
            vec![container("app", Some(q(&[("nvidia.com/gpu", "2")])), None)],
            vec![sidecar],
        );
        // app(2) + sidecar(1) run together = 3, not max(2,1)=2.
        assert_eq!(effective_gpu_request(&p, &cfg()), 3);
    }

    #[test]
    fn plain_and_restartable_init_containers_mix_correctly() {
        // A big plain init container (max phase) plus a small sidecar: the init peak includes the
        // sidecar that started before it; the app phase adds the sidecar to the app sum.
        let mut sidecar = container("sidecar", Some(q(&[("nvidia.com/gpu", "1")])), None);
        sidecar.restart_policy = Some("Always".to_string());
        let big_init = container("setup", Some(q(&[("nvidia.com/gpu", "4")])), None);
        let p = pod(
            "ksolver",
            None,
            Some("Pending"),
            vec![container("app", Some(q(&[("nvidia.com/gpu", "2")])), None)],
            vec![sidecar, big_init], // sidecar starts first, then the big plain init runs
        );
        // init peak = sidecar(1) + setup(4) = 5; app phase = app(2) + sidecar(1) = 3; max = 5.
        assert_eq!(effective_gpu_request(&p, &cfg()), 5);
    }

    #[test]
    fn falls_back_to_limits_when_no_requests() {
        let p = pod(
            "ksolver",
            None,
            Some("Pending"),
            vec![container("a", None, Some(q(&[("nvidia.com/gpu", "2")])))],
            vec![],
        );
        assert_eq!(effective_gpu_request(&p, &cfg()), 2);
    }

    #[test]
    fn counts_gpu_in_limits_when_requests_hold_only_cpu() {
        // The GPU is declared ONLY in limits while cpu sits in requests. Effective request is
        // per-resource (requests[r] else limits[r]), so the GPU must still count — a per-map
        // fallback would take requests wholesale, see no GPU, and drop the pod from scheduling.
        let p = pod(
            "ksolver",
            None,
            Some("Pending"),
            vec![container(
                "a",
                Some(q(&[("cpu", "2")])),
                Some(q(&[("cpu", "2"), ("nvidia.com/gpu", "1")])),
            )],
            vec![],
        );
        assert_eq!(effective_gpu_request(&p, &cfg()), 1);
        assert!(
            classify(&p, &cfg()).is_some(),
            "limits-only GPU pod must be in scope"
        );
    }

    #[test]
    fn requests_win_over_limits_per_resource_no_double_count() {
        // GPU in both requests and limits: count it once (requests wins), never sum the two.
        let p = pod(
            "ksolver",
            None,
            Some("Pending"),
            vec![container(
                "a",
                Some(q(&[("nvidia.com/gpu", "2")])),
                Some(q(&[("nvidia.com/gpu", "2")])),
            )],
            vec![],
        );
        assert_eq!(effective_gpu_request(&p, &cfg()), 2);
    }

    #[test]
    fn exact_name_match_ignores_gpu_memory() {
        let p = pod(
            "ksolver",
            None,
            Some("Pending"),
            vec![container(
                "a",
                Some(q(&[("nvidia.com/gpu-memory", "8")])),
                None,
            )],
            vec![],
        );
        assert_eq!(effective_gpu_request(&p, &cfg()), 0);
    }

    #[test]
    fn mig_slices_are_counted_and_in_scope() {
        // MIG mixed-strategy slice resources are recognized via the prefix matcher.
        let p = pod(
            "ksolver",
            None,
            Some("Pending"),
            vec![container(
                "a",
                Some(q(&[("nvidia.com/mig-1g.5gb", "2")])),
                None,
            )],
            vec![],
        );
        assert_eq!(effective_gpu_request(&p, &cfg()), 2);
        // classify accepts it (in-scope) with the summed slice count.
        let got = classify(&p, &cfg()).expect("MIG pod should be in scope");
        assert_eq!(got.gpu_request, 2);
    }

    #[test]
    fn mixed_whole_gpu_and_mig_slices_sum() {
        let p = pod(
            "ksolver",
            None,
            Some("Pending"),
            vec![container(
                "a",
                Some(q(&[
                    ("nvidia.com/gpu", "1"),
                    ("nvidia.com/mig-1g.5gb", "1"),
                ])),
                None,
            )],
            vec![],
        );
        assert_eq!(effective_gpu_request(&p, &cfg()), 2);
    }

    #[test]
    fn extracts_gang_key_from_label() {
        let mut p = pod(
            "ksolver",
            None,
            Some("Pending"),
            vec![container("main", Some(q(&[("nvidia.com/gpu", "1")])), None)],
            vec![],
        );
        p.metadata.labels = Some(BTreeMap::from([(
            "scheduling.x-k8s.io/pod-group".to_string(),
            "job-7".to_string(),
        )]));
        let got = classify(&p, &cfg()).expect("classify");
        assert_eq!(got.gang_key.as_deref(), Some("team-a/job-7"));
    }

    #[test]
    fn no_gang_key_when_label_absent() {
        let p = pod(
            "ksolver",
            None,
            Some("Pending"),
            vec![container("main", Some(q(&[("nvidia.com/gpu", "1")])), None)],
            vec![],
        );
        let got = classify(&p, &cfg()).unwrap();
        assert_eq!(got.gang_key, None);
        assert!(!got.colocate);
    }

    #[test]
    fn detects_pod_anti_affinity_and_spread() {
        let mut p = pod(
            "ksolver",
            None,
            Some("Pending"),
            vec![container("main", Some(q(&[("nvidia.com/gpu", "1")])), None)],
            vec![],
        );
        if let Some(spec) = p.spec.as_mut() {
            spec.affinity = Some(corev1::Affinity {
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
            spec.topology_spread_constraints = Some(vec![corev1::TopologySpreadConstraint {
                max_skew: 1,
                topology_key: "zone".to_string(),
                when_unsatisfiable: "DoNotSchedule".to_string(),
                ..Default::default()
            }]);
        }
        let got = classify(&p, &cfg()).expect("classify");
        assert!(got
            .unmodeled_constraints
            .contains(&"pod anti-affinity".to_string()));
        assert!(got
            .unmodeled_constraints
            .contains(&"topology spread".to_string()));
    }

    #[test]
    fn no_caveats_for_plain_pod() {
        let p = pod(
            "ksolver",
            None,
            Some("Pending"),
            vec![container("main", Some(q(&[("nvidia.com/gpu", "1")])), None)],
            vec![],
        );
        assert!(classify(&p, &cfg())
            .unwrap()
            .unmodeled_constraints
            .is_empty());
    }

    #[test]
    fn schedule_anyway_spread_is_not_a_caveat() {
        let mut p = pod(
            "ksolver",
            None,
            Some("Pending"),
            vec![container("main", Some(q(&[("nvidia.com/gpu", "1")])), None)],
            vec![],
        );
        if let Some(spec) = p.spec.as_mut() {
            spec.topology_spread_constraints = Some(vec![corev1::TopologySpreadConstraint {
                max_skew: 1,
                topology_key: "zone".to_string(),
                when_unsatisfiable: "ScheduleAnyway".to_string(),
                ..Default::default()
            }]);
        }
        assert!(classify(&p, &cfg())
            .unwrap()
            .unmodeled_constraints
            .is_empty());
    }

    #[test]
    fn modeled_matchlabels_hard_spread_is_not_a_caveat() {
        use k8s_openapi::apimachinery::pkg::apis::meta::v1::LabelSelector;
        let mut p = pod(
            "ksolver",
            None,
            Some("Pending"),
            vec![container("main", Some(q(&[("nvidia.com/gpu", "1")])), None)],
            vec![],
        );
        if let Some(spec) = p.spec.as_mut() {
            spec.topology_spread_constraints = Some(vec![corev1::TopologySpreadConstraint {
                max_skew: 1,
                topology_key: "topology.kubernetes.io/zone".to_string(),
                when_unsatisfiable: "DoNotSchedule".to_string(),
                label_selector: Some(LabelSelector {
                    match_labels: Some(BTreeMap::from([(
                        "app".to_string(),
                        "trainer".to_string(),
                    )])),
                    ..Default::default()
                }),
                ..Default::default()
            }]);
        }

        let got = classify(&p, &cfg()).expect("classify");

        assert!(!got
            .unmodeled_constraints
            .contains(&"topology spread".to_string()));
    }

    #[test]
    fn hard_spread_with_supported_match_expressions_is_not_a_caveat() {
        use k8s_openapi::apimachinery::pkg::apis::meta::v1::{
            LabelSelector, LabelSelectorRequirement,
        };
        let mut p = pod(
            "ksolver",
            None,
            Some("Pending"),
            vec![container("main", Some(q(&[("nvidia.com/gpu", "1")])), None)],
            vec![],
        );
        if let Some(spec) = p.spec.as_mut() {
            spec.topology_spread_constraints = Some(vec![corev1::TopologySpreadConstraint {
                max_skew: 1,
                topology_key: "topology.kubernetes.io/zone".to_string(),
                when_unsatisfiable: "DoNotSchedule".to_string(),
                label_selector: Some(LabelSelector {
                    match_expressions: Some(vec![LabelSelectorRequirement {
                        key: "app".to_string(),
                        operator: "In".to_string(),
                        values: Some(vec!["trainer".to_string()]),
                    }]),
                    ..Default::default()
                }),
                ..Default::default()
            }]);
        }

        let got = classify(&p, &cfg()).expect("classify");

        assert!(!got
            .unmodeled_constraints
            .contains(&"topology spread".to_string()));
    }

    #[test]
    fn hard_spread_with_advanced_fields_remains_a_caveat() {
        use k8s_openapi::apimachinery::pkg::apis::meta::v1::LabelSelector;

        let mut p = pod(
            "ksolver",
            None,
            Some("Pending"),
            vec![container("main", Some(q(&[("nvidia.com/gpu", "1")])), None)],
            vec![],
        );
        if let Some(spec) = p.spec.as_mut() {
            spec.topology_spread_constraints = Some(vec![corev1::TopologySpreadConstraint {
                max_skew: 1,
                topology_key: "topology.kubernetes.io/zone".to_string(),
                when_unsatisfiable: "DoNotSchedule".to_string(),
                min_domains: Some(2),
                label_selector: Some(LabelSelector {
                    match_labels: Some(BTreeMap::from([(
                        "app".to_string(),
                        "trainer".to_string(),
                    )])),
                    ..Default::default()
                }),
                ..Default::default()
            }]);
        }

        let got = classify(&p, &cfg()).expect("classify");

        assert!(got
            .unmodeled_constraints
            .contains(&"topology spread".to_string()));
    }

    #[test]
    fn hard_spread_with_unsupported_match_expression_remains_a_caveat() {
        use k8s_openapi::apimachinery::pkg::apis::meta::v1::{
            LabelSelector, LabelSelectorRequirement,
        };
        let mut p = pod(
            "ksolver",
            None,
            Some("Pending"),
            vec![container("main", Some(q(&[("nvidia.com/gpu", "1")])), None)],
            vec![],
        );
        if let Some(spec) = p.spec.as_mut() {
            spec.topology_spread_constraints = Some(vec![corev1::TopologySpreadConstraint {
                max_skew: 1,
                topology_key: "topology.kubernetes.io/zone".to_string(),
                when_unsatisfiable: "DoNotSchedule".to_string(),
                label_selector: Some(LabelSelector {
                    match_expressions: Some(vec![LabelSelectorRequirement {
                        key: "app".to_string(),
                        operator: "Gt".to_string(),
                        values: Some(vec!["trainer".to_string()]),
                    }]),
                    ..Default::default()
                }),
                ..Default::default()
            }]);
        }

        let got = classify(&p, &cfg()).expect("classify");

        assert!(got
            .unmodeled_constraints
            .contains(&"topology spread".to_string()));
    }

    fn set_anti_affinity(pod: &mut corev1::Pod, terms: Vec<corev1::PodAffinityTerm>) {
        if let Some(spec) = pod.spec.as_mut() {
            spec.affinity = Some(corev1::Affinity {
                pod_anti_affinity: Some(corev1::PodAntiAffinity {
                    required_during_scheduling_ignored_during_execution: Some(terms),
                    ..Default::default()
                }),
                ..Default::default()
            });
        }
    }

    fn set_affinity(pod: &mut corev1::Pod, terms: Vec<corev1::PodAffinityTerm>) {
        if let Some(spec) = pod.spec.as_mut() {
            spec.affinity = Some(corev1::Affinity {
                pod_affinity: Some(corev1::PodAffinity {
                    required_during_scheduling_ignored_during_execution: Some(terms),
                    ..Default::default()
                }),
                ..Default::default()
            });
        }
    }

    fn aa_term(
        topology_key: &str,
        match_labels: &[(&str, &str)],
        with_expr: bool,
        namespaces: Option<Vec<String>>,
    ) -> corev1::PodAffinityTerm {
        use k8s_openapi::apimachinery::pkg::apis::meta::v1::{
            LabelSelector, LabelSelectorRequirement,
        };
        let ml: BTreeMap<String, String> = match_labels
            .iter()
            .map(|(k, v)| (k.to_string(), v.to_string()))
            .collect();
        corev1::PodAffinityTerm {
            topology_key: topology_key.to_string(),
            namespaces,
            label_selector: Some(LabelSelector {
                match_labels: if ml.is_empty() { None } else { Some(ml) },
                match_expressions: if with_expr {
                    Some(vec![LabelSelectorRequirement {
                        key: "team".into(),
                        operator: "Exists".into(),
                        values: None,
                    }])
                } else {
                    None
                },
            }),
            ..Default::default()
        }
    }

    fn gpu_pending() -> corev1::Pod {
        pod(
            "ksolver",
            None,
            Some("Pending"),
            vec![container("m", Some(q(&[("nvidia.com/gpu", "1")])), None)],
            vec![],
        )
    }

    #[test]
    fn preferred_node_affinity_extracted_with_weight() {
        use k8s_openapi::api::core::v1::{
            NodeAffinity, NodeSelectorRequirement, NodeSelectorTerm, PreferredSchedulingTerm,
        };
        let mut p = gpu_pending();
        p.spec.as_mut().unwrap().affinity = Some(corev1::Affinity {
            node_affinity: Some(NodeAffinity {
                preferred_during_scheduling_ignored_during_execution: Some(vec![
                    PreferredSchedulingTerm {
                        weight: 10,
                        preference: NodeSelectorTerm {
                            match_expressions: Some(vec![NodeSelectorRequirement {
                                key: "zone".into(),
                                operator: "In".into(),
                                values: Some(vec!["a".into()]),
                            }]),
                            ..Default::default()
                        },
                    },
                ]),
                ..Default::default()
            }),
            ..Default::default()
        });
        let got = classify(&p, &cfg()).unwrap();
        assert_eq!(got.preferred_node_affinity.len(), 1);
        let t = &got.preferred_node_affinity[0];
        assert_eq!(t.weight, 10);
        assert_eq!(t.exprs.len(), 1);
        assert_eq!(t.exprs[0].key, "zone");
        assert_eq!(t.exprs[0].values, vec!["a".to_string()]);
    }

    #[test]
    fn preferred_node_affinity_extracts_metadata_name_match_field() {
        use k8s_openapi::api::core::v1::{
            NodeAffinity, NodeSelectorRequirement, NodeSelectorTerm, PreferredSchedulingTerm,
        };
        let mut p = gpu_pending();
        p.spec.as_mut().unwrap().affinity = Some(corev1::Affinity {
            node_affinity: Some(NodeAffinity {
                preferred_during_scheduling_ignored_during_execution: Some(vec![
                    PreferredSchedulingTerm {
                        weight: 20,
                        preference: NodeSelectorTerm {
                            match_fields: Some(vec![NodeSelectorRequirement {
                                key: "metadata.name".into(),
                                operator: "In".into(),
                                values: Some(vec!["node-a".into()]),
                            }]),
                            ..Default::default()
                        },
                    },
                ]),
                ..Default::default()
            }),
            ..Default::default()
        });
        let got = classify(&p, &cfg()).unwrap();
        assert_eq!(got.preferred_node_affinity.len(), 1);
        let t = &got.preferred_node_affinity[0];
        assert_eq!(t.weight, 20);
        assert!(t.exprs.is_empty());
        assert_eq!(t.fields.len(), 1);
        assert_eq!(t.fields[0].key, "metadata.name");
        assert_eq!(t.fields[0].values, vec!["node-a".to_string()]);
    }

    #[test]
    fn preferred_pod_affinity_extracted_both_directions() {
        let term = |val: &str, tk: &str| corev1::WeightedPodAffinityTerm {
            weight: 30,
            pod_affinity_term: corev1::PodAffinityTerm {
                label_selector: Some(
                    k8s_openapi::apimachinery::pkg::apis::meta::v1::LabelSelector {
                        match_labels: Some([("app".to_string(), val.to_string())].into()),
                        ..Default::default()
                    },
                ),
                topology_key: tk.to_string(),
                ..Default::default()
            },
        };
        let spec = corev1::PodSpec {
            affinity: Some(corev1::Affinity {
                pod_affinity: Some(corev1::PodAffinity {
                    preferred_during_scheduling_ignored_during_execution: Some(vec![term(
                        "cache",
                        "kubernetes.io/hostname",
                    )]),
                    ..Default::default()
                }),
                pod_anti_affinity: Some(corev1::PodAntiAffinity {
                    preferred_during_scheduling_ignored_during_execution: Some(vec![term(
                        "noisy",
                        "topology.kubernetes.io/zone",
                    )]),
                    ..Default::default()
                }),
                ..Default::default()
            }),
            ..Default::default()
        };
        let got = modeled_preferred_pod_affinity(&spec);
        assert_eq!(got.len(), 2);
        let aff = got.iter().find(|t| !t.anti).unwrap();
        assert_eq!(aff.weight, 30);
        assert_eq!(aff.topology_key, "kubernetes.io/hostname");
        assert_eq!(aff.selector.reqs.len(), 1);
        let anti = got.iter().find(|t| t.anti).unwrap();
        assert_eq!(anti.topology_key, "topology.kubernetes.io/zone");
        assert!(anti.anti);
    }

    #[test]
    fn required_pod_affinity_extracted_but_still_caveated() {
        let mut p = gpu_pending();
        set_affinity(
            &mut p,
            vec![aa_term(
                "topology.kubernetes.io/zone",
                &[("app", "trainer")],
                false,
                None,
            )],
        );

        let got = classify(&p, &cfg()).unwrap();

        assert_eq!(got.affinity_topology_selectors.len(), 1);
        assert_eq!(
            got.affinity_topology_selectors[0].0,
            "topology.kubernetes.io/zone"
        );
        let req = &got.affinity_topology_selectors[0].1.reqs[0];
        assert_eq!(req.key, "app");
        assert_eq!(req.operator, "In");
        assert_eq!(req.values, vec!["trainer".to_string()]);
        assert!(got
            .unmodeled_constraints
            .contains(&"pod affinity".to_string()));
    }

    #[test]
    fn hostname_matchlabels_extracted_but_still_caveated() {
        let mut p = gpu_pending();
        set_anti_affinity(
            &mut p,
            vec![aa_term(
                "kubernetes.io/hostname",
                &[("app", "trainer")],
                false,
                None,
            )],
        );
        let got = classify(&p, &cfg()).unwrap();
        assert_eq!(got.anti_affinity_host_selectors.len(), 1);
        assert!(got.anti_affinity_topology_selectors.is_empty());
        let req = &got.anti_affinity_host_selectors[0].reqs[0];
        assert_eq!(req.key, "app");
        assert_eq!(req.operator, "In");
        assert_eq!(req.values, vec!["trainer".to_string()]);
        assert!(got
            .unmodeled_constraints
            .contains(&"pod anti-affinity".to_string()));
    }

    #[test]
    fn zone_topology_captured_as_topology_selector() {
        // Phase 12: a zone anti-affinity term is captured into anti_affinity_topology_selectors
        // (not the hostname list), carrying its topologyKey, and is still caveated (best-effort).
        let mut p = gpu_pending();
        set_anti_affinity(
            &mut p,
            vec![aa_term(
                "topology.kubernetes.io/zone",
                &[("app", "trainer")],
                false,
                None,
            )],
        );
        let got = classify(&p, &cfg()).unwrap();
        assert!(got.anti_affinity_host_selectors.is_empty());
        assert_eq!(got.anti_affinity_topology_selectors.len(), 1);
        assert_eq!(
            got.anti_affinity_topology_selectors[0].0,
            "topology.kubernetes.io/zone"
        );
        let treq = &got.anti_affinity_topology_selectors[0].1.reqs[0];
        assert_eq!(treq.key, "app");
        assert_eq!(treq.operator, "In");
        assert_eq!(treq.values, vec!["trainer".to_string()]);
        assert!(got
            .unmodeled_constraints
            .contains(&"pod anti-affinity".to_string()));
    }

    #[test]
    fn matchexpressions_supported_operator_is_modeled() {
        // matchLabels (app=trainer -> In) AND a supported matchExpression (team Exists) are now
        // both modeled into one hostname selector's requirement list.
        let mut p = gpu_pending();
        set_anti_affinity(
            &mut p,
            vec![aa_term(
                "kubernetes.io/hostname",
                &[("app", "trainer")],
                true,
                None,
            )],
        );
        let got = classify(&p, &cfg()).unwrap();
        assert_eq!(got.anti_affinity_host_selectors.len(), 1);
        let sel = &got.anti_affinity_host_selectors[0];
        assert!(sel.reqs.iter().any(|r| r.key == "app"
            && r.operator == "In"
            && r.values == vec!["trainer".to_string()]));
        assert!(sel
            .reqs
            .iter()
            .any(|r| r.key == "team" && r.operator == "Exists"));
    }

    #[test]
    fn matchexpressions_unsupported_operator_not_modeled() {
        use k8s_openapi::apimachinery::pkg::apis::meta::v1::{
            LabelSelector, LabelSelectorRequirement,
        };
        let mut p = gpu_pending();
        // Operator "Gt" is not a valid pod-selector operator => whole term unmodeled.
        let term = corev1::PodAffinityTerm {
            topology_key: "kubernetes.io/hostname".to_string(),
            label_selector: Some(LabelSelector {
                match_labels: None,
                match_expressions: Some(vec![LabelSelectorRequirement {
                    key: "rank".into(),
                    operator: "Gt".into(),
                    values: Some(vec!["3".into()]),
                }]),
            }),
            ..Default::default()
        };
        set_anti_affinity(&mut p, vec![term]);
        let got = classify(&p, &cfg()).unwrap();
        assert!(got.anti_affinity_host_selectors.is_empty());
        assert!(got.anti_affinity_topology_selectors.is_empty());
        // Still disclosed as a caveat.
        assert!(got
            .unmodeled_constraints
            .contains(&"pod anti-affinity".to_string()));
    }

    #[test]
    fn cross_namespace_explicit_list_is_modeled() {
        // F-CNS-1: an explicit `namespaces` list IS now modeled (captured into the selector scope).
        let mut p = gpu_pending();
        set_anti_affinity(
            &mut p,
            vec![aa_term(
                "kubernetes.io/hostname",
                &[("app", "trainer")],
                false,
                Some(vec!["other".into()]),
            )],
        );
        let got = classify(&p, &cfg()).unwrap();
        assert_eq!(got.anti_affinity_host_selectors.len(), 1);
        assert_eq!(
            got.anti_affinity_host_selectors[0].namespaces,
            vec!["other".to_string()]
        );
    }

    #[test]
    fn namespace_selector_supported_is_modeled() {
        use k8s_openapi::apimachinery::pkg::apis::meta::v1::{
            LabelSelector, LabelSelectorRequirement,
        };
        let mut p = gpu_pending();
        // F-CNS-2: a namespaceSelector with a SUPPORTED operator (Exists) IS modeled.
        let mut ml = std::collections::BTreeMap::new();
        ml.insert("app".to_string(), "trainer".to_string());
        let term = corev1::PodAffinityTerm {
            topology_key: "kubernetes.io/hostname".to_string(),
            label_selector: Some(LabelSelector {
                match_labels: Some(ml),
                match_expressions: None,
            }),
            namespace_selector: Some(LabelSelector {
                match_labels: None,
                match_expressions: Some(vec![LabelSelectorRequirement {
                    key: "team".into(),
                    operator: "Exists".into(),
                    values: None,
                }]),
            }),
            ..Default::default()
        };
        set_anti_affinity(&mut p, vec![term]);
        let got = classify(&p, &cfg()).unwrap();
        assert_eq!(got.anti_affinity_host_selectors.len(), 1);
        let ns_sel = got.anti_affinity_host_selectors[0]
            .namespace_selector
            .as_ref()
            .expect("namespace_selector captured");
        assert!(ns_sel
            .iter()
            .any(|r| r.key == "team" && r.operator == "Exists"));
    }

    #[test]
    fn namespace_selector_unsupported_operator_not_modeled() {
        use k8s_openapi::apimachinery::pkg::apis::meta::v1::{
            LabelSelector, LabelSelectorRequirement,
        };
        let mut p = gpu_pending();
        // An unsupported namespaceSelector operator (Gt) ⇒ whole term unmodeled + caveated.
        let mut ml = std::collections::BTreeMap::new();
        ml.insert("app".to_string(), "trainer".to_string());
        let term = corev1::PodAffinityTerm {
            topology_key: "kubernetes.io/hostname".to_string(),
            label_selector: Some(LabelSelector {
                match_labels: Some(ml),
                match_expressions: None,
            }),
            namespace_selector: Some(LabelSelector {
                match_labels: None,
                match_expressions: Some(vec![LabelSelectorRequirement {
                    key: "rank".into(),
                    operator: "Gt".into(),
                    values: Some(vec!["3".into()]),
                }]),
            }),
            ..Default::default()
        };
        set_anti_affinity(&mut p, vec![term]);
        let got = classify(&p, &cfg()).unwrap();
        assert!(got.anti_affinity_host_selectors.is_empty());
        assert!(got
            .unmodeled_constraints
            .contains(&"pod anti-affinity".to_string()));
    }

    #[test]
    fn colocate_label_true_sets_colocate() {
        let mut p = pod(
            "ksolver",
            None,
            Some("Pending"),
            vec![container("main", Some(q(&[("nvidia.com/gpu", "1")])), None)],
            vec![],
        );
        p.metadata.labels = Some(BTreeMap::from([
            (
                "scheduling.x-k8s.io/pod-group".to_string(),
                "job".to_string(),
            ),
            (
                "scheduling.x-k8s.io/gang-colocate".to_string(),
                "true".to_string(),
            ),
        ]));
        assert!(classify(&p, &cfg()).unwrap().colocate);
    }
}
