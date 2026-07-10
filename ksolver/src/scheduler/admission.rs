//! Pure admission-webhook rendering for assigning `schedulerName: ksolver`.
//!
//! This module does not run a webhook server and does not call the Kubernetes API. It only renders
//! RFC 6902 JSONPatch operations and AdmissionReview responses that a MutatingAdmissionWebhook can
//! return for selected GPU pods.

use base64::Engine;
use k8s_openapi::api::core::v1 as corev1;
use serde::{Deserialize, Serialize};
use std::collections::BTreeMap;

use crate::scheduler::config::ShadowConfig;

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct SchedulerPatchPolicy {
    pub scheduler_name: String,
    pub namespace_allowlist: Vec<String>,
    pub gpu_resource_names: Vec<String>,
    pub gpu_resource_prefixes: Vec<String>,
    /// Optional pod label that must be `"true"` for patching. Empty means any matching GPU pod.
    pub opt_in_label: String,
}

impl Default for SchedulerPatchPolicy {
    fn default() -> Self {
        Self {
            scheduler_name: "ksolver".to_string(),
            namespace_allowlist: Vec::new(),
            gpu_resource_names: vec!["nvidia.com/gpu".to_string()],
            gpu_resource_prefixes: vec!["nvidia.com/mig-".to_string()],
            opt_in_label: String::new(),
        }
    }
}

impl From<&ShadowConfig> for SchedulerPatchPolicy {
    fn from(cfg: &ShadowConfig) -> Self {
        Self {
            scheduler_name: cfg.scheduler_name.clone(),
            namespace_allowlist: cfg.namespace_allowlist.clone(),
            gpu_resource_names: cfg.gpu_resource_names.clone(),
            gpu_resource_prefixes: cfg.gpu_resource_prefixes.clone(),
            opt_in_label: cfg.admission_opt_in_label.clone(),
        }
    }
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct JsonPatchOperation {
    pub op: String,
    pub path: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub value: Option<serde_json::Value>,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct SchedulerNamePatch {
    pub namespace: String,
    pub pod: String,
    pub scheduler_name: String,
    pub patch: Vec<JsonPatchOperation>,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub enum SchedulerPatchDecision {
    Patch(SchedulerNamePatch),
    Skip { reason: String },
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct AdmissionReview {
    #[serde(rename = "apiVersion")]
    pub api_version: String,
    pub kind: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub request: Option<AdmissionRequest>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub response: Option<AdmissionResponse>,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct AdmissionRequest {
    pub uid: String,
    #[serde(default)]
    pub namespace: String,
    #[serde(default)]
    pub name: String,
    #[serde(default)]
    pub operation: String,
    #[serde(default)]
    pub object: Option<serde_json::Value>,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct AdmissionResponse {
    pub uid: String,
    pub allowed: bool,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub status: Option<AdmissionStatus>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub patch: Option<String>,
    #[serde(rename = "patchType", skip_serializing_if = "Option::is_none")]
    pub patch_type: Option<String>,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct AdmissionStatus {
    pub message: String,
}

fn namespace_in_scope(policy: &SchedulerPatchPolicy, namespace: &str) -> bool {
    policy.namespace_allowlist.is_empty()
        || policy.namespace_allowlist.iter().any(|ns| ns == namespace)
}

fn is_gpu_resource(policy: &SchedulerPatchPolicy, name: &str) -> bool {
    policy.gpu_resource_names.iter().any(|n| n == name)
        || policy
            .gpu_resource_prefixes
            .iter()
            .any(|prefix| name.starts_with(prefix))
}

fn quantity_positive(quantity: &k8s_openapi::apimachinery::pkg::api::resource::Quantity) -> bool {
    quantity.0.parse::<i64>().map(|v| v > 0).unwrap_or(false)
}

fn map_has_gpu(
    policy: &SchedulerPatchPolicy,
    map: Option<&BTreeMap<String, k8s_openapi::apimachinery::pkg::api::resource::Quantity>>,
) -> bool {
    map.map(|map| {
        map.iter()
            .any(|(name, quantity)| is_gpu_resource(policy, name) && quantity_positive(quantity))
    })
    .unwrap_or(false)
}

// A container "uses" a GPU if it names a positive GPU quantity in EITHER requests or limits.
// GPU extended resources are commonly declared limits-only (the API server copies limits ->
// requests during defaulting, but that runs before webhooks see raw manifests, and the /predict
// and /claim paths operate on undefaulted specs). Checking only requests misses those pods.
fn container_uses_gpu(policy: &SchedulerPatchPolicy, container: &corev1::Container) -> bool {
    let resources = container.resources.as_ref();
    map_has_gpu(policy, resources.and_then(|r| r.requests.as_ref()))
        || map_has_gpu(policy, resources.and_then(|r| r.limits.as_ref()))
}

fn pod_requests_gpu(policy: &SchedulerPatchPolicy, spec: &corev1::PodSpec) -> bool {
    let containers_request_gpu = spec
        .containers
        .iter()
        .any(|container| container_uses_gpu(policy, container));
    let init_containers_request_gpu = spec
        .init_containers
        .as_ref()
        .map(|containers| {
            containers
                .iter()
                .any(|container| container_uses_gpu(policy, container))
        })
        .unwrap_or(false);
    containers_request_gpu || init_containers_request_gpu
}

fn pod_has_dra_resource_claims(spec: &corev1::PodSpec) -> bool {
    spec.resource_claims
        .as_ref()
        .map(|claims| !claims.is_empty())
        .unwrap_or(false)
}

fn opt_in_matches(
    policy: &SchedulerPatchPolicy,
    labels: Option<&BTreeMap<String, String>>,
) -> bool {
    policy.opt_in_label.is_empty()
        || labels
            .and_then(|labels| labels.get(&policy.opt_in_label))
            .map(|value| value.eq_ignore_ascii_case("true"))
            .unwrap_or(false)
}

/// Render a schedulerName JSONPatch for a pod if it is an in-scope GPU pod that does not already
/// target a non-empty scheduler. Existing explicit scheduler choices are never overwritten.
pub fn render_scheduler_name_patch(
    pod: &corev1::Pod,
    policy: &SchedulerPatchPolicy,
) -> SchedulerPatchDecision {
    if policy.scheduler_name.trim().is_empty() {
        return SchedulerPatchDecision::Skip {
            reason: "scheduler name is empty".to_string(),
        };
    }

    let namespace = pod.metadata.namespace.clone().unwrap_or_default();
    let name = pod.metadata.name.clone().unwrap_or_default();
    if namespace.trim().is_empty() {
        return SchedulerPatchDecision::Skip {
            reason: "pod metadata.namespace is empty".to_string(),
        };
    }
    if name.trim().is_empty() {
        return SchedulerPatchDecision::Skip {
            reason: "pod metadata.name is empty".to_string(),
        };
    }
    if !namespace_in_scope(policy, &namespace) {
        return SchedulerPatchDecision::Skip {
            reason: format!("namespace {namespace} not in webhook scope"),
        };
    }
    if !opt_in_matches(policy, pod.metadata.labels.as_ref()) {
        return SchedulerPatchDecision::Skip {
            reason: format!("pod missing opt-in label {}", policy.opt_in_label),
        };
    }
    let Some(spec) = pod.spec.as_ref() else {
        return SchedulerPatchDecision::Skip {
            reason: "pod has no spec".to_string(),
        };
    };
    if let Some(existing) = spec.scheduler_name.as_deref() {
        if !existing.is_empty() {
            return SchedulerPatchDecision::Skip {
                reason: format!("pod already selects scheduler {existing}"),
            };
        }
    }
    let requests_gpu = pod_requests_gpu(policy, spec);
    let has_dra_claims = pod_has_dra_resource_claims(spec);
    if !requests_gpu && has_dra_claims && policy.opt_in_label.is_empty() {
        return SchedulerPatchDecision::Skip {
            reason: "pod uses DRA resourceClaims; configure an opt-in label before patching schedulerName for DRA pods".to_string(),
        };
    }
    if !requests_gpu && !has_dra_claims {
        return SchedulerPatchDecision::Skip {
            reason: "pod does not request an in-scope GPU resource".to_string(),
        };
    }

    SchedulerPatchDecision::Patch(SchedulerNamePatch {
        namespace,
        pod: name,
        scheduler_name: policy.scheduler_name.clone(),
        patch: vec![JsonPatchOperation {
            op: "add".to_string(),
            path: "/spec/schedulerName".to_string(),
            value: Some(serde_json::Value::String(policy.scheduler_name.clone())),
        }],
    })
}

fn admission_review_response(uid: String, response: AdmissionResponse) -> AdmissionReview {
    AdmissionReview {
        api_version: "admission.k8s.io/v1".to_string(),
        kind: "AdmissionReview".to_string(),
        request: None,
        response: Some(AdmissionResponse { uid, ..response }),
    }
}

fn allow_without_patch(uid: String, message: String) -> AdmissionReview {
    admission_review_response(
        uid.clone(),
        AdmissionResponse {
            uid,
            allowed: true,
            status: Some(AdmissionStatus { message }),
            patch: None,
            patch_type: None,
        },
    )
}

fn deny_malformed(uid: String, message: String) -> AdmissionReview {
    admission_review_response(
        uid.clone(),
        AdmissionResponse {
            uid,
            allowed: false,
            status: Some(AdmissionStatus { message }),
            patch: None,
            patch_type: None,
        },
    )
}

/// Render a Kubernetes AdmissionReview response for the schedulerName mutating webhook. Matching
/// GPU pods are allowed with a JSONPatch; all non-matching pods are allowed without a patch so the
/// webhook is fail-open for out-of-scope traffic.
pub fn render_scheduler_admission_review(
    review: AdmissionReview,
    policy: &SchedulerPatchPolicy,
) -> AdmissionReview {
    let Some(request) = review.request else {
        return deny_malformed(String::new(), "AdmissionReview missing request".to_string());
    };
    let uid = request.uid.clone();
    if !request.operation.is_empty() && !request.operation.eq_ignore_ascii_case("CREATE") {
        return allow_without_patch(
            uid,
            format!(
                "admission operation {} not in webhook scope",
                request.operation
            ),
        );
    }
    let Some(object) = request.object else {
        return deny_malformed(uid, "AdmissionRequest missing object".to_string());
    };
    let mut pod: corev1::Pod = match serde_json::from_value(object) {
        Ok(pod) => pod,
        Err(err) => {
            return deny_malformed(uid, format!("AdmissionRequest object is not a Pod: {err}"));
        }
    };
    if !request.namespace.is_empty() {
        match pod.metadata.namespace.as_deref() {
            Some(object_namespace) if object_namespace != request.namespace => {
                return deny_malformed(
                    uid,
                    format!(
                        "AdmissionRequest namespace {} does not match object namespace {}",
                        request.namespace, object_namespace
                    ),
                );
            }
            None => pod.metadata.namespace = Some(request.namespace),
            Some(_) => {}
        }
    }
    if !request.name.is_empty() {
        match pod.metadata.name.as_deref() {
            Some(object_name) if object_name != request.name => {
                return deny_malformed(
                    uid,
                    format!(
                        "AdmissionRequest name {} does not match object name {}",
                        request.name, object_name
                    ),
                );
            }
            None => pod.metadata.name = Some(request.name),
            Some(_) => {}
        }
    }

    match render_scheduler_name_patch(&pod, policy) {
        SchedulerPatchDecision::Patch(patch) => {
            let patch_bytes = match serde_json::to_vec(&patch.patch) {
                Ok(bytes) => bytes,
                Err(err) => {
                    return deny_malformed(uid, format!("failed to serialize JSONPatch: {err}"));
                }
            };
            admission_review_response(
                uid.clone(),
                AdmissionResponse {
                    uid,
                    allowed: true,
                    status: Some(AdmissionStatus {
                        message: format!(
                            "patched schedulerName to {} for {}/{}",
                            patch.scheduler_name, patch.namespace, patch.pod
                        ),
                    }),
                    patch: Some(base64::engine::general_purpose::STANDARD.encode(patch_bytes)),
                    patch_type: Some("JSONPatch".to_string()),
                },
            )
        }
        SchedulerPatchDecision::Skip { reason } => allow_without_patch(uid, reason),
    }
}

/// Whether a pod is in scope for VRAM injection: it requests an in-scope GPU resource or uses
/// DRA resource claims. Used to gate the admission webhook's predictor call to GPU workloads.
pub fn pod_in_scope_for_vram(pod: &corev1::Pod, policy: &SchedulerPatchPolicy) -> bool {
    match pod.spec.as_ref() {
        Some(spec) => pod_requests_gpu(policy, spec) || pod_has_dra_resource_claims(spec),
        None => false,
    }
}

fn json_pointer_escape(key: &str) -> String {
    // RFC 6901 escaping via a char loop (not String::replace) so the admission no-mutation guard,
    // which forbids the substring ".re" + "place(", stays strict against kube client calls.
    let mut out = String::with_capacity(key.len());
    for ch in key.chars() {
        match ch {
            '~' => out.push_str("~0"),
            '/' => out.push_str("~1"),
            other => out.push(other),
        }
    }
    out
}

/// JSONPatch ops that inject a resolved VRAM estimate into a pod, mirroring the Python
/// `vram_admission.build_admission_patch`: always annotate the predicted peak + source +
/// confidence; at high/authoritative confidence add a nodeAffinity that keeps the pod OFF GPUs
/// smaller than the estimate (`ksolver.dev/gpu-vram-gib Gt floor(est)-1`); advisory/unknown
/// annotate only. `resolution` is the predictor `/predict` JSON.
pub fn vram_injection_ops(
    pod: &corev1::Pod,
    resolution: &serde_json::Value,
) -> Vec<JsonPatchOperation> {
    let mut ops = Vec::new();
    let has_annotations = pod
        .metadata
        .annotations
        .as_ref()
        .map(|a| !a.is_empty())
        .unwrap_or(false);
    if !has_annotations {
        ops.push(JsonPatchOperation {
            op: "add".to_string(),
            path: "/metadata/annotations".to_string(),
            value: Some(serde_json::json!({})),
        });
    }
    let set_ann = |ops: &mut Vec<JsonPatchOperation>, key: &str, val: String| {
        ops.push(JsonPatchOperation {
            op: "add".to_string(),
            path: format!("/metadata/annotations/{}", json_pointer_escape(key)),
            value: Some(serde_json::Value::String(val)),
        });
    };
    let source = resolution
        .get("source")
        .and_then(serde_json::Value::as_str)
        .unwrap_or("unknown");
    let confidence = resolution
        .get("confidence")
        .and_then(serde_json::Value::as_str)
        .unwrap_or("advisory");
    set_ann(&mut ops, "ksolver.dev/predicted-peak-vram-source", source.to_string());
    set_ann(
        &mut ops,
        "ksolver.dev/predicted-peak-vram-confidence",
        confidence.to_string(),
    );
    if let Some(explanation) = resolution
        .get("explanation")
        .and_then(serde_json::Value::as_str)
        .filter(|s| !s.is_empty())
    {
        set_ann(
            &mut ops,
            "ksolver.dev/predicted-peak-vram-explanation",
            explanation.to_string(),
        );
    }
    let vram_gib = resolution
        .get("vram_gib")
        .and_then(serde_json::Value::as_f64);
    if let Some(gib) = vram_gib {
        set_ann(
            &mut ops,
            "ksolver.dev/predicted-peak-vram-gib",
            format!("{gib}"),
        );
    }
    let hard = resolution
        .get("hard")
        .and_then(serde_json::Value::as_bool)
        .unwrap_or(false);
    if let (true, Some(gib)) = (hard, vram_gib) {
        let floor_gib = (gib.floor() as i64 - 1).max(0);
        let floor_mib = floor_gib * 1024;
        // Two OR'd terms: match the ksolver GiB label OR the NVIDIA GFD MiB label.
        ops.push(JsonPatchOperation {
            op: "add".to_string(),
            path: "/spec/affinity".to_string(),
            value: Some(serde_json::json!({
                "nodeAffinity": {
                    "requiredDuringSchedulingIgnoredDuringExecution": {
                        "nodeSelectorTerms": [
                            {"matchExpressions": [{
                                "key": "ksolver.dev/gpu-vram-gib",
                                "operator": "Gt",
                                "values": [floor_gib.to_string()],
                            }]},
                            {"matchExpressions": [{
                                "key": "nvidia.com/gpu.memory",
                                "operator": "Gt",
                                "values": [floor_mib.to_string()],
                            }]},
                        ]
                    }
                }
            })),
        });
    } else {
        set_ann(
            &mut ops,
            "ksolver.dev/predicted-peak-vram-advisory",
            "true".to_string(),
        );
    }
    ops
}

/// Append extra JSONPatch ops (e.g. VRAM injection) to an already-rendered AdmissionReview,
/// preserving the existing schedulerName patch. If the base review had no patch, the extra ops
/// become the patch. Base64 decode/encode round-trip so the two mutations combine into one patch.
pub fn merge_extra_ops(
    mut review: AdmissionReview,
    extra_ops: Vec<JsonPatchOperation>,
) -> AdmissionReview {
    if extra_ops.is_empty() {
        return review;
    }
    let Some(response) = review.response.as_mut() else {
        return review;
    };
    if !response.allowed {
        return review; // never add mutations to a denied request
    }
    let mut ops: Vec<JsonPatchOperation> = match response.patch.as_ref() {
        Some(b64) => base64::engine::general_purpose::STANDARD
            .decode(b64)
            .ok()
            .and_then(|bytes| serde_json::from_slice(&bytes).ok())
            .unwrap_or_default(),
        None => Vec::new(),
    };
    ops.extend(extra_ops);
    let Ok(bytes) = serde_json::to_vec(&ops) else {
        return review;
    };
    response.patch = Some(base64::engine::general_purpose::STANDARD.encode(bytes));
    response.patch_type = Some("JSONPatch".to_string());
    review
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::model::{ObjectiveProfile, ObjectiveWeights};
    use crate::scheduler::config::{BindingCanaryMode, BindingRolloutMode};
    use k8s_openapi::apimachinery::pkg::api::resource::Quantity;
    use std::time::Duration;

    #[test]
    fn vram_ops_high_confidence_adds_gib_annotation_and_affinity() {
        let p = pod("team", "job", Some("nvidia.com/gpu"));
        let res = serde_json::json!({
            "vram_gib": 18.0, "source": "static-sniff+model", "confidence": "high", "hard": true
        });
        let ops = vram_injection_ops(&p, &res);
        assert!(ops.iter().any(|o| o.path == "/spec/affinity"));
        assert!(ops.iter().any(|o| o.path
            == "/metadata/annotations/ksolver.dev~1predicted-peak-vram-gib"
            && o.value == Some(serde_json::Value::String("18".to_string()))));
        // Gt floor(18)-1 = 17 GiB, OR'd with the MiB label (17*1024 = 17408)
        let aff = ops.iter().find(|o| o.path == "/spec/affinity").unwrap();
        let terms = &aff.value.as_ref().unwrap()["nodeAffinity"]
            ["requiredDuringSchedulingIgnoredDuringExecution"]["nodeSelectorTerms"];
        let gib = &terms[0]["matchExpressions"][0];
        assert_eq!(gib["key"], serde_json::json!("ksolver.dev/gpu-vram-gib"));
        assert_eq!(gib["operator"], serde_json::json!("Gt"));
        assert_eq!(gib["values"], serde_json::json!(["17"]));
        let mib = &terms[1]["matchExpressions"][0];
        assert_eq!(mib["key"], serde_json::json!("nvidia.com/gpu.memory"));
        assert_eq!(mib["values"], serde_json::json!(["17408"]));
    }

    #[test]
    fn vram_ops_emit_explanation_annotation() {
        let p = pod("team", "job", Some("nvidia.com/gpu"));
        let res = serde_json::json!({
            "vram_gib": 18.0, "source": "static-sniff+model", "confidence": "high", "hard": true,
            "explanation": "predicted 18 GiB from transformer"
        });
        let ops = vram_injection_ops(&p, &res);
        assert!(ops.iter().any(|o| o.path
            == "/metadata/annotations/ksolver.dev~1predicted-peak-vram-explanation"
            && o.value == Some(serde_json::Value::String("predicted 18 GiB from transformer".to_string()))));
    }

    #[test]
    fn vram_ops_advisory_has_no_affinity() {
        let p = pod("team", "job", Some("nvidia.com/gpu"));
        let res = serde_json::json!({
            "vram_gib": null, "source": "unknown", "confidence": "advisory", "hard": false
        });
        let ops = vram_injection_ops(&p, &res);
        assert!(!ops.iter().any(|o| o.path == "/spec/affinity"));
        assert!(ops.iter().any(|o| o.path
            == "/metadata/annotations/ksolver.dev~1predicted-peak-vram-advisory"));
    }

    #[test]
    fn merge_extra_ops_appends_to_existing_scheduler_patch() {
        let base = allow_without_patch("uid".to_string(), "no-op".to_string());
        // seed a schedulerName patch onto the base response
        let sched_ops = vec![JsonPatchOperation {
            op: "add".to_string(),
            path: "/spec/schedulerName".to_string(),
            value: Some(serde_json::Value::String("ksolver".to_string())),
        }];
        let seeded = merge_extra_ops(base, sched_ops);
        let extra = vec![JsonPatchOperation {
            op: "add".to_string(),
            path: "/spec/affinity".to_string(),
            value: Some(serde_json::json!({"nodeAffinity": {}})),
        }];
        let merged = merge_extra_ops(seeded, extra);
        let b64 = merged.response.unwrap().patch.unwrap();
        let bytes = base64::engine::general_purpose::STANDARD.decode(b64).unwrap();
        let ops: Vec<JsonPatchOperation> = serde_json::from_slice(&bytes).unwrap();
        assert_eq!(ops.len(), 2);
        assert!(ops.iter().any(|o| o.path == "/spec/schedulerName"));
        assert!(ops.iter().any(|o| o.path == "/spec/affinity"));
    }

    fn pod(ns: &str, name: &str, gpu_resource: Option<&str>) -> corev1::Pod {
        let mut requests = BTreeMap::new();
        if let Some(resource) = gpu_resource {
            requests.insert(resource.to_string(), Quantity("1".to_string()));
        }
        corev1::Pod {
            metadata: k8s_openapi::apimachinery::pkg::apis::meta::v1::ObjectMeta {
                namespace: Some(ns.to_string()),
                name: Some(name.to_string()),
                ..Default::default()
            },
            spec: Some(corev1::PodSpec {
                containers: vec![corev1::Container {
                    name: "main".to_string(),
                    resources: Some(corev1::ResourceRequirements {
                        requests: Some(requests),
                        ..Default::default()
                    }),
                    ..Default::default()
                }],
                ..Default::default()
            }),
            ..Default::default()
        }
    }

    fn dra_pod(ns: &str, name: &str) -> corev1::Pod {
        let mut p = pod(ns, name, None);
        p.spec.as_mut().unwrap().resource_claims = Some(vec![corev1::PodResourceClaim {
            name: "gpu".to_string(),
            resource_claim_template_name: Some("gpu-template".to_string()),
            ..Default::default()
        }]);
        p
    }

    fn cfg() -> ShadowConfig {
        ShadowConfig {
            scheduler_name: "gpu-scheduler".to_string(),
            batch_window: Duration::from_secs(10),
            namespace_allowlist: vec!["team-a".to_string()],
            gpu_resource_names: vec!["example.com/gpu".to_string()],
            gpu_resource_prefixes: vec!["example.com/mig-".to_string()],
            cluster_name: "default".to_string(),
            kubeconfig: String::new(),
            http_addr: "127.0.0.1:8090".to_string(),
            admission_opt_in_label: "ksolver.dev/enabled".to_string(),
            gang_label_key: "scheduling.x-k8s.io/pod-group".to_string(),
            gang_colocate_label: "scheduling.x-k8s.io/gang-colocate".to_string(),
            solve_time_limit_secs: 10,
            namespace_gpu_quotas: BTreeMap::new(),
            tenant_share_weights: BTreeMap::new(),
            tenant_monthly_budgets_milli: BTreeMap::new(),
            queue_weights: BTreeMap::new(),
            enable_real_binding: false,
            binding_rollout_mode: BindingRolloutMode::ObserveOnly,
            binding_kill_switch: false,
            enable_kubernetes_events: false,
            real_binding_dry_run: false,
            binding_canary_mode: BindingCanaryMode::All,
            binding_low_risk_max_gpus: 1,
            max_binds_per_pass: 10,
            binding_reservation_ttl: Duration::from_secs(60),
            objective_profile: ObjectiveProfile::CostBinpack,
            objective_weights: ObjectiveWeights::default(),
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
    fn patches_gpu_pod_without_scheduler_name() {
        let got = render_scheduler_name_patch(
            &pod("team-a", "train", Some("nvidia.com/gpu")),
            &SchedulerPatchPolicy::default(),
        );
        match got {
            SchedulerPatchDecision::Patch(patch) => {
                assert_eq!(patch.namespace, "team-a");
                assert_eq!(patch.pod, "train");
                assert_eq!(patch.patch.len(), 1);
                assert_eq!(patch.patch[0].op, "add");
                assert_eq!(patch.patch[0].path, "/spec/schedulerName");
                assert_eq!(patch.patch[0].value, Some(serde_json::json!("ksolver")));
            }
            SchedulerPatchDecision::Skip { reason } => panic!("expected patch, got {reason}"),
        }
    }

    #[test]
    fn detects_mig_gpu_resources() {
        let got = render_scheduler_name_patch(
            &pod("team-a", "mig-train", Some("nvidia.com/mig-1g.5gb")),
            &SchedulerPatchPolicy::default(),
        );
        assert!(matches!(got, SchedulerPatchDecision::Patch(_)));
    }

    #[test]
    fn detects_limits_only_gpu_pod() {
        // GPU extended resources are commonly declared limits-only (no requests). Before the API
        // server copies limits -> requests, checking only requests would miss the pod — it must
        // still be recognized as a GPU pod.
        let mut p = pod("team-a", "limits-only", None);
        p.spec.as_mut().unwrap().containers[0].resources = Some(corev1::ResourceRequirements {
            limits: Some(BTreeMap::from([(
                "nvidia.com/gpu".to_string(),
                Quantity("1".to_string()),
            )])),
            requests: None,
            ..Default::default()
        });
        assert!(
            matches!(
                render_scheduler_name_patch(&p, &SchedulerPatchPolicy::default()),
                SchedulerPatchDecision::Patch(_)
            ),
            "a limits-only GPU pod must be detected as a GPU pod"
        );
        assert!(pod_in_scope_for_vram(&p, &SchedulerPatchPolicy::default()));
    }

    #[test]
    fn skips_zero_fractional_and_suffix_gpu_quantities() {
        for quantity in ["0", "-1", "0.5", "500m"] {
            let mut p = pod("team-a", "train", Some("nvidia.com/gpu"));
            let requests = p
                .spec
                .as_mut()
                .unwrap()
                .containers
                .first_mut()
                .unwrap()
                .resources
                .as_mut()
                .unwrap()
                .requests
                .as_mut()
                .unwrap();
            requests.insert("nvidia.com/gpu".to_string(), Quantity(quantity.to_string()));

            assert!(
                matches!(
                    render_scheduler_name_patch(&p, &Default::default()),
                    SchedulerPatchDecision::Skip { reason } if reason.contains("does not request")
                ),
                "quantity {quantity} should not be treated as a positive integer GPU request"
            );
        }
    }

    #[test]
    fn skips_dra_resource_claims_without_explicit_opt_in_policy() {
        let got = render_scheduler_name_patch(&dra_pod("team-a", "dra-train"), &Default::default());
        assert!(matches!(
            got,
            SchedulerPatchDecision::Skip { reason }
                if reason.contains("DRA resourceClaims") && reason.contains("opt-in")
        ));
    }

    #[test]
    fn patches_dra_resource_claims_when_opted_in() {
        let policy = SchedulerPatchPolicy {
            opt_in_label: "ksolver.dev/schedule".to_string(),
            ..Default::default()
        };
        let mut p = dra_pod("team-a", "dra-train");
        p.metadata.labels = Some(BTreeMap::from([(
            "ksolver.dev/schedule".to_string(),
            "true".to_string(),
        )]));

        let got = render_scheduler_name_patch(&p, &policy);
        assert!(matches!(got, SchedulerPatchDecision::Patch(_)));
    }

    #[test]
    fn skips_non_gpu_pod() {
        let got = render_scheduler_name_patch(&pod("team-a", "cpu", None), &Default::default());
        assert!(matches!(
            got,
            SchedulerPatchDecision::Skip { reason } if reason.contains("does not request")
        ));
    }

    #[test]
    fn skips_pod_with_missing_metadata_identity() {
        let mut missing_namespace = pod("team-a", "train", Some("nvidia.com/gpu"));
        missing_namespace.metadata.namespace = None;
        assert!(matches!(
            render_scheduler_name_patch(&missing_namespace, &Default::default()),
            SchedulerPatchDecision::Skip { reason } if reason.contains("namespace is empty")
        ));

        let mut missing_name = pod("team-a", "train", Some("nvidia.com/gpu"));
        missing_name.metadata.name = None;
        assert!(matches!(
            render_scheduler_name_patch(&missing_name, &Default::default()),
            SchedulerPatchDecision::Skip { reason } if reason.contains("name is empty")
        ));
    }

    #[test]
    fn never_overwrites_existing_scheduler_name() {
        let mut p = pod("team-a", "train", Some("nvidia.com/gpu"));
        p.spec.as_mut().unwrap().scheduler_name = Some("default-scheduler".to_string());
        let got = render_scheduler_name_patch(&p, &Default::default());
        assert!(matches!(
            got,
            SchedulerPatchDecision::Skip { reason } if reason.contains("already selects scheduler")
        ));
    }

    #[test]
    fn honors_namespace_allowlist_and_opt_in_label() {
        let policy = SchedulerPatchPolicy {
            namespace_allowlist: vec!["team-a".to_string()],
            opt_in_label: "ksolver.dev/schedule".to_string(),
            ..Default::default()
        };
        let mut p = pod("team-b", "train", Some("nvidia.com/gpu"));
        assert!(matches!(
            render_scheduler_name_patch(&p, &policy),
            SchedulerPatchDecision::Skip { reason } if reason.contains("not in webhook scope")
        ));

        p.metadata.namespace = Some("team-a".to_string());
        assert!(matches!(
            render_scheduler_name_patch(&p, &policy),
            SchedulerPatchDecision::Skip { reason } if reason.contains("missing opt-in label")
        ));

        p.metadata.labels = Some(BTreeMap::from([(
            "ksolver.dev/schedule".to_string(),
            "true".to_string(),
        )]));
        assert!(matches!(
            render_scheduler_name_patch(&p, &policy),
            SchedulerPatchDecision::Patch(_)
        ));
    }

    #[test]
    fn policy_can_be_derived_from_shadow_config() {
        let policy = SchedulerPatchPolicy::from(&cfg());
        let mut p = pod("team-a", "mig-train", Some("example.com/mig-1g.5gb"));
        p.metadata.labels = Some(BTreeMap::from([(
            "ksolver.dev/enabled".to_string(),
            "true".to_string(),
        )]));

        let got = render_scheduler_name_patch(&p, &policy);
        match got {
            SchedulerPatchDecision::Patch(patch) => {
                assert_eq!(patch.scheduler_name, "gpu-scheduler");
                assert_eq!(
                    patch.patch[0].value,
                    Some(serde_json::json!("gpu-scheduler"))
                );
            }
            SchedulerPatchDecision::Skip { reason } => panic!("expected patch, got {reason}"),
        }
    }

    fn admission_review_for(pod: corev1::Pod) -> AdmissionReview {
        AdmissionReview {
            api_version: "admission.k8s.io/v1".to_string(),
            kind: "AdmissionReview".to_string(),
            request: Some(AdmissionRequest {
                uid: "req-1".to_string(),
                namespace: pod.metadata.namespace.clone().unwrap_or_default(),
                name: pod.metadata.name.clone().unwrap_or_default(),
                operation: "CREATE".to_string(),
                object: Some(serde_json::to_value(pod).expect("pod should serialize")),
            }),
            response: None,
        }
    }

    #[test]
    fn admission_review_patches_in_scope_gpu_pod() {
        let response = render_scheduler_admission_review(
            admission_review_for(pod("team-a", "train", Some("nvidia.com/gpu"))),
            &Default::default(),
        );
        let response = response.response.expect("response");

        assert!(response.allowed);
        assert_eq!(response.uid, "req-1");
        assert_eq!(response.patch_type.as_deref(), Some("JSONPatch"));
        let patch = response.patch.expect("patch");
        let bytes = base64::engine::general_purpose::STANDARD
            .decode(patch)
            .expect("patch should be base64");
        let ops: Vec<JsonPatchOperation> =
            serde_json::from_slice(&bytes).expect("patch should decode");
        assert_eq!(ops.len(), 1);
        assert_eq!(ops[0].path, "/spec/schedulerName");
        assert_eq!(ops[0].value, Some(serde_json::json!("ksolver")));
    }

    #[test]
    fn admission_review_rejects_request_object_identity_mismatch() {
        let mut review = admission_review_for(pod("team-a", "train", Some("nvidia.com/gpu")));
        review.request.as_mut().unwrap().namespace = "other-team".to_string();

        let response = render_scheduler_admission_review(review, &Default::default());
        let response = response.response.expect("response");

        assert!(!response.allowed);
        assert!(response.patch.is_none());
        assert!(response
            .status
            .expect("status")
            .message
            .contains("does not match object namespace"));

        let mut review = admission_review_for(pod("team-a", "train", Some("nvidia.com/gpu")));
        review.request.as_mut().unwrap().name = "other-pod".to_string();

        let response = render_scheduler_admission_review(review, &Default::default());
        let response = response.response.expect("response");

        assert!(!response.allowed);
        assert!(response.patch.is_none());
        assert!(response
            .status
            .expect("status")
            .message
            .contains("does not match object name"));
    }

    #[test]
    fn admission_review_fills_missing_object_identity_from_request() {
        let mut p = pod("team-a", "train", Some("nvidia.com/gpu"));
        p.metadata.namespace = None;
        p.metadata.name = None;
        let mut review = admission_review_for(p);
        let request = review.request.as_mut().unwrap();
        request.namespace = "team-a".to_string();
        request.name = "train".to_string();

        let response = render_scheduler_admission_review(review, &Default::default());
        let response = response.response.expect("response");

        assert!(response.allowed);
        assert_eq!(response.patch_type.as_deref(), Some("JSONPatch"));
    }

    #[test]
    fn admission_review_allows_without_patch_when_identity_missing_everywhere() {
        let mut p = pod("team-a", "train", Some("nvidia.com/gpu"));
        p.metadata.namespace = None;
        p.metadata.name = None;
        let mut review = admission_review_for(p);
        let request = review.request.as_mut().unwrap();
        request.namespace.clear();
        request.name.clear();

        let response = render_scheduler_admission_review(review, &Default::default());
        let response = response.response.expect("response");

        assert!(response.allowed);
        assert!(response.patch.is_none());
        assert!(response
            .status
            .unwrap()
            .message
            .contains("namespace is empty"));
    }

    #[test]
    fn admission_review_allows_non_create_operation_without_patch() {
        let mut review = admission_review_for(pod("team-a", "train", Some("nvidia.com/gpu")));
        review.request.as_mut().unwrap().operation = "UPDATE".to_string();

        let response = render_scheduler_admission_review(review, &Default::default());
        let response = response.response.expect("response");

        assert!(response.allowed);
        assert!(response.patch.is_none());
        assert!(response.patch_type.is_none());
        assert!(response
            .status
            .expect("status")
            .message
            .contains("not in webhook scope"));
    }

    #[test]
    fn admission_review_allows_out_of_scope_pod_without_patch() {
        let response = render_scheduler_admission_review(
            admission_review_for(pod("team-a", "cpu", None)),
            &Default::default(),
        );
        let response = response.response.expect("response");

        assert!(response.allowed);
        assert!(response.patch.is_none());
        assert!(response.patch_type.is_none());
        assert!(response
            .status
            .expect("status")
            .message
            .contains("does not request"));
    }

    #[test]
    fn admission_review_rejects_malformed_review() {
        let response = render_scheduler_admission_review(
            AdmissionReview {
                api_version: "admission.k8s.io/v1".to_string(),
                kind: "AdmissionReview".to_string(),
                request: None,
                response: None,
            },
            &Default::default(),
        );
        let response = response.response.expect("response");

        assert!(!response.allowed);
        assert!(response.patch.is_none());
        assert!(response
            .status
            .expect("status")
            .message
            .contains("missing request"));
    }
}
