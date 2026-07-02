use crate::model::{ClusterSnapshot, Pod};
use crate::scheduler::observations::{infer_pod_framework, infer_pod_job_type, JobObservation};
use crate::scheduler::pod_filter::PendingGpuPod;
use crate::scheduler::trace::{PredictionAuditDetail, PredictionAuditMetrics};
use serde::{Deserialize, Serialize};
use std::collections::BTreeMap;

const GIB: f64 = 1024.0 * 1024.0 * 1024.0;
const MAX_RUNTIME_SECONDS: f64 = 30.0 * 24.0 * 3600.0;
const MAX_VRAM_BYTES: f64 = 1920.0 * GIB;

#[derive(Debug, Clone, Default, Serialize, Deserialize, PartialEq)]
pub struct TrainingHints {
    pub model_parameters_billions: f64,
    pub batch_size: i64,
    pub sequence_length: i64,
    pub precision: String,
}

#[derive(Debug, Clone, Default, Serialize, Deserialize, PartialEq)]
pub struct WorkloadPredictionRequest {
    pub command_hash: String,
    #[serde(default)]
    pub framework: String,
    #[serde(default)]
    pub job_type: String,
    pub gpu_request: i64,
    pub hints: TrainingHints,
}

#[derive(Debug, Clone, Default, Serialize, Deserialize, PartialEq, Eq)]
pub struct WorkloadPrediction {
    pub predicted_runtime_seconds: i64,
    pub predicted_peak_vram_bytes: i64,
    pub runtime_source: String,
    pub vram_source: String,
    #[serde(default)]
    pub prediction_key: String,
    pub sample_count: usize,
    /// 0..100 confidence score. This is an operator-facing calibration signal, not a probability.
    pub confidence: i64,
}

#[derive(Debug, Clone, Default)]
pub struct HistoricalJobPredictor {
    by_command_and_gpu: BTreeMap<(String, i64), SampleStats>,
    by_command: BTreeMap<String, SampleStats>,
    by_job_type_and_gpu: BTreeMap<(String, i64), SampleStats>,
    by_job_type: BTreeMap<String, SampleStats>,
    by_framework_and_gpu: BTreeMap<(String, i64), SampleStats>,
    by_framework: BTreeMap<String, SampleStats>,
}

#[derive(Debug, Clone, Default)]
struct SampleStats {
    runtime_seconds: Vec<i64>,
    peak_vram_bytes: Vec<i64>,
    gpu_requests: Vec<i64>,
}

impl HistoricalJobPredictor {
    pub fn from_observations(observations: &[JobObservation]) -> Self {
        let mut predictor = HistoricalJobPredictor::default();
        for observation in observations {
            if observation.gpu_request <= 0 {
                continue;
            }
            if observation.runtime_seconds <= 0 && observation.peak_memory_bytes <= 0 {
                continue;
            }
            if !observation.command_hash.is_empty() {
                predictor
                    .by_command_and_gpu
                    .entry((observation.command_hash.clone(), observation.gpu_request))
                    .or_default()
                    .push(observation);
                predictor
                    .by_command
                    .entry(observation.command_hash.clone())
                    .or_default()
                    .push(observation);
            }
            if !observation.job_type.is_empty() {
                predictor
                    .by_job_type_and_gpu
                    .entry((observation.job_type.clone(), observation.gpu_request))
                    .or_default()
                    .push(observation);
                predictor
                    .by_job_type
                    .entry(observation.job_type.clone())
                    .or_default()
                    .push(observation);
            }
            if !observation.framework.is_empty() {
                predictor
                    .by_framework_and_gpu
                    .entry((observation.framework.clone(), observation.gpu_request))
                    .or_default()
                    .push(observation);
                predictor
                    .by_framework
                    .entry(observation.framework.clone())
                    .or_default()
                    .push(observation);
            }
        }
        predictor
    }

    pub fn predict(&self, request: &WorkloadPredictionRequest) -> WorkloadPrediction {
        let gpu_request = request.gpu_request.max(1);
        if !request.command_hash.is_empty() {
            if let Some(stats) = self
                .by_command_and_gpu
                .get(&(request.command_hash.clone(), gpu_request))
            {
                return prediction_from_stats(
                    stats,
                    gpu_request,
                    "history_exact",
                    &prediction_key("command_hash", &request.command_hash),
                    90,
                );
            }
            if let Some(stats) = self.by_command.get(&request.command_hash) {
                return prediction_from_stats(
                    stats,
                    gpu_request,
                    "history_scaled",
                    &prediction_key("command_hash", &request.command_hash),
                    70,
                );
            }
        }
        if !request.job_type.is_empty() {
            if let Some(stats) = self
                .by_job_type_and_gpu
                .get(&(request.job_type.clone(), gpu_request))
            {
                return prediction_from_stats(
                    stats,
                    gpu_request,
                    "history_segment",
                    &prediction_key("job_type", &request.job_type),
                    55,
                );
            }
            if let Some(stats) = self.by_job_type.get(&request.job_type) {
                return prediction_from_stats(
                    stats,
                    gpu_request,
                    "history_segment",
                    &prediction_key("job_type", &request.job_type),
                    50,
                );
            }
        }
        if !request.framework.is_empty() {
            if let Some(stats) = self
                .by_framework_and_gpu
                .get(&(request.framework.clone(), gpu_request))
            {
                return prediction_from_stats(
                    stats,
                    gpu_request,
                    "history_segment",
                    &prediction_key("framework", &request.framework),
                    45,
                );
            }
            if let Some(stats) = self.by_framework.get(&request.framework) {
                return prediction_from_stats(
                    stats,
                    gpu_request,
                    "history_segment",
                    &prediction_key("framework", &request.framework),
                    40,
                );
            }
        }

        heuristic_prediction(&request.hints, gpu_request)
    }
}

impl SampleStats {
    fn push(&mut self, observation: &JobObservation) {
        if observation.runtime_seconds > 0 {
            self.runtime_seconds.push(observation.runtime_seconds);
        }
        if observation.peak_memory_bytes > 0 {
            self.peak_vram_bytes.push(observation.peak_memory_bytes);
        }
        if observation.gpu_request > 0 {
            self.gpu_requests.push(observation.gpu_request);
        }
    }

    fn sample_count(&self) -> usize {
        self.runtime_seconds
            .len()
            .max(self.peak_vram_bytes.len())
            .max(self.gpu_requests.len())
    }
}

fn prediction_from_stats(
    stats: &SampleStats,
    requested_gpus: i64,
    source: &str,
    key: &str,
    base_confidence: i64,
) -> WorkloadPrediction {
    let observed_gpus = median_i64(&stats.gpu_requests)
        .unwrap_or(requested_gpus)
        .max(1);
    let runtime = median_i64(&stats.runtime_seconds)
        .map(|seconds| scale_runtime(seconds, observed_gpus, requested_gpus))
        .unwrap_or(0);
    let peak_vram = median_i64(&stats.peak_vram_bytes).unwrap_or(0);
    let samples = stats.sample_count();
    WorkloadPrediction {
        predicted_runtime_seconds: runtime,
        predicted_peak_vram_bytes: peak_vram,
        runtime_source: if runtime > 0 {
            source.to_string()
        } else {
            "unknown".to_string()
        },
        vram_source: if peak_vram > 0 {
            source.to_string()
        } else {
            "unknown".to_string()
        },
        prediction_key: key.to_string(),
        sample_count: samples,
        confidence: confidence(base_confidence, samples),
    }
}

fn prediction_key(kind: &str, value: &str) -> String {
    if value.is_empty() {
        String::new()
    } else {
        format!("{kind}:{value}")
    }
}

pub fn heuristic_prediction(hints: &TrainingHints, gpu_request: i64) -> WorkloadPrediction {
    let runtime = heuristic_runtime_seconds(hints, gpu_request);
    let peak_vram = heuristic_peak_vram_bytes(hints, gpu_request);
    WorkloadPrediction {
        predicted_runtime_seconds: runtime,
        predicted_peak_vram_bytes: peak_vram,
        runtime_source: if runtime > 0 { "hint" } else { "unknown" }.to_string(),
        vram_source: if peak_vram > 0 { "hint" } else { "unknown" }.to_string(),
        prediction_key: if runtime > 0 || peak_vram > 0 {
            "training_hint".to_string()
        } else {
            String::new()
        },
        sample_count: 0,
        confidence: if runtime > 0 || peak_vram > 0 { 35 } else { 0 },
    }
}

pub fn audit_pending_predictions(
    snapshot: &ClusterSnapshot,
    pending: &[PendingGpuPod],
    observations: &[JobObservation],
) -> PredictionAuditMetrics {
    summarize_prediction_audit_details(&audit_pending_prediction_details(
        snapshot,
        pending,
        observations,
    ))
}

pub fn enrich_pending_with_historical_predictions(
    snapshot: &ClusterSnapshot,
    pending: &[PendingGpuPod],
    observations: &[JobObservation],
) -> Vec<PendingGpuPod> {
    let predictor = HistoricalJobPredictor::from_observations(observations);
    pending
        .iter()
        .map(|pod| {
            let raw = find_raw_pod(snapshot, pod);
            let prediction = predictor.predict(&prediction_request_for_pod(raw, pod));
            let mut enriched = pod.clone();
            if enriched.predicted_runtime_seconds <= 0 && prediction.predicted_runtime_seconds > 0 {
                enriched.predicted_runtime_seconds = prediction.predicted_runtime_seconds;
            }
            if enriched.predicted_peak_vram_bytes <= 0 && prediction.predicted_peak_vram_bytes > 0 {
                enriched.predicted_peak_vram_bytes = prediction.predicted_peak_vram_bytes;
            }
            enriched
        })
        .collect()
}

pub fn audit_pending_prediction_details(
    snapshot: &ClusterSnapshot,
    pending: &[PendingGpuPod],
    observations: &[JobObservation],
) -> Vec<PredictionAuditDetail> {
    let predictor = HistoricalJobPredictor::from_observations(observations);
    pending
        .iter()
        .map(|pod| {
            let raw = find_raw_pod(snapshot, pod);
            let request = prediction_request_for_pod(raw, pod);
            let command_hash = request.command_hash.clone();
            let command_fingerprint_matched = !command_hash.is_empty();
            let framework = request.framework.clone();
            let job_type = request.job_type.clone();
            let mut prediction = predictor.predict(&request);
            if prediction.predicted_runtime_seconds <= 0 && pod.predicted_runtime_seconds > 0 {
                prediction.predicted_runtime_seconds = pod.predicted_runtime_seconds;
                prediction.runtime_source = "pending_hint".to_string();
                if prediction.prediction_key.is_empty() {
                    prediction.prediction_key = "pending_hint".to_string();
                }
                prediction.confidence = prediction.confidence.max(35);
            }
            if prediction.predicted_peak_vram_bytes <= 0 && pod.predicted_peak_vram_bytes > 0 {
                prediction.predicted_peak_vram_bytes = pod.predicted_peak_vram_bytes;
                prediction.vram_source = "pending_hint".to_string();
                if prediction.prediction_key.is_empty() {
                    prediction.prediction_key = "pending_hint".to_string();
                }
                prediction.confidence = prediction.confidence.max(35);
            }
            let (runtime_lower, runtime_upper) = prediction_band(
                prediction.predicted_runtime_seconds,
                prediction.confidence,
                MAX_RUNTIME_SECONDS as i64,
            );
            let (vram_lower, vram_upper) = prediction_band(
                prediction.predicted_peak_vram_bytes,
                prediction.confidence,
                MAX_VRAM_BYTES as i64,
            );
            PredictionAuditDetail {
                uid: pod.uid.clone(),
                namespace: pod.namespace.clone(),
                name: pod.name.clone(),
                gpu_request: pod.gpu_request,
                command_fingerprint_matched,
                framework,
                job_type,
                prediction_key: prediction.prediction_key,
                predicted_runtime_seconds: prediction.predicted_runtime_seconds,
                predicted_runtime_lower_seconds: runtime_lower,
                predicted_runtime_upper_seconds: runtime_upper,
                predicted_peak_vram_bytes: prediction.predicted_peak_vram_bytes,
                predicted_peak_vram_lower_bytes: vram_lower,
                predicted_peak_vram_upper_bytes: vram_upper,
                runtime_source: prediction.runtime_source,
                vram_source: prediction.vram_source,
                sample_count: prediction.sample_count,
                confidence: prediction.confidence,
            }
        })
        .collect()
}

fn prediction_request_for_pod(
    raw: Option<&Pod>,
    pending: &PendingGpuPod,
) -> WorkloadPredictionRequest {
    WorkloadPredictionRequest {
        command_hash: raw.map(|p| p.command_hash.clone()).unwrap_or_default(),
        framework: raw.map(infer_pod_framework).unwrap_or_default(),
        job_type: raw.map(infer_pod_job_type).unwrap_or_default(),
        gpu_request: pending.gpu_request,
        ..Default::default()
    }
}

pub fn summarize_prediction_audit_details(
    details: &[PredictionAuditDetail],
) -> PredictionAuditMetrics {
    let mut metrics = PredictionAuditMetrics {
        pending_pods: details.len(),
        ..Default::default()
    };
    let mut confidence_sum = 0i64;
    for detail in details {
        if detail.command_fingerprint_matched {
            metrics.fingerprint_matched_pods += 1;
        }
        match prediction_bucket(detail) {
            "history_exact" => metrics.history_exact_pods += 1,
            "history_scaled" => metrics.history_scaled_pods += 1,
            "history_segment" => metrics.history_segment_pods += 1,
            "hint" => metrics.hint_pods += 1,
            _ => metrics.unknown_pods += 1,
        }
        if detail.predicted_runtime_seconds > 0 {
            metrics.predicted_runtime_pods += 1;
        }
        if detail.predicted_peak_vram_bytes > 0 {
            metrics.predicted_vram_pods += 1;
        }
        confidence_sum += detail.confidence;
    }
    if metrics.pending_pods > 0 {
        metrics.average_confidence = confidence_sum / metrics.pending_pods as i64;
    }
    metrics
}

fn prediction_bucket(detail: &PredictionAuditDetail) -> &'static str {
    for source in [&detail.runtime_source, &detail.vram_source] {
        match source.as_str() {
            "history_exact" => return "history_exact",
            "history_scaled" => return "history_scaled",
            "history_segment" => return "history_segment",
            _ => {}
        }
    }
    if [&detail.runtime_source, &detail.vram_source]
        .iter()
        .any(|source| matches!(source.as_str(), "hint" | "pending_hint"))
    {
        return "hint";
    }
    "unknown"
}

fn find_raw_pod<'a>(snapshot: &'a ClusterSnapshot, pending: &PendingGpuPod) -> Option<&'a Pod> {
    if !pending.uid.is_empty() {
        if let Some(pod) = snapshot.pods.iter().find(|pod| pod.uid == pending.uid) {
            return Some(pod);
        }
    }
    snapshot
        .pods
        .iter()
        .find(|pod| pod.namespace == pending.namespace && pod.name == pending.name)
}

fn heuristic_runtime_seconds(hints: &TrainingHints, gpu_request: i64) -> i64 {
    if hints.model_parameters_billions <= 0.0 || hints.batch_size <= 0 {
        return 0;
    }
    let seq_factor = sequence_factor(hints.sequence_length);
    let precision = precision_runtime_multiplier(&hints.precision);
    let gpu_factor = gpu_request.max(1) as f64;
    let seconds = 600.0
        * hints.model_parameters_billions
        * (hints.batch_size as f64 / 32.0)
        * seq_factor
        * precision
        / gpu_factor.sqrt();
    seconds.round().clamp(60.0, MAX_RUNTIME_SECONDS) as i64
}

fn heuristic_peak_vram_bytes(hints: &TrainingHints, gpu_request: i64) -> i64 {
    if hints.model_parameters_billions <= 0.0 || hints.batch_size <= 0 {
        return 0;
    }
    let seq_factor = sequence_factor(hints.sequence_length);
    let bytes_per_param = precision_bytes_per_param(&hints.precision);
    let gpu_factor = gpu_request.max(1) as f64;
    let model_gib = hints.model_parameters_billions * bytes_per_param * 1.25;
    let activation_gib =
        hints.model_parameters_billions * (hints.batch_size as f64 / 32.0) * seq_factor * 1.5;
    let per_gpu_gib = (model_gib + activation_gib) / gpu_factor.sqrt();
    (per_gpu_gib.clamp(1.0, MAX_VRAM_BYTES / GIB) * GIB).round() as i64
}

fn scale_runtime(seconds: i64, observed_gpus: i64, requested_gpus: i64) -> i64 {
    if seconds <= 0 {
        return 0;
    }
    let scaled =
        seconds as f64 * (observed_gpus.max(1) as f64 / requested_gpus.max(1) as f64).sqrt();
    scaled.round().clamp(1.0, MAX_RUNTIME_SECONDS) as i64
}

fn median_i64(values: &[i64]) -> Option<i64> {
    if values.is_empty() {
        return None;
    }
    let mut sorted = values.to_vec();
    sorted.sort_unstable();
    Some(sorted[sorted.len() / 2])
}

fn confidence(base: i64, samples: usize) -> i64 {
    let sample_bonus = match samples {
        0 => 0,
        1 => 0,
        2..=3 => 5,
        4..=9 => 10,
        _ => 15,
    };
    (base + sample_bonus).clamp(0, 95)
}

fn prediction_band(value: i64, confidence: i64, cap: i64) -> (i64, i64) {
    if value <= 0 {
        return (0, 0);
    }
    let confidence = confidence.clamp(0, 100);
    let uncertainty_percent = (100 - confidence).max(5);
    let delta = value.saturating_mul(uncertainty_percent) / 100;
    let lower = value.saturating_sub(delta).max(1);
    let upper = value.saturating_add(delta).min(cap.max(value));
    (lower, upper)
}

fn sequence_factor(sequence_length: i64) -> f64 {
    if sequence_length > 0 {
        (sequence_length as f64 / 2048.0).clamp(0.5, 8.0)
    } else {
        1.0
    }
}

fn precision_runtime_multiplier(raw: &str) -> f64 {
    match raw.trim().to_ascii_lowercase().as_str() {
        "fp32" | "float32" => 1.8,
        "bf16" | "bfloat16" | "fp16" | "float16" | "half" => 1.0,
        "fp8" | "int8" => 0.7,
        _ => 1.2,
    }
}

fn precision_bytes_per_param(raw: &str) -> f64 {
    match raw.trim().to_ascii_lowercase().as_str() {
        "fp32" | "float32" => 4.0,
        "bf16" | "bfloat16" | "fp16" | "float16" | "half" => 2.0,
        "fp8" | "int8" => 1.0,
        _ => 2.0,
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::collections::BTreeMap;

    fn observation(
        command_hash: &str,
        gpu_request: i64,
        runtime: i64,
        peak_gib: i64,
    ) -> JobObservation {
        JobObservation {
            command_hash: command_hash.to_string(),
            gpu_request,
            runtime_seconds: runtime,
            peak_memory_bytes: peak_gib * 1024 * 1024 * 1024,
            ..Default::default()
        }
    }

    fn segment_observation(
        job_type: &str,
        framework: &str,
        gpu_request: i64,
        runtime: i64,
        peak_gib: i64,
    ) -> JobObservation {
        JobObservation {
            job_type: job_type.to_string(),
            framework: framework.to_string(),
            gpu_request,
            runtime_seconds: runtime,
            peak_memory_bytes: peak_gib * 1024 * 1024 * 1024,
            ..Default::default()
        }
    }

    fn pending(uid: &str, namespace: &str, name: &str, gpu_request: i64) -> PendingGpuPod {
        PendingGpuPod {
            uid: uid.to_string(),
            namespace: namespace.to_string(),
            name: name.to_string(),
            gpu_request,
            priority: 0,
            priority_class_name: None,
            team: None,
            queue: None,
            business_value: 0,
            queue_wait_seconds: 0,
            deadline_unix_seconds: 0,
            min_gpus: 0,
            max_gpus: 0,
            preferred_gpus: 0,
            flexible: false,
            predicted_runtime_seconds: 0,
            predicted_peak_vram_bytes: 0,
            required_gpu_topology: Vec::new(),
            gang_key: None,
            colocate: false,
            unmodeled_constraints: Vec::new(),
            anti_affinity_host_selectors: Vec::new(),
            affinity_topology_selectors: Vec::new(),
            anti_affinity_topology_selectors: Vec::new(),
            preferred_node_affinity: Vec::new(),
            preferred_pod_affinity: Vec::new(),
        }
    }

    #[test]
    fn exact_history_uses_median_runtime_and_vram() {
        let predictor = HistoricalJobPredictor::from_observations(&[
            observation("cmd", 4, 100, 40),
            observation("cmd", 4, 120, 44),
            observation("cmd", 4, 90, 38),
        ]);

        let prediction = predictor.predict(&WorkloadPredictionRequest {
            command_hash: "cmd".to_string(),
            gpu_request: 4,
            ..Default::default()
        });

        assert_eq!(prediction.predicted_runtime_seconds, 100);
        assert_eq!(
            prediction.predicted_peak_vram_bytes,
            40 * 1024 * 1024 * 1024
        );
        assert_eq!(prediction.runtime_source, "history_exact");
        assert_eq!(prediction.prediction_key, "command_hash:cmd");
        assert_eq!(prediction.sample_count, 3);
        assert!(prediction.confidence >= 90);
    }

    #[test]
    fn command_history_scales_runtime_across_gpu_counts() {
        let predictor =
            HistoricalJobPredictor::from_observations(&[observation("cmd", 1, 3600, 24)]);

        let prediction = predictor.predict(&WorkloadPredictionRequest {
            command_hash: "cmd".to_string(),
            gpu_request: 4,
            ..Default::default()
        });

        assert_eq!(prediction.predicted_runtime_seconds, 1800);
        assert_eq!(
            prediction.predicted_peak_vram_bytes,
            24 * 1024 * 1024 * 1024
        );
        assert_eq!(prediction.runtime_source, "history_scaled");
        assert_eq!(prediction.prediction_key, "command_hash:cmd");
    }

    #[test]
    fn training_hints_produce_bootstrap_prediction_without_history() {
        let predictor = HistoricalJobPredictor::default();

        let prediction = predictor.predict(&WorkloadPredictionRequest {
            command_hash: "unknown".to_string(),
            gpu_request: 4,
            hints: TrainingHints {
                model_parameters_billions: 7.0,
                batch_size: 64,
                sequence_length: 4096,
                precision: "bf16".to_string(),
            },
            ..Default::default()
        });

        assert_eq!(prediction.predicted_runtime_seconds, 8400);
        assert!(prediction.predicted_peak_vram_bytes > 0);
        assert_eq!(prediction.runtime_source, "hint");
        assert_eq!(prediction.prediction_key, "training_hint");
        assert_eq!(prediction.confidence, 35);
    }

    #[test]
    fn job_type_history_predicts_when_command_history_is_absent() {
        let predictor = HistoricalJobPredictor::from_observations(&[
            segment_observation("kubeflow_pytorchjob", "pytorch", 4, 1200, 42),
            segment_observation("kubeflow_pytorchjob", "pytorch", 4, 1800, 48),
        ]);

        let prediction = predictor.predict(&WorkloadPredictionRequest {
            command_hash: "new-command".to_string(),
            job_type: "kubeflow_pytorchjob".to_string(),
            framework: "pytorch".to_string(),
            gpu_request: 4,
            ..Default::default()
        });

        assert_eq!(prediction.predicted_runtime_seconds, 1800);
        assert_eq!(
            prediction.predicted_peak_vram_bytes,
            48 * 1024 * 1024 * 1024
        );
        assert_eq!(prediction.runtime_source, "history_segment");
        assert_eq!(prediction.vram_source, "history_segment");
        assert_eq!(prediction.prediction_key, "job_type:kubeflow_pytorchjob");
        assert_eq!(prediction.sample_count, 2);
        assert!(prediction.confidence >= 55);
    }

    #[test]
    fn framework_history_predicts_when_job_type_history_is_absent() {
        let predictor = HistoricalJobPredictor::from_observations(&[segment_observation(
            "kubernetes_job",
            "jax",
            1,
            3600,
            80,
        )]);

        let prediction = predictor.predict(&WorkloadPredictionRequest {
            command_hash: "new-command".to_string(),
            framework: "jax".to_string(),
            gpu_request: 4,
            ..Default::default()
        });

        assert_eq!(prediction.predicted_runtime_seconds, 1800);
        assert_eq!(
            prediction.predicted_peak_vram_bytes,
            80 * 1024 * 1024 * 1024
        );
        assert_eq!(prediction.runtime_source, "history_segment");
        assert_eq!(prediction.prediction_key, "framework:jax");
        assert_eq!(prediction.confidence, 40);
    }

    #[test]
    fn empty_request_returns_unknown_prediction() {
        let prediction =
            HistoricalJobPredictor::default().predict(&WorkloadPredictionRequest::default());

        assert_eq!(prediction.predicted_runtime_seconds, 0);
        assert_eq!(prediction.predicted_peak_vram_bytes, 0);
        assert_eq!(prediction.runtime_source, "unknown");
        assert_eq!(prediction.vram_source, "unknown");
        assert_eq!(prediction.prediction_key, "");
        assert_eq!(prediction.confidence, 0);
    }

    #[test]
    fn audit_matches_pending_fingerprint_to_historical_prediction() {
        let snapshot = ClusterSnapshot {
            pods: vec![Pod {
                namespace: "team".to_string(),
                name: "train".to_string(),
                uid: "pending-uid".to_string(),
                command_hash: "cmd".to_string(),
                extended_resource_requests: BTreeMap::from([("nvidia.com/gpu".to_string(), 4)]),
                ..Default::default()
            }],
            ..Default::default()
        };
        let pending = vec![pending("pending-uid", "team", "train", 4)];
        let observations = vec![observation("cmd", 4, 120, 40)];

        let metrics = audit_pending_predictions(&snapshot, &pending, &observations);

        assert_eq!(metrics.pending_pods, 1);
        assert_eq!(metrics.fingerprint_matched_pods, 1);
        assert_eq!(metrics.history_exact_pods, 1);
        assert_eq!(metrics.history_scaled_pods, 0);
        assert_eq!(metrics.predicted_runtime_pods, 1);
        assert_eq!(metrics.predicted_vram_pods, 1);
        assert!(metrics.average_confidence >= 90);
    }

    #[test]
    fn audit_details_expose_prediction_source_per_pending_pod() {
        let snapshot = ClusterSnapshot {
            pods: vec![Pod {
                namespace: "team".to_string(),
                name: "train".to_string(),
                uid: "pending-uid".to_string(),
                command_hash: "cmd".to_string(),
                extended_resource_requests: BTreeMap::from([("nvidia.com/gpu".to_string(), 4)]),
                ..Default::default()
            }],
            ..Default::default()
        };
        let pending = vec![pending("pending-uid", "team", "train", 4)];
        let observations = vec![observation("cmd", 4, 120, 40)];

        let details = audit_pending_prediction_details(&snapshot, &pending, &observations);

        assert_eq!(details.len(), 1);
        assert_eq!(details[0].namespace, "team");
        assert_eq!(details[0].name, "train");
        assert!(details[0].command_fingerprint_matched);
        assert_eq!(details[0].prediction_key, "command_hash:cmd");
        assert_eq!(details[0].runtime_source, "history_exact");
        assert_eq!(details[0].vram_source, "history_exact");
        assert_eq!(details[0].predicted_runtime_seconds, 120);
        assert!(details[0].predicted_runtime_lower_seconds > 0);
        assert!(details[0].predicted_runtime_lower_seconds <= details[0].predicted_runtime_seconds);
        assert!(details[0].predicted_runtime_upper_seconds >= details[0].predicted_runtime_seconds);
        assert_eq!(
            details[0].predicted_peak_vram_bytes,
            40 * 1024 * 1024 * 1024
        );
        assert!(details[0].predicted_peak_vram_lower_bytes > 0);
        assert!(details[0].predicted_peak_vram_lower_bytes <= details[0].predicted_peak_vram_bytes);
        assert!(details[0].predicted_peak_vram_upper_bytes >= details[0].predicted_peak_vram_bytes);
        assert!(details[0].confidence >= 90);
    }

    #[test]
    fn pending_hint_predictions_are_reported_when_history_is_absent() {
        let snapshot = ClusterSnapshot::default();
        let mut pod = pending("pending-uid", "team", "train", 4);
        pod.predicted_runtime_seconds = 7200;
        pod.predicted_peak_vram_bytes = 48 * 1024 * 1024 * 1024;

        let details = audit_pending_prediction_details(&snapshot, &[pod], &[]);
        let metrics = summarize_prediction_audit_details(&details);

        assert_eq!(details.len(), 1);
        assert!(!details[0].command_fingerprint_matched);
        assert_eq!(details[0].runtime_source, "pending_hint");
        assert_eq!(details[0].vram_source, "pending_hint");
        assert_eq!(details[0].prediction_key, "pending_hint");
        assert_eq!(details[0].predicted_runtime_seconds, 7200);
        assert_eq!(details[0].predicted_runtime_lower_seconds, 2520);
        assert_eq!(details[0].predicted_runtime_upper_seconds, 11880);
        assert_eq!(
            details[0].predicted_peak_vram_bytes,
            48 * 1024 * 1024 * 1024
        );
        assert_eq!(details[0].predicted_peak_vram_lower_bytes, 18_038_862_644);
        assert_eq!(details[0].predicted_peak_vram_upper_bytes, 85_040_352_460);
        assert_eq!(metrics.hint_pods, 1);
        assert_eq!(metrics.unknown_pods, 0);
        assert_eq!(metrics.predicted_runtime_pods, 1);
        assert_eq!(metrics.predicted_vram_pods, 1);
        assert_eq!(metrics.average_confidence, 35);
    }

    #[test]
    fn enrich_pending_uses_history_when_pending_estimates_are_absent() {
        let snapshot = ClusterSnapshot {
            pods: vec![Pod {
                namespace: "team".to_string(),
                name: "train".to_string(),
                uid: "pending-uid".to_string(),
                command_hash: "cmd".to_string(),
                extended_resource_requests: BTreeMap::from([("nvidia.com/gpu".to_string(), 4)]),
                ..Default::default()
            }],
            ..Default::default()
        };
        let pending = vec![pending("pending-uid", "team", "train", 4)];
        let observations = vec![observation("cmd", 4, 120, 40)];

        let enriched =
            enrich_pending_with_historical_predictions(&snapshot, &pending, &observations);

        assert_eq!(enriched.len(), 1);
        assert_eq!(enriched[0].predicted_runtime_seconds, 120);
        assert_eq!(
            enriched[0].predicted_peak_vram_bytes,
            40 * 1024 * 1024 * 1024
        );
    }

    #[test]
    fn enrich_pending_preserves_existing_pending_estimates() {
        let snapshot = ClusterSnapshot {
            pods: vec![Pod {
                namespace: "team".to_string(),
                name: "train".to_string(),
                uid: "pending-uid".to_string(),
                command_hash: "cmd".to_string(),
                extended_resource_requests: BTreeMap::from([("nvidia.com/gpu".to_string(), 4)]),
                ..Default::default()
            }],
            ..Default::default()
        };
        let mut pod = pending("pending-uid", "team", "train", 4);
        pod.predicted_runtime_seconds = 7200;
        pod.predicted_peak_vram_bytes = 48 * 1024 * 1024 * 1024;
        let observations = vec![observation("cmd", 4, 120, 40)];

        let enriched = enrich_pending_with_historical_predictions(&snapshot, &[pod], &observations);

        assert_eq!(enriched.len(), 1);
        assert_eq!(enriched[0].predicted_runtime_seconds, 7200);
        assert_eq!(
            enriched[0].predicted_peak_vram_bytes,
            48 * 1024 * 1024 * 1024
        );
    }

    #[test]
    fn audit_reports_segment_history_prediction_for_matching_workload_type() {
        let snapshot = ClusterSnapshot {
            pods: vec![Pod {
                namespace: "team".to_string(),
                name: "train".to_string(),
                uid: "pending-uid".to_string(),
                owner_kind: "PyTorchJob".to_string(),
                container_images: vec!["repo/trainer:latest".to_string()],
                extended_resource_requests: BTreeMap::from([("nvidia.com/gpu".to_string(), 4)]),
                ..Default::default()
            }],
            ..Default::default()
        };
        let pending = vec![pending("pending-uid", "team", "train", 4)];
        let observations = vec![segment_observation(
            "kubeflow_pytorchjob",
            "pytorch",
            4,
            2400,
            64,
        )];

        let details = audit_pending_prediction_details(&snapshot, &pending, &observations);
        let metrics = summarize_prediction_audit_details(&details);

        assert_eq!(details.len(), 1);
        assert!(!details[0].command_fingerprint_matched);
        assert_eq!(details[0].framework, "pytorch");
        assert_eq!(details[0].job_type, "kubeflow_pytorchjob");
        assert_eq!(details[0].prediction_key, "job_type:kubeflow_pytorchjob");
        assert_eq!(details[0].runtime_source, "history_segment");
        assert_eq!(details[0].vram_source, "history_segment");
        assert_eq!(details[0].predicted_runtime_seconds, 2400);
        assert_eq!(metrics.history_segment_pods, 1);
        assert_eq!(metrics.unknown_pods, 0);
        assert_eq!(metrics.predicted_runtime_pods, 1);
        assert_eq!(metrics.predicted_vram_pods, 1);
    }

    #[test]
    fn audit_reports_framework_segment_key_when_job_type_history_is_absent() {
        let snapshot = ClusterSnapshot {
            pods: vec![Pod {
                namespace: "team".to_string(),
                name: "train".to_string(),
                uid: "pending-uid".to_string(),
                container_images: vec!["repo/jax-trainer:latest".to_string()],
                extended_resource_requests: BTreeMap::from([("nvidia.com/gpu".to_string(), 4)]),
                ..Default::default()
            }],
            ..Default::default()
        };
        let pending = vec![pending("pending-uid", "team", "train", 4)];
        let observations = vec![segment_observation("kubernetes_job", "jax", 1, 3600, 80)];

        let details = audit_pending_prediction_details(&snapshot, &pending, &observations);
        let metrics = summarize_prediction_audit_details(&details);

        assert_eq!(details.len(), 1);
        assert_eq!(details[0].framework, "jax");
        assert_eq!(details[0].job_type, "bare_pod");
        assert_eq!(details[0].prediction_key, "framework:jax");
        assert_eq!(details[0].runtime_source, "history_segment");
        assert_eq!(details[0].predicted_runtime_seconds, 1800);
        assert_eq!(metrics.history_segment_pods, 1);
        assert_eq!(metrics.unknown_pods, 0);
    }
}
