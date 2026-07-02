use crate::model::{ClusterSnapshot, Pod};
use serde::{Deserialize, Serialize};
use std::collections::BTreeSet;

#[derive(Debug, Clone, Default, Serialize, Deserialize, PartialEq, Eq)]
pub struct JobObservation {
    pub namespace: String,
    pub pod: String,
    pub uid: String,
    pub owner_kind: String,
    pub owner_name: String,
    pub phase: String,
    pub node: String,
    pub gpu_request: i64,
    pub runtime_seconds: i64,
    pub peak_memory_bytes: i64,
    #[serde(default)]
    pub predicted_runtime_seconds: i64,
    #[serde(default)]
    pub predicted_peak_vram_bytes: i64,
    pub command_hash: String,
    pub container_images: Vec<String>,
    #[serde(default)]
    pub framework: String,
    #[serde(default)]
    pub job_type: String,
}

#[derive(Debug, Clone, Default, Serialize, Deserialize, PartialEq, Eq)]
pub struct JobObservationMetrics {
    pub completed_gpu_pods: usize,
    pub runtime_observations: usize,
    pub failed_gpu_pods: usize,
    pub max_runtime_seconds: i64,
    pub max_peak_memory_bytes: i64,
    pub unique_command_hashes: usize,
    pub runtime_prediction_samples: usize,
    pub runtime_prediction_mape_milli: i64,
    pub max_runtime_prediction_error_seconds: i64,
    pub vram_prediction_samples: usize,
    pub vram_prediction_mape_milli: i64,
    pub max_vram_prediction_error_bytes: i64,
}

fn pod_gpu_request(pod: &Pod, is_gpu_resource: &dyn Fn(&str) -> bool) -> i64 {
    pod.extended_resource_requests
        .iter()
        .filter(|(resource, _)| is_gpu_resource(resource))
        .map(|(_, qty)| *qty)
        .sum::<i64>()
        .max(0)
}

fn peak_memory_bytes(pod: &Pod) -> i64 {
    pod.memory_history
        .max_bytes
        .max(pod.usage.memory_bytes)
        .max(0)
}

pub fn extract_completed_gpu_observations(
    snapshot: &ClusterSnapshot,
    is_gpu_resource: &dyn Fn(&str) -> bool,
) -> Vec<JobObservation> {
    let mut observations = Vec::new();
    for pod in &snapshot.pods {
        if !matches!(pod.phase.as_str(), "Succeeded" | "Failed") {
            continue;
        }
        let gpu_request = pod_gpu_request(pod, is_gpu_resource);
        if gpu_request <= 0 {
            continue;
        }
        if pod.start_time_unix <= 0 || pod.finish_time_unix <= pod.start_time_unix {
            continue;
        }
        observations.push(JobObservation {
            namespace: pod.namespace.clone(),
            pod: pod.name.clone(),
            uid: pod.uid.clone(),
            owner_kind: pod.owner_kind.clone(),
            owner_name: pod.owner_name.clone(),
            phase: pod.phase.clone(),
            node: pod.node_name.clone(),
            gpu_request,
            runtime_seconds: pod.finish_time_unix - pod.start_time_unix,
            peak_memory_bytes: peak_memory_bytes(pod),
            predicted_runtime_seconds: pod.predicted_runtime_seconds,
            predicted_peak_vram_bytes: pod.predicted_peak_vram_bytes,
            command_hash: pod.command_hash.clone(),
            container_images: pod.container_images.clone(),
            framework: infer_pod_framework(pod),
            job_type: infer_pod_job_type(pod),
        });
    }
    observations
}

pub fn infer_pod_framework(pod: &Pod) -> String {
    let owner = pod.owner_kind.to_ascii_lowercase();
    if owner.contains("pytorch") || image_contains(pod, "pytorch") || image_contains(pod, "torch") {
        return "pytorch".to_string();
    }
    if owner.contains("tensorflow")
        || owner == "tfjob"
        || image_contains(pod, "tensorflow")
        || image_contains(pod, "tf-")
    {
        return "tensorflow".to_string();
    }
    if owner.contains("ray")
        || pod.labels.contains_key("ray.io/cluster")
        || image_contains(pod, "ray")
    {
        return "ray".to_string();
    }
    if image_contains(pod, "jax") {
        return "jax".to_string();
    }
    if image_contains(pod, "deepspeed") {
        return "deepspeed".to_string();
    }
    String::new()
}

pub fn infer_pod_job_type(pod: &Pod) -> String {
    let owner = pod.owner_kind.to_ascii_lowercase();
    if owner.contains("pytorch") {
        return "kubeflow_pytorchjob".to_string();
    }
    if owner.contains("tensorflow") || owner == "tfjob" {
        return "kubeflow_tfjob".to_string();
    }
    if owner.contains("ray") || pod.labels.contains_key("ray.io/cluster") {
        return "rayjob".to_string();
    }
    if pod.labels.contains_key("volcano.sh/job-name")
        || pod.labels.contains_key("batch.volcano.sh/job-name")
    {
        return "volcano_job".to_string();
    }
    if pod.labels.contains_key("workflows.argoproj.io/workflow") {
        return "argo_workflow".to_string();
    }
    if owner == "job" {
        return "kubernetes_job".to_string();
    }
    if pod.owner_kind.is_empty() {
        return "bare_pod".to_string();
    }
    pod.owner_kind.to_ascii_lowercase()
}

fn image_contains(pod: &Pod, needle: &str) -> bool {
    let needle = needle.to_ascii_lowercase();
    pod.container_images
        .iter()
        .any(|image| image.to_ascii_lowercase().contains(&needle))
}

pub fn summarize_job_observations(observations: &[JobObservation]) -> JobObservationMetrics {
    let mut unique_command_hashes = BTreeSet::new();
    let mut metrics = JobObservationMetrics {
        completed_gpu_pods: observations.len(),
        runtime_observations: observations
            .iter()
            .filter(|o| o.runtime_seconds > 0)
            .count(),
        failed_gpu_pods: observations.iter().filter(|o| o.phase == "Failed").count(),
        ..Default::default()
    };
    for observation in observations {
        metrics.max_runtime_seconds = metrics.max_runtime_seconds.max(observation.runtime_seconds);
        metrics.max_peak_memory_bytes = metrics
            .max_peak_memory_bytes
            .max(observation.peak_memory_bytes);
        if observation.predicted_runtime_seconds > 0 && observation.runtime_seconds > 0 {
            metrics.runtime_prediction_samples += 1;
            let error = (observation.runtime_seconds - observation.predicted_runtime_seconds).abs();
            metrics.max_runtime_prediction_error_seconds =
                metrics.max_runtime_prediction_error_seconds.max(error);
            metrics.runtime_prediction_mape_milli +=
                error.saturating_mul(1000) / observation.runtime_seconds.max(1);
        }
        if observation.predicted_peak_vram_bytes > 0 && observation.peak_memory_bytes > 0 {
            metrics.vram_prediction_samples += 1;
            let error =
                (observation.peak_memory_bytes - observation.predicted_peak_vram_bytes).abs();
            metrics.max_vram_prediction_error_bytes =
                metrics.max_vram_prediction_error_bytes.max(error);
            metrics.vram_prediction_mape_milli +=
                error.saturating_mul(1000) / observation.peak_memory_bytes.max(1);
        }
        if !observation.command_hash.is_empty() {
            unique_command_hashes.insert(observation.command_hash.clone());
        }
    }
    if metrics.runtime_prediction_samples > 0 {
        metrics.runtime_prediction_mape_milli /= metrics.runtime_prediction_samples as i64;
    }
    if metrics.vram_prediction_samples > 0 {
        metrics.vram_prediction_mape_milli /= metrics.vram_prediction_samples as i64;
    }
    metrics.unique_command_hashes = unique_command_hashes.len();
    metrics
}

pub fn summarize_snapshot_observations(
    snapshot: &ClusterSnapshot,
    is_gpu_resource: &dyn Fn(&str) -> bool,
) -> JobObservationMetrics {
    let observations = extract_completed_gpu_observations(snapshot, is_gpu_resource);
    summarize_job_observations(&observations)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::model::{ClusterSnapshot, MemoryHistory, Pod, ResourceUsage};
    use std::collections::BTreeMap;

    fn is_gpu_resource(name: &str) -> bool {
        name == "nvidia.com/gpu" || name.starts_with("nvidia.com/mig-")
    }

    fn completed_gpu_pod(name: &str, phase: &str) -> Pod {
        Pod {
            namespace: "team".to_string(),
            name: name.to_string(),
            uid: format!("uid-{name}"),
            node_name: "gpu-1".to_string(),
            phase: phase.to_string(),
            start_time_unix: 100,
            finish_time_unix: 460,
            owner_kind: "Job".to_string(),
            owner_name: "train".to_string(),
            extended_resource_requests: BTreeMap::from([("nvidia.com/gpu".to_string(), 2)]),
            usage: ResourceUsage {
                memory_bytes: 8 * 1024 * 1024 * 1024,
                ..Default::default()
            },
            memory_history: MemoryHistory {
                max_bytes: 10 * 1024 * 1024 * 1024,
                ..Default::default()
            },
            predicted_runtime_seconds: 300,
            predicted_peak_vram_bytes: 8 * 1024 * 1024 * 1024,
            command_hash: "abc123".to_string(),
            container_images: vec!["repo/train:1".to_string()],
            ..Default::default()
        }
    }

    #[test]
    fn extracts_completed_gpu_pod_observations() {
        let snapshot = ClusterSnapshot {
            pods: vec![
                completed_gpu_pod("ok", "Succeeded"),
                Pod {
                    name: "running".to_string(),
                    phase: "Running".to_string(),
                    extended_resource_requests: BTreeMap::from([("nvidia.com/gpu".to_string(), 1)]),
                    ..Default::default()
                },
                Pod {
                    name: "cpu-only".to_string(),
                    phase: "Succeeded".to_string(),
                    start_time_unix: 1,
                    finish_time_unix: 2,
                    ..Default::default()
                },
            ],
            ..Default::default()
        };

        let observations = extract_completed_gpu_observations(&snapshot, &is_gpu_resource);

        assert_eq!(observations.len(), 1);
        assert_eq!(observations[0].pod, "ok");
        assert_eq!(observations[0].gpu_request, 2);
        assert_eq!(observations[0].runtime_seconds, 360);
        assert_eq!(observations[0].peak_memory_bytes, 10 * 1024 * 1024 * 1024);
        assert_eq!(observations[0].predicted_runtime_seconds, 300);
        assert_eq!(
            observations[0].predicted_peak_vram_bytes,
            8 * 1024 * 1024 * 1024
        );
        assert_eq!(observations[0].command_hash, "abc123");
        assert_eq!(observations[0].job_type, "kubernetes_job");
    }

    #[test]
    fn classifies_observation_framework_and_job_type() {
        let cases = vec![
            (
                Pod {
                    owner_kind: "PyTorchJob".to_string(),
                    container_images: vec!["registry/train:latest".to_string()],
                    ..completed_gpu_pod("pytorch", "Succeeded")
                },
                "pytorch",
                "kubeflow_pytorchjob",
            ),
            (
                Pod {
                    labels: BTreeMap::from([("ray.io/cluster".to_string(), "ray-a".to_string())]),
                    ..completed_gpu_pod("ray", "Succeeded")
                },
                "ray",
                "rayjob",
            ),
            (
                Pod {
                    labels: BTreeMap::from([(
                        "volcano.sh/job-name".to_string(),
                        "train".to_string(),
                    )]),
                    ..completed_gpu_pod("volcano", "Succeeded")
                },
                "",
                "volcano_job",
            ),
            (
                Pod {
                    labels: BTreeMap::from([(
                        "workflows.argoproj.io/workflow".to_string(),
                        "wf".to_string(),
                    )]),
                    ..completed_gpu_pod("argo", "Succeeded")
                },
                "",
                "argo_workflow",
            ),
            (
                Pod {
                    owner_kind: String::new(),
                    container_images: vec!["repo/tensorflow-train:1".to_string()],
                    ..completed_gpu_pod("bare", "Succeeded")
                },
                "tensorflow",
                "bare_pod",
            ),
        ];

        let snapshot = ClusterSnapshot {
            pods: cases.iter().map(|(pod, _, _)| pod.clone()).collect(),
            ..Default::default()
        };

        let observations = extract_completed_gpu_observations(&snapshot, &is_gpu_resource);

        for ((_, expected_framework, expected_job_type), observation) in
            cases.iter().zip(observations.iter())
        {
            assert_eq!(&observation.framework, expected_framework);
            assert_eq!(&observation.job_type, expected_job_type);
        }
    }

    #[test]
    fn summarizes_completed_gpu_observations() {
        let observations = vec![
            completed_gpu_pod("ok", "Succeeded"),
            completed_gpu_pod("failed", "Failed"),
        ];
        let observations = observations
            .into_iter()
            .map(|pod| {
                let runtime_seconds = pod.finish_time_unix - pod.start_time_unix;
                let peak_memory_bytes = peak_memory_bytes(&pod);
                JobObservation {
                    namespace: pod.namespace,
                    pod: pod.name,
                    uid: pod.uid,
                    owner_kind: pod.owner_kind,
                    owner_name: pod.owner_name,
                    phase: pod.phase,
                    node: pod.node_name,
                    gpu_request: 2,
                    runtime_seconds,
                    peak_memory_bytes,
                    predicted_runtime_seconds: pod.predicted_runtime_seconds,
                    predicted_peak_vram_bytes: pod.predicted_peak_vram_bytes,
                    command_hash: pod.command_hash,
                    container_images: pod.container_images,
                    ..Default::default()
                }
            })
            .collect::<Vec<_>>();

        let metrics = summarize_job_observations(&observations);

        assert_eq!(metrics.completed_gpu_pods, 2);
        assert_eq!(metrics.runtime_observations, 2);
        assert_eq!(metrics.failed_gpu_pods, 1);
        assert_eq!(metrics.max_runtime_seconds, 360);
        assert_eq!(metrics.unique_command_hashes, 1);
        assert_eq!(metrics.runtime_prediction_samples, 2);
        assert_eq!(metrics.runtime_prediction_mape_milli, 166);
        assert_eq!(metrics.max_runtime_prediction_error_seconds, 60);
        assert_eq!(metrics.vram_prediction_samples, 2);
        assert_eq!(metrics.vram_prediction_mape_milli, 200);
        assert_eq!(
            metrics.max_vram_prediction_error_bytes,
            2 * 1024 * 1024 * 1024
        );
    }
}
