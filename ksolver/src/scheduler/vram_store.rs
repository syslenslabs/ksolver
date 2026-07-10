//! Tier-4 VRAM observation store: let ksolver record measured peak-VRAM observations for
//! completed GPU pods, keyed by a workload fingerprint that byte-matches the Python resolver's
//! `pod_fingerprint` (`vram_resolver.py`). This closes the tier-4 loop — the predictor service
//! reads the same JSONL store the scheduler writes.
//!
//! The fingerprint is `sha256(canonical_json)` where canonical_json mirrors Python's
//! `sha256_json({"command":[...],"args":[...],"env":{name:value}})` with sorted keys and compact
//! separators. A measured peak is taken from the `ksolver.dev/observed-peak-vram-mib` annotation
//! (populated by a metrics source such as DCGM / a probe sidecar); ksolver does not measure VRAM
//! itself.

use k8s_openapi::api::core::v1 as corev1;
use sha2::{Digest, Sha256};
use std::collections::BTreeMap;

/// Annotation carrying a completed pod's measured peak VRAM in MiB (set by a metrics source).
pub const OBSERVED_PEAK_ANNOTATION: &str = "ksolver.dev/observed-peak-vram-mib";

/// sha256 hex of the canonical `{"args","command","env"}` JSON, matching Python `sha256_json`.
pub fn workload_command_hash(
    command: &[String],
    args: &[String],
    env: &BTreeMap<String, Option<String>>,
) -> String {
    let args_json = serde_json::to_string(args).unwrap_or_else(|_| "[]".to_string());
    let cmd_json = serde_json::to_string(command).unwrap_or_else(|_| "[]".to_string());
    // BTreeMap iterates in sorted key order == Python json.dumps(sort_keys=True).
    let env_parts: Vec<String> = env
        .iter()
        .map(|(k, v)| {
            let key = serde_json::to_string(k).unwrap_or_else(|_| "\"\"".to_string());
            let val = match v {
                Some(s) => serde_json::to_string(s).unwrap_or_else(|_| "\"\"".to_string()),
                None => "null".to_string(),
            };
            format!("{key}:{val}")
        })
        .collect();
    let canonical = format!(
        "{{\"args\":{args_json},\"command\":{cmd_json},\"env\":{{{}}}}}",
        env_parts.join(",")
    );
    let mut hasher = Sha256::new();
    hasher.update(canonical.as_bytes());
    format!("{:x}", hasher.finalize())
}

/// The GPU container's (image, command_hash) — mirrors the Python resolver's `pod_fingerprint`.
pub fn pod_command_hash(pod: &corev1::Pod) -> Option<(String, String)> {
    let spec = pod.spec.as_ref()?;
    let container = spec
        .containers
        .iter()
        .find(|c| {
            c.resources
                .as_ref()
                .into_iter()
                .flat_map(|r| r.requests.iter().chain(r.limits.iter()))
                .flat_map(|m| m.keys())
                .any(|k| k.to_lowercase().contains("gpu"))
        })
        .or_else(|| spec.containers.first())?;
    let image = container.image.clone().unwrap_or_default();
    let command = container.command.clone().unwrap_or_default();
    let args = container.args.clone().unwrap_or_default();
    let mut env: BTreeMap<String, Option<String>> = BTreeMap::new();
    for e in container.env.iter().flatten() {
        env.insert(e.name.clone(), e.value.clone());
    }
    Some((image, workload_command_hash(&command, &args, &env)))
}

/// A store row (JSONL); field set matches what the Python `load_observations` reads.
#[derive(Debug, serde::Serialize)]
pub struct ObservationRow {
    pub image: String,
    pub command_hash: String,
    pub peak_mib: f64,
}

/// Extract (image, command_hash, peak_mib) for completed pods carrying the observed-peak annotation.
pub fn observations_from_pods(pods: &[corev1::Pod]) -> Vec<ObservationRow> {
    let mut rows = Vec::new();
    for pod in pods {
        let phase = pod
            .status
            .as_ref()
            .and_then(|s| s.phase.as_deref())
            .unwrap_or("");
        if phase != "Succeeded" {
            continue;
        }
        let Some(peak) = pod
            .metadata
            .annotations
            .as_ref()
            .and_then(|a| a.get(OBSERVED_PEAK_ANNOTATION))
            .and_then(|v| v.parse::<f64>().ok())
        else {
            continue;
        };
        if peak <= 0.0 {
            continue;
        }
        if let Some((image, command_hash)) = pod_command_hash(pod) {
            rows.push(ObservationRow {
                image,
                command_hash,
                peak_mib: peak,
            });
        }
    }
    rows
}

/// Append observation rows to the JSONL store (one JSON object per line).
pub fn append_observations(
    store_path: &str,
    rows: &[ObservationRow],
) -> std::io::Result<()> {
    use std::io::Write;
    if rows.is_empty() {
        return Ok(());
    }
    let mut file = std::fs::OpenOptions::new()
        .create(true)
        .append(true)
        .open(store_path)?;
    for row in rows {
        let line = serde_json::to_string(row).unwrap_or_default();
        writeln!(file, "{line}")?;
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn command_hash_matches_python_pod_fingerprint() {
        // Fixture identical to vram_resolver.pod_fingerprint; expected hash computed by Python.
        let command = vec!["python".to_string(), "train.py".to_string()];
        let args = vec!["--bs".to_string(), "8".to_string()];
        let mut env = BTreeMap::new();
        env.insert("B".to_string(), Some("2".to_string()));
        env.insert("A".to_string(), Some("1".to_string()));
        let hash = workload_command_hash(&command, &args, &env);
        assert_eq!(
            hash,
            "1d38135f20ad3a389d575a9ff775d2e4b00c36b215958c9ae2717a36f46b2bd8"
        );
    }

    #[test]
    fn command_hash_matches_python_for_null_valuefrom_env() {
        // A valueFrom env has no literal value -> Python stores name:None (json null); the Rust
        // side must serialize null too, or tier-4 keys diverge across the webhook/predictor boundary.
        let command = vec!["python".to_string(), "t.py".to_string()];
        let args: Vec<String> = vec![];
        let mut env = BTreeMap::new();
        env.insert("A".to_string(), Some("1".to_string()));
        env.insert("B".to_string(), None); // valueFrom -> null
        let hash = workload_command_hash(&command, &args, &env);
        assert_eq!(
            hash,
            "eaa6a4fb61954e8cfb836369d109a019a83fe3416ccdcb315cc7f2957d322d36"
        );
    }

    #[test]
    fn observations_only_from_completed_annotated_pods() {
        let mk = |phase: &str, peak: Option<&str>| corev1::Pod {
            metadata: k8s_openapi::apimachinery::pkg::apis::meta::v1::ObjectMeta {
                annotations: peak.map(|p| {
                    std::collections::BTreeMap::from([(
                        OBSERVED_PEAK_ANNOTATION.to_string(),
                        p.to_string(),
                    )])
                }),
                ..Default::default()
            },
            spec: Some(corev1::PodSpec {
                containers: vec![corev1::Container {
                    name: "t".to_string(),
                    image: Some("img:1".to_string()),
                    ..Default::default()
                }],
                ..Default::default()
            }),
            status: Some(corev1::PodStatus {
                phase: Some(phase.to_string()),
                ..Default::default()
            }),
        };
        let pods = vec![
            mk("Succeeded", Some("8000")), // recorded
            mk("Running", Some("9000")),   // skipped: not completed
            mk("Succeeded", None),         // skipped: no measurement
            mk("Succeeded", Some("0")),    // skipped: non-positive
        ];
        let rows = observations_from_pods(&pods);
        assert_eq!(rows.len(), 1);
        assert_eq!(rows[0].peak_mib, 8000.0);
        assert_eq!(rows[0].image, "img:1");
    }
}
