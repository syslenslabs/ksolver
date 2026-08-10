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

/// Bridge live DCGM VRAM metrics into tier-4 observations. `peak_by_pod` maps `"namespace/pod"` to
/// a measured peak VRAM (MiB) — as produced by `historical_usage::query_pod_peak_vram_mib` from the
/// dcgm-exporter — and `pods` supplies the specs to fingerprint. DCGM keys observations by
/// (namespace, pod); the tier-4 store keys them by (image, command_hash), so this joins each metric
/// to its pod's spec, computes the fingerprint, and emits a row. Pods with no matching metric (or a
/// non-positive peak) are skipped; no value is invented for a pod the exporter never reported.
pub fn observations_from_vram_metrics(
    peak_by_pod: &BTreeMap<String, f64>,
    pods: &[corev1::Pod],
) -> Vec<ObservationRow> {
    let mut rows = Vec::new();
    for pod in pods {
        let namespace = pod.metadata.namespace.as_deref().unwrap_or_default();
        let name = pod.metadata.name.as_deref().unwrap_or_default();
        if namespace.is_empty() || name.is_empty() {
            continue;
        }
        let Some(&peak) = peak_by_pod.get(&format!("{namespace}/{name}")) else {
            continue;
        };
        if !peak.is_finite() || peak <= 0.0 {
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
pub fn append_observations(store_path: &str, rows: &[ObservationRow]) -> std::io::Result<()> {
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

/// Collect live DCGM VRAM observations and append them to the tier-4 store. Queries the configured
/// Prometheus (dcgm-exporter) for each pod's peak VRAM over `window`, lists cluster pods to resolve
/// their fingerprints, maps the metrics onto (image, command_hash) rows, and appends them. Returns
/// the number of rows written. Requires a real Prometheus/dcgm-exporter via `resolved`; it writes
/// nothing when the exporter reports no matching series (no observation is ever fabricated).
pub async fn collect_and_store_vram_observations(
    kubeconfig: &str,
    resolved: &crate::historical_usage::ResolvedHistoricalUsageConfig,
    window: &str,
    store_path: &str,
) -> anyhow::Result<usize> {
    use anyhow::Context;
    let peak_by_pod = crate::historical_usage::query_pod_peak_vram_mib(resolved, window)
        .await
        .context("query dcgm pod peak vram")?;
    if peak_by_pod.is_empty() {
        return Ok(0);
    }
    let client = crate::collector::build_client(kubeconfig, None)
        .await
        .context("build kube client for vram observation")?;
    let pods: kube::Api<corev1::Pod> = kube::Api::all(client);
    let list = pods
        .list(&kube::api::ListParams::default())
        .await
        .context("list pods for vram observation")?;
    let rows = observations_from_vram_metrics(&peak_by_pod, &list.items);
    append_observations(store_path, &rows).context("append vram observations")?;
    Ok(rows.len())
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

    #[test]
    fn vram_metrics_map_to_fingerprints_and_skip_unreported_pods() {
        // DCGM keys observations by namespace/pod; the tier-4 store keys them by (image,
        // command_hash). This is the join that turns a live per-pod peak into a fingerprinted row.
        let mk = |ns: &str, name: &str, image: &str| corev1::Pod {
            metadata: k8s_openapi::apimachinery::pkg::apis::meta::v1::ObjectMeta {
                namespace: Some(ns.to_string()),
                name: Some(name.to_string()),
                ..Default::default()
            },
            spec: Some(corev1::PodSpec {
                containers: vec![corev1::Container {
                    name: "t".to_string(),
                    image: Some(image.to_string()),
                    command: Some(vec!["python".to_string(), "train.py".to_string()]),
                    resources: Some(corev1::ResourceRequirements {
                        limits: Some(std::collections::BTreeMap::from([(
                            "nvidia.com/gpu".to_string(),
                            k8s_openapi::apimachinery::pkg::api::resource::Quantity(
                                "1".to_string(),
                            ),
                        )])),
                        ..Default::default()
                    }),
                    ..Default::default()
                }],
                ..Default::default()
            }),
            ..Default::default()
        };
        let pods = vec![
            mk("team", "job-a", "img:1"),
            mk("team", "job-b", "img:2"), // no metric -> skipped
        ];
        let peaks = BTreeMap::from([
            ("team/job-a".to_string(), 17000.0),
            ("team/ghost".to_string(), 9999.0), // metric for a pod not in the list -> ignored
        ]);
        let rows = observations_from_vram_metrics(&peaks, &pods);
        assert_eq!(rows.len(), 1, "only the reported, listed pod yields a row");
        assert_eq!(rows[0].image, "img:1");
        assert_eq!(rows[0].peak_mib, 17000.0);
        // The row's fingerprint must equal the pod's own fingerprint (webhook/predictor parity).
        let (_, expected_hash) = pod_command_hash(&pods[0]).unwrap();
        assert_eq!(rows[0].command_hash, expected_hash);
    }

    #[test]
    fn vram_metrics_skip_nonpositive_peaks() {
        let pod = corev1::Pod {
            metadata: k8s_openapi::apimachinery::pkg::apis::meta::v1::ObjectMeta {
                namespace: Some("team".to_string()),
                name: Some("job".to_string()),
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
            ..Default::default()
        };
        let peaks = BTreeMap::from([("team/job".to_string(), 0.0)]);
        assert!(observations_from_vram_metrics(&peaks, std::slice::from_ref(&pod)).is_empty());
    }

    #[test]
    fn append_observations_writes_python_readable_jsonl_contract() {
        // The store is a cross-language contract: this Rust writer feeds the Python resolver's
        // `load_observations`, which reads each JSONL row's {image, command_hash, peak_mib} and keys
        // by `image|command_hash` with `float(peak_mib)`. Pin the exact shape so a field rename or an
        // extra key can't silently break the predictor.
        let rows = vec![
            ObservationRow {
                image: "img:1".to_string(),
                command_hash: "abc".to_string(),
                peak_mib: 8000.0,
            },
            ObservationRow {
                image: "img:2".to_string(),
                command_hash: "def".to_string(),
                peak_mib: 12345.5,
            },
        ];
        let path = std::env::temp_dir().join(format!(
            "ksolver-vram-store-contract-{}.jsonl",
            std::process::id()
        ));
        let _ = std::fs::remove_file(&path);
        let p = path.to_str().unwrap();

        append_observations(p, &rows).unwrap();
        // Appending again must ADD, not overwrite (the store accumulates observations).
        append_observations(p, &rows[..1]).unwrap();

        let text = std::fs::read_to_string(&path).unwrap();
        let _ = std::fs::remove_file(&path);
        let lines: Vec<&str> = text.lines().filter(|l| !l.trim().is_empty()).collect();
        assert_eq!(lines.len(), 3, "append is additive (2 + 1)");

        let expected: Vec<&ObservationRow> = rows.iter().chain(rows[..1].iter()).collect();
        for (line, exp) in lines.iter().zip(expected) {
            let value: serde_json::Value = serde_json::from_str(line).expect("valid JSON line");
            let obj = value.as_object().expect("JSON object");
            assert_eq!(
                obj.get("image").and_then(|x| x.as_str()),
                Some(exp.image.as_str())
            );
            assert_eq!(
                obj.get("command_hash").and_then(|x| x.as_str()),
                Some(exp.command_hash.as_str())
            );
            // Must be a JSON number so Python `float(peak_mib)` works.
            assert_eq!(
                obj.get("peak_mib").and_then(serde_json::Value::as_f64),
                Some(exp.peak_mib)
            );
            assert_eq!(
                obj.len(),
                3,
                "exactly image/command_hash/peak_mib — no extra keys"
            );
        }
    }

    #[test]
    fn pod_command_hash_selects_gpu_container_not_first_sidecar() {
        // Multi-container pods (e.g. a logging/proxy sidecar first, trainer second) must fingerprint
        // the GPU container, matching Python `_gpu_container` (first GPU-requesting container, else
        // first). Picking the sidecar would produce a fingerprint that never matches the predictor.
        let pod = corev1::Pod {
            spec: Some(corev1::PodSpec {
                containers: vec![
                    corev1::Container {
                        name: "sidecar".to_string(),
                        image: Some("sidecar:1".to_string()),
                        command: Some(vec!["/proxy".to_string()]),
                        ..Default::default()
                    },
                    corev1::Container {
                        name: "trainer".to_string(),
                        image: Some("trainer:1".to_string()),
                        command: Some(vec!["python".to_string(), "train.py".to_string()]),
                        resources: Some(corev1::ResourceRequirements {
                            limits: Some(std::collections::BTreeMap::from([(
                                "nvidia.com/gpu".to_string(),
                                k8s_openapi::apimachinery::pkg::api::resource::Quantity(
                                    "1".to_string(),
                                ),
                            )])),
                            ..Default::default()
                        }),
                        ..Default::default()
                    },
                ],
                ..Default::default()
            }),
            ..Default::default()
        };
        let (image, hash) = pod_command_hash(&pod).expect("has a container");
        assert_eq!(
            image, "trainer:1",
            "must fingerprint the GPU container, not the sidecar"
        );
        // Hash must be the trainer's command hash, not the sidecar's.
        let trainer_hash = workload_command_hash(
            &["python".to_string(), "train.py".to_string()],
            &[],
            &BTreeMap::new(),
        );
        assert_eq!(hash, trainer_hash);
    }

    #[test]
    fn no_fabrication_when_no_metrics_or_no_rows() {
        // Core honesty invariant: with no metrics we produce NO observations, and appending an
        // empty row set writes NOTHING (not even an empty file). Guards the "never fabricate an
        // observation" contract the whole VRAM tier relies on.
        let pod = corev1::Pod {
            metadata: k8s_openapi::apimachinery::pkg::apis::meta::v1::ObjectMeta {
                namespace: Some("team".to_string()),
                name: Some("job".to_string()),
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
            ..Default::default()
        };
        // No DCGM metrics reported for any pod ⇒ zero rows (nothing invented).
        assert!(
            observations_from_vram_metrics(&BTreeMap::new(), std::slice::from_ref(&pod)).is_empty()
        );

        // Empty rows ⇒ the store file is not even created (no phantom/empty observation file).
        let path = std::env::temp_dir().join(format!(
            "ksolver-vram-store-empty-{}.jsonl",
            std::process::id()
        ));
        let _ = std::fs::remove_file(&path);
        append_observations(path.to_str().unwrap(), &[]).unwrap();
        assert!(!path.exists(), "empty append must not create a store file");
    }
}
