use serde::{Deserialize, Serialize};
use std::collections::VecDeque;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Mutex;

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
#[serde(tag = "kind", rename_all = "lowercase")]
pub enum PodPlacement {
    Placed { node: String },
    Unplaced { reason: String },
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct PodDecision {
    pub uid: String,
    pub namespace: String,
    pub name: String,
    pub gpu_request: i64,
    pub placement: PodPlacement,
    /// Scheduling constraints shadow does not model (e.g. pod anti-affinity); a
    /// placed recommendation may violate these. Empty when none.
    #[serde(default)]
    pub caveats: Vec<String>,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct DecisionTrace {
    pub sequence: u64,
    pub observed_pods: usize,
    pub decisions: Vec<PodDecision>,
    pub solver_status: String,
    pub solve_millis: u64,
    /// Time spent strictly inside the CP-SAT solve call (excludes collect/normalize/build);
    /// use this to verify the configured solve time limit.
    #[serde(default)]
    pub solve_core_millis: u64,
    pub snapshot_age_millis: u64,
    pub note: String,
}

pub struct TraceStore {
    capacity: usize,
    inner: Mutex<VecDeque<DecisionTrace>>,
    seq: AtomicU64,
}

impl TraceStore {
    pub fn new(capacity: usize) -> Self {
        Self {
            capacity: capacity.max(1),
            inner: Mutex::new(VecDeque::new()),
            seq: AtomicU64::new(0),
        }
    }

    pub fn next_sequence(&self) -> u64 {
        self.seq.fetch_add(1, Ordering::SeqCst) + 1
    }

    pub fn push(&self, trace: DecisionTrace) {
        let mut g = self.inner.lock().expect("trace store poisoned");
        if g.len() == self.capacity {
            g.pop_front();
        }
        g.push_back(trace);
    }

    pub fn recent(&self) -> Vec<DecisionTrace> {
        let g = self.inner.lock().expect("trace store poisoned");
        g.iter().rev().cloned().collect()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn trace(seq: u64) -> DecisionTrace {
        DecisionTrace {
            sequence: seq,
            observed_pods: 1,
            decisions: vec![PodDecision {
                uid: "u1".into(),
                namespace: "team-a".into(),
                name: "job-0".into(),
                gpu_request: 4,
                placement: PodPlacement::Placed {
                    node: "node-1".into(),
                },
                caveats: vec![],
            }],
            solver_status: "OPTIMAL".into(),
            solve_millis: 12,
            solve_core_millis: 8,
            snapshot_age_millis: 3,
            note: String::new(),
        }
    }

    #[test]
    fn recent_is_newest_first() {
        let s = TraceStore::new(8);
        s.push(trace(1));
        s.push(trace(2));
        let r = s.recent();
        assert_eq!(r[0].sequence, 2);
        assert_eq!(r[1].sequence, 1);
    }

    #[test]
    fn evicts_oldest_beyond_capacity() {
        let s = TraceStore::new(2);
        s.push(trace(1));
        s.push(trace(2));
        s.push(trace(3));
        let r = s.recent();
        assert_eq!(r.len(), 2);
        assert_eq!(r[0].sequence, 3);
    }

    #[test]
    fn sequence_is_monotonic() {
        let s = TraceStore::new(4);
        assert_eq!(s.next_sequence(), 1);
        assert_eq!(s.next_sequence(), 2);
    }

    #[test]
    fn pod_decision_deserializes_without_caveats() {
        // Backward compatibility: older traces omit the `caveats` field.
        let json = r#"{"uid":"u1","namespace":"team-a","name":"job-0","gpu_request":1,"placement":{"kind":"placed","node":"n1"}}"#;
        let d: PodDecision = serde_json::from_str(json).expect("deserialize");
        assert!(d.caveats.is_empty());
    }
}
