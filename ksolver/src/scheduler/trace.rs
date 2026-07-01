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
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct DecisionTrace {
    pub sequence: u64,
    pub observed_pods: usize,
    pub decisions: Vec<PodDecision>,
    pub solver_status: String,
    pub solve_millis: u64,
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
            }],
            solver_status: "OPTIMAL".into(),
            solve_millis: 12,
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
}
