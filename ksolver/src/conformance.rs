//! Feasibility conformance harness: compares our `feasible_on_node` verdict against real
//! kube-scheduler Filter decisions (via kube-scheduler-simulator), per (pod, node) pair.
//!
//! For each pair we get two verdicts — ours (`node_feasibility_reasons(...).is_empty()`)
//! and the scheduler's (present the simulator a snapshot with exactly that one node, empty
//! of other pods, plus the pod; the pod binds ⇒ Filter passed; unschedulable ⇒ Filter
//! failed). One node isolates Filter from Score. This module holds the pure classification
//! and reporting logic; the simulator round-trip reuses `verifier`'s client.

/// Outcome of comparing our feasibility verdict to the scheduler's for one (pod, node) pair.
/// `FalsePositive` (we say feasible, the scheduler rejects) is the dangerous case — it means
/// we would recommend a placement the real scheduler refuses.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Verdict {
    Agree,
    FalsePositive,
    FalseNegative,
}

/// Classify a pair from the two boolean verdicts.
pub fn classify(ours_feasible: bool, scheduler_feasible: bool) -> Verdict {
    match (ours_feasible, scheduler_feasible) {
        (true, true) | (false, false) => Verdict::Agree,
        (true, false) => Verdict::FalsePositive,
        (false, true) => Verdict::FalseNegative,
    }
}

/// Tally of verdicts across all compared pairs.
#[derive(Debug, Default, Clone, Copy)]
pub struct ConfusionMatrix {
    pub agree: usize,
    pub false_positive: usize,
    pub false_negative: usize,
}

impl ConfusionMatrix {
    pub fn record(&mut self, v: Verdict) {
        match v {
            Verdict::Agree => self.agree += 1,
            Verdict::FalsePositive => self.false_positive += 1,
            Verdict::FalseNegative => self.false_negative += 1,
        }
    }

    pub fn total(&self) -> usize {
        self.agree + self.false_positive + self.false_negative
    }

    /// Fraction of pairs where we agree with the scheduler. An empty matrix is vacuously 1.0.
    pub fn agreement_rate(&self) -> f64 {
        if self.total() == 0 {
            1.0
        } else {
            self.agree as f64 / self.total() as f64
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn classify_covers_all_combinations() {
        assert_eq!(classify(true, true), Verdict::Agree);
        assert_eq!(classify(false, false), Verdict::Agree);
        assert_eq!(classify(true, false), Verdict::FalsePositive);
        assert_eq!(classify(false, true), Verdict::FalseNegative);
    }

    #[test]
    fn confusion_matrix_records_and_rates() {
        let mut m = ConfusionMatrix::default();
        // empty is vacuously perfect agreement.
        assert_eq!(m.total(), 0);
        assert_eq!(m.agreement_rate(), 1.0);

        m.record(Verdict::Agree);
        m.record(Verdict::Agree);
        m.record(Verdict::Agree);
        m.record(Verdict::FalsePositive);
        assert_eq!(m.total(), 4);
        assert_eq!(m.agree, 3);
        assert_eq!(m.false_positive, 1);
        assert_eq!(m.false_negative, 0);
        assert!((m.agreement_rate() - 0.75).abs() < 1e-9);

        m.record(Verdict::FalseNegative);
        assert_eq!(m.false_negative, 1);
        assert_eq!(m.total(), 5);
    }
}
