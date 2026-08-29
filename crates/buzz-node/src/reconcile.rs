//! Pure desired-vs-observed reconciliation — no I/O, no clock, no async.
use std::collections::{BTreeMap, BTreeSet};

use buzz_core::AssignState;
use nostr::PublicKey;

use crate::model::{Action, DesiredAgent, Observed};

/// Compute the actions that converge `observed` to `desired`.
///
/// Deterministic: emits exactly one [`Action`] per agent in the union of the
/// desired and observed sets, ordered by pubkey. Pure — all effects are the
/// caller's to apply via the [`crate::substrate::Substrate`] trait.
pub fn reconcile(
    desired: &[DesiredAgent],
    observed: &BTreeMap<PublicKey, Observed>,
) -> Vec<Action> {
    let mut want_run: BTreeMap<PublicKey, &DesiredAgent> = BTreeMap::new();
    let mut universe: BTreeSet<PublicKey> = BTreeSet::new();
    for d in desired {
        universe.insert(d.agent_pubkey);
        if d.state == AssignState::Assigned {
            want_run.insert(d.agent_pubkey, d);
        }
    }
    universe.extend(observed.keys().copied());

    let mut actions = Vec::with_capacity(universe.len());
    for pk in universe {
        let obs = observed.get(&pk).copied().unwrap_or(Observed::Absent);
        let action = if let Some(d) = want_run.get(&pk) {
            match obs {
                Observed::Absent | Observed::Stopped => Action::Start(Box::new((*d).clone())),
                Observed::Crashed { .. } => Action::Restart(Box::new((*d).clone())),
                Observed::Starting | Observed::Running => Action::Noop(pk),
            }
        } else {
            match obs {
                Observed::Starting | Observed::Running => Action::Stop(pk),
                Observed::Absent | Observed::Stopped | Observed::Crashed { .. } => {
                    Action::Noop(pk)
                }
            }
        };
        actions.push(action);
    }
    actions
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::model::{fake_desired, Action, DesiredAgent, Observed};
    use buzz_core::AssignState::{Assigned, Unassigned};
    use nostr::{Keys, PublicKey};
    use std::collections::BTreeMap;

    fn obs(pairs: &[(&Keys, Observed)]) -> BTreeMap<PublicKey, Observed> {
        pairs.iter().map(|(k, o)| (k.public_key(), *o)).collect()
    }

    fn start_of(d: &DesiredAgent) -> Action {
        Action::Start(Box::new(d.clone()))
    }
    fn restart_of(d: &DesiredAgent) -> Action {
        Action::Restart(Box::new(d.clone()))
    }

    #[test]
    fn assigned_absent_starts() {
        let (a, n, o) = (Keys::generate(), Keys::generate(), Keys::generate());
        let d = fake_desired(&a, &n, &o, Assigned);
        assert_eq!(reconcile(std::slice::from_ref(&d), &BTreeMap::new()), vec![start_of(&d)]);
    }

    #[test]
    fn assigned_stopped_starts() {
        let (a, n, o) = (Keys::generate(), Keys::generate(), Keys::generate());
        let d = fake_desired(&a, &n, &o, Assigned);
        assert_eq!(
            reconcile(std::slice::from_ref(&d), &obs(&[(&a, Observed::Stopped)])),
            vec![start_of(&d)]
        );
    }

    #[test]
    fn assigned_crashed_restarts() {
        let (a, n, o) = (Keys::generate(), Keys::generate(), Keys::generate());
        let d = fake_desired(&a, &n, &o, Assigned);
        let observed = obs(&[(&a, Observed::Crashed { code: Some(1) })]);
        assert_eq!(reconcile(std::slice::from_ref(&d), &observed), vec![restart_of(&d)]);
    }

    #[test]
    fn assigned_running_noops() {
        let (a, n, o) = (Keys::generate(), Keys::generate(), Keys::generate());
        let d = fake_desired(&a, &n, &o, Assigned);
        assert_eq!(reconcile(&[d], &obs(&[(&a, Observed::Running)])), vec![Action::Noop(a.public_key())]);
    }

    #[test]
    fn assigned_starting_noops() {
        let (a, n, o) = (Keys::generate(), Keys::generate(), Keys::generate());
        let d = fake_desired(&a, &n, &o, Assigned);
        assert_eq!(reconcile(&[d], &obs(&[(&a, Observed::Starting)])), vec![Action::Noop(a.public_key())]);
    }

    #[test]
    fn unassigned_running_stops() {
        let (a, n, o) = (Keys::generate(), Keys::generate(), Keys::generate());
        let d = fake_desired(&a, &n, &o, Unassigned);
        assert_eq!(reconcile(&[d], &obs(&[(&a, Observed::Running)])), vec![Action::Stop(a.public_key())]);
    }

    #[test]
    fn unassigned_starting_stops() {
        let (a, n, o) = (Keys::generate(), Keys::generate(), Keys::generate());
        let d = fake_desired(&a, &n, &o, Unassigned);
        assert_eq!(reconcile(&[d], &obs(&[(&a, Observed::Starting)])), vec![Action::Stop(a.public_key())]);
    }

    #[test]
    fn unassigned_absent_noops() {
        let (a, n, o) = (Keys::generate(), Keys::generate(), Keys::generate());
        let d = fake_desired(&a, &n, &o, Unassigned);
        assert_eq!(reconcile(&[d], &BTreeMap::new()), vec![Action::Noop(a.public_key())]);
    }

    #[test]
    fn unassigned_stopped_noops() {
        let (a, n, o) = (Keys::generate(), Keys::generate(), Keys::generate());
        let d = fake_desired(&a, &n, &o, Unassigned);
        assert_eq!(reconcile(&[d], &obs(&[(&a, Observed::Stopped)])), vec![Action::Noop(a.public_key())]);
    }

    #[test]
    fn not_desired_running_stops() {
        // Agent running on the substrate but no longer in the desired set (moved away).
        let a = Keys::generate();
        assert_eq!(reconcile(&[], &obs(&[(&a, Observed::Running)])), vec![Action::Stop(a.public_key())]);
    }

    #[test]
    fn not_desired_crashed_noops() {
        let a = Keys::generate();
        let observed = obs(&[(&a, Observed::Crashed { code: None })]);
        assert_eq!(reconcile(&[], &observed), vec![Action::Noop(a.public_key())]);
    }

    #[test]
    fn empty_desired_and_observed_yields_nothing() {
        assert!(reconcile(&[], &BTreeMap::new()).is_empty());
    }

    #[test]
    fn multi_agent_output_is_sorted_by_pubkey() {
        let (n, o) = (Keys::generate(), Keys::generate());
        // Two agents; one assigned+absent (Start), one running-but-not-desired (Stop).
        let mut a1 = Keys::generate();
        let mut a2 = Keys::generate();
        if a1.public_key() > a2.public_key() {
            std::mem::swap(&mut a1, &mut a2);
        } // a1 < a2
        let d1 = fake_desired(&a1, &n, &o, Assigned);
        let out = reconcile(std::slice::from_ref(&d1), &obs(&[(&a2, Observed::Running)]));
        assert_eq!(out, vec![start_of(&d1), Action::Stop(a2.public_key())]);
    }
}
