//! Active smoke-probe health classification (spec §9).
//!
//! [`classify`] is the single place that turns "what the substrate observed"
//! plus "what the last active probe found" plus "is the crash-restart
//! breaker currently open" into the [`buzz_core::AgentHealth`] +
//! machine-readable reason published in `AGENT_NODE_STATUS`.
use std::time::Duration;

use buzz_core::AgentHealth;

use crate::model::Observed;

/// How often each running agent gets an active round-trip probe
/// ([`crate::runtime::AgentRuntime::probe`]), beyond mere OS-process
/// liveness.
pub const SMOKE_PROBE_INTERVAL: Duration = Duration::from_secs(300);

/// Combine a process observation, the latest probe outcome, and the
/// substrate's crash-restart breaker state into a reportable health +
/// machine-readable reason. Returns `None` when there is nothing worth
/// reporting (`Observed::Absent` — no process/record exists at all).
///
/// `probe_ok`: `None` means "not probed this cycle" (only a `Running` agent
/// is ever actively probed — see `crate::engine`'s wiring); `Some(false)`
/// means the round-trip probe itself failed even though the process is
/// still alive.
///
/// `breaker_open`: true when [`crate::substrate::Substrate::breaker_open`]
/// reports the agent's crash-restart circuit breaker is currently forbidding
/// a start — a deliberate cooldown after repeated crashes, not a fresh
/// unexpected failure. Checked FIRST, ahead of `observed`: a breaker-open
/// agent always reports `Stopped`/`"breaker-open"`, even if the last
/// observed value is a stale `Crashed` (the very crash that tripped the
/// breaker) — reporting that as `Crashed` again would misleadingly suggest
/// an active, ongoing failure rather than a deliberate, bounded cooldown
/// (carried Batch A/B review finding).
pub fn classify(
    observed: &Observed,
    probe_ok: Option<bool>,
    breaker_open: bool,
) -> Option<(AgentHealth, Option<String>)> {
    if breaker_open {
        return Some((AgentHealth::Stopped, Some("breaker-open".into())));
    }
    let health = match observed {
        Observed::Absent => return None,
        Observed::Starting => (AgentHealth::Starting, None),
        Observed::Running => match probe_ok {
            Some(false) => (AgentHealth::Crashed, Some("probe-failed".into())),
            _ => (AgentHealth::Running, None),
        },
        Observed::Stopped => (AgentHealth::Stopped, None),
        Observed::Crashed { code: Some(code) } => {
            (AgentHealth::Crashed, Some(format!("exit-{code}")))
        }
        Observed::Crashed { code: None } => (AgentHealth::Crashed, Some("exit-unknown".into())),
    };
    Some(health)
}

#[cfg(test)]
mod tests {
    use super::*;
    use buzz_core::AgentHealth;

    #[test]
    fn starting_and_healthy_running_have_no_reason() {
        assert_eq!(
            classify(&Observed::Starting, None, false),
            Some((AgentHealth::Starting, None))
        );
        assert_eq!(
            classify(&Observed::Running, Some(true), false),
            Some((AgentHealth::Running, None))
        );
        assert_eq!(
            classify(&Observed::Running, None, false),
            Some((AgentHealth::Running, None)),
            "not probed this cycle must not be treated as a failure"
        );
    }

    #[test]
    fn running_with_failed_probe_is_crashed_probe_failed() {
        let (h, r) = classify(&Observed::Running, Some(false), false).unwrap();
        assert_eq!(h, AgentHealth::Crashed);
        assert_eq!(r.as_deref(), Some("probe-failed"));
    }

    #[test]
    fn crashed_with_known_code_reports_exit_reason() {
        let (h, r) = classify(&Observed::Crashed { code: Some(1) }, None, false).unwrap();
        assert_eq!(h, AgentHealth::Crashed);
        assert_eq!(r.as_deref(), Some("exit-1"));
    }

    #[test]
    fn crashed_with_unknown_code_reports_exit_unknown() {
        let (h, r) = classify(&Observed::Crashed { code: None }, None, false).unwrap();
        assert_eq!(h, AgentHealth::Crashed);
        assert_eq!(r.as_deref(), Some("exit-unknown"));
    }

    #[test]
    fn stopped_has_no_reason() {
        assert_eq!(
            classify(&Observed::Stopped, None, false),
            Some((AgentHealth::Stopped, None))
        );
    }

    #[test]
    fn absent_is_not_reportable() {
        assert_eq!(classify(&Observed::Absent, None, false), None);
    }

    /// The carried review finding: a breaker-open agent must never be
    /// reported as a fresh `Crashed` — it's a deliberate cooldown, not an
    /// ongoing failure.
    #[test]
    fn breaker_open_overrides_a_stale_crashed_observation_as_stopped_cooldown() {
        let (h, r) = classify(&Observed::Crashed { code: Some(1) }, None, true).unwrap();
        assert_eq!(
            h,
            AgentHealth::Stopped,
            "breaker cooldown must not read as a fresh crash"
        );
        assert_eq!(r.as_deref(), Some("breaker-open"));
    }

    /// Breaker-open takes precedence even over an actively probed `Running`
    /// observation — defensive: in practice the two shouldn't coincide (a
    /// live child means the breaker let a start through), but `classify`
    /// must not contradict `breaker_open` when a caller passes both.
    #[test]
    fn breaker_open_overrides_running_too() {
        let (h, r) = classify(&Observed::Running, Some(true), true).unwrap();
        assert_eq!(h, AgentHealth::Stopped);
        assert_eq!(r.as_deref(), Some("breaker-open"));
    }
}
