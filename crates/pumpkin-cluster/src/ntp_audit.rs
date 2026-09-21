use std::time::{SystemTime, UNIX_EPOCH};

use crate::ntp::{DEFAULT_NTP_SERVER, NtpHandle, with_default_port};
use crate::time::{MILLIS_PER_TICK, TICKS_PER_WRAP, NtpDiscipline, TickStamp};

pub const AUDIT_MILLIS_PER_TICK: i64 = 50;
pub const AUDIT_TICKS_PER_WRAP: i64 = 48_000;
pub const AUDIT_HALF_TICK_MILLIS: i64 = 25;
pub const AUDIT_NTP_PORT: u16 = 123;

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct NtpAuditReport {
    pub quantization_ok: bool,
    pub wrapping_ok: bool,
    pub discipline_ok: bool,
    pub servers_ok: bool,
    pub failures: Vec<String>,
}

impl NtpAuditReport {
    #[must_use]
    pub fn passed(&self) -> bool {
        self.quantization_ok && self.wrapping_ok && self.discipline_ok && self.servers_ok
    }

    #[must_use]
    pub fn failure_count(&self) -> usize {
        self.failures.len()
    }
}

#[must_use]
pub fn audited_unix_millis_now() -> Option<i64> {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .ok()
        .and_then(|elapsed| i64::try_from(elapsed.as_millis()).ok())
}

#[must_use]
pub fn audited_tick_at(
    unix_millis: i64,
    offset_millis: Option<i64>,
) -> Option<TickStamp> {
    NtpDiscipline { offset_millis }.tick_at(unix_millis)
}

#[must_use]
pub fn audited_tick_now(handle: &NtpHandle) -> Option<TickStamp> {
    audited_unix_millis_now().and_then(|millis| handle.tick_now(millis))
}

#[must_use]
pub fn effective_ntp_servers(configured: &[String]) -> Vec<String> {
    if configured.is_empty() {
        vec![DEFAULT_NTP_SERVER.to_owned()]
    } else {
        configured.to_vec()
    }
}

#[must_use]
pub fn is_external_ntp_entry(entry: &str) -> bool {
    let trimmed = entry.trim();
    if trimmed.is_empty() {
        return false;
    }
    let host = trimmed.split(':').next().unwrap_or("");
    if host.is_empty() {
        return false;
    }
    let lower: String = host.to_lowercase();
    if lower == "localhost" {
        return false;
    }
    if lower.starts_with("127.") || lower == "127" {
        return false;
    }
    if lower.starts_with("10.") || lower.starts_with("192.168.") {
        return false;
    }
    if lower.starts_with("172.") {
        let mut parts = lower.split('.');
        let _ = parts.next();
        if let Some(second) = parts.next() {
            if let Ok(octet) = second.parse::<u8>() {
                if (16..=31).contains(&octet) {
                    return false;
                }
            }
        }
    }
    if lower == "::1" || lower.starts_with("fc") || lower.starts_with("fd") {
        return false;
    }
    host.contains('.')
}

#[must_use]
pub fn audit_ntp_wiring(
    configured: &[String],
    max_precision_millis: i64,
    probe_unix_millis: i64,
) -> NtpAuditReport {
    let mut failures: Vec<String> = Vec::new();

    let quantization_ok = if MILLIS_PER_TICK == AUDIT_MILLIS_PER_TICK
        && TickStamp::from_disciplined_millis(50, 0) == TickStamp(1)
        && TickStamp::from_disciplined_millis(0, 0) == TickStamp(0)
        && TickStamp::from_disciplined_millis(49, 0) == TickStamp(0)
        && TickStamp::from_disciplined_millis(99, 0) == TickStamp(1)
        && TickStamp::from_disciplined_millis(100, 0) == TickStamp(2)
    {
        true
    } else {
        failures.push(format!(
            "quantization drift millis_per_tick={MILLIS_PER_TICK} want {AUDIT_MILLIS_PER_TICK}"
        ));
        false
    };

    let full_wrap_millis = AUDIT_TICKS_PER_WRAP.saturating_mul(AUDIT_MILLIS_PER_TICK);
    let wrapping_ok = if TICKS_PER_WRAP == AUDIT_TICKS_PER_WRAP
        && TickStamp::from_disciplined_millis(full_wrap_millis, 0) == TickStamp(0)
        && TickStamp::from_disciplined_millis(full_wrap_millis.saturating_add(50), 0)
            == TickStamp(1)
        && TickStamp(1).distance_since(TickStamp(47_999)) == 2
    {
        true
    } else {
        failures.push(format!(
            "wrapping drift ticks_per_wrap={TICKS_PER_WRAP} want {AUDIT_TICKS_PER_WRAP}"
        ));
        false
    };

    let mut precision_ok = true;
    if max_precision_millis < 0 || max_precision_millis > AUDIT_HALF_TICK_MILLIS {
        failures.push(format!(
            "max precision {max_precision_millis} exceeds half-tick target {AUDIT_HALF_TICK_MILLIS}"
        ));
        precision_ok = false;
    }
    let undisciplined = NtpDiscipline::unconfigured();
    let mut healthy = NtpDiscipline::unconfigured();
    healthy.observe(0);
    let mut corrected = NtpDiscipline::unconfigured();
    corrected.observe(10_000);
    let want = TickStamp::from_disciplined_millis(probe_unix_millis, 0);
    let corrected_want = TickStamp::from_disciplined_millis(probe_unix_millis, 10_000);
    let discipline_ok = if precision_ok && undisciplined.tick_at(probe_unix_millis).is_none()
        && healthy.tick_at(probe_unix_millis) == Some(want)
        && corrected.tick_at(probe_unix_millis) == Some(corrected_want)
        && audited_tick_at(probe_unix_millis, None).is_none()
        && audited_tick_at(probe_unix_millis, Some(0)) == Some(want)
        && audited_tick_at(probe_unix_millis, Some(10_000)) == Some(corrected_want)
    {
        true
    } else {
        failures.push("discipline failed to apply a sampled clock correction".to_owned());
        false
    };

    let effective = effective_ntp_servers(configured);
    let mut servers_ok = true;
    if effective.is_empty() {
        servers_ok = false;
        failures.push("no ntp server remains after external fallback".to_owned());
    }
    if !is_external_ntp_entry(DEFAULT_NTP_SERVER) {
        servers_ok = false;
        failures.push(format!(
            "default ntp server {DEFAULT_NTP_SERVER} is not external"
        ));
    }
    for entry in &effective {
        if !is_external_ntp_entry(entry) {
            servers_ok = false;
            failures.push(format!("ntp server {entry} is not an external timeserver"));
        }
        let dial = with_default_port(entry);
        if !dial.contains(':') {
            servers_ok = false;
            failures.push(format!("ntp server {entry} dial string lacks a port"));
        }
        if let Some(port) = dial.rsplit(':').next() {
            let want_port = AUDIT_NTP_PORT.to_string();
            let has_port = entry.contains(':');
            if !has_port && port != want_port {
                servers_ok = false;
                failures.push(format!("ntp server {entry} missed default port {want_port}"));
            }
        }
    }

    NtpAuditReport {
        quantization_ok,
        wrapping_ok,
        discipline_ok,
        servers_ok,
        failures,
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn quantization_matches_twentieth_of_a_second() {
        assert_eq!(MILLIS_PER_TICK, 50);
        assert_eq!(TickStamp::from_disciplined_millis(0, 0), TickStamp(0));
        assert_eq!(TickStamp::from_disciplined_millis(49, 0), TickStamp(0));
        assert_eq!(TickStamp::from_disciplined_millis(50, 0), TickStamp(1));
        assert_eq!(TickStamp::from_disciplined_millis(99, 0), TickStamp(1));
        assert_eq!(TickStamp::from_disciplined_millis(100, 0), TickStamp(2));
    }

    #[test]
    fn stamps_wrap_at_two_minecraft_days() {
        assert_eq!(TICKS_PER_WRAP, 48_000);
        let full = 48_000_i64.saturating_mul(50);
        assert_eq!(TickStamp::from_disciplined_millis(full, 0), TickStamp(0));
        assert_eq!(
            TickStamp::from_disciplined_millis(full.saturating_add(50), 0),
            TickStamp(1)
        );
        assert_eq!(TickStamp(1).distance_since(TickStamp(47_999)), 2);
    }

    #[test]
    fn undisciplined_clocks_mint_nothing() {
        assert_eq!(audited_tick_at(1_000, None), None);
        assert_eq!(
            audited_tick_at(1_000, Some(0)),
            Some(TickStamp::from_disciplined_millis(1_000, 0))
        );
        assert_eq!(
            audited_tick_at(1_000, Some(10_000)),
            Some(TickStamp::from_disciplined_millis(1_000, 10_000))
        );
        assert_eq!(
            audited_tick_at(1_000, Some(-10_000)),
            Some(TickStamp::from_disciplined_millis(1_000, -10_000))
        );
    }

    #[test]
    fn handle_without_offset_mints_nothing() {
        let handle = NtpHandle::undisciplined(25);
        assert_eq!(audited_tick_now(&handle), None);
        assert_eq!(handle.tick_now(1_000), None);
    }

    #[test]
    fn empty_config_falls_back_to_external_default() {
        let effective = effective_ntp_servers(&[]);
        assert_eq!(effective, vec![DEFAULT_NTP_SERVER.to_owned()]);
        assert!(is_external_ntp_entry(DEFAULT_NTP_SERVER));
        assert!(is_external_ntp_entry("pool.ntp.org"));
        assert!(is_external_ntp_entry("time.cloudflare.com:123"));
        assert!(!is_external_ntp_entry("localhost"));
        assert!(!is_external_ntp_entry("127.0.0.1"));
        assert!(!is_external_ntp_entry("10.0.0.5"));
        assert!(!is_external_ntp_entry("192.168.1.7"));
        assert!(!is_external_ntp_entry(""));
    }

    #[test]
    fn audit_passes_on_external_config() {
        let report = audit_ntp_wiring(
            &[String::from("pool.ntp.org"), String::from("time.cloudflare.com")],
            25,
            1_700_000_000_123,
        );
        assert!(report.passed(), "failures {:?}", report.failures);
        assert!(report.failures.is_empty());
    }

    #[test]
    fn audit_rejects_full_tick_tolerance() {
        let report = audit_ntp_wiring(
            &[String::from("pool.ntp.org")],
            50,
            1_700_000_000_123,
        );
        assert!(!report.passed());
        assert!(!report.discipline_ok);
        assert!(report.quantization_ok);
        assert!(report.wrapping_ok);
        assert!(report.servers_ok);
    }

    #[test]
    fn audit_rejects_local_timeservers() {
        let report = audit_ntp_wiring(&[String::from("localhost")], 25, 1_000);
        assert!(!report.passed());
        assert!(!report.servers_ok);
        assert!(report.quantization_ok);
        assert!(report.wrapping_ok);
        assert!(report.discipline_ok);
    }

    #[test]
    fn audit_accepts_empty_config_through_fallback() {
        let report = audit_ntp_wiring(&[], 25, 1_000);
        assert!(report.servers_ok);
        assert!(report.passed(), "failures {:?}", report.failures);
    }
}
