use std::sync::atomic::{AtomicU64, Ordering};

pub const SLOW_FUSE_LOG_COOLDOWN_MILLIS: u64 = 60_000;

#[derive(Debug, Default)]
pub struct ClusterMetrics {
    updates_sent: AtomicU64,
    updates_dropped: AtomicU64,
    accepts_outstanding: AtomicU64,
    fuse_lag_warnings: AtomicU64,
    last_slow_fuse_log_millis: AtomicU64,
}

impl ClusterMetrics {
    #[must_use]
    pub fn new() -> Self {
        Self::default()
    }

    pub fn record_sent(&self, count: u64) {
        self.updates_sent.fetch_add(count, Ordering::Relaxed);
    }

    pub fn record_dropped(&self, count: u64) {
        self.updates_dropped.fetch_add(count, Ordering::Relaxed);
    }

    pub fn set_accepts_outstanding(&self, count: u64) {
        self.accepts_outstanding.store(count, Ordering::Relaxed);
    }

    pub fn record_fuse_lag_warning(&self) {
        self.fuse_lag_warnings.fetch_add(1, Ordering::Relaxed);
    }

    #[must_use]
    pub fn updates_sent(&self) -> u64 {
        self.updates_sent.load(Ordering::Relaxed)
    }

    #[must_use]
    pub fn updates_dropped(&self) -> u64 {
        self.updates_dropped.load(Ordering::Relaxed)
    }

    #[must_use]
    pub fn accepts_outstanding(&self) -> u64 {
        self.accepts_outstanding.load(Ordering::Relaxed)
    }

    #[must_use]
    pub fn fuse_lag_warnings(&self) -> u64 {
        self.fuse_lag_warnings.load(Ordering::Relaxed)
    }

    #[must_use]
    pub fn snapshot(&self) -> ClusterSnapshot {
        ClusterSnapshot {
            updates_sent: self.updates_sent(),
            updates_dropped: self.updates_dropped(),
            accepts_outstanding: self.accepts_outstanding(),
            fuse_lag_warnings: self.fuse_lag_warnings(),
        }
    }

    pub fn should_log_slow_fuse(&self, now_millis: u64) -> bool {
        let last = self.last_slow_fuse_log_millis.load(Ordering::Relaxed);
        if now_millis.saturating_sub(last) < SLOW_FUSE_LOG_COOLDOWN_MILLIS {
            return false;
        }
        self.last_slow_fuse_log_millis
            .compare_exchange(last, now_millis, Ordering::Relaxed, Ordering::Relaxed)
            .is_ok()
    }
}

#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub struct ClusterSnapshot {
    pub updates_sent: u64,
    pub updates_dropped: u64,
    pub accepts_outstanding: u64,
    pub fuse_lag_warnings: u64,
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn counters_accumulate() {
        let metrics = ClusterMetrics::new();
        metrics.record_sent(3);
        metrics.record_dropped(1);
        metrics.set_accepts_outstanding(7);
        metrics.record_fuse_lag_warning();
        let snapshot = metrics.snapshot();
        assert_eq!(snapshot.updates_sent, 3);
        assert_eq!(snapshot.updates_dropped, 1);
        assert_eq!(snapshot.accepts_outstanding, 7);
        assert_eq!(snapshot.fuse_lag_warnings, 1);
    }

    #[test]
    fn slow_fuse_log_has_cooldown() {
        let metrics = ClusterMetrics::new();
        assert!(metrics.should_log_slow_fuse(SLOW_FUSE_LOG_COOLDOWN_MILLIS));
        assert!(!metrics.should_log_slow_fuse(SLOW_FUSE_LOG_COOLDOWN_MILLIS + 1));
        assert!(metrics.should_log_slow_fuse(2 * SLOW_FUSE_LOG_COOLDOWN_MILLIS));
    }

    #[test]
    fn slow_fuse_log_suppresses_within_cooldown() {
        let metrics = ClusterMetrics::new();
        assert!(!metrics.should_log_slow_fuse(0));
        assert!(!metrics.should_log_slow_fuse(SLOW_FUSE_LOG_COOLDOWN_MILLIS - 1));
    }
}
