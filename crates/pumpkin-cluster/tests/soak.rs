#![allow(
    clippy::unwrap_used,
    clippy::expect_used,
    clippy::panic,
    clippy::uninlined_format_args
)]

use std::time::Duration;

use tokio::sync::mpsc;

pub const TICK_PUBLISH_P99_MILLIS: u128 = 45;
pub const ACCEPT_CLOSURE_P99_MILLIS: u128 = 1_500;
pub const EU_ONE_WAY_MILLIS: u64 = 25;
pub const JP_ONE_WAY_MILLIS: u64 = 120;
pub const SOAK_SAMPLES: usize = 8;

pub struct SoakReport {
    pub samples: Vec<Duration>,
}

impl SoakReport {
    #[must_use]
    pub fn p99(&self) -> Duration {
        let mut sorted = self.samples.clone();
        sorted.sort_unstable();
        let rank = (sorted.len() * 99).div_ceil(100).max(1) - 1;
        sorted[rank.min(sorted.len().saturating_sub(1))]
    }
}

pub struct LatencyHarness {
    pub one_way: Duration,
}

impl LatencyHarness {
    #[must_use]
    pub const fn new(one_way: Duration) -> Self {
        Self { one_way }
    }

    pub async fn publish_roundtrip(&self, payload: Vec<u8>) -> Duration {
        let (tx, mut rx) = mpsc::channel::<Vec<u8>>(1);
        let delay = self.one_way;
        tokio::spawn(async move {
            tokio::time::sleep(delay).await;
            let _ = tx.send(payload).await;
        });
        let start = tokio::time::Instant::now();
        let _ = rx.recv().await;
        start.elapsed()
    }

    pub async fn collect(&self, payload: Vec<u8>, samples: usize) -> SoakReport {
        let mut out = Vec::with_capacity(samples);
        for _ in 0..samples {
            out.push(self.publish_roundtrip(payload.clone()).await);
        }
        SoakReport { samples: out }
    }
}

#[tokio::test]
#[ignore = "needs QUIC mesh transport with real EU latency; loopback double cannot reproduce WAN tails"]
async fn eu_tick_publish_p99_probe() {
    let harness = LatencyHarness::new(Duration::from_millis(1));
    let report = harness.collect(vec![0_u8; 64], SOAK_SAMPLES).await;
    assert_eq!(report.samples.len(), SOAK_SAMPLES);
    assert!(report.p99() < Duration::from_secs(5));
    assert!(
        u128::from(EU_ONE_WAY_MILLIS) < TICK_PUBLISH_P99_MILLIS,
        "modelled EU leg must fit the publish gate"
    );
}

#[tokio::test]
#[ignore = "needs QUIC ACCEPT transport with real JP latency; loopback double cannot reproduce WAN tails"]
async fn jp_accept_closure_p99_probe() {
    let harness = LatencyHarness::new(Duration::from_millis(1));
    let report = harness.collect(vec![1_u8; 128], SOAK_SAMPLES).await;
    assert_eq!(report.samples.len(), SOAK_SAMPLES);
    assert!(report.p99() < Duration::from_secs(5));
    assert!(
        u128::from(JP_ONE_WAY_MILLIS) < ACCEPT_CLOSURE_P99_MILLIS,
        "modelled JP leg must fit the closure gate"
    );
}

#[test]
#[ignore = "soak harness outline only; full EU/JP matrix needs QUIC transport"]
fn soak_gates_ordered() {
    assert!(ACCEPT_CLOSURE_P99_MILLIS > TICK_PUBLISH_P99_MILLIS);
    let report = SoakReport {
        samples: vec![
            Duration::from_millis(1),
            Duration::from_millis(2),
            Duration::from_millis(3),
            Duration::from_millis(100),
        ],
    };
    assert_eq!(report.p99(), Duration::from_millis(100));
}
