//! NTP clock discipline: keeps every peer on the same wall-clock page.
//!
//! Pipeline per poll: UDP query each configured server, take the median
//! offset, smooth it through a divide-by-4 low-pass filter, then publish it
//! over a [`tokio::sync::watch`] channel. [`NtpHandle`] clones share that
//! channel, so [`NtpHandle::tick_now`] mints the same [`TickStamp`] on every
//! peer with a sampled offset (see [`crate::time`]).

use std::net::SocketAddr;
use std::sync::atomic::{AtomicI64, Ordering};
use std::time::{Duration, SystemTime, UNIX_EPOCH};

use tokio::{net::UdpSocket, sync::watch, time};

use crate::time::{NtpDiscipline, TickStamp};

/// How often [`NtpSync::run`] re-polls the server list.
pub const POLL_INTERVAL: Duration = Duration::from_secs(30);
/// Per-server UDP query deadline; slow servers are skipped, not waited on.
pub const QUERY_TIMEOUT: Duration = Duration::from_secs(2);
/// NTP datagram length in bytes (header plus four 64-bit timestamps).
pub const NTP_PACKET_LEN: usize = 48;
/// LI=0, VN=3, Mode=3 (client) first byte of an outgoing request.
pub const NTP_CLIENT_REQUEST: u8 = 0x1B;
/// Seconds between the NTP era (1900) and the Unix epoch (1970).
pub const NTP_UNIX_EPOCH_OFFSET_SECS: i64 = 2_208_988_800;
/// UDP port appended when a server string carries no explicit `:port`.
pub const NTP_DEFAULT_PORT: u16 = 123;
/// Low-pass divisor: each median sample moves the filter 1/4 toward itself.
pub const FILTER_DIVISOR: i64 = 4;
pub const DEFAULT_NTP_SERVER: &str = "pool.ntp.org";
pub const POLL_RETRY_INTERVAL: Duration = Duration::from_secs(5);
pub const QUERY_ADDR_TIMEOUT: Duration = Duration::from_millis(800);
pub const QUERY_RECV_LEN: usize = 512;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct NtpSample {
    pub offset_millis: i64,
    pub precision_millis: i64,
}

static SHARED_OFFSET_MILLIS: AtomicI64 = AtomicI64::new(i64::MAX);
static SHARED_PRECISION_MILLIS: AtomicI64 = AtomicI64::new(i64::MAX);
static SHARED_PRECISION_TARGET_MILLIS: AtomicI64 = AtomicI64::new(i64::MAX);

#[cfg(test)]
pub(crate) static SHARED_OFFSET_SERIAL: std::sync::Mutex<()> =
    std::sync::Mutex::new(());

#[must_use]
pub fn shared_offset_millis() -> Option<i64> {
    match SHARED_OFFSET_MILLIS.load(Ordering::Relaxed) {
        i64::MAX => None,
        offset => Some(offset),
    }
}

pub fn publish_shared_offset(offset: i64) {
    SHARED_OFFSET_MILLIS.store(offset, Ordering::Relaxed);
}

pub fn publish_shared_sample(offset: i64, precision: i64) {
    publish_shared_offset(offset);
    SHARED_PRECISION_MILLIS.store(precision, Ordering::Relaxed);
}

#[must_use]
pub fn shared_precision_millis() -> Option<i64> {
    match SHARED_PRECISION_MILLIS.load(Ordering::Relaxed) {
        i64::MAX => None,
        precision => Some(precision),
    }
}

#[must_use]
pub fn shared_precision_target_millis() -> Option<i64> {
    match SHARED_PRECISION_TARGET_MILLIS.load(Ordering::Relaxed) {
        i64::MAX => None,
        target => Some(target),
    }
}

pub fn withdraw_shared_offset() {
    SHARED_OFFSET_MILLIS.store(i64::MAX, Ordering::Relaxed);
    SHARED_PRECISION_MILLIS.store(i64::MAX, Ordering::Relaxed);
}

/// Which NTP servers to poll and the target sample precision.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct NtpConfig {
    /// Hostnames or `host:port` endpoints polled every [`POLL_INTERVAL`].
    pub servers: Vec<String>,
    /// Maximum half-round-trip in millis for a precise sample.
    pub max_precision_millis: i64,
}

impl NtpConfig {
    /// Configures the server list plus the sample-precision target.
    #[must_use]
    pub fn new(servers: Vec<String>, max_precision_millis: i64) -> Self {
        Self {
            servers,
            max_precision_millis,
        }
    }
}

/// Cheap cloneable reader of the disciplined offset; every peer polls alike.
///
/// `NtpHandle` borrows the watch channel fed by [`NtpSync`]. [`NtpHandle::tick_now`]
/// applies that offset and quantizes to 1/20 s, so healthy peers stamp the
/// same instant with the same wrapping [`u16`] (see [`crate::time::TickStamp`]).
#[derive(Debug, Clone)]
pub struct NtpHandle {
    samples: watch::Receiver<Option<NtpSample>>,
    max_precision_millis: i64,
}

impl NtpHandle {
    /// Starts undisciplined (`offset() == None`): stamps gate to `None`.
    #[must_use]
    pub fn undisciplined(max_precision_millis: i64) -> Self {
        let (_, receiver) = watch::channel(None);
        Self {
            samples: receiver,
            max_precision_millis,
        }
    }

    /// Latest filtered offset in millis (`server - local`), if disciplined.
    #[must_use]
    pub fn offset(&self) -> Option<i64> {
        self.samples.borrow().as_ref().map(|sample| sample.offset_millis)
    }

    #[must_use]
    pub fn precision_millis(&self) -> Option<i64> {
        self.samples
            .borrow()
            .as_ref()
            .map(|sample| sample.precision_millis)
    }

    /// Maximum half-round-trip in millis for a precise sample.
    #[must_use]
    pub fn max_precision_millis(&self) -> i64 {
        self.max_precision_millis
    }

    /// Stamps `unix_millis` with the live offset, or `None` when undisciplined/unhealthy.
    #[must_use]
    pub fn tick_now(&self, unix_millis: i64) -> Option<TickStamp> {
        NtpDiscipline {
            offset_millis: self.offset(),
        }
        .tick_at(unix_millis)
    }

    /// True once an NTP sample exists within the configured precision target.
    #[must_use]
    pub fn is_precise(&self) -> bool {
        self.precision_millis().is_some_and(|precision| {
            precision <= self.max_precision_millis && self.max_precision_millis >= 0
        })
    }
}

/// Polling half of the discipline pair: owns the servers and the watch sender.
///
/// [`NtpSync::new`] returns the poller plus its first [`NtpHandle`]; clone the
/// handle freely. [`NtpSync::refresh`] performs one median-plus-filter round,
/// [`NtpSync::run`] repeats it every [`POLL_INTERVAL`].
#[derive(Debug)]
pub struct NtpSync {
    servers: Vec<String>,
    sender: watch::Sender<Option<NtpSample>>,
    discipline: NtpDiscipline,
    filtered: Option<i64>,
}

impl NtpSync {
    /// Splits a fresh undisciplined poller/handle pair from one config.
    #[must_use]
    pub fn new(config: NtpConfig) -> (Self, NtpHandle) {
        let max_precision_millis = config.max_precision_millis;
        SHARED_PRECISION_TARGET_MILLIS.store(max_precision_millis, Ordering::Relaxed);
        let (sender, receiver) = watch::channel(None);
        let sync = Self {
            servers: config.servers,
            sender,
            discipline: NtpDiscipline::unconfigured(),
            filtered: None,
        };
        let handle = NtpHandle {
            samples: receiver,
            max_precision_millis,
        };
        (sync, handle)
    }

    /// Polls forever, publishing each filtered offset to all handles.
    pub async fn run(mut self) {
        loop {
            let answered = self.refresh().await;
            time::sleep(if answered {
                POLL_INTERVAL
            } else {
                POLL_RETRY_INTERVAL
            })
            .await;
        }
    }

    /// Queries every server, then median-filters into the shared offset.
    ///
    /// A round with zero answers keeps the previous offset rather than
    /// flapping back to undisciplined, so peers ride out short outages together.
    pub async fn refresh(&mut self) -> bool {
        let mut samples = Vec::with_capacity(self.servers.len());
        for server in self.servers.clone() {
            if let Some(sample) = query_server(&server, QUERY_TIMEOUT).await {
                samples.push(sample);
            }
        }
        match median_offset(&mut samples) {
            None => {
                tracing::warn!(
                    servers = self.servers.len(),
                    "ntp poll answered by no server; keeping previous offset"
                );
                false
            }
            Some(median) => {
                let next = low_pass_filter(self.filtered, median.offset_millis);
                self.filtered = Some(next);
                publish_shared_sample(next, median.precision_millis);
                self.discipline.observe(next);
                let sample = NtpSample {
                    offset_millis: next,
                    precision_millis: median.precision_millis,
                };
                if self.sender.send(Some(sample)).is_err() {
                    tracing::debug!("ntp offset updated with no listeners");
                } else {
                    tracing::debug!(
                        offset_millis = next,
                        precision_millis = median.precision_millis,
                        "ntp offset updated"
                    );
                }
                true
            }
        }
    }

    /// Latest filtered offset in millis, if any poll has succeeded.
    #[must_use]
    pub fn offset(&self) -> Option<i64> {
        self.filtered
    }
}

/// Appends `:123` unless the server string already names a port.
#[must_use]
pub fn with_default_port(server: &str) -> String {
    if let Some(stripped) = server.strip_prefix('[') {
        if let Some(end) = stripped.find(']') {
            let rest = &stripped[end + 1..];
            if rest.starts_with(':') && rest.len() > 1 {
                return server.to_owned();
            }
            return format!("{server}:{NTP_DEFAULT_PORT}");
        }
        return format!("{server}:{NTP_DEFAULT_PORT}");
    }
    let colon_count = server.bytes().filter(|byte| *byte == b':').count();
    if colon_count == 0 {
        return format!("{server}:{NTP_DEFAULT_PORT}");
    }
    if colon_count == 1 {
        if let Some(port) = server.rsplit(':').next() {
            if !port.is_empty() && port.bytes().all(|byte| byte.is_ascii_digit()) {
                return server.to_owned();
            }
        }
        return format!("{server}:{NTP_DEFAULT_PORT}");
    }
    format!("[{server}]:{NTP_DEFAULT_PORT}")
}

/// Splits Unix millis into the NTP seconds/fraction word pair.
///
/// Seconds shift by [`NTP_UNIX_EPOCH_OFFSET_SECS`]; the sub-second remainder
/// scales by `2^32 / 1000` with half-up rounding, saturating at [`u32::MAX`].
#[must_use]
pub fn unix_millis_to_ntp_timestamp(millis: i64) -> (u32, u32) {
    let seconds = millis.div_euclid(1000);
    let sub_second = millis.rem_euclid(1000);
    let ntp_seconds = seconds.saturating_add(NTP_UNIX_EPOCH_OFFSET_SECS);
    let whole = u32::try_from(ntp_seconds).unwrap_or(u32::MAX);
    let ticks =
        u64::from(u32::try_from(sub_second).unwrap_or(0))
            .saturating_mul(4_294_967_296_u64)
            .saturating_add(500)
            / 1000;
    let fraction = u32::try_from(ticks).unwrap_or(u32::MAX);
    (whole, fraction)
}

/// Joins an NTP seconds/fraction pair back into Unix millis.
///
/// Inverts [`unix_millis_to_ntp_timestamp`] within +-1 ms; saturates instead
/// of wrapping on far-future eras.
#[must_use]
pub fn ntp_timestamp_to_unix_millis(seconds: u32, fraction: u32) -> i64 {
    let unix_seconds = i64::from(seconds).saturating_sub(NTP_UNIX_EPOCH_OFFSET_SECS);
    let whole = unix_seconds.saturating_mul(1000);
    let sub_second = i64::from(fraction)
        .saturating_mul(1000)
        .saturating_add(1_i64 << 31)
        >> 32;
    whole.saturating_add(sub_second)
}

/// Builds a 48-byte client request carrying the transmit stamp in bytes 40..48.
#[must_use]
pub fn build_request_packet(transmit_unix_millis: i64) -> [u8; NTP_PACKET_LEN] {
    let (seconds, fraction) = unix_millis_to_ntp_timestamp(transmit_unix_millis);
    let transmit = (u64::from(seconds) << 32) | u64::from(fraction);
    let tail = transmit.to_be_bytes();
    let mut packet = [0_u8; NTP_PACKET_LEN];
    let prefix = NTP_PACKET_LEN - tail.len();
    for (index, byte) in tail.iter().enumerate() {
        if let Some(slot) = packet.get_mut(prefix + index) {
            *slot = *byte;
        }
    }
    if let Some(mode) = packet.first_mut() {
        *mode = NTP_CLIENT_REQUEST;
    }
    packet
}

/// Reads `(server_recv_millis, server_tx_millis)` from bytes 32..48.
///
/// Returns `None` on short packets; the caller still needs the two client-side
/// stamps to solve for the offset.
#[must_use]
pub fn parse_server_times(packet: &[u8]) -> Option<(i64, i64)> {
    let word = |chunk: &[u8]| -> Option<u32> { chunk.try_into().ok().map(u32::from_be_bytes) };
    let window = packet.get(32..48)?;
    let mut chunks = window.chunks_exact(4);
    let receive_seconds = word(chunks.next()?)?;
    let receive_fraction = word(chunks.next()?)?;
    let transmit_seconds = word(chunks.next()?)?;
    let transmit_fraction = word(chunks.next()?)?;
    Some((
        ntp_timestamp_to_unix_millis(receive_seconds, receive_fraction),
        ntp_timestamp_to_unix_millis(transmit_seconds, transmit_fraction),
    ))
}

/// Solves the classic four-timestamp offset: `((recv - sent) + (tx - arrived)) / 2`.
///
/// Symmetric network delay cancels out; asymmetric legs leave half the
/// asymmetry, which the median-plus-low-pass stages above then smooth away.
#[must_use]
pub fn compute_offset(
    client_send_ms: i64,
    server_recv_ms: i64,
    server_tx_ms: i64,
    client_recv_ms: i64,
) -> i64 {
    let inbound = server_recv_ms.saturating_sub(client_send_ms);
    let outbound = server_tx_ms.saturating_sub(client_recv_ms);
    inbound.saturating_add(outbound) / 2
}

/// Middle sample of the per-server offsets; even counts average the two middles.
///
/// `None` on zero samples, which tells [`NtpSync::refresh`] to hold the last
/// offset. The median rejects a single stray server so peers converge.
#[must_use]
pub fn median_offset(values: &mut [NtpSample]) -> Option<NtpSample> {
    if values.is_empty() {
        return None;
    }
    values.sort_unstable_by_key(|sample| sample.offset_millis);
    let middle = values.len() / 2;
    if values.len() == middle.saturating_mul(2) {
        let upper = values.get(middle).copied()?;
        let lower = values.get(middle.saturating_sub(1)).copied()?;
        Some(NtpSample {
            offset_millis: lower
                .offset_millis
                .saturating_add(upper.offset_millis.saturating_sub(lower.offset_millis) / 2),
            precision_millis: lower.precision_millis.max(upper.precision_millis),
        })
    } else {
        values.get(middle).copied()
    }
}

/// Seeds on the first sample, then steps 1/4 toward each new median.
///
/// The gentle approach keeps shared stamps stable when one poll jitters.
#[must_use]
pub fn low_pass_filter(previous: Option<i64>, sample: i64) -> i64 {
    match previous {
        None => sample,
        Some(prior) => prior.saturating_add(sample.saturating_sub(prior) / FILTER_DIVISOR),
    }
}

/// Wall-clock Unix millis now, or `None` before the Unix epoch / on overflow.
fn unix_millis_now() -> Option<i64> {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .ok()
        .and_then(|elapsed| i64::try_from(elapsed.as_millis()).ok())
}

/// One bounded UDP exchange against a single server (never panics, `None` on any failure).
async fn query_server(server: &str, timeout: Duration) -> Option<NtpSample> {
    time::timeout(timeout, exchange(server)).await.ok()?
}

/// Sends one request and solves the offset from the four timestamps.
async fn exchange(server: &str) -> Option<NtpSample> {
    let target = with_default_port(server);
    let addrs: Vec<SocketAddr> = match target.parse() {
        Ok(addr) => vec![addr],
        Err(_) => tokio::net::lookup_host(target).await.ok()?.collect(),
    };
    if addrs.is_empty() {
        return None;
    }
    let mut samples = Vec::with_capacity(addrs.len());
    for addr in addrs {
        if let Ok(Some(offset)) = time::timeout(QUERY_ADDR_TIMEOUT, exchange_with(addr)).await {
            samples.push(offset);
        }
    }
    median_offset(&mut samples)
}

async fn exchange_with(target: SocketAddr) -> Option<NtpSample> {
    let bind = if target.is_ipv4() {
        "0.0.0.0:0"
    } else {
        "[::]:0"
    };
    let socket = UdpSocket::bind(bind).await.ok()?;
    socket.connect(target).await.ok()?;
    let sent_millis = unix_millis_now()?;
    let request = build_request_packet(sent_millis);
    socket.send(&request).await.ok()?;
    let mut buffer = [0_u8; QUERY_RECV_LEN];
    let len = socket.recv(&mut buffer).await.ok()?;
    if len < NTP_PACKET_LEN {
        return None;
    }
    let received_millis = unix_millis_now()?;
    let (server_recv, server_tx) = parse_server_times(&buffer)?;
    let offset_millis = compute_offset(sent_millis, server_recv, server_tx, received_millis);
    let round_trip = received_millis
        .saturating_sub(sent_millis)
        .saturating_sub(server_tx.saturating_sub(server_recv))
        .max(0);
    Some(NtpSample {
        offset_millis,
        precision_millis: round_trip.saturating_add(1) / 2,
    })
}

#[cfg(test)]
mod tests {
    use super::*;
    use tokio::net::UdpSocket;

    #[test]
    fn unix_epoch_maps_to_ntp_offset() {
        assert_eq!(unix_millis_to_ntp_timestamp(0), (2_208_988_800, 0));
        assert_eq!(ntp_timestamp_to_unix_millis(2_208_988_800, 0), 0);
    }

    #[test]
    fn timestamps_roundtrip_within_a_millis() {
        for stamp in [0_i64, 1, 999, 1_000, 1_700_000_000_123, 2_000_000_000_999] {
            let (seconds, fraction) = unix_millis_to_ntp_timestamp(stamp);
            let back = ntp_timestamp_to_unix_millis(seconds, fraction);
            assert!(
                (back - stamp).abs() <= 1,
                "roundtrip drift for {stamp}: got {back}"
            );
        }
    }

    #[test]
    fn request_packet_carries_transmit_stamp() {
        let packet = build_request_packet(1_700_000_000_000);
        assert_eq!(packet.len(), NTP_PACKET_LEN);
        assert_eq!(packet[0], NTP_CLIENT_REQUEST);
        assert!(packet[1..40].iter().all(|byte| *byte == 0));
        let transmit = u64::from_be_bytes(packet[40..48].try_into().unwrap());
        let seconds = (transmit >> 32) as u32;
        let fraction = transmit as u32;
        let back = ntp_timestamp_to_unix_millis(seconds, fraction);
        assert!((back - 1_700_000_000_000).abs() <= 1);
    }

    #[test]
    fn response_parse_reads_server_times() {
        let (recv_secs, recv_frac) = unix_millis_to_ntp_timestamp(1_000);
        let (tx_secs, tx_frac) = unix_millis_to_ntp_timestamp(1_020);
        let mut packet = [0_u8; NTP_PACKET_LEN];
        packet[32..36].copy_from_slice(&recv_secs.to_be_bytes());
        packet[36..40].copy_from_slice(&recv_frac.to_be_bytes());
        packet[40..44].copy_from_slice(&tx_secs.to_be_bytes());
        packet[44..48].copy_from_slice(&tx_frac.to_be_bytes());
        assert_eq!(parse_server_times(&packet), Some((1_000, 1_020)));
        assert_eq!(parse_server_times(&[0_u8; 10]), None);
    }

    #[test]
    fn symmetric_delay_cancels_out() {
        assert_eq!(compute_offset(1_000, 1_010, 1_015, 1_025), 0);
    }

    #[test]
    fn fast_server_clock_shows_positive_offset() {
        assert_eq!(compute_offset(1_000, 1_110, 1_110, 1_020), 100);
    }

    #[test]
    fn median_picks_middle() {
        let sample = |offset_millis| NtpSample {
            offset_millis,
            precision_millis: 3,
        };
        let mut values = [sample(30), sample(10), sample(20)];
        assert_eq!(median_offset(&mut values), Some(sample(20)));
        let mut even = [sample(40), sample(10), sample(30), sample(20)];
        assert_eq!(
            median_offset(&mut even),
            Some(NtpSample {
                offset_millis: 25,
                precision_millis: 3,
            })
        );
        let mut single = [sample(7)];
        assert_eq!(median_offset(&mut single), Some(sample(7)));
        let mut empty: [NtpSample; 0] = [];
        assert_eq!(median_offset(&mut empty), None);
    }

    #[test]
    fn filter_seeds_then_averages() {
        assert_eq!(low_pass_filter(None, 100), 100);
        assert_eq!(low_pass_filter(Some(0), 100), 25);
        assert_eq!(low_pass_filter(Some(100), 100), 100);
    }

    #[test]
    fn atomic_offset_feeds_tick_stamp_now() {
        let _serial = SHARED_OFFSET_SERIAL.lock().unwrap();
        publish_shared_offset(100);
        assert_eq!(shared_offset_millis(), Some(100));
        let millis = unix_millis_now().unwrap();
        let expected = TickStamp::from_disciplined_millis(millis, 100);
        assert!(TickStamp::now().distance_since(expected) <= 1);
    }

    #[tokio::test]
    async fn refresh_large_offset_publishes_the_correction() {
        let _serial = SHARED_OFFSET_SERIAL.lock().unwrap();
        publish_shared_offset(40);
        let responder = UdpSocket::bind("127.0.0.1:0").await.unwrap();
        let addr = responder.local_addr().unwrap();
        tokio::spawn(async move {
            let mut buffer = [0_u8; NTP_PACKET_LEN];
            if let Ok((len, peer)) = responder.recv_from(&mut buffer).await {
                if len == NTP_PACKET_LEN {
                    let mut reply = [0_u8; NTP_PACKET_LEN];
                    reply[0] = 0x1C;
                    if let (Some(origin), Some(slots)) =
                        (buffer.get(40..48), reply.get_mut(32..48))
                    {
                        let transmit = u64::from_be_bytes(origin.try_into().unwrap());
                        let origin_millis = ntp_timestamp_to_unix_millis(
                            (transmit >> 32) as u32,
                            transmit as u32,
                        );
                        let (skew_secs, skew_frac) =
                            unix_millis_to_ntp_timestamp(origin_millis + 10_000_000);
                        let mut skew = [0_u8; 8];
                        skew[0..4].copy_from_slice(&skew_secs.to_be_bytes());
                        skew[4..8].copy_from_slice(&skew_frac.to_be_bytes());
                        if let Some(recv) = slots.get_mut(0..8) {
                            recv.copy_from_slice(&skew);
                        }
                        if let Some(tx) = slots.get_mut(8..16) {
                            tx.copy_from_slice(&skew);
                        }
                    }
                    let _ = responder.send_to(&reply, peer).await;
                }
            }
        });
        let (mut sync, handle) =
            NtpSync::new(NtpConfig::new(vec![format!("{addr}")], 250));
        sync.refresh().await;
        let offset = handle.offset().unwrap();
        assert!(offset > 9_000_000);
        assert_eq!(shared_offset_millis(), Some(offset));
        assert_eq!(
            handle.tick_now(1_000),
            Some(TickStamp::from_disciplined_millis(1_000, offset))
        );
        assert!(handle.is_precise());
    }

    #[test]
    fn default_port_applies_only_without_port() {
        assert_eq!(with_default_port("time.example"), "time.example:123");
        assert_eq!(with_default_port("time.example:456"), "time.example:456");
    }

    #[test]
    fn undisciplined_handle_yields_nothing() {
        let handle = NtpHandle::undisciplined(250);
        assert_eq!(handle.offset(), None);
        assert!(!handle.is_precise());
        assert_eq!(handle.tick_now(1_000), None);
    }

    #[test]
    fn sync_pair_starts_undisciplined() {
        let (sync, handle) = NtpSync::new(NtpConfig::new(Vec::new(), 250));
        assert_eq!(sync.offset(), None);
        assert_eq!(handle.offset(), None);
        assert!(!handle.is_precise());
    }

    #[tokio::test]
    async fn refresh_without_servers_keeps_none() {
        let (mut sync, handle) = NtpSync::new(NtpConfig::new(Vec::new(), 250));
        sync.refresh().await;
        assert_eq!(sync.offset(), None);
        assert_eq!(handle.offset(), None);
    }

    #[tokio::test]
    async fn refresh_against_local_responder_disciplines() {
        let _serial = SHARED_OFFSET_SERIAL.lock().unwrap();
        let responder = UdpSocket::bind("127.0.0.1:0").await.unwrap();
        let addr = responder.local_addr().unwrap();
        tokio::spawn(async move {
            let mut buffer = [0_u8; NTP_PACKET_LEN];
            if let Ok((len, peer)) = responder.recv_from(&mut buffer).await {
                if len == NTP_PACKET_LEN {
                    let mut reply = [0_u8; NTP_PACKET_LEN];
                    reply[0] = 0x1C;
                    if let (Some(origin), Some(slots)) =
                        (buffer.get(40..48), reply.get_mut(32..48))
                    {
                        if let Some(recv) = slots.get_mut(0..8) {
                            recv.copy_from_slice(origin);
                        }
                        if let Some(tx) = slots.get_mut(8..16) {
                            tx.copy_from_slice(origin);
                        }
                    }
                    let _ = responder.send_to(&reply, peer).await;
                }
            }
        });
        let (mut sync, handle) =
            NtpSync::new(NtpConfig::new(vec![format!("{addr}")], 250));
        sync.refresh().await;
        let offset = handle.offset().unwrap();
        assert!(
            offset.unsigned_abs() <= 5_000,
            "local offset out of range: {offset}"
        );
        assert!(handle.is_precise());
        assert!(handle.tick_now(1_000).is_some());
    }
}
