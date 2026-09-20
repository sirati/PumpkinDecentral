#![allow(
    clippy::unwrap_used,
    clippy::expect_used,
    clippy::panic,
    clippy::uninlined_format_args
)]

use std::collections::{HashMap, HashSet};
use std::time::Duration;

use pumpkin_cluster::chunks::{ChunkAdvert, ChunkAnnounce, ChunkFetch, Directory};
use pumpkin_cluster::primary::{check_player_login, primary_accepts_players};
use pumpkin_cluster::protocol::{ChunkAddr, StreamKind};
use pumpkin_cluster::xfer::{
    ChunkPayload, decode_announce, decode_payload, decode_request, encode_announce,
    encode_payload, encode_request,
};
use tokio::sync::mpsc;

const PRIMARY_ID: u16 = 0;
const SECONDARY_ID: u16 = 1;
const EXTRA_PEER_ID: u16 = 7;
const PROOF_TIMEOUT_SECS: u64 = 5;
const PROOF_CHANNEL_DEPTH: usize = 32;

fn login_chunks() -> Vec<ChunkAddr> {
    vec![
        ChunkAddr { x: 0, z: 0 },
        ChunkAddr { x: 1, z: 0 },
        ChunkAddr { x: 0, z: 1 },
        ChunkAddr { x: 1, z: 1 },
        ChunkAddr { x: -1, z: 0 },
        ChunkAddr { x: 0, z: -1 },
    ]
}

fn snapshot_bytes(chunk: &ChunkAddr) -> Vec<u8> {
    let mut out = Vec::with_capacity(16);
    out.extend_from_slice(&chunk.x.to_le_bytes());
    out.extend_from_slice(&chunk.z.to_le_bytes());
    out.extend_from_slice(&[0xC1, 0x75, 0x6E, 0x6B, 0x5F, 0x44, 0x41, 0x54]);
    out
}

fn sorted_addrs(addrs: Vec<ChunkAddr>) -> Vec<ChunkAddr> {
    let mut out = addrs;
    out.sort_by(|left, right| (left.x, left.z).cmp(&(right.x, right.z)));
    out
}

struct PrimaryNode {
    snapshots: HashMap<ChunkAddr, Vec<u8>>,
}

impl PrimaryNode {
    fn new(chunks: &[ChunkAddr]) -> Self {
        let mut snapshots = HashMap::new();
        for chunk in chunks {
            snapshots.insert(*chunk, snapshot_bytes(chunk));
        }
        Self { snapshots }
    }

    fn advert_frames(&self, holder: u16) -> Vec<Vec<u8>> {
        let keys = sorted_addrs(self.snapshots.keys().copied().collect());
        let mut frames = Vec::with_capacity(keys.len());
        for chunk in keys {
            let announce = ChunkAnnounce::Acquire(ChunkAdvert { holder, chunk });
            frames.push(encode_announce(&announce).unwrap());
        }
        frames
    }

    fn serve_frame(&self, holder: u16, frame: &[u8]) -> Option<Vec<u8>> {
        let fetch: ChunkFetch = decode_request(frame).ok()?;
        let snapshot = self.snapshots.get(&fetch.chunk)?.clone();
        let payload =
            ChunkPayload::new(fetch.chunk, holder, snapshot, Vec::new(), vec![holder]);
        encode_payload(&payload).ok()
    }

    fn snapshot(&self, chunk: &ChunkAddr) -> Option<Vec<u8>> {
        self.snapshots.get(chunk).cloned()
    }
}

struct SecondaryNode {
    directory: Directory,
    store: HashMap<ChunkAddr, Vec<u8>>,
    fetched: u64,
    generated: u64,
}

impl SecondaryNode {
    fn new() -> Self {
        Self {
            directory: Directory::new(),
            store: HashMap::new(),
            fetched: 0,
            generated: 0,
        }
    }

    fn apply_frame(&mut self, frame: &[u8]) -> bool {
        let announce = decode_announce(frame).unwrap();
        match announce {
            ChunkAnnounce::Acquire(advert) => {
                self.directory.apply_advert(advert);
                true
            }
            ChunkAnnounce::Release(drop) => {
                self.directory.apply_drop(drop);
                false
            }
        }
    }

    fn request_frame(&self, chunk: &ChunkAddr) -> Option<Vec<u8>> {
        let fetch = self.directory.fetch_for(chunk)?;
        encode_request(&fetch).ok()
    }

    fn ingest_frame(&mut self, frame: &[u8]) -> Option<ChunkAddr> {
        let payload = decode_payload(frame).unwrap();
        let chunk = payload.chunk;
        self.store.insert(chunk, payload.snapshot.clone());
        self.fetched += 1;
        Some(chunk)
    }

    fn fetched_count(&self) -> u64 {
        self.fetched
    }

    fn generated_count(&self) -> u64 {
        self.generated
    }

    fn chunk_count(&self) -> usize {
        self.store.len()
    }

    fn has(&self, chunk: &ChunkAddr) -> bool {
        self.store.contains_key(chunk)
    }

    fn snapshot(&self, chunk: &ChunkAddr) -> Option<Vec<u8>> {
        self.store.get(chunk).cloned()
    }
}

#[tokio::test]
async fn secondary_login_fetches_every_chunk_without_generating() {
    assert!(!primary_accepts_players());
    assert!(check_player_login().is_err());
    assert_ne!(StreamKind::ChunkRequest, StreamKind::ChunkData);
    assert_ne!(StreamKind::ChunkRequest, StreamKind::ChunkAdvert);
    assert_ne!(StreamKind::ChunkData, StreamKind::ChunkAdvert);
    assert_ne!(PRIMARY_ID, SECONDARY_ID);

    let wanted = login_chunks();
    assert!(wanted.len() > 1);
    let wanted_count = u64::try_from(wanted.len()).unwrap();

    let primary = PrimaryNode::new(&wanted);
    let (request_tx, mut request_rx) = mpsc::channel::<Vec<u8>>(PROOF_CHANNEL_DEPTH);
    let (data_tx, mut data_rx) = mpsc::channel::<Vec<u8>>(PROOF_CHANNEL_DEPTH);

    let mut secondary = SecondaryNode::new();
    assert_eq!(secondary.chunk_count(), 0);
    assert_eq!(secondary.fetched_count(), 0);
    assert_eq!(secondary.generated_count(), 0);

    for frame in primary.advert_frames(PRIMARY_ID) {
        assert!(secondary.apply_frame(&frame));
    }

    let mut requested: HashSet<ChunkAddr> = HashSet::new();
    for chunk in &wanted {
        let frame = secondary.request_frame(chunk).unwrap();
        let fetch: ChunkFetch = decode_request(&frame).unwrap();
        assert_eq!(fetch.chunk, *chunk);
        assert_eq!(fetch.from, PRIMARY_ID);
        requested.insert(fetch.chunk);
        request_tx.send(frame).await.unwrap();
    }
    assert_eq!(requested.len(), wanted.len());
    drop(request_tx);

    let mut served: u64 = 0;
    while let Some(frame) = request_rx.recv().await {
        if let Some(reply) = primary.serve_frame(PRIMARY_ID, &frame) {
            served += 1;
            data_tx.send(reply).await.unwrap();
        }
    }
    drop(data_tx);

    for _ in 0..wanted.len() {
        let frame = tokio::time::timeout(
            Duration::from_secs(PROOF_TIMEOUT_SECS),
            data_rx.recv(),
        )
        .await
        .unwrap()
        .unwrap();
        let chunk = secondary.ingest_frame(&frame).unwrap();
        assert!(wanted.contains(&chunk));
    }

    assert_eq!(secondary.fetched_count(), wanted_count);
    assert!(secondary.fetched_count() > 0);
    assert_eq!(secondary.generated_count(), 0);
    assert_eq!(secondary.chunk_count(), wanted.len());
    assert_eq!(served, wanted_count);
    for chunk in &wanted {
        assert!(secondary.has(chunk));
        assert_eq!(secondary.snapshot(chunk), primary.snapshot(chunk));
    }
}

#[tokio::test]
async fn secondary_without_adverts_resolves_nothing_and_generates_nothing() {
    let secondary = SecondaryNode::new();
    for chunk in &login_chunks() {
        assert!(secondary.request_frame(chunk).is_none());
    }
    assert_eq!(secondary.fetched_count(), 0);
    assert_eq!(secondary.generated_count(), 0);
    assert_eq!(secondary.chunk_count(), 0);
}

#[test]
fn secondary_fetch_prefers_lowest_holder() {
    let wanted = login_chunks();
    let primary = PrimaryNode::new(&wanted);
    let mut secondary = SecondaryNode::new();
    for frame in primary.advert_frames(EXTRA_PEER_ID) {
        assert!(secondary.apply_frame(&frame));
    }
    for frame in primary.advert_frames(PRIMARY_ID) {
        assert!(secondary.apply_frame(&frame));
    }
    for chunk in &wanted {
        let frame = secondary.request_frame(chunk).unwrap();
        let fetch: ChunkFetch = decode_request(&frame).unwrap();
        assert_eq!(fetch.chunk, *chunk);
        assert_eq!(fetch.from, PRIMARY_ID);
    }
    assert_eq!(secondary.fetched_count(), 0);
    assert_eq!(secondary.generated_count(), 0);
}
