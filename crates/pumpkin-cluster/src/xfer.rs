use serde::{Deserialize, Serialize};

use crate::chunks::{ChunkAnnounce, ChunkFetch};
use crate::codec::CodecError;
use crate::protocol::{ChunkAddr, StreamKind};

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct PendingRef {
    pub bytes: Vec<u8>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct ChunkPayload {
    pub chunk: ChunkAddr,
    pub holder: u16,
    pub snapshot: Vec<u8>,
    pub pendings: Vec<PendingRef>,
    pub holders: Vec<u16>,
}

impl ChunkPayload {
    #[must_use]
    pub fn new(
        chunk: ChunkAddr,
        holder: u16,
        snapshot: Vec<u8>,
        pendings: Vec<PendingRef>,
        holders: Vec<u16>,
    ) -> Self {
        let mut sorted = holders;
        sorted.sort_unstable();
        sorted.dedup();
        Self { chunk, holder, snapshot, pendings, holders: sorted }
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct InboundParcel {
    pub peer: u16,
    pub kind: StreamKind,
    pub bytes: Vec<u8>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct OutboundParcel {
    pub peer: u16,
    pub kind: StreamKind,
    pub bytes: Vec<u8>,
}

#[must_use]
pub fn lowest_holder(holders: &[u16]) -> Option<u16> {
    holders.iter().copied().min()
}

pub fn encode_announce(announce: &ChunkAnnounce) -> Result<Vec<u8>, CodecError> {
    postcard::to_allocvec(announce).map_err(|error| CodecError {
        message: format!("encode chunk announce: {error}"),
    })
}

pub fn decode_announce(bytes: &[u8]) -> Result<ChunkAnnounce, CodecError> {
    postcard::from_bytes(bytes).map_err(|error| CodecError {
        message: format!("decode chunk announce: {error}"),
    })
}

pub fn encode_request(fetch: &ChunkFetch) -> Result<Vec<u8>, CodecError> {
    postcard::to_allocvec(fetch).map_err(|error| CodecError {
        message: format!("encode chunk request: {error}"),
    })
}

pub fn decode_request(bytes: &[u8]) -> Result<ChunkFetch, CodecError> {
    postcard::from_bytes(bytes).map_err(|error| CodecError {
        message: format!("decode chunk request: {error}"),
    })
}

pub fn encode_payload(payload: &ChunkPayload) -> Result<Vec<u8>, CodecError> {
    postcard::to_allocvec(payload).map_err(|error| CodecError {
        message: format!("encode chunk payload: {error}"),
    })
}

pub fn decode_payload(bytes: &[u8]) -> Result<ChunkPayload, CodecError> {
    postcard::from_bytes(bytes).map_err(|error| CodecError {
        message: format!("decode chunk payload: {error}"),
    })
}


#[cfg(test)]
mod tests {
    use super::*;
    use crate::chunks::{ChunkAdvert, ChunkDrop};

    fn chunk(x: i32, z: i32) -> ChunkAddr {
        ChunkAddr { x, z }
    }

    #[test]
    fn lowest_holder_picks_minimum() {
        assert_eq!(lowest_holder(&[3, 1, 2]), Some(1));
        assert_eq!(lowest_holder(&[]), None);
    }

    #[test]
    fn announce_acquire_round_trips() {
        let announce = ChunkAnnounce::Acquire(ChunkAdvert {
            holder: 2,
            chunk: chunk(1, 2),
        });
        let bytes = encode_announce(&announce).expect("encode");
        assert_eq!(decode_announce(&bytes).expect("decode"), announce);
    }

    #[test]
    fn announce_release_round_trips() {
        let announce = ChunkAnnounce::Release(ChunkDrop {
            holder: 4,
            chunk: chunk(7, 7),
        });
        let bytes = encode_announce(&announce).expect("encode");
        assert_eq!(decode_announce(&bytes).expect("decode"), announce);
    }

    #[test]
    fn payload_holder_list_round_trips() {
        let payload = ChunkPayload::new(
            chunk(5, 6),
            2,
            vec![9, 8, 7],
            vec![PendingRef { bytes: vec![1] }],
            vec![3, 1, 2, 1],
        );
        let bytes = encode_payload(&payload).expect("encode");
        let back = decode_payload(&bytes).expect("decode");
        assert_eq!(back, payload);
        assert_eq!(back.holders, vec![1, 2, 3]);
    }

    #[test]
    fn request_round_trips() {
        let fetch = ChunkFetch {
            chunk: chunk(0, 0),
            from: 7,
        };
        let bytes = encode_request(&fetch).expect("encode");
        assert_eq!(decode_request(&bytes).expect("decode"), fetch);
    }
}
