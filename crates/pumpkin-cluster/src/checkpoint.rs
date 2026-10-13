use std::io;
use std::path::{Path, PathBuf};

use sha2::{Digest, Sha256};

use crate::membership::CLUSTER_PROTOCOL_VERSION;
use crate::protocol::ChunkKey;
use crate::xfer::ChunkPayload;

// The postcard Action envelope changed with protocol 56; old checkpoint
// bodies must be rejected before attempting to decode their enum ordinals.
const MAGIC: &[u8; 4] = b"CPK2";
const MAX_PAYLOAD: usize = 64 * 1024 * 1024;

#[derive(Debug, Clone, PartialEq)]
pub struct ChunkCheckpoint {
    pub generation: u64,
    pub payload: ChunkPayload,
}

pub fn path(folder: &Path, chunk: ChunkKey) -> PathBuf {
    folder.join("cluster-checkpoints").join(format!(
        "{}_{}_{}_{}.cpk",
        chunk.world.dimension,
        chunk.world.key,
        chunk.x(),
        chunk.z()
    ))
}

pub fn encode(checkpoint: &ChunkCheckpoint) -> io::Result<Vec<u8>> {
    if checkpoint.generation == 0
        || checkpoint
            .payload
            .ground_frontier
            .is_none_or(|frontier| frontier.epoch != checkpoint.generation)
        || checkpoint.payload.validate_transfer_evidence().is_err()
    {
        return Err(io::ErrorKind::InvalidData.into());
    }
    let body =
        postcard::to_allocvec(&checkpoint.payload).map_err(|_| io::ErrorKind::InvalidData)?;
    let length = u32::try_from(body.len()).map_err(|_| io::ErrorKind::InvalidData)?;
    if body.len() > MAX_PAYLOAD {
        return Err(io::ErrorKind::InvalidData.into());
    }
    let mut bytes = Vec::with_capacity(50 + body.len());
    bytes.extend_from_slice(MAGIC);
    bytes.extend_from_slice(&CLUSTER_PROTOCOL_VERSION.to_le_bytes());
    bytes.extend_from_slice(&checkpoint.generation.to_le_bytes());
    bytes.extend_from_slice(&length.to_le_bytes());
    bytes.extend_from_slice(&Sha256::digest(&body));
    bytes.extend_from_slice(&body);
    Ok(bytes)
}

pub fn decode(bytes: &[u8], expected: ChunkKey) -> io::Result<ChunkCheckpoint> {
    if bytes.len() < 50 || &bytes[..4] != MAGIC {
        return Err(io::ErrorKind::InvalidData.into());
    }
    if u16::from_le_bytes(bytes[4..6].try_into().unwrap()) != CLUSTER_PROTOCOL_VERSION {
        return Err(io::ErrorKind::InvalidData.into());
    }
    let generation = u64::from_le_bytes(bytes[6..14].try_into().unwrap());
    let length = u32::from_le_bytes(bytes[14..18].try_into().unwrap()) as usize;
    if generation == 0 || length > MAX_PAYLOAD || bytes.len() != 50 + length {
        return Err(io::ErrorKind::InvalidData.into());
    }
    let body = &bytes[50..];
    if Sha256::digest(body).as_slice() != &bytes[18..50] {
        return Err(io::ErrorKind::InvalidData.into());
    }
    let payload: ChunkPayload =
        postcard::from_bytes(body).map_err(|_| io::ErrorKind::InvalidData)?;
    if payload.chunk != expected
        || payload
            .ground_frontier
            .is_none_or(|frontier| frontier.epoch != generation)
        || payload.validate_transfer_evidence().is_err()
    {
        return Err(io::ErrorKind::InvalidData.into());
    }
    Ok(ChunkCheckpoint {
        generation,
        payload,
    })
}
