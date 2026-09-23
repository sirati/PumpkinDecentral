use serde::{Deserialize, Serialize};

use crate::chunks::{ChunkAnnounce, ChunkFetch};
use crate::codec::CodecError;
use crate::protocol::{ChunkKey, StreamKind};

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct PendingRef {
    pub state: crate::completion::ChunkPendingTick,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct ChunkPayload {
    pub chunk: ChunkKey,
    pub holder: u16,
    pub snapshot: Vec<u8>,
    pub pendings: Vec<PendingRef>,
    pub holders: Vec<u16>,
    pub inventory: Vec<crate::inventory::ChunkInventoryState>,
}

impl ChunkPayload {
    #[must_use]
    pub fn new(
        chunk: ChunkKey,
        holder: u16,
        snapshot: Vec<u8>,
        pendings: Vec<PendingRef>,
        holders: Vec<u16>,
    ) -> Self {
        let mut sorted = holders;
        sorted.sort_unstable();
        sorted.dedup();
        Self {
            chunk,
            holder,
            snapshot,
            pendings,
            holders: sorted,
            inventory: Vec::new(),
        }
    }

    #[must_use]
    pub fn with_inventory(
        mut self,
        inventory: Vec<crate::inventory::ChunkInventoryState>,
    ) -> Self {
        self.inventory = inventory;
        self
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

    fn chunk(x: i32, z: i32) -> ChunkKey {
        ChunkKey::new(crate::protocol::WorldId::new(0, 7), x, z)
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
            vec![PendingRef {
                state: crate::completion::ChunkPendingTick {
                    tick: crate::time::TickStamp(7),
                    chunk: chunk(5, 6),
                    holders: vec![crate::identity::ServerId(2)],
                    finishes: Vec::new(),
                    batches: Vec::new(),
                },
            }],
            vec![3, 1, 2, 1],
        );
        let bytes = encode_payload(&payload).expect("encode");
        let back = decode_payload(&bytes).expect("decode");
        assert_eq!(back, payload);
        assert_eq!(back.holders, vec![1, 2, 3]);
    }

    #[test]
    fn payload_carries_virtual_input_and_beacon_revision() {
        use crate::actor::ActorId;
        use crate::identity::{GlobalPlayerId, PlayerSlot, ServerId};
        use crate::inventory::{
            BeaconMenuState, ChunkInventoryState, InvLoc, InventoryCell,
            InventoryStateSnapshot, InventoryTarget, INV_MAIN,
        };
        let chunk = chunk(5, 6);
        let virtual_target = InventoryTarget::Virtual {
            host: ServerId(2),
            owner: ActorId::Player(GlobalPlayerId::new(ServerId(2), PlayerSlot(4))),
            session: 17,
            kind: 1,
        };
        let beacon_target = InventoryTarget::Block {
            chunk,
            position: crate::protocol::BlockPos { x: 80, y: 80, z: 96 },
        };
        let inventory = vec![
            ChunkInventoryState {
                snapshot: InventoryStateSnapshot {
                    target: virtual_target,
                    revision: 7,
                    cells: vec![InventoryCell {
                        loc: InvLoc::new(INV_MAIN, 0),
                        item: 4,
                        count: 2,
                        nbt: vec![1, 2],
                    }],
                },
                chunks: vec![chunk],
                beacon: None,
            },
            ChunkInventoryState {
                snapshot: InventoryStateSnapshot {
                    target: beacon_target,
                    revision: 9,
                    cells: vec![InventoryCell::empty(InvLoc::new(INV_MAIN, 0))],
                },
                chunks: vec![chunk],
                beacon: Some(BeaconMenuState {
                    revision: 11,
                    levels: 1,
                    primary_effect: Some(1),
                    secondary_effect: None,
                }),
            },
        ];
        let payload = ChunkPayload::new(chunk, 2, vec![9], Vec::new(), vec![2])
            .with_inventory(inventory.clone());
        assert_eq!(decode_payload(&encode_payload(&payload).unwrap()).unwrap().inventory, inventory);
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
