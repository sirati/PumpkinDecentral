use serde::{Deserialize, Serialize};

use crate::canonical_action::{LivePlayerLease, SpatialEpoch};
use crate::identity::PlayerSeq;
use crate::realm_completion::PeerGeneration;
use crate::time::TickStamp;

#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub enum SwingArm {
    Main,
    Offhand,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct SwingRecord {
    pub arm: SwingArm,
}

#[derive(Clone, Copy, Debug, Default, PartialEq, Eq, Serialize, Deserialize)]
pub struct SwingRecords {
    arms: u64,
    count: u8,
}

impl SwingRecords {
    pub fn single(arm: SwingArm) -> Self {
        Self {
            arms: u64::from(arm == SwingArm::Offhand),
            count: 1,
        }
    }

    pub fn len(&self) -> usize {
        usize::from(self.count)
    }

    pub fn is_empty(&self) -> bool {
        self.count == 0
    }

    pub fn clear(&mut self) {
        *self = Self::default();
    }

    pub fn iter(&self) -> impl Iterator<Item = SwingRecord> + '_ {
        (0..self.len().min(64)).map(|index| SwingRecord {
            arm: if self.arms & (1_u64 << index) == 0 {
                SwingArm::Main
            } else {
                SwingArm::Offhand
            },
        })
    }

    fn from_wire(bytes: &[u8]) -> Option<Self> {
        if bytes.len() > 64 {
            return None;
        }
        let mut arms = 0_u64;
        for (index, arm) in bytes.iter().enumerate() {
            match arm {
                0 => {}
                1 => arms |= 1_u64 << index,
                _ => return None,
            }
        }
        Some(Self {
            arms,
            count: bytes.len() as u8,
        })
    }
}

#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct PlayerBreakAnimation {
    pub position: [i32; 3],
    pub stage: i8,
    pub bedrock_rate: Option<i32>,
    pub start: bool,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct PlayerVisualEnvelope {
    pub lease: LivePlayerLease,
    pub source: PeerGeneration,
    pub epoch: SpatialEpoch,
    pub sequence: PlayerSeq,
    pub tick: TickStamp,
    pub skin_layers: Option<u8>,
    pub main_hand: Option<u8>,
    pub break_animation: Option<PlayerBreakAnimation>,
    pub swings: SwingRecords,
}

#[derive(Serialize, Deserialize)]
struct VisualHeader {
    lease: LivePlayerLease,
    source: PeerGeneration,
    epoch: SpatialEpoch,
    sequence: PlayerSeq,
    tick: TickStamp,
    skin_layers: Option<u8>,
    main_hand: Option<u8>,
    break_animation: Option<PlayerBreakAnimation>,
}

impl PlayerVisualEnvelope {
    pub fn valid(&self) -> bool {
        self.lease.incarnation != 0
            && !self.lease.key.0.is_nil()
            && self.source.server == self.lease.gid.server
            && self.source.generation != 0
            && match self.epoch {
                SpatialEpoch::Admission { lease, .. } => lease == self.lease,
                SpatialEpoch::Action(_) => true,
            }
            && (self.skin_layers.is_some()
                || self.main_hand.is_some()
                || self.break_animation.is_some()
                || !self.swings.is_empty())
            && self.main_hand.is_none_or(|hand| hand <= 1)
            && self.break_animation.is_none_or(|animation| {
                (-1..=9).contains(&animation.stage)
                    && animation.bedrock_rate.is_none_or(|rate| rate >= 0)
                    && animation
                        .position
                        .iter()
                        .all(|coordinate| coordinate.unsigned_abs() <= 30_000_000)
            })
            && self.swings.len() <= 64
    }

    pub fn encode(&self) -> Option<Vec<u8>> {
        if !self.valid() {
            return None;
        }
        let mut header_buffer = [0_u8; 512];
        let header = postcard::to_slice(
            &VisualHeader {
                lease: self.lease,
                source: self.source,
                epoch: self.epoch,
                sequence: self.sequence,
                tick: self.tick,
                skin_layers: self.skin_layers,
                main_hand: self.main_hand,
                break_animation: self.break_animation,
            },
            &mut header_buffer,
        )
        .ok()?;
        let length = u16::try_from(header.len()).ok()?;
        let mut bytes = Vec::with_capacity(7 + header.len() + self.swings.len());
        bytes.extend_from_slice(b"PRV4");
        bytes.extend_from_slice(&length.to_be_bytes());
        bytes.extend_from_slice(header);
        bytes.push(self.swings.len() as u8);
        bytes.extend(self.swings.iter().map(|record| match record.arm {
            SwingArm::Main => 0,
            SwingArm::Offhand => 1,
        }));
        (bytes.len() <= 4096).then_some(bytes)
    }

    pub fn decode(bytes: &[u8]) -> Option<Self> {
        if bytes.len() > 4096 || !bytes.starts_with(b"PRV4") {
            return None;
        }
        let length = usize::from(u16::from_be_bytes(bytes.get(4..6)?.try_into().ok()?));
        let header_end = 6_usize.checked_add(length)?;
        let (header, remainder) =
            postcard::take_from_bytes::<VisualHeader>(bytes.get(6..header_end)?).ok()?;
        let count = usize::from(*bytes.get(header_end)?);
        if !remainder.is_empty() || count > 64 || bytes.len() != header_end + 1 + count {
            return None;
        }
        let swings = SwingRecords::from_wire(&bytes[header_end + 1..])?;
        let envelope = Self {
            lease: header.lease,
            source: header.source,
            epoch: header.epoch,
            sequence: header.sequence,
            tick: header.tick,
            skin_layers: header.skin_layers,
            main_hand: header.main_hand,
            break_animation: header.break_animation,
            swings,
        };
        envelope.valid().then_some(envelope)
    }
}
