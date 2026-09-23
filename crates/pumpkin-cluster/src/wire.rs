use bytes::{BufMut, Bytes, BytesMut};
use zerocopy::byteorder::{LE, U16, U32, U64};
use zerocopy::{FromBytes, Immutable, IntoBytes, KnownLayout, Ref, Unaligned};

use crate::identity::{GlobalPlayerId, PlayerSeq, PlayerSlot, ServerId};
use crate::protocol::{PosUpdate, WorldId};
use crate::time::TickStamp;

const MAGIC: [u8; 4] = *b"PCM2";
const VERSION: u16 = 2;
const MOVEMENT_KIND: u8 = 1;
pub const MAX_WIRE_RECORDS: usize = 1024;
pub const MAX_WIRE_PAYLOAD: usize = 1024 * 1024;

#[repr(C)]
#[derive(Clone, Copy, FromBytes, IntoBytes, KnownLayout, Immutable, Unaligned)]
struct BatchHeader {
    magic: [u8; 4],
    version: U16<LE>,
    kind: u8,
    flags: u8,
    count: U16<LE>,
    record_size: U16<LE>,
    payload_len: U32<LE>,
}

#[repr(C, packed)]
#[derive(Clone, Copy, FromBytes, IntoBytes, KnownLayout, Immutable, Unaligned)]
pub struct MovementRecord {
    server: U16<LE>,
    player: U16<LE>,
    sequence: U16<LE>,
    tick: U16<LE>,
    dimension: u8,
    world: U64<LE>,
    pos_x: U64<LE>,
    pos_y: U64<LE>,
    pos_z: U64<LE>,
    vel_x: U64<LE>,
    vel_y: U64<LE>,
    vel_z: U64<LE>,
    body_yaw: U32<LE>,
    head_yaw: U32<LE>,
    pitch: U32<LE>,
    on_ground: u8,
}

impl MovementRecord {
    #[must_use]
    pub fn from_update(update: &PosUpdate) -> Self {
        Self {
            server: U16::new(update.gid.server.0),
            player: U16::new(update.gid.player.0),
            sequence: U16::new(update.seq.0),
            tick: U16::new(update.tick.0),
            dimension: update.world.dimension,
            world: U64::new(update.world.key),
            pos_x: U64::new(update.pos[0].to_bits()),
            pos_y: U64::new(update.pos[1].to_bits()),
            pos_z: U64::new(update.pos[2].to_bits()),
            vel_x: U64::new(update.vel[0].to_bits()),
            vel_y: U64::new(update.vel[1].to_bits()),
            vel_z: U64::new(update.vel[2].to_bits()),
            body_yaw: U32::new(update.yaw.to_bits()),
            head_yaw: U32::new(update.head_yaw.to_bits()),
            pitch: U32::new(update.pitch.to_bits()),
            on_ground: u8::from(update.on_ground),
        }
    }

    #[must_use]
    pub fn to_update(self) -> PosUpdate {
        PosUpdate {
            gid: GlobalPlayerId::new(ServerId(self.server.get()), PlayerSlot(self.player.get())),
            seq: PlayerSeq(self.sequence.get()),
            tick: TickStamp(self.tick.get()),
            world: WorldId::new(self.dimension, self.world.get()),
            pos: [
                f64::from_bits(self.pos_x.get()),
                f64::from_bits(self.pos_y.get()),
                f64::from_bits(self.pos_z.get()),
            ],
            vel: [
                f64::from_bits(self.vel_x.get()),
                f64::from_bits(self.vel_y.get()),
                f64::from_bits(self.vel_z.get()),
            ],
            yaw: f32::from_bits(self.body_yaw.get()),
            head_yaw: f32::from_bits(self.head_yaw.get()),
            pitch: f32::from_bits(self.pitch.get()),
            on_ground: self.on_ground != 0,
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum WireError {
    Header,
    Version(u16),
    Kind(u8),
    RecordCount(usize),
    PayloadSize(usize),
    RecordSize(usize),
    Length,
}

pub const MOVEMENT_RECORD_BYTES: usize = core::mem::size_of::<MovementRecord>();
const HEADER_BYTES: usize = core::mem::size_of::<BatchHeader>();

#[must_use]
pub const fn movement_batch_len(records: usize) -> Option<usize> {
    if records > MAX_WIRE_RECORDS {
        return None;
    }
    match records.checked_mul(MOVEMENT_RECORD_BYTES) {
        Some(payload) => HEADER_BYTES.checked_add(payload),
        None => None,
    }
}

pub fn encode_movement(updates: &[PosUpdate]) -> Result<Bytes, WireError> {
    let total = movement_batch_len(updates.len()).ok_or(WireError::RecordCount(updates.len()))?;
    let payload_len = total.saturating_sub(HEADER_BYTES);
    if payload_len > MAX_WIRE_PAYLOAD {
        return Err(WireError::PayloadSize(payload_len));
    }
    let count = u16::try_from(updates.len()).map_err(|_| WireError::RecordCount(updates.len()))?;
    let record_size = u16::try_from(MOVEMENT_RECORD_BYTES)
        .map_err(|_| WireError::RecordSize(MOVEMENT_RECORD_BYTES))?;
    let payload_len_u32 =
        u32::try_from(payload_len).map_err(|_| WireError::PayloadSize(payload_len))?;
    let header = BatchHeader {
        magic: MAGIC,
        version: U16::new(VERSION),
        kind: MOVEMENT_KIND,
        flags: 0,
        count: U16::new(count),
        record_size: U16::new(record_size),
        payload_len: U32::new(payload_len_u32),
    };
    let mut out = BytesMut::with_capacity(total);
    out.put_slice(header.as_bytes());
    for update in updates {
        out.put_slice(MovementRecord::from_update(update).as_bytes());
    }
    Ok(out.freeze())
}

pub struct MovementBatch {
    bytes: Bytes,
    count: usize,
}

impl MovementBatch {
    pub fn decode(bytes: Bytes) -> Result<Self, WireError> {
        let header_bytes = bytes.get(..HEADER_BYTES).ok_or(WireError::Header)?;
        let header =
            Ref::<_, BatchHeader>::from_bytes(header_bytes).map_err(|_| WireError::Header)?;
        if header.magic != MAGIC {
            return Err(WireError::Header);
        }
        if header.version.get() != VERSION {
            return Err(WireError::Version(header.version.get()));
        }
        if header.kind != MOVEMENT_KIND {
            return Err(WireError::Kind(header.kind));
        }
        let count = usize::from(header.count.get());
        if count > MAX_WIRE_RECORDS {
            return Err(WireError::RecordCount(count));
        }
        let record_size = usize::from(header.record_size.get());
        if record_size != MOVEMENT_RECORD_BYTES {
            return Err(WireError::RecordSize(record_size));
        }
        let payload_len = usize::try_from(header.payload_len.get())
            .map_err(|_| WireError::PayloadSize(usize::MAX))?;
        if payload_len > MAX_WIRE_PAYLOAD {
            return Err(WireError::PayloadSize(payload_len));
        }
        let expected = count.checked_mul(record_size).ok_or(WireError::Length)?;
        if payload_len != expected || bytes.len() != HEADER_BYTES.saturating_add(payload_len) {
            return Err(WireError::Length);
        }
        Ok(Self { bytes, count })
    }

    #[must_use]
    pub const fn len(&self) -> usize {
        self.count
    }

    #[must_use]
    pub const fn is_empty(&self) -> bool {
        self.count == 0
    }

    #[must_use]
    pub fn record(&self, index: usize) -> Option<Ref<&[u8], MovementRecord>> {
        if index >= self.count {
            return None;
        }
        let start = HEADER_BYTES.checked_add(index.checked_mul(MOVEMENT_RECORD_BYTES)?)?;
        let end = start.checked_add(MOVEMENT_RECORD_BYTES)?;
        Ref::from_bytes(self.bytes.get(start..end)?).ok()
    }

    pub fn updates(&self) -> impl Iterator<Item = PosUpdate> + '_ {
        (0..self.count).filter_map(|index| self.record(index).map(|record| (*record).to_update()))
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn update(player: u16) -> PosUpdate {
        PosUpdate {
            gid: GlobalPlayerId::new(ServerId(2), PlayerSlot(player)),
            seq: PlayerSeq(u16::MAX),
            tick: TickStamp(17),
            world: crate::protocol::WorldId::new(2, 0x1234),
            pos: [1.25, -64.0, 8.5],
            vel: [-0.5, 0.0, 2.0],
            yaw: 20.0,
            head_yaw: 30.0,
            pitch: -10.0,
            on_ground: true,
        }
    }

    #[test]
    fn movement_reader_rejects_length_and_limit_manipulation() {
        let encoded = encode_movement(&[update(1), update(2)]).unwrap();
        let batch = MovementBatch::decode(encoded.clone()).unwrap();
        assert_eq!(
            batch.updates().collect::<Vec<_>>(),
            vec![update(1), update(2)]
        );

        let mut truncated = encoded.to_vec();
        truncated.pop();
        assert_eq!(
            MovementBatch::decode(truncated.into()).err(),
            Some(WireError::Length)
        );

        let mut wrong_size = encoded.to_vec();
        wrong_size[10..12].copy_from_slice(&1_u16.to_le_bytes());
        assert_eq!(
            MovementBatch::decode(wrong_size.into()).err(),
            Some(WireError::RecordSize(1))
        );
    }
}
