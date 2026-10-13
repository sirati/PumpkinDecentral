use bytes::{BufMut, Bytes, BytesMut};
use zerocopy::byteorder::{LE, U16, U32, U64};
use zerocopy::{FromBytes, Immutable, IntoBytes, KnownLayout, Ref, Unaligned};

use crate::entity_body::EntitySpatial;
use crate::identity::{PlayerSeq, ServerId};
use crate::item_spatial::ItemSpatialSample;
use crate::protocol::{GlobalEntityId, WorldId};
use crate::realm_completion::PeerGeneration;
use crate::time::TickStamp;

const MAGIC: [u8; 4] = *b"PCI2";
const MAX_RECORDS: usize = 64;

#[repr(C)]
#[derive(Clone, Copy, FromBytes, IntoBytes, KnownLayout, Immutable, Unaligned)]
struct Header {
    magic: [u8; 4],
    count: U16<LE>,
    width: U16<LE>,
}

#[repr(C, packed)]
#[derive(Clone, Copy, FromBytes, IntoBytes, KnownLayout, Immutable, Unaligned)]
pub struct Record {
    origin: U16<LE>,
    local_id: U32<LE>,
    generation: U64<LE>,
    issuer: U16<LE>,
    incarnation: U64<LE>,
    birth_len: u8,
    birth: [u8; 96],
    epoch_len: u8,
    epoch: [u8; 64],
    tick: U16<LE>,
    sequence: U16<LE>,
    dimension: u8,
    world: U64<LE>,
    position: [U64<LE>; 3],
    velocity: [U64<LE>; 3],
    yaw: U32<LE>,
    pitch: U32<LE>,
    head_yaw: U32<LE>,
    grounded: u8,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum DecodeFailure {
    Header,
    Count,
    Length,
    Provenance,
    Geometry,
}

impl Record {
    fn capture(sample: &ItemSpatialSample) -> Result<Self, DecodeFailure> {
        let mut birth = [0; 96];
        let mut epoch = [0; 64];
        let birth_len = postcard::to_slice(&sample.birth, &mut birth)
            .map_err(|_| DecodeFailure::Provenance)?
            .len() as u8;
        let epoch_len = postcard::to_slice(&sample.epoch, &mut epoch)
            .map_err(|_| DecodeFailure::Provenance)?
            .len() as u8;
        let record = Self {
            origin: U16::new(sample.id.origin.0),
            local_id: U32::new(sample.id.local_id as u32),
            generation: U64::new(sample.id.generation),
            issuer: U16::new(sample.issuer.server.0),
            incarnation: U64::new(sample.issuer.generation),
            birth_len,
            birth,
            epoch_len,
            epoch,
            tick: U16::new(sample.tick.0),
            sequence: U16::new(sample.sequence.0),
            dimension: sample.spatial.world.dimension,
            world: U64::new(sample.spatial.world.key),
            position: sample.spatial.position.map(U64::new),
            velocity: sample
                .spatial
                .velocity_micro
                .map(|value| U64::new(value as u64)),
            yaw: U32::new(sample.spatial.yaw),
            pitch: U32::new(sample.spatial.pitch),
            head_yaw: U32::new(sample.head_yaw),
            grounded: u8::from(sample.spatial.on_ground),
        };
        record.sample()?;
        Ok(record)
    }

    pub fn sample(&self) -> Result<ItemSpatialSample, DecodeFailure> {
        let birth = postcard::from_bytes::<crate::entity_body::EntityBirth>(
            self.birth
                .get(..usize::from(self.birth_len))
                .ok_or(DecodeFailure::Length)?,
        )
        .map_err(|_| DecodeFailure::Provenance)?;
        let epoch = postcard::from_bytes::<crate::canonical_action::SpatialEpoch>(
            self.epoch
                .get(..usize::from(self.epoch_len))
                .ok_or(DecodeFailure::Length)?,
        )
        .map_err(|_| DecodeFailure::Provenance)?;
        let sample = ItemSpatialSample {
            head_yaw: self.head_yaw.get(),
            id: GlobalEntityId {
                origin: ServerId(self.origin.get()),
                local_id: self.local_id.get() as i32,
                generation: self.generation.get(),
            },
            birth,
            epoch,
            issuer: PeerGeneration {
                server: ServerId(self.issuer.get()),
                generation: self.incarnation.get(),
            },
            tick: TickStamp(self.tick.get()),
            sequence: PlayerSeq(self.sequence.get()),
            spatial: EntitySpatial {
                world: WorldId::new(self.dimension, self.world.get()),
                position: self.position.map(|value| value.get()),
                velocity_micro: self.velocity.map(|value| value.get() as i64),
                yaw: self.yaw.get(),
                pitch: self.pitch.get(),
                on_ground: self.grounded != 0,
            },
        };
        if self.grounded > 1
            || sample.spatial.position.iter().any(|bits| {
                let value = f64::from_bits(*bits);
                !value.is_finite() || value.abs() > 30_000_000.0
            })
            || !f32::from_bits(sample.spatial.yaw).is_finite()
            || !f32::from_bits(sample.spatial.pitch).is_finite()
            || !f32::from_bits(sample.head_yaw).is_finite()
        {
            return Err(DecodeFailure::Geometry);
        }
        if sample.id.generation == 0
            || sample.issuer.generation == 0
            || sample.birth.source.server != sample.id.origin
            || sample.birth.source.generation != sample.id.generation
            || sample.birth.uuid.is_nil()
            || sample.birth.action.tick.is_after(sample.tick)
            || !matches!(sample.epoch, crate::canonical_action::SpatialEpoch::Action(reference) if !reference.tick.is_after(sample.tick))
        {
            return Err(DecodeFailure::Provenance);
        }
        Ok(sample)
    }
}

pub fn batch_length(count: usize) -> Option<usize> {
    (count > 0 && count <= MAX_RECORDS).then(|| size_of::<Header>() + count * size_of::<Record>())
}

pub fn encode(samples: &[ItemSpatialSample]) -> Result<Bytes, DecodeFailure> {
    let length = batch_length(samples.len()).ok_or(DecodeFailure::Count)?;
    let mut bytes = BytesMut::with_capacity(length);
    bytes.put_slice(
        Header {
            magic: MAGIC,
            count: U16::new(samples.len() as u16),
            width: U16::new(size_of::<Record>() as u16),
        }
        .as_bytes(),
    );
    for sample in samples {
        bytes.put_slice(Record::capture(sample)?.as_bytes());
    }
    Ok(bytes.freeze())
}

pub struct Batch {
    bytes: Bytes,
    count: usize,
}

impl Batch {
    pub fn decode(bytes: Bytes) -> Result<Self, DecodeFailure> {
        let header = Ref::<_, Header>::from_bytes(
            bytes
                .get(..size_of::<Header>())
                .ok_or(DecodeFailure::Header)?,
        )
        .map_err(|_| DecodeFailure::Header)?;
        if header.magic != MAGIC || usize::from(header.width.get()) != size_of::<Record>() {
            return Err(DecodeFailure::Header);
        }
        let count = usize::from(header.count.get());
        if batch_length(count) != Some(bytes.len()) {
            return Err(DecodeFailure::Length);
        }
        let batch = Self { bytes, count };
        for index in 0..count {
            batch.record(index).ok_or(DecodeFailure::Length)?.sample()?;
        }
        Ok(batch)
    }

    pub fn record(&self, index: usize) -> Option<Ref<&[u8], Record>> {
        if index >= self.count {
            return None;
        }
        let start = size_of::<Header>() + index * size_of::<Record>();
        Ref::from_bytes(&self.bytes[start..start + size_of::<Record>()]).ok()
    }

    pub fn samples(&self) -> impl Iterator<Item = ItemSpatialSample> + '_ {
        (0..self.count).filter_map(|index| self.record(index)?.sample().ok())
    }
}
