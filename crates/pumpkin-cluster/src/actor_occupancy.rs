use serde::{Deserialize, Serialize};

use crate::actor::ActorId;
use crate::canonical_action::{Cell, LivePlayerLease, SpatialEpoch, Value};
use crate::entity_body::EntityBirth;
use crate::protocol::ChunkKey;

pub const MAX_OCCUPANCY_ENTRIES: usize = crate::canonical_action::MAX_LOGICAL_RECORDS;

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub enum OccupancyIdentity {
    Player(LivePlayerLease),
    Entity(EntityBirth),
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub struct ActorOccupancyCut {
    pub position: [u64; 3],
    pub sample_tick: crate::time::TickStamp,
    pub simulation_sequence: crate::identity::PlayerSeq,
}

impl ActorOccupancyCut {
    pub fn valid(&self) -> bool {
        self.position.iter().all(|bits| {
            let value = f64::from_bits(*bits);
            value.is_finite() && value.abs() <= 30_000_000.0
        })
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct CollisionShape {
    pub offsets: [[u64; 3]; 2],
    pub blocks_building: bool,
    pub hanging: bool,
}

impl CollisionShape {
    pub fn chunk_span(&self, position: [u64; 3]) -> Option<[[i32; 2]; 2]> {
        if !self.valid()
            || !position.iter().all(|bits| {
                let value = f64::from_bits(*bits);
                value.is_finite() && value.abs() <= 30_000_000.0
            })
        {
            return None;
        }
        let minimum = std::array::from_fn::<_, 3, _>(|axis| {
            f64::from_bits(position[axis]) + f64::from_bits(self.offsets[0][axis])
        });
        let maximum = std::array::from_fn::<_, 3, _>(|axis| {
            f64::from_bits(position[axis]) + f64::from_bits(self.offsets[1][axis])
        });
        Some([
            [
                (minimum[0] / 16.0).floor() as i32,
                (minimum[2] / 16.0).floor() as i32,
            ],
            [
                if maximum[0] == minimum[0] { (maximum[0] / 16.0).floor() as i32 } else { (maximum[0] / 16.0).ceil() as i32 - 1 },
                if maximum[2] == minimum[2] { (maximum[2] / 16.0).floor() as i32 } else { (maximum[2] / 16.0).ceil() as i32 - 1 },
            ],
        ])
    }
    pub fn valid(&self) -> bool {
        (0..3).all(|axis| {
            let minimum = f64::from_bits(self.offsets[0][axis]);
            let maximum = f64::from_bits(self.offsets[1][axis]);
            minimum.is_finite()
                && maximum.is_finite()
                && minimum <= maximum
                && minimum.abs() <= 1024.0
                && maximum.abs() <= 1024.0
        })
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct ActorOccupancyEntry {
    pub association: ChunkKey,
    pub owner: crate::realm_completion::PeerGeneration,
    pub cut: ActorOccupancyCut,
    pub actor: ActorId,
    pub identity: OccupancyIdentity,
    pub spatial_epoch: SpatialEpoch,
    pub shape: CollisionShape,
}

impl ActorOccupancyEntry {
    pub fn body_target(&self) -> crate::canonical_action::Target {
        let part = match self.actor {
            ActorId::Player(gid) => crate::canonical_action::ChunkPart::Player(gid),
            ActorId::Entity(id) => crate::canonical_action::ChunkPart::Entity(id),
            ActorId::Peer(_) => unreachable!(),
        };
        crate::canonical_action::Target::Chunk {
            key: self.association,
            part,
            inventory: None,
        }
    }
    pub fn covered_chunks(&self) -> impl Iterator<Item = ChunkKey> + '_ {
        let span = self.shape.chunk_span(self.cut.position);
        let world = self.association.world;
        let association = self.association;
        span.into_iter()
            .flat_map(move |[minimum, maximum]| {
                (minimum[0]..=maximum[0]).flat_map(move |x| {
                    (minimum[1]..=maximum[1]).map(move |z| ChunkKey::new(world, x, z))
                })
            })
            .filter(move |key| *key != association)
            .chain(std::iter::once(association))
    }
    pub fn covers_chunk(&self, key: ChunkKey) -> bool {
        key.world == self.association.world
            && (key == self.association
                || self
                    .shape
                    .chunk_span(self.cut.position)
                    .is_some_and(|[minimum, maximum]| {
                        key.x() >= minimum[0]
                            && key.x() <= maximum[0]
                            && key.z() >= minimum[1]
                            && key.z() <= maximum[1]
                    }))
    }
    pub fn accepts_cut(&self, cut: ActorOccupancyCut) -> bool {
        cut.valid()
            && (cut.sample_tick == self.cut.sample_tick
                || cut.sample_tick.is_after(self.cut.sample_tick))
            && (cut.sample_tick != self.cut.sample_tick
                || cut.simulation_sequence == self.cut.simulation_sequence
                || cut
                    .simulation_sequence
                    .is_newer_than(self.cut.simulation_sequence))
            && self.shape.chunk_span(cut.position) == self.shape.chunk_span(self.cut.position)
    }
    pub fn valid(&self) -> bool {
        self.shape.valid()
            && self.cut.valid()
            && self.owner.generation != 0
            && match (&self.actor, &self.identity) {
                (ActorId::Player(gid), OccupancyIdentity::Player(lease)) => {
                    *gid == lease.gid
                        && self.owner.server == gid.server
                        && lease.incarnation != 0
                        && !lease.key.0.is_nil()
                        && match self.spatial_epoch {
                            SpatialEpoch::Admission {
                                lease: admitted, ..
                            } => admitted == *lease,
                            SpatialEpoch::Action(reference) => reference.id.incarnation != 0,
                        }
                }
                (ActorId::Entity(id), OccupancyIdentity::Entity(birth)) => {
                    id.generation != 0
                        && id.origin == birth.source.server
                        && birth.source.generation != 0
                        && !birth.uuid.is_nil()
                        && matches!(self.spatial_epoch, SpatialEpoch::Action(reference) if reference.id.incarnation != 0)
                }
                _ => false,
            }
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct ActorOccupancyRoot {
    pub entries: crate::occupancy_membership::OccupancyEntries,
}

impl ActorOccupancyRoot {
    pub fn empty() -> Self {
        Self {
            entries: Default::default(),
        }
    }
    pub fn from_entries(entries: Vec<ActorOccupancyEntry>) -> Option<Self> {
        Some(Self {
            entries: crate::occupancy_membership::OccupancyEntries::from_sorted(entries)?,
        })
    }
    pub fn is_complete(&self) -> bool {
        self.entries.is_complete()
    }
    pub fn resolve_against(&self, current: &Self) -> Option<Self> {
        Some(Self {
            entries: self.entries.resolve_against(&current.entries)?,
        })
    }
    pub fn transition(
        &self,
        changes: &std::collections::BTreeMap<ActorId, Option<ActorOccupancyEntry>>,
    ) -> Option<(Self, Self)> {
        let (before, after) = self.entries.transition(changes)?;
        Some((Self { entries: before }, Self { entries: after }))
    }
    pub fn transition_to(&self, after: &Self) -> Option<(Self, Self)> {
        let (before, after) = self.entries.transition_to(&after.entries)?;
        Some((Self { entries: before }, Self { entries: after }))
    }
    pub fn valid(&self) -> bool {
        self.entries.valid()
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct ActorOccupancyPose {
    pub sample_tick: crate::time::TickStamp,
    pub entry: ActorOccupancyEntry,
    pub position: [u64; 3],
    pub simulation_sequence: u16,
}

impl ActorOccupancyPose {
    pub fn valid(&self) -> bool {
        self.entry.valid()
            && self.entry.accepts_cut(ActorOccupancyCut {
                position: self.position,
                sample_tick: self.sample_tick,
                simulation_sequence: crate::identity::PlayerSeq(self.simulation_sequence),
            })
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct CollisionBox {
    pub bounds: [[u64; 3]; 2],
}

impl CollisionBox {
    pub fn valid(&self) -> bool {
        (0..3).all(|axis| {
            let minimum = f64::from_bits(self.bounds[0][axis]);
            let maximum = f64::from_bits(self.bounds[1][axis]);
            minimum.is_finite()
                && maximum.is_finite()
                && minimum <= maximum
                && minimum.abs() <= 30_000_000.0
                && maximum.abs() <= 30_000_000.0
                && maximum - minimum <= 1024.0
        })
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct ActorOccupancyWitness {
    pub key: ChunkKey,
    pub revision: u64,
    pub poses: Vec<ActorOccupancyPose>,
    pub blocks: Vec<CollisionBox>,
}

impl ActorOccupancyWitness {
    pub fn valid(&self) -> bool {
        self.poses.len() <= MAX_OCCUPANCY_ENTRIES
            && self.blocks.len() <= 4096
            && !self.blocks.is_empty()
            && self.blocks.iter().all(CollisionBox::valid)
            && self.poses.iter().all(ActorOccupancyPose::valid)
            && self
                .poses
                .windows(2)
                .all(|pair| pair[0].entry.actor < pair[1].entry.actor)
    }

    pub fn matches_clear(&self, cell: &Cell) -> bool {
        self.matches_family_clear(cell, 0)
    }

    pub fn matches_hanging_clear(&self, cell: &Cell) -> bool {
        self.matches_family_clear(cell, 1)
    }

    pub fn matches_entities_clear(&self, cell: &Cell) -> bool {
        self.matches_family_clear(cell, 2)
    }

    pub fn matches_domain(&self, cell: &Cell) -> bool {
        self.matches_family_clear(cell, 3)
    }

    fn matches_family_clear(&self, cell: &Cell, family: u8) -> bool {
        let Value::ActorOccupancy(root) = &cell.value else {
            return false;
        };
        self.valid()
            && root.valid()
            && self.revision == cell.revision
            && root.is_complete()
            && root.entries.len() == self.poses.len()
            && root.entries.iter().zip(&self.poses).all(|(entry, pose)| {
                entry == &pose.entry
                    && entry.covers_chunk(self.key)
                    && (!(if family == 3 {
                        false
                    } else if family == 2 {
                        true
                    } else if family == 1 {
                        entry.shape.hanging
                    } else {
                        entry.shape.blocks_building
                    }) || self.blocks.iter().all(|block| {
                        !(0..3).all(|axis| {
                            let position = f64::from_bits(pose.position[axis]);
                            let minimum = position + f64::from_bits(entry.shape.offsets[0][axis]);
                            let maximum = position + f64::from_bits(entry.shape.offsets[1][axis]);
                            maximum > f64::from_bits(block.bounds[0][axis])
                                && minimum < f64::from_bits(block.bounds[1][axis])
                        })
                    }))
            })
    }
}

pub fn materialize_write(
    write: &crate::canonical_action::AppliedWrite,
    current: &Cell,
) -> Option<Cell> {
    if write.before.as_ref() != Some(current) {
        return None;
    }
    let Value::ActorOccupancy(prior) = &current.value else {
        return None;
    };
    let Cell {
        revision,
        value: Value::ActorOccupancy(after),
    } = write.after.as_ref()?
    else {
        return None;
    };
    let Value::ActorOccupancy(before) = &write.before.as_ref()?.value else {
        return None;
    };
    let root = after.resolve_against(prior)?;
    if before.resolve_against(&root).as_ref() != Some(prior) {
        return None;
    }
    Some(Cell {
        revision: *revision,
        value: Value::ActorOccupancy(root),
    })
}
pub fn compact_transition(before: &Cell, after: &Cell) -> Option<(Cell, Cell)> {
    let Value::ActorOccupancy(prior) = &before.value else {
        return None;
    };
    let Value::ActorOccupancy(next) = &after.value else {
        return None;
    };
    let (prior, next) = prior.transition_to(next)?;
    Some((
        Cell {
            revision: before.revision,
            value: Value::ActorOccupancy(prior),
        },
        Cell {
            revision: after.revision,
            value: Value::ActorOccupancy(next),
        },
    ))
}
pub fn compact_write(
    mut write: crate::canonical_action::AppliedWrite,
) -> Option<crate::canonical_action::AppliedWrite> {
    if matches!(
        write.target,
        crate::canonical_action::Target::Chunk {
            part: crate::canonical_action::ChunkPart::ActorOccupancy,
            inventory: None,
            ..
        }
    ) {
        let (before, after) = compact_transition(write.before.as_ref()?, write.after.as_ref()?)?;
        write.before = Some(before);
        write.after = Some(after);
    }
    Some(write)
}

#[cfg(test)]
mod span_tests {
    use super::*;
    #[test]
    fn exact_negative_boundaries_and_zero_extent_shapes_keep_complete_point_coverage() {
        let shape = CollisionShape { offsets: [[0.0_f64;3].map(f64::to_bits), [16.0_f64, 1.0, 16.0].map(f64::to_bits)], blocks_building: true, hanging: false };
        assert_eq!(shape.chunk_span([0.0_f64;3].map(f64::to_bits)), Some([[0,0],[0,0]]));
        assert_eq!(shape.chunk_span([-16.0_f64,0.0,-16.0].map(f64::to_bits)), Some([[-1,-1],[-1,-1]]));
        assert_eq!(shape.chunk_span([-16.1_f64,0.0,-16.1].map(f64::to_bits)), Some([[-2,-2],[-1,-1]]));
        let point = CollisionShape { offsets: [[0.0_f64.to_bits();3];2], blocks_building: false, hanging: false };
        assert_eq!(point.chunk_span([16.0_f64,0.0,-0.5].map(f64::to_bits)), Some([[1,-1],[1,-1]]));
    }
}
