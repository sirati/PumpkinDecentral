use serde::{Deserialize, Serialize};

use crate::actor::ActorId;
use crate::canonical_action::{ChunkPart, LivePlayerLease, SpatialEpoch, Target};
use crate::entity_body::EntityBirth;
use crate::identity::PlayerSeq;
use crate::protocol::{ChunkKey, GlobalEntityId, WorldId};
use crate::realm_completion::PeerGeneration;
use crate::time::TickStamp;

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub enum ContactIdentity {
    Player(LivePlayerLease),
    Entity {
        id: GlobalEntityId,
        birth: EntityBirth,
    },
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct ContactObservation {
    pub identity: ContactIdentity,
    pub owner: PeerGeneration,
    pub epoch: SpatialEpoch,
    pub world: WorldId,
    pub chunk: ChunkKey,
    pub position: [u64; 3],
    pub half_width: u64,
    pub height: u64,
    pub simulation_tick: TickStamp,
    pub observed_sequence: PlayerSeq,
}

impl ContactObservation {
    pub fn player(
        lease: LivePlayerLease,
        owner: PeerGeneration,
        epoch: SpatialEpoch,
        association: ChunkKey,
        effective: crate::controller::EffectiveControllerState,
        half_width: f64,
        height: f64,
        simulation_tick: TickStamp,
    ) -> Option<Self> {
        if effective.actor != lease.gid || effective.tick.is_after(simulation_tick) {
            return None;
        }
        let observation = Self {
            identity: ContactIdentity::Player(lease),
            owner,
            epoch,
            world: association.world,
            chunk: association,
            position: effective.position.map(f64::to_bits),
            half_width: half_width.to_bits(),
            height: height.to_bits(),
            simulation_tick,
            observed_sequence: effective.source_seq,
        };
        observation.valid().then_some(observation)
    }

    pub fn entity(
        body: &crate::entity_body::EntityBody,
        association: ChunkKey,
        spatial: &crate::entity_body::EntitySpatial,
        half_width: f64,
        height: f64,
        simulation_tick: TickStamp,
        observed_sequence: PlayerSeq,
    ) -> Option<Self> {
        if !body.belongs_to(body.id, association)
            || spatial.world != association.world
            || !f32::from_bits(spatial.yaw).is_finite()
            || !f32::from_bits(spatial.pitch).is_finite()
        {
            return None;
        }
        let observation = Self {
            identity: ContactIdentity::Entity {
                id: body.id,
                birth: body.birth.clone(),
            },
            owner: body.owner,
            epoch: body.spatial_epoch,
            world: association.world,
            chunk: association,
            position: spatial.position,
            half_width: half_width.to_bits(),
            height: height.to_bits(),
            simulation_tick,
            observed_sequence,
        };
        observation.valid().then_some(observation)
    }

    pub fn observed_chunk(&self) -> Option<ChunkKey> {
        let position = self.position.map(f64::from_bits);
        position
            .iter()
            .all(|value| value.is_finite() && value.abs() <= 30_000_000.0)
            .then(|| {
                ChunkKey::new(
                    self.world,
                    (position[0].floor() as i32).div_euclid(16),
                    (position[2].floor() as i32).div_euclid(16),
                )
            })
    }
    pub fn actor(&self) -> ActorId {
        match &self.identity {
            ContactIdentity::Player(lease) => ActorId::Player(lease.gid),
            ContactIdentity::Entity { id, .. } => ActorId::Entity(*id),
        }
    }

    pub fn target(&self) -> Target {
        Target::Chunk {
            key: self.chunk,
            part: match &self.identity {
                ContactIdentity::Player(lease) => ChunkPart::Player(lease.gid),
                ContactIdentity::Entity { id, .. } => ChunkPart::Entity(*id),
            },
            inventory: None,
        }
    }

    pub fn valid(&self) -> bool {
        let position = self.position.map(f64::from_bits);
        let width = f64::from_bits(self.half_width);
        let height = f64::from_bits(self.height);
        let provenance = match &self.identity {
            ContactIdentity::Player(lease) => {
                lease.gid.server == self.owner.server
                    && lease.incarnation != 0
                    && !lease.key.0.is_nil()
            }
            ContactIdentity::Entity { id, birth } => {
                birth.source.server == id.origin
                    && birth.source.generation != 0
                    && !birth.uuid.is_nil()
            }
        };
        let epoch_valid = match self.epoch {
            SpatialEpoch::Admission { lease, .. } => {
                matches!(self.identity, ContactIdentity::Player(actual) if actual == lease)
            }
            SpatialEpoch::Action(reference) => reference.id.incarnation != 0,
        };
        provenance
            && epoch_valid
            && self.owner.generation != 0
            && self.world == self.chunk.world
            && position
                .iter()
                .all(|value| value.is_finite() && value.abs() <= 30_000_000.0)
            && width.is_finite()
            && (0.0..=1024.0).contains(&width)
            && width > 0.0
            && height.is_finite()
            && (0.0..=2048.0).contains(&height)
            && height > 0.0
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct PickupContact {
    pub picker: ContactObservation,
    pub item: ContactObservation,
    pub producer: PeerGeneration,
    pub tick: TickStamp,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct MergeContact {
    pub source: ContactObservation,
    pub destination: ContactObservation,
    pub producer: PeerGeneration,
    pub tick: TickStamp,
}

impl MergeContact {
    pub fn valid(&self) -> bool {
        if !self.source.valid()
            || !self.destination.valid()
            || self.producer.generation == 0
            || !matches!(self.source.identity, ContactIdentity::Entity { .. })
            || !matches!(self.destination.identity, ContactIdentity::Entity { .. })
            || self.source.actor() == self.destination.actor()
            || self.source.world != self.destination.world
            || self.source.simulation_tick != self.tick
            || self.destination.simulation_tick != self.tick
        {
            return false;
        }
        let source = self.source.position.map(f64::from_bits);
        let destination = self.destination.position.map(f64::from_bits);
        let horizontal = f64::from_bits(self.source.half_width)
            + f64::from_bits(self.destination.half_width)
            + 0.5;
        (source[0] - destination[0]).abs() < horizontal
            && (source[2] - destination[2]).abs() < horizontal
            && source[1] < destination[1] + f64::from_bits(self.destination.height)
            && destination[1] < source[1] + f64::from_bits(self.source.height)
    }
}

impl PickupContact {
    pub fn valid(&self) -> bool {
        if !self.picker.valid()
            || !self.item.valid()
            || self.producer != self.picker.owner
            || self.picker.actor() == self.item.actor()
            || !matches!(self.item.identity, ContactIdentity::Entity { .. })
            || self.picker.world != self.item.world
            || self.picker.simulation_tick != self.tick
            || self.item.simulation_tick != self.tick
        {
            return false;
        }
        let picker = self.picker.position.map(f64::from_bits);
        let item = self.item.position.map(f64::from_bits);
        let reach =
            f64::from_bits(self.picker.half_width) + 1.0 + f64::from_bits(self.item.half_width);
        (picker[0] - item[0]).abs() < reach
            && (picker[2] - item[2]).abs() < reach
            && picker[1] - 0.5 < item[1] + f64::from_bits(self.item.height)
            && picker[1] + f64::from_bits(self.picker.height) + 0.5 > item[1]
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn accepted_entity_epoch_survives_half_range_and_wrap() {
        use crate::canonical_action::{ActionId, ActionRef};
        use crate::entity_body::{EntityBody, EntityPayload, EntitySpatial, ExperienceOrbState};
        use crate::identity::ServerId;
        let id = GlobalEntityId {
            origin: ServerId(1),
            local_id: 7,
            generation: 1,
        };
        let reference = ActionRef {
            tick: TickStamp(1),
            id: ActionId {
                actor: ActorId::Entity(id),
                incarnation: 1,
                sequence: 0,
                family: 1,
            },
        };
        let owner = PeerGeneration {
            server: ServerId(1),
            generation: 1,
        };
        let chunk = ChunkKey::new(WorldId::new(0, 0), 0, 0);
        let body = EntityBody { mount: None,
            tags: Default::default(),
            tags_revision: 1,
            id,
            birth: EntityBirth {
                action: reference,
                source: owner,
                uuid: uuid::Uuid::from_u128(7),
            },
            owner,
            kind: 1,
            passengers: Default::default(),
            relation_versions: Default::default(),
            spatial_epoch: SpatialEpoch::Action(reference),
            spatial: EntitySpatial {
                world: chunk.world,
                position: [0; 3],
                velocity_micro: [0; 3],
                yaw: 0,
                pitch: 0,
                on_ground: true,
            },
            payload: EntityPayload::Experience(ExperienceOrbState {
                amount: 1,
                count: 1,
                health_milli: 5000,
                age: 0,
                last_lifetime_tick: None,
            }),
        };
        for tick in [TickStamp(40000), TickStamp(0)] {
            assert!(body.valid_at(tick));
            let observation = ContactObservation::entity(
                &body,
                chunk,
                &body.spatial,
                0.25,
                0.5,
                tick,
                PlayerSeq(2),
            )
            .unwrap();
            assert_eq!(observation.epoch, SpatialEpoch::Action(reference));
            let mut malformed = observation;
            malformed.owner.generation = 0;
            assert!(!malformed.valid());
        }
    }
}

impl PickupContact {
    pub fn valid_hopper(&self) -> bool {
        if self.picker.half_width != 0.49_f64.to_bits() || self.picker.height != 0.7_f64.to_bits()
            || self.item.half_width != 0.125_f64.to_bits() || self.item.height != 0.25_f64.to_bits()
            || !self.picker.valid() || !self.item.valid() || self.producer != self.picker.owner
            || self.picker.actor() == self.item.actor()
            || !matches!(self.picker.identity, ContactIdentity::Entity { .. })
            || !matches!(self.item.identity, ContactIdentity::Entity { .. })
            || self.picker.world != self.item.world
            || self.picker.simulation_tick != self.tick || self.item.simulation_tick != self.tick
        { return false; }
        let picker = self.picker.position.map(f64::from_bits);
        let item = self.item.position.map(f64::from_bits);
        let width = f64::from_bits(self.item.half_width);
        let height = f64::from_bits(self.item.height);
        let intersects = |radius: f64, low: f64, high: f64| {
            picker[0] - radius < item[0] + width && picker[0] + radius > item[0] - width
                && picker[2] - radius < item[2] + width && picker[2] + radius > item[2] - width
                && picker[1] + low < item[1] + height && picker[1] + high > item[1]
        };
        intersects(0.5, 0.6875, 2.0) || intersects(0.74, 0.0, 0.7)
    }
}
