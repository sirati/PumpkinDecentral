use std::collections::BTreeMap;
use serde::{Deserialize, Serialize};
use crate::actor::ActorId;
use crate::canonical_action::{ActionRef, ApplyFailure, Cell, ChunkPart, SpatialEpoch, Target, Value};
use crate::entity_body::EntityBirth;
use crate::item_contact::{ContactIdentity, ContactObservation};
use crate::protocol::ChunkKey;

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct ProjectileShooterTeleport {
    pub shooter: ContactObservation,
    pub destination: ChunkKey,
    pub position: [u64; 3],
    pub projectile_birth: EntityBirth,
    pub projectile_epoch: SpatialEpoch,
}

impl ProjectileShooterTeleport {
    pub fn destination_target(&self) -> Target {
        Target::Chunk { key: self.destination, part: match self.shooter.actor() {
            ActorId::Player(player) => ChunkPart::Player(player), ActorId::Entity(entity) => ChunkPart::Entity(entity),
            ActorId::Peer(_) => unreachable!(),
        }, inventory: None }
    }

    pub fn targets(&self) -> [Target; 2] { [self.shooter.target(), self.destination_target()] }

    pub fn valid(&self) -> bool {
        let position = self.position.map(f64::from_bits);
        self.shooter.valid() && self.destination.world == self.shooter.world
            && position.iter().all(|value| value.is_finite() && value.abs() <= 30_000_000.0)
            && (position[0].floor() as i32).div_euclid(16) == self.destination.x()
            && (position[2].floor() as i32).div_euclid(16) == self.destination.z()
            && self.projectile_birth.source.generation != 0 && !self.projectile_birth.uuid.is_nil()
    }

    pub fn project(&self, reference: ActionRef, values: &BTreeMap<Target, Cell>) -> Result<Vec<(Target, Option<Value>)>, ApplyFailure> {
        if !self.valid() { return Err(ApplyFailure::InvalidAction); }
        let source = self.shooter.target();
        let destination = self.destination_target();
        if source != destination && values.contains_key(&destination) { return Err(ApplyFailure::Condition(destination)); }
        let previous = values.get(&source).ok_or(ApplyFailure::Condition(source))?;
        let mut value = previous.value.clone();
        if let Some(living) = match &mut value {
            Value::Living(living) => Some(living.as_mut()),
            Value::Entity(body) => match &mut body.payload { crate::entity_body::EntityPayload::Living(living) => Some(living), _ => None },
            _ => None,
        } {
            for (target, cell) in values {
                if let Some((body, property)) = crate::living_property::body_target(*target)
                    && body == source && let Value::LivingProperty(facet) = &cell.value
                    && !property.write(living, facet) {
                    return Err(ApplyFailure::Type(*target));
                }
            }
        }
        match (&self.shooter.identity, &mut value) {
            (ContactIdentity::Player(lease), Value::Living(living)) => {
                if !living.belongs_to(ActorId::Player(lease.gid)) { return Err(ApplyFailure::Type(source)); }
                let avatar = living.avatar.as_mut().ok_or(ApplyFailure::Type(source))?;
                if avatar.spatial_epoch != self.shooter.epoch { return Err(ApplyFailure::Condition(source)); }
                avatar.world = self.destination.world;
                avatar.position = self.position;
                avatar.fall_distance = 0_f32.to_bits();
                avatar.on_ground = false;
                avatar.movement_tick = reference.tick;
                avatar.movement_sequence = crate::identity::PlayerSeq(reference.id.sequence);
                avatar.spatial_epoch = SpatialEpoch::Action(reference);
                living.combat.velocity_micro = [0;3];
            }
            (ContactIdentity::Entity { id, birth }, Value::Entity(body)) => {
                if body.id != *id || body.birth != *birth || body.owner != self.shooter.owner || body.spatial_epoch != self.shooter.epoch {
                    return Err(ApplyFailure::Condition(source));
                }
                body.spatial.world = self.destination.world;
                body.spatial.position = self.position;
                body.spatial.velocity_micro = [0;3];
                body.spatial.on_ground = false;
                body.spatial_epoch = SpatialEpoch::Action(reference);
                if let crate::entity_body::EntityPayload::Living(living) = &mut body.payload {
                    living.combat.velocity_micro = [0;3];
                    if let Some(mob) = &mut living.mob { mob.brain.fall_distance = 0_f32.to_bits(); }
                }
            }
            _ => return Err(ApplyFailure::Type(source)),
        }
        let mut writes = Vec::new();
        if source != destination { writes.push((source, None)); }
        writes.push((destination, Some(value)));
        Ok(writes)
    }
}
