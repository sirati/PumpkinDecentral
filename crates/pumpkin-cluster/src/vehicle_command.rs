use serde::{Deserialize, Serialize};

use crate::action_command::{
    ActionFootprint, ActorReference, CommandContext, CommandResult, ResolutionRecord,
};
use crate::actor_relation::{ActorRelation, ActorRelationParticipant};
use crate::canonical_action::*;
use crate::entity_body::{EntityBody, EntityPayload, EntitySpatial, VehicleKind};
use crate::item_contact::ContactIdentity;
use crate::vehicle_property::{VehicleProperty as P, VehiclePropertyValue as V};

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct SeatAssociation {
    pub seat: u8,
    pub rider: ActorReference,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub enum VehicleStepCause {
    Timers,
    Activator {
        block: crate::world_command::BlockLocation,
        passengers: Box<[SeatAssociation]>,
    },
    BoatSubmersion {
        passengers: Box<[SeatAssociation]>,
    },
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct VehicleStep {
    pub vehicle: ActorReference,
    pub cause: VehicleStepCause,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct Dismount {
    pub vehicle: ActorReference,
    pub seat: u8,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct DriverClaim {
    pub sequence: crate::identity::PlayerSeq,
    pub transform: Option<crate::actor_relation::VehicleClaimedTransform>,
    pub forward: i8,
    pub sideways: i8,
    pub jumping: bool,
    pub paddles: Option<[bool; 2]>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct VehicleControl {
    pub vehicle: ActorReference,
    pub claim: DriverClaim,
    pub accepted: EntitySpatial,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub struct EntityVelocityCut {
    pub reliable: [i64; 3],
    pub body_revision: u64,
    pub effective: [i64; 3],
}

impl EntityVelocityCut {
    pub fn resolve(self, body: &EntityBody, revision: u64) -> [i64; 3] {
        if revision == self.body_revision && body.spatial.velocity_micro == self.reliable {
            self.effective
        } else {
            body.spatial.velocity_micro
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct VehicleObservation {
    pub actor: ActorReference,
    pub position: [u64; 3],
    pub yaw: u32,
    pub on_ground: bool,
    pub sequence: crate::identity::PlayerSeq,
    pub velocity: EntityVelocityCut,
}

impl VehicleObservation {
    pub fn target(&self) -> Target {
        self.actor.body_target()
    }

    pub fn contact(
        &self,
        tick: crate::time::TickStamp,
        geometry: crate::combat_command::ActorGeometry,
    ) -> Result<crate::item_contact::ContactObservation, ApplyFailure> {
        if !matches!(self.actor, ActorReference::Entity { .. })
            || !f32::from_bits(self.yaw).is_finite()
        {
            return Err(ApplyFailure::InvalidAction);
        }
        let contact = crate::item_contact::ContactObservation {
            identity: identity(&self.actor),
            owner: self.actor.owner(),
            epoch: self.actor.epoch(),
            world: self.actor.chunk().world,
            chunk: self.actor.chunk(),
            position: self.position,
            half_width: geometry.half_width,
            height: geometry.height,
            simulation_tick: tick,
            observed_sequence: self.sequence,
        };
        if !contact.valid() {
            return Err(ApplyFailure::InvalidAction);
        }
        Ok(contact)
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct VehicleAttack {
    pub source: crate::combat_command::ActorObservation,
    pub vehicle: VehicleObservation,
    pub protocol: crate::combat_command::MeleeProtocol,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct VehicleTerminal {
    pub vehicle: VehicleObservation,
    pub rules: crate::world_rules::WorldRulesWitness,
    pub outputs: Box<[crate::entity_origin::EntityOriginIdentity]>,
    pub passengers: Box<[SeatAssociation]>,
}

fn source_identity<S>(
    context: &CommandContext<'_, S>,
    source: &ActorReference,
) -> Result<ActionId, ApplyFailure> {
    let crate::action_identity::ActionIdentity::Issued(id) = context.identity else {
        return Err(ApplyFailure::InvalidAction);
    };
    let incarnation = match source {
        ActorReference::Player { lease, .. } => lease.incarnation,
        ActorReference::Entity { id, .. } => id.generation,
    };
    if id.actor != source.actor() || id.incarnation != incarnation {
        return Err(ApplyFailure::InvalidAction);
    }
    Ok(id)
}

fn identity(actor: &ActorReference) -> ContactIdentity {
    match actor {
        ActorReference::Player { lease, .. } => ContactIdentity::Player(*lease),
        ActorReference::Entity { id, birth, .. } => ContactIdentity::Entity {
            id: *id,
            birth: birth.clone(),
        },
    }
}

fn actor_matches(reference: &ActorReference, cell: &Cell) -> bool {
    match (reference, &cell.value) {
        (
            ActorReference::Player {
                lease,
                chunk,
                epoch,
                owner,
            },
            Value::Living(living),
        ) => {
            living.belongs_to(crate::actor::ActorId::Player(lease.gid))
                && living.avatar.as_ref().is_some_and(|avatar| {
                    avatar.spatial_epoch == *epoch && avatar.world == chunk.world
                })
                && owner.server == lease.gid.server
        }
        (
            ActorReference::Entity {
                id,
                birth,
                chunk,
                epoch,
                owner,
            },
            Value::Entity(body),
        ) => {
            body.id == *id
                && body.birth == *birth
                && body.spatial_epoch == *epoch
                && body.owner == *owner
                && body.belongs_to(*id, *chunk)
        }
        _ => false,
    }
}

fn vehicle<'a>(reference: &ActorReference, cell: &'a Cell) -> Result<&'a EntityBody, ApplyFailure> {
    let ActorReference::Entity {
        id,
        birth,
        epoch,
        owner,
        chunk,
    } = reference
    else {
        return Err(ApplyFailure::InvalidAction);
    };
    let Value::Entity(body) = &cell.value else {
        return Err(ApplyFailure::Type(reference.body_target()));
    };
    if body.id != *id
        || body.birth != *birth
        || body.spatial_epoch != *epoch
        || body.owner != *owner
        || !body.belongs_to(*id, *chunk)
        || !matches!(body.payload, EntityPayload::Vehicle(_))
    {
        return Err(ApplyFailure::Condition(reference.body_target()));
    }
    Ok(body)
}

fn mount_body<'a>(
    reference: &ActorReference,
    cell: &'a Cell,
) -> Result<&'a EntityBody, ApplyFailure> {
    if !actor_matches(reference, cell) {
        return Err(ApplyFailure::Condition(reference.body_target()));
    }
    let Value::Entity(body) = &cell.value else {
        return Err(ApplyFailure::Type(reference.body_target()));
    };
    if !matches!(&body.payload, EntityPayload::Vehicle(_))
        && !matches!(&body.payload, EntityPayload::Living(living)
        if living.mob.as_ref().is_some_and(|mob| matches!(mob.species, crate::mob_continuation::MobSpecies::Horse { .. })))
    {
        return Err(ApplyFailure::Type(reference.body_target()));
    }
    Ok(body)
}

fn relation(actor: &ActorReference, relation: ActorRelation) -> Target {
    Target::Chunk {
        key: actor.chunk(),
        part: ChunkPart::ActorRelation {
            actor: actor.actor(),
            relation,
        },
        inventory: None,
    }
}

fn property(actor: &ActorReference, property: P) -> Result<Target, ApplyFailure> {
    let ActorReference::Entity { id, chunk, .. } = actor else {
        return Err(ApplyFailure::InvalidAction);
    };
    Ok(Target::Chunk {
        key: *chunk,
        part: ChunkPart::VehicleProperty {
            actor: *id,
            property,
        },
        inventory: None,
    })
}

fn body_cell<S: ActionState>(
    context: &CommandContext<'_, S>,
    actor: &ActorReference,
) -> Result<Cell, ApplyFailure> {
    context
        .lookup(actor.body_target())?
        .cloned()
        .ok_or(ApplyFailure::Condition(actor.body_target()))
}

fn detach_footprint(
    vehicle: &ActorReference,
    passengers: &[SeatAssociation],
    footprint: &mut ActionFootprint,
) -> Result<(), ApplyFailure> {
    footprint.reads.insert(vehicle.body_target());
    footprint
        .reads
        .insert(relation(vehicle, ActorRelation::Mount));
    for seat in 0..32 {
        footprint
            .reads
            .insert(relation(vehicle, ActorRelation::Passenger(seat)));
    }
    let mut seats = std::collections::BTreeSet::new();
    let mut riders = std::collections::BTreeSet::new();
    for passenger in passengers {
        if passenger.seat >= 32
            || !seats.insert(passenger.seat)
            || !riders.insert(passenger.rider.actor())
            || passenger.rider.chunk().world != vehicle.chunk().world
        {
            return Err(ApplyFailure::InvalidAction);
        }
        footprint.reads.extend([
            passenger.rider.body_target(),
            relation(&passenger.rider, ActorRelation::Mount),
        ]);
        footprint.writes.extend([
            relation(vehicle, ActorRelation::Passenger(passenger.seat)),
            relation(&passenger.rider, ActorRelation::Mount),
        ]);
    }
    footprint
        .counterfactual_reads
        .extend(footprint.reads.iter().copied());
    footprint
        .counterfactual_writes
        .extend(footprint.writes.iter().copied());
    Ok(())
}

fn not_applied<S>(
    context: &CommandContext<'_, S>,
    footprint: ActionFootprint,
) -> Result<ResolutionRecord, ApplyFailure> {
    let mut record = ResolutionRecord {
        identity: context.identity,
        tick: context.tick,
        footprint,
        result: CommandResult::NotApplied,
    };
    record.normalize_footprint()?;
    Ok(record)
}

fn absent_body<S>(
    context: &CommandContext<'_, S>,
    mut footprint: ActionFootprint,
    queried: impl IntoIterator<Item = Target>,
) -> Result<ResolutionRecord, ApplyFailure> {
    footprint
        .counterfactual_reads
        .extend(footprint.reads.iter().copied());
    footprint.reads = queried.into_iter().collect();
    not_applied(context, footprint)
}

fn required_inputs<S>(
    context: &CommandContext<'_, S>,
    footprint: &ActionFootprint,
) -> Result<bool, ApplyFailure> {
    let mut present = true;
    for target in &footprint.reads {
        present &= context.lookup(*target)?.is_some();
    }
    Ok(present)
}

fn record<S: ActionState>(
    context: &CommandContext<'_, S>,
    footprint: ActionFootprint,
    effects: Vec<Effect>,
) -> Result<ResolutionRecord, ApplyFailure> {
    let mut projected = std::collections::BTreeMap::new();
    for target in footprint.reads.union(&footprint.writes) {
        if let Some(cell) = context.lookup(*target)? {
            projected.insert(*target, cell.clone());
        }
    }
    let mut touched = std::collections::BTreeSet::new();
    for effect in effects {
        let outputs = match effect {
            Effect::Put { target, value } => std::collections::BTreeMap::from([(target, value)]),
            Effect::TransformItems { intent } => {
                let crate::action_identity::ActionIdentity::Issued(id) = context.identity else {
                    return Err(ApplyFailure::InvalidAction);
                };
                intent
                    .derive(context.state, id, context.tick, &projected)?
                    .into_iter()
                    .map(|(target, cell)| (target, Value::Inventory(cell)))
                    .collect()
            }
            _ => return Err(ApplyFailure::InvalidAction),
        };
        for (target, value) in outputs {
            if !footprint.writes.contains(&target) {
                return Err(ApplyFailure::InvalidAction);
            }
            let before = context
                .values
                .get(&target)
                .ok_or(ApplyFailure::Condition(target))?;
            let revision = before
                .revision
                .checked_add(1)
                .ok_or(ApplyFailure::Overflow(target))?;
            projected.insert(target, Cell { revision, value });
            touched.insert(target);
        }
    }
    let writes = touched
        .into_iter()
        .filter_map(|target| {
            let before = context.values.get(&target).unwrap();
            let after = projected.remove(&target).unwrap();
            (before.value != after.value).then(|| AppliedWrite {
                target,
                before: Some(before.clone()),
                after: Some(after),
            })
        })
        .collect();
    let mut record = ResolutionRecord {
        identity: context.identity,
        tick: context.tick,
        footprint,
        result: CommandResult::Applied(writes),
    };
    record.normalize_footprint()?;
    Ok(record)
}

struct Values<'a>(&'a std::collections::BTreeMap<Target, Cell>);
impl ActionState for Values<'_> {
    fn read(&self, _: TruthView, target: Target) -> Option<Cell> {
        self.0.get(&target).cloned()
    }
    fn commit(
        &mut self,
        _: ActionContext,
        _: TruthView,
        _: &[AppliedWrite],
    ) -> Result<(), ApplyFailure> {
        Err(ApplyFailure::InvalidAction)
    }
}

impl VehicleStep {
    pub fn footprint<S: ActionState>(
        &self,
        _context: &CommandContext<'_, S>,
    ) -> Result<ActionFootprint, ApplyFailure> {
        let mut footprint = ActionFootprint::default();
        footprint.reads.insert(self.vehicle.body_target());
        for property_kind in [
            P::LifecycleTick,
            P::Fuel,
            P::FurnacePush,
            P::Fuse,
            P::Damage,
            P::Wobble,
        ] {
            let target = property(&self.vehicle, property_kind)?;
            footprint.reads.insert(target);
            footprint.writes.insert(target);
        }
        match &self.cause {
            VehicleStepCause::Timers => {}
            VehicleStepCause::Activator { block, passengers } => {
                if !block.valid() || block.world != self.vehicle.chunk().world {
                    return Err(ApplyFailure::InvalidAction);
                }
                footprint.reads.insert(block.target());
                let target = property(&self.vehicle, P::HopperEnabled)?;
                footprint.reads.insert(target);
                footprint.writes.insert(target);
                if !passengers.is_empty() {
                    detach_footprint(&self.vehicle, passengers, &mut footprint)?;
                }
            }
            VehicleStepCause::BoatSubmersion { passengers } => {
                footprint
                    .reads
                    .insert(property(&self.vehicle, P::Underwater)?);
                detach_footprint(&self.vehicle, passengers, &mut footprint)?;
            }
        }
        footprint
            .counterfactual_reads
            .extend(footprint.reads.iter().copied());
        footprint
            .counterfactual_writes
            .extend(footprint.writes.iter().copied());
        Ok(footprint)
    }

    pub fn derive<S: ActionState>(
        &self,
        context: &CommandContext<'_, S>,
    ) -> Result<ResolutionRecord, ApplyFailure> {
        let crate::action_identity::ActionIdentity::Issued(id) = context.identity else {
            return Err(ApplyFailure::InvalidAction);
        };
        if id.actor != self.vehicle.actor()
            || !matches!(&self.vehicle, ActorReference::Entity { id: entity, .. } if id.incarnation == entity.generation)
        {
            return Err(ApplyFailure::InvalidAction);
        }
        let footprint = self.footprint(context)?;
        if context.lookup(self.vehicle.body_target())?.is_none() {
            return absent_body(context, footprint, [self.vehicle.body_target()]);
        }
        if !required_inputs(context, &footprint)? {
            return not_applied(context, footprint);
        }
        let body = vehicle(&self.vehicle, &context.values[&self.vehicle.body_target()])?;
        let EntityPayload::Vehicle(state) = &body.payload else {
            unreachable!()
        };
        let passengers = match &self.cause {
            VehicleStepCause::Timers => &[][..],
            VehicleStepCause::Activator { passengers, .. }
            | VehicleStepCause::BoatSubmersion { passengers } => passengers,
        };
        if !matches!(self.cause, VehicleStepCause::Timers)
            && (body.mount.is_some()
                || body.passengers.len() != passengers.len()
                || passengers.iter().any(|association| {
                    body.passengers.get(&association.seat) != Some(&identity(&association.rider))
                }))
        {
            return Err(ApplyFailure::InvalidAction);
        }
        for association in passengers {
            let target = association.rider.body_target();
            let cell = context
                .values
                .get(&target)
                .ok_or(ApplyFailure::Condition(target))?;
            if !actor_matches(&association.rider, cell)
                || matches!(&association.rider, ActorReference::Player { lease, .. } if !context.state.validate_live_player_lease(*lease))
            {
                return Err(ApplyFailure::Condition(target));
            }
        }
        let detach = if passengers.is_empty() {
            None
        } else {
            if body.mount.is_some()
                || body.passengers.len() != passengers.len()
                || passengers.iter().any(|association| {
                    body.passengers.get(&association.seat) != Some(&identity(&association.rider))
                })
            {
                return Err(ApplyFailure::InvalidAction);
            }
            let participants = passengers
                .iter()
                .map(|association| ActorRelationParticipant {
                    identity: identity(&association.rider),
                    chunk: association.rider.chunk(),
                    owner: association.rider.owner(),
                    epoch: association.rider.epoch(),
                })
                .collect::<Vec<_>>();
            Some(
                crate::actor_relation::capture_owner_detach_from_participants(
                    &Values(context.values),
                    body,
                    self.vehicle.chunk(),
                    &participants,
                )
                .ok_or(ApplyFailure::InvalidAction)?,
            )
        };
        let activation = if let VehicleStepCause::Activator { block, .. } = &self.cause {
            let crate::action_identity::ActionIdentity::Issued(_) = context.identity else {
                return Err(ApplyFailure::InvalidAction);
            };
            Some(crate::vehicle_tick::VehicleActivation {
                block: block.target(),
                kind: state.kind,
                position: body.spatial.position,
                detach: detach.clone(),
            })
        } else {
            None
        };
        let intent = crate::vehicle_tick::VehicleTickIntent {
            body: self.vehicle.body_target(),
            birth: body.birth.clone(),
            owner: body.owner,
            epoch: body.spatial_epoch,
            activation,
        };
        let mut effects = intent.derive_with(context.state, context.tick, context.values)?;
        if matches!(self.cause, VehicleStepCause::BoatSubmersion { .. }) {
            if !matches!(state.kind, VehicleKind::Boat | VehicleKind::ChestBoat) {
                return Err(ApplyFailure::InvalidAction);
            }
            let target = property(&self.vehicle, P::Underwater)?;
            let Some(Cell {
                value: Value::VehicleProperty(V::Unsigned(ticks)),
                ..
            }) = context.values.get(&target)
            else {
                return Err(ApplyFailure::Type(target));
            };
            let lifecycle = property(&self.vehicle, P::LifecycleTick)?;
            let Some(Cell {
                value: Value::VehicleProperty(V::Tick(previous)),
                ..
            }) = context.values.get(&lifecycle)
            else {
                return Err(ApplyFailure::Type(lifecycle));
            };
            if *ticks >= 60 && previous.is_none_or(|previous| context.tick.is_after(previous)) {
                effects.extend(detach.into_iter().flat_map(|plan| plan.effects));
            }
        }
        record(context, footprint, effects)
    }
}

impl Dismount {
    pub fn footprint(&self, source: &ActorReference) -> Result<ActionFootprint, ApplyFailure> {
        if self.seat >= 32 || source.chunk().world != self.vehicle.chunk().world {
            return Err(ApplyFailure::InvalidAction);
        }
        let reads = std::collections::BTreeSet::from([
            source.body_target(),
            self.vehicle.body_target(),
            relation(source, ActorRelation::Mount),
            relation(&self.vehicle, ActorRelation::Passenger(self.seat)),
        ]);
        let writes = std::collections::BTreeSet::from([
            relation(source, ActorRelation::Mount),
            relation(&self.vehicle, ActorRelation::Passenger(self.seat)),
        ]);
        Ok(ActionFootprint {
            counterfactual_reads: reads.clone(),
            counterfactual_writes: writes.clone(),
            reads,
            writes,
        })
    }
    pub fn derive<S: ActionState>(
        &self,
        context: &CommandContext<'_, S>,
        source: &ActorReference,
    ) -> Result<ResolutionRecord, ApplyFailure> {
        source_identity(context, source)?;
        let footprint = self.footprint(source)?;
        if context.lookup(self.vehicle.body_target())?.is_none() {
            return absent_body(context, footprint, [self.vehicle.body_target()]);
        }
        if context.lookup(source.body_target())?.is_none() {
            return absent_body(
                context,
                footprint,
                [self.vehicle.body_target(), source.body_target()],
            );
        }
        if !required_inputs(context, &footprint)? {
            return not_applied(context, footprint);
        }
        mount_body(
            &self.vehicle,
            context
                .values
                .get(&self.vehicle.body_target())
                .ok_or(ApplyFailure::Condition(self.vehicle.body_target()))?,
        )?;
        let crate::action_identity::ActionIdentity::Issued(id) = context.identity else {
            return Err(ApplyFailure::InvalidAction);
        };
        let source_cell = context
            .values
            .get(&source.body_target())
            .ok_or(ApplyFailure::Condition(source.body_target()))?;
        if id.actor != source.actor()
            || !actor_matches(source, source_cell)
            || match source {
                ActorReference::Player { lease, .. } => {
                    id.incarnation != lease.incarnation
                        || !context.state.validate_live_player_lease(*lease)
                }
                ActorReference::Entity { id: entity, .. } => id.incarnation != entity.generation,
            }
        {
            return Err(ApplyFailure::InvalidAction);
        }
        let mount = relation(source, ActorRelation::Mount);
        let seat = relation(&self.vehicle, ActorRelation::Passenger(self.seat));
        if context.values.get(&mount).map(|cell| &cell.value)
            != Some(&Value::ActorRelation(Some(identity(&self.vehicle))))
            || context.values.get(&seat).map(|cell| &cell.value)
                != Some(&Value::ActorRelation(Some(identity(source))))
        {
            return Err(ApplyFailure::Condition(mount));
        }
        record(
            context,
            footprint,
            vec![
                Effect::Put {
                    target: mount,
                    value: Value::ActorRelation(None),
                },
                Effect::Put {
                    target: seat,
                    value: Value::ActorRelation(None),
                },
            ],
        )
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::actor::ActorId;
    use crate::entity_body::{EntityBirth, VehicleState};
    use crate::identity::ServerId;
    use crate::protocol::{ChunkKey, GlobalEntityId, WorldId};
    use crate::realm_completion::PeerGeneration;
    use crate::time::TickStamp;
    use std::collections::BTreeMap;

    pub(super) fn body(local_id: i32, chunk: ChunkKey, kind: VehicleKind) -> EntityBody {
        let owner = PeerGeneration {
            server: ServerId(1),
            generation: 2,
        };
        let id = GlobalEntityId {
            origin: owner.server,
            local_id,
            generation: owner.generation,
        };
        let reference = ActionRef {
            id: ActionId {
                actor: ActorId::Entity(id),
                incarnation: id.generation,
                sequence: 1,
                family: 4,
            },
            tick: TickStamp(1),
        };
        EntityBody {
            id,
            birth: EntityBirth {
                action: reference,
                source: owner,
                uuid: uuid::Uuid::from_u128(local_id as u128),
            },
            owner,
            kind: 1,
            spatial_epoch: SpatialEpoch::Action(reference),
            spatial: EntitySpatial {
                world: chunk.world,
                position: [f64::from(chunk.x()) * 16.0 + 1.0, 64.0, 1.0].map(f64::to_bits),
                velocity_micro: [0; 3],
                yaw: 0.0_f32.to_bits(),
                pitch: 0.0_f32.to_bits(),
                on_ground: true,
            },
            tags: Default::default(),
            tags_revision: 1,
            mount: None,
            passengers: Default::default(),
            relation_versions: Default::default(),
            payload: EntityPayload::Vehicle(VehicleState::new(kind)),
        }
    }
    pub(super) fn reference(body: &EntityBody, chunk: ChunkKey) -> ActorReference {
        ActorReference::Entity {
            id: body.id,
            birth: body.birth.clone(),
            chunk,
            epoch: body.spatial_epoch,
            owner: body.owner,
        }
    }
    pub(super) fn seed(
        body: &EntityBody,
        actor: &ActorReference,
        cells: &mut BTreeMap<Target, Cell>,
    ) {
        cells.insert(
            actor.body_target(),
            Cell {
                revision: 1,
                value: Value::Entity((body.clone()).into()),
            },
        );
        for property_kind in P::ALL {
            cells.insert(
                property(actor, property_kind).unwrap(),
                Cell {
                    revision: 1,
                    value: Value::VehicleProperty(
                        crate::vehicle_property::read(
                            &Value::Entity((body.clone()).into()),
                            property_kind,
                        )
                        .unwrap(),
                    ),
                },
            );
        }
        cells.insert(
            relation(actor, ActorRelation::Mount),
            Cell {
                revision: 1,
                value: Value::ActorRelation(body.mount.clone()),
            },
        );
        for seat in 0..32 {
            cells.insert(
                relation(actor, ActorRelation::Passenger(seat)),
                Cell {
                    revision: 1,
                    value: Value::ActorRelation(body.passengers.get(&seat).cloned()),
                },
            );
        }
    }
    #[test]
    fn absent_vehicle_is_not_applied_but_unknown_history_defers_without_current_state_fallback() {
        let chunk = ChunkKey::new(WorldId::new(0, 1), 0, 0);
        let body = body(1, chunk, VehicleKind::Minecart);
        let reference = reference(&body, chunk);
        let command = VehicleStep {
            vehicle: reference.clone(),
            cause: VehicleStepCause::Timers,
        };
        let mut current = BTreeMap::new();
        seed(&body, &reference, &mut current);
        let state = Values(&current);
        let values = BTreeMap::new();
        let absent = std::collections::BTreeSet::from([reference.body_target()]);
        let id = ActionId {
            actor: reference.actor(),
            incarnation: body.id.generation,
            sequence: 2,
            family: 4,
        };
        let mut context = CommandContext {
            state: &state,
            identity: crate::action_identity::ActionIdentity::Issued(id),
            tick: TickStamp(2),
            values: &values,
            known_absent: &absent,
        };
        let resolution = command.derive(&context).unwrap();
        assert!(matches!(resolution.result, CommandResult::NotApplied));
        assert_eq!(
            resolution.footprint.reads,
            std::collections::BTreeSet::from([reference.body_target()])
        );
        assert!(resolution.footprint.writes.is_empty());
        assert!(
            resolution
                .footprint
                .counterfactual_reads
                .contains(&property(&reference, P::Fuse).unwrap())
        );
        assert!(resolution.undo_writes().next().is_none());
        let unknown = std::collections::BTreeSet::new();
        context.known_absent = &unknown;
        assert!(matches!(
            command.derive(&context),
            Err(ApplyFailure::MissingStakeholder(_))
        ));
        assert!(matches!(
            body_cell(&context, &reference),
            Err(ApplyFailure::MissingStakeholder(_))
        ));
    }

    #[test]
    fn entity_velocity_cut_uses_effective_motion_only_for_the_exact_reliable_body_revision() {
        let chunk = ChunkKey::new(WorldId::new(0, 1), 0, 0);
        let mut body = body(1, chunk, VehicleKind::TntMinecart);
        body.spatial.velocity_micro = [100, 0, 200];
        let cut = EntityVelocityCut {
            reliable: [100, 0, 200],
            body_revision: 3,
            effective: [400, 0, 500],
        };
        assert_eq!(cut.resolve(&body, 3), [400, 0, 500]);
        assert_eq!(cut.resolve(&body, 4), [100, 0, 200]);
        body.spatial.velocity_micro[0] = 300;
        assert_eq!(cut.resolve(&body, 3), [300, 0, 200]);
    }

    #[test]
    fn boat_submersion_derives_cross_chunk_ejection_only_from_winning_counter_and_exact_rider() {
        let world = WorldId::new(0, 1);
        let home = ChunkKey::new(world, 0, 0);
        let other = ChunkKey::new(world, 1, 0);
        let mut boat = body(1, home, VehicleKind::Boat);
        let mut rider = body(2, other, VehicleKind::Minecart);
        let boat_ref = reference(&boat, home);
        let rider_ref = reference(&rider, other);
        boat.passengers.insert(0, identity(&rider_ref));
        rider.mount = Some(identity(&boat_ref));
        let EntityPayload::Vehicle(vehicle) = &mut boat.payload else {
            unreachable!()
        };
        vehicle.underwater_ticks = 60;
        let command = VehicleStep {
            vehicle: boat_ref.clone(),
            cause: VehicleStepCause::BoatSubmersion {
                passengers: vec![SeatAssociation {
                    seat: 0,
                    rider: rider_ref.clone(),
                }]
                .into_boxed_slice(),
            },
        };
        let mut cells = BTreeMap::new();
        seed(&boat, &boat_ref, &mut cells);
        seed(&rider, &rider_ref, &mut cells);
        let state = Values(&cells);
        let id = ActionId {
            actor: ActorId::Entity(boat.id),
            incarnation: boat.id.generation,
            sequence: 2,
            family: 4,
        };
        let context = CommandContext {
            known_absent: &std::collections::BTreeSet::new(),
            state: &state,
            values: &cells,
            identity: crate::action_identity::ActionIdentity::Issued(id),
            tick: TickStamp(2),
        };
        let resolved = command.derive(&context).unwrap();
        assert_eq!(
            resolved.footprint.chunks(|_| None).unwrap(),
            std::collections::BTreeSet::from([home, other])
        );
        let CommandResult::Applied(writes) = &resolved.result else {
            unreachable!()
        };
        assert!(writes.iter().any(|write| write.target
            == relation(&rider_ref, ActorRelation::Mount)
            && write.after.as_ref().unwrap().value == Value::ActorRelation(None)));
        assert!(resolved.undo_writes().any(|write| write.target
            == relation(&rider_ref, ActorRelation::Mount)
            && write.after.as_ref().unwrap().value
                == Value::ActorRelation(Some(identity(&boat_ref)))));
        cells
            .get_mut(&property(&boat_ref, P::Underwater).unwrap())
            .unwrap()
            .value = Value::VehicleProperty(V::Unsigned(59));
        let state = Values(&cells);
        let context = CommandContext {
            known_absent: &std::collections::BTreeSet::new(),
            state: &state,
            values: &cells,
            identity: crate::action_identity::ActionIdentity::Issued(id),
            tick: TickStamp(2),
        };
        let CommandResult::Applied(writes) = command.derive(&context).unwrap().result else {
            unreachable!()
        };
        assert!(!writes.iter().any(|write| matches!(
            write.target,
            Target::Chunk {
                part: ChunkPart::ActorRelation { .. },
                ..
            }
        )));
        let omitted = VehicleStep {
            vehicle: boat_ref,
            cause: VehicleStepCause::BoatSubmersion {
                passengers: Box::new([]),
            },
        };
        assert!(omitted.derive(&context).is_err());
    }
}

impl VehicleAttack {
    fn intent<S: ActionState, R: crate::combat_command::MeleeRegistry>(
        &self,
        context: &CommandContext<'_, S>,
        registry: &R,
    ) -> Result<crate::combat_action::CombatIntent, ApplyFailure> {
        use crate::combat_action::{
            AttackClockIntent, AttackContact, AttackIntent, AttackPose, CombatIntent,
        };
        use crate::living_property::LivingProperty as L;
        let crate::action_identity::ActionIdentity::Issued(id) = context.identity else {
            return Err(ApplyFailure::InvalidAction);
        };
        if id.actor != self.source.actor.actor()
            || self.source.actor.actor() == self.vehicle.actor.actor()
            || match &self.source.actor {
                ActorReference::Player { lease, .. } => id.incarnation != lease.incarnation,
                ActorReference::Entity { id: actor, .. } => id.incarnation != actor.generation,
            }
        {
            return Err(ApplyFailure::InvalidAction);
        }
        let mut source = crate::combat_command::winning_living(context, &self.source.actor)?;
        for property in [
            L::Health,
            L::Weapon,
            L::AttackStarted,
            L::AttackTicks,
            L::Attribute(registry.attributes().attack_damage),
            L::Attribute(registry.attributes().attack_speed),
            L::Attribute(registry.range_attribute()),
            L::Attribute(registry.scale_attribute()),
        ] {
            crate::combat_command::hydrate_property(
                context,
                &self.source.actor,
                &mut source,
                property,
            )?;
        }
        if matches!(self.source.actor, ActorReference::Player { .. }) {
            for property in [L::GameMode, L::PlayerStance, L::FallDistance] {
                crate::combat_command::hydrate_property(
                    context,
                    &self.source.actor,
                    &mut source,
                    property,
                )?;
            }
        }
        if source.combat.health_milli == 0
            || source
                .avatar
                .as_ref()
                .is_some_and(|avatar| avatar.game_mode == 3)
        {
            return Err(ApplyFailure::Condition(self.source.target()));
        }
        let cell = body_cell(context, &self.vehicle.actor)?;
        vehicle(&self.vehicle.actor, &cell)?;
        let shape = context
            .state
            .actor_collision_shape(self.vehicle.target(), &cell.value)?;
        if !shape.valid() {
            return Err(ApplyFailure::InvalidAction);
        }
        let height = f64::from_bits(shape.offsets[1][1]) - f64::from_bits(shape.offsets[0][1]);
        let width = [
            shape.offsets[0][0],
            shape.offsets[0][2],
            shape.offsets[1][0],
            shape.offsets[1][2],
        ]
        .into_iter()
        .map(f64::from_bits)
        .map(f64::abs)
        .fold(0.0_f64, f64::max);
        let geometry = registry.geometry(&self.source.actor, &source)?;
        let base = source
            .combat
            .effective_attributes
            .get(&registry.range_attribute())
            .copied()
            .map(f64::from_bits)
            .ok_or(ApplyFailure::InvalidAction)?;
        let reach = source.attribute(registry.range_attribute(), base);
        let contact = AttackContact {
            attacker: self.source.contact(context.tick, geometry)?,
            victim: self.vehicle.contact(
                context.tick,
                crate::combat_command::ActorGeometry {
                    half_width: width.to_bits(),
                    height: height.to_bits(),
                    eye_height: 0.0_f64.to_bits(),
                },
            )?,
            reach: reach.to_bits(),
            eye_height: geometry.eye_height,
            victim_yaw: self.vehicle.yaw,
            victim_on_ground: self.vehicle.on_ground,
            producer: self.source.actor.owner(),
            tick: context.tick,
        };
        if !contact.valid() {
            return Err(ApplyFailure::Condition(self.vehicle.target()));
        }
        let attack = AttackIntent {
            actor: id.actor,
            source: self.source.target(),
            target: self.vehicle.target(),
            tick: context.tick,
            contact,
            pose: AttackPose {
                sprinting: source
                    .avatar
                    .as_ref()
                    .is_some_and(|avatar| avatar.stance.sprinting),
                on_ground: self.source.on_ground,
                fall_distance: source
                    .avatar
                    .as_ref()
                    .map_or(0.0_f32.to_bits(), |avatar| avatar.fall_distance),
                bedrock: self.protocol == crate::combat_command::MeleeProtocol::Bedrock,
                yaw: self.source.yaw,
                knockback_enabled: false,
            },
            attributes: registry.attributes(),
            target_undead: false,
            target_arthropod: false,
            sweeping: false,
        };
        let held_attack = match &self.source.actor {
            ActorReference::Player { lease, chunk, .. } => Some(
                crate::canonical_inventory::InventoryEquipmentBinding::player(
                    *lease,
                    *chunk,
                    context.state.inventory_equipment_attribute_ids(),
                ),
            ),
            _ => None,
        };
        Ok(CombatIntent {
            damage_contacts: Vec::new(),
            falls: Vec::new(),
            damage_parent: None,
            environment: Vec::new(),
            exhaustions: Vec::new(),
            ticks: Vec::new(),
            damages: Vec::new(),
            attacks: Vec::new(),
            vehicle_attacks: vec![crate::vehicle_attack::VehicleAttackIntent { attack }],
            attack_clock: matches!(self.source.actor, ActorReference::Player { .. }).then_some(
                AttackClockIntent {
                    target: self.source.target(),
                    actor: id.actor,
                    tick: context.tick,
                },
            ),
            statistics: Vec::new(),
            item_transforms: Vec::new(),
            held_attack,
            equipment: Vec::new(),
        })
    }

    pub fn footprint<S: ActionState, R: crate::combat_command::MeleeRegistry>(
        &self,
        context: &CommandContext<'_, S>,
        registry: &R,
    ) -> Result<ActionFootprint, ApplyFailure> {
        use crate::living_property::LivingProperty as L;
        let mut reads =
            std::collections::BTreeSet::from([self.source.target(), self.vehicle.target()]);
        let mut writes = [P::Damage, P::Wobble, P::DropLoot]
            .into_iter()
            .map(|property_kind| property(&self.vehicle.actor, property_kind))
            .collect::<Result<std::collections::BTreeSet<_>, _>>()?;
        for property in [
            L::Health,
            L::Weapon,
            L::AttackStarted,
            L::AttackTicks,
            L::Attribute(registry.attributes().attack_damage),
            L::Attribute(registry.attributes().attack_speed),
            L::Attribute(registry.range_attribute()),
            L::Attribute(registry.scale_attribute()),
        ] {
            reads.insert(Target::Chunk {
                key: self.source.actor.chunk(),
                part: ChunkPart::LivingProperty {
                    actor: self.source.actor.actor(),
                    property,
                },
                inventory: None,
            });
        }
        if let ActorReference::Player { lease, chunk, .. } = &self.source.actor {
            for property in [L::GameMode, L::PlayerStance, L::FallDistance] {
                reads.insert(Target::Chunk {
                    key: *chunk,
                    part: ChunkPart::LivingProperty {
                        actor: self.source.id(),
                        property,
                    },
                    inventory: None,
                });
            }
            for property in [L::AttackStarted, L::AttackTicks] {
                writes.insert(Target::Chunk {
                    key: *chunk,
                    part: ChunkPart::LivingProperty {
                        actor: self.source.id(),
                        property,
                    },
                    inventory: None,
                });
            }
            let binding = crate::canonical_inventory::InventoryEquipmentBinding::player(
                *lease,
                *chunk,
                context.state.inventory_equipment_attribute_ids(),
            );
            reads.extend(binding.targets());
            writes.extend(binding.hotbar);
            writes.extend(binding.property_targets());
        }
        reads.extend(writes.iter().copied());
        let permanent = reads
            .iter()
            .filter_map(|target| match *target {
                Target::Chunk {
                    key,
                    part:
                        ChunkPart::LivingProperty {
                            actor,
                            property: L::Attribute(id),
                        },
                    inventory: None,
                } => Some(Target::Chunk {
                    key,
                    part: ChunkPart::LivingProperty {
                        actor,
                        property: L::PermanentAttributeModifiers(id),
                    },
                    inventory: None,
                }),
                _ => None,
            })
            .collect::<Vec<_>>();
        reads.extend(permanent);
        Ok(ActionFootprint {
            counterfactual_reads: reads.clone(),
            counterfactual_writes: writes.clone(),
            reads,
            writes,
        })
    }

    pub fn derive<S: ActionState, R: crate::combat_command::MeleeRegistry>(
        &self,
        context: &CommandContext<'_, S>,
        registry: &R,
    ) -> Result<ResolutionRecord, ApplyFailure> {
        source_identity(context, &self.source.actor)?;
        if self.source.actor.actor() == self.vehicle.actor.actor() {
            return Err(ApplyFailure::InvalidAction);
        }
        let footprint = self.footprint(context, registry)?;
        if context.lookup(self.vehicle.actor.body_target())?.is_none() {
            return absent_body(context, footprint, [self.vehicle.actor.body_target()]);
        }
        if context.lookup(self.source.actor.body_target())?.is_none() {
            return absent_body(
                context,
                footprint,
                [
                    self.vehicle.actor.body_target(),
                    self.source.actor.body_target(),
                ],
            );
        }
        if !required_inputs(context, &footprint)? {
            return not_applied(context, footprint);
        }
        let intent = match self.intent(context, registry) {
            Ok(intent) => intent,
            Err(ApplyFailure::Condition(_)) => {
                return not_applied(context, footprint);
            }
            Err(reason) => return Err(reason),
        };
        let crate::action_identity::ActionIdentity::Issued(id) = context.identity else {
            return Err(ApplyFailure::InvalidAction);
        };
        let effects = intent.derive_with(context.state, id, context.tick, context.values)?;
        record(context, footprint, effects)
    }
}

#[cfg(test)]
mod attack_tests {
    use super::*;
    use crate::combat_command::{
        ActorGeometry, ActorObservation, MeleeProtocol, MeleeRegistry, VelocityCut, VelocityValues,
    };
    use crate::living_property::{LivingProperty as L, LivingPropertyValue as LV};
    use std::collections::BTreeMap;

    struct State {
        values: BTreeMap<Target, Cell>,
        lease: LivePlayerLease,
    }
    impl ActionState for State {
        fn read(&self, _: TruthView, target: Target) -> Option<Cell> {
            if let Some(cell) = self.values.get(&target) {
                return Some(cell.clone());
            }
            if let Some(body) = crate::derived_living::actor_body_target(target) {
                return self
                    .values
                    .get(&body)
                    .and_then(|body| crate::derived_living::actor_facet_cell(body, target));
            }
            if let Target::Player {
                key,
                lease: Some(lease),
                inventory: Some(InventoryId::Player { inv, slot }),
            } = target
            {
                if key != self.lease.key || lease != self.lease {
                    return None;
                }
                let mut cell = crate::inventory::InventoryCell::empty(
                    crate::inventory::InvLoc::new(inv, slot),
                );
                if inv == crate::inventory::INV_SELECTED {
                    cell.item = 1;
                    cell.count = 1;
                }
                return Some(Cell {
                    revision: 1,
                    value: Value::Inventory(cell),
                });
            }
            None
        }
        fn validate_live_player_lease(&self, lease: LivePlayerLease) -> bool {
            lease == self.lease
        }
        fn actor_collision_shape(
            &self,
            _: Target,
            _: &Value,
        ) -> Result<crate::actor_occupancy::CollisionShape, ApplyFailure> {
            Ok(crate::actor_occupancy::CollisionShape {
                offsets: [
                    [
                        (-0.49_f64).to_bits(),
                        0.0_f64.to_bits(),
                        (-0.49_f64).to_bits(),
                    ],
                    [0.49_f64.to_bits(), 0.7_f64.to_bits(), 0.49_f64.to_bits()],
                ],
                blocks_building: true,
                hanging: false,
            })
        }
        fn inventory_equipment_facts(
            &self,
            _: &[(
                crate::canonical_inventory::InventoryEquipmentSlot,
                crate::inventory::InventoryCell,
            )],
        ) -> Result<crate::combat_action::CombatEquipmentFacts, ApplyFailure> {
            Ok(crate::combat_action::CombatEquipmentFacts {
                unarmed: true,
                ..Default::default()
            })
        }
        fn status_effect_facts(
            &self,
            _: &crate::active_effects::ActiveEffectSet,
        ) -> Result<crate::combat_action::CombatStatusFacts, ApplyFailure> {
            Ok(Default::default())
        }
        fn status_effect_modifiers(
            &self,
            _: &crate::active_effects::ActiveEffectSet,
        ) -> Result<Vec<crate::combat_action::CombatAttributeModifier>, ApplyFailure> {
            Ok(Vec::new())
        }
        fn commit(
            &mut self,
            _: ActionContext,
            _: TruthView,
            _: &[AppliedWrite],
        ) -> Result<(), ApplyFailure> {
            Err(ApplyFailure::InvalidAction)
        }
    }
    struct Registry;
    impl MeleeRegistry for Registry {
        fn attributes(&self) -> crate::combat_action::CombatAttributeIds {
            crate::combat_action::CombatAttributeIds {
                attack_damage: 1,
                attack_speed: 2,
                armor: 3,
                toughness: 4,
                knockback_resistance: 5,
            }
        }
        fn range_attribute(&self) -> u16 {
            6
        }
        fn scale_attribute(&self) -> u16 {
            7
        }
        fn geometry(
            &self,
            _: &ActorReference,
            _: &crate::item_action::ActorLivingState,
        ) -> Result<ActorGeometry, ApplyFailure> {
            Ok(ActorGeometry {
                half_width: 0.3_f64.to_bits(),
                height: 1.8_f64.to_bits(),
                eye_height: 1.62_f64.to_bits(),
            })
        }
        fn species(
            &self,
            _: &ActorReference,
            _: &crate::item_action::ActorLivingState,
        ) -> Result<[bool; 2], ApplyFailure> {
            Ok([false; 2])
        }
    }
    #[test]
    fn melee_vehicle_command_infers_damage_and_clock_and_rejects_changed_range_without_generic_payload()
     {
        let (lease, mut source, contact) = crate::combat_action::tests::fixture(1);
        source.combat.equipment.unarmed = true;
        for (attribute, value) in [(1, 3.0_f64), (2, 4.0), (6, 3.0), (7, 1.0)] {
            source
                .combat
                .effective_attributes
                .insert(attribute, value.to_bits());
        }
        let source_ref = ActorReference::Player {
            lease,
            chunk: contact.chunk,
            epoch: contact.epoch,
            owner: contact.owner,
        };
        let mut cart = super::tests::body(4, contact.chunk, VehicleKind::Minecart);
        cart.owner = contact.owner;
        cart.spatial.position = [3.0_f64, 80.0, 1.0].map(f64::to_bits);
        let target_ref = super::tests::reference(&cart, contact.chunk);
        let velocity = VelocityCut {
            reliable: VelocityValues {
                velocity: [0; 3],
                revisions: [0; 3],
            },
            effective: [0; 3],
        };
        let command = VehicleAttack {
            source: ActorObservation {
                actor: source_ref.clone(),
                position: contact.position,
                yaw: 0.0_f32.to_bits(),
                on_ground: true,
                sequence: contact.observed_sequence,
                velocity,
            },
            vehicle: VehicleObservation {
                actor: target_ref.clone(),
                position: cart.spatial.position,
                yaw: cart.spatial.yaw,
                on_ground: true,
                sequence: crate::identity::PlayerSeq(0),
                velocity: EntityVelocityCut {
                    reliable: cart.spatial.velocity_micro,
                    body_revision: 1,
                    effective: [0; 3],
                },
            },
            protocol: MeleeProtocol::Java,
        };
        let mut state = State {
            values: BTreeMap::from([(
                source_ref.body_target(),
                Cell {
                    revision: 1,
                    value: Value::Living(source.into()),
                },
            )]),
            lease,
        };
        super::tests::seed(&cart, &target_ref, &mut state.values);
        let id = crate::action_identity::ActionIdentity::Issued(ActionId {
            actor: source_ref.actor(),
            incarnation: lease.incarnation,
            sequence: 8,
            family: 7,
        });
        let context = CommandContext {
            known_absent: &std::collections::BTreeSet::new(),
            state: &state,
            values: &state.values,
            identity: id,
            tick: crate::time::TickStamp(100),
        };
        let footprint = command.footprint(&context, &Registry).unwrap();
        let mut values = footprint
            .reads
            .iter()
            .map(|target| (*target, state.read(TruthView::Local, *target).unwrap()))
            .collect::<BTreeMap<_, _>>();
        let context = CommandContext {
            known_absent: &std::collections::BTreeSet::new(),
            state: &state,
            values: &values,
            identity: id,
            tick: crate::time::TickStamp(100),
        };
        let resolved = command.derive(&context, &Registry).unwrap();
        let CommandResult::Applied(writes) = resolved.result else {
            unreachable!()
        };
        assert!(writes.iter().any(|write| write.target
            == property(&target_ref, P::Damage).unwrap()
            && write.after.as_ref().unwrap().value == Value::VehicleProperty(V::Unsigned(30_000))));
        let clock = Target::Chunk {
            key: source_ref.chunk(),
            part: ChunkPart::LivingProperty {
                actor: source_ref.actor(),
                property: L::AttackStarted,
            },
            inventory: None,
        };
        assert!(writes.iter().any(|write| write.target == clock));
        let range = Target::Chunk {
            key: source_ref.chunk(),
            part: ChunkPart::LivingProperty {
                actor: source_ref.actor(),
                property: L::Attribute(6),
            },
            inventory: None,
        };
        values.insert(
            range,
            Cell {
                revision: 2,
                value: Value::LivingProperty(LV::Attribute {
                    base: Some(0.25_f64.to_bits()),
                    modifiers: Vec::new(),
                    transient_modifiers: Vec::new(),
                }),
            },
        );
        let context = CommandContext {
            known_absent: &std::collections::BTreeSet::new(),
            state: &state,
            values: &values,
            identity: id,
            tick: crate::time::TickStamp(100),
        };
        assert_eq!(
            command.derive(&context, &Registry).unwrap().result,
            CommandResult::NotApplied
        );
    }
}
