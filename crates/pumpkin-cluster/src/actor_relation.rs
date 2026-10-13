use serde::{Deserialize, Serialize};

use crate::actor::ActorId;
use crate::canonical_action::{ChunkPart, Target, Value};
use crate::item_contact::ContactIdentity;

#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash, Serialize, Deserialize)]
pub enum ActorRelation {
    Mount,
    Passenger(u8),
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct VehicleClaimedTransform {
    pub position: [u64; 3],
    pub yaw: u32,
    pub pitch: u32,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct VehicleControlInput {
    pub driver: crate::canonical_action::LivePlayerLease,
    pub mount: ContactIdentity,
    pub simulation_tick: crate::time::TickStamp,
    pub input_sequence: crate::identity::PlayerSeq,
    pub driver_effective_yaw: u32,
    pub claimed: Option<VehicleClaimedTransform>,
    pub forward: i8,
    pub sideways: i8,
    pub jumping: bool,
    pub paddle_left: bool,
    pub paddle_right: bool,
    pub paddles_changed: bool,
}

impl VehicleControlInput {
    pub fn is_valid(&self) -> bool {
        self.driver.incarnation != 0
            && !self.driver.key.0.is_nil()
            && f32::from_bits(self.driver_effective_yaw).is_finite()
            && matches!(self.mount, ContactIdentity::Entity { .. })
            && identity_valid(&self.mount)
            && (-1..=1).contains(&self.forward)
            && (-1..=1).contains(&self.sideways)
            && self.claimed.as_ref().is_none_or(|claimed| {
                claimed.position.iter().all(|bits| {
                    f64::from_bits(*bits).is_finite() && f64::from_bits(*bits).abs() <= 30_000_000.0
                }) && f32::from_bits(claimed.yaw).is_finite()
                    && f32::from_bits(claimed.pitch).is_finite()
            })
    }
}

pub fn identity_valid(identity: &ContactIdentity) -> bool {
    match identity {
        ContactIdentity::Player(lease) => lease.incarnation != 0 && !lease.key.0.is_nil(),
        ContactIdentity::Entity { id, birth } => {
            id.generation != 0
                && !birth.uuid.is_nil()
                && birth.source.generation != 0
                && birth.action.id.incarnation != 0
        }
    }
}

pub fn accepts(actor: ActorId, relation: ActorRelation, value: &Option<ContactIdentity>) -> bool {
    matches!(
        (actor, relation),
        (ActorId::Player(_) | ActorId::Entity(_), ActorRelation::Mount)
            | (ActorId::Entity(_), ActorRelation::Passenger(0..=31))
    ) && value.as_ref().is_none_or(identity_valid)
        && (relation != ActorRelation::Mount || value.as_ref().is_none_or(|identity| matches!(identity, ContactIdentity::Entity { .. })))
}

pub fn body_target(target: Target) -> Option<(Target, ActorRelation)> {
    let Target::Chunk {
        key,
        part: ChunkPart::ActorRelation { actor, relation },
        inventory: None,
    } = target
    else {
        return None;
    };
    let part = match actor {
        ActorId::Player(id) => ChunkPart::Player(id),
        ActorId::Entity(id) => ChunkPart::Entity(id),
        ActorId::Peer(_) => return None,
    };
    Some((
        Target::Chunk {
            key,
            part,
            inventory: None,
        },
        relation,
    ))
}

pub fn read(body: &Value, relation: ActorRelation) -> Option<Option<ContactIdentity>> {
    match (body, relation) {
        (Value::Living(living), ActorRelation::Mount) => {
            Some(living.avatar.as_ref()?.mount.clone())
        }
        (Value::Entity(entity), ActorRelation::Mount) => Some(entity.mount.clone()),
        (Value::Entity(entity), ActorRelation::Passenger(seat)) if seat < 32 => {
            Some(entity.passengers.get(&seat).cloned())
        }
        _ => None,
    }
}

pub fn patch(body: &mut Value, relation: ActorRelation, value: Option<ContactIdentity>) -> bool {
    match (body, relation) {
        (Value::Living(living), ActorRelation::Mount) => {
            let Some(avatar) = living.avatar.as_mut() else {
                return false;
            };
            avatar.mount = value;
            true
        }
        (Value::Entity(entity), ActorRelation::Mount) => { entity.mount = value; true },
        (Value::Entity(entity), ActorRelation::Passenger(seat)) if seat < 32 => {
            if let Some(value) = value {
                entity.passengers.insert(seat, value);
            } else {
                entity.passengers.remove(&seat);
            }
            true
        }
        _ => false,
    }
}

pub fn cell(
    body: &crate::canonical_action::Cell,
    relation: ActorRelation,
) -> Option<crate::canonical_action::Cell> {
    let value = read(&body.value, relation)?;
    let revision = match &body.value {
        Value::Living(living) => living
            .avatar
            .as_ref()?
            .relation_versions
            .get(&relation)
            .copied()
            .unwrap_or(0),
        Value::Entity(entity) => entity
            .relation_versions
            .get(&relation)
            .copied()
            .unwrap_or(0),
        _ => return None,
    };
    Some(crate::canonical_action::Cell {
        revision,
        value: Value::ActorRelation(value),
    })
}

pub fn patch_cell(
    body: &mut crate::canonical_action::Cell,
    relation: ActorRelation,
    next: &crate::canonical_action::Cell,
) -> bool {
    let Value::ActorRelation(value) = &next.value else {
        return false;
    };
    if !patch(&mut body.value, relation, value.clone()) {
        return false;
    }
    let versions = match &mut body.value {
        Value::Living(living) => &mut living.avatar.as_mut().unwrap().relation_versions,
        Value::Entity(entity) => &mut entity.relation_versions,
        _ => return false,
    };
    if next.revision == 0 {
        versions.remove(&relation);
    } else {
        versions.insert(relation, next.revision);
    }
    true
}

pub fn all(body: &Value) -> Vec<ActorRelation> {
    match body {
        Value::Living(living) if living.avatar.is_some() => vec![ActorRelation::Mount],
        Value::Entity(_) => (0..32).map(ActorRelation::Passenger).collect(),
        _ => Vec::new(),
    }
}

pub fn normalize_body(
    before: &crate::canonical_action::Cell,
    after: &mut crate::canonical_action::Cell,
) -> bool {
    for relation in all(&after.value) {
        let Some(next) = cell(after, relation) else {
            return false;
        };
        let previous = cell(before, relation);
        let revision = match previous {
            Some(previous) if previous.value == next.value => previous.revision,
            Some(previous) => match previous.revision.checked_add(1) {
                Some(value) => value,
                None => return false,
            },
            None => next.revision,
        };
        if !patch_cell(
            after,
            relation,
            &crate::canonical_action::Cell {
                revision,
                value: next.value,
            },
        ) {
            return false;
        }
    }
    true
}

pub fn reciprocal(action: &crate::canonical_action::Action) -> bool {
    use crate::canonical_action::{ActionClass, Cell, Effect, Expected};
    if matches!(action.id.actor, ActorId::Entity(_)) { return owner_detach_all(action); }
    let relation_effects = action.effects.iter().filter(|effect| matches!(effect,
        Effect::Put { target: Target::Chunk { part: ChunkPart::ActorRelation { .. }, .. }, .. })).collect::<Vec<_>>();
    let respawn = matches!(&action.semantic_intent,
        Some(crate::canonical_action::SemanticIntent::Respawn(intent))
            if action.class == ActionClass::Replayable && intent.guards_valid(action));
    if (action.class != ActionClass::Undoable && !respawn) || relation_effects.len() != 2
    {
        return false;
    }
    let mut mount = None;
    let mut passenger = None;
    for effect in relation_effects {
        match effect {
            Effect::Put {
                target:
                    Target::Chunk {
                        key,
                        part:
                            ChunkPart::ActorRelation {
                                actor: ActorId::Player(player),
                                relation: ActorRelation::Mount,
                            },
                        inventory: None,
                    },
                value: Value::ActorRelation(value),
            } => mount = Some((*key, *player, value)),
            Effect::Put {
                target:
                    Target::Chunk {
                        key,
                        part:
                            ChunkPart::ActorRelation {
                                actor: ActorId::Entity(entity),
                                relation: ActorRelation::Passenger(seat),
                            },
                        inventory: None,
                    },
                value: Value::ActorRelation(value),
            } if *seat < 32 => passenger = Some((*key, *entity, *seat, value)),
            _ => return false,
        }
    }
    let (
        Some((player_chunk, player, mount_value)),
        Some((entity_chunk, entity, seat, passenger_value)),
    ) = (mount, passenger)
    else {
        return false;
    };
    if player_chunk.world != entity_chunk.world || action.id.actor != ActorId::Player(player) {
        return false;
    }
    let Some(lease) = action
        .player_leases
        .iter()
        .find(|lease| lease.gid == player && lease.incarnation == action.id.incarnation)
    else {
        return false;
    };
    let entity_body = Target::Chunk {
        key: entity_chunk,
        part: ChunkPart::Entity(entity),
        inventory: None,
    };
    let player_body = Target::Chunk {
        key: player_chunk,
        part: ChunkPart::Player(player),
        inventory: None,
    };
    let Some(birth) = action
        .conditions
        .iter()
        .find_map(|condition| match &condition.expected {
            Expected::EntityBirthOwned { birth, .. } | Expected::EntityBirthKind { birth, .. } if condition.target == entity_body => {
                Some(birth)
            }
            _ => None,
        })
    else {
        return false;
    };
    let expected_mount = Some(ContactIdentity::Entity {
        id: entity,
        birth: birth.clone(),
    });
    let expected_passenger = Some(ContactIdentity::Player(*lease));
    let mount_target = Target::Chunk {
        key: player_chunk,
        part: ChunkPart::ActorRelation {
            actor: ActorId::Player(player),
            relation: ActorRelation::Mount,
        },
        inventory: None,
    };
    let passenger_target = Target::Chunk {
        key: entity_chunk,
        part: ChunkPart::ActorRelation {
            actor: ActorId::Entity(entity),
            relation: ActorRelation::Passenger(seat),
        },
        inventory: None,
    };
    let before = |target| {
        action
            .conditions
            .iter()
            .find_map(|condition| match &condition.expected {
                Expected::Exact(Cell {
                    value: Value::ActorRelation(value),
                    ..
                }) if condition.target == target => Some(value),
                _ => None,
            })
    };
    let (Some(prior_mount), Some(prior_passenger)) =
        (before(mount_target), before(passenger_target))
    else {
        return false;
    };
    let attaching = prior_mount.is_none()
        && prior_passenger.is_none()
        && *mount_value == expected_mount
        && *passenger_value == expected_passenger;
    let detaching = *prior_mount == expected_mount
        && *prior_passenger == expected_passenger
        && mount_value.is_none()
        && passenger_value.is_none();
    (detaching || attaching && !respawn)
        && [player_body, entity_body].iter().all(|target| {
            action.conditions.iter().any(|condition| {
                condition.target == *target
                    && matches!(condition.expected, Expected::SpatialEpochEquals(_))
            })
        })
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct ActorRelationParticipant {
    pub identity: ContactIdentity,
    pub chunk: crate::protocol::ChunkKey,
    pub owner: crate::realm_completion::PeerGeneration,
    pub epoch: crate::canonical_action::SpatialEpoch,
}

impl ActorRelationParticipant {
    pub fn from_contact(contact: &crate::item_contact::ContactObservation) -> Option<Self> {
        contact.valid().then(|| Self { identity: contact.identity.clone(), chunk: contact.chunk,
            owner: contact.owner, epoch: contact.epoch })
    }
    pub fn from_entity(body: &crate::entity_body::EntityBody, chunk: crate::protocol::ChunkKey) -> Option<Self> {
        body.belongs_to(body.id, chunk).then(|| Self {
            identity: ContactIdentity::Entity { id: body.id, birth: body.birth.clone() },
            chunk, owner: body.owner, epoch: body.spatial_epoch })
    }
    pub fn actor(&self) -> ActorId {
        match self.identity { ContactIdentity::Player(lease) => ActorId::Player(lease.gid),
            ContactIdentity::Entity { id, .. } => ActorId::Entity(id) }
    }
    pub fn target(&self) -> Target {
        Target::Chunk { key: self.chunk, part: match self.identity {
            ContactIdentity::Player(lease) => ChunkPart::Player(lease.gid),
            ContactIdentity::Entity { id, .. } => ChunkPart::Entity(id),
        }, inventory: None }
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct OwnerDetachPlan {
    pub conditions: Vec<crate::canonical_action::Condition>,
    pub effects: Vec<crate::canonical_action::Effect>,
    pub leases: Vec<crate::canonical_action::LivePlayerLease>,
    pub chunks: Vec<crate::protocol::ChunkKey>,
}

pub fn capture_owner_detach<S: crate::canonical_action::ActionState>(
    state: &S,
    body: &crate::entity_body::EntityBody,
    chunk: crate::protocol::ChunkKey,
    riders: &[crate::item_contact::ContactObservation],
) -> Option<OwnerDetachPlan> {
    let participants = riders.iter().map(ActorRelationParticipant::from_contact)
        .collect::<Option<Vec<_>>>()?;
    capture_owner_detach_from_participants(state, body, chunk, &participants)
}

pub fn capture_owner_detach_from_participants<S: crate::canonical_action::ActionState>(
    state: &S,
    body: &crate::entity_body::EntityBody,
    chunk: crate::protocol::ChunkKey,
    participants: &[ActorRelationParticipant],
) -> Option<OwnerDetachPlan> {
    use crate::canonical_action::*;
    let actor = ActorId::Entity(body.id);
    let identity = ContactIdentity::Entity { id: body.id, birth: body.birth.clone() };
    let body_target = Target::Chunk { key: chunk, part: ChunkPart::Entity(body.id), inventory: None };
    let mut plan = OwnerDetachPlan {
        conditions: vec![Condition { target: body_target, expected: Expected::EntityBirthOwned { birth: body.birth.clone(), owner: body.owner } },
            Condition { target: body_target, expected: Expected::SpatialEpochEquals(body.spatial_epoch) }],
        effects: Vec::new(), leases: Vec::new(), chunks: vec![chunk],
    };
    let own_mount = Target::Chunk { key: chunk, part: ChunkPart::ActorRelation { actor, relation: ActorRelation::Mount }, inventory: None };
    let before = state.read(TruthView::Local, own_mount)?;
    if before.value != Value::ActorRelation(body.mount.clone()) { return None; }
    plan.conditions.push(Condition { target: own_mount, expected: Expected::Exact(before) });
    if let Some(parent_identity) = &body.mount {
        let parent = participants.iter().find(|participant| participant.identity == *parent_identity)?;
        let ContactIdentity::Entity { id, birth } = &parent.identity else { return None; };
        if parent.chunk.world != chunk.world || parent.owner.generation == 0 { return None; }
        let Value::Entity(parent_body) = state.read(TruthView::Local, parent.target())?.value else { return None; };
        if parent_body.id != *id || parent_body.birth != *birth || parent_body.owner != parent.owner
            || parent_body.spatial_epoch != parent.epoch { return None; }
        let mut seats = parent_body.passengers.iter().filter_map(|(seat, passenger)| (*passenger == identity).then_some(*seat));
        let seat = seats.next()?;
        if seats.next().is_some() { return None; }
        let passenger = Target::Chunk { key: parent.chunk, part: ChunkPart::ActorRelation { actor: parent.actor(), relation: ActorRelation::Passenger(seat) }, inventory: None };
        let before = state.read(TruthView::Local, passenger)?;
        if before.value != Value::ActorRelation(Some(identity.clone())) { return None; }
        plan.conditions.extend([Condition { target: passenger, expected: Expected::Exact(before) },
            Condition { target: parent.target(), expected: Expected::EntityBirthOwned { birth: birth.clone(), owner: parent.owner } },
            Condition { target: parent.target(), expected: Expected::SpatialEpochEquals(parent.epoch) }]);
        plan.effects.extend([Effect::Put { target: own_mount, value: Value::ActorRelation(None) },
            Effect::Put { target: passenger, value: Value::ActorRelation(None) }]);
        plan.chunks.push(parent.chunk);
    }
    for seat in 0..32 {
        let passenger = Target::Chunk { key: chunk, part: ChunkPart::ActorRelation { actor, relation: ActorRelation::Passenger(seat) }, inventory: None };
        let before = state.read(TruthView::Local, passenger)?;
        let occupant = body.passengers.get(&seat).cloned();
        if before.value != Value::ActorRelation(occupant.clone()) { return None; }
        plan.conditions.push(Condition { target: passenger, expected: Expected::Exact(before) });
        let Some(occupant) = occupant else { continue; };
        let rider = participants.iter().find(|participant| participant.identity == occupant)?;
        if rider.chunk.world != chunk.world || rider.owner.generation == 0 || !identity_valid(&occupant) { return None; }
        let mount = Target::Chunk { key: rider.chunk, part: ChunkPart::ActorRelation { actor: rider.actor(), relation: ActorRelation::Mount }, inventory: None };
        let before = state.read(TruthView::Local, mount)?;
        if before.value != Value::ActorRelation(Some(identity.clone())) { return None; }
        plan.conditions.push(Condition { target: mount, expected: Expected::Exact(before) });
        plan.conditions.push(Condition { target: rider.target(), expected: Expected::SpatialEpochEquals(rider.epoch) });
        match &occupant {
            ContactIdentity::Player(lease) => plan.leases.push(*lease),
            ContactIdentity::Entity { birth, .. } => plan.conditions.push(Condition { target: rider.target(),
                expected: Expected::EntityBirthOwned { birth: birth.clone(), owner: rider.owner } }),
        }
        plan.effects.extend([Effect::Put { target: mount, value: Value::ActorRelation(None) }, Effect::Put { target: passenger, value: Value::ActorRelation(None) }]);
        plan.chunks.push(rider.chunk);
    }
    plan.leases.sort_unstable(); plan.leases.dedup();
    plan.chunks.sort_unstable(); plan.chunks.dedup();
    Some(plan)
}

pub fn owner_detach_all(action: &crate::canonical_action::Action) -> bool {
    use crate::canonical_action::{ActionClass, Cell, Effect, Expected};
    let ActorId::Entity(entity) = action.id.actor else { return false; };
    if action.class != ActionClass::Undoable { return false; }
    let Some((body_target, birth)) = action.conditions.iter().find_map(|condition| match (&condition.target, &condition.expected) {
        (Target::Chunk { part: ChunkPart::Entity(id), .. }, Expected::EntityBirthOwned { birth, .. } | Expected::EntityBirthKind { birth, .. }) if *id == entity => Some((condition.target, birth)),
        _ => None,
    }) else { return false; };
    let Target::Chunk { key, .. } = body_target else { return false; };
    let exact = |target| action.conditions.iter().find_map(|condition| match &condition.expected {
        Expected::Exact(cell) if condition.target == target => Some(cell), _ => None,
    });
    let epoch = |target| action.conditions.iter().any(|condition| condition.target == target && matches!(condition.expected, Expected::SpatialEpochEquals(_)));
    let owned = |target, identity: &ContactIdentity| match identity {
        ContactIdentity::Player(lease) => action.player_leases.contains(lease),
        ContactIdentity::Entity { birth, .. } => action.conditions.iter().any(|condition|
            condition.target == target && matches!(&condition.expected,
                Expected::EntityBirthOwned { birth: expected, owner } | Expected::EntityBirthKind { birth: expected, owner, .. }
                if expected == birth && owner.generation != 0)),
    };
    if !epoch(body_target) { return false; }
    let mut required = std::collections::BTreeSet::new();
    let own_identity = ContactIdentity::Entity { id: entity, birth: birth.clone() };
    let own_mount = Target::Chunk { key, part: ChunkPart::ActorRelation { actor: action.id.actor, relation: ActorRelation::Mount }, inventory: None };
    let Some(Cell { value: Value::ActorRelation(parent), .. }) = exact(own_mount) else { return false; };
    if let Some(parent) = parent {
        let ContactIdentity::Entity { id: parent_id, .. } = parent else { return false; };
        let Some((passenger, parent_body)) = action.conditions.iter().find_map(|condition| {
            let Target::Chunk { key: parent_chunk, part: ChunkPart::ActorRelation { actor: ActorId::Entity(id), relation: ActorRelation::Passenger(0..=31) }, inventory: None } = condition.target else { return None; };
            if id != *parent_id || parent_chunk.world != key.world { return None; }
            let Expected::Exact(Cell { value: Value::ActorRelation(Some(value)), .. }) = &condition.expected else { return None; };
            (*value == own_identity).then_some((condition.target, Target::Chunk { key: parent_chunk, part: ChunkPart::Entity(id), inventory: None }))
        }) else { return false; };
        if !epoch(parent_body) || !owned(parent_body, parent) || !action.stakeholders.contains(&passenger.stakeholder()) { return false; }
        required.extend([own_mount, passenger]);
    }
    for seat in 0..32 {
        let target = Target::Chunk { key, part: ChunkPart::ActorRelation { actor: action.id.actor, relation: ActorRelation::Passenger(seat) }, inventory: None };
        let Some(Cell { value: Value::ActorRelation(passenger), .. }) = exact(target) else { return false; };
        let Some(passenger) = passenger else { continue; };
        let passenger_actor = match passenger { ContactIdentity::Player(lease) => ActorId::Player(lease.gid), ContactIdentity::Entity { id, .. } => ActorId::Entity(*id) };
        let Some((mount_target, mount_cell)) = action.conditions.iter().find_map(|condition| match (&condition.target, &condition.expected) {
            (Target::Chunk { key: rider_chunk, part: ChunkPart::ActorRelation { actor, relation: ActorRelation::Mount }, inventory: None }, Expected::Exact(cell))
                if *actor == passenger_actor && rider_chunk.world == key.world => Some((condition.target, cell)), _ => None,
        }) else { return false; };
        if mount_cell.value != Value::ActorRelation(Some(own_identity.clone())) { return false; }
        let Target::Chunk { key: rider_chunk, .. } = mount_target else { return false; };
        let rider_body = Target::Chunk { key: rider_chunk, part: match passenger_actor { ActorId::Player(gid) => ChunkPart::Player(gid), ActorId::Entity(id) => ChunkPart::Entity(id), _ => return false }, inventory: None };
        if !epoch(rider_body) || !owned(rider_body, passenger) || !action.stakeholders.contains(&rider_body.stakeholder()) { return false; }
        if !required.insert(target) || !required.insert(mount_target) { return false; }
    }
    if required.is_empty() { return false; }
    let mut actual = std::collections::BTreeSet::new();
    for effect in &action.effects {
        if let Effect::Put { target: Target::Chunk { part: ChunkPart::ActorRelation { .. }, .. }, .. } = effect {
            let Effect::Put { target, value: Value::ActorRelation(None) } = effect else { return false; };
            if !required.contains(target) || !actual.insert(*target) { return false; }
        }
    }
    actual == required && action.stakeholders.contains(&crate::canonical_action::Stakeholder::Chunk(key))
}

pub fn action(
    id: crate::canonical_action::ActionId,
    tick: crate::time::TickStamp,
    player: &crate::item_contact::ContactObservation,
    vehicle: &crate::item_contact::ContactObservation,
    seat: u8,
    attach: bool,
    before_mount: crate::canonical_action::Cell,
    before_passenger: crate::canonical_action::Cell,
) -> Option<crate::canonical_action::Action> {
    use crate::canonical_action::*;
    let ContactIdentity::Player(lease) = &player.identity else {
        return None;
    };
    let ContactIdentity::Entity { id: entity, birth } = &vehicle.identity else {
        return None;
    };
    let lease = *lease;
    let entity = *entity;
    if !player.valid()
        || !vehicle.valid()
        || seat >= 32
        || player.world != vehicle.world
        || player.simulation_tick.is_after(tick)
        || vehicle.simulation_tick.is_after(tick)
        || id.actor != ActorId::Player(lease.gid)
        || id.incarnation != lease.incarnation
    {
        return None;
    }
    let distance: f64 = player
        .position
        .iter()
        .zip(&vehicle.position)
        .map(|(a, b)| (f64::from_bits(*a) - f64::from_bits(*b)).powi(2))
        .sum();
    if attach && distance > 36.0 {
        return None;
    }
    let mount = Target::Chunk {
        key: player.chunk,
        part: ChunkPart::ActorRelation {
            actor: id.actor,
            relation: ActorRelation::Mount,
        },
        inventory: None,
    };
    let passenger = Target::Chunk {
        key: vehicle.chunk,
        part: ChunkPart::ActorRelation {
            actor: ActorId::Entity(entity),
            relation: ActorRelation::Passenger(seat),
        },
        inventory: None,
    };
    let player_body = Target::Chunk {
        key: player.chunk,
        part: ChunkPart::Player(lease.gid),
        inventory: None,
    };
    let entity_body = Target::Chunk {
        key: vehicle.chunk,
        part: ChunkPart::Entity(entity),
        inventory: None,
    };
    let conditions = vec![
        Condition {
            target: mount,
            expected: Expected::Exact(before_mount),
        },
        Condition {
            target: passenger,
            expected: Expected::Exact(before_passenger),
        },
        Condition {
            target: player_body,
            expected: Expected::SpatialEpochEquals(player.epoch),
        },
        Condition {
            target: entity_body,
            expected: Expected::SpatialEpochEquals(vehicle.epoch),
        },
        Condition {
            target: entity_body,
            expected: Expected::EntityBirthOwned {
                birth: birth.clone(),
                owner: vehicle.owner,
            },
        },
    ];
    let mut dependencies = [player.epoch, vehicle.epoch]
        .into_iter()
        .filter_map(|epoch| match epoch {
            SpatialEpoch::Action(reference) => Some(reference),
            _ => None,
        })
        .collect::<Vec<_>>();
    dependencies.sort_unstable();
    dependencies.dedup();
    let stakeholders = [
        Stakeholder::Chunk(player.chunk),
        Stakeholder::Chunk(vehicle.chunk),
    ]
    .into_iter()
    .collect::<std::collections::BTreeSet<_>>()
    .into_iter()
    .collect();
    let action = Action {
        edit_authority: None,
        id,
        tick,
        class: ActionClass::Undoable,
        chunk_transition: false,
        actor_chunk: Some(player.chunk),
        player_leases: vec![lease],
        player_chunks: Vec::new(),
        stakeholders,
        dependencies,
        conditions,
        effects: vec![
            Effect::Put {
                target: mount,
                value: Value::ActorRelation(attach.then(|| vehicle.identity.clone())),
            },
            Effect::Put {
                target: passenger,
                value: Value::ActorRelation(attach.then(|| player.identity.clone())),
            },
        ],
        counterfactual_conditions: Vec::new(),
        counterfactual_effects: Vec::new(),
        semantic_intent: None,
        factual_result: None,
        undo: vec![UndoProcedure::RestoreWrittenTargets],
        kind: ActionKind::PlayerAdministration,
        presentation: Vec::new(),
        client_origin: ClientOrigin::None,
    };
    action.validate().ok()?;
    Some(action)
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct PlayerDetachPlan {
    pub conditions: Vec<crate::canonical_action::Condition>,
    pub effects: Vec<crate::canonical_action::Effect>,
    pub chunks: Vec<crate::protocol::ChunkKey>,
}

pub fn capture_player_detach<S: crate::canonical_action::ActionState>(
    state: &S,
    lease: crate::canonical_action::LivePlayerLease,
    player_chunk: crate::protocol::ChunkKey,
    player_epoch: crate::canonical_action::SpatialEpoch,
    mount: &ContactIdentity,
    vehicle_chunk: crate::protocol::ChunkKey,
    body: &crate::entity_body::EntityBody,
) -> Result<PlayerDetachPlan, crate::canonical_action::ApplyFailure> {
    use crate::canonical_action::*;
    if !state.validate_live_player_lease(lease)
        || player_chunk.world != vehicle_chunk.world
        || !body.belongs_to(body.id, vehicle_chunk)
        || *mount != (ContactIdentity::Entity { id: body.id, birth: body.birth.clone() })
        || !matches!(&body.payload, crate::entity_body::EntityPayload::Vehicle(_))
            && !matches!(&body.payload, crate::entity_body::EntityPayload::Living(living)
                if living.mob.as_ref().is_some_and(|mob| matches!(mob.species, crate::mob_continuation::MobSpecies::Horse { .. })))
    {
        return Err(ApplyFailure::InvalidAction);
    }
    let seats = body.passengers.iter().filter_map(|(seat, passenger)|
        (*passenger == ContactIdentity::Player(lease)).then_some(*seat)).collect::<Vec<_>>();
    let [seat] = seats.as_slice() else { return Err(ApplyFailure::InvalidAction); };
    let mount_target = Target::Chunk {
        key: player_chunk,
        part: ChunkPart::ActorRelation { actor: ActorId::Player(lease.gid), relation: ActorRelation::Mount },
        inventory: None,
    };
    let passenger_target = Target::Chunk {
        key: vehicle_chunk,
        part: ChunkPart::ActorRelation { actor: ActorId::Entity(body.id), relation: ActorRelation::Passenger(*seat) },
        inventory: None,
    };
    let before_mount = state.read(TruthView::Local, mount_target).ok_or(ApplyFailure::Condition(mount_target))?;
    let before_passenger = state.read(TruthView::Local, passenger_target).ok_or(ApplyFailure::Condition(passenger_target))?;
    if before_mount.value != Value::ActorRelation(Some(mount.clone()))
        || before_passenger.value != Value::ActorRelation(Some(ContactIdentity::Player(lease)))
    {
        return Err(ApplyFailure::InvalidAction);
    }
    let player_target = Target::Chunk { key: player_chunk, part: ChunkPart::Player(lease.gid), inventory: None };
    let vehicle_target = Target::Chunk { key: vehicle_chunk, part: ChunkPart::Entity(body.id), inventory: None };
    let conditions = vec![
        Condition { target: mount_target, expected: Expected::Exact(before_mount) },
        Condition { target: passenger_target, expected: Expected::Exact(before_passenger) },
        Condition { target: player_target, expected: Expected::SpatialEpochEquals(player_epoch) },
        Condition { target: vehicle_target, expected: Expected::SpatialEpochEquals(body.spatial_epoch) },
        Condition { target: vehicle_target, expected: Expected::EntityBirthOwned { birth: body.birth.clone(), owner: body.owner } },
    ];
    let effects = vec![
        Effect::Put { target: mount_target, value: Value::ActorRelation(None) },
        Effect::Put { target: passenger_target, value: Value::ActorRelation(None) },
    ];
    let mut chunks = vec![player_chunk, vehicle_chunk];
    chunks.sort_unstable();
    chunks.dedup();
    Ok(PlayerDetachPlan { conditions, effects, chunks })
}
