use serde::{Deserialize, Serialize};

use crate::action_command::ActorReference;
use crate::actor::ActorId;
use crate::canonical_action::{ApplyFailure, ChunkPart, Target};
use crate::identity::PlayerSeq;
use crate::item_contact::{ContactIdentity, ContactObservation};
use crate::time::TickStamp;

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub struct VelocityValues {
    pub velocity: [i64; 3],
    pub revisions: [u64; 3],
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub struct VelocityCut {
    pub reliable: VelocityValues,
    pub effective: [i64; 3],
}

impl VelocityCut {
    pub fn resolve(self, winning: VelocityValues) -> [i64; 3] {
        if self.reliable == winning {
            self.effective
        } else {
            winning.velocity
        }
    }

    pub fn response_delta(
        self,
        winning: VelocityValues,
        response: [i64; 3],
        target: Target,
    ) -> Result<[i64; 3], ApplyFailure> {
        if response == self.resolve(winning) {
            return Ok([0; 3]);
        }
        let mut delta = [0; 3];
        for axis in 0..3 {
            delta[axis] = response[axis]
                .checked_sub(winning.velocity[axis])
                .ok_or(ApplyFailure::Overflow(target))?;
        }
        Ok(delta)
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct ActorObservation {
    pub actor: ActorReference,
    pub position: [u64; 3],
    pub yaw: u32,
    pub on_ground: bool,
    pub sequence: PlayerSeq,
    pub velocity: VelocityCut,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct ActorGeometry {
    pub half_width: u64,
    pub height: u64,
    pub eye_height: u64,
}

impl ActorGeometry {
    fn collision_shape(self) -> crate::actor_occupancy::CollisionShape {
        let half_width = f64::from_bits(self.half_width);
        crate::actor_occupancy::CollisionShape {
            offsets: [[-half_width, 0.0, -half_width].map(f64::to_bits),
                [half_width, f64::from_bits(self.height), half_width].map(f64::to_bits)],
            blocks_building: true, hanging: false,
        }
    }
}

impl ActorObservation {
    pub fn id(&self) -> ActorId {
        match &self.actor {
            ActorReference::Player { lease, .. } => ActorId::Player(lease.gid),
            ActorReference::Entity { id, .. } => ActorId::Entity(*id),
        }
    }

    pub fn target(&self) -> Target {
        Target::Chunk {
            key: self.actor.chunk(),
            part: match self.id() {
                ActorId::Player(gid) => ChunkPart::Player(gid),
                ActorId::Entity(id) => ChunkPart::Entity(id),
                ActorId::Peer(_) => unreachable!(),
            },
            inventory: None,
        }
    }

    pub fn contact(
        &self,
        tick: TickStamp,
        geometry: ActorGeometry,
    ) -> Result<ContactObservation, ApplyFailure> {
        let (identity, epoch) = match &self.actor {
            ActorReference::Player { lease, epoch, .. } => {
                (ContactIdentity::Player(*lease), *epoch)
            }
            ActorReference::Entity { id, birth, epoch, .. } => (
                ContactIdentity::Entity {
                    id: *id,
                    birth: birth.clone(),
                },
                *epoch,
            ),
        };
        let contact = ContactObservation {
            identity,
            owner: self.actor.owner(),
            epoch,
            world: self.actor.chunk().world,
            chunk: self.actor.chunk(),
            position: self.position,
            half_width: geometry.half_width,
            height: geometry.height,
            simulation_tick: tick,
            observed_sequence: self.sequence,
        };
        if !f32::from_bits(self.yaw).is_finite() || !contact.valid() {
            return Err(ApplyFailure::InvalidAction);
        }
        Ok(contact)
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub enum MeleeProtocol {
    Java,
    Bedrock,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct MeleeAttackCommand {
    pub source: ActorObservation,
    pub primary: ActorObservation,
    pub sweep: Vec<ActorObservation>,
    pub protocol: MeleeProtocol,
}

impl MeleeAttackCommand {
    pub fn actors(&self) -> impl Iterator<Item = &ActorObservation> {
        std::iter::once(&self.source)
            .chain(std::iter::once(&self.primary))
            .chain(&self.sweep)
    }

    pub fn chunks(&self) -> std::collections::BTreeSet<crate::protocol::ChunkKey> {
        self.actors().map(|observation| observation.actor.chunk()).collect()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::protocol::{ChunkKey, WorldId};

    fn target() -> Target {
        Target::Chunk {
            key: ChunkKey::new(WorldId::new(0, 0), 0, 0),
            part: ChunkPart::Whole,
            inventory: None,
        }
    }

    #[test]
    fn accepted_motion_is_used_until_a_winning_impulse_replaces_its_cut() {
        let original = VelocityValues {
            velocity: [0, 0, 0],
            revisions: [0, 0, 0],
        };
        let sample = VelocityCut {
            reliable: original,
            effective: [1_000_000, 200_000, 0],
        };
        assert_eq!(sample.resolve(original), [1_000_000, 200_000, 0]);
        assert_eq!(
            sample.response_delta(original, [600_000, 200_000, 0], target()).unwrap(),
            [600_000, 200_000, 0]
        );
        let winning = VelocityValues {
            velocity: [250_000, 0, 0],
            revisions: [1, 0, 0],
        };
        assert_eq!(sample.resolve(winning), [250_000, 0, 0]);
        assert_eq!(
            sample.response_delta(winning, [150_000, 0, 0], target()).unwrap(),
            [-100_000, 0, 0]
        );
    }

    #[test]
    fn damage_without_a_motion_response_preserves_the_reliable_cut() {
        let original = VelocityValues {
            velocity: [0; 3],
            revisions: [7; 3],
        };
        let sample = VelocityCut {
            reliable: original,
            effective: [1_000_000, 200_000, -300_000],
        };
        assert_eq!(sample.response_delta(original, sample.effective, target()), Ok([0; 3]));
        let overwritten = VelocityValues {
            revisions: [8, 7, 7],
            ..original
        };
        assert_eq!(sample.resolve(overwritten), [0; 3]);
        assert_eq!(sample.response_delta(overwritten, [0; 3], target()), Ok([0; 3]));
    }
}

pub(crate) fn velocity_values(
    values: &std::collections::BTreeMap<Target, crate::canonical_action::Cell>,
    body: Target,
) -> Result<VelocityValues, ApplyFailure> {
    use crate::canonical_action::Value;
    use crate::living_property::{LivingProperty as P, LivingPropertyValue as V};
    let Target::Chunk { key, part, inventory: None } = body else {
        return Err(ApplyFailure::InvalidAction);
    };
    let actor = match part {
        ChunkPart::Player(gid) => ActorId::Player(gid),
        ChunkPart::Entity(id) => ActorId::Entity(id),
        _ => return Err(ApplyFailure::InvalidAction),
    };
    let mut result = VelocityValues { velocity: [0; 3], revisions: [0; 3] };
    for (axis, property) in [P::VelocityX, P::VelocityY, P::VelocityZ].into_iter().enumerate() {
        let target = Target::Chunk {
            key,
            part: ChunkPart::LivingProperty { actor, property },
            inventory: None,
        };
        let cell = values.get(&target).cloned().or_else(|| {
            values.get(&body).and_then(|body| crate::derived_living::facet_cell(body, property))
        }).ok_or(ApplyFailure::Condition(target))?;
        let Value::LivingProperty(V::Signed(velocity)) = cell.value else {
            return Err(ApplyFailure::Type(target));
        };
        result.velocity[axis] = velocity;
        result.revisions[axis] = cell.revision;
    }
    Ok(result)
}

pub trait MeleeRegistry {
    fn attributes(&self) -> crate::combat_action::CombatAttributeIds;
    fn range_attribute(&self) -> u16;
    fn scale_attribute(&self) -> u16;
    fn geometry_properties(&self, _: &ActorReference, living: &crate::item_action::ActorLivingState)
        -> Vec<crate::living_property::LivingProperty> {
        use crate::living_property::LivingProperty as P;
        let mut properties = vec![P::Attribute(self.scale_attribute())];
        if living.avatar.is_some() { properties.push(P::PlayerStance); }
        if living.mob.is_some() { properties.extend([P::MobSpecies, P::MobReproduction]); }
        if living.stand.is_some() { properties.push(P::ArmorStand(crate::armor_stand::StandProperty::Flags)); }
        properties
    }
    fn mob_mount_targets(&self, source: &ActorReference, _: &std::collections::BTreeMap<Target, crate::canonical_action::Cell>)
        -> Result<std::collections::BTreeSet<Target>, ApplyFailure> {
        Ok(std::collections::BTreeSet::from([Target::Chunk { key: source.chunk(),
            part: ChunkPart::ActorRelation { actor: source.actor(), relation: crate::actor_relation::ActorRelation::Mount }, inventory: None }]))
    }
    fn mob_mount_shape(&self, source: &ActorReference, values: &std::collections::BTreeMap<Target, crate::canonical_action::Cell>)
        -> Result<Option<([u64; 3], crate::actor_occupancy::CollisionShape)>, ApplyFailure> {
        use crate::actor_relation::{self, ActorRelation};
        let target = Target::Chunk { key: source.chunk(), part: ChunkPart::ActorRelation { actor: source.actor(), relation: ActorRelation::Mount }, inventory: None };
        let cell = values.get(&target).cloned().or_else(|| values.get(&source.body_target()).and_then(|body| actor_relation::cell(body, ActorRelation::Mount)))
            .ok_or(ApplyFailure::Condition(target))?;
        match cell.value {
            crate::canonical_action::Value::ActorRelation(None) => Ok(None),
            crate::canonical_action::Value::ActorRelation(Some(_)) => Err(ApplyFailure::MissingStakeholder(target.stakeholder())),
            _ => Err(ApplyFailure::Type(target)),
        }
    }
    fn geometry(&self, actor: &ActorReference, living: &crate::item_action::ActorLivingState)
        -> Result<ActorGeometry, ApplyFailure>;
    fn species(&self, actor: &ActorReference, living: &crate::item_action::ActorLivingState)
        -> Result<[bool; 2], ApplyFailure>;
}

impl MeleeAttackCommand {
    pub fn resolve_attacks<S: crate::canonical_action::ActionState, R: MeleeRegistry>(
        &self, context: &crate::action_command::CommandContext<'_, S>, registry: &R,
    ) -> Result<Vec<crate::combat_action::AttackIntent>, ApplyFailure> {
        use crate::action_identity::ActionIdentity;
        use crate::combat_action::{AttackContact, AttackIntent, AttackPose};
        use crate::living_property::LivingProperty as P;
        let ActionIdentity::Issued(id) = context.identity else { return Err(ApplyFailure::InvalidAction); };
        if matches!(&self.source.actor, ActorReference::Entity { id: entity, .. } if id.incarnation != u64::from(entity.generation))
            || matches!(&self.source.actor, ActorReference::Player { lease, .. } if id.incarnation != lease.incarnation)
            || id.actor != self.source.id() || self.primary.id() == self.source.id()
            || self.sweep.windows(2).any(|pair| pair[0].id() >= pair[1].id())
            || self.sweep.iter().any(|victim| victim.id() == self.source.id() || victim.id() == self.primary.id()) {
            return Err(ApplyFailure::InvalidAction);
        }
        let mut source = winning_living(context, &self.source.actor)?;
        let mut source_properties = registry.geometry_properties(&self.source.actor, &source);
        if source.avatar.is_some() { source_properties.extend([P::PlayerStance, P::FallDistance, P::Attribute(registry.range_attribute())]); }
        else if source.mob.is_some() { source_properties.push(P::MobContinuation); }
        for property in source_properties { hydrate_property(context, &self.source.actor, &mut source, property)?; }
        if source.combat.health_milli == 0 { return Err(ApplyFailure::Condition(self.source.target())); }
        let range_id = registry.range_attribute();
        let reach = if source.avatar.is_some() {
            let base = source.combat.effective_attributes.get(&range_id).copied()
                .map(f64::from_bits).ok_or(ApplyFailure::Condition(self.source.target()))?;
            Some(source.attribute(range_id, base))
        } else { None };
        let geometry = registry.geometry(&self.source.actor, &source)?;
        let attacker = self.source.contact(context.tick, geometry)?;
        let pose = AttackPose {
            sprinting: source.avatar.as_ref().is_some_and(|avatar| avatar.stance.sprinting),
            on_ground: self.source.on_ground,
            fall_distance: source.avatar.as_ref().map_or_else(|| source.mob.as_ref().map_or(0.0_f32.to_bits(), |mob| mob.brain.fall_distance), |avatar| avatar.fall_distance),
            bedrock: self.protocol == MeleeProtocol::Bedrock,
            yaw: self.source.yaw,
            knockback_enabled: true,
        };
        let mut attacks = Vec::with_capacity(self.sweep.len().saturating_add(1));
        for (index, observation) in std::iter::once(&self.primary).chain(&self.sweep).enumerate() {
            let mut living = winning_living(context, &observation.actor)?;
            for property in registry.geometry_properties(&observation.actor, &living) {
                hydrate_property(context, &observation.actor, &mut living, property)?;
            }
            let victim_geometry = registry.geometry(&observation.actor, &living)?;
            let reach = if let Some(reach) = reach { reach } else {
                use crate::mob_melee::{MobMeleeShape, in_range};
                let source_shape = geometry.collision_shape();
                let target_shape = victim_geometry.collision_shape();
                let mount = registry.mob_mount_shape(&self.source.actor, context.values)?;
                let mount = mount.as_ref().map(|(position, shape)| MobMeleeShape { position: *position, shape });
                if !in_range(MobMeleeShape { position: self.source.position, shape: &source_shape }, mount,
                    MobMeleeShape { position: observation.position, shape: &target_shape })? {
                    if index == 0 { return Err(ApplyFailure::Condition(observation.target())); }
                    continue;
                }
                f64::MAX
            };
            let [target_undead, target_arthropod] = registry.species(&observation.actor, &living)?;
            let contact = AttackContact {
                attacker: attacker.clone(), victim: observation.contact(context.tick, victim_geometry)?,
                reach: reach.to_bits(), eye_height: geometry.eye_height, victim_yaw: observation.yaw,
                victim_on_ground: observation.on_ground, producer: self.source.actor.owner(), tick: context.tick,
            };
            if !contact.valid() {
                if index == 0 { return Err(ApplyFailure::Condition(observation.target())); }
                continue;
            }
            attacks.push(AttackIntent { actor: id.actor, source: self.source.target(), target: observation.target(),
                tick: context.tick, contact, pose, attributes: registry.attributes(), target_undead, target_arthropod,
                sweeping: index != 0 });
        }
        Ok(attacks)
    }
}

pub(crate) fn winning_living<S: crate::canonical_action::ActionState>(
    context: &crate::action_command::CommandContext<'_, S>, actor: &ActorReference,
) -> Result<crate::item_action::ActorLivingState, ApplyFailure> {
    use crate::canonical_action::Value;
    let target = actor.body_target();
    let cell = context.lookup(target)?.ok_or(ApplyFailure::Condition(target))?;
    match (actor, &cell.value) {
        (ActorReference::Player { lease, epoch, chunk, .. }, Value::Living(living)) => {
            if !context.state.validate_live_player_lease(*lease) || living.combat.actor != actor.actor()
                || living.avatar.as_ref().is_none_or(|avatar| avatar.spatial_epoch != *epoch || avatar.world != chunk.world) {
                return Err(ApplyFailure::Condition(target));
            }
        }
        (ActorReference::Entity { id, birth, epoch, owner, chunk }, Value::Entity(body)) => {
            if !body.belongs_to(*id, *chunk) || body.birth != *birth || body.spatial_epoch != *epoch || body.owner != *owner {
                return Err(ApplyFailure::Condition(target));
            }
        }
        _ => return Err(ApplyFailure::Type(target)),
    }
    crate::living_property::state(&cell.value).cloned().ok_or(ApplyFailure::Type(target))
}

pub(crate) fn hydrate_property<S: crate::canonical_action::ActionState>(
    context: &crate::action_command::CommandContext<'_, S>, actor: &ActorReference,
    living: &mut crate::item_action::ActorLivingState, property: crate::living_property::LivingProperty,
) -> Result<(), ApplyFailure> {
    use crate::canonical_action::Value;
    if let crate::living_property::LivingProperty::Attribute(id) = property {
        hydrate_property(context, actor, living, crate::living_property::LivingProperty::PermanentAttributeModifiers(id))?;
    }
    let target = Target::Chunk { key: actor.chunk(), part: ChunkPart::LivingProperty { actor: actor.actor(), property }, inventory: None };
    let cell = context.values.get(&target).cloned()
        .or_else(|| context.values.get(&actor.body_target()).and_then(|body| crate::derived_living::facet_cell(body, property)));
    let Some(cell) = cell else { return Err(ApplyFailure::Condition(target)); };
    let Value::LivingProperty(value) = cell.value else { return Err(ApplyFailure::Type(target)); };
    if !property.hydrate(living, &value) { return Err(ApplyFailure::Type(target)); }
    if cell.revision == 0 { living.property_versions.remove(&property); }
    else { living.property_versions.insert(property, cell.revision); }
    Ok(())
}

#[cfg(test)]
mod melee_tests {
    use super::*;
    use crate::action_command::CommandContext;
    use crate::canonical_action::{ActionContext, ActionId, ActionState, AppliedWrite, Cell, TruthView, Value};
    use crate::living_property::{LivingProperty as P, LivingPropertyValue as V};
    use std::collections::BTreeMap;

    struct State(Vec<crate::canonical_action::LivePlayerLease>);
    impl ActionState for State {
        fn read(&self, _: TruthView, _: Target) -> Option<Cell> { None }
        fn validate_live_player_lease(&self, lease: crate::canonical_action::LivePlayerLease) -> bool { self.0.contains(&lease) }
        fn commit(&mut self, _: ActionContext, _: TruthView, _: &[AppliedWrite]) -> Result<(), ApplyFailure> { unreachable!() }
    }
    struct Registry;
    impl MeleeRegistry for Registry {
        fn attributes(&self) -> crate::combat_action::CombatAttributeIds {
            crate::combat_action::CombatAttributeIds { attack_damage: 1, attack_speed: 2, armor: 3, toughness: 4, knockback_resistance: 5 }
        }
        fn range_attribute(&self) -> u16 { 6 }
        fn scale_attribute(&self) -> u16 { 7 }
        fn geometry(&self, _: &ActorReference, _: &crate::item_action::ActorLivingState) -> Result<ActorGeometry, ApplyFailure> {
            Ok(ActorGeometry { half_width: 0.3_f64.to_bits(), height: 1.8_f64.to_bits(), eye_height: 1.62_f64.to_bits() })
        }
        fn species(&self, _: &ActorReference, _: &crate::item_action::ActorLivingState) -> Result<[bool; 2], ApplyFailure> { Ok([false; 2]) }
    }
    fn observation(contact: crate::item_contact::ContactObservation) -> ActorObservation {
        let ContactIdentity::Player(lease) = contact.identity else { unreachable!() };
        ActorObservation { actor: ActorReference::Player { lease, chunk: contact.chunk, epoch: contact.epoch, owner: contact.owner },
            position: contact.position, yaw: 0.0_f32.to_bits(), on_ground: true, sequence: contact.observed_sequence,
            velocity: VelocityCut { reliable: VelocityValues { velocity: [0; 3], revisions: [0; 3] }, effective: [0; 3] } }
    }
    #[test]
    fn winning_range_and_stance_are_inferred_without_frozen_attack_options() {
        let (source_lease, mut source, source_contact) = crate::combat_action::tests::fixture(1);
        let (victim_lease, mut victim, mut victim_contact) = crate::combat_action::tests::fixture(2);
        source.combat.effective_attributes.insert(6, 3.0_f64.to_bits());
        source.combat.effective_attributes.insert(7, 1.0_f64.to_bits());
        victim.combat.effective_attributes.insert(7, 1.0_f64.to_bits());
        victim_contact.position[0] = 3.5_f64.to_bits();
        let source_target = source_contact.target();
        let victim_target = victim_contact.target();
        let command = MeleeAttackCommand { source: observation(source_contact), primary: observation(victim_contact), sweep: Vec::new(), protocol: MeleeProtocol::Java };
        let state = State(vec![source_lease, victim_lease]);
        let identity = crate::action_identity::ActionIdentity::Issued(ActionId { actor: command.source.id(), incarnation: source_lease.incarnation, sequence: 1, family: 7 });
        let mut values = BTreeMap::from([(source_target, Cell { revision: 0, value: Value::Living(source.into()) }), (victim_target, Cell { revision: 0, value: Value::Living(victim.into()) })]);
        let resolve = |values: &BTreeMap<Target, Cell>| command.resolve_attacks(&CommandContext { known_absent: &std::collections::BTreeSet::new(), state: &state, identity, tick: TickStamp(100), values }, &Registry);
        let attacks = resolve(&values).unwrap();
        assert_eq!(attacks.len(), 1);
        assert!(!attacks[0].pose.sprinting);
        let property = |property| Target::Chunk { key: command.source.actor.chunk(), part: ChunkPart::LivingProperty { actor: command.source.id(), property }, inventory: None };
        values.insert(property(P::PlayerStance), Cell { revision: 1, value: Value::LivingProperty(V::PlayerStance(crate::item_action::PlayerStance { sprinting: true, ..Default::default() })) });
        assert!(resolve(&values).unwrap()[0].pose.sprinting);
        values.insert(property(P::Attribute(6)), Cell { revision: 1, value: Value::LivingProperty(V::Attribute { base: Some(1.0_f64.to_bits()), modifiers: Vec::new(), transient_modifiers: Vec::new() }) });
        assert_eq!(resolve(&values), Err(ApplyFailure::Condition(victim_target)));
    }
    #[test]
    fn absent_actor_is_distinct_from_unavailable_original_facts() {
        let (source_lease, _, source_contact) = crate::combat_action::tests::fixture(1);
        let (victim_lease, _, victim_contact) = crate::combat_action::tests::fixture(2);
        let command = MeleeAttackCommand { source: observation(source_contact), primary: observation(victim_contact), sweep: Vec::new(), protocol: MeleeProtocol::Java };
        let state = State(vec![source_lease, victim_lease]);
        let identity = crate::action_identity::ActionIdentity::Issued(ActionId { actor: command.source.id(), incarnation: source_lease.incarnation, sequence: 1, family: 7 });
        let values = BTreeMap::new();
        let absent = std::collections::BTreeSet::new();
        let context = CommandContext { state: &state, identity, tick: TickStamp(100), values: &values, known_absent: &absent };
        assert_eq!(command.resolve_attacks(&context, &Registry), Err(ApplyFailure::MissingStakeholder(command.source.target().stakeholder())));
        let absent = std::collections::BTreeSet::from([command.source.target()]);
        let context = CommandContext { known_absent: &absent, ..context };
        assert_eq!(command.resolve_attacks(&context, &Registry), Err(ApplyFailure::Condition(command.source.target())));
    }

    #[test]
    fn entity_melee_infers_geometry_reach_without_player_range_attribute() {
        use crate::actor::ActorId;
        use crate::canonical_action::{ActionRef, SpatialEpoch};
        use crate::entity_body::{EntityBirth, EntityBody, EntityPayload, EntitySpatial};
        use crate::identity::ServerId;
        use crate::protocol::GlobalEntityId;
        use crate::realm_completion::PeerGeneration;
        let (_, mut source, source_contact) = crate::combat_action::tests::fixture(1);
        let (lease, mut victim, victim_contact) = crate::combat_action::tests::fixture(2);
        let id = GlobalEntityId { origin: ServerId(1), local_id: 31, generation: 2 };
        let owner = PeerGeneration { server: id.origin, generation: id.generation };
        let reference = ActionRef { tick: TickStamp(0), id: ActionId { actor: ActorId::Entity(id), incarnation: id.generation, sequence: 1, family: 4 } };
        source.combat.actor = ActorId::Entity(id);
        source.food.actor = ActorId::Entity(id);
        source.avatar = None;
        source.combat.effective_attributes.insert(7, 1.0_f64.to_bits());
        victim.combat.effective_attributes.insert(7, 1.0_f64.to_bits());
        let body = EntityBody { mount: None, tags: Default::default(), tags_revision: 0,
            relation_versions: Default::default(), passengers: Default::default(), id,
            birth: EntityBirth { action: reference, source: owner, uuid: uuid::Uuid::from_u128(31) },
            owner, kind: 4, spatial_epoch: SpatialEpoch::Action(reference),
            spatial: EntitySpatial { world: source_contact.world, position: source_contact.position,
                velocity_micro: [0; 3], yaw: 0.0_f32.to_bits(), pitch: 0.0_f32.to_bits(), on_ground: true },
            payload: EntityPayload::Living(source) };
        let mut source_observation = observation(source_contact);
        source_observation.actor = ActorReference::Entity { id, birth: body.birth.clone(), chunk: source_observation.actor.chunk(), epoch: body.spatial_epoch, owner };
        let command = MeleeAttackCommand { source: source_observation, primary: observation(victim_contact.clone()), sweep: Vec::new(), protocol: MeleeProtocol::Java };
        let values = BTreeMap::from([(command.source.target(), Cell { revision: 0, value: Value::Entity(body.into()) }),
            (command.primary.target(), Cell { revision: 0, value: Value::Living(victim.into()) })]);
        let identity = crate::action_identity::ActionIdentity::Issued(ActionId { actor: ActorId::Entity(id), incarnation: id.generation, sequence: 2, family: 7 });
        let attacks = command.resolve_attacks(&CommandContext { known_absent: &std::collections::BTreeSet::new(), state: &State(vec![lease]), identity, tick: TickStamp(100), values: &values }, &Registry).unwrap();
        assert_eq!(attacks.len(), 1);
        let reach = f64::from_bits(attacks[0].contact.reach);
        assert_eq!(reach, f64::MAX);
        assert_eq!(attacks[0].actor, ActorId::Entity(id));
        let mut elevated = command.clone();
        elevated.primary.position[1] = (f64::from_bits(elevated.source.position[1]) + 1.8).to_bits();
        assert_eq!(elevated.resolve_attacks(&CommandContext { state: &State(vec![lease]), identity, tick: TickStamp(100), values: &values, known_absent: &Default::default() }, &Registry),
            Err(ApplyFailure::Condition(elevated.primary.target())));
    }

}
