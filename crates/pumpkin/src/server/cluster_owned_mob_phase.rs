use std::collections::BTreeSet;

use pumpkin_cluster::actor_action_scope::ActorActionDraft;
use pumpkin_cluster::canonical_action::{Action, ActionId, ActionState, ApplyFailure, TruthView, Value};
use pumpkin_cluster::entity_body::{EntityBody, EntityPayload};
use pumpkin_cluster::item_contact::ContactObservation;
use pumpkin_cluster::mob_continuation::{MobBrain, continuation_action};
use pumpkin_cluster::mob_goal::{MobIntent, tick_goals};
use pumpkin_cluster::protocol::ChunkKey;
use pumpkin_cluster::time::TickStamp;

use super::cluster_owned_mob_goal_environment::OwnedMobGoalEnvironment;
use super::cluster_owned_mob_world::{MobWorldRules, MobWorldView};

#[derive(Clone)]
pub(super) struct FrozenMobPhase {
    pub body: EntityBody,
    pub contact: ContactObservation,
    pub brain: MobBrain,
    pub spatial: pumpkin_cluster::entity_body::EntitySpatial,
    pub intents: Vec<MobIntent>,
}

impl FrozenMobPhase {
    pub(super) fn capture<S: ActionState>(
        snapshot: &S,
        draft: &ActorActionDraft,
        contact: &ContactObservation,
        effective_spatial: &pumpkin_cluster::entity_body::EntitySpatial,
        contacts: &super::cluster_owned_mob_contacts::MobContactIndex,
        chunks: &BTreeSet<ChunkKey>,
        rules: MobWorldRules,
    ) -> Result<Option<Self>, ApplyFailure> {
        let body = match draft.view(snapshot).read(TruthView::Local, contact.target()) {
            Some(cell) => match cell.value {
                Value::Entity(body) => *body,
                Value::Living(_) => return Ok(None),
                _ => return Err(ApplyFailure::Type(contact.target())),
            },
            None => return Err(ApplyFailure::Condition(contact.target())),
        };
        let EntityPayload::Living(living) = &body.payload else { return Ok(None); };
        let Some(mob) = &living.mob else { return Ok(None); };
        if effective_spatial.world != contact.world || effective_spatial.position != contact.position
            || body.owner != contact.owner || body.spatial_epoch != contact.epoch
            || !matches!(&contact.identity, pumpkin_cluster::item_contact::ContactIdentity::Entity { id, birth }
                if *id == body.id && *birth == body.birth) {
            return Err(ApplyFailure::InvalidAction);
        }
        if living.combat.health_milli == 0
            || mob.brain.processed_tick.is_some_and(|previous|
                previous == contact.simulation_tick || previous.is_after(contact.simulation_tick)) {
            return Ok(None);
        }
        let mut brain = mob.brain.clone();
        brain.navigation.advance_clock();
        let intents = if brain.ai_disabled { Vec::new() } else {
            let dimensions = super::cluster_owned_mob_metadata::dimensions(living, body.kind)
                .ok_or(ApplyFailure::InvalidAction)?;
            let at = contact.position.map(f64::from_bits);
            let bounds = pumpkin_util::math::boundingbox::BoundingBox::new_from_pos(
                at[0], at[1], at[2], &dimensions);
            let fluid = MobWorldView::new(snapshot, draft, contact.world, chunks)
                .with_rules(rules).fluid_contacts(bounds, 4096)?;
            let environment = OwnedMobGoalEnvironment::new(snapshot, draft, &body, living,
                contact, contacts, chunks, rules, fluid.contacts[0].touching);
            let intents = tick_goals(&mut brain, &mob.reproduction, &environment);
            environment.finish()?;
            intents
        };
        brain.processed_tick = Some(contact.simulation_tick);
        if !brain.valid() { return Err(ApplyFailure::InvalidAction); }
        Ok(Some(Self { body, contact: contact.clone(), brain, spatial: effective_spatial.clone(), intents }))
    }

    pub(super) fn continuation(&self, id: ActionId) -> Result<Action, ApplyFailure> {
        continuation_action(&self.body, self.contact.chunk, self.contact.owner,
            self.contact.simulation_tick, id, self.brain.clone())
    }

    pub(super) fn graze<S: ActionState>(&self, snapshot: &S, id: ActionId, index: usize)
        -> Result<Action, ApplyFailure> {
        let Some(MobIntent::EatGrass { position, expected, replacement, modify_world }) =
            self.intents.get(index) else { return Err(ApplyFailure::InvalidAction); };
        pumpkin_cluster::mob_world_action::graze(snapshot, &self.body, self.contact.chunk,
            self.contact.owner, self.contact.simulation_tick, id, *position, *expected,
            *replacement, *modify_world)
    }

    pub(super) fn tick(&self) -> TickStamp { self.contact.simulation_tick }
}

pub(super) fn advance_fall(
    brain: &mut MobBrain,
    contact: ContactObservation,
    previous_y: f64,
    on_ground: bool,
    water: bool,
    damage_enabled: bool,
) -> Result<Option<pumpkin_cluster::combat_action::FallDamageIntent>, ApplyFailure> {
    use pumpkin_cluster::canonical_action::{ChunkPart, Target};
    if !contact.valid() || !previous_y.is_finite() || previous_y.abs() > 30_000_000.0
        || !f32::from_bits(brain.fall_distance).is_finite()
        || !(0.0..=30_000_000.0).contains(&f32::from_bits(brain.fall_distance)) {
        return Err(ApplyFailure::InvalidAction);
    }
    if water {
        brain.fall_distance = 0.0_f32.to_bits();
        return Ok(None);
    }
    let position = contact.position.map(f64::from_bits);
    let distance = (f64::from(f32::from_bits(brain.fall_distance))
        + (previous_y - position[1]).max(0.0)).min(30_000_000.0) as f32;
    if !on_ground {
        brain.fall_distance = distance.to_bits();
        return Ok(None);
    }
    brain.fall_distance = 0.0_f32.to_bits();
    if distance == 0.0 { return Ok(None); }
    let block = pumpkin_cluster::protocol::BlockPos { x: position[0].floor() as i32,
        y: (position[1] - 0.2).floor() as i32, z: position[2].floor() as i32 };
    let landing = Target::Chunk { key: ChunkKey::new(contact.world,
        block.x.div_euclid(16), block.z.div_euclid(16)),
        part: ChunkPart::Block(block), inventory: None };
    Ok(Some(pumpkin_cluster::combat_action::FallDamageIntent {
        contact, distance: distance.to_bits(), landing,
        jump_boost_effect: u16::from(pumpkin_data::effect::StatusEffect::JUMP_BOOST.id),
        damage_enabled, sneaking: false,
        armor_attribute: u16::from(pumpkin_data::attributes::Attributes::ARMOR.id),
        toughness_attribute: u16::from(pumpkin_data::attributes::Attributes::ARMOR_TOUGHNESS.id),
    }))
}

pub(super) struct MobTravelOutput {
    pub sample: pumpkin_cluster::item_spatial::ItemSpatialSample,
    pub fall: Option<pumpkin_cluster::combat_action::FallDamageIntent>,
    pub destination: ChunkKey,
}

impl FrozenMobPhase {
    pub(super) fn travel<S: ActionState>(
        &mut self,
        snapshot: &S,
        draft: &ActorActionDraft,
        chunks: &BTreeSet<ChunkKey>,
        rules: MobWorldRules,
        sequence: pumpkin_cluster::identity::PlayerSeq,
        damage_enabled: bool,
        entity_spatials: &std::collections::BTreeMap<pumpkin_cluster::actor::ActorId,
            pumpkin_cluster::entity_body::EntitySpatial>,
    ) -> Result<MobTravelOutput, ApplyFailure> {
        if self.body.mount.is_some() {
            return self.mounted_travel(snapshot, draft, chunks, sequence, entity_spatials);
        }
        use pumpkin_cluster::living_travel::{TravelFluid, TravelInput, TravelMove};
        use pumpkin_data::attributes::Attributes;
        use pumpkin_util::math::{boundingbox::BoundingBox, vector3::Vector3};
        let EntityPayload::Living(living) = &self.body.payload else {
            return Err(ApplyFailure::InvalidAction);
        };
        let dimensions = super::cluster_owned_mob_metadata::dimensions(living, self.body.kind)
            .ok_or(ApplyFailure::InvalidAction)?;
        let attribute = |kind: &Attributes| {
            let base = living.combat.effective_attributes.get(&u16::from(kind.id))
                .copied().map(f64::from_bits).unwrap_or(kind.default_value);
            living.attribute(u16::from(kind.id), base)
        };
        let position = self.spatial.position.map(f64::from_bits);
        let bounds = BoundingBox::new_from_pos(position[0], position[1], position[2], &dimensions);
        let world = MobWorldView::new(snapshot, draft, self.contact.world, chunks).with_rules(rules);
        let fluids = world.fluid_contacts(bounds, 4096)?;
        let water = fluids.contacts[0];
        let lava = fluids.contacts[1];
        let block = |offset: f64| pumpkin_cluster::protocol::BlockPos {
            x: position[0].floor() as i32, y: (position[1] - offset).floor() as i32,
            z: position[2].floor() as i32 };
        let slipperiness = f64::from(world.block(block(0.500_001))?.to_state().id.to_block().slipperiness);
        let feet = world.block(block(0.0))?.to_state().id.to_block();
        let climbing = feet.id == pumpkin_data::Block::LADDER.id
            || feet.id == pumpkin_data::Block::VINE.id;
        let movement_speed = attribute(&Attributes::MOVEMENT_SPEED);
        let step_height = attribute(&Attributes::STEP_HEIGHT);
        let yaw = f32::from_bits(self.spatial.yaw);
        let (mut yaw, jump) = self.brain.movement.advance(position, yaw,
            self.spatial.on_ground, movement_speed, step_height, f64::from(dimensions.width));
        if let Some((navigation_yaw, speed, navigation_jump)) = self.brain.navigation.ground_waypoint(
            position, position[1], dimensions.width, movement_speed, step_height,
            self.spatial.on_ground, yaw) {
            yaw = navigation_yaw;
            self.brain.movement.input = [0.0_f64.to_bits(), 0.0_f64.to_bits(), speed.to_bits()];
            self.brain.jump_requested |= navigation_jump;
        }
        self.brain.jump_requested |= jump || self.intents.iter().any(|intent| matches!(intent, MobIntent::Jump));
        for intent in &self.intents {
            if let MobIntent::LookAt(target) = intent {
                self.brain.look.look_at(target.map(f64::from_bits), 30.0, 30.0);
            }
        }
        let mut eye = position;
        eye[1] += f64::from(dimensions.eye_height);
        let pitch = self.brain.look.advance(eye, yaw, self.brain.navigation.goal.is_some(), 75.0);
        let effect = |kind: u16| living.effects.iter()
            .find(|effect| effect.kind == kind && effect.duration != 0).map(|effect| effect.amplifier);
        let mut velocity = self.spatial.velocity_micro.map(|value| value as f64 / 1_000_000.0);
        for (axis, property) in [pumpkin_cluster::living_property::LivingProperty::VelocityX,
            pumpkin_cluster::living_property::LivingProperty::VelocityY,
            pumpkin_cluster::living_property::LivingProperty::VelocityZ].into_iter().enumerate() {
            let target = pumpkin_cluster::canonical_action::Target::Chunk { key: self.contact.chunk,
                part: pumpkin_cluster::canonical_action::ChunkPart::LivingProperty {
                    actor: self.contact.actor(), property }, inventory: None };
            let cell = draft.view(snapshot).read(TruthView::Local, target)
                .ok_or(ApplyFailure::Condition(target))?;
            if self.brain.velocity_revision[axis] != cell.revision {
                velocity[axis] = living.combat.velocity_micro[axis] as f64 / 1_000_000.0;
                self.brain.velocity_revision[axis] = cell.revision;
            }
        }
        if self.brain.jump_requested {
            if water.touching || lava.touching { velocity[1] += 0.04; }
            else if self.spatial.on_ground {
                velocity[1] = attribute(&Attributes::JUMP_STRENGTH)
                    + effect(u16::from(pumpkin_data::effect::StatusEffect::JUMP_BOOST.id))
                        .map_or(0.0, |amplifier| (f64::from(amplifier) + 1.0) * 0.1);
            }
        }
        self.brain.jump_requested = false;
        let fluid = if water.touching { TravelFluid::Water { friction: 0.8,
            efficiency: attribute(&Attributes::WATER_MOVEMENT_EFFICIENCY).clamp(0.0, 1.0),
            dolphins_grace: effect(u16::from(pumpkin_data::effect::StatusEffect::DOLPHINS_GRACE.id)).is_some() } }
            else if lava.touching { TravelFluid::Lava { shallow: lava.height <= 0.4 } }
            else { TravelFluid::Air };
        let input = TravelInput { position, velocity,
            movement: self.brain.movement.input.map(f64::from_bits), yaw: f64::from(yaw),
            on_ground: self.spatial.on_ground, sprinting: false, climbing,
            jumping: false, flying: false, movement_speed, air_speed: 0.02,
            slipperiness, gravity: attribute(&Attributes::GRAVITY), vertical_drag: 0.98,
            slow_falling: effect(u16::from(pumpkin_data::effect::StatusEffect::SLOW_FALLING.id)).is_some(),
            levitation: effect(u16::from(pumpkin_data::effect::StatusEffect::LEVITATION.id)), fluid };
        let output = pumpkin_cluster::living_travel::travel(input, |_, desired| {
            let collision = pumpkin_util::math::collision::resolve_step_movement(
                bounds, Vector3::new(desired[0], desired[1], desired[2]),
                step_height.clamp(0.0, 10.0), self.spatial.on_ground,
                |bounds, movement| world.move_bounds(bounds, movement, 16384))?;
            let displacement = [collision.movement.x, collision.movement.y, collision.movement.z];
            let translated = BoundingBox::new_from_pos(position[0] + displacement[0],
                position[1] + displacement[1] + 0.6, position[2] + displacement[2], &dimensions);
            let fluid = world.fluid_contacts(translated, 4096)?;
            Ok::<_, super::cluster_owned_mob_world::MobReadFailure>(TravelMove {
                displacement, horizontal_collision: collision.horizontal_collision,
                vertical_collision: displacement[1] != desired[1],
                on_ground: desired[1] < 0.0 && displacement[1] != desired[1]
                    || desired[1] == 0.0 && self.spatial.on_ground,
                fluid_exit_clear: !fluid.contacts.iter().any(|contact| contact.touching),
            })
        })?.ok_or(ApplyFailure::InvalidAction)?;
        let destination = ChunkKey::new(self.contact.world,
            (output.position[0].floor() as i32).div_euclid(16),
            (output.position[2].floor() as i32).div_euclid(16));
        if !chunks.contains(&destination) { return Err(ApplyFailure::Capacity); }
        let mut spatial = self.spatial.clone();
        spatial.position = output.position.map(f64::to_bits);
        spatial.velocity_micro = output.velocity.map(|value| (value * 1_000_000.0).round() as i64);
        spatial.yaw = yaw.to_bits(); spatial.pitch = pitch.to_bits();
        spatial.on_ground = output.collision.on_ground;
        let mut contact = self.contact.clone();
        contact.position = spatial.position;
        let fall = advance_fall(&mut self.brain, contact, position[1], spatial.on_ground,
            output.reset_fall_distance, damage_enabled)?;
        Ok(MobTravelOutput { destination, fall,
            sample: pumpkin_cluster::item_spatial::ItemSpatialSample {
                id: self.body.id, birth: self.body.birth.clone(), epoch: self.body.spatial_epoch,
                issuer: self.body.owner, tick: self.contact.simulation_tick, sequence, spatial,
                head_yaw: self.brain.look.head_yaw } })
    }
}

impl FrozenMobPhase {
    pub(super) fn navigate(
        &mut self,
        original: &super::cluster_chunk_state::OwnedChunkStore,
        registry: std::sync::Arc<crate::block::registry::BlockRegistry>,
        draft: &ActorActionDraft,
        chunks: &BTreeSet<ChunkKey>,
        rules: MobWorldRules,
    ) -> Result<(), super::cluster_owned_mob_world::MobReadFailure> {
        if self.body.mount.is_some() { return Ok(()); }
        use super::cluster_owned_mob_navigation::{MobNavigationSnapshot, find_path};
        use super::cluster_owned_mob_world::MobReadFailure;
        let Some(goal) = self.brain.navigation.goal.clone() else { return Ok(()); };
        let destination = goal.destination.map(f64::from_bits);
        if !self.brain.navigation.needs_path(destination) { return Ok(()); }
        let EntityPayload::Living(living) = &self.body.payload else { return Err(MobReadFailure::InvalidBounds); };
        let dimensions = super::cluster_owned_mob_metadata::dimensions(living, self.body.kind)
            .ok_or(MobReadFailure::InvalidBounds)?;
        let position = self.spatial.position.map(f64::from_bits);
        let step = pumpkin_data::attributes::Attributes::STEP_HEIGHT;
        let base = living.combat.effective_attributes.get(&u16::from(step.id))
            .copied().map(f64::from_bits).unwrap_or(step.default_value);
        let step = living.attribute(u16::from(step.id), base);
        let data = crate::entity::ai::pathfinder::node_evaluator::MobData {
            position: pumpkin_util::math::vector3::Vector3::new(position[0], position[1], position[2]),
            width: dimensions.width, height: dimensions.height, max_step_height: step as f32,
            max_fall_distance: 3.0, can_swim: self.brain.navigation.can_float,
            can_walk_on_water: false, avoids_fire: true, avoids_water: false,
            on_ground: self.spatial.on_ground, is_in_water: false,
            sea_level: rules.sea_level, min_y: rules.minimum_y,
            path_type_malus: [None; crate::entity::ai::pathfinder::node::PATH_TYPE_COUNT],
        };
        let displacement = std::array::from_fn::<_, 3, _>(|axis| destination[axis] - position[axis]);
        let length = displacement.iter().map(|value| value * value).sum::<f64>().sqrt();
        let destination = if length > 16.0 {
            std::array::from_fn(|axis| position[axis] + displacement[axis] * 16.0 / length)
        } else { destination };
        let at = pumpkin_cluster::protocol::BlockPos { x: destination[0].floor() as i32,
            y: destination[1].floor() as i32, z: destination[2].floor() as i32 };
        let snapshot = MobNavigationSnapshot::capture(original, self.contact.world, chunks,
            rules.minimum_y, rules.sea_level);
        let result = find_path(snapshot, registry, draft, &self.brain.navigation,
            data, at, 1024, self.brain.navigation.reach_range, 16.0)?;
        match result {
            Some(path) => {
                self.brain.navigation.path = path.nodes;
                self.brain.navigation.path_reached = path.reached;
                self.brain.navigation.path_distance = path.distance;
                self.brain.navigation.target = Some(path.target);
                self.brain.navigation.cursor = 0;
                self.brain.navigation.speed = goal.speed;
                self.brain.navigation.repath_cooldown = 10;
            }
            None => {
                self.brain.navigation.path.clear();
                self.brain.navigation.cursor = 0;
                self.brain.navigation.repath_cooldown = 10;
            }
        }
        Ok(())
    }

    pub(super) fn attack<S: ActionState>(
        &self, snapshot: &S, id: ActionId, victim: &ContactObservation,
        victim_spatial: &pumpkin_cluster::entity_body::EntitySpatial,
    ) -> Result<Action, ApplyFailure> {
        use pumpkin_cluster::combat_action::{AttackContact, AttackPose};
        let EntityPayload::Living(living) = &self.body.payload else { return Err(ApplyFailure::InvalidAction); };
        let dimensions = super::cluster_owned_mob_metadata::dimensions(living, self.body.kind)
            .ok_or(ApplyFailure::InvalidAction)?;
        let victim_kind = match snapshot.read(TruthView::Local, victim.target()).map(|cell| cell.value) {
            Some(Value::Entity(body)) => pumpkin_data::entity::EntityType::from_raw(body.kind)
                .cloned().ok_or(ApplyFailure::InvalidAction)?,
            Some(Value::Living(_)) => pumpkin_data::entity::EntityType::PLAYER,
            _ => return Err(ApplyFailure::Type(victim.target())),
        };
        let width = f64::from_bits(self.contact.half_width) * 4.0;
        let reach = (width * width + f64::from_bits(victim.half_width) * 2.0).sqrt().min(8.0);
        let contact = AttackContact { attacker: self.contact.clone(), victim: victim.clone(),
            reach: reach.to_bits(), eye_height: f64::from(dimensions.eye_height).to_bits(),
            victim_yaw: victim_spatial.yaw, victim_on_ground: victim_spatial.on_ground,
            producer: self.contact.owner, tick: self.contact.simulation_tick };
        let pose = AttackPose { sprinting: false, on_ground: self.spatial.on_ground,
            fall_distance: self.brain.fall_distance, bedrock: false,
            yaw: self.spatial.yaw, knockback_enabled: true };
        super::cluster_world_combat_actions::materialize_attack(snapshot,
            super::cluster_world_combat_actions::AttackMaterialization {
                id, actor_chunk: self.contact.chunk, contact, pose, victim_kind,
                sweep_victims: Vec::new(),
            })
    }
}

pub(super) struct MobEmission {
    pub actions: Vec<(Action, ActorActionDraft)>,
    pub sample: pumpkin_cluster::item_spatial::ItemSpatialSample,
    pub destination: ChunkKey,
}

impl FrozenMobPhase {
    pub(super) fn action_count(&self, travel: &MobTravelOutput) -> Result<usize, ApplyFailure> {
        let mut count = 1 + usize::from(travel.fall.is_some());
        for intent in &self.intents {
            match intent {
                MobIntent::Attack(_) | MobIntent::Breed(_) | MobIntent::EatGrass { .. } => count += 1,
                MobIntent::LookAt(_) | MobIntent::Jump | MobIntent::StartGrazing => {},
                MobIntent::BeginUse | MobIntent::EndUse | MobIntent::Shoot { .. } | MobIntent::Door { .. } =>
                    return Err(ApplyFailure::InvalidAction),
            }
        }
        Ok(count)
    }

    pub(super) fn materialize(
        mut self,
        travel: MobTravelOutput,
        mut snapshot: std::sync::Arc<super::cluster_world_callback_snapshot::CallbackTickSnapshot>,
        mut draft: ActorActionDraft,
        ids: &[ActionId],
        allocator: &mut pumpkin_cluster::entity_origin::EntityOriginAllocator,
        primary: pumpkin_cluster::realm_completion::PeerGeneration,
        contacts: &super::cluster_owned_mob_contacts::MobContactIndex,
        entity_spatials: &std::collections::BTreeMap<pumpkin_cluster::actor::ActorId,
            pumpkin_cluster::entity_body::EntitySpatial>,
        timestamp: u64,
    ) -> Result<MobEmission, ApplyFailure> {
        use pumpkin_cluster::canonical_action::{ActionRef, SemanticIntent};
        use pumpkin_cluster::entity_body::EntitySpatial;
        use pumpkin_cluster::item_contact::ContactIdentity;
        if ids.len() != self.action_count(&travel)? { return Err(ApplyFailure::InvalidAction); }
        let mut output = Vec::with_capacity(ids.len());
        for (index, intent) in self.intents.clone().into_iter().enumerate() {
            let action = match intent {
                MobIntent::EatGrass { .. } => self.graze(&draft.view(snapshot.as_ref()), ids[output.len()], index)?,
                MobIntent::Attack(identity) => {
                    let victim = contacts.get(&identity)
                        .ok_or(ApplyFailure::InvalidAction)?;
                    let spatial = match &identity {
                        ContactIdentity::Player(lease) => {
                            let pose = snapshot.player_projection(lease.gid).ok_or(ApplyFailure::InvalidAction)?;
                            EntitySpatial { world: victim.world, position: victim.position,
                                velocity_micro: pose.frame.effective.velocity.map(|value| (value * 1_000_000.0).round() as i64),
                                yaw: pose.frame.effective.yaw.to_bits(), pitch: pose.frame.effective.pitch.to_bits(),
                                on_ground: pose.frame.effective.on_ground }
                        }
                        ContactIdentity::Entity { .. } => entity_spatials.get(&victim.actor())
                            .cloned().ok_or(ApplyFailure::InvalidAction)?,
                    };
                    self.attack(&draft.view(snapshot.as_ref()), ids[output.len()], victim, &spatial)?
                }
                MobIntent::Breed(identity) => {
                    let mate = contacts.get(&identity)
                        .ok_or(ApplyFailure::InvalidAction)?;
                    let Value::Entity(mate_body) = draft.view(snapshot.as_ref()).read(TruthView::Local, mate.target())
                        .ok_or(ApplyFailure::Condition(mate.target()))?.value
                        else { return Err(ApplyFailure::Type(mate.target())); };
                    let reference = ActionRef { id: ids[output.len()], tick: self.tick() };
                    let identities = allocator.reserve(self.contact.owner, reference, 2)
                        .map_err(|_| ApplyFailure::Capacity)?;
                    let continuation = super::cluster_owned_mob_breeding::child_continuation(
                        &self.body, &mate_body, &identities[0], &mut self.brain)?;
                    let experience = (self.brain.next_random() % 7 + 1) as u32;
                    let EntityPayload::Living(living) = &self.body.payload else { return Err(ApplyFailure::InvalidAction); };
                    let breeder = living.mob.as_ref().and_then(|mob| mob.reproduction.breeder);
                    let credit = breeder.and_then(|breeder| contacts.player(breeder)).and_then(|contact| match &contact.identity {
                        ContactIdentity::Player(lease) =>
                            Some(pumpkin_cluster::mob_breeding::MobBreedingCredit {
                                breeder: *lease, chunk: contact.chunk,
                                statistic_category: pumpkin_data::statistic::StatisticCategory::Custom as u16,
                                statistic_key: pumpkin_data::statistic::CustomStatistic::AnimalsBred as u32,
                                timestamp }),
                        _ => None,
                    });
                    let intent = super::cluster_owned_mob_breeding::prepare_intent(
                        [self.contact.clone(), mate.clone()], &self.body, reference, &identities,
                        continuation, experience, primary, credit)?;
                    snapshot = snapshot.with_reserved_admissions(allocator, reference, self.contact.owner,
                        &[intent.child.clone(), intent.experience.clone()])?;
                    intent.capture(&draft.view(snapshot.as_ref()))?
                }
                MobIntent::LookAt(_) | MobIntent::Jump | MobIntent::StartGrazing => continue,
                _ => return Err(ApplyFailure::InvalidAction),
            };
            draft.issue(snapshot.as_ref(), action).map_err(|_| ApplyFailure::InvalidAction)?;
            output.push((draft.actions().last().unwrap().clone(), draft.clone()));
        }
        if let Some(fall) = travel.fall {
            let intent = pumpkin_cluster::combat_action::CombatIntent { falls: vec![fall],
                ..Default::default() };
            let view = draft.view(snapshot.as_ref());
            let values = intent.targets().map(|target| view.read(TruthView::Local, target)
                .map(|cell| (target, cell)).ok_or(ApplyFailure::Condition(target)))
                .collect::<Result<std::collections::BTreeMap<_, _>, _>>()?;
            let action = intent.capture_with(&view, ids[output.len()], self.contact.chunk, &values, Vec::new())?;
            if !matches!(action.semantic_intent, Some(SemanticIntent::Combat(_))) {
                return Err(ApplyFailure::InvalidAction);
            }
            draft.issue(snapshot.as_ref(), action).map_err(|_| ApplyFailure::InvalidAction)?;
            output.push((draft.actions().last().unwrap().clone(), draft.clone()));
        }
        let action = self.continuation(ids[output.len()])?;
        draft.issue(snapshot.as_ref(), action).map_err(|_| ApplyFailure::InvalidAction)?;
        output.push((draft.actions().last().unwrap().clone(), draft));
        Ok(MobEmission { actions: output, sample: travel.sample, destination: travel.destination })
    }
}

impl FrozenMobPhase {
    fn mounted_travel<S: ActionState>(
        &mut self,
        snapshot: &S,
        draft: &ActorActionDraft,
        chunks: &BTreeSet<ChunkKey>,
        sequence: pumpkin_cluster::identity::PlayerSeq,
        entity_spatials: &std::collections::BTreeMap<pumpkin_cluster::actor::ActorId,
            pumpkin_cluster::entity_body::EntitySpatial>,
    ) -> Result<MobTravelOutput, ApplyFailure> {
        use pumpkin_cluster::canonical_action::{ChunkPart, Target};
        use pumpkin_cluster::actor::ActorId;
        use pumpkin_cluster::item_contact::ContactIdentity;
        let Some(ContactIdentity::Entity { id, birth }) = self.body.mount.as_ref() else {
            return Err(ApplyFailure::InvalidAction);
        };
        let parent = chunks.iter().find_map(|chunk| {
            let target = Target::Chunk { key: *chunk, part: ChunkPart::Entity(*id), inventory: None };
            match draft.view(snapshot).read(TruthView::Local, target)?.value {
                Value::Entity(body) if body.birth == *birth => Some(body),
                _ => None,
            }
        }).ok_or(ApplyFailure::Capacity)?;
        let identity = ContactIdentity::Entity { id: self.body.id, birth: self.body.birth.clone() };
        let mut seats = parent.passengers.iter().filter(|(_, rider)| **rider == identity);
        let seat = *seats.next().ok_or(ApplyFailure::InvalidAction)?.0;
        if seats.next().is_some() || parent.spatial.world != self.spatial.world {
            return Err(ApplyFailure::InvalidAction);
        }
        let effective = entity_spatials.get(&ActorId::Entity(parent.id))
            .ok_or(ApplyFailure::Capacity)?;
        if effective.world != self.spatial.world { return Err(ApplyFailure::InvalidAction); }
        let position = match &parent.payload {
            EntityPayload::Vehicle(_) => super::cluster_vehicle_control::passenger_attachment(
                &parent, effective, seat).ok_or(ApplyFailure::InvalidAction)?,
            EntityPayload::Living(living) => {
                let dimensions = super::cluster_owned_mob_metadata::dimensions(living, parent.kind)
                    .ok_or(ApplyFailure::InvalidAction)?;
                let mut at = effective.position.map(f64::from_bits);
                at[1] += f64::from(dimensions.height) * 0.75;
                at
            }
            _ => return Err(ApplyFailure::InvalidAction),
        };
        let destination = ChunkKey::new(self.spatial.world,
            (position[0].floor() as i32).div_euclid(16),
            (position[2].floor() as i32).div_euclid(16));
        if !chunks.contains(&destination) { return Err(ApplyFailure::Capacity); }
        let mut spatial = self.spatial.clone();
        spatial.position = position.map(f64::to_bits);
        spatial.velocity_micro = effective.velocity_micro;
        spatial.on_ground = effective.on_ground;
        self.brain.fall_distance = 0.0_f32.to_bits();
        self.brain.jump_requested = false;
        self.brain.movement.input = [0.0_f64.to_bits(); 3];
        self.brain.navigation.stop();
        Ok(MobTravelOutput { destination, fall: None,
            sample: pumpkin_cluster::item_spatial::ItemSpatialSample {
                id: self.body.id, birth: self.body.birth.clone(), epoch: self.body.spatial_epoch,
                issuer: self.body.owner, tick: self.contact.simulation_tick, sequence, spatial,
                head_yaw: self.brain.look.head_yaw } })
    }
}
