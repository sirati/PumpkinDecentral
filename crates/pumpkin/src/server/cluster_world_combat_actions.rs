use std::collections::BTreeMap;

use pumpkin_cluster::canonical_action::{Action, ActionId, ActionState, ApplyFailure, TruthView};
use pumpkin_cluster::combat_action::{
    AttackClockIntent, AttackContact, AttackIntent, AttackPose, CombatAttributeIds, CombatIntent,
};
use pumpkin_cluster::protocol::ChunkKey;
use pumpkin_data::attributes::Attributes;
use pumpkin_data::entity::EntityType;

#[derive(Clone)]
pub(super) struct AttackMaterialization {
    pub id: ActionId,
    pub actor_chunk: ChunkKey,
    pub contact: AttackContact,
    pub pose: AttackPose,
    pub victim_kind: EntityType,
    pub sweep_victims: Vec<super::cluster_combat_directory::CombatClientTarget>,
}

impl AttackMaterialization {
    pub(super) fn captured_resource_usage(&self) -> Result<(usize, usize), ApplyFailure> {
        if self.sweep_victims.len() > pumpkin_cluster::canonical_action::MAX_LOGICAL_RECORDS {
            return Err(ApplyFailure::ResourceLimit);
        }
        let victims = self.sweep_victims.iter().map(|victim|
            (&victim.contact, victim.kind, victim.game_mode, victim.yaw, victim.on_ground, victim.tags.as_ref()))
            .collect::<Vec<_>>();
        let owned = (&self.id, &self.actor_chunk, &self.contact, &self.pose, self.victim_kind.id, victims);
        let records = pumpkin_cluster::logical_records::count(&owned)?;
        let bytes = postcard::experimental::serialized_size(&owned).map_err(|_| ApplyFailure::InvalidAction)?;
        pumpkin_cluster::canonical_action::check_logical_budget(records, bytes)?;
        Ok((records, bytes))
    }
    pub(super) fn output_count(&self) -> Option<u32> {
        Some(1)
    }
}

pub(super) fn output_at(
    view: &impl ActionState,
    mut metadata: AttackMaterialization,
    ids: &[ActionId],
    index: usize,
) -> Result<Action, ApplyFailure> {
    if ids.len() != 1 || index != 0 { return Err(ApplyFailure::InvalidAction); }
    metadata.id = ids[0];
    materialize_attack(view, metadata)
}

pub(super) fn materialize_attack(
    view: &impl ActionState,
    metadata: AttackMaterialization,
) -> Result<Action, ApplyFailure> {
    let source = metadata.contact.attacker.target();
    let target = metadata.contact.victim.target();
    let attacker_kind = match view.read(TruthView::Local, source).map(|cell| cell.value) {
        Some(pumpkin_cluster::canonical_action::Value::Entity(body)) => {
            EntityType::from_raw(body.kind)
                .cloned()
                .ok_or(ApplyFailure::Type(source))?
        }
        Some(pumpkin_cluster::canonical_action::Value::Living(_)) => EntityType::PLAYER,
        _ => return Err(ApplyFailure::Type(source)),
    };
    if !metadata.contact.valid()
        || metadata.id.actor != metadata.contact.attacker.actor()
        || metadata.actor_chunk != metadata.contact.attacker.chunk
    {
        return Err(ApplyFailure::InvalidAction);
    }
    let mut intent = CombatIntent {
        damage_contacts: Vec::new(),
            falls: Vec::new(),
            damage_parent: None,
        environment: Vec::new(),
        exhaustions: Vec::new(),
        ticks: Vec::new(),
        damages: Vec::new(),
        vehicle_attacks: Vec::new(),
        attacks: vec![AttackIntent {
            actor: metadata.id.actor,
            source,
            target,
            tick: metadata.contact.tick,
            contact: metadata.contact.clone(),
            pose: metadata.pose,
            attributes: CombatAttributeIds {
                attack_damage: u16::from(Attributes::ATTACK_DAMAGE.id),
                attack_speed: u16::from(Attributes::ATTACK_SPEED.id),
                armor: u16::from(Attributes::ARMOR.id),
                toughness: u16::from(Attributes::ARMOR_TOUGHNESS.id),
                knockback_resistance: u16::from(Attributes::KNOCKBACK_RESISTANCE.id),
            },
            target_undead: [
                EntityType::ZOMBIE,
                EntityType::DROWNED,
                EntityType::HUSK,
                EntityType::ZOMBIE_VILLAGER,
                EntityType::ZOMBIFIED_PIGLIN,
                EntityType::SKELETON,
                EntityType::BOGGED,
                EntityType::PARCHED,
                EntityType::WITHER_SKELETON,
                EntityType::STRAY,
                EntityType::PHANTOM,
                EntityType::WITHER,
                EntityType::ZOMBIE_HORSE,
                EntityType::SKELETON_HORSE,
            ]
            .contains(&metadata.victim_kind),
            target_arthropod: [
                EntityType::SPIDER,
                EntityType::CAVE_SPIDER,
                EntityType::SILVERFISH,
                EntityType::ENDERMITE,
                EntityType::BEE,
            ]
            .contains(&metadata.victim_kind),
            sweeping: false,
        }],
        attack_clock: matches!(metadata.id.actor, pumpkin_cluster::actor::ActorId::Player(_)).then_some(AttackClockIntent {
            target: source,
            actor: metadata.id.actor,
            tick: metadata.contact.tick,
        }),
        statistics: Vec::new(),
        item_transforms: Vec::new(),
        held_attack: None,
        equipment: Vec::new(),
    };
    if matches!(view.read(TruthView::Local, target).map(|cell| cell.value),
        Some(pumpkin_cluster::canonical_action::Value::Entity(body))
            if matches!(body.payload, pumpkin_cluster::entity_body::EntityPayload::Vehicle(_)))
    {
        let attack = intent.attacks.remove(0);
        intent
            .vehicle_attacks
            .push(pumpkin_cluster::vehicle_attack::VehicleAttackIntent { attack });
        if let pumpkin_cluster::item_contact::ContactIdentity::Player(lease) =
            metadata.contact.attacker.identity
        {
            intent.held_attack = Some(
                pumpkin_cluster::canonical_inventory::InventoryEquipmentBinding::player(
                    lease,
                    metadata.actor_chunk,
                    view.inventory_equipment_attribute_ids(),
                ),
            );
        }
        let values = intent
            .targets()
            .map(|target| {
                view.read(TruthView::Local, target)
                    .map(|cell| (target, cell))
                    .ok_or(ApplyFailure::Condition(target))
            })
            .collect::<Result<BTreeMap<_, _>, _>>()?;
        return intent.capture_with(view, metadata.id, metadata.actor_chunk, &values, Vec::new());
    }
    let primary = intent.attacks[0].clone();
    for victim in &metadata.sweep_victims {
        let mut attack = primary.clone();
        attack.target = victim.contact.target();
        attack.contact.victim = victim.contact.clone();
        attack.contact.victim.simulation_tick = primary.tick;
        attack.contact.victim_yaw = victim.yaw;
        attack.contact.victim_on_ground = victim.on_ground;
        attack.target_undead = false;
        attack.target_arthropod = false;
        attack.sweeping = true;
        if !attack.is_valid() { return Err(ApplyFailure::InvalidAction); }
        intent.attacks.push(attack);
    }
    use pumpkin_cluster::canonical_action::{ChunkPart, Target};
    use pumpkin_cluster::combat_action::{CombatStatisticIntent, CombatStatisticKind};
    use pumpkin_cluster::living_property::LivingProperty;
    use pumpkin_data::statistic::{CustomStatistic, StatisticCategory};
    let mut statistics = BTreeMap::<Target, CombatStatisticIntent>::new();
    let mut record =
        |body: Target, damage_target: Target, category: StatisticCategory, key: u32, kind| {
            let Target::Chunk {
                key: chunk,
                part: ChunkPart::Player(gid),
                inventory: None,
            } = body
            else {
                return;
            };
            let target = Target::Chunk {
                key: chunk,
                part: ChunkPart::LivingProperty {
                    actor: pumpkin_cluster::actor::ActorId::Player(gid),
                    property: LivingProperty::Statistic {
                        category: category as u16,
                        key,
                    },
                },
                inventory: None,
            };
            let entry = statistics
                .entry(target)
                .or_insert_with(|| CombatStatisticIntent {
                    target,
                    damage_targets: Vec::new(),
                    kind,
                });
            if !entry.damage_targets.contains(&damage_target) {
                entry.damage_targets.push(damage_target);
            }
        };
    let mut victims = vec![(target, metadata.victim_kind)];
    victims.extend(metadata.sweep_victims.iter().map(|victim| EntityType::from_raw(victim.kind).cloned().map(|kind| (victim.contact.target(), kind)).ok_or(ApplyFailure::InvalidAction)).collect::<Result<Vec<_>, _>>()?);
    for (victim, kind) in victims {
        if !intent.attacks.iter().any(|attack| attack.target == victim) {
            continue;
        }
        for (body, statistic_kind, result_kind) in [
            (
                source,
                CustomStatistic::DamageDealt,
                CombatStatisticKind::Damage,
            ),
            (
                source,
                CustomStatistic::DamageDealtAbsorbed,
                CombatStatisticKind::Absorbed,
            ),
            (
                victim,
                CustomStatistic::DamageTaken,
                CombatStatisticKind::Damage,
            ),
            (victim, CustomStatistic::DamageBlockedByShield, CombatStatisticKind::Blocked),
            (victim, CustomStatistic::Deaths, CombatStatisticKind::Kill),
            (
                source,
                if kind == EntityType::PLAYER {
                    CustomStatistic::PlayerKills
                } else {
                    CustomStatistic::MobKills
                },
                CombatStatisticKind::Kill,
            ),
        ] {
            record(
                body,
                victim,
                StatisticCategory::Custom,
                statistic_kind as u32,
                result_kind,
            );
        }
        record(
            source,
            victim,
            StatisticCategory::Killed,
            u32::from(kind.id),
            CombatStatisticKind::Kill,
        );
        record(
            victim,
            victim,
            StatisticCategory::KilledBy,
            u32::from(attacker_kind.id),
            CombatStatisticKind::Kill,
        );
    }
    intent.statistics = statistics.into_values().collect();
    let conditions = Vec::new();
    let mut equipment = BTreeMap::new();
    for attack in &intent.attacks {
        if let pumpkin_cluster::item_contact::ContactIdentity::Player(lease) = attack.contact.victim.identity {
            equipment.entry(lease).or_insert_with(|| pumpkin_cluster::canonical_inventory::InventoryEquipmentBinding::player(
                lease, attack.contact.victim.chunk, view.inventory_equipment_attribute_ids(),
            ));
        }
    }
    intent.equipment = equipment.into_values().collect();
    if let pumpkin_cluster::item_contact::ContactIdentity::Player(lease) =
        metadata.contact.attacker.identity
    {
        intent.held_attack = Some(
            pumpkin_cluster::canonical_inventory::InventoryEquipmentBinding::player(
                lease,
                metadata.actor_chunk,
                view.inventory_equipment_attribute_ids(),
            ),
        );
    }
    let mut values = BTreeMap::new();
    for target in intent.targets().collect::<std::collections::BTreeSet<_>>() {
        values.insert(
            target,
            view.read(TruthView::Local, target)
                .ok_or(ApplyFailure::Condition(target))?,
        );
    }
    intent.capture_with(view, metadata.id, metadata.actor_chunk, &values, conditions)
}

pub(super) fn capture_proposal(
    state: &super::cluster_world_canonical::OwnedWorldActionState<'_>,
    original: &super::cluster_world_callback_snapshot::CallbackTickSnapshot,
    view: &impl ActionState,
    proposal: &pumpkin_cluster::canonical_action::ActionProposal,
    tick: pumpkin_cluster::time::TickStamp,
) -> Result<AttackMaterialization, ApplyFailure> {
    capture_proposal_with_sweep(state, original, view, proposal, tick, true)
}

fn capture_proposal_with_sweep(
    state: &super::cluster_world_canonical::OwnedWorldActionState<'_>,
    original: &super::cluster_world_callback_snapshot::CallbackTickSnapshot,
    view: &impl ActionState,
    proposal: &pumpkin_cluster::canonical_action::ActionProposal,
    tick: pumpkin_cluster::time::TickStamp, collect_sweep: bool,
) -> Result<AttackMaterialization, ApplyFailure> {
    use pumpkin_cluster::actor::ActorId;
    use pumpkin_cluster::canonical_action::{ChunkPart, ClaimedAction, Target, Value};

    let ClaimedAction::Combat(claim) = &proposal.claim else {
        return Err(ApplyFailure::InvalidAction);
    };
    let lease = proposal.lease.ok_or(ApplyFailure::InvalidAction)?;
    if lease.gid != claim.attacker
        || proposal.id.actor != ActorId::Player(lease.gid)
        || proposal.id.incarnation != lease.incarnation
        || proposal.id.family != 4
        || !original.validate_live_player_lease(lease)
        || original.player_install_generation(lease) != Some(claim.install_generation)
        || claim.target_observation.actor() != claim.target
        || claim.target_observation.chunk != claim.target_chunk
        || !claim.target_observation.valid()
        || claim.target_observation.simulation_tick.is_after(tick)
    {
        return Err(ApplyFailure::InvalidAction);
    }
    let motion = original.original_motion(&pumpkin_cluster::item_contact::ContactIdentity::Player(lease), tick)?;
    let spatial = original.player_projection(lease.gid).ok_or(ApplyFailure::InvalidAction)?;
    let association = motion.contact.chunk;
    let source_target = Target::Chunk {
        key: association,
        part: ChunkPart::Player(lease.gid),
        inventory: None,
    };
    let source = view
        .read(TruthView::Local, source_target)
        .ok_or(ApplyFailure::Condition(source_target))?;
    let Value::Living(living) = &source.value else {
        return Err(ApplyFailure::Type(source_target));
    };
    let avatar = living
        .avatar
        .as_ref()
        .ok_or(ApplyFailure::Type(source_target))?;
    if living.combat.health_milli == 0 || avatar.game_mode == 3 {
        return Err(ApplyFailure::InvalidAction);
    }
    let pose = if avatar.presentation.sleeping.is_some() {
        pumpkin_data::entity::EntityPose::Sleeping
    } else if spatial.gliding {
        pumpkin_data::entity::EntityPose::FallFlying
    } else if spatial.crawling || spatial.swimming {
        pumpkin_data::entity::EntityPose::Swimming
    } else if spatial.sneaking {
        pumpkin_data::entity::EntityPose::Crouching
    } else {
        pumpkin_data::entity::EntityPose::Standing
    };
    let dimensions = super::cluster_living_metadata::player_dimensions(living, pose);
    let contact = motion.contact;
    let issuer = contact.owner;
    let reach_id = u16::from(Attributes::ENTITY_INTERACTION_RANGE.id);
    let reach_base = living
        .combat
        .effective_attributes
        .get(&reach_id)
        .copied()
        .map(f64::from_bits)
        .ok_or(ApplyFailure::InvalidAction)?;
    let reach = living.attribute(reach_id, reach_base);
    let victim_target = original.combat_target(&claim.target_observation.identity)?;
    if victim_target.kind != claim.target_kind { return Err(ApplyFailure::InvalidAction); }
    let victim = victim_target.contact.clone();
    let victim_kind = EntityType::from_raw(victim_target.kind).ok_or(ApplyFailure::InvalidAction)?;
    let victim_position = victim.position.map(f64::from_bits);
    let original_targets = if collect_sweep { original.combat_targets(victim.world,
        [[victim_position[0] - 1.0, victim_position[2] - 1.0],
         [victim_position[0] + 1.0, victim_position[2] + 1.0]])? } else { Vec::new() };
    let mut sweep_victims = original_targets.iter()
        .filter(|target| {
            target.contact.actor() != proposal.id.actor
                && target.contact.actor() != claim.target
                && target.contact.world == victim.world
                && target.game_mode.is_none_or(|mode| {
                    mode != 3 && (mode != 1 || !state.server.advanced_config.pvp.protect_creative)
                })
                && {
                    let mut sweep = target.contact.clone();
                    sweep.simulation_tick = tick;
                    AttackContact {attacker: contact.clone(), victim: sweep,
                        reach: reach.to_bits(), eye_height: f64::from(dimensions.eye_height).to_bits(),
                        victim_yaw: target.yaw, victim_on_ground: target.on_ground,
                        producer: issuer, tick}.valid()
                }
                && target
                    .contact
                    .position
                    .map(f64::from_bits)
                    .iter()
                    .zip(victim_position)
                    .take(3)
                    .enumerate()
                    .all(|(axis, (position, victim))| {
                        (*position - victim).abs()
                            <= if axis == 1 {
                                1.0
                            } else {
                                1.0 + f64::from_bits(target.contact.half_width)
                            }
                    })
                && view
                    .read(TruthView::Local, target.contact.target())
                    .is_some_and(|cell| {
                        pumpkin_cluster::living_property::state(&cell.value).is_some()
                    })
        })
        .cloned()
        .collect::<Vec<_>>();
    sweep_victims.sort_by_key(|target| target.contact.actor());
    Ok(AttackMaterialization {
        id: proposal.id,
        actor_chunk: association,
        contact: AttackContact {
            attacker: contact,
            victim,
            reach: reach.to_bits(),
            eye_height: f64::from(dimensions.eye_height).to_bits(),
            victim_yaw: victim_target.yaw,
            victim_on_ground: victim_target.on_ground,
            producer: issuer,
            tick,
        },
        pose: AttackPose {
            sprinting: spatial.sprinting,
            on_ground: spatial.frame.effective.on_ground,
            fall_distance: avatar.fall_distance,
            bedrock: claim.bedrock,
            yaw: spatial.frame.effective.yaw.to_bits(),
            knockback_enabled: true,
        },
        victim_kind: victim_kind.clone(),
        sweep_victims,
    })
}

pub(super) fn materialize_proposal(
    state: &super::cluster_world_canonical::OwnedWorldActionState<'_>,
    original: &super::cluster_world_callback_snapshot::CallbackTickSnapshot,
    view: &impl ActionState,
    proposal: &pumpkin_cluster::canonical_action::ActionProposal,
    tick: pumpkin_cluster::time::TickStamp,
) -> Result<Action, ApplyFailure> {
    materialize_attack(view, capture_proposal(state, original, view, proposal, tick)?)
}

pub(super) fn capture_command(
    state: &super::cluster_world_canonical::OwnedWorldActionState<'_>,
    original: &super::cluster_world_callback_snapshot::CallbackTickSnapshot,
    view: &impl ActionState,
    proposal: &pumpkin_cluster::canonical_action::ActionProposal,
    tick: pumpkin_cluster::time::TickStamp,
) -> Result<pumpkin_cluster::combat_command::MeleeAttackCommand, ApplyFailure> {
    let metadata = capture_proposal_with_sweep(state, original, view, proposal, tick, false)?;
    let source = capture_motion(original, &metadata.contact.attacker.identity, tick)?;
    let primary = capture_motion(original, &metadata.contact.victim.identity, tick)?;
    let position = primary.position.map(f64::from_bits);
    let mut sweep = original.combat_targets(primary.actor.chunk().world,
        [[position[0] - 1.0, position[2] - 1.0], [position[0] + 1.0, position[2] + 1.0]])?
        .into_iter().filter(|candidate| candidate.contact.actor() != source.id() && candidate.contact.actor() != primary.id())
        .filter(|candidate| candidate.contact.position.map(f64::from_bits).into_iter().zip(position).enumerate()
            .all(|(axis, (coordinate, center))| (coordinate - center).abs() <= if axis == 1 { 1.0 } else { 1.0 + f64::from_bits(candidate.contact.half_width) }))
        .filter_map(|candidate| {
            let target = candidate.contact.target();
            match original.read(TruthView::Local, target) {
                Some(cell) if pumpkin_cluster::living_property::state(&cell.value).is_some() => Some(capture_motion(original, &candidate.contact.identity, tick)),
                Some(_) => None,
                None => Some(Err(ApplyFailure::MissingStakeholder(target.stakeholder()))),
            }
        }).collect::<Result<Vec<_>, _>>()?;
    sweep.sort_by_key(|candidate| candidate.id());
    let command = pumpkin_cluster::combat_command::MeleeAttackCommand { source, primary, sweep,
        protocol: if metadata.pose.bedrock { pumpkin_cluster::combat_command::MeleeProtocol::Bedrock }
            else { pumpkin_cluster::combat_command::MeleeProtocol::Java } };
    let records = pumpkin_cluster::logical_records::count(&command)?;
    let bytes = postcard::experimental::serialized_size(&command).map_err(|_| ApplyFailure::InvalidAction)?;
    pumpkin_cluster::canonical_action::check_logical_budget(records, bytes)?;
    Ok(command)
}

pub(super) fn capture_motion(
    original: &super::cluster_world_callback_snapshot::CallbackTickSnapshot,
    identity: &pumpkin_cluster::item_contact::ContactIdentity,
    tick: pumpkin_cluster::time::TickStamp,
) -> Result<pumpkin_cluster::combat_command::ActorObservation, ApplyFailure> {
    use pumpkin_cluster::action_command::ActorReference;
    use pumpkin_cluster::combat_command::{ActorObservation, VelocityCut};
    use pumpkin_cluster::item_contact::ContactIdentity;
    let sample = original.original_motion(identity, tick)?;
    let target = sample.contact.target();
    let actor = match identity {
        ContactIdentity::Player(lease) => ActorReference::Player { lease: *lease,
            chunk: sample.contact.chunk, epoch: sample.contact.epoch, owner: sample.contact.owner },
        ContactIdentity::Entity { id, birth } => ActorReference::Entity { id: *id, birth: birth.clone(),
            chunk: sample.contact.chunk, epoch: sample.contact.epoch, owner: sample.contact.owner },
    };
    let super::cluster_world_callback_snapshot::OriginalReliableVelocity::Living(reliable) = sample.canonical_velocity else {
        return Err(ApplyFailure::Type(target));
    };
    let presentation = original.combat_target(identity)?;
    Ok(ActorObservation { actor, position: sample.contact.position, yaw: presentation.yaw,
        on_ground: presentation.on_ground, sequence: sample.contact.observed_sequence,
        velocity: VelocityCut { reliable, effective: sample.velocity_micro } })
}

pub(super) struct OriginalMeleeRegistry<'a> {
    pub original: &'a super::cluster_world_callback_snapshot::CallbackTickSnapshot,
}

impl OriginalMeleeRegistry<'_> {
    fn mob_mount_parent(&self, source: &pumpkin_cluster::action_command::ActorReference,
        values: &std::collections::BTreeMap<pumpkin_cluster::canonical_action::Target, pumpkin_cluster::canonical_action::Cell>)
        -> Result<Option<pumpkin_cluster::action_command::ActorReference>, ApplyFailure> {
        use pumpkin_cluster::canonical_action::{ChunkPart, Target, Value};
        use pumpkin_cluster::actor_relation::{self, ActorRelation};
        let target = Target::Chunk { key: source.chunk(), part: ChunkPart::ActorRelation { actor: source.actor(), relation: ActorRelation::Mount }, inventory: None };
        let cell = values.get(&target).cloned().or_else(|| values.get(&source.body_target()).and_then(|body| actor_relation::cell(body, ActorRelation::Mount)))
            .ok_or(ApplyFailure::Condition(target))?;
        let identity = match cell.value { Value::ActorRelation(None) => return Ok(None),
            Value::ActorRelation(Some(identity)) => identity, _ => return Err(ApplyFailure::Type(target)) };
        let pumpkin_cluster::item_contact::ContactIdentity::Entity { id, birth } = identity else { return Err(ApplyFailure::InvalidAction); };
        let chunk = self.original.original_store().entity_body_chunk(false, id).ok_or(ApplyFailure::MissingStakeholder(target.stakeholder()))?;
        let cell = self.original.original_store().entity_body(false, chunk, id).ok_or(ApplyFailure::Condition(target))?;
        let Value::Entity(body) = &cell.value else { return Err(ApplyFailure::Type(target)); };
        if body.birth != birth || chunk.world != source.chunk().world || source.actor() == pumpkin_cluster::actor::ActorId::Entity(id) {
            return Err(ApplyFailure::InvalidAction);
        }
        Ok(Some(pumpkin_cluster::action_command::ActorReference::Entity { id, birth, chunk, epoch: body.spatial_epoch, owner: body.owner }))
    }

    fn kind(&self, actor: &pumpkin_cluster::action_command::ActorReference) -> Result<EntityType, ApplyFailure> {
        use pumpkin_cluster::action_command::ActorReference;
        match actor {
            ActorReference::Player { lease, chunk, epoch, owner } => {
                let participant = self.original.player_relation_participant(*lease)
                    .ok_or(ApplyFailure::Condition(actor.body_target()))?;
                if participant.chunk != *chunk || participant.epoch != *epoch || participant.owner != *owner {
                    return Err(ApplyFailure::Condition(actor.body_target()));
                }
                Ok(EntityType::PLAYER)
            }
            ActorReference::Entity { id, birth, chunk, epoch, owner } => {
                let Some(pumpkin_cluster::canonical_action::Cell { value: pumpkin_cluster::canonical_action::Value::Entity(body), .. }) =
                    self.original.original_store().entity_body(false, *chunk, *id) else {
                    return Err(ApplyFailure::Condition(actor.body_target()));
                };
                if body.birth != *birth || body.spatial_epoch != *epoch || body.owner != *owner {
                    return Err(ApplyFailure::Condition(actor.body_target()));
                }
                EntityType::from_raw(body.kind).cloned().ok_or(ApplyFailure::Type(actor.body_target()))
            }
        }
    }
}

impl pumpkin_cluster::combat_command::MeleeRegistry for OriginalMeleeRegistry<'_> {
    fn mob_mount_targets(&self, source: &pumpkin_cluster::action_command::ActorReference,
        values: &std::collections::BTreeMap<pumpkin_cluster::canonical_action::Target, pumpkin_cluster::canonical_action::Cell>)
        -> Result<std::collections::BTreeSet<pumpkin_cluster::canonical_action::Target>, ApplyFailure> {
        use pumpkin_cluster::actor_relation::ActorRelation;
        use pumpkin_cluster::canonical_action::{ChunkPart, Target};
        let mount = Target::Chunk { key: source.chunk(), part: ChunkPart::ActorRelation {
            actor: source.actor(), relation: ActorRelation::Mount }, inventory: None };
        let mut targets = std::collections::BTreeSet::from([mount]);
        if let Some(parent) = self.mob_mount_parent(source, values)? {
            targets.insert(parent.body_target());
            for seat in 0..32 {
                targets.insert(Target::Chunk { key: parent.chunk(), part: ChunkPart::ActorRelation {
                    actor: parent.actor(), relation: ActorRelation::Passenger(seat) }, inventory: None });
            }
            let cell = self.original.original_store().entity_body(false, parent.chunk(), match parent {
                pumpkin_cluster::action_command::ActorReference::Entity { id, .. } => id,
                _ => return Err(ApplyFailure::InvalidAction),
            }).ok_or(ApplyFailure::Condition(parent.body_target()))?;
            if let pumpkin_cluster::canonical_action::Value::Entity(body) = &cell.value {
                if let pumpkin_cluster::entity_body::EntityPayload::Living(living) = &body.payload {
                    for property in self.geometry_properties(&parent, living) {
                        targets.insert(Target::Chunk { key: parent.chunk(), part: ChunkPart::LivingProperty {
                            actor: parent.actor(), property }, inventory: None });
                        if let pumpkin_cluster::living_property::LivingProperty::Attribute(id) = property {
                            targets.insert(Target::Chunk { key: parent.chunk(), part: ChunkPart::LivingProperty {
                                actor: parent.actor(), property: pumpkin_cluster::living_property::LivingProperty::PermanentAttributeModifiers(id) }, inventory: None });
                        }
                    }
                }
            }
        }
        Ok(targets)
    }
    fn mob_mount_shape(&self, source: &pumpkin_cluster::action_command::ActorReference,
        values: &std::collections::BTreeMap<pumpkin_cluster::canonical_action::Target, pumpkin_cluster::canonical_action::Cell>)
        -> Result<Option<([u64; 3], pumpkin_cluster::actor_occupancy::CollisionShape)>, ApplyFailure> {
        use pumpkin_cluster::canonical_action::{ActionState, ChunkPart, Target, Value};
        use pumpkin_cluster::actor_relation::ActorRelation;
        let Some(parent) = self.mob_mount_parent(source, values)? else { return Ok(None); };
        let target = parent.body_target();
        let mut cell = values.get(&target).cloned().ok_or(ApplyFailure::Condition(target))?;
        let Value::Entity(body) = &mut cell.value else { return Err(ApplyFailure::Type(target)); };
        let pumpkin_cluster::action_command::ActorReference::Entity { id, birth, epoch, owner, chunk } = &parent else { return Err(ApplyFailure::InvalidAction); };
        if body.id != *id || body.birth != *birth || body.spatial_epoch != *epoch || body.owner != *owner || !body.belongs_to(*id, *chunk) {
            return Err(ApplyFailure::Condition(target));
        }
        let source_identity = match source {
            pumpkin_cluster::action_command::ActorReference::Player { lease, .. } => pumpkin_cluster::item_contact::ContactIdentity::Player(*lease),
            pumpkin_cluster::action_command::ActorReference::Entity { id, birth, .. } => pumpkin_cluster::item_contact::ContactIdentity::Entity { id: *id, birth: birth.clone() },
        };
        let mut count = 0;
        for seat in 0..32 {
            let relation_target = Target::Chunk { key: *chunk, part: ChunkPart::ActorRelation { actor: parent.actor(), relation: ActorRelation::Passenger(seat) }, inventory: None };
            let relation = values.get(&relation_target).cloned().or_else(|| pumpkin_cluster::actor_relation::cell(&cell, ActorRelation::Passenger(seat)))
                .ok_or(ApplyFailure::Condition(relation_target))?;
            match relation.value { Value::ActorRelation(Some(identity)) if identity == source_identity => count += 1,
                Value::ActorRelation(_) => {}, _ => return Err(ApplyFailure::Type(relation_target)) }
        }
        if count != 1 { return Err(ApplyFailure::Condition(target)); }
        let Value::Entity(body) = &mut cell.value else { unreachable!() };
        if let pumpkin_cluster::entity_body::EntityPayload::Living(living) = &mut body.payload {
            let mut properties = self.geometry_properties(&parent, living);
            let modifiers: Vec<_> = properties.iter().filter_map(|property| match property {
                pumpkin_cluster::living_property::LivingProperty::Attribute(id) => Some(pumpkin_cluster::living_property::LivingProperty::PermanentAttributeModifiers(*id)), _ => None }).collect();
            properties.extend(modifiers);
            for property in properties {
                let property_target = Target::Chunk { key: *chunk, part: ChunkPart::LivingProperty { actor: parent.actor(), property }, inventory: None };
                let property_cell = values.get(&property_target).ok_or(ApplyFailure::Condition(property_target))?;
                let Value::LivingProperty(value) = &property_cell.value else { return Err(ApplyFailure::Type(property_target)); };
                if !property.hydrate(living, value) { return Err(ApplyFailure::Type(property_target)); }
            }
        }
        let (actual_owner, cut) = self.original.actor_occupancy_cut(target, &cell.value)?;
        if actual_owner != *owner { return Err(ApplyFailure::Condition(target)); }
        Ok(Some((cut.position, self.original.actor_collision_shape(target, &cell.value)?)))
    }

    fn attributes(&self) -> CombatAttributeIds {
        CombatAttributeIds { attack_damage: u16::from(Attributes::ATTACK_DAMAGE.id),
            attack_speed: u16::from(Attributes::ATTACK_SPEED.id), armor: u16::from(Attributes::ARMOR.id),
            toughness: u16::from(Attributes::ARMOR_TOUGHNESS.id),
            knockback_resistance: u16::from(Attributes::KNOCKBACK_RESISTANCE.id) }
    }
    fn range_attribute(&self) -> u16 { u16::from(Attributes::ENTITY_INTERACTION_RANGE.id) }
    fn scale_attribute(&self) -> u16 { u16::from(Attributes::SCALE.id) }
    fn geometry(&self, actor: &pumpkin_cluster::action_command::ActorReference,
        living: &pumpkin_cluster::item_action::ActorLivingState)
        -> Result<pumpkin_cluster::combat_command::ActorGeometry, ApplyFailure> {
        let kind = self.kind(actor)?;
        let size = if kind == EntityType::PLAYER {
            super::cluster_living_metadata::player_dimensions(living, super::cluster_living_metadata::player_pose(living))
        } else {
            super::cluster_owned_mob_metadata::dimensions(living, kind.id).ok_or(ApplyFailure::Type(actor.body_target()))?
        };
        Ok(pumpkin_cluster::combat_command::ActorGeometry {
            half_width: (f64::from(size.width) * 0.5).to_bits(), height: f64::from(size.height).to_bits(),
            eye_height: f64::from(size.eye_height).to_bits(),
        })
    }
    fn species(&self, actor: &pumpkin_cluster::action_command::ActorReference,
        _: &pumpkin_cluster::item_action::ActorLivingState) -> Result<[bool; 2], ApplyFailure> {
        let kind = self.kind(actor)?;
        Ok([[EntityType::ZOMBIE, EntityType::DROWNED, EntityType::HUSK, EntityType::ZOMBIE_VILLAGER,
            EntityType::ZOMBIFIED_PIGLIN, EntityType::SKELETON, EntityType::BOGGED, EntityType::PARCHED,
            EntityType::WITHER_SKELETON, EntityType::STRAY, EntityType::PHANTOM, EntityType::WITHER,
            EntityType::ZOMBIE_HORSE, EntityType::SKELETON_HORSE].contains(&kind),
            [EntityType::SPIDER, EntityType::CAVE_SPIDER, EntityType::SILVERFISH, EntityType::ENDERMITE,
                EntityType::BEE].contains(&kind)])
    }
}
