use pumpkin_data::tag::Taggable;
use std::collections::{BTreeMap, HashMap};

use pumpkin_cluster::actor::ActorId;
use pumpkin_cluster::actor_action_scope::{ActorActionDraft, FrozenEntityIssueAuthority};
use pumpkin_cluster::canonical_action::{
    Action, ActionState, LivePlayerLease, SpatialEpoch, TruthView, Value,
};
use pumpkin_cluster::combat_action::{CombatIntent, LivingTickIntent};
use pumpkin_cluster::entity_body::{EntityBody, EntityPayload};
use pumpkin_cluster::item_contact::ContactObservation;
use pumpkin_cluster::protocol::ChunkKey;
use pumpkin_cluster::realm_completion::PeerGeneration;
use pumpkin_cluster::time::TickStamp;

#[derive(Clone)]
pub(super) struct FrozenLivingTickJob {
    identity: pumpkin_cluster::combat_action::LivingTickIdentity,
    contact: Option<ContactObservation>,
    hazard_resource_limit: bool,
    incarnation: u64,
    effective_entity_spatial: Option<pumpkin_cluster::entity_body::EntitySpatial>,
    death_inventory: Option<FrozenDeathInventory>,
    world_policy: Option<pumpkin_cluster::world_rules::WorldRulesSnapshot>,
    timestamp: u64,
    intent: CombatIntent,
    environment: CombatIntent,
    required_chunks: Vec<ChunkKey>,
    required_sections: Vec<pumpkin_cluster::canonical_action::Target>,
    pin: Option<super::cluster_actor_chunk_pin::ActorChunkPin>,
    snapshot: std::sync::Arc<super::cluster_world_callback_snapshot::CallbackTickSnapshot>,
    original_world: std::sync::Arc<super::cluster_chunk_state::OwnedChunkStore>,
    observations: std::sync::Arc<super::cluster_owned_mob_contacts::MobContactIndex>,
    entity_spatials: std::sync::Arc<BTreeMap<ActorId, pumpkin_cluster::entity_body::EntitySpatial>>,
    draft: Option<ActorActionDraft>,
}

impl FrozenLivingTickJob {
    pub(super) fn timestamp(&self) -> u64 {
        self.timestamp
    }
    pub(super) fn actor(&self) -> ActorId {
        self.identity.actor()
    }
    pub(super) fn hazard_resource_limit(&self) -> bool {
        self.hazard_resource_limit
    }
    pub(super) fn contact(&self) -> Option<&ContactObservation> {
        self.contact.as_ref()
    }
    pub(super) fn entity_spatials(
        &self,
    ) -> &BTreeMap<ActorId, pumpkin_cluster::entity_body::EntitySpatial> {
        &self.entity_spatials
    }
    pub(super) fn observations(&self) -> &[ContactObservation] {
        self.observations.observations()
    }
    pub(super) fn effective_entity_spatial(
        &self,
    ) -> Option<&pumpkin_cluster::entity_body::EntitySpatial> {
        self.effective_entity_spatial.as_ref()
    }
    pub(super) fn incarnation(&self) -> u64 {
        self.incarnation
    }
    pub(super) fn original_world(
        &self,
    ) -> &std::sync::Arc<super::cluster_chunk_state::OwnedChunkStore> {
        &self.original_world
    }
    pub(super) fn snapshot(
        &self,
    ) -> &std::sync::Arc<super::cluster_world_callback_snapshot::CallbackTickSnapshot> {
        &self.snapshot
    }
}

#[derive(Clone)]
pub(super) struct FrozenLivingCaptureInputs {
    snapshot: std::sync::Arc<super::cluster_world_callback_snapshot::CallbackTickSnapshot>,
    eligible_entities: std::collections::BTreeSet<pumpkin_cluster::protocol::GlobalEntityId>,
    contacts: Vec<(ContactObservation, u64)>,
    actors: std::collections::BTreeSet<(ActorId, u64)>,
    drafts: HashMap<(ActorId, u64, TickStamp), ActorActionDraft>,
    pins: HashMap<(ActorId, u64), super::cluster_actor_chunk_pin::ActorChunkPin>,
    executors: BTreeMap<ChunkKey, PeerGeneration>,
    timestamp: u64,
    producer: PeerGeneration,
    pub(super) tick: TickStamp,
}

impl FrozenLivingCaptureInputs {
    pub(super) fn new(
        snapshot: std::sync::Arc<super::cluster_world_callback_snapshot::CallbackTickSnapshot>,
        bodies: &[(ChunkKey, EntityBody)],
        mut contacts: Vec<(ContactObservation, u64)>,
        producer: PeerGeneration,
        tick: TickStamp,
        drafts: &HashMap<(ActorId, u64, TickStamp), ActorActionDraft>,
        pins: &HashMap<(ActorId, u64), super::cluster_actor_chunk_pin::ActorChunkPin>,
        executors: &BTreeMap<ChunkKey, PeerGeneration>,
    ) -> Result<Self, pumpkin_cluster::canonical_action::ApplyFailure> {
        use pumpkin_cluster::canonical_action::ApplyFailure;
        if snapshot.original_tick() != tick {
            return Err(ApplyFailure::InvalidAction);
        }
        contacts.retain(|(contact, _)| snapshot.world_rules_at(contact.world, tick).is_some());
        let eligible_entities = bodies
            .iter()
            .filter(|(chunk, body)| {
                body.owner == producer
                    && matches!(body.payload, EntityPayload::Living(_))
                    && snapshot.world_rules_at(chunk.world, tick).is_some()
            })
            .map(|(_, body)| body.id)
            .collect::<std::collections::BTreeSet<_>>();
        let mut actors = contacts
            .iter()
            .map(|(contact, incarnation)| (contact.actor(), *incarnation))
            .collect::<std::collections::BTreeSet<_>>();
        for (_, cell) in eligible_entity_cells(snapshot.original_store().as_ref(), &eligible_entities) {
            let Value::Entity(body) = &cell.value else {
                continue;
            };
            if body.owner != producer || !eligible_entities.contains(&body.id) {
                continue;
            }
            let EntityPayload::Living(living) = &body.payload else {
                continue;
            };
            let dimensions = super::cluster_owned_mob_metadata::dimensions(living, body.kind)
                .ok_or(ApplyFailure::InvalidAction)?;
            if dimensions.width == 0.0
                || dimensions.height == 0.0
                || super::cluster_living_environment::geometry_resource_limit(
                    snapshot.entity_effective_spatial(body.id).ok_or(ApplyFailure::InvalidAction)?.position,
                    dimensions.width, dimensions.height)
            {
                actors.insert((ActorId::Entity(body.id), body.id.generation));
            }
        }
        let timestamp = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .map_err(|_| ApplyFailure::InvalidAction)?
            .as_millis()
            .try_into()
            .map_err(|_| ApplyFailure::InvalidAction)?;
        let value = Self {
            snapshot,
            eligible_entities,
            contacts,
            drafts: drafts
                .iter()
                .filter(|((actor, incarnation, source_tick), _)| {
                    *source_tick == tick && actors.contains(&(*actor, *incarnation))
                })
                .map(|(key, draft)| (*key, draft.clone()))
                .collect(),
            pins: pins
                .iter()
                .filter(|(actor, _)| actors.contains(actor))
                .map(|(key, pin)| (*key, pin.clone()))
                .collect(),
            actors,
            executors: executors.clone(),
            timestamp,
            producer,
            tick,
        };
        value.resource_usage()?;
        Ok(value)
    }
    pub(super) fn actors(&self) -> impl Iterator<Item = (ActorId, u64)> + '_ {
        self.actors.iter().copied()
    }
    pub(super) fn resource_usage(
        &self,
    ) -> Result<(usize, usize), pumpkin_cluster::canonical_action::ApplyFailure> {
        use pumpkin_cluster::canonical_action::ApplyFailure;
        let payload = (
            &self.eligible_entities,
            &self.contacts,
            &self.actors,
            &self.executors,
            self.timestamp,
            self.producer,
            self.tick,
        );
        let mut records = pumpkin_cluster::logical_records::count(&payload)?;
        let mut bytes = postcard::experimental::serialized_size(&payload)
            .map_err(|_| ApplyFailure::InvalidAction)?;
        for (key, draft) in &self.drafts {
            let (count, size) = draft.resource_usage()?;
            records = records
                .checked_add(count)
                .and_then(|count| count.checked_add(1))
                .ok_or(ApplyFailure::ResourceLimit)?;
            bytes = bytes
                .checked_add(size)
                .and_then(|size| size.checked_add(std::mem::size_of_val(key)))
                .ok_or(ApplyFailure::ResourceLimit)?;
        }
        records = records
            .checked_add(self.pins.len())
            .ok_or(ApplyFailure::ResourceLimit)?;
        bytes = bytes
            .checked_add(
                self.pins
                    .len()
                    .checked_mul(std::mem::size_of::<(
                        (ActorId, u64),
                        super::cluster_actor_chunk_pin::ActorChunkPin,
                    )>())
                    .ok_or(ApplyFailure::ResourceLimit)?,
            )
            .ok_or(ApplyFailure::ResourceLimit)?;
        pumpkin_cluster::canonical_action::check_logical_budget(records, bytes)?;
        Ok((records, bytes))
    }
    pub(super) fn capture(
        &self,
        owners: &super::cluster_world_canonical::OwnedWorldActionState<'_>,
    ) -> Result<
        pumpkin_cluster::actor_tick_frame::FrozenActorTickFrame<FrozenLivingTickJob>,
        pumpkin_cluster::canonical_action::ApplyFailure,
    > {
        capture_frame_from_snapshot(
            self.snapshot.clone(),
            owners,
            &self.eligible_entities,
            self.contacts.clone(),
            self.producer,
            self.tick,
            &self.drafts,
            &self.pins,
            &self.executors,
            self.timestamp,
        )
    }
}

pub(super) fn capture_contacts(
    snapshot: &super::cluster_world_callback_snapshot::CallbackTickSnapshot,
    players: &[(
        LivePlayerLease,
        super::cluster_world_actor::ActorSpatialState,
        ChunkKey,
        SpatialEpoch,
    )],
    bodies: &[(ChunkKey, EntityBody)],
    producer: PeerGeneration,
    tick: TickStamp,
) -> Result<Vec<(ContactObservation, u64)>, pumpkin_cluster::canonical_action::ApplyFailure> {
    use pumpkin_cluster::canonical_action::ApplyFailure;
    if snapshot.original_tick() != tick {
        return Err(ApplyFailure::InvalidAction);
    }
    let eligible_players = players
        .iter()
        .map(|(lease, _, _, _)| *lease)
        .collect::<std::collections::HashSet<_>>();
    let eligible_entities = bodies
        .iter()
        .map(|(_, body)| body.id)
        .collect::<std::collections::BTreeSet<_>>();
    let mut contacts = snapshot
        .player_contacts(producer, tick)?
        .into_iter()
        .filter_map(|contact| {
            let pumpkin_cluster::item_contact::ContactIdentity::Player(lease) = contact.identity
            else {
                return None;
            };
            (contact.owner == producer && eligible_players.contains(&lease))
                .then_some((contact, lease.incarnation))
        })
        .collect::<Vec<_>>();
    for (chunk, cell) in eligible_entity_cells(snapshot.original_store().as_ref(), &eligible_entities) {
        let Value::Entity(body) = &cell.value else {
            continue;
        };
        if body.owner != producer || !eligible_entities.contains(&body.id) {
            continue;
        }
        let EntityPayload::Living(living) = &body.payload else {
            continue;
        };
        let Some(dimensions) = super::cluster_owned_mob_metadata::dimensions(living, body.kind)
        else {
            continue;
        };
        let effective = snapshot
            .entity_effective_spatial(body.id)
            .ok_or(ApplyFailure::InvalidAction)?;
        if super::cluster_living_environment::geometry_resource_limit(
            effective.position, dimensions.width, dimensions.height) { continue; }
        if let Some(contact) = ContactObservation::entity(
            body,
            chunk,
            effective,
            f64::from(dimensions.width) * 0.5,
            f64::from(dimensions.height),
            tick,
            pumpkin_cluster::identity::PlayerSeq(0),
        ) {
            contacts.push((contact, body.id.generation));
        }
    }
    contacts.sort_by_key(|(contact, _)| contact.actor());
    Ok(contacts)
}

pub(super) fn prepare_contacts(
    server: &super::Server,
    blocks: &super::cluster_chunk_state::OwnedChunkStore,
    contacts: Vec<(ContactObservation, u64)>,
    allocator: &mut super::cluster_actor_chunk_pin::ActorChunkPins,
    retained: &mut HashMap<(ActorId, u64), super::cluster_actor_chunk_pin::ActorChunkPin>,
) -> Vec<(ContactObservation, u64)> {
    let active = contacts
        .iter()
        .map(|(contact, incarnation)| (contact.actor(), *incarnation))
        .collect::<std::collections::HashSet<_>>();
    retained.retain(|actor, _| active.contains(actor));
    contacts
        .into_iter()
        .filter(|(contact, incarnation)| {
            let required = simulation_chunks(contact);
            let key = (contact.actor(), *incarnation);
            if retained
                .get(&key)
                .is_none_or(|pin| pin.chunks() != required.as_slice())
            {
                let Ok(pin) = allocator.acquire(server, &required) else {
                    return false;
                };
                retained.insert(key, pin);
            }
            retained.get(&key).is_some_and(|pin| pin.available(blocks))
        })
        .collect()
}

pub(super) fn capture_frame_from_snapshot(
    original_snapshot: std::sync::Arc<super::cluster_world_callback_snapshot::CallbackTickSnapshot>,
    owners: &super::cluster_world_canonical::OwnedWorldActionState<'_>,
    eligible_entities: &std::collections::BTreeSet<pumpkin_cluster::protocol::GlobalEntityId>,
    contacts: Vec<(ContactObservation, u64)>,
    producer: PeerGeneration,
    tick: TickStamp,
    drafts: &HashMap<(ActorId, u64, TickStamp), ActorActionDraft>,
    simulation_pins: &HashMap<(ActorId, u64), super::cluster_actor_chunk_pin::ActorChunkPin>,
    executors: &BTreeMap<ChunkKey, PeerGeneration>,
    timestamp: u64,
) -> Result<
    pumpkin_cluster::actor_tick_frame::FrozenActorTickFrame<FrozenLivingTickJob>,
    pumpkin_cluster::canonical_action::ApplyFailure,
> {
    use pumpkin_cluster::canonical_action::{ApplyFailure, ChunkPart, Target};
    use pumpkin_cluster::combat_action::{
        CombatStatisticIntent, CombatStatisticKind, LivingTickRules,
    };
    use pumpkin_cluster::living_property::LivingProperty;
    let mut jobs = Vec::with_capacity(contacts.len());
    if original_snapshot.original_tick() != tick {
        return Err(ApplyFailure::InvalidAction);
    }
    let original_snapshot = original_snapshot.with_scheduled_executors(executors);
    let original_world = original_snapshot.original_store().clone();
    let mut observations = original_snapshot.player_contacts(producer, tick)?;
    let mut entity_spatials = BTreeMap::new();
    let mut entity_participants = Vec::new();
    for (chunk, cell) in original_world.local_entity_bodies() {
        let Value::Entity(body) = &cell.value else {
            continue;
        };
        if body.owner != producer
            && original_snapshot
                .peer_incarnations()
                .get(&body.owner.server)
                .copied()
                != Some(body.owner.generation)
        {
            return Err(ApplyFailure::InvalidAction);
        }
        entity_participants.push(pumpkin_cluster::actor_relation::ActorRelationParticipant::from_entity(body, chunk)
            .ok_or(ApplyFailure::InvalidAction)?);
        entity_spatials.insert(
            ActorId::Entity(body.id),
            original_snapshot
                .entity_effective_spatial(body.id)
                .ok_or(ApplyFailure::InvalidAction)?
                .clone(),
        );
        let kind = pumpkin_data::entity::EntityType::from_raw(body.kind)
            .ok_or(ApplyFailure::InvalidAction)?;
        let dimensions = match &body.payload {
            EntityPayload::Living(living) => {
                super::cluster_owned_mob_metadata::dimensions(living, body.kind)
                    .ok_or(ApplyFailure::InvalidAction)?
            }
            _ => pumpkin_util::math::boundingbox::EntityDimensions::new(
                kind.dimension[0],
                kind.dimension[1],
                kind.eye_height,
            ),
        };
        if dimensions.width > 2048.0 || dimensions.height > 2048.0 {
            continue;
        }
        if dimensions.width == 0.0 || dimensions.height == 0.0 {
            continue;
        }
        let contact = ContactObservation::entity(
            body,
            chunk,
            original_snapshot
                .entity_effective_spatial(body.id)
                .ok_or(ApplyFailure::InvalidAction)?,
            f64::from(dimensions.width) * 0.5,
            f64::from(dimensions.height),
            tick,
            pumpkin_cluster::identity::PlayerSeq(0),
        )
        .ok_or(ApplyFailure::InvalidAction)?;
        observations.push(contact);
    }
    observations.sort_by_key(ContactObservation::actor);
    let observations = std::sync::Arc::new(super::cluster_owned_mob_contacts::MobContactIndex::new(observations)?.with_entity_participants(entity_participants)?);
    let entity_spatials = std::sync::Arc::new(entity_spatials);
    for (contact, incarnation) in contacts {
        let Some((rules_witness, policy)) = original_snapshot.world_rules_at(contact.world, tick)
        else {
            continue;
        };
        if contact.owner != producer || contact.simulation_tick != tick {
            return Err(ApplyFailure::InvalidAction);
        }
        let draft = drafts.get(&(contact.actor(), incarnation, tick)).cloned();
        let cell = if let Some(draft) = &draft {
            draft
                .view(original_snapshot.as_ref())
                .read(TruthView::Local, contact.target())
        } else {
            original_snapshot.read(TruthView::Local, contact.target())
        }
        .ok_or(ApplyFailure::Condition(contact.target()))?;
        let living = pumpkin_cluster::living_property::state(&cell.value)
            .ok_or(ApplyFailure::Type(contact.target()))?;
        if living
            .simulation_tick
            .is_some_and(|previous| previous == tick || previous.is_after(tick))
        {
            continue;
        }
        let (rules, maximum_y, environment_rules) = {
            (
                LivingTickRules {
                    minimum_y: policy.min_y,
                    fire_damage: policy.fire_damage
                        || matches!(contact.actor(), ActorId::Entity(_)),
                    difficulty: policy.difficulty,
                    natural_regeneration: policy.natural_regeneration,
                    max_health_attribute: u16::from(
                        pumpkin_data::attributes::Attributes::MAX_HEALTH.id,
                    ),
                    regeneration_effect: u16::from(
                        pumpkin_data::effect::StatusEffect::REGENERATION.id,
                    ),
                    poison_effect: u16::from(pumpkin_data::effect::StatusEffect::POISON.id),
                    wither_effect: u16::from(pumpkin_data::effect::StatusEffect::WITHER.id),
                    hunger_effect: u16::from(pumpkin_data::effect::StatusEffect::HUNGER.id),
                },
                policy.min_y + policy.height as i32,
                pumpkin_cluster::combat_action::EnvironmentalRules {
                    fire_damage: policy.fire_damage
                        || matches!(contact.actor(), ActorId::Entity(_)),
                    drowning_damage: policy.drowning_damage
                        || matches!(contact.actor(), ActorId::Entity(_)),
                    freeze_damage: policy.freeze_damage,
                    water_breathing_effect: u16::from(
                        pumpkin_data::effect::StatusEffect::WATER_BREATHING.id,
                    ),
                    can_breathe_water: matches!(&cell.value, Value::Entity(body) if super::cluster_living_environment::can_breathe_water(body.kind)),
                    freeze_immune: matches!(&cell.value,Value::Entity(body) if pumpkin_data::entity::EntityType::from_raw(body.kind).is_some_and(|kind|kind.has_tag(&pumpkin_data::tag::EntityType::MINECRAFT_FREEZE_IMMUNE_ENTITY_TYPES))),
                },
            )
        };
        let mut intent = CombatIntent {
            damage_contacts: Vec::new(),
            falls: Vec::new(),
            damage_parent: None,
            environment: Vec::new(),
            exhaustions: Vec::new(),
            ticks: vec![LivingTickIntent {
                identity: pumpkin_cluster::combat_action::LivingTickIdentity::from_contact(
                    &contact,
                ),
                effective_position: Some(contact.position),
                previous_tick: living.simulation_tick,
                attribute_ids: original_snapshot.inventory_equipment_attribute_ids(),
                fire_immune: matches!(&cell.value, Value::Entity(body) if pumpkin_data::entity::EntityType::from_raw(body.kind).is_some_and(|kind| kind.fire_immune)),
                armor_attribute: u16::from(pumpkin_data::attributes::Attributes::ARMOR.id),
                toughness_attribute: u16::from(
                    pumpkin_data::attributes::Attributes::ARMOR_TOUGHNESS.id,
                ),
                rules,
            }],
            damages: Vec::new(),
            attacks: Vec::new(),
            vehicle_attacks: Vec::new(),
            attack_clock: None,
            statistics: Vec::new(),
            item_transforms: Vec::new(),
            held_attack: None,
            equipment: Vec::new(),
        };
        let mut environment = CombatIntent {
            environment: super::cluster_living_environment::capture(
                &contact,
                super::cluster_living_environment::eye_height(&contact, &cell.value)?,
                intent.ticks[0].fire_immune,
                match &cell.value {
                    Value::Living(living) => living
                        .avatar
                        .as_ref()
                        .is_some_and(|avatar| avatar.position != contact.position),
                    Value::Entity(body) => body.spatial.position != contact.position,
                    _ => false,
                },
                living
                    .simulation_tick
                    .map_or(1, |previous| tick.distance_since(previous)),
                rules.minimum_y,
                maximum_y,
                environment_rules,
                Some(rules_witness),
                super::cluster_living_environment::shape_properties(&cell.value)?,
            )?,
            ..CombatIntent::default()
        };
        if let ActorId::Player(actor) = contact.actor() {
            use pumpkin_data::statistic::CustomStatistic;
            for (key, kind) in [
                (CustomStatistic::DamageTaken, CombatStatisticKind::Damage),
                (
                    CustomStatistic::DamageAbsorbed,
                    CombatStatisticKind::Absorbed,
                ),
                (CustomStatistic::Deaths, CombatStatisticKind::Kill),
            ] {
                intent.statistics.push(CombatStatisticIntent {
                    target: Target::Chunk {
                        key: contact.chunk,
                        part: ChunkPart::LivingProperty {
                            actor: ActorId::Player(actor),
                            property: LivingProperty::Statistic {
                                category: 8,
                                key: key as u32,
                            },
                        },
                        inventory: None,
                    },
                    damage_targets: vec![contact.target()],
                    kind,
                });
            }
        }
        environment.statistics = intent.statistics.clone();
        let snapshot = original_snapshot.clone();
        let required_chunks = simulation_chunks(&contact);
        let required_sections = super::cluster_actor_chunk_pin::section_targets(
            &required_chunks,
            rules.minimum_y,
            maximum_y,
        )?;
        jobs.push(FrozenLivingTickJob {
            required_chunks,
            required_sections,
            pin: Some(
                simulation_pins
                    .get(&(contact.actor(), incarnation))
                    .cloned()
                    .ok_or(ApplyFailure::InvalidAction)?,
            ),
            observations: observations.clone(),
            entity_spatials: entity_spatials.clone(),
            death_inventory: capture_death_inventory(
                original_snapshot.as_ref(),
                &pumpkin_cluster::combat_action::LivingTickIdentity::from_contact(&contact),
                living,
                owners,
                executors,
            )?,
            world_policy: Some(policy.clone()),
            timestamp,
            effective_entity_spatial: match &cell.value {
                Value::Entity(body) => Some(
                    original_snapshot
                        .entity_effective_spatial(body.id)
                        .ok_or(ApplyFailure::InvalidAction)?
                        .clone(),
                ),
                _ => None,
            },
            identity: pumpkin_cluster::combat_action::LivingTickIdentity::from_contact(&contact),
            contact: Some(contact),
            hazard_resource_limit: false,
            incarnation,
            intent,
            environment,
            snapshot,
            original_world: original_world.clone(),
            draft,
        });
    }
    for (chunk, original_cell) in eligible_entity_cells(original_world.as_ref(), eligible_entities) {
        let Value::Entity(original_body) = &original_cell.value else {
            continue;
        };
        if original_body.owner != producer || !eligible_entities.contains(&original_body.id) {
            continue;
        }
        let EntityPayload::Living(original_living) = &original_body.payload else {
            continue;
        };
        let Some(dimensions) =
            super::cluster_owned_mob_metadata::dimensions(original_living, original_body.kind)
        else {
            continue;
        };
        let hazard_resource_limit = super::cluster_living_environment::geometry_resource_limit(
            original_snapshot.entity_effective_spatial(original_body.id).ok_or(ApplyFailure::InvalidAction)?.position,
            dimensions.width, dimensions.height);
        if dimensions.width != 0.0 && dimensions.height != 0.0 && !hazard_resource_limit {
            continue;
        }
        let Some((_, policy)) = original_snapshot.world_rules_at(chunk.world, tick) else {
            continue;
        };
        let identity = pumpkin_cluster::combat_action::LivingTickIdentity {
            identity: pumpkin_cluster::item_contact::ContactIdentity::Entity {
                id: original_body.id,
                birth: original_body.birth.clone(),
            },
            owner: original_body.owner,
            epoch: original_body.spatial_epoch,
            chunk,
            simulation_tick: tick,
        };
        if !identity.valid() {
            return Err(ApplyFailure::InvalidAction);
        }
        let incarnation = original_body.id.generation;
        let draft = drafts.get(&(identity.actor(), incarnation, tick)).cloned();
        let cell = draft
            .as_ref()
            .map_or_else(
                || original_snapshot.read(TruthView::Local, identity.target()),
                |draft| {
                    draft
                        .view(original_snapshot.as_ref())
                        .read(TruthView::Local, identity.target())
                },
            )
            .ok_or(ApplyFailure::Condition(identity.target()))?;
        let living = pumpkin_cluster::living_property::state(&cell.value)
            .ok_or(ApplyFailure::Type(identity.target()))?;
        if living
            .simulation_tick
            .is_some_and(|previous| previous == tick || previous.is_after(tick))
        {
            continue;
        }
        let rules = LivingTickRules {
            minimum_y: policy.min_y,
            fire_damage: true,
            difficulty: policy.difficulty,
            natural_regeneration: policy.natural_regeneration,
            max_health_attribute: u16::from(pumpkin_data::attributes::Attributes::MAX_HEALTH.id),
            regeneration_effect: u16::from(pumpkin_data::effect::StatusEffect::REGENERATION.id),
            poison_effect: u16::from(pumpkin_data::effect::StatusEffect::POISON.id),
            wither_effect: u16::from(pumpkin_data::effect::StatusEffect::WITHER.id),
            hunger_effect: u16::from(pumpkin_data::effect::StatusEffect::HUNGER.id),
        };
        let effective = original_snapshot
            .entity_effective_spatial(original_body.id)
            .ok_or(ApplyFailure::InvalidAction)?
            .clone();
        let intent = CombatIntent {
            ticks: vec![LivingTickIntent {
                identity: identity.clone(),
                effective_position: Some(effective.position),
                previous_tick: living.simulation_tick,
                attribute_ids: original_snapshot.inventory_equipment_attribute_ids(),
                fire_immune: pumpkin_data::entity::EntityType::from_raw(original_body.kind)
                    .is_some_and(|kind| kind.fire_immune),
                armor_attribute: u16::from(pumpkin_data::attributes::Attributes::ARMOR.id),
                toughness_attribute: u16::from(
                    pumpkin_data::attributes::Attributes::ARMOR_TOUGHNESS.id,
                ),
                rules,
            }],
            ..Default::default()
        };
        let death_inventory = capture_death_inventory(
            original_snapshot.as_ref(),
            &identity,
            living,
            owners,
            executors,
        )?;
        let world_policy = Some(policy.clone());
        jobs.push(FrozenLivingTickJob {
            identity,
            contact: None,
            hazard_resource_limit,
            incarnation,
            effective_entity_spatial: Some(effective),
            death_inventory,
            world_policy,
            timestamp,
            intent,
            environment: CombatIntent::default(),
            required_chunks: Vec::new(),
            required_sections: Vec::new(),
            pin: None,
            snapshot: original_snapshot.clone(),
            original_world: original_world.clone(),
            observations: observations.clone(),
            entity_spatials: entity_spatials.clone(),
            draft,
        });
    }
    jobs.sort_by_key(|job| job.identity.actor());
    pumpkin_cluster::actor_tick_frame::FrozenActorTickFrame::new(producer, tick, jobs)
        .ok_or(ApplyFailure::InvalidAction)
}

pub(super) struct LivingEmissionPage {
    pub(super) completed_jobs: usize,
    pub(super) actions: Vec<(Action, ActorActionDraft)>,
    pub(super) samples: Vec<(ChunkKey, pumpkin_cluster::item_spatial::ItemSpatialSample)>,
    pub(super) closes: Vec<(
        pumpkin_cluster::canonical_action::ActionRef,
        super::cluster_inventory_close::FrozenDeathClose,
    )>,
}

pub(super) fn emit_frame(
    frame: &mut pumpkin_cluster::actor_tick_frame::FrozenActorTickFrame<FrozenLivingTickJob>,
    server: &super::Server,
    graphs: &pumpkin_cluster::action_graph::ActionGraphs,
    pins: &mut super::cluster_actor_chunk_pin::ActorChunkPins,
    clock: &mut pumpkin_cluster::action_issue::ActionIssueClock,
    capacity: usize,
    mut entities: Option<&mut pumpkin_cluster::entity_origin::EntityOriginAllocator>,
) -> Result<LivingEmissionPage, pumpkin_cluster::canonical_action::ApplyFailure> {
    use pumpkin_cluster::canonical_action::ApplyFailure;
    let mut actions = Vec::new();
    let mut samples = Vec::new();
    let mut closes = Vec::new();
    let mut completed_jobs = 0;
    let tick = frame.tick;
    let source = frame.source;
    for job in frame.remaining_mut() {
        if job.pin.is_none() && !job.required_chunks.is_empty() {
            match pins.acquire(server, &job.required_chunks) {
                Ok(pin) => job.pin = Some(pin),
                Err(ApplyFailure::Capacity) => break,
                Err(reason) => return Err(reason),
            }
        }
        match job
            .snapshot
            .expand_history(graphs, tick, &job.required_sections)
        {
            Ok(snapshot) => job.snapshot = snapshot,
            Err(ApplyFailure::Condition(_)) => break,
            Err(reason) => return Err(reason),
        }
        let environmental = !job.environment.environment.is_empty();
        let count = 1 + usize::from(environmental) + usize::from(job.death_inventory.is_some());
        if count > capacity.saturating_sub(actions.len()) {
            break;
        }
        let Some(reservation) =
            clock.reserve(job.identity.actor(), job.incarnation, tick, count as u32)
        else {
            break;
        };
        let Some(id) = reservation.id(0, 13) else {
            clock.release_unused(reservation);
            break;
        };
        let mut additional_reservation = None;
        let mut spatial_sample = None;
        let mut close_notification = None;
        let result: Result<Vec<(Action, ActorActionDraft)>, ApplyFailure> = (|| {
            let mut draft = match &job.draft {
                Some(draft) => draft.clone(),
                None => match &job.identity.identity {
                    pumpkin_cluster::item_contact::ContactIdentity::Player(_) => {
                        ActorActionDraft::new(
                            job.identity.actor(),
                            job.incarnation,
                            tick,
                            4096,
                            16384,
                        )
                    }
                    pumpkin_cluster::item_contact::ContactIdentity::Entity { birth, .. } => {
                        ActorActionDraft::new_entity(
                            job.snapshot.as_ref(),
                            FrozenEntityIssueAuthority {
                                target: job.identity.target(),
                                birth: birth.clone(),
                                producer: source,
                                controlled_owner: source,
                                effective_tick: tick,
                                next_sequence: id.sequence,
                            },
                            tick,
                            4096,
                            16384,
                        )
                        .map_err(|_| ApplyFailure::InvalidAction)?
                    }
                },
            };
            let mut emitted = Vec::with_capacity(count);
            for (index, intent) in std::iter::once(if environmental {
                &job.environment
            } else {
                &job.intent
            })
            .chain(environmental.then_some(&job.intent))
            .enumerate()
            {
                let id = reservation
                    .id(index as u32, 13)
                    .ok_or(ApplyFailure::InvalidAction)?;
                let view = draft.view(job.snapshot.as_ref());
                let values = intent
                    .targets()
                    .map(|target| {
                        view.read(TruthView::Local, target)
                            .map(|cell| (target, cell))
                            .ok_or(ApplyFailure::Condition(target))
                    })
                    .collect::<Result<BTreeMap<_, _>, _>>()?;
                let action =
                    intent.capture_with(&view, id, job.identity.chunk, &values, Vec::new())?;
                draft
                    .issue(job.snapshot.as_ref(), action)
                    .map_err(|_| ApplyFailure::InvalidAction)?;
                emitted.push((draft.actions().last().unwrap().clone(), draft.clone()));
            }
            if let Some(death) = &job.death_inventory {
                let id = reservation
                    .id(1 + u32::from(environmental), 13)
                    .ok_or(ApplyFailure::InvalidAction)?;
                let reference = pumpkin_cluster::canonical_action::ActionRef { tick, id };
                let mut close = death.close.clone();
                let close_count = close.as_ref().map_or(0, |close| close.birth_count());
                let birth_count = close_count + death.sources.len();
                let identities = if birth_count == 0 {
                    std::sync::Arc::from(Vec::new())
                } else {
                    entities
                        .as_deref_mut()
                        .ok_or(ApplyFailure::Capacity)?
                        .reserve(source, reference, birth_count)
                        .map_err(|_| ApplyFailure::Capacity)?
                };
                let spatial = job.effective_entity_spatial.clone().unwrap_or(
                    pumpkin_cluster::entity_body::EntitySpatial {
                        world: job.identity.chunk.world,
                        position: job.intent.ticks[0]
                            .effective_position
                            .ok_or(ApplyFailure::InvalidAction)?,
                        velocity_micro: [0; 3],
                        yaw: 0_f32.to_bits(),
                        pitch: 0_f32.to_bits(),
                        on_ground: false,
                    },
                );
                let position = spatial.position.map(f64::from_bits);
                let chunk = ChunkKey::new(
                    spatial.world,
                    (position[0].floor() as i32).div_euclid(16),
                    (position[2].floor() as i32).div_euclid(16),
                );
                let primary = job.snapshot.scheduled_executor(chunk)?;
                let mut intent = super::cluster_item_birth::death_inventory_intent(
                    reference,
                    job.identity.clone(),
                    spatial,
                    death.cause,
                    death.rules,
                    death.keep_inventory,
                    death.sources.clone(),
                    death.equipment.clone(),
                    death.entity_equipment.clone(),
                    &identities[close_count..],
                    primary,
                )?;
                if let Some(close) = &mut close {
                    close.bind_identities(reference, &identities[..close_count])?;
                    intent.close = Some(close.intent.clone());
                }
                if !identities.is_empty() {
                    let planned = intent
                        .drops
                        .iter()
                        .map(|drop| drop.destination)
                        .chain(
                            intent
                                .close
                                .iter()
                                .flat_map(|close| close.overflow.iter().map(|drop| drop.target)),
                        )
                        .map(|target| match target {
                            pumpkin_cluster::canonical_action::Target::Chunk {
                                key,
                                part: pumpkin_cluster::canonical_action::ChunkPart::Entity(id),
                                inventory: None,
                            } => Ok((key, id)),
                            _ => Err(ApplyFailure::InvalidAction),
                        })
                        .collect::<Result<Vec<_>, _>>()?;
                    job.snapshot = job.snapshot.clone().with_reserved_births(
                        entities.as_deref().ok_or(ApplyFailure::Capacity)?,
                        reference,
                        source,
                        &planned,
                    )?;
                }
                let action = pumpkin_cluster::death_inventory::capture(
                    &draft.view(job.snapshot.as_ref()),
                    id,
                    tick,
                    intent,
                )?;
                draft
                    .issue(job.snapshot.as_ref(), action)
                    .map_err(|_| ApplyFailure::InvalidAction)?;
                emitted.push((draft.actions().last().unwrap().clone(), draft.clone()));
                if let Some(close) = close {
                    close_notification = Some((reference, close));
                }
            }
            let mut retired = false;
            if matches!(job.identity.actor(), ActorId::Entity(_)) {
                if let Some(retirement) =
                    super::cluster_owned_mob_death::FrozenMobRetirement::capture_identity(
                        job.snapshot.as_ref(),
                        &draft,
                        &job.identity,
                        job.observations.observations(),
                    )?
                {
                    if count + 1 > capacity.saturating_sub(actions.len()) {
                        return Err(ApplyFailure::Capacity);
                    }
                    let extra = clock
                        .reserve(job.identity.actor(), job.incarnation, tick, 1)
                        .ok_or(ApplyFailure::Capacity)?;
                    let retirement_id = extra.id(0, 13).ok_or(ApplyFailure::InvalidAction)?;
                    additional_reservation = Some(extra);
                    let action = retirement.materialize(
                        &draft.view(job.snapshot.as_ref()),
                        retirement_id,
                        tick,
                    )?;
                    draft
                        .issue(job.snapshot.as_ref(), action)
                        .map_err(|_| ApplyFailure::InvalidAction)?;
                    emitted.push((draft.actions().last().unwrap().clone(), draft.clone()));
                    retired = true;
                }
            }
            if !retired
                && let (Some(spatial), Some(policy), Some(contact)) = (
                    &job.effective_entity_spatial,
                    &job.world_policy,
                    &job.contact,
                )
            {
                let rules = job
                    .snapshot
                    .mob_rules(job.identity.chunk.world)
                    .ok_or(ApplyFailure::InvalidAction)?;
                let chunks = job.required_chunks.iter().copied().collect();
                if let Some(mut phase) = super::cluster_owned_mob_phase::FrozenMobPhase::capture(
                    job.snapshot.as_ref(),
                    &draft,
                    contact,
                    spatial,
                    &job.observations,
                    &chunks,
                    rules,
                )? {
                    phase
                        .navigate(
                            &job.original_world,
                            server.block_registry.clone(),
                            &draft,
                            &chunks,
                            rules,
                        )
                        .map_err(ApplyFailure::from)?;
                    let fall_enabled =
                        policy.fall_damage || matches!(job.identity.actor(), ActorId::Entity(_));
                    let travel = phase.travel(
                        job.snapshot.as_ref(),
                        &draft,
                        &chunks,
                        rules,
                        pumpkin_cluster::identity::PlayerSeq(id.sequence),
                        fall_enabled,
                        job.entity_spatials.as_ref(),
                    )?;
                    let crossing = travel.destination != job.identity.chunk;
                    let phase_count = phase.action_count(&travel)?;
                    let additional = phase_count + usize::from(crossing);
                    if count + additional > capacity.saturating_sub(actions.len()) {
                        return Err(ApplyFailure::Capacity);
                    }
                    let reserved = clock
                        .reserve(
                            job.identity.actor(),
                            job.incarnation,
                            tick,
                            additional as u32,
                        )
                        .ok_or(ApplyFailure::Capacity)?;
                    let ids = (0..phase_count)
                        .map(|index| {
                            reserved
                                .id(index as u32, 13)
                                .ok_or(ApplyFailure::InvalidAction)
                        })
                        .collect::<Result<Vec<_>, _>>()?;
                    let transition_id = crossing
                        .then(|| reserved.id(phase_count as u32, 13))
                        .flatten();
                    additional_reservation = Some(reserved);
                    let primary = job.snapshot.scheduled_executor(job.identity.chunk)?;
                    let output = phase.materialize(
                        travel,
                        job.snapshot.clone(),
                        draft,
                        &ids,
                        entities.as_deref_mut().ok_or(ApplyFailure::Capacity)?,
                        primary,
                        &job.observations,
                        &job.entity_spatials,
                        job.timestamp,
                    )?;
                    let mut last_draft = output
                        .actions
                        .last()
                        .ok_or(ApplyFailure::InvalidAction)?
                        .1
                        .clone();
                    emitted.extend(output.actions);
                    if let Some(transition_id) = transition_id {
                        let target = job.identity.target();
                        let Value::Entity(body) = last_draft
                            .view(job.snapshot.as_ref())
                            .read(TruthView::Local, target)
                            .ok_or(ApplyFailure::Condition(target))?
                            .value
                        else {
                            return Err(ApplyFailure::Type(target));
                        };
                        let action = pumpkin_cluster::entity_transition::capture(
                            &body,
                            job.identity.chunk,
                            output.sample.spatial.clone(),
                            tick,
                            transition_id,
                        )?;
                        last_draft
                            .issue(job.snapshot.as_ref(), action)
                            .map_err(|_| ApplyFailure::InvalidAction)?;
                        emitted.push((last_draft.actions().last().unwrap().clone(), last_draft));
                    } else {
                        spatial_sample = Some((job.identity.chunk, output.sample));
                    }
                }
            }
            Ok(emitted)
        })();
        match result {
            Ok(emitted) => {
                actions.extend(emitted);
                samples.extend(spatial_sample);
                closes.extend(close_notification);
                completed_jobs += 1;
            }
            Err(reason) => {
                if let Some(extra) = additional_reservation {
                    clock.release_unused(extra);
                }
                clock.release_unused(reservation);
                return if actions.is_empty() {
                    Err(reason)
                } else {
                    Ok(LivingEmissionPage {
                        completed_jobs,
                        actions,
                        samples,
                        closes,
                    })
                };
            }
        }
    }
    Ok(LivingEmissionPage {
        completed_jobs,
        actions,
        samples,
        closes,
    })
}

#[derive(Clone)]
struct FrozenDeathInventory {
    cause: pumpkin_cluster::canonical_action::ActionRef,
    rules: pumpkin_cluster::world_rules::WorldRulesWitness,
    keep_inventory: bool,
    sources: Vec<pumpkin_cluster::canonical_inventory::InventorySlotBinding>,
    equipment: Vec<pumpkin_cluster::canonical_inventory::InventoryEquipmentBinding>,
    entity_equipment: Option<pumpkin_cluster::canonical_inventory::EntityInventoryEquipmentBinding>,
    close: Option<super::cluster_inventory_close::FrozenDeathClose>,
}

fn capture_death_inventory(
    snapshot: &super::cluster_world_callback_snapshot::CallbackTickSnapshot,
    contact: &pumpkin_cluster::combat_action::LivingTickIdentity,
    living: &pumpkin_cluster::item_action::ActorLivingState,
    owners: &super::cluster_world_canonical::OwnedWorldActionState<'_>,
    executors: &BTreeMap<ChunkKey, PeerGeneration>,
) -> Result<Option<FrozenDeathInventory>, pumpkin_cluster::canonical_action::ApplyFailure> {
    use pumpkin_cluster::canonical_action::ApplyFailure;
    use pumpkin_cluster::canonical_inventory::{InventoryEquipmentBinding, InventorySlotBinding};
    use pumpkin_cluster::inventory::{
        INV_MAX_STACK, INV_SELECTED, InventorySlotRef, InventoryTarget,
    };
    if living.combat.health_milli != 0 || living.combat.timing.death_inventory_processed {
        return Ok(None);
    }
    let cause = living
        .combat
        .timing
        .death_cause
        .ok_or(ApplyFailure::InvalidAction)?;
    let (rules, policy) = snapshot
        .world_rules_at(contact.chunk.world, contact.simulation_tick)
        .ok_or(ApplyFailure::InvalidAction)?;
    let sources: Vec<InventorySlotBinding> = snapshot.captured_inventory_targets().filter(|(_, owner, location)| {
        match &contact.identity {
            pumpkin_cluster::item_contact::ContactIdentity::Player(lease) => {
                matches!(owner, InventoryTarget::LeasedPlayer {key, player, incarnation}
                    if *key == lease.key.0 && *player == lease.gid && *incarnation == lease.incarnation)
                    && location.inv < INV_SELECTED
            }
            pumpkin_cluster::item_contact::ContactIdentity::Entity { id, .. } => {
                matches!(owner, InventoryTarget::Entity(entity) if *entity == (*id).into())
            }
        }
    }).map(|(target, owner, location)| InventorySlotBinding {
        slot: InventorySlotRef::new(owner, location), target, max_stack: INV_MAX_STACK,
    }).collect();
    let (keep_inventory, equipment) = match &contact.identity {
        pumpkin_cluster::item_contact::ContactIdentity::Player(lease) => (
            policy.keep_inventory,
            vec![InventoryEquipmentBinding::player(
                *lease,
                contact.chunk,
                snapshot.inventory_equipment_attribute_ids(),
            )],
        ),
        _ if living.stand.is_some() => (!policy.do_entity_drops, Vec::new()),
        _ if living.mob.is_some() => (!policy.do_mob_loot, Vec::new()),
        _ => return Err(ApplyFailure::InvalidAction),
    };
    let entity_equipment = match snapshot.read(TruthView::Local, contact.target()) {
        Some(pumpkin_cluster::canonical_action::Cell {
            value: Value::Entity(body),
            ..
        }) => super::cluster_world_inventory_actions::entity_equipment_binding(
            snapshot,
            &body,
            contact.chunk,
            &sources,
        )?,
        _ if matches!(contact.actor(), ActorId::Player(_)) => None,
        _ => return Err(ApplyFailure::InvalidAction),
    };
    let close = match &contact.identity {
        pumpkin_cluster::item_contact::ContactIdentity::Player(lease) => Some(
            super::cluster_inventory_close::capture_death_close_from_snapshot(
                snapshot,
                owners,
                *lease,
                contact.chunk,
                contact.simulation_tick,
                executors,
            )?,
        ),
        _ => None,
    };
    Ok(Some(FrozenDeathInventory {
        close,
        cause,
        rules,
        keep_inventory,
        sources,
        equipment,
        entity_equipment,
    }))
}

fn simulation_chunks(contact: &ContactObservation) -> Vec<ChunkKey> {
    let radius = if matches!(contact.actor(), ActorId::Entity(_)) {
        3
    } else {
        1
    };
    ((contact.chunk.x() - radius)..=(contact.chunk.x() + radius))
        .flat_map(|x| {
            ((contact.chunk.z() - radius)..=(contact.chunk.z() + radius))
                .map(move |z| ChunkKey::new(contact.world, x, z))
        })
        .collect()
}

fn eligible_entity_cells<'a>(
    store: &'a super::cluster_chunk_state::OwnedChunkStore,
    ids: &'a std::collections::BTreeSet<pumpkin_cluster::protocol::GlobalEntityId>,
) -> impl Iterator<Item = (ChunkKey, &'a pumpkin_cluster::canonical_action::Cell)> + 'a {
    ids.iter().filter_map(|id| {
        let chunk = store.entity_body_chunk(false, *id)?;
        Some((chunk, store.entity_body(false, chunk, *id)?))
    })
}
