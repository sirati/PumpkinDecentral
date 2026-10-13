use super::*;
use std::collections::BTreeMap;
use pumpkin_cluster::actor::ActorId;
use pumpkin_cluster::canonical_action::{ActionRef, Cell, Effect, SpatialEpoch, prepare_action};
use pumpkin_cluster::entity_body::{EntityBody, EntitySpatial};
use pumpkin_cluster::entity_origin::EntityOriginIdentity;
use pumpkin_cluster::identity::ServerId;
use pumpkin_cluster::protocol::{GlobalEntityId, WorldId};
use pumpkin_cluster::realm_completion::PeerGeneration;
use pumpkin_cluster::time::TickStamp;

struct State {
    body: Cell,
    target: Target,
    inventory: BTreeMap<Target, Cell>,
}
impl ActionState for State {
    fn death_inventory_rule(&self, reference: ActionRef, rules: pumpkin_cluster::world_rules::WorldRulesWitness, keep: bool, target: Target) -> Result<(), ApplyFailure> {
        let Target::Chunk { key, .. } = self.target else { return Err(ApplyFailure::InvalidAction); };
        let Value::Entity(body) = &self.body.value else { return Err(ApplyFailure::InvalidAction); };
        if target != self.target || keep || rules.rules.world != key.world || rules.slot as u16 != reference.tick.0
            || rules.rules.issuer != body.owner || rules.rules.epoch != 1
            || reference.id.actor != ActorId::Entity(body.id) || reference.id.incarnation != body.id.generation {
            return Err(ApplyFailure::InvalidAction);
        }
        Ok(())
    }
    fn inventory_equipment_attribute_ids(&self) -> Vec<u16> {
        pumpkin_data::attributes::Attributes::ALL.iter().map(|attribute| u16::from(attribute.id)).collect()
    }
    fn commit(&mut self, _: pumpkin_cluster::canonical_action::ActionContext,
        _: TruthView, _: &[pumpkin_cluster::canonical_action::AppliedWrite]) -> Result<(), ApplyFailure> {
        Err(ApplyFailure::InvalidAction)
    }
    fn read(&self, _: TruthView, target: Target) -> Option<Cell> {
        if target == self.target { return Some(self.body.clone()); }
        if pumpkin_cluster::derived_living::actor_body_target(target) == Some(self.target) {
            return pumpkin_cluster::derived_living::actor_facet_cell(&self.body, target);
        }
        self.inventory.get(&target).cloned()
    }
    fn entity_inventory_layout(&self, body: &EntityBody)
        -> Result<Vec<pumpkin_cluster::inventory::InvLoc>, ApplyFailure> {
        crate::server::cluster_owned_mob_birth::inventory_layout(body)
    }
    fn status_effect_facts(&self, effects: &pumpkin_cluster::active_effects::ActiveEffectSet)
        -> Result<pumpkin_cluster::combat_action::CombatStatusFacts, ApplyFailure> {
        Ok(crate::entity::combat::normalized_status(effects.visible.iter(), false))
    }
    fn status_effect_modifiers(&self, effects: &pumpkin_cluster::active_effects::ActiveEffectSet)
        -> Result<Vec<pumpkin_cluster::combat_action::CombatAttributeModifier>, ApplyFailure> {
        crate::server::cluster_item_status::modifiers(effects)
    }
}
fn initial() -> (State, ActionRef) {
    let owner = PeerGeneration { server: ServerId(7), generation: 3 };
    let birth = ActionRef { id: ActionId { actor: ActorId::Peer(owner.server),
        incarnation: owner.generation, sequence: 1, family: 14 }, tick: TickStamp(10) };
    let identity = EntityOriginIdentity { id: GlobalEntityId { origin: owner.server,
        local_id: 1, generation: owner.generation }, uuid: uuid::Uuid::from_u128(1), source: owner };
    let spatial = EntitySpatial { world: WorldId::new(0, 0), position: [0.5_f64, 64.0, 0.5].map(f64::to_bits),
        velocity_micro: [0; 3], yaw: 0.0_f32.to_bits(), pitch: 0.0_f32.to_bits(), on_ground: true };
    let mut root = pumpkin_nbt::compound::NbtCompound::new();
    root.put_float("Health", 0.0);
    let admission = crate::server::cluster_owned_mob_initial::paired_admission(&identity,
        birth, owner, &pumpkin_data::entity::EntityType::COW, spatial, &root).unwrap();
    let target = admission.target();
    let inventory = admission.inventory.entries(admission.chunk, identity.id)
        .map(|(target, cell)| (target, cell.clone())).collect();
    (State { target, body: Cell { revision: 0, value: Value::Entity((admission.body).into()) }, inventory }, birth)
}
fn elapsed(state: &mut State, tick: TickStamp) {
    use pumpkin_cluster::combat_action::{CombatIntent, LivingTickIdentity, LivingTickIntent, LivingTickRules};
    let Value::Entity(body) = &state.body.value else { panic!() };
    let EntityPayload::Living(living) = &body.payload else { panic!() };
    let Target::Chunk { key, .. } = state.target else { panic!() };
    let identity = LivingTickIdentity { identity: pumpkin_cluster::item_contact::ContactIdentity::Entity {
        id: body.id, birth: body.birth.clone() }, owner: body.owner, epoch: body.spatial_epoch,
        chunk: key, simulation_tick: tick };
    let id = ActionId { actor: ActorId::Entity(body.id), incarnation: body.id.generation,
        sequence: tick.0 * 2, family: 13 };
    let intent = CombatIntent { ticks: vec![LivingTickIntent { identity,
        effective_position: Some(body.spatial.position), previous_tick: living.simulation_tick,
        attribute_ids: state.inventory_equipment_attribute_ids(), fire_immune: false,
        armor_attribute: u16::from(pumpkin_data::attributes::Attributes::ARMOR.id),
        toughness_attribute: u16::from(pumpkin_data::attributes::Attributes::ARMOR_TOUGHNESS.id),
        rules: LivingTickRules { minimum_y: -64, fire_damage: true, difficulty: 2,
            natural_regeneration: true,
            max_health_attribute: u16::from(pumpkin_data::attributes::Attributes::MAX_HEALTH.id),
            regeneration_effect: u16::from(pumpkin_data::effect::StatusEffect::REGENERATION.id),
            poison_effect: u16::from(pumpkin_data::effect::StatusEffect::POISON.id),
            wither_effect: u16::from(pumpkin_data::effect::StatusEffect::WITHER.id),
            hunger_effect: u16::from(pumpkin_data::effect::StatusEffect::HUNGER.id) } }],
        ..Default::default() };
    let values = intent.targets().map(|target| (target, state.read(TruthView::Local, target).unwrap())).collect();
    let action = intent.capture_with(state, id, key, &values, Vec::new()).unwrap();
    let applied = prepare_action(state, &action, TruthView::Local).unwrap();
    state.body = pumpkin_cluster::derived_living::project_body_writes(state.target,
        Some(&state.body), &applied.writes).unwrap().unwrap();
}
fn process_empty_death_inventory(state: &mut State, cause: ActionRef, tick: TickStamp) {
    assert!(state.inventory.values().all(|cell| matches!(&cell.value, Value::Inventory(item) if item.is_empty())));
    let Value::Entity(body) = &state.body.value else { panic!() };
    let Target::Chunk { key, .. } = state.target else { panic!() };
    let identity = pumpkin_cluster::combat_action::LivingTickIdentity {
        identity: pumpkin_cluster::item_contact::ContactIdentity::Entity { id: body.id, birth: body.birth.clone() },
        owner: body.owner, epoch: body.spatial_epoch, chunk: key, simulation_tick: tick,
    };
    let intent = pumpkin_cluster::death_inventory::DeathInventoryIntent {
        identity, spatial: body.spatial.clone(), death_cause: cause, keep_inventory: false,
        rules: pumpkin_cluster::world_rules::WorldRulesWitness {
            rules: pumpkin_cluster::world_rules::WorldRulesId { world: key.world, issuer: body.owner, epoch: 1 },
            slot: u64::from(tick.0),
        }, drops: Vec::new(), equipment: Vec::new(), entity_equipment: None, close: None,
    };
    let id = ActionId { actor: ActorId::Entity(body.id), incarnation: body.id.generation,
        sequence: tick.0 * 2 + 1, family: 13 };
    let action = pumpkin_cluster::death_inventory::capture(state, id, tick, intent).unwrap();
    let applied = prepare_action(state, &action, TruthView::Local).unwrap();
    state.body = pumpkin_cluster::derived_living::project_body_writes(state.target,
        Some(&state.body), &applied.writes).unwrap().unwrap();
}

#[test]
fn zero_health_birth_reaches_retirement_without_fabricated_damage() {
    let (mut state, birth) = initial();
    let Value::Entity(body) = &state.body.value else { panic!() };
    let EntityPayload::Living(living) = &body.payload else { panic!() };
    assert_eq!(living.combat.timing.death_cause, Some(birth));
    assert_eq!(living.combat.timing.last_damage_kind, None);
    elapsed(&mut state, TickStamp(11));
    process_empty_death_inventory(&mut state, birth, TickStamp(11));
    for tick in 12..31 { elapsed(&mut state, TickStamp(tick)); }
    let Value::Entity(body) = &mut state.body.value else { panic!() };
    let EntityPayload::Living(living) = &mut body.payload else { panic!() };
    assert_eq!(living.combat.timing.death_ticks, 20);
    assert!(living.combat.timing.death_inventory_processed);
    let body = body.clone();
    let Target::Chunk { key, .. } = state.target else { panic!() };
    let detach = capture_owner_detach_from_participants(&state, &body, key, &[]).unwrap();
    let inventory = state.inventory.iter().map(|(target, cell)| {
        let Value::Inventory(item) = &cell.value else { panic!() };
        InventorySlotBinding { target: *target, slot: InventorySlotRef::new(InventoryTarget::Entity(body.id.into()), item.loc), max_stack: INV_MAX_STACK }
    }).collect();
    let retirement = EntityRetirementIntent { target: state.target, birth: body.birth.clone(), owner: body.owner,
        kind: body.kind, inventory, disposition: EntityInventoryDisposition::Destroy,
        living_death: Some(LivingDeathRetirement { death_cause: birth, epoch: SpatialEpoch::Action(birth) }) };
    let action = retirement.capture_action_with_detach(&state, ActionId { actor: ActorId::Entity(body.id),
        incarnation: body.id.generation, sequence: 61, family: 13 }, TickStamp(30), ClientOrigin::None, Some(detach)).unwrap();
    let applied = prepare_action(&state, &action, TruthView::Local).unwrap();
    assert!(applied.writes.iter().any(|write| write.target == state.target && write.after.is_none() && write.before == Some(state.body.clone())));
    assert!(state.inventory.keys().all(|target| applied.writes.iter().any(|write| write.target == *target && write.after.is_none())));
    assert!(action.effects.iter().all(|effect| !matches!(effect, Effect::Put { value: Value::LivingProperty(pumpkin_cluster::living_property::LivingPropertyValue::DamageKind(_)), .. })));
    let Value::Entity(body) = &mut state.body.value else { panic!() };
    let EntityPayload::Living(living) = &mut body.payload else { panic!() };
    living.combat.health_milli = 1;
    assert!(matches!(prepare_action(&state, &action, TruthView::Local), Err(ApplyFailure::Condition(_))));
}

struct IssuerState { authorized: bool }
impl ActionState for IssuerState {
    fn read(&self, _: TruthView, _: Target) -> Option<Cell> { None }
    fn commit(&mut self, _: pumpkin_cluster::canonical_action::ActionContext,
        _: TruthView, _: &[pumpkin_cluster::canonical_action::AppliedWrite]) -> Result<(), ApplyFailure> {
        Err(ApplyFailure::InvalidAction)
    }
    fn issuer_entity_admission_authority(&self, _: ActionRef,
        witness: &pumpkin_cluster::issuer_entity_admission::IssuerAdmissionWitness) -> Result<(), ApplyFailure> {
        if self.authorized { Ok(()) } else { Err(ApplyFailure::MissingStakeholder(witness.admission.target().stakeholder())) }
    }
    fn issuer_entity_initialization_matches(&self,
        witness: &pumpkin_cluster::issuer_entity_admission::IssuerAdmissionWitness) -> Result<(), ApplyFailure> {
        super::super::cluster_owned_mob_admission::validate_initialization(witness)
    }
    fn entity_inventory_layout(&self, body: &EntityBody) -> Result<Vec<pumpkin_cluster::inventory::InvLoc>, ApplyFailure> {
        super::super::cluster_owned_mob_birth::inventory_layout(body)
    }
}

fn primary_witness() -> pumpkin_cluster::issuer_entity_admission::IssuerAdmissionWitness {
    use pumpkin_cluster::issuer_entity_admission::{IssuerAdmissionAuthority, IssuerAdmissionWitness};
    let (state, _birth) = initial();
    let Value::Entity(body) = state.body.value else { panic!() };
    let Target::Chunk { key, .. } = state.target else { panic!() };
    let inventory = pumpkin_cluster::entity_inventory_admission::EntityInventoryAdmission {
        imported_revision: 0, cells: state.inventory.into_values().collect() };
    let mut root = pumpkin_nbt::compound::NbtCompound::new();
    root.put_float("Health", 0.0);
    let request = pumpkin_cluster::mob_initialization::EntityAdmissionRequest {
        kind: body.kind, spatial: body.spatial.clone(),
        initial: super::super::cluster_owned_mob_initial::parse_initialization(
            &pumpkin_data::entity::EntityType::COW, &body.spatial, &root).unwrap() };
    let witness = IssuerAdmissionWitness {
        issuer: body.owner, actor_chunk: key, lease: None, source_slot: 10,
        authority: IssuerAdmissionAuthority::Primary { rules: pumpkin_cluster::world_rules::WorldRulesWitness {
            rules: pumpkin_cluster::world_rules::WorldRulesId { world: key.world, issuer: body.owner, epoch: 1 }, slot: 10 } },
        admission: pumpkin_cluster::mob_breeding::MobBirthAdmission { chunk: key, body: *body, inventory }, request };
    witness
}

#[test]
fn issuer_birth_preserves_original_identity_and_requires_retained_authority() {
    let mut witness = primary_witness();
    let birth = witness.admission.body.birth.action;
    let action = witness.capture_action(ClientOrigin::None).unwrap();
    assert_eq!(action.id, birth.id);
    assert_eq!(action.tick, birth.tick);
    assert!(matches!(prepare_action(&IssuerState { authorized: false }, &action, TruthView::Local),
        Err(ApplyFailure::MissingStakeholder(_))));
    let applied = prepare_action(&IssuerState { authorized: true }, &action, TruthView::Local).unwrap();
    assert!(applied.writes.iter().all(|write| write.before.is_none()));
    assert!(applied.writes.iter().any(|write| write.target == witness.admission.target()
        && write.after.as_ref().is_some_and(|cell| cell.value == Value::Entity(witness.admission.body.clone().into()))));
    witness.admission.body.tags.insert("forged".to_owned());
    let forged = witness.capture_action(ClientOrigin::None).unwrap();
    assert!(matches!(prepare_action(&IssuerState { authorized: true }, &forged, TruthView::Local),
        Err(ApplyFailure::InvalidAction)));
}

#[test]
fn command_receipt_binds_original_phase_and_reserved_output() {
    use pumpkin_cluster::identity::{GlobalPlayerId, PlayerSlot};
    use pumpkin_cluster::issuer_entity_admission::{AcceptedCommandIssuance, CommandPermissionSnapshot,
        IssuerAdmissionAuthority, IssuerAdmissionWitness, request_digest};
    use pumpkin_cluster::player_command_control::CommandPrincipal;
    let mut witness = primary_witness();
    let issuance = AcceptedCommandIssuance {
        issuer: witness.issuer, origin: witness.issuer, transport_generation: 0,
        principal: CommandPrincipal::Player { gid: GlobalPlayerId { server: witness.issuer.server, player: PlayerSlot(1) },
            uuid: *uuid::Uuid::from_u128(55).as_bytes(), permission_revision: 11 },
        correlation: [1; 16], permission_snapshot: CommandPermissionSnapshot { revision: 11, level: 2 },
        request_digest: request_digest(&witness.request).unwrap(),
    };
    let receipt = issuance.bind(witness.source_slot, &witness.admission).unwrap();
    witness.authority = IssuerAdmissionAuthority::Command { receipt: Box::new(receipt) };
    let reference = witness.admission.body.birth.action;
    assert!(matches!(super::super::cluster_command_issuance::validate(reference, &witness, None),
        Err(ApplyFailure::MissingStakeholder(_))));
    assert!(matches!(super::super::cluster_command_issuance::validate(reference, &witness, Some(witness.source_slot + 65536)),
        Err(ApplyFailure::InvalidAction)));
    super::super::cluster_command_issuance::validate(reference, &witness, Some(witness.source_slot)).unwrap();
    let action = witness.capture_action(ClientOrigin::None).unwrap();
    prepare_action(&IssuerState { authorized: true }, &action, TruthView::Local).unwrap();
    let mutations: Vec<Box<dyn Fn(&mut IssuerAdmissionWitness)>> = vec![
        Box::new(|witness| { if let IssuerAdmissionAuthority::Command { receipt } = &mut witness.authority { receipt.reference.id.sequence += 1; } }),
        Box::new(|witness| { if let IssuerAdmissionAuthority::Command { receipt } = &mut witness.authority { receipt.source_slot += 65536; } }),
        Box::new(|witness| { if let IssuerAdmissionAuthority::Command { receipt } = &mut witness.authority { receipt.issuance.correlation[0] ^= 1; } }),
        Box::new(|witness| { if let IssuerAdmissionAuthority::Command { receipt } = &mut witness.authority {
            if let CommandPrincipal::Player { uuid, .. } = &mut receipt.issuance.principal { uuid[0] ^= 1; } } }),
        Box::new(|witness| { if let IssuerAdmissionAuthority::Command { receipt } = &mut witness.authority { receipt.issuance.request_digest[0] ^= 1; } }),
        Box::new(|witness| { if let IssuerAdmissionAuthority::Command { receipt } = &mut witness.authority { receipt.issuance.permission_snapshot.level = 0; } }),
        Box::new(|witness| { if let IssuerAdmissionAuthority::Command { receipt } = &mut witness.authority { receipt.issuance.transport_generation = 9; } }),
        Box::new(|witness| { witness.admission.body.birth.uuid = uuid::Uuid::from_u128(99); }),
        Box::new(|witness| { witness.admission.inventory.cells.pop(); }),
        Box::new(|witness| { witness.request.initial.tags.insert("changed".to_owned()); }),
    ];
    for mutate in mutations {
        let mut tampered = witness.clone();
        mutate(&mut tampered);
        assert!(!tampered.valid());
        assert!(matches!(tampered.capture_action(ClientOrigin::None), Err(ApplyFailure::InvalidAction)));
    }
}

#[test]
fn initial_health_clamps_after_imported_maximum_attributes() {
    let (state, reference) = initial();
    let Value::Entity(body) = state.body.value else { panic!() };
    let identity = EntityOriginIdentity { id: body.id, uuid: body.birth.uuid, source: body.owner };
    for (maximum, expected) in [(40.0, 35_000), (2.0, 2_000)] {
        let mut root = pumpkin_nbt::compound::NbtCompound::new();
        root.put_float("Health", 35.0);
        let mut attribute = pumpkin_nbt::compound::NbtCompound::new();
        attribute.put_string("id", pumpkin_data::attributes::Attributes::MAX_HEALTH.name.to_owned());
        attribute.put_double("base", maximum);
        root.put_list("attributes", vec![pumpkin_nbt::tag::NbtTag::Compound(attribute)]);
        let admission = super::super::cluster_owned_mob_initial::paired_admission(&identity,
            reference, body.owner, &pumpkin_data::entity::EntityType::COW, body.spatial.clone(), &root).unwrap();
        let EntityPayload::Living(living) = admission.body.payload else { panic!() };
        assert_eq!(living.combat.health_milli, expected);
        assert_eq!(living.combat.timing.death_cause, None);
    }
}

#[test]
fn reserved_trader_birth_generates_repeatable_actual_offers_and_preserves_supplied_offers() {
    let (state, reference) = initial();
    let Value::Entity(body) = state.body.value else { panic!() };
    let identity = EntityOriginIdentity { id: body.id, uuid: body.birth.uuid, source: body.owner };
    let kind = &pumpkin_data::entity::EntityType::WANDERING_TRADER;
    let root = pumpkin_nbt::compound::NbtCompound::new();
    let initial = crate::server::cluster_owned_mob_initial::parse_initialization(kind, &body.spatial, &root).unwrap();
    let factory = |initial| crate::server::cluster_owned_mob_initial::prepare_typed_body(
        &identity, reference, body.owner, kind, body.spatial.clone(), initial).unwrap().0;
    let generated = factory(&initial);
    assert_eq!(generated, factory(&initial));
    let EntityPayload::Living(living) = generated.payload else { panic!() };
    let offers = living.merchant.unwrap();
    assert_eq!(offers.offers.len(), 9);
    assert_eq!(offers.scheme, pumpkin_cluster::canonical_merchant::MerchantScheme::WanderingTrader);
    let mut supplied = initial.clone();
    supplied.merchant = Some(offers.clone());
    let EntityPayload::Living(living) = factory(&supplied).payload else { panic!() };
    assert_eq!(living.merchant, Some(offers));
}

#[test]
fn imported_mob_flags_reach_java_and_bedrock_metadata_and_clear() {
    let (state, reference) = initial();
    let Value::Entity(body) = state.body.value else { panic!() };
    let identity = EntityOriginIdentity { id: body.id, uuid: body.birth.uuid, source: body.owner };
    let mut root = pumpkin_nbt::compound::NbtCompound::new();
    root.put_bool("NoAI", true);
    root.put_bool("LeftHanded", true);
    root.put_bool("CanPickUpLoot", true);
    let admitted = crate::server::cluster_owned_mob_initial::paired_admission(
        &identity, reference, body.owner, &pumpkin_data::entity::EntityType::COW, body.spatial, &root).unwrap();
    let EntityPayload::Living(mut living) = admitted.body.payload else { panic!() };
    let version = pumpkin_util::version::JavaMinecraftVersion::V_1_21;
    let mut java = Vec::new();
    crate::server::cluster_owned_mob_metadata::write_java(&living, admitted.body.kind, version, &mut java).unwrap();
    assert_eq!(java[..3], [pumpkin_data::tracked_data::mob::DATA_MOB_FLAGS_ID.id.get(&version), 0, 11]);
    use pumpkin_protocol::bedrock::client::set_actor_data::{MetadataValue, entity_data_flag as flag, entity_data_key as key};
    let mut bedrock = crate::server::cluster_owned_mob_metadata::bedrock(&living, admitted.body.kind);
    assert!(matches!(bedrock.0.get(&key::FLAGS), Some(MetadataValue::Int64(flags)) if flags & (1_i64 << flag::NO_AI) != 0));
    let brain = &mut living.mob.as_mut().unwrap().brain;
    brain.ai_disabled = false;
    brain.left_handed = false;
    brain.can_pick_up_loot = false;
    java.clear();
    crate::server::cluster_owned_mob_metadata::write_java(&living, admitted.body.kind, version, &mut java).unwrap();
    assert_eq!(java[2], 0);
    crate::server::cluster_owned_mob_metadata::write_bedrock(&living, admitted.body.kind, &mut bedrock);
    assert!(matches!(bedrock.0.get(&key::FLAGS), Some(MetadataValue::Int64(flags)) if flags & (1_i64 << flag::NO_AI) == 0));
}

#[test]
fn horse_child_resets_taming_and_has_working_ageable_goals() {
    let (state, reference) = initial();
    let Value::Entity(body) = state.body.value else { panic!() };
    let identity = EntityOriginIdentity { id: body.id, uuid: body.birth.uuid, source: body.owner };
    let mut root = pumpkin_nbt::compound::NbtCompound::new();
    root.put_bool("Tame", true);
    root.put_uuid("Owner", uuid::Uuid::from_u128(99));
    root.put_int("Temper", 80);
    let parent = crate::server::cluster_owned_mob_initial::paired_admission(
        &identity, reference, body.owner, &pumpkin_data::entity::EntityType::HORSE, body.spatial, &root).unwrap().body;
    let EntityPayload::Living(living) = &parent.payload else { panic!() };
    let mut brain = living.mob.as_ref().unwrap().brain.clone();
    let child_identity = EntityOriginIdentity { id: GlobalEntityId { local_id: 2, ..identity.id },
        uuid: uuid::Uuid::from_u128(2), source: identity.source };
    let child = crate::server::cluster_owned_mob_breeding::child_continuation(
        &parent, &parent, &child_identity, &mut brain).unwrap();
    assert_eq!(child.reproduction.growth_age, -24000);
    assert!(matches!(child.species, pumpkin_cluster::mob_continuation::MobSpecies::Horse {
        flags: 0, owner: None, temper: 0, chested: false, .. }));
    assert!(child.brain.goals.iter().any(|goal| matches!(goal.progress,
        pumpkin_cluster::mob_continuation::MobGoalProgress::Follow { mode: pumpkin_cluster::mob_continuation::MobFollowMode::Parent, .. })));
    assert!(child.brain.goals.iter().any(|goal| matches!(goal.progress,
        pumpkin_cluster::mob_continuation::MobGoalProgress::Tempt { .. })));
}

#[test]
fn disk_mob_genesis_preserves_numeric_state_and_rejects_unrepresented_data() {
    let (state, reference) = initial();
    let Value::Entity(body) = state.body.value else { panic!() };
    let identity = EntityOriginIdentity { id: body.id, uuid: body.birth.uuid, source: body.owner };
    let Target::Chunk { key: chunk, .. } = state.target else { panic!() };
    let mut root = pumpkin_nbt::compound::NbtCompound::new();
    root.put_string("id", "minecraft:cow".to_owned());
    root.put_uuid("UUID", identity.uuid);
    root.put_float("Health", 18.0);
    let mut attribute = pumpkin_nbt::compound::NbtCompound::new();
    attribute.put_string("id", pumpkin_data::attributes::Attributes::MAX_HEALTH.name.to_owned());
    attribute.put_double("base", 20.0);
    root.put_list("attributes", vec![pumpkin_nbt::tag::NbtTag::Compound(attribute)]);
    root.put_float("AbsorptionAmount", 2.5);
    root.put_short("Air", 42);
    root.put_int("TicksFrozen", 23);
    root.put_short("Fire", 17);
    root.put_short("HurtTime", 3);
    root.put_bool("NoAI", true);
    let decode = |root: &pumpkin_nbt::compound::NbtCompound| crate::server::cluster_owned_mob_disk::decode_bootstrap(
        &identity, reference, body.owner, &pumpkin_data::entity::EntityType::COW, chunk, body.spatial.clone(), root);
    let admission = decode(&root).unwrap();
    assert_eq!(admission.body.birth.uuid, identity.uuid);
    assert_eq!(admission.body.birth.action, reference);
    let EntityPayload::Living(living) = admission.body.payload else { panic!() };
    assert_eq!(living.combat.health_milli, 18000);
    assert_eq!(living.combat.absorption_milli, 2500);
    assert_eq!(living.environment.air_ticks, 42);
    assert_eq!(living.environment.frozen_ticks, 23);
    assert_eq!(living.combat.timing.fire_ticks, 17);
    assert_eq!(living.combat.timing.hurt_ticks, 3);
    assert!(living.mob.unwrap().brain.ai_disabled);
    assert_eq!(admission.inventory.cells.len(), 6);
    let mut unsupported = root.clone();
    unsupported.put_bool("NoGravity", true);
    assert_eq!(decode(&unsupported).unwrap_err(), ApplyFailure::InvalidAction);
    let mut malformed = root.clone();
    malformed.put_int("Health", 18);
    assert_eq!(decode(&malformed).unwrap_err(), ApplyFailure::InvalidAction);
    let mut unknown_attribute = root.clone();
    let mut attribute = pumpkin_nbt::compound::NbtCompound::new();
    attribute.put_string("id", "mod:unrepresented".to_owned());
    attribute.put_double("base", 1.0);
    unknown_attribute.put_list("attributes", vec![pumpkin_nbt::tag::NbtTag::Compound(attribute)]);
    assert_eq!(decode(&unknown_attribute).unwrap_err(), ApplyFailure::InvalidAction);
    let mut malformed_effect = root.clone();
    let mut effect = pumpkin_nbt::compound::NbtCompound::new();
    effect.put_string("id", "minecraft:regeneration".to_owned());
    effect.put_int("duration", 20);
    effect.put_int("amplifier", 256);
    malformed_effect.put_list("active_effects", vec![pumpkin_nbt::tag::NbtTag::Compound(effect)]);
    assert_eq!(decode(&malformed_effect).unwrap_err(), ApplyFailure::InvalidAction);
    root.put_float("Health", 0.0);
    root.put_short("DeathTime", 7);
    let EntityPayload::Living(living) = decode(&root).unwrap().body.payload else { panic!() };
    assert_eq!(living.combat.timing.death_cause, Some(reference));
    assert_eq!(living.combat.timing.death_ticks, 7);
    assert!(!living.combat.timing.death_inventory_processed);
    assert_eq!(living.combat.timing.last_damage_kind, None);
}

#[test]
fn goal_visibility_distinguishes_unavailable_history_from_a_real_obstruction() {
    use pumpkin_cluster::mob_goal::MobGoalEnvironment;
    let (mut state, reference) = initial();
    let Value::Entity(mut body) = state.body.value.clone() else { panic!() };
    let EntityPayload::Living(living) = &mut body.payload else { panic!() };
    living.combat.health_milli = 10_000;
    living.combat.timing.death_cause = None;
    state.body.value = Value::Entity((body.clone()).into());
    let Target::Chunk { key: chunk, .. } = state.target else { panic!() };
    let source = ContactObservation::entity(&body, chunk, &body.spatial, 0.45, 1.4,
        reference.tick, pumpkin_cluster::identity::PlayerSeq(1)).unwrap();
    let mut other = body.clone();
    other.id.local_id += 1;
    let EntityPayload::Living(other_living) = &mut other.payload else { panic!() };
    other_living.combat.actor = ActorId::Entity(other.id);
    other.birth.uuid = uuid::Uuid::from_u128(2);
    other.birth.action.id.sequence += 1;
    other.spatial_epoch = SpatialEpoch::Action(other.birth.action);
    other.spatial.position[0] = 2.5_f64.to_bits();
    let target = ContactObservation::entity(&other, chunk, &other.spatial, 0.45, 1.4,
        reference.tick, pumpkin_cluster::identity::PlayerSeq(1)).unwrap();
    state.inventory.insert(target.target(), Cell { revision: 0, value: Value::Entity((other).into()) });
    let contacts = super::super::cluster_owned_mob_contacts::MobContactIndex::new(vec![source.clone(), target.clone()]).unwrap();
    let chunks = [chunk].into_iter().collect();
    let draft = ActorActionDraft::new(source.actor(), body.id.generation, reference.tick, 16, 512);
    let EntityPayload::Living(living) = &body.payload else { panic!() };
    let rules = super::super::cluster_owned_mob_world::MobWorldRules {
        mob_griefing: true, difficulty: 2, minimum_y: -64, maximum_y: 319, sea_level: 63,
    };
    let environment = super::super::cluster_owned_mob_goal_environment::OwnedMobGoalEnvironment::new(
        &state, &draft, &body, living, &source, &contacts, &chunks, rules, false);
    assert!(!environment.visible(&target.identity));
    assert!(matches!(environment.finish(), Err(ApplyFailure::Condition(_))));
    for x in 0..=2 {
        state.inventory.insert(Target::Chunk { key: chunk,
            part: ChunkPart::Block(pumpkin_cluster::protocol::BlockPos { x, y: 65, z: 0 }), inventory: None },
            Cell { revision: 0, value: Value::Block(pumpkin_data::Block::AIR.default_state.id.as_u16()) });
    }
    let environment = super::super::cluster_owned_mob_goal_environment::OwnedMobGoalEnvironment::new(
        &state, &draft, &body, living, &source, &contacts, &chunks, rules, false);
    assert!(environment.visible(&target.identity));
    let mob = living.mob.as_ref().unwrap();
    let mut brain = mob.brain.clone();
    brain.goals = vec![pumpkin_cluster::mob_continuation::MobGoal {
        selector: pumpkin_cluster::mob_continuation::MobGoalSelector::Behavior,
        every_tick: true, stoppable: true, priority: 0, controls: 2, running: true,
        progress: pumpkin_cluster::mob_continuation::MobGoalProgress::LookAt {
            target: Some(target.identity.clone()), remaining: 20, range: 8.0_f32.to_bits(),
            chance: 1.0_f32.to_bits(), kind: body.kind, forward: false,
        },
    }];
    let intents = pumpkin_cluster::mob_goal::tick_goals(&mut brain, &mob.reproduction, &environment);
    let expected_y = f64::from_bits(target.position[1]) + f64::from(pumpkin_data::entity::EntityType::COW.eye_height);
    assert!(intents.iter().any(|intent| matches!(intent, pumpkin_cluster::mob_goal::MobIntent::LookAt(position)
        if f64::from_bits(position[1]) == expected_y)));
    environment.finish().unwrap();
    state.inventory.insert(Target::Chunk { key: chunk,
        part: ChunkPart::Block(pumpkin_cluster::protocol::BlockPos { x: 1, y: 65, z: 0 }), inventory: None },
        Cell { revision: 1, value: Value::Block(pumpkin_data::Block::STONE.default_state.id.as_u16()) });
    let environment = super::super::cluster_owned_mob_goal_environment::OwnedMobGoalEnvironment::new(
        &state, &draft, &body, living, &source, &contacts, &chunks, rules, false);
    assert!(!environment.visible(&target.identity));
    environment.finish().unwrap();
}

#[test]
fn diagonal_mob_visibility_uses_ray_length_instead_of_enclosing_volume() {
    let (mut state, reference) = initial();
    let Target::Chunk { key: chunk, .. } = state.target else { panic!() };
    for x in 0..=35 {
        for y in 65..=100 {
            for z in 0..=35 {
                state.inventory.insert(Target::Chunk {
                    key: pumpkin_cluster::protocol::ChunkKey::new(chunk.world, x / 16, z / 16),
                    part: ChunkPart::Block(pumpkin_cluster::protocol::BlockPos { x, y, z }), inventory: None,
                }, Cell { revision: 0, value: Value::Block(pumpkin_data::Block::AIR.default_state.id.as_u16()) });
            }
        }
    }
    let chunks = (0..=2).flat_map(|x| (0..=2).map(move |z|
        pumpkin_cluster::protocol::ChunkKey::new(chunk.world, x, z))).collect();
    let Value::Entity(body) = &state.body.value else { panic!() };
    let draft = ActorActionDraft::new(ActorId::Entity(body.id), body.id.generation, reference.tick, 16, 512);
    let from = [0.5, 65.19, 0.5];
    let to = [35.5, 100.19, 35.5];
    let world = super::super::cluster_owned_mob_world::MobWorldView::new(&state, &draft, chunk.world, &chunks);
    assert!(world.visible_between(from, to, 128).unwrap());
    state.inventory.insert(Target::Chunk { key: pumpkin_cluster::protocol::ChunkKey::new(chunk.world, 1, 1),
        part: ChunkPart::Block(pumpkin_cluster::protocol::BlockPos { x: 18, y: 83, z: 18 }), inventory: None },
        Cell { revision: 1, value: Value::Block(pumpkin_data::Block::STONE.default_state.id.as_u16()) });
    let world = super::super::cluster_owned_mob_world::MobWorldView::new(&state, &draft, chunk.world, &chunks);
    assert!(!world.visible_between(from, to, 128).unwrap());
}

#[test]
fn typed_mob_birth_rejects_unrepresented_nbt_and_malformed_spatial_fields() {
    let (state, _) = initial();
    let Value::Entity(body) = state.body.value else { panic!() };
    let parse = |root: &pumpkin_nbt::compound::NbtCompound| super::super::cluster_owned_mob_initial::parse_initialization(
        &pumpkin_data::entity::EntityType::COW, &body.spatial, root);
    let mut root = pumpkin_nbt::compound::NbtCompound::new();
    root.put_float("Health", 8.0);
    assert_eq!(parse(&root).unwrap().health_milli, Some(8_000));
    root.put_bool("Silent", true);
    assert_eq!(parse(&root).unwrap_err(), ApplyFailure::InvalidAction);
    let mut root = pumpkin_nbt::compound::NbtCompound::new();
    root.put_list("Motion", vec![pumpkin_nbt::tag::NbtTag::Double(0.0)]);
    assert_eq!(parse(&root).unwrap_err(), ApplyFailure::InvalidAction);
    root.put_list("Motion", vec![pumpkin_nbt::tag::NbtTag::Double(0.0); 3]);
    assert!(parse(&root).is_ok());
    root.put_int("OnGround", 1);
    assert_eq!(parse(&root).unwrap_err(), ApplyFailure::InvalidAction);
    let mut root = pumpkin_nbt::compound::NbtCompound::new();
    root.put_int("UUID", 1);
    assert_eq!(parse(&root).unwrap_err(), ApplyFailure::InvalidAction);
    let mut root = pumpkin_nbt::compound::NbtCompound::new();
    root.put_list("Inventory", Vec::new());
    assert_eq!(parse(&root).unwrap_err(), ApplyFailure::InvalidAction);
}

#[test]
fn mob_death_loot_uses_exact_cause_and_bounded_deterministic_outputs() {
    let (state, cause) = initial();
    let Value::Entity(body) = state.body.value else { panic!() };
    let params = crate::world::loot::LootContextParameters {
        this_entity: Some(&pumpkin_data::entity::EntityType::COW), killed_by_player: Some(true),
        ..Default::default()
    };
    let evaluate = |enabled, capacity| super::super::cluster_owned_mob_death::death_loot_facts(
        &body, cause, 7, enabled, &params, capacity);
    let facts = evaluate(true, 64).unwrap();
    assert!(!facts.items.is_empty());
    assert_eq!(facts.items, evaluate(true, 64).unwrap().items);
    assert_eq!(facts.experience, pumpkin_data::entity::EntityType::COW.experience_reward);
    let disabled = evaluate(false, 0).unwrap();
    assert!(disabled.items.is_empty());
    assert_eq!(disabled.experience, 0);
    assert!(evaluate(true, 0).is_err());
    let mut wrong = cause;
    wrong.id.sequence += 1;
    assert!(super::super::cluster_owned_mob_death::death_loot_facts(&body, wrong, 7, true, &params, 64).is_err());
}

#[test]
fn natural_spawn_floor_requires_original_cells_and_respects_head_collision() {
    let position = pumpkin_cluster::protocol::BlockPos { x: 0, y: 64, z: 0 };
    let below = pumpkin_cluster::protocol::BlockPos { y: 63, ..position };
    let above = pumpkin_cluster::protocol::BlockPos { y: 65, ..position };
    let mut cells = BTreeMap::from([(below, pumpkin_data::Block::STONE.default_state),
        (position, pumpkin_data::Block::AIR.default_state)]);
    let world = WorldId::new(0, 0);
    let evaluate = |cells: &BTreeMap<_, _>| super::super::cluster_owned_mob_spawn::spawn_position_ok(
        &pumpkin_data::entity::EntityType::COW, position, true, |at| cells.get(&at).copied()
            .ok_or(ApplyFailure::Condition(Target::Chunk {
                key: pumpkin_cluster::protocol::ChunkKey::new(world, 0, 0),
                part: ChunkPart::Block(at), inventory: None })));
    assert!(matches!(evaluate(&cells), Err(ApplyFailure::Condition(_))));
    cells.insert(above, pumpkin_data::Block::AIR.default_state);
    assert!(evaluate(&cells).unwrap());
    cells.insert(above, pumpkin_data::Block::STONE.default_state);
    assert!(!evaluate(&cells).unwrap());
}

#[test]
fn living_query_nbt_preserves_represented_timers_and_mob_flags_for_disk_decode() {
    let (state, reference) = initial();
    let Value::Entity(mut body) = state.body.value else { panic!() };
    let EntityPayload::Living(living) = &mut body.payload else { panic!() };
    living.environment.air_ticks = 42;
    living.environment.frozen_ticks = 17;
    living.combat.timing.fire_ticks = 9;
    living.combat.timing.hurt_ticks = 3;
    living.combat.timing.death_ticks = 7;
    let mob = living.mob.as_mut().unwrap();
    mob.brain.ai_disabled = true;
    mob.reproduction.growth_age = -1200;
    let cells = state.inventory.into_values().filter_map(|cell| match cell.value {
        Value::Inventory(cell) => Some(cell), _ => None,
    }).collect::<Vec<_>>();
    let mut root = pumpkin_nbt::compound::NbtCompound::new();
    root.put_string("id", "minecraft:cow".to_owned());
    root.put_uuid("UUID", body.birth.uuid);
    super::super::cluster_owned_mob_nbt::write_living_nbt(&body, &mut root, Some(&cells)).unwrap();
    let identity = EntityOriginIdentity { id: body.id, uuid: body.birth.uuid, source: body.birth.source };
    let Target::Chunk { key: chunk, .. } = state.target else { panic!() };
    let decoded = super::super::cluster_owned_mob_disk::decode_bootstrap(&identity, reference,
        body.owner, &pumpkin_data::entity::EntityType::COW, chunk, body.spatial, &root).unwrap();
    let EntityPayload::Living(living) = decoded.body.payload else { panic!() };
    assert_eq!(living.environment.air_ticks, 42);
    assert_eq!(living.environment.frozen_ticks, 17);
    assert_eq!(living.combat.timing.fire_ticks, 9);
    assert_eq!(living.combat.timing.hurt_ticks, 3);
    assert_eq!(living.combat.timing.death_ticks, 7);
    assert_eq!(living.combat.timing.death_cause, Some(reference));
    let mob = living.mob.unwrap();
    assert!(mob.brain.ai_disabled);
    assert_eq!(mob.reproduction.growth_age, -1200);
}

#[test]
fn normal_horse_mount_uses_winning_saddle_slot_and_allows_untamed_empty_hand() {
    use pumpkin_cluster::inventory::{InventoryCell, InvLoc, INV_MAIN};
    let (state, reference) = initial();
    let Value::Entity(source) = state.body.value else { panic!() };
    let identity = EntityOriginIdentity { id: source.id, uuid: source.birth.uuid, source: source.owner };
    let mut root = pumpkin_nbt::compound::NbtCompound::new();
    root.put_float("Health", 4.0);
    let initial = super::super::cluster_owned_mob_initial::parse_initialization(
        &pumpkin_data::entity::EntityType::HORSE, &source.spatial, &root).unwrap();
    let (mut body, _) = super::super::cluster_owned_mob_initial::prepare_typed_body(&identity,
        reference, source.owner, &pumpkin_data::entity::EntityType::HORSE, source.spatial, &initial).unwrap();
    let held = InventoryCell::empty(InvLoc::new(INV_MAIN, 0));
    let empty = InventoryCell::empty(InvLoc::new(pumpkin_cluster::entity_inventory_admission::INV_EQUIPMENT, 7));
    assert!(super::super::cluster_owned_mob_initial::horse_mount_eligible(&body, &held, &empty).unwrap());
    let saddle = crate::server::cluster_invsee::inventory_cell_from_item_stack(empty.loc,
        &pumpkin_data::item_stack::ItemStack::static_new_java(1, &pumpkin_data::item::Item::SADDLE)).unwrap();
    let saddle_held = crate::server::cluster_invsee::inventory_cell_from_item_stack(held.loc,
        &pumpkin_data::item_stack::ItemStack::static_new_java(1, &pumpkin_data::item::Item::SADDLE)).unwrap();
    let EntityPayload::Living(living) = &mut body.payload else { panic!() };
    let pumpkin_cluster::mob_continuation::MobSpecies::Horse { flags, .. } = &mut living.mob.as_mut().unwrap().species else { panic!() };
    *flags = 2 | 4;
    assert!(!super::super::cluster_owned_mob_initial::horse_mount_eligible(&body, &saddle_held, &empty).unwrap());
    assert!(super::super::cluster_owned_mob_initial::horse_mount_eligible(&body, &saddle_held, &saddle).unwrap());
    let EntityPayload::Living(living) = &mut body.payload else { panic!() };
    living.mob.as_mut().unwrap().reproduction.growth_age = -1;
    assert!(!super::super::cluster_owned_mob_initial::horse_mount_eligible(&body, &held, &saddle).unwrap());
}
