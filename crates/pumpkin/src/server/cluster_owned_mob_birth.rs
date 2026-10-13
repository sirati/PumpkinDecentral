use std::collections::BTreeMap;

use pumpkin_cluster::actor::ActorId;
use pumpkin_cluster::canonical_action::{ActionId, ActionRef, ApplyFailure, SpatialEpoch};
use pumpkin_cluster::entity_body::{EntityBirth, EntityBody, EntityPayload, EntitySpatial};
use pumpkin_cluster::item_action::{
    ActorCombatState, ActorFoodState, ActorLivingState, CombatTiming,
};
use pumpkin_cluster::mob_continuation::MobContinuation;
use pumpkin_cluster::protocol::{ChunkKey, GlobalEntityId};
use pumpkin_cluster::realm_completion::PeerGeneration;
use pumpkin_cluster::time::TickStamp;
use pumpkin_data::entity::EntityType;

pub(super) struct MobBirthInput {
    pub tags: std::collections::BTreeSet<String>,
    pub entity: GlobalEntityId,
    pub action: ActionId,
    pub owner: PeerGeneration,
    pub tick: TickStamp,
    pub uuid: uuid::Uuid,
    pub kind: EntityType,
    pub spatial: EntitySpatial,
    pub continuation: MobContinuation,
}

pub(crate) fn inventory_layout(
    body: &EntityBody,
) -> Result<Vec<pumpkin_cluster::inventory::InvLoc>, ApplyFailure> {
    use pumpkin_cluster::entity_body::VehicleKind;
    use pumpkin_cluster::entity_inventory_admission::INV_EQUIPMENT;
    use pumpkin_cluster::inventory::{INV_MAIN, InvLoc};
    use pumpkin_cluster::mob_continuation::MobSpecies;
    use pumpkin_data::tag::Taggable;
    let kind = EntityType::from_raw(body.kind).ok_or(ApplyFailure::InvalidAction)?;
    let mut layout = Vec::new();
    let storage = match &body.payload {
        EntityPayload::Living(living) => {
            if !kind.mob && kind.resource_name != "armor_stand" {
                return Err(ApplyFailure::InvalidAction);
            }
            layout.extend((0..6).map(|slot| InvLoc::new(INV_EQUIPMENT, slot)));
            if kind
                .is_tagged_with("minecraft:can_wear_horse_armor")
                .unwrap_or(false)
                || kind
                    .is_tagged_with("minecraft:can_wear_nautilus_armor")
                    .unwrap_or(false)
                || kind
                    .is_tagged_with("minecraft:can_equip_harness")
                    .unwrap_or(false)
                || matches!(kind.resource_name, "wolf" | "llama" | "trader_llama")
            {
                layout.push(InvLoc::new(INV_EQUIPMENT, 6));
            }
            if kind
                .is_tagged_with("minecraft:can_equip_saddle")
                .unwrap_or(false)
            {
                layout.push(InvLoc::new(INV_EQUIPMENT, 7));
            }
            match living.mob.as_ref().map(|mob| &mob.species) {
                Some(MobSpecies::Horse {
                    kind:
                        pumpkin_cluster::mob_continuation::HorseSpeciesKind::Donkey
                        | pumpkin_cluster::mob_continuation::HorseSpeciesKind::Mule,
                    ..
                }) => 15,
                Some(MobSpecies::Llama { .. }) => 15,
                _ if matches!(kind.resource_name, "piglin" | "villager") => 8,
                _ => 0,
            }
        }
        EntityPayload::Vehicle(vehicle) => match vehicle.kind {
            VehicleKind::ChestBoat | VehicleKind::ChestMinecart => 27,
            VehicleKind::HopperMinecart => 5,
            _ => 0,
        },
        EntityPayload::Object(object) => {
            if matches!(
                object.kind,
                pumpkin_cluster::object_state::ObjectKind::ItemFrame(_)
            ) {
                1
            } else {
                0
            }
        }
        _ => 0,
    };
    layout.extend((0..storage).map(|slot| InvLoc::new(INV_MAIN, slot)));
    Ok(layout)
}

pub(crate) fn inventory_slot_limit(
    body: &EntityBody,
    stack: &pumpkin_data::item_stack::ItemStack,
    loc: pumpkin_cluster::inventory::InvLoc,
) -> Result<u8, ApplyFailure> {
    use pumpkin_cluster::entity_inventory_admission::INV_EQUIPMENT;
    use pumpkin_data::data_component_impl::EquipmentSlot;
    if !inventory_layout(body)?.contains(&loc) {
        return Err(ApplyFailure::InvalidAction);
    }
    if matches!(&body.payload, EntityPayload::Object(object) if matches!(object.kind, pumpkin_cluster::object_state::ObjectKind::ItemFrame(_)))
    {
        return Ok(1);
    }
    if loc.inv != INV_EQUIPMENT || loc.slot < 2 {
        return Ok(stack.get_max_stack_size());
    }
    let kind = EntityType::from_raw(body.kind).ok_or(ApplyFailure::InvalidAction)?;
    let slot = match loc.slot {
        2 => EquipmentSlot::FEET,
        3 => EquipmentSlot::LEGS,
        4 => EquipmentSlot::CHEST,
        5 => EquipmentSlot::HEAD,
        6 => EquipmentSlot::BODY,
        7 => EquipmentSlot::SADDLE,
        _ => return Err(ApplyFailure::InvalidAction),
    };
    if !crate::entity::mob::equipment::is_equippable_in_slot(kind, stack, &slot) {
        return Err(ApplyFailure::InvalidAction);
    }
    Ok(1)
}

pub(crate) fn paired_admission(
    mut body: EntityBody,
    chunk: ChunkKey,
    imported_revision: u64,
    supplied: Vec<pumpkin_cluster::inventory::InventoryCell>,
) -> Result<pumpkin_cluster::mob_breeding::MobBirthAdmission, ApplyFailure> {
    use pumpkin_cluster::canonical_action::{Cell, Value};
    let layout = inventory_layout(&body)?;
    let supplied_count = supplied.len();
    let mut supplied = supplied
        .into_iter()
        .map(|cell| (cell.loc, cell))
        .collect::<BTreeMap<_, _>>();
    if supplied.len() != supplied_count
        || supplied.keys().any(|location| !layout.contains(location))
    {
        return Err(ApplyFailure::InvalidAction);
    }
    let cells = layout
        .into_iter()
        .map(|location| Cell {
            revision: 0,
            value: Value::Inventory(
                supplied
                    .remove(&location)
                    .unwrap_or_else(|| pumpkin_cluster::inventory::InventoryCell::empty(location)),
            ),
        })
        .collect();
    let inventory = pumpkin_cluster::entity_inventory_admission::EntityInventoryAdmission {
        imported_revision,
        cells,
    };
    if !inventory.valid() {
        return Err(ApplyFailure::InvalidAction);
    }
    if let EntityPayload::Living(living) = &body.payload {
        let accessible = match living.mob.as_ref().map(|mob| &mob.species) {
            Some(pumpkin_cluster::mob_continuation::MobSpecies::Horse { chested, .. }) => {
                if *chested {
                    15
                } else {
                    0
                }
            }
            Some(pumpkin_cluster::mob_continuation::MobSpecies::Llama {
                chested,
                strength,
                ..
            }) => {
                if *chested {
                    u16::from(*strength) * 3
                } else {
                    0
                }
            }
            _ => u16::MAX,
        };
        if inventory.cells.iter().any(|cell| matches!(&cell.value, Value::Inventory(item) if item.loc.inv == pumpkin_cluster::inventory::INV_MAIN && item.loc.slot >= accessible && item.count != 0)) { return Err(ApplyFailure::InvalidAction); }
    }
    if matches!(body.payload, EntityPayload::Living(_)) {
        use pumpkin_data::data_component_impl::EquipmentSlot;
        let roles = [
            EquipmentSlot::MAIN_HAND,
            EquipmentSlot::OFF_HAND,
            EquipmentSlot::FEET,
            EquipmentSlot::LEGS,
            EquipmentSlot::CHEST,
            EquipmentSlot::HEAD,
            EquipmentSlot::BODY,
            EquipmentSlot::SADDLE,
        ];
        let mut gear = Vec::new();
        for cell in &inventory.cells {
            let Value::Inventory(item) = &cell.value else {
                return Err(ApplyFailure::InvalidAction);
            };
            if item.loc.inv != pumpkin_cluster::entity_inventory_admission::INV_EQUIPMENT
                || item.count == 0
            {
                continue;
            }
            let stack = super::cluster_invsee::item_stack_from_inventory_cell(item)
                .map_err(|_| ApplyFailure::InvalidAction)?;
            if item.count > inventory_slot_limit(&body, &stack, item.loc)? {
                return Err(ApplyFailure::InvalidAction);
            }
            let role = roles
                .get(usize::from(item.loc.slot))
                .ok_or(ApplyFailure::InvalidAction)?;
            gear.push((role, stack));
        }
        let mut facts = crate::entity::combat::normalized_equipment(
            gear.iter().map(|(role, stack)| (*role, stack)),
        )
        .ok_or(ApplyFailure::InvalidAction)?;
        let EntityPayload::Living(living) = &mut body.payload else {
            return Err(ApplyFailure::InvalidAction);
        };
        let active = pumpkin_cluster::active_effects::ActiveEffectSet {
            visible: living.effects.clone(), hidden: living.hidden_effects.clone() };
        facts.modifiers.extend(super::cluster_item_status::modifiers(&active)?);
        living.combat.equipment = facts;
        let attribute = pumpkin_data::attributes::Attributes::MAX_HEALTH;
        let base = living.combat.effective_attributes.get(&u16::from(attribute.id))
            .copied().map(f64::from_bits).unwrap_or(attribute.default_value);
        let maximum = living.attribute(u16::from(attribute.id), base);
        if !maximum.is_finite() || !(0.0..=f64::from(u32::MAX) / 1000.0).contains(&maximum) {
            return Err(ApplyFailure::InvalidAction);
        }
        living.combat.health_milli = living.combat.health_milli.min((maximum * 1000.0).round() as u32);
        if living.combat.health_milli == 0 && living.combat.timing.death_cause.is_none() {
            living.combat.timing.death_cause = Some(body.birth.action);
            living.combat.timing.death_ticks = 0;
        }
    }
    if !body.belongs_to(body.id, chunk) || !body.valid_at(body.birth.action.tick) {
        return Err(ApplyFailure::InvalidAction);
    }
    Ok(pumpkin_cluster::mob_breeding::MobBirthAdmission { body, chunk, inventory })
}

pub(crate) fn admission_action(
    body: EntityBody,
    chunk: ChunkKey,
    imported_revision: u64,
    supplied: Vec<pumpkin_cluster::inventory::InventoryCell>,
) -> Result<pumpkin_cluster::canonical_action::Action, ApplyFailure> {
    use pumpkin_cluster::canonical_action::{Action, ActionClass, ActionKind, ChunkPart,
        ClientOrigin, Effect, Stakeholder, Target, UndoProcedure};
    let pumpkin_cluster::mob_breeding::MobBirthAdmission { body, inventory, .. } =
        paired_admission(body, chunk, imported_revision, supplied)?;
    let action = Action {
        edit_authority: None,
        id: body.birth.action.id,
        tick: body.birth.action.tick,
        class: ActionClass::Infallible,
        chunk_transition: false,
        actor_chunk: Some(chunk),
        player_leases: Vec::new(),
        player_chunks: Vec::new(),
        stakeholders: vec![Stakeholder::Chunk(chunk)],
        dependencies: Vec::new(),
        conditions: Vec::new(),
        effects: vec![Effect::EntityAdmission {
            target: Target::Chunk {
                key: chunk,
                part: ChunkPart::Entity(body.id),
                inventory: None,
            },
            body,
            inventory,
        }],
        counterfactual_conditions: Vec::new(),
        counterfactual_effects: Vec::new(),
        semantic_intent: None,
        factual_result: None,
        undo: vec![UndoProcedure::RestoreWrittenTargets],
        presentation: Vec::new(),
        kind: ActionKind::Actor,
        client_origin: ClientOrigin::None,
    };
    action.validate()?;
    Ok(action)
}

pub(super) fn prepare_body(
    input: MobBirthInput,
    chunk: ChunkKey,
) -> Result<EntityBody, ApplyFailure> {
    let actor = ActorId::Entity(input.entity);
    if input.entity.origin != input.owner.server
        || input.entity.generation != input.owner.generation
        || input.owner.generation == 0
        || !input.kind.mob
        || !super::cluster_owned_mob_metadata::species_matches_kind(
            &input.continuation.species,
            input.kind.id,
        )
        || !input.continuation.valid()
    {
        return Err(ApplyFailure::InvalidAction);
    }
    let mut living = inherited_living(actor, &input.kind)?;
    living.mob = Some(input.continuation);
    living.combat.velocity_micro = input.spatial.velocity_micro;
    let reference = ActionRef {
        id: input.action,
        tick: input.tick,
    };
    let body = EntityBody { mount: None,
        tags: input.tags,
        tags_revision: 1,
        relation_versions: BTreeMap::new(),
        passengers: BTreeMap::new(),
        id: input.entity,
        birth: EntityBirth {
            action: reference,
            source: input.owner,
            uuid: input.uuid,
        },
        owner: input.owner,
        kind: input.kind.id,
        spatial: input.spatial,
        spatial_epoch: SpatialEpoch::Action(reference),
        payload: EntityPayload::Living(living),
    };
    if !super::cluster_world_canonical::valid_entity_body(&body, chunk, input.entity) {
        return Err(ApplyFailure::InvalidAction);
    }
    Ok(body)
}

pub(super) fn inherited_living(
    actor: ActorId,
    kind: &EntityType,
) -> Result<ActorLivingState, ApplyFailure> {
    if !matches!(actor, ActorId::Entity(_))
        || !kind.attributes.iter().any(|(attribute, _)| {
            attribute.id == pumpkin_data::attributes::Attributes::MAX_HEALTH.id
        })
    {
        return Err(ApplyFailure::InvalidAction);
    }
    let attributes = kind
        .attributes
        .iter()
        .map(|(attribute, value)| (u16::from(attribute.id), value.to_bits()))
        .collect::<BTreeMap<_, _>>();
    let maximum = kind
        .attributes
        .iter()
        .find(|(attribute, _)| attribute.id == pumpkin_data::attributes::Attributes::MAX_HEALTH.id)
        .map(|(_, value)| *value)
        .ok_or(ApplyFailure::InvalidAction)?;
    if !maximum.is_finite() || !(0.0..=u32::MAX as f64 / 1000.0).contains(&maximum) {
        return Err(ApplyFailure::InvalidAction);
    }
    Ok(ActorLivingState {
        permanent_attribute_modifiers: Default::default(),
        menu: Default::default(),
        environment: Default::default(),
        tags: std::collections::BTreeSet::new(),
        tags_revision: 1,
        advancements: Default::default(),
        stand: None,
        mob: None,
        merchant: None,
        simulation_tick: None,
        effects: Vec::new(),
        hidden_effects: BTreeMap::new(),
        property_versions: BTreeMap::new(),
        food: ActorFoodState {
            actor,
            exhaustion_milli: 0,
            tick_timer: 0,
            food: 20,
            saturation_milli: 5000,
            revision: 1,
        },
        combat: ActorCombatState {
            actor,
            revision: 1,
            health_milli: (maximum * 1000.0).round() as u32,
            absorption_milli: 0,
            velocity_micro: [0; 3],
            timing: CombatTiming::default(),
            effective_attributes: attributes,
            equipment: Default::default(),
            status: Default::default(),
        },
        use_state: None,
        cooldowns: BTreeMap::new(),
        avatar: None,
    })
}
