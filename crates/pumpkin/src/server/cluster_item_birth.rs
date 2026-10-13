use pumpkin_cluster::canonical_action::{Action, ActionRef};
use pumpkin_cluster::entity_body::{
    DroppedItem, EntityBirth, EntityBody, EntityPayload, EntitySpatial,
};
use pumpkin_cluster::inventory::InvLoc;
use pumpkin_cluster::item_lifecycle::{ItemLifecycleFailure, append_birth};
use pumpkin_cluster::protocol::{ChunkKey, GlobalEntityId};
use pumpkin_cluster::realm_completion::PeerGeneration;
use pumpkin_data::data_component_impl::{DamageResistantImpl, DamageResistantType};
use pumpkin_data::entity::EntityType;
use pumpkin_data::item_stack::ItemStack;

pub(crate) struct ReservedItemBirth {
    pub id: GlobalEntityId,
    pub uuid: uuid::Uuid,
}

pub(super) fn project_drop_template(template: &EntityBody, source: &pumpkin_cluster::inventory::InventoryCell)
    -> Result<EntityBody, pumpkin_cluster::canonical_action::ApplyFailure> {
    use pumpkin_cluster::canonical_action::ApplyFailure;
    if source.is_empty() || source.count > 99 { return Err(ApplyFailure::InvalidAction); }
    let stack = super::cluster_invsee::item_stack_from_inventory_cell(source).map_err(|_| ApplyFailure::InvalidAction)?;
    let mut body = template.clone();
    let EntityPayload::Item(item) = &mut body.payload else { return Err(ApplyFailure::InvalidAction); };
    if item.age != 0 || item.pickup_delay != 40 { return Err(ApplyFailure::InvalidAction); }
    item.item = source.item;
    item.count = source.count;
    item.components = source.nbt.clone();
    item.fire_immune = stack.get_data_component::<DamageResistantImpl>()
        .is_some_and(|resistance| resistance.res_type == DamageResistantType::Fire);
    Ok(body)
}

pub(crate) fn reserved_item_body(
    action: ActionRef, identity: &pumpkin_cluster::entity_origin::EntityOriginIdentity,
    primary: PeerGeneration, spatial: EntitySpatial, cell: &pumpkin_cluster::inventory::InventoryCell,
    pickup_delay: u16,
) -> Result<EntityBody, ItemLifecycleFailure> {
    if identity.source.generation == 0 || identity.id.origin != identity.source.server
        || identity.id.generation != identity.source.generation || identity.uuid.is_nil() || primary.generation == 0
        || cell.item == 0 || cell.count == 0 || cell.count > 99 {
        return Err(ItemLifecycleFailure::InvalidEntity);
    }
    let stack = super::cluster_invsee::item_stack_from_inventory_cell(cell).map_err(|_| ItemLifecycleFailure::InvalidChange)?;
    let fire_immune = stack.get_data_component::<DamageResistantImpl>()
        .is_some_and(|resistance| resistance.res_type == DamageResistantType::Fire);
    Ok(EntityBody { mount: None, id: identity.id, birth: EntityBirth { action, source: identity.source, uuid: identity.uuid },
        owner: primary, kind: EntityType::ITEM.id, spatial, spatial_epoch: pumpkin_cluster::canonical_action::SpatialEpoch::Action(action),
        tags: Default::default(), tags_revision: 1, relation_versions: Default::default(), passengers: Default::default(),
        payload: EntityPayload::Item(DroppedItem { item: cell.item, count: cell.count, components: cell.nbt.clone(),
            age: 0, last_lifetime_tick: None, pickup_delay, health_milli: 5_000,
            never_despawn: false, never_pickup: false, fire_immune }) })
}

pub(crate) fn reserved_experience_body(
    action: ActionRef, identity: &pumpkin_cluster::entity_origin::EntityOriginIdentity,
    primary: PeerGeneration, spatial: EntitySpatial, amount: u32,
) -> Result<EntityBody, ItemLifecycleFailure> {
    if identity.source.generation == 0 || identity.id.origin != identity.source.server
        || identity.id.generation != identity.source.generation || identity.uuid.is_nil() || primary.generation == 0
        || amount == 0 || amount > i32::MAX as u32 { return Err(ItemLifecycleFailure::InvalidEntity); }
    Ok(EntityBody { mount: None, id: identity.id, birth: EntityBirth { action, source: identity.source, uuid: identity.uuid },
        owner: primary, kind: EntityType::EXPERIENCE_ORB.id, spatial,
        spatial_epoch: pumpkin_cluster::canonical_action::SpatialEpoch::Action(action),
        tags: Default::default(), tags_revision: 1, relation_versions: Default::default(), passengers: Default::default(),
        payload: EntityPayload::Experience(pumpkin_cluster::entity_body::ExperienceOrbState {
            amount, count: 1, health_milli: 5_000, age: 0, last_lifetime_tick: None }) })
}

pub(crate) fn append_projectile_birth(
    action: &mut Action,
    effect: &pumpkin_cluster::item_action::ItemProjectileEffect,
    source: PeerGeneration,
    uuid: uuid::Uuid,
    shooter_birth: Option<EntityBirth>,
    shooter_lease: Option<pumpkin_cluster::canonical_action::LivePlayerLease>,
) -> Result<(), ItemLifecycleFailure> {
    if effect.spawn.id.origin != source.server || effect.spawn.id.generation != source.generation || source.generation == 0 || uuid.is_nil() {
        return Err(ItemLifecycleFailure::InvalidEntity);
    }
    if effect
        .spawn
        .position
        .iter()
        .any(|value| !value.is_finite() || value.abs() > 30_000_000.0)
        || effect
            .spawn
            .velocity
            .iter()
            .any(|value| !value.is_finite() || value.abs() > 100.0)
    {
        return Err(ItemLifecycleFailure::InvalidChange);
    }
    let position = effect.spawn.position;
    let [vx, vy, vz] = effect.spawn.velocity;
    let yaw = (-vx).atan2(vz).to_degrees() as f32;
    let pitch = (-vy).atan2(vx.hypot(vz)).to_degrees() as f32;
    let chunk = ChunkKey::new(
        effect.spawn.world,
        (position[0].floor() as i32).div_euclid(16),
        (position[2].floor() as i32).div_euclid(16),
    );
    let mut carried_item = effect.ammo_before.clone();
    carried_item.count = 1;
    append_birth(
        action,
        EntityBody { mount: None,
            tags: Default::default(),
            tags_revision: 1,
            relation_versions: Default::default(),
            passengers: Default::default(),
            id: effect.spawn.id,
            birth: EntityBirth {
                action: ActionRef {
                    tick: action.tick,
                    id: action.id,
                },
                source,
                uuid,
            },
            owner: source,
            kind: EntityType::ARROW.id,
            spatial: EntitySpatial {
                world: effect.spawn.world,
                position: position.map(f64::to_bits),
                velocity_micro: effect
                    .spawn
                    .velocity
                    .map(|velocity| (velocity * 1_000_000.0).round() as i64),
                yaw: yaw.to_bits(),
                pitch: pitch.to_bits(),
                on_ground: false,
            },
            spatial_epoch: pumpkin_cluster::canonical_action::SpatialEpoch::Action(ActionRef {
                tick: action.tick,
                id: action.id,
            }),
            payload: EntityPayload::Projectile(pumpkin_cluster::entity_body::ProjectileBody { last_simulation_tick: None, embedded_age: 0,
                fire_ticks: 0,
                knockback_level: 0,
                kind: pumpkin_cluster::entity_body::ProjectileKind::Arrow,
                shooter: action.id.actor,
                shooter_birth,
                shooter_lease,
                shot_tick: action.tick,
                carried_item,
                damage_milli: 2000,
                critical: effect.kind == pumpkin_cluster::item_action::ItemProjectileKind::Bow
                    && effect.power_milli == 1000,
                pierce: 0,
                age: 0,
                embedded: None,
                hit_actors: Vec::new(),
                pickup_allowed: true,
                pickup_creative_only: false,
            }),
        },
        chunk,
    )
}

pub(crate) fn append_break_births(
    action: &mut Action,
    chunk: ChunkKey,
    output: &crate::block::neighbor::NaturalBreakResult,
    source: PeerGeneration,
    primary: PeerGeneration,
    reserved: &[ReservedItemBirth],
) -> Result<(), ItemLifecycleFailure> {
    let facts = crate::world::loot::partition_loot_facts(&output.items, reserved.len())
        .ok_or(ItemLifecycleFailure::InvalidChange)?;
    let expected = facts.len() + usize::from(output.experience != 0);
    if source.generation == 0
        || primary.generation == 0
        || expected != reserved.len()
    {
        return Err(ItemLifecycleFailure::InvalidChange);
    }
    let position = [
        f64::from(output.position.0.x) + 0.5,
        f64::from(output.position.0.y) + 0.5,
        f64::from(output.position.0.z) + 0.5,
    ]
    .map(f64::to_bits);
    let mut prepared = action.clone();
    for (index, reservation) in reserved.iter().enumerate() {
        if reservation.id.origin != source.server || reservation.id.generation != source.generation || reservation.uuid.is_nil() {
            return Err(ItemLifecycleFailure::InvalidEntity);
        }
        let (kind, payload) = if let Some(fact) = facts.get(index) {
            let item = pumpkin_data::item::Item::from_id(fact.item)
                .ok_or(ItemLifecycleFailure::InvalidChange)?;
            let count =
                u8::try_from(fact.count).map_err(|_| ItemLifecycleFailure::InvalidChange)?;
            let stack = ItemStack::static_new_java(count, item);
            let cell = super::cluster_invsee::inventory_cell_from_item_stack(
                InvLoc { inv: 0, slot: 0 },
                &stack,
            )
            .map_err(|_| ItemLifecycleFailure::InvalidChange)?;
            let fire_immune = stack
                .get_data_component::<DamageResistantImpl>()
                .is_some_and(|resistance| resistance.res_type == DamageResistantType::Fire);
            (
                EntityType::ITEM.id,
                EntityPayload::Item(DroppedItem {
                    item: cell.item,
                    count: cell.count,
                    components: cell.nbt,
                    age: 0,
                    last_lifetime_tick: None,
                    pickup_delay: 10,
                    health_milli: 5000,
                    never_despawn: false,
                    never_pickup: false,
                    fire_immune,
                }),
            )
        } else {
            (
                EntityType::EXPERIENCE_ORB.id,
                EntityPayload::Experience(pumpkin_cluster::entity_body::ExperienceOrbState {
                    amount: output.experience,
                    count: 1,
                    health_milli: 5_000,
                    age: 0,
                    last_lifetime_tick: None,
                }),
            )
        };
        append_birth(
            &mut prepared,
            EntityBody { mount: None,
                tags: Default::default(),
                tags_revision: 1,
                relation_versions: Default::default(),
                passengers: Default::default(),
                id: reservation.id,
                birth: EntityBirth {
                    action: ActionRef {
                        tick: action.tick,
                        id: action.id,
                    },
                    source,
                    uuid: reservation.uuid,
                },
                owner: primary,
                spatial_epoch: pumpkin_cluster::canonical_action::SpatialEpoch::Action(ActionRef {
                    tick: action.tick,
                    id: action.id,
                }),
                kind,
                spatial: EntitySpatial {
                    world: chunk.world,
                    position,
                    velocity_micro: [0, 200_000, 0],
                    yaw: 0.0_f32.to_bits(),
                    pitch: 0.0_f32.to_bits(),
                    on_ground: false,
                },
                payload,
            },
            chunk,
        )?;
    }
    *action = prepared;
    Ok(())
}

pub(super) fn death_inventory_drop(template: &EntityBody, source: &pumpkin_cluster::inventory::InventoryCell)
    -> Result<Option<EntityBody>, pumpkin_cluster::canonical_action::ApplyFailure> {
    let stack = super::cluster_invsee::item_stack_from_inventory_cell(source)
        .map_err(|_| pumpkin_cluster::canonical_action::ApplyFailure::InvalidAction)?;
    if stack.get_enchantment_level(&pumpkin_data::Enchantment::VANISHING_CURSE) > 0 {
        return Ok(None);
    }
    project_drop_template(template, source).map(Some)
}

pub(super) fn death_inventory_intent(
    action: ActionRef,
    identity: pumpkin_cluster::combat_action::LivingTickIdentity,
    spatial: EntitySpatial,
    death_cause: ActionRef,
    rules: pumpkin_cluster::world_rules::WorldRulesWitness,
    keep_inventory: bool,
    sources: Vec<pumpkin_cluster::canonical_inventory::InventorySlotBinding>,
    equipment: Vec<pumpkin_cluster::canonical_inventory::InventoryEquipmentBinding>,
    entity_equipment: Option<pumpkin_cluster::canonical_inventory::EntityInventoryEquipmentBinding>,
    identities: &[pumpkin_cluster::entity_origin::EntityOriginIdentity],
    primary: PeerGeneration,
) -> Result<pumpkin_cluster::death_inventory::DeathInventoryIntent, pumpkin_cluster::canonical_action::ApplyFailure> {
    use pumpkin_cluster::canonical_action::{ApplyFailure, ChunkPart, Target};
    if identities.len() != sources.len() || action.id.actor != identity.actor()
        || identity.simulation_tick != action.tick || identities.iter().any(|reserved| reserved.source != identity.owner) {
        return Err(ApplyFailure::InvalidAction);
    }
    let at = spatial.position.map(f64::from_bits);
    let chunk = ChunkKey::new(spatial.world, (at[0].floor() as i32).div_euclid(16), (at[2].floor() as i32).div_euclid(16));
    let mut position = spatial.position.map(f64::from_bits);
    position[1] += 0.3;
    let mut drops = Vec::with_capacity(sources.len());
    for (source, identity) in sources.into_iter().zip(identities) {
        let stack = ItemStack::static_new_java(1, &pumpkin_data::item::Item::STONE);
        let cell = super::cluster_invsee::inventory_cell_from_item_stack(source.slot.loc, &stack)
            .map_err(|_| ApplyFailure::InvalidAction)?;
        let template = reserved_item_body(action, identity, primary, EntitySpatial { world: chunk.world,
            position: position.map(f64::to_bits), velocity_micro: [0, 200_000, 0], yaw: 0_f32.to_bits(),
            pitch: 0_f32.to_bits(), on_ground: false }, &cell, 40).map_err(|_| ApplyFailure::InvalidAction)?;
        drops.push(pumpkin_cluster::death_inventory::DeathInventoryDrop { source,
            destination: Target::Chunk { key: chunk, part: ChunkPart::Entity(identity.id), inventory: None }, template });
    }
    let intent = pumpkin_cluster::death_inventory::DeathInventoryIntent { identity, spatial, death_cause, rules, keep_inventory, drops, equipment, entity_equipment, close: None };
    if !intent.valid(action.id, action.tick) { return Err(ApplyFailure::InvalidAction); }
    Ok(intent)
}
