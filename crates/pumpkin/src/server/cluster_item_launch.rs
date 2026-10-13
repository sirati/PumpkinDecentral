use std::collections::BTreeMap;

use pumpkin_cluster::canonical_action::{ActionId, ActionRef, ApplyFailure, Cell, ChunkPart, Effect, Target, Value};
use pumpkin_cluster::canonical_inventory::{ItemTransformIntent, ItemTransformOperation};
use pumpkin_cluster::entity_body::{EntityBirth, EntityBody, EntityPayload, EntitySpatial, ProjectileBody, ProjectileKind};
use pumpkin_cluster::item_use::ItemUseIntent;
use pumpkin_cluster::time::TickStamp;
use pumpkin_data::item::Item;
use pumpkin_data::item_stack::ItemStack;

fn launch_velocity(id: ActionId, tick: TickStamp, yaw: f64, pitch: f64, speed: f64,
    inherited: [i64; 3], on_ground: bool) -> [i64; 3] {
    let direction = [-yaw.sin() * pitch.cos(), -pitch.sin(), yaw.cos() * pitch.cos()];
    std::array::from_fn(|index| {
        let seed = super::cluster_world_apply::CLUSTER_SEED ^ id.incarnation
            ^ (u64::from(id.sequence) << 32) ^ (u64::from(id.family) << 48) ^ index as u64;
        let first = pumpkin_cluster::order::rank_tick_actor(seed, tick, id.actor) as f64 / u64::MAX as f64;
        let second = pumpkin_cluster::order::rank_tick_actor(seed ^ 0xD1B54A32D192ED03, tick, id.actor) as f64 / u64::MAX as f64;
        let velocity = (direction[index] + (first - second) * 0.0172275) * speed;
        (velocity * 1_000_000.0).round() as i64 + if index == 1 && on_ground { 0 } else { inherited[index] }
    })
}

pub(super) fn thrown_kind(item: &Item) -> Option<(ProjectileKind, u16, f64, f64)> {
    if item == &Item::SNOWBALL { Some((ProjectileKind::Snowball, pumpkin_data::entity::EntityType::SNOWBALL.id, 1.5, 0.0)) }
    else if item == &Item::EGG { Some((ProjectileKind::Egg, pumpkin_data::entity::EntityType::EGG.id, 1.5, 0.0)) }
    else if item == &Item::ENDER_PEARL { Some((ProjectileKind::EnderPearl, pumpkin_data::entity::EntityType::ENDER_PEARL.id, 1.5, 0.0)) }
    else if item == &Item::EXPERIENCE_BOTTLE { Some((ProjectileKind::ExperienceBottle, pumpkin_data::entity::EntityType::EXPERIENCE_BOTTLE.id, 0.7, -20.0)) }
    else if item == &Item::SPLASH_POTION { Some((ProjectileKind::SplashPotion, pumpkin_data::entity::EntityType::SPLASH_POTION.id, 0.5, -20.0)) }
    else if item == &Item::LINGERING_POTION { Some((ProjectileKind::LingeringPotion, pumpkin_data::entity::EntityType::LINGERING_POTION.id, 0.5, -20.0)) }
    else { None }
}

pub(super) fn crossbow_items(intent: &pumpkin_cluster::crossbow_items::CrossbowItems,
    id: ActionId, tick: TickStamp, values: &BTreeMap<Target, Cell>)
    -> Result<BTreeMap<Target, pumpkin_cluster::inventory::InventoryCell>, ApplyFailure> {
    use pumpkin_data::data_component::DataComponent;
    use pumpkin_data::data_component_impl::ChargedProjectilesImpl;
    let Some(Cell { value: Value::Inventory(source), .. }) = values.get(&intent.weapon.target) else {
        return Err(ApplyFailure::Inventory(intent.weapon.target));
    };
    let mut weapon = super::cluster_invsee::item_stack_from_inventory_cell(source).map_err(|_| ApplyFailure::InvalidAction)?;
    if weapon.item != &Item::CROSSBOW { return Err(ApplyFailure::InvalidAction); }
    let creative = match intent.game_mode.and_then(|mode| values.get(&mode)).map(|cell| &cell.value) {
        Some(Value::LivingProperty(pumpkin_cluster::living_property::LivingPropertyValue::Unsigned(1))) => true,
        Some(Value::LivingProperty(pumpkin_cluster::living_property::LivingPropertyValue::Unsigned(3))) => return Err(ApplyFailure::InvalidAction),
        Some(Value::LivingProperty(pumpkin_cluster::living_property::LivingPropertyValue::Unsigned(_))) => false,
        None if matches!(id.actor, pumpkin_cluster::actor::ActorId::Entity(_)) => false,
        _ => return Err(ApplyFailure::InvalidAction),
    };
    let charged = weapon.get_data_component::<ChargedProjectilesImpl>().map(|value| value.projectiles.clone()).unwrap_or_default();
    let mut writes = BTreeMap::new();
    if intent.loading {
        if !charged.is_empty() { return Err(ApplyFailure::InvalidAction); }
        let projectile = if let Some(binding) = &intent.ammunition {
            let Some(Cell { value: Value::Inventory(ammunition), .. }) = values.get(&binding.target) else {
                return Err(ApplyFailure::Inventory(binding.target));
            };
            let stack = super::cluster_invsee::item_stack_from_inventory_cell(ammunition).map_err(|_| ApplyFailure::InvalidAction)?;
            if stack.item_count == 0 || !matches!(stack.item, item if item == &Item::ARROW || item == &Item::SPECTRAL_ARROW || item == &Item::TIPPED_ARROW || item == &Item::FIREWORK_ROCKET) {
                return Err(ApplyFailure::InvalidAction);
            }
            if !creative {
                let mut after = ammunition.clone();
                after.count -= 1;
                if after.count == 0 { after.item = 0; after.nbt.clear(); }
                writes.insert(binding.target, after);
            }
            stack.copy_with_count(1)
        } else if creative { ItemStack::static_new_java(1, &Item::ARROW) } else {
            return Err(ApplyFailure::InvalidAction);
        };
        let count = if weapon.get_enchantment_level(&pumpkin_data::Enchantment::MULTISHOT) > 0 { 3 } else { 1 };
        let mut compound = pumpkin_nbt::compound::NbtCompound::new();
        projectile.write_item_stack(&mut compound);
        weapon.patch.retain(|(component, _)| *component != DataComponent::ChargedProjectiles);
        weapon.patch.push((DataComponent::ChargedProjectiles, Some(Box::new(ChargedProjectilesImpl { projectiles: vec![compound; count] }))));
    } else {
        if charged.is_empty() || charged.len() > 3 { return Err(ApplyFailure::InvalidAction); }
        let amount = charged.iter().try_fold(0_u16, |wear, encoded| {
            let stack = ItemStack::read_item_stack(encoded).ok_or(ApplyFailure::InvalidAction)?;
            wear.checked_add(if stack.item == &Item::FIREWORK_ROCKET { 3 } else { 1 }).ok_or(ApplyFailure::InvalidAction)
        })?;
        weapon.patch.retain(|(component, _)| *component != DataComponent::ChargedProjectiles);
        weapon.patch.push((DataComponent::ChargedProjectiles, Some(Box::new(ChargedProjectilesImpl { projectiles: Vec::new() }))));
        if !creative {
            let cleared = super::cluster_invsee::inventory_cell_from_item_stack(source.loc, &weapon).map_err(|_| ApplyFailure::InvalidAction)?;
            let after = super::cluster_world_item_actions::derive_item_transform(&cleared, &ItemTransformOperation::Damage { amount }, id, tick)?.source_after;
            writes.insert(intent.weapon.target, after);
            return Ok(writes);
        }
    }
    writes.insert(intent.weapon.target, super::cluster_invsee::inventory_cell_from_item_stack(source.loc, &weapon).map_err(|_| ApplyFailure::InvalidAction)?);
    Ok(writes)
}

pub(super) fn crossbow_load(intent: &ItemUseIntent, id: ActionId, tick: TickStamp, started: TickStamp,
    weapon: &ItemStack, values: &BTreeMap<Target, Cell>) -> Result<Vec<Effect>, ApplyFailure> {
    let required = 25_i32.saturating_sub(5_i32.saturating_mul(weapon.get_enchantment_level(&pumpkin_data::Enchantment::QUICK_CHARGE).max(0))).max(0) as u16;
    if tick.distance_since(started) < required { return Ok(Vec::new()); }
    let launch = intent.launch.as_ref().ok_or(ApplyFailure::InvalidAction)?;
    let mut bindings = launch.ammunition.iter().collect::<Vec<_>>();
    bindings.sort_by_key(|binding| (binding.slot.loc.inv != pumpkin_cluster::inventory::INV_OFFHAND, binding.slot.loc.slot));
    let ammunition = bindings.into_iter().find(|binding| {
        let Some(Cell { value: Value::Inventory(cell), .. }) = values.get(&binding.target) else { return false; };
        super::cluster_invsee::item_stack_from_inventory_cell(cell).is_ok_and(|stack| stack.item_count > 0
            && matches!(stack.item, item if item == &Item::ARROW || item == &Item::SPECTRAL_ARROW || item == &Item::TIPPED_ARROW || item == &Item::FIREWORK_ROCKET))
    }).cloned();
    let game_mode = matches!(id.actor, pumpkin_cluster::actor::ActorId::Player(_))
        .then(|| intent.property_target(pumpkin_cluster::living_property::LivingProperty::GameMode));
    if ammunition.is_none() && game_mode.and_then(|mode| values.get(&mode)).is_none_or(|cell|
        cell.value != Value::LivingProperty(pumpkin_cluster::living_property::LivingPropertyValue::Unsigned(1))) {
        return Ok(Vec::new());
    }
    Ok(vec![Effect::CrossbowItems { intent: pumpkin_cluster::crossbow_items::CrossbowItems {
        loading: true, weapon: intent.source.clone(), ammunition, game_mode,
    } }])
}

pub(super) fn crossbow_fire(intent: &ItemUseIntent, id: ActionId, tick: TickStamp,
    weapon: &ItemStack, values: &BTreeMap<Target, Cell>) -> Result<Vec<Effect>, ApplyFailure> {
    let launch = intent.launch.as_ref().ok_or(ApplyFailure::InvalidAction)?;
    if !launch.valid() || launch.shooter.actor() != id.actor || launch.shooter.simulation_tick != tick {
        return Err(ApplyFailure::InvalidAction);
    }
    let charged = weapon.get_data_component::<pumpkin_data::data_component_impl::ChargedProjectilesImpl>()
        .ok_or(ApplyFailure::InvalidAction)?;
    if charged.projectiles.is_empty() || charged.projectiles.len() > launch.identities.len() { return Err(ApplyFailure::InvalidAction); }
    let game_mode = matches!(id.actor, pumpkin_cluster::actor::ActorId::Player(_))
        .then(|| intent.property_target(pumpkin_cluster::living_property::LivingProperty::GameMode));
    let creative = game_mode.and_then(|target| values.get(&target)).is_some_and(|cell|
        cell.value == Value::LivingProperty(pumpkin_cluster::living_property::LivingPropertyValue::Unsigned(1)));
    let mut effects = vec![Effect::CrossbowItems { intent: pumpkin_cluster::crossbow_items::CrossbowItems {
        loading: false, weapon: intent.source.clone(), ammunition: None, game_mode,
    } }];
    for (index, encoded) in charged.projectiles.iter().enumerate() {
        let projectile = ItemStack::read_item_stack(encoded).ok_or(ApplyFailure::InvalidAction)?;
        if projectile.item_count != 1 || !matches!(projectile.item, item if item == &Item::ARROW || item == &Item::SPECTRAL_ARROW || item == &Item::TIPPED_ARROW || item == &Item::FIREWORK_ROCKET) {
            return Err(ApplyFailure::InvalidAction);
        }
        let identity = &launch.identities[index];
        let firework = projectile.item == &Item::FIREWORK_ROCKET;
        let spectral = projectile.item == &Item::SPECTRAL_ARROW;
        let mut position = launch.shooter.position.map(f64::from_bits);
        position[1] += f64::from_bits(launch.eye_height) - 0.1;
        let yaw = f64::from(f32::from_bits(launch.yaw)) + [0.0, -10.0, 10.0][index];
        let velocity = launch_velocity(id, tick, yaw.to_radians(), f64::from(f32::from_bits(launch.pitch)).to_radians(),
            if firework { 1.6 } else { 3.15 }, launch.velocity_micro, launch.on_ground);
        let carried_item = super::cluster_invsee::inventory_cell_from_item_stack(intent.source.slot.loc, &projectile)
            .map_err(|_| ApplyFailure::InvalidAction)?;
        let (shooter_lease, shooter_birth) = match &launch.shooter.identity {
            pumpkin_cluster::item_contact::ContactIdentity::Player(lease) => (Some(*lease), None),
            pumpkin_cluster::item_contact::ContactIdentity::Entity { birth, .. } => (None, Some(birth.clone())),
        };
        let reference = ActionRef { tick, id };
        let body = EntityBody { mount: None, tags: Default::default(), tags_revision: 1, relation_versions: Default::default(), passengers: Default::default(),
            id: identity.id, birth: EntityBirth { action: reference, source: identity.source, uuid: identity.uuid }, owner: identity.source,
            kind: if firework { pumpkin_data::entity::EntityType::FIREWORK_ROCKET.id } else if spectral { pumpkin_data::entity::EntityType::SPECTRAL_ARROW.id } else { pumpkin_data::entity::EntityType::ARROW.id },
            spatial_epoch: pumpkin_cluster::canonical_action::SpatialEpoch::Action(reference), spatial: EntitySpatial { world: launch.spawn_chunk.world,
                position: position.map(f64::to_bits), velocity_micro: velocity, yaw: (yaw as f32).to_bits(), pitch: launch.pitch, on_ground: false },
            payload: EntityPayload::Projectile(ProjectileBody { last_simulation_tick: None, embedded_age: 0, kind: if firework { ProjectileKind::Firework } else if spectral { ProjectileKind::SpectralArrow } else { ProjectileKind::Arrow },
                shooter: id.actor, shooter_lease, shooter_birth, shot_tick: tick, carried_item, damage_milli: 2_000, fire_ticks: 0, knockback_level: 0,
                critical: true, pierce: u8::try_from(weapon.get_enchantment_level(&pumpkin_data::Enchantment::PIERCING).max(0)).map_err(|_| ApplyFailure::InvalidAction)?,
                age: 0, embedded: None, hit_actors: Vec::new(), pickup_allowed: matches!(id.actor, pumpkin_cluster::actor::ActorId::Player(_)) && !creative && index == 0, pickup_creative_only: matches!(id.actor, pumpkin_cluster::actor::ActorId::Player(_)) && (creative || index != 0) }) };
        if !body.valid_at(tick) { return Err(ApplyFailure::InvalidAction); }
        effects.push(Effect::Put { target: Target::Chunk { key: launch.spawn_chunk, part: ChunkPart::Entity(body.id), inventory: None }, value: Value::Entity((body).into()) });
    }
    Ok(effects)
}

pub(super) fn thrown(intent: &ItemUseIntent, id: ActionId, tick: TickStamp, weapon: &ItemStack,
    values: &BTreeMap<Target, Cell>) -> Result<Vec<Effect>, ApplyFailure> {
    let (kind, entity_kind, speed, pitch_offset) = thrown_kind(weapon.item).ok_or(ApplyFailure::InvalidAction)?;
    let launch = intent.launch.as_ref().ok_or(ApplyFailure::InvalidAction)?;
    if !launch.valid() || launch.shooter.actor() != id.actor || launch.shooter.simulation_tick != tick {
        return Err(ApplyFailure::InvalidAction);
    }
    let creative = match values.get(&intent.property_target(pumpkin_cluster::living_property::LivingProperty::GameMode)).map(|cell| &cell.value) {
        Some(Value::LivingProperty(pumpkin_cluster::living_property::LivingPropertyValue::Unsigned(1))) => true,
        Some(Value::LivingProperty(pumpkin_cluster::living_property::LivingPropertyValue::Unsigned(3))) => return Err(ApplyFailure::InvalidAction),
        Some(Value::LivingProperty(pumpkin_cluster::living_property::LivingPropertyValue::Unsigned(_))) => false,
        None if matches!(id.actor, pumpkin_cluster::actor::ActorId::Entity(_)) => false,
        _ => return Err(ApplyFailure::InvalidAction),
    };
    let Value::Inventory(source) = &values.get(&intent.source.target).ok_or(ApplyFailure::InvalidAction)?.value else {
        return Err(ApplyFailure::InvalidAction);
    };
    let identity = launch.identities.first().ok_or(ApplyFailure::InvalidAction)?;
    let mut position = launch.shooter.position.map(f64::from_bits);
    position[1] += f64::from_bits(launch.eye_height) - 0.1;
    let yaw = f64::from(f32::from_bits(launch.yaw)).to_radians();
    let pitch = (f64::from(f32::from_bits(launch.pitch)) + pitch_offset).to_radians();
    let velocity = launch_velocity(id, tick, yaw, pitch, speed, launch.velocity_micro, launch.on_ground);
    let (shooter_lease, shooter_birth) = match &launch.shooter.identity {
        pumpkin_cluster::item_contact::ContactIdentity::Player(lease) => (Some(*lease), None),
        pumpkin_cluster::item_contact::ContactIdentity::Entity { birth, .. } => (None, Some(birth.clone())),
    };
    let reference = ActionRef { tick, id };
    let mut carried_item = source.clone();
    carried_item.count = 1;
    let body = EntityBody { mount: None, id: identity.id, birth: EntityBirth { action: reference, source: identity.source, uuid: identity.uuid },
        owner: identity.source, kind: entity_kind, spatial: EntitySpatial { world: launch.spawn_chunk.world,
            position: position.map(f64::to_bits), velocity_micro: velocity,
            yaw: launch.yaw, pitch: launch.pitch, on_ground: false }, spatial_epoch: pumpkin_cluster::canonical_action::SpatialEpoch::Action(reference),
        tags: Default::default(), tags_revision: 1, relation_versions: Default::default(), passengers: Default::default(), payload: EntityPayload::Projectile(ProjectileBody { last_simulation_tick: None, embedded_age: 0,
            kind, shooter: id.actor, shooter_birth, shooter_lease, shot_tick: tick, carried_item, damage_milli: 0,
            fire_ticks: 0, knockback_level: 0, critical: false, pierce: 0, age: 0, embedded: None, hit_actors: Vec::new(), pickup_allowed: false, pickup_creative_only: false }) };
    if !body.valid_at(tick) { return Err(ApplyFailure::InvalidAction); }
    let mut effects = vec![Effect::Put { target: Target::Chunk { key: launch.spawn_chunk, part: ChunkPart::Entity(body.id), inventory: None }, value: Value::Entity(Box::new(body)) }];
    if !creative {
        effects.push(Effect::TransformItems { intent: ItemTransformIntent { source: intent.source.clone(), item: source.item,
            components: source.nbt.clone(), operation: ItemTransformOperation::Consume { count: 1 }, remainder_destinations: Vec::new() } });
    }
    Ok(effects)
}

pub(super) fn bow(
    intent: &ItemUseIntent, id: ActionId, tick: TickStamp, started: TickStamp, weapon: &ItemStack,
    values: &BTreeMap<Target, Cell>,
) -> Result<Vec<Effect>, ApplyFailure> {
    let elapsed = f64::from(tick.distance_since(started)) / 20.0;
    let power = ((elapsed * elapsed + 2.0 * elapsed) / 3.0).min(1.0);
    if power < 0.1 { return Ok(Vec::new()); }
    let launch = intent.launch.as_ref().ok_or(ApplyFailure::InvalidAction)?;
    if !launch.valid() || launch.shooter.actor() != id.actor || launch.shooter.simulation_tick != tick
        || launch.shooter.target() != intent.living || weapon.item != &Item::BOW {
        return Err(ApplyFailure::InvalidAction);
    }
    let game_mode = match values.get(&intent.property_target(pumpkin_cluster::living_property::LivingProperty::GameMode)).map(|cell| &cell.value) {
        Some(Value::LivingProperty(pumpkin_cluster::living_property::LivingPropertyValue::Unsigned(mode))) => *mode,
        None if matches!(id.actor, pumpkin_cluster::actor::ActorId::Entity(_)) => 0,
        _ => return Err(ApplyFailure::InvalidAction),
    };
    if game_mode == 3 { return Err(ApplyFailure::InvalidAction); }
    let creative = game_mode == 1;
    let selected = intent.equipment.first().and_then(|binding| values.get(&binding.selected)).and_then(|cell| match &cell.value {
        Value::Inventory(item) if (1..=9).contains(&item.item) => Some(item.item - 1), _ => None });
    let mut ammunition = launch.ammunition.iter().collect::<Vec<_>>();
    ammunition.sort_by_key(|binding| (if binding.slot.loc.inv == pumpkin_cluster::inventory::INV_OFFHAND { 0 }
        else if Some(binding.slot.loc.slot) == selected { 1 } else { 2 }, binding.slot.loc.slot));
    let ammunition = ammunition.into_iter().find_map(|binding| {
        let Value::Inventory(cell) = &values.get(&binding.target)?.value else { return None; };
        let stack = super::cluster_invsee::item_stack_from_inventory_cell(cell).ok()?;
        (stack.item_count > 0 && matches!(stack.item, item if item == &Item::ARROW || item == &Item::SPECTRAL_ARROW || item == &Item::TIPPED_ARROW))
            .then_some((binding, cell, stack))
    });
    let default;
    let (binding, cell, ammunition_stack) = if let Some((binding, cell, stack)) = ammunition {
        (Some(binding), cell.clone(), stack)
    } else if creative {
        let stack = ItemStack::static_new_java(1, &Item::ARROW);
        default = super::cluster_invsee::inventory_cell_from_item_stack(intent.source.slot.loc, &stack)
            .map_err(|_| ApplyFailure::InvalidAction)?;
        (None, default, stack)
    } else { return Ok(Vec::new()); };
    let infinity = weapon.get_enchantment_level(&pumpkin_data::Enchantment::INFINITY) > 0 && ammunition_stack.item == &Item::ARROW;
    let mut effects = Vec::new();
    if !creative && !infinity {
        let binding = binding.ok_or(ApplyFailure::InvalidAction)?;
        effects.push(Effect::TransformItems { intent: ItemTransformIntent { source: binding.clone(), item: cell.item,
            components: cell.nbt.clone(), operation: ItemTransformOperation::Consume { count: 1 }, remainder_destinations: Vec::new() } });
    }
    if !creative {
        let Value::Inventory(source) = &values.get(&intent.source.target).ok_or(ApplyFailure::Inventory(intent.source.target))?.value
            else { return Err(ApplyFailure::Inventory(intent.source.target)); };
        effects.push(Effect::TransformItems { intent: ItemTransformIntent { source: intent.source.clone(), item: source.item,
            components: source.nbt.clone(), operation: ItemTransformOperation::Damage { amount: 1 }, remainder_destinations: Vec::new() } });
    }
    let identity = launch.identities.first().ok_or(ApplyFailure::InvalidAction)?;
    let position = launch.shooter.position.map(f64::from_bits);
    let position = [position[0], position[1] + f64::from_bits(launch.eye_height) - 0.1, position[2]];
    if position.iter().any(|value| !value.is_finite() || value.abs() > 30_000_000.0)
        || (position[0].floor() as i32).div_euclid(16) != launch.spawn_chunk.x()
        || (position[2].floor() as i32).div_euclid(16) != launch.spawn_chunk.z() { return Err(ApplyFailure::InvalidAction); }
    let yaw = f64::from(f32::from_bits(launch.yaw)).to_radians();
    let pitch = f64::from(f32::from_bits(launch.pitch)).to_radians();
    let velocity = launch_velocity(id, tick, yaw, pitch, power * 3.0, launch.velocity_micro, launch.on_ground);
    let power_level = weapon.get_enchantment_level(&pumpkin_data::Enchantment::POWER).max(0) as u32;
    let damage_milli = 2_000_u32.checked_add(if power_level == 0 { 0 } else { power_level.checked_add(1)
        .and_then(|value| value.checked_mul(500)).ok_or(ApplyFailure::InvalidAction)? }).ok_or(ApplyFailure::InvalidAction)?;
    let mut carried_item = cell;
    carried_item.count = 1;
    let spectral = ammunition_stack.item == &Item::SPECTRAL_ARROW;
    let (shooter_lease, shooter_birth) = match &launch.shooter.identity {
        pumpkin_cluster::item_contact::ContactIdentity::Player(lease) => (Some(*lease), None),
        pumpkin_cluster::item_contact::ContactIdentity::Entity { birth, .. } => (None, Some(birth.clone())),
    };
    let reference = ActionRef { tick, id };
    let body = EntityBody { mount: None, id: identity.id, birth: EntityBirth { action: reference, source: identity.source, uuid: identity.uuid },
        owner: identity.source, kind: if spectral { pumpkin_data::entity::EntityType::SPECTRAL_ARROW.id } else { pumpkin_data::entity::EntityType::ARROW.id },
        spatial: EntitySpatial { world: launch.spawn_chunk.world, position: position.map(f64::to_bits),
            velocity_micro: velocity, yaw: launch.yaw, pitch: launch.pitch, on_ground: false },
        spatial_epoch: pumpkin_cluster::canonical_action::SpatialEpoch::Action(reference), tags: Default::default(), tags_revision: 1, relation_versions: Default::default(), passengers: Default::default(),
        payload: EntityPayload::Projectile(ProjectileBody { last_simulation_tick: None, embedded_age: 0, kind: if spectral { ProjectileKind::SpectralArrow } else { ProjectileKind::Arrow },
            shooter: id.actor, shooter_birth, shooter_lease, shot_tick: tick, carried_item, damage_milli,
            fire_ticks: if weapon.get_enchantment_level(&pumpkin_data::Enchantment::FLAME) > 0 { 2_000 } else { 0 },
            knockback_level: u16::try_from(weapon.get_enchantment_level(&pumpkin_data::Enchantment::PUNCH).max(0)).map_err(|_| ApplyFailure::InvalidAction)?,
            critical: power == 1.0, pierce: 0, age: 0, embedded: None, hit_actors: Vec::new(), pickup_allowed: matches!(id.actor, pumpkin_cluster::actor::ActorId::Player(_)) && !creative && !infinity, pickup_creative_only: matches!(id.actor, pumpkin_cluster::actor::ActorId::Player(_)) && (creative || infinity) }) };
    if !body.valid_at(tick) { return Err(ApplyFailure::InvalidAction); }
    effects.push(Effect::Put { target: Target::Chunk { key: launch.spawn_chunk, part: ChunkPart::Entity(body.id), inventory: None }, value: Value::Entity((body).into()) });
    Ok(effects)
}

pub(super) fn trident(intent: &ItemUseIntent, id: ActionId, tick: TickStamp, started: TickStamp,
    weapon: &ItemStack, values: &BTreeMap<Target, Cell>) -> Result<Vec<Effect>, ApplyFailure> {
    if weapon.item != &Item::TRIDENT || weapon.item_count != 1 {
        return Err(ApplyFailure::InvalidAction);
    }
    if tick.distance_since(started) < 10 { return Ok(Vec::new()); }
    if weapon.get_enchantment_level(&pumpkin_data::Enchantment::RIPTIDE) > 0 {
        return Err(ApplyFailure::InvalidAction);
    }
    let launch = intent.launch.as_ref().ok_or(ApplyFailure::InvalidAction)?;
    if !launch.valid() || launch.shooter.actor() != id.actor || launch.shooter.simulation_tick != tick
        || launch.shooter.target() != intent.living { return Err(ApplyFailure::InvalidAction); }
    let creative = match values.get(&intent.property_target(pumpkin_cluster::living_property::LivingProperty::GameMode)).map(|cell| &cell.value) {
        Some(Value::LivingProperty(pumpkin_cluster::living_property::LivingPropertyValue::Unsigned(1))) => true,
        Some(Value::LivingProperty(pumpkin_cluster::living_property::LivingPropertyValue::Unsigned(3))) => return Err(ApplyFailure::InvalidAction),
        Some(Value::LivingProperty(pumpkin_cluster::living_property::LivingPropertyValue::Unsigned(_))) => false,
        None if matches!(id.actor, pumpkin_cluster::actor::ActorId::Entity(_)) => false,
        _ => return Err(ApplyFailure::InvalidAction),
    };
    let Some(Cell { value: Value::Inventory(source), .. }) = values.get(&intent.source.target) else {
        return Err(ApplyFailure::Inventory(intent.source.target));
    };
    let carried_item = super::cluster_world_item_actions::derive_item_transform(source,
        &ItemTransformOperation::Damage { amount: 1 }, id, tick)?.source_after;
    if carried_item.is_empty() { return Ok(Vec::new()); }
    let identity = launch.identities.first().ok_or(ApplyFailure::InvalidAction)?;
    let mut position = launch.shooter.position.map(f64::from_bits);
    position[1] += f64::from_bits(launch.eye_height) - 0.1;
    let velocity = launch_velocity(id, tick, f64::from(f32::from_bits(launch.yaw)).to_radians(),
        f64::from(f32::from_bits(launch.pitch)).to_radians(), 2.5, launch.velocity_micro, launch.on_ground);
    let (shooter_lease, shooter_birth) = match &launch.shooter.identity {
        pumpkin_cluster::item_contact::ContactIdentity::Player(lease) => (Some(*lease), None),
        pumpkin_cluster::item_contact::ContactIdentity::Entity { birth, .. } => (None, Some(birth.clone())),
    };
    let reference = ActionRef { tick, id };
    let body = EntityBody { mount: None, id: identity.id, birth: EntityBirth { action: reference, source: identity.source, uuid: identity.uuid },
        owner: identity.source, kind: pumpkin_data::entity::EntityType::TRIDENT.id,
        spatial: EntitySpatial { world: launch.spawn_chunk.world, position: position.map(f64::to_bits),
            velocity_micro: velocity, yaw: launch.yaw, pitch: launch.pitch, on_ground: false },
        spatial_epoch: pumpkin_cluster::canonical_action::SpatialEpoch::Action(reference), tags: Default::default(), tags_revision: 1,
        relation_versions: Default::default(), passengers: Default::default(),
        payload: EntityPayload::Projectile(ProjectileBody { last_simulation_tick: None, embedded_age: 0, kind: ProjectileKind::Trident, shooter: id.actor,
            shooter_birth, shooter_lease, shot_tick: tick, carried_item, damage_milli: 8_000,
            fire_ticks: 0, knockback_level: 0, critical: false, pierce: 0, age: 0, embedded: None,
            hit_actors: Vec::new(), pickup_allowed: matches!(id.actor, pumpkin_cluster::actor::ActorId::Player(_)) && !creative, pickup_creative_only: matches!(id.actor, pumpkin_cluster::actor::ActorId::Player(_)) && creative }) };
    if !body.valid_at(tick) { return Err(ApplyFailure::InvalidAction); }
    let mut effects = vec![Effect::Put { target: Target::Chunk { key: launch.spawn_chunk,
        part: ChunkPart::Entity(body.id), inventory: None }, value: Value::Entity(Box::new(body)) }];
    if !creative {
        effects.push(Effect::TransformItems { intent: ItemTransformIntent { source: intent.source.clone(), item: source.item,
            components: source.nbt.clone(), operation: ItemTransformOperation::ThrowTrident, remainder_destinations: Vec::new() } });
    }
    Ok(effects)
}
