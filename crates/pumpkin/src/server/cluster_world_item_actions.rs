use std::collections::{HashMap, HashSet};

use crate::entity::EntityBase;
use pumpkin_cluster::actor::ActorId;
use pumpkin_cluster::completion::{ActionKey, TickFinish};
use pumpkin_cluster::identity::PlayerSeq;
use pumpkin_cluster::inventory::{
    InvLedger, InventoryApplyError, InventoryDelta, InventoryResolution, SemanticInventoryOp,
};
use pumpkin_cluster::item_action::{
    ActorItemSnapshot, EffectiveItemPose, ItemAction, ItemActionPlan, ItemActionRejection,
    ItemProjectileEffect, ItemProjectileKind, ItemUseMode, ItemUseState, ItemUseTransition,
};
use pumpkin_cluster::protocol::TickBatch;
use pumpkin_data::tag::Taggable;
use tokio::sync::mpsc;

use super::Server;
use super::cluster_item_action::LocalItemActionIntent;

pub(super) fn derive_item_transform(
    source: &pumpkin_cluster::inventory::InventoryCell,
    operation: &pumpkin_cluster::canonical_inventory::ItemTransformOperation,
    id: pumpkin_cluster::canonical_action::ActionId,
    tick: pumpkin_cluster::time::TickStamp,
) -> Result<
    pumpkin_cluster::canonical_inventory::ItemTransformOutput,
    pumpkin_cluster::canonical_action::ApplyFailure,
> {
    use pumpkin_cluster::canonical_action::ApplyFailure;
    use pumpkin_cluster::canonical_inventory::{ItemTransformOperation, ItemTransformOutput};
    let mut stack = super::cluster_invsee::item_stack_from_inventory_cell(source)
        .map_err(|_| ApplyFailure::InvalidAction)?;
    let mut remainder = None;
    match *operation {
        ItemTransformOperation::ExchangeBucket { contents, fill } => {
            use pumpkin_cluster::canonical_inventory::BucketContents;
            let filled = match contents {
                BucketContents::Water => &pumpkin_data::item::Item::WATER_BUCKET,
                BucketContents::Lava => &pumpkin_data::item::Item::LAVA_BUCKET,
                BucketContents::PowderSnow => &pumpkin_data::item::Item::POWDER_SNOW_BUCKET,
            };
            let expected = if fill { &pumpkin_data::item::Item::BUCKET } else { filled };
            if stack.item != expected || stack.item_count == 0 || !fill && stack.item_count != 1 {
                return Err(ApplyFailure::InvalidAction);
            }
            let result = pumpkin_data::item_stack::ItemStack::static_new_java(1,
                if fill { filled } else { &pumpkin_data::item::Item::BUCKET });
            let result = super::cluster_invsee::inventory_cell_from_item_stack(source.loc, &result)
                .map_err(|_| ApplyFailure::InvalidAction)?;
            if stack.item_count == 1 { return Ok(ItemTransformOutput { source_after: result, remainder: None }); }
            stack.item_count -= 1;
            return Ok(ItemTransformOutput { source_after: super::cluster_invsee::inventory_cell_from_item_stack(source.loc, &stack)
                .map_err(|_| ApplyFailure::InvalidAction)?, remainder: Some(result) });
        }
        ItemTransformOperation::MendingRepair { experience } => {
            return derive_mending_repair(source, experience).map(|(output, _)| output);
        }
        ItemTransformOperation::ThrowTrident => {
            if stack.item != &pumpkin_data::item::Item::TRIDENT || stack.item_count != 1 {
                return Err(ApplyFailure::InvalidAction);
            }
            let worn = derive_item_transform(source, &ItemTransformOperation::Damage { amount: 1 }, id, tick)?.source_after;
            if worn.is_empty() { return Err(ApplyFailure::InvalidAction); }
            return Ok(ItemTransformOutput { source_after: pumpkin_cluster::inventory::InventoryCell::empty(source.loc), remainder: None });
        }
        ItemTransformOperation::Consume { count } => {
            if count == 0 || stack.item_count < count {
                return Err(ApplyFailure::InvalidAction);
            }
            if let Some(component) =
                stack.get_data_component::<pumpkin_data::data_component_impl::UseRemainderImpl>()
            {
                let mut output = component
                    .remainder_stack()
                    .ok_or(ApplyFailure::InvalidAction)?;
                output.item_count = output
                    .item_count
                    .checked_mul(count)
                    .filter(|count| *count <= 99)
                    .ok_or(ApplyFailure::InvalidAction)?;
                remainder = Some(
                    super::cluster_invsee::inventory_cell_from_item_stack(source.loc, &output)
                        .map_err(|_| ApplyFailure::InvalidAction)?,
                );
            }
            stack.item_count -= count;
        }
        ItemTransformOperation::Damage { .. }
        | ItemTransformOperation::AttackDamage
        | ItemTransformOperation::MineDamage { .. }
        | ItemTransformOperation::ShieldDamage { .. }
        | ItemTransformOperation::ArmorDamage { .. } => {
            let amount = match *operation {
                ItemTransformOperation::Damage { amount } => amount,
                ItemTransformOperation::AttackDamage => {
                    let amount = stack
                        .get_data_component::<pumpkin_data::data_component_impl::WeaponImpl>()
                        .map_or(0, |weapon| weapon.item_damage_per_attack);
                    if amount == 0 || !stack.is_damageable() {
                        return Ok(ItemTransformOutput {
                            source_after: source.clone(),
                            remainder: None,
                        });
                    }
                    u16::try_from(amount).map_err(|_| ApplyFailure::InvalidAction)?
                }
                ItemTransformOperation::MineDamage { hardness } => {
                    let hardness = f32::from_bits(hardness);
                    if !hardness.is_finite() || hardness < 0.0 { return Err(ApplyFailure::InvalidAction); }
                    let wear = stack.get_data_component::<pumpkin_data::data_component_impl::ToolImpl>()
                        .map_or(1, |tool| tool.damage_per_block);
                    if hardness == 0.0 || wear == 0 || !stack.is_damageable() {
                        return Ok(ItemTransformOutput { source_after: source.clone(), remainder: None });
                    }
                    u16::try_from(wear).map_err(|_| ApplyFailure::InvalidAction)?
                }
                ItemTransformOperation::ShieldDamage { incoming_milli } => {
                    if incoming_milli == 0 { return Err(ApplyFailure::InvalidAction); }
                    if stack.get_data_component::<pumpkin_data::data_component_impl::BlocksAttacksImpl>().is_none()
                        || !stack.is_damageable() {
                        return Ok(ItemTransformOutput { source_after: source.clone(), remainder: None });
                    }
                    u16::try_from((incoming_milli / 1_000).max(1)).map_err(|_| ApplyFailure::InvalidAction)?
                }
                ItemTransformOperation::ArmorDamage {
                    incoming_milli,
                    armor_slot,
                } => {
                    use pumpkin_data::data_component_impl::EquipmentSlot;
                    let applicable = stack
                        .get_data_component::<pumpkin_data::data_component_impl::EquippableImpl>()
                        .is_some_and(|equippable| {
                            equippable.damage_on_hurt
                                && matches!(
                                    (armor_slot, equippable.slot),
                                    (0, EquipmentSlot::Feet(_))
                                        | (1, EquipmentSlot::Legs(_))
                                        | (2, EquipmentSlot::Chest(_))
                                        | (3, EquipmentSlot::Head(_))
                                )
                        });
                    if incoming_milli == 0 || armor_slot >= 4 {
                        return Err(ApplyFailure::InvalidAction);
                    }
                    if !applicable || !stack.is_armor() || !stack.is_damageable() {
                        return Ok(ItemTransformOutput {
                            source_after: source.clone(),
                            remainder: None,
                        });
                    }
                    u16::try_from((incoming_milli / 4_000).max(1))
                        .map_err(|_| ApplyFailure::InvalidAction)?
                }
                _ => unreachable!(),
            };
            if !stack.is_damageable() || amount == 0 {
                return Err(ApplyFailure::InvalidAction);
            }
            if !stack.is_unbreakable() {
                let level = stack
                    .get_enchantment_level(&pumpkin_data::Enchantment::UNBREAKING)
                    .max(0) as u64;
                let mut damage = 0_i32;
                for index in 0..amount {
                    let seed = super::cluster_world_apply::CLUSTER_SEED
                        ^ id.incarnation
                        ^ (u64::from(id.sequence) << 32)
                        ^ (u64::from(id.family) << 48)
                        ^ u64::from(index);
                    let random = pumpkin_cluster::order::rank_tick_actor(seed, tick, id.actor);
                    let applies = if stack.is_armor() {
                        u128::from(random) * u128::from(5 * (level + 1))
                            < u128::from(u64::MAX) * u128::from(3 * (level + 1) + 2)
                    } else {
                        random % (level + 1) == 0
                    };
                    if applies {
                        damage += 1;
                    }
                }
                let damage = stack.get_damage().saturating_add(damage);
                if damage >= stack.get_max_damage().ok_or(ApplyFailure::InvalidAction)? {
                    stack.item_count -= 1;
                    if stack.item_count != 0 {
                        stack.set_damage(0);
                    }
                } else {
                    stack.set_damage(damage);
                }
            }
        }
    }
    let source_after = super::cluster_invsee::inventory_cell_from_item_stack(source.loc, &stack)
        .map_err(|_| ApplyFailure::InvalidAction)?;
    Ok(ItemTransformOutput {
        source_after,
        remainder,
    })
}

fn derive_vehicle_use<S: pumpkin_cluster::canonical_action::ActionState>(
    context: &S,
    intent: &pumpkin_cluster::item_use::ItemUseIntent,
    action_id: pumpkin_cluster::canonical_action::ActionId,
    tick: pumpkin_cluster::time::TickStamp,
    values: &std::collections::BTreeMap<
        pumpkin_cluster::canonical_action::Target,
        pumpkin_cluster::canonical_action::Cell,
    >,
) -> Result<
    Vec<pumpkin_cluster::canonical_action::Effect>,
    pumpkin_cluster::canonical_action::ApplyFailure,
> {
    use pumpkin_cluster::canonical_action::{ApplyFailure, ChunkPart, Effect, Target, Value};
    use pumpkin_cluster::canonical_inventory::{ItemTransformIntent, ItemTransformOperation};
    use pumpkin_cluster::entity_body::{EntityPayload, VehicleKind};
    use pumpkin_cluster::vehicle_property::{VehicleProperty as P, VehiclePropertyValue as V};
    if !matches!(values.get(&intent.property_target(pumpkin_cluster::living_property::LivingProperty::Health)).map(|cell| &cell.value),
        Some(Value::LivingProperty(pumpkin_cluster::living_property::LivingPropertyValue::Unsigned(health))) if *health > 0)
    {
        return Err(ApplyFailure::InvalidAction);
    }
    let creative = match values
        .get(&intent.property_target(pumpkin_cluster::living_property::LivingProperty::GameMode))
        .map(|cell| &cell.value)
    {
        Some(Value::LivingProperty(
            pumpkin_cluster::living_property::LivingPropertyValue::Unsigned(1),
        )) => true,
        Some(Value::LivingProperty(
            pumpkin_cluster::living_property::LivingPropertyValue::Unsigned(3),
        )) => return Err(ApplyFailure::InvalidAction),
        Some(Value::LivingProperty(
            pumpkin_cluster::living_property::LivingPropertyValue::Unsigned(_),
        )) => false,
        None if matches!(action_id.actor, pumpkin_cluster::actor::ActorId::Entity(_)) => false,
        _ => return Err(ApplyFailure::InvalidAction),
    };
    let contact = intent.vehicle.as_ref().ok_or(ApplyFailure::InvalidAction)?;
    let Value::Entity(body) = &values
        .get(&contact.target())
        .ok_or(ApplyFailure::InvalidAction)?
        .value
    else {
        return Err(ApplyFailure::InvalidAction);
    };
    let pumpkin_cluster::item_contact::ContactIdentity::Entity { id, birth } = &contact.identity
    else {
        return Err(ApplyFailure::InvalidAction);
    };
    if body.id != *id
        || body.birth != *birth
        || body.owner != contact.owner
        || body.spatial_epoch != contact.epoch
    {
        return Err(ApplyFailure::InvalidAction);
    }
    let EntityPayload::Vehicle(vehicle) = &body.payload else {
        return Err(ApplyFailure::InvalidAction);
    };
    let Value::Inventory(source) = &values
        .get(&intent.source.target)
        .ok_or(ApplyFailure::InvalidAction)?
        .value
    else {
        return Err(ApplyFailure::InvalidAction);
    };
    let stack = super::cluster_invsee::item_stack_from_inventory_cell(source)
        .map_err(|_| ApplyFailure::InvalidAction)?;
    if vehicle.kind == VehicleKind::TntMinecart {
        let fuse = Target::Chunk {
            key: contact.chunk,
            part: ChunkPart::VehicleProperty {
                actor: *id,
                property: P::Fuse,
            },
            inventory: None,
        };
        if !matches!(
            values.get(&fuse).map(|cell| &cell.value),
            Some(Value::VehicleProperty(V::Fuse(None)))
        ) {
            return Err(ApplyFailure::InvalidAction);
        }
    }
    let (property, value, operation) = match vehicle.kind {
        VehicleKind::FurnaceMinecart
            if stack.item == &pumpkin_data::item::Item::COAL
                || stack.item == &pumpkin_data::item::Item::CHARCOAL =>
        {
            let target = Target::Chunk {
                key: contact.chunk,
                part: ChunkPart::VehicleProperty {
                    actor: *id,
                    property: P::Fuel,
                },
                inventory: None,
            };
            let Some(Value::VehicleProperty(V::Unsigned(fuel))) =
                values.get(&target).map(|cell| &cell.value)
            else {
                return Err(ApplyFailure::InvalidAction);
            };
            (
                P::Fuel,
                V::Unsigned(
                    fuel.checked_add(3600)
                        .filter(|fuel| *fuel <= 32000)
                        .ok_or(ApplyFailure::InvalidAction)?,
                ),
                ItemTransformOperation::Consume { count: 1 },
            )
        }
        VehicleKind::TntMinecart if stack.item == &pumpkin_data::item::Item::FIRE_CHARGE => (
            P::Fuse,
            V::Fuse(Some(80)),
            ItemTransformOperation::Consume { count: 1 },
        ),
        VehicleKind::TntMinecart if stack.item == &pumpkin_data::item::Item::FLINT_AND_STEEL => (
            P::Fuse,
            V::Fuse(Some(80)),
            ItemTransformOperation::Damage { amount: 1 },
        ),
        _ => return Err(ApplyFailure::InvalidAction),
    };
    let transform = ItemTransformIntent {
        source: intent.source.clone(),
        item: source.item,
        components: source.nbt.clone(),
        operation,
        remainder_destinations: intent.remainder_destinations.clone(),
    };
    let mut projected = values.clone();
    if !creative {
        for (target, value) in transform.derive(context, action_id, tick, &projected)? {
            projected.insert(
                target,
                pumpkin_cluster::canonical_action::Cell {
                    revision: values.get(&target).map_or(0, |cell| cell.revision),
                    value: Value::Inventory(value),
                },
            );
        }
    }
    let mut effects = vec![Effect::Put {
        target: Target::Chunk {
            key: contact.chunk,
            part: ChunkPart::VehicleProperty {
                actor: body.id,
                property,
            },
            inventory: None,
        },
        value: Value::VehicleProperty(value),
    }];
    if !creative {
        effects.push(Effect::TransformItems { intent: transform });
    }
    if vehicle.kind == VehicleKind::FurnaceMinecart {
        let driver = intent
            .vehicle_source
            .as_ref()
            .ok_or(ApplyFailure::InvalidAction)?;
        if driver.actor() != action_id.actor || driver.world != contact.world {
            return Err(ApplyFailure::InvalidAction);
        }
        let push = [0, 2].map(|axis| {
            f64::from_bits(contact.position[axis]) - f64::from_bits(driver.position[axis])
        });
        let magnitude = push[0].hypot(push[1]);
        let push = if magnitude > 0.00001 {
            push.map(|axis| (axis / magnitude * 1_000_000.0).round() as i64)
        } else {
            [0; 2]
        };
        effects.push(Effect::Put {
            target: Target::Chunk {
                key: contact.chunk,
                part: ChunkPart::VehicleProperty {
                    actor: body.id,
                    property: P::FurnacePush,
                },
                inventory: None,
            },
            value: Value::VehicleProperty(V::Push(push)),
        });
    }
    if !creative {
        pumpkin_cluster::canonical_inventory::append_equipment_effects_from_projection(
            values,
            &projected,
            &mut effects,
            &intent.equipment,
            context,
        )
        .map_err(|_| ApplyFailure::InvalidAction)?;
    }
    Ok(effects)
}

pub(super) fn derive_canonical_use<S: pumpkin_cluster::canonical_action::ActionState>(
    context: &S,
    intent: &pumpkin_cluster::item_use::ItemUseIntent,
    id: pumpkin_cluster::canonical_action::ActionId,
    tick: pumpkin_cluster::time::TickStamp,
    values: &std::collections::BTreeMap<
        pumpkin_cluster::canonical_action::Target,
        pumpkin_cluster::canonical_action::Cell,
    >,
) -> Result<
    Vec<pumpkin_cluster::canonical_action::Effect>,
    pumpkin_cluster::canonical_action::ApplyFailure,
> {
    use pumpkin_cluster::canonical_action::{ApplyFailure, Effect, Value};
    use pumpkin_cluster::canonical_inventory::{ItemTransformIntent, ItemTransformOperation};
    use pumpkin_cluster::item_use::ItemUseOperation;
    if intent.vehicle.is_some() {
        return derive_vehicle_use(context, intent, id, tick, values);
    }
    let failure = ApplyFailure::Inventory(intent.source.target);
    let source = match &values
        .get(&intent.source.target)
        .ok_or_else(|| failure.clone())?
        .value
    {
        Value::Inventory(item) => item,
        _ => return Err(failure.clone()),
    };
    use pumpkin_cluster::living_property::{LivingProperty as P, LivingPropertyValue as V};
    let read = |property| {
        values
            .get(&intent.property_target(property))
            .map(|cell| &cell.value)
            .ok_or(ApplyFailure::Type(intent.property_target(property)))
    };
    let Value::LivingProperty(V::ItemUse(before_use)) = read(P::ItemUse)? else {
        return Err(ApplyFailure::InvalidAction);
    };
    let Value::LivingProperty(V::Food(before_food)) = read(P::Food)? else {
        return Err(ApplyFailure::InvalidAction);
    };
    let Value::LivingProperty(V::Cooldowns(before_cooldowns)) = read(P::Cooldowns)? else {
        return Err(ApplyFailure::InvalidAction);
    };
    let Value::LivingProperty(V::StatusEffects(before_effects)) = read(P::StatusEffects)? else {
        return Err(ApplyFailure::InvalidAction);
    };
    let Value::LivingProperty(V::Unsigned(health)) = read(P::Health)? else {
        return Err(ApplyFailure::InvalidAction);
    };
    if *health == 0 {
        return Err(ApplyFailure::InvalidAction);
    }
    let mut active_use = before_use.clone();
    let mut food_state = *before_food;
    let mut cooldowns = before_cooldowns.clone();
    let mut active_effects = before_effects.clone();
    let stack = super::cluster_invsee::item_stack_from_inventory_cell(source)
        .map_err(|_| failure.clone())?;
    let creative = match values.get(&intent.property_target(P::GameMode)).map(|cell| &cell.value) {
        Some(Value::LivingProperty(V::Unsigned(1))) => true,
        Some(Value::LivingProperty(V::Unsigned(3))) => return Err(ApplyFailure::InvalidAction),
        Some(Value::LivingProperty(V::Unsigned(_))) => false,
        None if matches!(id.actor, ActorId::Entity(_)) => false,
        _ => return Err(ApplyFailure::InvalidAction),
    };
    let mut effects = Vec::new();
    let mut instant_projection = values.clone();
    if let Some((category, key)) = intent.used_statistic {
        if category != pumpkin_data::statistic::StatisticCategory::Used as u16
            || key != u32::from(source.item)
        {
            return Err(ApplyFailure::InvalidAction);
        }
    }
    match intent.operation {
        ItemUseOperation::Start => {
            let group = item_cooldown_group(source)?;
            if cooldowns.get(&group).is_some_and(|state| state.until.is_after(tick)) {
                return Err(ApplyFailure::InvalidAction);
            }
            if stack.item == &pumpkin_data::item::Item::CROSSBOW
                && stack.get_data_component::<pumpkin_data::data_component_impl::ChargedProjectilesImpl>()
                    .is_some_and(|charged| !charged.projectiles.is_empty()) {
                effects.extend(super::cluster_item_launch::crossbow_fire(intent, id, tick, &stack, values)?);
                active_use = None;
            } else if super::cluster_item_launch::thrown_kind(stack.item).is_some() {
                effects.extend(super::cluster_item_launch::thrown(intent, id, tick, &stack, values)?);
                if stack.item == &pumpkin_data::item::Item::ENDER_PEARL {
                    let revision = cooldowns.get(&group).map_or(Some(1), |state| state.revision.checked_add(1))
                        .ok_or(ApplyFailure::Overflow(intent.living))?;
                    cooldowns.insert(group.clone(), pumpkin_cluster::item_action::ItemCooldownState {
                        actor: id.actor, group, until: pumpkin_cluster::time::TickStamp(tick.0.wrapping_add(20)), revision,
                    });
                }
                active_use = None;
            } else {
            let consumable =
                stack.get_data_component::<pumpkin_data::data_component_impl::ConsumableImpl>();
            let blocking = stack
                .get_data_component::<pumpkin_data::data_component_impl::BlocksAttacksImpl>()
                .is_some();
            let mode = if blocking {
                ItemUseMode::Block
            } else if consumable.is_some() {
                ItemUseMode::Consume
            } else if stack.item == &pumpkin_data::item::Item::BOW {
                ItemUseMode::Bow
            } else if stack.item == &pumpkin_data::item::Item::CROSSBOW {
                ItemUseMode::Crossbow
            } else if stack.item == &pumpkin_data::item::Item::TRIDENT {
                ItemUseMode::Trident
            } else {
                return Err(ApplyFailure::InvalidAction);
            };
            if let Some(food) =
                stack.get_data_component::<pumpkin_data::data_component_impl::FoodImpl>()
                && food_state.food >= 20
                && !food.can_always_eat
                && !creative
            {
                return Err(ApplyFailure::InvalidAction);
            }
            active_use = Some(ItemUseState {
                actor: id.actor,
                source: intent.source.slot,
                item: source.clone(),
                started: tick,
                mode,
                duration_ticks: consumable
                    .map_or(72_000, |component| component.consume_ticks().max(0) as u32),
                cooldown_ticks: stack.get_use_cooldown().map_or(0, |cooldown| {
                    (cooldown.seconds.max(0.0) * 20.0).ceil() as u32
                }),
                has_remainder: stack
                    .get_data_component::<pumpkin_data::data_component_impl::UseRemainderImpl>()
                    .is_some(),
                blocking_delay_ticks: intent.blocking_delay_ticks,
            });
            }
        }
        ItemUseOperation::Release => {
            let active = active_use.as_ref().ok_or(ApplyFailure::InvalidAction)?;
            if active.actor != id.actor || active.source != intent.source.slot
                || active.item.item != source.item || active.item.nbt != source.nbt {
                return Err(ApplyFailure::InvalidAction);
            }
            match active.mode {
                ItemUseMode::Bow => effects.extend(super::cluster_item_launch::bow(
                    intent, id, tick, active.started, &stack, values,
                )?),
                ItemUseMode::Crossbow => effects.extend(super::cluster_item_launch::crossbow_load(
                    intent, id, tick, active.started, &stack, values,
                )?),
                ItemUseMode::Trident => effects.extend(super::cluster_item_launch::trident(
                    intent, id, tick, active.started, &stack, values,
                )?),
                _ => {}
            }
            active_use = None;
        }
        ItemUseOperation::Complete => {
            let active = active_use.as_ref().ok_or(ApplyFailure::InvalidAction)?;
            if active.actor != id.actor
                || active.mode != ItemUseMode::Consume
                || active.source != intent.source.slot
                || active.item.item != source.item
                || active.item.nbt != source.nbt
                || u32::from(tick.distance_since(active.started)) < active.duration_ticks
            {
                return Err(ApplyFailure::InvalidAction);
            }
            let consumable = stack
                .get_data_component::<pumpkin_data::data_component_impl::ConsumableImpl>()
                .ok_or(ApplyFailure::InvalidAction)?;
            if let Some(food) =
                stack.get_data_component::<pumpkin_data::data_component_impl::FoodImpl>()
            {
                if food.nutrition < 0
                    || !food.saturation.is_finite()
                    || !(0.0..=20.0).contains(&food.saturation)
                {
                    return Err(ApplyFailure::InvalidAction);
                }
                let effect = pumpkin_cluster::item_action::ItemFoodEffect::new(
                    food_state,
                    u8::try_from(food.nutrition).map_err(|_| ApplyFailure::InvalidAction)?,
                    (food.saturation * 1000.0).round() as u32,
                );
                food_state = effect.after;
            }
            use pumpkin_data::data_component_impl::{ConsumeEffect, IDSet};
            for (index, effect) in consumable.effects.iter().enumerate() {
                match effect {
                    ConsumeEffect::ApplyEffects((effects, probability)) => {
                        if !probability.is_finite() || !(0.0..=1.0).contains(probability) {
                            return Err(ApplyFailure::InvalidAction);
                        }
                        let random = pumpkin_cluster::order::rank_tick_actor(
                            super::cluster_world_apply::CLUSTER_SEED
                                ^ id.incarnation
                                ^ (u64::from(id.sequence) << 32)
                                ^ index as u64,
                            tick,
                            id.actor,
                        );
                        if *probability < 1.0
                            && random as f64 / u64::MAX as f64 >= f64::from(*probability)
                        {
                            continue;
                        }
                        for effect in effects.iter() {
                            let kind = pumpkin_data::effect::StatusEffect::from_minecraft_name(
                                &effect.effect_id,
                            )
                            .ok_or(ApplyFailure::InvalidAction)?;
                            if kind == &pumpkin_data::effect::StatusEffect::INSTANT_HEALTH
                                || kind == &pumpkin_data::effect::StatusEffect::INSTANT_DAMAGE
                            {
                                let healing =
                                    kind == &pumpkin_data::effect::StatusEffect::INSTANT_HEALTH;
                                let base: u32 = if healing { 4000 } else { 6000 };
                                let multiplier = 1u32
                                    .checked_shl(
                                        u32::try_from(effect.amplifier)
                                            .map_err(|_| ApplyFailure::InvalidAction)?,
                                    )
                                    .ok_or(ApplyFailure::InvalidAction)?;
                                let amount = base
                                    .checked_mul(multiplier)
                                    .ok_or(ApplyFailure::InvalidAction)?;
                                let delta = pumpkin_cluster::combat_action::derive_instant_health(
                                    id,
                                    intent.living,
                                    id.actor,
                                    tick,
                                    healing,
                                    amount,
                                    intent.max_health_attribute,
                                    &instant_projection,
                                )?;
                                for effect in delta {
                                    let Effect::Put { target, value } = &effect else {
                                        return Err(ApplyFailure::InvalidAction);
                                    };
                                    instant_projection.insert(
                                        *target,
                                        pumpkin_cluster::canonical_action::Cell {
                                            revision: values[target].revision,
                                            value: value.clone(),
                                        },
                                    );
                                }
                                continue;
                            }
                            if !active_effects.insert(pumpkin_cluster::player_admin::EffectState {
                                kind: u16::from(kind.id),
                                duration: effect.duration,
                                amplifier: u8::try_from(effect.amplifier)
                                    .map_err(|_| ApplyFailure::InvalidAction)?,
                                ambient: effect.ambient,
                                show_particles: effect.show_particles,
                                show_icon: effect.show_icon,
                                blend: false,
                            }) {
                                return Err(ApplyFailure::InvalidAction);
                            }
                        }
                    }
                    ConsumeEffect::RemoveEffects(IDSet::IDs(kinds)) => {
                        for kind in kinds.iter() {
                            active_effects.remove(u16::from(kind.id));
                        }
                    }
                    ConsumeEffect::ClearAllEffects => active_effects = Default::default(),
                    ConsumeEffect::PlaySound(_) => {}
                    ConsumeEffect::RemoveEffects(IDSet::Tag(_))
                    | ConsumeEffect::TeleportRandomly(_) => {
                        return Err(ApplyFailure::InvalidAction);
                    }
                }
                let resistance_target = intent.property_target(P::Resistance);
                let resistance = active_effects
                    .visible
                    .iter()
                    .find(|active| {
                        active.duration != 0
                            && active.kind
                                == u16::from(pumpkin_data::effect::StatusEffect::RESISTANCE.id)
                    })
                    .map(|active| active.amplifier);
                instant_projection.insert(
                    resistance_target,
                    pumpkin_cluster::canonical_action::Cell {
                        revision: values[&resistance_target].revision,
                        value: Value::LivingProperty(V::Amplifier(resistance)),
                    },
                );
                let status_target = intent.property_target(P::StatusEffects);
                instant_projection.insert(
                    status_target,
                    pumpkin_cluster::canonical_action::Cell {
                        revision: values[&status_target].revision,
                        value: Value::LivingProperty(V::StatusEffects(active_effects.clone())),
                    },
                );
                let mut normalization = Vec::new();
                pumpkin_cluster::canonical_inventory::append_equipment_effects_from_projection(
                    values,
                    &instant_projection,
                    &mut normalization,
                    &intent.equipment,
                    context,
                )
                .map_err(|_| ApplyFailure::InvalidAction)?;
                for effect in normalization {
                    let Effect::Put { target, value } = effect else {
                        return Err(ApplyFailure::InvalidAction);
                    };
                    instant_projection.insert(
                        target,
                        pumpkin_cluster::canonical_action::Cell {
                            revision: values[&target].revision,
                            value,
                        },
                    );
                }
            }
            let potion = super::cluster_projectile_potion::potion_effects(&stack)?;
            if !potion.is_empty() {
                let attributes = intent.equipment.iter().flat_map(|binding| binding.attribute_ids.iter().copied())
                    .collect::<std::collections::BTreeSet<_>>().into_iter().collect::<Vec<_>>();
                let deltas = super::cluster_projectile_potion::apply(context, id, tick, id.actor, intent.living,
                    &potion, 1.0, 1.0, 1, intent.max_health_attribute, &attributes, &instant_projection)?;
                for delta in deltas {
                    let Effect::Put { target, value } = delta else { return Err(ApplyFailure::InvalidAction); };
                    let revision = instant_projection.get(&target).ok_or(ApplyFailure::Condition(target))?.revision;
                    instant_projection.insert(target, pumpkin_cluster::canonical_action::Cell { revision, value });
                }
                let Some(pumpkin_cluster::canonical_action::Cell { value: Value::LivingProperty(V::StatusEffects(potion_active)), .. }) =
                    instant_projection.get(&intent.property_target(P::StatusEffects)) else { return Err(ApplyFailure::InvalidAction); };
                active_effects = potion_active.clone();
            }
            active_use = None;
            if let Some(cooldown) = stack.get_use_cooldown() {
                if !cooldown.seconds.is_finite() || !(0.0..=1600.0).contains(&cooldown.seconds) {
                    return Err(ApplyFailure::InvalidAction);
                }
                let group = cooldown
                    .cooldown_group
                    .clone()
                    .unwrap_or_else(|| stack.item.registry_key.to_string());
                let revision = cooldowns
                    .get(&group)
                    .map_or(Some(1), |state| state.revision.checked_add(1))
                    .ok_or(ApplyFailure::Overflow(intent.living))?;
                cooldowns.insert(
                    group.clone(),
                    pumpkin_cluster::item_action::ItemCooldownState {
                        actor: id.actor,
                        group,
                        until: pumpkin_cluster::time::TickStamp(
                            tick.0.wrapping_add((cooldown.seconds * 20.0).ceil() as u16),
                        ),
                        revision,
                    },
                );
            }
            if !creative { effects.push(Effect::TransformItems {
                intent: ItemTransformIntent {
                    source: intent.source.clone(),
                    item: source.item,
                    components: source.nbt.clone(),
                    operation: ItemTransformOperation::Consume { count: 1 },
                    remainder_destinations: intent.remainder_destinations.clone(),
                },
            }); }
        }
    }
    if let Some((category, key)) = intent.used_statistic
        && (intent.operation == ItemUseOperation::Complete || effects.iter().any(|effect|
            matches!(effect, Effect::Put { value: Value::Entity(body), .. }
                if matches!(body.payload, pumpkin_cluster::entity_body::EntityPayload::Projectile(_))))) {
        let target = intent.property_target(P::Statistic { category, key });
        let Value::LivingProperty(V::Unsigned(before)) = read(P::Statistic { category, key })? else {
            return Err(ApplyFailure::Type(target));
        };
        let after = before.checked_add(1).filter(|value| *value <= i32::MAX as u32)
            .ok_or(ApplyFailure::Overflow(target))?;
        effects.push(Effect::Put { target, value: Value::LivingProperty(V::Unsigned(after)) });
    }
    for (property, before, after) in [
        (
            P::ItemUse,
            V::ItemUse(before_use.clone()),
            V::ItemUse(active_use),
        ),
        (P::Food, V::Food(*before_food), V::Food(food_state)),
        (
            P::Cooldowns,
            V::Cooldowns(before_cooldowns.clone()),
            V::Cooldowns(cooldowns),
        ),
        (
            P::StatusEffects,
            V::StatusEffects(before_effects.clone()),
            V::StatusEffects(active_effects.clone()),
        ),
    ] {
        if before != after {
            effects.push(Effect::Put {
                target: intent.property_target(property),
                value: Value::LivingProperty(after),
            });
        }
    }
    if active_effects != *before_effects {
        let facts = context.status_effect_facts(&active_effects)?;
        for (property, value) in [
            (P::Strength, V::Amplifier(facts.strength)),
            (P::Weakness, V::Amplifier(facts.weakness)),
            (P::Resistance, V::Amplifier(facts.resistance)),
            (P::FireResistance, V::Boolean(facts.fire_resistance)),
            (P::Blindness, V::Boolean(facts.blindness)),
        ] {
            if read(property)? != &Value::LivingProperty(value.clone()) {
                effects.push(Effect::Put {
                    target: intent.property_target(property),
                    value: Value::LivingProperty(value),
                });
            }
        }
    }
    for (target, cell) in &instant_projection {
        if values[target].value != cell.value {
            if let Some(Effect::Put { value, .. }) = effects.iter_mut().find(|effect| matches!(effect, Effect::Put { target: existing, .. } if existing == target)) {
                *value = cell.value.clone();
            } else {
                effects.push(Effect::Put { target: *target, value: cell.value.clone() });
            }
        }
    }
    if intent.operation == ItemUseOperation::Complete && matches!(id.actor, ActorId::Player(_)) {
        let pumpkin_cluster::canonical_action::Target::Chunk { key, .. } = intent.living else {
            return Err(ApplyFailure::InvalidAction);
        };
        pumpkin_cluster::player_advancement_event::append_effects(context,
            pumpkin_cluster::canonical_action::ActionRef { id, tick }, id.actor, key, values, &mut effects,
            &pumpkin_cluster::player_advancement::AdvancementEvent::ConsumeItem { item: source.item })?;
    }
    let mut projected = values.clone();
    for effect in &effects {
        match effect {
            Effect::CrossbowItems { intent: crossbow } => {
                for (target, value) in crossbow.derive(context, id, tick, &projected)? {
                    projected.insert(target, pumpkin_cluster::canonical_action::Cell {
                        revision: values[&target].revision, value: Value::Inventory(value),
                    });
                }
            }
            Effect::TransformItems { intent: transform } => {
                for (target, value) in transform.derive(context, id, tick, &projected)? {
                    projected.insert(
                        target,
                        pumpkin_cluster::canonical_action::Cell {
                            revision: values[&target].revision,
                            value: Value::Inventory(value),
                        },
                    );
                }
            }
            Effect::Put { target, value } => {
                projected.insert(
                    *target,
                    pumpkin_cluster::canonical_action::Cell {
                        revision: values.get(target).map_or(0, |cell| cell.revision),
                        value: value.clone(),
                    },
                );
            }
            _ => return Err(ApplyFailure::InvalidAction),
        }
    }
    pumpkin_cluster::canonical_inventory::append_equipment_effects_from_projection(
        values,
        &projected,
        &mut effects,
        &intent.equipment,
        context,
    )
    .map_err(|_| ApplyFailure::InvalidAction)?;
    let mut writes = std::collections::BTreeMap::new();
    effects.retain(|effect| match effect {
        Effect::Put { target, value } => {
            writes.insert(*target, value.clone());
            false
        }
        _ => true,
    });
    effects.extend(
        writes
            .into_iter()
            .map(|(target, value)| Effect::Put { target, value }),
    );
    Ok(effects)
}

pub(super) struct PreparedItemAction {
    pub blocks: Vec<super::cluster_chunk_state::BlockChange>,
    pub block_undo: Vec<super::cluster_chunk_state::BlockChange>,
    pub inventory: pumpkin_cluster::inventory::InventoryMutation,
    pub inventory_undo: Vec<SemanticInventoryOp>,
    pub use_transition: Option<ItemUseTransition>,
    pub cooldown_transition: Option<pumpkin_cluster::item_action::ItemCooldownTransition>,
    pub combat: Option<pumpkin_cluster::item_action::ItemCombatEffect>,
    pub projectile: Option<ItemProjectileEffect>,
    pub actors: Vec<pumpkin_cluster::item_action::ItemActorMutation>,
    pub actor_effects: Vec<pumpkin_cluster::item_action::ItemActorEffect>,
    pub statistics: Vec<pumpkin_cluster::item_action::ItemStatisticMutation>,
}

pub(super) struct PreflightedItemAction {
    pub blocks: super::cluster_chunk_state::PreparedBlockMutation,
    pub inventory: Option<super::cluster_world_inventory::PreparedInventoryMutation>,
    pub actors: Vec<(ActorId, super::cluster_world_actor::ActorLivingState)>,
    pub statistics: Option<super::cluster_actor_statistics::PreparedStatisticMutation>,
}

impl PreparedItemAction {
    pub(super) fn from_action(action: &ItemAction) -> Result<Self, ItemActionRejection> {
        let mut seen_blocks = HashSet::new();
        let mut blocks = Vec::with_capacity(action.blocks.len());
        let mut block_undo = Vec::with_capacity(action.blocks.len());
        for mutation in &action.blocks {
            let chunk = pumpkin_cluster::protocol::ChunkKey::new(
                mutation.world,
                mutation.block.x.div_euclid(16),
                mutation.block.z.div_euclid(16),
            );
            let before = pumpkin_data::BlockStateId::new(mutation.before)
                .ok_or(ItemActionRejection::StaleBlock)?;
            let after = pumpkin_data::BlockStateId::new(mutation.after)
                .ok_or(ItemActionRejection::StaleBlock)?;
            if !action.affected.contains(&chunk) || !seen_blocks.insert((chunk, mutation.block)) {
                return Err(ItemActionRejection::UnsupportedEffect);
            }
            let change = super::cluster_chunk_state::BlockChange {
                chunk,
                position: mutation.block,
                before,
                after,
            };
            block_undo.push(super::cluster_chunk_state::BlockChange {
                before: after,
                after: before,
                ..change
            });
            blocks.push(change);
        }
        let inventory = action.inventory_mutation()?;
        let mut seen_slots = HashSet::new();
        for operation in &inventory.operations {
            if operation.target != action.source.target
                || operation.before.loc != operation.after.loc
                || !seen_slots.insert((operation.target, operation.before.loc))
            {
                return Err(ItemActionRejection::StaleInventory);
            }
        }
        let inventory_undo = inventory
            .operations
            .iter()
            .rev()
            .map(|operation| SemanticInventoryOp {
                target: operation.target,
                before: operation.after.clone(),
                after: operation.before.clone(),
            })
            .collect();
        Ok(Self {
            blocks,
            block_undo,
            inventory,
            inventory_undo,
            use_transition: action.use_transition.clone(),
            cooldown_transition: action.cooldown_transition.clone(),
            combat: action.combat.clone(),
            projectile: action.projectile.clone(),
            actors: action.actors.clone(),
            actor_effects: action.actor_effects.clone(),
            statistics: action.statistics.clone(),
        })
    }

    pub(super) fn preflight_owned(
        &self,
        action: &ItemAction,
        blocks: &super::cluster_chunk_state::OwnedChunkStore,
        inventory: &super::cluster_world_inventory::InventoryOwner,
        actors: &super::cluster_world_actor::OwnedLivingActors,
        statistics: &super::cluster_actor_statistics::OwnedActorStatistics,
    ) -> Result<PreflightedItemAction, ItemActionRejection> {
        if !self.actors.is_empty() {
            return Err(ItemActionRejection::UnsupportedEffect);
        }
        let prepared_blocks = blocks
            .prepare_local(&self.blocks)
            .map_err(|error| match error {
                super::cluster_chunk_state::BlockTransactionError::MissingChunk => {
                    ItemActionRejection::MissingBlock
                }
                _ => ItemActionRejection::StaleBlock,
            })?;
        let prepared_actors = actors.prepare_local(action).map_err(|error| match error {
            super::cluster_world_actor::ActorEffectError::MissingActor => {
                ItemActionRejection::MissingActor
            }
            _ => ItemActionRejection::StaleActor,
        })?;
        let prepared_inventory = if self.inventory.operations.is_empty() {
            None
        } else {
            Some(
                inventory
                    .preview_mutation(
                        action.actor,
                        action.sequence,
                        action.tick,
                        action.actor_chunk,
                        action.affected.clone(),
                        self.inventory.clone(),
                    )
                    .map_err(|_| ItemActionRejection::StaleInventory)?,
            )
        };
        let prepared_statistics = if self.statistics.is_empty() {
            None
        } else {
            let ActorId::Player(actor) = action.actor else {
                return Err(ItemActionRejection::ActorMismatch);
            };
            let mutations = self
                .statistics
                .iter()
                .map(
                    |mutation| super::cluster_actor_statistics::StatisticMutation::Increment {
                        category: mutation.category,
                        stat: mutation.statistic,
                        amount: mutation.amount,
                    },
                )
                .collect::<Vec<_>>();
            Some(
                statistics
                    .preview_item_mutations(
                        actor,
                        action.tick,
                        ActionKey::item_action(action),
                        &mutations,
                    )
                    .map_err(|_| ItemActionRejection::StaleActor)?,
            )
        };
        Ok(PreflightedItemAction {
            blocks: prepared_blocks,
            inventory: prepared_inventory,
            actors: prepared_actors,
            statistics: prepared_statistics,
        })
    }
}

pub(super) fn plan_item_action(
    server: &Server,
    blocks: &super::cluster_chunk_state::OwnedChunkStore,
    intent: &pumpkin_cluster::item_action::ItemActionIntent,
) -> Option<ItemActionPlan> {
    let pumpkin_cluster::item_action::ItemActionKind::UseOnBlock {
        world: world_id,
        block,
        hit,
        expected_block_state,
    } = &intent.kind
    else {
        return None;
    };
    let world = server
        .worlds
        .load()
        .iter()
        .find(|world| super::cluster_world_delta::world_id(world) == *world_id)?
        .clone();
    let ActorId::Player(gid) = intent.actor else {
        return None;
    };
    let player = server
        .get_all_players()
        .into_iter()
        .find(|player| player.cluster_gid() == Some(gid))?;
    if player.gamemode.load() == pumpkin_util::GameMode::Spectator {
        return None;
    }
    let clicked = pumpkin_util::math::position::BlockPos(
        pumpkin_util::math::vector3::Vector3::new(block.x, block.y, block.z),
    );
    let clicked_state = owned_block(blocks, *world_id, *block)?;
    if clicked_state.as_u16() != *expected_block_state {
        return None;
    }
    if world.is_in_spawn_protection(&player, &clicked) {
        return None;
    }
    if !player.get_entity().is_sneaking()
        && let Some(plan) = plan_block_activation(*world_id, *block, clicked_state, blocks)
    {
        return Some(plan);
    }
    if player.gamemode.load() != pumpkin_util::GameMode::Survival {
        return None;
    }
    let placed = pumpkin_data::Block::from_item_id(intent.expected_item.item)?;
    if placed.states.len() != 1 || placed.default_state.block_entity_type != u16::MAX {
        return None;
    }
    let target = adjacent_target(*block, hit.face)?;
    let destination = pumpkin_util::math::position::BlockPos(
        pumpkin_util::math::vector3::Vector3::new(target.x, target.y, target.z),
    );
    if owned_block(blocks, *world_id, target)?.as_u16() != pumpkin_data::BlockStateId::AIR.as_u16()
        || intent.expected_item.count == 0
        || world.is_in_spawn_protection(&player, &destination)
    {
        return None;
    }
    let after = decrement_source_cell(&intent.expected_item)?;
    Some(ItemActionPlan {
        source_after: Some(after),
        blocks: vec![pumpkin_cluster::item_action::ItemBlockMutation {
            world: *world_id,
            block: target,
            before: pumpkin_data::BlockStateId::AIR.as_u16(),
            after: placed.default_state.id.as_u16(),
        }],
        actors: Vec::new(),
        use_transition: None,
        cooldown_transition: None,
        combat: None,
        projectile: None,
        remainder: None,
        actor_effects: Vec::new(),
        statistics: Vec::new(),
    })
}

fn plan_block_activation(
    world_id: pumpkin_cluster::protocol::WorldId,
    position: pumpkin_cluster::protocol::BlockPos,
    state: pumpkin_data::BlockStateId,
    blocks: &super::cluster_chunk_state::OwnedChunkStore,
) -> Option<ItemActionPlan> {
    let block = pumpkin_data::Block::from_state_id(state);
    let mut mutations = Vec::new();
    if let Some(after) = toggle_single_block(block, state) {
        mutations.push(pumpkin_cluster::item_action::ItemBlockMutation {
            world: world_id,
            block: position,
            before: state.as_u16(),
            after: after.as_u16(),
        });
    } else if block.has_tag(&pumpkin_data::tag::Block::MINECRAFT_DOORS)
        && block != &pumpkin_data::Block::IRON_DOOR
    {
        let mut properties =
            pumpkin_data::block_properties::OakDoorLikeProperties::from_state_id(state);
        properties.open = !properties.open;
        let other_y = match properties.half {
            pumpkin_data::block_properties::DoubleBlockHalf::Upper => position.y.checked_sub(1)?,
            pumpkin_data::block_properties::DoubleBlockHalf::Lower => position.y.checked_add(1)?,
        };
        let other = pumpkin_cluster::protocol::BlockPos {
            x: position.x,
            y: other_y,
            z: position.z,
        };
        let other_state = owned_block(blocks, world_id, other)?;
        if pumpkin_data::Block::from_state_id(other_state) != block {
            return None;
        }
        let mut other_properties =
            pumpkin_data::block_properties::OakDoorLikeProperties::from_state_id(other_state);
        if other_properties.half == properties.half {
            return None;
        }
        other_properties.open = properties.open;
        mutations.push(pumpkin_cluster::item_action::ItemBlockMutation {
            world: world_id,
            block: position,
            before: state.as_u16(),
            after: properties.to_state_id(block).as_u16(),
        });
        mutations.push(pumpkin_cluster::item_action::ItemBlockMutation {
            world: world_id,
            block: other,
            before: other_state.as_u16(),
            after: other_properties.to_state_id(block).as_u16(),
        });
    } else {
        return None;
    }
    Some(ItemActionPlan {
        blocks: mutations,
        ..ItemActionPlan::default()
    })
}

fn owned_block(
    blocks: &super::cluster_chunk_state::OwnedChunkStore,
    world: pumpkin_cluster::protocol::WorldId,
    position: pumpkin_cluster::protocol::BlockPos,
) -> Option<pumpkin_data::BlockStateId> {
    blocks
        .block(
            pumpkin_cluster::protocol::ChunkKey::new(
                world,
                position.x.div_euclid(16),
                position.z.div_euclid(16),
            ),
            position,
        )
        .ok()
}

fn toggle_single_block(
    block: &pumpkin_data::Block,
    state: pumpkin_data::BlockStateId,
) -> Option<pumpkin_data::BlockStateId> {
    if block == &pumpkin_data::Block::LEVER {
        let mut properties =
            pumpkin_data::block_properties::LeverLikeProperties::from_state_id(state);
        properties.powered = !properties.powered;
        Some(properties.to_state_id(block))
    } else if block.has_tag(&pumpkin_data::tag::Block::MINECRAFT_TRAPDOORS)
        && block != &pumpkin_data::Block::IRON_TRAPDOOR
    {
        let mut properties =
            pumpkin_data::block_properties::OakTrapdoorLikeProperties::from_state_id(state);
        properties.open = !properties.open;
        Some(properties.to_state_id(block))
    } else {
        None
    }
}

fn find_arrow(
    ledger: &InvLedger,
    target: pumpkin_cluster::inventory::InventoryTarget,
) -> Option<(u64, pumpkin_cluster::inventory::InventoryCell)> {
    let snapshot = ledger.snapshot(target)?;
    snapshot.cells.into_iter().find_map(|cell| {
        (cell.count > 0
            && (cell.item == pumpkin_data::item::Item::ARROW.id
                || cell.item == pumpkin_data::item::Item::SPECTRAL_ARROW.id
                || cell.item == pumpkin_data::item::Item::TIPPED_ARROW.id))
            .then_some((snapshot.revision, cell))
    })
}

fn decrement_source_cell(
    before: &pumpkin_cluster::inventory::InventoryCell,
) -> Option<pumpkin_cluster::inventory::InventoryCell> {
    let mut stack = super::cluster_invsee::item_stack_from_inventory_cell(before).ok()?;
    if stack.item_count == 0 {
        return None;
    }
    stack.item_count -= 1;
    super::cluster_invsee::inventory_cell_from_item_stack(before.loc, &stack).ok()
}

fn damage_bow_cell(
    before: &pumpkin_cluster::inventory::InventoryCell,
) -> Option<pumpkin_cluster::inventory::InventoryCell> {
    let mut weapon = super::cluster_invsee::item_stack_from_inventory_cell(before).ok()?;
    if weapon.item != &pumpkin_data::item::Item::BOW
        || weapon.get_enchantment_level(&pumpkin_data::Enchantment::UNBREAKING) > 0
    {
        return None;
    }
    let _damage = weapon.damage_item(1);
    super::cluster_invsee::inventory_cell_from_item_stack(before.loc, &weapon).ok()
}

fn plan_remainder_effect(
    source: &pumpkin_cluster::inventory::InventoryCell,
    inventory: &pumpkin_cluster::inventory::InventoryStateSnapshot,
    remainder: &pumpkin_data::data_component_impl::UseRemainderImpl,
) -> Option<pumpkin_cluster::item_action::ItemRemainderEffect> {
    let remainder_stack = remainder.remainder_stack()?;
    if source.count == 0 || remainder_stack.is_empty() {
        return None;
    }
    let remainder_item =
        super::cluster_invsee::inventory_cell_from_item_stack(source.loc, &remainder_stack).ok()?;
    if source.count == 1 {
        let source_after = remainder_item.clone();
        return Some(pumpkin_cluster::item_action::ItemRemainderEffect {
            source_before: source.clone(),
            remainder_item,
            source_after,
            destinations: Vec::new(),
        });
    }
    let source_after = decrement_source_cell(source)?;
    let destination_locs: Vec<_> = (0..36u16)
        .map(|slot| {
            pumpkin_cluster::inventory::InvLoc::new(pumpkin_cluster::inventory::INV_MAIN, slot)
        })
        .filter(|loc| *loc != source.loc)
        .collect();
    let destinations = pumpkin_cluster::inventory::plan_inventory_insertion(
        inventory,
        Some(&source_after),
        &remainder_item,
        remainder_stack.get_max_stack_size(),
        &destination_locs,
        |current, inserted| {
            let current = super::cluster_invsee::item_stack_from_inventory_cell(current);
            let inserted = super::cluster_invsee::item_stack_from_inventory_cell(inserted);
            current
                .ok()
                .zip(inserted.ok())
                .is_some_and(|(current, inserted)| {
                    current.are_items_and_components_equal(&inserted)
                })
        },
        |item, loc, count| {
            let mut stack = super::cluster_invsee::item_stack_from_inventory_cell(item).ok()?;
            stack.item_count = count;
            super::cluster_invsee::inventory_cell_from_item_stack(loc, &stack).ok()
        },
    )?;
    Some(pumpkin_cluster::item_action::ItemRemainderEffect {
        source_before: source.clone(),
        source_after,
        remainder_item,
        destinations,
    })
}

fn plan_food_effect(
    stack: &pumpkin_data::item_stack::ItemStack,
    before: pumpkin_cluster::item_action::ActorFoodState,
) -> Option<pumpkin_cluster::item_action::ItemActorEffect> {
    let consumable =
        stack.get_data_component::<pumpkin_data::data_component_impl::ConsumableImpl>()?;
    if !consumable.effects.is_empty() {
        return None;
    }
    let food = stack.get_data_component::<pumpkin_data::data_component_impl::FoodImpl>()?;
    let nutrition = u8::try_from(food.nutrition).ok()?;
    if nutrition == 0 || !food.saturation.is_finite() || !(0.0..=20.0).contains(&food.saturation) {
        return None;
    }
    let saturation_gain_milli = (food.saturation * 1_000.0).round() as u32;
    Some(pumpkin_cluster::item_action::ItemActorEffect::Food(
        pumpkin_cluster::item_action::ItemFoodEffect::new(before, nutrition, saturation_gain_milli),
    ))
}

fn plan_bow_release(
    ledger: &InvLedger,
    intent: &pumpkin_cluster::item_action::ItemActionIntent,
    effective: EffectiveItemPose,
    before: &ItemUseState,
    projectile_id: pumpkin_cluster::protocol::GlobalEntityId,
) -> Option<ItemProjectileEffect> {
    let drawn = u32::from(intent.tick.distance_since(before.started));
    if drawn < 3 || drawn > before.duration_ticks {
        return None;
    }
    let (ammo_revision, ammo_before) = find_arrow(ledger, intent.source.target)?;
    let ammo_after = decrement_source_cell(&ammo_before)?;
    let weapon_after = damage_bow_cell(&intent.expected_item)?;
    let draw_fraction = drawn as f32 / 20.0;
    let power = ((draw_fraction * draw_fraction + draw_fraction * 2.0) / 3.0).min(1.0);
    let yaw = effective.yaw.to_radians();
    let pitch = effective.pitch.to_radians();
    let direction = [
        -yaw.sin() * pitch.cos(),
        -pitch.sin(),
        yaw.cos() * pitch.cos(),
    ];
    let power_milli = (power * 1_000.0) as u16;
    let spawn = pumpkin_cluster::item_action::ItemProjectileSpawn::for_bow(
        projectile_id,
        effective,
        direction,
        power_milli,
    )?;
    Some(ItemProjectileEffect {
        kind: ItemProjectileKind::Bow,
        spawn,
        weapon_after,
        ammo: pumpkin_cluster::inventory::InventorySlotRef::new(
            intent.source.target,
            ammo_before.loc,
        ),
        ammo_before,
        ammo_after,
        ammo_revision,
        direction,
        power_milli,
        durability_cost: 1,
    })
}

pub(super) struct ItemActionOwner {
    pub(super) receiver: mpsc::Receiver<LocalItemActionIntent>,
    sequences: HashMap<ActorId, u16>,
    origins: HashMap<ActionKey, Option<u64>>,
    local_use: HashMap<ActorId, ItemUseState>,
    ground_use: HashMap<ActorId, ItemUseState>,
}

impl ItemActionOwner {
    pub(super) fn attach(server: &Server) -> Self {
        Self {
            receiver: super::cluster_item_action::attach_item_action_owner(server),
            sequences: HashMap::new(),
            origins: HashMap::new(),
            local_use: HashMap::new(),
            ground_use: HashMap::new(),
        }
    }

    pub(super) fn plan(
        &self,
        server: &Server,
        blocks: &super::cluster_chunk_state::OwnedChunkStore,
        ledger: &InvLedger,
        intent: &pumpkin_cluster::item_action::ItemActionIntent,
        effective: EffectiveItemPose,
        projectile_id: Option<pumpkin_cluster::protocol::GlobalEntityId>,
        living_state: Option<&super::cluster_world_actor::ActorLivingState>,
    ) -> Option<ItemActionPlan> {
        match &intent.kind {
            pumpkin_cluster::item_action::ItemActionKind::Use { .. } => {
                let stack =
                    super::cluster_invsee::item_stack_from_inventory_cell(&intent.expected_item)
                        .ok()?;
                let item = stack.item;
                let blocking = stack
                    .get_data_component::<pumpkin_data::data_component_impl::BlocksAttacksImpl>()
                    .is_some();
                let consumable =
                    stack.get_data_component::<pumpkin_data::data_component_impl::ConsumableImpl>();
                let food =
                    stack.get_data_component::<pumpkin_data::data_component_impl::FoodImpl>();
                let mode = if blocking {
                    ItemUseMode::Block
                } else if consumable.is_some() {
                    ItemUseMode::Consume
                } else if item == &pumpkin_data::item::Item::BOW {
                    ItemUseMode::Bow
                } else {
                    return None;
                };
                if mode == ItemUseMode::Bow && find_arrow(ledger, intent.source.target).is_none() {
                    return None;
                }
                if let Some(food) = food {
                    let current = living_state?.food;
                    if current.actor != intent.actor || (!food.can_always_eat && current.food >= 20)
                    {
                        return None;
                    }
                }
                let duration_ticks =
                    consumable.map_or(72_000, |value| value.consume_ticks().max(0) as u32);
                let cooldown_ticks = stack
                    .get_data_component::<pumpkin_data::data_component_impl::UseCooldownImpl>()
                    .map_or(0, |value| (value.seconds.max(0.0) * 20.0) as u32);
                if stack
                    .get_data_component::<pumpkin_data::data_component_impl::UseCooldownImpl>()
                    .is_some_and(|value| {
                        !value.seconds.is_finite()
                            || value.seconds < 0.0
                            || cooldown_ticks >= 32_768
                    })
                {
                    return None;
                }
                if cooldown_ticks > 0 {
                    let cooldown = stack
                        .get_data_component::<pumpkin_data::data_component_impl::UseCooldownImpl>(
                        )?;
                    let group = cooldown
                        .cooldown_group
                        .clone()
                        .unwrap_or_else(|| item.registry_key.to_string());
                    if living_state?
                        .cooldowns
                        .get(&group)
                        .is_some_and(|state| state.until.is_after(intent.tick))
                    {
                        return None;
                    }
                }
                let before = self.local_use.get(&intent.actor).cloned();
                let after = before
                    .as_ref()
                    .filter(|state| {
                        state.source == intent.source && state.item == intent.expected_item
                    })
                    .cloned()
                    .unwrap_or_else(|| ItemUseState {
                        actor: intent.actor,
                        source: intent.source,
                        item: intent.expected_item.clone(),
                        started: intent.tick,
                        mode,
                        duration_ticks,
                        cooldown_ticks,
                        has_remainder: stack.get_data_component::<pumpkin_data::data_component_impl::UseRemainderImpl>().is_some(),
                        blocking_delay_ticks: 5,
                    });
                Some(ItemActionPlan {
                    use_transition: Some(ItemUseTransition {
                        before,
                        after: Some(after),
                    }),
                    ..ItemActionPlan::default()
                })
            }
            pumpkin_cluster::item_action::ItemActionKind::Release { .. } => {
                let before = self.local_use.get(&intent.actor)?.clone();
                if before.source != intent.source || before.item != intent.expected_item {
                    return None;
                }
                let projectile = if before.mode == ItemUseMode::Bow {
                    let fired = projectile_id
                        .and_then(|id| plan_bow_release(ledger, intent, effective, &before, id));
                    if u32::from(intent.tick.distance_since(before.started)) >= 3
                        && find_arrow(ledger, intent.source.target).is_some()
                        && fired.is_none()
                    {
                        return None;
                    }
                    fired
                } else {
                    None
                };
                let source_after = projectile
                    .as_ref()
                    .map(|effect| effect.weapon_after.clone());
                Some(ItemActionPlan {
                    use_transition: Some(ItemUseTransition {
                        before: Some(before),
                        after: None,
                    }),
                    projectile,
                    source_after,
                    ..ItemActionPlan::default()
                })
            }
            pumpkin_cluster::item_action::ItemActionKind::Complete => {
                let before = self.local_use.get(&intent.actor)?.clone();
                if before.mode != ItemUseMode::Consume
                    || before.source != intent.source
                    || before.item != intent.expected_item
                    || u32::from(intent.tick.distance_since(before.started)) < before.duration_ticks
                {
                    return None;
                }
                let snapshot = ledger.snapshot(intent.source.target)?;
                if snapshot.revision != intent.expected_inventory_revision {
                    return None;
                }
                let stack =
                    super::cluster_invsee::item_stack_from_inventory_cell(&intent.expected_item)
                        .ok()?;
                let living = living_state?;
                let actor_effects = vec![plan_food_effect(&stack, living.food)?];
                let cooldown_transition = if let Some(cooldown) =
                    stack.get_data_component::<pumpkin_data::data_component_impl::UseCooldownImpl>()
                {
                    if !cooldown.seconds.is_finite() || cooldown.seconds < 0.0 {
                        return None;
                    }
                    let ticks = (cooldown.seconds * 20.0) as u32;
                    if ticks == 0 || ticks >= 32_768 {
                        return None;
                    }
                    let group = cooldown
                        .cooldown_group
                        .clone()
                        .unwrap_or_else(|| stack.item.registry_key.to_string());
                    let previous = living.cooldowns.get(&group).cloned();
                    Some(pumpkin_cluster::item_action::ItemCooldownTransition {
                        before: previous.clone(),
                        after: pumpkin_cluster::item_action::ItemCooldownState {
                            actor: intent.actor,
                            group,
                            until: pumpkin_cluster::time::TickStamp(
                                intent.tick.0.wrapping_add(ticks as u16),
                            ),
                            revision: previous.map_or(0, |state| state.revision.wrapping_add(1)),
                        },
                    })
                } else {
                    None
                };
                let source_after;
                let remainder = if let Some(component) =
                    stack
                        .get_data_component::<pumpkin_data::data_component_impl::UseRemainderImpl>()
                {
                    let effect =
                        plan_remainder_effect(&intent.expected_item, &snapshot, component)?;
                    source_after = effect.source_after.clone();
                    Some(effect)
                } else {
                    source_after = decrement_source_cell(&intent.expected_item)?;
                    None
                };
                Some(ItemActionPlan {
                    source_after: Some(source_after),
                    use_transition: Some(ItemUseTransition {
                        before: Some(before),
                        after: None,
                    }),
                    remainder,
                    actor_effects,
                    cooldown_transition,
                    ..ItemActionPlan::default()
                })
            }
            _ => plan_item_action(server, blocks, intent),
        }
    }

    pub(super) fn capture(
        &mut self,
        staged: LocalItemActionIntent,
        effective: EffectiveItemPose,
        target: Option<ActorItemSnapshot>,
        mut plan: ItemActionPlan,
    ) -> Option<(TickBatch, TickFinish)> {
        if let ActorId::Player(actor) = staged.intent.actor
            && staged.intent.expected_item.item != 0
            && (matches!(
                &staged.intent.kind,
                pumpkin_cluster::item_action::ItemActionKind::Use { .. }
            ) && plan
                .use_transition
                .as_ref()
                .is_some_and(|transition| transition.after.is_some())
                || matches!(
                    &staged.intent.kind,
                    pumpkin_cluster::item_action::ItemActionKind::UseOnBlock { .. }
                ) && plan.source_after.is_some())
        {
            plan.statistics
                .push(pumpkin_cluster::item_action::ItemStatisticMutation {
                    actor,
                    category: pumpkin_data::statistic::StatisticCategory::Used as i32,
                    statistic: i32::from(staged.intent.expected_item.item),
                    amount: 1,
                });
        }
        let next = self.sequences.entry(staged.intent.actor).or_default();
        let sequence = staged.input_sequence.unwrap_or(PlayerSeq(*next));
        *next = sequence.0.wrapping_add(1);
        let action = staged.intent.capture(sequence, effective, target, plan);
        let key = ActionKey::item_action(&action);
        let mut batch = TickBatch::new(action.tick);
        batch.item_action.push(action);
        let finish = TickFinish::try_from_batch(&batch).ok()?;
        self.origins.insert(key, staged.origin);
        if let Some(transition) = &batch.item_action[0].use_transition {
            set_use_state(
                &mut self.local_use,
                batch.item_action[0].actor,
                transition.after.clone(),
            );
        }
        Some((batch, finish))
    }

    pub(super) fn preflight_use(&self, action: &ItemAction) -> Result<(), ItemActionRejection> {
        let Some(transition) = &action.use_transition else {
            return Ok(());
        };
        if self.ground_use.get(&action.actor) != transition.before.as_ref() {
            return Err(ItemActionRejection::StaleActor);
        }
        Ok(())
    }

    pub(super) fn settle_use(&mut self, action: &ItemAction, accepted: bool) {
        let Some(transition) = &action.use_transition else {
            return;
        };
        if accepted {
            set_use_state(&mut self.ground_use, action.actor, transition.after.clone());
            if self.local_use.get(&action.actor) == transition.before.as_ref() {
                set_use_state(&mut self.local_use, action.actor, transition.after.clone());
            }
        } else if self.local_use.get(&action.actor) == transition.after.as_ref() {
            set_use_state(&mut self.local_use, action.actor, transition.before.clone());
        }
    }

    pub(super) fn take_origin(&mut self, action: &ItemAction) -> Option<u64> {
        self.origins
            .remove(&ActionKey::item_action(action))
            .flatten()
    }

    pub(super) fn reject_unplanned(
        &mut self,
        server: &Server,
        ledger: &InvLedger,
        staged: LocalItemActionIntent,
    ) -> Result<(), InventoryApplyError> {
        let next = self.sequences.entry(staged.intent.actor).or_default();
        let sequence = staged.input_sequence.unwrap_or(PlayerSeq(*next));
        let key = ActionKey {
            incarnation: 0,
            actor: staged.intent.actor,
            sequence: sequence.0,
            family: 10,
        };
        *next = sequence.0.wrapping_add(1);
        let snapshot = ledger.snapshot(staged.intent.source.target);
        let correction = snapshot.as_ref().map_or_else(
            || InventoryDelta {
                revisions: Vec::new(),
                operations: Vec::new(),
            },
            |snapshot| {
                let actual = snapshot
                    .cells
                    .iter()
                    .find(|cell| cell.loc == staged.intent.source.loc)
                    .cloned()
                    .unwrap_or_else(|| {
                        pumpkin_cluster::inventory::InventoryCell::empty(staged.intent.source.loc)
                    });
                InventoryDelta {
                    revisions: vec![(
                        snapshot.target,
                        staged.intent.expected_inventory_revision,
                        snapshot.revision,
                    )],
                    operations: (actual != staged.intent.expected_item)
                        .then_some(SemanticInventoryOp {
                            target: snapshot.target,
                            before: staged.intent.expected_item.clone(),
                            after: actual,
                        })
                        .into_iter()
                        .collect(),
                }
            },
        );
        let actor = staged.intent.actor;
        let kind = staged.intent.kind;
        let origin = staged.origin;
        let result = super::cluster_invsee::publish_inventory_resolution(
            server,
            InventoryResolution {
                action: key,
                tick: staged.intent.tick,
                result: Err(InventoryApplyError::Rejected),
                authoritative: snapshot.into_iter().collect(),
                correction,
                effects: Vec::new(),
                menu_effects: Vec::new(),
            },
            origin,
        );
        if let ActorId::Player(gid) = actor {
            super::cluster_inventory_protocol::publish_item_action_origin(
                server, gid, &kind, origin, false,
            );
        }
        result
    }

    pub(super) fn publish_origin(
        &self,
        server: &Server,
        ledger: &InvLedger,
        publication: ItemActionPublication,
        result: Result<InventoryDelta, InventoryApplyError>,
    ) -> Result<(), InventoryApplyError> {
        let snapshot = ledger.snapshot(publication.action.source.target);
        let correction = snapshot.as_ref().map_or_else(
            || InventoryDelta {
                revisions: Vec::new(),
                operations: Vec::new(),
            },
            |snapshot| {
                let actual = snapshot
                    .cells
                    .iter()
                    .find(|cell| cell.loc == publication.action.source.loc)
                    .cloned()
                    .unwrap_or_else(|| {
                        pumpkin_cluster::inventory::InventoryCell::empty(
                            publication.action.source.loc,
                        )
                    });
                InventoryDelta {
                    revisions: vec![(
                        snapshot.target,
                        publication.action.expected_inventory_revision,
                        snapshot.revision,
                    )],
                    operations: (actual != publication.action.expected_item)
                        .then_some(SemanticInventoryOp {
                            target: snapshot.target,
                            before: publication.action.expected_item.clone(),
                            after: actual,
                        })
                        .into_iter()
                        .collect(),
                }
            },
        );
        let resolution = InventoryResolution {
            action: ActionKey::item_action(&publication.action),
            tick: publication.action.tick,
            result,
            authoritative: snapshot.into_iter().collect(),
            correction,
            effects: Vec::new(),
            menu_effects: Vec::new(),
        };
        let result = super::cluster_invsee::publish_inventory_resolution(
            server,
            resolution,
            publication.origin,
        );
        if let ActorId::Player(gid) = publication.action.actor {
            super::cluster_inventory_protocol::publish_item_action_origin(
                server,
                gid,
                &publication.action.kind,
                publication.origin,
                publication.accepted,
            );
        }
        result
    }
}

pub(super) fn preflight_effects(
    blocks: &super::cluster_chunk_state::OwnedChunkStore,
    action: &ItemAction,
) -> Result<(), ItemActionRejection> {
    let _ = PreparedItemAction::from_action(action)?;
    if !action.actors.is_empty()
        || !action.actor_effects.is_empty()
        || action.combat.is_some()
        || action.projectile.is_some()
        || action.remainder.is_some()
        || action.cooldown_transition.is_some()
    {
        return Err(ItemActionRejection::UnsupportedEffect);
    }
    match &action.kind {
        pumpkin_cluster::item_action::ItemActionKind::UseOnBlock {
            world, block, hit, ..
        } => {
            let clicked_state =
                owned_block(blocks, *world, *block).ok_or(ItemActionRejection::MissingBlock)?;
            if action
                .client_claim
                .clicked_block_state
                .is_some_and(|claimed| {
                    u32::from(pumpkin_data::BlockState::to_be_network_id(clicked_state)) != claimed
                })
            {
                return Err(ItemActionRejection::StaleBlock);
            }
            if let Some(expected) = plan_block_activation(*world, *block, clicked_state, blocks) {
                if action.blocks != expected.blocks
                    || action.source_after.is_some()
                    || action.use_transition.is_some()
                {
                    return Err(ItemActionRejection::UnsupportedEffect);
                }
            } else {
                let placed = pumpkin_data::Block::from_item_id(action.expected_item.item)
                    .ok_or(ItemActionRejection::UnsupportedEffect)?;
                let target =
                    adjacent_target(*block, hit.face).ok_or(ItemActionRejection::InvalidPose)?;
                if action.expected_item.count == 0
                    || placed.states.len() != 1
                    || placed.default_state.block_entity_type != u16::MAX
                    || action.blocks.len() != 1
                    || action.use_transition.is_some()
                {
                    return Err(ItemActionRejection::UnsupportedEffect);
                }
                let after = decrement_source_cell(&action.expected_item)
                    .ok_or(ItemActionRejection::StaleItem)?;
                let mutation = &action.blocks[0];
                if mutation.world != *world
                    || mutation.block != target
                    || mutation.before != pumpkin_data::BlockStateId::AIR.as_u16()
                    || mutation.after != placed.default_state.id.as_u16()
                    || action.source_after.as_ref() != Some(&after)
                {
                    return Err(ItemActionRejection::UnsupportedEffect);
                }
            }
        }
        pumpkin_cluster::item_action::ItemActionKind::Use { .. }
        | pumpkin_cluster::item_action::ItemActionKind::Release { .. } => {
            let stack =
                super::cluster_invsee::item_stack_from_inventory_cell(&action.expected_item)
                    .map_err(|_| ItemActionRejection::StaleItem)?;
            let item = stack.item;
            let blocking = stack
                .get_data_component::<pumpkin_data::data_component_impl::BlocksAttacksImpl>()
                .is_some();
            let consumable =
                stack.get_data_component::<pumpkin_data::data_component_impl::ConsumableImpl>();
            let expected_mode = if blocking {
                ItemUseMode::Block
            } else if consumable.is_some() {
                ItemUseMode::Consume
            } else if item == &pumpkin_data::item::Item::BOW {
                ItemUseMode::Bow
            } else {
                return Err(ItemActionRejection::UnsupportedEffect);
            };
            let expected_duration =
                consumable.map_or(72_000, |value| value.consume_ticks().max(0) as u32);
            let expected_remainder = stack
                .get_data_component::<pumpkin_data::data_component_impl::UseRemainderImpl>()
                .is_some();
            if !action.blocks.is_empty()
                || action.source_after.is_some()
                || action.use_transition.as_ref().is_none_or(|transition| {
                    transition
                        .before
                        .iter()
                        .chain(transition.after.iter())
                        .any(|state| {
                            state.mode != expected_mode
                                || state.duration_ticks != expected_duration
                                || state.cooldown_ticks != 0
                                || state.has_remainder != expected_remainder
                        })
                })
            {
                return Err(ItemActionRejection::UnsupportedEffect);
            }
        }
        pumpkin_cluster::item_action::ItemActionKind::UseOnActor { .. } => {
            return Err(ItemActionRejection::UnsupportedEffect);
        }
        pumpkin_cluster::item_action::ItemActionKind::Complete => {
            return Err(ItemActionRejection::UnsupportedEffect);
        }
    }
    for mutation in &action.blocks {
        if owned_block(blocks, mutation.world, mutation.block).map(|state| state.as_u16())
            != Some(mutation.before)
            || pumpkin_data::BlockStateId::new(mutation.after).is_none()
        {
            return Err(ItemActionRejection::StaleBlock);
        }
    }
    Ok(())
}

fn adjacent_target(
    block: pumpkin_cluster::protocol::BlockPos,
    face: u8,
) -> Option<pumpkin_cluster::protocol::BlockPos> {
    let delta = match face {
        0 => (0, -1, 0),
        1 => (0, 1, 0),
        2 => (0, 0, -1),
        3 => (0, 0, 1),
        4 => (-1, 0, 0),
        5 => (1, 0, 0),
        _ => return None,
    };
    Some(pumpkin_cluster::protocol::BlockPos {
        x: block.x.checked_add(delta.0)?,
        y: block.y.checked_add(delta.1)?,
        z: block.z.checked_add(delta.2)?,
    })
}

pub(super) fn apply_effects(action: &ItemAction) -> Result<(), ItemActionRejection> {
    if !action.blocks.is_empty() {
        return Err(ItemActionRejection::UnsupportedEffect);
    }
    Ok(())
}

fn set_use_state(
    states: &mut HashMap<ActorId, ItemUseState>,
    actor: ActorId,
    state: Option<ItemUseState>,
) {
    if let Some(state) = state {
        states.insert(actor, state);
    } else {
        states.remove(&actor);
    }
}

#[derive(Debug, Clone)]
pub(super) struct ItemActionPublication {
    pub action: ItemAction,
    pub accepted: bool,
    pub origin: Option<u64>,
}

#[cfg(test)]
mod tests {
    use super::{
        damage_bow_cell, decrement_source_cell, plan_remainder_effect, toggle_single_block,
    };
    use pumpkin_data::Block;
    use pumpkin_data::item::Item;
    use pumpkin_data::item_stack::ItemStack;

    #[test]
    fn lever_and_trapdoor_round_trip_without_opening_iron_trapdoors() {
        for block in [&Block::LEVER, &Block::OAK_TRAPDOOR] {
            let original = block.default_state.id;
            let activated = toggle_single_block(block, original).unwrap();
            assert_ne!(activated, original);
            assert_eq!(toggle_single_block(block, activated), Some(original));
        }
        assert_eq!(
            toggle_single_block(&Block::IRON_TRAPDOOR, Block::IRON_TRAPDOOR.default_state.id),
            None
        );
    }

    #[test]
    fn placement_decrement_keeps_serialized_stack_count_consistent() {
        let loc = pumpkin_cluster::inventory::InvLoc::new(pumpkin_cluster::inventory::INV_MAIN, 0);
        let before = super::super::cluster_invsee::inventory_cell_from_item_stack(
            loc,
            &ItemStack::new(2, &Item::STONE),
        )
        .unwrap();
        let after = decrement_source_cell(&before).unwrap();
        assert_eq!(after.count, 1);
        assert_eq!(after.nbt, before.nbt);
        let decoded = super::super::cluster_invsee::item_stack_from_inventory_cell(&after).unwrap();
        assert_eq!(decoded.item_count, 1);
        assert_eq!(decrement_source_cell(&after).unwrap().count, 0);
    }

    #[test]
    fn bow_fire_damage_has_exact_serialized_after_state() {
        let loc = pumpkin_cluster::inventory::InvLoc::new(pumpkin_cluster::inventory::INV_MAIN, 0);
        let before = super::super::cluster_invsee::inventory_cell_from_item_stack(
            loc,
            &ItemStack::new(1, &Item::BOW),
        )
        .unwrap();
        let after = damage_bow_cell(&before).unwrap();
        let decoded = super::super::cluster_invsee::item_stack_from_inventory_cell(&after).unwrap();
        assert_eq!(decoded.get_damage(), 1);
        assert_eq!(
            super::super::cluster_invsee::item_stack_from_inventory_cell(&before)
                .unwrap()
                .get_damage(),
            0
        );
    }

    #[test]
    fn honey_bottle_remainder_conserves_source_and_destination() {
        let loc = pumpkin_cluster::inventory::InvLoc::new(pumpkin_cluster::inventory::INV_MAIN, 0);
        let target = pumpkin_cluster::inventory::InventoryTarget::Player(
            pumpkin_cluster::identity::GlobalPlayerId::new(
                pumpkin_cluster::identity::ServerId(1),
                pumpkin_cluster::identity::PlayerSlot(1),
            ),
        );
        let one = ItemStack::new(1, &Item::HONEY_BOTTLE);
        let remainder = one
            .get_data_component::<pumpkin_data::data_component_impl::UseRemainderImpl>()
            .unwrap();
        let source_one =
            super::super::cluster_invsee::inventory_cell_from_item_stack(loc, &one).unwrap();
        let inventory_one = pumpkin_cluster::inventory::InventoryStateSnapshot {
            target,
            revision: 0,
            cells: vec![source_one.clone()],
        };
        let effect_one = plan_remainder_effect(&source_one, &inventory_one, remainder).unwrap();
        let one_after =
            super::super::cluster_invsee::item_stack_from_inventory_cell(&effect_one.source_after)
                .unwrap();
        assert_eq!(one_after.item, &Item::GLASS_BOTTLE);
        assert_eq!(one_after.item_count, 1);
        assert!(effect_one.destinations.is_empty());

        let source_two = super::super::cluster_invsee::inventory_cell_from_item_stack(
            loc,
            &ItemStack::new(2, &Item::HONEY_BOTTLE),
        )
        .unwrap();
        let inventory_two = pumpkin_cluster::inventory::InventoryStateSnapshot {
            target,
            revision: 0,
            cells: vec![source_two.clone()],
        };
        let effect_two = plan_remainder_effect(&source_two, &inventory_two, remainder).unwrap();
        let source_after =
            super::super::cluster_invsee::item_stack_from_inventory_cell(&effect_two.source_after)
                .unwrap();
        let bottle_after = super::super::cluster_invsee::item_stack_from_inventory_cell(
            &effect_two.destinations[0].after,
        )
        .unwrap();
        assert_eq!(source_after.item, &Item::HONEY_BOTTLE);
        assert_eq!(source_after.item_count, 1);
        assert_eq!(bottle_after.item, &Item::GLASS_BOTTLE);
        assert_eq!(bottle_after.item_count, 1);
        assert_ne!(effect_two.destinations[0].before.loc, loc);

        let bottle_loc =
            pumpkin_cluster::inventory::InvLoc::new(pumpkin_cluster::inventory::INV_MAIN, 4);
        let bottle_before = super::super::cluster_invsee::inventory_cell_from_item_stack(
            bottle_loc,
            &ItemStack::new(2, &Item::GLASS_BOTTLE),
        )
        .unwrap();
        let inventory_merge = pumpkin_cluster::inventory::InventoryStateSnapshot {
            target,
            revision: 0,
            cells: vec![source_two.clone(), bottle_before.clone()],
        };
        let merged = plan_remainder_effect(&source_two, &inventory_merge, remainder).unwrap();
        assert_eq!(merged.destinations.len(), 1);
        assert_eq!(merged.destinations[0].before, bottle_before);
        let merged_stack = super::super::cluster_invsee::item_stack_from_inventory_cell(
            &merged.destinations[0].after,
        )
        .unwrap();
        assert_eq!(merged_stack.item, &Item::GLASS_BOTTLE);
        assert_eq!(merged_stack.item_count, 3);
    }

    #[test]
    fn apple_food_plan_caps_hunger_and_saturation_from_real_components() {
        let actor = pumpkin_cluster::actor::ActorId::Player(
            pumpkin_cluster::identity::GlobalPlayerId::new(
                pumpkin_cluster::identity::ServerId(1),
                pumpkin_cluster::identity::PlayerSlot(1),
            ),
        );
        let before = pumpkin_cluster::item_action::ActorFoodState {
            exhaustion_milli: 0,
            tick_timer: 0,
            actor,
            food: 18,
            saturation_milli: 17_000,
            revision: 4,
        };
        let apple = ItemStack::new(1, &Item::APPLE);
        let effect = super::plan_food_effect(&apple, before).unwrap();
        let pumpkin_cluster::item_action::ItemActorEffect::Food(effect) = effect;
        assert_eq!(effect.nutrition, 4);
        assert_eq!(effect.saturation_gain_milli, 2_400);
        assert_eq!(effect.after.food, 20);
        assert_eq!(effect.after.saturation_milli, 19_400);
        assert_eq!(effect.after.revision, 5);
        let golden = ItemStack::new(1, &Item::GOLDEN_APPLE);
        assert!(super::plan_food_effect(&golden, before).is_none());
    }
}

pub(super) fn derive_mending_repair(
    source: &pumpkin_cluster::inventory::InventoryCell,
    experience: u32,
) -> Result<(pumpkin_cluster::canonical_inventory::ItemTransformOutput, u32), pumpkin_cluster::canonical_action::ApplyFailure> {
    use pumpkin_cluster::canonical_action::ApplyFailure;
    use pumpkin_cluster::canonical_inventory::ItemTransformOutput;
    if source.is_empty() {
        return Ok((ItemTransformOutput { source_after: source.clone(), remainder: None }, 0));
    }
    let mut stack = super::cluster_invsee::item_stack_from_inventory_cell(source)
        .map_err(|_| ApplyFailure::InvalidAction)?;
    if !stack.is_damageable() || stack.get_enchantment_level(&pumpkin_data::Enchantment::MENDING) <= 0 {
        return Ok((ItemTransformOutput { source_after: source.clone(), remainder: None }, 0));
    }
    let damage = u32::try_from(stack.get_damage()).map_err(|_| ApplyFailure::InvalidAction)?;
    let repaired = damage.min(experience.saturating_mul(2));
    if repaired == 0 {
        return Ok((ItemTransformOutput { source_after: source.clone(), remainder: None }, 0));
    }
    stack.patch.retain(|(component, _)| *component != pumpkin_data::data_component::DataComponent::Damage);
    stack.patch.push((pumpkin_data::data_component::DataComponent::Damage,
        Some(Box::new(pumpkin_data::data_component_impl::DamageImpl { damage: (damage - repaired) as i32 }))));
    let after = super::cluster_invsee::inventory_cell_from_item_stack(source.loc, &stack)
        .map_err(|_| ApplyFailure::InvalidAction)?;
    Ok((ItemTransformOutput { source_after: after, remainder: None }, repaired / 2))
}

pub(super) fn item_cooldown_group(source: &pumpkin_cluster::inventory::InventoryCell)
    -> Result<String, pumpkin_cluster::canonical_action::ApplyFailure> {
    let stack = super::cluster_invsee::item_stack_from_inventory_cell(source)
        .map_err(|_| pumpkin_cluster::canonical_action::ApplyFailure::InvalidAction)?;
    Ok(stack.get_use_cooldown().and_then(|cooldown| cooldown.cooldown_group.clone())
        .unwrap_or_else(|| stack.item.registry_key.to_string()))
}
