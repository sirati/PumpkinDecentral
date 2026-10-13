use std::collections::BTreeMap;
use pumpkin_cluster::canonical_action::{ActionId, ActionState, ApplyFailure, Cell, ChunkPart, Effect, Target, Value};
use pumpkin_cluster::combat_action::{CombatDamageContact, CombatIntent};
use pumpkin_cluster::entity_body::{EntityPayload, EntitySpatial, ProjectileKind};
use pumpkin_cluster::projectile_tick::ProjectileTickIntent;
use pumpkin_cluster::time::TickStamp;
use pumpkin_data::tag::Taggable;
use pumpkin_util::math::{boundingbox::BoundingBox, vector3::Vector3};

pub(super) fn segment_entry(start: [f64; 3], travel: [f64; 3], minimum: [f64; 3], maximum: [f64; 3]) -> Option<f64> {
    let mut entry = 0.0_f64;
    let mut exit = 1.0_f64;
    for axis in 0..3 {
        if travel[axis].abs() < 1e-12 {
            if start[axis] < minimum[axis] || start[axis] > maximum[axis] { return None; }
        } else {
            let first = (minimum[axis] - start[axis]) / travel[axis];
            let second = (maximum[axis] - start[axis]) / travel[axis];
            entry = entry.max(first.min(second));
            exit = exit.min(first.max(second));
            if entry > exit { return None; }
        }
    }
    Some(entry)
}

pub(super) fn evaluate<S: ActionState>(context: &S, intent: &ProjectileTickIntent, id: ActionId, tick: TickStamp,
    values: &BTreeMap<Target, Cell>) -> Result<Vec<Effect>, ApplyFailure> {
    let rules = context.world_rules(intent.rules, tick)?;
    let target = intent.projectile.target();
    let Some(Cell { value: Value::Entity(body), .. }) = values.get(&target) else { return Err(ApplyFailure::Type(target)); };
    let pumpkin_cluster::item_contact::ContactIdentity::Entity { id: entity, birth } = &intent.projectile.identity else {
        return Err(ApplyFailure::InvalidAction);
    };
    if body.id != *entity || body.birth != *birth || body.owner != intent.projectile.owner || body.spatial_epoch != intent.projectile.epoch {
        return Err(ApplyFailure::Condition(target));
    }
    let EntityPayload::Projectile(projectile) = &body.payload else { return Err(ApplyFailure::Type(target)); };
    if projectile.shot_tick.is_after(tick) { return Err(ApplyFailure::InvalidAction); }
    if projectile.last_simulation_tick.is_some_and(|previous| !tick.is_after(previous)) { return Ok(Vec::new()); }
    let position = intent.original.position.map(f64::from_bits);
    if position[1] < f64::from(rules.min_y) - 64.0 || projectile.age >= 6000
        || projectile.embedded.is_some() && projectile.embedded_age >= 1200 {
        return Ok(vec![Effect::Delete { target }]);
    }
    let mut next = body.clone();
    let EntityPayload::Projectile(after) = &mut next.payload else { return Err(ApplyFailure::Type(target)); };
    after.age = after.age.checked_add(1).ok_or(ApplyFailure::Overflow(target))?;
    after.last_simulation_tick = Some(tick);
    after.fire_ticks = after.fire_ticks.saturating_sub(1);
    let travel = intent.original.velocity_micro.map(|velocity| velocity as f64 / 1_000_000.0);
    let radius = 0.125;
    let origin = Vector3::new(position[0], position[1], position[2]);
    let velocity = Vector3::new(travel[0], travel[1], travel[2]);
    let bounds = BoundingBox::new(origin.add_raw(-radius, -radius, -radius), origin.add_raw(radius, radius, radius));
    let read = |at: pumpkin_util::math::position::BlockPos| {
        if at.0.y < rules.min_y || at.0.y >= rules.min_y + rules.height as i32 {
            return Ok(pumpkin_data::Block::AIR.default_state);
        }
        let target = Target::Chunk { key: pumpkin_cluster::protocol::ChunkKey::new(intent.original.world, at.0.x.div_euclid(16), at.0.z.div_euclid(16)),
            part: ChunkPart::Block(pumpkin_cluster::protocol::BlockPos { x: at.0.x, y: at.0.y, z: at.0.z }), inventory: None };
        let Some(Cell { value: Value::Block(state), .. }) = values.get(&target) else { return Err(ApplyFailure::Condition(target)); };
        pumpkin_data::BlockStateId::new(*state).map(|state| state.to_state()).ok_or(ApplyFailure::InvalidAction)
    };
    let fluids = pumpkin_world::fluid_snapshot::sample_entity_fluid_contacts(bounds, 256, false, |at| {
        let state = read(at)?;
        Ok(state.id)
    }).map_err(|failure| match failure {
        pumpkin_world::fluid_snapshot::FluidReadFailure::Read { reason, .. } => reason,
        _ => ApplyFailure::InvalidAction,
    })?;
    let in_water = fluids.fluids.iter().any(|fluid| fluid.id == pumpkin_data::fluid::Fluid::WATER.id
        || fluid.id == pumpkin_data::fluid::Fluid::FLOWING_WATER.id);
    if in_water { after.fire_ticks = 0; }
    let collisions = pumpkin_world::collision::collect_block_collisions(bounds.stretch(velocity), 4096, read, |_, _| None)
        .map_err(|_| ApplyFailure::InvalidAction)?;
    let mut block_hit = None;
    for (index, shape) in collisions.shapes.iter().enumerate() {
        if let Some(entry) = segment_entry(position, travel,
            [shape.min.x - radius, shape.min.y - radius, shape.min.z - radius],
            [shape.max.x + radius, shape.max.y + radius, shape.max.z + radius]) {
            let block = collisions.positions.iter().find(|(end, _)| index < *end).map(|(_, position)| *position)
                .ok_or(ApplyFailure::InvalidAction)?;
            if block_hit.is_none_or(|(before, _)| entry < before) { block_hit = Some((entry, block)); }
        }
    }
    if let Some(block) = projectile.embedded {
        let block = pumpkin_util::math::position::BlockPos::new(block.x, block.y, block.z);
        let state = read(block)?;
        let tolerance = bounds.expand(0.05, 0.05, 0.05);
        if state.get_block_collision_shapes_at(&block).any(|shape| shape.at_pos(block).intersects(&tolerance)) {
            after.embedded_age = after.embedded_age.checked_add(1).ok_or(ApplyFailure::Overflow(target))?;
            return Ok(vec![Effect::Put { target, value: Value::Entity(next) }]);
        }
    }
    after.embedded = None;
    after.embedded_age = 0;
    let mut hits = Vec::new();
    for victim in &intent.victims {
        if projectile.hit_actors.contains(&victim.actor()) || projectile.shooter == victim.actor() && projectile.age < 5 { continue; }
        let at = victim.position.map(f64::from_bits);
        let half = f64::from_bits(victim.half_width) + 0.3;
        let height = f64::from_bits(victim.height);
        if let Some(entry) = segment_entry(position, travel, [at[0] - half, at[1] - 0.3, at[2] - half],
            [at[0] + half, at[1] + height + 0.3, at[2] + half])
            && block_hit.is_none_or(|(block, _)| entry <= block) {
            hits.push((entry, victim));
        }
    }
    hits.sort_by(|(first, a), (second, b)| first.total_cmp(second).then_with(|| a.actor().cmp(&b.actor())));
    let impact = hits.first().map(|(entry, _)| *entry).or_else(|| block_hit.map(|(entry, _)| entry));
    let direct_hit = hits.first().map(|(_, victim)| victim.actor());
    let mut combat = CombatIntent { equipment: intent.equipment.clone(), ..Default::default() };
    let speed = travel.iter().map(|value| value * value).sum::<f64>().sqrt();
    let impaling = if projectile.kind == ProjectileKind::Trident {
        super::cluster_invsee::item_stack_from_inventory_cell(&projectile.carried_item)
            .map_err(|_| ApplyFailure::InvalidAction)?.get_enchantment_level(&pumpkin_data::Enchantment::IMPALING).max(0) as u32
    } else { 0 };
    let mut terminal = false;
    let mut rebound = None;
    let mut struck = Vec::new();
    for (_, victim) in hits {
        struck.push(victim);
        let damage = match projectile.kind {
            ProjectileKind::Arrow | ProjectileKind::SpectralArrow => ((speed * f64::from(projectile.damage_milli) / 1000.0).ceil() * 1000.0) as u32,
            ProjectileKind::Trident => {
                let sensitive = values.get(&victim.target()).and_then(|cell| match &cell.value {
                    Value::Entity(body) => pumpkin_data::entity::EntityType::from_raw(body.kind), _ => None,
                }).is_some_and(|kind| kind.has_tag(&pumpkin_data::tag::EntityType::MINECRAFT_SENSITIVE_TO_IMPALING));
                projectile.damage_milli.saturating_add(if sensitive { impaling.saturating_mul(2500) } else { 0 })
            }
            ProjectileKind::Snowball if values.get(&victim.target()).is_some_and(|cell|
                matches!(&cell.value, Value::Entity(body) if body.kind == pumpkin_data::entity::EntityType::BLAZE.id)) => 3000,
            _ => projectile.damage_milli,
        };
        if damage > 0 {
            let extra = if projectile.critical {
                let bound = damage / 2000 + 2;
                (pumpkin_cluster::order::rank_tick_actor(id.incarnation ^ u64::from(id.sequence), tick, victim.actor()) % u64::from(bound.max(1))) as u32 * 1000
            } else { 0 };
            let horizontal = travel[0].hypot(travel[2]);
            let resistance = if projectile.knockback_level != 0 {
                let attribute = u16::from(pumpkin_data::attributes::Attributes::KNOCKBACK_RESISTANCE.id);
                let target = Target::Chunk { key: victim.chunk, part: ChunkPart::LivingProperty {
                    actor: victim.actor(), property: pumpkin_cluster::living_property::LivingProperty::Attribute(attribute) }, inventory: None };
                let Some(Value::LivingProperty(pumpkin_cluster::living_property::LivingPropertyValue::Attribute { base, modifiers, .. }))
                    = values.get(&target).map(|cell| &cell.value) else { return Err(ApplyFailure::Type(target)); };
                pumpkin_cluster::combat_action::CombatEquipmentFacts { modifiers: modifiers.clone(), ..Default::default() }
                    .attribute(attribute, base.map(f64::from_bits).unwrap_or(pumpkin_data::attributes::Attributes::KNOCKBACK_RESISTANCE.default_value)).clamp(0.0, 1.0)
            } else { 0.0 };
            if !resistance.is_finite() { return Err(ApplyFailure::InvalidAction); }
            let strength = f64::from(projectile.knockback_level) * 0.6 * (1.0 - resistance);
            let knockback = if horizontal > 0.0 { [(travel[0] / horizontal * strength * 1_000_000.0).round() as i64,
                if strength > 0.0 { 100_000 } else { 0 }, (travel[2] / horizontal * strength * 1_000_000.0).round() as i64] } else { [0;3] };
            let fire_immune = values.get(&victim.target()).is_some_and(|cell| matches!(&cell.value,
                Value::Entity(body) if pumpkin_data::entity::EntityType::from_raw(body.kind).is_some_and(|kind| kind.fire_immune)));
            combat.damage_contacts.push(CombatDamageContact { contact: victim.clone(),
                damage: intent.damage(victim, projectile.shooter, tick, damage.saturating_add(extra), knockback,
                    if after.fire_ticks > 0 && !fire_immune { 100 } else { 0 }),
                attack_origin: intent.original.position, victim_yaw: *intent.victim_yaws.get(&victim.actor()).ok_or(ApplyFailure::InvalidAction)? });
        }
        after.hit_actors.push(victim.actor());
        if after.hit_actors.len() > usize::from(projectile.pierce) {
            terminal = projectile.kind != ProjectileKind::Trident;
            if projectile.kind == ProjectileKind::Trident { rebound = Some(intent.original.velocity_micro.map(|velocity| -velocity / 100)); }
            break;
        }
    }
    combat.equipment.retain(|binding| combat.damage_contacts.iter().any(|contact|
        matches!(contact.contact.identity, pumpkin_cluster::item_contact::ContactIdentity::Player(lease) if lease == binding.lease)));
    let mut original_projectile = (**body).clone();
    original_projectile.spatial = intent.original.clone();
    let mut effects = if combat.damage_contacts.is_empty() { Vec::new() } else { combat.derive_projectile(context, id, tick, values, &original_projectile)? };
    if matches!(projectile.kind, ProjectileKind::Arrow | ProjectileKind::SpectralArrow) {
        let stack = super::cluster_invsee::item_stack_from_inventory_cell(&projectile.carried_item).map_err(|_| ApplyFailure::InvalidAction)?;
        let potion = if projectile.kind == ProjectileKind::SpectralArrow {
            vec![pumpkin_cluster::player_admin::EffectState { kind: u16::from(pumpkin_data::effect::StatusEffect::GLOWING.id), duration: 200,
                amplifier: 0, ambient: false, show_particles: true, show_icon: true, blend: false }]
        } else { super::cluster_projectile_potion::potion_effects(&stack)? };
        for victim in struck {
            let projected = project(values, &effects);
            let deltas = super::cluster_projectile_potion::apply(context, id, tick, projectile.shooter, victim.target(), &potion,
                if projectile.kind == ProjectileKind::SpectralArrow { 1.0 } else { 0.125 }, 1.0, 1,
                intent.max_health_attribute, &intent.status_attributes, &projected)?;
            merge(&mut effects, deltas);
        }
    }
    if terminal {
        if let Some(impact) = impact {
            let projected = project(values, &effects);
            let deltas = impact_effects(context, intent, id, tick, projectile,
                std::array::from_fn(|axis| position[axis] + travel[axis] * impact), direct_hit, &projected)?;
            merge(&mut effects, deltas);
        }
        effects.push(Effect::Delete { target });
        return Ok(effects);
    }
    if let Some((entry, block)) = block_hit {
        match projectile.kind {
            ProjectileKind::Arrow | ProjectileKind::SpectralArrow | ProjectileKind::Trident => {
                next.spatial = intent.original.clone();
                next.spatial.position = std::array::from_fn(|axis| (position[axis] + travel[axis] * (entry - 0.001).max(0.0)).to_bits());
                next.spatial.velocity_micro = [0;3];
                after.embedded = Some(pumpkin_cluster::protocol::BlockPos { x: block.0.x, y: block.0.y, z: block.0.z });
            }
            _ => {
                let projected = project(values, &effects);
                let deltas = impact_effects(context, intent, id, tick, projectile,
                    std::array::from_fn(|axis| position[axis] + travel[axis] * entry), direct_hit, &projected)?;
                merge(&mut effects, deltas);
                effects.push(Effect::Delete { target }); return Ok(effects);
            }
        }
    } else {
        let mut spatial = intent.original.clone();
        spatial.position = std::array::from_fn(|axis| (position[axis] + travel[axis]).to_bits());
        let gravity = match projectile.kind { ProjectileKind::Arrow | ProjectileKind::SpectralArrow | ProjectileKind::Trident
            | ProjectileKind::SplashPotion | ProjectileKind::LingeringPotion => 0.05, ProjectileKind::Firework => 0.0, ProjectileKind::ExperienceBottle => 0.07, _ => 0.03 };
        let drag = if in_water { match projectile.kind { ProjectileKind::Arrow | ProjectileKind::SpectralArrow => 0.6,
            ProjectileKind::Trident => 0.99, _ => 0.8 } } else { 0.99 };
        spatial.velocity_micro = rebound.unwrap_or_else(|| std::array::from_fn(|axis|
            ((travel[axis] * drag - if axis == 1 { gravity } else { 0.0 }) * 1_000_000.0).round() as i64));
        spatial.on_ground = false;
        let yaw = (-travel[0]).atan2(travel[2]).to_degrees() as f32;
        let pitch = (-travel[1]).atan2(travel[0].hypot(travel[2])).to_degrees() as f32;
        spatial.yaw = yaw.to_bits(); spatial.pitch = pitch.to_bits();
        next.spatial = spatial;
    }
    effects.push(Effect::Put { target, value: Value::Entity((next).into()) });
    Ok(effects)
}

fn impact_effects<S: ActionState>(context: &S, intent: &ProjectileTickIntent, id: ActionId, tick: TickStamp,
    projectile: &pumpkin_cluster::entity_body::ProjectileBody, position: [f64; 3], direct_hit: Option<pumpkin_cluster::actor::ActorId>,
    values: &BTreeMap<Target, Cell>) -> Result<Vec<Effect>, ApplyFailure> {
    if projectile.kind == ProjectileKind::SplashPotion {
        return splash_effects(context, intent, id, tick, projectile, position, direct_hit, values);
    }
    if projectile.kind == ProjectileKind::EnderPearl {
        return pearl_effects(context, intent, id, tick, projectile, position, values);
    }
    if projectile.kind != ProjectileKind::ExperienceBottle { return Ok(Vec::new()); }
    let carried = super::cluster_invsee::item_stack_from_inventory_cell(&projectile.carried_item)
        .map_err(|_| ApplyFailure::InvalidAction)?;
    if carried.item != &pumpkin_data::item::Item::EXPERIENCE_BOTTLE || intent.births.len() != 3 {
        return Err(ApplyFailure::InvalidAction);
    }
    let seed = id.incarnation ^ (u64::from(id.sequence) << 32);
    let mut amount = 3 + (pumpkin_cluster::order::rank_tick_actor(seed, tick, id.actor) % 5) as u32
        + (pumpkin_cluster::order::rank_tick_actor(seed ^ 0xD1B54A32D192ED03, tick, id.actor) % 5) as u32;
    let chunk = pumpkin_cluster::protocol::ChunkKey::new(intent.original.world,
        (position[0].floor() as i32).div_euclid(16), (position[2].floor() as i32).div_euclid(16));
    let primary = context.scheduled_executor(chunk)?;
    let mut effects = Vec::new();
    for identity in &intent.births {
        if amount == 0 { break; }
        let value = [7, 3, 1].into_iter().find(|value| *value <= amount).ok_or(ApplyFailure::InvalidAction)?;
        let spatial = EntitySpatial { world: chunk.world, position: position.map(f64::to_bits),
            velocity_micro: [0, 200_000, 0], yaw: 0_f32.to_bits(), pitch: 0_f32.to_bits(), on_ground: false };
        let body = super::cluster_item_birth::reserved_experience_body(pumpkin_cluster::canonical_action::ActionRef { tick, id }, identity,
            primary, spatial, value).map_err(|_| ApplyFailure::InvalidAction)?;
        effects.push(Effect::Put { target: Target::Chunk { key: chunk, part: ChunkPart::Entity(identity.id), inventory: None }, value: Value::Entity((body).into()) });
        amount -= value;
    }
    if amount != 0 { return Err(ApplyFailure::InvalidAction); }
    Ok(effects)
}

fn pearl_effects<S: ActionState>(context: &S, intent: &ProjectileTickIntent, id: ActionId, tick: TickStamp,
    projectile: &pumpkin_cluster::entity_body::ProjectileBody, position: [f64;3], values: &BTreeMap<Target, Cell>) -> Result<Vec<Effect>, ApplyFailure> {
    use pumpkin_cluster::combat_action::{DamageIntent, DamageKind, DamageProtection, DamageReductionFacts};
    use pumpkin_cluster::living_property::{LivingProperty as P, LivingPropertyValue as V};
    let Some(shooter) = &intent.shooter else { return Ok(Vec::new()); };
    let provenance = match &shooter.identity {
        pumpkin_cluster::item_contact::ContactIdentity::Player(lease) => projectile.shooter_lease == Some(*lease),
        pumpkin_cluster::item_contact::ContactIdentity::Entity { id, birth } => projectile.shooter == pumpkin_cluster::actor::ActorId::Entity(*id)
            && projectile.shooter_birth.as_ref() == Some(birth),
    };
    if !provenance || shooter.actor() != projectile.shooter { return Err(ApplyFailure::InvalidAction); }
    let mounted = match values.get(&shooter.target()).map(|cell| &cell.value) {
        Some(Value::Living(living)) => living.avatar.as_ref().is_some_and(|avatar| avatar.mount.is_some()),
        Some(Value::Entity(body)) => body.mount.is_some(),
        _ => return Err(ApplyFailure::Condition(shooter.target())),
    };
    if mounted { return Ok(Vec::new()); }
    let health = Target::Chunk { key: shooter.chunk, part: ChunkPart::LivingProperty { actor: shooter.actor(), property: P::Health }, inventory: None };
    if !values.get(&health).is_some_and(|cell| matches!(cell.value, Value::LivingProperty(V::Unsigned(value)) if value > 0)) {
        return Ok(Vec::new());
    }
    let damage = DamageIntent { kind: DamageKind::Fall, source: shooter.actor(), target: shooter.target(), tick,
        damage_milli: 5000, knockback_micro: [0;3], bypass_hurt_cooldown: false, fire_ticks: 0, bypass_invulnerability: false,
        reduction: Some(DamageReductionFacts { armor_attribute: intent.armor_attribute, toughness_attribute: intent.toughness_attribute,
            bypass_armor: true, bypass_effects: false, bypass_enchantments: false, protection: DamageProtection::Fall, breach: 0 }) };
    let mut effects = CombatIntent { damages: vec![damage], ..Default::default() }.derive_with(context, id, tick, values)?;
    let destination = pumpkin_cluster::protocol::ChunkKey::new(shooter.world, (position[0].floor() as i32).div_euclid(16), (position[2].floor() as i32).div_euclid(16));
    effects.push(Effect::ProjectileShooterTeleport { intent: pumpkin_cluster::projectile_teleport::ProjectileShooterTeleport {
        shooter: shooter.clone(), destination, position: position.map(f64::to_bits),
        projectile_birth: match &intent.projectile.identity { pumpkin_cluster::item_contact::ContactIdentity::Entity { birth, .. } => birth.clone(), _ => return Err(ApplyFailure::InvalidAction) },
        projectile_epoch: intent.projectile.epoch,
    } });
    Ok(effects)
}

fn project(values: &BTreeMap<Target, Cell>, effects: &[Effect]) -> BTreeMap<Target, Cell> {
    let mut projected = values.clone();
    for effect in effects {
        if let Effect::Put { target, value } = effect {
            let revision = projected.get(target).map_or(0, |cell| cell.revision);
            projected.insert(*target, Cell { revision, value: value.clone() });
        }
    }
    projected
}

fn merge(effects: &mut Vec<Effect>, incoming: Vec<Effect>) {
    for effect in incoming {
        if let Effect::Put { target, .. } = &effect {
            effects.retain(|existing| !matches!(existing, Effect::Put { target: written, .. } if written == target));
        }
        effects.push(effect);
    }
}

fn splash_effects<S: ActionState>(context: &S, intent: &ProjectileTickIntent, id: ActionId, tick: TickStamp,
    projectile: &pumpkin_cluster::entity_body::ProjectileBody, position: [f64; 3], direct: Option<pumpkin_cluster::actor::ActorId>,
    values: &BTreeMap<Target, Cell>) -> Result<Vec<Effect>, ApplyFailure> {
    let stack = super::cluster_invsee::item_stack_from_inventory_cell(&projectile.carried_item).map_err(|_| ApplyFailure::InvalidAction)?;
    if stack.item != &pumpkin_data::item::Item::SPLASH_POTION { return Err(ApplyFailure::InvalidAction); }
    let potion = super::cluster_projectile_potion::potion_effects(&stack)?;
    let water = stack.get_data_component::<pumpkin_data::data_component_impl::PotionContentsImpl>()
        .is_some_and(|contents| contents.potion_id == Some(i32::from(pumpkin_data::potion::Potion::WATER.id)));
    let mut effects = Vec::new();
    for victim in &intent.victims {
        let at = victim.position.map(f64::from_bits);
        let distance = (0..3).map(|axis| (at[axis] - position[axis]).powi(2)).sum::<f64>().sqrt();
        if distance >= 4.0 { continue; }
        let projected = project(values, &effects);
        let intensity = if direct == Some(victim.actor()) { 1.0 } else { 1.0 - distance / 4.0 };
        let deltas = super::cluster_projectile_potion::apply(context, id, tick, projectile.shooter, victim.target(), &potion,
            intensity, 1.0, 20, intent.max_health_attribute, &intent.status_attributes, &projected)?;
        merge(&mut effects, deltas);
        if water {
            let target = Target::Chunk { key: victim.chunk, part: ChunkPart::LivingProperty { actor: victim.actor(),
                property: pumpkin_cluster::living_property::LivingProperty::FireTicks }, inventory: None };
            effects.retain(|effect| !matches!(effect, Effect::Put { target: written, .. } if *written == target));
            effects.push(Effect::Put { target, value: Value::LivingProperty(pumpkin_cluster::living_property::LivingPropertyValue::Unsigned(0)) });
        }
    }
    Ok(effects)
}

pub(super) fn derive<S: ActionState>(context: &S, intent: &ProjectileTickIntent, id: ActionId, tick: TickStamp,
    values: &BTreeMap<Target, Cell>) -> Result<Vec<Effect>, ApplyFailure> {
    let mut effects = evaluate(context, intent, id, tick, values)?;
    for effect in &mut effects {
        if let Effect::Put { target, value: Value::Entity(body) } = effect
            && *target == intent.projectile.target() && !body.spatial.belongs_to(intent.projectile.chunk) {
            let Some(Cell { value: Value::Entity(previous), .. }) = values.get(target) else { return Err(ApplyFailure::Type(*target)); };
            body.spatial = previous.spatial.clone();
        }
    }
    Ok(effects)
}
