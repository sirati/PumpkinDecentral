use serde::{Deserialize, Serialize};

use crate::actor::ActorId;
use crate::canonical_action::{
    Action, ActionClass, ActionId, ActionKind, ApplyFailure, Cell, ClientOrigin, Condition, Effect,
    Expected, SemanticIntent, Target, UndoProcedure, Value,
};
use crate::entity_body::EntityPayload;
use crate::item_action::ActorLivingState;
use crate::time::TickStamp;

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct DamageIntent {
    pub kind: DamageKind,
    pub source: ActorId,
    pub target: Target,
    pub tick: TickStamp,
    pub damage_milli: u32,
    pub knockback_micro: [i64; 3],
    pub bypass_hurt_cooldown: bool,
    pub fire_ticks: u32,
    pub reduction: Option<DamageReductionFacts>,
    pub bypass_invulnerability: bool,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub enum DamageKind {
    PlayerAttack,
    MobAttack,
    Magic,
    OnFire,
    InFire,
    Lava,
    Cactus,
    SweetBerry,
    Suffocation,
    Freeze,
    Wither,
    Starvation,
    Explosion,
    FallingBlock,
    Anvil,
    Fall,
    Drown,
    OutOfWorld,
    Projectile,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct RespawnClaim {
    pub world_plan: Option<crate::world_respawn::WorldRespawnPlan>,
    pub fallback_plan: Option<crate::world_respawn::WorldRespawnPlan>,
    pub owner_install_generation: u64,
    pub source_epoch: crate::canonical_action::SpatialEpoch,
    pub death_cause: Option<crate::canonical_action::ActionRef>,
    pub destination_chunk: crate::protocol::ChunkKey,
    pub avatar: crate::item_action::PlayerAvatarState,
    pub max_health_attribute: u16,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub struct DamageReductionFacts {
    pub armor_attribute: u16,
    pub toughness_attribute: u16,
    pub bypass_armor: bool,
    pub bypass_effects: bool,
    pub bypass_enchantments: bool,
    pub protection: DamageProtection,
    pub breach: u16,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub enum DamageProtection {
    General,
    Fire,
    Explosion,
    Projectile,
    Fall,
}

pub fn explosion_damage(
    contact: &crate::item_contact::ContactObservation,
    source: ActorId,
    center: [f64; 3],
    diameter: f64,
    exposure: f64,
    eye_height: f64,
    knockback_resistance: f64,
    armor_attribute: u16,
    toughness_attribute: u16,
) -> Result<Option<DamageIntent>, ApplyFailure> {
    if !contact.valid()
        || center
            .iter()
            .any(|value| !value.is_finite() || value.abs() > 30_000_000.0)
        || !diameter.is_finite()
        || !(0.0..=256.0).contains(&diameter)
        || diameter == 0.0
        || !exposure.is_finite()
        || !(0.0..=1.0).contains(&exposure)
        || !eye_height.is_finite()
        || !(0.0..=f64::from_bits(contact.height)).contains(&eye_height)
        || !knockback_resistance.is_finite()
        || !(0.0..=1.0).contains(&knockback_resistance)
        || matches!(source, ActorId::Peer(_))
    {
        return Err(ApplyFailure::InvalidAction);
    }
    let position = contact.position.map(f64::from_bits);
    let displacement = core::array::from_fn::<_, 3, _>(|axis| position[axis] - center[axis]);
    let distance = displacement
        .iter()
        .map(|value| value * value)
        .sum::<f64>()
        .sqrt()
        / diameter;
    if distance > 1.0 {
        return Ok(None);
    }
    let impact = (1.0 - distance) * exposure;
    let damage = ((impact * impact + impact) * 0.5 * 7.0 * diameter + 1.0).floor() * 1000.0;
    let direction = [
        displacement[0],
        displacement[1] + eye_height,
        displacement[2],
    ];
    let length = direction
        .iter()
        .map(|value| value * value)
        .sum::<f64>()
        .sqrt();
    let strength = impact * (1.0 - knockback_resistance);
    let knockback_micro = if length > 0.0 {
        direction.map(|value| (value / length * strength * 1_000_000.0).round() as i64)
    } else {
        [0; 3]
    };
    Ok(Some(DamageIntent {
        kind: DamageKind::Explosion,
        source,
        target: contact.target(),
        tick: contact.simulation_tick,
        damage_milli: damage as u32,
        knockback_micro,
        bypass_hurt_cooldown: false,
        fire_ticks: 0,
        reduction: Some(DamageReductionFacts {
            armor_attribute,
            toughness_attribute,
            bypass_armor: false,
            bypass_effects: false,
            bypass_enchantments: false,
            protection: DamageProtection::Explosion,
            breach: 0,
        }),
        bypass_invulnerability: false,
    }))
}

pub fn instant_health_targets(body: Target, max_health_attribute: u16) -> Vec<Target> {
    use crate::canonical_action::ChunkPart;
    use crate::living_property::LivingProperty as P;
    let Target::Chunk {
        key,
        part,
        inventory: None,
    } = body
    else {
        return Vec::new();
    };
    let actor = match part {
        ChunkPart::Player(id) => ActorId::Player(id),
        ChunkPart::Entity(id) => ActorId::Entity(id),
        _ => return Vec::new(),
    };
    let mut targets: Vec<_> = [
        P::Health,
        P::Absorption,
        P::VelocityX,
        P::VelocityY,
        P::VelocityZ,
        P::LastDamage,
        P::LastDamageKind,
        P::HurtStarted,
        P::HurtTicks,
        P::FireTicks,
        P::DeathTicks,
        P::DeathCause,
        P::LastAttacker,
        P::Invulnerable,
        P::ItemUse,
        P::Resistance,
        P::ArmorProtection,
        P::Attribute(max_health_attribute),
    ]
    .into_iter()
    .map(|property| Target::Chunk {
        key,
        part: ChunkPart::LivingProperty { actor, property },
        inventory: None,
    })
    .collect();
    if matches!(actor, ActorId::Player(_)) {
        targets.push(Target::Chunk {
            key,
            part: ChunkPart::LivingProperty {
                actor,
                property: P::GameMode,
            },
            inventory: None,
        });
    }
    targets
}

pub fn derive_instant_health(
    id: ActionId,
    body: Target,
    actor: ActorId,
    tick: TickStamp,
    healing: bool,
    amount_milli: u32,
    max_health_attribute: u16,
    values: &std::collections::BTreeMap<Target, Cell>,
) -> Result<Vec<Effect>, ApplyFailure> {
    let targets = instant_health_targets(body, max_health_attribute);
    if targets.is_empty()
        || amount_milli == 0
        || !matches!(targets.first(), Some(Target::Chunk {
        part: crate::canonical_action::ChunkPart::LivingProperty { actor: target_actor, .. }, ..
    }) if *target_actor == actor)
    {
        return Err(ApplyFailure::InvalidAction);
    }
    let mut living = ActorLivingState {
        permanent_attribute_modifiers: Default::default(),
        menu: Default::default(),
        environment: Default::default(),
        tags: Default::default(),
        tags_revision: 1,
        advancements: Default::default(),
        stand: None,
        mob: None,
        merchant: None,
        simulation_tick: None,
        effects: Vec::new(),
        hidden_effects: Default::default(),
        property_versions: Default::default(),
        food: crate::item_action::ActorFoodState {
            exhaustion_milli: 0,
            tick_timer: 0,
            actor,
            food: 20,
            saturation_milli: 0,
            revision: 0,
        },
        combat: crate::item_action::ActorCombatState {
            actor,
            revision: 0,
            health_milli: 0,
            absorption_milli: 0,
            velocity_micro: [0; 3],
            timing: Default::default(),
            effective_attributes: Default::default(),
            equipment: Default::default(),
            status: Default::default(),
        },
        use_state: None,
        cooldowns: Default::default(),
        avatar: None,
    };
    for target in &targets {
        let (_, property) =
            crate::living_property::body_target(*target).ok_or(ApplyFailure::Type(*target))?;
        let Some(Cell {
            value: Value::LivingProperty(value),
            ..
        }) = values.get(target)
        else {
            return Err(ApplyFailure::Condition(*target));
        };
        if property == crate::living_property::LivingProperty::GameMode {
            let crate::living_property::LivingPropertyValue::Unsigned(mode @ 0..=3) = value else {
                return Err(ApplyFailure::Type(*target));
            };
            if !healing && matches!(*mode, 1 | 3) {
                return Ok(Vec::new());
            }
            continue;
        }
        if !property.hydrate(&mut living, value) {
            return Err(ApplyFailure::Type(*target));
        }
    }
    let before = living.clone();
    if healing {
        let base = living
            .combat
            .effective_attributes
            .get(&max_health_attribute)
            .copied()
            .map(f64::from_bits)
            .ok_or(ApplyFailure::InvalidAction)?;
        let maximum = living.attribute(max_health_attribute, base);
        if !maximum.is_finite() || !(0.0..=u32::MAX as f64 / 1000.0).contains(&maximum) {
            return Err(ApplyFailure::InvalidAction);
        }
        if living.combat.health_milli != 0 {
            living.combat.health_milli = living
                .combat
                .health_milli
                .saturating_add(amount_milli)
                .min((maximum * 1000.0).round() as u32);
        }
    } else {
        DamageIntent {
            kind: DamageKind::Magic,
            source: actor,
            target: body,
            tick,
            damage_milli: amount_milli,
            knockback_micro: [0; 3],
            bypass_hurt_cooldown: false,
            fire_ticks: 0,
            reduction: Some(DamageReductionFacts {
                armor_attribute: 0,
                toughness_attribute: 0,
                bypass_armor: true,
                bypass_effects: false,
                bypass_enchantments: false,
                protection: DamageProtection::General,
                breach: 0,
            }),
            bypass_invulnerability: false,
        }
        .apply(&mut living)?;
    }
    let mut effects: Vec<_> = targets
        .into_iter()
        .filter_map(|target| {
            let (_, property) = crate::living_property::body_target(target)?;
            let after = property.read(&living);
            (property.read(&before) != after).then_some(Effect::Put {
                target,
                value: Value::LivingProperty(after),
            })
        })
        .collect();
    append_death_causes(id, tick, values, &mut effects)?;
    Ok(effects)
}

pub(crate) fn append_death_causes(
    id: ActionId,
    tick: TickStamp,
    values: &std::collections::BTreeMap<Target, Cell>,
    effects: &mut Vec<Effect>,
) -> Result<(), ApplyFailure> {
    use crate::canonical_action::ChunkPart;
    use crate::living_property::{LivingProperty as P, LivingPropertyValue as V};
    let mut causes = Vec::new();
    for effect in effects.iter() {
        let Effect::Put {
            target:
                Target::Chunk {
                    key,
                    part:
                        ChunkPart::LivingProperty {
                            actor,
                            property: P::Health,
                        },
                    inventory: None,
                },
            value: Value::LivingProperty(V::Unsigned(0)),
        } = effect
        else {
            continue;
        };
        let health = Target::Chunk {
            key: *key,
            part: ChunkPart::LivingProperty {
                actor: *actor,
                property: P::Health,
            },
            inventory: None,
        };
        let before = values
            .get(&health)
            .cloned()
            .or_else(|| {
                crate::living_property::body_target(health).and_then(|(body, property)| {
                    values
                        .get(&body)
                        .and_then(|cell| crate::derived_living::facet_cell(cell, property))
                })
            })
            .ok_or(ApplyFailure::Condition(health))?;
        if matches!(before.value, Value::LivingProperty(V::Unsigned(value)) if value > 0) {
            causes.push(Effect::Put {
                target: Target::Chunk {
                    key: *key,
                    part: ChunkPart::LivingProperty {
                        actor: *actor,
                        property: P::DeathCause,
                    },
                    inventory: None,
                },
                value: Value::LivingProperty(V::ActionReference(Some(
                    crate::canonical_action::ActionRef { id, tick },
                ))),
            });
        }
    }
    effects.extend(causes);
    Ok(())
}

impl DamageReductionFacts {
    fn reduce(self, damage_milli: u32, living: &ActorLivingState) -> Result<u32, ApplyFailure> {
        self.reduce_parts(damage_milli, &living.combat.status, &living.combat.equipment, |id| {
            let base = living.combat.effective_attributes.get(&id).copied()
                .map(f64::from_bits).ok_or(ApplyFailure::InvalidAction)?;
            let value = living.attribute(id, base);
            value.is_finite().then_some(value as f32).ok_or(ApplyFailure::InvalidAction)
        })
    }

    pub(crate) fn reduce_parts(self, damage_milli: u32, status: &CombatStatusFacts,
        equipment: &CombatEquipmentFacts, attribute: impl Fn(u16) -> Result<f32, ApplyFailure>)
        -> Result<u32, ApplyFailure> {
        if self.protection == DamageProtection::Fire && !self.bypass_effects && status.fire_resistance {
            return Ok(0);
        }
        let mut damage = damage_milli as f32 / 1000.0;
        if !self.bypass_armor {
            damage = damage_after_armor(damage, attribute(self.armor_attribute)?,
                attribute(self.toughness_attribute)?, u32::from(self.breach));
        }
        if !self.bypass_effects {
            if let Some(level) = status.resistance {
                damage *= (1.0 - (f32::from(level) + 1.0) * 0.2).max(0.0);
            }
        }
        if !self.bypass_enchantments {
            let bonus = match self.protection {
                DamageProtection::General => 0,
                DamageProtection::Fire => equipment.fire_protection.saturating_mul(2),
                DamageProtection::Explosion => equipment.blast_protection.saturating_mul(2),
                DamageProtection::Projectile => equipment.projectile_protection.saturating_mul(2),
                DamageProtection::Fall => equipment.feather_falling.saturating_mul(3),
            };
            damage = damage_after_protection(damage, equipment.protection.saturating_add(bonus) as f32);
        }
        if !damage.is_finite() || damage < 0.0 || damage as f64 * 1000.0 > f64::from(u32::MAX) {
            return Err(ApplyFailure::InvalidAction);
        }
        Ok((damage * 1000.0).round() as u32)
    }
}

impl DamageIntent {
    pub fn derive_effect(&self, cell: &Cell) -> Result<Option<Effect>, ApplyFailure> {
        let mut value = cell.value.clone();
        let living = match &mut value {
            Value::Living(living) => living,
            Value::Entity(body) => match &mut body.payload {
                EntityPayload::Living(living) => living,
                _ => return Err(ApplyFailure::Type(self.target)),
            },
            _ => return Err(ApplyFailure::Type(self.target)),
        };
        if !self.apply(living)? {
            return Ok(None);
        }
        if let Value::Entity(body) = &mut value {
            if let EntityPayload::Living(living) = &body.payload {
                body.spatial.velocity_micro = living.combat.velocity_micro;
            }
        }
        Ok(Some(Effect::Put {
            target: self.target,
            value,
        }))
    }

    fn apply_accounted(&self, living: &mut ActorLivingState) -> Result<(u32, u32), ApplyFailure> {
        let health = living.combat.health_milli;
        let absorption = living.combat.absorption_milli;
        self.apply(living)?;
        Ok((
            health.saturating_sub(living.combat.health_milli),
            absorption.saturating_sub(living.combat.absorption_milli),
        ))
    }

    fn apply(&self, living: &mut ActorLivingState) -> Result<bool, ApplyFailure> {
        let Some((damage, recent)) = self.pending_damage(&living.combat,
            living.avatar.as_ref().map(|avatar| avatar.game_mode)) else { return Ok(false); };
        let damage = self.reduction.map_or(Ok(damage), |reduction| reduction.reduce(damage, living))?;
        let applied = self.apply_resolved_damage(&mut living.combat, damage, recent)?;
        if applied && living.combat.health_milli == 0 { living.use_state = None; }
        Ok(applied)
    }

    pub(crate) fn pending_damage(&self, combat: &crate::item_action::ActorCombatState,
        game_mode: Option<u8>) -> Option<(u32, bool)> {
        if !self.bypass_invulnerability && game_mode.is_some_and(|mode| matches!(mode, 1 | 3))
            || combat.health_milli == 0 || combat.status.invulnerable && !self.bypass_invulnerability
            || self.damage_milli == 0 { return None; }
        let recent = combat.timing.last_damage_tick.is_some_and(|tick|
            self.tick == tick || self.tick.is_after(tick) && self.tick.distance_since(tick) < 10);
        let damage = if recent && !self.bypass_hurt_cooldown {
            self.damage_milli.checked_sub(combat.timing.last_damage_milli).filter(|damage| *damage != 0)?
        } else { self.damage_milli };
        Some((damage, recent))
    }

    pub(crate) fn apply_resolved_damage(&self, combat: &mut crate::item_action::ActorCombatState,
        damage: u32, recent: bool) -> Result<bool, ApplyFailure> {
        if damage == 0 { return Ok(false); }
        let mut velocity = combat.velocity_micro;
        for (velocity, knockback) in velocity.iter_mut().zip(self.knockback_micro) {
            *velocity = velocity.checked_add(knockback).ok_or(ApplyFailure::InvalidAction)?;
        }
        let absorbed = combat.absorption_milli.min(damage);
        combat.absorption_milli -= absorbed;
        combat.health_milli = combat.health_milli.saturating_sub(damage - absorbed);
        combat.velocity_micro = velocity;
        combat.revision = combat.revision.wrapping_add(1);
        combat.timing.last_damage_milli = self.damage_milli;
        combat.timing.last_damage_kind = Some(self.kind);
        if !recent || self.bypass_hurt_cooldown {
            combat.timing.last_damage_tick = Some(self.tick);
            combat.timing.hurt_ticks = 20;
        }
        combat.timing.fire_ticks = combat.timing.fire_ticks.max(self.fire_ticks);
        if self.source != combat.actor { combat.timing.last_attacker = Some(self.source); }
        if combat.health_milli == 0 { combat.timing.death_ticks = 0; }
        Ok(true)
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct AttackClaim {
    pub attacker: crate::identity::GlobalPlayerId,
    pub target: ActorId,
    pub target_chunk: crate::protocol::ChunkKey,
    pub install_generation: u64,
    pub sequence: crate::identity::PlayerSeq,
    pub tick: TickStamp,
    pub target_observation: crate::item_contact::ContactObservation,
    pub target_kind: u16,
    pub target_yaw: u32,
    pub target_on_ground: bool,
    pub bedrock: bool,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct CombatDamageContact {
    pub contact: crate::item_contact::ContactObservation,
    pub damage: DamageIntent,
    pub attack_origin: [u64; 3],
    pub victim_yaw: u32,
}

impl CombatDamageContact {
    pub fn valid(&self, tick: TickStamp) -> bool {
        self.contact.valid() && self.contact.simulation_tick == tick
            && self.damage.tick == tick && self.damage.target == self.contact.target()
            && self.attack_origin.iter().all(|bits| f64::from_bits(*bits).is_finite() && f64::from_bits(*bits).abs() <= 30_000_000.0)
            && f32::from_bits(self.victim_yaw).is_finite()
    }

    fn blocked_damage(&self, living: &ActorLivingState) -> u32 {
        if !living.combat.equipment.shield || !living.use_state.as_ref().is_some_and(|state|
            state.mode == crate::item_action::ItemUseMode::Block
                && (self.damage.tick == state.started || self.damage.tick.is_after(state.started))
                && self.damage.tick.distance_since(state.started) >= u16::from(state.blocking_delay_ticks)) { return 0; }
        let source = self.attack_origin.map(f64::from_bits);
        let victim = self.contact.position.map(f64::from_bits);
        let yaw = f64::from(f32::from_bits(self.victim_yaw)).to_radians();
        if (source[0] - victim[0]).mul_add(-yaw.sin(), (source[2] - victim[2]) * yaw.cos()) > 0.0 {
            self.damage.damage_milli
        } else { 0 }
    }
}

#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize, Deserialize)]
pub struct CombatIntent {
    pub damage_contacts: Vec<CombatDamageContact>,
    pub falls: Vec<FallDamageIntent>,
    pub damage_parent: Option<DamagingDependency>,
    pub environment: Vec<EnvironmentalDamageIntent>,
    pub exhaustions: Vec<FoodExhaustionIntent>,
    pub ticks: Vec<LivingTickIntent>,
    pub damages: Vec<DamageIntent>,
    pub attacks: Vec<AttackIntent>,
    pub vehicle_attacks: Vec<crate::vehicle_attack::VehicleAttackIntent>,
    pub attack_clock: Option<AttackClockIntent>,
    pub statistics: Vec<CombatStatisticIntent>,
    pub item_transforms: Vec<crate::canonical_inventory::ItemTransformIntent>,
    pub held_attack: Option<crate::canonical_inventory::InventoryEquipmentBinding>,
    pub equipment: Vec<crate::canonical_inventory::InventoryEquipmentBinding>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct FallDamageIntent {
    pub contact: crate::item_contact::ContactObservation,
    pub distance: u32,
    pub landing: Target,
    pub jump_boost_effect: u16,
    pub damage_enabled: bool,
    pub sneaking: bool,
    pub armor_attribute: u16,
    pub toughness_attribute: u16,
}

impl FallDamageIntent {
    fn valid(&self, tick: TickStamp) -> bool {
        let distance = f32::from_bits(self.distance);
        self.contact.valid() && self.contact.simulation_tick == tick
            && distance.is_finite() && (0.0..=30_000_000.0).contains(&distance)
            && matches!(self.landing, Target::Chunk {key,part:crate::canonical_action::ChunkPart::Block(block),inventory:None}
                if key.world == self.contact.world && key.x() == block.x.div_euclid(16) && key.z() == block.z.div_euclid(16)
                && block.x == f64::from_bits(self.contact.position[0]).floor() as i32
                && block.z == f64::from_bits(self.contact.position[2]).floor() as i32
                && block.y == (f64::from_bits(self.contact.position[1]) - 0.2).floor() as i32)
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct DamagingDependency {
    pub parent: crate::canonical_action::ActionRef,
    pub victim: crate::item_contact::ContactIdentity,
}

pub fn damaging_parent(
    action: &Action,
    applied: &crate::canonical_action::AppliedAction,
    victim: &crate::item_contact::ContactIdentity,
) -> bool {
    if applied.id != action.id || applied.tick != action.tick { return false; }
    let Some(SemanticIntent::Combat(intent)) = &action.semantic_intent else { return false };
    let Some(contact) = intent.attacks.iter().find(|attack| &attack.contact.victim.identity == victim)
        .map(|attack| &attack.contact.victim) else { return false };
    applied.writes.iter().any(|write| {
        let Some((body, property)) = crate::living_property::body_target(write.target) else { return false };
        if body != contact.target() || !matches!(property,
            crate::living_property::LivingProperty::Health | crate::living_property::LivingProperty::Absorption) { return false; }
        matches!((&write.before, &write.after),
            (Some(Cell {value: Value::LivingProperty(crate::living_property::LivingPropertyValue::Unsigned(before)), ..}),
             Some(Cell {value: Value::LivingProperty(crate::living_property::LivingPropertyValue::Unsigned(after)), ..})) if before > after)
    })
}

pub fn resolved_attack_charge(action: &Action) -> Option<u64> {
    use crate::living_property::{LivingProperty as P, LivingPropertyValue as V};
    let Some(SemanticIntent::Combat(intent)) = &action.semantic_intent else { return None };
    let primary = intent.attacks.iter().find(|attack| !attack.sweeping)?;
    let Target::Chunk {key, ..} = primary.source else { return None };
    let value = |property| action.conditions.iter().find_map(|condition| {
        let target = Target::Chunk {key, part: crate::canonical_action::ChunkPart::LivingProperty {
            actor: primary.actor, property,
        }, inventory: None};
        match (&condition.target, &condition.expected) {
            (found, Expected::Exact(Cell {value: Value::LivingProperty(value), ..})) if *found == target => Some(value),
            _ => None,
        }
    });
    if primary.pose.bedrock || !matches!(primary.actor, ActorId::Player(_)) { return Some(1.0f64.to_bits()); }
    let V::OptionalTick(last) = value(P::AttackStarted)? else { return None };
    let V::Weapon(weapon) = value(P::Weapon)? else { return None };
    let V::Attribute {base: Some(base), modifiers, ..} = value(P::Attribute(primary.attributes.attack_speed))? else { return None };
    let equipment = CombatEquipmentFacts {modifiers: modifiers.clone(), ..Default::default()};
    let speed = equipment.attribute(primary.attributes.attack_speed, f64::from_bits(*base))
        - if weapon.unarmed {2.4} else {0.0};
    if !speed.is_finite() { return None; }
    Some(last.map_or(1.0, |last| ((f64::from(primary.tick.distance_since(last)) + 0.5) * speed / 20.0)
        .clamp(0.0, 1.0)).to_bits())
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub enum EnvironmentalHazardKind {
    Fire,
    Lava,
    Cactus,
    SweetBerry,
    Suffocation,
    PowderSnow,
    Water,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct EnvironmentalHazardFacts {
    pub damage_milli: u32,
    pub fire_ticks: u32,
    pub bypass_armor: bool,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct EnvironmentalContactFacts {
    pub touching_water: bool,
    pub immersed: bool,
    pub breathable: bool,
    pub freeze_damage_milli: u32,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct EnvironmentalDamageIntent {
    pub support_blocks: Vec<Target>,
    pub shape_properties: Vec<crate::living_property::LivingProperty>,
    pub world_rules: Option<crate::world_rules::WorldRulesWitness>,
    pub elapsed: u16,
    pub rules: EnvironmentalRules,
    pub contact: crate::item_contact::ContactObservation,
    pub kind: EnvironmentalHazardKind,
    pub blocks: Vec<Target>,
    pub armor_attribute: u16,
    pub toughness_attribute: u16,
    pub fire_immune: bool,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub struct EnvironmentalRules {
    pub fire_damage: bool,
    pub drowning_damage: bool,
    pub freeze_damage: bool,
    pub water_breathing_effect: u16,
    pub can_breathe_water: bool,
    pub freeze_immune: bool,
}

pub const MAX_ENVIRONMENTAL_BLOCKS: usize = 17 * 17 * 17;

impl EnvironmentalDamageIntent {
    pub fn is_valid(&self, tick: TickStamp) -> bool {
        let position = self.contact.position.map(f64::from_bits);
        let radius = f64::from_bits(self.contact.half_width);
        let height = f64::from_bits(self.contact.height);
        self.contact.valid()
            && self.elapsed != 0 && self.elapsed < 0x8000
            && self.contact.simulation_tick == tick
            && (!self.blocks.is_empty() || (matches!(self.kind, EnvironmentalHazardKind::Water | EnvironmentalHazardKind::PowderSnow) && self.world_rules.is_some()))
            && self.blocks.len() <= MAX_ENVIRONMENTAL_BLOCKS
            && self.support_blocks.len() <= MAX_ENVIRONMENTAL_BLOCKS
            && (self.support_blocks.is_empty() || self.kind == EnvironmentalHazardKind::Water)
            && self.support_blocks.windows(2).all(|pair| pair[0] < pair[1])
            && self.shape_properties.len() <= 4
            && self.shape_properties.windows(2).all(|pair| pair[0] < pair[1])
            && self.support_blocks.iter().all(|target| match target {
                Target::Chunk {key,part:crate::canonical_action::ChunkPart::Block(block),inventory:None} =>
                    key.world == self.contact.world && block.x.div_euclid(16) == key.x() && block.z.div_euclid(16) == key.z()
                    && f64::from(block.x) < position[0] + radius && f64::from(block.x) + 1.0 > position[0] - radius
                    && block.y == (position[1] + height).ceil() as i32
                    && f64::from(block.z) < position[2] + radius && f64::from(block.z) + 1.0 > position[2] - radius,
                _ => false,
            })
            && self.blocks.windows(2).all(|pair| pair[0] < pair[1])
            && self.blocks.iter().all(|target| match target {
                Target::Chunk {
                    key,
                    part: crate::canonical_action::ChunkPart::Block(block),
                    inventory: None,
                } => {
                    key.world == self.contact.world
                        && block.x.div_euclid(16) == key.x()
                        && block.z.div_euclid(16) == key.z()
                        && f64::from(block.x) < position[0] + radius
                        && f64::from(block.x) + 1.0 > position[0] - radius
                        && f64::from(block.y) < position[1] + height
                        && f64::from(block.y) + 1.0 > position[1]
                        && f64::from(block.z) < position[2] + radius
                        && f64::from(block.z) + 1.0 > position[2] - radius
                }
                _ => false,
            })
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct FoodExhaustionIntent {
    pub lease: crate::canonical_action::LivePlayerLease,
    pub chunk: crate::protocol::ChunkKey,
    pub epoch: crate::canonical_action::SpatialEpoch,
    pub tick: TickStamp,
    pub amount_milli: u32,
}

impl FoodExhaustionIntent {
    pub fn target(&self) -> Target {
        Target::Chunk {
            key: self.chunk,
            part: crate::canonical_action::ChunkPart::Player(self.lease.gid),
            inventory: None,
        }
    }

    pub fn is_valid(&self) -> bool {
        self.lease.incarnation != 0
            && !self.lease.key.0.is_nil()
            && self.amount_milli <= 40_000
            && match self.epoch {
                crate::canonical_action::SpatialEpoch::Admission { lease, .. } => {
                    lease == self.lease
                }
                crate::canonical_action::SpatialEpoch::Action(reference) => {
                    reference.id.incarnation != 0 && !matches!(reference.id.actor, ActorId::Peer(_))
                }
            }
    }

    pub fn targets(&self) -> [Target; 2] {
        [
            crate::living_property::LivingProperty::Food,
            crate::living_property::LivingProperty::GameMode,
        ]
        .map(|property| Target::Chunk {
            key: self.chunk,
            part: crate::canonical_action::ChunkPart::LivingProperty {
                actor: ActorId::Player(self.lease.gid),
                property,
            },
            inventory: None,
        })
    }

    pub fn guard(&self) -> Condition {
        Condition {
            target: self.target(),
            expected: Expected::SpatialEpochEquals(self.epoch),
        }
    }

    pub fn apply(
        &self,
        food: &mut crate::item_action::ActorFoodState,
        game_mode: u8,
    ) -> Result<bool, ApplyFailure> {
        if !self.is_valid() || food.actor != ActorId::Player(self.lease.gid) || game_mode > 3 {
            return Err(ApplyFailure::InvalidAction);
        }
        if !matches!(game_mode, 0 | 2) {
            return Ok(false);
        }
        let after = food
            .exhaustion_milli
            .saturating_add(self.amount_milli)
            .min(40_000);
        if food.exhaustion_milli == after {
            return Ok(false);
        }
        food.revision = food
            .revision
            .checked_add(1)
            .ok_or(ApplyFailure::Overflow(self.targets()[0]))?;
        food.exhaustion_milli = after;
        Ok(true)
    }

    pub fn derive_effects(
        &self,
        values: &std::collections::BTreeMap<Target, Cell>,
    ) -> Result<Vec<Effect>, ApplyFailure> {
        use crate::living_property::LivingPropertyValue;
        let [food_target, mode_target] = self.targets();
        let Some(Cell {
            value: Value::LivingProperty(LivingPropertyValue::Food(before)),
            ..
        }) = values.get(&food_target)
        else {
            return Err(ApplyFailure::Condition(food_target));
        };
        let Some(Cell {
            value: Value::LivingProperty(LivingPropertyValue::Unsigned(mode)),
            ..
        }) = values.get(&mode_target)
        else {
            return Err(ApplyFailure::Condition(mode_target));
        };
        let mode = u8::try_from(*mode).map_err(|_| ApplyFailure::Type(mode_target))?;
        let mut after = before.clone();
        Ok(if self.apply(&mut after, mode)? {
            vec![Effect::Put {
                target: food_target,
                value: Value::LivingProperty(LivingPropertyValue::Food(after)),
            }]
        } else {
            Vec::new()
        })
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct LivingTickIdentity {
    pub identity: crate::item_contact::ContactIdentity,
    pub owner: crate::realm_completion::PeerGeneration,
    pub epoch: crate::canonical_action::SpatialEpoch,
    pub chunk: crate::protocol::ChunkKey,
    pub simulation_tick: TickStamp,
}

impl LivingTickIdentity {
    pub fn from_contact(contact: &crate::item_contact::ContactObservation) -> Self {
        Self { identity: contact.identity.clone(), owner: contact.owner, epoch: contact.epoch,
            chunk: contact.chunk, simulation_tick: contact.simulation_tick }
    }
    pub fn actor(&self) -> ActorId {
        match &self.identity {
            crate::item_contact::ContactIdentity::Player(lease) => ActorId::Player(lease.gid),
            crate::item_contact::ContactIdentity::Entity { id, .. } => ActorId::Entity(*id),
        }
    }
    pub fn target(&self) -> Target {
        Target::Chunk { key: self.chunk, part: match &self.identity {
            crate::item_contact::ContactIdentity::Player(lease) => crate::canonical_action::ChunkPart::Player(lease.gid),
            crate::item_contact::ContactIdentity::Entity { id, .. } => crate::canonical_action::ChunkPart::Entity(*id),
        }, inventory: None }
    }
    pub fn valid(&self) -> bool {
        let provenance = match &self.identity {
            crate::item_contact::ContactIdentity::Player(lease) => lease.gid.server == self.owner.server
                && lease.incarnation != 0 && !lease.key.0.is_nil(),
            crate::item_contact::ContactIdentity::Entity { id, birth } => birth.source.server == id.origin
                && birth.source.generation != 0 && !birth.uuid.is_nil(),
        };
        provenance && self.owner.generation != 0 && match self.epoch {
            crate::canonical_action::SpatialEpoch::Admission { lease, .. } =>
                matches!(self.identity, crate::item_contact::ContactIdentity::Player(actual) if actual == lease),
            crate::canonical_action::SpatialEpoch::Action(reference) => reference.id.incarnation != 0,
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct LivingTickIntent {
    pub identity: LivingTickIdentity,
    pub effective_position: Option<[u64; 3]>,
    pub previous_tick: Option<TickStamp>,
    pub attribute_ids: Vec<u16>,
    pub fire_immune: bool,
    pub armor_attribute: u16,
    pub toughness_attribute: u16,
    pub rules: LivingTickRules,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub struct LivingTickRules {
    pub minimum_y: i32,
    pub fire_damage: bool,
    pub difficulty: u8,
    pub natural_regeneration: bool,
    pub max_health_attribute: u16,
    pub regeneration_effect: u16,
    pub poison_effect: u16,
    pub wither_effect: u16,
    pub hunger_effect: u16,
}

pub(crate) fn periodic_tick_count(tick: TickStamp, elapsed: u16, period: u16) -> u32 {
    (0..elapsed)
        .filter(|offset| tick.0.wrapping_sub(*offset) % period == 0)
        .count() as u32
}

#[cfg(test)]
mod environmental_cadence_tests {
    use super::periodic_tick_count;
    use crate::time::TickStamp;

    #[test]
    fn retained_interval_counts_damage_before_final_tick() {
        assert_eq!(periodic_tick_count(TickStamp(21), 12, 10), 2);
        assert_eq!(periodic_tick_count(TickStamp(21), 1, 10), 0);
    }

    #[test]
    fn retained_interval_uses_wrapping_clock() {
        assert_eq!(periodic_tick_count(TickStamp(1), 3, 10), 1);
        assert_eq!(periodic_tick_count(TickStamp(0), 1, 40), 1);
    }
}

impl LivingTickIntent {
    #[cfg(test)]
    fn refresh_status(
        context: &dyn crate::canonical_action::ActionState,
        value: &mut Value,
        before: &crate::active_effects::ActiveEffectSet,
        max_health_attribute: u16,
    ) -> Result<(), ApplyFailure> {
        let living = crate::living_property::state_mut(value).ok_or(ApplyFailure::InvalidAction)?;
        let next = crate::active_effects::ActiveEffectSet {
            visible: living.effects.clone(), hidden: living.hidden_effects.clone(),
        };
        let facts = context.status_effect_facts(&next)?;
        living.combat.status.strength = facts.strength;
        living.combat.status.weakness = facts.weakness;
        living.combat.status.resistance = facts.resistance;
        living.combat.status.fire_resistance = facts.fire_resistance;
        living.combat.status.blindness = facts.blindness;
        let old_modifiers = context.status_effect_modifiers(before)?;
        let next_modifiers = context.status_effect_modifiers(&next)?;
        let removed_health_boost=old_modifiers.iter().find(|modifier|modifier.attribute==max_health_attribute && modifier.id=="minecraft:effect.health_boost")
            .is_some_and(|old|next_modifiers.iter().find(|next|next.attribute==max_health_attribute && next.id==old.id).is_none_or(|next|next!=old));
        living.combat.equipment.modifiers.retain(|modifier| !old_modifiers.iter().any(|old|
            old.attribute == modifier.attribute && old.slot == modifier.slot && old.id == modifier.id));
        for modifier in next_modifiers {
            living.combat.equipment.modifiers.retain(|old|
                old.attribute != modifier.attribute || old.slot != modifier.slot || old.id != modifier.id);
            living.combat.equipment.modifiers.push(modifier);
        }
        living.combat.equipment.modifiers.sort_by(|a, b|
            (a.attribute, a.slot, a.id.as_str()).cmp(&(b.attribute, b.slot, b.id.as_str())));
        if removed_health_boost {
            let base=living.combat.effective_attributes.get(&max_health_attribute).copied().map(f64::from_bits).ok_or(ApplyFailure::InvalidAction)?;
            let maximum=living.attribute(max_health_attribute,base);
            if !maximum.is_finite() || maximum>f64::from(u32::MAX)/1000.0 {return Err(ApplyFailure::InvalidAction);}
            living.combat.health_milli=living.combat.health_milli.min((maximum.max(0.0)*1000.0).round() as u32);
        }
        Ok(())
    }


    #[cfg(test)]
    fn apply(&self, value: &mut Value) -> Result<(u32, u32), ApplyFailure> {
        self.apply_with_context(value, None)
    }

    #[cfg(test)]
    fn apply_with_context(&self, value: &mut Value, context: Option<&dyn crate::canonical_action::ActionState>)
        -> Result<(u32,u32),ApplyFailure> {
        self.apply_accounted_with_context(value,context).map(|(health,absorption,_)|(health,absorption))
    }

    fn apply_accounted_with_context(&self, value: &mut Value, context: Option<&dyn crate::canonical_action::ActionState>)
        -> Result<(u32,u32,bool),ApplyFailure> {
        let living = crate::living_property::state(value).ok_or(ApplyFailure::Type(self.identity.target()))?;
        let mut draft = crate::living_numeric::NumericLivingDraft::new(crate::living_numeric::NumericLivingView::new(living));
        let accounting = draft.advance(self, context)?;
        let result = draft.finish();
        result.apply_to(crate::living_property::state_mut(value).ok_or(ApplyFailure::Type(self.identity.target()))?);
        Ok(accounting)
    }

    #[cfg(test)]
    fn apply_accounted_reference(&self, value: &mut Value, context: Option<&dyn crate::canonical_action::ActionState>)
        -> Result<(u32,u32,bool),ApplyFailure> {
        let elapsed = self.previous_tick.map_or(1, |previous| self.identity.simulation_tick.distance_since(previous));
        if elapsed == 0 || elapsed >= 0x8000 { return Err(ApplyFailure::InvalidAction); }
        if elapsed > 1 {
            if crate::living_property::state(value).ok_or(ApplyFailure::Type(self.identity.target()))?.simulation_tick != self.previous_tick {
                return Err(ApplyFailure::Condition(self.identity.target()));
            }
            let mut total = (0_u32,0_u32,false);
            let mut step = self.clone();
            for offset in (0..elapsed).rev() {
                step.identity.simulation_tick = TickStamp(self.identity.simulation_tick.0.wrapping_sub(offset));
                step.previous_tick = crate::living_property::state(value).ok_or(ApplyFailure::Type(self.identity.target()))?.simulation_tick;
                let change = step.apply_accounted_reference(value,context)?;
                total.0 = total.0.saturating_add(change.0);
                total.1 = total.1.saturating_add(change.1);
                total.2 |= change.2;
            }
            return Ok(total);
        }
        let before_effects=if context.is_some() {
            let living=crate::living_property::state(value).ok_or(ApplyFailure::Type(self.identity.target()))?;
            Some(crate::active_effects::ActiveEffectSet {visible:living.effects.clone(),hidden:living.hidden_effects.clone()})
        } else {None};
        let target = self.identity.target();
        let living = crate::living_property::state_mut(value).ok_or(ApplyFailure::Type(target))?;
        let previous_food = living.food;
        let mut damage = (0_u32, 0_u32);
        if living.simulation_tick != self.previous_tick {
            return Err(ApplyFailure::Condition(target));
        }
        let elapsed = self.previous_tick.map_or(1, |previous| {
            self.identity.simulation_tick.distance_since(previous)
        });
        if elapsed == 0 || elapsed >= 0x8000 {
            return Err(ApplyFailure::InvalidAction);
        }
        if self.effective_position.is_some_and(|position| f64::from_bits(position[1]) < f64::from(self.rules.minimum_y) - 64.0)
            && periodic_tick_count(self.identity.simulation_tick, elapsed, 10) != 0
        {
            let change = DamageIntent {
                kind: DamageKind::OutOfWorld,
                source: self.identity.actor(), target, tick:self.identity.simulation_tick,
                damage_milli:4000 * periodic_tick_count(self.identity.simulation_tick, elapsed, 10), knockback_micro:[0;3],bypass_hurt_cooldown:true,
                fire_ticks:0,bypass_invulnerability:true,reduction:None,
            }.apply_accounted(living)?;
            damage.0 = damage.0.saturating_add(change.0);
            damage.1 = damage.1.saturating_add(change.1);
        }
        if let Some(mob) = living.mob.as_mut() {
            mob.reproduction = mob.reproduction.advance(u32::from(elapsed));
        }
        let timing = &mut living.combat.timing;
        timing.hurt_ticks = timing.hurt_ticks.saturating_sub(elapsed);
        timing.attack_ticks = timing.attack_ticks.saturating_add(elapsed);
        if living.combat.health_milli == 0 {
            timing.death_ticks = timing.death_ticks.saturating_add(elapsed);
        }
        let fire_damage =
            timing.fire_ticks / 20 - timing.fire_ticks.saturating_sub(u32::from(elapsed)) / 20;
        timing.fire_ticks = timing.fire_ticks.saturating_sub(u32::from(elapsed));
        if fire_damage != 0 && self.rules.fire_damage && !self.fire_immune && !living.combat.status.fire_resistance {
            let change = DamageIntent {
                kind: DamageKind::OnFire,
                source: living
                    .combat
                    .timing
                    .last_attacker
                    .unwrap_or(self.identity.actor()),
                target,
                tick: self.identity.simulation_tick,
                damage_milli: fire_damage.saturating_mul(1000),
                knockback_micro: [0; 3],
                bypass_hurt_cooldown: false,
                fire_ticks: 0,
                bypass_invulnerability: false,
                reduction: Some(DamageReductionFacts {
                    armor_attribute: self.armor_attribute,
                    toughness_attribute: self.toughness_attribute,
                    bypass_armor: true,
                    bypass_effects: false,
                    bypass_enchantments: false,
                    protection: DamageProtection::Fire,
                    breach: 0,
                }),
            }
            .apply_accounted(living)?;
            damage.0 = damage.0.saturating_add(change.0);
            damage.1 = damage.1.saturating_add(change.1);
        }
        for effect in living.effects.clone() {
            let duration = if effect.duration < 0 {
                u32::from(elapsed)
            } else {
                u32::from(elapsed).min(effect.duration as u32)
            };
            if duration == 0 {
                continue;
            }
            if effect.kind == self.rules.hunger_effect
                && living
                    .avatar
                    .as_ref()
                    .is_some_and(|avatar| avatar.game_mode == 0 || avatar.game_mode == 2)
            {
                living.food.exhaustion_milli = living
                    .food
                    .exhaustion_milli
                    .saturating_add(
                        5_u32
                            .saturating_mul(u32::from(effect.amplifier) + 1)
                            .saturating_mul(duration),
                    )
                    .min(40_000);
                continue;
            }
            let base: u32 = if effect.kind == self.rules.regeneration_effect {
                50
            } else if effect.kind == self.rules.poison_effect {
                25
            } else if effect.kind == self.rules.wither_effect {
                40
            } else {
                continue;
            };
            let period = (base >> u32::from(effect.amplifier).min(31)).max(1);
            let count = if effect.duration < 0 {
                periodic_tick_count(self.identity.simulation_tick, duration as u16, period as u16)
            } else {
                effect.duration as u32 / period - (effect.duration as u32 - duration) / period
            };
            if count == 0 || living.combat.health_milli == 0 {
                continue;
            }
            let amount = count.saturating_mul(1000);
            if effect.kind == self.rules.regeneration_effect {
                let base = living
                    .combat
                    .effective_attributes
                    .get(&self.rules.max_health_attribute)
                    .copied()
                    .map(f64::from_bits)
                    .ok_or(ApplyFailure::InvalidAction)?;
                let maximum = living.attribute(self.rules.max_health_attribute, base);
                if !maximum.is_finite() || maximum < 0.0 || maximum > f64::from(u32::MAX) / 1000.0 {
                    return Err(ApplyFailure::InvalidAction);
                }
                living.combat.health_milli = living
                    .combat
                    .health_milli
                    .saturating_add(amount)
                    .min((maximum * 1000.0).round() as u32);
            } else {
                let amount = if effect.kind == self.rules.poison_effect {
                    if living.combat.health_milli <= 1000 {
                        continue;
                    }
                    amount.min(
                        living
                            .combat
                            .health_milli
                            .saturating_sub(1000)
                            .saturating_add(living.combat.absorption_milli),
                    )
                } else {
                    amount
                };
                let change = DamageIntent {
                    kind: if effect.kind == self.rules.poison_effect {
                        DamageKind::Magic
                    } else {
                        DamageKind::Wither
                    },
                    source: living
                        .combat
                        .timing
                        .last_attacker
                        .unwrap_or(self.identity.actor()),
                    target,
                    tick: self.identity.simulation_tick,
                    damage_milli: amount,
                    knockback_micro: [0; 3],
                    bypass_hurt_cooldown: false,
                    fire_ticks: 0,
                    bypass_invulnerability: false,
                    reduction: Some(DamageReductionFacts {
                        armor_attribute: self.armor_attribute,
                        toughness_attribute: self.toughness_attribute,
                        bypass_armor: true,
                        bypass_effects: false,
                        bypass_enchantments: true,
                        protection: DamageProtection::General,
                        breach: 0,
                    }),
                }
                .apply_accounted(living)?;
                damage.0 = damage.0.saturating_add(change.0);
                damage.1 = damage.1.saturating_add(change.1);
            }
        }
        let mut effects = crate::active_effects::ActiveEffectSet {
            visible: living.effects.clone(),
            hidden: living.hidden_effects.clone(),
        };
        effects.advance(elapsed);
        living.effects = effects.visible;
        living.hidden_effects = effects.hidden;
        if living
            .avatar
            .as_ref()
            .is_some_and(|avatar| avatar.game_mode == 0 || avatar.game_mode == 2)
        {
            let base = living
                .combat
                .effective_attributes
                .get(&self.rules.max_health_attribute)
                .copied()
                .map(f64::from_bits)
                .ok_or(ApplyFailure::InvalidAction)?;
            let maximum = living.attribute(self.rules.max_health_attribute, base);
            if !maximum.is_finite() || maximum < 0.0 || maximum > f64::from(u32::MAX) / 1000.0 {
                return Err(ApplyFailure::InvalidAction);
            }
            let maximum = (maximum * 1000.0).round() as u32;
            for _ in 0..elapsed {
                let food = &mut living.food;
                if food.exhaustion_milli > 4000 {
                    food.exhaustion_milli -= 4000;
                    if food.saturation_milli > 0 {
                        food.saturation_milli = food.saturation_milli.saturating_sub(1000);
                    } else if self.rules.difficulty != 0 {
                        food.food = food.food.saturating_sub(1);
                    }
                }
                if self.rules.natural_regeneration
                    && living.combat.health_milli != 0
                    && living.combat.health_milli < maximum
                    && food.food >= 18
                {
                    food.tick_timer += 1;
                    let saturated = food.food == 20 && food.saturation_milli != 0;
                    if food.tick_timer >= if saturated { 10 } else { 80 } {
                        let cost = if saturated {
                            food.saturation_milli.min(6000)
                        } else {
                            6000
                        };
                        let healing = if saturated { cost / 6 } else { 1000 };
                        living.combat.health_milli = living
                            .combat
                            .health_milli
                            .saturating_add(healing)
                            .min(maximum);
                        food.exhaustion_milli =
                            food.exhaustion_milli.saturating_add(cost).min(40_000);
                        food.tick_timer = 0;
                    }
                } else if food.food == 0 && living.combat.health_milli != 0 {
                    food.tick_timer += 1;
                    if food.tick_timer >= 80 {
                        food.tick_timer = 0;
                        if living.combat.health_milli > 10_000
                            || self.rules.difficulty == 3
                            || self.rules.difficulty == 2 && living.combat.health_milli > 1000
                        {
                            let change = DamageIntent {
                                kind: DamageKind::Starvation,
                                source: self.identity.actor(),
                                target,
                                tick: self.identity.simulation_tick,
                                damage_milli: 1000,
                                knockback_micro: [0; 3],
                                bypass_hurt_cooldown: false,
                                fire_ticks: 0,
                                reduction: None,
                                bypass_invulnerability: false,
                            }
                            .apply_accounted(living)?;
                            damage.0 = damage.0.saturating_add(change.0);
                            damage.1 = damage.1.saturating_add(change.1);
                        }
                    }
                } else {
                    food.tick_timer = 0;
                }
            }
        }
        if living.food != previous_food {
            living.food.revision = previous_food
                .revision
                .checked_add(1)
                .ok_or(ApplyFailure::Overflow(target))?;
        }
        living.simulation_tick = Some(self.identity.simulation_tick);
        let damage_killed=damage.0!=0 && living.combat.health_milli==0;
        if let (Some(context),Some(before_effects))=(context,before_effects) {Self::refresh_status(context,value,&before_effects,self.rules.max_health_attribute)?;}
        Ok((damage.0,damage.1,damage_killed))
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct CombatStatisticIntent {
    pub target: Target,
    pub damage_targets: Vec<Target>,
    pub kind: CombatStatisticKind,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub enum CombatStatisticKind {
    Blocked,
    Increment(u32),
    Damage,
    Absorbed,
    Kill,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct AttackClockIntent {
    pub target: Target,
    pub actor: ActorId,
    pub tick: TickStamp,
}

impl CombatIntent {
    pub fn permits_victim_armor_transform(&self, intent: &crate::canonical_inventory::ItemTransformIntent) -> bool {
        let Target::Player { lease: Some(lease), .. } = intent.source.target else { return false; };
        let Ok(index) = self.equipment.binary_search_by_key(&lease, |binding| binding.lease) else { return false; };
        let binding = &self.equipment[index];
        let valid_slot = match intent.operation {
            crate::canonical_inventory::ItemTransformOperation::ArmorDamage { armor_slot, .. } => binding.armor.get(usize::from(armor_slot)) == Some(&intent.source.target),
            crate::canonical_inventory::ItemTransformOperation::ShieldDamage { .. } => binding.offhand == intent.source.target || binding.hotbar.contains(&intent.source.target),
            _ => false,
        };
        intent.valid() && intent.remainder_destinations.is_empty() && valid_slot
            && intent.source.slot.target == crate::inventory::InventoryTarget::Player(binding.lease.gid)
    }
    pub fn permits_item_transform(
        &self,
        intent: &crate::canonical_inventory::ItemTransformIntent,
    ) -> bool {
        self.item_transforms.contains(intent)
            || self.permits_victim_armor_transform(intent)
            || self.held_attack.as_ref().is_some_and(|binding| {
                intent.valid()
                    && intent.operation
                        == crate::canonical_inventory::ItemTransformOperation::AttackDamage
                    && intent.remainder_destinations.is_empty()
                    && binding.hotbar.contains(&intent.source.target)
                    && intent.source.slot.target
                        == crate::inventory::InventoryTarget::Player(binding.lease.gid)
            })
    }
    pub fn resolution_targets(&self) -> std::collections::BTreeSet<Target> {
        self.property_targets()
            .into_iter()
            .chain(self.falls.iter().map(|intent| intent.landing))
            .chain(
                self.vehicle_attacks
                    .iter()
                    .flat_map(|intent| intent.targets()),
            )
            .chain(
                self.environment
                    .iter()
                    .flat_map(|intent| intent.blocks.iter().chain(&intent.support_blocks).copied()),
            )
            .chain(self.equipment.iter().flat_map(|binding| binding.targets()))
            .chain(
                self.held_attack
                    .iter()
                    .flat_map(|binding| binding.targets()),
            )
            .chain(self.item_transforms.iter().flat_map(|intent| {
                std::iter::once(intent.source.target).chain(
                    intent
                        .remainder_destinations
                        .iter()
                        .map(|binding| binding.target),
                )
            }))
            .chain(self.held_attack.iter().filter_map(|binding| {
                let Target::Chunk { key, .. } = binding.living else {
                    return None;
                };
                Some(Target::Chunk {
                    key,
                    part: crate::canonical_action::ChunkPart::LivingProperty {
                        actor: ActorId::Player(binding.lease.gid),
                        property: crate::living_property::LivingProperty::GameMode,
                    },
                    inventory: None,
                })
            }))
            .collect()
    }

    pub fn derive_with<S: crate::canonical_action::ActionState>(
        &self,
        context: &S,
        id: ActionId,
        tick: TickStamp,
        values: &std::collections::BTreeMap<Target, Cell>,
    ) -> Result<Vec<Effect>, ApplyFailure> {
        self.derive_with_motion(context, id, tick, values, &std::collections::BTreeMap::new())
    }

    pub(crate) fn derive_with_motion<S: crate::canonical_action::ActionState>(
        &self,
        context: &S,
        id: ActionId,
        tick: TickStamp,
        values: &std::collections::BTreeMap<Target, Cell>,
        motion: &std::collections::BTreeMap<Target, crate::combat_command::VelocityCut>,
    ) -> Result<Vec<Effect>, ApplyFailure> {
        self.derive_with_policy(context, id, tick, values, motion, false)
    }

    pub fn derive_projectile<S: crate::canonical_action::ActionState>(
        &self, context: &S, id: ActionId, tick: TickStamp,
        values: &std::collections::BTreeMap<Target, Cell>, original: &crate::entity_body::EntityBody,
    ) -> Result<Vec<Effect>, ApplyFailure> {
        use crate::entity_body::{EntityPayload, ProjectileKind};
        let EntityPayload::Projectile(projectile) = &original.payload else { return Err(ApplyFailure::InvalidAction); };
        if self.damage_contacts.iter().any(|contact| contact.damage.source != projectile.shooter
            || contact.damage.kind != DamageKind::Projectile || contact.attack_origin != original.spatial.position) {
            return Err(ApplyFailure::InvalidAction);
        }
        let bypass = projectile.pierce > 0 && matches!(projectile.kind, ProjectileKind::Arrow | ProjectileKind::SpectralArrow);
        self.derive_with_policy(context, id, tick, values, &std::collections::BTreeMap::new(), bypass)
    }

    fn derive_with_policy<S: crate::canonical_action::ActionState>(
        &self, context: &S, id: ActionId, tick: TickStamp,
        values: &std::collections::BTreeMap<Target, Cell>,
        motion: &std::collections::BTreeMap<Target, crate::combat_command::VelocityCut>, bypass_shield: bool,
    ) -> Result<Vec<Effect>, ApplyFailure> {
        if let Some(dependency) = &self.damage_parent {
            match context.combat_parent_damaged(dependency.parent, &dependency.victim) {
                Some(false) => return Ok(Vec::new()),
                Some(true) => {},
                None => return Err(ApplyFailure::InvalidAction),
            }
        }
        let mut resolved = self.clone();
        resolved.environment.clear();
        if bypass_shield { resolved.damage_contacts.clear(); }
        for incoming in &self.damage_contacts {
            if !incoming.valid(tick) { return Err(ApplyFailure::InvalidAction); }
            let living = values.get(&incoming.contact.target()).and_then(|cell| living_value(&cell.value)).ok_or(ApplyFailure::Condition(incoming.contact.target()))?;
            let mut damage = incoming.damage.clone();
            if !bypass_shield && incoming.blocked_damage(living) != 0 { damage.damage_milli = 0; }
            resolved.damages.push(damage);
        }
        resolved.falls.clear();
        for intent in &self.falls {
            let body = values.get(&intent.contact.target()).and_then(|cell| crate::living_property::state(&cell.value)).ok_or(ApplyFailure::Condition(intent.contact.target()))?;
            let Some(Cell {value:Value::Block(state),..}) = values.get(&intent.landing) else { return Err(ApplyFailure::Condition(intent.landing)) };
            let jump = body.effects.iter().find(|effect| effect.kind == intent.jump_boost_effect).map_or(0.0,|effect|f32::from(effect.amplifier)+1.0);
            let factor = context.fall_damage_factor(*state, intent.sneaking)?;
            let amount = ((f32::from_bits(intent.distance) - 3.0 - jump).ceil().max(0.0) * f32::from(factor)).round() as u32;
            if intent.damage_enabled && amount != 0 {
                resolved.damages.push(DamageIntent {kind:DamageKind::Fall,source:intent.contact.actor(),target:intent.contact.target(),tick,
                    damage_milli:amount,knockback_micro:[0;3],bypass_hurt_cooldown:false,fire_ticks:0,bypass_invulnerability:false,
                    reduction:Some(DamageReductionFacts {armor_attribute:intent.armor_attribute,toughness_attribute:intent.toughness_attribute,bypass_armor:true,bypass_effects:false,bypass_enchantments:false,protection:DamageProtection::Fall,breach:0})});
            }
        }
        let mut environment_effects = std::collections::BTreeMap::new();
        let mut extinguished = std::collections::BTreeSet::new();
        for intent in &self.environment {
            let contact_facts = context.environmental_contact(intent,values)?;
            if intent.blocks.is_empty() {
                let witness = intent.world_rules.ok_or(ApplyFailure::InvalidAction)?;
                let policy = context.world_rules(witness, tick)?;
                let bottom = f64::from_bits(intent.contact.position[1]);
                let top = bottom + f64::from_bits(intent.contact.height);
                let maximum = f64::from(policy.min_y) + f64::from(policy.height);
                if witness.rules.world != intent.contact.world
                    || !(top <= f64::from(policy.min_y) || bottom >= maximum) {
                    return Err(ApplyFailure::InvalidAction);
                }
            }
            let mut hazard = None::<EnvironmentalHazardFacts>;
            for target in &intent.blocks {
                let Some(Cell {
                    value: Value::Block(state),
                    ..
                }) = values.get(target)
                else {
                    return Err(ApplyFailure::Condition(*target));
                };
                if let Some(facts) = context.environmental_hazard(intent.kind, *state)? {
                    hazard = Some(hazard.map_or(facts, |before| EnvironmentalHazardFacts {
                        damage_milli: before.damage_milli.max(facts.damage_milli),
                        fire_ticks: before.fire_ticks.max(facts.fire_ticks),
                        bypass_armor: before.bypass_armor || facts.bypass_armor,
                    }));
                }
            }
            if matches!(intent.kind, EnvironmentalHazardKind::Water | EnvironmentalHazardKind::PowderSnow) {
                let body = values.get(&intent.contact.target())
                    .and_then(|cell| crate::living_property::state(&cell.value))
                    .ok_or(ApplyFailure::Condition(intent.contact.target()))?;
                let mut environment = environment_effects.get(&intent.contact.target()).copied().unwrap_or(body.environment);
                let mut damage_milli = 0;
                match intent.kind {
                    EnvironmentalHazardKind::Water => {
                        if contact_facts.touching_water { extinguished.insert(intent.contact.target()); }
                        let protected = contact_facts.breathable || intent.rules.can_breathe_water || body.effects.iter().any(|effect| effect.kind == intent.rules.water_breathing_effect && effect.duration != 0)
                            || body.avatar.as_ref().is_some_and(|avatar| matches!(avatar.game_mode, 1 | 3));
                        if contact_facts.immersed && !protected && body.combat.health_milli != 0 {
                            for offset in 0..intent.elapsed {
                                if body.combat.equipment.respiration != 0
                                    && crate::order::rank_tick_actor(0, TickStamp(tick.0.wrapping_sub(offset)), intent.contact.actor())
                                        % (u64::from(body.combat.equipment.respiration) + 1) != 0 {
                                    continue;
                                }
                                environment.air_ticks = environment.air_ticks.saturating_sub(1);
                                if environment.air_ticks <= -20 {
                                    environment.air_ticks = 0;
                                    if intent.rules.drowning_damage { damage_milli += 2000; }
                                }
                            }
                        } else {
                            environment.air_ticks = environment.air_ticks.saturating_add((u32::from(intent.elapsed) * 4).min(i16::MAX as u32) as i16).min(300);
                        }
                    }
                    EnvironmentalHazardKind::PowderSnow => {
                        if hazard.is_some() && !intent.rules.freeze_immune && !body.combat.equipment.freeze_immune
                            && !body.avatar.as_ref().is_some_and(|avatar| avatar.game_mode == 3)
                            && body.combat.health_milli != 0 {
                            for offset in (0..intent.elapsed).rev() {
                                environment.frozen_ticks = environment.frozen_ticks.saturating_add(1).min(140);
                                if intent.rules.freeze_damage && environment.frozen_ticks == 140 && tick.0.wrapping_sub(offset) % 40 == 0 {
                                    damage_milli += contact_facts.freeze_damage_milli;
                                }
                            }
                        } else {
                            environment.frozen_ticks = environment.frozen_ticks.saturating_sub(intent.elapsed.saturating_mul(2));
                        }
                    }
                    _ => unreachable!(),
                }
                if environment != body.environment {
                    environment_effects.insert(intent.contact.target(), environment);
                }
                hazard = (damage_milli != 0).then_some(EnvironmentalHazardFacts {damage_milli,fire_ticks:0,bypass_armor:true});
            }
            if let Some(facts) = hazard
                && (intent.rules.fire_damage || !matches!(intent.kind, EnvironmentalHazardKind::Fire | EnvironmentalHazardKind::Lava))
                && !(intent.fire_immune
                    && matches!(
                        intent.kind,
                        EnvironmentalHazardKind::Fire | EnvironmentalHazardKind::Lava
                    ))
            {
                resolved.damages.push(DamageIntent {
                    kind: match intent.kind {
                        EnvironmentalHazardKind::Fire => DamageKind::InFire,
                        EnvironmentalHazardKind::Lava => DamageKind::Lava,
                        EnvironmentalHazardKind::Cactus => DamageKind::Cactus,
                        EnvironmentalHazardKind::SweetBerry => DamageKind::SweetBerry,
                        EnvironmentalHazardKind::Suffocation => DamageKind::Suffocation,
                        EnvironmentalHazardKind::PowderSnow => DamageKind::Freeze,
                        EnvironmentalHazardKind::Water => DamageKind::Drown,
                    },
                    source: intent.contact.actor(),
                    target: intent.contact.target(),
                    tick: intent.contact.simulation_tick,
                    damage_milli: facts.damage_milli,
                    knockback_micro: [0; 3],
                    bypass_hurt_cooldown: false,
                    fire_ticks: facts.fire_ticks,
                    reduction: Some(DamageReductionFacts {
                        armor_attribute: intent.armor_attribute,
                        toughness_attribute: intent.toughness_attribute,
                        bypass_armor: facts.bypass_armor,
                        bypass_effects: false,
                        bypass_enchantments: false,
                        protection: if matches!(
                            intent.kind,
                            EnvironmentalHazardKind::Fire | EnvironmentalHazardKind::Lava
                        ) {
                            DamageProtection::Fire
                        } else {
                            DamageProtection::General
                        },
                        breach: 0,
                    }),
                    bypass_invulnerability: false,
                });
            }
        }
        let parent_charge = self.damage_parent.as_ref().map(|dependency|
            context.combat_parent_charge(dependency.parent).ok_or(ApplyFailure::InvalidAction)).transpose()?;
        if parent_charge.is_some_and(|charge| !f64::from_bits(charge).is_finite()
            || !(0.0..=1.0).contains(&f64::from_bits(charge))) { return Err(ApplyFailure::InvalidAction); }
        let (mut effects,_) = resolved.derive_effects_with_charge(values,parent_charge,Some(context),motion)?;
        for body in extinguished {
            let Target::Chunk { key, part, inventory: None } = body else { return Err(ApplyFailure::InvalidAction); };
            let actor = match part { crate::canonical_action::ChunkPart::Player(id) => ActorId::Player(id),
                crate::canonical_action::ChunkPart::Entity(id) => ActorId::Entity(id), _ => return Err(ApplyFailure::InvalidAction) };
            let target = Target::Chunk { key, part: crate::canonical_action::ChunkPart::LivingProperty {
                actor, property: crate::living_property::LivingProperty::FireTicks }, inventory: None };
            effects.retain(|effect| !matches!(effect, Effect::Put { target: previous, .. } if *previous == target));
            effects.push(Effect::Put { target, value: Value::LivingProperty(crate::living_property::LivingPropertyValue::Unsigned(0)) });
        }
        for (body, environment) in environment_effects {
            let Target::Chunk { key, part, inventory: None } = body else { return Err(ApplyFailure::InvalidAction) };
            let actor = match part { crate::canonical_action::ChunkPart::Player(id) => ActorId::Player(id), crate::canonical_action::ChunkPart::Entity(id) => ActorId::Entity(id), _ => return Err(ApplyFailure::InvalidAction) };
            effects.push(Effect::Put {
                target: Target::Chunk {key,part:crate::canonical_action::ChunkPart::LivingProperty {actor,property:crate::living_property::LivingProperty::Environment},inventory:None},
                value:Value::LivingProperty(crate::living_property::LivingPropertyValue::Environment(environment)),
            });
        }
        for vehicle in &self.vehicle_attacks {
            effects.extend(vehicle.derive(values)?);
        }
        append_death_causes(id, tick, values, &mut effects)?;
        let mut projected = values.clone();
        for effect in &effects {
            if let Effect::Put { target, value } = effect {
                let before = projected
                    .get(target)
                    .cloned()
                    .or_else(|| {
                        crate::living_property::body_target(*target).and_then(|(body, property)| {
                            values
                                .get(&body)
                                .and_then(|cell| crate::derived_living::facet_cell(cell, property))
                        })
                    })
                    .ok_or(ApplyFailure::Condition(*target))?;
                projected.insert(
                    *target,
                    Cell {
                        revision: before.revision.wrapping_add(1),
                        value: value.clone(),
                    },
                );
            }
        }
        for intent in &self.ticks {
            use crate::living_property::{LivingProperty as P, LivingPropertyValue as V};
            if intent.attribute_ids != context.inventory_equipment_attribute_ids() {
                return Err(ApplyFailure::InvalidAction);
            }
            let body = intent.identity.target();
            let property_target = |property| Target::Chunk {
                key: intent.identity.chunk,
                part: crate::canonical_action::ChunkPart::LivingProperty {
                    actor: intent.identity.actor(),
                    property,
                },
                inventory: None,
            };
            let old_living = values
                .get(&body)
                .and_then(|cell| crate::living_property::state(&cell.value))
                .ok_or(ApplyFailure::Condition(body))?;
            let old_effects = crate::active_effects::ActiveEffectSet {
                visible: old_living.effects.clone(),
                hidden: old_living.hidden_effects.clone(),
            };
            let effects_target = property_target(P::StatusEffects);
            let V::StatusEffects(next_effects) = projected
                .get(&effects_target)
                .and_then(|cell| match &cell.value {
                    Value::LivingProperty(value) => Some(value),
                    _ => None,
                })
                .ok_or(ApplyFailure::Condition(effects_target))?
            else {
                return Err(ApplyFailure::Type(effects_target));
            };
            let old_modifiers = context.status_effect_modifiers(&old_effects)?;
            let next_modifiers = context.status_effect_modifiers(next_effects)?;
            let status = context.status_effect_facts(next_effects)?;
            let mut changes = vec![
                (P::Strength, V::Amplifier(status.strength)),
                (P::Weakness, V::Amplifier(status.weakness)),
                (P::Resistance, V::Amplifier(status.resistance)),
                (P::FireResistance, V::Boolean(status.fire_resistance)),
                (P::Blindness, V::Boolean(status.blindness)),
            ];
            for attribute in &intent.attribute_ids {
                let target = property_target(P::Attribute(*attribute));
                let Value::LivingProperty(V::Attribute { base, transient_modifiers, .. }) = &projected
                    .get(&target)
                    .ok_or(ApplyFailure::Condition(target))?
                    .value
                else {
                    return Err(ApplyFailure::Type(target));
                };
                let mut modifiers = transient_modifiers.clone();
                modifiers.retain(|modifier| {
                    !old_modifiers
                        .iter()
                        .any(|old| old.attribute == modifier.attribute && old.slot == modifier.slot && old.id == modifier.id)
                });
                for modifier in next_modifiers
                    .iter()
                    .filter(|modifier| modifier.attribute == *attribute)
                {
                    modifiers.retain(|old| old.id != modifier.id || old.slot != modifier.slot);
                    modifiers.push(modifier.clone());
                }
                modifiers.sort_by(|a, b| {
                    (a.attribute, a.slot, a.id.as_str()).cmp(&(b.attribute, b.slot, b.id.as_str()))
                });
                let transient_modifiers = modifiers.clone();
                let mut combined = modifiers.into_iter().map(|modifier| (modifier.id.clone(), modifier)).collect::<std::collections::BTreeMap<_, _>>();
                for modifier in old_living.permanent_attribute_modifiers.get(attribute).into_iter().flatten() {
                    combined.insert(modifier.id.clone(), modifier.clone());
                }
                let modifiers = combined.into_values().collect();
                changes.push((

                    P::Attribute(*attribute),
                    V::Attribute {
                        base: *base,
                        modifiers,
                        transient_modifiers,
                    },
                ));
            }
            for (property, value) in changes {
                let target = property_target(property);
                let before = projected
                    .get(&target)
                    .ok_or(ApplyFailure::Condition(target))?;
                let value = Value::LivingProperty(value);
                if before.value != value {
                    let revision = before
                        .revision
                        .checked_add(1)
                        .ok_or(ApplyFailure::Overflow(target))?;
                    effects.retain(|effect| !matches!(effect, Effect::Put { target: existing, .. } if *existing == target));
                    effects.push(Effect::Put {
                        target,
                        value: value.clone(),
                    });
                    projected.insert(target, Cell { revision, value });
                }
            }
        }
        let vehicle_damaged = effects.iter().any(|effect| matches!(effect,
            Effect::Put { target, value: Value::VehicleProperty(crate::vehicle_property::VehiclePropertyValue::Unsigned(after)) }
                if matches!(target, Target::Chunk { part: crate::canonical_action::ChunkPart::VehicleProperty {
                    property: crate::vehicle_property::VehicleProperty::Damage, .. }, .. })
                    && values.get(target).is_some_and(|cell| matches!(&cell.value,
                        Value::VehicleProperty(crate::vehicle_property::VehiclePropertyValue::Unsigned(before)) if after > before))));
        let damaged = vehicle_damaged || effects.iter().any(|effect| {
            let Effect::Put { target, value: Value::LivingProperty(crate::living_property::LivingPropertyValue::Unsigned(after)) } = effect
                else { return false; };
            let Some((body, property @ (crate::living_property::LivingProperty::Health | crate::living_property::LivingProperty::Absorption)))
                = crate::living_property::body_target(*target) else { return false; };
            values.get(&body).and_then(|cell| crate::derived_living::facet_cell(cell, property))
                .is_some_and(|cell| matches!(cell.value, Value::LivingProperty(crate::living_property::LivingPropertyValue::Unsigned(before)) if *after < before))
        });
        let mut attacks_by_victim = std::collections::BTreeMap::<ActorId, Vec<&AttackIntent>>::new();
        for attack in &resolved.attacks { attacks_by_victim.entry(attack.contact.victim.actor()).or_default().push(attack); }
        let mut contacts_by_victim = std::collections::BTreeMap::<ActorId, Vec<&CombatDamageContact>>::new();
        for incoming in &self.damage_contacts { contacts_by_victim.entry(incoming.contact.actor()).or_default().push(incoming); }
        let mut transforms = self.item_transforms.clone();
        for binding in &self.equipment {
            let victim = values.get(&binding.living).and_then(|cell| living_value(&cell.value))
                .ok_or(ApplyFailure::Condition(binding.living))?;
            if victim.combat.health_milli == 0 || victim.combat.status.invulnerable
                || victim.avatar.as_ref().is_some_and(|avatar| matches!(avatar.game_mode, 1 | 3)) {
                continue;
            }
            let blocked = attacks_by_victim.get(&ActorId::Player(binding.lease.gid)).into_iter().flatten().copied()
                .map(|attack| attack.blocked_damage(values, parent_charge))
                .chain(contacts_by_victim.get(&ActorId::Player(binding.lease.gid)).into_iter().flatten().copied().map(|incoming| Ok(if bypass_shield { 0 } else { incoming.blocked_damage(victim) })))
                .collect::<Result<Vec<_>, _>>()?.into_iter().max().unwrap_or(0);
            if blocked != 0 {
                let use_state = victim.use_state.as_ref().ok_or(ApplyFailure::InvalidAction)?;
                let exact_owner = match use_state.source.target {
                    crate::inventory::InventoryTarget::Player(player) => player == binding.lease.gid,
                    crate::inventory::InventoryTarget::LeasedPlayer { key, player, incarnation } => key == binding.lease.key.0 && player == binding.lease.gid && incarnation == binding.lease.incarnation,
                    _ => false,
                };
                if !exact_owner { return Err(ApplyFailure::InvalidAction); }
                let source = binding.targets().find(|target| projected.get(target).is_some_and(|cell|
                    matches!(&cell.value, Value::Inventory(item) if item.loc == use_state.source.loc)))
                    .ok_or(ApplyFailure::InvalidAction)?;
                if source != binding.offhand && !binding.hotbar.contains(&source) { return Err(ApplyFailure::InvalidAction); }
                if source != binding.offhand && !projected.get(&binding.selected).is_some_and(|cell|
                    matches!(&cell.value, Value::Inventory(selected) if selected.count == 1 && selected.nbt.is_empty() && selected.item.checked_sub(1) == Some(use_state.source.loc.slot))) { return Err(ApplyFailure::InvalidAction); }
                let Some(Cell { value: Value::Inventory(item), .. }) = projected.get(&source) else { return Err(ApplyFailure::Inventory(source)); };
                let disable = attacks_by_victim.get(&ActorId::Player(binding.lease.gid)).into_iter().flatten().copied()
                    .any(|attack| values.get(&attack.source).and_then(|cell| living_value(&cell.value)).is_some_and(|source| source.combat.equipment.axe));
                if disable {
                    let Target::Chunk { key, .. } = binding.living else { return Err(ApplyFailure::InvalidAction); };
                    let actor = ActorId::Player(binding.lease.gid);
                    let group = context.item_cooldown_group(item)?;
                    if group.is_empty() || group.len() > 256 { return Err(ApplyFailure::InvalidAction); }
                    let mut cooldowns = victim.cooldowns.clone();
                    let revision = cooldowns.get(&group).map_or(Some(1), |before| before.revision.checked_add(1)).ok_or(ApplyFailure::InvalidAction)?;
                    cooldowns.insert(group.clone(), crate::item_action::ItemCooldownState {actor,group,until:TickStamp(tick.0.wrapping_add(100)),revision});
                    effects.push(Effect::Put {target:Target::Chunk {key,part:crate::canonical_action::ChunkPart::LivingProperty {actor,property:crate::living_property::LivingProperty::Cooldowns},inventory:None},value:Value::LivingProperty(crate::living_property::LivingPropertyValue::Cooldowns(cooldowns))});
                    effects.push(Effect::Put {target:Target::Chunk {key,part:crate::canonical_action::ChunkPart::LivingProperty {actor,property:crate::living_property::LivingProperty::ItemUse},inventory:None},value:Value::LivingProperty(crate::living_property::LivingPropertyValue::ItemUse(None))});
                }
                if !item.is_empty() {
                    transforms.push(crate::canonical_inventory::ItemTransformIntent {
                        source: crate::canonical_inventory::InventorySlotBinding {
                            slot: crate::inventory::InventorySlotRef::new(crate::inventory::InventoryTarget::Player(binding.lease.gid), use_state.source.loc), target: source, max_stack: context.inventory_limit(item, source)?,
                        },
                        item: item.item, components: item.nbt.clone(),
                        operation: crate::canonical_inventory::ItemTransformOperation::ShieldDamage { incoming_milli: blocked },
                        remainder_destinations: Vec::new(),
                    });
                }
            }
            let attack_incoming = attacks_by_victim.get(&ActorId::Player(binding.lease.gid)).into_iter().flatten().copied()
                .map(|attack| attack.damage_with_protection(values, false, parent_charge).map(|damage| {
                    if victim.combat.timing.last_damage_tick.is_some_and(|last|
                        damage.tick == last || damage.tick.is_after(last) && damage.tick.distance_since(last) < 10)
                        && damage.damage_milli <= victim.combat.timing.last_damage_milli { 0 } else { damage.damage_milli }
                }))
                .collect::<Result<Vec<_>, _>>()?.into_iter().max().unwrap_or(0);
            let raw_incoming = contacts_by_victim.get(&ActorId::Player(binding.lease.gid)).into_iter().flatten().copied().filter(|incoming| (bypass_shield || incoming.blocked_damage(victim) == 0)
                    && !incoming.damage.reduction.is_some_and(|reduction| reduction.bypass_armor))
                .filter(|incoming| !victim.combat.timing.last_damage_tick.is_some_and(|last|
                    incoming.damage.tick == last || incoming.damage.tick.is_after(last) && incoming.damage.tick.distance_since(last) < 10)
                    || incoming.damage.bypass_hurt_cooldown || incoming.damage.damage_milli > victim.combat.timing.last_damage_milli)
                .map(|incoming| incoming.damage.damage_milli).max().unwrap_or(0);
            let incoming = attack_incoming.max(raw_incoming);
            if incoming == 0 { continue; }
            for (slot, target) in binding.armor.iter().copied().enumerate() {
                let Some(Cell { value: Value::Inventory(item), .. }) = projected.get(&target) else {
                    return Err(ApplyFailure::Inventory(target));
                };
                if item.is_empty() { continue; }
                transforms.push(crate::canonical_inventory::ItemTransformIntent {
                    source: crate::canonical_inventory::InventorySlotBinding {
                        slot: crate::inventory::InventorySlotRef::new(crate::inventory::InventoryTarget::Player(binding.lease.gid), item.loc),
                        target, max_stack: context.inventory_limit(item, target)?,
                    },
                    item: item.item, components: item.nbt.clone(),
                    operation: crate::canonical_inventory::ItemTransformOperation::ArmorDamage { incoming_milli: incoming, armor_slot: slot as u8 },
                    remainder_destinations: Vec::new(),
                });
            }
        }
        if damaged {
            if let Some(binding) = &self.held_attack {
                let Target::Chunk { key, .. } = binding.living else {
                    return Err(ApplyFailure::InvalidAction);
                };
                let mode_target = Target::Chunk {
                    key,
                    part: crate::canonical_action::ChunkPart::LivingProperty {
                        actor: ActorId::Player(binding.lease.gid),
                        property: crate::living_property::LivingProperty::GameMode,
                    },
                    inventory: None,
                };
                let Some(Cell {
                    value:
                        Value::LivingProperty(crate::living_property::LivingPropertyValue::Unsigned(
                            mode,
                        )),
                    ..
                }) = projected.get(&mode_target)
                else {
                    return Err(ApplyFailure::Condition(mode_target));
                };
                if *mode == 0 || *mode == 2 {
                    let Some(Cell {
                        value: Value::Inventory(selected),
                        ..
                    }) = projected.get(&binding.selected)
                    else {
                        return Err(ApplyFailure::Inventory(binding.selected));
                    };
                    let slot = selected
                        .item
                        .checked_sub(1)
                        .filter(|slot| *slot < 9)
                        .ok_or(ApplyFailure::Inventory(binding.selected))?;
                    if selected.count != 1 || !selected.nbt.is_empty() {
                        return Err(ApplyFailure::Inventory(binding.selected));
                    }
                    let source = binding.hotbar[usize::from(slot)];
                    let Some(Cell {
                        value: Value::Inventory(item),
                        ..
                    }) = projected.get(&source)
                    else {
                        return Err(ApplyFailure::Inventory(source));
                    };
                    if !item.is_empty() {
                        transforms.push(crate::canonical_inventory::ItemTransformIntent {
                            source: crate::canonical_inventory::InventorySlotBinding {
                                slot: crate::inventory::InventorySlotRef::new(
                                    crate::inventory::InventoryTarget::Player(binding.lease.gid),
                                    item.loc,
                                ),
                                target: source,
                                max_stack: context.inventory_limit(item, source)?,
                            },
                            item: item.item,
                            components: item.nbt.clone(),
                            operation:
                                crate::canonical_inventory::ItemTransformOperation::AttackDamage,
                            remainder_destinations: Vec::new(),
                        });
                    }
                }
            }
        }
        for intent in transforms.iter().filter(|intent| damaged || matches!(intent.operation,
            crate::canonical_inventory::ItemTransformOperation::ArmorDamage { .. } | crate::canonical_inventory::ItemTransformOperation::ShieldDamage { .. })) {
            let writes = intent.derive(context, id, tick, &projected)?;
            if writes.iter().all(|(target, after)| {
                projected.get(target).is_some_and(
                    |before| matches!(&before.value, Value::Inventory(current) if current == after),
                )
            }) {
                continue;
            }
            if matches!(intent.operation, crate::canonical_inventory::ItemTransformOperation::ShieldDamage { .. })
                && writes.iter().any(|(target, after)| *target == intent.source.target && after.is_empty()) {
                if let Some(binding) = self.equipment.iter().find(|binding| binding.offhand == intent.source.target || binding.hotbar.contains(&intent.source.target)) {
                    let Target::Chunk { key, .. } = binding.living else { return Err(ApplyFailure::InvalidAction); };
                    effects.push(Effect::Put {
                        target: Target::Chunk { key, part: crate::canonical_action::ChunkPart::LivingProperty { actor: ActorId::Player(binding.lease.gid), property: crate::living_property::LivingProperty::ItemUse }, inventory: None },
                        value: Value::LivingProperty(crate::living_property::LivingPropertyValue::ItemUse(None)),
                    });
                }
            }
            for (target, after) in writes {
                let before = projected
                    .get(&target)
                    .ok_or(ApplyFailure::Condition(target))?;
                projected.insert(
                    target,
                    Cell {
                        revision: before.revision.wrapping_add(1),
                        value: Value::Inventory(after),
                    },
                );
            }
            effects.push(Effect::TransformItems {
                intent: intent.clone(),
            });
        }
        let mut equipment = self.equipment.clone();
        if let Some(binding) = &self.held_attack {
            if !equipment.contains(binding) {
                equipment.push(binding.clone());
            }
        }
        crate::canonical_inventory::append_equipment_effects_from_projection(
            values,
            &projected,
            &mut effects,
            &equipment,
            context,
        )
        .map_err(|_| ApplyFailure::InvalidAction)?;
        Ok(effects)
    }

    pub fn property_targets(&self) -> std::collections::BTreeSet<Target> {
        use crate::living_property::LivingProperty as P;
        let mut targets = std::collections::BTreeSet::new();
        let mut add = |body: Target, properties: &[P]| {
            let Target::Chunk {
                key,
                part,
                inventory: None,
            } = body
            else {
                return;
            };
            let actor = match part {
                crate::canonical_action::ChunkPart::Player(id) => ActorId::Player(id),
                crate::canonical_action::ChunkPart::Entity(id) => ActorId::Entity(id),
                _ => return,
            };
            targets.extend(properties.iter().map(|property| Target::Chunk {
                key,
                part: crate::canonical_action::ChunkPart::LivingProperty {
                    actor,
                    property: *property,
                },
                inventory: None,
            }));
        };
        let damage_properties = [
            P::Health,
            P::Absorption,
            P::VelocityX,
            P::VelocityY,
            P::VelocityZ,
            P::LastDamage,
            P::LastDamageKind,
            P::HurtStarted,
            P::HurtTicks,
            P::FireTicks,
            P::DeathTicks,
            P::DeathCause,
            P::LastAttacker,
            P::Invulnerable,
            P::ItemUse,
        ];
        for environment in &self.environment {
            add(environment.contact.target(), &environment.shape_properties);
            add(environment.contact.target(), &[P::Environment, P::StatusEffects]);
            add(environment.contact.target(), &damage_properties);
            add(
                environment.contact.target(),
                &[
                    P::Resistance,
                    P::FireResistance,
                    P::ArmorProtection,
                    P::Attribute(environment.armor_attribute),
                    P::Attribute(environment.toughness_attribute),
                ],
            );
            if matches!(environment.contact.actor(), ActorId::Player(_)) {
                add(environment.contact.target(), &[P::GameMode]);
            }
        }
        for fall in &self.falls {
            add(fall.contact.target(), &damage_properties);
            add(fall.contact.target(), &[P::StatusEffects,P::ArmorProtection,P::Resistance,P::FireResistance,P::GameMode,
                P::Attribute(fall.armor_attribute),P::Attribute(fall.toughness_attribute)]);
        }
        for tick in &self.ticks {
            if matches!(tick.identity.actor(), ActorId::Player(_)) {
                add(tick.identity.target(), &[P::GameMode]);
            }
            if matches!(tick.identity.actor(), ActorId::Entity(_)) {
                add(tick.identity.target(), &[P::MobReproduction]);
            }
            add(tick.identity.target(), &damage_properties);
            add(
                tick.identity.target(),
                &[P::ArmorProtection, P::Invulnerable],
            );
            add(
                tick.identity.target(),
                &[
                    P::SimulationTick,
                    P::HurtTicks,
                    P::AttackTicks,
                    P::DeathTicks,
                    P::DeathCause,
                    P::FireTicks,
                    P::StatusEffects,
                    P::Food,
                    P::Attribute(tick.rules.max_health_attribute),
                    P::Strength,
                    P::Weakness,
                    P::Resistance,
                    P::FireResistance,
                    P::Blindness,
                ],
            );
            for attribute in &tick.attribute_ids {
                add(tick.identity.target(), &[P::Attribute(*attribute)]);
            }
        }
        for damage in self.damages.iter().chain(self.damage_contacts.iter().map(|incoming| &incoming.damage)) {
            if matches!(
                damage.target,
                Target::Chunk {
                    part: crate::canonical_action::ChunkPart::Player(_),
                    ..
                }
            ) {
                add(damage.target, &[P::GameMode]);
            }
            add(damage.target, &damage_properties);
            if let Some(reduction) = damage.reduction {
                add(
                    damage.target,
                    &[
                        P::Attribute(reduction.armor_attribute),
                        P::Attribute(reduction.toughness_attribute),
                        P::ArmorProtection,
                        P::Resistance,
                        P::FireResistance,
                    ],
                );
            }
        }
        for attack in &self.attacks {
            if matches!(
                attack.target,
                Target::Chunk {
                    part: crate::canonical_action::ChunkPart::Player(_),
                    ..
                }
            ) {
                add(attack.target, &[P::GameMode]);
            }
            if matches!(attack.actor, ActorId::Player(_)) {
                add(attack.source, &[P::Food, P::GameMode, P::PlayerStance]);
            }
            add(
                attack.source,
                &[
                    P::Health,
                    P::Weapon,
                    P::Strength,
                    P::Weakness,
                    P::Blindness,
                    P::AttackStarted,
                    P::AttackTicks,
                    P::VelocityX,
                    P::VelocityY,
                    P::VelocityZ,
                    P::Attribute(attack.attributes.attack_damage),
                    P::Attribute(attack.attributes.attack_speed),
                ],
            );
            add(attack.target, &damage_properties);
            add(
                attack.target,
                &[
                    P::Attribute(attack.attributes.armor),
                    P::Attribute(attack.attributes.toughness),
                    P::Attribute(attack.attributes.knockback_resistance),
                    P::ArmorProtection,
                    P::Resistance,
                    P::FireResistance,
                    P::Blocking,
                    P::ItemUse,
                    P::Cooldowns,
                ],
            );
        }
        for incoming in &self.damage_contacts {
            add(incoming.contact.target(), &[P::Blocking, P::ItemUse, P::Cooldowns]);
        }
        for exhaustion in &self.exhaustions {
            add(exhaustion.target(), &[P::Food, P::GameMode]);
        }
        if let Some(clock) = &self.attack_clock {
            add(clock.target, &[P::AttackStarted, P::AttackTicks]);
        }
        targets.extend(self.statistics.iter().map(|statistic| statistic.target));
        targets
    }

    pub fn guards_valid(&self, action: &Action) -> bool {
        let leases = action.player_leases.iter().copied().collect::<std::collections::BTreeSet<_>>();
        let epochs = action.conditions.iter().filter_map(|guard| match guard.expected { Expected::SpatialEpochEquals(epoch) => Some((guard.target, epoch)), _ => None }).collect::<std::collections::BTreeSet<_>>();
        let mut births = std::collections::BTreeMap::<Target, Vec<(&crate::entity_body::EntityBirth, crate::realm_completion::PeerGeneration)>>::new();
        let mut controlled = std::collections::BTreeMap::<Target, Vec<(&crate::entity_body::EntityBirth, crate::realm_completion::PeerGeneration)>>::new();
        for guard in &action.conditions {
            match &guard.expected {
                Expected::EntityBirthOwned { birth, owner } => births.entry(guard.target).or_default().push((birth, *owner)),
                Expected::ActorControlled { birth, issuer } => controlled.entry(guard.target).or_default().push((birth, *issuer)),
                _ => {},
            }
        }
        if !self.damage_contacts.iter().all(|incoming| {
            let contact = &incoming.contact;
            epochs.contains(&(contact.target(), contact.epoch))
                && match &contact.identity {
                    crate::item_contact::ContactIdentity::Player(lease) => leases.contains(lease),
                    crate::item_contact::ContactIdentity::Entity { birth, .. } => action.conditions.iter().any(|guard|
                        guard.target == contact.target() && matches!(&guard.expected, Expected::EntityBirthOwned { birth: found, owner } if found == birth && *owner == contact.owner)),
                }
        }) { return false; }
        if !self.falls.iter().all(|intent| action.id.actor == intent.contact.actor()
            && action.actor_chunk == Some(intent.contact.chunk)
            && action.conditions.iter().any(|condition| condition.target == intent.contact.target()
                && condition.expected == Expected::SpatialEpochEquals(intent.contact.epoch))
            && match &intent.contact.identity {
                crate::item_contact::ContactIdentity::Player(lease) => leases.contains(lease) && action.id.incarnation == lease.incarnation,
                crate::item_contact::ContactIdentity::Entity {id,birth} => action.id.incarnation == u64::from(id.generation)
                    && action.conditions.iter().any(|condition| condition.target == intent.contact.target()
                        && matches!(&condition.expected,Expected::ActorControlled{birth:expected,issuer} if expected == birth && *issuer == intent.contact.owner)),
            }) { return false; }
        self.damage_parent.as_ref().is_none_or(|dependency| action.dependencies.contains(&dependency.parent)
            && dependency.parent.id.actor == action.id.actor
            && dependency.parent.id.incarnation == action.id.incarnation
            && dependency.parent.id != action.id)
            && self.environment.iter().all(|intent| action.id.actor == intent.contact.actor()
            && action.actor_chunk == Some(intent.contact.chunk)
            && action.conditions.iter().any(|condition| condition.target == intent.contact.target()
                && condition.expected == Expected::SpatialEpochEquals(intent.contact.epoch))
            && match &intent.contact.identity {
                crate::item_contact::ContactIdentity::Player(lease) => leases.contains(lease) && action.id.incarnation == lease.incarnation,
                crate::item_contact::ContactIdentity::Entity { id, birth } => action.id.incarnation == u64::from(id.generation)
                    && action.conditions.iter().any(|condition| condition.target == intent.contact.target()
                        && matches!(&condition.expected, Expected::ActorControlled { birth: expected, issuer } if expected == birth && *issuer == intent.contact.owner))
                    && action.conditions.iter().any(|condition| condition.target == intent.contact.target()
                        && matches!(&condition.expected, Expected::EntityBirthKind {birth:expected,owner,..} if expected == birth && *owner == intent.contact.owner)),
            }) && self.exhaustions.iter().all(|exhaustion| action.id.actor == ActorId::Player(exhaustion.lease.gid)
            && action.id.incarnation == exhaustion.lease.incarnation
            && action.actor_chunk == Some(exhaustion.chunk)
            && leases.contains(&exhaustion.lease)
            && action.conditions.iter().any(|condition| condition.target == exhaustion.target()
                && condition.expected == Expected::SpatialEpochEquals(exhaustion.epoch)))
        && self.ticks.iter().all(|tick| action.id.actor == tick.identity.actor()
            && action.actor_chunk == Some(tick.identity.chunk)
            && action.conditions.iter().any(|condition| condition.target == tick.identity.target()
                && condition.expected == Expected::SpatialEpochEquals(tick.identity.epoch))
            && match &tick.identity.identity {
                crate::item_contact::ContactIdentity::Player(lease) => leases.contains(lease) && action.id.incarnation == lease.incarnation,
                crate::item_contact::ContactIdentity::Entity { id, birth } => action.id.incarnation == u64::from(id.generation)
                    && action.conditions.iter().any(|condition| condition.target == tick.identity.target()
                        && matches!(&condition.expected, Expected::ActorControlled { birth: expected, issuer }
                            if expected == birth && *issuer == tick.identity.owner)),
            }) && self.attacks.iter().chain(self.vehicle_attacks.iter().map(|intent| &intent.attack)).all(|attack| {
            action.id.actor == attack.actor
                && action.actor_chunk == Some(attack.contact.attacker.chunk)
                && [&attack.contact.attacker, &attack.contact.victim].into_iter().all(|participant| {
                    epochs.contains(&(participant.target(), participant.epoch))
                        && match &participant.identity {
                            crate::item_contact::ContactIdentity::Player(lease) => leases.contains(lease),
                            crate::item_contact::ContactIdentity::Entity { birth, .. } => births.get(&participant.target()).is_some_and(|guards| guards.iter().any(|(found, owner)| *found == birth && *owner == participant.owner)),
                        }
                })
                && match &attack.contact.attacker.identity {
                    crate::item_contact::ContactIdentity::Player(lease) => action.id.incarnation == lease.incarnation,
                    crate::item_contact::ContactIdentity::Entity { id, birth } => {
                        action.id.incarnation == u64::from(id.generation)
                            && controlled.get(&attack.source).is_some_and(|guards| guards.iter().any(|(found, issuer)| *found == birth && *issuer == attack.contact.producer))
                    }
                }
        })
    }

    pub fn capture(
        &self,
        id: ActionId,
        actor_chunk: crate::protocol::ChunkKey,
        values: &std::collections::BTreeMap<Target, Cell>,
        conditions: Vec<Condition>,
    ) -> Result<Action, ApplyFailure> {
        if !self.item_transforms.is_empty()
            || !self.damage_contacts.is_empty()
            || !self.environment.is_empty() || !self.falls.is_empty()
            || self.held_attack.is_some()
            || !self.equipment.is_empty()
            || !self.ticks.is_empty()
        {
            return Err(ApplyFailure::InvalidAction);
        }
        self.capture_effects(
            id,
            actor_chunk,
            values,
            conditions,
            self.derive_effects(values)?,
        )
    }

    pub fn capture_with<S: crate::canonical_action::ActionState>(
        &self,
        context: &S,
        id: ActionId,
        actor_chunk: crate::protocol::ChunkKey,
        values: &std::collections::BTreeMap<Target, Cell>,
        conditions: Vec<Condition>,
    ) -> Result<Action, ApplyFailure> {
        if self.attacks.len().saturating_add(self.damages.len()).saturating_add(self.damage_contacts.len()) > crate::canonical_action::MAX_LOGICAL_RECORDS
            || self.statistics.len() > crate::canonical_action::MAX_LOGICAL_RECORDS
            || self.equipment.len() > crate::canonical_action::MAX_LOGICAL_RECORDS {
            return Err(ApplyFailure::ResourceLimit);
        }
        let minimum_records = self.resolution_targets().len().saturating_mul(3);
        let bytes = postcard::experimental::serialized_size(self).map_err(|_| ApplyFailure::InvalidAction)?;
        crate::canonical_action::check_logical_budget(minimum_records, bytes)?;
        let tick = self
            .attacks
            .first()
            .map(|attack| attack.tick)
            .or_else(|| self.damages.first().map(|damage| damage.tick))
            .or_else(|| self.damage_contacts.first().map(|incoming| incoming.damage.tick))
            .or_else(|| self.ticks.first().map(|tick| tick.identity.simulation_tick))
            .or_else(|| self.exhaustions.first().map(|exhaustion| exhaustion.tick))
            .or_else(|| self.falls.first().map(|intent| intent.contact.simulation_tick))
            .or_else(|| {
                self.environment
                    .first()
                    .map(|intent| intent.contact.simulation_tick)
            })
            .ok_or(ApplyFailure::InvalidAction)?;
        self.capture_effects(
            id,
            actor_chunk,
            values,
            conditions,
            self.derive_with(context, id, tick, values)?,
        )
    }

    fn capture_effects(
        &self,
        id: ActionId,
        actor_chunk: crate::protocol::ChunkKey,
        values: &std::collections::BTreeMap<Target, Cell>,
        mut conditions: Vec<Condition>,
        effects: Vec<Effect>,
    ) -> Result<Action, ApplyFailure> {
        let tick = self
            .damages
            .first()
            .map(|damage| damage.tick)
            .or_else(|| self.attacks.first().map(|attack| attack.tick))
            .or_else(|| self.damage_contacts.first().map(|incoming| incoming.damage.tick))
            .or_else(|| self.ticks.first().map(|tick| tick.identity.simulation_tick))
            .or_else(|| self.exhaustions.first().map(|exhaustion| exhaustion.tick))
            .or_else(|| self.falls.first().map(|intent| intent.contact.simulation_tick))
            .or_else(|| {
                self.environment
                    .first()
                    .map(|intent| intent.contact.simulation_tick)
            })
            .ok_or(ApplyFailure::InvalidAction)?;
        if id.incarnation == 0 || !self.is_valid(tick) {
            return Err(ApplyFailure::InvalidAction);
        }
        let targets = self.resolution_targets();
        for target in &targets {
            let cell = values
                .get(target)
                .cloned()
                .or_else(|| {
                    crate::living_property::body_target(*target).and_then(|(body, property)| {
                        values
                            .get(&body)
                            .and_then(|body| crate::derived_living::facet_cell(body, property))
                    })
                })
                .ok_or(ApplyFailure::Condition(*target))?;
            conditions.push(Condition {
                target: *target,
                expected: match &cell.value {
                    Value::Block(state) if self.environment.iter().any(|intent| intent.blocks.contains(target) || intent.support_blocks.contains(target)) => Expected::BlockState(*state),
                    _ => Expected::Exact(cell),
                },
            });
        }
        let mut player_leases = std::collections::BTreeSet::new();
        player_leases.extend(self.equipment.iter().map(|binding| binding.lease));
        player_leases.extend(self.held_attack.iter().map(|binding| binding.lease));
        for intent in &self.environment {
            if id.actor != intent.contact.actor() || actor_chunk != intent.contact.chunk {
                return Err(ApplyFailure::InvalidAction);
            }
            conditions.push(Condition {
                target: intent.contact.target(),
                expected: Expected::SpatialEpochEquals(intent.contact.epoch),
            });
            match &intent.contact.identity {
                crate::item_contact::ContactIdentity::Player(lease) => {
                    player_leases.insert(*lease);
                }
                crate::item_contact::ContactIdentity::Entity { birth, .. } => {
                    let Some(Cell {value:Value::Entity(body),..}) = values.get(&intent.contact.target()) else { return Err(ApplyFailure::Condition(intent.contact.target())); };
                    conditions.push(Condition {target:intent.contact.target(),expected:Expected::EntityBirthKind {birth:birth.clone(),owner:intent.contact.owner,kind:body.kind}});
                    conditions.push(Condition {
                        target: intent.contact.target(),
                        expected: Expected::ActorControlled {
                            birth: birth.clone(),
                            issuer: intent.contact.owner,
                        },
                    })
                }
            }
        }
        for intent in &self.falls {
            if id.actor != intent.contact.actor() || actor_chunk != intent.contact.chunk { return Err(ApplyFailure::InvalidAction) }
            conditions.push(Condition {target:intent.contact.target(),expected:Expected::SpatialEpochEquals(intent.contact.epoch)});
            match &intent.contact.identity {
                crate::item_contact::ContactIdentity::Player(lease) => {player_leases.insert(*lease);},
                crate::item_contact::ContactIdentity::Entity {birth,..} => conditions.push(Condition {target:intent.contact.target(),expected:Expected::ActorControlled{birth:birth.clone(),issuer:intent.contact.owner}}),
            }
        }
        for exhaustion in &self.exhaustions {
            if id.actor != ActorId::Player(exhaustion.lease.gid)
                || id.incarnation != exhaustion.lease.incarnation
                || actor_chunk != exhaustion.chunk
            {
                return Err(ApplyFailure::InvalidAction);
            }
            player_leases.insert(exhaustion.lease);
            conditions.push(Condition {
                target: exhaustion.target(),
                expected: Expected::SpatialEpochEquals(exhaustion.epoch),
            });
        }
        for tick in &self.ticks {
            if id.actor != tick.identity.actor() || actor_chunk != tick.identity.chunk {
                return Err(ApplyFailure::InvalidAction);
            }
            conditions.push(Condition {
                target: tick.identity.target(),
                expected: Expected::SpatialEpochEquals(tick.identity.epoch),
            });
            match &tick.identity.identity {
                crate::item_contact::ContactIdentity::Player(lease) => {
                    player_leases.insert(*lease);
                }
                crate::item_contact::ContactIdentity::Entity { birth, .. } => {
                    conditions.push(Condition {
                        target: tick.identity.target(),
                        expected: Expected::ActorControlled {
                            birth: birth.clone(),
                            issuer: tick.identity.owner,
                        },
                    })
                }
            }
        }
        for incoming in &self.damage_contacts {
            let contact = &incoming.contact;
            conditions.push(Condition { target: contact.target(), expected: Expected::SpatialEpochEquals(contact.epoch) });
            match &contact.identity {
                crate::item_contact::ContactIdentity::Player(lease) => { player_leases.insert(*lease); }
                crate::item_contact::ContactIdentity::Entity { birth, .. } => conditions.push(Condition {
                    target: contact.target(), expected: Expected::EntityBirthOwned { birth: birth.clone(), owner: contact.owner },
                }),
            }
        }
        for attack in self
            .attacks
            .iter()
            .chain(self.vehicle_attacks.iter().map(|intent| &intent.attack))
        {
            if id.actor != attack.actor || actor_chunk != attack.contact.attacker.chunk {
                return Err(ApplyFailure::InvalidAction);
            }
            for participant in [&attack.contact.attacker, &attack.contact.victim] {
                conditions.push(Condition {
                    target: participant.target(),
                    expected: Expected::SpatialEpochEquals(participant.epoch),
                });
                if let crate::item_contact::ContactIdentity::Player(lease) = participant.identity {
                    player_leases.insert(lease);
                } else if let crate::item_contact::ContactIdentity::Entity { birth, .. } = &participant.identity {
                    conditions.push(Condition {target:participant.target(),expected:Expected::EntityBirthOwned {birth:birth.clone(),owner:participant.owner}});
                }
            }
            if let crate::item_contact::ContactIdentity::Entity { birth, .. } =
                &attack.contact.attacker.identity
            {
                conditions.push(Condition {
                    target: attack.source,
                    expected: Expected::ActorControlled {
                        birth: birth.clone(),
                        issuer: attack.contact.producer,
                    },
                });
            }
        }
        let mut grouped = std::collections::BTreeMap::<Target, Vec<Condition>>::new();
        for condition in conditions {
            let guards = grouped.entry(condition.target).or_default();
            if !guards.contains(&condition) { guards.push(condition); }
        }
        let conditions = grouped.into_values().flatten().collect::<Vec<_>>();
        let player_chunks = self
            .equipment
            .iter()
            .chain(self.held_attack.iter())
            .filter_map(|binding| match binding.living {
                Target::Chunk { key, .. } => Some((binding.lease.key, key)),
                _ => None,
            })
            .collect::<std::collections::BTreeMap<_, _>>()
            .into_iter()
            .collect();
        let stakeholders = conditions
            .iter()
            .map(|condition| condition.target.stakeholder())
            .chain(targets.iter().map(|target| target.stakeholder()))
            .chain(std::iter::once(
                crate::canonical_action::Stakeholder::Chunk(actor_chunk),
            ))
            .collect::<std::collections::BTreeSet<_>>()
            .into_iter()
            .collect();
        Ok(Action {
            edit_authority: None,
            id,
            tick,
            class: ActionClass::Replayable,
            chunk_transition: false,
            actor_chunk: Some(actor_chunk),
            player_leases: player_leases.into_iter().collect(),
            player_chunks,
            stakeholders,
            dependencies: self.damage_parent.iter().map(|dependency| dependency.parent).collect(),
            conditions,
            effects,
            counterfactual_conditions: targets.iter().copied().collect(),
            counterfactual_effects: targets.into_iter().collect(),
            semantic_intent: Some(SemanticIntent::Combat(self.clone())),
            presentation: Vec::new(),
            factual_result: None,
            undo: vec![UndoProcedure::RestoreWrittenTargets],
            kind: ActionKind::Combat,
            client_origin: ClientOrigin::None,
        })
    }

    pub fn targets(&self) -> impl Iterator<Item = Target> + '_ {
        self.falls.iter().map(|intent|intent.contact.target()).chain(self.damages
            .iter()
            .map(|damage| damage.target))
            .chain(self.damage_contacts.iter().map(|incoming| incoming.contact.target()))
            .chain(
                self.attacks
                    .iter()
                    .flat_map(|attack| [attack.source, attack.target]),
            )
            .chain(self.attack_clock.iter().map(|clock| clock.target))
            .chain(self.ticks.iter().map(|tick| tick.identity.target()))
            .chain(
                self.environment
                    .iter()
                    .map(|intent| intent.contact.target()),
            )
            .chain(self.exhaustions.iter().map(FoodExhaustionIntent::target))
            .chain(self.statistics.iter().filter_map(|statistic| {
                crate::living_property::body_target(statistic.target).map(|(body, _)| body)
            }))
            .chain(self.resolution_targets())
    }

    pub fn is_valid(&self, tick: TickStamp) -> bool {
        let victim_leases = self.attacks.iter().filter_map(|attack| match attack.contact.victim.identity {
            crate::item_contact::ContactIdentity::Player(lease) => Some(lease), _ => None,
        }).chain(self.damage_contacts.iter().filter_map(|incoming| match incoming.contact.identity {
            crate::item_contact::ContactIdentity::Player(lease) => Some(lease), _ => None,
        })).collect::<std::collections::BTreeSet<_>>();

        self.damage_parent.as_ref().is_none_or(|dependency|
            dependency.parent.tick == tick && dependency.parent.id.incarnation != 0
                && !matches!(dependency.parent.id.actor, ActorId::Peer(_))
                && self.attacks.iter().all(|attack| attack.sweeping)
                && self.attack_clock.is_none() && self.held_attack.is_none())
            && self.damage_contacts.len() <= crate::canonical_action::MAX_LOGICAL_RECORDS && self.damage_contacts.iter().all(|incoming| incoming.valid(tick))
            && (!self.falls.is_empty() || !self.environment.is_empty() || !self.falls.is_empty() || !self.damage_contacts.is_empty()
            || !self.damages.is_empty()
            || !self.attacks.is_empty()
            || !self.vehicle_attacks.is_empty()
            || !self.ticks.is_empty()
            || !self.exhaustions.is_empty())
            && self.vehicle_attacks.len() <= 1
            && self
                .vehicle_attacks
                .iter()
                .all(|intent| intent.valid() && intent.attack.tick == tick)
            && self.environment.len() <= 7
            && self.falls.len() <= 1 && self.falls.iter().all(|intent|intent.valid(tick))
            && self.environment.iter().all(|intent| intent.is_valid(tick))
            && self.exhaustions.len() <= 1
            && self.exhaustions.iter().all(|intent| {
                intent.tick == tick
                    && intent.lease.incarnation != 0
                    && intent.amount_milli <= 40_000
                    && match intent.epoch {
                        crate::canonical_action::SpatialEpoch::Admission { lease, .. } => {
                            lease == intent.lease
                        }
                        crate::canonical_action::SpatialEpoch::Action(reference) => {
                            reference.id.incarnation != 0 && !matches!(reference.id.actor, ActorId::Peer(_))
                        }
                    }
            })
            && self.ticks.len() <= 1
            && self.ticks.iter().all(|intent| {
                intent.identity.simulation_tick == tick
                    && intent.identity.valid()
                    && intent.effective_position.is_none_or(|position| position.into_iter().all(|axis| f64::from_bits(axis).is_finite() && f64::from_bits(axis).abs() <= 30_000_000.0))
                    && intent
                        .previous_tick
                        .is_none_or(|previous| tick.is_after(previous))
                    && intent.attribute_ids.len() <= 256
                    && intent.attribute_ids.contains(&intent.armor_attribute)
                    && intent.attribute_ids.contains(&intent.toughness_attribute)
                    && intent
                        .attribute_ids
                        .contains(&intent.rules.max_health_attribute)
                    && intent.rules.difficulty <= 3
                    && [
                        intent.rules.regeneration_effect,
                        intent.rules.poison_effect,
                        intent.rules.wither_effect,
                        intent.rules.hunger_effect,
                    ]
                    .into_iter()
                    .collect::<std::collections::BTreeSet<_>>()
                    .len()
                        == 4
                    && intent
                        .attribute_ids
                        .iter()
                        .copied()
                        .collect::<std::collections::BTreeSet<_>>()
                        .len()
                        == intent.attribute_ids.len()
            })
            && self.damages.len().saturating_add(self.attacks.len()).saturating_add(self.damage_contacts.len()) <= crate::canonical_action::MAX_LOGICAL_RECORDS
            && self
                .attacks
                .iter()
                .all(|attack| attack.tick == tick && attack.is_valid())
            && (self.attacks.is_empty()
                || self
                    .attacks
                    .iter()
                    .filter(|attack| !attack.sweeping)
                    .count()
                    == usize::from(self.damage_parent.is_none()))
            && self.damages.iter().all(|damage| damage.tick == tick)
            && self
                .attack_clock
                .as_ref()
                .is_none_or(|clock| clock.tick == tick)
            && self.statistics.len() <= crate::canonical_action::MAX_LOGICAL_RECORDS
            && self.item_transforms.len() <= 16
            && self.equipment.len() <= crate::canonical_action::MAX_LOGICAL_RECORDS
            && self.equipment.windows(2).all(|pair| pair[0].lease < pair[1].lease)
            && self.held_attack.as_ref().is_none_or(|binding| {
                binding.is_valid()
                    && self
                        .attacks
                        .iter()
                        .chain(self.vehicle_attacks.iter().map(|intent| &intent.attack))
                        .any(|attack| {
                            !attack.sweeping
                                && attack.actor == ActorId::Player(binding.lease.gid)
                                && attack.source == binding.living
                                && attack.contact.attacker.identity
                                    == crate::item_contact::ContactIdentity::Player(binding.lease)
                        })
            })
            && self
                .item_transforms
                .iter()
                .all(crate::canonical_inventory::ItemTransformIntent::valid)
            && self
                .equipment
                .iter()
                .all(|binding| binding.is_valid() && victim_leases.contains(&binding.lease))
            && self.statistics.iter().all(|statistic| {
                matches!(
                    statistic.target,
                    Target::Chunk {
                        part: crate::canonical_action::ChunkPart::LivingProperty {
                            actor: ActorId::Player(_),
                            property: crate::living_property::LivingProperty::Statistic {
                                category: 0..=8,
                                ..
                            }
                        },
                        inventory: None,
                        ..
                    }
                ) && match statistic.kind {
                    CombatStatisticKind::Increment(amount) => {
                        amount > 0 && statistic.damage_targets.is_empty()
                    }
                    _ => !statistic.damage_targets.is_empty(),
                } && statistic.damage_targets.len() <= crate::canonical_action::MAX_LOGICAL_RECORDS
                    && statistic
                        .damage_targets
                        .iter()
                        .copied()
                        .collect::<std::collections::BTreeSet<_>>()
                        .len()
                        == statistic.damage_targets.len()
                    && statistic.damage_targets.iter().all(|target| {
                        self.damages.iter().any(|damage| damage.target == *target)
                            || self.attacks.iter().any(|attack| attack.target == *target)
                            || self
                                .ticks
                                .iter()
                                .any(|tick| tick.identity.target() == *target)
                    })
            })
            && self
                .statistics
                .iter()
                .map(|statistic| statistic.target)
                .collect::<std::collections::BTreeSet<_>>()
                .len()
                == self.statistics.len()
    }

    pub fn derive_effects(
        &self,
        values: &std::collections::BTreeMap<Target, Cell>,
    ) -> Result<Vec<Effect>, ApplyFailure> {
        self.derive_effects_with_charge(values,None,None,&std::collections::BTreeMap::new()).map(|(effects,_)|effects)
    }

    fn derive_effects_with_charge(
        &self,
        values: &std::collections::BTreeMap<Target, Cell>,
        parent_charge: Option<u64>,
        context: Option<&dyn crate::canonical_action::ActionState>,
        motion: &std::collections::BTreeMap<Target, crate::combat_command::VelocityCut>,
    ) -> Result<(Vec<Effect>,std::collections::BTreeSet<Target>),ApplyFailure> {
        let mut staged = values.clone();
        let mut written = std::collections::BTreeSet::new();
        let mut damage_totals = std::collections::BTreeMap::<Target, (u32, u32)>::new();
        let mut lethal_damage=std::collections::BTreeSet::new();
        for exhaustion in &self.exhaustions {
            let target = exhaustion.target();
            let cell = staged
                .get_mut(&target)
                .ok_or(ApplyFailure::Condition(target))?;
            let living = crate::living_property::state_mut(&mut cell.value)
                .ok_or(ApplyFailure::Type(target))?;
            let mode = living
                .avatar
                .as_ref()
                .ok_or(ApplyFailure::Type(target))?
                .game_mode;
            if exhaustion.apply(&mut living.food, mode)? {
                written.insert(target);
            }
        }
        for tick in &self.ticks {
            let target = tick.identity.target();
            let cell = staged
                .get_mut(&target)
                .ok_or(ApplyFailure::Condition(target))?;
            let (health,absorption,lethal)=tick.apply_accounted_with_context(&mut cell.value,context)?;
            damage_totals.insert(target,(health,absorption));
            if lethal {lethal_damage.insert(target);}
            written.insert(target);
        }
        let mut blocked_totals = std::collections::BTreeMap::<Target, u32>::new();
        for incoming in &self.damage_contacts {
            let living = staged.get(&incoming.contact.target()).and_then(|cell| living_value(&cell.value)).ok_or(ApplyFailure::Condition(incoming.contact.target()))?;
            let total = blocked_totals.entry(incoming.contact.target()).or_default();
            *total = total.saturating_add(incoming.blocked_damage(living));
        }
        let mut damages = self.damages.clone();
        let successful_primary = self
            .attacks
            .iter()
            .find(|attack| !attack.sweeping)
            .map(|attack| {
                let damage = attack.damage(&staged)?;
                let before = staged
                    .get(&damage.target)
                    .ok_or(ApplyFailure::Condition(damage.target))?;
                Ok::<_, ApplyFailure>(match damage.derive_effect(before)? {
                    Some(Effect::Put { value, .. }) => {
                        let previous = living_value(&before.value).ok_or(ApplyFailure::Type(damage.target))?;
                        let next = living_value(&value).ok_or(ApplyFailure::Type(damage.target))?;
                        next.combat.health_milli < previous.combat.health_milli
                            || next.combat.absorption_milli < previous.combat.absorption_milli
                    }
                    _ => false,
                })
            })
            .transpose()?
            .unwrap_or_else(|| self.damage_parent.is_some() || !self.vehicle_attacks.is_empty());
        for attack in &self.attacks {
            if attack.sweeping && !successful_primary {
                continue;
            }
            let blocked = attack.blocked_damage(&staged, parent_charge)?;
            let total = blocked_totals.entry(attack.target).or_default();
            *total = total.saturating_add(blocked);
            damages.push(attack.damage_derived(&staged, true, parent_charge, true, motion.get(&attack.target).copied())?);
        }
        for damage in &damages {
            let cell = staged
                .get(&damage.target)
                .ok_or(ApplyFailure::Condition(damage.target))?;
            if let Some(Effect::Put { target, value }) = damage.derive_effect(cell)? {
                let before =
                    crate::living_property::state(&cell.value).ok_or(ApplyFailure::Type(target))?;
                let after =
                    crate::living_property::state(&value).ok_or(ApplyFailure::Type(target))?;
                if before.combat.health_milli>0 && after.combat.health_milli==0 {lethal_damage.insert(target);}
                let total = damage_totals.entry(target).or_default();
                total.0 = total.0.saturating_add(
                    before
                        .combat
                        .health_milli
                        .saturating_sub(after.combat.health_milli),
                );
                total.1 = total.1.saturating_add(
                    before
                        .combat
                        .absorption_milli
                        .saturating_sub(after.combat.absorption_milli),
                );
                let revision = cell.revision.wrapping_add(1);
                staged.insert(target, Cell { revision, value });
                written.insert(target);
            }
        }
        if let Some(clock) = &self.attack_clock {
            let cell = staged
                .get_mut(&clock.target)
                .ok_or(ApplyFailure::Condition(clock.target))?;
            let living = crate::living_property::state_mut(&mut cell.value)
                .ok_or(ApplyFailure::Type(clock.target))?;
            if successful_primary && let Some(attack) = self.attacks.iter().find(|attack| !attack.sweeping) {
                let base = living.combat.effective_attributes.get(&attack.attributes.attack_speed)
                    .copied().map(f64::from_bits).ok_or(ApplyFailure::InvalidAction)?;
                let speed = living.attribute(attack.attributes.attack_speed, base)
                    - if living.combat.equipment.unarmed { 2.4 } else { 0.0 };
                let charge = if attack.pose.bedrock { 1.0 } else {
                    living.combat.timing.last_attack_tick.map_or(1.0, |last|
                        ((f64::from(attack.tick.distance_since(last)) + 0.5) * speed / 20.0).clamp(0.0, 1.0))
                };
                if attack.pose.knockback_enabled && (living.combat.equipment.knockback != 0
                    || attack.pose.sprinting && charge > 0.9) {
                    let initial = motion.get(&clock.target).map(|cut| {
                        crate::combat_command::velocity_values(values, clock.target).map(|winning| cut.resolve(winning))
                    }).transpose()?.unwrap_or(living.combat.velocity_micro);
                    let mut response = initial;
                    for axis in [0, 2] {
                        response[axis] = (initial[axis] as f64 * 0.6).round() as i64;
                    }
                    if response != initial {
                        living.combat.velocity_micro = response;
                    }
                    if let Some(avatar) = living.avatar.as_mut() { avatar.stance.sprinting = false; }
                }
            }
            if successful_primary
                && living
                    .avatar
                    .as_ref()
                    .is_some_and(|avatar| matches!(avatar.game_mode, 0 | 2))
            {
                let before = living.food.exhaustion_milli;
                living.food.exhaustion_milli = before.saturating_add(100).min(40_000);
                if living.food.exhaustion_milli != before {
                    living.food.revision = living
                        .food
                        .revision
                        .checked_add(1)
                        .ok_or(ApplyFailure::Overflow(clock.target))?;
                }
            }
            let living = match &mut cell.value {
                Value::Living(living) => living,
                Value::Entity(body) => match &mut body.payload {
                    EntityPayload::Living(living) => living,
                    _ => return Err(ApplyFailure::Type(clock.target)),
                },
                _ => return Err(ApplyFailure::Type(clock.target)),
            };
            if !living.belongs_to(clock.actor) {
                return Err(ApplyFailure::InvalidAction);
            }
            living.combat.timing.last_attack_tick = Some(clock.tick);
            living.combat.timing.attack_ticks = 0;
            living.combat.revision = living.combat.revision.wrapping_add(1);
            written.insert(clock.target);
        }
        let mut effects = Vec::new();
        for target in self.property_targets() {
            let (body, property) =
                crate::living_property::body_target(target).ok_or(ApplyFailure::Type(target))?;
            if !written.contains(&body) {
                continue;
            }
            let before = values
                .get(&body)
                .and_then(|body| crate::derived_living::facet_cell(body, property))
                .ok_or(ApplyFailure::Condition(target))?;
            let after = staged
                .get(&body)
                .and_then(|body| crate::derived_living::facet_cell(body, property))
                .ok_or(ApplyFailure::Condition(target))?;
            if before.value != after.value {
                effects.push(Effect::Put {
                    target,
                    value: after.value,
                });
            }
        }
        for statistic in &self.statistics {
            let mut amount = match statistic.kind {
                CombatStatisticKind::Increment(amount) => amount,
                _ => 0,
            };
            for damage_target in &statistic.damage_targets {
                let before_damage = values
                    .get(damage_target)
                    .and_then(|cell| crate::living_property::state(&cell.value))
                    .ok_or(ApplyFailure::Condition(*damage_target))?;
                let after_damage = staged
                    .get(damage_target)
                    .and_then(|cell| crate::living_property::state(&cell.value))
                    .ok_or(ApplyFailure::Condition(*damage_target))?;
                amount = amount.saturating_add(match statistic.kind {
                    CombatStatisticKind::Increment(_) => 0,
                    CombatStatisticKind::Damage => {
                        damage_totals
                            .get(damage_target)
                            .map_or(0, |total| total.0)
                            .saturating_add(50)
                            / 100
                    }
                    CombatStatisticKind::Blocked => blocked_totals.get(damage_target).copied().unwrap_or(0).saturating_add(50) / 100,
                    CombatStatisticKind::Absorbed => {
                        damage_totals
                            .get(damage_target)
                            .map_or(0, |total| total.1)
                            .saturating_add(50)
                            / 100
                    }
                    CombatStatisticKind::Kill => u32::from(
                        lethal_damage.contains(damage_target) &&                         before_damage.combat.health_milli > 0
                            && after_damage.combat.health_milli == 0,
                    ),
                });
            }
            if amount == 0 {
                continue;
            }
            let (body, property) = crate::living_property::body_target(statistic.target)
                .ok_or(ApplyFailure::Type(statistic.target))?;
            let cell = values
                .get(&body)
                .and_then(|body| crate::derived_living::facet_cell(body, property))
                .ok_or(ApplyFailure::Condition(statistic.target))?;
            let Value::LivingProperty(crate::living_property::LivingPropertyValue::Unsigned(
                current,
            )) = cell.value
            else {
                return Err(ApplyFailure::Type(statistic.target));
            };
            effects.push(Effect::Put {
                target: statistic.target,
                value: Value::LivingProperty(
                    crate::living_property::LivingPropertyValue::Unsigned(
                        current.saturating_add(amount).min(i32::MAX as u32),
                    ),
                ),
            });
        }
        Ok((effects,lethal_damage))
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Default, Serialize, Deserialize)]
pub struct CombatEquipmentFacts {
    pub respiration: u16,
    pub freeze_immune: bool,
    pub modifiers: Vec<CombatAttributeModifier>,
    pub sharpness: u16,
    pub smite: u16,
    pub bane_of_arthropods: u16,
    pub knockback: u16,
    pub fire_aspect: u16,
    pub breach: u16,
    pub sweeping_edge: u16,
    pub protection: u32,
    pub fire_protection: u32,
    pub blast_protection: u32,
    pub projectile_protection: u32,
    pub feather_falling: u32,
    pub sword: bool,
    pub axe: bool,
    pub mace: bool,
    pub shield: bool,
    pub unarmed: bool,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct CombatAttributeModifier {
    pub attribute: u16,
    pub slot: u8,
    pub id: String,
    pub amount: u64,
    pub operation: CombatModifierOperation,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub enum CombatModifierOperation {
    Add,
    MultiplyBase,
    MultiplyTotal,
}

impl CombatEquipmentFacts {
    pub fn is_valid(&self) -> bool {
        self.modifiers.len() <= 256
            && self.modifiers.iter().all(|modifier| {
                modifier.slot <= 7
                    && modifier.id.len() <= 256
                    && f64::from_bits(modifier.amount).is_finite()
            })
    }

    pub fn attribute(&self, attribute: u16, base: f64) -> f64 {
        self.attribute_with_permanent(attribute, base, &[])
    }

    pub fn attribute_with_permanent(&self, attribute: u16, base: f64, permanent: &[CombatAttributeModifier]) -> f64 {
        Self::attribute_from_sources(attribute, base, &self.modifiers, permanent)
    }

    pub fn attribute_from_sources(attribute: u16, base: f64, modifiers: &[CombatAttributeModifier], permanent: &[CombatAttributeModifier]) -> f64 {
        let selected = modifiers.iter()
            .filter(|modifier| modifier.attribute == attribute)
            .map(|modifier| (modifier.id.as_str(), modifier))
            .chain(permanent.iter().filter(|modifier| modifier.attribute == attribute)
                .map(|modifier| (modifier.id.as_str(), modifier)))
            .collect::<std::collections::BTreeMap<_, _>>();
        let mut added = base;
        for modifier in selected
            .values()
            .filter(|modifier| modifier.operation == CombatModifierOperation::Add)
        {
            added += f64::from_bits(modifier.amount);
        }
        let mut result = added;
        for modifier in selected
            .values()
            .filter(|modifier| modifier.operation == CombatModifierOperation::MultiplyBase)
        {
            result += added * f64::from_bits(modifier.amount);
        }
        for modifier in selected
            .values()
            .filter(|modifier| modifier.operation == CombatModifierOperation::MultiplyTotal)
        {
            result *= 1.0 + f64::from_bits(modifier.amount);
        }
        result
    }
}

pub fn damage_after_armor(damage: f32, armor: f32, toughness: f32, breach: u32) -> f32 {
    let toughness = 2.0 + toughness / 4.0;
    let armor = (armor - damage / toughness).clamp(armor * 0.2, 20.0);
    let mut fraction = armor / 25.0;
    if breach != 0 {
        fraction = (fraction * (1.0 - (breach as f32 * 0.15).min(1.0))).clamp(0.0, 1.0);
    }
    damage * (1.0 - fraction)
}

pub fn damage_after_protection(damage: f32, protection: f32) -> f32 {
    damage * (1.0 - protection.clamp(0.0, 20.0) / 25.0)
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct AttackIntent {
    pub actor: ActorId,
    pub source: Target,
    pub target: Target,
    pub tick: TickStamp,
    pub contact: AttackContact,
    pub pose: AttackPose,
    pub attributes: CombatAttributeIds,
    pub target_undead: bool,
    pub target_arthropod: bool,
    pub sweeping: bool,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct AttackContact {
    pub attacker: crate::item_contact::ContactObservation,
    pub victim: crate::item_contact::ContactObservation,
    pub reach: u64,
    pub eye_height: u64,
    pub victim_yaw: u32,
    pub victim_on_ground: bool,
    pub producer: crate::realm_completion::PeerGeneration,
    pub tick: TickStamp,
}

impl AttackContact {
    pub fn valid(&self) -> bool {
        let reach = f64::from_bits(self.reach);
        let eye_height = f64::from_bits(self.eye_height);
        if !self.attacker.valid()
            || !self.victim.valid()
            || self.attacker.owner != self.producer
            || self.attacker.actor() == self.victim.actor()
            || self.attacker.world != self.victim.world
            || self.attacker.simulation_tick != self.tick
            || self.victim.simulation_tick != self.tick
            || !reach.is_finite()
            || reach < 0.0
            || !eye_height.is_finite()
            || !(0.0..=2048.0).contains(&eye_height)
            || !f32::from_bits(self.victim_yaw).is_finite()
        {
            return false;
        }
        let source = self.attacker.position.map(f64::from_bits);
        let destination = self.victim.position.map(f64::from_bits);
        let width = f64::from_bits(self.victim.half_width);
        let height = f64::from_bits(self.victim.height);
        let eye = source[1] + eye_height;
        let dx = (source[0] - destination[0]).abs().max(width) - width;
        let dz = (source[2] - destination[2]).abs().max(width) - width;
        let dy = eye - eye.clamp(destination[1], destination[1] + height);
        (reach > 0.0 && reach > f64::MAX / reach) || dx.mul_add(dx, dz.mul_add(dz, dy * dy)) <= reach * reach
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub struct AttackPose {
    pub sprinting: bool,
    pub on_ground: bool,
    pub fall_distance: u32,
    pub bedrock: bool,
    pub yaw: u32,
    pub knockback_enabled: bool,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub struct CombatAttributeIds {
    pub attack_damage: u16,
    pub attack_speed: u16,
    pub armor: u16,
    pub toughness: u16,
    pub knockback_resistance: u16,
}

pub(crate) fn living_value(value: &Value) -> Option<&ActorLivingState> {
    match value {
        Value::Living(living) => Some(living),
        Value::Entity(body) => match &body.payload {
            EntityPayload::Living(living) => Some(living),
            _ => None,
        },
        _ => None,
    }
}

impl AttackIntent {
    pub fn is_valid(&self) -> bool {
        self.contact.valid()
            && self.actor == self.contact.attacker.actor()
            && self.source == self.contact.attacker.target()
            && self.target == self.contact.victim.target()
            && self.tick == self.contact.tick
            && f32::from_bits(self.pose.fall_distance).is_finite()
            && f32::from_bits(self.pose.fall_distance) >= 0.0
            && f32::from_bits(self.pose.yaw).is_finite()
            && self.source != self.target
    }

    pub fn damage(
        &self,
        values: &std::collections::BTreeMap<Target, Cell>,
    ) -> Result<DamageIntent, ApplyFailure> {
        self.damage_with_protection(values, true, None)
    }

    pub fn damage_before_protection(
        &self,
        values: &std::collections::BTreeMap<Target, Cell>,
    ) -> Result<DamageIntent, ApplyFailure> {
        self.damage_with_protection(values, false, None)
    }

    fn blocked_damage(&self, values: &std::collections::BTreeMap<Target, Cell>, parent_charge: Option<u64>) -> Result<u32, ApplyFailure> {
        let target = values.get(&self.target).and_then(|cell| living_value(&cell.value)).ok_or(ApplyFailure::Type(self.target))?;
        if !target.combat.equipment.shield || !target.use_state.as_ref().is_some_and(|state| {
            state.mode == crate::item_action::ItemUseMode::Block
                && (self.tick == state.started || self.tick.is_after(state.started))
                && self.tick.distance_since(state.started) >= u16::from(state.blocking_delay_ticks)
        }) { return Ok(0); }
        let attacker = self.contact.attacker.position.map(f64::from_bits);
        let victim = self.contact.victim.position.map(f64::from_bits);
        let yaw = f64::from(f32::from_bits(self.contact.victim_yaw)).to_radians();
        if (attacker[0] - victim[0]).mul_add(-yaw.sin(), (attacker[2] - victim[2]) * yaw.cos()) <= 0.0 { return Ok(0); }
        Ok(self.damage_derived(values, false, parent_charge, false, None)?.damage_milli)
    }

    fn damage_with_protection(
        &self,
        values: &std::collections::BTreeMap<Target, Cell>,
        reduce_protection: bool,
        parent_charge: Option<u64>,
    ) -> Result<DamageIntent, ApplyFailure> {
        self.damage_derived(values, reduce_protection, parent_charge, true, None)
    }

    fn damage_derived(
        &self,
        values: &std::collections::BTreeMap<Target, Cell>,
        reduce_protection: bool,
        parent_charge: Option<u64>,
        allow_blocking: bool,
        motion: Option<crate::combat_command::VelocityCut>,
    ) -> Result<DamageIntent, ApplyFailure> {
        let source = values
            .get(&self.source)
            .and_then(|cell| living_value(&cell.value))
            .ok_or(ApplyFailure::Type(self.source))?;
        let target = values
            .get(&self.target)
            .and_then(|cell| living_value(&cell.value))
            .ok_or(ApplyFailure::Type(self.target))?;
        if source.combat.health_milli == 0 { return Err(ApplyFailure::Condition(self.source)); }
        if !source.belongs_to(self.actor) || !self.is_valid() {
            return Err(ApplyFailure::InvalidAction);
        }
        let blocking = allow_blocking && target.combat.equipment.shield
            && target.use_state.as_ref().is_some_and(|state| {
                state.mode == crate::item_action::ItemUseMode::Block
                    && (self.tick == state.started || self.tick.is_after(state.started))
                    && self.tick.distance_since(state.started)
                        >= u16::from(state.blocking_delay_ticks)
            });
        if blocking {
            let attacker = self.contact.attacker.position.map(f64::from_bits);
            let victim = self.contact.victim.position.map(f64::from_bits);
            let yaw = f64::from(f32::from_bits(self.contact.victim_yaw)).to_radians();
            if (attacker[0] - victim[0]).mul_add(-yaw.sin(), (attacker[2] - victim[2]) * yaw.cos())
                > 0.0
            {
                return Ok(DamageIntent {
                    kind: if matches!(self.actor, ActorId::Player(_)) {
                        DamageKind::PlayerAttack
                    } else {
                        DamageKind::MobAttack
                    },
                    source: self.actor,
                    target: self.target,
                    tick: self.tick,
                    damage_milli: 0,
                    knockback_micro: [0; 3],
                    bypass_hurt_cooldown: false,
                    fire_ticks: 0,
                    reduction: None,
                    bypass_invulnerability: false,
                });
            }
        }
        let attribute = |living: &ActorLivingState, id| {
            let base = living
                .combat
                .effective_attributes
                .get(&id)
                .copied()
                .map(f64::from_bits)
                .ok_or(ApplyFailure::InvalidAction)?;
            let value = living.attribute(id, base);
            value
                .is_finite()
                .then_some(value)
                .ok_or(ApplyFailure::InvalidAction)
        };
        let cooldown = if let Some(charge) = parent_charge.filter(|_| self.sweeping) {
            f64::from_bits(charge)
        } else if self.pose.bedrock || !matches!(self.actor, ActorId::Player(_)) {
            1.0
        } else {
            let speed = attribute(source, self.attributes.attack_speed)?
                - if source.combat.equipment.unarmed { 2.4 } else { 0.0 };
            source.combat.timing.last_attack_tick.map_or(1.0, |last| {
                ((f64::from(self.tick.distance_since(last)) + 0.5) * speed / 20.0).clamp(0.0, 1.0)
            })
        };
        let weapon = &source.combat.equipment;
        let enchantment = if weapon.sharpness > 0 {
            0.5 * f64::from(weapon.sharpness) + 0.5
        } else {
            0.0
        } + if self.target_undead {
            2.5 * f64::from(weapon.smite)
        } else {
            0.0
        } + if self.target_arthropod {
            2.5 * f64::from(weapon.bane_of_arthropods)
        } else {
            0.0
        };
        let mut damage = attribute(source, self.attributes.attack_damage)?
            * cooldown.powi(2).mul_add(0.8, 0.2)
            + enchantment * cooldown;
        damage = damage.max(0.0);
        if self.sweeping {
            damage =
                if cooldown > 0.9 && weapon.sword && self.pose.on_ground && !self.pose.sprinting {
                    1.0 + attribute(source, self.attributes.attack_damage)?
                        * f64::from(weapon.sweeping_edge)
                        / (f64::from(weapon.sweeping_edge) + 1.0)
                } else {
                    0.0
                };
        }
        let fall = f32::from_bits(self.pose.fall_distance);
        let strong = cooldown > 0.9;
        let mace = !self.sweeping && weapon.mace && !self.pose.on_ground && fall > 1.5;
        let critical = !self.sweeping
            && strong
            && !self.pose.sprinting
            && !self.pose.on_ground
            && fall > 0.0
            && !mace
            && !source.combat.status.blindness;
        if critical {
            damage *= 1.5;
        }
        if mace {
            damage += 1.5 * f64::from(fall);
        }
        let raw_damage = damage.max(0.0) as f32;
        let damage = damage_after_armor(
            damage.max(0.0) as f32,
            attribute(target, self.attributes.armor)? as f32,
            attribute(target, self.attributes.toughness)? as f32,
            u32::from(weapon.breach),
        );
        let damage = target.combat.status.resistance.map_or(damage, |level| {
            damage * (1.0 - (f32::from(level) + 1.0) * 0.2).max(0.0)
        });
        let damage = if reduce_protection {
            damage_after_protection(damage, target.combat.equipment.protection as f32)
        } else { raw_damage };
        if !damage.is_finite() || damage < 0.0 || damage > u32::MAX as f32 / 1000.0 {
            return Err(ApplyFailure::InvalidAction);
        }
        let yaw = f64::from(f32::from_bits(self.pose.yaw)).to_radians();
        let resistance = attribute(target, self.attributes.knockback_resistance)?.clamp(0.0, 1.0);
        let recent = target.combat.timing.last_damage_tick.is_some_and(|tick| {
            self.tick == tick || self.tick.is_after(tick) && self.tick.distance_since(tick) < 10
        });
        let original_velocity = target.combat.velocity_micro;
        let winning_velocity = motion.map(|_| crate::combat_command::velocity_values(values, self.target)).transpose()?;
        let initial_velocity = match (motion, winning_velocity) {
            (Some(cut), Some(winning)) => cut.resolve(winning),
            _ => original_velocity,
        };
        let mut velocity = initial_velocity.map(|value| value as f64 / 1_000_000.0);
        let on_ground = self.contact.victim_on_ground;
        if self.pose.knockback_enabled && !recent {
            let attacker_position = self.contact.attacker.position.map(f64::from_bits);
            let victim_position = self.contact.victim.position.map(f64::from_bits);
            apply_knockback(
                &mut velocity,
                0.4 * (1.0 - resistance),
                [
                    attacker_position[0] - victim_position[0],
                    attacker_position[2] - victim_position[2],
                ],
                on_ground,
            );
        }
        let extra = if self.sweeping {
            0.0
        } else {
            (f64::from(weapon.knockback)
                + if strong && self.pose.sprinting {
                    1.0
                } else {
                    0.0
                })
                * 0.5
                * (1.0 - resistance)
        };
        if self.pose.knockback_enabled {
            apply_knockback(&mut velocity, extra, [yaw.sin(), -yaw.cos()], on_ground);
        }
        let next_velocity = velocity.map(|value| (value * 1_000_000.0).round() as i64);
        let knockback_micro = if let (Some(cut), Some(winning)) = (motion, winning_velocity) {
            cut.response_delta(winning, next_velocity, self.target)?
        } else {
            let mut delta = [0; 3];
            for axis in 0..3 {
                delta[axis] = next_velocity[axis]
                    .checked_sub(original_velocity[axis])
                    .ok_or(ApplyFailure::InvalidAction)?;
            }
            delta
        };
        Ok(DamageIntent {
            kind: if matches!(self.actor, ActorId::Player(_)) {
                DamageKind::PlayerAttack
            } else {
                DamageKind::MobAttack
            },
            source: self.actor,
            target: self.target,
            tick: self.tick,
            damage_milli: (damage * 1000.0).round() as u32,
            knockback_micro,
            bypass_hurt_cooldown: false,
            fire_ticks: if self.sweeping {
                0
            } else {
                u32::from(weapon.fire_aspect) * 80
            },
            reduction: None,
            bypass_invulnerability: false,
        })
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Default, Serialize, Deserialize)]
pub struct CombatStatusFacts {
    pub strength: Option<u8>,
    pub weakness: Option<u8>,
    pub resistance: Option<u8>,
    pub fire_resistance: bool,
    pub blindness: bool,
    pub invulnerable: bool,
}

fn apply_knockback(velocity: &mut [f64; 3], strength: f64, direction: [f64; 2], on_ground: bool) {
    if strength <= 0.0 {
        return;
    }
    let length = direction[0].hypot(direction[1]);
    let direction = if length > 1.0e-8 {
        direction.map(|value| value / length)
    } else {
        [1.0, 0.0]
    };
    velocity[0] = velocity[0] * 0.5 - direction[0] * strength;
    velocity[2] = velocity[2] * 0.5 - direction[1] * strength;
    if on_ground {
        velocity[1] = (velocity[1] * 0.5 + strength).min(0.4);
    }
}

#[cfg(test)]
#[path = "combat_action_tests.rs"]
pub(crate) mod tests;

#[cfg(test)]
mod numerical_identity_tests {
    use super::*;
    use crate::canonical_action::{ActionRef, SpatialEpoch};
    use crate::entity_body::EntityBirth;
    use crate::identity::ServerId;
    use crate::item_contact::ContactIdentity;
    use crate::protocol::{ChunkKey, GlobalEntityId, WorldId};
    use crate::realm_completion::PeerGeneration;

    pub(super) fn identity() -> LivingTickIdentity {
        let source = PeerGeneration { server: ServerId(1), generation: 2 };
        let id = GlobalEntityId { origin: source.server, local_id: 7, generation: source.generation };
        let birth_action = ActionRef { tick: TickStamp(0), id: ActionId {
            actor: ActorId::Entity(id), incarnation: 2, sequence: 0, family: 1 } };
        LivingTickIdentity { identity: ContactIdentity::Entity { id, birth: EntityBirth {
            action: birth_action, source, uuid: uuid::Uuid::from_u128(9) } }, owner: source,
            epoch: SpatialEpoch::Action(birth_action),
            chunk: ChunkKey::new(WorldId { dimension: 0, key: 1 }, 0, 0), simulation_tick: TickStamp(10) }
    }

    pub(super) fn tick(identity: LivingTickIdentity) -> LivingTickIntent {
        LivingTickIntent { identity, effective_position: None, previous_tick: Some(TickStamp(9)),
            attribute_ids: Vec::new(), fire_immune: false, armor_attribute: 0, toughness_attribute: 1,
            rules: LivingTickRules { minimum_y: -64, fire_damage: false, difficulty: 2,
                natural_regeneration: true, max_health_attribute: 2, regeneration_effect: 3,
                poison_effect: 4, wither_effect: 5, hunger_effect: 6 } }
    }

    pub(super) fn living(actor: ActorId) -> ActorLivingState {
ActorLivingState {
        permanent_attribute_modifiers: Default::default(),
        menu: Default::default(),
        environment: Default::default(),
        tags: Default::default(),
        tags_revision: 1,
        advancements: Default::default(),
        stand: None,
        mob: None,
        merchant: None,
        simulation_tick: Some(TickStamp(9)),
        effects: Vec::new(),
        hidden_effects: Default::default(),
        property_versions: Default::default(),
        food: crate::item_action::ActorFoodState {
            exhaustion_milli: 0,
            tick_timer: 0,
            actor,
            food: 20,
            saturation_milli: 0,
            revision: 0,
        },
        combat: crate::item_action::ActorCombatState {
            actor,
            revision: 0,
            health_milli: 10_000,
            absorption_milli: 0,
            velocity_micro: [0; 3],
            timing: Default::default(),
            effective_attributes: Default::default(),
            equipment: Default::default(),
            status: Default::default(),
        },
        use_state: None,
        cooldowns: Default::default(),
        avatar: None,
    }
    }

    #[test]
    fn numerical_timers_need_no_positive_spatial_contact() {
        let identity = identity();
        assert!(identity.valid());
        let mut body = living(identity.actor());
        body.combat.timing.hurt_ticks = 5;
        body.combat.timing.fire_ticks = 21;
        let mut value = Value::Living((body).into());
        assert_eq!(tick(identity).apply(&mut value).unwrap(), (0, 0));
        let Value::Living(body) = value else { unreachable!() };
        assert_eq!(body.combat.timing.hurt_ticks, 4);
        assert_eq!(body.combat.timing.fire_ticks, 20);
        assert_eq!(body.simulation_tick, Some(TickStamp(10)));
    }

    #[test]
    fn void_damage_requires_actual_frozen_effective_position() {
        let identity = identity();
        let body = living(identity.actor());
        let mut without_sample = Value::Living((body.clone()).into());
        let intent = tick(identity);
        assert_eq!(intent.apply(&mut without_sample).unwrap(), (0, 0));
        let mut with_sample = Value::Living((body).into());
        let mut intent = intent;
        intent.effective_position = Some([0_f64.to_bits(), (-129_f64).to_bits(), 0_f64.to_bits()]);
        assert_eq!(intent.apply(&mut with_sample).unwrap(), (4000, 0));
        let Value::Living(body) = with_sample else { unreachable!() };
        assert_eq!(body.combat.health_milli, 6000);
    }
}

#[cfg(test)]
mod retained_numerical_interval_tests {
    use super::*;

    #[test]
    fn fire_cooldown_advances_after_damage_inside_retained_interval() {
        let identity = super::numerical_identity_tests::identity();
        let mut body = super::numerical_identity_tests::living(identity.actor());
        body.simulation_tick = Some(TickStamp(0));
        body.combat.timing.fire_ticks = 40;
        let mut intent = super::numerical_identity_tests::tick(identity);
        intent.identity.simulation_tick = TickStamp(20);
        intent.previous_tick = Some(TickStamp(0));
        intent.rules.fire_damage = true;
        let mut value = Value::Living((body).into());
        assert_eq!(intent.apply(&mut value).unwrap(), (1000, 0));
        let Value::Living(body) = value else { unreachable!() };
        assert_eq!(body.combat.health_milli, 9000);
        assert_eq!(body.combat.timing.hurt_ticks, 1);
        assert_eq!(body.combat.timing.fire_ticks, 20);
        assert_eq!(body.simulation_tick, Some(TickStamp(20)));
    }
}

#[cfg(test)]
mod environmental_water_tests {
    use super::*;
    use crate::canonical_action::{ActionState, ChunkPart, TruthView};
    use crate::item_contact::ContactObservation;
    use crate::living_property::{LivingProperty as P, LivingPropertyValue as V};
    use crate::protocol::BlockPos;
    use std::collections::BTreeMap;

    struct Registry;
    impl ActionState for Registry {
        fn environmental_contact(&self, _: &EnvironmentalDamageIntent, _: &BTreeMap<Target,Cell>) -> Result<EnvironmentalContactFacts,ApplyFailure> {
            Ok(EnvironmentalContactFacts {touching_water:true,immersed:true,breathable:false,freeze_damage_milli:1000})
        }
        fn commit(&mut self, _: crate::canonical_action::ActionContext, _: TruthView,
            _: &[crate::canonical_action::AppliedWrite]) -> Result<(), ApplyFailure> {
            Err(ApplyFailure::InvalidAction)
        }
        fn read(&self, _: TruthView, _: Target) -> Option<Cell> { None }
        fn environmental_hazard(&self, kind: EnvironmentalHazardKind, state: u16)
            -> Result<Option<EnvironmentalHazardFacts>, ApplyFailure> {
            Ok((kind == EnvironmentalHazardKind::Water && state == 1).then_some(
                EnvironmentalHazardFacts { damage_milli: 0, fire_ticks: 0, bypass_armor: true }))
        }
    }

    fn wet(duration: i32) -> (CombatIntent, BTreeMap<Target, Cell>, ActionId, Target) {
        let identity = super::numerical_identity_tests::identity();
        let mut living = super::numerical_identity_tests::living(identity.actor());
        living.environment.air_ticks = 0;
        living.combat.timing.fire_ticks = 100;
        living.effects.push(crate::player_admin::EffectState { kind: 7, duration, amplifier: 0,
            ambient: false, show_particles: true, show_icon: true, blend: false });
        let contact = ContactObservation { identity: identity.identity.clone(), owner: identity.owner,
            epoch: identity.epoch, world: identity.chunk.world, chunk: identity.chunk,
            position: [0_f64.to_bits(); 3], half_width: 0.3_f64.to_bits(), height: 1.8_f64.to_bits(),
            simulation_tick: identity.simulation_tick, observed_sequence: crate::identity::PlayerSeq(0) };
        let block = Target::Chunk { key: identity.chunk, part: ChunkPart::Block(BlockPos { x: 0, y: 1, z: 0 }), inventory: None };
        let environment = EnvironmentalDamageIntent { support_blocks:Vec::new(),shape_properties:Vec::new(), world_rules: None, elapsed: 1,
            rules: EnvironmentalRules { fire_damage: true, drowning_damage: true, freeze_damage: true,
                water_breathing_effect: 7, can_breathe_water: false, freeze_immune: false },
            contact, kind: EnvironmentalHazardKind::Water, blocks: vec![block],
            armor_attribute: 0, toughness_attribute: 1, fire_immune: false };
        let values = [(identity.target(), Cell { revision: 1, value: Value::Living((living).into()) }),
            (block, Cell { revision: 1, value: Value::Block(1) })].into_iter().collect();
        let id = ActionId { actor: identity.actor(), incarnation: 2, sequence: 1, family: 13 };
        (CombatIntent { environment: vec![environment], ..Default::default() }, values, id, identity.target())
    }

    #[test]
    fn largest_legal_contact_preserves_all_environmental_cells() {
        let (mut intent, _, _, _) = wet(0);
        let environment = &mut intent.environment[0];
        environment.contact.position = [0.5_f64.to_bits(); 3];
        environment.contact.half_width = 8.0_f64.to_bits();
        environment.contact.height = 16.0_f64.to_bits();
        environment.kind = EnvironmentalHazardKind::PowderSnow;
        environment.blocks = (-8..9).flat_map(|x| (0..17).flat_map(move |y|
            (-8..9).map(move |z| BlockPos { x, y, z })))
            .map(|block| Target::Chunk { key: crate::protocol::ChunkKey::new(
                environment.contact.world, block.x.div_euclid(16), block.z.div_euclid(16)),
                part: ChunkPart::Block(block), inventory: None }).collect();
        environment.blocks.sort_unstable();
        assert_eq!(environment.blocks.len(), MAX_ENVIRONMENTAL_BLOCKS);
        assert!(environment.is_valid(TickStamp(10)));
        environment.blocks.push(environment.blocks[0]);
        assert!(!environment.is_valid(TickStamp(10)));
    }

    #[test]
    fn eye_water_extinguishes_fire_and_expired_breathing_does_not_protect() {
        let (intent, values, id, body) = wet(0);
        let effects = intent.derive_with(&Registry, id, TickStamp(10), &values).unwrap();
        let Target::Chunk { key, .. } = body else { unreachable!() };
        assert!(effects.iter().any(|effect| matches!(effect, Effect::Put {target,
            value: Value::LivingProperty(V::Unsigned(0))}
            if *target == (Target::Chunk {key,part:ChunkPart::LivingProperty {actor:id.actor,property:P::FireTicks},inventory:None}))));
        assert!(effects.iter().any(|effect| matches!(effect, Effect::Put {
            value: Value::LivingProperty(V::Environment(environment)), .. } if environment.air_ticks == -1)));
        let (intent, values, id, _) = wet(1);
        let effects = intent.derive_with(&Registry, id, TickStamp(10), &values).unwrap();
        assert!(effects.iter().any(|effect| matches!(effect, Effect::Put {
            value: Value::LivingProperty(V::Environment(environment)), .. } if environment.air_ticks == 4)));
    }
}

#[cfg(test)]
mod retained_status_tests {
    use super::*;
    use crate::canonical_action::{ActionContext, ActionState, AppliedWrite, TruthView};

    struct Registry;
    impl ActionState for Registry {
        fn read(&self, _: TruthView, _: Target) -> Option<Cell> { None }
        fn commit(&mut self, _: ActionContext, _: TruthView, _: &[AppliedWrite]) -> Result<(), ApplyFailure> {
            Err(ApplyFailure::InvalidAction)
        }
        fn status_effect_facts(&self, effects: &crate::active_effects::ActiveEffectSet)
            -> Result<CombatStatusFacts, ApplyFailure> {
            Ok(CombatStatusFacts { fire_resistance: effects.visible.iter().any(|effect|
                effect.kind == 7 && effect.duration != 0), ..Default::default() })
        }
        fn status_effect_modifiers(&self, _: &crate::active_effects::ActiveEffectSet)
            -> Result<Vec<CombatAttributeModifier>, ApplyFailure> { Ok(Vec::new()) }
    }

    #[test]
    fn expired_fire_resistance_stops_protecting_later_retained_steps() {
        let identity = super::numerical_identity_tests::identity();
        let mut living = super::numerical_identity_tests::living(identity.actor());
        living.simulation_tick = Some(TickStamp(0));
        living.combat.timing.fire_ticks = 21;
        living.combat.status.fire_resistance = true;
        living.effects.push(crate::player_admin::EffectState { kind: 7, duration: 1, amplifier: 0,
            ambient: false, show_particles: true, show_icon: true, blend: false });
        let mut intent = super::numerical_identity_tests::tick(identity);
        intent.identity.simulation_tick = TickStamp(20);
        intent.previous_tick = Some(TickStamp(0));
        intent.rules.fire_damage = true;
        let mut value = Value::Living((living).into());
        assert_eq!(intent.apply_with_context(&mut value, Some(&Registry)).unwrap(), (1000, 0));
        let Value::Living(living) = value else { unreachable!() };
        assert_eq!(living.combat.health_milli, 9000);
        assert!(!living.combat.status.fire_resistance);
        assert!(living.effects.is_empty());
    }
}

#[cfg(test)]
mod health_boost_expiry_tests {
    use super::*;
    use crate::canonical_action::{ActionContext,ActionState,AppliedWrite,TruthView};
    use std::collections::BTreeMap;
    pub(super) struct Registry;
    fn modifier(id:&str,slot:u8,amount:f64)->CombatAttributeModifier {
        CombatAttributeModifier {attribute:2,id:id.to_owned(),slot,amount:amount.to_bits(),operation:CombatModifierOperation::Add}
    }
    impl ActionState for Registry {
        fn read(&self,_:TruthView,_:Target)->Option<Cell>{None}
        fn commit(&mut self,_:ActionContext,_:TruthView,_:&[AppliedWrite])->Result<(),ApplyFailure>{Err(ApplyFailure::InvalidAction)}
        fn status_effect_facts(&self,_:&crate::active_effects::ActiveEffectSet)->Result<CombatStatusFacts,ApplyFailure>{Ok(Default::default())}
        fn status_effect_modifiers(&self,effects:&crate::active_effects::ActiveEffectSet)->Result<Vec<CombatAttributeModifier>,ApplyFailure>{
            Ok(effects.visible.iter().filter(|effect|effect.kind==20 && effect.duration!=0)
                .map(|effect|modifier("minecraft:effect.health_boost",7,4.0*(f64::from(effect.amplifier)+1.0))).collect())
        }
        fn inventory_equipment_attribute_ids(&self)->Vec<u16>{vec![2]}
    }
    pub(super) fn fixture()->(LivingTickIntent,Value){
        let identity=super::numerical_identity_tests::identity();
        let mut living=super::numerical_identity_tests::living(identity.actor());
        living.combat.effective_attributes.insert(2,20_f64.to_bits());
        living.combat.health_milli=39_000;
        living.combat.timing.last_damage_kind=Some(DamageKind::Cactus);
        living.combat.timing.last_damage_milli=777;
        living.combat.equipment.modifiers=vec![modifier("gear",4,5.0),modifier("minecraft:effect.health_boost",7,4.0)];
        living.permanent_attribute_modifiers.insert(2,vec![modifier("permanent",0,10.0)]);
        living.effects.push(crate::player_admin::EffectState {kind:20,duration:1,amplifier:0,
            ambient:false,show_particles:true,show_icon:true,blend:false});
        let mut intent=super::numerical_identity_tests::tick(identity);
        intent.attribute_ids=vec![2];
        (intent,Value::Living((living).into()))
    }
    #[test]
    fn expiry_clamps_to_composed_max_without_damage_or_credit(){
        for retained in [1,4] {
            let (mut intent,mut value)=fixture();
            intent.identity.simulation_tick=TickStamp(9+retained);
            let timing=crate::living_property::state(&value).unwrap().combat.timing;
            assert_eq!(intent.apply_accounted_with_context(&mut value,Some(&Registry)).unwrap(),(0,0,false));
            let living=crate::living_property::state(&value).unwrap();
            assert_eq!(living.combat.health_milli,35_000);
            assert_eq!(living.combat.timing.last_damage_milli,timing.last_damage_milli);
            assert_eq!(living.combat.timing.last_damage_kind,timing.last_damage_kind);
            assert_eq!(living.combat.timing.last_attacker,timing.last_attacker);
            assert_eq!(living.combat.timing.death_cause,timing.death_cause);
            assert!(living.combat.equipment.modifiers.iter().any(|modifier|modifier.id=="gear"));
            assert_eq!(living.permanent_attribute_modifiers.get(&2).unwrap(),&vec![modifier("permanent",0,10.0)]);
        }
    }
    #[test]
    fn unrelated_max_attribute_change_without_expiry_does_not_clamp(){
        let (intent,mut value)=fixture();
        let living=crate::living_property::state_mut(&mut value).unwrap();
        living.effects.clear();
        living.combat.equipment.modifiers.retain(|modifier|modifier.slot!=7);
        assert_eq!(intent.apply_accounted_with_context(&mut value,Some(&Registry)).unwrap(),(0,0,false));
        assert_eq!(crate::living_property::state(&value).unwrap().combat.health_milli,39_000);
    }
    #[test]
    fn expiry_reveals_gear_modifier_with_same_registry_id(){
        let (intent,mut value)=fixture();
        let living=crate::living_property::state_mut(&mut value).unwrap();
        living.combat.equipment.modifiers[0].id="minecraft:effect.health_boost".to_owned();
        assert_eq!(intent.apply_accounted_with_context(&mut value,Some(&Registry)).unwrap(),(0,0,false));
        let living=crate::living_property::state(&value).unwrap();
        assert_eq!(living.combat.health_milli,35_000);
        assert_eq!(living.attribute(2,20.0),35.0);
    }
    #[test]
    fn clamp_and_actual_damage_have_distinct_lethality(){
        let (mut intent,mut value)=fixture();
        let living=crate::living_property::state_mut(&mut value).unwrap();
        living.combat.effective_attributes.insert(2,0_f64.to_bits());
        living.permanent_attribute_modifiers.clear();
        living.combat.equipment.modifiers.retain(|modifier|modifier.slot==7);
        living.combat.health_milli=4000;
        living.combat.timing.fire_ticks=20;
        intent.rules.fire_damage=true;
        assert_eq!(intent.apply_accounted_with_context(&mut value,Some(&Registry)).unwrap(),(1000,0,false));
        assert_eq!(crate::living_property::state(&value).unwrap().combat.health_milli,0);
        let (mut intent,mut value)=fixture();
        let living=crate::living_property::state_mut(&mut value).unwrap();
        living.combat.health_milli=500;
        living.combat.timing.fire_ticks=20;
        intent.rules.fire_damage=true;
        assert_eq!(intent.apply_accounted_with_context(&mut value,Some(&Registry)).unwrap(),(500,0,true));
    }
    #[test]
    fn canonical_expiry_projects_health_without_damage_kind_or_death_cause(){
        let (intent,value)=fixture();
        let identity=intent.identity.clone();
        let mut source=BTreeMap::from([(identity.target(),Cell {revision:1,value})]);
        let combat=CombatIntent {ticks:vec![intent],..Default::default()};
        let body=source.get(&identity.target()).unwrap().clone();
        for target in combat.property_targets() {
            let (_,property)=crate::living_property::body_target(target).unwrap();
            source.insert(target,crate::derived_living::facet_cell(&body,property).unwrap());
        }
        let id=ActionId {actor:identity.actor(),incarnation:2,sequence:1,family:13};
        let effects=combat.derive_with(&Registry,id,identity.simulation_tick,&source).unwrap();
        use crate::living_property::{LivingProperty as P,LivingPropertyValue as V};
        assert!(effects.iter().any(|effect|matches!(effect,Effect::Put {target:Target::Chunk {part:crate::canonical_action::ChunkPart::LivingProperty {property:P::Health,..},..},value:Value::LivingProperty(V::Unsigned(35_000))})));
        assert!(!effects.iter().any(|effect|matches!(effect,Effect::Put {target:Target::Chunk {part:crate::canonical_action::ChunkPart::LivingProperty {property:P::LastDamage|P::LastDamageKind|P::LastAttacker|P::DeathCause,..},..},..})));
    }
}

#[cfg(test)]
mod nondamaging_expiry_lifecycle_tests {
    use super::*;
    use crate::canonical_action::{ActionRef, ChunkPart};
    use crate::living_property::{LivingProperty as P, LivingPropertyValue as V};
    use std::collections::BTreeMap;

    #[test]
    fn zero_max_expiry_has_exact_lifecycle_cause_without_damage_credit() {
        let (intent, mut value) = super::health_boost_expiry_tests::fixture();
        let living = crate::living_property::state_mut(&mut value).unwrap();
        living.combat.effective_attributes.insert(2, 0.0_f64.to_bits());
        living.permanent_attribute_modifiers.clear();
        living.combat.equipment.modifiers.retain(|modifier| modifier.slot == 7);
        living.combat.health_milli = 4000;
        let original_timing = living.combat.timing;
        let identity = intent.identity.clone();
        let combat = CombatIntent { ticks: vec![intent.clone()], ..Default::default() };
        let body = Cell { revision: 1, value };
        let mut values = BTreeMap::from([(identity.target(), body.clone())]);
        for target in combat.property_targets() {
            let (_, property) = crate::living_property::body_target(target).unwrap();
            values.insert(target, crate::derived_living::facet_cell(&body, property).unwrap());
        }
        let id = ActionId { actor: identity.actor(), incarnation: 2, sequence: 1, family: 13 };
        let reference = ActionRef { id, tick: identity.simulation_tick };
        let effects = combat.derive_with(&super::health_boost_expiry_tests::Registry, id,
            identity.simulation_tick, &values).unwrap();
        assert!(effects.iter().any(|effect| matches!(effect, Effect::Put {
            target: Target::Chunk { part: ChunkPart::LivingProperty { property: P::DeathCause, .. }, .. },
            value: Value::LivingProperty(V::ActionReference(Some(actual))) } if *actual == reference)));
        assert!(!effects.iter().any(|effect| matches!(effect, Effect::Put {
            target: Target::Chunk { part: ChunkPart::LivingProperty {
                property: P::LastDamage | P::LastDamageKind | P::LastAttacker | P::Statistic { .. }, .. }, .. }, .. })));
        let mut after = body.value;
        for effect in effects {
            let Effect::Put { target, value: Value::LivingProperty(value) } = effect else { panic!(); };
            let (_, property) = crate::living_property::body_target(target).unwrap();
            assert!(property.write(crate::living_property::state_mut(&mut after).unwrap(), &value));
        }
        let living = crate::living_property::state(&after).unwrap();
        assert_eq!(living.combat.health_milli, 0);
        assert_eq!(living.combat.timing.death_cause, Some(reference));
        assert!(!living.combat.timing.death_inventory_processed);
        assert_eq!(living.combat.timing.last_damage_kind, original_timing.last_damage_kind);
        let mut next = intent;
        next.previous_tick = Some(identity.simulation_tick);
        next.identity.simulation_tick = TickStamp(identity.simulation_tick.0.wrapping_add(20));
        next.apply_accounted_with_context(&mut after, Some(&super::health_boost_expiry_tests::Registry)).unwrap();
        let living = crate::living_property::state(&after).unwrap();
        assert_eq!(living.combat.timing.death_ticks, 20);
        assert_eq!(living.combat.timing.death_cause, Some(reference));
    }
}

#[cfg(test)]
mod projected_numerical_tests {
    use super::*;

    #[test]
    fn projected_step_matches_reference_with_large_unrelated_payloads() {
        for elapsed in [1, 4, 21] {
            let (mut intent, mut value) = super::health_boost_expiry_tests::fixture();
            let living = crate::living_property::state_mut(&mut value).unwrap();
            living.simulation_tick = Some(TickStamp(intent.identity.simulation_tick.0.wrapping_sub(elapsed)));
            intent.previous_tick = living.simulation_tick;
            living.combat.timing.fire_ticks = 42;
            living.combat.status.fire_resistance = true;
            living.avatar = super::tests::fixture(1).1.avatar;
            living.avatar.as_mut().unwrap().game_mode = 1;
            let payload = (0..32768).map(|id| ((0, id), id)).collect::<std::collections::BTreeMap<_, _>>();
            if let Some(avatar) = living.avatar.as_mut() { avatar.statistics = payload.clone(); }
            let payload_address = crate::living_property::state(&value).unwrap().avatar.as_ref().unwrap().statistics.first_key_value().unwrap().0 as *const (u16, u32);
            let mut reference = value.clone();
            let expected = intent.apply_accounted_reference(&mut reference, Some(&super::health_boost_expiry_tests::Registry)).unwrap();
            let actual = intent.apply_accounted_with_context(&mut value, Some(&super::health_boost_expiry_tests::Registry)).unwrap();
            assert_eq!(actual, expected);
            assert_eq!(value, reference);
            if let Some(avatar) = crate::living_property::state(&value).unwrap().avatar.as_ref() {
                assert_eq!(avatar.statistics, payload);
                assert_eq!(avatar.statistics.first_key_value().unwrap().0 as *const (u16, u32), payload_address);
            }
        }
    }
}
