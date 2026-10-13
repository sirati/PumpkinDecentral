use serde::{Deserialize, Serialize};

use crate::actor::ActorId;
use crate::combat_action::CombatAttributeModifier;
use crate::item_action::ActorLivingState;
use crate::time::TickStamp;

pub fn body_target(
    target: crate::canonical_action::Target,
) -> Option<(crate::canonical_action::Target, LivingProperty)> {
    use crate::canonical_action::{ChunkPart, Target};
    let Target::Chunk {
        key,
        part: ChunkPart::LivingProperty { actor, property },
        inventory: None,
    } = target
    else {
        return None;
    };
    let part = match actor {
        ActorId::Player(id) => ChunkPart::Player(id),
        ActorId::Entity(id) => ChunkPart::Entity(id),
        ActorId::Peer(_) => return None,
    };
    Some((
        Target::Chunk {
            key,
            part,
            inventory: None,
        },
        property,
    ))
}

pub fn state(value: &crate::canonical_action::Value) -> Option<&ActorLivingState> {
    match value {
        crate::canonical_action::Value::Living(living) => Some(living),
        crate::canonical_action::Value::Entity(body) => match &body.payload {
            crate::entity_body::EntityPayload::Living(living) => Some(living),
            _ => None,
        },
        _ => None,
    }
}

pub fn state_mut(value: &mut crate::canonical_action::Value) -> Option<&mut ActorLivingState> {
    match value {
        crate::canonical_action::Value::Living(living) => Some(living),
        crate::canonical_action::Value::Entity(body) => match &mut body.payload {
            crate::entity_body::EntityPayload::Living(living) => Some(living),
            _ => None,
        },
        _ => None,
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash, Serialize, Deserialize)]
pub enum LivingProperty {
    PermanentAttributeModifiers(u16),
    DeathInventoryProcessed,
    Environment,
    Advancements,
    ArmorStand(crate::armor_stand::StandProperty),
    MobContinuation,
    MobSpecies,
    MobSpeciesProperty(crate::mob_species_property::MobSpeciesProperty),
    MobReproduction,
    GameMode,
    PlayerStance,
    FallDistance,
    SimulationTick,
    Experience,
    MerchantOffers,
    MenuSession,
    ItemUse,
    Food,
    Cooldowns,
    StatusEffects,
    Statistic { category: u16, key: u32 },
    Health,
    Absorption,
    VelocityX,
    VelocityY,
    VelocityZ,
    LastDamage,
    LastDamageKind,
    HurtStarted,
    HurtTicks,
    AttackStarted,
    AttackTicks,
    FireTicks,
    DeathTicks,
    DeathCause,
    LastAttacker,
    Attribute(u16),
    Weapon,
    ArmorProtection,
    Blocking,
    Strength,
    Weakness,
    Resistance,
    FireResistance,
    Blindness,
    Invulnerable,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub enum LivingPropertyValue {
    PermanentAttributeModifiers(Vec<CombatAttributeModifier>),
    Environment(crate::item_action::LivingEnvironmentState),
    Advancements(crate::player_advancement::PlayerAdvancementState),
    DamageKind(Option<crate::combat_action::DamageKind>),
    ArmorStand(Option<crate::armor_stand::StandPropertyValue>),
    MobSpeciesProperty(Option<crate::mob_species_property::MobSpeciesPropertyValue>),
    PlayerStance(crate::item_action::PlayerStance),
    MobContinuation(Option<crate::mob_continuation::MobBrain>),
    MobSpecies(Option<crate::mob_continuation::MobSpecies>),
    MobReproduction(Option<crate::mob_continuation::MobReproduction>),
    Experience(crate::player_experience::PlayerExperienceState),
    MerchantOffers(Option<crate::canonical_merchant::MerchantOffersState>),
    MenuSession(crate::canonical_menu::MenuSessionState),
    ItemUse(Option<crate::item_action::ItemUseState>),
    Food(crate::item_action::ActorFoodState),
    Cooldowns(std::collections::BTreeMap<String, crate::item_action::ItemCooldownState>),
    StatusEffects(crate::active_effects::ActiveEffectSet),
    Unsigned(u32),
    Signed(i64),
    OptionalTick(Option<TickStamp>),
    ActionReference(Option<crate::canonical_action::ActionRef>),
    Actor(Option<ActorId>),
    Attribute {
        base: Option<u64>,
        modifiers: Vec<CombatAttributeModifier>,
        transient_modifiers: Vec<CombatAttributeModifier>,
    },
    Weapon(WeaponFacts),
    ArmorProtection(ArmorProtectionFacts),
    Boolean(bool),
    Amplifier(Option<u8>),
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub struct WeaponFacts {
    pub sharpness: u16,
    pub smite: u16,
    pub bane_of_arthropods: u16,
    pub knockback: u16,
    pub fire_aspect: u16,
    pub breach: u16,
    pub sweeping_edge: u16,
    pub sword: bool,
    pub axe: bool,
    pub mace: bool,
    pub unarmed: bool,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub struct ArmorProtectionFacts {
    pub respiration: u16,
    pub freeze_immune: bool,
    pub protection: u32,
    pub fire: u32,
    pub explosion: u32,
    pub projectile: u32,
    pub fall: u32,
}

impl LivingProperty {
    pub fn accepts(self, value: &LivingPropertyValue, actor: ActorId) -> bool {
        use LivingPropertyValue as V;
        if let (Self::DeathCause, V::ActionReference(reference)) = (self, value) {
            return reference.as_ref().is_none_or(|reference| {
                reference.id.incarnation != 0 && !matches!(reference.id.actor, ActorId::Peer(_))
            });
        }
        if matches!(actor, ActorId::Peer(_)) {
            return false;
        }
        match (self, value) {
            (Self::Environment, V::Environment(value)) => value.valid(),
            (Self::ArmorStand(property), V::ArmorStand(Some(value))) => {
                matches!(actor, ActorId::Entity(_)) && property.accepts(value)
            }
            (
                Self::Health | Self::Absorption | Self::LastDamage | Self::FireTicks,
                V::Unsigned(_),
            )
            | (Self::VelocityX | Self::VelocityY | Self::VelocityZ, V::Signed(_))
            | (Self::HurtStarted | Self::AttackStarted, V::OptionalTick(_))
            | (Self::Weapon, V::Weapon(_))
            | (Self::ArmorProtection, V::ArmorProtection(_))
            | (
                Self::DeathInventoryProcessed | Self::Blocking | Self::FireResistance | Self::Blindness | Self::Invulnerable,
                V::Boolean(_),
            )
            | (Self::Strength | Self::Weakness | Self::Resistance, V::Amplifier(_)) => true,
            (Self::HurtTicks | Self::AttackTicks | Self::DeathTicks, V::Unsigned(value)) => {
                *value <= u32::from(u16::MAX)
            }
            (Self::LastDamageKind, V::DamageKind(_)) => true,
            (Self::LastAttacker, V::Actor(value)) => {
                value.is_none_or(|actor| !matches!(actor, ActorId::Peer(_)))
            }
            (
                Self::Statistic {
                    category: 0..=8, ..
                },
                V::Unsigned(value),
            ) => matches!(actor, ActorId::Player(_)) && *value <= i32::MAX as u32,
            (Self::Experience, V::Experience(value)) => {
                matches!(actor, ActorId::Player(_)) && value.is_valid()
            }
            (Self::MerchantOffers, V::MerchantOffers(value)) => matches!(actor, ActorId::Entity(_))
                && value.as_ref().is_none_or(crate::canonical_merchant::MerchantOffersState::valid),
            (Self::MenuSession, V::MenuSession(value)) => matches!(actor, ActorId::Player(_)) && value.belongs_to(actor),
            (Self::Advancements, V::Advancements(value)) => matches!(actor, ActorId::Player(_)) && value.valid(),
            (Self::GameMode, V::Unsigned(value)) => {
                matches!(actor, ActorId::Player(_)) && *value <= 3
            }
            (Self::PlayerStance, V::PlayerStance(_)) => matches!(actor, ActorId::Player(_)),
            (Self::FallDistance, V::Unsigned(value)) => {
                matches!(actor, ActorId::Player(_))
                    && f32::from_bits(*value).is_finite()
                    && f32::from_bits(*value) >= 0.0
            }
            (Self::PermanentAttributeModifiers(id), V::PermanentAttributeModifiers(modifiers)) => {
                modifiers.len() <= 256
                    && modifiers.iter().all(|modifier| modifier.attribute == id && modifier.slot <= 7
                        && !modifier.id.is_empty() && modifier.id.len() <= 256
                        && f64::from_bits(modifier.amount).is_finite())
                    && modifiers.iter().map(|modifier| &modifier.id).collect::<std::collections::BTreeSet<_>>().len() == modifiers.len()
            }
            (Self::Attribute(id), V::Attribute { base, modifiers, transient_modifiers }) => {
                base.is_none_or(|bits| f64::from_bits(bits).is_finite())
                    && modifiers.len() <= 256
                    && transient_modifiers.len() <= 256
                    && modifiers.iter().chain(transient_modifiers).all(|modifier| {
                        modifier.attribute == id
                            && modifier.slot <= 7
                            && modifier.id.len() <= 256
                            && f64::from_bits(modifier.amount).is_finite()
                    })
            }
            (Self::ItemUse, V::ItemUse(value)) => value.as_ref().is_none_or(|state| {
                state.actor == actor
                    && !state.item.is_empty()
                    && state.item.count <= 64
                    && state.item.nbt.len() <= 65_536
                    && state.blocking_delay_ticks <= 5
            }),
            (Self::Food, V::Food(value)) => {
                value.actor == actor
                    && value.food <= 20
                    && value.exhaustion_milli <= 40_000
                    && value.tick_timer < 80
                    && value.saturation_milli <= u32::from(value.food) * 1000
            }
            (Self::Cooldowns, V::Cooldowns(value)) => {
                value.len() <= 256
                    && value.iter().all(|(group, state)| {
                        !group.is_empty()
                            && group.len() <= 256
                            && *group == state.group
                            && state.actor == actor
                            && state.revision != 0
                    })
            }
            (Self::StatusEffects, V::StatusEffects(value)) => value.valid(),
            (Self::SimulationTick, V::OptionalTick(_)) => true,
            (Self::MobContinuation, V::MobContinuation(value)) => {
                matches!(actor, ActorId::Entity(_))
                    && value
                        .as_ref()
                        .is_none_or(crate::mob_continuation::MobBrain::valid)
            }
            (Self::MobSpecies, V::MobSpecies(value)) => {
                matches!(actor, ActorId::Entity(_))
                    && value
                        .as_ref()
                        .is_none_or(crate::mob_continuation::MobSpecies::valid)
            }
            (Self::MobSpeciesProperty(property), V::MobSpeciesProperty(Some(value))) => {
                matches!(actor, ActorId::Entity(_)) && property.accepts(value)
            }
            (Self::MobReproduction, V::MobReproduction(value)) => {
                matches!(actor, ActorId::Entity(_))
                    && value
                        .as_ref()
                        .is_none_or(crate::mob_continuation::MobReproduction::valid)
            }
            _ => false,
        }
    }

    pub const FIXED: [Self; 41] = [
        Self::DeathInventoryProcessed,
        Self::Environment,
        Self::Advancements,
        Self::MobContinuation,
        Self::MobSpecies,
        Self::MobReproduction,
        Self::GameMode,
        Self::PlayerStance,
        Self::FallDistance,
        Self::SimulationTick,
        Self::StatusEffects,
        Self::Experience,
        Self::MerchantOffers,
        Self::MenuSession,
        Self::ItemUse,
        Self::Food,
        Self::Cooldowns,
        Self::Health,
        Self::Absorption,
        Self::VelocityX,
        Self::VelocityY,
        Self::VelocityZ,
        Self::LastDamage,
        Self::LastDamageKind,
        Self::HurtStarted,
        Self::HurtTicks,
        Self::AttackStarted,
        Self::AttackTicks,
        Self::FireTicks,
        Self::DeathTicks,
        Self::DeathCause,
        Self::LastAttacker,
        Self::Weapon,
        Self::ArmorProtection,
        Self::Blocking,
        Self::Strength,
        Self::Weakness,
        Self::Resistance,
        Self::FireResistance,
        Self::Blindness,
        Self::Invulnerable,
    ];

    pub fn all(living: &ActorLivingState) -> std::collections::BTreeSet<Self> {
        Self::FIXED
            .into_iter()
            .filter(|property| property.accepts(&property.read(living), living.combat.actor))
            .chain(
                crate::armor_stand::StandProperty::ALL
                    .into_iter()
                    .map(Self::ArmorStand)
                    .filter(|property| {
                        property.accepts(&property.read(living), living.combat.actor)
                    }),
            )
            .chain(
                crate::mob_species_property::MobSpeciesProperty::ALL
                    .iter()
                    .copied()
                    .map(Self::MobSpeciesProperty)
                    .filter(|property| {
                        property.accepts(&property.read(living), living.combat.actor)
                    }),
            )
            .chain(
                living
                    .avatar
                    .iter()
                    .flat_map(|avatar| avatar.statistics.keys())
                    .map(|(category, key)| Self::Statistic {
                        category: *category,
                        key: *key,
                    }),
            )
            .chain(
                living
                    .combat
                    .effective_attributes
                    .keys()
                    .copied()
                    .map(Self::Attribute),
            )
            .chain(
                living
                    .combat
                    .equipment
                    .modifiers
                    .iter()
                    .map(|modifier| Self::Attribute(modifier.attribute)),
            )
            .chain(living.permanent_attribute_modifiers.keys().copied().flat_map(|id| [Self::PermanentAttributeModifiers(id), Self::Attribute(id)]))
            .collect()
    }

    pub fn read(self, living: &ActorLivingState) -> LivingPropertyValue {
        use LivingPropertyValue as V;
        let state = &living.combat;
        let gear = &state.equipment;
        match self {
            Self::DeathInventoryProcessed => V::Boolean(state.timing.death_inventory_processed),
            Self::Environment => V::Environment(living.environment),
            Self::ArmorStand(property) => {
                V::ArmorStand(living.stand.as_ref().and_then(|stand| property.read(stand)))
            }
            Self::MobContinuation => {
                V::MobContinuation(living.mob.as_ref().map(|mob| mob.brain.clone()))
            }
            Self::MobSpecies => V::MobSpecies(living.mob.as_ref().map(|mob| mob.species.clone())),
            Self::MobSpeciesProperty(property) => V::MobSpeciesProperty(
                living
                    .mob
                    .as_ref()
                    .and_then(|mob| property.read(&mob.species)),
            ),
            Self::MobReproduction => {
                V::MobReproduction(living.mob.as_ref().map(|mob| mob.reproduction.clone()))
            }
            Self::GameMode => V::Unsigned(
                living
                    .avatar
                    .as_ref()
                    .map_or(0, |avatar| u32::from(avatar.game_mode)),
            ),
            Self::PlayerStance => V::PlayerStance(
                living
                    .avatar
                    .as_ref()
                    .map_or_else(Default::default, |avatar| avatar.stance),
            ),
            Self::FallDistance => V::Unsigned(
                living
                    .avatar
                    .as_ref()
                    .map_or(0, |avatar| avatar.fall_distance),
            ),
            Self::SimulationTick => V::OptionalTick(living.simulation_tick),
            Self::Experience => V::Experience(
                living
                    .avatar
                    .as_ref()
                    .map_or(Default::default(), |avatar| avatar.experience),
            ),
            Self::MerchantOffers => V::MerchantOffers(living.merchant.clone()),
            Self::MenuSession => V::MenuSession(living.menu.clone()),
            Self::Advancements => V::Advancements(living.advancements.clone()),
            Self::ItemUse => V::ItemUse(living.use_state.clone()),
            Self::Food => V::Food(living.food),
            Self::Cooldowns => V::Cooldowns(living.cooldowns.clone()),
            Self::StatusEffects => V::StatusEffects(crate::active_effects::ActiveEffectSet {
                visible: living.effects.clone(),
                hidden: living.hidden_effects.clone(),
            }),
            Self::Statistic { category, key } => V::Unsigned(
                living
                    .avatar
                    .as_ref()
                    .and_then(|avatar| avatar.statistics.get(&(category, key)))
                    .copied()
                    .unwrap_or(0),
            ),
            Self::Health => V::Unsigned(state.health_milli),
            Self::Absorption => V::Unsigned(state.absorption_milli),
            Self::VelocityX => V::Signed(state.velocity_micro[0]),
            Self::VelocityY => V::Signed(state.velocity_micro[1]),
            Self::VelocityZ => V::Signed(state.velocity_micro[2]),
            Self::LastDamage => V::Unsigned(state.timing.last_damage_milli),
            Self::LastDamageKind => V::DamageKind(state.timing.last_damage_kind),
            Self::HurtStarted => V::OptionalTick(state.timing.last_damage_tick),
            Self::HurtTicks => V::Unsigned(u32::from(state.timing.hurt_ticks)),
            Self::AttackStarted => V::OptionalTick(state.timing.last_attack_tick),
            Self::AttackTicks => V::Unsigned(u32::from(state.timing.attack_ticks)),
            Self::FireTicks => V::Unsigned(state.timing.fire_ticks),
            Self::DeathTicks => V::Unsigned(u32::from(state.timing.death_ticks)),
            Self::DeathCause => V::ActionReference(state.timing.death_cause),
            Self::LastAttacker => V::Actor(state.timing.last_attacker),
            Self::PermanentAttributeModifiers(id) => V::PermanentAttributeModifiers(living.permanent_attribute_modifiers.get(&id).cloned().unwrap_or_default()),
            Self::Attribute(id) => V::Attribute {
                base: state.effective_attributes.get(&id).copied(),
                modifiers: living.attribute_modifiers(id),
                transient_modifiers: gear.modifiers.iter().filter(|modifier| modifier.attribute == id).cloned().collect(),
            },
            Self::Weapon => V::Weapon(WeaponFacts {
                sharpness: gear.sharpness,
                smite: gear.smite,
                bane_of_arthropods: gear.bane_of_arthropods,
                knockback: gear.knockback,
                fire_aspect: gear.fire_aspect,
                breach: gear.breach,
                sweeping_edge: gear.sweeping_edge,
                sword: gear.sword,
                axe: gear.axe,
                mace: gear.mace,
                unarmed: gear.unarmed,
            }),
            Self::ArmorProtection => V::ArmorProtection(ArmorProtectionFacts {
                respiration: gear.respiration,
                freeze_immune: gear.freeze_immune,
                protection: gear.protection,
                fire: gear.fire_protection,
                explosion: gear.blast_protection,
                projectile: gear.projectile_protection,
                fall: gear.feather_falling,
            }),
            Self::Blocking => V::Boolean(gear.shield),
            Self::Strength => V::Amplifier(state.status.strength),
            Self::Weakness => V::Amplifier(state.status.weakness),
            Self::Resistance => V::Amplifier(state.status.resistance),
            Self::FireResistance => V::Boolean(state.status.fire_resistance),
            Self::Blindness => V::Boolean(state.status.blindness),
            Self::Invulnerable => V::Boolean(state.status.invulnerable),
        }
    }

    pub fn hydrate(self, living: &mut ActorLivingState, value: &LivingPropertyValue) -> bool {
        if let LivingPropertyValue::Attribute { base, modifiers, .. } = value {
            return self.write(living, &LivingPropertyValue::Attribute { base: *base,
                modifiers: modifiers.clone(), transient_modifiers: modifiers.clone() });
        }
        self.write(living, value)
    }

    pub fn write(self, living: &mut ActorLivingState, value: &LivingPropertyValue) -> bool {
        if !self.accepts(value, living.combat.actor) {
            return false;
        }
        use LivingPropertyValue as V;
        let state = &mut living.combat;
        match (self, value) {
            (Self::ArmorStand(property), V::ArmorStand(Some(value))) => {
                let Some(stand) = living.stand.as_mut() else {
                    return false;
                };
                if !property.write(stand, value) {
                    return false;
                }
            }
            (Self::MobContinuation, V::MobContinuation(value)) => {
                match (living.mob.as_mut(), value) {
                    (Some(mob), Some(brain)) => mob.brain = brain.clone(),
                    (None, None) => {}
                    _ => return false,
                }
            }
            (Self::MobSpecies, V::MobSpecies(value)) => match (living.mob.as_mut(), value) {
                (Some(mob), Some(species)) => mob.species = species.clone(),
                (None, None) => {}
                _ => return false,
            },
            (Self::MobSpeciesProperty(property), V::MobSpeciesProperty(Some(value))) => {
                let Some(mob) = living.mob.as_mut() else {
                    return false;
                };
                if !property.write(&mut mob.species, value) {
                    return false;
                }
            }
            (Self::MobReproduction, V::MobReproduction(value)) => {
                match (living.mob.as_mut(), value) {
                    (Some(mob), Some(reproduction)) => mob.reproduction = reproduction.clone(),
                    (None, None) => {}
                    _ => return false,
                }
            }
            (Self::Experience, V::Experience(value)) if value.is_valid() => {
                let Some(avatar) = living.avatar.as_mut() else {
                    return false;
                };
                avatar.experience = *value;
            }
            (Self::MerchantOffers, V::MerchantOffers(value)) => {
                if !matches!(state.actor, ActorId::Entity(_))
                    || value.as_ref().is_some_and(|offers| !offers.valid()) {
                    return false;
                }
                living.merchant = value.clone();
            }
            (Self::MenuSession, V::MenuSession(value)) => {
                if !matches!(state.actor, ActorId::Player(_)) || !value.belongs_to(state.actor) { return false; }
                living.menu = value.clone();
            }
            (Self::Advancements, V::Advancements(value)) if value.valid() && living.avatar.is_some() => living.advancements = value.clone(),
            (Self::ItemUse, V::ItemUse(value)) => {
                if value.as_ref().is_some_and(|active| {
                    active.actor != state.actor
                        || active.item.is_empty()
                        || active.blocking_delay_ticks > 5
                }) {
                    return false;
                }
                living.use_state = value.clone();
            }
            (Self::Food, V::Food(value)) => {
                if value.actor != state.actor
                    || value.food > 20
                    || value.saturation_milli > u32::from(value.food) * 1000
                {
                    return false;
                }
                living.food = *value;
            }
            (Self::StatusEffects, V::StatusEffects(value)) => {
                if !value.valid() {
                    return false;
                }
                living.effects = value.visible.clone();
                living.hidden_effects = value.hidden.clone();
            }
            (Self::Cooldowns, V::Cooldowns(value)) => {
                if value.len() > 256
                    || value.iter().any(|(group, cooldown)| {
                        group.is_empty()
                            || group.len() > 256
                            || *group != cooldown.group
                            || cooldown.actor != state.actor
                            || cooldown.revision == 0
                    })
                {
                    return false;
                }
                living.cooldowns = value.clone();
            }
            (Self::Statistic { category, key }, V::Unsigned(value))
                if *value <= i32::MAX as u32 =>
            {
                let Some(avatar) = living.avatar.as_mut() else {
                    return false;
                };
                avatar.statistics.insert((category, key), *value);
            }
            (Self::Health, V::Unsigned(value)) => state.health_milli = *value,
            (Self::Absorption, V::Unsigned(value)) => state.absorption_milli = *value,
            (Self::VelocityX, V::Signed(value)) => state.velocity_micro[0] = *value,
            (Self::VelocityY, V::Signed(value)) => state.velocity_micro[1] = *value,
            (Self::VelocityZ, V::Signed(value)) => state.velocity_micro[2] = *value,
            (Self::LastDamage, V::Unsigned(value)) => state.timing.last_damage_milli = *value,
            (Self::LastDamageKind, V::DamageKind(value)) => state.timing.last_damage_kind = *value,
            (Self::HurtStarted, V::OptionalTick(value)) => state.timing.last_damage_tick = *value,
            (Self::AttackStarted, V::OptionalTick(value)) => state.timing.last_attack_tick = *value,
            (Self::HurtTicks, V::Unsigned(value)) if *value <= u32::from(u16::MAX) => {
                state.timing.hurt_ticks = *value as u16
            }
            (Self::AttackTicks, V::Unsigned(value)) if *value <= u32::from(u16::MAX) => {
                state.timing.attack_ticks = *value as u16
            }
            (Self::DeathCause, V::ActionReference(value)) => state.timing.death_cause = *value,
            (Self::DeathInventoryProcessed, V::Boolean(value)) => state.timing.death_inventory_processed = *value,
            (Self::DeathTicks, V::Unsigned(value)) if *value <= u32::from(u16::MAX) => {
                state.timing.death_ticks = *value as u16
            }
            (Self::FireTicks, V::Unsigned(value)) => state.timing.fire_ticks = *value,
            (Self::SimulationTick, V::OptionalTick(value)) => living.simulation_tick = *value,
            (Self::Environment, V::Environment(value)) if value.valid() => living.environment = *value,
            (Self::GameMode, V::Unsigned(value)) => {
                let Some(avatar) = living.avatar.as_mut() else {
                    return false;
                };
                avatar.game_mode = *value as u8;
            }
            (Self::PlayerStance, V::PlayerStance(value)) => {
                let Some(avatar) = living.avatar.as_mut() else {
                    return false;
                };
                avatar.stance = *value;
            }
            (Self::FallDistance, V::Unsigned(value)) => {
                let Some(avatar) = living.avatar.as_mut() else {
                    return false;
                };
                if !f32::from_bits(*value).is_finite() || f32::from_bits(*value) < 0.0 {
                    return false;
                }
                avatar.fall_distance = *value;
            }
            (Self::LastAttacker, V::Actor(value)) => state.timing.last_attacker = *value,
            (Self::PermanentAttributeModifiers(id), V::PermanentAttributeModifiers(modifiers)) => {
                if !self.accepts(value, state.actor) { return false; }
                if modifiers.is_empty() { living.permanent_attribute_modifiers.remove(&id); }
                else { living.permanent_attribute_modifiers.insert(id, modifiers.clone()); }
            }
            (Self::Attribute(id), V::Attribute { base, modifiers, transient_modifiers }) => {
                if base.is_some_and(|value| !f64::from_bits(value).is_finite())
                    || modifiers.len() > 256 || transient_modifiers.len() > 256
                    || modifiers.iter().chain(transient_modifiers).any(|modifier| {
                        modifier.attribute != id
                            || modifier.slot > 7
                            || modifier.id.len() > 256
                            || !f64::from_bits(modifier.amount).is_finite()
                    })
                {
                    return false;
                }
                if state
                    .equipment
                    .modifiers
                    .iter()
                    .filter(|modifier| modifier.attribute != id)
                    .count()
                    + transient_modifiers.len()
                    > 256
                {
                    return false;
                }
                match base {
                    Some(value) => {
                        state.effective_attributes.insert(id, *value);
                    }
                    None => {
                        state.effective_attributes.remove(&id);
                    }
                }
                state
                    .equipment
                    .modifiers
                    .retain(|modifier| modifier.attribute != id);
                state.equipment.modifiers.extend(transient_modifiers.iter().cloned());
                state.equipment.modifiers.sort_by(|a, b| {
                    (a.attribute, &a.id, a.slot).cmp(&(b.attribute, &b.id, b.slot))
                });
            }
            (Self::Weapon, V::Weapon(value)) => {
                let gear = &mut state.equipment;
                gear.sharpness = value.sharpness;
                gear.smite = value.smite;
                gear.bane_of_arthropods = value.bane_of_arthropods;
                gear.knockback = value.knockback;
                gear.fire_aspect = value.fire_aspect;
                gear.breach = value.breach;
                gear.sweeping_edge = value.sweeping_edge;
                gear.sword = value.sword;
                gear.axe = value.axe;
                gear.mace = value.mace;
                gear.unarmed = value.unarmed;
            }
            (Self::ArmorProtection, V::ArmorProtection(value)) => {
                let gear = &mut state.equipment;
                gear.respiration = value.respiration;
                gear.freeze_immune = value.freeze_immune;
                gear.protection = value.protection;
                gear.fire_protection = value.fire;
                gear.blast_protection = value.explosion;
                gear.projectile_protection = value.projectile;
                gear.feather_falling = value.fall;
            }
            (Self::Blocking, V::Boolean(value)) => state.equipment.shield = *value,
            (Self::Strength, V::Amplifier(value)) => state.status.strength = *value,
            (Self::Weakness, V::Amplifier(value)) => state.status.weakness = *value,
            (Self::Resistance, V::Amplifier(value)) => state.status.resistance = *value,
            (Self::FireResistance, V::Boolean(value)) => state.status.fire_resistance = *value,
            (Self::Blindness, V::Boolean(value)) => state.status.blindness = *value,
            (Self::Invulnerable, V::Boolean(value)) => state.status.invulnerable = *value,
            _ => return false,
        }
        true
    }
}
