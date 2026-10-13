use std::borrow::Cow;
use std::collections::{BTreeMap, BTreeSet};

use crate::active_effects::ActiveEffectSet;
use crate::canonical_action::{ActionState, ApplyFailure};
use crate::combat_action::{
    CombatAttributeModifier, CombatEquipmentFacts, DamageIntent, DamageKind, DamageProtection,
    DamageReductionFacts, LivingTickIntent,
};
use crate::item_action::{ActorCombatState, ActorFoodState, ActorLivingState};
use crate::mob_continuation::MobReproduction;
use crate::time::TickStamp;

pub struct NumericLivingView<'a> {
    living: &'a ActorLivingState,
}

impl<'a> NumericLivingView<'a> {
    pub fn new(living: &'a ActorLivingState) -> Self {
        Self { living }
    }
}

struct NumericAttribute<'a> {
    combined: Cow<'a, [CombatAttributeModifier]>,
    transient: Cow<'a, [CombatAttributeModifier]>,
}

pub struct NumericLivingDraft<'a> {
    pub(crate) combat: ActorCombatState,
    food: ActorFoodState,
    effects: ActiveEffectSet,
    simulation_tick: Option<TickStamp>,
    reproduction: Option<MobReproduction>,
    game_mode: Option<u8>,
    clear_item_use: bool,
    original_attributes: &'a BTreeMap<u16, u64>,
    attribute_bases: BTreeMap<u16, Option<u64>>,
    original_modifiers: &'a [CombatAttributeModifier],
    original_permanent: &'a BTreeMap<u16, Vec<CombatAttributeModifier>>,
    permanent: BTreeMap<u16, &'a [CombatAttributeModifier]>,
    attributes: BTreeMap<u16, NumericAttribute<'a>>,
    changed_attributes: BTreeSet<u16>,
}

pub struct NumericLivingResult {
    combat: ActorCombatState,
    food: ActorFoodState,
    effects: ActiveEffectSet,
    simulation_tick: Option<TickStamp>,
    reproduction: Option<MobReproduction>,
    clear_item_use: bool,
    modifiers: Option<Vec<CombatAttributeModifier>>,
}

fn protection(facts: &CombatEquipmentFacts) -> CombatEquipmentFacts {
    CombatEquipmentFacts {
        modifiers: Vec::new(),
        respiration: facts.respiration,
        freeze_immune: facts.freeze_immune,
        sharpness: facts.sharpness,
        smite: facts.smite,
        bane_of_arthropods: facts.bane_of_arthropods,
        knockback: facts.knockback,
        fire_aspect: facts.fire_aspect,
        breach: facts.breach,
        sweeping_edge: facts.sweeping_edge,
        protection: facts.protection,
        fire_protection: facts.fire_protection,
        blast_protection: facts.blast_protection,
        projectile_protection: facts.projectile_protection,
        feather_falling: facts.feather_falling,
        sword: facts.sword,
        axe: facts.axe,
        mace: facts.mace,
        shield: facts.shield,
        unarmed: facts.unarmed,
    }
}

use crate::combat_action::periodic_tick_count;

impl<'a> NumericLivingDraft<'a> {
    pub fn new(view: NumericLivingView<'a>) -> Self {
        let living = view.living;
        let combat = &living.combat;
        Self {
            combat: ActorCombatState {
                actor: combat.actor,
                revision: combat.revision,
                health_milli: combat.health_milli,
                absorption_milli: combat.absorption_milli,
                velocity_micro: combat.velocity_micro,
                timing: combat.timing,
                effective_attributes: BTreeMap::new(),
                equipment: protection(&combat.equipment),
                status: combat.status.clone(),
            },
            food: living.food,
            effects: ActiveEffectSet {
                visible: living.effects.clone(),
                hidden: living.hidden_effects.clone(),
            },
            simulation_tick: living.simulation_tick,
            reproduction: living.mob.as_ref().map(|mob| mob.reproduction.clone()),
            game_mode: living.avatar.as_ref().map(|avatar| avatar.game_mode),
            clear_item_use: false,
            original_attributes: &combat.effective_attributes,
            attribute_bases: BTreeMap::new(),
            original_modifiers: &combat.equipment.modifiers,
            original_permanent: &living.permanent_attribute_modifiers,
            permanent: BTreeMap::new(),
            attributes: BTreeMap::new(),
            changed_attributes: BTreeSet::new(),
        }
    }

    fn permanent(&self, id: u16) -> &[CombatAttributeModifier] {
        self.permanent
            .get(&id)
            .copied()
            .unwrap_or_else(|| self.original_permanent.get(&id).map_or(&[], Vec::as_slice))
    }

    fn base(&self, id: u16) -> Option<u64> {
        self.attribute_bases
            .get(&id)
            .copied()
            .unwrap_or_else(|| self.original_attributes.get(&id).copied())
    }

    fn attribute(&self, id: u16, base: f64) -> f64 {
        let modifiers = self
            .attributes
            .get(&id)
            .map_or(self.original_modifiers, |attribute| {
                attribute.combined.as_ref()
            });
        CombatEquipmentFacts::attribute_from_sources(id, base, modifiers, self.permanent(id))
    }

    pub fn advance(
        &mut self,
        intent: &LivingTickIntent,
        context: Option<&dyn ActionState>,
    ) -> Result<(u32, u32, bool), ApplyFailure> {
        if self.simulation_tick != intent.previous_tick {
            return Err(ApplyFailure::Condition(intent.identity.target()));
        }
        let elapsed = intent.previous_tick.map_or(1, |previous| {
            intent.identity.simulation_tick.distance_since(previous)
        });
        if elapsed == 0 || elapsed >= 0x8000 {
            return Err(ApplyFailure::InvalidAction);
        }
        let mut total = (0_u32, 0_u32, false);
        let mut step = intent.clone();
        for offset in (0..elapsed).rev() {
            step.identity.simulation_tick =
                TickStamp(intent.identity.simulation_tick.0.wrapping_sub(offset));
            step.previous_tick = self.simulation_tick;
            let change = self.advance_one(&step, context)?;
            total.0 = total.0.saturating_add(change.0);
            total.1 = total.1.saturating_add(change.1);
            total.2 |= change.2;
        }
        Ok(total)
    }

    fn refresh_status(
        &mut self,
        context: &dyn ActionState,
        old: &[CombatAttributeModifier],
        max_health: u16,
    ) -> Result<(), ApplyFailure> {
        let facts = context.status_effect_facts(&self.effects)?;
        self.combat.status.strength = facts.strength;
        self.combat.status.weakness = facts.weakness;
        self.combat.status.resistance = facts.resistance;
        self.combat.status.fire_resistance = facts.fire_resistance;
        self.combat.status.blindness = facts.blindness;
        let next = context.status_effect_modifiers(&self.effects)?;
        let removed_boost = old
            .iter()
            .find(|modifier| {
                modifier.attribute == max_health && modifier.id == "minecraft:effect.health_boost"
            })
            .is_some_and(|old| {
                next.iter()
                    .find(|modifier| modifier.attribute == max_health && modifier.id == old.id)
                    .is_none_or(|next| next != old)
            });
        let attributes = old
            .iter()
            .chain(&next)
            .map(|modifier| modifier.attribute)
            .collect::<BTreeSet<_>>();
        for id in attributes {
            if old
                .iter()
                .filter(|modifier| modifier.attribute == id)
                .eq(next.iter().filter(|modifier| modifier.attribute == id))
            {
                continue;
            }
            let attribute = self
                .attributes
                .entry(id)
                .or_insert_with(|| NumericAttribute {
                    combined: Cow::Borrowed(self.original_modifiers),
                    transient: Cow::Borrowed(self.original_modifiers),
                });
            for source in [&mut attribute.combined, &mut attribute.transient] {
                let mut modifiers = source
                    .iter()
                    .filter(|modifier| {
                        modifier.attribute == id
                            && !old.iter().any(|old| {
                                old.attribute == id
                                    && old.slot == modifier.slot
                                    && old.id == modifier.id
                            })
                    })
                    .cloned()
                    .collect::<Vec<_>>();
                for modifier in next.iter().filter(|modifier| modifier.attribute == id) {
                    modifiers.retain(|old| old.id != modifier.id || old.slot != modifier.slot);
                    modifiers.push(modifier.clone());
                }
                modifiers.sort_by(|a, b| {
                    (a.attribute, a.slot, a.id.as_str()).cmp(&(b.attribute, b.slot, b.id.as_str()))
                });
                *source = Cow::Owned(modifiers);
            }
            self.changed_attributes.insert(id);
        }
        if removed_boost {
            let base = self
                .base(max_health)
                .map(f64::from_bits)
                .ok_or(ApplyFailure::InvalidAction)?;
            let maximum = self.attribute(max_health, base);
            if !maximum.is_finite() || maximum > f64::from(u32::MAX) / 1000.0 {
                return Err(ApplyFailure::InvalidAction);
            }
            self.combat.health_milli = self
                .combat
                .health_milli
                .min((maximum.max(0.0) * 1000.0).round() as u32);
        }
        Ok(())
    }

    pub fn finish(self) -> NumericLivingResult {
        let modifiers = (!self.changed_attributes.is_empty()).then(|| {
            let mut modifiers = self
                .original_modifiers
                .iter()
                .filter(|modifier| !self.changed_attributes.contains(&modifier.attribute))
                .cloned()
                .collect::<Vec<_>>();
            for id in &self.changed_attributes {
                modifiers.extend(self.attributes[id].transient.iter().cloned());
            }
            modifiers.sort_by(|a, b| {
                (a.attribute, a.slot, a.id.as_str()).cmp(&(b.attribute, b.slot, b.id.as_str()))
            });
            modifiers
        });
        NumericLivingResult {
            combat: self.combat,
            food: self.food,
            effects: self.effects,
            simulation_tick: self.simulation_tick,
            reproduction: self.reproduction,
            clear_item_use: self.clear_item_use,
            modifiers,
        }
    }
    fn advance_one(
        &mut self,
        intent: &LivingTickIntent,
        context: Option<&dyn ActionState>,
    ) -> Result<(u32, u32, bool), ApplyFailure> {
        let target = intent.identity.target();
        let old_modifiers = context
            .map(|context| context.status_effect_modifiers(&self.effects))
            .transpose()?;
        let previous_food = self.food;
        let mut damage = (0_u32, 0_u32);
        if self.simulation_tick != intent.previous_tick {
            return Err(ApplyFailure::Condition(target));
        }
        let elapsed = intent.previous_tick.map_or(1, |previous| {
            intent.identity.simulation_tick.distance_since(previous)
        });
        if elapsed == 0 || elapsed >= 0x8000 {
            return Err(ApplyFailure::InvalidAction);
        }
        if intent.effective_position.is_some_and(|position| {
            f64::from_bits(position[1]) < f64::from(intent.rules.minimum_y) - 64.0
        }) && periodic_tick_count(intent.identity.simulation_tick, elapsed, 10) != 0
        {
            let change = DamageIntent {
                kind: DamageKind::OutOfWorld,
                source: intent.identity.actor(),
                target,
                tick: intent.identity.simulation_tick,
                damage_milli: 4000
                    * periodic_tick_count(intent.identity.simulation_tick, elapsed, 10),
                knockback_micro: [0; 3],
                bypass_hurt_cooldown: true,
                fire_ticks: 0,
                bypass_invulnerability: true,
                reduction: None,
            }
            .apply_numeric(self)?;
            damage.0 = damage.0.saturating_add(change.0);
            damage.1 = damage.1.saturating_add(change.1);
        }
        if let Some(reproduction) = self.reproduction.as_mut() {
            *reproduction = reproduction.advance(u32::from(elapsed));
        }
        let timing = &mut self.combat.timing;
        timing.hurt_ticks = timing.hurt_ticks.saturating_sub(elapsed);
        timing.attack_ticks = timing.attack_ticks.saturating_add(elapsed);
        if self.combat.health_milli == 0 {
            timing.death_ticks = timing.death_ticks.saturating_add(elapsed);
        }
        let fire_damage =
            timing.fire_ticks / 20 - timing.fire_ticks.saturating_sub(u32::from(elapsed)) / 20;
        timing.fire_ticks = timing.fire_ticks.saturating_sub(u32::from(elapsed));
        if fire_damage != 0
            && intent.rules.fire_damage
            && !intent.fire_immune
            && !self.combat.status.fire_resistance
        {
            let change = DamageIntent {
                kind: DamageKind::OnFire,
                source: self
                    .combat
                    .timing
                    .last_attacker
                    .unwrap_or(intent.identity.actor()),
                target,
                tick: intent.identity.simulation_tick,
                damage_milli: fire_damage.saturating_mul(1000),
                knockback_micro: [0; 3],
                bypass_hurt_cooldown: false,
                fire_ticks: 0,
                bypass_invulnerability: false,
                reduction: Some(DamageReductionFacts {
                    armor_attribute: intent.armor_attribute,
                    toughness_attribute: intent.toughness_attribute,
                    bypass_armor: true,
                    bypass_effects: false,
                    bypass_enchantments: false,
                    protection: DamageProtection::Fire,
                    breach: 0,
                }),
            }
            .apply_numeric(self)?;
            damage.0 = damage.0.saturating_add(change.0);
            damage.1 = damage.1.saturating_add(change.1);
        }
        for index in 0..self.effects.visible.len() {
            let effect = self.effects.visible[index].clone();
            let duration = if effect.duration < 0 {
                u32::from(elapsed)
            } else {
                u32::from(elapsed).min(effect.duration as u32)
            };
            if duration == 0 {
                continue;
            }
            if effect.kind == intent.rules.hunger_effect
                && self.game_mode.is_some_and(|mode| mode == 0 || mode == 2)
            {
                self.food.exhaustion_milli = self
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
            let base: u32 = if effect.kind == intent.rules.regeneration_effect {
                50
            } else if effect.kind == intent.rules.poison_effect {
                25
            } else if effect.kind == intent.rules.wither_effect {
                40
            } else {
                continue;
            };
            let period = (base >> u32::from(effect.amplifier).min(31)).max(1);
            let count = if effect.duration < 0 {
                periodic_tick_count(
                    intent.identity.simulation_tick,
                    duration as u16,
                    period as u16,
                )
            } else {
                effect.duration as u32 / period - (effect.duration as u32 - duration) / period
            };
            if count == 0 || self.combat.health_milli == 0 {
                continue;
            }
            let amount = count.saturating_mul(1000);
            if effect.kind == intent.rules.regeneration_effect {
                let base = self
                    .base(intent.rules.max_health_attribute)
                    .map(f64::from_bits)
                    .ok_or(ApplyFailure::InvalidAction)?;
                let maximum = self.attribute(intent.rules.max_health_attribute, base);
                if !maximum.is_finite() || maximum < 0.0 || maximum > f64::from(u32::MAX) / 1000.0 {
                    return Err(ApplyFailure::InvalidAction);
                }
                self.combat.health_milli = self
                    .combat
                    .health_milli
                    .saturating_add(amount)
                    .min((maximum * 1000.0).round() as u32);
            } else {
                let amount = if effect.kind == intent.rules.poison_effect {
                    if self.combat.health_milli <= 1000 {
                        continue;
                    }
                    amount.min(
                        self.combat
                            .health_milli
                            .saturating_sub(1000)
                            .saturating_add(self.combat.absorption_milli),
                    )
                } else {
                    amount
                };
                let change = DamageIntent {
                    kind: if effect.kind == intent.rules.poison_effect {
                        DamageKind::Magic
                    } else {
                        DamageKind::Wither
                    },
                    source: self
                        .combat
                        .timing
                        .last_attacker
                        .unwrap_or(intent.identity.actor()),
                    target,
                    tick: intent.identity.simulation_tick,
                    damage_milli: amount,
                    knockback_micro: [0; 3],
                    bypass_hurt_cooldown: false,
                    fire_ticks: 0,
                    bypass_invulnerability: false,
                    reduction: Some(DamageReductionFacts {
                        armor_attribute: intent.armor_attribute,
                        toughness_attribute: intent.toughness_attribute,
                        bypass_armor: true,
                        bypass_effects: false,
                        bypass_enchantments: true,
                        protection: DamageProtection::General,
                        breach: 0,
                    }),
                }
                .apply_numeric(self)?;
                damage.0 = damage.0.saturating_add(change.0);
                damage.1 = damage.1.saturating_add(change.1);
            }
        }
        self.effects.advance(elapsed);
        if self.game_mode.is_some_and(|mode| mode == 0 || mode == 2) {
            let base = self
                .base(intent.rules.max_health_attribute)
                .map(f64::from_bits)
                .ok_or(ApplyFailure::InvalidAction)?;
            let maximum = self.attribute(intent.rules.max_health_attribute, base);
            if !maximum.is_finite() || maximum < 0.0 || maximum > f64::from(u32::MAX) / 1000.0 {
                return Err(ApplyFailure::InvalidAction);
            }
            let maximum = (maximum * 1000.0).round() as u32;
            for _ in 0..elapsed {
                let food = &mut self.food;
                if food.exhaustion_milli > 4000 {
                    food.exhaustion_milli -= 4000;
                    if food.saturation_milli > 0 {
                        food.saturation_milli = food.saturation_milli.saturating_sub(1000);
                    } else if intent.rules.difficulty != 0 {
                        food.food = food.food.saturating_sub(1);
                    }
                }
                if intent.rules.natural_regeneration
                    && self.combat.health_milli != 0
                    && self.combat.health_milli < maximum
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
                        self.combat.health_milli = self
                            .combat
                            .health_milli
                            .saturating_add(healing)
                            .min(maximum);
                        food.exhaustion_milli =
                            food.exhaustion_milli.saturating_add(cost).min(40_000);
                        food.tick_timer = 0;
                    }
                } else if food.food == 0 && self.combat.health_milli != 0 {
                    food.tick_timer += 1;
                    if food.tick_timer >= 80 {
                        food.tick_timer = 0;
                        if self.combat.health_milli > 10_000
                            || intent.rules.difficulty == 3
                            || intent.rules.difficulty == 2 && self.combat.health_milli > 1000
                        {
                            let change = DamageIntent {
                                kind: DamageKind::Starvation,
                                source: intent.identity.actor(),
                                target,
                                tick: intent.identity.simulation_tick,
                                damage_milli: 1000,
                                knockback_micro: [0; 3],
                                bypass_hurt_cooldown: false,
                                fire_ticks: 0,
                                reduction: None,
                                bypass_invulnerability: false,
                            }
                            .apply_numeric(self)?;
                            damage.0 = damage.0.saturating_add(change.0);
                            damage.1 = damage.1.saturating_add(change.1);
                        }
                    }
                } else {
                    food.tick_timer = 0;
                }
            }
        }
        if self.food != previous_food {
            self.food.revision = previous_food
                .revision
                .checked_add(1)
                .ok_or(ApplyFailure::Overflow(target))?;
        }
        self.simulation_tick = Some(intent.identity.simulation_tick);
        let damage_killed = damage.0 != 0 && self.combat.health_milli == 0;
        if let (Some(context), Some(old_modifiers)) = (context, old_modifiers) {
            self.refresh_status(context, &old_modifiers, intent.rules.max_health_attribute)?;
        }
        Ok((damage.0, damage.1, damage_killed))
    }
}

impl NumericLivingResult {
    pub fn apply_to(self, living: &mut ActorLivingState) {
        living.combat.revision = self.combat.revision;
        living.combat.health_milli = self.combat.health_milli;
        living.combat.absorption_milli = self.combat.absorption_milli;
        living.combat.velocity_micro = self.combat.velocity_micro;
        living.combat.timing = self.combat.timing;
        living.combat.status = self.combat.status;
        if let Some(modifiers) = self.modifiers {
            living.combat.equipment.modifiers = modifiers;
        }
        living.food = self.food;
        living.effects = self.effects.visible;
        living.hidden_effects = self.effects.hidden;
        living.simulation_tick = self.simulation_tick;
        if let (Some(mob), Some(reproduction)) = (living.mob.as_mut(), self.reproduction) {
            mob.reproduction = reproduction;
        }
        if self.clear_item_use {
            living.use_state = None;
        }
    }
}

impl DamageIntent {
    fn apply_numeric(
        &self,
        draft: &mut NumericLivingDraft<'_>,
    ) -> Result<(u32, u32), ApplyFailure> {
        let health = draft.combat.health_milli;
        let absorption = draft.combat.absorption_milli;
        let Some((damage, recent)) = self.pending_damage(&draft.combat, draft.game_mode) else {
            return Ok((0, 0));
        };
        let damage = self.reduction.map_or(Ok(damage), |reduction| {
            reduction.reduce_parts(
                damage,
                &draft.combat.status,
                &draft.combat.equipment,
                |id| {
                    let base = draft
                        .base(id)
                        .map(f64::from_bits)
                        .ok_or(ApplyFailure::InvalidAction)?;
                    let value = draft.attribute(id, base);
                    value
                        .is_finite()
                        .then_some(value as f32)
                        .ok_or(ApplyFailure::InvalidAction)
                },
            )
        })?;
        if self.apply_resolved_damage(&mut draft.combat, damage, recent)?
            && draft.combat.health_milli == 0
        {
            draft.clear_item_use = true;
        }
        Ok((
            health.saturating_sub(draft.combat.health_milli),
            absorption.saturating_sub(draft.combat.absorption_milli),
        ))
    }
}

impl<'a> NumericLivingDraft<'a> {
    pub fn hydrate(
        &mut self,
        property: crate::living_property::LivingProperty,
        value: &'a crate::living_property::LivingPropertyValue,
    ) -> bool {
        use crate::living_property::{LivingProperty as P, LivingPropertyValue as V};
        if !property.accepts(value, self.combat.actor) {
            return false;
        }
        match (property, value) {
            (P::PermanentAttributeModifiers(id), V::PermanentAttributeModifiers(modifiers)) => {
                self.permanent.insert(id, modifiers);
            }
            (
                P::Attribute(id),
                V::Attribute {
                    base,
                    modifiers,
                    transient_modifiers,
                },
            ) => {
                if *base != self.original_attributes.get(&id).copied() {
                    self.attribute_bases.insert(id, *base);
                } else {
                    self.attribute_bases.remove(&id);
                }
                self.attributes.insert(
                    id,
                    NumericAttribute {
                        combined: Cow::Borrowed(modifiers),
                        transient: Cow::Borrowed(transient_modifiers),
                    },
                );
            }
            (P::Health, V::Unsigned(value)) => self.combat.health_milli = *value,
            (P::Absorption, V::Unsigned(value)) => self.combat.absorption_milli = *value,
            (P::VelocityX, V::Signed(value)) => self.combat.velocity_micro[0] = *value,
            (P::VelocityY, V::Signed(value)) => self.combat.velocity_micro[1] = *value,
            (P::VelocityZ, V::Signed(value)) => self.combat.velocity_micro[2] = *value,
            (P::LastDamage, V::Unsigned(value)) => self.combat.timing.last_damage_milli = *value,
            (P::LastDamageKind, V::DamageKind(value)) => {
                self.combat.timing.last_damage_kind = *value
            }
            (P::HurtStarted, V::OptionalTick(value)) => {
                self.combat.timing.last_damage_tick = *value
            }
            (P::HurtTicks, V::Unsigned(value)) => self.combat.timing.hurt_ticks = *value as u16,
            (P::AttackTicks, V::Unsigned(value)) => self.combat.timing.attack_ticks = *value as u16,
            (P::FireTicks, V::Unsigned(value)) => self.combat.timing.fire_ticks = *value,
            (P::DeathTicks, V::Unsigned(value)) => self.combat.timing.death_ticks = *value as u16,
            (P::DeathCause, V::ActionReference(value)) => self.combat.timing.death_cause = *value,
            (P::LastAttacker, V::Actor(value)) => self.combat.timing.last_attacker = *value,
            (P::SimulationTick, V::OptionalTick(value)) => self.simulation_tick = *value,
            (P::Food, V::Food(value)) => self.food = *value,
            (P::StatusEffects, V::StatusEffects(value)) => self.effects = value.clone(),
            (P::Strength, V::Amplifier(value)) => self.combat.status.strength = *value,
            (P::Weakness, V::Amplifier(value)) => self.combat.status.weakness = *value,
            (P::Resistance, V::Amplifier(value)) => self.combat.status.resistance = *value,
            (P::FireResistance, V::Boolean(value)) => self.combat.status.fire_resistance = *value,
            (P::Blindness, V::Boolean(value)) => self.combat.status.blindness = *value,
            (P::Invulnerable, V::Boolean(value)) => self.combat.status.invulnerable = *value,
            (P::GameMode, V::Unsigned(value)) => self.game_mode = Some(*value as u8),
            (P::MobReproduction, V::MobReproduction(value)) => self.reproduction = value.clone(),
            (P::ItemUse, V::ItemUse(_)) => {}
            (P::ArmorProtection, V::ArmorProtection(value)) => {
                let equipment = &mut self.combat.equipment;
                equipment.respiration = value.respiration;
                equipment.freeze_immune = value.freeze_immune;
                equipment.protection = value.protection;
                equipment.fire_protection = value.fire;
                equipment.blast_protection = value.explosion;
                equipment.projectile_protection = value.projectile;
                equipment.feather_falling = value.fall;
            }
            _ => return false,
        }
        true
    }

    pub fn output(
        &self,
        property: crate::living_property::LivingProperty,
    ) -> Option<crate::living_property::LivingPropertyValue> {
        use crate::living_property::{LivingProperty as P, LivingPropertyValue as V};
        Some(match property {
            P::Health => V::Unsigned(self.combat.health_milli),
            P::Absorption => V::Unsigned(self.combat.absorption_milli),
            P::LastDamage => V::Unsigned(self.combat.timing.last_damage_milli),
            P::LastDamageKind => V::DamageKind(self.combat.timing.last_damage_kind),
            P::HurtStarted => V::OptionalTick(self.combat.timing.last_damage_tick),
            P::HurtTicks => V::Unsigned(u32::from(self.combat.timing.hurt_ticks)),
            P::AttackTicks => V::Unsigned(u32::from(self.combat.timing.attack_ticks)),
            P::FireTicks => V::Unsigned(self.combat.timing.fire_ticks),
            P::DeathTicks => V::Unsigned(u32::from(self.combat.timing.death_ticks)),
            P::DeathCause => V::ActionReference(self.combat.timing.death_cause),
            P::LastAttacker => V::Actor(self.combat.timing.last_attacker),
            P::SimulationTick => V::OptionalTick(self.simulation_tick),
            P::Food => V::Food(self.food),
            P::StatusEffects => V::StatusEffects(self.effects.clone()),
            P::Strength => V::Amplifier(self.combat.status.strength),
            P::Weakness => V::Amplifier(self.combat.status.weakness),
            P::Resistance => V::Amplifier(self.combat.status.resistance),
            P::FireResistance => V::Boolean(self.combat.status.fire_resistance),
            P::Blindness => V::Boolean(self.combat.status.blindness),
            P::MobReproduction => V::MobReproduction(self.reproduction.clone()),
            P::ItemUse if self.clear_item_use => V::ItemUse(None),
            P::Attribute(id) if self.changed_attributes.contains(&id) => {
                let transient = self
                    .attributes
                    .get(&id)
                    .map_or(self.original_modifiers, |attribute| {
                        attribute.transient.as_ref()
                    });
                let combined = transient
                    .iter()
                    .filter(|modifier| modifier.attribute == id)
                    .chain(self.permanent(id))
                    .map(|modifier| (modifier.id.as_str(), modifier))
                    .collect::<BTreeMap<_, _>>();
                V::Attribute {
                    base: self.base(id),
                    modifiers: combined.into_values().cloned().collect(),
                    transient_modifiers: transient
                        .iter()
                        .filter(|modifier| modifier.attribute == id)
                        .cloned()
                        .collect(),
                }
            }
            _ => return None,
        })
    }
}
