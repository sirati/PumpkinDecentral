#[allow(clippy::wildcard_imports)]
use super::*;
use std::sync::atomic::{AtomicU64, Ordering};
use pumpkin_cluster::combat::{capture_attack, next_combat_seq, stage_captured_attack};
use pumpkin_cluster::identity::ActionActor;
use pumpkin_cluster::protocol::{
    AttackCooldownTransition, AttackDamageType, AttackItemTransition, AttackKnockback,
    AttackVelocityTransition, CapturedAttack, CapturedAttackActorEffects, CapturedAttackOutcome,
    CapturedAttackTarget, CapturedAttackTargetOutcome, CombatAdvancement,
    CombatAdvancementTransition, CombatStatKind, CombatStatTransition, EntityMutationTarget,
    LivingAttackOutcome, LivingHurtCooldownResolution,
};
use pumpkin_cluster::time::TickStamp;
use pumpkin_data::data_component_impl::WeaponImpl;
use pumpkin_data::enchantment::Enchantment;
use pumpkin_data::item_stack::DamageResult;
use pumpkin_util::math::{boundingbox::BoundingBox, vector3::Vector3};

static UNIDENTIFIED_ENTITY_ATTACKS: AtomicU64 = AtomicU64::new(0);

impl JavaClient {
    pub fn handle_attack(&self, player: &Arc<Player>, attack: &SAttack, server: &Arc<Server>) {
        if !player.has_client_loaded() {
            return;
        }
        player.update_last_action_time();
        let entity_id = attack.entity_id;
        let player_entity = &player.get_entity();
        let world = player_entity.world.load_full();

        let config = &server.advanced_config.pvp;
        if !config.enabled {
            return;
        }

        if entity_id.0 == player.entity_id() {
            self.try_kick(&TextComponent::translate_cross(
                translation::java::MULTIPLAYER_DISCONNECT_INVALID_ENTITY_ATTACKED,
                translation::java::MULTIPLAYER_DISCONNECT_INVALID_ENTITY_ATTACKED,
                [],
            ));
            return;
        }

        let player_target = world.get_player_by_id(entity_id.0);
        let target: Option<Arc<dyn EntityBase>> = player_target
            .as_ref()
            .map(|p| Arc::clone(p) as Arc<dyn EntityBase>)
            .or_else(|| world.get_entity_by_id(entity_id.0));
        let Some(target) = target else {
            self.try_kick(&TextComponent::translate_cross(
                translation::java::MULTIPLAYER_DISCONNECT_INVALID_ENTITY_ATTACKED,
                translation::java::MULTIPLAYER_DISCONNECT_INVALID_ENTITY_ATTACKED,
                [],
            ));
            return;
        };
        if let Some(player_victim) = &player_target {
            if player_victim.living_entity.health.load() <= 0.0 {
                return;
            }
            if config.protect_creative && player_victim.gamemode.load() == GameMode::Creative {
                world.play_sound(
                    Sound::EntityPlayerAttackNodamage,
                    SoundCategory::Players,
                    &player_victim.position(),
                );
                return;
            }
        }
        attack_and_replicate(
            player,
            &target,
            player_target.is_some(),
            player_target
                .as_ref()
                .and_then(|victim| victim.cluster_gid()),
            server,
        );
    }
}

pub(crate) fn combat_tick() -> Option<TickStamp> {
    crate::server::cluster::disciplined_tick_now()
}

pub(crate) fn attack_and_replicate(
    attacker: &Player,
    victim: &Arc<dyn EntityBase>,
    _victim_is_player: bool,
    victim_gid: Option<pumpkin_cluster::identity::GlobalPlayerId>,
    server: &Arc<Server>,
) {
    if !server.advanced_config.cluster.enabled {
        attacker.attack(victim);
        return;
    }
    let Some(gid) = attacker.cluster_gid() else {
        return;
    };
    let Some(attacker_ref) = crate::server::cluster_entity_apply::entity_ref_for(attacker) else {
        let occurrence = UNIDENTIFIED_ENTITY_ATTACKS.fetch_add(1, Ordering::Relaxed) + 1;
        if occurrence == 1 || occurrence % 1024 == 0 {
            tracing::warn!(occurrence, "cluster attacker is missing its entity reference");
        }
        return;
    };
    let seq = next_combat_seq(gid);
    let Some(tick) = combat_tick() else {
        return;
    };
    let Some(victim_ref) = crate::server::cluster_entity_apply::entity_ref_for(victim.as_ref()) else {
        let occurrence = UNIDENTIFIED_ENTITY_ATTACKS.fetch_add(1, Ordering::Relaxed) + 1;
        if occurrence == 1 || occurrence % 1024 == 0 {
            tracing::warn!(occurrence, "cluster attack target is missing its entity reference");
        }
        return;
    };
    let target = match victim_gid {
        Some(gid) => EntityMutationTarget::Player {
            gid,
            chunk: victim_ref.chunk,
        },
        None => EntityMutationTarget::Entity(victim_ref),
    };
    let Some((raw_damage_milli, attack_type, held_item)) =
        attacker.capture_attack_profile(victim.as_ref())
    else {
        return;
    };
    if raw_damage_milli > pumpkin_cluster::combat::CombatLimits::default().max_damage_milli {
        return;
    }
    let fire_ticks = held_item
        .get_enchantment_level(&Enchantment::FIRE_ASPECT)
        .checked_mul(80)
        .and_then(|ticks| u32::try_from(ticks).ok())
        .unwrap_or_default();
    let Some(primary) = capture_attack_target(
        attacker,
        victim,
        target,
        raw_damage_milli,
        attack_type,
        fire_ticks,
        true,
        attacker_ref,
        tick,
        server,
    ) else {
        return;
    };
    let actor = attacker.cluster_captured_attack_actor_snapshot();
    let no_damage = matches!(&primary.outcome, CapturedAttackTargetOutcome::Living(outcome) if matches!(outcome.hurt_cooldown_resolution, LivingHurtCooldownResolution::NoDamage));
    let mut stats = Vec::new();
    if !held_item.is_empty() {
        let item = held_item.item.id;
        let before = combat_stat(&actor, CombatStatKind::Used, item);
        stats.push(CombatStatTransition {
            kind: CombatStatKind::Used,
            item,
            before,
            after: before.saturating_add(1),
        });
    }
    let mut item = None;
    if !no_damage && !matches!(attacker.gamemode.load(), GameMode::Creative | GameMode::Spectator) {
        let cost = held_item
            .get_data_component::<WeaponImpl>()
            .map_or(0, |weapon| i32::try_from(weapon.item_damage_per_attack).unwrap_or(i32::MAX));
        if cost > 0 {
            let mut after = held_item.clone();
            let damage = after.damage_item(cost);
            if damage != DamageResult::Untouched {
                let after = Player::cluster_attack_stack_snapshot(&after);
                item = Some(AttackItemTransition {
                    slot: attacker.cluster_attack_held_location(),
                    before: actor.held_item.clone().unwrap_or_else(pumpkin_cluster::inventory::InventoryStack::empty),
                    after,
                });
                if damage == DamageResult::Broken {
                    let before = combat_stat(&actor, CombatStatKind::Broken, held_item.item.id);
                    stats.push(CombatStatTransition {
                        kind: CombatStatKind::Broken,
                        item: held_item.item.id,
                        before,
                        after: before.saturating_add(1),
                    });
                }
            }
        }
    }
    let landed = !no_damage;
    let advancement_before = combat_advancement(&actor, CombatAdvancement::DealtOverkillDamage);
    let advancements = (landed && raw_damage_milli >= 100_000).then_some(CombatAdvancementTransition {
        advancement: CombatAdvancement::DealtOverkillDamage,
        before: advancement_before,
        after: true,
    }).into_iter().collect();
    let target_id = victim.get_entity().entity_id;
    let attack_tick = attacker.get_entity().age.load(Ordering::Relaxed);
    let is_mace_smash = matches!(attack_type, crate::entity::combat::AttackType::MaceSmash);
    let attacker_velocity_after = match &primary.outcome {
        CapturedAttackTargetOutcome::Living(outcome) if outcome.knockback.is_some() => {
            [
                f64::from_bits(actor.velocity_bits[0]) * 0.6,
                f64::from_bits(actor.velocity_bits[1]),
                f64::from_bits(actor.velocity_bits[2]) * 0.6,
            ]
            .map(f64::to_bits)
        }
        _ => actor.velocity_bits,
    };
    let effects = CapturedAttackActorEffects {
        cooldown: AttackCooldownTransition { before: actor.cooldown, after: 0 },
        velocity: AttackVelocityTransition { before_bits: actor.velocity_bits, after_bits: if landed { attacker_velocity_after } else { actor.velocity_bits } },
        last_attacking_id_before: actor.combat.last_attacking_id, last_attacking_id_after: if landed { target_id } else { actor.combat.last_attacking_id },
        last_attack_tick_before: actor.combat.last_attack_tick, last_attack_tick_after: if landed { attack_tick } else { actor.combat.last_attack_tick },
        fall_distance_before_bits: actor.fall_distance_bits, fall_distance_after_bits: if landed && is_mace_smash { 0.0f32.to_bits() } else { actor.fall_distance_bits },
        exhaustion_before_bits: actor.exhaustion_bits,
        exhaustion_after_bits: if landed {
            (f32::from_bits(actor.exhaustion_bits) + 0.1).to_bits()
        } else {
            actor.exhaustion_bits
        },
        item, stats, advancements,
    };
    let sweeping = if landed && matches!(attack_type, crate::entity::combat::AttackType::Sweeping) {
        let Some(sweeping) = capture_sweeping_targets(
            attacker,
            victim,
            raw_damage_milli,
            held_item.get_enchantment_level(&Enchantment::SWEEPING_EDGE),
            attacker_ref,
            tick,
            server,
        ) else {
            return;
        };
        sweeping
    } else {
        Vec::new()
    };
    let attack = capture_attack(CapturedAttack { actor: ActionActor::Player(gid), seq, tick, attacker: attacker_ref,
        outcome: if no_damage { CapturedAttackOutcome::NoDamage } else { CapturedAttackOutcome::Landed },
        primary, sweeping, attacker_effects: effects });
    if crate::server::cluster_entity_apply::stage_captured_attack(attack.clone()) {
        stage_captured_attack(attack);
    }
}

fn capture_attack_target(
    attacker: &Player,
    victim: &Arc<dyn EntityBase>,
    target: EntityMutationTarget,
    raw_damage_milli: u32,
    attack_type: crate::entity::combat::AttackType,
    fire_ticks: u32,
    allow_knockback: bool,
    attacker_ref: pumpkin_cluster::protocol::EntityRef,
    tick: TickStamp,
    server: &Arc<Server>,
) -> Option<CapturedAttackTarget> {
    if victim.get_entity().entity_type.id == pumpkin_data::entity::EntityType::ARMOR_STAND.id {
        return crate::entity::nonliving_attack::capture_nonliving_attack_target(
            victim.as_ref(),
            target,
            raw_damage_milli,
            attacker_ref,
            attacker.gameprofile.id,
            i64::from(tick.0),
            attacker.is_creative(),
        );
    }
    let living = victim.get_living_entity()?;
    let before = living.cluster_captured_attack_living_precondition();
    let defense = living.cluster_captured_attack_defense_snapshot()?;
    let armor = f64::from_bits(defense.armor_bits) as f32;
    let toughness = f64::from_bits(defense.toughness_bits) as f32;
    let mut effective = crate::entity::combat::CombatRules::get_damage_after_absorb(
        raw_damage_milli as f32 / 1000.0,
        armor,
        toughness,
        0,
    );
    if let Some(amplifier) = defense.resistance_amplifier {
        effective = (effective * (25 - (amplifier + 1) * 5) as f32 / 25.0).max(0.0);
    }
    effective = crate::entity::combat::CombatRules::get_damage_after_magic_absorb(
        effective, defense.protection_exact as f32,
    );
    let effective_damage_milli = (effective * 1000.0)
        .round()
        .clamp(0.0, i32::MAX as f32) as u32;
    let (delta, resolution) = if before.hurt_cooldown > 10
        && i32::try_from(effective_damage_milli)
            .is_ok_and(|damage| damage <= before.last_damage_taken_milli)
    {
        (0, LivingHurtCooldownResolution::NoDamage)
    } else {
        let delta = if before.hurt_cooldown > 10 {
            effective_damage_milli.saturating_sub(before.last_damage_taken_milli.max(0) as u32)
        } else {
            effective_damage_milli
        };
        let last_damage_taken_after_milli = i32::try_from(raw_damage_milli).ok()?;
        (
            delta,
            LivingHurtCooldownResolution::Applied {
                last_damage_taken_after_milli,
            },
        )
    };
    let absorption = delta.min(before.absorption_milli.max(0) as u32);
    let is_mace_smash = matches!(attack_type, crate::entity::combat::AttackType::MaceSmash);
    let mut outcome = LivingAttackOutcome {
        before: before.clone(),
        raw_damage_milli,
        effective_damage_milli,
        cooldown_damage_delta_milli: delta,
        absorption_damage_milli: absorption,
        health_damage_milli: delta - absorption,
        hurt_cooldown_resolution: resolution,
        damage_type: if is_mace_smash {
            AttackDamageType::MaceSmash
        } else {
            AttackDamageType::PlayerAttack
        },
        critical: matches!(attack_type, crate::entity::combat::AttackType::Critical),
        fire_ticks_before: before.fire_ticks,
        fire_ticks_after: fire_ticks_after(victim.as_ref(), before.fire_ticks, fire_ticks),
        visual_fire_before: before.visual_fire,
        visual_fire_after: before.visual_fire,
        knockback: None,
    };
    if matches!(outcome.hurt_cooldown_resolution, LivingHurtCooldownResolution::NoDamage) {
        outcome.fire_ticks_after = before.fire_ticks;
    } else if allow_knockback {
        let mut knockback_strength = f64::from(
            attacker
                .cluster_captured_attack_offense_snapshot()
                .map_or(0, |offense| offense.knockback),
        );
        if matches!(attack_type, crate::entity::combat::AttackType::Knockback) {
            knockback_strength += 1.0;
        }
        if server.advanced_config.pvp.knockback && knockback_strength > 0.0 {
            let resistance = f64::from_bits(defense.knockback_resistance_bits);
            let yaw = f64::from(attacker.get_entity().yaw.load());
            if !resistance.is_finite() || !yaw.is_finite() {
                return None;
            }
            let strength = knockback_strength * 0.5 * (1.0 - resistance);
            if strength > 0.0 && strength.is_finite() {
                let velocity = before.velocity_bits.map(f64::from_bits);
                if velocity.iter().any(|value| !value.is_finite()) {
                    return None;
                }
                let radians = yaw.to_radians();
                let horizontal_x = radians.sin();
                let horizontal_z = -radians.cos();
                let horizontal_length = horizontal_x.mul_add(horizontal_x, horizontal_z * horizontal_z);
                if horizontal_length < 1.0e-5 {
                    return None;
                }
                let scale = strength / horizontal_length.sqrt();
                let after = [
                    velocity[0] / 2.0 - horizontal_x * scale,
                    if victim.get_entity().on_ground.load(Ordering::Relaxed) {
                        (velocity[1] / 2.0 + strength).min(0.4)
                    } else {
                        velocity[1]
                    },
                    velocity[2] / 2.0 - horizontal_z * scale,
                ];
                if after.iter().any(|value| !value.is_finite()) {
                    return None;
                }
                outcome.knockback = Some(AttackKnockback {
                    velocity_before_bits: before.velocity_bits,
                    velocity_after_bits: after.map(f64::to_bits),
                });
            }
        }
    }
    Some(CapturedAttackTarget {
        target,
        outcome: CapturedAttackTargetOutcome::Living(outcome),
    })
}

fn fire_ticks_after(entity: &dyn EntityBase, before: u32, requested: u32) -> u32 {
    if requested == 0 {
        return before;
    }
    let base = entity.get_entity();
    (!base.fire_immune.load(Ordering::Relaxed))
        .then_some(before.max(requested))
        .unwrap_or(before)
}

fn capture_sweeping_targets(
    attacker: &Player,
    primary_victim: &Arc<dyn EntityBase>,
    primary_damage_milli: u32,
    sweeping_edge: i32,
    attacker_ref: pumpkin_cluster::protocol::EntityRef,
    tick: TickStamp,
    server: &Arc<Server>,
) -> Option<Vec<CapturedAttackTarget>> {
    if primary_victim.get_living_entity().is_none() {
        return Some(Vec::new());
    }
    let pos = primary_victim.get_entity().pos.load();
    let search_box = BoundingBox::new(
        Vector3::new(pos.x - 1.0, pos.y - 0.5, pos.z - 1.0),
        Vector3::new(pos.x + 1.0, pos.y + 0.5, pos.z + 1.0),
    );
    let primary_id = primary_victim.get_entity().entity_id;
    let attacker_id = attacker.entity_id();
    let mut victims: Vec<_> = attacker
        .world()
        .get_all_at_box(&search_box)
        .into_iter()
        .filter(|victim| {
            let id = victim.get_entity().entity_id;
            id != primary_id && id != attacker_id
        })
        .filter_map(|victim| entity_mutation_target(victim.as_ref()).map(|target| (target, victim)))
        .collect();
    victims.sort_by_key(|(target, _)| target_order(*target));
    let sweep_damage_milli = (1.0f32
        + primary_damage_milli as f32 / 1000.0
            * (sweeping_edge.max(0) as f32 / (sweeping_edge.max(0) as f32 + 1.0)))
        .mul_add(1000.0, 0.0)
        .clamp(0.0, i32::MAX as f32) as u32;
    victims
        .into_iter()
        .map(|(target, victim)| {
            capture_attack_target(
                attacker,
                &victim,
                target,
                sweep_damage_milli,
                crate::entity::combat::AttackType::Sweeping,
                0,
                false,
                attacker_ref,
                tick,
                server,
            )
        })
        .collect()
}

fn entity_mutation_target(entity: &dyn EntityBase) -> Option<EntityMutationTarget> {
    let entity_ref = crate::server::cluster_entity_apply::entity_ref_for(entity)?;
    entity
        .get_player()
        .and_then(Player::cluster_gid)
        .map(|gid| EntityMutationTarget::Player {
            gid,
            chunk: entity_ref.chunk,
        })
        .or(Some(EntityMutationTarget::Entity(entity_ref)))
}

fn target_order(target: EntityMutationTarget) -> (u8, u16, i32, i32, i32, u16, u16) {
    match target {
        EntityMutationTarget::Entity(entity) => (
            0,
            entity.origin.0,
            entity.local_id,
            entity.chunk.x,
            entity.chunk.z,
            entity.owner.0,
            0,
        ),
        EntityMutationTarget::Player { gid, chunk } => (
            1,
            gid.server.0,
            i32::from(gid.player.0),
            chunk.x,
            chunk.z,
            0,
            0,
        ),
    }
}

fn combat_stat(
    actor: &pumpkin_cluster::combat::CapturedAttackActorSnapshot,
    kind: CombatStatKind,
    item: u16,
) -> i32 {
    actor
        .stats
        .iter()
        .find_map(|value| (value.kind == kind && value.item == item).then_some(value.value))
        .unwrap_or_default()
}

fn combat_advancement(
    actor: &pumpkin_cluster::combat::CapturedAttackActorSnapshot,
    advancement: CombatAdvancement,
) -> bool {
    actor
        .advancements
        .iter()
        .find_map(|value| (value.advancement == advancement).then_some(value.complete))
        .unwrap_or(false)
}
