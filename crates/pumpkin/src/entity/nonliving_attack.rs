use pumpkin_cluster::combat::CapturedAttackTargetSnapshot;
use pumpkin_cluster::protocol::{
    CapturedAttack, CapturedAttackTarget, CapturedAttackTargetOutcome, EntityMutationTarget,
    EntityNbtTransition, ItemEntityAttackOutcome, ItemFrameAttackOutcome,
    NonLivingAttackKind, NonLivingAttackOutcome, VehicleAttackOutcome,
};
use std::sync::Arc;
use std::io::Cursor;

use pumpkin_nbt::{Nbt, compound::NbtCompound, deserializer::NbtReadHelperJava, tag::NbtTag};
use uuid::Uuid;

use crate::entity::EntityBase;

fn attack_lifecycle(
    present_before: bool,
    present_after: bool,
    spawned: Vec<pumpkin_cluster::protocol::AttackItemDrop>,
) -> pumpkin_cluster::protocol::NonLivingAttackLifecycle {
    pumpkin_cluster::protocol::NonLivingAttackLifecycle {
        present_before,
        present_after,
        spawned,
    }
}

pub trait NonLivingCapturedAttackState {
    fn captured_nonliving_attack_snapshot(
        &mut self,
        target: EntityMutationTarget,
    ) -> Option<NonLivingAttackOutcome>;

    fn apply_captured_nonliving_attack(
        &mut self,
        target: EntityMutationTarget,
        outcome: &NonLivingAttackOutcome,
    ) -> bool;

    fn restore_captured_nonliving_attack(
        &mut self,
        target: EntityMutationTarget,
        snapshot: &NonLivingAttackOutcome,
    ) -> bool;
}

pub struct ResolvedNonLivingCapturedAttackBinding {
    pub target: EntityMutationTarget,
    pub entity: Arc<dyn EntityBase>,
}

pub struct ResolvedNonLivingCapturedAttackState {
    bindings: Vec<ResolvedNonLivingCapturedAttackBinding>,
}

impl ResolvedNonLivingCapturedAttackState {
    #[must_use]
    pub fn new(bindings: Vec<ResolvedNonLivingCapturedAttackBinding>) -> Self {
        Self { bindings }
    }

    fn entity(&self, target: EntityMutationTarget) -> Option<&Arc<dyn EntityBase>> {
        self.bindings
            .iter()
            .find(|binding| binding.target.same_identity(target))
            .map(|binding| &binding.entity)
    }
}

impl NonLivingCapturedAttackState for ResolvedNonLivingCapturedAttackState {
    fn captured_nonliving_attack_snapshot(
        &mut self,
        target: EntityMutationTarget,
    ) -> Option<NonLivingAttackOutcome> {
        self.entity(target)?.cluster_nonliving_attack_snapshot()
    }

    fn apply_captured_nonliving_attack(
        &mut self,
        target: EntityMutationTarget,
        outcome: &NonLivingAttackOutcome,
    ) -> bool {
        self.entity(target)
            .is_some_and(|entity| entity.cluster_apply_nonliving_attack(outcome))
    }

    fn restore_captured_nonliving_attack(
        &mut self,
        target: EntityMutationTarget,
        snapshot: &NonLivingAttackOutcome,
    ) -> bool {
        self.entity(target)
            .is_some_and(|entity| entity.cluster_restore_nonliving_attack(snapshot))
    }
}

pub fn capture_nonliving_attack_target(
    entity: &dyn EntityBase,
    target: EntityMutationTarget,
    damage_milli: u32,
    attacker: pumpkin_cluster::protocol::EntityRef,
    attacker_uuid: Uuid,
    game_tick: i64,
    creative: bool,
) -> Option<CapturedAttackTarget> {
    let before = entity.cluster_nonliving_attack_snapshot()?;
    let outcome = match before {
        NonLivingAttackOutcome::ItemFrame(outcome) => {
            if outcome.fixed && !creative {
                NonLivingAttackOutcome::ItemFrame(outcome)
            } else if outcome.fixed {
                NonLivingAttackOutcome::ItemFrame(ItemFrameAttackOutcome {
                    item_after: pumpkin_cluster::inventory::InventoryStack::empty(),
                    removed_after: true,
                    drops: Vec::new(),
                    lifecycle: attack_lifecycle(!outcome.removed_before, false, Vec::new()),
                    ..outcome
                })
            } else if outcome.item_before.is_empty() {
                let frame = entity
                    .cast_any()
                    .downcast_ref::<crate::entity::decoration::item_frame::ItemFrameEntity>()?;
                let drops: Vec<pumpkin_cluster::protocol::AttackItemDrop> = (!creative)
                    .then(|| {
                        reserve_attack_drop(
                            entity,
                            attacker,
                            target.chunk(),
                            crate::entity::item::ItemEntity::cluster_stack_from_item_stack(
                                &frame.get_frame_item_stack_with_data(),
                            ),
                        )
                    })
                    .flatten()
                    .into_iter()
                    .collect();
                NonLivingAttackOutcome::ItemFrame(ItemFrameAttackOutcome {
                    item_after: outcome.item_before.clone(),
                    removed_after: true,
                    lifecycle: attack_lifecycle(!outcome.removed_before, false, drops.clone()),
                    drops,
                    ..outcome
                })
            } else {
                let drops: Vec<pumpkin_cluster::protocol::AttackItemDrop> = (!creative
                    && deterministic_item_frame_drop_roll(attacker, attacker_uuid, game_tick, target)
                        < f32::from_bits(outcome.item_drop_chance_bits))
                    .then(|| reserve_attack_drop(entity, attacker, target.chunk(), outcome.item_before.clone()))
                    .flatten()
                    .into_iter()
                    .collect();
                NonLivingAttackOutcome::ItemFrame(ItemFrameAttackOutcome {
                    item_after: pumpkin_cluster::inventory::InventoryStack::empty(),
                    lifecycle: attack_lifecycle(
                        !outcome.removed_before,
                        !outcome.removed_before,
                        drops.clone(),
                    ),
                    drops,
                    ..outcome
                })
            }
        }
        NonLivingAttackOutcome::Item(outcome) => {
            let health_before = f32::from_bits(outcome.health_before_bits);
            let health_after = (health_before - damage_milli as f32 / 1000.0).max(0.0);
            NonLivingAttackOutcome::Item(ItemEntityAttackOutcome {
                health_after_bits: health_after.to_bits(),
                removed_after: health_after <= 0.0,
                lifecycle: attack_lifecycle(!outcome.removed_before, health_after > 0.0, Vec::new()),
                ..outcome
            })
        }
        NonLivingAttackOutcome::Vehicle(outcome) => {
            let damage_after = f32::from_bits(outcome.damage_before_bits)
                + damage_milli as f32 / 100.0;
            NonLivingAttackOutcome::Vehicle(VehicleAttackOutcome {
                hurt_time_after: 10,
                hurt_dir_after: -outcome.hurt_dir_before,
                damage_after_bits: damage_after.to_bits(),
                removed_after: damage_after > 40.0,
                drops: Vec::new(),
                lifecycle: attack_lifecycle(!outcome.removed_before, damage_after <= 40.0, Vec::new()),
                ..outcome
            })
        }
        NonLivingAttackOutcome::Interaction(outcome) => {
            NonLivingAttackOutcome::Interaction(pumpkin_cluster::protocol::InteractionAttackOutcome {
                after: interaction_attack_after(&outcome.before, attacker_uuid, game_tick)?,
                ..outcome
            })
        }
        NonLivingAttackOutcome::ArmorStand(mut outcome) | NonLivingAttackOutcome::Destroy(mut outcome) => {
            if matches!(outcome.kind, NonLivingAttackKind::Marker | NonLivingAttackKind::Display) {
                NonLivingAttackOutcome::Destroy(outcome)
            } else {
                outcome.removed_after = true;
                outcome.drops = Vec::new();
                outcome.lifecycle = attack_lifecycle(!outcome.removed_before, false, Vec::new());
                if matches!(outcome.kind, NonLivingAttackKind::ArmorStand) {
                    NonLivingAttackOutcome::ArmorStand(outcome)
                } else {
                    NonLivingAttackOutcome::Destroy(outcome)
                }
            }
        }
        NonLivingAttackOutcome::Noop(kind) => NonLivingAttackOutcome::Noop(kind),
    };
    Some(CapturedAttackTarget {
        target,
        outcome: CapturedAttackTargetOutcome::NonLiving(outcome),
    })
}

fn deterministic_item_frame_drop_roll(
    attacker: pumpkin_cluster::protocol::EntityRef,
    attacker_uuid: Uuid,
    game_tick: i64,
    target: EntityMutationTarget,
) -> f32 {
    let target_words = match target {
        EntityMutationTarget::Entity(target) => [
            u64::from(target.origin.0),
            u64::from(target.owner.0),
            target.local_id as u32 as u64,
            target.chunk.x as u32 as u64 ^ ((target.chunk.z as u32 as u64) << 32),
        ],
        EntityMutationTarget::Player { gid, chunk } => [
            u64::from(gid.server.0),
            u64::from(gid.player.0),
            chunk.x as u32 as u64,
            chunk.z as u32 as u64,
        ],
    };
    let mut state = attacker_uuid.as_u128() as u64
        ^ (attacker_uuid.as_u128() >> 64) as u64
        ^ u64::from(attacker.origin.0)
        ^ (u64::from(attacker.owner.0) << 16)
        ^ ((attacker.local_id as u32 as u64) << 32)
        ^ game_tick as u64;
    for word in target_words {
        state ^= word.wrapping_add(0x9E37_79B9_7F4A_7C15);
        state ^= state >> 30;
        state = state.wrapping_mul(0xBF58_476D_1CE4_E5B9);
        state ^= state >> 27;
        state = state.wrapping_mul(0x94D0_49BB_1331_11EB);
        state ^= state >> 31;
    }
    (state >> 40) as f32 / (1_u32 << 24) as f32
}

fn reserve_attack_drop(
    source: &dyn EntityBase,
    attacker: pumpkin_cluster::protocol::EntityRef,
    chunk: pumpkin_cluster::protocol::ChunkAddr,
    stack: pumpkin_cluster::inventory::InventoryStack,
) -> Option<pumpkin_cluster::protocol::AttackItemDrop> {
    let entity = pumpkin_cluster::protocol::EntityRef {
        origin: attacker.owner,
        owner: attacker.owner,
        local_id: crate::entity::Entity::reserve_ids(1),
        chunk,
    };
    capture_attack_item_drop(source, entity, stack)
}

pub fn capture_attack_item_drop(
    source: &dyn EntityBase,
    entity: pumpkin_cluster::protocol::EntityRef,
    stack: pumpkin_cluster::inventory::InventoryStack,
) -> Option<pumpkin_cluster::protocol::AttackItemDrop> {
    if stack.is_empty() {
        return None;
    }
    let mut cursor = Cursor::new(stack.nbt.as_slice());
    let mut reader = NbtReadHelperJava::new(&mut cursor);
    let item = Nbt::read_unnamed(&mut reader).ok()?;
    if cursor.position() != stack.nbt.len() as u64 {
        return None;
    }
    let source = source.get_entity();
    let position = source.pos.load();
    let velocity = source.velocity.load();
    let mut entity_nbt = NbtCompound::new();
    entity_nbt.put_string("id", "minecraft:item".to_string());
    entity_nbt.put_uuid("UUID", attack_item_uuid(entity));
    entity_nbt.put(
        "Pos",
        NbtTag::List(vec![position.x.into(), position.y.into(), position.z.into()]),
    );
    entity_nbt.put(
        "Motion",
        NbtTag::List(vec![velocity.x.into(), velocity.y.into(), velocity.z.into()]),
    );
    entity_nbt.put(
        "Rotation",
        NbtTag::List(vec![source.yaw.load().into(), source.pitch.load().into()]),
    );
    entity_nbt.put_short("Fire", source.fire_ticks.load(std::sync::atomic::Ordering::Relaxed) as i16);
    entity_nbt.put_bool("OnGround", source.on_ground.load(std::sync::atomic::Ordering::Relaxed));
    entity_nbt.put_bool("Invulnerable", source.invulnerable.load(std::sync::atomic::Ordering::Relaxed));
    entity_nbt.put_int("PortalCooldown", source.portal_cooldown.load(std::sync::atomic::Ordering::Relaxed) as i32);
    entity_nbt.put_bool("HasVisualFire", source.has_visual_fire.load(std::sync::atomic::Ordering::Relaxed));
    entity_nbt.put_int("TicksFrozen", source.frozen_ticks.load(std::sync::atomic::Ordering::Relaxed));
    entity_nbt.put_compound("Item", item.root_tag);
    entity_nbt.put_short("Age", 0);
    entity_nbt.put_short("PickupDelay", crate::entity::item::ItemEntity::DEFAULT_PICKUP_DELAY.into());
    entity_nbt.put_short("Health", 5);
    Some(pumpkin_cluster::protocol::AttackItemDrop {
        entity,
        stack,
        entity_nbt: Nbt::from(entity_nbt).write_unnamed().to_vec(),
    })
}

fn attack_item_uuid(entity: pumpkin_cluster::protocol::EntityRef) -> Uuid {
    let mut bytes = [0_u8; 16];
    bytes[..4].copy_from_slice(b"PMPR");
    bytes[4..6].copy_from_slice(&entity.origin.0.to_be_bytes());
    bytes[6..10].copy_from_slice(&entity.local_id.to_be_bytes());
    Uuid::from_bytes(bytes)
}

fn interaction_attack_after(before: &[u8], attacker: Uuid, game_tick: i64) -> Option<Vec<u8>> {
    let mut cursor = Cursor::new(before);
    let mut reader = NbtReadHelperJava::new(&mut cursor);
    let nbt = Nbt::read_unnamed(&mut reader).ok()?;
    if cursor.position() != before.len() as u64 {
        return None;
    }
    let mut root: NbtCompound = nbt.root_tag;
    let action = crate::entity::interaction::PlayerAction {
        player: attacker,
        timestamp: game_tick,
    };
    root.put("attack", NbtTag::Compound(action.to_nbt()));
    Some(Nbt::from(root).write_unnamed().to_vec())
}

pub fn captured_nonliving_attack_snapshots(
    state: &mut impl NonLivingCapturedAttackState,
    attack: &CapturedAttack,
) -> Option<Vec<CapturedAttackTargetSnapshot>> {
    attack
        .targets()
        .filter_map(|target| match &target.outcome {
            CapturedAttackTargetOutcome::Living(_) => None,
            CapturedAttackTargetOutcome::NonLiving(_) => Some(target),
        })
        .map(|target| {
            state
                .captured_nonliving_attack_snapshot(target.target)
                .map(|outcome| CapturedAttackTargetSnapshot::NonLiving {
                    target: target.target,
                    outcome,
                })
        })
        .collect()
}

pub fn nonliving_attack_precondition(
    state: &mut impl NonLivingCapturedAttackState,
    target: &CapturedAttackTarget,
) -> bool {
    let CapturedAttackTargetOutcome::NonLiving(outcome) = &target.outcome else {
        return false;
    };
    outcome.is_well_formed()
        && state.captured_nonliving_attack_snapshot(target.target).as_ref()
            == Some(&nonliving_attack_before(outcome))
}

pub fn apply_nonliving_captured_attack(
    state: &mut impl NonLivingCapturedAttackState,
    attack: &CapturedAttack,
) -> bool {
    let nonliving: Vec<_> = attack
        .targets()
        .filter(|target| matches!(target.outcome, CapturedAttackTargetOutcome::NonLiving(_)))
        .collect();
    if nonliving
        .iter()
        .any(|target| !nonliving_attack_precondition(state, target))
    {
        return false;
    }
    let mut applied = Vec::with_capacity(nonliving.len());
    for target in nonliving {
        let CapturedAttackTargetOutcome::NonLiving(outcome) = &target.outcome else {
            return false;
        };
        if !state.apply_captured_nonliving_attack(target.target, outcome) {
            for (target, snapshot) in applied.into_iter().rev() {
                let _ = state.restore_captured_nonliving_attack(target, &snapshot);
            }
            return false;
        }
        applied.push((target.target, nonliving_attack_before(outcome)));
    }
    true
}

pub fn restore_nonliving_captured_attack(
    state: &mut impl NonLivingCapturedAttackState,
    snapshot: &CapturedAttackTargetSnapshot,
) -> bool {
    let CapturedAttackTargetSnapshot::NonLiving { target, outcome } = snapshot else {
        return false;
    };
    state.restore_captured_nonliving_attack(*target, outcome)
}

#[must_use]
pub fn nonliving_attack_before(outcome: &NonLivingAttackOutcome) -> NonLivingAttackOutcome {
    match outcome {
        NonLivingAttackOutcome::ItemFrame(outcome) => {
            NonLivingAttackOutcome::ItemFrame(ItemFrameAttackOutcome {
                kind: outcome.kind,
                fixed: outcome.fixed,
                item_drop_chance_bits: outcome.item_drop_chance_bits,
                item_before: outcome.item_before.clone(),
                item_after: outcome.item_before.clone(),
                rotation_before: outcome.rotation_before,
                rotation_after: outcome.rotation_before,
                removed_before: outcome.removed_before,
                removed_after: outcome.removed_before,
                drops: Vec::new(),
                lifecycle: attack_lifecycle(!outcome.removed_before, !outcome.removed_before, Vec::new()),
            })
        }
        NonLivingAttackOutcome::ArmorStand(outcome) => {
            NonLivingAttackOutcome::ArmorStand(EntityNbtTransition {
                kind: outcome.kind,
                before: outcome.before.clone(),
                after: outcome.before.clone(),
                removed_before: outcome.removed_before,
                removed_after: outcome.removed_before,
                drops: Vec::new(),
                lifecycle: attack_lifecycle(!outcome.removed_before, !outcome.removed_before, Vec::new()),
            })
        }
        NonLivingAttackOutcome::Vehicle(outcome) => {
            NonLivingAttackOutcome::Vehicle(VehicleAttackOutcome {
                kind: outcome.kind,
                hurt_time_before: outcome.hurt_time_before,
                hurt_time_after: outcome.hurt_time_before,
                hurt_dir_before: outcome.hurt_dir_before,
                hurt_dir_after: outcome.hurt_dir_before,
                damage_before_bits: outcome.damage_before_bits,
                damage_after_bits: outcome.damage_before_bits,
                removed_before: outcome.removed_before,
                removed_after: outcome.removed_before,
                drops: Vec::new(),
                lifecycle: attack_lifecycle(!outcome.removed_before, !outcome.removed_before, Vec::new()),
            })
        }
        NonLivingAttackOutcome::Item(outcome) => NonLivingAttackOutcome::Item(ItemEntityAttackOutcome {
            stack_before: outcome.stack_before.clone(),
            stack_after: outcome.stack_before.clone(),
            health_before_bits: outcome.health_before_bits,
            health_after_bits: outcome.health_before_bits,
            removed_before: outcome.removed_before,
            removed_after: outcome.removed_before,
            lifecycle: attack_lifecycle(!outcome.removed_before, !outcome.removed_before, Vec::new()),
        }),
        NonLivingAttackOutcome::Interaction(outcome) => {
            NonLivingAttackOutcome::Interaction(crate::entity::nonliving_attack::interaction_before(outcome))
        }
        NonLivingAttackOutcome::Destroy(outcome) => NonLivingAttackOutcome::Destroy(EntityNbtTransition {
            kind: outcome.kind,
            before: outcome.before.clone(),
            after: outcome.before.clone(),
            removed_before: outcome.removed_before,
            removed_after: outcome.removed_before,
            drops: Vec::new(),
            lifecycle: attack_lifecycle(!outcome.removed_before, !outcome.removed_before, Vec::new()),
        }),
        NonLivingAttackOutcome::Noop(kind) => NonLivingAttackOutcome::Noop(*kind),
    }
}

fn interaction_before(
    outcome: &pumpkin_cluster::protocol::InteractionAttackOutcome,
) -> pumpkin_cluster::protocol::InteractionAttackOutcome {
    pumpkin_cluster::protocol::InteractionAttackOutcome {
        before: outcome.before.clone(),
        after: outcome.before.clone(),
        lifecycle: attack_lifecycle(
            outcome.lifecycle.present_before,
            outcome.lifecycle.present_before,
            Vec::new(),
        ),
    }
}

#[cfg(test)]
mod tests {
    use std::collections::BTreeMap;

    use pumpkin_cluster::identity::ServerId;
    use pumpkin_cluster::inventory::InventoryStack;
    use pumpkin_cluster::protocol::{
        ChunkAddr, EntityRef, ItemFrameAttackOutcome, NonLivingAttackKind,
    };

    use super::*;

    struct State {
        outcomes: BTreeMap<(ServerId, i32), NonLivingAttackOutcome>,
    }

    impl NonLivingCapturedAttackState for State {
        fn captured_nonliving_attack_snapshot(
            &mut self,
            target: EntityMutationTarget,
        ) -> Option<NonLivingAttackOutcome> {
            let EntityMutationTarget::Entity(reference) = target else {
                return None;
            };
            self.outcomes
                .get(&(reference.origin, reference.local_id))
                .cloned()
        }

        fn apply_captured_nonliving_attack(
            &mut self,
            target: EntityMutationTarget,
            outcome: &NonLivingAttackOutcome,
        ) -> bool {
            let EntityMutationTarget::Entity(reference) = target else {
                return false;
            };
            self.outcomes
                .insert((reference.origin, reference.local_id), outcome.clone());
            true
        }

        fn restore_captured_nonliving_attack(
            &mut self,
            target: EntityMutationTarget,
            snapshot: &NonLivingAttackOutcome,
        ) -> bool {
            self.apply_captured_nonliving_attack(target, snapshot)
        }
    }

    #[test]
    fn item_frame_transition_checks_full_nbt_then_restores_exact_prestate() {
        let reference = EntityRef {
            origin: ServerId(2),
            owner: ServerId(2),
            local_id: 77,
            chunk: ChunkAddr { x: 1, z: 1 },
        };
        let stack = InventoryStack {
            item: 5,
            count: 1,
            nbt: vec![10, 0, 1, b'x', 1, 0, 0],
        };
        let outcome = NonLivingAttackOutcome::ItemFrame(ItemFrameAttackOutcome {
            kind: NonLivingAttackKind::ItemFrame,
            fixed: false,
            item_drop_chance_bits: 1.0f32.to_bits(),
            item_before: stack.clone(),
            item_after: InventoryStack::empty(),
            rotation_before: 3,
            rotation_after: 3,
            removed_before: false,
            removed_after: false,
            drops: Vec::new(),
            lifecycle: attack_lifecycle(true, true, Vec::new()),
        });
        let target = CapturedAttackTarget {
            target: EntityMutationTarget::Entity(reference),
            outcome: CapturedAttackTargetOutcome::NonLiving(outcome.clone()),
        };
        let mut state = State {
            outcomes: BTreeMap::from([((reference.origin, reference.local_id), nonliving_attack_before(&outcome))]),
        };
        assert!(nonliving_attack_precondition(&mut state, &target));
        let attack = CapturedAttack {
            actor: pumpkin_cluster::identity::ActionActor::Server(ServerId(2)),
            seq: pumpkin_cluster::identity::PlayerSeq(0),
            tick: pumpkin_cluster::time::TickStamp(1),
            attacker: reference,
            outcome: pumpkin_cluster::protocol::CapturedAttackOutcome::Landed,
            primary: target,
            sweeping: Vec::new(),
            attacker_effects: pumpkin_cluster::protocol::CapturedAttackActorEffects {
                cooldown: pumpkin_cluster::protocol::AttackCooldownTransition { before: 0, after: 0 },
                velocity: pumpkin_cluster::protocol::AttackVelocityTransition {
                    before_bits: [0; 3],
                    after_bits: [0; 3],
                },
                last_attacking_id_before: 0,
                last_attacking_id_after: 0,
                last_attack_tick_before: 0,
                last_attack_tick_after: 0,
                fall_distance_before_bits: 0,
                fall_distance_after_bits: 0,
                exhaustion_before_bits: 0,
                exhaustion_after_bits: 0,
                item: None,
                stats: Vec::new(),
                advancements: Vec::new(),
            },
        };
        assert!(apply_nonliving_captured_attack(&mut state, &attack));
        assert_eq!(
            state.outcomes.get(&(reference.origin, reference.local_id)),
            Some(&outcome)
        );
        let snapshot = CapturedAttackTargetSnapshot::NonLiving {
            target: EntityMutationTarget::Entity(reference),
            outcome: nonliving_attack_before(&outcome),
        };
        assert!(restore_nonliving_captured_attack(&mut state, &snapshot));
        assert_eq!(
            state.outcomes.get(&(reference.origin, reference.local_id)),
            Some(&nonliving_attack_before(&outcome))
        );
    }

    #[test]
    fn item_frame_drop_roll_is_stable_for_one_captured_action() {
        let attacker = EntityRef {
            origin: ServerId(4),
            owner: ServerId(4),
            local_id: 19,
            chunk: ChunkAddr { x: -2, z: 5 },
        };
        let target = EntityMutationTarget::Entity(EntityRef {
            origin: ServerId(9),
            owner: ServerId(9),
            local_id: 81,
            chunk: ChunkAddr { x: 7, z: -3 },
        });
        let uuid = Uuid::from_u128(0x6c5d_b64a_220c_43e2_91d8_4c2d_0e1a_0701);
        let first = deterministic_item_frame_drop_roll(attacker, uuid, 382, target);
        let second = deterministic_item_frame_drop_roll(attacker, uuid, 382, target);
        assert_eq!(first.to_bits(), second.to_bits());
        assert!((0.0..1.0).contains(&first));
    }
}
