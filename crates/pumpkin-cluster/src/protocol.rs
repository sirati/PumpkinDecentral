//! Cluster wire protocol: identities, addresses, streams, and tick batches.
//!
//! Shape: every player update carries a [`GlobalPlayerId`] (`server` + `player`),
//! a [`PlayerSeq`] ordering token, and a [`TickStamp`]. A [`ChunkAddr`] (`x`, `z`)
//! locates chunk-scoped work, [`BlockPos`] locates a single block, and
//! [`EntityRef`] (`owner` + `local_id` + `chunk`) names a foreign entity.
//!
//! Rules the code below encodes, so callers do not have to look elsewhere:
//! - [`StreamKind`] is one variant per update family: player traffic rides
//!   `PlayerVisual` | `PlayerTransient` | `PlayerWorld` | `PlayerCombat`,
//!   entity traffic rides `EntityPos` | `EntityVisual` | `EntityTransient` |
//!   `EntityCombat`, and mesh plumbing rides `Control` | `ChunkRequest` |
//!   `ChunkData` | `Accept`.
//! - Each update struct names its family via `stream_kind()`; every peer uses
//!   the same mapping, so demux stays consistent without a lookup table.
//! - [`TickBatch`] is one fused tick: `tick` plus one `Vec` per update type.
//!   [`TickBatch::is_empty`], [`TickBatch::len`], and
//!   [`TickBatch::append_bank`] cover every array, so a new array must touch
//!   all three.
//! - Wire form is `postcard` over these exact fields in declaration order;
//!   renames and reorders fork the wire.

use serde::{Deserialize, Serialize};

use crate::identity::{ActionActor, ActionSeq, GlobalPlayerId, PlayerSeq, ServerId};
use crate::inventory::{InvLoc, InventoryOp, InventoryStack};
use crate::time::TickStamp;

pub const INV_OP_SEED: u64 = 0x9E37_79B9_7F4A_7C15;

/// Chunk column address in chunk units (`x`, `z`).
///
/// Block `(bx, bz)` maps to chunk `(bx.div_euclid(16), bz.div_euclid(16))`.
/// Chunk-scoped updates (`break` / `place`) and chunk fetch carry this so
/// holders, targets, and waiters agree without recomputing.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, PartialOrd, Ord, Serialize, Deserialize)]
pub struct ChunkAddr {
    pub x: i32,
    pub z: i32,
}

/// Single block cell in world coordinates.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub struct BlockPos {
    pub x: i32,
    pub y: i32,
    pub z: i32,
}

/// Foreign entity handle: owning server, server-local id, and home chunk.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub struct EntityRef {
    pub origin: ServerId,
    pub owner: ServerId,
    pub local_id: i32,
    pub chunk: ChunkAddr,
}

impl EntityRef {
    #[must_use]
    pub const fn same_identity(self, other: Self) -> bool {
        self.origin.0 == other.origin.0 && self.local_id == other.local_id
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub enum PlayerGameMode {
    Survival,
    Creative,
    Adventure,
    Spectator,
}

/// Player position and velocity sample for one tick.
///
/// `gid` is `server` + `player`, `seq` orders samples from that player,
/// `tick` pins the sample to the fused tick. Rides
/// [`StreamKind::PlayerWorld`].
#[derive(Debug, Clone, Copy, PartialEq, Serialize, Deserialize)]
pub struct PosUpdate {
    pub gid: GlobalPlayerId,
    pub seq: PlayerSeq,
    pub tick: TickStamp,
    pub pos: [f64; 3],
    pub vel: [f64; 3],
    pub yaw: f32,
    pub pitch: f32,
}

impl PosUpdate {
    #[must_use]
    pub const fn stream_kind() -> StreamKind {
        StreamKind::PlayerWorld
    }
}

/// Armor slot content (`slot`, vanilla armor `item` id).
///
/// Rides [`StreamKind::PlayerVisual`].
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub struct ArmorUpdate {
    pub gid: GlobalPlayerId,
    pub seq: PlayerSeq,
    pub tick: TickStamp,
    pub slot: u8,
    pub item: u16,
}

impl ArmorUpdate {
    #[must_use]
    pub const fn stream_kind() -> StreamKind {
        StreamKind::PlayerVisual
    }
}

/// Selected hotbar slot.
///
/// Rides [`StreamKind::PlayerVisual`].
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub struct HeldUpdate {
    pub gid: GlobalPlayerId,
    pub seq: PlayerSeq,
    pub tick: TickStamp,
    pub slot: u8,
}

impl HeldUpdate {
    #[must_use]
    pub const fn stream_kind() -> StreamKind {
        StreamKind::PlayerVisual
    }
}

/// Sneak flag edge.
///
/// Rides [`StreamKind::PlayerVisual`].
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub struct SneakUpdate {
    pub gid: GlobalPlayerId,
    pub seq: PlayerSeq,
    pub tick: TickStamp,
    pub active: bool,
}

impl SneakUpdate {
    #[must_use]
    pub const fn stream_kind() -> StreamKind {
        StreamKind::PlayerVisual
    }
}

/// Sprint flag edge.
///
/// Rides [`StreamKind::PlayerVisual`].
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub struct SprintUpdate {
    pub gid: GlobalPlayerId,
    pub seq: PlayerSeq,
    pub tick: TickStamp,
    pub active: bool,
}

impl SprintUpdate {
    #[must_use]
    pub const fn stream_kind() -> StreamKind {
        StreamKind::PlayerVisual
    }
}

/// Shield-block flag edge.
///
/// Rides [`StreamKind::PlayerVisual`].
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub struct BlockingUpdate {
    pub gid: GlobalPlayerId,
    pub seq: PlayerSeq,
    pub tick: TickStamp,
    pub active: bool,
}

impl BlockingUpdate {
    #[must_use]
    pub const fn stream_kind() -> StreamKind {
        StreamKind::PlayerVisual
    }
}

/// Arm swing event (`hand` selects main/off hand).
///
/// Rides [`StreamKind::PlayerVisual`].
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub struct SwingUpdate {
    pub gid: GlobalPlayerId,
    pub seq: PlayerSeq,
    pub tick: TickStamp,
    pub hand: u8,
}

impl SwingUpdate {
    #[must_use]
    pub const fn stream_kind() -> StreamKind {
        StreamKind::PlayerVisual
    }
}

/// Skin-layer visibility mask.
///
/// Rides [`StreamKind::PlayerVisual`].
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub struct SkinLayersUpdate {
    pub gid: GlobalPlayerId,
    pub seq: PlayerSeq,
    pub tick: TickStamp,
    pub mask: u8,
}

impl SkinLayersUpdate {
    #[must_use]
    pub const fn stream_kind() -> StreamKind {
        StreamKind::PlayerVisual
    }
}

/// Eating-use start on a hotbar `slot`.
///
/// Rides [`StreamKind::PlayerTransient`].
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub struct EatStartUpdate {
    pub gid: GlobalPlayerId,
    pub seq: PlayerSeq,
    pub tick: TickStamp,
    pub slot: u8,
    pub item: u16,
    pub count: u8,
}

impl EatStartUpdate {
    #[must_use]
    pub const fn stream_kind() -> StreamKind {
        StreamKind::PlayerTransient
    }
}

/// Eating-use abort; no payload beyond identity and ordering.
///
/// Rides [`StreamKind::PlayerTransient`].
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub struct EatAbortUpdate {
    pub gid: GlobalPlayerId,
    pub seq: PlayerSeq,
    pub tick: TickStamp,
}

impl EatAbortUpdate {
    #[must_use]
    pub const fn stream_kind() -> StreamKind {
        StreamKind::PlayerTransient
    }
}

/// Block-break animation stage at `pos` (`255` stops).
///
/// Rides [`StreamKind::PlayerTransient`].
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub struct BreakAnimUpdate {
    pub gid: GlobalPlayerId,
    pub seq: PlayerSeq,
    pub tick: TickStamp,
    pub pos: BlockPos,
    pub stage: u8,
}

impl BreakAnimUpdate {
    #[must_use]
    pub const fn stream_kind() -> StreamKind {
        StreamKind::PlayerTransient
    }
}

/// Authoritative block-break intent with expected old state and home chunk.
///
/// Rides [`StreamKind::PlayerWorld`].
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub struct BreakBlockUpdate {
    pub gid: GlobalPlayerId,
    pub seq: PlayerSeq,
    pub tick: TickStamp,
    pub pos: BlockPos,
    pub expected_old_state: u16,
    pub chunk: ChunkAddr,
}

impl BreakBlockUpdate {
    #[must_use]
    pub const fn stream_kind() -> StreamKind {
        StreamKind::PlayerWorld
    }
}

/// Authoritative block-place intent with new state and inventory delta.
///
/// Rides [`StreamKind::PlayerWorld`].
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub struct PlaceBlockUpdate {
    pub gid: GlobalPlayerId,
    pub seq: PlayerSeq,
    pub tick: TickStamp,
    pub pos: BlockPos,
    pub expected_old_state: u16,
    pub new_state: u16,
    pub inv: u8,
    pub slot: u8,
    pub item: u16,
    pub count_before: u8,
    pub count_after: u8,
    pub chunk: ChunkAddr,
}

impl PlaceBlockUpdate {
    #[must_use]
    pub const fn stream_kind() -> StreamKind {
        StreamKind::PlayerWorld
    }
}

/// Rejected-write compensation: state and inventory to restore.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub struct BlockUndo {
    pub old_state: u16,
    pub count_before: u8,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub enum AttackDamageType {
    PlayerAttack,
    MaceSmash,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub struct AttackCooldownTransition {
    pub before: u32,
    pub after: u32,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub struct AttackKnockback {
    pub velocity_before_bits: [u64; 3],
    pub velocity_after_bits: [u64; 3],
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub struct CombatAttributeSnapshot {
    pub id: u16,
    pub value_bits: u64,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct CombatEquipmentSnapshot {
    pub slot: u8,
    pub item: InventoryStack,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct LivingAttackPrecondition {
    pub health_milli: i32,
    pub absorption_milli: i32,
    pub hurt_cooldown: i32,
    pub last_damage_taken_milli: i32,
    pub fire_ticks: u32,
    pub visual_fire: bool,
    pub velocity_bits: [u64; 3],
    pub effects: Vec<StatusEffectState>,
    pub attributes: Vec<CombatAttributeSnapshot>,
    pub equipment: Vec<CombatEquipmentSnapshot>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub enum LivingHurtCooldownResolution {
    NoDamage,
    Applied { last_damage_taken_after_milli: i32 },
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub struct AttackVelocityTransition {
    pub before_bits: [u64; 3],
    pub after_bits: [u64; 3],
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct LivingAttackOutcome {
    pub before: LivingAttackPrecondition,
    pub raw_damage_milli: u32,
    pub effective_damage_milli: u32,
    pub cooldown_damage_delta_milli: u32,
    pub absorption_damage_milli: u32,
    pub health_damage_milli: u32,
    pub hurt_cooldown_resolution: LivingHurtCooldownResolution,
    pub damage_type: AttackDamageType,
    pub critical: bool,
    pub fire_ticks_before: u32,
    pub fire_ticks_after: u32,
    pub visual_fire_before: bool,
    pub visual_fire_after: bool,
    pub knockback: Option<AttackKnockback>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub enum NonLivingAttackKind {
    ArmorStand,
    ItemFrame,
    GlowItemFrame,
    Item,
    Boat,
    ChestBoat,
    Minecart,
    ChestMinecart,
    FurnaceMinecart,
    HopperMinecart,
    TntMinecart,
    CommandBlockMinecart,
    SpawnerMinecart,
    Painting,
    EndCrystal,
    Interaction,
    Projectile,
    Display,
    Marker,
    Other(u16),
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct AttackItemDrop {
    pub entity: EntityRef,
    pub stack: InventoryStack,
    pub entity_nbt: Vec<u8>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct NonLivingAttackLifecycle {
    pub present_before: bool,
    pub present_after: bool,
    pub spawned: Vec<AttackItemDrop>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct EntityNbtTransition {
    pub kind: NonLivingAttackKind,
    pub before: Vec<u8>,
    pub after: Vec<u8>,
    pub removed_before: bool,
    pub removed_after: bool,
    pub drops: Vec<AttackItemDrop>,
    pub lifecycle: NonLivingAttackLifecycle,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct ItemFrameAttackOutcome {
    pub kind: NonLivingAttackKind,
    pub fixed: bool,
    pub item_drop_chance_bits: u32,
    pub item_before: InventoryStack,
    pub item_after: InventoryStack,
    pub rotation_before: u8,
    pub rotation_after: u8,
    pub removed_before: bool,
    pub removed_after: bool,
    pub drops: Vec<AttackItemDrop>,
    pub lifecycle: NonLivingAttackLifecycle,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct VehicleAttackOutcome {
    pub kind: NonLivingAttackKind,
    pub hurt_time_before: i32,
    pub hurt_time_after: i32,
    pub hurt_dir_before: i32,
    pub hurt_dir_after: i32,
    pub damage_before_bits: u32,
    pub damage_after_bits: u32,
    pub removed_before: bool,
    pub removed_after: bool,
    pub drops: Vec<AttackItemDrop>,
    pub lifecycle: NonLivingAttackLifecycle,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct ItemEntityAttackOutcome {
    pub stack_before: InventoryStack,
    pub stack_after: InventoryStack,
    pub health_before_bits: u32,
    pub health_after_bits: u32,
    pub removed_before: bool,
    pub removed_after: bool,
    pub lifecycle: NonLivingAttackLifecycle,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct InteractionAttackOutcome {
    pub before: Vec<u8>,
    pub after: Vec<u8>,
    pub lifecycle: NonLivingAttackLifecycle,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub enum NonLivingAttackOutcome {
    ItemFrame(ItemFrameAttackOutcome),
    ArmorStand(EntityNbtTransition),
    Vehicle(VehicleAttackOutcome),
    Item(ItemEntityAttackOutcome),
    Interaction(InteractionAttackOutcome),
    Destroy(EntityNbtTransition),
    Noop(NonLivingAttackKind),
}

impl NonLivingAttackOutcome {
    #[must_use]
    pub fn lifecycle(&self) -> Option<&NonLivingAttackLifecycle> {
        match self {
            Self::ItemFrame(outcome) => Some(&outcome.lifecycle),
            Self::ArmorStand(outcome) | Self::Destroy(outcome) => Some(&outcome.lifecycle),
            Self::Vehicle(outcome) => Some(&outcome.lifecycle),
            Self::Item(outcome) => Some(&outcome.lifecycle),
            Self::Interaction(outcome) => Some(&outcome.lifecycle),
            Self::Noop(_) => None,
        }
    }

    #[must_use]
    pub fn is_noop(&self) -> bool {
        match self {
            Self::ItemFrame(outcome) => {
                outcome.item_before == outcome.item_after
                    && outcome.rotation_before == outcome.rotation_after
                    && outcome.removed_before == outcome.removed_after
                    && outcome.drops.is_empty()
                    && outcome.lifecycle.present_before == outcome.lifecycle.present_after
                    && outcome.lifecycle.spawned.is_empty()
            }
            Self::Destroy(outcome) => {
                outcome.before == outcome.after
                    && outcome.removed_before == outcome.removed_after
                    && outcome.drops.is_empty()
                    && outcome.lifecycle.present_before == outcome.lifecycle.present_after
                    && outcome.lifecycle.spawned.is_empty()
            }
            Self::Noop(_) => true,
            _ => false,
        }
    }

    #[must_use]
    pub fn kind(&self) -> NonLivingAttackKind {
        match self {
            Self::ItemFrame(outcome) => outcome.kind,
            Self::ArmorStand(outcome) | Self::Destroy(outcome) => outcome.kind,
            Self::Vehicle(outcome) => outcome.kind,
            Self::Item(_) => NonLivingAttackKind::Item,
            Self::Interaction(_) => NonLivingAttackKind::Interaction,
            Self::Noop(kind) => *kind,
        }
    }

    #[must_use]
    pub fn is_well_formed(&self) -> bool {
        match self {
            Self::ItemFrame(outcome) => {
                matches!(outcome.kind, NonLivingAttackKind::ItemFrame | NonLivingAttackKind::GlowItemFrame)
                    && outcome.rotation_before < 8
                    && outcome.rotation_after < 8
                    && f32::from_bits(outcome.item_drop_chance_bits).is_finite()
                    && (0.0..=1.0).contains(&f32::from_bits(outcome.item_drop_chance_bits))
                    && attack_stack_is_normalized(&outcome.item_before)
                    && attack_stack_is_normalized(&outcome.item_after)
                    && attack_drops_are_well_formed(&outcome.drops)
                    && attack_lifecycle_is_well_formed(
                        &outcome.lifecycle,
                        outcome.removed_before,
                        outcome.removed_after,
                        &outcome.drops,
                    )
                    && (outcome.fixed
                        || outcome.item_before != outcome.item_after
                        || outcome.rotation_before != outcome.rotation_after
                        || outcome.removed_before != outcome.removed_after
                        || !outcome.drops.is_empty())
            }
            Self::ArmorStand(outcome) | Self::Destroy(outcome) => {
                !outcome.before.is_empty()
                    && !outcome.after.is_empty()
                    && attack_drops_are_well_formed(&outcome.drops)
                    && attack_lifecycle_is_well_formed(
                        &outcome.lifecycle,
                        outcome.removed_before,
                        outcome.removed_after,
                        &outcome.drops,
                    )
                    && (matches!(outcome.kind, NonLivingAttackKind::Marker | NonLivingAttackKind::Display)
                        || outcome.before != outcome.after
                        || outcome.removed_before != outcome.removed_after
                        || !outcome.drops.is_empty())
            }
            Self::Vehicle(outcome) => {
                matches!(
                    outcome.kind,
                    NonLivingAttackKind::Boat
                        | NonLivingAttackKind::ChestBoat
                        | NonLivingAttackKind::Minecart
                        | NonLivingAttackKind::ChestMinecart
                        | NonLivingAttackKind::FurnaceMinecart
                        | NonLivingAttackKind::HopperMinecart
                        | NonLivingAttackKind::TntMinecart
                        | NonLivingAttackKind::CommandBlockMinecart
                        | NonLivingAttackKind::SpawnerMinecart
                ) && f32::from_bits(outcome.damage_before_bits).is_finite()
                    && f32::from_bits(outcome.damage_after_bits).is_finite()
                    && attack_drops_are_well_formed(&outcome.drops)
                    && attack_lifecycle_is_well_formed(
                        &outcome.lifecycle,
                        outcome.removed_before,
                        outcome.removed_after,
                        &outcome.drops,
                    )
                    && (outcome.hurt_time_before != outcome.hurt_time_after
                        || outcome.hurt_dir_before != outcome.hurt_dir_after
                        || outcome.damage_before_bits != outcome.damage_after_bits
                        || outcome.removed_before != outcome.removed_after
                        || !outcome.drops.is_empty())
            }
            Self::Item(outcome) => {
                attack_stack_is_normalized(&outcome.stack_before)
                    && attack_stack_is_normalized(&outcome.stack_after)
                    && f32::from_bits(outcome.health_before_bits).is_finite()
                    && f32::from_bits(outcome.health_after_bits).is_finite()
                    && attack_lifecycle_is_well_formed(
                        &outcome.lifecycle,
                        outcome.removed_before,
                        outcome.removed_after,
                        &[],
                    )
                    && (outcome.stack_before != outcome.stack_after
                        || outcome.health_before_bits != outcome.health_after_bits
                        || outcome.removed_before != outcome.removed_after)
            }
            Self::Interaction(outcome) => {
                !outcome.before.is_empty()
                    && outcome.before != outcome.after
                    && outcome.lifecycle.present_before
                    && outcome.lifecycle.present_after
                    && outcome.lifecycle.spawned.is_empty()
            }
            Self::Noop(_) => true,
        }
    }
}

fn attack_stack_is_normalized(stack: &InventoryStack) -> bool {
    stack == &stack.clone().normalized()
}

fn attack_drops_are_well_formed(drops: &[AttackItemDrop]) -> bool {
    drops.iter().all(|drop| {
        attack_stack_is_normalized(&drop.stack)
            && !drop.stack.is_empty()
            && !drop.entity_nbt.is_empty()
    }) && drops.iter().enumerate().all(|(index, drop)| {
        drops
            .iter()
            .skip(index + 1)
            .all(|other| !drop.entity.same_identity(other.entity))
    })
}

fn attack_lifecycle_is_well_formed(
    lifecycle: &NonLivingAttackLifecycle,
    removed_before: bool,
    removed_after: bool,
    drops: &[AttackItemDrop],
) -> bool {
    lifecycle.present_before == !removed_before
        && lifecycle.present_after == !removed_after
        && lifecycle.spawned == drops
        && attack_drops_are_well_formed(&lifecycle.spawned)
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub enum CapturedAttackTargetOutcome {
    Living(LivingAttackOutcome),
    NonLiving(NonLivingAttackOutcome),
}

impl CapturedAttackTargetOutcome {
    #[must_use]
    pub fn is_noop(&self) -> bool {
        match self {
            Self::Living(outcome) => {
                outcome.raw_damage_milli == 0
                    && outcome.effective_damage_milli == 0
                    && outcome.cooldown_damage_delta_milli == 0
                    && outcome.absorption_damage_milli == 0
                    && outcome.health_damage_milli == 0
                    && outcome.hurt_cooldown_resolution == LivingHurtCooldownResolution::NoDamage
                    && outcome.fire_ticks_before == outcome.fire_ticks_after
                    && outcome.visual_fire_before == outcome.visual_fire_after
                    && outcome.knockback.is_none()
            }
            Self::NonLiving(outcome) => outcome.is_noop(),
        }
    }

    #[must_use]
    pub fn is_well_formed(&self) -> bool {
        match self {
            Self::Living(outcome) => {
                outcome.before.health_milli >= 0
                    && outcome.before.absorption_milli >= 0
                    && outcome.before.hurt_cooldown >= 0
                    && outcome.before.last_damage_taken_milli >= 0
                    && outcome.before.effects.iter().all(|effect| effect.is_valid())
                    && outcome.before.attributes.iter().all(|attribute| {
                        f64::from_bits(attribute.value_bits).is_finite()
                    })
                    && outcome.before.equipment.iter().all(|equipment| {
                        attack_stack_is_normalized(&equipment.item)
                    })
                    && outcome.fire_ticks_before == outcome.before.fire_ticks
                    && outcome.visual_fire_before == outcome.before.visual_fire
                    && outcome
                        .knockback
                        .is_none_or(|knockback| knockback.velocity_after_bits.iter().all(|bits| {
                            f64::from_bits(*bits).is_finite()
                        }))
            }
            Self::NonLiving(outcome) => outcome.is_well_formed(),
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct CapturedAttackTarget {
    pub target: EntityMutationTarget,
    pub outcome: CapturedAttackTargetOutcome,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub enum CombatStatKind {
    Used,
    Broken,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub struct CombatStatTransition {
    pub kind: CombatStatKind,
    pub item: u16,
    pub before: i32,
    pub after: i32,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub enum CombatAdvancement {
    DealtOverkillDamage,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub struct CombatAdvancementTransition {
    pub advancement: CombatAdvancement,
    pub before: bool,
    pub after: bool,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct AttackItemTransition {
    pub slot: InvLoc,
    pub before: InventoryStack,
    pub after: InventoryStack,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct CapturedAttackActorEffects {
    pub cooldown: AttackCooldownTransition,
    pub velocity: AttackVelocityTransition,
    pub last_attacking_id_before: i32,
    pub last_attacking_id_after: i32,
    pub last_attack_tick_before: i32,
    pub last_attack_tick_after: i32,
    pub fall_distance_before_bits: u32,
    pub fall_distance_after_bits: u32,
    pub exhaustion_before_bits: u32,
    pub exhaustion_after_bits: u32,
    pub item: Option<AttackItemTransition>,
    pub stats: Vec<CombatStatTransition>,
    pub advancements: Vec<CombatAdvancementTransition>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub enum CapturedAttackOutcome {
    Landed,
    NoDamage,
}

impl CapturedAttackActorEffects {
    #[must_use]
    pub fn is_noop(&self) -> bool {
        self.cooldown.before == self.cooldown.after
            && self.velocity.before_bits == self.velocity.after_bits
            && self.last_attacking_id_before == self.last_attacking_id_after
            && self.last_attack_tick_before == self.last_attack_tick_after
            && self.fall_distance_before_bits == self.fall_distance_after_bits
            && self.exhaustion_before_bits == self.exhaustion_after_bits
            && self.item.as_ref().is_none_or(|item| item.before == item.after)
            && self.stats.iter().all(|stat| stat.before == stat.after)
            && self.advancements.iter().all(|advancement| advancement.before == advancement.after)
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct CapturedAttack {
    pub actor: ActionActor,
    pub seq: PlayerSeq,
    pub tick: TickStamp,
    pub attacker: EntityRef,
    pub outcome: CapturedAttackOutcome,
    pub primary: CapturedAttackTarget,
    pub sweeping: Vec<CapturedAttackTarget>,
    pub attacker_effects: CapturedAttackActorEffects,
}

impl CapturedAttack {
    #[must_use]
    pub const fn stream_kind() -> StreamKind {
        StreamKind::PlayerCombat
    }

    #[must_use]
    pub fn targets(&self) -> impl Iterator<Item = &CapturedAttackTarget> {
        core::iter::once(&self.primary).chain(self.sweeping.iter())
    }

    #[must_use]
    pub fn is_noop(&self) -> bool {
        self.primary.outcome.is_noop()
            && self.sweeping.iter().all(|target| target.outcome.is_noop())
            && self.attacker_effects.is_noop()
    }
}

/// Projectile launch with kind tag, charge, and unit direction.
///
/// Rides [`StreamKind::PlayerCombat`].
#[derive(Debug, Clone, Copy, PartialEq, Serialize, Deserialize)]
pub struct FireProjectileUpdate {
    pub gid: GlobalPlayerId,
    pub seq: PlayerSeq,
    pub tick: TickStamp,
    pub chunk: ChunkAddr,
    pub kind: u8,
    pub charge_milli: u16,
    pub dir: [f32; 3],
}

impl FireProjectileUpdate {
    #[must_use]
    pub const fn stream_kind() -> StreamKind {
        StreamKind::PlayerCombat
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub enum EntityMutationTarget {
    Entity(EntityRef),
    Player {
        gid: GlobalPlayerId,
        chunk: ChunkAddr,
    },
}

impl EntityMutationTarget {
    #[must_use]
    pub const fn chunk(self) -> ChunkAddr {
        match self {
            Self::Entity(entity) => entity.chunk,
            Self::Player { chunk, .. } => chunk,
        }
    }

    #[must_use]
    pub fn same_identity(self, other: Self) -> bool {
        match (self, other) {
            (Self::Entity(left), Self::Entity(right)) => left.same_identity(right),
            (Self::Player { gid: left, .. }, Self::Player { gid: right, .. }) => left == right,
            _ => false,
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub struct StatusEffectState {
    pub effect: u16,
    pub duration_ticks: i32,
    pub amplifier: u8,
    pub flags: u8,
}

impl StatusEffectState {
    #[must_use]
    pub const fn is_valid(&self) -> bool {
        self.duration_ticks != 0 && self.duration_ticks >= -1
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub enum EntityMutation {
    AddEffect {
        before: Option<StatusEffectState>,
        after: StatusEffectState,
    },
    RemoveEffect {
        before: StatusEffectState,
    },
}

impl EntityMutation {
    #[must_use]
    pub const fn effect(self) -> u16 {
        match self {
            Self::AddEffect { after, .. } => after.effect,
            Self::RemoveEffect { before } => before.effect,
        }
    }

    #[must_use]
    pub const fn expected(self) -> Option<StatusEffectState> {
        match self {
            Self::AddEffect { before, .. } => before,
            Self::RemoveEffect { before } => Some(before),
        }
    }

    #[must_use]
    pub const fn applied(self) -> Option<StatusEffectState> {
        match self {
            Self::AddEffect { after, .. } => Some(after),
            Self::RemoveEffect { .. } => None,
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub struct EntityMutationUpdate {
    pub actor: ActionActor,
    pub seq: ActionSeq,
    pub tick: TickStamp,
    pub target: EntityMutationTarget,
    pub mutation: EntityMutation,
}

impl EntityMutationUpdate {
    #[must_use]
    pub const fn stream_kind(self) -> StreamKind {
        match self.actor {
            ActionActor::Player(_) => StreamKind::PlayerCombat,
            ActionActor::Server(_) => StreamKind::EntityCombat,
        }
    }

    #[must_use]
    pub const fn chunk(self) -> ChunkAddr {
        self.target.chunk()
    }

    #[must_use]
    pub fn same_identity(self, other: Self) -> bool {
        self.actor == other.actor
            && self.seq == other.seq
            && self.tick == other.tick
            && self.target.same_identity(other.target)
            && self.mutation == other.mutation
    }
}

/// One fused tick: `tick` plus one array per update type.
///
/// The fuse drains worker banks into these arrays; `postcard` carries the
/// whole batch as one frame. Keep `is_empty`, `len`, and `append_bank` in
/// sync with the field list.
#[derive(Debug, Clone, Default, PartialEq, Serialize, Deserialize)]
pub struct TickBatch {
    pub tick: TickStamp,
    pub pos: Vec<PosUpdate>,
    pub armor: Vec<ArmorUpdate>,
    pub held: Vec<HeldUpdate>,
    pub sneak: Vec<SneakUpdate>,
    pub sprint: Vec<SprintUpdate>,
    pub blocking: Vec<BlockingUpdate>,
    pub swing: Vec<SwingUpdate>,
    pub skin: Vec<SkinLayersUpdate>,
    pub eat_start: Vec<EatStartUpdate>,
    pub eat_abort: Vec<EatAbortUpdate>,
    pub break_anim: Vec<BreakAnimUpdate>,
    pub break_block: Vec<BreakBlockUpdate>,
    pub place_block: Vec<PlaceBlockUpdate>,
    pub inv_ops: Vec<InventoryOp>,
    pub attacks: Vec<CapturedAttack>,
    pub fire: Vec<FireProjectileUpdate>,
    pub entity_mutations: Vec<EntityMutationUpdate>,
}

impl TickBatch {
    #[must_use]
    pub fn new(tick: TickStamp) -> Self {
        Self {
            tick,
            ..Self::default()
        }
    }

    #[must_use]
    pub fn is_empty(&self) -> bool {
        self.pos.is_empty()
            && self.armor.is_empty()
            && self.held.is_empty()
            && self.sneak.is_empty()
            && self.sprint.is_empty()
            && self.blocking.is_empty()
            && self.swing.is_empty()
            && self.skin.is_empty()
            && self.eat_start.is_empty()
            && self.eat_abort.is_empty()
            && self.break_anim.is_empty()
            && self.break_block.is_empty()
            && self.place_block.is_empty()
            && self.inv_ops.is_empty()
            && self.attacks.is_empty()
            && self.fire.is_empty()
            && self.entity_mutations.is_empty()
    }

    #[must_use]
    pub fn len(&self) -> usize {
        self.pos.len()
            + self.armor.len()
            + self.held.len()
            + self.sneak.len()
            + self.sprint.len()
            + self.blocking.len()
            + self.swing.len()
            + self.skin.len()
            + self.eat_start.len()
            + self.eat_abort.len()
            + self.break_anim.len()
            + self.break_block.len()
            + self.place_block.len()
            + self.inv_ops.len()
            + self.attacks.len()
            + self.fire.len()
            + self.entity_mutations.len()
    }

    pub fn append_bank(&mut self, bank: &crate::banks::Bank) {
        self.pos.extend_from_slice(&bank.pos);
        self.armor.extend_from_slice(&bank.armor);
        self.held.extend_from_slice(&bank.held);
        self.sneak.extend_from_slice(&bank.sneak);
        self.sprint.extend_from_slice(&bank.sprint);
        self.blocking.extend_from_slice(&bank.blocking);
        self.swing.extend_from_slice(&bank.swing);
        self.skin.extend_from_slice(&bank.skin);
        self.eat_start.extend_from_slice(&bank.eat_start);
        self.eat_abort.extend_from_slice(&bank.eat_abort);
        self.break_anim.extend_from_slice(&bank.break_anim);
        self.break_block.extend_from_slice(&bank.break_block);
        self.place_block.extend_from_slice(&bank.place_block);
        self.inv_ops.extend_from_slice(&bank.inv_ops);
        self.attacks.extend_from_slice(&bank.attacks);
        self.fire.extend_from_slice(&bank.fire);
        self.entity_mutations
            .extend_from_slice(&bank.entity_mutations);
    }
}

/// Mesh uni-stream family: one variant per update family plus plumbing.
///
/// Player kinds are per `(peer, player)`; entity, control, chunk, and accept
/// kinds are shared per connection. Each player update's `stream_kind()`
/// names its family here, so routing is declared next to the payload.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub enum StreamKind {
    PlayerVisual,
    PlayerTransient,
    PlayerWorld,
    PlayerCombat,
    EntityPos,
    EntityVisual,
    EntityTransient,
    EntityCombat,
    Control,
    ChunkRequest,
    ChunkData,
    ChunkAdvert,
    Accept,
    PrimaryTick,
}

impl StreamKind {
    #[must_use]
    pub const fn is_player(self) -> bool {
        matches!(
            self,
            Self::PlayerVisual | Self::PlayerTransient | Self::PlayerWorld | Self::PlayerCombat
        )
    }

    #[must_use]
    pub const fn is_entity(self) -> bool {
        matches!(
            self,
            Self::EntityPos | Self::EntityVisual | Self::EntityTransient | Self::EntityCombat
        )
    }

    #[must_use]
    pub const fn is_chunk(self) -> bool {
        matches!(self, Self::ChunkRequest | Self::ChunkData)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::identity::ServerId;

    #[test]
    fn item_frame_attack_outcome_keeps_full_stack_nbt_and_drop_identity() {
        let frame = EntityRef {
            origin: ServerId(1),
            owner: ServerId(1),
            local_id: 44,
            chunk: ChunkAddr { x: 4, z: -3 },
        };
        let drop = EntityRef {
            origin: ServerId(1),
            owner: ServerId(1),
            local_id: 45,
            chunk: ChunkAddr { x: 4, z: -3 },
        };
        let stack = InventoryStack {
            item: 17,
            count: 1,
            nbt: vec![10, 0, 4, b't', b'e', b's', b't', 1, 0, 0],
        };
        let target = CapturedAttackTarget {
            target: EntityMutationTarget::Entity(frame),
            outcome: CapturedAttackTargetOutcome::NonLiving(NonLivingAttackOutcome::ItemFrame(
                ItemFrameAttackOutcome {
                    kind: NonLivingAttackKind::ItemFrame,
                    fixed: false,
                    item_drop_chance_bits: 1.0f32.to_bits(),
                    item_before: stack.clone(),
                    item_after: InventoryStack::empty(),
                    rotation_before: 7,
                    rotation_after: 7,
                    removed_before: false,
                    removed_after: false,
                    drops: vec![AttackItemDrop {
                        entity: drop,
                        stack: stack.clone(),
                        entity_nbt: vec![1, 2, 3, 4],
                    }],
                    lifecycle: NonLivingAttackLifecycle {
                        present_before: true,
                        present_after: true,
                        spawned: vec![AttackItemDrop {
                            entity: drop,
                            stack: stack.clone(),
                            entity_nbt: vec![1, 2, 3, 4],
                        }],
                    },
                },
            )),
        };
        assert!(target.outcome.is_well_formed());
        let bytes = postcard::to_allocvec(&target).unwrap();
        let decoded: CapturedAttackTarget = postcard::from_bytes(&bytes).unwrap();
        assert_eq!(decoded, target);
        let CapturedAttackTargetOutcome::NonLiving(NonLivingAttackOutcome::ItemFrame(outcome)) = decoded.outcome else {
            panic!();
        };
        assert_eq!(outcome.item_before.nbt, stack.nbt);
        assert_eq!(outcome.drops[0].entity, drop);
        assert_eq!(outcome.drops[0].stack.nbt, stack.nbt);
    }

    #[test]
    fn invulnerable_nonliving_target_is_explicit_noop() {
        let outcome = CapturedAttackTargetOutcome::NonLiving(NonLivingAttackOutcome::Noop(
            NonLivingAttackKind::EndCrystal,
        ));
        assert!(outcome.is_noop());
        assert_eq!(
            match &outcome {
                CapturedAttackTargetOutcome::NonLiving(outcome) => outcome.kind(),
                CapturedAttackTargetOutcome::Living(_) => unreachable!(),
            },
            NonLivingAttackKind::EndCrystal
        );
    }
}
