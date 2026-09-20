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

use crate::identity::{GlobalPlayerId, PlayerSeq, ServerId};
use crate::inventory::InventoryOp;
use crate::time::TickStamp;

pub const INV_OP_SEED: u64 = 0x9E37_79B9_7F4A_7C15;

/// Chunk column address in chunk units (`x`, `z`).
///
/// Block `(bx, bz)` maps to chunk `(bx.div_euclid(16), bz.div_euclid(16))`.
/// Chunk-scoped updates (`break` / `place`) and chunk fetch carry this so
/// holders, targets, and waiters agree without recomputing.
#[derive(
    Debug, Clone, Copy, PartialEq, Eq, Hash, PartialOrd, Ord, Serialize, Deserialize,
)]
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
    pub owner: ServerId,
    pub local_id: i32,
    pub chunk: ChunkAddr,
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

/// Player-on-player hit with millidamage and explicit target.
///
/// Rides [`StreamKind::PlayerCombat`].
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub struct HitPlayerUpdate {
    pub gid: GlobalPlayerId,
    pub seq: PlayerSeq,
    pub tick: TickStamp,
    pub target: GlobalPlayerId,
    pub damage_milli: u16,
}

impl HitPlayerUpdate {
    #[must_use]
    pub const fn stream_kind() -> StreamKind {
        StreamKind::PlayerCombat
    }
}

/// Player-on-entity hit with millidamage and explicit [`EntityRef`].
///
/// Rides [`StreamKind::PlayerCombat`].
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub struct HitEntityUpdate {
    pub gid: GlobalPlayerId,
    pub seq: PlayerSeq,
    pub tick: TickStamp,
    pub target: EntityRef,
    pub damage_milli: u16,
}

impl HitEntityUpdate {
    #[must_use]
    pub const fn stream_kind() -> StreamKind {
        StreamKind::PlayerCombat
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
    pub hit_player: Vec<HitPlayerUpdate>,
    pub hit_entity: Vec<HitEntityUpdate>,
    pub fire: Vec<FireProjectileUpdate>,
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
            && self.hit_player.is_empty()
            && self.hit_entity.is_empty()
            && self.fire.is_empty()
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
            + self.hit_player.len()
            + self.hit_entity.len()
            + self.fire.len()
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
        self.hit_player.extend_from_slice(&bank.hit_player);
        self.hit_entity.extend_from_slice(&bank.hit_entity);
        self.fire.extend_from_slice(&bank.fire);
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
}

impl StreamKind {
    #[must_use]
    pub const fn is_player(self) -> bool {
        matches!(
            self,
            Self::PlayerVisual
                | Self::PlayerTransient
                | Self::PlayerWorld
                | Self::PlayerCombat
        )
    }

    #[must_use]
    pub const fn is_entity(self) -> bool {
        matches!(
            self,
            Self::EntityPos
                | Self::EntityVisual
                | Self::EntityTransient
                | Self::EntityCombat
        )
    }

    #[must_use]
    pub const fn is_chunk(self) -> bool {
        matches!(self, Self::ChunkRequest | Self::ChunkData)
    }
}
