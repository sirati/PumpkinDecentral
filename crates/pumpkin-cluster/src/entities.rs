//! Entity routing policy, kept next to the ghost and owner tables so the rules
//! stay readable where they are enforced.
//!
//! - Routing key is the entity's current [`ChunkAddr`]: updates flow to the
//!   chunk's holders, never as a mesh-wide broadcast.
//! - Owner gating applies on top: a peer may only speak for entities whose
//!   [`EntityRef::owner`] matches the sending peer, and never for entities
//!   owned by the local peer (loopback is a bug, not a fast path).
//! - One shared stream per update family ([`ENTITY_STREAM_KINDS`]): pos,
//!   visual, transient, and combat each ride exactly one [`StreamKind`].
//!   There are no per-player entity streams.
//! - Inbound path is `should_accept` (owner + holder); outbound path is
//!   `fanout_to_holders` (holders minus the sender). No holders means no
//!   targets, never a broadcast fallback.

use std::collections::HashMap;

use serde::{Deserialize, Serialize};

use crate::identity::ServerId;
use crate::time::TickStamp;

pub use crate::protocol::{
    ChunkAddr, EntityRef, FireProjectileUpdate, HitEntityUpdate, StreamKind,
};

/// Exactly one shared stream per entity update family.
///
/// One [`StreamKind`] per update struct, shared by every peer. Entity traffic
/// must not open per-player streams: the demux already funnels all four kinds
/// into a single entity queue, and per-player fanout would break that back
/// pressure boundary.
pub const ENTITY_STREAM_KINDS: [StreamKind; 4] = [
    StreamKind::EntityPos,
    StreamKind::EntityVisual,
    StreamKind::EntityTransient,
    StreamKind::EntityCombat,
];

impl EntityPosUpdate {
    /// Single stream carrying position updates.
    #[must_use]
    pub const fn stream_kind() -> StreamKind {
        StreamKind::EntityPos
    }
}

impl EntityVisualUpdate {
    /// Single stream carrying visual updates.
    #[must_use]
    pub const fn stream_kind() -> StreamKind {
        StreamKind::EntityVisual
    }
}

impl EntityTransientUpdate {
    /// Single stream carrying transient updates.
    #[must_use]
    pub const fn stream_kind() -> StreamKind {
        StreamKind::EntityTransient
    }
}

impl EntityCombatUpdate {
    /// Single stream carrying combat updates.
    #[must_use]
    pub const fn stream_kind() -> StreamKind {
        StreamKind::EntityCombat
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Serialize, Deserialize)]
pub struct EntitySpawn {
    pub entity: EntityRef,
    pub tick: TickStamp,
    pub kind: u8,
    pub pos: [f64; 3],
    pub yaw: f32,
    pub pitch: f32,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub struct EntityDespawn {
    pub entity: EntityRef,
    pub tick: TickStamp,
}

#[derive(Debug, Clone, Copy, PartialEq, Serialize, Deserialize)]
pub struct EntityPosUpdate {
    pub entity: EntityRef,
    pub tick: TickStamp,
    pub pos: [f64; 3],
    pub vel: [f64; 3],
    pub yaw: f32,
    pub pitch: f32,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub struct EntityVisualUpdate {
    pub entity: EntityRef,
    pub tick: TickStamp,
    pub slot: u8,
    pub item: u16,
    pub flags: u8,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub struct EntityTransientUpdate {
    pub entity: EntityRef,
    pub tick: TickStamp,
    pub action: u8,
    pub value: u16,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub struct EntityCombatUpdate {
    pub entity: EntityRef,
    pub tick: TickStamp,
    pub kind: u8,
    pub amount: u16,
}

#[derive(Debug, Clone, Copy, PartialEq)]
pub struct GhostState {
    pub chunk: ChunkAddr,
    pub last_tick: TickStamp,
    pub pos: [f64; 3],
    pub yaw: f32,
    pub pitch: f32,
    pub kind: u8,
}

#[derive(Debug, Clone, Default, PartialEq)]
pub struct EntityGhosts {
    pub by_ref: HashMap<EntityRef, GhostState>,
}

impl EntityGhosts {
    #[must_use]
    pub fn new() -> Self {
        Self::default()
    }

    #[must_use]
    pub fn is_empty(&self) -> bool {
        self.by_ref.is_empty()
    }

    #[must_use]
    pub fn len(&self) -> usize {
        self.by_ref.len()
    }

    #[must_use]
    pub fn get(&self, entity: &EntityRef) -> Option<&GhostState> {
        self.by_ref.get(entity)
    }

    #[must_use]
    pub fn get_owned(&self, owner: ServerId, local_id: i32) -> Option<&GhostState> {
        let key = self.locate_key(owner, local_id)?;
        self.by_ref.get(&key)
    }

    #[must_use]
    pub fn contains(&self, owner: ServerId, local_id: i32) -> bool {
        self.locate_key(owner, local_id).is_some()
    }

    pub fn spawn(&mut self, update: &EntitySpawn) -> bool {
        let previous = self.locate_key(update.entity.owner, update.entity.local_id);
        if let Some(stale) = previous {
            let _evicted: Option<GhostState> = self.by_ref.remove(&stale);
        }
        let fresh = previous.is_none();
        self.by_ref.insert(
            update.entity,
            GhostState {
                chunk: update.entity.chunk,
                last_tick: update.tick,
                pos: update.pos,
                yaw: update.yaw,
                pitch: update.pitch,
                kind: update.kind,
            },
        );
        fresh
    }

    pub fn apply_pos(&mut self, update: &EntityPosUpdate) -> bool {
        let Some(state) = self.state_for(&update.entity, update.tick) else {
            return false;
        };
        state.pos = update.pos;
        state.yaw = update.yaw;
        state.pitch = update.pitch;
        true
    }

    pub fn apply_visual(&mut self, update: &EntityVisualUpdate) -> bool {
        self.state_for(&update.entity, update.tick).is_some()
    }

    pub fn apply_transient(&mut self, update: &EntityTransientUpdate) -> bool {
        self.state_for(&update.entity, update.tick).is_some()
    }

    pub fn apply_combat(&mut self, update: &EntityCombatUpdate) -> bool {
        self.state_for(&update.entity, update.tick).is_some()
    }

    pub fn despawn(&mut self, update: &EntityDespawn) -> bool {
        let previous = self.locate_key(update.entity.owner, update.entity.local_id);
        let Some(stale) = previous else {
            return false;
        };
        let _evicted: Option<GhostState> = self.by_ref.remove(&stale);
        true
    }

    fn locate_key(&self, owner: ServerId, local_id: i32) -> Option<EntityRef> {
        self.by_ref
            .keys()
            .find(|key| key.owner == owner && key.local_id == local_id)
            .copied()
    }

    fn state_for(&mut self, entity: &EntityRef, tick: TickStamp) -> Option<&mut GhostState> {
        let key = self.locate_key(entity.owner, entity.local_id)?;
        let previous = self.by_ref.remove(&key)?;
        let mut state = previous;
        state.chunk = entity.chunk;
        state.last_tick = tick;
        Some(self.by_ref.entry(*entity).or_insert(state))
    }
}

#[derive(Debug, Clone, PartialEq)]
pub struct OwnerTable {
    pub local_peer: ServerId,
    pub entries: HashMap<(ServerId, i32), ChunkAddr>,
}

impl OwnerTable {
    #[must_use]
    pub fn new(local_peer: ServerId) -> Self {
        Self {
            local_peer,
            entries: HashMap::new(),
        }
    }

    #[must_use]
    pub fn is_empty(&self) -> bool {
        self.entries.is_empty()
    }

    #[must_use]
    pub fn len(&self) -> usize {
        self.entries.len()
    }

    pub fn insert(&mut self, local_id: i32, chunk: ChunkAddr) {
        self.entries.insert((self.local_peer, local_id), chunk);
    }

    #[must_use]
    pub fn chunk_of(&self, local_id: i32) -> Option<ChunkAddr> {
        self.entries.get(&(self.local_peer, local_id)).copied()
    }

    pub fn note_moved(&mut self, local_id: i32, chunk: ChunkAddr) -> bool {
        let Some(entry) = self.entries.get_mut(&(self.local_peer, local_id)) else {
            return false;
        };
        *entry = chunk;
        true
    }

    pub fn remove(&mut self, local_id: i32) -> Option<ChunkAddr> {
        self.entries.remove(&(self.local_peer, local_id))
    }

    /// Ownership handoff targets the lowest holder besides self, per entity.
    ///
    /// Entities without another holder are omitted: with no holder there is
    /// nobody to hand to, and handoff must not fall back to broadcast.
    #[must_use]
    pub fn plan_handoff(
        &self,
        holders: &dyn Fn(ChunkAddr) -> Vec<u16>,
    ) -> Vec<EntityHandoff> {
        let mut out = Vec::new();
        for ((owner, local_id), chunk) in &self.entries {
            let mut candidates: Vec<u16> = holders(*chunk)
                .into_iter()
                .filter(|peer| *peer != self.local_peer.0)
                .collect();
            candidates.sort_unstable();
            candidates.dedup();
            if let Some(&target_peer) = candidates.first() {
                out.push(EntityHandoff {
                    entity_ref: EntityRef {
                        owner: *owner,
                        local_id: *local_id,
                        chunk: *chunk,
                    },
                    target_peer,
                });
            }
        }
        out.sort_by(|left, right| {
            (left.entity_ref.owner, left.entity_ref.local_id)
                .cmp(&(right.entity_ref.owner, right.entity_ref.local_id))
        });
        out
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub struct EntityHandoff {
    pub entity_ref: EntityRef,
    pub target_peer: u16,
}

/// Owner gating: only the recorded owner may speak for an entity.
///
/// Rejects loopback (`owner == local`) and spoofed senders (`peer != owner`)
/// before any chunk or ghost lookup happens.
#[must_use]
pub fn is_owner_sender(peer: ServerId, owner: ServerId, local: ServerId) -> bool {
    if owner == local {
        return false;
    }
    peer == owner
}

/// Holder gating: the local peer only applies updates for chunks it holds.
///
/// `directory` is the chunk-holder index; membership is checked after
/// canonical [`route_to_holders`] sorting so callers and tests share one
/// definition of "holder".
#[must_use]
pub fn is_holder(
    chunk: ChunkAddr,
    local: ServerId,
    directory: &dyn Fn(ChunkAddr) -> Vec<u16>,
) -> bool {
    route_to_holders(chunk, directory).contains(&local.0)
}

/// Inbound gate: owner check first, holder check second.
///
/// Accept an update only when the sender owns the entity and the local peer
/// holds the entity's current chunk. Argument order mirrors the check order.
#[must_use]
pub fn should_accept(
    peer: ServerId,
    local: ServerId,
    entity: &EntityRef,
    directory: &dyn Fn(ChunkAddr) -> Vec<u16>,
) -> bool {
    if !is_owner_sender(peer, entity.owner, local) {
        return false;
    }
    is_holder(entity.chunk, local, directory)
}

/// Canonical holder list for a chunk: sorted, deduplicated, no broadcast.
///
/// This is the only routing query outbound fanout may use. An empty holder
/// set means "drop", never "send to everyone".
#[must_use]
pub fn route_to_holders(
    chunk: ChunkAddr,
    directory: &dyn Fn(ChunkAddr) -> Vec<u16>,
) -> Vec<u16> {
    let mut holders = directory(chunk);
    holders.sort_unstable();
    holders.dedup();
    holders
}

/// Outbound fanout: holders of `chunk`, minus the sending peer.
///
/// Updates go only to holders. The sender (usually the owner) is excluded so
/// a peer never echoes an update back to itself; non-holders are never added.
#[must_use]
pub fn fanout_to_holders(
    chunk: ChunkAddr,
    directory: &dyn Fn(ChunkAddr) -> Vec<u16>,
    exclude: ServerId,
) -> Vec<u16> {
    route_to_holders(chunk, directory)
        .into_iter()
        .filter(|peer| *peer != exclude.0)
        .collect()
}

#[cfg(test)]
mod tests {
    use super::*;

    fn chunk(x: i32, z: i32) -> ChunkAddr {
        ChunkAddr { x, z }
    }

    fn entity_ref(owner: u16, local_id: i32, x: i32, z: i32) -> EntityRef {
        EntityRef {
            owner: ServerId(owner),
            local_id,
            chunk: chunk(x, z),
        }
    }

    fn spawn_update(owner: u16, local_id: i32, x: i32, z: i32) -> EntitySpawn {
        EntitySpawn {
            entity: entity_ref(owner, local_id, x, z),
            tick: TickStamp(10),
            kind: 3,
            pos: [1.0, 2.0, 3.0],
            yaw: 90.0,
            pitch: 0.0,
        }
    }

    fn pos_update(owner: u16, local_id: i32, x: i32, z: i32) -> EntityPosUpdate {
        EntityPosUpdate {
            entity: entity_ref(owner, local_id, x, z),
            tick: TickStamp(11),
            pos: [4.0, 5.0, 6.0],
            vel: [0.1, 0.0, 0.0],
            yaw: 180.0,
            pitch: 5.0,
        }
    }

    #[test]
    fn spawn_registers_ghost() {
        let mut ghosts = EntityGhosts::new();
        assert!(ghosts.spawn(&spawn_update(2, 7, 0, 0)));
        assert!(!ghosts.spawn(&spawn_update(2, 7, 0, 0)));
        assert_eq!(ghosts.len(), 1);
        assert!(ghosts.contains(ServerId(2), 7));
        let state = ghosts.get(&entity_ref(2, 7, 0, 0));
        assert!(state.is_some());
        assert_eq!(state.map(|state| state.kind), Some(3));
    }

    #[test]
    fn pos_update_moves_ghost_across_chunks() {
        let mut ghosts = EntityGhosts::new();
        assert!(ghosts.spawn(&spawn_update(2, 7, 0, 0)));
        assert!(ghosts.apply_pos(&pos_update(2, 7, 1, 1)));
        assert!(ghosts.get(&entity_ref(2, 7, 0, 0)).is_none());
        let owned = ghosts.get_owned(ServerId(2), 7);
        assert!(owned.is_some());
        assert_eq!(owned.map(|state| state.chunk), Some(chunk(1, 1)));
        assert_eq!(owned.map(|state| state.pos), Some([4.0, 5.0, 6.0]));
    }

    #[test]
    fn all_four_streams_refresh_liveness() {
        let mut ghosts = EntityGhosts::new();
        assert!(ghosts.spawn(&spawn_update(2, 7, 0, 0)));
        let visual = EntityVisualUpdate {
            entity: entity_ref(2, 7, 0, 0),
            tick: TickStamp(12),
            slot: 1,
            item: 42,
            flags: 0,
        };
        let transient = EntityTransientUpdate {
            entity: entity_ref(2, 7, 0, 0),
            tick: TickStamp(13),
            action: 2,
            value: 9,
        };
        let combat = EntityCombatUpdate {
            entity: entity_ref(2, 7, 0, 0),
            tick: TickStamp(14),
            kind: 1,
            amount: 500,
        };
        assert!(ghosts.apply_visual(&visual));
        assert!(ghosts.apply_transient(&transient));
        assert!(ghosts.apply_combat(&combat));
        assert!(ghosts.apply_pos(&pos_update(2, 7, 0, 0)));
        assert_eq!(
            ghosts
                .get_owned(ServerId(2), 7)
                .map(|state| state.last_tick),
            Some(TickStamp(11))
        );
    }

    #[test]
    fn updates_for_unknown_ghost_fail() {
        let mut ghosts = EntityGhosts::new();
        assert!(!ghosts.apply_pos(&pos_update(2, 7, 0, 0)));
        assert!(!ghosts.apply_visual(&EntityVisualUpdate {
            entity: entity_ref(2, 7, 0, 0),
            tick: TickStamp(12),
            slot: 0,
            item: 0,
            flags: 0,
        }));
        assert!(!ghosts.apply_transient(&EntityTransientUpdate {
            entity: entity_ref(2, 7, 0, 0),
            tick: TickStamp(13),
            action: 0,
            value: 0,
        }));
        assert!(!ghosts.apply_combat(&EntityCombatUpdate {
            entity: entity_ref(2, 7, 0, 0),
            tick: TickStamp(14),
            kind: 0,
            amount: 0,
        }));
        assert!(!ghosts.despawn(&EntityDespawn {
            entity: entity_ref(2, 7, 0, 0),
            tick: TickStamp(15),
        }));
    }

    #[test]
    fn despawn_removes_ghost() {
        let mut ghosts = EntityGhosts::new();
        assert!(ghosts.spawn(&spawn_update(2, 7, 0, 0)));
        assert!(ghosts.despawn(&EntityDespawn {
            entity: entity_ref(2, 7, 0, 0),
            tick: TickStamp(15),
        }));
        assert!(ghosts.is_empty());
        assert!(!ghosts.despawn(&EntityDespawn {
            entity: entity_ref(2, 7, 0, 0),
            tick: TickStamp(16),
        }));
    }

    #[test]
    fn fanout_reaches_only_holders() {
        let directory = |addr: ChunkAddr| {
            if addr == chunk(0, 0) {
                vec![3_u16, 2, 3]
            } else {
                Vec::new()
            }
        };
        assert_eq!(route_to_holders(chunk(0, 0), &directory), vec![2, 3]);
        assert!(route_to_holders(chunk(9, 9), &directory).is_empty());
    }

    #[test]
    fn handoff_targets_lowest_holder_besides_self() {
        let mut owners = OwnerTable::new(ServerId(1));
        owners.insert(7, chunk(0, 0));
        owners.insert(8, chunk(0, 0));
        owners.insert(9, chunk(5, 5));
        owners.insert(10, chunk(9, 9));
        let holders = |addr: ChunkAddr| {
            if addr == chunk(0, 0) {
                vec![3_u16, 1, 2]
            } else if addr == chunk(5, 5) {
                vec![1]
            } else {
                Vec::new()
            }
        };
        let plan = owners.plan_handoff(&holders);
        assert_eq!(plan.len(), 2);
        assert_eq!(plan[0].entity_ref.local_id, 7);
        assert_eq!(plan[0].target_peer, 2);
        assert_eq!(plan[1].entity_ref.local_id, 8);
        assert_eq!(plan[1].target_peer, 2);
    }

    #[test]
    fn single_stream_per_update_family() {
        assert_eq!(ENTITY_STREAM_KINDS.len(), 4);
        assert_eq!(EntityPosUpdate::stream_kind(), StreamKind::EntityPos);
        assert_eq!(
            EntityVisualUpdate::stream_kind(),
            StreamKind::EntityVisual
        );
        assert_eq!(
            EntityTransientUpdate::stream_kind(),
            StreamKind::EntityTransient
        );
        assert_eq!(
            EntityCombatUpdate::stream_kind(),
            StreamKind::EntityCombat
        );
        let mut kinds = ENTITY_STREAM_KINDS.to_vec();
        kinds.sort_by_key(|kind| *kind as u8);
        kinds.dedup();
        assert_eq!(kinds.len(), 4);
    }

    #[test]
    fn owner_gating_accepts_only_owner_sender() {
        let local = ServerId(1);
        assert!(is_owner_sender(ServerId(2), ServerId(2), local));
        assert!(!is_owner_sender(ServerId(3), ServerId(2), local));
        assert!(!is_owner_sender(ServerId(1), ServerId(1), local));
        assert!(!should_accept(ServerId(1), local, &entity_ref(1, 7, 0, 0), &|_| vec![1_u16]));
    }

    #[test]
    fn inbound_gate_needs_owner_and_holder() {
        let local = ServerId(1);
        let directory = |addr: ChunkAddr| {
            if addr == chunk(0, 0) {
                vec![1_u16, 2]
            } else {
                vec![2_u16]
            }
        };
        let held = entity_ref(2, 7, 0, 0);
        assert!(should_accept(ServerId(2), local, &held, &directory));
        assert!(!should_accept(ServerId(3), local, &held, &directory));
        let elsewhere = entity_ref(2, 7, 9, 9);
        assert!(!should_accept(ServerId(2), local, &elsewhere, &directory));
        let loopback = entity_ref(1, 7, 0, 0);
        assert!(!should_accept(local, local, &loopback, &directory));
    }

    #[test]
    fn fanout_sends_only_to_holders_minus_sender() {
        let directory = |addr: ChunkAddr| {
            if addr == chunk(0, 0) {
                vec![3_u16, 1, 2, 2]
            } else {
                Vec::new()
            }
        };
        assert_eq!(
            fanout_to_holders(chunk(0, 0), &directory, ServerId(1)),
            vec![2, 3]
        );
        assert!(fanout_to_holders(chunk(9, 9), &directory, ServerId(1)).is_empty());
    }

    #[test]
    fn owner_table_tracks_local_entities() {
        let mut owners = OwnerTable::new(ServerId(1));
        assert!(owners.is_empty());
        owners.insert(7, chunk(0, 0));
        assert_eq!(owners.chunk_of(7), Some(chunk(0, 0)));
        assert!(owners.note_moved(7, chunk(1, 1)));
        assert_eq!(owners.chunk_of(7), Some(chunk(1, 1)));
        assert!(!owners.note_moved(8, chunk(1, 1)));
        assert_eq!(owners.len(), 1);
        assert_eq!(owners.remove(7), Some(chunk(1, 1)));
        assert!(owners.is_empty());
    }
}
