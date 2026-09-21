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

use crate::identity::{GlobalPlayerId, ServerId};
use crate::presence::{PresenceProperty, is_valid_presence_name, is_valid_presence_property};
use crate::protocol::PlayerGameMode;
use crate::time::TickStamp;

pub use crate::protocol::{
    ChunkAddr, EntityRef, FireProjectileUpdate, StreamKind,
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

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct ItemStackState {
    pub item_id: u16,
    pub count: u8,
    pub nbt: Vec<u8>,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct PlayerEntityState {
    pub velocity: [f64; 3],
    pub on_ground: bool,
    pub flags: u8,
    pub fire_ticks: i32,
    pub health_milli: u16,
    pub absorption_milli: u16,
    pub fall_distance_milli: i32,
    pub food: u8,
    pub saturation_milli: u16,
    pub experience_level: i32,
    pub experience_progress_milli: u16,
    pub experience_points: i32,
    pub entity_nbt: Vec<u8>,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct PlayerSpawnState {
    pub gid: GlobalPlayerId,
    pub uuid: [u8; 16],
    pub name: String,
    pub properties: Vec<PresenceProperty>,
    pub gamemode: PlayerGameMode,
    pub source_reserved_entity_id: i32,
    pub world: String,
    pub dimension: String,
    pub entity: PlayerEntityState,
}

impl PlayerSpawnState {
    #[must_use]
    pub fn is_authenticated_for(&self, entity: EntityRef) -> bool {
        self.gid.server == entity.origin
            && entity.owner == entity.origin
            && is_valid_presence_name(&self.name)
            && self.properties.iter().all(is_valid_presence_property)
            && !self.world.is_empty()
            && !self.dimension.is_empty()
    }
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub enum EntitySpawnState {
    Entity {
        nbt: Vec<u8>,
    },
    ItemDrop {
        stack: ItemStackState,
        entity_nbt: Vec<u8>,
    },
    Player(PlayerSpawnState),
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct EntitySpawn {
    pub entity: EntityRef,
    pub tick: TickStamp,
    pub kind: u16,
    pub pos: [f64; 3],
    pub yaw: f32,
    pub pitch: f32,
    pub state: EntitySpawnState,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct EntityCodecError {
    pub message: String,
}

impl core::fmt::Display for EntityCodecError {
    fn fmt(&self, f: &mut core::fmt::Formatter<'_>) -> core::fmt::Result {
        f.write_str(&self.message)
    }
}

impl std::error::Error for EntityCodecError {}

fn entity_codec_error(context: &str, error: postcard::Error) -> EntityCodecError {
    EntityCodecError {
        message: format!("{context}: {error}"),
    }
}

pub fn encode_entity_spawn(update: &EntitySpawn) -> Result<Vec<u8>, EntityCodecError> {
    postcard::to_allocvec(update).map_err(|error| entity_codec_error("encode entity spawn", error))
}

pub fn decode_entity_spawn(bytes: &[u8]) -> Result<EntitySpawn, EntityCodecError> {
    postcard::from_bytes(bytes).map_err(|error| entity_codec_error("decode entity spawn", error))
}

pub fn decode_entity_spawn_prefix(
    bytes: &[u8],
) -> Result<(EntitySpawn, &[u8]), EntityCodecError> {
    postcard::take_from_bytes(bytes)
        .map_err(|error| entity_codec_error("decode entity spawn", error))
}

pub fn encode_entity_spawn_into(
    update: &EntitySpawn,
    out: Vec<u8>,
) -> Result<Vec<u8>, EntityCodecError> {
    postcard::to_extend(update, out)
        .map_err(|error| entity_codec_error("encode entity spawn", error))
}

pub fn encode_entity_spawn_to_slice<'out>(
    update: &EntitySpawn,
    out: &'out mut [u8],
) -> Result<&'out mut [u8], EntityCodecError> {
    postcard::to_slice(update, out).map_err(|error| entity_codec_error("encode entity spawn", error))
}

pub fn encoded_entity_spawn_len(update: &EntitySpawn) -> Result<usize, EntityCodecError> {
    postcard::experimental::serialized_size(update)
        .map_err(|error| entity_codec_error("size entity spawn", error))
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub struct EntityOrigin {
    pub server: ServerId,
    pub local_id: i32,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct EntityHandoff {
    pub origin: EntityOrigin,
    pub previous_owner: ServerId,
    pub successor: ServerId,
    pub spawn: EntitySpawn,
    pub velocity: [f64; 3],
}

pub const ENTITY_BOUNDARY_HANDOFF_MAGIC: u32 = 0x4548_4f46;

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub enum EntityBoundaryHandoff {
    Prepare(EntityHandoff),
    Confirm {
        origin: EntityOrigin,
        previous_owner: ServerId,
        successor: ServerId,
    },
    Reject {
        origin: EntityOrigin,
        previous_owner: ServerId,
        successor: ServerId,
    },
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct TaggedEntityBoundaryHandoff {
    pub magic: u32,
    pub frame: EntityBoundaryHandoff,
}

pub fn encode_entity_boundary_handoff(
    frame: &EntityBoundaryHandoff,
) -> Result<Vec<u8>, EntityCodecError> {
    postcard::to_allocvec(&TaggedEntityBoundaryHandoff {
        magic: ENTITY_BOUNDARY_HANDOFF_MAGIC,
        frame: frame.clone(),
    })
    .map_err(|error| entity_codec_error("encode entity boundary handoff", error))
}

pub fn decode_entity_boundary_handoff(
    bytes: &[u8],
) -> Result<EntityBoundaryHandoff, EntityCodecError> {
    let tagged: TaggedEntityBoundaryHandoff = postcard::from_bytes(bytes)
        .map_err(|error| entity_codec_error("decode entity boundary handoff", error))?;
    if tagged.magic != ENTITY_BOUNDARY_HANDOFF_MAGIC {
        return Err(EntityCodecError {
            message: "decode entity boundary handoff: unexpected magic".to_owned(),
        });
    }
    Ok(tagged.frame)
}

impl EntityHandoff {
    #[must_use]
    pub fn destination_ref(&self) -> EntityRef {
        EntityRef {
            origin: self.origin.server,
            owner: self.successor,
            local_id: self.origin.local_id,
            chunk: self.spawn.entity.chunk,
        }
    }

    #[must_use]
    pub fn destination_spawn(&self) -> EntitySpawn {
        let mut spawn = self.spawn.clone();
        spawn.entity = self.destination_ref();
        spawn
    }

    #[must_use]
    pub fn is_consistent(&self) -> bool {
        self.origin.server == self.spawn.entity.origin
            && self.origin.local_id == self.spawn.entity.local_id
            && self.previous_owner == self.spawn.entity.owner
            && self.previous_owner != self.successor
    }

    #[must_use]
    pub fn applies_to(&self, peer: ServerId) -> bool {
        self.successor == peer
    }
}

pub fn encode_entity_handoff(update: &EntityHandoff) -> Result<Vec<u8>, EntityCodecError> {
    postcard::to_allocvec(update)
        .map_err(|error| entity_codec_error("encode entity handoff", error))
}

pub fn decode_entity_handoff(bytes: &[u8]) -> Result<EntityHandoff, EntityCodecError> {
    postcard::from_bytes(bytes).map_err(|error| entity_codec_error("decode entity handoff", error))
}

pub fn decode_entity_handoff_prefix(
    bytes: &[u8],
) -> Result<(EntityHandoff, &[u8]), EntityCodecError> {
    postcard::take_from_bytes(bytes)
        .map_err(|error| entity_codec_error("decode entity handoff", error))
}

pub fn encode_entity_handoff_into(
    update: &EntityHandoff,
    out: Vec<u8>,
) -> Result<Vec<u8>, EntityCodecError> {
    postcard::to_extend(update, out)
        .map_err(|error| entity_codec_error("encode entity handoff", error))
}

pub fn encode_entity_handoff_to_slice<'out>(
    update: &EntityHandoff,
    out: &'out mut [u8],
) -> Result<&'out mut [u8], EntityCodecError> {
    postcard::to_slice(update, out)
        .map_err(|error| entity_codec_error("encode entity handoff", error))
}

pub fn encoded_entity_handoff_len(update: &EntityHandoff) -> Result<usize, EntityCodecError> {
    postcard::experimental::serialized_size(update)
        .map_err(|error| entity_codec_error("size entity handoff", error))
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
        self.insert_origin(self.local_peer, local_id, chunk);
    }

    pub fn insert_origin(&mut self, origin: ServerId, local_id: i32, chunk: ChunkAddr) {
        self.entries.insert((origin, local_id), chunk);
    }

    #[must_use]
    pub fn chunk_of(&self, local_id: i32) -> Option<ChunkAddr> {
        self.entries.get(&(self.local_peer, local_id)).copied()
    }

    pub fn note_moved(&mut self, local_id: i32, chunk: ChunkAddr) -> bool {
        self.note_origin_moved(self.local_peer, local_id, chunk)
    }

    pub fn note_origin_moved(&mut self, origin: ServerId, local_id: i32, chunk: ChunkAddr) -> bool {
        let Some(entry) = self.entries.get_mut(&(origin, local_id)) else {
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
    pub fn plan_handoff_targets(
        &self,
        holders: &dyn Fn(ChunkAddr) -> Vec<u16>,
    ) -> Vec<EntityHandoffTarget> {
        let mut out = Vec::new();
        for ((origin, local_id), chunk) in &self.entries {
            let mut candidates: Vec<u16> = holders(*chunk)
                .into_iter()
                .filter(|peer| *peer != self.local_peer.0)
                .collect();
            candidates.sort_unstable();
            candidates.dedup();
            if let Some(&target_peer) = candidates.first() {
                out.push(EntityHandoffTarget {
                    entity_ref: EntityRef {
                        origin: *origin,
                        owner: self.local_peer,
                        local_id: *local_id,
                        chunk: *chunk,
                    },
                    target_peer,
                });
            }
        }
        out.sort_by(|left, right| {
            (left.entity_ref.origin, left.entity_ref.local_id)
                .cmp(&(right.entity_ref.origin, right.entity_ref.local_id))
        });
        out
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub struct EntityHandoffTarget {
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
    use crate::identity::PlayerSlot;

    fn chunk(x: i32, z: i32) -> ChunkAddr {
        ChunkAddr { x, z }
    }

    fn entity_ref(owner: u16, local_id: i32, x: i32, z: i32) -> EntityRef {
        EntityRef {
            origin: ServerId(owner),
            owner: ServerId(owner),
            local_id,
            chunk: chunk(x, z),
        }
    }

    #[test]
    fn item_spawn_state_roundtrips_in_reused_frame_storage() {
        let update = EntitySpawn {
            entity: entity_ref(2, 7, 3, 4),
            tick: TickStamp(11),
            kind: 41,
            pos: [1.0, 64.0, -2.0],
            yaw: 90.0,
            pitch: -15.0,
            state: EntitySpawnState::ItemDrop {
                stack: ItemStackState {
                    item_id: 821,
                    count: 27,
                    nbt: vec![10, 0, 0, 1, 2, 3],
                },
                entity_nbt: vec![10, 0, 0, 4, 5, 6],
            },
        };
        let len = encoded_entity_spawn_len(&update).expect("spawn has a size");
        let mut storage = vec![0_u8; len];
        let frame = encode_entity_spawn_to_slice(&update, &mut storage).expect("spawn encodes");
        assert_eq!(frame.len(), len);
        let (decoded, tail) = decode_entity_spawn_prefix(frame).expect("spawn decodes");
        assert!(tail.is_empty());
        assert_eq!(decoded, update);
    }

    #[test]
    fn player_spawn_state_roundtrips_with_authenticated_profile_state() {
        let update = EntitySpawn {
            entity: entity_ref(2, 7, 3, 4),
            tick: TickStamp(11),
            kind: 142,
            pos: [1.0, 64.0, -2.0],
            yaw: 90.0,
            pitch: -15.0,
            state: EntitySpawnState::Player(PlayerSpawnState {
                gid: GlobalPlayerId::new(ServerId(2), PlayerSlot(7)),
                uuid: [9_u8; 16],
                name: "holder_player".to_owned(),
                properties: vec![PresenceProperty::new(
                    "textures".to_owned(),
                    "payload".to_owned(),
                    Some("signature".to_owned()),
                )],
                gamemode: PlayerGameMode::Creative,
                source_reserved_entity_id: 771,
                world: "world".to_owned(),
                dimension: "minecraft:overworld".to_owned(),
                entity: PlayerEntityState {
                    velocity: [0.1, 0.2, 0.3],
                    on_ground: true,
                    flags: 9,
                    fire_ticks: 21,
                    health_milli: 17_500,
                    absorption_milli: 2_000,
                    fall_distance_milli: 600,
                    food: 18,
                    saturation_milli: 3_500,
                    experience_level: 30,
                    experience_progress_milli: 250,
                    experience_points: 1_234,
                    entity_nbt: vec![10, 0, 0, 7, 8, 9],
                },
            }),
        };
        let len = encoded_entity_spawn_len(&update).expect("spawn has a size");
        let mut storage = vec![0_u8; len];
        let frame = encode_entity_spawn_to_slice(&update, &mut storage).expect("spawn encodes");
        let (decoded, tail) = decode_entity_spawn_prefix(frame).expect("spawn decodes");
        assert!(tail.is_empty());
        assert_eq!(decoded, update);
        let EntitySpawnState::Player(state) = &decoded.state else {
            panic!("player spawn state expected");
        };
        assert!(state.is_authenticated_for(decoded.entity));
        let mut wrong_server = state.clone();
        wrong_server.gid = GlobalPlayerId::new(ServerId(3), PlayerSlot(7));
        assert!(!wrong_server.is_authenticated_for(decoded.entity));
    }

    #[test]
    fn handoff_carries_item_state_to_the_successor() {
        let handoff = EntityHandoff {
            origin: EntityOrigin {
                server: ServerId(2),
                local_id: 7,
            },
            previous_owner: ServerId(2),
            successor: ServerId(5),
            spawn: EntitySpawn {
                entity: entity_ref(2, 7, 3, 4),
                tick: TickStamp(11),
                kind: 41,
                pos: [1.0, 64.0, -2.0],
                yaw: 90.0,
                pitch: -15.0,
                state: EntitySpawnState::ItemDrop {
                    stack: ItemStackState {
                        item_id: 821,
                        count: 27,
                        nbt: vec![10, 0, 0, 1, 2, 3],
                    },
                    entity_nbt: vec![10, 0, 0, 4, 5, 6],
                },
            },
            velocity: [0.1, 0.2, 0.3],
        };
        assert!(handoff.is_consistent());
        assert!(handoff.applies_to(ServerId(5)));
        let len = encoded_entity_handoff_len(&handoff).expect("handoff has a size");
        let mut storage = vec![0_u8; len];
        let frame =
            encode_entity_handoff_to_slice(&handoff, &mut storage).expect("handoff encodes");
        let (decoded, tail) = decode_entity_handoff_prefix(frame).expect("handoff decodes");
        assert!(tail.is_empty());
        assert_eq!(decoded.destination_spawn().entity.owner, ServerId(5));
        assert_eq!(decoded.destination_spawn().state, handoff.spawn.state);
    }

    #[test]
    fn boundary_handoff_prepare_and_confirm_are_tagged_and_typed() {
        let handoff = EntityHandoff {
            origin: EntityOrigin { server: ServerId(2), local_id: 9 },
            previous_owner: ServerId(2),
            successor: ServerId(3),
            spawn: EntitySpawn {
                entity: entity_ref(2, 9, 4, 5),
                tick: TickStamp(7),
                kind: 1,
                pos: [1.0, 2.0, 3.0],
                yaw: 0.0,
                pitch: 0.0,
                state: EntitySpawnState::Entity { nbt: Vec::new() },
            },
            velocity: [0.0, 0.0, 0.0],
        };
        let prepare = EntityBoundaryHandoff::Prepare(handoff.clone());
        assert_eq!(
            decode_entity_boundary_handoff(&encode_entity_boundary_handoff(&prepare).expect("encode"))
                .expect("decode"),
            prepare
        );
        let confirm = EntityBoundaryHandoff::Confirm {
            origin: handoff.origin,
            previous_owner: handoff.previous_owner,
            successor: handoff.successor,
        };
        assert_eq!(
            decode_entity_boundary_handoff(&encode_entity_boundary_handoff(&confirm).expect("encode"))
                .expect("decode"),
            confirm
        );
    }

    #[test]
    fn boundary_handoff_rejects_other_control_frames() {
        let unrelated = TaggedEntityBoundaryHandoff {
            magic: ENTITY_BOUNDARY_HANDOFF_MAGIC.wrapping_add(1),
            frame: EntityBoundaryHandoff::Reject {
                origin: EntityOrigin { server: ServerId(2), local_id: 9 },
                previous_owner: ServerId(2),
                successor: ServerId(3),
            },
        };
        let bytes = postcard::to_allocvec(&unrelated).expect("encode");
        assert!(decode_entity_boundary_handoff(&bytes).is_err());
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
        let plan = owners.plan_handoff_targets(&holders);
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
