use std::borrow::Cow;
use std::ptr::null_mut;
use std::sync::atomic::{AtomicBool, AtomicI32, AtomicPtr, AtomicU64, Ordering};
use std::sync::Arc;

use bytes::Bytes;
use arc_swap::ArcSwap;
use pumpkin_data::entity::EntityType;
use pumpkin_protocol::codec::bit_set::BitSet;
use pumpkin_protocol::codec::var_int::VarInt;
use pumpkin_protocol::java::client::play::{
    CBlockUpdate, CCenterChunk, CChunkBatchEnd, CChunkBatchStart,
    CChunkData as ClientChunkData, CGameEvent, CLogin, CPlayerPosition, CPlayerSpawnPosition,
    CRemoveEntities, CSetCamera, CSpawnEntity, CSubtitle, CSystemChatMessage, CTitleAnimation,
    CTitleText, CUpdateTime, ChunkHeightmaps, GameEvent, LightData, PlayerSpawnData,
};
use pumpkin_protocol::ser::NetworkWriteExt;
use pumpkin_protocol::bedrock::server::text::SText;
use pumpkin_util::GameMode;
use pumpkin_util::math::position::BlockPos;
use pumpkin_util::version::JavaMinecraftVersion;
use pumpkin_util::math::vector2::Vector2;
use pumpkin_util::math::vector3::Vector3;
use pumpkin_util::text::TextComponent;
use pumpkin_util::resource_location::ResourceLocation;
use pumpkin_world::cylindrical_chunk_iterator::Cylindrical;
use pumpkin_world::level::is_cluster_secondary;
use pumpkin_world::level::SyncChunk;
use tracing::info;
use uuid::Uuid;

use crate::net::ClientPlatform;
use crate::net::{GameProfile, PlayerConfig};
use crate::net::java::JavaClient;
use crate::server::Server;
use pumpkin_cluster::identity::GlobalPlayerId;
use tokio_util::sync::CancellationToken;

/// Marker entity id used only as a fake spectator camera target.
///
/// Never inserted into any world entity list; only ever sent to the owning
/// client inside the lobby burst and later removed with `CRemoveEntities`.
const LOBBY_ANCHOR_ID: i32 = -1_000_001;
pub const LOBBY_TELEPORT_ID: i32 = 0;
pub const FORCED_LOBBY_PERMISSION: &str = "minecraft:command.forcelobby";
pub const EXIT_LOBBY_OTHERS_PERMISSION: &str = "minecraft:command.exitlobby.others";
pub struct LobbyWaiter {
    pub client: Arc<ClientPlatform>,
    pub profile: GameProfile,
    pub config: PlayerConfig,
    pub gid: GlobalPlayerId,
    pub entity_id: i32,
    handoff: CancellationToken,
    pub forced: AtomicBool,
    handoff_armed: AtomicBool,
    handoff_send_active: AtomicBool,
    handoff_delivered: ArcSwap<Vec<Vector2<i32>>>,
    handoff_waiting_ack: AtomicU64,
    handoff_pending: ArcSwap<Vec<Vector2<i32>>>,
    handoff_diagnostic_last_millis: [AtomicU64; 3],
    bedrock_ready: AtomicBool,
    bedrock_handoff_started: AtomicBool,
    bedrock_lobby_dimension: AtomicI32,
}

impl LobbyWaiter {
    #[must_use]
    pub fn handoff_token(&self) -> CancellationToken {
        self.handoff.clone()
    }

    pub fn send_system_message(&self, text: &TextComponent) {
        if let Some(client) = self.client.java() {
            client.try_send_packet(&CSystemChatMessage::new(text, false));
        }
        if let Some(client) = self.client.bedrock() {
            client.try_enqueue_client_packet(&SText::system_message(text.clone().get_text()));
        }
    }

    pub fn release(&self) {
        self.forced.store(false, Ordering::Relaxed);
        self.handoff_armed.store(true, Ordering::Relaxed);
    }

    pub fn hold(&self) {
        self.forced.store(true, Ordering::Relaxed);
        self.handoff_armed.store(false, Ordering::Relaxed);
    }

    #[must_use]
    pub fn handoff_armed(&self) -> bool {
        self.handoff_armed.load(Ordering::Relaxed)
    }

    fn has_handoff_delivery(&self, pos: &Vector2<i32>) -> bool {
        self.handoff_delivered.load().contains(pos)
    }

    fn mark_handoff_delivery(&self, positions: Vec<Vector2<i32>>) {
        self.handoff_delivered.rcu(|current| {
            let mut next = (**current).clone();
            for position in &positions {
                if !next.contains(position) {
                    next.push(*position);
                }
            }
            Arc::new(next)
        });
    }

    fn handoff_diagnostic_cooldown_elapsed(&self, stage: usize) -> bool {
        let now = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .map(|age| age.as_millis() as u64)
            .unwrap_or(0);
        let last = self.handoff_diagnostic_last_millis[stage].load(Ordering::Relaxed);
        if now.saturating_sub(last) < 1_000 {
            return false;
        }
        self.handoff_diagnostic_last_millis[stage]
            .compare_exchange(last, now, Ordering::Relaxed, Ordering::Relaxed)
            .is_ok()
    }

    pub fn bedrock_lobby_dimension(&self) -> i32 {
        self.bedrock_lobby_dimension.load(Ordering::Acquire)
    }

    pub fn bedrock_mark_ready(&self) {
        self.bedrock_ready.store(true, Ordering::Release);
    }

    pub fn bedrock_finish_handoff(&self) {
        if self.bedrock_handoff_started.swap(false, Ordering::AcqRel) {
            self.handoff.cancel();
        }
    }

    fn bedrock_ready(&self) -> bool {
        self.bedrock_ready.load(Ordering::Acquire)
    }

    pub(crate) fn bedrock_start_handoff(&self) -> bool {
        self.bedrock_handoff_started
            .compare_exchange(false, true, Ordering::AcqRel, Ordering::Acquire)
            .is_ok()
    }

    fn bedrock_handoff_started(&self) -> bool {
        self.bedrock_handoff_started.load(Ordering::Acquire)
    }
}
const LOBBY_MARKER_UUID: Uuid = Uuid::from_u128(0x4C4F42425946414B454C4F4242594641);
/// Far-away lobby origin so fake blocks never collide with the real world.
const LOBBY_BLOCK_X: i32 = 1_000_008;
/// Far-away lobby origin so fake blocks never collide with the real world.
const LOBBY_BLOCK_Z: i32 = 1_000_008;
/// Block Y of the fake end-portal platform shown below the camera.
const LOBBY_PLATFORM_Y: i32 = 100;
/// Camera feet for an eye exactly one block above the portal top (`100 -> 101`).
///
/// The portal block at `100` occupies `100..101`, so feet at `100.38` put the
/// eye `1.62` higher at `102.0` while looking straight down.
const LOBBY_ANCHOR_Y: f64 = 100.38;
const LOBBY_CAMERA_Y: f64 = LOBBY_PLATFORM_Y as f64 + 2.0;
const LOBBY_SPAWN_DIMENSION: &str = "minecraft:overworld";
const LOBBY_SECTION_COUNT: usize = 24;
const LOBBY_LIGHT_ARRAY_BYTES: usize = 2048;
/// Fake chunk radius around the portal chunk (5x5, 2 out each side).
const LOBBY_CHUNK_RADIUS: i32 = 2;
/// Frozen noon shown while waiting; rate `0.0` keeps lobby time paused.
const LOBBY_FROZEN_DAYTIME: i64 = 6000;
/// Title stays `20` ticks so it auto-hides one second after updates stop.
const LOBBY_TITLE_STAY_TICKS: i32 = 20;
/// Chunk coordinate of the fake portal chunk (`1_000_000 >> 4`).
fn lobby_center_chunk() -> Vector2<i32> {
    Vector2::new(LOBBY_BLOCK_X >> 4, LOBBY_BLOCK_Z >> 4)
}

/// Enters the wait room as one non-blocking burst of fake client data.
///
/// Sends, back-to-back through the lock-free enqueue path so the outgoing
/// flush carries them together with no loading-terrain delay: the
/// waiting-chunks event, the client-side lobby anchor, a placeholder spawn position
/// corrected with the real dimension right after the replay, the center chunk,
/// marker exactly one block above the portal looking straight down, the
/// spectator disguise, the camera, a frozen clock and the `Loading` title.
/// Server-side position, gamemode and player data are left untouched.
#[derive(Clone)]
struct LobbyStatic {
    head: Box<[Bytes]>,
    tail: Box<[Bytes]>,
}
const LOBBY_IMAGE_SLOTS: usize = JavaMinecraftVersion::Unknown as usize + 1;
static LOBBY_IMAGES: [AtomicPtr<LobbyStatic>; LOBBY_IMAGE_SLOTS] =
    [const { AtomicPtr::new(null_mut()) }; LOBBY_IMAGE_SLOTS];
fn lobby_serialize<P: pumpkin_protocol::ClientPacket + Sync>(
    packet: &P,
    version: &JavaMinecraftVersion,
    out: &mut Vec<Bytes>,
) {
    if let Ok(data) =
        pumpkin_protocol::java::packet_encoder::serialize_packet(packet, version)
    {
        out.push(data);
    }
}
fn lobby_build_image(version: JavaMinecraftVersion) -> LobbyStatic {
    let mut head = Vec::new();
    let mut tail = Vec::new();
    let batch_framing = version >= JavaMinecraftVersion::V_1_20_2;
    let center = lobby_center_chunk();
    let anchor = Vector3::new(
        f64::from(LOBBY_BLOCK_X) + 0.5,
        LOBBY_ANCHOR_Y,
        f64::from(LOBBY_BLOCK_Z) + 0.5,
    );
    let camera = Vector3::new(
        f64::from(LOBBY_BLOCK_X) + 0.5,
        LOBBY_CAMERA_Y,
        f64::from(LOBBY_BLOCK_Z) + 0.5,
    );
    let platform_id = i32::from(pumpkin_data::Block::END_PORTAL.default_state.id.as_u16());
    if batch_framing {
        lobby_serialize(
            &CGameEvent::new(GameEvent::StartWaitingChunks, 0.0),
            &version,
            &mut head,
        );
    }
    lobby_serialize(
        &CPlayerPosition::new(
            VarInt(LOBBY_TELEPORT_ID),
            anchor,
            Vector3::new(0.0, 0.0, 0.0),
            0.0,
            90.0,
            Vec::new(),
        ),
        &version,
        &mut head,
    );
    lobby_serialize(
        &CPlayerSpawnPosition::new(
            BlockPos::new(LOBBY_BLOCK_X, LOBBY_PLATFORM_Y, LOBBY_BLOCK_Z),
            0.0,
            0.0,
            LOBBY_SPAWN_DIMENSION.to_owned(),
        ),
        &version,
        &mut head,
    );
    lobby_serialize(
        &CCenterChunk {
            chunk_x: VarInt(center.x),
            chunk_z: VarInt(center.y),
        },
        &version,
        &mut tail,
    );
    if batch_framing {
        lobby_serialize(&CChunkBatchStart, &version, &mut tail);
    }
    let chunk_data = lobby_fake_chunk_data(version);
    let mut batch_size: u16 = 0;
    for dz in -LOBBY_CHUNK_RADIUS..=LOBBY_CHUNK_RADIUS {
        for dx in -LOBBY_CHUNK_RADIUS..=LOBBY_CHUNK_RADIUS {
            let chunk = ClientChunkData::new(
                center.x + dx,
                center.y + dz,
                lobby_fake_heightmaps(),
                &chunk_data,
                Vec::new(),
                lobby_fake_light_data(),
            );
            lobby_serialize(&chunk, &version, &mut tail);
            batch_size = batch_size.saturating_add(1);
        }
    }
    if batch_framing {
        lobby_serialize(&CChunkBatchEnd::new(batch_size), &version, &mut tail);
    }
    lobby_serialize(
        &CBlockUpdate::new(
            BlockPos::new(LOBBY_BLOCK_X, LOBBY_PLATFORM_Y, LOBBY_BLOCK_Z),
            platform_id.into(),
        ),
        &version,
        &mut tail,
    );
    lobby_serialize(
        &CSpawnEntity::new(
            VarInt(LOBBY_ANCHOR_ID),
            LOBBY_MARKER_UUID,
            i32::from(EntityType::MARKER.id).into(),
            camera,
            90.0,
            0.0,
            0.0,
            VarInt(0),
            Vector3::new(0.0, 0.0, 0.0),
        ),
        &version,
        &mut tail,
    );
    lobby_serialize(
        &CGameEvent::new(
            GameEvent::ChangeGameMode,
            GameMode::Spectator as i32 as f32,
        ),
        &version,
        &mut tail,
    );
    lobby_serialize(
        &CSetCamera::new(VarInt(LOBBY_ANCHOR_ID)),
        &version,
        &mut tail,
    );
    lobby_serialize(
        &CUpdateTime::new_clock(0, 0, LOBBY_FROZEN_DAYTIME, 0.0, 0.0),
        &version,
        &mut tail,
    );
    LobbyStatic {
        head: head.into_boxed_slice(),
        tail: tail.into_boxed_slice(),
    }
}
fn lobby_static_image(version: JavaMinecraftVersion) -> Cow<'static, LobbyStatic> {
    if version as usize >= LOBBY_IMAGE_SLOTS {
        return Cow::Owned(lobby_build_image(version));
    }
    let slot = &LOBBY_IMAGES[version as usize];
    let cached = slot.load(Ordering::Acquire);
    if !cached.is_null() {
        return Cow::Borrowed(unsafe { &*cached });
    }
    let fresh = Box::into_raw(Box::new(lobby_build_image(version)));
    match slot.compare_exchange(null_mut(), fresh, Ordering::AcqRel, Ordering::Acquire) {
        Ok(_) => Cow::Borrowed(unsafe { &*fresh }),
        Err(winner) => {
            drop(unsafe { Box::from_raw(fresh) });
            Cow::Borrowed(unsafe { &*winner })
        }
    }
}
fn lobby_fake_heightmaps() -> ChunkHeightmaps {
    ChunkHeightmaps {
        world_surface: Some(vec![0; 37]),
        motion_blocking: Some(vec![0; 37]),
        motion_blocking_no_leaves: Some(vec![0; 37]),
    }
}

fn lobby_fake_light_data() -> LightData {
    let sky_populated = ((1_u64 << (LOBBY_SECTION_COUNT + 1)) - 1) & !1;
    let sky_empty = 1 | (1_u64 << (LOBBY_SECTION_COUNT + 1));
    let block_empty = (1_u64 << (LOBBY_SECTION_COUNT + 2)) - 1;
    LightData::new(
        true,
        BitSet::from_u64(sky_populated),
        BitSet::from_u64(0),
        BitSet::from_u64(sky_empty),
        BitSet::from_u64(block_empty),
        vec![vec![0xff; LOBBY_LIGHT_ARRAY_BYTES]; LOBBY_SECTION_COUNT],
        Vec::new(),
    )
}

fn lobby_fake_chunk_data(version: JavaMinecraftVersion) -> Vec<u8> {
    let mut data = Vec::new();
    for _ in 0..LOBBY_SECTION_COUNT {
        data.write_i16_be(0).unwrap();
        if version >= JavaMinecraftVersion::V_26_1 {
            data.write_i16_be(0).unwrap();
        }
        data.write_u8(0).unwrap();
        data.write_var_int(&VarInt(0)).unwrap();
        if version <= JavaMinecraftVersion::V_1_21_4 {
            data.write_var_int(&VarInt(0)).unwrap();
        }
        data.write_u8(0).unwrap();
        data.write_var_int(&VarInt(0)).unwrap();
        if version <= JavaMinecraftVersion::V_1_21_4 {
            data.write_var_int(&VarInt(0)).unwrap();
        }
    }
    if version == JavaMinecraftVersion::V_1_21_5 {
        data.resize(data.len() + LOBBY_SECTION_COUNT * 2, 0);
    }
    data
}

#[must_use]
pub fn lobby_enabled(server: &Server) -> bool {
    server.advanced_config.cluster.enabled && is_cluster_secondary()
}

#[must_use]
pub fn enter_java_lobby(
    server: &Arc<Server>,
    client: Arc<ClientPlatform>,
    profile: GameProfile,
    config: PlayerConfig,
) -> Arc<LobbyWaiter> {
    enter_java_lobby_with(server, client, profile, config, None, true)
}

pub fn enter_manual_java_lobby(
    server: &Arc<Server>,
    client: Arc<ClientPlatform>,
    profile: GameProfile,
    config: PlayerConfig,
    gid: GlobalPlayerId,
) -> Arc<LobbyWaiter> {
    enter_java_lobby_with(server, client, profile, config, Some(gid), false)
}

pub fn enter_bedrock_lobby(
    server: &Arc<Server>,
    client: Arc<ClientPlatform>,
    profile: GameProfile,
    config: PlayerConfig,
) -> Arc<LobbyWaiter> {
    let (_, actual_dimension) = lobby_handoff_target(server, &profile);
    let lobby_dimension = if actual_dimension == 0 { 1 } else { 0 };
    let waiter = new_lobby_waiter(
        server,
        client,
        profile,
        config,
        None,
        true,
        lobby_dimension,
    );
    server.insert_lobby_waiter(waiter.clone());
    super::cluster_presence::publish_lobby_login(server, waiter.gid, &waiter.profile);
    info!(uuid = %waiter.profile.id, gid = ?waiter.gid, "lobby enter");
    waiter
}

fn new_lobby_waiter(
    server: &Server,
    client: Arc<ClientPlatform>,
    profile: GameProfile,
    config: PlayerConfig,
    gid: Option<GlobalPlayerId>,
    handoff_armed: bool,
    bedrock_lobby_dimension: i32,
) -> Arc<LobbyWaiter> {
    Arc::new(LobbyWaiter {
        client,
        profile,
        config,
        gid: gid.unwrap_or_else(|| super::cluster_presence::assign_lobby_gid(server)),
        entity_id: crate::entity::Entity::reserve_ids(1),
        handoff: CancellationToken::new(),
        forced: AtomicBool::new(false),
        handoff_armed: AtomicBool::new(handoff_armed),
        handoff_send_active: AtomicBool::new(false),
        handoff_delivered: ArcSwap::from_pointee(Vec::new()),
        handoff_waiting_ack: AtomicU64::new(0),
        handoff_pending: ArcSwap::from_pointee(Vec::new()),
        handoff_diagnostic_last_millis: [const { AtomicU64::new(0) }; 3],
        bedrock_ready: AtomicBool::new(false),
        bedrock_handoff_started: AtomicBool::new(false),
        bedrock_lobby_dimension: AtomicI32::new(bedrock_lobby_dimension),
    })
}

fn enter_java_lobby_with(
    server: &Arc<Server>,
    client: Arc<ClientPlatform>,
    profile: GameProfile,
    config: PlayerConfig,
    gid: Option<GlobalPlayerId>,
    handoff_armed: bool,
) -> Arc<LobbyWaiter> {
    let Some(java_client) = client.java() else {
        unreachable!();
    };
    let waiter = new_lobby_waiter(
        server,
        client.clone(),
        profile,
        config,
        gid,
        handoff_armed,
        0,
    );
    let dimensions: Vec<ResourceLocation> = server
        .dimensions
        .iter()
        .map(|dimension| ResourceLocation::from(dimension.minecraft_name))
        .collect();
    java_client.try_send_packet(&CLogin::new(
        waiter.entity_id,
        server.basic_config.hardcore,
        &dimensions,
        server
            .advanced_config
            .networking
            .java
            .max_players
            .try_into()
            .unwrap_or(u16::MAX.into()),
        server.advanced_config.networking.java.view_distance.get().into(),
        server
            .advanced_config
            .networking
            .java
            .simulation_distance
            .get()
            .into(),
        false,
        true,
        false,
        PlayerSpawnData::new(
            pumpkin_data::dimension::Dimension::OVERWORLD,
            0,
            GameMode::Spectator as u8,
            -1,
            false,
            false,
            None,
            VarInt(0),
            VarInt(63),
        ),
        server.advanced_config.networking.java.online_mode,
        true,
    ));
    let image = lobby_static_image(java_client.version.load());
    for packet in &image.head {
        java_client.try_enqueue_packet(packet.clone());
    }
    for packet in &image.tail {
        java_client.try_enqueue_packet(packet.clone());
    }
    server.insert_lobby_waiter(waiter.clone());
    super::cluster_presence::publish_lobby_login(server, waiter.gid, &waiter.profile);
    info!(uuid = %waiter.profile.id, gid = ?waiter.gid, "lobby enter");
    waiter
}

fn lobby_handoff_target(server: &Server, profile: &GameProfile) -> (Arc<crate::world::World>, i32) {
    if let Some(location) = super::cluster_playerdata::cached_handoff_location(profile.id) {
        let world = server.get_world_from_dimension(&location.dimension);
        let dimension = if world.dimension == pumpkin_data::dimension::Dimension::THE_NETHER {
            1
        } else if world.dimension == pumpkin_data::dimension::Dimension::THE_END {
            2
        } else {
            0
        };
        return (world, dimension);
    }
    let world = server.get_world_from_dimension(&pumpkin_data::dimension::Dimension::OVERWORLD);
    (world, 0)
}

fn lobby_required_virtual(server: &Server, waiter: &LobbyWaiter) -> (Arc<crate::world::World>, Vec<Vector2<i32>>) {
    let (world, _) = lobby_handoff_target(server, &waiter.profile);
    let info = world.level_info.load();
    let center = if let Some(location) = super::cluster_playerdata::cached_handoff_location(waiter.profile.id) {
        Vector2::new(location.pos[0].floor() as i32 >> 4, location.pos[2].floor() as i32 >> 4)
    } else {
        Vector2::new(info.spawn_x >> 4, info.spawn_z >> 4)
    };
    let view_distance = if waiter.client.bedrock().is_some() {
        server.advanced_config.networking.bedrock.view_distance
    } else {
        server.advanced_config.networking.java.view_distance
    };
    let required = Cylindrical::new(center, view_distance)
        .all_chunks_within()
        .collect();
    (world, required)
}

fn lobby_show_virtual_progress(client: &JavaClient, sent: usize, total: usize) {
    let percent = sent.saturating_mul(100).saturating_div(total.max(1));
    let title = TextComponent::text("Loading");
    let subtitle = TextComponent::text(format!("{percent}%"));
    client.try_send_packet(&CTitleAnimation::new(0, LOBBY_TITLE_STAY_TICKS, 5));
    client.try_send_packet(&CTitleText::new(&title));
    client.try_send_packet(&CSubtitle::new(&subtitle));
}

fn lobby_show_connection_status(client: &JavaClient, status: &str) {
    let title = TextComponent::text("Loading");
    let subtitle = TextComponent::text(status.to_string());
    client.try_send_packet(&CTitleAnimation::new(0, LOBBY_TITLE_STAY_TICKS, 5));
    client.try_send_packet(&CTitleText::new(&title));
    client.try_send_packet(&CSubtitle::new(&subtitle));
}

fn lobby_bedrock_handoff_position(waiter: &LobbyWaiter, world: &crate::world::World) -> Vector3<f32> {
    if let Some(location) = super::cluster_playerdata::cached_handoff_location(waiter.profile.id) {
        return Vector3::new(
            location.pos[0] as f32,
            location.pos[1] as f32,
            location.pos[2] as f32,
        );
    }
    let info = world.level_info.load();
    Vector3::new(
        info.spawn_x as f32 + 0.5,
        info.spawn_y as f32,
        info.spawn_z as f32 + 0.5,
    )
}

fn lobby_bedrock_dimension(world: &crate::world::World) -> i32 {
    if world.dimension == pumpkin_data::dimension::Dimension::THE_NETHER {
        1
    } else if world.dimension == pumpkin_data::dimension::Dimension::THE_END {
        2
    } else {
        0
    }
}

pub fn tick_virtual_lobbies(server: &Arc<Server>) {
    if !lobby_enabled(server) {
        return;
    }
    if let Some(status) = super::cluster::cluster_lobby_connection_status() {
        for waiter in server.lobby_waiters.load().iter() {
            if let Some(client) = waiter.client.java() {
                lobby_show_connection_status(client, status);
            }
            if let Some(client) = waiter.client.bedrock() {
                client.show_virtual_lobby_status(status);
            }
        }
        return;
    }
    for waiter in server.lobby_waiters.load().iter() {
        if !waiter.handoff_armed() {
            continue;
        }
        if let Some(client) = waiter.client.bedrock() {
            if !waiter.bedrock_ready() || waiter.bedrock_handoff_started() {
                continue;
            }
            let (world, required) = lobby_required_virtual(server, waiter);
            let mut chunks = Vec::with_capacity(required.len());
            for pos in &required {
                if world.level.is_cluster_full(pos) {
                    if let Some(chunk) = world.level.read_chunk_sync(pos, Clone::clone) {
                        chunks.push(chunk);
                    }
                } else {
                    world.level.pin_cluster_chunk(*pos);
                    world.level.prefetch_cluster_chunk(*pos);
                }
            }
            if chunks.len() == required.len() && !chunks.is_empty() {
                let position = lobby_bedrock_handoff_position(waiter, &world);
                let dimension = lobby_bedrock_dimension(&world);
                let _ = client.start_virtual_lobby_handoff(
                    waiter,
                    &world,
                    &chunks,
                    position,
                    dimension,
                );
            } else {
                client.show_virtual_lobby_status("Preparing world");
            }
            continue;
        }
        let (world, required) = lobby_required_virtual(server, waiter);
        let total = required.len();
        let waiting_ack = waiter.handoff_waiting_ack.load(Ordering::Acquire);
        if waiting_ack != 0
            && waiter
                .client
                .java()
                .is_some_and(|client| client.chunk_batch_acknowledgements() >= waiting_ack)
        {
            let pending = waiter.handoff_pending.load_full();
            let first = pending.first().copied().unwrap_or(Vector2::new(0, 0));
            if waiter.handoff_diagnostic_cooldown_elapsed(1) {
                info!(
                    target: "cluster_lobby",
                    stage = "java_chunk_batch_acknowledged",
                    gid = ?waiter.gid,
                    chunk_x = first.x,
                    chunk_z = first.y,
                    chunks = pending.len(),
                    acknowledgement = waiting_ack,
                    "client acknowledged the real-world chunk batch"
                );
            }
            waiter.mark_handoff_delivery(pending.as_ref().clone());
            waiter.handoff_pending.store(Arc::new(Vec::new()));
            waiter.handoff_waiting_ack.store(0, Ordering::Release);
        }
        let mut delivered = 0;
        let mut ready = Vec::<(Vector2<i32>, SyncChunk)>::new();
        for pos in &required {
            if waiter.has_handoff_delivery(pos) {
                delivered += 1;
            } else if world.level.is_cluster_full(pos) {
                if let Some(chunk) = world.level.read_chunk_sync(pos, Clone::clone) {
                    ready.push((*pos, chunk));
                }
            } else {
                world.level.pin_cluster_chunk(*pos);
                world.level.prefetch_cluster_chunk(*pos);
            }
        }
        if !ready.is_empty()
            && waiter.handoff_waiting_ack.load(Ordering::Acquire) == 0
            && waiter
                .handoff_send_active
                .compare_exchange(false, true, Ordering::AcqRel, Ordering::Relaxed)
                .is_ok()
        {
            let waiter = Arc::clone(waiter);
            let positions = ready.iter().map(|(pos, _)| *pos).collect::<Vec<_>>();
            let chunks = ready.into_iter().map(|(_, chunk)| chunk).collect::<Vec<_>>();
            let source = server.advanced_config.cluster.server_id;
            let acknowledgement = waiter
                .client
                .java()
                .map_or(0, |client| client.chunk_batch_acknowledgements().wrapping_add(1));
            server.spawn_task(async move {
                let delivered = match waiter.client.java() {
                    Some(client) => client.send_virtual_handoff_chunks(&chunks).await,
                    None => false,
                };
                if delivered {
                    waiter.handoff_pending.store(Arc::new(positions));
                    waiter
                        .handoff_waiting_ack
                        .store(acknowledgement, Ordering::Release);
                    let pending = waiter.handoff_pending.load_full();
                    let first = pending.first().copied().unwrap_or(Vector2::new(0, 0));
                    if waiter.handoff_diagnostic_cooldown_elapsed(0) {
                        info!(
                            target: "cluster_lobby",
                            stage = "java_real_chunk_batch_enqueued",
                            gid = ?waiter.gid,
                            source,
                            chunk_x = first.x,
                            chunk_z = first.y,
                            chunks = pending.len(),
                            acknowledgement,
                            "real-world chunk batch entered the Java client queue"
                        );
                    }
                }
                waiter.handoff_send_active.store(false, Ordering::Release);
            });
        }
        if let Some(client) = waiter.client.java() {
            lobby_show_virtual_progress(client, delivered, total);
        }
        if total > 0 && delivered >= total {
            if waiter.handoff_diagnostic_cooldown_elapsed(2) {
                info!(
                    target: "cluster_lobby",
                    stage = "java_handoff_released",
                    gid = ?waiter.gid,
                    delivered,
                    total,
                    "client receipts cover the complete handoff chunk set"
                );
            }
            waiter.handoff.cancel();
        }
    }
}

pub fn finish_java_lobby(_server: &Server, waiter: &LobbyWaiter, client: &JavaClient) {
    client.try_send_packet(&CSetCamera::new(VarInt(waiter.entity_id)));
    let anchor = [VarInt(LOBBY_ANCHOR_ID)];
    client.try_send_packet(&CRemoveEntities::new(&anchor));
    info!(uuid = %waiter.profile.id, gid = ?waiter.gid, "lobby handoff");
}

pub fn execute_lobby_command(server: &Arc<Server>, waiter: Arc<LobbyWaiter>, command: String) {
    let dispatcher = server.command_dispatcher.load();
    dispatcher.handle_command(
        &crate::command::CommandSender::Lobby(waiter).into_source(server),
        &command,
    );
}

pub fn broadcast_lobby_chat(server: &Server, waiter: &LobbyWaiter, message: String) {
    if !pumpkin_cluster::chat_sync::is_valid_chat_body(&message) {
        return;
    }
    super::cluster_chat_out::broadcast_public_chat(waiter.gid, &waiter.profile.name, &message);
    let decorated = TextComponent::chat_decorated(
        &server.advanced_config.chat.format,
        &waiter.profile.name,
        &message,
    );
    for player in server.get_all_players() {
        player.send_system_message(&decorated);
    }
    for recipient in server.lobby_waiters.load().iter() {
        recipient.send_system_message(&decorated);
    }
}

pub fn apply_remote_lobby_control(
    server: &Server,
    control: pumpkin_cluster::lobby_control::LobbyControl,
) -> bool {
    if control.target().server.0 != server.advanced_config.cluster.server_id {
        return false;
    }
    let target = control.target();
    if let Some(waiter) = server
        .lobby_waiters
        .load()
        .iter()
        .find(|waiter| waiter.gid == target)
    {
        match control {
            pumpkin_cluster::lobby_control::LobbyControl::Hold { .. } => waiter.hold(),
            pumpkin_cluster::lobby_control::LobbyControl::Release { .. } => waiter.release(),
        }
        return true;
    }
    let Some(player) = server
        .get_all_players()
        .into_iter()
        .find(|player| player.cluster_gid() == Some(target))
    else {
        return false;
    };
    match control {
        pumpkin_cluster::lobby_control::LobbyControl::Hold { .. } => {
            let Some(client) = player
                .client
                .as_deref()
                .and_then(crate::net::ClientPlatform::java)
            else {
                return false;
            };
            client.request_virtual_lobby();
            true
        }
        pumpkin_cluster::lobby_control::LobbyControl::Release { .. } => false,
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn fake_chunk_packet_is_client_data_only() {
        for version in [
            JavaMinecraftVersion::V_1_20,
            JavaMinecraftVersion::V_1_21_5,
            JavaMinecraftVersion::V_26_2,
        ] {
            let data = lobby_fake_chunk_data(version);
            let packet = ClientChunkData::new(
                lobby_center_chunk().x,
                lobby_center_chunk().y,
                lobby_fake_heightmaps(),
                &data,
                Vec::new(),
                lobby_fake_light_data(),
            );
            assert!(pumpkin_protocol::java::packet_encoder::serialize_packet(&packet, &version)
                .is_ok());
        }
    }

    #[test]
    fn camera_head_sits_one_block_above_portal_top() {
        assert_eq!(LOBBY_CAMERA_Y, f64::from(LOBBY_PLATFORM_Y) + 2.0);
        let player_eye = LOBBY_ANCHOR_Y + 1.62;
        let portal_head = f64::from(LOBBY_PLATFORM_Y) + 2.0;
        assert!((player_eye - portal_head).abs() < 1e-9);
    }

    #[test]
    fn static_image_frames_center_batch_and_tail() {
        let image = lobby_build_image(JavaMinecraftVersion::V_26_2);
        assert_eq!(image.head.len(), 3);
        assert_eq!(image.tail.len(), 33);
        let legacy = lobby_build_image(JavaMinecraftVersion::V_1_20);
        assert_eq!(legacy.head.len(), 2);
        assert_eq!(legacy.tail.len(), 31);
    }

    #[test]
    fn fake_chunk_light_marks_every_empty_block_section() {
        let light = lobby_fake_light_data();
        for bit in 0..=LOBBY_SECTION_COUNT + 1 {
            assert!(light.empty_block_light_mask.get_bit(bit));
        }
    }

}
