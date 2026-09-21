use std::borrow::Cow;
use std::ptr::null_mut;
use std::sync::atomic::{AtomicPtr, Ordering};

use bytes::Bytes;
use pumpkin_data::Block;
use pumpkin_data::entity::EntityType;
use pumpkin_protocol::codec::var_int::VarInt;
use pumpkin_protocol::java::client::play::{
    CBlockEntityData, CBlockUpdate, CCenterChunk, CChunkBatchEnd, CChunkBatchStart, CGameEvent,
    CPlayerPosition, CPlayerSpawnPosition, CRemoveEntities, CSetCamera, CSpawnEntity, CUpdateTime,
    GameEvent,
};
use pumpkin_util::GameMode;
use pumpkin_util::math::position::BlockPos;
use pumpkin_util::version::JavaMinecraftVersion;
use pumpkin_util::math::vector2::Vector2;
use pumpkin_util::math::vector3::Vector3;
use pumpkin_util::text::TextComponent;
use pumpkin_world::chunk::format::LightContainer;
use pumpkin_world::cylindrical_chunk_iterator::Cylindrical;
use pumpkin_world::level::is_cluster_secondary;
use tracing::info;
use uuid::Uuid;

use crate::block::entities::BlockEntity;
use crate::block::entities::end_portal::EndPortalBlockEntity;
use crate::entity::EntityBase;
use crate::entity::player::{Player, TitleMode};
use crate::net::ClientPlatform;
use crate::net::java::chunk_data::CChunkData;
use crate::server::Server;

/// Marker entity id used only as a fake spectator camera target.
///
/// Never inserted into any world entity list; only ever sent to the owning
/// client inside the lobby burst and later removed with `CRemoveEntities`.
const LOBBY_ANCHOR_ID: i32 = -1_000_001;
pub const LOBBY_TELEPORT_ID: i32 = 0;
pub const FORCED_LOBBY_PERMISSION: &str = "minecraft:command.forcelobby";
pub const EXIT_LOBBY_OTHERS_PERMISSION: &str = "minecraft:command.exitlobby.others";
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
const LOBBY_CHUNK_BLOCKS: i32 = 16;
/// Fake chunk radius around the portal chunk (9x9, 4 out each side).
const LOBBY_CHUNK_RADIUS: i32 = 4;
/// Frozen noon shown while waiting; rate `0.0` keeps lobby time paused.
const LOBBY_FROZEN_DAYTIME: i64 = 6000;
/// Title stays `20` ticks so it auto-hides one second after updates stop.
const LOBBY_TITLE_STAY_TICKS: i32 = 20;
/// Whether the lobby wait room applies to this player at all.
///
/// Every client on a cluster secondary waits in the fake lobby; every other
/// path keeps the vanilla login flow untouched.
#[must_use]
pub fn lobby_applies_to(_player: &Player) -> bool {
    is_cluster_secondary()
}

/// Chunk coordinate of the fake portal chunk (`1_000_000 >> 4`).
fn lobby_center_chunk() -> Vector2<i32> {
    Vector2::new(LOBBY_BLOCK_X >> 4, LOBBY_BLOCK_Z >> 4)
}

/// Real-world chunks the client still has to load before the real join.
///
/// Progress (and the Loading percentage) is driven by these, never by the
/// fake lobby chunks which are fire-and-forget client-side filler.
fn lobby_required(player: &Player) -> Vec<Vector2<i32>> {
    let center = player.get_entity().chunk_pos.load();
    let view_distance = crate::world::chunker::get_view_distance(player);
    Cylindrical::new(center, view_distance)
        .all_chunks_within()
        .collect()
}

/// Shows `Loading` plus the live percentage, refreshed once per tick.
///
/// The title animation uses a 1s stay so the client hides it automatically
/// about a second after updates stop.
fn lobby_show_progress(player: &Player, sent: usize, total: usize) {
    let percent = sent.saturating_mul(100).saturating_div(total.max(1));
    player.send_title_animation(0, LOBBY_TITLE_STAY_TICKS, 5);
    player.show_title(&TextComponent::text("Loading"), &TitleMode::Title);
    player.show_title(
        &TextComponent::text(format!("{percent}%")),
        &TitleMode::SubTitle,
    );
}

/// Pushes the real world settings after loading finishes.
///
/// Time, weather, spawn and border are real-join data only; the lobby itself
/// stays frozen. Every read is best-effort (`try_lock`) so handoff never
/// blocks when the world is busy — the regular world ticks re-sync anyway.
fn lobby_sync_real_world_settings(player: &Player) {
    let world = player.world();
    player.send_time(&world);
    if let ClientPlatform::Java(client) = player.client.as_ref() {
        if let Ok(border) = world.worldborder.try_lock() {
            border.init_client(client);
        }
        let (spawn_pos, yaw, pitch) = {
            let info = world.level_info.load();
            (
                BlockPos::new(info.spawn_x, info.spawn_y, info.spawn_z),
                info.spawn_yaw,
                info.spawn_pitch,
            )
        };
        player.try_send_client_packet(
            &pumpkin_protocol::java::client::play::CPlayerSpawnPosition::new(
                spawn_pos,
                yaw,
                pitch,
                world.dimension.minecraft_name.to_owned(),
            ),
        );
        let _ = (yaw, pitch);
        if let Ok(weather) = world.weather.try_lock() {
            if weather.raining {
                player.try_send_client_packet(&CGameEvent::new(GameEvent::BeginRaining, 0.0));
                player.try_send_client_packet(&CGameEvent::new(
                    GameEvent::RainLevelChange,
                    weather.rain_level.clamp(0.0, 1.0),
                ));
                player.try_send_client_packet(&CGameEvent::new(
                    GameEvent::ThunderLevelChange,
                    weather.thunder_level.clamp(0.0, 1.0),
                ));
            }
        }
    }
}

/// Completes the wait room using fake-data teardown plus real-state sync.
///
/// The marker camera is removed, the client is teleported to its real
/// server-side position, and inventory, health, gamemode, abilities and world
/// settings are pushed so the client loads instantly. Nothing lobby-specific
/// is ever written to player data; the server-side position and gamemode were
/// never mutated by the lobby itself.
fn lobby_handoff(player: &Player) {
    info!(uuid = %player.gameprofile.id, "lobby handoff");
    for pos in lobby_required(player) {
        player.world().level.unpin_cluster_chunk(&pos);
    }
    player.set_in_cluster_lobby(false);
    if let Some(server) = player.world().server.upgrade() {
        super::cluster_presence::publish_lobby_state(&server, player);
    }
    let handoff_arc = player.world().get_player_by_uuid(player.gameprofile.id);
    let entity = player.get_entity();
    let real_pos = entity.pos.load();
    let yaw = entity.yaw.load();
    let pitch = entity.pitch.load();
    let real_mode = player.gamemode.load();
    player.request_teleport(real_pos, yaw, pitch);
    player.sync_inventory_to_client();
    player.send_health();
    player.send_abilities_update();
    player.try_send_client_packet(&CGameEvent::new(
        GameEvent::ChangeGameMode,
        real_mode as i32 as f32,
    ));
    player.try_send_client_packet(&CSetCamera::new(player.entity_id().into()));
    let anchor = [VarInt(LOBBY_ANCHOR_ID)];
    player.try_send_client_packet(&CRemoveEntities::new(&anchor));
    lobby_sync_real_world_settings(player);
    if let Some(joined) = handoff_arc {
        crate::world::chunker::update_position(&joined);
    }
    player.send_title_animation(0, LOBBY_TITLE_STAY_TICKS, 5);
    player.show_title(&TextComponent::text("Done"), &TitleMode::Title);
    player.show_title(&TextComponent::text("100%"), &TitleMode::SubTitle);
    player.set_client_loaded(true);
}

/// Enters the wait room as one non-blocking burst of fake client data.
///
/// Sends, back-to-back through the lock-free enqueue path so the outgoing
/// flush carries them together with no loading-terrain delay: the
/// waiting-chunks event, the client-side lobby anchor, a placeholder spawn position
/// corrected with the real dimension right after the replay, the center chunk,
/// the 9x9 air-filled chunk ring around the portal chunk
/// with motion-blocking heightmap and empty light masks inside chunk-batch
/// framing, the end-portal platform baked into the chunk data, the spectated
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
    let platform_id = i32::from(Block::END_PORTAL.default_state.id.as_u16());
    let portal_entity_id = EndPortalBlockEntity::new(BlockPos::new(
        LOBBY_BLOCK_X,
        LOBBY_PLATFORM_Y,
        LOBBY_BLOCK_Z,
    ))
    .get_id();
    let portal_nbt =
        pumpkin_nbt::Nbt::from(pumpkin_nbt::compound::NbtCompound::new()).write_unnamed();
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
    let mut batch_size: u16 = 0;
    for dz in -LOBBY_CHUNK_RADIUS..=LOBBY_CHUNK_RADIUS {
        for dx in -LOBBY_CHUNK_RADIUS..=LOBBY_CHUNK_RADIUS {
            let chunk = lobby_fake_chunk(center.x + dx, center.y + dz);
            lobby_serialize(&CChunkData(&chunk), &version, &mut tail);
            batch_size = batch_size.saturating_add(1);
        }
    }
    if batch_framing {
        lobby_serialize(&CChunkBatchEnd::new(batch_size), &version, &mut tail);
    }
    let portal_origin_x = center.x << 4;
    let portal_origin_z = center.y << 4;
    for dz in 0..LOBBY_CHUNK_BLOCKS {
        for dx in 0..LOBBY_CHUNK_BLOCKS {
            let portal_pos = BlockPos::new(
                portal_origin_x + dx,
                LOBBY_PLATFORM_Y,
                portal_origin_z + dz,
            );
            lobby_serialize(
                &CBlockUpdate::new(portal_pos, platform_id.into()),
                &version,
                &mut tail,
            );
            lobby_serialize(
                &CBlockEntityData::new(
                    portal_pos,
                    VarInt(portal_entity_id as i32),
                    portal_nbt.as_ref().into(),
                ),
                &version,
                &mut tail,
            );
        }
    }
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
fn lobby_fake_chunk(x: i32, z: i32) -> pumpkin_world::chunk::ChunkData {
    let chunk = pumpkin_world::chunk::ChunkData::empty(x, z);
    let portal = Block::END_PORTAL.default_state.id;
    let center = lobby_center_chunk();
    let holds_portal = x == center.x && z == center.y;
    if holds_portal {
        for dz in 0..LOBBY_CHUNK_BLOCKS {
            for dx in 0..LOBBY_CHUNK_BLOCKS {
                chunk.set_block_absolute_y(
                    dx as usize,
                    LOBBY_PLATFORM_Y,
                    dz as usize,
                    portal,
                );
            }
        }
        if let Ok(mut entities) = chunk.pending_block_entities.lock() {
            for dz in 0..LOBBY_CHUNK_BLOCKS {
                for dx in 0..LOBBY_CHUNK_BLOCKS {
                    let pos = BlockPos::new(
                        (center.x << 4) + dx,
                        LOBBY_PLATFORM_Y,
                        (center.y << 4) + dz,
                    );
                    entities.insert(pos, EndPortalBlockEntity::create_nbt(pos));
                }
            }
        }
    }
    if let Ok(mut heightmap) = chunk.heightmap.lock() {
        if heightmap.world_surface.is_none() {
            heightmap.world_surface = Some(vec![0; 37].into_boxed_slice());
        }
        if heightmap.motion_blocking.is_none() {
            heightmap.motion_blocking = Some(vec![0; 37].into_boxed_slice());
        }
        if heightmap.motion_blocking_no_leaves.is_none() {
            heightmap.motion_blocking_no_leaves = Some(vec![0; 37].into_boxed_slice());
        }
    }
    if let Ok(mut light) = chunk.light_engine.lock() {
        let sections = chunk.section.count;
        if holds_portal {
            light.sky_light =
                vec![LightContainer::new_filled(15); sections].into_boxed_slice();
        } else {
            light.sky_light = vec![LightContainer::Empty(0); sections].into_boxed_slice();
        }
        light.block_light = vec![LightContainer::Empty(0); sections].into_boxed_slice();
    }
    chunk
}
pub fn lobby_enter_on_login(player: &Player) {
    if !lobby_applies_to(player) {
        return;
    }
    let total = lobby_required(player).len();
    player.set_in_cluster_lobby(true);
    player.lobby_teleport_pending.store(true, Ordering::Relaxed);
    if let Some(server) = player.world().server.upgrade() {
        super::cluster_presence::publish_lobby_state(&server, player);
    }
    info!(uuid = %player.gameprofile.id, total = total, "lobby enter");
    if let ClientPlatform::Java(client) = player.client.as_ref() {
        let image = lobby_static_image(client.version.load());
        for packet in &image.head {
            client.try_enqueue_packet(packet.clone());
        }
        for packet in &image.tail {
            client.try_enqueue_packet(packet.clone());
        }
        player.try_send_client_packet(&CPlayerSpawnPosition::new(
            BlockPos::new(LOBBY_BLOCK_X, LOBBY_PLATFORM_Y, LOBBY_BLOCK_Z),
            0.0,
            0.0,
            player.world().dimension.minecraft_name.to_owned(),
        ));
    }
    player.send_title_animation(0, LOBBY_TITLE_STAY_TICKS, 5);
    lobby_show_progress(player, 0, total);
}

/// Advances the wait room once per player tick without ever blocking.
///
/// Re-sends the live percentage every tick so progress never sticks, and
/// hands off (teleport, inventory, gamemode, player data, real world time)
/// as soon as every required real chunk is loaded.
#[must_use]
pub fn lobby_enter_manual(player: &Player) -> bool {
    if player.is_in_cluster_lobby() {
        return false;
    }
    player.lobby_hold.store(true, Ordering::Relaxed);
    for pos in lobby_required(player) {
        player.world().level.unpin_cluster_chunk(&pos);
    }
    player.set_in_cluster_lobby(true);
    if let Some(server) = player.world().server.upgrade() {
        super::cluster_presence::publish_lobby_state(&server, player);
    }
    player.lobby_teleport_pending.store(true, Ordering::Relaxed);
    player.set_client_loaded(false);
    let total = lobby_required(player).len();
    info!(uuid = %player.gameprofile.id, total = total, "lobby enter");
    if let ClientPlatform::Java(client) = player.client.as_ref() {
        let image = lobby_static_image(client.version.load());
        for packet in &image.head {
            client.try_enqueue_packet(packet.clone());
        }
        for packet in &image.tail {
            client.try_enqueue_packet(packet.clone());
        }
        player.try_send_client_packet(&CPlayerSpawnPosition::new(
            BlockPos::new(LOBBY_BLOCK_X, LOBBY_PLATFORM_Y, LOBBY_BLOCK_Z),
            0.0,
            0.0,
            player.world().dimension.minecraft_name.to_owned(),
        ));
    }
    player.send_title_animation(0, LOBBY_TITLE_STAY_TICKS, 5);
    lobby_show_progress(player, 0, total);
    true
}

#[must_use]
pub fn lobby_exit_manual(player: &Player) -> bool {
    if !player.is_in_cluster_lobby() {
        return false;
    }
    player.lobby_hold.store(false, Ordering::Relaxed);
    player.lobby_forced.store(false, Ordering::Relaxed);
    true
}

pub fn lobby_tick_on_player_tick(player: &Player, _server: &Server) {
    if !lobby_applies_to(player) {
        return;
    }
    if !player.is_in_cluster_lobby() {
        return;
    }
    if player.lobby_hold.load(Ordering::Relaxed) {
        let required = lobby_required(player);
        let total = required.len();
        let world = player.world();
        let mut loaded = 0;
        for pos in &required {
            if world.level.is_cluster_held(pos) {
                loaded += 1;
            }
        }
        lobby_show_progress(player, loaded, total);
        return;
    }
    let required = lobby_required(player);
    let total = required.len();
    let world = player.world();
    let mut loaded = 0;
    for pos in &required {
        if world.level.is_cluster_held(pos) {
            loaded += 1;
        } else {
            world.level.pin_cluster_chunk(*pos);
            world.level.prefetch_cluster_chunk(*pos);
        }
    }
    if total > 0 && loaded >= total {
        lobby_handoff(player);
    } else {
        lobby_show_progress(player, loaded, total);
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn fake_chunk_carries_full_heightmaps_and_light() {
        let center = lobby_center_chunk();
        let chunk = lobby_fake_chunk(center.x, center.y);
        assert_eq!(chunk.section.count, 24);
        assert_eq!(chunk.section.min_y, -64);
        let heightmap = chunk.heightmap.lock().unwrap();
        let maps = [
            &heightmap.world_surface,
            &heightmap.motion_blocking,
            &heightmap.motion_blocking_no_leaves,
        ];
        for map in maps {
            assert_eq!(map.as_ref().map(|entries| entries.len()), Some(37));
        }
        drop(heightmap);
        let light = chunk.light_engine.lock().unwrap();
        assert_eq!(light.sky_light.len(), 24);
        assert_eq!(light.block_light.len(), 24);
    }

    #[test]
    fn fake_platform_fills_portal_chunk_edge_to_edge() {
        let center = lobby_center_chunk();
        let portal = Block::END_PORTAL.default_state.id;
        let air = Block::AIR.default_state.id;
        let middle = lobby_fake_chunk(center.x, center.y);
        assert_eq!(
            middle.section.get_block_absolute_y(0, LOBBY_PLATFORM_Y, 0),
            Some(portal)
        );
        assert_eq!(
            middle.section.get_block_absolute_y(15, LOBBY_PLATFORM_Y, 15),
            Some(portal)
        );
        assert_eq!(
            middle.section.get_block_absolute_y(7, LOBBY_PLATFORM_Y, 9),
            Some(portal)
        );
        let west = lobby_fake_chunk(center.x - 1, center.y);
        assert_eq!(
            west.section.get_block_absolute_y(15, LOBBY_PLATFORM_Y, 8),
            Some(air)
        );
        let north = lobby_fake_chunk(center.x, center.y - 1);
        assert_eq!(
            north.section.get_block_absolute_y(8, LOBBY_PLATFORM_Y, 15),
            Some(air)
        );
        let far = lobby_fake_chunk(center.x + 5, center.y + 5);
        assert_eq!(
            far.section.get_block_absolute_y(8, LOBBY_PLATFORM_Y, 8),
            Some(air)
        );
    }

    #[test]
    fn portal_chunk_carries_full_sky_light() {
        let center = lobby_center_chunk();
        let middle = lobby_fake_chunk(center.x, center.y);
        let light = middle.light_engine.lock().unwrap();
        assert!(light.sky_light.iter().all(|section| matches!(
            section,
            LightContainer::Full(_)
        )));
        assert!(light
            .block_light
            .iter()
            .all(|section| section.is_empty()));
        if let LightContainer::Full(data) = &light.sky_light[10] {
            assert!(data.iter().all(|byte| *byte == 0xFF));
        } else {
            panic!();
        }
        drop(light);
        let far = lobby_fake_chunk(center.x + 5, center.y + 5);
        let far_light = far.light_engine.lock().unwrap();
        assert!(far_light
            .sky_light
            .iter()
            .all(|section| section.is_empty()));
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
        assert_eq!(image.tail.len(), 600);
        let legacy = lobby_build_image(JavaMinecraftVersion::V_1_20);
        assert_eq!(legacy.head.len(), 2);
        assert_eq!(legacy.tail.len(), 598);
    }

    #[test]
    fn fake_portal_chunks_carry_end_portal_block_entities() {
        let center = lobby_center_chunk();
        let middle = lobby_fake_chunk(center.x, center.y);
        let entities = middle.pending_block_entities.lock().unwrap();
        assert_eq!(entities.len(), 256);
        for nbt in entities.values() {
            assert_eq!(nbt.get_string("id"), Some("minecraft:end_portal"));
        }
        drop(entities);
        let west = lobby_fake_chunk(center.x - 1, center.y);
        assert!(west.pending_block_entities.lock().unwrap().is_empty());
        let far = lobby_fake_chunk(center.x + 5, center.y + 5);
        assert!(far.pending_block_entities.lock().unwrap().is_empty());
    }
}
