use std::collections::BTreeSet;
use std::error::Error;
use std::fs;
use std::io::{Result, Write};
use std::num::NonZero;
use std::path::Path;
use std::sync::Arc;
use std::sync::atomic::Ordering;
use std::time::Instant;

use arc_swap::ArcSwap;
use flate2::{Compression, write::GzEncoder};
use pumpkin_config::{AdvancedConfiguration, BasicConfiguration, TelemetryConfig};
use pumpkin_util::{GameMode, math::{vector2::Vector2, vector3::Vector3}};
use pumpkin_world::cylindrical_chunk_iterator::Cylindrical;
use serde::Serialize;

use crate::data::VanillaData;
use crate::entity::{EntityBase, player::Player};
use crate::net::{ChunkSender, ClientPlatform, GameProfile, PlayerConfig};
use crate::net::java::{JavaClient, JavaOfflineSink};
use super::Server;

pub(super) struct RegularCombatFixture {
    server: Arc<Server>,
    players: Vec<Arc<Player>>,
    sinks: Vec<JavaOfflineSink>,
    attack_frames: Vec<Vec<u8>>,
    _directory: tempfile::TempDir,
}

#[derive(Debug, Serialize)]
pub(super) struct RegularCombatSample {
    clock_ns: u128,
    actor_tick_and_enqueue_ns: u128,
    combat_and_enqueue_ns: u128,
    frame_ns: u128,
    total_ns: u128,
    attack_attempts: usize,
    packets: usize,
    framed_bytes: usize,
    peak_queued_packets: usize,
    peak_queued_payload_bytes: usize,
}

impl RegularCombatFixture {
    pub(super) async fn new(count: usize, actors_per_chunk: usize) -> std::result::Result<Self, Box<dyn Error>> {
        if count == 0 || count > 1000 || count % 2 != 0 || ![16, 64].contains(&actors_per_chunk) {
            return Err("invalid regular combat workload".into());
        }
        let directory = tempfile::tempdir()?;
        write_flat_world(directory.path())?;
        let basic = BasicConfiguration { default_level_name: directory.path().join("world").to_string_lossy().into_owned(),
            allow_nether: false, allow_end: false, allow_chat_reports: false, use_favicon: false,
            ..BasicConfiguration::default() };
        let mut advanced = AdvancedConfiguration::default();
        advanced.cluster.enabled = false;
        advanced.networking.java.enabled = false;
        advanced.networking.java.online_mode = false;
        advanced.networking.java.encryption = false;
        advanced.networking.java.authentication.enabled = false;
        advanced.networking.bedrock.enabled = false;
        advanced.networking.bedrock.online_mode = false;
        advanced.networking.bedrock.authentication.enabled = false;
        advanced.player_data.save_player_data = false;
        advanced.advancement.save_advancements = false;
        advanced.world.autosave_ticks = 0;
        let server = Server::new(basic, advanced, TelemetryConfig { enabled: false, ..Default::default() }, VanillaData::empty()).await;
        let world = server.worlds.load().first().cloned().ok_or("regular benchmark world missing")?;
        let mut locations = Vec::with_capacity(count);
        let mut required = BTreeSet::new();
        for index in 0..count {
            let group = index / actors_per_chunk;
            let home = Vector2::new((group % 8) as i32 * 5, (group / 8) as i32 * 5);
            let local = index % actors_per_chunk;
            let x = if index / 2 % 10 == 0 { 15.7 + (index % 2) as f64 }
                else { 2.0 + (local % 8) as f64 * 1.5 };
            let position = Vector3::new(f64::from(home.x) * 16.0 + x, -60.0,
                f64::from(home.y) * 16.0 + 2.0 + (local / 8) as f64 * 1.5);
            let chunk = Vector2::new((position.x.floor() as i32).div_euclid(16), (position.z.floor() as i32).div_euclid(16));
            let ready = (chunk.x - 2..=chunk.x + 2).flat_map(|x|
                (chunk.y - 2..=chunk.y + 2).map(move |z| Vector2::new(x, z))).collect::<Vec<_>>();
            required.extend(ready.iter().map(|chunk| (chunk.x, chunk.y)));
            locations.push((position, chunk, ready));
        }
        for (x, z) in required { world.level.get_or_fetch_chunk(Vector2::new(x, z), |_| ()).await; }
        let mut players = Vec::with_capacity(count);
        let mut sinks = Vec::with_capacity(count);
        for (index, (position, chunk, ready)) in locations.into_iter().enumerate() {
            let profile = GameProfile { id: uuid::Uuid::from_u128(index as u128 + 1), name: format!("bench_{index}"),
                properties: ArcSwap::from_pointee(Vec::new()), profile_actions: None };
            let config = PlayerConfig { view_distance: NonZero::new(2).ok_or("invalid view distance")?, ..Default::default() };
            let compression = &server.advanced_config.networking.java.compression;
            let (client, sink) = JavaClient::offline(index as u64, profile.clone(), config.clone(), pumpkin_data::packet::CURRENT_MC_VERSION,
                compression.enabled.then_some((compression.info.threshold as usize, compression.info.level)));
            let mut sender = ChunkSender::new();
            sender.register_batch_for_acknowledgement(0, ready);
            if sender.on_batch_acknowledged(10.0, 0) != crate::net::chunk_sender::BatchAckResult::Applied {
                return Err("regular benchmark chunk readiness failed".into());
            }
            let player = Arc::new(Player::new_with_entity_id(Arc::new(ClientPlatform::Java(client)), profile, config,
                &world, GameMode::Survival, None, Some(sender)));
            player.get_entity().set_pos(position);
            player.get_entity().on_ground.store(true, Ordering::Relaxed);
            player.watched_section.store(Cylindrical::new(chunk, NonZero::new(2).ok_or("invalid view distance")?));
            player.living_entity.set_max_health(1000.0);
            player.living_entity.set_health(1000.0);
            let ClientPlatform::Java(client) = player.client.as_ref() else { unreachable!() };
            client.set_player(player.clone());
            player.set_client_loaded(true);
            world.add_player(&player)?;
            players.push(player);
            sinks.push(sink);
        }
        for player in &players { world.pair_new_player_with_tracked_entities(player); }
        let attack_frames = (0..players.len()).map(|index| {
            use pumpkin_protocol::ClientPacket;
            let packet = pumpkin_protocol::java::server::play::SAttack { entity_id: pumpkin_protocol::codec::var_int::VarInt(players[index ^ 1].entity_id()) };
            let mut bytes = Vec::new();
            packet.write_packet_data(&mut bytes, &pumpkin_data::packet::CURRENT_MC_VERSION)?;
            Ok::<_, Box<dyn Error>>(bytes)
        }).collect::<std::result::Result<Vec<_>, _>>()?;
        let mut fixture = Self { server, players, sinks, attack_frames, _directory: directory };
        fixture.drain()?;
        Ok(fixture)
    }

    fn drain(&mut self) -> std::result::Result<(usize, usize, usize, usize), Box<dyn Error>> {
        let mut totals = (0, 0, 0, 0);
        for sink in &mut self.sinks {
            let drain = sink.drain(4352)?;
            totals.0 += drain.packets;
            totals.1 += drain.framed_bytes;
            totals.2 = totals.2.max(drain.queued_packets);
            totals.3 = totals.3.max(drain.queued_payload_bytes);
        }
        if self.players.iter().any(|player| player.client.java().is_some_and(JavaClient::is_closed)) {
            return Err("regular benchmark client queue overflow".into());
        }
        Ok(totals)
    }

    pub(super) async fn round(&mut self, tick: usize, attack_period: usize) -> std::result::Result<RegularCombatSample, Box<dyn Error>> {
        if attack_period == 0 { return Err("invalid attack cadence".into()); }
        let start = Instant::now();
        let world = self.server.worlds.load().first().cloned().ok_or("regular benchmark world missing")?;
        world.level_time.lock().map_err(|_| "regular benchmark clock poisoned")?.tick(false);
        let clock_ns = start.elapsed().as_nanos();
        let stage = Instant::now();
        self.server.tick_players_and_network();
        let actor_tick_and_enqueue_ns = stage.elapsed().as_nanos();
        let stage = Instant::now();
        let mut attack_attempts = 0;
        for (index, player) in self.players.iter().enumerate() {
            if (index + tick) % attack_period != 0 { continue; }
            use pumpkin_protocol::ServerPacket;
            let mut payload = self.attack_frames[index].as_slice();
            let packet = pumpkin_protocol::java::server::play::SAttack::read(&mut payload, &pumpkin_data::packet::CURRENT_MC_VERSION)?;
            if !payload.is_empty() { return Err("regular attack payload not consumed".into()); }
            let ClientPlatform::Java(client) = player.client.as_ref() else { unreachable!() };
            client.handle_attack(player, &packet, &self.server);
            attack_attempts += 1;
        }
        let combat_and_enqueue_ns = stage.elapsed().as_nanos();
        let stage = Instant::now();
        tokio::task::yield_now().await;
        let (packets, framed_bytes, peak_queued_packets, peak_queued_payload_bytes) = self.drain()?;
        let frame_ns = stage.elapsed().as_nanos();
        Ok(RegularCombatSample { clock_ns, actor_tick_and_enqueue_ns, combat_and_enqueue_ns, frame_ns,
            total_ns: start.elapsed().as_nanos(), attack_attempts, packets, framed_bytes,
            peak_queued_packets, peak_queued_payload_bytes })
    }
}

#[tokio::test(flavor = "current_thread")]
#[ignore = "explicit optimized offline benchmark"]
async fn regular_player_tick_and_combat_baseline() -> std::result::Result<(), Box<dyn Error>> {
    if cfg!(debug_assertions) { return Err("benchmark requires optimized build".into()); }
    let status = fs::read_to_string("/proc/self/status")?;
    let allowed = status.lines().find_map(|line| line.strip_prefix("Cpus_allowed_list:"))
        .ok_or("CPU affinity unavailable")?.trim();
    let cores = allowed.split(',').try_fold(0_usize, |count, part| {
        let mut range = part.split('-');
        let first = range.next().ok_or("invalid CPU affinity")?.parse::<usize>()?;
        let last = range.next().map(str::parse::<usize>).transpose()?.unwrap_or(first);
        Ok::<_, Box<dyn Error>>(count + last - first + 1)
    })?;
    if cores != 1 { return Err("benchmark requires one CPU affinity".into()); }
    let count = std::env::var("PUMPKIN_COMBAT_BENCH_ACTORS").unwrap_or_else(|_| "1000".into()).parse()?;
    let density = std::env::var("PUMPKIN_COMBAT_BENCH_DENSITY").unwrap_or_else(|_| "16".into()).parse()?;
    let mut fixture = RegularCombatFixture::new(count, density).await?;
    for tick in 0..50 { fixture.round(tick, 100).await?; }
    let mut samples = Vec::with_capacity(500);
    for tick in 50..550 { samples.push(fixture.round(tick, 100).await?); }
    if fixture.players.iter().all(|player| player.living_entity.health.load() >= 1000.0) {
        return Err("regular production attacks produced no actual damage".into());
    }
    let status = fs::read_to_string("/proc/self/status")?;
    let peak_rss_kib = status.lines().find_map(|line| line.strip_prefix("VmHWM:"))
        .and_then(|value| value.split_whitespace().next()).ok_or("peak RSS unavailable")?.parse::<u64>()?;
    let mut tick_ns = samples.iter().map(|sample| sample.total_ns).collect::<Vec<_>>();
    tick_ns.sort_unstable();
    let output = serde_json::json!({ "path": "regular Server::tick_players_and_network + SAttack::read/handle_attack + production Java encoder",
        "actors": count, "actors_per_chunk": density, "logical_tick_hz": 20, "cpu_affinity": allowed,
        "p95_tick_ns": tick_ns[tick_ns.len() * 95 / 100], "p99_tick_ns": tick_ns[tick_ns.len() * 99 / 100],
        "peak_rss_kib": peak_rss_kib, "allocation_profile": null, "canonical_comparison": null, "samples": samples });
    let path = format!("/tmp/pumpkin_agent_coordination/regular_combat_{count}_{density}.json");
    serde_json::to_writer_pretty(fs::File::create(path)?, &output)?;
    Ok(())
}
fn write_flat_world(node_root: &Path) -> Result<()> {
    let world = node_root.join("world");
    fs::create_dir_all(&world)?;

    let mut level = Vec::new();
    nbt_header(&mut level, 10, "");
    nbt_header(&mut level, 10, "Data");
    nbt_int(&mut level, "DataVersion", 4903);
    nbt_int(&mut level, "version", 19133);
    nbt_string(&mut level, "LevelName", "Pumpkin cluster E2E");
    nbt_byte(&mut level, "allowCommands", 1);
    nbt_byte(&mut level, "Difficulty", 2);
    nbt_int(&mut level, "SpawnX", 0);
    nbt_int(&mut level, "SpawnY", -59);
    nbt_int(&mut level, "SpawnZ", 0);
    nbt_header(&mut level, 10, "WorldGenSettings");
    nbt_long(&mut level, "seed", 1_787_949_120_387_005_756);
    level.push(0);
    level.push(0);
    level.push(0);
    write_gzip(&world.join("level.dat"), &level)?;

    let data_dir = world.join("data/minecraft");
    fs::create_dir_all(&data_dir)?;
    let mut settings = Vec::new();
    nbt_header(&mut settings, 10, "");
    nbt_int(&mut settings, "DataVersion", 4903);
    nbt_long(&mut settings, "seed", 1_787_949_120_387_005_756);
    nbt_byte(&mut settings, "generate_structures", 0);
    nbt_byte(&mut settings, "bonus_chest", 0);
    nbt_header(&mut settings, 10, "dimensions");
    nbt_header(&mut settings, 10, "minecraft:overworld");
    nbt_string(&mut settings, "type", "minecraft:overworld");
    nbt_header(&mut settings, 10, "generator");
    nbt_string(&mut settings, "type", "minecraft:flat");
    nbt_header(&mut settings, 10, "settings");
    nbt_string(&mut settings, "biome", "minecraft:plains");
    nbt_header(&mut settings, 9, "layers");
    settings.push(10);
    settings.extend_from_slice(&3_i32.to_be_bytes());
    for (block, height) in [
        ("minecraft:bedrock", 1),
        ("minecraft:dirt", 2),
        ("minecraft:grass_block", 1),
    ] {
        nbt_string(&mut settings, "block", block);
        nbt_int(&mut settings, "height", height);
        settings.push(0);
    }
    settings.push(0);
    settings.push(0);
    settings.push(0);
    settings.push(0);
    settings.push(0);
    write_gzip(&data_dir.join("world_gen_settings.dat"), &settings)
}
fn nbt_header(out: &mut Vec<u8>, kind: u8, name: &str) {
    out.push(kind);
    nbt_raw_string(out, name);
}

fn nbt_raw_string(out: &mut Vec<u8>, value: &str) {
    out.extend_from_slice(&u16::try_from(value.len()).unwrap_or(u16::MAX).to_be_bytes());
    out.extend_from_slice(value.as_bytes());
}

fn nbt_byte(out: &mut Vec<u8>, name: &str, value: i8) {
    nbt_header(out, 1, name);
    out.push(value as u8);
}

fn nbt_int(out: &mut Vec<u8>, name: &str, value: i32) {
    nbt_header(out, 3, name);
    out.extend_from_slice(&value.to_be_bytes());
}

fn nbt_long(out: &mut Vec<u8>, name: &str, value: i64) {
    nbt_header(out, 4, name);
    out.extend_from_slice(&value.to_be_bytes());
}

fn nbt_string(out: &mut Vec<u8>, name: &str, value: &str) {
    nbt_header(out, 8, name);
    nbt_raw_string(out, value);
}

fn write_gzip(path: &Path, bytes: &[u8]) -> Result<()> {
    let mut encoder = GzEncoder::new(fs::File::create(path)?, Compression::fast());
    encoder.write_all(bytes)?;
    encoder.finish()?;
    Ok(())
}
