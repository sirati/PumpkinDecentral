use std::{
    collections::{BTreeMap, HashSet},
    env, fs,
    future::Future,
    io::Write,
    net::SocketAddr,
    os::unix::fs::PermissionsExt,
    path::{Path, PathBuf},
    process::Stdio,
    sync::{
        OnceLock,
        atomic::{AtomicU64, Ordering},
    },
    time::{Duration, SystemTime, UNIX_EPOCH},
};

use anyhow::{Context, Result, bail};
use azalea::account::Account;
use azalea::registry::builtin::{BlockKind, EntityKind, ItemKind, MenuKind};
use azalea::{
    BlockPos, Client, ClientInformation, Event, Vec3, WalkDirection,
    core::direction::Direction,
    ecs::query::With,
    entity::{
        LookDirection,
        metadata::{AbstractEntityShiftKeyDown, Zombie},
    },
};
use azalea_inventory::components::CustomName;
use azalea_protocol::common::movements::MoveFlags;
use azalea_protocol::packets::game::ClientboundGamePacket;
use azalea_protocol::packets::game::c_game_event::EventType;
use azalea_protocol::packets::game::s_chunk_batch_received::ServerboundChunkBatchReceived;
use azalea_protocol::packets::game::s_command_suggestion::ServerboundCommandSuggestion;
use azalea_protocol::packets::game::s_container_close::ServerboundContainerClose;
use azalea_protocol::packets::game::s_interact::InteractionHand;
use azalea_protocol::packets::game::s_move_player_pos_rot::ServerboundMovePlayerPosRot;
use azalea_protocol::packets::game::s_player_action::{
    Action as PlayerAction, ServerboundPlayerAction,
};
use azalea_protocol::packets::game::s_player_input::ServerboundPlayerInput;
use azalea_protocol::packets::game::s_set_beacon::ServerboundSetBeacon;
use azalea_protocol::packets::game::s_use_item_on::{BlockHit, ServerboundUseItemOn};
use flate2::{Compression, write::GzEncoder};
use sha2::{Digest, Sha256};
use tempfile::TempDir;
use tokio::{
    io::{AsyncReadExt, AsyncWriteExt},
    net::{TcpListener, UdpSocket},
    process::{Child, Command},
    task::{JoinHandle, LocalSet},
    time,
};
use uuid::Uuid;

mod scenario_catalog;
mod spec_scenarios;

const BOT_A: &str = "PumpkinE2eA";
const BOT_B: &str = "PumpkinE2eB";
const BOT_C: &str = "PumpkinE2eC";
const BOT_D: &str = "PumpkinE2eD";
const BOT_E: &str = "PumpkinE2eE";
const E2E_DEADLINE: Duration = Duration::from_secs(600);
const E2E_STAGE_DEADLINE: Duration = Duration::from_secs(45);
const CONNECT_ATTEMPT_TIMEOUT: Duration = Duration::from_secs(2);
const CONNECT_RETRY_DELAY: Duration = Duration::from_millis(100);
const CLEANUP_TIMEOUT: Duration = Duration::from_secs(1);
const SNTP_EXCHANGE_TIMEOUT: Duration = Duration::from_millis(800);
const E2E_PREGEN_RADIUS: u8 = 16;
const MULTI_PRIMARY_PREGEN_CHUNK_CENTERS: &str = "-512,0;512,0";
static LOBBY_LOGIN_HANDSHAKE: OnceLock<time::Instant> = OnceLock::new();
static NEXT_PUMPKIN_SCOPE: AtomicU64 = AtomicU64::new(0);

fn pumpkin_scope(id: u16) -> Result<String> {
    let scope = format!(
        "pumpkin-e2e-{}-{id}-{}.scope",
        std::process::id(),
        NEXT_PUMPKIN_SCOPE.fetch_add(1, Ordering::Relaxed)
    );
    if let Some(path) = env::var_os("PUMPKIN_E2E_SCOPE_JOURNAL") {
        let mut journal = fs::OpenOptions::new()
            .create(true)
            .append(true)
            .open(path)?;
        writeln!(journal, "{scope}")?;
        journal.sync_all()?;
    }
    Ok(scope)
}

fn pumpkin_command(executable: &Path, scope: &str) -> Command {
    let mut command = Command::new("systemd-run");
    command
        .args(["--user", "--scope", "--quiet", "--collect"])
        .arg(format!("--unit={scope}"))
        .args(["-p", "MemoryMax=1G", "-p", "TimeoutStopSec=5s"]);
    if let Ok(slice) = env::var("PUMPKIN_E2E_SYSTEMD_SLICE") {
        command.arg("-p").arg(format!("Slice={slice}"));
    }
    command.arg("--").arg(executable);
    command
}

async fn stop_pumpkin_scope(scope: &str) -> Result<()> {
    let status = Command::new("systemctl")
        .args(["--user", "stop", scope])
        .status()
        .await
        .with_context(|| format!("stopping {scope}"))?;
    if status.success() {
        Ok(())
    } else {
        bail!("could not stop {scope}")
    }
}

async fn kill_pumpkin_scope(scope: &str) {
    let _ = Command::new("systemctl")
        .args(["--user", "kill", "--signal=SIGKILL", scope])
        .status()
        .await;
}

fn client_view_distance() -> u8 {
    if env::var_os("PUMPKIN_E2E_FULL_RADIUS").is_some()
        || env::var("PUMPKIN_E2E_SPEC_SCENARIOS")
            .unwrap_or_default()
            .split(',')
            .any(|scenario| scenario == "pinned-radius-view-change")
    {
        8
    } else {
        2
    }
}

fn expected_client_chunks() -> u64 {
    match client_view_distance() {
        2 => 49,
        8 => 329,
        _ => unreachable!(),
    }
}

struct Deadline {
    end: time::Instant,
}

impl Deadline {
    fn start() -> Self {
        let duration = if env::var("PUMPKIN_E2E_SPEC_SCENARIOS")
            .ok()
            .is_some_and(|selected| selected.split(',').any(|id| id == "acceptance-cooldown"))
        {
            Duration::from_secs(900)
        } else {
            E2E_DEADLINE
        };
        Self {
            end: time::Instant::now() + duration,
        }
    }

    fn remaining(&self, stage: &str) -> Result<Duration> {
        self.end
            .checked_duration_since(time::Instant::now())
            .filter(|remaining| !remaining.is_zero())
            .with_context(|| format!("Azalea E2E deadline expired during {stage}"))
    }
}

async fn within<T, F>(deadline: &Deadline, stage: &str, future: F) -> Result<T>
where
    F: Future<Output = Result<T>>,
{
    let remaining = deadline.remaining(stage)?;
    time::timeout(remaining.min(E2E_STAGE_DEADLINE), future)
        .await
        .with_context(|| format!("Azalea E2E stage deadline expired during {stage}"))?
}

#[derive(Clone)]
struct Node {
    id: u16,
    role: &'static str,
    quic: String,
    java: Option<String>,
    client_java: Option<String>,
    alternate_java: Option<String>,
    unrelated_java: Option<String>,
    malformed_java: Option<String>,
    cancel_java: Option<String>,
    root: PathBuf,
    pin: String,
}

struct Fixture {
    _root: TempDir,
    nodes: Vec<Node>,
    sntp: JoinHandle<()>,
    proxies: Vec<JoinHandle<()>>,
    secondary_persistence: Vec<(u16, BTreeMap<PathBuf, [u8; 32]>)>,
}

impl Drop for Fixture {
    fn drop(&mut self) {
        for node in self.nodes.iter().filter(|node| node.role == "secondary") {
            for name in ["world", "data"] {
                let _ = set_tree_permissions(&node.root.join(name), false);
            }
        }
    }
}

fn set_tree_permissions(path: &Path, read_only: bool) -> Result<()> {
    if !path.exists() {
        return Ok(());
    }
    let metadata =
        fs::metadata(path).with_context(|| format!("reading {} permissions", path.display()))?;
    if metadata.is_dir() {
        for entry in fs::read_dir(path).with_context(|| format!("reading {}", path.display()))? {
            set_tree_permissions(&entry?.path(), read_only)?;
        }
    }
    let mode = if metadata.is_dir() {
        if read_only { 0o555 } else { 0o755 }
    } else if read_only {
        0o444
    } else {
        0o644
    };
    fs::set_permissions(path, fs::Permissions::from_mode(mode))
        .with_context(|| format!("setting {} permissions", path.display()))
}

struct NodeProcess {
    id: u16,
    scope: String,
    child: Child,
    log: PathBuf,
}

struct Supervisor {
    children: Vec<NodeProcess>,
    primary_started: bool,
}

struct ExerciseEvidence {
    observed_entity_uuids: HashSet<Uuid>,
    controller_positions: Vec<Vec3>,
}

#[derive(Default)]
struct LobbyPacketEvidence {
    packet_trace: Vec<&'static str>,
    login_received_at: Option<time::Instant>,
    active_batch_chunks: Option<u32>,
    completed_batches: Vec<(u32, u32)>,
    chunk_positions: HashSet<(i32, i32)>,
    incomplete_fake_chunks: Vec<(i32, i32)>,
    marker_spawned: bool,
    marker_camera: bool,
    anchor_position: bool,
    spectator_login: bool,
    spectator_presentation: bool,
    frozen_clock: bool,
    platform_updates: HashSet<(i32, i32, i32)>,
    platform_block_entities: HashSet<(i32, i32, i32)>,
    received_commands: bool,
    command_roots: HashSet<String>,
    progress_subtitles: Vec<String>,
}

impl LobbyPacketEvidence {
    fn observe(&mut self, packet: &ClientboundGamePacket) -> Result<()> {
        match packet {
            ClientboundGamePacket::Login(packet) => {
                self.packet_trace.push("Login");
                self.login_received_at
                    .get_or_insert_with(time::Instant::now);
                self.spectator_login = packet.common.game_type.to_id() == 3;
            }
            ClientboundGamePacket::AddEntity(packet)
                if packet.uuid == Uuid::from_u128(0x4C4F42425946414B454C4F4242594641)
                    && packet.id.0 == -1_000_001
                    && packet.entity_type == EntityKind::Marker
                    && (packet.position.x - 1_000_008.5).abs() < 0.01
                    && (packet.position.y - 102.0).abs() < 0.01
                    && (packet.position.z - 1_000_008.5).abs() < 0.01 =>
            {
                self.marker_spawned = true;
            }
            ClientboundGamePacket::GameEvent(packet)
                if packet.event == EventType::WaitForLevelChunks =>
            {
                self.packet_trace.push("WaitForLevelChunks");
            }
            ClientboundGamePacket::PlayerPosition(packet) => {
                self.packet_trace.push("PlayerPosition");
                let pos = packet.change.pos;
                self.anchor_position |= (pos.x - 1_000_008.5).abs() < 0.01
                    && (pos.y - 100.38).abs() < 0.01
                    && (pos.z - 1_000_008.5).abs() < 0.01
                    && (packet.change.look_direction.x_rot() - 90.0).abs() < 0.01;
            }
            ClientboundGamePacket::SetDefaultSpawnPosition(_) => {
                self.packet_trace.push("SetDefaultSpawnPosition")
            }
            ClientboundGamePacket::SetChunkCacheCenter(_) => {
                self.packet_trace.push("SetChunkCacheCenter")
            }
            ClientboundGamePacket::ChunkBatchStart(_) => {
                self.packet_trace.push("ChunkBatchStart");
                if self.active_batch_chunks.replace(0).is_some() {
                    bail!("received a nested lobby chunk batch");
                }
            }
            ClientboundGamePacket::LevelChunkWithLight(packet) => {
                let chunks = self
                    .active_batch_chunks
                    .as_mut()
                    .context("received a lobby chunk outside a chunk batch")?;
                *chunks += 1;
                self.chunk_positions.insert((packet.x, packet.z));
                let heightmaps = &packet.chunk_data.heightmaps;
                let light = &packet.light_data;
                if heightmaps.len() < 3
                    || heightmaps.iter().any(|(_, data)| data.len() != 37)
                    || light.sky_updates.len() < 24
                    || light.block_updates.len() < 24
                {
                    self.incomplete_fake_chunks.push((packet.x, packet.z));
                }
            }
            ClientboundGamePacket::ChunkBatchFinished(packet) => {
                self.packet_trace.push("ChunkBatchFinished");
                let observed = self
                    .active_batch_chunks
                    .take()
                    .context("received a lobby chunk-batch end without a start")?;
                self.completed_batches.push((packet.batch_size, observed));
            }
            ClientboundGamePacket::Commands(packet) => {
                self.received_commands = true;
                let root = packet
                    .entries
                    .get(packet.root_index as usize)
                    .context("command graph root index is invalid")?;
                for child in &root.children {
                    let node = packet
                        .entries
                        .get(*child as usize)
                        .context("command graph root child index is invalid")?;
                    if let Some(name) = node.name() {
                        self.command_roots.insert(name.to_owned());
                    }
                }
            }
            ClientboundGamePacket::SetCamera(packet) => {
                self.packet_trace.push("SetCamera");
                self.marker_camera |= packet.camera_id.0 == -1_000_001;
            }
            ClientboundGamePacket::GameEvent(packet)
                if packet.event == EventType::ChangeGameMode =>
            {
                self.spectator_presentation |= packet.param == 3.0;
            }
            ClientboundGamePacket::SetTime(packet) => {
                self.frozen_clock |= !packet.clock_updates.is_empty()
                    && packet.clock_updates.values().all(|clock| clock.rate == 0.0);
            }
            ClientboundGamePacket::SetSubtitleText(packet) => {
                let display = packet.text.to_string();
                if display.matches('%').count() == 2 {
                    let azalea::FormattedText::Text(first) = &packet.text else {
                        bail!("lobby progress subtitle was not a text component: {display}");
                    };
                    let second = first
                        .base
                        .siblings
                        .first()
                        .context("lobby progress omitted client-received percentage")?;
                    let peer_color = first
                        .base
                        .style
                        .color
                        .as_ref()
                        .and_then(|color| color.name.as_deref());
                    let client_color = second
                        .get_base()
                        .style
                        .color
                        .as_ref()
                        .and_then(|color| color.name.as_deref());
                    if peer_color != Some("dark_gray") || client_color != Some("white") {
                        bail!(
                            "lobby progress colors are wrong: peer={peer_color:?}, client={client_color:?}"
                        );
                    }
                }
                self.progress_subtitles.push(packet.text.to_string());
            }
            ClientboundGamePacket::BlockUpdate(packet) => {
                self.platform_updates
                    .insert((packet.pos.x, packet.pos.y, packet.pos.z));
            }
            ClientboundGamePacket::BlockEntityData(packet) => {
                self.platform_block_entities
                    .insert((packet.pos.x, packet.pos.y, packet.pos.z));
            }
            _ => {}
        }
        Ok(())
    }

    fn validate_virtual_lobby_scene(&self) -> Result<()> {
        let expected = [
            "Login",
            "WaitForLevelChunks",
            "PlayerPosition",
            "SetDefaultSpawnPosition",
            "SetChunkCacheCenter",
            "ChunkBatchStart",
            "ChunkBatchFinished",
            "SetCamera",
        ];
        let indices =
            expected.map(|name| self.packet_trace.iter().position(|packet| *packet == name));
        if indices.iter().any(Option::is_none) || !indices.windows(2).all(|pair| pair[0] < pair[1])
        {
            bail!(
                "virtual lobby packet order diverged from recovered login sequence: {}",
                self.packet_trace.join(" -> ")
            )
        }
        let Some(&(declared, observed)) = self.completed_batches.first() else {
            bail!("virtual lobby spawned without a completed chunk batch");
        };
        if declared != 81 || observed != 81 {
            bail!(
                "virtual lobby first chunk batch declared {declared} and contained {observed} chunks; expected 81"
            );
        }
        let expected_chunks = (62_496..=62_504)
            .flat_map(|x| (62_496..=62_504).map(move |z| (x, z)))
            .collect::<HashSet<_>>();
        if self.chunk_positions != expected_chunks {
            bail!("virtual lobby chunk coordinates differ from the exact 9x9 layout");
        }
        if !self.incomplete_fake_chunks.is_empty() {
            bail!(
                "virtual lobby chunks lack full heightmaps or lighting: {:?}",
                self.incomplete_fake_chunks
            );
        }
        let expected_platform = (1_000_000..1_000_016)
            .flat_map(|x| (1_000_000..1_000_016).map(move |z| (x, 100, z)))
            .collect::<HashSet<_>>();
        if self.platform_updates != expected_platform {
            bail!("virtual lobby did not send all 256 portal block updates");
        }
        if self.platform_block_entities != expected_platform {
            bail!("virtual lobby did not send all 256 portal block entities");
        }
        if !self.marker_spawned
            || !self.marker_camera
            || !self.anchor_position
            || !self.spectator_login
            || !self.spectator_presentation
            || !self.frozen_clock
        {
            bail!(
                "virtual lobby omitted its marker camera, spectator presentation, or frozen clock"
            );
        }
        if !self.received_commands {
            bail!("virtual lobby spawned without receiving the command graph");
        }
        Ok(())
    }

    fn validate_virtual_lobby(&self) -> Result<()> {
        self.validate_virtual_lobby_scene()?;
        for required in [
            "exitlobby",
            "list",
            "ban",
            "pardon",
            "op",
            "deop",
            "kick",
            "spectate",
            "msg",
            "teammsg",
            "hide",
            "invsee",
            "syncstats",
            "gamerule",
            "clustertest",
        ] {
            if !self.command_roots.contains(required) {
                bail!(
                    "virtual lobby command graph is missing /{required}; roots={:?}",
                    self.command_roots
                );
            }
        }
        Ok(())
    }

    fn validate_warm_reconnect(&self) -> Result<()> {
        let first = self
            .progress_subtitles
            .first()
            .context("warm reconnect received no lobby progress subtitle")?;
        if !first.starts_with("100%") {
            bail!("warm reconnect first peer progress was not 100%: {first}");
        }
        if first.split_whitespace().count() != 2
            || !first.split_whitespace().all(|part| part.ends_with('%'))
        {
            bail!("warm reconnect progress did not show peer and client percentages: {first}");
        }
        Ok(())
    }
}

#[tokio::main(flavor = "current_thread")]
async fn main() -> Result<()> {
    let selected = env::var("PUMPKIN_E2E_SPEC_SCENARIOS").unwrap_or_default();
    for id in selected.split(',').filter(|id| !id.is_empty()) {
        if !scenario_catalog::REQUIRED_SCENARIOS.contains(&id) {
            bail!("unknown mandatory scenario {id}");
        }
    }
    if env::var_os("PUMPKIN_E2E_NORMAL_BIN").is_some() && !selected.is_empty() {
        bail!("normal-feature omission must run separately from feature scenarios");
    }
    LocalSet::new().run_until(run()).await?;
    if let Some(path) = env::var_os("PUMPKIN_E2E_RECEIPT_PATH") {
        let case = env::var("PUMPKIN_E2E_CASE").context("receipt requires case identity")?;
        let nonce = env::var("PUMPKIN_E2E_NONCE").context("receipt requires invocation nonce")?;
        fs::write(
            path,
            serde_json::to_vec(&serde_json::json!({
                "status": "passed", "scope": "partial", "case": case, "nonce": nonce,
                "scenarios": selected.split(',').filter(|id| !id.is_empty()).collect::<Vec<_>>(),
                "normal_feature": env::var_os("PUMPKIN_E2E_NORMAL_BIN").is_some(),
                "full_radius": env::var_os("PUMPKIN_E2E_FULL_RADIUS").is_some(),
                "one_way_delay_ms": env::var("PUMPKIN_E2E_ONE_WAY_DELAY_MS").unwrap_or_default(),
                "recovered_handoff_max_ms": env::var("PUMPKIN_E2E_RECOVERED_HANDOFF_MAX_MS").unwrap_or_default(),
                "delayed_chunk": env::var("PUMPKIN_E2E_DELAY_CHUNK").unwrap_or_default(),
                "large_admin": env::var_os("PUMPKIN_E2E_LARGE_ADMIN").is_some(),
            }))?,
        )?;
    }
    println!(
        "Partial E2E invocation passed; full acceptance requires pumpkin-cluster-full-spec and separate protocol/audit gates."
    );
    Ok(())
}

async fn run() -> Result<()> {
    let setup_deadline = Deadline::start();
    let fixture = within(&setup_deadline, "fixture creation", Fixture::create()).await?;
    if let Some(normal_binary) = env::var_os("PUMPKIN_E2E_NORMAL_BIN") {
        let normal_binary = PathBuf::from(normal_binary);
        let deadline = Deadline::start();
        let mut supervisor = within(
            &deadline,
            "normal-feature secondary startup",
            Supervisor::start(
                &normal_binary,
                &fixture
                    .nodes
                    .iter()
                    .filter(|node| node.role == "secondary")
                    .cloned()
                    .collect::<Vec<_>>(),
            ),
        )
        .await?;
        let result = verify_normal_binary_command_absence(&fixture.nodes, &deadline).await;
        let logs = supervisor.shutdown().await;
        return result.with_context(|| log_failure_context(&logs));
    }
    let executable = pumpkin_executable()?;
    let selected_scenarios = env::var("PUMPKIN_E2E_SPEC_SCENARIOS").unwrap_or_default();
    if selected_scenarios
        .split(',')
        .any(|id| id == "multi-primary-no-coholder")
    {
        if selected_scenarios
            .split(',')
            .filter(|id| !id.is_empty())
            .count()
            != 1
        {
            bail!("multi-primary-no-coholder is a separate four-process topology run");
        }
        let deadline = Deadline::start();
        let mut supervisor = within(
            &deadline,
            "two-primary fixture startup",
            Supervisor::start(&executable, &fixture.nodes),
        )
        .await?;
        let result = verify_multi_primary_settlement(&fixture.nodes, &deadline).await;
        let logs = supervisor.shutdown().await;
        result.with_context(|| log_failure_context(&logs))?;
        validate_multi_primary_settlement(&logs)?;
        fixture.validate_secondary_persistence()?;
        return Ok(());
    }
    if let Some(membership_id) = selected_scenarios.split(',').find(|id| {
        matches!(
            *id,
            "membership-ack-ten-seconds" | "membership-expand-set" | "membership-peer-removal"
        )
    }) {
        if selected_scenarios
            .split(',')
            .filter(|id| !id.is_empty())
            .count()
            != 1
        {
            bail!("{membership_id} is a separate isolated topology run");
        }
        let deadline = Deadline::start();
        let mut supervisor = within(
            &deadline,
            "isolated membership nodes",
            Supervisor::start(&executable, &fixture.nodes),
        )
        .await?;
        let result =
            verify_isolated_membership(&fixture.nodes, &mut supervisor, membership_id, &deadline)
                .await;
        let logs = supervisor.shutdown().await;
        result.with_context(|| log_failure_context(&logs))?;
        spec_scenarios::validate_world_scenario_records(&logs)?;
        let node2_log = logs
            .iter()
            .find(|(id, _)| *id == 2)
            .context("candidate log absent")?
            .1
            .as_str();
        let uuid = offline_uuid(BOT_B).to_string();
        let ack = node2_log
            .lines()
            .map(strip_ansi)
            .find(|line| {
                line.contains("cluster e2e lobby client chunks acknowledged")
                    && structured_value(line, "uuid") == Some(uuid.as_str())
            })
            .context("candidate promoted without complete client chunk ACK record")?;
        if structured_value(&ack, "required") != Some("49")
            || structured_value(&ack, "acknowledged") != Some("49")
        {
            bail!("candidate promoted without complete 49/49 client chunk ACK: {ack}");
        }
        return Ok(());
    }
    let deadline = Deadline::start();
    let mut supervisor = within(
        &deadline,
        "secondary process startup",
        Supervisor::start(
            &executable,
            &fixture
                .nodes
                .iter()
                .filter(|node| node.role == "secondary")
                .cloned()
                .collect::<Vec<_>>(),
        ),
    )
    .await?;
    let result = exercise(&fixture.nodes, &executable, &mut supervisor, &deadline).await;
    let mut logs = supervisor.shutdown().await;
    for node in &fixture.nodes {
        if !logs.iter().any(|(id, _)| *id == node.id) {
            let path = node.root.join("server.log");
            logs.push((
                node.id,
                fs::read_to_string(&path).unwrap_or_else(|error| {
                    format!("failed to read node log {}: {error}", path.display())
                }),
            ));
        }
    }
    fixture.sntp.abort();
    for proxy in &fixture.proxies {
        proxy.abort();
    }
    let persistence_check = fixture.validate_secondary_persistence();
    let virtual_request_check = validate_no_virtual_chunk_requests(&logs)
        .and_then(|()| validate_virtual_lobby_world_footprint(&logs));
    match result {
        Ok(evidence) => {
            persistence_check?;
            virtual_request_check?;
            validate_primary_pregen(&logs)?;
            validate_server_lobby_latency(&logs)?;
            validate_initial_client_chunk_acknowledgements(&logs)?;
            validate_early_real_chunk_streaming(&logs)?;
            validate_controller_divergence_logs(&logs, &evidence)?;
            validate_primary_natural_spawn(&logs, &evidence)?;
            validate_malformed_ack_log(&logs)?;
            validate_manual_lobby_gid_log(&logs)?;
            validate_departure_admin_logs(&logs)?;
            spec_scenarios::validate_world_scenario_records(&logs)?;
            spec_scenarios::validate_acceptance_delay_records(&logs)?;
            spec_scenarios::validate_acceptance_cooldown_records(&logs)?;
            spec_scenarios::validate_saturated_departure_records(&logs)?;
            spec_scenarios::validate_login_plugin_cancel_record(&logs)?;
            validate_large_admin_snapshot(&logs)?;
            validate_logs(&logs)
        }
        Err(error) => {
            let error = match persistence_check {
                Ok(()) => error,
                Err(invariant) => error.context(invariant),
            };
            if let Err(invariant) = virtual_request_check {
                Err(error.context(invariant).context(log_failure_context(&logs)))
            } else {
                Err(error.context(log_failure_context(&logs)))
            }
        }
    }
}

fn validate_primary_pregen(logs: &[(u16, String)]) -> Result<()> {
    let primary = logs
        .iter()
        .find(|(id, _)| *id == 0)
        .context("missing primary log for same-process world preparation")?;
    let complete = primary
        .1
        .lines()
        .map(strip_ansi)
        .find(|line| line.contains("primary_pregen_complete"))
        .context("primary did not complete same-process world preparation")?;
    let count = structured_value(&complete, "count")
        .context("primary world preparation omitted chunk count")?
        .parse::<usize>()
        .context("invalid primary world preparation chunk count")?;
    let radius = structured_value(&complete, "radius")
        .context("primary world preparation omitted radius")?
        .parse::<u8>()
        .context("invalid primary world preparation radius")?;
    if count != 1057 || radius != E2E_PREGEN_RADIUS {
        bail!("primary same-process world preparation was incomplete: {complete}");
    }
    Ok(())
}

fn validate_initial_client_chunk_acknowledgements(logs: &[(u16, String)]) -> Result<()> {
    for (node, player) in [(1, BOT_A), (2, BOT_B)] {
        let log = &logs
            .iter()
            .find(|(id, _)| *id == node)
            .with_context(|| format!("missing secondary {node} log"))?
            .1;
        let uuid = offline_uuid(player).to_string();
        let observed = log.lines().find_map(|line| {
            let line = strip_ansi(line);
            if !line.contains("cluster e2e lobby client chunks acknowledged")
                || structured_value(&line, "uuid") != Some(uuid.as_str())
            {
                return None;
            }
            Some((
                structured_value(&line, "required").and_then(|value| value.parse::<u64>().ok()),
                structured_value(&line, "acknowledged").and_then(|value| value.parse::<u64>().ok()),
            ))
        });
        let Some((Some(required), Some(acknowledged))) = observed else {
            bail!(
                "missing complete client chunk acknowledgement record for {player} on node {node}"
            )
        };
        if required != expected_client_chunks() || acknowledged != required {
            bail!(
                "incomplete client chunk acknowledgement for {player} on node {node}: required={required}, acknowledged={acknowledged}"
            )
        }
    }
    Ok(())
}

fn validate_large_admin_snapshot(logs: &[(u16, String)]) -> Result<()> {
    if env::var_os("PUMPKIN_E2E_LARGE_ADMIN").is_none() {
        eprintln!(
            "untested large-admin-snapshot: set PUMPKIN_E2E_LARGE_ADMIN after multipart snapshot applied records exist"
        );
        return Ok(());
    }
    for id in [1, 2] {
        let log = &logs
            .iter()
            .find(|(candidate, _)| *candidate == id)
            .with_context(|| format!("missing secondary {id} large-admin log"))?
            .1;
        let record = log.lines().map(strip_ansi).find(|line| {
            line.contains("cluster e2e admin snapshot applied")
                && structured_value(line, "entries")
                    .and_then(|value| value.parse::<usize>().ok())
                    .is_some_and(|count| count >= 12_002)
                && structured_value(line, "bytes")
                    .and_then(|value| value.parse::<usize>().ok())
                    .is_some_and(|bytes| bytes > 1_048_576)
        });
        if record.is_none() {
            bail!("secondary {id} did not prove application of a multipart >1MiB admin snapshot");
        }
    }
    Ok(())
}

fn validate_early_real_chunk_streaming(logs: &[(u16, String)]) -> Result<()> {
    let delayed_chunk = env::var("PUMPKIN_E2E_DELAY_CHUNK").ok();
    if delayed_chunk.is_some() {
        let primary = &logs
            .iter()
            .find(|(id, _)| *id == 0)
            .context("missing primary log for deterministic delayed chunk")?
            .1;
        if !primary.contains("cluster e2e chunk serve intentionally delayed") {
            bail!("delayed-chunk scenario did not actually delay the configured primary chunk");
        }
    }
    for (node, player) in [(1, BOT_A), (2, BOT_B)] {
        let log = &logs
            .iter()
            .find(|(id, _)| *id == node)
            .with_context(|| format!("missing secondary {node} log"))?
            .1;
        let uuid = offline_uuid(player).to_string();
        let records = log.lines().map(strip_ansi).collect::<Vec<_>>();
        let first = records.iter().enumerate().find(|(_, line)| {
            line.contains("cluster e2e lobby first real batch enqueued")
                && structured_value(line, "uuid") == Some(uuid.as_str())
        });
        let complete = records.iter().enumerate().find(|(_, line)| {
            line.contains("cluster e2e lobby peer chunks complete")
                && structured_value(line, "uuid") == Some(uuid.as_str())
        });
        let (Some((first_index, first)), Some((complete_index, complete))) = (first, complete)
        else {
            bail!("missing early-stream or peer-complete record for {player}")
        };
        let first_peer = structured_value(first, "peer")
            .and_then(|value| value.parse::<u64>().ok())
            .context("invalid first-batch peer count")?;
        let required = structured_value(first, "required")
            .and_then(|value| value.parse::<u64>().ok())
            .context("invalid first-batch required count")?;
        let batch_chunks = structured_value(first, "batch_chunks")
            .and_then(|value| value.parse::<u64>().ok())
            .context("invalid first-batch chunk count")?;
        if first_index >= complete_index || first_peer >= required || batch_chunks == 0 {
            bail!("real chunk stream for {player} waited for peer completion: {first}; {complete}")
        }
    }
    Ok(())
}

impl Fixture {
    async fn create() -> Result<Self> {
        let root = tempfile::tempdir().context("creating E2E fixture directory")?;
        let (sntp_addr, sntp) = start_sntp().await?;
        let primary_quic = unused_udp_address().await?;
        let secondary_one_quic = unused_udp_address().await?;
        let secondary_two_quic = unused_udp_address().await?;
        let secondary_one_java = unused_tcp_address().await?;
        let secondary_two_java = unused_tcp_address().await?;
        let mut nodes = Vec::new();
        for (id, role, quic, java, directory) in [
            (0, "primary", primary_quic, None, "primary"),
            (
                1,
                "secondary",
                secondary_one_quic,
                Some(secondary_one_java),
                "secondary-1",
            ),
            (
                2,
                "secondary",
                secondary_two_quic,
                Some(secondary_two_java),
                "secondary-2",
            ),
        ] {
            let node_root = root.path().join(directory);
            fs::create_dir_all(&node_root)
                .with_context(|| format!("creating fixture node {}", node_root.display()))?;
            nodes.push(Node {
                id,
                role,
                quic,
                java,
                client_java: None,
                alternate_java: None,
                unrelated_java: None,
                malformed_java: None,
                cancel_java: None,
                root: node_root,
                pin: write_keypair(&root.path().join(directory))?,
            });
            write_flat_world(&root.path().join(directory))?;
        }
        if env::var("PUMPKIN_E2E_SPEC_SCENARIOS")
            .unwrap_or_default()
            .split(',')
            .any(|id| id == "multi-primary-no-coholder")
        {
            let node_root = root.path().join("primary-3");
            fs::create_dir_all(&node_root).context("creating second primary directory")?;
            nodes.push(Node {
                id: 3,
                role: "primary",
                quic: unused_udp_address().await?,
                java: None,
                client_java: None,
                alternate_java: None,
                unrelated_java: None,
                malformed_java: None,
                cancel_java: None,
                pin: write_keypair(&node_root)?,
                root: node_root.clone(),
            });
            write_flat_world(&node_root)?;
        }
        if env::var("PUMPKIN_E2E_SPEC_SCENARIOS")
            .unwrap_or_default()
            .split(',')
            .any(|id| id == "relay-drop-newest-ground-election")
        {
            for id in [3, 4] {
                let node_root = root.path().join(format!("holder-{id}"));
                fs::create_dir_all(&node_root)?;
                nodes.push(Node {
                    id,
                    role: "secondary",
                    quic: unused_udp_address().await?,
                    java: None,
                    client_java: None,
                    alternate_java: None,
                    unrelated_java: None,
                    malformed_java: None,
                    cancel_java: None,
                    pin: write_keypair(&node_root)?,
                    root: node_root.clone(),
                });
                write_flat_world(&node_root)?;
            }
        }
        let mut proxies = Vec::new();
        let proxy_delay = env::var("PUMPKIN_E2E_ONE_WAY_DELAY_MS")
            .ok()
            .map(|value| value.parse::<u64>())
            .transpose()
            .context("parsing PUMPKIN_E2E_ONE_WAY_DELAY_MS")?
            .map(Duration::from_millis)
            .unwrap_or_default();
        for node in &mut nodes {
            let Some(backend) = node.java.clone() else {
                continue;
            };
            let name = match node.id {
                1 => BOT_A,
                2 => BOT_B,
                id => bail!("unexpected Java secondary {id}"),
            };
            let (address, proxy) = start_forward_proxy(backend, name, proxy_delay).await?;
            node.client_java = Some(address);
            proxies.push(proxy);
        }
        let node_two = nodes
            .iter_mut()
            .find(|node| node.id == 2)
            .context("missing secondary two for cross-secondary reconnect")?;
        let backend = node_two
            .java
            .clone()
            .context("secondary two Java listener missing")?;
        let (address, proxy) = start_forward_proxy(backend, BOT_A, proxy_delay).await?;
        node_two.alternate_java = Some(address);
        proxies.push(proxy);
        let node_two = nodes
            .iter_mut()
            .find(|node| node.id == 2)
            .context("missing secondary two for unrelated test client")?;
        let backend = node_two
            .java
            .clone()
            .context("secondary two Java listener missing")?;
        let (address, proxy) = start_forward_proxy(backend, BOT_C, proxy_delay).await?;
        node_two.unrelated_java = Some(address);
        proxies.push(proxy);
        let node_one = nodes
            .iter_mut()
            .find(|node| node.id == 1)
            .context("missing secondary one for malformed-ack test client")?;
        let backend = node_one
            .java
            .clone()
            .context("secondary one Java listener missing")?;
        let (address, proxy) = start_forward_proxy(backend, BOT_D, proxy_delay).await?;
        node_one.malformed_java = Some(address);
        proxies.push(proxy);
        let node_one = nodes
            .iter_mut()
            .find(|node| node.id == 1)
            .context("missing secondary one for canceled-login client")?;
        let backend = node_one
            .java
            .clone()
            .context("secondary one Java listener missing")?;
        let (address, proxy) = start_forward_proxy(backend, BOT_E, proxy_delay).await?;
        node_one.cancel_java = Some(address);
        proxies.push(proxy);
        for node in &nodes {
            write_config(node, &nodes, &sntp_addr)?;
        }
        for node in &nodes {
            write_ops(&node.root)?;
        }
        let secondary_persistence = nodes
            .iter()
            .filter(|node| node.role == "secondary")
            .map(|node| Ok((node.id, persistence_snapshot(&node.root)?)))
            .collect::<Result<Vec<_>>>()?;
        for node in nodes.iter().filter(|node| node.role == "secondary") {
            for name in ["world", "data"] {
                set_tree_permissions(&node.root.join(name), true)?;
            }
        }
        Ok(Self {
            _root: root,
            nodes,
            sntp,
            proxies,
            secondary_persistence,
        })
    }

    fn validate_secondary_persistence(&self) -> Result<()> {
        for (id, before) in &self.secondary_persistence {
            let node = self
                .nodes
                .iter()
                .find(|node| node.id == *id)
                .context("secondary persistence node disappeared")?;
            let after = persistence_snapshot(&node.root)?;
            if &after != before {
                let changes = before
                    .keys()
                    .chain(after.keys())
                    .collect::<HashSet<_>>()
                    .into_iter()
                    .filter(|path| before.get(*path) != after.get(*path))
                    .map(|path| path.display().to_string())
                    .collect::<Vec<_>>();
                bail!("secondary {id} modified its world or persistence data: {changes:?}");
            }
        }
        Ok(())
    }
}

fn persistence_snapshot(node_root: &Path) -> Result<BTreeMap<PathBuf, [u8; 32]>> {
    let mut files = Vec::new();
    for path in [node_root.join("world"), node_root.join("data")] {
        collect_files(&path, &mut files)?;
    }
    files.sort();
    let mut snapshot = BTreeMap::new();
    for file in files {
        let relative = file.strip_prefix(node_root).unwrap_or(&file).to_path_buf();
        let contents = fs::read(&file).with_context(|| format!("hashing {}", file.display()))?;
        snapshot.insert(relative, Sha256::digest(contents).into());
    }
    Ok(snapshot)
}

fn collect_files(path: &Path, files: &mut Vec<PathBuf>) -> Result<()> {
    if !path.exists() {
        return Ok(());
    }
    for entry in fs::read_dir(path).with_context(|| format!("reading {}", path.display()))? {
        let path = entry?.path();
        if path.is_dir() {
            collect_files(&path, files)?;
        } else {
            files.push(path);
        }
    }
    Ok(())
}

fn write_flat_world(node_root: &Path) -> Result<()> {
    let world = node_root.join("world");
    fs::create_dir_all(&world).with_context(|| format!("creating {}", world.display()))?;

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
    fs::create_dir_all(&data_dir).with_context(|| format!("creating {}", data_dir.display()))?;
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

fn write_gzip(path: &Path, bytes: &[u8]) -> Result<()> {
    let file = fs::File::create(path).with_context(|| format!("creating {}", path.display()))?;
    let mut encoder = GzEncoder::new(file, Compression::fast());
    encoder
        .write_all(bytes)
        .with_context(|| format!("writing {}", path.display()))?;
    encoder
        .finish()
        .with_context(|| format!("finishing {}", path.display()))?;
    Ok(())
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

impl Supervisor {
    async fn start(executable: &Path, nodes: &[Node]) -> Result<Self> {
        let mut children = Vec::with_capacity(nodes.len());
        let mut primary_started = false;
        for node in nodes {
            match Self::spawn_node(executable, node, node.id == 0 && !primary_started) {
                Ok(process) => {
                    children.push(process);
                    primary_started |= node.id == 0;
                }
                Err(error) => {
                    let mut supervisor = Self {
                        children,
                        primary_started,
                    };
                    let _ = supervisor.shutdown().await;
                    return Err(error);
                }
            }
        }
        Ok(Self {
            children,
            primary_started,
        })
    }

    fn start_node(&mut self, executable: &Path, node: &Node) -> Result<()> {
        let process = Self::spawn_node(executable, node, node.id == 0 && !self.primary_started)?;
        self.children.push(process);
        self.primary_started |= node.id == 0;
        Ok(())
    }

    async fn stop_node(&mut self, id: u16, deadline: &Deadline) -> Result<()> {
        let index = self
            .children
            .iter()
            .position(|process| process.id == id)
            .with_context(|| format!("node {id} is not running"))?;
        let mut process = self.children.swap_remove(index);
        stop_pumpkin_scope(&process.scope)
            .await
            .with_context(|| format!("stopping node {id}"))?;
        match time::timeout(
            deadline
                .remaining("primary dropout")?
                .min(Duration::from_secs(15)),
            process.child.wait(),
        )
        .await
        {
            Ok(Ok(_)) => Ok(()),
            Ok(Err(error)) => bail!("node {id} failed to stop: {error}"),
            Err(_) => {
                kill_pumpkin_scope(&process.scope).await;
                let _ = process.child.start_kill();
                let _ = process.child.wait().await;
                bail!("node {id} did not stop within the dropout deadline")
            }
        }
    }

    fn spawn_node(executable: &Path, node: &Node, pregen: bool) -> Result<NodeProcess> {
        let log_path = node.root.join("server.log");
        let log = fs::OpenOptions::new()
            .create(true)
            .append(true)
            .open(&log_path)
            .with_context(|| format!("creating node {} log", node.id))?;
        let error_log = log
            .try_clone()
            .with_context(|| format!("cloning node {} log handle", node.id))?;
        let scope = pumpkin_scope(node.id)?;
        let mut command = pumpkin_command(executable, &scope);
        command
            .current_dir(&node.root)
            .env("RUST_LOG", "info,pumpkin::server::cluster_lobby=debug,pumpkin::server::cluster_world_owner=info")
            .env("PUMPKIN_CLUSTER_E2E_PLAYER_A", BOT_A)
            .env("PUMPKIN_CLUSTER_E2E_PLAYER_B", BOT_B)
            .env("PUMPKIN_CLUSTER_E2E_PLAYER_C", BOT_C)
            .stdin(Stdio::null())
            .stdout(Stdio::from(log))
            .stderr(Stdio::from(error_log));
        if pregen
            || (node.role == "primary"
                && env::var("PUMPKIN_E2E_SPEC_SCENARIOS")
                    .unwrap_or_default()
                    .split(',')
                    .any(|id| id == "multi-primary-no-coholder"))
        {
            command.env(
                "PUMPKIN_CLUSTER_E2E_PREGEN_RADIUS",
                E2E_PREGEN_RADIUS.to_string(),
            );
        }
        if node.role == "primary"
            && env::var("PUMPKIN_E2E_SPEC_SCENARIOS")
                .unwrap_or_default()
                .split(',')
                .any(|id| id == "multi-primary-no-coholder")
        {
            command.env(
                "PUMPKIN_CLUSTER_E2E_PREGEN_CENTERS",
                MULTI_PRIMARY_PREGEN_CHUNK_CENTERS,
            );
        }
        if node.id == 0 {
            if let Some(delayed) = env::var_os("PUMPKIN_E2E_DELAY_CHUNK") {
                command.env("PUMPKIN_CLUSTER_E2E_DELAY_CHUNK", delayed);
            }
        }
        if node.id == 2
            && env::var("PUMPKIN_E2E_SPEC_SCENARIOS")
                .ok()
                .is_some_and(|selected| {
                    selected.split(',').any(|id| {
                        matches!(
                            id,
                            "membership-ack-ten-seconds"
                                | "membership-expand-set"
                                | "membership-peer-removal"
                        )
                    })
                })
        {
            command.env("PUMPKIN_CLUSTER_E2E_BLOCK_DIRECT_PEER", "1");
        }
        if node.id == 1
            && env::var("PUMPKIN_E2E_SPEC_SCENARIOS")
                .ok()
                .is_some_and(|selected| selected.split(',').any(|id| id == "login-plugin-cancel"))
        {
            command.env("PUMPKIN_CLUSTER_E2E_CANCEL_LOGIN_NAME", BOT_E);
        }
        if node.id == 1
            && env::var("PUMPKIN_E2E_SPEC_SCENARIOS")
                .ok()
                .is_some_and(|selected| selected.split(',').any(|id| id == "login-plugin-delay"))
        {
            command
                .env("PUMPKIN_CLUSTER_E2E_DELAY_LOGIN_NAME", BOT_E)
                .env("PUMPKIN_CLUSTER_E2E_DELAY_LOGIN_MS", "3000");
        }
        if node.id == 1
            && env::var("PUMPKIN_E2E_SPEC_SCENARIOS")
                .ok()
                .is_some_and(|selected| {
                    selected
                        .split(',')
                        .any(|id| id == "handoff-callback-early-confirm")
                })
        {
            command.env("PUMPKIN_CLUSTER_TEST_EARLY_CONFIRM_NAME", BOT_E);
        }
        let child = command
            .spawn()
            .with_context(|| format!("starting {} node", node.role))?;
        Ok(NodeProcess {
            id: node.id,
            scope,
            child,
            log: log_path,
        })
    }

    async fn restart_all(
        &mut self,
        executable: &Path,
        nodes: &[Node],
        deadline: &Deadline,
    ) -> Result<()> {
        let processes = std::mem::take(&mut self.children);
        for mut process in processes {
            stop_pumpkin_scope(&process.scope)
                .await
                .with_context(|| format!("stopping node {} for persistence restart", process.id))?;
            match time::timeout(
                deadline
                    .remaining("graceful three-node restart")?
                    .min(Duration::from_secs(15)),
                process.child.wait(),
            )
            .await
            {
                Ok(Ok(_)) => {}
                Ok(Err(error)) => bail!(
                    "node {} failed to stop for persistence restart: {error}",
                    process.id
                ),
                Err(_) => {
                    kill_pumpkin_scope(&process.scope).await;
                    let _ = process.child.start_kill();
                    let _ = process.child.wait().await;
                    bail!(
                        "node {} did not stop gracefully for persistence restart",
                        process.id
                    )
                }
            }
        }
        for node in nodes {
            self.start_node(executable, node)?;
        }
        Ok(())
    }

    async fn shutdown(&mut self) -> Vec<(u16, String)> {
        for process in &mut self.children {
            if let Ok(Some(status)) = process.child.try_wait() {
                eprintln!(
                    "node {} exited before fixture shutdown: {status}",
                    process.id
                );
            }
            let _ = stop_pumpkin_scope(&process.scope).await;
        }
        if time::timeout(CLEANUP_TIMEOUT, async {
            for process in &mut self.children {
                let _ = process.child.wait().await;
            }
        })
        .await
        .is_err()
        {
            for process in &mut self.children {
                kill_pumpkin_scope(&process.scope).await;
                let _ = process.child.start_kill();
                let _ = process.child.wait().await;
            }
        }
        self.children
            .iter()
            .map(|process| {
                (
                    process.id,
                    fs::read_to_string(&process.log).unwrap_or_else(|error| {
                        format!("failed to read node log {}: {error}", process.log.display())
                    }),
                )
            })
            .collect()
    }
}

fn validate_server_lobby_latency(logs: &[(u16, String)]) -> Result<()> {
    let mut receipts = BTreeMap::new();
    let mut enqueues = BTreeMap::new();
    for (node, output) in logs {
        for line in output.lines() {
            let line = strip_ansi(line);
            let target = if line.contains("cluster e2e final configuration received") {
                &mut receipts
            } else if line.contains("cluster e2e lobby first enqueue") {
                &mut enqueues
            } else {
                continue;
            };
            let uuid = line
                .split_whitespace()
                .find_map(|part| part.strip_prefix("uuid="))
                .context("lobby timing record lacked uuid")?;
            let micros = line
                .split_whitespace()
                .find_map(|part| part.strip_prefix("micros="))
                .context("lobby timing record lacked monotonic timestamp")?
                .parse::<u128>()
                .context("invalid lobby timing timestamp")?;
            target.entry((*node, uuid.to_owned())).or_insert(micros);
        }
    }
    for (node, name) in [(1, BOT_A), (2, BOT_B)] {
        let key = (node, offline_uuid(name).to_string());
        let received = receipts
            .get(&key)
            .with_context(|| format!("missing final configuration receipt for {name}"))?;
        let enqueued = enqueues
            .get(&key)
            .with_context(|| format!("missing first lobby enqueue for {name}"))?;
        let elapsed = enqueued
            .checked_sub(*received)
            .with_context(|| format!("lobby enqueue preceded configuration receipt for {name}"))?;
        if elapsed > 50_000 {
            bail!(
                "final configuration receipt to first lobby enqueue exceeded 50 ms for {name}: {elapsed} us"
            );
        }
    }
    Ok(())
}

fn log_failure_context(logs: &[(u16, String)]) -> String {
    let mut context = String::new();
    for (id, log) in logs {
        context.push_str(&format!(
            "\nnode {id} chunk trace counts: requests={} snapshots={} unavailable={} installed={} client_batches={} handoff_diagnostics={}\n",
            log.matches("cluster chunk request received").count(),
            log.matches("cluster chunk snapshot ready").count(),
            log.matches("cluster chunk not held, drop announced").count(),
            log.matches("cluster chunk installed").count(),
            log.matches("cluster lobby chunk batch").count(),
            log.matches("cluster e2e lobby handoff exceeded 30 seconds").count(),
        ));
        context.push_str(&format!(
            "node {id} chunk delivery counts: enqueued={} export_full={} export_timeout={} payload_encode={} send_failed={} install_failed={} unroutable={}\n",
            log.matches("cluster chunk snapshot enqueued").count(),
            log.matches("cluster chunk inventory export route full").count(),
            log.matches("cluster chunk inventory export did not complete").count(),
            log.matches("cluster chunk payload encode failed").count(),
            log.matches("cluster chunk snapshot send failed").count(),
            log.matches("cluster chunk inventory install rejected or timed out").count(),
            log.matches("cluster chunk want without routable holder").count(),
        ));
        let routed = log
            .lines()
            .filter(|line| line.contains("cluster reliable inbound routed"))
            .collect::<Vec<_>>();
        context.push_str(&format!(
            "node {id} reliable inbound counts: active={} total={} control={} chunk_data={} chunk_advert={}\n",
            log.matches("cluster reliable peer admission active").count(),
            routed.len(),
            routed.iter().filter(|line| line.contains("Control")).count(),
            routed.iter().filter(|line| line.contains("ChunkData")).count(),
            routed.iter().filter(|line| line.contains("ChunkAdvert")).count(),
        ));
        let mut inbound_by_peer = BTreeMap::<(u32, String), usize>::new();
        for line in &routed {
            let line = strip_ansi(line);
            if let (Some(peer), Some(kind)) = (
                structured_value(&line, "server").and_then(|value| value.parse::<u32>().ok()),
                structured_value(&line, "kind"),
            ) {
                *inbound_by_peer.entry((peer, kind.to_owned())).or_default() += 1;
            }
        }
        context.push_str(&format!(
            "node {id} reliable inbound by peer/kind: {inbound_by_peer:?}\n"
        ));
        context.push_str(&format!(
            "node {id} handoff route counts: connected={} dropped={} hint_routed={} hint_received={}\n",
            log.matches("cluster e2e handoff primary connected").count(),
            log.matches("cluster e2e handoff primary dropped").count(),
            log.matches("cluster e2e hint request routed").count(),
            log.matches("cluster e2e hint response received").count(),
        ));
        context.push_str(&format!(
            "node {id} handshake/hint counts: queued={} inbound={} primary_hint_received={}\n",
            log.matches("cluster handshake queued").count(),
            log.matches("cluster handshake inbound").count(),
            log.matches("cluster e2e primary hint request received")
                .count(),
        ));
        context.push_str(&format!(
            "node {id} peer connection closes: {}\n",
            log.matches("cluster peer connection closed").count(),
        ));
        context.push_str(&format!(
            "node {id} promotion/replica counts: client_chunks_acknowledged={} boundary_pending={} boundary_applied={} boundary_applied_late={}\n",
            log.matches("cluster e2e lobby client chunks acknowledged").count(),
            log.matches("remote player boundary pending").count(),
            log.matches("remote player boundary applied").count(),
            log.matches("remote player boundary applied after dependency arrived").count(),
        ));
        for line in log
            .lines()
            .filter(|line| line.contains("cluster peer connection closed"))
            .take(8)
        {
            context.push_str(line);
            context.push('\n');
        }
        let handoff = log
            .lines()
            .filter(|line| {
                line.contains("cluster e2e lobby handoff exceeded 30 seconds")
                    || line.contains("joined the game")
            })
            .take(12)
            .collect::<Vec<_>>();
        context.push_str(&format!("node {id} selected events:\n"));
        for line in handoff {
            context.push_str(line);
            context.push('\n');
        }
        context.push_str(&format!("node {id} promotion and replica events:\n"));
        for line in log
            .lines()
            .filter(|line| {
                line.contains("cluster e2e lobby client chunks acknowledged")
                    || line.contains("remote player boundary pending")
                    || line.contains("remote player boundary applied")
            })
            .take(16)
        {
            context.push_str(line);
            context.push('\n');
        }
        context.push_str(&format!("node {id} unroutable acquisition breakdown:\n"));
        for line in log
            .lines()
            .filter(|line| line.contains("cluster chunks without routable holders"))
            .take(4)
        {
            context.push_str(line);
            context.push('\n');
        }
        let connections = log
            .lines()
            .filter(|line| {
                line.contains("peer connection")
                    || line.contains("cluster peer joined")
                    || line.contains("cluster join hello")
                    || line.contains("cluster e2e hint")
                    || line.contains("cluster e2e handoff primary")
                    || line.contains("cluster handshake queued")
                    || line.contains("cluster handshake inbound")
                    || line.contains("cluster e2e primary hint request received")
                    || line.contains("cluster peer connection closed")
            })
            .rev()
            .take(20)
            .collect::<Vec<_>>();
        context.push_str(&format!("node {id} recent connection events:\n"));
        for line in connections.into_iter().rev() {
            context.push_str(line);
            context.push('\n');
        }
        let tail = log.lines().rev().take(30).collect::<Vec<_>>();
        context.push_str(&format!("\nnode {id} log tail:\n"));
        for line in tail.into_iter().rev() {
            context.push_str(line);
            context.push('\n');
        }
    }
    context
}

fn validate_logs(logs: &[(u16, String)]) -> Result<()> {
    let mut unexpected = Vec::new();
    for (id, log) in logs {
        let mut development_warning = 0;
        let mut non_tty_warning = 0;
        let mut expected_peer_closes = 0;
        let mut peer_joins = 0;
        let mut peer_leaves = 0;
        for raw in log.lines() {
            let line = strip_ansi(raw);
            peer_joins += usize::from(line.contains("cluster peer joined"));
            peer_leaves += usize::from(line.contains("cluster peer left"));
            let lower = line.to_ascii_lowercase();
            if !(lower.contains(" warn") || lower.contains(" error") || lower.contains("panic")) {
                continue;
            }
            if line.contains("Pumpkin is currently under heavy development!") {
                development_warning += 1;
                continue;
            }
            if line.contains("The input is not a TTY") {
                non_tty_warning += 1;
                continue;
            }
            if line.contains("cluster peer connection closed")
                && !line.contains("capacity exhausted")
                && !line.contains("write failed")
            {
                expected_peer_closes += 1;
                continue;
            }
            if env::var("PUMPKIN_E2E_SPEC_SCENARIOS")
                .ok()
                .is_some_and(|selected| {
                    selected
                        .split(',')
                        .any(|id| matches!(id, "acceptance-delay" | "acceptance-cooldown"))
                })
                && line.contains("cluster acceptance delayed")
            {
                continue;
            }
            if line.contains("cluster lobby handoff invariant failed")
                && line.contains(&offline_uuid(BOT_D).to_string())
            {
                continue;
            }
            if env::var("PUMPKIN_E2E_SPEC_SCENARIOS")
                .unwrap_or_default()
                .split(',')
                .any(|scenario| scenario == "handoff-revision-mismatch")
                && line.contains(&offline_uuid(BOT_E).to_string())
                && (line.contains("playerdata handoff revision mismatch")
                    || line.contains("unavailable playerdata revision mismatch"))
            {
                continue;
            }
            if env::var("PUMPKIN_E2E_SPEC_SCENARIOS")
                .unwrap_or_default()
                .split(',')
                .any(|case| case == "relay-drop-newest-ground-election")
                && line.contains("cluster peer dropped recovery reconciled")
                && line.contains("ERROR")
                && structured_value(&line, "peer") == Some("3")
            {
                continue;
            }
            if unexpected.len() < 20 {
                unexpected.push(format!("node {id}: {line}"));
            }
        }
        if development_warning > 2 {
            unexpected.push(format!(
                "node {id}: development warning repeated {development_warning} times"
            ));
        }
        if non_tty_warning > 2 {
            unexpected.push(format!(
                "node {id}: non-TTY warning repeated {non_tty_warning} times"
            ));
        }
        if expected_peer_closes > logs.len() + 1 {
            unexpected.push(format!(
                "node {id}: peer connection close repeated {expected_peer_closes} times"
            ));
        }
        if peer_joins > 2 * (logs.len() - 1) + 4 || peer_leaves > logs.len() + 2 {
            unexpected.push(format!("node {id}: membership transitions repeated: joined={peer_joins}, left={peer_leaves}"));
        }
    }
    if unexpected.is_empty() {
        Ok(())
    } else {
        bail!("unexpected cluster diagnostics:\n{}", unexpected.join("\n"))
    }
}

fn validate_malformed_ack_log(logs: &[(u16, String)]) -> Result<()> {
    let uuid = offline_uuid(BOT_D).to_string();
    let records = logs
        .iter()
        .flat_map(|(_, log)| log.lines().map(strip_ansi))
        .filter(|line| {
            line.contains("cluster lobby handoff invariant failed") && line.contains(&uuid)
        })
        .collect::<Vec<_>>();
    let [record] = records.as_slice() else {
        bail!(
            "expected one malformed chunk-ack invariant diagnostic, observed {}",
            records.len()
        );
    };
    if !record.contains("Unexpected chunk batch acknowledgement") {
        bail!("malformed chunk-ack diagnostic has the wrong reason: {record}");
    }
    Ok(())
}

fn validate_manual_lobby_gid_log(logs: &[(u16, String)]) -> Result<()> {
    let uuid = offline_uuid(BOT_A).to_string();
    let records = logs
        .iter()
        .flat_map(|(_, log)| log.lines().map(strip_ansi))
        .filter(|line| {
            line.contains("cluster e2e manual lobby gid continuity") && line.contains(&uuid)
        })
        .collect::<Vec<_>>();
    let [record] = records.as_slice() else {
        bail!(
            "expected one manual lobby gid-continuity record, observed {}",
            records.len()
        );
    };
    let before = structured_u32(record, "gid_before")?;
    let lobby = structured_u32(record, "gid_lobby")?;
    let after = structured_u32(record, "gid_after")?;
    if before != lobby || before != after {
        bail!("manual lobby changed GlobalPlayerId: {record}");
    }
    Ok(())
}

fn validate_departure_admin_logs(logs: &[(u16, String)]) -> Result<()> {
    for (kind, coordinate) in [
        ("DepartureAdminLobby", "2.5"),
        ("DepartureAdminDisconnect", "4.5"),
    ] {
        let records = logs
            .iter()
            .flat_map(|(_, log)| log.lines().map(strip_ansi))
            .filter(|line| {
                line.contains("cluster test departure admin committed") && line.contains(kind)
            })
            .collect::<Vec<_>>();
        let [record] = records.as_slice() else {
            bail!(
                "expected one durable {kind} record, observed {}",
                records.len()
            );
        };
        let revision = structured_value(record, "revision")
            .context("departure record has no committed revision")?
            .parse::<u64>()
            .context("departure record has invalid committed revision")?;
        if revision == 0
            || !record.contains("gid=")
            || !record.contains(coordinate)
            || structured_value(record, "canonical_ground") != Some("true")
            || !record.contains("ground_action=Some(")
            || !record.contains("correlation=Some(")
        {
            bail!("departure record lacks the committed actor or target: {record}");
        }
    }
    Ok(())
}

fn structured_u32(line: &str, name: &str) -> Result<u32> {
    structured_value(line, name)
        .with_context(|| format!("structured record has no {name}"))?
        .parse()
        .with_context(|| format!("structured record has invalid {name}"))
}

fn validate_no_virtual_chunk_requests(logs: &[(u16, String)]) -> Result<()> {
    let leaked = logs
        .iter()
        .flat_map(|(id, log)| log.lines().map(move |line| (*id, strip_ansi(line))))
        .filter(|(_, line)| line.contains("cluster chunk request received"))
        .filter_map(|(id, line)| {
            let x = structured_i32(&line, "chunk_x")?;
            let z = structured_i32(&line, "chunk_z")?;
            ((x.abs() >= 60_000) || (z.abs() >= 60_000)).then_some((id, x, z))
        })
        .take(20)
        .collect::<Vec<_>>();
    if leaked.is_empty() {
        Ok(())
    } else {
        bail!("virtual lobby coordinates entered real chunk acquisition: {leaked:?}")
    }
}

fn validate_virtual_lobby_world_footprint(logs: &[(u16, String)]) -> Result<()> {
    for (node, player) in [(1, BOT_A), (2, BOT_B)] {
        let log = &logs
            .iter()
            .find(|(id, _)| *id == node)
            .with_context(|| format!("missing secondary {node} log"))?
            .1;
        let uuid = offline_uuid(player).to_string();
        let record = log
            .lines()
            .map(strip_ansi)
            .find(|line| {
                line.contains("cluster e2e virtual lobby world footprint")
                    && structured_value(line, "uuid") == Some(uuid.as_str())
            })
            .with_context(|| format!("missing virtual lobby footprint record for {player}"))?;
        if structured_value(&record, "real_players") != Some("0") {
            bail!("virtual lobby created a real player for {player}: {record}")
        }
        for field in [
            "real_chunk_wants",
            "real_chunk_fetches",
            "real_chunk_holders",
        ] {
            let before = structured_value(&record, &format!("{field}_before"))
                .and_then(|value| value.parse::<u64>().ok())
                .with_context(|| format!("missing {field}_before in virtual lobby footprint"))?;
            let after = structured_value(&record, &format!("{field}_after"))
                .and_then(|value| value.parse::<u64>().ok())
                .with_context(|| format!("missing {field}_after in virtual lobby footprint"))?;
            if after != before {
                bail!("virtual lobby changed {field} for {player}: {record}")
            }
        }
    }
    Ok(())
}

fn validate_controller_divergence_logs(
    logs: &[(u16, String)],
    evidence: &ExerciseEvidence,
) -> Result<()> {
    let records = logs
        .iter()
        .flat_map(|(_, log)| log.lines().map(strip_ansi))
        .filter(|line| line.contains("cluster test controller divergence applied"))
        .collect::<Vec<_>>();
    let [record] = records.as_slice() else {
        bail!(
            "expected exactly one controller divergence record, observed {}",
            records.len()
        );
    };
    let claimed_x = structured_f64(record, "claimed_x")?;
    let claimed_y = structured_f64(record, "claimed_y")?;
    let claimed_z = structured_f64(record, "claimed_z")?;
    let effective_x = structured_f64(record, "effective_x")?;
    let effective_y = structured_f64(record, "effective_y")?;
    let effective_z = structured_f64(record, "effective_z")?;
    if (effective_x - claimed_x - 3.0).abs() > 1e-9
        || (effective_y - claimed_y).abs() > 1e-9
        || (effective_z - claimed_z).abs() > 1e-9
        || !record.contains("Modified(1)")
    {
        bail!("controller divergence record does not prove the one-shot +3 X filter: {record}");
    }
    let effective = Vec3::new(effective_x, effective_y, effective_z);
    let claimed = Vec3::new(claimed_x, claimed_y, claimed_z);
    let observed = evidence
        .controller_positions
        .last()
        .context("controller divergence produced no remote position")?;
    if !position_matches(*observed, effective) {
        bail!("remote observer did not settle on the effective controller position");
    }
    if evidence
        .controller_positions
        .iter()
        .any(|position| position_matches(*position, claimed))
    {
        bail!("remote observer received the raw claimed controller position");
    }
    Ok(())
}

fn validate_primary_natural_spawn(
    logs: &[(u16, String)],
    evidence: &ExerciseEvidence,
) -> Result<()> {
    let records = logs
        .iter()
        .flat_map(|(_, log)| log.lines().map(strip_ansi))
        .filter(|line| line.contains("primary_natural_entity_spawn"))
        .collect::<Vec<_>>();
    let [record] = records.as_slice() else {
        bail!(
            "expected one bounded primary natural-spawn record, observed {}",
            records.len()
        );
    };
    if structured_i32(record, "owner") != Some(0) {
        bail!("natural-spawn record is not primary-owned: {record}");
    }
    let uuid = structured_value(record, "uuid")
        .context("natural-spawn record has no UUID")?
        .parse::<Uuid>()
        .context("natural-spawn record UUID is invalid")?;
    if !evidence.observed_entity_uuids.contains(&uuid) {
        bail!("primary-owned natural entity {uuid} was not observed by the Java client");
    }
    Ok(())
}

fn structured_i32(line: &str, name: &str) -> Option<i32> {
    structured_value(line, name)?.parse().ok()
}

fn structured_value<'a>(line: &'a str, name: &str) -> Option<&'a str> {
    line.split_whitespace()
        .find_map(|field| field.strip_prefix(&format!("{name}=")))
        .map(|value| value.trim_matches('"'))
}

fn structured_f64(line: &str, name: &str) -> Result<f64> {
    line.split_whitespace()
        .find_map(|field| field.strip_prefix(&format!("{name}=")))
        .with_context(|| format!("controller divergence record has no {name}"))?
        .trim_matches('"')
        .parse()
        .with_context(|| format!("controller divergence record has invalid {name}"))
}

fn strip_ansi(line: &str) -> String {
    let mut plain = String::with_capacity(line.len());
    let mut bytes = line.bytes();
    while let Some(byte) = bytes.next() {
        if byte == 0x1b {
            if bytes.next() == Some(b'[') {
                for suffix in bytes.by_ref() {
                    if suffix.is_ascii_alphabetic() {
                        break;
                    }
                }
            }
        } else {
            plain.push(char::from(byte));
        }
    }
    plain
}

async fn verify_normal_binary_command_absence(nodes: &[Node], deadline: &Deadline) -> Result<()> {
    let secondary = nodes
        .iter()
        .find(|node| node.id == 1)
        .context("normal binary fixture lacks secondary one")?;
    let endpoint = secondary
        .client_java
        .as_deref()
        .context("normal binary fixture lacks Java forwarding endpoint")?;
    let (client, mut events) = connect(BOT_A, endpoint, deadline).await?;
    let login = wait_login(&mut events, deadline, "normal binary Java login", None).await?;
    let lobby = wait_virtual_lobby_spawn(
        &mut events,
        login,
        deadline,
        "normal binary virtual lobby",
        None,
    )
    .await?;
    lobby.validate_virtual_lobby_scene()?;
    if lobby.command_roots.contains("clustertest") {
        bail!("normal binary exposed /clustertest in decoded CCommands");
    }
    client.chat("/clustertest arena");
    within(deadline, "normal binary rejects test command", async {
        while let Some(event) = events.recv().await {
            match event {
                Event::Chat(chat) => {
                    let message = chat.message().to_string();
                    if message.contains("cluster test") || message.contains("arena accepted") {
                        bail!("normal binary executed a feature-only command: {message}");
                    }
                    if message.contains("Unknown")
                        || message.contains("unknown")
                        || message.contains("Incomplete")
                    {
                        return Ok(());
                    }
                }
                Event::Disconnect(reason) => {
                    bail!("normal binary disconnected client on unknown test command: {reason:?}")
                }
                Event::Packet(packet)
                    if matches!(
                        packet.as_ref(),
                        ClientboundGamePacket::Respawn(_) | ClientboundGamePacket::Login(_)
                    ) =>
                {
                    bail!("normal binary test command caused a terrain transition");
                }
                _ => {}
            }
        }
        bail!("normal binary event stream ended without unknown-command feedback")
    })
    .await?;
    client.disconnect();
    eprintln!(
        "passed normal-binary-command-absence: decoded CCommands omitted /clustertest and execution was rejected"
    );
    Ok(())
}

async fn verify_multi_primary_settlement(nodes: &[Node], deadline: &Deadline) -> Result<()> {
    let mut clients = Vec::new();
    for (node_id, name, remote) in [(1, BOT_A, BOT_B), (2, BOT_B, BOT_A)] {
        let node = nodes
            .iter()
            .find(|node| node.id == node_id)
            .context("multi-primary secondary missing")?;
        let (client, mut events) = connect(
            name,
            node.client_java
                .as_deref()
                .context("multi-primary Java proxy missing")?,
            deadline,
        )
        .await?;
        let login = wait_login(&mut events, deadline, "multi-primary login", None).await?;
        let lobby =
            wait_virtual_lobby_spawn(&mut events, login, deadline, "multi-primary lobby", None)
                .await?;
        lobby.validate_virtual_lobby_scene()?;
        wait_for_local_promotion(
            &client,
            &mut events,
            remote,
            false,
            deadline,
            "multi-primary promotion",
        )
        .await?;
        clients.push((client, events));
    }
    let (actor, mut actor_events) = clients.remove(0);
    let (observer, mut observer_events) = clients.remove(0);
    while actor_events.try_recv().is_ok() {}
    while observer_events.try_recv().is_ok() {}
    actor.chat("/clustertest multi-primary-no-coholder");
    let (source_window, destination_window) = tokio::try_join!(
        wait_multi_primary_chest(&mut actor_events, true, deadline),
        wait_multi_primary_chest(&mut observer_events, false, deadline),
    )?;
    for (client, x) in [(&actor, -8192), (&observer, 8192)] {
        wait_for_position(
            client,
            Vec3::new(f64::from(x) + 0.5, 80.0, 0.5),
            deadline,
            "distant chest holder position",
        )
        .await?;
        let chest = BlockPos::new(x, 80, 1);
        wait_until(client, deadline, "real distant chest chunk", || {
            Ok(client
                .world()?
                .read()
                .get_block_state(chest)
                .is_some_and(|state| BlockKind::from(state) == BlockKind::Chest)
                .then_some(()))
        })
        .await?;
    }
    actor.chat("/clustertest multi-primary-no-coholder-run");
    within(deadline, "cross-primary chest transaction ground settlement", async {
        let mut source_empty = false;
        let mut destination_one = false;
        let mut accepted = false;
        while !(source_empty && destination_one && accepted) {
            tokio::select! {
                event = actor_events.recv() => match event.context("multi-primary source stream closed")? {
                    Event::Packet(packet) => {
                        source_empty |= match packet.as_ref() {
                            ClientboundGamePacket::ContainerSetSlot(update) if update.container_id == source_window && update.slot == 0 => update.item_stack.is_empty(),
                            ClientboundGamePacket::ContainerSetContent(update) if update.container_id == source_window => update.items.first().is_some_and(|stack| stack.is_empty()),
                            _ => false,
                        };
                    }
                    Event::Chat(chat) if chat.message().to_string() == "cluster test multi-primary-no-coholder accepted" => accepted = true,
                    Event::Disconnect(reason) => bail!("joint primary resolution disconnected source: {reason:?}"),
                    _ => {}
                },
                event = observer_events.recv() => match event.context("multi-primary destination stream closed")? {
                    Event::Packet(packet) => {
                        let stack = match packet.as_ref() {
                            ClientboundGamePacket::ContainerSetSlot(update) if update.container_id == destination_window && update.slot == 0 => Some(&update.item_stack),
                            ClientboundGamePacket::ContainerSetContent(update) if update.container_id == destination_window => update.items.first(),
                            _ => None,
                        };
                        destination_one |= stack.is_some_and(|stack| stack.kind() == ItemKind::Stone && stack.count() == 1);
                    }
                    Event::Disconnect(reason) => bail!("joint primary resolution disconnected destination: {reason:?}"),
                    _ => {}
                },
            }
        }
        Ok(())
    }).await?;
    actor.chat("/list");
    wait_chat_contains(
        &mut actor_events,
        BOT_B,
        deadline,
        "joint settlement source responsiveness",
    )
    .await?;
    observer.chat("/list");
    wait_chat_contains(
        &mut observer_events,
        BOT_A,
        deadline,
        "joint settlement destination responsiveness",
    )
    .await?;
    wait_ticks(
        &actor,
        110,
        deadline,
        "settlement owner five-second retention",
    )
    .await?;
    for (client, count) in [(&actor, 0), (&observer, 1)] {
        let menu = client
            .get_inventory()?
            .menu()?
            .context("joint chest screen disappeared")?;
        let stone: i32 = menu
            .slots()
            .iter()
            .filter(|stack| stack.kind() == ItemKind::Stone)
            .map(|stack| stack.count())
            .sum();
        if stone != count {
            bail!("joint chest transaction failed conservation: expected {count}, got {stone}");
        }
    }
    actor.disconnect();
    observer.disconnect();
    Ok(())
}

async fn wait_multi_primary_chest(
    events: &mut tokio::sync::mpsc::UnboundedReceiver<Event>,
    source: bool,
    deadline: &Deadline,
) -> Result<i32> {
    within(deadline, "cross-primary chest preparation", async {
        let mut window = None;
        while let Some(event) = events.recv().await {
            match event {
                Event::Packet(packet) => match packet.as_ref() {
                    ClientboundGamePacket::OpenScreen(open)
                        if open.menu_type == MenuKind::Generic9x3 =>
                    {
                        window = Some(open.container_id)
                    }
                    ClientboundGamePacket::ContainerSetContent(content)
                        if Some(content.container_id) == window =>
                    {
                        let stack = content
                            .items
                            .first()
                            .context("prepared chest has no slot zero")?;
                        if (source && stack.kind() == ItemKind::Stone && stack.count() == 1)
                            || (!source && stack.is_empty())
                        {
                            return Ok(content.container_id);
                        }
                    }
                    _ => {}
                },
                Event::Disconnect(reason) => {
                    bail!("multi-primary preparation disconnected client: {reason:?}")
                }
                _ => {}
            }
        }
        bail!("multi-primary chest preparation ended without accepted content");
    })
    .await
}

fn validate_multi_primary_settlement(logs: &[(u16, String)]) -> Result<()> {
    let mut hashes = HashSet::new();
    let mut elected = HashSet::new();
    for node in [0, 1, 2, 3] {
        let log = logs
            .iter()
            .find(|(candidate, _)| *candidate == node)
            .with_context(|| format!("node {node} multi-primary log missing"))?
            .1
            .as_str();
        let record = log
            .lines()
            .map(strip_ansi)
            .find(|line| line.contains("cluster e2e multi primary joint settled"))
            .with_context(|| format!("node {node} has no accepted joint-primary outcome"))?;
        for (field, expected) in [
            ("initial_coholders", "0"),
            ("source_count", "0"),
            ("destination_count", "1"),
            ("all_legs_ground", "true"),
            ("dependency_notices", "true"),
            ("primary_pending_retained", "true"),
            ("peer_action_replies", "0"),
        ] {
            if structured_value(&record, field) != Some(expected) {
                bail!("cross-primary settlement omitted {field} on node {node}: {record}");
            }
        }
        let owner = structured_value(&record, "resolution_owner")
            .context("joint resolution owner absent")?
            .parse::<u16>()?;
        let loads = structured_value(&record, "primary_loads")
            .context("joint primary load advertisement absent")?
            .split(',')
            .map(|entry| {
                let (peer, load) = entry
                    .split_once(':')
                    .context("malformed primary load entry")?;
                Ok((peer.parse::<u16>()?, load.parse::<u64>()?))
            })
            .collect::<Result<BTreeMap<_, _>>>()?;
        if loads.len() != 2
            || !loads.contains_key(&0)
            || !loads.contains_key(&3)
            || loads.get(&owner) != loads.values().min()
        {
            bail!("joint resolver was not selected from primaries with least load: {record}");
        }
        if node == owner
            && (structured_value(&record, "remote_legs_loaded") != Some("1")
                || structured_value(&record, "retained_ms")
                    .and_then(|v| v.parse::<u64>().ok())
                    .is_none_or(|elapsed| elapsed < 5_000))
        {
            bail!("joint resolver failed remote-leg acquisition or retention: {record}");
        }
        elected.insert(owner);
        hashes.insert(
            structured_value(&record, "joint_ground_hash")
                .context("joint settled hash absent")?
                .to_owned(),
        );
        if [0, 3].contains(&node) && !log.contains("primary_pregen_complete") {
            bail!("primary {node} did not complete fixture pregeneration");
        }
        if [0, 3].contains(&node) {
            let centers = log.lines().map(strip_ansi).find(|line| {
                line.contains("cluster e2e primary additional centers pregenerated")
                    && structured_value(line, "centers") == Some(MULTI_PRIMARY_PREGEN_CHUNK_CENTERS)
                    && structured_value(line, "complete") == Some("true")
            });
            if centers.is_none() {
                bail!("primary {node} has no complete distant-leg pregeneration evidence");
            }
        }
    }
    if hashes.len() != 1 || elected.len() != 1 {
        bail!("primary resolution election or ground outcome diverged");
    }
    Ok(())
}

async fn verify_isolated_membership(
    nodes: &[Node],
    supervisor: &mut Supervisor,
    scenario: &str,
    deadline: &Deadline,
) -> Result<()> {
    let secondary = nodes
        .iter()
        .find(|node| node.id == 2)
        .context("membership fixture lacks candidate secondary")?;
    let endpoint = secondary
        .client_java
        .as_deref()
        .context("membership fixture lacks Java forwarding endpoint")?;
    let (client, mut events) = connect(BOT_B, endpoint, deadline).await?;
    let login = wait_login(&mut events, deadline, "ack-timeout candidate login", None).await?;
    let lobby = wait_virtual_lobby_spawn(
        &mut events,
        login,
        deadline,
        "ack-timeout candidate virtual lobby",
        None,
    )
    .await?;
    lobby.validate_virtual_lobby_scene()?;
    client.chat("/list");
    wait_chat_contains(
        &mut events,
        BOT_B,
        deadline,
        "candidate lobby remains responsive before admission",
    )
    .await?;
    if scenario == "membership-expand-set" {
        client.chat("/clustertest membership-expand-set");
        wait_chat_exact(
            &mut events,
            "cluster test membership-expand-set armed",
            deadline,
            "dynamic required-set expansion",
        )
        .await?;
    } else if scenario == "membership-peer-removal" {
        client.chat("/clustertest membership-waiting");
        wait_chat_exact(
            &mut events,
            "cluster test membership-waiting active",
            deadline,
            "candidate requires node1 before removal",
        )
        .await?;
        supervisor.stop_node(1, deadline).await?;
    }
    within(deadline, "candidate admitted through ten-second acknowledgement path", async {
        let mut centered = false;
        let mut batches = 0;
        let mut chunks = 0;
        while let Some(event) = events.recv().await {
            match event {
                Event::Packet(packet) => match packet.as_ref() {
                    ClientboundGamePacket::Login(_) | ClientboundGamePacket::Respawn(_) | ClientboundGamePacket::StartConfiguration(_) => {
                        bail!("acknowledgement-path handoff sent a terrain-loading packet");
                    }
                    ClientboundGamePacket::SetChunkCacheCenter(_) => centered = true,
                    ClientboundGamePacket::ChunkBatchFinished(_) if centered => batches += 1,
                    ClientboundGamePacket::LevelChunkWithLight(_) if centered => chunks += 1,
                    ClientboundGamePacket::PlayerPosition(_) if centered => {
                        if batches < 2 || chunks == 0 {
                            bail!("acknowledgement-path handoff teleported before pipelined real chunks");
                        }
                        return Ok(());
                    }
                    _ => {}
                },
                Event::Disconnect(reason) => bail!("candidate disconnected while awaiting dynamic admission: {reason:?}"),
                _ => {}
            }
        }
        bail!("candidate never promoted through acknowledgement path")
    }).await?;
    wait_until(
        &client,
        deadline,
        "acknowledgement-path real position",
        || {
            Ok(client
                .position()
                .ok()
                .filter(|pos| pos.x.abs() < 100_000.0 && pos.z.abs() < 100_000.0)
                .map(|_| ()))
        },
    )
    .await?;
    client.disconnect();
    eprintln!(
        "passed {scenario} client behavior: virtual lobby responsive, then pipelined real chunks and teleport without terrain transition"
    );
    Ok(())
}

async fn exercise(
    nodes: &[Node],
    executable: &Path,
    supervisor: &mut Supervisor,
    deadline: &Deadline,
) -> Result<ExerciseEvidence> {
    let one = nodes
        .iter()
        .find(|node| node.id == 1)
        .context("missing secondary one")?;
    let two = nodes
        .iter()
        .find(|node| node.id == 2)
        .context("missing secondary two")?;
    let lobby_started = time::Instant::now();
    let ((bot_a, mut a_events), (bot_b, mut b_events)) = tokio::try_join!(
        connect(
            BOT_A,
            one.client_java
                .as_deref()
                .context("secondary one forwarding listener missing")?,
            deadline,
        ),
        connect(
            BOT_B,
            two.client_java
                .as_deref()
                .context("secondary two forwarding listener missing")?,
            deadline,
        ),
    )?;
    let connected_elapsed = lobby_started.elapsed();
    for client in [&bot_a, &bot_b] {
        client.set_client_information(ClientInformation {
            view_distance: client_view_distance(),
            allows_listing: true,
            ..ClientInformation::default()
        })?;
    }
    let (a_login, b_login) = tokio::try_join!(
        wait_login(
            &mut a_events,
            deadline,
            "secondary one login",
            Some(lobby_started)
        ),
        wait_login(
            &mut b_events,
            deadline,
            "secondary two login",
            Some(lobby_started)
        ),
    )?;
    let login_elapsed = lobby_started.elapsed();
    let (a_lobby, b_lobby) = tokio::try_join!(
        wait_virtual_lobby_spawn(
            &mut a_events,
            a_login,
            deadline,
            "secondary one virtual lobby",
            Some(lobby_started),
        ),
        wait_virtual_lobby_spawn(
            &mut b_events,
            b_login,
            deadline,
            "secondary two virtual lobby",
            Some(lobby_started),
        ),
    )?;
    a_lobby.validate_virtual_lobby()?;
    b_lobby.validate_virtual_lobby()?;
    for client in [&bot_a, &bot_b] {
        client.set_client_information(ClientInformation {
            view_distance: client_view_distance(),
            allows_listing: true,
            ..ClientInformation::default()
        })?;
    }
    let lobby_elapsed = lobby_started.elapsed();
    let handshake_elapsed = LOBBY_LOGIN_HANDSHAKE
        .get()
        .context("proxy did not observe an initial login-state handshake")?
        .elapsed();
    eprintln!(
        "lobby timing client_connect={connected_elapsed:?} client_login={login_elapsed:?} client_full_scene={lobby_elapsed:?} login_handshake_to_full_scene={handshake_elapsed:?}"
    );
    if handshake_elapsed > Duration::from_secs(1) {
        bail!(
            "virtual lobby entry exceeded one second from login handshake: {handshake_elapsed:?}"
        );
    }
    tokio::try_join!(
        wait_for_forwarded_profiles(&bot_a, deadline, "secondary one lobby profiles"),
        wait_for_forwarded_profiles(&bot_b, deadline, "secondary two lobby profiles"),
    )?;
    wait_ticks(&bot_a, 5, deadline, "lobby listing publication").await?;
    tokio::try_join!(
        assert_status_profiles(one, &[BOT_A, BOT_B], deadline, "secondary one lobby status"),
        assert_status_profiles(two, &[BOT_A, BOT_B], deadline, "secondary two lobby status"),
    )?;
    if bot_a.entity_id_by_uuid(offline_uuid(BOT_B)).is_some()
        || bot_b.entity_id_by_uuid(offline_uuid(BOT_A)).is_some()
    {
        bail!("virtual lobby exposed a real remote player entity before primary startup");
    }
    assert_player_suggestions(
        &bot_a,
        &mut a_events,
        &[BOT_A, BOT_B],
        &[],
        0x5a17,
        deadline,
        "lobby global player suggestions",
    )
    .await?;
    bot_a.chat("/hide");
    wait_until(&bot_b, deadline, "pre-primary hidden lobby tab", || {
        let tab = bot_b.tab_list()?;
        Ok((!tab.contains_key(&offline_uuid(BOT_A))).then_some(()))
    })
    .await?;
    assert_player_suggestions(
        &bot_b,
        &mut b_events,
        &[BOT_B],
        &[BOT_A],
        0x5a18,
        deadline,
        "pre-primary hidden lobby suggestions",
    )
    .await?;
    wait_ticks(
        &bot_b,
        3,
        deadline,
        "pre-primary hidden lobby status publication",
    )
    .await?;
    tokio::try_join!(
        assert_status_profiles(one, &[BOT_B], deadline, "secondary one hidden lobby status"),
        assert_status_profiles(two, &[BOT_B], deadline, "secondary two hidden lobby status"),
    )?;
    bot_a.chat("/hide");
    wait_until(&bot_b, deadline, "pre-primary restored lobby tab", || {
        let tab = bot_b.tab_list()?;
        Ok(tab.contains_key(&offline_uuid(BOT_A)).then_some(()))
    })
    .await?;
    assert_player_suggestions(
        &bot_b,
        &mut b_events,
        &[BOT_A, BOT_B],
        &[],
        0x5a19,
        deadline,
        "pre-primary restored lobby suggestions",
    )
    .await?;
    bot_a.chat("/tp 0 80 6");
    wait_chat_exact(
        &mut a_events,
        "An entity is required to run this command here",
        deadline,
        "pre-primary entity-required command",
    )
    .await?;
    bot_a.chat("/list");
    wait_chat_contains(&mut a_events, BOT_B, deadline, "pre-primary global /list").await?;
    wait_ticks(
        &bot_b,
        3,
        deadline,
        "pre-primary restored lobby status publication",
    )
    .await?;
    tokio::try_join!(
        assert_status_profiles(
            one,
            &[BOT_A, BOT_B],
            deadline,
            "secondary one restored lobby status"
        ),
        assert_status_profiles(
            two,
            &[BOT_A, BOT_B],
            deadline,
            "secondary two restored lobby status"
        ),
    )?;
    bot_a.chat("/exitlobby");
    wait_chat_exact(
        &mut a_events,
        "You left the lobby wait room.",
        deadline,
        "pre-primary exitlobby",
    )
    .await?;
    assert_lobby_chat(&bot_a, &mut b_events, deadline).await?;
    assert_lobby_keepalive(&mut a_events, deadline).await?;
    bot_a.chat(&format!("/deop {BOT_B}"));
    wait_chat_exact(
        &mut a_events,
        &format!("Queued /deop for {BOT_B}; pending primary durability."),
        deadline,
        "pre-primary queued moderation",
    )
    .await?;
    let primary = nodes
        .iter()
        .find(|node| node.role == "primary")
        .context("missing primary")?;
    supervisor.start_node(executable, primary)?;
    tokio::try_join!(
        wait_for_local_promotion(
            &bot_a,
            &mut a_events,
            BOT_B,
            true,
            deadline,
            "player A real-world promotion"
        ),
        wait_for_local_promotion(
            &bot_b,
            &mut b_events,
            BOT_A,
            false,
            deadline,
            "player B real-world promotion"
        ),
    )?;
    let remote_visibility_started = time::Instant::now();
    let (bot_b_uuid, bot_a_uuid) = tokio::try_join!(
        wait_for_player(
            &bot_a,
            BOT_B,
            deadline,
            "remote player B entity after both promotions"
        ),
        wait_for_player(
            &bot_b,
            BOT_A,
            deadline,
            "remote player A entity after both promotions"
        ),
    )?;
    eprintln!(
        "both remote player entities visible after {:?}",
        remote_visibility_started.elapsed()
    );
    verify_cross_peer_chat_commands(&bot_a, &bot_b, &mut a_events, &mut b_events, deadline).await?;
    prepare_arena(&bot_a, &bot_b, &mut a_events, deadline).await?;
    verify_invsee_window(&bot_a, &mut a_events, &mut b_events, BOT_B, deadline).await?;
    verify_remote_interaction(
        &bot_a,
        bot_b_uuid,
        &mut a_events,
        deadline,
        "remote player right-click after initial promotion",
    )
    .await?;
    verify_mining_input(&bot_a, &mut a_events, deadline).await?;
    verify_item_placement_and_rejection(&bot_a, &bot_b, &mut a_events, deadline).await?;
    let (mut unrelated, mut unrelated_events) = connect(
        BOT_C,
        two.unrelated_java
            .as_deref()
            .context("secondary one unrelated forwarding listener missing")?,
        deadline,
    )
    .await?;
    eprintln!("unrelated client connector returned");
    let unrelated_login = wait_login(
        &mut unrelated_events,
        deadline,
        "unrelated player login",
        None,
    )
    .await?;
    let unrelated_lobby = wait_virtual_lobby_spawn(
        &mut unrelated_events,
        unrelated_login,
        deadline,
        "unrelated player virtual lobby",
        None,
    )
    .await?;
    unrelated_lobby.validate_virtual_lobby_scene()?;
    wait_for_player_transition(
        &unrelated,
        &mut unrelated_events,
        BOT_A,
        deadline,
        "unrelated player handoff",
    )
    .await?;
    tokio::try_join!(
        assert_status_profiles_with_count(
            one,
            &[BOT_A, BOT_B],
            3,
            deadline,
            "secondary one opt-out status"
        ),
        assert_status_profiles_with_count(
            two,
            &[BOT_A, BOT_B],
            3,
            deadline,
            "secondary two opt-out status"
        ),
    )?;
    let unrelated_before = unrelated.position()?;
    prepare_arena(&bot_a, &bot_b, &mut a_events, deadline).await?;
    wait_ticks(&unrelated, 2, deadline, "unrelated arena exclusion").await?;
    if position_changed(unrelated.position()?, unrelated_before) {
        bail!("arena preparation changed an unrelated joined player")
    }
    verify_beacon_menu(&bot_a, &mut a_events, deadline).await?;
    spec_scenarios::run_world_scenarios(
        &bot_a,
        &bot_b,
        Some(&unrelated),
        &mut a_events,
        &mut b_events,
        deadline,
    )
    .await?;
    spec_scenarios::verify_handoff_revision_mismatch(one, &bot_a, &bot_b, &mut a_events, deadline)
        .await?;
    spec_scenarios::verify_login_plugin_cancel(one, &bot_a, deadline).await?;
    spec_scenarios::verify_login_plugin_delay(one, &bot_a, &mut a_events, deadline).await?;
    spec_scenarios::verify_handoff_callback_early_confirm(
        one,
        &bot_a,
        &bot_b,
        &mut a_events,
        &mut b_events,
        deadline,
    )
    .await?;
    if let Some((replacement, replacement_events)) = spec_scenarios::verify_live_lease_reconnect(
        two,
        &bot_a,
        &mut a_events,
        &unrelated,
        deadline,
    )
    .await?
    {
        unrelated = replacement;
        unrelated_events = replacement_events;
    }
    verify_global_moderation_roundtrip(
        &bot_a,
        &mut a_events,
        &unrelated,
        &mut unrelated_events,
        two,
        deadline,
    )
    .await?;
    unrelated.disconnect();
    time::sleep(Duration::from_millis(100)).await;
    verify_hide_visibility(
        &bot_a,
        &bot_b,
        &mut a_events,
        &mut b_events,
        one,
        two,
        deadline,
    )
    .await?;
    verify_manual_lobby_reentry(
        &bot_a,
        &bot_b,
        bot_a_uuid,
        &mut a_events,
        &mut b_events,
        deadline,
    )
    .await?;
    verify_primary_drop_lobby_capability(
        &bot_a,
        &bot_b,
        bot_a_uuid,
        &mut a_events,
        &mut b_events,
        supervisor,
        executable,
        primary,
        deadline,
    )
    .await?;
    verify_remote_interaction(
        &bot_a,
        bot_b_uuid,
        &mut a_events,
        deadline,
        "remote player right-click",
    )
    .await?;
    verify_remote_visuals(&bot_a, &bot_b, bot_a_uuid, &mut b_events, deadline).await?;
    let controller_positions =
        verify_controller_divergence(&bot_a, &bot_b, bot_a_uuid, &mut a_events, deadline).await?;
    prepare_arena(&bot_a, &bot_b, &mut a_events, deadline).await?;
    let before_move = player_position(&bot_b, bot_a_uuid)?;
    bot_a.walk(WalkDirection::Forward);
    wait_ticks(&bot_a, 5, deadline, "local movement").await?;
    bot_a.walk(WalkDirection::None);
    wait_for_position_change(
        &bot_b,
        bot_a_uuid,
        before_move,
        deadline,
        "remote movement replication",
    )
    .await?;
    wait_for_player(&bot_a, BOT_B, deadline, "remote player B after movement").await?;
    fight_until_death_and_respawn(&bot_a, bot_b_uuid, &mut a_events, &mut b_events, deadline)
        .await?;
    summon_secondary_owned_zombie(&bot_a, &bot_b, deadline).await?;
    bot_a.chat("/clustertest departure-admin-disconnect");
    wait_chat_exact(
        &mut a_events,
        "cluster test departure-admin-disconnect staged",
        deadline,
        "disconnect departure admin submission",
    )
    .await?;
    spec_scenarios::prepare_cursor_crafting_departure(&bot_a, &mut a_events, deadline).await?;
    let delayed_chat_started = if env::var("PUMPKIN_E2E_SPEC_SCENARIOS")
        .unwrap_or_default()
        .split(',')
        .any(|id| id == "chat-departure-delay")
    {
        let started = time::Instant::now();
        bot_a.chat("e2e-delayed-chat-departure");
        Some(started)
    } else {
        eprintln!(
            "untested chat-departure-delay: select PUMPKIN_E2E_SPEC_SCENARIOS=chat-departure-delay after feature-only delayed chat plugin callback exists"
        );
        None
    };
    let delayed_command_started = if env::var("PUMPKIN_E2E_SPEC_SCENARIOS")
        .unwrap_or_default()
        .split(',')
        .any(|id| id == "command-departure-delay")
    {
        let started = time::Instant::now();
        bot_a.chat("/me e2e-delayed-command-departure");
        Some(started)
    } else {
        eprintln!(
            "untested command-departure-delay: select PUMPKIN_E2E_SPEC_SCENARIOS=command-departure-delay after feature-only delayed command plugin callback exists"
        );
        None
    };
    spec_scenarios::arm_packet_preflight_departure(
        &bot_a,
        &mut a_events,
        deadline,
        "packet-preflight-disconnect",
    )
    .await?;
    bot_a.disconnect();
    time::sleep(Duration::from_millis(100)).await;
    let (reconnected_a, mut reconnect_events) = connect(
        BOT_A,
        one.client_java
            .as_deref()
            .context("secondary one forwarding listener missing")?,
        deadline,
    )
    .await?;
    let reconnect_login = wait_login(
        &mut reconnect_events,
        deadline,
        "same-secondary reconnect login",
        None,
    )
    .await?;
    let mut reconnect_lobby = wait_virtual_lobby_spawn(
        &mut reconnect_events,
        reconnect_login,
        deadline,
        "same-secondary reconnect lobby",
        None,
    )
    .await?;
    reconnect_lobby.validate_virtual_lobby()?;
    if let Some(started) = delayed_chat_started.or(delayed_command_started) {
        verify_retiring_reconnect_lobby(
            &reconnected_a,
            &mut reconnect_events,
            &mut reconnect_lobby,
            started,
            deadline,
        )
        .await?;
    }
    if let Some(started) = delayed_chat_started {
        wait_chat_contains(
            &mut b_events,
            "e2e-delayed-chat-departure",
            deadline,
            "chat admitted before departure drain",
        )
        .await?;
        if started.elapsed() < Duration::from_secs(3) {
            bail!("chat plugin callback was not actually delayed across disconnect");
        }
    }
    if let Some(started) = delayed_command_started {
        wait_chat_contains(
            &mut b_events,
            "e2e-delayed-command-departure",
            deadline,
            "command admitted before departure drain",
        )
        .await?;
        if started.elapsed() < Duration::from_secs(3) {
            bail!("command plugin callback was not actually delayed across disconnect");
        }
    }
    wait_first_reconnect_progress(&mut reconnect_events, &mut reconnect_lobby, deadline).await?;
    reconnect_lobby.validate_warm_reconnect()?;

    wait_for_player_transition(
        &reconnected_a,
        &mut reconnect_events,
        BOT_B,
        deadline,
        "same-secondary retained handoff",
    )
    .await?;
    spec_scenarios::verify_cursor_crafting_after_reconnect(&reconnected_a, deadline).await?;
    wait_for_position(
        &reconnected_a,
        Vec3::new(0.5, 80.0, 4.5),
        deadline,
        "accepted admin position after disconnect",
    )
    .await?;
    reconnected_a.disconnect();
    time::sleep(Duration::from_millis(100)).await;
    let (cross_reconnected_a, mut cross_reconnect_events) = connect(
        BOT_A,
        two.alternate_java
            .as_deref()
            .context("secondary two alternate forwarding listener missing")?,
        deadline,
    )
    .await?;
    let cross_login = wait_login(
        &mut cross_reconnect_events,
        deadline,
        "cross-secondary reconnect login",
        None,
    )
    .await?;
    let cross_lobby = wait_virtual_lobby_spawn(
        &mut cross_reconnect_events,
        cross_login,
        deadline,
        "cross-secondary reconnect lobby",
        None,
    )
    .await?;
    cross_lobby.validate_virtual_lobby()?;
    wait_for_player_transition(
        &cross_reconnected_a,
        &mut cross_reconnect_events,
        BOT_B,
        deadline,
        "cross-secondary retained handoff",
    )
    .await?;
    let observed_entity_uuids =
        prove_primary_natural_spawn_boundary(&cross_reconnected_a, deadline).await?;
    verify_remote_spectate(
        &cross_reconnected_a,
        &mut cross_reconnect_events,
        BOT_B,
        deadline,
    )
    .await?;
    let saved_position = cross_reconnected_a.position()?;
    cross_reconnected_a.chat(&format!("/ban {BOT_C} persistence-check"));
    wait_chat_contains(
        &mut cross_reconnect_events,
        "Cluster admin change is durable on the primary.",
        deadline,
        "persistent ban before cluster restart",
    )
    .await?;
    assert_malformed_chunk_ack(one, deadline).await?;
    cross_reconnected_a.disconnect();
    bot_b.disconnect();
    time::sleep(Duration::from_millis(500)).await;
    supervisor.restart_all(executable, nodes, deadline).await?;
    let ((restored_a, mut restored_a_events), (restored_b, mut restored_b_events)) = tokio::try_join!(
        connect(
            BOT_A,
            one.client_java
                .as_deref()
                .context("restart A listener missing")?,
            deadline
        ),
        connect(
            BOT_B,
            two.client_java
                .as_deref()
                .context("restart B listener missing")?,
            deadline
        ),
    )?;
    let (a_login, b_login) = tokio::try_join!(
        wait_login(
            &mut restored_a_events,
            deadline,
            "post-restart A login",
            None
        ),
        wait_login(
            &mut restored_b_events,
            deadline,
            "post-restart B login",
            None
        ),
    )?;
    let (a_lobby, b_lobby) = tokio::try_join!(
        wait_virtual_lobby_spawn(
            &mut restored_a_events,
            a_login,
            deadline,
            "post-restart A lobby",
            None
        ),
        wait_virtual_lobby_spawn(
            &mut restored_b_events,
            b_login,
            deadline,
            "post-restart B lobby",
            None
        ),
    )?;
    a_lobby.validate_virtual_lobby_scene()?;
    b_lobby.validate_virtual_lobby_scene()?;
    tokio::try_join!(
        wait_for_forwarded_profiles(&restored_a, deadline, "post-restart A profile textures"),
        wait_for_forwarded_profiles(&restored_b, deadline, "post-restart B profile textures"),
    )?;
    tokio::try_join!(
        wait_for_local_promotion(
            &restored_a,
            &mut restored_a_events,
            BOT_B,
            false,
            deadline,
            "post-restart A promotion"
        ),
        wait_for_local_promotion(
            &restored_b,
            &mut restored_b_events,
            BOT_A,
            false,
            deadline,
            "post-restart B promotion"
        ),
    )?;
    tokio::try_join!(
        assert_status_profiles(one, &[BOT_A, BOT_B], deadline, "post-restart node1 status"),
        assert_status_profiles(two, &[BOT_A, BOT_B], deadline, "post-restart node2 status"),
    )?;
    wait_for_position(
        &restored_a,
        saved_position,
        deadline,
        "post-restart persisted resume position",
    )
    .await?;
    spec_scenarios::verify_ender_chest_after_restart(&restored_a, &mut restored_a_events, deadline)
        .await?;
    assert_persisted_ban(two, deadline).await?;
    if env::var("PUMPKIN_E2E_SPEC_SCENARIOS")
        .unwrap_or_default()
        .split(',')
        .any(|id| id == "entity-owner-leave")
    {
        verify_entity_owner_graceful_leave(
            &restored_a,
            &restored_b,
            &mut restored_a_events,
            &mut restored_b_events,
            supervisor,
            one,
            deadline,
        )
        .await?;
    } else {
        eprintln!(
            "untested entity-owner-leave: select PUMPKIN_E2E_SPEC_SCENARIOS=entity-owner-leave after canonical entity ownership-transfer hook exists"
        );
        restored_a.disconnect();
    }
    restored_b.disconnect();
    Ok(ExerciseEvidence {
        observed_entity_uuids,
        controller_positions,
    })
}

async fn verify_entity_owner_graceful_leave(
    actor: &Client,
    observer: &Client,
    actor_events: &mut tokio::sync::mpsc::UnboundedReceiver<Event>,
    observer_events: &mut tokio::sync::mpsc::UnboundedReceiver<Event>,
    supervisor: &mut Supervisor,
    old_owner: &Node,
    deadline: &Deadline,
) -> Result<()> {
    let existing = zombie_uuids(observer)?;
    actor.chat("/clustertest entity-owner-leave");
    wait_chat_exact(
        actor_events,
        "cluster test entity-owner-leave accepted",
        deadline,
        "owned entity setup",
    )
    .await?;
    let entity = wait_until(
        observer,
        deadline,
        "owned zombie visible before graceful leave",
        || {
            Ok(zombie_uuids(observer)?
                .difference(&existing)
                .next()
                .copied())
        },
    )
    .await?;
    actor.disconnect();
    wait_observer_presence(
        observer,
        observer_events,
        offline_uuid(BOT_A),
        false,
        deadline,
        "old owner has no hosted player",
    )
    .await?;
    let before = observer
        .entity_by_uuid(entity)
        .context("owned zombie vanished before leave")?
        .position()
        .context("owned zombie position absent")?;
    supervisor.stop_node(old_owner.id, deadline).await?;
    wait_for_position_change(
        observer,
        entity,
        before,
        deadline,
        "transferred entity ticked by new owner",
    )
    .await?;
    if observer.entity_by_uuid(entity).is_none() {
        bail!("graceful old-owner leave removed the transferred entity");
    }
    eprintln!(
        "passed entity-owner-leave: playerless secondary exited and former entity continued moving on the other holder"
    );
    Ok(())
}

async fn assert_malformed_chunk_ack(node: &Node, deadline: &Deadline) -> Result<()> {
    let (client, mut events) = connect(
        BOT_D,
        node.malformed_java
            .as_deref()
            .context("malformed-ack forwarding listener missing")?,
        deadline,
    )
    .await?;
    let login = wait_login(&mut events, deadline, "malformed-ack client login", None).await?;
    let lobby = wait_virtual_lobby_spawn(
        &mut events,
        login,
        deadline,
        "malformed-ack virtual lobby",
        None,
    )
    .await?;
    lobby.validate_virtual_lobby_scene()?;
    for _ in 0..3 {
        client.write_packet(ServerboundChunkBatchReceived {
            desired_chunks_per_tick: 20.0,
        });
    }
    within(deadline, "malformed chunk acknowledgement disconnect", async {
        while let Some(event) = events.recv().await {
            if let Event::Disconnect(reason) = event {
                let reason = reason.map_or_else(
                    || String::from("connection closed"),
                    |text| text.to_string(),
                );
                if !reason.contains("Unexpected chunk batch acknowledgement") {
                    bail!("malformed chunk acknowledgement used the wrong disconnect: {reason}");
                }
                while let Ok(event) = events.try_recv() {
                    if matches!(event, Event::Disconnect(_)) {
                        bail!("malformed chunk acknowledgement produced repeated disconnect events");
                    }
                }
                return Ok(());
            }
        }
        bail!("malformed chunk acknowledgement did not disconnect the client")
    })
    .await
}

async fn assert_lobby_keepalive(
    events: &mut tokio::sync::mpsc::UnboundedReceiver<Event>,
    deadline: &Deadline,
) -> Result<()> {
    within(deadline, "virtual lobby keepalive", async {
        while let Some(event) = events.recv().await {
            match event {
                Event::KeepAlive(_) => return Ok(()),
                Event::Disconnect(reason) => bail!(
                    "Azalea disconnected before a lobby keepalive: {}",
                    reason.map_or_else(
                        || String::from("connection closed"),
                        |text| text.to_string()
                    )
                ),
                _ => {}
            }
        }
        bail!("Azalea event stream closed before a lobby keepalive")
    })
    .await
}

async fn verify_controller_divergence(
    actor: &Client,
    observer: &Client,
    actor_uuid: Uuid,
    actor_events: &mut tokio::sync::mpsc::UnboundedReceiver<Event>,
    deadline: &Deadline,
) -> Result<Vec<Vec3>> {
    actor.chat("/clustertest controller-divergence");
    let before = player_position(observer, actor_uuid)?;
    for (offset, yaw, pitch) in [(0.1, 11.0, -4.0), (0.2, 22.0, -8.0), (0.3, 33.0, -12.0)] {
        actor.write_packet(ServerboundMovePlayerPosRot {
            pos: Vec3::new(before.x + offset, before.y, before.z),
            look_direction: LookDirection::new(yaw, pitch),
            flags: MoveFlags {
                on_ground: true,
                horizontal_collision: false,
            },
        });
    }
    actor.write_packet(ServerboundPlayerInput {
        forward: true,
        shift: true,
        ..Default::default()
    });
    if position_changed(player_position(observer, actor_uuid)?, before) {
        bail!(
            "remote movement was applied directly from the client packet handler before a server tick"
        )
    }
    within(deadline, "controller divergence observation", async {
        let mut ticks = observer.get_tick_broadcaster();
        let mut positions = Vec::new();
        let mut effective_observed = false;
        loop {
            tokio::select! {
                tick = next_tick(&mut ticks) => {
                    tick.context("Azalea tick stream closed during controller divergence observation")?;
                    let after = player_position(observer, actor_uuid)?;
                    if position_changed(after, before)
                        && positions
                            .last()
                            .is_none_or(|previous| position_changed(after, *previous))
                    {
                        positions.push(after);
                    }
                    if (after.x - before.x).abs() >= 2.5 {
                        effective_observed = true;
                    }
                }
                event = actor_events.recv() => {
                    match event.context("Azalea event stream closed during controller divergence observation")? {
                        Event::Chat(chat)
                            if chat.message().to_string()
                                == "cluster test controller-divergence accepted" =>
                        {
                            if !effective_observed {
                                bail!("controller divergence was accepted before the remote effective position was observed")
                            }
                            if positions.len() != 1 {
                                bail!("controller divergence published {} remote movement states instead of one", positions.len())
                            }
                            let remote = observer
                                .entity_by_uuid(actor_uuid)
                                .context("controller divergence remote actor disappeared")?;
                            let look = remote.component::<LookDirection>()?;
                            if (look.y_rot() - 33.0).abs() > 0.01
                                || (look.x_rot() + 12.0).abs() > 0.01
                            {
                                bail!("controller divergence lost the final claimed rotation")
                            }
                            if !remote.component::<AbstractEntityShiftKeyDown>()?.0 {
                                bail!("controller divergence lost the final claimed input visual")
                            }
                            return Ok(positions)
                        }
                        Event::Disconnect(reason) => bail!(
                            "Azalea disconnected during controller divergence: {}",
                            reason.map_or_else(
                                || String::from("connection closed"),
                                |text| text.to_string()
                            )
                        ),
                        _ => {}
                    }
                }
            }
        }
    })
    .await
}

async fn assert_lobby_chat(
    sender: &Client,
    receiver_events: &mut tokio::sync::mpsc::UnboundedReceiver<Event>,
    deadline: &Deadline,
) -> Result<()> {
    const MESSAGE: &str = "pumpkin-cluster-e2e-lobby-chat";
    sender.chat(MESSAGE);
    within(deadline, "cross-secondary lobby chat", async {
        while let Some(event) = receiver_events.recv().await {
            match event {
                Event::Chat(chat) if chat.message().to_string().contains(MESSAGE) => return Ok(()),
                Event::Disconnect(reason) => bail!(
                    "Azalea disconnected while awaiting lobby chat: {}",
                    reason.map_or_else(
                        || String::from("connection closed"),
                        |text| text.to_string()
                    )
                ),
                _ => {}
            }
        }
        bail!("Azalea event stream closed before lobby chat arrived")
    })
    .await
}

async fn assert_player_suggestions(
    client: &Client,
    events: &mut tokio::sync::mpsc::UnboundedReceiver<Event>,
    required: &[&str],
    forbidden: &[&str],
    request_id: u32,
    deadline: &Deadline,
    stage: &str,
) -> Result<()> {
    assert_command_suggestions(
        client, events, "/msg ", required, forbidden, request_id, deadline, stage,
    )
    .await
}

async fn assert_command_suggestions(
    client: &Client,
    events: &mut tokio::sync::mpsc::UnboundedReceiver<Event>,
    command: &str,
    required: &[&str],
    forbidden: &[&str],
    request_id: u32,
    deadline: &Deadline,
    stage: &str,
) -> Result<()> {
    client.write_packet(ServerboundCommandSuggestion {
        id: request_id,
        command: String::from(command),
    });
    within(deadline, stage, async {
        while let Some(event) = events.recv().await {
            match event {
                Event::Packet(packet) => {
                    let ClientboundGamePacket::CommandSuggestions(response) = packet.as_ref()
                    else {
                        continue;
                    };
                    if response.id != request_id {
                        continue;
                    }
                    let values = response
                        .suggestions
                        .list()
                        .iter()
                        .map(|suggestion| suggestion.text())
                        .collect::<HashSet<_>>();
                    for name in required {
                        if !values.contains(*name) {
                            bail!("{stage} command suggestions are missing {name}; observed: {values:?}");
                        }
                    }
                    for name in forbidden {
                        if values.contains(*name) {
                            bail!("{stage} command suggestions expose {name}");
                        }
                    }
                    return Ok(());
                }
                Event::Disconnect(reason) => bail!(
                    "Azalea disconnected while awaiting lobby suggestions: {}",
                    reason.map_or_else(
                        || String::from("connection closed"),
                        |text| text.to_string()
                    )
                ),
                _ => {}
            }
        }
        bail!("Azalea event stream closed before lobby suggestions arrived")
    })
    .await
}

async fn wait_chat_exact(
    events: &mut tokio::sync::mpsc::UnboundedReceiver<Event>,
    expected: &str,
    deadline: &Deadline,
    stage: &str,
) -> Result<()> {
    within(deadline, stage, async {
        while let Some(event) = events.recv().await {
            match event {
                Event::Chat(chat) if chat.message().to_string() == expected => return Ok(()),
                Event::Disconnect(reason) => bail!(
                    "Azalea disconnected during {stage}: {}",
                    reason.map_or_else(
                        || String::from("connection closed"),
                        |text| text.to_string()
                    )
                ),
                _ => {}
            }
        }
        bail!("Azalea event stream closed during {stage}")
    })
    .await
}

async fn wait_chat_contains(
    events: &mut tokio::sync::mpsc::UnboundedReceiver<Event>,
    expected: &str,
    deadline: &Deadline,
    stage: &str,
) -> Result<()> {
    within(deadline, stage, async {
        while let Some(event) = events.recv().await {
            match event {
                Event::Chat(chat) if chat.message().to_string().contains(expected) => return Ok(()),
                Event::Disconnect(reason) => {
                    bail!("Azalea disconnected during {stage}: {reason:?}")
                }
                _ => {}
            }
        }
        bail!("Azalea event stream closed during {stage}")
    })
    .await
}

async fn wait_disconnect(
    events: &mut tokio::sync::mpsc::UnboundedReceiver<Event>,
    deadline: &Deadline,
    stage: &str,
) -> Result<()> {
    within(deadline, stage, async {
        while let Some(event) = events.recv().await {
            if matches!(event, Event::Disconnect(_)) {
                return Ok(());
            }
        }
        bail!("Azalea event stream closed without a disconnect during {stage}")
    })
    .await
}

async fn verify_global_moderation_roundtrip(
    operator: &Client,
    operator_events: &mut tokio::sync::mpsc::UnboundedReceiver<Event>,
    target: &Client,
    target_events: &mut tokio::sync::mpsc::UnboundedReceiver<Event>,
    node: &Node,
    deadline: &Deadline,
) -> Result<()> {
    for (index, command) in ["op", "deop"].into_iter().enumerate() {
        assert_command_suggestions(
            operator,
            operator_events,
            &format!("/{command} "),
            &[BOT_C],
            &[],
            0x5b00 + index as u32,
            deadline,
            &format!("global /{command} suggestions"),
        )
        .await?;
        operator.chat(&format!("/{command} {BOT_C}"));
        wait_chat_contains(
            operator_events,
            "Cluster admin change is durable on the primary.",
            deadline,
            &format!("durable /{command} on unrelated player"),
        )
        .await?;
        wait_permission_command_graph(target_events, command == "op", deadline).await?;
    }
    target.chat(&format!("/op {BOT_C}"));
    within(
        deadline,
        "deopped player cannot restore its own permission",
        async {
            while let Some(event) = target_events.recv().await {
                match event {
                    Event::Chat(chat) => {
                        let message = chat.message().to_string();
                        if message.contains("Queued /op")
                            || message.contains("durable on the primary")
                        {
                            bail!("deopped player submitted an unauthorized global op action");
                        }
                        let text = message.to_lowercase();
                        if text.contains("permission")
                            || text.contains("unknown")
                            || text.contains("incomplete command")
                        {
                            return Ok(());
                        }
                    }
                    Event::Disconnect(reason) => {
                        bail!("permission rejection disconnected target: {reason:?}")
                    }
                    _ => {}
                }
            }
            bail!("deopped player's unauthorized op received no rejection");
        },
    )
    .await?;
    assert_command_suggestions(
        operator,
        operator_events,
        "/kick ",
        &[BOT_C],
        &[],
        0x5b02,
        deadline,
        "global /kick suggestions",
    )
    .await?;
    operator.chat(&format!("/kick {BOT_C} e2e-kick"));
    wait_disconnect(
        target_events,
        deadline,
        "cross-peer /kick target disconnect",
    )
    .await?;
    let (kicked_return, mut kicked_events) = connect(
        BOT_C,
        node.unrelated_java
            .as_deref()
            .context("unrelated proxy missing")?,
        deadline,
    )
    .await?;
    let kicked_login = wait_login(&mut kicked_events, deadline, "post-kick login", None).await?;
    let kicked_lobby = wait_virtual_lobby_spawn(
        &mut kicked_events,
        kicked_login,
        deadline,
        "post-kick virtual lobby",
        None,
    )
    .await?;
    kicked_lobby.validate_virtual_lobby_scene()?;
    if ["op", "deop", "ban", "pardon"]
        .iter()
        .any(|command| kicked_lobby.command_roots.contains(*command))
    {
        bail!("unauthorized self-op restored operator commands after reconnect");
    }
    wait_for_player_transition(
        &kicked_return,
        &mut kicked_events,
        BOT_A,
        deadline,
        "post-kick real handoff",
    )
    .await?;
    assert_command_suggestions(
        operator,
        operator_events,
        "/ban ",
        &[BOT_C],
        &[],
        0x5b03,
        deadline,
        "global /ban suggestions",
    )
    .await?;
    operator.chat(&format!("/ban {BOT_C} e2e-ban"));
    wait_disconnect(
        &mut kicked_events,
        deadline,
        "cross-peer /ban target disconnect",
    )
    .await?;
    wait_chat_contains(
        operator_events,
        "Cluster admin change is durable on the primary.",
        deadline,
        "durable /ban",
    )
    .await?;
    assert_command_suggestions(
        operator,
        operator_events,
        "/pardon ",
        &[BOT_C],
        &[],
        0x5b04,
        deadline,
        "global /pardon suggestions",
    )
    .await?;
    operator.chat(&format!("/pardon {BOT_C}"));
    wait_chat_contains(
        operator_events,
        "Cluster admin change is durable on the primary.",
        deadline,
        "durable /pardon",
    )
    .await?;
    let (pardoned, mut pardoned_events) = connect(
        BOT_C,
        node.unrelated_java
            .as_deref()
            .context("unrelated proxy missing")?,
        deadline,
    )
    .await?;
    let pardon_login =
        wait_login(&mut pardoned_events, deadline, "post-pardon login", None).await?;
    let pardon_lobby = wait_virtual_lobby_spawn(
        &mut pardoned_events,
        pardon_login,
        deadline,
        "post-pardon virtual lobby",
        None,
    )
    .await?;
    pardon_lobby.validate_virtual_lobby_scene()?;
    wait_for_player_transition(
        &pardoned,
        &mut pardoned_events,
        BOT_A,
        deadline,
        "post-pardon real handoff",
    )
    .await?;
    pardoned.disconnect();
    Ok(())
}

async fn wait_permission_command_graph(
    events: &mut tokio::sync::mpsc::UnboundedReceiver<Event>,
    operator: bool,
    deadline: &Deadline,
) -> Result<()> {
    within(deadline, "remote operator command graph update", async {
        while let Some(event) = events.recv().await {
            match event {
                Event::Packet(packet) => {
                    let ClientboundGamePacket::Commands(commands) = packet.as_ref() else {
                        continue;
                    };
                    let root = commands
                        .entries
                        .get(commands.root_index as usize)
                        .context("permission command graph root invalid")?;
                    let names = root
                        .children
                        .iter()
                        .map(|child| {
                            Ok(commands
                                .entries
                                .get(*child as usize)
                                .context("permission command graph child invalid")?
                                .name())
                        })
                        .collect::<Result<Vec<_>>>()?;
                    if ["op", "deop", "ban", "pardon"]
                        .iter()
                        .all(|name| names.contains(&Some(*name)) == operator)
                    {
                        return Ok(());
                    }
                }
                Event::Disconnect(reason) => {
                    bail!("operator graph update disconnected player: {reason:?}")
                }
                _ => {}
            }
        }
        bail!("remote op/deop did not update the actual Java command graph");
    })
    .await
}

async fn assert_persisted_ban(node: &Node, deadline: &Deadline) -> Result<()> {
    let address = node
        .unrelated_java
        .as_deref()
        .context("post-restart banned profile proxy missing")?;
    within(deadline, "primary moderation persisted across restart", async {
        let (client, mut events) = match Client::join(Account::offline(BOT_C), address).await {
            Ok(connected) => connected,
            Err(error) => {
                if error.to_string().to_ascii_lowercase().contains("ban") {
                    return Ok(());
                }
                bail!("post-restart banned login failed for a different reason: {error}")
            }
        };
        while let Some(event) = events.recv().await {
            match event {
                Event::Disconnect(reason) => {
                    let reason = reason.map_or_else(String::new, |text| text.to_string());
                    if reason.to_ascii_lowercase().contains("ban") {
                        client.disconnect();
                        return Ok(());
                    }
                    bail!("post-restart banned profile disconnected for a different reason: {reason}")
                }
                Event::Login => bail!("primary restart admitted a persistently banned profile"),
                Event::Packet(packet) if matches!(packet.as_ref(), ClientboundGamePacket::Login(_)) => {
                    bail!("primary restart sent CLogin to a persistently banned profile")
                }
                _ => {}
            }
        }
        bail!("banned profile connection closed without a ban decision")
    }).await
}

async fn verify_cross_peer_chat_commands(
    bot_a: &Client,
    bot_b: &Client,
    a_events: &mut tokio::sync::mpsc::UnboundedReceiver<Event>,
    b_events: &mut tokio::sync::mpsc::UnboundedReceiver<Event>,
    deadline: &Deadline,
) -> Result<()> {
    bot_a.chat(&format!("/msg {BOT_B} e2e-private-a"));
    wait_chat_contains(
        b_events,
        "e2e-private-a",
        deadline,
        "cross-peer private message A to B",
    )
    .await?;
    bot_b.chat(&format!("/msg {BOT_A} e2e-private-b"));
    wait_chat_contains(
        a_events,
        "e2e-private-b",
        deadline,
        "cross-peer private message B to A",
    )
    .await?;
    bot_a.chat("/me e2e-action-a");
    wait_chat_contains(
        b_events,
        "e2e-action-a",
        deadline,
        "cross-peer /me delivery",
    )
    .await?;
    bot_a.chat("/say e2e-announcement-a");
    wait_chat_contains(
        b_events,
        "e2e-announcement-a",
        deadline,
        "cross-peer /say delivery",
    )
    .await?;
    bot_a.chat("/team add E2eFixture");
    bot_a.chat(&format!("/team join E2eFixture {BOT_A}"));
    bot_a.chat(&format!("/team join E2eFixture {BOT_B}"));
    wait_ticks(bot_a, 3, deadline, "team membership publication").await?;
    bot_b.chat("/teammsg e2e-team-b");
    wait_chat_contains(
        a_events,
        "e2e-team-b",
        deadline,
        "cross-peer team message B to A",
    )
    .await?;
    Ok(())
}

async fn verify_remote_visuals(
    actor: &Client,
    observer: &Client,
    actor_uuid: Uuid,
    events: &mut tokio::sync::mpsc::UnboundedReceiver<Event>,
    deadline: &Deadline,
) -> Result<()> {
    let entity_id = observer
        .entity_by_uuid(actor_uuid)
        .context("remote visual actor is absent")?
        .minecraft_id()?;
    actor.set_crouching(true)?;
    actor.set_direction(45.0, 0.0)?;
    actor.set_selected_hotbar_slot(1);
    within(deadline, "remote visual replication", async {
        let mut saw_metadata = false;
        let mut saw_head = false;
        let mut saw_equipment = false;
        while !(saw_metadata && saw_head && saw_equipment) {
            match events
                .recv()
                .await
                .context("Azalea event stream closed during remote visual replication")?
            {
                Event::Disconnect(reason) => bail!(
                    "Azalea disconnected during remote visual replication: {}",
                    reason.map_or_else(
                        || String::from("connection closed"),
                        |text| text.to_string()
                    )
                ),
                Event::Packet(packet) => match packet.as_ref() {
                    ClientboundGamePacket::SetEntityData(packet) if packet.id == entity_id => {
                        saw_metadata = true;
                    }
                    ClientboundGamePacket::RotateHead(packet) if packet.entity_id == entity_id => {
                        saw_head = true;
                    }
                    ClientboundGamePacket::SetEquipment(packet)
                        if packet.entity_id == entity_id =>
                    {
                        saw_equipment = true;
                    }
                    _ => {}
                },
                _ => {}
            }
        }
        Ok(())
    })
    .await?;
    within(deadline, "remote crouch state applied", async {
        let mut ticks = observer.get_tick_broadcaster();
        loop {
            let remote = observer
                .entity_by_uuid(actor_uuid)
                .context("remote visual actor despawned")?;
            let crouching = remote
                .get_component::<azalea::entity::metadata::AbstractEntityShiftKeyDown>()
                .is_some_and(|value| value.0);
            if crouching {
                return Ok(());
            }
            next_tick(&mut ticks)
                .await
                .context("Azalea tick stream closed before crouch application")?;
        }
    })
    .await?;
    actor.set_crouching(false)?;
    actor.set_selected_hotbar_slot(0);
    Ok(())
}

async fn verify_remote_interaction(
    client: &Client,
    target: Uuid,
    events: &mut tokio::sync::mpsc::UnboundedReceiver<Event>,
    deadline: &Deadline,
    stage: &str,
) -> Result<()> {
    let target = client
        .entity_by_uuid(target)
        .context("remote interaction target is absent")?;
    target.look_at()?;
    target.interact();
    within(deadline, stage, async {
        let mut ticks = client.get_tick_broadcaster();
        let mut observed = 0;
        while observed < 5 {
            tokio::select! {
                tick = next_tick(&mut ticks) => {
                    tick.context("Azalea tick stream closed after remote interaction")?;
                    observed += 1;
                }
                event = events.recv() => {
                    match event.context("Azalea event stream closed after remote interaction")? {
                        Event::Disconnect(reason) => bail!(
                            "remote player interaction disconnected the client: {}",
                            reason.map_or_else(
                                || String::from("connection closed"),
                                |text| text.to_string()
                            )
                        ),
                        Event::Login | Event::Spawn => {
                            bail!("remote player interaction triggered a world transition")
                        }
                        _ => {}
                    }
                }
            }
        }
        Ok(())
    })
    .await
}

async fn verify_mining_input(
    client: &Client,
    events: &mut tokio::sync::mpsc::UnboundedReceiver<Event>,
    deadline: &Deadline,
) -> Result<()> {
    let pos = BlockPos::new(0, 80, 0);
    client.write_packet(ServerboundPlayerAction {
        action: PlayerAction::StartDestroyBlock,
        pos,
        direction: Direction::Up,
        seq: 0x601,
    });
    within(deadline, "arena mining input", async {
        let mut ticks = client.get_tick_broadcaster();
        let mut observed = 0;
        let mut acknowledged = HashSet::new();
        while observed < 7 {
            tokio::select! {
                tick = next_tick(&mut ticks) => {
                    tick.context("Azalea tick stream closed after mining input")?;
                    observed += 1;
                    if observed == 2 {
                        client.write_packet(ServerboundPlayerAction {
                            action: PlayerAction::AbortDestroyBlock,
                            pos,
                            direction: Direction::Up,
                            seq: 0x602,
                        });
                    }
                }
                event = events.recv() => {
                    match event.context("Azalea event stream closed after mining input")? {
                        Event::Disconnect(reason) => bail!(
                            "mining input disconnected the client: {}",
                            reason.map_or_else(
                                || String::from("connection closed"),
                                |text| text.to_string()
                            )
                        ),
                        Event::Login | Event::Spawn => bail!("mining input triggered a world transition"),
                        Event::Packet(packet) => {
                            if let ClientboundGamePacket::BlockChangedAck(ack) = packet.as_ref() {
                                acknowledged.insert(ack.seq);
                            }
                        }
                        _ => {}
                    }
                }
            }
        }
        if !acknowledged.contains(&0x601) || !acknowledged.contains(&0x602) {
            bail!("mining start/abort did not receive both block-change ACKs: {acknowledged:?}")
        }
        Ok(())
    })
    .await
}

async fn verify_item_placement_and_rejection(
    actor: &Client,
    observer: &Client,
    events: &mut tokio::sync::mpsc::UnboundedReceiver<Event>,
    deadline: &Deadline,
) -> Result<()> {
    let target = BlockPos::new(0, 81, 2);
    for (test_id, sequence, expected_count, accepted) in [
        ("item-place", 0x671_u32, 1, true),
        ("item-stale", 0x672_u32, 2, false),
    ] {
        while events.try_recv().is_ok() {}
        actor.chat(&format!("/clustertest {test_id}"));
        wait_chat_exact(
            events,
            &format!("cluster test {test_id} accepted"),
            deadline,
            &format!("accepted {test_id} setup"),
        )
        .await?;
        wait_until(actor, deadline, "item fixture client inventory", || {
            let inventory = actor
                .get_inventory()?
                .menu()?
                .context("player inventory menu absent")?;
            Ok(inventory
                .slots()
                .get(40)
                .is_some_and(|stack| stack.kind() == ItemKind::Stone && stack.count() == 2)
                .then_some(()))
        })
        .await?;
        actor.set_selected_hotbar_slot(4);
        actor.write_packet(ServerboundUseItemOn {
            hand: InteractionHand::MainHand,
            block_hit: BlockHit {
                block_pos: BlockPos::new(0, 80, 2),
                direction: Direction::Up,
                location: Vec3::new(0.5, 80.5, 2.5),
                inside: false,
                world_border: false,
            },
            seq: sequence,
        });
        within(deadline, test_id, async {
            let mut ack = false;
            let mut corrected = false;
            let mut slot_count = false;
            let mut ticks = actor.get_tick_broadcaster();
            let mut elapsed = 0;
            while elapsed < 100 {
                tokio::select! {
                    tick = next_tick(&mut ticks) => {
                        tick.context("Azalea tick stream closed during item action")?;
                        elapsed += 1;
                        let expected = if accepted {
                            BlockKind::Stone.into()
                        } else {
                            BlockKind::Obsidian.into()
                        };
                        let actor_state = actor.world()?.read().get_block_state(target);
                        let observer_state = observer.world()?.read().get_block_state(target);
                        if ack && slot_count && (accepted || corrected)
                            && actor_state == Some(expected)
                            && observer_state == Some(expected)
                        {
                            return Ok(());
                        }
                    }
                    event = events.recv() => {
                        match event.context("item action event stream closed")? {
                            Event::Packet(packet) => match packet.as_ref() {
                                ClientboundGamePacket::BlockChangedAck(packet) if packet.seq == sequence => ack = true,
                                ClientboundGamePacket::BlockUpdate(packet) if packet.pos == target => corrected = true,
                                ClientboundGamePacket::ContainerSetSlot(packet)
                                    if packet.container_id == 0 && packet.slot == 40
                                        && packet.item_stack.count() == expected_count => slot_count = true,
                                ClientboundGamePacket::ContainerSetContent(packet)
                                    if packet.container_id == 0
                                        && packet.items.get(40).is_some_and(|item| item.count() == expected_count) => slot_count = true,
                                _ => {}
                            },
                            Event::Disconnect(reason) => bail!("{test_id} disconnected the client: {reason:?}"),
                            _ => {}
                        }
                    }
                }
            }
            bail!("{test_id} did not converge: ack={ack}, slot={slot_count}, correction={corrected}")
        }).await?;
    }
    Ok(())
}

async fn wait_for_forwarded_profiles(
    client: &Client,
    deadline: &Deadline,
    stage: &str,
) -> Result<()> {
    let outcome = time::timeout(
        Duration::from_secs(10),
        wait_until(client, deadline, stage, || {
            let tab = client.tab_list().context("reading global lobby tab list")?;
            for name in [BOT_A, BOT_B] {
                let uuid = offline_uuid(name);
                let Some(info) = tab.get(&uuid) else {
                    return Ok(None);
                };
                if info.profile.name != name || info.profile.uuid != uuid {
                    bail!("global profile identity differs for {name}");
                }
                let texture = info
                    .profile
                    .properties
                    .map
                    .get("textures")
                    .with_context(|| {
                        format!("global profile for {name} has no texture property")
                    })?;
                let expected_value = format!("pumpkin-e2e-texture-{name}");
                let expected_signature = format!("pumpkin-e2e-signature-{name}");
                if texture.value != expected_value
                    || texture.signature.as_deref() != Some(expected_signature.as_str())
                {
                    bail!("global texture property differs for {name}");
                }
            }
            Ok(Some(()))
        }),
    )
    .await;
    match outcome {
        Ok(result) => result,
        Err(_) => {
            let tab = client
                .tab_list()
                .context("reading tab list after profile deadline")?;
            let observed = tab
                .iter()
                .map(|(uuid, info)| {
                    format!(
                        "{}:{}:textures={}",
                        info.profile.name,
                        uuid,
                        info.profile.properties.map.contains_key("textures")
                    )
                })
                .collect::<Vec<_>>();
            bail!(
                "{stage} did not receive both forwarded profiles within 10 seconds; observed tab entries: {observed:?}"
            )
        }
    }
}

async fn verify_hide_visibility(
    actor: &Client,
    observer: &Client,
    actor_events: &mut tokio::sync::mpsc::UnboundedReceiver<Event>,
    observer_events: &mut tokio::sync::mpsc::UnboundedReceiver<Event>,
    one: &Node,
    two: &Node,
    deadline: &Deadline,
) -> Result<()> {
    actor.chat(format!("/deop {BOT_B}"));
    wait_ticks(actor, 5, deadline, "global deop propagation").await?;
    actor.chat("/hide");
    wait_chat_exact(
        actor_events,
        "You are now hidden.",
        deadline,
        "hide acknowledgement",
    )
    .await?;
    wait_chat_exact(
        observer_events,
        &format!("{BOT_A} left the game"),
        deadline,
        "cross-peer hidden leave presentation",
    )
    .await?;
    wait_until(observer, deadline, "hidden player removal", || {
        let tab = observer.tab_list().context("reading observer tab list")?;
        Ok((!tab.contains_key(&offline_uuid(BOT_A))
            && observer.entity_id_by_uuid(offline_uuid(BOT_A)).is_none())
        .then_some(()))
    })
    .await?;
    assert_player_suggestions(
        observer,
        observer_events,
        &[BOT_B],
        &[BOT_A],
        0x5a18,
        deadline,
        "hidden player suggestions",
    )
    .await?;
    tokio::try_join!(
        assert_status_profiles(one, &[BOT_B], deadline, "secondary one hidden status"),
        assert_status_profiles(two, &[BOT_B], deadline, "secondary two hidden status"),
    )?;
    actor.chat("/hide");
    wait_chat_exact(
        actor_events,
        "You are now visible.",
        deadline,
        "show acknowledgement",
    )
    .await?;
    wait_chat_exact(
        observer_events,
        &format!("{BOT_A} joined the game"),
        deadline,
        "cross-peer visible join presentation",
    )
    .await?;
    wait_until(observer, deadline, "visible player restoration", || {
        let tab = observer.tab_list().context("reading observer tab list")?;
        Ok((tab.contains_key(&offline_uuid(BOT_A))
            && observer.entity_id_by_uuid(offline_uuid(BOT_A)).is_some())
        .then_some(()))
    })
    .await?;
    assert_player_suggestions(
        observer,
        observer_events,
        &[BOT_A, BOT_B],
        &[],
        0x5a19,
        deadline,
        "visible player suggestions",
    )
    .await?;
    tokio::try_join!(
        assert_status_profiles(
            one,
            &[BOT_A, BOT_B],
            deadline,
            "secondary one restored status"
        ),
        assert_status_profiles(
            two,
            &[BOT_A, BOT_B],
            deadline,
            "secondary two restored status"
        ),
    )?;
    Ok(())
}

async fn verify_primary_drop_lobby_capability(
    actor: &Client,
    observer: &Client,
    actor_uuid: Uuid,
    actor_events: &mut tokio::sync::mpsc::UnboundedReceiver<Event>,
    observer_events: &mut tokio::sync::mpsc::UnboundedReceiver<Event>,
    supervisor: &mut Supervisor,
    executable: &Path,
    primary: &Node,
    deadline: &Deadline,
) -> Result<()> {
    while actor_events.try_recv().is_ok() {}
    while observer_events.try_recv().is_ok() {}
    let prior_position = actor.position()?;
    actor.chat("/lobby");
    wait_reentry_feedback(
        actor_events,
        "You are now waiting in the lobby.",
        deadline,
        "primary-drop lobby entry",
    )
    .await?;
    wait_observer_presence(
        observer,
        observer_events,
        actor_uuid,
        false,
        deadline,
        "primary-drop lobby replica removal",
    )
    .await?;
    let virtual_position = actor.position()?;
    supervisor.stop_node(primary.id, deadline).await?;
    actor.chat(&format!("/tp {BOT_B} 0.5 80 7.5"));
    wait_for_position(
        observer,
        Vec3::new(0.5, 80.0, 7.5),
        deadline,
        "primary-drop remote /tp",
    )
    .await?;
    wait_chat_contains(
        actor_events,
        "Teleported",
        deadline,
        "primary-drop remote /tp feedback",
    )
    .await?;
    if position_changed(actor.position()?, virtual_position)
        || observer.entity_id_by_uuid(actor_uuid).is_some()
    {
        bail!("primary-drop lobby command materialized the virtual actor")
    }
    assert_lobby_chat(actor, observer_events, deadline).await?;
    supervisor.start_node(executable, primary)?;
    actor.chat("/exitlobby");
    wait_reentry_feedback(
        actor_events,
        "You left the lobby wait room.",
        deadline,
        "primary-drop lobby exit",
    )
    .await?;
    wait_observer_presence(
        observer,
        observer_events,
        actor_uuid,
        true,
        deadline,
        "primary-drop replica restoration",
    )
    .await?;
    wait_for_position(
        actor,
        prior_position,
        deadline,
        "primary-drop resumed position",
    )
    .await?;
    Ok(())
}

async fn verify_manual_lobby_reentry(
    actor: &Client,
    observer: &Client,
    actor_uuid: Uuid,
    actor_events: &mut tokio::sync::mpsc::UnboundedReceiver<Event>,
    observer_events: &mut tokio::sync::mpsc::UnboundedReceiver<Event>,
    deadline: &Deadline,
) -> Result<()> {
    while actor_events.try_recv().is_ok() {}
    while observer_events.try_recv().is_ok() {}
    let packet_preflight_lobby = env::var("PUMPKIN_E2E_SPEC_SCENARIOS")
        .unwrap_or_default()
        .split(',')
        .any(|id| id == "packet-preflight-lobby");
    if packet_preflight_lobby {
        actor.chat(&format!("/op {BOT_B}"));
        wait_chat_exact(
            actor_events,
            &format!("Queued /op for {BOT_B}; pending primary durability."),
            deadline,
            "independent preflight operator grant queued",
        )
        .await?;
        wait_chat_exact(
            actor_events,
            "Cluster admin change is durable on the primary.",
            deadline,
            "independent preflight operator grant durable",
        )
        .await?;
        wait_permission_command_graph(observer_events, true, deadline).await?;
    }
    actor.chat("/clustertest departure-admin-lobby");
    wait_chat_exact(
        actor_events,
        "cluster test departure-admin-lobby staged",
        deadline,
        "manual lobby departure admin submission",
    )
    .await?;
    spec_scenarios::arm_packet_preflight_departure(
        actor,
        actor_events,
        deadline,
        "packet-preflight-lobby",
    )
    .await?;
    if packet_preflight_lobby {
        observer.chat("/clustertest packet-preflight-lobby-depart");
    } else {
        actor.chat("/lobby");
    }
    wait_reentry_feedback(
        actor_events,
        "You are now waiting in the lobby.",
        deadline,
        "manual lobby entry",
    )
    .await?;
    if packet_preflight_lobby {
        actor.chat(&format!("/deop {BOT_B}"));
        wait_chat_exact(
            actor_events,
            &format!("Queued /deop for {BOT_B}; pending primary durability."),
            deadline,
            "independent preflight operator cleanup queued",
        )
        .await?;
        wait_chat_exact(
            actor_events,
            "Cluster admin change is durable on the primary.",
            deadline,
            "independent preflight operator cleanup durable",
        )
        .await?;
    }
    wait_observer_presence(
        observer,
        observer_events,
        actor_uuid,
        false,
        deadline,
        "manual lobby replica removal",
    )
    .await?;
    let profile = observer
        .tab_list()
        .context("reading tab list during manual lobby")?
        .get(&actor_uuid)
        .cloned()
        .context("manual lobby removed the persistent tab profile")?;
    let virtual_position = actor.position()?;
    actor.chat(format!("/tp {BOT_B} 0.5 80 6.5"));
    wait_for_position(
        observer,
        Vec3::new(0.5, 80.0, 6.5),
        deadline,
        "lobby operator remote target teleport",
    )
    .await?;
    wait_chat_contains(
        actor_events,
        "Teleported",
        deadline,
        "lobby operator remote teleport feedback",
    )
    .await?;
    for command in [
        format!("/tp {BOT_B} ~ ~ ~"),
        format!("/tp {BOT_B} ^ ^ ^"),
        format!("/tp {BOT_B} 0.5 80 6.5 ~ ~"),
    ] {
        let target_before = observer.position()?;
        actor.chat(&command);
        wait_chat_exact(
            actor_events,
            "An entity is required to run this command here",
            deadline,
            "virtual executor relative teleport rejection",
        )
        .await?;
        for _ in 0..10 {
            wait_ticks(
                observer,
                1,
                deadline,
                "relative lobby teleport remains unchanged",
            )
            .await?;
            if position_changed(observer.position()?, target_before)
                || position_changed(actor.position()?, virtual_position)
                || observer.entity_id_by_uuid(actor_uuid).is_some()
            {
                bail!("relative lobby teleport mutated actor/target: {command}");
            }
        }
    }
    actor.chat("/tp 0 80 6");
    wait_chat_exact(
        actor_events,
        "An entity is required to run this command here",
        deadline,
        "lobby operator entity-required command",
    )
    .await?;
    if position_changed(actor.position()?, virtual_position)
        || observer.entity_id_by_uuid(actor_uuid).is_some()
    {
        bail!("lobby operator command materialized or moved the virtual actor");
    }
    for command in ["op", "deop"] {
        actor.chat(&format!("/{command} {BOT_B}"));
        wait_chat_exact(
            actor_events,
            &format!("Queued /{command} for {BOT_B}; pending primary durability."),
            deadline,
            "virtual lobby UUID administration queued",
        )
        .await?;
        wait_chat_exact(
            actor_events,
            "Cluster admin change is durable on the primary.",
            deadline,
            "virtual lobby UUID administration persisted",
        )
        .await?;
        if observer.entity_id_by_uuid(actor_uuid).is_some() {
            bail!("virtual lobby administration materialized the operator avatar");
        }
    }
    assert_lobby_chat(actor, observer_events, deadline).await?;
    actor.chat("/exitlobby");
    wait_reentry_feedback(
        actor_events,
        "You left the lobby wait room.",
        deadline,
        "manual lobby exit",
    )
    .await?;
    wait_observer_presence(
        observer,
        observer_events,
        actor_uuid,
        true,
        deadline,
        "manual lobby replica restoration",
    )
    .await?;
    wait_for_position(
        actor,
        Vec3::new(0.5, 80.0, 2.5),
        deadline,
        "accepted admin position after manual lobby",
    )
    .await?;
    let restored = observer
        .tab_list()
        .context("reading tab list after manual lobby")?
        .get(&actor_uuid)
        .cloned()
        .context("manual lobby repromotion lost the tab profile")?;
    if restored.profile != profile.profile {
        bail!("manual lobby repromotion changed the global profile")
    }
    Ok(())
}

async fn wait_reentry_feedback(
    events: &mut tokio::sync::mpsc::UnboundedReceiver<Event>,
    expected: &str,
    deadline: &Deadline,
    stage: &str,
) -> Result<()> {
    within(deadline, stage, async {
        while let Some(event) = events.recv().await {
            match event {
                Event::Chat(chat) if chat.message().to_string() == expected => return Ok(()),
                Event::Login | Event::Spawn => {
                    bail!("{stage} produced a logical logout/login or world spawn")
                }
                Event::Packet(packet) => match packet.as_ref() {
                    ClientboundGamePacket::Login(_)
                    | ClientboundGamePacket::Respawn(_)
                    | ClientboundGamePacket::StartConfiguration(_) => {
                        bail!("{stage} produced a loading-terrain transition: {packet:?}")
                    }
                    _ => {}
                },
                Event::Disconnect(reason) => bail!(
                    "{stage} disconnected: {}",
                    reason.map_or_else(
                        || String::from("connection closed"),
                        |text| text.to_string()
                    )
                ),
                _ => {}
            }
        }
        bail!("event stream closed during {stage}")
    })
    .await
}

async fn wait_observer_presence(
    observer: &Client,
    events: &mut tokio::sync::mpsc::UnboundedReceiver<Event>,
    actor_uuid: Uuid,
    present: bool,
    deadline: &Deadline,
    stage: &str,
) -> Result<()> {
    within(deadline, stage, async {
        let mut ticks = observer.get_tick_broadcaster();
        loop {
            tokio::select! {
                tick = next_tick(&mut ticks) => {
                    tick.context("observer tick stream closed during manual lobby")?;
                    if observer.entity_id_by_uuid(actor_uuid).is_some() == present {
                        return Ok(())
                    }
                }
                event = events.recv() => {
                    match event.context("observer event stream closed during manual lobby")? {
                        Event::Chat(chat) => {
                            let message = chat.message().to_string();
                            if message.contains("left the game") || message.contains("joined the game") {
                                bail!("{stage} emitted a fake leave/join message: {message}")
                            }
                        }
                        Event::Disconnect(reason) => bail!(
                            "observer disconnected during {stage}: {}",
                            reason.map_or_else(
                                || String::from("connection closed"),
                                |text| text.to_string()
                            )
                        ),
                        _ => {}
                    }
                }
            }
        }
    })
    .await
}

async fn assert_status_profiles(
    node: &Node,
    expected: &[&str],
    deadline: &Deadline,
    stage: &str,
) -> Result<()> {
    assert_status_profiles_with_count(node, expected, expected.len(), deadline, stage).await
}

async fn assert_status_profiles_with_count(
    node: &Node,
    expected: &[&str],
    online: usize,
    deadline: &Deadline,
    stage: &str,
) -> Result<()> {
    let address = node
        .java
        .as_deref()
        .context("status target has no Java listener")?;
    let status = within(deadline, stage, async {
        let mut stream = tokio::net::TcpStream::connect(address)
            .await
            .with_context(|| format!("connecting status client to {address}"))?;
        let socket: SocketAddr = address.parse().context("parsing status address")?;
        let mut handshake = Vec::new();
        write_varint_vec(&mut handshake, 0);
        write_varint_vec(&mut handshake, 0);
        let host = socket.ip().to_string();
        write_varint_vec(&mut handshake, i32::try_from(host.len())?);
        handshake.extend_from_slice(host.as_bytes());
        handshake.extend_from_slice(&socket.port().to_be_bytes());
        write_varint_vec(&mut handshake, 1);
        let mut frame = Vec::new();
        write_varint_vec(&mut frame, i32::try_from(handshake.len())?);
        frame.extend_from_slice(&handshake);
        frame.extend_from_slice(&[1, 0]);
        stream
            .write_all(&frame)
            .await
            .context("writing status request")?;
        let length = usize::try_from(read_varint_async(&mut stream).await?)
            .context("negative status packet length")?;
        let mut response = vec![0; length];
        stream
            .read_exact(&mut response)
            .await
            .context("reading status response")?;
        let mut offset = 0;
        if read_varint_slice(&response, &mut offset)? != 0 {
            bail!("status response used an unexpected packet id");
        }
        let json_length = usize::try_from(read_varint_slice(&response, &mut offset)?)
            .context("negative status JSON length")?;
        let end = offset
            .checked_add(json_length)
            .context("status JSON length overflow")?;
        let json = response.get(offset..end).context("truncated status JSON")?;
        serde_json::from_slice::<serde_json::Value>(json).context("decoding status JSON")
    })
    .await?;
    let players = status
        .get("players")
        .and_then(serde_json::Value::as_object)
        .context("status has no players object")?;
    if players.get("online").and_then(serde_json::Value::as_u64) != Some(u64::try_from(online)?) {
        bail!("{stage} reported the wrong online count: {players:?}");
    }
    let sample = players
        .get("sample")
        .and_then(serde_json::Value::as_array)
        .context("status has no player sample")?;
    for name in expected {
        let uuid = offline_uuid(name).to_string();
        if !sample.iter().any(|entry| {
            entry.get("name").and_then(serde_json::Value::as_str) == Some(name)
                && entry.get("id").and_then(serde_json::Value::as_str) == Some(uuid.as_str())
        }) {
            bail!("{stage} sample is missing {name}: {sample:?}");
        }
    }
    for name in [BOT_A, BOT_B] {
        if expected.contains(&name) {
            continue;
        }
        if sample
            .iter()
            .any(|entry| entry.get("name").and_then(serde_json::Value::as_str) == Some(name))
        {
            bail!("{stage} sample exposes {name}: {sample:?}");
        }
    }
    if sample
        .iter()
        .any(|entry| entry.get("name").and_then(serde_json::Value::as_str) == Some(BOT_C))
    {
        bail!("{stage} sample exposes opted-out {BOT_C}: {sample:?}");
    }
    Ok(())
}

async fn prepare_arena(
    bot_a: &Client,
    bot_b: &Client,
    events: &mut tokio::sync::mpsc::UnboundedReceiver<Event>,
    deadline: &Deadline,
) -> Result<()> {
    while events.try_recv().is_ok() {}
    bot_a.chat("/clustertest arena");
    within(deadline, "accepted arena preparation", async {
        let mut received_health = false;
        let mut received_inventory = false;
        let mut received_teleport = false;
        while let Some(event) = events.recv().await {
            match event {
                Event::Chat(chat) if chat.message().to_string() == "cluster test arena accepted" => {
                    if !received_health || !received_inventory || !received_teleport {
                        bail!(
                            "arena accepted before client-observed reset state: health={received_health} inventory={received_inventory} teleport={received_teleport}"
                        )
                    }
                    return Ok(())
                }
                Event::Chat(chat) if chat.message().to_string() == "cluster test arena failed" => {
                    bail!("arena preparation reported a failed administrative intent")
                }
                Event::Packet(packet) => match packet.as_ref() {
                    ClientboundGamePacket::SetHealth(_) => received_health = true,
                    ClientboundGamePacket::ContainerSetContent(_) => received_inventory = true,
                    ClientboundGamePacket::PlayerPosition(_) => received_teleport = true,
                    _ => {}
                },
                Event::Disconnect(reason) => bail!(
                    "Azalea disconnected during arena preparation: {}",
                    reason.map_or_else(
                        || String::from("connection closed"),
                        |text| text.to_string()
                    )
                ),
                _ => {}
            }
        }
        bail!("Azalea event stream closed before arena acceptance")
    })
    .await?;

    let expected_a = Vec3::new(-0.5, 80.0, 0.5);
    let expected_b = Vec3::new(1.5, 80.0, 0.5);
    wait_until(bot_a, deadline, "arena position A", || {
        Ok((position_matches(bot_a.position()?, expected_a)).then_some(()))
    })
    .await?;
    wait_until(bot_b, deadline, "arena position B", || {
        Ok((position_matches(bot_b.position()?, expected_b)).then_some(()))
    })
    .await?;
    for (client, name) in [(bot_a, BOT_A), (bot_b, BOT_B)] {
        wait_until(
            client,
            deadline,
            &format!("arena deck and pits for {name}"),
            || {
                let world = client.world()?;
                let world = world.read();
                let expected = [
                    (BlockPos::new(-1, 80, 0), BlockKind::Air.into()),
                    (BlockPos::new(-1, 79, 0), BlockKind::Stone.into()),
                    (BlockPos::new(0, 80, 0), BlockKind::Stone.into()),
                    (BlockPos::new(1, 80, 0), BlockKind::Air.into()),
                    (BlockPos::new(1, 79, 0), BlockKind::Stone.into()),
                ];
                Ok(expected
                    .iter()
                    .all(|(pos, state)| world.get_block_state(*pos) == Some(*state))
                    .then_some(()))
            },
        )
        .await?;
    }
    let distance_squared = (expected_a.x - expected_b.x).powi(2)
        + (expected_a.y - expected_b.y).powi(2)
        + (expected_a.z - expected_b.z).powi(2);
    if distance_squared > 9.0 {
        bail!("arena positions exceed normal interaction reach");
    }
    Ok(())
}

async fn verify_remote_spectate(
    client: &Client,
    events: &mut tokio::sync::mpsc::UnboundedReceiver<Event>,
    target: &str,
    deadline: &Deadline,
) -> Result<()> {
    let target_id = client
        .entity_by_uuid(offline_uuid(target))
        .context("spectate target replica is absent")?
        .minecraft_id()
        .context("reading spectate target network entity id")?;
    while events.try_recv().is_ok() {}
    assert_command_suggestions(
        client,
        events,
        "/spectate ",
        &[target],
        &[],
        0x5b05,
        deadline,
        "global /spectate suggestions",
    )
    .await?;
    client.chat(&format!("/spectate {target}"));
    within(deadline, "remote /spectate camera delivery", async {
        while let Some(event) = events.recv().await {
            match event {
                Event::Packet(packet) => {
                    if let ClientboundGamePacket::SetCamera(camera) = packet.as_ref()
                        && camera.camera_id == target_id
                    {
                        return Ok(());
                    }
                }
                Event::Disconnect(reason) => {
                    bail!("remote /spectate disconnected client: {reason:?}")
                }
                _ => {}
            }
        }
        bail!("remote /spectate did not change the client camera")
    })
    .await
}

async fn verify_invsee_window(
    client: &Client,
    events: &mut tokio::sync::mpsc::UnboundedReceiver<Event>,
    target_events: &mut tokio::sync::mpsc::UnboundedReceiver<Event>,
    target: &str,
    deadline: &Deadline,
) -> Result<()> {
    while events.try_recv().is_ok() {}
    assert_command_suggestions(
        client,
        events,
        "/invsee edit ",
        &[target],
        &[],
        0x5b06,
        deadline,
        "remote /invsee suggestions",
    )
    .await?;
    client.chat(&format!("/invsee edit {target}"));
    let container_id = within(deadline, "remote editable invsee presentation", async {
        let mut opened = None;
        while let Some(event) = events.recv().await {
            match event {
                Event::Packet(packet) => match packet.as_ref() {
                    ClientboundGamePacket::OpenScreen(packet) => {
                        if packet.menu_type != MenuKind::Generic9x5 {
                            bail!("/invsee opened the wrong Java menu: {:?}", packet.menu_type)
                        }
                        opened = Some(packet.container_id);
                    }
                    ClientboundGamePacket::ContainerSetContent(packet)
                        if opened == Some(packet.container_id) =>
                    {
                        if packet.items.len() != 81 {
                            bail!("/invsee sent {} slots instead of 81", packet.items.len())
                        }
                        for slot in 4..8 {
                            if packet.items[slot].kind() != ItemKind::GrayStainedGlassPane {
                                bail!("/invsee filler slot {slot} is not dark gray glass")
                            }
                            if packet.items[slot]
                                .get_component::<CustomName>()
                                .is_none_or(|name| !name.name.to_string().is_empty())
                            {
                                bail!("/invsee filler slot {slot} exposes a visible item name")
                            }
                        }
                        for (slot, expected) in [
                            (0, ItemKind::IronHelmet),
                            (1, ItemKind::IronChestplate),
                            (2, ItemKind::IronLeggings),
                            (3, ItemKind::IronBoots),
                            (8, ItemKind::Shield),
                            (36, ItemKind::IronSword),
                            (37, ItemKind::Bow),
                            (38, ItemKind::Arrow),
                        ] {
                            if packet.items[slot].kind() != expected {
                                bail!(
                                    "/invsee slot {slot} has {:?}, expected {expected:?}",
                                    packet.items[slot].kind()
                                )
                            }
                        }
                        if packet.items[38].count() != 64 {
                            bail!("/invsee did not show the target's complete arrow stack")
                        }
                        return Ok(packet.container_id);
                    }
                    _ => {}
                },
                Event::Disconnect(reason) => {
                    bail!("client disconnected opening /invsee: {reason:?}")
                }
                _ => {}
            }
        }
        bail!("/invsee window did not receive its complete content")
    })
    .await?;
    let open = client
        .get_inventory()
        .context("reading editable /invsee window")?;
    if open.id() != container_id {
        bail!("Azalea did not retain the editable /invsee window")
    }
    open.left_click(37usize);
    wait_inventory_slot_kind(
        target_events,
        0,
        37,
        ItemKind::Air,
        deadline,
        "remote /invsee bow removal",
    )
    .await?;
    open.left_click(37usize);
    wait_inventory_slot_kind(
        target_events,
        0,
        37,
        ItemKind::Bow,
        deadline,
        "remote /invsee bow restoration",
    )
    .await?;
    client.write_packet(ServerboundContainerClose { container_id });
    Ok(())
}

async fn wait_inventory_slot_kind(
    events: &mut tokio::sync::mpsc::UnboundedReceiver<Event>,
    container_id: i32,
    slot: u16,
    expected: ItemKind,
    deadline: &Deadline,
    stage: &str,
) -> Result<()> {
    within(deadline, stage, async {
        while let Some(event) = events.recv().await {
            match event {
                Event::Packet(packet) => match packet.as_ref() {
                    ClientboundGamePacket::ContainerSetSlot(packet)
                        if packet.container_id == container_id && packet.slot == slot =>
                    {
                        if packet.item_stack.kind() == expected {
                            return Ok(());
                        }
                    }
                    ClientboundGamePacket::ContainerSetContent(packet)
                        if packet.container_id == container_id =>
                    {
                        if packet
                            .items
                            .get(slot as usize)
                            .is_some_and(|item| item.kind() == expected)
                        {
                            return Ok(());
                        }
                    }
                    _ => {}
                },
                Event::Disconnect(reason) => bail!("{stage} disconnected the target: {reason:?}"),
                _ => {}
            }
        }
        bail!("{stage} closed before the authoritative inventory update")
    })
    .await
}

async fn verify_beacon_menu(
    client: &Client,
    events: &mut tokio::sync::mpsc::UnboundedReceiver<Event>,
    deadline: &Deadline,
) -> Result<()> {
    while events.try_recv().is_ok() {}
    client.chat("/clustertest beacon");
    within(deadline, "accepted beacon fixture seed", async {
        while let Some(event) = events.recv().await {
            match event {
                Event::Chat(chat)
                    if chat.message().to_string() == "cluster test beacon prepared" =>
                {
                    return Ok(());
                }
                Event::Chat(chat) if chat.message().to_string() == "cluster test beacon failed" => {
                    bail!("beacon world preparation failed")
                }
                Event::Disconnect(reason) => {
                    bail!("Azalea disconnected during beacon setup: {reason:?}")
                }
                _ => {}
            }
        }
        bail!("Azalea event stream closed before beacon preparation")
    })
    .await?;
    client.write_packet(ServerboundUseItemOn {
        hand: InteractionHand::MainHand,
        block_hit: BlockHit {
            block_pos: BlockPos::new(-2, 80, 0),
            direction: Direction::East,
            location: Vec3::new(-1.5, 80.5, 0.5),
            inside: false,
            world_border: false,
        },
        seq: 0xbeac0,
    });
    let container_id = within(deadline, "beacon menu and accepted seed", async {
        let mut opened = None;
        let mut payment = false;
        let mut levels = false;
        let mut accepted = false;
        while let Some(event) = events.recv().await {
            match event {
                Event::Chat(chat)
                    if chat.message().to_string() == "cluster test beacon accepted" =>
                {
                    accepted = true
                }
                Event::Chat(chat) if chat.message().to_string() == "cluster test beacon failed" => {
                    bail!("beacon seed was rejected")
                }
                Event::Packet(packet) => match packet.as_ref() {
                    ClientboundGamePacket::OpenScreen(packet)
                        if packet.menu_type == MenuKind::Beacon =>
                    {
                        opened = Some(packet.container_id);
                    }
                    ClientboundGamePacket::ContainerSetContent(packet)
                        if opened == Some(packet.container_id)
                            && packet.items.first().is_some_and(|item| item.count() == 1) =>
                    {
                        payment = true;
                    }
                    ClientboundGamePacket::ContainerSetData(packet)
                        if opened == Some(packet.container_id)
                            && packet.id == 0
                            && packet.value >= 1 =>
                    {
                        levels = true;
                    }
                    _ => {}
                },
                Event::Disconnect(reason) => {
                    bail!("Azalea disconnected during beacon menu open: {reason:?}")
                }
                _ => {}
            }
            if let Some(container_id) = opened
                && payment
                && levels
                && accepted
            {
                return Ok(container_id);
            }
        }
        bail!("Azalea event stream closed before the accepted beacon menu")
    })
    .await?;
    client.write_packet(ServerboundSetBeacon {
        primary: Some(1),
        secondary: None,
    });
    client.write_packet(ServerboundSetBeacon {
        primary: Some(3),
        secondary: None,
    });
    within(deadline, "beacon conflict conservation", async {
        let mut configured = false;
        let mut conserved = false;
        let mut empty_payment = false;
        let mut effect = false;
        while let Some(event) = events.recv().await {
            match event {
                Event::Chat(chat)
                    if chat.message().to_string() == "cluster test beacon configured" =>
                {
                    configured = true
                }
                Event::Chat(chat)
                    if chat.message().to_string() == "cluster test beacon conserved" =>
                {
                    conserved = true
                }
                Event::Packet(packet) => match packet.as_ref() {
                    ClientboundGamePacket::ContainerSetSlot(packet)
                        if packet.container_id == container_id
                            && packet.slot == 0
                            && packet.item_stack.is_empty() =>
                    {
                        empty_payment = true;
                    }
                    ClientboundGamePacket::ContainerSetContent(packet)
                        if packet.container_id == container_id
                            && packet.items.first().is_some_and(|item| item.is_empty()) =>
                    {
                        empty_payment = true;
                    }
                    ClientboundGamePacket::ContainerSetData(packet)
                        if packet.container_id == container_id
                            && packet.id == 1
                            && matches!(packet.value, 1 | 3) =>
                    {
                        effect = true;
                    }
                    _ => {}
                },
                Event::Disconnect(reason) => {
                    bail!("Azalea disconnected during beacon conflict: {reason:?}")
                }
                _ => {}
            }
            if configured && conserved && empty_payment && effect {
                return Ok(());
            }
        }
        bail!("Azalea event stream closed before beacon conflict settled")
    })
    .await?;
    client.write_packet(ServerboundContainerClose { container_id });
    Ok(())
}

fn position_matches(actual: Vec3, expected: Vec3) -> bool {
    (actual.x - expected.x).abs() <= 0.01
        && (actual.y - expected.y).abs() <= 0.01
        && (actual.z - expected.z).abs() <= 0.01
}

async fn connect(
    name: &str,
    address: &str,
    deadline: &Deadline,
) -> Result<(Client, tokio::sync::mpsc::UnboundedReceiver<Event>)> {
    let started = time::Instant::now();
    loop {
        let remaining = deadline
            .remaining(&format!("connecting {name}"))?
            .min(E2E_STAGE_DEADLINE.saturating_sub(started.elapsed()));
        if remaining.is_zero() {
            bail!("Azalea connection stage deadline expired for {name}")
        }
        let last_error = match time::timeout(
            remaining.min(CONNECT_ATTEMPT_TIMEOUT),
            Client::join(Account::offline(name), address),
        )
        .await
        {
            Ok(Ok((client, events))) => {
                client.set_client_information(ClientInformation {
                    view_distance: client_view_distance(),
                    ..ClientInformation::default()
                })?;
                return Ok((client, events));
            }
            Ok(Err(error)) => error.to_string(),
            Err(_) => String::from("connection attempt timed out"),
        };
        let remaining = deadline
            .remaining(&format!("connecting {name}"))?
            .min(E2E_STAGE_DEADLINE.saturating_sub(started.elapsed()));
        if remaining <= CONNECT_RETRY_DELAY {
            bail!("Azalea could not connect {name}: {last_error}");
        }
        time::sleep(CONNECT_RETRY_DELAY).await;
    }
}

async fn unused_tcp_address() -> Result<String> {
    let listener = TcpListener::bind("127.0.0.1:0")
        .await
        .context("reserving an E2E TCP address")?;
    Ok(listener
        .local_addr()
        .context("reading an E2E TCP address")?
        .to_string())
}

async fn start_forward_proxy(
    backend: String,
    name: &'static str,
    delay: Duration,
) -> Result<(String, JoinHandle<()>)> {
    let listener = TcpListener::bind("127.0.0.1:0")
        .await
        .context("binding Bungee forwarding proxy")?;
    let address = listener
        .local_addr()
        .context("reading Bungee forwarding proxy address")?
        .to_string();
    let task = tokio::task::spawn_local(async move {
        loop {
            let Ok((client, _)) = listener.accept().await else {
                return;
            };
            let backend = backend.clone();
            tokio::task::spawn_local(async move {
                let _ = forward_connection(client, &backend, name, delay).await;
            });
        }
    });
    Ok((address, task))
}

async fn forward_connection(
    mut client: tokio::net::TcpStream,
    backend: &str,
    name: &str,
    delay: Duration,
) -> Result<()> {
    let mut server = tokio::net::TcpStream::connect(backend)
        .await
        .with_context(|| format!("connecting forwarding proxy to {backend}"))?;
    let length = read_varint_async(&mut client).await?;
    if length < 0 || length > 4096 {
        bail!("invalid Minecraft handshake length {length}");
    }
    let mut handshake = vec![0; usize::try_from(length).context("negative handshake length")?];
    client
        .read_exact(&mut handshake)
        .await
        .context("reading Minecraft handshake")?;
    let rewritten = rewrite_handshake(&handshake, name)?;
    if !delay.is_zero() {
        time::sleep(delay).await;
    }
    write_varint_async(&mut server, i32::try_from(rewritten.len())?).await?;
    server
        .write_all(&rewritten)
        .await
        .context("writing forwarded Minecraft handshake")?;
    let (client_read, client_write) = client.into_split();
    let (server_read, server_write) = server.into_split();
    tokio::try_join!(
        relay_with_delay(client_read, server_write, delay),
        relay_with_delay(server_read, client_write, delay),
    )?;
    Ok(())
}

async fn relay_with_delay<R, W>(mut reader: R, mut writer: W, delay: Duration) -> Result<()>
where
    R: tokio::io::AsyncRead + Unpin,
    W: tokio::io::AsyncWrite + Unpin,
{
    let mut buffer = [0; 16 * 1024];
    loop {
        let read = reader
            .read(&mut buffer)
            .await
            .context("reading delayed relay")?;
        if read == 0 {
            writer.shutdown().await.context("closing delayed relay")?;
            return Ok(());
        }
        if !delay.is_zero() {
            time::sleep(delay).await;
        }
        writer
            .write_all(&buffer[..read])
            .await
            .context("writing delayed relay")?;
    }
}

fn rewrite_handshake(packet: &[u8], name: &str) -> Result<Vec<u8>> {
    let mut offset = 0;
    let packet_id = read_varint_slice(packet, &mut offset)?;
    if packet_id != 0 {
        bail!("expected handshake packet id 0, received {packet_id}");
    }
    let protocol = read_varint_slice(packet, &mut offset)?;
    let address_len = usize::try_from(read_varint_slice(packet, &mut offset)?)
        .context("negative handshake address length")?;
    let end = offset
        .checked_add(address_len)
        .filter(|end| *end <= packet.len())
        .context("truncated handshake address")?;
    let address = std::str::from_utf8(&packet[offset..end]).context("invalid handshake address")?;
    offset = end;
    if packet.len().saturating_sub(offset) < 2 {
        bail!("truncated handshake port");
    }
    let port = &packet[offset..offset + 2];
    offset += 2;
    let next_state = read_varint_slice(packet, &mut offset)?;
    if next_state == 2 && (name == BOT_A || name == BOT_B) {
        let _ = LOBBY_LOGIN_HANDSHAKE.set(time::Instant::now());
    }
    if offset != packet.len() {
        bail!("unexpected trailing handshake data");
    }

    let uuid = offline_uuid(name).simple().to_string();
    let texture = format!("pumpkin-e2e-texture-{name}");
    let properties = serde_json::to_string(&[serde_json::json!({
        "name": "textures",
        "value": texture,
        "signature": format!("pumpkin-e2e-signature-{name}"),
    })])?;
    let forwarded = format!("{address}\0127.0.0.1\0{uuid}\0{properties}");
    let mut output = Vec::with_capacity(packet.len() + forwarded.len());
    write_varint_vec(&mut output, packet_id);
    write_varint_vec(&mut output, protocol);
    write_varint_vec(&mut output, i32::try_from(forwarded.len())?);
    output.extend_from_slice(forwarded.as_bytes());
    output.extend_from_slice(port);
    write_varint_vec(&mut output, next_state);
    Ok(output)
}

async fn read_varint_async(stream: &mut tokio::net::TcpStream) -> Result<i32> {
    let mut value = 0_u32;
    for shift in (0..35).step_by(7) {
        let byte = stream.read_u8().await.context("reading VarInt")?;
        value |= u32::from(byte & 0x7f) << shift;
        if byte & 0x80 == 0 {
            return Ok(value as i32);
        }
    }
    bail!("VarInt exceeds five bytes")
}

async fn write_varint_async(stream: &mut tokio::net::TcpStream, value: i32) -> Result<()> {
    let mut encoded = Vec::with_capacity(5);
    write_varint_vec(&mut encoded, value);
    stream.write_all(&encoded).await.context("writing VarInt")
}

fn read_varint_slice(bytes: &[u8], offset: &mut usize) -> Result<i32> {
    let mut value = 0_u32;
    for shift in (0..35).step_by(7) {
        let byte = *bytes.get(*offset).context("truncated VarInt")?;
        *offset += 1;
        value |= u32::from(byte & 0x7f) << shift;
        if byte & 0x80 == 0 {
            return Ok(value as i32);
        }
    }
    bail!("VarInt exceeds five bytes")
}

fn write_varint_vec(output: &mut Vec<u8>, value: i32) {
    let mut value = value as u32;
    loop {
        if value & !0x7f == 0 {
            output.push(value as u8);
            return;
        }
        output.push((value as u8 & 0x7f) | 0x80);
        value >>= 7;
    }
}

async fn unused_udp_address() -> Result<String> {
    let socket = UdpSocket::bind("127.0.0.1:0")
        .await
        .context("reserving an E2E UDP address")?;
    Ok(socket
        .local_addr()
        .context("reading an E2E UDP address")?
        .to_string())
}

async fn wait_login(
    events: &mut tokio::sync::mpsc::UnboundedReceiver<Event>,
    deadline: &Deadline,
    stage: &str,
    started: Option<time::Instant>,
) -> Result<LobbyPacketEvidence> {
    let mut disconnects = 0_u32;
    let mut last_disconnect = String::new();
    let result = within(deadline, stage, async {
        let mut evidence = LobbyPacketEvidence::default();
        while let Some(event) = events.recv().await {
            match event {
                Event::Login => {
                    if let Some(started) = started {
                        eprintln!("{stage} logical Login at {:?}", started.elapsed());
                    }
                    if disconnects != 0 {
                        eprintln!("{stage} recovered after {disconnects} pre-login disconnects");
                    }
                    return Ok(evidence);
                }
                Event::Disconnect(reason) => {
                    disconnects += 1;
                    last_disconnect = reason.map_or_else(
                        || String::from("connection closed"),
                        |text| text.to_string(),
                    );
                }
                Event::Packet(packet) => {
                    if let Some(started) = started
                        && matches!(packet.as_ref(), ClientboundGamePacket::Login(_))
                    {
                        eprintln!("{stage} CLogin at {:?}", started.elapsed());
                    }
                    let before = evidence.packet_trace.len();
                    evidence.observe(packet.as_ref())?;
                    if evidence.packet_trace.len() > before && evidence.packet_trace.len() <= 10 {
                        eprintln!(
                            "{stage} lobby packet trace: {}",
                            evidence.packet_trace.join(" -> ")
                        );
                    }
                }
                _ => {}
            }
        }
        bail!("Azalea event stream closed before login")
    })
    .await;
    result.with_context(|| {
        format!(
            "Azalea login did not complete on {stage}; pre-login disconnects={disconnects}, last={last_disconnect}"
        )
    })
}

async fn wait_virtual_lobby_spawn(
    events: &mut tokio::sync::mpsc::UnboundedReceiver<Event>,
    mut evidence: LobbyPacketEvidence,
    deadline: &Deadline,
    stage: &str,
    started: Option<time::Instant>,
) -> Result<LobbyPacketEvidence> {
    within(deadline, stage, async {
        let mut first_chunk = false;
        let mut spawned = false;
        while let Some(event) = events.recv().await {
            match event {
                Event::Spawn => {
                    if let Some(started) = started {
                        eprintln!("{stage} Spawn at {:?}", started.elapsed());
                    }
                    spawned = true;
                }
                Event::Disconnect(reason) => {
                    bail!(
                        "Azalea disconnected in the virtual lobby: {}",
                        reason.map_or_else(
                            || String::from("connection closed"),
                            |text| text.to_string()
                        )
                    );
                }
                Event::Packet(packet) => {
                    if let Some(started) = started {
                        match packet.as_ref() {
                            ClientboundGamePacket::LevelChunkWithLight(_) if !first_chunk => {
                                first_chunk = true;
                                eprintln!("{stage} first fake chunk at {:?}", started.elapsed());
                            }
                            ClientboundGamePacket::ChunkBatchFinished(_) => {
                                eprintln!("{stage} fake batch end at {:?}", started.elapsed());
                            }
                            _ => {}
                        }
                    }
                    let before = evidence.packet_trace.len();
                    evidence.observe(packet.as_ref())?;
                    if evidence.packet_trace.len() > before && evidence.packet_trace.len() <= 10 {
                        eprintln!("{stage} lobby packet trace: {}", evidence.packet_trace.join(" -> "));
                    }
                }
                _ => {}
            }
            if spawned
                && !evidence.completed_batches.is_empty()
                && evidence.platform_updates.len() == 256
                && evidence.platform_block_entities.len() == 256
                && evidence.received_commands
            {
                if started.is_some() {
                    let login_received = evidence.login_received_at
                        .context("complete lobby scene arrived without decoded CLogin timestamp")?;
                    let scene_elapsed = login_received.elapsed();
                    let one_way_delay = env::var("PUMPKIN_E2E_ONE_WAY_DELAY_MS")
                        .ok()
                        .and_then(|value| value.parse::<u64>().ok())
                        .unwrap_or(0);
                    let allowed = Duration::from_millis(one_way_delay.saturating_mul(2).saturating_add(250));
                    if scene_elapsed > allowed {
                        bail!("{stage} CLogin-to-complete-scene exceeded {allowed:?}: {scene_elapsed:?}")
                    }
                }
                return Ok(evidence);
            }
        }
        bail!("Azalea event stream closed before virtual lobby spawn")
    })
    .await
    .with_context(|| format!("Azalea virtual lobby did not establish a client world on {stage}"))
}

async fn wait_for_player_transition(
    client: &Client,
    events: &mut tokio::sync::mpsc::UnboundedReceiver<Event>,
    name: &str,
    deadline: &Deadline,
    stage: &str,
) -> Result<Uuid> {
    wait_for_local_promotion(client, events, name, false, deadline, stage).await?;
    wait_for_player(client, name, deadline, stage).await
}

async fn wait_for_local_promotion(
    client: &Client,
    events: &mut tokio::sync::mpsc::UnboundedReceiver<Event>,
    name: &str,
    require_durable_admin: bool,
    deadline: &Deadline,
    stage: &str,
) -> Result<()> {
    let name = String::from(name);
    within(deadline, stage, async {
        let mut ticks = client.get_tick_broadcaster();
        let mut real_batch_centered = false;
        let mut last_packet_was_center = false;
        let mut real_batches_started = 0_u32;
        let mut real_batches_finished = 0_u32;
        let mut real_chunk_packets = 0_u32;
        let mut promotion_teleport = false;
        let mut received_inventory = false;
        let mut received_health = false;
        let mut received_abilities = false;
        let mut received_game_mode = false;
        let mut received_camera = false;
        let mut removed_marker = false;
        let mut lobby_chat_responsive = false;
        let mut lobby_suggestions_responsive = false;
        let mut durable_admin = !require_durable_admin;
        let mut promotion_packets = Vec::new();
        let started = time::Instant::now();
        let mut reported = false;
        let change_pinned_view = name == BOT_A
            && env::var("PUMPKIN_E2E_SPEC_SCENARIOS")
                .unwrap_or_default()
                .split(',')
                .any(|scenario| scenario == "pinned-radius-view-change");
        let mut changed_pinned_view = false;
        loop {
            tokio::select! {
                tick = next_tick(&mut ticks) => {
                    tick.context("Azalea tick stream closed during lobby handoff")?;
                    if !reported && started.elapsed() >= Duration::from_secs(10) {
                        reported = true;
                        let tab_uuid = client.player_uuid_by_username(&name)?;
                        eprintln!(
                            "{stage} handoff observation after 10s: centered={real_batch_centered} batches_started={real_batches_started} batches_finished={real_batches_finished} real_chunks={real_chunk_packets} teleport={promotion_teleport} inventory={received_inventory} real_position={} list={lobby_chat_responsive} suggestions={lobby_suggestions_responsive} tab_uuid={tab_uuid:?} remote_entity={:?}",
                            client.position().is_ok_and(|position| position.x.abs() < 100_000.0 && position.z.abs() < 100_000.0),
                            tab_uuid.and_then(|uuid| client.entity_id_by_uuid(uuid))
                        );
                    }
                    if real_batch_centered
                        && real_batches_started >= 2
                        && real_batches_finished >= 2
                        && real_chunk_packets != 0
                        && promotion_teleport
                        && received_inventory
                        && received_health
                        && received_abilities
                        && received_game_mode
                        && received_camera
                        && removed_marker
                        && lobby_chat_responsive
                        && lobby_suggestions_responsive
                        && durable_admin
                        && (!change_pinned_view || changed_pinned_view)
                        && client.position().is_ok_and(|position| position.x.abs() < 100_000.0 && position.z.abs() < 100_000.0)
                    {
                        let ordered = ["PlayerPosition", "ContainerSetContent", "SetHealth", "PlayerAbilities", "ChangeGameMode", "SetCamera", "RemoveEntities"];
                        let indices = ordered.map(|name| promotion_packets.iter().position(|packet| packet == name));
                        if !indices.windows(2).all(|pair| pair[0] < pair[1]) {
                            bail!("lobby handoff packet order diverged from recovered teleport-first sequence: {}", promotion_packets.join(" -> "))
                        }
                        if env::var_os("PUMPKIN_E2E_FULL_RADIUS").is_some() {
                            let max_ms = env::var("PUMPKIN_E2E_RECOVERED_HANDOFF_MAX_MS")
                                .context("full-radius performance gate requires PUMPKIN_E2E_RECOVERED_HANDOFF_MAX_MS measured from the recovered build")?
                                .parse::<u64>()
                                .context("invalid recovered full-radius handoff limit")?;
                            if started.elapsed() > Duration::from_millis(max_ms) {
                                bail!("{stage} full-radius handoff regressed past recovered limit {max_ms} ms: {:?}", started.elapsed());
                            }
                        }
                        eprintln!("{stage} promotion packets: {}", promotion_packets.join(" -> "));
                        return Ok(());
                    }
                }
                event = events.recv() => {
                    match event.context("Azalea event stream closed during lobby handoff")? {
                        Event::Login | Event::Spawn => {
                            bail!("lobby handoff triggered a second login, respawn, or world spawn")
                        }
                        Event::Disconnect(reason) => {
                            bail!(
                                "Azalea disconnected during lobby handoff: {}",
                                reason.map_or_else(
                                    || String::from("connection closed"),
                                    |text| text.to_string()
                                )
                            )
                        }
                        Event::Chat(chat) => {
                            let message = chat.message().to_string();
                            if message == "Cluster admin change is durable on the primary." {
                                durable_admin = true;
                            }
                            if real_batch_centered && message.contains(BOT_A) && message.contains(BOT_B) {
                                lobby_chat_responsive = true;
                            }
                        }
                        Event::Packet(packet) => {
                            match packet.as_ref() {
                                ClientboundGamePacket::Login(_)
                                | ClientboundGamePacket::Respawn(_)
                                | ClientboundGamePacket::StartConfiguration(_) => {
                                    bail!("lobby handoff sent a terrain-loading transition packet: {packet:?}")
                                }
                                ClientboundGamePacket::SetChunkCacheCenter(_) => {
                                    promotion_packets.push(String::from("SetChunkCacheCenter"));
                                    last_packet_was_center = true;
                                    if change_pinned_view && !changed_pinned_view {
                                        client.set_client_information(ClientInformation {
                                            view_distance: 2,
                                            allows_listing: true,
                                            ..ClientInformation::default()
                                        })?;
                                        changed_pinned_view = true;
                                    }
                                    client.chat("/list");
                                    client.write_packet(ServerboundCommandSuggestion {
                                        id: 0x5a20,
                                        command: String::from("/msg "),
                                    });
                                    continue;
                                }
                                ClientboundGamePacket::ChunkBatchStart(_) => {
                                    if !real_batch_centered && !last_packet_was_center {
                                        bail!("first real chunk batch was not immediately preceded by its chunk center")
                                    }
                                    real_batch_centered = true;
                                    real_batches_started += 1;
                                    promotion_packets.push(String::from("ChunkBatchStart"));
                                }
                                ClientboundGamePacket::ChunkBatchFinished(_) if real_batch_centered => {
                                    real_batches_finished += 1;
                                    promotion_packets.push(String::from("ChunkBatchFinished"));
                                }
                                ClientboundGamePacket::LevelChunkWithLight(_) if real_batch_centered => {
                                    real_chunk_packets += 1;
                                }
                                ClientboundGamePacket::CommandSuggestions(response)
                                    if response.id == 0x5a20 =>
                                {
                                    let values = response
                                        .suggestions
                                        .list()
                                        .iter()
                                        .map(|suggestion| suggestion.text())
                                        .collect::<HashSet<_>>();
                                    lobby_suggestions_responsive = values.contains(BOT_A)
                                        && values.contains(BOT_B);
                                }
                                ClientboundGamePacket::PlayerPosition(_) if real_batch_centered => {
                                    if real_batches_started < 2 || real_batches_finished < 2 {
                                        bail!("promotion teleport preceded several pipelined real chunk batches")
                                    }
                                    promotion_teleport = true;
                                    promotion_packets.push(String::from("PlayerPosition"));
                                }
                                ClientboundGamePacket::ContainerSetContent(_) if real_batch_centered => {
                                    received_inventory = true;
                                    promotion_packets.push(String::from("ContainerSetContent"));
                                }
                                ClientboundGamePacket::SetHealth(_) if real_batch_centered => {
                                    received_health = true;
                                    promotion_packets.push(String::from("SetHealth"));
                                }
                                ClientboundGamePacket::PlayerAbilities(_) if real_batch_centered => {
                                    received_abilities = true;
                                    promotion_packets.push(String::from("PlayerAbilities"));
                                }
                                ClientboundGamePacket::SetCamera(_) if real_batch_centered => {
                                    received_camera = true;
                                    promotion_packets.push(String::from("SetCamera"));
                                }
                                ClientboundGamePacket::RemoveEntities(_) if real_batch_centered => {
                                    removed_marker = true;
                                    promotion_packets.push(String::from("RemoveEntities"));
                                }
                                ClientboundGamePacket::GameEvent(packet) if real_batch_centered && packet.event == EventType::ChangeGameMode => {
                                    received_game_mode = true;
                                    promotion_packets.push(String::from("ChangeGameMode"));
                                }
                                ClientboundGamePacket::SetDefaultSpawnPosition(_) if real_batch_centered => {
                                    promotion_packets.push(String::from("SetDefaultSpawnPosition"));
                                }
                                _ => {}
                            }
            last_packet_was_center = false;
                        }
                        _ => {}
                    }
                }
            }
        }
    })
    .await
    .with_context(|| format!("Azalea state condition did not complete during {stage}"))
}

async fn wait_for_player(
    client: &Client,
    name: &str,
    deadline: &Deadline,
    stage: &str,
) -> Result<Uuid> {
    let name = String::from(name);
    let started = time::Instant::now();
    let uuid = wait_until(client, deadline, stage, move || {
        let uuid = client
            .player_uuid_by_username(&name)
            .context("reading Azalea tab list")?;
        Ok(uuid.filter(|uuid| client.entity_id_by_uuid(*uuid).is_some()))
    })
    .await?;
    eprintln!("{stage} visible after {:?}", started.elapsed());
    Ok(uuid)
}

async fn wait_for_position_change(
    client: &Client,
    uuid: Uuid,
    before: Vec3,
    deadline: &Deadline,
    stage: &str,
) -> Result<()> {
    wait_until(client, deadline, stage, || {
        let Some(entity) = client.entity_by_uuid(uuid) else {
            return Ok(None);
        };
        let position = entity
            .position()
            .context("reading remote player position")?;
        Ok(position_changed(position, before).then_some(()))
    })
    .await
}

fn position_changed(left: Vec3, right: Vec3) -> bool {
    (left.x - right.x).abs() > 0.01
        || (left.y - right.y).abs() > 0.01
        || (left.z - right.z).abs() > 0.01
}

async fn wait_until<T, F>(
    client: &Client,
    deadline: &Deadline,
    stage: &str,
    mut condition: F,
) -> Result<T>
where
    F: FnMut() -> Result<Option<T>>,
{
    within(deadline, stage, async {
        let mut ticks = client.get_tick_broadcaster();
        loop {
            match ticks.recv().await {
                Ok(_) | Err(tokio::sync::broadcast::error::RecvError::Lagged(_)) => {
                    if let Some(value) = condition()? {
                        return Ok(value);
                    }
                }
                Err(tokio::sync::broadcast::error::RecvError::Closed) => {
                    bail!("Azalea tick stream closed")
                }
            }
        }
    })
    .await
    .with_context(|| format!("Azalea state condition did not complete during {stage}"))
}

async fn next_tick(ticks: &mut tokio::sync::broadcast::Receiver<()>) -> Result<()> {
    loop {
        match ticks.recv().await {
            Ok(()) => return Ok(()),
            Err(tokio::sync::broadcast::error::RecvError::Lagged(_)) => continue,
            Err(tokio::sync::broadcast::error::RecvError::Closed) => {
                bail!("Azalea tick stream closed")
            }
        }
    }
}

async fn wait_for_position(
    client: &Client,
    expected: Vec3,
    deadline: &Deadline,
    stage: &str,
) -> Result<()> {
    wait_until(client, deadline, stage, || {
        let position = client.position()?;
        Ok(((position.x - expected.x).abs() < 0.01
            && (position.y - expected.y).abs() < 0.01
            && (position.z - expected.z).abs() < 0.01)
            .then_some(()))
    })
    .await
}

async fn wait_ticks(client: &Client, count: usize, deadline: &Deadline, stage: &str) -> Result<()> {
    within(deadline, stage, async {
        let mut ticks = client.get_tick_broadcaster();
        for _ in 0..count {
            loop {
                match ticks.recv().await {
                    Ok(_) => break,
                    Err(tokio::sync::broadcast::error::RecvError::Lagged(_)) => continue,
                    Err(tokio::sync::broadcast::error::RecvError::Closed) => {
                        bail!("Azalea tick stream closed")
                    }
                }
            }
        }
        Ok(())
    })
    .await
}

fn player_position(client: &Client, uuid: Uuid) -> Result<Vec3> {
    client
        .entity_by_uuid(uuid)
        .context("remote player is absent from the client world")?
        .position()
        .context("reading remote player position")
}

async fn fight_until_death_and_respawn(
    attacker: &Client,
    target_uuid: Uuid,
    attacker_events: &mut tokio::sync::mpsc::UnboundedReceiver<Event>,
    target_events: &mut tokio::sync::mpsc::UnboundedReceiver<Event>,
    deadline: &Deadline,
) -> Result<()> {
    within(deadline, "cross-node combat and death", async {
        let mut death_message = false;
        for _ in 0..40 {
            let target = attacker
                .entity_by_uuid(target_uuid)
                .context("combat target left the attacking client world")?;
            attacker.look_at(
                target
                    .eye_position()
                    .context("reading combat target eye position")?,
            );
            let entity = attacker
                .entity_id_by_uuid(target_uuid)
                .context("combat target entity id is absent")?;
            if !attacker.has_attack_cooldown() {
                attacker.attack(entity);
            }
            wait_ticks(attacker, 2, deadline, "cross-node combat and death").await?;
            while let Ok(event) = attacker_events.try_recv() {
                if let Event::Chat(chat) = event {
                    let message = chat.message().to_string();
                    if message.contains(BOT_A) && message.contains(BOT_B) {
                        death_message = true;
                        break;
                    }
                }
            }
            if death_message {
                break;
            }
        }
        if !death_message {
            bail!("cross-node attacks did not produce the deterministic death message");
        }
        let mut saw_death = false;
        while let Some(event) = target_events.recv().await {
            match event {
                Event::Death(_) => saw_death = true,
                Event::Spawn if saw_death => return Ok(()),
                Event::Disconnect(reason) => bail!(
                    "target disconnected during deterministic death/respawn: {}",
                    reason.map_or_else(
                        || String::from("connection closed"),
                        |text| text.to_string()
                    )
                ),
                _ => {}
            }
        }
        bail!("target event stream closed before deterministic death/respawn completed")
    })
    .await
}

async fn summon_secondary_owned_zombie(
    summoner: &Client,
    observer: &Client,
    deadline: &Deadline,
) -> Result<()> {
    within(
        deadline,
        "secondary-owned summoned entity replication",
        async {
            let existing = zombie_uuids(observer)?;
            summoner.chat("/summon minecraft:zombie ~ ~ ~");
            let zombie_uuid = wait_until(observer, deadline, "summoned entity replication", || {
                let zombies = observer
                    .nearest_entities::<With<Zombie>>()
                    .context("searching observer world for summoned zombie")?;
                for zombie in zombies {
                    let uuid = zombie.uuid().context("reading summoned zombie UUID")?;
                    if !existing.contains(&uuid) {
                        return Ok(Some(uuid));
                    }
                }
                Ok(None)
            })
            .await?;
            let before = observer
                .entity_by_uuid(zombie_uuid)
                .context("summoned zombie left observer world")?
                .position()
                .context("reading summoned zombie position")?;
            wait_for_position_change(
                observer,
                zombie_uuid,
                before,
                deadline,
                "summoned entity replication movement",
            )
            .await?;
            eprintln!("secondary-owned deterministic mob movement observed through Azalea");
            Ok(())
        },
    )
    .await
}

fn zombie_uuids(client: &Client) -> Result<HashSet<Uuid>> {
    client
        .nearest_entities::<With<Zombie>>()
        .context("reading observer zombie UUIDs")?
        .into_iter()
        .map(|entity| entity.uuid().context("reading observer zombie UUID"))
        .collect()
}

async fn prove_primary_natural_spawn_boundary(
    client: &Client,
    deadline: &Deadline,
) -> Result<HashSet<Uuid>> {
    client.chat("/time set midnight");
    wait_ticks(
        client,
        200,
        deadline,
        "primary natural-spawn observation boundary",
    )
    .await?;
    client
        .nearest_entities::<()>()
        .context("reading entities after primary natural-spawn window")?
        .into_iter()
        .map(|entity| entity.uuid().context("reading observed entity UUID"))
        .collect()
}

fn pumpkin_executable() -> Result<PathBuf> {
    if let Some(path) = env::var_os("PUMPKIN_E2E_PUMPKIN_BIN") {
        return Ok(PathBuf::from(path));
    }
    let root = Path::new(env!("CARGO_MANIFEST_DIR"))
        .ancestors()
        .nth(2)
        .context("locating repository root")?;
    Ok(root.join("result-dev/bin/pumpkin"))
}

async fn start_sntp() -> Result<(String, JoinHandle<()>)> {
    let socket = UdpSocket::bind("127.0.0.1:0")
        .await
        .context("binding local SNTP responder")?;
    let address = socket
        .local_addr()
        .context("reading local SNTP responder address")?;
    let task = tokio::spawn(async move {
        let mut request = [0_u8; 48];
        loop {
            let Ok((length, peer)) = socket.recv_from(&mut request).await else {
                return;
            };
            if length != request.len() {
                continue;
            }
            let mut reply = [0_u8; 48];
            let Some(received) = sntp_timestamp() else {
                continue;
            };
            reply[0] = 0x24;
            reply[1] = 1;
            reply[2] = 6;
            reply[3] = (-20_i8) as u8;
            reply[12..16].copy_from_slice(b"LOCL");
            reply[16..24].copy_from_slice(&received);
            reply[24..32].copy_from_slice(&request[40..48]);
            reply[32..40].copy_from_slice(&received);
            let Some(transmitted) = sntp_timestamp() else {
                continue;
            };
            reply[40..48].copy_from_slice(&transmitted);
            let _ = socket.send_to(&reply, peer).await;
        }
    });
    if let Err(error) = prove_sntp_exchange(address).await {
        task.abort();
        return Err(error);
    }
    Ok((address.to_string(), task))
}

async fn prove_sntp_exchange(server: SocketAddr) -> Result<()> {
    let bind = if server.is_ipv4() {
        "0.0.0.0:0"
    } else {
        "[::]:0"
    };
    let socket = UdpSocket::bind(bind)
        .await
        .context("binding production-shaped SNTP fixture client")?;
    socket
        .connect(server)
        .await
        .context("connecting production-shaped SNTP fixture client")?;
    let (request, transmitted) =
        sntp_request().context("creating production-shaped SNTP fixture request")?;
    socket
        .send(&request)
        .await
        .context("sending production-shaped SNTP fixture request")?;
    let mut reply = [0_u8; 512];
    let length = time::timeout(SNTP_EXCHANGE_TIMEOUT, socket.recv(&mut reply))
        .await
        .context("local SNTP responder did not answer a production-shaped exchange")?
        .context("receiving local SNTP responder reply")?;
    if length < 48 {
        bail!("local SNTP responder returned a short {length}-byte reply");
    }
    if reply[0] & 0x07 != 4 {
        bail!(
            "local SNTP responder returned non-server mode {}",
            reply[0] & 0x07
        );
    }
    if reply[1] == 0 {
        bail!("local SNTP responder returned a kiss-of-death reply");
    }
    if reply[24..32] != transmitted {
        bail!("local SNTP responder did not preserve the request origin timestamp");
    }
    if reply[32..40].iter().all(|byte| *byte == 0) || reply[40..48].iter().all(|byte| *byte == 0) {
        bail!("local SNTP responder returned an incomplete server timestamp pair");
    }
    Ok(())
}

fn sntp_timestamp() -> Option<[u8; 8]> {
    let elapsed = SystemTime::now().duration_since(UNIX_EPOCH).ok()?;
    let seconds = elapsed.as_secs().checked_add(2_208_988_800)?;
    let fraction = u64::from(elapsed.subsec_nanos()).checked_shl(32)? / 1_000_000_000;
    Some(((seconds << 32) | fraction).to_be_bytes())
}

fn sntp_request() -> Option<([u8; 48], [u8; 8])> {
    let elapsed = SystemTime::now().duration_since(UNIX_EPOCH).ok()?;
    let millis = i64::try_from(elapsed.as_millis()).ok()?;
    let seconds = millis.div_euclid(1_000).checked_add(2_208_988_800)?;
    let fraction = u64::try_from(millis.rem_euclid(1_000))
        .ok()?
        .checked_mul(4_294_967_296)?
        .checked_add(500)?
        / 1_000;
    let seconds = u32::try_from(seconds).ok()?;
    let fraction = u32::try_from(fraction).ok()?;
    let transmitted = ((u64::from(seconds) << 32) | u64::from(fraction)).to_be_bytes();
    let mut request = [0_u8; 48];
    request[0] = 0x1B;
    request[40..48].copy_from_slice(&transmitted);
    Some((request, transmitted))
}

fn write_keypair(node_root: &Path) -> Result<String> {
    let keypair = rcgen::generate_simple_self_signed(vec![String::from("pumpkin-mesh")])
        .context("generating mesh certificate")?;
    let certificate = keypair.cert.der().to_vec();
    let key = keypair.signing_key.serialize_der();
    fs::write(node_root.join("cluster-cert.der"), &certificate)
        .with_context(|| format!("writing certificate in {}", node_root.display()))?;
    fs::write(node_root.join("cluster-key.der"), key)
        .with_context(|| format!("writing key in {}", node_root.display()))?;
    Ok(hex::encode(Sha256::digest(certificate)))
}

fn write_config(node: &Node, nodes: &[Node], sntp: &str) -> Result<()> {
    let peers = nodes
        .iter()
        .filter(|peer| peer.id != node.id)
        .map(|peer| {
            format!(
                "{{ server_id = {}, addr = \"{}\", pubkey_sha256_hex = \"{}\" }}",
                peer.id, peer.quic, peer.pin
            )
        })
        .collect::<Vec<_>>()
        .join(", ");
    let java_enabled = node.java.is_some();
    let java_address = node.java.as_deref().unwrap_or("127.0.0.1:25565");
    let save_player_data = node.role == "primary";
    let bootstrap_vanilla_world = node.role == "primary";
    let config = format!(
        "seed = \"1787949120387005756\"\ndefault_level_name = \"world\"\n\n[world]\nautosave_ticks = 6000\n\n[world.chunk]\ntype = \"anvil\"\nwrite_in_place = false\n\n[networking.java]\nenabled = {java_enabled}\naddress = \"{java_address}\"\nonline_mode = false\nencryption = false\n\n[networking.proxy]\nenabled = {java_enabled}\n\n[networking.proxy.bungeecord]\nenabled = {java_enabled}\nsecret = \"\"\n\n[networking.bedrock]\nenabled = false\n\n[networking.bedrock.nethernet]\nenabled = false\naddress = \"127.0.0.1:19132\"\n\n[networking.query]\nenabled = false\naddress = \"127.0.0.1:25565\"\n\n[networking.rcon]\nenabled = false\naddress = \"127.0.0.1:25575\"\n\n[networking.lan_broadcast]\nenabled = false\n\n[player_data]\nsave_player_data = {save_player_data}\n\n[cluster]\nenabled = true\nrole = \"{}\"\nserver_id = {}\nbootstrap_vanilla_world = {bootstrap_vanilla_world}\nbind_addr = \"{}\"\ncert_path = \"cluster-cert.der\"\nkey_path = \"cluster-key.der\"\npeers = [{peers}]\nntp_servers = [\"{sntp}\"]\nmax_offset_millis = 25\n\n[telemetry]\nenabled = false\n",
        node.role, node.id, node.quic
    );
    fs::write(node.root.join("pumpkin.toml"), config)
        .with_context(|| format!("writing {} configuration", node.role))
}

fn write_ops(primary_root: &Path) -> Result<()> {
    let mut ops = [BOT_A, BOT_B]
        .into_iter()
        .map(|name| {
            serde_json::json!({
                "uuid": offline_uuid(name),
                "name": name,
                "level": 4,
                "bypasses_player_limit": true,
            })
        })
        .collect::<Vec<_>>();
    if env::var_os("PUMPKIN_E2E_LARGE_ADMIN").is_some() {
        for index in 0..12_000_u128 {
            ops.push(serde_json::json!({
                "uuid": Uuid::from_u128(0x9a10_0000_0000_0000_0000_0000_0000_0000 | index),
                "name": format!("Test{index:012x}"),
                "level": 1,
                "bypasses_player_limit": false,
            }));
        }
    }
    let bytes =
        serde_json::to_vec_pretty(&ops).context("serializing deterministic offline operators")?;
    if env::var_os("PUMPKIN_E2E_LARGE_ADMIN").is_some() && bytes.len() <= 1_048_576 {
        bail!("large administration fixture did not exceed the one-MiB snapshot boundary");
    }
    fs::write(primary_root.join("ops.json"), &bytes)
        .with_context(|| format!("writing {}", primary_root.join("ops.json").display()))?;
    let bootstrap = primary_root.join("data/ops.json");
    fs::create_dir_all(
        bootstrap
            .parent()
            .context("operator bootstrap parent missing")?,
    )
    .context("creating operator bootstrap directory")?;
    fs::write(&bootstrap, bytes).with_context(|| format!("writing {}", bootstrap.display()))
}

fn offline_uuid(name: &str) -> Uuid {
    Account::offline(name).uuid()
}

async fn verify_retiring_reconnect_lobby(
    client: &Client,
    events: &mut tokio::sync::mpsc::UnboundedReceiver<Event>,
    evidence: &mut LobbyPacketEvidence,
    callback_started: time::Instant,
    deadline: &Deadline,
) -> Result<()> {
    let earliest_completion = callback_started + Duration::from_secs(3);
    if time::Instant::now() >= earliest_completion {
        bail!("reconnect did not establish virtual lobby while original callback was pending");
    }
    if !evidence.progress_subtitles.is_empty() {
        bail!("reconnect pinned progress before original delayed callback completed");
    }
    client.chat("/list");
    within(deadline,"responsive reconnect while retirement is pending",async {
        let mut responsive=false;
        loop {
            tokio::select! {
                _=time::sleep_until(earliest_completion)=>{
                    if !responsive {bail!("pending retirement stalled normal virtual lobby /list");}
                    return Ok(());
                }
                event=events.recv()=>match event {
                    Some(Event::Packet(packet))=>{
                        match packet.as_ref() {
                            ClientboundGamePacket::LevelChunkWithLight(chunk) if !evidence.chunk_positions.contains(&(chunk.x,chunk.z))=>bail!("real chunk arrived before old callback could complete"),
                            ClientboundGamePacket::PlayerPosition(position) if (position.change.pos.x-1_000_008.5).abs()>0.01 || (position.change.pos.z-1_000_008.5).abs()>0.01=>bail!("real teleport arrived before old callback could complete"),
                            ClientboundGamePacket::ContainerSetContent(_)|ClientboundGamePacket::SetHealth(_)=>bail!("real player state arrived before old callback could complete"),
                            _=>{}
                        }
                        evidence.observe(packet.as_ref())?;
                        if !evidence.progress_subtitles.is_empty() {bail!("target progress was pinned before old callback could complete");}
                    }
                    Some(Event::Chat(chat))=>{responsive|=chat.message().to_string().contains(BOT_B);}
                    Some(Event::Disconnect(reason))=>bail!("reconnect disconnected during retirement barrier: {reason:?}"),
                    None=>bail!("reconnect event stream closed during retirement barrier"),
                    _=>{}
                }
            }
        }
    }).await
}

async fn wait_first_reconnect_progress(
    events: &mut tokio::sync::mpsc::UnboundedReceiver<Event>,
    evidence: &mut LobbyPacketEvidence,
    deadline: &Deadline,
) -> Result<()> {
    within(deadline,"first retained reconnect progress",async {
        while evidence.progress_subtitles.is_empty() {
            match events.recv().await {
                Some(Event::Packet(packet))=>{
                    if matches!(packet.as_ref(),ClientboundGamePacket::LevelChunkWithLight(chunk) if !evidence.chunk_positions.contains(&(chunk.x,chunk.z))) {
                        bail!("real chunks arrived before first retained progress");
                    }
                    evidence.observe(packet.as_ref())?;
                }
                Some(Event::Disconnect(reason))=>bail!("reconnect disconnected before retained progress: {reason:?}"),
                None=>bail!("reconnect stream closed before retained progress"),
                _=>{}
            }
        }
        Ok(())
    }).await
}
