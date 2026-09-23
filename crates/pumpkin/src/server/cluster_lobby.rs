use std::collections::{HashMap, HashSet, VecDeque};
use std::sync::Arc;
use std::sync::OnceLock;
use std::sync::atomic::{AtomicBool, Ordering};

use bytes::Bytes;
use pumpkin_data::Block;
use pumpkin_data::entity::EntityType;
use pumpkin_protocol::codec::var_int::VarInt;
use pumpkin_protocol::java::client::play::{
    CBlockEntityData, CBlockUpdate, CCenterChunk, CChunkBatchEnd, CChunkBatchStart, CGameEvent,
    CLogin, CPlayerInfoUpdate, CPlayerPosition, CPlayerSpawnPosition, CRemovePlayerInfo,
    CSetCamera, CSpawnEntity, CUpdateTime, GameEvent, PlayerAction, PlayerInfoFlags,
    PlayerSpawnData,
};
use pumpkin_protocol::java::server::play::{
    SChatAck, SChatCommand, SChatCommandSigned, SChatMessage, SChunkBatch, SClientInformationPlay,
    SCommandSuggestion, SCustomPayload, SPlayPong, SPlayResourcePack, SPlayerSession,
};
use pumpkin_protocol::packet::MultiVersionJavaPacket;
use pumpkin_protocol::{RawPacket, ServerPacket};
use pumpkin_util::GameMode;
use pumpkin_util::math::position::BlockPos;
use pumpkin_util::math::vector2::Vector2;
use pumpkin_util::math::vector3::Vector3;
use pumpkin_util::resource_location::ResourceLocation;
use pumpkin_util::text::{TextComponent, color::NamedColor};
use pumpkin_util::version::JavaMinecraftVersion;
use pumpkin_world::chunk::format::LightContainer;
use pumpkin_world::cylindrical_chunk_iterator::Cylindrical;
use pumpkin_world::level::is_cluster_secondary;
use rustc_hash::FxHashMap;
use tokio::sync::mpsc;
use uuid::Uuid;

use crate::block::entities::BlockEntity;
use crate::block::entities::end_portal::EndPortalBlockEntity;
use crate::entity::player::Player;
use crate::entity::{Entity, NBTStorage};
use crate::net::chunk_sender::{BatchAckResult, EncodedChunk};
use crate::net::java::JavaClient;
use crate::net::java::chunk_data::CChunkData;
use crate::net::{ChunkSender, ClientPlatform, GameProfile, PlayerConfig};
use crate::server::Server;
use crate::world::World;

/// Marker entity id used only as a fake spectator camera target.
///
/// Never inserted into any world entity list; only ever sent to the owning
/// client inside the lobby burst and later removed with `CRemoveEntities`.
const LOBBY_ANCHOR_ID: i32 = -1_000_001;

#[cfg(feature = "cluster-e2e")]
pub(crate) fn e2e_lobby_clock_micros() -> u128 {
    static START: std::sync::LazyLock<std::time::Instant> =
        std::sync::LazyLock::new(std::time::Instant::now);
    START.elapsed().as_micros()
}
pub(crate) const fn lobby_anchor_id() -> i32 {
    LOBBY_ANCHOR_ID
}
pub const LOBBY_TELEPORT_ID: i32 = 0;
pub const FORCED_LOBBY_PERMISSION: &str = "minecraft:command.forcelobby";
pub const EXIT_LOBBY_OTHERS_PERMISSION: &str = "minecraft:command.exitlobby.others";

pub struct LobbyDirectory {
    snapshot: Arc<arc_swap::ArcSwap<HashMap<String, LobbyDirectoryEntry>>>,
    tx: mpsc::Sender<LobbyDirectoryCommand>,
}

#[derive(Clone)]
struct LobbyDirectoryEntry {
    gid: pumpkin_cluster::identity::GlobalPlayerId,
    release: std::sync::Weak<AtomicBool>,
    client: std::sync::Weak<ClientPlatform>,
}

pub(crate) enum LobbyDirectoryCommand {
    Register {
        name: String,
        gid: pumpkin_cluster::identity::GlobalPlayerId,
        release: std::sync::Weak<AtomicBool>,
        client: std::sync::Weak<ClientPlatform>,
    },
    Unregister(pumpkin_cluster::identity::GlobalPlayerId),
}

impl LobbyDirectory {
    pub(crate) fn new(capacity: usize) -> (Self, mpsc::Receiver<LobbyDirectoryCommand>) {
        let (tx, rx) = mpsc::channel(capacity);
        (
            Self {
                snapshot: Arc::new(arc_swap::ArcSwap::from_pointee(HashMap::new())),
                tx,
            },
            rx,
        )
    }

    fn register(&self, source: &LobbyCommandSender) -> bool {
        self.register_entry(
            source.profile.name.to_ascii_lowercase(),
            source.gid,
            Arc::downgrade(&source.release),
            Arc::downgrade(&source.client),
        )
    }

    fn register_entry(
        &self,
        name: String,
        gid: pumpkin_cluster::identity::GlobalPlayerId,
        release: std::sync::Weak<AtomicBool>,
        client: std::sync::Weak<ClientPlatform>,
    ) -> bool {
        self.tx
            .try_send(LobbyDirectoryCommand::Register {
                name,
                gid,
                release,
                client,
            })
            .is_ok()
    }

    fn unregister(&self, gid: pumpkin_cluster::identity::GlobalPlayerId) -> bool {
        self.tx
            .try_send(LobbyDirectoryCommand::Unregister(gid))
            .is_ok()
    }

    pub fn release(&self, name: &str) -> bool {
        let name = name.to_ascii_lowercase();
        let snapshot = self.snapshot.load();
        let Some(entry) = snapshot.get(&name) else {
            return false;
        };
        let Some(release) = entry.release.upgrade() else {
            return false;
        };
        release.store(true, Ordering::Release);
        true
    }

    pub fn disconnect(&self, name: &str, message: &TextComponent) -> bool {
        let snapshot = self.snapshot.load();
        let Some(entry) = snapshot.get(&name.to_ascii_lowercase()) else {
            return false;
        };
        let Some(client) = entry.client.upgrade() else {
            return false;
        };
        client.try_kick(crate::net::DisconnectReason::Kicked, message);
        true
    }

    pub fn disconnect_gid(
        &self,
        gid: pumpkin_cluster::identity::GlobalPlayerId,
        message: &TextComponent,
    ) -> bool {
        let snapshot = self.snapshot.load();
        let Some(entry) = snapshot.values().find(|entry| entry.gid == gid) else {
            return false;
        };
        let Some(client) = entry.client.upgrade() else {
            return false;
        };
        client.try_kick(crate::net::DisconnectReason::Kicked, message);
        true
    }

    pub fn route_message(&self, name: &str, message: &TextComponent) -> bool {
        let snapshot = self.snapshot.load();
        let Some(entry) = snapshot.get(&name.to_ascii_lowercase()) else {
            return false;
        };
        let Some(client) = entry.client.upgrade() else {
            return false;
        };
        if let ClientPlatform::Java(client) = client.as_ref() {
            client.try_send_packet(
                &pumpkin_protocol::java::client::play::CSystemChatMessage::new(message, false),
            );
            return true;
        }
        false
    }
}

pub(crate) fn spawn_lobby_directory(
    server: &Arc<Server>,
    mut rx: mpsc::Receiver<LobbyDirectoryCommand>,
) {
    let runtime = server.cluster_lobby_directory.snapshot.clone();
    server.spawn_task(async move {
        let mut entries = HashMap::new();
        while let Some(command) = rx.recv().await {
            match command {
                LobbyDirectoryCommand::Register {
                    name,
                    gid,
                    release,
                    client,
                } => {
                    entries.insert(
                        name,
                        LobbyDirectoryEntry {
                            gid,
                            release,
                            client,
                        },
                    );
                }
                LobbyDirectoryCommand::Unregister(gid) => {
                    entries.retain(|_, entry| entry.gid != gid);
                }
            }
            entries.retain(|_, entry| {
                entry.release.strong_count() > 0 && entry.client.strong_count() > 0
            });
            runtime.store(Arc::new(entries.clone()));
        }
    });
}

pub struct LobbyCommandSender {
    pub gid: pumpkin_cluster::identity::GlobalPlayerId,
    pub client: Arc<ClientPlatform>,
    pub profile: GameProfile,
    pub locale: String,
    pub permission: pumpkin_util::PermissionLvl,
    pub forced: bool,
    pub release: Arc<AtomicBool>,
}

impl LobbyCommandSender {
    pub fn send_message(&self, text: &TextComponent) {
        if let ClientPlatform::Java(client) = self.client.as_ref() {
            client.try_send_packet(
                &pumpkin_protocol::java::client::play::CSystemChatMessage::new(text, false),
            );
        }
    }
}

pub struct LobbyPromotion {
    pub gid: pumpkin_cluster::identity::GlobalPlayerId,
    pub entity_id: i32,
    pub config: PlayerConfig,
    pub world: Arc<World>,
    pub data: Option<pumpkin_nbt::compound::NbtCompound>,
    pub hint: pumpkin_cluster::handoff::PlayerResumeHint,
    pub inventory_install_token: u64,
    pub(crate) chunk_need: super::cluster::ChunkNeedLease,
    pub chunk_sender: ChunkSender,
    pub chat_session: Option<SPlayerSession>,
    pub plugin_payloads: VecDeque<(String, Bytes)>,
    pub resource_pack_results: Vec<(Uuid, VarInt)>,
    pub dialog_actions: VecDeque<(String, Option<Bytes>)>,
    pub reentered: bool,
}

pub(crate) struct LocalLobbySnapshot {
    pub hint: pumpkin_cluster::handoff::PlayerResumeHint,
    pub data: pumpkin_nbt::compound::NbtCompound,
    pub commit_token: Option<u64>,
}

pub(crate) struct DeferredDeparture {
    pub retained: Option<super::cluster_presence::RetainedDeparture>,
    pub ready: tokio::sync::oneshot::Receiver<super::cluster_controller::DepartureStatus>,
}

pub(crate) fn begin_player_departure(
    server: &Arc<Server>,
    player: &Arc<Player>,
    logout: bool,
) -> Option<DeferredDeparture> {
    let gid = player.cluster_gid()?;
    if player
        .cluster_departing
        .compare_exchange(false, true, Ordering::AcqRel, Ordering::Acquire)
        .is_err()
    {
        tracing::error!(?gid, "duplicate cluster player departure");
        return None;
    }
    player.set_movement_locked(true);
    player.discard_cluster_packet_preflight();
    let expected_close_token = server
        .cluster_inventory_close_token
        .fetch_add(1, Ordering::Relaxed);
    match super::cluster_inventory_protocol::stage_departure_screen_close_any(
        player,
        expected_close_token,
    ) {
        Ok(()) => {}
        Err(error) => {
            player.world().suspend_player_visibility(player);
            let _ = super::cluster_presence::freeze_player_handoff(player);
            if logout {
                super::cluster_presence::publish_logout_gid(server, gid);
            }
            tracing::error!(
                ?gid,
                ?error,
                "cluster player screen close could not be staged"
            );
            return None;
        }
    }
    player.world().suspend_player_visibility(player);
    let retained = super::cluster_presence::freeze_player_handoff(player);
    if logout {
        super::cluster_presence::publish_logout_gid(server, gid);
    }
    match server
        .cluster_controller
        .request_player_departure(gid, expected_close_token, logout)
    {
        Ok(ready) => Some(DeferredDeparture { retained, ready }),
        Err(error) => {
            tracing::error!(?gid, ?error, "cluster player departure ticket failed");
            None
        }
    }
}

pub(crate) async fn await_player_departure(
    mut ready: tokio::sync::oneshot::Receiver<super::cluster_controller::DepartureStatus>,
    uuid: Uuid,
) -> bool {
    let outcome = match tokio::time::timeout(std::time::Duration::from_secs(60), &mut ready).await {
        Ok(outcome) => outcome,
        Err(_) => {
            tracing::error!(%uuid, "cluster player departure exceeded 60 seconds");
            ready.await
        }
    };
    match outcome {
        Ok(super::cluster_controller::DepartureStatus::Ready) => true,
        result => {
            tracing::error!(%uuid, ?result, "cluster player departure did not settle");
            false
        }
    }
}

pub(crate) fn local_snapshot_from_player(
    player: &Player,
    server: &Server,
) -> Option<LocalLobbySnapshot> {
    let gid = player.cluster_gid()?;
    let statistics = server.cluster_actor_statistics.load();
    let statistics = statistics.get(&gid)?;
    if statistics.leave_game_recorded {
        tracing::error!(
            ?gid,
            revision = statistics.revision,
            "cluster lobby snapshot has logout statistics"
        );
        return None;
    }
    let world = player.world();
    let position = player.position();
    let rotation = player.rotation();
    let revision = server
        .cluster_handoff
        .load()
        .hints
        .get(&player.gameprofile.id)
        .map_or(0, |hint| hint.playerdata_revision);
    let world_id = super::cluster_world_delta::world_id(&world);
    let hint = pumpkin_cluster::handoff::PlayerResumeHint {
        uuid: *player.gameprofile.id.as_bytes(),
        dimension: world.dimension.minecraft_name.to_string(),
        position: [position.x, position.y, position.z],
        rotation: [rotation.0, rotation.1],
        view_radius: player.config.load().view_distance.get(),
        playerdata_revision: revision,
        last_owner: Some(server.advanced_config.cluster.server_id),
        retained_chunks: player
            .watched_section
            .load()
            .all_chunks_within()
            .map(|chunk| pumpkin_cluster::protocol::ChunkKey::new(world_id, chunk.x, chunk.y))
            .collect(),
        retained_until_millis: 0,
    };
    let mut data = pumpkin_nbt::compound::NbtCompound::new();
    player.write_nbt(&mut data);
    statistics.write_nbt(&mut data);
    Some(LocalLobbySnapshot {
        hint,
        data,
        commit_token: None,
    })
}

pub struct LobbySession {
    client: Arc<ClientPlatform>,
    profile: GameProfile,
    config: PlayerConfig,
    lobby_world: Arc<World>,
    target_world: Option<Arc<World>>,
    entity_id: i32,
    gid: pumpkin_cluster::identity::GlobalPlayerId,
    source: Arc<LobbyCommandSender>,
    handoff: pumpkin_cluster::handoff::PinnedHandoff,
    sender: ChunkSender,
    encoded: FxHashMap<Vector2<i32>, EncodedChunk>,
    required: Vec<Vector2<i32>>,
    chunk_need: Option<super::cluster::ChunkNeedLease>,
    requested_hint: bool,
    requested_playerdata: bool,
    inventory_install_revision: Option<u64>,
    handoff_outbox: VecDeque<super::cluster_handoff::HandoffCommand>,
    playerdata_token: u64,
    image_sent: bool,
    real_center_sent: bool,
    tab_seen: HashMap<Uuid, (String, Vec<pumpkin_cluster::presence::PresenceProperty>)>,
    presence_revision: u64,
    invariant_failed: bool,
    held: bool,
    directory_registered: bool,
    chat_rx: tokio::sync::broadcast::Receiver<(Option<String>, TextComponent)>,
    chat_session: Option<SPlayerSession>,
    chat_session_checks: VecDeque<tokio::sync::oneshot::Receiver<Option<SPlayerSession>>>,
    plugin_channels: HashSet<String>,
    plugin_payloads: VecDeque<(String, Bytes)>,
    resource_pack_results: Vec<(Uuid, VarInt)>,
    dialog_actions: VecDeque<(String, Option<Bytes>)>,
    local_hint: Option<pumpkin_cluster::handoff::PlayerResumeHint>,
    local_playerdata: Option<pumpkin_nbt::compound::NbtCompound>,
    local_snapshot_rx: Option<tokio::sync::oneshot::Receiver<LocalLobbySnapshot>>,
    local_commit_token: Option<u64>,
    #[cfg(feature = "cluster-e2e")]
    created_at: std::time::Instant,
    #[cfg(feature = "cluster-e2e")]
    stall_reported: bool,
    #[cfg(feature = "cluster-e2e")]
    first_real_batch_reported: bool,
    #[cfg(feature = "cluster-e2e")]
    peer_complete_reported: bool,
}

impl LobbySession {
    #[must_use]
    pub const fn gid(&self) -> pumpkin_cluster::identity::GlobalPlayerId {
        self.gid
    }

    pub async fn new(
        client: Arc<ClientPlatform>,
        profile: GameProfile,
        config: PlayerConfig,
        server: &Arc<Server>,
    ) -> Option<Self> {
        Self::create(
            client, profile, config, server, false, true, None, None, None, None,
        )
        .await
    }

    pub(crate) async fn from_departing_player(
        player: &Player,
        server: &Arc<Server>,
        forced: bool,
        snapshot_rx: tokio::sync::oneshot::Receiver<LocalLobbySnapshot>,
    ) -> Option<Self> {
        Self::create(
            player.client.clone(),
            player.gameprofile.clone(),
            (**player.config.load()).clone(),
            server,
            forced,
            false,
            player.cluster_gid(),
            None,
            None,
            Some(snapshot_rx),
        )
        .await
    }

    async fn create(
        client: Arc<ClientPlatform>,
        profile: GameProfile,
        config: PlayerConfig,
        server: &Arc<Server>,
        forced: bool,
        send_login: bool,
        existing_gid: Option<pumpkin_cluster::identity::GlobalPlayerId>,
        local_hint: Option<pumpkin_cluster::handoff::PlayerResumeHint>,
        local_playerdata: Option<pumpkin_nbt::compound::NbtCompound>,
        local_snapshot_rx: Option<tokio::sync::oneshot::Receiver<LocalLobbySnapshot>>,
    ) -> Option<Self> {
        let lobby_world = server.worlds.load().first().cloned()?;
        let entity_id = Entity::reserve_ids(1);
        let playerdata_token = server
            .cluster_lobby_session_generation
            .fetch_add(1, Ordering::Relaxed);
        let gid =
            existing_gid.unwrap_or_else(|| super::cluster_presence::reserve_login_gid(server));
        let permission = super::cluster_presence::permission_for_uuid(server, profile.id);
        let source = Arc::new(LobbyCommandSender {
            gid,
            client: client.clone(),
            profile: profile.clone(),
            locale: config.locale.clone(),
            permission,
            forced,
            release: Arc::new(AtomicBool::new(false)),
        });
        let directory_registered = server.cluster_lobby_directory.register(&source);
        super::cluster_presence::publish_lobby_profile(
            server,
            gid,
            &profile,
            config.server_listing,
        );
        let ClientPlatform::Java(java) = client.as_ref() else {
            return None;
        };
        if send_login {
            send_lobby_login(java, entity_id, &lobby_world, server);
        }
        let command_source =
            crate::command::CommandSender::PreJoin(source.clone()).into_source(server);
        crate::command::client_suggestions::send_java_commands_packet(
            java,
            &command_source,
            &server.command_dispatcher.load(),
        );
        let mut sender = ChunkSender::new();
        sender.max_in_flight = u16::MAX;
        let mut session = Self {
            client,
            profile,
            config,
            lobby_world,
            target_world: None,
            entity_id,
            gid,
            source,
            handoff: pumpkin_cluster::handoff::PinnedHandoff::new(),
            sender,
            encoded: FxHashMap::default(),
            required: Vec::new(),
            chunk_need: None,
            requested_hint: false,
            requested_playerdata: false,
            inventory_install_revision: None,
            handoff_outbox: VecDeque::new(),
            playerdata_token,
            image_sent: false,
            real_center_sent: false,
            tab_seen: HashMap::new(),
            presence_revision: u64::MAX,
            invariant_failed: false,
            held: !send_login,
            directory_registered,
            chat_rx: server.cluster_lobby_chat_tx.subscribe(),
            chat_session: None,
            chat_session_checks: VecDeque::new(),
            plugin_channels: HashSet::new(),
            plugin_payloads: VecDeque::new(),
            resource_pack_results: Vec::new(),
            dialog_actions: VecDeque::new(),
            local_hint,
            local_playerdata,
            local_snapshot_rx,
            local_commit_token: None,
            #[cfg(feature = "cluster-e2e")]
            created_at: std::time::Instant::now(),
            #[cfg(feature = "cluster-e2e")]
            stall_reported: false,
            #[cfg(feature = "cluster-e2e")]
            first_real_batch_reported: false,
            #[cfg(feature = "cluster-e2e")]
            peer_complete_reported: false,
        };
        session.send_image(server);
        session.sync_roster(server);
        if session.local_hint.is_none() && session.local_snapshot_rx.is_none() {
            session.request_hint();
        }
        Some(session)
    }

    fn java(&self) -> &JavaClient {
        let ClientPlatform::Java(client) = self.client.as_ref() else {
            unreachable!()
        };
        client
    }

    fn send_image(&mut self, _server: &Server) {
        if self.image_sent {
            return;
        }
        #[cfg(feature = "cluster-e2e")]
        tracing::info!(uuid = %self.profile.id, micros = e2e_lobby_clock_micros(), "cluster e2e lobby first enqueue");
        #[cfg(feature = "cluster-e2e")]
        let real_chunk_wants_before = super::cluster::cluster_wanted_chunks();
        #[cfg(feature = "cluster-e2e")]
        let real_chunk_fetches_before = super::cluster::cluster_inflight_fetch();
        #[cfg(feature = "cluster-e2e")]
        let real_chunk_holders_before = _server.cluster_owned_chunk_blocks.load().len();
        let version = self.java().version.load();
        if version >= JavaMinecraftVersion::V_1_20_2 {
            self.sender
                .register_batch_for_acknowledgement(self.playerdata_token as u32, Vec::new());
        }
        let java = self.java();
        let Some(image) = lobby_static_image(version) else {
            java.try_kick(&TextComponent::text("Unsupported Java lobby version"));
            self.invariant_failed = true;
            return;
        };
        let (head, tail) = if java.compression_enabled {
            let Some(compressed) = image.compressed.as_ref() else {
                java.try_kick(&TextComponent::text("Lobby compression state unavailable"));
                self.invariant_failed = true;
                return;
            };
            (&compressed.head, &compressed.tail)
        } else {
            (&image.uncompressed.head, &image.uncompressed.tail)
        };
        for frame in head.iter().chain(tail.iter()) {
            java.try_enqueue_preframed(frame.clone());
        }
        #[cfg(feature = "cluster-e2e")]
        tracing::debug!(
            uuid = %self.profile.id,
            real_players = _server.worlds.load().iter().map(|world| world.players.load().len()).sum::<usize>(),
            real_chunk_wants_before,
            real_chunk_wants_after = super::cluster::cluster_wanted_chunks(),
            real_chunk_fetches_before,
            real_chunk_fetches_after = super::cluster::cluster_inflight_fetch(),
            real_chunk_holders_before,
            real_chunk_holders_after = _server.cluster_owned_chunk_blocks.load().len(),
            "cluster e2e virtual lobby world footprint"
        );
        self.image_sent = true;
        self.show_progress(0, 0);
    }

    fn request_hint(&mut self) {
        if self.requested_hint {
            return;
        }
        self.handoff_outbox
            .push_back(super::cluster_handoff::HandoffCommand::RequestHint {
                uuid: self.profile.id,
            });
        self.requested_hint = true;
    }

    fn flush_handoff_outbox(&mut self, server: &Server) {
        let Some(command) = self.handoff_outbox.pop_front() else {
            return;
        };
        let Some(tx) = server.cluster_handoff_tx.get() else {
            self.handoff_outbox.push_front(command);
            return;
        };
        match tx.try_send(command) {
            Ok(()) => {}
            Err(tokio::sync::mpsc::error::TrySendError::Full(command)) => {
                self.handoff_outbox.push_front(command);
            }
            Err(tokio::sync::mpsc::error::TrySendError::Closed(_)) => {
                self.java()
                    .try_kick(&TextComponent::text("Cluster handoff unavailable"));
            }
        }
    }

    fn pin_target(&mut self, server: &Server) {
        if self.handoff.hint().is_some()
            || self.local_snapshot_rx.is_some()
            || self.local_commit_token.is_some()
        {
            return;
        }
        let snapshot = server.cluster_handoff.load();
        if snapshot.failed_hints.contains(&self.profile.id) {
            self.invariant_failed = true;
            self.java()
                .try_kick(&TextComponent::text("Resume target identity mismatch"));
            return;
        }
        let hint = self
            .local_hint
            .take()
            .or_else(|| snapshot.hints.get(&self.profile.id).cloned())
            .or_else(|| {
                snapshot.missing_hints.contains(&self.profile.id).then(|| {
                    let info = self.lobby_world.level_info.load();
                    pumpkin_cluster::handoff::PlayerResumeHint {
                        uuid: *self.profile.id.as_bytes(),
                        dimension: self.lobby_world.dimension.minecraft_name.to_string(),
                        position: [
                            f64::from(info.spawn_x) + 0.5,
                            f64::from(info.spawn_y),
                            f64::from(info.spawn_z) + 0.5,
                        ],
                        rotation: [info.spawn_yaw, info.spawn_pitch],
                        view_radius: self.config.view_distance.get(),
                        playerdata_revision: 0,
                        last_owner: None,
                        retained_chunks: Vec::new(),
                        retained_until_millis: 0,
                    }
                })
            });
        let Some(hint) = hint else {
            return;
        };
        let Some(dimension) = pumpkin_data::dimension::Dimension::from_name(&hint.dimension) else {
            if !self.invariant_failed {
                self.invariant_failed = true;
                self.java()
                    .try_kick(&TextComponent::text("Resume dimension unavailable"));
            }
            return;
        };
        let target_world = server
            .worlds
            .load()
            .iter()
            .find(|world| world.dimension.minecraft_name == dimension.minecraft_name)
            .cloned();
        let Some(target_world) = target_world else {
            if !self.invariant_failed {
                self.invariant_failed = true;
                self.java()
                    .try_kick(&TextComponent::text("Resume world unavailable"));
            }
            return;
        };
        self.target_world = Some(target_world);
        if self.handoff.pin(hint.clone()).is_err() {
            return;
        }
        let center = Vector2::new(
            hint.position[0].floor() as i32 >> 4,
            hint.position[2].floor() as i32 >> 4,
        );
        let radius = std::num::NonZeroU8::new(hint.view_radius).unwrap_or(std::num::NonZeroU8::MIN);
        self.required = Cylindrical::new(center, radius)
            .all_chunks_within()
            .collect();
        self.chunk_need = Some(super::cluster::require_lobby_chunks(
            self.target_world.as_ref().unwrap_or(&self.lobby_world),
            &self.required,
        ));
        for position in &self.required {
            self.sender.enqueue_chunk(*position);
        }
    }

    pub fn handle_packet(&mut self, packet: &RawPacket, server: &Arc<Server>) {
        let version = self.java().version.load();
        match self.java().dispatch_common_session_packet(None, packet) {
            Ok(true) => return,
            Ok(false) => {}
            Err(_) => {
                self.fail_invariant("Malformed common lobby packet");
                return;
            }
        }
        let mut payload = &packet.payload[..];
        if packet.id == SChunkBatch::to_id(version) {
            match SChunkBatch::read(&mut payload, &version) {
                Ok(packet) if payload.is_empty() => match self
                    .sender
                    .on_batch_acknowledged(packet.chunks_per_tick, self.playerdata_token as u32)
                {
                    BatchAckResult::Applied => {}
                    BatchAckResult::Missing => {
                        self.fail_invariant("Unexpected chunk batch acknowledgement");
                    }
                    BatchAckResult::RevisionMismatch { expected, received } => {
                        self.fail_invariant(&format!(
                            "Chunk batch acknowledgement revision mismatch: expected {expected}, received {received}"
                        ));
                    }
                },
                _ => {
                    self.fail_invariant("Malformed chunk batch acknowledgement");
                }
            }
        } else if packet.id == SChatCommand::to_id(version) {
            if let Ok(packet) = SChatCommand::read(&mut payload, &version) {
                self.submit_command(
                    server,
                    packet.command.strip_prefix('/').unwrap_or(packet.command),
                );
            }
        } else if packet.id == SChatCommandSigned::to_id(version) {
            if let Ok(packet) = SChatCommandSigned::read(&mut payload, &version) {
                self.submit_command(
                    server,
                    packet.command.strip_prefix('/').unwrap_or(packet.command),
                );
            }
        } else if packet.id == SChatMessage::to_id(version) {
            if let Ok(packet) = SChatMessage::read(&mut payload, &version)
                && let Some(command) = packet.message.strip_prefix('/')
            {
                self.submit_command(server, command);
            } else if let Ok(packet) = SChatMessage::read(&mut &packet.payload[..], &version) {
                let decorated = TextComponent::chat_decorated(
                    &server.advanced_config.chat.format,
                    &self.profile.name,
                    packet.message,
                );
                if super::cluster_chat_out::broadcast_public_chat(
                    server,
                    self.gid,
                    &self.profile.name,
                    packet.message,
                ) {
                    let _ = server.cluster_lobby_chat_tx.send((None, decorated));
                } else {
                    self.source
                        .send_message(&TextComponent::text("Cluster chat queue is full"));
                }
            }
        } else if packet.id == SCommandSuggestion::to_id(version) {
            if let Ok(packet) = SCommandSuggestion::read(&mut payload, &version) {
                self.suggest(&packet, server);
            }
        } else if packet.id == SClientInformationPlay::to_id(version)
            && let Ok(packet) = SClientInformationPlay::read(&mut payload, &version)
        {
            self.update_config(&packet, server);
        } else if packet.id == SPlayerSession::to_id(version) {
            if let Ok(packet) = SPlayerSession::read(&mut payload, &version) {
                if self.chat_session_checks.len() >= 4 {
                    self.fail_invariant("Too many pending chat sessions");
                    return;
                }
                let (tx, rx) = tokio::sync::oneshot::channel();
                self.chat_session_checks.push_back(rx);
                let client = self.client.clone();
                let server = server.clone();
                let uuid = self.profile.id;
                server.clone().spawn_task(async move {
                    let ClientPlatform::Java(java) = client.as_ref() else {
                        let _ = tx.send(None);
                        return;
                    };
                    let result = java
                        .validate_chat_session_uuid(uuid, &server, &packet)
                        .await
                        .ok()
                        .map(|()| packet);
                    let _ = tx.send(result);
                });
            }
        } else if packet.id == SCustomPayload::to_id(version) {
            if let Ok(packet) = SCustomPayload::read(&mut payload, &version) {
                if self.plugin_payloads.len() >= 64 {
                    self.fail_invariant("Too many pending lobby plugin messages");
                    return;
                }
                let mut event = crate::plugin::api::events::player::LobbyCustomPayloadEvent {
                    source: self.source.clone(),
                    channel: packet.channel.to_string(),
                    data: Bytes::copy_from_slice(packet.data),
                };
                let plugin_server = server.clone();
                server.spawn_task(async move {
                    plugin_server
                        .plugin_manager
                        .fire(&plugin_server, &mut event)
                        .await;
                });
                self.plugin_payloads.push_back((
                    packet.channel.to_string(),
                    Bytes::copy_from_slice(packet.data),
                ));
                if let Ok(channels) = std::str::from_utf8(packet.data) {
                    if packet.channel == "minecraft:register" {
                        self.plugin_channels.extend(
                            channels
                                .split('\0')
                                .filter(|channel| !channel.is_empty())
                                .map(str::to_owned),
                        );
                    } else if packet.channel == "minecraft:unregister" {
                        for channel in channels.split('\0') {
                            self.plugin_channels.remove(channel);
                        }
                    }
                }
            }
        } else if packet.id == SPlayResourcePack::to_id(version) {
            if let Ok(packet) = SPlayResourcePack::read(&mut payload, &version) {
                if self.resource_pack_results.len() >= 32 {
                    self.fail_invariant("Too many pending lobby resource pack results");
                    return;
                }
                let mut event = crate::plugin::api::events::player::LobbyResourcePackStatusEvent {
                    source: self.source.clone(),
                    pack_id: packet.uuid,
                    status: format!("{:?}", packet.response_result()),
                };
                let plugin_server = server.clone();
                server.spawn_task(async move {
                    plugin_server
                        .plugin_manager
                        .fire(&plugin_server, &mut event)
                        .await;
                });
                self.resource_pack_results
                    .push((packet.uuid, packet.result));
                if server.advanced_config.resource_pack.java.force
                    && matches!(
                    packet.response_result(),
                    pumpkin_protocol::java::server::play::PlayResourcePackResult::Declined
                        | pumpkin_protocol::java::server::play::PlayResourcePackResult::DownloadFail
                ) {
                    self.java().try_kick(&TextComponent::text(
                        "You must accept the resource pack to play on this server.",
                    ));
                }
            }
        } else if packet.id == SPlayPong::to_id(version) {
            let _ = SPlayPong::read(&mut payload, &version);
        } else if packet.id == SChatAck::to_id(version) {
            let _ = SChatAck::read(&mut payload, &version);
        } else if packet.id
            == pumpkin_protocol::java::server::play::SCustomClickAction::to_id(version)
            && let Ok(packet) = pumpkin_protocol::java::server::play::SCustomClickAction::read(
                &mut payload,
                &version,
            )
        {
            if self.dialog_actions.len() >= 64 {
                self.fail_invariant("Too many pending lobby dialog actions");
                return;
            }
            let action_id = packet.action_id.to_string();
            let action_payload = packet.payload.map(Bytes::copy_from_slice);
            let mut event = crate::plugin::api::events::player::LobbyDialogActionEvent {
                source: self.source.clone(),
                action_id: action_id.clone(),
                payload: action_payload.clone(),
            };
            let plugin_server = server.clone();
            server.spawn_task(async move {
                plugin_server
                    .plugin_manager
                    .fire(&plugin_server, &mut event)
                    .await;
            });
            self.dialog_actions.push_back((action_id, action_payload));
        }
    }

    fn submit_command(&self, server: &Arc<Server>, command: &str) {
        if !super::cluster_session_ingress::submit_lobby_command(
            server,
            self.source.clone(),
            command.to_string(),
        ) {
            self.source
                .send_message(&TextComponent::text("Cluster command queue is full"));
        }
    }

    fn suggest(&self, packet: &SCommandSuggestion<'_>, server: &Arc<Server>) {
        let source =
            crate::command::CommandSender::PreJoin(self.source.clone()).into_source(server);
        let _ = super::cluster_command_suggestions::submit(
            server,
            self.client.clone(),
            source,
            packet.command.to_string(),
            packet.id,
        );
    }

    fn update_config(&mut self, packet: &SClientInformationPlay<'_>, server: &Arc<Server>) {
        let (Ok(main_hand), Ok(chat_mode), Some(view_distance)) = (
            pumpkin_util::Hand::try_from(packet.main_hand.0),
            crate::entity::player::ChatMode::try_from(packet.chat_mode.0),
            std::num::NonZeroU8::new(packet.view_distance as u8),
        ) else {
            return;
        };
        self.config = PlayerConfig {
            locale: packet.locale.to_string(),
            view_distance,
            chat_mode,
            chat_colors: packet.chat_colors,
            skin_parts: packet.skin_parts,
            main_hand,
            text_filtering: packet.text_filtering,
            server_listing: packet.server_listing,
        };
        self.source = Arc::new(LobbyCommandSender {
            gid: self.source.gid,
            client: self.source.client.clone(),
            profile: self.source.profile.clone(),
            locale: self.config.locale.clone(),
            permission: self.source.permission,
            forced: self.source.forced,
            release: self.source.release.clone(),
        });
        super::cluster_presence::publish_lobby_profile(
            server,
            self.gid,
            &self.profile,
            self.config.server_listing,
        );
    }

    pub fn tick(&mut self, server: &Arc<Server>) -> Option<LobbyPromotion> {
        if self.invariant_failed {
            return None;
        }
        while let Some(check) = self.chat_session_checks.front_mut() {
            match check.try_recv() {
                Ok(Some(session)) => {
                    self.chat_session = Some(session);
                    self.chat_session_checks.pop_front();
                }
                Ok(None) | Err(tokio::sync::oneshot::error::TryRecvError::Closed) => {
                    self.fail_invariant("Invalid chat session");
                    return None;
                }
                Err(tokio::sync::oneshot::error::TryRecvError::Empty) => break,
            }
        }
        #[cfg(feature = "cluster-e2e")]
        if !self.stall_reported && self.created_at.elapsed() >= std::time::Duration::from_secs(30) {
            self.stall_reported = true;
            let handoff_snapshot = server.cluster_handoff.load();
            let held = self.target_world.as_ref().map_or(0, |world| {
                self.required
                    .iter()
                    .filter(|position| world.level.is_cluster_held(position))
                    .count()
            });
            let ready = self
                .required
                .iter()
                .filter(|position| self.sender.is_chunk_ready(position))
                .count();
            tracing::warn!(
                uuid = %self.profile.id,
                config_radius = self.config.view_distance.get(),
                hint_radius = self.handoff.hint().map(|hint| hint.view_radius),
                hint_revision = self.handoff.hint().map(|hint| hint.playerdata_revision),
                required = self.required.len(),
                held,
                pending = self.sender.pending_chunks.len(),
                enqueued = self.sender.sent_chunks_count(),
                acknowledged = ready,
                in_flight_batches = self.sender.in_flight_batches,
                requested_hint = self.requested_hint,
                requested_playerdata = self.requested_playerdata,
                inventory_install_revision = self.inventory_install_revision,
                playerdata_outbox = self.handoff_outbox.len(),
                playerdata_response = ?handoff_snapshot.playerdata.get(&self.profile.id).map(|data| (data.token, data.revision)),
                playerdata_unavailable = ?handoff_snapshot.missing_playerdata.get(&self.profile.id).copied(),
                playerdata_failed = ?handoff_snapshot.failed_playerdata.get(&self.profile.id).copied(),
                local_playerdata = self.local_playerdata.is_some(),
                held = self.held,
                released = self.source.release.load(Ordering::Acquire),
                directory_registered = self.directory_registered,
                inventory_route_ready = server.cluster_inventory_install_route_tx.borrow().as_ref().is_some_and(|route| !route.is_closed()),
                join_allowed = super::cluster::cluster_player_join_allowed(),
                "cluster e2e lobby handoff exceeded 30 seconds"
            );
        }
        if let Some(receiver) = self.local_snapshot_rx.as_mut() {
            match receiver.try_recv() {
                Ok(snapshot) => {
                    self.local_hint = Some(snapshot.hint);
                    self.local_playerdata = Some(snapshot.data);
                    self.local_commit_token = snapshot.commit_token;
                    self.local_snapshot_rx = None;
                }
                Err(tokio::sync::oneshot::error::TryRecvError::Empty) => {}
                Err(tokio::sync::oneshot::error::TryRecvError::Closed) => {
                    self.fail_invariant("Local lobby handoff failed");
                    return None;
                }
            }
        }
        if let Some(token) = self.local_commit_token {
            let snapshot = server.cluster_handoff.load();
            if let Some(outcome) = snapshot.commit_outcomes.get(&(self.profile.id, token)) {
                if !outcome.accepted {
                    self.fail_invariant("Local playerdata commit rejected");
                    return None;
                }
                if let Some(hint) = self.local_hint.as_mut() {
                    hint.playerdata_revision = outcome.revision;
                }
                self.local_commit_token = None;
            }
        }
        if !self.directory_registered {
            self.directory_registered = server.cluster_lobby_directory.register(&self.source);
        }
        self.flush_handoff_outbox(server);
        self.poll_virtual_events(server);
        self.pin_target(server);
        let hint = self.handoff.hint()?.clone();
        let world = self.target_world.as_ref()?;
        let center = Vector2::new(
            hint.position[0].floor() as i32 >> 4,
            hint.position[2].floor() as i32 >> 4,
        );
        let peer = self
            .required
            .iter()
            .filter(|position| world.level.is_cluster_held(position))
            .count();
        let received = self
            .required
            .iter()
            .filter(|position| self.sender.is_chunk_ready(position))
            .count();
        self.show_progress(peer, received);
        #[cfg(feature = "cluster-e2e")]
        if peer == self.required.len() && !self.peer_complete_reported {
            self.peer_complete_reported = true;
            tracing::debug!(uuid = %self.profile.id, peer, required = self.required.len(), "cluster e2e lobby peer chunks complete");
        }
        while let Some(batch) = self.sender.prepare_batch(
            &world.level,
            center,
            self.config.view_distance,
            self.playerdata_token as u32,
            self.java().version.load(),
        ) {
            let encoded = ChunkSender::encode_batch(&batch, &mut self.encoded);
            if encoded.is_empty() {
                self.fail_invariant("Real chunk encoding failed");
                return None;
            }
            if !self.real_center_sent {
                self.java().try_send_packet(&CCenterChunk {
                    chunk_x: VarInt(center.x),
                    chunk_z: VarInt(center.y),
                });
                self.real_center_sent = true;
            }
            #[cfg(feature = "cluster-e2e")]
            if !self.first_real_batch_reported {
                self.first_real_batch_reported = true;
                tracing::debug!(uuid = %self.profile.id, peer, required = self.required.len(), batch_chunks = encoded.len(), "cluster e2e lobby first real batch enqueued");
            }
            self.sender
                .commit_batch(&batch, &encoded, &self.client, self.playerdata_token as u32);
        }
        if peer != self.required.len() || !super::cluster::cluster_player_join_allowed() {
            return None;
        }
        if self.local_playerdata.is_none() && !self.requested_playerdata {
            self.handoff_outbox.push_back(
                super::cluster_handoff::HandoffCommand::RequestPlayerdata {
                    uuid: self.profile.id,
                    revision: hint.playerdata_revision,
                    token: self.playerdata_token,
                },
            );
            self.requested_playerdata = true;
            return None;
        }
        let snapshot = server.cluster_handoff.load();
        if snapshot
            .failed_playerdata
            .get(&self.profile.id)
            .is_some_and(|token| *token == self.playerdata_token)
        {
            self.invariant_failed = true;
            self.java()
                .try_kick(&TextComponent::text("Playerdata identity mismatch"));
            return None;
        }
        let data = if let Some(data) = self.local_playerdata.as_ref() {
            Some(data.clone())
        } else if let Some(data) = snapshot.playerdata.get(&self.profile.id) {
            if data.token != self.playerdata_token {
                return None;
            }
            if let Err(pumpkin_cluster::handoff::HandoffError::RevisionMismatch {
                expected,
                received,
            }) = self.handoff.validate_playerdata_revision(data.revision)
            {
                self.invariant_failed = true;
                tracing::warn!(uuid = %self.profile.id, expected, received, "playerdata handoff revision mismatch");
                self.java()
                    .try_kick(&TextComponent::text("Playerdata revision mismatch"));
                return None;
            }
            super::cluster_handoff::deserialize_nbt(&data.data)
        } else if let Some((token, revision)) = snapshot.missing_playerdata.get(&self.profile.id) {
            if *token != self.playerdata_token {
                return None;
            }
            if *revision != hint.playerdata_revision {
                self.invariant_failed = true;
                tracing::warn!(
                    uuid = %self.profile.id,
                    expected = hint.playerdata_revision,
                    received = *revision,
                    "unavailable playerdata revision mismatch"
                );
                self.java()
                    .try_kick(&TextComponent::text("Playerdata revision mismatch"));
                return None;
            }
            None
        } else {
            return None;
        };
        if self.inventory_install_revision.is_none() {
            let chunk = pumpkin_cluster::protocol::ChunkKey::new(
                super::cluster_world_delta::world_id(world),
                center.x,
                center.y,
            );
            match super::cluster_invsee::submit_handoff_inventory(
                server,
                self.gid,
                self.playerdata_token,
                chunk,
                data.as_ref(),
            ) {
                Ok(revision) => self.inventory_install_revision = Some(revision),
                Err(pumpkin_cluster::inventory::InventoryApplyError::QueueFull)
                | Err(pumpkin_cluster::inventory::InventoryApplyError::TargetUnavailable) => {}
                Err(error) => {
                    tracing::error!(uuid = %self.profile.id, ?error, "cluster lobby inventory handoff rejected");
                    self.fail_invariant("Inventory handoff unavailable");
                    return None;
                }
            }
        }
        if !self
            .required
            .iter()
            .all(|position| self.sender.is_chunk_ready(position))
        {
            return None;
        }
        if self.held && !self.source.release.load(Ordering::Acquire) {
            return None;
        }
        let Some(expected_inventory_revision) = self.inventory_install_revision else {
            return None;
        };
        let installed = server.cluster_inventory_handoff_installed.load();
        let Some(status) = installed.get(&self.gid) else {
            return None;
        };
        if status.token != self.playerdata_token {
            return None;
        }
        if status.revision != Ok(expected_inventory_revision) {
            tracing::error!(uuid = %self.profile.id, token = self.playerdata_token, ?status.revision, expected_inventory_revision, "cluster lobby inventory handoff completion rejected");
            self.fail_invariant("Inventory handoff unavailable");
            return None;
        }
        if !self.chat_session_checks.is_empty() {
            return None;
        }
        if server
            .cluster_inventory_install_route_tx
            .borrow()
            .as_ref()
            .is_none_or(tokio::sync::mpsc::Sender::is_closed)
        {
            return None;
        }
        if !self.directory_registered {
            return None;
        }
        #[cfg(feature = "cluster-e2e")]
        tracing::debug!(
            uuid = %self.profile.id,
            token = self.playerdata_token,
            revision = hint.playerdata_revision,
            required = self.required.len(),
            acknowledged = self.required.iter().filter(|position| self.sender.is_chunk_ready(position)).count(),
            "cluster e2e lobby client chunks acknowledged"
        );
        Some(LobbyPromotion {
            gid: self.gid,
            entity_id: self.entity_id,
            config: self.config.clone(),
            world: world.clone(),
            data,
            hint,
            inventory_install_token: self.playerdata_token,
            chunk_need: self.chunk_need.take()?,
            chunk_sender: std::mem::take(&mut self.sender),
            chat_session: self.chat_session.take(),
            plugin_payloads: std::mem::take(&mut self.plugin_payloads),
            resource_pack_results: std::mem::take(&mut self.resource_pack_results),
            dialog_actions: std::mem::take(&mut self.dialog_actions),
            reentered: self.held,
        })
    }

    pub fn poll_virtual_events(&mut self, server: &Arc<Server>) {
        while let Ok((target, message)) = self.chat_rx.try_recv() {
            if target
                .as_ref()
                .is_none_or(|target| target.eq_ignore_ascii_case(&self.profile.name))
            {
                self.source.send_message(&message);
            }
        }
        self.sync_roster(server);
    }

    pub fn finish_promotion(&mut self, server: &Server) -> bool {
        if !self.directory_registered || !server.cluster_lobby_directory.unregister(self.gid) {
            return false;
        }
        self.directory_registered = false;
        true
    }

    pub fn close(&self, server: &Server) {
        if self.inventory_install_revision.is_some() {
            super::cluster_invsee::cancel_handoff_inventory_install(
                server,
                self.gid,
                self.playerdata_token,
            );
        }
        if self.directory_registered {
            let _ = server.cluster_lobby_directory.unregister(self.gid);
        }
    }

    fn fail_invariant(&mut self, message: &str) {
        if self.invariant_failed {
            return;
        }
        self.invariant_failed = true;
        tracing::warn!(uuid = %self.profile.id, reason = message, "cluster lobby handoff invariant failed");
        self.java()
            .try_kick(&TextComponent::text(message.to_string()));
    }

    fn sync_roster(&mut self, server: &Arc<Server>) {
        let snapshot = server.cluster_presence.snapshot();
        if snapshot.revision == self.presence_revision {
            return;
        }
        self.presence_revision = snapshot.revision;
        let permission = snapshot.permission_for_uuid(self.profile.id.as_bytes());
        if permission != self.source.permission {
            self.source = Arc::new(LobbyCommandSender {
                gid: self.source.gid,
                client: self.source.client.clone(),
                profile: self.source.profile.clone(),
                locale: self.config.locale.clone(),
                permission,
                forced: self.source.forced,
                release: self.source.release.clone(),
            });
            let command_source =
                crate::command::CommandSender::PreJoin(self.source.clone()).into_source(server);
            crate::command::client_suggestions::send_java_commands_packet(
                self.java(),
                &command_source,
                &server.command_dispatcher.load(),
            );
        }
        let mut desired = HashMap::new();
        for (gid, entry) in &snapshot.entries {
            if snapshot.is_visible_to(gid, &entry.uuid, Some(self.profile.id.as_bytes())) {
                desired.insert(
                    Uuid::from_bytes(entry.uuid),
                    (entry.name.clone(), entry.properties.clone()),
                );
            }
        }
        if snapshot.is_visible_to(
            &self.gid,
            self.profile.id.as_bytes(),
            Some(self.profile.id.as_bytes()),
        ) {
            desired.entry(self.profile.id).or_insert_with(|| {
                let properties = self
                    .profile
                    .properties
                    .load()
                    .iter()
                    .map(|property| {
                        pumpkin_cluster::presence::PresenceProperty::new(
                            property.name.to_string(),
                            property.value.to_string(),
                            property.signature.as_ref().map(ToString::to_string),
                        )
                    })
                    .collect();
                (self.profile.name.clone(), properties)
            });
        }
        let removed = self
            .tab_seen
            .keys()
            .filter(|uuid| !desired.contains_key(uuid))
            .copied()
            .collect::<Vec<_>>();
        if !removed.is_empty() {
            self.java()
                .try_send_packet(&CRemovePlayerInfo::new(&removed));
        }
        for (uuid, (name, properties)) in &desired {
            if self.tab_seen.get(uuid) == Some(&(name.clone(), properties.clone())) {
                continue;
            }
            if self.tab_seen.contains_key(uuid) {
                self.java()
                    .try_send_packet(&CRemovePlayerInfo::new(&[*uuid]));
            }
            let properties = super::cluster_chat_pm::protocol_properties(properties);
            self.send_roster_entry(*uuid, name, &properties);
        }
        self.tab_seen = desired;
    }

    fn send_roster_entry(&self, uuid: Uuid, name: &str, properties: &[pumpkin_protocol::Property]) {
        let actions = [
            PlayerAction::AddPlayer { name, properties },
            PlayerAction::UpdateGameMode(VarInt(GameMode::Spectator as i32)),
            PlayerAction::UpdateListed(true),
            PlayerAction::UpdateLatency(VarInt(0)),
        ];
        let players = [pumpkin_protocol::java::client::play::Player {
            uuid,
            actions: &actions,
        }];
        self.java().try_send_packet(&CPlayerInfoUpdate::new(
            (PlayerInfoFlags::ADD_PLAYER
                | PlayerInfoFlags::UPDATE_GAME_MODE
                | PlayerInfoFlags::UPDATE_LISTED
                | PlayerInfoFlags::UPDATE_LATENCY)
                .bits(),
            &players,
        ));
    }

    fn show_progress(&self, peer: usize, received: usize) {
        let total = self.required.len().max(1);
        let peer = peer.saturating_mul(100).saturating_div(total);
        let received = received.saturating_mul(100).saturating_div(total);
        self.java()
            .try_send_packet(&pumpkin_protocol::java::client::play::CTitleAnimation::new(
                0,
                LOBBY_TITLE_STAY_TICKS,
                5,
            ));
        self.java()
            .try_send_packet(&pumpkin_protocol::java::client::play::CTitleText::new(
                &TextComponent::text("Loading"),
            ));
        self.java()
            .try_send_packet(&pumpkin_protocol::java::client::play::CSubtitle::new(
                &lobby_progress_subtitle(peer, received),
            ));
    }
}

fn send_lobby_login(client: &JavaClient, entity_id: i32, world: &World, server: &Server) {
    let dimensions: Vec<ResourceLocation> = server
        .dimensions
        .iter()
        .map(|dimension| ResourceLocation::from(dimension.minecraft_name))
        .collect();
    client.try_send_packet(&CLogin::new(
        entity_id,
        server.basic_config.hardcore,
        &dimensions,
        server
            .advanced_config
            .networking
            .java
            .max_players
            .try_into()
            .unwrap_or(u16::MAX.into()),
        server
            .advanced_config
            .networking
            .java
            .view_distance
            .get()
            .into(),
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
            world.dimension.clone(),
            pumpkin_world::biome::hash_seed(world.level.seed.0),
            GameMode::Spectator as u8,
            -1,
            false,
            false,
            None,
            VarInt(0),
            world.sea_level.into(),
        ),
        server.advanced_config.networking.java.online_mode,
        true,
    ));
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
const LOBBY_CHUNK_BLOCKS: i32 = 16;
/// Fake chunk radius around the portal chunk (9x9, 4 out each side).
const LOBBY_CHUNK_RADIUS: i32 = 4;
/// Frozen noon shown while waiting; rate `0.0` keeps lobby time paused.
const LOBBY_FROZEN_DAYTIME: i64 = 6000;
/// Title stays `20` ticks so it auto-hides one second after updates stop.
const LOBBY_TITLE_STAY_TICKS: i32 = 20;

fn lobby_center_chunk() -> Vector2<i32> {
    Vector2::new(LOBBY_BLOCK_X >> 4, LOBBY_BLOCK_Z >> 4)
}

fn lobby_progress_subtitle(peer_percent: usize, client_percent: usize) -> TextComponent {
    TextComponent::text(format!("{peer_percent}% "))
        .color_named(NamedColor::DarkGray)
        .add_child(TextComponent::text(format!("{client_percent}%")).color_named(NamedColor::White))
}

#[derive(Clone)]
struct LobbyStatic {
    uncompressed: LobbyFrames,
    compressed: Option<LobbyFrames>,
}

#[derive(Clone)]
struct LobbyFrames {
    head: Box<[Bytes]>,
    tail: Box<[Bytes]>,
}
const LOBBY_IMAGE_SLOTS: usize = JavaMinecraftVersion::Unknown as usize + 1;
static LOBBY_IMAGES: [OnceLock<LobbyStatic>; LOBBY_IMAGE_SLOTS] =
    [const { OnceLock::new() }; LOBBY_IMAGE_SLOTS];
const LOBBY_IMAGE_VERSIONS: [JavaMinecraftVersion; 51] = [
    JavaMinecraftVersion::V_1_7_2,
    JavaMinecraftVersion::V_1_7_6,
    JavaMinecraftVersion::V_1_8,
    JavaMinecraftVersion::V_1_9,
    JavaMinecraftVersion::V_1_9_1,
    JavaMinecraftVersion::V_1_9_2,
    JavaMinecraftVersion::V_1_9_3,
    JavaMinecraftVersion::V_1_10,
    JavaMinecraftVersion::V_1_11,
    JavaMinecraftVersion::V_1_11_1,
    JavaMinecraftVersion::V_1_12,
    JavaMinecraftVersion::V_1_12_1,
    JavaMinecraftVersion::V_1_12_2,
    JavaMinecraftVersion::V_1_13,
    JavaMinecraftVersion::V_1_13_1,
    JavaMinecraftVersion::V_1_13_2,
    JavaMinecraftVersion::V_1_14,
    JavaMinecraftVersion::V_1_14_1,
    JavaMinecraftVersion::V_1_14_2,
    JavaMinecraftVersion::V_1_14_3,
    JavaMinecraftVersion::V_1_14_4,
    JavaMinecraftVersion::V_1_15,
    JavaMinecraftVersion::V_1_15_1,
    JavaMinecraftVersion::V_1_15_2,
    JavaMinecraftVersion::V_1_16,
    JavaMinecraftVersion::V_1_16_1,
    JavaMinecraftVersion::V_1_16_2,
    JavaMinecraftVersion::V_1_16_3,
    JavaMinecraftVersion::V_1_16_4,
    JavaMinecraftVersion::V_1_17,
    JavaMinecraftVersion::V_1_17_1,
    JavaMinecraftVersion::V_1_18,
    JavaMinecraftVersion::V_1_18_2,
    JavaMinecraftVersion::V_1_19,
    JavaMinecraftVersion::V_1_19_1,
    JavaMinecraftVersion::V_1_19_3,
    JavaMinecraftVersion::V_1_19_4,
    JavaMinecraftVersion::V_1_20,
    JavaMinecraftVersion::V_1_20_2,
    JavaMinecraftVersion::V_1_20_3,
    JavaMinecraftVersion::V_1_20_5,
    JavaMinecraftVersion::V_1_21,
    JavaMinecraftVersion::V_1_21_2,
    JavaMinecraftVersion::V_1_21_4,
    JavaMinecraftVersion::V_1_21_5,
    JavaMinecraftVersion::V_1_21_6,
    JavaMinecraftVersion::V_1_21_7,
    JavaMinecraftVersion::V_1_21_9,
    JavaMinecraftVersion::V_1_21_11,
    JavaMinecraftVersion::V_26_1,
    JavaMinecraftVersion::V_26_2,
];

pub(crate) fn initialize_lobby_images(server: &Server) {
    let config = &server.advanced_config.networking.java.compression;
    let compression = config
        .enabled
        .then_some((config.info.threshold as usize, config.info.level));
    for version in LOBBY_IMAGE_VERSIONS {
        let _ = LOBBY_IMAGES[version as usize].set(lobby_build_image(version, compression));
    }
}
fn lobby_serialize<P: pumpkin_protocol::ClientPacket + Sync>(
    packet: &P,
    version: &JavaMinecraftVersion,
    out: &mut Vec<Bytes>,
) {
    if let Ok(data) = pumpkin_protocol::java::packet_encoder::serialize_packet(packet, version) {
        out.push(data);
    }
}
fn lobby_build_image(
    version: JavaMinecraftVersion,
    compression: Option<(usize, u32)>,
) -> LobbyStatic {
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
            let portal_pos =
                BlockPos::new(portal_origin_x + dx, LOBBY_PLATFORM_Y, portal_origin_z + dz);
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
        &CGameEvent::new(GameEvent::ChangeGameMode, GameMode::Spectator as i32 as f32),
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
    let frame = |packets: &[Bytes],
                 encoder: &mut pumpkin_protocol::java::packet_encoder::TCPNetworkEncoder<
        tokio::io::Sink,
    >| {
        packets
            .iter()
            .map(|packet| {
                let mut framed = Vec::new();
                encoder
                    .frame_packet(packet, &mut framed)
                    .expect("valid startup lobby packet");
                Bytes::from(framed)
            })
            .collect::<Vec<_>>()
            .into_boxed_slice()
    };
    let mut encoder =
        pumpkin_protocol::java::packet_encoder::TCPNetworkEncoder::new(tokio::io::sink());
    let uncompressed = LobbyFrames {
        head: frame(&head, &mut encoder),
        tail: frame(&tail, &mut encoder),
    };
    let compressed = if version >= JavaMinecraftVersion::V_1_8 {
        compression.map(|compression| {
            let mut encoder =
                pumpkin_protocol::java::packet_encoder::TCPNetworkEncoder::new(tokio::io::sink());
            encoder.set_compression(compression);
            LobbyFrames {
                head: frame(&head, &mut encoder),
                tail: frame(&tail, &mut encoder),
            }
        })
    } else {
        None
    };
    LobbyStatic {
        uncompressed,
        compressed,
    }
}
fn lobby_static_image(version: JavaMinecraftVersion) -> Option<&'static LobbyStatic> {
    LOBBY_IMAGES.get(version as usize)?.get()
}
fn lobby_fake_chunk(x: i32, z: i32) -> pumpkin_world::chunk::ChunkData {
    let chunk = pumpkin_world::chunk::ChunkData::empty(x, z);
    let portal = Block::END_PORTAL.default_state.id;
    let center = lobby_center_chunk();
    let holds_portal = x == center.x && z == center.y;
    if holds_portal {
        for dz in 0..LOBBY_CHUNK_BLOCKS {
            for dx in 0..LOBBY_CHUNK_BLOCKS {
                chunk.set_block_absolute_y(dx as usize, LOBBY_PLATFORM_Y, dz as usize, portal);
            }
        }
        if let Ok(mut entities) = chunk.pending_block_entities.lock() {
            for dz in 0..LOBBY_CHUNK_BLOCKS {
                for dx in 0..LOBBY_CHUNK_BLOCKS {
                    let pos =
                        BlockPos::new((center.x << 4) + dx, LOBBY_PLATFORM_Y, (center.y << 4) + dz);
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
            light.sky_light = vec![LightContainer::new_filled(15); sections].into_boxed_slice();
        } else {
            light.sky_light = vec![LightContainer::Empty(0); sections].into_boxed_slice();
        }
        light.block_light = vec![LightContainer::Empty(0); sections].into_boxed_slice();
    }
    chunk
}

#[must_use]
pub fn lobby_enter_manual(player: &Player, forced: bool) -> bool {
    if !is_cluster_secondary() {
        return false;
    }
    let ClientPlatform::Java(client) = player.client.as_ref() else {
        return false;
    };
    client.request_cluster_lobby(forced)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn cached_lobby_frames_decode_with_and_without_compression() {
        for (version, compression) in [
            (JavaMinecraftVersion::V_1_21_11, None),
            (JavaMinecraftVersion::V_1_21_11, Some((256, 4))),
            (JavaMinecraftVersion::V_26_2, None),
            (JavaMinecraftVersion::V_26_2, Some((256, 4))),
        ] {
            let image = lobby_build_image(version, compression);
            let (head, tail) = if compression.is_some() {
                let frames = image.compressed.as_ref().unwrap();
                (&frames.head, &frames.tail)
            } else {
                (&image.uncompressed.head, &image.uncompressed.tail)
            };
            let frames = head.iter().chain(tail.iter());
            let mut stream = Vec::new();
            let mut frame_count = 0;
            for frame in frames {
                stream.extend_from_slice(frame);
                frame_count += 1;
            }
            let mut decoder =
                pumpkin_protocol::java::packet_decoder::TCPNetworkDecoder::new(stream.as_slice());
            if let Some((threshold, _)) = compression {
                decoder.set_compression(threshold);
            }
            let mut packet_ids = Vec::with_capacity(frame_count);
            for _ in 0..frame_count {
                packet_ids.push(decoder.get_raw_packet().await.unwrap().id);
            }
            assert_eq!(packet_ids[0], CGameEvent::to_id(version));
            assert_eq!(packet_ids[1], CPlayerPosition::to_id(version));
            assert_eq!(packet_ids[2], CPlayerSpawnPosition::to_id(version));
            assert_eq!(packet_ids[3], CCenterChunk::to_id(version));
            assert_eq!(packet_ids[4], CChunkBatchStart::to_id(version));
            assert_eq!(
                packet_ids
                    .iter()
                    .filter(|id| **id == CChunkData::to_id(version))
                    .count(),
                81
            );
            assert_eq!(packet_ids[86], CChunkBatchEnd::to_id(version));
        }
    }

    #[test]
    fn lobby_directory_saturation_retains_live_session_control() {
        let (directory, mut rx) = LobbyDirectory::new(1);
        let first = Arc::new(AtomicBool::new(false));
        let second = Arc::new(AtomicBool::new(false));
        let first_gid = pumpkin_cluster::identity::GlobalPlayerId::new(
            pumpkin_cluster::identity::ServerId(1),
            pumpkin_cluster::identity::PlayerSlot(1),
        );
        let second_gid = pumpkin_cluster::identity::GlobalPlayerId::new(
            pumpkin_cluster::identity::ServerId(1),
            pumpkin_cluster::identity::PlayerSlot(2),
        );

        assert!(directory.register_entry(
            "first".to_string(),
            first_gid,
            Arc::downgrade(&first),
            std::sync::Weak::new(),
        ));
        assert!(!directory.register_entry(
            "second".to_string(),
            second_gid,
            Arc::downgrade(&second),
            std::sync::Weak::new(),
        ));
        assert!(
            matches!(rx.try_recv(), Ok(LobbyDirectoryCommand::Register { gid, .. }) if gid == first_gid)
        );
        assert!(directory.register_entry(
            "second".to_string(),
            second_gid,
            Arc::downgrade(&second),
            std::sync::Weak::new(),
        ));
    }

    #[test]
    fn lobby_directory_does_not_release_expired_session() {
        let (directory, _rx) = LobbyDirectory::new(1);
        let release = Arc::new(AtomicBool::new(false));
        directory.snapshot.store(Arc::new(HashMap::from([(
            "player".to_string(),
            LobbyDirectoryEntry {
                gid: pumpkin_cluster::identity::GlobalPlayerId::new(
                    pumpkin_cluster::identity::ServerId(1),
                    pumpkin_cluster::identity::PlayerSlot(1),
                ),
                release: Arc::downgrade(&release),
                client: std::sync::Weak::new(),
            },
        )])));
        assert!(directory.release("PLAYER"));
        assert!(release.load(Ordering::Acquire));
        drop(release);
        assert!(!directory.release("player"));
    }

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
            middle
                .section
                .get_block_absolute_y(15, LOBBY_PLATFORM_Y, 15),
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
        assert!(
            light
                .sky_light
                .iter()
                .all(|section| matches!(section, LightContainer::Full(_)))
        );
        assert!(light.block_light.iter().all(|section| section.is_empty()));
        if let LightContainer::Full(data) = &light.sky_light[10] {
            assert!(data.iter().all(|byte| *byte == 0xFF));
        } else {
            panic!();
        }
        drop(light);
        let far = lobby_fake_chunk(center.x + 5, center.y + 5);
        let far_light = far.light_engine.lock().unwrap();
        assert!(far_light.sky_light.iter().all(|section| section.is_empty()));
    }

    #[test]
    fn camera_head_sits_one_block_above_portal_top() {
        assert_eq!(LOBBY_CAMERA_Y, f64::from(LOBBY_PLATFORM_Y) + 2.0);
        let player_eye = LOBBY_ANCHOR_Y + 1.62;
        let portal_head = f64::from(LOBBY_PLATFORM_Y) + 2.0;
        assert!((player_eye - portal_head).abs() < 1e-9);
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
