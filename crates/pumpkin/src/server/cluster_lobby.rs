use std::collections::{HashMap, VecDeque};
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, Ordering};

use bytes::Bytes;
use pumpkin_data::Block;
use pumpkin_data::entity::EntityType;
use pumpkin_protocol::codec::var_int::VarInt;
use pumpkin_protocol::java::client::play::{
    CBlockEntityData, CBlockUpdate, CCenterChunk, CChunkBatchEnd, CChunkBatchStart, CGameEvent,
    CLogin, CPlayerPosition, CPlayerSpawnPosition, CSetCamera, CSpawnEntity, CUpdateTime,
    GameEvent, PlayerSpawnData,
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
#[cfg(test)]
use pumpkin_world::chunk::format::LightContainer;
use pumpkin_world::cylindrical_chunk_iterator::Cylindrical;
use pumpkin_world::level::is_cluster_secondary;
use tokio::sync::mpsc;
use uuid::Uuid;

use crate::block::entities::BlockEntity;
use crate::block::entities::end_portal::EndPortalBlockEntity;
use crate::entity::player::Player;
use crate::entity::{Entity, NBTStorage};
use crate::net::chunk_sender::{BatchAckResult, ClusterChunkEncoder};
use crate::net::java::JavaClient;
#[cfg(test)]
use crate::net::java::chunk_data::CChunkData;
use crate::net::java::play::session::bounded_view_distance as bounded_lobby_view_distance;
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
pub(crate) struct LobbyDirectoryEntry {
    pub(crate) gid: pumpkin_cluster::identity::GlobalPlayerId,
    pub(crate) uuid: [u8; 16],
    pub(crate) session: u64,
    release: std::sync::Weak<AtomicBool>,
    pub(crate) client: std::sync::Weak<ClientPlatform>,
}

pub(crate) enum LobbyDirectoryCommand {
    Register {
        name: String,
        gid: pumpkin_cluster::identity::GlobalPlayerId,
        uuid: [u8; 16],
        session: u64,
        release: std::sync::Weak<AtomicBool>,
        client: std::sync::Weak<ClientPlatform>,
    },
    Unregister(pumpkin_cluster::identity::GlobalPlayerId, [u8; 16], u64),
}

impl LobbyDirectory {
    pub(crate) fn sessions(&self) -> Arc<HashMap<String, LobbyDirectoryEntry>> {
        self.snapshot.load_full()
    }

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
            *source.profile.id.as_bytes(),
            source.session_generation,
            Arc::downgrade(&source.release),
            Arc::downgrade(&source.client),
        )
    }

    fn register_entry(
        &self,
        name: String,
        gid: pumpkin_cluster::identity::GlobalPlayerId,
        uuid: [u8; 16],
        session: u64,
        release: std::sync::Weak<AtomicBool>,
        client: std::sync::Weak<ClientPlatform>,
    ) -> bool {
        self.tx
            .try_send(LobbyDirectoryCommand::Register {
                name,
                gid,
                uuid,
                session,
                release,
                client,
            })
            .is_ok()
    }

    fn unregister(
        &self,
        gid: pumpkin_cluster::identity::GlobalPlayerId,
        uuid: [u8; 16],
        session: u64,
    ) -> bool {
        self.tx
            .try_send(LobbyDirectoryCommand::Unregister(gid, uuid, session))
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

    pub fn disconnect_ip(&self, ip: std::net::IpAddr, message: &TextComponent) {
        for entry in self.snapshot.load().values() {
            if let Some(client) = entry.client.upgrade() {
                if client.address().ip() == ip {
                    client.try_kick(crate::net::DisconnectReason::Kicked, message);
                }
            }
        }
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

    pub fn route_message_identity(
        &self,
        gid: pumpkin_cluster::identity::GlobalPlayerId,
        uuid: [u8; 16],
        session: u64,
        message: &TextComponent,
    ) -> bool {
        let snapshot = self.snapshot.load();
        let Some(entry) = snapshot
            .values()
            .find(|entry| (entry.gid, entry.uuid, entry.session) == (gid, uuid, session))
        else {
            return false;
        };
        let Some(client) = entry.client.upgrade() else {
            return false;
        };
        send_session_message(
            client.as_ref(),
            message,
            pumpkin_util::translation::Locale::EnUs,
        );
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
        send_session_message(
            client.as_ref(),
            message,
            pumpkin_util::translation::Locale::EnUs,
        );
        true
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
                    uuid,
                    session,
                    release,
                    client,
                } => {
                    if entries
                        .get(&name)
                        .is_some_and(|entry: &LobbyDirectoryEntry| entry.session > session)
                    {
                        continue;
                    }
                    entries.insert(
                        name,
                        LobbyDirectoryEntry {
                            gid,
                            uuid,
                            session,
                            release,
                            client,
                        },
                    );
                }
                LobbyDirectoryCommand::Unregister(gid, uuid, session) => {
                    entries.retain(|_, entry| {
                        (entry.gid, entry.uuid, entry.session) != (gid, uuid, session)
                    });
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
    pub session_generation: u64,
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
        let locale = self
            .locale
            .parse()
            .unwrap_or(pumpkin_util::translation::Locale::EnUs);
        send_session_message(self.client.as_ref(), text, locale);
    }
}

fn send_session_message(
    client: &ClientPlatform,
    text: &TextComponent,
    locale: pumpkin_util::translation::Locale,
) {
    match client {
        ClientPlatform::Java(client) => {
            client.try_send_packet(
                &pumpkin_protocol::java::client::play::CSystemChatMessage::new(text, false),
            );
        }
        ClientPlatform::Bedrock(client) => {
            use pumpkin_protocol::bedrock::server::text::SText;
            let packet = match &*text.0.content {
                pumpkin_util::text::TextContent::Translate {
                    translate,
                    bedrock_translate,
                    with,
                } => SText::translation(
                    bedrock_translate
                        .as_deref()
                        .unwrap_or(translate.as_ref())
                        .to_string(),
                    with.iter()
                        .map(pumpkin_util::text::TextComponentBase::to_bedrock_string)
                        .collect(),
                ),
                _ => SText::system_message(text.0.to_bedrock_legacy(locale)),
            };
            client.try_enqueue_client_packet(&packet);
        }
    }
}

pub(crate) struct PreparedLobbyChunkNeed {
    actor: pumpkin_cluster::identity::GlobalPlayerId,
    install_generation: u64,
    need: Option<super::cluster::ChunkNeedLease>,
    delivered: Vec<pumpkin_cluster::protocol::ChunkKey>,
    retirement: Option<tokio::sync::mpsc::OwnedPermit<super::cluster_chunk_watch::WatchCommand>>,
}

impl PreparedLobbyChunkNeed {
    fn into_activation(
        mut self,
    ) -> (
        super::cluster::ChunkNeedLease,
        tokio::sync::mpsc::OwnedPermit<super::cluster_chunk_watch::WatchCommand>,
    ) {
        (
            self.need.take().expect("prepared chunk claim"),
            self.retirement
                .take()
                .expect("prepared retirement capacity"),
        )
    }
}

impl Drop for PreparedLobbyChunkNeed {
    fn drop(&mut self) {
        if let Some(need) = self.need.take() {
            self.retirement
                .take()
                .expect("prepared retirement capacity")
                .send(super::cluster_chunk_watch::WatchCommand::RetainPrepared {
                    actor: self.actor,
                    install_generation: self.install_generation,
                    need,
                    delivered: std::mem::take(&mut self.delivered),
                    retained_until: super::cluster_chunk_watch::retirement_now_millis()
                        .saturating_add(30_000),
                });
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
    pub(crate) chunk_need: PreparedLobbyChunkNeed,
    pub chunk_sender: ChunkSender,
    pub(crate) sent_presentations: rustc_hash::FxHashMap<
        Vector2<i32>,
        Arc<super::cluster_chunk_presentation::ChunkPresentation>,
    >,
    pub chat_session: Option<SPlayerSession>,
    pub reentered: bool,
}

#[derive(Debug, Clone, PartialEq)]
pub(crate) struct LocalLobbySnapshot {
    pub hint: pumpkin_cluster::handoff::PlayerResumeHint,
    pub data: Arc<[u8]>,
    pub commit_token: Option<u64>,
}

pub(crate) struct DeferredDeparture {
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
    let retained = super::cluster_presence::freeze_player_handoff(server, player);
    if logout {
        super::cluster_presence::publish_logout_gid(
            server,
            gid,
            *player.gameprofile.id.as_bytes(),
            player
                .cluster_owner_install_generation
                .load(Ordering::Acquire),
        );
    }
    let lease = super::cluster_world_owner::player_action_lease(server, player);
    let install_generation = player
        .cluster_owner_install_generation
        .load(std::sync::atomic::Ordering::Acquire);
    match server.cluster_controller.request_player_departure(
        player.clone(),
        lease,
        install_generation,
        expected_close_token,
        logout,
        retained,
    ) {
        Ok(ready) => Some(DeferredDeparture { ready }),
        Err(error) => {
            tracing::error!(?gid, ?error, "cluster player departure ticket failed");
            None
        }
    }
}

pub(crate) fn local_snapshot_from_player(
    player: &Player,
    server: &Server,
    frozen: &super::cluster_controller::FrozenPlayerDeparture,
    retained: Option<&super::cluster_presence::RetainedDeparture>,
) -> Option<LocalLobbySnapshot> {
    if player.cluster_gid() != Some(frozen.lease.gid) || frozen.lease.key.0 != player.gameprofile.id
    {
        return None;
    }
    let worlds = server.worlds.load();
    let Some(world) = worlds
        .iter()
        .find(|world| super::cluster_world_delta::world_id(world) == frozen.spatial.world)
    else {
        return None;
    };
    let effective = frozen.spatial.frame.effective;
    let revision = server
        .cluster_handoff
        .load()
        .hints
        .get(&player.gameprofile.id)
        .map_or(0, |hint| hint.playerdata_revision);
    let mut data = player.cluster_handoff_base_nbt.clone();
    if super::cluster_player_projection::write_owned_player_nbt(
        server,
        &frozen.living,
        frozen.spatial,
        &mut data,
    )
    .is_err()
    {
        return None;
    }
    if super::cluster_invsee::write_frozen_player_inventory_nbt(
        &mut data,
        frozen.lease,
        &frozen.player_inventory,
        &frozen.ender_inventory,
    )
    .is_err()
    {
        return None;
    }
    Some(LocalLobbySnapshot {
        hint: pumpkin_cluster::handoff::PlayerResumeHint {
            uuid: *player.gameprofile.id.as_bytes(),
            dimension: world.dimension.minecraft_name.to_string(),
            position: effective.position,
            rotation: [effective.yaw, effective.pitch],
            view_radius: player.config.load().view_distance.get(),
            playerdata_revision: revision,
            last_owner: Some(server.advanced_config.cluster.server_id),
            retained_chunks: retained.map_or_else(Vec::new, |retained| {
                retained
                    .chunks
                    .iter()
                    .filter(|chunk| chunk.world == frozen.spatial.world)
                    .copied()
                    .collect()
            }),
            retained_until_millis: retained.map_or(0, |retained| retained.retained_until),
        },
        data: super::cluster_handoff::serialize_nbt(data).into(),
        commit_token: None,
    })
}

fn previous_session_world(server: &Server, uuid: Uuid) -> Option<Arc<World>> {
    let player = server
        .get_all_players()
        .into_iter()
        .find(|player| player.gameprofile.id == uuid)?;
    let world = player.cluster_gid().and_then(|gid| {
        server
            .cluster_actor_spatial
            .load()
            .get(&gid)
            .map(|spatial| spatial.truth.local.world)
    });
    world
        .and_then(|world_id| {
            server
                .worlds
                .load()
                .iter()
                .find(|world| super::cluster_world_delta::world_id(world) == world_id)
                .cloned()
        })
        .or_else(|| Some(player.world()))
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
    encoded: ClusterChunkEncoder,
    required: Vec<Vector2<i32>>,
    chunk_need: Option<PreparedLobbyChunkNeed>,
    requested_hint: bool,
    requested_playerdata: bool,
    inventory_install_revision: Option<u64>,
    handoff_outbox: VecDeque<super::cluster_handoff::HandoffCommand>,
    playerdata_token: u64,
    image_sent: bool,
    bedrock_pack_ready: bool,
    bedrock_initialized: bool,
    bedrock_chunk_receipts: VecDeque<(Vector2<i32>, tokio::sync::oneshot::Receiver<()>)>,
    pending_skin_seed: Option<super::cluster_profile_skins::SkinSeed>,
    real_center_sent: bool,
    roster_registered_ready: Option<bool>,
    presence_revision: u64,
    invariant_failed: bool,
    #[cfg(feature = "cluster-e2e")]
    revision_mismatch_detected: bool,
    #[cfg(feature = "cluster-e2e")]
    originally_pinned: Option<pumpkin_cluster::handoff::PlayerResumeHint>,
    #[cfg(feature = "cluster-e2e")]
    retirement_barrier: Option<super::cluster_test_handoff::RetirementBarrierEvidence>,
    #[cfg(feature = "cluster-e2e")]
    retirement_barrier_observed: bool,
    held: bool,
    directory_registered: bool,
    chat_rx: tokio::sync::broadcast::Receiver<(Option<String>, TextComponent)>,
    chat_session: Option<SPlayerSession>,
    chat_session_checks: VecDeque<tokio::sync::oneshot::Receiver<Option<SPlayerSession>>>,
    session_work:
        VecDeque<tokio::sync::oneshot::Receiver<super::cluster_session_ingress::SessionOutcome>>,
    local_hint: Option<pumpkin_cluster::handoff::PlayerResumeHint>,
    retired_playerdata_bytes: Option<Arc<[u8]>>,
    local_snapshot_rx:
        Option<tokio::sync::oneshot::Receiver<super::cluster_controller::DepartureStatus>>,
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
    pub const fn uuid(&self) -> Uuid {
        self.profile.id
    }

    pub const fn session_generation(&self) -> u64 {
        self.playerdata_token
    }

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
            client, profile, config, server, false, true, None, None, None, None, None,
        )
        .await
    }

    pub(crate) async fn from_departing_player(
        player: &Player,
        server: &Arc<Server>,
        forced: bool,
        snapshot_rx: tokio::sync::oneshot::Receiver<super::cluster_controller::DepartureStatus>,
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
            Some(player.world()),
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
        retired_playerdata_bytes: Option<Arc<[u8]>>,
        local_snapshot_rx: Option<
            tokio::sync::oneshot::Receiver<super::cluster_controller::DepartureStatus>,
        >,
        lobby_world_override: Option<Arc<World>>,
    ) -> Option<Self> {
        let mut config = config;
        config.view_distance = match client.as_ref() {
            ClientPlatform::Java(_) => bounded_lobby_view_distance(config.view_distance, server),
            ClientPlatform::Bedrock(_) => std::num::NonZeroU8::new(
                config
                    .view_distance
                    .get()
                    .max(2)
                    .min(
                        server
                            .advanced_config
                            .networking
                            .bedrock
                            .view_distance
                            .get()
                            .max(2),
                    )
                    .min(pumpkin_data::chunk_view_lut::MAX_VIEW_DISTANCE),
            )
            .expect("bounded Bedrock view radius"),
        };
        let previous_world = previous_session_world(server, profile.id);
        let pending_commit = super::cluster_handoff::pending_commit(server, profile.id);
        let retirement_pending = previous_world.is_some() || pending_commit.is_some();
        let local_hint = local_hint.or_else(|| {
            (send_login && !retirement_pending)
                .then(|| {
                    server
                        .cluster_handoff
                        .load()
                        .hints
                        .get(&profile.id)
                        .cloned()
                })
                .flatten()
        });
        let worlds = server.worlds.load();
        let lobby_world = lobby_world_override
            .or_else(|| {
                pending_commit.as_ref().and_then(|(_, dimension)| {
                    worlds
                        .iter()
                        .find(|world| world.dimension.minecraft_name == dimension)
                        .cloned()
                })
            })
            .or(previous_world)
            .or_else(|| {
                local_hint.as_ref().and_then(|hint| {
                    worlds
                        .iter()
                        .find(|world| world.dimension.minecraft_name == hint.dimension)
                        .cloned()
                })
            })
            .or_else(|| worlds.first().cloned())?;
        let entity_id = Entity::reserve_ids(1);
        let playerdata_token = server
            .cluster_lobby_session_generation
            .fetch_add(1, Ordering::Relaxed);
        let gid =
            existing_gid.unwrap_or_else(|| super::cluster_presence::reserve_login_gid(server));
        let pending_skin_seed = if let ClientPlatform::Bedrock(bedrock) = client.as_ref() {
            let data = bedrock.client_data.load_full();
            let Some(skin) = data
                .as_ref()
                .as_ref()
                .and_then(|data| super::cluster_profile_skins::native_skin(data))
            else {
                client.try_kick(
                    crate::net::DisconnectReason::BadPacket,
                    &TextComponent::text("Invalid Bedrock skin"),
                );
                return None;
            };
            super::cluster_profile_skins::seed(server, gid, profile.id, skin, playerdata_token)
                .err()
        } else {
            None
        };
        let permission = super::cluster_presence::permission_for_uuid(server, profile.id);
        let source = Arc::new(LobbyCommandSender {
            session_generation: playerdata_token,
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
            matches!(client.as_ref(), ClientPlatform::Bedrock(_)),
            playerdata_token,
        );
        let command_source =
            crate::command::CommandSender::PreJoin(source.clone()).into_source(server);
        match client.as_ref() {
            ClientPlatform::Java(java) => {
                if send_login {
                    send_lobby_login(java, entity_id, &lobby_world, server);
                }
                crate::command::client_suggestions::send_java_commands_packet(
                    java,
                    &command_source,
                    &server.command_dispatcher.load(),
                );
            }
            ClientPlatform::Bedrock(bedrock) => {
                if !send_login {
                    crate::command::client_suggestions::send_bedrock_commands_for_source(
                        bedrock,
                        &command_source,
                        &server.command_dispatcher.load(),
                    );
                }
            }
        }
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
            encoded: ClusterChunkEncoder::new(),
            required: Vec::new(),
            chunk_need: None,
            requested_hint: false,
            requested_playerdata: false,
            inventory_install_revision: None,
            handoff_outbox: VecDeque::new(),
            playerdata_token,
            image_sent: false,
            bedrock_pack_ready: !send_login,
            bedrock_initialized: !send_login,
            bedrock_chunk_receipts: VecDeque::new(),
            pending_skin_seed,
            real_center_sent: false,
            roster_registered_ready: None,
            presence_revision: u64::MAX,
            invariant_failed: false,
            #[cfg(feature = "cluster-e2e")]
            revision_mismatch_detected: false,
            #[cfg(feature = "cluster-e2e")]
            originally_pinned: None,
            #[cfg(feature = "cluster-e2e")]
            retirement_barrier: None,
            #[cfg(feature = "cluster-e2e")]
            retirement_barrier_observed: false,
            held: !send_login,
            directory_registered,
            chat_rx: server.cluster_lobby_chat_tx.subscribe(),
            chat_session: None,
            chat_session_checks: VecDeque::new(),
            session_work: VecDeque::new(),
            local_hint,
            retired_playerdata_bytes,
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
        if session.local_hint.is_none()
            && session.local_snapshot_rx.is_none()
            && !retirement_pending
        {
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

    fn kick(&self, message: &TextComponent) {
        self.client
            .try_kick(crate::net::DisconnectReason::Kicked, message);
    }

    fn send_image(&mut self, server: &Server) {
        if self.image_sent {
            return;
        }
        if let ClientPlatform::Bedrock(client) = self.client.as_ref() {
            if !self.bedrock_pack_ready {
                return;
            }
            client.try_enqueue_client_packet(
                &pumpkin_protocol::bedrock::client::CSetPlayerGameType {
                    player_game_type: pumpkin_protocol::bedrock::client::GameType::Spectator,
                },
            );
            client.try_enqueue_client_packet(&pumpkin_protocol::bedrock::client::CMovePlayer::new(
                (self.entity_id as u64).into(),
                Vector3::new(
                    LOBBY_BLOCK_X as f32,
                    LOBBY_CAMERA_Y as f32,
                    LOBBY_BLOCK_Z as f32,
                ),
                90.0,
                0.0,
                0.0,
                pumpkin_protocol::bedrock::client::CMovePlayer::MODE_TELEPORT,
                false,
                0u64.into(),
                0,
                0,
                0u64.into(),
            ));
            let Some(image) = server
                .cluster_lobby_images
                .bedrock
                .get(self.lobby_world.dimension.minecraft_name)
            else {
                self.fail_invariant("Bedrock lobby image unavailable");
                return;
            };
            client.try_enqueue_client_packet(
                &pumpkin_protocol::bedrock::client::CNetworkChunkPublisherUpdate::new(
                    BlockPos::new(LOBBY_BLOCK_X, LOBBY_PLATFORM_Y, LOBBY_BLOCK_Z),
                    64,
                ),
            );
            for chunk in image.chunks.iter() {
                client.try_enqueue_packet(chunk.clone());
                if client.is_closed() {
                    self.fail_invariant("Bedrock lobby packet queue is full");
                    return;
                }
            }
            client.try_enqueue_client_packet(
                &pumpkin_protocol::bedrock::client::CPlayStatus::PlayerSpawn,
            );
            client.try_enqueue_client_packet(&pumpkin_protocol::bedrock::client::CSetTitle::new(
                pumpkin_protocol::bedrock::client::TitleType::Title,
                "Loading".into(),
                0,
                i32::MAX,
                0,
            ));
            self.image_sent = true;
            return;
        }
        #[cfg(feature = "cluster-e2e")]
        tracing::info!(uuid = %self.profile.id, micros = e2e_lobby_clock_micros(), "cluster e2e lobby first enqueue");
        #[cfg(feature = "cluster-e2e")]
        let real_chunk_wants_before = super::cluster::cluster_wanted_chunks();
        #[cfg(feature = "cluster-e2e")]
        let real_chunk_fetches_before = super::cluster::cluster_inflight_fetch();
        #[cfg(feature = "cluster-e2e")]
        let real_chunk_holders_before = server.cluster_owned_chunk_blocks.load().len();
        let version = self.java().version.load();
        if version >= JavaMinecraftVersion::V_1_20_2 {
            self.sender
                .register_batch_for_acknowledgement(self.playerdata_token as u32, Vec::new());
        }
        let java = self.java();
        let Some(image) = server
            .cluster_lobby_images
            .get(version, self.lobby_world.dimension.minecraft_name)
        else {
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
            real_players = server.worlds.load().iter().map(|world| world.players.load().len()).sum::<usize>(),
            real_chunk_wants_before,
            real_chunk_wants_after = super::cluster::cluster_wanted_chunks(),
            real_chunk_fetches_before,
            real_chunk_fetches_after = super::cluster::cluster_inflight_fetch(),
            real_chunk_holders_before,
            real_chunk_holders_after = server.cluster_owned_chunk_blocks.load().len(),
            "cluster e2e virtual lobby world footprint"
        );
        java.try_send_packet(&pumpkin_protocol::java::client::play::CTitleAnimation::new(
            0,
            i32::MAX,
            0,
        ));
        java.try_send_packet(&pumpkin_protocol::java::client::play::CTitleText::new(
            &TextComponent::text("Loading"),
        ));
        self.image_sent = true;
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
                self.kick(&TextComponent::text("Cluster handoff unavailable"));
            }
        }
    }

    #[cfg(feature = "cluster-e2e")]
    fn observe_retirement_barrier(&mut self, server: &Server) {
        if self.retirement_barrier_observed {
            return;
        }
        let old_install = server
            .get_all_players()
            .into_iter()
            .find(|player| player.gameprofile.id == self.profile.id)
            .map(|player| {
                player
                    .cluster_owner_install_generation
                    .load(Ordering::Acquire)
            });
        let pending_commit =
            super::cluster_handoff::pending_commit(server, self.profile.id).map(|(token, _)| token);
        if old_install.is_none() && pending_commit.is_none() {
            return;
        }
        self.retirement_barrier_observed = true;
        let expected_revision = server
            .cluster_handoff
            .load()
            .hints
            .get(&self.profile.id)
            .map(|hint| hint.playerdata_revision);
        self.retirement_barrier = super::cluster_test_handoff::RetirementBarrierEvidence::entered(
            self.profile.id,
            self.playerdata_token,
            old_install,
            pending_commit,
            expected_revision,
        );
    }

    fn pin_target(&mut self, server: &Server) {
        if self.handoff.hint().is_some()
            || self.local_snapshot_rx.is_some()
            || self.local_commit_token.is_some()
        {
            return;
        }
        if previous_session_world(server, self.profile.id).is_some() {
            #[cfg(feature = "cluster-e2e")]
            self.observe_retirement_barrier(server);
            return;
        }
        if let Some((token, _)) = super::cluster_handoff::pending_commit(server, self.profile.id) {
            #[cfg(feature = "cluster-e2e")]
            self.observe_retirement_barrier(server);
            if server
                .cluster_handoff
                .load()
                .commit_outcomes
                .get(&(self.profile.id, token))
                .is_some_and(|outcome| !outcome.accepted)
            {
                tracing::error!(uuid = %self.profile.id, session = self.playerdata_token, token, "cluster lobby prior retirement commit rejected");
                self.invariant_failed = true;
                self.kick(&TextComponent::text("Previous playerdata commit rejected"));
            }
            return;
        }
        let snapshot = server.cluster_handoff.load();
        if snapshot.failed_hints.contains(&self.profile.id) {
            self.invariant_failed = true;
            self.kick(&TextComponent::text("Resume target identity mismatch"));
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
        if !(2..=pumpkin_data::chunk_view_lut::MAX_VIEW_DISTANCE).contains(&hint.view_radius) {
            self.fail_invariant("Invalid resume view radius");
            return;
        }
        if hint.position.iter().any(|value| !value.is_finite())
            || hint.rotation.iter().any(|value| !value.is_finite())
        {
            self.fail_invariant("Invalid resume position");
            return;
        }
        let Some(dimension) = pumpkin_data::dimension::Dimension::from_name(&hint.dimension) else {
            if !self.invariant_failed {
                self.invariant_failed = true;
                self.kick(&TextComponent::text("Resume dimension unavailable"));
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
                self.kick(&TextComponent::text("Resume world unavailable"));
            }
            return;
        };
        self.target_world = Some(target_world);
        if self.handoff.pin(hint.clone()).is_err() {
            return;
        }
        #[cfg(feature = "cluster-e2e")]
        if let Some(evidence) = self.retirement_barrier.take() {
            evidence.released(hint.playerdata_revision, &hint.dimension, hint.position);
        }
        #[cfg(feature = "cluster-e2e")]
        if self.originally_pinned.is_none() {
            self.originally_pinned = Some(hint.clone());
        }
        if hint.dimension != self.lobby_world.dimension.minecraft_name {
            tracing::error!(uuid = %self.profile.id, session = self.playerdata_token, revision = hint.playerdata_revision, login_dimension = self.lobby_world.dimension.minecraft_name, target_dimension = %hint.dimension, "cluster lobby target requires forbidden dimension transition");
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
        for position in &self.required {
            self.sender.enqueue_chunk(*position);
        }
    }

    pub fn handle_packet(&mut self, packet: &RawPacket, server: &Arc<Server>) {
        if matches!(self.client.as_ref(), ClientPlatform::Bedrock(_)) {
            self.handle_bedrock_packet(packet, server);
            return;
        }
        let version = self.java().version.load();
        match self.java().dispatch_common_session_packet(None, packet) {
            Ok(true) => return,
            Ok(false) => {}
            Err(_) => {
                self.fail_invariant("Malformed common lobby packet");
                return;
            }
        }
        use crate::net::java::play::session::PlaySessionPacket;
        let session_packet = match crate::net::java::play::session::decode(packet, version) {
            Ok(packet) => packet,
            Err(_) => {
                self.fail_invariant("Malformed lobby session packet");
                return;
            }
        };
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
            if let Some(PlaySessionPacket::Command(packet)) = session_packet {
                self.submit_command(
                    server,
                    packet.command.strip_prefix('/').unwrap_or(packet.command),
                );
            }
        } else if packet.id == SChatCommandSigned::to_id(version) {
            if let Some(PlaySessionPacket::SignedCommand(packet)) = session_packet {
                self.submit_command(server, packet.strip_prefix('/').unwrap_or(packet));
            }
        } else if packet.id == SChatMessage::to_id(version) {
            if let Some(PlaySessionPacket::Chat(packet)) = session_packet {
                if let Some(command) = packet.message.strip_prefix('/') {
                    self.submit_command(server, command);
                } else {
                    let received_at = std::time::Instant::now();
                    let staged = if server.basic_config.allow_chat_reports {
                        super::cluster_session_ingress::StagedSessionInput::SecureJava {
                            input: super::cluster_secure_chat::SignedInput {
                                body: packet.message.to_owned(),
                                timestamp: packet.timestamp,
                                salt: packet.salt,
                                signature: packet.signature.map(Into::into),
                                message_count: packet.message_count.0,
                                acknowledged: packet.acknowledged.into(),
                                checksum: packet.checksum,
                            },
                            received_at,
                        }
                    } else {
                        super::cluster_session_ingress::StagedSessionInput::Java {
                            body: packet.message.to_owned(),
                            received_at,
                        }
                    };
                    self.submit_session_work(server, staged);
                }
            }
        } else if packet.id == SCommandSuggestion::to_id(version) {
            if let Some(PlaySessionPacket::Suggestions(packet)) = session_packet {
                self.suggest(&packet, server);
            }
        } else if packet.id == SClientInformationPlay::to_id(version)
            && let Some(PlaySessionPacket::Settings(packet)) = session_packet
        {
            self.update_config(&packet, server);
        } else if packet.id == SPlayerSession::to_id(version) {
            if let Some(PlaySessionPacket::Session(packet)) = session_packet {
                if self.chat_session_checks.len() >= 4 {
                    self.fail_invariant("Too many pending chat sessions");
                    return;
                }
                if let Some(result) = super::cluster_session_ingress::submit_lobby_session(
                    server,
                    self.source.clone(),
                    packet,
                ) {
                    self.chat_session_checks.push_back(result);
                } else {
                    self.fail_invariant("Chat session queue is full");
                }
            }
        } else if packet.id == SCustomPayload::to_id(version) {
            if let Some(PlaySessionPacket::Payload(packet)) = session_packet {
                let staged = super::cluster_session_ingress::StagedSessionInput::Payload {
                    channel: packet.channel.to_string(),
                    data: Bytes::copy_from_slice(packet.data),
                    received_at: std::time::Instant::now(),
                };
                if !self.submit_session_work(server, staged) {
                    return;
                }
            }
        } else if packet.id == SPlayResourcePack::to_id(version) {
            if let Some(PlaySessionPacket::ResourcePack(packet)) = session_packet {
                let staged = super::cluster_session_ingress::StagedSessionInput::ResourcePack {
                    uuid: packet.uuid,
                    result: packet.result,
                    received_at: std::time::Instant::now(),
                };
                self.submit_session_work(server, staged);
            }
        } else if packet.id == SPlayPong::to_id(version) {
            let _ = session_packet;
        } else if packet.id == SChatAck::to_id(version) {
            if server.basic_config.allow_chat_reports {
                if let Some(PlaySessionPacket::ChatAck(packet)) = session_packet {
                    let staged = super::cluster_session_ingress::StagedSessionInput::SecureAck {
                        offset: packet.offset.0,
                        received_at: std::time::Instant::now(),
                    };
                    self.submit_session_work(server, staged);
                }
            }
        } else if packet.id
            == pumpkin_protocol::java::server::play::SCustomClickAction::to_id(version)
            && let Some(PlaySessionPacket::Dialog(packet)) = session_packet
        {
            let staged = super::cluster_session_ingress::StagedSessionInput::Dialog {
                action_id: packet.action_id.to_string(),
                payload: packet.payload.map(Bytes::copy_from_slice),
                received_at: std::time::Instant::now(),
            };
            self.submit_session_work(server, staged);
        }
    }

    fn handle_bedrock_packet(&mut self, packet: &RawPacket, server: &Arc<Server>) {
        use pumpkin_protocol::bedrock::server::{
            command_request::SCommandRequest, request_chunk_radius::SRequestChunkRadius,
            resource_pack_client_response::SResourcePackClientResponse, text::SText,
        };
        use pumpkin_protocol::packet::Packet;
        use pumpkin_protocol::serial::PacketRead;
        let mut payload = &packet.payload[..];
        let input = match packet.id {
            pumpkin_protocol::bedrock::server::SSetLocalPlayerAsInitialized::PACKET_ID => {
                let Ok(initialized) =
                    pumpkin_protocol::bedrock::server::SSetLocalPlayerAsInitialized::read(
                        &mut payload,
                    )
                else {
                    self.fail_invariant("Malformed lobby initialized packet");
                    return;
                };
                if initialized.player_id.0 != self.entity_id as u64 {
                    self.fail_invariant("Lobby initialized identity mismatch");
                    return;
                }
                self.bedrock_initialized = true;
                None
            }
            pumpkin_protocol::bedrock::server::SPlayerAuthInput::PACKET_ID => {
                if !self.directory_registered {
                    return;
                }
                if let ClientPlatform::Bedrock(client) = self.client.as_ref() {
                    client.try_enqueue_client_packet(
                        &pumpkin_protocol::bedrock::client::CMovePlayer::new(
                            (self.entity_id as u64).into(),
                            Vector3::new(
                                LOBBY_BLOCK_X as f32,
                                LOBBY_CAMERA_Y as f32,
                                LOBBY_BLOCK_Z as f32,
                            ),
                            90.0,
                            0.0,
                            0.0,
                            pumpkin_protocol::bedrock::client::CMovePlayer::MODE_RESET,
                            false,
                            0u64.into(),
                            0,
                            0,
                            0u64.into(),
                        ),
                    );
                }
                None
            }
            SResourcePackClientResponse::PACKET_ID => {
                let Ok(response) = SResourcePackClientResponse::read(&mut payload) else {
                    self.fail_invariant("Malformed resource pack response");
                    return;
                };
                let ClientPlatform::Bedrock(client) = self.client.as_ref() else {
                    return;
                };
                match response.response {
                    SResourcePackClientResponse::STATUS_HAVE_ALL_PACKS => {
                        use pumpkin_protocol::bedrock::client::{
                            CResourcePackStackPacket, PackInstanceId,
                        };
                        let config = &server.advanced_config.resource_pack.bedrock;
                        client.try_enqueue_client_packet(&CResourcePackStackPacket {
                            texture_pack_required: config.force,
                            texture_pack_list: if config.enabled {
                                config
                                    .packs
                                    .iter()
                                    .map(|pack| PackInstanceId {
                                        pack_id: pack.uuid.to_string(),
                                        version: pack.version.clone(),
                                        sub_pack_name: String::new(),
                                    })
                                    .collect()
                            } else {
                                Vec::new()
                            },
                            base_game_version: pumpkin_world::CURRENT_BEDROCK_MC_VERSION
                                .to_string(),
                            experiments:
                                pumpkin_protocol::bedrock::client::start_game::Experiments {
                                    toggles: Vec::new(),
                                    experiments_ever_toggled: false,
                                },
                            include_editor_packs: false,
                        });
                    }
                    SResourcePackClientResponse::STATUS_COMPLETED if !self.bedrock_pack_ready => {
                        super::cluster_bedrock_lobby::start(
                            client,
                            self.entity_id,
                            server,
                            &self.lobby_world.dimension,
                        );
                        let source = crate::command::CommandSender::PreJoin(self.source.clone())
                            .into_source(server);
                        crate::command::client_suggestions::send_bedrock_commands_for_source(
                            client,
                            &source,
                            &server.command_dispatcher.load(),
                        );
                        self.bedrock_pack_ready = true;
                        self.send_image(server);
                    }
                    SResourcePackClientResponse::STATUS_COMPLETED => {}
                    _ => self.fail_invariant("Resource pack negotiation failed"),
                }
                None
            }
            SCommandRequest::PACKET_ID => match SCommandRequest::read(&mut payload) {
                Ok(command) => Some(
                    super::cluster_session_ingress::StagedSessionInput::Command {
                        command: command
                            .command
                            .strip_prefix('/')
                            .unwrap_or(&command.command)
                            .to_string(),
                        received_at: std::time::Instant::now(),
                    },
                ),
                Err(_) => {
                    self.fail_invariant("Malformed lobby command");
                    return;
                }
            },
            SText::PACKET_ID => match SText::read(&mut payload) {
                Ok(text) => Some(
                    super::cluster_session_ingress::StagedSessionInput::Bedrock {
                        body: text.message.into_owned(),
                        xuid: text.xuid.into_owned(),
                        filtered: text.filtered_message.map(std::borrow::Cow::into_owned),
                        received_at: std::time::Instant::now(),
                    },
                ),
                Err(_) => {
                    self.fail_invariant("Malformed lobby chat");
                    return;
                }
            },
            SRequestChunkRadius::PACKET_ID => {
                let Ok(radius) = SRequestChunkRadius::read(&mut payload) else {
                    self.fail_invariant("Malformed lobby view radius");
                    return;
                };
                if radius.chunk_radius.0 < 1 {
                    self.fail_invariant("Invalid view radius");
                    return;
                }
                let maximum = server
                    .advanced_config
                    .networking
                    .bedrock
                    .view_distance
                    .get()
                    .max(2)
                    .min(pumpkin_data::chunk_view_lut::MAX_VIEW_DISTANCE);
                let radius = radius.chunk_radius.0.clamp(2, i32::from(maximum)) as u8;
                self.config.view_distance =
                    std::num::NonZeroU8::new(radius).expect("validated lobby radius");
                if let ClientPlatform::Bedrock(client) = self.client.as_ref() {
                    client.try_enqueue_client_packet(
                        &pumpkin_protocol::bedrock::client::CChunkRadiusUpdated {
                            chunk_radius: VarInt(i32::from(radius)),
                        },
                    );
                }
                None
            }
            _ => None,
        };
        if let Some(input) = input {
            self.submit_session_work(server, input);
        }
    }

    fn submit_command(&mut self, server: &Arc<Server>, command: &str) {
        self.submit_session_work(
            server,
            super::cluster_session_ingress::StagedSessionInput::Command {
                command: command.to_string(),
                received_at: std::time::Instant::now(),
            },
        );
    }

    fn submit_session_work(
        &mut self,
        server: &Server,
        staged: super::cluster_session_ingress::StagedSessionInput,
    ) -> bool {
        if self.session_work.len() >= 128 {
            self.fail_invariant("Too many pending lobby session callbacks");
            return false;
        }
        let Some(receipt) =
            super::cluster_session_ingress::submit_lobby_work(server, self.source.clone(), staged)
        else {
            self.fail_invariant("Lobby session callback queue is full");
            return false;
        };
        self.session_work.push_back(receipt);
        true
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
        let Ok(config) = crate::net::java::play::session::player_config(packet, server) else {
            self.fail_invariant("Invalid client settings");
            return;
        };
        self.config = config;
        self.source = Arc::new(LobbyCommandSender {
            session_generation: self.playerdata_token,
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
            matches!(self.client.as_ref(), ClientPlatform::Bedrock(_)),
            self.playerdata_token,
        );
    }

    pub fn tick(&mut self, server: &Arc<Server>) -> Option<LobbyPromotion> {
        self.poll_virtual_events(server);
        if self.invariant_failed {
            return None;
        }
        if !self.image_sent {
            return None;
        }
        if matches!(self.client.as_ref(), ClientPlatform::Bedrock(_)) && !self.bedrock_initialized {
            return None;
        }
        #[cfg(feature = "cluster-e2e")]
        if !self.stall_reported && self.created_at.elapsed() >= std::time::Duration::from_secs(30) {
            self.stall_reported = true;
            let handoff_snapshot = server.cluster_handoff.load();
            let held = self.target_world.as_ref().map_or(0, |world| {
                self.required
                    .iter()
                    .filter(|position| {
                        server
                            .cluster_chunk_presentations
                            .load()
                            .get(&pumpkin_cluster::protocol::ChunkKey::new(
                                super::cluster_world_delta::world_id(world),
                                position.x,
                                position.y,
                            ))
                            .is_some()
                    })
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
                retired_playerdata_bytes = self.retired_playerdata_bytes.is_some(),
                held = self.held,
                released = self.source.release.load(Ordering::Acquire),
                directory_registered = self.directory_registered,
                inventory_install_seen = server.cluster_inventory_handoff_installed.load().contains_key(&(self.gid, self.playerdata_token)),
                join_allowed = super::cluster::cluster_player_join_allowed(),
                "cluster e2e lobby handoff exceeded 30 seconds"
            );
        }
        if let Some(receiver) = self.local_snapshot_rx.as_mut() {
            match receiver.try_recv() {
                Ok(super::cluster_controller::DepartureStatus::Retired(snapshot)) => {
                    self.local_hint = Some(snapshot.hint);
                    self.retired_playerdata_bytes = Some(snapshot.data);
                    self.local_commit_token = snapshot.commit_token;
                    self.local_snapshot_rx = None;
                }
                Ok(_) => {
                    tracing::error!(uuid = %self.profile.id, session = self.playerdata_token, "cluster local departure did not retire");
                    self.fail_invariant("Local lobby handoff failed");
                    return None;
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
                self.handoff_outbox.push_back(
                    super::cluster_handoff::HandoffCommand::ReleaseCommitOutcome {
                        uuid: self.profile.id,
                        token,
                    },
                );
            }
        }
        if !self.directory_registered {
            self.directory_registered = server.cluster_lobby_directory.register(&self.source);
        }
        self.flush_handoff_outbox(server);
        self.pin_target(server);
        if self
            .handoff
            .hint()
            .is_some_and(|hint| hint.dimension != self.lobby_world.dimension.minecraft_name)
        {
            self.show_progress(0, 0);
            return None;
        }
        if self.chunk_need.is_none() && self.handoff.hint().is_some() {
            let route = server.cluster_chunk_watch_route.load_full()?;
            let retirement = route.as_ref().clone().try_reserve_owned().ok()?;
            let need = super::cluster::try_require_lobby_chunks(
                server,
                self.target_world.as_ref()?,
                &self.required,
                self.playerdata_token,
            )?;
            self.chunk_need = Some(PreparedLobbyChunkNeed {
                actor: self.gid,
                install_generation: self.playerdata_token,
                need: Some(need),
                delivered: Vec::new(),
                retirement: Some(retirement),
            });
        }
        let hint = self.handoff.hint()?.clone();
        let world = self.target_world.as_ref()?;
        let center = Vector2::new(
            hint.position[0].floor() as i32 >> 4,
            hint.position[2].floor() as i32 >> 4,
        );
        let peer = self
            .required
            .iter()
            .filter(|position| {
                server
                    .cluster_chunk_presentations
                    .load()
                    .get(&pumpkin_cluster::protocol::ChunkKey::new(
                        super::cluster_world_delta::world_id(world),
                        position.x,
                        position.y,
                    ))
                    .is_some()
            })
            .count();
        while let Some((position, mut receipt)) = self.bedrock_chunk_receipts.pop_front() {
            match receipt.try_recv() {
                Ok(()) => self.sender.mark_delivered(&[position]),
                Err(tokio::sync::oneshot::error::TryRecvError::Empty) => {
                    self.bedrock_chunk_receipts.push_front((position, receipt));
                    break;
                }
                Err(tokio::sync::oneshot::error::TryRecvError::Closed) => {
                    self.fail_invariant("Bedrock real chunk transport failed");
                    return None;
                }
            }
        }
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
        let target_version = match self.client.as_ref() {
            ClientPlatform::Java(java) => java.version.load(),
            ClientPlatform::Bedrock(_) => JavaMinecraftVersion::Unknown,
        };
        let presentations = server.cluster_chunk_presentations.load_full();
        let encoding = if matches!(self.client.as_ref(), ClientPlatform::Bedrock(_)) {
            let dimension = match world.dimension.minecraft_name {
                "minecraft:the_nether" => 1,
                "minecraft:the_end" => 2,
                _ => 0,
            };
            self.encoded.advance_bedrock(
                &presentations,
                super::cluster_world_delta::world_id(world),
                &self.sender.pending_chunks,
                dimension,
            )
        } else {
            self.encoded.advance(
                &presentations,
                super::cluster_world_delta::world_id(world),
                &self.sender.pending_chunks,
                target_version,
            )
        };
        if let Err(error) = encoding {
            self.fail_invariant(&error);
            return None;
        }
        while let Some((batch, encoded)) = self.sender.prepare_encoded_batch(
            &mut self.encoded,
            center,
            self.playerdata_token as u32,
            target_version,
        ) {
            if !self.real_center_sent {
                match self.client.as_ref() {
                    ClientPlatform::Java(java) => java.try_send_packet(&CCenterChunk {
                        chunk_x: VarInt(center.x),
                        chunk_z: VarInt(center.y),
                    }),
                    ClientPlatform::Bedrock(bedrock) => bedrock.try_enqueue_client_packet(
                        &pumpkin_protocol::bedrock::client::CNetworkChunkPublisherUpdate::new(
                            BlockPos::new(
                                hint.position[0].floor() as i32,
                                hint.position[1].floor() as i32,
                                hint.position[2].floor() as i32,
                            ),
                            u32::from(hint.view_radius) * 16,
                        ),
                    ),
                }
                self.real_center_sent = true;
            }
            #[cfg(feature = "cluster-e2e")]
            if !self.first_real_batch_reported {
                self.first_real_batch_reported = true;
                tracing::debug!(uuid = %self.profile.id, peer, required = self.required.len(), batch_chunks = encoded.len(), "cluster e2e lobby first real batch enqueued");
            }
            if let ClientPlatform::Bedrock(client) = self.client.as_ref() {
                let mut receipts = Vec::with_capacity(encoded.len());
                for chunk in &encoded {
                    match client.try_enqueue_owned_chunk(&chunk.payload) {
                        Ok(receipt) => receipts.push(receipt),
                        Err(error) => {
                            self.fail_invariant(&error);
                            return None;
                        }
                    }
                }
                let dispatched = self.sender.commit_encoded_bedrock_batch(
                    &batch,
                    &encoded,
                    self.playerdata_token as u32,
                );
                self.bedrock_chunk_receipts
                    .extend(dispatched.into_iter().zip(receipts));
            } else {
                self.sender.commit_batch(
                    &batch,
                    &encoded,
                    &self.client,
                    self.playerdata_token as u32,
                );
            }
        }
        if peer != self.required.len() || !super::cluster::cluster_player_join_allowed() {
            return None;
        }
        if self.retired_playerdata_bytes.is_none() && !self.requested_playerdata {
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
            self.kick(&TextComponent::text("Playerdata identity mismatch"));
            return None;
        }
        let data = if let Some(data) = self.retired_playerdata_bytes.as_ref() {
            let Some(data) = super::cluster_handoff::deserialize_retired_playerdata(data, &hint)
            else {
                self.fail_invariant("Retired playerdata decode failed");
                return None;
            };
            Some(data)
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
                #[cfg(feature = "cluster-e2e")]
                {
                    self.revision_mismatch_detected = true;
                }
                tracing::warn!(uuid = %self.profile.id, session = self.playerdata_token, expected, received, target_dimension = %hint.dimension, target_position = ?hint.position, "playerdata handoff revision mismatch");
                self.kick(&TextComponent::text("Playerdata revision mismatch"));
                return None;
            }
            super::cluster_handoff::deserialize_nbt(&data.data)
        } else if let Some((token, revision)) = snapshot.missing_playerdata.get(&self.profile.id) {
            if *token != self.playerdata_token {
                return None;
            }
            if *revision != hint.playerdata_revision {
                self.invariant_failed = true;
                #[cfg(feature = "cluster-e2e")]
                {
                    self.revision_mismatch_detected = true;
                }
                tracing::warn!(
                    uuid = %self.profile.id,
                    expected = hint.playerdata_revision,
                    received = *revision,
                    session = self.playerdata_token,
                    target_dimension = %hint.dimension,
                    target_position = ?hint.position,
                    "unavailable playerdata revision mismatch"
                );
                self.kick(&TextComponent::text("Playerdata revision mismatch"));
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
                self.profile.id,
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
        let Some(status) = installed.get(&(self.gid, self.playerdata_token)) else {
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
        if !self.chat_session_checks.is_empty()
            || !self.session_work.is_empty()
            || !self.handoff_outbox.is_empty()
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
            sent_presentations: self.encoded.take_sent_presentations(),
            chat_session: self.chat_session.take(),
            reentered: self.held,
        })
    }

    pub fn poll_virtual_events(&mut self, server: &Arc<Server>) {
        if let Some(seed) = self.pending_skin_seed.take() {
            self.pending_skin_seed = super::cluster_profile_skins::seed(
                server,
                seed.gid,
                seed.uuid,
                seed.skin,
                seed.session,
            )
            .err();
        }
        while let Some(check) = self.chat_session_checks.front_mut() {
            match check.try_recv() {
                Ok(Some(session)) => {
                    self.chat_session = Some(session);
                    self.chat_session_checks.pop_front();
                }
                Ok(None) | Err(tokio::sync::oneshot::error::TryRecvError::Closed) => {
                    self.fail_invariant("Invalid chat session");
                    return;
                }
                Err(tokio::sync::oneshot::error::TryRecvError::Empty) => break,
            }
        }
        while let Some(receipt) = self.session_work.front_mut() {
            match receipt.try_recv() {
                Ok(
                    super::cluster_session_ingress::SessionOutcome::Completed
                    | super::cluster_session_ingress::SessionOutcome::Rejected,
                ) => {
                    self.session_work.pop_front();
                }
                Err(tokio::sync::oneshot::error::TryRecvError::Empty) => break,
                Ok(super::cluster_session_ingress::SessionOutcome::Failed)
                | Err(tokio::sync::oneshot::error::TryRecvError::Closed) => {
                    self.fail_invariant("Lobby session callback did not complete");
                    return;
                }
            }
        }
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

    pub(crate) fn session_work_complete(&self) -> bool {
        !self.invariant_failed
            && self.session_work.is_empty()
            && self.chat_session_checks.is_empty()
    }

    pub fn finish_promotion(&mut self, server: &Server) -> bool {
        if !self.directory_registered
            || !server.cluster_lobby_directory.unregister(
                self.gid,
                *self.profile.id.as_bytes(),
                self.playerdata_token,
            )
        {
            return false;
        }
        self.directory_registered = false;
        true
    }

    pub fn close(&self, server: &Server) {
        #[cfg(feature = "cluster-e2e")]
        if self.revision_mismatch_detected {
            let worlds = server.worlds.load();
            let player_registered = worlds.iter().any(|world| {
                world
                    .players
                    .load()
                    .iter()
                    .any(|player| player.gameprofile.id == self.profile.id)
            });
            let replica_registered = worlds.iter().any(|world| {
                world
                    .entities
                    .load()
                    .iter()
                    .any(|entity| entity.get_entity().entity_uuid == self.profile.id)
            });
            let inventory_installed =
                server
                    .cluster_inventory_snapshots
                    .load()
                    .keys()
                    .any(|target| match target {
                        pumpkin_cluster::inventory::InventoryTarget::Player(gid) => {
                            *gid == self.gid
                        }
                        pumpkin_cluster::inventory::InventoryTarget::LeasedPlayer {
                            key,
                            player,
                            ..
                        }
                        | pumpkin_cluster::inventory::InventoryTarget::LeasedVirtual {
                            key,
                            player,
                            ..
                        } => *key == self.profile.id && *player == self.gid,
                        _ => false,
                    });
            let target_rechosen = self.originally_pinned.as_ref() != self.handoff.hint();
            tracing::debug!(uuid = %self.profile.id, player_registered, inventory_installed,
                replica_registered, target_rechosen, "cluster e2e revision mismatch cleanup");
        }
        if self.inventory_install_revision.is_some() {
            super::cluster_invsee::cancel_handoff_inventory_install(
                server,
                self.gid,
                self.playerdata_token,
            );
        }
        if self.directory_registered {
            let _ = server.cluster_lobby_directory.unregister(
                self.gid,
                *self.profile.id.as_bytes(),
                self.playerdata_token,
            );
        }
    }

    fn fail_invariant(&mut self, message: &str) {
        if self.invariant_failed {
            return;
        }
        self.invariant_failed = true;
        tracing::warn!(uuid = %self.profile.id, reason = message, "cluster lobby handoff invariant failed");
        self.kick(&TextComponent::text(message.to_string()));
    }

    fn sync_roster(&mut self, server: &Arc<Server>) {
        let snapshot = server.cluster_presence.snapshot();
        if snapshot.revision == self.presence_revision
            && matches!(self.client.as_ref(), ClientPlatform::Java(_))
        {
            return;
        }
        self.presence_revision = snapshot.revision;
        let permission = snapshot.permission_for_uuid(self.profile.id.as_bytes());
        if permission != self.source.permission {
            self.source = Arc::new(LobbyCommandSender {
                session_generation: self.playerdata_token,
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
            match self.client.as_ref() {
                ClientPlatform::Java(java) => {
                    crate::command::client_suggestions::send_java_commands_packet(
                        java,
                        &command_source,
                        &server.command_dispatcher.load(),
                    )
                }
                ClientPlatform::Bedrock(bedrock) => {
                    crate::command::client_suggestions::send_bedrock_commands_for_source(
                        bedrock,
                        &command_source,
                        &server.command_dispatcher.load(),
                    )
                }
            }
        }
        let ready =
            matches!(self.client.as_ref(), ClientPlatform::Java(_)) || self.bedrock_pack_ready;
        if self.roster_registered_ready != Some(ready) {
            if super::cluster_roster::register(
                server,
                self.client.clone(),
                self.gid,
                *self.profile.id.as_bytes(),
                self.playerdata_token,
                ready,
            ) {
                self.roster_registered_ready = Some(ready);
            } else {
                self.presence_revision = u64::MAX;
            }
        }
    }

    fn show_progress(&self, peer: usize, received: usize) {
        let total = self.required.len().max(1);
        let peer = peer.saturating_mul(100).saturating_div(total);
        let received = received.saturating_mul(100).saturating_div(total);
        if let ClientPlatform::Bedrock(client) = self.client.as_ref() {
            use pumpkin_protocol::bedrock::client::{CSetTitle, TitleType};
            client.try_enqueue_client_packet(&CSetTitle::new(
                TitleType::Title,
                "Loading".into(),
                0,
                LOBBY_TITLE_STAY_TICKS,
                5,
            ));
            client.try_enqueue_client_packet(&CSetTitle::new(
                TitleType::Subtitle,
                format!("§8{peer}% §f{received}%"),
                0,
                LOBBY_TITLE_STAY_TICKS,
                5,
            ));
            return;
        }
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
const LOBBY_CHUNK_BLOCKS: i32 = 16;
/// Fake chunk radius around the portal chunk (9x9, 4 out each side).
const LOBBY_CHUNK_RADIUS: i32 = 4;
/// Frozen noon shown while waiting; rate `0.0` keeps lobby time paused.
const LOBBY_FROZEN_DAYTIME: i64 = 6000;
/// Title stays `20` ticks so it auto-hides one second after updates stop.
const LOBBY_TITLE_STAY_TICKS: i32 = 20;

pub(crate) fn lobby_center_chunk() -> Vector2<i32> {
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
pub(crate) struct LobbyImages {
    images: HashMap<&'static str, Box<[Option<LobbyStatic>]>>,
    pub(crate) bedrock: HashMap<&'static str, super::cluster_bedrock_lobby::BedrockLobbyImage>,
}
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

impl LobbyImages {
    pub(crate) fn new(
        enabled: bool,
        compression: Option<(usize, u32)>,
        bedrock_compression: (usize, u32),
    ) -> Self {
        let mut images = HashMap::new();
        let mut bedrock = HashMap::new();
        if enabled {
            for dimension in [
                &pumpkin_data::dimension::Dimension::OVERWORLD,
                &pumpkin_data::dimension::Dimension::OVERWORLD_CAVES,
                &pumpkin_data::dimension::Dimension::THE_NETHER,
                &pumpkin_data::dimension::Dimension::THE_END,
            ] {
                let mut versions = vec![None; LOBBY_IMAGE_SLOTS].into_boxed_slice();
                for version in LOBBY_IMAGE_VERSIONS {
                    versions[version as usize] =
                        Some(lobby_build_dimension_image(version, compression, dimension));
                }
                images.insert(dimension.minecraft_name, versions);
                bedrock.insert(
                    dimension.minecraft_name,
                    super::cluster_bedrock_lobby::BedrockLobbyImage::for_dimension(
                        bedrock_compression,
                        dimension,
                    ),
                );
            }
        }
        Self { images, bedrock }
    }

    fn get(&self, version: JavaMinecraftVersion, dimension: &str) -> Option<&LobbyStatic> {
        self.images.get(dimension)?.get(version as usize)?.as_ref()
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
fn lobby_build_dimension_image(
    version: JavaMinecraftVersion,
    compression: Option<(usize, u32)>,
    dimension: &pumpkin_data::dimension::Dimension,
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
            dimension.minecraft_name.to_owned(),
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
            let (payload, light) =
                lobby_fake_chunk_packet(center.x + dx, center.y + dz, version, dimension);
            tail.push(payload);
            if let Some(light) = light {
                tail.push(light);
            }
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
fn lobby_fake_chunk_packet(
    x: i32,
    z: i32,
    version: JavaMinecraftVersion,
    dimension: &pumpkin_data::dimension::Dimension,
) -> (Bytes, Option<Bytes>) {
    use pumpkin_world::chunk::palette::{BiomePalette, BlockPalette};
    let center = lobby_center_chunk();
    let portal = x == center.x && z == center.y;
    let section_count = (dimension.height / 16) as usize;
    let mut sections = vec![BlockPalette::default(); section_count];
    let mut entities = Vec::new();
    if portal {
        let y = (LOBBY_PLATFORM_Y - dimension.min_y) as usize;
        for dz in 0..16 {
            for dx in 0..16 {
                sections[y / 16].set(dx, y % 16, dz, Block::END_PORTAL.default_state.id);
                let position =
                    BlockPos::new((x << 4) + dx as i32, LOBBY_PLATFORM_Y, (z << 4) + dz as i32);
                let bytes = pumpkin_nbt::Nbt::from(EndPortalBlockEntity::create_nbt(position))
                    .write_unnamed()
                    .to_vec();
                entities.push((
                    pumpkin_cluster::protocol::BlockPos {
                        x: position.0.x,
                        y: position.0.y,
                        z: position.0.z,
                    },
                    pumpkin_cluster::canonical_action::Cell {
                        revision: 0,
                        value: pumpkin_cluster::canonical_action::Value::BlockEntity(bytes),
                    },
                ));
            }
        }
    }
    let blocks = super::cluster_chunk_state::OwnedChunkBlocks::from_palettes(
        dimension.min_y,
        sections.into_boxed_slice(),
        vec![BiomePalette::default(); section_count].into_boxed_slice(),
    );
    let sky = if portal && dimension.has_skylight {
        255
    } else {
        0
    };
    let lighting = pumpkin_world::lighting::owned::OwnedChunkLighting {
        block: (0..section_count).map(|_| Box::new([0; 2048])).collect(),
        sky: (0..section_count).map(|_| Box::new([sky; 2048])).collect(),
    };
    crate::net::java::chunk_data::owned::encode(
        pumpkin_cluster::protocol::ChunkKey::new(
            pumpkin_cluster::protocol::WorldId::new(0, 0),
            x,
            z,
        ),
        &pumpkin_nbt::compound::NbtCompound::new(),
        &blocks,
        &entities,
        Some(&lighting),
        version,
    )
    .expect("virtual lobby packet image")
}

#[cfg(test)]
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
    match player.client.as_ref() {
        ClientPlatform::Java(client) => client.request_cluster_lobby(forced),
        ClientPlatform::Bedrock(client) => client.request_cluster_lobby(forced),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn cached_lobby_frames_decode_with_and_without_compression() {
        for dimension in [
            &pumpkin_data::dimension::Dimension::OVERWORLD,
            &pumpkin_data::dimension::Dimension::THE_NETHER,
            &pumpkin_data::dimension::Dimension::THE_END,
        ] {
            for (version, compression) in [
                (JavaMinecraftVersion::V_1_21_11, None),
                (JavaMinecraftVersion::V_1_21_11, Some((256, 4))),
                (JavaMinecraftVersion::V_26_2, None),
                (JavaMinecraftVersion::V_26_2, Some((256, 4))),
            ] {
                let image = lobby_build_dimension_image(version, compression, dimension);
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
                let mut decoder = pumpkin_protocol::java::packet_decoder::TCPNetworkDecoder::new(
                    stream.as_slice(),
                );
                if let Some((threshold, _)) = compression {
                    decoder.set_compression(threshold);
                }
                let mut packet_ids = Vec::with_capacity(frame_count);
                for _ in 0..frame_count {
                    let packet = decoder.get_raw_packet().await.unwrap();
                    if packet.id == CPlayerSpawnPosition::to_id(version) {
                        let spawn =
                            CPlayerSpawnPosition::read(&mut packet.payload.as_ref(), &version)
                                .unwrap();
                        assert_eq!(spawn.dimension_name, dimension.minecraft_name);
                        assert_eq!(spawn.location.0.y, 100);
                    }
                    packet_ids.push(packet.id);
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
    }

    #[test]
    fn unactivated_lobby_claim_moves_through_reserved_retirement_capacity_without_avatar() {
        let actor = pumpkin_cluster::identity::GlobalPlayerId::new(
            pumpkin_cluster::identity::ServerId(1),
            pumpkin_cluster::identity::PlayerSlot(3),
        );
        let chunk = pumpkin_cluster::protocol::ChunkKey::new(
            pumpkin_cluster::protocol::WorldId::new(0, 0),
            4,
            5,
        );
        let (need_tx, mut need_rx) = tokio::sync::mpsc::channel(1);
        let (watch_tx, mut watch_rx) = tokio::sync::mpsc::channel(1);
        let guard = PreparedLobbyChunkNeed {
            actor,
            install_generation: 42,
            need: Some(super::super::cluster::ChunkNeedLease::test_owned(
                need_tx.try_reserve_owned().unwrap(),
                50,
                vec![chunk],
            )),
            delivered: vec![chunk],
            retirement: Some(watch_tx.try_reserve_owned().unwrap()),
        };
        let cut = super::super::cluster_chunk_watch::retirement_now_millis();
        drop(guard);
        assert!(matches!(
            need_rx.try_recv(),
            Err(tokio::sync::mpsc::error::TryRecvError::Empty)
        ));
        match watch_rx.try_recv().unwrap() {
            super::super::cluster_chunk_watch::WatchCommand::RetainPrepared {
                actor: received,
                install_generation,
                need,
                delivered,
                retained_until,
            } => {
                assert_eq!(received, actor);
                assert_eq!(install_generation, 42);
                assert_eq!(delivered, vec![chunk]);
                assert_eq!(need.chunks(), &[chunk]);
                assert!(retained_until >= cut.saturating_add(30_000));
                drop(need);
            }
            _ => panic!("expected owned prepared retirement"),
        }
        assert!(
            matches!(need_rx.try_recv(), Ok(super::super::cluster::ChunkNeed::ReleaseSet { lease: 50, chunks }) if chunks == vec![chunk])
        );
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
            [1; 16],
            1,
            Arc::downgrade(&first),
            std::sync::Weak::new(),
        ));
        assert!(!directory.register_entry(
            "second".to_string(),
            second_gid,
            [2; 16],
            2,
            Arc::downgrade(&second),
            std::sync::Weak::new(),
        ));
        assert!(
            matches!(rx.try_recv(), Ok(LobbyDirectoryCommand::Register { gid, .. }) if gid == first_gid)
        );
        assert!(directory.register_entry(
            "second".to_string(),
            second_gid,
            [2; 16],
            2,
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
                uuid: [3; 16],
                session: 3,
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

pub(crate) struct ActivatedLobbyWatch {
    install_generation: u64,
    actor: pumpkin_cluster::identity::GlobalPlayerId,
    session: u64,
    release: Option<tokio::sync::mpsc::OwnedPermit<super::cluster_chunk_watch::WatchCommand>>,
}

impl ActivatedLobbyWatch {
    pub(crate) fn transfer_to_controller(mut self, player: &Player) -> Result<(), Self> {
        if let Some(release) = self.release.take() {
            if let Err(release) = player.cluster_initial_watch_release.push(release) {
                self.release = Some(release);
                return Err(self);
            }
        }
        Ok(())
    }
}

impl Drop for ActivatedLobbyWatch {
    fn drop(&mut self) {
        if let Some(release) = self.release.take() {
            release.send(super::cluster_chunk_watch::WatchCommand::Retire {
                actor: self.actor,
                session: self.session,
                install_generation: self.install_generation,
                retained: None,
            });
        }
    }
}

pub(crate) fn activate_lobby_watch(
    server: Arc<Server>,
    player: Arc<Player>,
    hint: pumpkin_cluster::handoff::PlayerResumeHint,
    sent: rustc_hash::FxHashMap<
        pumpkin_util::math::vector2::Vector2<i32>,
        Arc<super::cluster_chunk_presentation::ChunkPresentation>,
    >,
    mut need: PreparedLobbyChunkNeed,
) -> impl std::future::Future<Output = Option<ActivatedLobbyWatch>> {
    #[cfg(feature = "cluster-e2e")]
    let mut early_confirm_gate = super::cluster_test_handoff::begin(&server, &player);
    let world = need
        .need
        .as_ref()
        .and_then(|need| need.chunks().first())
        .map(|chunk| chunk.world);
    if let Some(world) = world {
        need.delivered = sent
            .keys()
            .map(|position| pumpkin_cluster::protocol::ChunkKey::new(world, position.x, position.y))
            .collect();
    }
    let mut payload = Some((sent, need));
    let mut receipt: Option<
        tokio::sync::oneshot::Receiver<
            Result<
                super::cluster_chunk_watch::WatchActivated,
                super::cluster_chunk_watch::WatchActivationError,
            >,
        >,
    > = None;
    let mut guard = None;
    std::future::poll_fn(move |_| {
        use std::task::Poll;
        if player.client.closed() {
            return Poll::Ready(None);
        }
        #[cfg(feature = "cluster-e2e")]
        if let Some(gate) = early_confirm_gate.as_mut() {
            match gate.started() {
                Ok(true) => {}
                Ok(false) => return Poll::Pending,
                Err(()) => return Poll::Ready(None),
            }
        }
        if let Some(receipt) = receipt.as_mut() {
            return match receipt.try_recv() {
                Ok(Ok(activated))
                    if guard.as_ref().is_some_and(|guard: &ActivatedLobbyWatch| {
                        guard.actor == activated.actor && guard.session == activated.session
                    }) =>
                {
                    Poll::Ready(guard.take())
                }
                Ok(Err(error)) => {
                    tracing::error!(uuid = %player.gameprofile.id, install_generation = player.cluster_owner_install_generation.load(Ordering::Acquire), ?error, "cluster lobby watch activation rejected");
                    Poll::Ready(None)
                }
                Ok(_) | Err(tokio::sync::oneshot::error::TryRecvError::Closed) => Poll::Ready(None),
                Err(tokio::sync::oneshot::error::TryRecvError::Empty) => Poll::Pending,
            };
        }
        let Some(lease) = super::cluster_world_owner::player_action_lease(&server, &player) else {
            return Poll::Pending;
        };
        let publications = server.cluster_actor_spatial.load();
        let Some(spatial) = publications.get(&lease.gid) else {
            return Poll::Pending;
        };
        let Some(route) = server.cluster_chunk_watch_route.load_full() else {
            return Poll::Pending;
        };
        let Ok(activate) = route.as_ref().clone().try_reserve_owned() else {
            return Poll::Pending;
        };
        let (sent, prepared_need) = payload.take().unwrap();
        let install_generation = prepared_need.install_generation;
        let (need, release) = prepared_need.into_activation();
        let (tx, rx) = tokio::sync::oneshot::channel();
        let world = spatial.truth.local.world;
        let effective = spatial.truth.local.frame.effective;
        activate.send(super::cluster_chunk_watch::WatchCommand::Activate(
            super::cluster_chunk_watch::WatchActivationRequest {
                actor: lease.gid,
                session: lease.incarnation,
                player: player.clone(),
                view: pumpkin_cluster::controller::ViewIntent {
                    actor: lease.gid,
                    owner_install_generation: player
                        .cluster_owner_install_generation
                        .load(Ordering::Acquire),
                    world,
                    center: pumpkin_cluster::protocol::ChunkAddr {
                        x: (hint.position[0].floor() as i32).div_euclid(16),
                        z: (hint.position[2].floor() as i32).div_euclid(16),
                    },
                    radius: std::num::NonZeroU8::new(hint.view_radius).unwrap(),
                    spectator_generates: player.cluster_initial_gamemode
                        != pumpkin_util::GameMode::Spectator as u8,
                    simulation_distance: server
                        .advanced_config
                        .networking
                        .java
                        .simulation_distance
                        .get(),
                    chunk_send_epoch: player.chunk_send_epoch.load(Ordering::Relaxed),
                    tick: effective.tick,
                },
                sent: sent
                    .into_iter()
                    .map(|(position, image)| {
                        (
                            pumpkin_cluster::protocol::ChunkKey::new(world, position.x, position.y),
                            image,
                        )
                    })
                    .collect(),
                need,
                respond: tx,
            },
        ));
        guard = Some(ActivatedLobbyWatch {
            install_generation,
            actor: lease.gid,
            session: lease.incarnation,
            release: Some(release),
        });
        receipt = Some(rx);
        Poll::Pending
    })
}

pub(crate) fn retire_failed_handoff(server: Arc<Server>, player: Arc<Player>) {
    if begin_player_departure(&server, &player, true).is_none() {
        tracing::error!(uuid = %player.gameprofile.id, "cluster failed handoff departure unavailable");
    }
}
