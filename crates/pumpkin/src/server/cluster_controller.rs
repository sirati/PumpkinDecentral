use std::collections::{HashMap, HashSet, VecDeque};
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, Ordering};

use arc_swap::ArcSwap;
use crossbeam::queue::ArrayQueue;
use pumpkin_cluster::completion::ActionKey;
use pumpkin_cluster::controller::{
    ClientClaimedState, ControllerActorIntent, ControllerActorSource, ControllerDecision,
    ControllerFilter, ControllerFrame, ControllerMovementStatistic, EffectiveControllerState,
    ViewIntent, apply_filters,
};
use pumpkin_cluster::identity::{GlobalPlayerId, PlayerSeq};
use pumpkin_cluster::player_admin::PlayerAdministrativeState;
use pumpkin_cluster::player_admin::{
    PlayerAdministrativeAction, PlayerAdministrativePublication, PlayerLivingAdministrativeState,
};
use pumpkin_cluster::protocol::ChunkKey;
use pumpkin_cluster::time::TickStamp;
use pumpkin_util::math::vector3::Vector3;
use tokio::sync::oneshot;

use super::Server;
use crate::entity::{EntityBase, player::Player};

const ADMIN_OUTSTANDING_LIMIT: usize = 4096;
const ADMIN_PUBLICATION_LIMIT: usize = 8192;
const SESSION_OUTSTANDING_LIMIT: usize = 128;

type AdministrativePublicationTarget = (
    pumpkin_cluster::identity::ServerId,
    u64,
    PlayerAdministrativePublication,
);

#[derive(Debug, Clone, Copy, PartialEq)]
pub enum ControllerPatch {
    Respawn,
    Wake,
    WakeMorning(pumpkin_cluster::world_time::MorningTransition),
    Cosmetic(super::cluster_remote_player_presentation::CosmeticChange),
    Position {
        position: [f64; 3],
        on_ground: bool,
    },
    PositionRotation {
        position: [f64; 3],
        yaw: f32,
        pitch: f32,
        on_ground: bool,
    },
    Rotation {
        yaw: f32,
        pitch: f32,
        on_ground: bool,
    },
    Ground(bool),
    Input {
        flags: u64,
        sneaking: Option<bool>,
        sprinting: Option<bool>,
    },
    Sneak(bool),
    Sprint(bool),
    Pose {
        swimming: Option<bool>,
        gliding: Option<bool>,
    },
    Vehicle {
        position: [f64; 3],
        yaw: f32,
        pitch: f32,
    },
    Paddles {
        left: bool,
        right: bool,
    },
    VehicleFlags {
        forward: i8,
        sideways: i8,
        jumping: bool,
    },
    Bedrock {
        position: [f64; 3],
        velocity: [f64; 3],
        yaw: f32,
        pitch: f32,
        on_ground: bool,
        flags: u64,
        sneaking: Option<bool>,
        sprinting: Option<bool>,
        flying: Option<bool>,
        crawling: Option<bool>,
    },
    TeleportConfirm(pumpkin_protocol::codec::var_int::VarInt),
}

#[derive(Clone, Copy)]
enum RetainedLifecycleRequest {
    Respawn,
    Wake {
        epoch: pumpkin_cluster::canonical_action::SpatialEpoch,
        sleeping: pumpkin_cluster::item_action::PlayerSleepingState,
        morning: Option<pumpkin_cluster::world_time::MorningTransition>,
    },
    Dismount {
        epoch: pumpkin_cluster::canonical_action::SpatialEpoch,
        birth: pumpkin_cluster::canonical_action::ActionRef,
        uuid: uuid::Uuid,
    },
}

#[derive(Default)]
struct ControllerOwner {
    admission_results: Option<
        tokio::sync::mpsc::Receiver<super::cluster_world_actor::ActorAdmissionInstallResult>,
    >,
    admission_outcomes:
        HashMap<GlobalPlayerId, super::cluster_world_actor::ActorAdmissionInstallResult>,
    pending_departure_requests: VecDeque<DepartureRequest>,
    game_mode_pending: HashMap<[u8; 16], pumpkin_cluster::canonical_action::LivePlayerLease>,
    game_mode_preflighted: HashSet<[u8; 16]>,
    handoff_completions: Option<tokio::sync::mpsc::Receiver<HandoffCompletion>>,
    frame_epochs: HashMap<GlobalPlayerId, pumpkin_cluster::canonical_action::SpatialEpoch>,
    awaiting_bedrock_teleport: HashSet<GlobalPlayerId>,
    projected_epochs: HashMap<GlobalPlayerId, pumpkin_cluster::canonical_action::SpatialEpoch>,
    morning_cuts: HashMap<
        GlobalPlayerId,
        (
            pumpkin_cluster::canonical_action::LivePlayerLease,
            u64,
            Option<pumpkin_cluster::world_time::MorningTransition>,
        ),
    >,
    active_mining_leases:
        HashMap<GlobalPlayerId, (pumpkin_cluster::canonical_action::LivePlayerLease, u64)>,
    active_mining:
        HashMap<GlobalPlayerId, Option<super::cluster_inventory_protocol::ActiveMiningInput>>,
    retained_controller_patches: HashMap<GlobalPlayerId, ControllerPatch>,
    retained_respawn_inputs: HashMap<
        GlobalPlayerId,
        (
            TickStamp,
            RetainedLifecycleRequest,
            Option<pumpkin_cluster::canonical_action::ActionProposal>,
        ),
    >,
    test_departure_arms: HashMap<[u8; 16], TickStamp>,
    velocity_cuts: HashMap<
        GlobalPlayerId,
        (
            pumpkin_cluster::canonical_action::LivePlayerLease,
            [u64; 3],
            [i64; 3],
        ),
    >,
    vehicle_corrections: HashMap<GlobalPlayerId, (uuid::Uuid, TickStamp, PlayerSeq)>,
    pending_settings: VecDeque<(
        pumpkin_cluster::canonical_action::LivePlayerLease,
        crate::net::PlayerConfig,
        u8,
    )>,
    retained_raw_cosmetic_inputs: HashMap<
        GlobalPlayerId,
        (
            super::cluster_remote_player_presentation::CosmeticChange,
            TickStamp,
        ),
    >,
    retained_cosmetic_inputs:
        HashMap<GlobalPlayerId, super::cluster_remote_player_presentation::LocalPlayerCosmetic>,
    retained_vehicle_inputs:
        HashMap<GlobalPlayerId, pumpkin_cluster::actor_relation::VehicleControlInput>,
    chunk_deliveries: HashMap<GlobalPlayerId, JoinedChunkDelivery>,
    next_chunk_lease: u64,
    retained_command_proposals: VecDeque<pumpkin_cluster::canonical_action::ActionProposal>,
    entity_command_sequences: HashMap<(pumpkin_cluster::actor::ActorId, u64), u16>,
    retained_player_commands: VecDeque<super::cluster_player_commands::AuthenticatedPlayerCommand>,
    failed_player_commands: VecDeque<(
        pumpkin_cluster::realm_completion::PeerGeneration,
        [u8; 16],
        super::cluster_player_commands::PlayerCommandSubmitError,
    )>,
    player_commands: Option<
        tokio::sync::mpsc::Receiver<super::cluster_player_commands::AuthenticatedPlayerCommand>,
    >,
    frames: HashMap<GlobalPlayerId, ControllerFrame>,
    applied: HashMap<GlobalPlayerId, TickStamp>,
    saturation_last: HashMap<GlobalPlayerId, TickStamp>,
    pending_policy: HashMap<GlobalPlayerId, u64>,
    next_policy_generation: u64,
    book_preflights:
        HashMap<GlobalPlayerId, Option<super::cluster_inventory_protocol::BookPreflight>>,
    action_sequences: HashMap<GlobalPlayerId, u16>,
    session_progress: HashMap<GlobalPlayerId, SessionProgress>,
    departures: HashMap<GlobalPlayerId, DepartureTicket>,
    departing: HashSet<GlobalPlayerId>,
    administrative_sequences: HashMap<GlobalPlayerId, PlayerSeq>,
    administrative_origins: HashMap<ActionKey, pumpkin_cluster::identity::ServerId>,
    awaiting_teleport:
        HashMap<GlobalPlayerId, (pumpkin_protocol::codec::var_int::VarInt, Vector3<f64>)>,
    abilities: HashMap<GlobalPlayerId, crate::entity::player::Abilities>,
    administrative_states: HashMap<GlobalPlayerId, PlayerAdministrativeState>,
    pending_administrative: HashMap<ActionKey, PendingAdministrative>,
    deferred_administrative: VecDeque<super::cluster_player_admin::PlayerAdministrativeResolution>,
    administrative_publications: VecDeque<AdministrativePublicationTarget>,
    #[cfg(feature = "cluster-e2e")]
    offsets: HashMap<GlobalPlayerId, [f64; 3]>,
    #[cfg(feature = "cluster-e2e")]
    held_departure_admin: HashMap<GlobalPlayerId, (u64, TickStamp)>,
    #[cfg(feature = "cluster-e2e")]
    logged_divergence: HashSet<GlobalPlayerId>,
}

struct JoinedChunkDelivery {
    bootstrap_complete: bool,
    bedrock_receipts: VecDeque<(
        u32,
        pumpkin_util::math::vector2::Vector2<i32>,
        oneshot::Receiver<()>,
    )>,
    active: bool,
    sent_cuts: std::collections::BTreeMap<
        ChunkKey,
        Arc<super::cluster_chunk_presentation::ChunkPresentation>,
    >,
    refresh: Option<(
        oneshot::Receiver<
            Result<
                super::cluster_chunk_watch::WatchActivated,
                super::cluster_chunk_watch::WatchActivationError,
            >,
        >,
        tokio::sync::mpsc::OwnedPermit<super::cluster_chunk_watch::WatchCommand>,
    )>,
    sender: crate::net::ChunkSender,
    encoder: crate::net::chunk_sender::ClusterChunkEncoder,
    view: Option<ViewIntent>,
    need: Option<super::cluster::ChunkNeedLease>,
    release_initial: Option<(GlobalPlayerId, u64)>,
    release_permit:
        Option<tokio::sync::mpsc::OwnedPermit<super::cluster_chunk_watch::WatchCommand>>,
}

impl JoinedChunkDelivery {
    fn advance(&mut self, server: &Server, player: &Arc<Player>, next_lease: &mut u64) {
        while let Some((epoch, position, mut receipt)) = self.bedrock_receipts.pop_front() {
            match receipt.try_recv() {
                Ok(()) => {
                    if self.view.is_some_and(|view| view.chunk_send_epoch == epoch) {
                        self.sender.mark_delivered(&[position]);
                    }
                }
                Err(oneshot::error::TryRecvError::Empty) => {
                    self.bedrock_receipts.push_front((epoch, position, receipt));
                    break;
                }
                Err(oneshot::error::TryRecvError::Closed) => {
                    player.kick(
                        crate::net::DisconnectReason::Unknown,
                        &pumpkin_util::text::TextComponent::text("Chunk delivery failed"),
                    );
                    return;
                }
            }
        }
        if self.release_permit.is_none() {
            self.release_permit = player.cluster_initial_watch_release.pop();
        }
        while let Some((rate, epoch)) = player.cluster_chunk_acknowledgements.pop() {
            if let crate::net::chunk_sender::BatchAckResult::RevisionMismatch { .. } =
                self.sender.on_batch_acknowledged(rate, epoch)
            {
                player.kick(
                    crate::net::DisconnectReason::Unknown,
                    &pumpkin_util::text::TextComponent::text("Chunk delivery revision mismatch"),
                );
                return;
            }
        }
        if player.cluster_departing.load(Ordering::Acquire) {
            return;
        }
        let watches = server.cluster_chunk_watches.load();
        let Some(watch) = watches.get(&player.cluster_gid().unwrap()) else {
            return;
        };
        let view = watch.view;
        self.advance_watch(
            server,
            player,
            view,
            watch.session,
            &watch.delivered,
            next_lease,
        );
        let positions = pumpkin_world::cylindrical_chunk_iterator::Cylindrical::new(
            pumpkin_util::math::vector2::Vector2::new(view.center.x, view.center.z),
            view.radius,
        )
        .all_chunks_within();
        let positions = positions.into_iter().collect::<rustc_hash::FxHashSet<_>>();
        let changed = self.view.is_none_or(|old| {
            old.world != view.world
                || old.center != view.center
                || old.radius != view.radius
                || old.chunk_send_epoch != view.chunk_send_epoch
        });
        if changed {
            let world = server
                .worlds
                .load()
                .iter()
                .find(|world| super::cluster_world_delta::world_id(world) == view.world)
                .cloned();
            let Some(world) = world else {
                return;
            };
            *next_lease = (*next_lease).max(1_u64 << 61).wrapping_add(1);
            let Some(need) = super::cluster::try_require_lobby_chunks(
                server,
                &world,
                &positions.iter().copied().collect::<Vec<_>>(),
                *next_lease,
            ) else {
                return;
            };
            if let Some(old) = self.view {
                if old.world != view.world || old.chunk_send_epoch != view.chunk_send_epoch {
                    self.sender.reset();
                }
                let old_positions = pumpkin_world::cylindrical_chunk_iterator::Cylindrical::new(
                    pumpkin_util::math::vector2::Vector2::new(old.center.x, old.center.z),
                    old.radius,
                )
                .all_chunks_within();
                for position in old_positions {
                    if old.world == view.world && positions.contains(&position) {
                        continue;
                    }
                    self.sender.unload_chunk(&player.client, position);
                    self.encoder.forget(position);
                }
            }
            if let crate::net::ClientPlatform::Java(client) = player.client.as_ref() {
                client.try_send_packet(&pumpkin_protocol::java::client::play::CCenterChunk {
                    chunk_x: view.center.x.into(),
                    chunk_z: view.center.z.into(),
                });
            }
            for position in &positions {
                if !self.sender.is_chunk_sent(position)
                    && !self.sender.pending_chunks.contains(position)
                {
                    self.sender.enqueue_chunk(*position);
                }
            }
            self.need = Some(need);
            self.view = Some(view);
            self.release_initial = Some((watch.view.actor, watch.session));
        }
        if self.need.is_some() && self.release_initial.is_some() {
            if let (Some((actor, session)), Some(permit)) =
                (self.release_initial, self.release_permit.take())
            {
                permit.send(super::cluster_chunk_watch::WatchCommand::ReleaseInitial {
                    actor,
                    session,
                });
                self.release_initial = None;
                self.active = true;
            }
        }
        if let crate::net::ClientPlatform::Bedrock(client) = player.client.as_ref() {
            let worlds = server.worlds.load();
            let Some(world) = worlds
                .iter()
                .find(|world| super::cluster_world_delta::world_id(world) == view.world)
            else {
                return;
            };
            let dimension = match world.dimension.minecraft_name {
                "minecraft:the_nether" => 1,
                "minecraft:the_end" => 2,
                _ => 0,
            };
            if let Err(reason) = self.encoder.advance_bedrock(
                &server.cluster_chunk_presentations.load(),
                view.world,
                &positions,
                dimension,
            ) {
                tracing::error!(actor = view.actor.pack_u32(), %reason, "cluster joined Bedrock chunk encoding failed");
                player.kick(
                    crate::net::DisconnectReason::Unknown,
                    &pumpkin_util::text::TextComponent::text("Chunk encoding failed"),
                );
                return;
            }
            if let Some((batch, encoded)) = self.sender.prepare_encoded_batch(
                &mut self.encoder,
                pumpkin_util::math::vector2::Vector2::new(view.center.x, view.center.z),
                view.chunk_send_epoch,
                pumpkin_util::version::JavaMinecraftVersion::Unknown,
            ) {
                for chunk in &encoded {
                    match client.try_enqueue_owned_chunk(&chunk.payload) {
                        Ok(receipt) => self.bedrock_receipts.push_back((
                            view.chunk_send_epoch,
                            chunk.position,
                            receipt,
                        )),
                        Err(reason) => {
                            tracing::error!(actor = view.actor.pack_u32(), %reason, "cluster joined Bedrock chunk enqueue failed");
                            player.kick(
                                crate::net::DisconnectReason::Unknown,
                                &pumpkin_util::text::TextComponent::text("Chunk delivery failed"),
                            );
                            return;
                        }
                    }
                }
                self.sender
                    .commit_encoded_bedrock_batch(&batch, &encoded, view.chunk_send_epoch);
            }
            return;
        }
        let crate::net::ClientPlatform::Java(client) = player.client.as_ref() else {
            return;
        };
        let version = client.version.load();
        if let Err(reason) = self.encoder.advance(
            &server.cluster_chunk_presentations.load(),
            view.world,
            &positions,
            version,
        ) {
            tracing::error!(actor = view.actor.pack_u32(), %reason, "cluster joined chunk encoding failed");
            player.kick(
                crate::net::DisconnectReason::Unknown,
                &pumpkin_util::text::TextComponent::text("Chunk encoding failed"),
            );
            return;
        }
        if let Some((batch, encoded)) = self.sender.prepare_encoded_batch(
            &mut self.encoder,
            pumpkin_util::math::vector2::Vector2::new(view.center.x, view.center.z),
            view.chunk_send_epoch,
            version,
        ) {
            self.sender
                .commit_batch(&batch, &encoded, &player.client, view.chunk_send_epoch);
        }
    }

    fn advance_watch(
        &mut self,
        server: &Server,
        player: &Arc<Player>,
        view: ViewIntent,
        session: u64,
        delivered: &std::collections::BTreeSet<ChunkKey>,
        next_lease: &mut u64,
    ) {
        self.sent_cuts.extend(
            self.encoder
                .take_sent_presentations()
                .into_iter()
                .map(|(position, cut)| (ChunkKey::new(view.world, position.x, position.y), cut)),
        );
        self.sent_cuts.retain(|key, _| {
            key.world == view.world
                && (i64::from(key.x()) - i64::from(view.center.x)).abs()
                    <= i64::from(view.radius.get()) + 1
                && (i64::from(key.z()) - i64::from(view.center.z)).abs()
                    <= i64::from(view.radius.get()) + 1
        });
        if let Some((receiver, _)) = self.refresh.as_mut() {
            match receiver.try_recv() {
                Ok(Ok(receipt)) if receipt.actor == view.actor && receipt.session == session => {
                    let (_, permit) = self.refresh.take().unwrap();
                    permit.send(super::cluster_chunk_watch::WatchCommand::ReleaseInitial {
                        actor: view.actor,
                        session,
                    });
                }
                Ok(_) | Err(oneshot::error::TryRecvError::Closed) => {
                    self.refresh.take();
                }
                Err(oneshot::error::TryRecvError::Empty) => return,
            }
        }
        let sent = self
            .sent_cuts
            .iter()
            .filter(|(key, _)| {
                key.world == view.world
                    && !delivered.contains(key)
                    && self
                        .sender
                        .is_chunk_ready(&pumpkin_util::math::vector2::Vector2::new(
                            key.x(),
                            key.z(),
                        ))
            })
            .map(|(key, cut)| (*key, cut.clone()))
            .collect::<std::collections::BTreeMap<_, _>>();
        if sent.is_empty() || !self.active {
            return;
        }
        let Some(route) = server.cluster_chunk_watch_route.load_full() else {
            return;
        };
        let Ok(release) = route.as_ref().clone().try_reserve_owned() else {
            return;
        };
        let Ok(submit) = route.as_ref().clone().try_reserve_owned() else {
            return;
        };
        let worlds = server.worlds.load();
        let Some(world) = worlds
            .iter()
            .find(|world| super::cluster_world_delta::world_id(world) == view.world)
        else {
            return;
        };
        *next_lease = (*next_lease).max(1_u64 << 61).wrapping_add(1);
        let positions = sent
            .keys()
            .map(|key| pumpkin_util::math::vector2::Vector2::new(key.x(), key.z()))
            .collect::<Vec<_>>();
        let Some(need) =
            super::cluster::try_require_lobby_chunks(server, world, &positions, *next_lease)
        else {
            return;
        };
        let (respond, receiver) = oneshot::channel();
        submit.send(super::cluster_chunk_watch::WatchCommand::Refresh(
            super::cluster_chunk_watch::WatchActivationRequest {
                actor: view.actor,
                session,
                player: player.clone(),
                view,
                sent,
                need,
                respond,
            },
        ));
        self.refresh = Some((receiver, release));
    }
}

#[derive(Default)]
struct SessionProgress {
    player: Option<std::sync::Weak<Player>>,
    retained: Option<(
        super::cluster_session_ingress::SessionInput,
        oneshot::Receiver<super::cluster_session_ingress::SessionOutcome>,
    )>,
    outstanding: VecDeque<oneshot::Receiver<super::cluster_session_ingress::SessionOutcome>>,
    completed_this_advance: bool,
    failed: bool,
}

#[derive(Default)]
pub struct ControllerSnapshot {
    pub frames: HashMap<GlobalPlayerId, ControllerFrame>,
    applied: HashMap<GlobalPlayerId, TickStamp>,
    active: HashMap<GlobalPlayerId, (pumpkin_cluster::canonical_action::LivePlayerLease, u64)>,
    installed: HashMap<GlobalPlayerId, (pumpkin_cluster::canonical_action::LivePlayerLease, u64)>,
}

#[derive(Clone, Copy)]
struct HandoffCompletion {
    lease: pumpkin_cluster::canonical_action::LivePlayerLease,
    install_generation: u64,
}

pub(crate) struct HandoffCompletionPermit {
    permit: tokio::sync::mpsc::OwnedPermit<HandoffCompletion>,
    completion: HandoffCompletion,
}

impl HandoffCompletionPermit {
    pub(crate) fn complete(self) {
        self.permit.send(self.completion);
    }
}

pub struct ControllerRuntime {
    admission_result_tx:
        tokio::sync::mpsc::Sender<super::cluster_world_actor::ActorAdmissionInstallResult>,
    handoff_completion_tx: tokio::sync::mpsc::Sender<HandoffCompletion>,
    settings: ArrayQueue<(
        pumpkin_cluster::canonical_action::LivePlayerLease,
        crate::net::PlayerConfig,
    )>,
    #[cfg(feature = "cluster-e2e")]
    filters: ArrayQueue<(GlobalPlayerId, [f64; 3])>,
    #[cfg(feature = "cluster-e2e")]
    departure_admin_holds: ArrayQueue<(GlobalPlayerId, u64)>,
    owner: ArrayQueue<ControllerOwner>,
    actor_intents: ArrayQueue<ControllerActorIntent>,
    actor_intent_credits: ArrayQueue<()>,
    view_intents: ArrayQueue<ViewIntent>,
    owner_unavailable_reported: AtomicBool,
    published: ArcSwap<ControllerSnapshot>,
    policy_results: Arc<ArrayQueue<PolicyResult>>,
    game_mode_results: Arc<ArrayQueue<super::cluster_gamemode_commands::PolicyResult>>,
    notices: ArrayQueue<ControllerNotice>,
    departure_requests: ArrayQueue<DepartureRequest>,
}

#[derive(Debug, Clone, PartialEq)]
pub enum DepartureStatus {
    Ready(Box<FrozenPlayerDeparture>),
    Retired(Box<super::cluster_lobby::LocalLobbySnapshot>),
    MissingPlayer,
    Duplicate,
}

#[derive(Debug, Clone, PartialEq)]
pub struct FrozenPlayerDeparture {
    pub lease: pumpkin_cluster::canonical_action::LivePlayerLease,
    pub install_generation: u64,
    pub living: pumpkin_cluster::item_action::ActorLivingState,
    pub spatial: super::cluster_world_actor::ActorSpatialState,
    pub player_inventory: pumpkin_cluster::inventory::InventoryStateSnapshot,
    pub ender_inventory: pumpkin_cluster::inventory::InventoryStateSnapshot,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum DepartureRequestError {
    QueueFull,
    InvalidIdentity,
}

struct DepartureRequest {
    player: Arc<Player>,
    retained: Option<super::cluster_presence::RetainedDeparture>,
    actor: GlobalPlayerId,
    lease: Option<pumpkin_cluster::canonical_action::LivePlayerLease>,
    install_generation: u64,
    expected_close: u64,
    logout: bool,
    respond: oneshot::Sender<DepartureStatus>,
}

struct DepartureTicket {
    retention_need: Option<super::cluster::ChunkNeedLease>,
    retention_complete: bool,
    retention_exact: bool,
    watch_retired: bool,
    retention_reported: bool,
    watch_retire_reported: bool,
    player: Arc<Player>,
    retained: Option<super::cluster_presence::RetainedDeparture>,
    frozen: Option<FrozenPlayerDeparture>,
    local_snapshot: Option<super::cluster_lobby::LocalLobbySnapshot>,
    commit_token: Option<u64>,
    retired: bool,
    registry_removed: bool,
    final_error_reported: bool,
    source: Option<crate::command::CommandSource>,
    lease: pumpkin_cluster::canonical_action::LivePlayerLease,
    install_generation: u64,
    respond: oneshot::Sender<DepartureStatus>,
    expected_close: u64,
    close_staged: bool,
    close_stage_reported: bool,
    logout: bool,
    world: Option<oneshot::Receiver<super::cluster_world_owner::ActorDrainStatus>>,
    world_ready: bool,
    inventory: Option<(
        pumpkin_cluster::inventory::InventoryStateSnapshot,
        pumpkin_cluster::inventory::InventoryStateSnapshot,
    )>,
    through_tick: TickStamp,
}

enum ControllerNotice {
    EpochTeleport(
        GlobalPlayerId,
        pumpkin_cluster::canonical_action::SpatialEpoch,
        pumpkin_protocol::codec::var_int::VarInt,
        Vector3<f64>,
    ),
    Teleport(
        GlobalPlayerId,
        Option<(pumpkin_protocol::codec::var_int::VarInt, Vector3<f64>)>,
    ),
    Abilities(GlobalPlayerId, crate::entity::player::Abilities),
}

#[derive(Clone)]
struct PendingAdministrative {
    update: PlayerAdministrativeAction,
    living_result: Option<bool>,
    recipients: Vec<pumpkin_cluster::identity::ServerId>,
    world_changed: bool,
}

#[derive(Debug, Clone)]
pub(crate) struct LivingAdministrativeProjection {
    pub action: ActionKey,
    pub state: PlayerLivingAdministrativeState,
    pub world_transition: Option<PlayerAdministrativeState>,
    pub applied: Option<bool>,
}

#[derive(Debug)]
pub(crate) struct LivingAdministrativeResult {
    pub action: ActionKey,
    pub applied: bool,
}

pub(crate) fn retain_living_result(
    results: &ArrayQueue<LivingAdministrativeResult>,
    pending: &ArrayQueue<LivingAdministrativeResult>,
    result: LivingAdministrativeResult,
) -> Result<bool, LivingAdministrativeResult> {
    if let Err(result) = results.push(result) {
        pending.push(result).map(|()| false)
    } else {
        Ok(true)
    }
}

#[derive(Debug, Clone, Copy)]
pub(crate) enum ControllerPlayerEffect {
    Jump,
    Movement {
        amount: i32,
        delta: Vector3<f64>,
        height_difference: f64,
        on_ground: bool,
        flying: bool,
        creative: bool,
        position: Vector3<f64>,
    },
}

pub(crate) struct PolicyResult {
    actor: GlobalPlayerId,
    generation: u64,
    frame: ControllerFrame,
    controls: ControlChanges,
    saturated: bool,
    timed_out: bool,
}

fn retain_policy_result(
    results: &ArrayQueue<PolicyResult>,
    retry: &ArrayQueue<PolicyResult>,
    result: PolicyResult,
) -> Result<(), PolicyResult> {
    match results.push(result) {
        Ok(()) => Ok(()),
        Err(mut result) => {
            result.saturated = true;
            retry.push(result)
        }
    }
}

impl ControllerRuntime {
    pub(crate) fn note_settings(
        &self,
        lease: pumpkin_cluster::canonical_action::LivePlayerLease,
        config: crate::net::PlayerConfig,
    ) -> Result<(), crate::net::PlayerConfig> {
        self.settings
            .push((lease, config))
            .map_err(|(_, config)| config)
    }
    pub(crate) fn installed_for_handoff(
        &self,
        lease: pumpkin_cluster::canonical_action::LivePlayerLease,
        install_generation: u64,
    ) -> bool {
        self.published.load().installed.get(&lease.gid) == Some(&(lease, install_generation))
    }
    pub(crate) fn reserve_handoff_completion(
        &self,
        lease: pumpkin_cluster::canonical_action::LivePlayerLease,
        install_generation: u64,
    ) -> Option<HandoffCompletionPermit> {
        if !self.installed_for_handoff(lease, install_generation) {
            return None;
        }
        Some(HandoffCompletionPermit {
            permit: self
                .handoff_completion_tx
                .clone()
                .try_reserve_owned()
                .ok()?,
            completion: HandoffCompletion {
                lease,
                install_generation,
            },
        })
    }
    pub(crate) fn active_for_simulation(
        &self,
        lease: pumpkin_cluster::canonical_action::LivePlayerLease,
        install_generation: u64,
    ) -> bool {
        self.published.load().active.get(&lease.gid) == Some(&(lease, install_generation))
    }
    pub fn new_with_player_commands(
        capacity: usize,
        receiver: tokio::sync::mpsc::Receiver<
            super::cluster_player_commands::AuthenticatedPlayerCommand,
        >,
    ) -> Self {
        let runtime = Self::new(capacity);
        let mut owner = runtime.owner.pop().expect("new controller owner");
        owner.player_commands = Some(receiver);
        assert!(runtime.owner.push(owner).is_ok());
        runtime
    }

    pub fn new(capacity: usize) -> Self {
        let owner = ArrayQueue::new(1);
        let (admission_result_tx, admission_results) = tokio::sync::mpsc::channel(65536);
        let (handoff_completion_tx, handoff_completions) =
            tokio::sync::mpsc::channel(capacity.max(1));
        assert!(
            owner
                .push(ControllerOwner {
                    admission_results: Some(admission_results),
                    handoff_completions: Some(handoff_completions),
                    ..Default::default()
                })
                .is_ok()
        );
        let actor_intent_credits = ArrayQueue::new(1_024);
        for _ in 0..1_024 {
            assert!(actor_intent_credits.push(()).is_ok());
        }
        Self {
            admission_result_tx,
            handoff_completion_tx,
            #[cfg(feature = "cluster-e2e")]
            filters: ArrayQueue::new(64),
            #[cfg(feature = "cluster-e2e")]
            departure_admin_holds: ArrayQueue::new(64),
            owner,
            settings: ArrayQueue::new(capacity.max(1)),
            actor_intents: ArrayQueue::new(capacity.saturating_mul(2).max(1)),
            actor_intent_credits,
            view_intents: ArrayQueue::new(capacity.saturating_mul(2).max(1)),
            owner_unavailable_reported: AtomicBool::new(false),
            published: ArcSwap::from_pointee(ControllerSnapshot::default()),
            policy_results: Arc::new(ArrayQueue::new(capacity)),
            game_mode_results: Arc::new(ArrayQueue::new(128)),
            notices: ArrayQueue::new(capacity),
            departure_requests: ArrayQueue::new(usize::from(u16::MAX) + 1),
        }
    }

    pub(crate) fn reserve_actor_admission_result(
        &self,
    ) -> Option<
        tokio::sync::mpsc::OwnedPermit<super::cluster_world_actor::ActorAdmissionInstallResult>,
    > {
        self.admission_result_tx.clone().try_reserve_owned().ok()
    }

    pub fn request_player_departure(
        &self,
        player: Arc<Player>,
        lease: Option<pumpkin_cluster::canonical_action::LivePlayerLease>,
        install_generation: u64,
        expected_close: u64,
        logout: bool,
        retained: Option<super::cluster_presence::RetainedDeparture>,
    ) -> Result<oneshot::Receiver<DepartureStatus>, DepartureRequestError> {
        let actor = player
            .cluster_gid()
            .ok_or(DepartureRequestError::InvalidIdentity)?;
        if lease.is_some_and(|lease| lease.gid != actor || lease.key.0 != player.gameprofile.id) {
            return Err(DepartureRequestError::InvalidIdentity);
        }
        let (respond, receive) = oneshot::channel();
        self.departure_requests
            .push(DepartureRequest {
                player,
                retained,
                actor,
                lease,
                install_generation,
                expected_close,
                logout,
                respond,
            })
            .map_err(|_| DepartureRequestError::QueueFull)?;
        Ok(receive)
    }

    fn try_request_actor_drain(
        server: &Server,
        actor: GlobalPlayerId,
        lease: pumpkin_cluster::canonical_action::LivePlayerLease,
        install_generation: u64,
        through_tick: TickStamp,
        logout: bool,
    ) -> Option<oneshot::Receiver<super::cluster_world_owner::ActorDrainStatus>> {
        let route = server.cluster_actor_drain_route.load_full()?;
        let (respond, receive) = oneshot::channel();
        route
            .try_send(super::cluster_world_owner::ActorDrainRequest {
                gid: actor,
                lease,
                install_generation,
                through_tick,
                logout,
                respond,
            })
            .ok()?;
        Some(receive)
    }

    #[cfg(feature = "cluster-e2e")]
    pub fn set_test_offset(&self, actor: GlobalPlayerId, offset: [f64; 3]) -> bool {
        self.filters.push((actor, offset)).is_ok()
    }

    #[cfg(feature = "cluster-e2e")]
    pub fn arm_test_departure_admin(&self, actor: GlobalPlayerId, origin: u64) -> bool {
        self.departure_admin_holds.push((actor, origin)).is_ok()
    }

    pub fn frame(&self, actor: GlobalPlayerId) -> Option<ControllerFrame> {
        self.published.load().frames.get(&actor).copied()
    }

    pub fn drain_actor_intents(&self) -> Vec<ControllerActorIntent> {
        let mut intents = Vec::with_capacity(self.actor_intents.len());
        while let Some(intent) = self.actor_intents.pop() {
            intents.push(intent);
        }
        intents
    }

    pub fn drain_view_intents(&self) -> Vec<ViewIntent> {
        let mut intents = Vec::with_capacity(self.view_intents.len());
        while let Some(intent) = self.view_intents.pop() {
            intents.push(intent);
        }
        intents
    }

    fn has_intent_capacity(&self) -> bool {
        !self.actor_intent_credits.is_empty()
            && !self.actor_intents.is_full()
            && !self.view_intents.is_full()
    }

    pub(crate) fn return_actor_intent_credit(&self) {
        assert!(self.actor_intent_credits.push(()).is_ok());
    }

    fn stage_actor_intent(
        &self,
        server: &Server,
        player: &Player,
        intent: ControllerActorIntent,
    ) -> bool {
        if !self.has_intent_capacity() {
            return false;
        }
        if self.actor_intent_credits.pop().is_none() {
            return false;
        }
        let view = controller_view_intent(server, player, intent);
        assert!(self.actor_intents.push(intent).is_ok());
        assert!(self.view_intents.push(view).is_ok());
        true
    }

    pub fn effective_for_tick(
        &self,
        actor: GlobalPlayerId,
        tick: TickStamp,
    ) -> Option<ControllerFrame> {
        let published = self.published.load();
        (published.applied.get(&actor) == Some(&tick))
            .then(|| published.frames.get(&actor).copied())
            .flatten()
    }

    pub fn note_teleport(
        &self,
        actor: GlobalPlayerId,
        pending: Option<(pumpkin_protocol::codec::var_int::VarInt, Vector3<f64>)>,
    ) -> bool {
        self.notices
            .push(ControllerNotice::Teleport(actor, pending))
            .is_ok()
    }

    pub fn note_epoch_teleport(
        &self,
        actor: GlobalPlayerId,
        epoch: pumpkin_cluster::canonical_action::SpatialEpoch,
        id: pumpkin_protocol::codec::var_int::VarInt,
        position: Vector3<f64>,
    ) -> bool {
        self.notices
            .push(ControllerNotice::EpochTeleport(actor, epoch, id, position))
            .is_ok()
    }

    pub fn note_abilities(
        &self,
        actor: GlobalPlayerId,
        abilities: crate::entity::player::Abilities,
    ) -> bool {
        self.notices
            .push(ControllerNotice::Abilities(actor, abilities))
            .is_ok()
    }

    pub fn advance(&self, server: &Arc<Server>, tick: TickStamp) {
        if !self.actor_intents.is_empty() || !self.view_intents.is_empty() {
            return;
        }
        let Some(ControllerOwner {
            mut admission_results,
            mut admission_outcomes,
            mut pending_departure_requests,
            mut game_mode_pending,
            mut game_mode_preflighted,
            mut handoff_completions,
            mut frame_epochs,
            mut awaiting_bedrock_teleport,
            mut projected_epochs,
            mut morning_cuts,
            mut active_mining_leases,
            mut active_mining,
            mut retained_controller_patches,
            mut retained_respawn_inputs,
            mut test_departure_arms,
            mut velocity_cuts,
            mut vehicle_corrections,
            mut pending_settings,
            mut retained_raw_cosmetic_inputs,
            mut retained_cosmetic_inputs,
            mut retained_vehicle_inputs,
            mut chunk_deliveries,
            mut next_chunk_lease,
            mut retained_command_proposals,
            mut entity_command_sequences,
            mut retained_player_commands,
            mut failed_player_commands,
            mut player_commands,
            mut frames,
            mut applied,
            mut saturation_last,
            mut pending_policy,
            mut next_policy_generation,
            mut book_preflights,
            mut action_sequences,
            mut session_progress,
            mut departures,
            mut departing,
            mut administrative_sequences,
            mut administrative_origins,
            mut awaiting_teleport,
            mut abilities,
            mut administrative_states,
            mut pending_administrative,
            mut deferred_administrative,
            mut administrative_publications,
            #[cfg(feature = "cluster-e2e")]
            mut offsets,
            #[cfg(feature = "cluster-e2e")]
            mut held_departure_admin,
            #[cfg(feature = "cluster-e2e")]
            mut logged_divergence,
        }) = self.owner.pop()
        else {
            if !self
                .owner_unavailable_reported
                .swap(true, Ordering::Relaxed)
            {
                tracing::error!("cluster controller owner unavailable at tick boundary");
            }
            return;
        };
        flush_administrative_publications(server, &mut administrative_publications);
        while let Some(notice) = self.notices.pop() {
            match notice {
                ControllerNotice::EpochTeleport(actor, epoch, id, position) => {
                    projected_epochs.insert(actor, epoch);
                    awaiting_teleport.insert(actor, (id, position));
                }
                ControllerNotice::Teleport(actor, pending) => {
                    if let Some(pending) = pending {
                        awaiting_teleport.insert(actor, pending);
                    } else {
                        awaiting_teleport.remove(&actor);
                    }
                }
                ControllerNotice::Abilities(actor, value) => {
                    abilities.insert(actor, value);
                }
            }
        }
        #[cfg(feature = "cluster-e2e")]
        while let Some((actor, offset)) = self.filters.pop() {
            offsets.insert(actor, offset);
            logged_divergence.remove(&actor);
        }
        #[cfg(feature = "cluster-e2e")]
        while let Some((actor, origin)) = self.departure_admin_holds.pop() {
            held_departure_admin.insert(actor, (origin, tick));
        }
        let players: HashMap<_, _> = server
            .get_all_players()
            .into_iter()
            .filter_map(|player| player.cluster_gid().map(|gid| (gid, player)))
            .collect();
        chunk_deliveries.retain(|actor, _| players.contains_key(actor));
        for (actor, player) in &players {
            if let Some(sender) = player.cluster_initial_chunk_sender.pop() {
                chunk_deliveries.insert(
                    *actor,
                    JoinedChunkDelivery {
                        bootstrap_complete: false,
                        bedrock_receipts: VecDeque::new(),
                        sender,
                        encoder: crate::net::chunk_sender::ClusterChunkEncoder::new(),
                        view: None,
                        need: None,
                        release_initial: None,
                        release_permit: None,
                        active: false,
                        sent_cuts: Default::default(),
                        refresh: None,
                    },
                );
            }
            if let Some(delivery) = chunk_deliveries.get_mut(actor) {
                delivery.advance(server, player, &mut next_chunk_lease);
            }
        }
        drain_session_inputs(server, &players, &mut session_progress);
        if let Some(receiver) = handoff_completions.as_mut() {
            while let Ok(completion) = receiver.try_recv() {
                if players.get(&completion.lease.gid).is_some_and(|player| {
                    super::cluster_world_owner::player_action_lease(server, player)
                        == Some(completion.lease)
                        && player
                            .cluster_owner_install_generation
                            .load(Ordering::Acquire)
                            == completion.install_generation
                }) && let Some(delivery) = chunk_deliveries.get_mut(&completion.lease.gid)
                {
                    delivery.bootstrap_complete = true;
                }
            }
        }
        while let Some(mut result) = self.game_mode_results.pop() {
            game_mode_pending.remove(&result.command.request.correlation);
            let valid = players.get(&result.lease.gid).is_some_and(|player| {
                super::cluster_world_owner::player_action_lease(server, player)
                    == Some(result.lease)
                    && player
                        .cluster_owner_install_generation
                        .load(Ordering::Acquire)
                        == result.generation
                    && !player.cluster_departing.load(Ordering::Acquire)
            }) && server
                .cluster_actor_living
                .load()
                .get(&result.lease.gid.into())
                .and_then(|state| state.avatar.as_ref())
                .is_some_and(|avatar| avatar.spatial_epoch == result.epoch);
            if valid
                && let Some(next) = result.mode
                && let pumpkin_cluster::player_command_control::PlayerCommandMutation::GameMode {
                    mode,
                    ..
                } = &mut result.command.request.mutation
            {
                *mode = next;
                game_mode_preflighted.insert(result.command.request.correlation);
                retained_player_commands.push_front(result.command);
            } else {
                failed_player_commands.push_back((
                    result.command.origin,
                    result.command.request.correlation,
                    super::cluster_player_commands::PlayerCommandSubmitError::InvalidInput,
                ));
            }
        }
        while let Some((origin, correlation, reason)) = failed_player_commands.front().copied() {
            if server
                .cluster_player_commands
                .record_input_failure(origin, correlation, reason)
                .is_err()
            {
                break;
            }
            failed_player_commands.pop_front();
        }
        while let Some(proposal) = retained_command_proposals.pop_front() {
            if super::cluster_world_owner::submit_action(
                server,
                pumpkin_cluster::canonical_action::ActionInput::Proposal(proposal.clone()),
            )
            .is_err()
            {
                retained_command_proposals.push_front(proposal);
                break;
            }
        }
        if let Some(receiver) = player_commands.as_mut() {
            while retained_player_commands.len()
                + failed_player_commands.len()
                + retained_command_proposals.len()
                < SESSION_OUTSTANDING_LIMIT
            {
                let Ok(command) = receiver.try_recv() else {
                    break;
                };
                retained_player_commands.push_back(command);
            }
        }
        for _ in 0..retained_player_commands.len() {
            if !retained_command_proposals.is_empty() {
                break;
            }
            let Some(command) = retained_player_commands.pop_front() else {
                break;
            };
            if let pumpkin_cluster::player_command_control::CommandTarget::World { owner, .. } =
                &command.request.target
            {
                if let Err(reason) =
                    super::cluster_player_commands::authorize_authenticated(server, &command)
                {
                    failed_player_commands.push_back((
                        command.origin,
                        command.request.correlation,
                        reason,
                    ));
                    continue;
                }
                let pumpkin_cluster::player_command_control::PlayerCommandMutation::Summon(request) =
                    &command.request.mutation
                else {
                    failed_player_commands.push_back((
                        command.origin,
                        command.request.correlation,
                        super::cluster_player_commands::PlayerCommandSubmitError::InvalidInput,
                    ));
                    continue;
                };
                let actor = pumpkin_cluster::actor::ActorId::Peer(owner.server);
                let raw_sequence = entity_command_sequences
                    .entry((actor, owner.generation))
                    .or_insert(0);
                *raw_sequence = raw_sequence.wrapping_add(1);
                let proposal = pumpkin_cluster::canonical_action::ActionProposal {
                    command: Some((
                        command.origin,
                        command.transport_epoch,
                        command.request.clone(),
                    )),
                    id: pumpkin_cluster::canonical_action::ActionId {
                        actor,
                        incarnation: owner.generation,
                        sequence: *raw_sequence,
                        family: 7,
                    },
                    tick,
                    lease: None,
                    client_origin: pumpkin_cluster::canonical_action::ClientOrigin::None,
                    screen_generation: 0,
                    claimed_targets: Vec::new(),
                    claim: pumpkin_cluster::canonical_action::ClaimedAction::EntityAdmissionRequest(
                        request.clone(),
                    ),
                };
                if super::cluster_world_owner::submit_action(
                    server,
                    pumpkin_cluster::canonical_action::ActionInput::Proposal(proposal.clone()),
                )
                .is_err()
                {
                    retained_command_proposals.push_back(proposal);
                }
                continue;
            }
            let Some((target, target_uuid)) = command.request.player_target() else {
                if let Err(reason) =
                    super::cluster_player_commands::authorize_authenticated(server, &command)
                {
                    failed_player_commands.push_back((
                        command.origin,
                        command.request.correlation,
                        reason,
                    ));
                    continue;
                }
                let pumpkin_cluster::player_command_control::CommandTarget::Entity {
                    identity, ..
                } = &command.request.target
                else {
                    unreachable!();
                };
                let Some(mut proposal) =
                    pumpkin_cluster::canonical_action::ActionProposal::entity_command(
                        identity.clone(),
                        command.origin,
                        command.transport_epoch,
                        command.request.clone(),
                        tick,
                    )
                else {
                    failed_player_commands.push_back((
                        command.origin,
                        command.request.correlation,
                        super::cluster_player_commands::PlayerCommandSubmitError::InvalidInput,
                    ));
                    continue;
                };
                let raw_sequence = entity_command_sequences
                    .entry((proposal.id.actor, proposal.id.incarnation))
                    .or_insert(0);
                *raw_sequence = raw_sequence.wrapping_add(1);
                proposal.id.sequence = *raw_sequence;
                if super::cluster_world_owner::submit_action(
                    server,
                    pumpkin_cluster::canonical_action::ActionInput::Proposal(proposal.clone()),
                )
                .is_err()
                {
                    retained_command_proposals.push_back(proposal);
                }
                continue;
            };
            let departure_test =
                cfg!(feature = "cluster-e2e") && command.request.correlation[..8] == *b"PumpDAdm";
            if departure_test
                && players.get(&target).is_some_and(|player| {
                    player.gameprofile.id.as_bytes() == &target_uuid
                        && !player.cluster_departing.load(Ordering::Acquire)
                })
                && super::cluster_player_commands::authorize_authenticated(server, &command).is_ok()
            {
                let started = *test_departure_arms
                    .entry(command.request.correlation)
                    .or_insert(tick);
                if tick.distance_since(started) >= 600 {
                    test_departure_arms.remove(&command.request.correlation);
                    failed_player_commands.push_back((
                        command.origin,
                        command.request.correlation,
                        super::cluster_player_commands::PlayerCommandSubmitError::InvalidInput,
                    ));
                    continue;
                }
                retained_player_commands.push_back(command);
                continue;
            }
            test_departure_arms.remove(&command.request.correlation);
            let failure = super::cluster_player_commands::authorize_authenticated(server, &command)
                .err()
                .or_else(|| {
                    players
                        .get(&target)
                        .filter(|player| {
                            player.gameprofile.id.as_bytes() == &target_uuid
                                && (departure_test
                                    || !player.cluster_departing.load(Ordering::Acquire))
                        })
                        .is_none()
                        .then_some(
                            super::cluster_player_commands::PlayerCommandSubmitError::NoTarget,
                        )
                });
            if let Some(reason) = failure {
                game_mode_preflighted.remove(&command.request.correlation);
                failed_player_commands.push_back((
                    command.origin,
                    command.request.correlation,
                    reason,
                ));
                continue;
            }
            if game_mode_pending.values().any(|lease| lease.gid == target) {
                retained_player_commands.push_back(command);
                continue;
            }
            let player = &players[&target];
            let Some(lease) = super::cluster_world_owner::player_action_lease(server, player)
            else {
                retained_player_commands.push_back(command);
                continue;
            };
            if matches!(command.request.mutation, pumpkin_cluster::player_command_control::PlayerCommandMutation::GameMode { mode, previous } if mode != previous)
                && server.plugin_manager.has_handlers::<crate::plugin::api::events::player::player_gamemode_change::PlayerGamemodeChangeEvent>()
                && !game_mode_preflighted.contains(&command.request.correlation) {
                if game_mode_pending.len() >= 128 {
                    retained_player_commands.push_back(command);
                    continue;
                }
                let correlation = command.request.correlation;
                game_mode_pending.insert(correlation, lease);
                if let Err(command) = super::cluster_gamemode_commands::begin_policy(server, player, command, self.game_mode_results.clone()) {
                    game_mode_pending.remove(&correlation);
                    failed_player_commands.push_back((command.origin, correlation,
                        super::cluster_player_commands::PlayerCommandSubmitError::InvalidInput));
                }
                continue;
            }
            game_mode_preflighted.remove(&command.request.correlation);
            let mut proposal = pumpkin_cluster::canonical_action::ActionProposal::player_command(
                lease,
                command.origin,
                command.transport_epoch,
                command.request.clone(),
                tick,
            );
            if let pumpkin_cluster::player_command_control::PlayerCommandMutation::Summon(request) =
                &command.request.mutation
            {
                proposal.claim =
                    pumpkin_cluster::canonical_action::ClaimedAction::EntityAdmissionRequest(
                        request.clone(),
                    );
            }
            let raw_sequence = action_sequences.entry(target).or_insert(0);
            *raw_sequence = raw_sequence.wrapping_add(1);
            proposal.id.sequence = *raw_sequence;
            if super::cluster_world_owner::submit_action(
                server,
                pumpkin_cluster::canonical_action::ActionInput::Proposal(proposal.clone()),
            )
            .is_err()
            {
                retained_command_proposals.push_back(proposal);
            }
        }
        departing.retain(|actor| players.contains_key(actor));
        velocity_cuts.retain(|actor, _| players.contains_key(actor));
        frames.retain(|actor, _| players.contains_key(actor));
        frame_epochs.retain(|actor, _| players.contains_key(actor));
        projected_epochs.retain(|actor, _| players.contains_key(actor));
        awaiting_bedrock_teleport.retain(|actor| players.contains_key(actor));
        retained_controller_patches.retain(|actor, _| players.contains_key(actor));
        applied.retain(|actor, _| players.contains_key(actor));
        saturation_last.retain(|actor, _| players.contains_key(actor));
        pending_policy.retain(|actor, _| players.contains_key(actor));
        book_preflights.retain(|actor, _| players.contains_key(actor));
        active_mining.retain(|actor, _| players.contains_key(actor));
        active_mining_leases.retain(|actor, _| players.contains_key(actor));
        for (actor, player) in &players {
            let binding =
                super::cluster_world_owner::player_action_lease(server, player).map(|lease| {
                    (
                        lease,
                        player
                            .cluster_owner_install_generation
                            .load(Ordering::Acquire),
                    )
                });
            if binding != active_mining_leases.get(actor).copied() {
                if active_mining_leases.contains_key(actor) {
                    frames.remove(actor);
                    frame_epochs.remove(actor);
                    awaiting_bedrock_teleport.remove(actor);
                    pending_policy.remove(actor);
                    retained_controller_patches.remove(actor);
                    retained_respawn_inputs.remove(actor);
                    awaiting_teleport.remove(actor);
                }
                active_mining.remove(actor);
                if let Some(binding) = binding {
                    active_mining_leases.insert(*actor, binding);
                } else {
                    active_mining_leases.remove(actor);
                }
            }
        }
        awaiting_teleport.retain(|actor, _| players.contains_key(actor));
        abilities.retain(|actor, _| players.contains_key(actor));
        #[cfg(feature = "cluster-e2e")]
        offsets.retain(|actor, _| players.contains_key(actor));
        #[cfg(feature = "cluster-e2e")]
        logged_divergence.retain(|actor| players.contains_key(actor));
        morning_cuts.retain(|actor, _| players.contains_key(actor));
        let worlds = server.worlds.load();
        for (actor, player) in &players {
            if player.cluster_departing.load(Ordering::Acquire) {
                continue;
            }
            let Some(lease) = super::cluster_world_owner::player_action_lease(server, player)
            else {
                continue;
            };
            let generation = player
                .cluster_owner_install_generation
                .load(Ordering::Acquire);
            let living = server.cluster_actor_living.load();
            let Some(avatar) = living
                .get(&pumpkin_cluster::actor::ActorId::Player(*actor))
                .and_then(|state| state.avatar.as_ref())
            else {
                continue;
            };
            let Some(world) = worlds
                .iter()
                .find(|world| super::cluster_world_delta::world_id(world) == avatar.world)
            else {
                continue;
            };
            let environment = world.cluster_environment_view.load();
            let morning = environment.morning.filter(|morning| {
                super::cluster_world_rules::primary(server) == Some(morning.issuer)
            });
            let cut = (lease, generation, morning);
            if morning_cuts.get(actor) != Some(&cut)
                && let Some(morning) = morning
                && avatar.presentation.sleeping.is_some()
            {
                let Some(current_slot) = super::cluster_world_rules::slot_for_tick(tick) else {
                    continue;
                };
                let Some(sleeping) = avatar.presentation.sleeping else {
                    continue;
                };
                let age = tick.distance_since(sleeping.since);
                if age < 32768
                    && current_slot
                        .checked_sub(u64::from(age))
                        .is_some_and(|sleep_slot| sleep_slot <= morning.slot)
                    && !submit_patch(server, player, ControllerPatch::WakeMorning(morning))
                {
                    continue;
                }
            }
            morning_cuts.insert(*actor, cut);
        }
        let living_publication = server.cluster_actor_living.load();
        for (actor, player) in &players {
            if !frames.contains_key(actor) {
                let claimed = initial_claim(player, *actor, tick);
                let frame = ControllerFrame {
                    claimed,
                    effective: claimed.into(),
                    decision: ControllerDecision::Accepted,
                };
                let initial_controls = ControlChanges {
                    sneaking: Some(
                        claimed.input_flags
                            & pumpkin_protocol::java::server::play::SPlayerInput::SNEAK as u64
                            != 0,
                    ),
                    sprinting: Some(
                        claimed.input_flags
                            & pumpkin_protocol::java::server::play::SPlayerInput::SPRINT as u64
                            != 0,
                    ),
                    ..ControlChanges::default()
                };
                let intent = controller_actor_intent(
                    server,
                    player,
                    frame,
                    initial_controls,
                    ControllerActorSource::Initial,
                    super::cluster_world_delta::world_id(&player.world()),
                    tick,
                    abilities
                        .get(actor)
                        .is_some_and(|abilities| abilities.flying),
                    Some(player.cluster_initial_gamemode),
                );
                if !self.stage_actor_intent(server, player, intent) {
                    player.kick(
                        crate::net::DisconnectReason::Unknown,
                        &pumpkin_util::text::TextComponent::text(
                            "Controller actor queue saturated",
                        ),
                    );
                    continue;
                }
                frames.insert(*actor, frame);
            }
            if let Some(living) =
                living_publication.get(&pumpkin_cluster::actor::ActorId::Player(*actor))
                && let Some(avatar) = living.avatar.as_ref()
            {
                if frame_epochs
                    .get(actor)
                    .is_some_and(|epoch| *epoch != avatar.spatial_epoch)
                {
                    let Some(lease) =
                        super::cluster_world_owner::player_action_lease(server, player)
                    else {
                        continue;
                    };
                    let generation = player
                        .cluster_owner_install_generation
                        .load(Ordering::Acquire);
                    let Some(world) = worlds
                        .iter()
                        .find(|world| super::cluster_world_delta::world_id(world) == avatar.world)
                    else {
                        continue;
                    };
                    if !world.mirror_cluster_player_world(
                        player,
                        lease,
                        generation,
                        avatar.spatial_epoch,
                    ) {
                        pending_policy.insert(*actor, 0);
                        continue;
                    }
                    let mut frame = frames[actor];
                    frame.effective.position = avatar.position.map(f64::from_bits);
                    frame.effective.velocity = living
                        .combat
                        .velocity_micro
                        .map(|value| value as f64 / 1_000_000.0);
                    frame.effective.yaw = f32::from_bits(avatar.yaw);
                    frame.effective.pitch = f32::from_bits(avatar.pitch);
                    frame.effective.on_ground = avatar.on_ground;
                    frame.effective.tick = tick;
                    frame.decision = ControllerDecision::Modified(4);
                    let intent = controller_actor_intent(
                        server,
                        player,
                        frame,
                        ControlChanges::default(),
                        ControllerActorSource::Client,
                        avatar.world,
                        tick,
                        abilities
                            .get(actor)
                            .is_some_and(|abilities| abilities.flying),
                        None,
                    );
                    if self
                        .view_intents
                        .push(controller_view_intent(server, player, intent))
                        .is_err()
                    {
                        pending_policy.insert(*actor, 0);
                        continue;
                    }
                    if matches!(
                        player.client.as_ref(),
                        crate::net::ClientPlatform::Bedrock(_)
                    ) {
                        awaiting_bedrock_teleport.insert(*actor);
                    }
                    frames.insert(*actor, frame);
                    pending_policy.remove(actor);
                    applied.insert(*actor, tick);
                }
                frame_epochs.insert(*actor, avatar.spatial_epoch);
            }
            administrative_states.entry(*actor).or_insert_with(|| {
                player
                    .cluster_initial_administrative
                    .clone()
                    .expect("prepared cluster administrative state")
            });
            if let Some(state) = living_publication
                .get(&pumpkin_cluster::actor::ActorId::Player(*actor))
                .and_then(|state| state.player_administrative_state())
            {
                if administrative_states
                    .get(actor)
                    .is_none_or(|before| before.gamemode != state.gamemode)
                {
                    apply_administrative_controller(player, &state, &mut abilities);
                    if let Some(ability) = abilities.get(actor) {
                        player.send_abilities_snapshot(*ability);
                    }
                }
                administrative_states.insert(*actor, state);
            }
            while let Some(result) = player.cluster_administrative_results.pop() {
                if let Some(pending) = pending_administrative.get_mut(&result.action) {
                    pending.living_result = Some(result.applied);
                }
            }
        }
        while pending_settings.len() < self.settings.capacity() {
            let Some(settings) = self.settings.pop() else {
                break;
            };
            pending_settings.push_back((settings.0, settings.1, 0));
        }
        while let Some((lease, config, mut cosmetics)) = pending_settings.pop_front() {
            let Some(player) = players.get(&lease.gid) else {
                continue;
            };
            if super::cluster_world_owner::player_action_lease(server, player) != Some(lease)
                || player.cluster_departing.load(Ordering::Acquire)
            {
                continue;
            }
            let Some(frame) = frames.get(&lease.gid).copied() else {
                pending_settings.push_front((lease, config, cosmetics));
                break;
            };
            if self.view_intents.is_full() {
                pending_settings.push_front((lease, config, cosmetics));
                break;
            }
            let listing_changed = player.config.load().server_listing != config.server_listing;
            let listing_permit = if listing_changed {
                let Some(permit) = server.cluster_presence.try_reserve() else {
                    pending_settings.push_front((lease, config, cosmetics));
                    break;
                };
                Some(permit)
            } else {
                None
            };
            let skin_changed = player.config.load().skin_parts != config.skin_parts;
            if skin_changed
                && cosmetics & 1 == 0
                && player
                    .cluster_controller_inputs
                    .push(ControllerPatch::Cosmetic(
                        super::cluster_remote_player_presentation::CosmeticChange::Skin(
                            config.skin_parts,
                        ),
                    ))
                    .is_err()
            {
                pending_settings.push_front((lease, config, cosmetics));
                break;
            }
            cosmetics |= 1;
            if player.config.load().main_hand != config.main_hand
                && cosmetics & 2 == 0
                && player
                    .cluster_controller_inputs
                    .push(ControllerPatch::Cosmetic(
                        super::cluster_remote_player_presentation::CosmeticChange::MainHand(
                            config.main_hand as u8,
                        ),
                    ))
                    .is_err()
            {
                pending_settings.push_front((lease, config, cosmetics));
                break;
            }
            if let Some(permit) = listing_permit {
                let login = pumpkin_cluster::presence::PresenceLogin::new(
                    lease.gid,
                    player.gameprofile.id.into_bytes(),
                    player.gameprofile.name.clone(),
                    super::cluster_presence::presence_properties_for(player),
                    false,
                )
                .with_listing(config.server_listing);
                permit.send(super::cluster_presence::PresenceCommand::LocalLogin(
                    login,
                    player
                        .cluster_owner_install_generation
                        .load(Ordering::Acquire),
                ));
            }
            player.config.store(Arc::new(config));
            let intent = controller_actor_intent(
                server,
                player,
                frame,
                ControlChanges::default(),
                ControllerActorSource::Client,
                player
                    .cluster_initial_administrative
                    .as_ref()
                    .expect("prepared settings world")
                    .world,
                tick,
                abilities
                    .get(&lease.gid)
                    .is_some_and(|abilities| abilities.flying),
                None,
            );
            let mut view = controller_view_intent(server, player, intent);
            if let Some(spatial) = server.cluster_actor_spatial.load().get(&lease.gid) {
                view.world = spatial.truth.local.world;
            }
            assert!(self.view_intents.push(view).is_ok());
        }
        while deferred_administrative.len() + pending_administrative.len() < ADMIN_OUTSTANDING_LIMIT
        {
            let Some(resolution) = server.cluster_player_admin.pop_resolution() else {
                break;
            };
            deferred_administrative.push_back(resolution);
        }
        let deferred_count = deferred_administrative.len();
        for _ in 0..deferred_count {
            let Some(resolution) = deferred_administrative.pop_front() else {
                break;
            };
            #[cfg(feature = "cluster-e2e")]
            if let Some((origin, armed_at)) =
                held_departure_admin.get(&resolution.update.target).copied()
                && resolution.update.origin == Some(origin)
            {
                let departing_now = players
                    .get(&resolution.update.target)
                    .is_some_and(|player| player.cluster_departing.load(Ordering::Acquire));
                if resolution.accepted && !departing_now && tick.distance_since(armed_at) < 200 {
                    deferred_administrative.push_back(resolution);
                    continue;
                }
                if !departing_now && tick.distance_since(armed_at) >= 200 {
                    tracing::error!(
                        actor = resolution.update.target.pack_u32(),
                        origin,
                        "cluster test departure admin hold expired"
                    );
                }
                held_departure_admin.remove(&resolution.update.target);
            }
            let Some(current) = administrative_states.get(&resolution.update.target) else {
                deferred_administrative.push_back(resolution);
                continue;
            };
            if !resolution.accepted {
                let recipients = if resolution.action.family == (7 | 0x80) {
                    Vec::new()
                } else {
                    administrative_recipients(
                        administrative_origins.get(&resolution.action).copied(),
                        resolution.holders.clone(),
                    )
                };
                if !queue_administrative_publication(
                    server,
                    &mut administrative_publications,
                    recipients,
                    PlayerAdministrativePublication {
                        action: resolution.action,
                        tick: resolution.update.tick,
                        target: resolution.update.target,
                        origin: resolution.update.origin,
                        accepted: false,
                        state: current.clone(),
                    },
                ) {
                    deferred_administrative.push_back(resolution);
                    continue;
                }
                administrative_origins.remove(&resolution.action);
                continue;
            }
            let canonical_administration = resolution.action.family == (7 | 0x80);
            let player_phase_applied = canonical_administration
                || current.living() == resolution.update.forward.living()
                    && current.world == resolution.update.forward.world;
            let world_changed = current.world != resolution.update.forward.world;
            if !player_phase_applied {
                let Some(player) = players.get(&resolution.update.target) else {
                    deferred_administrative.push_back(resolution);
                    continue;
                };
                let mut living = resolution.update.forward.living();
                living.gamemode = current.gamemode;
                if player
                    .cluster_administrative_inputs
                    .push(LivingAdministrativeProjection {
                        action: resolution.action,
                        state: living,
                        world_transition: (current.world != resolution.update.forward.world)
                            .then(|| resolution.update.forward.clone()),
                        applied: None,
                    })
                    .is_err()
                {
                    deferred_administrative.push_back(resolution);
                    continue;
                }
            }
            pending_administrative.insert(
                resolution.action,
                PendingAdministrative {
                    update: resolution.update,
                    living_result: player_phase_applied.then_some(true),
                    recipients: if canonical_administration {
                        administrative_origins.remove(&resolution.action);
                        Vec::new()
                    } else {
                        administrative_recipients(
                            administrative_origins.remove(&resolution.action),
                            resolution.holders,
                        )
                    },
                    world_changed,
                },
            );
        }
        let failed: Vec<_> = pending_administrative
            .iter()
            .filter_map(|(action, pending)| {
                (pending.living_result == Some(false)).then_some(*action)
            })
            .collect();
        for action in failed {
            let Some(pending) = pending_administrative.get_mut(&action) else {
                continue;
            };
            let Some(player) = players.get(&pending.update.target) else {
                continue;
            };
            let Some(current) = administrative_states.get(&pending.update.target) else {
                continue;
            };
            let mut living = pending.update.forward.living();
            living.gamemode = current.gamemode;
            let world_transition = (super::cluster_world_delta::world_id(&player.world())
                != pending.update.forward.world)
                .then(|| pending.update.forward.clone());
            if player
                .cluster_administrative_inputs
                .push(LivingAdministrativeProjection {
                    action,
                    state: living,
                    world_transition,
                    applied: None,
                })
                .is_ok()
            {
                pending.living_result = None;
            }
        }
        let mut administrative_applied = HashSet::new();
        let ready: Vec<_> = pending_administrative
            .iter()
            .filter_map(|(action, pending)| {
                (pending.living_result == Some(true)).then_some(*action)
            })
            .collect();
        for action in ready {
            let Some(pending) = pending_administrative.get(&action) else {
                continue;
            };
            if !publication_has_capacity(&administrative_publications, &pending.recipients) {
                continue;
            }
            let canonical_administration = action.family == (7 | 0x80);
            if !canonical_administration && !self.has_intent_capacity() {
                continue;
            }
            let Some(player) = players.get(&pending.update.target) else {
                continue;
            };
            let state = if canonical_administration {
                let Some(state) = administrative_states.get(&pending.update.target).cloned() else {
                    continue;
                };
                let ability = abilities.entry(pending.update.target).or_default();
                ability.invulnerable = matches!(state.gamemode, 1 | 3);
                ability.creative = state.gamemode == 1;
                ability.allow_flying = matches!(state.gamemode, 1 | 3);
                ability.flying = state.gamemode == 3 || ability.allow_flying && ability.flying;
                ability.allow_modify_world = matches!(state.gamemode, 0 | 1);
                state
            } else {
                let Some(state) = apply_administrative_controller(
                    player,
                    &pending.update.forward,
                    &mut abilities,
                ) else {
                    continue;
                };
                state
            };
            administrative_states.insert(pending.update.target, state.clone());
            if pending.world_changed {
                let abilities = abilities.entry(pending.update.target).or_default();
                player.send_abilities_snapshot(*abilities);
            }
            let claimed = frames.get(&pending.update.target).map_or_else(
                || initial_claim(player, pending.update.target, tick),
                |frame| frame.claimed,
            );
            let frame = administrative_frame(claimed, &state, tick);
            let intent = controller_actor_intent(
                server,
                player,
                frame,
                ControlChanges {
                    position_changed: true,
                    velocity_changed: true,
                    rotation_changed: true,
                    ..ControlChanges::default()
                },
                ControllerActorSource::AcceptedAdministrative(action, pending.update.tick),
                state.world,
                tick,
                abilities
                    .get(&pending.update.target)
                    .is_some_and(|abilities| abilities.flying),
                Some(state.gamemode),
            );
            if !canonical_administration {
                assert!(self.stage_actor_intent(server, player, intent));
            }
            frames.insert(pending.update.target, frame);
            applied.insert(pending.update.target, tick);
            pending_policy.remove(&pending.update.target);
            administrative_applied.insert(pending.update.target);
            let queued = queue_administrative_publication(
                server,
                &mut administrative_publications,
                pending.recipients.clone(),
                PlayerAdministrativePublication {
                    action,
                    tick: pending.update.tick,
                    target: pending.update.target,
                    origin: pending.update.origin,
                    accepted: true,
                    state,
                },
            );
            assert!(queued);
            pending_administrative.remove(&action);
        }
        let mut policy_applied = HashSet::new();
        super::cluster_player_admin::capture_pending(
            server,
            tick,
            &players,
            &departing,
            &administrative_states,
            &mut administrative_sequences,
            &mut administrative_origins,
            ADMIN_OUTSTANDING_LIMIT,
        );
        let mut policy_results = Vec::new();
        while let Some(result) = self.policy_results.pop() {
            policy_results.push(result);
        }
        for player in players.values() {
            while let Some(result) = player.cluster_controller_policy_retry.pop() {
                policy_results.push(result);
            }
        }
        for mut result in policy_results {
            if pending_policy.get(&result.actor) != Some(&result.generation) {
                continue;
            }
            if result.saturated
                && saturation_last
                    .get(&result.actor)
                    .is_none_or(|last| tick.distance_since(*last) >= 1_200)
            {
                tracing::warn!(
                    actor = result.actor.pack_u32(),
                    "cluster controller policy queue saturated"
                );
                saturation_last.insert(result.actor, tick);
            }
            let Some(player) = players.get(&result.actor) else {
                pending_policy.remove(&result.actor);
                continue;
            };
            if player.cluster_departing.load(Ordering::Acquire) {
                pending_policy.remove(&result.actor);
                continue;
            }
            if result.timed_out {
                pending_policy.remove(&result.actor);
                player.kick(
                    crate::net::DisconnectReason::Unknown,
                    &pumpkin_util::text::TextComponent::text("Controller policy timed out"),
                );
                continue;
            }
            result.frame.effective.tick = tick;
            result.controls.flying = result.controls.flying.map(|value| {
                value
                    && abilities
                        .get(&result.actor)
                        .is_some_and(|state| state.allow_flying)
            });
            let effective_flying = abilities
                .get(&result.actor)
                .is_some_and(|state| state.allow_flying)
                && result.controls.flying.unwrap_or_else(|| {
                    abilities
                        .get(&result.actor)
                        .is_some_and(|abilities| abilities.flying)
                });
            let intent = controller_actor_intent(
                server,
                player,
                result.frame,
                result.controls,
                ControllerActorSource::Client,
                super::cluster_world_delta::world_id(&player.world()),
                tick,
                effective_flying,
                None,
            );
            if !self.stage_actor_intent(server, player, intent) {
                player.kick(
                    crate::net::DisconnectReason::Unknown,
                    &pumpkin_util::text::TextComponent::text("Controller actor queue saturated"),
                );
                pending_policy.remove(&result.actor);
                continue;
            }
            publish_controller_correction(server, player, &result.frame);
            if let Some(value) = result.controls.flying {
                let state = abilities.entry(result.actor).or_default();
                state.flying = state.allow_flying && value;
                player.send_abilities_snapshot(*state);
            }
            update_administrative_controller(
                administrative_states.get_mut(&result.actor),
                player,
                &result.frame,
            );
            frames.insert(result.actor, result.frame);
            applied.insert(result.actor, tick);
            pending_policy.remove(&result.actor);
            policy_applied.insert(result.actor);
        }
        for (actor, player) in &players {
            if !frames.contains_key(actor) {
                continue;
            }
            if let Some((source_tick, request, mut input)) = retained_respawn_inputs.remove(actor) {
                if input.is_none() {
                    input = match request {
                        RetainedLifecycleRequest::Respawn => {
                            if server
                                .cluster_actor_living
                                .load()
                                .get(&pumpkin_cluster::actor::ActorId::Player(*actor))
                                .is_some_and(|state| state.combat.health_milli != 0)
                            {
                                continue;
                            }
                            capture_respawn_proposal(
                                server,
                                player,
                                tick,
                                action_sequences.entry(*actor).or_insert(0),
                            )
                        }
                        RetainedLifecycleRequest::Dismount { epoch, birth, uuid } => {
                            let living = server.cluster_actor_living.load();
                            if living.get(&pumpkin_cluster::actor::ActorId::Player(*actor))
                                .and_then(|state| state.avatar.as_ref()).is_none_or(|avatar|
                                    avatar.spatial_epoch != epoch || !matches!(avatar.mount.as_ref(),
                                        Some(pumpkin_cluster::item_contact::ContactIdentity::Entity { birth: mounted, .. })
                                            if mounted.action == birth && mounted.uuid == uuid)) { continue; }
                            super::cluster_vehicle_control::capture_dismount_proposal(
                                server,
                                player,
                                source_tick,
                                action_sequences.entry(*actor).or_insert(0),
                            )
                        }
                        RetainedLifecycleRequest::Wake {
                            epoch,
                            sleeping,
                            morning,
                        } => {
                            let living = server.cluster_actor_living.load();
                            if living
                                .get(&pumpkin_cluster::actor::ActorId::Player(*actor))
                                .and_then(|state| state.avatar.as_ref())
                                .is_none_or(|avatar| {
                                    avatar.spatial_epoch != epoch
                                        || avatar.presentation.sleeping != Some(sleeping)
                                })
                            {
                                continue;
                            }
                            capture_wake_proposal(
                                server,
                                player,
                                source_tick,
                                action_sequences.entry(*actor).or_insert(0),
                                morning,
                            )
                        }
                    };
                }
                let Some(proposal) = input else {
                    retained_respawn_inputs.insert(*actor, (source_tick, request, None));
                    continue;
                };
                if super::cluster_world_owner::submit_action(
                    server,
                    pumpkin_cluster::canonical_action::ActionInput::Proposal(proposal.clone()),
                )
                .is_err()
                {
                    retained_respawn_inputs.insert(*actor, (source_tick, request, Some(proposal)));
                    continue;
                }
            }
            if let Some(lease) = super::cluster_world_owner::player_action_lease(server, player)
                && let Some(living) = server
                    .cluster_actor_living
                    .load()
                    .get(&pumpkin_cluster::actor::ActorId::Player(*actor))
            {
                use pumpkin_cluster::living_property::LivingProperty;
                let versions = [
                    LivingProperty::VelocityX,
                    LivingProperty::VelocityY,
                    LivingProperty::VelocityZ,
                ]
                .map(|property| {
                    living
                        .property_versions
                        .get(&property)
                        .copied()
                        .unwrap_or(0)
                });
                let cut = (lease, versions, living.combat.velocity_micro);
                if velocity_cuts
                    .insert(*actor, cut)
                    .is_some_and(|prior| prior.0 == lease && prior != cut)
                    && let Some(frame) = frames.get_mut(actor)
                {
                    frame.effective.velocity = cut.2.map(|value| value as f64 / 1_000_000.0);
                }
            }
            if let Some(input) = retained_vehicle_inputs.remove(actor) {
                if let Err(input) = try_send_vehicle_input(server, input) {
                    retained_vehicle_inputs.insert(*actor, input);
                    continue;
                }
            }
            if let Some((change, original_tick)) = retained_raw_cosmetic_inputs.remove(actor) {
                let Some(input) = super::cluster_remote_player_presentation::capture_cosmetic(
                    server,
                    player,
                    change,
                    original_tick,
                ) else {
                    retained_raw_cosmetic_inputs.insert(*actor, (change, original_tick));
                    continue;
                };
                if let Err(input) =
                    super::cluster_remote_player_presentation::try_send_cosmetic(server, input)
                {
                    retained_cosmetic_inputs.insert(*actor, input);
                    continue;
                }
            }
            if let Some(input) = retained_cosmetic_inputs.remove(actor) {
                if let Err(input) =
                    super::cluster_remote_player_presentation::try_send_cosmetic(server, input)
                {
                    retained_cosmetic_inputs.insert(*actor, input);
                    continue;
                }
            }
            let dropped = player.cluster_controller_dropped.swap(0, Ordering::Relaxed);
            if dropped != 0
                && saturation_last
                    .get(actor)
                    .is_none_or(|last| tick.distance_since(*last) >= 1_200)
            {
                tracing::warn!(
                    actor = actor.pack_u32(),
                    dropped,
                    "cluster controller input queue saturated"
                );
                saturation_last.insert(*actor, tick);
            }
            if administrative_applied.contains(actor) {
                crate::server::cluster_inventory_protocol::drain_inventory_inputs(
                    server,
                    player,
                    tick,
                    action_sequences.entry(*actor).or_insert(0),
                    effective_item_frame(server, &frames, *actor, tick),
                    book_preflights.entry(*actor).or_default(),
                    active_mining.entry(*actor).or_default(),
                );
                continue;
            }
            if !player.cluster_departing.load(Ordering::Acquire)
                && !chunk_deliveries
                    .get(actor)
                    .is_some_and(|delivery| delivery.active && delivery.bootstrap_complete)
            {
                continue;
            }
            let mut claimed = frames.get(actor).map_or_else(
                || initial_claim(player, *actor, tick),
                |frame| frame.claimed,
            );
            let mut controls = ControlChanges::default();
            let mut changed = false;
            let mut invalid = false;
            if pending_policy.contains_key(actor) {
                continue;
            }
            if policy_applied.contains(actor) {
                crate::server::cluster_inventory_protocol::drain_inventory_inputs(
                    server,
                    player,
                    tick,
                    action_sequences.entry(*actor).or_insert(0),
                    effective_item_frame(server, &frames, *actor, tick),
                    book_preflights.entry(*actor).or_default(),
                    active_mining.entry(*actor).or_default(),
                );
                continue;
            }

            while let Some((patch, resumed)) = retained_controller_patches
                .remove(actor)
                .map(|patch| (patch, true))
                .or_else(|| {
                    player
                        .cluster_controller_inputs
                        .pop()
                        .map(|patch| (patch, false))
                })
            {
                if !resumed && matches!(patch, ControllerPatch::Sneak(true)) {
                    let living = server.cluster_actor_living.load();
                    if let Some(avatar) = living
                        .get(&pumpkin_cluster::actor::ActorId::Player(*actor))
                        .and_then(|state| state.avatar.as_ref())
                        && let Some(pumpkin_cluster::item_contact::ContactIdentity::Entity {
                            birth,
                            ..
                        }) = avatar.mount.as_ref()
                    {
                        let request = RetainedLifecycleRequest::Dismount {
                            epoch: avatar.spatial_epoch,
                            birth: birth.action,
                            uuid: birth.uuid,
                        };
                        let proposal = super::cluster_vehicle_control::capture_dismount_proposal(
                            server,
                            player,
                            tick,
                            action_sequences.entry(*actor).or_insert(0),
                        );
                        if proposal.as_ref().is_none_or(|proposal| {
                            super::cluster_world_owner::submit_action(
                                server,
                                pumpkin_cluster::canonical_action::ActionInput::Proposal(
                                    proposal.clone(),
                                ),
                            )
                            .is_err()
                        }) {
                            retained_respawn_inputs.insert(*actor, (tick, request, proposal));
                            retained_controller_patches.insert(*actor, patch);
                            break;
                        }
                    }
                }
                if matches!(
                    patch,
                    ControllerPatch::Wake | ControllerPatch::WakeMorning(_)
                ) {
                    let morning = match patch {
                        ControllerPatch::WakeMorning(value) => Some(value),
                        _ => None,
                    };
                    let living = server.cluster_actor_living.load();
                    let Some(avatar) = living
                        .get(&pumpkin_cluster::actor::ActorId::Player(*actor))
                        .and_then(|state| state.avatar.as_ref())
                    else {
                        continue;
                    };
                    let Some(sleeping) = avatar.presentation.sleeping else {
                        continue;
                    };
                    let request = RetainedLifecycleRequest::Wake {
                        epoch: avatar.spatial_epoch,
                        sleeping,
                        morning,
                    };
                    let Some(proposal) = capture_wake_proposal(
                        server,
                        player,
                        tick,
                        action_sequences.entry(*actor).or_insert(0),
                        morning,
                    ) else {
                        retained_respawn_inputs.insert(*actor, (tick, request, None));
                        break;
                    };
                    if super::cluster_world_owner::submit_action(
                        server,
                        pumpkin_cluster::canonical_action::ActionInput::Proposal(proposal.clone()),
                    )
                    .is_err()
                    {
                        retained_respawn_inputs.insert(*actor, (tick, request, Some(proposal)));
                        break;
                    }
                    continue;
                }
                if matches!(patch, ControllerPatch::Respawn) {
                    if server
                        .cluster_actor_living
                        .load()
                        .get(&pumpkin_cluster::actor::ActorId::Player(*actor))
                        .is_some_and(|state| state.combat.health_milli != 0)
                    {
                        continue;
                    }
                    let Some(proposal) = capture_respawn_proposal(
                        server,
                        player,
                        tick,
                        action_sequences.entry(*actor).or_insert(0),
                    ) else {
                        retained_respawn_inputs
                            .insert(*actor, (tick, RetainedLifecycleRequest::Respawn, None));
                        break;
                    };
                    if super::cluster_world_owner::submit_action(
                        server,
                        pumpkin_cluster::canonical_action::ActionInput::Proposal(proposal.clone()),
                    )
                    .is_err()
                    {
                        retained_respawn_inputs.insert(
                            *actor,
                            (tick, RetainedLifecycleRequest::Respawn, Some(proposal)),
                        );
                        break;
                    }
                    continue;
                }
                if let ControllerPatch::Cosmetic(change) = patch {
                    let Some(input) = super::cluster_remote_player_presentation::capture_cosmetic(
                        server, player, change, tick,
                    ) else {
                        retained_raw_cosmetic_inputs.insert(*actor, (change, tick));
                        break;
                    };
                    if let Err(input) =
                        super::cluster_remote_player_presentation::try_send_cosmetic(server, input)
                    {
                        retained_cosmetic_inputs.insert(*actor, input);
                        break;
                    }
                    continue;
                }
                if matches!(
                    patch,
                    ControllerPatch::Vehicle { .. }
                        | ControllerPatch::Paddles { .. }
                        | ControllerPatch::VehicleFlags { .. }
                ) {
                    let Some(input) = capture_vehicle_input(
                        server,
                        player,
                        frames.get(actor).unwrap(),
                        patch,
                        tick,
                        action_sequences.entry(*actor).or_insert(0),
                    ) else {
                        continue;
                    };
                    if let Err(input) = try_send_vehicle_input(server, input) {
                        retained_vehicle_inputs.insert(*actor, input);
                        break;
                    }
                    continue;
                }
                if let ControllerPatch::TeleportConfirm(id) = patch {
                    match awaiting_teleport.remove(actor) {
                        Some((expected, position)) if expected == id => {
                            claimed.position = [position.x, position.y, position.z];
                            claimed.tick = tick;
                            changed = true;
                        }
                        _ => player.kick(
                            crate::net::DisconnectReason::Unknown,
                            &pumpkin_util::text::TextComponent::text("Wrong teleport id"),
                        ),
                    }
                    continue;
                }
                if !player.has_client_loaded()
                    || living_publication
                        .get(&pumpkin_cluster::actor::ActorId::Player(*actor))
                        .is_none_or(|living| living.combat.health_milli == 0)
                    || player.is_movement_locked.load(Ordering::Relaxed)
                {
                    continue;
                }
                if let ControllerPatch::Bedrock { flags, .. } = patch
                    && flags & (1u64 << pumpkin_protocol::bedrock::server::player_auth_input::InputData::HandledTeleport as u8) != 0 {
                    awaiting_bedrock_teleport.remove(actor);
                }
                if patch.moves()
                    && ((matches!(player.client.as_ref(), crate::net::ClientPlatform::Java(_))
                        && frame_epochs.get(actor) != projected_epochs.get(actor))
                        || awaiting_bedrock_teleport.contains(actor)
                        || awaiting_teleport.contains_key(actor)
                        || living_publication
                            .get(&pumpkin_cluster::actor::ActorId::Player(*actor))
                            .and_then(|living| living.avatar.as_ref())
                            .is_some_and(|avatar| avatar.mount.is_some()))
                {
                    if let Some((next, claimed_controls)) = apply_patch(claimed, patch, tick) {
                        claimed = next;
                        controls.input_changed |= claimed_controls.input_changed;
                        controls.rotation_changed |= !awaiting_teleport.contains_key(actor)
                            && !awaiting_bedrock_teleport.contains(actor)
                            && (!matches!(
                                player.client.as_ref(),
                                crate::net::ClientPlatform::Java(_)
                            ) || frame_epochs.get(actor) == projected_epochs.get(actor))
                            && claimed_controls.rotation_changed;
                        changed = true;
                    }
                    continue;
                }
                let Some((next, next_controls)) = apply_patch(claimed, patch, tick) else {
                    player.kick(
                        crate::net::DisconnectReason::Unknown,
                        &pumpkin_util::text::TextComponent::text("Invalid player movement"),
                    );
                    invalid = true;
                    break;
                };
                claimed = next;
                controls.merge(next_controls);
                changed = true;
            }
            if invalid {
                continue;
            }
            if retained_vehicle_inputs.contains_key(actor)
                || retained_respawn_inputs.contains_key(actor)
                || retained_controller_patches.contains_key(actor)
                || retained_raw_cosmetic_inputs.contains_key(actor)
                || retained_cosmetic_inputs.contains_key(actor)
            {
                continue;
            }
            let mounted = mounted_controller_snapshot(server, player);
            if let Some((position, velocity, ground, _, correction)) = &mounted {
                changed |= frames.get(actor).is_some_and(|frame| {
                    frame.effective.position != *position
                        || frame.effective.velocity != *velocity
                        || frame.effective.on_ground != *ground
                });
                changed |= correction
                    .as_ref()
                    .is_some_and(|(key, _)| vehicle_corrections.get(actor) != Some(key));
                claimed.tick = tick;
            }
            if !changed {
                if let Some(frame) = frames.get(actor).copied() {
                    let world = administrative_states.get(actor).map_or_else(
                        || super::cluster_world_delta::world_id(&player.world()),
                        |state| state.world,
                    );
                    let intent = controller_actor_intent(
                        server,
                        player,
                        frame,
                        ControlChanges::default(),
                        ControllerActorSource::Client,
                        world,
                        tick,
                        abilities.get(actor).is_some_and(|ability| ability.flying),
                        None,
                    );
                    let view = controller_view_intent(server, player, intent);
                    if server
                        .cluster_chunk_watches
                        .load()
                        .get(actor)
                        .is_some_and(|watch| {
                            watch.view.spectator_generates != view.spectator_generates
                        })
                        && !self.view_intents.is_full()
                    {
                        assert!(self.view_intents.push(view).is_ok());
                    }
                }
                crate::server::cluster_inventory_protocol::drain_inventory_inputs(
                    server,
                    player,
                    tick,
                    action_sequences.entry(*actor).or_insert(0),
                    effective_item_frame(server, &frames, *actor, tick),
                    book_preflights.entry(*actor).or_default(),
                    active_mining.entry(*actor).or_default(),
                );
                continue;
            }
            #[cfg(feature = "cluster-e2e")]
            let mut offset = TestOffset(offsets.get(actor).copied());
            #[cfg(feature = "cluster-e2e")]
            let mut filters: [&mut dyn ControllerFilter; 1] = [&mut offset];
            #[cfg(not(feature = "cluster-e2e"))]
            let mut filters: [&mut dyn ControllerFilter; 0] = [];
            let mut frame = apply_filters(claimed, &mut filters);
            let previous = frames
                .get(actor)
                .map_or(frame.effective, |frame| frame.effective);
            preserve_unclaimed_fields(&mut frame, &controls, previous);
            if mounted.is_none() {
                filter_readonly_player_collision(server, &mut frame, previous);
            }
            if let Some((position, velocity, on_ground, _, correction)) = &mounted {
                frame.effective.position = *position;
                frame.effective.velocity = *velocity;
                frame.effective.on_ground = *on_ground;
                frame.decision = ControllerDecision::Modified(8);
                if let Some((key, spatial)) = correction {
                    if vehicle_corrections.get(actor) != Some(key) {
                        if let crate::net::ClientPlatform::Java(client) = player.client.as_ref() {
                            let position = spatial.position.map(f64::from_bits);
                            client.try_send_packet(
                                &pumpkin_protocol::java::client::play::CMoveVehicle::new(
                                    position[0],
                                    position[1],
                                    position[2],
                                    f32::from_bits(spatial.yaw),
                                    f32::from_bits(spatial.pitch),
                                ),
                            );
                        }
                        vehicle_corrections.insert(*actor, *key);
                    }
                }
            }
            let position = Vector3::new(
                previous.position[0],
                previous.position[1],
                previous.position[2],
            );
            controls.position_changed |= frame.effective.position != previous.position;
            controls.velocity_changed |= frame.effective.velocity != previous.velocity;
            controls.rotation_changed |=
                frame.effective.yaw != previous.yaw || frame.effective.pitch != previous.pitch;
            controls.ground_changed |= frame.effective.on_ground != previous.on_ground;
            if mounted.is_none() && requires_policy(server, &frame, &controls, previous.position) {
                next_policy_generation = next_policy_generation.wrapping_add(1);
                pending_policy.insert(*actor, next_policy_generation);
                spawn_policy(
                    server.clone(),
                    player.clone(),
                    next_policy_generation,
                    position,
                    frame,
                    controls,
                    frozen_movement_statistic(
                        server,
                        player,
                        &frame,
                        &controls,
                        abilities
                            .get(actor)
                            .is_some_and(|abilities| abilities.flying),
                    ),
                    previous,
                    self.policy_results.clone(),
                );
                continue;
            }
            controls.flying = controls
                .flying
                .map(|value| value && abilities.get(actor).is_some_and(|state| state.allow_flying));
            let effective_flying = abilities.get(actor).is_some_and(|state| state.allow_flying)
                && controls.flying.unwrap_or_else(|| {
                    abilities
                        .get(actor)
                        .is_some_and(|abilities| abilities.flying)
                });
            let intent = controller_actor_intent(
                server,
                player,
                frame,
                controls,
                ControllerActorSource::Client,
                mounted.as_ref().map_or_else(
                    || super::cluster_world_delta::world_id(&player.world()),
                    |(_, _, _, world, _)| *world,
                ),
                tick,
                effective_flying,
                None,
            );
            if !self.stage_actor_intent(server, player, intent) {
                player.kick(
                    crate::net::DisconnectReason::Unknown,
                    &pumpkin_util::text::TextComponent::text("Controller actor queue saturated"),
                );
                continue;
            }
            if mounted.is_none() {
                publish_controller_correction(server, player, &frame);
            }
            if let Some(value) = controls.flying {
                let state = abilities.entry(*actor).or_default();
                state.flying = state.allow_flying && value;
                player.send_abilities_snapshot(*state);
            }
            update_administrative_controller(administrative_states.get_mut(actor), player, &frame);
            #[cfg(feature = "cluster-e2e")]
            if matches!(frame.decision, ControllerDecision::Modified(_))
                && logged_divergence.insert(*actor)
            {
                tracing::info!(
                    actor = frame.claimed.actor.pack_u32(),
                    claimed_x = frame.claimed.position[0],
                    claimed_y = frame.claimed.position[1],
                    claimed_z = frame.claimed.position[2],
                    effective_x = frame.effective.position[0],
                    effective_y = frame.effective.position[1],
                    effective_z = frame.effective.position[2],
                    "cluster test controller divergence applied"
                );
            }
            frames.insert(*actor, frame);
            applied.insert(*actor, tick);
            crate::server::cluster_inventory_protocol::drain_inventory_inputs(
                server,
                player,
                tick,
                action_sequences.entry(*actor).or_insert(0),
                effective_item_frame(server, &frames, *actor, tick),
                book_preflights.entry(*actor).or_default(),
                active_mining.entry(*actor).or_default(),
            );
        }
        if let Some(results) = admission_results.as_mut() {
            while let Ok(result) = results.try_recv() {
                admission_outcomes.insert(result.actor, result);
            }
        }
        while let Some(request) = self.departure_requests.pop() {
            pending_departure_requests.push_back(request);
        }
        for _ in 0..pending_departure_requests.len() {
            let Some(mut request) = pending_departure_requests.pop_front() else {
                break;
            };
            if request.lease.is_none() {
                let exact = players.get(&request.actor).is_some_and(|player| {
                    Arc::ptr_eq(player, &request.player)
                        && player
                            .cluster_owner_install_generation
                            .load(Ordering::Acquire)
                            == request.install_generation
                });
                if !exact {
                    let _ = request.respond.send(DepartureStatus::MissingPlayer);
                    continue;
                }
                if let Some(result) = admission_outcomes.get(&request.actor)
                    && result.key == request.player.gameprofile.id
                    && result.install_generation == request.install_generation
                    && result.client_entity_id == request.player.living_entity.entity.entity_id
                    && !result.accepted
                {
                    if request
                        .player
                        .world()
                        .cancel_rejected_cluster_player_registry(&request.player, result)
                    {
                        admission_outcomes.remove(&request.actor);
                        let _ = request.respond.send(DepartureStatus::MissingPlayer);
                    } else {
                        pending_departure_requests.push_back(request);
                    }
                    continue;
                }
                let accepted = admission_outcomes
                    .get(&request.actor)
                    .filter(|result| {
                        result.accepted
                            && result.key == request.player.gameprofile.id
                            && result.install_generation == request.install_generation
                            && result.client_entity_id
                                == request.player.living_entity.entity.entity_id
                    })
                    .and_then(|result| result.lease);
                if accepted.is_none()
                    || super::cluster_world_owner::player_action_lease(server, &request.player)
                        != accepted
                {
                    pending_departure_requests.push_back(request);
                    continue;
                }
                request.lease = accepted;
            }
            let lease = request.lease.expect("resolved departure admission lease");
            if !players.get(&request.actor).is_some_and(|player| {
                Arc::ptr_eq(player, &request.player)
                    && super::cluster_world_owner::player_action_lease(server, player)
                        == Some(lease)
                    && player
                        .cluster_owner_install_generation
                        .load(std::sync::atomic::Ordering::Acquire)
                        == request.install_generation
            }) {
                let _ = request.respond.send(DepartureStatus::MissingPlayer);
            } else if departing.contains(&request.actor) {
                let _ = request.respond.send(DepartureStatus::Duplicate);
            } else {
                departing.insert(request.actor);
                departures.insert(
                    request.actor,
                    DepartureTicket {
                        retention_need: None,
                        retention_complete: false,
                        retention_exact: false,
                        watch_retired: false,
                        retention_reported: false,
                        watch_retire_reported: false,
                        player: request.player,
                        retained: request.retained,
                        frozen: None,
                        local_snapshot: None,
                        commit_token: None,
                        retired: false,
                        registry_removed: false,
                        final_error_reported: false,
                        source: None,
                        lease,
                        install_generation: request.install_generation,
                        respond: request.respond,
                        expected_close: request.expected_close,
                        close_staged: false,
                        close_stage_reported: false,
                        logout: request.logout,
                        world: None,
                        world_ready: false,
                        inventory: None,
                        through_tick: tick,
                    },
                );
            }
        }
        let mut departures_ready = Vec::new();
        let mut departures_stale = Vec::new();
        for (actor, departure) in &mut departures {
            if departure.frozen.is_some() {
                departures_ready.push(*actor);
                continue;
            }
            if !departure.close_staged {
                let Some(player) = players.get(actor).filter(|player| {
                    super::cluster_world_owner::player_action_lease(server, player)
                        == Some(departure.lease)
                        && player
                            .cluster_owner_install_generation
                            .load(Ordering::Acquire)
                            == departure.install_generation
                }) else {
                    departures_stale.push(*actor);
                    continue;
                };
                match super::cluster_inventory_protocol::stage_departure_screen_close_any(
                    player,
                    departure.expected_close,
                ) {
                    Ok(()) => departure.close_staged = true,
                    Err(error) => {
                        if !departure.close_stage_reported {
                            tracing::error!(
                                ?actor,
                                ?error,
                                token = departure.expected_close,
                                "cluster departure screen close deferred"
                            );
                            departure.close_stage_reported = true;
                        }
                        continue;
                    }
                }
            }
            active_mining.remove(actor);
            let packet_preflight_complete = players
                .get(actor)
                .is_some_and(|player| player.departing_packet_preflight_complete());
            let world_ready_on_prior_advance = departure.world_ready;
            if !departure.world_ready {
                if departure.world.is_none()
                    && packet_preflight_complete
                    && !retained_command_proposals.iter().any(|proposal| {
                        proposal.command.as_ref().is_some_and(|(_, _, request)| {
                            request
                                .player_target()
                                .is_some_and(|(target, _)| target == *actor)
                        })
                    })
                    && !retained_player_commands.iter().any(|command| {
                        command
                            .request
                            .player_target()
                            .is_some_and(|(target, _)| target == *actor)
                    })
                    && players.get(actor).is_some_and(|player| {
                        player.cluster_inventory_inputs.is_empty()
                            && player.cluster_inventory_pending_close.is_empty()
                            && player.cluster_inventory_departure_close.is_empty()
                            && !retained_vehicle_inputs.contains_key(actor)
                            && !retained_respawn_inputs.contains_key(actor)
                            && !retained_controller_patches.contains_key(actor)
                            && !retained_raw_cosmetic_inputs.contains_key(actor)
                            && !retained_cosmetic_inputs.contains_key(actor)
                            && player.cluster_session_staged.is_empty()
                    })
                    && !session_progress
                        .get(actor)
                        .is_some_and(session_progress_pending)
                {
                    departure.world = Self::try_request_actor_drain(
                        server,
                        *actor,
                        departure.lease,
                        departure.install_generation,
                        tick,
                        departure.logout,
                    );
                    if departure.world.is_some() {
                        departure.through_tick = tick;
                    }
                }
                if let Some(world) = departure.world.as_mut() {
                    match world.try_recv() {
                        Ok(super::cluster_world_owner::ActorDrainStatus::Ready {
                            player_inventory,
                            ender_inventory,
                        }) => {
                            departure.inventory = Some((player_inventory, ender_inventory));
                            departure.world_ready = true;
                        }
                        Ok(super::cluster_world_owner::ActorDrainStatus::StalePlayer) => {
                            departures_stale.push(*actor);
                            continue;
                        }
                        Err(oneshot::error::TryRecvError::Closed) => departure.world = None,
                        Err(oneshot::error::TryRecvError::Empty) => {}
                    }
                }
            }
            let local_pending = retained_command_proposals.iter().any(|proposal| {
                proposal.command.as_ref().is_some_and(|(_, _, request)| {
                    request
                        .player_target()
                        .is_some_and(|(target, _)| target == *actor)
                })
            }) || administrative_origins
                .keys()
                .any(|action| action.actor == (*actor).into())
                || pending_administrative
                    .values()
                    .any(|pending| pending.update.target == *actor)
                || deferred_administrative
                    .iter()
                    .any(|resolution| resolution.update.target == *actor);
            let book_pending = book_preflights.get(actor).is_some_and(Option::is_some);
            let session_pending = session_progress
                .get(actor)
                .is_some_and(session_progress_pending);
            let player_pending = players.get(actor).is_some_and(|player| {
                !player.cluster_administrative_inputs.is_empty()
                    || !player.cluster_administrative_results.is_empty()
                    || !player.cluster_administrative_pending_result.is_empty()
                    || !player.cluster_inventory_inputs.is_empty()
                    || !player.cluster_inventory_pending_close.is_empty()
                    || !player.cluster_inventory_departure_close.is_empty()
                    || retained_vehicle_inputs.contains_key(actor)
                    || retained_respawn_inputs.contains_key(actor)
                    || retained_controller_patches.contains_key(actor)
                    || retained_raw_cosmetic_inputs.contains_key(actor)
                    || retained_cosmetic_inputs.contains_key(actor)
                    || !player.cluster_session_staged.is_empty()
            });
            let close_accepted = server
                .cluster_inventory_close_status
                .load()
                .get(&(*actor, departure.expected_close))
                .is_some_and(|status| status.token == departure.expected_close);
            if world_ready_on_prior_advance
                && packet_preflight_complete
                && !server.cluster_player_admin.has_queued_resolutions()
                && !local_pending
                && !book_pending
                && !session_pending
                && !player_pending
                && close_accepted
            {
                departures_ready.push(*actor);
            }
        }
        for actor in departures_stale {
            if let Some(ticket) = departures.remove(&actor) {
                let _ = ticket.respond.send(DepartureStatus::MissingPlayer);
            }
        }
        let departure_signals: Vec<_> = departures_ready
            .into_iter()
            .filter_map(|actor| {
                let departure = departures.get_mut(&actor)?;
                let player = departure.player.clone();
                if !departure.retention_complete {
                    let chunks = departure
                        .retained
                        .as_ref()
                        .map_or(&[][..], |retained| retained.chunks.as_slice());
                    let expires = std::time::SystemTime::now()
                        .duration_since(std::time::UNIX_EPOCH)
                        .map_or(30_000, |elapsed| {
                            (elapsed.as_millis().min(u128::from(u64::MAX)) as u64)
                                .saturating_add(30_000)
                        });
                    if !departure.retention_exact
                        && departure.retention_need.is_none()
                        && !chunks.is_empty()
                    {
                        next_chunk_lease = next_chunk_lease.max(1_u64 << 61).wrapping_add(1);
                        departure.retention_need =
                            super::cluster::try_require_chunk_set(server, chunks, next_chunk_lease);
                        if departure.retention_need.is_none() {
                            if !departure.retention_reported {
                                tracing::warn!(
                                    ?actor,
                                    lease = departure.lease.incarnation,
                                    chunks = chunks.len(),
                                    "cluster retirement chunk retention deferred"
                                );
                                departure.retention_reported = true;
                            }
                            return None;
                        }
                    }
                    if let Some(need) = departure.retention_need.take()
                        && let Err(need) =
                            super::cluster::try_retain_chunk_need(server, need, expires)
                    {
                        departure.retention_need = Some(need);
                        if !departure.retention_reported {
                            tracing::warn!(
                                ?actor,
                                lease = departure.lease.incarnation,
                                "cluster retirement chunk retention deferred"
                            );
                            departure.retention_reported = true;
                        }
                        return None;
                    }
                    departure.retention_exact = true;
                    if let Some(delivery) = chunk_deliveries.get_mut(&actor)
                        && let Some(need) = delivery.need.take()
                        && let Err(need) =
                            super::cluster::try_retain_chunk_need(server, need, expires)
                    {
                        delivery.need = Some(need);
                        if !departure.retention_reported {
                            tracing::warn!(
                                ?actor,
                                lease = departure.lease.incarnation,
                                "cluster retirement chunk retention deferred"
                            );
                            departure.retention_reported = true;
                        }
                        return None;
                    }
                    departure.retention_complete = true;
                }
                if departure.frozen.is_none() {
                    let lease = super::cluster_world_owner::player_action_lease(server, &player)?;
                    if departure.lease != lease
                        || player
                            .cluster_owner_install_generation
                            .load(Ordering::Acquire)
                            != departure.install_generation
                    {
                        return None;
                    }
                    let spatial = server.cluster_actor_spatial.load().get(&actor)?.clone();
                    let living = server
                        .cluster_actor_living
                        .load()
                        .get(&actor.into())?
                        .clone();
                    let avatar = living.avatar.as_ref()?;
                    if spatial.local_epoch != avatar.spatial_epoch {
                        return None;
                    }
                    if let pumpkin_cluster::canonical_action::SpatialEpoch::Admission {
                        lease: admitted,
                        ..
                    } = avatar.spatial_epoch
                        && admitted != lease
                    {
                        return None;
                    }
                    let (player_inventory, ender_inventory) = departure.inventory.clone()?;
                    let world = server
                        .worlds
                        .load()
                        .iter()
                        .find(|world| {
                            super::cluster_world_delta::world_id(world) == spatial.truth.local.world
                        })
                        .cloned()?;
                    let frame = spatial.truth.local.frame.effective;
                    departure.source = Some(crate::command::CommandSource::new(
                        crate::command::CommandSender::Player(player.clone()),
                        world,
                        Some(player.clone()),
                        Vector3::new(frame.position[0], frame.position[1], frame.position[2]),
                        pumpkin_util::math::vector2::Vector2::new(frame.yaw, frame.pitch),
                        player.gameprofile.name.clone(),
                        pumpkin_util::text::TextComponent::text(player.gameprofile.name.clone()),
                        server.clone(),
                    ));
                    departure.frozen = Some(FrozenPlayerDeparture {
                        lease,
                        install_generation: departure.install_generation,
                        living,
                        spatial: spatial.truth.local,
                        player_inventory,
                        ender_inventory,
                    });
                }
                let frozen = departure.frozen.as_ref()?;
                if departure.local_snapshot.is_none() {
                    departure.local_snapshot = super::cluster_lobby::local_snapshot_from_player(
                        &player,
                        server,
                        frozen,
                        departure.retained.as_ref(),
                    );
                    if departure.local_snapshot.is_none() {
                        if !departure.final_error_reported {
                            tracing::error!(
                                ?actor,
                                "cluster frozen retirement snapshot unavailable"
                            );
                            departure.final_error_reported = true;
                        }
                        return None;
                    }
                }
                if departure.commit_token.is_none() {
                    let (chunks, until) = departure
                        .retained
                        .as_ref()
                        .map_or((&[][..], 0), |retained| {
                            (retained.chunks.as_slice(), retained.retained_until)
                        });
                    match super::cluster_handoff::try_commit_logout(
                        server,
                        &player,
                        chunks,
                        until,
                        departure.logout,
                        frozen,
                    ) {
                        Ok(token) => departure.commit_token = Some(token),
                        Err(super::cluster_handoff::RetirementCommitError::Capacity) => {
                            return None;
                        }
                        Err(error) => {
                            if !departure.final_error_reported {
                                tracing::error!(
                                    ?actor,
                                    ?error,
                                    "cluster frozen retirement deferred"
                                );
                                departure.final_error_reported = true;
                            }
                            return None;
                        }
                    }
                }
                if !departure.retired {
                    if !super::cluster_player_lifecycle::retired(
                        server,
                        &player,
                        departure.logout,
                        departure.source.clone()?,
                    ) {
                        return None;
                    }
                    departure.retired = true;
                }
                if !departure.watch_retired {
                    let Some(route) = server.cluster_chunk_watch_route.load_full() else {
                        if !departure.watch_retire_reported {
                            tracing::warn!(
                                ?actor,
                                lease = departure.lease.incarnation,
                                "cluster retirement watch route unavailable"
                            );
                            departure.watch_retire_reported = true;
                        }
                        return None;
                    };
                    let Ok(permit) = route.try_reserve() else {
                        if !departure.watch_retire_reported {
                            tracing::warn!(
                                ?actor,
                                lease = departure.lease.incarnation,
                                "cluster retirement watch publication deferred"
                            );
                            departure.watch_retire_reported = true;
                        }
                        return None;
                    };
                    let retained = departure.retained.as_ref().map(|retained| {
                        super::cluster_presence::RetainedDeparture {
                            chunks: retained.chunks.clone(),
                            retained_until: retained.retained_until,
                        }
                    });
                    permit.send(super::cluster_chunk_watch::WatchCommand::Retire {
                        install_generation: departure.install_generation,
                        actor,
                        session: departure.lease.incarnation,
                        retained,
                    });
                    departure.watch_retired = true;
                }
                if !departure.registry_removed {
                    if !player.remove_cluster_after_departure(frozen) {
                        if !departure.final_error_reported {
                            tracing::error!(
                                ?actor,
                                "cluster frozen retirement registry proof failed"
                            );
                            departure.final_error_reported = true;
                        }
                        return None;
                    }
                    departure.registry_removed = true;
                }
                let mut snapshot = departure.local_snapshot.take()?;
                snapshot.commit_token = departure.commit_token;
                active_mining.remove(&actor);
                let respond = departures.remove(&actor)?.respond;
                server.cluster_inventory_close_status.rcu(|current| {
                    let mut next = (**current).clone();
                    next.retain(|(entry_actor, _), _| *entry_actor != actor);
                    Arc::new(next)
                });
                Some((respond, snapshot))
            })
            .collect();
        self.published.store(Arc::new(ControllerSnapshot {
            frames: frames.clone(),
            applied: applied.clone(),
            installed: players
                .iter()
                .filter_map(|(gid, player)| {
                    if !chunk_deliveries.contains_key(gid)
                        || player.cluster_departing.load(Ordering::Acquire)
                    {
                        return None;
                    }
                    let lease = super::cluster_world_owner::player_action_lease(server, player)?;
                    let spatial = server.cluster_actor_spatial.load();
                    let spatial = spatial.get(gid)?;
                    let living = server.cluster_actor_living.load();
                    let avatar = living.get(&pumpkin_cluster::actor::ActorId::Player(*gid))?.avatar.as_ref()?;
                    if avatar.spatial_epoch != spatial.local_epoch
                        || matches!(avatar.spatial_epoch, pumpkin_cluster::canonical_action::SpatialEpoch::Admission { lease: installed, .. } if installed != lease) {
                        return None;
                    }
                    Some((
                        *gid,
                        (
                            lease,
                            player
                                .cluster_owner_install_generation
                                .load(Ordering::Acquire),
                        ),
                    ))
                })
                .collect(),
            active: players
                .iter()
                .filter_map(|(gid, player)| {
                    if !chunk_deliveries
                        .get(gid)
                        .is_some_and(|delivery| delivery.active && delivery.bootstrap_complete)
                        || player.cluster_departing.load(Ordering::Acquire)
                    {
                        return None;
                    }
                    let lease = super::cluster_world_owner::player_action_lease(server, player)?;
                    Some((
                        *gid,
                        (
                            lease,
                            player
                                .cluster_owner_install_generation
                                .load(Ordering::Acquire),
                        ),
                    ))
                })
                .collect(),
        }));
        assert!(
            self.owner
                .push(ControllerOwner {
                    game_mode_pending,
                    game_mode_preflighted,
                    handoff_completions,
                    frame_epochs,
                    awaiting_bedrock_teleport,
                    projected_epochs,
                    morning_cuts,
                    active_mining_leases,
                    active_mining,
                    retained_controller_patches,
                    retained_respawn_inputs,
                    test_departure_arms,
                    velocity_cuts,
                    vehicle_corrections,
                    pending_settings,
                    retained_raw_cosmetic_inputs,
                    retained_cosmetic_inputs,
                    retained_vehicle_inputs,
                    chunk_deliveries,
                    next_chunk_lease,
                    retained_command_proposals,
                    entity_command_sequences,
                    retained_player_commands,
                    failed_player_commands,
                    player_commands,
                    frames,
                    applied,
                    saturation_last,
                    pending_policy,
                    next_policy_generation,
                    book_preflights,
                    action_sequences,
                    session_progress,
                    admission_results,
                    admission_outcomes,
                    pending_departure_requests,
                    departures,
                    departing,
                    administrative_sequences,
                    administrative_origins,
                    awaiting_teleport,
                    abilities,
                    administrative_states,
                    pending_administrative,
                    deferred_administrative,
                    administrative_publications,
                    #[cfg(feature = "cluster-e2e")]
                    offsets,
                    #[cfg(feature = "cluster-e2e")]
                    held_departure_admin,
                    #[cfg(feature = "cluster-e2e")]
                    logged_divergence,
                })
                .is_ok()
        );
        for (respond, snapshot) in departure_signals {
            let _ = respond.send(DepartureStatus::Retired(Box::new(snapshot)));
        }
    }
}

fn try_send_vehicle_input(
    server: &Server,
    input: pumpkin_cluster::actor_relation::VehicleControlInput,
) -> Result<(), pumpkin_cluster::actor_relation::VehicleControlInput> {
    let Some(route) = server.cluster_vehicle_control_route.load_full() else {
        return Err(input);
    };
    route
        .try_send(input)
        .map_err(tokio::sync::mpsc::error::TrySendError::into_inner)
}

fn capture_respawn_proposal(
    server: &Server,
    player: &Player,
    tick: TickStamp,
    sequence: &mut u16,
) -> Option<pumpkin_cluster::canonical_action::ActionProposal> {
    use pumpkin_cluster::canonical_action::{
        ActionId, ActionProposal, ChunkPart, ClaimedAction, ClientOrigin, Target,
    };
    let lease = super::cluster_world_owner::player_action_lease(server, player)?;
    let spatial = server.cluster_actor_spatial.load();
    let source = spatial.get(&lease.gid)?;
    let living = server.cluster_actor_living.load();
    let state = living.get(&pumpkin_cluster::actor::ActorId::Player(lease.gid))?;
    if state.combat.health_milli != 0 {
        return None;
    }
    let mut avatar = state.avatar.clone()?;
    let source_epoch = avatar.spatial_epoch;
    let worlds = server.worlds.load();
    let world = worlds
        .iter()
        .find(|world| world.dimension == pumpkin_data::dimension::Dimension::OVERWORLD)
        .or_else(|| worlds.first())?;
    let world_id = super::cluster_world_delta::world_id(world);
    let spawn = super::cluster_environment::spawn_at(server, world_id, tick)?;
    let border = super::cluster_environment::border_at(server, world_id, tick)?;
    let scale = f64::from(super::cluster_living_metadata::player_scale(state));
    let half_width = 0.3 * scale;
    let height = 1.8 * scale;
    let radius = f64::from_bits(border.diameter) * 0.5;
    let clamp_spawn = |value: i32, center: u64| {
        let center = f64::from_bits(center);
        let minimum = (center - radius + half_width - 0.5)
            .ceil()
            .max(-30_000_000.0);
        let maximum = (center + radius - half_width - 0.5)
            .floor()
            .min(30_000_000.0);
        if minimum <= maximum {
            f64::from(value).clamp(minimum, maximum) as i32
        } else {
            (center - 0.5).floor().clamp(-30_000_000.0, 30_000_000.0) as i32
        }
    };
    let position = pumpkin_util::math::position::BlockPos::new(
        clamp_spawn(spawn.x, border.center[0]),
        spawn.y,
        clamp_spawn(spawn.z, border.center[1]),
    );
    let (yaw, pitch) = (spawn.yaw, spawn.pitch);
    let fallback_point = pumpkin_cluster::item_action::PlayerRespawnState {
        world: world_id,
        position: pumpkin_cluster::protocol::BlockPos {
            x: position.0.x,
            y: position.0.y,
            z: position.0.z,
        },
        yaw: yaw.to_bits(),
        forced: true,
    };
    let make_plan = |point: pumpkin_cluster::item_action::PlayerRespawnState| {
        let world = worlds
            .iter()
            .find(|world| super::cluster_world_delta::world_id(world) == point.world)?;
        let max_y = world.dimension.min_y.checked_add(world.dimension.height)?;
        let mut config_version = 0xcbf29ce484222325_u64;
        for value in [
            world.dimension.min_y as u64,
            max_y as u64,
            half_width.to_bits(),
            height.to_bits(),
            u64::from(!world.dimension.bed_rule.explodes),
            u64::from(world.dimension.respawn_anchor_works),
        ] {
            for byte in value.to_le_bytes() {
                config_version = (config_version ^ u64::from(byte)).wrapping_mul(0x100000001b3);
            }
        }
        Some(pumpkin_cluster::world_respawn::WorldRespawnPlan {
            point,
            min_y: world.dimension.min_y,
            max_y,
            half_width: half_width.to_bits(),
            height: height.to_bits(),
            border: super::cluster_environment::border_at(server, point.world, tick)?,
            bed_works: !world.dimension.bed_rule.explodes,
            anchor_works: world.dimension.respawn_anchor_works,
            config_version: config_version.max(1),
        })
    };
    let world_plan = avatar.presentation.respawn.and_then(make_plan);
    let fallback_plan = Some(make_plan(fallback_point)?);
    let position = [
        f64::from(position.0.x) + 0.5,
        f64::from(position.0.y),
        f64::from(position.0.z) + 0.5,
    ];
    let destination_chunk = ChunkKey::new(
        world_id,
        (position[0].floor() as i32).div_euclid(16),
        (position[2].floor() as i32).div_euclid(16),
    );
    avatar.world = world_id;
    avatar.position = position.map(f64::to_bits);
    avatar.yaw = yaw.to_bits();
    avatar.head_yaw = yaw.to_bits();
    avatar.pitch = pitch.to_bits();
    let attribute = u16::from(pumpkin_data::attributes::Attributes::MAX_HEALTH.id);
    *sequence = sequence.wrapping_add(1);
    Some(ActionProposal {
        command: None,
        id: ActionId {
            actor: pumpkin_cluster::actor::ActorId::Player(lease.gid),
            incarnation: lease.incarnation,
            sequence: *sequence,
            family: 7,
        },
        tick,
        lease: Some(lease),
        client_origin: ClientOrigin::None,
        screen_generation: 0,
        claimed_targets: vec![Target::Chunk {
            key: source.truth.local.chunk,
            part: ChunkPart::Player(lease.gid),
            inventory: None,
        }],
        claim: ClaimedAction::Respawn(pumpkin_cluster::combat_action::RespawnClaim {
            owner_install_generation: player
                .cluster_owner_install_generation
                .load(Ordering::Acquire),
            source_epoch,
            death_cause: state.combat.timing.death_cause,
            destination_chunk,
            avatar,
            world_plan,
            fallback_plan,
            max_health_attribute: attribute,
        }),
    })
}

fn mounted_controller_snapshot(
    server: &Server,
    player: &Player,
) -> Option<(
    [f64; 3],
    [f64; 3],
    bool,
    pumpkin_cluster::protocol::WorldId,
    Option<(
        (uuid::Uuid, TickStamp, PlayerSeq),
        pumpkin_cluster::entity_body::EntitySpatial,
    )>,
)> {
    let lease = super::cluster_world_owner::player_action_lease(server, player)?;
    let living = server.cluster_actor_living.load();
    let mount = living
        .get(&pumpkin_cluster::actor::ActorId::Player(lease.gid))?
        .avatar
        .as_ref()?
        .mount
        .as_ref()?;
    let pumpkin_cluster::item_contact::ContactIdentity::Entity { id, birth } = mount else {
        return None;
    };
    let views = server.cluster_vehicle_views.load();
    let vehicle = views.vehicles.get(id)?;
    if &vehicle.birth != birth {
        return None;
    }
    let (seat, _) = vehicle.passengers.iter().find(|(_, identity)| {
        **identity == pumpkin_cluster::item_contact::ContactIdentity::Player(lease)
    })?;
    let position = pumpkin_cluster::vehicle_control::passenger_position(
        vehicle.kind,
        *seat,
        &vehicle.spatial,
    )?;
    let correction = vehicle.controller.as_ref().and_then(|record| {
        if !matches!(
            record.filter,
            pumpkin_cluster::vehicle_control::VehicleFilterDecision::ClippedClaim
                | pumpkin_cluster::vehicle_control::VehicleFilterDecision::RejectedDistance
        ) {
            return None;
        }
        let input = record.claimed.as_ref()?;
        if input.driver != lease {
            return None;
        }
        Some((
            (record.birth.uuid, record.tick, input.input_sequence),
            record.effective.clone(),
        ))
    });
    Some((
        position,
        vehicle
            .spatial
            .velocity_micro
            .map(|value| value as f64 / 1_000_000.0),
        vehicle.spatial.on_ground,
        vehicle.spatial.world,
        correction,
    ))
}

fn capture_vehicle_input(
    server: &Server,
    player: &Player,
    frame: &ControllerFrame,
    patch: ControllerPatch,
    tick: TickStamp,
    sequence: &mut u16,
) -> Option<pumpkin_cluster::actor_relation::VehicleControlInput> {
    use pumpkin_cluster::actor_relation::{VehicleClaimedTransform, VehicleControlInput};
    use pumpkin_protocol::java::server::play::SPlayerInput;
    let driver = super::cluster_world_owner::player_action_lease(server, player)?;
    let publication = server.cluster_actor_living.load();
    let mount = publication
        .get(&pumpkin_cluster::actor::ActorId::Player(driver.gid))?
        .avatar
        .as_ref()?
        .mount
        .clone()?;
    let flags = frame.effective.input_flags;
    let pressed = |bit: i8| flags & bit as u64 != 0;
    let (claimed, paddle_left, paddle_right) = match patch {
        ControllerPatch::Vehicle {
            position,
            yaw,
            pitch,
        } => (
            Some(VehicleClaimedTransform {
                position: position.map(f64::to_bits),
                yaw: yaw.to_bits(),
                pitch: pitch.to_bits(),
            }),
            false,
            false,
        ),
        ControllerPatch::Paddles { left, right } => (None, left, right),
        ControllerPatch::VehicleFlags { .. } => (None, false, false),
        _ => return None,
    };
    *sequence = sequence.wrapping_add(1);
    let (forward, sideways, jumping) = match patch {
        ControllerPatch::VehicleFlags {
            forward,
            sideways,
            jumping,
        } => (forward, sideways, jumping),
        _ => (
            i8::from(pressed(SPlayerInput::FORWARD)) - i8::from(pressed(SPlayerInput::BACKWARD)),
            i8::from(pressed(SPlayerInput::LEFT)) - i8::from(pressed(SPlayerInput::RIGHT)),
            pressed(SPlayerInput::JUMP),
        ),
    };
    let input = VehicleControlInput {
        driver_effective_yaw: frame.effective.yaw.to_bits(),
        driver,
        mount,
        simulation_tick: tick,
        input_sequence: PlayerSeq(*sequence),
        claimed,
        forward,
        sideways,
        jumping,
        paddle_left,
        paddle_right,
        paddles_changed: matches!(patch, ControllerPatch::Paddles { .. }),
    };
    input.is_valid().then_some(input)
}

fn effective_item_frame(
    server: &Server,
    frames: &HashMap<GlobalPlayerId, ControllerFrame>,
    actor: GlobalPlayerId,
    tick: TickStamp,
) -> Option<(ControllerFrame, f64)> {
    let mut frame = frames.get(&actor).copied()?;
    frame.effective.tick = tick;
    let publication = server.cluster_actor_spatial.load();
    let spatial = &publication.get(&actor)?.truth.local;
    let eye_height = if spatial.crawling || spatial.swimming || spatial.gliding {
        0.4
    } else if spatial.sneaking {
        1.27
    } else {
        f64::from(pumpkin_data::entity::EntityType::PLAYER.eye_height)
    };
    let living = server.cluster_actor_living.load();
    let state = living.get(&pumpkin_cluster::actor::ActorId::Player(actor))?;
    Some((
        frame,
        eye_height * f64::from(super::cluster_living_metadata::player_scale(state)),
    ))
}

fn session_progress_pending(progress: &SessionProgress) -> bool {
    progress.failed
        || progress.completed_this_advance
        || progress.retained.is_some()
        || !progress.outstanding.is_empty()
}

fn drain_session_inputs(
    server: &Arc<Server>,
    players: &HashMap<GlobalPlayerId, Arc<Player>>,
    progress: &mut HashMap<GlobalPlayerId, SessionProgress>,
) {
    for (actor, player) in players {
        let state = progress.entry(*actor).or_default();
        if let Some(previous) = state.player.as_ref().and_then(std::sync::Weak::upgrade)
            && !Arc::ptr_eq(&previous, player)
        {
            if state.failed || state.retained.is_some() || !state.outstanding.is_empty() {
                if !state.failed {
                    tracing::error!(
                        actor = actor.pack_u32(),
                        "cluster session player incarnation changed with pending work"
                    );
                }
                state.failed = true;
                continue;
            }
            *state = SessionProgress::default();
        }
        state.player = Some(Arc::downgrade(player));
        state.completed_this_advance = false;
        while let Some(mut receiver) = state.outstanding.pop_front() {
            match receiver.try_recv() {
                Ok(
                    super::cluster_session_ingress::SessionOutcome::Completed
                    | super::cluster_session_ingress::SessionOutcome::Rejected,
                ) => state.completed_this_advance = true,
                Ok(super::cluster_session_ingress::SessionOutcome::Failed)
                | Err(oneshot::error::TryRecvError::Closed) => {
                    if !state.failed {
                        tracing::error!(actor = actor.pack_u32(), "cluster session worker failed");
                        player.client.try_kick(
                            crate::net::DisconnectReason::Kicked,
                            &pumpkin_util::text::TextComponent::text(
                                "Cluster session processing failed",
                            ),
                        );
                    }
                    state.failed = true;
                }
                Err(oneshot::error::TryRecvError::Empty) => {
                    state.outstanding.push_front(receiver);
                    break;
                }
            }
        }
        if state.failed {
            continue;
        }
        if let Some((input, receiver)) = state.retained.take() {
            match super::cluster_session_ingress::try_submit(server, input) {
                Ok(()) => state.outstanding.push_back(receiver),
                Err(input) => {
                    state.retained = Some((input, receiver));
                    continue;
                }
            }
        }
        while state.outstanding.len() < SESSION_OUTSTANDING_LIMIT {
            let Some(staged) = player.cluster_session_staged.pop() else {
                break;
            };
            let (sender, receiver) = oneshot::channel();
            let input =
                super::cluster_session_ingress::SessionInput::new(player.clone(), staged, sender);
            match super::cluster_session_ingress::try_submit(server, input) {
                Ok(()) => state.outstanding.push_back(receiver),
                Err(input) => {
                    state.retained = Some((input, receiver));
                    break;
                }
            }
        }
    }
    progress.retain(|actor, state| players.contains_key(actor) || session_progress_pending(state));
}

fn queue_administrative_publication(
    server: &Server,
    outbox: &mut VecDeque<AdministrativePublicationTarget>,
    recipients: Vec<pumpkin_cluster::identity::ServerId>,
    publication: PlayerAdministrativePublication,
) -> bool {
    if !publication_has_capacity(outbox, &recipients) {
        return false;
    }
    server.cluster_player_admin.publish(publication.clone());
    let local = pumpkin_cluster::identity::ServerId(server.advanced_config.cluster.server_id);
    let connected = server.cluster_connected_peers.load();
    let generations = server.cluster_peer_generations.load();
    for peer in recipients.into_iter().filter(|peer| *peer != local) {
        if connected.contains(&peer)
            && let Some(generation) = generations.get(&peer)
        {
            outbox.push_back((peer, *generation, publication.clone()));
        }
    }
    flush_administrative_publications(server, outbox);
    true
}

fn publication_has_capacity(
    outbox: &VecDeque<AdministrativePublicationTarget>,
    recipients: &[pumpkin_cluster::identity::ServerId],
) -> bool {
    outbox.len().saturating_add(recipients.len()) <= ADMIN_PUBLICATION_LIMIT
}

fn administrative_recipients(
    origin: Option<pumpkin_cluster::identity::ServerId>,
    holders: Vec<pumpkin_cluster::identity::ServerId>,
) -> Vec<pumpkin_cluster::identity::ServerId> {
    let mut recipients: HashSet<_> = holders.into_iter().collect();
    recipients.extend(origin);
    recipients.into_iter().collect()
}

fn flush_administrative_publications(
    server: &Server,
    outbox: &mut VecDeque<AdministrativePublicationTarget>,
) {
    let connected = server.cluster_connected_peers.load();
    let generations = server.cluster_peer_generations.load();
    while let Some((peer, generation, publication)) = outbox.front() {
        if !connected.contains(peer) || generations.get(peer) != Some(generation) {
            outbox.pop_front();
            continue;
        }
        if !super::cluster_player_admin::send_publication(server, *peer, publication) {
            break;
        }
        outbox.pop_front();
    }
}

fn apply_administrative_controller(
    player: &Arc<Player>,
    target: &PlayerAdministrativeState,
    abilities: &mut HashMap<GlobalPlayerId, crate::entity::player::Abilities>,
) -> Option<PlayerAdministrativeState> {
    let actor = player.cluster_gid()?;
    pumpkin_util::GameMode::try_from(target.gamemode).ok()?;
    let state = abilities.entry(actor).or_default();
    state.invulnerable = matches!(target.gamemode, 1 | 3);
    state.creative = target.gamemode == 1;
    state.allow_flying = matches!(target.gamemode, 1 | 3);
    state.flying = target.gamemode == 3 || state.allow_flying && state.flying;
    state.allow_modify_world = matches!(target.gamemode, 0 | 1);
    Some(target.clone())
}

fn update_administrative_controller(
    state: Option<&mut PlayerAdministrativeState>,
    player: &Player,
    frame: &ControllerFrame,
) {
    let Some(state) = state else {
        return;
    };
    if let Some(server) = player.world().server.upgrade() {
        state.world = server
            .cluster_actor_spatial
            .load()
            .get(&frame.claimed.actor)
            .map_or(state.world, |spatial| spatial.truth.local.world);
    }
    state.position = frame.effective.position;
    state.yaw = frame.effective.yaw;
    state.pitch = frame.effective.pitch;
    state.velocity = frame.effective.velocity;
}

fn administrative_frame(
    claimed: ClientClaimedState,
    state: &PlayerAdministrativeState,
    tick: TickStamp,
) -> ControllerFrame {
    let mut effective = EffectiveControllerState::from(claimed);
    effective.tick = tick;
    effective.position = state.position;
    effective.velocity = state.velocity;
    effective.yaw = state.yaw;
    effective.pitch = state.pitch;
    ControllerFrame {
        claimed,
        effective,
        decision: ControllerDecision::Modified(4),
    }
}

fn preserve_unclaimed_fields(
    frame: &mut ControllerFrame,
    controls: &ControlChanges,
    current: EffectiveControllerState,
) {
    if !controls.position_changed {
        frame.effective.position = current.position;
    }
    if !controls.velocity_changed {
        frame.effective.velocity = current.velocity;
    }
    if !controls.rotation_changed {
        frame.effective.yaw = current.yaw;
        frame.effective.pitch = current.pitch;
    }
    if !controls.ground_changed {
        frame.effective.on_ground = current.on_ground;
    }
    if !controls.input_changed {
        frame.effective.input_flags = current.input_flags;
    }
}

impl ControllerPatch {
    const fn moves(self) -> bool {
        matches!(
            self,
            Self::Position { .. }
                | Self::PositionRotation { .. }
                | Self::Rotation { .. }
                | Self::Ground(_)
                | Self::Bedrock { .. }
        )
    }
}

fn initial_claim(player: &Player, actor: GlobalPlayerId, tick: TickStamp) -> ClientClaimedState {
    let initial = player
        .cluster_initial_administrative
        .as_ref()
        .expect("prepared controller baseline");
    ClientClaimedState {
        actor,
        seq: PlayerSeq(u16::MAX),
        tick,
        position: initial.position,
        velocity: initial.velocity,
        yaw: initial.yaw,
        pitch: initial.pitch,
        on_ground: player
            .cluster_handoff_base_nbt
            .get_bool("OnGround")
            .unwrap_or(false),
        input_flags: 0,
    }
}

pub(crate) fn apply_patch(
    mut state: ClientClaimedState,
    patch: ControllerPatch,
    tick: TickStamp,
) -> Option<(ClientClaimedState, ControlChanges)> {
    let mut controls = ControlChanges::default();
    match patch {
        ControllerPatch::Position {
            position,
            on_ground,
        } => {
            if position.iter().any(|value| !value.is_finite()) {
                return None;
            }
            state.position = clamp_position(position);
            state.on_ground = on_ground;
            controls.position_changed = true;
            controls.ground_changed = true;
        }
        ControllerPatch::PositionRotation {
            position,
            yaw,
            pitch,
            on_ground: _,
        }
        | ControllerPatch::Bedrock {
            position,
            yaw,
            pitch,
            on_ground: _,
            velocity: _,
            flags: _,
            sneaking: _,
            sprinting: _,
            flying: _,
            crawling: _,
        } if position.iter().any(|value| !value.is_finite())
            || !yaw.is_finite()
            || !pitch.is_finite() =>
        {
            return None;
        }
        ControllerPatch::PositionRotation {
            position,
            yaw,
            pitch,
            on_ground,
        } => {
            state.position = clamp_position(position);
            state.yaw = pumpkin_util::math::wrap_degrees(yaw) % 360.0;
            state.pitch = pumpkin_util::math::wrap_degrees(pitch);
            state.on_ground = on_ground;
            controls.position_changed = true;
            controls.rotation_changed = true;
            controls.ground_changed = true;
        }
        ControllerPatch::Rotation {
            yaw,
            pitch,
            on_ground,
        } => {
            if !yaw.is_finite() || !pitch.is_finite() {
                return None;
            }
            state.yaw = pumpkin_util::math::wrap_degrees(yaw) % 360.0;
            state.pitch = pumpkin_util::math::wrap_degrees(pitch);
            state.on_ground = on_ground;
            controls.rotation_changed = true;
            controls.ground_changed = true;
        }
        ControllerPatch::Ground(on_ground) => {
            state.on_ground = on_ground;
            controls.ground_changed = true;
        }
        ControllerPatch::Input {
            flags,
            sneaking: next_sneaking,
            sprinting: next_sprinting,
        } => {
            state.input_flags = flags;
            controls.input_changed = true;
            controls.sneaking = next_sneaking;
            controls.sprinting = next_sprinting;
        }
        ControllerPatch::Sneak(sneaking) => {
            let mask = pumpkin_protocol::java::server::play::SPlayerInput::SNEAK as u64;
            state.input_flags = if sneaking {
                state.input_flags | mask
            } else {
                state.input_flags & !mask
            };
            controls.input_changed = true;
            controls.sneaking = Some(sneaking);
        }
        ControllerPatch::Bedrock {
            position,
            velocity,
            yaw,
            pitch,
            on_ground,
            flags,
            sneaking: next_sneaking,
            sprinting: next_sprinting,
            flying,
            crawling,
        } => {
            if velocity.iter().any(|value| !value.is_finite()) {
                return None;
            }
            state.position = clamp_position(position);
            state.velocity = velocity;
            state.yaw = pumpkin_util::math::wrap_degrees(yaw) % 360.0;
            state.pitch = pumpkin_util::math::wrap_degrees(pitch);
            state.on_ground = on_ground;
            state.input_flags = flags;
            controls.input_changed = true;
            controls.sneaking = next_sneaking;
            controls.sprinting = next_sprinting;
            controls.flying = flying;
            controls.crawling = crawling;
            controls.position_changed = true;
            controls.rotation_changed = true;
            controls.velocity_changed = true;
            controls.ground_changed = true;
        }
        ControllerPatch::Sprint(sprinting) => {
            controls.sprinting = Some(sprinting);
        }
        ControllerPatch::Pose { swimming, gliding } => {
            controls.swimming = swimming;
            controls.gliding = gliding;
        }
        ControllerPatch::Cosmetic(_)
        | ControllerPatch::Respawn
        | ControllerPatch::Wake
        | ControllerPatch::WakeMorning(_)
        | ControllerPatch::TeleportConfirm(_)
        | ControllerPatch::Vehicle { .. }
        | ControllerPatch::Paddles { .. }
        | ControllerPatch::VehicleFlags { .. } => return Some((state, controls)),
    }
    state.seq = PlayerSeq(state.seq.0.wrapping_add(1));
    state.tick = tick;
    Some((state, controls))
}

#[derive(Clone, Copy, Default)]
pub(crate) struct ControlChanges {
    pub(crate) sneaking: Option<bool>,
    pub(crate) sprinting: Option<bool>,
    pub(crate) flying: Option<bool>,
    pub(crate) crawling: Option<bool>,
    pub(crate) swimming: Option<bool>,
    pub(crate) gliding: Option<bool>,
    input_changed: bool,
    position_changed: bool,
    rotation_changed: bool,
    velocity_changed: bool,
    ground_changed: bool,
    movement_stat: MovementStat,
}

#[derive(Clone, Copy, Default)]
enum MovementStat {
    #[default]
    Unchecked,
    Cancelled,
    Amount(i32),
}

fn requires_policy(
    server: &Server,
    frame: &ControllerFrame,
    controls: &ControlChanges,
    position: [f64; 3],
) -> bool {
    use crate::plugin::api::events::player::player_input::PlayerInputEvent;
    use crate::plugin::api::events::player::player_move::PlayerMoveEvent;
    use crate::plugin::api::events::player::player_statistic_increment::PlayerStatisticIncrementEvent;
    use crate::plugin::api::events::player::player_toggle_flight_event::PlayerToggleFlightEvent;
    use crate::plugin::api::events::player::player_toggle_sneak_event::PlayerToggleSneakEvent;

    controls.position_changed
        && frame.effective.position != position
        && server.plugin_manager.has_handlers::<PlayerMoveEvent>()
        || controls.position_changed
            && server
                .plugin_manager
                .has_handlers::<PlayerStatisticIncrementEvent>()
        || controls.input_changed && server.plugin_manager.has_handlers::<PlayerInputEvent>()
        || controls.sneaking.is_some()
            && server
                .plugin_manager
                .has_handlers::<PlayerToggleSneakEvent>()
        || controls.flying.is_some()
            && server
                .plugin_manager
                .has_handlers::<PlayerToggleFlightEvent>()
}

fn spawn_policy(
    server: Arc<Server>,
    player: Arc<Player>,
    generation: u64,
    from: Vector3<f64>,
    mut frame: ControllerFrame,
    mut controls: ControlChanges,
    movement_statistic: crate::entity::player::statistics::CustomStatistic,
    previous_effective: pumpkin_cluster::controller::EffectiveControllerState,
    results: Arc<ArrayQueue<PolicyResult>>,
) {
    let actor = frame.claimed.actor;
    let runtime = server.runtime.clone();
    let collision_avatar = server
        .cluster_actor_living
        .load()
        .get(&pumpkin_cluster::actor::ActorId::Player(actor))
        .and_then(|state| {
            state.avatar.clone().map(|avatar| {
                (
                    avatar,
                    f64::from(super::cluster_living_metadata::player_scale(state)),
                    player_step_height(state),
                )
            })
        });
    let collision_chunks = server.cluster_chunk_presentations.load_full();
    runtime.spawn(async move {
        let fallback_frame = frame;
        let fallback_controls = controls;
        let policy = async {
        use crate::plugin::api::events::player::player_input::PlayerInputEvent;
        use crate::plugin::api::events::player::player_move::PlayerMoveEvent;
        use crate::plugin::api::events::player::player_statistic_increment::PlayerStatisticIncrementEvent;
        use crate::plugin::api::events::player::player_toggle_flight_event::PlayerToggleFlightEvent;
        use crate::plugin::api::events::player::player_toggle_sneak_event::PlayerToggleSneakEvent;

        let target = Vector3::new(
            frame.effective.position[0],
            frame.effective.position[1],
            frame.effective.position[2],
        );
        if controls.position_changed && target != from {
            let mut event = PlayerMoveEvent::new(player.clone(), from, target);
            server.plugin_manager.fire(&server, &mut event).await;
            if event.cancelled {
                frame.effective.position = [from.x, from.y, from.z];
                frame.decision = pumpkin_cluster::controller::ControllerDecision::Rejected(2);
            } else {
                frame.effective.position = [event.to.x, event.to.y, event.to.z];
            }
        }
        if controls.position_changed
            && server
                .plugin_manager
                .has_handlers::<PlayerStatisticIncrementEvent>()
        {
            let distance = from
                .squared_distance_to_vec(&Vector3::new(
                    frame.effective.position[0],
                    frame.effective.position[1],
                    frame.effective.position[2],
                ))
                .sqrt();
            let amount = (distance * 100.0) as i32;
            if amount > 0 {
                let statistic = movement_statistic;
                let mut event = PlayerStatisticIncrementEvent {
                    player: player.clone(),
                    statistic_id: format!("{:?}:{}", crate::entity::player::statistics::StatisticCategory::Custom, statistic as i32),
                    amount,
                    cancelled: false,
                };
                server.plugin_manager.fire(&server, &mut event).await;
                controls.movement_stat = if event.cancelled {
                    MovementStat::Cancelled
                } else {
                    MovementStat::Amount(event.amount)
                };
            }
        }
        if controls.input_changed {
            let mut event =
                PlayerInputEvent::new(player.clone(), format!("{:b}", frame.claimed.input_flags));
            server.plugin_manager.fire(&server, &mut event).await;
            if event.cancelled {
                frame.effective.input_flags =
                    previous_effective.input_flags;
                frame.decision = pumpkin_cluster::controller::ControllerDecision::Rejected(3);
                controls.input_changed = false;
                controls.sneaking = None;
                controls.sprinting = None;
            }
        }
        if let Some(sneaking) = controls.sneaking {
            let mut event = PlayerToggleSneakEvent::new(player.clone(), sneaking);
            server.plugin_manager.fire(&server, &mut event).await;
            controls.sneaking = (!event.cancelled).then_some(event.is_sneaking);
        }
        if let Some(flying) = controls.flying {
            let mut event = PlayerToggleFlightEvent::new(player.clone(), flying);
            server.plugin_manager.fire(&server, &mut event).await;
            controls.flying = (!event.cancelled).then_some(event.is_flying);
        }
        (frame, controls)
        };
        let source = crate::command::CommandSource::new(
            crate::command::CommandSender::Player(player.clone()), player.world(), Some(player.clone()),
            from, pumpkin_util::math::vector2::Vector2::new(fallback_frame.effective.yaw, fallback_frame.effective.pitch),
            player.gameprofile.name.clone(),
            pumpkin_util::text::TextComponent::text(player.gameprofile.name.clone()), server.clone(),
        );
        let policy = super::cluster_permissions::with_source(&server, &source, policy);
        let (mut frame, controls, timed_out) = match tokio::time::timeout(std::time::Duration::from_secs(5), policy).await {
            Ok((frame, controls)) => (frame, controls, false),
            Err(_) => (fallback_frame, fallback_controls, true),
        };
        if let Some((avatar, scale, step_height)) = collision_avatar.as_ref() {
            filter_player_collision_cut(avatar, *scale, *step_height, &collision_chunks, &mut frame, previous_effective);
        }
        if retain_policy_result(&results, &player.cluster_controller_policy_retry, PolicyResult {
            actor,
            generation,
            frame,
            controls,
            saturated: false,
            timed_out,
        }).is_err() {
            player.kick(
                crate::net::DisconnectReason::Unknown,
                &pumpkin_util::text::TextComponent::text("Controller policy queue saturated"),
            );
        }
    });
}

impl ControlChanges {
    fn merge(&mut self, newer: Self) {
        if newer.sneaking.is_some() {
            self.sneaking = newer.sneaking;
        }
        if newer.sprinting.is_some() {
            self.sprinting = newer.sprinting;
        }
        if newer.flying.is_some() {
            self.flying = newer.flying;
        }
        if newer.crawling.is_some() {
            self.crawling = newer.crawling;
        }
        if newer.swimming.is_some() {
            self.swimming = newer.swimming;
        }
        if newer.gliding.is_some() {
            self.gliding = newer.gliding;
        }
        self.input_changed |= newer.input_changed;
        self.position_changed |= newer.position_changed;
        self.rotation_changed |= newer.rotation_changed;
        self.velocity_changed |= newer.velocity_changed;
        self.ground_changed |= newer.ground_changed;
        if !matches!(newer.movement_stat, MovementStat::Unchecked) {
            self.movement_stat = newer.movement_stat;
        }
    }
}

fn clamp_position(position: [f64; 3]) -> [f64; 3] {
    [
        position[0].clamp(-3.0E7, 3.0E7),
        position[1].clamp(-2.0E7, 2.0E7),
        position[2].clamp(-3.0E7, 3.0E7),
    ]
}

fn controller_actor_intent(
    server: &Server,
    player: &Player,
    frame: ControllerFrame,
    controls: ControlChanges,
    source: ControllerActorSource,
    world: pumpkin_cluster::protocol::WorldId,
    tick: TickStamp,
    flying: bool,
    gamemode: Option<u8>,
) -> ControllerActorIntent {
    let published = server.cluster_actor_spatial.load();
    let spatial = published.get(&frame.claimed.actor);
    let world = if matches!(source, ControllerActorSource::Client) {
        spatial.map_or(world, |state| state.truth.local.world)
    } else {
        world
    };
    ControllerActorIntent {
        actor: frame.claimed.actor,
        spatial_epoch: spatial.map(|state| state.local_epoch),
        player_key: player.gameprofile.id,
        owner_install_generation: player
            .cluster_owner_install_generation
            .load(std::sync::atomic::Ordering::Acquire),
        head_yaw: frame.effective.yaw,
        source,
        world,
        tick,
        frame,
        position_changed: controls.position_changed,
        velocity_changed: controls.velocity_changed,
        rotation_changed: controls.rotation_changed,
        ground_changed: controls.ground_changed,
        input_changed: controls.input_changed,
        sneaking: controls.sneaking,
        sprinting: controls.sprinting,
        crawling: controls.crawling,
        swimming: controls.swimming,
        gliding: controls.gliding,
        flying: controls.flying,
        effective_flying: flying,
        gamemode,
        movement_statistic: match controls.movement_stat {
            MovementStat::Unchecked => ControllerMovementStatistic::Unchecked,
            MovementStat::Cancelled => ControllerMovementStatistic::Cancelled,
            MovementStat::Amount(amount) => ControllerMovementStatistic::Amount(amount),
        },
    }
}

fn controller_view_intent(
    server: &Server,
    player: &Player,
    actor: ControllerActorIntent,
) -> ViewIntent {
    let position = actor.frame.effective.position;
    let world = server
        .worlds
        .load()
        .iter()
        .find(|world| super::cluster_world_delta::world_id(world) == actor.world)
        .cloned();
    let gamemode = server
        .cluster_actor_living
        .load()
        .get(&pumpkin_cluster::actor::ActorId::Player(actor.actor))
        .and_then(|living| living.avatar.as_ref())
        .map_or(player.cluster_initial_gamemode, |avatar| avatar.game_mode);
    ViewIntent {
        actor: actor.actor,
        owner_install_generation: actor.owner_install_generation,
        world: actor.world,
        center: pumpkin_cluster::protocol::ChunkAddr {
            x: (position[0].floor() as i32).div_euclid(16),
            z: (position[2].floor() as i32).div_euclid(16),
        },
        radius: crate::world::chunker::get_view_distance(player),
        spectator_generates: gamemode != pumpkin_util::GameMode::Spectator as u8
            || world.is_some_and(|world| {
                world
                    .level_info
                    .load()
                    .game_rules
                    .spectators_generate_chunks
            }),
        simulation_distance: server
            .advanced_config
            .networking
            .java
            .simulation_distance
            .get(),
        chunk_send_epoch: player.chunk_send_epoch.load(Ordering::Relaxed),
        tick: actor.tick,
    }
}

pub fn submit_patch(server: &Server, player: &Player, patch: ControllerPatch) -> bool {
    let _ = server;
    if player.cluster_gid().is_none() {
        return false;
    }
    if enqueue_controller_patch(&player.cluster_controller_inputs, patch) {
        return true;
    }
    player
        .cluster_controller_dropped
        .fetch_add(1, Ordering::Relaxed);
    player.kick(
        crate::net::DisconnectReason::Unknown,
        &pumpkin_util::text::TextComponent::text("Too many pending controller actions"),
    );
    false
}

fn enqueue_controller_patch(queue: &ArrayQueue<ControllerPatch>, patch: ControllerPatch) -> bool {
    queue.push(patch).is_ok()
}

#[cfg(feature = "cluster-e2e")]
struct TestOffset(Option<[f64; 3]>);

#[cfg(feature = "cluster-e2e")]
impl ControllerFilter for TestOffset {
    fn filter(
        &mut self,
        _claimed: &ClientClaimedState,
        effective: &mut pumpkin_cluster::controller::EffectiveControllerState,
    ) -> ControllerDecision {
        let Some(offset) = self.0 else {
            return ControllerDecision::Accepted;
        };
        for (position, offset) in effective.position.iter_mut().zip(offset) {
            *position += offset;
        }
        ControllerDecision::Modified(1)
    }
}

#[cfg(test)]
mod tests {
    use pumpkin_cluster::identity::{PlayerSlot, ServerId};
    use pumpkin_cluster::protocol::WorldId;

    use super::*;

    #[test]
    fn actor_intent_credits_bound_unprocessed_owner_inputs() {
        let runtime = ControllerRuntime::new(4_096);
        for _ in 0..1_024 {
            assert!(runtime.actor_intent_credits.pop().is_some());
        }
        assert!(!runtime.has_intent_capacity());
        runtime.return_actor_intent_credit();
        assert!(runtime.has_intent_capacity());
    }

    fn claimed() -> ClientClaimedState {
        ClientClaimedState {
            actor: GlobalPlayerId::new(ServerId(2), PlayerSlot(4)),
            seq: PlayerSeq(8),
            tick: TickStamp(10),
            position: [1.0, 2.0, 3.0],
            velocity: [0.0; 3],
            yaw: 4.0,
            pitch: 5.0,
            on_ground: false,
            input_flags: 0,
        }
    }

    #[test]
    fn partial_claims_compose_in_arrival_order_without_live_entity_reads() {
        let (position, _) = apply_patch(
            claimed(),
            ControllerPatch::Position {
                position: [20.0, 70.0, -2.0],
                on_ground: true,
            },
            TickStamp(20),
        )
        .unwrap();
        let (rotation, _) = apply_patch(
            position,
            ControllerPatch::Rotation {
                yaw: 90.0,
                pitch: 12.0,
                on_ground: true,
            },
            TickStamp(20),
        )
        .unwrap();
        let (input, controls) = apply_patch(
            rotation,
            ControllerPatch::Input {
                flags: 32,
                sneaking: Some(true),
                sprinting: None,
            },
            TickStamp(20),
        )
        .unwrap();

        assert_eq!(input.position, [20.0, 70.0, -2.0]);
        assert_eq!((input.yaw, input.pitch), (90.0, 12.0));
        assert_eq!(input.seq, PlayerSeq(11));
        assert_eq!(input.tick, TickStamp(20));
        assert_eq!(input.input_flags, 32);
        assert_eq!(controls.sneaking, Some(true));
    }

    #[test]
    fn interact_sneak_claim_preserves_other_player_input_flags() {
        let mut initial = claimed();
        initial.input_flags = 8;
        let (sneaking, controls) =
            apply_patch(initial, ControllerPatch::Sneak(true), TickStamp(20)).unwrap();
        assert_eq!(sneaking.input_flags, 40);
        assert_eq!(controls.sneaking, Some(true));
        let (released, controls) =
            apply_patch(sneaking, ControllerPatch::Sneak(false), TickStamp(20)).unwrap();
        assert_eq!(released.input_flags, 8);
        assert_eq!(released.seq, PlayerSeq(10));
        assert_eq!(controls.sneaking, Some(false));
    }

    #[test]
    fn saturation_preserves_accepted_input_order_across_tick_drain() {
        use std::sync::{Arc, Barrier};

        let queue = Arc::new(ArrayQueue::new(2));
        let release = Arc::new(Barrier::new(2));
        let first = ControllerPatch::Position {
            position: [20.0, 70.0, -2.0],
            on_ground: false,
        };
        let second = ControllerPatch::Input {
            flags: 1,
            sneaking: Some(true),
            sprinting: None,
        };
        let rejected = ControllerPatch::Input {
            flags: 2,
            sneaking: Some(false),
            sprinting: None,
        };
        let later = ControllerPatch::Rotation {
            yaw: 90.0,
            pitch: 12.0,
            on_ground: true,
        };
        assert!(enqueue_controller_patch(&queue, first));
        assert!(enqueue_controller_patch(&queue, second));
        assert!(!enqueue_controller_patch(&queue, rejected));

        let first_applied = std::thread::scope(|scope| {
            let producer_queue = queue.clone();
            let producer_release = release.clone();
            let producer = scope.spawn(move || {
                producer_release.wait();
                assert!(enqueue_controller_patch(&producer_queue, later));
            });
            let (state, _) = apply_patch(claimed(), queue.pop().unwrap(), TickStamp(20)).unwrap();
            release.wait();
            producer.join().unwrap();
            state
        });

        let (state, controls) =
            apply_patch(first_applied, queue.pop().unwrap(), TickStamp(20)).unwrap();
        let (state, _) = apply_patch(state, queue.pop().unwrap(), TickStamp(20)).unwrap();
        assert_eq!(state.position, [20.0, 70.0, -2.0]);
        assert_eq!((state.yaw, state.pitch), (90.0, 12.0));
        assert_eq!(state.input_flags, 1);
        assert_eq!(state.seq, PlayerSeq(11));
        assert_eq!(controls.sneaking, Some(true));
        assert_eq!(queue.pop(), None);
    }

    #[test]
    fn bedrock_claim_with_nonfinite_velocity_is_rejected() {
        let patch = ControllerPatch::Bedrock {
            position: [1.0, 2.0, 3.0],
            velocity: [f64::NAN, 0.0, 0.0],
            yaw: 0.0,
            pitch: 0.0,
            on_ground: true,
            flags: 0,
            sneaking: None,
            sprinting: None,
            flying: None,
            crawling: None,
        };
        assert!(apply_patch(claimed(), patch, TickStamp(20)).is_none());
    }

    #[test]
    fn input_after_administrative_teleport_preserves_actual_pose_and_client_claim() {
        let original = claimed();
        let state = PlayerAdministrativeState {
            world: WorldId::new(1, 2),
            position: [40.0, 80.0, 60.0],
            yaw: 90.0,
            pitch: 15.0,
            velocity: [0.0, 0.0, 0.0],
            health: 20.0,
            absorption: 0.0,
            food: 20,
            saturation: 5.0,
            gamemode: 0,
            effects: Vec::new(),
            fall_distance: 0.0,
            on_fire: false,
        };
        let teleported = administrative_frame(original, &state, TickStamp(11));
        let (input, controls) = apply_patch(
            teleported.claimed,
            ControllerPatch::Input {
                flags: 1,
                sneaking: None,
                sprinting: None,
            },
            TickStamp(12),
        )
        .unwrap();
        let mut next = apply_filters(input, &mut []);
        preserve_unclaimed_fields(&mut next, &controls, teleported.effective);

        assert_eq!(next.claimed.position, original.position);
        assert_eq!(next.effective.position, state.position);
        assert_eq!((next.effective.yaw, next.effective.pitch), (90.0, 15.0));
        assert_eq!(next.effective.input_flags, 1);
    }

    #[test]
    fn saturated_phase_result_is_retained_until_controller_drain() {
        let results = ArrayQueue::new(1);
        let pending = ArrayQueue::new(1);
        let first = LivingAdministrativeResult {
            action: ActionKey {
                incarnation: 0,
                actor: claimed().actor.into(),
                sequence: 1,
                family: 7,
            },
            applied: true,
        };
        let second = LivingAdministrativeResult {
            action: ActionKey {
                incarnation: 0,
                actor: claimed().actor.into(),
                sequence: 2,
                family: 7,
            },
            applied: true,
        };
        let third = LivingAdministrativeResult {
            action: ActionKey {
                incarnation: 0,
                actor: claimed().actor.into(),
                sequence: 3,
                family: 7,
            },
            applied: true,
        };
        assert_eq!(
            retain_living_result(&results, &pending, first).unwrap(),
            true
        );
        assert_eq!(
            retain_living_result(&results, &pending, second).unwrap(),
            false
        );
        let third = retain_living_result(&results, &pending, third).unwrap_err();
        assert_eq!(results.pop().map(|result| result.action.sequence), Some(1));
        let retained = pending.pop().unwrap();
        assert_eq!(
            retain_living_result(&results, &pending, retained).unwrap(),
            true
        );
        assert_eq!(results.pop().map(|result| result.action.sequence), Some(2));
        assert_eq!(
            retain_living_result(&results, &pending, third).unwrap(),
            true
        );
        assert_eq!(results.pop().map(|result| result.action.sequence), Some(3));
    }

    #[test]
    fn policy_results_do_not_replace_an_earlier_retry() {
        let results = ArrayQueue::new(1);
        let retry = ArrayQueue::new(1);
        let policy = |sequence| {
            let mut claim = claimed();
            claim.seq = PlayerSeq(sequence);
            PolicyResult {
                actor: claim.actor,
                generation: 1,
                frame: ControllerFrame {
                    claimed: claim,
                    effective: claim.into(),
                    decision: ControllerDecision::Accepted,
                },
                controls: ControlChanges::default(),
                saturated: false,
                timed_out: false,
            }
        };
        assert!(retain_policy_result(&results, &retry, policy(1)).is_ok());
        assert!(retain_policy_result(&results, &retry, policy(2)).is_ok());
        let third = retain_policy_result(&results, &retry, policy(3))
            .err()
            .unwrap();
        assert_eq!(results.pop().unwrap().frame.claimed.seq, PlayerSeq(1));
        assert_eq!(retry.pop().unwrap().frame.claimed.seq, PlayerSeq(2));
        assert!(retain_policy_result(&results, &retry, third).is_ok());
        assert_eq!(results.pop().unwrap().frame.claimed.seq, PlayerSeq(3));
    }
}

fn frozen_movement_statistic(
    server: &Server,
    player: &Player,
    frame: &ControllerFrame,
    controls: &ControlChanges,
    flying: bool,
) -> crate::entity::player::statistics::CustomStatistic {
    use crate::entity::player::statistics::CustomStatistic;
    use pumpkin_data::tag::Taggable;
    let living = server.cluster_actor_living.load();
    let avatar = living
        .get(&pumpkin_cluster::actor::ActorId::Player(
            frame.claimed.actor,
        ))
        .and_then(|state| state.avatar.as_ref());
    if controls.flying.unwrap_or(flying) {
        return CustomStatistic::FlyOneCm;
    }
    if controls
        .gliding
        .unwrap_or_else(|| avatar.is_some_and(|avatar| avatar.stance.gliding))
    {
        return CustomStatistic::AviateOneCm;
    }
    if controls
        .swimming
        .unwrap_or_else(|| avatar.is_some_and(|avatar| avatar.stance.swimming))
    {
        return CustomStatistic::SwimOneCm;
    }
    let world = avatar.map_or_else(
        || super::cluster_world_delta::world_id(&player.world()),
        |avatar| avatar.world,
    );
    let position = frame.effective.position;
    let at = pumpkin_cluster::protocol::BlockPos {
        x: position[0].floor() as i32,
        y: position[1].floor() as i32,
        z: position[2].floor() as i32,
    };
    let chunk = ChunkKey::new(world, at.x.div_euclid(16), at.z.div_euclid(16));
    if let Some(state) = server
        .cluster_chunk_presentations
        .load()
        .get(&chunk)
        .and_then(|view| view.blocks.block(chunk, at).ok())
    {
        let state = state.to_state();
        if state
            .id
            .to_block()
            .has_tag(&pumpkin_data::tag::Block::MINECRAFT_CLIMBABLE)
        {
            return CustomStatistic::ClimbOneCm;
        }
        if state.is_liquid() || state.is_waterlogged() {
            return CustomStatistic::WalkUnderWaterOneCm;
        }
    }
    if controls
        .sneaking
        .unwrap_or_else(|| avatar.is_some_and(|avatar| avatar.stance.sneaking))
    {
        return CustomStatistic::CrouchOneCm;
    }
    if controls
        .sprinting
        .unwrap_or_else(|| avatar.is_some_and(|avatar| avatar.stance.sprinting))
    {
        return CustomStatistic::SprintOneCm;
    }
    if !frame.effective.on_ground && frame.effective.velocity[1] < -0.005 {
        return CustomStatistic::FallOneCm;
    }
    CustomStatistic::WalkOneCm
}

fn filter_readonly_player_collision(
    server: &Server,
    frame: &mut ControllerFrame,
    previous: pumpkin_cluster::controller::EffectiveControllerState,
) {
    let living = server.cluster_actor_living.load();
    let Some(state) = living.get(&pumpkin_cluster::actor::ActorId::Player(
        frame.claimed.actor,
    )) else {
        return;
    };
    let Some(avatar) = state.avatar.as_ref() else {
        return;
    };
    let scale = f64::from(super::cluster_living_metadata::player_scale(state));
    let chunks = server.cluster_chunk_presentations.load();
    filter_player_collision_cut(
        avatar,
        scale,
        player_step_height(state),
        &chunks,
        frame,
        previous,
    );
}

fn player_step_height(state: &pumpkin_cluster::item_action::ActorLivingState) -> f64 {
    let attribute = &pumpkin_data::attributes::Attributes::STEP_HEIGHT;
    let base = state
        .combat
        .effective_attributes
        .get(&u16::from(attribute.id))
        .copied()
        .map(f64::from_bits)
        .unwrap_or(attribute.default_value);
    state
        .attribute(u16::from(attribute.id), base)
        .clamp(0.0, 10.0)
}

fn filter_player_collision_cut(
    avatar: &pumpkin_cluster::item_action::PlayerAvatarState,
    scale: f64,
    step_height: f64,
    chunks: &HashMap<ChunkKey, Arc<super::cluster_chunk_presentation::ChunkPresentation>>,
    frame: &mut ControllerFrame,
    previous: pumpkin_cluster::controller::EffectiveControllerState,
) {
    use pumpkin_util::math::{boundingbox::BoundingBox, collision::resolve_block_movement};
    if avatar.game_mode == 3 || avatar.mount.is_some() {
        return;
    }
    let displacement = std::array::from_fn::<_, 3, _>(|axis| {
        frame.effective.position[axis] - previous.position[axis]
    });

    let height = if avatar.stance.swimming || avatar.stance.gliding || avatar.stance.crawling {
        0.6
    } else if avatar.stance.sneaking {
        1.5
    } else {
        1.8
    };
    let bounds = BoundingBox {
        min: Vector3::new(
            previous.position[0] - 0.3 * scale,
            previous.position[1],
            previous.position[2] - 0.3 * scale,
        ),
        max: Vector3::new(
            previous.position[0] + 0.3 * scale,
            previous.position[1] + height * scale,
            previous.position[2] + 0.3 * scale,
        ),
    };
    let movement = Vector3::new(displacement[0], displacement[1], displacement[2]);
    let collide = |bounds: BoundingBox, movement: Vector3<f64>| {
        let collisions = pumpkin_world::collision::collect_block_collisions(
            bounds.stretch(movement),
            4096,
            |at| {
                let position = pumpkin_cluster::protocol::BlockPos {
                    x: at.0.x,
                    y: at.0.y,
                    z: at.0.z,
                };
                let chunk = ChunkKey::new(
                    avatar.world,
                    position.x.div_euclid(16),
                    position.z.div_euclid(16),
                );
                let blocks = &chunks.get(&chunk).ok_or(())?.blocks;
                let max_y = i64::from(blocks.min_y()) + blocks.block_palettes().len() as i64 * 16;
                if position.y < blocks.min_y() || i64::from(position.y) >= max_y {
                    return Ok(pumpkin_data::Block::AIR.default_state);
                }
                blocks
                    .block(chunk, position)
                    .map(|state| state.to_state())
                    .map_err(|_| ())
            },
            |_, _| None,
        )?;
        Ok::<_, pumpkin_world::collision::CollisionReadFailure<()>>(resolve_block_movement(
            bounds,
            movement,
            &collisions.shapes,
            &collisions.positions,
        ))
    };
    let resolved = pumpkin_util::math::collision::resolve_step_movement(
        bounds,
        movement,
        step_height,
        previous.on_ground,
        collide,
    );
    let Ok(resolved) = resolved else {
        frame.effective.position = previous.position;
        frame.effective.on_ground = previous.on_ground;
        frame.decision = ControllerDecision::Rejected(9);
        return;
    };
    let actual = [
        resolved.movement.x,
        resolved.movement.y,
        resolved.movement.z,
    ];
    let shift = Vector3::new(actual[0], actual[1], actual[2]);
    let end_bounds = BoundingBox {
        min: bounds.min + shift,
        max: bounds.max + shift,
    };
    let Ok(support) = collide(end_bounds, Vector3::new(0.0, -1.0e-5, 0.0)) else {
        frame.effective.position = previous.position;
        frame.effective.on_ground = previous.on_ground;
        frame.decision = ControllerDecision::Rejected(9);
        return;
    };
    let on_ground = displacement[1] <= 0.0 && support.supporting_block.is_some();
    if actual != displacement || frame.effective.on_ground != on_ground {
        frame.effective.position =
            std::array::from_fn(|axis| previous.position[axis] + actual[axis]);
        frame.effective.on_ground = on_ground;
        frame.decision = ControllerDecision::Modified(9);
    }
}

fn publish_controller_correction(server: &Server, player: &Player, frame: &ControllerFrame) {
    if frame.claimed.position == frame.effective.position {
        return;
    }
    let position = frame.effective.position;
    let position = Vector3::new(position[0], position[1], position[2]);
    match player.client.as_ref() {
        crate::net::ClientPlatform::Java(client) => {
            let id = player
                .teleport_id_count
                .fetch_add(1, Ordering::Relaxed)
                .wrapping_add(1);
            if !server
                .cluster_controller
                .note_teleport(frame.claimed.actor, Some((id.into(), position)))
            {
                player.kick(
                    crate::net::DisconnectReason::Unknown,
                    &pumpkin_util::text::TextComponent::text(
                        "Controller correction queue saturated",
                    ),
                );
                return;
            }
            client.try_send_packet(&pumpkin_protocol::java::client::play::CPlayerPosition::new(
                id.into(),
                position,
                Vector3::new(0.0, 0.0, 0.0),
                frame.effective.yaw,
                frame.effective.pitch,
                Vec::new(),
            ));
        }
        crate::net::ClientPlatform::Bedrock(client) => {
            let living = server.cluster_actor_living.load();
            let Some(state) = living.get(&pumpkin_cluster::actor::ActorId::Player(
                frame.claimed.actor,
            )) else {
                return;
            };
            let eye_height = super::cluster_living_metadata::player_eye_height(state);
            let packet = pumpkin_protocol::bedrock::client::CMovePlayer::new(
                (player.entity_id() as u64).into(),
                Vector3::new(
                    position.x as f32,
                    position.y as f32 + eye_height,
                    position.z as f32,
                ),
                frame.effective.pitch,
                frame.effective.yaw,
                frame.effective.yaw,
                pumpkin_protocol::bedrock::client::CMovePlayer::MODE_TELEPORT,
                frame.effective.on_ground,
                0u64.into(),
                0,
                0,
                u64::from(frame.effective.tick.0).into(),
            );
            if let Ok(bytes) = client.serialize_packet(&packet) {
                client.try_enqueue_packet(bytes);
            }
        }
    }
}

fn capture_wake_proposal(
    server: &Server,
    player: &Player,
    tick: TickStamp,
    sequence: &mut u16,
    morning: Option<pumpkin_cluster::world_time::MorningTransition>,
) -> Option<pumpkin_cluster::canonical_action::ActionProposal> {
    use pumpkin_cluster::canonical_action::{
        ActionId, ActionProposal, ChunkPart, ClaimedAction, ClientOrigin, Target,
    };
    use pumpkin_cluster::sleep_action::{BedRelease, WakeClaim};
    use pumpkin_data::block_properties::{BedPart, HorizontalFacing, WhiteBedLikeProperties};
    use pumpkin_data::tag::Taggable;
    let lease = super::cluster_world_owner::player_action_lease(server, player)?;
    let living = server.cluster_actor_living.load();
    let avatar = living
        .get(&pumpkin_cluster::actor::ActorId::Player(lease.gid))?
        .avatar
        .as_ref()?;
    let sleeping = avatar.presentation.sleeping?;
    let spatial = server.cluster_actor_spatial.load();
    let source = spatial.get(&lease.gid)?;
    let chunks = server.cluster_chunk_presentations.load();
    let target_at = |at: pumpkin_cluster::protocol::BlockPos| Target::Chunk {
        key: ChunkKey::new(avatar.world, at.x.div_euclid(16), at.z.div_euclid(16)),
        part: ChunkPart::Block(at),
        inventory: None,
    };
    let read = |at: pumpkin_cluster::protocol::BlockPos| {
        let chunk = ChunkKey::new(avatar.world, at.x.div_euclid(16), at.z.div_euclid(16));
        chunks.get(&chunk)?.blocks.block(chunk, at).ok()
    };
    let bed_key = ChunkKey::new(
        avatar.world,
        sleeping.bed.x.div_euclid(16),
        sleeping.bed.z.div_euclid(16),
    );
    let bed_entity = chunks
        .get(&bed_key)?
        .block_entities
        .iter()
        .find(|(position, _)| *position == sleeping.bed)
        .map(|(_, cell)| cell.clone());
    let mut beds = Vec::new();
    let bed = read(sleeping.bed)?;
    if bed
        .to_block()
        .has_tag(&pumpkin_data::tag::Block::MINECRAFT_BEDS)
    {
        let mut properties = WhiteBedLikeProperties::from_state_id(bed);
        let (dx, dz) = match properties.facing {
            HorizontalFacing::North => (0, -1),
            HorizontalFacing::South => (0, 1),
            HorizontalFacing::West => (-1, 0),
            HorizontalFacing::East => (1, 0),
        };
        let sign = if properties.part == BedPart::Head {
            -1
        } else {
            1
        };
        let other = pumpkin_cluster::protocol::BlockPos {
            x: sleeping.bed.x.checked_add(dx * sign)?,
            y: sleeping.bed.y,
            z: sleeping.bed.z.checked_add(dz * sign)?,
        };
        properties.occupied = false;
        beds.push(BedRelease {
            target: target_at(sleeping.bed),
            before: bed.as_u16(),
            after: properties.to_state_id(bed.to_block()).as_u16(),
        });
        let second = read(other)?;
        if second.to_block() == bed.to_block() {
            let mut properties = WhiteBedLikeProperties::from_state_id(second);
            properties.occupied = false;
            beds.push(BedRelease {
                target: target_at(other),
                before: second.as_u16(),
                after: properties.to_state_id(second.to_block()).as_u16(),
            });
        }
    }
    *sequence = sequence.wrapping_add(1);
    Some(ActionProposal {
        command: None,
        id: ActionId {
            actor: pumpkin_cluster::actor::ActorId::Player(lease.gid),
            incarnation: lease.incarnation,
            sequence: *sequence,
            family: 7,
        },
        tick,
        lease: Some(lease),
        client_origin: ClientOrigin::None,
        screen_generation: 0,
        claimed_targets: vec![Target::Chunk {
            key: source.truth.local.chunk,
            part: ChunkPart::Player(lease.gid),
            inventory: None,
        }],
        claim: ClaimedAction::Wake(WakeClaim {
            owner_install_generation: player
                .cluster_owner_install_generation
                .load(Ordering::Acquire),
            source_epoch: avatar.spatial_epoch,
            sleeping,
            beds,
            bed_entity,
            morning,
        }),
    })
}
