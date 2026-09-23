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

#[derive(Default)]
struct ControllerOwner {
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
}

pub struct ControllerRuntime {
    #[cfg(feature = "cluster-e2e")]
    filters: ArrayQueue<(GlobalPlayerId, [f64; 3])>,
    #[cfg(feature = "cluster-e2e")]
    departure_admin_holds: ArrayQueue<(GlobalPlayerId, u64)>,
    owner: ArrayQueue<ControllerOwner>,
    actor_intents: ArrayQueue<ControllerActorIntent>,
    view_intents: ArrayQueue<ViewIntent>,
    owner_unavailable_reported: AtomicBool,
    published: ArcSwap<ControllerSnapshot>,
    policy_results: Arc<ArrayQueue<PolicyResult>>,
    notices: ArrayQueue<ControllerNotice>,
    departure_requests: ArrayQueue<DepartureRequest>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum DepartureStatus {
    Ready,
    MissingPlayer,
    Duplicate,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum DepartureRequestError {
    QueueFull,
}

struct DepartureRequest {
    actor: GlobalPlayerId,
    expected_close: u64,
    logout: bool,
    respond: oneshot::Sender<DepartureStatus>,
}

struct DepartureTicket {
    respond: oneshot::Sender<DepartureStatus>,
    expected_close: u64,
    logout: bool,
    world: Option<oneshot::Receiver<super::cluster_world_owner::ActorDrainStatus>>,
    world_ready: bool,
    through_tick: TickStamp,
}

enum ControllerNotice {
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
    pub fn new(capacity: usize) -> Self {
        let owner = ArrayQueue::new(1);
        assert!(owner.push(ControllerOwner::default()).is_ok());
        Self {
            #[cfg(feature = "cluster-e2e")]
            filters: ArrayQueue::new(64),
            #[cfg(feature = "cluster-e2e")]
            departure_admin_holds: ArrayQueue::new(64),
            owner,
            actor_intents: ArrayQueue::new(capacity.saturating_mul(2).max(1)),
            view_intents: ArrayQueue::new(capacity.saturating_mul(2).max(1)),
            owner_unavailable_reported: AtomicBool::new(false),
            published: ArcSwap::from_pointee(ControllerSnapshot::default()),
            policy_results: Arc::new(ArrayQueue::new(capacity)),
            notices: ArrayQueue::new(capacity),
            departure_requests: ArrayQueue::new(usize::from(u16::MAX) + 1),
        }
    }

    pub fn request_player_departure(
        &self,
        actor: GlobalPlayerId,
        expected_close: u64,
        logout: bool,
    ) -> Result<oneshot::Receiver<DepartureStatus>, DepartureRequestError> {
        let (respond, receive) = oneshot::channel();
        self.departure_requests
            .push(DepartureRequest {
                actor,
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
        through_tick: TickStamp,
        logout: bool,
    ) -> Option<oneshot::Receiver<super::cluster_world_owner::ActorDrainStatus>> {
        let route = server.cluster_actor_drain_route.load_full()?;
        let (respond, receive) = oneshot::channel();
        route
            .try_send(super::cluster_world_owner::ActorDrainRequest {
                gid: actor,
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
        !self.actor_intents.is_full() && !self.view_intents.is_full()
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
        drain_session_inputs(server, &players, &mut session_progress);
        departing.retain(|actor| players.contains_key(actor));
        frames.retain(|actor, _| players.contains_key(actor));
        applied.retain(|actor, _| players.contains_key(actor));
        saturation_last.retain(|actor, _| players.contains_key(actor));
        pending_policy.retain(|actor, _| players.contains_key(actor));
        book_preflights.retain(|actor, _| players.contains_key(actor));
        awaiting_teleport.retain(|actor, _| players.contains_key(actor));
        abilities.retain(|actor, _| players.contains_key(actor));
        #[cfg(feature = "cluster-e2e")]
        offsets.retain(|actor, _| players.contains_key(actor));
        #[cfg(feature = "cluster-e2e")]
        logged_divergence.retain(|actor| players.contains_key(actor));
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
            administrative_states
                .entry(*actor)
                .or_insert_with(|| super::cluster_player_admin::capture_initial_state(player));
            if let Some(state) = administrative_states.get_mut(actor) {
                state.set_living(super::cluster_player_admin::capture_living_projection(
                    player,
                ));
            }
            while let Some(result) = player.cluster_administrative_results.pop() {
                if let Some(pending) = pending_administrative.get_mut(&result.action) {
                    pending.living_result = Some(result.applied);
                }
            }
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
                let recipients = administrative_recipients(
                    administrative_origins.get(&resolution.action).copied(),
                    resolution.holders.clone(),
                );
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
            let player_phase_applied = current.living() == resolution.update.forward.living()
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
                    recipients: administrative_recipients(
                        administrative_origins.remove(&resolution.action),
                        resolution.holders,
                    ),
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
            if !self.has_intent_capacity() {
                continue;
            }
            let Some(player) = players.get(&pending.update.target) else {
                continue;
            };
            let Some(state) =
                apply_administrative_controller(player, &pending.update.forward, &mut abilities)
            else {
                continue;
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
                frame,
                ControlChanges {
                    position_changed: true,
                    velocity_changed: true,
                    rotation_changed: true,
                    ..ControlChanges::default()
                },
                ControllerActorSource::AcceptedAdministrative(action),
                state.world,
                tick,
                abilities
                    .get(&pending.update.target)
                    .is_some_and(|abilities| abilities.flying),
                Some(state.gamemode),
            );
            assert!(self.stage_actor_intent(server, player, intent));
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
            let effective_flying = result.controls.flying.unwrap_or_else(|| {
                abilities
                    .get(&result.actor)
                    .is_some_and(|abilities| abilities.flying)
            });
            let intent = controller_actor_intent(
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
            if let Some(value) = result.controls.flying {
                let state = abilities.entry(result.actor).or_default();
                state.flying = value;
                player.send_abilities_snapshot(*state);
            }
            apply_legacy_controller_projection(player, intent);
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
                while player.cluster_controller_inputs.pop().is_some() {}
                crate::server::cluster_inventory_protocol::drain_inventory_inputs(
                    server,
                    player,
                    tick,
                    action_sequences.entry(*actor).or_insert(0),
                    effective_item_frame(&frames, *actor, player, tick),
                    book_preflights.entry(*actor).or_default(),
                );
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
                    effective_item_frame(&frames, *actor, player, tick),
                    book_preflights.entry(*actor).or_default(),
                );
                continue;
            }

            while let Some(patch) = player.cluster_controller_inputs.pop() {
                if let ControllerPatch::TeleportConfirm(id) = patch {
                    match awaiting_teleport.remove(actor) {
                        Some((expected, position)) if expected == id => {
                            player.get_entity().set_pos(position);
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
                    || player.living_entity.dead.load(Ordering::Relaxed)
                    || player.living_entity.health.load() <= 0.0
                    || player.is_movement_locked.load(Ordering::Relaxed)
                {
                    continue;
                }
                if patch.moves()
                    && (player.get_entity().has_vehicle() || awaiting_teleport.contains_key(actor))
                {
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
            if !changed {
                crate::server::cluster_inventory_protocol::drain_inventory_inputs(
                    server,
                    player,
                    tick,
                    action_sequences.entry(*actor).or_insert(0),
                    effective_item_frame(&frames, *actor, player, tick),
                    book_preflights.entry(*actor).or_default(),
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
            preserve_unchanged_effective(player, &mut frame, &controls);
            let entity = player.get_entity();
            let position = entity.pos.load();
            let velocity = entity.velocity.load();
            controls.position_changed |=
                frame.effective.position != [position.x, position.y, position.z];
            controls.velocity_changed |=
                frame.effective.velocity != [velocity.x, velocity.y, velocity.z];
            controls.rotation_changed |= frame.effective.yaw != entity.yaw.load()
                || frame.effective.pitch != entity.pitch.load();
            controls.ground_changed |=
                frame.effective.on_ground != entity.on_ground.load(Ordering::Relaxed);
            if requires_policy(server, &frame, &controls, player) {
                next_policy_generation = next_policy_generation.wrapping_add(1);
                pending_policy.insert(*actor, next_policy_generation);
                spawn_policy(
                    server.clone(),
                    player.clone(),
                    next_policy_generation,
                    position,
                    frame,
                    controls,
                    self.policy_results.clone(),
                );
                continue;
            }
            let effective_flying = controls.flying.unwrap_or_else(|| {
                abilities
                    .get(actor)
                    .is_some_and(|abilities| abilities.flying)
            });
            let intent = controller_actor_intent(
                frame,
                controls,
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
                continue;
            }
            if let Some(value) = controls.flying {
                let state = abilities.entry(*actor).or_default();
                state.flying = value;
                player.send_abilities_snapshot(*state);
            }
            apply_legacy_controller_projection(player, intent);
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
                effective_item_frame(&frames, *actor, player, tick),
                book_preflights.entry(*actor).or_default(),
            );
        }
        while let Some(request) = self.departure_requests.pop() {
            if !players.contains_key(&request.actor) {
                let _ = request.respond.send(DepartureStatus::MissingPlayer);
            } else if departing.contains(&request.actor) {
                let _ = request.respond.send(DepartureStatus::Duplicate);
            } else {
                departing.insert(request.actor);
                departures.insert(
                    request.actor,
                    DepartureTicket {
                        respond: request.respond,
                        expected_close: request.expected_close,
                        logout: request.logout,
                        world: None,
                        world_ready: false,
                        through_tick: tick,
                    },
                );
            }
        }
        let mut departures_ready = Vec::new();
        for (actor, departure) in &mut departures {
            let world_ready_on_prior_advance = departure.world_ready;
            if !departure.world_ready {
                if departure.world.is_none()
                    && players.get(actor).is_some_and(|player| {
                        player.cluster_inventory_inputs.is_empty()
                            && player.cluster_inventory_pending_close.is_empty()
                            && player.cluster_inventory_departure_close.is_empty()
                            && player.cluster_session_staged.is_empty()
                    })
                    && !session_progress
                        .get(actor)
                        .is_some_and(session_progress_pending)
                {
                    departure.world =
                        Self::try_request_actor_drain(server, *actor, tick, departure.logout);
                    if departure.world.is_some() {
                        departure.through_tick = tick;
                    }
                }
                if let Some(world) = departure.world.as_mut() {
                    match world.try_recv() {
                        Ok(super::cluster_world_owner::ActorDrainStatus::Ready) => {
                            departure.world_ready = true;
                        }
                        Err(oneshot::error::TryRecvError::Closed) => departure.world = None,
                        Err(oneshot::error::TryRecvError::Empty) => {}
                    }
                }
            }
            let local_pending = administrative_origins
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
                    || !player.cluster_session_staged.is_empty()
            });
            let close_accepted = server
                .cluster_inventory_close_status
                .load()
                .get(&(*actor, departure.expected_close))
                .is_some_and(|status| status.token == departure.expected_close);
            if world_ready_on_prior_advance
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
        let departure_signals: Vec<_> = departures_ready
            .into_iter()
            .filter_map(|actor| {
                let respond = departures.remove(&actor)?.respond;
                server.cluster_inventory_close_status.rcu(|current| {
                    let mut next = (**current).clone();
                    next.retain(|(entry_actor, _), _| *entry_actor != actor);
                    Arc::new(next)
                });
                Some(respond)
            })
            .collect();
        self.published.store(Arc::new(ControllerSnapshot {
            frames: frames.clone(),
            applied: applied.clone(),
        }));
        assert!(
            self.owner
                .push(ControllerOwner {
                    frames,
                    applied,
                    saturation_last,
                    pending_policy,
                    next_policy_generation,
                    book_preflights,
                    action_sequences,
                    session_progress,
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
        for respond in departure_signals {
            let _ = respond.send(DepartureStatus::Ready);
        }
    }
}

fn effective_item_frame(
    frames: &HashMap<GlobalPlayerId, ControllerFrame>,
    actor: GlobalPlayerId,
    player: &Player,
    tick: TickStamp,
) -> Option<(ControllerFrame, f64)> {
    let mut frame = frames.get(&actor).copied()?;
    frame.effective.tick = tick;
    Some((frame, player.get_entity().get_eye_height()))
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
    let current_world = player.world();
    let entity = player.get_entity();
    let current_position = entity.pos.load();
    if super::cluster_world_delta::world_id(&current_world) != target.world {
        return None;
    } else if [current_position.x, current_position.y, current_position.z] != target.position
        || entity.yaw.load() != target.yaw
        || entity.pitch.load() != target.pitch
    {
        if !player.request_cluster_teleport(
            Vector3::new(target.position[0], target.position[1], target.position[2]),
            target.yaw,
            target.pitch,
        ) {
            return None;
        }
    }
    entity.velocity.store(Vector3::new(
        target.velocity[0],
        target.velocity[1],
        target.velocity[2],
    ));
    let gamemode = pumpkin_util::GameMode::try_from(target.gamemode).ok()?;
    if player.gamemode.load() != gamemode {
        let state = abilities.entry(actor).or_default();
        player.apply_cluster_gamemode(gamemode, state);
    }
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
    state.world = super::cluster_world_delta::world_id(&player.world());
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

fn preserve_unchanged_effective(
    player: &Player,
    frame: &mut ControllerFrame,
    controls: &ControlChanges,
) {
    let entity = player.get_entity();
    let position = entity.pos.load();
    let velocity = entity.velocity.load();
    let current = EffectiveControllerState {
        actor: frame.effective.actor,
        source_seq: frame.effective.source_seq,
        tick: frame.effective.tick,
        position: [position.x, position.y, position.z],
        velocity: [velocity.x, velocity.y, velocity.z],
        yaw: entity.yaw.load(),
        pitch: entity.pitch.load(),
        on_ground: entity.on_ground.load(Ordering::Relaxed),
        input_flags: player.last_input.load(Ordering::Relaxed) as u8 as u64,
    };
    preserve_unclaimed_fields(frame, controls, current);
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
    let entity = player.get_entity();
    let position = entity.pos.load();
    let velocity = entity.velocity.load();
    ClientClaimedState {
        actor,
        seq: PlayerSeq(u16::MAX),
        tick,
        position: [position.x, position.y, position.z],
        velocity: [velocity.x, velocity.y, velocity.z],
        yaw: entity.yaw.load(),
        pitch: entity.pitch.load(),
        on_ground: entity.on_ground.load(Ordering::Relaxed),
        input_flags: player.last_input.load(Ordering::Relaxed) as u8 as u64,
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
        ControllerPatch::TeleportConfirm(_) => return Some((state, controls)),
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
    player: &Player,
) -> bool {
    use crate::plugin::api::events::player::player_input::PlayerInputEvent;
    use crate::plugin::api::events::player::player_move::PlayerMoveEvent;
    use crate::plugin::api::events::player::player_statistic_increment::PlayerStatisticIncrementEvent;
    use crate::plugin::api::events::player::player_toggle_flight_event::PlayerToggleFlightEvent;
    use crate::plugin::api::events::player::player_toggle_sneak_event::PlayerToggleSneakEvent;

    let position = player.get_entity().pos.load();
    controls.position_changed
        && frame.effective.position != [position.x, position.y, position.z]
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
    results: Arc<ArrayQueue<PolicyResult>>,
) {
    let actor = frame.claimed.actor;
    let runtime = server.runtime.clone();
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
                let statistic = player.get_movement_statistic();
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
                    player.last_input.load(Ordering::Relaxed) as u8 as u64;
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
        let (frame, controls, timed_out) = match tokio::time::timeout(std::time::Duration::from_secs(5), policy).await {
            Ok((frame, controls)) => (frame, controls, false),
            Err(_) => (fallback_frame, fallback_controls, true),
        };
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
    frame: ControllerFrame,
    controls: ControlChanges,
    source: ControllerActorSource,
    world: pumpkin_cluster::protocol::WorldId,
    tick: TickStamp,
    flying: bool,
    gamemode: Option<u8>,
) -> ControllerActorIntent {
    ControllerActorIntent {
        actor: frame.claimed.actor,
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
    let world = player.world();
    ViewIntent {
        actor: actor.actor,
        world: actor.world,
        center: pumpkin_cluster::protocol::ChunkAddr {
            x: (position[0].floor() as i32).div_euclid(16),
            z: (position[2].floor() as i32).div_euclid(16),
        },
        radius: crate::world::chunker::get_view_distance(player),
        spectator_generates: !player.is_spectator()
            || world
                .level_info
                .load()
                .game_rules
                .spectators_generate_chunks,
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

fn apply_legacy_controller_projection(player: &Arc<Player>, intent: ControllerActorIntent) {
    let frame = intent.frame;
    let flying = intent.effective_flying;
    let state = frame.effective;
    let entity = player.get_entity();
    let old_position = entity.pos.load();
    let old_on_ground = entity.on_ground.load(Ordering::Relaxed);
    let new_position = Vector3::new(state.position[0], state.position[1], state.position[2]);
    let position_changed = intent.position_changed && new_position != old_position;
    let old_yaw = entity.yaw.load();
    let old_pitch = entity.pitch.load();
    let rotation_changed =
        intent.rotation_changed && (state.yaw != old_yaw || state.pitch != old_pitch);
    if intent.position_changed {
        entity.set_pos(new_position);
    }
    if intent.velocity_changed {
        entity.velocity.store(Vector3::new(
            state.velocity[0],
            state.velocity[1],
            state.velocity[2],
        ));
    }
    if intent.rotation_changed {
        entity.set_rotation(state.yaw, state.pitch);
    }
    if intent.ground_changed {
        entity.on_ground.store(state.on_ground, Ordering::Relaxed);
    }
    player
        .last_input
        .store(state.input_flags as i8, Ordering::Relaxed);
    if let Some(sneaking) = intent.sneaking {
        entity.set_sneaking(sneaking);
        if sneaking
            && player.gamemode.load() == pumpkin_util::GameMode::Spectator
            && player.camera_target_id.load().is_some()
        {
            player.camera_target_id.store(None);
            player.try_send_client_packet(&pumpkin_protocol::java::client::play::CSetCamera::new(
                player.entity_id().into(),
            ));
        }
    }
    if let Some(sprinting) = intent.sprinting {
        player.set_sprinting(sprinting);
    }
    if let Some(crawling) = intent.crawling {
        if crawling {
            entity.set_pose(pumpkin_data::entity::EntityPose::Swimming);
        } else {
            player.update_player_pose();
        }
    }
    let delta = new_position - old_position;
    if position_changed {
        let distance = old_position.squared_distance_to_vec(&new_position).sqrt();
        let centimeters = (distance * 100.0) as i32;
        let centimeters = match intent.movement_statistic {
            ControllerMovementStatistic::Unchecked => centimeters,
            ControllerMovementStatistic::Cancelled => 0,
            ControllerMovementStatistic::Amount(amount) => amount,
        };
        let height_difference = new_position.y - old_position.y;
        if old_on_ground && !state.on_ground && height_difference > 0.0 {
            if player
                .cluster_controller_effects
                .push(ControllerPlayerEffect::Jump)
                .is_err()
            {
                player.kick(
                    crate::net::DisconnectReason::Unknown,
                    &pumpkin_util::text::TextComponent::text("Controller effect queue saturated"),
                );
                return;
            }
        }
        if state.on_ground && entity.is_fall_flying() {
            entity.set_fall_flying(false);
        }
        crate::world::chunker::update_position(player);
        if centimeters != 0 || delta != Vector3::new(0.0, 0.0, 0.0) {
            if player
                .cluster_controller_effects
                .push(ControllerPlayerEffect::Movement {
                    amount: centimeters,
                    delta,
                    height_difference,
                    on_ground: state.on_ground,
                    flying,
                    creative: player.gamemode.load() == pumpkin_util::GameMode::Creative,
                    position: new_position,
                })
                .is_err()
            {
                player.kick(
                    crate::net::DisconnectReason::Unknown,
                    &pumpkin_util::text::TextComponent::text("Controller effect queue saturated"),
                );
                return;
            }
        }
    }
    if position_changed || rotation_changed {
        broadcast_movement(
            player,
            old_position,
            new_position,
            position_changed,
            rotation_changed,
            state.yaw,
            state.pitch,
            state.on_ground,
        );
    }
}

#[expect(clippy::too_many_arguments)]
fn broadcast_movement(
    player: &Player,
    old_position: Vector3<f64>,
    new_position: Vector3<f64>,
    position_changed: bool,
    rotation_changed: bool,
    yaw: f32,
    pitch: f32,
    on_ground: bool,
) {
    use pumpkin_protocol::bedrock::client::CMovePlayer;
    use pumpkin_protocol::codec::var_ulong::VarULong;
    use pumpkin_protocol::java::client::play::{
        CEntityPositionSync, CHeadRot, CUpdateEntityPos, CUpdateEntityPosRot, CUpdateEntityRot,
    };

    let entity = player.get_entity();
    let world = player.world();
    let entity_id = player.entity_id();
    let protocol_yaw = (yaw * 256.0 / 360.0).rem_euclid(256.0);
    let protocol_pitch = (pitch * 256.0 / 360.0).rem_euclid(256.0);
    let bedrock = CMovePlayer::new(
        VarULong(entity_id as u64),
        Vector3::new(
            new_position.x as f32,
            new_position.y as f32 + entity.entity_type.eye_height,
            new_position.z as f32,
        ),
        pitch,
        yaw,
        yaw,
        if position_changed {
            CMovePlayer::MODE_NORMAL
        } else {
            CMovePlayer::MODE_ROTATION
        },
        on_ground,
        VarULong(0),
        0,
        0,
        VarULong(0),
    );
    let delta = new_position - old_position;
    if position_changed && delta.length_squared() >= 64.0 {
        world.send_to_tracking_players_editioned(
            entity,
            &CEntityPositionSync::new(
                entity_id.into(),
                new_position,
                Vector3::new(0.0, 0.0, 0.0),
                yaw,
                pitch,
                on_ground,
            ),
            &bedrock,
        );
    } else if position_changed && rotation_changed {
        world.send_to_tracking_players_editioned(
            entity,
            &CUpdateEntityPosRot::new(
                entity_id.into(),
                Vector3::new(
                    new_position.x.mul_add(4096.0, -(old_position.x * 4096.0)) as i16,
                    new_position.y.mul_add(4096.0, -(old_position.y * 4096.0)) as i16,
                    new_position.z.mul_add(4096.0, -(old_position.z * 4096.0)) as i16,
                ),
                protocol_yaw as u8,
                protocol_pitch as u8,
                on_ground,
            ),
            &bedrock,
        );
    } else if position_changed {
        world.send_to_tracking_players_editioned(
            entity,
            &CUpdateEntityPos::new(
                entity_id.into(),
                Vector3::new(
                    new_position.x.mul_add(4096.0, -(old_position.x * 4096.0)) as i16,
                    new_position.y.mul_add(4096.0, -(old_position.y * 4096.0)) as i16,
                    new_position.z.mul_add(4096.0, -(old_position.z * 4096.0)) as i16,
                ),
                on_ground,
            ),
            &bedrock,
        );
    } else if rotation_changed {
        world.send_to_tracking_players_editioned(
            entity,
            &CUpdateEntityRot::new(
                entity_id.into(),
                protocol_yaw as u8,
                protocol_pitch as u8,
                on_ground,
            ),
            &bedrock,
        );
    }
    if rotation_changed {
        world
            .send_to_tracking_players(entity, &CHeadRot::new(entity_id.into(), protocol_yaw as u8));
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
                actor: claimed().actor.into(),
                sequence: 1,
                family: 7,
            },
            applied: true,
        };
        let second = LivingAdministrativeResult {
            action: ActionKey {
                actor: claimed().actor.into(),
                sequence: 2,
                family: 7,
            },
            applied: true,
        };
        let third = LivingAdministrativeResult {
            action: ActionKey {
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
