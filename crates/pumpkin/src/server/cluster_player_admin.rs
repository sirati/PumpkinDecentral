use std::collections::{HashMap, HashSet};
use std::sync::Arc;

use arc_swap::ArcSwapOption;
use crossbeam::queue::ArrayQueue;
use pumpkin_cluster::completion::ActionKey;
use pumpkin_cluster::identity::{GlobalPlayerId, PlayerSeq, ServerId};
use pumpkin_cluster::player_admin::{
    EffectState, PlayerAdministrativeAction, PlayerAdministrativeIntent,
    PlayerAdministrativeMutation, PlayerAdministrativePublication, PlayerAdministrativeState,
    PlayerLivingAdministrativeState,
};
use pumpkin_cluster::protocol::ChunkKey;
use pumpkin_cluster::protocol::StreamKind;
use pumpkin_cluster::streams::{OutboundParcel, StreamHeader};
use pumpkin_cluster::time::TickStamp;
use tokio::sync::{broadcast, mpsc};

use serde::{Deserialize, Serialize};

use super::Server;
use crate::entity::{EntityBase, player::Player};

pub type PlayerAdministrativeActionRoute = mpsc::Sender<PlayerAdministrativeAction>;

#[derive(Debug)]
pub enum PlayerAdministrativeSubmitError {
    NoRoute(PlayerAdministrativeIntent),
    QueueFull(PlayerAdministrativeIntent),
    Unauthorized(PlayerAdministrativeIntent),
}

pub struct PlayerAdministrativeRuntime {
    intents: ArrayQueue<RoutedAdministrativeIntent>,
    action_route: ArcSwapOption<PlayerAdministrativeActionRoute>,
    intent_routes: arc_swap::ArcSwap<HashMap<ServerId, mpsc::Sender<PlayerAdministrativeIntent>>>,
    outbound: ArcSwapOption<mpsc::Sender<OutboundParcel>>,
    publications: broadcast::Sender<PlayerAdministrativePublication>,
    failures: broadcast::Sender<PlayerAdministrativeIntentFailure>,
    resolutions: ArrayQueue<PlayerAdministrativeResolution>,
}

#[derive(Clone)]
pub(crate) struct PlayerAdministrativeResolution {
    pub action: ActionKey,
    pub update: PlayerAdministrativeAction,
    pub accepted: bool,
    pub holders: Vec<ServerId>,
}

pub(crate) enum PlayerAdministrativeQueueError {
    Full(PlayerAdministrativeResolution),
}

struct RoutedAdministrativeIntent {
    source: ServerId,
    intent: PlayerAdministrativeIntent,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct PlayerAdministrativeIntentFailure {
    pub source: ServerId,
    pub target: GlobalPlayerId,
    pub origin: Option<u64>,
    pub reason: PlayerAdministrativeFailureReason,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub enum PlayerAdministrativeFailureReason {
    NoTarget,
    PendingAuthority,
    Unauthorized,
    QueueFull,
}

impl PlayerAdministrativeRuntime {
    pub fn new(capacity: usize) -> Self {
        let (publications, _) = broadcast::channel(capacity);
        let (failures, _) = broadcast::channel(capacity);
        Self {
            intents: ArrayQueue::new(capacity),
            action_route: ArcSwapOption::empty(),
            intent_routes: arc_swap::ArcSwap::from_pointee(HashMap::new()),
            outbound: ArcSwapOption::empty(),
            publications,
            failures,
            resolutions: ArrayQueue::new(capacity),
        }
    }

    pub fn attach_action_route(&self, route: PlayerAdministrativeActionRoute) {
        self.action_route.store(Some(Arc::new(route)));
    }

    pub fn attach_intent_route(
        &self,
        target: ServerId,
        route: mpsc::Sender<PlayerAdministrativeIntent>,
    ) {
        let mut routes = (*self.intent_routes.load_full()).clone();
        routes.insert(target, route);
        self.intent_routes.store(Arc::new(routes));
    }

    pub fn install_transport(&self, outbound: mpsc::Sender<OutboundParcel>) {
        self.outbound.store(Some(Arc::new(outbound)));
    }

    pub fn accept_remote_intent(
        &self,
        source: ServerId,
        intent: PlayerAdministrativeIntent,
    ) -> Result<(), PlayerAdministrativeSubmitError> {
        self.intents
            .push(RoutedAdministrativeIntent { source, intent })
            .map_err(|routed| PlayerAdministrativeSubmitError::QueueFull(routed.intent))
    }

    pub fn publish(&self, publication: PlayerAdministrativePublication) {
        let _ = self.publications.send(publication);
    }

    pub fn publish_failure(&self, failure: PlayerAdministrativeIntentFailure) {
        let _ = self.failures.send(failure);
    }

    pub(crate) fn queue_resolution(
        &self,
        action: ActionKey,
        update: PlayerAdministrativeAction,
        accepted: bool,
        holders: Vec<ServerId>,
    ) -> Result<(), PlayerAdministrativeQueueError> {
        self.resolutions
            .push(PlayerAdministrativeResolution {
                action,
                update,
                accepted,
                holders,
            })
            .map_err(PlayerAdministrativeQueueError::Full)
    }

    pub(crate) fn pop_resolution(&self) -> Option<PlayerAdministrativeResolution> {
        self.resolutions.pop()
    }

    pub(crate) fn has_queued_resolutions(&self) -> bool {
        !self.resolutions.is_empty()
    }
}

pub fn submit_player_administrative(
    server: &Server,
    intent: PlayerAdministrativeIntent,
) -> Result<(), PlayerAdministrativeSubmitError> {
    if intent.target.server != ServerId(server.advanced_config.cluster.server_id) {
        if server.advanced_config.cluster.role != pumpkin_config::ClusterRole::Primary {
            return Err(PlayerAdministrativeSubmitError::NoRoute(intent));
        }
        if let Some(route) = server
            .cluster_player_admin
            .intent_routes
            .load()
            .get(&intent.target.server)
            .cloned()
        {
            return route
                .try_send(intent)
                .map_err(|error| PlayerAdministrativeSubmitError::QueueFull(error.into_inner()));
        }
        let Some(outbound) = server.cluster_player_admin.outbound.load_full() else {
            return Err(PlayerAdministrativeSubmitError::NoRoute(intent));
        };
        let peer = intent.target.server;
        let source = ServerId(server.advanced_config.cluster.server_id);
        let Ok(bytes) = encode_intent(source, &intent) else {
            return Err(PlayerAdministrativeSubmitError::NoRoute(intent));
        };
        return outbound
            .try_send(OutboundParcel {
                peer,
                header: StreamHeader::new(StreamKind::Control, Some(intent.target)),
                bytes,
            })
            .map_err(|_| PlayerAdministrativeSubmitError::QueueFull(intent));
    }
    server
        .cluster_player_admin
        .intents
        .push(RoutedAdministrativeIntent {
            source: ServerId(server.advanced_config.cluster.server_id),
            intent,
        })
        .map_err(|routed| PlayerAdministrativeSubmitError::QueueFull(routed.intent))
}

pub fn submit_lobby_player_administrative(
    server: &Server,
    issuer: GlobalPlayerId,
    issuer_uuid: uuid::Uuid,
    intent: PlayerAdministrativeIntent,
) -> Result<(), PlayerAdministrativeSubmitError> {
    let local = ServerId(server.advanced_config.cluster.server_id);
    if issuer.server != local {
        return Err(PlayerAdministrativeSubmitError::Unauthorized(intent));
    }
    if intent.target.server == local {
        return submit_player_administrative(server, intent);
    }
    let Some(outbound) = server.cluster_player_admin.outbound.load_full() else {
        return Err(PlayerAdministrativeSubmitError::NoRoute(intent));
    };
    let frame = LobbyPlayerAdministrativeIntentFrame {
        source: local,
        issuer,
        issuer_uuid: *issuer_uuid.as_bytes(),
        permission_revision: super::cluster_presence::permission_revision(server),
        intent: intent.clone(),
    };
    let Ok(payload) = postcard::to_allocvec(&frame) else {
        return Err(PlayerAdministrativeSubmitError::NoRoute(intent));
    };
    let mut bytes = Vec::with_capacity(LOBBY_INTENT_PREFIX.len() + payload.len());
    bytes.extend_from_slice(LOBBY_INTENT_PREFIX);
    bytes.extend_from_slice(&payload);
    outbound
        .try_send(OutboundParcel {
            peer: intent.target.server,
            header: StreamHeader::new(StreamKind::Control, Some(intent.target)),
            bytes,
        })
        .map_err(|_| PlayerAdministrativeSubmitError::QueueFull(intent))
}

pub fn authorize_lobby_intent(
    server: &Server,
    frame: &LobbyPlayerAdministrativeIntentFrame,
    peer: ServerId,
    generation: u64,
) -> Result<(), PlayerAdministrativeFailureReason> {
    let local = ServerId(server.advanced_config.cluster.server_id);
    if frame.source != peer
        || frame.issuer.server != peer
        || frame.intent.target.server != local
        || server.cluster_peer_generations.load().get(&peer).copied() != Some(generation)
    {
        return Err(PlayerAdministrativeFailureReason::Unauthorized);
    }
    let presence = server.cluster_presence.snapshot();
    let Some(issuer) = presence.entries.get(&frame.issuer) else {
        return Err(PlayerAdministrativeFailureReason::PendingAuthority);
    };
    if issuer.uuid != frame.issuer_uuid {
        return Err(PlayerAdministrativeFailureReason::Unauthorized);
    }
    if presence.permission_revision != frame.permission_revision {
        return Err(PlayerAdministrativeFailureReason::PendingAuthority);
    }
    if presence.permission_for_uuid(&frame.issuer_uuid) < pumpkin_util::PermissionLvl::Two {
        return Err(PlayerAdministrativeFailureReason::Unauthorized);
    }
    Ok(())
}

const INTENT_PREFIX: &[u8; 8] = b"PAdmInt1";
const LOBBY_INTENT_PREFIX: &[u8; 8] = b"PAdmReq1";
const PUBLICATION_PREFIX: &[u8; 8] = b"PAdmPub1";
const FAILURE_PREFIX: &[u8; 8] = b"PAdmErr1";

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct PlayerAdministrativeIntentFrame {
    pub source: ServerId,
    pub intent: PlayerAdministrativeIntent,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct LobbyPlayerAdministrativeIntentFrame {
    pub source: ServerId,
    pub issuer: GlobalPlayerId,
    pub issuer_uuid: [u8; 16],
    pub permission_revision: u64,
    pub intent: PlayerAdministrativeIntent,
}

pub fn decode_lobby_intent(bytes: &[u8]) -> Option<LobbyPlayerAdministrativeIntentFrame> {
    bytes
        .strip_prefix(LOBBY_INTENT_PREFIX)
        .and_then(|payload| postcard::from_bytes(payload).ok())
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct PlayerAdministrativePublicationFrame {
    pub source: ServerId,
    pub publication: PlayerAdministrativePublication,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct PlayerAdministrativeFailureFrame {
    pub source: ServerId,
    pub failure: PlayerAdministrativeIntentFailure,
}

fn encode_intent(
    source: ServerId,
    intent: &PlayerAdministrativeIntent,
) -> Result<Vec<u8>, postcard::Error> {
    let payload = postcard::to_allocvec(&PlayerAdministrativeIntentFrame {
        source,
        intent: intent.clone(),
    })?;
    let mut bytes = Vec::with_capacity(INTENT_PREFIX.len() + payload.len());
    bytes.extend_from_slice(INTENT_PREFIX);
    bytes.extend_from_slice(&payload);
    Ok(bytes)
}

pub fn decode_intent(bytes: &[u8]) -> Option<PlayerAdministrativeIntentFrame> {
    bytes
        .strip_prefix(INTENT_PREFIX)
        .and_then(|payload| postcard::from_bytes(payload).ok())
}

pub fn send_publication(
    server: &Server,
    peer: ServerId,
    publication: &PlayerAdministrativePublication,
) -> bool {
    let Some(outbound) = server.cluster_player_admin.outbound.load_full() else {
        return false;
    };
    let frame = PlayerAdministrativePublicationFrame {
        source: ServerId(server.advanced_config.cluster.server_id),
        publication: publication.clone(),
    };
    let Ok(payload) = postcard::to_allocvec(&frame) else {
        return false;
    };
    let mut bytes = Vec::with_capacity(PUBLICATION_PREFIX.len() + payload.len());
    bytes.extend_from_slice(PUBLICATION_PREFIX);
    bytes.extend_from_slice(&payload);
    outbound
        .try_send(OutboundParcel {
            peer,
            header: StreamHeader::new(StreamKind::Control, Some(publication.target)),
            bytes,
        })
        .is_ok()
}

pub fn decode_publication(bytes: &[u8]) -> Option<PlayerAdministrativePublicationFrame> {
    bytes
        .strip_prefix(PUBLICATION_PREFIX)
        .and_then(|payload| postcard::from_bytes(payload).ok())
}

pub fn send_failure(
    server: &Server,
    peer: ServerId,
    failure: &PlayerAdministrativeIntentFailure,
) -> bool {
    let frame = PlayerAdministrativeFailureFrame {
        source: ServerId(server.advanced_config.cluster.server_id),
        failure: failure.clone(),
    };
    let Ok(payload) = postcard::to_allocvec(&frame) else {
        return false;
    };
    let mut bytes = Vec::with_capacity(FAILURE_PREFIX.len() + payload.len());
    bytes.extend_from_slice(FAILURE_PREFIX);
    bytes.extend_from_slice(&payload);
    server
        .cluster_global_outbound
        .try_event(server, vec![peer], bytes)
}

pub fn reject_lobby_intent(
    server: &Server,
    source: ServerId,
    intent: &PlayerAdministrativeIntent,
    reason: PlayerAdministrativeFailureReason,
) -> bool {
    let failure = PlayerAdministrativeIntentFailure {
        source,
        target: intent.target,
        origin: intent.origin,
        reason,
    };
    if source == ServerId(server.advanced_config.cluster.server_id) {
        server.cluster_player_admin.publish_failure(failure);
        true
    } else {
        send_failure(server, source, &failure)
    }
}

pub fn decode_failure(bytes: &[u8]) -> Option<PlayerAdministrativeFailureFrame> {
    bytes
        .strip_prefix(FAILURE_PREFIX)
        .and_then(|payload| postcard::from_bytes(payload).ok())
}

pub fn authorized_failure_frame(
    frame: &PlayerAdministrativeFailureFrame,
    peer: ServerId,
    generation: u64,
    current_generation: Option<u64>,
    local: ServerId,
) -> bool {
    frame.source == peer
        && frame.failure.target.server == peer
        && frame.failure.source == local
        && current_generation == Some(generation)
}

pub fn authorized_publication_frame(
    frame: &PlayerAdministrativePublicationFrame,
    peer: ServerId,
    generation: u64,
    current_generation: Option<u64>,
) -> bool {
    frame.source == peer
        && frame.publication.target.server == peer
        && current_generation == Some(generation)
}

pub fn authorized_intent_frame(
    frame: &PlayerAdministrativeIntentFrame,
    peer: ServerId,
    generation: u64,
    current_generation: Option<u64>,
    local: ServerId,
    source_is_primary: bool,
) -> bool {
    source_is_primary
        && frame.source == peer
        && frame.intent.target.server == local
        && current_generation == Some(generation)
}

pub fn subscribe_player_administrative(
    server: &Server,
) -> broadcast::Receiver<PlayerAdministrativePublication> {
    server.cluster_player_admin.publications.subscribe()
}

pub fn subscribe_player_administrative_failures(
    server: &Server,
) -> broadcast::Receiver<PlayerAdministrativeIntentFailure> {
    server.cluster_player_admin.failures.subscribe()
}

pub(crate) fn capture_living_projection(player: &Player) -> PlayerLivingAdministrativeState {
    PlayerLivingAdministrativeState {
        health: player.living_entity.health.load(),
        absorption: player.get_absorption(),
        food: player.get_food_level(),
        saturation: player.get_food_saturation(),
        gamemode: player.gamemode.load() as u8,
        effects: player
            .get_active_effects()
            .into_iter()
            .map(|effect| EffectState {
                kind: u16::from(effect.effect_type.id),
                duration: effect.duration,
                amplifier: effect.amplifier,
                ambient: effect.ambient,
                show_particles: effect.show_particles,
                show_icon: effect.show_icon,
                blend: effect.blend,
            })
            .collect(),
        fall_distance: player.living_entity.fall_distance.load(),
        on_fire: player.get_entity().is_on_fire(),
    }
}

pub(crate) fn capture_initial_state(player: &Player) -> PlayerAdministrativeState {
    let entity = player.get_entity();
    let position = entity.pos.load();
    let velocity = entity.velocity.load();
    let living = capture_living_projection(player);
    PlayerAdministrativeState {
        world: super::cluster_world_delta::world_id(&player.world()),
        position: [position.x, position.y, position.z],
        yaw: entity.yaw.load(),
        pitch: entity.pitch.load(),
        velocity: [velocity.x, velocity.y, velocity.z],
        health: living.health,
        absorption: living.absorption,
        food: living.food,
        saturation: living.saturation,
        gamemode: living.gamemode,
        effects: living.effects,
        fall_distance: living.fall_distance,
        on_fire: living.on_fire,
    }
}

fn publish_no_target_failure(
    server: &Server,
    source: ServerId,
    intent: &PlayerAdministrativeIntent,
) {
    let failure = PlayerAdministrativeIntentFailure {
        source,
        target: intent.target,
        origin: intent.origin,
        reason: PlayerAdministrativeFailureReason::NoTarget,
    };
    if source == ServerId(server.advanced_config.cluster.server_id) {
        server.cluster_player_admin.publish_failure(failure);
    } else {
        let _ = send_failure(server, source, &failure);
    }
}

pub(crate) fn apply_living_projection(
    player: &Player,
    state: &PlayerLivingAdministrativeState,
) -> bool {
    if !state.effects.is_empty() {
        return false;
    }
    player.living_entity.health.store(state.health);
    player.living_entity.absorption.store(state.absorption);
    player.hunger_manager.set_level(state.food);
    player.hunger_manager.set_saturation(state.saturation);
    player.remove_all_effects();
    player
        .living_entity
        .fall_distance
        .store(state.fall_distance);
    player.get_entity().set_on_fire(state.on_fire);
    if !player
        .cluster_departing
        .load(std::sync::atomic::Ordering::Acquire)
    {
        player.send_health();
    }
    capture_living_projection(player) == *state
}

pub(crate) fn capture_pending(
    server: &Server,
    tick: TickStamp,
    active: &HashMap<GlobalPlayerId, Arc<Player>>,
    departing: &HashSet<GlobalPlayerId>,
    states: &HashMap<GlobalPlayerId, PlayerAdministrativeState>,
    sequences: &mut HashMap<GlobalPlayerId, PlayerSeq>,
    origins: &mut HashMap<ActionKey, ServerId>,
    outstanding_limit: usize,
) {
    while origins.len() < outstanding_limit {
        let Some(routed) = server.cluster_player_admin.intents.pop() else {
            break;
        };
        let intent = routed.intent;
        if !active.contains_key(&intent.target) || departing.contains(&intent.target) {
            publish_no_target_failure(server, routed.source, &intent);
            continue;
        }
        let Some(expected) = states.get(&intent.target).cloned() else {
            publish_no_target_failure(server, routed.source, &intent);
            continue;
        };
        let retained = intent.clone();
        let Some(route) = server.cluster_player_admin.action_route.load_full() else {
            let _ = server
                .cluster_player_admin
                .intents
                .push(RoutedAdministrativeIntent {
                    source: routed.source,
                    intent,
                });
            break;
        };
        let mut forward = expected.clone();
        apply_mutation(&mut forward, &intent.mutation);
        let mut affected = vec![chunk_key(
            expected.world,
            expected.position[0],
            expected.position[2],
        )];
        let destination = chunk_key(forward.world, forward.position[0], forward.position[2]);
        if affected[0] != destination {
            affected.push(destination);
        }
        let next = sequences
            .get(&intent.target)
            .map_or(PlayerSeq(0), |seq| PlayerSeq(seq.0.wrapping_add(1)));
        let action = PlayerAdministrativeAction {
            target: intent.target,
            seq: next,
            tick,
            affected,
            expected: expected.clone(),
            forward,
            undo: expected,
            origin: intent.origin,
        };
        let action_key = ActionKey::player_administrative(&action);
        if route.try_send(action).is_err() {
            let _ = server
                .cluster_player_admin
                .intents
                .push(RoutedAdministrativeIntent {
                    source: routed.source,
                    intent: retained,
                });
            break;
        }
        origins.insert(action_key, routed.source);
        sequences.insert(intent.target, next);
    }
}

fn apply_mutation(state: &mut PlayerAdministrativeState, mutation: &PlayerAdministrativeMutation) {
    match mutation {
        PlayerAdministrativeMutation::Reset {
            health,
            absorption,
            food,
            saturation,
            gamemode,
            clear_effects,
            velocity,
            fall_distance,
            on_fire,
        } => {
            state.health = *health;
            state.absorption = *absorption;
            state.food = *food;
            state.saturation = *saturation;
            state.gamemode = *gamemode;
            if *clear_effects {
                state.effects.clear();
            }
            state.velocity = *velocity;
            state.fall_distance = *fall_distance;
            state.on_fire = *on_fire;
        }
        PlayerAdministrativeMutation::Teleport {
            world,
            position,
            yaw,
            pitch,
        } => {
            state.world = *world;
            state.position = *position;
            state.yaw = *yaw;
            state.pitch = *pitch;
        }
    }
}

fn chunk_key(world: pumpkin_cluster::protocol::WorldId, x: f64, z: f64) -> ChunkKey {
    #[allow(clippy::cast_possible_truncation)]
    ChunkKey::new(
        world,
        (x.floor() as i32).div_euclid(16),
        (z.floor() as i32).div_euclid(16),
    )
}

#[cfg(test)]
mod tests {
    use pumpkin_cluster::identity::{PlayerSlot, ServerId};
    use pumpkin_cluster::player_admin::PlayerAdministrativeMutation;
    use pumpkin_cluster::protocol::WorldId;

    use super::*;

    #[test]
    fn intent_frame_rejects_other_control_protocols() {
        let intent = PlayerAdministrativeIntent {
            target: GlobalPlayerId::new(ServerId(7), PlayerSlot(12)),
            mutation: PlayerAdministrativeMutation::Teleport {
                world: WorldId::new(1, 9),
                position: [24.0, 80.0, -11.0],
                yaw: 90.0,
                pitch: -12.0,
            },
            origin: Some(44),
        };
        let frame = PlayerAdministrativeIntentFrame {
            source: ServerId(3),
            intent,
        };
        let bytes = encode_intent(frame.source, &frame.intent).unwrap();

        assert_eq!(decode_intent(&bytes), Some(frame));
        assert_eq!(decode_intent(&bytes[1..]), None);
        assert_eq!(decode_intent(b"unrelated-control-frame"), None);
    }

    #[test]
    fn intent_frame_requires_current_authenticated_connection() {
        let frame = PlayerAdministrativeIntentFrame {
            source: ServerId(3),
            intent: PlayerAdministrativeIntent {
                target: GlobalPlayerId::new(ServerId(7), PlayerSlot(12)),
                mutation: PlayerAdministrativeMutation::Teleport {
                    world: WorldId::new(1, 9),
                    position: [24.0, 80.0, -11.0],
                    yaw: 90.0,
                    pitch: -12.0,
                },
                origin: Some(44),
            },
        };

        assert!(authorized_intent_frame(
            &frame,
            ServerId(3),
            18,
            Some(18),
            ServerId(7),
            true,
        ));
        assert!(!authorized_intent_frame(
            &frame,
            ServerId(3),
            17,
            Some(18),
            ServerId(7),
            true,
        ));
        assert!(!authorized_intent_frame(
            &frame,
            ServerId(4),
            18,
            Some(18),
            ServerId(7),
            true,
        ));
        assert!(!authorized_intent_frame(
            &frame,
            ServerId(3),
            18,
            Some(18),
            ServerId(7),
            false,
        ));
    }

    #[test]
    fn missing_target_failure_is_bound_to_target_owner_and_requester() {
        let frame = PlayerAdministrativeFailureFrame {
            source: ServerId(7),
            failure: PlayerAdministrativeIntentFailure {
                source: ServerId(3),
                target: GlobalPlayerId::new(ServerId(7), PlayerSlot(12)),
                origin: Some(44),
                reason: PlayerAdministrativeFailureReason::NoTarget,
            },
        };
        assert!(authorized_failure_frame(
            &frame,
            ServerId(7),
            18,
            Some(18),
            ServerId(3),
        ));
        assert!(!authorized_failure_frame(
            &frame,
            ServerId(7),
            17,
            Some(18),
            ServerId(3),
        ));
        assert!(!authorized_failure_frame(
            &frame,
            ServerId(9),
            18,
            Some(18),
            ServerId(3),
        ));
        assert!(!authorized_failure_frame(
            &frame,
            ServerId(7),
            18,
            Some(18),
            ServerId(4),
        ));
    }
}
