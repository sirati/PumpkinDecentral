use std::collections::HashMap;

use pumpkin_cluster::actor::ActorId;
use pumpkin_cluster::controller::{ControllerActorIntent, ControllerActorSource, ControllerFrame};
use pumpkin_cluster::identity::GlobalPlayerId;
use pumpkin_cluster::item_action::{ItemAction, ItemActorEffect, PlayerAvatarState};
use pumpkin_cluster::protocol::{
    BlockPos, ChunkKey, PlayerChunkTransition, PlayerVisualSnapshot, PosUpdate, WorldId,
};
use uuid::Uuid;

pub use pumpkin_cluster::item_action::ActorLivingState;

#[derive(Debug, Clone, Copy, PartialEq, serde::Serialize, serde::Deserialize)]
pub struct ActorSpatialState {
    pub previous_position: Option<[u64; 3]>,
    pub frame: ControllerFrame,
    pub head_yaw: f32,
    pub source: ControllerActorSource,
    pub world: WorldId,
    pub chunk: ChunkKey,
    pub biome_id: Option<u8>,
    pub sneaking: bool,
    pub sprinting: bool,
    pub crawling: bool,
    pub swimming: bool,
    pub gliding: bool,
    pub flying: bool,
    pub fall_distance: f32,
    pub game_mode: u8,
    pub revision: u64,
}

impl ActorSpatialState {
    pub(crate) fn stance(&self) -> pumpkin_cluster::item_action::PlayerStance {
        pumpkin_cluster::item_action::PlayerStance {
            sneaking: self.sneaking,
            sprinting: self.sprinting,
            crawling: self.crawling,
            swimming: self.swimming,
            gliding: self.gliding,
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq)]
pub struct ActorSpatialTruth {
    pub ground: ActorSpatialState,
    pub local: ActorSpatialState,
}

#[derive(Debug, Clone, Copy, PartialEq)]
pub struct ActorSpatialPublication {
    pub truth: ActorSpatialTruth,
    pub local_epoch: pumpkin_cluster::canonical_action::SpatialEpoch,
    pub accepted_epoch: pumpkin_cluster::canonical_action::SpatialEpoch,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum ActorSpatialError {
    MismatchedActor,
    InvalidPosition,
    StaleInput,
    InvalidGameMode,
    MissingInitialState,
}

#[derive(Default)]
pub struct OwnedActors {
    publication_changes: std::collections::BTreeSet<GlobalPlayerId>,
    actor_chunks: HashMap<GlobalPlayerId, Vec<ChunkKey>>,
    chunk_actors: HashMap<ChunkKey, std::collections::BTreeSet<GlobalPlayerId>>,
    initial_fall_distances: HashMap<GlobalPlayerId, f32>,
    actors: HashMap<GlobalPlayerId, ActorSpatialTruth>,
    input_projections: HashMap<GlobalPlayerId, ActorSpatialState>,
    local_epochs: HashMap<GlobalPlayerId, pumpkin_cluster::canonical_action::SpatialEpoch>,
    local_velocity_cuts: HashMap<GlobalPlayerId, ([u64; 3], [i64; 3])>,
    local_keys: HashMap<GlobalPlayerId, Uuid>,
    local_actors_by_key: HashMap<Uuid, GlobalPlayerId>,
    remote_leases: HashMap<GlobalPlayerId, pumpkin_cluster::canonical_action::LivePlayerLease>,
    retired_remote_leases: HashMap<
        GlobalPlayerId,
        (
            pumpkin_cluster::canonical_action::LivePlayerLease,
            pumpkin_cluster::realm_completion::PeerGeneration,
        ),
    >,
    remote_admissions: HashMap<GlobalPlayerId, RemoteAvatarAdmission>,
    prepared_spatial: HashMap<GlobalPlayerId, ActorSpatialState>,
    spatial_visibility: HashMap<GlobalPlayerId, (bool, bool)>,
    local_client_entities: HashMap<GlobalPlayerId, (Uuid, u64, u64, i32)>,
    remote_client_entities:
        HashMap<GlobalPlayerId, super::cluster_replica_binding::ReplicaBindingNotice>,
}

#[derive(Clone, Copy, PartialEq, Eq)]
pub(crate) struct RemoteAvatarAdmission {
    pub lease: pumpkin_cluster::canonical_action::LivePlayerLease,
    pub owner: pumpkin_cluster::realm_completion::PeerGeneration,
    pub reference: pumpkin_cluster::canonical_action::ActionRef,
    pub install_generation: u64,
    pub chunk: ChunkKey,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ActorLivingTruth {
    pub ground: ActorLivingState,
    pub local: ActorLivingState,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ActorLivingImport {
    pub actor: ActorId,
    pub state: ActorLivingState,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) struct ActorAdmissionInstallResult {
    pub lease: Option<pumpkin_cluster::canonical_action::LivePlayerLease>,
    pub actor: GlobalPlayerId,
    pub key: Uuid,
    pub install_generation: u64,
    pub client_entity_id: i32,
    pub accepted: bool,
}

pub(crate) struct ActorAdmissionImport {
    pub completion: tokio::sync::mpsc::OwnedPermit<ActorAdmissionInstallResult>,
    pub actor: GlobalPlayerId,
    pub key: Uuid,
    pub statistics: super::cluster_actor_statistics::ActorStatisticsImport,
    pub experience: pumpkin_cluster::player_experience::PlayerExperienceState,
    pub living: ActorLivingState,
    pub presentation: pumpkin_cluster::item_action::PlayerPresentationState,
    pub fall_distance: f32,
    pub client_entity_id: i32,
    pub install_generation: u64,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ActorEffectError {
    MissingActor,
    StaleFood,
    StaleCombat,
    StaleUse,
    StaleCooldown,
    InvalidActor,
}

#[derive(Default)]
pub struct OwnedLivingActors {
    publication_changes: std::collections::BTreeSet<ActorId>,
    actors: HashMap<ActorId, ActorLivingTruth>,
    visibility: HashMap<ActorId, (bool, bool)>,
    initial_presentations: HashMap<ActorId, pumpkin_cluster::item_action::PlayerPresentationState>,
    initial_fall_distances: HashMap<ActorId, u32>,
    initial_statistics: HashMap<ActorId, std::collections::BTreeMap<(u16, u32), u32>>,
    initial_experience: HashMap<ActorId, pumpkin_cluster::player_experience::PlayerExperienceState>,
    dirty: bool,
}

pub struct PreparedLivingReplacement {
    ground: bool,
    actors: HashMap<ActorId, ActorLivingState>,
}

pub struct PreparedSpatialReplacement {
    ground: bool,
    actors: HashMap<GlobalPlayerId, ActorSpatialState>,
}

impl OwnedActors {
    pub(crate) fn publication_changes(&self) -> &std::collections::BTreeSet<GlobalPlayerId> {
        &self.publication_changes
    }

    pub(crate) fn take_publication_changes(
        &mut self,
    ) -> std::collections::BTreeSet<GlobalPlayerId> {
        std::mem::take(&mut self.publication_changes)
    }

    fn refresh_actor_chunks(&mut self, actor: GlobalPlayerId) {
        self.publication_changes.insert(actor);
        if let Some(previous) = self.actor_chunks.remove(&actor) {
            for chunk in previous {
                if let Some(actors) = self.chunk_actors.get_mut(&chunk) {
                    actors.remove(&actor);
                    if actors.is_empty() {
                        self.chunk_actors.remove(&chunk);
                    }
                }
            }
        }
        let mut chunks = Vec::with_capacity(4);
        if let Some(truth) = self.actors.get(&actor) {
            chunks.extend([truth.ground.chunk, truth.local.chunk]);
        }
        if let Some(projection) = self.input_projections.get(&actor) {
            chunks.push(projection.chunk);
        }
        if let Some(prepared) = self.prepared_spatial.get(&actor) {
            chunks.push(prepared.chunk);
        }
        chunks.sort_unstable();
        chunks.dedup();
        if chunks.is_empty() {
            return;
        }
        for chunk in &chunks {
            self.chunk_actors.entry(*chunk).or_default().insert(actor);
        }
        self.actor_chunks.insert(actor, chunks);
    }

    fn insert_spatial(
        &mut self,
        actor: GlobalPlayerId,
        truth: ActorSpatialTruth,
    ) -> Option<ActorSpatialTruth> {
        let old = self.actors.insert(actor, truth);
        self.refresh_actor_chunks(actor);
        old
    }

    fn insert_projection(
        &mut self,
        actor: GlobalPlayerId,
        spatial: ActorSpatialState,
    ) -> Option<ActorSpatialState> {
        let old = self.input_projections.insert(actor, spatial);
        self.refresh_actor_chunks(actor);
        old
    }

    pub(crate) fn chunks_for_player(&self, player: GlobalPlayerId) -> &[ChunkKey] {
        self.actor_chunks.get(&player).map_or(&[], Vec::as_slice)
    }

    pub(crate) fn lease_for_player(
        &self,
        player: GlobalPlayerId,
    ) -> Option<pumpkin_cluster::canonical_action::LivePlayerLease> {
        if let Some((uuid, incarnation, _, _)) = self.local_client_entities.get(&player) {
            return Some(pumpkin_cluster::canonical_action::LivePlayerLease {
                key: pumpkin_cluster::canonical_action::PlayerKey(*uuid),
                gid: player,
                incarnation: *incarnation,
            });
        }
        self.remote_leases.get(&player).copied().or_else(|| {
            self.remote_admissions
                .get(&player)
                .map(|admission| admission.lease)
        })
    }

    pub(crate) fn players_for_chunk(
        &self,
        chunk: ChunkKey,
    ) -> impl Iterator<Item = GlobalPlayerId> + '_ {
        self.chunk_actors
            .get(&chunk)
            .into_iter()
            .flat_map(|actors| actors.iter().copied())
    }

    pub(crate) fn preflight_players(
        &self,
        leases: &[pumpkin_cluster::canonical_action::LivePlayerLease],
    ) -> Result<(), ActorSpatialError> {
        let mut seen = std::collections::HashSet::with_capacity(leases.len());
        for lease in leases {
            let qualified = if let Some((uuid, incarnation, _, _)) =
                self.local_client_entities.get(&lease.gid)
            {
                *uuid == lease.key.0 && *incarnation == lease.incarnation
            } else if let Some(current) = self.remote_leases.get(&lease.gid) {
                current == lease
            } else if let Some(admission) = self.remote_admissions.get(&lease.gid) {
                admission.lease == *lease
            } else {
                self.local_epochs.get(&lease.gid).is_some_and(|epoch| matches!(epoch,
                pumpkin_cluster::canonical_action::SpatialEpoch::Admission { lease: admitted, .. } if admitted == lease))
            };
            if !qualified || !seen.insert(lease.gid) {
                return Err(ActorSpatialError::MismatchedActor);
            }
        }
        Ok(())
    }

    pub(crate) fn take_players(
        &mut self,
        leases: &[pumpkin_cluster::canonical_action::LivePlayerLease],
    ) -> Result<Self, ActorSpatialError> {
        self.preflight_players(leases)?;
        let mut shard = Self::default();
        for lease in leases {
            if self.publication_changes.remove(&lease.gid) {
                shard.publication_changes.insert(lease.gid);
            }
            if let Some(value) = self.initial_fall_distances.remove(&lease.gid) {
                shard.initial_fall_distances.insert(lease.gid, value);
            }
            if let Some(value) = self.actors.remove(&lease.gid) {
                shard.actors.insert(lease.gid, value);
            }
            if let Some(value) = self.input_projections.remove(&lease.gid) {
                shard.input_projections.insert(lease.gid, value);
            }
            if let Some(value) = self.local_epochs.remove(&lease.gid) {
                shard.local_epochs.insert(lease.gid, value);
            }
            if let Some(value) = self.local_velocity_cuts.remove(&lease.gid) {
                shard.local_velocity_cuts.insert(lease.gid, value);
            }
            if let Some(value) = self.local_keys.remove(&lease.gid) {
                shard.local_keys.insert(lease.gid, value);
            }
            if let Some(value) = self.remote_leases.remove(&lease.gid) {
                shard.remote_leases.insert(lease.gid, value);
            }
            if let Some(value) = self.retired_remote_leases.remove(&lease.gid) {
                shard.retired_remote_leases.insert(lease.gid, value);
            }
            if let Some(value) = self.remote_admissions.remove(&lease.gid) {
                shard.remote_admissions.insert(lease.gid, value);
            }
            if let Some(value) = self.prepared_spatial.remove(&lease.gid) {
                shard.prepared_spatial.insert(lease.gid, value);
            }
            if let Some(value) = self.spatial_visibility.remove(&lease.gid) {
                shard.spatial_visibility.insert(lease.gid, value);
            }
            if let Some(value) = self.local_client_entities.remove(&lease.gid) {
                shard.local_client_entities.insert(lease.gid, value);
            }
            if let Some(value) = self.remote_client_entities.remove(&lease.gid) {
                shard.remote_client_entities.insert(lease.gid, value);
            }
            if let Some(key) = shard.local_keys.get(&lease.gid)
                && let Some(actor) = self.local_actors_by_key.remove(key)
            {
                shard.local_actors_by_key.insert(*key, actor);
            }
            self.refresh_actor_chunks(lease.gid);
            shard.refresh_actor_chunks(lease.gid);
        }
        Ok(shard)
    }

    pub(crate) fn returned_player_ids(&self) -> std::collections::BTreeSet<GlobalPlayerId> {
        self.actors
            .keys()
            .chain(self.input_projections.keys())
            .chain(self.local_epochs.keys())
            .chain(self.local_velocity_cuts.keys())
            .chain(self.local_keys.keys())
            .chain(self.remote_leases.keys())
            .chain(self.retired_remote_leases.keys())
            .chain(self.remote_admissions.keys())
            .chain(self.prepared_spatial.keys())
            .chain(self.spatial_visibility.keys())
            .chain(self.local_client_entities.keys())
            .chain(self.initial_fall_distances.keys())
            .copied()
            .collect()
    }

    pub(crate) fn returned_player_keys(&self) -> impl Iterator<Item = Uuid> + '_ {
        self.local_actors_by_key.keys().copied()
    }

    pub(crate) fn can_return_players(&self, shard: &Self) -> bool {
        !(shard
            .initial_fall_distances
            .keys()
            .any(|actor| self.initial_fall_distances.contains_key(actor))
            || shard
                .actors
                .keys()
                .any(|actor| self.actors.contains_key(actor))
            || shard
                .input_projections
                .keys()
                .any(|actor| self.input_projections.contains_key(actor))
            || shard
                .local_epochs
                .keys()
                .any(|actor| self.local_epochs.contains_key(actor))
            || shard
                .local_velocity_cuts
                .keys()
                .any(|actor| self.local_velocity_cuts.contains_key(actor))
            || shard
                .local_keys
                .keys()
                .any(|actor| self.local_keys.contains_key(actor))
            || shard
                .remote_leases
                .keys()
                .any(|actor| self.remote_leases.contains_key(actor))
            || shard
                .retired_remote_leases
                .keys()
                .any(|actor| self.retired_remote_leases.contains_key(actor))
            || shard
                .remote_admissions
                .keys()
                .any(|actor| self.remote_admissions.contains_key(actor))
            || shard
                .prepared_spatial
                .keys()
                .any(|actor| self.prepared_spatial.contains_key(actor))
            || shard
                .spatial_visibility
                .keys()
                .any(|actor| self.spatial_visibility.contains_key(actor))
            || shard
                .local_client_entities
                .keys()
                .any(|actor| self.local_client_entities.contains_key(actor))
            || shard
                .remote_client_entities
                .keys()
                .any(|actor| self.remote_client_entities.contains_key(actor))
            || shard
                .local_actors_by_key
                .keys()
                .any(|key| self.local_actors_by_key.contains_key(key)))
    }

    pub(crate) fn return_players(&mut self, shard: Self) -> Result<(), Self> {
        if !self.can_return_players(&shard) {
            return Err(shard);
        }
        self.publication_changes.extend(shard.publication_changes);
        let returned = shard.actor_chunks.keys().copied().collect::<Vec<_>>();
        self.initial_fall_distances
            .extend(shard.initial_fall_distances);
        self.actors.extend(shard.actors);
        self.input_projections.extend(shard.input_projections);
        self.local_epochs.extend(shard.local_epochs);
        self.local_velocity_cuts.extend(shard.local_velocity_cuts);
        self.local_keys.extend(shard.local_keys);
        self.remote_leases.extend(shard.remote_leases);
        self.retired_remote_leases
            .extend(shard.retired_remote_leases);
        self.remote_admissions.extend(shard.remote_admissions);
        self.prepared_spatial.extend(shard.prepared_spatial);
        self.spatial_visibility.extend(shard.spatial_visibility);
        self.local_client_entities
            .extend(shard.local_client_entities);
        self.remote_client_entities
            .extend(shard.remote_client_entities);
        self.local_actors_by_key.extend(shard.local_actors_by_key);
        for actor in returned {
            self.refresh_actor_chunks(actor);
        }
        Ok(())
    }

    pub(crate) fn prepared_remote_admission(
        &self,
        gid: GlobalPlayerId,
    ) -> Option<RemoteAvatarAdmission> {
        self.remote_admissions.get(&gid).copied()
    }

    pub(crate) fn prepared_remote_admissions(
        &self,
    ) -> impl Iterator<Item = RemoteAvatarAdmission> + '_ {
        self.remote_admissions.values().copied()
    }

    pub(crate) fn prepare_remote_admission(
        &mut self,
        action: &pumpkin_cluster::canonical_action::Action,
        source: pumpkin_cluster::realm_completion::PeerGeneration,
    ) -> bool {
        use pumpkin_cluster::canonical_action::{ActionRef, ChunkPart, Effect, Target};
        let [
            Effect::PlayerAvatarLifecycle {
                target:
                    Target::Chunk {
                        key: chunk,
                        part: ChunkPart::Player(gid),
                        inventory: None,
                    },
                intent:
                    pumpkin_cluster::avatar_lifecycle::AvatarLifecycleIntent::Admission {
                        lease,
                        install_generation,
                        owner,
                        initial,
                        ..
                    },
            },
        ] = action.effects.as_slice()
        else {
            return false;
        };
        if *owner != source || self.local_keys.contains_key(gid) || action.validate().is_err() {
            return false;
        }
        let declaration = RemoteAvatarAdmission {
            lease: *lease,
            owner: *owner,
            reference: ActionRef {
                tick: action.tick,
                id: action.id,
            },
            install_generation: *install_generation,
            chunk: *chunk,
        };
        if let Some(previous) = self.remote_admissions.get(gid) {
            return *previous == declaration;
        }
        if self.remote_leases.contains_key(gid) || self.actors.contains_key(gid) {
            return false;
        }
        let Some(avatar) = initial.avatar.as_ref() else {
            return false;
        };
        let movement = PosUpdate {
            spatial_epoch: avatar.spatial_epoch,
            gid: *gid,
            incarnation: lease.incarnation,
            seq: avatar.movement_sequence,
            tick: avatar.movement_tick,
            world: avatar.world,
            pos: avatar.position.map(f64::from_bits),
            vel: initial
                .combat
                .velocity_micro
                .map(|value| value as f64 / 1_000_000.0),
            yaw: f32::from_bits(avatar.yaw),
            head_yaw: f32::from_bits(avatar.head_yaw),
            pitch: f32::from_bits(avatar.pitch),
            on_ground: avatar.on_ground,
        };
        let claimed = pumpkin_cluster::controller::ClientClaimedState {
            actor: *gid,
            seq: movement.seq,
            tick: movement.tick,
            position: movement.pos,
            velocity: movement.vel,
            yaw: movement.yaw,
            pitch: movement.pitch,
            on_ground: movement.on_ground,
            input_flags: 0,
        };
        self.prepared_spatial.insert(
            *gid,
            ActorSpatialState {
                previous_position: None,
                frame: ControllerFrame {
                    claimed,
                    effective: claimed.into(),
                    decision: pumpkin_cluster::controller::ControllerDecision::Accepted,
                },
                source: ControllerActorSource::Initial,
                head_yaw: movement.head_yaw,
                world: movement.world,
                chunk: *chunk,
                biome_id: None,
                sneaking: false,
                sprinting: false,
                crawling: false,
                swimming: false,
                gliding: false,
                flying: false,
                fall_distance: f32::from_bits(avatar.fall_distance),
                game_mode: avatar.game_mode,
                revision: 0,
            },
        );
        self.remote_leases.insert(*gid, *lease);
        self.remote_admissions.insert(*gid, declaration);
        self.refresh_actor_chunks(*gid);
        true
    }
    pub(crate) fn update_remote_client_binding(
        &mut self,
        notice: super::cluster_replica_binding::ReplicaBindingNotice,
    ) -> bool {
        if !notice.valid() {
            return false;
        }
        if notice.retired {
            if self
                .remote_client_entities
                .get(&notice.lease.gid)
                .is_some_and(|current| {
                    current.lease == notice.lease
                        && current.source == notice.source
                        && current.client_entity_id == notice.client_entity_id
                })
            {
                self.remote_client_entities.remove(&notice.lease.gid);
                self.publication_changes.insert(notice.lease.gid);
                return true;
            }
            return false;
        }
        if self.remote_leases.get(&notice.lease.gid) != Some(&notice.lease)
            || self
                .remote_admissions
                .get(&notice.lease.gid)
                .is_some_and(|admission| {
                    admission.lease != notice.lease || admission.owner != notice.source
                })
        {
            return false;
        }
        if self.remote_client_entities.iter().any(|(gid, bound)| {
            *gid != notice.lease.gid && bound.client_entity_id == notice.client_entity_id
        }) || self
            .local_client_entities
            .values()
            .any(|binding| binding.3 == notice.client_entity_id)
        {
            return false;
        }
        self.publication_changes.insert(notice.lease.gid);
        self.remote_client_entities.insert(notice.lease.gid, notice);
        true
    }

    pub(crate) fn remote_lease(
        &self,
        gid: GlobalPlayerId,
    ) -> Option<pumpkin_cluster::canonical_action::LivePlayerLease> {
        self.remote_leases.get(&gid).copied()
    }

    pub(crate) fn remote_lease_retired(
        &self,
        lease: pumpkin_cluster::canonical_action::LivePlayerLease,
        source: pumpkin_cluster::realm_completion::PeerGeneration,
    ) -> bool {
        self.retired_remote_leases
            .get(&lease.gid)
            .is_some_and(|(retired, owner)| {
                *owner == source && (retired.incarnation > lease.incarnation || *retired == lease)
            })
    }

    pub(crate) fn update_remote_movement(
        &mut self,
        source: pumpkin_cluster::realm_completion::PeerGeneration,
        lease: pumpkin_cluster::canonical_action::LivePlayerLease,
        epoch: pumpkin_cluster::canonical_action::SpatialEpoch,
        update: PosUpdate,
    ) -> bool {
        if self.remote_leases.get(&update.gid) != Some(&lease)
            || source.server != lease.gid.server
            || source.generation == 0
            || update.gid != lease.gid
            || update.incarnation != lease.incarnation
            || update.spatial_epoch != epoch
            || Self::validate_remote(update, PlayerVisualSnapshot::default()).is_err()
        {
            return false;
        }
        let Some(truth) = self.actors.get(&lease.gid) else {
            return false;
        };
        if truth.local.world != update.world {
            return false;
        }
        let previous = self
            .input_projections
            .get(&lease.gid)
            .copied()
            .unwrap_or(truth.local);
        if self.local_epochs.get(&lease.gid) == Some(&epoch)
            && !update
                .seq
                .is_newer_than(previous.frame.effective.source_seq)
        {
            return false;
        }
        let effective = pumpkin_cluster::controller::EffectiveControllerState {
            actor: lease.gid,
            source_seq: update.seq,
            tick: update.tick,
            position: update.pos,
            velocity: update.vel,
            yaw: update.yaw,
            pitch: update.pitch,
            on_ground: update.on_ground,
            input_flags: previous.frame.effective.input_flags,
        };
        self.insert_projection(
            lease.gid,
            ActorSpatialState {
                previous_position: if previous.frame.effective.tick == update.tick {
                    previous.previous_position
                } else {
                    Some(previous.frame.effective.position.map(f64::to_bits))
                },
                frame: ControllerFrame {
                    effective,
                    ..previous.frame
                },
                head_yaw: update.head_yaw,
                revision: previous.revision.wrapping_add(1),
                ..previous
            },
        );
        self.local_epochs.insert(lease.gid, epoch);
        true
    }

    pub(crate) fn remote_client_entity_id(
        &self,
        lease: pumpkin_cluster::canonical_action::LivePlayerLease,
        source: pumpkin_cluster::realm_completion::PeerGeneration,
    ) -> Option<i32> {
        if self.remote_leases.get(&lease.gid) != Some(&lease) {
            return None;
        }
        self.remote_client_entities
            .get(&lease.gid)
            .and_then(|binding| {
                (binding.lease == lease && binding.source == source && !binding.retired)
                    .then_some(binding.client_entity_id)
            })
    }

    pub(crate) fn player_client_identity(
        &self,
        gid: GlobalPlayerId,
        source: pumpkin_cluster::realm_completion::PeerGeneration,
    ) -> Option<(pumpkin_cluster::canonical_action::LivePlayerLease, i32)> {
        if source.server != gid.server {
            return None;
        }
        if let Some((uuid, incarnation, _, client_id)) = self.local_client_entities.get(&gid) {
            let lease = pumpkin_cluster::canonical_action::LivePlayerLease {
                key: pumpkin_cluster::canonical_action::PlayerKey(*uuid),
                gid,
                incarnation: *incarnation,
            };
            return (self.local_keys.get(&gid) == Some(uuid)).then_some((lease, *client_id));
        }
        let lease = *self.remote_leases.get(&gid)?;
        self.remote_client_entity_id(lease, source)
            .map(|client_id| (lease, client_id))
    }

    pub fn player_observations<'a>(
        &'a self,
        living: &'a OwnedLivingActors,
    ) -> impl Iterator<
        Item = (
            GlobalPlayerId,
            ActorSpatialState,
            ChunkKey,
            pumpkin_cluster::canonical_action::SpatialEpoch,
        ),
    > + 'a {
        self.actors.iter().filter_map(move |(gid, truth)| {
            let avatar = living.get(ActorId::Player(*gid))?.local.avatar.as_ref()?;
            Some((
                *gid,
                self.input_projection(*gid)?,
                truth.local.chunk,
                avatar.spatial_epoch,
            ))
        })
    }

    pub fn synchronize_local_epochs(&mut self, living: &OwnedLivingActors) {
        let mut changed_associations = Vec::new();
        for gid in self.actors.keys() {
            let Some(state) = living.phase_state(ActorId::Player(*gid), false) else {
                continue;
            };
            let Some(epoch) = state.avatar.as_ref().map(|avatar| avatar.spatial_epoch) else {
                continue;
            };
            use pumpkin_cluster::living_property::LivingProperty;
            let versions = [
                LivingProperty::VelocityX,
                LivingProperty::VelocityY,
                LivingProperty::VelocityZ,
            ]
            .map(|property| state.property_versions.get(&property).copied().unwrap_or(0));
            let cut = (versions, state.combat.velocity_micro);
            if self
                .local_velocity_cuts
                .insert(*gid, cut)
                .is_some_and(|before| before != cut)
            {
                let overlay = self
                    .input_projections
                    .entry(*gid)
                    .or_insert(self.actors[gid].local);
                overlay.frame.effective.velocity =
                    cut.1.map(|component| component as f64 / 1_000_000.0);
                overlay.revision = overlay.revision.wrapping_add(1);
            }
            if self
                .local_epochs
                .insert(*gid, epoch)
                .is_some_and(|old| old != epoch)
            {
                self.input_projections.remove(gid);
                changed_associations.push(*gid);
            }
        }
        for actor in changed_associations {
            self.refresh_actor_chunks(actor);
        }
    }

    pub fn publish(&mut self, server: &super::Server, living: &OwnedLivingActors) {
        self.synchronize_local_epochs(living);
        let mut current = self.actors.clone();
        for (actor, overlay) in &self.input_projections {
            if let Some(truth) = current.get_mut(actor) {
                truth.local = *overlay;
            }
        }
        let current: HashMap<_, _> = current
            .into_iter()
            .filter_map(|(actor, truth)| {
                self.phase_spatial(actor, true)?;
                let accepted_epoch = living
                    .phase_state(ActorId::Player(actor), true)?
                    .avatar
                    .as_ref()?
                    .spatial_epoch;
                Some((
                    actor,
                    ActorSpatialPublication {
                        truth,
                        local_epoch: living
                            .phase_state(ActorId::Player(actor), false)?
                            .avatar
                            .as_ref()?
                            .spatial_epoch,
                        accepted_epoch,
                    },
                ))
            })
            .collect();
        let published = server.cluster_actor_spatial.load();
        if **published != current {
            server
                .cluster_actor_spatial
                .store(std::sync::Arc::new(current));
        }
    }

    pub(crate) fn next_fall_distance(&self, intent: &ControllerActorIntent) -> f32 {
        if matches!(intent.source, ControllerActorSource::Initial) {
            return self
                .initial_fall_distances
                .get(&intent.actor)
                .copied()
                .unwrap_or(0.0);
        }
        let Some(previous) = self
            .input_projections
            .get(&intent.actor)
            .copied()
            .or_else(|| self.actors.get(&intent.actor).map(|truth| truth.local))
        else {
            return 0.0;
        };
        if !self.input_epoch_matches(intent) {
            return previous.fall_distance;
        }
        if !matches!(intent.source, ControllerActorSource::Client)
            || intent.frame.effective.on_ground
            || intent.effective_flying
            || intent.swimming.unwrap_or(previous.swimming)
            || intent.gliding.unwrap_or(previous.gliding)
        {
            return 0.0;
        }
        let descent =
            (previous.frame.effective.position[1] - intent.frame.effective.position[1]).max(0.0);
        (f64::from(previous.fall_distance) + descent).min(f64::from(f32::MAX)) as f32
    }

    pub(crate) fn next_environment_fall_distance(
        &self,
        intent: &ControllerActorIntent,
        in_liquid: bool,
        mounted: bool,
    ) -> f32 {
        if (in_liquid || mounted) && self.input_epoch_matches(intent) {
            0.0
        } else {
            self.next_fall_distance(intent)
        }
    }

    pub(crate) fn input_epoch_matches(&self, intent: &ControllerActorIntent) -> bool {
        self.local_epochs.get(&intent.actor).copied() == intent.spatial_epoch
    }

    pub fn capture_controller_input(
        &mut self,
        mut intent: ControllerActorIntent,
        biome_at: impl FnOnce(ChunkKey, BlockPos) -> Option<u8>,
    ) -> Result<ActorSpatialState, ActorSpatialError> {
        if self.remote_leases.contains_key(&intent.actor) {
            return Err(ActorSpatialError::MismatchedActor);
        }
        let current = *self
            .actors
            .get(&intent.actor)
            .ok_or(ActorSpatialError::MissingInitialState)?;
        if self.local_epochs.get(&intent.actor).copied() != intent.spatial_epoch {
            let baseline = current.local.frame.effective;
            intent.frame.effective.position = baseline.position;
            intent.frame.effective.velocity = baseline.velocity;
            intent.frame.effective.yaw = baseline.yaw;
            intent.frame.effective.pitch = baseline.pitch;
            intent.frame.effective.on_ground = baseline.on_ground;
            intent.head_yaw = current.local.head_yaw;
            intent.position_changed = false;
            intent.velocity_changed = false;
            intent.rotation_changed = false;
            intent.ground_changed = false;
            intent.frame.decision = pumpkin_cluster::controller::ControllerDecision::Modified(7);
        }
        let mut shadow = Self::default();
        shadow.actors.insert(
            intent.actor,
            ActorSpatialTruth {
                ground: current.ground,
                local: self
                    .input_projections
                    .get(&intent.actor)
                    .copied()
                    .unwrap_or(current.local),
            },
        );
        let projected = shadow.apply_controller(intent, biome_at)?;
        self.insert_projection(intent.actor, projected);
        Ok(projected)
    }

    pub fn input_projection(&self, actor: GlobalPlayerId) -> Option<ActorSpatialState> {
        self.input_projections
            .get(&actor)
            .copied()
            .or_else(|| self.actors.get(&actor).map(|truth| truth.local))
    }

    pub(crate) fn set_input_fall_distance(
        &mut self,
        actor: GlobalPlayerId,
        distance: f32,
    ) -> Option<ActorSpatialState> {
        if !distance.is_finite() || distance < 0.0 {
            return None;
        }
        let state = self.input_projections.get_mut(&actor)?;
        state.fall_distance = distance;
        Some(*state)
    }

    pub fn validate_accepted_transition(
        update: &PlayerChunkTransition,
    ) -> Result<(), ActorSpatialError> {
        let movement = update.snapshot.movement;
        Self::validate_remote(movement, update.snapshot.visual)?;
        if movement.gid != update.gid
            || movement.incarnation != update.incarnation
            || update.incarnation == 0
            || movement.seq != update.seq
            || movement.tick != update.tick
            || movement.world != update.to.world
            || update.from == update.to
            || ChunkKey::new(
                movement.world,
                (movement.pos[0].floor() as i32).div_euclid(16),
                (movement.pos[2].floor() as i32).div_euclid(16),
            ) != update.to
        {
            return Err(ActorSpatialError::InvalidPosition);
        }
        Ok(())
    }

    pub fn prepare_accepted_transition(
        &self,
        update: &PlayerChunkTransition,
        biome_id: Option<u8>,
    ) -> Result<Option<ActorSpatialState>, ActorSpatialError> {
        let movement = update.snapshot.movement;
        Self::validate_accepted_transition(update)?;
        let Some(truth) = self.actors.get(&update.gid) else {
            return Ok(None);
        };
        if truth.ground.chunk != update.from
            || !movement
                .seq
                .is_newer_than(truth.ground.frame.effective.source_seq)
        {
            return Err(ActorSpatialError::InvalidPosition);
        }
        let mut next = truth.ground;
        next.source = ControllerActorSource::Client;
        next.world = movement.world;
        next.chunk = update.to;
        next.biome_id = biome_id;
        next.frame.effective.position = movement.pos;
        next.frame.effective.velocity = movement.vel;
        next.frame.effective.yaw = movement.yaw;
        next.head_yaw = movement.head_yaw;
        next.frame.effective.pitch = movement.pitch;
        next.frame.effective.on_ground = movement.on_ground;
        next.frame.effective.source_seq = movement.seq;
        next.frame.effective.tick = movement.tick;
        next.revision = next.revision.wrapping_add(1);
        Ok(Some(next))
    }

    pub fn clone_player_subset(
        &self,
        players: &std::collections::BTreeSet<GlobalPlayerId>,
    ) -> Self {
        let actors = players
            .iter()
            .filter_map(|key| self.actors.get(key).copied().map(|state| (*key, state)))
            .collect();
        Self {
            publication_changes: self
                .publication_changes
                .intersection(players)
                .copied()
                .collect(),
            initial_fall_distances: self
                .initial_fall_distances
                .iter()
                .filter(|(gid, _)| players.contains(gid))
                .map(|(gid, distance)| (*gid, *distance))
                .collect(),
            actors,
            input_projections: self
                .input_projections
                .iter()
                .filter(|(gid, _)| players.contains(gid))
                .map(|(gid, state)| (*gid, *state))
                .collect(),
            local_keys: HashMap::new(),
            local_client_entities: HashMap::new(),
            remote_client_entities: HashMap::new(),
            actor_chunks: HashMap::new(),
            chunk_actors: HashMap::new(),
            local_epochs: self
                .local_epochs
                .iter()
                .filter(|(gid, _)| players.contains(gid))
                .map(|(gid, epoch)| (*gid, *epoch))
                .collect(),
            local_velocity_cuts: self
                .local_velocity_cuts
                .iter()
                .filter(|(gid, _)| players.contains(gid))
                .map(|(gid, cut)| (*gid, *cut))
                .collect(),
            local_actors_by_key: HashMap::new(),
            prepared_spatial: self
                .prepared_spatial
                .iter()
                .filter(|(gid, _)| players.contains(gid))
                .map(|(gid, spatial)| (*gid, *spatial))
                .collect(),
            spatial_visibility: self
                .spatial_visibility
                .iter()
                .filter(|(gid, _)| players.contains(gid))
                .map(|(gid, flags)| (*gid, *flags))
                .collect(),
            remote_admissions: self
                .remote_admissions
                .iter()
                .filter(|(gid, _)| players.contains(gid))
                .map(|(gid, declaration)| (*gid, *declaration))
                .collect(),
            remote_leases: self
                .remote_leases
                .iter()
                .filter(|(gid, _)| players.contains(gid))
                .map(|(gid, lease)| (*gid, *lease))
                .collect(),
            retired_remote_leases: self
                .retired_remote_leases
                .iter()
                .filter(|(gid, _)| players.contains(gid))
                .map(|(gid, retired)| (*gid, *retired))
                .collect(),
        }
    }

    pub fn can_import_remote(&self, state: &pumpkin_cluster::xfer::PlayerLivingGround) -> bool {
        !self.actors.contains_key(&state.lease.gid)
            && !self.local_keys.contains_key(&state.lease.gid)
            && !self.remote_leases.contains_key(&state.lease.gid)
    }

    pub fn import_remote(&mut self, state: &pumpkin_cluster::xfer::PlayerLivingGround) -> bool {
        use pumpkin_cluster::controller::{ClientClaimedState, ControllerDecision};
        if !self.can_import_remote(state) {
            return false;
        }
        let movement = state.spatial;
        let claimed = ClientClaimedState {
            actor: movement.gid,
            seq: movement.seq,
            tick: movement.tick,
            position: movement.pos,
            velocity: movement.vel,
            yaw: movement.yaw,
            pitch: movement.pitch,
            on_ground: movement.on_ground,
            input_flags: 0,
        };
        let avatar = match &state.cell.value {
            pumpkin_cluster::canonical_action::Value::Living(living) => living.avatar.as_ref(),
            _ => None,
        };
        let Some(avatar) = avatar else {
            return false;
        };
        let spatial = ActorSpatialState {
            previous_position: None,
            frame: ControllerFrame {
                claimed,
                effective: claimed.into(),
                decision: ControllerDecision::Accepted,
            },
            source: ControllerActorSource::Initial,
            head_yaw: movement.head_yaw,
            world: movement.world,
            chunk: match state.target {
                pumpkin_cluster::canonical_action::Target::Chunk { key, .. } => key,
                _ => return false,
            },
            biome_id: None,
            sneaking: false,
            sprinting: false,
            crawling: false,
            swimming: false,
            gliding: false,
            flying: false,
            fall_distance: f32::from_bits(avatar.fall_distance),
            game_mode: avatar.game_mode,
            revision: state.cell.revision,
        };
        self.insert_spatial(
            movement.gid,
            ActorSpatialTruth {
                ground: spatial,
                local: spatial,
            },
        );
        self.remote_leases.insert(movement.gid, state.lease);
        true
    }

    pub fn prepare_replacement(
        &self,
        ground: bool,
        changes: Vec<(GlobalPlayerId, ActorSpatialState)>,
    ) -> Result<PreparedSpatialReplacement, ActorSpatialError> {
        let mut actors = HashMap::new();
        for (actor, state) in changes {
            if actors.insert(actor, state).is_some() || !self.actors.contains_key(&actor) {
                return Err(ActorSpatialError::MissingInitialState);
            }
        }
        Ok(PreparedSpatialReplacement { ground, actors })
    }

    pub fn install_replacement(&mut self, prepared: PreparedSpatialReplacement) {
        for (actor, state) in prepared.actors {
            let truth = self
                .actors
                .get_mut(&actor)
                .expect("prepared spatial actor retired before commit");
            if prepared.ground {
                truth.ground = state;
                if self
                    .input_projections
                    .get(&actor)
                    .is_some_and(|projection| {
                        let incoming = state.frame.effective;
                        let provisional = projection.frame.effective;
                        !provisional.source_seq.is_newer_than(incoming.source_seq)
                            && (provisional.source_seq != incoming.source_seq
                                || !provisional.tick.is_after(incoming.tick))
                    })
                {
                    self.input_projections.remove(&actor);
                }
            } else {
                truth.local = state;
            }
            self.refresh_actor_chunks(actor);
        }
    }
}

impl OwnedLivingActors {
    pub(crate) fn publication_changes(&self) -> &std::collections::BTreeSet<ActorId> {
        &self.publication_changes
    }

    pub(crate) fn take_publication_changes(&mut self) -> std::collections::BTreeSet<ActorId> {
        std::mem::take(&mut self.publication_changes)
    }

    pub(crate) fn preflight_actors(&self, actors: &[ActorId]) -> Result<(), ActorEffectError> {
        let mut seen = std::collections::HashSet::with_capacity(actors.len());
        if actors.iter().any(|actor| !seen.insert(*actor)) {
            return Err(ActorEffectError::InvalidActor);
        }
        Ok(())
    }

    pub(crate) fn take_actors(&mut self, actors: &[ActorId]) -> Result<Self, ActorEffectError> {
        self.preflight_actors(actors)?;
        let mut shard = Self {
            dirty: self.dirty,
            ..Self::default()
        };
        for actor in actors {
            if self.publication_changes.remove(actor) {
                shard.publication_changes.insert(*actor);
            }
            if let Some(value) = self.actors.remove(actor) {
                shard.actors.insert(*actor, value);
            }
            if let Some(value) = self.visibility.remove(actor) {
                shard.visibility.insert(*actor, value);
            }
            if let Some(value) = self.initial_presentations.remove(actor) {
                shard.initial_presentations.insert(*actor, value);
            }
            if let Some(value) = self.initial_fall_distances.remove(actor) {
                shard.initial_fall_distances.insert(*actor, value);
            }
            if let Some(value) = self.initial_statistics.remove(actor) {
                shard.initial_statistics.insert(*actor, value);
            }
            if let Some(value) = self.initial_experience.remove(actor) {
                shard.initial_experience.insert(*actor, value);
            }
        }
        Ok(shard)
    }

    pub(crate) fn returned_actor_ids(&self) -> std::collections::BTreeSet<ActorId> {
        self.actors
            .keys()
            .chain(self.visibility.keys())
            .chain(self.initial_presentations.keys())
            .chain(self.initial_fall_distances.keys())
            .chain(self.initial_statistics.keys())
            .chain(self.initial_experience.keys())
            .copied()
            .collect()
    }

    pub(crate) fn can_return_actors(&self, shard: &Self) -> bool {
        !(shard
            .actors
            .keys()
            .any(|actor| self.actors.contains_key(actor))
            || shard
                .visibility
                .keys()
                .any(|actor| self.visibility.contains_key(actor))
            || shard
                .initial_presentations
                .keys()
                .any(|actor| self.initial_presentations.contains_key(actor))
            || shard
                .initial_fall_distances
                .keys()
                .any(|actor| self.initial_fall_distances.contains_key(actor))
            || shard
                .initial_statistics
                .keys()
                .any(|actor| self.initial_statistics.contains_key(actor))
            || shard
                .initial_experience
                .keys()
                .any(|actor| self.initial_experience.contains_key(actor)))
    }

    pub(crate) fn return_actors(&mut self, shard: Self) -> Result<(), Self> {
        if !self.can_return_actors(&shard) {
            return Err(shard);
        }
        self.publication_changes.extend(shard.publication_changes);
        self.dirty |= shard.dirty;
        self.actors.extend(shard.actors);
        self.visibility.extend(shard.visibility);
        self.initial_presentations
            .extend(shard.initial_presentations);
        self.initial_fall_distances
            .extend(shard.initial_fall_distances);
        self.initial_statistics.extend(shard.initial_statistics);
        self.initial_experience.extend(shard.initial_experience);
        Ok(())
    }

    pub fn can_import_remote(&self, state: &pumpkin_cluster::xfer::PlayerLivingGround) -> bool {
        !self.actors.contains_key(&ActorId::Player(state.lease.gid))
            && matches!(
                &state.cell.value,
                pumpkin_cluster::canonical_action::Value::Living(living)
                    if living.belongs_to(ActorId::Player(state.lease.gid))
            )
    }

    pub fn import_remote(&mut self, state: &pumpkin_cluster::xfer::PlayerLivingGround) -> bool {
        if !self.can_import_remote(state) {
            return false;
        }
        let pumpkin_cluster::canonical_action::Value::Living(living) = &state.cell.value else {
            return false;
        };
        self.actors.insert(
            ActorId::Player(state.lease.gid),
            ActorLivingTruth {
                ground: living.as_ref().clone(),
                local: living.as_ref().clone(),
            },
        );
        self.publication_changes
            .insert(ActorId::Player(state.lease.gid));
        self.dirty = true;
        true
    }

    pub fn prepare_accepted_transition(
        &self,
        update: &PlayerChunkTransition,
    ) -> Result<Option<ActorLivingState>, ActorEffectError> {
        let actor = ActorId::Player(update.gid);
        let Some(truth) = self.actors.get(&actor) else {
            return Ok(None);
        };
        let mut next = truth.ground.clone();
        let avatar = next.avatar.as_mut().ok_or(ActorEffectError::InvalidActor)?;
        let movement = update.snapshot.movement;
        avatar.world = movement.world;
        avatar.position = movement.pos.map(f64::to_bits);
        avatar.yaw = movement.yaw.to_bits();
        avatar.head_yaw = movement.head_yaw.to_bits();
        avatar.pitch = movement.pitch.to_bits();
        avatar.on_ground = movement.on_ground;
        avatar.movement_tick = movement.tick;
        avatar.movement_sequence = movement.seq;
        next.combat.velocity_micro = movement
            .vel
            .map(|value| (value * 1_000_000.0).round() as i64);
        Ok(Some(next))
    }

    pub fn clone_player_subset(
        &self,
        players: &std::collections::BTreeSet<GlobalPlayerId>,
    ) -> Self {
        Self {
            publication_changes: players
                .iter()
                .map(|gid| ActorId::Player(*gid))
                .filter(|actor| self.publication_changes.contains(actor))
                .collect(),
            actors: players
                .iter()
                .filter_map(|key| {
                    let actor = ActorId::Player(*key);
                    self.actors.get(&actor).cloned().map(|state| (actor, state))
                })
                .collect(),
            dirty: self.dirty,
            visibility: self
                .visibility
                .iter()
                .filter(|(actor, _)| matches!(actor, ActorId::Player(gid) if players.contains(gid)))
                .map(|(actor, value)| (*actor, *value))
                .collect(),
            initial_experience: self
                .initial_experience
                .iter()
                .filter(|(actor, _)| matches!(actor, ActorId::Player(gid) if players.contains(gid)))
                .map(|(actor, value)| (*actor, *value))
                .collect(),
            initial_statistics: self
                .initial_statistics
                .iter()
                .filter(|(actor, _)| matches!(actor, ActorId::Player(gid) if players.contains(gid)))
                .map(|(actor, values)| (*actor, values.clone()))
                .collect(),
            initial_presentations: self
                .initial_presentations
                .iter()
                .filter(|(actor, _)| matches!(actor, ActorId::Player(gid) if players.contains(gid)))
                .map(|(actor, presentation)| (*actor, presentation.clone()))
                .collect(),
            initial_fall_distances: self
                .initial_fall_distances
                .iter()
                .filter(|(actor, _)| matches!(actor, ActorId::Player(gid) if players.contains(gid)))
                .map(|(actor, distance)| (*actor, *distance))
                .collect(),
        }
    }

    pub fn project_player_spatial(
        &mut self,
        actor: GlobalPlayerId,
        spatial: ActorSpatialState,
        epoch: pumpkin_cluster::canonical_action::SpatialEpoch,
    ) -> bool {
        let Some(truth) = self.actors.get_mut(&ActorId::Player(actor)) else {
            return false;
        };
        let presentation = self
            .initial_presentations
            .remove(&ActorId::Player(actor))
            .unwrap_or_default();
        let initial_fall = self.initial_fall_distances.remove(&ActorId::Player(actor));
        let experience = self
            .initial_experience
            .remove(&ActorId::Player(actor))
            .unwrap_or_default();
        let statistics = self
            .initial_statistics
            .remove(&ActorId::Player(actor))
            .unwrap_or_default();
        let effective = spatial.frame.effective;
        let velocity = effective
            .velocity
            .map(|value| (value * 1_000_000.0).round() as i64);
        let states: &mut [&mut ActorLivingState] =
            if matches!(spatial.source, ControllerActorSource::Client) {
                &mut [&mut truth.local]
            } else {
                &mut [&mut truth.local, &mut truth.ground]
            };
        for state in states {
            let base = state.avatar.take().unwrap_or(PlayerAvatarState {
                stance: Default::default(),
                mount: None,
                relation_versions: Default::default(),
                statistics: statistics.clone(),
                experience,
                presentation: presentation.clone(),
                spatial_epoch: epoch,
                world: spatial.world,
                position: effective.position.map(f64::to_bits),
                yaw: effective.yaw.to_bits(),
                head_yaw: spatial.head_yaw.to_bits(),
                pitch: effective.pitch.to_bits(),
                on_ground: effective.on_ground,
                movement_tick: effective.tick,
                movement_sequence: effective.source_seq,
                game_mode: spatial.game_mode,
                fall_distance: initial_fall.unwrap_or(0),
                on_fire: false,
            });
            state.avatar = Some(PlayerAvatarState {
                stance: spatial.stance(),
                spatial_epoch: epoch,
                world: spatial.world,
                position: effective.position.map(f64::to_bits),
                yaw: effective.yaw.to_bits(),
                head_yaw: spatial.head_yaw.to_bits(),
                pitch: effective.pitch.to_bits(),
                on_ground: effective.on_ground,
                movement_tick: effective.tick,
                movement_sequence: effective.source_seq,
                game_mode: spatial.game_mode,
                ..base
            });
            state.combat.velocity_micro = velocity;
        }
        self.publication_changes.insert(ActorId::Player(actor));
        self.dirty = true;
        true
    }

    pub fn prepare_replacement(
        &self,
        ground: bool,
        changes: Vec<(ActorId, ActorLivingState)>,
    ) -> Result<PreparedLivingReplacement, ActorEffectError> {
        let mut actors = HashMap::new();
        for (actor, state) in changes {
            if actors.contains_key(&actor) || !state.belongs_to(actor) {
                return Err(ActorEffectError::InvalidActor);
            }
            if !self.actors.contains_key(&actor) {
                return Err(ActorEffectError::MissingActor);
            }
            actors.insert(actor, state);
        }
        Ok(PreparedLivingReplacement { ground, actors })
    }

    pub fn install_replacement(&mut self, prepared: PreparedLivingReplacement) {
        self.dirty |= !prepared.actors.is_empty();
        for (actor, state) in prepared.actors {
            self.publication_changes.insert(actor);
            let truth = self
                .actors
                .get_mut(&actor)
                .expect("prepared living actor retired before commit");
            if prepared.ground {
                truth.ground = state;
            } else {
                truth.local = state;
            }
        }
    }

    pub fn install(
        &mut self,
        actor: ActorId,
        state: ActorLivingState,
    ) -> Result<(), ActorEffectError> {
        if !state.belongs_to(actor) {
            return Err(ActorEffectError::InvalidActor);
        }
        self.actors.insert(
            actor,
            ActorLivingTruth {
                ground: state.clone(),
                local: state,
            },
        );
        self.publication_changes.insert(actor);
        self.dirty = true;
        Ok(())
    }

    pub fn get(&self, actor: ActorId) -> Option<&ActorLivingTruth> {
        self.visibility
            .get(&actor)
            .is_none_or(|(_, local)| *local)
            .then(|| self.actors.get(&actor))
            .flatten()
    }

    pub fn mark_prepared(&mut self, actor: ActorId) {
        self.visibility.insert(actor, (false, false));
        self.publication_changes.insert(actor);
        self.dirty = true;
    }

    pub fn prepared(&self, actor: ActorId) -> Option<&ActorLivingTruth> {
        self.actors.get(&actor)
    }

    pub fn phase_state(&self, actor: ActorId, ground: bool) -> Option<&ActorLivingState> {
        let visible = self
            .visibility
            .get(&actor)
            .is_none_or(|(ground_visible, local_visible)| {
                if ground {
                    *ground_visible
                } else {
                    *local_visible
                }
            });
        if !visible {
            return None;
        }
        self.actors
            .get(&actor)
            .map(|truth| if ground { &truth.ground } else { &truth.local })
    }

    pub fn install_lifecycle(
        &mut self,
        actor: ActorId,
        ground: bool,
        state: Option<ActorLivingState>,
    ) {
        let visible = self.visibility.entry(actor).or_insert((true, true));
        if ground {
            visible.0 = state.is_some();
        } else {
            visible.1 = state.is_some();
        }
        if let Some(state) = state {
            if let Some(truth) = self.actors.get_mut(&actor) {
                if ground {
                    truth.ground = state;
                } else {
                    truth.local = state;
                }
            } else {
                self.actors.insert(
                    actor,
                    ActorLivingTruth {
                        ground: state.clone(),
                        local: state,
                    },
                );
            }
        }
        self.publication_changes.insert(actor);
        self.dirty = true;
    }

    pub fn contains(&self, actor: ActorId) -> bool {
        self.actors.contains_key(&actor)
    }

    pub fn prepare_local(
        &self,
        action: &ItemAction,
    ) -> Result<Vec<(ActorId, ActorLivingState)>, ActorEffectError> {
        self.prepare(action, false)
    }

    pub fn prepare_ground(
        &self,
        action: &ItemAction,
    ) -> Result<Vec<(ActorId, ActorLivingState)>, ActorEffectError> {
        self.prepare(action, true)
    }

    fn prepare(
        &self,
        action: &ItemAction,
        ground: bool,
    ) -> Result<Vec<(ActorId, ActorLivingState)>, ActorEffectError> {
        let mut changed = HashMap::<ActorId, ActorLivingState>::new();
        for effect in &action.actor_effects {
            let ItemActorEffect::Food(effect) = effect;
            let state = self.state_for(effect.before.actor, ground, &mut changed)?;
            if state.food != effect.before {
                return Err(ActorEffectError::StaleFood);
            }
            state.food = effect.after;
        }
        if let Some(effect) = &action.combat {
            let state = self.state_for(effect.before.actor, ground, &mut changed)?;
            if state.combat != effect.before {
                return Err(ActorEffectError::StaleCombat);
            }
            state.combat = effect.after.clone();
        }
        if let Some(transition) = &action.use_transition {
            let state = self.state_for(action.actor, ground, &mut changed)?;
            if state.use_state != transition.before {
                return Err(ActorEffectError::StaleUse);
            }
            state.use_state = transition.after.clone();
        }
        if let Some(transition) = &action.cooldown_transition {
            let state = self.state_for(action.actor, ground, &mut changed)?;
            if state.cooldowns.get(&transition.after.group) != transition.before.as_ref() {
                return Err(ActorEffectError::StaleCooldown);
            }
            state
                .cooldowns
                .insert(transition.after.group.clone(), transition.after.clone());
        }
        Ok(changed.into_iter().collect())
    }

    fn state_for<'a>(
        &'a self,
        actor: ActorId,
        ground: bool,
        changed: &'a mut HashMap<ActorId, ActorLivingState>,
    ) -> Result<&'a mut ActorLivingState, ActorEffectError> {
        if !changed.contains_key(&actor) {
            let truth = self
                .actors
                .get(&actor)
                .ok_or(ActorEffectError::MissingActor)?;
            changed.insert(
                actor,
                if ground {
                    truth.ground.clone()
                } else {
                    truth.local.clone()
                },
            );
        }
        changed
            .get_mut(&actor)
            .ok_or(ActorEffectError::MissingActor)
    }

    pub fn commit_local(
        &mut self,
        prepared: Vec<(ActorId, ActorLivingState)>,
    ) -> Result<(), ActorEffectError> {
        for (actor, _) in &prepared {
            if !self.actors.contains_key(actor) {
                return Err(ActorEffectError::MissingActor);
            }
        }
        for (actor, state) in prepared {
            self.publication_changes.insert(actor);
            self.actors
                .get_mut(&actor)
                .ok_or(ActorEffectError::MissingActor)?
                .local = state;
        }
        self.dirty = true;
        Ok(())
    }

    pub fn promote_ground(
        &mut self,
        prepared: Vec<(ActorId, ActorLivingState)>,
    ) -> Result<(), ActorEffectError> {
        for (actor, _) in &prepared {
            if !self.actors.contains_key(actor) {
                return Err(ActorEffectError::MissingActor);
            }
        }
        for (actor, state) in prepared {
            self.publication_changes.insert(actor);
            self.actors
                .get_mut(&actor)
                .ok_or(ActorEffectError::MissingActor)?
                .ground = state;
        }
        self.dirty = true;
        Ok(())
    }

    pub fn remove(&mut self, actor: ActorId) {
        self.publication_changes.insert(actor);
        self.visibility.remove(&actor);
        self.initial_statistics.remove(&actor);
        self.initial_experience.remove(&actor);
        self.initial_presentations.remove(&actor);
        self.initial_fall_distances.remove(&actor);
        self.dirty |= self.actors.remove(&actor).is_some();
    }

    pub fn stage_initial_presentation(
        &mut self,
        actor: ActorId,
        presentation: pumpkin_cluster::item_action::PlayerPresentationState,
    ) {
        self.initial_presentations.insert(actor, presentation);
    }

    pub fn stage_initial_fall_distance(&mut self, actor: ActorId, distance: f32) -> bool {
        if !distance.is_finite() || distance < 0.0 {
            return false;
        }
        self.initial_fall_distances
            .insert(actor, distance.to_bits());
        true
    }

    pub fn stage_initial_experience(
        &mut self,
        actor: ActorId,
        value: pumpkin_cluster::player_experience::PlayerExperienceState,
    ) {
        self.initial_experience.insert(actor, value);
    }

    pub fn stage_initial_statistics(
        &mut self,
        actor: ActorId,
        values: std::collections::BTreeMap<(u16, u32), u32>,
    ) {
        self.initial_statistics.insert(actor, values);
    }

    pub fn publish(&mut self, server: &super::Server) {
        if !self.dirty {
            return;
        }
        server.cluster_ground_sleep.store(std::sync::Arc::new(
            self.actors
                .iter()
                .filter(|(actor, _)| self.visibility.get(actor).is_none_or(|(ground, _)| *ground))
                .filter_map(|(actor, truth)| {
                    if !matches!(actor, ActorId::Player(_)) {
                        return None;
                    }
                    let avatar = truth.ground.avatar.as_ref()?;
                    Some((
                        *actor,
                        super::cluster_environment::GroundSleepView {
                            world: avatar.world,
                            spectator: avatar.game_mode == 3,
                            health: truth.ground.combat.health_milli as f32 / 1000.0,
                            sleeping_since: avatar
                                .presentation
                                .sleeping
                                .map(|sleeping| sleeping.since),
                        },
                    ))
                })
                .collect(),
        ));
        server.cluster_actor_living.store(std::sync::Arc::new(
            self.actors
                .iter()
                .filter(|(actor, _)| self.visibility.get(actor).is_none_or(|(_, local)| *local))
                .map(|(actor, truth)| (*actor, truth.local.clone()))
                .collect(),
        ));
        self.dirty = false;
    }
}

impl OwnedActors {
    pub fn validate_remote(
        movement: PosUpdate,
        visual: PlayerVisualSnapshot,
    ) -> Result<(), ActorSpatialError> {
        if !movement
            .pos
            .iter()
            .all(|value| value.is_finite() && value.abs() <= 30_000_000.0)
            || !movement.vel.iter().all(|value| value.is_finite())
            || !movement.yaw.is_finite()
            || !movement.head_yaw.is_finite()
            || !movement.pitch.is_finite()
        {
            return Err(ActorSpatialError::InvalidPosition);
        }
        if visual.game_mode > 3 {
            return Err(ActorSpatialError::InvalidGameMode);
        }
        Ok(())
    }

    pub fn can_admit_local(&self, actor: GlobalPlayerId) -> bool {
        !self.local_keys.contains_key(&actor) && !self.remote_leases.contains_key(&actor)
    }

    pub(crate) fn stage_initial_fall_distance(&mut self, actor: GlobalPlayerId, distance: f32) {
        assert!(distance.is_finite() && distance >= 0.0);
        self.initial_fall_distances.insert(actor, distance);
    }

    pub(crate) fn can_admit_local_key(&self, actor: GlobalPlayerId, key: Uuid) -> bool {
        !key.is_nil() && self.can_admit_local(actor) && !self.local_actors_by_key.contains_key(&key)
    }

    pub(crate) fn can_admit_local_candidate(
        &self,
        actor: GlobalPlayerId,
        key: Uuid,
        install_generation: u64,
        entity_id: i32,
    ) -> bool {
        self.can_admit_local_key(actor, key)
            && install_generation != 0
            && entity_id > 0
            && !self
                .local_client_entities
                .values()
                .any(|binding| binding.3 == entity_id)
    }

    pub fn admit_local(&mut self, actor: GlobalPlayerId, key: Uuid) -> bool {
        if !self.can_admit_local_key(actor, key) {
            return false;
        }
        self.local_keys.insert(actor, key);
        self.local_actors_by_key.insert(key, actor);
        self.publication_changes.insert(actor);
        true
    }

    pub fn local_key(&self, actor: GlobalPlayerId) -> Option<Uuid> {
        self.local_keys.get(&actor).copied()
    }

    pub fn bind_local_client_entity(
        &mut self,
        lease: pumpkin_cluster::canonical_action::LivePlayerLease,
        install_generation: u64,
        entity_id: i32,
    ) -> bool {
        if self.local_key(lease.gid) != Some(lease.key.0)
            || lease.incarnation == 0
            || install_generation == 0
            || entity_id <= 0
            || self
                .local_client_entities
                .iter()
                .any(|(gid, binding)| *gid != lease.gid && binding.3 == entity_id)
        {
            return false;
        }
        let binding = (
            lease.key.0,
            lease.incarnation,
            install_generation,
            entity_id,
        );
        match self.local_client_entities.entry(lease.gid) {
            std::collections::hash_map::Entry::Vacant(entry) => {
                entry.insert(binding);
                self.publication_changes.insert(lease.gid);
                true
            }
            std::collections::hash_map::Entry::Occupied(entry) => *entry.get() == binding,
        }
    }

    pub fn local_client_entity_id(
        &self,
        lease: pumpkin_cluster::canonical_action::LivePlayerLease,
        install_generation: u64,
    ) -> Option<i32> {
        self.local_client_entities
            .get(&lease.gid)
            .and_then(|binding| {
                (binding.0 == lease.key.0
                    && binding.1 == lease.incarnation
                    && binding.2 == install_generation)
                    .then_some(binding.3)
            })
    }

    pub fn local_contact_client_id(
        &self,
        contact: &pumpkin_cluster::item_contact::ContactObservation,
    ) -> Option<i32> {
        let pumpkin_cluster::item_contact::ContactIdentity::Player(lease) = &contact.identity
        else {
            return None;
        };
        self.local_client_entities
            .get(&lease.gid)
            .and_then(|(key, incarnation, _, id)| {
                (*key == lease.key.0 && *incarnation == lease.incarnation).then_some(*id)
            })
    }

    pub fn local_actor_for_key(&self, key: Uuid) -> Option<GlobalPlayerId> {
        self.local_actors_by_key.get(&key).copied()
    }

    pub fn apply_controller(
        &mut self,
        intent: ControllerActorIntent,
        biome_at: impl FnOnce(ChunkKey, BlockPos) -> Option<u8>,
    ) -> Result<ActorSpatialState, ActorSpatialError> {
        if self.remote_leases.contains_key(&intent.actor)
            || intent.actor != intent.frame.claimed.actor
            || intent.actor != intent.frame.effective.actor
        {
            return Err(ActorSpatialError::MismatchedActor);
        }
        let position = intent.frame.effective.position;
        if !position
            .iter()
            .all(|coordinate| coordinate.is_finite() && coordinate.abs() <= 30_000_000.0)
            || !intent
                .frame
                .effective
                .velocity
                .iter()
                .all(|value| value.is_finite())
            || !intent.frame.effective.yaw.is_finite()
            || !intent.head_yaw.is_finite()
            || !intent.frame.effective.pitch.is_finite()
        {
            return Err(ActorSpatialError::InvalidPosition);
        }
        let previous = self.actors.get(&intent.actor).map(|state| state.local);
        let game_mode = match (intent.source, intent.gamemode, previous) {
            (ControllerActorSource::Initial, Some(mode), _)
            | (ControllerActorSource::AcceptedAdministrative(_, _), Some(mode), _)
                if mode <= 3 =>
            {
                mode
            }
            (ControllerActorSource::Client, None, Some(previous)) => previous.game_mode,
            (ControllerActorSource::Client, None, None) => {
                return Err(ActorSpatialError::MissingInitialState);
            }
            _ => return Err(ActorSpatialError::InvalidGameMode),
        };
        if let Some(previous) = previous {
            let prior_seq = previous.frame.effective.source_seq;
            let incoming_seq = intent.frame.effective.source_seq;
            let accepted_administrative = matches!(intent.source, ControllerActorSource::AcceptedAdministrative(action, tick)
                if previous.source != ControllerActorSource::AcceptedAdministrative(action, tick));
            if !accepted_administrative
                && !incoming_seq.is_newer_than(prior_seq)
                && !(incoming_seq == prior_seq
                    && intent.tick.is_after(previous.frame.effective.tick))
            {
                return Err(ActorSpatialError::StaleInput);
            }
        }
        let block = BlockPos {
            x: position[0].floor() as i32,
            y: position[1].floor() as i32,
            z: position[2].floor() as i32,
        };
        let chunk = ChunkKey::new(intent.world, block.x.div_euclid(16), block.z.div_euclid(16));
        let state = ActorSpatialState {
            previous_position: if matches!(intent.source, ControllerActorSource::Client)
                && !matches!(
                    intent.frame.decision,
                    pumpkin_cluster::controller::ControllerDecision::Modified(7)
                ) {
                previous.and_then(|state| {
                    if state.frame.effective.tick == intent.frame.effective.tick {
                        state.previous_position
                    } else {
                        Some(state.frame.effective.position.map(f64::to_bits))
                    }
                })
            } else {
                None
            },
            frame: intent.frame,
            head_yaw: intent.head_yaw,
            source: intent.source,
            world: intent.world,
            chunk,
            biome_id: biome_at(chunk, block),
            sneaking: intent
                .sneaking
                .unwrap_or_else(|| previous.is_some_and(|state| state.sneaking)),
            sprinting: intent
                .sprinting
                .unwrap_or_else(|| previous.is_some_and(|state| state.sprinting)),
            crawling: intent
                .crawling
                .unwrap_or_else(|| previous.is_some_and(|state| state.crawling)),
            flying: intent.effective_flying,
            fall_distance: self.next_fall_distance(&intent),
            swimming: intent
                .swimming
                .unwrap_or_else(|| previous.is_some_and(|state| state.swimming)),
            gliding: intent
                .gliding
                .unwrap_or_else(|| previous.is_some_and(|state| state.gliding)),
            game_mode,
            revision: previous.map_or(0, |state| state.revision.wrapping_add(1)),
        };
        let ground = if matches!(intent.source, ControllerActorSource::Client) {
            self.actors
                .get(&intent.actor)
                .ok_or(ActorSpatialError::MissingInitialState)?
                .ground
        } else {
            state
        };
        self.insert_spatial(
            intent.actor,
            ActorSpatialTruth {
                ground,
                local: state,
            },
        );
        if !matches!(intent.source, ControllerActorSource::Client) {
            self.input_projections.remove(&intent.actor);
        }
        Ok(state)
    }

    pub fn get(&self, actor: GlobalPlayerId) -> Option<&ActorSpatialTruth> {
        self.spatial_visibility
            .get(&actor)
            .is_none_or(|(_, local)| *local)
            .then(|| self.actors.get(&actor))
            .flatten()
    }

    pub fn phase_spatial(&self, actor: GlobalPlayerId, ground: bool) -> Option<ActorSpatialState> {
        if !self
            .spatial_visibility
            .get(&actor)
            .is_none_or(|(ground_visible, local_visible)| {
                if ground {
                    *ground_visible
                } else {
                    *local_visible
                }
            })
        {
            return None;
        }
        self.actors
            .get(&actor)
            .map(|truth| if ground { truth.ground } else { truth.local })
    }

    pub fn stage_initial_spatial(&mut self, actor: GlobalPlayerId) {
        if let Some(truth) = self.actors.remove(&actor) {
            self.prepared_spatial.insert(actor, truth.local);
        }
        self.input_projections.remove(&actor);
        self.local_epochs.remove(&actor);
        self.local_velocity_cuts.remove(&actor);
        self.refresh_actor_chunks(actor);
    }

    pub fn prepared_spatial(&self, actor: GlobalPlayerId) -> Option<ActorSpatialState> {
        self.prepared_spatial.get(&actor).copied()
    }

    pub fn install_spatial_lifecycle(
        &mut self,
        actor: GlobalPlayerId,
        ground: bool,
        spatial: Option<ActorSpatialState>,
    ) {
        let visible = self
            .spatial_visibility
            .entry(actor)
            .or_insert((false, false));
        if ground {
            visible.0 = spatial.is_some();
        } else {
            visible.1 = spatial.is_some();
        }
        if let Some(spatial) = spatial {
            if let Some(truth) = self.actors.get_mut(&actor) {
                if ground {
                    truth.ground = spatial;
                } else {
                    truth.local = spatial;
                }
            } else {
                self.insert_spatial(
                    actor,
                    ActorSpatialTruth {
                        ground: spatial,
                        local: spatial,
                    },
                );
            }
        }
        if ground && spatial.is_none() {
            if let Some(admission) = self.remote_admissions.get(&actor) {
                self.retired_remote_leases
                    .insert(actor, (admission.lease, admission.owner));
            } else if let Some(binding) = self.remote_client_entities.get(&actor) {
                self.retired_remote_leases
                    .insert(actor, (binding.lease, binding.source));
            }
            self.remote_admissions.remove(&actor);
        }
        self.refresh_actor_chunks(actor);
    }

    pub fn remove(&mut self, actor: GlobalPlayerId) {
        self.prepared_spatial.remove(&actor);
        self.spatial_visibility.remove(&actor);
        self.remote_admissions.remove(&actor);
        self.local_client_entities.remove(&actor);
        self.remote_client_entities.remove(&actor);
        self.actors.remove(&actor);
        self.input_projections.remove(&actor);
        self.local_epochs.remove(&actor);
        self.local_velocity_cuts.remove(&actor);
        self.remote_leases.remove(&actor);
        self.initial_fall_distances.remove(&actor);
        if let Some(key) = self.local_keys.remove(&actor) {
            self.local_actors_by_key.remove(&key);
        }
        self.refresh_actor_chunks(actor);
    }
}

#[cfg(test)]
mod tests {
    use pumpkin_cluster::canonical_action::{
        Cell, ChunkPart, LivePlayerLease, PlayerKey, Target, Value,
    };
    use pumpkin_cluster::controller::{
        ClientClaimedState, ControllerActorIntent, ControllerActorSource, ControllerDecision,
        ControllerFrame, ControllerMovementStatistic, EffectiveControllerState,
    };
    use pumpkin_cluster::identity::{GlobalPlayerId, PlayerSeq, PlayerSlot, ServerId};
    use pumpkin_cluster::inventory::{
        INV_MAIN, InvLoc, InventoryCell, InventorySlotRef, InventoryTarget,
    };
    use pumpkin_cluster::item_action::{
        ClaimedHit, EffectiveItemPose, ItemActionClientClaim, ItemActionIntent, ItemActionKind,
        ItemActionPlan, ItemCombatEffect,
    };
    use pumpkin_cluster::protocol::{
        BlockPos, ChunkKey, PlayerChunkTransition, PlayerReplicaSnapshot, PlayerVisualSnapshot,
        PosUpdate, WorldId,
    };
    use pumpkin_cluster::time::TickStamp;

    use super::{ActorSpatialError, OwnedActors, OwnedLivingActors};

    #[test]
    fn actor_publication_changes_survive_owned_work_without_consuming_render_dirty_state() {
        let mut living = OwnedLivingActors::default();
        let first = GlobalPlayerId::new(ServerId(1), PlayerSlot(1));
        let second = GlobalPlayerId::new(ServerId(1), PlayerSlot(2));
        for gid in [first, second] {
            let initial = super::super::cluster_actor_import::from_player_nbt(gid, None)
                .unwrap()
                .state;
            living.install(gid.into(), initial).unwrap();
        }
        living.take_publication_changes();
        living.dirty = false;
        let actor = pumpkin_cluster::actor::ActorId::Player(first);
        let mut shard = living.take_actors(&[actor]).unwrap();
        let mut changed = shard.get(actor).unwrap().local.clone();
        changed.combat.health_milli = 7000;
        let replacement = shard
            .prepare_replacement(false, vec![(actor, changed)])
            .unwrap();
        shard.install_replacement(replacement);
        assert!(living.return_actors(shard).is_ok());
        let contact_page = living.publication_changes().clone();
        let renderer_page = living.publication_changes().clone();
        assert_eq!(contact_page, std::collections::BTreeSet::from([actor]));
        assert_eq!(renderer_page, contact_page);
        assert!(living.dirty);
        assert_eq!(living.take_publication_changes(), contact_page);
        assert!(living.publication_changes().is_empty());
        assert!(living.dirty);
        living.remove(second.into());
        assert_eq!(
            living.publication_changes(),
            &std::collections::BTreeSet::from([second.into()])
        );
        assert!(living.get(second.into()).is_none());
    }

    #[test]
    fn owned_actor_shard_take_return_preserves_qualified_state_and_chunk_index() {
        let mut actors = OwnedActors::default();
        let input = intent(1, 1, 2.0, 2.0);
        let gid = input.actor;
        actors.apply_controller(input, |_, _| None).unwrap();
        let lease = LivePlayerLease {
            key: PlayerKey(uuid::Uuid::from_u128(501)),
            gid,
            incarnation: 3,
        };
        actors
            .local_client_entities
            .insert(gid, (lease.key.0, lease.incarnation, 9, 42));
        actors.local_keys.insert(gid, lease.key.0);
        actors.local_actors_by_key.insert(lease.key.0, gid);
        actors.initial_fall_distances.insert(gid, 4.5);
        let before = *actors.get(gid).unwrap();
        let chunk = before.local.chunk;
        assert_eq!(
            actors.players_for_chunk(chunk).collect::<Vec<_>>(),
            vec![gid]
        );
        let stale = LivePlayerLease {
            incarnation: 2,
            ..lease
        };
        assert!(actors.take_players(&[stale]).is_err());
        assert_eq!(actors.get(gid), Some(&before));
        let shard = actors.take_players(&[lease]).unwrap();
        assert!(actors.get(gid).is_none());
        assert_eq!(actors.players_for_chunk(chunk).count(), 0);
        assert_eq!(shard.get(gid), Some(&before));
        assert_eq!(shard.initial_fall_distances[&gid], 4.5);
        assert_eq!(shard.local_actors_by_key[&lease.key.0], gid);
        assert!(actors.return_players(shard).is_ok());
        assert_eq!(actors.get(gid), Some(&before));
        assert_eq!(
            actors.players_for_chunk(chunk).collect::<Vec<_>>(),
            vec![gid]
        );
        let mut living = OwnedLivingActors::default();
        let initial = super::super::cluster_actor_import::from_player_nbt(gid, None)
            .unwrap()
            .state;
        living.install(gid.into(), initial).unwrap();
        let before = living.get(gid.into()).unwrap().clone();
        let mut shard = living.take_actors(&[gid.into()]).unwrap();
        assert!(living.get(gid.into()).is_none());
        shard
            .actors
            .get_mut(&gid.into())
            .unwrap()
            .local
            .combat
            .health_milli = 7000;
        assert!(living.return_actors(shard).is_ok());
        assert_eq!(living.get(gid.into()).unwrap().ground, before.ground);
        assert_eq!(
            living.get(gid.into()).unwrap().local.combat.health_milli,
            7000
        );
    }

    #[test]
    fn remote_binding_rejects_old_install_and_exactly_retires_current_session() {
        use super::super::cluster_replica_binding::ReplicaBindingNotice;
        use pumpkin_cluster::realm_completion::PeerGeneration;
        let gid = GlobalPlayerId::new(ServerId(2), PlayerSlot(7));
        let lease = LivePlayerLease {
            key: PlayerKey(uuid::Uuid::from_u128(80)),
            gid,
            incarnation: 4,
        };
        let source = PeerGeneration {
            server: gid.server,
            generation: 12,
        };
        let notice = ReplicaBindingNotice {
            lease,
            source,
            client_entity_id: 34,
            retired: false,
        };
        let mut actors = OwnedActors::default();
        assert!(!actors.update_remote_client_binding(notice));
        actors.remote_leases.insert(gid, lease);
        assert!(actors.update_remote_client_binding(notice));
        let newer = LivePlayerLease {
            incarnation: 5,
            ..lease
        };
        actors.remote_leases.insert(gid, newer);
        let current = ReplicaBindingNotice {
            lease: newer,
            client_entity_id: 35,
            ..notice
        };
        assert!(actors.update_remote_client_binding(current));
        actors.remote_admissions.insert(
            gid,
            super::RemoteAvatarAdmission {
                lease: newer,
                owner: source,
                reference: pumpkin_cluster::canonical_action::ActionRef {
                    tick: TickStamp(30),
                    id: pumpkin_cluster::canonical_action::ActionId {
                        actor: pumpkin_cluster::actor::ActorId::Player(gid),
                        incarnation: newer.incarnation,
                        sequence: 1,
                        family: 1,
                    },
                },
                install_generation: 1,
                chunk: ChunkKey::new(WorldId::new(0, 0), 0, 0),
            },
        );
        assert!(!actors.update_remote_client_binding(ReplicaBindingNotice {
            source: PeerGeneration {
                generation: 13,
                ..source
            },
            ..current
        }));
        assert!(!actors.update_remote_client_binding(notice));
        assert!(!actors.update_remote_client_binding(ReplicaBindingNotice {
            retired: true,
            ..notice
        }));
        assert!(!actors.update_remote_client_binding(ReplicaBindingNotice {
            client_entity_id: 34,
            retired: true,
            ..current
        }));
        assert!(!actors.update_remote_client_binding(ReplicaBindingNotice {
            source: PeerGeneration {
                generation: 11,
                ..source
            },
            retired: true,
            ..current
        }));
        assert_eq!(actors.remote_client_entity_id(newer, source), Some(35));
        assert!(actors.update_remote_client_binding(ReplicaBindingNotice {
            retired: true,
            ..current
        }));
        assert_eq!(actors.remote_client_entity_id(newer, source), None);
    }

    #[test]
    fn client_entity_binding_requires_exact_actor_session() {
        let gid = GlobalPlayerId::new(ServerId(1), PlayerSlot(4));
        let lease = LivePlayerLease {
            key: PlayerKey(uuid::Uuid::from_u128(44)),
            gid,
            incarnation: 7,
        };
        let mut actors = OwnedActors::default();
        assert!(!actors.bind_local_client_entity(lease, 20, 31));
        assert!(actors.admit_local(gid, lease.key.0));
        assert!(actors.bind_local_client_entity(lease, 20, 31));
        assert_eq!(actors.local_client_entity_id(lease, 20), Some(31));
        assert_eq!(actors.local_client_entity_id(lease, 21), None);
        let newer = LivePlayerLease {
            incarnation: 8,
            ..lease
        };
        assert_eq!(actors.local_client_entity_id(newer, 20), None);
        assert!(!actors.bind_local_client_entity(newer, 21, 32));
        let other = LivePlayerLease {
            key: PlayerKey(uuid::Uuid::from_u128(45)),
            gid: GlobalPlayerId::new(ServerId(1), PlayerSlot(5)),
            incarnation: 9,
        };
        assert!(actors.admit_local(other.gid, other.key.0));
        assert!(!actors.bind_local_client_entity(other, 22, 31));
        actors.remove(gid);
        assert_eq!(actors.local_client_entity_id(lease, 20), None);
        assert!(actors.admit_local(gid, lease.key.0));
        assert!(actors.bind_local_client_entity(newer, 21, 32));
        assert_eq!(actors.local_client_entity_id(lease, 20), None);
        assert_eq!(actors.local_client_entity_id(newer, 21), Some(32));
    }

    #[test]
    fn imported_remote_avatar_reserves_gid_until_exact_retirement() {
        let gid = GlobalPlayerId::new(ServerId(2), PlayerSlot(7));
        let world = WorldId::new(0, 0);
        let chunk = ChunkKey::new(world, 5, 6);
        let lease = LivePlayerLease {
            key: PlayerKey(uuid::Uuid::from_u128(83)),
            gid,
            incarnation: 4,
        };
        let mut living_state = super::super::cluster_actor_import::from_player_nbt(gid, None)
            .unwrap()
            .state;
        living_state.avatar = Some(pumpkin_cluster::item_action::PlayerAvatarState {
            stance: Default::default(),
            mount: None,
            relation_versions: Default::default(),
            statistics: Default::default(),
            experience: Default::default(),
            presentation: Default::default(),
            spatial_epoch: pumpkin_cluster::canonical_action::SpatialEpoch::Admission {
                lease,
                admitted_at: TickStamp(20),
            },
            world,
            position: [81.0_f64.to_bits(), 80.0_f64.to_bits(), 97.0_f64.to_bits()],
            yaw: 0.0_f32.to_bits(),
            head_yaw: 0.0_f32.to_bits(),
            pitch: 0.0_f32.to_bits(),
            on_ground: true,
            movement_tick: TickStamp(20),
            movement_sequence: PlayerSeq(9),
            game_mode: 0,
            fall_distance: 0.0_f32.to_bits(),
            on_fire: false,
        });
        let imported = pumpkin_cluster::xfer::PlayerLivingGround {
            lease,
            admitted_at: TickStamp(20),
            target: Target::Chunk {
                key: chunk,
                part: ChunkPart::Player(gid),
                inventory: None,
            },
            cell: Cell {
                revision: 7,
                value: Value::Living((living_state).into()),
            },
            spatial: PosUpdate {
                spatial_epoch: pumpkin_cluster::canonical_action::SpatialEpoch::Admission {
                    lease,
                    admitted_at: TickStamp(20),
                },
                gid,
                incarnation: lease.incarnation,
                seq: PlayerSeq(9),
                tick: TickStamp(20),
                world,
                pos: [81.0, 80.0, 97.0],
                vel: [0.0; 3],
                yaw: 0.0,
                head_yaw: 0.0,
                pitch: 0.0,
                on_ground: true,
            },
        };
        let mut actors = OwnedActors::default();
        let mut living = OwnedLivingActors::default();
        assert!(actors.import_remote(&imported));
        assert!(living.import_remote(&imported));
        assert_eq!(actors.get(gid).unwrap().ground.chunk, chunk);
        assert_eq!(actors.get(gid).unwrap().ground.revision, 7);
        assert!(!actors.admit_local(gid, uuid::Uuid::from_u128(84)));
        let mut next = imported;
        next.lease.incarnation = 5;
        next.spatial.incarnation = 5;
        assert!(!actors.import_remote(&next));
        actors.remove(gid);
        living.remove(pumpkin_cluster::actor::ActorId::Player(gid));
        assert!(actors.import_remote(&next));
        assert!(living.import_remote(&next));
    }

    #[test]
    fn spatial_projection_keeps_distinct_local_and_ground_avatar_effects() {
        let gid = GlobalPlayerId::new(ServerId(2), PlayerSlot(7));
        let import = super::super::cluster_actor_import::from_player_nbt(gid, None).unwrap();
        let mut living = OwnedLivingActors::default();
        living.install(import.actor, import.state).unwrap();
        let mut actors = OwnedActors::default();
        let first = actors
            .apply_controller(intent(1, 1, 50.0, 2.0), |_, _| None)
            .unwrap();
        assert!(living.project_player_spatial(gid, first, admission_epoch(gid)));
        let truth = living.actors.get_mut(&import.actor).unwrap();
        truth.local.avatar.as_mut().unwrap().on_fire = true;
        let next = actors
            .apply_controller(intent(2, 2, 50.0, 3.0), |_, _| None)
            .unwrap();
        assert!(living.project_player_spatial(gid, next, admission_epoch(gid)));
        let truth = living.get(import.actor).unwrap();
        assert_eq!(
            f64::from_bits(truth.local.avatar.as_ref().unwrap().position[0]),
            3.0
        );
        assert_eq!(
            f64::from_bits(truth.ground.avatar.as_ref().unwrap().position[0]),
            3.0
        );
        assert!(truth.local.avatar.as_ref().unwrap().on_fire);
        assert!(!truth.ground.avatar.as_ref().unwrap().on_fire);
        let mut reset = truth.local.player_administrative_state().unwrap();
        reset.health = 10.0;
        reset.on_fire = false;
        let after = truth
            .local
            .with_player_administrative_state(&reset)
            .unwrap();
        assert_eq!(after.combat.health_milli, 10_000);
        assert_eq!(after.combat.revision, truth.local.combat.revision + 1);
        assert!(!after.avatar.as_ref().unwrap().on_fire);
        assert_eq!(
            after.player_administrative_state().unwrap().position[0],
            3.0
        );
        let mut forged = after.clone();
        forged.avatar.as_mut().unwrap().position[0] = f64::NAN.to_bits();
        assert_eq!(
            living.install(import.actor, forged),
            Err(super::ActorEffectError::InvalidActor)
        );

        let ground_before = living.get(import.actor).unwrap().ground.clone();
        let mut ground_after = ground_before.clone();
        ground_after.combat.health_milli = 5_000;
        let prepared_ground = living
            .prepare_replacement(true, vec![(import.actor, ground_after)])
            .unwrap();
        let prepared_local = living
            .prepare_replacement(false, vec![(import.actor, after)])
            .unwrap();
        living.install_replacement(prepared_ground);
        living.install_replacement(prepared_local);
        let combined = living.get(import.actor).unwrap();
        assert_eq!(combined.ground.combat.health_milli, 5_000);
        assert_eq!(combined.local.combat.health_milli, 10_000);
        let spatial = actors.get(gid).unwrap();
        let mut ground_spatial = spatial.ground;
        ground_spatial.frame.effective.position[0] = 4.0;
        let mut local_spatial = spatial.local;
        local_spatial.frame.effective.position[0] = 5.0;
        let prepared_ground = actors
            .prepare_replacement(true, vec![(gid, ground_spatial)])
            .unwrap();
        let prepared_local = actors
            .prepare_replacement(false, vec![(gid, local_spatial)])
            .unwrap();
        actors.install_replacement(prepared_ground);
        actors.install_replacement(prepared_local);
        assert_eq!(
            actors.get(gid).unwrap().ground.frame.effective.position[0],
            4.0
        );
        assert_eq!(
            actors.get(gid).unwrap().local.frame.effective.position[0],
            5.0
        );
    }

    fn admission_epoch(gid: GlobalPlayerId) -> pumpkin_cluster::canonical_action::SpatialEpoch {
        pumpkin_cluster::canonical_action::SpatialEpoch::Admission {
            lease: pumpkin_cluster::canonical_action::LivePlayerLease {
                key: pumpkin_cluster::canonical_action::PlayerKey(uuid::Uuid::nil()),
                gid,
                incarnation: 1,
            },
            admitted_at: TickStamp(0),
        }
    }

    fn intent(seq: u16, tick: u16, claimed_x: f64, effective_x: f64) -> ControllerActorIntent {
        let actor = GlobalPlayerId::new(ServerId(2), PlayerSlot(7));
        let claimed = ClientClaimedState {
            actor,
            seq: PlayerSeq(seq),
            tick: TickStamp(tick),
            position: [claimed_x, 80.0, 0.0],
            velocity: [0.0; 3],
            yaw: 0.0,
            pitch: 0.0,
            on_ground: true,
            input_flags: 0,
        };
        ControllerActorIntent {
            actor,
            player_key: uuid::Uuid::from_u128(7),
            spatial_epoch: Some(pumpkin_cluster::canonical_action::SpatialEpoch::Admission {
                lease: pumpkin_cluster::canonical_action::LivePlayerLease {
                    key: pumpkin_cluster::canonical_action::PlayerKey(uuid::Uuid::from_u128(7)),
                    gid: actor,
                    incarnation: 1,
                },
                admitted_at: TickStamp(tick),
            }),
            owner_install_generation: 9,
            head_yaw: 0.0,
            source: ControllerActorSource::Initial,
            world: WorldId::new(0, 3),
            gamemode: Some(0),
            tick: TickStamp(tick),
            frame: ControllerFrame {
                claimed,
                effective: EffectiveControllerState {
                    actor,
                    source_seq: PlayerSeq(seq),
                    tick: TickStamp(tick),
                    position: [effective_x, 80.0, 0.0],
                    velocity: [0.0; 3],
                    yaw: 0.0,
                    pitch: 0.0,
                    on_ground: true,
                    input_flags: 0,
                },
                decision: ControllerDecision::Modified(1),
            },
            position_changed: true,
            velocity_changed: false,
            rotation_changed: false,
            ground_changed: false,
            input_changed: false,
            sneaking: None,
            sprinting: None,
            crawling: None,
            swimming: None,
            gliding: None,
            flying: None,
            effective_flying: false,
            movement_statistic: ControllerMovementStatistic::Unchecked,
        }
    }

    #[test]
    fn mounted_motion_resets_fall_only_for_the_accepted_epoch() {
        let mut actors = OwnedActors::default();
        let initial = intent(1, 10, 0.0, 0.0);
        actors.apply_controller(initial, |_, _| None).unwrap();
        actors
            .local_epochs
            .insert(initial.actor, initial.spatial_epoch.unwrap());
        actors
            .actors
            .get_mut(&initial.actor)
            .unwrap()
            .local
            .fall_distance = 3.0;
        let mut falling = intent(2, 11, 0.0, 0.0);
        falling.source = ControllerActorSource::Client;
        falling.spatial_epoch = initial.spatial_epoch;
        falling.frame.effective.position[1] = 75.0;
        falling.frame.effective.on_ground = false;
        assert_eq!(
            actors.next_environment_fall_distance(&falling, false, false),
            8.0
        );
        assert_eq!(
            actors.next_environment_fall_distance(&falling, false, true),
            0.0
        );
        falling.spatial_epoch = None;
        assert_eq!(
            actors.next_environment_fall_distance(&falling, false, true),
            3.0
        );
    }

    #[test]
    fn local_admission_key_is_retired_before_slot_reuse() {
        let mut actors = OwnedActors::default();
        let gid = GlobalPlayerId::new(ServerId(2), PlayerSlot(7));
        let old = uuid::Uuid::from_u128(1);
        let next = uuid::Uuid::from_u128(2);
        assert!(actors.admit_local(gid, old));
        assert!(!actors.admit_local(gid, next));
        assert!(!actors.admit_local(GlobalPlayerId::new(ServerId(2), PlayerSlot(8)), old,));
        assert_eq!(actors.local_key(gid), Some(old));
        assert_eq!(actors.local_actor_for_key(old), Some(gid));
        actors.remove(gid);
        assert_eq!(actors.local_actor_for_key(old), None);
        assert!(actors.admit_local(gid, next));
        assert_eq!(actors.local_key(gid), Some(next));
        assert_eq!(actors.local_actor_for_key(next), Some(gid));
    }

    #[test]
    fn client_movement_does_not_advance_accepted_actor_state() {
        let gid = GlobalPlayerId::new(ServerId(2), PlayerSlot(7));
        let import = super::super::cluster_actor_import::from_player_nbt(gid, None).unwrap();
        let mut actors = OwnedActors::default();
        let mut living = OwnedLivingActors::default();
        living.install(import.actor, import.state).unwrap();
        let first = actors
            .apply_controller(intent(1, 1, 0.0, 0.0), |_, _| None)
            .unwrap();
        assert!(living.project_player_spatial(gid, first, admission_epoch(gid)));
        let mut movement = intent(2, 2, 3.0, 3.0);
        movement.source = ControllerActorSource::Client;
        movement.gamemode = None;
        movement.frame.effective.velocity = [1.0, 0.0, 0.0];
        let next = actors.apply_controller(movement, |_, _| None).unwrap();
        assert!(living.project_player_spatial(gid, next, admission_epoch(gid)));
        let spatial = actors.get(gid).unwrap();
        assert_eq!(spatial.local.frame.effective.position[0], 3.0);
        assert_eq!(spatial.ground.frame.effective.position[0], 0.0);
        let state = living.get(import.actor).unwrap();
        assert_eq!(state.local.combat.velocity_micro, [1_000_000, 0, 0]);
        assert_eq!(state.ground.combat.velocity_micro, [0; 3]);
        assert_eq!(state.local.combat.revision, state.ground.combat.revision);
        assert_eq!(
            f64::from_bits(state.ground.avatar.as_ref().unwrap().position[0]),
            0.0
        );
    }

    #[test]
    fn combat_ground_refuses_a_local_velocity_witness() {
        let gid = GlobalPlayerId::new(ServerId(2), PlayerSlot(7));
        let import = super::super::cluster_actor_import::from_player_nbt(gid, None).unwrap();
        let mut living = OwnedLivingActors::default();
        living.install(import.actor, import.state).unwrap();
        let mut actors = OwnedActors::default();
        let first = actors
            .apply_controller(intent(1, 1, 0.0, 0.0), |_, _| None)
            .unwrap();
        assert!(living.project_player_spatial(gid, first, admission_epoch(gid)));
        let mut movement = intent(2, 2, 3.0, 3.0);
        movement.source = ControllerActorSource::Client;
        movement.gamemode = None;
        movement.frame.effective.velocity = [1.0, 0.0, 0.0];
        let next = actors.apply_controller(movement, |_, _| None).unwrap();
        assert!(living.project_player_spatial(gid, next, admission_epoch(gid)));
        let before = living.get(import.actor).unwrap().local.combat.clone();
        let world = WorldId::new(0, 3);
        let loc = InvLoc::new(INV_MAIN, 0);
        let mut action = ItemActionIntent {
            actor: import.actor,
            tick: TickStamp(2),
            source: InventorySlotRef::new(InventoryTarget::Player(gid), loc),
            expected_inventory_revision: 0,
            expected_item: InventoryCell {
                loc,
                item: 4,
                count: 1,
                nbt: Vec::new(),
            },
            actor_chunk: ChunkKey::new(world, 0, 0),
            affected: Vec::new(),
            kind: ItemActionKind::UseOnBlock {
                world,
                block: BlockPos { x: 0, y: 80, z: 0 },
                hit: ClaimedHit {
                    position: [0.5; 3],
                    face: 1,
                },
                expected_block_state: 1,
            },
            client_claim: ItemActionClientClaim::default(),
        }
        .capture(
            PlayerSeq(2),
            EffectiveItemPose {
                actor: import.actor,
                tick: TickStamp(2),
                world,
                position: [3.0, 80.0, 0.0],
                eye_position: [3.0, 81.62, 0.0],
                yaw: 0.0,
                pitch: 0.0,
                reach: 5.0,
            },
            None,
            ItemActionPlan::default(),
        );
        action.combat = ItemCombatEffect::new(import.actor, before, 1_000, [100_000, 0, 0]);
        let local = living.prepare_local(&action).unwrap();
        assert_eq!(local[0].1.combat.velocity_micro, [1_100_000, 0, 0]);
        assert_eq!(
            living.prepare_ground(&action),
            Err(super::ActorEffectError::StaleCombat)
        );
    }

    #[test]
    fn accepted_boundary_uses_its_snapshot_after_later_local_movement() {
        let gid = GlobalPlayerId::new(ServerId(2), PlayerSlot(7));
        let import = super::super::cluster_actor_import::from_player_nbt(gid, None).unwrap();
        let world = WorldId::new(0, 3);
        let mut actors = OwnedActors::default();
        let mut living = OwnedLivingActors::default();
        living.install(import.actor, import.state).unwrap();
        let first = actors
            .apply_controller(intent(1, 1, 0.0, 0.0), |_, _| None)
            .unwrap();
        assert!(living.project_player_spatial(gid, first, admission_epoch(gid)));
        for (seq, x) in [(2, 17.0), (3, 33.0)] {
            let mut movement = intent(seq, seq, x, x);
            movement.source = ControllerActorSource::Client;
            movement.gamemode = None;
            let next = actors.apply_controller(movement, |_, _| None).unwrap();
            assert!(living.project_player_spatial(gid, next, admission_epoch(gid)));
        }
        let update = PlayerChunkTransition {
            gid,
            incarnation: 1,
            seq: PlayerSeq(2),
            tick: TickStamp(2),
            from: ChunkKey::new(world, 0, 0),
            to: ChunkKey::new(world, 1, 0),
            snapshot: PlayerReplicaSnapshot {
                movement: PosUpdate {
                    spatial_epoch: pumpkin_cluster::canonical_action::SpatialEpoch::Admission {
                        lease: pumpkin_cluster::canonical_action::LivePlayerLease {
                            key: pumpkin_cluster::canonical_action::PlayerKey(uuid::Uuid::nil()),
                            gid: gid,
                            incarnation: 1,
                        },
                        admitted_at: TickStamp(2),
                    },
                    gid,
                    incarnation: 1,
                    seq: PlayerSeq(2),
                    tick: TickStamp(2),
                    world,
                    pos: [17.0, 80.0, 0.0],
                    vel: [0.25, 0.0, 0.0],
                    yaw: 1.0,
                    head_yaw: 1.0,
                    pitch: 0.0,
                    on_ground: true,
                },
                visual: PlayerVisualSnapshot::default(),
            },
        };
        let spatial = actors
            .prepare_accepted_transition(&update, Some(4))
            .unwrap()
            .unwrap();
        let avatar = living
            .prepare_accepted_transition(&update)
            .unwrap()
            .unwrap();
        let prepared_spatial = actors
            .prepare_replacement(true, vec![(gid, spatial)])
            .unwrap();
        let prepared_avatar = living
            .prepare_replacement(true, vec![(import.actor, avatar)])
            .unwrap();
        actors.install_replacement(prepared_spatial);
        living.install_replacement(prepared_avatar);
        assert_eq!(
            actors.get(gid).unwrap().local.chunk,
            ChunkKey::new(world, 2, 0)
        );
        assert_eq!(actors.get(gid).unwrap().ground.chunk, update.to);
        assert_eq!(actors.get(gid).unwrap().ground.biome_id, Some(4));
        let state = living.get(import.actor).unwrap();
        assert_eq!(
            f64::from_bits(state.local.avatar.as_ref().unwrap().position[0]),
            33.0
        );
        assert_eq!(
            f64::from_bits(state.ground.avatar.as_ref().unwrap().position[0]),
            17.0
        );
        assert_eq!(state.ground.combat.velocity_micro, [250_000, 0, 0]);
        let mut second = update;
        second.seq = PlayerSeq(3);
        second.tick = TickStamp(3);
        second.from = update.to;
        second.to = ChunkKey::new(world, 2, 0);
        second.snapshot.movement.seq = second.seq;
        second.snapshot.movement.tick = second.tick;
        second.snapshot.movement.pos[0] = 33.0;
        let spatial = actors
            .prepare_accepted_transition(&second, Some(5))
            .unwrap()
            .unwrap();
        let avatar = living
            .prepare_accepted_transition(&second)
            .unwrap()
            .unwrap();
        let prepared_spatial = actors
            .prepare_replacement(true, vec![(gid, spatial)])
            .unwrap();
        let prepared_avatar = living
            .prepare_replacement(true, vec![(import.actor, avatar)])
            .unwrap();
        actors.install_replacement(prepared_spatial);
        living.install_replacement(prepared_avatar);
        assert_eq!(actors.get(gid).unwrap().ground.chunk, second.to);
        assert_eq!(
            f64::from_bits(
                living
                    .get(import.actor)
                    .unwrap()
                    .ground
                    .avatar
                    .as_ref()
                    .unwrap()
                    .position[0]
            ),
            33.0
        );
    }

    #[test]
    fn effective_position_owns_chunk_while_claim_is_preserved_for_evidence() {
        let mut actors = OwnedActors::default();
        let applied = actors
            .apply_controller(intent(1, 10, 32.0, -0.25), |_, _| Some(4))
            .unwrap();
        assert_eq!(applied.chunk.x(), -1);
        assert_eq!(applied.biome_id, Some(4));
        assert_eq!(applied.frame.claimed.position[0], 32.0);
        assert_eq!(applied.frame.effective.position[0], -0.25);
        assert_eq!(
            actors.get(applied.frame.claimed.actor).unwrap().ground,
            applied
        );
    }

    #[test]
    fn controller_input_is_readable_without_mutating_local_or_ground_truth() {
        let mut actors = OwnedActors::default();
        let initial = actors
            .apply_controller(intent(1, 10, 0.0, 0.0), |_, _| None)
            .unwrap();
        let actor = initial.frame.effective.actor;
        let before = *actors.get(actor).unwrap();
        let mut movement = intent(2, 11, 17.0, 17.0);
        movement.source = ControllerActorSource::Client;
        movement.gamemode = None;
        let projected = actors
            .capture_controller_input(movement, |_, _| None)
            .unwrap();
        assert_eq!(projected.chunk.x(), 1);
        assert_eq!(*actors.get(actor).unwrap(), before);
        assert_eq!(actors.input_projection(actor), Some(projected));

        let mut next = intent(3, 12, 18.0, 18.0);
        next.source = ControllerActorSource::Client;
        next.gamemode = None;
        let projected = actors.capture_controller_input(next, |_, _| None).unwrap();
        assert_eq!(projected.revision, before.local.revision + 2);
        assert_eq!(*actors.get(actor).unwrap(), before);
    }

    #[test]
    fn delayed_policy_correction_replaces_same_sequence_only_on_later_tick() {
        let mut actors = OwnedActors::default();
        actors
            .apply_controller(intent(1, 10, 0.0, 0.0), |_, _| None)
            .unwrap();
        let mut same_tick = intent(1, 10, 0.0, 4.0);
        same_tick.source = ControllerActorSource::Client;
        same_tick.gamemode = None;
        assert_eq!(
            actors.apply_controller(same_tick, |_, _| None),
            Err(ActorSpatialError::StaleInput)
        );
        let mut later = intent(1, 11, 0.0, 4.0);
        later.source = ControllerActorSource::Client;
        later.gamemode = None;
        let corrected = actors.apply_controller(later, |_, _| None).unwrap();
        assert_eq!(corrected.frame.effective.position[0], 4.0);
        assert_eq!(corrected.revision, 1);
    }

    #[test]
    fn initial_frame_does_not_block_wrapped_first_movement() {
        let mut actors = OwnedActors::default();
        actors
            .apply_controller(intent(u16::MAX, 10, 0.0, 0.0), |_, _| None)
            .unwrap();
        let mut move_input = intent(0, 10, 0.0, 17.0);
        move_input.source = ControllerActorSource::Client;
        move_input.gamemode = None;
        let moved = actors.apply_controller(move_input, |_, _| None).unwrap();
        assert_eq!(moved.chunk.x(), 1);
        assert_eq!(moved.revision, 1);
    }

    #[test]
    fn accepted_admin_teleport_overrides_same_tick_client_pose_once() {
        let mut actors = OwnedActors::default();
        let first = intent(5, 10, 0.0, 1.0);
        actors.apply_controller(first, |_, _| None).unwrap();
        let mut teleport = intent(5, 10, 0.0, 48.0);
        teleport.source = ControllerActorSource::AcceptedAdministrative(
            pumpkin_cluster::completion::ActionKey {
                incarnation: 0,
                actor: pumpkin_cluster::actor::ActorId::Player(teleport.actor),
                sequence: 9,
                family: 7,
            },
            TickStamp(5),
        );
        teleport.gamemode = Some(1);
        let applied = actors.apply_controller(teleport, |_, _| None).unwrap();
        assert_eq!(applied.chunk.x(), 3);
        assert_eq!(applied.game_mode, 1);
        assert_eq!(
            actors.apply_controller(teleport, |_, _| None),
            Err(ActorSpatialError::StaleInput)
        );
    }

    #[test]
    fn client_movement_cannot_change_accepted_game_mode() {
        let mut actors = OwnedActors::default();
        let first = intent(1, 10, 0.0, 0.0);
        let actor = first.actor;
        actors.apply_controller(first, |_, _| None).unwrap();
        let mut move_input = intent(2, 11, 0.0, 1.0);
        move_input.source = ControllerActorSource::Client;
        move_input.gamemode = None;
        assert_eq!(
            actors
                .apply_controller(move_input, |_, _| None)
                .unwrap()
                .game_mode,
            0
        );
        let mut teleport = intent(2, 11, 0.0, 48.0);
        teleport.source = ControllerActorSource::AcceptedAdministrative(
            pumpkin_cluster::completion::ActionKey {
                incarnation: 0,
                actor: pumpkin_cluster::actor::ActorId::Player(actor),
                sequence: 9,
                family: 7,
            },
            TickStamp(2),
        );
        teleport.gamemode = Some(1);
        assert_eq!(
            actors
                .apply_controller(teleport, |_, _| None)
                .unwrap()
                .game_mode,
            1
        );
        let mut forged = intent(3, 12, 0.0, 49.0);
        forged.source = ControllerActorSource::Client;
        forged.gamemode = Some(0);
        assert_eq!(
            actors.apply_controller(forged, |_, _| None),
            Err(ActorSpatialError::InvalidGameMode)
        );
        forged.gamemode = None;
        assert_eq!(
            actors
                .apply_controller(forged, |_, _| None)
                .unwrap()
                .game_mode,
            1
        );
    }
}
