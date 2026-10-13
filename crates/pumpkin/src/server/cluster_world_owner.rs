use std::collections::{BTreeMap, BTreeSet, HashMap, HashSet, VecDeque};
use std::sync::Arc;

use pumpkin_cluster::accept::{AcceptanceDelayLevel, AcceptanceDelayTracker};
use pumpkin_cluster::canonical_action::{ActionInput, ActionState, Target, TruthView};
use pumpkin_cluster::codec::decode_batch;
use pumpkin_cluster::completion::{
    ActionKey, ChunkActions, CompletionMessage, LocalTickBatch, TickFinish, encode_finish,
    encode_realm_finish,
};
use pumpkin_cluster::identity::{GlobalPlayerId, ServerId};
use pumpkin_cluster::inventory::InventoryCaptureBarrier;
use pumpkin_cluster::protocol::{
    ChunkKey, GlobalEntityId, PlayerBoundaryUpdate, StreamKind, TickBatch,
};
use pumpkin_cluster::realm_completion::{
    CompletionFailure as RealmCompletionFailure, HolderDomain, PeerGeneration,
    RealmCompletionLedger, RequirementKey,
};
use pumpkin_cluster::streams::{InboundParcel, OutboundParcel, StreamHeader};
use pumpkin_cluster::time::TickStamp;
use tokio::sync::{mpsc, oneshot, watch};

use super::cluster_chunk_state::OwnedChunkStore;
use super::cluster_world_actions::{ReadyPromotion, WorldTickBuckets};
use super::{Server, cluster_datagram};

struct PendingInboundBatch {
    source: PeerGeneration,
    encoded_bytes: usize,
    batch: TickBatch,
}

fn new_actor_draft<S: ActionState>(
    owners: &S,
    action: &pumpkin_cluster::canonical_action::Action,
) -> Result<
    pumpkin_cluster::actor_action_scope::ActorActionDraft,
    pumpkin_cluster::actor_action_scope::ScopeFailure,
> {
    use pumpkin_cluster::actor_action_scope::{
        ActorActionDraft, FrozenEntityIssueAuthority, ScopeFailure,
    };
    let pumpkin_cluster::actor::ActorId::Entity(entity) = action.id.actor else {
        return Ok(ActorActionDraft::new(
            action.id.actor,
            action.id.incarnation,
            action.tick,
            4096,
            16384,
        ));
    };
    let chunk = action.actor_chunk.ok_or(ScopeFailure::Identity)?;
    let target = Target::Chunk {
        key: chunk,
        part: pumpkin_cluster::canonical_action::ChunkPart::Entity(entity),
        inventory: None,
    };
    let cell = owners
        .borrow(TruthView::Local, target)
        .ok_or(ScopeFailure::Identity)?;
    let pumpkin_cluster::canonical_action::Value::Entity(body) = &cell.value else {
        return Err(ScopeFailure::Identity);
    };
    ActorActionDraft::new_entity(
        owners,
        FrozenEntityIssueAuthority {
            target,
            birth: body.birth.clone(),
            producer: action
                .effects
                .iter()
                .find_map(|effect| match effect {
                    pumpkin_cluster::canonical_action::Effect::ChangeDroppedItem {
                        issuer, ..
                    } => Some(*issuer),
                    pumpkin_cluster::canonical_action::Effect::MoveEntityBody {
                        authority: Some(authority),
                        ..
                    } => Some(authority.issuer),
                    _ => None,
                })
                .unwrap_or(body.owner),
            controlled_owner: body.owner,
            effective_tick: action.tick,
            next_sequence: action.id.sequence,
        },
        action.tick,
        4096,
        16384,
    )
}

type PlayerBoundaryKey = (GlobalPlayerId, u64, TickStamp, u16, ChunkKey, ChunkKey);
type PendingReplicaBoundary = (pumpkin_cluster::protocol::PlayerReplicaSnapshot, u64);

fn note_terminal_actor_generation(
    terminal: &mut HashMap<GlobalPlayerId, u64>,
    actor: GlobalPlayerId,
    generation: u64,
) {
    terminal
        .entry(actor)
        .and_modify(|previous| {
            let distance = generation.wrapping_sub(*previous);
            if distance != 0 && distance < (1_u64 << 63) {
                *previous = generation;
            }
        })
        .or_insert(generation);
}

fn actor_generation_is_terminal(
    terminal: &HashMap<GlobalPlayerId, u64>,
    actor: GlobalPlayerId,
    generation: u64,
) -> bool {
    terminal.get(&actor).is_some_and(|retired| {
        let distance = generation.wrapping_sub(*retired);
        distance == 0 || distance >= (1_u64 << 63)
    })
}

#[derive(Clone, Copy)]
struct BoundaryFrontier {
    generation: u64,
    incarnation: u64,
    leave: Option<(TickStamp, pumpkin_cluster::identity::PlayerSeq)>,
    latest_snapshot: Option<(TickStamp, pumpkin_cluster::identity::PlayerSeq)>,
}

fn boundary_after(
    tick: TickStamp,
    seq: pumpkin_cluster::identity::PlayerSeq,
    prior: (TickStamp, pumpkin_cluster::identity::PlayerSeq),
) -> bool {
    tick.is_after(prior.0) || (tick == prior.0 && seq.is_newer_than(prior.1))
}

impl BoundaryFrontier {
    fn accepts_leave(self, tick: TickStamp, seq: pumpkin_cluster::identity::PlayerSeq) -> bool {
        self.leave
            .is_none_or(|leave| boundary_after(tick, seq, leave))
            && self
                .latest_snapshot
                .is_none_or(|snapshot| !boundary_after(snapshot.0, snapshot.1, (tick, seq)))
    }
}

#[derive(Default)]
struct LocalFinishes {
    pending: BTreeMap<TickStamp, Vec<TickFinish>>,
    last_tick: Option<TickStamp>,
    closed_through: Option<TickStamp>,
}

struct OwnerTickScope {
    full_source_slot: Option<u64>,
    local: ServerId,
    directory: Arc<pumpkin_cluster::chunks::Directory>,
    local_chunks: Arc<HashMap<ChunkKey, Arc<super::cluster_chunk_state::OwnedChunkBlocks>>>,
    incarnations: Arc<HashMap<ServerId, u64>>,
    transport_generations: Arc<HashMap<ServerId, u64>>,
    producers: BTreeSet<PeerGeneration>,
    primary_producers: BTreeSet<PeerGeneration>,
    players: BTreeMap<pumpkin_cluster::canonical_action::LivePlayerLease, ChunkKey>,
    #[cfg(test)]
    entities: BTreeMap<GlobalEntityId, (ChunkKey, PeerGeneration)>,
}

fn primary_dropped_lifecycle(
    action: &pumpkin_cluster::canonical_action::Action,
    source: PeerGeneration,
) -> bool {
    use pumpkin_cluster::actor::ActorId;
    use pumpkin_cluster::canonical_action::{ActionKind, ChunkPart, Effect};
    let body = match action.id.actor {
        ActorId::Entity(id) if action.id.incarnation == u64::from(id.generation) => {
            action.actor_chunk.map(|key| Target::Chunk {
                key,
                part: ChunkPart::Entity(id),
                inventory: None,
            })
        }
        ActorId::Peer(peer)
            if peer == source.server && action.id.incarnation == source.generation =>
        {
            None
        }
        _ => return false,
    };
    if action.kind == ActionKind::Actor && action.chunk_transition {
        return matches!(action.effects.as_slice(), [Effect::MoveEntityBody { source: target, authority: Some(authority), .. }]
            if body == Some(*target) && authority.issuer == source);
    }
    action.kind == ActionKind::Item
        && !action.chunk_transition
        && !action.effects.is_empty()
        && action.effects.iter().all(|effect| match effect {
            Effect::ChangeDroppedItem { target, issuer, .. } => {
                *issuer == source && body.is_none_or(|body| body == *target)
            }
            Effect::MergeDroppedItems { source, .. } => body.is_none_or(|body| body == *source),
            _ => false,
        })
}

impl OwnerTickScope {
    fn freeze(
        server: &Server,
        inventory: &super::cluster_world_inventory::InventoryOwner,
        _blocks: &OwnedChunkStore,
        actors: &super::cluster_world_actor::OwnedActors,
        living: &super::cluster_world_actor::OwnedLivingActors,
        tick: TickStamp,
    ) -> Self {
        let local = ServerId(server.advanced_config.cluster.server_id);
        let incarnations = server.cluster_peer_incarnations.load_full();
        let producers = incarnations
            .iter()
            .map(|(&server, &generation)| PeerGeneration { server, generation })
            .collect();
        let mut primary_servers = server.cluster_primary_peers.load().as_ref().clone();
        if server.is_cluster_primary() {
            primary_servers.insert(local);
        }
        let primary_producers = primary_servers
            .into_iter()
            .filter_map(|server| {
                incarnations
                    .get(&server)
                    .copied()
                    .map(|generation| PeerGeneration { server, generation })
            })
            .collect();
        let mut players = inventory
            .ground_player_bindings()
            .into_iter()
            .collect::<BTreeMap<_, _>>();
        for declaration in actors.prepared_remote_admissions() {
            if (tick == declaration.reference.tick || tick.is_after(declaration.reference.tick))
                && incarnations.get(&declaration.owner.server)
                    == Some(&declaration.owner.generation)
                && !players
                    .keys()
                    .any(|lease| lease.gid == declaration.lease.gid && *lease != declaration.lease)
            {
                let chunk = living
                    .phase_state(
                        pumpkin_cluster::actor::ActorId::Player(declaration.lease.gid),
                        true,
                    )
                    .and_then(|_| {
                        actors
                            .get(declaration.lease.gid)
                            .map(|truth| truth.ground.chunk)
                    })
                    .unwrap_or(declaration.chunk);
                players.insert(declaration.lease, chunk);
            }
        }
        Self {
            full_source_slot: super::cluster_world_rules::slot_for_tick(tick),
            local,
            #[cfg(test)]
            entities: _blocks.ground_entity_bindings(),
            directory: server.cluster_chunk_directory.load_full(),
            local_chunks: server.cluster_owned_chunk_blocks.load_full(),
            incarnations,
            transport_generations: server.cluster_peer_generations.load_full(),
            producers,
            primary_producers,
            players,
        }
    }

    fn source(&self, server: ServerId) -> Result<PeerGeneration, RealmCompletionFailure> {
        self.incarnations
            .get(&server)
            .copied()
            .map(|generation| PeerGeneration { server, generation })
            .ok_or(RealmCompletionFailure::MissingPeerIncarnation)
    }

    fn tentative_local_manifest(
        &self,
        tick: TickStamp,
        actions: &[pumpkin_cluster::canonical_action::Action],
        issued: &BTreeMap<RequirementKey, BTreeSet<pumpkin_cluster::canonical_action::ActionId>>,
        completion: &RealmCompletionLedger,
        mut entity_authority: impl FnMut(
            &pumpkin_cluster::canonical_action::Action,
        ) -> Option<(ChunkKey, PeerGeneration)>,
    ) -> Result<
        (
            pumpkin_cluster::realm_completion::HolisticFinish,
            BTreeSet<PeerGeneration>,
        ),
        RealmCompletionFailure,
    > {
        let mut candidate = RealmCompletionLedger::default();
        let mut issued = issued.clone();
        let mut recipients = self.producers.clone();
        for action in actions {
            if action.tick != tick {
                return Err(RealmCompletionFailure::InvalidActionRequirements);
            }
            let requirements = self.capture_action_with_entity_authority(
                &mut candidate,
                self.local,
                action,
                entity_authority(action),
            )?;
            for key in requirements {
                match key.stakeholder {
                    pumpkin_cluster::canonical_action::Stakeholder::Chunk(chunk) => {
                        recipients.extend(self.chunk_holders(chunk)?);
                    }
                    pumpkin_cluster::canonical_action::Stakeholder::Player(player) => {
                        let chunk = action
                            .player_chunks
                            .iter()
                            .find_map(|(key, chunk)| (*key == player).then_some(*chunk))
                            .ok_or(RealmCompletionFailure::InvalidActionRequirements)?;
                        recipients.extend(self.chunk_holders(chunk)?);
                        let lease = action
                            .player_leases
                            .iter()
                            .find(|lease| lease.key == player)
                            .ok_or(RealmCompletionFailure::InvalidActionRequirements)?;
                        recipients.insert(self.source(ServerId(lease.gid.server.0))?);
                    }
                }
                issued.entry(key).or_default().insert(action.id);
            }
        }
        recipients.remove(&self.source(self.local)?);
        completion.validate_capture_subset(&candidate)?;
        Ok((
            pumpkin_cluster::realm_completion::HolisticFinish {
                full_source_slot: self
                    .full_source_slot
                    .ok_or(RealmCompletionFailure::MissingRequirement)?,
                acquisition_prefix: None,
                tick,
                issued,
            },
            recipients,
        ))
    }

    fn authenticated_source(
        &self,
        transport: PeerGeneration,
    ) -> Result<PeerGeneration, RealmCompletionFailure> {
        if self.transport_generations.get(&transport.server) != Some(&transport.generation) {
            return Err(RealmCompletionFailure::MissingPeerIncarnation);
        }
        self.source(transport.server)
    }

    fn avatar_bootstrap_chunk(
        &self,
        action: &pumpkin_cluster::canonical_action::Action,
        lease: &pumpkin_cluster::canonical_action::LivePlayerLease,
    ) -> Option<ChunkKey> {
        if action.validate().is_err()
            || self
                .players
                .keys()
                .any(|current| current.gid == lease.gid && current != lease)
        {
            return None;
        }
        action.effects.iter().find_map(|effect| match effect {
            pumpkin_cluster::canonical_action::Effect::PlayerAvatarLifecycle {
                target:
                    Target::Chunk {
                        key,
                        part: pumpkin_cluster::canonical_action::ChunkPart::Player(gid),
                        inventory: None,
                    },
                intent:
                    pumpkin_cluster::avatar_lifecycle::AvatarLifecycleIntent::Admission {
                        lease: declared,
                        owner,
                        ..
                    },
            } if declared == lease
                && *gid == lease.gid
                && self.source(gid.server).ok() == Some(*owner)
                && action.id.actor == pumpkin_cluster::actor::ActorId::Player(*gid)
                && action.id.incarnation == lease.incarnation =>
            {
                Some(*key)
            }
            _ => None,
        })
    }

    fn record_avatar_declaration(
        &mut self,
        action: &pumpkin_cluster::canonical_action::Action,
        tick: TickStamp,
    ) {
        if tick != action.tick && !tick.is_after(action.tick) {
            return;
        }
        for effect in &action.effects {
            if let pumpkin_cluster::canonical_action::Effect::PlayerAvatarLifecycle {
                intent:
                    pumpkin_cluster::avatar_lifecycle::AvatarLifecycleIntent::Admission {
                        lease, ..
                    },
                ..
            } = effect
                && let Some(chunk) = self.avatar_bootstrap_chunk(action, lease)
            {
                self.players.entry(*lease).or_insert(chunk);
            }
        }
    }

    fn legacy_producers(&self) -> Option<BTreeMap<ServerId, u64>> {
        self.producers
            .iter()
            .map(|peer| {
                let generation = if peer.server == self.local {
                    peer.generation
                } else {
                    *self.transport_generations.get(&peer.server)?
                };
                Some((peer.server, generation))
            })
            .collect()
    }

    fn legacy_holders(&self, chunk: ChunkKey) -> Vec<u16> {
        let mut holders = self
            .directory
            .holders_of(&chunk)
            .into_iter()
            .flatten()
            .copied()
            .collect::<Vec<_>>();
        if self.local_chunks.contains_key(&chunk) {
            holders.push(self.local.0);
        }
        holders.sort_unstable();
        holders.dedup();
        holders
    }

    fn chunk_holders(
        &self,
        chunk: ChunkKey,
    ) -> Result<BTreeSet<PeerGeneration>, RealmCompletionFailure> {
        if !self.local_chunks.contains_key(&chunk) {
            return Err(RealmCompletionFailure::MissingRequirement);
        }
        let mut holders = self
            .directory
            .holders_of(&chunk)
            .into_iter()
            .flatten()
            .copied()
            .map(ServerId)
            .collect::<BTreeSet<_>>();
        holders.insert(self.local);
        holders
            .into_iter()
            .map(|server| self.source(server))
            .collect()
    }

    fn actor_phase_cut_witness(
        &self,
    ) -> Result<
        pumpkin_cluster::actor_local_cut::ActorPhaseCutWitness,
        pumpkin_cluster::canonical_action::ApplyFailure,
    > {
        let holders = BTreeMap::new();
        let witness = pumpkin_cluster::actor_local_cut::ActorPhaseCutWitness {
            full_source_slot: self
                .full_source_slot
                .ok_or(pumpkin_cluster::canonical_action::ApplyFailure::InvalidAction)?,
            sources: self.producers.clone(),
            primaries: self.primary_producers.clone(),
            holders,
            players: self.player_associations(),
        };
        if !witness.valid() {
            return Err(pumpkin_cluster::canonical_action::ApplyFailure::InvalidAction);
        }
        Ok(witness)
    }

    fn scheduled_executor(
        &self,
        chunk: ChunkKey,
    ) -> Result<PeerGeneration, pumpkin_cluster::canonical_action::ApplyFailure> {
        self.frozen_chunk_holders(chunk)
            .map_err(|_| {
                pumpkin_cluster::canonical_action::ApplyFailure::MissingStakeholder(
                    pumpkin_cluster::canonical_action::Stakeholder::Chunk(chunk),
                )
            })?
            .intersection(&self.primary_producers)
            .copied()
            .next()
            .ok_or(
                pumpkin_cluster::canonical_action::ApplyFailure::MissingStakeholder(
                    pumpkin_cluster::canonical_action::Stakeholder::Chunk(chunk),
                ),
            )
    }

    fn scheduled_executors(&self) -> BTreeMap<ChunkKey, PeerGeneration> {
        self.local_chunks
            .keys()
            .filter_map(|&chunk| {
                self.scheduled_executor(chunk)
                    .ok()
                    .map(|executor| (chunk, executor))
            })
            .collect()
    }

    pub(crate) fn player_associations(
        &self,
    ) -> BTreeMap<pumpkin_cluster::canonical_action::LivePlayerLease, (ChunkKey, PeerGeneration)>
    {
        self.players
            .iter()
            .filter_map(|(lease, chunk)| {
                self.source(lease.gid.server)
                    .ok()
                    .map(|source| (*lease, (*chunk, source)))
            })
            .collect()
    }

    fn frozen_chunk_holders(
        &self,
        chunk: ChunkKey,
    ) -> Result<BTreeSet<PeerGeneration>, RealmCompletionFailure> {
        let mut holders = self
            .directory
            .holders_of(&chunk)
            .into_iter()
            .flatten()
            .copied()
            .map(ServerId)
            .collect::<BTreeSet<_>>();
        if self.local_chunks.contains_key(&chunk) {
            holders.insert(self.local);
        }
        if holders.is_empty() {
            return Err(RealmCompletionFailure::MissingRequirement);
        }
        holders
            .into_iter()
            .map(|server| self.source(server))
            .collect()
    }

    #[cfg(test)]
    fn capture_action(
        &self,
        ledger: &mut RealmCompletionLedger,
        source: ServerId,
        action: &pumpkin_cluster::canonical_action::Action,
    ) -> Result<BTreeSet<RequirementKey>, RealmCompletionFailure> {
        self.capture_action_with_entity_authority(
            ledger,
            source,
            action,
            match action.id.actor {
                pumpkin_cluster::actor::ActorId::Entity(id) => self.entities.get(&id).copied(),
                _ => None,
            },
        )
    }

    fn capture_action_with_entity_authority(
        &self,
        ledger: &mut RealmCompletionLedger,
        source: ServerId,
        action: &pumpkin_cluster::canonical_action::Action,
        entity_authority: Option<(ChunkKey, PeerGeneration)>,
    ) -> Result<BTreeSet<RequirementKey>, RealmCompletionFailure> {
        use pumpkin_cluster::actor::ActorId;
        use pumpkin_cluster::canonical_action::Stakeholder;

        if action.validate().is_err()
            || (!matches!(action.id.actor, ActorId::Entity(_))
                && action.id.actor.origin() != source)
        {
            return Err(RealmCompletionFailure::InvalidActionRequirements);
        }
        let source = self.source(source)?;
        if let Some(witness) = pumpkin_cluster::issuer_entity_admission::witness(action) {
            let authorized = witness.issuer == source
                && self.full_source_slot == Some(witness.source_slot)
                && witness.lease.is_none_or(|lease| {
                    self.players.get(&lease) == Some(&witness.actor_chunk)
                        && self.source(lease.gid.server).ok() == Some(source)
                }) && match &witness.authority {
                pumpkin_cluster::issuer_entity_admission::IssuerAdmissionAuthority::Primary {
                    rules,
                } => {
                    self.primary_producers.contains(&source)
                        && self.primary_producers.contains(&rules.rules.issuer)
                        && self.scheduled_executor(witness.admission.chunk).ok() == Some(source)
                }
                pumpkin_cluster::issuer_entity_admission::IssuerAdmissionAuthority::Command {
                    receipt,
                } => {
                    receipt.matches(witness)
                        && receipt.issuance.origin == source
                        && (receipt.issuance.transport_generation == 0
                            || self.transport_generations.get(&source.server)
                                == Some(&receipt.issuance.transport_generation))
                }
            };
            if !authorized {
                return Err(RealmCompletionFailure::InvalidActionRequirements);
            }
        }
        if let Some(authority) = &action.edit_authority
            && matches!(
                authority.policy,
                pumpkin_cluster::inventory_edit_authority::InventoryEditPolicy::PrivilegedView(_)
            )
            && (authority.issuer != source
                || !self.players.contains_key(&authority.viewer)
                || authority.preview_provider != self.source(authority.target.gid.server).ok())
        {
            return Err(RealmCompletionFailure::InvalidActionRequirements);
        }
        if let Some(pumpkin_cluster::canonical_action::SemanticIntent::InventoryGestures(intent)) =
            &action.semantic_intent
        {
            let foreign = intent
                .bindings
                .iter()
                .filter_map(|binding| match binding.target {
                    pumpkin_cluster::canonical_action::Target::Player {
                        lease: Some(lease),
                        inventory: Some(_),
                        ..
                    } if action.id.actor != ActorId::Player(lease.gid) => Some(lease),
                    _ => None,
                })
                .collect::<BTreeSet<_>>();
            if !foreign.is_empty()
                && action.edit_authority.as_ref().is_none_or(|authority| {
                    authority.issuer != source
                        || !matches!(
                        authority.policy,
                        pumpkin_cluster::inventory_edit_authority::InventoryEditPolicy::Privileged(
                            _
                        )
                    ) || foreign.len() != 1
                        || !foreign.contains(&authority.target)
                        || !self.players.contains_key(&authority.viewer)
                        || !self.players.contains_key(&authority.target)
                        || intent
                            .bindings
                            .iter()
                            .any(|binding| !authority.allows(binding))
                })
            {
                return Err(RealmCompletionFailure::InvalidActionRequirements);
            }
        }
        if matches!(action.id.actor, ActorId::Peer(_) if action.id.incarnation != source.generation)
            || action.effects.iter().any(|effect| match effect {
                pumpkin_cluster::canonical_action::Effect::PlayerAvatarLifecycle {
                    intent, ..
                } => match intent {
                    pumpkin_cluster::avatar_lifecycle::AvatarLifecycleIntent::Admission {
                        owner,
                        ..
                    }
                    | pumpkin_cluster::avatar_lifecycle::AvatarLifecycleIntent::Retirement {
                        owner,
                        ..
                    } => *owner != source,
                },
                _ => false,
            })
        {
            return Err(RealmCompletionFailure::InvalidActionRequirements);
        }
        if action.effects.iter().any(|effect| match effect {
            pumpkin_cluster::canonical_action::Effect::ScheduleTick { target, tick } => {
                matches!(tick.origin, pumpkin_cluster::canonical_action::ScheduledTickOrigin::Action { producer, .. } if producer != source)
                    || match target {
                        pumpkin_cluster::canonical_action::Target::Chunk { key, .. } => self.scheduled_executor(*key).ok() != Some(tick.executor),
                        _ => true,
                    }
            }
            _ => false,
        })
        {
            return Err(RealmCompletionFailure::InvalidActionRequirements);
        }
        let primary_lifecycle =
            self.primary_producers.contains(&source) && primary_dropped_lifecycle(action, source);
        if primary_lifecycle
            && action
                .actor_chunk
                .is_none_or(|chunk| self.scheduled_executor(chunk).ok() != Some(source))
        {
            return Err(RealmCompletionFailure::InvalidActionRequirements);
        }
        if action.effects.iter().any(|effect| match effect {
            pumpkin_cluster::canonical_action::Effect::CompleteScheduledTick { fact, .. } => {
                fact.executor != source || !self.primary_producers.contains(&source)
            }
            _ => false,
        }) {
            return Err(RealmCompletionFailure::InvalidActionRequirements);
        }
        if action.effects.iter().any(|effect| {
            matches!(
                effect,
                pumpkin_cluster::canonical_action::Effect::ChangeDroppedItem { .. }
                    | pumpkin_cluster::canonical_action::Effect::MergeDroppedItems { .. }
            )
        }) && !primary_lifecycle
        {
            return Err(RealmCompletionFailure::InvalidActionRequirements);
        }
        if let ActorId::Entity(id) = action.id.actor {
            if entity_authority.is_none_or(|(chunk, owner)| {
                (owner != source && !primary_lifecycle)
                    || action.actor_chunk != Some(chunk)
                    || action.id.incarnation != u64::from(id.generation)
            }) {
                return Err(RealmCompletionFailure::InvalidActionRequirements);
            }
        }
        if !self.producers.contains(&source) {
            return Err(RealmCompletionFailure::SourceNotParticipant);
        }
        for effect in &action.effects {
            let (target, body) = match effect {
                pumpkin_cluster::canonical_action::Effect::Put {
                    target,
                    value: pumpkin_cluster::canonical_action::Value::Entity(body),
                    ..
                } => (target, body.as_ref()),
                pumpkin_cluster::canonical_action::Effect::EntityAdmission {
                    target, body, ..
                } => (target, body),
                _ => continue,
            };
            if body.birth.action
                == (pumpkin_cluster::canonical_action::ActionRef {
                    tick: action.tick,
                    id: action.id,
                })
                && (body.birth.source != source
                    || body.id.origin != source.server
                    || body.id.generation != source.generation
                    || body.id.local_id < 0
                    || matches!(
                        body.payload,
                        pumpkin_cluster::entity_body::EntityPayload::Item(_)
                            | pumpkin_cluster::entity_body::EntityPayload::Experience(_)
                    ) && match target {
                        pumpkin_cluster::canonical_action::Target::Chunk { key, .. } => {
                            self.scheduled_executor(*key).ok() != Some(body.owner)
                        }
                        _ => true,
                    }
                    || body.owner != source
                        && (!matches!(
                            body.payload,
                            pumpkin_cluster::entity_body::EntityPayload::Item(_)
                                | pumpkin_cluster::entity_body::EntityPayload::Experience(_)
                        ) || !self.primary_producers.contains(&body.owner)))
            {
                return Err(RealmCompletionFailure::InvalidActionRequirements);
            }
        }
        let chunks = action
            .stakeholders
            .iter()
            .filter_map(|stakeholder| match stakeholder {
                Stakeholder::Chunk(chunk) => Some(*chunk),
                Stakeholder::Player(_) => None,
            })
            .collect::<Vec<_>>();
        let mut captured = ledger.clone();
        captured.capture_action_issuer(action, source)?;
        let mut requirements = BTreeSet::new();
        for chunk in chunks {
            let holders = self.chunk_holders(chunk)?;
            let key =
                captured.capture_chunk(action.tick, chunk, holders, self.producers.clone())?;
            requirements.insert(key);
        }
        for lease in &action.player_leases {
            let owner = self.source(ServerId(lease.gid.server.0))?;
            captured.capture_player_lease(action.tick, *lease, owner)?;
            let Some(chunk) = action
                .player_chunks
                .iter()
                .find_map(|(key, chunk)| (*key == lease.key).then_some(*chunk))
            else {
                let targets = action
                    .targets()
                    .into_iter()
                    .filter_map(|target| target.player_avatar())
                    .filter(|(_, gid)| *gid == lease.gid)
                    .collect::<Vec<_>>();
                let association = self
                    .players
                    .get(lease)
                    .copied()
                    .or_else(|| self.avatar_bootstrap_chunk(action, lease));
                if targets.is_empty()
                    || targets.iter().any(|(chunk, _)| {
                        association != Some(*chunk)
                            && !association.is_some_and(|from| {
                                action.effects.iter().any(|effect| {
                                    match effect {
                            pumpkin_cluster::canonical_action::Effect::MovePlayerAvatar {
                                source,
                                destination,
                                ..
                            }
                            | pumpkin_cluster::canonical_action::Effect::RespawnPlayerAvatar {
                                source,
                                destination,
                                ..
                            } => {
                                source.player_avatar() == Some((from, lease.gid))
                                    && destination.player_avatar() == Some((*chunk, lease.gid))
                            }
                            _ => false,
                        }
                                })
                            })
                    })
                    || action.id.actor == ActorId::Player(lease.gid)
                        && (action.id.incarnation != lease.incarnation
                            || action.actor_chunk != association)
                {
                    return Err(RealmCompletionFailure::InvalidActionRequirements);
                }
                continue;
            };
            if self
                .players
                .get(lease)
                .copied()
                .or_else(|| self.avatar_bootstrap_chunk(action, lease))
                != Some(chunk)
            {
                return Err(RealmCompletionFailure::InvalidActionRequirements);
            }
            captured.capture_player_chunk(action.tick, lease.key, chunk)?;
            let mut holders = self.chunk_holders(chunk)?;
            holders.insert(owner);
            let key = RequirementKey {
                stakeholder: Stakeholder::Player(lease.key),
                domain: HolderDomain::LiveInventory {
                    actor_chunk: chunk,
                    owner: owner.server,
                    epoch: lease.incarnation,
                },
            };
            captured.capture_with_producers(action.tick, key, holders, self.producers.clone())?;
            requirements.insert(key);
        }
        captured.note_action(source, action, requirements.clone())?;
        *ledger = captured;
        Ok(requirements)
    }
}

const MAX_PENDING_RELIABLE_PER_PEER: usize = 1_200;

struct PendingCanonical {
    action: pumpkin_cluster::canonical_action::Action,
}

fn action_retention_chunks(
    action: &pumpkin_cluster::canonical_action::Action,
) -> impl Iterator<Item = ChunkKey> + '_ {
    action
        .stakeholders
        .iter()
        .filter_map(|stakeholder| match stakeholder {
            pumpkin_cluster::canonical_action::Stakeholder::Chunk(chunk) => Some(*chunk),
            _ => None,
        })
        .chain(action.player_chunks.iter().map(|(_, chunk)| *chunk))
        .chain(action.actor_chunk)
}

fn publish_primary_unsettled_chunks(
    server: &Server,
    blocks: &super::cluster_chunk_state::OwnedChunkStore,
    buckets: &mut WorldTickBuckets,
    pending: &HashMap<(TickStamp, pumpkin_cluster::canonical_action::ActionId), PendingCanonical>,
    graphs: &pumpkin_cluster::action_graph::ActionGraphs,
    retained: &mut HashMap<ChunkKey, Option<std::time::Instant>>,
) {
    if !server.is_cluster_primary() {
        return;
    }
    let mut active = buckets
        .pending_chunks()
        .into_iter()
        .map(|entry| entry.chunk)
        .collect::<HashSet<_>>();
    active.extend(buckets.pending_legacy_chunks());
    active.extend(
        pending
            .values()
            .flat_map(|entry| action_retention_chunks(&entry.action)),
    );
    active.extend(
        graphs
            .ticks
            .values()
            .flat_map(|graph| graph.actions.values())
            .flat_map(|node| action_retention_chunks(&node.action)),
    );
    active.extend(blocks.owned_chunks());
    active.retain(|chunk| blocks.contains(*chunk));
    let now = std::time::Instant::now();
    let worlds = server.worlds.load();
    for chunk in &active {
        let claimed = worlds.iter().any(|world| {
            world.level.cluster_world_key == chunk.world.key
                && world.level.cluster_dimension == chunk.world.dimension
                && world.level.claim_primary_chunk_for_actions(
                    pumpkin_util::math::vector2::Vector2::new(chunk.x(), chunk.z()),
                )
        });
        if !claimed {
            buckets.quarantine_chunk(*chunk);
        }
        retained.insert(*chunk, None);
    }
    let mut released = Vec::new();
    retained.retain(|chunk, release_after| {
        if active.contains(chunk) {
            return true;
        }
        let due = *release_after.get_or_insert(now + std::time::Duration::from_secs(5));
        if now < due {
            true
        } else {
            released.push(*chunk);
            false
        }
    });
    for world in worlds.iter() {
        for chunk in released.iter().filter(|chunk| {
            chunk.world.key == world.level.cluster_world_key
                && chunk.world.dimension == world.level.cluster_dimension
        }) {
            world.level.release_primary_chunk_action_claim(
                pumpkin_util::math::vector2::Vector2::new(chunk.x(), chunk.z()),
            );
        }
        if !world.level.cluster_genesis_required {
            continue;
        }
        let positions = retained
            .keys()
            .filter(|chunk| {
                chunk.world.key == world.level.cluster_world_key
                    && chunk.world.dimension == world.level.cluster_dimension
            })
            .map(|chunk| pumpkin_util::math::vector2::Vector2::new(chunk.x(), chunk.z()))
            .collect::<rustc_hash::FxHashSet<_>>();
        world
            .level
            .cluster_unsettled_primary_chunks
            .store(std::sync::Arc::new(positions));
    }
}

fn claim_primary_chunk_before_install(server: &Server, chunk: ChunkKey) -> bool {
    if !server.is_cluster_primary() {
        return true;
    }
    server.worlds.load().iter().any(|world| {
        if world.level.cluster_world_key != chunk.world.key
            || world.level.cluster_dimension != chunk.world.dimension
        {
            return false;
        }
        let position = pumpkin_util::math::vector2::Vector2::new(chunk.x(), chunk.z());
        world.level.install_primary_chunk_fence(position);
        world.level.claim_primary_chunk_for_actions(position)
    })
}

pub struct ActorDrainRequest {
    pub gid: GlobalPlayerId,
    pub lease: pumpkin_cluster::canonical_action::LivePlayerLease,
    pub install_generation: u64,
    pub through_tick: TickStamp,
    pub logout: bool,
    pub respond: oneshot::Sender<ActorDrainStatus>,
}

pub enum ActionSubmitError {
    Unavailable(ActionInput),
    Full(ActionInput),
}

pub fn submit_action(server: &Server, action: ActionInput) -> Result<(), ActionSubmitError> {
    let Some(route) = server.cluster_action_route.load_full() else {
        return Err(ActionSubmitError::Unavailable(action));
    };
    route.try_send(action).map_err(|error| match error {
        mpsc::error::TrySendError::Full(action) => ActionSubmitError::Full(action),
        mpsc::error::TrySendError::Closed(action) => ActionSubmitError::Unavailable(action),
    })
}

pub(crate) fn player_action_lease(
    server: &Server,
    player: &crate::entity::player::Player,
) -> Option<pumpkin_cluster::canonical_action::LivePlayerLease> {
    let gid = player.cluster_gid()?;
    let snapshot = server.cluster_actor_statistics.load();
    let incarnation = snapshot.get(&gid)?.incarnation;
    Some(pumpkin_cluster::canonical_action::LivePlayerLease {
        key: pumpkin_cluster::canonical_action::PlayerKey(player.gameprofile.id),
        gid,
        incarnation,
    })
}

fn prepare_frozen_player_action<S: pumpkin_cluster::canonical_action::ActionState>(
    scope: &OwnerTickScope,
    action: &pumpkin_cluster::canonical_action::Action,
    frozen: &S,
) -> Result<
    pumpkin_cluster::canonical_action::AppliedAction,
    pumpkin_cluster::canonical_action::ApplyFailure,
> {
    use pumpkin_cluster::canonical_action::{ApplyFailure, prepare_action};
    let pumpkin_cluster::actor::ActorId::Player(gid) = action.id.actor else {
        return Err(ApplyFailure::InvalidAction);
    };
    if gid.server != scope.local
        || !action.player_leases.iter().any(|lease| {
            lease.gid == gid
                && lease.incarnation == action.id.incarnation
                && scope.players.get(lease).copied() == action.actor_chunk
        })
    {
        return Err(ApplyFailure::InvalidAction);
    }
    scope
        .capture_action_with_entity_authority(
            &mut RealmCompletionLedger::default(),
            scope.local,
            action,
            None,
        )
        .map_err(|_| ApplyFailure::InvalidAction)?;
    prepare_action(frozen, action, TruthView::Local)
}

fn prepare_frozen_entity_action<S: pumpkin_cluster::canonical_action::ActionState>(
    scope: &OwnerTickScope,
    action: &pumpkin_cluster::canonical_action::Action,
    frozen: &S,
) -> Result<
    pumpkin_cluster::canonical_action::AppliedAction,
    pumpkin_cluster::canonical_action::ApplyFailure,
> {
    use pumpkin_cluster::canonical_action::{ApplyFailure, ChunkPart, Value, prepare_action};
    let pumpkin_cluster::actor::ActorId::Entity(id) = action.id.actor else {
        return Err(ApplyFailure::InvalidAction);
    };
    let source = scope
        .source(scope.local)
        .map_err(|_| ApplyFailure::InvalidAction)?;
    let (target, birth, owner) = action
        .captured_entity_source()
        .ok_or(ApplyFailure::InvalidAction)?;
    let Target::Chunk {
        key,
        part: ChunkPart::Entity(affected),
        inventory: None,
    } = target
    else {
        return Err(ApplyFailure::InvalidAction);
    };
    if affected != id
        || action.id.incarnation != u64::from(id.generation)
        || owner != source
        || action.actor_chunk != Some(key)
    {
        return Err(ApplyFailure::InvalidAction);
    }
    let Some(pumpkin_cluster::canonical_action::Cell {
        value: Value::Entity(body),
        ..
    }) = frozen.read(TruthView::Local, target)
    else {
        return Err(ApplyFailure::Condition(target));
    };
    if body.id != id || body.birth != *birth || body.owner != source {
        return Err(ApplyFailure::Condition(target));
    }
    scope
        .capture_action_with_entity_authority(
            &mut RealmCompletionLedger::default(),
            scope.local,
            action,
            Some((key, source)),
        )
        .map_err(|_| ApplyFailure::InvalidAction)?;
    prepare_action(frozen, action, TruthView::Local)
}

fn prepare_frozen_environment_action<S: pumpkin_cluster::canonical_action::ActionState>(
    scope: &OwnerTickScope,
    action: &pumpkin_cluster::canonical_action::Action,
    frozen: &S,
) -> Result<
    pumpkin_cluster::canonical_action::AppliedAction,
    pumpkin_cluster::canonical_action::ApplyFailure,
> {
    use pumpkin_cluster::canonical_action::{ApplyFailure, prepare_action};
    let source = scope
        .source(scope.local)
        .map_err(|_| ApplyFailure::InvalidAction)?;
    if action.id.actor != pumpkin_cluster::actor::ActorId::Peer(scope.local)
        || action.id.incarnation != source.generation
        || !action.player_leases.is_empty()
        || !scope.primary_producers.contains(&source)
    {
        return Err(ApplyFailure::InvalidAction);
    }
    scope
        .capture_action_with_entity_authority(
            &mut RealmCompletionLedger::default(),
            scope.local,
            action,
            None,
        )
        .map_err(|_| ApplyFailure::InvalidAction)?;
    prepare_action(frozen, action, TruthView::Local)
}

fn validate_captured_source_resources<'a>(
    works: impl IntoIterator<Item = &'a super::cluster_action_work::FrozenProposalWork>,
) -> Result<(), pumpkin_cluster::canonical_action::ApplyFailure> {
    use pumpkin_cluster::canonical_action::{ApplyFailure, check_logical_budget};
    let mut snapshots = BTreeSet::new();
    let mut records = 0usize;
    let mut bytes = 0usize;
    for work in works {
        let (owned_records, owned_bytes) = work.captured_resource_usage()?;
        records = records
            .checked_add(owned_records)
            .ok_or(ApplyFailure::ResourceLimit)?;
        bytes = bytes
            .checked_add(owned_bytes)
            .ok_or(ApplyFailure::ResourceLimit)?;
        let snapshot = work.original_snapshot();
        if snapshots.insert(Arc::as_ptr(&snapshot) as usize) {
            let (snapshot_records, snapshot_bytes) = snapshot.resource_usage()?;
            records = records
                .checked_add(snapshot_records)
                .ok_or(ApplyFailure::ResourceLimit)?;
            bytes = bytes
                .checked_add(snapshot_bytes)
                .ok_or(ApplyFailure::ResourceLimit)?;
        }
        check_logical_budget(records, bytes)?;
    }
    Ok(())
}

fn prepare_canonical_action(
    server: &Server,
    source: Option<PeerGeneration>,
    source_is_primary: bool,
    graphs: &pumpkin_cluster::action_graph::ActionGraphs,
    blocks: &mut super::cluster_chunk_state::OwnedChunkStore,
    actors: &mut super::cluster_world_actor::OwnedActors,
    statistics: &super::cluster_actor_statistics::OwnedActorStatistics,
    living: &mut super::cluster_world_actor::OwnedLivingActors,
    inventory: &mut super::cluster_world_inventory::InventoryOwner,
    versions: &mut BTreeMap<(TruthView, Target), u64>,
    private_draft: Option<&pumpkin_cluster::actor_action_scope::ActorActionDraft>,
    input: ActionInput,
) -> Result<
    Option<pumpkin_cluster::canonical_action::AppliedAction>,
    pumpkin_cluster::canonical_action::ApplyFailure,
> {
    let ActionInput::Complete(action) = input else {
        return Err(pumpkin_cluster::canonical_action::ApplyFailure::InvalidAction);
    };
    let Some(source) = source else {
        return Err(pumpkin_cluster::canonical_action::ApplyFailure::InvalidAction);
    };
    let primary_lifecycle = source_is_primary && primary_dropped_lifecycle(&action, source);
    let bootstrap = action.effects.iter().find_map(|effect| match effect {
        pumpkin_cluster::canonical_action::Effect::PlayerAvatarLifecycle {
            target,
            intent:
                pumpkin_cluster::avatar_lifecycle::AvatarLifecycleIntent::Admission {
                    lease,
                    install_generation,
                    owner,
                    inventory: root,
                    ..
                },
        } if *owner == source
            && inventory.prepared_player_admission(*lease).as_ref() == Some(root)
            && inventory.player_install_generation(lease.gid) == Some(*install_generation)
            && action.id.actor == pumpkin_cluster::actor::ActorId::Player(lease.gid)
            && action.id.incarnation == lease.incarnation =>
        {
            Some((*target, *lease))
        }
        _ => None,
    });
    if action.effects.iter().any(|effect| match effect {
        pumpkin_cluster::canonical_action::Effect::CompleteScheduledTick { fact, .. } => {
            fact.executor != source || !source_is_primary
        }
        _ => false,
    }) {
        return Err(pumpkin_cluster::canonical_action::ApplyFailure::InvalidAction);
    }
    if action.effects.iter().any(|effect| matches!(effect,
        pumpkin_cluster::canonical_action::Effect::ScheduleTick { tick, .. }
            if matches!(tick.origin, pumpkin_cluster::canonical_action::ScheduledTickOrigin::Action { producer, .. } if producer != source)))
    {
        return Err(pumpkin_cluster::canonical_action::ApplyFailure::InvalidAction);
    }
    if action.effects.iter().any(|effect| {
        matches!(
            effect,
            pumpkin_cluster::canonical_action::Effect::ChangeDroppedItem { .. }
                | pumpkin_cluster::canonical_action::Effect::MergeDroppedItems { .. }
        )
    }) && !primary_lifecycle
    {
        return Err(pumpkin_cluster::canonical_action::ApplyFailure::InvalidAction);
    }
    for lease in &action.player_leases {
        let inventory_leg = action.targets().into_iter().any(|target| {
            matches!(target, Target::Player { key, inventory: Some(_), .. } if key == lease.key)
        });
        let avatar_leg = action.targets().into_iter().any(|target| {
            target
                .player_avatar()
                .is_some_and(|(_, gid)| gid == lease.gid)
        });
        if !(inventory.live_player_lease_matches(*lease)
            || bootstrap.is_some_and(|(_, admitted)| admitted == *lease)
            || avatar_leg && !inventory_leg && actors.remote_lease(lease.gid) == Some(*lease))
            || !avatar_leg && !inventory_leg
            || inventory_leg
                && action
                    .player_chunks
                    .iter()
                    .find(|(key, _)| *key == lease.key)
                    .is_none()
            || avatar_leg
                && !action
                    .targets()
                    .into_iter()
                    .filter(|target| {
                        target
                            .player_avatar()
                            .is_some_and(|(_, gid)| gid == lease.gid)
                    })
                    .all(|target| {
                        let body = pumpkin_cluster::derived_living::actor_body_target(target)
                            .unwrap_or(target);
                        graphs.player_avatar_admissions.get(&body) == Some(lease)
                            || bootstrap == Some((body, *lease))
                    })
            || action
                .targets()
                .into_iter()
                .chain(action.counterfactual_conditions.iter().copied())
                .chain(action.counterfactual_effects.iter().copied())
                .filter_map(|target| match target {
                    Target::Player {
                        key,
                        lease: Some(target_lease),
                        inventory: Some(id),
                    } if key == lease.key && target_lease == *lease => Some(id.action_realm()),
                    _ => None,
                })
                .any(|realm| {
                    graphs
                        .player_inventory_frontiers
                        .get(&(lease.key, realm, lease.gid, lease.incarnation))
                        .is_none_or(|frontier| {
                            if bootstrap.is_some_and(|(_, admitted)| admitted == *lease) {
                                return false;
                            }
                            action
                                .player_chunks
                                .iter()
                                .find(|(key, _)| *key == lease.key)
                                .is_none_or(|(_, chunk)| *chunk != frontier.accepted_chunk)
                                || !action.tick.is_after(frontier.settled_through)
                        })
                })
        {
            return Err(pumpkin_cluster::canonical_action::ApplyFailure::InvalidAction);
        }
    }
    let local = ServerId(server.advanced_config.cluster.server_id);
    let source_is_owner = match action.id.actor {
        pumpkin_cluster::actor::ActorId::Player(gid) => {
            gid.server == local
                && statistics.incarnation(gid) == Some(action.id.incarnation)
                && (actors.local_key(gid).is_some()
                    || bootstrap.is_some_and(|(_, lease)| lease.gid == gid))
        }
        pumpkin_cluster::actor::ActorId::Entity(_) => blocks
            .historical_entity_authority(&action)
            .or_else(|| entity_authority_at_action_tick(graphs, &action))
            .is_some_and(|(chunk, owner)| {
                action.actor_chunk == Some(chunk) && (owner == source || primary_lifecycle)
            }),
        pumpkin_cluster::actor::ActorId::Peer(peer) => peer == local,
    };
    if source.server != local || !source_is_owner
        || action.stakeholders.iter().any(|stakeholder| {
            matches!(stakeholder, pumpkin_cluster::canonical_action::Stakeholder::Chunk(chunk) if !blocks.contains(*chunk))
        })
    {
        return Err(pumpkin_cluster::canonical_action::ApplyFailure::InvalidAction);
    }
    if (bootstrap.is_some() || action.effects.iter().any(|effect| {
        matches!(
            effect,
            pumpkin_cluster::canonical_action::Effect::EntityAdmission { .. }
                | pumpkin_cluster::canonical_action::Effect::MenuVirtualInventoryAdmission { .. }
        )
    })) && !inventory.record_canonical_admission(&action, source)
    {
        return Err(pumpkin_cluster::canonical_action::ApplyFailure::InvalidAction);
    }
    let owned = super::cluster_world_canonical::OwnedWorldActionState {
        server,
        blocks,
        actors,
        living,
        inventory,
        versions,
    };
    let applied = match private_draft {
        Some(draft) => pumpkin_cluster::canonical_action::prepare_action(
            &draft.view(&owned),
            &action,
            TruthView::Local,
        ),
        None => {
            pumpkin_cluster::canonical_action::prepare_action(&owned, &action, TruthView::Local)
        }
    };
    match applied {
        Ok(applied) => Ok(Some(applied)),
        Err(
            pumpkin_cluster::canonical_action::ApplyFailure::Condition(_)
            | pumpkin_cluster::canonical_action::ApplyFailure::Inventory(_)
            | pumpkin_cluster::canonical_action::ApplyFailure::InventoryConservation
            | pumpkin_cluster::canonical_action::ApplyFailure::Type(_)
            | pumpkin_cluster::canonical_action::ApplyFailure::Overflow(_),
        ) => Ok(None),
        Err(reason) => Err(reason),
    }
}

#[derive(Clone, Copy)]
pub(crate) struct PlayerAdministrationMaterialization {
    pub lease: pumpkin_cluster::canonical_action::LivePlayerLease,
    pub association: ChunkKey,
    pub effective: pumpkin_cluster::controller::ControllerFrame,
    pub epoch: pumpkin_cluster::canonical_action::SpatialEpoch,
    pub id: pumpkin_cluster::canonical_action::ActionId,
}

#[derive(Clone)]
pub(crate) struct FrozenPlayerCommandMaterialization {
    pub detach: Option<Box<pumpkin_cluster::actor_relation::PlayerDetachPlan>>,
    pub actor: PlayerAdministrationMaterialization,
    pub expected: pumpkin_cluster::player_admin::PlayerAdministrativeState,
    pub contact: Option<pumpkin_cluster::item_contact::ContactObservation>,
}

pub(crate) fn freeze_player_command(
    server: &Server,
    actors: &super::cluster_world_actor::OwnedActors,
    living: &super::cluster_world_actor::OwnedLivingActors,
    inventory: &super::cluster_world_inventory::InventoryOwner,
    proposal: &pumpkin_cluster::canonical_action::ActionProposal,
    tick: TickStamp,
) -> Result<
    FrozenPlayerCommandMaterialization,
    super::cluster_player_commands::PlayerCommandSubmitError,
> {
    let (actor, expected, contact) =
        capture_player_command(server, actors, living, inventory, proposal, tick)?;
    Ok(FrozenPlayerCommandMaterialization {
        detach: None,
        actor,
        expected,
        contact,
    })
}

fn capture_player_command(
    server: &Server,
    actors: &super::cluster_world_actor::OwnedActors,
    living: &super::cluster_world_actor::OwnedLivingActors,
    inventory: &super::cluster_world_inventory::InventoryOwner,
    proposal: &pumpkin_cluster::canonical_action::ActionProposal,
    tick: TickStamp,
) -> Result<
    (
        PlayerAdministrationMaterialization,
        pumpkin_cluster::player_admin::PlayerAdministrativeState,
        Option<pumpkin_cluster::item_contact::ContactObservation>,
    ),
    super::cluster_player_commands::PlayerCommandSubmitError,
> {
    use super::cluster_player_commands::PlayerCommandSubmitError as Error;
    use pumpkin_cluster::player_command_control::{PlayerCommandMutation, TeleportDestination};
    let (origin, transport_epoch, request) =
        proposal.command.as_ref().ok_or(Error::InvalidInput)?;
    super::cluster_player_commands::authorize_authenticated(
        server,
        &super::cluster_player_commands::AuthenticatedPlayerCommand {
            origin: *origin,
            transport_epoch: *transport_epoch,
            request: request.clone(),
        },
    )?;
    let lease = proposal.lease.ok_or(Error::InvalidInput)?;
    let (target, target_uuid) = request.player_target().ok_or(Error::NoTarget)?;
    let departure_test = cfg!(feature = "cluster-e2e") && request.correlation[..8] == *b"PumpDAdm";
    if lease.gid != target
        || lease.key.0.as_bytes() != &target_uuid
        || !inventory.live_player_lease_matches(lease)
        || actors.local_key(lease.gid) != Some(lease.key.0)
        || server
            .get_all_players()
            .iter()
            .find(|player| player.cluster_gid() == Some(lease.gid))
            .is_none_or(|player| {
                !departure_test
                    && player
                        .cluster_departing
                        .load(std::sync::atomic::Ordering::Acquire)
            })
    {
        return Err(Error::NoTarget);
    }
    let actor = actors.get(lease.gid).ok_or(Error::NoTarget)?;
    let effective = actors.input_projection(lease.gid).ok_or(Error::NoTarget)?;
    let state = living
        .get(pumpkin_cluster::actor::ActorId::Player(lease.gid))
        .ok_or(Error::NoTarget)?;
    let epoch = state
        .local
        .avatar
        .as_ref()
        .ok_or(Error::NoTarget)?
        .spatial_epoch;
    let mut expected = state
        .local
        .player_administrative_state()
        .ok_or(Error::NoTarget)?;
    expected.world = actor.local.chunk.world;
    expected.position = effective.frame.effective.position;
    expected.velocity = effective.frame.effective.velocity;
    expected.yaw = effective.frame.effective.yaw;
    expected.pitch = effective.frame.effective.pitch;
    let destination = match &request.mutation {
        PlayerCommandMutation::Teleport(TeleportDestination::Player { gid, uuid }) => {
            Some((*gid, *uuid))
        }
        PlayerCommandMutation::Spectate {
            target: Some(target),
        } => Some((target.gid, target.uuid)),
        _ => None,
    };
    let contact = destination
        .map(|(gid, uuid)| {
            let lease = inventory
                .player_lease_for_gid(gid)
                .or_else(|| actors.remote_lease(gid))
                .ok_or(Error::NoRoute)?;
            if lease.key.0.as_bytes() != &uuid {
                return Err(Error::NoTarget);
            }
            let actor = actors.get(gid).ok_or(Error::NoRoute)?;
            let overlay = actors.input_projection(gid).ok_or(Error::NoRoute)?;
            let avatar = living
                .get(pumpkin_cluster::actor::ActorId::Player(gid))
                .and_then(|truth| truth.local.avatar.as_ref())
                .ok_or(Error::NoRoute)?;
            let generation = server
                .cluster_peer_incarnations
                .load()
                .get(&gid.server)
                .copied()
                .ok_or(Error::NoRoute)?;
            let observation = pumpkin_cluster::item_contact::ContactObservation {
                identity: pumpkin_cluster::item_contact::ContactIdentity::Player(lease),
                owner: PeerGeneration {
                    server: gid.server,
                    generation,
                },
                epoch: avatar.spatial_epoch,
                world: actor.local.chunk.world,
                chunk: actor.local.chunk,
                position: overlay.frame.effective.position.map(f64::to_bits),
                half_width: 0.3_f64.to_bits(),
                height: (if overlay.crawling || overlay.swimming || overlay.gliding {
                    0.6_f64
                } else if overlay.sneaking {
                    1.5_f64
                } else {
                    1.8_f64
                })
                .to_bits(),
                simulation_tick: tick,
                observed_sequence: overlay.frame.effective.source_seq,
            };
            observation
                .valid()
                .then_some(observation)
                .ok_or(Error::InvalidInput)
        })
        .transpose()?;
    Ok((
        PlayerAdministrationMaterialization {
            lease,
            association: actor.local.chunk,
            effective: effective.frame,
            epoch,
            id: proposal.id,
        },
        expected,
        contact,
    ))
}

pub(crate) fn materialize_player_command(
    view: &impl ActionState,
    mut metadata: PlayerAdministrationMaterialization,
    _expected: pumpkin_cluster::player_admin::PlayerAdministrativeState,
    contact: Option<pumpkin_cluster::item_contact::ContactObservation>,
    detach: Option<pumpkin_cluster::actor_relation::PlayerDetachPlan>,
    proposal: &pumpkin_cluster::canonical_action::ActionProposal,
    tick: TickStamp,
    ready: impl Fn(ChunkKey) -> bool,
) -> Result<
    Result<pumpkin_cluster::canonical_action::Action, ChunkKey>,
    pumpkin_cluster::canonical_action::ApplyFailure,
> {
    use pumpkin_cluster::canonical_action::{Action, ApplyFailure, ChunkPart, Value};
    use pumpkin_cluster::player_command_control::{PlayerCommandMutation, TeleportDestination};
    let request = &proposal
        .command
        .as_ref()
        .ok_or(ApplyFailure::InvalidAction)?
        .2;
    let target = Target::Chunk {
        key: metadata.association,
        part: ChunkPart::Player(metadata.lease.gid),
        inventory: None,
    };
    let before = view
        .read(TruthView::Local, target)
        .ok_or(ApplyFailure::InvalidAction)?;
    let Value::Living(current) = &before.value else {
        return Err(ApplyFailure::Type(target));
    };
    let avatar = current.avatar.as_ref().ok_or(ApplyFailure::Type(target))?;
    let mut expected = current
        .player_administrative_state()
        .ok_or(ApplyFailure::Type(target))?;
    if avatar.spatial_epoch == metadata.epoch {
        expected.world = metadata.association.world;
        expected.position = metadata.effective.effective.position;
        expected.velocity = metadata.effective.effective.velocity;
        expected.yaw = metadata.effective.effective.yaw;
        expected.pitch = metadata.effective.effective.pitch;
    } else {
        metadata.epoch = avatar.spatial_epoch;
        metadata.effective.effective.position = expected.position;
        metadata.effective.effective.velocity = expected.velocity;
        metadata.effective.effective.yaw = expected.yaw;
        metadata.effective.effective.pitch = expected.pitch;
    }
    match &request.mutation {
        PlayerCommandMutation::GameMode { mode, previous } => {
            Ok(Ok(super::cluster_gamemode_commands::materialize(
                view, metadata, tick, *mode, *previous, detach,
            )?))
        }
        PlayerCommandMutation::Attribute(mutation) => {
            Ok(Ok(super::cluster_attribute_commands::materialize(
                view,
                metadata.id,
                tick,
                target,
                Some(metadata.lease),
                metadata.epoch,
                None,
                mutation,
            )?))
        }
        PlayerCommandMutation::Summon(_) => Err(ApplyFailure::InvalidAction),
        PlayerCommandMutation::Kill => {
            let intent = pumpkin_cluster::combat_action::CombatIntent {
                damages: vec![pumpkin_cluster::combat_action::DamageIntent {
                    kind: pumpkin_cluster::combat_action::DamageKind::OutOfWorld,
                    source: metadata.id.actor,
                    target,
                    tick,
                    damage_milli: u32::MAX,
                    knockback_micro: [0; 3],
                    bypass_hurt_cooldown: true,
                    fire_ticks: 0,
                    reduction: None,
                    bypass_invulnerability: true,
                }],
                ..Default::default()
            };
            let source_epoch = avatar.spatial_epoch;
            let values = BTreeMap::from([(target, before)]);
            let guards = vec![pumpkin_cluster::canonical_action::Condition {
                target,
                expected: pumpkin_cluster::canonical_action::Expected::SpatialEpochEquals(
                    source_epoch,
                ),
            }];
            Ok(Ok(intent.capture_with(
                view,
                metadata.id,
                metadata.association,
                &values,
                guards,
            )?))
        }
        PlayerCommandMutation::Recipes { ids, granted } => {
            use pumpkin_cluster::living_property::{LivingProperty, LivingPropertyValue};
            if ids.iter().any(|id| !view.recipe_known(id)) {
                return Err(ApplyFailure::InvalidAction);
            }
            let facet = Target::Chunk {
                key: metadata.association,
                part: ChunkPart::LivingProperty {
                    actor: metadata.id.actor,
                    property: LivingProperty::Advancements,
                },
                inventory: None,
            };
            let before = view
                .read(TruthView::Local, facet)
                .ok_or(ApplyFailure::InvalidAction)?;
            let Value::LivingProperty(LivingPropertyValue::Advancements(mut state)) =
                before.value.clone()
            else {
                return Err(ApplyFailure::Type(facet));
            };
            for id in ids {
                if *granted {
                    state.unlocked_recipes.insert(id.clone());
                } else {
                    state.unlocked_recipes.remove(id);
                }
            }
            if !state.valid() {
                return Err(ApplyFailure::InvalidAction);
            }
            Ok(Ok(player_command_facet_action(
                metadata,
                tick,
                facet,
                before,
                Value::LivingProperty(LivingPropertyValue::Advancements(state)),
            )?))
        }
        PlayerCommandMutation::Advancements {
            ids,
            criterion,
            granted,
            achieved_millis,
        } => {
            use pumpkin_cluster::living_property::{LivingProperty, LivingPropertyValue};
            let facet = Target::Chunk {
                key: metadata.association,
                part: ChunkPart::LivingProperty {
                    actor: metadata.id.actor,
                    property: LivingProperty::Advancements,
                },
                inventory: None,
            };
            let before = view
                .read(TruthView::Local, facet)
                .ok_or(ApplyFailure::InvalidAction)?;
            let Value::LivingProperty(LivingPropertyValue::Advancements(mut state)) =
                before.value.clone()
            else {
                return Err(ApplyFailure::Type(facet));
            };
            let mut experience = 0u32;
            for id in ids {
                let definition = super::cluster_advancements::definition(id)
                    .ok_or(ApplyFailure::InvalidAction)?;
                let was_complete = state
                    .progress
                    .get(&definition.id)
                    .is_some_and(|progress| progress.completed);
                state = pumpkin_cluster::player_advancement::change(
                    &state,
                    &definition,
                    criterion.as_deref(),
                    *granted,
                    *achieved_millis,
                )
                .ok_or(ApplyFailure::InvalidAction)?;
                if !was_complete
                    && state
                        .progress
                        .get(&definition.id)
                        .is_some_and(|progress| progress.completed)
                {
                    let advancement = pumpkin_data::Advancement::from_minecraft_name(id)
                        .or_else(|| pumpkin_data::Advancement::from_name(id))
                        .ok_or(ApplyFailure::InvalidAction)?;
                    experience = experience
                        .checked_add(
                            u32::try_from(advancement.reward.experience)
                                .map_err(|_| ApplyFailure::InvalidAction)?,
                        )
                        .ok_or(ApplyFailure::Overflow(facet))?;
                }
            }
            let mut action = player_command_facet_action(
                metadata,
                tick,
                facet,
                before,
                Value::LivingProperty(LivingPropertyValue::Advancements(state)),
            )?;
            if experience != 0 {
                let target = Target::Chunk {
                    key: metadata.association,
                    part: ChunkPart::LivingProperty {
                        actor: metadata.id.actor,
                        property: LivingProperty::Experience,
                    },
                    inventory: None,
                };
                let before = view
                    .read(TruthView::Local, target)
                    .ok_or(ApplyFailure::InvalidAction)?;
                let Value::LivingProperty(LivingPropertyValue::Experience(previous)) =
                    &before.value
                else {
                    return Err(ApplyFailure::Type(target));
                };
                let next = previous
                    .collect(experience)
                    .ok_or(ApplyFailure::Overflow(target))?;
                action
                    .conditions
                    .push(pumpkin_cluster::canonical_action::Condition {
                        target,
                        expected: pumpkin_cluster::canonical_action::Expected::Exact(before),
                    });
                action
                    .effects
                    .push(pumpkin_cluster::canonical_action::Effect::Put {
                        target,
                        value: Value::LivingProperty(LivingPropertyValue::Experience(next)),
                    });
            }
            action.validate()?;
            Ok(Ok(action))
        }
        PlayerCommandMutation::Tags { tag, present } => {
            pumpkin_cluster::actor_tags::ActorTagsIntent {
                actor: metadata.id.actor,
                chunk: metadata.association,
                tag: tag.clone(),
                present: *present,
            }
            .capture(
                view,
                pumpkin_cluster::canonical_inventory::InventoryActionCapture {
                    id: metadata.id,
                    tick,
                    edit_authority: None,
                    actor_chunk: Some(metadata.association),
                    player_leases: vec![metadata.lease],
                    player_chunks: Vec::new(),
                    guards: Vec::new(),
                    client_origin: proposal.client_origin,
                },
            )
            .map(Ok)
        }
        PlayerCommandMutation::Teleport(destination) => {
            let (world, position, rotation) = match destination {
                TeleportDestination::Absolute {
                    world,
                    position,
                    rotation,
                } => (*world, *position, *rotation),
                TeleportDestination::Player { .. } => {
                    let contact = contact.as_ref().ok_or(ApplyFailure::InvalidAction)?;
                    (contact.world, contact.position.map(f64::from_bits), None)
                }
            };
            let rotation = rotation.unwrap_or([expected.yaw, expected.pitch]);
            let mutation = pumpkin_cluster::player_admin::PlayerAdministrativeMutation::Teleport {
                world,
                position,
                yaw: rotation[0],
                pitch: rotation[1],
            };
            let mut forward = expected.clone();
            mutation.apply(&mut forward);
            let destination = ChunkKey::new(
                world,
                (position[0].floor() as i32).div_euclid(16),
                (position[2].floor() as i32).div_euclid(16),
            );
            let mut affected = vec![metadata.association, destination];
            affected.sort_unstable();
            affected.dedup();
            let update = pumpkin_cluster::player_admin::PlayerAdministrativeAction {
                target: metadata.lease.gid,
                seq: pumpkin_cluster::identity::PlayerSeq(metadata.id.sequence),
                tick,
                affected,
                forward,
                undo: expected.clone(),
                expected,
                mutation,
                origin: None,
            };
            let mut result =
                materialize_player_administration(view, metadata, proposal, &update, tick, ready)?;
            if let Ok(action) = &mut result {
                if let Some(contact) = contact {
                    action
                        .conditions
                        .push(pumpkin_cluster::canonical_action::Condition {
                            target: contact.target(),
                            expected:
                                pumpkin_cluster::canonical_action::Expected::SpatialEpochEquals(
                                    contact.epoch,
                                ),
                        });
                    if let pumpkin_cluster::item_contact::ContactIdentity::Player(lease) =
                        contact.identity
                    {
                        if !action.player_leases.contains(&lease) {
                            action.player_leases.push(lease);
                        }
                    }
                    action.validate()?;
                }
            }
            Ok(result)
        }
        PlayerCommandMutation::Reset(reset) => {
            let mutation = pumpkin_cluster::player_admin::PlayerAdministrativeMutation::Reset {
                health: reset.health,
                absorption: reset.absorption,
                food: reset.food,
                saturation: reset.saturation,
                gamemode: reset.gamemode,
                clear_effects: reset.clear_effects,
                velocity: reset.velocity,
                fall_distance: reset.fall_distance,
                on_fire: reset.on_fire,
            };
            let mut forward = expected.clone();
            mutation.apply(&mut forward);
            let update = pumpkin_cluster::player_admin::PlayerAdministrativeAction {
                target: metadata.lease.gid,
                seq: pumpkin_cluster::identity::PlayerSeq(metadata.id.sequence),
                tick,
                affected: vec![metadata.association],
                forward,
                undo: expected.clone(),
                expected,
                mutation,
                origin: None,
            };
            materialize_player_administration(view, metadata, proposal, &update, tick, ready)
        }
        PlayerCommandMutation::Spectate { .. } => {
            if let Some(contact) = &contact {
                if !ready(contact.chunk) {
                    return Ok(Err(contact.chunk));
                }
            }
            Action::from_player_presentation(
                metadata.id,
                metadata.lease,
                tick,
                metadata.association,
                before,
                pumpkin_cluster::item_action::PlayerPresentationChange::Camera(contact),
                Vec::new(),
                Vec::new(),
            )
            .map(Ok)
        }
    }
}

fn player_command_facet_action(
    metadata: PlayerAdministrationMaterialization,
    tick: TickStamp,
    target: Target,
    before: pumpkin_cluster::canonical_action::Cell,
    after: pumpkin_cluster::canonical_action::Value,
) -> Result<
    pumpkin_cluster::canonical_action::Action,
    pumpkin_cluster::canonical_action::ApplyFailure,
> {
    use pumpkin_cluster::canonical_action::*;
    let dependencies = match metadata.epoch {
        SpatialEpoch::Action(reference) => vec![reference],
        _ => Vec::new(),
    };
    let action = Action {
        id: metadata.id,
        tick,
        class: ActionClass::Undoable,
        chunk_transition: false,
        edit_authority: None,
        actor_chunk: Some(metadata.association),
        player_leases: vec![metadata.lease],
        player_chunks: Vec::new(),
        stakeholders: vec![Stakeholder::Chunk(metadata.association)],
        dependencies,
        conditions: vec![Condition {
            target,
            expected: Expected::Exact(before),
        }],
        effects: vec![Effect::Put {
            target,
            value: after,
        }],
        counterfactual_conditions: Vec::new(),
        counterfactual_effects: Vec::new(),
        semantic_intent: None,
        factual_result: None,
        undo: vec![UndoProcedure::RestoreWrittenTargets],
        kind: ActionKind::PlayerAdministration,
        presentation: Vec::new(),
        client_origin: ClientOrigin::None,
    };
    action.validate()?;
    Ok(action)
}

fn reject_registered_player_command(
    server: &Server,
    pending: &mut VecDeque<super::cluster_player_commands::CommandObservation>,
    origin: Option<(PeerGeneration, [u8; 16])>,
) {
    if let Some((origin, correlation)) = origin {
        if let Err(observation) = server.cluster_player_commands.record_input_failure(
            origin,
            correlation,
            super::cluster_player_commands::PlayerCommandSubmitError::InvalidInput,
        ) {
            pending.push_back(observation);
        }
    }
}

pub(crate) fn materialize_player_administration(
    view: &impl ActionState,
    metadata: PlayerAdministrationMaterialization,
    proposal: &pumpkin_cluster::canonical_action::ActionProposal,
    update: &pumpkin_cluster::player_admin::PlayerAdministrativeAction,
    tick: TickStamp,
    destination_ready: impl Fn(ChunkKey) -> bool,
) -> Result<
    Result<pumpkin_cluster::canonical_action::Action, ChunkKey>,
    pumpkin_cluster::canonical_action::ApplyFailure,
> {
    use pumpkin_cluster::canonical_action::{Action, ApplyFailure, ChunkPart, Value};

    let lease = metadata.lease;
    if lease.gid != update.target
        || update.tick != tick
        || proposal.lease != Some(lease)
        || metadata.id.actor != pumpkin_cluster::actor::ActorId::Player(update.target)
        || metadata.id.incarnation != lease.incarnation
        || metadata.id.sequence != update.seq.0
        || metadata.id.family != 7
    {
        return Err(ApplyFailure::InvalidAction);
    }
    let old_chunk = metadata.association;
    let old_target = Target::Chunk {
        key: old_chunk,
        part: ChunkPart::Player(update.target),
        inventory: None,
    };
    let before = view
        .read(TruthView::Local, old_target)
        .ok_or(ApplyFailure::InvalidAction)?;
    let Value::Living(current) = &before.value else {
        return Err(ApplyFailure::Type(old_target));
    };
    let mut target = current
        .player_administrative_state()
        .ok_or(ApplyFailure::Type(old_target))?;
    if current.avatar.as_ref().map(|avatar| avatar.spatial_epoch) != Some(metadata.epoch) {
        return Err(ApplyFailure::Condition(old_target));
    }
    target.world = old_chunk.world;
    target.position = metadata.effective.effective.position;
    target.velocity = metadata.effective.effective.velocity;
    target.yaw = metadata.effective.effective.yaw;
    target.pitch = metadata.effective.effective.pitch;
    if target != update.expected || update.undo != update.expected {
        return Err(ApplyFailure::Condition(old_target));
    }
    update.mutation.apply(&mut target);
    if target != update.forward {
        return Err(ApplyFailure::InvalidAction);
    }
    let mut after = current
        .with_player_administrative_state(&target)
        .ok_or(ApplyFailure::InvalidAction)?;
    after.combat.status = crate::entity::combat::normalized_status(
        target.effects.iter(),
        current.combat.status.invulnerable,
    );
    let new_chunk = ChunkKey::new(
        target.world,
        (target.position[0].floor() as i32).div_euclid(16),
        (target.position[2].floor() as i32).div_euclid(16),
    );
    let mut affected = vec![old_chunk, new_chunk];
    affected.sort_unstable();
    affected.dedup();
    if update.affected != affected {
        return Err(ApplyFailure::InvalidAction);
    }
    if !destination_ready(new_chunk) {
        return Ok(Err(new_chunk));
    }
    let mut action = Action::from_player_administration(
        lease, update.seq, tick, old_chunk, new_chunk, before, after,
    )
    .ok_or(ApplyFailure::InvalidAction)?;
    action.player_leases = vec![lease];
    action
        .conditions
        .push(pumpkin_cluster::canonical_action::Condition {
            target: old_target,
            expected: pumpkin_cluster::canonical_action::Expected::SpatialEpochEquals(
                metadata.epoch,
            ),
        });
    action.validate()?;
    Ok(Ok(action))
}

fn queue_player_administration_projection(
    server: &Server,
    pending: &mut VecDeque<super::cluster_player_admin::PlayerAdministrativeResolution>,
    action: ActionKey,
    update: pumpkin_cluster::player_admin::PlayerAdministrativeAction,
    accepted: bool,
) {
    if accepted && action.family == (7 | 0x80) {
        return;
    }
    let resolution = super::cluster_player_admin::PlayerAdministrativeResolution {
        action,
        update,
        accepted,
        holders: Vec::new(),
    };
    if !pending.is_empty() {
        pending.push_back(resolution);
    } else if let Err(super::cluster_player_admin::PlayerAdministrativeQueueError::Full(
        resolution,
    )) = server.cluster_player_admin.queue_resolution(
        resolution.action,
        resolution.update.clone(),
        resolution.accepted,
        resolution.holders.clone(),
    ) {
        pending.push_back(resolution);
    }
    assert!(
        pending.len() <= 4_096,
        "cluster player administration projection capacity exhausted"
    );
}

fn publish_canonical_changes(
    server: &Server,
    blocks: &super::cluster_chunk_state::OwnedChunkStore,
    writes: &[pumpkin_cluster::canonical_action::AppliedWrite],
) {
    let changed = writes
        .iter()
        .filter_map(|write| match write.target {
            Target::Chunk { key, .. } => Some(key),
            Target::Player { .. } => None,
        })
        .collect::<BTreeSet<_>>();
    let snapshots = changed
        .into_iter()
        .filter_map(|chunk| blocks.snapshot(chunk).map(|snapshot| (chunk, snapshot)))
        .collect::<Vec<_>>();
    if !snapshots.is_empty() {
        super::cluster_chunk_presentation::refresh(
            server,
            blocks,
            snapshots.iter().map(|(chunk, _)| *chunk),
        );
        server.cluster_owned_chunk_blocks.rcu(|current| {
            let mut next = (**current).clone();
            next.extend(snapshots.iter().cloned());
            Arc::new(next)
        });
    }
    for write in writes {
        let Target::Chunk {
            key,
            part: pumpkin_cluster::canonical_action::ChunkPart::Block(position),
            inventory: None,
        } = write.target
        else {
            continue;
        };
        let Some(pumpkin_cluster::canonical_action::Cell {
            value: pumpkin_cluster::canonical_action::Value::Block(state),
            ..
        }) = &write.after
        else {
            continue;
        };
        let Some(world) = world_for(server, key.world) else {
            continue;
        };
        let Some(state_id) = pumpkin_data::BlockStateId::new(*state) else {
            continue;
        };
        let position =
            pumpkin_util::math::position::BlockPos::new(position.x, position.y, position.z);
        world.broadcast_to_chunk_editioned(
            pumpkin_util::math::vector2::Vector2::new(key.x(), key.z()),
            &pumpkin_protocol::java::client::play::CBlockUpdate::new(
                position,
                i32::from(*state).into(),
            ),
            &pumpkin_protocol::bedrock::client::CUpdateBlock::new(
                position,
                pumpkin_data::BlockState::to_be_network_id(state_id) as u32,
            ),
        );
    }
    publish_canonical_block_entity_changes(server, blocks, false, writes);
}

fn publish_canonical_block_entity_changes(
    server: &Server,
    blocks: &super::cluster_chunk_state::OwnedChunkStore,
    ground: bool,
    writes: &[pumpkin_cluster::canonical_action::AppliedWrite],
) {
    use pumpkin_cluster::canonical_action::{ChunkPart, Value};
    for write in writes {
        let Target::Chunk {
            key,
            part: ChunkPart::BlockEntity(position),
            inventory: None,
        } = write.target
        else {
            continue;
        };
        let Some(pumpkin_cluster::canonical_action::Cell {
            value: Value::BlockEntity(bytes),
            ..
        }) = &write.after
        else {
            continue;
        };
        let Some(state) = (if ground {
            blocks.ground_block(key, position)
        } else {
            blocks.block(key, position)
        })
        .ok() else {
            continue;
        };
        let mut reader = pumpkin_nbt::deserializer::NbtReadHelperJava::new(std::io::Cursor::new(
            bytes.as_slice(),
        ));
        let Ok(document) = pumpkin_nbt::Nbt::read_unnamed(&mut reader) else {
            continue;
        };
        let Some(world) = world_for(server, key.world) else {
            continue;
        };
        let position =
            pumpkin_util::math::position::BlockPos::new(position.x, position.y, position.z);
        let chunk = pumpkin_util::math::vector2::Vector2::new(key.x(), key.z());
        let be_type = pumpkin_data::BlockState::from_id(state).block_entity_type;
        world.broadcast_to_chunk(
            chunk,
            &pumpkin_protocol::java::client::play::CBlockEntityData::new(
                position,
                pumpkin_protocol::codec::var_int::VarInt::new(i32::from(be_type)),
                bytes.clone().into_boxed_slice(),
            ),
        );
        if let Some(actor) =
            super::cluster_world_callbacks::bedrock_block_actor(state, position, &document.root_tag)
        {
            world.broadcast_to_chunk_bedrock(
                chunk,
                &pumpkin_protocol::bedrock::client::CBlockActorData::new(position, actor),
            );
        }
    }
}

fn publish_current_local_after_ground(
    server: &Server,
    blocks: &super::cluster_chunk_state::OwnedChunkStore,
    writes: &[pumpkin_cluster::canonical_action::AppliedWrite],
) {
    use pumpkin_cluster::canonical_action::{AppliedWrite, Cell, ChunkPart, Value};
    let mut current = BTreeMap::new();
    for write in writes {
        let Target::Chunk {
            key,
            part,
            inventory: None,
        } = write.target
        else {
            continue;
        };
        let after = match part {
            ChunkPart::Block(position) => blocks.block(key, position).ok().map(|state| Cell {
                revision: 0,
                value: Value::Block(state.as_u16()),
            }),
            ChunkPart::BlockEntity(position) => blocks.block_entity(false, key, position).cloned(),
            _ => continue,
        };
        current.insert(
            write.target,
            AppliedWrite {
                target: write.target,
                before: None,
                after,
            },
        );
    }
    if !current.is_empty() {
        publish_canonical_changes(server, blocks, &current.into_values().collect::<Vec<_>>());
    }
}

pub enum ChunkInventoryRequest {
    ActorCutRoute {
        output: mpsc::Sender<ActorCutExport>,
        input: mpsc::Receiver<(
            PeerGeneration,
            pumpkin_cluster::actor_local_cut::ActorLocalChunkImage,
        )>,
    },
    Acquisition(super::cluster::AcquisitionSourceInput),
    AcquisitionReceipts(pumpkin_cluster::acquisition_prefix::AcquisitionReceiptLedger),
    AdvanceInhabited {
        tick: u32,
        chunks: Vec<ChunkKey>,
    },
    Export {
        chunk: ChunkKey,
        respond: ChunkExportReply,
    },
    Install {
        chunk: ChunkKey,
        persisted_generation: Option<u64>,
        peer_reservation_generation: Option<u64>,
        peer_source: Option<(ServerId, u64)>,
        genesis: Option<pumpkin_cluster::chunk_genesis::ChunkGenesisCertificate>,
        occupancy: Option<pumpkin_cluster::canonical_action::Cell>,
        image: Arc<[u8]>,
        blocks: super::cluster_chunk_state::OwnedChunkBlocks,
        states: Vec<pumpkin_cluster::inventory::ChunkInventoryState>,
        pendings: Vec<pumpkin_cluster::xfer::PendingRef>,
        snapshot_cut: TickStamp,
        graphs: Vec<pumpkin_cluster::action_graph::TickGraphSnapshot>,
        ground_anchors: Vec<(
            pumpkin_cluster::canonical_action::Target,
            pumpkin_cluster::action_graph::GroundAnchor,
        )>,
        completion: Option<pumpkin_cluster::realm_completion::RealmCompletionSnapshot>,
        ground_frontier: Option<pumpkin_cluster::action_graph::ChunkGroundFrontier>,
        player_inventory_frontiers: Vec<(
            pumpkin_cluster::action_graph::InventoryRealmId,
            pumpkin_cluster::action_graph::PlayerInventoryGroundFrontier,
        )>,
        player_living: Vec<pumpkin_cluster::xfer::PlayerLivingGround>,
        player_avatar_absent: Vec<pumpkin_cluster::xfer::PlayerAvatarAbsentGround>,
        entity_bodies: Vec<(Target, Option<pumpkin_cluster::canonical_action::Cell>)>,
        entity_history: Vec<pumpkin_cluster::entity_history::EntityHistoryEvidence>,
        dependency_history: Vec<pumpkin_cluster::entity_history::EntityHistoryEvidence>,
        item_spatial: Vec<pumpkin_cluster::item_spatial::ItemSpatialSample>,
        respond: oneshot::Sender<Result<(), pumpkin_cluster::inventory::InventoryApplyError>>,
    },
    CommitReady {
        chunk: ChunkKey,
        identity: ChunkAdmissionIdentity,
        respond: oneshot::Sender<Result<(), pumpkin_cluster::inventory::InventoryApplyError>>,
    },
}

fn inhabited_tick_is_newer(last: Option<u32>, tick: u32) -> bool {
    last.is_none_or(|last| tick != last && tick.wrapping_sub(last) < 1_u32 << 31)
}

fn scheduled_queue_from_image(
    image: &[u8],
    chunk: ChunkKey,
    cut: TickStamp,
    executor: PeerGeneration,
) -> Option<pumpkin_cluster::canonical_action::Cell> {
    use pumpkin_cluster::canonical_action::{Cell, ScheduledTickFact, ScheduledTickKind, Value};
    use pumpkin_world::level::ClusterScheduledKind;
    let queue = pumpkin_world::level::cluster_snapshot_scheduled_ticks(image)?
        .into_iter()
        .enumerate()
        .map(|(source_order, tick)| {
            (tick.position.0.x.div_euclid(16) == chunk.x()
                && tick.position.0.z.div_euclid(16) == chunk.z())
            .then_some(ScheduledTickFact {
                origin: pumpkin_cluster::canonical_action::ScheduledTickOrigin::GenesisPrimary,
                executor,
                kind: match tick.kind {
                    ClusterScheduledKind::Block => ScheduledTickKind::Block,
                    ClusterScheduledKind::Fluid => ScheduledTickKind::Fluid,
                },
                id: tick.id,
                position: pumpkin_cluster::protocol::BlockPos {
                    x: tick.position.0.x,
                    y: tick.position.0.y,
                    z: tick.position.0.z,
                },
                due: TickStamp(cut.0.wrapping_add(1).wrapping_add(u16::from(tick.delay))),
                priority: tick.priority as i8,
                source_order: source_order as u64,
            })
        })
        .collect::<Option<Vec<_>>>()?;
    Some(Cell {
        revision: 0,
        value: Value::ScheduledQueue(queue),
    })
}

pub(super) fn scheduled_raw_image_matches(
    projected: &[pumpkin_cluster::canonical_action::ScheduledTickFact],
    image: &[pumpkin_world::level::ClusterScheduledTick],
    cut: TickStamp,
) -> bool {
    let mut projected = projected
        .iter()
        .map(|tick| (tick.kind, tick.id, tick.position, tick.due, tick.priority))
        .collect::<Vec<_>>();
    let mut image = image
        .iter()
        .map(|tick| {
            let kind = match tick.kind {
                pumpkin_world::level::ClusterScheduledKind::Block => {
                    pumpkin_cluster::canonical_action::ScheduledTickKind::Block
                }
                pumpkin_world::level::ClusterScheduledKind::Fluid => {
                    pumpkin_cluster::canonical_action::ScheduledTickKind::Fluid
                }
            };
            (
                kind,
                tick.id,
                pumpkin_cluster::protocol::BlockPos {
                    x: tick.position.0.x,
                    y: tick.position.0.y,
                    z: tick.position.0.z,
                },
                TickStamp(cut.0.wrapping_add(1).wrapping_add(u16::from(tick.delay))),
                tick.priority as i8,
            )
        })
        .collect::<Vec<_>>();
    projected.sort_unstable();
    image.sort_unstable();
    projected == image
}

fn scheduled_queue_image_matches(
    projected: &[pumpkin_cluster::canonical_action::ScheduledTickFact],
    image: &[pumpkin_cluster::canonical_action::ScheduledTickFact],
) -> bool {
    let mut projected = projected
        .iter()
        .map(|tick| (tick.kind, tick.id, tick.position, tick.due, tick.priority))
        .collect::<Vec<_>>();
    let mut image = image
        .iter()
        .map(|tick| (tick.kind, tick.id, tick.position, tick.due, tick.priority))
        .collect::<Vec<_>>();
    projected.sort_unstable();
    image.sort_unstable();
    projected == image
}

fn block_entities_from_image(
    image: &[u8],
    chunk: ChunkKey,
) -> Option<
    Vec<(
        pumpkin_cluster::protocol::BlockPos,
        pumpkin_cluster::canonical_action::Cell,
    )>,
> {
    use pumpkin_cluster::canonical_action::{Cell, Value};
    pumpkin_world::level::cluster_snapshot_block_entities(image)?
        .into_iter()
        .map(|(position, bytes)| {
            (position.0.x.div_euclid(16) == chunk.x() && position.0.z.div_euclid(16) == chunk.z())
                .then_some((
                    pumpkin_cluster::protocol::BlockPos {
                        x: position.0.x,
                        y: position.0.y,
                        z: position.0.z,
                    },
                    Cell {
                        revision: 0,
                        value: Value::BlockEntity(bytes),
                    },
                ))
        })
        .collect()
}

fn block_inventory_root_images(
    chunk: ChunkKey,
    blocks: &super::cluster_chunk_state::OwnedChunkBlocks,
    entities: &[(
        pumpkin_cluster::protocol::BlockPos,
        pumpkin_cluster::canonical_action::Cell,
    )],
) -> Result<
    Vec<super::cluster_inventory_block_roots::BlockInventoryRootImage>,
    pumpkin_cluster::inventory::InventoryApplyError,
> {
    entities
        .iter()
        .map(|(position, cell)| {
            let state = blocks
                .block(chunk, *position)
                .map_err(|_| pumpkin_cluster::inventory::InventoryApplyError::Rejected)?;
            Ok(
                super::cluster_inventory_block_roots::BlockInventoryRootImage {
                    position: *position,
                    state: state.as_u16(),
                    block_entity: cell.clone(),
                },
            )
        })
        .collect()
}

fn block_entities_match_blocks(
    chunk: ChunkKey,
    blocks: &super::cluster_chunk_state::OwnedChunkBlocks,
    entities: &[(
        pumpkin_cluster::protocol::BlockPos,
        pumpkin_cluster::canonical_action::Cell,
    )],
) -> bool {
    use pumpkin_cluster::canonical_action::Value;
    entities.iter().all(|(position, cell)| {
        let Value::BlockEntity(bytes) = &cell.value else {
            return false;
        };
        blocks.block(chunk, *position).is_ok_and(|state| {
            pumpkin_world::level::cluster_block_entity_document_type_matches(state, bytes)
        })
    })
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct ChunkAdmissionIdentity {
    pub persisted_generation: Option<u64>,
    pub peer_reservation_generation: Option<u64>,
    pub peer_source: Option<(ServerId, u64)>,
}

pub(crate) struct CheckpointSaveResult {
    pub request: u64,
    pub checkpoint_fsynced: bool,
}

fn complete_primary_checkpoint(
    dirty: &mut BTreeSet<ChunkKey>,
    revisions: &mut HashMap<ChunkKey, u64>,
    requested: Vec<(ChunkKey, u64)>,
    success: bool,
) {
    if success {
        for (chunk, revision) in requested {
            if revisions.get(&chunk) == Some(&revision) {
                revisions.remove(&chunk);
                dirty.remove(&chunk);
            }
        }
    }
}

pub enum ChunkExportReply {
    Capture {
        cut: TickStamp,
    },
    Checkpoint {
        request: u64,
        respond: Option<oneshot::Sender<ChunkStateExport>>,
    },
    Serve {
        peer: ServerId,
        generation: u64,
        chunk: ChunkKey,
        requested_cut: Option<TickStamp>,
        subscription: Option<pumpkin_cluster::chunks::ChunkSubscriptionRequest>,
        route: mpsc::Sender<ChunkServeExport>,
        respond: Option<mpsc::OwnedPermit<ChunkServeExport>>,
    },
}

pub struct ActorCutExport {
    pub requester: PeerGeneration,
    pub image: pumpkin_cluster::actor_local_cut::ActorLocalChunkImage,
}

pub struct ChunkServeExport {
    pub subscription: Option<pumpkin_cluster::chunks::ChunkSubscriptionRequest>,
    pub peer: ServerId,
    pub generation: u64,
    pub chunk: ChunkKey,
    pub export: Option<ChunkStateExport>,
}

impl ChunkExportReply {
    fn is_closed(&self) -> bool {
        match self {
            Self::Capture { .. } => false,
            Self::Checkpoint { respond, .. } => {
                respond.as_ref().is_none_or(oneshot::Sender::is_closed)
            }
            Self::Serve { route, .. } => route.is_closed(),
        }
    }

    fn send(mut self, export: ChunkStateExport) {
        match &mut self {
            Self::Capture { .. } => {}
            Self::Checkpoint { respond, .. } => {
                if let Some(route) = respond.take() {
                    let _ = route.send(export);
                }
            }
            Self::Serve {
                peer,
                generation,
                chunk,
                respond,
                subscription,
                ..
            } => {
                if let Some(respond) = respond.take() {
                    respond.send(ChunkServeExport {
                        subscription: *subscription,
                        peer: *peer,
                        generation: *generation,
                        chunk: *chunk,
                        export: Some(export),
                    });
                }
            }
        }
    }
}

impl Drop for ChunkExportReply {
    fn drop(&mut self) {
        if let Self::Serve {
            peer,
            generation,
            chunk,
            respond,
            subscription,
            ..
        } = self
            && let Some(respond) = respond.take()
        {
            respond.send(ChunkServeExport {
                subscription: *subscription,
                peer: *peer,
                generation: *generation,
                chunk: *chunk,
                export: None,
            });
        }
    }
}

#[derive(Clone)]
pub struct ChunkStateExport {
    pub genesis: Option<pumpkin_cluster::chunk_genesis::ChunkGenesisCertificate>,
    pub image: Arc<[u8]>,
    pub occupancy: Option<pumpkin_cluster::canonical_action::Cell>,
    pub origin: pumpkin_world::chunk::ChunkOrigin,
    pub snapshot_cut: TickStamp,
    pub ground_frontier: Option<pumpkin_cluster::action_graph::ChunkGroundFrontier>,
    pub player_inventory_frontiers: Vec<(
        pumpkin_cluster::action_graph::InventoryRealmId,
        pumpkin_cluster::action_graph::PlayerInventoryGroundFrontier,
    )>,
    pub ground_blocks: Arc<super::cluster_chunk_state::OwnedChunkBlocks>,
    pub holders: Vec<u16>,
    pub inventory: Vec<pumpkin_cluster::inventory::ChunkInventoryState>,
    pub player_living: Vec<pumpkin_cluster::xfer::PlayerLivingGround>,
    pub player_avatar_absent: Vec<pumpkin_cluster::xfer::PlayerAvatarAbsentGround>,
    pub entity_bodies: Vec<(Target, Option<pumpkin_cluster::canonical_action::Cell>)>,
    pub entity_history: Vec<pumpkin_cluster::entity_history::EntityHistoryEvidence>,
    pub dependency_history: Vec<pumpkin_cluster::entity_history::EntityHistoryEvidence>,
    pub item_spatial: Vec<pumpkin_cluster::item_spatial::ItemSpatialSample>,
    pub pendings: Vec<pumpkin_cluster::xfer::PendingRef>,
    pub graphs: Vec<pumpkin_cluster::action_graph::TickGraphSnapshot>,
    pub ground_anchors: Vec<(
        pumpkin_cluster::canonical_action::Target,
        pumpkin_cluster::action_graph::GroundAnchor,
    )>,
    pub finishes: Vec<(
        pumpkin_cluster::realm_completion::PeerGeneration,
        pumpkin_cluster::realm_completion::HolisticFinish,
    )>,
    pub completion: pumpkin_cluster::realm_completion::RealmCompletionSnapshot,
}

impl ChunkStateExport {
    pub(super) fn retain_environment_roots(
        &self,
        roots: &mut pumpkin_cluster::world_time::EnvironmentRoots,
    ) {
        for (_, finish) in self.finishes.iter().chain(&self.completion.finishes).chain(
            self.entity_history
                .iter()
                .chain(&self.dependency_history)
                .flat_map(|frame| &frame.completion.finishes),
        ) {
            roots.pin_source(finish.full_source_slot);
        }
        for action in self
            .graphs
            .iter()
            .chain(
                self.entity_history
                    .iter()
                    .chain(&self.dependency_history)
                    .flat_map(|frame| &frame.graphs),
            )
            .flat_map(|graph| &graph.nodes)
            .map(|node| &node.action)
        {
            for slot in action.source_slot_witnesses() {
                roots.pin_source(slot);
            }
            for witness in action.world_border_witnesses() {
                roots.pin_border(witness);
            }
            for (world, morning) in action.morning_witnesses() {
                roots.pin_morning(world, morning);
            }
        }
    }

    pub fn into_payload(self, chunk: ChunkKey, holder: u16) -> pumpkin_cluster::xfer::ChunkPayload {
        let occupancy = self.occupancy;
        let mut payload = pumpkin_cluster::xfer::ChunkPayload::new(
            chunk,
            holder,
            self.snapshot_cut,
            self.image.to_vec(),
            self.pendings,
            self.holders,
        )
        .with_inventory(self.inventory)
        .with_player_living(self.player_living)
        .with_player_avatar_absent(self.player_avatar_absent)
        .with_entity_bodies(self.entity_bodies)
        .with_entity_history(self.entity_history)
        .with_dependency_history(self.dependency_history)
        .with_item_spatial(self.item_spatial)
        .with_graph_state(self.graphs, self.ground_anchors, self.finishes)
        .with_completion(self.completion)
        .with_ground_frontier(self.ground_frontier)
        .with_player_inventory_frontiers(self.player_inventory_frontiers);
        payload.genesis = self.genesis;
        payload.occupancy = occupancy;
        payload
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum ActorDrainStatus {
    Ready {
        player_inventory: pumpkin_cluster::inventory::InventoryStateSnapshot,
        ender_inventory: pumpkin_cluster::inventory::InventoryStateSnapshot,
    },
    StalePlayer,
}

struct PendingActorDrain {
    request: ActorDrainRequest,
    admitted_tick: Option<TickStamp>,
    leave_applied: bool,
}

impl PendingActorDrain {
    fn ready(
        &mut self,
        source_tick: Option<TickStamp>,
        resolution_tick: TickStamp,
        local_finishes: &LocalFinishes,
        buckets: &WorldTickBuckets,
        graphs: &pumpkin_cluster::action_graph::ActionGraphs,
        player_lease: Option<pumpkin_cluster::canonical_action::LivePlayerLease>,
    ) -> bool {
        if player_lease != Some(self.request.lease) {
            return false;
        }
        if self.admitted_tick.is_none()
            && source_tick.is_some_and(|tick| {
                tick == self.request.through_tick || tick.is_after(self.request.through_tick)
            })
        {
            self.admitted_tick = Some(resolution_tick);
        }
        self.admitted_tick.is_some_and(|tick| {
            local_finishes.closed_through(tick)
                && !buckets.actor_unresolved_through(self.request.gid, tick)
                && !buckets.canonical_admission_unresolved_through(
                    self.request.gid,
                    player_lease,
                    tick,
                )
                && !graphs
                    .unsettled_actor_actions(pumpkin_cluster::actor::ActorId::Player(
                        self.request.gid,
                    ))
                    .any(|reference| {
                        tick.distance_since(reference.tick) < 32_768
                            && graphs
                                .ticks
                                .get(&reference.tick)
                                .and_then(|graph| graph.actions.get(&reference.id))
                                .is_some_and(|node| {
                                    node.action.id.actor
                                        == pumpkin_cluster::actor::ActorId::Player(self.request.gid)
                                        && node.action.id.incarnation
                                            == self.request.lease.incarnation
                                        || node.action.player_leases.contains(&self.request.lease)
                                })
                    })
        })
    }
}

struct CanonicalParcelReservation {
    parcels: Vec<OutboundParcel>,
    counts: HashMap<ServerId, usize>,
}

struct ReservedSourceBatch {
    batch: TickBatch,
    parcels: CanonicalParcelReservation,
    legacy: TickFinish,
    realm: pumpkin_cluster::realm_completion::HolisticFinish,
}

fn frozen_canonical_parcels(
    scope: &OwnerTickScope,
    batch: &TickBatch,
) -> Result<Vec<OutboundParcel>, RealmCompletionFailure> {
    use pumpkin_cluster::actor::ActorId;
    use pumpkin_cluster::canonical_action::Stakeholder;
    let mut routed =
        BTreeMap::<(ServerId, Option<pumpkin_cluster::identity::GlobalPlayerId>), TickBatch>::new();
    for action in &batch.canonical_actions {
        if action.tick != batch.tick {
            return Err(RealmCompletionFailure::InvalidActionRequirements);
        }
        let mut peers = BTreeSet::new();
        for stakeholder in &action.stakeholders {
            match stakeholder {
                Stakeholder::Chunk(chunk) => {
                    peers.extend(scope.frozen_chunk_holders(*chunk)?);
                }
                Stakeholder::Player(key) => {
                    let lease = action
                        .player_leases
                        .iter()
                        .find(|lease| lease.key == *key)
                        .ok_or(RealmCompletionFailure::InvalidActionRequirements)?;
                    let chunk = action
                        .player_chunks
                        .iter()
                        .find_map(|(player, chunk)| (*player == *key).then_some(*chunk))
                        .ok_or(RealmCompletionFailure::InvalidActionRequirements)?;
                    if scope.players.get(lease) != Some(&chunk) {
                        return Err(RealmCompletionFailure::MissingRequirement);
                    }
                    peers.extend(scope.frozen_chunk_holders(chunk)?);
                    peers.insert(scope.source(lease.gid.server)?);
                }
            }
        }
        let gid = match action.id.actor {
            ActorId::Player(gid) => Some(gid),
            _ => None,
        };
        for peer in peers.into_iter().filter(|peer| peer.server != scope.local) {
            routed
                .entry((peer.server, gid))
                .or_insert_with(|| TickBatch::new(batch.tick))
                .canonical_actions
                .push(action.clone());
        }
    }
    routed
        .into_iter()
        .map(|((peer, gid), routed)| {
            let bytes = pumpkin_cluster::codec::encode_batch(&routed)
                .map_err(|_| RealmCompletionFailure::InvalidActionRequirements)?;
            let header = StreamHeader::new(
                if gid.is_some() {
                    StreamKind::PlayerWorld
                } else {
                    StreamKind::EntityWorld
                },
                gid,
            );
            if bytes.len() <= pumpkin_cluster::action_wire_pages::MAX_PAGE_BYTES {
                return Ok(vec![OutboundParcel {
                    peer,
                    header,
                    bytes,
                }]);
            }
            let pages = pumpkin_cluster::action_wire_pages::encode(bytes::Bytes::from(bytes))
                .map_err(|_| RealmCompletionFailure::InvalidActionRequirements)?;
            Ok(pages
                .map(|bytes| OutboundParcel {
                    peer,
                    header,
                    bytes: bytes.to_vec(),
                })
                .collect::<Vec<_>>())
        })
        .collect::<Result<Vec<_>, RealmCompletionFailure>>()
        .map(|pages| pages.into_iter().flatten().collect())
}

type CanonicalPageKey = (ServerId, u64, StreamKind, Option<GlobalPlayerId>, [u8; 32]);

struct OwnerCanonicalPages {
    assembler: pumpkin_cluster::action_wire_pages::Reassembler<CanonicalPageKey>,
    completed: HashMap<CanonicalPageKey, TickStamp>,
    deferred: VecDeque<InboundParcel>,
    deferred_bytes: usize,
}

impl OwnerCanonicalPages {
    fn new() -> Self {
        Self {
            assembler: pumpkin_cluster::action_wire_pages::Reassembler::new(
                4096,
                4 * pumpkin_cluster::action_wire_pages::MAX_LOGICAL_BYTES,
            ),
            completed: HashMap::new(),
            deferred: VecDeque::new(),
            deferred_bytes: 0,
        }
    }

    fn has_input_capacity(&self) -> bool {
        self.deferred.len() < 4096
            && self.deferred_bytes
                <= pumpkin_cluster::action_wire_pages::MAX_LOGICAL_BYTES.saturating_sub(1024 * 1024)
    }

    fn decode_or_defer(
        &mut self,
        server: &Server,
        parcel: &InboundParcel,
        pending_bytes: usize,
        outbox: &mut HolisticOutbox,
    ) -> Option<(TickBatch, usize)> {
        use pumpkin_cluster::action_wire_pages::{MAX_LOGICAL_BYTES, PageError};
        if server.cluster_peer_generations.load().get(&parcel.peer) != Some(&parcel.generation) {
            return None;
        }
        let result = (|| {
            if !pumpkin_cluster::action_wire_pages::is_page(&parcel.bytes) {
                if parcel.bytes.len() > MAX_LOGICAL_BYTES {
                    return Err(PageError::LogicalTooLarge);
                }
                if pending_bytes.saturating_add(parcel.bytes.len()) > MAX_LOGICAL_BYTES {
                    return Err(PageError::ByteCapacity);
                }
                return decode_batch(&parcel.bytes)
                    .map(|batch| Some((batch, parcel.bytes.len())))
                    .map_err(|_| PageError::InvalidHeader);
            }
            let page = pumpkin_cluster::action_wire_pages::decode(bytes::Bytes::copy_from_slice(
                &parcel.bytes,
            ))?;
            if pending_bytes.saturating_add(page.logical_len()) > MAX_LOGICAL_BYTES {
                return Err(PageError::ByteCapacity);
            }
            let key = (
                parcel.peer,
                parcel.generation,
                parcel.header.kind,
                parcel.header.player,
                page.digest(),
            );
            let Some(logical) = self.assembler.push(key, page)? else {
                return Ok(None);
            };
            let batch = decode_batch(&logical).map_err(|_| PageError::InvalidHeader)?;
            self.completed.insert(key, batch.tick);
            Ok(Some((batch, logical.len())))
        })();
        match result {
            Ok(Some((batch, size))) => {
                let records = batch
                    .canonical_actions
                    .iter()
                    .try_fold(0usize, |count, action| {
                        action.validate_resource_budget()?;
                        count
                            .checked_add(action.logical_record_count()?)
                            .ok_or(pumpkin_cluster::canonical_action::ApplyFailure::ResourceLimit)
                    });
                if records.is_err()
                    || records.is_ok_and(|count| {
                        count > pumpkin_cluster::canonical_action::MAX_LOGICAL_RECORDS
                    })
                {
                    outbox.fail_peer(parcel.peer, "canonical logical resource limit exceeded");
                    return None;
                }
                Some((batch, size))
            }
            Ok(None) => None,
            Err(PageError::ByteCapacity | PageError::MessageCapacity) => {
                if self.deferred_bytes.saturating_add(parcel.bytes.len()) <= MAX_LOGICAL_BYTES
                    && self.deferred.len() < 4096
                {
                    self.deferred_bytes += parcel.bytes.len();
                    self.deferred.push_back(parcel.clone());
                } else {
                    outbox.fail_peer(
                        parcel.peer,
                        "canonical deferred page resource limit exceeded",
                    );
                }
                None
            }
            Err(_) => {
                outbox.fail_peer(parcel.peer, "invalid canonical logical page");
                None
            }
        }
    }

    fn retire_tick(&mut self, tick: TickStamp) {
        let retired = self
            .completed
            .iter()
            .filter_map(|(key, completed_tick)| (*completed_tick == tick).then_some(*key))
            .collect::<HashSet<_>>();
        self.assembler.retain_keys(|key| !retired.contains(key));
        self.completed
            .retain(|_, completed_tick| *completed_tick != tick);
    }

    fn retain_sources(&mut self, generations: &HashMap<ServerId, u64>) {
        self.assembler
            .retain_keys(|key| generations.get(&key.0) == Some(&key.1));
        self.completed
            .retain(|key, _| generations.get(&key.0) == Some(&key.1));
        self.deferred
            .retain(|parcel| generations.get(&parcel.peer) == Some(&parcel.generation));
        self.deferred_bytes = self.deferred.iter().map(|parcel| parcel.bytes.len()).sum();
    }
}

#[derive(Default)]
struct OwnerAcquisitionBank {
    bank: Option<pumpkin_cluster::acquisition_prefix::AcquisitionSourceBank>,
    pending: VecDeque<super::cluster::AcquisitionSourceInput>,
    events: Option<mpsc::Sender<super::cluster::AcquisitionOwnerEvent>>,
    observed: VecDeque<(
        PeerGeneration,
        pumpkin_cluster::acquisition_prefix::AcquisitionPrefix,
    )>,
    sealed_through: Option<TickStamp>,
    rostered: BTreeMap<ServerId, u64>,
}

struct PreparedAcquisitionClose {
    bank: pumpkin_cluster::acquisition_prefix::AcquisitionSourceBank,
    prefix: pumpkin_cluster::acquisition_prefix::AcquisitionPrefix,
    permit: mpsc::OwnedPermit<super::cluster::AcquisitionOwnerEvent>,
    parcels: CanonicalParcelReservation,
    rostered: BTreeMap<ServerId, u64>,
}

impl OwnerAcquisitionBank {
    fn progress(&mut self, scope: &OwnerTickScope, outbox: &mut HolisticOutbox) {
        use super::cluster::{AcquisitionOwnerEvent, AcquisitionSourceInput};
        use pumpkin_cluster::acquisition_prefix::{AcquisitionSourceBank, encode};
        let Ok(source) = scope.source(scope.local) else {
            return;
        };
        if self.bank.is_none() {
            self.bank = AcquisitionSourceBank::new(source).ok();
        }
        while let Some(input) = self.pending.front() {
            let (control, events) = match input {
                AcquisitionSourceInput::Seal { tick, events } => {
                    if self
                        .sealed_through
                        .is_none_or(|previous| *tick == previous || tick.is_after(previous))
                    {
                        self.sealed_through = Some(*tick);
                    }
                    self.events = Some(events.clone());
                    self.pending.pop_front();
                    continue;
                }
                AcquisitionSourceInput::Control { control, events } => (control, events.clone()),
            };
            let Ok(permit) = events.try_reserve() else {
                break;
            };
            let Some(mut next) = self.bank.clone() else {
                break;
            };
            let Ok(issued) = next.issue_control(control.clone()) else {
                break;
            };
            let Ok(bytes) = encode(&issued) else {
                break;
            };
            let recipients = scope
                .producers
                .iter()
                .filter(|peer| **peer != source)
                .map(|peer| peer.server)
                .collect::<BTreeSet<_>>();
            if !outbox.try_queue_control_batch(
                recipients
                    .into_iter()
                    .map(|peer| OutboundParcel {
                        peer,
                        header: StreamHeader::new(StreamKind::ChunkRequest, None),
                        bytes: bytes.clone(),
                    })
                    .collect(),
            ) {
                break;
            }
            self.bank = Some(next);
            self.events = Some(events.clone());
            self.pending.pop_front();
            permit.send(AcquisitionOwnerEvent::Issued(issued));
        }
        if let Some(events) = &self.events {
            while let Some((authenticated, prefix)) = self.observed.front().copied() {
                if events
                    .try_send(AcquisitionOwnerEvent::ObservedPrefix {
                        authenticated,
                        prefix,
                    })
                    .is_err()
                {
                    break;
                }
                self.observed.pop_front();
            }
        }
    }

    fn prepare_close(
        &self,
        tick: TickStamp,
        scope: &OwnerTickScope,
        outbox: &mut HolisticOutbox,
    ) -> Option<PreparedAcquisitionClose> {
        if !self.pending.is_empty()
            || self
                .sealed_through
                .is_none_or(|sealed| sealed != tick && !sealed.is_after(tick))
        {
            return None;
        }
        let events = self.events.as_ref()?.clone();
        let permit = events.try_reserve_owned().ok()?;
        let mut bank = self.bank.clone()?;
        let prefix = bank.close(tick).ok()?;
        let rostered = scope
            .producers
            .iter()
            .filter(|peer| peer.server != scope.local)
            .filter_map(|peer| {
                scope
                    .transport_generations
                    .get(&peer.server)
                    .copied()
                    .map(|generation| (peer.server, generation))
            })
            .filter(|(peer, generation)| self.rostered.get(peer) != Some(generation))
            .collect::<BTreeMap<_, _>>();
        let bytes = if rostered.is_empty() {
            Vec::new()
        } else {
            pumpkin_cluster::acquisition_prefix::encode_roster(&bank.close_roster(tick).ok()?)
                .ok()?
        };
        let parcels = outbox.reserve_control_batch(
            rostered
                .keys()
                .copied()
                .map(|peer| OutboundParcel {
                    peer,
                    header: StreamHeader::new(StreamKind::ChunkRequest, None),
                    bytes: bytes.clone(),
                })
                .collect(),
        )?;
        Some(PreparedAcquisitionClose {
            bank,
            prefix,
            permit,
            parcels,
            rostered,
        })
    }

    fn commit_close(&mut self, prepared: PreparedAcquisitionClose, outbox: &mut HolisticOutbox) {
        self.bank = Some(prepared.bank);
        self.rostered.extend(prepared.rostered);
        outbox.commit_canonical_reservation(prepared.parcels);
        prepared
            .permit
            .send(super::cluster::AcquisitionOwnerEvent::ClosedPrefix(
                prepared.prefix,
            ));
    }
}

struct HolisticOutbox {
    outbound: mpsc::Sender<OutboundParcel>,
    drop_request: mpsc::Sender<ServerId>,
    pending: BTreeMap<ServerId, VecDeque<OutboundParcel>>,
    reserved: HashMap<ServerId, usize>,
    finish_reservations: BTreeMap<TickStamp, HashSet<ServerId>>,
    quarantined: HashSet<ServerId>,
    unsent_drops: HashSet<ServerId>,
}

impl HolisticOutbox {
    fn reserve_control_batch(
        &mut self,
        parcels: Vec<OutboundParcel>,
    ) -> Option<CanonicalParcelReservation> {
        let mut needed = HashMap::<ServerId, usize>::new();
        for parcel in &parcels {
            *needed.entry(parcel.peer).or_default() += 1;
        }
        if needed.iter().any(|(peer, count)| {
            self.quarantined.contains(peer)
                || self
                    .pending
                    .get(peer)
                    .map_or(0, VecDeque::len)
                    .saturating_add(self.reserved.get(peer).copied().unwrap_or(0))
                    .saturating_add(*count)
                    > MAX_PENDING_RELIABLE_PER_PEER
        }) {
            return None;
        }
        for (peer, count) in &needed {
            *self.reserved.entry(*peer).or_default() += count;
        }
        Some(CanonicalParcelReservation {
            parcels,
            counts: needed,
        })
    }
    fn try_queue_control_batch(&mut self, parcels: Vec<OutboundParcel>) -> bool {
        let mut counts = HashMap::<ServerId, usize>::new();
        for parcel in &parcels {
            *counts.entry(parcel.peer).or_default() += 1;
        }
        if counts.iter().any(|(peer, count)| {
            self.quarantined.contains(peer)
                || self
                    .pending
                    .get(peer)
                    .map_or(0, VecDeque::len)
                    .saturating_add(self.reserved.get(peer).copied().unwrap_or(0))
                    .saturating_add(*count)
                    > MAX_PENDING_RELIABLE_PER_PEER
        }) {
            return false;
        }
        self.queue_parcels(parcels);
        true
    }
    fn try_queue_control(&mut self, parcel: OutboundParcel) -> Result<(), OutboundParcel> {
        if self.quarantined.contains(&parcel.peer)
            || self
                .pending
                .get(&parcel.peer)
                .map_or(0, VecDeque::len)
                .saturating_add(self.reserved.get(&parcel.peer).copied().unwrap_or(0))
                >= MAX_PENDING_RELIABLE_PER_PEER
        {
            return Err(parcel);
        }
        self.queue_parcels([parcel]);
        Ok(())
    }
    fn new(outbound: mpsc::Sender<OutboundParcel>, drop_request: mpsc::Sender<ServerId>) -> Self {
        Self {
            outbound,
            drop_request,
            pending: BTreeMap::new(),
            reserved: HashMap::new(),
            finish_reservations: BTreeMap::new(),
            quarantined: HashSet::new(),
            unsent_drops: HashSet::new(),
        }
    }

    fn queue(&mut self, connected: &HashSet<ServerId>, local: ServerId, finish: &TickFinish) {
        let Ok(bytes) = encode_finish(finish) else {
            tracing::error!(
                tick = finish.tick.0,
                "cluster holistic finish encoding failed"
            );
            return;
        };
        self.queue_parcels(connected.iter().filter(|peer| **peer != local).map(|peer| {
            OutboundParcel {
                peer: *peer,
                header: StreamHeader::new(StreamKind::Accept, None),
                bytes: bytes.clone(),
            }
        }));
    }

    fn queue_realm(
        &mut self,
        connected: &HashSet<ServerId>,
        local: ServerId,
        finish: &pumpkin_cluster::realm_completion::HolisticFinish,
    ) {
        let Ok(bytes) = encode_realm_finish(finish) else {
            tracing::error!(tick = finish.tick.0, "cluster realm finish encoding failed");
            return;
        };
        self.queue_parcels(connected.iter().filter(|peer| **peer != local).map(|peer| {
            OutboundParcel {
                peer: *peer,
                header: StreamHeader::new(StreamKind::Accept, None),
                bytes: bytes.clone(),
            }
        }));
    }

    fn queue_batch(&mut self, connected: &HashSet<ServerId>, batch: &TickBatch) {
        let (parcels, failed) = super::cluster_tick_pump::routed_tick_parcels(batch, connected);
        for peer in failed {
            self.fail_peer(peer, "cluster action batch encode failed");
        }
        self.queue_parcels(parcels);
    }

    fn try_queue_canonical_batch(&mut self, scope: &OwnerTickScope, batch: &TickBatch) -> bool {
        let Some(reservation) = self.reserve_canonical_batch(scope, batch) else {
            return false;
        };
        self.commit_canonical_reservation(reservation);
        true
    }

    fn reserve_source_batch(
        &mut self,
        scope: &OwnerTickScope,
        batch: TickBatch,
        prior_legacy: &TickFinish,
        prior_realm: &BTreeMap<
            RequirementKey,
            BTreeSet<pumpkin_cluster::canonical_action::ActionId>,
        >,
        completion: &RealmCompletionLedger,
        entity_authority: impl FnMut(
            &pumpkin_cluster::canonical_action::Action,
        ) -> Option<(ChunkKey, PeerGeneration)>,
    ) -> Result<ReservedSourceBatch, TickBatch> {
        if prior_legacy.tick != batch.tick {
            return Err(batch);
        }
        let Ok(legacy) = TickFinish::try_from_batch(&batch) else {
            return Err(batch);
        };
        let mut cumulative = BTreeMap::<ChunkKey, BTreeSet<ActionKey>>::new();
        for entry in prior_legacy.chunks.iter().chain(&legacy.chunks) {
            cumulative
                .entry(entry.chunk)
                .or_default()
                .extend(entry.actions.iter().copied());
        }
        let cumulative = TickFinish {
            tick: batch.tick,
            chunks: cumulative
                .into_iter()
                .map(|(chunk, actions)| ChunkActions {
                    chunk,
                    actions: actions.into_iter().collect(),
                })
                .collect(),
        };
        let Ok((realm, recipients)) = scope.tentative_local_manifest(
            batch.tick,
            &batch.canonical_actions,
            prior_realm,
            completion,
            entity_authority,
        ) else {
            return Err(batch);
        };
        if encode_finish(&cumulative).is_err() || encode_realm_finish(&realm).is_err() {
            return Err(batch);
        }
        let Some(parcels) = self.reserve_canonical_batch(scope, &batch) else {
            return Err(batch);
        };
        if !self.reserve_tick_finish(batch.tick, &recipients) {
            self.release_canonical_reservation(&parcels);
            return Err(batch);
        }
        Ok(ReservedSourceBatch {
            batch,
            parcels,
            legacy,
            realm,
        })
    }

    fn reserve_canonical_batch(
        &mut self,
        scope: &OwnerTickScope,
        batch: &TickBatch,
    ) -> Option<CanonicalParcelReservation> {
        let parcels = frozen_canonical_parcels(scope, batch).ok()?;
        let mut needed = HashMap::<ServerId, usize>::new();
        for parcel in &parcels {
            *needed.entry(parcel.peer).or_default() += 1;
        }
        if needed.iter().any(|(peer, count)| {
            self.quarantined.contains(peer)
                || self
                    .pending
                    .get(peer)
                    .map_or(0, VecDeque::len)
                    .saturating_add(self.reserved.get(peer).copied().unwrap_or(0))
                    .saturating_add(*count)
                    > MAX_PENDING_RELIABLE_PER_PEER
        }) {
            return None;
        }
        for (peer, count) in &needed {
            *self.reserved.entry(*peer).or_default() += count;
        }
        Some(CanonicalParcelReservation {
            parcels,
            counts: needed,
        })
    }

    fn release_canonical_reservation(&mut self, reservation: &CanonicalParcelReservation) {
        for (peer, count) in &reservation.counts {
            let remaining = self
                .reserved
                .get_mut(peer)
                .expect("canonical parcel reservation missing");
            *remaining = remaining
                .checked_sub(*count)
                .expect("canonical parcel reservation credit mismatch");
            if *remaining == 0 {
                self.reserved.remove(peer);
            }
        }
    }

    fn reserve_tick_finish(
        &mut self,
        tick: TickStamp,
        recipients: &BTreeSet<PeerGeneration>,
    ) -> bool {
        let prior = self.finish_reservations.get(&tick);
        let additional = recipients
            .iter()
            .map(|peer| peer.server)
            .filter(|peer| prior.is_none_or(|existing| !existing.contains(peer)))
            .collect::<HashSet<_>>();
        if additional.iter().any(|peer| {
            self.quarantined.contains(peer)
                || self
                    .pending
                    .get(peer)
                    .map_or(0, VecDeque::len)
                    .saturating_add(self.reserved.get(peer).copied().unwrap_or(0))
                    .saturating_add(2)
                    > MAX_PENDING_RELIABLE_PER_PEER
        }) {
            return false;
        }
        for peer in &additional {
            *self.reserved.entry(*peer).or_default() += 2;
        }
        self.finish_reservations
            .entry(tick)
            .or_default()
            .extend(additional);
        true
    }

    fn queue_reserved_tick_finish(
        &mut self,
        legacy: &TickFinish,
        realm: &pumpkin_cluster::realm_completion::HolisticFinish,
    ) -> bool {
        if legacy.tick != realm.tick {
            return false;
        }
        let (Ok(legacy_bytes), Ok(realm_bytes)) =
            (encode_finish(legacy), encode_realm_finish(realm))
        else {
            return false;
        };
        let Some(peers) = self.finish_reservations.remove(&realm.tick) else {
            return false;
        };
        let mut parcels = Vec::with_capacity(peers.len() * 2);
        for peer in peers {
            let remaining = self
                .reserved
                .get_mut(&peer)
                .expect("holistic finish reservation missing");
            *remaining = remaining
                .checked_sub(2)
                .expect("holistic finish reservation credit mismatch");
            if *remaining == 0 {
                self.reserved.remove(&peer);
            }
            parcels.push(OutboundParcel {
                peer,
                header: StreamHeader::new(StreamKind::Accept, None),
                bytes: realm_bytes.clone(),
            });
            parcels.push(OutboundParcel {
                peer,
                header: StreamHeader::new(StreamKind::Accept, None),
                bytes: legacy_bytes.clone(),
            });
        }
        self.queue_parcels(parcels);
        true
    }

    fn commit_canonical_reservation(&mut self, reservation: CanonicalParcelReservation) {
        self.release_canonical_reservation(&reservation);
        self.queue_parcels(reservation.parcels);
    }

    fn queue_parcels(&mut self, parcels: impl IntoIterator<Item = OutboundParcel>) {
        for parcel in parcels {
            let peer = parcel.peer;
            if self.quarantined.contains(&peer) {
                continue;
            }
            if self
                .pending
                .get(&peer)
                .map_or(0, VecDeque::len)
                .saturating_add(self.reserved.get(&peer).copied().unwrap_or(0))
                >= MAX_PENDING_RELIABLE_PER_PEER
            {
                self.fail_peer(peer, "cluster reliable action delivery capacity exhausted");
                continue;
            }
            self.pending.entry(peer).or_default().push_back(parcel);
        }
        self.flush();
    }

    fn fail_peer(&mut self, peer: ServerId, reason: &'static str) {
        if self.quarantined.insert(peer) {
            self.pending.remove(&peer);
            self.unsent_drops.insert(peer);
            tracing::error!(
                peer = peer.0,
                reason,
                "cluster peer reliable delivery failed"
            );
        }
    }

    fn flush(&mut self) {
        for peer in self.unsent_drops.clone() {
            if self.drop_request.try_send(peer).is_ok() {
                self.unsent_drops.remove(&peer);
            }
        }
        for pending in self.pending.values_mut() {
            while let Some(parcel) = pending.front() {
                if self.outbound.try_send(parcel.clone()).is_err() {
                    break;
                }
                pending.pop_front();
            }
        }
        self.pending.retain(|_, pending| !pending.is_empty());
    }

    fn peer_dropped(&mut self, peer: ServerId) {
        self.pending.remove(&peer);
        self.quarantined.remove(&peer);
        self.unsent_drops.remove(&peer);
    }
}

fn reserve_actor_source_page(
    outbox: &mut HolisticOutbox,
    scope: Option<&OwnerTickScope>,
    batch: TickBatch,
    finishes: &LocalFinishes,
    queued: &[pumpkin_cluster::canonical_action::Action],
    reserved: &VecDeque<ReservedSourceBatch>,
    issued: Option<
        &BTreeMap<RequirementKey, BTreeSet<pumpkin_cluster::canonical_action::ActionId>>,
    >,
    completion: &RealmCompletionLedger,
    entity_authority: impl FnMut(
        &pumpkin_cluster::canonical_action::Action,
    ) -> Option<(ChunkKey, PeerGeneration)>,
) -> Result<ReservedSourceBatch, TickBatch> {
    let Some(scope) = scope else {
        return Err(batch);
    };
    let Ok(prior) = finishes.source_prefix(batch.tick, queued, reserved) else {
        return Err(batch);
    };
    let mut realms = issued.cloned().unwrap_or_default();
    for page in reserved.iter().filter(|page| page.batch.tick == batch.tick) {
        for (key, actions) in &page.realm.issued {
            realms
                .entry(*key)
                .or_default()
                .extend(actions.iter().copied());
        }
    }
    outbox.reserve_source_batch(scope, batch, &prior, &realms, completion, entity_authority)
}

fn entity_spatial_issuer_authorized(
    scope: &OwnerTickScope,
    body: &pumpkin_cluster::entity_body::EntityBody,
    chunk: ChunkKey,
    issuer: PeerGeneration,
) -> bool {
    if scope.source(issuer.server).ok() != Some(issuer) {
        return false;
    }
    match body.payload {
        pumpkin_cluster::entity_body::EntityPayload::Item(_) => {
            scope.scheduled_executor(chunk).ok() == Some(issuer)
        }
        pumpkin_cluster::entity_body::EntityPayload::Experience(_) => {
            body.owner == issuer && scope.scheduled_executor(chunk).ok() == Some(issuer)
        }
        _ => body.owner == issuer,
    }
}

pub(super) fn publish_owned_entity_spatial(
    server: &Server,
    blocks: &super::cluster_chunk_state::OwnedChunkStore,
    cache: &mut pumpkin_cluster::item_spatial::OwnedItemSpatial,
    scope: &OwnerTickScope,
    samples: impl IntoIterator<Item = (ChunkKey, pumpkin_cluster::item_spatial::ItemSpatialSample)>,
) {
    let mut peers = BTreeMap::<ServerId, Vec<_>>::new();
    for (chunk, sample) in samples {
        let Some(pumpkin_cluster::canonical_action::Cell {
            value: pumpkin_cluster::canonical_action::Value::Entity(body),
            ..
        }) = blocks.entity_body(false, chunk, sample.id)
        else {
            continue;
        };
        if !entity_spatial_issuer_authorized(scope, body, chunk, sample.issuer)
            || sample.spatial.world != chunk.world
        {
            continue;
        }
        let issuer = sample.issuer;
        if !cache.apply(sample.clone(), body, chunk, issuer) {
            continue;
        }
        if let Ok(holders) = scope.frozen_chunk_holders(chunk) {
            for holder in holders {
                if holder.server != issuer.server {
                    peers.entry(holder.server).or_default().push(sample.clone());
                }
            }
        }
    }
    let limits = server.cluster_datagram_limits.load();
    for (peer, samples) in peers {
        if let Some(limit) = limits.get(&peer) {
            super::cluster_item_physics::forward_samples(
                server,
                peer,
                (*limit).min(super::cluster_datagram::UDP_PATH_BYTES.saturating_sub(100)),
                &samples,
            );
        }
    }
}

impl LocalFinishes {
    fn source_prefix(
        &self,
        tick: TickStamp,
        queued: &[pumpkin_cluster::canonical_action::Action],
        reserved: &VecDeque<ReservedSourceBatch>,
    ) -> Result<TickFinish, ()> {
        let mut batch = TickBatch::new(tick);
        batch.canonical_actions.extend_from_slice(queued);
        let queued = TickFinish::try_from_batch(&batch).map_err(|_| ())?;
        let mut chunks = BTreeMap::<ChunkKey, BTreeSet<ActionKey>>::new();
        for manifest in self
            .pending
            .get(&tick)
            .into_iter()
            .flatten()
            .chain(
                reserved
                    .iter()
                    .filter(|page| page.batch.tick == tick)
                    .map(|page| &page.legacy),
            )
            .chain(std::iter::once(&queued))
        {
            for entry in &manifest.chunks {
                chunks
                    .entry(entry.chunk)
                    .or_default()
                    .extend(entry.actions.iter().copied());
            }
        }
        Ok(TickFinish {
            tick,
            chunks: chunks
                .into_iter()
                .map(|(chunk, actions)| ChunkActions {
                    chunk,
                    actions: actions.into_iter().collect(),
                })
                .collect(),
        })
    }
    fn resolution_tick(&self, observed: TickStamp) -> TickStamp {
        match self.last_tick {
            Some(previous) if previous.is_after(observed) => previous,
            _ => observed,
        }
    }

    fn record(&mut self, finish: TickFinish) -> bool {
        if self
            .last_tick
            .is_some_and(|last| last.is_after(finish.tick))
        {
            return false;
        }
        self.pending.entry(finish.tick).or_default().push(finish);
        true
    }

    fn advance(&mut self, now: TickStamp) -> Vec<TickFinish> {
        if let Some(previous) = self.last_tick {
            if !now.is_after(previous) && now != previous {
                return Vec::new();
            }
            let distance = now.distance_since(previous);
            if distance > 0 && distance < 1_200 {
                for step in 1..=distance {
                    self.pending
                        .entry(TickStamp(previous.0.wrapping_add(step)))
                        .or_default();
                }
            }
        }
        self.pending.entry(now).or_default();
        self.last_tick = Some(now);
        let mut ready: Vec<_> = self
            .pending
            .keys()
            .copied()
            .filter(|tick| {
                let distance = now.distance_since(*tick);
                distance > 0 && distance < 32_768
            })
            .collect();
        ready.sort_by_key(|tick| std::cmp::Reverse(now.distance_since(*tick)));
        let closed = ready
            .into_iter()
            .filter_map(|tick| {
                let manifests = self.pending.remove(&tick)?;
                let mut chunks: BTreeMap<ChunkKey, BTreeSet<ActionKey>> = BTreeMap::new();
                for manifest in manifests {
                    for entry in manifest.chunks {
                        chunks.entry(entry.chunk).or_default().extend(entry.actions);
                    }
                }
                Some(TickFinish {
                    tick,
                    chunks: chunks
                        .into_iter()
                        .map(|(chunk, actions)| ChunkActions {
                            chunk,
                            actions: actions.into_iter().collect(),
                        })
                        .collect(),
                })
            })
            .collect::<Vec<_>>();
        closed
    }

    fn closed_through(&self, tick: TickStamp) -> bool {
        self.closed_through
            .is_some_and(|closed| closed == tick || closed.is_after(tick))
    }
}

fn entity_authority_at_action_tick(
    graphs: &pumpkin_cluster::action_graph::ActionGraphs,
    action: &pumpkin_cluster::canonical_action::Action,
) -> Option<(ChunkKey, PeerGeneration)> {
    let pumpkin_cluster::actor::ActorId::Entity(id) = action.id.actor else {
        return None;
    };
    let chunk = action.actor_chunk?;
    let target = Target::Chunk {
        key: chunk,
        part: pumpkin_cluster::canonical_action::ChunkPart::Entity(id),
        inventory: None,
    };
    match graphs.symbolic_anchor_for(action.tick, target).ok()? {
        Some(cell) => {
            let pumpkin_cluster::canonical_action::Value::Entity(body) = cell.value else {
                return None;
            };
            (body.belongs_to(id, chunk) && entity_move_witness_matches(action, &body))
                .then_some((chunk, body.owner))
        }
        None => action
            .dependencies
            .iter()
            .filter(|reference| reference.tick == action.tick)
            .find_map(|reference| {
                let node = graphs
                    .ticks
                    .get(&reference.tick)?
                    .actions
                    .get(&reference.id)?;
                node.action.effects.iter().find_map(|effect| {
                    let pumpkin_cluster::canonical_action::Effect::Put {
                        target: written,
                        value: pumpkin_cluster::canonical_action::Value::Entity(body),
                    } = effect
                    else {
                        return None;
                    };
                    (*written == target
                        && body.belongs_to(id, chunk)
                        && entity_move_witness_matches(action, body)
                        && body.birth.action == *reference
                        && node.action.id.actor.origin() == body.birth.source.server)
                        .then_some((chunk, body.owner))
                })
            }),
    }
}

fn entity_move_witness_matches(
    action: &pumpkin_cluster::canonical_action::Action,
    body: &pumpkin_cluster::entity_body::EntityBody,
) -> bool {
    action.effects.iter().all(|effect| match effect {
        pumpkin_cluster::canonical_action::Effect::MoveEntityBody {
            authority: Some(witness),
            ..
        } => {
            witness.birth == body.birth
                && witness.body_owner == body.owner
                && (witness.issuer == body.owner
                    || matches!(
                        body.payload,
                        pumpkin_cluster::entity_body::EntityPayload::Item(_)
                    ))
        }
        pumpkin_cluster::canonical_action::Effect::MoveEntityBody {
            authority: None, ..
        } => false,
        _ => true,
    })
}

fn collect_canonical_graph_inputs(
    server: &Server,
    current_tick: TickStamp,
    buckets: &mut WorldTickBuckets,
    scopes: &BTreeMap<TickStamp, OwnerTickScope>,
    completion: &mut RealmCompletionLedger,
    graphs: &mut pumpkin_cluster::action_graph::ActionGraphs,
    ground: &super::cluster_world_canonical::OwnedWorldActionState<'_>,
    diagnostics: &mut HashSet<TickStamp>,
    local_issued: &mut BTreeMap<
        TickStamp,
        BTreeMap<RequirementKey, BTreeSet<pumpkin_cluster::canonical_action::ActionId>>,
    >,
    changed: &mut Vec<pumpkin_cluster::canonical_action::ActionRef>,
) -> BTreeSet<TickStamp> {
    let mut invalid_ticks = BTreeSet::new();
    let mut collected =
        BTreeMap::<TickStamp, Vec<(ServerId, pumpkin_cluster::canonical_action::Action)>>::new();
    for (source, action) in buckets.drain_admitted_canonical_actions_matching(|_, action| {
        (scopes.contains_key(&action.tick) || current_tick.is_after(action.tick))
            && action.effects.iter().all(|effect| match effect {
                pumpkin_cluster::canonical_action::Effect::PlayerAvatarLifecycle {
                    target,
                    intent:
                        pumpkin_cluster::avatar_lifecycle::AvatarLifecycleIntent::Admission {
                            lease,
                            ..
                        },
                } => {
                    scopes.get(&action.tick).is_some_and(|scope| {
                        scope.players.contains_key(lease)
                            || scope.avatar_bootstrap_chunk(action, lease).is_some()
                    }) && ground.available(TruthView::Ground, *target)
                        && ground.read(TruthView::Ground, *target).is_none()
                        && graphs
                            .player_avatar_admissions
                            .get(target)
                            .is_none_or(|current| current == lease)
                }
                _ => true,
            })
            && (!matches!(action.id.actor, pumpkin_cluster::actor::ActorId::Entity(_))
                || ground
                    .blocks
                    .historical_entity_authority(action)
                    .or_else(|| entity_authority_at_action_tick(graphs, action))
                    .is_some())
    }) {
        collected
            .entry(action.tick)
            .or_default()
            .push((source, action));
    }
    let local = ServerId(server.advanced_config.cluster.server_id);
    for (tick, actions) in collected {
        let scope = scopes.get(&tick);
        let mut captured = completion.clone();
        let mut issued =
            BTreeMap::<RequirementKey, BTreeSet<pumpkin_cluster::canonical_action::ActionId>>::new(
            );
        let mut graph_actions = Vec::with_capacity(actions.len());
        for (source, action) in actions {
            let mut admitted = false;
            {
                match scope
                    .ok_or(RealmCompletionFailure::MissingRequirement)
                    .and_then(|scope| {
                        scope.capture_action_with_entity_authority(
                            &mut captured,
                            source,
                            &action,
                            ground
                                .blocks
                                .historical_entity_authority(&action)
                                .or_else(|| entity_authority_at_action_tick(graphs, &action)),
                        )
                    }) {
                    Ok(requirements) if source == local => {
                        admitted = true;
                        for key in requirements {
                            issued.entry(key).or_default().insert(action.id);
                        }
                    }
                    Ok(_) => {
                        admitted = true;
                    }
                    Err(reason) => {
                        invalid_ticks.insert(tick);
                        if diagnostics.len() < 1_024 && diagnostics.insert(tick) {
                            tracing::error!(
                                tick = tick.0,
                                ?reason,
                                "cluster canonical completion capture failed"
                            );
                        }
                    }
                }
            }
            if admitted {
                graph_actions.push(action);
            }
        }
        if graph_actions.is_empty() || invalid_ticks.contains(&tick) {
            continue;
        }
        let mut seeded_anchors = Vec::new();
        let mut seeded_avatar_admissions = Vec::new();
        let mut seeded_inventory_frontiers = Vec::new();
        for action in &graph_actions {
            let admission_targets = action
                .effects
                .iter()
                .filter(|effect| {
                    matches!(
                        effect,
                        pumpkin_cluster::canonical_action::Effect::EntityAdmission { .. }
                    )
                })
                .flat_map(pumpkin_cluster::canonical_action::Effect::targets)
                .collect::<Vec<_>>();
            if !admission_targets.is_empty() {
                if admission_targets.iter().any(|target| {
                    !ground.available(TruthView::Ground, *target)
                        || ground.read(TruthView::Ground, *target).is_some()
                }) {
                    invalid_ticks.insert(tick);
                    break;
                }
                match graphs.seed_entity_admission_bootstrap(action) {
                    Ok(anchors) => seeded_anchors.extend(anchors),
                    Err(_) => {
                        invalid_ticks.insert(tick);
                        break;
                    }
                }
            }
            for target in action
                .targets()
                .into_iter()
                .chain(action.counterfactual_conditions.iter().copied())
                .chain(action.counterfactual_effects.iter().copied())
            {
                if let Some((key, gid)) = target.player_avatar()
                    && !graphs
                        .player_avatar_admissions
                        .get(&Target::Chunk {
                            key,
                            part: pumpkin_cluster::canonical_action::ChunkPart::Player(gid),
                            inventory: None,
                        })
                        .is_some_and(|lease| {
                            lease.gid == gid && action.player_leases.contains(lease)
                        })
                {
                    let body = Target::Chunk {
                        key,
                        part: pumpkin_cluster::canonical_action::ChunkPart::Player(gid),
                        inventory: None,
                    };
                    let bootstrap = action.effects.iter().find_map(|effect| match effect {
                        pumpkin_cluster::canonical_action::Effect::PlayerAvatarLifecycle {
                            target: affected,
                            intent: pumpkin_cluster::avatar_lifecycle::AvatarLifecycleIntent::Admission { lease, owner, .. },
                        } if *affected == body && lease.gid == gid
                            && scope.is_some_and(|scope| (scope.players.get(lease) == Some(&key) || scope.avatar_bootstrap_chunk(action, lease) == Some(key)) && scope.source(gid.server).ok() == Some(*owner)) => Some(*lease),
                        _ => None,
                    });
                    let had_anchor = graphs.ground_anchors.contains_key(&body);
                    if let Some(lease) = bootstrap
                        && ground.available(TruthView::Ground, body)
                        && ground.read(TruthView::Ground, body).is_none()
                        && graphs
                            .seed_player_avatar_bootstrap(body, lease, tick)
                            .is_ok()
                    {
                        seeded_avatar_admissions.push(body);
                        if !had_anchor {
                            seeded_anchors.push(body);
                        }
                        match graphs.seed_player_inventory_bootstrap(action) {
                            Ok((anchors, frontiers)) => {
                                seeded_anchors.extend(anchors);
                                seeded_inventory_frontiers.extend(frontiers);
                            }
                            Err(_) => {
                                invalid_ticks.insert(tick);
                                break;
                            }
                        }
                    } else {
                        invalid_ticks.insert(tick);
                        break;
                    }
                }
                if graphs.ground_anchors.contains_key(&target) {
                    continue;
                }
                match target {
                    Target::Chunk { key, .. } => {
                        if matches!(
                            target,
                            Target::Chunk {
                                part: pumpkin_cluster::canonical_action::ChunkPart::Entity(_),
                                inventory: None,
                                ..
                            }
                        ) && ground.read(TruthView::Ground, target).is_some()
                        {
                            invalid_ticks.insert(tick);
                            break;
                        }
                        if matches!(
                            target,
                            Target::Chunk {
                                part: pumpkin_cluster::canonical_action::ChunkPart::Player(_),
                                inventory: None,
                                ..
                            }
                        ) {
                            // Avatar genesis has its own accepted cut. The chunk
                            // frontier may predate this player's admission.
                            invalid_ticks.insert(tick);
                            break;
                        }
                        let Some(frontier) = graphs.chunk_frontiers.get(&key).copied() else {
                            invalid_ticks.insert(tick);
                            break;
                        };
                        if !action.tick.is_after(frontier.settled_through)
                            || !ground.available(TruthView::Ground, target)
                            || graphs
                                .seed_chunk_target_anchor(
                                    target,
                                    key,
                                    ground.read(TruthView::Ground, target),
                                )
                                .is_err()
                        {
                            invalid_ticks.insert(tick);
                            break;
                        }
                        seeded_anchors.push(target);
                    }
                    Target::Player {
                        key,
                        lease: Some(lease),
                        inventory: Some(_),
                    } => {
                        if !action.player_leases.contains(&lease) {
                            invalid_ticks.insert(tick);
                            break;
                        }
                        let pumpkin_cluster::action_graph::ActionRealm::Inventory(
                            pumpkin_cluster::canonical_action::Stakeholder::Player(_),
                            realm,
                        ) = target.action_realm()
                        else {
                            invalid_ticks.insert(tick);
                            break;
                        };
                        let Some(frontier) = graphs.player_inventory_frontiers.get(&(
                            key,
                            realm,
                            lease.gid,
                            lease.incarnation,
                        )) else {
                            invalid_ticks.insert(tick);
                            break;
                        };
                        if !action.tick.is_after(frontier.settled_through)
                            || frontier.lease != lease
                            || action
                                .player_chunks
                                .binary_search_by_key(&key, |(player, _)| *player)
                                .ok()
                                .is_none_or(|index| {
                                    action.player_chunks[index].1 != frontier.accepted_chunk
                                })
                            || !ground.available(TruthView::Ground, target)
                            || graphs
                                .seed_player_inventory_target_anchor(
                                    target,
                                    lease,
                                    ground.read(TruthView::Ground, target),
                                )
                                .is_err()
                        {
                            invalid_ticks.insert(tick);
                            break;
                        }
                        seeded_anchors.push(target);
                    }
                    Target::Player { .. } => {
                        invalid_ticks.insert(tick);
                        break;
                    }
                }
            }
            if invalid_ticks.contains(&tick) {
                break;
            }
        }
        if invalid_ticks.contains(&tick) {
            for key in seeded_inventory_frontiers {
                graphs.player_inventory_frontiers.remove(&key);
            }
            for target in seeded_avatar_admissions {
                graphs.player_avatar_admissions.remove(&target);
                graphs.player_avatar_admitted_at.remove(&target);
            }
            for target in seeded_anchors {
                graphs.ground_anchors.remove(&target);
            }
            continue;
        }
        match graphs.insert_collected(tick, graph_actions) {
            Ok(connected) => {
                changed.extend(
                    connected
                        .into_iter()
                        .map(|id| pumpkin_cluster::canonical_action::ActionRef { tick, id }),
                );
                *completion = captured;
                let recorded = local_issued.entry(tick).or_default();
                for (key, ids) in issued {
                    recorded.entry(key).or_default().extend(ids);
                }
            }
            Err(reason) => {
                for key in seeded_inventory_frontiers {
                    graphs.player_inventory_frontiers.remove(&key);
                }
                for target in seeded_avatar_admissions {
                    graphs.player_avatar_admissions.remove(&target);
                    graphs.player_avatar_admitted_at.remove(&target);
                }
                for target in seeded_anchors {
                    graphs.ground_anchors.remove(&target);
                }
                invalid_ticks.insert(tick);
                if diagnostics.len() < 1_024 && diagnostics.insert(tick) {
                    tracing::error!(
                        tick = tick.0,
                        ?reason,
                        "cluster canonical graph intake failed"
                    );
                }
            }
        }
    }
    invalid_ticks
}

fn retick_batch(batch: &mut TickBatch, tick: TickStamp) {
    batch.tick = tick;
    macro_rules! retick {
        ($($field:ident),+ $(,)?) => {
            $(for update in &mut batch.$field { update.tick = tick; })+
        };
    }
    retick!(
        canonical_actions,
        pos,
        equipment,
        held,
        sneak,
        sprint,
        pose,
        blocking,
        swing,
        skin,
        eat_start,
        eat_abort,
        break_anim,
        break_block,
        place_block,
        inv_ops,
        hit_player,
        hit_entity,
        fire,
        administrative,
        player_administrative,
        player_chunk_transition,
        entity_chunk_transition,
        item_action,
    );
}

pub fn spawn_world_apply(
    server: &Arc<Server>,
    mut world_rx: mpsc::Receiver<InboundParcel>,
    mut combat_rx: mpsc::Receiver<InboundParcel>,
    mut completion_rx: mpsc::Receiver<(
        pumpkin_cluster::realm_completion::PeerGeneration,
        CompletionMessage,
    )>,
    mut local_tick_rx: mpsc::Receiver<LocalTickBatch>,
    mut peer_drop_rx: watch::Receiver<Arc<HashMap<ServerId, u64>>>,
    drop_complete_tx: watch::Sender<Arc<HashMap<ServerId, u64>>>,
    _peers: Vec<ServerId>,
    outbound: mpsc::Sender<OutboundParcel>,
    drop_request: mpsc::Sender<ServerId>,
    mut drain_rx: mpsc::Receiver<ActorDrainRequest>,
    mut chunk_inventory_rx: mpsc::Receiver<ChunkInventoryRequest>,
) {
    let inventory = super::cluster_world_inventory::InventoryOwner::attach(server);
    let (watch_tx, mut watch_rx) = mpsc::channel(256);
    server
        .cluster_chunk_watch_route
        .store(Some(Arc::new(watch_tx)));
    let (stats_import_tx, mut stats_import_rx) = mpsc::channel(256);
    server
        .cluster_actor_statistics_import_route
        .store(Some(Arc::new(stats_import_tx)));
    let (stats_retire_tx, mut stats_retire_rx) = mpsc::channel(256);
    server
        .cluster_actor_statistics_retire_route
        .store(Some(Arc::new(stats_retire_tx)));
    let (living_import_tx, mut living_import_rx) = mpsc::channel(256);
    server
        .cluster_actor_living_import_route
        .store(Some(Arc::new(living_import_tx)));
    let (vehicle_control_tx, mut vehicle_control_rx) = mpsc::channel(1024);
    server
        .cluster_vehicle_control_route
        .store(Some(Arc::new(vehicle_control_tx)));
    let (admission_import_tx, mut admission_import_rx) = mpsc::channel(256);
    server
        .cluster_actor_admission_import_route
        .store(Some(Arc::new(admission_import_tx)));
    let (action_tx, mut action_rx) = mpsc::channel(4_096);
    server.cluster_action_route.store(Some(Arc::new(action_tx)));
    let (item_spatial_tx, mut item_spatial_rx) = mpsc::channel(4096);
    server
        .cluster_item_spatial_route
        .store(Some(Arc::new(item_spatial_tx)));
    let (replica_binding_tx, mut replica_binding_rx) = mpsc::channel(4_096);
    server
        .cluster_replica_binding_route
        .store(Some(Arc::new(replica_binding_tx)));
    let (checkpoint_result_tx, mut checkpoint_result_rx) = mpsc::channel(256);
    if server.is_cluster_primary() {
        server
            .cluster_checkpoint_result_route
            .store(Some(Arc::new(checkpoint_result_tx)));
    }
    let (fresh_tx, mut fresh_rx) = mpsc::channel(256);
    if server.is_cluster_primary() {
        server
            .cluster_fresh_admission_route
            .store(Some(Arc::new(fresh_tx.clone())));
        for world in server.worlds.load().iter() {
            world
                .level
                .fresh_chunk_admission
                .store(Some(Arc::new(fresh_tx.clone())));
            if let Some(wake) = world.level.fresh_chunk_wake.load_full() {
                let _ = wake.try_send(());
            }
        }
    }
    let server = Arc::clone(server);
    let task_server = Arc::clone(&server);
    server.spawn_task(async move {
        let server = task_server;
        let local = ServerId(server.advanced_config.cluster.server_id);
        let mut buckets = WorldTickBuckets::new();
        let mut chunk_blocks = super::cluster_chunk_state::OwnedChunkStore::default();
        let mut canonical_versions = BTreeMap::<(TruthView, Target), u64>::new();
        let mut canonical_pending = HashMap::new();
        let mut unsettled_primary_retention = HashMap::<ChunkKey, Option<std::time::Instant>>::new();
        let mut chunk_origins = HashMap::<ChunkKey, pumpkin_world::chunk::ChunkOrigin>::new();
        let mut chunk_images = HashMap::<ChunkKey, pumpkin_world::level::OwnedChunkImage>::new();
        let mut chunk_inhabited_delta = HashMap::<ChunkKey, u64>::new();
        let mut chunk_lighting = HashMap::<ChunkKey, pumpkin_world::lighting::owned::OwnedChunkLighting>::new();
        let mut presentation_lighting = super::cluster_chunk_presentation::PresentationLighting::default();
        let mut chunk_watches = super::cluster_chunk_watch::OwnedChunkWatches::default();
        let mut actor_views = super::cluster_world_view::OwnedViews::default();
        let mut pending_actor_views = HashMap::<pumpkin_cluster::identity::GlobalPlayerId, pumpkin_cluster::controller::ViewIntent>::new();
        let mut dirty_lighting = BTreeSet::<ChunkKey>::new();
        let mut lighting_work: Option<(ChunkKey, pumpkin_world::lighting::owned::OwnedLightingWork)> = None;
        let mut lighting_chunk_needs = HashMap::<ChunkKey, (u64, std::time::Instant)>::new();
        let mut next_lighting_lease = 1_u64 << 62;
        let mut last_inhabited_tick = HashMap::<ChunkKey, u32>::new();
        let mut pending_inventory_retirements = HashSet::new();
        let mut pending_remote_avatar_retirements = HashSet::new();
        let mut pending_avatar_retirements = HashMap::<pumpkin_cluster::canonical_action::LivePlayerLease, (super::cluster_actor_statistics::ActorStatisticsRetire, Option<pumpkin_cluster::canonical_action::ActionRef>)>::new();
        let mut pending_primary_save = None;
        let mut dirty_primary_chunks = BTreeSet::<ChunkKey>::new();
        let mut primary_dirty_revisions = HashMap::<ChunkKey, u64>::new();
        let mut primary_change_serial = 0_u64;
        let mut primary_checkpoint_request = 0_u64;
        let mut primary_checkpoint_inflight = HashMap::<u64, Vec<(ChunkKey, u64)>>::new();
        let mut primary_checkpoint_roots = HashMap::<u64, pumpkin_cluster::world_time::EnvironmentRoots>::new();
        let mut primary_checkpoint_retry_at = None;
        let mut legacy_ground_without_graph = HashSet::<ChunkKey>::new();
        let mut legacy_canonical_diagnostics = HashSet::<ChunkKey>::new();
        let mut unsupported_legacy_ticks = HashSet::<TickStamp>::new();
        let mut canonical_queued = BTreeMap::<TickStamp, Vec<pumpkin_cluster::canonical_action::Action>>::new();
        let mut canonical_reserved_batches = VecDeque::<ReservedSourceBatch>::new();
        let mut canonical_changed_pending = BTreeSet::<pumpkin_cluster::canonical_action::ActionRef>::new();
        let mut pending_actor_bindings = BTreeSet::<pumpkin_cluster::canonical_action::ActionRef>::new();
        let mut canonical_bucket_grounded_pending = BTreeSet::<pumpkin_cluster::canonical_action::ActionRef>::new();
        let mut last_graph_resolution_tick = None;
        let mut canonical_block_acknowledged = HashSet::<pumpkin_cluster::canonical_action::ActionId>::new();
        let mut canonical_graphs = pumpkin_cluster::action_graph::ActionGraphs::default();
        #[cfg(feature = "cluster-e2e")]
        let mut test_graph_observer = super::cluster_test_observation::GraphObserver::default();
        let mut chunk_done_archive = pumpkin_cluster::chunk_archive::ChunkDoneArchive::default();
        let mut historical_acquisition_receipts = pumpkin_cluster::acquisition_prefix::AcquisitionReceiptLedger::default();
        let mut historical_chunk_cuts = pumpkin_cluster::chunk_cut_archive::ChunkCutArchive::<ChunkStateExport>::new(8192, 256 * 1024 * 1024);
        let mut pending_historical_captures = BTreeSet::<(ChunkKey, TickStamp)>::new();
        let mut historical_capture_sources = BTreeMap::<TickStamp, BTreeSet<PeerGeneration>>::new();
        let mut historical_actor_requests = BTreeMap::<(PeerGeneration, ChunkKey, u64), pumpkin_cluster::acquisition_prefix::HistoricalChunkRequest>::new();
        let mut source_actor_snapshots = BTreeMap::<u64, (Arc<super::cluster_world_callback_snapshot::CallbackTickSnapshot>, usize, BTreeSet<PeerGeneration>)>::new();
        let mut source_actor_snapshot_bytes = 0usize;
        let mut actor_cut_route = None::<mpsc::Sender<ActorCutExport>>;
        let mut actor_cut_input = None::<mpsc::Receiver<(PeerGeneration, pumpkin_cluster::actor_local_cut::ActorLocalChunkImage)>>;
        let mut pending_actor_cut_image = None::<(PeerGeneration, pumpkin_cluster::actor_local_cut::ActorLocalChunkImage)>;
        let mut served_actor_cuts = BTreeMap::<(PeerGeneration, ChunkKey, u64), pumpkin_cluster::acquisition_prefix::HistoricalChunkRequest>::new();
        let mut historical_actor_cuts = BTreeMap::<(u64, ChunkKey), (super::cluster_actor_local_cut::ActorLocalChunkCut, usize)>::new();
        let mut historical_actor_need_pins = BTreeMap::<(u64, ChunkKey), super::cluster::HistoricalChunkNeedLease>::new();
        let mut next_historical_actor_lease = 1_u64;
        let mut historical_phase_tick = None::<TickStamp>;
        let mut historical_phase_chunks = BTreeSet::<ChunkKey>::new();
        let mut historical_pins = BTreeMap::<(ChunkKey, PeerGeneration, u64), pumpkin_cluster::chunks::ChunkSubscription>::new();

        let mut canonical_graph_diagnostics = HashSet::<TickStamp>::new();
        let mut canonical_invalid_ticks = BTreeSet::<TickStamp>::new();
        let mut canonical_scopes = BTreeMap::<TickStamp, OwnerTickScope>::new();
        let mut realm_completion = RealmCompletionLedger::default();
        let mut local_realm_issued = BTreeMap::<TickStamp, BTreeMap<RequirementKey, BTreeSet<pumpkin_cluster::canonical_action::ActionId>>>::new();
        let mut pending_realm_finishes = BTreeMap::<
            TickStamp,
            Vec<(PeerGeneration, pumpkin_cluster::realm_completion::HolisticFinish)>,
        >::new();
        let mut pending_realm_finish_count = 0_usize;
        let mut pending_inbound_batches = VecDeque::<PendingInboundBatch>::new();
        let mut pending_inbound_finishes = VecDeque::<(PeerGeneration, TickFinish)>::new();
        let mut pending_action_inputs = Vec::new();
        let mut mining_chunk_needs = HashMap::<pumpkin_cluster::canonical_action::ActionId, BTreeMap<ChunkKey, u64>>::new();
        let mut frozen_mining_work = HashMap::<pumpkin_cluster::canonical_action::ActionId, super::cluster_world_mining_context::FrozenMiningInput>::new();
        let mut frozen_proposal_work = HashMap::<pumpkin_cluster::canonical_action::ActionId, super::cluster_action_work::FrozenProposalWork>::new();
        let mut pending_inventory_open_sources = HashMap::<pumpkin_cluster::canonical_action::ActionId, Arc<super::cluster_invsee::InventoryOpenSource>>::new();
        #[cfg(feature = "cluster-e2e")]
        let mut inventory_scenario_source = super::cluster_inventory_scenario_source::InventoryScenarioSource::default();
        #[cfg(feature = "cluster-e2e")]
        let mut pending_inventory_scenario_requests = VecDeque::new();
        #[cfg(feature = "cluster-e2e")]
        let mut inventory_scenario_issued = BTreeMap::<pumpkin_cluster::canonical_action::ActionRef, (pumpkin_cluster::canonical_action::ActionProposal, Vec<pumpkin_cluster::canonical_action::ActionRef>)>::new();
        let mut completed_callback_work = HashMap::<pumpkin_cluster::canonical_action::ActionRef, super::cluster_world_callback_work::CompletedCallbackWork>::new();
        let mut callback_input_order = HashMap::<pumpkin_cluster::canonical_action::ActionRef, pumpkin_cluster::canonical_action::ActionId>::new();
        let mut action_issue_clock = pumpkin_cluster::action_issue::ActionIssueClock::default();
        let mut entity_origin_allocator = None::<pumpkin_cluster::entity_origin::EntityOriginAllocator>;
        let mut last_item_lifecycle_tick = None;
        let mut pending_item_lifecycle_tick = None;
        let mut item_lifecycle_page = None::<Vec<pumpkin_cluster::canonical_action::Action>>;
        let mut item_lifecycle_frame = None::<pumpkin_cluster::actor_tick_frame::FrozenActorTickFrame<(ChunkKey, pumpkin_cluster::entity_body::EntityBody)>>;
        let mut living_tick_frame = None::<pumpkin_cluster::actor_tick_frame::FrozenActorTickFrame<super::cluster_living_tick::FrozenLivingTickJob>>;
        let mut living_chunk_pins = super::cluster_actor_chunk_pin::ActorChunkPins::new(0).unwrap();
        let mut living_simulation_pins = HashMap::new();
        let mut pending_living_page = None::<super::cluster_living_tick::LivingEmissionPage>;
        let mut pending_living_tick = None::<TickStamp>;
        let mut pending_living_capture = None::<super::cluster_living_tick::FrozenLivingCaptureInputs>;
        let mut last_living_tick = None::<TickStamp>;
        let mut living_tick_emission_failure = None;
        let mut living_capture_diagnostics = HashMap::<std::mem::Discriminant<pumpkin_cluster::canonical_action::ApplyFailure>, std::time::Instant>::new();
        let mut living_hazard_diagnostics = HashMap::<(pumpkin_cluster::actor::ActorId,u64),std::time::Instant>::new();
        let mut object_tick_frame = None::<pumpkin_cluster::actor_tick_frame::FrozenActorTickFrame<super::cluster_object_tick::FrozenObjectTickJob>>;
        let mut projectile_tick_frame = None::<pumpkin_cluster::actor_tick_frame::FrozenActorTickFrame<super::cluster_projectile_source::FrozenProjectileTickJob>>;
        let mut pending_projectile_page = None::<super::cluster_projectile_source::ProjectileEmissionPage>;
        let mut pending_projectile_tick = None::<TickStamp>;
        let mut pending_projectile_cut_keys = BTreeSet::<ChunkKey>::new();
        let mut last_projectile_tick = None::<TickStamp>;
        let mut projectile_tick_failure = None;
        let mut pending_object_page = None::<super::cluster_object_tick::ObjectEmissionPage>;
        let mut object_terminal_reference = None::<pumpkin_cluster::canonical_action::ActionRef>;
        let mut object_terminal_inflight = false;
        let mut completed_object_terminal = None::<super::cluster_world_callback_work::CompletedCallbackWork>;

        let mut pending_object_tick = None::<TickStamp>;
        let mut last_object_tick = None::<TickStamp>;
        let mut object_tick_emission_failure = None;
        let mut retained_internal_actors = pumpkin_cluster::actor_source_retention::ActorSourceRetention::default();
        let mut callback_work = super::cluster_world_callback_work::CallbackWorkOwner::new();
        let mut environment_source_bank = None::<super::cluster_world_environment_bank::EnvironmentSourceBank>;
        let mut environment_source_input = None::<super::cluster_world_environment_bank::FrozenEnvironmentSourceInput>;
        let mut last_environment_source_tick = None::<TickStamp>;
        let mut environment_source_diagnostic = None::<std::time::Instant>;
        let mut environment_source_fault = None::<TickStamp>;
        let mut last_item_lifecycle_warning = None::<std::time::Instant>;
        let mut canonical_rejections = HashSet::new();
        let mut rejected_legacy_admin_peers = HashSet::new();
        let mut actors = super::cluster_world_actor::OwnedActors::default();
        let mut entity_client_ids = super::cluster_entity_projection::OwnerEntityClientIds::new(
            crate::entity::Entity::try_reserve_ids(65_536),
            65_536,
        );
        let mut entity_presentation = super::cluster_entity_projection::OwnerBodyPresentation::default();
        let mut remote_player_presentation = super::cluster_remote_player_presentation::OwnedRemotePlayerPresentation::new(&server, outbound.clone());
        let mut player_presentation = super::cluster_player_projection::OwnedPlayerClientProjection::default();
        let mut item_spatial = pumpkin_cluster::item_spatial::OwnedItemSpatial::default();
        let mut pending_item_boundaries = BTreeMap::new();
        let mut physics_source = None::<super::cluster_item_physics_source::FrozenPhysicsSource>;
        let mut physics_page = None::<super::cluster_item_physics_source::PhysicsEmissionPage>;
        let mut pending_physics_tick = None::<TickStamp>;
        let mut last_physics_tick = None::<TickStamp>;
        let mut physics_failure = None::<pumpkin_cluster::canonical_action::ApplyFailure>;

        let mut item_contact_frame = None::<super::cluster_item_contacts::ItemContactFrame>;
        let mut pending_item_contact_tick = None::<TickStamp>;
        let mut item_contact_page = None::<Vec<pumpkin_cluster::canonical_action::Action>>;
        let mut item_contact_failure = None::<pumpkin_cluster::canonical_action::ApplyFailure>;

        let mut last_item_contact_tick = None::<TickStamp>;
        let mut living = super::cluster_world_actor::OwnedLivingActors::default();
        let mut statistics = super::cluster_actor_statistics::OwnedActorStatistics::with_publication(
            server.cluster_actor_statistics.clone(),
        );
        let mut rejected_actor_intents = HashSet::new();
        let mut pending_actor_intents = VecDeque::<pumpkin_cluster::controller::ControllerActorIntent>::new();
        let mut pending_vehicle_controls = VecDeque::<pumpkin_cluster::vehicle_wire::VehicleControlFrame>::new();
        let mut frozen_vehicle_control = None::<super::cluster_vehicle_control::FrozenVehicleControl>;
        let mut pending_vehicle_page = None::<(Vec<pumpkin_cluster::canonical_action::Action>,
            pumpkin_cluster::actor_action_scope::ActorActionDraft,
            pumpkin_cluster::vehicle_control::VehicleControllerFrame)>;
        let mut vehicle_idle_frame = None::<pumpkin_cluster::actor_tick_frame::FrozenActorTickFrame<super::cluster_vehicle_control::FrozenVehicleControl>>;
        let mut vehicle_idle_page = None::<Vec<(Vec<pumpkin_cluster::canonical_action::Action>,
            pumpkin_cluster::actor_action_scope::ActorActionDraft, pumpkin_cluster::vehicle_control::VehicleControllerFrame)>>;
        let mut last_vehicle_idle_tick = None::<TickStamp>;
        let mut vehicle_controllers = pumpkin_cluster::vehicle_control::VehicleControllerBank::default();
        let mut vehicle_source_diagnostics = HashSet::new();
        let mut vehicle_chunk_pins = super::cluster_actor_chunk_pin::ActorChunkPins::new(2).unwrap();
        let mut vehicle_idle_pins = BTreeMap::<pumpkin_cluster::protocol::GlobalEntityId,
            (pumpkin_cluster::entity_body::EntityBirth, super::cluster_actor_chunk_pin::ActorChunkPin)>::new();
        let mut pending_vehicle_boundaries = BTreeMap::<pumpkin_cluster::protocol::GlobalEntityId,
            pumpkin_cluster::canonical_action::ActionRef>::new();
        let mut retired_actor_install_generations = HashMap::<GlobalPlayerId, u64>::new();
        let mut inventory = inventory;
        let mut pending_imports = HashMap::<
            ChunkKey,
            (ChunkAdmissionIdentity, super::cluster_world_inventory::CommittedInventoryImport),
        >::new();
        let mut pending_fresh = HashMap::<ChunkKey, u64>::new();
        let mut fresh_diagnostics = HashSet::<(ChunkKey, u64)>::new();
        let mut deferred_chunk_exports = VecDeque::new();
        let mut deferred_inventory_lifecycle = VecDeque::new();
        let mut deferred_inventory_actions = VecDeque::new();
        let mut deferred_inventory_screens = VecDeque::new();
        let mut deferred_inventory_menus = VecDeque::new();
        let mut deferred_inventory_snapshots = VecDeque::new();
        let mut inventory_capture_barrier = InventoryCaptureBarrier::default();
        let mut administration =
            super::cluster_world_administration::AdministrativeOwner::attach(&server);
        let mut block_actions = super::cluster_world_block_actions::BlockActionOwner::new();
        let mut item_actions =
            super::cluster_world_item_actions::ItemActionOwner::attach(&server);
        let mut canonical_pages = OwnerCanonicalPages::new();
        let mut pending_inbound_bytes = 0usize;
        let mut world_rules_revision = 0usize;
        let mut world_border_revision = 0u64;
        let mut world_morning_revision = 0u64;
        let mut acquisition_source = OwnerAcquisitionBank::default();
        let mut holistic_outbox = HolisticOutbox::new(outbound.clone(), drop_request.clone());
        let mut settlement =
            super::cluster_world_settlement::SettlementOwner::new(local, outbound, drop_request);
        let mut observed_drops = HashMap::<ServerId, u64>::new();
        let mut pending_drains = Vec::<PendingActorDrain>::new();
        let mut last_local_source_tick = None;
        let (player_admin_tx, mut player_admin_rx) = mpsc::channel(256);
        server.cluster_player_admin.attach_action_route(player_admin_tx);
        let mut canonical_player_admin = HashMap::new();
        let mut canonical_commands = HashMap::<pumpkin_cluster::canonical_action::ActionRef, (PeerGeneration, [u8; 16])>::new();
        let mut canonical_command_needs = HashMap::<pumpkin_cluster::canonical_action::ActionRef, (ChunkKey, u64)>::new();
        let mut pending_controller_statistics = VecDeque::<super::cluster_living_motion::FrozenMotionAction>::new();
        let mut rejected_controller_statistics = HashSet::new();
        let mut command_observations = VecDeque::new();
        let mut pending_admin_inputs = VecDeque::new();
        let mut pending_admin_needs = HashMap::<
            pumpkin_cluster::canonical_action::ActionId,
            (ChunkKey, u64),
        >::new();
        let (admin_chunk_ready_tx, mut admin_chunk_ready_rx) = mpsc::channel::<pumpkin_world::level::OwnerChunkReady>(1_024);
        let mut ready_admin_needs = HashMap::<(ChunkKey, u64), u64>::new();
        let mut pending_admin_need_releases = VecDeque::new();
        let mut next_admin_need_lease = 0_u64;
        let mut combat = super::cluster_combat_apply::CombatApplyState::new();
        let mut acceptance_delays = AcceptanceDelayTracker::default();
        let mut local_finishes = LocalFinishes::default();
        let acceptance_clock = std::time::Instant::now();
        let mut pending_boundaries = HashMap::new();
        let mut promoted_boundaries = HashMap::new();
        let mut pending_replica_boundaries = HashMap::new();
        let mut boundary_leaves = HashMap::new();
        let mut replica_boundary_diagnostics = HashSet::new();
        let mut pending_player_admin = VecDeque::<
            super::cluster_player_admin::PlayerAdministrativeResolution,
        >::new();
        let mut pulse = tokio::time::interval(std::time::Duration::from_millis(10));

        loop {
            inventory.retry_canonical_closes(&server);
            while let Some(observation) = command_observations.pop_front() {
                if let Err(observation) = server.cluster_player_commands.retry_observation(observation) {
                    command_observations.push_front(observation);
                    break;
                }
            }
            let mut command_ground_outcomes = BTreeMap::<TickStamp, Vec<(pumpkin_cluster::canonical_action::ActionRef, bool)>>::new();
            let mut pending_local_tick = None;
            tokio::select! {
                import = stats_import_rx.recv() => {
                    if let Some(import) = import {
                        statistics.import(import);
                    }
                }
                retire = stats_retire_rx.recv() => {
                    if let Some(retire) = retire {
                        let valid_retirement = statistics.incarnation(retire.actor) == Some(retire.incarnation)
                            && inventory.player_install_generation(retire.actor) == Some(retire.install_generation)
                            && inventory.player_lease_for_gid(retire.actor).is_some_and(|lease| {
                                lease.key.0 == retire.player_key && lease.incarnation == retire.incarnation
                            });
                        if valid_retirement {
                        note_terminal_actor_generation(&mut retired_actor_install_generations, retire.actor, retire.install_generation);
                        let before = pending_actor_intents.len();
                        pending_actor_intents.retain(|intent| {
                            intent.actor != retire.actor
                                || intent.owner_install_generation != retire.install_generation
                        });
                        for _ in pending_actor_intents.len()..before {
                            server.cluster_controller.return_actor_intent_credit();
                        }
                        }
                        if valid_retirement {
                            if let Some(lease) = inventory.player_lease_for_gid(retire.actor)
                                && lease.incarnation == retire.incarnation
                            {
                                pending_avatar_retirements.entry(lease).or_insert((retire, None));
                            }
                            block_actions.retire(retire.actor);
                            pending_boundaries.retain(|(gid, _, _, _, _, _), _| *gid != retire.actor);
                            promoted_boundaries.retain(|(gid, _, _, _, _, _), _| *gid != retire.actor);
                        }
                    }
                }
                import = living_import_rx.recv() => {
                    if let Some(import) = import {
                        let _ = living.install(import.actor, import.state);
                    }
                }
                import = admission_import_rx.recv() => {
                    if let Some(import) = import {
                        let outcome = install_actor_admission(&mut actors, &mut statistics, &mut living, &mut inventory, import);
                        if !outcome.accepted {
                            note_terminal_actor_generation(&mut retired_actor_install_generations, outcome.actor, outcome.install_generation);
                        }
                    }
                }
                action = action_rx.recv(), if pending_action_inputs.len() + canonical_queued.values().map(Vec::len).sum::<usize>() < 4096 => {
                    if let Some(action) = action {
                        pending_action_inputs.push(action);
                    }
                }
                binding = replica_binding_rx.recv() => {
                    if let Some(binding) = binding {
                        if binding.retired || server.cluster_peer_incarnations.load().get(&binding.source.server) == Some(&binding.source.generation) {
                            actors.update_remote_client_binding(binding);
                        }
                    }
                }
                input = vehicle_control_rx.recv(), if pending_vehicle_controls.len() < 1024
                    && server.cluster_peer_incarnations.load().contains_key(&local) => {
                    if let Some(input) = input
                        && let Some(generation) = server.cluster_peer_incarnations.load().get(&local).copied()
                    {
                        let source = PeerGeneration { server: local, generation };
                        pending_vehicle_controls.push_back(pumpkin_cluster::vehicle_wire::VehicleControlFrame {
                            source, recipient: source, input,
                        });
                    }
                }
                parcel = world_rx.recv(), if pending_vehicle_controls.len() < 1024 && pending_inbound_batches.len() < 4096 && canonical_pages.has_input_capacity() => {
                    let Some(parcel) = parcel else { break };
                    if parcel.header.kind == StreamKind::EntityWorld
                        && pumpkin_cluster::vehicle_wire::recognizes(&parcel.bytes) {
                        if let Some(frame) = pumpkin_cluster::vehicle_wire::decode(&parcel.bytes)
                            && frame.source.server == parcel.peer && frame.source.generation == parcel.generation
                            && frame.recipient.server == local
                            && server.cluster_peer_incarnations.load().get(&local) == Some(&frame.recipient.generation)
                        {
                            pending_vehicle_controls.push_back(frame);
                        }
                    } else if parcel.header.kind == StreamKind::PlayerBoundary {
                        ingest_boundary(
                            &server,
                            &mut pending_boundaries,
                            &mut promoted_boundaries,
                            &mut pending_replica_boundaries,
                            &mut boundary_leaves,
                            &mut replica_boundary_diagnostics,
                            parcel,
                        );
                    } else if matches!(parcel.header.kind, StreamKind::PlayerWorld | StreamKind::EntityWorld)
                        && let Some((batch, encoded_bytes)) = canonical_pages.decode_or_defer(&server, &parcel, pending_inbound_bytes, &mut holistic_outbox)
                    {
                        let pending = PendingInboundBatch {
                            encoded_bytes,
                            source: PeerGeneration {
                                server: parcel.peer,
                                generation: parcel.generation,
                            },
                            batch,
                        };
                        if pending.batch.canonical_actions.is_empty() {
                            ingest_decoded_batch(
                                &server,
                                None,
                                &mut settlement,
                                &mut buckets,
                                &mut rejected_legacy_admin_peers,
                                local,
                                pending,
                            );
                        } else if pending_inbound_batches.len() < 4_096 {
                            pending_inbound_bytes += pending.encoded_bytes;
                            pending_inbound_batches.push_back(pending);
                        } else {
                            canonical_invalid_ticks.insert(pending.batch.tick);
                            holistic_outbox.fail_peer(parcel.peer, "action inbox capacity exhausted");
                        }
                    }
                }
                parcel = combat_rx.recv(), if pending_inbound_batches.len() < 4096 && canonical_pages.has_input_capacity() => {
                    let Some(parcel) = parcel else { break };
                    if parcel.header.kind == StreamKind::PlayerCombat
                        && let Some((batch, encoded_bytes)) = canonical_pages.decode_or_defer(&server, &parcel, pending_inbound_bytes, &mut holistic_outbox)
                    {
                        let pending = PendingInboundBatch {
                            encoded_bytes,
                            source: PeerGeneration {
                                server: parcel.peer,
                                generation: parcel.generation,
                            },
                            batch,
                        };
                        if pending.batch.canonical_actions.is_empty() {
                            ingest_decoded_batch(
                                &server,
                                None,
                                &mut settlement,
                                &mut buckets,
                                &mut rejected_legacy_admin_peers,
                                local,
                                pending,
                            );
                        } else if pending_inbound_batches.len() < 4_096 {
                            pending_inbound_bytes += pending.encoded_bytes;
                            pending_inbound_batches.push_back(pending);
                        } else {
                            canonical_invalid_ticks.insert(pending.batch.tick);
                            holistic_outbox.fail_peer(parcel.peer, "combat action inbox capacity exhausted");
                        }
                    }
                }
                local_tick = local_tick_rx.recv() => {
                    let Some(local_tick) = local_tick else { break };
                    pending_local_tick = Some(local_tick);
                }
                completion = completion_rx.recv() => {
                    let Some((source_identity, completion)) = completion else { break };
                    let source = source_identity.server;
                    match completion {
                        CompletionMessage::Finish(finish) => {
                            let names_canonical = finish.chunks.iter().any(|chunk| {
                                chunk.actions.iter().any(|action| action.family & 0x80 != 0)
                            });
                            if !names_canonical {
                                if server.cluster_peer_generations.load().get(&source) == Some(&source_identity.generation) {
                                    buckets.note_authenticated_finish(source, source_identity.generation, local, finish, |chunk| holders(&server, chunk), |entity| super::cluster_entity_apply::current_entity_owner(&server, entity));
                                }
                            } else if pending_inbound_finishes.len() < 4_096 {
                                pending_inbound_finishes.push_back((source_identity, finish));
                            } else {
                                holistic_outbox.fail_peer(source, "tick finish inbox capacity exhausted");
                                canonical_invalid_ticks.insert(finish.tick);
                            }
                        }
                        CompletionMessage::RealmFinish(finish) => {
                            if pending_realm_finish_count < 4_096 {
                                pending_realm_finishes
                                    .entry(finish.tick)
                                    .or_default()
                                    .push((source_identity, finish));
                                pending_realm_finish_count += 1;
                            } else {
                                holistic_outbox.fail_peer(source, "realm finish inbox capacity exhausted");
                                if canonical_graph_diagnostics.len() < 1_024
                                    && canonical_graph_diagnostics.insert(finish.tick)
                                {
                                    tracing::error!(tick = finish.tick.0, "cluster realm finish inbox capacity exhausted");
                                }
                            }
                        }
                        CompletionMessage::DropReport(report) => {
                            settlement.peer_dropped(
                                &server,
                                &mut buckets,
                                report.dropped,
                            );
                            buckets.recover_drop(report);
                        }
                        CompletionMessage::Dependency(notice) => {
                            settlement.note_dependency(
                                &server,
                                &mut buckets,
                                source,
                                notice,
                            );
                        }
                        CompletionMessage::Settlement(publication) => {
                            settlement.observe_settlement(
                                &server,
                                source,
                                publication,
                                &mut buckets,
                            );
                        }
                    }
                }
                changed = peer_drop_rx.changed() => {
                    if changed.is_err() { break; }
                    let dropped = peer_drop_rx.borrow_and_update().clone();
                    for (peer, epoch) in dropped.iter() {
                        if observed_drops.get(peer).is_none_or(|previous| previous < epoch) {
                            holistic_outbox.peer_dropped(*peer);
                            pending_replica_boundaries.retain(|gid, _| gid.server != *peer);
                            boundary_leaves.retain(|gid, _| gid.server != *peer);
                            replica_boundary_diagnostics.retain(|(gid, _)| gid.server != *peer);
                            pending_boundaries.retain(|(gid, _, _, _, _, _), _| gid.server != *peer);
                            promoted_boundaries.retain(|(gid, _, _, _, _, _), _| gid.server != *peer);
                            observed_drops.insert(*peer, *epoch);
                        }
                    }
                    drop_complete_tx.send_replace(dropped);
                }
                request = drain_rx.recv(), if pending_drains.len() < 256 => {
                    if let Some(request) = request {
                        if request.gid != request.lease.gid
                            || inventory.player_lease_for_gid(request.gid) != Some(request.lease)
                            || actors.local_client_entity_id(request.lease, request.install_generation).is_none()
                        {
                            let _ = request.respond.send(ActorDrainStatus::StalePlayer);
                            continue;
                        }
                        let leave_applied = !request.logout;
                        pending_drains.push(PendingActorDrain {
                            request,
                            admitted_tick: None,
                            leave_applied,
                        });
                    }
                }
                _ = pulse.tick() => {}
            }
            let millis = std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .map(|age| age.as_millis() as i64)
                .unwrap_or(0);
            let resolution_tick = local_finishes.resolution_tick(super::cluster::disciplined_tick_stamp(millis));
            for _ in 0..256 {
                let Ok(admission) = fresh_rx.try_recv() else { break; };
                match admission {
                    pumpkin_world::level::FreshChunkAdmission::Prepare {
                        world_key,
                        dimension,
                        position,
                        generation,
                        image,
                        palettes,
                        respond,
                        wake,
                    } => {
                        let chunk = ChunkKey::new(
                            pumpkin_cluster::protocol::WorldId::new(dimension, world_key),
                            position.x,
                            position.y,
                        );
                        let epoch = server.cluster_peer_incarnations.load().get(&local).copied();
                        let scheduled_queue = epoch.filter(|_| server.is_cluster_primary()).and_then(|generation| scheduled_queue_from_image(
                            &image,
                            chunk,
                            TickStamp(resolution_tick.0.wrapping_sub(1)),
                            PeerGeneration { server: local, generation },
                        ));
                        let block_entities = block_entities_from_image(&image, chunk);
                        let owned_image = pumpkin_world::level::OwnedChunkImage::decode(&image);
                        let light_sections = owned_image.as_ref().and_then(pumpkin_world::level::OwnedChunkImage::light_sections);
                        let min_y = palettes.min_y;
                        let section_count = palettes.blocks.len();
                        let biome_count = palettes.biomes.len();
                        let owned_blocks = super::cluster_chunk_state::OwnedChunkBlocks::from_palettes(
                            min_y,
                            palettes.blocks,
                            palettes.biomes,
                        );
                        let prepared_fresh_inventory = block_entities.as_ref()
                            .ok_or(pumpkin_cluster::inventory::InventoryApplyError::Rejected)
                            .and_then(|entities| block_inventory_root_images(chunk, &owned_blocks, entities))
                            .and_then(|images| inventory.prepare_fresh_block_roots(chunk, images));
                        let mut fresh_graph_candidate = canonical_graphs.clone();
                        let fresh_inventory_ready = epoch.is_some_and(|epoch| {
                            fresh_graph_candidate.install_chunk_frontier(chunk, pumpkin_cluster::action_graph::ChunkGroundFrontier {
                                epoch, settled_through: TickStamp(resolution_tick.0.wrapping_sub(1)),
                            }).is_ok()
                        }) && prepared_fresh_inventory.as_ref().is_ok_and(|prepared| {
                            prepared.block_root_anchors().into_iter().all(|(target, cell)| {
                                fresh_graph_candidate.seed_chunk_target_anchor(target, chunk, Some(cell)).is_ok()
                            }) && block_entities.as_ref().is_some_and(|entities| entities.iter().all(|(position, cell)| {
                                let target = Target::Chunk { key: chunk, part: pumpkin_cluster::canonical_action::ChunkPart::BlockEntity(*position), inventory: None };
                                fresh_graph_candidate.seed_chunk_target_anchor(target, chunk, Some(cell.clone())).is_ok()
                            }))
                        });
                        let eligible = epoch.is_some()
                            && fresh_inventory_ready
                            && owned_image.is_some()
                            && light_sections.is_some()
                            && scheduled_queue.is_some()
                            && block_entities.as_ref().is_some_and(|entities| {
                                block_entities_match_blocks(chunk, &owned_blocks, entities)
                            })
                            && !chunk_blocks.contains(chunk)
                            && !pending_fresh.contains_key(&chunk)
                            && server.cluster_chunk_directory.load_full()
                                .holders_of(&chunk)
                                .is_none_or(|holders| holders.is_empty())
                            && buckets.export_pending_chunk(chunk).is_empty()
                            && !canonical_graphs.ticks.values().any(|graph| {
                                graph.actions.values().any(|node| {
                                    node.realms.iter().any(|realm| realm.held_by_chunk(chunk))
                                })
                            })
                            && server.worlds.load().iter().any(|world| {
                                world.level.cluster_world_key == world_key
                                    && world.level.cluster_dimension == dimension
                                    && world.level.cluster_genesis_required
                                    && world.dimension.min_y == min_y
                                    && world.dimension.height as usize / 16 == section_count
                                    && section_count == biome_count
                            })
                            && pumpkin_world::level::cluster_snapshot_layout_matches(
                                &image,
                                chunk.x(),
                                chunk.z(),
                                min_y,
                                section_count,
                            );
                        if eligible && claim_primary_chunk_before_install(&server, chunk) {
                            let frontier = pumpkin_cluster::action_graph::ChunkGroundFrontier {
                                epoch: epoch.unwrap(),
                                settled_through: TickStamp(resolution_tick.0.wrapping_sub(1)),
                            };
                            if canonical_graphs.install_chunk_frontier(chunk, frontier).is_ok() {
                                canonical_graphs = fresh_graph_candidate;
                                let committed_inventory = inventory.commit_prepared_from_chunk(prepared_fresh_inventory.expect("validated fresh block roots"))
                                    .expect("private fresh block root commit");
                                chunk_blocks.install(chunk, owned_blocks);
                                let occupancy = pumpkin_cluster::canonical_action::Cell {
                                    revision: 0,
                                    value: pumpkin_cluster::canonical_action::Value::ActorOccupancy(
                                        pumpkin_cluster::actor_occupancy::ActorOccupancyRoot::empty()),
                                };
                                let occupancy_target = Target::Chunk { key: chunk,
                                    part: pumpkin_cluster::canonical_action::ChunkPart::ActorOccupancy, inventory: None };
                                assert!(canonical_graphs.seed_chunk_target_anchor(occupancy_target, chunk, Some(occupancy.clone())).is_ok());
                                assert!(chunk_blocks.install_occupancy_phase(false, chunk, occupancy.clone()));
                                assert!(chunk_blocks.install_occupancy_phase(true, chunk, occupancy));
                                for (section_y, cell) in chunk_blocks.initial_block_sections(chunk).expect("private admission palettes") {
                                    let target = Target::Chunk { key: chunk, part: pumpkin_cluster::canonical_action::ChunkPart::BlockSection(section_y), inventory: None };
                                    assert!(canonical_graphs.seed_chunk_target_anchor(target, chunk, Some(cell.clone())).is_ok());
                                    chunk_blocks.install_block_section_phase(false, chunk, section_y, cell.clone());
                                    chunk_blocks.install_block_section_phase(true, chunk, section_y, cell);
                                }
                                for (section_y, sky, block) in light_sections.expect("validated admission lighting") {
                                    let cell = pumpkin_cluster::canonical_action::Cell { revision: 0, value: pumpkin_cluster::canonical_action::Value::LightSection { sky, block } };
                                    let target = Target::Chunk { key: chunk, part: pumpkin_cluster::canonical_action::ChunkPart::LightSection(section_y), inventory: None };
                                    assert!(canonical_graphs.seed_chunk_target_anchor(target, chunk, Some(cell.clone())).is_ok());
                                    chunk_blocks.install_light_section_phase(false, chunk, section_y, Some(cell.clone()));
                                    chunk_blocks.install_light_section_phase(true, chunk, section_y, Some(cell));
                                }
                                assert!(chunk_blocks.install_scheduled_queue(chunk, scheduled_queue.unwrap()));
                                for (position, cell) in block_entities.unwrap() {
                                    chunk_blocks.install_block_entity_phase(false, chunk, position, Some(cell.clone()));
                                    chunk_blocks.install_block_entity_phase(true, chunk, position, Some(cell));
                                }
                            chunk_images.insert(chunk, owned_image.expect("validated immutable image"));
                                chunk_lighting.remove(&chunk);
                                chunk_inhabited_delta.remove(&chunk);
                                buckets.quarantine_chunk(chunk);
                                pending_fresh.insert(chunk, generation);
                                if respond.send(true).is_err() {
                                    inventory.rollback_fresh_block_roots(chunk);
                                    pending_fresh.remove(&chunk);
                                    super::cluster_chunk_presentation::retire(&server, chunk);
                                    chunk_blocks.remove(chunk);
                                    chunk_images.remove(&chunk);
                                    canonical_graphs.chunk_frontiers.remove(&chunk);
                                    canonical_graphs.ground_anchors.retain(|target, _| !matches!(target, Target::Chunk { key, .. } if *key == chunk));
                                    buckets.ready_chunk(chunk);
                                    for world in server.worlds.load().iter().filter(|world| {
                                        world.level.cluster_world_key == chunk.world.key
                                            && world.level.cluster_dimension == chunk.world.dimension
                                    }) {
                                        world.level.release_primary_chunk_action_claim(position);
                                        world.level.retire_primary_chunk_fence(position);
                                    }
                                } else {
                                    let _ = inventory.publish_committed_from_chunk(&server, committed_inventory);
                                }
                                let _ = wake.try_send(());
                                continue;
                            }
                        }
                        if !chunk_blocks.contains(chunk) {
                            for world in server.worlds.load().iter().filter(|world| {
                                world.level.cluster_world_key == chunk.world.key
                                    && world.level.cluster_dimension == chunk.world.dimension
                            }) {
                                world.level.release_primary_chunk_action_claim(position);
                                world.level.retire_primary_chunk_fence(position);
                            }
                        }
                        let _ = respond.send(false);
                        let _ = wake.try_send(());
                    }
                    pumpkin_world::level::FreshChunkAdmission::Published {
                        world_key,
                        dimension,
                        position,
                        generation,
                    } => {
                        let chunk = ChunkKey::new(
                            pumpkin_cluster::protocol::WorldId::new(dimension, world_key),
                            position.x,
                            position.y,
                        );
                        if pending_fresh.get(&chunk) == Some(&generation) {
                            pending_fresh.remove(&chunk);
                            if let Some(image) = chunk_images.get(&chunk) {
                                super::cluster_chunk_presentation::publish(&server, chunk, &chunk_blocks, image, chunk_lighting.get(&chunk));
                            }
                            if let Some(snapshot) = chunk_blocks.snapshot(chunk) {
                                server.cluster_owned_chunk_blocks.rcu(|current| {
                                    let mut next = (**current).clone();
                                    next.insert(chunk, snapshot.clone());
                                    Arc::new(next)
                                });
                                buckets.ready_chunk(chunk);
                                if server.is_cluster_primary() {
                                    primary_change_serial = primary_change_serial.wrapping_add(1);
                                    primary_dirty_revisions.insert(chunk, primary_change_serial);
                                    dirty_primary_chunks.insert(chunk);
                                    pending_primary_save = Some(resolution_tick);
                                }
                            }
                        } else if fresh_diagnostics.len() < 1_024
                            && fresh_diagnostics.insert((chunk, generation))
                        {
                            tracing::error!(x = chunk.x(), z = chunk.z(), generation, "cluster fresh chunk publication did not match an owner admission");
                        }
                    }
                    pumpkin_world::level::FreshChunkAdmission::Aborted {
                        world_key,
                        dimension,
                        position,
                        generation,
                    } => {
                        let chunk = ChunkKey::new(
                            pumpkin_cluster::protocol::WorldId::new(dimension, world_key),
                            position.x,
                            position.y,
                        );
                        if pending_fresh.get(&chunk) == Some(&generation) {
                            pending_fresh.remove(&chunk);
                            super::cluster_chunk_presentation::retire(&server, chunk);
                            chunk_blocks.remove(chunk);
                            chunk_images.remove(&chunk);
                            canonical_graphs.chunk_frontiers.remove(&chunk);
                            buckets.ready_chunk(chunk);
                            for world in server.worlds.load().iter().filter(|world| {
                                world.level.cluster_world_key == chunk.world.key
                                    && world.level.cluster_dimension == chunk.world.dimension
                            }) {
                                world.level.release_primary_chunk_action_claim(position);
                                world.level.retire_primary_chunk_fence(position);
                            }
                        }
                    }
                }
            }
            canonical_scopes
                .entry(resolution_tick)
                .or_insert_with(|| OwnerTickScope::freeze(&server, &inventory, &chunk_blocks, &actors, &living, resolution_tick));
            if entity_origin_allocator.is_none() {
                if let Ok(source) = canonical_scopes[&resolution_tick].source(local) {
                    entity_origin_allocator = pumpkin_cluster::entity_origin::EntityOriginAllocator::new(source).ok();
                }
            }
            canonical_pages.retain_sources(&server.cluster_peer_generations.load());
            for _ in 0..canonical_pages.deferred.len() {
                if pending_inbound_batches.len() >= 4096 { break; }
                let Some(parcel) = canonical_pages.deferred.pop_front() else { break; };
                canonical_pages.deferred_bytes -= parcel.bytes.len();
                if let Some((batch, encoded_bytes)) = canonical_pages.decode_or_defer(&server, &parcel, pending_inbound_bytes, &mut holistic_outbox) {
                    let pending = PendingInboundBatch { source: PeerGeneration { server: parcel.peer, generation: parcel.generation }, encoded_bytes, batch };
                    if pending.batch.canonical_actions.is_empty() {
                        ingest_decoded_batch(&server, None, &mut settlement, &mut buckets, &mut rejected_legacy_admin_peers, local, pending);
                    } else {
                        pending_inbound_bytes += pending.encoded_bytes;
                        pending_inbound_batches.push_back(pending);
                    }
                }
            }
            for _ in 0..pending_inbound_batches.len() {
                let Some(pending) = pending_inbound_batches.pop_front() else { break; };
                pending_inbound_bytes -= pending.encoded_bytes;
                let Some(scope) = canonical_scopes.get(&pending.batch.tick) else {
                    if resolution_tick.is_after(pending.batch.tick) {
                        canonical_pages.retire_tick(pending.batch.tick);
                        canonical_invalid_ticks.insert(pending.batch.tick);
                        if canonical_graph_diagnostics.len() < 1_024
                            && canonical_graph_diagnostics.insert(pending.batch.tick)
                        {
                            tracing::error!(tick = pending.batch.tick.0, "cluster action arrived without its frozen tick scope");
                        }
                    } else {
                        pending_inbound_bytes += pending.encoded_bytes;
                        pending_inbound_batches.push_back(pending);
                    }
                    continue;
                };
                let Ok(logical_source) = scope.authenticated_source(pending.source) else { continue; };
                if server.cluster_peer_generations.load().get(&pending.source.server) != Some(&pending.source.generation) { continue; }
                if pending.source.server != local && pending.batch.canonical_actions.iter().any(|action| {
                    matches!(action.effects.as_slice(), [pumpkin_cluster::canonical_action::Effect::PlayerAvatarLifecycle {
                        intent: pumpkin_cluster::avatar_lifecycle::AvatarLifecycleIntent::Admission { .. }, ..
                    }]) && (!actors.prepare_remote_admission(action, logical_source)
                        || !inventory.record_canonical_admission(action, logical_source))
                }) {
                    pending_inbound_bytes += pending.encoded_bytes;
                        pending_inbound_batches.push_back(pending);
                    continue;
                }
                for action in &pending.batch.canonical_actions {
                    for (tick, scope) in &mut canonical_scopes {
                        scope.record_avatar_declaration(action, *tick);
                    }
                }
                if !inventory.record_canonical_entity_page(&pending.batch.canonical_actions, logical_source) {
                    canonical_invalid_ticks.insert(pending.batch.tick);
                    tracing::error!(tick = pending.batch.tick.0, peer = pending.source.server.0, "cluster entity inventory declaration rejected");
                    continue;
                }
                let scope = canonical_scopes.get(&pending.batch.tick).unwrap();
                ingest_decoded_batch(
                    &server,
                    Some(scope),
                    &mut settlement,
                    &mut buckets,
                    &mut rejected_legacy_admin_peers,
                    local,
                    pending,
                );
            }
            for _ in 0..pending_inbound_finishes.len() {
                let Some((source, finish)) = pending_inbound_finishes.pop_front() else { break; };
                let Some(scope) = canonical_scopes.get(&finish.tick) else {
                    if resolution_tick.is_after(finish.tick) {
                        canonical_invalid_ticks.insert(finish.tick);
                        if canonical_graph_diagnostics.len() < 1_024
                            && canonical_graph_diagnostics.insert(finish.tick)
                        {
                            tracing::error!(tick = finish.tick.0, "cluster finish arrived without its frozen tick scope");
                        }
                    } else {
                        pending_inbound_finishes.push_back((source, finish));
                    }
                    continue;
                };
                if server.cluster_peer_generations.load().get(&source.server) == Some(&source.generation) {
                    buckets.note_authenticated_finish(source.server, source.generation, local, finish, |chunk| scope.legacy_holders(chunk), |entity| super::cluster_entity_apply::current_entity_owner(&server, entity));
                }
            }
            let ready_realm_finishes = pending_realm_finishes
                .keys()
                .copied()
                .filter(|tick| {
                    canonical_scopes.contains_key(tick)
                        || resolution_tick.is_after(*tick)
                            && resolution_tick.distance_since(*tick) >= 600
                })
                .collect::<Vec<_>>();
            for tick in ready_realm_finishes {
                let Some(pending) = pending_realm_finishes.remove(&tick) else {
                    continue;
                };
                pending_realm_finish_count = pending_realm_finish_count.saturating_sub(pending.len());
                let scope = canonical_scopes.get(&tick);
                for (transport, finish) in pending {
                    if finish.acquisition_prefix.is_some() && acquisition_source.observed.len() >= 8192 {
                        pending_realm_finishes.entry(tick).or_default().push((transport, finish));
                        pending_realm_finish_count += 1;
                        continue;
                    }
                    let result = scope
                        .ok_or(RealmCompletionFailure::MissingRequirement)
                        .and_then(|scope| {
                            if scope.full_source_slot != Some(finish.full_source_slot) { return Err(RealmCompletionFailure::InvalidActionRequirements); }
                            scope.authenticated_source(transport)
                        })
                        .and_then(|source| {
                            let prefix = finish.acquisition_prefix.map(|sequence| pumpkin_cluster::acquisition_prefix::AcquisitionPrefix { source, tick: finish.tick, sequence });
                            realm_completion.note_finish(source, finish)?;
                            if let Some(prefix) = prefix { acquisition_source.observed.push_back((source, prefix)); }
                            Ok(())
                        });
                    if let Err(reason) = result {
                        canonical_invalid_ticks.insert(tick);
                        if canonical_graph_diagnostics.len() < 1_024
                            && canonical_graph_diagnostics.insert(tick)
                        {
                            tracing::error!(tick = tick.0, peer = transport.server.0, ?reason, "cluster remote realm finish rejected");
                        }
                    }
                }
            }
            while let Ok(import) = stats_import_rx.try_recv() {
                statistics.import(import);
            }
            while let Ok(retire) = stats_retire_rx.try_recv() {
                let valid_retirement = statistics.incarnation(retire.actor) == Some(retire.incarnation)
                    && inventory.player_install_generation(retire.actor) == Some(retire.install_generation)
                    && inventory.player_lease_for_gid(retire.actor).is_some_and(|lease| {
                        lease.key.0 == retire.player_key && lease.incarnation == retire.incarnation
                    });
                if valid_retirement {
                note_terminal_actor_generation(&mut retired_actor_install_generations, retire.actor, retire.install_generation);
                let before = pending_actor_intents.len();
                pending_actor_intents.retain(|intent| {
                    intent.actor != retire.actor
                        || intent.owner_install_generation != retire.install_generation
                });
                for _ in pending_actor_intents.len()..before {
                    server.cluster_controller.return_actor_intent_credit();
                }
                }
                if valid_retirement {
                    if let Some(lease) = inventory.player_lease_for_gid(retire.actor)
                        && lease.incarnation == retire.incarnation
                    {
                        pending_avatar_retirements.entry(lease).or_insert((retire, None));
                    }
                    block_actions.retire(retire.actor);
                    pending_boundaries.retain(|(gid, _, _, _, _, _), _| *gid != retire.actor);
                    promoted_boundaries.retain(|(gid, _, _, _, _, _), _| *gid != retire.actor);
                }
            }
            while let Ok(import) = living_import_rx.try_recv() {
                let _ = living.install(import.actor, import.state);
            }
            while let Ok(input) = item_spatial_rx.try_recv() {
                let Some(scope) = canonical_scopes.get(&input.sample.tick) else { continue; };
                if scope.transport_generations.get(&input.source) != Some(&input.transport_generation)
                    || input.source != input.sample.issuer.server { continue; }
                let association = chunk_blocks.local_entity_bodies().find_map(|(chunk, cell)| match &cell.value {
                    pumpkin_cluster::canonical_action::Value::Entity(body) if body.id == input.sample.id => Some((chunk, body)),
                    _ => None,
                });
                if let Some((chunk, body)) = association {
                    let issuer = input.sample.issuer;
                    if entity_spatial_issuer_authorized(scope, body, chunk, issuer) {
                        item_spatial.apply(input.sample, body, chunk, issuer);
                    }
                }
            }
            while let Ok(import) = admission_import_rx.try_recv() {
                let outcome = install_actor_admission(&mut actors, &mut statistics, &mut living, &mut inventory, import);
                        if !outcome.accepted {
                            note_terminal_actor_generation(&mut retired_actor_install_generations, outcome.actor, outcome.install_generation);
                        }
            }
            if let Some(local_tick) = pending_local_tick.as_mut() {
                for view in local_tick.view_intents.drain(..) {
                    if !actor_generation_is_terminal(&retired_actor_install_generations, view.actor, view.owner_install_generation) {
                        pending_actor_views.insert(view.actor, view);
                    }
                }
                for intent in local_tick.actor_intents.drain(..) {
                    if actor_generation_is_terminal(&retired_actor_install_generations, intent.actor, intent.owner_install_generation) {
                        server.cluster_controller.return_actor_intent_credit();
                        continue;
                    }
                    assert!(pending_actor_intents.len() < 1_024);
                    pending_actor_intents.push_back(intent);
                }
            }
            let mut actor_drafts = HashMap::<_, pumpkin_cluster::actor_action_scope::ActorActionDraft>::new();
            let source_phase_bank_available = canonical_scopes.get(&resolution_tick)
                .and_then(|scope| scope.full_source_slot)
                .is_some_and(|slot| source_actor_snapshots.contains_key(&slot) || source_actor_snapshots.len() < 2);
            for _ in 0..pending_actor_intents.len() {
                let Some(intent) = pending_actor_intents.pop_front() else { break; };
                if !source_phase_bank_available {
                    pending_actor_intents.push_back(intent);
                    continue;
                }
                if pending_controller_statistics.iter().any(|work| work.lease.gid == intent.actor) {
                    pending_actor_intents.push_back(intent);
                    continue;
                }
                if inventory.player_lease_for_gid(intent.actor).is_some_and(|lease|
                    retained_internal_actors.contains(&(pumpkin_cluster::actor::ActorId::Player(intent.actor), lease.incarnation))) {
                    pending_actor_intents.push_back(intent);
                    continue;
                }
                if pending_controller_statistics.len() >= 1024 {
                    pending_actor_intents.push_front(intent);
                    break;
                }
                let previous_spatial = actors.input_projection(intent.actor);
                if actor_generation_is_terminal(&retired_actor_install_generations, intent.actor, intent.owner_install_generation) {
                    server.cluster_controller.return_actor_intent_credit();
                    continue;
                }
                let installed_generation = inventory.player_install_generation(intent.actor);
                let newer_install_pending = installed_generation.is_none_or(|installed| {
                    let distance = intent.owner_install_generation.wrapping_sub(installed);
                    distance != 0 && distance < (1_u64 << 63)
                });
                if intent.owner_install_generation != 0 && newer_install_pending {
                    pending_actor_intents.push_back(intent);
                    continue;
                }
                if !living.contains(pumpkin_cluster::actor::ActorId::Player(intent.actor))
                    || (!matches!(intent.source, pumpkin_cluster::controller::ControllerActorSource::Initial)
                        && living.phase_state(pumpkin_cluster::actor::ActorId::Player(intent.actor), false).is_none())
                    || (matches!(intent.source, pumpkin_cluster::controller::ControllerActorSource::Initial)
                        && inventory.player_lease_for_gid(intent.actor).and_then(|lease| inventory.prepared_player_admission(lease)).is_none())
                {
                    pending_actor_intents.push_back(intent);
                    continue;
                }
                let admission_id = if matches!(intent.source, pumpkin_cluster::controller::ControllerActorSource::Initial) {
                    let Some(lease) = inventory.player_lease_for_gid(intent.actor) else { pending_actor_intents.push_back(intent); continue; };
                    if pending_action_inputs.len() >= 4096 || !canonical_scopes.get(&resolution_tick).is_some_and(|scope| scope.source(local).is_ok()) {
                        pending_actor_intents.push_back(intent); continue;
                    }
                    let Some(reservation) = action_issue_clock.reserve(pumpkin_cluster::actor::ActorId::Player(intent.actor), lease.incarnation, resolution_tick, 1) else {
                        pending_actor_intents.push_back(intent); continue;
                    };
                    Some(reservation.id(0, 15).unwrap())
                } else { None };
                let position = intent.frame.effective.position;
                let feet = pumpkin_cluster::protocol::BlockPos { x: position[0].floor() as i32, y: position[1].floor() as i32, z: position[2].floor() as i32 };
                let position_chunk = ChunkKey::new(intent.world, feet.x.div_euclid(16), feet.z.div_euclid(16));
                let in_liquid = chunk_blocks.block(position_chunk, feet).is_ok_and(|state| { let state = pumpkin_data::BlockState::from_id(state); state.is_liquid() || state.is_waterlogged() });
                let mounted = living.phase_state(pumpkin_cluster::actor::ActorId::Player(intent.actor), false)
                    .and_then(|state| state.avatar.as_ref())
                    .is_some_and(|avatar| avatar.mount.is_some() && Some(avatar.spatial_epoch) == intent.spatial_epoch);
                let fall_distance = actors.next_environment_fall_distance(&intent, in_liquid, mounted);
                let stance_reservation = if matches!(intent.source, pumpkin_cluster::controller::ControllerActorSource::Client)
                    && previous_spatial.is_some_and(|previous|
                        intent.sneaking.is_some_and(|value| value != previous.sneaking)
                            || intent.sprinting.is_some_and(|value| value != previous.sprinting)
                            || intent.crawling.is_some_and(|value| value != previous.crawling)
                            || intent.swimming.is_some_and(|value| value != previous.swimming)
                            || intent.gliding.is_some_and(|value| value != previous.gliding)
                            || fall_distance != previous.fall_distance)
                {
                    let Some(lease) = inventory.player_lease_for_gid(intent.actor) else { pending_actor_intents.push_back(intent); continue; };
                    if pending_action_inputs.len() >= 4096 || !canonical_scopes.get(&resolution_tick).is_some_and(|scope| scope.source(local).is_ok()) {
                        pending_actor_intents.push_back(intent); continue;
                    }
                    let Some(reservation) = action_issue_clock.reserve(pumpkin_cluster::actor::ActorId::Player(intent.actor), lease.incarnation, resolution_tick, 1) else {
                        pending_actor_intents.push_back(intent); continue;
                    };
                    Some(reservation)
                } else { None };
                let applied = if intent.actor.server != local
                    || intent.owner_install_generation == 0
                    || installed_generation != Some(intent.owner_install_generation)
                    || !inventory.player_lease_for_gid(intent.actor).is_some_and(|lease| {
                        lease.key.0 == intent.player_key
                    })
                {
                        Err(super::cluster_world_actor::ActorSpatialError::MismatchedActor)
                } else if matches!(intent.source, pumpkin_cluster::controller::ControllerActorSource::Initial)
                        && actors.get(intent.actor).is_some()
                    {
                        Err(super::cluster_world_actor::ActorSpatialError::MismatchedActor)
                } else if !living.contains(pumpkin_cluster::actor::ActorId::Player(intent.actor)) {
                    pending_actor_intents.push_back(intent);
                    continue;
                } else if !matches!(intent.source, pumpkin_cluster::controller::ControllerActorSource::Initial)
                    && living.phase_state(pumpkin_cluster::actor::ActorId::Player(intent.actor), false).is_none()
                {
                    pending_actor_intents.push_back(intent);
                    continue;
                } else {
                        if matches!(intent.source, pumpkin_cluster::controller::ControllerActorSource::Client) {
                            actors.capture_controller_input(intent, |chunk, block| {
                                chunk_blocks.rough_biome_id(chunk, block)
                            }).map(|spatial| actors.set_input_fall_distance(intent.actor, fall_distance).unwrap_or(spatial))
                        } else {
                            actors.apply_controller(intent, |chunk, block| {
                                chunk_blocks.rough_biome_id(chunk, block)
                            })
                        }
                };
                match applied {
                    Ok(spatial) => {
                        if matches!(intent.source, pumpkin_cluster::controller::ControllerActorSource::Initial) {
                            let lease = inventory.player_lease_for_gid(intent.actor).expect("admitted controller lease");
                            assert!(living.project_player_spatial(intent.actor, spatial, pumpkin_cluster::canonical_action::SpatialEpoch::Admission { lease, admitted_at: resolution_tick }));
                            actors.stage_initial_spatial(intent.actor);
                            let initial = living.prepared(pumpkin_cluster::actor::ActorId::Player(intent.actor)).unwrap().local.clone();
                            let owner = canonical_scopes.get(&resolution_tick).unwrap().source(local).unwrap();
                            let lifecycle = pumpkin_cluster::avatar_lifecycle::AvatarLifecycleIntent::Admission {
                                lease, install_generation: intent.owner_install_generation, owner, initial,
                                inventory: inventory.prepared_player_admission(lease).expect("prepared exact player inventory admission"),
                            };
                            if let Some(action) = pumpkin_cluster::avatar_lifecycle::action(admission_id.unwrap(), resolution_tick, spatial.chunk, lifecycle, None) {
                                pending_action_inputs.push(ActionInput::Complete(action));
                            } else {
                                tracing::error!(gid = ?intent.actor, "cluster prepared avatar admission invariant failed");
                            }
                        } else if matches!(intent.source, pumpkin_cluster::controller::ControllerActorSource::Client) {
                            if let (Some(previous), Some(lease), Some(epoch)) = (
                                previous_spatial, inventory.player_lease_for_gid(intent.actor),
                                living.get(pumpkin_cluster::actor::ActorId::Player(intent.actor))
                                    .and_then(|truth| truth.local.avatar.as_ref()).map(|avatar| avatar.spatial_epoch),
                            ) {
                                if let Some(reservation) = stance_reservation {
                                    let action = pumpkin_cluster::canonical_action::Action::from_player_stance(
                                        reservation.id(0, 16).unwrap(), lease, resolution_tick,
                                        actors.get(intent.actor).unwrap().local.chunk, spatial.stance(), spatial.fall_distance, epoch,
                                    ).expect("validated owned player stance");
                                    pending_action_inputs.push(ActionInput::Complete(action));
                                }
                                let before = previous.frame.effective;
                                let after = spatial.frame.effective;
                                let delta: [f64; 3] = core::array::from_fn(|axis| after.position[axis] - before.position[axis]);
                                let distance = (delta.iter().map(|value| value * value).sum::<f64>().sqrt() * 100.0).round().clamp(0.0, f64::from(i32::MAX)) as u32;
                                let amount = match intent.movement_statistic {
                                    pumpkin_cluster::controller::ControllerMovementStatistic::Unchecked => distance,
                                    pumpkin_cluster::controller::ControllerMovementStatistic::Cancelled => 0,
                                    pumpkin_cluster::controller::ControllerMovementStatistic::Amount(amount) => amount.max(0) as u32,
                                };
                                let mut increments = Vec::new();
                                if amount > 0 {
                                    use pumpkin_data::statistic::CustomStatistic as Stat;
                                    let statistic = if spatial.flying { Stat::FlyOneCm } else if spatial.crawling { Stat::SwimOneCm }
                                        else if spatial.sneaking { Stat::CrouchOneCm } else if spatial.sprinting { Stat::SprintOneCm } else { Stat::WalkOneCm };
                                    increments.push((8, statistic as u32, amount));
                                }
                                if before.on_ground && !after.on_ground && delta[1] > 0.0 {
                                    increments.push((8, pumpkin_data::statistic::CustomStatistic::Jump as u32, 1));
                                }
                                if !increments.is_empty() || (!before.on_ground && after.on_ground) {
                                    let chunk = actors.get(intent.actor).unwrap().local.chunk;
                                    let jumped = before.on_ground && !after.on_ground && delta[1] > 0.0;
                                    let mut exhaustion = if spatial.flying { 0 }
                                        else if spatial.swimming { distance.saturating_add(5) / 10 }
                                        else if spatial.sprinting && after.on_ground { distance } else { 0 };
                                    if jumped { exhaustion = exhaustion.saturating_add(if spatial.sprinting { 200 } else { 50 }); }
                                    let draft = actor_drafts.get(&(pumpkin_cluster::actor::ActorId::Player(lease.gid), lease.incarnation, resolution_tick)).cloned();
                                    let owned = super::cluster_world_canonical::OwnedWorldActionState {
                                        server: &server, blocks: &mut chunk_blocks, actors: &mut actors,
                                        living: &mut living, inventory: &mut inventory, versions: &mut canonical_versions,
                                    };
                                    match super::cluster_living_motion::FrozenMotionAction::capture(server.clone(), &owned,
                                        lease, chunk, epoch, resolution_tick, &increments, exhaustion.min(40_000), previous, spatial, draft) {
                                        Ok(work) => pending_controller_statistics.push_back(work),
                                        Err(reason) => {
                                            if rejected_controller_statistics.insert(lease.gid) {
                                                tracing::error!(gid = ?lease.gid, ?reason, "cluster motion source capture invariant failed");
                                            }
                                        }
                                    }
                                }
                            }
                        }
                    }
                    Err(reason) => {
                        if let Some(reservation) = stance_reservation { assert!(action_issue_clock.release_unused(reservation)); }
                        if rejected_actor_intents.insert((intent.actor, reason)) {
                            tracing::error!(gid = ?intent.actor, ?reason, "cluster actor controller intent rejected");
                        }
                    }
                }
                server.cluster_controller.return_actor_intent_credit();
            }
            pending_actor_views.retain(|actor, view| {
                if actor_generation_is_terminal(&retired_actor_install_generations, *actor, view.owner_install_generation) {
                    actor_views.remove(*actor);
                    return false;
                }
                let Some(spatial) = actors.input_projection(*actor) else { return true; };
                let Some(lease) = inventory.player_lease_for_gid(*actor) else { return true; };
                if actors.local_client_entity_id(lease, view.owner_install_generation).is_none() {
                    return false;
                }
                if spatial.frame.effective.tick != view.tick && !spatial.frame.effective.tick.is_after(view.tick)
                    || pending_actor_intents.iter().any(|intent| intent.actor == *actor) {
                    return true;
                }
                if actor_views.apply(*view).is_ok() {
                    chunk_watches.update_view(*view);
                }
                false
            });
            inventory.stage_pending_menu_opens(&server);
            source_actor_snapshots.retain(|slot, (snapshot, bytes, sources)| {
                let pinned = historical_actor_requests.values().any(|request| request.full_source_slot == *slot);
                let closed = historical_acquisition_receipts.cut_closed(TickStamp(*slot as u16), sources);
                if closed && !pinned && Arc::strong_count(snapshot) == 1 {
                    source_actor_snapshot_bytes = source_actor_snapshot_bytes.saturating_sub(*bytes);
                    false
                } else { true }
            });
            if let Some(scope) = canonical_scopes.get(&resolution_tick)
                && let Some(slot) = scope.full_source_slot
                && !source_actor_snapshots.contains_key(&slot)
                && source_actor_snapshots.len() < 2
            {
                let owned = super::cluster_world_canonical::OwnedWorldActionState {
                    server: &server, blocks: &mut chunk_blocks, actors: &mut actors,
                    living: &mut living, inventory: &mut inventory, versions: &mut canonical_versions,
                };
                if let Ok(snapshot) = super::cluster_world_callback_snapshot::CallbackTickSnapshot::capture_actor_phase(server.clone(), &owned, resolution_tick)
                    .and_then(|snapshot| Ok(snapshot.with_scheduled_executors(&scope.scheduled_executors()).with_player_associations(scope.player_associations()).with_original_directory(scope.directory.clone(), scope.source(scope.local).map_err(|_| pumpkin_cluster::canonical_action::ApplyFailure::InvalidAction)?)))
                    .and_then(|snapshot| snapshot.with_entity_occupancy_poses(&item_spatial))
                    .and_then(|snapshot| snapshot.with_original_phase(Arc::new(scope.actor_phase_cut_witness()?)))
                    && snapshot.original_source_slot() == Some(slot)
                    && let Ok((_, bytes)) = snapshot.backing_resource_usage()
                    && source_actor_snapshot_bytes.checked_add(bytes).is_some()
                {
                    source_actor_snapshot_bytes += bytes;
                    source_actor_snapshots.insert(slot, (snapshot, bytes, scope.producers.clone()));
                }
            }
            let source_actor_snapshot = canonical_scopes.get(&resolution_tick).and_then(|scope| scope.full_source_slot)
                .and_then(|slot| source_actor_snapshots.get(&slot)).map(|(snapshot, _, _)| snapshot.clone());
            #[cfg(feature = "cluster-e2e")]
            {
                while pending_inventory_scenario_requests.len() < 128 {
                    let Some(request) = server.cluster_test.scenarios.pop_inventory() else { break; };
                    pending_inventory_scenario_requests.push_back(request);
                }
                for _ in 0..pending_inventory_scenario_requests.len() {
                    let mut request = pending_inventory_scenario_requests.pop_front().unwrap();
                    if matches!(request.command, super::cluster_test_scenarios::InventoryScenarioCommand::InitialAvatarInventory) {
                        request.command = super::cluster_test_scenarios::InventoryScenarioCommand::Loadout(super::cluster_test_scenarios::FixtureLoadoutTemplate::InitialAvatar);
                    }
                    if matches!(request.command, super::cluster_test_scenarios::InventoryScenarioCommand::Loadout(_)) {
                        if request.target.gid.server == local && inventory_scenario_source.watch_loadout(&server, local, request).is_err() {
                            pending_inventory_scenario_requests.push_back(request);
                        }
                    } else {
                        pending_inventory_scenario_requests.push_back(request);
                    }
                }
                inventory_scenario_source.advance(&server);
            }
            while pending_action_inputs.len() + canonical_queued.values().map(Vec::len).sum::<usize>() < 4096 {
                let Ok(action) = action_rx.try_recv() else { break; };
                pending_action_inputs.push(action);
            }
            for (lease, (retire, issued)) in &mut pending_avatar_retirements {
                if issued.is_some() || pending_action_inputs.len() >= 4096 { continue; }
                let Some(spatial) = actors.phase_spatial(lease.gid, false) else { continue; };
                let Some(initial) = living.phase_state(pumpkin_cluster::actor::ActorId::Player(lease.gid), false) else { continue; };
                let Some(owner) = canonical_scopes.get(&resolution_tick).and_then(|scope| scope.source(local).ok()) else { continue; };
                let Some(reservation) = action_issue_clock.reserve(pumpkin_cluster::actor::ActorId::Player(lease.gid), lease.incarnation, resolution_tick, 1) else { continue; };
                let id = reservation.id(0, 15).unwrap();
                let target = Target::Chunk { key: spatial.chunk, part: pumpkin_cluster::canonical_action::ChunkPart::Player(lease.gid), inventory: None };
                let before = pumpkin_cluster::canonical_action::Cell { revision: canonical_versions.get(&(TruthView::Local, target)).copied().unwrap_or(0), value: pumpkin_cluster::canonical_action::Value::Living((initial.clone()).into()) };
                let intent = pumpkin_cluster::avatar_lifecycle::AvatarLifecycleIntent::Retirement { lease: *lease, install_generation: retire.install_generation, owner };
                if let Some(action) = pumpkin_cluster::avatar_lifecycle::action(id, resolution_tick, spatial.chunk, intent, Some(before)) {
                    *issued = Some(pumpkin_cluster::canonical_action::ActionRef { id, tick: resolution_tick });
                    pending_action_inputs.push(ActionInput::Complete(action));
                }
            }
            while let Ok(binding) = replica_binding_rx.try_recv() {
                if binding.retired || server.cluster_peer_incarnations.load().get(&binding.source.server) == Some(&binding.source.generation) {
                    actors.update_remote_client_binding(binding);
                }
            }
            if server.is_cluster_primary() && last_item_lifecycle_tick != Some(resolution_tick) {
                let tick = *pending_item_lifecycle_tick.get_or_insert(resolution_tick);
                let capacity = 4096_usize.saturating_sub(canonical_pending.len() + pending_action_inputs.len());
                if let Some(scope) = canonical_scopes.get(&tick) && let Ok(source) = scope.source(local) {
                    if item_lifecycle_frame.is_none() {
                        item_lifecycle_frame = scope.full_source_slot.and_then(|slot| source_actor_snapshots.get(&slot))
                            .and_then(|(snapshot, _, _)| super::cluster_item_lifecycle::capture_frame(snapshot, source, tick,
                                |chunk, _| scope.scheduled_executor(chunk).ok() == Some(source)));
                        if let Some(frame) = &item_lifecycle_frame {
                            for (_, body) in frame.remaining() {
                                retained_internal_actors.hold((pumpkin_cluster::actor::ActorId::Entity(body.id), body.id.generation), tick,
                                    pumpkin_cluster::actor_source_retention::ActorSourceFamily::ItemLifetime);
                            }
                        }
                    }
                    if let Some(frame) = item_lifecycle_frame.as_mut() {
                        if item_lifecycle_page.is_none() {
                            match super::cluster_item_lifecycle::emit_frame(frame, &mut action_issue_clock, capacity) {
                                Ok(actions) if !actions.is_empty() || frame.complete() => item_lifecycle_page = Some(actions),
                                Ok(_) => {},
                                Err(reason) if last_item_lifecycle_warning.is_none_or(|last| last.elapsed().as_secs() >= 60) => {
                                    tracing::error!(?tick, ?reason, "cluster primary item lifecycle capture failed");
                                    last_item_lifecycle_warning = Some(std::time::Instant::now());
                                }
                                Err(_) => {},
                            }
                        }
                        if let Some(actions) = item_lifecycle_page.take() {
                            let mut batch = TickBatch::new(tick);
                            batch.canonical_actions = actions.clone();
                            match reserve_actor_source_page(&mut holistic_outbox, Some(scope), batch, &local_finishes,
                                canonical_queued.get(&tick).map_or(&[], Vec::as_slice), &canonical_reserved_batches,
                                local_realm_issued.get(&tick), &realm_completion, |action|
                                    chunk_blocks.historical_entity_authority(action).or_else(|| entity_authority_at_action_tick(&canonical_graphs, action))) {
                                Ok(page) => {
                                    let emitted = actions.len();
                                    for action in actions {
                                        retained_internal_actors.release((action.id.actor, action.id.incarnation), tick,
                                            pumpkin_cluster::actor_source_retention::ActorSourceFamily::ItemLifetime);
                                        canonical_pending.insert((action.tick, action.id), PendingCanonical { action });
                                    }
                                    canonical_reserved_batches.push_back(page);
                                    assert!(frame.commit_emitted(emitted));
                                    if frame.complete() {
                                        last_item_lifecycle_tick = Some(tick);
                                        pending_item_lifecycle_tick = None;
                                        item_lifecycle_frame = None;
                                    }
                                }
                                Err(_) => item_lifecycle_page = Some(actions),
                            }
                        }
                    }
                }
            }
            if server.is_cluster_primary() {
                if physics_source.is_none() && last_physics_tick.is_none_or(|tick| resolution_tick.is_after(tick)) {
                    let tick = *pending_physics_tick.get_or_insert(resolution_tick);
                    if let Some(snapshot) = canonical_scopes.get(&tick).and_then(|scope| scope.full_source_slot)
                        .and_then(|slot| source_actor_snapshots.get(&slot)).map(|(snapshot, _, _)| snapshot.clone())
                        && let Some(source) = canonical_scopes.get(&tick).and_then(|scope| scope.source(local).ok()) {
                        match super::cluster_item_physics_source::capture(snapshot, source, tick) {
                            Ok(frame) => {
                                for job in frame.frame.remaining() {
                                    retained_internal_actors.hold(job.actor(), tick, pumpkin_cluster::actor_source_retention::ActorSourceFamily::ItemPhysics);
                                }
                                physics_source = Some(frame);
                            }
                            Err(reason) => {
                                if physics_failure != Some(reason.clone()) {
                                    tracing::error!(?tick, ?reason, "cluster original item physics capture unavailable");
                                    physics_failure = Some(reason);
                                }
                            }
                        }
                    }
                }
                if let Some(source) = physics_source.as_mut() {
                    let mut physics_cut_ready = true;
                    if let Some(pumpkin_cluster::canonical_action::ApplyFailure::Condition(pumpkin_cluster::canonical_action::Target::Chunk {key:chunk,..})) = physics_failure.as_ref() {
                        let original_cut = source.original_snapshot().clone();
                        if !original_cut.original_store().contains(*chunk) && let Some(slot) = original_cut.original_source_slot() {
                            physics_cut_ready = false;
                            let key = (slot,*chunk);
                            if let Some((cut,_)) = historical_actor_cuts.get(&key) {
                                if source.with_historical_chunks(std::slice::from_ref(cut)).is_ok() { physics_failure = None; physics_cut_ready = true; }
                            } else if !historical_actor_need_pins.contains_key(&key) && !local_finishes.closed_through(source.frame.tick)
                                && let Ok(providers) = original_cut.original_frozen_holders(*chunk)
                                && let Some(next) = next_historical_actor_lease.checked_add(1)
                                && let Some(pin) = super::cluster::try_require_original_actor_chunk(&server,*chunk,slot,next_historical_actor_lease,providers)
                            {
                                next_historical_actor_lease = next;
                                historical_actor_need_pins.insert(key,pin);
                            }
                        }
                    }
                    let capacity = 4096usize.saturating_sub(canonical_pending.len().saturating_add(pending_action_inputs.len()));
                    if physics_page.is_none() && physics_cut_ready {
                        match super::cluster_item_physics_source::emit(source, &mut action_issue_clock, capacity) {
                            Ok(page) if page.completed_jobs > 0 || source.frame.complete() => physics_page = Some(page),
                            Ok(_) => {},
                            Err(reason) => {
                                if physics_failure != Some(reason.clone()) {
                                    tracing::error!(tick = ?source.frame.tick, ?reason, "cluster original item physics emission unavailable");
                                    physics_failure = Some(reason);
                                }
                            }
                        }
                    }
                    if let Some(page) = physics_page.take() {
                        let mut batch = TickBatch::new(source.frame.tick);
                        batch.canonical_actions = page.actions.clone();
                        match reserve_actor_source_page(&mut holistic_outbox, canonical_scopes.get(&source.frame.tick), batch,
                            &local_finishes, canonical_queued.get(&source.frame.tick).map_or(&[], Vec::as_slice), &canonical_reserved_batches,
                            local_realm_issued.get(&source.frame.tick), &realm_completion, |action|
                                chunk_blocks.historical_entity_authority(action).or_else(|| entity_authority_at_action_tick(&canonical_graphs, action))) {
                            Ok(reserved) => {
                                for job in source.frame.remaining().iter().take(page.completed_jobs) {
                                    retained_internal_actors.release(job.actor(), source.frame.tick, pumpkin_cluster::actor_source_retention::ActorSourceFamily::ItemPhysics);
                                }
                                for action in page.actions {
                                    if action.chunk_transition {
                                        let pumpkin_cluster::actor::ActorId::Entity(id) = action.id.actor else { unreachable!(); };
                                        pending_item_boundaries.insert(id, pumpkin_cluster::canonical_action::ActionRef { id: action.id, tick: action.tick });
                                    }
                                    canonical_pending.insert((action.tick, action.id), PendingCanonical { action });
                                }
                                if let Some(scope) = canonical_scopes.get(&source.frame.tick) {
                                    publish_owned_entity_spatial(&server, &chunk_blocks, &mut item_spatial, scope, page.samples);
                                }
                                canonical_reserved_batches.push_back(reserved);
                                assert!(source.frame.commit_emitted(page.completed_jobs));
                                if source.frame.complete() {
                                    last_physics_tick = Some(source.frame.tick);
                                    pending_physics_tick = None;
                                    physics_source = None;
                                    physics_failure = None;
                                }
                            }
                            Err(_) => physics_page = Some(page),
                        }
                    }
                }
            }
            {
            let administrative_view = super::cluster_world_canonical::OwnedWorldActionState {
                server: &server, blocks: &mut chunk_blocks, actors: &mut actors, living: &mut living,
                inventory: &mut inventory, versions: &mut canonical_versions,
            };
            pending_action_inputs.extend(
                administration
                    .retry_pending(&server, administrative_view.blocks, &administrative_view, resolution_tick, |chunk| {
                        !pending_fresh.contains_key(&chunk)
                            && !pending_imports.contains_key(&chunk)
                            && !buckets.is_quarantined(chunk)
                    })
                    .into_iter()
                    .map(ActionInput::Complete),
            );
            while let Ok(intent) = administration.receiver.try_recv() {
                if let Some(actions) = administration.prepare_canonical(
                    &server,
                    administrative_view.blocks,
                    &administrative_view,
                    intent,
                    resolution_tick,
                    |chunk| {
                        !pending_fresh.contains_key(&chunk)
                            && !pending_imports.contains_key(&chunk)
                            && !buckets.is_quarantined(chunk)
                    },
                ) {
                    pending_action_inputs.extend(actions.into_iter().map(ActionInput::Complete));
                }
            }
            }
            if let Some(route) = server.cluster_chunk_need_route.load_full() {
                while let Some((chunk, lease)) = pending_admin_need_releases.front().copied() {
                    if route
                        .try_send(super::cluster::ChunkNeed::Release { chunk, lease })
                        .is_err()
                    {
                        break;
                    }
                    pending_admin_need_releases.pop_front();
                    ready_admin_needs.remove(&(chunk, lease));
                }
            }
            while let Ok(ready) = admin_chunk_ready_rx.try_recv() {
                let chunk = ChunkKey::new(
                    pumpkin_cluster::protocol::WorldId::new(ready.dimension, ready.world_key),
                    ready.position.x,
                    ready.position.y,
                );
                if pending_admin_needs.values().any(|held| *held == (chunk, ready.lease)) {
                    ready_admin_needs.insert((chunk, ready.lease), ready.publication_generation);
                }
            }
            while pending_action_inputs.len() + canonical_queued.values().map(Vec::len).sum::<usize>() < 4096 {
                let Some(proposal) = pending_admin_inputs.pop_front() else { break; };
                pending_action_inputs.push(ActionInput::Proposal(proposal));
            }
            while deferred_inventory_menus.len() < 512 {
                let Ok(menu) = inventory.menus.try_recv() else { break; };
                deferred_inventory_menus.push_back(menu);
            }
            while deferred_inventory_screens.len() < 512 {
                let Ok(screen) = inventory.screens.try_recv() else { break; };
                deferred_inventory_screens.push_back(screen);
            }
            while deferred_inventory_actions.len() < 512 {
                let Ok(action) = inventory.actions.try_recv() else { break; };
                deferred_inventory_actions.push_back(action);
            }
            for _ in 0..deferred_inventory_menus.len() {
                let Some(menu) = deferred_inventory_menus.pop_front() else { break; };
                let Some(sequence) = menu.input_sequence else {
                    deferred_inventory_menus.push_back(menu);
                    continue;
                };
                let pumpkin_cluster::actor::ActorId::Player(gid) = menu.intent.actor else {
                    deferred_inventory_menus.push_back(menu);
                    continue;
                };
                let Some(lease) = inventory.player_lease_for_gid(gid) else {
                    deferred_inventory_menus.push_back(menu);
                    continue;
                };
                if pending_action_inputs.len() + canonical_queued.values().map(Vec::len).sum::<usize>() >= 4096 {
                    deferred_inventory_menus.push_back(menu);
                    continue;
                }
                pending_action_inputs.push(ActionInput::Proposal(pumpkin_cluster::canonical_action::ActionProposal {
                    command: None,
                    id: pumpkin_cluster::canonical_action::ActionId { actor: menu.intent.actor,
                        incarnation: lease.incarnation, sequence: sequence.0, family: 2 },
                    tick: menu.intent.tick,
                    lease: Some(lease),
                    client_origin: super::cluster_inventory_protocol::inventory_client_origin(menu.origin, Some(menu.intent.session)),
                    screen_generation: menu.intent.session.session,
                    claimed_targets: Vec::new(),
                    claim: pumpkin_cluster::canonical_action::ClaimedAction::InventoryMenu(
                        pumpkin_cluster::canonical_recipe::InventoryMenuProposal {
                            session: menu.intent.session, sync_id: menu.intent.sync_id,
                            window_kind: menu.intent.window_kind, actor_chunk: menu.intent.actor_chunk,
                            kind: menu.intent.kind, companion_gestures: menu.intent.companion_gestures,
                        }),
                }));
            }
            for _ in 0..deferred_inventory_screens.len() {
                let Some(screen) = deferred_inventory_screens.pop_front() else { break; };
                let session = match &screen.command {
                    pumpkin_cluster::inventory::InventoryScreenCommand::BeginQuickCraft { session, .. }
                    | pumpkin_cluster::inventory::InventoryScreenCommand::AddQuickCraftSlot { session, .. }
                    | pumpkin_cluster::inventory::InventoryScreenCommand::FinishQuickCraft { session } => *session,
                    pumpkin_cluster::inventory::InventoryScreenCommand::Close { session } => {
                        let Some(sequence) = screen.input_sequence else {
                            deferred_inventory_screens.push_back(screen);
                            continue;
                        };
                        let Some(token) = screen.close_token else {
                            deferred_inventory_screens.push_back(screen);
                            continue;
                        };
                        let Some(lease) = inventory.player_lease_for_gid(session.actor) else {
                            deferred_inventory_screens.push_back(screen);
                            continue;
                        };
                        let Some(actor) = actors.get(session.actor) else {
                            deferred_inventory_screens.push_back(screen);
                            continue;
                        };
                        if pending_action_inputs.len() + canonical_queued.values().map(Vec::len).sum::<usize>() >= 4096 {
                            deferred_inventory_screens.push_back(screen);
                            continue;
                        }
                        pending_action_inputs.push(ActionInput::Proposal(pumpkin_cluster::canonical_action::ActionProposal::inventory_close(
                            lease, sequence, screen.tick,
                            super::cluster_inventory_protocol::inventory_client_origin(screen.origin, Some(*session)),
                            pumpkin_cluster::canonical_inventory_close::InventoryCloseProposal {
                                session: Some(*session), token, actor_chunk: actor.local.chunk,
                            },
                        )));
                        continue;
                    }
                };
                let prior_action = deferred_inventory_actions.iter().any(|action| action.intent.actor == session.actor.into()
                    && input_precedes(action.input_sequence, screen.input_sequence, 0, 1));
                let prior_menu = deferred_inventory_menus.iter().any(|menu| menu.intent.actor == session.actor.into()
                    && input_precedes(menu.input_sequence, screen.input_sequence, 2, 1));
                let prior_screen = deferred_inventory_screens.iter().any(|other| {
                    let other_session = match &other.command {
                        pumpkin_cluster::inventory::InventoryScreenCommand::BeginQuickCraft { session, .. }
                        | pumpkin_cluster::inventory::InventoryScreenCommand::AddQuickCraftSlot { session, .. }
                        | pumpkin_cluster::inventory::InventoryScreenCommand::FinishQuickCraft { session }
                        | pumpkin_cluster::inventory::InventoryScreenCommand::Close { session } => session,
                    };
                    other_session.actor == session.actor && input_precedes(other.input_sequence, screen.input_sequence, 1, 1)
                });
                if prior_action || prior_menu || prior_screen || deferred_inventory_actions.len() >= 512
                    || screen.affected.iter().any(|chunk| pending_imports.contains_key(chunk))
                {
                    deferred_inventory_screens.push_back(screen);
                    continue;
                }
                if let Some(action) = inventory.capture_screen_gesture(screen) {
                    deferred_inventory_actions.push_back(action);
                }
            }
            for _ in 0..deferred_inventory_actions.len() {
                let Some(action) = deferred_inventory_actions.pop_front() else { break; };
                let actor = action.intent.actor;
                let Some(sequence) = action.input_sequence else {
                    deferred_inventory_actions.push_back(action);
                    continue;
                };
                let pumpkin_cluster::actor::ActorId::Player(gid) = actor else {
                    deferred_inventory_actions.push_back(action);
                    continue;
                };
                let Some(lease) = action.lease.filter(|lease| lease.gid == gid) else {
                    deferred_inventory_actions.push_back(action);
                    continue;
                };
                let targets = inventory.action_targets(&action);
                let prior_menu = deferred_inventory_menus.iter().any(|menu| menu.intent.actor == actor
                    && input_precedes(menu.input_sequence, action.input_sequence, 2, 0));
                let prior_screen = deferred_inventory_screens.iter().any(|screen| {
                    let session = match &screen.command {
                        pumpkin_cluster::inventory::InventoryScreenCommand::BeginQuickCraft { session, .. }
                        | pumpkin_cluster::inventory::InventoryScreenCommand::AddQuickCraftSlot { session, .. }
                        | pumpkin_cluster::inventory::InventoryScreenCommand::FinishQuickCraft { session }
                        | pumpkin_cluster::inventory::InventoryScreenCommand::Close { session } => session,
                    };
                    session.actor == gid && input_precedes(screen.input_sequence, action.input_sequence, 1, 0)
                });
                if prior_menu || prior_screen || inventory_capture_barrier.blocks(actor, &targets)
                    || action.intent.affected.iter().any(|chunk| pending_imports.contains_key(chunk))
                    || pending_action_inputs.len() + canonical_queued.values().map(Vec::len).sum::<usize>() >= 4096
                {
                    deferred_inventory_actions.push_back(action);
                    continue;
                }
                if let Some(open) = action.open.clone() {
                    if pending_inventory_open_sources.len() >= 512 { deferred_inventory_actions.push_back(action); continue; }
                    let original = canonical_scopes.get(&action.intent.tick).and_then(|scope| scope.full_source_slot)
                        .and_then(|slot| source_actor_snapshots.get(&slot)).map(|(snapshot, _, _)| snapshot);
                    let Some(original) = original else { deferred_inventory_actions.push_back(action); continue; };
                    let Ok(viewer) = super::cluster_inventory_menu::capture_menu_viewer(original, lease, action.intent.actor_chunk, action.intent.tick) else {
                        deferred_inventory_actions.push_back(action); continue;
                    };
                    let id = pumpkin_cluster::canonical_action::ActionId { actor, incarnation: lease.incarnation, sequence: sequence.0, family: 2 };
                    let proposal = pumpkin_cluster::canonical_action::ActionProposal {
                        command: None, id, tick: action.intent.tick, lease: Some(lease),
                        client_origin: pumpkin_cluster::canonical_action::ClientOrigin::None, screen_generation: open.screen.session.session,
                        claimed_targets: Vec::new(),
                        claim: pumpkin_cluster::canonical_action::ClaimedAction::InventoryMenuSession(pumpkin_cluster::canonical_menu::MenuSessionProposal {
                            actor_chunk: action.intent.actor_chunk, virtual_inventory: None,
                            operation: pumpkin_cluster::canonical_menu::MenuSessionOperation::Open {
                                state: pumpkin_cluster::canonical_menu::MenuSessionState { session: open.screen.session.session,
                                    window_kind: open.screen.window_kind, source: open.source.clone(), selection: pumpkin_cluster::canonical_menu::MenuSelection::None, ..Default::default() }, viewer,
                            },
                        }),
                    };
                    pending_inventory_open_sources.insert(id, open);
                    pending_action_inputs.push(ActionInput::Proposal(proposal));
                    continue;
                }
                let pumpkin_cluster::inventory::InventoryIntentPayload::Gestures(gestures) = action.intent.payload else { unreachable!() };
                pending_action_inputs.push(ActionInput::Proposal(pumpkin_cluster::canonical_action::ActionProposal::inventory(
                    lease, sequence, action.intent.tick,
                    super::cluster_inventory_protocol::inventory_client_origin(action.origin, action.intent.screen_session),
                    pumpkin_cluster::canonical_inventory::InventoryGestureProposal {
                        gestures, screen_session: action.intent.screen_session, actor_chunk: action.intent.actor_chunk,
                    },
                )));
            }
            for _ in 0..pending_controller_statistics.len() {
                if pending_action_inputs.len() + canonical_queued.values().map(Vec::len).sum::<usize>() >= 4096 { break; }
                let Some(work) = pending_controller_statistics.pop_front() else { break; };
                let lease = work.lease;
                if frozen_proposal_work.keys().any(|head| head.actor == pumpkin_cluster::actor::ActorId::Player(lease.gid) && head.incarnation == lease.incarnation)
                    || callback_work.blocked_actor(pumpkin_cluster::actor::ActorId::Player(lease.gid), lease.incarnation)
                    || completed_callback_work.keys().any(|reference| reference.id.actor == pumpkin_cluster::actor::ActorId::Player(lease.gid) && reference.id.incarnation == lease.incarnation)
                    || retained_internal_actors.contains(&(pumpkin_cluster::actor::ActorId::Player(lease.gid), lease.incarnation))
                {
                    pending_controller_statistics.push_back(work);
                    continue;
                }
                if !inventory.live_player_lease_matches(lease) { continue; }
                let Some(reservation) = action_issue_clock.reserve(pumpkin_cluster::actor::ActorId::Player(lease.gid), lease.incarnation, work.tick, 1) else {
                    pending_controller_statistics.push_back(work);
                    continue;
                };
                let id = reservation.id(0, 12).unwrap();
                match work.materialize(id) {
                    Ok((action, draft)) => {
                        actor_drafts.insert((action.id.actor, action.id.incarnation, action.tick), draft);
                        pending_action_inputs.push(ActionInput::Complete(action));
                    }
                    Err(reason) => {
                        action_issue_clock.release_unused(reservation);
                        if rejected_controller_statistics.insert(lease.gid) {
                            tracing::error!(gid = ?lease.gid, ?reason, "cluster controller statistic capture failed");
                        }
                    }
                }
            }
            for _ in 0..callback_work.remaining_capacity().min(4096usize.saturating_sub(pending_action_inputs.len())) {
                let Ok(staged) = item_actions.receiver.try_recv() else { break; };
                let pumpkin_cluster::actor::ActorId::Player(gid) = staged.intent.actor else { continue; };
                let Some(lease) = inventory.player_lease_for_gid(gid) else { continue; };
                let Some((frame, eye_height)) = staged.effective_frame else {
                    let _ = item_actions.reject_unplanned(&server, inventory.ledger(), staged);
                    continue;
                };
                let id = pumpkin_cluster::canonical_action::ActionId { actor: staged.intent.actor,
                    incarnation: lease.incarnation, sequence: staged.input_sequence.map_or(0, |sequence| sequence.0), family: 10 };
                pending_action_inputs.push(ActionInput::Proposal(pumpkin_cluster::canonical_action::ActionProposal {
                    command: None, id, tick: resolution_tick, lease: Some(lease),
                    client_origin: super::cluster_inventory_protocol::item_client_origin(staged.origin), screen_generation: 0,
                    claimed_targets: Vec::new(), claim: pumpkin_cluster::canonical_action::ClaimedAction::ItemUse(
                        pumpkin_cluster::item_use::ItemUseClaim { intent: staged.intent, effective_frame: frame, eye_height,
                            target_observation: staged.target_observation,
                            owner_install_generation: staged.owner_install_generation }),
                }));
            }
            if environment_source_fault.is_none() && environment_source_bank.is_none() && environment_source_input.is_none() && last_environment_source_tick != Some(resolution_tick) {
                let scope = &canonical_scopes[&resolution_tick];
                if let Ok(source) = scope.source(local) {
                    let _ = action_issue_clock.open_tick(pumpkin_cluster::actor::ActorId::Peer(source.server), source.generation, resolution_tick);
                    let categories = scope.local_chunks.keys().copied().collect::<BTreeSet<_>>().into_iter().filter(|chunk| {
                        chunk_blocks.contains(*chunk) && !pending_fresh.contains_key(chunk)
                            && !pending_imports.contains_key(chunk) && !buckets.is_quarantined(*chunk)
                    }).filter_map(|chunk| {
                        let world = world_for(&server, chunk.world)?;
                        let elected_primary = scope.frozen_chunk_holders(chunk).ok().and_then(|holders| {
                            holders.intersection(&scope.primary_producers).copied().next()
                        }) == Some(source);
                        let random_ticks = server.is_cluster_primary() && elected_primary;
                        Some(super::cluster_world_environment_bank::EnvironmentCategory { world, chunk, random_ticks })
                    }).collect();
                    match source_actor_snapshot.clone().ok_or(pumpkin_cluster::canonical_action::ApplyFailure::Capacity)
                        .and_then(|snapshot| super::cluster_world_environment_bank::FrozenEnvironmentSourceInput::new(snapshot, source, resolution_tick, categories, scope.scheduled_executors())) {
                        Ok(input) => environment_source_input = Some(input),
                        Err(reason) => {
                            environment_source_fault = Some(resolution_tick);
                            let now = std::time::Instant::now();
                            if environment_source_diagnostic.is_none_or(|last| now.duration_since(last).as_secs() >= 300) {
                                environment_source_diagnostic = Some(now);
                                tracing::error!(?source, tick=resolution_tick.0, ?reason, "cluster environmental source original cut unavailable");
                            }
                        }
                    }
                }
            }
            if let Some(input) = &mut environment_source_input {
                if let Err(reason) = input.refresh_policy_archive() {
                    let now = std::time::Instant::now();
                    if environment_source_diagnostic.is_none_or(|last| now.duration_since(last).as_secs() >= 300) {
                        environment_source_diagnostic = Some(now);
                        tracing::error!(tick=input.tick().0, ?reason, "cluster environmental source policy archive extension rejected");
                    }
                }
            }
            if let Some(input) = environment_source_input.as_mut().filter(|input| environment_source_fault.is_none() && input.ready()) {
                let owned = super::cluster_world_canonical::OwnedWorldActionState {
                    server: &server, blocks: &mut chunk_blocks, actors: &mut actors,
                    living: &mut living, inventory: &mut inventory, versions: &mut canonical_versions,
                };
                match input.capture(server.clone(), &owned) {
                    Ok(bank) => { environment_source_bank = Some(bank); environment_source_input = None; }
                    Err(reason) => {
                        if matches!(reason, pumpkin_cluster::canonical_action::ApplyFailure::ResourceLimit) { environment_source_fault = Some(input.tick()); }
                        let now = std::time::Instant::now();
                        if environment_source_diagnostic.is_none_or(|last| now.duration_since(last).as_secs() >= 300) {
                            environment_source_diagnostic = Some(now);
                            tracing::error!(tick=input.tick().0, ?reason, "cluster environmental source retained original cut unavailable");
                        }
                    }
                }
            }
            if let Some(bank) = &mut environment_source_bank {
                let actor = pumpkin_cluster::actor::ActorId::Peer(local);
                let available = 4096usize.saturating_sub(pending_action_inputs.len() + canonical_queued.values().map(Vec::len).sum::<usize>());
                if environment_source_fault.is_none() && bank.open_source_tick(&mut action_issue_clock) && !completed_callback_work.keys().any(|reference| reference.id.actor == actor)
                    && available > 0
                {
                    for _ in 0..callback_work.remaining_capacity().min(128).min(available) {
                    if let Err(reason) = bank.advance(&mut callback_work, &mut action_issue_clock, server.is_cluster_primary(), 512) {
                        if bank.ordinal_exhausted(&action_issue_clock) { environment_source_fault = Some(bank.tick()); }
                        let now = std::time::Instant::now();
                        if environment_source_diagnostic.is_none_or(|last| now.duration_since(last).as_secs() >= 300) {
                            environment_source_diagnostic = Some(now);
                            tracing::error!(tick=bank.tick().0, ?reason, "cluster environmental source input retained");
                        }
                        break;
                    }
                    if bank.complete() { break; }
                    }
                }
                if bank.complete() && !callback_work.blocked_actor(actor, canonical_scopes[&bank.tick()].source(local).unwrap().generation)
                    && !completed_callback_work.keys().any(|reference| reference.id.actor == actor)
                {
                    last_environment_source_tick = Some(bank.tick());
                    environment_source_bank = None;
                }
            }
            for (reference, result) in callback_work.advance(&server, &canonical_graphs, entity_origin_allocator.as_mut()) {
                if object_terminal_reference == Some(reference) {
                    object_terminal_inflight = false;
                    if let Ok(work) = result {
                        completed_object_terminal = Some(work);
                    }
                    continue;
                }
                if let Ok(work) = result {
                    pending_action_inputs.push(ActionInput::Complete(work.action.clone()));
                    completed_callback_work.insert(reference, work);
                } else {
                    callback_input_order.remove(&reference);
                }
            }
            let mut input_order = HashMap::new();
            for input in &pending_action_inputs {
                let category = match input {
                    ActionInput::Complete(action) if !callback_input_order.contains_key(&pumpkin_cluster::canonical_action::ActionRef { id: action.id, tick: action.tick }) => 0_u8,
                    _ => 1,
                };
                let id = match input {
                    ActionInput::Complete(action) => callback_input_order.get(&pumpkin_cluster::canonical_action::ActionRef { id: action.id, tick: action.tick }).copied().unwrap_or(action.id),
                    ActionInput::Proposal(proposal) => proposal.id,
                };
                input_order.entry((id.actor, id.incarnation, category)).and_modify(|first: &mut u16| {
                    if first.wrapping_sub(id.sequence) < 32768 { *first = id.sequence; }
                }).or_insert(id.sequence);
            }
            pending_action_inputs.sort_by_key(|input| {
                let category = match input {
                    ActionInput::Complete(action) if !callback_input_order.contains_key(&pumpkin_cluster::canonical_action::ActionRef { id: action.id, tick: action.tick }) => 0_u8,
                    _ => 1,
                };
                let (id, tick) = match input {
                    ActionInput::Complete(action) => (callback_input_order.get(&pumpkin_cluster::canonical_action::ActionRef { id: action.id, tick: action.tick }).copied().unwrap_or(action.id), action.tick),
                    ActionInput::Proposal(proposal) => (proposal.id, proposal.tick),
                };
                (id.actor, id.incarnation, std::cmp::Reverse(resolution_tick.distance_since(tick)), category, id.sequence.wrapping_sub(input_order[&(id.actor, id.incarnation, category)]))
            });
            let mut resource_rejected_proposals = BTreeMap::new();
            let mut proposal_segments = BTreeMap::<_, Vec<pumpkin_cluster::canonical_action::ActionProposal>>::new();
            let mut mixed_segments = HashSet::new();
            for input in &pending_action_inputs {
                let (id, tick) = match input {
                    ActionInput::Complete(action) => { mixed_segments.insert((action.id.actor, action.id.incarnation, action.tick)); continue; }
                    ActionInput::Proposal(proposal) => (proposal.id, proposal.tick),
                };
                let ActionInput::Proposal(proposal) = input else { unreachable!() };
                if super::cluster_action_work::FrozenProposalWork::supports(proposal)
                    && !frozen_proposal_work.contains_key(&id)
                {
                    proposal_segments.entry((id.actor, id.incarnation, tick)).or_default().push(proposal.clone());
                } else { mixed_segments.insert((id.actor, id.incarnation, tick)); }
            }
            let mut blocked_proposal_actors = HashSet::new();
            for (key, proposals) in proposal_segments {
                if mixed_segments.contains(&key)
                    || retained_internal_actors.contains(&(key.0, key.1))
                    || proposals.iter().any(|proposal| frozen_proposal_work.contains_key(&proposal.id))
                    || frozen_proposal_work.keys().any(|head| head.actor == key.0 && head.incarnation == key.1)
                    || callback_work.blocked_actor(key.0, key.1)
                    || completed_callback_work.keys().any(|reference| reference.id.actor == key.0 && reference.id.incarnation == key.1)
                { continue; }
                let draft = actor_drafts.get(&key).cloned().unwrap_or_else(|| pumpkin_cluster::actor_action_scope::ActorActionDraft::new(key.0, key.1, key.2, 4096, 16384));
                let owned = super::cluster_world_canonical::OwnedWorldActionState {
                    server: &server, blocks: &mut chunk_blocks, actors: &mut actors,
                    living: &mut living, inventory: &mut inventory, versions: &mut canonical_versions,
                };
                let mut captured = Vec::with_capacity(proposals.len());
                let input_count = proposals.len();
                for proposal in proposals {
                    let correlation = proposal.id;
                    let Some(scope) = canonical_scopes.get(&key.2) else { blocked_proposal_actors.insert((key.0, key.1)); captured.clear(); break; };
                    let original_snapshot = scope.full_source_slot.and_then(|slot| source_actor_snapshots.get(&slot)).map(|(snapshot, _, _)| snapshot.clone());
                    match original_snapshot.ok_or(pumpkin_cluster::canonical_action::ApplyFailure::Capacity)
                        .and_then(|snapshot| super::cluster_action_work::FrozenProposalWork::capture_static_from_snapshot(snapshot, server.clone(), &owned, &draft, proposal, key.2,
                        scope.source(local).unwrap(), &scope.scheduled_executors()))
                        .and_then(|mut work| { if let Some(open) = pending_inventory_open_sources.get(&correlation) { work.attach_menu_open_source(open.clone())?; } Ok(work) }) {
                        Ok(work) => captured.push((correlation, work)),
                        Err(reason) => {
                            match super::cluster_action_work::SourceCaptureFailure::from(reason.clone()) {
                                super::cluster_action_work::SourceCaptureFailure::RetainedCapacity => { blocked_proposal_actors.insert((key.0, key.1)); }
                                super::cluster_action_work::SourceCaptureFailure::IntrinsicResourceLimit | super::cluster_action_work::SourceCaptureFailure::Invalid(_) => { resource_rejected_proposals.insert(correlation, reason); }
                            }
                            captured.clear();
                            break;
                        }
                    }
                }
                if captured.len() != input_count { continue; }
                if let Some(reason) = captured.iter().find_map(|(_, work)| validate_captured_source_resources(std::iter::once(work)).err()) {
                    resource_rejected_proposals.extend(captured.into_iter().map(|(id, _)| (id, reason.clone())));
                    continue;
                }
                if validate_captured_source_resources(frozen_proposal_work.values().chain(captured.iter().map(|(_, work)| work))).is_err() {
                    blocked_proposal_actors.insert((key.0, key.1));
                    continue;
                }
                frozen_proposal_work.extend(captured);
            }
            let mut unallocated_source_phases = BTreeMap::<_, Vec<_>>::new();
            for input in &pending_action_inputs {
                let ActionInput::Proposal(proposal) = input else { continue; };
                if let Some(work) = frozen_proposal_work.get(&proposal.id)
                    && work.allocated_ids().is_empty()
                {
                    unallocated_source_phases.entry((proposal.id.actor, proposal.id.incarnation, work.tick)).or_default().push(proposal.id);
                }
            }
            for (key, correlations) in unallocated_source_phases {
                if let Some(reason) = correlations.iter().find_map(|correlation| frozen_proposal_work[correlation].source_readiness().err()) {
                    match super::cluster_action_work::SourceCaptureFailure::from(reason.clone()) {
                        super::cluster_action_work::SourceCaptureFailure::RetainedCapacity => { blocked_proposal_actors.insert((key.0, key.1)); }
                        super::cluster_action_work::SourceCaptureFailure::IntrinsicResourceLimit | super::cluster_action_work::SourceCaptureFailure::Invalid(_) => {
                            for correlation in correlations { frozen_proposal_work.remove(&correlation); resource_rejected_proposals.insert(correlation, reason.clone()); }
                        }
                    }
                    continue;
                }
                let count = correlations.iter().try_fold(0_u32, |total, correlation| {
                    let count = frozen_proposal_work[correlation].output_count()?;
                    total.checked_add(count).ok_or(pumpkin_cluster::canonical_action::ApplyFailure::ResourceLimit)
                }).and_then(|count| {
                    if count > pumpkin_cluster::action_issue::MAX_ACTIONS_PER_ACTOR_TICK {
                        Err(pumpkin_cluster::canonical_action::ApplyFailure::ResourceLimit)
                    } else { Ok(count) }
                });
                let count = match count {
                    Ok(count) => Some(count),
                    Err(reason) => {
                        for correlation in correlations {
                            frozen_proposal_work.remove(&correlation);
                            resource_rejected_proposals.insert(correlation, reason.clone());
                        }
                        continue;
                    }
                };
                let Some(reservation) = count.and_then(|count| action_issue_clock.reserve(key.0, key.1, key.2, count)) else {
                    blocked_proposal_actors.insert((key.0, key.1));
                    continue;
                };
                let mut offset = 0;
                for correlation in correlations {
                    let work = frozen_proposal_work.get_mut(&correlation).unwrap();
                    let count = work.output_count().unwrap();
                    let ids = (offset..offset + count).map(|index| reservation.id(index, correlation.family).unwrap()).collect();
                    work.allocate_ids(ids).unwrap();
                    offset += count;
                }
            }
            for input in core::mem::take(&mut pending_action_inputs) {
                let input_id = match &input { ActionInput::Complete(action) => action.id, ActionInput::Proposal(proposal) => proposal.id };
                if let Some(reason) = resource_rejected_proposals.remove(&input_id) {
                    if let Some(open) = pending_inventory_open_sources.remove(&input_id) {
                        if let Some(player) = super::cluster_invsee::find_inventory_player(&server, open.screen.session.actor) {
                            let _ = player.cluster_current_inventory_session.compare_exchange(open.screen.session.session, 0, std::sync::atomic::Ordering::AcqRel, std::sync::atomic::Ordering::Acquire);
                        }
                    }
                    if let ActionInput::Proposal(proposal) = &input {
                        if let Some((origin, _, request)) = proposal.command.as_ref()
                            && let Err(observation) = server.cluster_player_commands.record_input_failure(*origin, request.correlation,
                                super::cluster_player_commands::PlayerCommandSubmitError::InvalidInput) {
                            command_observations.push_back(observation);
                        }
                        super::cluster_world_inventory_actions::reject_inventory_proposal(&server, &inventory, proposal, proposal.tick);
                        if matches!(proposal.claim, pumpkin_cluster::canonical_action::ClaimedAction::ItemUse(_)) {
                            super::cluster_item_use::reject(&server, proposal);
                        }
                    }
                    if canonical_rejections.len() < 1024 && canonical_rejections.insert(input_id) {
                        tracing::warn!(?input_id, ?reason, "cluster original source input rejected");
                    }
                    continue;
                }

                let input_tick = match &input { ActionInput::Complete(action) => action.tick, ActionInput::Proposal(proposal) => proposal.tick };
                let peer_input = matches!(input_id.actor, pumpkin_cluster::actor::ActorId::Peer(_));
                let prior_callback = |reference: &pumpkin_cluster::canonical_action::ActionRef| {
                    reference.tick != input_tick && input_tick.is_after(reference.tick)
                        || reference.tick == input_tick && reference.id.sequence != input_id.sequence
                            && input_id.sequence.wrapping_sub(reference.id.sequence) < 32768
                };
                if blocked_proposal_actors.contains(&(input_id.actor, input_id.incarnation))
                    || retained_internal_actors.contains(&(input_id.actor, input_id.incarnation))
                    || frozen_mining_work.iter().any(|(head, work)| head.actor == input_id.actor && head.incarnation == input_id.incarnation && *head != input_id && (work.proposal.tick != input_tick && input_tick.is_after(work.proposal.tick) || work.proposal.tick == input_tick && input_id.sequence.wrapping_sub(head.sequence) < 32768))
                    || frozen_proposal_work.iter().any(|(head, work)| {
                        head.actor == input_id.actor && head.incarnation == input_id.incarnation && *head != input_id
                            && frozen_proposal_work.get(&input_id).is_none_or(|candidate| candidate.tick.is_after(work.tick)
                                || candidate.tick == work.tick && candidate.proposal.id.sequence != work.proposal.id.sequence
                                    && candidate.proposal.id.sequence.wrapping_sub(work.proposal.id.sequence) < 32768)
                    })
                    || callback_work.oldest_reference(input_id.actor, input_id.incarnation).is_some_and(|reference| {
                        if peer_input { prior_callback(&reference) }
                        else { !matches!(&input, ActionInput::Complete(action) if action.id == reference.id && action.tick == reference.tick) }
                    })
                    || completed_callback_work.keys().any(|reference| reference.id.actor == input_id.actor
                        && reference.id.incarnation == input_id.incarnation
                        && if peer_input { prior_callback(reference) } else { reference.id != input_id })
                {
                    pending_action_inputs.push(input);
                    continue;
                }
                if let ActionInput::Complete(action) = &input {
                    let reference = pumpkin_cluster::canonical_action::ActionRef { id: action.id, tick: action.tick };
                    if let Some(mut work) = completed_callback_work.remove(&reference) {
                        if peer_input && !work.is_issued() && let Some(bank) = environment_source_bank.as_ref().filter(|bank| bank.tick() == reference.tick) {
                            match bank.prepare_category(&work) {
                                Ok((draft, true)) => {
                                    match callback_work.resume_environment(work, draft) {
                                        Ok(()) => {}
                                        Err(work) => {
                                            completed_callback_work.insert(reference, work);
                                            pending_action_inputs.push(input);
                                        }
                                    }
                                    continue;
                                }
                                Ok((draft, false)) => { work.replace_draft(draft); }
                                Err(reason) => {
                                    if environment_source_fault.is_none() {
                                        environment_source_fault = Some(reference.tick);
                                        tracing::error!(?reference, ?reason, "cluster environmental category merge invariant failed");
                                    }
                                    completed_callback_work.insert(reference, work);
                                    continue;
                                }
                            }
                        }
                        let prepared = if work.is_issued() { Ok(()) } else { canonical_scopes.get(&reference.tick)
                            .ok_or(pumpkin_cluster::canonical_action::ApplyFailure::InvalidAction)
                            .and_then(|scope| match work.action.id.actor {
                                pumpkin_cluster::actor::ActorId::Peer(_) => prepare_frozen_environment_action(scope, &work.action, &work.view()),
                                pumpkin_cluster::actor::ActorId::Entity(_) => prepare_frozen_entity_action(scope, &work.action, &work.view()),
                                pumpkin_cluster::actor::ActorId::Player(_) => prepare_frozen_player_action(scope, &work.action, &work.view()),
                            }).map(|_| ()) };
                        match prepared {
                            Ok(_) => match work.issue() {
                                Ok(()) => {
                                    let mut peer_page = None;
                                    if peer_input {
                                        let mut batch = TickBatch::new(reference.tick);
                                        batch.canonical_actions.push(work.draft().actions().last().unwrap().clone());
                                        let prior = local_finishes.source_prefix(reference.tick,
                                            canonical_queued.get(&reference.tick).map_or(&[], Vec::as_slice), &canonical_reserved_batches);
                                        let mut prior_realm = local_realm_issued.get(&reference.tick).cloned().unwrap_or_default();
                                        for page in canonical_reserved_batches.iter().filter(|page| page.batch.tick == reference.tick) {
                                            for (key, issued) in &page.realm.issued { prior_realm.entry(*key).or_default().extend(issued.iter().copied()); }
                                        }
                                        peer_page = prior.ok().and_then(|prior| canonical_scopes.get(&reference.tick).and_then(|scope| {
                                            holistic_outbox.reserve_source_batch(scope, batch, &prior, &prior_realm, &realm_completion,
                                                |action| chunk_blocks.historical_entity_authority(action).or_else(|| entity_authority_at_action_tick(&canonical_graphs, action))).ok()
                                        }));
                                        if peer_page.is_none() {
                                            completed_callback_work.insert(reference, work);
                                            pending_action_inputs.push(input);
                                            continue;
                                        }
                                    }
                                    if let Some(bank) = environment_source_bank.as_mut()
                                        && reference.id.actor == pumpkin_cluster::actor::ActorId::Peer(local)
                                        && bank.tick() == reference.tick
                                    {
                                        if let Err(reason) = bank.observe_category(&work) {
                                            if let Some(page) = &peer_page { holistic_outbox.release_canonical_reservation(&page.parcels); }
                                            if environment_source_fault.is_none() {
                                                environment_source_fault = Some(reference.tick);
                                                tracing::error!(?reference, ?reason, "cluster environmental source draft invariant failed");
                                            }
                                            completed_callback_work.insert(reference, work);
                                            continue;
                                        }
                                    }
                                    let draft = work.draft().clone();
                                    let action = draft.actions().last().unwrap().clone();
                                    actor_drafts.insert((action.id.actor, action.id.incarnation, action.tick), draft);
                                    canonical_pending.insert((action.tick, action.id), PendingCanonical { action: action.clone() });
                                    if let Some(page) = peer_page { canonical_reserved_batches.push_back(page); }
                                    else { canonical_queued.entry(action.tick).or_default().push(action); }
                                    callback_input_order.remove(&reference);
                                }
                                Err(reason) => {
                                    callback_input_order.remove(&reference);
                                    if canonical_rejections.len() < 1024 && canonical_rejections.insert(reference.id) {
                                        tracing::error!(?reference, ?reason, "cluster completed callback source issue failed");
                                    }
                                    administration.publish_canonical_ground(&server, &chunk_blocks, &work.action, false);
                                }
                            },
                            Err(pumpkin_cluster::canonical_action::ApplyFailure::Capacity
                                | pumpkin_cluster::canonical_action::ApplyFailure::MissingStakeholder(_)) => {
                                blocked_proposal_actors.insert((reference.id.actor, reference.id.incarnation));
                                completed_callback_work.insert(reference, work);
                                pending_action_inputs.push(input);
                            }
                            Err(reason) => {
                                callback_input_order.remove(&reference);
                                if canonical_rejections.len() < 1024 && canonical_rejections.insert(reference.id) {
                                    tracing::error!(?reference, ?reason, "cluster completed callback original scope invalid");
                                }
                                administration.publish_canonical_ground(&server, &chunk_blocks, &work.action, false);
                            }
                        }
                        continue;
                    }
                }
                if let ActionInput::Proposal(original) = &input
                    && matches!(original.claim, pumpkin_cluster::canonical_action::ClaimedAction::Block(_))
                {
                    let correlation = original.id;
                    let mut work = if let Some(work) = frozen_mining_work.remove(&correlation) { work } else {
                        let owned = super::cluster_world_canonical::OwnedWorldActionState { server: &server, blocks: &mut chunk_blocks, actors: &mut actors, living: &mut living, inventory: &mut inventory, versions: &mut canonical_versions };
                        let draft = actor_drafts.entry((correlation.actor, correlation.incarnation, resolution_tick)).or_insert_with(|| pumpkin_cluster::actor_action_scope::ActorActionDraft::new(correlation.actor, correlation.incarnation, resolution_tick, 4096, 16384));
                        match source_actor_snapshot.clone().ok_or(pumpkin_cluster::canonical_action::ApplyFailure::Capacity)
                            .and_then(|snapshot| super::cluster_world_mining_context::FrozenMiningInput::capture_from_snapshot(snapshot, server.clone(), &owned, draft, original.clone(), resolution_tick, &canonical_scopes[&resolution_tick].scheduled_executors())) {
                            Ok(work) => {
                                if let Some(needs) = mining_chunk_needs.remove(&correlation) { pending_admin_need_releases.extend(needs); }
                                work
                            },
                            Err(pumpkin_cluster::canonical_action::ApplyFailure::MissingStakeholder(pumpkin_cluster::canonical_action::Stakeholder::Chunk(chunk))) => {
                                let needs = mining_chunk_needs.entry(correlation).or_default();
                                if !needs.contains_key(&chunk) && let Some(route) = server.cluster_chunk_need_route.load_full() {
                                    next_admin_need_lease = next_admin_need_lease.wrapping_add(1).max(1);
                                    let lease = next_admin_need_lease;
                                    if route.try_send(super::cluster::ChunkNeed::Acquire { chunk, lease, ready: None }).is_ok() { needs.insert(chunk, lease); }
                                }
                                blocked_proposal_actors.insert((correlation.actor, correlation.incarnation)); pending_action_inputs.push(input); continue;
                            }
                            Err(pumpkin_cluster::canonical_action::ApplyFailure::Capacity | pumpkin_cluster::canonical_action::ApplyFailure::MissingStakeholder(_)) => {
                                blocked_proposal_actors.insert((correlation.actor, correlation.incarnation)); pending_action_inputs.push(input); continue;
                            }
                            Err(reason) => {
                                if let Some(needs) = mining_chunk_needs.remove(&correlation) { pending_admin_need_releases.extend(needs); }
                                if canonical_rejections.len() < 1024 && canonical_rejections.insert(correlation) { tracing::warn!(?correlation, ?reason, "cluster mining capture rejected"); }
                                if let pumpkin_cluster::canonical_action::ClaimedAction::Block(claim) = &original.claim { let _ = super::cluster_inventory_protocol::publish_block_action_resolution(&server, claim.intent.actor, claim.intent.position, None, claim.intent.client_sequence, false); }
                                continue;
                            }
                        }
                    };
                    let decision = work.stage(&mut block_actions);
                    let pumpkin_cluster::canonical_action::ClaimedAction::Block(claim) = &work.proposal.claim else { unreachable!() };
                    let intent = claim.intent;
                    let source = canonical_scopes[&work.proposal.tick].source(local).unwrap();
                    if !work.cosmetic_prepared {
                        let animation = match decision {
                            pumpkin_cluster::block_action::MiningDecision::Started { stage } => Some((stage as i8, true)),
                            pumpkin_cluster::block_action::MiningDecision::Advanced { stage } => Some((stage as i8, false)),
                            pumpkin_cluster::block_action::MiningDecision::Broken { .. } | pumpkin_cluster::block_action::MiningDecision::Aborted | pumpkin_cluster::block_action::MiningDecision::Rejected => Some((-1, false)),
                            pumpkin_cluster::block_action::MiningDecision::Ignored => None,
                        };
                        work.cosmetic = animation.map(|(stage, start)| super::cluster_player_cosmetics::owned_break_animation(work.context.lease, source, work.context.epoch, work.proposal.tick, pumpkin_cluster::replica_visual::PlayerBreakAnimation { position: [intent.position.x, intent.position.y, intent.position.z], stage, bedrock_rate: None, start }));
                        work.cosmetic_prepared = true;
                    }
                    if let Some(cosmetic) = work.cosmetic.take()
                        && let Err(cosmetic) = super::cluster_player_cosmetics::try_send_cosmetic(&server, cosmetic)
                    {
                        work.cosmetic = Some(cosmetic); frozen_mining_work.insert(correlation, work); blocked_proposal_actors.insert((correlation.actor, correlation.incarnation)); pending_action_inputs.push(input); continue;
                    }
                    if !matches!(decision, pumpkin_cluster::block_action::MiningDecision::Broken { .. }) {
                        let accepted = !matches!(decision, pumpkin_cluster::block_action::MiningDecision::Rejected);
                        let _ = super::cluster_inventory_protocol::publish_block_action_resolution(&server, intent.actor, intent.position, None, intent.client_sequence, accepted);
                        continue;
                    }
                    if !work.identity_bound {
                        let Some(reservation) = action_issue_clock.reserve(correlation.actor, correlation.incarnation, work.proposal.tick, 1) else {
                            frozen_mining_work.insert(correlation, work); blocked_proposal_actors.insert((correlation.actor, correlation.incarnation)); pending_action_inputs.push(input); continue;
                        };
                        work.proposal.id = reservation.id(0, correlation.family).unwrap(); work.identity_bound = true;
                    }
                    let reference = pumpkin_cluster::canonical_action::ActionRef { id: work.proposal.id, tick: work.proposal.tick };
                    let Some(world) = world_for(&server, intent.target_chunk.world) else {
                        frozen_mining_work.insert(correlation, work); blocked_proposal_actors.insert((correlation.actor, correlation.incarnation)); pending_action_inputs.push(input); continue;
                    };
                    match callback_work.stage_mining(work, source, world) {
                        Ok(()) => { callback_input_order.insert(reference, correlation); }
                        Err(work) => { frozen_mining_work.insert(correlation, work); blocked_proposal_actors.insert((correlation.actor, correlation.incarnation)); pending_action_inputs.push(input); }
                    }
                    continue;
                }
                if let ActionInput::Proposal(original) = &input
                    && super::cluster_action_work::FrozenProposalWork::supports(original)
                {
                    let correlation = original.id;
                    if !frozen_proposal_work.contains_key(&correlation) {
                        let proposal = original.clone();
                        let owned = super::cluster_world_canonical::OwnedWorldActionState {
                            server: &server, blocks: &mut chunk_blocks, actors: &mut actors,
                            living: &mut living, inventory: &mut inventory, versions: &mut canonical_versions,
                        };
                        let draft = actor_drafts.entry((correlation.actor, correlation.incarnation, proposal.tick)).or_insert_with(|| {
                            pumpkin_cluster::actor_action_scope::ActorActionDraft::new(correlation.actor, correlation.incarnation, proposal.tick, 4096, 16384)
                        });
                        let source_tick = proposal.tick;
                        let Some(scope) = canonical_scopes.get(&source_tick) else { pending_action_inputs.push(input); continue; };
                        let original_snapshot = scope.full_source_slot.and_then(|slot| source_actor_snapshots.get(&slot)).map(|(snapshot, _, _)| snapshot.clone());
                        match original_snapshot.ok_or(pumpkin_cluster::canonical_action::ApplyFailure::Capacity)
                            .and_then(|snapshot| super::cluster_action_work::FrozenProposalWork::capture_static_from_snapshot(snapshot, server.clone(), &owned, draft, proposal, source_tick,
                            scope.source(local).unwrap(), &scope.scheduled_executors()))
                            .and_then(|mut work| { if let Some(open) = pending_inventory_open_sources.get(&correlation) { work.attach_menu_open_source(open.clone())?; } Ok(work) }) {
                            Ok(work) => {
                                frozen_proposal_work.insert(correlation, work);
                                blocked_proposal_actors.insert((correlation.actor, correlation.incarnation));
                                pending_action_inputs.push(input);
                                continue;
                            }
                            Err(pumpkin_cluster::canonical_action::ApplyFailure::Capacity
                                | pumpkin_cluster::canonical_action::ApplyFailure::MissingStakeholder(_)) => {
                                blocked_proposal_actors.insert((correlation.actor, correlation.incarnation));
                                pending_action_inputs.push(input);
                                continue;
                            }
                            Err(reason) => {
                                if let Some((origin, _, request)) = original.command.as_ref() {
                                    if let Err(observation) = server.cluster_player_commands.record_input_failure(*origin, request.correlation,
                                        super::cluster_player_commands::PlayerCommandSubmitError::InvalidInput) { command_observations.push_back(observation); }
                                }
                                if canonical_rejections.len() < 1024 && canonical_rejections.insert(correlation) {
                                    tracing::warn!(?correlation, ?reason, "cluster source work capture rejected");
                                }
                                super::cluster_world_inventory_actions::reject_inventory_proposal(&server, &inventory, original, original.tick);
                                if matches!(original.claim, pumpkin_cluster::canonical_action::ClaimedAction::ItemUse(_)) {
                                    super::cluster_item_use::reject(&server, original);
                                }
                                continue;
                            }
                        }
                    }
                    let mut work = frozen_proposal_work.remove(&correlation).unwrap();
                    let original_cut = work.original_snapshot();
                    let missing_chunks = work.historical_footprint().into_iter().filter(|chunk| !original_cut.original_store().contains(*chunk)).collect::<Vec<_>>();
                    if !missing_chunks.is_empty() {
                        let Some(slot) = original_cut.original_source_slot() else {
                            frozen_proposal_work.insert(correlation, work);
                            pending_action_inputs.push(input);
                            continue;
                        };
                        let mut cuts = Vec::new();
                        for chunk in &missing_chunks {
                            let key = (slot,*chunk);
                            if let Some((cut,_)) = historical_actor_cuts.get(&key) { cuts.push(cut.clone()); continue; }
                            if !historical_actor_need_pins.contains_key(&key) && !local_finishes.closed_through(work.tick)
                                && let Ok(providers) = original_cut.original_frozen_holders(*chunk)
                                && let Some(next) = next_historical_actor_lease.checked_add(1)
                                && let Some(pin) = super::cluster::try_require_original_actor_chunk(&server,*chunk,slot,next_historical_actor_lease,providers)
                            {
                                next_historical_actor_lease = next;
                                historical_actor_need_pins.insert(key,pin);
                            }
                        }
                        if cuts.len() != missing_chunks.len() || work.enrich_original_chunks(&cuts).is_err() {
                            blocked_proposal_actors.insert((correlation.actor,correlation.incarnation));
                            frozen_proposal_work.insert(correlation,work);
                            pending_action_inputs.push(input);
                            continue;
                        }
                    }
                    let ids = work.allocated_ids().to_vec();
                    let bound = entity_origin_allocator.as_mut().ok_or(pumpkin_cluster::canonical_action::ApplyFailure::Capacity)
                        .and_then(|allocator| work.bind_ids(ids, allocator));
                    if let Err(reason) = bound {
                        if matches!(reason, pumpkin_cluster::canonical_action::ApplyFailure::Capacity) {
                            blocked_proposal_actors.insert((correlation.actor, correlation.incarnation));
                            frozen_proposal_work.insert(correlation, work);
                            pending_action_inputs.push(input);
                        } else {
                            if let Some(allocator) = entity_origin_allocator.as_mut() {
                                allocator.discard_unissued(pumpkin_cluster::canonical_action::ActionRef { id: work.proposal.id, tick: work.tick });
                            }
                            if canonical_rejections.len() < 1024 && canonical_rejections.insert(correlation) {
                            tracing::error!(?correlation, ?reason, "cluster frozen source identity binding failed");
                            }
                            super::cluster_world_inventory_actions::reject_inventory_proposal(&server, &inventory, original, original.tick);
                            if matches!(original.claim, pumpkin_cluster::canonical_action::ClaimedAction::ItemUse(_)) {
                                super::cluster_item_use::reject(&server, original);
                            }
                        }
                        continue;
                    }
                    if !work.is_prepared() && let Some(draft) = actor_drafts.get(&(correlation.actor, correlation.incarnation, work.tick)) {
                        work.replace_draft(draft.clone());
                    }
                    let targets = work.targets().to_vec();
                    let work_tick = work.tick;
                    let result = work.expand(&canonical_graphs, &targets).and_then(|_| work.prepare_outputs(|action, view| {
                            let scope = canonical_scopes.get(&work_tick).ok_or(pumpkin_cluster::canonical_action::ApplyFailure::InvalidAction)?;
                            match action.id.actor {
                                pumpkin_cluster::actor::ActorId::Peer(_) => prepare_frozen_environment_action(scope, action, view)?,
                                pumpkin_cluster::actor::ActorId::Entity(_) => prepare_frozen_entity_action(scope, action, view)?,
                                pumpkin_cluster::actor::ActorId::Player(_) => prepare_frozen_player_action(scope, action, view)?,
                            };
                            Ok(())
                        }));
                    match result {
                        Ok(actions) => {
                            let action = &actions[0];
                            let reference = pumpkin_cluster::canonical_action::ActionRef { id: action.id, tick: action.tick };
                            let mut batch = TickBatch::new(work_tick);
                            batch.canonical_actions = actions.clone();
                            let prior = local_finishes.source_prefix(work_tick,
                                canonical_queued.get(&work_tick).map_or(&[], Vec::as_slice), &canonical_reserved_batches);
                            let mut prior_realm = local_realm_issued.get(&work_tick).cloned().unwrap_or_default();
                            for page in canonical_reserved_batches.iter().filter(|page| page.batch.tick == work_tick) {
                                for (key, issued) in &page.realm.issued { prior_realm.entry(*key).or_default().extend(issued.iter().copied()); }
                            }
                            let page = prior.ok().and_then(|prior| canonical_scopes.get(&work_tick).and_then(|scope| {
                                holistic_outbox.reserve_source_batch(scope, batch, &prior, &prior_realm, &realm_completion,
                                    |action| chunk_blocks.historical_entity_authority(action).or_else(|| entity_authority_at_action_tick(&canonical_graphs, action))).ok()
                            }));
                            let Some(page) = page else {
                                blocked_proposal_actors.insert((correlation.actor, correlation.incarnation));
                                frozen_proposal_work.insert(correlation, work);
                                pending_action_inputs.push(input);
                                continue;
                            };
                            if let Err(reason) = work.reserve_open_presentation(&server) {
                                holistic_outbox.release_canonical_reservation(&page.parcels);
                                if matches!(reason, pumpkin_cluster::canonical_action::ApplyFailure::Capacity) {
                                    blocked_proposal_actors.insert((correlation.actor, correlation.incarnation));
                                    frozen_proposal_work.insert(correlation, work);
                                    pending_action_inputs.push(input);
                                } else {
                                    super::cluster_world_inventory_actions::reject_inventory_proposal(&server, &inventory, original, original.tick);
                                }
                                continue;
                            }
                            if matches!(work.proposal.claim, pumpkin_cluster::canonical_action::ClaimedAction::InventoryClose(_)) {
                                if inventory.track_canonical_close(action).is_err()
                                    || !inventory.can_attach_canonical_close_notification(reference)
                                {
                                    holistic_outbox.release_canonical_reservation(&page.parcels);
                                    blocked_proposal_actors.insert((correlation.actor, correlation.incarnation));
                                    frozen_proposal_work.insert(correlation, work);
                                    pending_action_inputs.push(input);
                                    continue;
                                }
                                if let Some(notification) = work.take_close_notification() {
                                    inventory.attach_canonical_close_notification(reference, notification).unwrap();
                                }
                            }
                            if let Some((origin, _, request)) = work.proposal.command.as_ref() {
                                if !command_observations.is_empty()
                                    || server.cluster_player_commands.register_action(*origin, request.correlation, reference, super::cluster_owned_mob_admission::command_receipt(&action).cloned()).is_err()
                                {
                                    holistic_outbox.release_canonical_reservation(&page.parcels);
                                    blocked_proposal_actors.insert((correlation.actor, correlation.incarnation));
                                    frozen_proposal_work.insert(correlation, work);
                                    pending_action_inputs.push(input);
                                    continue;
                                }
                            }
                            {
                                    let draft = work.draft().clone();
                                    if let Some((origin, _, request)) = work.proposal.command.as_ref() {
                                        canonical_commands.insert(reference, (*origin, request.correlation));
                                        if let Some(need) = pending_admin_needs.remove(&correlation) { canonical_command_needs.insert(reference, need); }
                                    }
                                    if let pumpkin_cluster::canonical_action::ClaimedAction::PlayerAdministration(update) = &work.proposal.claim {
                                        let mut update = update.clone();
                                        update.tick = action.tick;
                                        update.seq = pumpkin_cluster::identity::PlayerSeq(action.id.sequence);
                                        canonical_player_admin.insert((action.tick, action.id), (update, pending_admin_needs.remove(&correlation)));
                                    }
                                    #[cfg(feature = "cluster-e2e")]
                                    if inventory_scenario_source.tracks_inventory_source(original) {
                                        let references = actions.iter().map(|action| pumpkin_cluster::canonical_action::ActionRef { id: action.id, tick: action.tick }).collect::<Vec<_>>();
                                        inventory_scenario_issued.insert(reference, (original.clone(), references));
                                    }
                                    pending_inventory_open_sources.remove(&correlation);
                                    work.publish_open_presentation();
                                    actor_drafts.insert((action.id.actor, action.id.incarnation, action.tick), draft);
                                    for action in &actions {
                                        canonical_pending.insert((action.tick, action.id), PendingCanonical { action: action.clone() });
                                    }
                                    canonical_reserved_batches.push_back(page);
                            }
                        }
                        Err(reason @ (pumpkin_cluster::canonical_action::ApplyFailure::Capacity
                            | pumpkin_cluster::canonical_action::ApplyFailure::MissingStakeholder(_))) => {
                            if matches!(work.proposal.claim, pumpkin_cluster::canonical_action::ClaimedAction::PlayerCommand
                                | pumpkin_cluster::canonical_action::ClaimedAction::EntityAdmissionRequest(_)
                                | pumpkin_cluster::canonical_action::ClaimedAction::PlayerAdministration(_))
                                && let pumpkin_cluster::canonical_action::ApplyFailure::MissingStakeholder(pumpkin_cluster::canonical_action::Stakeholder::Chunk(chunk)) = &reason
                                && pending_admin_needs.get(&correlation).is_none_or(|(held, _)| *held != *chunk)
                            {
                                if let Some(need) = pending_admin_needs.remove(&correlation) { pending_admin_need_releases.push_back(need); }
                                if let Some(route) = server.cluster_chunk_need_route.load_full() {
                                    next_admin_need_lease = next_admin_need_lease.wrapping_add(1).max(1);
                                    let lease = next_admin_need_lease;
                                    if route.try_send(super::cluster::ChunkNeed::Acquire { chunk: *chunk, lease, ready: Some(admin_chunk_ready_tx.clone()) }).is_ok() {
                                        pending_admin_needs.insert(correlation, (*chunk, lease));
                                    }
                                }
                            }
                            blocked_proposal_actors.insert((correlation.actor, correlation.incarnation));
                            frozen_proposal_work.insert(correlation, work);
                            pending_action_inputs.push(input);
                        }
                        Err(reason) => {
                            if matches!(work.proposal.claim, pumpkin_cluster::canonical_action::ClaimedAction::InventoryClose(_)) {
                                let reference = pumpkin_cluster::canonical_action::ActionRef { tick: work.tick, id: work.proposal.id };
                                let _ = inventory.finish_canonical_close(&server, reference, false, false);
                            }
                            if let Some(need) = pending_admin_needs.remove(&correlation) { pending_admin_need_releases.push_back(need); }
                            if let Some((origin, _, request)) = work.proposal.command.as_ref() {
                                if let Err(observation) = server.cluster_player_commands.record_input_failure(*origin, request.correlation,
                                    super::cluster_player_commands::PlayerCommandSubmitError::InvalidInput) { command_observations.push_back(observation); }
                            }
                            if canonical_rejections.len() < 1024 && canonical_rejections.insert(correlation) {
                                tracing::warn!(?correlation, ?reason, "cluster frozen source materialization rejected");
                            }
                            super::cluster_world_inventory_actions::reject_inventory_proposal(&server, &inventory, original, original.tick);
                            if matches!(original.claim, pumpkin_cluster::canonical_action::ClaimedAction::ItemUse(_)) {
                                super::cluster_item_use::reject(&server, original);
                            }
                        }
                    }
                    continue;
                }
                let mut command_origin = None;
                let (action, block_correction, admin_update) = match input {
                    ActionInput::Complete(action) => (Some(action), None, None),
                    ActionInput::Proposal(mut proposal) => {
                        let proposal_id = proposal.id;
                        let Some(reservation) = action_issue_clock.reserve(proposal.id.actor, proposal.id.incarnation, resolution_tick, 1) else {
                            blocked_proposal_actors.insert((proposal.id.actor, proposal.id.incarnation));
                            pending_action_inputs.push(ActionInput::Proposal(proposal));
                            continue;
                        };
                        proposal.id = reservation.id(0, proposal.id.family).unwrap();
                        if matches!(proposal.claim, pumpkin_cluster::canonical_action::ClaimedAction::PlayerCommand) {
                            if !command_observations.is_empty() {
                                proposal.id = proposal_id;
                                pending_action_inputs.push(ActionInput::Proposal(proposal));
                                continue;
                            }
                            let result = capture_player_command(&server, &actors, &living, &inventory, &proposal, resolution_tick)
                                .and_then(|(mut metadata, expected, contact)| {
                                    let owned = super::cluster_world_canonical::OwnedWorldActionState {
                                        server: &server, blocks: &mut chunk_blocks, actors: &mut actors,
                                        living: &mut living, inventory: &mut inventory, versions: &mut canonical_versions,
                                    };
                                    let draft = actor_drafts.entry((proposal.id.actor, proposal.id.incarnation, resolution_tick)).or_insert_with(|| {
                                        pumpkin_cluster::actor_action_scope::ActorActionDraft::new(proposal.id.actor, proposal.id.incarnation, resolution_tick, 4096, 16384)
                                    });
                                    metadata.association = draft.player_association(metadata.lease.gid, metadata.association)
                                        .ok_or(super::cluster_player_commands::PlayerCommandSubmitError::NoTarget)?;
                                    materialize_player_command(&draft.view(&owned), metadata, expected, contact, None, &proposal, resolution_tick, |chunk| {
                                        world_for(&server, chunk.world).is_some() && owned.blocks.contains(chunk)
                                            && !pending_fresh.contains_key(&chunk) && !pending_imports.contains_key(&chunk)
                                            && !buckets.is_quarantined(chunk)
                                            && pending_admin_needs.get(&proposal_id).is_none_or(|need| need.0 != chunk || ready_admin_needs.contains_key(need))
                                    }).map_err(|_| super::cluster_player_commands::PlayerCommandSubmitError::InvalidInput)
                                });
                            let (origin, _, request) = proposal.command.as_ref().unwrap();
                            match result {
                                Ok(Ok(action)) => {
                                    let reference = pumpkin_cluster::canonical_action::ActionRef { id: action.id, tick: action.tick };
                                    if server.cluster_player_commands.register_action(*origin, request.correlation, reference, super::cluster_owned_mob_admission::command_receipt(&action).cloned()).is_err() {
                                        proposal.id = proposal_id;
                                        pending_action_inputs.push(ActionInput::Proposal(proposal));
                                        continue;
                                    }
                                    command_origin = Some((*origin, request.correlation));
                                    if let Some(need) = pending_admin_needs.remove(&proposal_id) { canonical_command_needs.insert(reference, need); }
                                    (Some(action), None, None)
                                }
                                Ok(Err(chunk)) => {
                                    if pending_admin_needs.get(&proposal_id).is_none_or(|(held, _)| *held != chunk) {
                                        if let Some(need) = pending_admin_needs.remove(&proposal_id) { pending_admin_need_releases.push_back(need); }
                                        if let Some(route) = server.cluster_chunk_need_route.load_full() {
                                            next_admin_need_lease = next_admin_need_lease.wrapping_add(1).max(1);
                                            let lease = next_admin_need_lease;
                                            if route.try_send(super::cluster::ChunkNeed::Acquire { chunk, lease, ready: Some(admin_chunk_ready_tx.clone()) }).is_ok() {
                                                pending_admin_needs.insert(proposal_id, (chunk, lease));
                                            }
                                        }
                                    }
                                    proposal.id = proposal_id;
                                    pending_action_inputs.push(ActionInput::Proposal(proposal));
                                    (None, None, None)
                                }
                                Err(reason) => {
                                    if let Some(need) = pending_admin_needs.remove(&proposal_id) { pending_admin_need_releases.push_back(need); }
                                    if let Err(observation) = server.cluster_player_commands.record_input_failure(*origin, request.correlation, reason) {
                                        command_observations.push_back(observation);
                                    }
                                    (None, None, None)
                                }
                            }
                        } else if let pumpkin_cluster::canonical_action::ClaimedAction::PlayerAdministration(update) = &proposal.claim {
                            let mut update = update.clone();
                            update.tick = resolution_tick;
                            update.seq = pumpkin_cluster::identity::PlayerSeq(proposal.id.sequence);
                            let result = (|| {
                                let lease = proposal.lease.ok_or(pumpkin_cluster::canonical_action::ApplyFailure::InvalidAction)?;
                                if !inventory.live_player_lease_matches(lease)
                                    || statistics.incarnation(update.target) != Some(lease.incarnation)
                                    || actors.local_key(update.target) != Some(lease.key.0)
                                { return Err(pumpkin_cluster::canonical_action::ApplyFailure::InvalidAction); }
                                let actor = actors.get(update.target).ok_or(pumpkin_cluster::canonical_action::ApplyFailure::InvalidAction)?;
                                let effective = actors.input_projection(update.target).ok_or(pumpkin_cluster::canonical_action::ApplyFailure::InvalidAction)?;
                                let epoch = living.get(pumpkin_cluster::actor::ActorId::Player(update.target))
                                    .and_then(|actor| actor.local.avatar.as_ref()).map(|avatar| avatar.spatial_epoch)
                                    .ok_or(pumpkin_cluster::canonical_action::ApplyFailure::InvalidAction)?;
                                let metadata = PlayerAdministrationMaterialization {
                                    lease, association: actor.local.chunk, effective: effective.frame,
                                    epoch, id: proposal.id,
                                };
                                let owned = super::cluster_world_canonical::OwnedWorldActionState {
                                    server: &server, blocks: &mut chunk_blocks, actors: &mut actors,
                                    living: &mut living, inventory: &mut inventory, versions: &mut canonical_versions,
                                };
                                let draft = actor_drafts.entry((proposal.id.actor, proposal.id.incarnation, resolution_tick)).or_insert_with(|| {
                                    pumpkin_cluster::actor_action_scope::ActorActionDraft::new(proposal.id.actor, proposal.id.incarnation, resolution_tick, 4096, 16384)
                                });
                                materialize_player_administration(&draft.view(&owned), metadata, &proposal, &update, resolution_tick, |chunk| {
                                    world_for(&server, chunk.world).is_some()
                                        && owned.blocks.contains(chunk)
                                        && !pending_fresh.contains_key(&chunk)
                                        && !pending_imports.contains_key(&chunk)
                                        && !buckets.is_quarantined(chunk)
                                        && pending_admin_needs.get(&proposal_id).is_none_or(|need| need.0 != chunk || ready_admin_needs.contains_key(need))
                                })
                            })();
                            match result {
                                Ok(Ok(action)) => (
                                    Some(action),
                                    None,
                                    Some((update, pending_admin_needs.remove(&proposal_id))),
                                ),
                                Ok(Err(chunk)) => {
                                    if pending_admin_needs.get(&proposal_id).is_none_or(|(held, _)| *held != chunk) {
                                        if let Some((held, lease)) = pending_admin_needs.remove(&proposal_id) {
                                            pending_admin_need_releases.push_back((held, lease));
                                        }
                                        if let Some(route) = server.cluster_chunk_need_route.load_full() {
                                            next_admin_need_lease = next_admin_need_lease.wrapping_add(1).max(1);
                                            let lease = next_admin_need_lease;
                                            if route.try_send(super::cluster::ChunkNeed::Acquire { chunk, lease, ready: Some(admin_chunk_ready_tx.clone()) }).is_ok() {
                                                pending_admin_needs.insert(proposal_id, (chunk, lease));
                                            }
                                        }
                                    }
                                    if pending_admin_inputs.len() < 4_096 {
                                        proposal.id = proposal_id;
                                        pending_admin_inputs.push_back(proposal);
                                    } else {
                                        if let Some((held, lease)) = pending_admin_needs.remove(&proposal_id) {
                                            pending_admin_need_releases.push_back((held, lease));
                                        }
                                        queue_player_administration_projection(
                                            &server,
                                            &mut pending_player_admin,
                                            ActionKey::canonical_id(proposal_id),
                                            update,
                                            false,
                                        );
                                    }
                                    (None, None, None)
                                }
                                Err(reason) => {
                                    if let Some((held, lease)) = pending_admin_needs.remove(&proposal_id) {
                                        pending_admin_need_releases.push_back((held, lease));
                                    }
                                    if canonical_rejections.len() < 1_024 && canonical_rejections.insert(proposal_id) {
                                        tracing::error!(?proposal_id, ?reason, "cluster player administration proposal rejected");
                                    }
                                    queue_player_administration_projection(
                                        &server,
                                        &mut pending_player_admin,
                                        ActionKey::canonical_id(proposal_id),
                                        update,
                                        false,
                                    );
                                    (None, None, None)
                                }
                            }
                        } else if matches!(proposal.claim, pumpkin_cluster::canonical_action::ClaimedAction::ItemUse(_)) {
                            let owned = super::cluster_world_canonical::OwnedWorldActionState {
                                server: &server, blocks: &mut chunk_blocks, actors: &mut actors, living: &mut living,
                                inventory: &mut inventory, versions: &mut canonical_versions,
                            };
                            let draft = actor_drafts.entry((proposal.id.actor, proposal.id.incarnation, resolution_tick)).or_insert_with(|| {
                                pumpkin_cluster::actor_action_scope::ActorActionDraft::new(proposal.id.actor, proposal.id.incarnation, resolution_tick, 4096, 16384)
                            });
                            if source_actor_snapshot.clone().ok_or(pumpkin_cluster::canonical_action::ApplyFailure::Capacity)
                                .and_then(|snapshot| callback_work.capture_use_from_snapshot(snapshot, &server, &owned, draft, &proposal, resolution_tick, &canonical_scopes[&resolution_tick].scheduled_executors())).is_err() {
                                super::cluster_item_use::reject(&server, &proposal);
                            } else {
                                callback_input_order.insert(pumpkin_cluster::canonical_action::ActionRef { id: proposal.id, tick: proposal.tick }, proposal_id);
                            }
                            (None, None, None)
                        } else {
                            unreachable!("block proposals must pass frozen mining staging");
                        }
                    }
                };
                let Some(mut action) = action else { continue; };
                let id = action.id;
                if local_finishes.closed_through(action.tick)
                    || local_finishes
                        .last_tick
                        .is_some_and(|last| last.is_after(action.tick))
                {
                    reject_registered_player_command(&server, &mut command_observations, command_origin);
                    if let Some(need) = canonical_command_needs.remove(&pumpkin_cluster::canonical_action::ActionRef { id, tick: resolution_tick }) { pending_admin_need_releases.push_back(need); }
                    if canonical_rejections.len() < 1_024 && canonical_rejections.insert(id) {
                        tracing::warn!(?id, tick = action.tick.0, "cluster canonical action reached a closed local tick");
                    }
                    if let Some((gid, position, sequence)) = block_correction {
                        let _ = super::cluster_inventory_protocol::publish_block_action_resolution(
                            &server, gid, position, None, sequence, false,
                        );
                    }
                    if action.kind == pumpkin_cluster::canonical_action::ActionKind::World {
                        administration.publish_canonical_ground(
                            &server,
                            &chunk_blocks,
                            &action,
                            false,
                        );
                    }
                    if let Some((update, need)) = admin_update {
                        if let Some(need) = need {
                            pending_admin_need_releases.push_back(need);
                        }
                        queue_player_administration_projection(
                            &server,
                            &mut pending_player_admin,
                            ActionKey::canonical_id(id),
                            update,
                            false,
                        );
                    }
                    continue;
                }
                match prepare_canonical_action(
                    &server,
                    canonical_scopes.get(&action.tick).and_then(|scope| scope.source(local).ok()),
                    canonical_scopes.get(&action.tick).is_some_and(|scope| scope.source(local).is_ok_and(|source| scope.primary_producers.contains(&source))),
                    &canonical_graphs,
                    &mut chunk_blocks,
                    &mut actors,
                    &statistics,
                    &mut living,
                    &mut inventory,
                    &mut canonical_versions,
                    actor_drafts.get(&(action.id.actor, action.id.incarnation, action.tick)),
                    ActionInput::Complete(action.clone()),
                ) {
                    Ok(applied) => {
                        let owned = super::cluster_world_canonical::OwnedWorldActionState {
                            server: &server,
                            blocks: &mut chunk_blocks,
                            actors: &mut actors,
                            living: &mut living,
                            inventory: &mut inventory,
                            versions: &mut canonical_versions,
                        };
                        let key = (action.id.actor, action.id.incarnation, action.tick);
                        if !actor_drafts.contains_key(&key) {
                            let Ok(draft) = new_actor_draft(&owned, &action) else {
                                reject_registered_player_command(&server, &mut command_observations, command_origin);
                    if let Some(need) = canonical_command_needs.remove(&pumpkin_cluster::canonical_action::ActionRef { id, tick: resolution_tick }) { pending_admin_need_releases.push_back(need); }
                                continue;
                            };
                            actor_drafts.insert(key, draft);
                        }
                        let draft = actor_drafts.get_mut(&key).unwrap();
                        if let Err((reason, rejected)) = draft.issue(&owned, action) {
                            reject_registered_player_command(&server, &mut command_observations, command_origin);
                    if let Some(need) = canonical_command_needs.remove(&pumpkin_cluster::canonical_action::ActionRef { id, tick: resolution_tick }) { pending_admin_need_releases.push_back(need); }
                            if canonical_rejections.len() < 1024 && canonical_rejections.insert(rejected.id) {
                                tracing::error!(id = ?rejected.id, ?reason, "cluster actor draft issue rejected");
                            }
                            super::cluster_world_inventory_actions::reject_inventory_action(&server, &inventory, &rejected);
                            continue;
                        }
                        let action = draft.actions().last().unwrap().clone();
                        if let Some(origin) = command_origin {
                            canonical_commands.insert(pumpkin_cluster::canonical_action::ActionRef { id: action.id, tick: action.tick }, origin);
                        }
                        if let Some(update) = admin_update {
                            canonical_player_admin.insert((action.tick, id), update);
                        }
                        let _ = applied;
                        canonical_pending.insert((action.tick, id), PendingCanonical {
                            action: action.clone(),
                        });
                        canonical_queued.entry(action.tick).or_default().push(action);
                    }
                    Err(reason) => {
                        reject_registered_player_command(&server, &mut command_observations, command_origin);
                    if let Some(need) = canonical_command_needs.remove(&pumpkin_cluster::canonical_action::ActionRef { id, tick: resolution_tick }) { pending_admin_need_releases.push_back(need); }
                        if canonical_rejections.len() < 1_024 && canonical_rejections.insert(id) {
                            tracing::error!(?id, ?reason, "cluster canonical action admission invariant failed");
                        }
                        if let Some((gid, position, sequence)) = block_correction {
                            let _ = super::cluster_inventory_protocol::publish_block_action_resolution(
                                &server, gid, position, None, sequence, false,
                            );
                        }
                        if action.kind == pumpkin_cluster::canonical_action::ActionKind::World {
                            administration.publish_canonical_ground(
                                &server,
                                &chunk_blocks,
                                &action,
                                false,
                            );
                        }
                        if let Some((update, need)) = admin_update {
                            if let Some(need) = need {
                                pending_admin_need_releases.push_back(need);
                            }
                            queue_player_administration_projection(
                                &server,
                                &mut pending_player_admin,
                                ActionKey::canonical_id(id),
                                update,
                                false,
                            );
                        }
                    }
                }
            }
            if let Some(source) = canonical_scopes.get(&resolution_tick).and_then(|scope| scope.source(local).ok()) {
                let mut observations = actors.player_observations(&living).filter_map(|(gid, state, chunk, epoch)| {
                    (gid.server == source.server).then(|| inventory.player_lease_for_gid(gid).filter(|lease| {
                        inventory.player_install_generation(gid).is_some_and(|generation|
                            server.cluster_controller.active_for_simulation(*lease, generation))
                            && !blocked_proposal_actors.contains(&(pumpkin_cluster::actor::ActorId::Player(gid), lease.incarnation))
                            && !retained_internal_actors.contains(&(pumpkin_cluster::actor::ActorId::Player(gid), lease.incarnation))
                            && !frozen_proposal_work.keys().any(|head| head.actor == pumpkin_cluster::actor::ActorId::Player(gid) && head.incarnation == lease.incarnation)
                            && !callback_work.blocked_actor(pumpkin_cluster::actor::ActorId::Player(gid), lease.incarnation)
                            && !completed_callback_work.keys().any(|reference| reference.id.actor == pumpkin_cluster::actor::ActorId::Player(gid) && reference.id.incarnation == lease.incarnation)
                    }).map(|lease| (lease, state, chunk, epoch))).flatten()
                }).collect::<Vec<_>>();
                observations.sort_by_key(|(lease, _, _, _)| lease.gid);
                let vehicle_forward_count = pending_vehicle_controls.len();
                for _ in 0..vehicle_forward_count {
                    let Some(frame) = pending_vehicle_controls.pop_front() else { break };
                    let pumpkin_cluster::item_contact::ContactIdentity::Entity { id, birth } = &frame.input.mount else {
                        continue;
                    };
                    let body = chunk_blocks.local_entity_bodies().find_map(|(_, cell)| match &cell.value {
                        pumpkin_cluster::canonical_action::Value::Entity(body)
                            if body.id == *id && body.birth == *birth => Some(body),
                        _ => None,
                    });
                    let Some(body) = body else {
                        pending_vehicle_controls.push_back(frame);
                        continue;
                    };
                    if body.owner.server == local {
                        pending_vehicle_controls.push_back(frame);
                        continue;
                    }
                    if frame.source.server != local {
                        pending_vehicle_controls.push_back(frame);
                        continue;
                    }
                    let mut forwarded = frame.clone();
                    forwarded.recipient = body.owner;
                    let Some(bytes) = pumpkin_cluster::vehicle_wire::encode(&forwarded) else {
                        continue;
                    };
                    let parcel = OutboundParcel {
                        peer: body.owner.server,
                        header: StreamHeader::new(StreamKind::EntityWorld, None),
                        bytes,
                    };
                    if holistic_outbox.try_queue_control(parcel).is_err() {
                        pending_vehicle_controls.push_front(frame);
                        break;
                    }
                }
                if frozen_vehicle_control.is_none() {
                    if let Some(index) = pending_vehicle_controls.iter().position(|frame| {
                        let pumpkin_cluster::item_contact::ContactIdentity::Entity { id, birth } = &frame.input.mount else { return false; };
                        chunk_blocks.entity_body_chunk(false, *id)
                            .and_then(|chunk| chunk_blocks.entity_body(false, chunk, *id))
                            .is_some_and(|cell| matches!(&cell.value, pumpkin_cluster::canonical_action::Value::Entity(body)
                                if body.birth == *birth && body.owner == source))
                    }) {
                        if let Some(frame) = pending_vehicle_controls.remove(index) {
                            pending_vehicle_controls.push_front(frame);
                        }
                    }
                    if let Some(frame) = pending_vehicle_controls.front() {
                        if let pumpkin_cluster::item_contact::ContactIdentity::Entity { id, birth } = &frame.input.mount {
                            let body = chunk_blocks.entity_body_chunk(false, *id).and_then(|chunk|
                                chunk_blocks.entity_body(false, chunk, *id).and_then(|cell| match &cell.value {
                                    pumpkin_cluster::canonical_action::Value::Entity(body)
                                        if body.birth == *birth && body.owner == source => Some((chunk, item_spatial.project(body))),
                                    _ => None,
                                }));
                            if let Some((chunk, body)) = body.filter(|(_, body)|
                                pending_vehicle_boundaries.get(&body.id).is_none_or(|reference|
                                    body.spatial_epoch == pumpkin_cluster::canonical_action::SpatialEpoch::Action(*reference)
                                        && resolution_tick.is_after(reference.tick))) {
                                if pending_vehicle_boundaries.remove(&body.id).is_some() {
                                    item_spatial.retire(body.id);
                                }
                                let actor = pumpkin_cluster::actor::ActorId::Entity(body.id);
                                if !retained_internal_actors.contains(&(actor, u64::from(body.id.generation)))
                                    && !frozen_proposal_work.keys().any(|head| head.actor == actor)
                                    && !callback_work.blocked_actor(actor, u64::from(body.id.generation))
                                    && !completed_callback_work.keys().any(|reference| reference.id.actor == actor)
                                    && let Some(driver) = actors.phase_spatial(frame.input.driver.gid, false)
                                    && let Some(snapshot) = source_actor_snapshot.clone()
                                {
                                    let draft = actor_drafts.get(&(actor, u64::from(body.id.generation), resolution_tick)).cloned();
                                    match super::cluster_vehicle_control::FrozenVehicleControl::capture(snapshot,
                                        frame.input.clone(), body, chunk, driver.chunk, resolution_tick, draft) {
                                        Ok(work) => {
                                            retained_internal_actors.hold(work.actor(), work.tick,
                                                pumpkin_cluster::actor_source_retention::ActorSourceFamily::Vehicle);
                                            frozen_vehicle_control = Some(work); pending_vehicle_controls.pop_front();
                                        }
                                        Err(pumpkin_cluster::canonical_action::ApplyFailure::Capacity) => {}
                                        Err(_) => { pending_vehicle_controls.pop_front(); }
                                    }
                                }
                            }
                        }
                    }
                }
                if let Some(work) = frozen_vehicle_control.as_mut() {
                    let _ = work.prepare(&server, &mut vehicle_chunk_pins, &chunk_blocks, &canonical_graphs);
                    if work.ready() {
                    if pending_vehicle_page.is_none() {
                        let (actor, incarnation) = work.actor();
                        let count = work.action_count();
                        if let Some(reservation) = action_issue_clock.reserve(actor, incarnation, work.tick, count) {
                            match work.materialize(reservation.id(0, 14).unwrap(),
                                (count >= 2).then(|| reservation.id(1, 15).unwrap()),
                                work.has_hopper().then(|| reservation.id(2, 16).unwrap()),
                                work.transitions().then(|| reservation.id(2 + u32::from(work.has_hopper()), 8).unwrap()), entity_origin_allocator.as_mut()) {
                                Ok(page) => pending_vehicle_page = Some(page),
                                Err(reason) => {
                                    action_issue_clock.release_unused(reservation);
                                    if vehicle_source_diagnostics.len() < 1024
                                        && vehicle_source_diagnostics.insert((work.body.id, work.body.birth.action)) {
                                        tracing::error!(entity = ?work.body.id, tick = work.tick.0, ?reason,
                                            "cluster frozen vehicle source invariant failed");
                                    }
                                }
                            }
                        }
                    }
                    if let Some((actions, draft, effective)) = pending_vehicle_page.take() {
                        let mut batch = TickBatch::new(work.tick);
                        batch.canonical_actions = actions.clone();
                        let reserved = reserve_actor_source_page(&mut holistic_outbox, canonical_scopes.get(&work.tick),
                            batch, &local_finishes, canonical_queued.get(&work.tick).map_or(&[], Vec::as_slice),
                            &canonical_reserved_batches, local_realm_issued.get(&work.tick), &realm_completion,
                            |action| chunk_blocks.historical_entity_authority(action)
                                .or_else(|| entity_authority_at_action_tick(&canonical_graphs, action)));
                        if let Ok(page) = reserved {
                            vehicle_controllers.accept(&work.body, &effective);
                            retained_internal_actors.release(work.actor(), work.tick,
                                pumpkin_cluster::actor_source_retention::ActorSourceFamily::Vehicle);
                            let (actor, incarnation) = work.actor();
                            actor_drafts.insert((actor, incarnation, work.tick), draft);
                            if work.publishes_sample() && let Some(scope) = canonical_scopes.get(&work.tick) {
                                let sample = work.sample(&effective, &item_spatial);
                                publish_owned_entity_spatial(&server, &chunk_blocks, &mut item_spatial, scope, [(work.chunk, sample)]);
                            }
                            for action in actions {
                                if action.chunk_transition { pending_vehicle_boundaries.insert(work.body.id,
                                    pumpkin_cluster::canonical_action::ActionRef { id: action.id, tick: action.tick }); }
                                canonical_pending.insert((action.tick, action.id), PendingCanonical { action });
                            }
                            canonical_reserved_batches.push_back(page);
                            frozen_vehicle_control = None;
                        } else { pending_vehicle_page = Some((actions, draft, effective)); }
                    }
                }
                    }
                if vehicle_idle_frame.is_none() && last_vehicle_idle_tick.is_none_or(|tick| resolution_tick.is_after(tick)) {
                    let present = chunk_blocks.local_entity_bodies().filter_map(|(_, cell)| match &cell.value {
                        pumpkin_cluster::canonical_action::Value::Entity(body)
                            if body.owner == source && matches!(body.payload, pumpkin_cluster::entity_body::EntityPayload::Vehicle(_)) =>
                                Some((body.id, (&body.birth, body.owner))),
                        _ => None,
                    }).collect::<BTreeMap<_, _>>();
                    vehicle_controllers.retain(|id, birth, owner| present.get(&id)
                        .is_some_and(|(current_birth, current_owner)| *current_birth == birth && *current_owner == owner));
                    vehicle_idle_pins.retain(|id, (birth, _)| present.get(id)
                        .is_some_and(|(current_birth, _)| *current_birth == birth));
                    drop(present);
                    let owned = super::cluster_world_canonical::OwnedWorldActionState {
                        server: &server, blocks: &mut chunk_blocks, actors: &mut actors,
                        living: &mut living, inventory: &mut inventory, versions: &mut canonical_versions,
                    };
                    if let Ok(snapshot) = source_actor_snapshot.clone().ok_or(pumpkin_cluster::canonical_action::ApplyFailure::Capacity) {
                        let snapshot = snapshot.with_scheduled_executors(&canonical_scopes[&resolution_tick].scheduled_executors());
                        let controlled_vehicles = pending_vehicle_controls.iter().filter_map(|control| match &control.input.mount {
                            pumpkin_cluster::item_contact::ContactIdentity::Entity { id, .. } => Some(*id), _ => None,
                        }).collect::<BTreeSet<_>>();
                        let mut vehicle_inputs = Vec::new();
                        for (chunk, cell) in owned.blocks.local_entity_bodies() {
                            let pumpkin_cluster::canonical_action::Value::Entity(body) = &cell.value else { continue; };
                            let actor = pumpkin_cluster::actor::ActorId::Entity(body.id);
                            if let Some(reference) = pending_vehicle_boundaries.get(&body.id).copied() {
                                if body.spatial_epoch != pumpkin_cluster::canonical_action::SpatialEpoch::Action(reference)
                                    || !resolution_tick.is_after(reference.tick) { continue; }
                                pending_vehicle_boundaries.remove(&body.id);
                                item_spatial.retire(body.id);
                            }
                            let key = (actor, u64::from(body.id.generation));
                            if body.owner != source || !matches!(body.payload, pumpkin_cluster::entity_body::EntityPayload::Vehicle(_))
                                || retained_internal_actors.contains(&key)
                                || frozen_proposal_work.keys().any(|head| head.actor == actor)
                                || callback_work.blocked_actor(key.0, key.1)
                                || controlled_vehicles.contains(&body.id) {
                                continue;
                            }
                            let draft = actor_drafts.get(&(key.0, key.1, resolution_tick)).cloned();
                            let projected = item_spatial.project(body);
                            let Ok(needed) = super::cluster_vehicle_control::required_chunks(&projected) else { continue; };
                            if vehicle_idle_pins.get(&body.id).is_none_or(|(birth, pin)| *birth != body.birth || pin.chunks() != needed) {
                                let Ok(pin) = vehicle_chunk_pins.acquire(&server, &needed) else { continue; };
                                vehicle_idle_pins.insert(body.id, (body.birth.clone(), pin));
                            }
                            let Some((_, pin)) = vehicle_idle_pins.get(&body.id) else { continue; };
                            if !pin.available(owned.blocks) { continue; }
                            vehicle_inputs.push((projected, chunk, draft, pin.clone()));
                        }
                        use rayon::prelude::*;
                        let mut jobs = vehicle_inputs.into_par_iter().filter_map(|(body, chunk, draft, pin)|
                            super::cluster_vehicle_control::FrozenVehicleControl::idle(snapshot.clone(),
                                body, chunk, resolution_tick, draft, pin).ok().flatten()).collect::<Vec<_>>();
                        jobs.sort_by_key(|job| job.body.id);
                        for job in &jobs {
                            retained_internal_actors.hold(job.actor(), resolution_tick,
                                pumpkin_cluster::actor_source_retention::ActorSourceFamily::Vehicle);
                        }
                        vehicle_idle_frame = pumpkin_cluster::actor_tick_frame::FrozenActorTickFrame::new(source, resolution_tick, jobs);
                    }
                }
                if let Some(frame) = vehicle_idle_frame.as_mut() {
                    for job in frame.remaining_mut() {
                        let _ = job.prepare_facts(&server, &mut vehicle_chunk_pins, &chunk_blocks, &canonical_graphs);
                    }
                    {
                        use rayon::prelude::*;
                        frame.remaining_mut().par_iter_mut().for_each(|job| { let _ = job.finish_prepare(); });
                    }
                    if vehicle_idle_page.is_none() {
                        let capacity = 4096usize.saturating_sub(canonical_pending.len().saturating_add(pending_action_inputs.len()));
                        match super::cluster_vehicle_control::emit_frame(frame, &mut action_issue_clock, capacity, entity_origin_allocator.as_mut()) {
                            Ok(actions) if !actions.is_empty() || frame.complete() => vehicle_idle_page = Some(actions),
                            Err(reason) => {
                                if let Some(job) = frame.remaining().first()
                                    && vehicle_source_diagnostics.len() < 1024
                                    && vehicle_source_diagnostics.insert((job.body.id, job.body.birth.action)) {
                                    tracing::error!(entity = ?job.body.id, tick = frame.tick.0, ?reason,
                                        "cluster frozen vehicle source invariant failed");
                                }
                            }
                            _ => {}
                        }
                    }
                    if let Some(actions) = vehicle_idle_page.take() {
                        let mut batch = TickBatch::new(frame.tick);
                        batch.canonical_actions = actions.iter().flat_map(|(actions, _, _)| actions.iter().cloned()).collect();
                        let reserved = reserve_actor_source_page(&mut holistic_outbox, canonical_scopes.get(&frame.tick),
                            batch, &local_finishes, canonical_queued.get(&frame.tick).map_or(&[], Vec::as_slice),
                            &canonical_reserved_batches, local_realm_issued.get(&frame.tick), &realm_completion,
                            |action| chunk_blocks.historical_entity_authority(action)
                                .or_else(|| entity_authority_at_action_tick(&canonical_graphs, action)));
                        if let Ok(page) = reserved {
                            let count = actions.len();
                            let mut samples = Vec::new();
                            for (job, (actions, draft, effective)) in frame.remaining().iter().zip(actions) {
                                vehicle_controllers.accept(&job.body, &effective);
                                let (actor, incarnation) = job.actor();
                                retained_internal_actors.release((actor, incarnation), job.tick,
                                    pumpkin_cluster::actor_source_retention::ActorSourceFamily::Vehicle);
                                actor_drafts.insert((actor, incarnation, job.tick), draft);
                                if job.publishes_sample() { samples.push((job.chunk, job.sample(&effective, &item_spatial))); }
                                for action in actions {
                                    if action.chunk_transition { pending_vehicle_boundaries.insert(job.body.id,
                                        pumpkin_cluster::canonical_action::ActionRef { id: action.id, tick: action.tick }); }
                                    canonical_pending.insert((action.tick, action.id), PendingCanonical { action });
                                }
                            }
                            if let Some(scope) = canonical_scopes.get(&frame.tick) {
                                publish_owned_entity_spatial(&server, &chunk_blocks, &mut item_spatial, scope, samples);
                            }
                            canonical_reserved_batches.push_back(page);
                            assert!(frame.commit_emitted(count));
                            if frame.complete() { last_vehicle_idle_tick = Some(frame.tick); vehicle_idle_frame = None; }
                        } else { vehicle_idle_page = Some(actions); }
                    }
                }
                let mut bodies = chunk_blocks.local_entity_bodies().filter(|(chunk, _)| {
                    !buckets.is_quarantined(*chunk) && !pending_fresh.contains_key(chunk) && !pending_imports.contains_key(chunk)
                }).filter_map(|(chunk, cell)| match &cell.value {
                    pumpkin_cluster::canonical_action::Value::Entity(body) => Some((chunk, body.as_ref().clone())),
                    _ => None,
                }).collect::<Vec<_>>();
                bodies.sort_by_key(|(_, body)| body.id);
                        bodies.retain(|(_, body)| {
                    let actor = pumpkin_cluster::actor::ActorId::Entity(body.id);
                    !blocked_proposal_actors.contains(&(actor, u64::from(body.id.generation)))
                        && !retained_internal_actors.contains(&(actor, u64::from(body.id.generation)))
                        && !frozen_proposal_work.keys().any(|head| head.actor == actor && head.incarnation == u64::from(body.id.generation))
                        && !callback_work.blocked_actor(actor, u64::from(body.id.generation))
                        && !completed_callback_work.keys().any(|reference| reference.id.actor == actor && reference.id.incarnation == u64::from(body.id.generation))
                });
                let capacity = 4096usize.saturating_sub(canonical_pending.len().saturating_add(pending_action_inputs.len()));
                let scope = &canonical_scopes[&resolution_tick];
                let owned = super::cluster_world_canonical::OwnedWorldActionState {
                    server: &server, blocks: &mut chunk_blocks, actors: &mut actors, living: &mut living,
                    inventory: &mut inventory, versions: &mut canonical_versions,
                };
                if projectile_tick_frame.is_none()
                    && last_projectile_tick.is_none_or(|tick| resolution_tick.is_after(tick)) {
                    let original_tick = *pending_projectile_tick.get_or_insert(resolution_tick);
                    let captured = canonical_scopes.get(&original_tick).ok_or(pumpkin_cluster::canonical_action::ApplyFailure::Capacity)
                        .and_then(|original_scope| {
                            let source = original_scope.source(local).map_err(|_| pumpkin_cluster::canonical_action::ApplyFailure::InvalidAction)?;
                            let snapshot = original_scope.full_source_slot.and_then(|slot| source_actor_snapshots.get(&slot))
                                .map(|(snapshot, _, _)| snapshot.clone()).ok_or(pumpkin_cluster::canonical_action::ApplyFailure::Capacity)?;
                            let slot = original_scope.full_source_slot.ok_or(pumpkin_cluster::canonical_action::ApplyFailure::Capacity)?;
                            for chunk in &pending_projectile_cut_keys {
                                let key = (slot,*chunk);
                                if !historical_actor_cuts.contains_key(&key) {
                                    if !historical_actor_need_pins.contains_key(&key) && !local_finishes.closed_through(original_tick)
                                        && let Ok(providers) = snapshot.original_frozen_holders(*chunk)
                                        && let Some(next) = next_historical_actor_lease.checked_add(1)
                                        && let Some(pin) = super::cluster::try_require_original_actor_chunk(&server,*chunk,slot,next_historical_actor_lease,providers)
                                    {
                                        next_historical_actor_lease = next;
                                        historical_actor_need_pins.insert(key,pin);
                                    }
                                    return Err(pumpkin_cluster::canonical_action::ApplyFailure::Condition(
                                        pumpkin_cluster::canonical_action::Target::Chunk {key:*chunk,part:pumpkin_cluster::canonical_action::ChunkPart::Whole,inventory:None}));
                                }
                            }
                            let cuts = pending_projectile_cut_keys.iter().filter_map(|chunk|
                                historical_actor_cuts.get(&(slot,*chunk)).map(|(cut,_)|cut.clone())).collect::<Vec<_>>();
                            super::cluster_projectile_source::capture_frame_with_cuts(snapshot, source, original_tick, &actor_drafts, &cuts)
                        });
                    match captured {
                        Ok(frame) => {
                            for job in frame.remaining() {
                                retained_internal_actors.hold((job.actor(), job.incarnation()), frame.tick,
                                    pumpkin_cluster::actor_source_retention::ActorSourceFamily::Projectile);
                            }
                            projectile_tick_frame = Some(frame);
                        }
                        Err(reason) => {
                            if let pumpkin_cluster::canonical_action::ApplyFailure::Condition(
                                pumpkin_cluster::canonical_action::Target::Chunk {key,..}) = &reason {
                                pending_projectile_cut_keys.insert(*key);
                            }
                            let key = reason.clone();
                            if projectile_tick_failure != Some(key.clone()) {
                                tracing::error!(tick = ?original_tick, ?reason, "cluster projectile capture invariant failed");
                                projectile_tick_failure = Some(key);
                            }
                        }
                    }
                }
                if let Some(frame) = projectile_tick_frame.as_mut() {
                    if pending_projectile_page.is_none() {
                        let emission = entity_origin_allocator.as_mut().ok_or(pumpkin_cluster::canonical_action::ApplyFailure::Capacity)
                            .and_then(|entities| super::cluster_projectile_source::emit_frame(frame, &mut action_issue_clock, entities, capacity));
                        match emission {
                            Ok(page) if page.completed_jobs != 0 || frame.complete() => pending_projectile_page = Some(page),
                            Ok(_) => {},
                            Err(reason) => {
                                let key = reason.clone();
                                if projectile_tick_failure != Some(key.clone()) {
                                    tracing::error!(tick = ?frame.tick, ?reason, "cluster projectile emission invariant failed");
                                    projectile_tick_failure = Some(key);
                                }
                            }
                        }
                    }
                    if let Some(page) = pending_projectile_page.take() {
                        let mut batch = TickBatch::new(frame.tick);
                        batch.canonical_actions = page.actions.iter().map(|(action, _)| action.clone()).collect();
                        let reserved = reserve_actor_source_page(&mut holistic_outbox, canonical_scopes.get(&frame.tick), batch,
                            &local_finishes, canonical_queued.get(&frame.tick).map_or(&[], Vec::as_slice), &canonical_reserved_batches,
                            local_realm_issued.get(&frame.tick), &realm_completion, |action|
                                owned.blocks.historical_entity_authority(action).or_else(|| entity_authority_at_action_tick(&canonical_graphs, action)));
                        if let Ok(reserved) = reserved {
                            if page.actions.iter().any(|(action, _)| object_terminal_reference
                                == Some(pumpkin_cluster::canonical_action::ActionRef { id: action.id, tick: action.tick })) {
                                object_terminal_reference = None;
                                object_terminal_inflight = false;
                            }
                            for (action, draft) in page.actions {
                                retained_internal_actors.release((action.id.actor, action.id.incarnation), action.tick,
                                    pumpkin_cluster::actor_source_retention::ActorSourceFamily::Projectile);
                                actor_drafts.insert((action.id.actor, action.id.incarnation, action.tick), draft);
                                canonical_pending.insert((action.tick, action.id), PendingCanonical { action });
                            }
                            if let Some(scope) = canonical_scopes.get(&frame.tick) {
                                publish_owned_entity_spatial(&server, owned.blocks, &mut item_spatial, scope, page.samples);
                            }
                            canonical_reserved_batches.push_back(reserved);
                            assert!(frame.commit_emitted(page.completed_jobs));
                            if frame.complete() {
                                retained_internal_actors.release_tick(frame.tick, pumpkin_cluster::actor_source_retention::ActorSourceFamily::Projectile);
                                last_projectile_tick = Some(frame.tick);
                                pending_projectile_tick = None;
                                pending_projectile_cut_keys.clear();
                                projectile_tick_frame = None;
                                projectile_tick_failure = None;
                            }
                        } else { pending_projectile_page = Some(page); }
                    }
                }
                 if object_tick_frame.is_none() && pending_object_tick.is_none()
                    && last_object_tick.is_none_or(|tick| resolution_tick.is_after(tick)) {
                    pending_object_tick = Some(resolution_tick);
                    let mut bounds = BTreeMap::new();
                    let object_observations = bodies.iter().filter_map(|(chunk, body)| {
                        if body.owner != source || !(matches!(&body.payload, pumpkin_cluster::entity_body::EntityPayload::Object(object)
                            if matches!(object.kind, pumpkin_cluster::object_state::ObjectKind::PrimedTnt(_) | pumpkin_cluster::object_state::ObjectKind::FallingBlock(_)))
                            || super::cluster_vehicle_control::due_tnt(body, resolution_tick)) { return None; }
                        let blocks = owned.blocks.snapshot(*chunk)?;
                        bounds.insert(chunk.world, (blocks.min_y(), blocks.min_y() + blocks.block_palettes().len() as i32 * 16));
                        let spatial = item_spatial.get(body.id).filter(|sample| sample.matches_body(body))
                            .map_or_else(|| body.spatial.clone(), |sample| sample.spatial.clone());
                        Some(super::cluster_object_tick::ObjectPhaseObservation { chunk: *chunk, body: body.clone(), effective_spatial: spatial })
                    }).collect::<Vec<_>>();
                    for observation in &object_observations {
                        if matches!(&observation.body.payload, pumpkin_cluster::entity_body::EntityPayload::Object(object)
                            if matches!(object.kind, pumpkin_cluster::object_state::ObjectKind::PrimedTnt(_) | pumpkin_cluster::object_state::ObjectKind::FallingBlock(_)))
                            || super::cluster_vehicle_control::due_tnt(&observation.body, resolution_tick) {
                            retained_internal_actors.hold((pumpkin_cluster::actor::ActorId::Entity(observation.body.id), u64::from(observation.body.id.generation)), resolution_tick,
                                pumpkin_cluster::actor_source_retention::ActorSourceFamily::Object);
                        }
                    }
                    if object_observations.is_empty() {
                        pending_object_tick = None;
                        last_object_tick = Some(resolution_tick);
                    } else {
                    let captured = source_actor_snapshot.clone().ok_or(pumpkin_cluster::canonical_action::ApplyFailure::Capacity)
                        .map(|snapshot| snapshot.with_scheduled_executors(&scope.scheduled_executors()))
                        .and_then(|snapshot| super::cluster_object_tick::capture_frame(snapshot, object_observations, source,
                            resolution_tick, &bounds, &actor_drafts, &item_spatial));
                    match captured {
                        Ok(frame) => object_tick_frame = Some(frame),
                        Err(pumpkin_cluster::canonical_action::ApplyFailure::MissingStakeholder(_)) => {
                            retained_internal_actors.release_tick(resolution_tick, pumpkin_cluster::actor_source_retention::ActorSourceFamily::Object);
                            pending_object_tick = None;
                        }
                        Err(reason) => tracing::error!(tick = ?resolution_tick, ?reason, "cluster object source frame invariant failed"),
                    }
                    }
                }
                if let Some(frame) = object_tick_frame.as_mut() {
                    if pending_object_page.is_none()
                        && let Some(job) = frame.remaining().first().filter(|job| job.plan.terminal.is_some())
                    {
                        if object_terminal_reference.is_none() {
                            if let Some(reservation) = action_issue_clock.reserve(job.actor(), job.incarnation(), frame.tick, 1) {
                                object_terminal_reference = reservation.id(0, 15).map(|id|
                                    pumpkin_cluster::canonical_action::ActionRef { id, tick: frame.tick });
                            }
                        }
                        if let Some(reference) = object_terminal_reference {
                            if !object_terminal_inflight && completed_object_terminal.is_none() {
                                if let Ok((context, draft, loot, fixed)) = super::cluster_object_tick::prepare_terminal(job, reference, &server) {
                                    if callback_work.stage_object_terminal(context, fixed, draft, loot).is_ok() {
                                        object_terminal_inflight = true;
                                    }
                                }
                            }
                            if let Some(mut work) = completed_object_terminal.take() {
                                match work.issue() {
                                    Ok(()) => pending_object_page = Some(super::cluster_object_tick::ObjectEmissionPage {
                                        completed_jobs: 1, actions: vec![(work.action.clone(), work.draft().clone())], samples: Vec::new(),
                                    }),
                                    Err(_) => completed_object_terminal = Some(work),
                                }
                            }
                        }
                    }
                    if pending_object_page.is_none() {
                        match super::cluster_object_tick::emit_continuations(frame, &mut action_issue_clock, capacity) {
                            Ok(page) if page.completed_jobs != 0 || frame.complete() => pending_object_page = Some(page),
                            Ok(_) => {},
                            Err(reason) => {
                                if let Some(job) = frame.remaining().first() {
                                    let key = (frame.tick, job.actor());
                                    if object_tick_emission_failure != Some(key) {
                                        tracing::error!(tick = ?frame.tick, actor = ?key.1, ?reason, "cluster object source emission invariant failed");
                                        object_tick_emission_failure = Some(key);
                                    }
                                }
                            }
                        }
                    }
                    if let Some(page) = pending_object_page.take() {
                        let mut batch = TickBatch::new(frame.tick);
                        batch.canonical_actions = page.actions.iter().map(|(action, _)| action.clone()).collect();
                        let reserved = reserve_actor_source_page(&mut holistic_outbox, canonical_scopes.get(&frame.tick), batch,
                            &local_finishes, canonical_queued.get(&frame.tick).map_or(&[], Vec::as_slice), &canonical_reserved_batches,
                            local_realm_issued.get(&frame.tick), &realm_completion, |action|
                                owned.blocks.historical_entity_authority(action).or_else(|| entity_authority_at_action_tick(&canonical_graphs, action)));
                        if let Ok(reserved) = reserved {
                            for (action, draft) in page.actions {
                                retained_internal_actors.release((action.id.actor, action.id.incarnation), action.tick,
                                    pumpkin_cluster::actor_source_retention::ActorSourceFamily::Object);
                                actor_drafts.insert((action.id.actor, action.id.incarnation, action.tick), draft);
                                canonical_pending.insert((action.tick, action.id), PendingCanonical { action });
                            }
                            if let Some(scope) = canonical_scopes.get(&frame.tick) {
                                publish_owned_entity_spatial(&server, owned.blocks, &mut item_spatial, scope, page.samples);
                            }
                            canonical_reserved_batches.push_back(reserved);
                            assert!(frame.commit_emitted(page.completed_jobs));
                            if frame.complete() {
                                retained_internal_actors.release_tick(frame.tick, pumpkin_cluster::actor_source_retention::ActorSourceFamily::Object);
                                last_object_tick = Some(frame.tick);
                                pending_object_tick = None;
                                object_tick_frame = None;
                                object_tick_emission_failure = None;
                            }
                        } else { pending_object_page = Some(page); }
                    }
                }
                if living_tick_frame.is_none() && pending_living_capture.is_none() && pending_living_tick.is_none()
                    && last_living_tick.is_none_or(|tick| resolution_tick.is_after(tick)) {
                    pending_living_tick = Some(resolution_tick);
                    let captured = source_actor_snapshot.clone().ok_or(pumpkin_cluster::canonical_action::ApplyFailure::Capacity)
                        .and_then(|snapshot| {
                            let contacts = super::cluster_living_tick::capture_contacts(snapshot.as_ref(), &observations, &bodies, source, resolution_tick)?;
                            let contacts = super::cluster_living_tick::prepare_contacts(&server, owned.blocks, contacts, &mut living_chunk_pins, &mut living_simulation_pins);
                            super::cluster_living_tick::FrozenLivingCaptureInputs::new(snapshot,&bodies,contacts,source,resolution_tick,
                                &actor_drafts,&living_simulation_pins,&scope.scheduled_executors())
                        });
                    match captured {
                        Ok(inputs) => {
                            for actor in inputs.actors() {
                                retained_internal_actors.hold(actor,inputs.tick,pumpkin_cluster::actor_source_retention::ActorSourceFamily::Living);
                            }
                            pending_living_capture=Some(inputs);
                        }
                        Err(reason) => {
                            pending_living_tick=None;
                            last_living_tick=Some(resolution_tick);
                            let now=std::time::Instant::now();
                            let key=std::mem::discriminant(&reason);
                            if living_capture_diagnostics.get(&key).is_none_or(|last|now.duration_since(*last)>=std::time::Duration::from_secs(300)) {
                                tracing::error!(tick=?resolution_tick,?reason,"cluster living source capture inputs rejected");
                                living_capture_diagnostics.insert(key,now);
                            }
                        }
                    }
                }
                if living_tick_frame.is_none() && let Some(inputs)=pending_living_capture.as_ref() {
                    let original_tick=inputs.tick;
                    match inputs.capture(&owned) {
                        Ok(frame) => {
                            let present=frame.remaining().first().into_iter().flat_map(|job|job.original_world().local_entity_bodies())
                                .filter_map(|(_,cell)|match &cell.value {pumpkin_cluster::canonical_action::Value::Entity(body)=>
                                    Some((pumpkin_cluster::actor::ActorId::Entity(body.id),body.id.generation)),_=>None}).collect::<std::collections::HashSet<_>>();
                            living_hazard_diagnostics.retain(|actor,_|present.contains(actor));
                            let now=std::time::Instant::now();
                            for job in frame.remaining() {
                                let key=(job.actor(),job.incarnation());
                                retained_internal_actors.hold(key,original_tick,pumpkin_cluster::actor_source_retention::ActorSourceFamily::Living);
                                if job.hazard_resource_limit() && living_hazard_diagnostics.get(&key)
                                    .is_none_or(|last|now.duration_since(*last)>=std::time::Duration::from_secs(300)) {
                                    tracing::error!(actor=?key.0,incarnation=key.1,tick=?original_tick,
                                        reason=?pumpkin_cluster::canonical_action::ApplyFailure::ResourceLimit,
                                        "cluster living hazard geometry exceeds contact representation");
                                    living_hazard_diagnostics.insert(key,now);
                                }
                            }
                            living_tick_frame=Some(frame);
                            pending_living_capture=None;
                        }
                        Err(reason) => {
                            let recoverable=matches!(reason,pumpkin_cluster::canonical_action::ApplyFailure::Capacity
                                | pumpkin_cluster::canonical_action::ApplyFailure::MissingStakeholder(_));
                            if !recoverable {
                                retained_internal_actors.release_tick(original_tick,pumpkin_cluster::actor_source_retention::ActorSourceFamily::Living);
                                pending_living_tick=None;
                                last_living_tick=Some(original_tick);
                                pending_living_capture=None;
                            }
                            let now=std::time::Instant::now();
                            let key=std::mem::discriminant(&reason);
                            if living_capture_diagnostics.get(&key).is_none_or(|last|now.duration_since(*last)>=std::time::Duration::from_secs(300)) {
                                tracing::error!(tick=?original_tick,?reason,retained=recoverable,"cluster living source capture unavailable");
                                living_capture_diagnostics.insert(key,now);
                            }
                        }
                    }
                }
                if let Some(frame) = living_tick_frame.as_mut() {
                    if pending_living_page.is_none() {
                        match super::cluster_living_tick::emit_frame(frame, &server, &canonical_graphs, &mut living_chunk_pins, &mut action_issue_clock, capacity, entity_origin_allocator.as_mut()) {
                            Ok(page) if page.completed_jobs != 0 || frame.complete() => pending_living_page = Some(page),
                            Ok(_) => {},
                            Err(reason) => {
                                if let Some(job) = frame.remaining().first() {
                                    let key = (frame.tick, job.actor());
                                    if living_tick_emission_failure != Some(key) {
                                        tracing::error!(tick = ?frame.tick, actor = ?key.1, ?reason, "cluster living source emission invariant failed");
                                        living_tick_emission_failure = Some(key);
                                    }
                                }
                            }
                        }
                    }
                    if let Some(emission) = pending_living_page.take() {
                        let mut batch = TickBatch::new(frame.tick);
                        batch.canonical_actions = emission.actions.iter().map(|(action, _)| action.clone()).collect();
                        let reserved = reserve_actor_source_page(&mut holistic_outbox, canonical_scopes.get(&frame.tick),
                            batch, &local_finishes, canonical_queued.get(&frame.tick).map_or(&[], Vec::as_slice),
                            &canonical_reserved_batches, local_realm_issued.get(&frame.tick), &realm_completion,
                            |action| owned.blocks.historical_entity_authority(action)
                                .or_else(|| entity_authority_at_action_tick(&canonical_graphs, action)));
                        let reserved = reserved.map_err(|_| ()).and_then(|page| {
                            for (reference, _) in &emission.closes {
                                let Some((action, _)) = emission.actions.iter().find(|(action, _)|
                                    action.id == reference.id && action.tick == reference.tick) else {
                                    holistic_outbox.release_canonical_reservation(&page.parcels);
                                    return Err(());
                                };
                                if owned.inventory.track_canonical_close(action).is_err()
                                    || !owned.inventory.can_attach_canonical_close_notification(*reference) {
                                    holistic_outbox.release_canonical_reservation(&page.parcels);
                                    return Err(());
                                }
                            }
                            Ok(page)
                        });
                        if let Ok(page) = reserved {
                            for (reference, close) in &emission.closes {
                                if let Some(notification) = close.take_notification(*reference) {
                                    owned.inventory.attach_canonical_close_notification(*reference, notification).unwrap();
                                }
                            }
                            let count = emission.completed_jobs;
                            let issued_references = emission.actions.iter().map(|(action, _)|
                                pumpkin_cluster::canonical_action::ActionRef { id: action.id, tick: action.tick }).collect::<Vec<_>>();
                            for (action, draft) in emission.actions {
                                retained_internal_actors.release((action.id.actor, action.id.incarnation), action.tick,
                                    pumpkin_cluster::actor_source_retention::ActorSourceFamily::Living);
                                actor_drafts.insert((action.id.actor, action.id.incarnation, action.tick), draft);
                                canonical_pending.insert((action.tick, action.id), PendingCanonical { action });
                            }
                            if let Some(scope) = canonical_scopes.get(&frame.tick) {
                                publish_owned_entity_spatial(&server, owned.blocks, &mut item_spatial, scope, emission.samples);
                            }
                            canonical_reserved_batches.push_back(page);
                            assert!(frame.commit_emitted(count));
                            if let Some(allocator) = entity_origin_allocator.as_mut() {
                                for reference in issued_references { allocator.release_issued(reference); }
                            }
                            if frame.complete() {
                                retained_internal_actors.release_tick(frame.tick,
                                    pumpkin_cluster::actor_source_retention::ActorSourceFamily::Living);
                                last_living_tick = Some(frame.tick);
                                pending_living_tick = None;
                                living_tick_frame = None;
                                living_tick_emission_failure = None;
                            }
                        } else {
                            pending_living_page = Some(emission);
                        }
                    }
                }
                let capacity = 4096usize.saturating_sub(canonical_pending.len().saturating_add(pending_action_inputs.len()));
                if let Some(original) = source_actor_snapshot.as_deref() {
                for action in super::cluster_item_use::complete(original, &observations, resolution_tick,
                    &mut action_issue_clock, &mut actor_drafts, capacity) {
                    canonical_pending.insert((action.tick, action.id), PendingCanonical { action: action.clone() });
                    canonical_queued.entry(action.tick).or_default().push(action);
                }
                }
                let capacity = 4096usize.saturating_sub(canonical_pending.len().saturating_add(pending_action_inputs.len()));
                let tick = pending_item_contact_tick.unwrap_or(resolution_tick);
                if item_contact_frame.is_none() && last_item_contact_tick.is_none_or(|last| tick.is_after(last)) {
                    pending_item_contact_tick = Some(tick);
                    let captured = canonical_scopes.get(&tick).and_then(|scope| scope.full_source_slot)
                        .and_then(|slot| source_actor_snapshots.get(&slot)).map(|(snapshot, _, _)| snapshot.clone())
                        .ok_or(pumpkin_cluster::canonical_action::ApplyFailure::Capacity)
                        .and_then(|snapshot| {
                            let original_source = canonical_scopes.get(&tick).and_then(|scope| scope.source(local).ok())
                                .ok_or(pumpkin_cluster::canonical_action::ApplyFailure::Capacity)?;
                            super::cluster_item_contacts::ItemContactFrame::new(snapshot,
                                original_source, tick, server.is_cluster_primary())
                        });
                    match captured {
                        Ok(frame) => {
                            for actor in frame.actors() {
                                retained_internal_actors.hold(actor, tick, pumpkin_cluster::actor_source_retention::ActorSourceFamily::Item);
                            }
                            item_contact_frame = Some(frame);
                        }
                        Err(reason) => {
                            if item_contact_failure != Some(reason.clone()) {
                                tracing::error!(?tick, ?reason, "cluster original item contact capture unavailable");
                                item_contact_failure = Some(reason);
                            }
                        }
                    }
                }
                if let Some(frame) = item_contact_frame.as_mut() {
                    if item_contact_page.is_none() {
                        let actions = frame.emit(&mut action_issue_clock, &mut actor_drafts, capacity);
                        if !actions.is_empty() || frame.complete() { item_contact_page = Some(actions); }
                    }
                    if let Some(actions) = item_contact_page.take() {
                        let mut batch = TickBatch::new(frame.tick);
                        batch.canonical_actions = actions.clone();
                        match reserve_actor_source_page(&mut holistic_outbox, canonical_scopes.get(&frame.tick), batch,
                            &local_finishes, canonical_queued.get(&frame.tick).map_or(&[], Vec::as_slice), &canonical_reserved_batches,
                            local_realm_issued.get(&frame.tick), &realm_completion, |action|
                                owned.blocks.historical_entity_authority(action).or_else(|| entity_authority_at_action_tick(&canonical_graphs, action))) {
                            Ok(page) => {
                                for action in actions {
                                    canonical_pending.insert((action.tick, action.id), PendingCanonical { action });
                                }
                                canonical_reserved_batches.push_back(page);
                                if frame.complete() {
                                    retained_internal_actors.release_tick(frame.tick, pumpkin_cluster::actor_source_retention::ActorSourceFamily::Item);
                                    last_item_contact_tick = Some(frame.tick);
                                    pending_item_contact_tick = None;
                                    item_contact_frame = None;
                                    item_contact_failure = None;
                                }
                            }
                            Err(_) => item_contact_page = Some(actions),
                        }
                    }
                }
            }
            for _ in 0..canonical_reserved_batches.len() {
                let page = canonical_reserved_batches.pop_front().unwrap();
                if !buckets.can_admit_canonical(page.batch.canonical_actions.len()) {
                    canonical_reserved_batches.push_back(page);
                    continue;
                }
                let tick = page.batch.tick;
                let Some(scope) = canonical_scopes.get(&tick) else {
                    canonical_reserved_batches.push_back(page);
                    continue;
                };
                if !inventory.record_canonical_entity_page(&page.batch.canonical_actions, scope.source(local).unwrap()) {
                    canonical_reserved_batches.push_back(page);
                    continue;
                }
                if let Some(allocator) = entity_origin_allocator.as_mut() {
                    for action in &page.batch.canonical_actions {
                        allocator.release_issued(pumpkin_cluster::canonical_action::ActionRef { tick: action.tick, id: action.id });
                    }
                }
                holistic_outbox.commit_canonical_reservation(page.parcels);
                for (key, actions) in page.realm.issued {
                    local_realm_issued.entry(tick).or_default().entry(key).or_default().extend(actions);
                }
                settlement.register_local(&server, &mut buckets, &page.batch);
                buckets.push_authenticated_batch(local, local_legacy_generation(&server, local), local, page.batch,
                    |chunk| scope.legacy_holders(chunk), |entity| super::cluster_entity_apply::current_entity_owner(&server, entity));
                local_finishes.record(page.legacy);
            }
            for (tick, actions) in core::mem::take(&mut canonical_queued) {
                let mut actor_segments = BTreeMap::<_, Vec<_>>::new();
                for action in actions {
                    actor_segments.entry((action.id.actor, action.id.incarnation)).or_default().push(action);
                }
                for actions in actor_segments.into_values() {
                let mut batch = TickBatch::new(tick);
                batch.canonical_actions = actions;
                let capacity_available = buckets.can_admit_canonical(batch.canonical_actions.len());
                match (TickFinish::try_from_batch(&batch), capacity_available) {
                    (Ok(_), true) => {
                        let prior = local_finishes.source_prefix(tick, &[], &canonical_reserved_batches);
                        let mut prior_realm = local_realm_issued.get(&tick).cloned().unwrap_or_default();
                        for page in canonical_reserved_batches.iter().filter(|page| page.batch.tick == tick) {
                            for (key, actions) in &page.realm.issued {
                                prior_realm.entry(*key).or_default().extend(actions.iter().copied());
                            }
                        }
                        let reservation = match (canonical_scopes.get(&tick), prior) {
                            (Some(scope), Ok(prior)) => holistic_outbox.reserve_source_batch(scope, batch,
                                &prior, &prior_realm, &realm_completion, |action| {
                                    chunk_blocks.historical_entity_authority(action)
                                        .or_else(|| entity_authority_at_action_tick(&canonical_graphs, action))
                                }),
                            _ => Err(batch),
                        };
                        match reservation {
                            Ok(page) => canonical_reserved_batches.push_back(page),
                            Err(batch) => canonical_queued.entry(tick).or_default().extend(batch.canonical_actions),
                        }
                    }
                    (manifest, _) => {
                        if canonical_graph_diagnostics.len() < 1_024
                            && canonical_graph_diagnostics.insert(tick)
                        {
                            tracing::error!(tick = tick.0, reason = ?manifest.err(), capacity_available, "cluster canonical batch admission rejected");
                        }
                        canonical_queued.entry(tick).or_default().extend(batch.canonical_actions);
                    }
                }
                }
            }
            statistics.publish();
            if let Some(mut local_tick) = pending_local_tick {
                last_local_source_tick = Some(local_tick.batch.tick);
                retick_batch(&mut local_tick.batch, resolution_tick);
                if let Ok(finish) = TickFinish::try_from_batch(&local_tick.batch) {
                    holistic_outbox.queue_batch(&server.cluster_connected_peers.load(), &local_tick.batch);
                    buckets.push_authenticated_batch(local, local_legacy_generation(&server, local), local, local_tick.batch, |chunk| canonical_scopes[&resolution_tick].legacy_holders(chunk), |entity| super::cluster_entity_apply::current_entity_owner(&server, entity));
                    local_finishes.record(finish);
                }
            }

            let quarantined: HashSet<_> = pending_imports.keys().copied().collect();
            while deferred_inventory_lifecycle.len() < 512 {
                let Ok(lifecycle) = inventory.lifecycle.try_recv() else { break; };
                deferred_inventory_lifecycle.push_back(lifecycle);
            }
            for _ in 0..deferred_inventory_lifecycle.len() {
                let Some(lifecycle) = deferred_inventory_lifecycle.pop_front() else { break; };
                if !inventory.can_accept_lifecycle(&lifecycle) || inventory.lifecycle_touches_quarantine(&server, &lifecycle, &quarantined) {
                    deferred_inventory_lifecycle.push_back(lifecycle);
                } else {
                    if matches!(&lifecycle, super::cluster_invsee::LocalInventoryLifecycle::BeginDeparture { .. })
                        && deferred_inventory_screens.len() >= 512
                    {
                        deferred_inventory_lifecycle.push_back(lifecycle);
                        continue;
                    }
                    let install_attempt = match &lifecycle {
                        super::cluster_invsee::LocalInventoryLifecycle::InstallPlayer {
                            gid, generation, ..
                        } => Some((*gid, *generation)),
                        _ => None,
                    };
                    if let Some(screen) = inventory.lifecycle(&server, lifecycle) {
                        deferred_inventory_screens.push_back(screen);
                    }
                    if let Some((gid, generation)) = install_attempt
                        && inventory.player_install_generation(gid) != Some(generation)
                    {
                        note_terminal_actor_generation(&mut retired_actor_install_generations, gid, generation);
                        let before = pending_actor_intents.len();
                        pending_actor_intents.retain(|intent| {
                            intent.actor != gid
                                || intent.owner_install_generation != generation
                        });
                        for _ in pending_actor_intents.len()..before {
                            server.cluster_controller.return_actor_intent_credit();
                        }
                    }
                }
            }
            inventory.drain_handoff_cancellations(&server);
            while deferred_inventory_menus.len() < 512 {
                let Ok(menu) = inventory.menus.try_recv() else { break; };
                deferred_inventory_menus.push_back(menu);
            }
            while deferred_inventory_actions.len() < 512 {
                let Ok(action) = inventory.actions.try_recv() else { break; };
                deferred_inventory_actions.push_back(action);
            }
            while deferred_inventory_screens.len() < 512 {
                let Ok(screen) = inventory.screens.try_recv() else { break; };
                deferred_inventory_screens.push_back(screen);
            }
            for _ in 0..deferred_inventory_actions.len() {
                let Some(action) = deferred_inventory_actions.pop_front() else { break; };
                if action.input_sequence.is_some() {
                    deferred_inventory_actions.push_back(action);
                    continue;
                }
                let action_targets = inventory.action_targets(&action);
                let prior_menu = deferred_inventory_menus.iter().any(|menu| {
                    menu.intent.actor == action.intent.actor
                        && input_precedes(menu.input_sequence, action.input_sequence, 2, 0)
                        && inventory.menu_targets(menu).is_some_and(|targets| {
                            targets.iter().any(|target| action_targets.contains(target))
                        })
                });
                let prior_screen = deferred_inventory_screens.iter().any(|screen| {
                    let actor = match &screen.command {
                        pumpkin_cluster::inventory::InventoryScreenCommand::BeginQuickCraft { session, .. }
                        | pumpkin_cluster::inventory::InventoryScreenCommand::AddQuickCraftSlot { session, .. }
                        | pumpkin_cluster::inventory::InventoryScreenCommand::FinishQuickCraft { session }
                        | pumpkin_cluster::inventory::InventoryScreenCommand::Close { session } =>
                            pumpkin_cluster::actor::ActorId::Player(session.actor),
                    };
                    actor == action.intent.actor
                        && input_precedes(screen.input_sequence, action.input_sequence, 1, 0)
                        && inventory.screen_targets(&server, screen).is_some_and(|targets| {
                            targets.iter().any(|target| action_targets.contains(target))
                        })
                });
                if prior_menu
                    || prior_screen
                    || inventory_capture_barrier.blocks(action.intent.actor, &action_targets)
                    || action.intent.affected.iter().any(|chunk| quarantined.contains(chunk)) {
                    deferred_inventory_actions.push_back(action);
                    continue;
                }
                if let Some((mut batch, _)) = inventory.action(&server, action) {
                    retick_batch(&mut batch, resolution_tick);
                    let Ok(finish) = TickFinish::try_from_batch(&batch) else { continue; };
                    for operation in &batch.inv_ops {
                        inventory_capture_barrier.register(operation);
                    }
                    settlement.register_local(&server, &mut buckets, &batch);
                    holistic_outbox.queue_batch(&server.cluster_connected_peers.load(), &batch);
                    buckets.push_authenticated_batch(local, local_legacy_generation(&server, local), local, batch, |chunk| canonical_scopes[&resolution_tick].legacy_holders(chunk), |entity| super::cluster_entity_apply::current_entity_owner(&server, entity));
                    local_finishes.record(finish);
                }
            }
            for _ in 0..deferred_inventory_screens.len() {
                let Some(screen) = deferred_inventory_screens.pop_front() else { break; };
                let Some(screen_targets) = inventory.screen_targets(&server, &screen) else {
                    inventory.reject_untrusted_screen(&server, screen);
                    continue;
                };
                if screen.input_sequence.is_some()
                    && matches!(screen.command, pumpkin_cluster::inventory::InventoryScreenCommand::Close { .. })
                {
                    deferred_inventory_screens.push_back(screen);
                    continue;
                }
                let screen_actor = match &screen.command {
                    pumpkin_cluster::inventory::InventoryScreenCommand::BeginQuickCraft { session, .. }
                    | pumpkin_cluster::inventory::InventoryScreenCommand::AddQuickCraftSlot { session, .. }
                    | pumpkin_cluster::inventory::InventoryScreenCommand::FinishQuickCraft { session }
                    | pumpkin_cluster::inventory::InventoryScreenCommand::Close { session } =>
                        pumpkin_cluster::actor::ActorId::Player(session.actor),
                };
                let prior_menu = deferred_inventory_menus.iter().any(|menu| {
                    menu.intent.actor == screen_actor
                        && input_precedes(menu.input_sequence, screen.input_sequence, 2, 1)
                        && inventory.menu_targets(menu).is_some_and(|targets| {
                            targets.iter().any(|target| screen_targets.contains(target))
                        })
                });
                let prior_action = deferred_inventory_actions.iter().any(|action| {
                    action.intent.actor == screen_actor
                        && input_precedes(action.input_sequence, screen.input_sequence, 0, 1)
                        && inventory.action_targets(action).iter().any(|target| screen_targets.contains(target))
                });
                if prior_menu
                    || prior_action
                    || inventory_capture_barrier.blocks(screen_actor, &screen_targets)
                    || screen.affected.iter().any(|chunk| quarantined.contains(chunk)) {
                    deferred_inventory_screens.push_back(screen);
                    continue;
                }
                if let Some((mut batch, _)) = inventory.screen(&server, screen) {
                    retick_batch(&mut batch, resolution_tick);
                    let Ok(finish) = TickFinish::try_from_batch(&batch) else { continue; };
                    for operation in &batch.inv_ops {
                        inventory_capture_barrier.register(operation);
                    }
                    settlement.register_local(&server, &mut buckets, &batch);
                    holistic_outbox.queue_batch(&server.cluster_connected_peers.load(), &batch);
                    buckets.push_authenticated_batch(local, local_legacy_generation(&server, local), local, batch, |chunk| canonical_scopes[&resolution_tick].legacy_holders(chunk), |entity| super::cluster_entity_apply::current_entity_owner(&server, entity));
                    local_finishes.record(finish);
                }
            }
            for _ in 0..deferred_inventory_menus.len() {
                let Some(menu) = deferred_inventory_menus.pop_front() else { break; };
                if menu.input_sequence.is_some() {
                    deferred_inventory_menus.push_back(menu);
                    continue;
                }
                let Some(menu_targets) = inventory.menu_targets(&menu) else {
                    inventory.reject_untrusted_menu(&server, menu);
                    continue;
                };
                let prior_action = deferred_inventory_actions.iter().any(|action| {
                    action.intent.actor == menu.intent.actor
                        && input_precedes(action.input_sequence, menu.input_sequence, 0, 2)
                        && inventory.action_targets(action).iter().any(|target| menu_targets.contains(target))
                });
                let prior_screen = deferred_inventory_screens.iter().any(|screen| {
                    let actor = match &screen.command {
                        pumpkin_cluster::inventory::InventoryScreenCommand::BeginQuickCraft { session, .. }
                        | pumpkin_cluster::inventory::InventoryScreenCommand::AddQuickCraftSlot { session, .. }
                        | pumpkin_cluster::inventory::InventoryScreenCommand::FinishQuickCraft { session }
                        | pumpkin_cluster::inventory::InventoryScreenCommand::Close { session } =>
                            pumpkin_cluster::actor::ActorId::Player(session.actor),
                    };
                    actor == menu.intent.actor
                        && input_precedes(screen.input_sequence, menu.input_sequence, 1, 2)
                        && inventory.screen_targets(&server, screen).is_some_and(|targets| {
                            targets.iter().any(|target| menu_targets.contains(target))
                        })
                });
                if prior_action
                    || prior_screen
                    || inventory_capture_barrier.blocks(menu.intent.actor, &menu_targets)
                    || menu.intent.affected.iter().any(|chunk| quarantined.contains(chunk))
                {
                    deferred_inventory_menus.push_back(menu);
                    continue;
                }
                if let Some((mut batch, _)) = inventory.menu(&server, menu) {
                    retick_batch(&mut batch, resolution_tick);
                    let Ok(finish) = TickFinish::try_from_batch(&batch) else { continue; };
                    for operation in &batch.inv_ops {
                        inventory_capture_barrier.register(operation);
                    }
                    settlement.register_local(&server, &mut buckets, &batch);
                    holistic_outbox.queue_batch(&server.cluster_connected_peers.load(), &batch);
                    buckets.push_authenticated_batch(local, local_legacy_generation(&server, local), local, batch, |chunk| canonical_scopes[&resolution_tick].legacy_holders(chunk), |entity| super::cluster_entity_apply::current_entity_owner(&server, entity));
                    local_finishes.record(finish);
                }
            }
            while deferred_inventory_snapshots.len() < 512 {
                let Ok(request) = inventory.snapshots.try_recv() else { break; };
                deferred_inventory_snapshots.push_back(request);
            }
            for _ in 0..deferred_inventory_snapshots.len() {
                let Some(request) = deferred_inventory_snapshots.pop_front() else { break; };
                if inventory
                    .ledger()
                    .target_chunks(request.target)
                    .is_some_and(|chunks| chunks.iter().any(|chunk| quarantined.contains(chunk)))
                {
                    deferred_inventory_snapshots.push_back(request);
                    continue;
                }
                if let Some(provider) = canonical_scopes.get(&resolution_tick).and_then(|scope| scope.source(local).ok()) {
                    let target = match request.target {
                        pumpkin_cluster::inventory::InventoryTarget::Player(gid) => inventory.local_target_chunks(request.target)
                            .filter(|chunks| chunks.len() == 1).map(|chunks| Target::Chunk {
                                key: chunks[0], part: pumpkin_cluster::canonical_action::ChunkPart::Player(gid), inventory: None,
                            }),
                        _ => None,
                    };
                    let cell = target.and_then(|target| {
                        super::cluster_world_canonical::OwnedWorldActionState {
                            server: &server, blocks: &mut chunk_blocks, actors: &mut actors,
                            living: &mut living, inventory: &mut inventory, versions: &mut canonical_versions,
                        }.read(TruthView::Local, target)
                    });
                    inventory.snapshot(request, provider, resolution_tick, &canonical_graphs, cell, &canonical_versions);
                } else {
                    let _ = request.reply.send(None);
                }
            }
            for _ in 0..256 {
                if acquisition_source.pending.len() >= 8192 { break; }
                let Ok(request) = chunk_inventory_rx.try_recv() else { break; };
                match request {
                    ChunkInventoryRequest::ActorCutRoute { output, input } => {
                        actor_cut_route = Some(output);
                        actor_cut_input = Some(input);
                        served_actor_cuts.clear();
                    }
                    ChunkInventoryRequest::Acquisition(input) => {
                        acquisition_source.pending.push_back(input);
                    }
                    ChunkInventoryRequest::AdvanceInhabited { tick, chunks } => {
                        for chunk in chunks {
                            if !inhabited_tick_is_newer(last_inhabited_tick.get(&chunk).copied(), tick) {
                                continue;
                            }
                            if !chunk_images.contains_key(&chunk) {
                                tracing::error!(chunk_x = chunk.x(), chunk_z = chunk.z(), "published chunk lacks owned image for inhabited tick");
                                buckets.quarantine_chunk(chunk);
                                super::cluster_chunk_presentation::retire(&server, chunk);
                                server.cluster_owned_chunk_blocks.rcu(|current| {
                                    let mut next = (**current).clone();
                                    next.remove(&chunk);
                                    Arc::new(next)
                                });
                                continue;
                            }
                            let count = chunk_inhabited_delta.entry(chunk).or_default();
                            let Some(next) = count.checked_add(1) else {
                                tracing::error!(chunk_x = chunk.x(), chunk_z = chunk.z(), "cluster inhabited counter overflow");
                                buckets.quarantine_chunk(chunk);
                                continue;
                            };
                            *count = next;
                            last_inhabited_tick.insert(chunk, tick);
                            if server.is_cluster_primary() {
                                primary_change_serial = primary_change_serial.wrapping_add(1);
                                primary_dirty_revisions.insert(chunk, primary_change_serial);
                                dirty_primary_chunks.insert(chunk);
                                pending_primary_save = Some(resolution_tick);
                            }
                        }
                    }
                    ChunkInventoryRequest::AcquisitionReceipts(receipts) => {
                        let desired = receipts.active_historical().into_iter()
                            .map(|(source, request)| ((source, request.chunk, request.lease), request)).collect::<BTreeMap<_, _>>();
                        let mut cuts = historical_chunk_cuts.clone();
                        for ((source, chunk, lease), _) in &historical_actor_requests {
                            if !desired.contains_key(&(*source, *chunk, *lease)) {
                                cuts.release_historical(*source, *chunk, *lease);
                            }
                        }
                        if desired.iter().all(|((source, _, _), request)| cuts.pin_historical(*source, *request).is_ok()) {
                            historical_chunk_cuts = cuts;
                            historical_actor_requests = desired;
                            historical_acquisition_receipts = receipts;
                        }
                    }
                    ChunkInventoryRequest::Export { chunk, respond } => {
                        deferred_chunk_exports.push_back((chunk, respond));
                    }
                    ChunkInventoryRequest::Install { chunk, persisted_generation, peer_reservation_generation, peer_source, genesis, occupancy, image, blocks, states, pendings, snapshot_cut, graphs, ground_anchors, completion, ground_frontier, player_inventory_frontiers, player_living, player_avatar_absent, entity_bodies, entity_history, dependency_history, item_spatial: imported_item_spatial, respond } => {
                        if genesis.as_ref().is_some_and(|certificate| certificate.chunk != chunk
                            || !server.cluster_namespaces.snapshot().authenticates_chunk_genesis(certificate)
                            || chunk_blocks.genesis(chunk).is_some_and(|old| old != certificate)) {
                            let _ = respond.send(Err(pumpkin_cluster::inventory::InventoryApplyError::Rejected));
                            continue;
                        }
                        let mut historical_frames = entity_history.clone();
                        for frame in &dependency_history {
                            if !historical_frames.contains(frame) { historical_frames.push(frame.clone()); }
                        }
                        let Ok(binding_proofs) = pumpkin_cluster::entity_history::EntityBindingProofContext::from_frames(&historical_frames) else {
                            let _ = respond.send(Err(pumpkin_cluster::inventory::InventoryApplyError::Rejected));
                            continue;
                        };
                        if entity_history.len() > 128 || entity_history.iter().any(|frame| snapshot_cut.distance_since(frame.snapshot_cut) >= 32_768 || frame.validate_with_context(Some(&binding_proofs)).is_err()) || !chunk_blocks.accepts_entity_history(&entity_history, &entity_bodies) {
                            let _ = respond.send(Err(pumpkin_cluster::inventory::InventoryApplyError::Rejected));
                            continue;
                        }
                        let scheduled_image = pumpkin_world::level::cluster_snapshot_scheduled_ticks(&image);
                        let mut block_entities = block_entities_from_image(&image, chunk);
                        let owned_image = pumpkin_world::level::OwnedChunkImage::decode(&image);
                        let light_sections = owned_image.as_ref().and_then(pumpkin_world::level::OwnedChunkImage::light_sections);
                        let mut candidate = buckets.clone();
                        let mut graph_candidate = canonical_graphs.clone();
                        if dependency_history.len() > 128 || dependency_history.iter().any(|frame| {
                            snapshot_cut.distance_since(frame.snapshot_cut) >= 32_768
                                || graph_candidate.install_dependency_history(Arc::new(frame.clone()), Some(&binding_proofs)).is_err()
                        }) {
                            let _ = respond.send(Err(pumpkin_cluster::inventory::InventoryApplyError::Rejected));
                            continue;
                        }
                        let mut completion_candidate = realm_completion.clone();
                        let graph_actions = graphs.iter().flat_map(|graph| graph.nodes.iter().map(move |node| (
                            pumpkin_cluster::canonical_action::ActionRef { tick: graph.tick, id: node.action.id },
                            node.action.clone(),
                        ))).collect::<BTreeMap<_, _>>();
                        let graph_action_count = graphs.iter().map(|graph| graph.nodes.len()).sum::<usize>();
                        let completion_ready = completion.as_ref().is_some_and(|snapshot|
                            completion_candidate.import_snapshot(snapshot, &graph_actions).is_ok());
                        let graph_evidence = if completion_ready {
                            pumpkin_cluster::action_graph::project_chunk_graph_snapshot_ground_with_context(
                                chunk, snapshot_cut, &graphs, &ground_anchors,
                                Some((&binding_proofs, &completion_candidate)))
                        } else { Err(pumpkin_cluster::canonical_action::ApplyFailure::InvalidAction) };
                        let occupancy_target = Target::Chunk { key: chunk,
                            part: pumpkin_cluster::canonical_action::ChunkPart::ActorOccupancy, inventory: None };
                        let occupancy_ready = graph_evidence.as_ref().is_ok_and(|projected| {
                            if occupancy.is_none() && !projected.contains_key(&occupancy_target) { return true; }
                            projected.get(&occupancy_target) == Some(&occupancy)
                                && occupancy.as_ref().is_some_and(|cell| matches!(&cell.value,
                                    pumpkin_cluster::canonical_action::Value::ActorOccupancy(root) if root.valid()))
                        });
                        let held_inventory_proof_ready = occupancy_ready && player_inventory_frontiers.iter().all(|(realm, frontier)| {
                            canonical_graphs.player_inventory_frontiers.get(&(
                                frontier.lease.key, *realm, frontier.lease.gid, frontier.lease.incarnation,
                            )).is_none_or(|existing| existing == frontier)
                        }) && graph_evidence.as_ref().is_ok_and(|projected| {
                            projected.iter().all(|(target, value)| {
                                if !matches!(target, Target::Player { inventory: Some(_), .. }) { return true; }
                                canonical_graphs.ground_anchors.get(target).is_none_or(|existing| {
                                    ground_anchors.iter().find(|(at, _)| at == target)
                                        .is_some_and(|(_, incoming)| incoming == existing)
                                        && &existing.value == value
                                })
                            })
                        });
                        let queue_target = Target::Chunk { key: chunk,
                            part: pumpkin_cluster::canonical_action::ChunkPart::Whole, inventory: None };
                        let mut scheduled_queue = graph_evidence.as_ref().ok()
                            .and_then(|projected| projected.get(&queue_target))
                            .and_then(|cell| cell.as_ref()).filter(|cell| {
                                matches!(&cell.value, pumpkin_cluster::canonical_action::Value::ScheduledQueue(queue)
                                    if scheduled_image.as_ref().is_some_and(|image| scheduled_raw_image_matches(queue, image, snapshot_cut)))
                            }).cloned();
                        let physical_absence_matches = player_avatar_absent.iter().all(|state| {
                            physical_player_avatar_absent(&actors, &living, state.target)
                        });
                        let entity_uuids = entity_bodies.iter().filter_map(|(_, cell)| match cell {
                            Some(pumpkin_cluster::canonical_action::Cell { value: pumpkin_cluster::canonical_action::Value::Entity(body), .. }) => Some(body.birth.uuid),
                            _ => None,
                        }).collect::<Vec<_>>();
                        let entity_evidence_matches = entity_uuids.iter().collect::<BTreeSet<_>>().len() == entity_uuids.len() && entity_bodies.iter().map(|(target, _)| *target).collect::<BTreeSet<_>>().len() == entity_bodies.len()
                            && entity_bodies.iter().all(|(target, cell)| {
                                matches!(target, Target::Chunk { key, part: pumpkin_cluster::canonical_action::ChunkPart::Entity(_), inventory: None } if *key == chunk)
                                    && graph_evidence.as_ref().is_ok_and(|projected| projected.get(target) == Some(cell))
                            });
                        let metadata_ready = pumpkin_world::level::cluster_snapshot_layout_matches(
                            &image,
                            chunk.x(),
                            chunk.z(),
                            blocks.min_y(),
                            blocks.block_palettes().len(),
                        ) && entity_evidence_matches && owned_image.is_some() && light_sections.is_some() && scheduled_queue.is_some() && block_entities.as_ref().is_some_and(|entities| {
                            block_entities_match_blocks(chunk, &blocks, entities)
                        }) && player_living.iter().all(|state| {
                            actors.can_import_remote(state) && living.can_import_remote(state)
                        }) && physical_absence_matches && graph_action_count == graph_actions.len() && graph_evidence.as_ref().is_ok_and(|observed| {
                            blocks.section_cells().iter().all(|(section_y, image)| {
                                let target = Target::Chunk { key: chunk, part: pumpkin_cluster::canonical_action::ChunkPart::BlockSection(*section_y), inventory: None };
                                observed.get(&target).and_then(|cell| cell.as_ref()).is_some_and(|cell| cell.value == image.value)
                            }) && observed.iter().all(|(target, cell)| match target {
                                Target::Chunk { key, part: pumpkin_cluster::canonical_action::ChunkPart::BlockSection(section_y), inventory: None } => *key == chunk && cell.as_ref().is_some_and(|cell| blocks.section_cells().iter().any(|(y, image)| y == section_y && cell.value == image.value)),
                                Target::Chunk { key, part: pumpkin_cluster::canonical_action::ChunkPart::LightSection(section_y), inventory: None } => *key == chunk && matches!(cell, Some(pumpkin_cluster::canonical_action::Cell { value: pumpkin_cluster::canonical_action::Value::LightSection { sky, block }, .. }) if light_sections.as_ref().is_some_and(|sections| sections.iter().any(|(y, stored_sky, stored_block)| y == section_y && stored_sky == sky && stored_block == block))),
                                Target::Chunk {
                                    key,
                                    part: pumpkin_cluster::canonical_action::ChunkPart::Block(position),
                                    inventory: None,
                                } => *key == chunk
                                    && matches!(cell, Some(pumpkin_cluster::canonical_action::Cell {
                                        value: pumpkin_cluster::canonical_action::Value::Block(state),
                                        ..
                                    }) if blocks.block(chunk, *position).is_ok_and(|block| block.as_u16() == *state)),
                                Target::Chunk {
                                    part: pumpkin_cluster::canonical_action::ChunkPart::Player(_),
                                    inventory: None,
                                    ..
                                } => player_living.iter().any(|state| {
                                    state.target == *target && Some(&state.cell) == cell.as_ref()
                                }),
                                Target::Chunk {
                                    key,
                                    part: pumpkin_cluster::canonical_action::ChunkPart::Entity(id),
                                    inventory: None,
                                } => *key == chunk && entity_bodies.iter().any(|(at, body)| {
                                    at == target && body == cell
                                        && body.as_ref().is_none_or(|cell| matches!(&cell.value,
                                            pumpkin_cluster::canonical_action::Value::Entity(body)
                                                if super::cluster_world_canonical::valid_entity_body(body, chunk, *id)
                                                    && chunk_blocks.entity_birth_matches(*id, &body.birth)))
                                        && chunk_blocks.entity_body_chunk(true, *id).is_none()
                                        && chunk_blocks.entity_body_chunk(false, *id).is_none()
                                }),
                                Target::Chunk {
                                    key,
                                    part: pumpkin_cluster::canonical_action::ChunkPart::Whole,
                                    inventory: None,
                                } => *key == chunk
                                    && matches!((cell, &scheduled_queue),
                                        (Some(pumpkin_cluster::canonical_action::Cell {
                                            value: pumpkin_cluster::canonical_action::Value::ScheduledQueue(projected), ..
                                        }), Some(pumpkin_cluster::canonical_action::Cell {
                                            value: pumpkin_cluster::canonical_action::Value::ScheduledQueue(image), ..
                                        })) if scheduled_queue_image_matches(projected, image)),
                                Target::Chunk {
                                    key,
                                    part: pumpkin_cluster::canonical_action::ChunkPart::BlockEntity(position),
                                    inventory: None,
                                } => *key == chunk && match cell {
                                    Some(pumpkin_cluster::canonical_action::Cell {
                                        value: pumpkin_cluster::canonical_action::Value::BlockEntity(projected),
                                        ..
                                    }) => block_entities.as_ref().is_some_and(|entities| entities.iter().any(|(at, image)| at == position && matches!(&image.value, pumpkin_cluster::canonical_action::Value::BlockEntity(bytes) if bytes == projected))),
                                    None => block_entities.as_ref().is_some_and(|entities| !entities.iter().any(|(at, _)| at == position)),
                                    _ => false,
                                },
                                _ => true,
                            })
                            &&
                            completion_ready
                            && player_inventory_frontiers.iter().all(|(realm, frontier)| {
                                graph_candidate.install_player_inventory_frontier(*realm, *frontier).is_ok()
                            })
                            && player_living.iter().all(|state| {
                                ground_anchors.iter().find(|(target, _)| *target == state.target)
                                    .is_some_and(|(_, anchor)| {
                                        graph_candidate.install_player_avatar_admission_at(
                                            state.target, state.lease, state.admitted_at,
                                            anchor.clone(),
                                        ).is_ok()
                                    })
                            })
                            && player_avatar_absent.iter().all(|state| {
                                ground_anchors.iter().find(|(target, _)| *target == state.target)
                                    .is_some_and(|(_, anchor)| {
                                        anchor.value.is_none()
                                            && graph_candidate.install_player_avatar_admission_at(
                                                state.target, state.lease, state.admitted_at,
                                                anchor.clone(),
                                            ).is_ok()
                                    })
                            })
                            && ground_frontier.is_none_or(|frontier| {
                                graph_candidate.install_chunk_frontier(chunk, frontier).is_ok()
                            })
                            && graph_candidate.merge_chunk_snapshot_with_context(
                                chunk, snapshot_cut, &graphs, &ground_anchors, &observed,
                                Some((&binding_proofs, &completion_candidate)),
                            ).is_ok()
                        });
                        let persisted_eligible = persisted_generation.is_none_or(|generation| {
                            server.is_cluster_primary()
                                && server.cluster_peer_incarnations.load().get(&local) == Some(&generation)
                                && !dirty_primary_chunks.contains(&chunk)
                                && !legacy_ground_without_graph.contains(&chunk)
                                && ground_frontier.is_some_and(|frontier| frontier.epoch == generation)
                        });
                        let peer_reservation_eligible = peer_reservation_generation.is_none_or(|generation| {
                            server.worlds.load().iter().find(|world| {
                                world.level.cluster_world_key == chunk.world.key
                                    && world.level.cluster_dimension == chunk.world.dimension
                            }).is_some_and(|world| {
                                world.level.cluster_publication_reservations.load().get(&pumpkin_util::math::vector2::Vector2::new(chunk.x(), chunk.z())).is_some_and(|reservation| {
                                    reservation.generation == generation
                                        && reservation.state == pumpkin_world::level::PersistedChunkReservationState::Recovery
                                })
                            })
                        });
                        let peer_source_eligible = peer_source.is_none_or(|(source, generation)| {
                            server.cluster_peer_generations.load().get(&source).copied() == Some(generation)
                        });
                        let menu_inventory_proof = pumpkin_cluster::xfer::validate_menu_inventory_roots(&states, &player_living,
                            graphs.iter().chain(entity_history.iter().chain(&dependency_history)
                                .flat_map(|frame| &frame.graphs)).flat_map(|graph| &graph.nodes),
                            completion.iter().chain(entity_history.iter().chain(&dependency_history).map(|frame| &frame.completion)));
                        let prepared_block_inventory = block_entities.as_ref().ok_or(pumpkin_cluster::inventory::InventoryApplyError::Rejected)
                            .and_then(|entities| {
                                let observed = graph_evidence.as_ref().map_err(|_| pumpkin_cluster::inventory::InventoryApplyError::Rejected)?;
                                let accepted = entities.iter().map(|(position, image_cell)| {
                                    let target = Target::Chunk { key: chunk, part: pumpkin_cluster::canonical_action::ChunkPart::BlockEntity(*position), inventory: None };
                                    let cell = observed.get(&target).and_then(|cell| cell.as_ref())
                                        .filter(|cell| cell.value == image_cell.value)
                                        .ok_or(pumpkin_cluster::inventory::InventoryApplyError::Rejected)?;
                                    Ok((*position, cell.clone()))
                                }).collect::<Result<Vec<_>, pumpkin_cluster::inventory::InventoryApplyError>>()?;
                                let images = block_inventory_root_images(chunk, &blocks, &accepted)?;
                                menu_inventory_proof.map_err(|_| pumpkin_cluster::inventory::InventoryApplyError::Rejected)?;
                                inventory.prepare_from_chunk(chunk, states).and_then(|prepared| inventory.bind_imported_block_roots(prepared, images))
                            });
                        let new_primary_claim = !chunk_blocks.contains(chunk)
                            && !pending_imports.contains_key(&chunk)
                            && prepared_block_inventory.is_ok()
                            && claim_primary_chunk_before_install(&server, chunk);
                        let result = if !held_inventory_proof_ready {
                            Err(pumpkin_cluster::inventory::InventoryApplyError::TargetUnavailable)
                        } else if !persisted_eligible || !peer_reservation_eligible || !peer_source_eligible
                            || pending_imports.contains_key(&chunk)
                            || chunk_blocks.contains(chunk)
                            || !new_primary_claim
                            || !metadata_ready
                            || pendings.iter().any(|entry| entry.state.chunk != chunk)
                            || candidate
                                .import_pending_chunk(
                                    local,
                                    pendings.into_iter().map(|entry| entry.state).collect(),
                                    |entity| super::cluster_entity_apply::current_entity_owner(&server, entity),
                                )
                                .is_err()
                        {
                            Err(pumpkin_cluster::inventory::InventoryApplyError::Rejected)
                        } else {
                            prepared_block_inventory.and_then(|prepared| inventory.commit_prepared_from_chunk(prepared))
                        };
                        if let Ok(committed) = result {
                            if let Some(certificate) = genesis { assert!(chunk_blocks.install_genesis(certificate).is_ok()); }
                            for state in &player_living {
                                assert!(actors.import_remote(state));
                                assert!(living.import_remote(state));
                            }
                            chunk_origins.insert(chunk, if persisted_generation.is_some() {
                                pumpkin_world::chunk::ChunkOrigin::Persisted
                            } else {
                                pumpkin_world::chunk::ChunkOrigin::PeerSnapshot
                            });
                            candidate.quarantine_chunk(chunk);
                            buckets = candidate;
                            canonical_graphs = graph_candidate;
                            realm_completion = completion_candidate;
                            if let Ok(observed) = &graph_evidence {
                                if let Some((_, Some(cell))) = observed.iter().find(|(target, _)| {
                                    matches!(target, Target::Chunk {
                                        key,
                                        part: pumpkin_cluster::canonical_action::ChunkPart::Whole,
                                        inventory: None,
                                    } if *key == chunk)
                                }) {
                                    scheduled_queue = Some(cell.clone());
                                }
                                if let Some(entities) = &mut block_entities {
                                    for (target, cell) in observed {
                                        let Target::Chunk {
                                            key,
                                            part: pumpkin_cluster::canonical_action::ChunkPart::BlockEntity(position),
                                            inventory: None,
                                        } = target else { continue; };
                                        if *key != chunk { continue; }
                                        if let Some((_, stored)) = entities.iter_mut().find(|(at, _)| at == position) {
                                            if let Some(cell) = cell { *stored = cell.clone(); }
                                        }
                                    }
                                }
                            }
                            chunk_blocks.install(chunk, blocks);
                            if let Some(cell) = occupancy {
                                assert!(chunk_blocks.install_occupancy_phase(false, chunk, cell.clone()));
                                assert!(chunk_blocks.install_occupancy_phase(true, chunk, cell));
                            }
                            for (section_y, initial) in chunk_blocks.initial_block_sections(chunk).expect("imported admission palettes") {
                                let target = Target::Chunk { key: chunk, part: pumpkin_cluster::canonical_action::ChunkPart::BlockSection(section_y), inventory: None };
                                let cell = graph_evidence.as_ref().ok().and_then(|observed| observed.get(&target)).and_then(|cell| cell.clone());
                                if let Some(cell) = cell {
                                    assert_eq!(cell.value, initial.value);
                                    chunk_blocks.install_block_section_phase(false, chunk, section_y, cell.clone());
                                    chunk_blocks.install_block_section_phase(true, chunk, section_y, cell);
                                }
                            }
                            for (section_y, sky, block) in light_sections.expect("validated imported lighting") {
                                let target = Target::Chunk { key: chunk, part: pumpkin_cluster::canonical_action::ChunkPart::LightSection(section_y), inventory: None };
                                let cell = graph_evidence.as_ref().ok().and_then(|observed| observed.get(&target)).and_then(|cell| cell.clone()).unwrap_or(pumpkin_cluster::canonical_action::Cell { revision: 0, value: pumpkin_cluster::canonical_action::Value::LightSection { sky, block } });
                                chunk_blocks.install_light_section_phase(false, chunk, section_y, Some(cell.clone()));
                                chunk_blocks.install_light_section_phase(true, chunk, section_y, Some(cell));
                            }
                            assert!(chunk_blocks.retain_entity_ground_frames(entity_history.into_iter().map(Arc::new).collect()));
                            assert!(chunk_blocks.install_scheduled_queue(chunk, scheduled_queue.unwrap()));
                            for (position, cell) in block_entities.unwrap() {
                                chunk_blocks.install_block_entity_phase(false, chunk, position, Some(cell.clone()));
                                chunk_blocks.install_block_entity_phase(true, chunk, position, Some(cell));
                            }
                                for (target, cell) in &entity_bodies {
                                if let Target::Chunk { key, part: pumpkin_cluster::canonical_action::ChunkPart::Entity(id), inventory: None } = *target {
                                    chunk_blocks.install_entity_body_phase(true, key, id, cell.clone());
                                    chunk_blocks.install_entity_body_phase(false, key, id, cell.clone());
                                }
                            }
                            for sample in imported_item_spatial {
                                let accepted = chunk_blocks.entity_body(false, chunk, sample.id)
                                    .and_then(|cell| match &cell.value {
                                        pumpkin_cluster::canonical_action::Value::Entity(body) => Some(body),
                                        _ => None,
                                    }).is_some_and(|body| canonical_scopes.get(&sample.tick).or_else(|| canonical_scopes.get(&resolution_tick))
                                        .is_some_and(|scope| entity_spatial_issuer_authorized(scope, body, chunk, sample.issuer))
                                        && item_spatial.apply(sample.clone(), body, chunk, sample.issuer));
                                let _ = accepted;
                            }
                            chunk_images.insert(chunk, owned_image.expect("validated immutable image"));
                                chunk_lighting.remove(&chunk);
                            chunk_inhabited_delta.remove(&chunk);
                            for (target, cell) in graph_evidence.unwrap_or_default() {
                                if let Some(cell) = cell {
                                    canonical_versions.insert((TruthView::Ground, target), cell.revision);
                                    canonical_versions.insert((TruthView::Local, target), cell.revision);
                                }
                            }
                            pending_imports.insert(chunk, (
                                ChunkAdmissionIdentity {
                                    persisted_generation,
                                    peer_reservation_generation,
                                    peer_source,
                                },
                                committed,
                            ));
                            let _ = respond.send(Ok(()));
                        } else {
                            if new_primary_claim && server.is_cluster_primary() {
                                for world in server.worlds.load().iter().filter(|world| {
                                    world.level.cluster_world_key == chunk.world.key
                                        && world.level.cluster_dimension == chunk.world.dimension
                                }) {
                                    world.level.release_primary_chunk_action_claim(pumpkin_util::math::vector2::Vector2::new(chunk.x(), chunk.z()));
                                    world.level.retire_primary_chunk_fence(pumpkin_util::math::vector2::Vector2::new(chunk.x(), chunk.z()));
                                }
                            }
                            let _ = respond.send(Err(pumpkin_cluster::inventory::InventoryApplyError::Rejected));
                        }
                    }
                    ChunkInventoryRequest::CommitReady { chunk, identity, respond } => {
                        if pending_imports.get(&chunk).is_some_and(|(installed, _)| *installed == identity) {
                            let (_, committed) = pending_imports.remove(&chunk).expect("checked admission identity");
                            let result = inventory.publish_committed_from_chunk(&server, committed);
                            if result.is_err() {
                                tracing::error!(chunk_x = chunk.x(), chunk_z = chunk.z(), "cluster inventory import publication failed");
                            }
                            if result.is_ok() {
                                if let Some(image) = chunk_images.get(&chunk) {
                                    super::cluster_chunk_presentation::publish(&server, chunk, &chunk_blocks, image, chunk_lighting.get(&chunk));
                                }
                                if let Some(snapshot) = chunk_blocks.snapshot(chunk) {
                                    server.cluster_owned_chunk_blocks.rcu(|current| {
                                        let mut next = (**current).clone();
                                        next.insert(chunk, snapshot.clone());
                                        std::sync::Arc::new(next)
                                    });
                                }
                                buckets.ready_chunk(chunk);
                            }
                            let _ = respond.send(result);
                        } else {
                            let _ = respond.send(Err(pumpkin_cluster::inventory::InventoryApplyError::Rejected));
                        }
                    }
                }
            }
            while pending_action_inputs.len() + canonical_queued.values().map(Vec::len).sum::<usize>() < 4096 {
                let Ok(action) = player_admin_rx.try_recv() else { break; };
                if let Some(lease) = inventory.player_lease_for_gid(action.target) {
                    pending_action_inputs.push(ActionInput::Proposal(pumpkin_cluster::canonical_action::ActionProposal::player_administration(action, lease)));
                }
            }
            publish_primary_unsettled_chunks(
                &server,
                &chunk_blocks,
                &mut buckets,
                &canonical_pending,
                &canonical_graphs,
                &mut unsettled_primary_retention,
            );
            let mut canonical_changed = Vec::new();
            {
                let ground = super::cluster_world_canonical::OwnedWorldActionState {
                    server: &server,
                    blocks: &mut chunk_blocks,
                    actors: &mut actors,
                    living: &mut living,
                    inventory: &mut inventory,
                    versions: &mut canonical_versions,
                };
                canonical_invalid_ticks.extend(collect_canonical_graph_inputs(
                    &server,
                    resolution_tick,
                    &mut buckets,
                    &canonical_scopes,
                    &mut realm_completion,
                    &mut canonical_graphs,
                    &ground,
                    &mut canonical_graph_diagnostics,
                    &mut local_realm_issued,
                    &mut canonical_changed,
                ));
            }
            for reference in &canonical_changed {
                if let Some(node) = canonical_graphs.ticks.get(&reference.tick)
                    .and_then(|graph| graph.actions.get(&reference.id))
                    && let Some(intent) = (match &node.action.semantic_intent {
                        Some(pumpkin_cluster::canonical_action::SemanticIntent::InventoryClose(intent)) => Some(intent),
                        _ => node.action.death_inventory_intent().and_then(|death| death.close.as_ref()),
                    })
                    && super::cluster_invsee::find_inventory_player(&server, intent.lease.gid)
                        .is_some_and(|player| player_action_lease(&server, &player) == Some(intent.lease))
                    && inventory.track_canonical_close(&node.action).is_err()
                {
                    canonical_changed_pending.insert(*reference);
                }
            }
            canonical_changed_pending.extend(canonical_changed);
            pending_actor_bindings.extend(canonical_changed_pending.iter().filter(|reference| {
                canonical_graphs.ticks.get(&reference.tick).and_then(|graph| graph.actions.get(&reference.id))
                    .is_some_and(|node| node.actor_binding.is_none() && node.action.captured_entity_source().is_some())
            }).copied());
            let finish_through = canonical_queued.keys().copied().fold(resolution_tick, |oldest, tick| {
                if oldest.is_after(tick) { tick } else { oldest }
            });
            let finish_through = canonical_reserved_batches.iter().map(|page| page.batch.tick)
                .fold(finish_through, |oldest, tick| if oldest.is_after(tick) { tick } else { oldest });
            let finish_through = pending_physics_tick.filter(|tick| finish_through.is_after(*tick)).unwrap_or(finish_through);
            let finish_through = pending_item_lifecycle_tick.filter(|tick| finish_through.is_after(*tick))
                .unwrap_or(finish_through);
            let finish_through = pending_living_tick.filter(|tick| finish_through.is_after(*tick))
                .unwrap_or(finish_through);
            let finish_through = pending_object_tick.filter(|tick| finish_through.is_after(*tick))
                .unwrap_or(finish_through);
            let finish_through = pending_projectile_tick.filter(|tick| finish_through.is_after(*tick))
                .unwrap_or(finish_through);
            let finish_through = pending_item_contact_tick.filter(|tick| finish_through.is_after(*tick)).unwrap_or(finish_through);
            let finish_through = item_contact_frame.as_ref().map_or(finish_through, |frame|
                if finish_through.is_after(frame.tick) { frame.tick } else { finish_through });
            let finish_through = frozen_vehicle_control.as_ref().map_or(finish_through, |work|
                if finish_through.is_after(work.tick) { work.tick } else { finish_through });
            let finish_through = vehicle_idle_frame.as_ref().map_or(finish_through, |frame|
                if finish_through.is_after(frame.tick) { frame.tick } else { finish_through });
            let finish_through = pending_controller_statistics.iter().map(|work| work.tick)
                .fold(finish_through, |oldest, tick| if oldest.is_after(tick) { tick } else { oldest });
            let finish_through = frozen_mining_work.values().map(|work| work.proposal.tick).fold(finish_through, |oldest, tick| if oldest.is_after(tick) { tick } else { oldest });
            let finish_through = callback_work.oldest_tick(finish_through);
            let finish_through = environment_source_bank.as_ref().map_or(finish_through, |bank| {
                if finish_through.is_after(bank.tick()) { bank.tick() } else { finish_through }
            });
            let finish_through = environment_source_input.as_ref().map_or(finish_through, |input| if finish_through.is_after(input.tick()) { input.tick() } else { finish_through });
            let finish_through = environment_source_fault.map_or(finish_through, |tick| {
                if finish_through.is_after(tick) { tick } else { finish_through }
            });
            let finish_through = deferred_inventory_actions.iter().map(|action| action.intent.tick)
                .chain(deferred_inventory_menus.iter().map(|menu| menu.intent.tick))
                .chain(deferred_inventory_screens.iter().map(|screen| screen.tick))
                .fold(finish_through, |oldest, tick| if oldest.is_after(tick) { tick } else { oldest });
            let finish_through = pending_action_inputs.iter().map(|input| match input {
                ActionInput::Complete(action) => action.tick,
                ActionInput::Proposal(proposal) => proposal.tick,
            }).fold(finish_through, |oldest, tick| if oldest.is_after(tick) { tick } else { oldest });
            if let Some(scope) = canonical_scopes.get(&resolution_tick) {
                acquisition_source.progress(scope, &mut holistic_outbox);
            }
            let mut ready_finishes = VecDeque::from(local_finishes.advance(finish_through));
            while let Some(finish) = ready_finishes.pop_front() {
                if let Some(scope) = canonical_scopes.get(&finish.tick)
                    && let Ok(source) = scope.source(local)
                    && !canonical_invalid_ticks.contains(&finish.tick)
                {
                    if scope.full_source_slot.is_none_or(|slot| !source_actor_snapshots.contains_key(&slot)) {
                        local_finishes.pending.entry(finish.tick).or_default().push(finish);
                        for remaining in ready_finishes { local_finishes.pending.entry(remaining.tick).or_default().push(remaining); }
                        break;
                    }
                    let Some(acquisition_close) = acquisition_source.prepare_close(finish.tick, scope, &mut holistic_outbox) else {
                        local_finishes.pending.entry(finish.tick).or_default().push(finish);
                        for remaining in ready_finishes { local_finishes.pending.entry(remaining.tick).or_default().push(remaining); }
                        break;
                    };
                    let realm_finish = pumpkin_cluster::realm_completion::HolisticFinish {
                        full_source_slot: scope.full_source_slot.unwrap(),
                        acquisition_prefix: Some(acquisition_close.prefix.sequence),
                        tick: finish.tick,
                        issued: local_realm_issued.get(&finish.tick).cloned().unwrap_or_default(),
                    };
                    let finish_valid = realm_completion.clone().note_finish(source, realm_finish.clone()).is_ok();
                    let ready = finish_valid && realm_finish.acquisition_prefix.is_some()
                        && action_issue_clock.can_close_tick(finish.tick)
                        && scope.tentative_local_manifest(finish.tick, &[], &realm_finish.issued, &realm_completion, |_| None)
                        .is_ok_and(|(_, recipients)| holistic_outbox.reserve_tick_finish(finish.tick, &recipients)
                            && holistic_outbox.queue_reserved_tick_finish(&finish, &realm_finish));
                    if !ready {
                        holistic_outbox.release_canonical_reservation(&acquisition_close.parcels);
                        local_finishes.pending.entry(finish.tick).or_default().push(finish);
                        for remaining in ready_finishes { local_finishes.pending.entry(remaining.tick).or_default().push(remaining); }
                        break;
                    }
                    acquisition_source.commit_close(acquisition_close, &mut holistic_outbox);
                    local_realm_issued.remove(&finish.tick);
                    if let Err(reason) = realm_completion.note_finish(source, realm_finish.clone()) {
                        canonical_invalid_ticks.insert(finish.tick);
                        if canonical_graph_diagnostics.len() < 1_024
                            && canonical_graph_diagnostics.insert(finish.tick)
                        {
                            tracing::error!(tick = finish.tick.0, ?reason, "cluster local realm finish rejected");
                        }
                    }
                } else {
                    local_finishes.pending.entry(finish.tick).or_default().push(finish);
                    for remaining in ready_finishes { local_finishes.pending.entry(remaining.tick).or_default().push(remaining); }
                    break;
                }
                local_finishes.closed_through = Some(finish.tick);
                action_issue_clock.close_tick(finish.tick);
                let finish_tick = finish.tick;
                buckets.note_authenticated_finish(local, local_legacy_generation(&server, local), local, finish, |chunk| canonical_scopes.get(&finish_tick).map_or_else(|| holders(&server, chunk), |scope| scope.legacy_holders(chunk)), |entity| super::cluster_entity_apply::current_entity_owner(&server, entity));
            }
            holistic_outbox.flush();
            settlement.progress(&server, &mut buckets);
            if last_graph_resolution_tick != Some(resolution_tick) {
                let mut proofs_by_entity = BTreeMap::new();
                pending_actor_bindings.retain(|reference| {
                    let Some(node) = canonical_graphs.ticks.get(&reference.tick)
                        .and_then(|graph| graph.actions.get(&reference.id)) else { return false; };
                    if node.actor_binding.is_some() || !node.applied_realms.is_empty() { return false; }
                    let action = node.action.clone();
                    let pumpkin_cluster::actor::ActorId::Entity(entity) = action.id.actor else { return false; };
                    let Some((source, birth, issuer)) = action.captured_entity_source() else { return false; };
                    let cut = TickStamp(reference.tick.0.wrapping_sub(1));
                    let Some(anchor) = canonical_graphs.ground_anchors.get(&source) else { return true; };
                    if anchor.settled_through != cut { return true; }
                    if matches!(anchor.value.as_ref(), Some(pumpkin_cluster::canonical_action::Cell {
                        value: pumpkin_cluster::canonical_action::Value::Entity(body), ..
                    }) if body.birth == *birth && body.owner == issuer) { return false; }
                    let proofs = proofs_by_entity.entry(entity).or_insert_with(|| chunk_blocks.entity_binding_proofs_for(entity));
                    let Ok(proofs) = proofs else { return true; };
                    let Ok(binding) = proofs.binding_candidate(&action) else { return true; };
                    let Some(binding) = binding else { return false; };
                    let Some(scope) = canonical_scopes.get(&reference.tick) else { return true; };
                    let Ok(chunks) = proofs.binding_chunks(&action, &binding) else { return true; };
                    let mut staged_completion = realm_completion.clone();
                    let mut keys = BTreeSet::new();
                    for chunk in chunks {
                        let Ok(holders) = scope.frozen_chunk_holders(chunk) else { return true; };
                        let key = RequirementKey::chunk(chunk, &holders);
                        if staged_completion.capture_with_producers(reference.tick, key, holders, scope.producers.clone()).is_err() { return true; }
                        keys.insert(key);
                    }
                    match canonical_graphs.install_actor_binding(binding, proofs, &mut staged_completion, keys) {
                        Ok(changed) => {
                            realm_completion = staged_completion;
                            canonical_changed_pending.extend(changed);
                            false
                        }
                        Err(_) => true,
                    }
                });
            }
            let current_rules_revision = server.cluster_world_rules.load().revision();
            let current_border_revision = server.cluster_world_borders.load().revision();
            let current_morning_revision = server.cluster_mornings.load().revision();
            if current_rules_revision != world_rules_revision || current_border_revision != world_border_revision || current_morning_revision != world_morning_revision {
                world_rules_revision = current_rules_revision;
                world_border_revision = current_border_revision;
                world_morning_revision = current_morning_revision;
                for tick in canonical_graphs.ticks.keys().copied().collect::<Vec<_>>() {
                    let graph = canonical_graphs.ticks.get_mut(&tick).unwrap();
                    canonical_changed_pending.extend(graph.invalidate_resolution_context().into_iter()
                        .map(|id| pumpkin_cluster::canonical_action::ActionRef { tick, id }));
                }
            }
            if !canonical_changed_pending.is_empty()
                && last_graph_resolution_tick != Some(resolution_tick)
            {
                let published_slots = server.cluster_original_source_slots.load();
                let additions = canonical_changed_pending.iter().filter(|reference| !published_slots.contains_key(reference))
                    .filter_map(|reference| realm_completion.action_source_slot(*reference)
                        .or_else(|| canonical_scopes.get(&reference.tick).and_then(|scope| scope.full_source_slot))
                        .map(|slot| (*reference, slot))).collect::<Vec<_>>();
                if !additions.is_empty() {
                    let mut slots = (**published_slots).clone();
                    slots.extend(additions);
                    server.cluster_original_source_slots.store(Arc::new(slots));
                }
                last_graph_resolution_tick = Some(resolution_tick);
                let held_chunks = canonical_graphs
                    .ticks
                    .values()
                    .flat_map(|graph| graph.actions.values())
                    .flat_map(|node| node.semantic_action().stakeholders.iter())
                    .filter_map(|stakeholder| match stakeholder {
                        pumpkin_cluster::canonical_action::Stakeholder::Chunk(chunk)
                            if chunk_blocks.contains(*chunk) => Some(*chunk),
                        _ => None,
                    })
                    .collect::<BTreeSet<_>>();
                let held_realms = canonical_graphs.ticks.values()
                    .flat_map(|graph| graph.actions.values())
                    .flat_map(|node| {
                        let chunks = &held_chunks;
                        let action = node.semantic_action();
                        node.realms.iter().copied().filter(move |realm| {
                            chunks.iter().any(|chunk| pumpkin_cluster::action_graph::action_realm_held_by_chunk(action, *realm, *chunk))
                        })
                    })
                    .collect::<BTreeSet<_>>();
                let mut owned = super::cluster_world_canonical::OwnedWorldActionState {
                    server: &server,
                    blocks: &mut chunk_blocks,
                    actors: &mut actors,
                    living: &mut living,
                    inventory: &mut inventory,
                    versions: &mut canonical_versions,
                };
                match pumpkin_cluster::action_solver::prepare_local_turn(
                    &canonical_graphs,
                    resolution_tick,
                    canonical_changed_pending.iter().copied(),
                    super::cluster_world_apply::CLUSTER_SEED,
                    |realm| held_realms.contains(&realm),
                    &owned,
                ) {
                    Ok(prepared) => {
                        let identities = entity_client_ids.prepare_local(&prepared.deltas.steps);
                        match owned.commit_local(&prepared.deltas) {
                        Ok(publication) => {
                            if let Ok(identities) = identities { entity_client_ids.commit(identities); }
                            canonical_graphs = prepared.graphs;
                            owned.inventory.publish_canonical_screens(&server, &canonical_graphs, owned.living);
                            owned.actors.synchronize_local_epochs(owned.living);
                            remote_player_presentation.ensure_bindings(&server, owned.actors, owned.living);
                            let entity_changes = super::cluster_entity_projection::EntityPresentationChanges::capture(&mut *owned.blocks, &mut item_spatial);
                            super::cluster_combat_directory::publish(&server, owned.actors, owned.living, owned.blocks, &item_spatial, &entity_client_ids, resolution_tick);
                            remote_player_presentation.publish(&server, owned.actors, owned.living, owned.inventory, &prepared.deltas.steps, resolution_tick);
                            entity_presentation.publish(&server, owned.blocks, &item_spatial, &mut entity_client_ids, owned.inventory, owned.actors, &remote_player_presentation, &canonical_graphs, &entity_changes, &prepared.deltas.steps);
                            player_presentation.publish(&server, &prepared.deltas.steps, |contact| {
                                owned.actors.local_contact_client_id(contact).or_else(|| {
                                    if let pumpkin_cluster::item_contact::ContactIdentity::Player(lease) = &contact.identity {
                                        owned.actors.remote_client_entity_id(*lease, contact.owner)
                                    } else { entity_client_ids.resolve_contact(contact) }
                                })
                            }, |gid| owned.living.get(pumpkin_cluster::actor::ActorId::Player(gid)).map(|truth| truth.local.clone()));
                            for reference in &canonical_changed_pending {
                                let Some(node) = canonical_graphs
                                    .ticks
                                    .get(&reference.tick)
                                    .and_then(|graph| graph.actions.get(&reference.id))
                                else {
                                    continue;
                                };
                                if matches!(node.action.semantic_intent, Some(pumpkin_cluster::canonical_action::SemanticIntent::ItemUse(_)))
                                    && node.action.id.actor.origin() == local {
                                    if let pumpkin_cluster::actor::ActorId::Player(gid) = node.action.id.actor {
                                        let first = canonical_block_acknowledged.insert(reference.id);
                                        super::cluster_inventory_protocol::publish_item_origin_only(&server, gid,
                                            first.then(|| super::cluster_inventory_protocol::canonical_item_origin(node.action.client_origin)).flatten(),
                                            node.factual.result.is_some());
                                    }
                                    continue;
                                }
                                if node.action.kind != pumpkin_cluster::canonical_action::ActionKind::Block
                                    || node.action.id.actor.origin() != local
                                {
                                    continue;
                                }
                                let Some((chunk, position)) = node.action.presentation.iter().find_map(|intent| match intent {
                                    pumpkin_cluster::world_presentation::WorldPresentationIntent::BreakParticles { chunk, position, .. } => Some((*chunk, *position)),
                                    _ => None,
                                }).or_else(|| node.action.targets().into_iter().find_map(|target| match target {
                                    Target::Chunk {
                                        key,
                                        part: pumpkin_cluster::canonical_action::ChunkPart::Block(position),
                                        inventory: None,
                                    } => Some((key, position)),
                                    _ => None,
                                })) else {
                                    continue;
                                };
                                let pumpkin_cluster::actor::ActorId::Player(gid) = node.action.id.actor else {
                                    continue;
                                };
                                let prior_ack = !canonical_block_acknowledged.insert(reference.id);
                                let accepted = node.factual.result.is_some();
                                let state = if accepted && !prior_ack {
                                    None
                                } else {
                                    owned.blocks.block(chunk, position).ok()
                                };
                                let sequence = match node.action.client_origin {
                                    pumpkin_cluster::canonical_action::ClientOrigin::JavaBlockSequence(sequence)
                                        if !prior_ack => Some(sequence),
                                    _ => None,
                                };
                                let _ = super::cluster_inventory_protocol::publish_block_action_resolution(
                                    &server, gid, position, state, sequence, accepted,
                                );
                            }
                            for reference in &canonical_changed_pending {
                                let Some(node) = canonical_graphs
                                    .ticks
                                    .get(&reference.tick)
                                    .and_then(|graph| graph.actions.get(&reference.id))
                                else {
                                    continue;
                                };
                                if node.action.kind != pumpkin_cluster::canonical_action::ActionKind::Inventory
                                    || node.action.id.actor.origin() != local
                                {
                                    continue;
                                }
                                let authoritative = match owned.inventory_snapshots_for_action(&node.action) {
                                    Ok(snapshots) => snapshots,
                                    Err(reason) => {
                                        if canonical_graph_diagnostics.len() < 1_024
                                            && canonical_graph_diagnostics.insert(reference.tick)
                                        {
                                            tracing::error!(tick = reference.tick.0, ?reason, "cluster canonical inventory outcome lacks owner snapshot");
                                        }
                                        continue;
                                    }
                                };
                                let accepted = node.factual.result.is_some();
                                let empty = pumpkin_cluster::inventory::InventoryDelta {
                                    revisions: Vec::new(),
                                    operations: Vec::new(),
                                };
                                if !accepted
                                    && let pumpkin_cluster::actor::ActorId::Player(gid) = node.action.id.actor
                                {
                                    let targets = authoritative.iter().map(|snapshot| snapshot.target).collect::<Vec<_>>();
                                    let _ = super::cluster_inventory_protocol::publish_authoritative_inventory_correction(
                                        &server,
                                        gid,
                                        super::cluster_inventory_protocol::canonical_inventory_session(node.action.client_origin),
                                        &targets,
                                    );
                                }
                                let _ = server.cluster_inventory_publication_tx.send(
                                    super::cluster_invsee::InventoryPublication {
                                        resolution: pumpkin_cluster::inventory::InventoryResolution {
                                            action: ActionKey::canonical(&node.action),
                                            tick: reference.tick,
                                            result: if accepted { Ok(empty.clone()) } else { Err(pumpkin_cluster::inventory::InventoryApplyError::Rejected) },
                                            authoritative,
                                            correction: empty,
                                            effects: Vec::new(),
                                            menu_effects: Vec::new(),
                                        },
                                        origin: super::cluster_inventory_protocol::canonical_inventory_origin(node.action.client_origin),
                                        session: super::cluster_inventory_protocol::canonical_inventory_session(node.action.client_origin),
                                    },
                                );
                            }
                            canonical_changed_pending.clear();
                            if let Some(publication) = publication {
                                let result = super::cluster_invsee::publish_committed_canonical_inventory(
                                    &server,
                                    &publication.snapshots,
                                    &publication.operations,
                                    |target| inventory.current_player_target_matches(target),
                                );
                                inventory.queue_canonical_publication(&publication.snapshots);
                                inventory.publish_canonical_relocations(
                                    &server,
                                    &publication.relocated_targets,
                                );
                                if result.is_err() {
                                    super::cluster_invsee::close_canonical_inventory_viewers(
                                        &server,
                                        &publication.snapshots,
                                        |target| inventory.current_player_target_matches(target),
                                    );
                                }
                            }
                            if !prepared.deltas.writes.is_empty() {
                                publish_canonical_changes(&server, &chunk_blocks, &prepared.deltas.writes);
                            }
                            for step in &prepared.deltas.steps {
                                if step.direction != pumpkin_cluster::action_graph::LocalDeltaDirection::Apply {
                                    continue;
                                }
                                if let Some(node) = canonical_graphs.ticks.get(&step.reference.tick)
                                    .and_then(|graph| graph.actions.get(&step.reference.id)) {
                                    super::cluster_world_presentation::publish(&server, node.semantic_action());
                                }
                            }
                        }
                        Err(reason) => {
                            if let Some(reference) = canonical_changed_pending.first()
                                && canonical_graph_diagnostics.len() < 1_024
                                && canonical_graph_diagnostics.insert(reference.tick)
                            {
                                tracing::error!(tick = reference.tick.0, ?reason, "cluster canonical local graph commit failed");
                            }
                        }
                    }
                    },
                    Err(reason) => {
                        if let Some(reference) = canonical_changed_pending.first()
                            && canonical_graph_diagnostics.len() < 1_024
                            && canonical_graph_diagnostics.insert(reference.tick)
                        {
                            tracing::error!(tick = reference.tick.0, ?reason, "cluster canonical local graph resolution deferred");
                        }
                    }
                }
            }
            while let Ok(result) = checkpoint_result_rx.try_recv() {
                primary_checkpoint_roots.remove(&result.request);
                if let Some(requested) = primary_checkpoint_inflight.remove(&result.request) {
                    complete_primary_checkpoint(
                        &mut dirty_primary_chunks,
                        &mut primary_dirty_revisions,
                        requested,
                        result.checkpoint_fsynced,
                    );
                    if !result.checkpoint_fsynced {
                        primary_checkpoint_retry_at = Some(std::time::Instant::now() + std::time::Duration::from_secs(5));
                    }
                    if !dirty_primary_chunks.is_empty() {
                        pending_primary_save = Some(resolution_tick);
                    }
                }
            }
            if let Some(tick) = pending_primary_save {
                let retry_ready = primary_checkpoint_retry_at.is_none_or(|at| std::time::Instant::now() >= at);
                if retry_ready {
                    let in_flight = primary_checkpoint_inflight.values().flatten().map(|(chunk, _)| *chunk).collect::<HashSet<_>>();
                    let dirty = dirty_primary_chunks.iter().filter(|chunk| !in_flight.contains(chunk)).take(64).copied().collect::<Vec<_>>();
                    if !dirty.is_empty() {
                        primary_checkpoint_request = primary_checkpoint_request.wrapping_add(1);
                        let request = primary_checkpoint_request;
                        if postcard::to_allocvec(&(request, &dirty))
                            .ok()
                            .is_some_and(|payload| server
                                .primary_save
                                .as_ref()
                                .is_some_and(|handle| handle.try_submit(tick, payload).is_ok()))
                        {
                            primary_checkpoint_inflight.insert(request, dirty.iter().map(|chunk| (*chunk, primary_dirty_revisions[chunk])).collect());
                            primary_checkpoint_retry_at = None;
                        }
                    } else if dirty_primary_chunks.is_empty() && primary_checkpoint_inflight.is_empty() {
                        pending_primary_save = None;
                    }
                }
            }
            let pending_legacy_ticks = buckets
                .pending_chunks()
                .into_iter()
                .map(|pending| pending.tick)
                .collect::<BTreeSet<_>>();
            let oldest_frontier_age = canonical_graphs
                .chunk_frontiers
                .values()
                .map(|frontier| resolution_tick.distance_since(frontier.settled_through))
                .chain(
                    canonical_graphs
                        .player_inventory_frontiers
                        .values()
                        .map(|frontier| resolution_tick.distance_since(frontier.settled_through)),
                )
                .max()
                .unwrap_or(0);
            let expired_scopes = canonical_scopes
                .keys()
                .copied()
                .filter(|tick| {
                    resolution_tick.distance_since(*tick) >= 600
                        && !canonical_graphs.ticks.contains_key(tick)
                        && !canonical_scopes.get(tick).and_then(|scope| scope.full_source_slot)
                            .is_some_and(|slot| source_actor_snapshots.contains_key(&slot))
                        && !pending_legacy_ticks.contains(tick)
                        && !frozen_proposal_work.values().any(|work| work.tick == *tick)
                        && !frozen_mining_work.values().any(|work| work.proposal.tick == *tick)
                        && !completed_callback_work.keys().any(|reference| reference.tick == *tick)
                        && !pending_action_inputs.iter().any(|input| match input {
                            ActionInput::Complete(action) => action.tick == *tick,
                            ActionInput::Proposal(proposal) => proposal.tick == *tick,
                        })
                        && !callback_work.oldest_pending_tick().is_some_and(|oldest| !oldest.is_after(*tick))
                        && !environment_source_input.as_ref().is_some_and(|input| !input.tick().is_after(*tick))
                        && !environment_source_bank.as_ref().is_some_and(|bank| !bank.tick().is_after(*tick))
                        && !environment_source_fault.is_some_and(|fault| !fault.is_after(*tick))
                        && !pending_historical_captures.iter().any(|(_, cut)| cut == tick)
                        && resolution_tick.distance_since(*tick) >= oldest_frontier_age
                })
                .collect::<Vec<_>>();
            for tick in expired_scopes {
                canonical_pages.retire_tick(tick);
                canonical_scopes.remove(&tick);
                realm_completion.retire_tick(tick);
                buckets.retire_authenticated_tick(tick);
                local_realm_issued.remove(&tick);
                canonical_graph_diagnostics.remove(&tick);
                canonical_invalid_ticks.remove(&tick);
            }
            while let Some(resolution) = pending_player_admin.pop_front() {
                if let Err(super::cluster_player_admin::PlayerAdministrativeQueueError::Full(resolution)) =
                    server.cluster_player_admin.queue_resolution(
                        resolution.action,
                        resolution.update.clone(),
                        resolution.accepted,
                        resolution.holders.clone(),
                    )
                {
                    pending_player_admin.push_front(resolution);
                    break;
                }
            }
            #[cfg(feature = "cluster-e2e")]
            test_graph_observer.observe(&server, &canonical_graphs, &living);
            let menu_archive_roots_ready = chunk_done_archive.set_action_roots(inventory.retained_menu_births()).is_ok();
            let published_directory = server.cluster_chunk_directory.load_full();
            #[cfg(feature = "cluster-e2e")]
            {
                inventory_scenario_issued.retain(|_, (proposal, references)| {
                    if references.iter().all(|reference| canonical_graphs.ticks.get(&reference.tick).is_some_and(|graph| graph.actions.contains_key(&reference.id))) {
                        inventory_scenario_source.issued_inventory_source(proposal, references);
                        false
                    } else { true }
                });
                inventory_scenario_source.advance(&server);
            }
            let wanted_history_pins = published_directory.active_subscriptions_for(chunk_blocks.owned_chunks()).into_iter()
                .filter(|subscription| !published_directory.holders_of(&subscription.chunk)
                    .is_some_and(|holders| holders.contains(&subscription.subscriber)))
                .map(|subscription| ((subscription.chunk, PeerGeneration {
                    server: ServerId(subscription.subscriber), generation: subscription.incarnation,
                }, subscription.subscription_id), subscription)).collect::<BTreeMap<_, _>>();
            for (key, subscription) in &wanted_history_pins {
                if !historical_pins.contains_key(key)
                    && historical_chunk_cuts.pin(*subscription).is_ok()
                    && chunk_done_archive.pin(*subscription).is_ok()
                {
                    historical_pins.insert(*key, *subscription);
                }
            }
            historical_pins.retain(|key, subscription| {
                if wanted_history_pins.contains_key(key) { return true; }
                historical_chunk_cuts.release(*subscription);
                chunk_done_archive.unpin(*subscription);
                false
            });
            for _ in 0..2 {
                let incoming = pending_actor_cut_image.take().or_else(|| actor_cut_input.as_mut().and_then(|input| input.try_recv().ok()));
                let Some((provider, image)) = incoming else { break; };
                let request = image.request;
                if image.ground.genesis.as_ref().is_some_and(|certificate|
                    !server.cluster_namespaces.snapshot().authenticates_chunk_genesis(certificate)) { continue; }
                let requester = PeerGeneration { server: local, generation: *server.cluster_peer_incarnations.load().get(&local).unwrap_or(&0) };
                if historical_actor_requests.get(&(requester, request.chunk, request.lease)) != Some(&request)
                    || server.cluster_peer_incarnations.load().get(&provider.server).copied() != Some(provider.generation) { continue; }
                let Some((original, _, _)) = source_actor_snapshots.get(&request.full_source_slot) else { continue; };
                let leases = image.phase.players.keys().copied().collect::<Vec<_>>();
                let Ok(phase) = original.original_chunk_phase(request.chunk, &leases) else { continue; };
                let bytes = match postcard::experimental::serialized_size(&image) { Ok(bytes) => bytes, Err(_) => continue };
                let key = (request.full_source_slot, request.chunk);
                let retained = historical_actor_cuts.iter().filter(|(held, _)| **held != key)
                    .try_fold(0_usize, |total, (_, (_, bytes))| total.checked_add(*bytes));
                if retained.and_then(|retained| retained.checked_add(bytes)).is_none_or(|total| total > pumpkin_cluster::canonical_action::MAX_LOGICAL_ACTION_BYTES) {
                    pending_actor_cut_image = Some((provider, image));
                    break;
                }
                if let Ok(cut) = super::cluster_actor_local_cut::ActorLocalChunkCut::from_validated_image(provider, requester, &phase, image) {
                    historical_actor_cuts.entry(key).or_insert((cut, bytes));
                }
            }
            served_actor_cuts.retain(|key, request| historical_actor_requests.get(key) == Some(request));
            historical_actor_need_pins.retain(|(slot,_),_| !local_finishes.closed_through(TickStamp(*slot as u16)));
            historical_actor_cuts.retain(|(slot, chunk), _| historical_actor_requests.iter().any(|((source, _, _), request)| source.server == local && request.full_source_slot == *slot && request.chunk == *chunk));
            if let Some(route) = &actor_cut_route {
                for (key, request) in &historical_actor_requests {
                    if served_actor_cuts.get(key) == Some(request) || request.provider.server != local
                        || server.cluster_peer_incarnations.load().get(&local).copied() != Some(request.provider.generation) { continue; }
                    let Ok(permit) = route.clone().try_reserve_owned() else { break; };
                    let Some((original, _, _)) = source_actor_snapshots.get(&request.full_source_slot) else { continue; };
                    let Some(export) = historical_chunk_cuts.get(request.chunk, request.cut()) else { continue; };
                    let Ok(image) = super::cluster_actor_local_cut::ActorLocalChunkCut::image_from_original(key.0, *request, (*export).clone(), original) else { continue; };
                    permit.send(ActorCutExport { requester: key.0, image });
                    served_actor_cuts.insert(*key, *request);
                }
            }
            historical_chunk_cuts.prune(&historical_acquisition_receipts);
            chunk_done_archive.prune_closed_source_cuts(&historical_acquisition_receipts);
            if let Some(previous) = historical_phase_tick {
                if resolution_tick.is_after(previous) && pending_historical_captures.is_empty() {
                    let distance = resolution_tick.distance_since(previous);
                    let capture_sources = canonical_scopes.get(&previous).into_iter()
                        .chain(canonical_scopes.get(&resolution_tick)).flat_map(|scope| scope.producers.iter().copied())
                        .collect::<BTreeSet<_>>();
                    for offset in 0..distance {
                        let cut = TickStamp(previous.0.wrapping_add(offset));
                        historical_capture_sources.entry(cut).or_insert_with(|| capture_sources.clone());
                        for chunk in &historical_phase_chunks {
                            let pinned = wanted_history_pins.values().any(|subscription| subscription.chunk == *chunk
                                && subscription.from_tick.0 == cut.0.wrapping_add(1));
                            let closed = historical_acquisition_receipts.cut_closed(TickStamp(cut.0.wrapping_add(1)), &capture_sources);
                            if chunk_blocks.contains(*chunk) && historical_chunk_cuts.get(*chunk, cut).is_none()
                                && (pinned || !closed) && pending_historical_captures.len() < 8192
                            {
                                pending_historical_captures.insert((*chunk, cut));
                                deferred_chunk_exports.push_front((*chunk, ChunkExportReply::Capture { cut }));
                            }
                        }
                    }
                }
            }
            let historical_ground_ready = pending_historical_captures.is_empty();
            if historical_ground_ready {
                historical_phase_tick = Some(resolution_tick);
                historical_phase_chunks = chunk_blocks.owned_chunks().collect();
            }
            let graph_ticks = if historical_ground_ready {
                canonical_graphs.ticks.keys().copied().collect::<Vec<_>>()
            } else { Vec::new() };
            let pending_legacy_chunks = buckets.pending_legacy_chunks();
            canonical_graphs.advance_dependency_history_cut(resolution_tick);
            for tick in graph_ticks {
                if canonical_invalid_ticks.contains(&tick) {
                    continue;
                }
                let held_chunks = canonical_graphs.ticks[&tick]
                    .actions
                    .values()
                    .flat_map(|node| node.semantic_action().stakeholders.iter())
                    .filter_map(|stakeholder| match stakeholder {
                        pumpkin_cluster::canonical_action::Stakeholder::Chunk(chunk)
                            if chunk_blocks.contains(*chunk) => Some(*chunk),
                        _ => None,
                    })
                    .collect::<BTreeSet<_>>();
                let mut blocked_legacy = pending_legacy_chunks.clone();
                blocked_legacy.extend(legacy_ground_without_graph.iter().copied());
                advance_graph_empty_chunk_frontiers(
                    &mut canonical_graphs,
                    &mut realm_completion,
                    &buckets,
                    &canonical_scopes,
                    tick,
                    &held_chunks,
                    &blocked_legacy,
                );
                let held_player_realms = canonical_graphs.ticks[&tick]
                        .actions
                        .values()
                        .flat_map(|node| {
                            node.realms.iter().filter_map(|realm| {
                                let pumpkin_cluster::action_graph::ActionRealm::Inventory(
                                    pumpkin_cluster::canonical_action::Stakeholder::Player(key),
                                    inventory_realm,
                                ) = realm
                                else {
                                    return None;
                                };
                                node.action
                                    .player_leases
                                    .iter()
                                    .find(|lease| lease.key == *key)
                                    .map(|lease| {
                                        (*key, *inventory_realm, lease.gid, lease.incarnation)
                                    })
                            })
                        })
                        .collect::<BTreeSet<_>>();
                advance_graph_empty_player_frontiers(
                    &mut canonical_graphs,
                    &mut realm_completion,
                    &buckets,
                    &canonical_scopes,
                    tick,
                    &held_player_realms,
                    &blocked_legacy,
                );
                let catch_up_targets = canonical_graphs.ticks[&tick]
                    .actions
                    .values()
                    .flat_map(|node| {
                        node.action.targets().into_iter()
                            .chain(node.action.counterfactual_conditions.iter().copied())
                            .chain(node.action.counterfactual_effects.iter().copied())
                            .chain(node.semantic_action().targets())
                            .chain(node.semantic_action().counterfactual_conditions.iter().copied())
                            .chain(node.semantic_action().counterfactual_effects.iter().copied())
                    })
                    .filter(|target| {
                        match target {
                            Target::Chunk { key, .. } => held_chunks.contains(key),
                            Target::Player {
                                key,
                                lease: Some(lease),
                                inventory: Some(id),
                            } => canonical_graphs
                                .player_inventory_frontiers
                                .get(&(*key, id.action_realm(), lease.gid, lease.incarnation))
                                .is_some_and(|frontier| {
                                    held_chunks.contains(&frontier.accepted_chunk)
                                }),
                            Target::Player { .. } => false,
                        }
                    })
                    .collect::<BTreeSet<_>>();
                let observed_catch_up = {
                    let owned = super::cluster_world_canonical::OwnedWorldActionState {
                        server: &server,
                        blocks: &mut chunk_blocks,
                        actors: &mut actors,
                        living: &mut living,
                        inventory: &mut inventory,
                        versions: &mut canonical_versions,
                    };
                    catch_up_targets
                        .iter()
                        .filter_map(|target| {
                            owned.available(TruthView::Ground, *target).then(|| {
                                (*target, owned.read(TruthView::Ground, *target))
                            })
                        })
                        .collect::<Vec<_>>()
                };
                for (target, value) in observed_catch_up {
                    match target {
                        Target::Chunk { .. } => {
                            canonical_graphs.catch_up_chunk_target_anchor(target, value);
                        }
                        Target::Player { .. } => {
                            canonical_graphs.catch_up_player_target_anchor(target, value);
                        }
                    }
                }
                if held_chunks.iter().any(|chunk| {
                    canonical_graphs.chunk_frontiers.get(chunk).is_none_or(|frontier| {
                        !legacy_chunk_range_closed(
                            &buckets,
                            &canonical_scopes,
                            frontier.settled_through,
                            tick,
                            *chunk,
                        )
                    })
                }) {
                    continue;
                }
                if held_chunks.iter().any(|chunk| !buckets.chunk_manifest_complete(tick, *chunk)) {
                    continue;
                }
                let blocked_legacy = held_chunks.iter().filter(|chunk| {
                    legacy_ground_without_graph.contains(chunk)
                        || pending_legacy_chunks.contains(chunk)
                }).copied().collect::<Vec<_>>();
                if !blocked_legacy.is_empty() {
                    for chunk in blocked_legacy {
                        if legacy_canonical_diagnostics.len() < 1_024
                            && legacy_canonical_diagnostics.insert(chunk)
                        {
                            tracing::error!(chunk_x = chunk.x(), chunk_z = chunk.z(), "cluster canonical ground blocked by legacy truth outside action graph");
                        }
                    }
                    continue;
                }
                let locally_held_realms = canonical_graphs.ticks[&tick]
                    .actions
                    .values()
                    .flat_map(|node| {
                        node.realms.iter().copied().filter(|realm| match realm {
                            pumpkin_cluster::action_graph::ActionRealm::Inventory(
                                pumpkin_cluster::canonical_action::Stakeholder::Player(player),
                                _,
                            ) => node
                                .action
                                .player_chunks
                                .iter()
                                .any(|(key, chunk)| key == player && held_chunks.contains(chunk)),
                            _ => held_chunks.iter().any(|chunk| realm.held_by_chunk(*chunk)),
                        })
                    })
                    .collect::<BTreeSet<_>>();
                let held = |realm: pumpkin_cluster::action_graph::ActionRealm| {
                    locally_held_realms.contains(&realm)
                };
                let pending_binding_entities = pending_actor_bindings.iter().filter_map(|reference| {
                    match reference.id.actor {
                        pumpkin_cluster::actor::ActorId::Entity(entity) if reference.tick == tick => Some(entity),
                        _ => None,
                    }
                }).collect::<BTreeSet<_>>();
                let rules_history = server.cluster_world_rules.load();
                let border_history = server.cluster_world_borders.load();
                let morning_history = server.cluster_mornings.load();
                let unavailable_rules_realms = canonical_graphs.ticks[&tick].actions.values()
                    .filter(|node| {
                        node.action.world_rule_witnesses().into_iter().any(|witness| !rules_history.contains(witness.rules))
                            || node.action.world_border_witnesses().into_iter().any(|witness| !border_history.contains(witness))
                            || node.action.morning_witnesses().into_iter().any(|(world, morning)|
                                !morning_history.contains(world, morning)
                                    || realm_completion.action_source_slot(pumpkin_cluster::canonical_action::ActionRef { tick, id: node.action.id }).is_none())
                            || node.action.source_slot_witnesses().into_iter().any(|slot|
                                realm_completion.action_source_slot(pumpkin_cluster::canonical_action::ActionRef { tick, id: node.action.id }) != Some(slot))
                    }).flat_map(|node| node.realms.iter().copied()).collect::<BTreeSet<_>>();
                let plan = canonical_graphs.ticks[&tick].ground_ready_plan(
                    tick,
                    |realm| realm_completion.realm_complete(tick, realm)
                        && !unavailable_rules_realms.contains(&realm)
                        && !matches!(realm, pumpkin_cluster::action_graph::ActionRealm::Entity(_, entity)
                            if pending_binding_entities.contains(&entity)),
                    |dependency| {
                        canonical_graphs
                            .ticks
                            .get(&dependency.tick)
                            .and_then(|graph| graph.actions.get(&dependency.id))
                            .is_some_and(|node| node.is_ground_applied())
                    },
                );
                if plan.components.is_empty() {
                    continue;
                }
                let mut owned = super::cluster_world_canonical::OwnedWorldActionState {
                    server: &server,
                    blocks: &mut chunk_blocks,
                    actors: &mut actors,
                    living: &mut living,
                    inventory: &mut inventory,
                    versions: &mut canonical_versions,
                };
                let staged = pumpkin_cluster::action_solver::stage_ground_ready(
                    &canonical_graphs.ticks[&tick],
                    &plan,
                    held,
                    &owned,
                );
                match staged.and_then(|staged| {
                    owned
                        .commit_graph_joint(&staged, &[])
                        .map(|publication| (staged, publication))
                        .map_err(pumpkin_cluster::action_solver::GroundDeltaFailure::Commit)
                }) {
                    Ok((staged, publication)) => {
                        if staged.applied_realms.is_empty() {
                            continue;
                        }
                        let mut published_blocks = BTreeMap::<ChunkKey, Vec<_>>::new();
                        for write in &staged.writes {
                            if let pumpkin_cluster::canonical_action::Target::Chunk {
                                key: chunk,
                                part: pumpkin_cluster::canonical_action::ChunkPart::Block(position),
                                inventory: None,
                            } = write.target
                                && let Some(pumpkin_cluster::canonical_action::Cell {
                                    value: pumpkin_cluster::canonical_action::Value::Block(state),
                                    ..
                                }) = &write.after
                            {
                                published_blocks.entry(chunk).or_default().push((position, *state));
                                let changed_light = write.before.as_ref().and_then(|cell| {
                                    match cell.value {
                                        pumpkin_cluster::canonical_action::Value::Block(old) =>
                                            pumpkin_data::BlockStateId::new(old),
                                        _ => None,
                                    }
                                }).zip(pumpkin_data::BlockStateId::new(*state)).is_some_and(|(old, new)| {
                                    pumpkin_world::lighting::LightEngine::has_different_light_properties(
                                        old.to_state(), new.to_state(),
                                    )
                                });
                                if changed_light {
                                    for dx in -1..=1 {
                                        for dz in -1..=1 {
                                            if let Some((x, z)) = chunk.x().checked_add(dx).zip(chunk.z().checked_add(dz)) {
                                                let affected = ChunkKey::new(chunk.world, x, z);
                                                dirty_lighting.insert(affected);
                                                chunk_lighting.remove(&affected);
                                                if lighting_work.as_ref().is_some_and(|(key, _)| *key == affected) {
                                                    lighting_work = None;
                                                }
                                                if server.is_cluster_primary() && chunk_blocks.contains(affected) {
                                                    primary_change_serial = primary_change_serial.wrapping_add(1);
                                                    primary_dirty_revisions.insert(affected, primary_change_serial);
                                                    dirty_primary_chunks.insert(affected);
                                                }
                                            }
                                        }
                                    }
                                }
                            }
                        }
                        for (chunk, changes) in published_blocks {
                            super::cluster_ground::publish_ground_blocks(&server, chunk, changes);
                            if let Some(snapshot) = chunk_blocks.snapshot(chunk) {
                                server.cluster_owned_chunk_blocks.rcu(|current| {
                                    let mut next = (**current).clone();
                                    next.insert(chunk, snapshot.clone());
                                    std::sync::Arc::new(next)
                                });
                            }
                        }
                        publish_current_local_after_ground(&server, &chunk_blocks, &staged.writes);
                        for (reference, realm) in staged.applied_realms {
                            if server.is_cluster_primary() {
                                if let Some(node) = canonical_graphs
                                    .ticks
                                    .get(&reference.tick)
                                    .and_then(|graph| graph.actions.get(&reference.id))
                                {
                                    for chunk in node.action.stakeholders.iter().filter_map(|stakeholder| match stakeholder {
                                        pumpkin_cluster::canonical_action::Stakeholder::Chunk(chunk) => Some(*chunk),
                                        _ => None,
                                    }) {
                                        primary_change_serial = primary_change_serial.wrapping_add(1);
                                        primary_dirty_revisions.insert(chunk, primary_change_serial);
                                        dirty_primary_chunks.insert(chunk);
                                    }
                                }
                            }
                            if let Err(reason) = canonical_graphs.mark_ground_realm_applied(reference, realm) {
                                tracing::error!(tick = tick.0, ?reason, "cluster canonical ground marker failed after commit");
                                canonical_invalid_ticks.insert(tick);
                            }
                            if canonical_graphs
                                .ticks
                                .get(&reference.tick)
                                .and_then(|graph| graph.actions.get(&reference.id))
                                .is_some_and(|node| node.is_ground_applied())
                            {
                                canonical_bucket_grounded_pending.insert(reference);
                                if let Some(node) = canonical_graphs.ticks.get(&reference.tick).and_then(|graph| graph.actions.get(&reference.id))
                                    && let [pumpkin_cluster::canonical_action::Effect::PlayerAvatarLifecycle { intent: pumpkin_cluster::avatar_lifecycle::AvatarLifecycleIntent::Retirement { lease, owner, .. }, .. }] = node.action.effects.as_slice()
                                    && owner.server != local
                                { pending_remote_avatar_retirements.insert(*lease); }
                                let retired = pending_avatar_retirements.iter().find_map(|(lease, (_, issued))| (*issued == Some(reference)).then_some(*lease));
                                if let Some(lease) = retired {
                                    if let Some((retire, _)) = pending_avatar_retirements.remove(&lease) {
                                        let _ = statistics.retire(retire);
                                        pending_inventory_retirements.insert(lease);
                                    }
                                }
                                if canonical_commands.remove(&reference).is_some() {
                                    if let Some(need) = canonical_command_needs.remove(&reference) {
                                        pending_admin_need_releases.push_back(need);
                                    }
                                    let accepted = canonical_graphs.ticks.get(&reference.tick)
                                        .and_then(|graph| graph.actions.get(&reference.id)).is_some_and(|node| node.factual.result.is_some());
                                    command_ground_outcomes.entry(reference.tick).or_default().push((reference, accepted));
                                }
                                if let Some(node) = canonical_graphs
                                    .ticks
                                    .get(&reference.tick)
                                    .and_then(|graph| graph.actions.get(&reference.id))
                                {
                                    if node.action.kind == pumpkin_cluster::canonical_action::ActionKind::World {
                                        administration.publish_canonical_ground(
                                            &server,
                                            &chunk_blocks,
                                            &node.action,
                                            node.factual.result.is_some(),
                                        );
                                    } else if node.action.kind == pumpkin_cluster::canonical_action::ActionKind::PlayerAdministration
                                        && let Some((mut update, need)) = canonical_player_admin.remove(&(reference.tick, reference.id))
                                    {
                                        if let Some(need) = need {
                                            pending_admin_need_releases.push_back(need);
                                        }
                                        let accepted = node.factual.result.is_some();
                                        if accepted
                                            && let Some(state) = living
                                                .get(pumpkin_cluster::actor::ActorId::Player(update.target))
                                                .and_then(|truth| truth.ground.player_administrative_state())
                                        {
                                            update.forward = state;
                                        }
                                        queue_player_administration_projection(
                                            &server,
                                            &mut pending_player_admin,
                                            ActionKey::canonical(&node.action),
                                            update,
                                            accepted,
                                        );
                                    }
                                }
                                canonical_pending.remove(&(reference.tick, reference.id));
                                canonical_block_acknowledged.remove(&reference.id);
                            }
                        }
                        let entity_history_references = staged.actions.iter().filter_map(|(reference, writes)| {
                            writes.iter().any(|write| {
                                if !matches!(write.target, Target::Chunk { part: pumpkin_cluster::canonical_action::ChunkPart::Entity(_), inventory: None, .. }) {
                                    return false;
                                }
                                let owner = |cell: &Option<pumpkin_cluster::canonical_action::Cell>| match cell {
                                    Some(pumpkin_cluster::canonical_action::Cell { value: pumpkin_cluster::canonical_action::Value::Entity(body), .. }) => Some(body.owner),
                                    _ => None,
                                };
                                owner(&write.before) != owner(&write.after)
                            }).then_some(*reference)
                        }).collect::<Vec<_>>();
                        if !entity_history_references.is_empty() {
                            let chunks = entity_history_references.iter().filter_map(|reference| {
                                canonical_graphs.ticks.get(&reference.tick)?.actions.get(&reference.id)
                            }).flat_map(|node| node.action.stakeholders.iter().filter_map(|stakeholder| match stakeholder {
                                pumpkin_cluster::canonical_action::Stakeholder::Chunk(chunk) => Some(*chunk),
                                _ => None,
                            })).collect();
                            if let Some(evidence) = pumpkin_cluster::entity_history::EntityHistoryEvidence::capture(
                                &canonical_graphs, &realm_completion, resolution_tick, &chunks,
                            ) {
                                chunk_blocks.retain_entity_ground_evidence(&entity_history_references, Arc::new(evidence));
                            }
                        }
                        if let Some(publication) = publication {
                            let result = super::cluster_invsee::publish_committed_canonical_inventory(
                                &server,
                                &publication.snapshots,
                                &publication.operations,
                                |target| inventory.current_player_target_matches(target),
                            );
                            inventory.queue_canonical_publication(&publication.snapshots);
                            inventory.publish_canonical_relocations(
                                &server,
                                &publication.relocated_targets,
                            );
                            if result.is_err() {
                                super::cluster_invsee::close_canonical_inventory_viewers(
                                    &server,
                                    &publication.snapshots,
                                    |target| inventory.current_player_target_matches(target),
                                );
                            }
                        }
                        if server.is_cluster_primary() {
                            pending_primary_save = Some(tick);
                        }
                    }
                    Err(reason) => {
                        if canonical_graph_diagnostics.len() < 1_024
                            && canonical_graph_diagnostics.insert(tick)
                        {
                            tracing::error!(tick = tick.0, ?reason, "cluster canonical ground graph promotion deferred");
                        }
                    }
                }
            }
            for reference in canonical_bucket_grounded_pending.iter().copied().collect::<Vec<_>>() {
                if buckets.commit_canonical_ground_applied(reference) {
                    canonical_bucket_grounded_pending.remove(&reference);
                }
            }
            while historical_ground_ready {
                let Some(ready) = buckets.pop_ready() else { break; };
                if ready.batches.iter().any(|sourced| {
                    !sourced.batch.canonical_actions.is_empty()
                        || !sourced.batch.break_block.is_empty()
                        || !sourced.batch.place_block.is_empty()
                        || !sourced.batch.administrative.is_empty()
                }) {
                    if unsupported_legacy_ticks.len() < 1_024
                        && unsupported_legacy_ticks.insert(ready.tick)
                    {
                        tracing::error!(
                            tick = ready.tick.0,
                            "cluster action cannot enter legacy apply outside canonical graph"
                        );
                    }
                    break;
                }
                let completed = apply_ready(
                    &server,
                    &mut chunk_blocks,
                    &mut actors,
                    &mut living,
                    &mut inventory,
                    &mut item_actions,
                    &mut combat,
                    &mut pending_boundaries,
                    &mut promoted_boundaries,
                    &mut pending_replica_boundaries,
                    &mut boundary_leaves,
                    &mut replica_boundary_diagnostics,
                    &mut settlement,
                    &mut inventory_capture_barrier,
                    &mut pending_player_admin,
                    &ready,
                );
                if completed {
                    let committed_tick = ready.tick;
                    for sourced in &ready.batches {
                        let mut legacy = sourced.batch.clone();
                        legacy.canonical_actions.clear();
                        if legacy.is_empty() {
                            continue;
                        }
                        if let Ok(finish) = TickFinish::try_from_batch(&legacy) {
                            legacy_ground_without_graph
                                .extend(finish.chunks.into_iter().map(|entry| entry.chunk));
                        }
                    }
                    if server.is_cluster_primary() {
                        for chunk in &ready.chunks {
                            primary_change_serial = primary_change_serial.wrapping_add(1);
                            primary_dirty_revisions.insert(*chunk, primary_change_serial);
                            dirty_primary_chunks.insert(*chunk);
                        }
                    }
                    buckets.commit_ready(ready);
                    if server.is_cluster_primary() {
                        pending_primary_save = Some(committed_tick);
                    }
                } else {
                    break;
                }
            }
            let pending_legacy_chunks = buckets.pending_legacy_chunks();
            #[cfg(feature = "cluster-e2e")]
            test_graph_observer.observe(&server, &canonical_graphs, &living);
            let drain_ticks = if historical_ground_ready {
                canonical_graphs.ticks.keys().copied().collect::<Vec<_>>()
            } else { Vec::new() };
            for tick in drain_ticks {
                let Some(graph) = canonical_graphs.ticks.get(&tick) else {
                    continue;
                };
                if canonical_invalid_ticks.contains(&tick)
                    || inventory.canonical_close_tick_pending(tick)
                    || graph.actions.is_empty()
                    || graph.actions.values().any(|node| {
                        !node.is_ground_applied()
                            || node
                                .realms
                                .iter()
                                .any(|realm| !realm_completion.realm_complete(tick, *realm))
                    })
                {
                    continue;
                }
                let chunks = graph
                    .actions
                    .values()
                    .flat_map(|node| {
                        node.action
                            .stakeholders
                            .iter()
                            .chain(&node.semantic_action().stakeholders)
                            .filter_map(|stakeholder| match stakeholder {
                                pumpkin_cluster::canonical_action::Stakeholder::Chunk(chunk) => {
                                    Some(*chunk)
                                }
                                _ => None,
                            })
                            .chain(node.action.player_chunks.iter().map(|(_, chunk)| *chunk))
                            .chain(node.action.actor_chunk)
                    })
                    .collect::<BTreeSet<_>>();
                if chunks.is_empty()
                    || chunks.iter().any(|chunk| {
                        !chunk_blocks.contains(*chunk)
                            || pending_legacy_chunks.contains(chunk)
                            || legacy_ground_without_graph.contains(chunk)
                            || canonical_graphs.chunk_frontiers.get(chunk).is_none_or(|frontier| {
                                !legacy_chunk_range_closed(
                                    &buckets,
                                    &canonical_scopes,
                                    frontier.settled_through,
                                    tick,
                                    *chunk,
                                )
                            })
                    })
                {
                    continue;
                }
                let entity_history_references = graph.actions.values().filter_map(|node| {
                    node.factual.result.as_ref().filter(|result| {
                        result.writes.iter().any(|write| {
                            matches!(write.target, Target::Chunk { part: pumpkin_cluster::canonical_action::ChunkPart::Entity(_), inventory: None, .. })
                                && write.before.as_ref().and_then(|cell| match &cell.value {
                                    pumpkin_cluster::canonical_action::Value::Entity(body) => Some(body.owner),
                                    _ => None,
                                }) != write.after.as_ref().and_then(|cell| match &cell.value {
                                    pumpkin_cluster::canonical_action::Value::Entity(body) => Some(body.owner),
                                    _ => None,
                                })
                        })
                    }).map(|_| pumpkin_cluster::canonical_action::ActionRef { tick, id: node.action.id })
                }).collect::<Vec<_>>();
                if !entity_history_references.is_empty() {
                    let history_chunks = entity_history_references.iter().filter_map(|reference| graph.actions.get(&reference.id)).flat_map(|node| node.action.stakeholders.iter().chain(&node.semantic_action().stakeholders).filter_map(|stakeholder| match stakeholder {
                        pumpkin_cluster::canonical_action::Stakeholder::Chunk(chunk) => Some(*chunk), _ => None,
                    })).collect();
                    let Some(evidence) = pumpkin_cluster::entity_history::EntityHistoryEvidence::capture(
                        &canonical_graphs, &realm_completion, resolution_tick, &history_chunks,
                    ) else {
                        continue;
                    };
                    if !chunk_blocks.retain_entity_ground_evidence(&entity_history_references, Arc::new(evidence)) {
                        continue;
                    }
                }
                let Some(targets) = canonical_graphs.ground_drain_targets(tick) else { continue; };
                let observed = {
                    let owned = super::cluster_world_canonical::OwnedWorldActionState {
                        server: &server,
                        blocks: &mut chunk_blocks,
                        actors: &mut actors,
                        living: &mut living,
                        inventory: &mut inventory,
                        versions: &mut canonical_versions,
                    };
                    targets
                        .iter()
                        .map(|target| {
                            owned.available(TruthView::Ground, *target).then(|| {
                                (*target, owned.read(TruthView::Ground, *target))
                            })
                        })
                        .collect::<Option<BTreeMap<_, _>>>()
                };
                if let Some(observed) = observed {
                    if canonical_graphs.dependency_history_needed(tick) {
                        let Some(evidence) = pumpkin_cluster::entity_history::EntityHistoryEvidence::capture(
                            &canonical_graphs, &realm_completion, resolution_tick, &chunks,
                        ) else { continue; };
                        let proofs = if evidence.graphs.iter().flat_map(|graph| &graph.nodes).any(|node| node.actor_binding.is_some()) {
                            let Some(frames) = chunks.iter().map(|chunk| chunk_blocks.entity_history_for_chunk(*chunk)).collect::<Option<Vec<_>>>() else { continue; };
                            let frames = frames.into_iter().flatten().collect::<Vec<_>>();
                            let Ok(proofs) = pumpkin_cluster::entity_history::EntityBindingProofContext::from_frames(
                                frames.iter().chain(std::iter::once(&evidence)),
                            ) else { continue; };
                            Some(proofs)
                        } else { None };
                        if canonical_graphs.install_dependency_history(Arc::new(evidence), proofs.as_ref()).is_err() {
                            continue;
                        }
                    }
                    if !menu_archive_roots_ready { continue; }
                    let archive_sources = canonical_scopes.get(&tick).into_iter()
                        .chain(canonical_scopes.get(&resolution_tick))
                        .flat_map(|scope| scope.producers.iter().copied()).collect::<BTreeSet<_>>();
                    if chunk_done_archive.requires(tick, &chunks)
                        || !historical_acquisition_receipts.cut_closed(TickStamp(resolution_tick.0.wrapping_add(1)), &archive_sources)
                    {
                        let Some(evidence) = pumpkin_cluster::entity_history::EntityHistoryEvidence::capture(
                            &canonical_graphs, &realm_completion, resolution_tick, &chunks,
                        ) else { continue; };
                        let Some(frames) = chunks.iter().map(|chunk| chunk_blocks.entity_history_for_chunk(*chunk)).collect::<Option<Vec<_>>>() else { continue; };
                        let frames = frames.into_iter().flatten().collect::<Vec<_>>();
                        let Ok(proofs) = pumpkin_cluster::entity_history::EntityBindingProofContext::from_frames(
                            frames.iter().chain(std::iter::once(&evidence)),
                        ) else { continue; };
                        if chunk_done_archive.reserve_before_source_cut_drain(tick, Arc::new(evidence), Some(&proofs),
                            archive_sources, &historical_acquisition_receipts).is_err() {
                            continue;
                        }
                    }
                    if let Some(drained) = canonical_graphs.drain_grounded_tick(tick, true, &observed) {
                        if let Some(allocator) = entity_origin_allocator.as_mut() {
                            for action in drained {
                                allocator.retire_settled(pumpkin_cluster::canonical_action::ActionRef { tick: action.tick, id: action.id });
                            }
                        }
                    }
                }
            }
            if let Some(route) = server.cluster_chunk_need_route.load_full() {
                let now = std::time::Instant::now();
                lighting_chunk_needs.retain(|chunk, (lease, last_needed)| {
                    if lighting_work.as_ref().is_some_and(|(center, _)| {
                        center.world == chunk.world
                            && (i64::from(center.x()) - i64::from(chunk.x())).abs() <= 1
                            && (i64::from(center.z()) - i64::from(chunk.z())).abs() <= 1
                    }) {
                        *last_needed = now;
                    }
                    now.duration_since(*last_needed) < std::time::Duration::from_secs(5)
                        || route.try_send(super::cluster::ChunkNeed::Release {
                            chunk: *chunk, lease: *lease,
                        }).is_err()
                });
            }
            let mut lighting_resolved_this_turn = lighting_work.is_some();
            for _ in 0..256 {
                let Ok(command) = watch_rx.try_recv() else { break; };
                chunk_watches.command(command);
            }
            chunk_watches.advance(&server, &chunk_blocks, |actor, session| {
                inventory.player_lease_for_gid(actor).map(|lease| lease.incarnation == session)
            });
            super::cluster_sync_stats::publish(&server, &canonical_graphs, resolution_tick);
            presentation_lighting.advance(&server, |chunk| world_for(&server, chunk.world).map(|world| (world.level.lighting_config, world.dimension.has_skylight)));
            if let Some((_, work)) = &mut lighting_work {
                if work.advance(250_000) {
                    let (chunk, work) = lighting_work.take().expect("present owned light work");
                    chunk_lighting.insert(chunk, work.finish().expect("completed owned light work"));
                    dirty_lighting.remove(&chunk);
                }
            }
            let queued_captures = deferred_chunk_exports.iter().filter_map(|(chunk, reply)| {
                if let ChunkExportReply::Capture { cut } = reply { Some((*chunk, *cut)) } else { None }
            }).collect::<BTreeSet<_>>();
            for (chunk, cut) in pending_historical_captures.difference(&queued_captures).copied().collect::<Vec<_>>() {
                deferred_chunk_exports.push_front((chunk, ChunkExportReply::Capture { cut }));
            }
            let export_count = deferred_chunk_exports.len();
            for _ in 0..export_count {
                let Some((chunk, respond)) = deferred_chunk_exports.pop_front() else { break; };
                if let ChunkExportReply::Serve { requested_cut: Some(cut), .. } = &respond {
                    if let Some(export) = historical_chunk_cuts.get(chunk, *cut) {
                        respond.send((*export).clone());
                    } else {
                        deferred_chunk_exports.push_back((chunk, respond));
                    }
                    continue;
                }
                let export_cut = match &respond {
                    ChunkExportReply::Capture { cut } => *cut,
                    _ => resolution_tick,
                };
                if pending_fresh.contains_key(&chunk) {
                    drop(respond);
                    continue;
                }
                let Some(&origin) = chunk_origins.get(&chunk) else {
                    drop(respond);
                    continue;
                };
                let Some(base_image) = chunk_images.get(&chunk) else {
                    drop(respond);
                    continue;
                };
                let delta = chunk_inhabited_delta.get(&chunk).copied().unwrap_or(0);
                let Some(pumpkin_cluster::canonical_action::Cell {
                    value: pumpkin_cluster::canonical_action::Value::ScheduledQueue(queue),
                    ..
                }) = chunk_blocks.scheduled_queue(true, chunk) else {
                    tracing::error!(chunk_x = chunk.x(), chunk_z = chunk.z(), "cluster scheduled queue missing at export");
                    drop(respond);
                    continue;
                };
                if pumpkin_cluster::canonical_action::due_scheduled_ticks(
                    queue,
                    export_cut,
                    0,
                )
                .is_err()
                    || queue
                        .iter()
                        .any(|tick| tick.due.distance_since(export_cut) > 256)
                {
                    tracing::error!(chunk_x = chunk.x(), chunk_z = chunk.z(), "cluster scheduled queue has unprocessed or unencodable due tick");
                    drop(respond);
                    continue;
                }
                let mut ordered = queue.iter().collect::<Vec<_>>();
                ordered.sort_by_key(|tick| {
                    let delay = if tick.due.is_after(export_cut) {
                        tick.due.distance_since(export_cut).saturating_sub(1).min(255)
                    } else { 0 };
                    (delay, tick.priority, tick.source_order, tick.kind, tick.position, tick.id)
                });
                let scheduled = ordered.into_iter().map(|tick| {
                    let delay = if tick.due.is_after(export_cut) {
                        tick.due.distance_since(export_cut).saturating_sub(1).min(255) as u8
                    } else { 0 };
                    pumpkin_world::level::ClusterScheduledTick {
                        kind: match tick.kind {
                            pumpkin_cluster::canonical_action::ScheduledTickKind::Block => pumpkin_world::level::ClusterScheduledKind::Block,
                            pumpkin_cluster::canonical_action::ScheduledTickKind::Fluid => pumpkin_world::level::ClusterScheduledKind::Fluid,
                        },
                        id: tick.id,
                        position: pumpkin_util::math::position::BlockPos::new(tick.position.x, tick.position.y, tick.position.z),
                        delay,
                        priority: pumpkin_world::tick::TickPriority::try_from(i32::from(tick.priority)).expect("validated scheduled priority"),
                    }
                }).collect::<Vec<_>>();
                let block_entities = chunk_blocks
                    .block_entities_for_chunk(true, chunk)
                    .into_iter()
                    .map(|(position, cell)| match cell.value {
                        pumpkin_cluster::canonical_action::Value::BlockEntity(bytes)
                            if chunk_blocks.ground_block(chunk, position).ok().is_some_and(|state| {
                                pumpkin_world::level::cluster_block_entity_document_type_matches(state, &bytes)
                            }) => Some((
                                pumpkin_util::math::position::BlockPos::new(position.x, position.y, position.z),
                                bytes,
                            )),
                        _ => None,
                    })
                    .collect::<Option<Vec<_>>>();
                let Some(block_entities) = block_entities else {
                    tracing::error!(chunk_x = chunk.x(), chunk_z = chunk.z(), "cluster block entity image export failed");
                    drop(respond);
                    continue;
                };
                let Some(ground_blocks) = chunk_blocks.ground_snapshot(chunk) else {
                    tracing::error!(
                        chunk_x = chunk.x(),
                        chunk_z = chunk.z(),
                        "cluster chunk export missing owned ground snapshot"
                    );
                    continue;
                };
                if dirty_lighting.contains(&chunk) {
                    if lighting_resolved_this_turn {
                        deferred_chunk_exports.push_back((chunk, respond));
                        continue;
                    }
                    let halo = (-1..=1).flat_map(|dx| (-1..=1).map(move |dz| (dx, dz)))
                        .map(|(dx, dz)| {
                            let x = chunk.x().checked_add(dx)?;
                            let z = chunk.z().checked_add(dz)?;
                            let key = ChunkKey::new(chunk.world, x, z);
                            if let Some((_, needed)) = lighting_chunk_needs.get_mut(&key) {
                                *needed = std::time::Instant::now();
                            } else if lighting_chunk_needs.len() < 8_192 {
                                if let Some(route) = server.cluster_chunk_need_route.load_full() {
                                    next_lighting_lease = next_lighting_lease.wrapping_add(1) | (1_u64 << 62);
                                    let lease = next_lighting_lease;
                                    if route.try_send(super::cluster::ChunkNeed::Acquire {
                                        chunk: key, lease, ready: None,
                                    }).is_ok() {
                                        lighting_chunk_needs.insert(key, (lease, std::time::Instant::now()));
                                    }
                                }
                            }
                            if buckets.is_quarantined(key) || pending_fresh.contains_key(&key)
                                || pending_imports.contains_key(&key) {
                                return None;
                            }
                            let snapshot = chunk_blocks.ground_snapshot(key)?;
                            (snapshot.min_y() == ground_blocks.min_y()).then_some(snapshot)
                        }).collect::<Vec<_>>().into_iter().collect::<Option<Vec<_>>>();
                    let Some(halo) = halo else {
                        if !respond.is_closed() {
                            deferred_chunk_exports.push_back((chunk, respond));
                        }
                        continue;
                    };
                    let Some((lighting_config, has_skylight)) = server.worlds.load().iter()
                        .find(|world| world.level.cluster_world_key == chunk.world.key
                            && world.level.cluster_dimension == chunk.world.dimension)
                        .map(|world| (world.level.lighting_config, world.dimension.has_skylight)) else {
                        drop(respond);
                        continue;
                    };
                    lighting_resolved_this_turn = true;
                    match pumpkin_world::lighting::owned::OwnedLightingWork::start(
                        std::array::from_fn(|index| halo[index].block_palettes()),
                        lighting_config,
                        has_skylight,
                    ) {
                        Ok(mut work) => {
                            if work.advance(250_000) {
                                chunk_lighting.insert(chunk, work.finish().expect("completed owned light work"));
                                dirty_lighting.remove(&chunk);
                            } else {
                                lighting_work = Some((chunk, work));
                                if !respond.is_closed() {
                                    deferred_chunk_exports.push_back((chunk, respond));
                                }
                                continue;
                            }
                        }
                        Err(reason) => {
                            tracing::warn!(chunk_x = chunk.x(), chunk_z = chunk.z(), ?reason, "cluster owned lighting export deferred");
                            drop(respond);
                            continue;
                        }
                    }
                }
                let Some(image) = base_image.render_ground(
                    ground_blocks.min_y(),
                    ground_blocks.block_palettes(),
                    ground_blocks.biome_palettes(),
                    delta,
                    &scheduled,
                    &block_entities,
                        chunk_lighting.get(&chunk),
                ).map(Arc::<[u8]>::from) else {
                    tracing::error!(chunk_x = chunk.x(), chunk_z = chunk.z(), "cluster owned image export failed");
                    drop(respond);
                    continue;
                };
                let authenticated = server.cluster_peer_incarnations.load();
                let mut holders = server.cluster_chunk_directory.load_full()
                    .holders_of(&chunk)
                    .into_iter()
                    .flatten()
                    .copied()
                    .filter(|peer| authenticated.contains_key(&ServerId(*peer)))
                    .collect::<Vec<_>>();
                holders.push(local.0);
                holders.sort_unstable();
                holders.dedup();
                let Some(inventory_states) = inventory.export_for_chunk(chunk) else {
                    tracing::error!(chunk_x = chunk.x(), chunk_z = chunk.z(), "cluster chunk export missing player lease");
                    continue;
                };
                let Ok(graphs) = canonical_graphs.snapshot_for_chunk_at_cut(chunk, export_cut) else {
                    deferred_chunk_exports.push_back((chunk, respond));
                    continue;
                };
                let ground_anchors = canonical_graphs.snapshot_anchors_for_graphs(chunk, &graphs);
                let Some(entity_history) = chunk_blocks.entity_history_for_chunk(chunk) else {
                    tracing::error!(chunk_x = chunk.x(), chunk_z = chunk.z(), "cluster chunk export entity history exceeds transfer capacity");
                    continue;
                };
                let mut dependency_roots = chunk_blocks.scheduled_dependency_roots(Some(chunk));
                dependency_roots.extend(graphs.iter().flat_map(|graph| graph.nodes.iter().flat_map(|node| node.action.dependencies.iter().copied())));
                let Ok(dependency_frames) = canonical_graphs.dependency_history_for(&dependency_roots) else {
                    continue;
                };
                let mut dependency_history = dependency_frames.into_iter().map(|frame| (*frame).clone()).collect::<Vec<_>>();
                for frame in chunk_done_archive.history_for_chunk_at_cut(chunk, export_cut) {
                    if !dependency_history.contains(frame.as_ref()) { dependency_history.push((*frame).clone()); }
                }
                let mut historical_frames = entity_history.clone();
                for frame in &dependency_history {
                    if !historical_frames.contains(frame) { historical_frames.push(frame.clone()); }
                }
                let Ok(binding_proofs) = pumpkin_cluster::entity_history::EntityBindingProofContext::from_frames(&historical_frames) else {
                    tracing::error!(chunk_x = chunk.x(), chunk_z = chunk.z(), "cluster chunk export entity proof dependencies invalid");
                    continue;
                };
                let Some(projected_avatar_targets) = pumpkin_cluster::action_graph::project_chunk_graph_snapshot_ground_with_context(
                    chunk, export_cut, &graphs, &ground_anchors,
                    Some((&binding_proofs, &realm_completion)),
                ).ok() else {
                    tracing::error!(chunk_x = chunk.x(), chunk_z = chunk.z(), "cluster chunk export cannot project graph ground");
                    continue;
                };
                let entity_bodies = projected_avatar_targets.iter().filter_map(|(target, cell)| {
                    matches!(target, Target::Chunk { key, part: pumpkin_cluster::canonical_action::ChunkPart::Entity(_), inventory: None } if *key == chunk)
                        .then_some((*target, cell.clone()))
                }).collect::<Vec<_>>();
                if entity_bodies.iter().any(|(target, expected)| {
                    let Target::Chunk { key, part: pumpkin_cluster::canonical_action::ChunkPart::Entity(id), inventory: None } = *target else { return true; };
                    chunk_blocks.entity_body(true, key, id) != expected.as_ref()
                }) {
                    tracing::error!(chunk_x = chunk.x(), chunk_z = chunk.z(), "cluster chunk export entity graph disagrees with owned ground");
                    continue;
                }
                let player_living = ground_anchors
                    .iter()
                    .filter_map(|(target, _)| match target {
                        Target::Chunk {
                            key,
                            part: pumpkin_cluster::canonical_action::ChunkPart::Player(gid),
                            inventory: None,
                        } if *key == chunk && projected_avatar_targets.get(target).is_some_and(Option::is_some) => Some((*target, *gid)),
                        _ => None,
                    })
                    .map(|(target, gid)| {
                        let lease = inventory.player_lease_for_gid(gid)?;
                        let spatial = actors.get(gid)?;
                        let avatar = living.phase_state(pumpkin_cluster::actor::ActorId::Player(gid), true)?;
                        (spatial.ground.chunk == chunk).then_some(())?;
                        Some(pumpkin_cluster::xfer::PlayerLivingGround {
                            lease,
                            admitted_at: *canonical_graphs.player_avatar_admitted_at.get(&target)?,
                            target,
                            cell: pumpkin_cluster::canonical_action::Cell {
                                revision: canonical_versions
                                    .get(&(TruthView::Ground, target))
                                    .copied()
                                    .unwrap_or(0),
                                value: pumpkin_cluster::canonical_action::Value::Living(
                                    (avatar.clone()).into(),
                                ),
                            },
                            spatial: pumpkin_cluster::protocol::PosUpdate {
 spatial_epoch: avatar.avatar.as_ref()?.spatial_epoch,
                                gid,
                                incarnation: lease.incarnation,
                                seq: avatar.avatar.as_ref()?.movement_sequence,
                                tick: avatar.avatar.as_ref()?.movement_tick,
                                world: spatial.ground.world,
                                pos: spatial.ground.frame.effective.position,
                                vel: spatial.ground.frame.effective.velocity,
                                yaw: spatial.ground.frame.effective.yaw,
                                head_yaw: f32::from_bits(avatar.avatar.as_ref()?.head_yaw),
                                pitch: spatial.ground.frame.effective.pitch,
                                on_ground: avatar.avatar.as_ref()?.on_ground,
                            },
                        })
                    })
                    .collect::<Option<Vec<_>>>();
                let Some(player_living) = player_living else {
                    tracing::error!(chunk_x = chunk.x(), chunk_z = chunk.z(), "cluster chunk export missing accepted player avatar");
                    continue;
                };
                let player_avatar_absent = ground_anchors
                    .iter()
                    .filter_map(|(target, _)| match target {
                        Target::Chunk {
                            key,
                            part: pumpkin_cluster::canonical_action::ChunkPart::Player(_),
                            inventory: None,
                        } if *key == chunk && projected_avatar_targets.get(target) == Some(&None) => Some(*target),
                        _ => None,
                    })
                    .map(|target| {
                        Some(pumpkin_cluster::xfer::PlayerAvatarAbsentGround {
                            lease: *canonical_graphs.player_avatar_admissions.get(&target)?,
                            admitted_at: *canonical_graphs.player_avatar_admitted_at.get(&target)?,
                            target,
                        })
                    })
                    .collect::<Option<Vec<_>>>();
                let Some(player_avatar_absent) = player_avatar_absent else {
                    tracing::error!(chunk_x = chunk.x(), chunk_z = chunk.z(), "cluster chunk export missing accepted player absence");
                    continue;
                };
                let physical_absence_matches = player_avatar_absent.iter().all(|state| {
                    physical_player_avatar_absent(&actors, &living, state.target)
                });
                if !physical_absence_matches {
                    tracing::error!(chunk_x = chunk.x(), chunk_z = chunk.z(), "cluster chunk export graph absence disagrees with owned ground");
                    continue;
                }
                let player_inventory_frontiers = canonical_graphs
                    .player_inventory_frontiers
                    .iter()
                    .filter_map(|((_, realm, _, _), frontier)| {
                        (frontier.accepted_chunk == chunk
                            && inventory_states.iter().any(|state| {
                                state.player_lease == Some(frontier.lease)
                                    && state.chunks.contains(&chunk)
                            }))
                        .then_some((*realm, *frontier))
                    })
                    .collect();
                let references = graphs
                    .iter()
                    .flat_map(|graph| {
                        graph.nodes.iter().map(|node| {
                            pumpkin_cluster::canonical_action::ActionRef {
                                tick: graph.tick,
                                id: node.action.id,
                            }
                        })
                    })
                    .collect();
                let Some(completion) = realm_completion.snapshot_for_chunk(
                    chunk,
                    TickStamp(export_cut.0.wrapping_sub(32_767)),
                    export_cut,
                    &references,
                ) else {
                    tracing::error!(chunk_x = chunk.x(), chunk_z = chunk.z(), "cluster chunk export missing frozen completion scope");
                    continue;
                };
                let exported_item_spatial = entity_bodies.iter().filter_map(|(_, cell)| {
                    let Some(pumpkin_cluster::canonical_action::Cell { value: pumpkin_cluster::canonical_action::Value::Entity(body), .. }) = cell else { return None; };
                    item_spatial.get(body.id).filter(|sample| !sample.tick.is_after(export_cut) && sample.birth == body.birth
                        && sample.epoch == body.spatial_epoch
                        && sample.spatial.world == chunk.world
                        && sample.spatial.position.iter().all(|value| f64::from_bits(*value).is_finite())
                        && f32::from_bits(sample.spatial.yaw).is_finite()
                        && f32::from_bits(sample.spatial.pitch).is_finite()
                        && f32::from_bits(sample.head_yaw).is_finite()).cloned()
                }).collect();
                let export = ChunkStateExport {
                    genesis: chunk_blocks.genesis(chunk).cloned(),
                    image,
                    occupancy: chunk_blocks.occupancy_root(true, chunk).cloned(),
                    origin,
                    snapshot_cut: export_cut,
                    ground_frontier: if legacy_ground_without_graph.contains(&chunk) {
                        None
                    } else {
                        canonical_graphs.chunk_frontiers.get(&chunk).copied()
                    },
                    player_inventory_frontiers,
                    ground_blocks,
                    holders,
                    inventory: inventory_states,
                    player_living,
                    player_avatar_absent,
                    entity_bodies,
                    entity_history,
                    dependency_history,
                    item_spatial: exported_item_spatial,
                    pendings: buckets
                        .export_pending_chunk(chunk)
                        .into_iter()
                        .filter(|state| !state.tick.is_after(export_cut))
                        .map(|state| pumpkin_cluster::xfer::PendingRef { state })
                        .collect(),
                    graphs,
                    ground_anchors,
                    finishes: realm_completion.snapshot_finishes_through(export_cut),
                    completion,
                };
                if matches!(respond, ChunkExportReply::Capture { .. }) {
                    let payload = export.clone().into_payload(chunk, local.0);
                    if payload.validate_transfer_evidence().is_err() {
                        deferred_chunk_exports.push_back((chunk, respond));
                        continue;
                    }
                    let bytes = postcard::experimental::serialized_size(&payload).ok()
                        .and_then(|bytes| bytes.checked_add(export.image.len()));
                    let sources = historical_capture_sources.get(&export_cut).cloned();
                    if let Some(bytes) = bytes && let Some(sources) = sources
                        && historical_chunk_cuts.retain(chunk, export_cut, Arc::new(export), bytes, sources).is_ok()
                    {
                        pending_historical_captures.remove(&(chunk, export_cut));
                    } else {
                        deferred_chunk_exports.push_back((chunk, respond));
                    }
                } else {
                    if let ChunkExportReply::Checkpoint { request, .. } = &respond {
                        if !primary_checkpoint_inflight.contains_key(request) { continue; }
                        export.retain_environment_roots(primary_checkpoint_roots.entry(*request).or_default());
                    }
                    respond.send(export);
                }
            }
            historical_capture_sources.retain(|cut, _| pending_historical_captures.iter().any(|(_, pending)| pending == cut));
            pending_remote_avatar_retirements.retain(|lease| {
                if !canonical_graphs.retire_player_avatar_admission(*lease) { return true; }
                if actors.remote_lease(lease.gid) == Some(*lease) {
                    actors.remove(lease.gid);
                    living.remove(pumpkin_cluster::actor::ActorId::Player(lease.gid));
                }
                false
            });
            pending_inventory_retirements.retain(|lease| {
                let pending_local = canonical_pending.values().any(|entry| {
                    entry.action.player_leases.contains(lease)
                });
                let pending_graph = canonical_graphs.actor_action_refs(pumpkin_cluster::actor::ActorId::Player(lease.gid))
                    .filter_map(|reference| canonical_graphs.ticks.get(&reference.tick)?.actions.get(&reference.id))
                    .any(|node| node.action.player_leases.contains(lease));
                if pending_local || pending_graph {
                    true
                } else {
                    if !canonical_graphs.retire_player_inventory_lease(*lease) {
                        return true;
                    }
                    if !canonical_graphs.retire_player_avatar_admission(*lease) {
                        return true;
                    }
                    actors.remove(lease.gid);
                    living.remove(pumpkin_cluster::actor::ActorId::Player(lease.gid));
                    inventory.retire_player_after_actor(&server, *lease);
                    false
                }
            });
            pending_replica_boundaries.retain(|gid, (snapshot, generation)| {
                if server
                    .cluster_peer_generations
                    .load()
                    .get(&gid.server)
                    .copied()
                    != Some(*generation)
                    || boundary_leaves
                        .get(gid)
                        .is_some_and(|frontier| {
                            frontier.generation == *generation
                                && (snapshot.movement.incarnation < frontier.incarnation
                                    || (snapshot.movement.incarnation == frontier.incarnation
                                        && frontier.leave.is_some_and(|leave| {
                                            !boundary_after(
                                                snapshot.movement.tick,
                                                snapshot.movement.seq,
                                                leave,
                                            )
                                        })))
                        })
                {
                    return false;
                }
                let outcome = super::cluster_ghost::apply_remote_player_boundary(
                    &server,
                    snapshot.clone(),
                );
                if outcome == super::cluster_ghost::BoundaryApplyOutcome::Applied {
                    #[cfg(feature = "cluster-e2e")]
                    tracing::info!(?gid, "cluster e2e remote player boundary applied after dependency arrived");
                    false
                } else if outcome == super::cluster_ghost::BoundaryApplyOutcome::StaleIncarnation {
                    false
                } else {
                    if replica_boundary_diagnostics.insert((*gid, outcome)) {
                        #[cfg(feature = "cluster-e2e")]
                        tracing::info!(?gid, ?outcome, "cluster e2e remote player boundary pending");
                    }
                    true
                }
            });
            let mut waiting = Vec::new();
            for mut pending in pending_drains.drain(..) {
                if inventory.player_lease_for_gid(pending.request.gid) != Some(pending.request.lease)
                    || actors.local_client_entity_id(pending.request.lease, pending.request.install_generation).is_none()
                {
                    let _ = pending.request.respond.send(ActorDrainStatus::StalePlayer);
                    continue;
                }
                if !pending.leave_applied {
                    pending.leave_applied = statistics
                        .leave_game_status(pending.request.gid)
                        .is_some_and(|applied| {
                            applied
                                || statistics.apply(
                                    super::cluster_actor_statistics::StatisticDelta {
                                        actor: pending.request.gid,
                                        category: pumpkin_data::statistic::StatisticCategory::Custom as i32,
                                        stat: pumpkin_data::statistic::CustomStatistic::LeaveGame as i32,
                                        amount: 1,
                                        source: super::cluster_actor_statistics::StatisticSource::LeaveGame,
                                    },
                                )
                        });
                    statistics.publish();
                }
                if pending.leave_applied
                    && !statistics.has_pending(pending.request.gid)
                    && !pending_controller_statistics
                        .iter()
                        .any(|work| work.lease.gid == pending.request.gid)
                    && pending.ready(
                        last_local_source_tick,
                        resolution_tick,
                        &local_finishes,
                        &buckets,
                        &canonical_graphs,
                        inventory.player_lease_for_gid(pending.request.gid),
                    )
                    && !pending_player_admin
                        .iter()
                        .any(|resolution| resolution.update.target == pending.request.gid)
                {
                    let lease = pending.request.lease;
                    let player_inventory = inventory.phase_snapshot(pumpkin_cluster::canonical_action::TruthView::Ground,
                        pumpkin_cluster::inventory::InventoryTarget::LeasedPlayer { key: lease.key.0, player: lease.gid, incarnation: lease.incarnation });
                    let ender_inventory = inventory.phase_snapshot(pumpkin_cluster::canonical_action::TruthView::Ground,
                        pumpkin_cluster::inventory::InventoryTarget::LeasedVirtual { host: lease.gid.server, key: lease.key.0, player: lease.gid, incarnation: lease.incarnation, session: 0, kind: 0x100 });
                    if let (Some(player_inventory), Some(ender_inventory)) = (player_inventory, ender_inventory) {
                        let _ = pending.request.respond.send(ActorDrainStatus::Ready { player_inventory, ender_inventory });
                    } else { waiting.push(pending); }
                } else {
                    waiting.push(pending);
                }
            }
            pending_drains = waiting;
            publish_primary_unsettled_chunks(
                &server,
                &chunk_blocks,
                &mut buckets,
                &canonical_pending,
                &canonical_graphs,
                &mut unsettled_primary_retention,
            );
            let pending = buckets.pending_chunks();
            let mut pending_ticks: Vec<_> = pending.iter().map(|entry| entry.tick).collect();
            pending_ticks.extend(buckets.settlement_ticks());
            pending_ticks.sort_unstable();
            pending_ticks.dedup();
            for diagnostic in acceptance_delays.observe(acceptance_clock.elapsed(), &pending_ticks) {
                let blockers: Vec<_> = pending
                    .iter()
                    .filter(|entry| entry.tick == diagnostic.tick)
                    .flat_map(|entry| entry.blockers.iter())
                    .collect();
                let settlement_actions = buckets.settlement_actions(diagnostic.tick);
                match diagnostic.level {
                    AcceptanceDelayLevel::Warning => tracing::warn!(
                        tick = diagnostic.tick.0,
                        delayed_seconds = diagnostic.delayed_seconds,
                        ?blockers,
                        ?settlement_actions,
                        "cluster action settlement delayed"
                    ),
                    AcceptanceDelayLevel::Error => tracing::error!(
                        tick = diagnostic.tick.0,
                        delayed_seconds = diagnostic.delayed_seconds,
                        ?blockers,
                        ?settlement_actions,
                        "cluster action settlement delayed"
                    ),
                }
            }
            for (tick, outcomes) in command_ground_outcomes {
                if let Err(observation) = server.cluster_player_commands.accepted_tick(tick, outcomes) {
                    command_observations.push_back(observation);
                }
            }
            remote_player_presentation.drain_movement(&server, &mut actors, &living);
            remote_player_presentation.ensure_bindings(&server, &mut actors, &living);
            super::cluster_vehicle_snapshot::publish(&server, &chunk_blocks, &item_spatial, &vehicle_controllers, resolution_tick);
            let mut environment_roots = pumpkin_cluster::world_time::EnvironmentRoots::default();
            for slot in source_actor_snapshots.keys() { environment_roots.pin_source(*slot); }
            for scope in canonical_scopes.values() {
                if let Some(slot) = scope.full_source_slot { environment_roots.pin_source(slot); }
            }
            environment_roots.published_through_slot = canonical_scopes.get(&resolution_tick).and_then(|scope| scope.full_source_slot);
            for slot in realm_completion.retained_source_slots() {
                environment_roots.pin_source(slot);
            }
            for (_, finish) in pending_realm_finishes.values().flatten() {
                environment_roots.pin_source(finish.full_source_slot);
            }
            for action in canonical_graphs.ticks.values().flat_map(|graph| graph.actions.values().map(|node| &node.action))
                .chain(canonical_pending.values().map(|entry| &entry.action)) {
                for slot in action.source_slot_witnesses() { environment_roots.pin_source(slot); }
                for witness in action.world_border_witnesses() { environment_roots.pin_border(witness); }
                for (world, morning) in action.morning_witnesses() { environment_roots.pin_morning(world, morning); }
            }
            for roots in primary_checkpoint_roots.values() {
                environment_roots.source_slots.extend(&roots.source_slots);
                environment_roots.borders.extend(&roots.borders);
                environment_roots.mornings.extend(&roots.mornings);
            }
            for export in historical_chunk_cuts.retained_values() {
                export.retain_environment_roots(&mut environment_roots);
            }
            for frame in chunk_done_archive.retained_frames() {
                for (_, finish) in &frame.completion.finishes { environment_roots.pin_source(finish.full_source_slot); }
                for action in frame.graphs.iter().flat_map(|graph| &graph.nodes).map(|node| &node.action) {
                    for slot in action.source_slot_witnesses() { environment_roots.pin_source(slot); }
                    for witness in action.world_border_witnesses() { environment_roots.pin_border(witness); }
                    for (world, morning) in action.morning_witnesses() { environment_roots.pin_morning(world, morning); }
                }
            }
            let mut original_source_slots = BTreeMap::new();
            for action in canonical_graphs.ticks.values().flat_map(|graph| graph.actions.values().map(|node| &node.action))
                .chain(canonical_pending.values().map(|entry| &entry.action)) {
                let reference = pumpkin_cluster::canonical_action::ActionRef { tick: action.tick, id: action.id };
                if let Some(slot) = realm_completion.action_source_slot(reference)
                    .or_else(|| canonical_scopes.get(&action.tick).and_then(|scope| scope.full_source_slot)) {
                    original_source_slots.insert(reference, slot);
                }
            }
            for completion in historical_chunk_cuts.retained_values().flat_map(|export| {
                std::iter::once(&export.completion).chain(export.entity_history.iter().chain(&export.dependency_history).map(|frame| &frame.completion))
            }).chain(chunk_done_archive.retained_frames().map(|frame| &frame.completion)) {
                let slots = completion.finishes.iter().map(|(source, finish)| ((finish.tick, *source), finish.full_source_slot)).collect::<BTreeMap<_, _>>();
                for action in &completion.actions {
                    if let Some(slot) = slots.get(&(action.reference.tick, action.source)) {
                        match original_source_slots.entry(action.reference) {
                            std::collections::btree_map::Entry::Vacant(entry) => { entry.insert(*slot); }
                            std::collections::btree_map::Entry::Occupied(entry) if entry.get() != slot => {
                                canonical_invalid_ticks.insert(action.reference.tick);
                            }
                            _ => {}
                        }
                    }
                }
            }
            let prior_source_slots = server.cluster_original_source_slots.load();
            for reference in original_source_slots.keys().filter(|reference| !prior_source_slots.contains_key(reference)) {
                if let Some(node) = canonical_graphs.ticks.get_mut(&reference.tick).and_then(|graph| graph.actions.get_mut(&reference.id))
                    && !node.is_ground_applied() {
                    node.stable = false;
                    node.semantic_generation = None;
                    canonical_changed_pending.insert(*reference);
                }
            }
            server.cluster_original_source_slots.store(Arc::new(original_source_slots));
            server.cluster_environment_roots.store(Arc::new(environment_roots));
            actors.publish(&server, &living);
            living.publish(&server);
            let entity_changes = super::cluster_entity_projection::EntityPresentationChanges::capture(&mut chunk_blocks, &mut item_spatial);
            super::cluster_combat_directory::publish(&server, &actors, &living, &chunk_blocks, &item_spatial, &entity_client_ids, resolution_tick);
            remote_player_presentation.publish(&server, &actors, &living, &inventory, &[], resolution_tick);
            entity_presentation.publish(&server, &chunk_blocks, &item_spatial, &mut entity_client_ids, &inventory, &actors, &remote_player_presentation, &canonical_graphs, &entity_changes, &[]);
            super::cluster_combat_directory::publish(&server, &actors, &living, &chunk_blocks, &item_spatial, &entity_client_ids, resolution_tick);
            player_presentation.publish(&server, &[], |contact| {
                actors.local_contact_client_id(contact).or_else(|| {
                    if let pumpkin_cluster::item_contact::ContactIdentity::Player(lease) = &contact.identity {
                        actors.remote_client_entity_id(*lease, contact.owner)
                    } else { entity_client_ids.resolve_contact(contact) }
                })
            }, |gid| living.get(pumpkin_cluster::actor::ActorId::Player(gid)).map(|truth| truth.local.clone()));
            inventory.publish_canonical_merchant_screens(&server, &living, &chunk_blocks);
            for reference in inventory.pending_canonical_closes() {
                let Some(node) = canonical_graphs.ticks.get(&reference.tick)
                    .and_then(|graph| graph.actions.get(&reference.id)) else { continue; };
                if !node.is_ground_applied() { continue; }
                if node.local_generation != Some(node.generation)
                    || node.factual.result.is_some() && node.local_result.is_none() {
                    continue;
                }
                let accepted = node.factual.result.is_some();
                let actual_close = node.factual.result.as_ref().is_some_and(|result| result.writes.iter().any(|write|
                    matches!(&write.after, Some(pumpkin_cluster::canonical_action::Cell {
                        value: pumpkin_cluster::canonical_action::Value::LivingProperty(
                            pumpkin_cluster::living_property::LivingPropertyValue::MenuSession(menu)), .. })
                        if menu.last_close == Some(reference))
                    || matches!(&write.after, Some(pumpkin_cluster::canonical_action::Cell {
                        value: pumpkin_cluster::canonical_action::Value::Living(living), .. })
                        if living.menu.last_close == Some(reference))));
                if inventory.finish_canonical_close(&server, reference, accepted, actual_close).is_err() {
                    continue;
                }
            }
        }
    });
}

fn install_actor_admission(
    actors: &mut super::cluster_world_actor::OwnedActors,
    statistics: &mut super::cluster_actor_statistics::OwnedActorStatistics,
    living: &mut super::cluster_world_actor::OwnedLivingActors,
    inventory: &mut super::cluster_world_inventory::InventoryOwner,
    import: super::cluster_world_actor::ActorAdmissionImport,
) -> super::cluster_world_actor::ActorAdmissionInstallResult {
    let mut outcome = super::cluster_world_actor::ActorAdmissionInstallResult {
        lease: None,
        actor: import.actor,
        key: import.key,
        install_generation: import.install_generation,
        client_entity_id: import.client_entity_id,
        accepted: false,
    };
    let actor = pumpkin_cluster::actor::ActorId::Player(import.actor);
    if import.statistics.actor != import.actor
        || !import.fall_distance.is_finite()
        || import.fall_distance < 0.0
        || !import.living.belongs_to(actor)
        || import.install_generation == 0
        || import.living.food.actor != actor
        || import.living.combat.actor != actor
        || living.contains(actor)
        || !statistics.can_import(import.actor)
        || !actors.can_admit_local_candidate(
            import.actor,
            import.key,
            import.install_generation,
            import.client_entity_id,
        )
    {
        inventory.discard_prepared_candidate(import.actor, import.key, import.install_generation);
        import.completion.send(outcome);
        return outcome;
    }
    let initial_statistics = import
        .statistics
        .values
        .iter()
        .filter_map(|((category, key), value)| {
            Some((
                (u16::try_from(*category).ok()?, u32::try_from(*key).ok()?),
                u32::try_from(*value).ok()?,
            ))
        })
        .collect();
    {
        assert!(statistics.import(import.statistics));
        assert!(actors.admit_local(import.actor, import.key));
        if let Some(incarnation) = statistics.incarnation(import.actor) {
            let lease = pumpkin_cluster::canonical_action::LivePlayerLease {
                key: pumpkin_cluster::canonical_action::PlayerKey(import.key),
                gid: import.actor,
                incarnation,
            };
            outcome.lease = Some(lease);
            inventory.bind_player_incarnation(import.actor, incarnation);
            assert!(actors.bind_local_client_entity(
                pumpkin_cluster::canonical_action::LivePlayerLease {
                    key: pumpkin_cluster::canonical_action::PlayerKey(import.key),
                    gid: import.actor,
                    incarnation,
                },
                import.install_generation,
                import.client_entity_id,
            ));
        }
        actors.stage_initial_fall_distance(import.actor, import.fall_distance);
        living.stage_initial_presentation(actor, import.presentation);
        assert!(living.stage_initial_fall_distance(actor, import.fall_distance));
        living.stage_initial_experience(actor, import.experience);
        living.stage_initial_statistics(actor, initial_statistics);
        assert!(living.install(actor, import.living).is_ok());
        living.mark_prepared(actor);
        outcome.accepted = true;
    }
    import.completion.send(outcome);
    outcome
}

fn holders(server: &Server, chunk: ChunkKey) -> Vec<u16> {
    let mut holders = cluster_datagram::holders_for_chunk(chunk);
    if server
        .cluster_owned_chunk_blocks
        .load()
        .contains_key(&chunk)
    {
        holders.push(server.advanced_config.cluster.server_id);
        holders.sort_unstable();
        holders.dedup();
    }
    holders
}

fn input_precedes(
    previous: Option<pumpkin_cluster::identity::PlayerSeq>,
    current: Option<pumpkin_cluster::identity::PlayerSeq>,
    previous_family: u8,
    current_family: u8,
) -> bool {
    match (previous, current) {
        (Some(previous), Some(current)) => current.is_newer_than(previous),
        (None, Some(_)) => true,
        (Some(_), None) => false,
        (None, None) => previous_family < current_family,
    }
}

fn ingest_decoded_batch(
    server: &Server,
    scope: Option<&OwnerTickScope>,
    settlement: &mut super::cluster_world_settlement::SettlementOwner,
    buckets: &mut WorldTickBuckets,
    rejected_legacy_admin_peers: &mut HashSet<ServerId>,
    local: ServerId,
    pending: PendingInboundBatch,
) -> Vec<pumpkin_cluster::canonical_action::Action> {
    let source = pending.source;
    if server.cluster_peer_generations.load().get(&source.server) != Some(&source.generation) {
        return Vec::new();
    }
    let batch = pending.batch;
    if !batch.administrative.is_empty() {
        if rejected_legacy_admin_peers.insert(source.server) {
            tracing::error!(
                peer = source.server.0,
                "legacy administrative action received after canonical cutover"
            );
        }
        return Vec::new();
    }
    settlement.register_local(server, buckets, &batch);
    buckets.push_authenticated_batch(
        source.server,
        source.generation,
        local,
        batch,
        |chunk| {
            scope.map_or_else(
                || holders(server, chunk),
                |scope| scope.legacy_holders(chunk),
            )
        },
        |entity| super::cluster_entity_apply::current_entity_owner(&server, entity),
    )
}

fn local_legacy_generation(server: &Server, local: ServerId) -> u64 {
    server
        .cluster_peer_incarnations
        .load()
        .get(&local)
        .copied()
        .unwrap_or_default()
}

fn legacy_chunk_range_closed(
    buckets: &WorldTickBuckets,
    scopes: &BTreeMap<TickStamp, OwnerTickScope>,
    after: TickStamp,
    through: TickStamp,
    chunk: ChunkKey,
) -> bool {
    let span = through.distance_since(after);
    if span == 0 || span >= 32_768 {
        return false;
    }
    (1..=span).all(|offset| {
        let tick = TickStamp(after.0.wrapping_add(offset));
        scopes
            .get(&tick)
            .and_then(OwnerTickScope::legacy_producers)
            .is_some_and(|producers| buckets.legacy_chunk_closed(tick, chunk, &producers))
    })
}

fn advance_graph_empty_chunk_frontiers(
    graphs: &mut pumpkin_cluster::action_graph::ActionGraphs,
    completion: &mut RealmCompletionLedger,
    buckets: &WorldTickBuckets,
    scopes: &BTreeMap<TickStamp, OwnerTickScope>,
    through: TickStamp,
    chunks: &BTreeSet<ChunkKey>,
    blocked_legacy: &HashSet<ChunkKey>,
) {
    let mut remaining = 32;
    for chunk in chunks {
        if blocked_legacy.contains(chunk) {
            continue;
        }
        while remaining > 0 {
            let Some(frontier) = graphs.chunk_frontiers.get(chunk) else {
                break;
            };
            let next = TickStamp(frontier.settled_through.0.wrapping_add(1));
            if next == through || !through.is_after(next) {
                break;
            }
            let Some(scope) = scopes.get(&next) else {
                break;
            };
            let Ok(holders) = scope.chunk_holders(*chunk) else {
                break;
            };
            let producers = scope.producers.clone();
            let Some(legacy_producers) = scope.legacy_producers() else {
                break;
            };
            if completion
                .capture_chunk(next, *chunk, holders.clone(), producers.clone())
                .is_err()
                || !completion.chunk_producer_cut_complete(next, *chunk, &holders, &producers)
                || !buckets.legacy_chunk_closed(next, *chunk, &legacy_producers)
                || !graphs.advance_empty_chunk_frontier(next, *chunk, true)
            {
                break;
            }
            remaining -= 1;
        }
    }
}

fn advance_graph_empty_player_frontiers(
    graphs: &mut pumpkin_cluster::action_graph::ActionGraphs,
    completion: &mut RealmCompletionLedger,
    buckets: &WorldTickBuckets,
    scopes: &BTreeMap<TickStamp, OwnerTickScope>,
    through: TickStamp,
    keys: &BTreeSet<(
        pumpkin_cluster::canonical_action::PlayerKey,
        pumpkin_cluster::action_graph::InventoryRealmId,
        GlobalPlayerId,
        u64,
    )>,
    blocked_legacy: &HashSet<ChunkKey>,
) {
    use pumpkin_cluster::canonical_action::Stakeholder;

    let mut remaining = 32;
    for &key in keys {
        while remaining > 0 {
            let Some(frontier) = graphs.player_inventory_frontiers.get(&key).copied() else {
                break;
            };
            let next = TickStamp(frontier.settled_through.0.wrapping_add(1));
            if next == through
                || !through.is_after(next)
                || blocked_legacy.contains(&frontier.accepted_chunk)
            {
                break;
            }
            let Some(scope) = scopes.get(&next) else {
                break;
            };
            if scope.players.get(&frontier.lease) != Some(&frontier.accepted_chunk) {
                break;
            }
            let Ok(owner) = scope.source(ServerId(frontier.lease.gid.server.0)) else {
                break;
            };
            let Ok(mut holders) = scope.chunk_holders(frontier.accepted_chunk) else {
                break;
            };
            holders.insert(owner);
            let producers = scope.producers.clone();
            let Some(legacy_producers) = scope.legacy_producers() else {
                break;
            };
            let requirement = RequirementKey {
                stakeholder: Stakeholder::Player(key.0),
                domain: HolderDomain::LiveInventory {
                    actor_chunk: frontier.accepted_chunk,
                    owner: owner.server,
                    epoch: frontier.domain_epoch,
                },
            };
            let mut captured = completion.clone();
            if captured
                .capture_player_lease(next, frontier.lease, owner)
                .and_then(|()| {
                    captured.capture_player_chunk(next, frontier.lease.key, frontier.accepted_chunk)
                })
                .and_then(|()| {
                    captured.capture_with_producers(
                        next,
                        requirement,
                        holders.clone(),
                        producers.clone(),
                    )
                })
                .is_err()
                || !captured.requirement_producer_cut_complete(
                    next,
                    requirement,
                    &holders,
                    &producers,
                )
                || !buckets.legacy_chunk_closed(next, frontier.accepted_chunk, &legacy_producers)
                || !graphs.advance_empty_player_inventory_frontier(next, key, true)
            {
                break;
            }
            *completion = captured;
            remaining -= 1;
        }
    }
}

fn physical_player_avatar_absent(
    actors: &super::cluster_world_actor::OwnedActors,
    living: &super::cluster_world_actor::OwnedLivingActors,
    target: Target,
) -> bool {
    let Target::Chunk {
        key,
        part: pumpkin_cluster::canonical_action::ChunkPart::Player(gid),
        inventory: None,
    } = target
    else {
        return false;
    };
    let spatial_present = actors
        .get(gid)
        .is_some_and(|truth| truth.ground.chunk == key);
    let living_present_or_invalid = living
        .get(pumpkin_cluster::actor::ActorId::Player(gid))
        .and_then(|truth| truth.ground.avatar.as_ref())
        .is_some_and(|avatar| {
            let position = avatar.position.map(f64::from_bits);
            !position.iter().all(|coordinate| coordinate.is_finite())
                || avatar.world == key.world
                    && (position[0].floor() as i32).div_euclid(16) == key.x()
                    && (position[2].floor() as i32).div_euclid(16) == key.z()
        });
    !spatial_present && !living_present_or_invalid
}

fn apply_ready(
    server: &Arc<Server>,
    chunk_blocks: &mut super::cluster_chunk_state::OwnedChunkStore,
    actors: &mut super::cluster_world_actor::OwnedActors,
    living: &mut super::cluster_world_actor::OwnedLivingActors,
    inventory: &mut super::cluster_world_inventory::InventoryOwner,
    item_actions: &mut super::cluster_world_item_actions::ItemActionOwner,
    combat: &mut super::cluster_combat_apply::CombatApplyState,
    pending_boundaries: &mut HashMap<PlayerBoundaryKey, (PlayerBoundaryUpdate, u64)>,
    promoted_boundaries: &mut HashMap<PlayerBoundaryKey, bool>,
    pending_replica_boundaries: &mut HashMap<GlobalPlayerId, PendingReplicaBoundary>,
    boundary_leaves: &mut HashMap<GlobalPlayerId, BoundaryFrontier>,
    replica_boundary_diagnostics: &mut HashSet<(
        GlobalPlayerId,
        super::cluster_ghost::BoundaryApplyOutcome,
    )>,
    settlement: &mut super::cluster_world_settlement::SettlementOwner,
    inventory_capture_barrier: &mut InventoryCaptureBarrier,
    pending_player_admin: &mut VecDeque<
        super::cluster_player_admin::PlayerAdministrativeResolution,
    >,
    ready: &ReadyPromotion,
) -> bool {
    let mut remaining_transitions = ready
        .batches
        .iter()
        .flat_map(|sourced| &sourced.batch.player_chunk_transition)
        .copied()
        .collect::<Vec<_>>();
    let transition_players = remaining_transitions
        .iter()
        .map(|update| update.gid)
        .collect::<BTreeSet<_>>();
    let mut staged_actors = actors.clone_player_subset(&transition_players);
    let mut staged_living = living.clone_player_subset(&transition_players);
    let mut staged_inventory = None;
    let mut accepted_transitions = Vec::new();
    while !remaining_transitions.is_empty() {
        remaining_transitions.sort_by_key(|update| {
            let prior = staged_actors
                .get(update.gid)
                .map_or(0, |state| state.ground.frame.effective.source_seq.0);
            (update.gid.pack_u32(), update.seq.0.wrapping_sub(prior))
        });
        let mut selected = None;
        for (index, update) in remaining_transitions.iter().enumerate() {
            if super::cluster_world_actor::OwnedActors::validate_accepted_transition(update)
                .is_err()
            {
                continue;
            }
            if staged_actors.get(update.gid).is_some_and(|truth| {
                truth.ground.chunk == update.to
                    && inventory
                        .player_lease_for_gid(update.gid)
                        .is_some_and(|lease| lease.incarnation == update.incarnation)
                    && matches!(
                        truth.ground.source,
                        pumpkin_cluster::controller::ControllerActorSource::AcceptedAdministrative(
                            _, accepted_tick
                        ) if accepted_tick.is_after(update.tick)
                    )
                    && !update
                        .seq
                        .is_newer_than(truth.ground.frame.effective.source_seq)
            }) {
                if inventory.accepted_player_inventory_at(
                    staged_inventory.as_ref(),
                    update.gid,
                    update.to,
                ) {
                    selected = Some((index, None, staged_inventory.clone(), false));
                    break;
                }
                if let Ok(Some(relocation)) = inventory.prepare_accepted_player_transition(
                    staged_inventory.clone(),
                    update.gid,
                    update.incarnation,
                    update.from,
                    update.to,
                    staged_actors.get(update.gid).map(|state| state.local.chunk),
                ) {
                    selected = Some((index, None, Some(relocation), false));
                    break;
                }
            }
            let position = update.snapshot.movement.pos;
            let block = pumpkin_cluster::protocol::BlockPos {
                x: position[0].floor() as i32,
                y: position[1].floor() as i32,
                z: position[2].floor() as i32,
            };
            let spatial = staged_actors.prepare_accepted_transition(
                update,
                chunk_blocks.rough_ground_biome_id(update.to, block),
            );
            let avatar = staged_living.prepare_accepted_transition(update);
            let relocation = inventory.prepare_accepted_player_transition(
                staged_inventory.clone(),
                update.gid,
                update.incarnation,
                update.from,
                update.to,
                staged_actors.get(update.gid).map(|state| state.local.chunk),
            );
            match (spatial, avatar, relocation) {
                (Ok(Some(spatial)), Ok(Some(avatar)), Ok(Some(relocation))) => {
                    selected = Some((index, Some((spatial, avatar)), Some(relocation), true));
                    break;
                }
                (Ok(None), Ok(None), Ok(relocation)) => {
                    selected = Some((
                        index,
                        None,
                        relocation.or_else(|| staged_inventory.clone()),
                        true,
                    ));
                    break;
                }
                _ => {}
            }
        }
        let Some((index, actor, relocation, publish_boundary)) = selected else {
            return false;
        };
        let update = remaining_transitions.remove(index);
        if let Some((spatial, avatar)) = actor {
            let prepared_spatial = staged_actors
                .prepare_replacement(true, vec![(update.gid, spatial)])
                .expect("accepted transition spatial staging changed");
            let prepared_avatar = staged_living
                .prepare_replacement(
                    true,
                    vec![(pumpkin_cluster::actor::ActorId::Player(update.gid), avatar)],
                )
                .expect("accepted transition living staging changed");
            staged_actors.install_replacement(prepared_spatial);
            staged_living.install_replacement(prepared_avatar);
            let local_may_follow = staged_actors.get(update.gid).is_some_and(|truth| {
                !truth
                    .local
                    .frame
                    .effective
                    .source_seq
                    .is_newer_than(spatial.frame.effective.source_seq)
            });
            if local_may_follow {
                let prepared_local = staged_actors
                    .prepare_replacement(false, vec![(update.gid, spatial)])
                    .expect("accepted transition local staging changed");
                staged_actors.install_replacement(prepared_local);
                assert!(staged_living.project_player_spatial(
                    update.gid,
                    spatial,
                    pumpkin_cluster::canonical_action::SpatialEpoch::Action(
                        pumpkin_cluster::canonical_action::ActionRef {
                            tick: update.tick,
                            id: pumpkin_cluster::canonical_action::ActionId {
                                actor: pumpkin_cluster::actor::ActorId::Player(update.gid),
                                incarnation: update.incarnation,
                                sequence: update.seq.0,
                                family: 8
                            },
                        }
                    )
                ));
            }
        }
        staged_inventory = relocation;
        accepted_transitions.push((update, publish_boundary));
    }
    let completed = true;
    let mut mixed: Vec<_> = ready
        .batches
        .iter()
        .flat_map(|sourced| {
            sourced
                .batch
                .inv_ops
                .iter()
                .cloned()
                .map(pumpkin_cluster::item_action::InventoryItemAction::Inventory)
                .chain(
                    sourced
                        .batch
                        .item_action
                        .iter()
                        .cloned()
                        .map(pumpkin_cluster::item_action::InventoryItemAction::Item),
                )
        })
        .collect();
    pumpkin_cluster::item_action::sort_inventory_item_actions(
        super::cluster_world_apply::CLUSTER_SEED,
        ready.tick,
        &mut mixed,
    );
    for action in mixed {
        match action {
            pumpkin_cluster::item_action::InventoryItemAction::Inventory(action) => {
                let key = ActionKey::inventory(&action);
                inventory.apply_one(server, ready.tick, action);
                inventory_capture_barrier.resolve(key);
            }
            pumpkin_cluster::item_action::InventoryItemAction::Item(action) => {
                let origin = item_actions.take_origin(&action);
                let mut reason =
                    validate_item_action(server, &chunk_blocks, inventory.ledger(), &action)
                        .and_then(|()| {
                            super::cluster_world_item_actions::preflight_effects(
                                &chunk_blocks,
                                &action,
                            )
                        })
                        .and_then(|()| item_actions.preflight_use(&action))
                        .err();
                let result = if reason.is_none() && inventory.preview_item_source(&action).is_ok() {
                    match super::cluster_world_item_actions::apply_effects(&action) {
                        Ok(()) => match inventory.apply_item_source(&action) {
                            Ok(delta) => Ok(delta),
                            Err(error) => Err(error),
                        },
                        Err(effect_error) => {
                            reason = Some(effect_error);
                            Err(pumpkin_cluster::inventory::InventoryApplyError::Rejected)
                        }
                    }
                } else {
                    Err(pumpkin_cluster::inventory::InventoryApplyError::Rejected)
                };
                let publication = super::cluster_world_item_actions::ItemActionPublication {
                    action,
                    accepted: result.is_ok(),
                    origin,
                };
                item_actions.settle_use(&publication.action, publication.accepted);
                let _ =
                    item_actions.publish_origin(server, inventory.ledger(), publication, result);
            }
        }
    }
    for (target, state, handle) in inventory.take_beacon_publications() {
        let Some(beacon) = handle
            .as_any()
            .downcast_ref::<crate::block::entities::beacon::BeaconBlockEntity>()
        else {
            tracing::error!(
                ?target,
                "accepted beacon handle has the wrong block entity type"
            );
            continue;
        };
        beacon.set_data(
            crate::block::entities::beacon::BeaconBlockEntity::DATA_PRIMARY,
            state.primary_effect.map_or(-1, i32::from),
        );
        beacon.set_data(
            crate::block::entities::beacon::BeaconBlockEntity::DATA_SECONDARY,
            state.secondary_effect.map_or(-1, i32::from),
        );
        let payment = inventory
            .ledger()
            .snapshot(target)
            .and_then(|snapshot| {
                snapshot.cells.into_iter().find(|cell| {
                    cell.loc
                        == pumpkin_cluster::inventory::InvLoc::new(
                            pumpkin_cluster::inventory::INV_MAIN,
                            0,
                        )
                })
            })
            .map_or_else(
                || Ok(pumpkin_data::item_stack::ItemStack::EMPTY.clone()),
                |cell| super::cluster_invsee::item_stack_from_inventory_cell(&cell),
            );
        match payment {
            Ok(payment) => handle.set_stack(0, payment),
            Err(error) => tracing::error!(
                ?target,
                ?error,
                "accepted beacon payment could not be projected"
            ),
        }
    }
    for sourced in &ready.batches {
        let batch = &sourced.batch;
        super::cluster_combat_apply::apply_resolved_batch(server, combat, batch);
        for update in &batch.player_administrative {
            if update.target.server.0 != server.advanced_config.cluster.server_id {
                continue;
            }
            let key = ActionKey::player_administrative(update);
            let resolution = super::cluster_player_admin::PlayerAdministrativeResolution {
                action: key,
                update: update.clone(),
                accepted: true,
                holders: ready.holders.iter().copied().collect(),
            };
            if !pending_player_admin.is_empty() {
                pending_player_admin.push_back(resolution);
            } else if let Err(super::cluster_player_admin::PlayerAdministrativeQueueError::Full(
                resolution,
            )) = server.cluster_player_admin.queue_resolution(
                resolution.action,
                resolution.update.clone(),
                resolution.accepted,
                resolution.holders.clone(),
            ) {
                pending_player_admin.push_back(resolution);
            }
            assert!(pending_player_admin.len() <= 4_096);
        }
    }
    let spatial_changes = transition_players
        .iter()
        .filter_map(|gid| staged_actors.get(*gid).map(|truth| (*gid, truth.ground)))
        .collect();
    let living_changes = transition_players
        .iter()
        .filter_map(|gid| {
            let actor = pumpkin_cluster::actor::ActorId::Player(*gid);
            let staged = staged_living.get(actor)?.ground.avatar.as_ref()?;
            let mut current = living.get(actor)?.ground.clone();
            let avatar = current.avatar.as_mut()?;
            avatar.world = staged.world;
            avatar.position = staged.position;
            avatar.yaw = staged.yaw;
            avatar.pitch = staged.pitch;
            current.combat.velocity_micro = staged_living.get(actor)?.ground.combat.velocity_micro;
            Some((actor, current))
        })
        .collect();
    let prepared_spatial = actors
        .prepare_replacement(true, spatial_changes)
        .expect("accepted transition spatial owner changed within owner turn");
    let prepared_living = living
        .prepare_replacement(true, living_changes)
        .expect("accepted transition living owner changed within owner turn");
    actors.install_replacement(prepared_spatial);
    living.install_replacement(prepared_living);
    if let Some(relocation) = staged_inventory {
        inventory.commit_accepted_player_transition(server, relocation);
    }
    for (update, publish_boundary) in accepted_transitions {
        let key = (
            update.gid,
            update.incarnation,
            update.tick,
            update.seq.0,
            update.from,
            update.to,
        );
        if let Some((boundary, generation)) = pending_boundaries.remove(&key) {
            if publish_boundary
                && server
                    .cluster_peer_generations
                    .load()
                    .get(&update.gid.server)
                    .copied()
                    == Some(generation)
            {
                apply_boundary(
                    server,
                    pending_replica_boundaries,
                    boundary_leaves,
                    replica_boundary_diagnostics,
                    boundary,
                    generation,
                );
            }
            promoted_boundaries.insert(key, false);
        } else {
            promoted_boundaries.insert(key, publish_boundary);
        }
        if promoted_boundaries.len() > 4_096 {
            let oldest = promoted_boundaries
                .keys()
                .max_by_key(|(_, _, tick, _, _, _)| ready.tick.distance_since(*tick))
                .copied();
            if let Some(oldest) = oldest {
                promoted_boundaries.remove(&oldest);
            }
        }
    }
    if completed {
        settlement.promoted(server, ready);
    }
    completed
}

fn ingest_boundary(
    server: &Arc<Server>,
    pending: &mut HashMap<PlayerBoundaryKey, (PlayerBoundaryUpdate, u64)>,
    promoted: &mut HashMap<PlayerBoundaryKey, bool>,
    pending_replica_boundaries: &mut HashMap<GlobalPlayerId, PendingReplicaBoundary>,
    boundary_leaves: &mut HashMap<GlobalPlayerId, BoundaryFrontier>,
    replica_boundary_diagnostics: &mut HashSet<(
        GlobalPlayerId,
        super::cluster_ghost::BoundaryApplyOutcome,
    )>,
    parcel: InboundParcel,
) {
    let Ok(boundary) = pumpkin_cluster::boundary::decode(&parcel.bytes) else {
        return;
    };
    let gid = match &boundary {
        PlayerBoundaryUpdate::Enter(snapshot) => snapshot.movement.gid,
        PlayerBoundaryUpdate::Leave { gid, .. } | PlayerBoundaryUpdate::Transition { gid, .. } => {
            *gid
        }
    };
    let incarnation = match &boundary {
        PlayerBoundaryUpdate::Enter(snapshot) => snapshot.movement.incarnation,
        PlayerBoundaryUpdate::Leave { incarnation, .. }
        | PlayerBoundaryUpdate::Transition { incarnation, .. } => *incarnation,
    };
    let embedded_gid_matches = match &boundary {
        PlayerBoundaryUpdate::Transition {
            incarnation,
            seq,
            tick,
            to,
            snapshot: Some(snapshot),
            ..
        } => {
            let movement = snapshot.movement;
            movement.gid == gid
                && movement.incarnation == *incarnation
                && movement.seq == *seq
                && movement.tick == *tick
                && ChunkKey::new(
                    movement.world,
                    (movement.pos[0].floor() as i32).div_euclid(16),
                    (movement.pos[2].floor() as i32).div_euclid(16),
                ) == *to
        }
        _ => true,
    };
    if gid.server != parcel.peer
        || incarnation == 0
        || !embedded_gid_matches
        || parcel.header.player != Some(gid)
        || server
            .cluster_peer_generations
            .load()
            .get(&parcel.peer)
            .copied()
            != Some(parcel.generation)
    {
        if replica_boundary_diagnostics.insert((
            gid,
            super::cluster_ghost::BoundaryApplyOutcome::UnauthorizedSource,
        )) {
            tracing::error!(
                peer = parcel.peer.0,
                ?gid,
                generation = parcel.generation,
                "cluster player boundary source rejected"
            );
        }
        return;
    }
    if boundary_leaves.get(&gid).is_some_and(|frontier| {
        frontier.generation == parcel.generation && incarnation < frontier.incarnation
    }) {
        return;
    }
    if boundary_leaves.get(&gid).is_none_or(|frontier| {
        frontier.generation != parcel.generation || frontier.incarnation < incarnation
    }) {
        boundary_leaves.insert(
            gid,
            BoundaryFrontier {
                generation: parcel.generation,
                incarnation,
                leave: None,
                latest_snapshot: None,
            },
        );
        pending.retain(|(player, known_incarnation, _, _, _, _), _| {
            *player != gid || *known_incarnation == incarnation
        });
        promoted.retain(|(player, known_incarnation, _, _, _, _), _| {
            *player != gid || *known_incarnation == incarnation
        });
        pending_replica_boundaries.retain(|player, (snapshot, _)| {
            *player != gid || snapshot.movement.incarnation == incarnation
        });
    }
    match boundary {
        PlayerBoundaryUpdate::Transition {
            gid,
            incarnation,
            seq,
            tick,
            from,
            to,
            ..
        } => {
            let key = (gid, incarnation, tick, seq.0, from, to);
            if let Some(should_publish) = promoted.get_mut(&key) {
                if *should_publish {
                    apply_boundary(
                        server,
                        pending_replica_boundaries,
                        boundary_leaves,
                        replica_boundary_diagnostics,
                        boundary,
                        parcel.generation,
                    );
                    *should_publish = false;
                }
            } else {
                pending.insert(key, (boundary, parcel.generation));
            }
        }
        _ => apply_boundary(
            server,
            pending_replica_boundaries,
            boundary_leaves,
            replica_boundary_diagnostics,
            boundary,
            parcel.generation,
        ),
    }
}

fn apply_boundary(
    server: &Arc<Server>,
    pending: &mut HashMap<GlobalPlayerId, PendingReplicaBoundary>,
    boundary_leaves: &mut HashMap<GlobalPlayerId, BoundaryFrontier>,
    diagnostics: &mut HashSet<(GlobalPlayerId, super::cluster_ghost::BoundaryApplyOutcome)>,
    boundary: PlayerBoundaryUpdate,
    source_generation: u64,
) {
    match boundary {
        PlayerBoundaryUpdate::Enter(snapshot)
        | PlayerBoundaryUpdate::Transition {
            snapshot: Some(snapshot),
            ..
        } => {
            let gid = snapshot.movement.gid;
            if boundary_leaves.get(&gid).is_some_and(|frontier| {
                frontier.generation == source_generation
                    && (snapshot.movement.incarnation < frontier.incarnation
                        || (snapshot.movement.incarnation == frontier.incarnation
                            && frontier.leave.is_some_and(|leave| {
                                !boundary_after(
                                    snapshot.movement.tick,
                                    snapshot.movement.seq,
                                    leave,
                                )
                            })))
            }) {
                return;
            }
            if pending.get(&gid).is_some_and(|(current, _)| {
                current.movement.incarnation > snapshot.movement.incarnation
            }) {
                return;
            }
            if boundary_leaves.get(&gid).is_some_and(|frontier| {
                frontier.generation == source_generation
                    && frontier.incarnation == snapshot.movement.incarnation
                    && frontier.latest_snapshot.is_some_and(|latest| {
                        boundary_after(
                            latest.0,
                            latest.1,
                            (snapshot.movement.tick, snapshot.movement.seq),
                        )
                    })
            }) {
                return;
            }
            if let Err(reason) = super::cluster_world_actor::OwnedActors::validate_remote(
                snapshot.movement,
                snapshot.visual,
            ) {
                if diagnostics.insert((
                    gid,
                    super::cluster_ghost::BoundaryApplyOutcome::InvalidSpatialState,
                )) {
                    tracing::error!(
                        ?gid,
                        ?reason,
                        "cluster player boundary spatial state rejected"
                    );
                }
                return;
            }
            if let Some(frontier) = boundary_leaves.get_mut(&gid)
                && frontier.generation == source_generation
                && frontier.incarnation == snapshot.movement.incarnation
            {
                frontier.latest_snapshot = Some((snapshot.movement.tick, snapshot.movement.seq));
            }
            let outcome =
                super::cluster_ghost::apply_remote_player_boundary(server, snapshot.clone());
            if outcome == super::cluster_ghost::BoundaryApplyOutcome::StaleIncarnation {
                return;
            }
            if outcome != super::cluster_ghost::BoundaryApplyOutcome::Applied {
                if pending.get(&gid).is_none_or(|(current, _)| {
                    current.movement.incarnation <= snapshot.movement.incarnation
                }) {
                    pending.insert(gid, (snapshot, source_generation));
                }
                if diagnostics.insert((gid, outcome)) {
                    #[cfg(feature = "cluster-e2e")]
                    tracing::info!(?gid, ?outcome, "cluster e2e remote player boundary pending");
                }
            } else {
                if pending.get(&gid).is_some_and(|(current, _)| {
                    current.movement.incarnation == snapshot.movement.incarnation
                }) {
                    pending.remove(&gid);
                }
                #[cfg(feature = "cluster-e2e")]
                tracing::info!(?gid, "cluster e2e remote player boundary applied");
            }
        }
        PlayerBoundaryUpdate::Leave {
            gid,
            incarnation,
            seq,
            tick,
            ..
        }
        | PlayerBoundaryUpdate::Transition {
            gid,
            incarnation,
            seq,
            tick,
            snapshot: None,
            ..
        } => {
            let Some(frontier) = boundary_leaves.get_mut(&gid) else {
                return;
            };
            if frontier.generation != source_generation || frontier.incarnation != incarnation {
                return;
            }
            if !frontier.accepts_leave(tick, seq) {
                return;
            }
            frontier.leave = Some((tick, seq));
            if pending.get(&gid).is_some_and(|(current, _)| {
                current.movement.incarnation == incarnation
                    && !boundary_after(current.movement.tick, current.movement.seq, (tick, seq))
            }) {
                pending.remove(&gid);
            }
        }
    }
}

fn world_for(
    server: &Server,
    id: pumpkin_cluster::protocol::WorldId,
) -> Option<Arc<crate::world::World>> {
    server
        .worlds
        .load()
        .iter()
        .find(|world| super::cluster_world_delta::world_id(world) == id)
        .cloned()
}

#[cfg(test)]
mod checkpoint_tests {
    use super::*;

    #[test]
    fn older_disk_completion_does_not_clear_a_newer_chunk_change() {
        let chunk = ChunkKey::new(pumpkin_cluster::protocol::WorldId::new(0, 1), 2, 3);
        let mut dirty = BTreeSet::from([chunk]);
        let mut revisions = HashMap::from([(chunk, 2)]);

        complete_primary_checkpoint(&mut dirty, &mut revisions, vec![(chunk, 1)], true);
        assert!(dirty.contains(&chunk));
        assert_eq!(revisions.get(&chunk), Some(&2));

        complete_primary_checkpoint(&mut dirty, &mut revisions, vec![(chunk, 2)], false);
        assert!(dirty.contains(&chunk));

        complete_primary_checkpoint(&mut dirty, &mut revisions, vec![(chunk, 2)], true);
        assert!(dirty.is_empty());
        assert!(revisions.is_empty());
    }
}

pub(crate) fn detach_departing_watchers(
    old_world: Arc<crate::world::World>,
    old_cylinder: pumpkin_world::cylindrical_chunk_iterator::Cylindrical,
) {
    let Some(server) = old_world.server.upgrade() else {
        return;
    };
    server.spawn_task(async move {
        let chunks = old_cylinder.all_chunks_within().collect::<Vec<_>>();
        let clean = old_world.level.mark_chunks_as_not_watched(&chunks).await;
        let clean = clean
            .into_iter()
            .filter(|chunk| !old_world.level.is_cluster_pinned(chunk))
            .collect::<Vec<_>>();
        if !clean.is_empty() {
            if pumpkin_world::level::is_cluster_secondary() {
                for chunk in &clean {
                    old_world.level.unwant_cluster_chunk(*chunk);
                }
            }
            old_world.remove_entities_in_chunks(&clean).await;
            old_world.level.clean_entity_chunks(&clean);
        }
    });
}

fn effective_item_pose(
    staged: &super::cluster_item_action::LocalItemActionIntent,
) -> Option<pumpkin_cluster::item_action::EffectiveItemPose> {
    let intent = &staged.intent;
    let (frame, eye_height) = staged.effective_frame?;
    if frame.effective.tick != intent.tick {
        return None;
    }
    let effective = frame.effective;
    Some(pumpkin_cluster::item_action::EffectiveItemPose {
        actor: intent.actor,
        tick: intent.tick,
        world: intent.actor_chunk.world,
        position: effective.position,
        eye_position: [
            effective.position[0],
            effective.position[1] + f64::from(eye_height),
            effective.position[2],
        ],
        yaw: effective.yaw,
        pitch: effective.pitch,
        reach: 4.5,
    })
}

fn item_target_snapshot(
    server: &Server,
    intent: &pumpkin_cluster::item_action::ItemActionIntent,
) -> Option<pumpkin_cluster::item_action::ActorItemSnapshot> {
    let pumpkin_cluster::item_action::ItemActionKind::UseOnActor { target, .. } = intent.kind
    else {
        return None;
    };
    match target {
        pumpkin_cluster::actor::ActorId::Player(gid) => {
            let player = server
                .get_all_players()
                .into_iter()
                .find(|player| player.cluster_gid() == Some(gid))?;
            let position = player.living_entity.entity.pos.load();
            Some(pumpkin_cluster::item_action::ActorItemSnapshot {
                actor: target,
                chunk: ChunkKey::new(
                    super::cluster_world_delta::world_id(&player.world()),
                    (position.x.floor() as i32).div_euclid(16),
                    (position.z.floor() as i32).div_euclid(16),
                ),
                position: [position.x, position.y, position.z],
                revision: actor_item_revision(target, [position.x, position.y, position.z]),
            })
        }
        pumpkin_cluster::actor::ActorId::Entity(entity) => server
            .worlds
            .load()
            .iter()
            .flat_map(|world| world.entities.load().iter().cloned().collect::<Vec<_>>())
            .find_map(|candidate| {
                let candidate_id = if let Some(replica) =
                    candidate
                        .cast_any()
                        .downcast_ref::<crate::entity::cluster_entity_replica::RemoteEntityReplica>(
                        ) {
                    replica.cluster_ref.id()
                } else {
                    let inner = candidate.get_entity();
                    pumpkin_cluster::protocol::GlobalEntityId {
                        origin: ServerId(server.advanced_config.cluster.server_id),
                        local_id: inner.entity_id,
                        generation: inner.cluster_generation(),
                    }
                };
                (candidate_id == entity).then(|| {
                    let inner = candidate.get_entity();
                    let position = inner.pos.load();
                    let world = inner.world.load();
                    pumpkin_cluster::item_action::ActorItemSnapshot {
                        actor: target,
                        chunk: ChunkKey::new(
                            super::cluster_world_delta::world_id(&world),
                            (position.x.floor() as i32).div_euclid(16),
                            (position.z.floor() as i32).div_euclid(16),
                        ),
                        position: [position.x, position.y, position.z],
                        revision: actor_item_revision(target, [position.x, position.y, position.z]),
                    }
                })
            }),
        pumpkin_cluster::actor::ActorId::Peer(_) => None,
    }
}

fn actor_item_revision(actor: pumpkin_cluster::actor::ActorId, position: [f64; 3]) -> u64 {
    position.into_iter().fold(
        match actor {
            pumpkin_cluster::actor::ActorId::Player(gid) => {
                (u64::from(gid.server.0) << 16) | u64::from(gid.player.0)
            }
            pumpkin_cluster::actor::ActorId::Entity(entity) => {
                (u64::from(entity.origin.0) << 48)
                    ^ (entity.local_id as u32 as u64)
                    ^ (u64::from(entity.generation) << 16)
            }
            pumpkin_cluster::actor::ActorId::Peer(peer) => u64::from(peer.0) << 48,
        },
        |revision, coordinate| revision.rotate_left(17) ^ coordinate.to_bits(),
    )
}

fn validate_item_action(
    server: &Server,
    blocks: &super::cluster_chunk_state::OwnedChunkStore,
    ledger: &pumpkin_cluster::inventory::InvLedger,
    action: &pumpkin_cluster::item_action::ItemAction,
) -> Result<(), pumpkin_cluster::item_action::ItemActionRejection> {
    let inventory = ledger.snapshot(action.source.target);
    action.validate(
        inventory.as_ref(),
        |world, pos| {
            blocks
                .block(
                    ChunkKey::new(world, pos.x.div_euclid(16), pos.z.div_euclid(16)),
                    pos,
                )
                .ok()
                .map(pumpkin_data::BlockStateId::as_u16)
        },
        |_| {
            item_target_snapshot(
                server,
                &pumpkin_cluster::item_action::ItemActionIntent {
                    actor: action.actor,
                    tick: action.tick,
                    source: action.source,
                    expected_inventory_revision: action.expected_inventory_revision,
                    expected_item: action.expected_item.clone(),
                    actor_chunk: action.actor_chunk,
                    affected: action.affected.clone(),
                    kind: action.kind.clone(),
                    client_claim: action.client_claim.clone(),
                },
            )
        },
    )
}

#[cfg(test)]
mod tests {
    use super::*;
    use pumpkin_cluster::actor::ActorId;
    use pumpkin_cluster::identity::{GlobalPlayerId, PlayerSlot};
    use pumpkin_cluster::protocol::WorldId;

    #[test]
    fn refused_chunk_export_returns_its_reserved_result_without_waiting() {
        let chunk = ChunkKey::new(WorldId::new(0, 7), 0, 0);
        let (route, mut receive) = mpsc::channel(1);
        let respond = route.clone().try_reserve_owned().unwrap();
        let reply = ChunkExportReply::Serve {
            peer: ServerId(2),
            generation: 9,
            chunk,
            requested_cut: None,
            subscription: None,
            route: route.clone(),
            respond: Some(respond),
        };
        assert!(route.try_reserve().is_err());
        drop(reply);
        let result = receive.try_recv().unwrap();
        assert_eq!(result.peer, ServerId(2));
        assert_eq!(result.generation, 9);
        assert_eq!(result.chunk, chunk);
        assert!(result.export.is_none());
        assert!(receive.try_recv().is_err());
        assert!(route.try_reserve().is_ok());
    }

    #[test]
    fn virtual_destination_scope_uses_frozen_remote_holders_without_local_vote() {
        let chunk = ChunkKey::new(WorldId::new(0, 7), 1, 0);
        let local = ServerId(1);
        let remote = PeerGeneration {
            server: ServerId(2),
            generation: 12,
        };
        let mut directory = pumpkin_cluster::chunks::Directory::new();
        directory.apply_advert(pumpkin_cluster::chunks::ChunkAdvert { holder: 2, chunk });
        let scope = OwnerTickScope {
            full_source_slot: Some(0),
            primary_producers: BTreeSet::new(),
            local,
            directory: Arc::new(directory),
            local_chunks: Arc::new(HashMap::new()),
            incarnations: Arc::new(HashMap::from([
                (local, 11),
                (remote.server, remote.generation),
            ])),
            transport_generations: Arc::new(HashMap::from([(remote.server, 3)])),
            producers: BTreeSet::from([
                PeerGeneration {
                    server: local,
                    generation: 11,
                },
                remote,
            ]),
            players: BTreeMap::new(),
            entities: BTreeMap::new(),
        };
        assert!(scope.chunk_holders(chunk).is_err());
        assert_eq!(
            scope.frozen_chunk_holders(chunk).unwrap(),
            BTreeSet::from([remote])
        );
        assert!(
            scope
                .frozen_chunk_holders(ChunkKey::new(chunk.world, 2, 0))
                .is_err()
        );
        let mut replacement = (*scope.directory).clone();
        replacement.apply_drop(pumpkin_cluster::chunks::ChunkDrop { holder: 2, chunk });
        replacement.apply_advert(pumpkin_cluster::chunks::ChunkAdvert { holder: 3, chunk });
        assert_eq!(
            scope.frozen_chunk_holders(chunk).unwrap(),
            BTreeSet::from([remote])
        );
    }

    #[test]
    fn entity_action_authority_uses_frozen_accepted_owner() {
        use pumpkin_cluster::canonical_action::{
            Action, ActionClass, ActionId, ActionKind, Cell, ChunkPart, ClientOrigin, Condition,
            Effect, Expected, Stakeholder, UndoProcedure, Value,
        };
        let source = ServerId(2);
        let id = GlobalEntityId {
            origin: ServerId(1),
            local_id: 42,
            generation: 3,
        };
        let chunk = ChunkKey::new(WorldId::new(0, 7), 0, 0);
        let target = Target::Chunk {
            key: chunk,
            part: ChunkPart::Block(pumpkin_cluster::protocol::BlockPos { x: 1, y: 64, z: 1 }),
            inventory: None,
        };
        let mut action = Action {
            edit_authority: None,
            id: ActionId {
                actor: ActorId::Entity(id),
                incarnation: 3,
                sequence: 1,
                family: 8,
            },
            tick: TickStamp(8),
            class: ActionClass::Undoable,
            kind: ActionKind::World,
            chunk_transition: false,
            actor_chunk: Some(chunk),
            stakeholders: vec![Stakeholder::Chunk(chunk)],
            player_leases: Vec::new(),
            player_chunks: Vec::new(),
            dependencies: Vec::new(),
            conditions: vec![Condition {
                target,
                expected: Expected::Exact(Cell {
                    revision: 1,
                    value: Value::Block(1),
                }),
            }],
            effects: vec![Effect::Put {
                target,
                value: Value::Block(2),
            }],
            counterfactual_conditions: Vec::new(),
            counterfactual_effects: Vec::new(),
            semantic_intent: None,
            factual_result: None,
            undo: vec![UndoProcedure::RestoreWrittenTargets],
            client_origin: ClientOrigin::None,
            presentation: Vec::new(),
        };
        let owner = PeerGeneration {
            server: source,
            generation: 12,
        };
        let scope = OwnerTickScope {
            full_source_slot: Some(0),
            primary_producers: BTreeSet::new(),
            local: source,
            directory: Arc::new(pumpkin_cluster::chunks::Directory::new()),
            local_chunks: Arc::new(HashMap::from([(
                chunk,
                Arc::new(
                    super::super::cluster_chunk_state::OwnedChunkBlocks::from_palettes(
                        0,
                        Box::new([]),
                        Box::new([]),
                    ),
                ),
            )])),
            incarnations: Arc::new(HashMap::from([(source, 12)])),
            transport_generations: Arc::new(HashMap::new()),
            producers: BTreeSet::from([owner]),
            players: BTreeMap::new(),
            entities: BTreeMap::from([(id, (chunk, owner))]),
        };
        let mut ledger = RealmCompletionLedger::default();
        let entity_target = Target::Chunk {
            key: chunk,
            part: ChunkPart::Entity(id),
            inventory: None,
        };
        let body = pumpkin_cluster::entity_body::EntityBody {
            mount: None,
            tags: Default::default(),
            tags_revision: 0,
            relation_versions: Default::default(),
            passengers: Default::default(),
            id,
            spatial_epoch: pumpkin_cluster::canonical_action::SpatialEpoch::Action(
                pumpkin_cluster::canonical_action::ActionRef {
                    tick: TickStamp(6),
                    id: ActionId {
                        actor: ActorId::Peer(id.origin),
                        incarnation: 9,
                        sequence: 0,
                        family: 8,
                    },
                },
            ),
            birth: pumpkin_cluster::entity_body::EntityBirth {
                action: pumpkin_cluster::canonical_action::ActionRef {
                    tick: TickStamp(6),
                    id: ActionId {
                        actor: ActorId::Peer(id.origin),
                        incarnation: 9,
                        sequence: 0,
                        family: 8,
                    },
                },
                source: PeerGeneration {
                    server: id.origin,
                    generation: 9,
                },
                uuid: uuid::Uuid::from_u128(42),
            },
            owner,
            kind: pumpkin_data::entity::EntityType::EXPERIENCE_ORB.id,
            spatial: pumpkin_cluster::entity_body::EntitySpatial {
                world: chunk.world,
                position: [0.5_f64.to_bits(), 80_f64.to_bits(), 0.5_f64.to_bits()],
                velocity_micro: [0; 3],
                yaw: 0,
                pitch: 0,
                on_ground: false,
            },
            payload: pumpkin_cluster::entity_body::EntityPayload::Experience(
                pumpkin_cluster::entity_body::ExperienceOrbState {
                    amount: 7,
                    count: 1,
                    health_milli: 5_000,
                    age: 0,
                    last_lifetime_tick: None,
                },
            ),
        };
        action.conditions.push(Condition {
            target: entity_target,
            expected: Expected::ActorControlled {
                birth: body.birth.clone(),
                issuer: owner,
            },
        });
        assert!(scope.capture_action(&mut ledger, source, &action).is_ok());
        let mut graphs = pumpkin_cluster::action_graph::ActionGraphs::default();
        assert_eq!(entity_authority_at_action_tick(&graphs, &action), None);
        graphs
            .install_ground_anchor(
                entity_target,
                pumpkin_cluster::action_graph::GroundAnchor {
                    settled_through: TickStamp(7),
                    value: Some(Cell {
                        revision: 1,
                        value: Value::Entity((body.clone()).into()),
                    }),
                },
            )
            .unwrap();
        assert_eq!(
            entity_authority_at_action_tick(&graphs, &action),
            Some((chunk, owner))
        );
        let mut newer_scope = scope;
        newer_scope.entities.insert(
            id,
            (
                chunk,
                PeerGeneration {
                    server: ServerId(3),
                    generation: 14,
                },
            ),
        );
        assert!(
            newer_scope
                .capture_action(&mut ledger, source, &action)
                .is_err()
        );
        assert!(
            newer_scope
                .capture_action_with_entity_authority(
                    &mut ledger,
                    source,
                    &action,
                    entity_authority_at_action_tick(&graphs, &action)
                )
                .is_ok()
        );
        let scope = newer_scope;
        let mut birth = body;
        birth.owner = birth.birth.source;
        birth.birth.action.tick = action.tick;
        let birth_ref = birth.birth.action;
        let birth_owner = birth.owner;
        let mut spawn = action.clone();
        spawn.id = birth_ref.id;
        spawn.conditions = vec![Condition {
            target: entity_target,
            expected: Expected::Missing,
        }];
        spawn.effects = vec![Effect::Put {
            target: entity_target,
            value: Value::Entity(birth.into()),
        }];
        let mut birth_graphs = pumpkin_cluster::action_graph::ActionGraphs::default();
        birth_graphs
            .install_ground_anchor(
                entity_target,
                pumpkin_cluster::action_graph::GroundAnchor {
                    settled_through: TickStamp(7),
                    value: None,
                },
            )
            .unwrap();
        birth_graphs.insert_collected(action.tick, [spawn]).unwrap();
        assert_eq!(
            entity_authority_at_action_tick(&birth_graphs, &action),
            None
        );
        action.dependencies.push(birth_ref);
        assert_eq!(
            entity_authority_at_action_tick(&birth_graphs, &action),
            Some((chunk, birth_owner))
        );
        action.dependencies.clear();
        action.id.incarnation = 4;
        assert!(scope.capture_action(&mut ledger, source, &action).is_err());
        action.id.incarnation = 3;
        action.actor_chunk = Some(ChunkKey::new(chunk.world, 1, 0));
        assert!(scope.capture_action(&mut ledger, source, &action).is_err());
        action.actor_chunk = Some(chunk);
        assert!(
            scope
                .capture_action(&mut ledger, id.origin, &action)
                .is_err()
        );
    }

    #[test]
    fn inhabited_tick_dedup_handles_counter_wrap() {
        assert!(inhabited_tick_is_newer(None, 0));
        assert!(!inhabited_tick_is_newer(Some(7), 7));
        assert!(!inhabited_tick_is_newer(Some(7), 6));
        assert!(inhabited_tick_is_newer(Some(u32::MAX), 0));
        assert!(!inhabited_tick_is_newer(Some(0), u32::MAX));
    }

    #[test]
    fn imported_scheduled_image_requires_exact_due_and_priority() {
        use pumpkin_cluster::canonical_action::{ScheduledTickFact, ScheduledTickKind};
        let tick = ScheduledTickFact {
            origin: pumpkin_cluster::canonical_action::ScheduledTickOrigin::GenesisPrimary,
            executor: PeerGeneration {
                server: ServerId(0),
                generation: 1,
            },
            kind: ScheduledTickKind::Fluid,
            id: 3,
            position: pumpkin_cluster::protocol::BlockPos { x: 1, y: 70, z: 2 },
            due: TickStamp(9),
            priority: -1,
            source_order: 99,
        };
        assert!(scheduled_queue_image_matches(
            &[tick],
            &[ScheduledTickFact {
                source_order: 0,
                ..tick
            }]
        ));
        assert!(!scheduled_queue_image_matches(
            &[tick],
            &[ScheduledTickFact {
                due: TickStamp(10),
                ..tick
            }]
        ));
        assert!(!scheduled_queue_image_matches(
            &[tick],
            &[ScheduledTickFact {
                priority: 0,
                ..tick
            }]
        ));
    }

    fn key(sequence: u16) -> ActionKey {
        ActionKey {
            incarnation: 0,
            actor: ActorId::Player(GlobalPlayerId::new(ServerId(1), PlayerSlot(1))),
            sequence,
            family: 2,
        }
    }

    #[test]
    fn terminal_actor_generation_preserves_newer_relog_inputs_across_wrap() {
        let actor = GlobalPlayerId::new(ServerId(1), PlayerSlot(7));
        let mut terminal = HashMap::new();
        note_terminal_actor_generation(&mut terminal, actor, u64::MAX);
        assert!(actor_generation_is_terminal(&terminal, actor, u64::MAX));
        assert!(!actor_generation_is_terminal(&terminal, actor, 0));
        note_terminal_actor_generation(&mut terminal, actor, 0);
        assert!(actor_generation_is_terminal(&terminal, actor, u64::MAX));
        assert!(actor_generation_is_terminal(&terminal, actor, 0));
        assert!(!actor_generation_is_terminal(&terminal, actor, 1));
        note_terminal_actor_generation(&mut terminal, actor, u64::MAX);
        assert_eq!(terminal[&actor], 0);
    }

    #[test]
    fn avatar_transition_captures_both_chunk_realms_without_inventory_domain() {
        use pumpkin_cluster::canonical_action::{Action, Cell, LivePlayerLease, PlayerKey, Value};
        use pumpkin_cluster::item_action::{
            ActorCombatState, ActorFoodState, ActorLivingState, PlayerAvatarState,
        };

        let local = ServerId(1);
        let gid = GlobalPlayerId::new(local, PlayerSlot(7));
        let world = WorldId::new(0, 7);
        let from = ChunkKey::new(world, 0, 0);
        let to = ChunkKey::new(world, 1, 0);
        let lease = LivePlayerLease {
            key: PlayerKey(uuid::Uuid::from_u128(700)),
            gid,
            incarnation: 9,
        };
        let before = ActorLivingState {
            permanent_attribute_modifiers: Default::default(),
            menu: Default::default(),
            advancements: Default::default(),
            tags: Default::default(),
            tags_revision: 0,
            environment: Default::default(),
            stand: None,
            mob: None,
            merchant: None,
            simulation_tick: None,
            effects: Vec::new(),
            hidden_effects: BTreeMap::new(),
            property_versions: std::collections::BTreeMap::new(),
            food: ActorFoodState {
                exhaustion_milli: 0,
                tick_timer: 0,
                actor: ActorId::Player(gid),
                food: 20,
                saturation_milli: 5_000,
                revision: 0,
            },
            combat: ActorCombatState {
                actor: ActorId::Player(gid),
                revision: 0,
                health_milli: 20_000,
                absorption_milli: 0,
                velocity_micro: [0; 3],
                timing: Default::default(),
                effective_attributes: Default::default(),
                equipment: Default::default(),
                status: Default::default(),
            },
            use_state: None,
            cooldowns: BTreeMap::new(),
            avatar: Some(PlayerAvatarState {
                stance: Default::default(),
                mount: None,
                relation_versions: Default::default(),
                statistics: Default::default(),
                experience: Default::default(),
                presentation: Default::default(),
                spatial_epoch: pumpkin_cluster::canonical_action::SpatialEpoch::Admission {
                    lease,
                    admitted_at: TickStamp(0),
                },
                world,
                position: [1.0_f64.to_bits(), 80.0_f64.to_bits(), 1.0_f64.to_bits()],
                yaw: 0.0_f32.to_bits(),
                head_yaw: 0.0_f32.to_bits(),
                pitch: 0.0_f32.to_bits(),
                on_ground: true,
                movement_tick: TickStamp(0),
                movement_sequence: pumpkin_cluster::identity::PlayerSeq(0),
                game_mode: 0,
                fall_distance: 0.0_f32.to_bits(),
                on_fire: false,
            }),
        };
        let mut after = before.clone();
        after.avatar.as_mut().unwrap().position[0] = 17.0_f64.to_bits();
        let frozen_before = Cell {
            revision: 4,
            value: Value::Living((before).into()),
        };
        let mut action = Action::from_player_administration(
            lease,
            pumpkin_cluster::identity::PlayerSeq(3),
            TickStamp(20),
            from,
            to,
            frozen_before.clone(),
            after,
        )
        .unwrap();
        action.validate().unwrap();
        let chunk_image = || {
            Arc::new(
                super::super::cluster_chunk_state::OwnedChunkBlocks::from_palettes(
                    0,
                    Box::new([]),
                    Box::new([]),
                ),
            )
        };
        let scope = OwnerTickScope {
            full_source_slot: Some(u64::from(action.tick.0)),
            primary_producers: BTreeSet::new(),
            local,
            directory: Arc::new(pumpkin_cluster::chunks::Directory::new()),
            local_chunks: Arc::new(HashMap::from([(from, chunk_image()), (to, chunk_image())])),
            incarnations: Arc::new(HashMap::from([(local, 42)])),
            transport_generations: Arc::new(HashMap::new()),
            producers: BTreeSet::from([PeerGeneration {
                server: local,
                generation: 42,
            }]),
            players: BTreeMap::from([(lease, from)]),
            entities: BTreeMap::new(),
        };
        let mut completion = RealmCompletionLedger::default();
        let reference = pumpkin_cluster::canonical_action::ActionRef {
            tick: action.tick,
            id: action.id,
        };
        let (tentative, recipients) = scope
            .tentative_local_manifest(
                action.tick,
                std::slice::from_ref(&action),
                &BTreeMap::new(),
                &completion,
                |_| None,
            )
            .unwrap();
        assert_eq!(tentative.issued.len(), 2);
        assert!(recipients.is_empty());
        assert!(completion.requirements_for(reference).is_none());
        let Value::Living(mut initial) = frozen_before.value.clone() else {
            panic!();
        };
        initial.avatar.as_mut().unwrap().spatial_epoch =
            pumpkin_cluster::canonical_action::SpatialEpoch::Admission {
                lease,
                admitted_at: action.tick,
            };
        let inventory_root = pumpkin_cluster::avatar_lifecycle::PlayerInventoryAdmission {
            imported_revision: 7,
            supplemental: [
                (
                    pumpkin_cluster::avatar_lifecycle::PLAYER_CRAFTING_INVENTORY_KIND,
                    5,
                ),
                (
                    pumpkin_cluster::avatar_lifecycle::PLAYER_ENDER_INVENTORY_KIND,
                    27,
                ),
            ]
            .into_iter()
            .map(|(kind, size)| {
                pumpkin_cluster::avatar_lifecycle::PlayerVirtualInventoryAdmission {
                    kind,
                    imported_revision: 7,
                    cells: (0..size)
                        .map(|slot| Cell {
                            revision: 7,
                            value: Value::Inventory(
                                pumpkin_cluster::inventory::InventoryCell::empty(
                                    pumpkin_cluster::inventory::InvLoc::new(0, slot),
                                ),
                            ),
                        })
                        .collect(),
                }
            })
            .collect(),
            cells: pumpkin_cluster::avatar_lifecycle::PlayerInventoryAdmission::slots()
                .map(|loc| {
                    let mut item = pumpkin_cluster::inventory::InventoryCell::empty(loc);
                    if loc.inv == pumpkin_cluster::inventory::INV_SELECTED {
                        item.item = 1;
                        item.count = 1;
                    }
                    Cell {
                        revision: 7,
                        value: Value::Inventory(item),
                    }
                })
                .collect(),
        };
        let admission = pumpkin_cluster::avatar_lifecycle::action(
            action.id,
            action.tick,
            from,
            pumpkin_cluster::avatar_lifecycle::AvatarLifecycleIntent::Admission {
                lease,
                install_generation: 1,
                owner: scope.source(local).unwrap(),
                initial: *initial,
                inventory: inventory_root,
            },
            None,
        )
        .unwrap();
        let mut bootstrap_graphs = pumpkin_cluster::action_graph::ActionGraphs::default();
        bootstrap_graphs
            .install_chunk_frontier(
                from,
                pumpkin_cluster::action_graph::ChunkGroundFrontier {
                    epoch: 1,
                    settled_through: TickStamp(18),
                },
            )
            .unwrap();
        let body = Target::Chunk {
            key: from,
            part: pumpkin_cluster::canonical_action::ChunkPart::Player(gid),
            inventory: None,
        };
        bootstrap_graphs
            .seed_player_avatar_bootstrap(body, lease, action.tick)
            .unwrap();
        let (anchors, frontiers) = bootstrap_graphs
            .seed_player_inventory_bootstrap(&admission)
            .unwrap();
        assert_eq!(anchors.len(), 75);
        assert_eq!(frontiers.len(), 7);
        assert!(
            anchors
                .iter()
                .all(|target| bootstrap_graphs.ground_anchors[target]
                    == pumpkin_cluster::action_graph::GroundAnchor {
                        settled_through: TickStamp(18),
                        value: None
                    })
        );
        assert_eq!(
            bootstrap_graphs
                .seed_player_inventory_bootstrap(&admission)
                .unwrap(),
            (Vec::new(), Vec::new())
        );
        let (repeated, _) = scope
            .tentative_local_manifest(
                action.tick,
                std::slice::from_ref(&action),
                &tentative.issued,
                &completion,
                |_| None,
            )
            .unwrap();
        assert_eq!(repeated, tentative);
        assert!(
            scope
                .tentative_local_manifest(
                    TickStamp(action.tick.0.wrapping_add(1)),
                    std::slice::from_ref(&action),
                    &BTreeMap::new(),
                    &completion,
                    |_| None,
                )
                .is_err()
        );
        assert!(completion.requirements_for(reference).is_none());
        let requirements = scope
            .capture_action(&mut completion, local, &action)
            .unwrap();
        assert_eq!(requirements.len(), 2);
        assert!(requirements.iter().all(|key| {
            matches!(
                key.stakeholder,
                pumpkin_cluster::canonical_action::Stakeholder::Chunk(_)
            )
        }));
        let mut frozen = pumpkin_cluster::canonical_action::OwnedStakeholderSet::default();
        frozen.local.insert(
            Target::Chunk {
                key: from,
                part: pumpkin_cluster::canonical_action::ChunkPart::Player(gid),
                inventory: None,
            },
            frozen_before,
        );
        assert!(prepare_frozen_player_action(&scope, &action, &frozen).is_ok());
        action.id.incarnation = action.id.incarnation.wrapping_add(1);
        assert_eq!(
            prepare_frozen_player_action(&scope, &action, &frozen),
            Err(pumpkin_cluster::canonical_action::ApplyFailure::InvalidAction)
        );
    }

    #[test]
    fn tick_boundary_merges_racing_action_families_once() {
        let tick = TickStamp(41);
        let chunk = ChunkKey::new(WorldId::new(0, 7), 3, 4);
        let manifest = |sequence| TickFinish {
            tick,
            chunks: vec![ChunkActions {
                chunk,
                actions: vec![key(sequence)],
            }],
        };
        for order in [[1, 2], [2, 1]] {
            let mut finishes = LocalFinishes::default();
            finishes.advance(tick);
            assert!(finishes.record(manifest(order[0])));
            assert!(finishes.record(manifest(order[1])));
            let closed = finishes.advance(TickStamp(42));
            assert_eq!(closed.len(), 1);
            assert_eq!(closed[0].actions_for(chunk), &[key(1), key(2)]);
            assert!(finishes.advance(TickStamp(42)).is_empty());
            assert!(!finishes.record(manifest(3)));
        }
    }

    #[test]
    fn empty_tick_boundary_stays_bounded_and_declares_zero_actions() {
        let mut finishes = LocalFinishes::default();
        finishes.advance(TickStamp(u16::MAX));
        for tick in [TickStamp(0), TickStamp(1), TickStamp(2)] {
            let closed = finishes.advance(tick);
            assert_eq!(closed.len(), 1);
            assert!(closed[0].chunks.is_empty());
            assert_eq!(finishes.pending.len(), 1);
        }
    }

    #[test]
    fn queued_action_crossing_boundary_uses_owner_resolution_tick() {
        let chunk = ChunkKey::new(WorldId::new(0, 7), 3, 4);
        let mut batch = TickBatch::new(TickStamp(70));
        batch
            .administrative
            .push(pumpkin_cluster::world_delta::AdministrativeDelta {
                holder: ServerId(1),
                sequence: 3,
                chunk,
                tick: TickStamp(70),
                edits: vec![pumpkin_cluster::world_delta::BlockDelta {
                    pos: pumpkin_cluster::protocol::BlockPos {
                        x: 48,
                        y: 70,
                        z: 64,
                    },
                    old_state: 1,
                    new_state: 2,
                }],
            });
        let mut finishes = LocalFinishes::default();
        finishes.advance(TickStamp(70));
        retick_batch(&mut batch, TickStamp(71));
        finishes.record(TickFinish::try_from_batch(&batch).unwrap());
        let previous = finishes.advance(TickStamp(71));
        assert_eq!(previous.len(), 1);
        assert!(previous[0].chunks.is_empty());
        let current = finishes.advance(TickStamp(72));
        assert_eq!(current.len(), 1);
        assert_eq!(current[0].tick, TickStamp(71));
        assert_eq!(
            current[0].actions_for(chunk),
            &[ActionKey::administrative(&batch.administrative[0])]
        );
    }

    #[test]
    fn departure_waits_for_post_controller_pump_and_owner_tick_closure() {
        let gid = GlobalPlayerId::new(ServerId(1), PlayerSlot(7));
        let (respond, _) = oneshot::channel();
        let mut pending = PendingActorDrain {
            request: ActorDrainRequest {
                gid,
                lease: pumpkin_cluster::canonical_action::LivePlayerLease {
                    key: pumpkin_cluster::canonical_action::PlayerKey(uuid::Uuid::from_u128(7)),
                    gid,
                    incarnation: 1,
                },
                install_generation: 9,
                through_tick: TickStamp(70),
                logout: false,
                respond,
            },
            admitted_tick: None,
            leave_applied: true,
        };
        let buckets = WorldTickBuckets::new();
        let graphs = pumpkin_cluster::action_graph::ActionGraphs::default();
        let mut finishes = LocalFinishes::default();
        finishes.advance(TickStamp(71));
        assert!(!pending.ready(
            Some(TickStamp(69)),
            TickStamp(71),
            &finishes,
            &buckets,
            &graphs,
            Some(pending.request.lease)
        ));
        assert!(!pending.ready(
            Some(TickStamp(70)),
            TickStamp(71),
            &finishes,
            &buckets,
            &graphs,
            Some(pending.request.lease)
        ));
        finishes.advance(TickStamp(72));
        assert!(!pending.ready(
            Some(TickStamp(70)),
            TickStamp(72),
            &finishes,
            &buckets,
            &graphs,
            Some(pending.request.lease)
        ));
        finishes.closed_through = Some(TickStamp(71));
        assert!(pending.ready(
            Some(TickStamp(70)),
            TickStamp(72),
            &finishes,
            &buckets,
            &graphs,
            Some(pending.request.lease)
        ));
    }

    #[test]
    fn admission_duplicate_uuid_is_rejected_before_any_actor_leg_changes() {
        use pumpkin_cluster::actor::ActorId;
        use pumpkin_cluster::combat::ActorCombatState;
        use pumpkin_cluster::food::ActorFoodState;
        use pumpkin_cluster::item_action::ActorLivingState;
        let existing = GlobalPlayerId::new(ServerId(1), PlayerSlot(6));
        let candidate = GlobalPlayerId::new(ServerId(1), PlayerSlot(7));
        let key = uuid::Uuid::from_u128(700);
        let mut actors = super::super::cluster_world_actor::OwnedActors::default();
        assert!(actors.admit_local(existing, key));
        let mut statistics =
            super::super::cluster_actor_statistics::OwnedActorStatistics::default();
        let mut living = super::super::cluster_world_actor::OwnedLivingActors::default();
        let mut inventory = super::super::cluster_world_inventory::InventoryOwner::detached();
        let actor = ActorId::Player(candidate);
        let state = ActorLivingState {
            permanent_attribute_modifiers: Default::default(),
            menu: Default::default(),
            advancements: Default::default(),
            tags: Default::default(),
            tags_revision: 0,
            environment: Default::default(),
            stand: None,
            mob: None,
            merchant: None,
            simulation_tick: None,
            effects: Vec::new(),
            hidden_effects: BTreeMap::new(),
            property_versions: BTreeMap::new(),
            use_state: None,
            cooldowns: BTreeMap::new(),
            avatar: None,
            food: ActorFoodState {
                actor,
                exhaustion_milli: 0,
                tick_timer: 0,
                food: 20,
                saturation_milli: 5000,
                revision: 0,
            },
            combat: ActorCombatState {
                actor,
                revision: 0,
                health_milli: 20000,
                absorption_milli: 0,
                velocity_micro: [0; 3],
                timing: Default::default(),
                effective_attributes: Default::default(),
                equipment: Default::default(),
                status: Default::default(),
            },
        };
        assert!(state.belongs_to(actor));
        let (send, mut received) = mpsc::channel(1);
        let proof = install_actor_admission(
            &mut actors,
            &mut statistics,
            &mut living,
            &mut inventory,
            super::super::cluster_world_actor::ActorAdmissionImport {
                completion: send.try_reserve_owned().unwrap(),
                actor: candidate,
                key,
                statistics: super::super::cluster_actor_statistics::ActorStatisticsImport {
                    actor: candidate,
                    values: Default::default(),
                },
                experience: Default::default(),
                living: state,
                presentation: Default::default(),
                fall_distance: 0.0,
                client_entity_id: 11,
                install_generation: 9,
            },
        );
        assert_eq!(received.try_recv().unwrap(), proof);
        assert!(!proof.accepted);
        assert!(proof.lease.is_none());
        assert_eq!(actors.local_key(existing), Some(key));
        assert_eq!(actors.local_key(candidate), None);
        assert!(statistics.current_revision(candidate).is_none());
        assert!(!living.contains(actor));
    }

    #[test]
    fn departure_waits_for_unresolved_canonical_player_action() {
        let gid = GlobalPlayerId::new(ServerId(1), PlayerSlot(7));
        let chunk = ChunkKey::new(WorldId::new(0, 7), 3, 4);
        let (respond, _) = oneshot::channel();
        let mut pending = PendingActorDrain {
            request: ActorDrainRequest {
                gid,
                lease: pumpkin_cluster::canonical_action::LivePlayerLease {
                    key: pumpkin_cluster::canonical_action::PlayerKey(uuid::Uuid::from_u128(7)),
                    gid,
                    incarnation: 1,
                },
                install_generation: 9,
                through_tick: TickStamp(70),
                logout: true,
                respond,
            },
            admitted_tick: None,
            leave_applied: true,
        };
        let action = pumpkin_cluster::canonical_action::Action::from_break(
            &pumpkin_cluster::protocol::BreakBlockUpdate {
                gid,
                seq: pumpkin_cluster::identity::PlayerSeq(1),
                tick: TickStamp(70),
                pos: pumpkin_cluster::protocol::BlockPos {
                    x: 48,
                    y: 70,
                    z: 64,
                },
                expected_old_state: 1,
                actor_chunk: chunk,
                chunk,
            },
            1,
            0,
            pumpkin_cluster::canonical_action::ClientOrigin::None,
        );
        let mut graphs = pumpkin_cluster::action_graph::ActionGraphs::default();
        graphs.insert_collected(TickStamp(70), [action]).unwrap();
        let buckets = WorldTickBuckets::new();
        let mut finishes = LocalFinishes::default();
        finishes.advance(TickStamp(71));
        finishes.advance(TickStamp(72));
        assert!(!pending.ready(
            Some(TickStamp(70)),
            TickStamp(72),
            &finishes,
            &buckets,
            &graphs,
            Some(pending.request.lease)
        ));
    }

    fn sustained_outbound_backpressure_declares_peer_failed() {
        let (outbound, mut receiver) = mpsc::channel(1);
        let (drop_request, mut drops) = mpsc::channel(1);
        outbound
            .try_send(OutboundParcel {
                peer: ServerId(9),
                header: StreamHeader::new(StreamKind::Accept, None),
                bytes: vec![0],
            })
            .unwrap();
        let mut outbox = HolisticOutbox::new(outbound, drop_request);
        let connected = HashSet::from([ServerId(2)]);
        for tick in 0..=MAX_PENDING_RELIABLE_PER_PEER {
            outbox.queue(
                &connected,
                ServerId(1),
                &TickFinish {
                    tick: TickStamp(tick as u16),
                    chunks: Vec::new(),
                },
            );
        }
        assert_eq!(drops.try_recv().unwrap(), ServerId(2));
        assert!(outbox.quarantined.contains(&ServerId(2)));
        assert!(!outbox.pending.contains_key(&ServerId(2)));
        assert_eq!(receiver.try_recv().unwrap().peer, ServerId(9));
        assert!(receiver.try_recv().is_err());
    }

    #[test]
    fn acquisition_raw_seal_keeps_bank_open_until_actual_source_finish_reservation() {
        use super::super::cluster::{AcquisitionOwnerEvent, AcquisitionSourceInput};
        let local = ServerId(1);
        let source = PeerGeneration {
            server: local,
            generation: 11,
        };
        let scope = OwnerTickScope {
            full_source_slot: Some(0),
            primary_producers: BTreeSet::new(),
            local,
            directory: Arc::new(pumpkin_cluster::chunks::Directory::new()),
            local_chunks: Arc::new(HashMap::new()),
            incarnations: Arc::new(HashMap::from([(local, source.generation)])),
            transport_generations: Arc::new(HashMap::new()),
            producers: BTreeSet::from([source]),
            players: BTreeMap::new(),
            entities: BTreeMap::new(),
        };
        let (events, mut received) = mpsc::channel(1);
        assert!(
            events
                .try_send(AcquisitionOwnerEvent::ObservedPrefix {
                    authenticated: source,
                    prefix: pumpkin_cluster::acquisition_prefix::AcquisitionPrefix {
                        source,
                        tick: TickStamp(0),
                        sequence: 0
                    },
                })
                .is_ok()
        );
        let (outbound, _) = mpsc::channel(1);
        let (drop_request, _) = mpsc::channel(1);
        let mut outbox = HolisticOutbox::new(outbound, drop_request);
        let mut bank = OwnerAcquisitionBank::default();
        bank.pending.push_back(AcquisitionSourceInput::Seal {
            tick: TickStamp(3),
            events,
        });
        bank.progress(&scope, &mut outbox);
        assert_eq!(bank.sealed_through, Some(TickStamp(3)));
        assert!(bank.pending.is_empty());
        assert!(
            bank.prepare_close(TickStamp(1), &scope, &mut outbox)
                .is_none()
        );
        received.try_recv().unwrap();
        let prepared = bank
            .prepare_close(TickStamp(1), &scope, &mut outbox)
            .unwrap();
        assert_eq!(prepared.prefix.sequence, 0);
        bank.commit_close(prepared, &mut outbox);
        assert!(
            matches!(received.try_recv().unwrap(), AcquisitionOwnerEvent::ClosedPrefix(prefix)
            if prefix.tick == TickStamp(1) && prefix.sequence == 0)
        );
        assert!(
            bank.prepare_close(TickStamp(4), &scope, &mut outbox)
                .is_none()
        );
    }

    #[test]
    fn acquisition_control_batch_backpressure_keeps_every_peer_and_finish_unmodified() {
        let (outbound, mut receiver) = mpsc::channel(1);
        let (drop_request, mut drops) = mpsc::channel(1);
        outbound
            .try_send(OutboundParcel {
                peer: ServerId(9),
                header: StreamHeader::new(StreamKind::Control, None),
                bytes: vec![9],
            })
            .unwrap();
        let mut outbox = HolisticOutbox::new(outbound, drop_request);
        outbox
            .reserved
            .insert(ServerId(3), MAX_PENDING_RELIABLE_PER_PEER);
        let batch = || {
            [ServerId(2), ServerId(3)]
                .into_iter()
                .map(|peer| OutboundParcel {
                    peer,
                    header: StreamHeader::new(StreamKind::ChunkRequest, None),
                    bytes: b"CAQ2".to_vec(),
                })
                .collect::<Vec<_>>()
        };
        assert!(!outbox.try_queue_control_batch(batch()));
        assert!(outbox.pending.is_empty());
        assert!(outbox.quarantined.is_empty());
        assert!(drops.try_recv().is_err());
        outbox.reserved.remove(&ServerId(3));
        assert!(outbox.try_queue_control_batch(batch()));
        assert_eq!(outbox.pending[&ServerId(2)].len(), 1);
        assert_eq!(outbox.pending[&ServerId(3)].len(), 1);
        assert_eq!(receiver.try_recv().unwrap().peer, ServerId(9));
    }

    #[test]
    fn action_backpressure_keeps_actions_before_holistic_finish() {
        let (outbound, mut receiver) = mpsc::channel(1);
        let (drop_request, mut drops) = mpsc::channel(1);
        outbound
            .try_send(OutboundParcel {
                peer: ServerId(9),
                header: StreamHeader::new(StreamKind::Control, None),
                bytes: vec![0],
            })
            .unwrap();
        let mut outbox = HolisticOutbox::new(outbound, drop_request);
        outbox.queue_parcels([1_u8, 2].map(|marker| OutboundParcel {
            peer: ServerId(2),
            header: StreamHeader::new(StreamKind::PlayerWorld, None),
            bytes: vec![marker],
        }));
        outbox.queue(
            &HashSet::from([ServerId(2)]),
            ServerId(1),
            &TickFinish {
                tick: TickStamp(4),
                chunks: Vec::new(),
            },
        );
        assert_eq!(receiver.try_recv().unwrap().peer, ServerId(9));
        outbox.flush();
        let first = receiver.try_recv().unwrap();
        assert_eq!(first.header.kind, StreamKind::PlayerWorld);
        assert_eq!(first.bytes, vec![1]);
        outbox.flush();
        let second = receiver.try_recv().unwrap();
        assert_eq!(second.header.kind, StreamKind::PlayerWorld);
        assert_eq!(second.bytes, vec![2]);
        outbox.flush();
        let finish = receiver.try_recv().unwrap();
        assert_eq!(finish.header.kind, StreamKind::Accept);
        assert!(drops.try_recv().is_err());
    }

    #[test]
    fn realm_finish_uses_reliable_accept_stream_with_holistic_zero_evidence() {
        let (outbound, mut receiver) = mpsc::channel(2);
        let (drop_request, _) = mpsc::channel(1);
        let mut outbox = HolisticOutbox::new(outbound, drop_request);
        let finish = pumpkin_cluster::realm_completion::HolisticFinish {
            full_source_slot: u64::from((TickStamp(12)).0),
            acquisition_prefix: None,
            tick: TickStamp(12),
            issued: BTreeMap::new(),
        };
        outbox.queue_realm(
            &HashSet::from([ServerId(1), ServerId(2)]),
            ServerId(1),
            &finish,
        );
        let parcel = receiver.try_recv().unwrap();
        assert_eq!(parcel.peer, ServerId(2));
        assert_eq!(parcel.header.kind, StreamKind::Accept);
        assert_eq!(
            pumpkin_cluster::completion::decode_message(&parcel.bytes).unwrap(),
            CompletionMessage::RealmFinish(finish)
        );
        assert!(receiver.try_recv().is_err());
    }

    #[test]
    fn new_peer_receives_only_finishes_closed_after_join() {
        let (outbound, mut receiver) = mpsc::channel(8);
        let (drop_request, _) = mpsc::channel(1);
        let mut outbox = HolisticOutbox::new(outbound, drop_request);
        outbox.queue(
            &HashSet::from([ServerId(2)]),
            ServerId(1),
            &TickFinish {
                tick: TickStamp(8),
                chunks: Vec::new(),
            },
        );
        outbox.queue(
            &HashSet::from([ServerId(2), ServerId(3)]),
            ServerId(1),
            &TickFinish {
                tick: TickStamp(9),
                chunks: Vec::new(),
            },
        );
        let mut deliveries = Vec::new();
        while let Ok(parcel) = receiver.try_recv() {
            deliveries.push((
                parcel.peer,
                pumpkin_cluster::completion::decode_finish(&parcel.bytes)
                    .unwrap()
                    .tick,
            ));
        }
        assert_eq!(
            deliveries,
            vec![
                (ServerId(2), TickStamp(8)),
                (ServerId(2), TickStamp(9)),
                (ServerId(3), TickStamp(9))
            ]
        );
    }
}
