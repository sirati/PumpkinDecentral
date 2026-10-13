use crate::canonical_action::{ActionRef, ApplyFailure, Target};
use serde::{Deserialize, Serialize};
use std::collections::BTreeMap;

use crate::chunks::{ChunkAnnounce, ChunkFetch, ChunkSubscriptionControl};
use crate::codec::CodecError;
use crate::protocol::{ChunkKey, StreamKind};

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct PendingRef {
    pub state: crate::completion::ChunkPendingTick,
}

/// Accepted player state at the provider's snapshot cut. This is separate
/// from lossy movement and from the client-facing replica projection.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct PlayerLivingGround {
    pub lease: crate::canonical_action::LivePlayerLease,
    pub admitted_at: crate::time::TickStamp,
    pub target: crate::canonical_action::Target,
    pub cell: crate::canonical_action::Cell,
    pub spatial: crate::protocol::PosUpdate,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct PlayerAvatarAbsentGround {
    pub lease: crate::canonical_action::LivePlayerLease,
    pub admitted_at: crate::time::TickStamp,
    pub target: crate::canonical_action::Target,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct ChunkPayload {
    pub genesis: Option<crate::chunk_genesis::ChunkGenesisCertificate>,
    pub chunk: ChunkKey,
    pub holder: u16,
    pub snapshot_cut: crate::time::TickStamp,
    pub subscription: Option<crate::chunks::ChunkSubscriptionStart>,
    pub occupancy: Option<crate::canonical_action::Cell>,
    pub ground_frontier: Option<crate::action_graph::ChunkGroundFrontier>,
    pub player_inventory_frontiers: Vec<(
        crate::action_graph::InventoryRealmId,
        crate::action_graph::PlayerInventoryGroundFrontier,
    )>,
    pub snapshot: Vec<u8>,
    pub pendings: Vec<PendingRef>,
    pub graphs: Vec<crate::action_graph::TickGraphSnapshot>,
    pub ground_anchors: Vec<(
        crate::canonical_action::Target,
        crate::action_graph::GroundAnchor,
    )>,
    pub finishes: Vec<(
        crate::realm_completion::PeerGeneration,
        crate::realm_completion::HolisticFinish,
    )>,
    pub completion: Option<crate::realm_completion::RealmCompletionSnapshot>,
    pub holders: Vec<u16>,
    pub inventory: Vec<crate::inventory::ChunkInventoryState>,
    pub player_living: Vec<PlayerLivingGround>,
    pub player_avatar_absent: Vec<PlayerAvatarAbsentGround>,
    pub entity_bodies: Vec<(
        crate::canonical_action::Target,
        Option<crate::canonical_action::Cell>,
    )>,
    pub entity_history: Vec<crate::entity_history::EntityHistoryEvidence>,
    pub dependency_history: Vec<crate::entity_history::EntityHistoryEvidence>,
    pub item_spatial: Vec<crate::item_spatial::ItemSpatialSample>,
}

pub fn validate_menu_inventory_roots<'a>(
    states: &[crate::inventory::ChunkInventoryState],
    players: &[PlayerLivingGround],
    nodes: impl Iterator<Item = &'a crate::action_graph::GraphNodeSnapshot> + Clone,
    completions: impl Iterator<Item = &'a crate::realm_completion::RealmCompletionSnapshot> + Clone,
) -> Result<(), ApplyFailure> {
    use crate::canonical_action::{Effect, Value};
    for state in states {
        let Some((reference, admission)) = &state.menu_admission else {
            if matches!(state.snapshot.target, crate::inventory::InventoryTarget::LeasedVirtual { session, .. } if session != 0) {
                return Err(ApplyFailure::InvalidAction);
            }
            continue;
        };
        let slot = (admission.session & !(1u64 << 63)) >> 16;
        if crate::canonical_menu_identity::MenuPresentationIdentity::from_source(*reference, slot)
            .is_none_or(|identity| identity.session != admission.session)
            || reference.id.actor != crate::actor::ActorId::Player(admission.lease.gid)
            || reference.id.incarnation != admission.lease.incarnation
            || state.player_lease != Some(admission.lease)
            || state.snapshot.target != admission.inventory_target() {
            return Err(ApplyFailure::InvalidAction);
        }
        let grounded_session = players.iter().any(|player| {
            player.lease == admission.lease && matches!(&player.cell.value, Value::Living(living)
                if living.menu.valid() && living.menu.session == admission.session
                    && living.menu.window_kind == admission.layout.window_kind())
        });
        let source_slot_proven = completions.clone().any(|completion| {
            completion.actions.iter().find(|action| action.reference == *reference).is_some_and(|action| {
                completion.finishes.iter().any(|(source, finish)| *source == action.source
                    && finish.tick == reference.tick && finish.full_source_slot == slot)
            })
        });
        let grounded_birth = source_slot_proven && nodes.clone().any(|node| {
            node.action.id == reference.id && node.action.tick == reference.tick
                && node.action.validate().is_ok()
                && node.action.effects.iter().any(|effect| matches!(effect,
                    Effect::MenuVirtualInventoryAdmission { admission: actual } if actual == admission))
                && node.factual.result.is_some()
                && admission.entries().iter().all(|(target, _)| node.applied_realms.contains(&target.action_realm()))
        });
        if !grounded_session && !grounded_birth { return Err(ApplyFailure::InvalidAction); }
    }
    Ok(())
}

impl ChunkPayload {
    fn history_proofs(
        &self,
    ) -> Result<crate::entity_history::EntityBindingProofContext, ApplyFailure> {
        let mut frames = self.entity_history.clone();
        for frame in &self.dependency_history {
            if !frames.contains(frame) {
                frames.push(frame.clone());
            }
        }
        crate::entity_history::EntityBindingProofContext::from_frames(&frames)
    }
    pub fn project_ground(
        &self,
    ) -> Result<BTreeMap<Target, Option<crate::canonical_action::Cell>>, ApplyFailure> {
        self.project_truth(false)
    }

    pub fn project_local(&self) -> Result<BTreeMap<Target, Option<crate::canonical_action::Cell>>, ApplyFailure> {
        self.validate_transfer_evidence()?;
        self.project_truth(true)
    }

    fn project_truth(&self, local: bool) -> Result<BTreeMap<Target, Option<crate::canonical_action::Cell>>, ApplyFailure> {
        let actions = self
            .graphs
            .iter()
            .flat_map(|graph| {
                graph.nodes.iter().map(move |node| {
                    (
                        ActionRef {
                            tick: graph.tick,
                            id: node.action.id,
                        },
                        node.action.clone(),
                    )
                })
            })
            .collect::<BTreeMap<_, _>>();
        if actions.len()
            != self
                .graphs
                .iter()
                .map(|graph| graph.nodes.len())
                .sum::<usize>()
        {
            return Err(ApplyFailure::InvalidAction);
        }
        let mut ledger = crate::realm_completion::RealmCompletionLedger::default();
        if let Some(snapshot) = &self.completion {
            ledger
                .import_snapshot(snapshot, &actions)
                .map_err(|_| ApplyFailure::InvalidAction)?;
        } else if !actions.is_empty() {
            return Err(ApplyFailure::InvalidAction);
        }
        let proofs = self.history_proofs()?;
        let projector = if local { crate::action_graph::project_chunk_graph_snapshot_local_with_context }
            else { crate::action_graph::project_chunk_graph_snapshot_ground_with_context };
        projector(self.chunk, self.snapshot_cut, &self.graphs, &self.ground_anchors, Some((&proofs, &ledger)))
    }

    pub fn validate_transfer_evidence(&self) -> Result<(), crate::canonical_action::ApplyFailure> {
        use std::collections::{BTreeMap, BTreeSet};

        use crate::canonical_action::{ActionRef, ApplyFailure, ChunkPart, Target};

        if self.genesis.as_ref().is_some_and(|genesis| genesis.chunk != self.chunk || genesis.validate_structural().is_err()) {
            return Err(ApplyFailure::InvalidAction);
        }
        if let Some(start) = self.subscription {
            use sha2::Digest;
            let hash: [u8; 32] = sha2::Sha256::digest(&self.snapshot).into();
            if !start.has_contiguous_cut() || start.subscription.chunk != self.chunk
                || start.provider != self.holder || start.provider_incarnation == 0
                || start.subscription.incarnation == 0 || start.subscription.subscription_id == 0
                || start.snapshot_cut != self.snapshot_cut || start.snapshot_hash != hash
            {
                return Err(ApplyFailure::InvalidAction);
            }
        }
        if self.graphs.len() > 32_768
            || self
                .graphs
                .iter()
                .map(|graph| graph.nodes.len())
                .sum::<usize>()
                > 8_192
            || self.ground_anchors.len() > 32_768
            || self.finishes.len() > 32_768
            || self.pendings.len() > 32_768
            || self.inventory.len() > 8_192
            || self.player_inventory_frontiers.len() > 8_192
            || self.player_living.len() > 1_024
            || self.player_avatar_absent.len() > 1_024
            || self.entity_bodies.len() > 8_192
            || self.entity_history.len() > 128
            || self.dependency_history.len() > 128
            || self
                .entity_history
                .iter()
                .chain(&self.dependency_history)
                .flat_map(|frame| &frame.graphs)
                .try_fold(0usize, |count, graph| count.checked_add(graph.nodes.len()))
                .is_none_or(|count| count > 8192)
            || self.ground_frontier.is_some_and(|frontier| {
                self.snapshot_cut.distance_since(frontier.settled_through) >= 32_768
            })
        {
            return Err(ApplyFailure::InvalidAction);
        }
        let mut history_births = BTreeMap::new();
        let mut history_uuids = BTreeMap::new();
        let binding_proofs = self.history_proofs()?;
        for frame in &self.dependency_history {
            if self.snapshot_cut.distance_since(frame.snapshot_cut) >= 32_768
                || frame
                    .fixed_dependency_outcomes(Some(&binding_proofs))?
                    .is_empty()
            {
                return Err(ApplyFailure::InvalidAction);
            }
        }
        for frame in &self.entity_history {
            if self.snapshot_cut.distance_since(frame.snapshot_cut) >= 32_768 {
                return Err(ApplyFailure::InvalidAction);
            }
            frame.validate_with_context(Some(&binding_proofs))?;
            for (id, birth) in frame.births()? {
                if history_births
                    .insert(id, birth.clone())
                    .is_some_and(|previous| previous != birth)
                    || history_uuids
                        .insert(birth.uuid, id)
                        .is_some_and(|previous| previous != id)
                {
                    return Err(ApplyFailure::InvalidAction);
                }
            }
        }
        for write in self
            .graphs
            .iter()
            .flat_map(|graph| &graph.nodes)
            .flat_map(|node| node.factual.result.iter())
            .flat_map(|result| &result.writes)
        {
            for cell in [&write.before, &write.after].into_iter().flatten() {
                let crate::canonical_action::Value::Entity(body) = &cell.value else {
                    continue;
                };
                let Target::Chunk {
                    key,
                    part: ChunkPart::Entity(id),
                    inventory: None,
                } = write.target
                else {
                    return Err(ApplyFailure::InvalidAction);
                };
                if !body.belongs_to(id, key)
                    || !body.valid_at(self.snapshot_cut)
                    || history_births
                        .insert(id, body.birth.clone())
                        .is_some_and(|previous| previous != body.birth)
                    || history_uuids
                        .insert(body.birth.uuid, id)
                        .is_some_and(|previous| previous != id)
                {
                    return Err(ApplyFailure::InvalidAction);
                }
            }
        }
        let mut actions = BTreeMap::new();
        let mut player_frontiers = BTreeSet::new();
        for (realm, frontier) in &self.player_inventory_frontiers {
            let address = match realm {
                crate::action_graph::InventoryRealmId::Player(_) => {
                    let targets = [
                        crate::inventory::InventoryTarget::Player(frontier.lease.gid),
                        crate::inventory::InventoryTarget::LeasedPlayer {
                            key: frontier.lease.key.0,
                            player: frontier.lease.gid,
                            incarnation: frontier.lease.incarnation,
                        },
                    ];
                    let matching = targets
                        .into_iter()
                        .filter(|target| {
                            self.inventory.iter().any(|state| {
                                state.player_lease == Some(frontier.lease)
                                    && state.snapshot.target == *target
                                    && state.chunks.contains(&self.chunk)
                            })
                        })
                        .collect::<Vec<_>>();
                    if matching.len() != 1 {
                        return Err(ApplyFailure::InvalidAction);
                    }
                    matching[0]
                }
                crate::action_graph::InventoryRealmId::Virtual {
                    host,
                    kind,
                    session,
                    ..
                } => {
                    let targets = [
                        crate::inventory::InventoryTarget::Virtual {
                            host: *host,
                            owner: crate::actor::ActorId::Player(frontier.lease.gid),
                            session: *session,
                            kind: *kind,
                        },
                        crate::inventory::InventoryTarget::LeasedVirtual {
                            host: *host,
                            key: frontier.lease.key.0,
                            player: frontier.lease.gid,
                            incarnation: frontier.lease.incarnation,
                            session: *session,
                            kind: *kind,
                        },
                    ];
                    let matching = targets
                        .into_iter()
                        .filter(|target| {
                            self.inventory.iter().any(|state| {
                                state.player_lease == Some(frontier.lease)
                                    && state.snapshot.target == *target
                                    && state.chunks.contains(&self.chunk)
                            })
                        })
                        .collect::<Vec<_>>();
                    if matching.len() != 1 {
                        return Err(ApplyFailure::InvalidAction);
                    }
                    matching[0]
                }
                crate::action_graph::InventoryRealmId::Block(_)
                | crate::action_graph::InventoryRealmId::Entity(_) => {
                    return Err(ApplyFailure::InvalidAction);
                }
            };
            if frontier.accepted_chunk != self.chunk
                || frontier.lease.incarnation == 0
                || frontier.domain_epoch != frontier.lease.incarnation
                || self.snapshot_cut.distance_since(frontier.settled_through) >= 32_768
                || !player_frontiers.insert((frontier.lease, *realm))
                || !self.inventory.iter().any(|state| {
                    state.player_lease == Some(frontier.lease)
                        && state.snapshot.target == address
                        && state.chunks.contains(&self.chunk)
                })
            {
                return Err(ApplyFailure::InvalidAction);
            }
        }
        for state in self
            .inventory
            .iter()
            .filter(|state| state.player_lease.is_some())
        {
            let lease = state.player_lease.ok_or(ApplyFailure::InvalidAction)?;
            let mut realms = match state.snapshot.target {
                crate::inventory::InventoryTarget::Player(gid) if gid == lease.gid => {
                    (crate::inventory::INV_MAIN..=crate::inventory::INV_SELECTED)
                        .map(crate::action_graph::InventoryRealmId::Player)
                        .collect::<BTreeSet<_>>()
                }
                crate::inventory::InventoryTarget::LeasedPlayer {
                    key,
                    player: gid,
                    incarnation,
                } if key == lease.key.0 && gid == lease.gid && incarnation == lease.incarnation => {
                    (crate::inventory::INV_MAIN..=crate::inventory::INV_SELECTED)
                        .map(crate::action_graph::InventoryRealmId::Player)
                        .collect::<BTreeSet<_>>()
                }
                crate::inventory::InventoryTarget::Virtual {
                    host,
                    owner: crate::actor::ActorId::Player(gid),
                    session,
                    kind,
                } if gid == lease.gid => {
                    BTreeSet::from([crate::action_graph::InventoryRealmId::Virtual {
                        host,
                        kind,
                        session,
                        inv: crate::inventory::INV_MAIN,
                    }])
                }
                crate::inventory::InventoryTarget::LeasedVirtual {
                    host,
                    key,
                    player: gid,
                    incarnation,
                    session,
                    kind,
                } if key == lease.key.0 && gid == lease.gid && incarnation == lease.incarnation => {
                    BTreeSet::from([crate::action_graph::InventoryRealmId::Virtual {
                        host,
                        kind,
                        session,
                        inv: crate::inventory::INV_MAIN,
                    }])
                }
                _ => return Err(ApplyFailure::InvalidAction),
            };
            for cell in &state.snapshot.cells {
                let realm = match state.snapshot.target {
                    crate::inventory::InventoryTarget::Player(_)
                    | crate::inventory::InventoryTarget::LeasedPlayer { .. } => {
                        crate::action_graph::InventoryRealmId::Player(cell.loc.inv)
                    }
                    crate::inventory::InventoryTarget::Virtual {
                        host,
                        session,
                        kind,
                        ..
                    }
                    | crate::inventory::InventoryTarget::LeasedVirtual {
                        host,
                        session,
                        kind,
                        ..
                    } => crate::action_graph::InventoryRealmId::Virtual {
                        host,
                        kind,
                        session,
                        inv: cell.loc.inv,
                    },
                    _ => return Err(ApplyFailure::InvalidAction),
                };
                realms.insert(realm);
            }
            if !state.chunks.contains(&self.chunk)
                || realms.iter().any(|realm| {
                    !self
                        .player_inventory_frontiers
                        .iter()
                        .any(|(known, frontier)| {
                            known == realm
                                && frontier.lease == lease
                                && frontier.accepted_chunk == self.chunk
                        })
                })
            {
                return Err(ApplyFailure::InvalidAction);
            }
        }
        for graph in &self.graphs {
            for node in &graph.nodes {
                for (key, accepted_chunk) in &node.action.player_chunks {
                    if *accepted_chunk != self.chunk {
                        continue;
                    }
                    let lease = node
                        .action
                        .player_leases
                        .iter()
                        .find(|lease| lease.key == *key)
                        .ok_or(ApplyFailure::InvalidAction)?;
                    for realm in node.required_realms.iter().filter_map(|realm| match realm {
                        crate::action_graph::ActionRealm::Inventory(
                            crate::canonical_action::Stakeholder::Player(player),
                            realm,
                        ) if player == key => Some(*realm),
                        _ => None,
                    }) {
                        if !self
                            .player_inventory_frontiers
                            .iter()
                            .any(|(known, frontier)| {
                                *known == realm
                                    && frontier.lease == *lease
                                    && frontier.accepted_chunk == *accepted_chunk
                            })
                        {
                            return Err(ApplyFailure::InvalidAction);
                        }
                    }
                }
                let reference = ActionRef {
                    tick: graph.tick,
                    id: node.action.id,
                };
                if actions.insert(reference, node.action.clone()).is_some() {
                    return Err(ApplyFailure::InvalidAction);
                }
            }
        }
        let mut pending_ticks = BTreeSet::new();
        for pending in &self.pendings {
            if pending.state.chunk != self.chunk
                || self.snapshot_cut.distance_since(pending.state.tick) >= 32_768
                || !pending_ticks.insert(pending.state.tick)
            {
                return Err(ApplyFailure::InvalidAction);
            }
            if pending.state.generations.len() > 1_024
                || pending
                    .state
                    .generations
                    .windows(2)
                    .any(|pair| pair[0].0 >= pair[1].0)
                || pending
                    .state
                    .generations
                    .iter()
                    .any(|(_, generation)| *generation == 0)
                || pending.state.finishes.iter().any(|(source, _)| {
                    !pending
                        .state
                        .generations
                        .iter()
                        .any(|(peer, _)| peer == source)
                })
                || pending.state.batches.iter().any(|sourced| {
                    !pending
                        .state
                        .generations
                        .iter()
                        .any(|(peer, _)| *peer == sourced.source)
                })
            {
                return Err(ApplyFailure::InvalidAction);
            }
            if pending
                .state
                .generations
                .iter()
                .any(|(source, generation)| {
                    self.finishes.iter().any(|(peer, finish)| {
                        finish.tick == pending.state.tick
                            && peer.server == *source
                            && peer.generation != *generation
                    })
                })
            {
                return Err(ApplyFailure::InvalidAction);
            }
            for sourced in &pending.state.batches {
                if sourced.batch.tick != pending.state.tick {
                    return Err(ApplyFailure::InvalidAction);
                }
                for action in &sourced.batch.canonical_actions {
                    let reference = ActionRef {
                        tick: action.tick,
                        id: action.id,
                    };
                    if action.tick != pending.state.tick || actions.get(&reference) != Some(action)
                    {
                        return Err(ApplyFailure::InvalidAction);
                    }
                }
            }
        }
        let Some(completion) = &self.completion else {
            return Err(ApplyFailure::InvalidAction);
        };
        if completion
            .requirements
            .iter()
            .any(|entry| self.snapshot_cut.distance_since(entry.tick) >= 32_768)
            || completion
                .players
                .iter()
                .any(|entry| self.snapshot_cut.distance_since(entry.tick) >= 32_768)
            || completion
                .actions
                .iter()
                .any(|entry| self.snapshot_cut.distance_since(entry.reference.tick) >= 32_768)
            || completion
                .finishes
                .iter()
                .any(|(_, entry)| self.snapshot_cut.distance_since(entry.tick) >= 32_768)
            || completion
                .conflicting_finishes
                .iter()
                .any(|(tick, _)| self.snapshot_cut.distance_since(*tick) >= 32_768)
        {
            return Err(ApplyFailure::InvalidAction);
        }
        if completion
            .actions
            .iter()
            .map(|entry| entry.reference)
            .collect::<BTreeSet<_>>()
            != actions.keys().copied().collect()
        {
            return Err(ApplyFailure::InvalidAction);
        }
        let mut ledger = crate::realm_completion::RealmCompletionLedger::default();
        ledger
            .import_snapshot(completion, &actions)
            .map_err(|_| ApplyFailure::InvalidAction)?;
        for (target, anchor) in &self.ground_anchors {
            if !target.is_valid() {
                return Err(ApplyFailure::InvalidAction);
            }
            if let crate::canonical_action::Target::Chunk { key, .. } = target {
                if *key != self.chunk
                    || self.ground_frontier.is_none_or(|frontier| {
                        frontier
                            .settled_through
                            .distance_since(anchor.settled_through)
                            >= 32_768
                    })
                {
                    return Err(ApplyFailure::InvalidAction);
                }
                continue;
            }
            let crate::canonical_action::Target::Player {
                key,
                lease: Some(target_lease),
                inventory: Some(id),
            } = target
            else {
                continue;
            };
            let realm = id.action_realm();
            if !self
                .player_inventory_frontiers
                .iter()
                .any(|(entry_realm, frontier)| {
                    *entry_realm == realm
                        && frontier.lease.key == *key
                        && frontier.lease == *target_lease
                        && frontier
                            .settled_through
                            .distance_since(anchor.settled_through)
                            < 32_768
                })
            {
                return Err(ApplyFailure::InvalidAction);
            }
        }
        let projected = crate::action_graph::project_chunk_graph_snapshot_ground_with_context(
            self.chunk,
            self.snapshot_cut,
            &self.graphs,
            &self.ground_anchors,
            Some((&binding_proofs, &ledger)),
        )?;
        validate_menu_inventory_roots(&self.inventory, &self.player_living,
            self.graphs.iter().chain(self.entity_history.iter().chain(&self.dependency_history)
                .flat_map(|frame| &frame.graphs)).flat_map(|graph| &graph.nodes),
            self.completion.iter().chain(self.entity_history.iter().chain(&self.dependency_history).map(|frame| &frame.completion)))?;
        let occupancy_target = Target::Chunk { key: self.chunk, part: ChunkPart::ActorOccupancy, inventory: None };
        if self.occupancy.is_some() || projected.contains_key(&occupancy_target) {
            if projected.get(&occupancy_target) != Some(&self.occupancy)
                || !self.occupancy.as_ref().is_some_and(|cell| {
                    matches!(&cell.value, crate::canonical_action::Value::ActorOccupancy(root) if root.valid() && root.is_complete())
                })
            { return Err(ApplyFailure::InvalidAction); }
        }
        let entity_bodies = self
            .entity_bodies
            .iter()
            .cloned()
            .collect::<BTreeMap<_, _>>();
        if entity_bodies.len() != self.entity_bodies.len() {
            return Err(ApplyFailure::InvalidAction);
        }
        let mut entity_uuids = BTreeSet::new();
        for (target, cell) in &entity_bodies {
            let Target::Chunk {
                key,
                part: ChunkPart::Entity(id),
                inventory: None,
            } = *target
            else {
                return Err(ApplyFailure::InvalidAction);
            };
            if key != self.chunk
                || cell.as_ref().is_some_and(|cell| matches!(&cell.value, crate::canonical_action::Value::Entity(body) if history_births.get(&id).is_some_and(|birth| *birth != body.birth) || history_uuids.get(&body.birth.uuid).is_some_and(|previous| *previous != id)))
                || cell.as_ref().is_some_and(|cell| matches!(&cell.value,
                    crate::canonical_action::Value::Entity(body) if !entity_uuids.insert(body.birth.uuid)))
                || projected.get(target) != Some(cell)
                || cell.as_ref().is_some_and(|cell| {
                    !matches!(&cell.value,
                    crate::canonical_action::Value::Entity(body) if body.belongs_to(id, key) && body.valid_at(self.snapshot_cut))
                })
            {
                return Err(ApplyFailure::InvalidAction);
            }
        }
        if projected.keys().any(|target| {
            matches!(
                target,
                Target::Chunk {
                    part: ChunkPart::Entity(_),
                    inventory: None,
                    ..
                }
            ) && !entity_bodies.contains_key(target)
        }) {
            return Err(ApplyFailure::InvalidAction);
        }
        let mut living = BTreeMap::new();
        for state in &self.player_living {
            let Target::Chunk {
                key,
                part: ChunkPart::Player(gid),
                inventory: None,
            } = state.target
            else {
                return Err(ApplyFailure::InvalidAction);
            };
            let crate::canonical_action::Value::Living(avatar) = &state.cell.value else {
                return Err(ApplyFailure::InvalidAction);
            };
            let Some(position) = avatar.avatar.as_ref() else {
                return Err(ApplyFailure::InvalidAction);
            };
            if key != self.chunk
                || gid != state.lease.gid
                || state.lease.incarnation == 0
                || state.spatial.gid != gid
                || state.spatial.incarnation != state.lease.incarnation
                || state.spatial.spatial_epoch != position.spatial_epoch
                || state.spatial.world != key.world
                || !state.spatial.vel.iter().all(|value| value.is_finite())
                || !state.spatial.head_yaw.is_finite()
                || state.spatial.pos.map(f64::to_bits) != position.position
                || state.spatial.yaw.to_bits() != position.yaw
                || state.spatial.head_yaw.to_bits() != position.head_yaw
                || state.spatial.pitch.to_bits() != position.pitch
                || state.spatial.on_ground != position.on_ground
                || state.spatial.tick != position.movement_tick
                || state.spatial.seq != position.movement_sequence
                || state
                    .spatial
                    .vel
                    .map(|value| (value * 1_000_000.0).round() as i64)
                    != avatar.combat.velocity_micro
                || !self.player_inventory_frontiers.iter().any(|(_, frontier)| {
                    frontier.lease == state.lease && frontier.accepted_chunk == self.chunk
                })
                || !avatar.belongs_to(crate::actor::ActorId::Player(gid))
                || position.world != key.world
                || (f64::from_bits(position.position[0]).floor() as i32).div_euclid(16) != key.x()
                || (f64::from_bits(position.position[2]).floor() as i32).div_euclid(16) != key.z()
                || projected.get(&state.target) != Some(&Some(state.cell.clone()))
                || !self
                    .ground_anchors
                    .iter()
                    .any(|(target, _)| *target == state.target)
                || living.insert(state.target, state).is_some()
            {
                return Err(ApplyFailure::InvalidAction);
            }
        }
        let mut absent = BTreeSet::new();
        for state in &self.player_avatar_absent {
            let Target::Chunk {
                key,
                part: ChunkPart::Player(gid),
                inventory: None,
            } = state.target
            else {
                return Err(ApplyFailure::InvalidAction);
            };
            if key != self.chunk
                || gid != state.lease.gid
                || state.lease.incarnation == 0
                || living.contains_key(&state.target)
                || projected.get(&state.target) != Some(&None)
                || !self
                    .ground_anchors
                    .iter()
                    .any(|(target, anchor)| *target == state.target && anchor.value.is_none())
                || !absent.insert(state.target)
            {
                return Err(ApplyFailure::InvalidAction);
            }
        }
        if projected.keys().any(|target| {
            matches!(
                target,
                Target::Chunk {
                    part: ChunkPart::Player(_),
                    inventory: None,
                    ..
                }
            ) && !living.contains_key(target)
                && !absent.contains(target)
        }) {
            return Err(ApplyFailure::InvalidAction);
        }
        if let Some(crate::canonical_action::Cell { value: crate::canonical_action::Value::ActorOccupancy(root), .. }) = &self.occupancy {
            let mut membership = BTreeMap::new();
            for (target, cell) in &self.entity_bodies {
                if let Target::Chunk { key, part: ChunkPart::Entity(id), inventory: None } = *target
                    && key == self.chunk
                    && let Some(crate::canonical_action::Cell { value: crate::canonical_action::Value::Entity(body), .. }) = cell
                {
                    membership.insert(crate::actor::ActorId::Entity(id),
                        (crate::actor_occupancy::OccupancyIdentity::Entity(body.birth.clone()), body.spatial_epoch, Some(body.owner)));
                }
            }
            for player in &self.player_living {
                let crate::canonical_action::Value::Living(living) = &player.cell.value else { return Err(ApplyFailure::InvalidAction); };
                let Some(avatar) = living.avatar.as_ref() else { return Err(ApplyFailure::InvalidAction); };
                membership.insert(crate::actor::ActorId::Player(player.lease.gid),
                    (crate::actor_occupancy::OccupancyIdentity::Player(player.lease), avatar.spatial_epoch, None));
            }
            let mut covered_home = std::collections::BTreeSet::new();
            for entry in root.entries.iter() {
                if !entry.covers_chunk(self.chunk) {
                    return Err(ApplyFailure::InvalidAction);
                }
                if entry.association != self.chunk {
                    continue;
                }
                let Some((identity, epoch, owner)) = membership.get(&entry.actor) else {
                    return Err(ApplyFailure::InvalidAction);
                };
                if identity != &entry.identity
                    || epoch != &entry.spatial_epoch
                    || owner.is_some_and(|owner| owner != entry.owner)
                    || !covered_home.insert(entry.actor)
                {
                    return Err(ApplyFailure::InvalidAction);
                }
            }
            if covered_home.len() != membership.len() {
                return Err(ApplyFailure::InvalidAction);
            }
        }
        let inventories = self
            .inventory
            .iter()
            .map(|state| (state.snapshot.target, &state.snapshot))
            .collect::<BTreeMap<_, _>>();
        if inventories.len() != self.inventory.len() {
            return Err(ApplyFailure::InvalidAction);
        }
        for (target, expected) in projected {
            let id = match target {
                crate::canonical_action::Target::Chunk {
                    inventory: Some(id),
                    ..
                }
                | crate::canonical_action::Target::Player {
                    inventory: Some(id),
                    ..
                } => id,
                _ => continue,
            };
            let address = match (target, id) {
                (
                    crate::canonical_action::Target::Player {
                        key,
                        lease: Some(lease),
                        ..
                    },
                    crate::canonical_action::InventoryId::Player { .. },
                ) => {
                    let lease = self
                        .player_inventory_frontiers
                        .iter()
                        .find(|(realm, frontier)| {
                            frontier.lease.key == key
                                && frontier.lease == lease
                                && *realm == id.action_realm()
                        })
                        .ok_or(ApplyFailure::InvalidAction)?
                        .1
                        .lease;
                    let addresses = [
                        crate::inventory::InventoryTarget::Player(lease.gid),
                        crate::inventory::InventoryTarget::LeasedPlayer {
                            key: lease.key.0,
                            player: lease.gid,
                            incarnation: lease.incarnation,
                        },
                    ];
                    let mut matching = addresses.into_iter().filter(|address| {
                        self.inventory.iter().any(|state| {
                            state.snapshot.target == *address && state.player_lease == Some(lease)
                        })
                    });
                    let address = matching.next().ok_or(ApplyFailure::InvalidAction)?;
                    if matching.next().is_some() {
                        return Err(ApplyFailure::InvalidAction);
                    }
                    address
                }
                (
                    crate::canonical_action::Target::Player {
                        key,
                        lease: Some(lease),
                        ..
                    },
                    crate::canonical_action::InventoryId::Virtual {
                        host,
                        kind,
                        session,
                        ..
                    },
                ) => {
                    let lease = self
                        .player_inventory_frontiers
                        .iter()
                        .find(|(realm, frontier)| {
                            frontier.lease.key == key
                                && frontier.lease == lease
                                && *realm == id.action_realm()
                        })
                        .ok_or(ApplyFailure::InvalidAction)?
                        .1
                        .lease;
                    let addresses = [
                        crate::inventory::InventoryTarget::Virtual {
                            host,
                            owner: crate::actor::ActorId::Player(lease.gid),
                            session,
                            kind,
                        },
                        crate::inventory::InventoryTarget::LeasedVirtual {
                            host,
                            key: lease.key.0,
                            player: lease.gid,
                            incarnation: lease.incarnation,
                            session,
                            kind,
                        },
                    ];
                    let mut matching = addresses.into_iter().filter(|address| {
                        self.inventory.iter().any(|state| {
                            state.snapshot.target == *address && state.player_lease == Some(lease)
                        })
                    });
                    let address = matching.next().ok_or(ApplyFailure::InvalidAction)?;
                    if matching.next().is_some() {
                        return Err(ApplyFailure::InvalidAction);
                    }
                    address
                }
                (
                    crate::canonical_action::Target::Chunk {
                        key,
                        part: crate::canonical_action::ChunkPart::Block(position),
                        ..
                    },
                    crate::canonical_action::InventoryId::Block { .. },
                ) => crate::inventory::InventoryTarget::Block {
                    chunk: key,
                    position,
                },
                (
                    crate::canonical_action::Target::Chunk {
                        key,
                        part: crate::canonical_action::ChunkPart::Block(position),
                        ..
                    },
                    crate::canonical_action::InventoryId::Virtual {
                        host,
                        kind,
                        session,
                        ..
                    },
                ) => crate::inventory::InventoryTarget::BlockVirtual {
                    host,
                    chunk: key,
                    position,
                    session,
                    kind,
                },
                (
                    crate::canonical_action::Target::Chunk {
                        part: crate::canonical_action::ChunkPart::Entity(entity),
                        ..
                    },
                    crate::canonical_action::InventoryId::Entity { .. },
                ) => crate::inventory::InventoryTarget::Entity(entity.into()),
                (
                    crate::canonical_action::Target::Chunk {
                        part: crate::canonical_action::ChunkPart::Entity(entity),
                        ..
                    },
                    crate::canonical_action::InventoryId::Virtual {
                        host,
                        kind,
                        session,
                        ..
                    },
                ) => crate::inventory::InventoryTarget::Virtual {
                    host,
                    owner: crate::actor::ActorId::Entity(entity),
                    session,
                    kind,
                },
                _ => return Err(ApplyFailure::InvalidAction),
            };
            let Some(snapshot) = inventories.get(&address) else {
                return Err(ApplyFailure::InvalidAction);
            };
            let (inv, slot) = id.location();
            let loc = crate::inventory::InvLoc { inv, slot };
            let current = snapshot
                .cells
                .iter()
                .find(|cell| cell.loc == loc)
                .cloned()
                .unwrap_or_else(|| crate::inventory::InventoryCell::empty(loc));
            if !matches!(
                expected,
                Some(crate::canonical_action::Cell {
                    value: crate::canonical_action::Value::Inventory(cell),
                    ..
                }) if cell == current
            ) {
                return Err(ApplyFailure::InvalidAction);
            }
        }
        if self.item_spatial.len() > self.entity_bodies.len() {
            return Err(ApplyFailure::InvalidAction);
        }
        let mut spatial_ids = std::collections::BTreeSet::new();
        for sample in &self.item_spatial {
            let Some(body) = self.entity_bodies.iter().find_map(|(_, cell)| match cell {
                Some(crate::canonical_action::Cell {
                    value: crate::canonical_action::Value::Entity(body),
                    ..
                }) if body.id == sample.id => Some(body),
                _ => None,
            }) else {
                return Err(ApplyFailure::InvalidAction);
            };
            if !spatial_ids.insert(sample.id)
                || sample.birth != body.birth
                || sample.epoch != body.spatial_epoch
                || sample.issuer.generation == 0
                || sample.spatial.world != self.chunk.world
                || sample
                    .spatial
                    .position
                    .iter()
                    .any(|value| !f64::from_bits(*value).is_finite())
                || !f32::from_bits(sample.spatial.yaw).is_finite()
                || !f32::from_bits(sample.spatial.pitch).is_finite()
                || sample.tick.is_after(self.snapshot_cut)
                || sample.tick.distance_since(body.birth.action.tick) >= 32768
                || !matches!(sample.epoch, crate::canonical_action::SpatialEpoch::Action(reference) if sample.tick.distance_since(reference.tick) < 32768)
                || (!matches!(body.payload, crate::entity_body::EntityPayload::Item(_)) && sample.issuer != body.owner)
            {
                return Err(ApplyFailure::InvalidAction);
            }
        }
        Ok(())
    }

    #[must_use]
    pub fn new(
        chunk: ChunkKey,
        holder: u16,
        snapshot_cut: crate::time::TickStamp,
        snapshot: Vec<u8>,
        pendings: Vec<PendingRef>,
        holders: Vec<u16>,
    ) -> Self {
        let mut sorted = holders;
        sorted.sort_unstable();
        sorted.dedup();
        Self {
            genesis: None,
            chunk,
            holder,
            snapshot_cut,
            subscription: None,
            occupancy: None,
            ground_frontier: None,
            player_inventory_frontiers: Vec::new(),
            snapshot,
            pendings,
            graphs: Vec::new(),
            ground_anchors: Vec::new(),
            finishes: Vec::new(),
            completion: None,
            holders: sorted,
            inventory: Vec::new(),
            player_living: Vec::new(),
            player_avatar_absent: Vec::new(),
            entity_bodies: Vec::new(),
            entity_history: Vec::new(),
            dependency_history: Vec::new(),
            item_spatial: Vec::new(),
        }
    }

    #[must_use]
    pub fn with_entity_history(
        mut self,
        history: Vec<crate::entity_history::EntityHistoryEvidence>,
    ) -> Self {
        self.entity_history = history;
        self
    }

    pub fn with_dependency_history(
        mut self,
        history: Vec<crate::entity_history::EntityHistoryEvidence>,
    ) -> Self {
        self.dependency_history = history;
        self
    }

    #[must_use]
    pub fn with_item_spatial(
        mut self,
        samples: Vec<crate::item_spatial::ItemSpatialSample>,
    ) -> Self {
        self.item_spatial = samples;
        self
    }

    #[must_use]
    pub fn with_entity_bodies(
        mut self,
        bodies: Vec<(
            crate::canonical_action::Target,
            Option<crate::canonical_action::Cell>,
        )>,
    ) -> Self {
        self.entity_bodies = bodies;
        self
    }

    #[must_use]
    pub fn with_inventory(mut self, inventory: Vec<crate::inventory::ChunkInventoryState>) -> Self {
        self.inventory = inventory;
        self
    }

    #[must_use]
    pub fn with_player_living(mut self, player_living: Vec<PlayerLivingGround>) -> Self {
        self.player_living = player_living;
        self
    }

    #[must_use]
    pub fn with_player_avatar_absent(
        mut self,
        player_avatar_absent: Vec<PlayerAvatarAbsentGround>,
    ) -> Self {
        self.player_avatar_absent = player_avatar_absent;
        self
    }

    #[must_use]
    pub fn with_graph_state(
        mut self,
        graphs: Vec<crate::action_graph::TickGraphSnapshot>,
        ground_anchors: Vec<(
            crate::canonical_action::Target,
            crate::action_graph::GroundAnchor,
        )>,
        finishes: Vec<(
            crate::realm_completion::PeerGeneration,
            crate::realm_completion::HolisticFinish,
        )>,
    ) -> Self {
        self.graphs = graphs;
        self.ground_anchors = ground_anchors;
        self.finishes = finishes;
        self
    }

    #[must_use]
    pub fn with_completion(
        mut self,
        completion: crate::realm_completion::RealmCompletionSnapshot,
    ) -> Self {
        self.completion = Some(completion);
        self
    }

    #[must_use]
    pub fn with_ground_frontier(
        mut self,
        frontier: Option<crate::action_graph::ChunkGroundFrontier>,
    ) -> Self {
        self.ground_frontier = frontier;
        self
    }

    #[must_use]
    pub fn with_player_inventory_frontiers(
        mut self,
        frontiers: Vec<(
            crate::action_graph::InventoryRealmId,
            crate::action_graph::PlayerInventoryGroundFrontier,
        )>,
    ) -> Self {
        self.player_inventory_frontiers = frontiers;
        self
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct InboundParcel {
    pub peer: u16,
    pub kind: StreamKind,
    pub bytes: Vec<u8>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct OutboundParcel {
    pub peer: u16,
    pub kind: StreamKind,
    pub bytes: Vec<u8>,
}

#[must_use]
pub fn lowest_holder(holders: &[u16]) -> Option<u16> {
    holders.iter().copied().min()
}

pub fn encode_announce(announce: &ChunkAnnounce) -> Result<Vec<u8>, CodecError> {
    postcard::to_allocvec(announce).map_err(|error| CodecError {
        message: format!("encode chunk announce: {error}"),
    })
}

pub fn decode_announce(bytes: &[u8]) -> Result<ChunkAnnounce, CodecError> {
    postcard::from_bytes(bytes).map_err(|error| CodecError {
        message: format!("decode chunk announce: {error}"),
    })
}

pub fn encode_subscription(control: &ChunkSubscriptionControl) -> Result<Vec<u8>, CodecError> {
    let encoded = postcard::to_allocvec(control).map_err(|error| CodecError {
        message: format!("encode chunk subscription: {error}"),
    })?;
    let mut bytes = Vec::with_capacity(4 + encoded.len());
    bytes.extend_from_slice(b"CSB1");
    bytes.extend_from_slice(&encoded);
    Ok(bytes)
}

pub fn decode_subscription(bytes: &[u8]) -> Result<ChunkSubscriptionControl, CodecError> {
    let payload = bytes.strip_prefix(b"CSB1").ok_or_else(|| CodecError {
        message: "invalid chunk subscription header".to_string(),
    })?;
    let control: ChunkSubscriptionControl =
        postcard::from_bytes(payload).map_err(|error| CodecError {
            message: format!("decode chunk subscription: {error}"),
        })?;
    if let ChunkSubscriptionControl::Start(start) = &control
        && !start.has_contiguous_cut()
    {
        return Err(CodecError {
            message: "noncontiguous chunk subscription cut".to_string(),
        });
    }
    Ok(control)
}

pub fn encode_request(fetch: &ChunkFetch) -> Result<Vec<u8>, CodecError> {
    postcard::to_allocvec(fetch).map_err(|error| CodecError {
        message: format!("encode chunk request: {error}"),
    })
}

pub fn decode_request(bytes: &[u8]) -> Result<ChunkFetch, CodecError> {
    postcard::from_bytes(bytes).map_err(|error| CodecError {
        message: format!("decode chunk request: {error}"),
    })
}

pub fn encode_payload(payload: &ChunkPayload) -> Result<Vec<u8>, CodecError> {
    let encoded = postcard::to_allocvec(payload).map_err(|error| CodecError {
        message: format!("encode chunk payload: {error}"),
    })?;
    if encoded.len() > 64 * 1024 * 1024 {
        return Err(CodecError {
            message: "chunk payload exceeds wire limit".to_string(),
        });
    }
    Ok(encoded)
}

pub fn decode_payload(bytes: &[u8]) -> Result<ChunkPayload, CodecError> {
    if bytes.len() > 64 * 1024 * 1024 {
        return Err(CodecError {
            message: "chunk payload exceeds wire limit".to_string(),
        });
    }
    postcard::from_bytes(bytes).map_err(|error| CodecError {
        message: format!("decode chunk payload: {error}"),
    })
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::chunks::{
        ChunkAdvert, ChunkDrop, ChunkSubscription, ChunkSubscriptionRequest, ChunkSubscriptionStart,
    };

    fn chunk(x: i32, z: i32) -> ChunkKey {
        ChunkKey::new(crate::protocol::WorldId::new(0, 7), x, z)
    }

    #[test]
    fn lowest_holder_picks_minimum() {
        assert_eq!(lowest_holder(&[3, 1, 2]), Some(1));
        assert_eq!(lowest_holder(&[]), None);
    }

    #[test]
    fn announce_acquire_round_trips() {
        let announce = ChunkAnnounce::Acquire(ChunkAdvert {
            holder: 2,
            chunk: chunk(1, 2),
        });
        let bytes = encode_announce(&announce).expect("encode");
        assert_eq!(decode_announce(&bytes).expect("decode"), announce);
    }

    #[test]
    fn announce_release_round_trips() {
        let announce = ChunkAnnounce::Release(ChunkDrop {
            holder: 4,
            chunk: chunk(7, 7),
        });
        let bytes = encode_announce(&announce).expect("encode");
        assert_eq!(decode_announce(&bytes).expect("decode"), announce);
    }

    #[test]
    fn subscription_control_is_distinct_from_holder_announces() {
        let subscription = ChunkSubscription {
            chunk: chunk(7, 9),
            subscriber: 4,
            incarnation: 3,
            subscription_id: 8,
            from_tick: crate::time::TickStamp(65_535),
        };
        for control in [
            ChunkSubscriptionControl::Request(ChunkSubscriptionRequest {
                chunk: subscription.chunk,
                subscriber: subscription.subscriber,
                incarnation: subscription.incarnation,
                subscription_id: subscription.subscription_id,
                from_tick: subscription.from_tick,
            }),
            ChunkSubscriptionControl::Start(ChunkSubscriptionStart::new(
                ChunkSubscriptionRequest {
                    chunk: subscription.chunk,
                    subscriber: subscription.subscriber,
                    incarnation: subscription.incarnation,
                    subscription_id: subscription.subscription_id,
                    from_tick: subscription.from_tick,
                },
                1,
                5,
                crate::time::TickStamp(65_534),
                [9; 32],
            )),
        ] {
            let bytes = encode_subscription(&control).unwrap();
            assert_eq!(decode_subscription(&bytes).unwrap(), control);
            assert!(decode_announce(&bytes).is_err());
            assert!(decode_subscription(&bytes[1..]).is_err());
        }
        let malformed = ChunkSubscriptionControl::Start(ChunkSubscriptionStart {
            subscription,
            provider: 1,
            provider_incarnation: 5,
            snapshot_cut: crate::time::TickStamp(3),
            snapshot_hash: [9; 32],
        });
        assert!(decode_subscription(&encode_subscription(&malformed).unwrap()).is_err());
    }

    #[test]
    fn payload_holder_list_round_trips() {
        let payload = ChunkPayload::new(
            chunk(5, 6),
            2,
            crate::time::TickStamp(7),
            vec![9, 8, 7],
            vec![PendingRef {
                state: crate::completion::ChunkPendingTick {
                    tick: crate::time::TickStamp(7),
                    chunk: chunk(5, 6),
                    holders: vec![crate::identity::ServerId(2)],
                    generations: Vec::new(),
                    finishes: Vec::new(),
                    batches: Vec::new(),
                },
            }],
            vec![3, 1, 2, 1],
        )
        .with_ground_frontier(Some(crate::action_graph::ChunkGroundFrontier {
            epoch: 3,
            settled_through: crate::time::TickStamp(6),
        }));
        let bytes = encode_payload(&payload).expect("encode");
        let back = decode_payload(&bytes).expect("decode");
        assert_eq!(back, payload);
        assert_eq!(back.holders, vec![1, 2, 3]);
    }

    #[test]
    fn pending_transfer_needs_authenticated_generation_for_each_finish() {
        let target = chunk(5, 6);
        let mut payload = ChunkPayload::new(
            target,
            2,
            crate::time::TickStamp(7),
            Vec::new(),
            vec![PendingRef {
                state: crate::completion::ChunkPendingTick {
                    tick: crate::time::TickStamp(7),
                    chunk: target,
                    holders: vec![crate::identity::ServerId(2)],
                    generations: vec![(crate::identity::ServerId(2), 9)],
                    finishes: vec![(
                        crate::identity::ServerId(2),
                        crate::completion::TickFinish {
                            tick: crate::time::TickStamp(7),
                            chunks: Vec::new(),
                        },
                    )],
                    batches: Vec::new(),
                },
            }],
            vec![2],
        )
        .with_completion(crate::realm_completion::RealmCompletionSnapshot {
            requirements: Vec::new(),
            players: Vec::new(),
            actions: Vec::new(),
            finishes: Vec::new(),
            conflicting_finishes: Vec::new(),
        });
        assert!(payload.validate_transfer_evidence().is_ok());
        payload.ground_anchors.push((
            crate::canonical_action::Target::Chunk {
                key: target,
                part: crate::canonical_action::ChunkPart::Player(
                    crate::identity::GlobalPlayerId::new(
                        crate::identity::ServerId(2),
                        crate::identity::PlayerSlot(1),
                    ),
                ),
                inventory: None,
            },
            crate::action_graph::GroundAnchor {
                settled_through: crate::time::TickStamp(6),
                value: None,
            },
        ));
        assert!(payload.validate_transfer_evidence().is_err());
        payload.ground_anchors.clear();
        payload.pendings.push(payload.pendings[0].clone());
        assert!(payload.validate_transfer_evidence().is_err());
        payload.pendings.pop();
        payload.pendings[0].state.tick = crate::time::TickStamp(8);
        assert!(payload.validate_transfer_evidence().is_err());
        payload.pendings[0].state.tick = crate::time::TickStamp(7);
        payload.finishes.push((
            crate::realm_completion::PeerGeneration {
                server: crate::identity::ServerId(2),
                generation: 10,
            },
            crate::realm_completion::HolisticFinish {
                full_source_slot: u64::from((crate::time::TickStamp(7)).0),
                acquisition_prefix: None,
                tick: crate::time::TickStamp(7),
                issued: Default::default(),
            },
        ));
        assert!(payload.validate_transfer_evidence().is_err());
        payload.finishes.clear();
        payload.pendings[0].state.generations.clear();
        assert!(payload.validate_transfer_evidence().is_err());
    }

    #[test]
    fn transfer_evidence_rejects_orphan_anchor_and_future_frontier() {
        let chunk = chunk(5, 6);
        let mut payload = ChunkPayload::new(
            chunk,
            2,
            crate::time::TickStamp(7),
            Vec::new(),
            Vec::new(),
            vec![2],
        )
        .with_completion(crate::realm_completion::RealmCompletionSnapshot {
            requirements: Vec::new(),
            players: Vec::new(),
            actions: Vec::new(),
            finishes: Vec::new(),
            conflicting_finishes: Vec::new(),
        });
        payload.ground_anchors.push((
            crate::canonical_action::Target::Chunk {
                key: chunk,
                part: crate::canonical_action::ChunkPart::Block(crate::protocol::BlockPos {
                    x: 80,
                    y: 64,
                    z: 96,
                }),
                inventory: None,
            },
            crate::action_graph::GroundAnchor {
                settled_through: crate::time::TickStamp(6),
                value: None,
            },
        ));
        assert!(payload.validate_transfer_evidence().is_err());
        payload.ground_anchors.clear();
        payload.ground_frontier = Some(crate::action_graph::ChunkGroundFrontier {
            epoch: 1,
            settled_through: crate::time::TickStamp(6),
        });
        payload.ground_anchors.push((
            crate::canonical_action::Target::Chunk {
                key: chunk,
                part: crate::canonical_action::ChunkPart::Entity(crate::protocol::GlobalEntityId {
                    origin: crate::identity::ServerId(2),
                    local_id: 1,
                    generation: 1,
                }),
                inventory: None,
            },
            crate::action_graph::GroundAnchor {
                settled_through: crate::time::TickStamp(6),
                value: None,
            },
        ));
        assert!(payload.validate_transfer_evidence().is_err());
        let target = payload.ground_anchors[0].0;
        payload.entity_bodies.push((target, None));
        assert!(payload.validate_transfer_evidence().is_ok());
        let encoded = encode_payload(&payload).unwrap();
        assert_eq!(
            decode_payload(&encoded).unwrap().entity_bodies,
            payload.entity_bodies
        );
        payload.entity_bodies.push((target, None));
        assert!(payload.validate_transfer_evidence().is_err());
        payload.entity_bodies.clear();
        payload.ground_anchors.clear();
        payload.ground_frontier = Some(crate::action_graph::ChunkGroundFrontier {
            epoch: 1,
            settled_through: crate::time::TickStamp(8),
        });
        assert!(payload.validate_transfer_evidence().is_err());
        payload.ground_frontier = None;
        payload.completion.as_mut().unwrap().finishes.push((
            crate::realm_completion::PeerGeneration {
                server: crate::identity::ServerId(2),
                generation: 1,
            },
            crate::realm_completion::HolisticFinish {
                full_source_slot: u64::from((crate::time::TickStamp(8)).0),
                acquisition_prefix: None,
                tick: crate::time::TickStamp(8),
                issued: Default::default(),
            },
        ));
        assert!(payload.validate_transfer_evidence().is_err());
    }

    #[test]
    fn entity_transfer_requires_exact_accepted_body_history() {
        use crate::canonical_action::{ActionId, ActionRef, Cell, ChunkPart, Target, Value};
        use crate::entity_body::{
            DroppedItem, EntityBirth, EntityBody, EntityPayload, EntitySpatial,
        };
        use crate::identity::ServerId;
        use crate::realm_completion::{PeerGeneration, RealmCompletionSnapshot};
        use crate::time::TickStamp;
        let chunk = chunk(0, 0);
        let id = crate::protocol::GlobalEntityId {
            origin: ServerId(2),
            local_id: 42,
            generation: 3,
        };
        let peer = PeerGeneration {
            server: ServerId(2),
            generation: 9,
        };
        let target = Target::Chunk {
            key: chunk,
            part: ChunkPart::Entity(id),
            inventory: None,
        };
        let cell = Cell {
            revision: 4,
            value: Value::Entity((EntityBody { mount: None,
                tags: Default::default(),
                tags_revision: 1,
                relation_versions: Default::default(),
                passengers: Default::default(),
                id,
                spatial_epoch: crate::canonical_action::SpatialEpoch::Action(ActionRef {
                    tick: TickStamp(2),
                    id: ActionId {
                        actor: crate::actor::ActorId::Peer(ServerId(2)),
                        incarnation: 9,
                        sequence: 1,
                        family: 8,
                    },
                }),
                birth: EntityBirth {
                    action: ActionRef {
                        tick: TickStamp(2),
                        id: ActionId {
                            actor: crate::actor::ActorId::Peer(ServerId(2)),
                            incarnation: 9,
                            sequence: 1,
                            family: 8,
                        },
                    },
                    source: peer,
                    uuid: uuid::Uuid::from_u128(42),
                },
                owner: peer,
                kind: 1,
                spatial: EntitySpatial {
                    world: chunk.world,
                    position: [1.0_f64.to_bits(), 64.0_f64.to_bits(), 1.0_f64.to_bits()],
                    velocity_micro: [0; 3],
                    yaw: 0.0_f32.to_bits(),
                    pitch: 0.0_f32.to_bits(),
                    on_ground: false,
                },
                payload: EntityPayload::Item(DroppedItem {
                    item: 5,
                    count: 2,
                    components: vec![9],
                    age: 4,
                    last_lifetime_tick: Some(TickStamp(5)),
                    pickup_delay: 0,
                    health_milli: 5000,
                    never_despawn: false,
                    never_pickup: false,
                    fire_immune: false,
                }),
            }).into()),
        };
        let mut payload =
            ChunkPayload::new(chunk, 2, TickStamp(7), Vec::new(), Vec::new(), vec![2])
                .with_completion(RealmCompletionSnapshot {
                    requirements: Vec::new(),
                    players: Vec::new(),
                    actions: Vec::new(),
                    finishes: Vec::new(),
                    conflicting_finishes: Vec::new(),
                })
                .with_entity_bodies(vec![(target, Some(cell.clone()))]);
        payload.ground_frontier = Some(crate::action_graph::ChunkGroundFrontier {
            epoch: 9,
            settled_through: TickStamp(6),
        });
        payload.ground_anchors.push((
            target,
            crate::action_graph::GroundAnchor {
                settled_through: TickStamp(6),
                value: Some(cell.clone()),
            },
        ));
        assert!(payload.validate_transfer_evidence().is_ok());
        assert_eq!(
            decode_payload(&encode_payload(&payload).unwrap())
                .unwrap()
                .entity_bodies,
            payload.entity_bodies
        );
        payload.entity_bodies[0].1.as_mut().unwrap().revision += 1;
        assert!(payload.validate_transfer_evidence().is_err());
        let mut future = cell.clone();
        let Value::Entity(body) = &mut future.value else {
            unreachable!();
        };
        let EntityPayload::Item(item) = &mut body.payload else {
            unreachable!();
        };
        item.last_lifetime_tick = Some(TickStamp(8));
        payload.entity_bodies[0].1 = Some(future.clone());
        payload.ground_anchors[0].1.value = Some(future);
        assert!(payload.validate_transfer_evidence().is_err());
        payload.ground_anchors[0].1.value = Some(cell.clone());
        payload.entity_bodies[0].1 = Some(cell.clone());
        let Value::Entity(body) = &mut payload.entity_bodies[0].1.as_mut().unwrap().value else {
            unreachable!();
        };
        let EntityPayload::Item(item) = &mut body.payload else {
            unreachable!();
        };
        item.count += 1;
        assert!(payload.validate_transfer_evidence().is_err());
        payload.entity_bodies[0].1 = None;
        assert!(payload.validate_transfer_evidence().is_err());
        payload.entity_bodies.clear();
        assert!(payload.validate_transfer_evidence().is_err());
    }

    #[test]
    fn folded_player_inventory_transfer_checks_accepted_slot() {
        use crate::action_graph::{GroundAnchor, InventoryRealmId, PlayerInventoryGroundFrontier};
        use crate::canonical_action::{
            Cell, InventoryId, LivePlayerLease, PlayerKey, Target, Value,
        };
        use crate::identity::{GlobalPlayerId, PlayerSlot, ServerId};
        use crate::inventory::{
            ChunkInventoryState, InvLoc, InventoryCell, InventoryStateSnapshot, InventoryTarget,
        };

        let chunk = chunk(5, 6);
        let lease = LivePlayerLease {
            key: PlayerKey(uuid::Uuid::from_u128(91)),
            gid: GlobalPlayerId::new(ServerId(2), PlayerSlot(4)),
            incarnation: 3,
        };
        let slot = InvLoc::new(0, 4);
        let item = InventoryCell {
            loc: slot,
            item: 1,
            count: 1,
            nbt: Vec::new(),
        };
        let target = Target::Player {
            key: lease.key,
            lease: Some(lease),
            inventory: Some(InventoryId::Player { inv: 0, slot: 4 }),
        };
        let mut payload = ChunkPayload::new(
            chunk,
            2,
            crate::time::TickStamp(50),
            Vec::new(),
            Vec::new(),
            vec![2],
        )
        .with_inventory(vec![ChunkInventoryState {
            menu_admission: None,
            snapshot: InventoryStateSnapshot {
                target: InventoryTarget::Player(lease.gid),
                revision: 2,
                cells: vec![item.clone()],
            },
            chunks: vec![chunk],
            beacon: None,
            player_lease: Some(lease),
        }])
        .with_player_inventory_frontiers(
            (crate::inventory::INV_MAIN..=crate::inventory::INV_SELECTED)
                .map(|inv| {
                    (
                        InventoryRealmId::Player(inv),
                        PlayerInventoryGroundFrontier {
                            lease,
                            accepted_chunk: chunk,
                            domain_epoch: lease.incarnation,
                            settled_through: crate::time::TickStamp(50),
                        },
                    )
                })
                .collect(),
        )
        .with_graph_state(
            Vec::new(),
            vec![(
                target,
                GroundAnchor {
                    settled_through: crate::time::TickStamp(50),
                    value: Some(Cell {
                        revision: 1,
                        value: Value::Inventory(item.clone()),
                    }),
                },
            )],
            Vec::new(),
        )
        .with_completion(crate::realm_completion::RealmCompletionSnapshot {
            requirements: Vec::new(),
            players: Vec::new(),
            actions: Vec::new(),
            finishes: Vec::new(),
            conflicting_finishes: Vec::new(),
        });
        payload.validate_transfer_evidence().unwrap();
        let actor = crate::actor::ActorId::Player(lease.gid);
        let living_target = Target::Chunk {
            key: chunk,
            part: crate::canonical_action::ChunkPart::Player(lease.gid),
            inventory: None,
        };
        let living_cell = Cell {
            revision: 2,
            value: Value::Living((crate::item_action::ActorLivingState {
                permanent_attribute_modifiers: Default::default(),
                menu: Default::default(),
                environment: Default::default(),
                tags: Default::default(),
                tags_revision: 0,
                advancements: Default::default(),
                stand: None,
                mob: None,
                merchant: None,
                simulation_tick: None,
                effects: Vec::new(),
                hidden_effects: BTreeMap::new(),
                property_versions: std::collections::BTreeMap::new(),
                food: crate::item_action::ActorFoodState {
                    exhaustion_milli: 0,
                    tick_timer: 0,
                    actor,
                    food: 20,
                    saturation_milli: 5_000,
                    revision: 0,
                },
                combat: crate::item_action::ActorCombatState {
                    actor,
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
                cooldowns: Default::default(),
                avatar: Some(crate::item_action::PlayerAvatarState {
                    stance: Default::default(),
                    mount: None,
                    relation_versions: Default::default(),
                    experience: Default::default(),
                    statistics: Default::default(),
                    presentation: Default::default(),
                    spatial_epoch: crate::canonical_action::SpatialEpoch::Admission {
                        lease,
                        admitted_at: crate::time::TickStamp(50),
                    },
                    world: chunk.world,
                    position: [81.0_f64.to_bits(), 80.0_f64.to_bits(), 97.0_f64.to_bits()],
                    yaw: 0.0_f32.to_bits(),
                    head_yaw: 0.0_f32.to_bits(),
                    pitch: 0.0_f32.to_bits(),
                    on_ground: true,
                    movement_tick: crate::time::TickStamp(50),
                    movement_sequence: crate::identity::PlayerSeq(1),
                    game_mode: 0,
                    fall_distance: 0.0_f32.to_bits(),
                    on_fire: false,
                }),
            }).into()),
        };
        let mut with_living = payload.clone();
        with_living.ground_frontier = Some(crate::action_graph::ChunkGroundFrontier {
            epoch: 1,
            settled_through: crate::time::TickStamp(50),
        });
        with_living.ground_anchors.push((
            living_target,
            GroundAnchor {
                settled_through: crate::time::TickStamp(50),
                value: Some(living_cell.clone()),
            },
        ));
        assert!(with_living.validate_transfer_evidence().is_err());
        with_living.player_living.push(PlayerLivingGround {
            lease,
            admitted_at: crate::time::TickStamp(50),
            target: living_target,
            cell: living_cell,
            spatial: crate::protocol::PosUpdate {
                spatial_epoch: crate::canonical_action::SpatialEpoch::Admission {
                    lease,
                    admitted_at: crate::time::TickStamp(50),
                },
                gid: lease.gid,
                incarnation: lease.incarnation,
                seq: crate::identity::PlayerSeq(1),
                tick: crate::time::TickStamp(50),
                world: chunk.world,
                pos: [81.0, 80.0, 97.0],
                vel: [0.0; 3],
                yaw: 0.0,
                head_yaw: 0.0,
                pitch: 0.0,
                on_ground: true,
            },
        });
        with_living.validate_transfer_evidence().unwrap();
        let mut destination_absent = with_living.clone();
        destination_absent.player_living.clear();
        destination_absent
            .ground_anchors
            .iter_mut()
            .find(|(target, _)| *target == living_target)
            .unwrap()
            .1
            .value = None;
        assert!(destination_absent.validate_transfer_evidence().is_err());
        destination_absent
            .player_avatar_absent
            .push(PlayerAvatarAbsentGround {
                lease,
                admitted_at: crate::time::TickStamp(50),
                target: living_target,
            });
        destination_absent.validate_transfer_evidence().unwrap();
        decode_payload(&encode_payload(&destination_absent).unwrap())
            .unwrap()
            .validate_transfer_evidence()
            .unwrap();
        let mut backdated_actor = with_living.clone();
        backdated_actor.player_living[0].admitted_at = crate::time::TickStamp(51);
        assert!(backdated_actor.validate_transfer_evidence().is_err());
        let transferred = decode_payload(&encode_payload(&with_living).unwrap()).unwrap();
        transferred.validate_transfer_evidence().unwrap();
        assert_eq!(transferred.player_living, with_living.player_living);
        let mut wrong_chunk = with_living.clone();
        if let Value::Living(state) = &mut wrong_chunk.player_living[0].cell.value {
            state.avatar.as_mut().unwrap().position[0] = 1.0_f64.to_bits();
        }
        assert!(wrong_chunk.validate_transfer_evidence().is_err());
        let mut overlapping = payload.clone();
        let next = LivePlayerLease {
            incarnation: lease.incarnation + 1,
            ..lease
        };
        overlapping.inventory.push(ChunkInventoryState {
            menu_admission: None,
            snapshot: InventoryStateSnapshot {
                target: InventoryTarget::LeasedPlayer {
                    key: next.key.0,
                    player: next.gid,
                    incarnation: next.incarnation,
                },
                revision: 0,
                cells: vec![InventoryCell {
                    item: 2,
                    ..item.clone()
                }],
            },
            chunks: vec![chunk],
            beacon: None,
            player_lease: Some(next),
        });
        overlapping.player_inventory_frontiers.extend(
            (crate::inventory::INV_MAIN..=crate::inventory::INV_SELECTED).map(|inv| {
                (
                    InventoryRealmId::Player(inv),
                    PlayerInventoryGroundFrontier {
                        lease: next,
                        accepted_chunk: chunk,
                        domain_epoch: next.incarnation,
                        settled_through: crate::time::TickStamp(50),
                    },
                )
            }),
        );
        overlapping.ground_anchors.push((
            Target::Player {
                key: next.key,
                lease: Some(next),
                inventory: Some(InventoryId::Player { inv: 0, slot: 4 }),
            },
            GroundAnchor {
                settled_through: crate::time::TickStamp(50),
                value: Some(Cell {
                    revision: 0,
                    value: Value::Inventory(InventoryCell {
                        item: 2,
                        ..item.clone()
                    }),
                }),
            },
        ));
        overlapping.validate_transfer_evidence().unwrap();
        overlapping.inventory.push(ChunkInventoryState {
            menu_admission: None,
            snapshot: InventoryStateSnapshot {
                target: InventoryTarget::LeasedVirtual {
                    host: ServerId(2),
                    key: next.key.0,
                    player: next.gid,
                    incarnation: next.incarnation,
                    session: 0,
                    kind: 0x100,
                },
                revision: 0,
                cells: Vec::new(),
            },
            chunks: vec![chunk],
            beacon: None,
            player_lease: Some(next),
        });
        overlapping.player_inventory_frontiers.push((
            InventoryRealmId::Virtual {
                host: ServerId(2),
                kind: 0x100,
                session: 0,
                inv: 0,
            },
            PlayerInventoryGroundFrontier {
                lease: next,
                accepted_chunk: chunk,
                domain_epoch: next.incarnation,
                settled_through: crate::time::TickStamp(50),
            },
        ));
        overlapping.validate_transfer_evidence().unwrap();
        payload.inventory[0].snapshot.target = InventoryTarget::LeasedPlayer {
            key: lease.key.0,
            player: lease.gid,
            incarnation: lease.incarnation,
        };
        payload.validate_transfer_evidence().unwrap();
        payload.inventory[0].snapshot.target = InventoryTarget::LeasedPlayer {
            key: lease.key.0,
            player: lease.gid,
            incarnation: lease.incarnation + 1,
        };
        assert!(payload.validate_transfer_evidence().is_err());
        payload.inventory[0].snapshot.target = InventoryTarget::Player(lease.gid);
        payload.ground_anchors[0].0 = Target::Player {
            key: lease.key,
            lease: None,
            inventory: Some(InventoryId::Player { inv: 0, slot: 4 }),
        };
        assert!(payload.validate_transfer_evidence().is_err());
        payload.ground_anchors[0].0 = target;
        let missing_realm = payload.player_inventory_frontiers.remove(3);
        assert!(payload.validate_transfer_evidence().is_err());
        payload.player_inventory_frontiers.insert(3, missing_realm);
        payload.player_inventory_frontiers.push((
            InventoryRealmId::Virtual {
                host: ServerId(2),
                kind: 0x100,
                session: 0,
                inv: 0,
            },
            payload.player_inventory_frontiers[0].1,
        ));
        assert!(payload.validate_transfer_evidence().is_err());
        payload.player_inventory_frontiers.pop();
        payload.ground_anchors[0].1.settled_through = crate::time::TickStamp(49);
        payload.validate_transfer_evidence().unwrap();
        payload.ground_anchors[0].1.settled_through = crate::time::TickStamp(50);
        payload.inventory[0].snapshot.cells[0].count = 2;
        assert!(payload.validate_transfer_evidence().is_err());
        payload.inventory[0].snapshot.cells[0] = item;
        payload.player_inventory_frontiers[0].1.settled_through = crate::time::TickStamp(49);
        assert!(payload.validate_transfer_evidence().is_err());
    }

    #[test]
    fn payload_carries_virtual_input_and_beacon_revision() {
        use crate::actor::ActorId;
        use crate::identity::{GlobalPlayerId, PlayerSlot, ServerId};
        use crate::inventory::{
            BeaconMenuState, ChunkInventoryState, INV_MAIN, InvLoc, InventoryCell,
            InventoryStateSnapshot, InventoryTarget,
        };
        let chunk = chunk(5, 6);
        let virtual_target = InventoryTarget::Virtual {
            host: ServerId(2),
            owner: ActorId::Player(GlobalPlayerId::new(ServerId(2), PlayerSlot(4))),
            session: 17,
            kind: 1,
        };
        let beacon_target = InventoryTarget::Block {
            chunk,
            position: crate::protocol::BlockPos {
                x: 80,
                y: 80,
                z: 96,
            },
        };
        let inventory = vec![
            ChunkInventoryState {
            menu_admission: None,
                snapshot: InventoryStateSnapshot {
                    target: virtual_target,
                    revision: 7,
                    cells: vec![InventoryCell {
                        loc: InvLoc::new(INV_MAIN, 0),
                        item: 4,
                        count: 2,
                        nbt: vec![1, 2],
                    }],
                },
                chunks: vec![chunk],
                beacon: None,
                player_lease: None,
            },
            ChunkInventoryState {
            menu_admission: None,
                snapshot: InventoryStateSnapshot {
                    target: beacon_target,
                    revision: 9,
                    cells: vec![InventoryCell::empty(InvLoc::new(INV_MAIN, 0))],
                },
                chunks: vec![chunk],
                beacon: Some(BeaconMenuState {
                    revision: 11,
                    levels: 1,
                    primary_effect: Some(1),
                    secondary_effect: None,
                }),
                player_lease: None,
            },
        ];
        let payload = ChunkPayload::new(
            chunk,
            2,
            crate::time::TickStamp(7),
            vec![9],
            Vec::new(),
            vec![2],
        )
        .with_inventory(inventory.clone());
        assert_eq!(
            decode_payload(&encode_payload(&payload).unwrap())
                .unwrap()
                .inventory,
            inventory
        );
    }

    #[test]
    fn request_round_trips() {
        let fetch = ChunkFetch {
            chunk: chunk(0, 0),
            from: 7,
        };
        let bytes = encode_request(&fetch).expect("encode");
        assert_eq!(decode_request(&bytes).expect("decode"), fetch);
    }
}
