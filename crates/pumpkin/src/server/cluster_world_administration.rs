use std::collections::{BTreeMap, BTreeSet, HashMap};
use std::time::{Duration, Instant};

use pumpkin_cluster::actor::ActorId;
use pumpkin_cluster::canonical_action::{
    Action, ActionClass, ActionId, ActionKind, ChunkPart, ClientOrigin, Condition, Effect,
    Expected, Stakeholder, Target, UndoProcedure, Value,
};
use pumpkin_cluster::completion::ActionKey;
use pumpkin_cluster::identity::ServerId;
use pumpkin_cluster::protocol::ChunkKey;
use pumpkin_cluster::time::TickStamp;
use pumpkin_cluster::world_delta::{BlockDelta, WORLD_DELTA_MAX_EDITS, chunk_of_block};
use tokio::sync::mpsc;

use super::Server;
use super::cluster_chunk_state::OwnedChunkStore;
use super::cluster_world_delta::LocalAdministrativeIntent;

fn administrative_required_chunks(
    intent: &LocalAdministrativeIntent,
) -> Option<BTreeSet<ChunkKey>> {
    let mut required = BTreeSet::new();
    for edit in &intent.edits {
        let center = chunk_of_block(intent.world, edit.pos.x, edit.pos.z);
        for z in -2..=2 {
            for x in -2..=2 {
                required.insert(ChunkKey::new(
                    center.world,
                    center.x().checked_add(x)?,
                    center.z().checked_add(z)?,
                ));
                if required.len() > 8192 {
                    return None;
                }
            }
        }
    }
    Some(required)
}

fn append_block_entity_transition(
    chunk: ChunkKey,
    position: pumpkin_cluster::protocol::BlockPos,
    old_state: pumpkin_data::BlockStateId,
    new_state: pumpkin_data::BlockStateId,
    old_entity: Option<pumpkin_cluster::canonical_action::Cell>,
    conditions: &mut Vec<Condition>,
    effects: &mut Vec<Effect>,
) -> Option<()> {
    let old_type = pumpkin_data::BlockState::from_id(old_state).block_entity_type;
    let new_type = pumpkin_data::BlockState::from_id(new_state).block_entity_type;
    if old_type == new_type {
        return Some(());
    }
    let target = Target::Chunk {
        key: chunk,
        part: ChunkPart::BlockEntity(position),
        inventory: None,
    };
    conditions.push(Condition {
        target,
        expected: old_entity
            .clone()
            .map_or(Expected::Missing, Expected::Exact),
    });
    if new_type == u16::MAX {
        if old_entity.is_some() {
            effects.push(Effect::Delete { target });
        }
    } else {
        let position =
            pumpkin_util::math::position::BlockPos::new(position.x, position.y, position.z);
        let bytes =
            pumpkin_world::level::cluster_default_block_entity_document(new_state, position)?;
        effects.push(Effect::Put {
            target,
            value: Value::BlockEntity(bytes),
        });
    }
    Some(())
}

pub struct AdministrativeOwner {
    pub receiver: mpsc::Receiver<LocalAdministrativeIntent>,
    pending: Vec<LocalAdministrativeIntent>,
    action_groups: HashMap<(TickStamp, ActionId), u64>,
    groups: HashMap<u64, AdministrativeGroup>,
    next_group: u64,
    sequence: u16,
    issued_tick: Option<TickStamp>,
    issued_count: usize,
    chunk_ready_tx: mpsc::Sender<pumpkin_world::level::OwnerChunkReady>,
    chunk_ready_rx: mpsc::Receiver<pumpkin_world::level::OwnerChunkReady>,
    chunk_needs: HashMap<ChunkKey, AdministrativeChunkNeed>,
    next_chunk_lease: u64,
}

struct AdministrativeChunkNeed {
    lease: u64,
    ready_generation: Option<u64>,
    release_after: Option<Instant>,
}

struct AdministrativeGroup {
    origin: Option<u64>,
    tick: TickStamp,
    chunk: ChunkKey,
    positions: Vec<(ChunkKey, pumpkin_cluster::protocol::BlockPos, u16)>,
    required_chunks: BTreeSet<ChunkKey>,
    remaining: BTreeSet<(TickStamp, ActionId)>,
    accepted: bool,
}

impl AdministrativeOwner {
    pub fn attach(server: &Server) -> Self {
        let (chunk_ready_tx, chunk_ready_rx) = mpsc::channel(1_024);
        Self {
            receiver: super::cluster_world_delta::attach_administrative_owner(server),
            pending: Vec::new(),
            action_groups: HashMap::new(),
            groups: HashMap::new(),
            next_group: 0,
            sequence: 0,
            issued_tick: None,
            issued_count: 0,
            chunk_ready_tx,
            chunk_ready_rx,
            chunk_needs: HashMap::new(),
            next_chunk_lease: 1_u64 << 63,
        }
    }

    fn service_chunk_needs(&mut self, server: &Server) {
        while let Ok(ready) = self.chunk_ready_rx.try_recv() {
            let chunk = ChunkKey::new(
                pumpkin_cluster::protocol::WorldId::new(ready.dimension, ready.world_key),
                ready.position.x,
                ready.position.y,
            );
            if let Some(need) = self.chunk_needs.get_mut(&chunk)
                && need.lease == ready.lease
            {
                need.ready_generation = Some(ready.publication_generation);
            }
        }
        let mut required = BTreeSet::new();
        for intent in &self.pending {
            if let Some(chunks) = administrative_required_chunks(intent) {
                required.extend(chunks);
            }
        }
        for group in self.groups.values() {
            required.extend(group.required_chunks.iter().copied());
        }
        let now = Instant::now();
        let Some(route) = server.cluster_chunk_need_route.load_full() else {
            return;
        };
        self.chunk_needs.retain(|chunk, need| {
            if required.contains(chunk) {
                need.release_after = None;
                return true;
            }
            let due = *need
                .release_after
                .get_or_insert(now + Duration::from_secs(5));
            if now < due {
                return true;
            }
            route
                .try_send(super::cluster::ChunkNeed::Release {
                    chunk: *chunk,
                    lease: need.lease,
                })
                .is_err()
        });
    }

    fn ensure_chunk_need(&mut self, server: &Server, chunk: ChunkKey) -> bool {
        if let Some(need) = self.chunk_needs.get_mut(&chunk) {
            need.release_after = None;
            return need.ready_generation.is_some();
        }
        if self.chunk_needs.len() >= 8_192 {
            return false;
        }
        let Some(route) = server.cluster_chunk_need_route.load_full() else {
            return false;
        };
        self.next_chunk_lease = self.next_chunk_lease.wrapping_add(1) | (1_u64 << 63);
        let lease = self.next_chunk_lease;
        if route
            .try_send(super::cluster::ChunkNeed::Acquire {
                chunk,
                lease,
                ready: Some(self.chunk_ready_tx.clone()),
            })
            .is_err()
        {
            return false;
        }
        self.chunk_needs.insert(
            chunk,
            AdministrativeChunkNeed {
                lease,
                ready_generation: None,
                release_after: None,
            },
        );
        false
    }

    pub fn prepare_canonical<S: pumpkin_cluster::canonical_action::ActionState>(
        &mut self,
        server: &Server,
        blocks: &OwnedChunkStore,
        state: &S,
        intent: LocalAdministrativeIntent,
        tick: TickStamp,
        destination_ready: impl Fn(ChunkKey) -> bool,
    ) -> Option<Vec<Action>> {
        let local = ServerId(server.advanced_config.cluster.server_id);
        if self.issued_tick != Some(tick) {
            self.issued_tick = Some(tick);
            self.issued_count = 0;
        }
        let Some(incarnation) = server.cluster_peer_incarnations.load().get(&local).copied() else {
            self.publish_rejected_intent(server, &intent, tick, local, 0);
            return Some(Vec::new());
        };
        if intent.edits.len() > 4096 || self.groups.len() >= 64 {
            self.publish_rejected_intent(server, &intent, tick, local, incarnation);
            return Some(Vec::new());
        }
        let mut grouped = BTreeMap::<ChunkKey, Vec<_>>::new();
        let mut unique_positions = std::collections::BTreeSet::new();
        for edit in &intent.edits {
            if pumpkin_data::BlockStateId::new(edit.new_state).is_none()
                || !unique_positions.insert(edit.pos)
            {
                self.publish_rejected_intent(server, &intent, tick, local, incarnation);
                return Some(Vec::new());
            }
            grouped
                .entry(chunk_of_block(intent.world, edit.pos.x, edit.pos.z))
                .or_default()
                .push(*edit);
        }
        if grouped.keys().any(|chunk| {
            !server.worlds.load().iter().any(|world| {
                world.level.cluster_world_key == chunk.world.key
                    && world.level.cluster_dimension == chunk.world.dimension
            })
        }) {
            self.publish_rejected_intent(server, &intent, tick, local, incarnation);
            return Some(Vec::new());
        }
        let page_count = intent.edits.len().div_ceil(WORLD_DELTA_MAX_EDITS);
        if page_count > 256 || self.issued_count.saturating_add(page_count) > 8192 {
            self.publish_rejected_intent(server, &intent, tick, local, incarnation);
            return Some(Vec::new());
        }
        let Some(required_chunks) = administrative_required_chunks(&intent) else {
            self.publish_rejected_intent(server, &intent, tick, local, incarnation);
            return Some(Vec::new());
        };
        let mut all_ready = true;
        for chunk in required_chunks.iter().copied() {
            all_ready &= self.ensure_chunk_need(server, chunk)
                && blocks.contains(chunk)
                && destination_ready(chunk);
        }
        if !all_ready {
            if self.pending.len() < 128 {
                self.pending.push(intent);
            } else {
                self.publish_rejected_intent(server, &intent, tick, local, incarnation);
            }
            return None;
        }
        let ordered = grouped
            .into_iter()
            .flat_map(|(chunk, edits)| edits.into_iter().map(move |edit| (chunk, edit)))
            .collect::<Vec<_>>();
        let mut actions = Vec::<Action>::new();
        let worlds = server.worlds.load_full();
        let Some(runtime_world) = worlds.iter().find(|world| {
            world.level.cluster_world_key == intent.world.key
                && world.level.cluster_dimension == intent.world.dimension
        }) else {
            self.publish_rejected_intent(server, &intent, tick, local, incarnation);
            return Some(Vec::new());
        };
        let mut draft = pumpkin_cluster::actor_action_scope::ActorActionDraft::new(
            ActorId::Peer(local),
            incarnation,
            tick,
            4096,
            16384,
        );
        for page in ordered.chunks(WORLD_DELTA_MAX_EDITS) {
            let mut conditions = Vec::with_capacity(page.len());
            let mut effects = Vec::with_capacity(page.len());
            let mut stakeholders = BTreeSet::new();
            let mut resolution = crate::block::neighbor::BlockCallbackResolution::default();
            for (chunk, edit) in page {
                stakeholders.insert(Stakeholder::Chunk(*chunk));
                let target = Target::Chunk {
                    key: *chunk,
                    part: ChunkPart::Block(edit.pos),
                    inventory: None,
                };
                let Ok(old) = blocks.block(*chunk, edit.pos) else {
                    self.publish_rejected_intent(server, &intent, tick, local, incarnation);
                    return Some(Vec::new());
                };
                conditions.push(Condition {
                    target,
                    expected: Expected::BlockState(old.as_u16()),
                });
                effects.push(Effect::Put {
                    target,
                    value: Value::Block(edit.new_state),
                });
                let new_state = pumpkin_data::BlockStateId::new(edit.new_state)
                    .expect("validated administrative block state");
                resolution.writes.push((
                    pumpkin_util::math::position::BlockPos::new(edit.pos.x, edit.pos.y, edit.pos.z),
                    old,
                    new_state,
                ));
                if append_block_entity_transition(
                    *chunk,
                    edit.pos,
                    old,
                    new_state,
                    blocks.block_entity(false, *chunk, edit.pos).cloned(),
                    &mut conditions,
                    &mut effects,
                )
                .is_none()
                {
                    self.publish_rejected_intent(server, &intent, tick, local, incarnation);
                    return Some(Vec::new());
                }
            }
            let id = ActionId {
                actor: ActorId::Peer(local),
                incarnation,
                sequence: self.sequence,
                family: 6,
            };
            self.sequence = self.sequence.wrapping_add(1);
            self.issued_count += 1;
            let mut action = Action {
                edit_authority: None,
                presentation: Vec::new(),
                id,
                tick,
                class: ActionClass::Undoable,
                chunk_transition: false,
                actor_chunk: None,
                player_leases: Vec::new(),
                player_chunks: Vec::new(),
                stakeholders: stakeholders.into_iter().collect(),
                dependencies: Vec::new(),
                conditions,
                effects,
                counterfactual_conditions: Vec::new(),
                counterfactual_effects: Vec::new(),
                semantic_intent: None,
                factual_result: None,
                undo: vec![UndoProcedure::RestoreWrittenTargets],
                kind: ActionKind::World,
                client_origin: ClientOrigin::None,
            };
            if super::cluster_world_callbacks::append_resolution_light(
                &draft.view(state),
                runtime_world,
                intent.world,
                &mut action,
                &resolution,
            )
            .is_err()
                || action.validate().is_err()
                || draft.issue(state, action).is_err()
            {
                self.publish_rejected_intent(server, &intent, tick, local, incarnation);
                return Some(Vec::new());
            }
            actions.push(draft.actions().last().unwrap().clone());
        }
        if let Some(first) = actions.first() {
            let Some(Stakeholder::Chunk(group_chunk)) = first.stakeholders.first().copied() else {
                self.publish_rejected_intent(server, &intent, tick, local, incarnation);
                return Some(Vec::new());
            };
            let group_id = self.next_group;
            self.next_group = self.next_group.wrapping_add(1);
            let remaining = actions
                .iter()
                .map(|action| (action.tick, action.id))
                .collect();
            for action in &actions {
                self.action_groups
                    .insert((action.tick, action.id), group_id);
            }
            self.groups.insert(
                group_id,
                AdministrativeGroup {
                    origin: intent.origin,
                    tick,
                    chunk: group_chunk,
                    positions: intent
                        .edits
                        .iter()
                        .map(|edit| {
                            (
                                chunk_of_block(intent.world, edit.pos.x, edit.pos.z),
                                edit.pos,
                                edit.new_state,
                            )
                        })
                        .collect(),
                    required_chunks,
                    remaining,
                    accepted: true,
                },
            );
        }
        Some(actions)
    }

    fn publish_rejected_intent(
        &mut self,
        server: &Server,
        intent: &LocalAdministrativeIntent,
        tick: TickStamp,
        local: ServerId,
        incarnation: u64,
    ) {
        let sequence = self.sequence;
        self.sequence = self.sequence.wrapping_add(1);
        self.issued_count = self.issued_count.saturating_add(1);
        if let Some(first) = intent.edits.first() {
            let chunk = chunk_of_block(intent.world, first.pos.x, first.pos.z);
            super::cluster_world_delta::publish_administrative(
                server,
                super::cluster_world_delta::AdministrativePublication {
                    origin: intent.origin,
                    tick,
                    action: ActionKey {
                        actor: ActorId::Peer(local),
                        incarnation,
                        sequence,
                        family: 0x86,
                    },
                    chunk,
                    accepted: false,
                    ground: Vec::new(),
                },
            );
        }
    }

    pub fn retry_pending<S: pumpkin_cluster::canonical_action::ActionState>(
        &mut self,
        server: &Server,
        blocks: &OwnedChunkStore,
        state: &S,
        tick: TickStamp,
        destination_ready: impl Fn(ChunkKey) -> bool,
    ) -> Vec<Action> {
        self.service_chunk_needs(server);
        let pending = core::mem::take(&mut self.pending);
        pending
            .into_iter()
            .filter_map(|intent| {
                self.prepare_canonical(server, blocks, state, intent, tick, &destination_ready)
            })
            .flatten()
            .collect()
    }

    pub fn publish_canonical_ground(
        &mut self,
        server: &Server,
        blocks: &OwnedChunkStore,
        action: &Action,
        accepted: bool,
    ) {
        let Some(group) = self.finish_page(action.tick, action.id, accepted) else {
            return;
        };
        let expected_positions = group.positions.len();
        let observed = group
            .positions
            .into_iter()
            .filter_map(|(chunk, position, expected)| {
                let state = blocks.ground_block(chunk, position).ok()?.as_u16();
                Some((
                    BlockDelta {
                        pos: position,
                        old_state: state,
                        new_state: state,
                    },
                    state == expected,
                ))
            })
            .collect::<Vec<_>>();
        let accepted = group.accepted
            && observed.len() == expected_positions
            && observed.iter().all(|(_, matched)| *matched);
        let ground = observed.into_iter().map(|(delta, _)| delta).collect();
        super::cluster_world_delta::publish_administrative(
            server,
            super::cluster_world_delta::AdministrativePublication {
                origin: group.origin,
                tick: group.tick,
                action: ActionKey::canonical(action),
                chunk: group.chunk,
                accepted,
                ground,
            },
        );
    }

    fn finish_page(
        &mut self,
        tick: TickStamp,
        id: ActionId,
        accepted: bool,
    ) -> Option<AdministrativeGroup> {
        let group_id = self.action_groups.remove(&(tick, id))?;
        let group = self.groups.get_mut(&group_id)?;
        group.accepted &= accepted;
        group.remaining.remove(&(tick, id));
        if group.remaining.is_empty() {
            self.groups.remove(&group_id)
        } else {
            None
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use pumpkin_cluster::protocol::WorldId;

    #[test]
    fn administrative_block_entity_type_change_is_one_action_leg() {
        let chunk = ChunkKey::new(WorldId::new(0, 0), 0, 0);
        let position = pumpkin_cluster::protocol::BlockPos { x: 1, y: 64, z: 2 };
        let chest = pumpkin_world::level::cluster_default_block_entity_document(
            pumpkin_data::Block::CHEST.default_state.id,
            pumpkin_util::math::position::BlockPos::new(1, 64, 2),
        )
        .unwrap();
        let prior = pumpkin_cluster::canonical_action::Cell {
            revision: 7,
            value: Value::BlockEntity(chest),
        };
        let mut conditions = Vec::new();
        let mut effects = Vec::new();
        append_block_entity_transition(
            chunk,
            position,
            pumpkin_data::Block::CHEST.default_state.id,
            pumpkin_data::Block::FURNACE.default_state.id,
            Some(prior.clone()),
            &mut conditions,
            &mut effects,
        )
        .unwrap();
        assert_eq!(conditions.len(), 1);
        assert_eq!(conditions[0].expected, Expected::Exact(prior.clone()));
        assert_eq!(effects.len(), 1);
        let Effect::Put {
            value: Value::BlockEntity(bytes),
            ..
        } = &effects[0]
        else {
            panic!("type change must replace the old block entity");
        };
        assert!(
            pumpkin_world::level::cluster_block_entity_document_type_matches(
                pumpkin_data::Block::FURNACE.default_state.id,
                bytes,
            )
        );
        conditions.clear();
        effects.clear();
        append_block_entity_transition(
            chunk,
            position,
            pumpkin_data::Block::CHEST.default_state.id,
            pumpkin_data::Block::STONE.default_state.id,
            Some(prior),
            &mut conditions,
            &mut effects,
        )
        .unwrap();
        assert!(matches!(effects.as_slice(), [Effect::Delete { .. }]));
    }

    #[test]
    fn bulk_origin_waits_for_every_page_even_when_results_arrive_out_of_order() {
        let (_, receiver) = mpsc::channel(1);
        let (chunk_ready_tx, chunk_ready_rx) = mpsc::channel(1);
        let tick = TickStamp(12);
        let mut owner = AdministrativeOwner {
            receiver,
            pending: Vec::new(),
            action_groups: HashMap::new(),
            groups: HashMap::new(),
            next_group: 1,
            sequence: 0,
            issued_tick: Some(tick),
            issued_count: 0,
            chunk_ready_tx,
            chunk_ready_rx,
            chunk_needs: HashMap::new(),
            next_chunk_lease: 1_u64 << 63,
        };
        let chunk = ChunkKey::new(WorldId::new(0, 1), 0, 0);
        let ids = [3, 1, 2].map(|sequence| ActionId {
            actor: ActorId::Peer(ServerId(0)),
            incarnation: 4,
            sequence,
            family: 6,
        });
        for id in ids {
            owner.action_groups.insert((tick, id), 0);
        }
        owner.groups.insert(
            0,
            AdministrativeGroup {
                origin: Some(99),
                tick,
                chunk,
                required_chunks: BTreeSet::from([chunk]),
                positions: Vec::new(),
                remaining: ids.into_iter().map(|id| (tick, id)).collect(),
                accepted: true,
            },
        );
        assert!(owner.finish_page(tick, ids[2], true).is_none());
        assert!(owner.finish_page(tick, ids[0], false).is_none());
        let final_group = owner.finish_page(tick, ids[1], true).unwrap();
        assert!(!final_group.accepted);
        assert!(owner.finish_page(tick, ids[1], true).is_none());
    }
}
