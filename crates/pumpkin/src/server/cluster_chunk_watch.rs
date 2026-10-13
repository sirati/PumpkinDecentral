use std::{
    collections::{BTreeMap, BTreeSet, HashMap, HashSet, VecDeque},
    sync::{Arc, Weak},
};

use pumpkin_cluster::{
    canonical_action::Value, controller::ViewIntent, identity::GlobalPlayerId, protocol::ChunkKey,
};
use pumpkin_protocol::codec::var_int::VarInt;
use pumpkin_util::version::JavaMinecraftVersion;
use tokio::sync::oneshot;

use super::{
    Server, cluster::ChunkNeedLease, cluster_chunk_presentation::ChunkPresentation,
    cluster_chunk_state::OwnedChunkStore,
};
use crate::{entity::player::Player, net::ClientPlatform};

pub(crate) struct WatchActivationRequest {
    pub actor: GlobalPlayerId,
    pub session: u64,
    pub player: Arc<Player>,
    pub view: ViewIntent,
    pub sent: BTreeMap<ChunkKey, Arc<ChunkPresentation>>,
    pub need: ChunkNeedLease,
    pub respond: oneshot::Sender<Result<WatchActivated, WatchActivationError>>,
}

pub(crate) enum WatchCommand {
    Activate(WatchActivationRequest),
    Refresh(WatchActivationRequest),
    ReleaseInitial {
        actor: GlobalPlayerId,
        session: u64,
    },
    RetainPrepared {
        actor: GlobalPlayerId,
        install_generation: u64,
        need: ChunkNeedLease,
        delivered: Vec<ChunkKey>,
        retained_until: u64,
    },
    Retire {
        actor: GlobalPlayerId,
        session: u64,
        install_generation: u64,
        retained: Option<super::cluster_presence::RetainedDeparture>,
    },
}

#[derive(Clone)]
pub(crate) struct WatchPublication {
    pub session: u64,
    pub view: ViewIntent,
    pub player: Weak<Player>,
    pub delivered: BTreeSet<ChunkKey>,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) struct WatchActivated {
    pub actor: GlobalPlayerId,
    pub session: u64,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) enum WatchActivationError {
    Invalid,
    Departed,
    Duplicate,
    Capacity,
}

struct PendingWatch {
    request: WatchActivationRequest,
    chunks: Vec<ChunkKey>,
    chunk: usize,
    offset: usize,
    observing: bool,
    be_positions: Option<Vec<pumpkin_cluster::protocol::BlockPos>>,
    be_offset: usize,
}

struct RetiringWatch {
    needs: Vec<ChunkNeedLease>,
    covered: BTreeSet<ChunkKey>,
    coverage_complete: bool,
    queued: bool,
    required: BTreeSet<ChunkKey>,
    expires_millis: u64,
    failed_reported: bool,
}

impl RetiringWatch {
    fn add_need(&mut self, need: ChunkNeedLease) {
        self.covered.extend(need.chunks().iter().copied());
        self.needs.push(need);
        self.coverage_complete = self.required.is_subset(&self.covered);
    }
}

#[derive(Default)]
pub(crate) struct OwnedChunkWatches {
    pending: VecDeque<PendingWatch>,
    active: HashMap<GlobalPlayerId, (u64, Vec<ChunkNeedLease>)>,
    publication: HashMap<GlobalPlayerId, WatchPublication>,
    live_retirement_slots: HashSet<(GlobalPlayerId, u64)>,
    retiring: HashMap<(GlobalPlayerId, u64), RetiringWatch>,
    retirement_work: VecDeque<(GlobalPlayerId, u64)>,
    rejected_needs: Vec<(u64, ChunkNeedLease)>,
    prepared: HashMap<(GlobalPlayerId, u64), RetiringWatch>,
    prepared_work: VecDeque<(GlobalPlayerId, u64)>,
}

impl OwnedChunkWatches {
    pub fn has_capacity(&self) -> bool {
        self.pending.len() < 256
    }

    pub fn command(&mut self, command: WatchCommand) {
        let refresh = matches!(&command, WatchCommand::Refresh(_));
        match command {
            WatchCommand::ReleaseInitial { actor, session } => {
                if self
                    .active
                    .get(&actor)
                    .is_some_and(|(current, _)| *current == session)
                {
                    self.active.remove(&actor);
                }
            }
            WatchCommand::RetainPrepared {
                actor,
                install_generation,
                need,
                delivered,
                retained_until,
            } => {
                let key = (actor, install_generation);
                let prepared = self.prepared.entry(key).or_insert_with(|| RetiringWatch {
                    needs: Vec::new(),
                    covered: BTreeSet::new(),
                    coverage_complete: true,
                    queued: false,
                    required: BTreeSet::new(),
                    expires_millis: retained_until,
                    failed_reported: false,
                });
                prepared.required.extend(delivered);
                prepared.required.extend(need.chunks().iter().copied());
                prepared.add_need(need);
                prepared.expires_millis = prepared.expires_millis.min(retained_until);
                if !prepared.coverage_complete && !prepared.queued {
                    prepared.queued = true;
                    self.prepared_work.push_back(key);
                }
            }
            WatchCommand::Retire {
                actor,
                session,
                install_generation,
                retained,
            } => {
                self.retire(actor, session, install_generation, retained);
            }
            WatchCommand::Activate(request) | WatchCommand::Refresh(request) => {
                if let Some(retiring) = self.retiring.get_mut(&(request.actor, request.session)) {
                    retiring.add_need(request.need);
                    let _ = request.respond.send(Err(WatchActivationError::Departed));
                    return;
                }
                if !self.has_capacity() {
                    self.reject_watch(request, WatchActivationError::Capacity);
                    return;
                }
                let supplied = request.sent.keys().copied().collect::<BTreeSet<_>>();
                let needed = request
                    .need
                    .chunks()
                    .iter()
                    .copied()
                    .collect::<BTreeSet<_>>();
                if supplied != needed
                    || supplied.is_empty()
                    || request.view.actor != request.actor
                    || supplied.iter().any(|chunk| {
                        chunk.world != request.view.world
                            || chunk
                                .x()
                                .checked_mul(16)
                                .and_then(|x| x.checked_add(15))
                                .is_none()
                            || chunk
                                .z()
                                .checked_mul(16)
                                .and_then(|z| z.checked_add(15))
                                .is_none()
                    })
                    || request.view.owner_install_generation
                        != request
                            .player
                            .cluster_owner_install_generation
                            .load(std::sync::atomic::Ordering::Acquire)
                    || request.player.cluster_gid() != Some(request.actor)
                {
                    self.reject_watch(request, WatchActivationError::Invalid);
                    return;
                }
                if self
                    .pending
                    .iter()
                    .any(|pending| pending.request.actor == request.actor)
                    || (!refresh && self.publication.contains_key(&request.actor))
                    || (refresh
                        && !self
                            .publication
                            .get(&request.actor)
                            .is_some_and(|current| current.session == request.session))
                {
                    self.reject_watch(request, WatchActivationError::Duplicate);
                    return;
                }
                let key = (request.actor, request.session);
                if !self.live_retirement_slots.contains(&key)
                    && self.live_retirement_slots.len() + self.retiring.len() >= 65_536
                {
                    self.reject_watch(request, WatchActivationError::Capacity);
                    return;
                }
                self.live_retirement_slots.insert(key);
                let chunks = supplied.into_iter().collect();
                self.pending.push_back(PendingWatch {
                    request,
                    chunks,
                    chunk: 0,
                    offset: 0,
                    observing: false,
                    be_positions: None,
                    be_offset: 0,
                });
            }
        }
    }

    fn reject_watch(&mut self, request: WatchActivationRequest, error: WatchActivationError) {
        self.rejected_needs
            .push((retirement_now_millis().saturating_add(30_000), request.need));
        let _ = request.respond.send(Err(error));
    }

    fn retire(
        &mut self,
        actor: GlobalPlayerId,
        session: u64,
        install_generation: u64,
        retained: Option<super::cluster_presence::RetainedDeparture>,
    ) {
        let key = (actor, session);
        let mut needs = Vec::new();
        let mut prepared_required = BTreeSet::new();
        let mut prepared_deadline = None;
        if let Some(prepared) = self.prepared.remove(&(actor, install_generation)) {
            needs.extend(prepared.needs);
            prepared_required = prepared.required;
            prepared_deadline = Some(prepared.expires_millis);
        }
        let mut remaining = VecDeque::new();
        while let Some(pending) = self.pending.pop_front() {
            if (pending.request.actor, pending.request.session) == key {
                needs.push(pending.request.need);
                let _ = pending
                    .request
                    .respond
                    .send(Err(WatchActivationError::Departed));
            } else {
                remaining.push_back(pending);
            }
        }
        self.pending = remaining;
        if self
            .active
            .get(&actor)
            .is_some_and(|(current, _)| *current == session)
        {
            needs.extend(self.active.remove(&actor).unwrap().1);
        }
        let mut required: BTreeSet<ChunkKey> = retained
            .as_ref()
            .map(|retained| retained.chunks.iter().copied().collect())
            .unwrap_or_default();
        required.extend(prepared_required);
        if let Some(publication) = self
            .publication
            .get(&actor)
            .filter(|entry| entry.session == session)
        {
            required.extend(publication.delivered.iter().copied());
        }
        let mut expires_millis = retained.map_or_else(
            || retirement_now_millis().saturating_add(30_000),
            |retained| retained.retained_until,
        );
        if let Some(deadline) = prepared_deadline {
            expires_millis = expires_millis.min(deadline);
        }
        let reserved = self.live_retirement_slots.remove(&key);
        if !reserved && !self.retiring.contains_key(&key) && needs.is_empty() {
            return;
        }
        let retiring = self.retiring.entry(key).or_insert_with(|| RetiringWatch {
            needs: Vec::new(),
            covered: BTreeSet::new(),
            coverage_complete: true,
            queued: false,
            required: BTreeSet::new(),
            expires_millis,
            failed_reported: false,
        });
        retiring.required.extend(required);
        retiring.coverage_complete = retiring.required.is_subset(&retiring.covered);
        for need in needs {
            retiring.add_need(need);
        }
        if !retiring.coverage_complete && !retiring.queued {
            retiring.queued = true;
            self.retirement_work.push_back(key);
        }
        retiring.expires_millis = retiring.expires_millis.min(expires_millis);
    }

    fn retire_need(
        &mut self,
        actor: GlobalPlayerId,
        session: u64,
        install_generation: u64,
        need: ChunkNeedLease,
    ) {
        self.retire(actor, session, install_generation, None);
        self.retiring
            .get_mut(&(actor, session))
            .expect("activated watch retirement slot")
            .add_need(need);
    }

    fn complete_retirement_coverage(&mut self, now: u64) {
        self.prepared
            .retain(|_, prepared| !prepared.coverage_complete || now < prepared.expires_millis);
        self.prepared_work
            .retain(|key| self.prepared.contains_key(key));
        self.rejected_needs.retain(|(deadline, _)| now < *deadline);
        let mut expired = Vec::new();
        for (key, retiring) in &self.retiring {
            if retiring.coverage_complete {
                if self
                    .publication
                    .get(&key.0)
                    .is_some_and(|entry| entry.session == key.1)
                {
                    self.publication.remove(&key.0);
                }
                if now >= retiring.expires_millis {
                    expired.push(*key);
                }
            }
        }
        if !expired.is_empty() {
            for key in &expired {
                self.retiring.remove(key);
            }
            let expired = expired.into_iter().collect::<HashSet<_>>();
            self.retirement_work.retain(|key| !expired.contains(key));
        }
    }

    fn acquire_retirement_coverage(
        server: &Server,
        key: (GlobalPlayerId, u64),
        retiring: &mut RetiringWatch,
    ) {
        let missing = retiring
            .required
            .difference(&retiring.covered)
            .copied()
            .collect::<Vec<_>>();
        let worlds = missing
            .iter()
            .map(|chunk| chunk.world)
            .collect::<BTreeSet<_>>();
        for world_id in worlds {
            let worlds = server.worlds.load();
            let Some(world) = worlds
                .iter()
                .find(|world| super::cluster_world_delta::world_id(world) == world_id)
            else {
                if !retiring.failed_reported {
                    tracing::error!(actor = ?key.0, session = key.1, ?world_id, "cluster retired watch world unavailable");
                    retiring.failed_reported = true;
                }
                continue;
            };
            let positions = missing
                .iter()
                .filter(|chunk| chunk.world == world_id)
                .map(|chunk| pumpkin_util::math::vector2::Vector2::new(chunk.x(), chunk.z()))
                .collect::<Vec<_>>();
            let lease = server
                .cluster_lobby_session_generation
                .fetch_add(1, std::sync::atomic::Ordering::Relaxed);
            if let Some(need) =
                super::cluster::try_require_lobby_chunks(server, world, &positions, lease)
            {
                retiring.add_need(need);
            }
        }
    }

    fn advance_retirements(&mut self, server: &Server) {
        let now = retirement_now_millis();
        for _ in 0..self.retirement_work.len().min(8) {
            let key = self.retirement_work.pop_front().unwrap();
            let Some(retiring) = self.retiring.get_mut(&key) else {
                continue;
            };
            retiring.queued = false;
            Self::acquire_retirement_coverage(server, key, retiring);
            if !retiring.coverage_complete {
                retiring.queued = true;
                self.retirement_work.push_back(key);
            }
        }
        for _ in 0..self.prepared_work.len().min(8) {
            let key = self.prepared_work.pop_front().unwrap();
            let Some(prepared) = self.prepared.get_mut(&key) else {
                continue;
            };
            prepared.queued = false;
            Self::acquire_retirement_coverage(server, key, prepared);
            if !prepared.coverage_complete {
                prepared.queued = true;
                self.prepared_work.push_back(key);
            }
        }
        self.complete_retirement_coverage(now);
    }

    pub fn update_view(&mut self, view: ViewIntent) {
        if let Some(current) = self.publication.get_mut(&view.actor) {
            if current.view.owner_install_generation != view.owner_install_generation {
                return;
            }
            if view.tick == current.view.tick || view.tick.is_after(current.view.tick) {
                current.view = view;
                let cylinder = pumpkin_world::cylindrical_chunk_iterator::Cylindrical::new(
                    pumpkin_util::math::vector2::Vector2::new(view.center.x, view.center.z),
                    view.radius,
                );
                current.delivered.retain(|chunk| {
                    chunk.world == view.world && cylinder.is_within_distance(chunk.x(), chunk.z())
                });
            }
        }
    }

    pub fn advance(
        &mut self,
        server: &Server,
        blocks: &OwnedChunkStore,
        admitted: impl Fn(GlobalPlayerId, u64) -> Option<bool>,
    ) {
        self.advance_retirements(server);
        let mut budget = 65_536_usize;
        let mut chunk_budget = 16_usize;
        for _ in 0..self.pending.len().min(32) {
            let Some(mut pending) = self.pending.pop_front() else {
                break;
            };
            if pending.request.player.client.closed()
                || pending
                    .request
                    .player
                    .cluster_departing
                    .load(std::sync::atomic::Ordering::Acquire)
            {
                let actor = pending.request.actor;
                let session = pending.request.session;
                let _ = pending
                    .request
                    .respond
                    .send(Err(WatchActivationError::Departed));
                self.retire_need(
                    actor,
                    session,
                    pending.request.view.owner_install_generation,
                    pending.request.need,
                );
                continue;
            }
            match admitted(pending.request.actor, pending.request.session) {
                None => {
                    self.pending.push_back(pending);
                    continue;
                }
                Some(false) => {
                    let actor = pending.request.actor;
                    let session = pending.request.session;
                    let _ = pending
                        .request
                        .respond
                        .send(Err(WatchActivationError::Invalid));
                    self.retire_need(
                        actor,
                        session,
                        pending.request.view.owner_install_generation,
                        pending.request.need,
                    );
                    continue;
                }
                Some(true) => {}
            }
            if !pending.observing {
                let view = self
                    .publication
                    .get(&pending.request.actor)
                    .map(|current| current.view)
                    .filter(|current| current.tick.is_after(pending.request.view.tick))
                    .unwrap_or(pending.request.view);
                let mut delivered = self
                    .publication
                    .get(&pending.request.actor)
                    .map(|current| current.delivered.clone())
                    .unwrap_or_default();
                delivered.extend(pending.chunks.iter().copied());
                let cylinder = pumpkin_world::cylindrical_chunk_iterator::Cylindrical::new(
                    pumpkin_util::math::vector2::Vector2::new(view.center.x, view.center.z),
                    view.radius,
                );
                delivered.retain(|chunk| {
                    chunk.world == view.world && cylinder.is_within_distance(chunk.x(), chunk.z())
                });
                self.publication.insert(
                    pending.request.actor,
                    WatchPublication {
                        session: pending.request.session,
                        view,
                        player: Arc::downgrade(&pending.request.player),
                        delivered,
                    },
                );
                server
                    .cluster_chunk_watches
                    .store(Arc::new(self.publication.clone()));
                pending.observing = true;
            }
            while pending.chunk < pending.chunks.len() && budget > 0 && chunk_budget > 0 {
                let key = pending.chunks[pending.chunk];
                let presentations = server.cluster_chunk_presentations.load_full();
                let Some(current) = presentations.get(&key) else {
                    break;
                };
                let old = &pending.request.sent[&key];
                if old.blocks.min_y() != current.blocks.min_y()
                    || old.blocks.block_palettes().len() != current.blocks.block_palettes().len()
                {
                    break;
                }
                let total = old.blocks.block_palettes().len() * 4096;
                if Arc::ptr_eq(old, current)
                    || old.image.document().root_tag.get_list("sections")
                        == current.image.document().root_tag.get_list("sections")
                {
                    pending.offset = total;
                }
                while pending.offset < total && budget > 0 {
                    let offset = pending.offset;
                    let section = offset / 4096;
                    let local = offset % 4096;
                    let x = local / 256;
                    let z = local / 16 % 16;
                    let y = local % 16;
                    let before = old.blocks.block_palettes()[section].get(x, y, z);
                    let after = current.blocks.block_palettes()[section].get(x, y, z);
                    if before != after {
                        let Some(x) = key
                            .x()
                            .checked_mul(16)
                            .and_then(|base| base.checked_add(x as i32))
                        else {
                            break;
                        };
                        let Some(z) = key
                            .z()
                            .checked_mul(16)
                            .and_then(|base| base.checked_add(z as i32))
                        else {
                            break;
                        };
                        let position = pumpkin_util::math::position::BlockPos::new(
                            x,
                            old.blocks.min_y() + (section * 16 + y) as i32,
                            z,
                        );
                        pending.request.player.try_enqueue_packet_editioned(
                            &pumpkin_protocol::java::client::play::CBlockUpdate::new(
                                position,
                                i32::from(after.as_u16()).into(),
                            ),
                            &pumpkin_protocol::bedrock::client::CUpdateBlock::new(
                                position,
                                pumpkin_data::BlockState::to_be_network_id(after) as u32,
                            ),
                        );
                    }
                    pending.offset += 1;
                    budget -= 1;
                }
                if pending.offset != total {
                    break;
                }
                let positions = pending.be_positions.get_or_insert_with(|| {
                    current
                        .block_entities
                        .iter()
                        .map(|(position, _)| *position)
                        .collect()
                });
                while pending.be_offset < positions.len() && budget > 0 {
                    let position = positions[pending.be_offset];
                    pending.be_offset += 1;
                    budget -= 1;
                    let Some(cell) = blocks.block_entity(false, key, position) else {
                        continue;
                    };
                    if old
                        .block_entities
                        .iter()
                        .any(|(at, previous)| *at == position && previous.value == cell.value)
                    {
                        continue;
                    }
                    let Value::BlockEntity(bytes) = &cell.value else {
                        continue;
                    };
                    let Ok(state) = blocks.block(key, position) else {
                        continue;
                    };
                    let position = pumpkin_util::math::position::BlockPos::new(
                        position.x, position.y, position.z,
                    );
                    let java = pumpkin_protocol::java::client::play::CBlockEntityData::new(
                        position,
                        VarInt::new(i32::from(state.to_state().block_entity_type)),
                        bytes.clone().into_boxed_slice(),
                    );
                    let mut reader = pumpkin_nbt::deserializer::NbtReadHelperJava::new(
                        std::io::Cursor::new(bytes.as_slice()),
                    );
                    let actor =
                        pumpkin_nbt::Nbt::read_unnamed(&mut reader)
                            .ok()
                            .and_then(|document| {
                                super::cluster_world_callbacks::bedrock_block_actor(
                                    state,
                                    position,
                                    &document.root_tag,
                                )
                            });
                    if let Some(actor) = actor {
                        pending.request.player.try_enqueue_packet_editioned(
                            &java,
                            &pumpkin_protocol::bedrock::client::CBlockActorData::new(
                                position, actor,
                            ),
                        );
                    } else {
                        pending.request.player.try_send_client_packet(&java);
                    }
                }
                if pending.be_offset != positions.len() {
                    break;
                }
                if !current.admission_lighting_valid && current.lighting.is_none() {
                    break;
                }
                if let ClientPlatform::Java(client) = pending.request.player.client.as_ref()
                    && client.version.load() >= JavaMinecraftVersion::V_1_14
                    && !Arc::ptr_eq(old, current)
                {
                    let Ok(light) = crate::net::java::chunk_data::owned::presentation_light(
                        current,
                        client.version.load(),
                    ) else {
                        break;
                    };
                    pending.request.player.try_send_client_packet(
                        &pumpkin_protocol::java::client::play::CLightUpdate::new(
                            key.x().into(),
                            key.z().into(),
                            light,
                        ),
                    );
                }
                pending.chunk += 1;
                pending.offset = 0;
                pending.be_positions = None;
                pending.be_offset = 0;
                chunk_budget -= 1;
            }
            if pending.chunk == pending.chunks.len() {
                let actor = pending.request.actor;
                let session = pending.request.session;
                if pending.request.player.client.closed() {
                    let _ = pending
                        .request
                        .respond
                        .send(Err(WatchActivationError::Departed));
                    self.retire_need(
                        actor,
                        session,
                        pending.request.view.owner_install_generation,
                        pending.request.need,
                    );
                    continue;
                }
                self.active
                    .entry(actor)
                    .or_insert_with(|| (session, Vec::new()))
                    .1
                    .push(pending.request.need);
                if pending
                    .request
                    .respond
                    .send(Ok(WatchActivated { actor, session }))
                    .is_err()
                {
                    self.retire(
                        actor,
                        session,
                        pending.request.view.owner_install_generation,
                        None,
                    );
                }
            } else {
                self.pending.push_back(pending);
            }
        }
        server
            .cluster_chunk_watches
            .store(Arc::new(self.publication.clone()));
    }
}

pub(crate) fn retirement_now_millis() -> u64 {
    std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .map_or(0, |elapsed| {
            elapsed.as_millis().min(u128::from(u64::MAX)) as u64
        })
}

pub(crate) fn recipients(server: &Server, chunk: ChunkKey) -> Vec<Arc<Player>> {
    let watches = server.cluster_chunk_watches.load_full();
    let mut result = watches
        .iter()
        .filter_map(|(actor, watch)| {
            let cylinder = pumpkin_world::cylindrical_chunk_iterator::Cylindrical::new(
                pumpkin_util::math::vector2::Vector2::new(watch.view.center.x, watch.view.center.z),
                watch.view.radius,
            );
            if !watch.delivered.contains(&chunk)
                || watch.view.world != chunk.world
                || !cylinder.is_within_distance(chunk.x(), chunk.z())
            {
                return None;
            }
            let player = watch.player.upgrade()?;
            (player.cluster_gid() == Some(*actor)
                && super::cluster_world_owner::player_action_lease(server, &player)
                    .is_some_and(|lease| lease.incarnation == watch.session)
                && !player
                    .cluster_departing
                    .load(std::sync::atomic::Ordering::Acquire)
                && !player.client.closed())
            .then_some(player)
        })
        .collect::<Vec<_>>();
    result.sort_by_key(|player| player.cluster_gid());
    result
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn prepared_watch_grace_expires_without_import_and_merges_only_exact_install() {
        let actor = GlobalPlayerId::new(
            pumpkin_cluster::identity::ServerId(1),
            pumpkin_cluster::identity::PlayerSlot(4),
        );
        let chunk = ChunkKey::new(pumpkin_cluster::protocol::WorldId::new(0, 0), 6, 7);
        for transfer in [false, true] {
            let (tx, mut rx) = tokio::sync::mpsc::channel(1);
            let need = ChunkNeedLease::test_owned(tx.try_reserve_owned().unwrap(), 60, vec![chunk]);
            let mut watches = OwnedChunkWatches::default();
            watches.command(WatchCommand::RetainPrepared {
                actor,
                install_generation: 11,
                need,
                delivered: vec![chunk],
                retained_until: 100,
            });
            watches.complete_retirement_coverage(99);
            assert!(matches!(
                rx.try_recv(),
                Err(tokio::sync::mpsc::error::TryRecvError::Empty)
            ));
            if transfer {
                watches.command(WatchCommand::Retire {
                    actor,
                    session: 7,
                    install_generation: 12,
                    retained: Some(super::super::cluster_presence::RetainedDeparture {
                        chunks: vec![chunk],
                        retained_until: 200,
                    }),
                });
                assert!(watches.prepared.contains_key(&(actor, 11)));
                assert!(watches.retiring.is_empty());
                watches.command(WatchCommand::Retire {
                    actor,
                    session: 7,
                    install_generation: 11,
                    retained: Some(super::super::cluster_presence::RetainedDeparture {
                        chunks: vec![chunk],
                        retained_until: 200,
                    }),
                });
                assert!(watches.prepared.is_empty());
                assert_eq!(watches.retiring[&(actor, 7)].expires_millis, 100);
            }
            watches.complete_retirement_coverage(100);
            assert!(
                matches!(rx.try_recv(), Ok(super::super::cluster::ChunkNeed::ReleaseSet { lease: 60, chunks }) if chunks == vec![chunk])
            );
            assert!(watches.prepared.is_empty());
            assert!(watches.retiring.is_empty());
        }
    }

    #[test]
    fn rejected_watch_need_keeps_claim_until_grace_deadline() {
        let chunk = ChunkKey::new(pumpkin_cluster::protocol::WorldId::new(0, 0), 2, 3);
        let (tx, mut rx) = tokio::sync::mpsc::channel(1);
        let need = ChunkNeedLease::test_owned(tx.try_reserve_owned().unwrap(), 30, vec![chunk]);
        let mut watches = OwnedChunkWatches::default();
        watches.rejected_needs.push((300, need));
        watches.complete_retirement_coverage(299);
        assert!(matches!(
            rx.try_recv(),
            Err(tokio::sync::mpsc::error::TryRecvError::Empty)
        ));
        watches.complete_retirement_coverage(300);
        assert!(
            matches!(rx.try_recv(), Ok(super::super::cluster::ChunkNeed::ReleaseSet { lease: 30, chunks }) if chunks == vec![chunk])
        );
        assert!(watches.rejected_needs.is_empty());
    }

    #[test]
    fn retired_watch_holds_original_grace_and_missing_coverage() {
        let actor = GlobalPlayerId::new(
            pumpkin_cluster::identity::ServerId(1),
            pumpkin_cluster::identity::PlayerSlot(2),
        );
        let world = pumpkin_cluster::protocol::WorldId::new(0, 0);
        let first = ChunkKey::new(world, 0, 0);
        let second = ChunkKey::new(world, 1, 0);
        let (tx, mut rx) = tokio::sync::mpsc::channel(2);
        let first_need =
            ChunkNeedLease::test_owned(tx.clone().try_reserve_owned().unwrap(), 10, vec![first]);
        let mut watches = OwnedChunkWatches::default();
        watches.live_retirement_slots.insert((actor, 7));
        watches.active.insert(actor, (7, vec![first_need]));
        watches.command(WatchCommand::Retire {
            actor,
            session: 7,
            install_generation: 1,
            retained: Some(super::super::cluster_presence::RetainedDeparture {
                chunks: vec![first, second],
                retained_until: 100,
            }),
        });
        watches.complete_retirement_coverage(99);
        assert!(matches!(
            rx.try_recv(),
            Err(tokio::sync::mpsc::error::TryRecvError::Empty)
        ));
        watches.complete_retirement_coverage(101);
        assert!(matches!(
            rx.try_recv(),
            Err(tokio::sync::mpsc::error::TryRecvError::Empty)
        ));
        assert_eq!(watches.retiring[&(actor, 7)].needs.len(), 1);
        let second_need =
            ChunkNeedLease::test_owned(tx.try_reserve_owned().unwrap(), 11, vec![second]);
        watches
            .retiring
            .get_mut(&(actor, 7))
            .unwrap()
            .add_need(second_need);
        watches.complete_retirement_coverage(102);
        assert!(watches.retiring.is_empty());
        assert!(
            matches!(rx.try_recv(), Ok(super::super::cluster::ChunkNeed::ReleaseSet { lease: 10, chunks }) if chunks == vec![first])
        );
        assert!(
            matches!(rx.try_recv(), Ok(super::super::cluster::ChunkNeed::ReleaseSet { lease: 11, chunks }) if chunks == vec![second])
        );
    }

    #[test]
    fn covered_retired_watch_releases_at_frozen_deadline() {
        let actor = GlobalPlayerId::new(
            pumpkin_cluster::identity::ServerId(1),
            pumpkin_cluster::identity::PlayerSlot(2),
        );
        let chunk = ChunkKey::new(pumpkin_cluster::protocol::WorldId::new(0, 0), 0, 0);
        let (tx, mut rx) = tokio::sync::mpsc::channel(1);
        let need = ChunkNeedLease::test_owned(tx.try_reserve_owned().unwrap(), 20, vec![chunk]);
        let mut watches = OwnedChunkWatches::default();
        watches.live_retirement_slots.insert((actor, 8));
        watches.active.insert(actor, (8, vec![need]));
        watches.command(WatchCommand::Retire {
            actor,
            session: 8,
            install_generation: 1,
            retained: Some(super::super::cluster_presence::RetainedDeparture {
                chunks: vec![chunk],
                retained_until: 200,
            }),
        });
        watches.complete_retirement_coverage(199);
        assert!(matches!(
            rx.try_recv(),
            Err(tokio::sync::mpsc::error::TryRecvError::Empty)
        ));
        let fresh_view = ViewIntent {
            actor,
            owner_install_generation: 99,
            world: chunk.world,
            center: pumpkin_cluster::protocol::ChunkAddr { x: 0, z: 0 },
            radius: std::num::NonZeroU8::new(2).unwrap(),
            spectator_generates: false,
            simulation_distance: 2,
            chunk_send_epoch: 1,
            tick: pumpkin_cluster::time::TickStamp(1),
        };
        watches.publication.insert(
            actor,
            WatchPublication {
                session: 9,
                view: fresh_view,
                player: Weak::new(),
                delivered: BTreeSet::from([chunk]),
            },
        );
        watches.update_view(ViewIntent {
            owner_install_generation: 1,
            tick: pumpkin_cluster::time::TickStamp(2),
            ..fresh_view
        });
        assert_eq!(watches.publication[&actor].view, fresh_view);
        watches.complete_retirement_coverage(200);
        assert_eq!(watches.publication[&actor].session, 9);
        assert!(
            matches!(rx.try_recv(), Ok(super::super::cluster::ChunkNeed::ReleaseSet { lease: 20, chunks }) if chunks == vec![chunk])
        );
        assert!(watches.retiring.is_empty());
        watches.command(WatchCommand::Retire {
            actor,
            session: 8,
            install_generation: 1,
            retained: Some(super::super::cluster_presence::RetainedDeparture {
                chunks: vec![chunk],
                retained_until: 200,
            }),
        });
        assert!(watches.retiring.is_empty());
    }
}
