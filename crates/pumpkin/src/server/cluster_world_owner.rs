use std::collections::{BTreeMap, BTreeSet, HashMap, HashSet, VecDeque};
use std::sync::Arc;

use pumpkin_cluster::accept::{AcceptanceDelayLevel, AcceptanceDelayTracker};
use pumpkin_cluster::canonical_action::CanonicalAction;
use pumpkin_cluster::codec::decode_batch;
use pumpkin_cluster::completion::{
    ActionKey, ChunkActions, CompletionMessage, LocalTickBatch, TickFinish, encode_finish,
};
use pumpkin_cluster::identity::{GlobalPlayerId, ServerId};
use pumpkin_cluster::inventory::InventoryCaptureBarrier;
use pumpkin_cluster::protocol::{ChunkKey, PlayerBoundaryUpdate, StreamKind, TickBatch};
use pumpkin_cluster::streams::{InboundParcel, OutboundParcel, StreamHeader};
use pumpkin_cluster::time::TickStamp;
use tokio::sync::{mpsc, oneshot, watch};

use super::cluster_world_actions::{ReadyPromotion, WorldTickBuckets};
use super::{Server, cluster_datagram};

#[derive(Default)]
struct LocalFinishes {
    pending: BTreeMap<TickStamp, Vec<TickFinish>>,
    last_tick: Option<TickStamp>,
    closed_through: Option<TickStamp>,
}

const MAX_PENDING_RELIABLE_PER_PEER: usize = 1_200;

pub struct ActorDrainRequest {
    pub gid: GlobalPlayerId,
    pub through_tick: TickStamp,
    pub logout: bool,
    pub respond: oneshot::Sender<ActorDrainStatus>,
}

pub enum ActionSubmitError {
    Unavailable(CanonicalAction),
    Full(CanonicalAction),
}

pub fn submit_action(server: &Server, action: CanonicalAction) -> Result<(), ActionSubmitError> {
    let Some(route) = server.cluster_action_route.load_full() else {
        return Err(ActionSubmitError::Unavailable(action));
    };
    route.try_send(action).map_err(|error| match error {
        mpsc::error::TrySendError::Full(action) => ActionSubmitError::Full(action),
        mpsc::error::TrySendError::Closed(action) => ActionSubmitError::Unavailable(action),
    })
}

pub enum ChunkInventoryRequest {
    Export {
        chunk: ChunkKey,
        blocks: super::cluster_chunk_state::OwnedChunkBlocks,
        respond: oneshot::Sender<ChunkStateExport>,
    },
    Install {
        chunk: ChunkKey,
        blocks: super::cluster_chunk_state::OwnedChunkBlocks,
        states: Vec<pumpkin_cluster::inventory::ChunkInventoryState>,
        pendings: Vec<pumpkin_cluster::xfer::PendingRef>,
        respond: oneshot::Sender<Result<(), pumpkin_cluster::inventory::InventoryApplyError>>,
    },
    CommitReady {
        chunk: ChunkKey,
        respond: oneshot::Sender<Result<(), pumpkin_cluster::inventory::InventoryApplyError>>,
    },
}

pub struct ChunkStateExport {
    pub inventory: Vec<pumpkin_cluster::inventory::ChunkInventoryState>,
    pub pendings: Vec<pumpkin_cluster::xfer::PendingRef>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ActorDrainStatus {
    Ready,
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
    ) -> bool {
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
        })
    }
}

struct HolisticOutbox {
    outbound: mpsc::Sender<OutboundParcel>,
    drop_request: mpsc::Sender<ServerId>,
    pending: BTreeMap<ServerId, VecDeque<OutboundParcel>>,
    quarantined: HashSet<ServerId>,
    unsent_drops: HashSet<ServerId>,
}

impl HolisticOutbox {
    fn new(outbound: mpsc::Sender<OutboundParcel>, drop_request: mpsc::Sender<ServerId>) -> Self {
        Self {
            outbound,
            drop_request,
            pending: BTreeMap::new(),
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

    fn queue_batch(&mut self, connected: &HashSet<ServerId>, batch: &TickBatch) {
        let (parcels, failed) = super::cluster_tick_pump::routed_tick_parcels(batch, connected);
        for peer in failed {
            self.fail_peer(peer, "cluster action batch encode failed");
        }
        self.queue_parcels(parcels);
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
                .is_some_and(|pending| pending.len() >= MAX_PENDING_RELIABLE_PER_PEER)
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

impl LocalFinishes {
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
        if let Some(last) = closed.last() {
            self.closed_through = Some(last.tick);
        }
        closed
    }

    fn closed_through(&self, tick: TickStamp) -> bool {
        self.closed_through
            .is_some_and(|closed| closed == tick || closed.is_after(tick))
    }
}

fn retick_batch(batch: &mut TickBatch, tick: TickStamp) {
    batch.tick = tick;
    macro_rules! retick {
        ($($field:ident),+ $(,)?) => {
            $(for update in &mut batch.$field { update.tick = tick; })+
        };
    }
    retick!(
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
    mut completion_rx: mpsc::Receiver<(ServerId, CompletionMessage)>,
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
    let (admission_import_tx, mut admission_import_rx) = mpsc::channel(256);
    server
        .cluster_actor_admission_import_route
        .store(Some(Arc::new(admission_import_tx)));
    let (action_tx, mut action_rx) = mpsc::channel(4_096);
    server.cluster_action_route.store(Some(Arc::new(action_tx)));
    let server = Arc::clone(server);
    let task_server = Arc::clone(&server);
    server.spawn_task(async move {
        let server = task_server;
        let local = ServerId(server.advanced_config.cluster.server_id);
        let mut buckets = WorldTickBuckets::new();
        let mut chunk_blocks = super::cluster_chunk_state::OwnedChunkStore::default();
        let mut actors = super::cluster_world_actor::OwnedActors::default();
        let mut living = super::cluster_world_actor::OwnedLivingActors::default();
        let mut statistics = super::cluster_actor_statistics::OwnedActorStatistics::with_publication(
            server.cluster_actor_statistics.clone(),
        );
        let mut rejected_actor_intents = HashSet::new();
        let mut inventory = inventory;
        let mut pending_imports = HashMap::<
            ChunkKey,
            super::cluster_world_inventory::CommittedInventoryImport,
        >::new();
        let mut deferred_inventory_lifecycle = VecDeque::new();
        let mut deferred_inventory_actions = VecDeque::new();
        let mut deferred_inventory_screens = VecDeque::new();
        let mut deferred_inventory_menus = VecDeque::new();
        let mut deferred_inventory_snapshots = VecDeque::new();
        let mut inventory_capture_barrier = InventoryCaptureBarrier::default();
        let mut administration =
            super::cluster_world_administration::AdministrativeOwner::attach(&server);
        let mut block_actions =
            super::cluster_world_block_actions::BlockActionOwner::attach(&server);
        let mut block_correction_diagnostics = HashSet::new();
        let mut item_actions =
            super::cluster_world_item_actions::ItemActionOwner::attach(&server);
        let mut holistic_outbox = HolisticOutbox::new(outbound.clone(), drop_request.clone());
        let mut settlement =
            super::cluster_world_settlement::SettlementOwner::new(local, outbound, drop_request);
        let mut observed_drops = HashMap::<ServerId, u64>::new();
        let mut pending_drains = Vec::<PendingActorDrain>::new();
        let mut last_local_source_tick = None;
        let (player_admin_tx, mut player_admin_rx) = mpsc::channel(256);
        server.cluster_player_admin.attach_action_route(player_admin_tx);
        let mut combat = super::cluster_combat_apply::CombatApplyState::new();
        let mut acceptance_delays = AcceptanceDelayTracker::default();
        let mut local_finishes = LocalFinishes::default();
        let acceptance_clock = std::time::Instant::now();
        let mut pending_boundaries = HashMap::new();
        let mut promoted_boundaries = HashSet::new();
        let mut pending_replica_boundaries = HashMap::new();
        let mut replica_boundary_diagnostics = HashSet::new();
        let mut pending_player_admin = VecDeque::<
            super::cluster_player_admin::PlayerAdministrativeResolution,
        >::new();
        let mut pulse = tokio::time::interval(std::time::Duration::from_millis(10));

        loop {
            let mut pending_local_tick = None;
            tokio::select! {
                import = stats_import_rx.recv() => {
                    if let Some(import) = import {
                        statistics.import(import);
                    }
                }
                retire = stats_retire_rx.recv() => {
                    if let Some(retire) = retire {
                        statistics.retire(retire);
                    }
                }
                import = living_import_rx.recv() => {
                    if let Some(import) = import {
                        let _ = living.install(import.actor, import.state);
                    }
                }
                import = admission_import_rx.recv() => {
                    if let Some(import) = import {
                        install_actor_admission(&mut statistics, &mut living, import);
                    }
                }
                action = action_rx.recv() => {
                    if let Some(action) = action {
                        stage_canonical_action(&server, &mut actors, &mut chunk_blocks, &mut inventory, &mut living, &mut statistics, action);
                    }
                }
                parcel = world_rx.recv() => {
                    let Some(parcel) = parcel else { break };
                    if parcel.header.kind == StreamKind::PlayerBoundary {
                        ingest_boundary(
                            &server,
                            &mut pending_boundaries,
                            &mut promoted_boundaries,
                            &mut pending_replica_boundaries,
                            &mut replica_boundary_diagnostics,
                            parcel,
                        );
                    } else {
                        ingest_parcel(
                            &server,
                            &mut settlement,
                            &mut buckets,
                            local,
                            parcel,
                            StreamKind::PlayerWorld,
                        );
                    }
                }
                parcel = combat_rx.recv() => {
                    let Some(parcel) = parcel else { break };
                    ingest_parcel(
                        &server,
                        &mut settlement,
                        &mut buckets,
                        local,
                        parcel,
                        StreamKind::PlayerCombat,
                    );
                }
                local_tick = local_tick_rx.recv() => {
                    let Some(local_tick) = local_tick else { break };
                    pending_local_tick = Some(local_tick);
                }
                completion = completion_rx.recv() => {
                    let Some((source, completion)) = completion else { break };
                    match completion {
                        CompletionMessage::Finish(finish) => {
                            buckets.note_finish_with_entity_owners(source, local, finish, holders, |entity| super::cluster_entity_apply::current_entity_owner(&server, entity));
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
                            replica_boundary_diagnostics.retain(|(gid, _)| gid.server != *peer);
                            observed_drops.insert(*peer, *epoch);
                        }
                    }
                    drop_complete_tx.send_replace(dropped);
                }
                request = drain_rx.recv(), if pending_drains.len() < 256 => {
                    if let Some(request) = request {
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
            while let Ok(import) = stats_import_rx.try_recv() {
                statistics.import(import);
            }
            while let Ok(retire) = stats_retire_rx.try_recv() {
                statistics.retire(retire);
            }
            while let Ok(import) = living_import_rx.try_recv() {
                let _ = living.install(import.actor, import.state);
            }
            while let Ok(import) = admission_import_rx.try_recv() {
                install_actor_admission(&mut statistics, &mut living, import);
            }
            while let Ok(action) = action_rx.try_recv() {
                stage_canonical_action(&server, &mut actors, &mut chunk_blocks, &mut inventory, &mut living, &mut statistics, action);
            }
            statistics.publish();
            if let Some(mut local_tick) = pending_local_tick {
                last_local_source_tick = Some(local_tick.batch.tick);
                for intent in local_tick.actor_intents.drain(..) {
                    if let Err(reason) = actors.apply_controller(intent, |chunk, block| {
                        chunk_blocks.rough_biome_id(chunk, block)
                    }) {
                        if rejected_actor_intents.insert((intent.actor, reason)) {
                            tracing::error!(gid = ?intent.actor, ?reason, "cluster actor controller intent rejected");
                        }
                    }
                }
                retick_batch(&mut local_tick.batch, resolution_tick);
                if let Ok(finish) = TickFinish::try_from_batch(&local_tick.batch) {
                    holistic_outbox.queue_batch(&server.cluster_connected_peers.load(), &local_tick.batch);
                    buckets.push_batch_with_entity_owners(local, local, local_tick.batch, holders, |entity| super::cluster_entity_apply::current_entity_owner(&server, entity));
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
                if inventory.lifecycle_touches_quarantine(&server, &lifecycle, &quarantined) {
                    deferred_inventory_lifecycle.push_back(lifecycle);
                } else {
                    if matches!(&lifecycle, super::cluster_invsee::LocalInventoryLifecycle::BeginDeparture { .. })
                        && deferred_inventory_screens.len() >= 512
                    {
                        deferred_inventory_lifecycle.push_back(lifecycle);
                        continue;
                    }
                    if let Some(screen) = inventory.lifecycle(&server, lifecycle) {
                        deferred_inventory_screens.push_back(screen);
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
                    buckets.push_batch_with_entity_owners(local, local, batch, holders, |entity| super::cluster_entity_apply::current_entity_owner(&server, entity));
                    local_finishes.record(finish);
                }
            }
            for _ in 0..deferred_inventory_screens.len() {
                let Some(screen) = deferred_inventory_screens.pop_front() else { break; };
                let Some(screen_targets) = inventory.screen_targets(&server, &screen) else {
                    inventory.reject_untrusted_screen(&server, screen);
                    continue;
                };
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
                    buckets.push_batch_with_entity_owners(local, local, batch, holders, |entity| super::cluster_entity_apply::current_entity_owner(&server, entity));
                    local_finishes.record(finish);
                }
            }
            for _ in 0..deferred_inventory_menus.len() {
                let Some(menu) = deferred_inventory_menus.pop_front() else { break; };
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
                    buckets.push_batch_with_entity_owners(local, local, batch, holders, |entity| super::cluster_entity_apply::current_entity_owner(&server, entity));
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
                inventory.snapshot(request);
            }
            while let Ok(request) = chunk_inventory_rx.try_recv() {
                match request {
                    ChunkInventoryRequest::Export { chunk, blocks, respond } => {
                        if !chunk_blocks.contains(chunk) {
                            chunk_blocks.install(chunk, blocks);
                            if let Some(snapshot) = chunk_blocks.snapshot(chunk) {
                                server.cluster_owned_chunk_blocks.rcu(|current| {
                                    let mut next = (**current).clone();
                                    next.insert(chunk, snapshot.clone());
                                    std::sync::Arc::new(next)
                                });
                            }
                        }
                        let _ = respond.send(ChunkStateExport {
                            inventory: inventory.export_for_chunk(chunk),
                            pendings: buckets
                                .export_pending_chunk(chunk)
                                .into_iter()
                                .map(|state| pumpkin_cluster::xfer::PendingRef { state })
                                .collect(),
                        });
                    }
                    ChunkInventoryRequest::Install { chunk, blocks, states, pendings, respond } => {
                        let mut candidate = buckets.clone();
                        let result = if pending_imports.contains_key(&chunk)
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
                            inventory
                                .prepare_from_chunk(chunk, states)
                                .and_then(|prepared| {
                                    inventory.commit_prepared_from_chunk(prepared)
                                })
                        };
                        if let Ok(committed) = result {
                            candidate.quarantine_chunk(chunk);
                            buckets = candidate;
                            chunk_blocks.install(chunk, blocks);
                            pending_imports.insert(chunk, committed);
                            let _ = respond.send(Ok(()));
                        } else {
                            let _ = respond.send(Err(pumpkin_cluster::inventory::InventoryApplyError::Rejected));
                        }
                    }
                    ChunkInventoryRequest::CommitReady { chunk, respond } => {
                        if let Some(committed) = pending_imports.remove(&chunk) {
                            let result = inventory.publish_committed_from_chunk(&server, committed);
                            if result.is_err() {
                                tracing::error!(chunk_x = chunk.x(), chunk_z = chunk.z(), "cluster inventory import publication failed");
                            }
                            if result.is_ok() {
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
            while let Ok(intent) = administration.receiver.try_recv() {
                if let Some((mut batch, _)) = administration.prepare(&server, intent) {
                    retick_batch(&mut batch, resolution_tick);
                    let Ok(finish) = TickFinish::try_from_batch(&batch) else { continue; };
                    settlement.register_local(&server, &mut buckets, &batch);
                    holistic_outbox.queue_batch(&server.cluster_connected_peers.load(), &batch);
                    buckets.push_batch_with_entity_owners(local, local, batch, holders, |entity| super::cluster_entity_apply::current_entity_owner(&server, entity));
                    local_finishes.record(finish);
                }
            }
            for (mut batch, _) in administration.retry_pending(&server) {
                retick_batch(&mut batch, resolution_tick);
                let Ok(finish) = TickFinish::try_from_batch(&batch) else { continue; };
                settlement.register_local(&server, &mut buckets, &batch);
                holistic_outbox.queue_batch(&server.cluster_connected_peers.load(), &batch);
                buckets.push_batch_with_entity_owners(local, local, batch, holders, |entity| super::cluster_entity_apply::current_entity_owner(&server, entity));
                local_finishes.record(finish);
            }
            while let Ok(action) = player_admin_rx.try_recv() {
                let mut batch = TickBatch::new(action.tick);
                batch.player_administrative.push(action);
                retick_batch(&mut batch, resolution_tick);
                if let Ok(finish) = pumpkin_cluster::completion::TickFinish::try_from_batch(&batch) {
                    settlement.register_local(&server, &mut buckets, &batch);
                    holistic_outbox.queue_batch(&server.cluster_connected_peers.load(), &batch);
                    buckets.push_batch_with_entity_owners(local, local, batch, holders, |entity| super::cluster_entity_apply::current_entity_owner(&server, entity));
                    local_finishes.record(finish);
                }
            }
            while let Ok(intent) = block_actions.receiver.try_recv() {
                if let Ok(Some((mut batch, _))) = block_actions.capture(intent, &chunk_blocks) {
                    retick_batch(&mut batch, resolution_tick);
                    let Ok(finish) = TickFinish::try_from_batch(&batch) else { continue; };
                    settlement.register_local(&server, &mut buckets, &batch);
                    holistic_outbox.queue_batch(&server.cluster_connected_peers.load(), &batch);
                    buckets.push_batch_with_entity_owners(local, local, batch, holders, |entity| super::cluster_entity_apply::current_entity_owner(&server, entity));
                    local_finishes.record(finish);
                } else if matches!(
                    intent.kind,
                    pumpkin_cluster::block_action::BlockActionKind::StopBreak
                        | pumpkin_cluster::block_action::BlockActionKind::CreativeBreak
                ) {
                    if let Err(reason) = super::cluster_inventory_protocol::publish_block_action_resolution(
                        &server,
                        intent.actor,
                        intent.position,
                        None,
                        intent.client_sequence,
                        false,
                    ) && block_correction_diagnostics.len() < 1_024
                        && block_correction_diagnostics.insert((intent.actor, intent.position))
                    {
                        tracing::warn!(?reason, gid = ?intent.actor, position = ?intent.position, "cluster block action correction unavailable");
                    }
                }
            }
            while let Ok(staged) = item_actions.receiver.try_recv() {
                let Some(effective) = effective_item_pose(&staged) else {
                    let _ = item_actions.reject_unplanned(&server, inventory.ledger(), staged);
                    continue;
                };
                let target = item_target_snapshot(&server, &staged.intent);
                let living_state = living.get(staged.intent.actor).map(|truth| &truth.local);
                let Some(plan) = item_actions.plan(&server, &chunk_blocks, inventory.ledger(), &staged.intent, effective, None, living_state) else {
                    let _ = item_actions.reject_unplanned(&server, inventory.ledger(), staged);
                    continue;
                };
                if let Some((mut batch, _)) = item_actions.capture(staged, effective, target, plan) {
                    retick_batch(&mut batch, resolution_tick);
                    let Ok(finish) = TickFinish::try_from_batch(&batch) else { continue; };
                    settlement.register_local(&server, &mut buckets, &batch);
                    holistic_outbox.queue_batch(&server.cluster_connected_peers.load(), &batch);
                    buckets.push_batch_with_entity_owners(local, local, batch, holders, |entity| super::cluster_entity_apply::current_entity_owner(&server, entity));
                    local_finishes.record(finish);
                }
            }
            for finish in local_finishes.advance(resolution_tick) {
                holistic_outbox.queue(&server.cluster_connected_peers.load(), local, &finish);
                buckets.note_finish_with_entity_owners(local, local, finish, holders, |entity| super::cluster_entity_apply::current_entity_owner(&server, entity));
            }
            holistic_outbox.flush();
            settlement.progress(&server, &mut buckets);
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
            while let Some(ready) = buckets.pop_ready() {
                apply_ready(
                    &server,
                    &mut chunk_blocks,
                    &mut living,
                    &mut statistics,
                    &mut inventory,
                    &mut item_actions,
                    &mut block_actions,
                    &mut administration,
                    &mut combat,
                    &mut pending_boundaries,
                    &mut promoted_boundaries,
                    &mut pending_replica_boundaries,
                    &mut replica_boundary_diagnostics,
                    &mut settlement,
                    &mut inventory_capture_barrier,
                    &mut block_correction_diagnostics,
                    &mut pending_player_admin,
                    ready,
                );
            }
            pending_replica_boundaries.retain(|gid, snapshot| {
                let outcome = super::cluster_ghost::apply_remote_player_boundary(
                    &server,
                    snapshot.clone(),
                );
                if outcome == super::cluster_ghost::BoundaryApplyOutcome::Applied {
                    #[cfg(feature = "cluster-e2e")]
                    tracing::info!(?gid, "cluster e2e remote player boundary applied after dependency arrived");
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
                    && pending.ready(last_local_source_tick, resolution_tick, &local_finishes, &buckets)
                    && !pending_player_admin
                        .iter()
                        .any(|resolution| resolution.update.target == pending.request.gid)
                {
                    let _ = pending.request.respond.send(ActorDrainStatus::Ready);
                } else {
                    waiting.push(pending);
                }
            }
            pending_drains = waiting;
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
            living.publish(&server);
        }
    });
}

fn install_actor_admission(
    statistics: &mut super::cluster_actor_statistics::OwnedActorStatistics,
    living: &mut super::cluster_world_actor::OwnedLivingActors,
    import: super::cluster_world_actor::ActorAdmissionImport,
) {
    let actor = pumpkin_cluster::actor::ActorId::Player(import.actor);
    if import.statistics.actor != import.actor
        || import.living.food.actor != actor
        || import.living.combat.actor != actor
        || living.contains(actor)
        || statistics.current_revision(import.actor).is_some()
    {
        return;
    }
    if statistics.import(import.statistics) {
        let _ = living.install(actor, import.living);
    }
}

fn holders(chunk: ChunkKey) -> Vec<u16> {
    cluster_datagram::holders_for_chunk(chunk)
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

fn ingest_parcel(
    server: &Server,
    settlement: &mut super::cluster_world_settlement::SettlementOwner,
    buckets: &mut WorldTickBuckets,
    local: ServerId,
    parcel: InboundParcel,
    expected: StreamKind,
) {
    if parcel.header.kind != expected
        && !(expected == StreamKind::PlayerWorld && parcel.header.kind == StreamKind::EntityWorld)
    {
        return;
    }
    if let Ok(batch) = decode_batch(&parcel.bytes) {
        settlement.register_local(server, buckets, &batch);
        buckets.push_batch_with_entity_owners(parcel.peer, local, batch, holders, |entity| {
            super::cluster_entity_apply::current_entity_owner(&server, entity)
        });
    }
}

fn apply_ready(
    server: &Arc<Server>,
    chunk_blocks: &mut super::cluster_chunk_state::OwnedChunkStore,
    living: &mut super::cluster_world_actor::OwnedLivingActors,
    statistics: &mut super::cluster_actor_statistics::OwnedActorStatistics,
    inventory: &mut super::cluster_world_inventory::InventoryOwner,
    item_actions: &mut super::cluster_world_item_actions::ItemActionOwner,
    block_actions: &mut super::cluster_world_block_actions::BlockActionOwner,
    administration: &mut super::cluster_world_administration::AdministrativeOwner,
    combat: &mut super::cluster_combat_apply::CombatApplyState,
    pending_boundaries: &mut HashMap<
        (
            pumpkin_cluster::identity::GlobalPlayerId,
            pumpkin_cluster::time::TickStamp,
            ChunkKey,
            ChunkKey,
        ),
        PlayerBoundaryUpdate,
    >,
    promoted_boundaries: &mut HashSet<(
        pumpkin_cluster::identity::GlobalPlayerId,
        pumpkin_cluster::time::TickStamp,
        ChunkKey,
        ChunkKey,
    )>,
    pending_replica_boundaries: &mut HashMap<
        GlobalPlayerId,
        pumpkin_cluster::protocol::PlayerReplicaSnapshot,
    >,
    replica_boundary_diagnostics: &mut HashSet<(
        GlobalPlayerId,
        super::cluster_ghost::BoundaryApplyOutcome,
    )>,
    settlement: &mut super::cluster_world_settlement::SettlementOwner,
    inventory_capture_barrier: &mut InventoryCaptureBarrier,
    block_correction_diagnostics: &mut HashSet<(
        GlobalPlayerId,
        pumpkin_cluster::protocol::BlockPos,
    )>,
    pending_player_admin: &mut VecDeque<
        super::cluster_player_admin::PlayerAdministrativeResolution,
    >,
    ready: ReadyPromotion,
) {
    let promotion_holders: Vec<_> = ready.holders.iter().copied().collect();
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
    administration.apply_promoted(server, ready.tick, &ready.batches);
    for sourced in &ready.batches {
        let batch = &sourced.batch;
        super::cluster_combat_apply::apply_resolved_batch(server, combat, batch);
        for update in &batch.break_block {
            let applied = world_for(server, update.chunk.world).is_some_and(|world| {
                super::cluster_world_apply::apply_break_to_world(&world, update).applied
            });
            let client_sequence = block_actions.take_client_sequence(update);
            if update.gid.server.0 == server.advanced_config.cluster.server_id {
                if let Err(reason) =
                    super::cluster_inventory_protocol::publish_block_action_resolution(
                        server,
                        update.gid,
                        update.pos,
                        None,
                        client_sequence,
                        applied,
                    )
                    && block_correction_diagnostics.len() < 1_024
                    && block_correction_diagnostics.insert((update.gid, update.pos))
                {
                    tracing::warn!(?reason, gid = ?update.gid, position = ?update.pos, "cluster block action correction unavailable");
                }
            }
        }
        for update in &batch.place_block {
            if let Some(world) = world_for(server, update.chunk.world) {
                let _ = super::cluster_world_apply::apply_place_to_world(
                    &world,
                    inventory.ledger_mut(),
                    update,
                );
            }
        }
        for update in &batch.player_administrative {
            if update.target.server.0 != server.advanced_config.cluster.server_id {
                continue;
            }
            let key = pumpkin_cluster::completion::ActionKey::player_administrative(update);
            let resolution = super::cluster_player_admin::PlayerAdministrativeResolution {
                action: key,
                update: update.clone(),
                accepted: true,
                holders: promotion_holders.clone(),
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
            assert!(
                pending_player_admin.len() <= 4_096,
                "cluster player administration resolution capacity exhausted"
            );
        }
        for update in &batch.player_chunk_transition {
            inventory.lifecycle(
                server,
                super::cluster_invsee::LocalInventoryLifecycle::Relocate {
                    target: pumpkin_cluster::inventory::InventoryTarget::Player(update.gid),
                    chunks: vec![update.to],
                },
            );
            let key = (update.gid, update.tick, update.from, update.to);
            if let Some(boundary) = pending_boundaries.remove(&key) {
                apply_boundary(
                    server,
                    pending_replica_boundaries,
                    replica_boundary_diagnostics,
                    boundary,
                );
            } else {
                promoted_boundaries.insert(key);
            }
        }
    }
    settlement.promoted(server, &ready);
}

fn ingest_boundary(
    server: &Arc<Server>,
    pending: &mut HashMap<
        (
            pumpkin_cluster::identity::GlobalPlayerId,
            pumpkin_cluster::time::TickStamp,
            ChunkKey,
            ChunkKey,
        ),
        PlayerBoundaryUpdate,
    >,
    promoted: &mut HashSet<(
        pumpkin_cluster::identity::GlobalPlayerId,
        pumpkin_cluster::time::TickStamp,
        ChunkKey,
        ChunkKey,
    )>,
    pending_replica_boundaries: &mut HashMap<
        GlobalPlayerId,
        pumpkin_cluster::protocol::PlayerReplicaSnapshot,
    >,
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
    let embedded_gid_matches = match &boundary {
        PlayerBoundaryUpdate::Transition {
            snapshot: Some(snapshot),
            ..
        } => snapshot.movement.gid == gid,
        _ => true,
    };
    if gid.server != parcel.peer
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
    match boundary {
        PlayerBoundaryUpdate::Transition {
            gid,
            tick,
            from,
            to,
            ..
        } => {
            let key = (gid, tick, from, to);
            if promoted.remove(&key) {
                apply_boundary(
                    server,
                    pending_replica_boundaries,
                    replica_boundary_diagnostics,
                    boundary,
                );
            } else {
                pending.insert(key, boundary);
            }
        }
        _ => apply_boundary(
            server,
            pending_replica_boundaries,
            replica_boundary_diagnostics,
            boundary,
        ),
    }
}

fn apply_boundary(
    server: &Arc<Server>,
    pending: &mut HashMap<GlobalPlayerId, pumpkin_cluster::protocol::PlayerReplicaSnapshot>,
    diagnostics: &mut HashSet<(GlobalPlayerId, super::cluster_ghost::BoundaryApplyOutcome)>,
    boundary: PlayerBoundaryUpdate,
) {
    match boundary {
        PlayerBoundaryUpdate::Enter(snapshot)
        | PlayerBoundaryUpdate::Transition {
            snapshot: Some(snapshot),
            ..
        } => {
            let gid = snapshot.movement.gid;
            let outcome =
                super::cluster_ghost::apply_remote_player_boundary(server, snapshot.clone());
            if outcome != super::cluster_ghost::BoundaryApplyOutcome::Applied {
                pending.insert(gid, snapshot);
                if diagnostics.insert((gid, outcome)) {
                    #[cfg(feature = "cluster-e2e")]
                    tracing::info!(?gid, ?outcome, "cluster e2e remote player boundary pending");
                }
            } else {
                pending.remove(&gid);
                #[cfg(feature = "cluster-e2e")]
                tracing::info!(?gid, "cluster e2e remote player boundary applied");
            }
        }
        PlayerBoundaryUpdate::Leave { gid, .. }
        | PlayerBoundaryUpdate::Transition {
            gid,
            snapshot: None,
            ..
        } => {
            pending.remove(&gid);
            super::cluster_ghost::remove_remote_player_ghost(server, gid);
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

    fn key(sequence: u16) -> ActionKey {
        ActionKey {
            actor: ActorId::Player(GlobalPlayerId::new(ServerId(1), PlayerSlot(1))),
            sequence,
            family: 2,
        }
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
                through_tick: TickStamp(70),
                logout: false,
                respond,
            },
            admitted_tick: None,
            leave_applied: true,
        };
        let buckets = WorldTickBuckets::new();
        let mut finishes = LocalFinishes::default();
        finishes.advance(TickStamp(71));
        assert!(!pending.ready(Some(TickStamp(69)), TickStamp(71), &finishes, &buckets));
        assert!(!pending.ready(Some(TickStamp(70)), TickStamp(71), &finishes, &buckets));
        finishes.advance(TickStamp(72));
        assert!(pending.ready(Some(TickStamp(70)), TickStamp(72), &finishes, &buckets));
    }

    #[test]
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
