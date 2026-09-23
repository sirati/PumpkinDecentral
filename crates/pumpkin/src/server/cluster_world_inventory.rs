use pumpkin_inventory::Inventory;
use std::collections::{BTreeSet, HashMap, HashSet};
use std::sync::Arc;

use pumpkin_cluster::actor::ActorId;
use pumpkin_cluster::completion::{ActionKey, TickFinish};
use pumpkin_cluster::identity::GlobalPlayerId;
use pumpkin_cluster::inventory::{
    BeaconMenuState, ChunkInventoryState, INV_CURSOR, INV_MAIN, InvLedger, InventoryApplyError,
    InventoryDelta, InventoryGesture, InventoryIntentPayload, InventoryMenuEffect,
    InventoryMenuMetadata, InventoryMutationIntent, InventoryOp, InventoryScreenCommand,
    InventoryScreenSession, InventoryScreenSessions, InventorySequence, InventorySlotRef,
    InventoryTarget, SemanticInventoryOp, reduce_inventory_menu,
};
use pumpkin_cluster::item_action::ItemAction;
use pumpkin_cluster::protocol::{ChunkKey, TickBatch};
use tokio::sync::mpsc;

use super::Server;
use super::cluster_invsee::{
    LocalInventoryAction, LocalInventoryLifecycle, LocalInventoryScreenCommand,
    LocalInventorySnapshotRequest,
};

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) struct InventoryCloseStatus {
    pub actor: pumpkin_cluster::identity::GlobalPlayerId,
    pub session: u64,
    pub token: u64,
    pub accepted_tick: pumpkin_cluster::time::TickStamp,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) struct InventoryHandoffInstallStatus {
    pub token: u64,
    pub revision: Result<u64, InventoryApplyError>,
}

pub(super) struct PreparedInventoryImport {
    chunk: ChunkKey,
    states: Vec<ChunkInventoryState>,
    ledger: InvLedger,
    ground_ledger: InvLedger,
    beacons: HashMap<InventoryTarget, BeaconMenuState>,
    snapshots: Vec<pumpkin_cluster::inventory::InventoryStateSnapshot>,
}

pub(super) struct CommittedInventoryImport {
    targets: Vec<InventoryTarget>,
}

pub(super) struct PreparedInventoryMutation {
    pub(super) operation: InventoryOp,
    pub(super) delta: InventoryDelta,
    ground_delta: InventoryDelta,
}

fn validate_inventory_changes(changes: &[SemanticInventoryOp]) -> Result<(), InventoryApplyError> {
    for change in changes {
        super::cluster_invsee::item_stack_from_inventory_cell(&change.before)?;
        super::cluster_invsee::item_stack_from_inventory_cell(&change.after)?;
    }
    Ok(())
}

pub(super) struct InventoryOwner {
    pub(super) actions: mpsc::Receiver<LocalInventoryAction>,
    pub(super) screens: mpsc::Receiver<LocalInventoryScreenCommand>,
    pub(super) snapshots: mpsc::Receiver<LocalInventorySnapshotRequest>,
    pub(super) lifecycle: mpsc::Receiver<LocalInventoryLifecycle>,
    pub(super) menus: mpsc::Receiver<super::cluster_inventory_protocol::LocalInventoryMenuIntent>,
    ledger: InvLedger,
    ground_ledger: InvLedger,
    sequences: HashMap<ActorId, InventorySequence>,
    origins: HashMap<ActionKey, Option<u64>>,
    remote_previews: HashMap<ActionKey, (InventoryScreenSession, GlobalPlayerId, u64)>,
    screen_sessions: InventoryScreenSessions,
    screen_chunks: HashMap<InventoryScreenSession, BTreeSet<ChunkKey>>,
    menu_metadata: HashMap<InventoryScreenSession, InventoryMenuMetadata>,
    ender_open_sessions: HashSet<InventoryScreenSession>,
    closing: HashMap<ActionKey, (InventoryScreenSession, Option<InventoryTarget>, u64)>,
    beacons: HashMap<InventoryTarget, BeaconMenuState>,
    beacon_handles: HashMap<InventoryTarget, Arc<dyn Inventory>>,
    beacon_publications: Vec<(InventoryTarget, BeaconMenuState, Arc<dyn Inventory>)>,
    pending_invsee_subscriptions: HashMap<(InventoryScreenSession, GlobalPlayerId), bool>,
    pending_invsee_removals: HashSet<GlobalPlayerId>,
    pending_invsee_relocations: HashSet<GlobalPlayerId>,
    pending_invsee_changes: HashMap<GlobalPlayerId, u64>,
    pending_invsee_applied: HashMap<(InventoryScreenSession, GlobalPlayerId), u64>,
    pending_invsee_rejected: HashMap<(InventoryScreenSession, GlobalPlayerId, u64), bool>,
    pending_invsee_egress: HashMap<
        InventoryScreenSession,
        (
            pumpkin_cluster::inventory::InventoryScreenSnapshot,
            pumpkin_cluster::inventory::InventoryScreenSnapshot,
        ),
    >,
}

impl InventoryOwner {
    pub(super) fn preview_mutation(
        &self,
        actor: ActorId,
        sequence: pumpkin_cluster::identity::PlayerSeq,
        tick: pumpkin_cluster::time::TickStamp,
        actor_chunk: ChunkKey,
        affected: Vec<ChunkKey>,
        mutation: pumpkin_cluster::inventory::InventoryMutation,
    ) -> Result<PreparedInventoryMutation, InventoryApplyError> {
        if mutation.operations.is_empty() {
            return Err(InventoryApplyError::Rejected);
        }
        let operation = pumpkin_cluster::inventory::capture_inventory_mutation(
            actor,
            sequence,
            tick,
            mutation.expected_revisions,
            mutation.operations,
            actor_chunk,
            affected,
        );
        validate_inventory_changes(&operation.changes)?;
        let delta = self.ledger.clone().apply_op_result(&operation)?;
        let ground_delta = self.ground_ledger.clone().apply_op_result(&operation)?;
        if delta != ground_delta {
            return Err(InventoryApplyError::Rejected);
        }
        Ok(PreparedInventoryMutation {
            operation,
            delta,
            ground_delta,
        })
    }

    pub(super) fn commit_prepared_mutation(
        &mut self,
        prepared: PreparedInventoryMutation,
    ) -> Result<InventoryDelta, InventoryApplyError> {
        validate_inventory_changes(&prepared.operation.changes)?;
        let mut next = self.ledger.clone();
        let mut ground_next = self.ground_ledger.clone();
        let delta = next.apply_op_result(&prepared.operation)?;
        let ground_delta = ground_next.apply_op_result(&prepared.operation)?;
        if delta != prepared.delta || ground_delta != prepared.ground_delta || delta != ground_delta
        {
            return Err(InventoryApplyError::Rejected);
        }
        self.ledger = next;
        self.ground_ledger = ground_next;
        Ok(delta)
    }

    pub(super) fn attach(server: &Server) -> Self {
        Self {
            actions: super::cluster_invsee::attach_inventory_action_owner(server),
            screens: super::cluster_invsee::attach_inventory_screen_owner(server),
            snapshots: super::cluster_invsee::attach_inventory_snapshot_owner(server),
            lifecycle: super::cluster_invsee::attach_inventory_install_owner(server),
            menus: super::cluster_inventory_protocol::attach_inventory_menu_owner(server),
            ledger: InvLedger::new(),
            ground_ledger: InvLedger::new(),
            sequences: HashMap::new(),
            origins: HashMap::new(),
            remote_previews: HashMap::new(),
            screen_sessions: InventoryScreenSessions::new(),
            screen_chunks: HashMap::new(),
            menu_metadata: HashMap::new(),
            ender_open_sessions: HashSet::new(),
            closing: HashMap::new(),
            beacons: HashMap::new(),
            beacon_handles: HashMap::new(),
            beacon_publications: Vec::new(),
            pending_invsee_subscriptions: HashMap::new(),
            pending_invsee_removals: HashSet::new(),
            pending_invsee_relocations: HashSet::new(),
            pending_invsee_changes: HashMap::new(),
            pending_invsee_applied: HashMap::new(),
            pending_invsee_rejected: HashMap::new(),
            pending_invsee_egress: HashMap::new(),
        }
    }

    pub(super) fn snapshot(&self, request: LocalInventorySnapshotRequest) {
        let _ = request.reply.send(self.ledger.snapshot(request.target));
    }

    pub(super) fn drain_handoff_cancellations(&mut self, server: &Server) {
        let pending = server.cluster_inventory_cancel_requests.load_full();
        let statuses = server.cluster_inventory_handoff_installed.load_full();
        let mut completed = Vec::new();
        for (gid, token) in pending.iter() {
            if let Some(status) = statuses.get(gid)
                && status.token == *token
            {
                if status.revision.is_ok() {
                    let player = InventoryTarget::Player(*gid);
                    let crafting = InventoryTarget::Virtual {
                        host: gid.server,
                        owner: ActorId::Player(*gid),
                        session: 0,
                        kind: pumpkin_data::screen::WindowType::Crafting as u16 + 1,
                    };
                    let ender = super::cluster_invsee::ender_chest_target(*gid);
                    self.ledger.remove_target(player);
                    self.ledger.remove_target(crafting);
                    self.ledger.remove_target(ender);
                    self.ground_ledger.remove_target(player);
                    self.ground_ledger.remove_target(crafting);
                    self.ground_ledger.remove_target(ender);
                    super::cluster_invsee::unpublish_inventory_snapshot(server, player);
                    super::cluster_invsee::unpublish_inventory_snapshot(server, crafting);
                    super::cluster_invsee::unpublish_inventory_snapshot(server, ender);
                    super::cluster_invsee::unpublish_inventory_screen(
                        server,
                        InventoryScreenSession {
                            actor: *gid,
                            session: 0,
                        },
                    );
                    self.pending_invsee_changes.remove(gid);
                    self.pending_invsee_removals.insert(*gid);
                }
                super::cluster_invsee::retire_handoff_inventory_install(server, *gid, *token);
                completed.push((*gid, *token));
            }
        }
        if !completed.is_empty() {
            server.cluster_inventory_cancel_requests.rcu(|published| {
                let mut next = (**published).clone();
                for item in &completed {
                    next.remove(item);
                }
                Arc::new(next)
            });
        }
        self.flush_invsee_notifications(server);
    }

    fn queue_invsee_screen(
        &mut self,
        screen: &pumpkin_cluster::inventory::InventoryScreenSnapshot,
        subscribe: bool,
    ) {
        if let Some(InventoryTarget::Player(target)) =
            screen.preview.as_ref().map(|preview| preview.target)
        {
            self.pending_invsee_subscriptions
                .insert((screen.session, target), subscribe);
        }
    }

    fn flush_invsee_notifications(&mut self, server: &Server) {
        let mut delivered = Vec::new();
        let mut rejected = Vec::new();
        self.pending_invsee_egress
            .retain(|session, (before, after)| {
                if super::cluster_invsee::inventory_screen_snapshot(server, *session).as_ref()
                    != Some(after)
                {
                    if let Some(preview) = after.preview.as_ref()
                        && let InventoryTarget::Player(target) = preview.target
                    {
                        let expected = before
                            .preview
                            .as_ref()
                            .map_or(preview.revision, |prior| prior.revision);
                        rejected.push(((*session, target, expected), true));
                    }
                    return false;
                }
                let before = std::iter::once((*session, before.clone())).collect();
                let after = std::iter::once((*session, after.clone())).collect();
                if super::cluster_inventory_protocol::publish_inventory_screen_deltas(
                    server, &before, &after,
                )
                .is_ok()
                {
                    if let Some(preview) = after
                        .get(session)
                        .and_then(|screen| screen.preview.as_ref())
                        && let InventoryTarget::Player(target) = preview.target
                    {
                        delivered.push(((*session, target), preview.revision));
                    }
                    false
                } else {
                    true
                }
            });
        self.pending_invsee_applied.extend(delivered);
        self.pending_invsee_rejected.extend(rejected);
        self.pending_invsee_subscriptions
            .retain(|(session, target), subscribe| {
                let result = if *subscribe {
                    super::cluster_invsee::subscribe_inventory_screen(server, *session, *target)
                } else {
                    super::cluster_invsee::unsubscribe_inventory_screen(server, *session, *target)
                };
                result.is_err()
            });
        self.pending_invsee_removals.retain(|target| {
            super::cluster_invsee::notify_inventory_target_removed(server, *target).is_err()
        });
        self.pending_invsee_relocations.retain(|target| {
            super::cluster_invsee::notify_inventory_target_relocated(server, *target).is_err()
        });
        self.pending_invsee_changes.retain(|target, revision| {
            super::cluster_invsee::notify_inventory_target_changed(server, *target, *revision)
                .is_err()
        });
        self.pending_invsee_applied
            .retain(|(session, target), revision| {
                super::cluster_invsee::notify_inventory_screen_refresh_applied(
                    server, *session, *target, *revision,
                )
                .is_err()
            });
        self.pending_invsee_rejected
            .retain(|(session, target, revision), retry| {
                super::cluster_invsee::notify_inventory_screen_refresh_rejected(
                    server, *session, *target, *revision, *retry,
                )
                .is_err()
            });
    }

    pub(super) fn ledger_mut(&mut self) -> &mut InvLedger {
        &mut self.ledger
    }

    pub(super) fn ledger(&self) -> &InvLedger {
        &self.ledger
    }

    pub(super) fn ground_snapshot_for_chunk(
        &self,
        target: InventoryTarget,
        chunk: ChunkKey,
    ) -> Option<pumpkin_cluster::inventory::InventoryStateSnapshot> {
        self.ground_ledger
            .target_chunks(target)?
            .contains(&chunk)
            .then(|| self.ground_ledger.snapshot(target))?
    }

    pub(super) fn prepare_joint_inventory_action(
        &self,
        operation: InventoryOp,
        legs: &[pumpkin_cluster::inventory::JointInventoryLeg],
    ) -> Result<PreparedInventoryMutation, pumpkin_cluster::inventory::JointInventoryError> {
        validate_inventory_changes(&operation.changes)
            .map_err(pumpkin_cluster::inventory::JointInventoryError::Apply)?;
        let delta = pumpkin_cluster::inventory::preview_joint_inventory_action(&operation, legs)?;
        let local_delta = self
            .ledger
            .clone()
            .apply_op_result(&operation)
            .map_err(pumpkin_cluster::inventory::JointInventoryError::Apply)?;
        let ground_delta = self
            .ground_ledger
            .clone()
            .apply_op_result(&operation)
            .map_err(pumpkin_cluster::inventory::JointInventoryError::Apply)?;
        if local_delta != delta || ground_delta != delta {
            return Err(pumpkin_cluster::inventory::JointInventoryError::Apply(
                InventoryApplyError::Rejected,
            ));
        }
        Ok(PreparedInventoryMutation {
            operation,
            delta,
            ground_delta,
        })
    }

    pub(super) fn action_targets(&self, action: &LocalInventoryAction) -> Vec<InventoryTarget> {
        pumpkin_cluster::inventory::inventory_intent_targets(&action.intent)
    }

    pub(super) fn menu_targets(
        &self,
        menu: &super::cluster_inventory_protocol::LocalInventoryMenuIntent,
    ) -> Option<Vec<InventoryTarget>> {
        self.menu_metadata
            .get(&menu.intent.session)
            .map(pumpkin_cluster::inventory::inventory_menu_targets)
    }

    pub(super) fn screen_targets(
        &self,
        server: &Server,
        screen: &LocalInventoryScreenCommand,
    ) -> Option<Vec<InventoryTarget>> {
        let session = match &screen.command {
            InventoryScreenCommand::BeginQuickCraft { session, .. }
            | InventoryScreenCommand::AddQuickCraftSlot { session, .. }
            | InventoryScreenCommand::FinishQuickCraft { session }
            | InventoryScreenCommand::Close { session } => *session,
        };
        super::cluster_invsee::inventory_screen_snapshot(server, session)
            .map(|snapshot| {
                if matches!(&screen.command, InventoryScreenCommand::Close { .. }) {
                    let mut targets = vec![InventoryTarget::Player(session.actor)];
                    targets.extend(snapshot.slots.iter().filter_map(|slot| {
                        matches!(slot.slot.target, InventoryTarget::Virtual { kind, .. } if kind != 0x100)
                            .then_some(slot.slot.target)
                    }));
                    targets.sort_unstable();
                    targets.dedup();
                    targets
                } else {
                    pumpkin_cluster::inventory::inventory_screen_targets(&snapshot)
                }
            })
    }

    pub(super) fn lifecycle_touches_quarantine(
        &self,
        server: &Server,
        lifecycle: &LocalInventoryLifecycle,
        quarantined: &HashSet<ChunkKey>,
    ) -> bool {
        if quarantined.is_empty() {
            return false;
        }
        let target_touches = |target: InventoryTarget| {
            self.ledger
                .target_chunks(target)
                .is_some_and(|chunks| chunks.iter().any(|chunk| quarantined.contains(chunk)))
                || target
                    .intrinsic_chunk()
                    .is_some_and(|chunk| quarantined.contains(&chunk))
        };
        let screen_touches = |screen: &pumpkin_cluster::inventory::InventoryScreenSnapshot| {
            screen
                .affected
                .iter()
                .any(|chunk| quarantined.contains(chunk))
                || screen
                    .slots
                    .iter()
                    .any(|slot| target_touches(slot.slot.target))
        };
        match lifecycle {
            LocalInventoryLifecycle::InstallPlayer {
                player,
                crafting,
                ender,
                screen,
                chunks,
                ..
            } => {
                chunks.iter().any(|chunk| quarantined.contains(chunk))
                    || target_touches(player.target)
                    || target_touches(crafting.target)
                    || target_touches(ender.target)
                    || screen_touches(screen)
            }
            LocalInventoryLifecycle::Install {
                snapshot,
                chunks,
                screen,
                ..
            } => {
                chunks.iter().any(|chunk| quarantined.contains(chunk))
                    || target_touches(snapshot.target)
                    || screen.as_ref().is_some_and(|screen| screen_touches(screen))
            }
            LocalInventoryLifecycle::Relocate { target, chunks } => {
                target_touches(*target) || chunks.iter().any(|chunk| quarantined.contains(chunk))
            }
            LocalInventoryLifecycle::Remove { target }
            | LocalInventoryLifecycle::BeaconSnapshot { target, .. } => target_touches(*target),
            LocalInventoryLifecycle::OpenScreen { screen, .. } => screen_touches(screen),
            LocalInventoryLifecycle::CloseScreen { session } => {
                self.screen_chunks
                    .get(session)
                    .is_some_and(|chunks| chunks.iter().any(|chunk| quarantined.contains(chunk)))
                    || super::cluster_invsee::inventory_screen_snapshot(server, *session)
                        .is_none_or(|screen| screen_touches(&screen))
            }
            LocalInventoryLifecycle::BeginDeparture { actor, .. } => {
                target_touches(InventoryTarget::Player(*actor))
                    || server.cluster_inventory_screens.load().values().any(|screen| {
                        screen.session.actor == *actor
                            && screen.session.session != 0
                            && screen.slots.iter().any(|slot| {
                                matches!(slot.slot.target, InventoryTarget::Virtual { kind, .. } if kind != 0x100)
                                    && target_touches(slot.slot.target)
                            })
                    })
            }
            LocalInventoryLifecycle::RefreshRemotePreview { session, .. } => {
                super::cluster_invsee::inventory_screen_snapshot(server, *session)
                    .is_some_and(|screen| screen_touches(&screen))
            }
        }
    }

    pub(super) fn export_for_chunk(&self, chunk: ChunkKey) -> Vec<ChunkInventoryState> {
        self.ground_ledger
            .targets_for_chunk(chunk)
            .into_iter()
            .filter_map(|target| {
                Some(ChunkInventoryState {
                    snapshot: self.ground_ledger.snapshot(target)?,
                    chunks: self.ground_ledger.target_chunks(target)?.to_vec(),
                    beacon: self.beacons.get(&target).copied(),
                })
            })
            .collect()
    }

    pub(super) fn prepare_from_chunk(
        &self,
        chunk: ChunkKey,
        states: Vec<ChunkInventoryState>,
    ) -> Result<PreparedInventoryImport, InventoryApplyError> {
        let mut ledger = self.ledger.clone();
        let mut ground_ledger = self.ground_ledger.clone();
        let mut beacons = self.beacons.clone();
        let mut targets = std::collections::HashSet::new();
        for state in &states {
            let target = state.snapshot.target;
            let mut chunks = state.chunks.clone();
            chunks.sort_unstable();
            if !targets.insert(target)
                || !chunks.contains(&chunk)
                || chunks.windows(2).any(|pair| pair[0] == pair[1])
                || target
                    .intrinsic_chunk()
                    .is_some_and(|intrinsic| !chunks.contains(&intrinsic))
                || state.beacon.is_some() && !matches!(target, InventoryTarget::Block { .. })
                || state.beacon.is_none() && beacons.contains_key(&target)
            {
                return Err(InventoryApplyError::Rejected);
            }
            if let Some(current) = ledger.snapshot(target) {
                if current.revision > state.snapshot.revision {
                    return Err(InventoryApplyError::Rejected);
                }
                if current.revision == state.snapshot.revision {
                    let current_chunks = ledger.target_chunks(target).unwrap_or_default();
                    if current != state.snapshot || current_chunks != chunks.as_slice() {
                        return Err(InventoryApplyError::Rejected);
                    }
                }
            }
            if let Some(incoming) = state.beacon {
                if let Some(current) = beacons.get(&target) {
                    if current.revision > incoming.revision
                        || current.revision == incoming.revision && *current != incoming
                    {
                        return Err(InventoryApplyError::Rejected);
                    }
                }
                beacons.insert(target, incoming);
            }
            ledger
                .install_owned_snapshot(state.snapshot.clone(), chunks)
                .map_err(|_| InventoryApplyError::Rejected)?;
            ground_ledger
                .install_owned_snapshot(state.snapshot.clone(), state.chunks.clone())
                .map_err(|_| InventoryApplyError::Rejected)?;
        }
        let snapshots: Vec<_> = states.iter().map(|state| state.snapshot.clone()).collect();
        super::cluster_invsee::validate_imported_inventory_snapshots(&snapshots)?;
        Ok(PreparedInventoryImport {
            chunk,
            states,
            ledger,
            ground_ledger,
            beacons,
            snapshots,
        })
    }

    pub(super) fn commit_prepared_from_chunk(
        &mut self,
        prepared: PreparedInventoryImport,
    ) -> Result<CommittedInventoryImport, InventoryApplyError> {
        let prepared = self.prepare_from_chunk(prepared.chunk, prepared.states)?;
        let targets = prepared
            .snapshots
            .iter()
            .map(|snapshot| snapshot.target)
            .collect();
        self.ledger = prepared.ledger;
        self.ground_ledger = prepared.ground_ledger;
        self.beacons = prepared.beacons;
        Ok(CommittedInventoryImport { targets })
    }

    pub(super) fn publish_committed_from_chunk(
        &self,
        server: &Server,
        committed: CommittedInventoryImport,
    ) -> Result<(), InventoryApplyError> {
        let snapshots = committed
            .targets
            .into_iter()
            .map(|target| {
                self.ledger
                    .snapshot(target)
                    .ok_or(InventoryApplyError::TargetUnavailable)
            })
            .collect::<Result<Vec<_>, _>>()?;
        super::cluster_invsee::publish_imported_inventory_snapshots(server, snapshots)?;
        server
            .cluster_beacon_states
            .store(Arc::new(self.beacons.clone()));
        Ok(())
    }

    fn item_source_op(
        &self,
        action: &ItemAction,
    ) -> Result<Option<InventoryOp>, InventoryApplyError> {
        let chunks = self
            .ledger
            .target_chunks(action.source.target)
            .ok_or(InventoryApplyError::Rejected)?;
        if !action.affected.contains(&action.actor_chunk)
            || chunks.iter().any(|chunk| !action.affected.contains(chunk))
        {
            return Err(InventoryApplyError::Rejected);
        }
        let snapshot = self
            .ledger
            .snapshot(action.source.target)
            .ok_or(InventoryApplyError::Rejected)?;
        if snapshot.revision != action.expected_inventory_revision {
            return Err(InventoryApplyError::Rejected);
        }
        let actual = snapshot
            .cells
            .iter()
            .find(|cell| cell.loc == action.source.loc)
            .cloned()
            .unwrap_or_else(|| pumpkin_cluster::inventory::InventoryCell::empty(action.source.loc));
        if actual != action.expected_item {
            return Err(InventoryApplyError::Rejected);
        }
        let Some(after) = action.source_after.as_ref() else {
            return Ok(None);
        };
        if after.loc != action.source.loc {
            return Err(InventoryApplyError::Rejected);
        }
        if *after == actual {
            return Ok(None);
        }
        Ok(Some(
            pumpkin_cluster::inventory::capture_inventory_mutation(
                action.actor,
                action.sequence,
                action.tick,
                vec![(action.source.target, snapshot.revision)],
                vec![SemanticInventoryOp {
                    target: action.source.target,
                    before: actual,
                    after: after.clone(),
                }],
                action.actor_chunk,
                action.affected.clone(),
            ),
        ))
    }

    pub(super) fn preview_item_source(
        &self,
        action: &ItemAction,
    ) -> Result<InventoryDelta, InventoryApplyError> {
        let Some(op) = self.item_source_op(action)? else {
            return Ok(InventoryDelta {
                revisions: Vec::new(),
                operations: Vec::new(),
            });
        };
        validate_inventory_changes(&op.changes)?;
        let local = self.ledger.clone().apply_op_result(&op)?;
        let ground = self.ground_ledger.clone().apply_op_result(&op)?;
        (local == ground)
            .then_some(local)
            .ok_or(InventoryApplyError::Rejected)
    }

    pub(super) fn preview_one(
        &self,
        original: &InventoryOp,
    ) -> Result<InventoryDelta, InventoryApplyError> {
        validate_inventory_changes(&original.changes)?;
        let rebased = if original.menu_effects.is_empty() {
            self.ledger
                .rebase_gesture_op(original)
                .map_err(|_| InventoryApplyError::Rejected)?
        } else {
            original.clone()
        };
        self.beacon_effect_preflight(&rebased)?;
        let local = self.ledger.clone().apply_op_result(&rebased)?;
        let ground = self.ground_ledger.clone().apply_op_result(&rebased)?;
        (local == ground)
            .then_some(local)
            .ok_or(InventoryApplyError::Rejected)
    }

    pub(super) fn apply_item_source(
        &mut self,
        action: &ItemAction,
    ) -> Result<InventoryDelta, InventoryApplyError> {
        let Some(op) = self.item_source_op(action)? else {
            return Ok(InventoryDelta {
                revisions: Vec::new(),
                operations: Vec::new(),
            });
        };
        validate_inventory_changes(&op.changes)?;
        let mut local = self.ledger.clone();
        let mut ground = self.ground_ledger.clone();
        let local_delta = local.apply_op_result(&op)?;
        let ground_delta = ground.apply_op_result(&op)?;
        if local_delta != ground_delta {
            return Err(InventoryApplyError::Rejected);
        }
        self.ledger = local;
        self.ground_ledger = ground;
        Ok(local_delta)
    }

    pub(super) fn beacon_effect_preflight(
        &self,
        operation: &InventoryOp,
    ) -> Result<Option<(InventoryTarget, BeaconMenuState)>, InventoryApplyError> {
        let mut pending = None;
        for effect in &operation.menu_effects {
            let InventoryMenuEffect::BeaconConfigured {
                target,
                before,
                primary_effect,
                secondary_effect,
            } = effect
            else {
                return Err(InventoryApplyError::Rejected);
            };
            let payment_valid = operation.changes.len() == 1
                && operation.changes.first().is_some_and(|change| {
                    change.target == *target
                        && change.before.loc
                            == pumpkin_cluster::inventory::InvLoc::new(
                                pumpkin_cluster::inventory::INV_MAIN,
                                0,
                            )
                        && change.after.loc == change.before.loc
                        && change.before.count > 0
                        && pumpkin_data::tag::Item::MINECRAFT_BEACON_PAYMENT_ITEMS
                            .1
                            .contains(&change.before.item)
                        && if change.before.count == 1 {
                            change.after.is_empty()
                        } else {
                            change.after.item == change.before.item
                                && change.after.count == change.before.count - 1
                                && change.after.nbt == change.before.nbt
                        }
                });
            if pending.is_some()
                || operation.menu_effects.len() != 1
                || !matches!(target, InventoryTarget::Block { .. })
                || !payment_valid
                || self.beacons.get(target) != Some(before)
                || !self.beacon_handles.contains_key(target)
                || !pumpkin_cluster::inventory::beacon_choice_valid(
                    before.levels,
                    *primary_effect,
                    *secondary_effect,
                )
                || !operation
                    .expected_revisions
                    .iter()
                    .any(|(inventory, _)| inventory == target)
            {
                return Err(InventoryApplyError::Rejected);
            }
            if let Some(payment) = operation.changes.first() {
                super::cluster_invsee::item_stack_from_inventory_cell(&payment.before)?;
                super::cluster_invsee::item_stack_from_inventory_cell(&payment.after)?;
            }
            pending = Some((
                *target,
                BeaconMenuState {
                    revision: before.revision.wrapping_add(1),
                    levels: before.levels,
                    primary_effect: Some(*primary_effect),
                    secondary_effect: *secondary_effect,
                },
            ));
        }
        if pending.is_some() {
            self.ledger.clone().apply_op_result(operation)?;
        }
        Ok(pending)
    }

    pub(super) fn take_beacon_publications(
        &mut self,
    ) -> Vec<(InventoryTarget, BeaconMenuState, Arc<dyn Inventory>)> {
        std::mem::take(&mut self.beacon_publications)
    }

    fn refresh_beacon_menus(&mut self, server: &Server, target: InventoryTarget) {
        let (Some(snapshot), Some(state)) = (
            self.ledger.snapshot(target),
            self.beacons.get(&target).copied(),
        ) else {
            return;
        };
        let payment_valid = snapshot.cells.iter().any(|cell| {
            pumpkin_data::tag::Item::MINECRAFT_BEACON_PAYMENT_ITEMS
                .1
                .contains(&cell.item)
        });
        for screen in super::cluster_invsee::inventory_screens_for_target(server, target) {
            if screen.window_kind == pumpkin_data::screen::WindowType::Beacon as u16
                && let Ok(menu) = pumpkin_cluster::inventory::beacon_menu_metadata(
                    &screen,
                    &snapshot,
                    state,
                    payment_valid,
                )
            {
                self.menu_metadata.insert(screen.session, menu);
            }
        }
    }

    pub(super) fn lifecycle(
        &mut self,
        server: &Server,
        lifecycle: LocalInventoryLifecycle,
    ) -> Option<LocalInventoryScreenCommand> {
        match lifecycle {
            LocalInventoryLifecycle::BeginDeparture {
                actor,
                token,
                input_sequence,
            } => {
                let active = server
                    .cluster_inventory_screens
                    .load()
                    .values()
                    .filter(|screen| screen.session.actor == actor && screen.session.session != 0)
                    .max_by_key(|screen| screen.session.session)
                    .cloned();
                let session = active
                    .as_ref()
                    .map_or(InventoryScreenSession { actor, session: 0 }, |screen| {
                        screen.session
                    });
                let mut affected = self
                    .ledger
                    .target_chunks(InventoryTarget::Player(actor))
                    .unwrap_or_default()
                    .to_vec();
                if let Some(screen) = active.as_ref() {
                    for slot in &screen.slots {
                        if matches!(slot.slot.target, InventoryTarget::Virtual { kind, .. } if kind != 0x100)
                        {
                            affected.extend(
                                self.ledger
                                    .target_chunks(slot.slot.target)
                                    .into_iter()
                                    .flatten()
                                    .copied(),
                            );
                        }
                    }
                }
                affected.sort_unstable();
                affected.dedup();
                if let Some(screen) = active
                    && let Some(player) =
                        super::cluster_invsee::find_inventory_player(server, actor)
                {
                    if super::cluster_inventory_protocol::publish_cluster_screen_close(
                        &player,
                        screen.sync_id,
                        screen.window_kind,
                    )
                    .is_err()
                    {
                        player.client.try_kick(
                            crate::net::DisconnectReason::Unknown,
                            &pumpkin_util::text::TextComponent::text(
                                "Inventory close unavailable during departure",
                            ),
                        );
                    }
                }
                Some(LocalInventoryScreenCommand {
                    command: InventoryScreenCommand::Close { session },
                    tick: pumpkin_cluster::time::TickStamp::default(),
                    affected,
                    origin: None,
                    input_sequence: Some(input_sequence),
                    close_token: Some(token),
                })
            }
            regular => {
                self.lifecycle_regular(server, regular);
                None
            }
        }
    }

    fn lifecycle_regular(&mut self, server: &Server, lifecycle: LocalInventoryLifecycle) {
        match lifecycle {
            LocalInventoryLifecycle::InstallPlayer {
                gid,
                token,
                player,
                crafting,
                ender,
                screen,
                chunks,
            } => {
                if let Some(cancelled_token) = token.filter(|token| {
                    server
                        .cluster_inventory_cancel_requests
                        .load()
                        .contains(&(gid, *token))
                }) {
                    server.cluster_inventory_cancel_requests.rcu(|published| {
                        let mut next = (**published).clone();
                        next.remove(&(gid, cancelled_token));
                        Arc::new(next)
                    });
                    return;
                }
                let revision = player.revision;
                let result = (|| {
                    if player.target != InventoryTarget::Player(gid)
                        || !matches!(
                            crafting.target,
                            InventoryTarget::Virtual {
                                owner: ActorId::Player(owner),
                                session: 0,
                                ..
                            } if owner == gid
                        )
                        || screen.session.actor != gid
                        || ender.target != super::cluster_invsee::ender_chest_target(gid)
                        || screen.session.session != 0
                        || chunks.is_empty()
                        || screen.affected != chunks
                    {
                        return Err(InventoryApplyError::Rejected);
                    }
                    super::cluster_invsee::validate_imported_inventory_snapshots(&[
                        player.clone(),
                        crafting.clone(),
                        ender.clone(),
                    ])?;
                    let mut next = self.ledger.clone();
                    let mut ground_next = self.ground_ledger.clone();
                    next.install_owned_snapshot(player.clone(), chunks.clone())
                        .and_then(|_| next.install_owned_snapshot(crafting.clone(), chunks.clone()))
                        .and_then(|_| next.install_owned_snapshot(ender.clone(), chunks))
                        .map_err(|_| InventoryApplyError::Rejected)?;
                    for target in [player.clone(), crafting.clone(), ender.clone()] {
                        let owned_chunks = next
                            .target_chunks(target.target)
                            .ok_or(InventoryApplyError::Rejected)?
                            .to_vec();
                        ground_next
                            .install_owned_snapshot(target, owned_chunks)
                            .map_err(|_| InventoryApplyError::Rejected)?;
                    }
                    self.ledger = next;
                    self.ground_ledger = ground_next;
                    super::cluster_invsee::publish_inventory_snapshots(
                        server,
                        [player, crafting, ender],
                    );
                    super::cluster_invsee::publish_inventory_screen(server, screen);
                    self.pending_invsee_changes.insert(gid, revision);
                    Ok(revision)
                })();
                if let Some(token) = token {
                    server.cluster_inventory_handoff_installed.rcu(|published| {
                        let mut next = (**published).clone();
                        next.insert(
                            gid,
                            InventoryHandoffInstallStatus {
                                token,
                                revision: result,
                            },
                        );
                        Arc::new(next)
                    });
                }
            }
            LocalInventoryLifecycle::Install {
                snapshot,
                chunks,
                mut screen,
                menu,
            } => {
                let target = snapshot.target;
                let published = if let Some(existing) = self.ledger.snapshot(target) {
                    if self.ground_ledger.snapshot(target) != Some(existing.clone()) {
                        return;
                    }
                    existing
                } else {
                    let mut next = self.ledger.clone();
                    let mut ground_next = self.ground_ledger.clone();
                    if next
                        .install_owned_snapshot(snapshot.clone(), chunks.clone())
                        .is_err()
                        || ground_next
                            .install_owned_snapshot(snapshot.clone(), chunks)
                            .is_err()
                    {
                        return;
                    }
                    self.ledger = next;
                    self.ground_ledger = ground_next;
                    snapshot
                };
                if let Some(ref mut screen) = screen {
                    for slot in &mut screen.slots {
                        if slot.slot.target == target {
                            slot.cell = published
                                .cells
                                .iter()
                                .find(|cell| cell.loc == slot.slot.loc)
                                .cloned()
                                .unwrap_or_else(|| {
                                    pumpkin_cluster::inventory::InventoryCell::empty(slot.slot.loc)
                                });
                        }
                    }
                }
                let menu = if let Some(ref screen) = screen
                    && screen.window_kind == pumpkin_data::screen::WindowType::Beacon as u16
                    && let Some(state) = self.beacons.get(&target)
                {
                    let payment_valid = published.cells.iter().any(|cell| {
                        pumpkin_data::tag::Item::MINECRAFT_BEACON_PAYMENT_ITEMS
                            .1
                            .contains(&cell.item)
                    });
                    pumpkin_cluster::inventory::beacon_menu_metadata(
                        screen,
                        &published,
                        *state,
                        payment_valid,
                    )
                    .ok()
                } else {
                    menu
                };
                super::cluster_invsee::publish_inventory_snapshots(server, [published]);
                if let Some(screen) = screen {
                    if let Some(menu) = menu {
                        self.menu_metadata.insert(screen.session, menu);
                    }
                    super::cluster_invsee::publish_inventory_screen(server, screen.clone());
                    self.queue_invsee_screen(&screen, true);
                }
                if let InventoryTarget::Player(gid) = target {
                    if let Some(snapshot) = self.ledger.snapshot(target) {
                        self.pending_invsee_changes.insert(gid, snapshot.revision);
                    }
                }
            }
            LocalInventoryLifecycle::Relocate { target, chunks } => {
                let mut next = self.ledger.clone();
                let mut ground_next = self.ground_ledger.clone();
                if next.relocate_target(target, chunks.clone()).is_ok()
                    && ground_next.relocate_target(target, chunks).is_ok()
                {
                    self.ledger = next;
                    self.ground_ledger = ground_next;
                    for mut screen in
                        super::cluster_invsee::inventory_screens_for_target(server, target)
                    {
                        screen.affected = screen
                            .slots
                            .iter()
                            .flat_map(|slot| {
                                self.ledger
                                    .target_chunks(slot.slot.target)
                                    .into_iter()
                                    .flatten()
                                    .copied()
                            })
                            .collect();
                        screen.affected.sort_unstable();
                        screen.affected.dedup();
                        super::cluster_invsee::publish_inventory_screen(server, screen);
                    }
                    if let InventoryTarget::Player(gid) = target {
                        self.pending_invsee_relocations.insert(gid);
                    }
                }
            }
            LocalInventoryLifecycle::Remove { target } => {
                for screen in super::cluster_invsee::inventory_screens_for_target(server, target) {
                    self.queue_invsee_screen(&screen, false);
                }
                self.ledger.remove_target(target);
                self.ground_ledger.remove_target(target);
                self.beacons.remove(&target);
                self.beacon_handles.remove(&target);
                server.cluster_beacon_states.rcu(|current| {
                    let mut next = (**current).clone();
                    next.remove(&target);
                    Arc::new(next)
                });
                super::cluster_invsee::unpublish_inventory_snapshot(server, target);
                super::cluster_invsee::unpublish_inventory_screens_for_target(server, target);
                if let InventoryTarget::Player(gid) = target {
                    self.pending_invsee_changes.remove(&gid);
                    self.pending_invsee_removals.insert(gid);
                }
            }
            LocalInventoryLifecycle::OpenScreen {
                screen,
                menu,
                presentation,
            } => {
                let actor =
                    super::cluster_invsee::find_inventory_player(server, screen.session.actor);
                if actor.as_ref().is_none_or(|player| {
                    player
                        .cluster_departing
                        .load(std::sync::atomic::Ordering::Acquire)
                        || player.client.closed()
                        || player
                            .cluster_current_inventory_session
                            .load(std::sync::atomic::Ordering::Acquire)
                            != screen.session.session
                }) {
                    if let Some(player) = actor.as_ref() {
                        if player
                            .cluster_current_inventory_session
                            .compare_exchange(
                                screen.session.session,
                                u64::MAX,
                                std::sync::atomic::Ordering::AcqRel,
                                std::sync::atomic::Ordering::Acquire,
                            )
                            .is_ok()
                        {
                            player.cluster_inventory_screen_target.store(None);
                            player
                                .cluster_current_inventory_session
                                .store(0, std::sync::atomic::Ordering::Release);
                        }
                    }
                    return;
                }
                if let Some(presentation) = &presentation
                    && super::cluster_inventory_protocol::publish_cluster_screen_open(
                        server,
                        &screen,
                        presentation,
                    )
                    .is_err()
                {
                    if let Some(player) =
                        super::cluster_invsee::find_inventory_player(server, screen.session.actor)
                        && player
                            .cluster_current_inventory_session
                            .compare_exchange(
                                screen.session.session,
                                u64::MAX,
                                std::sync::atomic::Ordering::AcqRel,
                                std::sync::atomic::Ordering::Acquire,
                            )
                            .is_ok()
                    {
                        player.cluster_inventory_screen_target.store(None);
                        player
                            .cluster_current_inventory_session
                            .store(0, std::sync::atomic::Ordering::Release);
                    }
                    return;
                }
                if let Some(player) = actor.as_ref()
                    && (player
                        .cluster_departing
                        .load(std::sync::atomic::Ordering::Acquire)
                        || player.client.closed())
                {
                    if presentation.is_some()
                        && super::cluster_inventory_protocol::publish_cluster_screen_close(
                            player,
                            screen.sync_id,
                            screen.window_kind,
                        )
                        .is_err()
                    {
                        player.client.try_kick(
                            crate::net::DisconnectReason::Unknown,
                            &pumpkin_util::text::TextComponent::text(
                                "Inventory screen closed during departure",
                            ),
                        );
                    }
                    if player
                        .cluster_current_inventory_session
                        .compare_exchange(
                            screen.session.session,
                            u64::MAX,
                            std::sync::atomic::Ordering::AcqRel,
                            std::sync::atomic::Ordering::Acquire,
                        )
                        .is_ok()
                    {
                        player.cluster_inventory_screen_target.store(None);
                        player
                            .cluster_current_inventory_session
                            .store(0, std::sync::atomic::Ordering::Release);
                    }
                    return;
                }
                if let Some(menu) = menu {
                    self.menu_metadata.insert(screen.session, menu);
                }
                super::cluster_invsee::publish_inventory_screen(server, screen.clone());
                self.queue_invsee_screen(&screen, true);
                if screen.slots.iter().any(|slot| {
                    matches!(
                        slot.slot.target,
                        InventoryTarget::Virtual { kind: 0x100, .. }
                    )
                }) && let Some(player) =
                    super::cluster_invsee::find_inventory_player(server, screen.session.actor)
                    && !player.is_spectator()
                    && self.ender_open_sessions.insert(screen.session)
                {
                    player.ender_chest_inventory().on_open();
                }
            }
            LocalInventoryLifecycle::CloseScreen { session } => {
                if let Some(screen) =
                    super::cluster_invsee::inventory_screen_snapshot(server, session)
                {
                    self.queue_invsee_screen(&screen, false);
                }
                super::cluster_invsee::unpublish_inventory_screen(server, session);
                self.screen_chunks.remove(&session);
                self.menu_metadata.remove(&session);
            }
            LocalInventoryLifecycle::BeginDeparture { .. } => unreachable!(),
            LocalInventoryLifecycle::RefreshRemotePreview {
                session,
                target,
                expected_revision,
                snapshot,
            } => {
                let Some(current) =
                    super::cluster_invsee::inventory_screen_snapshot(server, session)
                else {
                    self.pending_invsee_rejected
                        .insert((session, target, expected_revision), true);
                    return;
                };
                let Some(viewer_chunk) = self
                    .ledger
                    .target_chunks(InventoryTarget::Player(session.actor))
                    .and_then(|chunks| chunks.first().copied())
                else {
                    self.pending_invsee_rejected
                        .insert((session, target, expected_revision), false);
                    return;
                };
                let Ok(refreshed) = super::cluster_invsee::refreshed_invsee_screen(
                    &current,
                    target,
                    expected_revision,
                    viewer_chunk,
                    &snapshot,
                ) else {
                    let retry = current.preview.as_ref().is_some_and(|preview| {
                        preview.target == InventoryTarget::Player(target)
                            && preview.revision != expected_revision
                    });
                    self.pending_invsee_rejected
                        .insert((session, target, expected_revision), retry);
                    return;
                };
                let revision = refreshed.preview.as_ref().map(|preview| preview.revision);
                super::cluster_invsee::publish_inventory_screen(server, refreshed.clone());
                let before = std::iter::once((session, current.clone())).collect();
                let after = std::iter::once((session, refreshed.clone())).collect();
                let egress = super::cluster_inventory_protocol::publish_inventory_screen_deltas(
                    server, &before, &after,
                );
                if egress.is_err() {
                    self.pending_invsee_egress
                        .insert(session, (current, refreshed));
                } else if let Some(revision) = revision {
                    self.pending_invsee_applied
                        .insert((session, target), revision);
                }
            }
            LocalInventoryLifecycle::BeaconSnapshot {
                target,
                state,
                handle,
            } => {
                if matches!(target, InventoryTarget::Block { .. }) {
                    let current = match self.beacons.get(&target).copied() {
                        Some(existing) if existing.revision >= state.revision => existing,
                        _ => {
                            self.beacons.insert(target, state);
                            state
                        }
                    };
                    self.beacon_handles.insert(target, handle);
                    server.cluster_beacon_states.rcu(|published| {
                        let mut next = (**published).clone();
                        next.insert(target, current);
                        Arc::new(next)
                    });
                    self.refresh_beacon_menus(server, target);
                }
            }
        }
    }

    pub(super) fn action(
        &mut self,
        server: &Server,
        action: LocalInventoryAction,
    ) -> Option<(TickBatch, TickFinish)> {
        let sequence = self
            .sequences
            .entry(action.intent.actor)
            .or_default()
            .issue_or_observe(action.input_sequence);
        let actor = action.intent.actor;
        let tick = action.intent.tick;
        let targets = pumpkin_cluster::inventory::inventory_intent_targets(&action.intent);
        let expected_revisions = match &action.intent.payload {
            pumpkin_cluster::inventory::InventoryIntentPayload::Mutation(mutation) => {
                mutation.expected_revisions.clone()
            }
            pumpkin_cluster::inventory::InventoryIntentPayload::Gestures(_) => Vec::new(),
        };
        let screen_session = action.intent.screen_session;
        let preview = action
            .intent
            .screen_session
            .and_then(|session| (ActorId::Player(session.actor) == actor).then_some(session))
            .and_then(|session| super::cluster_invsee::inventory_screen_snapshot(server, session))
            .filter(|screen| {
                screen
                    .affected
                    .iter()
                    .all(|chunk| action.intent.affected.contains(chunk))
            })
            .and_then(|screen| screen.preview);
        let operation =
            match action
                .intent
                .into_action_with_preview(sequence, &self.ledger, preview.as_ref())
            {
                Ok(operation) => operation,
                Err(_) => {
                    if let (Some(session), Some(preview)) = (screen_session, preview.as_ref())
                        && let InventoryTarget::Player(target) = preview.target
                    {
                        let _ = super::cluster_invsee::request_inventory_screen_refresh(
                            server,
                            session,
                            target,
                            preview.revision,
                        );
                    }
                    self.reject_input(
                        server,
                        actor,
                        tick,
                        Some(sequence),
                        action.origin,
                        targets,
                        &expected_revisions,
                    );
                    if let ActorId::Player(actor) = actor
                        && let Some(player) =
                            super::cluster_invsee::find_inventory_player(server, actor)
                    {
                        super::cluster_inventory_protocol::resync_inventory_screen(&player);
                    }
                    return None;
                }
            };
        let key = ActionKey::inventory(&operation);
        let outgoing = batch(operation)?;
        if let (Some(session), Some(preview)) = (screen_session, preview)
            && let InventoryTarget::Player(target) = preview.target
        {
            self.remote_previews
                .insert(key, (session, target, preview.revision));
        }
        self.origins.insert(key, action.origin);
        Some(outgoing)
    }

    fn reject_input(
        &mut self,
        server: &Server,
        actor: ActorId,
        tick: pumpkin_cluster::time::TickStamp,
        input_sequence: Option<pumpkin_cluster::identity::PlayerSeq>,
        origin: Option<u64>,
        targets: impl IntoIterator<Item = InventoryTarget>,
        expected_revisions: &[(InventoryTarget, u64)],
    ) {
        let sequence = self
            .sequences
            .entry(actor)
            .or_default()
            .issue_or_observe(input_sequence);
        let mut targets: Vec<_> = targets.into_iter().collect();
        targets.sort_unstable();
        targets.dedup();
        let snapshots: Vec<_> = targets
            .into_iter()
            .filter_map(|target| self.ledger.snapshot(target))
            .collect();
        let correction = InventoryDelta {
            revisions: snapshots
                .iter()
                .filter_map(|snapshot| {
                    expected_revisions.iter().find_map(|(target, expected)| {
                        (*target == snapshot.target).then_some((
                            snapshot.target,
                            *expected,
                            snapshot.revision,
                        ))
                    })
                })
                .collect(),
            operations: Vec::new(),
        };
        let _ = super::cluster_invsee::publish_inventory_resolution(
            server,
            pumpkin_cluster::inventory::InventoryResolution {
                action: ActionKey {
                    actor,
                    sequence: sequence.0,
                    family: 2,
                },
                tick,
                result: Err(InventoryApplyError::Rejected),
                authoritative: snapshots,
                correction,
                effects: Vec::new(),
                menu_effects: Vec::new(),
            },
            origin,
        );
    }

    pub(super) fn reject_untrusted_menu(
        &mut self,
        server: &Server,
        staged: super::cluster_inventory_protocol::LocalInventoryMenuIntent,
    ) {
        let actor = staged.intent.actor;
        let player = InventoryTarget::Player(staged.intent.session.actor);
        let targets = self.menu_targets(&staged).unwrap_or_else(|| vec![player]);
        self.reject_input(
            server,
            actor,
            staged.intent.tick,
            staged.input_sequence,
            staged.origin,
            targets,
            &staged.intent.expected_revisions,
        );
    }

    pub(super) fn reject_untrusted_screen(
        &mut self,
        server: &Server,
        staged: LocalInventoryScreenCommand,
    ) {
        let session = match staged.command {
            InventoryScreenCommand::BeginQuickCraft { session, .. }
            | InventoryScreenCommand::AddQuickCraftSlot { session, .. }
            | InventoryScreenCommand::FinishQuickCraft { session }
            | InventoryScreenCommand::Close { session } => session,
        };
        self.reject_input(
            server,
            ActorId::Player(session.actor),
            staged.tick,
            staged.input_sequence,
            staged.origin,
            [InventoryTarget::Player(session.actor)],
            &[],
        );
    }

    pub(super) fn menu(
        &mut self,
        server: &Server,
        staged: super::cluster_inventory_protocol::LocalInventoryMenuIntent,
    ) -> Option<(TickBatch, TickFinish)> {
        let session = staged.intent.session;
        let screen = super::cluster_invsee::inventory_screen_snapshot(server, session);
        let metadata = self.menu_metadata.get(&session).cloned();
        if staged.intent.actor != ActorId::Player(session.actor) {
            self.reject_untrusted_menu(server, staged);
            return None;
        }
        let Some(screen) = screen else {
            self.reject_untrusted_menu(server, staged);
            return None;
        };
        let mut allowed: Vec<_> = screen.slots.iter().map(|slot| slot.slot.target).collect();
        allowed.push(InventoryTarget::Player(session.actor));
        allowed.sort_unstable();
        allowed.dedup();
        let trusted_revisions = metadata
            .as_ref()
            .map_or(&staged.intent.expected_revisions, |metadata| {
                &metadata.expected_revisions
            });
        let snapshots: Vec<_> = trusted_revisions
            .iter()
            .filter(|(target, _)| allowed.contains(target))
            .filter_map(|(target, _)| self.ledger.snapshot(*target))
            .collect();
        let sequence = self
            .sequences
            .entry(staged.intent.actor)
            .or_default()
            .issue_or_observe(staged.input_sequence);
        let operation = metadata
            .as_ref()
            .and_then(|metadata| {
                if metadata
                    .expected_revisions
                    .iter()
                    .any(|(target, _)| !allowed.contains(target))
                {
                    return None;
                }
                let mut intent = staged.intent.clone();
                intent.expected_revisions = metadata.expected_revisions.clone();
                reduce_inventory_menu(&screen, &snapshots, metadata, intent).ok()
            })
            .and_then(|reduction| reduction.into_action(sequence, &self.ledger).ok());
        let Some(operation) = operation else {
            let rejected = pumpkin_cluster::inventory::capture_inventory_mutation(
                staged.intent.actor,
                sequence,
                staged.intent.tick,
                staged.intent.expected_revisions,
                Vec::new(),
                staged.intent.actor_chunk,
                staged.intent.affected,
            );
            let correction =
                pumpkin_cluster::inventory::corrective_inventory_delta(&rejected, &snapshots);
            let _ = super::cluster_invsee::publish_inventory_resolution(
                server,
                pumpkin_cluster::inventory::InventoryResolution {
                    action: ActionKey::inventory(&rejected),
                    tick: rejected.tick,
                    result: Err(InventoryApplyError::Rejected),
                    authoritative: snapshots,
                    correction,
                    effects: Vec::new(),
                    menu_effects: Vec::new(),
                },
                staged.origin,
            );
            return None;
        };
        let key = ActionKey::inventory(&operation);
        let outgoing = batch(operation)?;
        self.origins.insert(key, staged.origin);
        Some(outgoing)
    }

    pub(super) fn screen(
        &mut self,
        server: &Server,
        staged: LocalInventoryScreenCommand,
    ) -> Option<(TickBatch, TickFinish)> {
        let session = match &staged.command {
            InventoryScreenCommand::BeginQuickCraft { session, .. }
            | InventoryScreenCommand::AddQuickCraftSlot { session, .. }
            | InventoryScreenCommand::FinishQuickCraft { session }
            | InventoryScreenCommand::Close { session } => *session,
        };
        self.screen_chunks
            .entry(session)
            .or_default()
            .extend(staged.affected.iter().copied());
        let closing = matches!(&staged.command, InventoryScreenCommand::Close { .. });
        if closing {
            return self.close_screen(server, staged, session);
        }
        let gesture = self.screen_sessions.apply(staged.command);
        let gesture = gesture?;
        let player = server
            .get_all_players()
            .into_iter()
            .find(|player| player.cluster_gid() == Some(session.actor))?;
        let position = player.living_entity.entity.pos.load();
        let actor_chunk = ChunkKey::new(
            super::cluster_world_delta::world_id(&player.world()),
            (position.x.floor() as i32).div_euclid(16),
            (position.z.floor() as i32).div_euclid(16),
        );
        let intent = InventoryMutationIntent {
            actor: session.actor.into(),
            tick: staged.tick,
            payload: InventoryIntentPayload::Gestures(vec![gesture]),
            screen_session: Some(session),
            actor_chunk,
            affected: self
                .screen_chunks
                .get(&session)
                .into_iter()
                .flat_map(|chunks| chunks.iter().copied())
                .collect(),
        };
        let sequence = self
            .sequences
            .entry(session.actor.into())
            .or_default()
            .issue_or_observe(staged.input_sequence);
        let preview = super::cluster_invsee::inventory_screen_snapshot(server, session)
            .and_then(|screen| screen.preview);
        let operation = intent
            .into_action_with_preview(sequence, &self.ledger, preview.as_ref())
            .ok()?;
        let key = ActionKey::inventory(&operation);
        let outgoing = batch(operation)?;
        self.origins.insert(key, staged.origin);
        Some(outgoing)
    }

    fn close_screen(
        &mut self,
        server: &Server,
        staged: LocalInventoryScreenCommand,
        session: InventoryScreenSession,
    ) -> Option<(TickBatch, TickFinish)> {
        let screen = super::cluster_invsee::inventory_screen_snapshot(server, session)?;
        let player_target = InventoryTarget::Player(session.actor);
        self.ledger.snapshot(player_target)?;
        let virtual_target = screen
            .slots
            .iter()
            .find(|slot| {
                matches!(slot.slot.target, InventoryTarget::Virtual { kind, .. } if kind != 0x100)
            })
            .map(|slot| slot.slot.target);
        if let Some(target) = virtual_target {
            self.ledger.snapshot(target)?;
        }
        let destinations: Vec<_> = screen
            .slots
            .iter()
            .filter(|slot| {
                slot.slot.target == player_target
                    && slot.slot.loc.inv == INV_MAIN
                    && slot.slot.loc.slot < 36
            })
            .map(|slot| (slot.slot, slot.limit))
            .collect();
        let cursor = InventorySlotRef::new(
            player_target,
            pumpkin_cluster::inventory::InvLoc::new(INV_CURSOR, 0),
        );
        let mut sources = vec![cursor];
        if let Some(target) = virtual_target {
            sources.extend(screen.slots.iter().filter_map(|slot| {
                (slot.slot.target == target
                    && slot.screen_slot < screen.container_slots
                    && !close_result_slot(
                        screen.window_kind,
                        slot.screen_slot,
                        session.session == 0,
                    ))
                .then_some(slot.slot)
            }));
        }
        let mut gestures = Vec::new();
        for source in sources {
            gestures.push(InventoryGesture::DrainToPlayer {
                player: session.actor,
                source,
                destinations: destinations.clone(),
            });
        }
        if let Some(target) = virtual_target {
            for slot in &screen.slots {
                if slot.slot.target != target
                    || slot.screen_slot >= screen.container_slots
                    || !close_result_slot(
                        screen.window_kind,
                        slot.screen_slot,
                        session.session == 0,
                    )
                {
                    continue;
                }
                gestures.push(InventoryGesture::ClearResult { slot: slot.slot });
            }
        }
        let player = server
            .get_all_players()
            .into_iter()
            .find(|player| player.cluster_gid() == Some(session.actor))?;
        let position = player.living_entity.entity.pos.load();
        let actor_chunk = ChunkKey::new(
            super::cluster_world_delta::world_id(&player.world()),
            (position.x.floor() as i32).div_euclid(16),
            (position.z.floor() as i32).div_euclid(16),
        );
        let mut affected = self
            .ledger
            .target_chunks(player_target)
            .unwrap_or_default()
            .to_vec();
        if let Some(target) = virtual_target {
            affected.extend(
                self.ledger
                    .target_chunks(target)
                    .into_iter()
                    .flatten()
                    .copied(),
            );
        }
        affected.push(actor_chunk);
        affected.sort_unstable();
        affected.dedup();
        let sequence = self
            .sequences
            .entry(session.actor.into())
            .or_default()
            .issue_or_observe(staged.input_sequence);
        let operation = InventoryMutationIntent {
            actor: session.actor.into(),
            tick: staged.tick,
            payload: InventoryIntentPayload::Gestures(gestures),
            screen_session: Some(session),
            actor_chunk,
            affected,
        }
        .into_action(sequence, &self.ledger)
        .ok()?;
        let key = ActionKey::inventory(&operation);
        let outgoing = batch(operation)?;
        self.closing
            .insert(key, (session, virtual_target, staged.close_token?));
        self.origins.insert(key, staged.origin);
        Some(outgoing)
    }

    pub(super) fn apply_one(
        &mut self,
        server: &Server,
        tick: pumpkin_cluster::time::TickStamp,
        original: InventoryOp,
    ) {
        let key = ActionKey::inventory(&original);
        let rebased = validate_inventory_changes(&original.changes).and_then(|_| {
            if original.menu_effects.is_empty() {
                self.ledger
                    .rebase_gesture_op(&original)
                    .map_err(|_| InventoryApplyError::Rejected)
            } else {
                Ok(original.clone())
            }
        });
        let (rebased, result) = match rebased {
            Ok(rebased) => {
                let beacon = self.beacon_effect_preflight(&rebased);
                let result = match beacon {
                    Ok(pending) => {
                        let result = (|| {
                            let mut local = self.ledger.clone();
                            let mut ground = self.ground_ledger.clone();
                            let local_delta = local.apply_op_result(&rebased)?;
                            let ground_delta = ground.apply_op_result(&rebased)?;
                            if local_delta != ground_delta {
                                return Err(InventoryApplyError::Rejected);
                            }
                            self.ledger = local;
                            self.ground_ledger = ground;
                            Ok(local_delta)
                        })();
                        if result.is_ok() {
                            if let Some((target, state)) = pending {
                                self.beacons.insert(target, state);
                                if let Some(handle) = self.beacon_handles.get(&target) {
                                    self.beacon_publications
                                        .push((target, state, handle.clone()));
                                }
                                server.cluster_beacon_states.rcu(|published| {
                                    let mut next = (**published).clone();
                                    next.insert(target, state);
                                    Arc::new(next)
                                });
                            }
                        }
                        result
                    }
                    Err(error) => Err(error),
                };
                (rebased, result)
            }
            Err(_) => (
                original.clone(),
                Err(pumpkin_cluster::inventory::InventoryApplyError::Rejected),
            ),
        };
        let mut targets: Vec<_> = original
            .expected_revisions
            .iter()
            .map(|(target, _)| *target)
            .chain(original.changes.iter().map(|change| change.target))
            .collect();
        targets.sort_unstable();
        targets.dedup();
        let authoritative: Vec<_> = targets
            .into_iter()
            .filter_map(|target| self.ledger.snapshot(target))
            .collect();
        let correction =
            pumpkin_cluster::inventory::corrective_inventory_delta(&original, &authoritative);
        let effects = result
            .is_ok()
            .then(|| rebased.effects.clone())
            .unwrap_or_default();
        let closing = self.closing.remove(&key);
        let accepted_close = result.is_ok();
        let menu_effects = result
            .is_ok()
            .then(|| rebased.menu_effects.clone())
            .unwrap_or_default();
        let beacon_targets: Vec<_> = authoritative
            .iter()
            .filter_map(|snapshot| {
                self.beacons
                    .contains_key(&snapshot.target)
                    .then_some(snapshot.target)
            })
            .collect();
        let resolution = pumpkin_cluster::inventory::InventoryResolution {
            action: key,
            tick,
            result,
            authoritative,
            correction,
            effects,
            menu_effects,
        };
        if let Some((session, target, revision)) = self.remote_previews.remove(&key)
            && resolution.result.is_err()
        {
            let _ = super::cluster_invsee::request_inventory_screen_refresh(
                server, session, target, revision,
            );
        }
        let published = super::cluster_invsee::publish_inventory_resolution(
            server,
            resolution,
            self.origins.remove(&key).flatten(),
        )
        .is_ok();
        if let Some((session, virtual_target, token)) = closing
            && accepted_close
            && published
        {
            self.screen_sessions.close(session);
            self.screen_chunks.remove(&session);
            if session.session != 0 {
                if self.ender_open_sessions.remove(&session)
                    && let Some(player) =
                        super::cluster_invsee::find_inventory_player(server, session.actor)
                {
                    player.ender_chest_inventory().on_close();
                }
                self.menu_metadata.remove(&session);
                if let Some(screen) =
                    super::cluster_invsee::inventory_screen_snapshot(server, session)
                {
                    self.queue_invsee_screen(&screen, false);
                }
                super::cluster_invsee::unpublish_inventory_screen(server, session);
                if let Some(target) = virtual_target {
                    self.ledger.remove_target(target);
                    self.ground_ledger.remove_target(target);
                    super::cluster_invsee::unpublish_inventory_snapshot(server, target);
                }
            }
            server.cluster_inventory_close_status.rcu(|current| {
                let mut next = (**current).clone();
                if !next
                    .keys()
                    .any(|(actor, existing)| *actor == session.actor && *existing > token)
                {
                    next.retain(|(actor, _), _| *actor != session.actor);
                    next.insert(
                        (session.actor, token),
                        InventoryCloseStatus {
                            actor: session.actor,
                            session: session.session,
                            token,
                            accepted_tick: tick,
                        },
                    );
                }
                Arc::new(next)
            });
        }
        for target in beacon_targets {
            self.refresh_beacon_menus(server, target);
        }
    }
}

fn batch(operation: pumpkin_cluster::inventory::InventoryOp) -> Option<(TickBatch, TickFinish)> {
    let mut batch = TickBatch::new(operation.tick);
    batch.inv_ops.push(operation);
    let finish = TickFinish::try_from_batch(&batch).ok()?;
    Some((batch, finish))
}

fn close_result_slot(window_kind: u16, slot: u16, player_screen: bool) -> bool {
    use pumpkin_data::screen::WindowType;

    if player_screen {
        return slot == 0;
    }
    match window_kind {
        kind if kind == WindowType::Crafting as u16 => slot == 0,
        kind if kind == WindowType::Anvil as u16
            || kind == WindowType::Grindstone as u16
            || kind == WindowType::Merchant as u16
            || kind == WindowType::CartographyTable as u16 =>
        {
            slot == 2
        }
        kind if kind == WindowType::Loom as u16 || kind == WindowType::Smithing as u16 => slot == 3,
        kind if kind == WindowType::Stonecutter as u16 => slot == 1,
        _ => false,
    }
}
