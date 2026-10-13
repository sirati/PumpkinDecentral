use pumpkin_inventory::Inventory;
use std::collections::{BTreeMap, BTreeSet, HashMap, HashSet};
use std::sync::Arc;

use pumpkin_cluster::actor::ActorId;
use pumpkin_cluster::completion::{ActionKey, TickFinish};
use pumpkin_cluster::identity::GlobalPlayerId;
use pumpkin_cluster::inventory::{
    BeaconMenuState, ChunkInventoryState, INV_ARMOR, INV_CURSOR, INV_MAIN, INV_OFFHAND,
    INV_SELECTED, InvLedger, InventoryApplyError, InventoryDelta, InventoryGesture,
    InventoryIntentPayload, InventoryMenuEffect, InventoryMenuMetadata, InventoryMutationIntent,
    InventoryOp, InventoryScreenCommand, InventoryScreenSession, InventoryScreenSessions,
    InventorySequence, InventorySlotRef, InventoryTarget, SemanticInventoryOp,
    reduce_inventory_menu,
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
    menu_roots: HashMap<InventoryTarget, CanonicalMenuInventoryAdmission>,
    block_root_images: Option<Vec<super::cluster_inventory_block_roots::BlockInventoryRootImage>>,
    block_roots: HashMap<InventoryTarget, super::cluster_inventory_block_roots::CanonicalBlockInventoryRoot>,
    chunk: ChunkKey,
    states: Vec<ChunkInventoryState>,
    ledger: InvLedger,
    ground_ledger: InvLedger,
    beacons: HashMap<InventoryTarget, BeaconMenuState>,
    snapshots: Vec<pumpkin_cluster::inventory::InventoryStateSnapshot>,
    player_keys: HashMap<uuid::Uuid, GlobalPlayerId>,
    historical_player_keys: HashMap<GlobalPlayerId, uuid::Uuid>,
    historical_player_leases: HashMap<(GlobalPlayerId, u64), uuid::Uuid>,
    player_incarnations: HashMap<GlobalPlayerId, u64>,
}

impl PreparedInventoryImport {
    pub(super) fn block_root_anchors(&self) -> Vec<(pumpkin_cluster::canonical_action::Target, pumpkin_cluster::canonical_action::Cell)> {
        self.block_roots.values().filter(|root| root.chunk == self.chunk).flat_map(|root| {
            let snapshot = self.ground_ledger.snapshot(root.inventory_target());
            root.bindings().into_iter().filter_map(move |binding| {
                let item = snapshot.as_ref()?.cells.iter().find(|item| item.loc == binding.slot.loc)?.clone();
                Some((binding.target, pumpkin_cluster::canonical_action::Cell { revision: 0,
                    value: pumpkin_cluster::canonical_action::Value::Inventory(item) }))
            })
        }).collect()
    }
}

pub(super) struct CommittedInventoryImport {
    targets: Vec<InventoryTarget>,
}

pub(super) struct PreparedInventoryMutation {
    pub(super) operation: InventoryOp,
    pub(super) delta: InventoryDelta,
    ground_delta: InventoryDelta,
}

#[derive(Clone)]
pub(super) struct PreparedPlayerInventoryRelocation {
    local: InvLedger,
    ground: InvLedger,
    targets: Vec<(InventoryTarget, ChunkKey, ChunkKey, Option<ChunkKey>)>,
}

fn relocated_inventory_chunks(
    ledger: &InvLedger,
    target: InventoryTarget,
    from: ChunkKey,
    to: ChunkKey,
) -> Result<Vec<ChunkKey>, InventoryApplyError> {
    let chunks = ledger
        .target_chunks(target)
        .ok_or(InventoryApplyError::Rejected)?;
    if !chunks.contains(&from) {
        return Err(InventoryApplyError::Rejected);
    }
    if matches!(target, InventoryTarget::Virtual { owner: ActorId::Player(_), .. } | InventoryTarget::LeasedVirtual { .. }) {
        return Ok(vec![to]);
    }
    Ok(chunks.iter().map(|chunk| if *chunk == from { to } else { *chunk }).collect())
}
fn validate_inventory_changes(changes: &[SemanticInventoryOp]) -> Result<(), InventoryApplyError> {
    for change in changes {
        super::cluster_invsee::item_stack_from_inventory_cell(&change.before)?;
        super::cluster_invsee::item_stack_from_inventory_cell(&change.after)?;
    }
    Ok(())
}

fn bind_imported_player_lease(
    target: InventoryTarget,
    lease: Option<pumpkin_cluster::canonical_action::LivePlayerLease>,
    keys: &mut HashMap<uuid::Uuid, GlobalPlayerId>,
    historical_keys: &mut HashMap<GlobalPlayerId, uuid::Uuid>,
    historical_leases: &mut HashMap<(GlobalPlayerId, u64), uuid::Uuid>,
    incarnations: &mut HashMap<GlobalPlayerId, u64>,
) -> Result<(), InventoryApplyError> {
    let player = match target {
        InventoryTarget::Player(gid)
        | InventoryTarget::LeasedPlayer { player: gid, .. }
        | InventoryTarget::LeasedVirtual { player: gid, .. }
        | InventoryTarget::Virtual {
            owner: ActorId::Player(gid),
            ..
        } => Some(gid),
        _ => None,
    };
    match player {
        Some(gid) => {
            let lease = lease.ok_or(InventoryApplyError::Rejected)?;
            if lease.gid != gid
                || matches!(target, InventoryTarget::LeasedPlayer { key, .. } | InventoryTarget::LeasedVirtual { key, .. } if key != lease.key.0)
                || matches!(target, InventoryTarget::LeasedPlayer { incarnation, .. } | InventoryTarget::LeasedVirtual { incarnation, .. } if incarnation != lease.incarnation)
                || lease.incarnation == 0
                || historical_leases
                    .get(&(gid, lease.incarnation))
                    .is_some_and(|known| *known != lease.key.0)
            {
                return Err(InventoryApplyError::Rejected);
            }
            if !matches!(
                target,
                InventoryTarget::LeasedPlayer { .. } | InventoryTarget::LeasedVirtual { .. }
            ) && (historical_keys
                .get(&gid)
                .is_some_and(|known| *known != lease.key.0)
                || incarnations
                    .get(&gid)
                    .is_some_and(|known| *known != lease.incarnation))
            {
                return Err(InventoryApplyError::Rejected);
            }
            historical_leases.insert((gid, lease.incarnation), lease.key.0);
            if matches!(
                target,
                InventoryTarget::LeasedPlayer { .. } | InventoryTarget::LeasedVirtual { .. }
            ) {
                return Ok(());
            }
            historical_keys.insert(gid, lease.key.0);
            keys.entry(lease.key.0).or_insert(gid);
            incarnations.insert(gid, lease.incarnation);
            Ok(())
        }
        None if lease.is_some() => Err(InventoryApplyError::Rejected),
        _ => Ok(()),
    }
}

#[derive(Clone, PartialEq)]
pub(super) struct CanonicalMenuInventoryAdmission {
    pub reference: pumpkin_cluster::canonical_action::ActionRef,
    pub admission: pumpkin_cluster::canonical_menu::MenuVirtualInventoryAdmission,
}

#[derive(Clone, Copy, PartialEq, Eq, PartialOrd, Ord)]
pub(super) enum InventoryUnitReturnKey {
    LocalRoot(InventoryTarget),
    GroundRoot(InventoryTarget),
    BlockRoot(InventoryTarget),
    MenuRoot(InventoryTarget),
    EntityRoot(pumpkin_cluster::protocol::GlobalEntityId),
    PlayerRoot(pumpkin_cluster::canonical_action::LivePlayerLease),
    PlayerKey(uuid::Uuid),
    HistoricalPlayer(GlobalPlayerId),
    HistoricalLease(GlobalPlayerId, u64),
    InstallToken(GlobalPlayerId),
    Incarnation(GlobalPlayerId),
    LatestInstallToken(GlobalPlayerId),
}

pub(super) struct InventoryOwnerInbox<T>(Option<mpsc::Receiver<T>>);

impl<T> Default for InventoryOwnerInbox<T> {
    fn default() -> Self { Self(None) }
}

impl<T> From<mpsc::Receiver<T>> for InventoryOwnerInbox<T> {
    fn from(receiver: mpsc::Receiver<T>) -> Self { Self(Some(receiver)) }
}

impl<T> InventoryOwnerInbox<T> {
    pub(super) fn try_recv(&mut self) -> Result<T, mpsc::error::TryRecvError> {
        self.0.as_mut().map_or(Err(mpsc::error::TryRecvError::Disconnected), |receiver| receiver.try_recv())
    }
}

pub(super) struct InventoryOwner {
    canonical_block_roots: HashMap<InventoryTarget, super::cluster_inventory_block_roots::CanonicalBlockInventoryRoot>,
    canonical_menu_admissions: HashMap<InventoryTarget, CanonicalMenuInventoryAdmission>,
    replacement_epoch: u64,
    canonical_entity_admissions:
        HashMap<pumpkin_cluster::protocol::GlobalEntityId, CanonicalEntityInventoryAdmission>,
    pub(super) actions: InventoryOwnerInbox<LocalInventoryAction>,
    pub(super) screens: InventoryOwnerInbox<LocalInventoryScreenCommand>,
    pub(super) snapshots: InventoryOwnerInbox<LocalInventorySnapshotRequest>,
    pub(super) lifecycle: InventoryOwnerInbox<LocalInventoryLifecycle>,
    pub(super) menus: InventoryOwnerInbox<super::cluster_inventory_protocol::LocalInventoryMenuIntent>,
    ledger: InvLedger,
    ground_ledger: InvLedger,
    prepared_player_inventories: HashMap<GlobalPlayerId, PreparedPlayerInventoryAdmission>,
    canonical_admission_roots: HashMap<
        pumpkin_cluster::canonical_action::LivePlayerLease,
        CanonicalPlayerInventoryAdmission,
    >,
    player_keys: HashMap<uuid::Uuid, GlobalPlayerId>,
    historical_player_keys: HashMap<GlobalPlayerId, uuid::Uuid>,
    historical_player_leases: HashMap<(GlobalPlayerId, u64), uuid::Uuid>,
    player_install_tokens: HashMap<GlobalPlayerId, u64>,
    player_incarnations: HashMap<GlobalPlayerId, u64>,
    latest_player_install_tokens: HashMap<GlobalPlayerId, u64>,
    sequences: HashMap<ActorId, InventorySequence>,
    origins: HashMap<ActionKey, Option<u64>>,
    remote_previews: HashMap<ActionKey, (InventoryScreenSession, GlobalPlayerId, u64)>,
    screen_sessions: InventoryScreenSessions,
    active_screens: HashMap<GlobalPlayerId, InventoryScreenSession>,
    pending_canonical_opens: HashMap<pumpkin_cluster::canonical_action::ActionRef, (super::cluster_invsee::CanonicalInventoryOpenPresentation, bool)>,
    pending_raw_menu_opens: std::collections::VecDeque<Arc<super::cluster_invsee::InventoryOpenSource>>,
    canonical_closes: HashMap<pumpkin_cluster::canonical_action::ActionRef,
        pumpkin_cluster::canonical_inventory_close::InventoryCloseIntent>,
    canonical_close_notifications: HashMap<pumpkin_cluster::canonical_action::ActionRef,
        super::cluster_inventory_close::CanonicalCloseNotification>,
    canonical_close_retries: HashMap<(GlobalPlayerId, u64),
        pumpkin_cluster::canonical_inventory_close::InventoryCloseIntent>,
    canonical_death_closes: HashMap<pumpkin_cluster::canonical_action::ActionRef, pumpkin_cluster::canonical_action::ActionRef>,
    canonical_close_status_order: HashMap<GlobalPlayerId, std::collections::VecDeque<u64>>,
    canonical_merchant_publications: HashMap<InventoryScreenSession, (u64, u32)>,
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

#[derive(Clone)]
struct PreparedPlayerInventoryAdmission {
    key: uuid::Uuid,
    generation: u64,
    player: pumpkin_cluster::inventory::InventoryStateSnapshot,
    crafting: pumpkin_cluster::inventory::InventoryStateSnapshot,
    ender: pumpkin_cluster::inventory::InventoryStateSnapshot,
    screen: pumpkin_cluster::inventory::InventoryScreenSnapshot,
    chunks: Vec<ChunkKey>,
}

#[derive(Clone, PartialEq, Eq)]
pub(super) struct CanonicalPlayerInventoryAdmission {
    pub(super) reference: pumpkin_cluster::canonical_action::ActionRef,
    pub(super) install_generation: u64,
    pub(super) owner: pumpkin_cluster::realm_completion::PeerGeneration,
    pub(super) chunk: ChunkKey,
    pub(super) initial: pumpkin_cluster::item_action::ActorLivingState,
    pub(super) inventory: pumpkin_cluster::avatar_lifecycle::PlayerInventoryAdmission,
}

#[derive(Clone, PartialEq, Eq)]
pub(super) struct CanonicalEntityInventoryAdmission {
    pub(super) reference: pumpkin_cluster::canonical_action::ActionRef,
    pub(super) chunk: ChunkKey,
    pub(super) body: pumpkin_cluster::entity_body::EntityBody,
    pub(super) inventory: pumpkin_cluster::entity_inventory_admission::EntityInventoryAdmission,
}

impl InventoryOwner {
    pub(super) fn publish_canonical_merchant_screens(
        &mut self,
        server: &Server,
        living: &super::cluster_world_actor::OwnedLivingActors,
        blocks: &super::cluster_chunk_state::OwnedChunkStore,
    ) {
        use pumpkin_cluster::canonical_action::{TruthView, Value};
        let before = server.cluster_inventory_screens.load_full();
        if !before.values().any(|screen| screen.window_kind == pumpkin_data::screen::WindowType::Merchant as u16) {
            self.canonical_merchant_publications.clear();
            return;
        }
        let mut next = before.as_ref().clone();
        for screen in next.values_mut().filter(|screen| screen.window_kind == pumpkin_data::screen::WindowType::Merchant as u16) {
            let Some(actor) = living.get(ActorId::Player(screen.session.actor)) else { continue; };
            if actor.local.menu.session != screen.session.session { continue; }
            let pumpkin_cluster::canonical_menu::MenuSource::Merchant { contact, kind } = &actor.local.menu.source else { continue; };
            let pumpkin_cluster::item_contact::ContactIdentity::Entity { id, birth } = &contact.identity else { continue; };
            let Some(chunk) = blocks.entity_body_chunk(false, *id) else { continue; };
            let Some(pumpkin_cluster::canonical_action::Cell { value: Value::Entity(body), .. }) = blocks.entity_body(false, chunk, *id)
                else { continue; };
            if body.birth != *birth || body.kind != *kind { continue; }
            let pumpkin_cluster::entity_body::EntityPayload::Living(merchant) = &body.payload else { continue; };
            let Some(offers) = merchant.merchant.as_ref().filter(|offers| offers.valid()) else { continue; };
            let mut targets = screen.slots.iter().map(|slot| slot.slot.target).collect::<BTreeSet<_>>();
            let Some(lease) = self.player_lease_for_gid(screen.session.actor) else { continue; };
            let player = InventoryTarget::LeasedPlayer { key: lease.key.0, player: lease.gid, incarnation: lease.incarnation };
            targets.insert(player);
            let snapshots = targets.into_iter().map(|target| self.phase_snapshot(TruthView::Local, target)
                .map(|snapshot| (target, snapshot))).collect::<Option<BTreeMap<_, _>>>();
            let Some(snapshots) = snapshots else { continue; };
            let prior_slots = screen.slots.clone();
            let prior_cursor = screen.cursor.clone();
            for slot in &mut screen.slots {
                let Some(snapshot) = snapshots.get(&slot.slot.target) else { continue; };
                slot.cell = snapshot.cells.iter().find(|cell| cell.loc == slot.slot.loc).cloned()
                    .unwrap_or_else(|| pumpkin_cluster::inventory::InventoryCell::empty(slot.slot.loc));
            }
            screen.cursor = snapshots[&player].cells.iter().find(|cell| cell.loc.inv == pumpkin_cluster::inventory::INV_CURSOR && cell.loc.slot == 0)
                .cloned().unwrap_or_else(|| pumpkin_cluster::inventory::InventoryCell::empty(
                    pumpkin_cluster::inventory::InvLoc::new(pumpkin_cluster::inventory::INV_CURSOR, 0)));
            let selected = match &actor.local.menu.selection {
                pumpkin_cluster::canonical_menu::MenuSelection::TradeOffer(offer) => *offer,
                _ => u32::MAX,
            };
            super::cluster_inventory_merchant::refresh_merchant_preview(screen, offers, selected);
            if screen.slots != prior_slots || screen.cursor != prior_cursor { screen.state_id = screen.state_id.wrapping_add(1); }
            let revision = (offers.version, selected);
            if self.canonical_merchant_publications.get(&screen.session) != Some(&revision)
                && let Some(player) = super::cluster_invsee::find_inventory_player(server, screen.session.actor)
                && !player.client.closed()
                && let Ok(presentation) = super::cluster_inventory_merchant::merchant_presentation(*id, offers)
                && super::cluster_inventory_protocol::send_trade_presentation(server, &player, screen.sync_id, &presentation).is_ok() {
                self.canonical_merchant_publications.insert(screen.session, revision);
            }
        }
        self.canonical_merchant_publications.retain(|session, _| next.contains_key(session));
        let after = Arc::new(next);
        server.cluster_inventory_screens.store(after.clone());
        let _ = super::cluster_inventory_protocol::publish_inventory_screen_deltas(server, &before, &after);
    }

    pub(super) fn can_accept_lifecycle(&self, lifecycle: &LocalInventoryLifecycle) -> bool {
        match lifecycle {
            LocalInventoryLifecycle::OpenScreen { .. } => self.pending_raw_menu_opens.len() < 512,
            LocalInventoryLifecycle::CanonicalOpenPresentation(open) => self.pending_canonical_opens.contains_key(&open.reference)
                || self.pending_canonical_opens.len() < 512,
            _ => true,
        }
    }

    pub(super) fn stage_pending_menu_opens(&mut self, server: &super::Server) {
        for _ in 0..self.pending_raw_menu_opens.len() {
            let open = self.pending_raw_menu_opens.pop_front().unwrap();
            let Some(player) = super::cluster_invsee::find_inventory_player(server, open.lease.gid) else { continue; };
            if super::cluster_world_owner::player_action_lease(server, &player) != Some(open.lease)
                || player.cluster_departing.load(std::sync::atomic::Ordering::Acquire) || player.client.closed() { continue; }
            if !player.cluster_inventory_pending_close.is_empty() || !player.cluster_inventory_departure_close.is_empty()
                || player.cluster_inventory_inputs.is_full() {
                self.pending_raw_menu_opens.push_back(open);
                continue;
            }
            if let Err(super::cluster_inventory_protocol::StagedInventoryInput::OpenScreen(open)) = player.cluster_inventory_inputs.push(super::cluster_inventory_protocol::StagedInventoryInput::OpenScreen(open)) {
                self.pending_raw_menu_opens.push_back(open);
            }
        }
    }

    fn refresh_canonical_open(&self, server: &Server, open: &mut super::cluster_invsee::CanonicalInventoryOpenPresentation,
        living: &super::cluster_world_actor::OwnedLivingActors,
    ) -> Result<(), InventoryApplyError> {
        for slot in &mut open.screen.slots {
            if slot.limit == 0 { continue; }
            let snapshot = self.phase_snapshot(pumpkin_cluster::canonical_action::TruthView::Local, slot.slot.target)
                .or_else(|| open.screen.preview.as_ref().filter(|preview| preview.target == slot.slot.target).cloned())
                .ok_or(InventoryApplyError::TargetUnavailable)?;
            slot.cell = snapshot.cells.iter().find(|cell| cell.loc == slot.slot.loc).cloned()
                .ok_or(InventoryApplyError::TargetUnavailable)?;
        }
        let player = self.phase_snapshot(pumpkin_cluster::canonical_action::TruthView::Local,
            InventoryTarget::LeasedPlayer { key: open.lease.key.0, player: open.lease.gid, incarnation: open.lease.incarnation })
            .ok_or(InventoryApplyError::TargetUnavailable)?;
        open.screen.cursor = player.cells.iter().find(|cell| cell.loc == pumpkin_cluster::inventory::InvLoc::new(pumpkin_cluster::inventory::INV_CURSOR, 0))
            .cloned().ok_or(InventoryApplyError::TargetUnavailable)?;
        super::cluster_inventory_menu::refresh_crafting_preview(server, &mut open.screen);
        if let Some(merchant) = &open.merchant {
            let offers = living.get(ActorId::Entity(merchant.merchant)).and_then(|truth| truth.local.merchant.as_ref())
                .ok_or(InventoryApplyError::TargetUnavailable)?;
            let selection = living.get(ActorId::Player(open.lease.gid)).map(|truth| &truth.local.menu.selection);
            let selected = match selection { Some(pumpkin_cluster::canonical_menu::MenuSelection::TradeOffer(offer)) => *offer, _ => u32::MAX };
            super::cluster_inventory_merchant::refresh_merchant_preview(&mut open.screen, offers, selected);
        }
        if let Some(presentation) = &mut open.presentation {
            for slot in &open.screen.slots {
                let stack = presentation.stacks.get_mut(usize::from(slot.screen_slot)).ok_or(InventoryApplyError::Rejected)?;
                *stack = super::cluster_invsee::item_stack_from_inventory_cell(&slot.cell)?;
            }
        }
        Ok(())
    }

    pub(super) fn publish_canonical_screens(
        &mut self,
        server: &Server,
        graphs: &pumpkin_cluster::action_graph::ActionGraphs,
        living: &super::cluster_world_actor::OwnedLivingActors,
    ) {
        use pumpkin_cluster::action_graph::SemanticWrapper;
        for reference in self.pending_canonical_opens.keys().copied().collect::<Vec<_>>() {
            let Some((mut open, mut published)) = self.pending_canonical_opens.remove(&reference) else { continue; };
            if !self.live_player_lease_matches(open.lease) { continue; }
            let Some(node) = graphs.ticks.get(&reference.tick).and_then(|graph| graph.actions.get(&reference.id)) else {
                self.pending_canonical_opens.insert(reference, (open, published));
                continue;
            };
            let matches_menu = living.get(ActorId::Player(open.lease.gid)).is_some_and(|truth| {
                truth.local.menu.session == open.screen.session.session
                    && truth.local.menu.window_kind == open.screen.window_kind
                    && truth.local.menu.source == open.source
            });
            let caused_menu = node.semantic_action().effects.iter().any(|effect| matches!(effect,
                pumpkin_cluster::canonical_action::Effect::Put { target: pumpkin_cluster::canonical_action::Target::Chunk {
                    part: pumpkin_cluster::canonical_action::ChunkPart::LivingProperty { actor: ActorId::Player(gid),
                        property: pumpkin_cluster::living_property::LivingProperty::MenuSession }, .. },
                    value: pumpkin_cluster::canonical_action::Value::LivingProperty(pumpkin_cluster::living_property::LivingPropertyValue::MenuSession(menu)) }
                    if *gid == open.lease.gid && menu.session == open.screen.session.session
                        && menu.window_kind == open.screen.window_kind && menu.source == open.source));
            let applied = node.wrapper != SemanticWrapper::Undone && node.local_result.is_some()
                && node.local_generation == Some(node.generation) && matches_menu && caused_menu;
            let Some(player) = super::cluster_invsee::find_inventory_player(server, open.lease.gid) else {
                self.pending_canonical_opens.insert(reference, (open, published));
                continue;
            };
            if !applied && published {
                if super::cluster_inventory_protocol::publish_cluster_screen_close(&player, open.screen.sync_id, open.screen.window_kind).is_err() {
                    self.pending_canonical_opens.insert(reference, (open, published));
                    continue;
                }
                self.lifecycle_regular(server, LocalInventoryLifecycle::CloseScreen { session: open.screen.session });
                let _ = player.cluster_current_inventory_session.compare_exchange(open.screen.session.session, 0,
                    std::sync::atomic::Ordering::AcqRel, std::sync::atomic::Ordering::Acquire);
                published = false;
            }
            if applied && !published && !player.cluster_departing.load(std::sync::atomic::Ordering::Acquire)
                && !player.client.closed()
            {
                let merchant = if let Some(original) = &open.merchant {
                    let Some(offers) = living.get(ActorId::Entity(original.merchant)).and_then(|truth| truth.local.merchant.as_ref()) else {
                        self.pending_canonical_opens.insert(reference, (open, published));
                        continue;
                    };
                    let Ok(presentation) = super::cluster_inventory_merchant::merchant_presentation(original.merchant, offers) else {
                        self.pending_canonical_opens.insert(reference, (open, published));
                        continue;
                    };
                    Some(presentation)
                } else { None };
                if self.refresh_canonical_open(server, &mut open, living).is_err() {
                    self.pending_canonical_opens.insert(reference, (open, published));
                    continue;
                }
                player.cluster_current_inventory_session.store(open.screen.session.session, std::sync::atomic::Ordering::Release);
                if open.presentation.as_ref().is_some_and(|presentation|
                    super::cluster_inventory_protocol::publish_cluster_screen_open(server, &open.screen, presentation).is_err()) {
                    self.pending_canonical_opens.insert(reference, (open, published));
                    continue;
                }
                if let Some(menu) = &open.menu { self.menu_metadata.insert(open.screen.session, menu.clone()); }
                if merchant.as_ref().is_some_and(|merchant| super::cluster_inventory_protocol::send_trade_presentation(
                    server, &player, open.screen.sync_id, merchant).is_err()) {
                    self.pending_canonical_opens.insert(reference, (open, published));
                    continue;
                }
                super::cluster_invsee::publish_inventory_screen(server, open.screen.clone());
                self.queue_invsee_screen(&open.screen, true);
                self.active_screens.insert(open.lease.gid, open.screen.session);
                if open.screen.slots.iter().any(|slot| matches!(slot.slot.target,
                    InventoryTarget::Virtual { kind: 0x100, .. } | InventoryTarget::LeasedVirtual { kind: 0x100, .. })) {
                    self.ender_open_sessions.insert(open.screen.session);
                }
                published = true;
            }
            if !node.is_ground_applied() || applied && !published {
                self.pending_canonical_opens.insert(reference, (open, published));
            }
        }
    }

    pub(super) fn retained_menu_births(&self) -> BTreeSet<pumpkin_cluster::canonical_action::ActionRef> {
        self.canonical_menu_admissions.values().map(|root| root.reference).collect()
    }

    pub(super) fn canonical_menu_admission_slot(&self, target: pumpkin_cluster::canonical_action::Target)
        -> Option<&CanonicalMenuInventoryAdmission> {
        use pumpkin_cluster::canonical_action::{Target, InventoryId};
        let Target::Player { key, lease: Some(lease), inventory: Some(InventoryId::Virtual { host, kind, session, .. }) } = target
            else { return None; };
        if key != lease.key { return None; }
        self.canonical_menu_admissions.get(&InventoryTarget::LeasedVirtual {
            host, key: key.0, player: lease.gid, incarnation: lease.incarnation, session, kind,
        }).filter(|root| root.admission.entries().iter().any(|(known, _)| *known == target))
    }

    pub(super) fn original_inventory_root_targets(&self) -> BTreeSet<InventoryTarget> {
        self.ledger.targets().into_iter().chain(self.canonical_menu_admissions.keys().copied()).collect()
    }

    pub(super) fn menu_inventory_unallocated(&self, admission: &pumpkin_cluster::canonical_menu::MenuVirtualInventoryAdmission) -> bool {
        self.live_player_lease_matches(admission.lease) && admission.session != 0
            && !self.canonical_menu_admissions.contains_key(&admission.inventory_target())
            && self.ledger.snapshot(admission.inventory_target()).is_none()
            && self.ground_ledger.snapshot(admission.inventory_target()).is_none()
    }

    pub(super) fn record_canonical_entity_page(
        &mut self,
        actions: &[pumpkin_cluster::canonical_action::Action],
        source: pumpkin_cluster::realm_completion::PeerGeneration,
    ) -> bool {
        use pumpkin_cluster::canonical_action::*;
        let mut menu_roots = HashMap::new();
        let mut roots = HashMap::new();
        for action in actions {
            for effect in &action.effects {
                let Effect::MenuVirtualInventoryAdmission { admission } = effect else { continue; };
                if action.validate().is_err() || action.id.actor != ActorId::Player(admission.lease.gid)
                    || source.server != admission.lease.gid.server { return false; }
                let root = CanonicalMenuInventoryAdmission { reference: ActionRef { id: action.id, tick: action.tick }, admission: admission.clone() };
                let target = admission.inventory_target();
                if menu_roots.get(&target).is_some_and(|prior| prior != &root)
                    || self.canonical_menu_admissions.get(&target).is_some_and(|prior| prior != &root) { return false; }
                menu_roots.insert(target, root);
            }
            for effect in &action.effects {
                let Effect::EntityAdmission { target: Target::Chunk { key, .. }, body, inventory } = effect else { continue; };
                if action.validate().is_err() || body.birth.source != source
                    || body.birth.action != (ActionRef { tick: action.tick, id: action.id }) { return false; }
                let root = CanonicalEntityInventoryAdmission { reference: body.birth.action, chunk: *key, body: body.clone(), inventory: inventory.clone() };
                if roots.get(&body.id).is_some_and(|prior| prior != &root)
                    || self.canonical_entity_admissions.get(&body.id).is_some_and(|prior| prior != &root) { return false; }
                roots.insert(body.id, root);
            }
        }
        let fresh = roots.keys().filter(|entity| !self.canonical_entity_admissions.contains_key(entity)).count();
        if self.canonical_entity_admissions.len().saturating_add(fresh) > 4096 { return false; }
        if self.canonical_menu_admissions.len().saturating_add(menu_roots.keys().filter(|target| !self.canonical_menu_admissions.contains_key(target)).count()) > pumpkin_cluster::canonical_action::MAX_LOGICAL_RECORDS { return false; }
        self.canonical_menu_admissions.extend(menu_roots);
        self.canonical_entity_admissions.extend(roots);
        true
    }

    pub(super) fn record_canonical_admission(
        &mut self,
        action: &pumpkin_cluster::canonical_action::Action,
        source: pumpkin_cluster::realm_completion::PeerGeneration,
    ) -> bool {
        use pumpkin_cluster::canonical_action::*;
        if action.effects.iter().any(|effect| matches!(effect, Effect::MenuVirtualInventoryAdmission { .. })) {
            return self.record_canonical_entity_page(std::slice::from_ref(action), source);
        }
        let entity_roots = action.effects.iter().filter_map(|effect| match effect {
            Effect::EntityAdmission { target: Target::Chunk { key, .. }, body, inventory } => Some((*key, body, inventory)),
            _ => None,
        }).collect::<Vec<_>>();
        if !entity_roots.is_empty() {
            if action.validate().is_err() {
                return false;
            }
            let reference = ActionRef { tick: action.tick, id: action.id };
            let mut roots = HashMap::new();
            for (chunk, body, inventory) in entity_roots {
                if body.birth.source != source || body.birth.action != reference { return false; }
                let root = CanonicalEntityInventoryAdmission { reference, chunk, body: body.clone(), inventory: inventory.clone() };
                if roots.insert(body.id, root.clone()).is_some()
                    || self.canonical_entity_admissions.get(&body.id).is_some_and(|existing| existing != &root)
                { return false; }
            }
            let fresh = roots.keys().filter(|entity| !self.canonical_entity_admissions.contains_key(entity)).count();
            if self.canonical_entity_admissions.len().saturating_add(fresh) > 4096 { return false; }
            for (entity, root) in roots {
                self.canonical_entity_admissions.entry(entity).or_insert(root);
            }
            return true;
        }
        let [
            Effect::PlayerAvatarLifecycle {
                target: Target::Chunk { key, .. },
                intent:
                    pumpkin_cluster::avatar_lifecycle::AvatarLifecycleIntent::Admission {
                        lease,
                        owner,
                        inventory,
                        install_generation,
                        initial,
                    },
            },
        ] = action.effects.as_slice()
        else {
            return false;
        };
        if *owner != source || action.validate().is_err() {
            return false;
        }
        let root = CanonicalPlayerInventoryAdmission {
            reference: ActionRef {
                tick: action.tick,
                id: action.id,
            },
            install_generation: *install_generation,
            owner: *owner,
            chunk: *key,
            initial: initial.clone(),
            inventory: inventory.clone(),
        };
        if let Some(existing) = self.canonical_admission_roots.get(lease) {
            return existing == &root;
        }
        if self.canonical_admission_roots.len() >= 4096 {
            return false;
        }
        self.canonical_admission_roots.insert(*lease, root);
        true
    }

    pub(super) fn canonical_admission_root(
        &self,
        lease: pumpkin_cluster::canonical_action::LivePlayerLease,
    ) -> Option<&CanonicalPlayerInventoryAdmission> {
        self.canonical_admission_roots.get(&lease)
    }

    pub(super) fn canonical_admission_body_known(
        &self,
        gid: GlobalPlayerId,
        chunk: ChunkKey,
    ) -> bool {
        self.canonical_admission_roots.iter().any(|(lease, root)| {
            lease.gid == gid
                && root.chunk == chunk
                && self
                    .player_incarnations
                    .get(&gid)
                    .is_none_or(|incarnation| *incarnation == lease.incarnation)
        })
    }

    pub(super) fn admission_slot_known(
        &self,
        target: pumpkin_cluster::canonical_action::Target,
    ) -> bool {
        use pumpkin_cluster::canonical_action::*;
        if self.canonical_menu_admission_slot(target).is_some() { return true; }
        if let Target::Chunk {
            key,
            part: ChunkPart::Entity(entity),
            inventory: Some(InventoryId::Entity { .. }),
        } = target
        {
            return self
                .canonical_entity_admissions
                .get(&entity)
                .is_some_and(|root| {
                    root.chunk == key
                        && root
                            .inventory
                            .entries(key, entity)
                            .any(|(known, _)| known == target)
                });
        }
        let Target::Player {
            key,
            lease: Some(lease),
            inventory: Some(_),
        } = target
        else {
            return false;
        };
        key == lease.key
            && self
                .canonical_admission_roots
                .get(&lease)
                .is_some_and(|root| root.inventory.targets(lease).any(|known| known == target))
    }

    pub(super) fn canonical_entity_admission_root(
        &self,
        entity: pumpkin_cluster::protocol::GlobalEntityId,
    ) -> Option<&CanonicalEntityInventoryAdmission> {
        self.canonical_entity_admissions.get(&entity)
    }

    pub(super) fn activate_player_admission(
        &mut self,
        lease: pumpkin_cluster::canonical_action::LivePlayerLease,
        server: &Server,
        local: bool,
    ) {
        let root = self
            .canonical_admission_roots
            .get(&lease)
            .expect("committed inventory admission root missing");
        self.historical_player_keys.insert(lease.gid, lease.key.0);
        self.historical_player_leases
            .insert((lease.gid, lease.incarnation), lease.key.0);
        self.player_incarnations
            .insert(lease.gid, lease.incarnation);
        self.player_install_tokens
            .insert(lease.gid, root.install_generation);
        if local
            && let Some(prepared) = self.prepared_player_inventories.get(&lease.gid)
            && prepared.key == lease.key.0
            && prepared.generation == root.install_generation
        {
            let mut screen = prepared.screen.clone();
            for slot in &mut screen.slots {
                slot.slot.target = match slot.slot.target {
                    InventoryTarget::Player(gid) if gid == lease.gid => {
                        InventoryTarget::LeasedPlayer {
                            key: lease.key.0,
                            player: gid,
                            incarnation: lease.incarnation,
                        }
                    }
                    InventoryTarget::Virtual {
                        host,
                        owner: ActorId::Player(gid),
                        session,
                        kind,
                    } if gid == lease.gid => InventoryTarget::LeasedVirtual {
                        host,
                        key: lease.key.0,
                        player: gid,
                        incarnation: lease.incarnation,
                        session,
                        kind,
                    },
                    target => target,
                };
            }
            super::cluster_invsee::publish_inventory_screen(server, screen);
        }
    }
    pub(super) fn discard_prepared_candidate(
        &mut self,
        gid: GlobalPlayerId,
        key: uuid::Uuid,
        generation: u64,
    ) -> bool {
        if !self.prepared_player_inventories.get(&gid).is_some_and(|prepared| {
            prepared.key == key && prepared.generation == generation
        }) {
            return false;
        }
        self.prepared_player_inventories.remove(&gid);
        if !self.player_incarnations.contains_key(&gid)
            && self.player_install_tokens.get(&gid) == Some(&generation)
        {
            self.player_install_tokens.remove(&gid);
            if self.player_keys.get(&key) == Some(&gid) {
                self.player_keys.remove(&key);
            }
        }
        true
    }

    pub(super) fn prepared_player_admission(
        &self,
        lease: pumpkin_cluster::canonical_action::LivePlayerLease,
    ) -> Option<pumpkin_cluster::avatar_lifecycle::PlayerInventoryAdmission> {
        let prepared = self.prepared_player_inventories.get(&lease.gid)?;
        if prepared.key != lease.key.0
            || self.player_incarnations.get(&lease.gid) != Some(&lease.incarnation)
            || self.player_install_tokens.get(&lease.gid) != Some(&prepared.generation)
        {
            return None;
        }
        let admission = pumpkin_cluster::avatar_lifecycle::PlayerInventoryAdmission {
            imported_revision: prepared.player.revision,
            cells: pumpkin_cluster::avatar_lifecycle::PlayerInventoryAdmission::slots()
                .map(|loc| {
                    prepared
                        .player
                        .cells
                        .iter()
                        .find(|item| item.loc == loc)
                        .cloned()
                        .unwrap_or_else(|| pumpkin_cluster::inventory::InventoryCell::empty(loc))
                })
                .map(|item| pumpkin_cluster::canonical_action::Cell {
                    revision: 0,
                    value: pumpkin_cluster::canonical_action::Value::Inventory(item),
                })
                .collect(),
            supplemental: [(&prepared.crafting, 5u16), (&prepared.ender, 27u16)]
                .into_iter()
                .map(|(snapshot, size)| {
                    let InventoryTarget::Virtual {
                        kind,
                        host,
                        owner: ActorId::Player(actor),
                        session: 0,
                    } = snapshot.target
                    else {
                        return None;
                    };
                    if host != lease.gid.server || actor != lease.gid {
                        return None;
                    }
                    Some(
                        pumpkin_cluster::avatar_lifecycle::PlayerVirtualInventoryAdmission {
                            kind,
                            imported_revision: snapshot.revision,
                            cells: (0..size)
                                .map(|slot| pumpkin_cluster::inventory::InvLoc::new(INV_MAIN, slot))
                                .map(|loc| {
                                    snapshot
                                        .cells
                                        .iter()
                                        .find(|item| item.loc == loc)
                                        .cloned()
                                        .unwrap_or_else(|| {
                                            pumpkin_cluster::inventory::InventoryCell::empty(loc)
                                        })
                                })
                                .map(|item| pumpkin_cluster::canonical_action::Cell {
                                    revision: 0,
                                    value: pumpkin_cluster::canonical_action::Value::Inventory(
                                        item,
                                    ),
                                })
                                .collect(),
                        },
                    )
                })
                .collect::<Option<Vec<_>>>()?,
        };
        admission.valid().then_some(admission)
    }
    pub(super) fn local_target_chunks(&self, target: InventoryTarget) -> Option<Vec<ChunkKey>> {
        let target = self.current_inventory_address(target)?;
        self.ledger.target_chunks(target).map(<[ChunkKey]>::to_vec)
    }

    pub(super) fn current_inventory_address(
        &self,
        target: InventoryTarget,
    ) -> Option<InventoryTarget> {
        let target = match target {
            InventoryTarget::Player(gid) => {
                let lease = self.player_lease_for_gid(gid)?;
                let leased = InventoryTarget::LeasedPlayer {
                    key: lease.key.0,
                    player: gid,
                    incarnation: lease.incarnation,
                };
                if self.ledger.snapshot(leased).is_some()
                    || self.ground_ledger.snapshot(leased).is_some()
                {
                    leased
                } else {
                    target
                }
            }
            InventoryTarget::Virtual {
                host,
                owner: ActorId::Player(gid),
                session,
                kind,
            } => {
                let lease = self.player_lease_for_gid(gid)?;
                let leased = InventoryTarget::LeasedVirtual {
                    host,
                    key: lease.key.0,
                    player: gid,
                    incarnation: lease.incarnation,
                    session,
                    kind,
                };
                if self.ledger.snapshot(leased).is_some()
                    || self.ground_ledger.snapshot(leased).is_some()
                {
                    leased
                } else {
                    target
                }
            }
            _ => target,
        };
        Some(target)
    }

    pub(super) fn issue_input_sequence(
        &mut self,
        actor: ActorId,
        input: Option<pumpkin_cluster::identity::PlayerSeq>,
    ) -> pumpkin_cluster::identity::PlayerSeq {
        self.sequences
            .entry(actor)
            .or_default()
            .issue_or_observe(input)
    }

    pub(super) fn accepted_player_inventory_at(
        &self,
        prepared: Option<&PreparedPlayerInventoryRelocation>,
        gid: GlobalPlayerId,
        chunk: ChunkKey,
    ) -> bool {
        let Some(lease) = self.player_lease_for_gid(gid) else {
            return false;
        };
        let Some(target) = self.player_inventory_address(
            Some(pumpkin_cluster::canonical_action::TruthView::Ground),
            lease,
        ) else {
            return false;
        };
        prepared
            .map_or(&self.ground_ledger, |state| &state.ground)
            .target_chunks(target)
            .is_some_and(|chunks| chunks.contains(&chunk))
    }

    pub(super) fn prepare_accepted_player_transition(
        &self,
        previous: Option<PreparedPlayerInventoryRelocation>,
        gid: GlobalPlayerId,
        incarnation: u64,
        from: ChunkKey,
        to: ChunkKey,
        local_actor_chunk: Option<ChunkKey>,
    ) -> Result<Option<PreparedPlayerInventoryRelocation>, InventoryApplyError> {
        let Some(lease) = self.player_lease_for_gid(gid) else {
            return Ok(None);
        };
        if lease.incarnation != incarnation {
            return Err(InventoryApplyError::Rejected);
        }
        let mut prepared = previous.unwrap_or_else(|| PreparedPlayerInventoryRelocation {
            local: self.ledger.clone(),
            ground: self.ground_ledger.clone(),
            targets: Vec::new(),
        });
        let primary = self
            .player_inventory_address(
                Some(pumpkin_cluster::canonical_action::TruthView::Ground),
                lease,
            )
            .ok_or(InventoryApplyError::Rejected)?;
        if !prepared
            .ground
            .target_chunks(primary)
            .is_some_and(|chunks| chunks.contains(&from))
        {
            return Err(InventoryApplyError::Rejected);
        }
        let targets: Vec<_> = self
            .ground_ledger
            .targets_for_chunk(from)
            .into_iter()
            .filter(|target| match target {
                InventoryTarget::Player(player) => *player == gid && *target == primary,
                InventoryTarget::LeasedPlayer {
                    key,
                    player,
                    incarnation,
                } => *key == lease.key.0 && *player == gid && *incarnation == lease.incarnation,
                InventoryTarget::Virtual {
                    owner: ActorId::Player(player),
                    ..
                } => *player == gid,
                InventoryTarget::LeasedVirtual {
                    key,
                    player,
                    incarnation,
                    ..
                } => *key == lease.key.0 && *player == gid && *incarnation == lease.incarnation,
                _ => false,
            })
            .collect();
        for target in &targets {
            if !prepared
                .ground
                .target_chunks(*target)
                .is_some_and(|chunks| chunks.contains(&from))
            {
                return Err(InventoryApplyError::Rejected);
            }
            let local_chunks = prepared
                .local
                .target_chunks(*target)
                .ok_or(InventoryApplyError::Rejected)?;
            let local_destination = if local_chunks.contains(&from) {
                Some(local_actor_chunk.unwrap_or(to))
            } else {
                None
            };
            if let Some(destination) = local_destination {
                let chunks =
                    relocated_inventory_chunks(&prepared.local, *target, from, destination)?;
                prepared
                    .local
                    .relocate_target(*target, chunks)
                    .map_err(|_| InventoryApplyError::Rejected)?;
            }
            let chunks = relocated_inventory_chunks(&prepared.ground, *target, from, to)?;
            prepared
                .ground
                .relocate_target(*target, chunks)
                .map_err(|_| InventoryApplyError::Rejected)?;
            prepared
                .targets
                .push((*target, from, to, local_destination));
        }
        Ok(Some(prepared))
    }

    pub(super) fn commit_accepted_player_transition(
        &mut self,
        server: &super::Server,
        prepared: PreparedPlayerInventoryRelocation,
    ) {
        for (target, from, to, local_destination) in &prepared.targets {
            if let Some(destination) = local_destination {
                let chunks = relocated_inventory_chunks(&self.ledger, *target, *from, *destination)
                    .expect(
                        "preflighted local player inventory changed before accepted transition",
                    );
                self.ledger.relocate_target(*target, chunks).expect(
                    "preflighted local player inventory retired during accepted transition",
                );
            }
            let chunks = relocated_inventory_chunks(&self.ground_ledger, *target, *from, *to)
                .expect("preflighted ground player inventory changed before accepted transition");
            self.ground_ledger
                .relocate_target(*target, chunks)
                .expect("preflighted ground player inventory retired during accepted transition");
        }
        let published = prepared
            .targets
            .iter()
            .filter_map(|(target, from, _, _)| {
                self.ledger
                    .target_chunks(*target)
                    .and_then(|chunks| chunks.first().copied())
                    .map(|chunk| (*target, *from, chunk))
            })
            .collect::<Vec<_>>();
        self.publish_canonical_relocations(server, &published);
        for (target, _, _, _) in prepared.targets {
            if let InventoryTarget::Player(gid)
            | InventoryTarget::LeasedPlayer { player: gid, .. } = target
            {
                self.pending_invsee_relocations.insert(gid);
            }
        }
    }

    pub(super) fn phase_candidate(
        &self,
        view: pumpkin_cluster::canonical_action::TruthView,
    ) -> InvLedger {
        match view {
            pumpkin_cluster::canonical_action::TruthView::Local => self.ledger.clone(),
            pumpkin_cluster::canonical_action::TruthView::Ground => self.ground_ledger.clone(),
        }
    }

    pub(super) fn preview_phase_mutation(
        &self,
        view: pumpkin_cluster::canonical_action::TruthView,
        mutation: pumpkin_cluster::inventory::InventoryMutation,
    ) -> Result<InvLedger, InventoryApplyError> {
        let mut candidate = self.phase_candidate(view);
        candidate.apply_mutation(mutation)?;
        Ok(candidate)
    }

    pub(super) fn install_phase_mutation(
        &mut self,
        view: pumpkin_cluster::canonical_action::TruthView,
        candidate: InvLedger,
    ) {
        match view {
            pumpkin_cluster::canonical_action::TruthView::Local => self.ledger = candidate,
            pumpkin_cluster::canonical_action::TruthView::Ground => self.ground_ledger = candidate,
        }
    }

    pub(super) fn phase_snapshot(
        &self,
        view: pumpkin_cluster::canonical_action::TruthView,
        target: InventoryTarget,
    ) -> Option<pumpkin_cluster::inventory::InventoryStateSnapshot> {
        let target = self.current_inventory_address(target)?;
        self.exact_phase_snapshot(view, target)
    }

    pub(super) fn phase_revision(
        &self,
        view: pumpkin_cluster::canonical_action::TruthView,
        target: InventoryTarget,
    ) -> Option<(u64, u64)> {
        let target = self.current_inventory_address(target)?;
        let revision = match view {
            pumpkin_cluster::canonical_action::TruthView::Local => self.ledger.revision(target),
            pumpkin_cluster::canonical_action::TruthView::Ground => {
                self.ground_ledger.revision(target)
            }
        }?;
        Some((self.replacement_epoch, revision))
    }

    fn exact_phase_snapshot(
        &self,
        view: pumpkin_cluster::canonical_action::TruthView,
        target: InventoryTarget,
    ) -> Option<pumpkin_cluster::inventory::InventoryStateSnapshot> {
        let snapshot = match view {
            pumpkin_cluster::canonical_action::TruthView::Local => self.ledger.snapshot(target),
            pumpkin_cluster::canonical_action::TruthView::Ground => {
                self.ground_ledger.snapshot(target)
            }
        }?;
        self.complete_owned_snapshot(snapshot)
    }

    fn complete_owned_snapshot(
        &self,
        mut snapshot: pumpkin_cluster::inventory::InventoryStateSnapshot,
    ) -> Option<pumpkin_cluster::inventory::InventoryStateSnapshot> {
        use pumpkin_cluster::inventory::*;
        let expected = match snapshot.target {
            InventoryTarget::Player(_) | InventoryTarget::LeasedPlayer { .. } => {
                pumpkin_cluster::avatar_lifecycle::PlayerInventoryAdmission::slots().collect::<Vec<_>>()
            }
            InventoryTarget::LeasedVirtual { kind, session: 0, .. } => {
                let size = match kind {
                    pumpkin_cluster::avatar_lifecycle::PLAYER_CRAFTING_INVENTORY_KIND => 5,
                    pumpkin_cluster::avatar_lifecycle::PLAYER_ENDER_INVENTORY_KIND => 27,
                    _ => return None,
                };
                (0..size).map(|slot| InvLoc::new(INV_MAIN, slot)).collect()
            }
            InventoryTarget::Block { .. } => {
                let root = self.canonical_block_roots.get(&snapshot.target)?;
                if root.deferred_loot { return None; }
                (0..root.slots).map(|slot| InvLoc::new(INV_MAIN, slot)).collect()
            }
            InventoryTarget::LeasedVirtual { session, .. } if session != 0 => {
                let root = self.canonical_menu_admissions.get(&snapshot.target)?;
                (0..root.admission.layout.slots()).map(|slot| InvLoc::new(INV_MAIN, slot)).collect()
            }
            InventoryTarget::Entity(id) => {
                let id = pumpkin_cluster::protocol::GlobalEntityId { origin: id.origin, local_id: id.local_id, generation: id.generation };
                let root = self.canonical_entity_admissions.get(&id)?;
                root.inventory.cells.iter().map(|cell| match &cell.value {
                    pumpkin_cluster::canonical_action::Value::Inventory(item) => Some(item.loc), _ => None,
                }).collect::<Option<Vec<_>>>()?
            }
            _ => return Some(snapshot),
        };
        let mut cells = snapshot
            .cells
            .into_iter()
            .map(|cell| (cell.loc, cell))
            .collect::<std::collections::BTreeMap<_, _>>();
        if cells.keys().any(|slot| !expected.contains(slot)) {
            return None;
        }
        if expected
            .iter()
            .any(|slot| slot.inv == INV_SELECTED && !cells.contains_key(slot))
        {
            return None;
        }
        snapshot.cells = expected
            .into_iter()
            .map(|slot| {
                cells
                    .remove(&slot)
                    .unwrap_or_else(|| InventoryCell::empty(slot))
            })
            .collect();
        Some(snapshot)
    }

    pub(super) fn actor_phase_inventory_snapshots(
        &self,
    ) -> Vec<pumpkin_cluster::inventory::InventoryStateSnapshot> {
        self.ledger
            .targets()
            .into_iter()
            .filter(|target| match target {
                InventoryTarget::LeasedPlayer {
                    key,
                    player,
                    incarnation,
                } => self
                    .player_lease_for_gid(*player)
                    .is_some_and(|lease| lease.key.0 == *key && lease.incarnation == *incarnation),
                InventoryTarget::Player(gid) => self.player_lease_for_gid(*gid).is_some(),
                InventoryTarget::Entity(_) => true,
                InventoryTarget::Block { .. } => self.canonical_block_roots.contains_key(target),
                InventoryTarget::LeasedVirtual { key, player, incarnation, .. } => self.player_lease_for_gid(*player)
                    .is_some_and(|lease| lease.key.0 == *key && lease.incarnation == *incarnation),
                InventoryTarget::Virtual { owner: ActorId::Player(player), .. } => self.player_lease_for_gid(*player).is_some(),
                _ => false,
            })
            .filter_map(|target| {
                self.exact_phase_snapshot(
                    pumpkin_cluster::canonical_action::TruthView::Local,
                    target,
                )
            })
            .collect()
    }

    pub(super) fn player_install_generation(&self, gid: GlobalPlayerId) -> Option<u64> {
        self.player_install_tokens.get(&gid).copied()
    }

    pub(super) fn player_lease_for_gid(
        &self,
        gid: GlobalPlayerId,
    ) -> Option<pumpkin_cluster::canonical_action::LivePlayerLease> {
        Some(pumpkin_cluster::canonical_action::LivePlayerLease {
            key: pumpkin_cluster::canonical_action::PlayerKey(
                *self.historical_player_keys.get(&gid)?,
            ),
            gid,
            incarnation: *self.player_incarnations.get(&gid)?,
        })
    }

    pub(super) fn ground_player_bindings(
        &self,
    ) -> Vec<(pumpkin_cluster::canonical_action::LivePlayerLease, ChunkKey)> {
        self.historical_player_keys
            .iter()
            .filter_map(|(gid, key)| {
                let incarnation = *self.player_incarnations.get(gid)?;
                let chunks = self
                    .ground_ledger
                    .target_chunks(InventoryTarget::Player(*gid))?;
                (chunks.len() == 1).then_some((
                    pumpkin_cluster::canonical_action::LivePlayerLease {
                        key: pumpkin_cluster::canonical_action::PlayerKey(*key),
                        gid: *gid,
                        incarnation,
                    },
                    chunks[0],
                ))
            })
            .collect()
    }

    pub(super) fn bind_player_incarnation(&mut self, gid: GlobalPlayerId, incarnation: u64) {
        self.player_incarnations.insert(gid, incarnation);
        if let Some(key) = self.historical_player_keys.get(&gid) {
            self.historical_player_leases
                .insert((gid, incarnation), *key);
        }
    }

    pub(super) fn live_player_lease_matches(
        &self,
        lease: pumpkin_cluster::canonical_action::LivePlayerLease,
    ) -> bool {
        self.historical_player_keys.get(&lease.gid) == Some(&lease.key.0)
            && self.player_incarnations.get(&lease.gid) == Some(&lease.incarnation)
            && (self
                .ledger
                .snapshot(InventoryTarget::Player(lease.gid))
                .is_some()
                || self
                    .ledger
                    .snapshot(InventoryTarget::LeasedPlayer {
                        key: lease.key.0,
                        player: lease.gid,
                        incarnation: lease.incarnation,
                    })
                    .is_some())
    }

    pub(super) fn historical_player_lease_matches(
        &self,
        lease: pumpkin_cluster::canonical_action::LivePlayerLease,
    ) -> bool {
        self.historical_player_leases
            .get(&(lease.gid, lease.incarnation))
            == Some(&lease.key.0)
    }

    pub(super) fn current_player_target_matches(&self, target: InventoryTarget) -> bool {
        match target {
            InventoryTarget::Player(gid) => {
                self.player_install_tokens.contains_key(&gid)
                    && self.player_lease_for_gid(gid).is_some_and(|lease| {
                        self.ledger
                            .snapshot(InventoryTarget::LeasedPlayer {
                                key: lease.key.0,
                                player: gid,
                                incarnation: lease.incarnation,
                            })
                            .is_none()
                    })
            }
            InventoryTarget::LeasedPlayer {
                key,
                player,
                incarnation,
            }
            | InventoryTarget::LeasedVirtual {
                key,
                player,
                incarnation,
                ..
            } => {
                self.historical_player_keys.get(&player) == Some(&key)
                    && self.player_incarnations.get(&player) == Some(&incarnation)
                    && self.ledger.snapshot(target).is_some()
                    && match target {
                        InventoryTarget::LeasedPlayer { .. } => self
                            .ledger
                            .snapshot(InventoryTarget::Player(player))
                            .is_none(),
                        InventoryTarget::LeasedVirtual {
                            host,
                            session,
                            kind,
                            ..
                        } => self
                            .ledger
                            .snapshot(InventoryTarget::Virtual {
                                host,
                                owner: ActorId::Player(player),
                                session,
                                kind,
                            })
                            .is_none(),
                        _ => false,
                    }
            }
            InventoryTarget::Virtual {
                owner: ActorId::Player(gid),
                host,
                session,
                kind,
            } => {
                self.player_install_tokens.contains_key(&gid)
                    && self.player_lease_for_gid(gid).is_some_and(|lease| {
                        self.ledger
                            .snapshot(InventoryTarget::LeasedVirtual {
                                host,
                                key: lease.key.0,
                                player: gid,
                                incarnation: lease.incarnation,
                                session,
                                kind,
                            })
                            .is_none()
                    })
            }
            _ => true,
        }
    }

    pub(super) fn player_inventory_address(
        &self,
        view: Option<pumpkin_cluster::canonical_action::TruthView>,
        lease: pumpkin_cluster::canonical_action::LivePlayerLease,
    ) -> Option<InventoryTarget> {
        if !self.historical_player_lease_matches(lease) {
            return None;
        }
        let leased = InventoryTarget::LeasedPlayer {
            key: lease.key.0,
            player: lease.gid,
            incarnation: lease.incarnation,
        };
        let plain = InventoryTarget::Player(lease.gid);
        let leased_present = view.map_or_else(
            || {
                self.ledger.snapshot(leased).is_some()
                    || self.ground_ledger.snapshot(leased).is_some()
            },
            |view| self.phase_snapshot(view, leased).is_some(),
        );
        let plain_present = view.map_or_else(
            || {
                self.ledger.snapshot(plain).is_some()
                    || self.ground_ledger.snapshot(plain).is_some()
            },
            |view| self.exact_phase_snapshot(view, plain).is_some(),
        );
        if leased_present && plain_present {
            return None;
        }
        if leased_present {
            return Some(leased);
        }
        (plain_present && self.live_player_lease_matches(lease)).then_some(plain)
    }

    pub(super) fn player_virtual_address(
        &self,
        view: Option<pumpkin_cluster::canonical_action::TruthView>,
        lease: pumpkin_cluster::canonical_action::LivePlayerLease,
        host: pumpkin_cluster::identity::ServerId,
        session: u64,
        kind: u16,
    ) -> Option<InventoryTarget> {
        if !self.historical_player_lease_matches(lease) {
            return None;
        }
        let leased = InventoryTarget::LeasedVirtual {
            host,
            key: lease.key.0,
            player: lease.gid,
            incarnation: lease.incarnation,
            session,
            kind,
        };
        let plain = InventoryTarget::Virtual {
            host,
            owner: ActorId::Player(lease.gid),
            session,
            kind,
        };
        let leased_present = view.map_or_else(
            || {
                self.ledger.snapshot(leased).is_some()
                    || self.ground_ledger.snapshot(leased).is_some()
            },
            |view| self.phase_snapshot(view, leased).is_some(),
        );
        let plain_present = view.map_or_else(
            || {
                self.ledger.snapshot(plain).is_some()
                    || self.ground_ledger.snapshot(plain).is_some()
            },
            |view| self.exact_phase_snapshot(view, plain).is_some(),
        );
        if leased_present && plain_present {
            return None;
        }
        if leased_present {
            return Some(leased);
        }
        (plain_present && self.live_player_lease_matches(lease)).then_some(plain)
    }

    pub(super) fn queue_canonical_publication(
        &mut self,
        snapshots: &[pumpkin_cluster::inventory::InventoryStateSnapshot],
    ) {
        for snapshot in snapshots {
            if let InventoryTarget::Player(gid) | InventoryTarget::LeasedPlayer { player: gid, .. } =
                snapshot.target
                && self.current_player_target_matches(snapshot.target)
            {
                self.pending_invsee_changes.insert(gid, snapshot.revision);
            }
        }
    }

    pub(super) fn publish_canonical_relocations(
        &mut self,
        server: &super::Server,
        targets: &[(InventoryTarget, ChunkKey, ChunkKey)],
    ) {
        for (target, old_chunk, new_chunk) in targets {
            let screens: Vec<_> = server
                .cluster_inventory_screens
                .load()
                .values()
                .filter(|screen| {
                    screen.slots.iter().any(|slot| slot.slot.target == *target)
                        || match target {
                            InventoryTarget::Player(gid) => screen
                                .preview
                                .as_ref()
                                .is_some_and(|preview| {
                                    matches!(preview.target, InventoryTarget::Player(player) if player == *gid)
                                }),
                            InventoryTarget::LeasedPlayer {
                                player: gid,
                                incarnation,
                                ..
                            } => {
                                screen.preview_incarnation == Some(*incarnation)
                                    && screen.preview.as_ref().is_some_and(|preview| {
                                        matches!(preview.target, InventoryTarget::Player(player) if player == *gid)
                                    })
                            }
                            _ => false,
                        }
                })
                .cloned()
                .collect();
            for mut screen in screens {
                let mut affected: Vec<_> = screen
                    .affected
                    .iter()
                    .copied()
                    .filter(|chunk| chunk != old_chunk)
                    .collect();
                affected.push(*new_chunk);
                affected.extend(screen.slots.iter().flat_map(|slot| {
                    self.ledger
                        .target_chunks(slot.slot.target)
                        .into_iter()
                        .flatten()
                        .copied()
                }));
                if let Some(lease) = self.player_lease_for_gid(screen.session.actor)
                    && let Some(address) = self.player_inventory_address(
                        Some(pumpkin_cluster::canonical_action::TruthView::Local),
                        lease,
                    )
                {
                    affected.extend(
                        self.ledger
                            .target_chunks(address)
                            .into_iter()
                            .flatten()
                            .copied(),
                    );
                }
                affected.sort_unstable();
                affected.dedup();
                screen.affected = affected;
                super::cluster_invsee::publish_inventory_screen(server, screen);
            }
            if let InventoryTarget::Player(gid)
            | InventoryTarget::LeasedPlayer { player: gid, .. } = target
            {
                self.pending_invsee_relocations.insert(*gid);
            }
        }
    }

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

    pub(super) fn detached() -> Self {
        Self {
            actions: InventoryOwnerInbox::default(),
            screens: InventoryOwnerInbox::default(),
            snapshots: InventoryOwnerInbox::default(),
            lifecycle: InventoryOwnerInbox::default(),
            menus: InventoryOwnerInbox::default(),
            ledger: InvLedger::new(),
            ground_ledger: InvLedger::new(),
            prepared_player_inventories: HashMap::new(),
            canonical_admission_roots: HashMap::new(),
            canonical_menu_admissions: HashMap::new(),
            canonical_block_roots: HashMap::new(),
            replacement_epoch: 0,
            canonical_entity_admissions: HashMap::new(),
            player_keys: HashMap::new(),
            historical_player_keys: HashMap::new(),
            historical_player_leases: HashMap::new(),
            player_install_tokens: HashMap::new(),
            player_incarnations: HashMap::new(),
            latest_player_install_tokens: HashMap::new(),
            sequences: HashMap::new(),
            origins: HashMap::new(),
            remote_previews: HashMap::new(),
            screen_sessions: InventoryScreenSessions::new(),
            active_screens: HashMap::new(),
            pending_canonical_opens: HashMap::new(),
            pending_raw_menu_opens: std::collections::VecDeque::new(),
            canonical_closes: HashMap::new(),
            canonical_close_notifications: HashMap::new(),
            canonical_close_retries: HashMap::new(),
            canonical_death_closes: HashMap::new(),
            canonical_close_status_order: HashMap::new(),
            canonical_merchant_publications: HashMap::new(),
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

    pub(super) fn attach(server: &Server) -> Self {
        let mut owner = Self::detached();
        owner.actions = super::cluster_invsee::attach_inventory_action_owner(server).into();
        owner.screens = super::cluster_invsee::attach_inventory_screen_owner(server).into();
        owner.snapshots = super::cluster_invsee::attach_inventory_snapshot_owner(server).into();
        owner.lifecycle = super::cluster_invsee::attach_inventory_install_owner(server).into();
        owner.menus = super::cluster_inventory_protocol::attach_inventory_menu_owner(server).into();
        owner
    }

    pub(super) fn preflight_chunk_unit(&self, chunks: &BTreeSet<ChunkKey>, players: &[pumpkin_cluster::canonical_action::LivePlayerLease], entities: &[pumpkin_cluster::protocol::GlobalEntityId], roots: &BTreeSet<InventoryTarget>) -> Result<(), InventoryApplyError> {
        self.ledger.preflight_chunk_unit(chunks).map_err(|_| InventoryApplyError::TargetUnavailable)?;
        self.ground_ledger.preflight_chunk_unit(chunks).map_err(|_| InventoryApplyError::TargetUnavailable)?;
        if players.iter().any(|lease| lease.incarnation == 0 || lease.key.0.is_nil())
            || entities.iter().any(|id| id.generation == 0)
            || roots.iter().any(|target| target.intrinsic_chunk().is_some_and(|chunk| !chunks.contains(&chunk))
                || self.canonical_block_roots.get(target).is_some_and(|root| !chunks.contains(&root.chunk))
                || [self.ledger.target_chunks(*target), self.ground_ledger.target_chunks(*target)].into_iter().flatten().flatten().any(|chunk| !chunks.contains(chunk))) {
            return Err(InventoryApplyError::TargetUnavailable);
        }
        Ok(())
    }

    pub(super) fn unit_dependencies(&self, chunks: &BTreeSet<ChunkKey>, roots: &BTreeSet<InventoryTarget>) -> Result<(BTreeSet<ChunkKey>, BTreeSet<pumpkin_cluster::canonical_action::LivePlayerLease>, BTreeSet<pumpkin_cluster::protocol::GlobalEntityId>, BTreeSet<InventoryTarget>), InventoryApplyError> {
        use pumpkin_cluster::canonical_action::{LivePlayerLease, PlayerKey};
        let mut all_chunks = chunks.clone();
        let mut all_roots = roots.clone();
        let mut players = BTreeSet::new();
        let mut entities = BTreeSet::new();
        let mut pending_chunks = std::collections::VecDeque::from_iter(chunks.iter().copied());
        let mut pending_roots = std::collections::VecDeque::from_iter(roots.iter().copied());
        loop {
            while let Some(chunk) = pending_chunks.pop_front() {
                for target in self.ledger.targets_for_chunk(chunk).into_iter().chain(self.ground_ledger.targets_for_chunk(chunk)) {
                    if all_roots.insert(target) { pending_roots.push_back(target); }
                }
            }
            let Some(target) = pending_roots.pop_front() else { break; };
            for chunk in [self.ledger.target_chunks(target), self.ground_ledger.target_chunks(target)].into_iter().flatten().flatten().copied().chain(target.intrinsic_chunk()) {
                if all_chunks.insert(chunk) { pending_chunks.push_back(chunk); }
            }
            let lease = match target {
                InventoryTarget::LeasedPlayer { key, player, incarnation } | InventoryTarget::LeasedVirtual { key, player, incarnation, .. } => Some(LivePlayerLease { key: PlayerKey(key), gid: player, incarnation }),
                InventoryTarget::Player(player) | InventoryTarget::Virtual { owner: ActorId::Player(player), .. } => Some(self.player_lease_for_gid(player).ok_or(InventoryApplyError::TargetUnavailable)?),
                InventoryTarget::Entity(id) => {
                    let id = pumpkin_cluster::protocol::GlobalEntityId { origin: id.origin, local_id: id.local_id, generation: id.generation };
                    entities.insert(id);
                    if self.ledger.target_chunks(target).is_none() && self.ground_ledger.target_chunks(target).is_none()
                        && let Some(root) = self.canonical_entity_admissions.get(&id)
                        && all_chunks.insert(root.chunk) { pending_chunks.push_back(root.chunk); }
                    None
                }
                _ => None,
            };
            if let Some(lease) = lease {
                if lease.incarnation == 0 || lease.key.0.is_nil() { return Err(InventoryApplyError::TargetUnavailable); }
                players.insert(lease);
                if let Some(current) = self.player_lease_for_gid(lease.gid).filter(|current| *current != lease) {
                    let current_root = InventoryTarget::LeasedPlayer { key: current.key.0, player: current.gid, incarnation: current.incarnation };
                    if all_roots.insert(current_root) { pending_roots.push_back(current_root); }
                }
                let player = InventoryTarget::LeasedPlayer { key: lease.key.0, player: lease.gid, incarnation: lease.incarnation };
                if all_roots.insert(player) { pending_roots.push_back(player); }
                if self.ledger.target_chunks(player).is_none() && self.ground_ledger.target_chunks(player).is_none()
                    && let Some(root) = self.canonical_admission_roots.get(&lease)
                    && all_chunks.insert(root.chunk) { pending_chunks.push_back(root.chunk); }
            }
        }
        Ok((all_chunks, players, entities, all_roots))
    }

    pub(super) fn take_chunk_unit(&mut self, chunks: &BTreeSet<ChunkKey>, players: &[pumpkin_cluster::canonical_action::LivePlayerLease], entities: &[pumpkin_cluster::protocol::GlobalEntityId], roots: &BTreeSet<InventoryTarget>) -> Result<Self, InventoryApplyError> {
        self.preflight_chunk_unit(chunks, players, entities, roots)?;
        let targets: BTreeSet<_> = chunks.iter().flat_map(|chunk| self.ledger.targets_for_chunk(*chunk).into_iter().chain(self.ground_ledger.targets_for_chunk(*chunk))).chain(roots.iter().copied()).collect();
        let mut unit = Self::detached();
        unit.ledger = self.ledger.take_chunk_unit(chunks).map_err(|_| InventoryApplyError::TargetUnavailable)?;
        unit.ground_ledger = self.ground_ledger.take_chunk_unit(chunks).map_err(|_| InventoryApplyError::TargetUnavailable)?;
        unit.replacement_epoch = self.replacement_epoch;
        let mut leases: BTreeSet<_> = players.iter().copied().collect();
        for &id in entities {
            if let Some(root) = self.canonical_entity_admissions.remove(&id) { unit.canonical_entity_admissions.insert(id, root); }
        }
        for target in targets {
            if let Some(root) = self.canonical_block_roots.remove(&target) { unit.canonical_block_roots.insert(target, root); }
            if let Some(root) = self.canonical_menu_admissions.remove(&target) { unit.canonical_menu_admissions.insert(target, root); }
            let lease = match target {
                InventoryTarget::LeasedPlayer { key, player, incarnation } | InventoryTarget::LeasedVirtual { key, player, incarnation, .. } => Some(pumpkin_cluster::canonical_action::LivePlayerLease { key: pumpkin_cluster::canonical_action::PlayerKey(key), gid: player, incarnation }),
                InventoryTarget::Player(player) | InventoryTarget::Virtual { owner: ActorId::Player(player), .. } => self.player_lease_for_gid(player),
                InventoryTarget::Entity(id) => {
                    let id = pumpkin_cluster::protocol::GlobalEntityId { origin: id.origin, local_id: id.local_id, generation: id.generation };
                    if let Some(root) = self.canonical_entity_admissions.remove(&id) { unit.canonical_entity_admissions.insert(id, root); }
                    None
                }
                _ => None,
            };
            if let Some(lease) = lease { leases.insert(lease); }
        }
        for lease in leases {
                if let Some(root) = self.canonical_admission_roots.remove(&lease) { unit.canonical_admission_roots.insert(lease, root); }
                if let Some(key) = self.historical_player_leases.remove(&(lease.gid, lease.incarnation)) { unit.historical_player_leases.insert((lease.gid, lease.incarnation), key); }
                if let Some(gid) = self.player_keys.remove(&lease.key.0) { unit.player_keys.insert(lease.key.0, gid); }
                if let Some(key) = self.historical_player_keys.remove(&lease.gid) { unit.historical_player_keys.insert(lease.gid, key); }
                if let Some(value) = self.player_install_tokens.remove(&lease.gid) { unit.player_install_tokens.insert(lease.gid, value); }
                if let Some(value) = self.player_incarnations.remove(&lease.gid) { unit.player_incarnations.insert(lease.gid, value); }
                if let Some(value) = self.latest_player_install_tokens.remove(&lease.gid) { unit.latest_player_install_tokens.insert(lease.gid, value); }
        }
        Ok(unit)
    }

    pub(super) fn unit_return_keys(&self) -> impl Iterator<Item = InventoryUnitReturnKey> + '_ {
        use InventoryUnitReturnKey as K;
        self.ledger.target_keys().map(K::LocalRoot)
            .chain(self.ground_ledger.target_keys().map(K::GroundRoot))
            .chain(self.canonical_block_roots.keys().copied().map(K::BlockRoot))
            .chain(self.canonical_menu_admissions.keys().copied().map(K::MenuRoot))
            .chain(self.canonical_entity_admissions.keys().copied().map(K::EntityRoot))
            .chain(self.canonical_admission_roots.keys().copied().map(K::PlayerRoot))
            .chain(self.player_keys.keys().copied().map(K::PlayerKey))
            .chain(self.historical_player_keys.keys().copied().map(K::HistoricalPlayer))
            .chain(self.historical_player_leases.keys().map(|(gid, incarnation)| K::HistoricalLease(*gid, *incarnation)))
            .chain(self.player_install_tokens.keys().copied().map(K::InstallToken))
            .chain(self.player_incarnations.keys().copied().map(K::Incarnation))
            .chain(self.latest_player_install_tokens.keys().copied().map(K::LatestInstallToken))
    }

    pub(super) fn claim_unit_return_keys(&self, claimed: &mut BTreeSet<InventoryUnitReturnKey>) -> bool {
        if self.unit_return_keys().any(|key| claimed.contains(&key)) { return false; }
        claimed.extend(self.unit_return_keys());
        true
    }

    pub(super) fn can_return_chunk_unit(&self, unit: &Self) -> bool {
        !(!self.ledger.can_return_chunk_unit(&unit.ledger)
            || !self.ground_ledger.can_return_chunk_unit(&unit.ground_ledger)
            || unit.canonical_block_roots.keys().any(|target| self.canonical_block_roots.contains_key(target))
            || unit.canonical_menu_admissions.keys().any(|target| self.canonical_menu_admissions.contains_key(target))
            || unit.canonical_entity_admissions.keys().any(|target| self.canonical_entity_admissions.contains_key(target))
            || unit.canonical_admission_roots.keys().any(|target| self.canonical_admission_roots.contains_key(target))
            || unit.player_keys.keys().any(|target| self.player_keys.contains_key(target))
            || unit.historical_player_keys.keys().any(|target| self.historical_player_keys.contains_key(target))
            || unit.historical_player_leases.keys().any(|target| self.historical_player_leases.contains_key(target))
            || unit.player_install_tokens.keys().any(|target| self.player_install_tokens.contains_key(target))
            || unit.player_incarnations.keys().any(|target| self.player_incarnations.contains_key(target))
            || unit.latest_player_install_tokens.keys().any(|target| self.latest_player_install_tokens.contains_key(target)))
    }

    pub(super) fn return_chunk_unit(&mut self, mut unit: Self) -> Result<(), Self> {
        if !self.can_return_chunk_unit(&unit) { return Err(unit); }
        if let Err(ledger) = self.ledger.return_chunk_unit(std::mem::take(&mut unit.ledger)) { unit.ledger = ledger; return Err(unit); }
        if let Err(ledger) = self.ground_ledger.return_chunk_unit(std::mem::take(&mut unit.ground_ledger)) { unit.ground_ledger = ledger; return Err(unit); }
        self.canonical_block_roots.extend(unit.canonical_block_roots.drain());
        self.canonical_menu_admissions.extend(unit.canonical_menu_admissions.drain());
        self.canonical_entity_admissions.extend(unit.canonical_entity_admissions.drain());
        self.canonical_admission_roots.extend(unit.canonical_admission_roots.drain());
        self.player_keys.extend(unit.player_keys.drain());
        self.historical_player_keys.extend(unit.historical_player_keys.drain());
        self.historical_player_leases.extend(unit.historical_player_leases.drain());
        self.player_install_tokens.extend(unit.player_install_tokens.drain());
        self.player_incarnations.extend(unit.player_incarnations.drain());
        self.latest_player_install_tokens.extend(unit.latest_player_install_tokens.drain());
        self.pending_invsee_changes.extend(unit.pending_invsee_changes.drain());
        self.pending_invsee_relocations.extend(unit.pending_invsee_relocations.drain());
        self.pending_invsee_removals.extend(unit.pending_invsee_removals.drain());
        self.replacement_epoch = self.replacement_epoch.max(unit.replacement_epoch);
        Ok(())
    }

    pub(super) fn snapshot(
        &self,
        request: LocalInventorySnapshotRequest,
        provider: pumpkin_cluster::realm_completion::PeerGeneration,
        cut: pumpkin_cluster::time::TickStamp,
        graphs: &pumpkin_cluster::action_graph::ActionGraphs,
        living: Option<pumpkin_cluster::canonical_action::Cell>,
        versions: &std::collections::BTreeMap<
            (
                pumpkin_cluster::canonical_action::TruthView,
                pumpkin_cluster::canonical_action::Target,
            ),
            u64,
        >,
    ) {
        let result = match request.target {
            InventoryTarget::Player(gid) => self.player_lease_for_gid(gid).and_then(|lease| {
                self.live_player_lease_matches(lease)
                    .then_some(lease)
                    .and_then(|lease| {
                        let target = self.player_inventory_address(
                            Some(pumpkin_cluster::canonical_action::TruthView::Local),
                            lease,
                        )?;
                        let mut state = self.ledger.snapshot(target)?;
                        let chunks = self.ledger.target_chunks(target)?.to_vec();
                        if chunks.len() != 1 || provider.server != lease.gid.server || provider.generation == 0 { return None; }
                        let frontiers = [INV_MAIN, INV_ARMOR, INV_OFFHAND, INV_SELECTED].into_iter().map(|inv| {
                            let realm = pumpkin_cluster::action_graph::InventoryRealmId::Player(inv);
                            let frontier = graphs.player_inventory_frontiers.get(&(lease.key, realm, lease.gid, lease.incarnation))?;
                            (frontier.lease == lease && frontier.accepted_chunk == chunks[0]
                                && !frontier.settled_through.is_after(cut)).then_some((realm, *frontier))
                        }).collect::<Option<Vec<_>>>()?;
                        state.target = InventoryTarget::Player(gid);
                        let mut inventory_cells = std::collections::BTreeMap::new();
                        for (inv, count) in [(INV_MAIN, 36), (INV_ARMOR, 4), (INV_OFFHAND, 1), (INV_CURSOR, 1), (INV_SELECTED, 1)] {
                            for slot in 0..count {
                                let loc = pumpkin_cluster::inventory::InvLoc::new(inv, slot);
                                let target = pumpkin_cluster::canonical_action::Target::Player { key: lease.key, lease: Some(lease),
                                    inventory: Some(pumpkin_cluster::canonical_action::InventoryId::Player { inv, slot }) };
                                inventory_cells.insert(loc, pumpkin_cluster::canonical_action::Cell {
                                    revision: versions.get(&(pumpkin_cluster::canonical_action::TruthView::Local, target)).copied().unwrap_or(state.revision),
                                    value: pumpkin_cluster::canonical_action::Value::Inventory(state.cells.iter().find(|cell| cell.loc == loc)
                                        .cloned().unwrap_or_else(|| pumpkin_cluster::inventory::InventoryCell::empty(loc))),
                                });
                            }
                        }
                        Some(super::cluster_invsee::LocalInventorySnapshotResponse {
                            state,
                            lease,
                            authority: pumpkin_cluster::invsee::InventoryPreviewAuthority {
                                provider, cut, frontiers, living: living.clone()?,
                                inventory_cells,
                                living_ground_anchor: graphs.ground_anchors.get(&pumpkin_cluster::canonical_action::Target::Chunk {
                                    key: chunks[0], part: pumpkin_cluster::canonical_action::ChunkPart::Player(gid), inventory: None,
                                })?.clone(),
                            },
                            chunks,
                        })
                    })
            }),
            _ => None,
        };
        let _ = request.reply.send(result);
    }

    pub(super) fn retire_player_after_actor(
        &mut self,
        server: &Server,
        lease: pumpkin_cluster::canonical_action::LivePlayerLease,
    ) {
        self.retire_player_inventory(server, lease);
    }

    fn retire_player_inventory(
        &mut self,
        server: &Server,
        lease: pumpkin_cluster::canonical_action::LivePlayerLease,
    ) {
        let gid = lease.gid;
        let leased = InventoryTarget::LeasedPlayer {
            key: lease.key.0,
            player: gid,
            incarnation: lease.incarnation,
        };
        if self.historical_player_leases.get(&(gid, lease.incarnation)) != Some(&lease.key.0) {
            return;
        }
        super::cluster_invsee::unpublish_inventory_snapshot(server, leased);
        super::cluster_invsee::unpublish_inventory_screens_for_target(server, leased);
        let mut historical_targets = self.ledger.targets();
        historical_targets.extend(self.ground_ledger.targets());
        historical_targets.sort_unstable();
        historical_targets.dedup();
        for target in historical_targets {
            if matches!(target, InventoryTarget::LeasedVirtual { key, player, incarnation, .. }
                if key == lease.key.0 && player == gid && incarnation == lease.incarnation)
            {
                super::cluster_invsee::unpublish_inventory_snapshot(server, target);
                super::cluster_invsee::unpublish_inventory_screens_for_target(server, target);
            }
        }
        if self.historical_player_keys.get(&gid) != Some(&lease.key.0)
            || self.player_incarnations.get(&gid) != Some(&lease.incarnation)
        {
            return;
        }
        self.canonical_close_status_order.remove(&gid);
        server.cluster_inventory_close_status.rcu(|current| {
            let mut next = (**current).clone();
            next.retain(|(actor, _), _| *actor != gid);
            Arc::new(next)
        });
        let player = InventoryTarget::Player(gid);
        let crafting = InventoryTarget::Virtual {
            host: gid.server,
            owner: ActorId::Player(gid),
            session: 0,
            kind: pumpkin_data::screen::WindowType::Crafting as u16 + 1,
        };
        let ender = super::cluster_invsee::ender_chest_target(gid);
        self.ledger.remove_target(player);
        self.ledger.remove_target(crafting);
        self.ledger.remove_target(ender);
        self.ground_ledger.remove_target(player);
        self.ground_ledger.remove_target(crafting);
        self.ground_ledger.remove_target(ender);
        self.player_keys.retain(|_, actor| *actor != gid);
        self.historical_player_keys.remove(&gid);
        self.player_install_tokens.remove(&gid);
        self.player_incarnations.remove(&gid);
        self.prepared_player_inventories.remove(&gid);
        for target in [player, crafting, ender] {
            super::cluster_invsee::unpublish_inventory_snapshot(server, target);
        }
        super::cluster_invsee::unpublish_inventory_screen(
            server,
            InventoryScreenSession {
                actor: gid,
                session: 0,
            },
        );
        self.pending_invsee_changes.remove(&gid);
        self.pending_invsee_removals.insert(gid);
    }

    pub(super) fn drain_handoff_cancellations(&mut self, server: &Server) {
        let pending = server.cluster_inventory_cancel_requests.load_full();
        let statuses = server.cluster_inventory_handoff_installed.load_full();
        let mut completed = Vec::new();
        for (gid, token) in pending.iter() {
            if self.latest_player_install_tokens.get(gid) == Some(token)
                && !statuses.contains_key(&(*gid, *token))
            {
                completed.push((*gid, *token));
                continue;
            }
            if self
                .latest_player_install_tokens
                .get(gid)
                .is_some_and(|latest| {
                    let distance = latest.wrapping_sub(*token);
                    distance != 0 && distance < (1_u64 << 63)
                })
            {
                completed.push((*gid, *token));
                continue;
            }
            if let Some(status) = statuses.get(&(*gid, *token)) {
                if status.revision.is_ok() && self.player_install_tokens.get(gid) == Some(token) {
                    if let Some(lease) = self.player_lease_for_gid(*gid) {
                        self.retire_player_inventory(server, lease);
                    }
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

    pub(super) fn phase_target_chunks(
        &self,
        view: pumpkin_cluster::canonical_action::TruthView,
        target: InventoryTarget,
    ) -> Option<&[ChunkKey]> {
        match view {
            pumpkin_cluster::canonical_action::TruthView::Local => {
                self.ledger.target_chunks(target)
            }
            pumpkin_cluster::canonical_action::TruthView::Ground => {
                self.ground_ledger.target_chunks(target)
            }
        }
    }

    pub(super) fn ground_snapshot_for_chunk(
        &self,
        target: InventoryTarget,
        chunk: ChunkKey,
    ) -> Option<pumpkin_cluster::inventory::InventoryStateSnapshot> {
        self.ground_ledger
            .target_chunks(target)?
            .contains(&chunk)
            .then(|| {
                self.exact_phase_snapshot(
                    pumpkin_cluster::canonical_action::TruthView::Ground,
                    target,
                )
            })?
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

    pub(super) fn active_inventory_screen(&self, actor: GlobalPlayerId) -> InventoryScreenSession {
        self.active_screens.get(&actor).copied().unwrap_or(InventoryScreenSession { actor, session: 0 })
    }

    pub(super) fn retained_death_close(&self, lease: pumpkin_cluster::canonical_action::LivePlayerLease,
        reference: pumpkin_cluster::canonical_action::ActionRef, cause: pumpkin_cluster::canonical_action::ActionRef)
        -> Option<pumpkin_cluster::canonical_inventory_close::InventoryCloseIntent> {
        if self.canonical_death_closes.get(&reference) != Some(&cause) { return None; }
        self.canonical_closes.get(&reference).filter(|intent| intent.lease == lease).cloned()
    }

    pub(super) fn certifies_retained_closed_slot(
        &self,
        lease: pumpkin_cluster::canonical_action::LivePlayerLease,
        reference: pumpkin_cluster::canonical_action::ActionRef,
        cause: pumpkin_cluster::canonical_action::ActionRef,
        menu: &pumpkin_cluster::canonical_menu::MenuSessionState,
        binding: &pumpkin_cluster::canonical_inventory::InventorySlotBinding,
    ) -> bool {
        if menu.last_close != Some(reference) || self.canonical_death_closes.get(&reference) != Some(&cause) { return false; }
        let Some(close) = self.canonical_closes.get(&reference).filter(|close| close.lease == lease) else { return false; };
        let Some(root) = self.canonical_menu_admission_slot(binding.target) else { return false; };
        root.admission.lease == lease && root.admission.session == close.session.session
            && root.admission.layout.window_kind() == close.window_kind
            && root.admission.inventory_target() == binding.slot.target
            && root.admission.entries().iter().any(|(target, cell)| *target == binding.target && cell.loc == binding.slot.loc)
            && close.bindings().iter().any(|retained| retained == binding)
            && menu.closed_sessions.iter().any(|closed| closed.action == reference && closed.session == close.session.session && closed.window_kind == close.window_kind)
    }

    pub(super) fn track_canonical_close(&mut self, action: &pumpkin_cluster::canonical_action::Action)
        -> Result<(), pumpkin_cluster::canonical_action::ApplyFailure> {
        let intent = match &action.semantic_intent {
            Some(pumpkin_cluster::canonical_action::SemanticIntent::InventoryClose(intent)) => intent,
            _ => match action.death_inventory_intent().and_then(|death| death.close.as_ref()) {
                Some(intent) => intent,
                None => return Ok(()),
            },
        };
        let reference = pumpkin_cluster::canonical_action::ActionRef { tick: action.tick, id: action.id };
        if let Some(existing) = self.canonical_closes.get(&reference) {
            return if existing == intent { Ok(()) } else {
                Err(pumpkin_cluster::canonical_action::ApplyFailure::InvalidAction)
            };
        }
        if self.canonical_closes.len() + self.canonical_close_retries.len() >= 4096 {
            return Err(pumpkin_cluster::canonical_action::ApplyFailure::Capacity);
        }
        self.canonical_closes.insert(reference, intent.clone());
        if let Some(death) = action.death_inventory_intent() { self.canonical_death_closes.insert(reference, death.death_cause); }
        Ok(())
    }

    pub(super) fn pending_canonical_closes(&self) -> Vec<pumpkin_cluster::canonical_action::ActionRef> {
        self.canonical_closes.keys().copied().collect()
    }

    pub(super) fn canonical_close_tick_pending(&self, tick: pumpkin_cluster::time::TickStamp) -> bool {
        self.canonical_closes.keys().any(|reference| reference.tick == tick)
    }

    pub(super) fn attach_canonical_close_notification(&mut self,
        reference: pumpkin_cluster::canonical_action::ActionRef,
        notification: super::cluster_inventory_close::CanonicalCloseNotification)
        -> Result<(), pumpkin_cluster::canonical_action::ApplyFailure> {
        if !self.can_attach_canonical_close_notification(reference) {
            return Err(pumpkin_cluster::canonical_action::ApplyFailure::Capacity);
        }
        if self.canonical_closes.get(&reference).is_none_or(|intent|
            intent.lease != notification.lease || intent.session != notification.session
                || intent.window_kind != notification.window_kind) {
            return Err(pumpkin_cluster::canonical_action::ApplyFailure::InvalidAction);
        }
        self.canonical_close_notifications.insert(reference, notification);
        Ok(())
    }

    pub(super) fn can_attach_canonical_close_notification(&self,
        reference: pumpkin_cluster::canonical_action::ActionRef) -> bool {
        !self.canonical_close_notifications.contains_key(&reference)
            && self.canonical_close_notifications.len() < 4096
            && self.canonical_closes.contains_key(&reference)
    }

    pub(super) fn finish_canonical_close(&mut self, server: &Server,
        reference: pumpkin_cluster::canonical_action::ActionRef, accepted: bool, actual_close: bool)
        -> Result<(), pumpkin_cluster::canonical_action::ApplyFailure> {
        let Some(intent) = self.canonical_closes.get(&reference).cloned() else { return Ok(()); };
        if !accepted {
            if self.canonical_death_closes.remove(&reference).is_some() {
                self.canonical_closes.remove(&reference);
                self.canonical_close_notifications.remove(&reference);
                return Ok(());
            }
            let key = (intent.lease.gid, intent.token);
            if self.canonical_close_retries.get(&key).is_some_and(|existing| existing != &intent) {
                return Err(pumpkin_cluster::canonical_action::ApplyFailure::InvalidAction);
            }
            self.canonical_close_retries.insert(key, intent);
            self.canonical_closes.remove(&reference);
            self.canonical_close_notifications.remove(&reference);
            return Ok(());
        }
        if actual_close && super::cluster_invsee::find_inventory_player(server, intent.lease.gid).is_some()
            && !self.canonical_close_notifications.contains_key(&reference) {
            return Err(pumpkin_cluster::canonical_action::ApplyFailure::Capacity);
        }
        if actual_close && intent.session.session != 0 {
            if let Some(screen) = super::cluster_invsee::inventory_screen_snapshot(server, intent.session) {
                self.queue_invsee_screen(&screen, false);
            }
            super::cluster_invsee::unpublish_inventory_screen(server, intent.session);
            self.screen_chunks.remove(&intent.session);
            self.menu_metadata.remove(&intent.session);
            self.ender_open_sessions.remove(&intent.session);
        }
        if actual_close && self.active_screens.get(&intent.lease.gid) == Some(&intent.session) {
            self.active_screens.remove(&intent.lease.gid);
        }
        let mut statuses = server.cluster_inventory_close_status.load_full().as_ref().clone();
        let order = self.canonical_close_status_order.entry(intent.lease.gid).or_default();
        if !order.contains(&intent.token) { order.push_back(intent.token); }
        while order.len() > 64 {
            if let Some(token) = order.pop_front() { statuses.remove(&(intent.lease.gid, token)); }
        }
        statuses.insert((intent.lease.gid, intent.token), InventoryCloseStatus {
            actor: intent.lease.gid, session: intent.session.session,
            token: intent.token, accepted_tick: reference.tick });
        server.cluster_inventory_close_status.store(Arc::new(statuses));
        self.canonical_closes.remove(&reference);
        self.canonical_death_closes.remove(&reference);
        if let Some(notification) = self.canonical_close_notifications.remove(&reference) {
            if actual_close {
                notification.permit.send(super::cluster_invsee::InvseeCommand::AcceptedClose {
                    player: notification.player, lease: intent.lease, session: intent.session,
                    window_kind: intent.window_kind });
            }
        }
        Ok(())
    }

    pub(super) fn retry_canonical_closes(&mut self, server: &Server) {
        let retries = self.canonical_close_retries.keys().copied().collect::<Vec<_>>();
        for key in retries {
            let Some(intent) = self.canonical_close_retries.get(&key) else { continue; };
            if !self.live_player_lease_matches(intent.lease) {
                self.canonical_close_retries.remove(&key);
                continue;
            }
            let Some(player) = super::cluster_invsee::find_inventory_player(server, intent.lease.gid) else {
                continue;
            };
            let session = (!intent.departure).then_some(intent.session);
            if super::cluster_inventory_protocol::stage_inventory_close_retry(&player, session, intent.token).is_ok() {
                self.canonical_close_retries.remove(&key);
            }
        }
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
        super::cluster_invsee::inventory_screen_snapshot(server, session).map(|snapshot| {
            if matches!(&screen.command, InventoryScreenCommand::Close { .. }) {
                let mut targets = vec![InventoryTarget::Player(session.actor)];
                targets.extend(snapshot.slots.iter().filter_map(|slot| {
                    slot.slot
                        .target
                        .is_screen_virtual()
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
            LocalInventoryLifecycle::CanonicalOpenPresentation(open) => screen_touches(&open.screen),
            LocalInventoryLifecycle::CloseScreen { session } => {
                self.screen_chunks
                    .get(session)
                    .is_some_and(|chunks| chunks.iter().any(|chunk| quarantined.contains(chunk)))
                    || super::cluster_invsee::inventory_screen_snapshot(server, *session)
                        .is_none_or(|screen| screen_touches(&screen))
            }
            LocalInventoryLifecycle::BeginDeparture { actor, .. } => {
                target_touches(InventoryTarget::Player(*actor))
                    || server
                        .cluster_inventory_screens
                        .load()
                        .values()
                        .any(|screen| {
                            screen.session.actor == *actor
                                && screen.session.session != 0
                                && screen.slots.iter().any(|slot| {
                                    slot.slot.target.is_screen_virtual()
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

    pub(super) fn rollback_fresh_block_roots(&mut self, chunk: ChunkKey) {
        let targets = self.canonical_block_roots.keys().copied().filter(|target|
            matches!(target, InventoryTarget::Block { chunk: key, .. } if *key == chunk)).collect::<Vec<_>>();
        for target in targets {
            self.canonical_block_roots.remove(&target);
            self.ledger.remove_target(target);
            self.ground_ledger.remove_target(target);
        }
    }

    pub(super) fn block_inventory_bindings(&self, chunk: ChunkKey, position: pumpkin_cluster::protocol::BlockPos)
        -> Result<Vec<pumpkin_cluster::canonical_inventory::InventorySlotBinding>, InventoryApplyError> {
        let root = self.canonical_block_roots.get(&InventoryTarget::Block { chunk, position })
            .ok_or(InventoryApplyError::TargetUnavailable)?;
        if root.deferred_loot { return Err(InventoryApplyError::Rejected); }
        Ok(root.bindings())
    }

    pub(super) fn prepare_fresh_block_roots(&self, chunk: ChunkKey,
        images: Vec<super::cluster_inventory_block_roots::BlockInventoryRootImage>,
    ) -> Result<PreparedInventoryImport, InventoryApplyError> {
        let mut states = Vec::new();
        for image in &images {
            if let Some(root) = super::cluster_inventory_block_roots::CanonicalBlockInventoryRoot::from_image(chunk, image.clone())? {
                states.push(ChunkInventoryState { menu_admission: None, snapshot: root.persisted_snapshot()?, chunks: vec![chunk], beacon: None, player_lease: None });
            }
        }
        let prepared = self.prepare_from_chunk(chunk, states)?;
        self.bind_imported_block_roots(prepared, images)
    }

    pub(super) fn bind_imported_block_roots(&self, mut prepared: PreparedInventoryImport,
        images: Vec<super::cluster_inventory_block_roots::BlockInventoryRootImage>,
    ) -> Result<PreparedInventoryImport, InventoryApplyError> {
        let mut roots = self.canonical_block_roots.clone();
        roots.retain(|target, _| !matches!(target, InventoryTarget::Block { chunk, .. } if *chunk == prepared.chunk));
        let mut expected = BTreeSet::new();
        let mut positions = BTreeSet::new();
        for image in &images {
            if !positions.insert(image.position) { return Err(InventoryApplyError::Rejected); }
            if let Some(root) = super::cluster_inventory_block_roots::CanonicalBlockInventoryRoot::from_image(prepared.chunk, image.clone())? {
                let target = root.inventory_target();
                let state = prepared.states.iter().find(|state| state.snapshot.target == target)
                    .ok_or(InventoryApplyError::Rejected)?;
                root.validate_snapshot(&state.snapshot)?;
                expected.insert(target);
                roots.insert(target, root);
            }
        }
        let actual = prepared.states.iter().filter_map(|state| matches!(state.snapshot.target, InventoryTarget::Block { .. })
            .then_some(state.snapshot.target)).collect::<BTreeSet<_>>();
        if actual != expected { return Err(InventoryApplyError::Rejected); }
        prepared.block_root_images = Some(images);
        prepared.block_roots = roots;
        Ok(prepared)
    }

    pub(super) fn export_for_chunk(&self, chunk: ChunkKey) -> Option<Vec<ChunkInventoryState>> {
        self.ground_ledger
            .targets_for_chunk(chunk)
            .into_iter()
            .map(|target| {
                let player_gid = match target {
                    InventoryTarget::Player(gid)
                    | InventoryTarget::LeasedPlayer { player: gid, .. }
                    | InventoryTarget::LeasedVirtual { player: gid, .. }
                    | InventoryTarget::Virtual {
                        owner: ActorId::Player(gid),
                        ..
                    } => Some(gid),
                    _ => None,
                };
                let player_lease = match player_gid {
                    Some(gid) => {
                        let (key, incarnation) = match target {
                            InventoryTarget::LeasedPlayer {
                                key, incarnation, ..
                            }
                            | InventoryTarget::LeasedVirtual {
                                key, incarnation, ..
                            } => {
                                if self.historical_player_leases.get(&(gid, incarnation))
                                    != Some(&key)
                                {
                                    return None;
                                }
                                (key, incarnation)
                            }
                            _ => (
                                *self.historical_player_keys.get(&gid)?,
                                *self.player_incarnations.get(&gid)?,
                            ),
                        };
                        Some(pumpkin_cluster::canonical_action::LivePlayerLease {
                            key: pumpkin_cluster::canonical_action::PlayerKey(key),
                            gid,
                            incarnation,
                        })
                    }
                    None => None,
                };
                Some(ChunkInventoryState {
            menu_admission: self.canonical_menu_admissions.get(&target).map(|root| (root.reference, root.admission.clone())),
                    snapshot: self.exact_phase_snapshot(
                        pumpkin_cluster::canonical_action::TruthView::Ground,
                        target,
                    )?,
                    chunks: self.ground_ledger.target_chunks(target)?.to_vec(),
                    beacon: self.beacons.get(&target).copied(),
                    player_lease,
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
        let mut player_keys = self.player_keys.clone();
        let mut historical_player_keys = self.historical_player_keys.clone();
        let mut historical_player_leases = self.historical_player_leases.clone();
        let mut player_incarnations = self.player_incarnations.clone();
        let mut menu_roots = self.canonical_menu_admissions.clone();
        let mut targets = std::collections::HashSet::new();
        for state in &states {
            let target = state.snapshot.target;
            if let Some((reference, admission)) = &state.menu_admission {
                if reference.id.actor != ActorId::Player(admission.lease.gid) || reference.id.incarnation != admission.lease.incarnation
                    || state.player_lease != Some(admission.lease) || admission.session == 0
                    || admission.inventory_target() != target || state.snapshot.cells.len() != usize::from(admission.layout.slots())
                    || state.snapshot.cells.iter().map(|cell| cell.loc).collect::<BTreeSet<_>>()
                        != admission.entries().iter().map(|(_, cell)| cell.loc).collect() {
                    return Err(InventoryApplyError::Rejected);
                }
                let root = CanonicalMenuInventoryAdmission { reference: *reference, admission: admission.clone() };
                if menu_roots.get(&target).is_some_and(|prior| prior != &root) { return Err(InventoryApplyError::Rejected); }
                menu_roots.insert(target, root);
            } else if matches!(target, InventoryTarget::LeasedVirtual { session, .. } if session != 0) {
                return Err(InventoryApplyError::Rejected);
            }
            if !state.snapshot.has_complete_persistent_cells() {
                return Err(InventoryApplyError::Rejected);
            }
            bind_imported_player_lease(
                target,
                state.player_lease,
                &mut player_keys,
                &mut historical_player_keys,
                &mut historical_player_leases,
                &mut player_incarnations,
            )?;
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
            if let Some(current) = ground_ledger.snapshot(target) {
                let current = self
                    .complete_owned_snapshot(current)
                    .ok_or(InventoryApplyError::TargetUnavailable)?;
                let mut incoming_cells = state.snapshot.cells.clone();
                incoming_cells.sort_by_key(|cell| (cell.loc.inv, cell.loc.slot));
                let mut current_cells = current.cells;
                current_cells.sort_by_key(|cell| (cell.loc.inv, cell.loc.slot));
                if current_cells != incoming_cells
                    || ground_ledger.target_chunks(target) != Some(chunks.as_slice())
                    || ledger.snapshot(target).is_none()
                    || beacons.get(&target).copied() != state.beacon
                {
                    return Err(InventoryApplyError::TargetUnavailable);
                }
                continue;
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
            menu_roots,
            block_root_images: None,
            block_roots: self.canonical_block_roots.clone(),            chunk,
            states,
            ledger,
            ground_ledger,
            beacons,
            snapshots,
            player_keys,
            historical_player_keys,
            historical_player_leases,
            player_incarnations,
        })
    }

    pub(super) fn commit_prepared_from_chunk(
        &mut self,
        prepared: PreparedInventoryImport,
    ) -> Result<CommittedInventoryImport, InventoryApplyError> {
        let images = prepared.block_root_images;
        let prepared = self.prepare_from_chunk(prepared.chunk, prepared.states)?;
        let prepared = if let Some(images) = images { self.bind_imported_block_roots(prepared, images)? } else { prepared };
        let targets = prepared
            .snapshots
            .iter()
            .map(|snapshot| snapshot.target)
            .collect();
        self.ledger = prepared.ledger;
        self.ground_ledger = prepared.ground_ledger;
        self.canonical_block_roots = prepared.block_roots;
        self.canonical_menu_admissions = prepared.menu_roots;
        self.replacement_epoch = self
            .replacement_epoch
            .checked_add(1)
            .expect("inventory replacement epoch exhausted");
        self.beacons = prepared.beacons;
        self.player_keys = prepared.player_keys;
        self.historical_player_keys = prepared.historical_player_keys;
        self.historical_player_leases = prepared.historical_player_leases;
        self.player_incarnations = prepared.player_incarnations;
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
            LocalInventoryLifecycle::CanonicalOpenPresentation(open) => {
                if open.screen.session.actor != open.lease.gid || open.reference.id.actor != ActorId::Player(open.lease.gid)
                    || open.reference.id.incarnation != open.lease.incarnation { return None; }
                self.pending_canonical_opens.entry(open.reference).or_insert((open, false));
                None
            }
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
                        if slot.slot.target.is_screen_virtual() {
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
                key,
                generation,
                token,
                new_player: _,
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
                    if self
                        .phase_snapshot(
                            pumpkin_cluster::canonical_action::TruthView::Local,
                            InventoryTarget::Player(gid),
                        )
                        .is_some()
                        && self.player_install_tokens.get(&gid).copied() != Some(generation)
                    {
                        return Err(InventoryApplyError::Rejected);
                    }
                    if self
                        .historical_player_keys
                        .get(&gid)
                        .is_some_and(|known| *known != key)
                    {
                        return Err(InventoryApplyError::Rejected);
                    }
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
                    if self
                        .prepared_player_inventories
                        .get(&gid)
                        .is_some_and(|prepared| {
                            prepared.key != key || prepared.generation != generation
                        })
                    {
                        return Err(InventoryApplyError::Rejected);
                    }
                    self.prepared_player_inventories.insert(
                        gid,
                        PreparedPlayerInventoryAdmission {
                            key,
                            generation,
                            player,
                            crafting,
                            ender,
                            screen,
                            chunks,
                        },
                    );
                    self.player_keys
                        .retain(|stored, actor| *stored != key && *actor != gid);
                    self.player_keys.insert(key, gid);
                    self.historical_player_keys.insert(gid, key);
                    if let Some(incarnation) = self.player_incarnations.get(&gid) {
                        self.historical_player_leases
                            .insert((gid, *incarnation), key);
                    }
                    self.player_install_tokens.insert(gid, generation);
                    if let Some(token) = token {
                        self.latest_player_install_tokens.insert(gid, token);
                    }
                    Ok(revision)
                })();
                if let Some(token) = token {
                    server.cluster_inventory_handoff_installed.rcu(|published| {
                        let mut next = (**published).clone();
                        next.insert(
                            (gid, token),
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
                if matches!(target, InventoryTarget::Player(_)) {
                    return;
                }
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
            }
            LocalInventoryLifecycle::OpenScreen { lease, screen, source, menu, presentation } => {
                let Some(player) = super::cluster_invsee::find_inventory_player(server, screen.session.actor) else { return; };
                let source = match source {
                    Some(source) => source,
                    None => match screen.preview_lease {
                        Some(lease) if lease.gid != screen.session.actor => pumpkin_cluster::canonical_menu::MenuSource::RemotePlayer { lease },
                        Some(_) => pumpkin_cluster::canonical_menu::MenuSource::Virtual { kind: screen.window_kind },
                        None => return,
                    },
                };
                if super::cluster_world_owner::player_action_lease(server, &player) != Some(lease) { return; }
                self.pending_raw_menu_opens.push_back(Arc::new(super::cluster_invsee::InventoryOpenSource { lease, screen, source, menu, presentation }));
            }
            LocalInventoryLifecycle::CloseScreen { session } => {
                if let Some(screen) =
                    super::cluster_invsee::inventory_screen_snapshot(server, session)
                {
                    self.queue_invsee_screen(&screen, false);
                }
                super::cluster_invsee::unpublish_inventory_screen(server, session);
                if self.active_screens.get(&session.actor) == Some(&session) {
                    self.active_screens.remove(&session.actor);
                }
                self.screen_chunks.remove(&session);
                self.menu_metadata.remove(&session);
            }
            LocalInventoryLifecycle::BeginDeparture { .. } => unreachable!(),
            LocalInventoryLifecycle::CanonicalOpenPresentation(_) => unreachable!(),
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
                    incarnation: 0,
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
        let actor_chunks = self.local_target_chunks(InventoryTarget::Player(session.actor))?;
        let [actor_chunk] = actor_chunks.as_slice() else {
            return None;
        };
        let actor_chunk = *actor_chunk;
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

    pub(super) fn capture_screen_gesture(
        &mut self,
        staged: LocalInventoryScreenCommand,
    ) -> Option<LocalInventoryAction> {
        let session = match &staged.command {
            InventoryScreenCommand::BeginQuickCraft { session, .. }
            | InventoryScreenCommand::AddQuickCraftSlot { session, .. }
            | InventoryScreenCommand::FinishQuickCraft { session } => *session,
            InventoryScreenCommand::Close { .. } => return None,
        };
        self.screen_chunks
            .entry(session)
            .or_default()
            .extend(staged.affected.iter().copied());
        let gesture = self.screen_sessions.apply(staged.command)?;
        let target = InventoryTarget::Player(session.actor);
        let chunks = self.local_target_chunks(target)?;
        if chunks.len() != 1 {
            return None;
        }
        Some(LocalInventoryAction {
            open: None,
            lease: self.player_lease_for_gid(session.actor),
            intent: InventoryMutationIntent {
                actor: session.actor.into(),
                tick: staged.tick,
                payload: InventoryIntentPayload::Gestures(vec![gesture]),
                screen_session: Some(session),
                actor_chunk: chunks[0],
                affected: self
                    .screen_chunks
                    .get(&session)
                    .into_iter()
                    .flat_map(|chunks| chunks.iter().copied())
                    .collect(),
            },
            origin: staged.origin,
            input_sequence: staged.input_sequence,
        })
    }

    fn close_screen(
        &mut self,
        server: &Server,
        staged: LocalInventoryScreenCommand,
        session: InventoryScreenSession,
    ) -> Option<(TickBatch, TickFinish)> {
        let screen = super::cluster_invsee::inventory_screen_snapshot(server, session)?;
        let player_target =
            self.current_inventory_address(InventoryTarget::Player(session.actor))?;
        self.ledger.snapshot(player_target)?;
        let virtual_target = screen
            .slots
            .iter()
            .find(|slot| slot.slot.target.is_screen_virtual())
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
        let actor_chunks = self.local_target_chunks(player_target)?;
        let [actor_chunk] = actor_chunks.as_slice() else {
            return None;
        };
        let actor_chunk = *actor_chunk;
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
                self.ender_open_sessions.remove(&session);
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

pub(super) fn close_result_slot(window_kind: u16, slot: u16, player_screen: bool) -> bool {
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

#[cfg(test)]
mod lease_tests {
    use super::*;
    use pumpkin_cluster::canonical_action::{LivePlayerLease, PlayerKey};
    use pumpkin_cluster::identity::{PlayerSlot, ServerId};
    use pumpkin_cluster::protocol::WorldId;

    #[test]
    fn retained_closed_slot_certification_requires_exact_lease_cause_session_and_admitted_slot() {
        use pumpkin_cluster::canonical_action::{ActionId, ActionRef, InventoryId, Target};
        use pumpkin_cluster::inventory::InvLoc;
        use pumpkin_cluster::time::TickStamp;
        use pumpkin_cluster::canonical_inventory::InventorySlotBinding;
        use pumpkin_cluster::canonical_inventory_close::{InventoryCloseDrop, InventoryCloseIntent};
        use pumpkin_cluster::canonical_menu::{MenuClosedSession, MenuSessionState, MenuVirtualInventoryAdmission, MenuVirtualInventoryLayout};
        let gid = GlobalPlayerId::new(ServerId(2), PlayerSlot(7));
        let lease = LivePlayerLease { key: PlayerKey(uuid::Uuid::from_u128(12)), gid, incarnation: 41 };
        let chunk = ChunkKey::new(WorldId::new(0, 1), 0, 0);
        let reference = ActionRef { tick: TickStamp(100), id: ActionId { actor: ActorId::Player(gid), incarnation: 41, family: 7, sequence: 3 } };
        let cause = ActionRef { tick: TickStamp(99), id: ActionId { sequence: 2, ..reference.id } };
        let admission = MenuVirtualInventoryAdmission { lease, session: 7, layout: MenuVirtualInventoryLayout::Beacon };
        let (target, cell) = admission.entries().into_iter().next().unwrap();
        let binding = InventorySlotBinding { slot: InventorySlotRef::new(admission.inventory_target(), cell.loc), target, max_stack: 1 };
        let destinations = (0..36).map(|slot| InventorySlotBinding {
            slot: InventorySlotRef::new(InventoryTarget::LeasedPlayer { key: lease.key.0, player: gid, incarnation: lease.incarnation }, InvLoc::new(INV_MAIN, slot)),
            target: Target::Player { key: lease.key, lease: Some(lease), inventory: Some(InventoryId::Player { inv: INV_MAIN, slot }) }, max_stack: 64,
        }).collect();
        let owner = pumpkin_cluster::realm_completion::PeerGeneration { server: gid.server, generation: 1 };
        let identity = pumpkin_cluster::entity_origin::EntityOriginIdentity { id: pumpkin_cluster::protocol::GlobalEntityId { origin: gid.server, local_id: 31, generation: 1 }, uuid: uuid::Uuid::from_u128(31), source: owner };
        let payment = super::super::cluster_invsee::inventory_cell_from_item_stack(cell.loc, &pumpkin_data::item_stack::ItemStack::new(1, &pumpkin_data::item::Item::IRON_INGOT)).unwrap();
        let template = super::super::cluster_item_birth::reserved_item_body(reference, &identity, owner, pumpkin_cluster::entity_body::EntitySpatial {
            world: chunk.world, position: [0.5_f64.to_bits(), 64_f64.to_bits(), 0.5_f64.to_bits()], velocity_micro: [0; 3], yaw: 0, pitch: 0, on_ground: false,
        }, &payment, 40).unwrap();
        let close = InventoryCloseIntent { lease, session: InventoryScreenSession { actor: gid, session: 7 }, token: 11, departure: false,
            menu: Target::Chunk { key: chunk, part: pumpkin_cluster::canonical_action::ChunkPart::LivingProperty { actor: ActorId::Player(gid), property: pumpkin_cluster::living_property::LivingProperty::MenuSession }, inventory: None },
            window_kind: 9, sources: vec![binding.clone()], destinations, results: Vec::new(), overflow: vec![InventoryCloseDrop { source: binding.target,
                target: Target::Chunk { key: chunk, part: pumpkin_cluster::canonical_action::ChunkPart::Entity(identity.id), inventory: None }, template }] };
        assert!(close.valid(reference));
        let mut owner = InventoryOwner::detached();
        owner.canonical_menu_admissions.insert(admission.inventory_target(), CanonicalMenuInventoryAdmission { reference, admission });
        owner.canonical_closes.insert(reference, close);
        owner.canonical_death_closes.insert(reference, cause);
        let menu = MenuSessionState { last_close: Some(reference), last_closed_session: 7,
            closed_sessions: vec![MenuClosedSession { session: 7, token: 11, window_kind: 9, action: reference }], ..Default::default() };
        assert!(owner.certifies_retained_closed_slot(lease, reference, cause, &menu, &binding));
        let empty_snapshot = pumpkin_cluster::inventory::InventoryStateSnapshot { target: binding.slot.target, revision: 0, cells: Vec::new() };
        let completed = owner.complete_owned_snapshot(empty_snapshot.clone()).unwrap();
        assert_eq!(completed.cells, vec![pumpkin_cluster::inventory::InventoryCell::empty(binding.slot.loc)]);
        assert!(!owner.certifies_retained_closed_slot(LivePlayerLease { incarnation: 42, ..lease }, reference, cause, &menu, &binding));
        assert!(!owner.certifies_retained_closed_slot(lease, reference, ActionRef { tick: TickStamp(98), ..cause }, &menu, &binding));
        let mut wrong_session = menu.clone(); wrong_session.closed_sessions[0].session = 8;
        assert!(!owner.certifies_retained_closed_slot(lease, reference, cause, &wrong_session, &binding));
        let mut unknown = binding.clone(); unknown.slot.loc.slot = 1;
        let Target::Player { inventory: Some(InventoryId::Virtual { slot, .. }), .. } = &mut unknown.target else { panic!() }; *slot = 1;
        assert!(!owner.certifies_retained_closed_slot(lease, reference, cause, &menu, &unknown));
        owner.canonical_menu_admissions.clear();
        assert!(!owner.certifies_retained_closed_slot(lease, reference, cause, &menu, &binding));
        assert!(owner.complete_owned_snapshot(empty_snapshot).is_none());
    }

    #[test]
    fn transition_keeps_virtual_inventory_with_its_owner() {
        let gid = GlobalPlayerId::new(ServerId(2), PlayerSlot(7));
        let target = InventoryTarget::Virtual {
            host: ServerId(2),
            owner: ActorId::Player(gid),
            session: 9,
            kind: 1,
        };
        let world = WorldId::new(0, 1);
        let old = ChunkKey::new(world, 0, 0);
        let next = ChunkKey::new(world, 1, 0);
        let block = ChunkKey::new(world, 5, 0);
        let mut ledger = InvLedger::new();
        ledger
            .install_owned_snapshot(
                pumpkin_cluster::inventory::InventoryStateSnapshot {
                    target,
                    revision: 4,
                    cells: Vec::new(),
                },
                vec![old, block],
            )
            .unwrap();
        let chunks = relocated_inventory_chunks(&ledger, target, old, next).unwrap();
        ledger.relocate_target(target, chunks).unwrap();
        assert_eq!(ledger.target_chunks(target), Some(&[next][..]));
    }

    #[test]
    fn imported_player_snapshot_cannot_replace_an_active_incarnation() {
        let gid = GlobalPlayerId::new(ServerId(2), PlayerSlot(7));
        let key = PlayerKey(uuid::Uuid::from_u128(12));
        let mut keys = HashMap::new();
        let mut historical_keys = HashMap::new();
        let mut historical_leases = HashMap::new();
        let mut incarnations = HashMap::new();
        let first = LivePlayerLease {
            key,
            gid,
            incarnation: 41,
        };
        bind_imported_player_lease(
            InventoryTarget::Player(gid),
            Some(first),
            &mut keys,
            &mut historical_keys,
            &mut historical_leases,
            &mut incarnations,
        )
        .expect("first snapshot");
        let stale = LivePlayerLease {
            incarnation: 40,
            ..first
        };
        assert_eq!(
            bind_imported_player_lease(
                InventoryTarget::Player(gid),
                Some(stale),
                &mut keys,
                &mut historical_keys,
                &mut historical_leases,
                &mut incarnations,
            ),
            Err(InventoryApplyError::Rejected)
        );
        assert_eq!(incarnations.get(&gid), Some(&41));
        let reused = LivePlayerLease {
            key: PlayerKey(uuid::Uuid::from_u128(13)),
            incarnation: 42,
            ..first
        };
        assert_eq!(
            bind_imported_player_lease(
                InventoryTarget::Player(gid),
                Some(reused),
                &mut keys,
                &mut historical_keys,
                &mut historical_leases,
                &mut incarnations,
            ),
            Err(InventoryApplyError::Rejected)
        );
        assert_eq!(keys.get(&key.0), Some(&gid));
        let virtual_target = InventoryTarget::Virtual {
            host: ServerId(2),
            owner: ActorId::Player(gid),
            session: 9,
            kind: 1,
        };
        assert_eq!(
            bind_imported_player_lease(
                virtual_target,
                Some(stale),
                &mut keys,
                &mut historical_keys,
                &mut historical_leases,
                &mut incarnations,
            ),
            Err(InventoryApplyError::Rejected)
        );
    }

    #[test]
    fn newer_login_keeps_the_older_inventory_lease_addressable() {
        let key = PlayerKey(uuid::Uuid::from_u128(12));
        let first_gid = GlobalPlayerId::new(ServerId(2), PlayerSlot(7));
        let next_gid = GlobalPlayerId::new(ServerId(2), PlayerSlot(8));
        let first = LivePlayerLease {
            key,
            gid: first_gid,
            incarnation: 41,
        };
        let next = LivePlayerLease {
            key,
            gid: next_gid,
            incarnation: 42,
        };
        let mut keys = HashMap::new();
        let mut historical_keys = HashMap::new();
        let mut historical_leases = HashMap::new();
        let mut incarnations = HashMap::new();
        bind_imported_player_lease(
            InventoryTarget::Player(first_gid),
            Some(first),
            &mut keys,
            &mut historical_keys,
            &mut historical_leases,
            &mut incarnations,
        )
        .unwrap();
        bind_imported_player_lease(
            InventoryTarget::Player(next_gid),
            Some(next),
            &mut keys,
            &mut historical_keys,
            &mut historical_leases,
            &mut incarnations,
        )
        .unwrap();
        assert_eq!(historical_keys.get(&first_gid), Some(&key.0));
        assert_eq!(historical_keys.get(&next_gid), Some(&key.0));
        assert_eq!(incarnations.get(&first_gid), Some(&41));
        assert_eq!(incarnations.get(&next_gid), Some(&42));
    }

    #[test]
    fn imported_leased_inventory_requires_exact_player_identity() {
        let gid = GlobalPlayerId::new(ServerId(2), PlayerSlot(7));
        let lease = LivePlayerLease {
            key: PlayerKey(uuid::Uuid::from_u128(12)),
            gid,
            incarnation: 41,
        };
        let mut keys = HashMap::new();
        let mut historical_keys = HashMap::new();
        let mut historical_leases = HashMap::new();
        let mut incarnations = HashMap::new();
        let target = InventoryTarget::LeasedPlayer {
            key: lease.key.0,
            player: gid,
            incarnation: lease.incarnation,
        };
        bind_imported_player_lease(
            target,
            Some(lease),
            &mut keys,
            &mut historical_keys,
            &mut historical_leases,
            &mut incarnations,
        )
        .unwrap();
        assert_eq!(
            historical_leases.get(&(gid, lease.incarnation)),
            Some(&lease.key.0)
        );
        assert!(!historical_keys.contains_key(&gid));
        assert!(!incarnations.contains_key(&gid));
        let next = LivePlayerLease {
            incarnation: 42,
            ..lease
        };
        bind_imported_player_lease(
            InventoryTarget::LeasedPlayer {
                key: next.key.0,
                player: gid,
                incarnation: next.incarnation,
            },
            Some(next),
            &mut keys,
            &mut historical_keys,
            &mut historical_leases,
            &mut incarnations,
        )
        .unwrap();
        assert_eq!(historical_leases.get(&(gid, 41)), Some(&lease.key.0));
        assert_eq!(historical_leases.get(&(gid, 42)), Some(&next.key.0));
        assert_eq!(
            bind_imported_player_lease(
                InventoryTarget::LeasedPlayer {
                    key: lease.key.0,
                    player: gid,
                    incarnation: 42,
                },
                Some(lease),
                &mut keys,
                &mut historical_keys,
                &mut historical_leases,
                &mut incarnations,
            ),
            Err(InventoryApplyError::Rejected)
        );
    }
}
