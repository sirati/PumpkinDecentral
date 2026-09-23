use std::collections::{HashMap, HashSet};
use std::sync::Arc;
use std::time::{Duration, Instant};

use pumpkin_cluster::identity::{GlobalPlayerId, ServerId};
use pumpkin_cluster::inventory::{
    INV_CURSOR, INV_MAIN, INV_SELECTED, InventoryApplyError, InventoryIntentPayload,
    InventoryMutationIntent, InventoryResolution, InventoryStateSnapshot, InventoryTarget,
};
use pumpkin_cluster::invsee::{
    INVSEE_MAX_SLOT_NBT_BYTES, INVSEE_REQUEST_TIMEOUT_MS, InventorySnapshot, InvseeControl,
    InvseeInvalidation, InvseeRequest, InvseeResponse, InvseeSubscription, InvseeWrite,
    decode_control, encode_control, inventory_snapshot_from_state, player_inventory_loc,
    request_parcel, response_parcel,
};
use pumpkin_cluster::streams::{InboundParcel, OutboundParcel, StreamHeader};
use pumpkin_cluster::time::TickStamp;
use pumpkin_cluster::world_delta::chunk_of_block;
use tokio::sync::{broadcast, mpsc, oneshot};

use pumpkin_data::item_stack::ItemStack;
use pumpkin_inventory::player::player_inventory::ClusterPlayerInventorySnapshot;
use pumpkin_inventory::{Inventory, screen_handler::ScreenHandler};
use pumpkin_nbt::Nbt;
use pumpkin_protocol::codec::item_stack_seralizer::ItemStackSerializer;
use pumpkin_protocol::java::client::play::{CSetCursorItem, CSetPlayerInventory};
use pumpkin_util::text::TextComponent;

use super::Server;
pub(crate) struct LocalInventoryAction {
    pub intent: InventoryMutationIntent,
    pub origin: Option<u64>,
    pub input_sequence: Option<pumpkin_cluster::identity::PlayerSeq>,
}

#[derive(Debug, Clone)]
pub(crate) struct InventoryPublication {
    pub resolution: InventoryResolution,
    pub origin: Option<u64>,
}

pub(crate) type InventoryActionRoute = Option<mpsc::Sender<LocalInventoryAction>>;

pub(crate) struct LocalInventoryScreenCommand {
    pub command: pumpkin_cluster::inventory::InventoryScreenCommand,
    pub tick: TickStamp,
    pub affected: Vec<pumpkin_cluster::protocol::ChunkKey>,
    pub origin: Option<u64>,
    pub input_sequence: Option<pumpkin_cluster::identity::PlayerSeq>,
    pub close_token: Option<u64>,
}

pub(crate) type InventoryScreenRoute = Option<mpsc::Sender<LocalInventoryScreenCommand>>;

pub(crate) struct LocalInventorySnapshotRequest {
    pub target: InventoryTarget,
    pub reply: oneshot::Sender<Option<InventoryStateSnapshot>>,
}

pub(crate) type InventorySnapshotRoute = Option<mpsc::Sender<LocalInventorySnapshotRequest>>;

pub(crate) struct ClusterScreenPresentation {
    pub title: TextComponent,
    pub window_type: pumpkin_data::screen::WindowType,
    pub stacks: Vec<ItemStack>,
}

pub(crate) enum BlockMenuKind {
    GenericStorage { rows: u8 },
    Hopper,
}

pub(crate) struct BlockMenuLeg {
    pub target: InventoryTarget,
    pub chunk: pumpkin_cluster::protocol::ChunkKey,
    pub block_state: pumpkin_data::BlockStateId,
    pub block_revision: u64,
    pub inventory: InventoryStateSnapshot,
}

pub(crate) struct BlockMenuDescriptor {
    pub first: BlockMenuLeg,
    pub second: Option<BlockMenuLeg>,
    pub kind: BlockMenuKind,
    pub title: TextComponent,
}

pub(crate) fn block_menu_screen(
    session: pumpkin_cluster::inventory::InventoryScreenSession,
    sync_id: u8,
    viewer_chunk: pumpkin_cluster::protocol::ChunkKey,
    viewer: &InventoryStateSnapshot,
    descriptor: BlockMenuDescriptor,
) -> Result<
    (
        pumpkin_cluster::inventory::InventoryScreenSnapshot,
        ClusterScreenPresentation,
    ),
    InventoryApplyError,
> {
    use pumpkin_cluster::inventory::{
        INV_CURSOR, INV_MAIN, InvLoc, InventoryCell, InventoryScreenSlot, InventoryScreenSnapshot,
        InventorySlotRef,
    };

    let valid_leg = |leg: &BlockMenuLeg| {
        matches!(leg.target, InventoryTarget::Block { .. })
            && leg.target.intrinsic_chunk() == Some(leg.chunk)
            && leg.inventory.target == leg.target
    };
    if !valid_leg(&descriptor.first)
        || descriptor.second.as_ref().is_some_and(|second| {
            !valid_leg(second) || second.target == descriptor.first.target
        })
        || viewer.target != InventoryTarget::Player(session.actor)
    {
        return Err(InventoryApplyError::Rejected);
    }
    let (container_slots, window_type) = match descriptor.kind {
        BlockMenuKind::GenericStorage { rows: 1 } => {
            (9, pumpkin_data::screen::WindowType::Generic9x1)
        }
        BlockMenuKind::GenericStorage { rows: 2 } => {
            (18, pumpkin_data::screen::WindowType::Generic9x2)
        }
        BlockMenuKind::GenericStorage { rows: 3 } => {
            (27, pumpkin_data::screen::WindowType::Generic9x3)
        }
        BlockMenuKind::GenericStorage { rows: 4 } => {
            (36, pumpkin_data::screen::WindowType::Generic9x4)
        }
        BlockMenuKind::GenericStorage { rows: 5 } => {
            (45, pumpkin_data::screen::WindowType::Generic9x5)
        }
        BlockMenuKind::GenericStorage { rows: 6 } => {
            (54, pumpkin_data::screen::WindowType::Generic9x6)
        }
        BlockMenuKind::Hopper => (5, pumpkin_data::screen::WindowType::Hopper),
        _ => return Err(InventoryApplyError::Rejected),
    };
    if (container_slots == 54) != descriptor.second.is_some() {
        return Err(InventoryApplyError::Rejected);
    }
    validate_imported_inventory_snapshots(std::slice::from_ref(viewer))?;
    for leg in std::iter::once(&descriptor.first).chain(descriptor.second.iter()) {
        validate_imported_inventory_snapshots(std::slice::from_ref(&leg.inventory))?;
        let mut occupied = HashSet::new();
        let size = if descriptor.second.is_some() {
            27
        } else {
            container_slots
        };
        if leg.inventory.cells.iter().any(|cell| {
            cell.loc.inv != INV_MAIN
                || cell.loc.slot >= size as u16
                || !occupied.insert(cell.loc)
        }) {
            return Err(InventoryApplyError::Rejected);
        }
    }
    let mut slots = Vec::with_capacity(container_slots + 36);
    let mut stacks = Vec::with_capacity(container_slots + 36);
    for screen_slot in 0..container_slots + 36 {
        let (target, snapshot, loc) = if screen_slot < container_slots {
            let (leg, local_slot) = if screen_slot >= 27 {
                if let Some(second) = descriptor.second.as_ref() {
                    (second, screen_slot - 27)
                } else {
                    (&descriptor.first, screen_slot)
                }
            } else {
                (&descriptor.first, screen_slot)
            };
            (
                leg.target,
                &leg.inventory,
                InvLoc::new(INV_MAIN, local_slot as u16),
            )
        } else {
            let index = screen_slot - container_slots;
            let player_slot = if index < 27 { index + 9 } else { index - 27 };
            (
                viewer.target,
                viewer,
                InvLoc::new(INV_MAIN, player_slot as u16),
            )
        };
        let cell = snapshot
            .cells
            .iter()
            .find(|cell| cell.loc == loc)
            .cloned()
            .unwrap_or_else(|| InventoryCell::empty(loc));
        stacks.push(item_stack_from_inventory_cell(&cell)?);
        slots.push(InventoryScreenSlot {
            screen_slot: screen_slot as u16,
            slot: InventorySlotRef::new(target, loc),
            cell,
            limit: 64,
        });
    }
    let cursor_loc = InvLoc::new(INV_CURSOR, 0);
    let cursor = viewer
        .cells
        .iter()
        .find(|cell| cell.loc == cursor_loc)
        .cloned()
        .unwrap_or_else(|| InventoryCell::empty(cursor_loc));
    let mut affected = vec![viewer_chunk, descriptor.first.chunk];
    affected.extend(descriptor.second.as_ref().map(|second| second.chunk));
    affected.sort_unstable();
    affected.dedup();
    Ok((
        InventoryScreenSnapshot {
            session,
            sync_id,
            window_kind: window_type as u16,
            container_slots: container_slots as u16,
            slots,
            cursor,
            affected,
            preview: None,
        },
        ClusterScreenPresentation {
            title: descriptor.title,
            window_type,
            stacks,
        },
    ))
}

pub(crate) fn open_cluster_block_menu(
    player: &crate::entity::player::Player,
    descriptor: BlockMenuDescriptor,
) -> Result<u8, InventoryApplyError> {
    use pumpkin_cluster::inventory::InventoryScreenSession;

    if player
        .cluster_departing
        .load(std::sync::atomic::Ordering::Acquire)
        || player.client.closed()
    {
        return Err(InventoryApplyError::TargetUnavailable);
    }
    let server = player
        .world()
        .server
        .upgrade()
        .ok_or(InventoryApplyError::TargetUnavailable)?;
    let actor = player
        .cluster_gid()
        .ok_or(InventoryApplyError::TargetUnavailable)?;
    let viewer = inventory_snapshot(&server, InventoryTarget::Player(actor))
        .ok_or(InventoryApplyError::TargetUnavailable)?;
    let position = player.position();
    let viewer_chunk = chunk_of_block(
        super::cluster_world_delta::world_id(&player.world()),
        position.x.floor() as i32,
        position.z.floor() as i32,
    );
    player.increment_screen_handler_sync_id();
    let sync_id = player
        .screen_handler_sync_id
        .load(std::sync::atomic::Ordering::Relaxed);
    let session_token = server
        .cluster_inventory_close_token
        .fetch_add(1, std::sync::atomic::Ordering::Relaxed);
    let session = InventoryScreenSession {
        actor,
        session: session_token,
    };
    let target = descriptor.first.target;
    let (screen, presentation) =
        block_menu_screen(session, sync_id, viewer_chunk, &viewer, descriptor)?;
    player
        .cluster_current_inventory_session
        .compare_exchange(
            0,
            session_token,
            std::sync::atomic::Ordering::AcqRel,
            std::sync::atomic::Ordering::Acquire,
        )
        .map_err(|_| InventoryApplyError::Rejected)?;
    player
        .cluster_inventory_screen_target
        .store(Some(Arc::new(target)));
    if player
        .cluster_departing
        .load(std::sync::atomic::Ordering::Acquire)
        || player.client.closed()
    {
        player.cluster_inventory_screen_target.store(None);
        let _ = player.cluster_current_inventory_session.compare_exchange(
            session_token,
            0,
            std::sync::atomic::Ordering::AcqRel,
            std::sync::atomic::Ordering::Acquire,
        );
        return Err(InventoryApplyError::TargetUnavailable);
    }
    let submitted = open_inventory_screen_with_presentation(
        &server,
        screen,
        None,
        Some(presentation),
    );
    if submitted.is_err() {
        player.cluster_inventory_screen_target.store(None);
        let _ = player.cluster_current_inventory_session.compare_exchange(
            session_token,
            0,
            std::sync::atomic::Ordering::AcqRel,
            std::sync::atomic::Ordering::Acquire,
        );
    }
    submitted?;
    Ok(sync_id)
}

pub(crate) enum LocalInventoryLifecycle {
    InstallPlayer {
        gid: GlobalPlayerId,
        token: Option<u64>,
        player: InventoryStateSnapshot,
        crafting: InventoryStateSnapshot,
        ender: InventoryStateSnapshot,
        screen: pumpkin_cluster::inventory::InventoryScreenSnapshot,
        chunks: Vec<pumpkin_cluster::protocol::ChunkKey>,
    },
    Install {
        snapshot: InventoryStateSnapshot,
        chunks: Vec<pumpkin_cluster::protocol::ChunkKey>,
        screen: Option<pumpkin_cluster::inventory::InventoryScreenSnapshot>,
        menu: Option<pumpkin_cluster::inventory::InventoryMenuMetadata>,
    },
    Relocate {
        target: InventoryTarget,
        chunks: Vec<pumpkin_cluster::protocol::ChunkKey>,
    },
    Remove {
        target: InventoryTarget,
    },
    OpenScreen {
        screen: pumpkin_cluster::inventory::InventoryScreenSnapshot,
        menu: Option<pumpkin_cluster::inventory::InventoryMenuMetadata>,
        presentation: Option<ClusterScreenPresentation>,
    },
    CloseScreen {
        session: pumpkin_cluster::inventory::InventoryScreenSession,
    },
    BeginDeparture {
        actor: GlobalPlayerId,
        token: u64,
        input_sequence: pumpkin_cluster::identity::PlayerSeq,
    },
    RefreshRemotePreview {
        session: pumpkin_cluster::inventory::InventoryScreenSession,
        target: GlobalPlayerId,
        expected_revision: u64,
        snapshot: InventorySnapshot,
    },
    BeaconSnapshot {
        target: InventoryTarget,
        state: pumpkin_cluster::inventory::BeaconMenuState,
        handle: Arc<dyn Inventory>,
    },
}

pub(crate) type InventoryInstallRoute = Option<mpsc::Sender<LocalInventoryLifecycle>>;

pub(crate) enum InvseeCommand {
    Configure {
        local: ServerId,
        outbound: mpsc::Sender<OutboundParcel>,
    },
    Snapshot {
        viewer: GlobalPlayerId,
        target: Option<GlobalPlayerId>,
        target_name: String,
        editable: bool,
        reply: oneshot::Sender<SnapshotPending>,
    },
    Cancel(u64),
    Refresh {
        session: pumpkin_cluster::inventory::InventoryScreenSession,
        target: GlobalPlayerId,
        expected_revision: u64,
    },
    Subscribe(InvseeSubscription),
    Unsubscribe(InvseeSubscription),
    TargetChanged(GlobalPlayerId, u64),
    TargetRemoved(GlobalPlayerId),
    TargetRelocated(GlobalPlayerId),
    RefreshApplied {
        session: pumpkin_cluster::inventory::InventoryScreenSession,
        target: GlobalPlayerId,
        revision: u64,
    },
    RefreshRejected {
        session: pumpkin_cluster::inventory::InventoryScreenSession,
        target: GlobalPlayerId,
        expected_revision: u64,
        retry: bool,
    },
    Write(InvseeWrite),
    SnapshotResponse(InvseeResponse),
    Inbound(InboundParcel),
}

pub(crate) struct SnapshotPending {
    request_id: u64,
    response: oneshot::Receiver<Option<InventorySnapshot>>,
}

struct InvseeOwner {
    server: Arc<Server>,
    local: ServerId,
    outbound: Option<mpsc::Sender<OutboundParcel>>,
    next_request: u64,
    pending: HashMap<u64, PendingSnapshot>,
    pending_refresh: HashMap<pumpkin_cluster::inventory::InventoryScreenSession, u64>,
    deferred_refresh: HashMap<
        pumpkin_cluster::inventory::InventoryScreenSession,
        (GlobalPlayerId, u64, InventorySnapshot, Instant),
    >,
    viewer_subscriptions: HashSet<InvseeSubscription>,
    forced_refresh: HashSet<pumpkin_cluster::inventory::InventoryScreenSession>,
    owner_refresh_inflight:
        HashMap<pumpkin_cluster::inventory::InventoryScreenSession, (GlobalPlayerId, u64, Instant)>,
    target_subscriptions: HashMap<InvseeSubscription, (Instant, Option<u64>)>,
}

enum PendingSnapshot {
    Request(oneshot::Sender<Option<InventorySnapshot>>),
    Refresh {
        session: pumpkin_cluster::inventory::InventoryScreenSession,
        target: GlobalPlayerId,
        expected_revision: u64,
        sent_at: Instant,
    },
}

impl InvseeOwner {
    fn new(server: Arc<Server>) -> Self {
        Self {
            server,
            local: ServerId(0),
            outbound: None,
            next_request: 0,
            pending: HashMap::new(),
            pending_refresh: HashMap::new(),
            deferred_refresh: HashMap::new(),
            viewer_subscriptions: HashSet::new(),
            forced_refresh: HashSet::new(),
            owner_refresh_inflight: HashMap::new(),
            target_subscriptions: HashMap::new(),
        }
    }

    fn send(&self, peer: ServerId, bytes: Vec<u8>) -> bool {
        let Some(outbound) = &self.outbound else {
            return false;
        };
        outbound
            .try_send(OutboundParcel {
                peer,
                header: StreamHeader::new(pumpkin_cluster::protocol::StreamKind::Control, None),
                bytes,
            })
            .is_ok()
    }

    fn find_player(&self, gid: GlobalPlayerId) -> Option<Arc<crate::entity::player::Player>> {
        for world in self.server.worlds.load().iter() {
            for player in world.players.load().iter() {
                if player.cluster_gid() == Some(gid) {
                    return Some(player.clone());
                }
            }
        }
        None
    }

    fn find_local_target(
        &self,
        target: GlobalPlayerId,
        target_name: &str,
    ) -> Option<Arc<crate::entity::player::Player>> {
        self.find_player(target).or_else(|| {
            (!target_name.is_empty())
                .then(|| self.server.get_player_by_name(target_name))
                .flatten()
        })
    }

    fn send_response(&self, response: InvseeResponse) {
        if let Ok(parcel) = response_parcel(&response) {
            let _ = self.send(ServerId(parcel.peer), parcel.bytes);
        }
    }

    fn send_control(&self, peer: ServerId, message: &InvseeControl) -> bool {
        encode_control(message).is_ok_and(|bytes| self.send(peer, bytes))
    }

    fn screen_subscription_active(&self, subscription: InvseeSubscription) -> bool {
        let session = pumpkin_cluster::inventory::InventoryScreenSession {
            actor: subscription.viewer,
            session: subscription.session,
        };
        inventory_screen_snapshot(&self.server, session).is_some_and(|screen| {
            screen.preview.as_ref().is_some_and(|preview| {
                preview.target == InventoryTarget::Player(subscription.target)
            })
        })
    }

    fn begin_subscription(&mut self, subscription: InvseeSubscription) {
        if subscription.viewer.server != self.local
            || !self.screen_subscription_active(subscription)
        {
            return;
        }
        self.viewer_subscriptions.insert(subscription);
        let _ = self.send_control(
            subscription.target.server,
            &InvseeControl::Subscribe(subscription),
        );
    }

    fn end_subscription(&mut self, subscription: InvseeSubscription) {
        let session = pumpkin_cluster::inventory::InventoryScreenSession {
            actor: subscription.viewer,
            session: subscription.session,
        };
        self.deferred_refresh.remove(&session);
        self.forced_refresh.remove(&session);
        self.owner_refresh_inflight.remove(&session);
        if let Some(request_id) = self.pending_refresh.remove(&session) {
            self.pending.remove(&request_id);
        }
        if self.viewer_subscriptions.remove(&subscription) {
            let _ = self.send_control(
                subscription.target.server,
                &InvseeControl::Unsubscribe(subscription),
            );
        }
    }

    fn renew_subscriptions(&mut self) {
        let subscriptions: Vec<_> = self.viewer_subscriptions.iter().copied().collect();
        for subscription in subscriptions {
            if self.screen_subscription_active(subscription) {
                let _ = self.send_control(
                    subscription.target.server,
                    &InvseeControl::Subscribe(subscription),
                );
            } else {
                self.end_subscription(subscription);
            }
        }
        self.target_subscriptions
            .retain(|_, (expiry, _)| *expiry > Instant::now());
    }

    fn accept_subscription(&mut self, subscription: InvseeSubscription) {
        if subscription.target.server != self.local {
            return;
        }
        self.target_subscriptions
            .retain(|_, (expiry, _)| *expiry > Instant::now());
        if !self.target_subscriptions.contains_key(&subscription)
            && self.target_subscriptions.len() >= 1024
        {
            return;
        }
        let revision =
            inventory_snapshot(&self.server, InventoryTarget::Player(subscription.target))
                .map(|snapshot| snapshot.revision);
        let first = !self.target_subscriptions.contains_key(&subscription);
        let message = InvseeControl::Invalidated(InvseeInvalidation {
            subscription,
            revision,
            force_refresh: first,
        });
        if self.send_control(subscription.viewer.server, &message) {
            self.target_subscriptions.insert(
                subscription,
                (Instant::now() + Duration::from_secs(120), revision),
            );
        }
    }

    fn invalidate_subscribers(
        &mut self,
        target: GlobalPlayerId,
        revision: Option<u64>,
        force_refresh: bool,
    ) {
        self.target_subscriptions
            .retain(|_, (expiry, _)| *expiry > Instant::now());
        let subscriptions: Vec<_> = self
            .target_subscriptions
            .iter()
            .filter_map(|(subscription, (_, last_revision))| {
                (subscription.target == target && (force_refresh || *last_revision != revision))
                    .then_some(*subscription)
            })
            .collect();
        for subscription in subscriptions {
            let message = InvseeControl::Invalidated(InvseeInvalidation {
                subscription,
                revision,
                force_refresh,
            });
            if self.send_control(subscription.viewer.server, &message) {
                if let Some((_, last_revision)) = self.target_subscriptions.get_mut(&subscription) {
                    *last_revision = revision;
                }
            }
        }
    }

    fn receive_invalidation(&mut self, invalidation: InvseeInvalidation) {
        let subscription = invalidation.subscription;
        if !self.viewer_subscriptions.contains(&subscription)
            || !self.screen_subscription_active(subscription)
        {
            return;
        }
        let session = pumpkin_cluster::inventory::InventoryScreenSession {
            actor: subscription.viewer,
            session: subscription.session,
        };
        if self.pending_refresh.contains_key(&session)
            || self.deferred_refresh.contains_key(&session)
            || self.owner_refresh_inflight.contains_key(&session)
        {
            self.forced_refresh.insert(session);
        }
        let Some(screen) = inventory_screen_snapshot(&self.server, session) else {
            return;
        };
        let Some(preview) = screen.preview else {
            return;
        };
        if invalidation.force_refresh || Some(preview.revision) != invalidation.revision {
            self.begin_refresh(session, subscription.target, preview.revision);
        }
    }

    fn refresh_applied(
        &mut self,
        session: pumpkin_cluster::inventory::InventoryScreenSession,
        target: GlobalPlayerId,
        revision: u64,
    ) {
        if self
            .owner_refresh_inflight
            .get(&session)
            .is_none_or(|(pending_target, _, _)| *pending_target != target)
        {
            return;
        }
        self.owner_refresh_inflight.remove(&session);
        if !self.viewer_subscriptions.contains(&InvseeSubscription {
            viewer: session.actor,
            session: session.session,
            target,
        }) {
            return;
        }
        if self.forced_refresh.remove(&session) {
            self.begin_refresh(session, target, revision);
        }
    }

    fn refresh_rejected(
        &mut self,
        session: pumpkin_cluster::inventory::InventoryScreenSession,
        target: GlobalPlayerId,
        expected_revision: u64,
        retry: bool,
    ) {
        if self.owner_refresh_inflight.get(&session).is_none_or(
            |(pending_target, pending_revision, _)| {
                *pending_target != target || *pending_revision != expected_revision
            },
        ) {
            return;
        }
        self.owner_refresh_inflight.remove(&session);
        if !self.screen_subscription_active(InvseeSubscription {
            viewer: session.actor,
            session: session.session,
            target,
        }) {
            return;
        }
        if !retry {
            if let Some(player) = self.find_player(session.actor) {
                player.client.try_kick(
                    crate::net::DisconnectReason::Unknown,
                    &TextComponent::text("Remote inventory update was rejected"),
                );
            }
            return;
        }
        let Some(screen) = inventory_screen_snapshot(&self.server, session) else {
            return;
        };
        let Some(preview) = screen.preview else {
            return;
        };
        if preview.target == InventoryTarget::Player(target) {
            self.begin_refresh(session, target, preview.revision);
        }
    }

    fn fail_refresh(
        &self,
        session: pumpkin_cluster::inventory::InventoryScreenSession,
        target: GlobalPlayerId,
    ) {
        if self.screen_subscription_active(InvseeSubscription {
            viewer: session.actor,
            session: session.session,
            target,
        }) && let Some(player) = self.find_player(session.actor)
        {
            player.client.try_kick(
                crate::net::DisconnectReason::Unknown,
                &TextComponent::text("Remote inventory update unavailable"),
            );
        }
    }

    fn answer(&self, request: InvseeRequest) {
        let Some(target) = self.find_local_target(request.target, &request.target_name) else {
            self.send_response(InvseeResponse::offline(&request));
            return;
        };
        if target.is_in_cluster_lobby() {
            self.send_response(InvseeResponse::offline(&request));
            return;
        }
        let (reply, response) = oneshot::channel();
        let snapshot_route = self
            .server
            .cluster_inventory_snapshot_route_tx
            .borrow()
            .clone();
        if snapshot_route
            .and_then(|route| {
                route
                    .try_send(LocalInventorySnapshotRequest {
                        target: InventoryTarget::Player(request.target),
                        reply,
                    })
                    .ok()
            })
            .is_none()
        {
            self.send_response(InvseeResponse::offline(&request));
            return;
        }
        let commands = self.server.cluster_invsee_tx.clone();
        let owner_name = target.gameprofile.name.clone();
        let position = target.position();
        let target_chunk = chunk_of_block(
            super::cluster_world_delta::world_id(&target.world()),
            position.x.floor() as i32,
            position.z.floor() as i32,
        );
        self.server.spawn_task(async move {
            let outcome =
                tokio::time::timeout(Duration::from_millis(INVSEE_REQUEST_TIMEOUT_MS), response)
                    .await;
            let response = match outcome {
                Ok(Ok(Some(state))) => {
                    let selected = state
                        .cells
                        .iter()
                        .find(|cell| cell.loc.inv == INV_SELECTED && cell.loc.slot == 0)
                        .and_then(|cell| cell.item.checked_sub(1))
                        .and_then(|slot| u8::try_from(slot).ok())
                        .filter(|slot| *slot < 9)
                        .unwrap_or_default();
                    InvseeResponse::found(
                        &request,
                        inventory_snapshot_from_state(state, owner_name, selected)
                            .with_affected(vec![target_chunk]),
                    )
                }
                _ => InvseeResponse::offline(&request),
            };
            let _ = commands.try_send(InvseeCommand::SnapshotResponse(response));
        });
    }

    fn resolve(&mut self, response: InvseeResponse) {
        let Some(pending) = self.pending.remove(&response.request_id) else {
            return;
        };
        match pending {
            PendingSnapshot::Request(reply) => {
                let _ = reply.send(response.snapshot().cloned());
            }
            PendingSnapshot::Refresh {
                session,
                target,
                expected_revision,
                ..
            } => {
                if self.pending_refresh.get(&session) != Some(&response.request_id) {
                    return;
                }
                self.pending_refresh.remove(&session);
                if response.viewer != session.actor || response.target != target {
                    self.fail_refresh(session, target);
                    return;
                }
                if let Some(snapshot) = response.snapshot() {
                    self.enqueue_refresh(session, target, expected_revision, snapshot.clone());
                } else {
                    self.fail_refresh(session, target);
                }
            }
        }
    }

    fn begin_refresh(
        &mut self,
        session: pumpkin_cluster::inventory::InventoryScreenSession,
        target: GlobalPlayerId,
        expected_revision: u64,
    ) {
        let Some(screen) = inventory_screen_snapshot(&self.server, session) else {
            return;
        };
        if screen.preview.as_ref().is_none_or(|preview| {
            preview.target != InventoryTarget::Player(target)
                || preview.revision != expected_revision
        }) {
            return;
        }
        if self
            .pending_refresh
            .get(&session)
            .is_some_and(|request_id| {
                matches!(
                    self.pending.get(request_id),
                    Some(PendingSnapshot::Refresh {
                        target: pending_target,
                        expected_revision: pending_revision,
                        ..
                    }) if *pending_target == target && *pending_revision == expected_revision
                )
            })
        {
            return;
        }
        if self.deferred_refresh.contains_key(&session) {
            return;
        }
        if self.owner_refresh_inflight.contains_key(&session) {
            return;
        }
        let request_id = self.next_request;
        self.next_request = self.next_request.wrapping_add(1);
        let request = InvseeRequest::new(request_id, session.actor, target, String::new(), false);
        let Ok(parcel) = request_parcel(&request) else {
            self.fail_refresh(session, target);
            return;
        };
        if !self.send(ServerId(parcel.peer), parcel.bytes) {
            self.fail_refresh(session, target);
            return;
        }
        if let Some(previous) = self.pending_refresh.insert(session, request_id) {
            self.pending.remove(&previous);
        }
        self.pending.insert(
            request_id,
            PendingSnapshot::Refresh {
                session,
                target,
                expected_revision,
                sent_at: Instant::now(),
            },
        );
    }

    fn enqueue_refresh(
        &mut self,
        session: pumpkin_cluster::inventory::InventoryScreenSession,
        target: GlobalPlayerId,
        expected_revision: u64,
        snapshot: InventorySnapshot,
    ) {
        if snapshot.target != target || snapshot.affected.len() != 1 {
            self.fail_refresh(session, target);
            return;
        }
        if !self.deferred_refresh.contains_key(&session) && self.deferred_refresh.len() >= 1024 {
            if let Some(player) = self.find_player(session.actor) {
                player.client.try_kick(
                    crate::net::DisconnectReason::Unknown,
                    &TextComponent::text("Remote inventory update unavailable"),
                );
            }
            return;
        }
        self.deferred_refresh.insert(
            session,
            (target, expected_revision, snapshot, Instant::now()),
        );
        self.flush_deferred_refresh();
    }

    fn flush_deferred_refresh(&mut self) {
        let expired_requests: Vec<_> = self
            .pending_refresh
            .iter()
            .filter_map(|(session, request_id)| {
                matches!(
                    self.pending.get(request_id),
                    Some(PendingSnapshot::Refresh { sent_at, .. })
                        if sent_at.elapsed() > Duration::from_millis(INVSEE_REQUEST_TIMEOUT_MS)
                )
                .then_some((*session, *request_id))
            })
            .collect();
        for (session, request_id) in expired_requests {
            self.pending_refresh.remove(&session);
            if let Some(PendingSnapshot::Refresh { target, .. }) = self.pending.remove(&request_id)
            {
                self.fail_refresh(session, target);
            }
        }
        let expired: Vec<_> = self
            .owner_refresh_inflight
            .iter()
            .filter_map(|(session, (_, _, queued_at))| {
                (queued_at.elapsed() > Duration::from_secs(5)).then_some(*session)
            })
            .collect();
        for session in expired {
            self.owner_refresh_inflight.remove(&session);
            if let Some(player) = self.find_player(session.actor) {
                player.client.try_kick(
                    crate::net::DisconnectReason::Unknown,
                    &TextComponent::text("Remote inventory update timed out"),
                );
            }
        }
        let route = self
            .server
            .cluster_inventory_install_route_tx
            .borrow()
            .clone();
        let sessions: Vec<_> = self.deferred_refresh.keys().copied().collect();
        for session in sessions {
            let Some((target, expected_revision, snapshot, queued_at)) =
                self.deferred_refresh.get(&session)
            else {
                continue;
            };
            if queued_at.elapsed() > Duration::from_secs(5) {
                if let Some(player) = self.find_player(session.actor) {
                    player.client.try_kick(
                        crate::net::DisconnectReason::Unknown,
                        &TextComponent::text("Remote inventory update timed out"),
                    );
                }
                self.deferred_refresh.remove(&session);
                continue;
            }
            if !self.screen_subscription_active(InvseeSubscription {
                viewer: session.actor,
                session: session.session,
                target: *target,
            }) {
                self.deferred_refresh.remove(&session);
                continue;
            }
            let pending_target = *target;
            let pending_revision = *expected_revision;
            let command = LocalInventoryLifecycle::RefreshRemotePreview {
                session,
                target: pending_target,
                expected_revision: pending_revision,
                snapshot: snapshot.clone(),
            };
            if route
                .as_ref()
                .is_some_and(|route| route.try_send(command).is_ok())
            {
                self.deferred_refresh.remove(&session);
                self.owner_refresh_inflight
                    .insert(session, (pending_target, pending_revision, Instant::now()));
            }
        }
    }

    fn inbound(&mut self, parcel: InboundParcel) {
        let Ok(message) = decode_control(&parcel.bytes) else {
            return;
        };
        match message {
            InvseeControl::Request(request) if request.target.server == self.local => {
                self.answer(request);
            }
            InvseeControl::Response(response) if response.viewer.server == self.local => {
                self.resolve(response);
            }
            InvseeControl::Write(write) if write.target.server == self.local => {
                self.submit_local(write);
            }
            InvseeControl::Subscribe(subscription) if subscription.viewer.server == parcel.peer => {
                self.accept_subscription(subscription);
            }
            InvseeControl::Unsubscribe(subscription)
                if subscription.viewer.server == parcel.peer
                    && subscription.target.server == self.local =>
            {
                self.target_subscriptions.remove(&subscription);
            }
            InvseeControl::Invalidated(invalidation)
                if invalidation.subscription.target.server == parcel.peer
                    && invalidation.subscription.viewer.server == self.local =>
            {
                self.receive_invalidation(invalidation);
            }
            _ => {}
        }
    }

    fn begin_snapshot(
        &mut self,
        viewer: GlobalPlayerId,
        requested_target: Option<GlobalPlayerId>,
        target_name: String,
        editable: bool,
        reply: oneshot::Sender<SnapshotPending>,
    ) {
        let request_id = self.next_request;
        self.next_request = self.next_request.wrapping_add(1);
        let (response, receiver) = oneshot::channel();
        let pending = SnapshotPending {
            request_id,
            response: receiver,
        };
        let target = requested_target
            .or_else(|| {
                super::cluster_chat_pm::directory_snapshot(&self.server)
                    .locate_by_name(&target_name)
            })
            .filter(|target| target.server != self.local);
        let Some(target) = target else {
            let _ = response.send(None);
            let _ = reply.send(pending);
            return;
        };
        let request = InvseeRequest::new(request_id, viewer, target, target_name, editable);
        let Ok(parcel) = request_parcel(&request) else {
            let _ = response.send(None);
            let _ = reply.send(pending);
            return;
        };
        if self.send(ServerId(parcel.peer), parcel.bytes) {
            self.pending
                .insert(request_id, PendingSnapshot::Request(response));
        } else {
            let _ = response.send(None);
        }
        let _ = reply.send(pending);
    }

    fn submit_write(&mut self, mut write: InvseeWrite) {
        let request_id = self.next_request;
        self.next_request = self.next_request.wrapping_add(1);
        write.request_id = request_id;
        self.submit_local(write);
    }

    fn submit_local(&mut self, write: InvseeWrite) {
        let affected = write.affected.clone();
        if affected.is_empty() {
            return;
        }
        let Ok(mutation) = write.inventory_mutation() else {
            return;
        };
        let _ = submit_inventory_mutation(
            &self.server,
            write.viewer,
            TickStamp::now(),
            mutation,
            affected,
            Some(write.request_id),
        );
    }

    fn configure(&mut self, local: ServerId, outbound: mpsc::Sender<OutboundParcel>) {
        self.local = local;
        self.outbound = Some(outbound);
    }

    fn resolution(&mut self, publication: InventoryPublication) {
        super::cluster_inventory_protocol::publish_inventory_origin(&self.server, &publication);
        let resolution = publication.resolution;
        for snapshot in &resolution.authoritative {
            if let InventoryTarget::Player(target) = snapshot.target
                && target.server == self.local
            {
                self.invalidate_subscribers(target, Some(snapshot.revision), false);
            }
        }
        if resolution.result.is_ok() {
            return;
        }
        let pumpkin_cluster::actor::ActorId::Player(actor) = resolution.action.actor else {
            return;
        };
        if let Some(player) = self.find_player(actor) {
            player.send_system_message(&TextComponent::text(
                "Inventory changed before the edit was accepted.",
            ));
        }
    }
}

pub(crate) fn submit_inventory_mutation(
    server: &Arc<Server>,
    actor: GlobalPlayerId,
    tick: TickStamp,
    mutation: pumpkin_cluster::inventory::InventoryMutation,
    affected: Vec<pumpkin_cluster::protocol::ChunkKey>,
    origin: Option<u64>,
) -> Result<(), InventoryApplyError> {
    submit_inventory_mutation_with_sequence(server, actor, tick, mutation, affected, origin, None)
}

pub(crate) fn submit_inventory_mutation_with_sequence(
    server: &Arc<Server>,
    actor: GlobalPlayerId,
    tick: TickStamp,
    mutation: pumpkin_cluster::inventory::InventoryMutation,
    affected: Vec<pumpkin_cluster::protocol::ChunkKey>,
    origin: Option<u64>,
    input_sequence: Option<pumpkin_cluster::identity::PlayerSeq>,
) -> Result<(), InventoryApplyError> {
    let actor_chunk = affected
        .first()
        .copied()
        .ok_or(InventoryApplyError::Rejected)?;
    submit_inventory_intent_with_sequence(
        server,
        InventoryMutationIntent {
            actor: actor.into(),
            tick,
            payload: InventoryIntentPayload::Mutation(mutation),
            actor_chunk,
            affected,
            screen_session: None,
        },
        origin,
        input_sequence,
    )
}

pub(crate) fn submit_selected_slot_mutation(
    server: &Arc<Server>,
    actor: GlobalPlayerId,
    tick: TickStamp,
    selected_slot: u8,
    affected: Vec<pumpkin_cluster::protocol::ChunkKey>,
    origin: Option<u64>,
) -> Result<(), InventoryApplyError> {
    submit_selected_slot_mutation_with_sequence(
        server,
        actor,
        tick,
        selected_slot,
        affected,
        origin,
        None,
    )
}

pub(crate) fn submit_selected_slot_mutation_with_sequence(
    server: &Arc<Server>,
    actor: GlobalPlayerId,
    tick: TickStamp,
    selected_slot: u8,
    affected: Vec<pumpkin_cluster::protocol::ChunkKey>,
    origin: Option<u64>,
    input_sequence: Option<pumpkin_cluster::identity::PlayerSeq>,
) -> Result<(), InventoryApplyError> {
    let snapshot = inventory_snapshot(server, InventoryTarget::Player(actor))
        .ok_or(InventoryApplyError::TargetUnavailable)?;
    let mutation =
        pumpkin_cluster::inventory::InventoryMutation::select_hotbar(&snapshot, selected_slot)
            .map_err(|_| InventoryApplyError::Rejected)?;
    submit_inventory_mutation_with_sequence(
        server,
        actor,
        tick,
        mutation,
        affected,
        origin,
        input_sequence,
    )
}

pub(crate) fn submit_inventory_intent(
    server: &Arc<Server>,
    intent: InventoryMutationIntent,
    origin: Option<u64>,
) -> Result<(), InventoryApplyError> {
    submit_inventory_intent_with_sequence(server, intent, origin, None)
}

pub(crate) fn submit_inventory_intent_with_sequence(
    server: &Arc<Server>,
    intent: InventoryMutationIntent,
    origin: Option<u64>,
    input_sequence: Option<pumpkin_cluster::identity::PlayerSeq>,
) -> Result<(), InventoryApplyError> {
    server
        .cluster_inventory_action_route_tx
        .borrow()
        .clone()
        .ok_or(InventoryApplyError::TargetUnavailable)?
        .try_send(LocalInventoryAction {
            intent,
            origin,
            input_sequence,
        })
        .map_err(|_| InventoryApplyError::QueueFull)?;
    Ok(())
}

pub(crate) fn subscribe_inventory_publications(
    server: &Server,
) -> broadcast::Receiver<InventoryPublication> {
    server.cluster_inventory_publication_tx.subscribe()
}

pub(crate) fn inventory_snapshot(
    server: &Server,
    target: InventoryTarget,
) -> Option<InventoryStateSnapshot> {
    server
        .cluster_inventory_snapshots
        .load()
        .get(&target)
        .cloned()
}

pub(crate) fn inventory_screen_snapshot(
    server: &Server,
    session: pumpkin_cluster::inventory::InventoryScreenSession,
) -> Option<pumpkin_cluster::inventory::InventoryScreenSnapshot> {
    server
        .cluster_inventory_screens
        .load()
        .get(&session)
        .cloned()
}

pub(crate) fn request_inventory_screen_refresh(
    server: &Server,
    session: pumpkin_cluster::inventory::InventoryScreenSession,
    target: GlobalPlayerId,
    expected_revision: u64,
) -> Result<(), InventoryApplyError> {
    server
        .cluster_invsee_tx
        .try_send(InvseeCommand::Refresh {
            session,
            target,
            expected_revision,
        })
        .map_err(|_| InventoryApplyError::QueueFull)
}

pub(crate) fn subscribe_inventory_screen(
    server: &Server,
    session: pumpkin_cluster::inventory::InventoryScreenSession,
    target: GlobalPlayerId,
) -> Result<(), InventoryApplyError> {
    server
        .cluster_invsee_tx
        .try_send(InvseeCommand::Subscribe(InvseeSubscription {
            viewer: session.actor,
            session: session.session,
            target,
        }))
        .map_err(|_| InventoryApplyError::QueueFull)
}

pub(crate) fn unsubscribe_inventory_screen(
    server: &Server,
    session: pumpkin_cluster::inventory::InventoryScreenSession,
    target: GlobalPlayerId,
) -> Result<(), InventoryApplyError> {
    server
        .cluster_invsee_tx
        .try_send(InvseeCommand::Unsubscribe(InvseeSubscription {
            viewer: session.actor,
            session: session.session,
            target,
        }))
        .map_err(|_| InventoryApplyError::QueueFull)
}

pub(crate) fn notify_inventory_target_removed(
    server: &Server,
    target: GlobalPlayerId,
) -> Result<(), InventoryApplyError> {
    server
        .cluster_invsee_tx
        .try_send(InvseeCommand::TargetRemoved(target))
        .map_err(|_| InventoryApplyError::QueueFull)
}

pub(crate) fn notify_inventory_target_changed(
    server: &Server,
    target: GlobalPlayerId,
    revision: u64,
) -> Result<(), InventoryApplyError> {
    server
        .cluster_invsee_tx
        .try_send(InvseeCommand::TargetChanged(target, revision))
        .map_err(|_| InventoryApplyError::QueueFull)
}

pub(crate) fn notify_inventory_target_relocated(
    server: &Server,
    target: GlobalPlayerId,
) -> Result<(), InventoryApplyError> {
    server
        .cluster_invsee_tx
        .try_send(InvseeCommand::TargetRelocated(target))
        .map_err(|_| InventoryApplyError::QueueFull)
}

pub(crate) fn notify_inventory_screen_refresh_applied(
    server: &Server,
    session: pumpkin_cluster::inventory::InventoryScreenSession,
    target: GlobalPlayerId,
    revision: u64,
) -> Result<(), InventoryApplyError> {
    server
        .cluster_invsee_tx
        .try_send(InvseeCommand::RefreshApplied {
            session,
            target,
            revision,
        })
        .map_err(|_| InventoryApplyError::QueueFull)
}

pub(crate) fn notify_inventory_screen_refresh_rejected(
    server: &Server,
    session: pumpkin_cluster::inventory::InventoryScreenSession,
    target: GlobalPlayerId,
    expected_revision: u64,
    retry: bool,
) -> Result<(), InventoryApplyError> {
    server
        .cluster_invsee_tx
        .try_send(InvseeCommand::RefreshRejected {
            session,
            target,
            expected_revision,
            retry,
        })
        .map_err(|_| InventoryApplyError::QueueFull)
}

pub(super) fn refreshed_invsee_screen(
    screen: &pumpkin_cluster::inventory::InventoryScreenSnapshot,
    target: GlobalPlayerId,
    expected_revision: u64,
    viewer_chunk: pumpkin_cluster::protocol::ChunkKey,
    snapshot: &InventorySnapshot,
) -> Result<pumpkin_cluster::inventory::InventoryScreenSnapshot, InventoryApplyError> {
    if snapshot.target != target || snapshot.affected.len() != 1 {
        return Err(InventoryApplyError::Rejected);
    }
    let mut cells = Vec::with_capacity(snapshot.slots.len() + 1);
    for slot in &snapshot.slots {
        let loc = player_inventory_loc(slot.index).ok_or(InventoryApplyError::Rejected)?;
        cells.push(pumpkin_cluster::inventory::InventoryCell {
            loc,
            item: slot.item_id,
            count: slot.count,
            nbt: slot.nbt.clone(),
        });
    }
    cells.push(pumpkin_cluster::inventory::InventoryCell {
        loc: pumpkin_cluster::inventory::InvLoc::new(INV_SELECTED, 0),
        item: u16::from(snapshot.selected) + 1,
        count: 1,
        nbt: Vec::new(),
    });
    let preview = InventoryStateSnapshot {
        target: InventoryTarget::Player(target),
        revision: snapshot.revision,
        cells,
    };
    validate_imported_inventory_snapshots(std::slice::from_ref(&preview))?;
    if screen.preview.as_ref().is_none_or(|current| {
        current.target != preview.target || current.revision != expected_revision
    }) {
        return Err(InventoryApplyError::Rejected);
    }
    let mut next = screen.clone();
    for slot in &mut next.slots {
        if slot.slot.target == preview.target {
            slot.cell = preview
                .cells
                .iter()
                .find(|cell| cell.loc == slot.slot.loc)
                .cloned()
                .unwrap_or_else(|| pumpkin_cluster::inventory::InventoryCell::empty(slot.slot.loc));
        }
    }
    next.preview = Some(preview);
    next.affected = vec![viewer_chunk, snapshot.affected[0]];
    next.affected.sort_unstable();
    next.affected.dedup();
    Ok(next)
}

#[cfg(test)]
mod invsee_refresh_tests {
    use super::*;
    use pumpkin_cluster::identity::PlayerSlot;
    use pumpkin_cluster::inventory::{
        INV_ARMOR, INV_MAIN, INV_OFFHAND, InvLoc, InventoryScreenSession, InventoryScreenSlot,
        InventorySlotRef,
    };
    use pumpkin_cluster::protocol::{ChunkKey, WorldId};

    #[test]
    fn remote_change_refreshes_editable_slots_without_reopening_window() {
        let viewer = GlobalPlayerId::new(ServerId(2), PlayerSlot(1));
        let target = GlobalPlayerId::new(ServerId(3), PlayerSlot(1));
        let world = WorldId::new(0, 1);
        let old_chunk = ChunkKey::new(world, 0, 0);
        let new_chunk = ChunkKey::new(world, 2, 0);
        let viewer_chunk = ChunkKey::new(world, 1, 0);
        let armor = InvLoc::new(INV_ARMOR, 3);
        let offhand = InvLoc::new(INV_OFFHAND, 0);
        let hotbar = InvLoc::new(INV_MAIN, 0);
        let empty = |loc| pumpkin_cluster::inventory::InventoryCell::empty(loc);
        let target_slot = |screen_slot, loc| InventoryScreenSlot {
            screen_slot,
            slot: InventorySlotRef::new(InventoryTarget::Player(target), loc),
            cell: empty(loc),
            limit: 64,
        };
        let screen = pumpkin_cluster::inventory::InventoryScreenSnapshot {
            session: InventoryScreenSession {
                actor: viewer,
                session: 17,
            },
            sync_id: 17,
            window_kind: 0,
            container_slots: 45,
            slots: vec![
                target_slot(0, armor),
                target_slot(8, offhand),
                target_slot(36, hotbar),
            ],
            cursor: empty(InvLoc::new(INV_CURSOR, 0)),
            affected: vec![old_chunk, viewer_chunk],
            preview: Some(InventoryStateSnapshot {
                target: InventoryTarget::Player(target),
                revision: 5,
                cells: Vec::new(),
            }),
        };
        let filled = |loc, item| {
            inventory_cell_from_item_stack(loc, &ItemStack::new(1, item))
                .expect("serializable test item")
        };
        let new_cells = [
            filled(armor, &pumpkin_data::item::Item::STONE),
            filled(offhand, &pumpkin_data::item::Item::DIRT),
            filled(hotbar, &pumpkin_data::item::Item::DIAMOND),
        ];
        let remote = InventorySnapshot::new(
            target,
            "remote".to_string(),
            6,
            0,
            new_cells
                .iter()
                .zip([39, 40, 0])
                .map(|(cell, index)| {
                    pumpkin_cluster::invsee::InvseeSlot::new(
                        index,
                        cell.item,
                        cell.count,
                        cell.nbt.clone(),
                    )
                })
                .collect(),
        )
        .with_affected(vec![new_chunk]);
        let refreshed = refreshed_invsee_screen(&screen, target, 5, viewer_chunk, &remote)
            .expect("fresh matching remote snapshot");
        assert_eq!(refreshed.session, screen.session);
        assert_eq!(
            refreshed
                .slots
                .iter()
                .map(|slot| slot.cell.item)
                .collect::<Vec<_>>(),
            vec![new_cells[0].item, new_cells[1].item, new_cells[2].item,]
        );
        assert_eq!(refreshed.affected, vec![viewer_chunk, new_chunk]);
        assert!(refreshed.slots.iter().all(|slot| slot.screen_slot != 4));
        assert!(matches!(
            refreshed_invsee_screen(&refreshed, target, 5, viewer_chunk, &remote),
            Err(InventoryApplyError::Rejected)
        ));
    }

    #[test]
    fn stack_template_survives_semantic_count_changes() {
        let location = InvLoc::new(INV_MAIN, 2);
        let original = ItemStack::new(17, &pumpkin_data::item::Item::STONE);
        let mut cell = inventory_cell_from_item_stack(location, &original).unwrap();
        assert_eq!(
            item_stack_from_inventory_cell(&cell).unwrap().item_count,
            17
        );
        cell.count = 4;
        assert_eq!(item_stack_from_inventory_cell(&cell).unwrap().item_count, 4);
        let smaller = ItemStack::new(4, &pumpkin_data::item::Item::STONE);
        assert_eq!(
            cell.nbt,
            inventory_cell_from_item_stack(location, &smaller)
                .unwrap()
                .nbt
        );
    }

    #[test]
    fn differently_sized_stacks_merge_and_project_with_exact_counts() {
        use pumpkin_cluster::inventory::{
            InventoryGesture, InventoryService, InventorySlotRef, InventoryStateSnapshot,
            InventoryTarget, reduce_inventory_gestures,
        };

        let target = InventoryTarget::Player(GlobalPlayerId::new(ServerId(1), PlayerSlot(2)));
        let first = inventory_cell_from_item_stack(
            InvLoc::new(INV_MAIN, 0),
            &ItemStack::new(17, &pumpkin_data::item::Item::STONE),
        )
        .unwrap();
        let second = inventory_cell_from_item_stack(
            InvLoc::new(INV_MAIN, 1),
            &ItemStack::new(4, &pumpkin_data::item::Item::STONE),
        )
        .unwrap();
        let snapshot = InventoryStateSnapshot {
            target,
            revision: 5,
            cells: vec![first, second],
        };
        let reduction = reduce_inventory_gestures(
            &[snapshot.clone()],
            &[InventoryGesture::Transfer {
                source: InventorySlotRef::new(target, InvLoc::new(INV_MAIN, 0)),
                destination: InventorySlotRef::new(target, InvLoc::new(INV_MAIN, 1)),
                count: 3,
                destination_limit: 64,
            }],
        )
        .unwrap();
        let mut service = InventoryService::new();
        service
            .install(target, snapshot.revision, snapshot.cells)
            .unwrap();
        service.mutate(reduction.mutation).unwrap();
        let resulting = service.snapshot(target).unwrap();
        let counts: Vec<_> = resulting
            .cells
            .iter()
            .map(|cell| item_stack_from_inventory_cell(cell).unwrap().item_count)
            .collect();
        assert_eq!(counts, vec![14, 7]);
    }

    #[test]
    fn frozen_playerdata_yields_main_armor_offhand_and_selected_without_player_locks() {
        let gid = GlobalPlayerId::new(ServerId(1), PlayerSlot(7));
        let mut data = pumpkin_nbt::compound::NbtCompound::new();
        data.put_int("SelectedItemSlot", 4);
        let mut main = pumpkin_nbt::compound::NbtCompound::new();
        main.put_byte("Slot", 4);
        ItemStack::new(17, &pumpkin_data::item::Item::STONE).write_item_stack(&mut main);
        let mut list_offhand = pumpkin_nbt::compound::NbtCompound::new();
        list_offhand.put_byte("Slot", -106);
        ItemStack::new(1, &pumpkin_data::item::Item::DIRT).write_item_stack(&mut list_offhand);
        data.put(
            "Inventory",
            pumpkin_nbt::tag::NbtTag::List(vec![
                pumpkin_nbt::tag::NbtTag::Compound(main),
                pumpkin_nbt::tag::NbtTag::Compound(list_offhand),
            ]),
        );
        let mut equipment = pumpkin_nbt::compound::NbtCompound::new();
        let mut head = pumpkin_nbt::compound::NbtCompound::new();
        ItemStack::new(1, &pumpkin_data::item::Item::DIAMOND).write_item_stack(&mut head);
        equipment.put_compound("head", head);
        data.put_compound("equipment", equipment);

        let snapshot = player_inventory_snapshot_from_nbt(gid, Some(&data)).unwrap();
        let at = |loc| snapshot.cells.iter().find(|cell| cell.loc == loc).unwrap();
        assert_eq!(at(InvLoc::new(INV_MAIN, 4)).count, 17);
        assert_eq!(
            at(InvLoc::new(INV_ARMOR, 3)).item,
            pumpkin_data::item::Item::DIAMOND.id
        );
        assert_eq!(
            at(InvLoc::new(INV_OFFHAND, 0)).item,
            pumpkin_data::item::Item::DIRT.id
        );
        assert_eq!(at(InvLoc::new(INV_SELECTED, 0)).item, 5);

        let chunk = ChunkKey::new(WorldId::new(0, 1), 2, 3);
        let (crafting, screen) = player_screen_snapshot(gid, &snapshot, vec![chunk]).unwrap();
        assert!(crafting.cells.is_empty());
        assert_eq!(screen.slots.len(), 46);
        assert_eq!(screen.slots[0].limit, 0);
        assert_eq!(screen.slots[5].slot.loc, InvLoc::new(INV_ARMOR, 3));
        assert_eq!(screen.slots[36].slot.loc, InvLoc::new(INV_MAIN, 0));
        assert_eq!(screen.slots[45].slot.loc, InvLoc::new(INV_OFFHAND, 0));
        assert_eq!(screen.affected, vec![chunk]);

        let new_player = player_inventory_snapshot_from_nbt(gid, None).unwrap();
        assert_eq!(new_player.cells.len(), 1);
        assert_eq!(new_player.cells[0].loc, InvLoc::new(INV_SELECTED, 0));
        assert_eq!(new_player.cells[0].item, 1);
    }

    #[test]
    fn accepted_ender_chest_state_survives_logout_serialization() {
        use crate::entity::NBTStorage;
        use pumpkin_inventory::player::ender_chest_inventory::EnderChestInventory;

        let gid = GlobalPlayerId::new(ServerId(1), PlayerSlot(3));
        let mut initial = ender_chest_stacks_from_nbt(None);
        initial[0] = ItemStack::new(1, &pumpkin_data::item::Item::STONE);
        let chest = EnderChestInventory::from_stacks(initial);
        let accepted = ItemStack::new(3, &pumpkin_data::item::Item::DIAMOND);
        let cell = inventory_cell_from_item_stack(InvLoc::new(INV_MAIN, 0), &accepted).unwrap();
        let snapshot = InventoryStateSnapshot {
            target: ender_chest_target(gid),
            revision: 2,
            cells: vec![cell],
        };
        chest.publish_cluster_snapshot(ender_chest_stacks_from_state(&snapshot).unwrap());
        let mut logout = pumpkin_nbt::compound::NbtCompound::new();
        chest.write_nbt(&mut logout);
        let saved = logout.get_list("EnderItems").unwrap();
        let saved = saved[0].extract_compound().unwrap();
        let saved = ItemStack::read_item_stack(saved).unwrap();
        assert_eq!(saved.item.id, pumpkin_data::item::Item::DIAMOND.id);
        assert_eq!(saved.item_count, 3);
    }
}

pub(crate) fn publish_inventory_screen(
    server: &Server,
    screen: pumpkin_cluster::inventory::InventoryScreenSnapshot,
) {
    let mut next = (**server.cluster_inventory_screens.load()).clone();
    next.insert(screen.session, screen);
    server.cluster_inventory_screens.store(Arc::new(next));
}

pub(crate) fn unpublish_inventory_screen(
    server: &Server,
    session: pumpkin_cluster::inventory::InventoryScreenSession,
) {
    let mut next = (**server.cluster_inventory_screens.load()).clone();
    next.remove(&session);
    server.cluster_inventory_screens.store(Arc::new(next));
}

pub(crate) fn unpublish_inventory_screens_for_target(server: &Server, target: InventoryTarget) {
    let mut next = (**server.cluster_inventory_screens.load()).clone();
    next.retain(|_, screen| !screen.slots.iter().any(|slot| slot.slot.target == target));
    server.cluster_inventory_screens.store(Arc::new(next));
}

pub(crate) fn inventory_screens_for_target(
    server: &Server,
    target: InventoryTarget,
) -> Vec<pumpkin_cluster::inventory::InventoryScreenSnapshot> {
    server
        .cluster_inventory_screens
        .load()
        .values()
        .filter(|screen| screen.slots.iter().any(|slot| slot.slot.target == target))
        .cloned()
        .collect()
}

fn refresh_inventory_screens(server: &Server, snapshots: &[InventoryStateSnapshot]) {
    let mut next = (**server.cluster_inventory_screens.load()).clone();
    for screen in next.values_mut() {
        for slot in &mut screen.slots {
            let Some(snapshot) = snapshots
                .iter()
                .find(|snapshot| snapshot.target == slot.slot.target)
            else {
                continue;
            };
            slot.cell = snapshot
                .cells
                .iter()
                .find(|cell| cell.loc == slot.slot.loc)
                .cloned()
                .unwrap_or_else(|| pumpkin_cluster::inventory::InventoryCell::empty(slot.slot.loc));
        }
        if let Some(snapshot) = snapshots
            .iter()
            .find(|snapshot| snapshot.target == InventoryTarget::Player(screen.session.actor))
        {
            let cursor =
                pumpkin_cluster::inventory::InvLoc::new(pumpkin_cluster::inventory::INV_CURSOR, 0);
            screen.cursor = snapshot
                .cells
                .iter()
                .find(|cell| cell.loc == cursor)
                .cloned()
                .unwrap_or_else(|| pumpkin_cluster::inventory::InventoryCell::empty(cursor));
        }
    }
    server.cluster_inventory_screens.store(Arc::new(next));
}

pub(crate) fn publish_inventory_snapshots(
    server: &Server,
    snapshots: impl IntoIterator<Item = InventoryStateSnapshot>,
) {
    let snapshots: Vec<_> = snapshots.into_iter().collect();
    server.cluster_inventory_snapshots.rcu(|current| {
        let mut next = (**current).clone();
        for snapshot in &snapshots {
            next.insert(snapshot.target, snapshot.clone());
        }
        Arc::new(next)
    });
}

pub(crate) fn publish_imported_inventory_snapshots(
    server: &Server,
    snapshots: impl IntoIterator<Item = InventoryStateSnapshot>,
) -> Result<(), InventoryApplyError> {
    let snapshots: Vec<_> = snapshots.into_iter().collect();
    validate_imported_inventory_snapshots(&snapshots)?;
    let before = server.cluster_inventory_screens.load_full();
    publish_inventory_snapshots(server, snapshots.iter().cloned());
    refresh_inventory_screens(server, &snapshots);
    let after = server.cluster_inventory_screens.load_full();
    super::cluster_inventory_protocol::publish_inventory_screen_deltas(server, &before, &after)
}

pub(crate) fn validate_imported_inventory_snapshots(
    snapshots: &[InventoryStateSnapshot],
) -> Result<(), InventoryApplyError> {
    for snapshot in snapshots {
        for cell in &snapshot.cells {
            if cell.loc.inv == INV_SELECTED && cell.loc.slot == 0 {
                let selected = cell
                    .item
                    .checked_sub(1)
                    .ok_or(InventoryApplyError::Rejected)?;
                if selected > 8 || cell.count != 1 || !cell.nbt.is_empty() {
                    return Err(InventoryApplyError::Rejected);
                }
            } else {
                item_stack_from_inventory_cell(cell)?;
            }
        }
    }
    Ok(())
}

pub(crate) fn unpublish_inventory_snapshot(server: &Server, target: InventoryTarget) {
    server.cluster_inventory_snapshots.rcu(|current| {
        let mut next = (**current).clone();
        next.remove(&target);
        Arc::new(next)
    });
}

pub(crate) fn publish_inventory_resolution(
    server: &Server,
    resolution: InventoryResolution,
    origin: Option<u64>,
) -> Result<(), InventoryApplyError> {
    validate_inventory_resolution_projection(&resolution)?;
    apply_inventory_resolution(server, &resolution)?;
    super::cluster_inventory_protocol::apply_inventory_effects(server, &resolution)?;
    publish_inventory_snapshots(server, resolution.authoritative.iter().cloned());
    let screens_before = server.cluster_inventory_screens.load_full();
    refresh_inventory_screens(server, &resolution.authoritative);
    let screens_after = server.cluster_inventory_screens.load_full();
    super::cluster_inventory_protocol::publish_inventory_screen_deltas(
        server,
        &screens_before,
        &screens_after,
    )?;
    super::cluster_inventory_protocol::publish_inventory_menu_effects(server, &resolution)?;
    let _ = server
        .cluster_inventory_publication_tx
        .send(InventoryPublication { resolution, origin });
    Ok(())
}

fn validate_inventory_resolution_projection(
    resolution: &InventoryResolution,
) -> Result<(), InventoryApplyError> {
    validate_imported_inventory_snapshots(&resolution.authoritative)
}

pub(crate) fn apply_inventory_resolution(
    server: &Server,
    resolution: &InventoryResolution,
) -> Result<(), InventoryApplyError> {
    for snapshot in &resolution.authoritative {
        if let InventoryTarget::Virtual {
            owner: pumpkin_cluster::actor::ActorId::Player(gid),
            session: 0,
            kind: 0x100,
            ..
        } = snapshot.target
            && gid.server == ServerId(server.advanced_config.cluster.server_id)
            && let Some(player) = find_inventory_player(server, gid)
        {
            player
                .ender_chest_inventory
                .publish_cluster_snapshot(ender_chest_stacks_from_state(snapshot)?);
        }
    }
    let delta = match &resolution.result {
        Ok(delta) => delta,
        Err(_) => {
            for snapshot in &resolution.authoritative {
                if let InventoryTarget::Player(target) = snapshot.target
                    && target.server == ServerId(server.advanced_config.cluster.server_id)
                {
                    publish_player_inventory_snapshot(server, target, snapshot)?;
                }
            }
            return Ok(());
        }
    };
    validate_inventory_effects(server, &resolution.effects)?;
    let mut players = HashMap::new();
    let local = ServerId(server.advanced_config.cluster.server_id);
    for operation in &delta.operations {
        let InventoryTarget::Player(target) = operation.target else {
            continue;
        };
        if target.server != local {
            continue;
        }
        if !players.contains_key(&target) {
            let player = find_inventory_player(server, target)
                .ok_or(InventoryApplyError::TargetUnavailable)?;
            players.insert(target, player);
        }
        if operation.after.loc.inv == INV_SELECTED && operation.after.loc.slot == 0 {
            let selected = operation
                .after
                .item
                .checked_sub(1)
                .ok_or(InventoryApplyError::Rejected)?;
            if selected > 8 || operation.after.count != 1 || !operation.after.nbt.is_empty() {
                return Err(InventoryApplyError::Rejected);
            }
            continue;
        }
        item_stack_from_inventory_cell(&operation.after)?;
        if operation.after.loc.inv != INV_CURSOR || operation.after.loc.slot != 0 {
            player_inventory_slot(operation.after.loc).ok_or(InventoryApplyError::Rejected)?;
        }
    }
    let mut equipment = HashMap::<
        GlobalPlayerId,
        Vec<(pumpkin_data::data_component_impl::EquipmentSlot, ItemStack)>,
    >::new();
    for snapshot in &resolution.authoritative {
        let InventoryTarget::Player(target) = snapshot.target else {
            continue;
        };
        if target.server != local {
            continue;
        }
        let player = publish_player_inventory_snapshot(server, target, snapshot)?;
        players.insert(target, player);
    }
    for operation in &delta.operations {
        let InventoryTarget::Player(target) = operation.target else {
            continue;
        };
        if target.server != local {
            continue;
        }
        let player = players.get(&target).unwrap().clone();
        if (operation.after.loc.inv == INV_CURSOR || operation.after.loc.inv == INV_SELECTED)
            && operation.after.loc.slot == 0
        {
            continue;
        }
        let slot =
            player_inventory_slot(operation.after.loc).ok_or(InventoryApplyError::Rejected)?;
        let stack = item_stack_from_inventory_cell(&operation.after)?;
        if let Some(equipment_slot) = equipment_slot(player.as_ref(), slot) {
            equipment
                .entry(target)
                .or_default()
                .push((equipment_slot, stack));
        }
    }
    for (target, slots) in equipment {
        if let Some(player) = players.get(&target) {
            player.living_entity.send_equipment_changes(&slots);
        }
    }
    Ok(())
}

fn publish_player_inventory_snapshot(
    server: &Server,
    target: GlobalPlayerId,
    snapshot: &InventoryStateSnapshot,
) -> Result<Arc<crate::entity::player::Player>, InventoryApplyError> {
    let player =
        find_inventory_player(server, target).ok_or(InventoryApplyError::TargetUnavailable)?;
    let projection = cluster_player_inventory_snapshot_from_state(snapshot)?;
    let stacks = projection.stacks.clone();
    let selected_slot = projection.selected_slot;
    let cursor = snapshot
        .cells
        .iter()
        .find(|cell| cell.loc.inv == INV_CURSOR && cell.loc.slot == 0)
        .map(item_stack_from_inventory_cell)
        .transpose()?
        .unwrap_or_else(|| ItemStack::EMPTY.clone());
    if !player.inventory.publish_cluster_snapshot(projection) {
        return Err(InventoryApplyError::Rejected);
    }
    for (slot, stack) in stacks.into_iter().enumerate() {
        player.try_send_slot_set_packet(&CSetPlayerInventory::new(
            (slot as i32).into(),
            &ItemStackSerializer::from(stack),
        ));
    }
    let serializer = ItemStackSerializer::from(cursor);
    player.try_send_cursor_packet(&CSetCursorItem::new(&serializer));
    player.try_send_selected_slot(selected_slot);
    Ok(player)
}

pub(crate) fn cluster_player_inventory_snapshot_from_state(
    snapshot: &InventoryStateSnapshot,
) -> Result<ClusterPlayerInventorySnapshot, InventoryApplyError> {
    let mut stacks = vec![ItemStack::EMPTY.clone(); 41];
    let mut selected_slot = None;
    for cell in &snapshot.cells {
        if cell.loc.inv == INV_SELECTED && cell.loc.slot == 0 {
            let selected = cell
                .item
                .checked_sub(1)
                .ok_or(InventoryApplyError::Rejected)?;
            let selected = u8::try_from(selected).map_err(|_| InventoryApplyError::Rejected)?;
            if selected > 8 || cell.count != 1 || !cell.nbt.is_empty() {
                return Err(InventoryApplyError::Rejected);
            }
            selected_slot = Some(selected);
            continue;
        }
        let stack = item_stack_from_inventory_cell(cell)?;
        if let Some(slot) = player_inventory_slot(cell.loc) {
            stacks[slot] = stack;
        }
    }
    let selected_slot = selected_slot.ok_or(InventoryApplyError::Rejected)?;
    Ok(ClusterPlayerInventorySnapshot {
        stacks,
        selected_slot,
    })
}

fn validate_inventory_effects(
    server: &Server,
    effects: &[pumpkin_cluster::inventory::InventoryGestureEffect],
) -> Result<(), InventoryApplyError> {
    use pumpkin_cluster::inventory::InventoryGestureEffect;
    let local = ServerId(server.advanced_config.cluster.server_id);
    for effect in effects {
        let (target, stack) = match effect {
            InventoryGestureEffect::Removed { source, stack, .. } => (source.target, stack),
            InventoryGestureEffect::Crafted {
                destination, stack, ..
            } => (destination.target, stack),
            InventoryGestureEffect::CreatedDrop { owner, stack } => (*owner, stack),
        };
        let player = match target {
            InventoryTarget::Player(player) => Some(player),
            InventoryTarget::Virtual {
                owner: pumpkin_cluster::actor::ActorId::Player(player),
                ..
            } => Some(player),
            _ => None,
        };
        if let Some(player) = player.filter(|player| player.server == local) {
            find_inventory_player(server, player).ok_or(InventoryApplyError::TargetUnavailable)?;
            item_stack_from_inventory_cell(stack)?;
        }
    }
    Ok(())
}

pub(crate) fn find_inventory_player(
    server: &Server,
    target: GlobalPlayerId,
) -> Option<Arc<crate::entity::player::Player>> {
    server
        .worlds
        .load()
        .iter()
        .flat_map(|world| world.players.load().iter().cloned().collect::<Vec<_>>())
        .find(|player| player.cluster_gid() == Some(target))
}

fn player_inventory_slot(loc: pumpkin_cluster::inventory::InvLoc) -> Option<usize> {
    match (loc.inv, loc.slot) {
        (pumpkin_cluster::inventory::INV_MAIN, slot @ 0..=35) => Some(usize::from(slot)),
        (pumpkin_cluster::inventory::INV_ARMOR, slot @ 0..=3) => Some(usize::from(slot + 36)),
        (pumpkin_cluster::inventory::INV_OFFHAND, 0) => Some(40),
        _ => None,
    }
}

fn equipment_slot(
    player: &crate::entity::player::Player,
    slot: usize,
) -> Option<pumpkin_data::data_component_impl::EquipmentSlot> {
    use pumpkin_data::data_component_impl::EquipmentSlot;
    match slot {
        36 => Some(EquipmentSlot::FEET),
        37 => Some(EquipmentSlot::LEGS),
        38 => Some(EquipmentSlot::CHEST),
        39 => Some(EquipmentSlot::HEAD),
        40 => Some(EquipmentSlot::OFF_HAND),
        selected if selected == usize::from(player.inventory.get_selected_slot()) => {
            Some(EquipmentSlot::MAIN_HAND)
        }
        _ => None,
    }
}

pub(crate) fn item_stack_from_inventory_cell(
    cell: &pumpkin_cluster::inventory::InventoryCell,
) -> Result<ItemStack, InventoryApplyError> {
    if cell.is_empty() {
        return Ok(ItemStack::EMPTY.clone());
    }
    let mut cursor = std::io::Cursor::new(cell.nbt.as_slice());
    let mut reader = pumpkin_nbt::deserializer::NbtReadHelperJava::new(&mut cursor);
    let document = Nbt::read_unnamed(&mut reader).map_err(|_| InventoryApplyError::Rejected)?;
    if cursor.position() != cell.nbt.len() as u64
        || document
            .clone()
            .write_unnamed_canonical()
            .map_err(|_| InventoryApplyError::Rejected)?
            .as_ref()
            != cell.nbt.as_slice()
    {
        return Err(InventoryApplyError::Rejected);
    }
    let stack =
        ItemStack::read_item_stack(&document.root_tag).ok_or(InventoryApplyError::Rejected)?;
    if stack.item.id != cell.item || stack.item_count != 1 {
        return Err(InventoryApplyError::Rejected);
    }
    let mut stack = stack;
    stack.item_count = cell.count;
    Ok(stack)
}

pub(crate) fn install_player_inventory(
    server: &Server,
    target: GlobalPlayerId,
    player: &crate::entity::player::Player,
    snapshot: InventoryStateSnapshot,
    ender: InventoryStateSnapshot,
) -> Result<(), InventoryApplyError> {
    if snapshot.target != InventoryTarget::Player(target)
        || ender.target != ender_chest_target(target)
    {
        return Err(InventoryApplyError::Rejected);
    }
    let position = player.position();
    let chunks = vec![chunk_of_block(
        super::cluster_world_delta::world_id(&player.world()),
        position.x.floor() as i32,
        position.z.floor() as i32,
    )];
    let (crafting, screen) = player_screen_snapshot(target, &snapshot, chunks.clone())?;
    server
        .cluster_inventory_install_route_tx
        .borrow()
        .clone()
        .ok_or(InventoryApplyError::TargetUnavailable)?
        .try_send(LocalInventoryLifecycle::InstallPlayer {
            gid: target,
            token: None,
            player: snapshot,
            crafting,
            ender,
            screen,
            chunks,
        })
        .map_err(|_| InventoryApplyError::QueueFull)
}

pub(crate) fn submit_handoff_inventory(
    server: &Server,
    gid: GlobalPlayerId,
    token: u64,
    chunk: pumpkin_cluster::protocol::ChunkKey,
    data: Option<&pumpkin_nbt::compound::NbtCompound>,
) -> Result<u64, InventoryApplyError> {
    let player = player_inventory_snapshot_from_nbt(gid, data)?;
    let ender = ender_chest_snapshot_from_nbt(gid, data)?;
    let revision = player.revision;
    let chunks = vec![chunk];
    let (crafting, screen) = player_screen_snapshot(gid, &player, chunks.clone())?;
    server
        .cluster_inventory_install_route_tx
        .borrow()
        .clone()
        .ok_or(InventoryApplyError::TargetUnavailable)?
        .try_send(LocalInventoryLifecycle::InstallPlayer {
            gid,
            token: Some(token),
            player,
            crafting,
            ender,
            screen,
            chunks,
        })
        .map_err(|_| InventoryApplyError::QueueFull)?;
    Ok(revision)
}

pub(crate) fn retire_handoff_inventory_install(server: &Server, gid: GlobalPlayerId, token: u64) {
    server.cluster_inventory_handoff_installed.rcu(|published| {
        let mut next = (**published).clone();
        if next.get(&gid).is_some_and(|status| status.token == token) {
            next.remove(&gid);
        }
        Arc::new(next)
    });
}

pub(crate) fn cancel_handoff_inventory_install(server: &Server, gid: GlobalPlayerId, token: u64) {
    server.cluster_inventory_cancel_requests.rcu(|published| {
        let mut next = (**published).clone();
        next.insert((gid, token));
        Arc::new(next)
    });
}

fn player_screen_snapshot(
    target: GlobalPlayerId,
    player_snapshot: &InventoryStateSnapshot,
    chunks: Vec<pumpkin_cluster::protocol::ChunkKey>,
) -> Result<
    (
        InventoryStateSnapshot,
        pumpkin_cluster::inventory::InventoryScreenSnapshot,
    ),
    InventoryApplyError,
> {
    let crafting = InventoryTarget::Virtual {
        host: target.server,
        owner: pumpkin_cluster::actor::ActorId::Player(target),
        session: 0,
        kind: pumpkin_data::screen::WindowType::Crafting as u16 + 1,
    };
    let crafting_cells = Vec::new();
    let mut slots = Vec::new();
    for screen_slot in 0..46 {
        let slot = match screen_slot {
            0..=4 => pumpkin_cluster::inventory::InventorySlotRef::new(
                crafting,
                pumpkin_cluster::inventory::InvLoc::new(
                    pumpkin_cluster::inventory::INV_MAIN,
                    screen_slot as u16,
                ),
            ),
            5..=8 => pumpkin_cluster::inventory::InventorySlotRef::new(
                InventoryTarget::Player(target),
                pumpkin_cluster::inventory::InvLoc::new(
                    pumpkin_cluster::inventory::INV_ARMOR,
                    (8 - screen_slot) as u16,
                ),
            ),
            9..=35 => pumpkin_cluster::inventory::InventorySlotRef::new(
                InventoryTarget::Player(target),
                pumpkin_cluster::inventory::InvLoc::new(
                    pumpkin_cluster::inventory::INV_MAIN,
                    screen_slot as u16,
                ),
            ),
            36..=44 => pumpkin_cluster::inventory::InventorySlotRef::new(
                InventoryTarget::Player(target),
                pumpkin_cluster::inventory::InvLoc::new(
                    pumpkin_cluster::inventory::INV_MAIN,
                    (screen_slot - 36) as u16,
                ),
            ),
            45 => pumpkin_cluster::inventory::InventorySlotRef::new(
                InventoryTarget::Player(target),
                pumpkin_cluster::inventory::InvLoc::new(pumpkin_cluster::inventory::INV_OFFHAND, 0),
            ),
            _ => continue,
        };
        let cell = player_snapshot
            .cells
            .iter()
            .find(|cell| slot.target == InventoryTarget::Player(target) && cell.loc == slot.loc)
            .cloned()
            .unwrap_or_else(|| pumpkin_cluster::inventory::InventoryCell::empty(slot.loc));
        slots.push(pumpkin_cluster::inventory::InventoryScreenSlot {
            screen_slot: screen_slot as u16,
            slot,
            cell,
            limit: if screen_slot == 0 {
                0
            } else if (5..=8).contains(&screen_slot) {
                1
            } else {
                64
            },
        });
    }
    let cursor_loc =
        pumpkin_cluster::inventory::InvLoc::new(pumpkin_cluster::inventory::INV_CURSOR, 0);
    let cursor = player_snapshot
        .cells
        .iter()
        .find(|cell| cell.loc == cursor_loc)
        .cloned()
        .unwrap_or_else(|| pumpkin_cluster::inventory::InventoryCell::empty(cursor_loc));
    let revision = pumpkin_cluster::inventory::inventory_content_revision(0, &crafting_cells)
        .map_err(|_| InventoryApplyError::Rejected)?;
    Ok((
        InventoryStateSnapshot {
            target: crafting,
            revision,
            cells: crafting_cells,
        },
        pumpkin_cluster::inventory::InventoryScreenSnapshot {
            session: pumpkin_cluster::inventory::InventoryScreenSession {
                actor: target,
                session: 0,
            },
            sync_id: 0,
            window_kind: 0,
            container_slots: 5,
            slots,
            cursor,
            affected: chunks,
            preview: None,
        },
    ))
}

pub(crate) fn install_inventory(
    server: &Server,
    snapshot: InventoryStateSnapshot,
    chunks: Vec<pumpkin_cluster::protocol::ChunkKey>,
) -> Result<(), InventoryApplyError> {
    server
        .cluster_inventory_install_route_tx
        .borrow()
        .clone()
        .ok_or(InventoryApplyError::TargetUnavailable)?
        .try_send(LocalInventoryLifecycle::Install {
            snapshot: snapshot.clone(),
            chunks,
            screen: None,
            menu: None,
        })
        .map_err(|_| InventoryApplyError::QueueFull)?;
    Ok(())
}

pub(crate) fn install_screen_inventory(
    server: &Server,
    snapshot: InventoryStateSnapshot,
    chunks: Vec<pumpkin_cluster::protocol::ChunkKey>,
    screen: pumpkin_cluster::inventory::InventoryScreenSnapshot,
) -> Result<(), InventoryApplyError> {
    install_screen_inventory_with_menu(server, snapshot, chunks, screen, None)
}

pub(crate) fn install_screen_inventory_with_menu(
    server: &Server,
    snapshot: InventoryStateSnapshot,
    chunks: Vec<pumpkin_cluster::protocol::ChunkKey>,
    screen: pumpkin_cluster::inventory::InventoryScreenSnapshot,
    menu: Option<pumpkin_cluster::inventory::InventoryMenuMetadata>,
) -> Result<(), InventoryApplyError> {
    server
        .cluster_inventory_install_route_tx
        .borrow()
        .clone()
        .ok_or(InventoryApplyError::TargetUnavailable)?
        .try_send(LocalInventoryLifecycle::Install {
            snapshot,
            chunks,
            screen: Some(screen),
            menu,
        })
        .map_err(|_| InventoryApplyError::QueueFull)
}

pub(crate) fn open_inventory_screen(
    server: &Server,
    screen: pumpkin_cluster::inventory::InventoryScreenSnapshot,
) -> Result<(), InventoryApplyError> {
    open_inventory_screen_with_menu(server, screen, None)
}

pub(crate) fn open_inventory_screen_with_menu(
    server: &Server,
    screen: pumpkin_cluster::inventory::InventoryScreenSnapshot,
    menu: Option<pumpkin_cluster::inventory::InventoryMenuMetadata>,
) -> Result<(), InventoryApplyError> {
    open_inventory_screen_with_presentation(server, screen, menu, None)
}

pub(crate) fn open_inventory_screen_with_presentation(
    server: &Server,
    screen: pumpkin_cluster::inventory::InventoryScreenSnapshot,
    menu: Option<pumpkin_cluster::inventory::InventoryMenuMetadata>,
    presentation: Option<ClusterScreenPresentation>,
) -> Result<(), InventoryApplyError> {
    if presentation.as_ref().is_some_and(|presentation| {
        item_stack_from_inventory_cell(&screen.cursor).is_err()
            || presentation.window_type as u16 != screen.window_kind
            || presentation.stacks.len()
                != screen
                    .slots
                    .iter()
                    .map(|slot| slot.screen_slot as usize)
                    .max()
                    .unwrap_or_default()
                    + 1
    }) {
        return Err(InventoryApplyError::Rejected);
    }
    server
        .cluster_inventory_install_route_tx
        .borrow()
        .clone()
        .ok_or(InventoryApplyError::TargetUnavailable)?
        .try_send(LocalInventoryLifecycle::OpenScreen {
            screen,
            menu,
            presentation,
        })
        .map_err(|_| InventoryApplyError::QueueFull)
}

pub(crate) fn open_cluster_ender_chest_screen(
    player: &crate::entity::player::Player,
    title: TextComponent,
) -> Result<u8, InventoryApplyError> {
    use pumpkin_cluster::inventory::{
        INV_CURSOR, INV_MAIN, InvLoc, InventoryCell, InventoryScreenSession, InventoryScreenSlot,
        InventoryScreenSnapshot, InventorySlotRef,
    };

    let server = player
        .world()
        .server
        .upgrade()
        .ok_or(InventoryApplyError::TargetUnavailable)?;
    let actor = player
        .cluster_gid()
        .ok_or(InventoryApplyError::TargetUnavailable)?;
    let player_target = InventoryTarget::Player(actor);
    let ender_target = ender_chest_target(actor);
    let player_state =
        inventory_snapshot(&server, player_target).ok_or(InventoryApplyError::TargetUnavailable)?;
    let ender_state =
        inventory_snapshot(&server, ender_target).ok_or(InventoryApplyError::TargetUnavailable)?;
    player.increment_screen_handler_sync_id();
    let sync_id = player
        .screen_handler_sync_id
        .load(std::sync::atomic::Ordering::Relaxed);
    let session_token = server
        .cluster_inventory_close_token
        .fetch_add(1, std::sync::atomic::Ordering::Relaxed);
    let session = InventoryScreenSession {
        actor,
        session: session_token,
    };
    let mut slots = Vec::with_capacity(63);
    let mut stacks = Vec::with_capacity(63);
    for screen_slot in 0..63 {
        let (target, state, loc) = if screen_slot < 27 {
            (
                ender_target,
                &ender_state,
                InvLoc::new(INV_MAIN, screen_slot as u16),
            )
        } else {
            let index = screen_slot - 27;
            let player_slot = if index < 27 { index + 9 } else { index - 27 };
            (
                player_target,
                &player_state,
                InvLoc::new(INV_MAIN, player_slot as u16),
            )
        };
        let cell = state
            .cells
            .iter()
            .find(|cell| cell.loc == loc)
            .cloned()
            .unwrap_or_else(|| InventoryCell::empty(loc));
        stacks.push(item_stack_from_inventory_cell(&cell)?);
        slots.push(InventoryScreenSlot {
            screen_slot: screen_slot as u16,
            slot: InventorySlotRef::new(target, loc),
            cell,
            limit: 64,
        });
    }
    let cursor_loc = InvLoc::new(INV_CURSOR, 0);
    let cursor = player_state
        .cells
        .iter()
        .find(|cell| cell.loc == cursor_loc)
        .cloned()
        .unwrap_or_else(|| InventoryCell::empty(cursor_loc));
    let position = player.position();
    let affected = vec![chunk_of_block(
        super::cluster_world_delta::world_id(&player.world()),
        position.x.floor() as i32,
        position.z.floor() as i32,
    )];
    let screen = InventoryScreenSnapshot {
        session,
        sync_id,
        window_kind: pumpkin_data::screen::WindowType::Generic9x3 as u16,
        container_slots: 27,
        slots,
        cursor,
        affected,
        preview: None,
    };
    player
        .cluster_current_inventory_session
        .compare_exchange(
            0,
            session_token,
            std::sync::atomic::Ordering::AcqRel,
            std::sync::atomic::Ordering::Acquire,
        )
        .map_err(|_| InventoryApplyError::Rejected)?;
    player
        .cluster_inventory_screen_target
        .store(Some(Arc::new(ender_target)));
    let submitted = open_inventory_screen_with_presentation(
        &server,
        screen,
        None,
        Some(ClusterScreenPresentation {
            title,
            window_type: pumpkin_data::screen::WindowType::Generic9x3,
            stacks,
        }),
    );
    if submitted.is_err() {
        player.cluster_inventory_screen_target.store(None);
        player
            .cluster_current_inventory_session
            .store(0, std::sync::atomic::Ordering::Release);
    }
    submitted?;
    Ok(sync_id)
}

pub(crate) fn close_inventory_screen(
    server: &Server,
    session: pumpkin_cluster::inventory::InventoryScreenSession,
) -> Result<(), InventoryApplyError> {
    server
        .cluster_inventory_install_route_tx
        .borrow()
        .clone()
        .ok_or(InventoryApplyError::TargetUnavailable)?
        .try_send(LocalInventoryLifecycle::CloseScreen { session })
        .map_err(|_| InventoryApplyError::QueueFull)
}

pub(crate) fn begin_departure_inventory_close(
    server: &Server,
    actor: GlobalPlayerId,
    token: u64,
    input_sequence: pumpkin_cluster::identity::PlayerSeq,
) -> Result<(), InventoryApplyError> {
    server
        .cluster_inventory_install_route_tx
        .borrow()
        .clone()
        .ok_or(InventoryApplyError::TargetUnavailable)?
        .try_send(LocalInventoryLifecycle::BeginDeparture {
            actor,
            token,
            input_sequence,
        })
        .map_err(|_| InventoryApplyError::QueueFull)
}

pub(crate) fn install_beacon_state(
    server: &Server,
    target: InventoryTarget,
    state: pumpkin_cluster::inventory::BeaconMenuState,
    handle: Arc<dyn Inventory>,
) -> Result<(), InventoryApplyError> {
    server
        .cluster_inventory_install_route_tx
        .borrow()
        .clone()
        .ok_or(InventoryApplyError::TargetUnavailable)?
        .try_send(LocalInventoryLifecycle::BeaconSnapshot {
            target,
            state,
            handle,
        })
        .map_err(|_| InventoryApplyError::QueueFull)
}

pub(crate) fn beacon_menu_at_open(
    server: &Server,
    handler: &dyn ScreenHandler,
    screen: &pumpkin_cluster::inventory::InventoryScreenSnapshot,
    payment: &InventoryStateSnapshot,
) -> Option<(
    pumpkin_cluster::inventory::InventoryMenuMetadata,
    pumpkin_cluster::inventory::BeaconMenuState,
    Arc<dyn Inventory>,
)> {
    use std::sync::atomic::Ordering;

    let beacon = handler
        .as_any()
        .downcast_ref::<pumpkin_inventory::beacon_screen_handler::BeaconScreenHandler>()?;
    let block = beacon
        .inventory
        .as_any()
        .downcast_ref::<crate::block::entities::beacon::BeaconBlockEntity>()?;
    let state = server
        .cluster_beacon_states
        .load()
        .get(&payment.target)
        .copied()
        .or_else(|| {
            let levels = u8::try_from(block.levels.load(Ordering::Relaxed)).ok()?;
            let primary = block.primary_effect.load(Ordering::Relaxed);
            let secondary = block.secondary_effect.load(Ordering::Relaxed);
            Some(pumpkin_cluster::inventory::BeaconMenuState {
                revision: 0,
                levels,
                primary_effect: u16::try_from(primary).ok(),
                secondary_effect: u16::try_from(secondary).ok(),
            })
        })?;
    let payment_slot = screen.slots.iter().find(|slot| slot.screen_slot == 0)?;
    let payment_valid = payment.cells.iter().any(|cell| {
        cell.loc == payment_slot.slot.loc
            && pumpkin_data::tag::Item::MINECRAFT_BEACON_PAYMENT_ITEMS
                .1
                .contains(&cell.item)
    });
    let menu =
        pumpkin_cluster::inventory::beacon_menu_metadata(screen, payment, state, payment_valid)
            .ok()?;
    Some((menu, state, beacon.inventory.clone()))
}

pub(crate) fn remove_inventory(
    server: &Server,
    target: InventoryTarget,
) -> Result<(), InventoryApplyError> {
    server
        .cluster_inventory_install_route_tx
        .borrow()
        .clone()
        .ok_or(InventoryApplyError::TargetUnavailable)?
        .try_send(LocalInventoryLifecycle::Remove { target })
        .map_err(|_| InventoryApplyError::QueueFull)?;
    Ok(())
}

pub(crate) fn inventory_cell_from_item_stack(
    loc: pumpkin_cluster::inventory::InvLoc,
    stack: &ItemStack,
) -> Result<pumpkin_cluster::inventory::InventoryCell, InventoryApplyError> {
    if stack.is_empty() {
        return Ok(pumpkin_cluster::inventory::InventoryCell::empty(loc));
    }
    let mut compound = pumpkin_nbt::compound::NbtCompound::new();
    let mut template = stack.clone();
    template.item_count = 1;
    template.write_item_stack(&mut compound);
    let nbt = Nbt::new(String::new(), compound)
        .write_unnamed_canonical()
        .map_err(|_| InventoryApplyError::Rejected)?
        .to_vec();
    if nbt.len() > INVSEE_MAX_SLOT_NBT_BYTES {
        return Err(InventoryApplyError::Rejected);
    }
    Ok(pumpkin_cluster::inventory::InventoryCell {
        loc,
        item: stack.item.id,
        count: stack.item_count,
        nbt,
    })
}

pub(crate) fn player_inventory_snapshot_from_nbt(
    target: GlobalPlayerId,
    data: Option<&pumpkin_nbt::compound::NbtCompound>,
) -> Result<InventoryStateSnapshot, InventoryApplyError> {
    let mut stacks = vec![ItemStack::EMPTY.clone(); 41];
    let selected = data
        .and_then(|data| data.get_int("SelectedItemSlot"))
        .and_then(|slot| u8::try_from(slot).ok())
        .filter(|slot| *slot < 9)
        .unwrap_or(0);
    if let Some(items) = data.and_then(|data| data.get_list("Inventory")) {
        for tag in items {
            let Some(compound) = tag.extract_compound() else {
                continue;
            };
            let index = match compound.get_byte("Slot") {
                Some(100) => Some(36),
                Some(101) => Some(37),
                Some(102) => Some(38),
                Some(103) => Some(39),
                Some(-106) => Some(40),
                Some(slot @ 0..=40) => Some(slot as usize),
                _ => None,
            };
            if let (Some(index), Some(stack)) = (index, ItemStack::read_item_stack(compound)) {
                stacks[index] = stack;
            }
        }
    }
    if let Some(equipment) = data.and_then(|data| data.get_compound("equipment")) {
        for (name, index) in [
            ("offhand", 40),
            ("head", 39),
            ("chest", 38),
            ("legs", 37),
            ("feet", 36),
        ] {
            if let Some(stack) = equipment
                .get_compound(name)
                .and_then(ItemStack::read_item_stack)
            {
                stacks[index] = stack;
            }
        }
    }
    let mut cells = Vec::new();
    for (index, stack) in stacks.iter().enumerate() {
        let Some(loc) = player_inventory_loc(index as u16) else {
            continue;
        };
        let cell = inventory_cell_from_item_stack(loc, stack)?;
        if !cell.is_empty() {
            cells.push(cell);
        }
    }
    cells.push(pumpkin_cluster::inventory::InventoryCell {
        loc: pumpkin_cluster::inventory::InvLoc::new(INV_SELECTED, 0),
        item: u16::from(selected) + 1,
        count: 1,
        nbt: Vec::new(),
    });
    let revision = pumpkin_cluster::inventory::inventory_content_revision(selected, &cells)
        .map_err(|_| InventoryApplyError::Rejected)?;
    Ok(InventoryStateSnapshot {
        target: InventoryTarget::Player(target),
        revision,
        cells,
    })
}

pub(crate) fn ender_chest_stacks_from_nbt(
    data: Option<&pumpkin_nbt::compound::NbtCompound>,
) -> [ItemStack;
    pumpkin_inventory::player::ender_chest_inventory::EnderChestInventory::INVENTORY_SIZE] {
    let mut stacks = std::array::from_fn(|_| ItemStack::EMPTY.clone());
    if let Some(items) = data.and_then(|data| data.get_list("EnderItems")) {
        for tag in items {
            if let Some(compound) = tag.extract_compound()
                && let Some(slot @ 0..=26) = compound.get_byte("Slot")
                && let Some(stack) = ItemStack::read_item_stack(compound)
            {
                stacks[slot as usize] = stack;
            }
        }
    }
    stacks
}

pub(crate) fn ender_chest_target(gid: GlobalPlayerId) -> InventoryTarget {
    InventoryTarget::Virtual {
        host: gid.server,
        owner: pumpkin_cluster::actor::ActorId::Player(gid),
        session: 0,
        kind: 0x100,
    }
}

pub(crate) fn ender_chest_snapshot_from_nbt(
    gid: GlobalPlayerId,
    data: Option<&pumpkin_nbt::compound::NbtCompound>,
) -> Result<InventoryStateSnapshot, InventoryApplyError> {
    let mut cells = Vec::new();
    for (index, stack) in ender_chest_stacks_from_nbt(data).iter().enumerate() {
        let cell = inventory_cell_from_item_stack(
            pumpkin_cluster::inventory::InvLoc::new(INV_MAIN, index as u16),
            stack,
        )?;
        if !cell.is_empty() {
            cells.push(cell);
        }
    }
    let revision = pumpkin_cluster::inventory::inventory_content_revision(0, &cells)
        .map_err(|_| InventoryApplyError::Rejected)?;
    Ok(InventoryStateSnapshot {
        target: ender_chest_target(gid),
        revision,
        cells,
    })
}

pub(crate) fn ender_chest_stacks_from_state(
    snapshot: &InventoryStateSnapshot,
) -> Result<[ItemStack; 27], InventoryApplyError> {
    let mut stacks = std::array::from_fn(|_| ItemStack::EMPTY.clone());
    for cell in &snapshot.cells {
        if cell.loc.inv != INV_MAIN || cell.loc.slot >= 27 {
            return Err(InventoryApplyError::Rejected);
        }
        stacks[cell.loc.slot as usize] = item_stack_from_inventory_cell(cell)?;
    }
    Ok(stacks)
}

async fn owner_task(server: Arc<Server>, mut commands: mpsc::Receiver<InvseeCommand>) {
    let mut publications = subscribe_inventory_publications(&server);
    let mut owner = InvseeOwner::new(server);
    let mut renewal = tokio::time::interval(Duration::from_secs(60));
    let mut deferred_retry = tokio::time::interval(Duration::from_millis(50));
    loop {
        let command = tokio::select! {
            command = commands.recv() => command,
            _ = deferred_retry.tick(), if !owner.pending_refresh.is_empty() || !owner.deferred_refresh.is_empty() || !owner.owner_refresh_inflight.is_empty() => {
                owner.flush_deferred_refresh();
                continue;
            }
            _ = renewal.tick() => {
                owner.renew_subscriptions();
                continue;
            }
            resolution = publications.recv() => {
                if let Ok(resolution) = resolution {
                    owner.resolution(resolution);
                }
                continue;
            }
        };
        let Some(command) = command else {
            break;
        };
        match command {
            InvseeCommand::Configure { local, outbound } => owner.configure(local, outbound),
            InvseeCommand::Snapshot {
                viewer,
                target,
                target_name,
                editable,
                reply,
            } => owner.begin_snapshot(viewer, target, target_name, editable, reply),
            InvseeCommand::Cancel(request_id) => {
                owner.pending.remove(&request_id);
            }
            InvseeCommand::Refresh {
                session,
                target,
                expected_revision,
            } => owner.begin_refresh(session, target, expected_revision),
            InvseeCommand::Subscribe(subscription) => owner.begin_subscription(subscription),
            InvseeCommand::Unsubscribe(subscription) => owner.end_subscription(subscription),
            InvseeCommand::TargetChanged(target, revision) => {
                owner.invalidate_subscribers(target, Some(revision), false);
            }
            InvseeCommand::TargetRemoved(target) => {
                owner.invalidate_subscribers(target, None, true);
            }
            InvseeCommand::TargetRelocated(target) => {
                let revision = inventory_snapshot(&owner.server, InventoryTarget::Player(target))
                    .map(|snapshot| snapshot.revision);
                owner.invalidate_subscribers(target, revision, true);
            }
            InvseeCommand::RefreshApplied {
                session,
                target,
                revision,
            } => owner.refresh_applied(session, target, revision),
            InvseeCommand::RefreshRejected {
                session,
                target,
                expected_revision,
                retry,
            } => owner.refresh_rejected(session, target, expected_revision, retry),
            InvseeCommand::Write(write) => owner.submit_write(write),
            InvseeCommand::SnapshotResponse(response) => owner.send_response(response),
            InvseeCommand::Inbound(parcel) => owner.inbound(parcel),
        }
    }
}

pub(crate) fn spawn_invsee_owner(server: &Arc<Server>, commands: mpsc::Receiver<InvseeCommand>) {
    server.spawn_task(owner_task(server.clone(), commands));
}

pub(crate) fn attach_inventory_action_owner(
    server: &Server,
) -> mpsc::Receiver<LocalInventoryAction> {
    let (tx, rx) = mpsc::channel(256);
    server
        .cluster_inventory_action_route_tx
        .send_replace(Some(tx));
    rx
}

pub(crate) fn attach_inventory_screen_owner(
    server: &Server,
) -> mpsc::Receiver<LocalInventoryScreenCommand> {
    let (tx, rx) = mpsc::channel(256);
    server
        .cluster_inventory_screen_route_tx
        .send_replace(Some(tx));
    rx
}

pub(crate) fn submit_inventory_screen_command(
    server: &Server,
    command: pumpkin_cluster::inventory::InventoryScreenCommand,
    tick: TickStamp,
    affected: Vec<pumpkin_cluster::protocol::ChunkKey>,
    origin: Option<u64>,
) -> Result<(), InventoryApplyError> {
    submit_inventory_screen_command_with_sequence(server, command, tick, affected, origin, None)
}

pub(crate) fn submit_inventory_screen_command_with_sequence(
    server: &Server,
    command: pumpkin_cluster::inventory::InventoryScreenCommand,
    tick: TickStamp,
    affected: Vec<pumpkin_cluster::protocol::ChunkKey>,
    origin: Option<u64>,
    input_sequence: Option<pumpkin_cluster::identity::PlayerSeq>,
) -> Result<(), InventoryApplyError> {
    submit_inventory_screen_command_with_sequence_and_close_token(
        server,
        command,
        tick,
        affected,
        origin,
        input_sequence,
        None,
    )
}

pub(crate) fn submit_inventory_screen_command_with_sequence_and_close_token(
    server: &Server,
    command: pumpkin_cluster::inventory::InventoryScreenCommand,
    tick: TickStamp,
    affected: Vec<pumpkin_cluster::protocol::ChunkKey>,
    origin: Option<u64>,
    input_sequence: Option<pumpkin_cluster::identity::PlayerSeq>,
    close_token: Option<u64>,
) -> Result<(), InventoryApplyError> {
    server
        .cluster_inventory_screen_route_tx
        .borrow()
        .clone()
        .ok_or(InventoryApplyError::TargetUnavailable)?
        .try_send(LocalInventoryScreenCommand {
            command,
            tick,
            affected,
            origin,
            input_sequence,
            close_token,
        })
        .map_err(|_| InventoryApplyError::QueueFull)
}

pub(crate) fn attach_inventory_snapshot_owner(
    server: &Server,
) -> mpsc::Receiver<LocalInventorySnapshotRequest> {
    let (tx, rx) = mpsc::channel(256);
    server
        .cluster_inventory_snapshot_route_tx
        .send_replace(Some(tx));
    rx
}

pub(crate) fn attach_inventory_install_owner(
    server: &Server,
) -> mpsc::Receiver<LocalInventoryLifecycle> {
    let (tx, rx) = mpsc::channel(256);
    server
        .cluster_inventory_install_route_tx
        .send_replace(Some(tx));
    rx
}

pub(crate) fn configure(server: &Server, local: ServerId, outbound: mpsc::Sender<OutboundParcel>) {
    let _ = server
        .cluster_invsee_tx
        .try_send(InvseeCommand::Configure { local, outbound });
}

pub(crate) fn submit_remote_write(server: &Server, write: InvseeWrite) {
    let _ = server
        .cluster_invsee_tx
        .try_send(InvseeCommand::Write(write));
}

pub async fn request_remote_snapshot(
    server: &Arc<Server>,
    viewer: GlobalPlayerId,
    target_name: &str,
    editable: bool,
) -> Option<InventorySnapshot> {
    if !server.advanced_config.cluster.enabled || target_name.trim().is_empty() {
        return None;
    }
    let (reply, pending) = oneshot::channel();
    server
        .cluster_invsee_tx
        .try_send(InvseeCommand::Snapshot {
            viewer,
            target: None,
            target_name: target_name.trim().to_string(),
            editable,
            reply,
        })
        .ok()?;
    let pending = pending.await.ok()?;
    let response = tokio::time::timeout(
        Duration::from_millis(INVSEE_REQUEST_TIMEOUT_MS),
        pending.response,
    )
    .await;
    match response {
        Ok(Ok(snapshot)) => snapshot,
        _ => {
            let _ = server
                .cluster_invsee_tx
                .try_send(InvseeCommand::Cancel(pending.request_id));
            None
        }
    }
}

pub(crate) async fn request_inventory_snapshot(
    server: &Arc<Server>,
    viewer: GlobalPlayerId,
    target: GlobalPlayerId,
) -> Option<InventorySnapshot> {
    if target.server == ServerId(server.advanced_config.cluster.server_id) {
        let owner = InvseeOwner::new(server.clone());
        let player = owner.find_player(target)?;
        if player.is_in_cluster_lobby() {
            return None;
        }
        let owner_name = player.gameprofile.name.clone();
        let selected = player.inventory.get_selected_slot();
        let (reply, response) = oneshot::channel();
        server
            .cluster_inventory_snapshot_route_tx
            .borrow()
            .clone()?
            .try_send(LocalInventorySnapshotRequest {
                target: InventoryTarget::Player(target),
                reply,
            })
            .ok()?;
        let state =
            tokio::time::timeout(Duration::from_millis(INVSEE_REQUEST_TIMEOUT_MS), response)
                .await
                .ok()?
                .ok()??;
        let position = player.position();
        let chunk = chunk_of_block(
            super::cluster_world_delta::world_id(&player.world()),
            position.x.floor() as i32,
            position.z.floor() as i32,
        );
        return Some(
            inventory_snapshot_from_state(state, owner_name, selected).with_affected(vec![chunk]),
        );
    }
    let (reply, pending) = oneshot::channel();
    server
        .cluster_invsee_tx
        .try_send(InvseeCommand::Snapshot {
            viewer,
            target: Some(target),
            target_name: String::new(),
            editable: true,
            reply,
        })
        .ok()?;
    let pending = pending.await.ok()?;
    let response = tokio::time::timeout(
        Duration::from_millis(INVSEE_REQUEST_TIMEOUT_MS),
        pending.response,
    )
    .await;
    match response {
        Ok(Ok(snapshot)) => snapshot,
        _ => {
            let _ = server
                .cluster_invsee_tx
                .try_send(InvseeCommand::Cancel(pending.request_id));
            None
        }
    }
}
