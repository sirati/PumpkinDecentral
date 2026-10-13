use std::collections::BTreeMap;

#[cfg(test)]
use super::cluster_remote_player_packets::prepare_frame;
use super::cluster_remote_player_packets::{prepare_remove, prepare_swing};

use pumpkin_cluster::canonical_action::{LivePlayerLease, TruthView};
use pumpkin_cluster::identity::GlobalPlayerId;
use pumpkin_cluster::inventory::{
    INV_ARMOR, INV_MAIN, INV_OFFHAND, INV_SELECTED, InvLoc, InventoryCell, InventoryTarget,
};
use pumpkin_cluster::item_action::ActorLivingState;
use pumpkin_cluster::presence::RemotePlayerEntry;
use pumpkin_cluster::realm_completion::PeerGeneration;

#[derive(Clone, Copy, Debug)]
pub(crate) struct RemotePlayerMovement {
    pub source: PeerGeneration,
    pub update: pumpkin_cluster::protocol::PosUpdate,
}

pub use super::cluster_player_cosmetics::CosmeticChange;
pub(crate) use super::cluster_player_cosmetics::{LocalPlayerCosmetic, submit_cosmetic};
pub(super) use super::cluster_player_cosmetics::{capture_cosmetic, try_send_cosmetic};

pub(crate) fn submit_movement(server: &super::Server, input: RemotePlayerMovement) -> bool {
    server
        .cluster_remote_player_movement_route
        .load_full()
        .is_some_and(|route| route.try_send(input).is_ok())
}

#[derive(Clone)]
pub(crate) struct RemotePlayerSkin(pub std::sync::Arc<pumpkin_protocol::bedrock::client::Skin>);

impl std::fmt::Debug for RemotePlayerSkin {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter
            .debug_tuple("RemotePlayerSkin")
            .field(&self.0.skin_id)
            .finish()
    }
}

impl PartialEq for RemotePlayerSkin {
    fn eq(&self, other: &Self) -> bool {
        std::sync::Arc::ptr_eq(&self.0, &other.0)
    }
}

#[derive(Clone)]
pub(crate) struct RemotePlayerEnder(
    std::sync::Arc<pumpkin_cluster::replica_inventory::PlayerEnderSnapshot>,
);
impl std::fmt::Debug for RemotePlayerEnder {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter
            .debug_tuple("RemotePlayerEnder")
            .field(&self.0.lease)
            .field(&self.0.sequence)
            .field(&self.0.revision)
            .finish()
    }
}
impl PartialEq for RemotePlayerEnder {
    fn eq(&self, other: &Self) -> bool {
        std::sync::Arc::ptr_eq(&self.0, &other.0)
    }
}

#[derive(Clone, Debug, PartialEq)]
pub(crate) struct RemotePlayerFrame {
    pub(super) packet_image: u64,
    pub lease: LivePlayerLease,
    pub source: PeerGeneration,
    pub client_entity_id: i32,
    pub profile: std::sync::Arc<RemotePlayerEntry>,
    pub bedrock_skin: Option<RemotePlayerSkin>,
    ender: Option<RemotePlayerEnder>,
    pub mask_health: bool,
    pub health_policy: (
        Option<PeerGeneration>,
        pumpkin_cluster::health_policy::HealthPolicySnapshot,
    ),
    pub spatial: super::cluster_world_actor::ActorSpatialState,
    pub living: std::sync::Arc<ActorLivingState>,
    pub equipment: Option<std::sync::Arc<[InventoryCell; 6]>>,
    inventory_cells: Option<std::sync::Arc<Vec<InventoryCell>>>,
    inventory_revision: Option<(u64, u64)>,
    pub skin_layers: Option<u8>,
    pub main_hand: Option<u8>,
}

#[derive(Default)]
pub(crate) struct RemotePlayerDirectory {
    uuids: std::collections::HashMap<uuid::Uuid, Option<GlobalPlayerId>>,
    players: std::collections::HashMap<
        GlobalPlayerId,
        std::sync::Arc<crate::entity::cluster_player_ghost::RemotePlayerReplica>,
    >,
    worlds: std::collections::HashMap<
        pumpkin_cluster::protocol::WorldId,
        std::sync::Arc<Vec<std::sync::Arc<dyn crate::entity::EntityBase>>>,
    >,
}

impl RemotePlayerDirectory {
    pub(crate) fn by_uuid(
        &self,
        uuid: uuid::Uuid,
    ) -> Option<&std::sync::Arc<crate::entity::cluster_player_ghost::RemotePlayerReplica>> {
        self.players.get(self.uuids.get(&uuid)?.as_ref()?)
    }

    pub(crate) fn get(
        &self,
        gid: &GlobalPlayerId,
    ) -> Option<&std::sync::Arc<crate::entity::cluster_player_ghost::RemotePlayerReplica>> {
        self.players.get(gid)
    }

    pub(crate) fn values(
        &self,
    ) -> impl Iterator<Item = &std::sync::Arc<crate::entity::cluster_player_ghost::RemotePlayerReplica>>
    {
        self.players.values()
    }

    pub(crate) fn world_entities(
        &self,
        world: pumpkin_cluster::protocol::WorldId,
    ) -> Option<std::sync::Arc<Vec<std::sync::Arc<dyn crate::entity::EntityBase>>>> {
        self.worlds.get(&world).cloned()
    }
}

impl RemotePlayerFrame {
    pub(crate) fn ender_cells(&self) -> Option<&[InventoryCell; 27]> {
        self.ender.as_ref()?.0.cells.as_deref()
    }
    pub(crate) fn canonical_ender_cells(
        &self,
        server: &super::Server,
    ) -> Option<&[InventoryCell; 27]> {
        let cells = self.ender_cells()?;
        super::cluster_player_ender::matches_canonical_root(server, self.lease, cells)
            .then_some(cells)
    }
    pub(crate) fn inventory_cells(&self) -> Option<&[InventoryCell]> {
        self.inventory_cells.as_deref().map(Vec::as_slice)
    }

    pub(crate) fn stance(&self) -> pumpkin_cluster::item_action::PlayerStance {
        self.living
            .avatar
            .as_ref()
            .map_or_else(Default::default, |avatar| avatar.stance)
    }
    pub(crate) fn pose(&self) -> pumpkin_data::entity::EntityPose {
        super::cluster_living_metadata::player_pose(&self.living)
    }
    pub(crate) fn same_session(&self, other: &Self) -> bool {
        self.lease == other.lease
            && self.source == other.source
            && self.client_entity_id == other.client_entity_id
    }

    fn rendered_equal(&self, other: &Self) -> bool {
        let before = self.spatial.frame.effective;
        let after = other.spatial.frame.effective;
        self.same_session(other)
            && self.profile == other.profile
            && self.bedrock_skin == other.bedrock_skin
            && self.mask_health == other.mask_health
            && self.health_policy == other.health_policy
            && self.spatial.world == other.spatial.world
            && self.spatial.game_mode == other.spatial.game_mode
            && before.position == after.position
            && before.velocity == after.velocity
            && before.yaw == after.yaw
            && before.pitch == after.pitch
            && before.on_ground == after.on_ground
            && self.spatial.head_yaw == other.spatial.head_yaw
            && self.spatial.sneaking == other.spatial.sneaking
            && self.spatial.sprinting == other.spatial.sprinting
            && self.spatial.crawling == other.spatial.crawling
            && self.spatial.swimming == other.spatial.swimming
            && self.spatial.gliding == other.spatial.gliding
            && self.stance() == other.stance()
            && self.living.combat.health_milli == other.living.combat.health_milli
            && self.living.combat.absorption_milli == other.living.combat.absorption_milli
            && self.living.combat.velocity_micro == other.living.combat.velocity_micro
            && (self.living.combat.timing.fire_ticks != 0)
                == (other.living.combat.timing.fire_ticks != 0)
            && self.living.use_state == other.living.use_state
            && self.living.environment == other.living.environment
            && super::cluster_living_metadata::player_effects_presentation_equal(
                &self.living,
                &other.living,
            )
            && self
                .living
                .avatar
                .as_ref()
                .map(|avatar| &avatar.presentation)
                == other
                    .living
                    .avatar
                    .as_ref()
                    .map(|avatar| &avatar.presentation)
            && self.skin_layers == other.skin_layers
            && self.main_hand == other.main_hand
            && self.equipment == other.equipment
            && super::cluster_living_metadata::player_attributes_presentation_equal(
                &self.living,
                &other.living,
            )
    }
}

fn equipment_from_cells(cells: &[InventoryCell]) -> Option<std::sync::Arc<[InventoryCell; 6]>> {
    let cell = |inv, slot| {
        cells
            .iter()
            .find(|cell| cell.loc == InvLoc { inv, slot })
            .cloned()
    };
    let selected = cell(INV_SELECTED, 0)?;
    if selected.count != 1 || !(1..=9).contains(&selected.item) || !selected.nbt.is_empty() {
        return None;
    }
    Some(std::sync::Arc::new([
        cell(INV_MAIN, selected.item - 1)?,
        cell(INV_OFFHAND, 0)?,
        cell(INV_ARMOR, 0)?,
        cell(INV_ARMOR, 1)?,
        cell(INV_ARMOR, 2)?,
        cell(INV_ARMOR, 3)?,
    ]))
}

pub(super) struct OwnedRemotePlayerPresentation {
    next_packet_image: u64,
    frames: BTreeMap<GlobalPlayerId, RemotePlayerFrame>,
    ender_heads: BTreeMap<
        GlobalPlayerId,
        std::sync::Arc<pumpkin_cluster::replica_inventory::PlayerEnderSnapshot>,
    >,
    retained_visual: Option<pumpkin_cluster::streams::InboundParcel>,
    identities: BTreeMap<GlobalPlayerId, super::cluster_replica_binding::ReplicaBindingNotice>,
    movement: tokio::sync::mpsc::Receiver<RemotePlayerMovement>,
    visual_transport: Option<tokio::sync::mpsc::Receiver<pumpkin_cluster::streams::InboundParcel>>,
    visual_diagnostics:
        BTreeMap<(pumpkin_cluster::identity::ServerId, &'static str), std::time::Instant>,
    cosmetics: BTreeMap<GlobalPlayerId, pumpkin_cluster::replica_visual::PlayerVisualEnvelope>,
    pending_visual:
        std::collections::VecDeque<pumpkin_cluster::replica_visual::PlayerVisualEnvelope>,
    local_cosmetics: super::cluster_player_cosmetics::OwnedPlayerCosmetics,
    last_presence: Option<std::sync::Arc<super::cluster_presence::PresenceSnapshot>>,
    viewers: BTreeMap<(GlobalPlayerId, u64, u32), BTreeMap<GlobalPlayerId, RemotePlayerFrame>>,
}

impl OwnedRemotePlayerPresentation {
    pub(super) fn visible_player_id(
        &self,
        viewer: GlobalPlayerId,
        install_generation: u64,
        epoch: u32,
        lease: LivePlayerLease,
        source: PeerGeneration,
    ) -> Option<i32> {
        delivered_player_id(
            &self.viewers,
            (viewer, install_generation, epoch),
            lease,
            source,
        )
    }

    pub(super) fn new(
        server: &super::Server,
        outbound: tokio::sync::mpsc::Sender<pumpkin_cluster::streams::OutboundParcel>,
    ) -> Self {
        let (tx, movement) = tokio::sync::mpsc::channel(4096);
        server
            .cluster_remote_player_movement_route
            .store(Some(std::sync::Arc::new(tx)));
        let local_cosmetics =
            super::cluster_player_cosmetics::OwnedPlayerCosmetics::new(server, outbound);
        Self {
            frames: BTreeMap::new(),
            ender_heads: BTreeMap::new(),
            next_packet_image: 0,
            retained_visual: None,
            identities: BTreeMap::new(),
            movement,
            visual_transport: None,
            visual_diagnostics: BTreeMap::new(),
            cosmetics: BTreeMap::new(),
            pending_visual: std::collections::VecDeque::new(),
            viewers: BTreeMap::new(),
            local_cosmetics,
            last_presence: None,
        }
    }

    pub(super) fn drain_movement(
        &mut self,
        server: &super::Server,
        actors: &mut super::cluster_world_actor::OwnedActors,
        living: &super::cluster_world_actor::OwnedLivingActors,
    ) {
        let generations = server.cluster_peer_incarnations.load();
        let held = server.cluster_owned_chunk_blocks.load();
        while let Ok(input) = self.movement.try_recv() {
            if generations.get(&input.source.server) != Some(&input.source.generation) {
                continue;
            }
            let Some(lease) = actors.remote_lease(input.update.gid) else {
                continue;
            };
            let Some(state) =
                living.phase_state(pumpkin_cluster::actor::ActorId::Player(lease.gid), false)
            else {
                continue;
            };
            let Some(avatar) = state.avatar.as_ref() else {
                continue;
            };
            let Some(spatial) = actors.phase_spatial(lease.gid, false) else {
                continue;
            };
            if !held.contains_key(&spatial.chunk) {
                continue;
            }
            if actors.update_remote_movement(
                input.source,
                lease,
                avatar.spatial_epoch,
                input.update,
            ) {
                super::cluster_ghost::record_movement_applied();
            }
        }
    }

    fn accept_ender(
        &mut self,
        server: &super::Server,
        snapshot: std::sync::Arc<pumpkin_cluster::replica_inventory::PlayerEnderSnapshot>,
    ) {
        if !snapshot.valid()
            || server
                .cluster_peer_incarnations
                .load()
                .get(&snapshot.source.server)
                != Some(&snapshot.source.generation)
        {
            return;
        }
        if self
            .ender_heads
            .get(&snapshot.lease.gid)
            .is_some_and(|previous| !snapshot.is_newer_than(previous))
        {
            return;
        }
        if !self.ender_heads.contains_key(&snapshot.lease.gid)
            && self.ender_heads.len() >= pumpkin_cluster::canonical_action::MAX_LOGICAL_RECORDS
        {
            let key = (snapshot.source.server, "ender capacity");
            if self
                .visual_diagnostics
                .get(&key)
                .is_none_or(|last| last.elapsed().as_secs() >= 300)
            {
                tracing::warn!(?snapshot.lease, "cluster ender snapshot capacity exhausted");
                self.visual_diagnostics
                    .insert(key, std::time::Instant::now());
            }
            return;
        }
        self.ender_heads.insert(snapshot.lease.gid, snapshot);
    }

    fn drain_visual(
        &mut self,
        server: &super::Server,
        actors: &super::cluster_world_actor::OwnedActors,
        living: &super::cluster_world_actor::OwnedLivingActors,
    ) -> Vec<pumpkin_cluster::replica_visual::PlayerVisualEnvelope> {
        let generations = server.cluster_peer_incarnations.load();
        let mut pulses = Vec::new();
        let held = server.cluster_owned_chunk_blocks.load();
        let frames = &self.frames;
        self.cosmetics.retain(|gid, envelope| {
            generations.get(&envelope.source.server) == Some(&envelope.source.generation)
                && !actors.remote_lease_retired(envelope.lease, envelope.source)
                && actors
                    .phase_spatial(envelope.lease.gid, false)
                    .map(|spatial| spatial.chunk)
                    .or_else(|| frames.get(gid).map(|frame| frame.spatial.chunk))
                    .is_none_or(|chunk| held.contains_key(&chunk))
        });
        if self.visual_transport.is_none() {
            self.visual_transport = server.cluster_remote_player_visual_inbox.pop();
        }
        let mut received_bytes = 0_usize;
        for _ in 0..4096 {
            if self.pending_visual.len() >= 4096 {
                break;
            }
            let parcel = match self.retained_visual.take().or_else(|| {
                self.visual_transport
                    .as_mut()
                    .and_then(|receiver| receiver.try_recv().ok())
            }) {
                Some(parcel) => parcel,
                None => break,
            };
            let next_bytes = received_bytes.checked_add(parcel.bytes.len());
            if next_bytes.is_none_or(|bytes| {
                bytes > pumpkin_cluster::canonical_action::MAX_LOGICAL_ACTION_BYTES
            }) {
                self.retained_visual = Some(parcel);
                break;
            }
            received_bytes = next_bytes.unwrap();
            let decoded = if pumpkin_cluster::replica_inventory::PlayerEnderSnapshot::is_wire(
                &parcel.bytes,
            ) {
                super::cluster_visual::decode_ender(server, &parcel).map(|snapshot| {
                    self.accept_ender(server, std::sync::Arc::new(snapshot));
                    None
                })
            } else {
                super::cluster_visual::decode_visual(server, &parcel).map(Some)
            };
            match decoded {
                Ok(Some(envelope)) => self.pending_visual.push_back(envelope),
                Ok(None) => {}
                Err(reason) => {
                    let key = (parcel.peer, reason);
                    if self
                        .visual_diagnostics
                        .get(&key)
                        .is_none_or(|last| last.elapsed().as_secs() >= 300)
                    {
                        tracing::warn!(
                            peer = parcel.peer.0,
                            stream = "player-visual",
                            record_count = 0,
                            reason,
                            "cluster visual envelope rejected"
                        );
                        self.visual_diagnostics
                            .insert(key, std::time::Instant::now());
                    }
                }
            }
        }
        for _ in 0..self.pending_visual.len() {
            let Some(envelope) = self.pending_visual.pop_front() else {
                break;
            };
            if !envelope.valid()
                || generations.get(&envelope.source.server) != Some(&envelope.source.generation)
                || actors.remote_lease_retired(envelope.lease, envelope.source)
            {
                continue;
            }
            if actors
                .player_client_identity(envelope.lease.gid, envelope.source)
                .is_some_and(|(lease, _)| {
                    lease.incarnation > envelope.lease.incarnation
                        || (lease.incarnation == envelope.lease.incarnation
                            && lease != envelope.lease)
                })
            {
                continue;
            }
            let actor = pumpkin_cluster::actor::ActorId::Player(envelope.lease.gid);
            let avatar = living
                .phase_state(actor, false)
                .and_then(|state| state.avatar.as_ref());
            if actors
                .player_client_identity(envelope.lease.gid, envelope.source)
                .map(|(lease, _)| lease)
                != Some(envelope.lease)
                || avatar.is_none_or(|avatar| avatar.spatial_epoch != envelope.epoch)
            {
                let settled = avatar
                    .zip(
                        living
                            .phase_state(actor, true)
                            .and_then(|state| state.avatar.as_ref()),
                    )
                    .is_some_and(|(local, ground)| {
                        local.spatial_epoch == ground.spatial_epoch
                            && ground.movement_tick.is_after(envelope.tick)
                    });
                if !settled {
                    self.pending_visual.push_back(envelope);
                }
                continue;
            }
            if self
                .cosmetics
                .get(&envelope.lease.gid)
                .is_some_and(|previous| stale_visual(previous, &envelope))
            {
                continue;
            }
            let mut stored = envelope.clone();
            stored.swings.clear();
            stored.break_animation = None;
            if stored.main_hand.is_none() {
                stored.main_hand = self
                    .cosmetics
                    .get(&envelope.lease.gid)
                    .filter(|previous| {
                        previous.lease == envelope.lease && previous.source == envelope.source
                    })
                    .and_then(|previous| previous.main_hand);
            }
            if stored.skin_layers.is_none() {
                stored.skin_layers = self
                    .cosmetics
                    .get(&envelope.lease.gid)
                    .filter(|previous| {
                        previous.lease == envelope.lease && previous.source == envelope.source
                    })
                    .and_then(|previous| previous.skin_layers);
            }
            self.cosmetics.insert(envelope.lease.gid, stored);
            super::cluster_visual::record_applied();
            if !envelope.swings.is_empty() || envelope.break_animation.is_some() {
                pulses.push(envelope);
            }
        }
        pulses
    }

    pub(super) fn ensure_bindings(
        &mut self,
        server: &super::Server,
        actors: &mut super::cluster_world_actor::OwnedActors,
        living: &super::cluster_world_actor::OwnedLivingActors,
    ) {
        let generations = server.cluster_peer_incarnations.load();
        let mut wanted = BTreeMap::new();
        for (gid, _, _, _) in actors.player_observations(living) {
            if living
                .phase_state(pumpkin_cluster::actor::ActorId::Player(gid), false)
                .is_none()
                || actors.phase_spatial(gid, false).is_none()
            {
                continue;
            }
            let Some(generation) = generations.get(&gid.server).copied() else {
                continue;
            };
            let source = PeerGeneration {
                server: gid.server,
                generation,
            };
            let Some(lease) = actors.remote_lease(gid) else {
                continue;
            };
            wanted.insert(gid, (lease, source));
        }
        for (&gid, notice) in &self.identities {
            if wanted.get(&gid) != Some(&(notice.lease, notice.source)) {
                actors.update_remote_client_binding(
                    super::cluster_replica_binding::ReplicaBindingNotice {
                        retired: true,
                        ..*notice
                    },
                );
            }
        }
        self.identities
            .retain(|gid, notice| wanted.get(gid) == Some(&(notice.lease, notice.source)));
        for (gid, (lease, source)) in wanted {
            if actors.remote_client_entity_id(lease, source).is_some() {
                continue;
            }
            let notice = if let Some(notice) = self.identities.get(&gid).copied() {
                notice
            } else {
                let Some(client_entity_id) = crate::entity::Entity::try_reserve_ids(1) else {
                    break;
                };
                let notice = super::cluster_replica_binding::ReplicaBindingNotice {
                    lease,
                    source,
                    client_entity_id,
                    retired: false,
                };
                self.identities.insert(gid, notice);
                notice
            };
            actors.update_remote_client_binding(notice);
        }
    }

    pub(super) fn reconcile(
        &mut self,
        server: &super::Server,
        actors: &super::cluster_world_actor::OwnedActors,
        living: &super::cluster_world_actor::OwnedLivingActors,
        inventory: &super::cluster_world_inventory::InventoryOwner,
    ) -> BTreeMap<GlobalPlayerId, RemotePlayerFrame> {
        let mut next = BTreeMap::new();
        let generations = server.cluster_peer_incarnations.load();
        let presence = server.cluster_presence.snapshot();
        let held = server.cluster_owned_chunk_blocks.load();
        let skins = server.cluster_profile_skins.load();
        let health_policy = super::cluster_health_policy::render_cut(server);
        let mask_health =
            health_policy.1.mode != pumpkin_cluster::health_policy::HealthMaskMode::Off;
        let previous_frames = &self.frames;
        self.ender_heads.retain(|gid, snapshot| {
            if generations.get(&snapshot.source.server) != Some(&snapshot.source.generation)
                || actors.remote_lease_retired(snapshot.lease, snapshot.source)
            {
                return false;
            }
            let association = actors
                .player_client_identity(*gid, snapshot.source)
                .filter(|(lease, _)| *lease == snapshot.lease)
                .and_then(|_| {
                    actors
                        .phase_spatial(*gid, false)
                        .map(|spatial| spatial.chunk)
                })
                .or_else(|| {
                    previous_frames
                        .get(gid)
                        .filter(|frame| {
                            frame.lease == snapshot.lease
                                && frame.source == snapshot.source
                                && frame.profile.session == snapshot.install_generation
                        })
                        .map(|frame| frame.spatial.chunk)
                });
            let retain = association.is_none_or(|chunk| held.contains_key(&chunk));
            retain
        });
        let unchanged_presence = self
            .last_presence
            .as_ref()
            .is_some_and(|previous| std::sync::Arc::ptr_eq(previous, &presence));
        for (gid, spatial, chunk, epoch) in actors.player_observations(living) {
            let Some(generation) = generations.get(&gid.server).copied() else {
                continue;
            };
            let source = PeerGeneration {
                server: gid.server,
                generation,
            };
            let Some((lease, client_entity_id)) = actors.player_client_identity(gid, source) else {
                continue;
            };
            let Some(profile) = presence.entries.get(&gid) else {
                continue;
            };
            let install_generation = if actors.local_key(gid).is_some() {
                inventory.player_install_generation(gid)
            } else {
                actors
                    .prepared_remote_admission(gid)
                    .filter(|admission| admission.lease == lease && admission.owner == source)
                    .map(|admission| admission.install_generation)
            };
            if !install_generation.is_some_and(|generation| {
                profile_matches_avatar(profile, lease, source, generation)
            }) || !held.contains_key(&chunk)
            {
                continue;
            }
            let Some(state) =
                living.phase_state(pumpkin_cluster::actor::ActorId::Player(gid), false)
            else {
                continue;
            };
            if actors.phase_spatial(gid, false).is_none() {
                continue;
            }
            let Some(avatar) = state.avatar.as_ref() else {
                continue;
            };
            if avatar.world != spatial.world || avatar.spatial_epoch != epoch {
                continue;
            }
            let previous = self
                .frames
                .get(&gid)
                .filter(|old| old.lease == lease && old.source == source);
            let revision = inventory
                .live_player_lease_matches(lease)
                .then(|| inventory.phase_revision(TruthView::Local, InventoryTarget::Player(gid)))
                .flatten();
            let retained = previous
                .filter(|old| revision.is_some() && old.inventory_revision == revision)
                .and_then(|old| old.inventory_cells.clone());
            let fresh = retained.or_else(|| {
                inventory
                    .live_player_lease_matches(lease)
                    .then(|| {
                        inventory.phase_snapshot(TruthView::Local, InventoryTarget::Player(gid))
                    })
                    .flatten()
                    .filter(|snapshot| snapshot.has_complete_player_cells())
                    .map(|snapshot| std::sync::Arc::new(snapshot.cells))
            });
            let inventory_revision = fresh
                .as_ref()
                .and(revision)
                .or_else(|| previous.and_then(|old| old.inventory_revision));
            let inventory_cells =
                fresh.or_else(|| previous.and_then(|old| old.inventory_cells.clone()));
            let equipment = previous
                .filter(|old| {
                    inventory_cells
                        .as_ref()
                        .zip(old.inventory_cells.as_ref())
                        .is_some_and(|(new, old)| std::sync::Arc::ptr_eq(new, old))
                })
                .and_then(|old| old.equipment.clone())
                .or_else(|| {
                    inventory_cells
                        .as_ref()
                        .and_then(|cells| equipment_from_cells(cells))
                })
                .or_else(|| {
                    self.frames
                        .get(&gid)
                        .filter(|old| old.lease == lease && old.source == source)
                        .and_then(|old| old.equipment.clone())
                });
            if equipment.is_none() {
                continue;
            }
            let mut spatial = spatial;
            spatial.sneaking = avatar.stance.sneaking;
            spatial.sprinting = avatar.stance.sprinting;
            spatial.crawling = avatar.stance.crawling;
            spatial.swimming = avatar.stance.swimming;
            spatial.gliding = avatar.stance.gliding;
            spatial.game_mode = avatar.game_mode;
            let mut frame = RemotePlayerFrame {
                packet_image: previous.map_or(0, |frame| frame.packet_image),
                lease,
                source,
                client_entity_id,
                profile: previous
                    .filter(|old| unchanged_presence || old.profile.as_ref() == profile)
                    .map(|old| old.profile.clone())
                    .unwrap_or_else(|| std::sync::Arc::new(profile.clone())),
                mask_health,
                health_policy,
                ender: self
                    .ender_heads
                    .get(&gid)
                    .filter(|snapshot| snapshot.matches(lease, source, install_generation.unwrap()))
                    .cloned()
                    .map(RemotePlayerEnder)
                    .or_else(|| {
                        previous
                            .and_then(|frame| frame.ender.clone())
                            .filter(|ender| {
                                ender.0.matches(lease, source, install_generation.unwrap())
                            })
                    }),
                spatial,
                bedrock_skin: skins
                    .get(&lease.key.0)
                    .filter(|skin| {
                        skin.source == source
                            && super::cluster_profile_skins::matches_profile(server, skin, profile)
                    })
                    .map(|skin| RemotePlayerSkin(skin.skin.clone())),
                living: previous
                    .filter(|old| old.living.as_ref() == state)
                    .map(|old| old.living.clone())
                    .unwrap_or_else(|| std::sync::Arc::new(state.clone())),
                equipment,
                inventory_cells,
                inventory_revision,
                skin_layers: self
                    .cosmetics
                    .get(&gid)
                    .filter(|entry| entry.lease == lease && entry.source == source)
                    .and_then(|entry| entry.skin_layers),
                main_hand: self
                    .cosmetics
                    .get(&gid)
                    .filter(|entry| entry.lease == lease && entry.source == source)
                    .and_then(|entry| entry.main_hand),
            };
            if previous.is_none_or(|before| before != &frame) {
                self.next_packet_image = self.next_packet_image.wrapping_add(1);
                frame.packet_image = self.next_packet_image;
            }
            next.insert(gid, frame);
        }
        self.last_presence = Some(presence);
        self.frames = next.clone();
        next
    }

    pub(super) fn publish(
        &mut self,
        server: &super::Server,
        actors: &super::cluster_world_actor::OwnedActors,
        living: &super::cluster_world_actor::OwnedLivingActors,
        inventory: &super::cluster_world_inventory::InventoryOwner,
        steps: &[pumpkin_cluster::action_graph::LocalDeltaStep],
        tick: pumpkin_cluster::time::TickStamp,
    ) {
        self.local_cosmetics.emit(server, actors, living, tick);
        for snapshot in self.local_cosmetics.take_ender_presentations() {
            self.accept_ender(server, snapshot);
        }
        self.pending_visual
            .extend(self.local_cosmetics.take_presentations());
        let pulses = self.drain_visual(server, actors, living);
        let mut pulses_by_actor = rustc_hash::FxHashMap::<GlobalPlayerId, Vec<usize>>::default();
        for (index, pulse) in pulses.iter().enumerate() {
            pulses_by_actor
                .entry(pulse.lease.gid)
                .or_default()
                .push(index);
        }
        let changes = self.ordered_living_changes(steps);
        let mut changes_by_actor = BTreeMap::<GlobalPlayerId, Vec<usize>>::new();
        for (index, (gid, _, _, _)) in changes.iter().enumerate() {
            changes_by_actor.entry(*gid).or_default().push(index);
        }
        let next = self.reconcile(server, actors, living, inventory);
        let mut packet_cache = super::cluster_remote_player_packet_cache::PlayerPacketCache::new(
            self.next_packet_image,
        );
        let previous = server.cluster_remote_players.load();
        let mut registry = std::collections::HashMap::new();
        let mut actors_by_chunk =
            BTreeMap::<pumpkin_cluster::protocol::ChunkKey, Vec<GlobalPlayerId>>::new();
        for (gid, frame) in &next {
            actors_by_chunk
                .entry(frame.spatial.chunk)
                .or_default()
                .push(*gid);
            if actors.local_key(*gid).is_some() {
                continue;
            }
            if let Some(old) = previous.get(gid).filter(|old| old.frame == *frame) {
                registry.insert(*gid, old.clone());
                continue;
            }
            let Some(world) = server
                .worlds
                .load()
                .iter()
                .find(|world| super::cluster_world_delta::world_id(world) == frame.spatial.world)
                .cloned()
            else {
                continue;
            };
            registry.insert(
                *gid,
                crate::entity::cluster_player_ghost::RemotePlayerReplica::new(
                    world,
                    frame.clone(),
                    previous.get(gid).map(|previous| previous.as_ref()),
                ),
            );
        }
        let mut world_entities = std::collections::HashMap::new();
        let mut uuids = std::collections::HashMap::new();
        for replica in registry.values() {
            uuids
                .entry(replica.frame.lease.key.0)
                .and_modify(|gid| *gid = None)
                .or_insert(Some(replica.frame.lease.gid));
            world_entities
                .entry(replica.frame.spatial.world)
                .or_insert_with(Vec::new)
                .push(replica.clone() as std::sync::Arc<dyn crate::entity::EntityBase>);
        }
        server
            .cluster_remote_players
            .store(std::sync::Arc::new(RemotePlayerDirectory {
                uuids,
                players: registry,
                worlds: world_entities
                    .into_iter()
                    .map(|(world, entities)| (world, std::sync::Arc::new(entities)))
                    .collect(),
            }));
        let presence = server.cluster_presence.snapshot();
        let mut active = std::collections::BTreeSet::new();
        for world in server.worlds.load().iter() {
            let world_id = super::cluster_world_delta::world_id(world);
            for player in world.players.load().iter() {
                let Some(gid) = player.cluster_gid() else {
                    continue;
                };
                let Some(lease) = inventory.player_lease_for_gid(gid) else {
                    continue;
                };
                let Some(generation) = inventory.player_install_generation(gid) else {
                    continue;
                };
                if player.gameprofile.id != lease.key.0
                    || player.client.closed()
                    || player
                        .cluster_owner_install_generation
                        .load(std::sync::atomic::Ordering::Acquire)
                        != generation
                {
                    continue;
                }
                let epoch = player
                    .chunk_send_epoch
                    .load(std::sync::atomic::Ordering::Acquire);
                let key = (gid, generation, epoch);
                active.insert(key);
                let ready = player.ready_chunk_view.load_full();
                let spectator = living
                    .phase_state(pumpkin_cluster::actor::ActorId::Player(gid), false)
                    .and_then(|state| state.avatar.as_ref())
                    .is_some_and(|avatar| avatar.game_mode == 3);
                let visible = ready
                    .chunks
                    .iter()
                    .filter(|_| ready.epoch == Some(epoch))
                    .filter_map(|position| {
                        actors_by_chunk.get(&pumpkin_cluster::protocol::ChunkKey::new(
                            world_id, position.x, position.y,
                        ))
                    })
                    .flatten()
                    .filter_map(|other| next.get(other).map(|frame| (other, frame)))
                    .filter(|(other, frame)| {
                        **other != gid
                            && (!matches!(
                                player.client.as_ref(),
                                crate::net::ClientPlatform::Bedrock(_)
                            ) || frame.bedrock_skin.is_some())
                            && frame.spatial.world == world_id
                            && presence.is_visible_to(
                                other,
                                &frame.profile.uuid,
                                Some(player.gameprofile.id.as_bytes()),
                            )
                            && (frame.spatial.game_mode != 3 || spectator)
                            && ready.contains(
                                epoch,
                                &pumpkin_util::math::vector2::Vector2::new(
                                    frame.spatial.chunk.x(),
                                    frame.spatial.chunk.z(),
                                ),
                            )
                    })
                    .map(|(gid, frame)| (*gid, frame.clone()))
                    .collect::<BTreeMap<_, _>>();
                let mut old = self.viewers.get(&key).cloned().unwrap_or_default();
                let mut frames = Vec::new();
                let mut frame_bytes = 0;
                let mut baseline_only = true;
                let prepared = (|| {
                    for (previous_key, previous_view) in
                        self.viewers.range((gid, 0, 0)..=(gid, u64::MAX, u32::MAX))
                    {
                        if previous_key.0 == gid && *previous_key != key {
                            for previous in previous_view.values() {
                                append_frames(
                                    &mut frames,
                                    &mut frame_bytes,
                                    prepare_remove(&player.client, previous.client_entity_id)?,
                                )?;
                            }
                        }
                    }
                    let changed = old
                        .keys()
                        .filter_map(|actor| changes_by_actor.get(actor))
                        .flatten()
                        .copied()
                        .collect::<std::collections::BTreeSet<_>>();
                    for index in changed {
                        let (changed_gid, lease, state, cells) = &changes[index];
                        if !old.contains_key(changed_gid) {
                            continue;
                        }
                        let after =
                            state.as_ref().and_then(|state| {
                                let mut frame = visible
                                    .get(changed_gid)
                                    .filter(|frame| Some(frame.lease) == *lease)
                                    .or_else(|| {
                                        old.get(changed_gid).filter(|frame| {
                                            Some(frame.lease) == *lease
                                                && frame.spatial.world == world_id
                                                && presence.is_visible_to(
                                                    changed_gid,
                                                    &frame.profile.uuid,
                                                    Some(player.gameprofile.id.as_bytes()),
                                                )
                                                && ready.contains(
                                                    epoch,
                                                    &pumpkin_util::math::vector2::Vector2::new(
                                                        frame.spatial.chunk.x(),
                                                        frame.spatial.chunk.z(),
                                                    ),
                                                )
                                        })
                                    })?
                                    .clone();
                                if state.combat.actor
                                    != pumpkin_cluster::actor::ActorId::Player(*changed_gid)
                                {
                                    return None;
                                }
                                let avatar = state.avatar.as_ref()?;
                                if avatar.world != frame.spatial.world {
                                    return None;
                                }
                                if frame.living.avatar.as_ref().is_some_and(|before| {
                                    before.spatial_epoch != avatar.spatial_epoch
                                }) {
                                    frame.spatial.frame.effective.position =
                                        avatar.position.map(f64::from_bits);
                                    frame.spatial.frame.effective.yaw = f32::from_bits(avatar.yaw);
                                    frame.spatial.frame.effective.pitch =
                                        f32::from_bits(avatar.pitch);
                                    frame.spatial.head_yaw = f32::from_bits(avatar.head_yaw);
                                }
                                frame.living = state.clone();
                                frame.packet_image =
                                    packet_cache.modified_image(frame.packet_image, index);
                                if let Some(cells) = cells {
                                    if !frame
                                        .inventory_cells
                                        .as_ref()
                                        .is_some_and(|old| std::sync::Arc::ptr_eq(old, cells))
                                    {
                                        frame.equipment =
                                            equipment_from_cells(cells).or(frame.equipment);
                                        frame.inventory_cells = Some(cells.clone());
                                    }
                                }
                                Some(frame)
                            });
                        match after {
                            Some(after) => {
                                if old
                                    .get(changed_gid)
                                    .is_some_and(|before| !before.same_session(&after))
                                    && let Some(before) = old.remove(changed_gid)
                                {
                                    append_frames(
                                        &mut frames,
                                        &mut frame_bytes,
                                        prepare_remove(&player.client, before.client_entity_id)?,
                                    )?;
                                }
                                let before = old
                                    .get(changed_gid)
                                    .filter(|before| before.same_session(&after));
                                if before.is_none_or(|before| !before.rendered_equal(&after)) {
                                    append_frames(
                                        &mut frames,
                                        &mut frame_bytes,
                                        packet_cache.prepare(&player.client, before, &after)?,
                                    )?;
                                }
                                old.insert(*changed_gid, after);
                            }
                            None => {
                                if old
                                    .get(changed_gid)
                                    .is_some_and(|before| Some(before.lease) == *lease)
                                    && let Some(before) = old.remove(changed_gid)
                                {
                                    append_frames(
                                        &mut frames,
                                        &mut frame_bytes,
                                        prepare_remove(&player.client, before.client_entity_id)?,
                                    )?;
                                }
                            }
                        }
                    }
                    for (gid, before) in &old {
                        if visible
                            .get(gid)
                            .is_none_or(|after| !before.same_session(after))
                        {
                            append_frames(
                                &mut frames,
                                &mut frame_bytes,
                                prepare_remove(&player.client, before.client_entity_id)?,
                            )?;
                        }
                    }
                    old.retain(|gid, before| {
                        visible
                            .get(gid)
                            .is_some_and(|after| before.same_session(after))
                    });
                    baseline_only = frames.is_empty();
                    let mut projection_order = visible.keys().copied().collect::<Vec<_>>();
                    projection_order.sort_by_key(|gid| !old.contains_key(gid));
                    for gid in projection_order {
                        let after = &visible[&gid];
                        let before = old.get(&gid).filter(|before| before.same_session(after));
                        if before.is_some_and(|before| before.rendered_equal(after)) {
                            old.insert(gid, after.clone());
                            continue;
                        }
                        let packets = packet_cache.prepare(&player.client, before, after)?;
                        if before.is_none() {
                            if !append_spawn_frames(&mut frames, &mut frame_bytes, packets)? {
                                break;
                            }
                        } else {
                            baseline_only = false;
                            append_frames(&mut frames, &mut frame_bytes, packets)?;
                        }
                        old.insert(gid, after.clone());
                    }
                    let visible_pulses = old
                        .keys()
                        .filter_map(|actor| pulses_by_actor.get(actor))
                        .flatten()
                        .copied()
                        .collect::<Vec<_>>();
                    for index in visible_pulses {
                        let pulse = &pulses[index];
                        if let Some(after) = old.get(&pulse.lease.gid).filter(|frame| {
                            frame.lease == pulse.lease
                                && frame.source == pulse.source
                                && frame
                                    .living
                                    .avatar
                                    .as_ref()
                                    .is_some_and(|avatar| avatar.spatial_epoch == pulse.epoch)
                        }) {
                            if self
                                .viewers
                                .get(&key)
                                .is_some_and(|view| view.contains_key(&pulse.lease.gid))
                            {
                                baseline_only = false;
                            }
                            if let Some(animation) = pulse.break_animation {
                                let target_chunk = pumpkin_util::math::vector2::Vector2::new(
                                    animation.position[0].div_euclid(16),
                                    animation.position[2].div_euclid(16),
                                );
                                if ready.contains(epoch, &target_chunk) {
                                    append_frames(
                                    &mut frames,
                                    &mut frame_bytes,
                                    super::cluster_remote_player_packets::prepare_break_animation(
                                        &player.client,
                                        after.client_entity_id,
                                        animation,
                                    )?,
                                    )?;
                                }
                            }
                            for swing in pulse.swings.iter() {
                                append_frames(
                                    &mut frames,
                                    &mut frame_bytes,
                                    prepare_swing(
                                        &player.client,
                                        after.client_entity_id,
                                        swing.arm,
                                    )?,
                                )?;
                            }
                        }
                    }
                    Ok::<(), String>(())
                })();
                if let Err(reason) = prepared {
                    tracing::error!(?gid, generation, epoch, %reason, "cluster remote player presentation failed");
                    player.client.try_kick(
                        crate::net::DisconnectReason::Unknown,
                        &pumpkin_util::text::TextComponent::text(
                            "Player state could not be delivered",
                        ),
                    );
                    continue;
                }
                if player
                    .chunk_send_epoch
                    .load(std::sync::atomic::Ordering::Acquire)
                    != epoch
                {
                    continue;
                }
                enqueue_viewer_projection_with_policy(
                    &mut self.viewers,
                    key,
                    &player.client,
                    frames,
                    old,
                    baseline_only,
                );
            }
        }
        self.viewers.retain(|key, _| active.contains(key));
        self.next_packet_image = packet_cache.next_image();
    }

    fn ordered_living_changes(
        &self,
        steps: &[pumpkin_cluster::action_graph::LocalDeltaStep],
    ) -> Vec<(
        GlobalPlayerId,
        Option<LivePlayerLease>,
        Option<std::sync::Arc<ActorLivingState>>,
        Option<std::sync::Arc<Vec<InventoryCell>>>,
    )> {
        use pumpkin_cluster::canonical_action::{ChunkPart, Target, Value};
        if steps.is_empty() {
            return Vec::new();
        }
        let affected = steps
            .iter()
            .flat_map(|step| &step.writes)
            .filter_map(|write| match write.target {
                pumpkin_cluster::canonical_action::Target::Chunk {
                    part: pumpkin_cluster::canonical_action::ChunkPart::Player(gid),
                    ..
                } => Some(gid),
                pumpkin_cluster::canonical_action::Target::Chunk {
                    part:
                        pumpkin_cluster::canonical_action::ChunkPart::LivingProperty {
                            actor: pumpkin_cluster::actor::ActorId::Player(gid),
                            ..
                        }
                        | pumpkin_cluster::canonical_action::ChunkPart::ActorRelation {
                            actor: pumpkin_cluster::actor::ActorId::Player(gid),
                            ..
                        },
                    ..
                } => Some(gid),
                pumpkin_cluster::canonical_action::Target::Player {
                    lease: Some(lease), ..
                } => Some(lease.gid),
                _ => None,
            })
            .collect::<std::collections::BTreeSet<_>>();
        let frames = || {
            affected
                .iter()
                .filter_map(|gid| self.frames.get(gid).map(|frame| (gid, frame)))
        };
        let mut states = frames()
            .map(|(gid, frame)| (*gid, frame.living.clone()))
            .collect::<BTreeMap<_, _>>();
        let mut leases = frames()
            .map(|(gid, frame)| (*gid, frame.lease))
            .collect::<BTreeMap<_, _>>();
        let mut inventories = frames()
            .filter_map(|(gid, frame)| frame.inventory_cells.clone().map(|cells| (*gid, cells)))
            .collect::<BTreeMap<_, _>>();
        let mut changes = Vec::new();
        for step in steps {
            let mut touched = std::collections::BTreeSet::new();
            let mut full = std::collections::BTreeSet::new();
            for write in &step.writes {
                let Target::Chunk {
                    part: ChunkPart::Player(gid),
                    inventory: None,
                    ..
                } = write.target
                else {
                    continue;
                };
                full.insert(gid);
                touched.insert(gid);
                match write.after.as_ref().map(|cell| &cell.value) {
                    Some(Value::Living(state)) => {
                        if let Some(pumpkin_cluster::canonical_action::SpatialEpoch::Admission {
                            lease,
                            ..
                        }) = state.avatar.as_ref().map(|avatar| avatar.spatial_epoch)
                        {
                            if lease.gid == gid {
                                if leases.get(&gid) != Some(&lease) {
                                    inventories.remove(&gid);
                                }
                                leases.insert(gid, lease);
                            }
                        }
                        states.insert(gid, std::sync::Arc::new(state.as_ref().clone()));
                    }
                    None => {
                        states.remove(&gid);
                    }
                    _ => {}
                }
            }
            for write in &step.writes {
                if let Target::Player {
                    lease: Some(lease),
                    inventory:
                        Some(pumpkin_cluster::canonical_action::InventoryId::Player { inv, slot }),
                    ..
                } = write.target
                {
                    if leases.get(&lease.gid) == Some(&lease) {
                        if let Some(Value::Inventory(cell)) =
                            write.after.as_ref().map(|cell| &cell.value)
                        {
                            if cell.loc == (InvLoc { inv, slot }) {
                                let cells = std::sync::Arc::make_mut(
                                    inventories.entry(lease.gid).or_default(),
                                );
                                if let Some(previous) =
                                    cells.iter_mut().find(|previous| previous.loc == cell.loc)
                                {
                                    *previous = cell.clone();
                                } else {
                                    cells.push(cell.clone());
                                }
                                touched.insert(lease.gid);
                            }
                        }
                    }
                    continue;
                }
                if let Target::Chunk {
                    part:
                        ChunkPart::ActorRelation {
                            actor: pumpkin_cluster::actor::ActorId::Player(gid),
                            relation: pumpkin_cluster::actor_relation::ActorRelation::Mount,
                        },
                    inventory: None,
                    ..
                } = write.target
                {
                    if !full.contains(&gid) {
                        if let Some(state) = states.get_mut(&gid) {
                            let state = std::sync::Arc::make_mut(state);
                            if let Some(avatar) = state.avatar.as_mut() {
                                if let Some(Value::ActorRelation(mount)) =
                                    write.after.as_ref().map(|cell| &cell.value)
                                {
                                    avatar.mount = mount.clone();
                                    touched.insert(gid);
                                }
                            }
                        }
                    }
                    continue;
                }
                let Target::Chunk {
                    part:
                        ChunkPart::LivingProperty {
                            actor: pumpkin_cluster::actor::ActorId::Player(gid),
                            property,
                        },
                    inventory: None,
                    ..
                } = write.target
                else {
                    continue;
                };
                if full.contains(&gid) {
                    continue;
                }
                let Some(state) = states.get_mut(&gid) else {
                    continue;
                };
                let state = std::sync::Arc::make_mut(state);
                let Some(Value::LivingProperty(value)) =
                    write.after.as_ref().map(|cell| &cell.value)
                else {
                    continue;
                };
                if property.write(state, value) {
                    touched.insert(gid);
                }
            }
            for gid in touched {
                changes.push((
                    gid,
                    leases.get(&gid).copied(),
                    states.get(&gid).cloned(),
                    inventories.get(&gid).cloned(),
                ));
            }
        }
        changes
    }
}

const MAX_REMOTE_PLAYER_BURST_BYTES: usize = 32 * 1024 * 1024;

fn append_frames(
    destination: &mut Vec<bytes::Bytes>,
    byte_count: &mut usize,
    frames: Vec<bytes::Bytes>,
) -> Result<(), String> {
    let next_bytes = byte_count.saturating_add(frames.iter().map(bytes::Bytes::len).sum::<usize>());
    if destination.len().saturating_add(frames.len()) > 16384
        || next_bytes > MAX_REMOTE_PLAYER_BURST_BYTES
    {
        return Err("remote player projection exceeds bounded client burst".to_owned());
    }
    *byte_count = next_bytes;
    destination.extend(frames);
    Ok(())
}

fn profile_matches_avatar(
    profile: &RemotePlayerEntry,
    lease: LivePlayerLease,
    source: PeerGeneration,
    install_generation: u64,
) -> bool {
    install_generation != 0
        && !profile.in_lobby
        && profile.gid == lease.gid
        && profile.uuid == *lease.key.0.as_bytes()
        && profile.source == Some(source)
        && profile.session == install_generation
}

fn stale_visual(
    previous: &pumpkin_cluster::replica_visual::PlayerVisualEnvelope,
    incoming: &pumpkin_cluster::replica_visual::PlayerVisualEnvelope,
) -> bool {
    previous.lease == incoming.lease
        && previous.source == incoming.source
        && previous.epoch == incoming.epoch
        && !incoming.tick.is_after(previous.tick)
        && (incoming.tick != previous.tick || !incoming.sequence.is_newer_than(previous.sequence))
}

#[cfg(test)]
mod visual_order_tests {
    use super::*;
    use pumpkin_cluster::canonical_action::{PlayerKey, SpatialEpoch};
    use pumpkin_cluster::identity::{PlayerSeq, PlayerSlot, ServerId};
    use pumpkin_cluster::replica_visual::PlayerVisualEnvelope;
    use pumpkin_cluster::time::TickStamp;

    fn envelope(sequence: u16) -> PlayerVisualEnvelope {
        let lease = LivePlayerLease {
            key: PlayerKey(uuid::Uuid::from_u128(80)),
            gid: GlobalPlayerId::new(ServerId(2), PlayerSlot(7)),
            incarnation: 4,
        };
        PlayerVisualEnvelope {
            lease,
            source: PeerGeneration {
                server: lease.gid.server,
                generation: 12,
            },
            epoch: SpatialEpoch::Admission {
                lease,
                admitted_at: TickStamp(10),
            },
            tick: TickStamp(11),
            sequence: PlayerSeq(sequence),
            skin_layers: Some(0x7f),
            main_hand: None,
            break_animation: None,
            swings: Default::default(),
        }
    }

    #[test]
    fn successor_avatar_cannot_reuse_prior_presence_skin_session() {
        let current = envelope(0);
        let mut profile = RemotePlayerEntry::new(
            current.lease.gid,
            *current.lease.key.0.as_bytes(),
            "player".into(),
            Vec::new(),
            0,
            false,
            true,
        );
        profile.source = Some(current.source);
        profile.session = 80;
        assert!(profile_matches_avatar(
            &profile,
            current.lease,
            current.source,
            80
        ));
        let mut successor = current.lease;
        successor.incarnation += 1;
        assert!(!profile_matches_avatar(
            &profile,
            successor,
            current.source,
            81
        ));
        profile.session = 81;
        assert!(profile_matches_avatar(
            &profile,
            successor,
            current.source,
            81
        ));
        let restarted = PeerGeneration {
            generation: current.source.generation + 1,
            ..current.source
        };
        assert!(!profile_matches_avatar(&profile, successor, restarted, 81));
        profile.in_lobby = true;
        assert!(!profile_matches_avatar(
            &profile,
            successor,
            current.source,
            81
        ));
    }

    #[test]
    fn reordered_visuals_cannot_restore_old_skin_or_repeat_a_swing() {
        let current = envelope(20);
        assert!(stale_visual(&current, &envelope(19)));
        assert!(stale_visual(&current, &envelope(20)));
        assert!(!stale_visual(&current, &envelope(21)));
        assert!(!stale_visual(&envelope(u16::MAX), &envelope(0)));
        assert!(stale_visual(&envelope(0), &envelope(u16::MAX)));
        let mut successor = envelope(0);
        successor.lease.incarnation += 1;
        assert!(!stale_visual(&current, &successor));
        let mut restarted = envelope(0);
        restarted.source.generation += 1;
        assert!(!stale_visual(&current, &restarted));
        let mut relocated = envelope(0);
        relocated.epoch = SpatialEpoch::Admission {
            lease: relocated.lease,
            admitted_at: TickStamp(20),
        };
        assert!(!stale_visual(&current, &relocated));
        let mut later_tick = envelope(40_000);
        later_tick.tick = TickStamp(12);
        assert!(!stale_visual(&current, &later_tick));
        assert!(stale_visual(&later_tick, &current));
    }
}

fn append_spawn_frames(
    destination: &mut Vec<bytes::Bytes>,
    byte_count: &mut usize,
    frames: Vec<bytes::Bytes>,
) -> Result<bool, String> {
    let spawn_bytes = frames.iter().map(bytes::Bytes::len).sum::<usize>();
    if frames.len() > 4096 || spawn_bytes > MAX_REMOTE_PLAYER_BURST_BYTES {
        return Err("remote player baseline exceeds bounded client burst".to_owned());
    }
    let next_bytes = byte_count.saturating_add(spawn_bytes);
    if destination.len().saturating_add(frames.len()) > 4096
        || next_bytes > MAX_REMOTE_PLAYER_BURST_BYTES
    {
        return Ok(false);
    }
    append_frames(destination, byte_count, frames)?;
    Ok(true)
}

#[cfg(test)]
mod spawn_budget_tests {
    use super::*;

    #[test]
    fn impossible_single_baseline_fails_without_deferral_or_mutation() {
        let mut packets = Vec::new();
        let mut byte_count = 0;
        assert!(
            append_spawn_frames(
                &mut packets,
                &mut byte_count,
                vec![bytes::Bytes::new(); 4097]
            )
            .is_err()
        );
        assert!(packets.is_empty());
        assert_eq!(byte_count, 0);
    }

    #[test]
    fn large_skin_baselines_defer_atomically_and_fit_the_next_tick() {
        let mut packets = vec![bytes::Bytes::from(vec![
            0;
            MAX_REMOTE_PLAYER_BURST_BYTES - 64
        ])];
        let mut bytes = packets[0].len();
        let spawn = vec![bytes::Bytes::from_static(&[1; 128])];
        assert!(!append_spawn_frames(&mut packets, &mut bytes, spawn.clone()).unwrap());
        assert_eq!(packets.len(), 1);
        assert_eq!(bytes, MAX_REMOTE_PLAYER_BURST_BYTES - 64);
        packets.clear();
        bytes = 0;
        assert!(append_spawn_frames(&mut packets, &mut bytes, spawn.clone()).unwrap());
        assert_eq!(packets, spawn);
        assert_eq!(bytes, 128);
    }
}
#[cfg(test)]
pub(super) fn frozen_test_frame(
    lease: pumpkin_cluster::canonical_action::LivePlayerLease,
    source: pumpkin_cluster::realm_completion::PeerGeneration,
    client_entity_id: i32,
    spatial: super::cluster_world_actor::ActorSpatialState,
    living: pumpkin_cluster::item_action::ActorLivingState,
    profile: pumpkin_cluster::presence::RemotePlayerEntry,
    inventory: Vec<pumpkin_cluster::inventory::InventoryCell>,
) -> RemotePlayerFrame {
    assert_eq!(profile.gid, lease.gid);
    assert!(living.belongs_to(pumpkin_cluster::actor::ActorId::Player(lease.gid)));
    assert_eq!(living.avatar.as_ref().unwrap().world, spatial.world);
    let equipment = equipment_from_cells(&inventory).unwrap();
    RemotePlayerFrame {
        packet_image: client_entity_id as u64,
        lease,
        source,
        client_entity_id,
        profile: std::sync::Arc::new(profile),
        bedrock_skin: None,
        ender: None,
        mask_health: true,
        health_policy: (
            None,
            pumpkin_cluster::health_policy::HealthPolicySnapshot::new(
                0,
                pumpkin_cluster::health_policy::HealthMaskMode::Players,
            ),
        ),
        spatial,
        living: std::sync::Arc::new(living),
        equipment: Some(equipment),
        inventory_cells: Some(std::sync::Arc::new(inventory)),
        inventory_revision: Some((1, 0)),
        skin_layers: None,
        main_hand: None,
    }
}

pub(super) fn delivered_player_id(
    views: &BTreeMap<(GlobalPlayerId, u64, u32), BTreeMap<GlobalPlayerId, RemotePlayerFrame>>,
    viewer: (GlobalPlayerId, u64, u32),
    lease: LivePlayerLease,
    source: PeerGeneration,
) -> Option<i32> {
    let frame = views.get(&viewer)?.get(&lease.gid)?;
    (frame.lease == lease && frame.source == source).then_some(frame.client_entity_id)
}

pub(super) fn enqueue_viewer_projection(
    views: &mut BTreeMap<(GlobalPlayerId, u64, u32), BTreeMap<GlobalPlayerId, RemotePlayerFrame>>,
    key: (GlobalPlayerId, u64, u32),
    client: &crate::net::ClientPlatform,
    packets: Vec<bytes::Bytes>,
    delivered: BTreeMap<GlobalPlayerId, RemotePlayerFrame>,
) -> bool {
    let baseline_only = views.get(&key).is_none_or(BTreeMap::is_empty);
    enqueue_viewer_projection_with_policy(views, key, client, packets, delivered, baseline_only)
}

fn enqueue_viewer_projection_with_policy(
    views: &mut BTreeMap<(GlobalPlayerId, u64, u32), BTreeMap<GlobalPlayerId, RemotePlayerFrame>>,
    key: (GlobalPlayerId, u64, u32),
    client: &crate::net::ClientPlatform,
    packets: Vec<bytes::Bytes>,
    delivered: BTreeMap<GlobalPlayerId, RemotePlayerFrame>,
    baseline_only: bool,
) -> bool {
    if client.closed() {
        return false;
    }
    if client.try_enqueue_packet_burst(packets).is_err() {
        if !baseline_only && !client.closed() {
            client.try_kick(
                crate::net::DisconnectReason::Unknown,
                &pumpkin_util::text::TextComponent::text("Player state could not be delivered"),
            );
        }
        return false;
    }
    if client.closed() {
        return false;
    }
    let obsolete = views
        .range((key.0, 0, 0)..=(key.0, u64::MAX, u32::MAX))
        .map(|(key, _)| *key)
        .filter(|previous| *previous != key)
        .collect::<Vec<_>>();
    for previous in obsolete {
        views.remove(&previous);
    }
    views.insert(key, delivered);
    true
}

#[cfg(all(test, feature = "cluster-e2e"))]
pub(super) fn baseline_test_frame(client_entity_id: i32) -> RemotePlayerFrame {
    use pumpkin_cluster::canonical_action::{PlayerKey, SpatialEpoch};
    use pumpkin_cluster::controller::{
        ClientClaimedState, ControllerActorSource, ControllerDecision, ControllerFrame,
    };
    use pumpkin_cluster::identity::{PlayerSeq, PlayerSlot, ServerId};
    use pumpkin_cluster::protocol::{ChunkKey, WorldId};
    use pumpkin_cluster::time::TickStamp;
    let gid = GlobalPlayerId::new(ServerId(2), PlayerSlot(7));
    let lease = LivePlayerLease {
        key: PlayerKey(uuid::Uuid::from_u128(42)),
        gid,
        incarnation: 9,
    };
    let source = PeerGeneration {
        server: gid.server,
        generation: 12,
    };
    let mut living = super::cluster_actor_import::from_player_nbt(gid, None)
        .unwrap()
        .state;
    let avatar = living.avatar.as_mut().unwrap();
    avatar.world = WorldId::new(0, 3);
    avatar.position = [8.0_f64, 80.0, 8.0].map(f64::to_bits);
    avatar.spatial_epoch = SpatialEpoch::Admission {
        lease,
        admitted_at: TickStamp(1),
    };
    let claimed = ClientClaimedState {
        actor: gid,
        seq: PlayerSeq(0),
        tick: TickStamp(1),
        position: [8.0, 80.0, 8.0],
        velocity: [0.0; 3],
        yaw: 0.0,
        pitch: 0.0,
        on_ground: true,
        input_flags: 0,
    };
    let spatial = super::cluster_world_actor::ActorSpatialState {
        previous_position: None,
        frame: ControllerFrame {
            claimed,
            effective: claimed.into(),
            decision: ControllerDecision::Accepted,
        },
        head_yaw: 0.0,
        source: ControllerActorSource::Initial,
        world: avatar.world,
        chunk: ChunkKey::new(avatar.world, 0, 0),
        biome_id: None,
        sneaking: false,
        sprinting: false,
        crawling: false,
        swimming: false,
        gliding: false,
        flying: false,
        fall_distance: 0.0,
        game_mode: 0,
        revision: 0,
    };
    let mut profile = RemotePlayerEntry::new(
        gid,
        *lease.key.0.as_bytes(),
        "replica".into(),
        Vec::new(),
        0,
        false,
        true,
    );
    profile.source = Some(source);
    profile.session = 80;
    let inventory = pumpkin_cluster::avatar_lifecycle::PlayerInventoryAdmission::slots()
        .map(|loc| {
            if loc.inv == pumpkin_cluster::inventory::INV_SELECTED {
                InventoryCell {
                    loc,
                    item: 1,
                    count: 1,
                    nbt: Vec::new(),
                }
            } else {
                InventoryCell::empty(loc)
            }
        })
        .collect();
    frozen_test_frame(
        lease,
        source,
        client_entity_id,
        spatial,
        living,
        profile,
        inventory,
    )
}

#[cfg(all(test, feature = "cluster-e2e"))]
mod delivered_replica_tests {
    use super::*;
    fn assert_receiver_birth(packets: &[bytes::Bytes], frame: &RemotePlayerFrame) {
        use pumpkin_protocol::MultiVersionJavaPacket;
        use pumpkin_protocol::java::client::play::{CPlayerInfoUpdate, CSpawnEntity};
        use pumpkin_protocol::ser::NetworkReadExt;
        let version = pumpkin_data::packet::CURRENT_MC_VERSION;
        let mut profile_at = None;
        let mut birth_at = None;
        for (index, packet) in packets.iter().enumerate() {
            let mut bytes = packet.as_ref();
            let id = bytes.get_var_int().unwrap().0;
            if id == CPlayerInfoUpdate::to_id(version) {
                profile_at = Some(index);
            }
            if id == CSpawnEntity::to_id(version) {
                let decoded = CSpawnEntity::read_packet_data(bytes, &version).unwrap();
                assert_eq!(decoded.entity_id.0, frame.client_entity_id);
                assert_eq!(decoded.entity_uuid, frame.lease.key.0);
                birth_at = Some(index);
            }
        }
        assert!(
            profile_at
                .zip(birth_at)
                .is_some_and(|(profile, birth)| profile < birth)
        );
    }
    #[tokio::test(flavor = "current_thread")]
    async fn readiness_follows_actual_enqueue_and_retires_exact_old_lease_and_view_epoch() {
        let profile = crate::net::GameProfile {
            id: uuid::Uuid::from_u128(1),
            name: "viewer".into(),
            properties: arc_swap::ArcSwap::from_pointee(Vec::new()),
            profile_actions: None,
        };
        let (client, mut sink) = crate::net::java::JavaClient::offline(
            1,
            profile,
            crate::net::PlayerConfig::default(),
            pumpkin_data::packet::CURRENT_MC_VERSION,
            None,
        );
        let client = crate::net::ClientPlatform::Java(client);
        let frame = baseline_test_frame(71);
        let viewer = GlobalPlayerId::new(
            pumpkin_cluster::identity::ServerId(1),
            pumpkin_cluster::identity::PlayerSlot(1),
        );
        let key = (viewer, 10, 3);
        let mut views = BTreeMap::new();
        assert!(!enqueue_viewer_projection(
            &mut views,
            key,
            &client,
            vec![bytes::Bytes::from_static(&[0]); 4097],
            BTreeMap::from([(frame.lease.gid, frame.clone())]),
        ));
        assert!(!client.closed());
        assert_eq!(sink.drain(4096).unwrap().packets, 0);
        assert!(views.is_empty());
        assert_eq!(
            delivered_player_id(&views, key, frame.lease, frame.source),
            None
        );
        let packets = prepare_frame(&client, None, &frame).unwrap();
        assert_receiver_birth(&packets, &frame);
        let count = packets.len();
        assert!(count > 0);
        assert!(enqueue_viewer_projection(
            &mut views,
            key,
            &client,
            packets,
            BTreeMap::from([(frame.lease.gid, frame.clone())])
        ));
        assert_eq!(sink.drain(4096).unwrap().packets, count);
        assert_eq!(
            delivered_player_id(&views, key, frame.lease, frame.source),
            Some(71)
        );
        let mut successor = frame.clone();
        successor.lease.incarnation += 1;
        successor.client_entity_id = 72;
        std::sync::Arc::make_mut(&mut successor.profile).session = 81;
        std::sync::Arc::make_mut(&mut successor.living)
            .avatar
            .as_mut()
            .unwrap()
            .spatial_epoch = pumpkin_cluster::canonical_action::SpatialEpoch::Admission {
            lease: successor.lease,
            admitted_at: pumpkin_cluster::time::TickStamp(2),
        };
        let mut packets = prepare_remove(&client, 71).unwrap();
        packets.extend(prepare_frame(&client, None, &successor).unwrap());
        assert_receiver_birth(&packets, &successor);
        let next_key = (viewer, 11, 4);
        assert!(enqueue_viewer_projection(
            &mut views,
            next_key,
            &client,
            packets,
            BTreeMap::from([(successor.lease.gid, successor.clone())])
        ));
        assert!(sink.drain(4096).unwrap().packets > 0);
        assert_eq!(
            delivered_player_id(&views, key, frame.lease, frame.source),
            None
        );
        assert_eq!(
            delivered_player_id(&views, next_key, frame.lease, frame.source),
            None
        );
        assert_eq!(
            delivered_player_id(&views, next_key, successor.lease, successor.source),
            Some(72)
        );
        assert!(enqueue_viewer_projection(
            &mut views,
            next_key,
            &client,
            prepare_remove(&client, 72).unwrap(),
            BTreeMap::new()
        ));
        assert_eq!(
            delivered_player_id(&views, next_key, successor.lease, successor.source),
            None
        );
        assert!(sink.drain(4096).unwrap().packets > 0);
        let packets = prepare_frame(&client, None, &successor).unwrap();
        assert!(enqueue_viewer_projection(
            &mut views,
            next_key,
            &client,
            packets,
            BTreeMap::from([(successor.lease.gid, successor.clone())])
        ));
        assert!(sink.drain(4096).unwrap().packets > 0);
        assert_eq!(
            delivered_player_id(&views, next_key, successor.lease, successor.source),
            Some(72)
        );
        client.java().unwrap().close();
        let closed_key = (viewer, 12, 5);
        assert!(!enqueue_viewer_projection(
            &mut views,
            closed_key,
            &client,
            Vec::new(),
            BTreeMap::from([(successor.lease.gid, successor.clone())])
        ));
        assert_eq!(
            delivered_player_id(&views, closed_key, successor.lease, successor.source),
            None
        );
    }
}
