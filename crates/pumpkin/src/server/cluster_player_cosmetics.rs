use pumpkin_cluster::canonical_action::LivePlayerLease;
use pumpkin_cluster::identity::GlobalPlayerId;
use pumpkin_cluster::realm_completion::PeerGeneration;
use std::collections::BTreeMap;

#[derive(Clone, Copy, Debug, PartialEq)]
pub enum CosmeticChange {
    Skin(u8),
    MainHand(u8),
    Swing(pumpkin_cluster::replica_visual::SwingArm),
    Break(pumpkin_cluster::replica_visual::PlayerBreakAnimation),
}

#[derive(Clone, Copy, Debug)]
pub(crate) struct LocalPlayerCosmetic {
    lease: LivePlayerLease,
    source: PeerGeneration,
    epoch: pumpkin_cluster::canonical_action::SpatialEpoch,
    tick: pumpkin_cluster::time::TickStamp,
    change: CosmeticChange,
}

pub(crate) fn submit_cosmetic(
    server: &super::Server,
    player: &crate::entity::player::Player,
    change: CosmeticChange,
) -> bool {
    super::cluster_controller::submit_patch(
        server,
        player,
        super::cluster_controller::ControllerPatch::Cosmetic(change),
    )
}

pub(super) fn capture_cosmetic(
    server: &super::Server,
    player: &crate::entity::player::Player,
    change: CosmeticChange,
    tick: pumpkin_cluster::time::TickStamp,
) -> Option<LocalPlayerCosmetic> {
    let lease = super::cluster_world_owner::player_action_lease(server, player)?;
    let living = server.cluster_actor_living.load();
    let avatar = living
        .get(&pumpkin_cluster::actor::ActorId::Player(lease.gid))
        .and_then(|state| state.avatar.as_ref())?;
    let generation = server
        .cluster_peer_incarnations
        .load()
        .get(&lease.gid.server)
        .copied()?;
    Some(LocalPlayerCosmetic {
        lease,
        source: PeerGeneration {
            server: lease.gid.server,
            generation,
        },
        epoch: avatar.spatial_epoch,
        tick,
        change,
    })
}

pub(crate) fn try_send_cosmetic(
    server: &super::Server,
    input: LocalPlayerCosmetic,
) -> Result<(), LocalPlayerCosmetic> {
    let Some(route) = server.cluster_local_player_cosmetic_route.load_full() else {
        return Err(input);
    };
    route
        .try_send(input)
        .map_err(tokio::sync::mpsc::error::TrySendError::into_inner)
}

pub(crate) fn owned_break_animation(
    lease: LivePlayerLease,
    source: PeerGeneration,
    epoch: pumpkin_cluster::canonical_action::SpatialEpoch,
    tick: pumpkin_cluster::time::TickStamp,
    animation: pumpkin_cluster::replica_visual::PlayerBreakAnimation,
) -> LocalPlayerCosmetic {
    LocalPlayerCosmetic {
        lease,
        source,
        epoch,
        tick,
        change: CosmeticChange::Break(animation),
    }
}

pub(super) struct OwnedPlayerCosmetics {
    ender: super::cluster_player_ender::OwnedPlayerEnderSnapshots,
    presentations: Vec<pumpkin_cluster::replica_visual::PlayerVisualEnvelope>,
    local_cosmetics: tokio::sync::mpsc::Receiver<LocalPlayerCosmetic>,
    local_sequences: BTreeMap<
        GlobalPlayerId,
        (
            LivePlayerLease,
            PeerGeneration,
            pumpkin_cluster::identity::PlayerSeq,
        ),
    >,
    outbound: tokio::sync::mpsc::Sender<pumpkin_cluster::streams::OutboundParcel>,
    pending_cosmetics: std::collections::VecDeque<pumpkin_cluster::streams::OutboundParcel>,
    retained_local_cosmetic: Option<LocalPlayerCosmetic>,
    diagnostics: BTreeMap<(GlobalPlayerId, uuid::Uuid, &'static str), std::time::Instant>,
    known_local_skin: BTreeMap<
        GlobalPlayerId,
        (
            LivePlayerLease,
            PeerGeneration,
            pumpkin_cluster::canonical_action::SpatialEpoch,
            u8,
        ),
    >,
    skin_recipients: BTreeMap<GlobalPlayerId, std::collections::BTreeSet<u16>>,
    known_local_hand: BTreeMap<
        GlobalPlayerId,
        (
            LivePlayerLease,
            PeerGeneration,
            pumpkin_cluster::canonical_action::SpatialEpoch,
            u8,
        ),
    >,
    hand_recipients: BTreeMap<GlobalPlayerId, std::collections::BTreeSet<u16>>,
}

impl OwnedPlayerCosmetics {
    pub(super) fn new(
        server: &super::Server,
        outbound: tokio::sync::mpsc::Sender<pumpkin_cluster::streams::OutboundParcel>,
    ) -> Self {
        let (tx, local_cosmetics) = tokio::sync::mpsc::channel(4096);
        server
            .cluster_local_player_cosmetic_route
            .store(Some(std::sync::Arc::new(tx)));
        Self {
            ender: super::cluster_player_ender::OwnedPlayerEnderSnapshots::new(),
            local_cosmetics,
            presentations: Vec::new(),
            local_sequences: BTreeMap::new(),
            outbound,
            pending_cosmetics: std::collections::VecDeque::new(),
            retained_local_cosmetic: None,
            diagnostics: BTreeMap::new(),
            known_local_skin: BTreeMap::new(),
            skin_recipients: BTreeMap::new(),
            known_local_hand: BTreeMap::new(),
            hand_recipients: BTreeMap::new(),
        }
    }
    pub(super) fn take_ender_presentations(
        &mut self,
    ) -> Vec<std::sync::Arc<pumpkin_cluster::replica_inventory::PlayerEnderSnapshot>> {
        self.ender.take_presentations()
    }
    pub(super) fn take_presentations(
        &mut self,
    ) -> Vec<pumpkin_cluster::replica_visual::PlayerVisualEnvelope> {
        std::mem::take(&mut self.presentations)
    }
    pub(super) fn emit(
        &mut self,
        server: &super::Server,
        actors: &super::cluster_world_actor::OwnedActors,
        living: &super::cluster_world_actor::OwnedLivingActors,
        tick: pumpkin_cluster::time::TickStamp,
    ) {
        self.local_sequences.retain(|gid, (lease, source, _)| {
            actors.local_key(*gid) == Some(lease.key.0)
                && server.cluster_peer_incarnations.load().get(&source.server)
                    == Some(&source.generation)
        });
        self.known_local_skin.retain(|gid, (lease, source, _, _)| {
            actors.local_key(*gid) == Some(lease.key.0)
                && server.cluster_peer_incarnations.load().get(&source.server)
                    == Some(&source.generation)
        });
        self.skin_recipients
            .retain(|gid, _| actors.local_key(*gid).is_some());
        self.known_local_hand.retain(|gid, (lease, source, _, _)| {
            actors.local_key(*gid) == Some(lease.key.0)
                && server.cluster_peer_incarnations.load().get(&source.server)
                    == Some(&source.generation)
        });
        self.hand_recipients
            .retain(|gid, _| actors.local_key(*gid).is_some());
        let directory = server.cluster_chunk_directory.load();
        while let Some(parcel) = self.pending_cosmetics.pop_front() {
            if let Err(error) = self.outbound.try_send(parcel) {
                self.pending_cosmetics.push_front(error.into_inner());
                return;
            }
        }
        let players = server.get_all_players();
        for player in &players {
            let Some(lease) = super::cluster_world_owner::player_action_lease(server, &player)
            else {
                continue;
            };
            let Some(avatar) = living
                .phase_state(pumpkin_cluster::actor::ActorId::Player(lease.gid), false)
                .and_then(|state| state.avatar.as_ref())
            else {
                continue;
            };
            let Some(generation) = server
                .cluster_peer_incarnations
                .load()
                .get(&lease.gid.server)
                .copied()
            else {
                continue;
            };
            let source = PeerGeneration {
                server: lease.gid.server,
                generation,
            };
            let mask = player.config.load().skin_parts;
            let Some(spatial) = actors.phase_spatial(lease.gid, false) else {
                continue;
            };
            let mut recipients = directory
                .sorted_holders(&spatial.chunk)
                .into_iter()
                .collect::<std::collections::BTreeSet<_>>();
            recipients.extend(
                directory
                    .subscriptions_at(spatial.chunk, tick)
                    .into_iter()
                    .map(|entry| entry.subscriber),
            );
            recipients.remove(&lease.gid.server.0);
            if (self.known_local_skin.get(&lease.gid)
                != Some(&(lease, source, avatar.spatial_epoch, mask))
                || self.skin_recipients.get(&lease.gid) != Some(&recipients))
                && submit_cosmetic(server, &player, CosmeticChange::Skin(mask))
            {
                self.known_local_skin
                    .insert(lease.gid, (lease, source, avatar.spatial_epoch, mask));
                self.skin_recipients.insert(lease.gid, recipients.clone());
            }
            let hand = player.config.load().main_hand as u8;
            if (self.known_local_hand.get(&lease.gid)
                != Some(&(lease, source, avatar.spatial_epoch, hand))
                || self.hand_recipients.get(&lease.gid) != Some(&recipients))
                && submit_cosmetic(server, &player, CosmeticChange::MainHand(hand))
            {
                self.known_local_hand
                    .insert(lease.gid, (lease, source, avatar.spatial_epoch, hand));
                self.hand_recipients.insert(lease.gid, recipients);
            }
        }
        while self.pending_cosmetics.len() < 65535 {
            let input = match self.retained_local_cosmetic.take() {
                Some(input) => input,
                None => match self.local_cosmetics.try_recv() {
                    Ok(input) => input,
                    Err(_) => break,
                },
            };
            if actors.local_key(input.lease.gid) != Some(input.lease.key.0)
                || server
                    .cluster_peer_incarnations
                    .load()
                    .get(&input.source.server)
                    != Some(&input.source.generation)
                || actors
                    .player_client_identity(input.lease.gid, input.source)
                    .is_none_or(|(lease, _)| lease != input.lease)
                || living
                    .phase_state(
                        pumpkin_cluster::actor::ActorId::Player(input.lease.gid),
                        false,
                    )
                    .and_then(|state| state.avatar.as_ref())
                    .is_none_or(|avatar| avatar.spatial_epoch != input.epoch)
            {
                continue;
            }
            let Some(spatial) = actors.phase_spatial(input.lease.gid, false) else {
                continue;
            };
            let mut recipients = directory
                .sorted_holders(&spatial.chunk)
                .into_iter()
                .collect::<std::collections::BTreeSet<_>>();
            recipients.extend(
                directory
                    .subscriptions_at(spatial.chunk, input.tick)
                    .into_iter()
                    .map(|entry| entry.subscriber),
            );
            recipients.remove(&input.lease.gid.server.0);
            if self.pending_cosmetics.len() + recipients.len() > 65535 {
                self.retained_local_cosmetic = Some(input);
                break;
            }
            let slot = self.local_sequences.entry(input.lease.gid).or_insert((
                input.lease,
                input.source,
                pumpkin_cluster::identity::PlayerSeq(0),
            ));
            if (slot.0, slot.1) != (input.lease, input.source) {
                *slot = (
                    input.lease,
                    input.source,
                    pumpkin_cluster::identity::PlayerSeq(0),
                );
            }
            let sequence = slot.2;
            slot.2 = pumpkin_cluster::identity::PlayerSeq(sequence.0.wrapping_add(1));
            let (skin_layers, swings, break_animation) = match input.change {
                CosmeticChange::Skin(mask) => (Some(mask), Default::default(), None),
                CosmeticChange::MainHand(_) => (None, Default::default(), None),
                CosmeticChange::Swing(arm) => (
                    None,
                    pumpkin_cluster::replica_visual::SwingRecords::single(arm),
                    None,
                ),
                CosmeticChange::Break(animation) => (None, Default::default(), Some(animation)),
            };
            let envelope = pumpkin_cluster::replica_visual::PlayerVisualEnvelope {
                lease: input.lease,
                source: input.source,
                epoch: input.epoch,
                sequence,
                tick: input.tick,
                skin_layers,
                main_hand: match input.change {
                    CosmeticChange::MainHand(hand) => Some(hand),
                    _ => None,
                },
                break_animation,
                swings,
            };
            let Some(bytes) = envelope.encode() else {
                self.diagnose(
                    input.lease.gid,
                    input.lease.key.0,
                    "encode",
                    "invalid qualified cosmetic envelope",
                );
                continue;
            };
            self.presentations.push(envelope);
            for peer in recipients {
                self.pending_cosmetics
                    .push_back(pumpkin_cluster::streams::OutboundParcel {
                        peer: pumpkin_cluster::identity::ServerId(peer),
                        header: pumpkin_cluster::streams::StreamHeader::new(
                            pumpkin_cluster::protocol::StreamKind::PlayerVisual,
                            Some(input.lease.gid),
                        ),
                        bytes: bytes.clone(),
                    });
            }
        }
        if self.retained_local_cosmetic.is_none() {
            self.ender
                .emit(server, actors, &players, &mut self.pending_cosmetics, tick);
        }
        while let Some(parcel) = self.pending_cosmetics.pop_front() {
            if let Err(error) = self.outbound.try_send(parcel) {
                self.pending_cosmetics.push_front(error.into_inner());
                break;
            }
        }
    }
    fn diagnose(
        &mut self,
        gid: GlobalPlayerId,
        viewer: uuid::Uuid,
        kind: &'static str,
        reason: &str,
    ) {
        let now = std::time::Instant::now();
        self.diagnostics
            .retain(|_, last| now.duration_since(*last).as_secs() < 300);
        if let std::collections::btree_map::Entry::Vacant(entry) =
            self.diagnostics.entry((gid, viewer, kind))
        {
            entry.insert(now);
            tracing::error!(?gid, %viewer, kind, reason, "cluster player cosmetic delivery failed");
        }
    }
}
