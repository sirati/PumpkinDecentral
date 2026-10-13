use std::collections::{HashMap, HashSet};
use std::sync::Arc;
use pumpkin_cluster::actor::ActorId;
use pumpkin_cluster::actor_action_scope::ActorActionDraft;
use pumpkin_cluster::canonical_action::{Action, ActionState, LivePlayerLease, SpatialEpoch};
use pumpkin_cluster::entity_body::EntityBody;
use pumpkin_cluster::identity::GlobalPlayerId;
use pumpkin_cluster::item_spatial::OwnedItemSpatial;
use pumpkin_cluster::protocol::{ChunkKey, GlobalEntityId};
use pumpkin_cluster::realm_completion::PeerGeneration;
use pumpkin_cluster::time::TickStamp;

type PlayerObservation = (LivePlayerLease, super::cluster_world_actor::ActorSpatialState, ChunkKey, SpatialEpoch);

pub(super) struct ItemContactFrame {
    snapshot: Arc<super::cluster_world_callback_snapshot::CallbackTickSnapshot>,
    players: Vec<PlayerObservation>,
    bodies: Vec<(ChunkKey, EntityBody)>,
    spatial: OwnedItemSpatial,
    pub source: PeerGeneration,
    pub tick: TickStamp,
    pickups: HashSet<(GlobalPlayerId, TickStamp, GlobalEntityId)>,
    merges: HashSet<(TickStamp, GlobalEntityId, GlobalEntityId)>,
    pickup_complete: bool,
    merge_complete: bool,
}

impl ItemContactFrame {
    pub(super) fn new(snapshot: Arc<super::cluster_world_callback_snapshot::CallbackTickSnapshot>,
        source: PeerGeneration, tick: TickStamp, primary: bool) -> Result<Self, pumpkin_cluster::canonical_action::ApplyFailure> {
        use pumpkin_cluster::canonical_action::{ApplyFailure, ChunkPart, Target, Value};
        let mut players = Vec::new();
        for contact in snapshot.player_contacts(source, tick)? {
            let pumpkin_cluster::item_contact::ContactIdentity::Player(lease) = &contact.identity else { return Err(ApplyFailure::InvalidAction); };
            let lease = *lease;
            if contact.owner != source { continue; }
            let original = snapshot.player_projection(lease.gid).ok_or(ApplyFailure::Condition(contact.target()))?;
            players.push((lease, *original, contact.chunk, contact.epoch));
        }
        let bodies = snapshot.captured_entity_bodies().filter(|(_, body)| matches!(body.payload,
            pumpkin_cluster::entity_body::EntityPayload::Item(_) | pumpkin_cluster::entity_body::EntityPayload::Experience(_)))
            .map(|(chunk, body)| (chunk, body.clone())).collect::<Vec<_>>();
        let mut spatial = OwnedItemSpatial::default();
        for (chunk, body) in &bodies {
            let target = Target::Chunk { key: *chunk, part: ChunkPart::Entity(body.id), inventory: None };
            let sample = snapshot.original_entity_sample(body.id).ok_or(ApplyFailure::Condition(target))?;
            snapshot.actor_occupancy_cut(target, &Value::Entity(Box::new(body.clone())))?;
            if !spatial.apply(sample.clone(), body, *chunk, snapshot.scheduled_executor(*chunk)?) { return Err(ApplyFailure::InvalidAction); }
        }
        Ok(Self { snapshot, players, bodies, spatial, source, tick, pickups: HashSet::new(), merges: HashSet::new(),
            pickup_complete: false, merge_complete: !primary })
    }

    pub(super) fn actors(&self) -> impl Iterator<Item = (ActorId, u64)> + '_ {
        self.players.iter().filter(|(lease, _, _, _)| lease.gid.server == self.source.server)
            .map(|(lease, _, _, _)| (ActorId::Player(lease.gid), lease.incarnation))
            .chain(self.bodies.iter().filter_map(|(chunk, body)| (!self.merge_complete
                && self.snapshot.scheduled_executor(*chunk).ok() == Some(self.source)
                && matches!(body.payload,
                pumpkin_cluster::entity_body::EntityPayload::Item(_) | pumpkin_cluster::entity_body::EntityPayload::Experience(_))
                ).then_some((ActorId::Entity(body.id), body.id.generation))))
    }

    pub(super) fn emit(&mut self, clock: &mut pumpkin_cluster::action_issue::ActionIssueClock,
        drafts: &mut HashMap<(ActorId, u64, TickStamp), ActorActionDraft>, capacity: usize) -> Vec<Action> {
        if capacity == 0 { return Vec::new(); }
        let mut actions = Vec::new();
        if !self.pickup_complete {
            let (captured, complete) = super::cluster_item_pickup::capture(self.snapshot.as_ref(), &self.players, &self.bodies,
                &self.spatial, self.source, self.tick, clock, drafts, &mut self.pickups, capacity);
            actions = captured;
            self.pickup_complete = complete;
        }
        let available = capacity.saturating_sub(actions.len());
        if self.pickup_complete && !self.merge_complete && available != 0 {
            let (merges, complete) = super::cluster_item_merge::capture(self.snapshot.as_ref(), &self.bodies, &self.spatial,
                self.source, self.tick, clock, drafts, &mut self.merges, available,
                |chunk, _| self.snapshot.scheduled_executor(chunk).ok() == Some(self.source));
            self.merge_complete = complete;
            actions.extend(merges);
        }
        actions
    }

    pub(super) fn complete(&self) -> bool { self.pickup_complete && self.merge_complete }
}
