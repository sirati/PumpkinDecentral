use std::collections::BTreeMap;

use serde::{Deserialize, Serialize};

use crate::canonical_action::SpatialEpoch;
use crate::entity_body::{EntityBirth, EntityBody, EntityPayload, EntitySpatial};
use crate::identity::PlayerSeq;
use crate::protocol::{ChunkKey, GlobalEntityId};
use crate::realm_completion::PeerGeneration;
use crate::time::TickStamp;

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct ItemSpatialSample {
    pub id: GlobalEntityId,
    pub birth: EntityBirth,
    pub epoch: SpatialEpoch,
    pub issuer: PeerGeneration,
    pub tick: TickStamp,
    pub sequence: PlayerSeq,
    pub spatial: EntitySpatial,
    pub head_yaw: u32,
}

impl ItemSpatialSample {
    pub fn authorized_issuer(&self, body: &EntityBody, primary: PeerGeneration) -> bool {
        self.issuer.generation != 0
            && match body.payload {
                EntityPayload::Item(_) => primary.generation != 0 && self.issuer == primary,
                EntityPayload::Experience(_) => {
                    primary.generation != 0 && self.issuer == primary && body.owner == primary
                }
                EntityPayload::Living(_)
                | EntityPayload::Vehicle(_)
                | EntityPayload::Object(_)
                | EntityPayload::Projectile(_) => self.issuer == body.owner,
            }
    }

    pub fn matches_body(&self, body: &EntityBody) -> bool {
        self.id == body.id
            && self.birth == body.birth
            && self.epoch == body.spatial_epoch
            && (matches!(body.payload, EntityPayload::Item(_)) || self.issuer == body.owner)
    }
}

#[derive(Clone, Default)]
pub struct OwnedItemSpatial {
    samples: BTreeMap<GlobalEntityId, ItemSpatialSample>,
    revision: u64,
    changes: std::collections::BTreeSet<GlobalEntityId>,
    previous: BTreeMap<GlobalEntityId, ItemSpatialSample>,
}

impl OwnedItemSpatial {
    pub fn revision(&self) -> u64 {
        self.revision
    }
    pub fn take_changes(&mut self) -> std::collections::BTreeSet<GlobalEntityId> {
        std::mem::take(&mut self.changes)
    }

    pub fn get(&self, id: GlobalEntityId) -> Option<&ItemSpatialSample> {
        self.samples.get(&id)
    }

    pub fn previous_sample(&self, id: GlobalEntityId) -> Option<&ItemSpatialSample> {
        self.previous.get(&id)
    }

    pub fn retire(&mut self, id: GlobalEntityId) {
        self.previous.remove(&id);
        if self.samples.remove(&id).is_some() {
            self.changes.insert(id);
            self.revision = self.revision.wrapping_add(1);
        }
    }

    pub fn apply(
        &mut self,
        sample: ItemSpatialSample,
        body: &EntityBody,
        chunk: ChunkKey,
        primary: PeerGeneration,
    ) -> bool {
        if sample.id != body.id
            || sample.birth != body.birth
            || sample.epoch != body.spatial_epoch
            || !sample.authorized_issuer(body, primary)
            || !body.belongs_to(body.id, chunk)
            || sample.spatial.world != chunk.world
            || sample.spatial.position.iter().any(|value| {
                !f64::from_bits(*value).is_finite() || f64::from_bits(*value).abs() > 30_000_000.0
            })
            || !f32::from_bits(sample.spatial.yaw).is_finite()
            || !f32::from_bits(sample.spatial.pitch).is_finite()
            || !f32::from_bits(sample.head_yaw).is_finite()
            || match sample.epoch {
                SpatialEpoch::Action(epoch) => {
                    epoch.tick.is_after(sample.tick) || body.birth.action.tick.is_after(sample.tick)
                }
                SpatialEpoch::Admission { .. } => true,
            }
        {
            return false;
        }
        if let Some(previous) = self.samples.get(&body.id) {
            if previous.birth == sample.birth
                && previous.epoch == sample.epoch
                && previous.issuer == sample.issuer
                && (!sample.sequence.is_newer_than(previous.sequence)
                    || previous.tick.is_after(sample.tick))
            {
                return false;
            }
        }
        if let Some(prior) = self.samples.get(&body.id) {
            if prior.birth == sample.birth && prior.epoch == sample.epoch && prior.issuer == sample.issuer {
                if sample.tick.is_after(prior.tick) { self.previous.insert(body.id, prior.clone()); }
            } else { self.previous.remove(&body.id); }
        }
        self.samples.insert(body.id, sample);
        self.changes.insert(body.id);
        self.revision = self.revision.wrapping_add(1);
        true
    }

    pub fn project(&self, body: &EntityBody) -> EntityBody {
        let mut projected = body.clone();
        if let Some(sample) = self.samples.get(&body.id)
            && sample.matches_body(body)
        {
            projected.spatial = sample.spatial.clone();
        }
        projected
    }

    pub fn head_yaw(&self, body: &EntityBody) -> Option<u32> {
        self.samples
            .get(&body.id)
            .filter(|sample| sample.matches_body(body))
            .map(|sample| sample.head_yaw)
    }

    pub fn spatial<'a>(&'a self, body: &'a EntityBody) -> &'a EntitySpatial {
        self.samples
            .get(&body.id)
            .filter(|sample| sample.matches_body(body))
            .map_or(&body.spatial, |sample| &sample.spatial)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::{canonical_action::{ActionId, ActionRef, SpatialEpoch}, entity_body::{DroppedItem, EntityPayload}, identity::ServerId, protocol::WorldId};

    #[test]
    fn previous_tick_sample_survives_same_tick_updates_and_dirty_ids_drain() {
        let owner = PeerGeneration { server: ServerId(1), generation: 7 };
        let id = GlobalEntityId { origin: owner.server, generation: owner.generation, local_id: 1 };
        let action = ActionRef { tick: TickStamp(10), id: ActionId { actor: crate::actor::ActorId::Entity(id), incarnation: 7, sequence: 0, family: 2 } };
        let chunk = ChunkKey::new(WorldId::new(0, 0), 0, 0);
        let body = EntityBody { id, birth: EntityBirth { action, source: owner, uuid: uuid::Uuid::from_u128(1) }, owner, kind: 1,
            spatial: EntitySpatial { world: chunk.world, position: [0.5_f64.to_bits(), 80.0_f64.to_bits(), 0.5_f64.to_bits()],
                velocity_micro: [0; 3], yaw: 0, pitch: 0, on_ground: true }, spatial_epoch: SpatialEpoch::Action(action),
            mount: None, passengers: Default::default(), tags: Default::default(), tags_revision: 1, relation_versions: Default::default(),
            payload: EntityPayload::Item(DroppedItem { item: 1, count: 1, components: vec![0], age: 0, last_lifetime_tick: None,
                pickup_delay: 0, health_milli: 5000, never_despawn: false, never_pickup: false, fire_immune: false }) };
        let sample = |tick, sequence, x: f64| ItemSpatialSample { id, birth: body.birth.clone(), epoch: body.spatial_epoch,
            issuer: owner, tick: TickStamp(tick), sequence: PlayerSeq(sequence), spatial: EntitySpatial { position: [x.to_bits(), body.spatial.position[1], body.spatial.position[2]], ..body.spatial.clone() }, head_yaw: 0 };
        let mut spatial = OwnedItemSpatial::default();
        let initial = sample(10, 0, 0.5);
        assert!(spatial.apply(initial.clone(), &body, chunk, owner));
        assert!(spatial.previous_sample(id).is_none());
        assert_eq!(spatial.take_changes(), std::collections::BTreeSet::from([id]));
        assert!(spatial.take_changes().is_empty());
        assert!(spatial.apply(sample(11, 1, 0.75), &body, chunk, owner));
        assert!(spatial.apply(sample(11, 2, 1.0), &body, chunk, owner));
        assert_eq!(spatial.previous_sample(id), Some(&initial));
        assert!(!spatial.apply(sample(11, 1, 0.75), &body, chunk, owner));
        assert_eq!(spatial.take_changes(), std::collections::BTreeSet::from([id]));
        spatial.retire(id);
        assert!(spatial.previous_sample(id).is_none());
        assert_eq!(spatial.take_changes(), std::collections::BTreeSet::from([id]));
    }
}
