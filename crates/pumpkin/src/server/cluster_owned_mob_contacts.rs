use std::collections::{BTreeMap, BTreeSet};

use pumpkin_cluster::actor::ActorId;
use pumpkin_cluster::canonical_action::ApplyFailure;
use pumpkin_cluster::item_contact::{ContactIdentity, ContactObservation};
use pumpkin_cluster::protocol::ChunkKey;

pub(super) struct MobContactIndex {
    observations: Vec<ContactObservation>,
    actors: BTreeMap<ActorId, usize>,
    chunks: BTreeMap<ChunkKey, Vec<usize>>,
    players: BTreeMap<uuid::Uuid, Option<usize>>,
    nonspatial: BTreeMap<ActorId, pumpkin_cluster::actor_relation::ActorRelationParticipant>,
}

impl MobContactIndex {
    pub(super) fn new(observations: Vec<ContactObservation>) -> Result<Self, ApplyFailure> {
        let mut actors = BTreeMap::new();
        let mut players = BTreeMap::new();
        let mut chunks = BTreeMap::<ChunkKey, Vec<usize>>::new();
        for (index, observation) in observations.iter().enumerate() {
            if !observation.valid() || actors.insert(observation.actor(), index).is_some() {
                return Err(ApplyFailure::InvalidAction);
            }
            if let ContactIdentity::Player(lease) = &observation.identity {
                players.entry(lease.key.0).and_modify(|known| *known = None).or_insert(Some(index));
            }
            chunks.entry(observation.chunk).or_default().push(index);
        }
        Ok(Self { observations, actors, chunks, players, nonspatial: BTreeMap::new() })
    }

    pub(super) fn observations(&self) -> &[ContactObservation] {
        &self.observations
    }

    pub(super) fn get(&self, identity: &ContactIdentity) -> Option<&ContactObservation> {
        let actor = match identity {
            ContactIdentity::Player(lease) => ActorId::Player(lease.gid),
            ContactIdentity::Entity { id, .. } => ActorId::Entity(*id),
        };
        let observation = self.observations.get(*self.actors.get(&actor)?)?;
        (observation.identity == *identity).then_some(observation)
    }

    pub(super) fn player(&self, uuid: uuid::Uuid) -> Option<&ContactObservation> {
        self.observations.get((*self.players.get(&uuid)?)?)
    }

    pub(super) fn in_chunks<'a>(&'a self, chunks: &'a BTreeSet<ChunkKey>)
        -> impl Iterator<Item = &'a ContactObservation> + 'a {
        chunks.iter().filter_map(|chunk| self.chunks.get(chunk))
            .flat_map(|indices| indices.iter().map(|index| &self.observations[*index]))
    }
}


impl MobContactIndex {
    pub(super) fn with_entity_participants(mut self,
        participants: impl IntoIterator<Item = pumpkin_cluster::actor_relation::ActorRelationParticipant>)
        -> Result<Self, ApplyFailure> {
        for participant in participants {
            if let Some(contact) = self.get(&participant.identity) {
                if contact.owner != participant.owner || contact.epoch != participant.epoch || contact.chunk != participant.chunk {
                    return Err(ApplyFailure::InvalidAction);
                }
            } else if self.nonspatial.insert(participant.actor(), participant).is_some() {
                return Err(ApplyFailure::InvalidAction);
            }
        }
        Ok(self)
    }

    pub(super) fn participant(&self, identity: &ContactIdentity)
        -> Option<pumpkin_cluster::actor_relation::ActorRelationParticipant> {
        if let Some(contact) = self.get(identity) {
            return pumpkin_cluster::actor_relation::ActorRelationParticipant::from_contact(contact);
        }
        let actor = match identity {
            ContactIdentity::Player(lease) => ActorId::Player(lease.gid),
            ContactIdentity::Entity { id, .. } => ActorId::Entity(*id),
        };
        self.nonspatial.get(&actor).filter(|participant| participant.identity == *identity).cloned()
    }
}
