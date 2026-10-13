use std::collections::HashMap;
use std::sync::Arc;

use crate::canonical_action::ActionRef;
use crate::protocol::GlobalEntityId;
use crate::realm_completion::PeerGeneration;

#[derive(Debug, Clone, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
pub struct EntityOriginIdentity {
    pub id: GlobalEntityId,
    pub uuid: uuid::Uuid,
    pub source: PeerGeneration,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum EntityOriginFailure {
    InvalidNamespace,
    WrongNamespace,
    Capacity,
    Exhausted,
    ChangedReservation,
}

pub struct EntityOriginAllocator {
    source: PeerGeneration,
    next: u32,
    reservations: HashMap<ActionRef, Arc<[EntityOriginIdentity]>>,
    reserved_count: usize,
    imported_uuids: HashMap<uuid::Uuid, GlobalEntityId>,
}

impl EntityOriginAllocator {
    pub fn new(admitted_source: PeerGeneration) -> Result<Self, EntityOriginFailure> {
        if admitted_source.generation == 0 {
            return Err(EntityOriginFailure::InvalidNamespace);
        }
        Ok(Self {
            source: admitted_source,
            next: 1,
            reservations: HashMap::new(),
            reserved_count: 0,
            imported_uuids: HashMap::new(),
        })
    }

    pub fn source(&self) -> PeerGeneration {
        self.source
    }

    pub fn reservation(&self, cause: ActionRef) -> Option<&[EntityOriginIdentity]> {
        self.reservations.get(&cause).map(AsRef::as_ref)
    }

    pub fn reserve(
        &mut self,
        admitted_source: PeerGeneration,
        cause: ActionRef,
        count: usize,
    ) -> Result<Arc<[EntityOriginIdentity]>, EntityOriginFailure> {
        if admitted_source != self.source {
            return Err(EntityOriginFailure::WrongNamespace);
        }
        if let Some(existing) = self.reservations.get(&cause) {
            return if existing.len() == count {
                Ok(existing.clone())
            } else {
                Err(EntityOriginFailure::ChangedReservation)
            };
        }
        if count == 0 || count > 4096 || self.reserved_count.saturating_add(count) > 4096 {
            return Err(EntityOriginFailure::Capacity);
        }
        let count_u32 = u32::try_from(count).map_err(|_| EntityOriginFailure::Capacity)?;
        let next = self
            .next
            .checked_add(count_u32)
            .ok_or(EntityOriginFailure::Exhausted)?;
        if next - 1 > i32::MAX as u32 {
            return Err(EntityOriginFailure::Exhausted);
        }
        let values = (self.next..next)
            .map(|local_id| EntityOriginIdentity {
                id: GlobalEntityId {
                    origin: self.source.server,
                    local_id: i32::try_from(local_id).expect("checked entity origin ordinal"),
                    generation: self.source.generation,
                },
                uuid: uuid::Uuid::new_v4(),
                source: self.source,
            })
            .collect::<Vec<_>>();
        let values: Arc<[EntityOriginIdentity]> = values.into();
        self.next = next;
        self.reserved_count += count;
        self.reservations.insert(cause, values.clone());
        Ok(values)
    }

    pub fn reserve_existing_uuids(
        &mut self,
        admitted_source: PeerGeneration,
        cause: ActionRef,
        uuids: &[uuid::Uuid],
    ) -> Result<Arc<[EntityOriginIdentity]>, EntityOriginFailure> {
        if admitted_source != self.source || cause.id.actor.origin() != self.source.server
            || cause.id.incarnation != self.source.generation { return Err(EntityOriginFailure::WrongNamespace); }
        if let Some(existing) = self.reservations.get(&cause) {
            return if existing.iter().map(|identity| identity.uuid).eq(uuids.iter().copied()) {
                Ok(existing.clone())
            } else { Err(EntityOriginFailure::ChangedReservation) };
        }
        let unique = uuids.iter().copied().collect::<std::collections::HashSet<_>>();
        if unique.len() != uuids.len() || uuids.iter().any(|uuid| uuid.is_nil() || self.imported_uuids.contains_key(uuid)) {
            return Err(EntityOriginFailure::ChangedReservation);
        }
        if uuids.is_empty() || self.reserved_count.checked_add(uuids.len()).is_none_or(|count| count > 1_048_576)
            || self.imported_uuids.len().checked_add(uuids.len()).is_none_or(|count| count > 1_048_576) {
            return Err(EntityOriginFailure::Capacity);
        }
        let count = u32::try_from(uuids.len()).map_err(|_| EntityOriginFailure::Capacity)?;
        let next = self.next.checked_add(count).ok_or(EntityOriginFailure::Exhausted)?;
        if next - 1 > i32::MAX as u32 { return Err(EntityOriginFailure::Exhausted); }
        let identities: Arc<[EntityOriginIdentity]> = uuids.iter().zip(self.next..next).map(|(uuid, local_id)| EntityOriginIdentity {
            id: GlobalEntityId { origin: self.source.server, local_id: local_id as i32, generation: self.source.generation },
            uuid: *uuid, source: self.source,
        }).collect::<Vec<_>>().into();
        for identity in identities.iter() { self.imported_uuids.insert(identity.uuid, identity.id); }
        self.next = next;
        self.reserved_count += identities.len();
        self.reservations.insert(cause, identities.clone());
        Ok(identities)
    }

    pub fn release_issued(&mut self, cause: ActionRef) {
        self.retire_settled(cause);
    }

    pub fn retire_settled(&mut self, cause: ActionRef) {
        if let Some(values) = self.reservations.remove(&cause) {
            self.reserved_count -= values.len();
        }
    }

    pub fn discard_unissued(&mut self, cause: ActionRef) { self.retire_settled(cause); }

    pub fn retire_settled_tick(&mut self, tick: crate::time::TickStamp) {
        self.reservations.retain(|cause, identities| {
            if cause.tick == tick { self.reserved_count -= identities.len(); false } else { true }
        });
    }
}

#[cfg(test)]
mod bootstrap_tests {
    use super::*;
    use crate::actor::ActorId;
    use crate::canonical_action::ActionId;
    use crate::identity::ServerId;
    use crate::time::TickStamp;

    #[test]
    fn persisted_uuid_reservation_retries_exactly_without_reusing_ids() {
        let source = PeerGeneration { server: ServerId(3), generation: 7 };
        let mut allocator = EntityOriginAllocator::new(source).unwrap();
        let cause = ActionRef { tick: TickStamp(9), id: ActionId {
            actor: ActorId::Peer(source.server), incarnation: source.generation, sequence: 1, family: 1,
        } };
        let uuids = [uuid::Uuid::from_u128(1), uuid::Uuid::from_u128(2)];
        let initial = allocator.reserve_existing_uuids(source, cause, &uuids).unwrap();
        assert_eq!(initial.iter().map(|identity| identity.uuid).collect::<Vec<_>>(), uuids);
        assert!(Arc::ptr_eq(&initial, &allocator.reserve_existing_uuids(source, cause, &uuids).unwrap()));
        assert_eq!(allocator.reserve_existing_uuids(source, cause, &[uuids[1], uuids[0]]), Err(EntityOriginFailure::ChangedReservation));
        allocator.discard_unissued(cause);
        assert_eq!(allocator.reserve_existing_uuids(source, cause, &uuids), Err(EntityOriginFailure::ChangedReservation));
        let next = allocator.reserve_existing_uuids(source, cause, &[uuid::Uuid::from_u128(3)]).unwrap();
        assert!(next[0].id.local_id > initial[1].id.local_id);
    }
}
