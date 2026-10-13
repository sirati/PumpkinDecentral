use crate::structural_map::StructuralMap;
use std::collections::{BTreeMap, BTreeSet};
use std::ops::Deref;

use crate::canonical_action::{ActionId, ChunkPart, Target};

type Posting<I> = BTreeMap<I, u16>;

#[derive(Clone)]
pub struct LocationPostings<I: Ord + Copy = ActionId> {
    exact: StructuralMap<Target, Posting<I>>,
    overlap: StructuralMap<Target, Posting<I>>,
    facets: StructuralMap<Target, BTreeSet<Target>>,
}

impl<I: Ord + Copy> Default for LocationPostings<I> {
    fn default() -> Self {
        Self {
            exact: StructuralMap::default(),
            overlap: StructuralMap::default(),
            facets: StructuralMap::default(),
        }
    }
}

impl<I: Ord + Copy> Deref for LocationPostings<I> {
    type Target = StructuralMap<Target, Posting<I>>;
    fn deref(&self) -> &Self::Target {
        &self.exact
    }
}

impl<I: Ord + Copy> LocationPostings<I> {
    pub(crate) fn clear(&mut self) {
        self.exact.clear();
        self.overlap.clear();
        self.facets.clear();
    }

    pub fn get(&self, target: &Target) -> Option<&Posting<I>> {
        if let Some(body) = crate::derived_living::actor_body_target(*target) {
            self.overlap.get(target).or_else(|| self.exact.get(&body))
        } else if is_body(*target) {
            self.overlap.get(target).or_else(|| self.exact.get(target))
        } else {
            self.exact.get(target)
        }
    }

    pub(crate) fn insert_reference(&mut self, target: Target, id: I) {
        if !self
            .exact
            .get(&target)
            .is_some_and(|posting| posting.contains_key(&id))
        {
            self.add(target, id);
        }
    }

    pub(crate) fn add(&mut self, target: Target, id: I) {
        if let Some(body) = crate::derived_living::actor_body_target(target) {
            let inherited = self.exact.get(&body).cloned().unwrap_or_default();
            increment(self.overlap.entry(target).or_insert(inherited), id, 1);
            increment(self.overlap.entry(body).or_default(), id, 1);
            self.facets.entry(body).or_default().insert(target);
        } else if is_body(target) {
            increment(self.overlap.entry(target).or_default(), id, 1);
            for facet in self.facets.get(&target).into_iter().flatten() {
                increment(
                    self.overlap
                        .get_mut(facet)
                        .expect("registered facet posting"),
                    id,
                    1,
                );
            }
        }
        increment(self.exact.entry(target).or_default(), id, 1);
    }

    pub(crate) fn subtract(&mut self, target: Target, id: I, count: u16) {
        decrement(
            self.exact.get_mut(&target).expect("indexed location"),
            id,
            count,
        );
        if self.exact[&target].is_empty() {
            self.exact.remove(&target);
        }
        if let Some(body) = crate::derived_living::actor_body_target(target) {
            decrement(
                self.overlap.get_mut(&target).expect("indexed facet"),
                id,
                count,
            );
            decrement(
                self.overlap.get_mut(&body).expect("indexed body"),
                id,
                count,
            );
        } else if is_body(target) {
            decrement(
                self.overlap.get_mut(&target).expect("indexed body"),
                id,
                count,
            );
            for facet in self.facets.get(&target).into_iter().flatten() {
                decrement(
                    self.overlap
                        .get_mut(facet)
                        .expect("registered facet posting"),
                    id,
                    count,
                );
            }
        }
    }

    pub(crate) fn remove_action(&mut self, target: Target, id: I) {
        if let Some(count) = self
            .exact
            .get(&target)
            .and_then(|actions| actions.get(&id))
            .copied()
        {
            self.subtract(target, id, count);
        }
    }
}

fn is_body(target: Target) -> bool {
    matches!(
        target,
        Target::Chunk {
            part: ChunkPart::Player(_) | ChunkPart::Entity(_),
            inventory: None,
            ..
        }
    )
}

fn increment<I: Ord + Copy>(posting: &mut Posting<I>, id: I, count: u16) {
    let value = posting.entry(id).or_default();
    *value = value
        .checked_add(count)
        .expect("bounded action footprint posting");
}

fn decrement<I: Ord + Copy>(posting: &mut Posting<I>, id: I, count: u16) {
    let value = posting.get_mut(&id).expect("indexed action");
    *value = value.checked_sub(count).expect("indexed posting count");
    if *value == 0 {
        posting.remove(&id);
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::actor::ActorId;
    use crate::identity::ServerId;
    use crate::living_property::LivingProperty;
    use crate::protocol::{ChunkKey, GlobalEntityId, WorldId};

    #[test]
    fn whole_body_overlaps_each_facet_without_crossing_between_facets() {
        let actor = ActorId::Entity(GlobalEntityId {
            origin: ServerId(1),
            local_id: 3,
            generation: 4,
        });
        let key = ChunkKey::new(WorldId::new(0, 0), 0, 0);
        let facet = |property| Target::Chunk {
            key,
            part: ChunkPart::LivingProperty { actor, property },
            inventory: None,
        };
        let health = facet(LivingProperty::Health);
        let fire = facet(LivingProperty::FireTicks);
        let passenger = Target::Chunk {
            key,
            part: ChunkPart::ActorRelation {
                actor,
                relation: crate::actor_relation::ActorRelation::Passenger(0),
            },
            inventory: None,
        };
        let body = crate::living_property::body_target(health).unwrap().0;
        let action = |sequence| ActionId {
            actor,
            incarnation: 4,
            sequence,
            family: 0,
        };
        let health_id = action(1);
        let fire_id = action(2);
        let body_id = action(3);
        for order in [[health, fire, body], [body, fire, health]] {
            let mut index = LocationPostings::default();
            for target in order {
                index.add(
                    target,
                    if target == body {
                        body_id
                    } else if target == health {
                        health_id
                    } else {
                        fire_id
                    },
                );
            }
            assert_eq!(
                index
                    .get(&health)
                    .unwrap()
                    .keys()
                    .copied()
                    .collect::<BTreeSet<_>>(),
                BTreeSet::from([health_id, body_id])
            );
            assert_eq!(
                index
                    .get(&fire)
                    .unwrap()
                    .keys()
                    .copied()
                    .collect::<BTreeSet<_>>(),
                BTreeSet::from([fire_id, body_id])
            );
            assert_eq!(index.get(&body).unwrap().len(), 3);
            index.add(body, body_id);
            assert_eq!(index.get(&health).unwrap()[&body_id], 2);
            index.subtract(body, body_id, 1);
            index.remove_action(body, body_id);
            assert_eq!(index.get(&health).unwrap().len(), 1);
            index.remove_action(health, health_id);
            assert!(index.get(&health).unwrap().is_empty());
            assert_eq!(
                index
                    .get(&body)
                    .unwrap()
                    .keys()
                    .copied()
                    .collect::<Vec<_>>(),
                vec![fire_id]
            );
            index.remove_action(fire, fire_id);
            assert!(index.is_empty());
            index.add(passenger, fire_id);
            index.add(health, health_id);
            index.add(body, body_id);
            assert_eq!(
                index
                    .get(&passenger)
                    .unwrap()
                    .keys()
                    .copied()
                    .collect::<BTreeSet<_>>(),
                BTreeSet::from([fire_id, body_id])
            );
            assert_eq!(
                index
                    .get(&health)
                    .unwrap()
                    .keys()
                    .copied()
                    .collect::<BTreeSet<_>>(),
                BTreeSet::from([health_id, body_id])
            );
        }
    }
}
