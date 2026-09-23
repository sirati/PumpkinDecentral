use std::collections::{BTreeMap, BTreeSet, HashMap};

use serde::{Deserialize, Serialize};

use crate::actor::ActorId;
use crate::completion::ActionKey;
use crate::entities::EntitySpawn;
use crate::identity::ServerId;
use crate::protocol::{ChunkKey, GlobalEntityId, StreamKind};
use crate::time::TickStamp;

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub struct EntityChunkTransition {
    pub entity: GlobalEntityId,
    pub sequence: u16,
    pub tick: TickStamp,
    pub from: ChunkKey,
    pub to: ChunkKey,
}

impl EntityChunkTransition {
    #[must_use]
    pub const fn action_key(self) -> ActionKey {
        ActionKey::entity_chunk_transition(&self)
    }

    #[must_use]
    pub const fn stream_kind() -> StreamKind {
        StreamKind::EntityPos
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum TransitionError {
    WrongOwner,
    UnknownActor,
    ConflictingTransition,
    SameChunk,
    StaleSequence,
    PendingPredecessor,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct IndexedEntityAction {
    pub action: ActionKey,
    pub tick: TickStamp,
    pub actor_chunks: BTreeMap<ActorId, ChunkKey>,
}

#[derive(Debug)]
struct StoredEntityAction {
    original: IndexedEntityAction,
    current: IndexedEntityAction,
}

#[derive(Debug)]
struct EntityTrack {
    current_owner: ServerId,
    ground: ChunkKey,
    last_sequence: u16,
    last_promoted: Option<EntityChunkTransition>,
    pending: Vec<EntityChunkTransition>,
}

#[derive(Debug, Default)]
pub struct EntityDepartureIndex {
    entities: HashMap<GlobalEntityId, EntityTrack>,
    actions: BTreeMap<(TickStamp, ActionKey), StoredEntityAction>,
}

impl EntityDepartureIndex {
    pub fn install(
        &mut self,
        entity: GlobalEntityId,
        current_owner: ServerId,
        chunk: ChunkKey,
        last_sequence: u16,
    ) {
        self.entities.insert(
            entity,
            EntityTrack {
                current_owner,
                ground: chunk,
                last_sequence,
                last_promoted: None,
                pending: Vec::new(),
            },
        );
    }

    pub fn transfer_owner(
        &mut self,
        entity: GlobalEntityId,
        expected: ServerId,
        next: ServerId,
    ) -> Result<(), TransitionError> {
        let Some(track) = self.entities.get_mut(&entity) else {
            return Err(TransitionError::UnknownActor);
        };
        if track.current_owner != expected {
            return Err(TransitionError::WrongOwner);
        }
        track.current_owner = next;
        Ok(())
    }

    pub fn index(&mut self, mut action: IndexedEntityAction) -> BTreeSet<ChunkKey> {
        let original = action.clone();
        self.relocate_action(&mut action);
        let chunks = action.actor_chunks.values().copied().collect();
        self.actions.insert(
            (action.tick, action.action),
            StoredEntityAction {
                original,
                current: action,
            },
        );
        chunks
    }

    pub fn transition(
        &mut self,
        source: ServerId,
        transition: EntityChunkTransition,
    ) -> Result<Vec<ActionKey>, TransitionError> {
        if transition.from == transition.to {
            return Err(TransitionError::SameChunk);
        }
        let Some(track) = self.entities.get_mut(&transition.entity) else {
            return Err(TransitionError::UnknownActor);
        };
        if track.current_owner != source {
            return Err(TransitionError::WrongOwner);
        }
        if let Some(existing) = track
            .pending
            .iter()
            .find(|pending| pending.sequence == transition.sequence)
        {
            return if *existing == transition {
                Ok(Vec::new())
            } else {
                Err(TransitionError::ConflictingTransition)
            };
        }
        if track.last_promoted == Some(transition) {
            return Ok(Vec::new());
        }
        if !sequence_after(transition.sequence, track.last_sequence) {
            return Err(TransitionError::StaleSequence);
        }
        if track
            .pending
            .iter()
            .any(|pending| pending.tick == transition.tick)
        {
            return Err(TransitionError::ConflictingTransition);
        }
        track.pending.push(transition);
        let last_sequence = track.last_sequence;
        track
            .pending
            .sort_by_key(|pending| pending.sequence.wrapping_sub(last_sequence));
        if track.pending[0].sequence.wrapping_sub(track.last_sequence) == 1
            && track.pending[0].from != track.ground
        {
            track.pending.retain(|pending| *pending != transition);
            return Err(TransitionError::ConflictingTransition);
        }
        for pair in track.pending.windows(2) {
            if pair[1].sequence.wrapping_sub(pair[0].sequence) == 1
                && pair[1].from != pair[0].to
            {
                track.pending.retain(|pending| *pending != transition);
                return Err(TransitionError::ConflictingTransition);
            }
        }
        let mut relocated = Vec::new();
        for ((_, key), action) in &mut self.actions {
            let before = action.current.actor_chunks.clone();
            action.current = action.original.clone();
            relocate_from_entities(&self.entities, &mut action.current);
            if action.current.actor_chunks != before {
                relocated.push(*key);
            }
        }
        Ok(relocated)
    }

    pub fn promote(
        &mut self,
        entity: GlobalEntityId,
        tick: TickStamp,
    ) -> Result<ChunkKey, TransitionError> {
        let Some(track) = self.entities.get_mut(&entity) else {
            return Err(TransitionError::UnknownActor);
        };
        let Some(transition) = track.pending.first().copied() else {
            return Err(TransitionError::UnknownActor);
        };
        if transition.tick != tick {
            return Err(TransitionError::PendingPredecessor);
        }
        if transition.from != track.ground {
            return Err(TransitionError::PendingPredecessor);
        }
        track.pending.remove(0);
        track.ground = transition.to;
        track.last_sequence = transition.sequence;
        track.last_promoted = Some(transition);
        Ok(transition.to)
    }

    pub fn retire_action_tick(&mut self, tick: TickStamp) -> usize {
        let before = self.actions.len();
        self.actions.retain(|(action_tick, _), action| {
            if *action_tick != tick {
                return true;
            }
            action.current.actor_chunks.keys().any(|actor| {
                let ActorId::Entity(entity) = actor else {
                    return false;
                };
                self.entities.get(entity).is_some_and(|track| {
                    track
                        .pending
                        .iter()
                        .any(|transition| action_tick.is_after(transition.tick))
                })
            })
        });
        before - self.actions.len()
    }

    #[must_use]
    pub fn action_chunks(&self, tick: TickStamp, action: ActionKey) -> BTreeSet<ChunkKey> {
        self.actions
            .get(&(tick, action))
            .map(|entry| entry.current.actor_chunks.values().copied().collect())
            .unwrap_or_default()
    }

    #[must_use]
    pub fn departure(&self, entity: GlobalEntityId) -> Option<EntityChunkTransition> {
        self.entities
            .get(&entity)
            .and_then(|track| track.pending.first().copied())
    }

    #[must_use]
    pub fn pending(&self, entity: GlobalEntityId) -> &[EntityChunkTransition] {
        self.entities
            .get(&entity)
            .map_or(&[], |track| track.pending.as_slice())
    }

    fn relocate_action(&self, action: &mut IndexedEntityAction) {
        relocate_from_entities(&self.entities, action);
    }
}

fn relocate_from_entities(
    entities: &HashMap<GlobalEntityId, EntityTrack>,
    action: &mut IndexedEntityAction,
) {
    for (actor, chunk) in &mut action.actor_chunks {
        let ActorId::Entity(entity) = actor else {
            continue;
        };
        let Some(track) = entities.get(entity) else {
            continue;
        };
        for transition in &track.pending {
            if tick_after(action.tick, transition.tick) && *chunk == transition.from {
                *chunk = transition.to;
            }
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq)]
pub enum EntityBoundaryDisposition {
    Leave,
    Snapshot(EntitySpawn),
}

#[derive(Debug, Clone, Copy, PartialEq)]
pub struct RoutedEntityTransition {
    pub peer: ServerId,
    pub transition: EntityChunkTransition,
    pub disposition: EntityBoundaryDisposition,
}

#[must_use]
pub fn route_transition(
    transition: EntityChunkTransition,
    old_holders: &[ServerId],
    new_holders: &[ServerId],
    snapshot: EntitySpawn,
) -> Vec<RoutedEntityTransition> {
    let old = old_holders.iter().copied().collect::<BTreeSet<_>>();
    let new = new_holders.iter().copied().collect::<BTreeSet<_>>();
    old.union(&new)
        .copied()
        .map(|peer| RoutedEntityTransition {
            peer,
            transition,
            disposition: if new.contains(&peer) {
                EntityBoundaryDisposition::Snapshot(snapshot)
            } else {
                EntityBoundaryDisposition::Leave
            },
        })
        .collect()
}

fn tick_after(tick: TickStamp, boundary: TickStamp) -> bool {
    tick.is_after(boundary)
}

fn sequence_after(sequence: u16, boundary: u16) -> bool {
    crate::identity::PlayerSeq(sequence).is_newer_than(crate::identity::PlayerSeq(boundary))
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::entities::EntityVisualState;
    use crate::identity::{GlobalPlayerId, PlayerSlot};
    use crate::protocol::{EntityRef, WorldId};

    const WORLD: WorldId = WorldId::new(0, 19);

    fn chunk(x: i32) -> ChunkKey {
        ChunkKey::new(WORLD, x, 0)
    }

    fn entity() -> GlobalEntityId {
        GlobalEntityId {
            origin: ServerId(2),
            local_id: 41,
            generation: 7,
        }
    }

    fn transition(tick: u16) -> EntityChunkTransition {
        transition_with(5, tick, chunk(0), chunk(1))
    }

    fn transition_with(
        sequence: u16,
        tick: u16,
        from: ChunkKey,
        to: ChunkKey,
    ) -> EntityChunkTransition {
        EntityChunkTransition {
            entity: entity(),
            sequence,
            tick: TickStamp(tick),
            from,
            to,
        }
    }

    fn action(sequence: u16, tick: u16, references: &[(ActorId, ChunkKey)]) -> IndexedEntityAction {
        IndexedEntityAction {
            action: ActionKey {
                actor: GlobalPlayerId::new(ServerId(3), PlayerSlot(9)).into(),
                sequence,
                family: 3,
            },
            tick: TickStamp(tick),
            actor_chunks: references.iter().copied().collect(),
        }
    }

    #[test]
    fn crossing_becomes_visible_after_the_transition_tick() {
        let actor = ActorId::Entity(entity());
        let mut index = EntityDepartureIndex::default();
        index.install(entity(), ServerId(2), chunk(0), 4);
        let same_tick = action(1, 12, &[(actor, chunk(0))]);
        let next_tick = action(2, 13, &[(actor, chunk(0))]);
        index.index(same_tick);
        index.index(next_tick);

        let relocated = index.transition(ServerId(2), transition(12)).unwrap();

        assert_eq!(relocated.len(), 1);
        assert!(
            index
                .action_chunks(
                    TickStamp(12),
                    ActionKey {
                        actor: GlobalPlayerId::new(ServerId(3), PlayerSlot(9)).into(),
                        sequence: 1,
                        family: 3,
                    }
                )
                .contains(&chunk(0))
        );
        assert!(
            index
                .action_chunks(TickStamp(13), relocated[0])
                .contains(&chunk(1))
        );
    }

    #[test]
    fn late_old_chunk_reference_is_relocated_without_a_message() {
        let actor = ActorId::Entity(entity());
        let target = ActorId::Player(GlobalPlayerId::new(ServerId(4), PlayerSlot(8)));
        let mut index = EntityDepartureIndex::default();
        index.install(entity(), ServerId(2), chunk(0), 4);
        index.transition(ServerId(2), transition(u16::MAX)).unwrap();

        let chunks = index.index(action(
            4,
            0,
            &[(target, chunk(9)), (actor, chunk(0))],
        ));

        assert_eq!(chunks, BTreeSet::from([chunk(1), chunk(9)]));
    }

    #[test]
    fn joint_action_relocates_every_entity_reference_and_keeps_both_chunks() {
        let first = entity();
        let second = GlobalEntityId {
            origin: ServerId(4),
            local_id: 8,
            generation: 2,
        };
        let mut index = EntityDepartureIndex::default();
        index.install(first, ServerId(2), chunk(0), 4);
        index.install(second, ServerId(4), chunk(7), 10);
        index.transition(ServerId(2), transition(12)).unwrap();
        index
            .transition(
                ServerId(4),
                EntityChunkTransition {
                    entity: second,
                    sequence: 11,
                    tick: TickStamp(12),
                    from: chunk(7),
                    to: chunk(8),
                },
            )
            .unwrap();

        let chunks = index.index(action(
            9,
            13,
            &[
                (ActorId::Entity(first), chunk(0)),
                (ActorId::Entity(second), chunk(7)),
            ],
        ));

        assert_eq!(chunks, BTreeSet::from([chunk(1), chunk(8)]));
    }

    #[test]
    fn another_peer_cannot_move_or_supersede_the_owned_entity() {
        let mut index = EntityDepartureIndex::default();
        index.install(entity(), ServerId(2), chunk(0), 4);
        assert_eq!(
            index.transition(ServerId(8), transition(4)),
            Err(TransitionError::WrongOwner)
        );
        index.transition(ServerId(2), transition(4)).unwrap();
        assert_eq!(
            index.transition(ServerId(2), transition(5)),
            Err(TransitionError::ConflictingTransition)
        );
    }

    #[test]
    fn promotion_releases_departure_and_changes_the_ground_origin() {
        let mut index = EntityDepartureIndex::default();
        index.install(entity(), ServerId(2), chunk(0), 4);
        index.transition(ServerId(2), transition(12)).unwrap();
        assert_eq!(index.promote(entity(), TickStamp(12)), Ok(chunk(1)));
        assert_eq!(index.departure(entity()), None);
        let mut later = transition(13);
        later.sequence = 6;
        later.from = chunk(1);
        later.to = chunk(2);
        assert!(index.transition(ServerId(2), later).is_ok());
    }

    #[test]
    fn ownership_transfer_changes_authority_without_changing_identity() {
        let mut index = EntityDepartureIndex::default();
        index.install(entity(), ServerId(2), chunk(0), 4);
        index
            .transfer_owner(entity(), ServerId(2), ServerId(7))
            .unwrap();
        assert_eq!(
            index.transition(ServerId(2), transition(12)),
            Err(TransitionError::WrongOwner)
        );
        assert!(index.transition(ServerId(7), transition(12)).is_ok());
        assert_eq!(entity().origin, ServerId(2));
    }

    #[test]
    fn consecutive_unresolved_crossings_relocate_through_the_chain() {
        let actor = ActorId::Entity(entity());
        let mut index = EntityDepartureIndex::default();
        index.install(entity(), ServerId(2), chunk(0), 4);
        index
            .transition(
                ServerId(2),
                transition_with(5, 12, chunk(0), chunk(1)),
            )
            .unwrap();
        index
            .transition(
                ServerId(2),
                transition_with(6, 13, chunk(1), chunk(2)),
            )
            .unwrap();

        let chunks = index.index(action(9, 14, &[(actor, chunk(0))]));

        assert_eq!(chunks, BTreeSet::from([chunk(2)]));
        assert_eq!(index.pending(entity()).len(), 2);
    }

    #[test]
    fn out_of_order_crossings_settle_into_the_same_chain() {
        let actor = ActorId::Entity(entity());
        let mut ordered = EntityDepartureIndex::default();
        let mut reversed = EntityDepartureIndex::default();
        ordered.install(entity(), ServerId(2), chunk(0), 4);
        reversed.install(entity(), ServerId(2), chunk(0), 4);
        let first = transition_with(5, 12, chunk(0), chunk(1));
        let second = transition_with(6, 13, chunk(1), chunk(2));
        ordered.transition(ServerId(2), first).unwrap();
        ordered.transition(ServerId(2), second).unwrap();
        reversed.transition(ServerId(2), second).unwrap();
        reversed.transition(ServerId(2), first).unwrap();

        let reference = action(9, 14, &[(actor, chunk(0))]);
        assert_eq!(ordered.index(reference.clone()), reversed.index(reference));
        assert_eq!(ordered.pending(entity()), reversed.pending(entity()));
        for index in [&mut ordered, &mut reversed] {
            assert_eq!(index.promote(entity(), TickStamp(12)), Ok(chunk(1)));
            assert_eq!(index.promote(entity(), TickStamp(13)), Ok(chunk(2)));
        }
    }

    #[test]
    fn duplicate_is_idempotent_and_stale_wrapping_sequence_is_rejected() {
        let mut index = EntityDepartureIndex::default();
        index.install(entity(), ServerId(2), chunk(0), u16::MAX);
        let crossing = transition_with(0, 12, chunk(0), chunk(1));
        assert!(index.transition(ServerId(2), crossing).unwrap().is_empty());
        assert!(index.transition(ServerId(2), crossing).unwrap().is_empty());
        assert_eq!(index.promote(entity(), TickStamp(12)), Ok(chunk(1)));
        assert!(index.transition(ServerId(2), crossing).unwrap().is_empty());
        assert_eq!(
            index.transition(
                ServerId(2),
                transition_with(u16::MAX, 11, chunk(0), chunk(1))
            ),
            Err(TransitionError::StaleSequence)
        );
    }

    #[test]
    fn accepted_tick_retirement_bounds_action_references() {
        let actor = ActorId::Entity(entity());
        let mut index = EntityDepartureIndex::default();
        index.install(entity(), ServerId(2), chunk(0), 4);
        for sequence in 1..=64 {
            index.index(action(sequence, 20, &[(actor, chunk(0))]));
        }
        assert_eq!(index.retire_action_tick(TickStamp(20)), 64);
        assert!(
            index
                .action_chunks(
                    TickStamp(20),
                    ActionKey {
                        actor: GlobalPlayerId::new(ServerId(3), PlayerSlot(9)).into(),
                        sequence: 64,
                        family: 3,
                    }
                )
                .is_empty()
        );
    }

    #[test]
    fn action_references_remain_until_relevant_departure_reaches_ground() {
        let actor = ActorId::Entity(entity());
        let mut index = EntityDepartureIndex::default();
        index.install(entity(), ServerId(2), chunk(0), 4);
        index.transition(ServerId(2), transition(12)).unwrap();
        index.index(action(9, 13, &[(actor, chunk(0))]));
        assert_eq!(index.retire_action_tick(TickStamp(13)), 0);
        index.promote(entity(), TickStamp(12)).unwrap();
        assert_eq!(index.retire_action_tick(TickStamp(13)), 1);
    }

    #[test]
    fn player_and_entity_departures_change_association_on_the_next_tick() {
        let player = GlobalPlayerId::new(ServerId(3), PlayerSlot(9));
        let player_actor = crate::boundary::ActorChunkId::Player(player);
        let mut player_index = crate::boundary::DepartureIndex::default();
        player_index.install(player_actor, chunk(0));
        player_index.transition(crate::boundary::ChunkTransition {
            actor: player_actor,
            tick: TickStamp(12),
            from: chunk(0),
            to: chunk(1),
        });

        let entity_actor = ActorId::Entity(entity());
        let mut entity_index = EntityDepartureIndex::default();
        entity_index.install(entity(), ServerId(2), chunk(0), 4);
        entity_index
            .transition(ServerId(2), transition(12))
            .unwrap();

        assert_eq!(
            player_index.index(crate::boundary::IndexedAction {
                action: 1,
                actor: player_actor,
                tick: TickStamp(12),
                chunk: chunk(0),
            }),
            chunk(0)
        );
        assert_eq!(
            entity_index.index(action(1, 12, &[(entity_actor, chunk(0))])),
            BTreeSet::from([chunk(0)])
        );
        assert_eq!(
            player_index.index(crate::boundary::IndexedAction {
                action: 2,
                actor: player_actor,
                tick: TickStamp(13),
                chunk: chunk(0),
            }),
            chunk(1)
        );
        assert_eq!(
            entity_index.index(action(2, 13, &[(entity_actor, chunk(0))])),
            BTreeSet::from([chunk(1)])
        );
    }

    #[test]
    fn boundary_routes_leave_to_old_only_and_snapshot_to_every_new_holder() {
        let entity_ref = EntityRef {
            origin: entity().origin,
            local_id: entity().local_id,
            generation: entity().generation,
            chunk: chunk(1),
        };
        let snapshot = EntitySpawn {
            entity: entity_ref,
            tick: TickStamp(12),
            kind: 6,
            pos: [16.5, 70.0, 0.5],
            yaw: 90.0,
            pitch: 0.0,
            visual: EntityVisualState::default(),
        };
        let routed = route_transition(
            transition(12),
            &[ServerId(2), ServerId(3)],
            &[ServerId(3), ServerId(4)],
            snapshot,
        );

        assert_eq!(
            routed.iter().map(|route| route.peer).collect::<Vec<_>>(),
            vec![ServerId(2), ServerId(3), ServerId(4)]
        );
        assert!(matches!(
            routed[0].disposition,
            EntityBoundaryDisposition::Leave
        ));
        assert!(matches!(
            routed[1].disposition,
            EntityBoundaryDisposition::Snapshot(_)
        ));
        assert!(matches!(
            routed[2].disposition,
            EntityBoundaryDisposition::Snapshot(_)
        ));
    }
}
