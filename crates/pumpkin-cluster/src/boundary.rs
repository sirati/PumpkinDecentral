use std::collections::{BTreeMap, BTreeSet, HashMap};

use crate::identity::{GlobalPlayerId, ServerId};
use crate::protocol::{
    ChunkKey, EntityRef, PlayerBoundaryUpdate, PlayerReplicaSnapshot, PosUpdate,
};
use crate::time::TickStamp;

#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub enum ActorChunkId {
    Player(GlobalPlayerId),
    Entity {
        origin: ServerId,
        local_id: i32,
        generation: u64,
    },
}

impl From<EntityRef> for ActorChunkId {
    fn from(entity: EntityRef) -> Self {
        Self::Entity {
            origin: entity.origin,
            local_id: entity.local_id,
            generation: entity.generation,
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct ChunkTransition {
    pub actor: ActorChunkId,
    pub tick: TickStamp,
    pub from: ChunkKey,
    pub to: ChunkKey,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct IndexedAction {
    pub action: u64,
    pub actor: ActorChunkId,
    pub tick: TickStamp,
    pub chunk: ChunkKey,
}

#[derive(Debug, Default)]
pub struct DepartureIndex {
    ground: HashMap<ActorChunkId, ChunkKey>,
    departures: HashMap<ActorChunkId, ChunkTransition>,
    actions: BTreeMap<(TickStamp, u64), IndexedAction>,
}

impl DepartureIndex {
    pub fn install(&mut self, actor: ActorChunkId, chunk: ChunkKey) {
        self.ground.insert(actor, chunk);
    }

    pub fn index(&mut self, mut action: IndexedAction) -> ChunkKey {
        if let Some(transition) = self.departures.get(&action.actor)
            && tick_after(action.tick, transition.tick)
            && action.chunk == transition.from
        {
            action.chunk = transition.to;
        }
        let chunk = action.chunk;
        self.actions.insert((action.tick, action.action), action);
        chunk
    }

    pub fn transition(&mut self, transition: ChunkTransition) -> Vec<u64> {
        if self.ground.get(&transition.actor).copied() != Some(transition.from) {
            return Vec::new();
        }
        self.departures.insert(transition.actor, transition);
        let mut relocated = Vec::new();
        for action in self.actions.values_mut() {
            if action.actor == transition.actor
                && tick_after(action.tick, transition.tick)
                && action.chunk == transition.from
            {
                action.chunk = transition.to;
                relocated.push(action.action);
            }
        }
        relocated
    }

    pub fn promote(&mut self, actor: ActorChunkId, tick: TickStamp) -> bool {
        let Some(transition) = self.departures.get(&actor).copied() else {
            return false;
        };
        if transition.tick != tick {
            return false;
        }
        self.departures.remove(&actor);
        self.ground.insert(actor, transition.to);
        true
    }

    #[must_use]
    pub fn action_chunk(&self, tick: TickStamp, action: u64) -> Option<ChunkKey> {
        self.actions.get(&(tick, action)).map(|entry| entry.chunk)
    }

    #[must_use]
    pub fn departure(&self, actor: ActorChunkId) -> Option<ChunkTransition> {
        self.departures.get(&actor).copied()
    }
}

fn tick_after(tick: TickStamp, boundary: TickStamp) -> bool {
    tick.is_after(boundary)
}

#[derive(Debug, Clone, Copy, PartialEq)]
pub struct RoutedBoundary {
    pub peer: ServerId,
    pub gid: GlobalPlayerId,
    pub update: PlayerBoundaryUpdate,
}

#[derive(Debug, Default)]
pub struct PlayerBoundaryTracker {
    chunks: HashMap<GlobalPlayerId, (u64, ChunkKey, crate::identity::PlayerSeq)>,
    holders: HashMap<GlobalPlayerId, BTreeSet<u16>>,
}

impl PlayerBoundaryTracker {
    #[must_use]
    pub fn new() -> Self {
        Self::default()
    }

    pub fn route(
        &mut self,
        updates: &[PosUpdate],
        holders: impl Fn(ChunkKey) -> Vec<u16>,
        snapshot: impl Fn(PosUpdate) -> PlayerReplicaSnapshot,
    ) -> Vec<RoutedBoundary> {
        let mut routed = Vec::new();
        for update in updates {
            if self
                .chunks
                .get(&update.gid)
                .is_some_and(|(incarnation, _, _)| *incarnation > update.incarnation)
            {
                continue;
            }
            let next = chunk_of(update);
            let previous = self
                .chunks
                .insert(update.gid, (update.incarnation, next, update.seq));
            let new_holders = holders(next).into_iter().collect::<BTreeSet<_>>();
            let old_holders = self
                .holders
                .insert(update.gid, new_holders.clone())
                .unwrap_or_default();
            if previous.is_some_and(|(incarnation, chunk, _)| {
                incarnation == update.incarnation && chunk == next
            }) {
                for peer in old_holders.difference(&new_holders) {
                    routed.push(RoutedBoundary {
                        peer: ServerId(*peer),
                        gid: update.gid,
                        update: PlayerBoundaryUpdate::Leave {
                            gid: update.gid,
                            incarnation: update.incarnation,
                            seq: update.seq,
                            tick: update.tick,
                            chunk: next,
                        },
                    });
                }
                for peer in new_holders.difference(&old_holders) {
                    routed.push(RoutedBoundary {
                        peer: ServerId(*peer),
                        gid: update.gid,
                        update: PlayerBoundaryUpdate::Enter(snapshot(*update)),
                    });
                }
                continue;
            }
            if let Some((old_incarnation, old, old_seq)) = previous {
                if old_incarnation != update.incarnation {
                    for peer in old_holders {
                        routed.push(RoutedBoundary {
                            peer: ServerId(peer),
                            gid: update.gid,
                            update: PlayerBoundaryUpdate::Leave {
                                gid: update.gid,
                                incarnation: old_incarnation,
                                seq: old_seq,
                                tick: update.tick,
                                chunk: old,
                            },
                        });
                    }
                    for peer in new_holders {
                        routed.push(RoutedBoundary {
                            peer: ServerId(peer),
                            gid: update.gid,
                            update: PlayerBoundaryUpdate::Enter(snapshot(*update)),
                        });
                    }
                    continue;
                }
                for peer in old_holders.union(&new_holders) {
                    routed.push(RoutedBoundary {
                        peer: ServerId(*peer),
                        gid: update.gid,
                        update: PlayerBoundaryUpdate::Transition {
                            gid: update.gid,
                            incarnation: update.incarnation,
                            seq: update.seq,
                            tick: update.tick,
                            from: old,
                            to: next,
                            snapshot: new_holders.contains(peer).then(|| snapshot(*update)),
                        },
                    });
                }
            } else {
                for peer in new_holders {
                    routed.push(RoutedBoundary {
                        peer: ServerId(peer),
                        gid: update.gid,
                        update: PlayerBoundaryUpdate::Enter(snapshot(*update)),
                    });
                }
            }
        }
        routed
    }
}

fn chunk_of(update: &PosUpdate) -> ChunkKey {
    ChunkKey::new(
        update.world,
        (update.pos[0].floor() as i32).div_euclid(16),
        (update.pos[2].floor() as i32).div_euclid(16),
    )
}

pub fn encode(update: &PlayerBoundaryUpdate) -> Result<Vec<u8>, postcard::Error> {
    postcard::to_allocvec(update)
}

pub fn decode(bytes: &[u8]) -> Result<PlayerBoundaryUpdate, postcard::Error> {
    postcard::from_bytes(bytes)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::identity::{PlayerSeq, PlayerSlot};
    use crate::time::TickStamp;

    const WORLD: crate::protocol::WorldId = crate::protocol::WorldId::new(0, 7);

    fn update(x: f64, seq: u16) -> PosUpdate {
        PosUpdate {
            gid: GlobalPlayerId::new(ServerId(1), PlayerSlot(4)),
            incarnation: 1,
            spatial_epoch: crate::canonical_action::SpatialEpoch::Admission {
                lease: crate::canonical_action::LivePlayerLease {
                    key: crate::canonical_action::PlayerKey(uuid::Uuid::nil()),
                    gid: GlobalPlayerId::new(ServerId(1), PlayerSlot(4)),
                    incarnation: 1,
                },
                admitted_at: TickStamp(0),
            },
            seq: PlayerSeq(seq),
            tick: TickStamp(seq),
            world: WORLD,
            pos: [x, 64.0, 0.0],
            vel: [0.0; 3],
            yaw: 0.0,
            head_yaw: 0.0,
            pitch: 0.0,
            on_ground: true,
        }
    }

    fn actor() -> ActorChunkId {
        ActorChunkId::Player(GlobalPlayerId::new(ServerId(1), PlayerSlot(4)))
    }

    fn indexed(action: u64, tick: u16, chunk: ChunkKey) -> IndexedAction {
        IndexedAction {
            action,
            actor: actor(),
            tick: TickStamp(tick),
            chunk,
        }
    }

    #[test]
    fn crossing_routes_leave_and_snapshot_only_to_changed_holders() {
        let mut tracker = PlayerBoundaryTracker::new();
        let holders = |chunk: ChunkKey| match chunk.x() {
            0 => vec![2, 3],
            1 => vec![3, 4],
            _ => Vec::new(),
        };
        let snapshot = |movement| PlayerReplicaSnapshot {
            movement,
            visual: Default::default(),
        };
        let initial = tracker.route(&[update(1.0, 1)], holders, snapshot);
        assert_eq!(
            initial.iter().map(|route| route.peer.0).collect::<Vec<_>>(),
            vec![2, 3]
        );

        let crossed = tracker.route(&[update(17.0, 2)], holders, snapshot);
        assert_eq!(crossed.len(), 3);
        assert!(matches!(
            crossed[0].update,
            PlayerBoundaryUpdate::Transition { snapshot: None, .. }
        ));
        assert_eq!(crossed[0].peer, ServerId(2));
        assert!(matches!(
            crossed[1].update,
            PlayerBoundaryUpdate::Transition {
                snapshot: Some(_),
                ..
            }
        ));
        assert_eq!(crossed[1].peer, ServerId(3));
        assert!(matches!(
            crossed[2].update,
            PlayerBoundaryUpdate::Transition {
                snapshot: Some(_),
                ..
            }
        ));
        assert_eq!(crossed[2].peer, ServerId(4));
    }

    #[test]
    fn late_holder_receives_stationary_player_once_and_dropped_holder_leaves() {
        let mut tracker = PlayerBoundaryTracker::new();
        let mut active = vec![2_u16];
        let snapshot = |movement| PlayerReplicaSnapshot {
            movement,
            visual: Default::default(),
        };
        let first = tracker.route(&[update(1.0, 1)], |_| active.clone(), snapshot);
        assert_eq!(first.len(), 1);
        assert_eq!(first[0].peer, ServerId(2));

        active.push(3);
        let acquired = tracker.route(&[update(1.0, 2)], |_| active.clone(), snapshot);
        assert_eq!(acquired.len(), 1);
        assert_eq!(acquired[0].peer, ServerId(3));
        assert!(matches!(acquired[0].update, PlayerBoundaryUpdate::Enter(_)));
        assert!(
            tracker
                .route(&[update(1.0, 3)], |_| active.clone(), snapshot)
                .is_empty()
        );

        active.remove(0);
        let dropped = tracker.route(&[update(1.0, 4)], |_| active.clone(), snapshot);
        assert_eq!(dropped.len(), 1);
        assert_eq!(dropped[0].peer, ServerId(2));
        assert!(matches!(
            dropped[0].update,
            PlayerBoundaryUpdate::Leave { .. }
        ));
    }

    #[test]
    fn reused_player_slot_retires_old_incarnation_before_new_enter() {
        let mut tracker = PlayerBoundaryTracker::new();
        let snapshot = |movement| PlayerReplicaSnapshot {
            movement,
            visual: Default::default(),
        };
        tracker.route(&[update(1.0, 1)], |_| vec![2], snapshot);
        let mut replacement = update(17.0, 2);
        replacement.incarnation = 2;
        let routed = tracker.route(&[replacement], |_| vec![2], snapshot);
        assert_eq!(routed.len(), 2);
        assert!(matches!(
            routed[0].update,
            PlayerBoundaryUpdate::Leave { incarnation: 1, .. }
        ));
        assert!(matches!(
            routed[1].update,
            PlayerBoundaryUpdate::Enter(PlayerReplicaSnapshot {
                movement: PosUpdate { incarnation: 2, .. },
                ..
            })
        ));
    }

    #[test]
    fn action_indexed_before_transition_is_relocated() {
        let old = ChunkKey::new(WORLD, 0, 0);
        let new = ChunkKey::new(WORLD, 1, 0);
        let mut index = DepartureIndex::default();
        index.install(actor(), old);
        assert_eq!(index.index(indexed(7, 13, old)), old);
        assert_eq!(
            index.transition(ChunkTransition {
                actor: actor(),
                tick: TickStamp(12),
                from: old,
                to: new,
            }),
            vec![7]
        );
        assert_eq!(index.action_chunk(TickStamp(13), 7), Some(new));
    }

    #[test]
    fn transition_indexed_before_action_relocates_on_insert() {
        let old = ChunkKey::new(WORLD, 0, 0);
        let new = ChunkKey::new(WORLD, 1, 0);
        let mut index = DepartureIndex::default();
        index.install(actor(), old);
        index.transition(ChunkTransition {
            actor: actor(),
            tick: TickStamp(12),
            from: old,
            to: new,
        });
        assert_eq!(index.index(indexed(7, 13, old)), new);
    }

    #[test]
    fn future_tick_old_chunk_action_uses_departure_index() {
        let old = ChunkKey::new(WORLD, 0, 0);
        let new = ChunkKey::new(WORLD, 1, 0);
        let mut index = DepartureIndex::default();
        index.install(actor(), old);
        index.transition(ChunkTransition {
            actor: actor(),
            tick: TickStamp(u16::MAX),
            from: old,
            to: new,
        });
        assert_eq!(index.index(indexed(9, 0, old)), new);
    }

    #[test]
    fn ground_promotion_removes_departure_but_keeps_new_association() {
        let old = ChunkKey::new(WORLD, 0, 0);
        let new = ChunkKey::new(WORLD, 1, 0);
        let mut index = DepartureIndex::default();
        index.install(actor(), old);
        index.transition(ChunkTransition {
            actor: actor(),
            tick: TickStamp(12),
            from: old,
            to: new,
        });
        assert!(index.promote(actor(), TickStamp(12)));
        assert_eq!(index.departure(actor()), None);
        assert!(
            index
                .transition(ChunkTransition {
                    actor: actor(),
                    tick: TickStamp(13),
                    from: old,
                    to: ChunkKey::new(WORLD, 2, 0),
                })
                .is_empty()
        );
    }
}
