use std::cmp::Ordering;
use std::hash::Hasher;

use crate::actor::ActorId;
use crate::identity::GlobalPlayerId;
use crate::time::TickStamp;

#[must_use]
pub fn rank_tick_player(cluster_seed: u64, tick: TickStamp, player: GlobalPlayerId) -> u64 {
    let mut hasher = xxhash_rust::xxh64::Xxh64::new(cluster_seed);
    hasher.write_u16(tick.0);
    hasher.write_u16(player.server.0);
    hasher.write_u16(player.player.0);
    hasher.finish()
}

#[must_use]
pub fn rank_tick_actor(cluster_seed: u64, tick: TickStamp, actor: ActorId) -> u64 {
    let mut hasher = xxhash_rust::xxh64::Xxh64::new(cluster_seed);
    hasher.write_u16(tick.0);
    match actor {
        ActorId::Player(player) => {
            hasher.write_u8(0);
            hasher.write_u16(player.server.0);
            hasher.write_u16(player.player.0);
        }
        ActorId::Entity(entity) => {
            hasher.write_u8(1);
            hasher.write_u16(entity.origin.0);
            hasher.write_i32(entity.local_id);
            hasher.write_u64(entity.generation);
        }
        ActorId::Peer(peer) => {
            hasher.write_u8(2);
            hasher.write_u16(peer.0);
        }
    }
    hasher.finish()
}

#[must_use]
pub fn order_actors(cluster_seed: u64, tick: TickStamp, left: ActorId, right: ActorId) -> Ordering {
    rank_tick_actor(cluster_seed, tick, left)
        .cmp(&rank_tick_actor(cluster_seed, tick, right))
        .then_with(|| left.cmp(&right))
}

#[must_use]
pub fn order_players(
    cluster_seed: u64,
    tick: TickStamp,
    left: GlobalPlayerId,
    right: GlobalPlayerId,
) -> Ordering {
    let by_hash = rank_tick_player(cluster_seed, tick, left).cmp(&rank_tick_player(
        cluster_seed,
        tick,
        right,
    ));
    if by_hash == Ordering::Equal {
        (left.server, left.player).cmp(&(right.server, right.player))
    } else {
        by_hash
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::identity::{PlayerSlot, ServerId};
    use crate::protocol::GlobalEntityId;

    fn gid(server: u16, player: u16) -> GlobalPlayerId {
        GlobalPlayerId::new(ServerId(server), PlayerSlot(player))
    }

    #[test]
    fn ranking_is_deterministic() {
        let tick = TickStamp(77);
        assert_eq!(
            rank_tick_player(9, tick, gid(1, 2)),
            rank_tick_player(9, tick, gid(1, 2))
        );
    }

    #[test]
    fn ordering_is_antisymmetric() {
        let tick = TickStamp(77);
        let left = gid(1, 2);
        let right = gid(2, 1);
        assert_eq!(
            order_players(9, tick, left, right).reverse(),
            order_players(9, tick, right, left)
        );
    }

    #[test]
    fn ordering_matches_sorted_ranks() {
        let tick = TickStamp(5);
        let players = [gid(1, 1), gid(1, 2), gid(2, 1), gid(3, 7)];
        let mut sorted = players;
        sorted.sort_by(|left, right| order_players(1234, tick, *left, *right));
        for (index, left) in sorted.iter().enumerate() {
            for right in sorted.iter().skip(index + 1) {
                assert_eq!(order_players(1234, tick, *left, *right), Ordering::Less);
            }
        }
    }

    #[test]
    fn same_player_is_equal() {
        let tick = TickStamp(5);
        assert_eq!(
            order_players(1, tick, gid(1, 1), gid(1, 1)),
            Ordering::Equal
        );
    }

    #[test]
    fn entity_and_player_ids_have_a_stable_total_order() {
        let tick = TickStamp(18);
        let actors = [
            ActorId::Player(gid(1, 4)),
            ActorId::Entity(GlobalEntityId {
                origin: ServerId(1),
                local_id: 4,
                generation: 0,
            }),
            ActorId::Entity(GlobalEntityId {
                origin: ServerId(1),
                local_id: 4,
                generation: 1,
            }),
        ];
        let mut forward = actors;
        forward.sort_by(|left, right| order_actors(77, tick, *left, *right));
        let mut reversed = actors;
        reversed.reverse();
        reversed.sort_by(|left, right| order_actors(77, tick, *left, *right));
        assert_eq!(forward, reversed);
        assert_eq!(
            forward
                .iter()
                .copied()
                .collect::<std::collections::BTreeSet<_>>()
                .len(),
            3
        );
    }
}
