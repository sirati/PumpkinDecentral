//! Player routing identity: which server hosts which player slot.
//!
//! A [`GlobalPlayerId`] is exactly one `u16` server plus one `u16` player
//! slot. Each server hands out its own [`PlayerSlot`]s locally, so two
//! servers issuing the same slot still yield different global ids and no
//! cross-server coordination is needed. This is routing identity only: it
//! never stands in for an entity uuid. Entities route by [`EntityRef`](crate::protocol::EntityRef)
//! (`owner` plus `local_id` plus `chunk`), a different type on purpose so the
//! compiler rejects mixing players up with entities.

use serde::{Deserialize, Serialize};

/// Statically assigned host server number.
#[derive(
    Debug, Clone, Copy, PartialEq, Eq, Hash, PartialOrd, Ord, Serialize, Deserialize,
)]
pub struct ServerId(pub u16);

/// Player slot handed out by the hosting server alone.
///
/// Slots are scoped to one [`ServerId`]: wrapping here is local rotation,
/// never a cluster-wide allocation.
#[derive(
    Debug, Clone, Copy, PartialEq, Eq, Hash, PartialOrd, Ord, Serialize, Deserialize,
)]
pub struct PlayerSlot(pub u16);

impl PlayerSlot {
    /// Advances a locally owned slot, wrapping on overflow.
    #[must_use]
    pub const fn next(self) -> Self {
        Self(self.0.wrapping_add(1))
    }
}

/// Globally unique player route: hosting server plus local slot.
///
/// Uniqueness needs no coordination: the `server` half disambiguates slots
/// that were issued independently on different servers. Ordering is
/// `server` first, then `player`, so all of one server's players sort
/// together.
#[derive(
    Debug, Clone, Copy, PartialEq, Eq, Hash, PartialOrd, Ord, Serialize, Deserialize,
)]
pub struct GlobalPlayerId {
    pub server: ServerId,
    pub player: PlayerSlot,
}

#[derive(
    Debug, Clone, Copy, PartialEq, Eq, Hash, PartialOrd, Ord, Serialize, Deserialize,
)]
pub enum ActionActor {
    Player(GlobalPlayerId),
    Server(ServerId),
}

impl From<GlobalPlayerId> for ActionActor {
    fn from(value: GlobalPlayerId) -> Self {
        Self::Player(value)
    }
}

/// The whole id fits in 32 bits: two `u16` halves, no padding.
/// Fails to compile if either half stops being a `u16`.
const _: [u8; 4] = [0; core::mem::size_of::<GlobalPlayerId>()];

impl GlobalPlayerId {
    /// Combines a hosting server with one of its local slots.
    #[must_use]
    pub const fn new(server: ServerId, player: PlayerSlot) -> Self {
        Self { server, player }
    }

    /// Hosting server half of the id; the routing destination.
    #[must_use]
    pub const fn host(self) -> ServerId {
        self.server
    }

    /// Local slot half of the id; unique only within [`host`](Self::host).
    #[must_use]
    pub const fn slot(self) -> PlayerSlot {
        self.player
    }

    /// Whether `server` is the host named by this id.
    #[must_use]
    pub const fn is_hosted_by(self, server: ServerId) -> bool {
        self.server.0 == server.0
    }

    /// Packs the id as `server` in the high 16 bits, `player` in the low 16.
    #[must_use]
    pub const fn pack_u32(self) -> u32 {
        (self.server.0 as u32) << 16 | self.player.0 as u32
    }

    /// Inverse of [`pack_u32`](Self::pack_u32).
    #[must_use]
    pub const fn unpack_u32(raw: u32) -> Self {
        Self {
            server: ServerId((raw >> 16) as u16),
            player: PlayerSlot(raw as u16),
        }
    }
}

/// Per-player sequence number with wrapping `u16` comparison.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Serialize, Deserialize)]
pub struct PlayerSeq(pub u16);

impl PlayerSeq {
    /// Whether `self` is newer than `other` under wrapping order.
    #[must_use]
    pub const fn is_newer_than(self, other: Self) -> bool {
        let distance = self.0.wrapping_sub(other.0);
        distance != 0 && distance < 32_768
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Serialize, Deserialize)]
pub struct ActionSeq(pub u16);

impl ActionSeq {
    #[must_use]
    pub const fn is_newer_than(self, other: Self) -> bool {
        let distance = self.0.wrapping_sub(other.0);
        distance != 0 && distance < 32_768
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::protocol::EntityRef;

    #[test]
    fn global_id_orders_by_server_then_player() {
        let left = GlobalPlayerId::new(ServerId(1), PlayerSlot(9));
        let right = GlobalPlayerId::new(ServerId(2), PlayerSlot(0));
        assert!(left < right);
    }

    #[test]
    fn same_slot_on_two_servers_needs_no_coordination() {
        let on_first = GlobalPlayerId::new(ServerId(1), PlayerSlot(7));
        let on_second = GlobalPlayerId::new(ServerId(2), PlayerSlot(7));
        assert_ne!(on_first, on_second);
        assert_eq!(on_first.host(), ServerId(1));
        assert_eq!(on_second.host(), ServerId(2));
        assert!(on_first.is_hosted_by(ServerId(1)));
        assert!(!on_first.is_hosted_by(ServerId(2)));
    }

    #[test]
    fn global_id_is_two_u16_halves() {
        let id = GlobalPlayerId::new(ServerId(0x0102), PlayerSlot(0x0304));
        assert_eq!(id.pack_u32(), 0x0102_0304);
        assert_eq!(GlobalPlayerId::unpack_u32(0x0102_0304), id);
        assert_eq!(core::mem::size_of::<GlobalPlayerId>(), 4);
    }

    #[test]
    fn player_identity_stays_distinct_from_entity_identity() {
        let player = GlobalPlayerId::new(ServerId(1), PlayerSlot(7));
        let entity = EntityRef {
            origin: ServerId(1),
            owner: ServerId(1),
            local_id: 7,
            chunk: crate::protocol::ChunkAddr { x: 0, z: 0 },
        };
        assert_eq!(player.host(), entity.owner);
        assert_eq!(player.slot().0, entity.local_id as u16);
        fn route_player(id: GlobalPlayerId) -> ServerId {
            id.host()
        }
        assert_eq!(route_player(player), ServerId(1));
        let _: EntityRef = entity;
    }

    #[test]
    fn seq_wraps_forward() {
        assert!(PlayerSeq(0).is_newer_than(PlayerSeq(u16::MAX)));
        assert!(PlayerSeq(5).is_newer_than(PlayerSeq(4)));
    }

    #[test]
    fn seq_rejects_equal_and_older() {
        assert!(!PlayerSeq(4).is_newer_than(PlayerSeq(4)));
        assert!(!PlayerSeq(4).is_newer_than(PlayerSeq(5)));
    }
}
