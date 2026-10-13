use serde::{Deserialize, Serialize};

use crate::canonical_action::ActionId;
use crate::protocol::{BlockPos, ChunkKey, WorldId};
use crate::time::TickStamp;

#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash, Serialize, Deserialize)]
pub enum MachineKind {
    Furnace,
    Hopper,
    BrewingStand,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash, Serialize, Deserialize)]
pub struct MachineActionId {
    pub genesis: [u8; 32],
    pub world: WorldId,
    pub position: BlockPos,
    pub source_slot: u64,
    pub kind: MachineKind,
}

impl MachineActionId {
    pub fn valid(self) -> bool {
        self.genesis != [0; 32]
            && self.position.x.unsigned_abs() <= 30_000_000
            && self.position.z.unsigned_abs() <= 30_000_000
    }

    pub fn tick(self) -> TickStamp {
        TickStamp(self.source_slot as u16)
    }

    pub fn chunk(self) -> ChunkKey {
        ChunkKey::new(
            self.world,
            self.position.x.div_euclid(16),
            self.position.z.div_euclid(16),
        )
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash, Serialize, Deserialize)]
pub enum ActionIdentity {
    Issued(ActionId),
    Derived(MachineActionId),
}

impl ActionIdentity {
    pub fn requires_creation_transmission(self) -> bool {
        matches!(self, Self::Issued(_))
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn machine_identity_preserves_full_slot_position_genesis_and_kind() {
        let base = MachineActionId {
            genesis: [1; 32],
            world: WorldId::new(0, 1),
            position: BlockPos { x: -1, y: 80, z: 16 },
            source_slot: 7,
            kind: MachineKind::Furnace,
        };
        assert!(base.valid());
        assert_eq!(base.chunk(), ChunkKey::new(base.world, -1, 1));
        let wrapped = MachineActionId { source_slot: 65_543, ..base };
        assert_eq!(base.tick(), wrapped.tick());
        assert_ne!(base, wrapped);
        assert_ne!(base, MachineActionId { kind: MachineKind::Hopper, ..base });
        assert_ne!(base, MachineActionId { genesis: [2; 32], ..base });
        assert_ne!(base, MachineActionId {
            position: BlockPos { x: 65_535, ..base.position },
            ..base
        });
        let identity = ActionIdentity::Derived(base);
        assert!(!identity.requires_creation_transmission());
        let bytes = postcard::to_allocvec(&identity).unwrap();
        assert_eq!(postcard::from_bytes::<ActionIdentity>(&bytes).unwrap(), identity);
    }
}
