use crate::identity::GlobalPlayerId;
use crate::protocol::{BlockPos, BlockUndo, EntityRef};

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct EquipmentCorrection {
    pub player: GlobalPlayerId,
    pub slot: u8,
    pub item: u16,
}

#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct ReconcilePlan {
    pub block_rollbacks: Vec<(BlockPos, u16)>,
    pub equipment_corrections: Vec<EquipmentCorrection>,
    pub ghost_despawns: Vec<EntityRef>,
}

impl ReconcilePlan {
    #[must_use]
    pub fn new() -> Self {
        Self::default()
    }

    #[must_use]
    pub fn is_empty(&self) -> bool {
        self.block_rollbacks.is_empty()
            && self.equipment_corrections.is_empty()
            && self.ghost_despawns.is_empty()
    }

    #[must_use]
    pub fn len(&self) -> usize {
        self.block_rollbacks.len()
            + self.equipment_corrections.len()
            + self.ghost_despawns.len()
    }

    pub fn clear(&mut self) {
        self.block_rollbacks.clear();
        self.equipment_corrections.clear();
        self.ghost_despawns.clear();
    }

    pub fn merge(&mut self, other: &mut Self) {
        self.block_rollbacks.append(&mut other.block_rollbacks);
        self.equipment_corrections
            .append(&mut other.equipment_corrections);
        self.ghost_despawns.append(&mut other.ghost_despawns);
    }

    pub fn push_block_rollback(&mut self, pos: BlockPos, old_state: u16) {
        self.block_rollbacks.push((pos, old_state));
    }

    pub fn push_block_undo(&mut self, pos: BlockPos, undo: &BlockUndo) {
        self.push_block_rollback(pos, undo.old_state);
    }

    pub fn extend_loser_undos(&mut self, undos: &[(BlockPos, BlockUndo)]) {
        for (pos, undo) in undos {
            self.push_block_undo(*pos, undo);
        }
    }

    #[must_use]
    pub fn from_loser_undos(undos: &[(BlockPos, BlockUndo)]) -> Self {
        let mut plan = Self::new();
        plan.extend_loser_undos(undos);
        plan
    }

    pub fn push_equipment_correction(&mut self, player: GlobalPlayerId, slot: u8, item: u16) {
        self.equipment_corrections.push(EquipmentCorrection {
            player,
            slot,
            item,
        });
    }

    pub fn push_ghost_despawn(&mut self, ghost: EntityRef) {
        self.ghost_despawns.push(ghost);
    }

    pub fn apply_blocks(&self, mut on_block: impl FnMut(BlockPos, u16)) {
        for (pos, old_state) in &self.block_rollbacks {
            on_block(*pos, *old_state);
        }
    }

    pub fn apply_equipment(&self, mut on_equipment: impl FnMut(EquipmentCorrection)) {
        for correction in &self.equipment_corrections {
            on_equipment(*correction);
        }
    }

    pub fn apply_despawns(&self, mut on_despawn: impl FnMut(EntityRef)) {
        for ghost in &self.ghost_despawns {
            on_despawn(*ghost);
        }
    }

    pub fn apply_all(
        &self,
        on_block: impl FnMut(BlockPos, u16),
        on_equipment: impl FnMut(EquipmentCorrection),
        on_despawn: impl FnMut(EntityRef),
    ) {
        self.apply_blocks(on_block);
        self.apply_equipment(on_equipment);
        self.apply_despawns(on_despawn);
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::identity::{PlayerSlot, ServerId};
    use crate::protocol::ChunkAddr;

    fn pos(x: i32, y: i32, z: i32) -> BlockPos {
        BlockPos { x, y, z }
    }

    fn undo(old_state: u16) -> BlockUndo {
        BlockUndo {
            old_state,
            count_before: 1,
        }
    }

    #[test]
    fn empty_plan_reports_empty() {
        let plan = ReconcilePlan::new();
        assert!(plan.is_empty());
        assert_eq!(plan.len(), 0);
    }

    #[test]
    fn loser_undos_become_rollbacks() {
        let undos = [(pos(1, 2, 3), undo(7)), (pos(4, 5, 6), undo(9))];
        let plan = ReconcilePlan::from_loser_undos(&undos);
        assert_eq!(plan.len(), 2);
        assert_eq!(plan.block_rollbacks[0], (pos(1, 2, 3), 7));
        assert_eq!(plan.block_rollbacks[1], (pos(4, 5, 6), 9));
    }

    #[test]
    fn apply_helpers_visit_everything_in_order() {
        let mut plan = ReconcilePlan::new();
        plan.push_block_rollback(pos(0, 0, 0), 3);
        let gid = GlobalPlayerId::new(ServerId(1), PlayerSlot(2));
        plan.push_equipment_correction(gid, 4, 11);
        let ghost = EntityRef {
            origin: ServerId(1),
            owner: ServerId(1),
            local_id: 8,
            chunk: ChunkAddr { x: 0, z: 0 },
        };
        plan.push_ghost_despawn(ghost);

        let mut blocks = Vec::new();
        let mut fixes = Vec::new();
        let mut despawns = Vec::new();
        plan.apply_all(
            |block_pos, old| blocks.push((block_pos, old)),
            |fix| fixes.push(fix),
            |entity| despawns.push(entity),
        );
        assert_eq!(blocks, [(pos(0, 0, 0), 3)]);
        assert_eq!(fixes.len(), 1);
        assert_eq!(despawns, [ghost]);
    }

    #[test]
    fn merge_drains_other() {
        let mut left = ReconcilePlan::from_loser_undos(&[(pos(0, 0, 0), undo(1))]);
        let mut right = ReconcilePlan::from_loser_undos(&[(pos(1, 1, 1), undo(2))]);
        left.merge(&mut right);
        assert_eq!(left.len(), 2);
        assert!(right.is_empty());
    }
}
