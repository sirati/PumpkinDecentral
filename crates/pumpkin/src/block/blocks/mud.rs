use pumpkin_macros::pumpkin_block;

use crate::block::{BlockBehaviour, PathComputationType};

#[pumpkin_block("minecraft:mud")]
pub struct MudBlock;

#[pumpkin_macros::canonical_callback_defaults]
impl BlockBehaviour for MudBlock {
    fn is_pathfindable(
        &self,
        _state: &pumpkin_data::BlockState,
        _computation_type: PathComputationType,
    ) -> bool {
        false
    }
}
