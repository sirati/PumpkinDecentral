use pumpkin_data::BlockId;
use pumpkin_data::BlockStateId;

use crate::block::{
    BlockBehaviour, BlockMetadata, CanPlaceAtArgs, GetStateForNeighborUpdateArgs,
    blocks::plant::PlantBlockBase,
};

pub struct BushBlock;

impl BlockMetadata for BushBlock {
    fn ids() -> Box<[BlockId]> {
        [BlockId::BUSH, BlockId::FIREFLY_BUSH].into()
    }
}

#[pumpkin_macros::canonical_callback_defaults]
impl BlockBehaviour for BushBlock {
    fn callback_intent(
        &self,
        _args: crate::block::neighbor::BlockCallbackIntentArgs<'_>,
    ) -> Option<Vec<crate::block::neighbor::BlockCallbackEffect>> {
        Some(Vec::new())
    }

    fn can_place_at(&self, args: CanPlaceAtArgs<'_>) -> bool {
        <Self as PlantBlockBase>::can_place_at(self, args.block_accessor, args.position)
    }

    fn get_state_for_neighbor_update(
        &self,
        args: GetStateForNeighborUpdateArgs<'_>,
    ) -> BlockStateId {
        <Self as PlantBlockBase>::get_state_for_neighbor_update(
            self,
            args.world,
            args.position,
            args.state_id,
        )
    }
}

impl PlantBlockBase for BushBlock {}
