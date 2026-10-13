use pumpkin_macros::pumpkin_block;

use crate::block::BlockBehaviour;

#[pumpkin_block("minecraft:tinted_glass")]
pub struct TintedGlassBlock;

#[pumpkin_macros::canonical_callback_defaults]
impl BlockBehaviour for TintedGlassBlock {}
