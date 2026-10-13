use pumpkin_macros::pumpkin_block;

use crate::block::BlockBehaviour;

#[pumpkin_block("minecraft:structure_void")]
pub struct StructureVoidBlock;

#[pumpkin_macros::canonical_callback_defaults]
impl BlockBehaviour for StructureVoidBlock {}
