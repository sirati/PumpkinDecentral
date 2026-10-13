use pumpkin_cluster::canonical_inventory::BucketContents;
use pumpkin_data::{Block, BlockStateId, fluid::Fluid, item::Item};

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(super) struct BucketBlockTransition {
    pub after: BlockStateId,
    pub contents: BucketContents,
    pub fluid: Option<u16>,
    pub delay: Option<u8>,
    pub replace_with_drops: bool,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(super) enum BucketEmptyResult {
    Block(BucketBlockTransition),
    Evaporated,
}

pub(super) fn contents(item: u16) -> Option<BucketContents> {
    match item {
        id if id == Item::WATER_BUCKET.id => Some(BucketContents::Water),
        id if id == Item::LAVA_BUCKET.id => Some(BucketContents::Lava),
        id if id == Item::POWDER_SNOW_BUCKET.id => Some(BucketContents::PowderSnow),
        _ => None,
    }
}

pub(super) fn fill(state: BlockStateId) -> Option<BucketBlockTransition> {
    let block = state.to_block();
    if block == &Block::POWDER_SNOW {
        return Some(BucketBlockTransition {
            after: Block::AIR.default_state.id,
            contents: BucketContents::PowderSnow,
            fluid: None,
            delay: None,
            replace_with_drops: false,
        });
    }
    if block.is_waterlogged(state) {
        return Some(BucketBlockTransition {
            after: block.set_waterlogged(state, false)?,
            contents: BucketContents::Water,
            fluid: Some(Fluid::WATER.id),
            delay: Some(5),
            replace_with_drops: false,
        });
    }
    let contents = if state == Block::WATER.default_state.id {
        BucketContents::Water
    } else if state == Block::LAVA.default_state.id {
        BucketContents::Lava
    } else {
        return None;
    };
    Some(BucketBlockTransition {
        after: Block::AIR.default_state.id,
        contents,
        fluid: None,
        delay: None,
        replace_with_drops: false,
    })
}

pub(super) fn empty(
    state: BlockStateId,
    contents: BucketContents,
    water_evaporates: bool,
    lava_delay: u8,
) -> Option<BucketEmptyResult> {
    if contents == BucketContents::Water && water_evaporates {
        return Some(BucketEmptyResult::Evaporated);
    }
    let block = state.to_block();
    if contents == BucketContents::Water && block.is_waterloggable() && !block.is_waterlogged(state)
    {
        return Some(BucketEmptyResult::Block(BucketBlockTransition {
            after: block.set_waterlogged(state, true)?,
            contents,
            fluid: Some(Fluid::WATER.id),
            delay: Some(5),
            replace_with_drops: false,
        }));
    }
    if !state.to_state().is_air()
        && !state.to_state().is_liquid()
        && !state.to_state().replaceable()
    {
        return None;
    }
    let (after, fluid, delay) = match contents {
        BucketContents::Water => (
            Block::WATER.default_state.id,
            Some(Fluid::WATER.id),
            Some(5),
        ),
        BucketContents::Lava if matches!(lava_delay, 10 | 30) => (
            Block::LAVA.default_state.id,
            Some(Fluid::LAVA.id),
            Some(lava_delay),
        ),
        BucketContents::Lava => return None,
        BucketContents::PowderSnow => (Block::POWDER_SNOW.default_state.id, None, None),
    };
    Some(BucketEmptyResult::Block(BucketBlockTransition {
        after,
        contents,
        fluid,
        delay,
        replace_with_drops: !state.to_state().is_air()
            && !state.to_state().is_liquid()
            && state != after,
    }))
}

#[cfg(test)]
mod tests {
    use super::*;
    use pumpkin_data::block_properties::OakStairsLikeProperties;

    #[test]
    fn filling_requires_a_real_source_and_preserves_waterlogged_properties() {
        assert_eq!(
            fill(Block::WATER.default_state.id).unwrap().contents,
            BucketContents::Water
        );
        assert_eq!(
            fill(Block::LAVA.default_state.id).unwrap().contents,
            BucketContents::Lava
        );
        let flowing = Block::WATER
            .states
            .iter()
            .find(|state| state.id != Block::WATER.default_state.id)
            .unwrap()
            .id;
        assert!(fill(flowing).is_none());
        let mut stairs = OakStairsLikeProperties::default(&Block::OAK_STAIRS);
        assert!(fill(stairs.to_state_id(&Block::OAK_STAIRS)).is_none());
        stairs.waterlogged = true;
        let state = stairs.to_state_id(&Block::OAK_STAIRS);
        let after = fill(state).unwrap();
        assert_eq!(after.after.to_block(), &Block::OAK_STAIRS);
        assert_eq!(
            after.after,
            state.to_block().set_waterlogged(state, false).unwrap()
        );
        assert_eq!(after.delay, Some(5));
        assert!(fill(Block::STONE.default_state.id).is_none());
    }

    #[test]
    fn emptying_is_registry_classified_and_uses_original_dimension_policy() {
        let air = Block::AIR.default_state.id;
        assert_eq!(
            empty(air, BucketContents::Water, true, 30),
            Some(BucketEmptyResult::Evaporated)
        );
        assert!(matches!(
            empty(air, BucketContents::Lava, true, 10),
            Some(BucketEmptyResult::Block(BucketBlockTransition {
                delay: Some(10),
                ..
            }))
        ));
        assert!(empty(air, BucketContents::Lava, false, 5).is_none());
        assert!(
            empty(
                Block::STONE.default_state.id,
                BucketContents::PowderSnow,
                false,
                30
            )
            .is_none()
        );
        assert_eq!(
            contents(Item::POWDER_SNOW_BUCKET.id),
            Some(BucketContents::PowderSnow)
        );
        assert!(contents(Item::COD_BUCKET.id).is_none());
    }
}
