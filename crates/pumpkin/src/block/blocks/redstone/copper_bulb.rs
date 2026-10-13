use crate::block::blocks::redstone::block_receives_redstone_power;
use crate::block::blocks::weathering_copper::{
    ChangeOverTimeBlock, WeatherState, WeatheringCopper, change_over_time, get_chance_modifier,
    get_first, get_next, get_previous, get_weather_state,
};
use crate::block::{
    BlockBehaviour, BlockMetadata, GetComparatorOutputArgs, OnNeighborUpdateArgs, OnPlaceArgs,
    RandomTickArgs,
};
use pumpkin_data::BlockId;
use pumpkin_data::BlockStateId;
use pumpkin_data::sound::{Sound, SoundCategory};
use pumpkin_world::world::BlockFlags;

type CopperBulbLikeProperties = pumpkin_data::block_properties::CopperBulbLikeProperties;

pub struct CopperBulbBlock;

impl ChangeOverTimeBlock<WeatherState> for CopperBulbBlock {
    fn get_age(&self, block: &pumpkin_data::Block) -> Option<WeatherState> {
        get_weather_state(block)
    }

    fn get_chance_modifier(&self, age: WeatherState) -> f32 {
        get_chance_modifier(age)
    }

    fn get_next(&self, block: &pumpkin_data::Block) -> Option<&'static pumpkin_data::Block> {
        get_next(block)
    }

    fn get_previous(&self, block: &pumpkin_data::Block) -> Option<&'static pumpkin_data::Block> {
        get_previous(block)
    }

    fn get_first(&self, block: &pumpkin_data::Block) -> Option<&'static pumpkin_data::Block> {
        get_first(block)
    }
}

impl WeatheringCopper for CopperBulbBlock {}

impl BlockMetadata for CopperBulbBlock {
    fn ids() -> Box<[BlockId]> {
        [
            BlockId::COPPER_BULB,
            BlockId::EXPOSED_COPPER_BULB,
            BlockId::WEATHERED_COPPER_BULB,
            BlockId::OXIDIZED_COPPER_BULB,
            BlockId::WAXED_COPPER_BULB,
            BlockId::WAXED_EXPOSED_COPPER_BULB,
            BlockId::WAXED_WEATHERED_COPPER_BULB,
            BlockId::WAXED_OXIDIZED_COPPER_BULB,
        ]
        .into()
    }
}

#[pumpkin_macros::canonical_callback_defaults]
impl BlockBehaviour for CopperBulbBlock {
    fn placement_intent(
        &self,
        args: crate::block::PlacementIntentArgs<'_>,
    ) -> Option<BlockStateId> {
        let mut properties = CopperBulbLikeProperties::default(args.block);
        properties.powered = args.world.receives_redstone_power(args.position);
        properties.lit = properties.powered;
        Some(properties.to_state_id(args.block))
    }

    fn on_place(&self, args: OnPlaceArgs<'_>) -> BlockStateId {
        let mut props = CopperBulbLikeProperties::default(args.block);
        let is_receiving_power = block_receives_redstone_power(args.world, args.position);
        if is_receiving_power {
            props.lit = true;
            args.world.play_block_sound(
                Sound::BlockCopperBulbTurnOn,
                SoundCategory::Blocks,
                *args.position,
            );
            props.powered = true;
        }
        props.to_state_id(args.block)
    }

    fn on_neighbor_update(&self, args: OnNeighborUpdateArgs<'_>) {
        crate::block::neighbor::apply_legacy_neighbor_intent(self, args);
    }

    fn callback_intent(
        &self,
        args: crate::block::neighbor::BlockCallbackIntentArgs<'_>,
    ) -> Option<Vec<crate::block::neighbor::BlockCallbackEffect>> {
        use crate::block::neighbor::{BlockCallbackEffect, BlockCallbackEvent};
        if !matches!(args.event, BlockCallbackEvent::Neighbor { .. }) {
            return Some(Vec::new());
        }
        let state = args.world.get_block_state_id(args.position);
        if state.to_block_id() != args.block.id {
            return Some(Vec::new());
        }
        let mut properties = CopperBulbLikeProperties::from_state_id(state);
        let powered = args.world.receives_redstone_power(args.position);
        if properties.powered == powered {
            return Some(Vec::new());
        }
        let mut effects = Vec::new();
        if !properties.powered {
            properties.lit = !properties.lit;
            effects.push(BlockCallbackEffect::Sound {
                position: *args.position,
                sound: if properties.lit {
                    Sound::BlockCopperBulbTurnOn
                } else {
                    Sound::BlockCopperBulbTurnOff
                },
                volume_milli: 1000,
                pitch_milli: 1000,
            });
        }
        properties.powered = powered;
        effects.push(BlockCallbackEffect::SetBlock {
            position: *args.position,
            state: properties.to_state_id(args.block),
            flags: BlockFlags::NOTIFY_ALL,
        });
        Some(effects)
    }

    fn random_tick(&self, args: RandomTickArgs<'_>) {
        change_over_time(args.world, args.position, args.block);
    }

    fn random_tick_intent(
        &self,
        args: crate::block::RandomTickIntentArgs<'_>,
    ) -> Option<Vec<crate::block::neighbor::BlockCallbackEffect>> {
        crate::block::blocks::weathering_copper::change_over_time_intent(args)
    }

    fn get_comparator_output(&self, args: GetComparatorOutputArgs<'_>) -> Option<u8> {
        let props = CopperBulbLikeProperties::from_state_id(args.state.id);
        if props.lit { Some(15) } else { Some(0) }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::block::neighbor::{
        BlockCallbackEffect, BlockCallbackEvent, BlockCallbackIntentArgs, CapturedPower,
        NeighborSnapshot, NeighborTranscript,
    };
    use pumpkin_data::Block;
    use pumpkin_util::math::position::BlockPos;

    #[test]
    fn rising_edges_toggle_but_held_power_and_falling_edges_do_not() {
        let position = BlockPos::new(0, 80, 0);
        let source = position.up();
        let mut state = Block::COPPER_BULB.default_state.id;
        for (powered, expected_lit, expected_effects) in [
            (true, true, 2),
            (true, true, 0),
            (false, true, 1),
            (true, false, 2),
            (false, false, 1),
        ] {
            let source_state = if powered {
                Block::REDSTONE_BLOCK.default_state.id
            } else {
                BlockStateId::AIR
            };
            let mut snapshot = NeighborSnapshot::default();
            snapshot.blocks.insert(position, state);
            snapshot.blocks.insert(source, source_state);
            snapshot.receives_power.insert(
                position,
                CapturedPower {
                    value: powered,
                    reads: vec![(source, source_state)],
                    entity_reads: Vec::new(),
                },
            );
            let view = NeighborTranscript::new(snapshot, 32).unwrap();
            let effects = CopperBulbBlock
                .callback_intent(BlockCallbackIntentArgs {
                    world: &view,
                    block: &Block::COPPER_BULB,
                    position: &position,
                    event: BlockCallbackEvent::Neighbor {
                        source: Block::REDSTONE_BLOCK.id,
                        notify: true,
                    },
                })
                .unwrap();
            assert_eq!(effects.len(), expected_effects);
            for effect in effects {
                if let BlockCallbackEffect::SetBlock { state: after, .. } = effect {
                    state = after;
                }
            }
            assert!(view.finish().is_some());
            let properties = CopperBulbLikeProperties::from_state_id(state);
            assert_eq!(properties.lit, expected_lit);
            assert_eq!(properties.powered, powered);
        }
    }
}
