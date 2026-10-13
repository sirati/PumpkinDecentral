use std::sync::Arc;

use crate::block::BlockIsReplacing;
use crate::block::CanPlaceAtArgs;
use crate::block::EmitsRedstonePowerArgs;
use crate::block::GetRedstonePowerArgs;
use crate::block::GetStateForNeighborUpdateArgs;
use crate::block::OnNeighborUpdateArgs;
use crate::block::OnPlaceArgs;
use crate::block::OnScheduledTickArgs;
use crate::block::OnStateReplacedArgs;
use crate::block::PlacedArgs;
use crate::entity::EntityBase;
use pumpkin_data::Block;
use pumpkin_data::BlockDirection;
use pumpkin_data::BlockId;
use pumpkin_data::BlockStateId;
use pumpkin_data::FacingExt;
use pumpkin_data::HorizontalFacingExt;
use pumpkin_data::block_properties::Facing;
use pumpkin_util::math::position::BlockPos;
use pumpkin_world::tick::TickPriority;
use pumpkin_world::world::BlockAccessor;
use pumpkin_world::world::BlockFlags;

type RWallTorchProps = pumpkin_data::block_properties::FurnaceLikeProperties;
type RTorchProps = pumpkin_data::block_properties::RedstoneOreLikeProperties;

use crate::block::{BlockBehaviour, BlockMetadata};
use crate::world::World;

use super::get_redstone_power;

pub struct RedstoneTorchBlock;

impl BlockMetadata for RedstoneTorchBlock {
    fn ids() -> Box<[BlockId]> {
        [BlockId::REDSTONE_TORCH, BlockId::REDSTONE_WALL_TORCH].into()
    }
}

#[pumpkin_macros::canonical_callback_defaults]
impl BlockBehaviour for RedstoneTorchBlock {
    fn on_place(&self, args: OnPlaceArgs<'_>) -> BlockStateId {
        let world = args.world;
        let block = args.block;
        let location = args.position;

        if args.direction == BlockDirection::Down {
            let support_block = world.get_block_state(&location.down());
            if support_block.is_center_solid(BlockDirection::Up) {
                return block.default_state.id;
            }
        }
        let mut directions = args.player.get_entity().get_entity_facing_order();

        if args.replacing == BlockIsReplacing::None {
            let face = args.direction.to_facing();
            let mut i = 0;
            while i < directions.len() && directions[i] != face {
                i += 1;
            }

            if i > 0 {
                directions.copy_within(0..i, 1);
                directions[0] = face;
            }
        } else if directions[0] == Facing::Down {
            let support_block = world.get_block_state(&location.down());
            if support_block.is_center_solid(BlockDirection::Up) {
                return block.default_state.id;
            }
        }

        for dir in directions {
            if dir != Facing::Up
                && dir != Facing::Down
                && can_place_at(world, location, dir.to_block_direction())
            {
                let mut torch_props = RWallTorchProps::default(&Block::REDSTONE_WALL_TORCH);
                if let Some(facing) = dir.opposite().to_horizontal_facing() {
                    torch_props.facing = facing;
                    return torch_props.to_state_id(&Block::REDSTONE_WALL_TORCH);
                }
            }
        }

        let support_block = world.get_block_state(&location.down());
        if support_block.is_center_solid(BlockDirection::Up) {
            block.default_state.id
        } else {
            BlockStateId::AIR
        }
    }

    fn can_place_at(&self, args: CanPlaceAtArgs<'_>) -> bool {
        let support_block = args.block_accessor.get_block_state(&args.position.down());
        if support_block.is_center_solid(BlockDirection::Up) {
            return true;
        }
        for dir in BlockDirection::horizontal() {
            if can_place_at(args.block_accessor, args.position, dir.to_block_direction()) {
                return true;
            }
        }
        false
    }

    fn get_state_for_neighbor_update(
        &self,
        args: GetStateForNeighborUpdateArgs<'_>,
    ) -> BlockStateId {
        if args.block == &Block::REDSTONE_WALL_TORCH {
            let props = RWallTorchProps::from_state_id(args.state_id);
            if props.facing.to_block_direction().opposite() == args.direction
                && !can_place_at(
                    args.world,
                    args.position,
                    props.facing.to_block_direction().opposite(),
                )
            {
                return BlockStateId::AIR;
            }
        } else if args.direction == BlockDirection::Down {
            let support_block = args.world.get_block_state(&args.position.down());
            if !support_block.is_center_solid(BlockDirection::Up) {
                return BlockStateId::AIR;
            }
        }
        args.state_id
    }

    fn on_neighbor_update(&self, args: OnNeighborUpdateArgs<'_>) {
        crate::block::neighbor::apply_legacy_neighbor_intent(self, args);
    }

    fn callback_intent(
        &self,
        args: crate::block::neighbor::BlockCallbackIntentArgs<'_>,
    ) -> Option<Vec<crate::block::neighbor::BlockCallbackEffect>> {
        use crate::block::neighbor::{BlockCallbackEffect, BlockCallbackEvent};
        let notify_around = || {
            BlockDirection::all()
                .into_iter()
                .map(|direction| BlockCallbackEffect::NotifyNeighbors {
                    position: args.position.offset(direction.to_offset()),
                    except: None,
                })
                .collect()
        };
        match args.event {
            BlockCallbackEvent::Placed { .. } | BlockCallbackEvent::StateReplaced { .. } => {
                return Some(notify_around());
            }
            BlockCallbackEvent::Prepare { .. } => return Some(Vec::new()),
            BlockCallbackEvent::Neighbor { .. } | BlockCallbackEvent::Scheduled => {}
        }
        let state = args.world.get_block_state_id(args.position);
        if state.to_block_id() != args.block.id {
            return Some(Vec::new());
        }
        let (lit, direction) = if args.block == &Block::REDSTONE_WALL_TORCH {
            let properties = RWallTorchProps::from_state_id(state);
            (
                properties.lit,
                properties.facing.to_block_direction().opposite(),
            )
        } else if args.block == &Block::REDSTONE_TORCH {
            (RTorchProps::from_state_id(state).lit, BlockDirection::Down)
        } else {
            return None;
        };
        if matches!(args.event, BlockCallbackEvent::Neighbor { .. })
            && args
                .world
                .is_block_tick_scheduled(args.position, args.block)
        {
            return Some(Vec::new());
        }
        let support = args.position.offset(direction.to_offset());
        let should_be_lit = args.world.get_directional_power(&support, direction) == 0;
        if lit == should_be_lit {
            return Some(Vec::new());
        }
        if matches!(args.event, BlockCallbackEvent::Neighbor { .. }) {
            return Some(vec![BlockCallbackEffect::ScheduleBlock {
                position: *args.position,
                block: args.block.id,
                delay: 2,
                priority: TickPriority::Normal,
            }]);
        }
        let after = if args.block == &Block::REDSTONE_WALL_TORCH {
            let mut properties = RWallTorchProps::from_state_id(state);
            properties.lit = should_be_lit;
            properties.to_state_id(args.block)
        } else {
            let mut properties = RTorchProps::from_state_id(state);
            properties.lit = should_be_lit;
            properties.to_state_id(args.block)
        };
        let mut effects = vec![BlockCallbackEffect::SetBlock {
            position: *args.position,
            state: after,
            flags: BlockFlags::NOTIFY_ALL,
        }];
        effects.extend(notify_around());
        Some(effects)
    }

    fn emits_redstone_power(&self, _args: EmitsRedstonePowerArgs<'_>) -> bool {
        true
    }

    fn get_weak_redstone_power(&self, args: GetRedstonePowerArgs<'_>) -> u8 {
        if args.block == &Block::REDSTONE_WALL_TORCH {
            let props = RWallTorchProps::from_state_id(args.state.id);
            if props.lit && args.direction != props.facing.to_block_direction() {
                return 15;
            }
        } else if args.block == &Block::REDSTONE_TORCH {
            let props = RTorchProps::from_state_id(args.state.id);
            if props.lit && args.direction != BlockDirection::Up {
                return 15;
            }
        }
        0
    }

    fn get_strong_redstone_power(&self, args: GetRedstonePowerArgs<'_>) -> u8 {
        if args.direction == BlockDirection::Down {
            if args.block == &Block::REDSTONE_WALL_TORCH {
                let props = RWallTorchProps::from_state_id(args.state.id);
                if props.lit {
                    return 15;
                }
            } else if args.block == &Block::REDSTONE_TORCH {
                let props = RTorchProps::from_state_id(args.state.id);
                if props.lit {
                    return 15;
                }
            }
        }
        0
    }

    fn scheduled_tick_intent(
        &self,
        args: crate::block::ScheduledTickIntentArgs<'_>,
    ) -> Option<Vec<crate::block::neighbor::BlockCallbackEffect>> {
        self.callback_intent(crate::block::neighbor::BlockCallbackIntentArgs {
            world: args.world,
            block: args.block,
            position: args.position,
            event: crate::block::neighbor::BlockCallbackEvent::Scheduled,
        })
    }

    fn on_scheduled_tick(&self, args: OnScheduledTickArgs<'_>) {
        crate::block::neighbor::apply_legacy_scheduled_intent(self, args);
    }

    fn placed(&self, args: PlacedArgs<'_>) {
        update_neighbors(args.world, args.position);
    }

    fn on_state_replaced(&self, args: OnStateReplacedArgs<'_>) {
        update_neighbors(args.world, args.position);
    }
}

pub fn should_be_lit(world: &World, pos: &BlockPos, face: BlockDirection) -> bool {
    let other_pos = pos.offset(face.to_offset());
    let (block, state) = world.get_block_and_state(&other_pos);
    get_redstone_power(block, state, world, &other_pos, face) == 0
}

pub fn update_neighbors(world: &Arc<World>, pos: &BlockPos) {
    for dir in BlockDirection::all() {
        let other_pos = pos.offset(dir.to_offset());
        world.update_neighbors(&other_pos, None);
    }
}

fn can_place_at(world: &dyn BlockAccessor, block_pos: &BlockPos, facing: BlockDirection) -> bool {
    world
        .get_block_state(&block_pos.offset(facing.to_offset()))
        .is_side_solid(facing.opposite())
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::block::neighbor::{
        BlockCallbackEffect, BlockCallbackEvent, BlockCallbackIntentArgs, CapturedNumericPower,
        NeighborFact, NeighborSnapshot, NeighborTranscript,
    };

    #[test]
    fn powered_support_schedules_then_switches_off_without_duplicate_neighbor_tick() {
        let position = BlockPos::new(0, 80, 0);
        let support = position.down();
        let mut properties = RTorchProps::default(&Block::REDSTONE_TORCH);
        properties.lit = true;
        let mut snapshot = NeighborSnapshot::default();
        snapshot
            .blocks
            .insert(position, properties.to_state_id(&Block::REDSTONE_TORCH));
        snapshot
            .blocks
            .insert(support, Block::REDSTONE_BLOCK.default_state.id);
        snapshot
            .scheduled
            .insert((position, u16::from(Block::REDSTONE_TORCH.id)), false);
        snapshot.directional_power.insert(
            (support, BlockDirection::Down as u8),
            CapturedNumericPower {
                value: 15,
                reads: vec![(support, Block::REDSTONE_BLOCK.default_state.id)],
                entity_reads: Vec::new(),
            },
        );
        let evaluate = |snapshot, event| {
            let view = NeighborTranscript::new(snapshot, 64).unwrap();
            let result = RedstoneTorchBlock
                .callback_intent(BlockCallbackIntentArgs {
                    world: &view,
                    block: &Block::REDSTONE_TORCH,
                    position: &position,
                    event,
                })
                .unwrap();
            (result, view.finish().unwrap().0)
        };
        let neighbor = BlockCallbackEvent::Neighbor {
            source: Block::REDSTONE_BLOCK.id,
            notify: true,
        };
        let (pending, facts) = evaluate(snapshot.clone(), neighbor);
        assert_eq!(
            pending,
            vec![BlockCallbackEffect::ScheduleBlock {
                position,
                block: Block::REDSTONE_TORCH.id,
                delay: 2,
                priority: TickPriority::Normal,
            }]
        );
        assert!(facts.contains(&NeighborFact::Block(
            support,
            Block::REDSTONE_BLOCK.default_state.id
        )));
        snapshot
            .scheduled
            .insert((position, u16::from(Block::REDSTONE_TORCH.id)), true);
        assert!(evaluate(snapshot.clone(), neighbor).0.is_empty());
        let (switched, _) = evaluate(snapshot, BlockCallbackEvent::Scheduled);
        let BlockCallbackEffect::SetBlock { state, .. } = switched[0] else {
            panic!("missing torch transition");
        };
        assert!(!RTorchProps::from_state_id(state).lit);
        assert_eq!(switched.len(), 7);
        assert!(
            switched[1..]
                .iter()
                .all(|effect| matches!(effect, BlockCallbackEffect::NotifyNeighbors { .. }))
        );
    }
}
