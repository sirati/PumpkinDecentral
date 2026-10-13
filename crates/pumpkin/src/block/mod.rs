use pumpkin_data::fluid::Fluid;
use pumpkin_data::tag::{self, Taggable};
use pumpkin_data::{Block, BlockId, BlockState};

use pumpkin_data::BlockStateId;
use pumpkin_util::math::position::BlockPos;
use pumpkin_util::random::get_seed;

use crate::entity::experience_orb::ExperienceOrbEntity;
use crate::entity::player::Player;
use crate::world::World;
use crate::world::loot::LootContextParameters;
use std::sync::Arc;

pub mod blocks;
pub mod entities;
pub mod fluid;
pub mod neighbor;
pub mod registry;
pub mod viewer;

use crate::block::registry::BlockActionResult;
use crate::entity::EntityBase;
use crate::server::Server;
use pumpkin_data::BlockDirection;
use pumpkin_data::block_rotation::{Mirror, Rotation};
use pumpkin_data::data_component_impl::EquipmentSlot;
use pumpkin_data::item_stack::ItemStack;
use pumpkin_inventory::screen_handler::ScreenHandlerFactory;
use pumpkin_protocol::java::server::play::SUseItemOn;
use pumpkin_util::math::boundingbox::BoundingBox;
use pumpkin_util::math::vector3::Vector3;
use pumpkin_world::world::{BlockAccessor, BlockFlags};

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum PathComputationType {
    Land,
    Water,
    Air,
}

pub trait BlockMetadata {
    fn ids() -> Box<[BlockId]>;
}

pub trait FluidMetadata {
    fn ids() -> Box<[u16]>;
}

pub(crate) fn stop_vertical_movement_after_fall(entity: &dyn EntityBase) {
    let entity = entity.get_entity();
    let mut velocity = entity.velocity.load();
    velocity.y = 0.0;
    entity.velocity.store(velocity);
}

pub(crate) fn bounce_entity_after_fall(entity: &dyn EntityBase, bounce_multiplier: f64) {
    let base_entity = entity.get_entity();
    let mut velocity = base_entity.velocity.load();

    if base_entity.is_sneaking() {
        velocity.y = 0.0;
    } else if velocity.y < 0.0 {
        let entity_factor = if entity.get_living_entity().is_some() {
            1.0
        } else {
            0.8
        };
        velocity.y = -velocity.y * bounce_multiplier * entity_factor;
    }

    base_entity.velocity.store(velocity);
}

pub trait BlockBehaviour: Send + Sync {
    fn is_valid_bonemeal_target(&self, _args: BonemealArgs<'_>) -> bool {
        false
    }

    fn is_bonemeal_success(&self, _args: BonemealArgs<'_>) -> bool {
        true
    }

    fn perform_bonemeal(&self, _args: BonemealArgs<'_>) {}

    fn normal_use(&self, _args: NormalUseArgs<'_>) -> BlockActionResult {
        BlockActionResult::Pass
    }

    fn normal_use_intent(
        &self,
        _args: NormalUseIntentArgs<'_>,
    ) -> Option<(BlockActionResult, Vec<neighbor::BlockCallbackEffect>)> {
        None
    }

    fn get_screen_handler_factory(
        &self,
        _args: GetScreenHandlerFactoryArgs<'_>,
    ) -> Option<Box<dyn ScreenHandlerFactory>> {
        None
    }

    fn use_with_item(&self, _args: UseWithItemArgs<'_>) -> BlockActionResult {
        BlockActionResult::PassToDefaultBlockAction
    }

    fn on_entity_collision(&self, _args: OnEntityCollisionArgs<'_>) {}

    fn on_projectile_hit(&self, _args: OnProjectileHitArgs<'_>) {}

    /// Called when an entity is standing on / walking over the top face of this block.
    fn on_entity_step(&self, _args: OnEntityStepArgs<'_>) {}

    fn should_drop_items_on_explosion(&self) -> bool {
        true
    }

    fn explode(&self, _args: ExplodeArgs<'_>) {}

    /// Handles the block event, which is an event specific to a block with an integer ID and data.
    ///
    /// returns whether the event was handled successfully
    fn on_synced_block_event(&self, _args: OnSyncedBlockEventArgs<'_>) -> bool {
        false
    }

    /// getPlacementState in source code
    fn on_place(&self, args: OnPlaceArgs<'_>) -> BlockStateId {
        args.block.default_state.id
    }

    fn placement_intent(&self, _args: PlacementIntentArgs<'_>) -> Option<BlockStateId> {
        None
    }

    fn random_tick(&self, _args: RandomTickArgs<'_>) {}

    fn can_place_at(&self, _args: CanPlaceAtArgs<'_>) -> bool {
        true
    }

    fn can_update_intent(&self, _args: CanUpdateIntentArgs<'_>) -> Option<bool> {
        None
    }

    fn can_update_at(&self, _args: CanUpdateAtArgs<'_>) -> bool {
        false
    }

    /// onBlockAdded in source code
    fn placed(&self, _args: PlacedArgs<'_>) {}

    fn player_placed(&self, _args: PlayerPlacedArgs<'_>) {}

    fn on_landed_upon(&self, args: OnLandedUponArgs<'_>) {
        if let Some(living) = args.entity.get_living_entity() {
            living.handle_fall_damage(args.entity, args.fall_distance, 1.0);
        }
    }

    fn update_entity_movement_after_fall_on(&self, args: UpdateEntityMovementAfterFallOnArgs<'_>) {
        stop_vertical_movement_after_fall(args.entity);
    }

    fn broken(&self, _args: BrokenArgs<'_>) {}

    fn player_broken_intent(
        &self,
        _args: neighbor::PlayerBrokenIntentArgs<'_>,
    ) -> Option<Vec<neighbor::BlockCallbackEffect>> {
        None
    }

    fn on_neighbor_update(&self, _args: OnNeighborUpdateArgs<'_>) {}

    /// Called if a block state is replaced or it replaces another state
    fn prepare(&self, _args: PrepareArgs<'_>) {}

    fn get_state_for_neighbor_update(
        &self,
        args: GetStateForNeighborUpdateArgs<'_>,
    ) -> BlockStateId {
        args.state_id
    }

    fn on_scheduled_tick(&self, _args: OnScheduledTickArgs<'_>) {}

    /// Returns the callback's semantic requests against a captured owner view.
    /// None means this callback has not yet moved to the pure interpreter.
    fn scheduled_tick_intent(
        &self,
        _args: ScheduledTickIntentArgs<'_>,
    ) -> Option<Vec<neighbor::BlockCallbackEffect>> {
        None
    }

    fn random_tick_intent(
        &self,
        _args: RandomTickIntentArgs<'_>,
    ) -> Option<Vec<neighbor::BlockCallbackEffect>> {
        None
    }

    fn callback_intent(
        &self,
        args: neighbor::BlockCallbackIntentArgs<'_>,
    ) -> Option<Vec<neighbor::BlockCallbackEffect>> {
        match args.event {
            neighbor::BlockCallbackEvent::Scheduled => {
                self.scheduled_tick_intent(ScheduledTickIntentArgs {
                    world: args.world,
                    block: args.block,
                    position: args.position,
                })
            }
            _ => None,
        }
    }

    fn on_state_replaced(&self, _args: OnStateReplacedArgs<'_>) {}

    // --- Redstone/Comparator Methods ---

    /// Sides where redstone connects to
    fn emits_redstone_power(&self, _args: EmitsRedstonePowerArgs<'_>) -> bool {
        false
    }

    /// Weak redstone power, aka. block that should be powered needs to be directly next to the source block
    fn get_weak_redstone_power(&self, _args: GetRedstonePowerArgs<'_>) -> u8 {
        0
    }

    /// Strong redstone power. this can power a block that then gives power
    fn get_strong_redstone_power(&self, _args: GetRedstonePowerArgs<'_>) -> u8 {
        0
    }

    fn get_comparator_output(&self, _args: GetComparatorOutputArgs<'_>) -> Option<u8> {
        None
    }

    fn get_inside_collision_shape(&self, _args: GetInsideCollisionShapeArgs<'_>) -> BoundingBox {
        BoundingBox::full_block()
    }

    fn mirror(&self, block: &Block, state_id: BlockStateId, mirror: Mirror) -> &'static BlockState {
        block.mirror(state_id, mirror)
    }

    fn rotate(
        &self,
        block: &Block,
        state_id: BlockStateId,
        rotation: Rotation,
    ) -> &'static BlockState {
        block.rotate(state_id, rotation)
    }

    fn is_pathfindable(&self, state: &BlockState, computation_type: PathComputationType) -> bool {
        match computation_type {
            PathComputationType::Water => {
                state.is_waterlogged()
                    || Fluid::from_state_id(state.id)
                        .is_some_and(|f| f.has_tag(&tag::Fluid::MINECRAFT_WATER))
            }
            PathComputationType::Land | PathComputationType::Air => !state.is_full_cube(),
        }
    }
}

#[derive(Clone, Copy)]
pub struct BonemealArgs<'a> {
    pub world: &'a Arc<World>,
    pub block: &'a Block,
    pub position: &'a BlockPos,
    pub state_id: BlockStateId,
}

pub struct NormalUseArgs<'a> {
    pub server: &'a Server,
    pub world: &'a Arc<World>,
    pub block: &'a Block,
    pub position: &'a BlockPos,
    pub player: &'a Arc<Player>,
    pub hit: &'a BlockHitResult<'a>,
}

pub struct NormalUseIntentArgs<'a> {
    pub world: &'a dyn NeighborUpdateWorld,
    pub block: &'a Block,
    pub position: &'a BlockPos,
    pub can_modify_world: bool,
    pub sneaking: bool,
    pub facing: pumpkin_data::block_properties::HorizontalFacing,
}

#[derive(Clone, Copy)]
pub struct GetScreenHandlerFactoryArgs<'a> {
    pub server: &'a Server,
    pub world: &'a Arc<World>,
    pub block: &'a Block,
    pub position: &'a BlockPos,
    pub player: &'a Arc<Player>,
}

pub struct UseWithItemArgs<'a> {
    pub server: &'a Server,
    pub world: &'a Arc<World>,
    pub block: &'a Block,
    pub position: &'a BlockPos,
    pub player: &'a Arc<Player>,
    pub hit: &'a BlockHitResult<'a>,
    pub item_stack: &'a mut ItemStack,
    pub equipment_slot: &'a EquipmentSlot,
}

pub struct BlockHitResult<'a> {
    pub face: &'a BlockDirection,
    pub cursor_pos: &'a Vector3<f32>,
}

pub struct OnEntityCollisionArgs<'a> {
    pub server: &'a Server,
    pub world: &'a Arc<World>,
    pub block: &'a Block,
    pub state: &'a BlockState,
    pub position: &'a BlockPos,
    pub entity: &'a dyn EntityBase,
}

pub struct OnProjectileHitArgs<'a> {
    pub server: &'a Server,
    pub world: &'a Arc<World>,
    pub block: &'a Block,
    pub state: &'a BlockState,
    pub position: &'a BlockPos,
    pub projectile: &'a dyn EntityBase,
    pub hit_pos: &'a Vector3<f64>,
}

pub struct OnEntityStepArgs<'a> {
    pub world: &'a Arc<World>,
    pub block: &'a Block,
    pub state: &'a BlockState,
    pub position: &'a BlockPos,
    pub entity: &'a dyn EntityBase,
    pub below_supporting_block: bool,
}

pub struct ExplodeArgs<'a> {
    pub world: &'a Arc<World>,
    pub block: &'a Block,
    pub position: &'a BlockPos,
}

pub struct OnSyncedBlockEventArgs<'a> {
    pub world: &'a Arc<World>,
    pub block: &'a Block,
    pub position: &'a BlockPos,
    pub r#type: u8,
    pub data: u8,
}

pub struct OnPlaceArgs<'a> {
    pub server: &'a Server,
    pub world: &'a World,
    pub block: &'a Block,
    pub position: &'a BlockPos,
    pub direction: BlockDirection,
    pub player: &'a Player,
    pub replacing: BlockIsReplacing,
    pub use_item_on: &'a SUseItemOn,
}

pub struct PlacementIntentArgs<'a> {
    pub world: &'a dyn NeighborUpdateWorld,
    pub block: &'a Block,
    pub position: &'a BlockPos,
    pub direction: BlockDirection,
    pub facing: pumpkin_data::block_properties::HorizontalFacing,
    pub replacing: BlockIsReplacing,
    pub hit: [f32; 3],
}

pub struct RandomTickArgs<'a> {
    pub world: &'a Arc<World>,
    pub block: &'a Block,
    pub position: &'a BlockPos,
}

pub struct RandomTickIntentArgs<'a> {
    pub world: &'a dyn NeighborUpdateWorld,
    pub block: &'a Block,
    pub position: &'a BlockPos,
    pub random: &'a mut pumpkin_util::random::RandomGenerator,
    pub ambient_darkness: u8,
}

pub struct CanPlaceAtArgs<'a> {
    pub server: Option<&'a Server>,
    pub world: Option<&'a World>,
    pub block_accessor: &'a dyn BlockAccessor,
    pub block: &'a Block,
    pub state: &'a BlockState,
    pub position: &'a BlockPos,
    pub direction: Option<BlockDirection>,
    pub player: Option<&'a Player>,
    pub use_item_on: Option<&'a SUseItemOn>,
}

pub struct CanUpdateIntentArgs<'a> {
    pub world: &'a dyn NeighborUpdateWorld,
    pub block: &'a Block,
    pub state_id: BlockStateId,
    pub position: &'a BlockPos,
    pub direction: BlockDirection,
    pub hit: [f32; 3],
}

pub struct CanUpdateAtArgs<'a> {
    pub world: &'a World,
    pub block: &'a Block,
    pub state_id: BlockStateId,
    pub position: &'a BlockPos,
    pub direction: BlockDirection,
    pub player: &'a Player,
    pub use_item_on: &'a SUseItemOn,
}

pub struct PlacedArgs<'a> {
    pub world: &'a Arc<World>,
    pub block: &'a Block,
    pub state_id: BlockStateId,
    pub old_state_id: BlockStateId,
    pub position: &'a BlockPos,
    pub notify: bool,
}

pub struct PlayerPlacedArgs<'a> {
    pub world: &'a Arc<World>,
    pub block: &'a Block,
    pub state_id: BlockStateId,
    pub position: &'a BlockPos,
    pub direction: BlockDirection,
    pub player: &'a Player,
}

pub struct OnLandedUponArgs<'a> {
    pub world: &'a Arc<World>,
    pub fall_distance: f32,
    pub entity: &'a dyn EntityBase,
}

pub struct UpdateEntityMovementAfterFallOnArgs<'a> {
    pub entity: &'a dyn EntityBase,
}

pub struct BrokenArgs<'a> {
    pub block: &'a Block,
    pub player: &'a Arc<Player>,
    pub position: &'a BlockPos,
    pub server: &'a Server,
    pub world: &'a Arc<World>,
    pub state: &'a BlockState,
}

pub struct OnNeighborUpdateArgs<'a> {
    pub world: &'a Arc<World>,
    pub block: &'a Block,
    pub position: &'a BlockPos,
    pub source_block: &'a Block,
    pub notify: bool,
}

pub struct PrepareArgs<'a> {
    pub world: &'a Arc<World>,
    pub block: &'a Block,
    pub state_id: BlockStateId,
    pub position: &'a BlockPos,
    pub flags: BlockFlags,
}

pub struct GetStateForNeighborUpdateArgs<'a> {
    pub world: &'a dyn NeighborUpdateWorld,
    pub block: &'a Block,
    pub state_id: BlockStateId,
    pub position: &'a BlockPos,
    pub direction: BlockDirection,
    pub neighbor_position: &'a BlockPos,
    pub neighbor_state_id: BlockStateId,
}

/// The state-query callback only observes blocks and emits scheduled work.
/// Owner interpreters implement this interface against their private snapshot.
pub trait NeighborUpdateWorld: BlockAccessor {
    fn get_directional_power(&self, position: &BlockPos, direction: BlockDirection) -> u8;
    fn receives_redstone_power(&self, position: &BlockPos) -> bool;
    fn get_raw_brightness(&self, position: &BlockPos, ambient_darkness: u8) -> u8;
    fn emits_redstone_power(
        &self,
        block: &Block,
        state: &BlockState,
        direction: BlockDirection,
    ) -> bool;
    fn get_power_on_side(
        &self,
        position: &BlockPos,
        side: pumpkin_data::block_properties::HorizontalFacing,
        only_gate: bool,
    ) -> u8;
    fn schedule_block_tick(
        &self,
        block: &Block,
        position: BlockPos,
        delay: u8,
        priority: pumpkin_world::tick::TickPriority,
    );
    fn schedule_fluid_tick(
        &self,
        fluid: &Fluid,
        position: BlockPos,
        delay: u8,
        priority: pumpkin_world::tick::TickPriority,
    );
    fn is_block_tick_scheduled(&self, position: &BlockPos, block: &Block) -> bool;
    fn get_block_and_state_id(&self, position: &BlockPos) -> (&'static Block, BlockStateId) {
        let id = self.get_block_state_id(position);
        (Block::from_state_id(id), id)
    }
}

impl<T: NeighborUpdateWorld + ?Sized> NeighborUpdateWorld for Arc<T> {
    fn get_directional_power(&self, position: &BlockPos, direction: BlockDirection) -> u8 {
        (**self).get_directional_power(position, direction)
    }
    fn receives_redstone_power(&self, position: &BlockPos) -> bool {
        (**self).receives_redstone_power(position)
    }
    fn get_raw_brightness(&self, position: &BlockPos, ambient_darkness: u8) -> u8 {
        (**self).get_raw_brightness(position, ambient_darkness)
    }
    fn emits_redstone_power(
        &self,
        block: &Block,
        state: &BlockState,
        direction: BlockDirection,
    ) -> bool {
        (**self).emits_redstone_power(block, state, direction)
    }
    fn get_power_on_side(
        &self,
        position: &BlockPos,
        side: pumpkin_data::block_properties::HorizontalFacing,
        only_gate: bool,
    ) -> u8 {
        (**self).get_power_on_side(position, side, only_gate)
    }
    fn schedule_block_tick(
        &self,
        block: &Block,
        position: BlockPos,
        delay: u8,
        priority: pumpkin_world::tick::TickPriority,
    ) -> () {
        (**self).schedule_block_tick(block, position, delay, priority)
    }
    fn schedule_fluid_tick(
        &self,
        fluid: &Fluid,
        position: BlockPos,
        delay: u8,
        priority: pumpkin_world::tick::TickPriority,
    ) -> () {
        (**self).schedule_fluid_tick(fluid, position, delay, priority)
    }
    fn is_block_tick_scheduled(&self, position: &BlockPos, block: &Block) -> bool {
        (**self).is_block_tick_scheduled(position, block)
    }
}

impl NeighborUpdateWorld for World {
    fn get_directional_power(&self, position: &BlockPos, direction: BlockDirection) -> u8 {
        let (block, state) = self.get_block_and_state(position);
        crate::block::blocks::redstone::get_redstone_power(block, state, self, position, direction)
    }
    fn receives_redstone_power(&self, position: &BlockPos) -> bool {
        crate::block::blocks::redstone::block_receives_redstone_power(self, position)
    }
    fn get_raw_brightness(&self, position: &BlockPos, ambient_darkness: u8) -> u8 {
        World::get_raw_brightness(self, position, ambient_darkness)
    }
    fn emits_redstone_power(
        &self,
        block: &Block,
        state: &BlockState,
        direction: BlockDirection,
    ) -> bool {
        self.block_registry
            .emits_redstone_power(block, state, direction)
    }
    fn get_power_on_side(
        &self,
        position: &BlockPos,
        side: pumpkin_data::block_properties::HorizontalFacing,
        only_gate: bool,
    ) -> u8 {
        crate::block::blocks::redstone::abstract_redstone_gate::get_power_on_side(
            self, position, side, only_gate,
        )
    }
    fn schedule_block_tick(
        &self,
        block: &Block,
        position: BlockPos,
        delay: u8,
        priority: pumpkin_world::tick::TickPriority,
    ) {
        World::schedule_block_tick(self, block, position, delay, priority);
    }
    fn schedule_fluid_tick(
        &self,
        fluid: &Fluid,
        position: BlockPos,
        delay: u8,
        priority: pumpkin_world::tick::TickPriority,
    ) {
        World::schedule_fluid_tick(self, fluid, position, delay, priority);
    }
    fn is_block_tick_scheduled(&self, position: &BlockPos, block: &Block) -> bool {
        World::is_block_tick_scheduled(self, position, block)
    }
}

pub struct OnScheduledTickArgs<'a> {
    pub world: &'a Arc<World>,
    pub block: &'a Block,
    pub position: &'a BlockPos,
}

pub struct ScheduledTickIntentArgs<'a> {
    pub world: &'a dyn NeighborUpdateWorld,
    pub block: &'a Block,
    pub position: &'a BlockPos,
}

pub struct OnStateReplacedArgs<'a> {
    pub world: &'a Arc<World>,
    pub block: &'a Block,
    pub old_state_id: BlockStateId,
    pub position: &'a BlockPos,
    pub moved: bool,
}

pub struct EmitsRedstonePowerArgs<'a> {
    pub block: &'a Block,
    pub state: &'a BlockState,
    pub direction: BlockDirection,
}

pub struct GetRedstonePowerArgs<'a> {
    pub world: &'a dyn RedstonePowerWorld,
    pub block: &'a Block,
    pub state: &'a BlockState,
    pub position: &'a BlockPos,
    pub direction: BlockDirection,
}

pub trait RedstonePowerWorld: BlockAccessor {
    fn power_registry(&self) -> &registry::BlockRegistry;
    fn block_entity_signal(&self, position: &BlockPos, signal: BlockEntitySignal) -> u8;
    fn comparator_output(&self, position: &BlockPos) -> u8 {
        self.block_entity_signal(position, BlockEntitySignal::Comparator)
    }
}

#[derive(Clone, Copy)]
pub enum BlockEntitySignal {
    Comparator,
    TrappedChestViewers,
    TestBlockPowered,
    JukeboxPlaying,
}

impl<T: RedstonePowerWorld + ?Sized> RedstonePowerWorld for Arc<T> {
    fn power_registry(&self) -> &registry::BlockRegistry {
        (**self).power_registry()
    }
    fn block_entity_signal(&self, position: &BlockPos, signal: BlockEntitySignal) -> u8 {
        (**self).block_entity_signal(position, signal)
    }
}

impl RedstonePowerWorld for World {
    fn power_registry(&self) -> &registry::BlockRegistry {
        &self.block_registry
    }
    fn block_entity_signal(&self, position: &BlockPos, signal: BlockEntitySignal) -> u8 {
        use crate::block::entities::{
            comparator::ComparatorBlockEntity, jukebox::JukeboxBlockEntity,
            test_block::TestBlockBlockEntity, trapped_chest::TrappedChestBlockEntity,
        };
        let Some(entity) = self.get_block_entity(position) else {
            return 0;
        };
        match signal {
            BlockEntitySignal::Comparator => entity
                .as_any()
                .downcast_ref::<ComparatorBlockEntity>()
                .map_or(0, |comparator| {
                    comparator
                        .output_signal
                        .load(std::sync::atomic::Ordering::Relaxed)
                }),
            BlockEntitySignal::TrappedChestViewers => entity
                .as_any()
                .downcast_ref::<TrappedChestBlockEntity>()
                .map_or(0, |chest| chest.get_viewer_count().min(15) as u8),
            BlockEntitySignal::TestBlockPowered => entity
                .as_any()
                .downcast_ref::<TestBlockBlockEntity>()
                .map_or(0, |block| if block.is_powered() { 15 } else { 0 }),
            BlockEntitySignal::JukeboxPlaying => entity
                .as_any()
                .downcast_ref::<JukeboxBlockEntity>()
                .map_or(0, |jukebox| if jukebox.is_playing() { 15 } else { 0 }),
        }
    }
}

pub struct GetComparatorOutputArgs<'a> {
    pub world: &'a World,
    pub block: &'a Block,
    pub state: &'a BlockState,
    pub position: &'a BlockPos,
    /// Face of this block that the reading comparator sits against.
    pub direction: BlockDirection,
}

pub struct GetInsideCollisionShapeArgs<'a> {
    pub world: &'a World,
    pub block: &'a Block,
    pub state: &'a BlockState,
    pub position: &'a BlockPos,
}

#[derive(Clone)]
pub struct BlockEvent {
    pub pos: BlockPos,
    pub r#type: u8,
    pub data: u8,
}

pub fn drop_loot(
    world: &Arc<World>,
    block: &Block,
    pos: &BlockPos,
    experience: bool,
    params: &LootContextParameters,
) {
    let key = format!("minecraft:blocks/{}", block.name);
    if let Some(loot_table) = pumpkin_data::loot_table::get_loot_table(&key) {
        let seed: i64 = rand::random();
        let items = crate::world::loot::generate_loot_with_context(loot_table, seed, params);
        if !items.is_empty() {
            let mut event = crate::plugin::block::block_drop_item::BlockDropItemEvent {
                block_pos: *pos,
                world: world.clone(),
                player: None,
                items,
                cancelled: false,
            };
            if let Some(server) = world.server.upgrade() {
                server.plugin_manager.fire_blocking(&server, &mut event);
            }
            if !event.cancelled {
                let block_entity = world.get_block_entity(pos);
                for mut stack in event.items {
                    if let Some(block_entity) = &block_entity
                        && Block::from_item_id(stack.item.id) == Some(block)
                    {
                        block_entity.collect_item_components(&mut stack);
                    }
                    world.drop_stack(pos, stack);
                }
            }
        }
    }

    if experience && block.experience.is_some() {
        let amount = crate::world::loot::block_experience_fact(block, get_seed(), true, params);
        if amount > 0 {
            let mut event = crate::plugin::block::block_exp::BlockExpEvent {
                block_pos: *pos,
                world: world.clone(),
                exp: i32::try_from(amount).expect("block XP provider returns an i32"),
            };
            if let Some(server) = world.server.upgrade() {
                server.plugin_manager.fire_blocking(&server, &mut event);
            }
            if event.exp > 0 {
                ExperienceOrbEntity::spawn(world, pos.to_f64(), event.exp as u32);
            }
        }
    }
}

pub fn calc_block_breaking(player: &Player, state: &BlockState, block: &'static Block) -> f32 {
    let hardness = state.hardness;
    #[expect(clippy::float_cmp)]
    if hardness == -1.0 {
        // unbreakable
        return 0.0;
    }
    let i = if player.can_harvest(state, block) {
        30.0
    } else {
        100.0
    };

    player.get_mining_speed(block) / hardness / i
}

#[derive(Clone, Copy, PartialEq, Eq, Debug)]
pub enum BlockIsReplacing {
    Itself(BlockStateId),
    Water(u8),
    Other,
    None,
}

impl BlockIsReplacing {
    #[must_use]
    /// Returns true if the block was a water source block.
    pub const fn water_source(&self) -> bool {
        match self {
            // Level 0 means the water is a source block
            Self::Water(level) => *level == 0,
            _ => false,
        }
    }
}

/// Vanilla `AbstractContainerMenu.getRedstoneSignalFromBlockEntity`: read a block
/// entity's own inventory as a comparator would.
#[must_use]
pub fn container_comparator_output(args: &GetComparatorOutputArgs<'_>) -> Option<u8> {
    let inventory = args
        .world
        .get_block_entity(args.position)?
        .get_inventory()?;
    Some(calculate_comparator_output(inventory.as_ref()))
}

pub fn calculate_comparator_output(inventory: &dyn pumpkin_inventory::Inventory) -> u8 {
    let size = inventory.size();
    if size == 0 {
        return 0;
    }
    let mut fill_sum = 0.0;
    let mut non_empty_count = 0;
    for i in 0..size {
        let stack = inventory.get_stack(i);
        if !stack.is_empty() {
            let max_stack = stack.get_max_stack_size() as f32;
            let count = stack.item_count as f32;
            fill_sum += count / max_stack;
            non_empty_count += 1;
        }
    }
    if non_empty_count == 0 {
        return 0;
    }
    let percentage = fill_sum / (size as f32);
    let output = 1.0 + percentage * 14.0;
    output.floor() as u8
}
