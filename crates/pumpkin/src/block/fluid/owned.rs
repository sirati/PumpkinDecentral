use std::collections::{HashMap, HashSet, VecDeque};

use pumpkin_data::fluid::{EnumVariants, Falling, Fluid, FluidProperties, Level};
use pumpkin_data::{Block, BlockDirection, BlockStateId};
use pumpkin_util::math::position::BlockPos;

use super::{flowing_trait::FlowingFluidProperties, physics};

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct FluidRules {
    pub fast_lava: bool,
    pub water_source_conversion: bool,
    pub lava_source_conversion: bool,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash)]
pub enum FluidPolicy {
    Flow {
        from: BlockPos,
        to: BlockPos,
        fluid: u16,
    },
    Form {
        position: BlockPos,
        state: BlockStateId,
    },
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct FluidWrite {
    pub position: BlockPos,
    pub before: BlockStateId,
    pub after: BlockStateId,
    pub replace_with_drops: bool,
    pub delay: Option<u8>,
    pub fizz: bool,
}

pub struct FluidPlan {
    pub reads: Vec<(BlockPos, BlockStateId)>,
    pub writes: Vec<FluidWrite>,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum FluidUnavailable {
    Block(BlockPos),
    Policy(FluidPolicy),
    Capacity,
    Invalid,
}

pub trait FluidPhaseView {
    fn block(&self, position: BlockPos) -> Option<BlockStateId>;
    fn policy(&self, policy: FluidPolicy) -> Option<bool>;
}

struct Flow<'a, V> {
    view: &'a V,
    fluid: &'static Fluid,
    rules: FluidRules,
    reads: HashMap<BlockPos, BlockStateId>,
    writes: Vec<FluidWrite>,
    capacity: usize,
}

impl<V: FluidPhaseView> Flow<'_, V> {
    fn read(&mut self, position: BlockPos) -> Result<BlockStateId, FluidUnavailable> {
        if let Some(write) = self
            .writes
            .iter()
            .rev()
            .find(|write| write.position == position)
        {
            return Ok(write.after);
        }
        if let Some(state) = self.reads.get(&position) {
            return Ok(*state);
        }
        if self.reads.len() >= self.capacity {
            return Err(FluidUnavailable::Capacity);
        }
        let state = self
            .view
            .block(position)
            .ok_or(FluidUnavailable::Block(position))?;
        self.reads.insert(position, state);
        Ok(state)
    }

    fn offset(position: BlockPos, direction: BlockDirection) -> Result<BlockPos, FluidUnavailable> {
        let offset = direction.to_offset();
        Ok(BlockPos::new(
            position
                .0
                .x
                .checked_add(offset.x)
                .ok_or(FluidUnavailable::Invalid)?,
            position
                .0
                .y
                .checked_add(offset.y)
                .ok_or(FluidUnavailable::Invalid)?,
            position
                .0
                .z
                .checked_add(offset.z)
                .ok_or(FluidUnavailable::Invalid)?,
        ))
    }

    fn water(&self) -> bool {
        self.fluid == &Fluid::FLOWING_WATER
    }
    fn delay(&self) -> u8 {
        if self.water() {
            5
        } else if self.rules.fast_lava {
            10
        } else {
            30
        }
    }
    fn distance(&self) -> u8 {
        if self.water() {
            4
        } else if self.rules.fast_lava {
            5
        } else {
            3
        }
    }
    fn conversion(&self) -> bool {
        if self.water() {
            self.rules.water_source_conversion
        } else {
            self.rules.lava_source_conversion
        }
    }
    fn properties(&self, state: BlockStateId) -> Option<FlowingFluidProperties> {
        if self
            .fluid
            .states
            .iter()
            .any(|entry| entry.block_state_id == state)
        {
            return Some(FlowingFluidProperties::from_state_id(state, self.fluid));
        }
        (self.water() && state.to_block().is_waterlogged(state))
            .then(|| self.properties_for(8, false))
    }
    fn properties_for(&self, level: u8, falling: bool) -> FlowingFluidProperties {
        FlowingFluidProperties {
            level: Level::from_index(u16::from(level - 1)),
            falling: if falling {
                Falling::True
            } else {
                Falling::False
            },
        }
    }
    fn source(props: &FlowingFluidProperties) -> bool {
        props.level == Level::L8 && props.falling == Falling::False
    }
    fn allowed(&self, policy: FluidPolicy) -> Result<bool, FluidUnavailable> {
        self.view
            .policy(policy)
            .ok_or(FluidUnavailable::Policy(policy))
    }
    fn hole(&mut self, position: BlockPos) -> Result<bool, FluidUnavailable> {
        let below = self.read(Self::offset(position, BlockDirection::Down)?)?;
        Ok(physics::can_be_replaced(
            below.to_state(),
            below.to_block(),
            self.fluid,
        ))
    }
    fn new_liquid(
        &mut self,
        position: BlockPos,
    ) -> Result<Option<FlowingFluidProperties>, FluidUnavailable> {
        let current = self.read(position)?;
        if let Some(props) = self.properties(current) {
            if Self::source(&props) {
                return Ok(Some(props));
            }
        }
        let mut highest = 0u8;
        let mut sources = 0;
        for direction in [
            BlockDirection::North,
            BlockDirection::South,
            BlockDirection::West,
            BlockDirection::East,
        ] {
            let neighbor = self.read(Self::offset(position, direction)?)?;
            if let Some(props) = self.properties(neighbor) {
                sources += u8::from(Self::source(&props));
                highest = highest.max(if props.falling == Falling::True {
                    8
                } else {
                    props.level.to_index() as u8 + 1
                });
            }
        }
        if self.conversion() && sources >= 2 {
            let below = self.read(Self::offset(position, BlockDirection::Down)?)?;
            if below.to_state().is_solid_block()
                || self
                    .properties(below)
                    .is_some_and(|props| Self::source(&props))
            {
                return Ok(Some(self.properties_for(8, false)));
            }
        }
        let above = self.read(Self::offset(position, BlockDirection::Up)?)?;
        if self.properties(above).is_some() {
            return Ok(Some(self.properties_for(8, true)));
        }
        let drop = if self.water() || self.rules.fast_lava {
            1
        } else {
            2
        };
        Ok(highest
            .checked_sub(drop)
            .filter(|level| *level > 0)
            .map(|level| self.properties_for(level, false)))
    }
    fn put(
        &mut self,
        position: BlockPos,
        after: BlockStateId,
        drops: bool,
        delay: Option<u8>,
        fizz: bool,
    ) -> Result<(), FluidUnavailable> {
        let before = self.read(position)?;
        if before == after {
            return Ok(());
        }
        if self.writes.len() >= self.capacity {
            return Err(FluidUnavailable::Capacity);
        }
        self.writes.push(FluidWrite {
            position,
            before,
            after,
            replace_with_drops: drops,
            delay,
            fizz,
        });
        Ok(())
    }
    fn lava_neighbors(&mut self, position: BlockPos) -> Result<bool, FluidUnavailable> {
        if self.water() {
            return Ok(true);
        }
        let current = self.read(position)?;
        let soul_soil = self
            .read(Self::offset(position, BlockDirection::Down)?)?
            .to_block()
            == &Block::SOUL_SOIL;
        for direction in BlockDirection::all() {
            if direction == BlockDirection::Down {
                continue;
            }
            let neighbor = self.read(Self::offset(position, direction)?)?;
            let water = neighbor.to_block() == &Block::WATER
                || neighbor.to_block().is_waterlogged(neighbor);
            let formed = if water {
                Some(if current == Block::LAVA.default_state.id {
                    Block::OBSIDIAN.default_state.id
                } else {
                    Block::COBBLESTONE.default_state.id
                })
            } else if soul_soil && neighbor.to_block() == &Block::BLUE_ICE {
                Some(Block::BASALT.default_state.id)
            } else {
                None
            };
            if let Some(state) = formed {
                if self.allowed(FluidPolicy::Form { position, state })? {
                    self.put(position, state, false, None, true)?;
                }
                return Ok(false);
            }
        }
        Ok(true)
    }
    fn spread(
        &mut self,
        from: BlockPos,
        to: BlockPos,
        mut props: FlowingFluidProperties,
    ) -> Result<(), FluidUnavailable> {
        let current = self.read(to)?;
        if current.to_block().is_waterlogged(current) {
            return Ok(());
        }
        if !self.water() && props.falling == Falling::True && current.to_block() == &Block::WATER {
            let state = Block::STONE.default_state.id;
            if self.allowed(FluidPolicy::Form {
                position: to,
                state,
            })? {
                self.put(to, state, false, None, true)?;
            }
            return Ok(());
        }
        if let Some(old) = self.properties(current) {
            if Self::source(&old) {
                return Ok(());
            }
            if let Some(converted) = self.new_liquid(to)? {
                if Self::source(&converted) {
                    props = converted;
                }
            }
            if !Self::source(&props)
                && old.falling == props.falling
                && props.level.to_index() <= old.level.to_index()
            {
                return Ok(());
            }
        }
        if !self.allowed(FluidPolicy::Flow {
            from,
            to,
            fluid: self.fluid.id,
        })? {
            return Ok(());
        }
        let drops = !current.to_state().is_air() && Fluid::from_state_id(current).is_none();
        self.put(
            to,
            props.to_state_id(self.fluid),
            drops,
            (!Self::source(&props)).then(|| self.delay()),
            false,
        )
    }
    fn slope_distance(
        &mut self,
        start: BlockPos,
        exclude: BlockDirection,
    ) -> Result<u8, FluidUnavailable> {
        let mut queue = VecDeque::from([(start, 1u8, exclude)]);
        let mut visited = HashSet::new();
        while let Some((position, distance, exclude)) = queue.pop_front() {
            if distance > self.distance() || !visited.insert(position) {
                continue;
            }
            if visited.len() > self.capacity {
                return Err(FluidUnavailable::Capacity);
            }
            if self.hole(position)? {
                return Ok(distance);
            }
            for direction in [
                BlockDirection::North,
                BlockDirection::South,
                BlockDirection::West,
                BlockDirection::East,
            ] {
                if direction == exclude {
                    continue;
                }
                let next = Self::offset(position, direction)?;
                let state = self.read(next)?;
                if physics::can_be_replaced(state.to_state(), state.to_block(), self.fluid)
                    && !self
                        .properties(state)
                        .is_some_and(|props| Self::source(&props))
                {
                    queue.push_back((next, distance + 1, direction.opposite()));
                }
            }
        }
        Ok(u8::MAX)
    }
    fn sides(&mut self, position: BlockPos) -> Result<(), FluidUnavailable> {
        let mut candidates = Vec::with_capacity(4);
        let mut minimum = u8::MAX;
        for direction in [
            BlockDirection::North,
            BlockDirection::South,
            BlockDirection::West,
            BlockDirection::East,
        ] {
            let side = Self::offset(position, direction)?;
            let state = self.read(side)?;
            if !physics::can_be_replaced(state.to_state(), state.to_block(), self.fluid)
                || self
                    .properties(state)
                    .is_some_and(|props| Self::source(&props))
            {
                continue;
            }
            let Some(props) = self.new_liquid(side)? else {
                continue;
            };
            let distance = if self.hole(side)? {
                0
            } else {
                self.slope_distance(side, direction.opposite())?
            };
            if distance < minimum {
                candidates.clear();
                minimum = distance;
            }
            if distance == minimum {
                candidates.push((side, props));
            }
        }
        for (side, props) in candidates {
            self.spread(position, side, props)?;
        }
        Ok(())
    }
    fn tick(&mut self, position: BlockPos) -> Result<(), FluidUnavailable> {
        let state = self.read(position)?;
        let Some(mut props) = self.properties(state) else {
            return Ok(());
        };
        if !self.lava_neighbors(position)? {
            return Ok(());
        }
        if !Self::source(&props) && !state.to_block().is_waterlogged(state) {
            let Some(next) = self.new_liquid(position)? else {
                self.put(position, BlockStateId::AIR, false, None, false)?;
                return Ok(());
            };
            props = next;
            self.put(
                position,
                props.to_state_id(self.fluid),
                false,
                Some(self.delay()),
                false,
            )?;
        }
        if self.hole(position)? {
            self.spread(
                position,
                Self::offset(position, BlockDirection::Down)?,
                self.properties_for(8, true),
            )?;
            if Self::source(&props) {
                let mut sources = 0;
                for direction in [
                    BlockDirection::North,
                    BlockDirection::South,
                    BlockDirection::West,
                    BlockDirection::East,
                ] {
                    let state = self.read(Self::offset(position, direction)?)?;
                    sources += u8::from(
                        self.properties(state)
                            .is_some_and(|props| Self::source(&props)),
                    );
                }
                if sources >= 3 {
                    self.sides(position)?;
                }
            }
        } else {
            self.sides(position)?;
        }
        Ok(())
    }
}

pub fn scheduled<V: FluidPhaseView>(
    view: &V,
    fluid: u16,
    position: BlockPos,
    rules: FluidRules,
    capacity: usize,
) -> Result<FluidPlan, FluidUnavailable> {
    let fluid = match Fluid::from_id(fluid) {
        Some(fluid) if fluid.matches_type(&Fluid::FLOWING_WATER) => &Fluid::FLOWING_WATER,
        Some(fluid) if fluid.matches_type(&Fluid::FLOWING_LAVA) => &Fluid::FLOWING_LAVA,
        _ => return Err(FluidUnavailable::Invalid),
    };
    if capacity == 0 || capacity > 65_536 {
        return Err(FluidUnavailable::Capacity);
    }
    let mut flow = Flow {
        view,
        fluid,
        rules,
        reads: HashMap::new(),
        writes: Vec::new(),
        capacity,
    };
    flow.tick(position)?;
    let mut reads: Vec<_> = flow.reads.into_iter().collect();
    reads.sort_by_key(|(position, _)| (position.0.x, position.0.y, position.0.z));
    Ok(FluidPlan {
        reads,
        writes: flow.writes,
    })
}
