use pumpkin_data::{Block, BlockStateId, sound::Sound, world::WorldEvent};

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) enum ToolTransformKind {
    StripLog,
    ScrapeCopper,
    RemoveWax,
}

#[derive(Clone, Copy, Debug)]
pub(crate) struct ToolTransformResult {
    pub state: BlockStateId,
    pub sound: Sound,
    pub event: Option<WorldEvent>,
    pub damage: u16,
}

pub(crate) fn resolve(kind: ToolTransformKind, state: BlockStateId) -> Option<ToolTransformResult> {
    let block = state.to_block();
    let (target, sound, event) = match kind {
        ToolTransformKind::StripLog => {
            if !(block.name.ends_with("_log")
                || block.name.ends_with("_wood")
                || block.name.ends_with("_stem")
                || block.name.ends_with("_hyphae")
                || block == &Block::BAMBOO_BLOCK)
            {
                return None;
            }
            let target = Block::from_registry_key(&format!("stripped_{}", block.name))?;
            (target, Sound::ItemAxeStrip, None)
        }
        ToolTransformKind::ScrapeCopper => {
            let target = crate::block::blocks::weathering_copper::get_previous(block)?;
            (
                target,
                Sound::ItemAxeScrape,
                Some(WorldEvent::ParticlesScrape),
            )
        }
        ToolTransformKind::RemoveWax => {
            let name = block.name.strip_prefix("waxed_")?;
            let target = Block::from_registry_key(name)?;
            crate::block::blocks::weathering_copper::get_weather_state(target)?;
            (
                target,
                Sound::ItemAxeWaxOff,
                Some(WorldEvent::ParticlesWaxOff),
            )
        }
    };
    if target == block {
        return None;
    }
    let after = if target.states.len() <= 1 {
        target.default_state.id
    } else {
        let properties = block.properties(state)?.to_props();
        target.from_properties(&properties).to_state_id(target)
    };
    Some(ToolTransformResult {
        state: after,
        sound,
        event,
        damage: 1,
    })
}

#[cfg(test)]
mod tests {
    use super::*;
    use pumpkin_data::block_properties::{
        Axis, CopperBulbLikeProperties, PaleOakWoodLikeProperties,
    };

    #[test]
    fn strip_accepts_each_current_strippable_variant_and_preserves_axis() {
        for block in [
            &Block::OAK_LOG,
            &Block::CRIMSON_HYPHAE,
            &Block::BAMBOO_BLOCK,
        ] {
            let mut properties = PaleOakWoodLikeProperties::default(block);
            properties.axis = Axis::Z;
            let before = properties.to_state_id(block);
            let result = resolve(ToolTransformKind::StripLog, before).unwrap();
            assert_eq!(
                result.state.to_block().name,
                format!("stripped_{}", block.name)
            );
            assert_eq!(
                PaleOakWoodLikeProperties::from_state_id(result.state).axis,
                Axis::Z
            );
            assert_eq!(result.damage, 1);
            assert!(resolve(ToolTransformKind::StripLog, result.state).is_none());
        }
        assert!(resolve(ToolTransformKind::StripLog, Block::STONE.default_state.id).is_none());
    }

    #[test]
    fn wax_removal_and_scrape_preserve_bulb_power_and_light() {
        let mut properties = CopperBulbLikeProperties::default(&Block::WAXED_WEATHERED_COPPER_BULB);
        properties.lit = true;
        properties.powered = true;
        let waxed = properties.to_state_id(&Block::WAXED_WEATHERED_COPPER_BULB);
        assert!(resolve(ToolTransformKind::ScrapeCopper, waxed).is_none());
        let unwaxed = resolve(ToolTransformKind::RemoveWax, waxed).unwrap();
        assert_eq!(unwaxed.state.to_block(), &Block::WEATHERED_COPPER_BULB);
        let scraped = resolve(ToolTransformKind::ScrapeCopper, unwaxed.state).unwrap();
        assert_eq!(scraped.state.to_block(), &Block::EXPOSED_COPPER_BULB);
        let after = CopperBulbLikeProperties::from_state_id(scraped.state);
        assert!(after.lit && after.powered);
        assert!(resolve(ToolTransformKind::RemoveWax, scraped.state).is_none());
    }
}

pub(super) fn command_attributes() -> pumpkin_cluster::world_command::ToolAttributes {
    pumpkin_cluster::world_command::ToolAttributes {
        scale: u16::from(pumpkin_data::attributes::Attributes::SCALE.id),
        interaction_range: u16::from(
            pumpkin_data::attributes::Attributes::BLOCK_INTERACTION_RANGE.id,
        ),
        used_statistic: pumpkin_data::statistic::StatisticCategory::Used as u16,
    }
}

pub(super) fn command_transform(
    operation: pumpkin_cluster::world_command::ToolOperation,
    current: u16,
    item: u16,
) -> Result<Option<u16>, pumpkin_cluster::canonical_action::ApplyFailure> {
    if !pumpkin_data::tag::Item::MINECRAFT_AXES.1.contains(&item) {
        return Ok(None);
    }
    let state = BlockStateId::new(current)
        .ok_or(pumpkin_cluster::canonical_action::ApplyFailure::InvalidAction)?;
    if state.to_state().block_entity_type != u16::MAX {
        return Err(pumpkin_cluster::canonical_action::ApplyFailure::InvalidAction);
    }
    let kind = match operation {
        pumpkin_cluster::world_command::ToolOperation::StripLog => ToolTransformKind::StripLog,
        pumpkin_cluster::world_command::ToolOperation::ScrapeCopper => {
            ToolTransformKind::ScrapeCopper
        }
        pumpkin_cluster::world_command::ToolOperation::RemoveWax => ToolTransformKind::RemoveWax,
    };
    Ok(resolve(kind, state).map(|result| result.state.as_u16()))
}

pub(super) fn command_light_plans(
    server: &super::Server,
    location: pumpkin_cluster::world_command::BlockLocation,
    operation: pumpkin_cluster::world_command::ToolOperation,
    current: u16,
) -> Result<
    Vec<pumpkin_cluster::world_action::WorldLightPlan>,
    pumpkin_cluster::canonical_action::ApplyFailure,
> {
    use pumpkin_cluster::canonical_action::ApplyFailure;
    let before = BlockStateId::new(current).ok_or(ApplyFailure::InvalidAction)?;
    let kind = match operation {
        pumpkin_cluster::world_command::ToolOperation::StripLog => ToolTransformKind::StripLog,
        pumpkin_cluster::world_command::ToolOperation::ScrapeCopper => {
            ToolTransformKind::ScrapeCopper
        }
        pumpkin_cluster::world_command::ToolOperation::RemoveWax => ToolTransformKind::RemoveWax,
    };
    let Some(after) = resolve(kind, before) else {
        return Ok(Vec::new());
    };
    if !pumpkin_world::lighting::LightEngine::has_different_light_properties(
        before.to_state(),
        after.state.to_state(),
    ) {
        return Ok(Vec::new());
    }
    super::cluster_world_callback_light::location_light_plans(server, location)
}

pub(super) fn command_companion(
    location: pumpkin_cluster::world_command::BlockLocation,
    current: u16,
) -> Result<
    Option<pumpkin_cluster::world_command::ToolCompanion>,
    pumpkin_cluster::canonical_action::ApplyFailure,
> {
    use pumpkin_cluster::canonical_action::ApplyFailure;
    use pumpkin_data::{
        block_properties::{DoubleBlockHalf, OakDoorLikeProperties},
        tag::Taggable,
    };
    let current = BlockStateId::new(current).ok_or(ApplyFailure::InvalidAction)?;
    if !current
        .to_block()
        .has_tag(&pumpkin_data::tag::Block::MINECRAFT_DOORS)
    {
        return Ok(None);
    }
    let mut properties = OakDoorLikeProperties::from_state_id(current);
    let (offset, half) = match properties.half {
        DoubleBlockHalf::Lower => (1, DoubleBlockHalf::Upper),
        DoubleBlockHalf::Upper => (-1, DoubleBlockHalf::Lower),
    };
    properties.half = half;
    let mut position = location.position;
    position.y = position
        .y
        .checked_add(offset)
        .ok_or(ApplyFailure::InvalidAction)?;
    let location = pumpkin_cluster::world_command::BlockLocation {
        world: location.world,
        position,
    };
    if !location.valid() {
        return Err(ApplyFailure::InvalidAction);
    }
    Ok(Some(pumpkin_cluster::world_command::ToolCompanion {
        location,
        expected_state: properties.to_state_id(current.to_block()).as_u16(),
    }))
}

#[cfg(test)]
mod companion_tests {
    use super::*;
    use pumpkin_data::block_properties::{DoubleBlockHalf, OakDoorLikeProperties};

    #[test]
    fn door_companion_is_inferred_and_preserves_other_properties() {
        let block = &Block::WAXED_WEATHERED_COPPER_DOOR;
        let mut properties = OakDoorLikeProperties::default(block);
        properties.half = DoubleBlockHalf::Lower;
        properties.open = true;
        properties.powered = true;
        let location = pumpkin_cluster::world_command::BlockLocation {
            world: pumpkin_cluster::protocol::WorldId::new(0, 0),
            position: pumpkin_cluster::protocol::BlockPos {
                x: -1,
                y: 70,
                z: 16,
            },
        };
        let original = properties.to_state_id(block);
        let companion = command_companion(location, original.as_u16())
            .unwrap()
            .unwrap();
        assert_eq!(companion.location.position.y, 71);
        let other = BlockStateId::new(companion.expected_state).unwrap();
        let mirrored = OakDoorLikeProperties::from_state_id(other);
        assert_eq!(mirrored.half, DoubleBlockHalf::Upper);
        assert!(mirrored.open && mirrored.powered);
        assert_eq!(
            command_companion(companion.location, companion.expected_state)
                .unwrap()
                .unwrap()
                .expected_state,
            original.as_u16()
        );
        let stripped = command_transform(
            pumpkin_cluster::world_command::ToolOperation::RemoveWax,
            companion.expected_state,
            pumpkin_data::item::Item::IRON_AXE.id,
        )
        .unwrap()
        .unwrap();
        assert_eq!(
            BlockStateId::new(stripped).unwrap().to_block(),
            &Block::WEATHERED_COPPER_DOOR
        );
    }
}

pub(super) fn command_presentations(
    operation: pumpkin_cluster::world_command::ToolOperation,
    location: pumpkin_cluster::world_command::BlockLocation,
    record: &pumpkin_cluster::action_command::ResolutionRecord,
) -> Vec<pumpkin_cluster::world_presentation::WorldPresentationIntent> {
    use pumpkin_cluster::{
        action_command::CommandResult,
        canonical_action::{Cell, Value},
        world_presentation::WorldPresentationIntent,
    };
    let CommandResult::Applied(writes) = &record.result else {
        return Vec::new();
    };
    let Some(write) = writes
        .iter()
        .find(|write| write.target == location.target())
    else {
        return Vec::new();
    };
    let (
        Some(Cell {
            value: Value::Block(before),
            ..
        }),
        Some(Cell {
            value: Value::Block(after),
            ..
        }),
    ) = (&write.before, &write.after)
    else {
        return Vec::new();
    };
    let Some(before) = BlockStateId::new(*before) else {
        return Vec::new();
    };
    let kind = match operation {
        pumpkin_cluster::world_command::ToolOperation::StripLog => ToolTransformKind::StripLog,
        pumpkin_cluster::world_command::ToolOperation::ScrapeCopper => {
            ToolTransformKind::ScrapeCopper
        }
        pumpkin_cluster::world_command::ToolOperation::RemoveWax => ToolTransformKind::RemoveWax,
    };
    let Some(result) = resolve(kind, before).filter(|result| result.state.as_u16() == *after)
    else {
        return Vec::new();
    };
    let mut presentation = vec![WorldPresentationIntent::Sound {
        chunk: location.chunk(),
        position: location.position,
        sound: result.sound as u16,
        volume_milli: 1000,
        pitch_milli: 1000,
        category: pumpkin_data::sound::SoundCategory::Blocks as u8,
    }];
    if let Some(event) = result.event {
        presentation.push(WorldPresentationIntent::WorldEvent {
            chunk: location.chunk(),
            position: location.position,
            event: event as u16,
            data: 0,
        });
    }
    presentation
}

#[cfg(test)]
mod presentation_tests {
    use super::*;
    use pumpkin_cluster::{
        action_command::{ActionFootprint, CommandResult, ResolutionRecord},
        action_identity::ActionIdentity,
        actor::ActorId,
        canonical_action::{ActionId, AppliedWrite, Cell, Value},
        identity::{GlobalPlayerId, PlayerSlot, ServerId},
        protocol::{BlockPos, WorldId},
        time::TickStamp,
        world_command::{BlockLocation, ToolOperation},
        world_presentation::WorldPresentationIntent,
    };

    #[test]
    fn projection_requires_an_applied_matching_registry_transition() {
        let location = BlockLocation {
            world: WorldId::new(0, 0),
            position: BlockPos { x: 3, y: 70, z: 4 },
        };
        let before = Block::OXIDIZED_COPPER.default_state.id;
        let after = resolve(ToolTransformKind::ScrapeCopper, before)
            .unwrap()
            .state;
        let mut record = ResolutionRecord {
            identity: ActionIdentity::Issued(ActionId {
                actor: ActorId::Player(GlobalPlayerId {
                    server: ServerId(1),
                    player: PlayerSlot(1),
                }),
                incarnation: 1,
                sequence: 1,
                family: 4,
            }),
            tick: TickStamp(1),
            footprint: ActionFootprint::default(),
            result: CommandResult::Applied(vec![AppliedWrite {
                target: location.target(),
                before: Some(Cell {
                    revision: 1,
                    value: Value::Block(before.as_u16()),
                }),
                after: Some(Cell {
                    revision: 2,
                    value: Value::Block(after.as_u16()),
                }),
            }]),
        };
        let events = command_presentations(ToolOperation::ScrapeCopper, location, &record);
        assert_eq!(events.len(), 2);
        assert!(matches!(
            events[1],
            WorldPresentationIntent::WorldEvent {
                event: 3005,
                data: 0,
                ..
            }
        ));
        if let CommandResult::Applied(writes) = &mut record.result {
            writes[0].after.as_mut().unwrap().value =
                Value::Block(Block::STONE.default_state.id.as_u16());
        }
        assert!(command_presentations(ToolOperation::ScrapeCopper, location, &record).is_empty());
        record.result = CommandResult::NotApplied;
        assert!(command_presentations(ToolOperation::ScrapeCopper, location, &record).is_empty());
    }
}
