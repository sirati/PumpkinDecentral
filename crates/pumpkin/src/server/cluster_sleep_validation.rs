use pumpkin_cluster::canonical_action::{ApplyFailure, ChunkPart, Target};
use pumpkin_cluster::item_action::PlayerSleepingState;
use pumpkin_cluster::protocol::{BlockPos, ChunkKey, WorldId};
use pumpkin_cluster::sleep_action::BedRelease;
use pumpkin_data::block_properties::{BedPart, HorizontalFacing, WhiteBedLikeProperties};
use pumpkin_data::tag::Taggable;

pub(crate) fn validate_beds(
    world: WorldId,
    sleeping: PlayerSleepingState,
    beds: &[BedRelease],
) -> Result<(), ApplyFailure> {
    if beds.is_empty() {
        return Ok(());
    }
    if beds.len() != 2 {
        return Err(ApplyFailure::InvalidAction);
    }
    let at = |position: BlockPos| Target::Chunk {
        key: ChunkKey::new(world, position.x.div_euclid(16), position.z.div_euclid(16)),
        part: ChunkPart::Block(position),
        inventory: None,
    };
    let first = beds
        .iter()
        .find(|bed| bed.target == at(sleeping.bed))
        .ok_or(ApplyFailure::InvalidAction)?;
    let state = pumpkin_data::BlockStateId::new(first.before).ok_or(ApplyFailure::InvalidAction)?;
    let block = state.to_block();
    if !block.has_tag(&pumpkin_data::tag::Block::MINECRAFT_BEDS) {
        return Err(ApplyFailure::InvalidAction);
    }
    let properties = WhiteBedLikeProperties::from_state_id(state);
    if !properties.occupied {
        return Err(ApplyFailure::InvalidAction);
    }
    let (dx, dz) = match properties.facing {
        HorizontalFacing::North => (0, -1),
        HorizontalFacing::South => (0, 1),
        HorizontalFacing::West => (-1, 0),
        HorizontalFacing::East => (1, 0),
    };
    let sign = if properties.part == BedPart::Head {
        -1
    } else {
        1
    };
    let other = BlockPos {
        x: sleeping
            .bed
            .x
            .checked_add(dx * sign)
            .ok_or(ApplyFailure::InvalidAction)?,
        y: sleeping.bed.y,
        z: sleeping
            .bed
            .z
            .checked_add(dz * sign)
            .ok_or(ApplyFailure::InvalidAction)?,
    };
    let second = beds
        .iter()
        .find(|bed| bed.target == at(other))
        .ok_or(ApplyFailure::InvalidAction)?;
    let second_state =
        pumpkin_data::BlockStateId::new(second.before).ok_or(ApplyFailure::InvalidAction)?;
    if second_state.to_block() != block {
        return Err(ApplyFailure::InvalidAction);
    }
    let mut second_properties = WhiteBedLikeProperties::from_state_id(second_state);
    if !second_properties.occupied
        || second_properties.facing != properties.facing
        || second_properties.part == properties.part
    {
        return Err(ApplyFailure::InvalidAction);
    }
    let mut properties = properties;
    properties.occupied = false;
    second_properties.occupied = false;
    if first.after != properties.to_state_id(block).as_u16()
        || second.after != second_properties.to_state_id(block).as_u16()
    {
        return Err(ApplyFailure::InvalidAction);
    }
    Ok(())
}

pub(crate) fn rest_stat() -> (u16, u32) {
    (
        pumpkin_data::statistic::StatisticCategory::Custom as u16,
        pumpkin_data::statistic::CustomStatistic::TimeSinceRest as u32,
    )
}

pub(crate) fn capture_claim(
    state: &impl pumpkin_cluster::canonical_action::ActionState,
    lease: pumpkin_cluster::canonical_action::LivePlayerLease,
    chunk: ChunkKey,
    owner_install_generation: u64,
    morning: Option<pumpkin_cluster::world_time::MorningTransition>,
) -> Result<pumpkin_cluster::sleep_action::WakeClaim, ApplyFailure> {
    use pumpkin_cluster::canonical_action::{TruthView, Value};
    let body = Target::Chunk {
        key: chunk,
        part: ChunkPart::Player(lease.gid),
        inventory: None,
    };
    let cell = state
        .read(TruthView::Local, body)
        .ok_or(ApplyFailure::Condition(body))?;
    let Value::Living(living) = cell.value else {
        return Err(ApplyFailure::Type(body));
    };
    let avatar = living.avatar.ok_or(ApplyFailure::Type(body))?;
    let sleeping = avatar
        .presentation
        .sleeping
        .ok_or(ApplyFailure::Condition(body))?;
    let at = |position: BlockPos, entity: bool| Target::Chunk {
        key: ChunkKey::new(
            chunk.world,
            position.x.div_euclid(16),
            position.z.div_euclid(16),
        ),
        part: if entity {
            ChunkPart::BlockEntity(position)
        } else {
            ChunkPart::Block(position)
        },
        inventory: None,
    };
    let read = |position| {
        let target = at(position, false);
        if !state.available(TruthView::Local, target) {
            let Target::Chunk { key, .. } = target else {
                unreachable!()
            };
            return Err(ApplyFailure::MissingStakeholder(
                pumpkin_cluster::canonical_action::Stakeholder::Chunk(key),
            ));
        }
        match state.read(TruthView::Local, target).map(|cell| cell.value) {
            Some(Value::Block(id)) => {
                pumpkin_data::BlockStateId::new(id).ok_or(ApplyFailure::InvalidAction)
            }
            _ => Err(ApplyFailure::Type(target)),
        }
    };
    let first = read(sleeping.bed)?;
    let mut beds = Vec::new();
    if first
        .to_block()
        .has_tag(&pumpkin_data::tag::Block::MINECRAFT_BEDS)
    {
        let properties = WhiteBedLikeProperties::from_state_id(first);
        let (dx, dz) = match properties.facing {
            HorizontalFacing::North => (0, -1),
            HorizontalFacing::South => (0, 1),
            HorizontalFacing::West => (-1, 0),
            HorizontalFacing::East => (1, 0),
        };
        let sign = if properties.part == BedPart::Head {
            -1
        } else {
            1
        };
        let other = BlockPos {
            x: sleeping
                .bed
                .x
                .checked_add(dx * sign)
                .ok_or(ApplyFailure::Capacity)?,
            y: sleeping.bed.y,
            z: sleeping
                .bed
                .z
                .checked_add(dz * sign)
                .ok_or(ApplyFailure::Capacity)?,
        };
        for (position, before) in [(sleeping.bed, first), (other, read(other)?)] {
            let mut properties = WhiteBedLikeProperties::from_state_id(before);
            properties.occupied = false;
            beds.push(BedRelease {
                target: at(position, false),
                before: before.as_u16(),
                after: properties.to_state_id(before.to_block()).as_u16(),
            });
        }
    }
    validate_beds(chunk.world, sleeping, &beds)?;
    let bed_target = at(sleeping.bed, true);
    if !state.available(TruthView::Local, bed_target) {
        let Target::Chunk { key, .. } = bed_target else {
            unreachable!()
        };
        return Err(ApplyFailure::MissingStakeholder(
            pumpkin_cluster::canonical_action::Stakeholder::Chunk(key),
        ));
    }
    Ok(pumpkin_cluster::sleep_action::WakeClaim {
        owner_install_generation,
        source_epoch: avatar.spatial_epoch,
        sleeping,
        bed_entity: state.read(TruthView::Local, bed_target),
        morning,
        beds,
    })
}
