use pumpkin_cluster::canonical_action::{ApplyFailure, ChunkPart, Target, Value};
use pumpkin_cluster::protocol::{BlockPos, ChunkKey};
use pumpkin_world::chunk::ChunkHeightmapType;

pub(super) fn light(
    snapshot: &super::cluster_world_callback_snapshot::CallbackTickSnapshot,
    chunk: ChunkKey,
    position: BlockPos,
) -> Result<(u8, u8), ApplyFailure> {
    if position.x.div_euclid(16) != chunk.position.x || position.z.div_euclid(16) != chunk.position.z {
        return Err(ApplyFailure::InvalidAction);
    }
    let target = Target::Chunk { key: chunk, part: ChunkPart::LightSection(position.y.div_euclid(16)), inventory: None };
    let cell = snapshot.original_store().light_section(false, chunk, position.y.div_euclid(16))
        .ok_or(ApplyFailure::MissingStakeholder(target.stakeholder()))?;
    let Value::LightSection { sky, block } = &cell.value else { return Err(ApplyFailure::Type(target)); };
    if sky.len() != 2048 || block.len() != 2048 { return Err(ApplyFailure::Type(target)); }
    let index = ((position.y.rem_euclid(16) << 8) | (position.z.rem_euclid(16) << 4) | position.x.rem_euclid(16)) as usize;
    Ok(((sky[index / 2] >> ((index % 2) * 4)) & 15, (block[index / 2] >> ((index % 2) * 4)) & 15))
}

pub(super) fn column_height(
    snapshot: &super::cluster_world_callback_snapshot::CallbackTickSnapshot,
    chunk: ChunkKey,
    x: i32,
    z: i32,
    kind: ChunkHeightmapType,
) -> Result<i32, ApplyFailure> {
    if x.div_euclid(16) != chunk.position.x || z.div_euclid(16) != chunk.position.z { return Err(ApplyFailure::InvalidAction); }
    let palettes = snapshot.original_palettes(chunk)
        .ok_or(ApplyFailure::MissingStakeholder(pumpkin_cluster::canonical_action::Stakeholder::Chunk(chunk)))?;
    let rules = snapshot.mob_rules(chunk.world).ok_or(ApplyFailure::MissingStakeholder(
        pumpkin_cluster::canonical_action::Stakeholder::Chunk(chunk)))?;
    column_height_palettes(&palettes, chunk, rules.minimum_y, rules.maximum_y, x, z, kind)
}

pub(crate) fn column_height_palettes(
    palettes: &super::cluster_chunk_state::OwnedChunkBlocks,
    chunk: ChunkKey, minimum_y: i32, maximum_y: i32, x: i32, z: i32, kind: ChunkHeightmapType,
) -> Result<i32, ApplyFailure> {
    if x.div_euclid(16) != chunk.position.x || z.div_euclid(16) != chunk.position.z { return Err(ApplyFailure::InvalidAction); }
    let expected = maximum_y.checked_sub(minimum_y).and_then(|height| height.checked_add(1))
        .filter(|height| *height > 0 && height.rem_euclid(16) == 0).ok_or(ApplyFailure::InvalidAction)?;
    if palettes.min_y() != minimum_y || palettes.min_y().rem_euclid(16) != 0
        || palettes.block_palettes().len() != usize::try_from(expected / 16).map_err(|_| ApplyFailure::InvalidAction)? {
        return Err(ApplyFailure::MissingStakeholder(pumpkin_cluster::canonical_action::Stakeholder::Chunk(chunk)));
    }
    let local_x = x.rem_euclid(16) as usize;
    let local_z = z.rem_euclid(16) as usize;
    for (section, palette) in palettes.block_palettes().iter().enumerate().rev() {
        for local_y in (0..16).rev() {
            let state = palette.get(local_x, local_y, local_z);
            if kind.is_opaque(state.to_state()) {
                let offset = i32::try_from(section).ok().and_then(|section| section.checked_mul(16))
                    .and_then(|offset| offset.checked_add(local_y as i32 + 1)).ok_or(ApplyFailure::InvalidAction)?;
                return palettes.min_y().checked_add(offset).ok_or(ApplyFailure::InvalidAction);
            }
        }
    }
    Ok(palettes.min_y())
}

pub(super) fn biome(
    snapshot: &super::cluster_world_callback_snapshot::CallbackTickSnapshot,
    chunk: ChunkKey,
    position: BlockPos,
) -> Result<u8, ApplyFailure> {
    if position.x.div_euclid(16) != chunk.position.x || position.z.div_euclid(16) != chunk.position.z {
        return Err(ApplyFailure::InvalidAction);
    }
    snapshot.original_palettes(chunk).and_then(|palettes| palettes.rough_biome_id(position))
        .ok_or(ApplyFailure::MissingStakeholder(pumpkin_cluster::canonical_action::Stakeholder::Chunk(chunk)))
}

pub(super) fn placement<S: pumpkin_cluster::canonical_action::ActionState>(
    world: &super::cluster_owned_mob_world::MobWorldView<'_, S>,
    position: BlockPos,
    kind: &'static pumpkin_data::entity::EntityType,
    inside_border: bool,
) -> Result<bool, ApplyFailure> {
    super::cluster_owned_mob_spawn::spawn_position_ok(kind, position, inside_border, |at|
        world.block(at).map(|state| state.to_state()).map_err(Into::into))
}

pub(super) fn brightness(
    snapshot: &super::cluster_world_callback_snapshot::CallbackTickSnapshot,
    chunk: ChunkKey,
    position: BlockPos,
    dimension: &pumpkin_data::dimension::Dimension,
    environment: &super::cluster_environment::EnvironmentView,
) -> Result<u8, ApplyFailure> {
    let (sky, block) = light(snapshot, chunk, position)?;
    Ok(sky.saturating_sub(super::cluster_environment::sky_darkening(environment, dimension) as u8).max(block))
}

pub(super) fn monster_darkness(
    sky: u8,
    block: u8,
    dimension: &pumpkin_data::dimension::Dimension,
    thundering: bool,
    random: &mut pumpkin_util::random::RandomGenerator,
) -> bool {
    use pumpkin_util::random::RandomImpl;
    if sky > 15 || block > 15 || i32::from(sky) > random.next_bounded_i32(32) { return false; }
    if dimension.monster_spawn_block_light_limit < 15 && block > dimension.monster_spawn_block_light_limit { return false; }
    let brightness = if thundering { sky.saturating_sub(10).max(block) } else { sky.max(block) };
    i32::from(brightness) <= dimension.monster_spawn_light_level.get(random)
}
