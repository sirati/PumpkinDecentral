use super::cluster_chunk_state::OwnedChunkStore;
use super::cluster_world_callbacks::CallbackCaptureError;
use pumpkin_cluster::canonical_action::{AppliedWrite, ChunkPart, Target, TruthView, Value};
use pumpkin_cluster::protocol::ChunkKey;
use pumpkin_config::lighting::LightingEngineConfig;
use pumpkin_data::BlockStateId;
use pumpkin_world::chunk::palette::BlockPalette;
use pumpkin_world::lighting::owned::OwnedChunkLighting;
use std::collections::{BTreeMap, BTreeSet};

pub(super) fn derived_light_changes(
    view: TruthView,
    blocks: &OwnedChunkStore,
    writes: &[AppliedWrite],
    config: LightingEngineConfig,
    has_skylight: bool,
) -> Result<BTreeMap<Target, Value>, CallbackCaptureError> {
    let ground = view == TruthView::Ground;
    let mut centers = BTreeSet::new();
    let mut palettes: BTreeMap<ChunkKey, (i32, Box<[BlockPalette]>)> = BTreeMap::new();
    for write in writes {
        let Target::Chunk {
            key,
            part: ChunkPart::Block(position),
            inventory: None,
        } = write.target
        else {
            continue;
        };
        let (Some(before), Some(after)) = (&write.before, &write.after) else {
            return Err(CallbackCaptureError::InvalidWrite(write.target));
        };
        let (Value::Block(old), Value::Block(new)) = (&before.value, &after.value) else {
            return Err(CallbackCaptureError::InvalidWrite(write.target));
        };
        let (Some(old), Some(new)) = (BlockStateId::new(*old), BlockStateId::new(*new)) else {
            return Err(CallbackCaptureError::InvalidWrite(write.target));
        };
        let current = if ground {
            blocks.ground_block(key, position)
        } else {
            blocks.block(key, position)
        }
        .map_err(|_| CallbackCaptureError::MissingChunk(key))?;
        if current != old {
            return Err(CallbackCaptureError::InvalidWrite(write.target));
        }
        if !palettes.contains_key(&key) {
            let snapshot = if ground {
                blocks.ground_snapshot(key)
            } else {
                blocks.snapshot(key)
            }
            .ok_or(CallbackCaptureError::MissingChunk(key))?;
            palettes.insert(
                key,
                (
                    snapshot.min_y(),
                    snapshot.block_palettes().to_vec().into_boxed_slice(),
                ),
            );
        }
        let (min_y, sections) = palettes.get_mut(&key).expect("captured block palette");
        let y = position
            .y
            .checked_sub(*min_y)
            .ok_or(CallbackCaptureError::InvalidWrite(write.target))?;
        let section = usize::try_from(y.div_euclid(16))
            .ok()
            .and_then(|index| sections.get_mut(index))
            .ok_or(CallbackCaptureError::InvalidWrite(write.target))?;
        section.set(
            position.x.rem_euclid(16) as usize,
            y.rem_euclid(16) as usize,
            position.z.rem_euclid(16) as usize,
            new,
        );
        if pumpkin_world::lighting::LightEngine::has_different_light_properties(
            old.to_state(),
            new.to_state(),
        ) {
            for x in -1..=1 {
                for z in -1..=1 {
                    let Some(cx) = key.x().checked_add(x) else {
                        return Err(CallbackCaptureError::Budget);
                    };
                    let Some(cz) = key.z().checked_add(z) else {
                        return Err(CallbackCaptureError::Budget);
                    };
                    centers.insert(ChunkKey::new(key.world, cx, cz));
                }
            }
        }
    }
    if centers.len() > 64 {
        return Err(CallbackCaptureError::Budget);
    }
    let mut result = BTreeMap::new();
    for center in centers {
        let mut halo = Vec::with_capacity(9);
        for z in -1..=1 {
            for x in -1..=1 {
                let key = ChunkKey::new(
                    center.world,
                    center
                        .x()
                        .checked_add(x)
                        .ok_or(CallbackCaptureError::Budget)?,
                    center
                        .z()
                        .checked_add(z)
                        .ok_or(CallbackCaptureError::Budget)?,
                );
                if !palettes.contains_key(&key) {
                    let snapshot = if ground {
                        blocks.ground_snapshot(key)
                    } else {
                        blocks.snapshot(key)
                    }
                    .ok_or(CallbackCaptureError::MissingChunk(key))?;
                    palettes.insert(
                        key,
                        (
                            snapshot.min_y(),
                            snapshot.block_palettes().to_vec().into_boxed_slice(),
                        ),
                    );
                }
                halo.push(key);
            }
        }
        let min_y = palettes[&center].0;
        if halo.iter().any(|key| palettes[key].0 != min_y) {
            return Err(CallbackCaptureError::Budget);
        }
        let cut = std::array::from_fn(|index| palettes[&halo[index]].1.as_ref());
        let lighting = OwnedChunkLighting::resolve(cut, config, has_skylight, 32_000_000)
            .map_err(|_| CallbackCaptureError::Budget)?;
        for (index, (sky, block)) in lighting.sky.iter().zip(&lighting.block).enumerate() {
            let section_y = min_y
                .div_euclid(16)
                .checked_add(i32::try_from(index).map_err(|_| CallbackCaptureError::Budget)?)
                .ok_or(CallbackCaptureError::Budget)?;
            let target = Target::Chunk {
                key: center,
                part: ChunkPart::LightSection(section_y),
                inventory: None,
            };
            let before = blocks
                .light_section(ground, center, section_y)
                .ok_or(CallbackCaptureError::MissingLight(target))?;
            let after = Value::LightSection {
                sky: sky.as_slice().to_vec(),
                block: block.as_slice().to_vec(),
            };
            if before.value != after {
                result.insert(target, after);
            }
        }
    }
    Ok(result)
}

pub(super) fn validate_derived_light_writes(
    view: TruthView,
    blocks: &OwnedChunkStore,
    writes: &[AppliedWrite],
    config: LightingEngineConfig,
    has_skylight: bool,
) -> Result<(), CallbackCaptureError> {
    let mut required = derived_light_changes(view, blocks, writes, config, has_skylight)?;
    for write in writes {
        let Target::Chunk {
            key,
            part: ChunkPart::LightSection(section_y),
            inventory: None,
        } = write.target
        else {
            continue;
        };
        let before = blocks
            .light_section(view == TruthView::Ground, key, section_y)
            .ok_or(CallbackCaptureError::MissingLight(write.target))?;
        if write.before.as_ref() != Some(before)
            || write.after.as_ref().map(|cell| &cell.value)
                != required.remove(&write.target).as_ref()
        {
            return Err(CallbackCaptureError::InvalidWrite(write.target));
        }
    }
    if let Some(target) = required.keys().next().copied() {
        return Err(CallbackCaptureError::InvalidWrite(target));
    }
    Ok(())
}

pub(super) fn derive_section_light(
    center: ChunkKey,
    min_section_y: i32,
    section_count: usize,
    values: &BTreeMap<Target, pumpkin_cluster::canonical_action::Cell>,
    config: LightingEngineConfig,
    has_skylight: bool,
    work_budget: usize,
) -> Result<BTreeMap<Target, Value>, CallbackCaptureError> {
    if section_count == 0 || section_count > 256 || work_budget == 0 {
        return Err(CallbackCaptureError::Budget);
    }
    let mut halo = Vec::with_capacity(9);
    for z in -1..=1 {
        for x in -1..=1 {
            let key = ChunkKey::new(
                center.world,
                center
                    .x()
                    .checked_add(x)
                    .ok_or(CallbackCaptureError::Budget)?,
                center
                    .z()
                    .checked_add(z)
                    .ok_or(CallbackCaptureError::Budget)?,
            );
            let mut sections = Vec::with_capacity(section_count);
            for section in 0..section_count {
                let y = min_section_y
                    .checked_add(i32::try_from(section).map_err(|_| CallbackCaptureError::Budget)?)
                    .ok_or(CallbackCaptureError::Budget)?;
                let target = Target::Chunk {
                    key,
                    part: ChunkPart::BlockSection(y),
                    inventory: None,
                };
                let Some(pumpkin_cluster::canonical_action::Cell {
                    value: Value::BlockSection(states),
                    ..
                }) = values.get(&target)
                else {
                    return Err(CallbackCaptureError::InvalidWrite(target));
                };
                if states.len() != 4096 {
                    return Err(CallbackCaptureError::InvalidWrite(target));
                }
                let first = BlockStateId::new(states[0])
                    .ok_or(CallbackCaptureError::InvalidWrite(target))?;
                let mut palette = BlockPalette::Homogeneous(first);
                for (index, &state) in states.iter().enumerate() {
                    let state = BlockStateId::new(state)
                        .ok_or(CallbackCaptureError::InvalidWrite(target))?;
                    palette.set(index & 15, index >> 8, (index >> 4) & 15, state);
                }
                sections.push(palette);
            }
            halo.push(sections.into_boxed_slice());
        }
    }
    let slices = std::array::from_fn(|index| halo[index].as_ref());
    let light = OwnedChunkLighting::resolve(slices, config, has_skylight, work_budget)
        .map_err(|_| CallbackCaptureError::Budget)?;
    let mut result = BTreeMap::new();
    for (section, (sky, block)) in light.sky.iter().zip(&light.block).enumerate() {
        let y = min_section_y
            .checked_add(i32::try_from(section).map_err(|_| CallbackCaptureError::Budget)?)
            .ok_or(CallbackCaptureError::Budget)?;
        result.insert(
            Target::Chunk {
                key: center,
                part: ChunkPart::LightSection(y),
                inventory: None,
            },
            Value::LightSection {
                sky: sky.as_slice().to_vec(),
                block: block.as_slice().to_vec(),
            },
        );
    }
    Ok(result)
}

pub(super) fn derive_symbolic_light(
    server: &super::Server,
    plan: pumpkin_cluster::world_action::WorldLightPlan,
    values: &BTreeMap<Target, pumpkin_cluster::canonical_action::Cell>,
) -> Result<
    Vec<pumpkin_cluster::canonical_action::Effect>,
    pumpkin_cluster::canonical_action::ApplyFailure,
> {
    use pumpkin_cluster::canonical_action::{ApplyFailure, Effect};
    use pumpkin_cluster::world_action::WorldLightMode;
    let worlds = server.worlds.load_full();
    let world = worlds
        .iter()
        .find(|world| {
            world.level.cluster_world_key == plan.center.world.key
                && world.level.cluster_dimension == plan.center.world.dimension
        })
        .ok_or(ApplyFailure::InvalidAction)?;
    let mode = match world.level.lighting_config {
        LightingEngineConfig::Default => WorldLightMode::Default,
        LightingEngineConfig::Full => WorldLightMode::Full,
        LightingEngineConfig::Dark => WorldLightMode::Dark,
    };
    if plan.config_version != plan.config_fingerprint()
        || plan.mode != mode
        || plan.has_skylight != world.dimension.has_skylight
        || plan.min_section_y != world.dimension.min_y.div_euclid(16)
        || usize::from(plan.section_count) != world.dimension.height as usize / 16
    {
        return Err(ApplyFailure::InvalidAction);
    }
    let derived = derive_section_light(
        plan.center,
        plan.min_section_y,
        usize::from(plan.section_count),
        values,
        world.level.lighting_config,
        plan.has_skylight,
        32_000_000,
    )
    .map_err(|_| ApplyFailure::InvalidAction)?;
    let mut effects = Vec::new();
    for (target, value) in derived {
        let before = values.get(&target).ok_or(ApplyFailure::Condition(target))?;
        if !matches!(&before.value, Value::LightSection { sky, block } if sky.len() == 2048 && block.len() == 2048)
        {
            return Err(ApplyFailure::Type(target));
        }
        if before.value != value {
            effects.push(Effect::Put { target, value });
        }
    }
    Ok(effects)
}

pub(super) fn location_light_plans(
    server: &super::Server,
    location: pumpkin_cluster::world_command::BlockLocation,
) -> Result<
    Vec<pumpkin_cluster::world_action::WorldLightPlan>,
    pumpkin_cluster::canonical_action::ApplyFailure,
> {
    use pumpkin_cluster::canonical_action::ApplyFailure;
    if !location.valid() {
        return Err(ApplyFailure::InvalidAction);
    }
    let world = server
        .worlds
        .load()
        .iter()
        .find(|world| super::cluster_world_delta::world_id(world) == location.world)
        .cloned()
        .ok_or(ApplyFailure::InvalidAction)?;
    let mode = match world.level.lighting_config {
        pumpkin_config::lighting::LightingEngineConfig::Default => {
            pumpkin_cluster::world_action::WorldLightMode::Default
        }
        pumpkin_config::lighting::LightingEngineConfig::Full => {
            pumpkin_cluster::world_action::WorldLightMode::Full
        }
        pumpkin_config::lighting::LightingEngineConfig::Dark => {
            pumpkin_cluster::world_action::WorldLightMode::Dark
        }
    };
    let center = location.chunk();
    let mut plans = Vec::with_capacity(9);
    for z in -1..=1 {
        for x in -1..=1 {
            plans.push(
                pumpkin_cluster::world_action::WorldLightPlan {
                    center: pumpkin_cluster::protocol::ChunkKey::new(
                        center.world,
                        center
                            .x()
                            .checked_add(x)
                            .ok_or(ApplyFailure::InvalidAction)?,
                        center
                            .z()
                            .checked_add(z)
                            .ok_or(ApplyFailure::InvalidAction)?,
                    ),
                    min_section_y: world.dimension.min_y.div_euclid(16),
                    section_count: (world.dimension.height / 16)
                        .try_into()
                        .map_err(|_| ApplyFailure::InvalidAction)?,
                    has_skylight: world.dimension.has_skylight,
                    mode,
                    config_version: 1,
                }
                .with_config_fingerprint(),
            );
        }
    }
    Ok(plans)
}
