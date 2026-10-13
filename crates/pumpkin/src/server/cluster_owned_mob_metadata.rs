use pumpkin_cluster::item_action::ActorLivingState;
use pumpkin_cluster::mob_continuation::MobSpecies;
use pumpkin_protocol::codec::var_int::VarInt;
use pumpkin_protocol::java::client::play::Metadata;
use pumpkin_protocol::ser::WritingError;
use pumpkin_util::version::JavaMinecraftVersion;

pub(crate) fn write_java(
    living: &ActorLivingState,
    kind: u16,
    version: JavaMinecraftVersion,
    bytes: &mut Vec<u8>,
) -> Result<(), WritingError> {
    let Some(mob) = &living.mob else {
        return Ok(());
    };
    macro_rules! write {
        ($tracked:expr, $value:expr) => {
            if $tracked.id.get(&version) != 255 {
                Metadata::new($tracked, $value).write(&mut *bytes, &version)?;
            }
        };
    }
    let attacking = mob.brain.goals.iter().any(|goal| goal.running
        && matches!(goal.progress, pumpkin_cluster::mob_continuation::MobGoalProgress::Melee { .. }
            | pumpkin_cluster::mob_continuation::MobGoalProgress::Ranged { .. }));
    let flags = u8::from(mob.brain.ai_disabled) | (u8::from(mob.brain.left_handed) << 1)
        | (u8::from(attacking) << 2) | (u8::from(mob.brain.can_pick_up_loot) << 3);
    write!(pumpkin_data::tracked_data::mob::DATA_MOB_FLAGS_ID, flags as i8);
    if ageable_kind(kind) {
        write!(
            pumpkin_data::tracked_data::ageable_mob::DATA_BABY_ID,
            mob.reproduction.growth_age < 0
        );
    }
    match &mob.species {
        MobSpecies::Horse {
            color,
            markings,
            flags,
            chested,
            ..
        } => {
            write!(
                pumpkin_data::tracked_data::abstract_horse::DATA_ID_FLAGS,
                *flags as i8
            );
            if pumpkin_data::entity::EntityType::from_raw(kind)
                .is_some_and(|kind| matches!(kind.resource_name, "donkey" | "mule"))
            {
                write!(
                    pumpkin_data::tracked_data::abstract_chested_horse::DATA_ID_CHEST,
                    *chested
                );
            }
            if pumpkin_data::entity::EntityType::from_raw(kind)
                .is_some_and(|kind| kind.resource_name == "horse")
            {
                write!(
                    pumpkin_data::tracked_data::horse::DATA_ID_TYPE_VARIANT,
                    VarInt(i32::from(*color) | i32::from(*markings) << 8)
                );
            }
        }
        MobSpecies::Llama {
            variant,
            strength,
            flags,
            chested,
            ..
        } => {
            write!(
                pumpkin_data::tracked_data::llama::DATA_VARIANT_ID,
                VarInt(i32::from(*variant))
            );
            write!(
                pumpkin_data::tracked_data::llama::DATA_STRENGTH_ID,
                VarInt(i32::from(*strength))
            );
            write!(
                pumpkin_data::tracked_data::llama::DATA_ID_FLAGS,
                *flags as i8
            );
            write!(pumpkin_data::tracked_data::llama::DATA_ID_CHEST, *chested);
        }
        MobSpecies::Sheep { color, sheared } => write!(
            pumpkin_data::tracked_data::sheep::WOOL_ID,
            (*color | if *sheared { 16 } else { 0 }) as i8
        ),
        MobSpecies::Creeper {
            fuse,
            previous_fuse,
            powered,
            ignited,
            ..
        } => {
            write!(
                pumpkin_data::tracked_data::creeper::FUSE_ID,
                VarInt(if fuse > previous_fuse { 1 } else { -1 })
            );
            write!(pumpkin_data::tracked_data::creeper::CHARGED, *powered);
            write!(pumpkin_data::tracked_data::creeper::IS_IGNITED, *ignited);
        }
        MobSpecies::Slime { size, .. } => {
            let tracked = if pumpkin_data::tracked_data::slime::ID_SIZE.id.get(&version) != 255 {
                pumpkin_data::tracked_data::slime::ID_SIZE
            } else {
                pumpkin_data::tracked_data::slime::SLIME_SIZE
            };
            write!(tracked, VarInt(*size as i32));
        }
        MobSpecies::Enderman {
            carried_block,
            aggressive,
            stared_at,
            ..
        } => {
            write!(
                pumpkin_data::tracked_data::enderman::CARRY_STATE,
                VarInt(carried_block.map_or(0, i32::from))
            );
            write!(pumpkin_data::tracked_data::enderman::CREEPY, *aggressive);
            write!(pumpkin_data::tracked_data::enderman::STARED_AT, *stared_at);
        }
        MobSpecies::Zombie { baby, .. } => write!(pumpkin_data::tracked_data::zombie::BABY, *baby),
        MobSpecies::Bat { resting, .. } => write!(
            pumpkin_data::tracked_data::bat::DATA_ID_FLAGS,
            u8::from(*resting)
        ),
        MobSpecies::Phantom { size, .. } => {
            let tracked = if pumpkin_data::tracked_data::phantom::ID_SIZE
                .id
                .get(&version)
                != 255
            {
                pumpkin_data::tracked_data::phantom::ID_SIZE
            } else {
                pumpkin_data::tracked_data::phantom::SIZE
            };
            write!(tracked, VarInt(*size as i32));
        }
        MobSpecies::Blaze { charged, .. } => write!(
            pumpkin_data::tracked_data::blaze::DATA_FLAGS_ID,
            u8::from(*charged)
        ),
        MobSpecies::Generic | MobSpecies::Skeleton { .. } => {}
    }
    Ok(())
}

pub(crate) fn write_bedrock(
    living: &ActorLivingState,
    kind: u16,
    metadata: &mut pumpkin_protocol::bedrock::client::set_actor_data::SyncedActorDataList,
) {
    use pumpkin_protocol::bedrock::client::set_actor_data::{
        MetadataValue, entity_data_flag as flag, entity_data_key as key,
    };
    let Some(mob) = &living.mob else {
        return;
    };
    metadata.set_flag(key::FLAGS, flag::NO_AI as u8, mob.brain.ai_disabled);
    if ageable_kind(kind) {
        metadata.set_flag(
            key::FLAGS,
            flag::BABY as u8,
            mob.reproduction.growth_age < 0,
        );
    }
    match &mob.species {
        MobSpecies::Horse {
            color,
            markings,
            flags,
            chested,
            ..
        } => {
            metadata.set(key::VARIANT, MetadataValue::Int(i32::from(*color)));
            metadata.set(key::MARK_VARIANT, MetadataValue::Int(i32::from(*markings)));
            metadata.set_flag(key::FLAGS, flag::TAMED as u8, flags & 2 != 0);
            metadata.set_flag(key::FLAGS, flag::SADDLED as u8, flags & 4 != 0);
            metadata.set_flag(key::FLAGS, flag::EATING as u8, flags & 16 != 0);
            metadata.set_flag(key::FLAGS, flag::CHESTED as u8, *chested);
        }
        MobSpecies::Llama {
            variant,
            flags,
            chested,
            ..
        } => {
            metadata.set(key::VARIANT, MetadataValue::Int(i32::from(*variant)));
            metadata.set_flag(key::FLAGS, flag::TAMED as u8, flags & 2 != 0);
            metadata.set_flag(key::FLAGS, flag::CHESTED as u8, *chested);
        }
        MobSpecies::Sheep { color, sheared } => {
            metadata.set(key::COLOR_INDEX, MetadataValue::Byte(*color as i8));
            metadata.set_flag(key::FLAGS, flag::SHEARED as u8, *sheared);
        }
        MobSpecies::Creeper {
            powered, ignited, ..
        } => {
            metadata.set_flag(key::FLAGS, flag::POWERED as u8, *powered);
            metadata.set_flag(key::FLAGS, flag::IGNITED as u8, *ignited);
        }
        MobSpecies::Slime { size, .. } => {
            metadata.set(key::SCALE, MetadataValue::Float(*size as f32))
        }
        MobSpecies::Enderman {
            aggressive,
            carried_block,
            ..
        } => {
            metadata.set_flag(key::FLAGS, flag::ANGRY as u8, *aggressive);
            let runtime = carried_block
                .and_then(pumpkin_data::BlockStateId::new)
                .map(|state| u32::from(pumpkin_data::BlockState::to_be_network_id(state)))
                .unwrap_or(0);
            metadata.set(
                key::CARRY_BLOCK_RUNTIME_ID,
                MetadataValue::Int(runtime as i32),
            );
        }
        MobSpecies::Zombie { baby, .. } => metadata.set_flag(key::FLAGS, flag::BABY as u8, *baby),
        MobSpecies::Bat { resting, .. } => {
            metadata.set_flag(key::FLAGS, flag::RESTING as u8, *resting)
        }
        MobSpecies::Phantom { size, .. } => {
            metadata.set(key::SCALE, MetadataValue::Float(1.0 + *size as f32 * 0.15))
        }
        MobSpecies::Blaze { charged, .. } => {
            metadata.set_flag(key::FLAGS, flag::CHARGED as u8, *charged)
        }
        MobSpecies::Generic | MobSpecies::Skeleton { .. } => {}
    }
}

pub(crate) fn bedrock(
    living: &ActorLivingState,
    kind: u16,
) -> pumpkin_protocol::bedrock::client::set_actor_data::SyncedActorDataList {
    let mut metadata = super::cluster_living_metadata::bedrock_living_presentation(living);
    write_bedrock(living, kind, &mut metadata);
    metadata
}

pub(crate) fn ageable_kind(kind: u16) -> bool {
    pumpkin_data::entity::EntityType::from_raw(kind).is_some_and(|kind| {
        matches!(
            kind.resource_name,
            "sheep"
                | "cow"
                | "mooshroom"
                | "pig"
                | "chicken"
                | "rabbit"
                | "horse"
                | "donkey"
                | "mule"
                | "llama"
                | "trader_llama"
                | "polar_bear"
                | "goat"
                | "axolotl"
                | "frog"
                | "camel"
                | "sniffer"
                | "armadillo"
                | "turtle"
                | "ocelot"
                | "cat"
                | "wolf"
                | "fox"
                | "panda"
                | "bee"
                | "hoglin"
                | "villager"
        )
    })
}

pub(crate) fn dimensions(
    living: &ActorLivingState,
    kind: u16,
) -> Option<pumpkin_util::math::boundingbox::EntityDimensions> {
    use pumpkin_data::attributes::Attributes;
    let base = living.combat.effective_attributes.get(&u16::from(Attributes::SCALE.id))
        .copied().map(f64::from_bits).unwrap_or(Attributes::SCALE.default_value);
    dimensions_parts(kind, living.attribute(u16::from(Attributes::SCALE.id), base),
        living.mob.as_ref().map(|mob| &mob.species),
        living.mob.as_ref().map(|mob| &mob.reproduction),
        living.stand.as_ref().map(|stand| stand.flags))
}

pub(crate) fn dimensions_parts(
    kind: u16,
    scale: f64,
    species: Option<&MobSpecies>,
    reproduction: Option<&pumpkin_cluster::mob_continuation::MobReproduction>,
    stand_flags: Option<u8>,
) -> Option<pumpkin_util::math::boundingbox::EntityDimensions> {
    let kind = pumpkin_data::entity::EntityType::from_raw(kind)?;
    if !scale.is_finite() || scale <= 0.0 || species.is_some() != reproduction.is_some() { return None; }
    let species_scale = species.map_or(1.0, |species| match species {
        MobSpecies::Slime { size, .. } => f64::from(*size),
        MobSpecies::Phantom { size, .. } => 1.0 + f64::from(*size) * 0.15,
        MobSpecies::Zombie { baby: true, .. } => 0.5,
        _ if ageable_kind(kind.id) && reproduction.is_some_and(|reproduction| reproduction.growth_age < 0) => 0.5,
        _ => 1.0,
    });
    let stand_scale = stand_flags.map_or(1.0, |flags| {
        if flags & 0x10 != 0 { 0.0 } else if flags & 1 != 0 { 0.5 } else { 1.0 }
    });
    let factor = (scale.clamp(0.0625, 16.0) * species_scale * stand_scale) as f32;
    Some(pumpkin_util::math::boundingbox::EntityDimensions::new(
        kind.dimension[0] * factor, kind.dimension[1] * factor, kind.eye_height * factor))
}

pub(crate) fn same_visible_state(before: &ActorLivingState, after: &ActorLivingState) -> bool {
    if grazing(before) != grazing(after) {
        return false;
    }
    let (Some(before), Some(after)) = (&before.mob, &after.mob) else {
        return before.mob.is_none() && after.mob.is_none();
    };
    if (before.reproduction.growth_age < 0) != (after.reproduction.growth_age < 0) {
        return false;
    }
    match (&before.species, &after.species) {
        (
            MobSpecies::Horse {
                color: a,
                markings: b,
                flags: c,
                chested: d,
                ..
            },
            MobSpecies::Horse {
                color: x,
                markings: y,
                flags: z,
                chested: w,
                ..
            },
        ) => (a, b, c, d) == (x, y, z, w),
        (
            MobSpecies::Llama {
                variant: a,
                strength: b,
                carpet_color: c,
                flags: d,
                chested: e,
                ..
            },
            MobSpecies::Llama {
                variant: x,
                strength: y,
                carpet_color: z,
                flags: w,
                chested: v,
                ..
            },
        ) => (a, b, c, d, e) == (x, y, z, w, v),
        (
            MobSpecies::Sheep {
                color: a,
                sheared: b,
            },
            MobSpecies::Sheep {
                color: x,
                sheared: y,
            },
        ) => (a, b) == (x, y),
        (
            MobSpecies::Creeper {
                fuse: a,
                previous_fuse: b,
                powered: c,
                ignited: d,
                ..
            },
            MobSpecies::Creeper {
                fuse: x,
                previous_fuse: y,
                powered: z,
                ignited: w,
                ..
            },
        ) => (a > b, c, d) == (x > y, z, w),
        (MobSpecies::Slime { size: a, .. }, MobSpecies::Slime { size: b, .. })
        | (MobSpecies::Phantom { size: a, .. }, MobSpecies::Phantom { size: b, .. }) => a == b,
        (
            MobSpecies::Enderman {
                carried_block: a,
                aggressive: b,
                stared_at: c,
                ..
            },
            MobSpecies::Enderman {
                carried_block: x,
                aggressive: y,
                stared_at: z,
                ..
            },
        ) => (a, b, c) == (x, y, z),
        (MobSpecies::Zombie { baby: a, .. }, MobSpecies::Zombie { baby: b, .. }) => a == b,
        (MobSpecies::Bat { resting: a, .. }, MobSpecies::Bat { resting: b, .. }) => a == b,
        (MobSpecies::Blaze { charged: a, .. }, MobSpecies::Blaze { charged: b, .. }) => a == b,
        (MobSpecies::Generic, MobSpecies::Generic)
        | (MobSpecies::Skeleton { .. }, MobSpecies::Skeleton { .. }) => true,
        _ => false,
    }
}

pub(crate) fn started_grazing(before: &ActorLivingState, after: &ActorLivingState) -> bool {
    !grazing(before) && grazing(after)
}

fn grazing(living: &ActorLivingState) -> bool {
    living.mob.as_ref().is_some_and(|mob| matches!(mob.species, MobSpecies::Sheep { .. }) && mob.brain.goals.iter().any(|goal|
        goal.running && matches!(goal.progress, pumpkin_cluster::mob_continuation::MobGoalProgress::Graze { remaining } if remaining > 0)))
}

pub(crate) fn species_matches_kind(species: &MobSpecies, kind: u16) -> bool {
    let Some(kind) = pumpkin_data::entity::EntityType::from_raw(kind) else {
        return false;
    };
    if !kind.mob {
        return false;
    }
    match species {
        MobSpecies::Horse { kind: horse, .. } => {
            kind.resource_name
                == match horse {
                    pumpkin_cluster::mob_continuation::HorseSpeciesKind::Horse => "horse",
                    pumpkin_cluster::mob_continuation::HorseSpeciesKind::Donkey => "donkey",
                    pumpkin_cluster::mob_continuation::HorseSpeciesKind::Mule => "mule",
                    pumpkin_cluster::mob_continuation::HorseSpeciesKind::Skeleton => {
                        "skeleton_horse"
                    }
                    pumpkin_cluster::mob_continuation::HorseSpeciesKind::Zombie => "zombie_horse",
                }
        }
        MobSpecies::Llama { trader, .. } => {
            kind.resource_name == if *trader { "trader_llama" } else { "llama" }
        }
        MobSpecies::Sheep { .. } => kind.resource_name == "sheep",
        MobSpecies::Creeper { .. } => kind.resource_name == "creeper",
        MobSpecies::Slime { .. } => matches!(kind.resource_name, "slime" | "magma_cube"),
        MobSpecies::Enderman { .. } => kind.resource_name == "enderman",
        MobSpecies::Zombie { .. } => matches!(
            kind.resource_name,
            "zombie" | "husk" | "drowned" | "zombie_villager" | "zombified_piglin"
        ),
        MobSpecies::Skeleton { .. } => matches!(
            kind.resource_name,
            "skeleton" | "stray" | "bogged" | "parched" | "wither_skeleton"
        ),
        MobSpecies::Bat { .. } => kind.resource_name == "bat",
        MobSpecies::Phantom { .. } => kind.resource_name == "phantom",
        MobSpecies::Blaze { .. } => kind.resource_name == "blaze",
        MobSpecies::Generic => !matches!(
            kind.resource_name,
            "horse"
                | "donkey"
                | "mule"
                | "skeleton_horse"
                | "zombie_horse"
                | "llama"
                | "trader_llama"
                | "sheep"
                | "creeper"
                | "slime"
                | "magma_cube"
                | "enderman"
                | "zombie"
                | "husk"
                | "drowned"
                | "zombie_villager"
                | "zombified_piglin"
                | "skeleton"
                | "stray"
                | "bogged"
                | "parched"
                | "wither_skeleton"
                | "bat"
                | "phantom"
                | "blaze"
        ),
    }
}
