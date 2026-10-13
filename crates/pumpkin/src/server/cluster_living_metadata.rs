pub(super) use super::cluster_living_attributes::{
    player_attributes_presentation_equal, prepare_player_attributes, publish_player_attributes,
};
use pumpkin_cluster::item_action::ActorLivingState;
use pumpkin_data::data_component_impl::IDSetContent;
use pumpkin_data::entity::EntityPose;
use pumpkin_protocol::codec::var_int::VarInt;
use pumpkin_protocol::java::client::play::Metadata;
use pumpkin_protocol::ser::WritingError;
use pumpkin_util::math::position::BlockPos;
use pumpkin_util::math::vector3::Vector3;
use pumpkin_util::version::JavaMinecraftVersion;

pub(super) fn publish_player_effect_delta(
    client: &crate::net::ClientPlatform,
    entity_id: i32,
    before: &ActorLivingState,
    after: &ActorLivingState,
    tick: pumpkin_cluster::time::TickStamp,
) -> bool {
    let Ok(packets) = prepare_player_effect_delta(client, entity_id, Some(before), after, tick)
    else {
        return false;
    };
    for packet in packets {
        client.try_enqueue_packet(packet);
    }
    true
}

pub(super) fn prepare_player_effect_delta(
    client: &crate::net::ClientPlatform,
    entity_id: i32,
    before: Option<&ActorLivingState>,
    after: &ActorLivingState,
    tick: pumpkin_cluster::time::TickStamp,
) -> Result<Vec<bytes::Bytes>, String> {
    use pumpkin_protocol::bedrock::client::CMobEffect;
    use pumpkin_protocol::codec::var_ulong::VarULong;
    use pumpkin_protocol::java::client::play::{CRemoveMobEffect, CUpdateMobEffect};
    if before.map_or(0, |state| state.effects.len()) > 256 || after.effects.len() > 256 {
        return Err("player effect count exceeds limit".to_owned());
    }
    let old: std::collections::BTreeMap<_, _> = before
        .map_or(&[][..], |state| state.effects.as_slice())
        .iter()
        .filter(|effect| effect.duration != 0)
        .map(|effect| (effect.kind, effect))
        .collect();
    let new: std::collections::BTreeMap<_, _> = after
        .effects
        .iter()
        .filter(|effect| effect.duration != 0)
        .map(|effect| (effect.kind, effect))
        .collect();
    if old.len()
        != before.map_or(0, |state| {
            state
                .effects
                .iter()
                .filter(|effect| effect.duration != 0)
                .count()
        })
        || new.len()
            != after
                .effects
                .iter()
                .filter(|effect| effect.duration != 0)
                .count()
    {
        return Err("duplicate player effect".to_owned());
    }
    let elapsed = match (
        before.and_then(|state| state.simulation_tick),
        after.simulation_tick,
    ) {
        (Some(before), Some(after)) if after == before || after.is_after(before) => {
            after.distance_since(before)
        }
        _ => 0,
    };
    let mut packets = Vec::with_capacity(old.len() + new.len());
    for (&kind, effect) in &old {
        if new.contains_key(&kind) {
            continue;
        }
        let Some(effect_type) = pumpkin_data::effect::StatusEffect::from_id(kind) else {
            return Err("unknown player effect".to_owned());
        };
        let encoded = match client {
            crate::net::ClientPlatform::Java(client) => client
                .serialize_packet(&CRemoveMobEffect::new(
                    entity_id.into(),
                    VarInt(i32::from(kind)),
                ))
                .map_err(|error| error.to_string()),
            crate::net::ClientPlatform::Bedrock(client) => client
                .serialize_packet(&CMobEffect {
                    target_runtime_id: VarULong(entity_id as u64),
                    event_id: CMobEffect::EVENT_REMOVE,
                    effect_id: VarInt(effect_type.to_bedrock_id()),
                    effect_amplifier: VarInt(i32::from(effect.amplifier)),
                    show_particles: effect.show_particles,
                    effect_duration_ticks: VarInt(0),
                    tick: VarULong(u64::from(tick.0)),
                    ambient: effect.ambient,
                })
                .map_err(|error| error.to_string()),
        };
        packets.push(encoded?);
    }
    for (&kind, effect) in &new {
        if effect.duration < -1 {
            return Err("invalid player effect duration".to_owned());
        }
        let Some(effect_type) = pumpkin_data::effect::StatusEffect::from_id(kind) else {
            return Err("unknown player effect".to_owned());
        };
        let unchanged = old.get(&kind).is_some_and(|previous| {
            let duration = if previous.duration < 0 {
                previous.duration
            } else {
                previous.duration.saturating_sub(i32::from(elapsed))
            };
            duration == effect.duration
                && previous.amplifier == effect.amplifier
                && previous.ambient == effect.ambient
                && previous.show_particles == effect.show_particles
                && previous.show_icon == effect.show_icon
                && previous.blend == effect.blend
        });
        if unchanged {
            continue;
        }
        let flags = i8::from(effect.ambient)
            | (i8::from(effect.show_particles) << 1)
            | (i8::from(effect.show_icon) << 2)
            | (i8::from(effect.blend) << 3);
        let encoded = match client {
            crate::net::ClientPlatform::Java(client) => client
                .serialize_packet(&CUpdateMobEffect::new(
                    entity_id.into(),
                    VarInt(i32::from(kind)),
                    VarInt(i32::from(effect.amplifier)),
                    VarInt(effect.duration),
                    flags,
                ))
                .map_err(|error| error.to_string()),
            crate::net::ClientPlatform::Bedrock(client) => client
                .serialize_packet(&CMobEffect {
                    target_runtime_id: VarULong(entity_id as u64),
                    event_id: if old.contains_key(&kind) {
                        CMobEffect::EVENT_MODIFY
                    } else {
                        CMobEffect::EVENT_ADD
                    },
                    effect_id: VarInt(effect_type.to_bedrock_id()),
                    effect_amplifier: VarInt(i32::from(effect.amplifier)),
                    show_particles: effect.show_particles,
                    effect_duration_ticks: VarInt(effect.duration),
                    tick: VarULong(u64::from(tick.0)),
                    ambient: effect.ambient,
                })
                .map_err(|error| error.to_string()),
        };
        packets.push(encoded?);
    }
    Ok(packets)
}

pub(super) fn player_effects_presentation_equal(
    before: &ActorLivingState,
    after: &ActorLivingState,
) -> bool {
    let elapsed = match (before.simulation_tick, after.simulation_tick) {
        (Some(before), Some(after)) if after == before || after.is_after(before) => {
            after.distance_since(before)
        }
        _ => 0,
    };
    let old_count = before
        .effects
        .iter()
        .filter(|effect| effect.duration != 0)
        .count();
    let new_count = after
        .effects
        .iter()
        .filter(|effect| effect.duration != 0)
        .count();
    old_count == new_count
        && after
            .effects
            .iter()
            .filter(|effect| effect.duration != 0)
            .all(|effect| {
                before
                    .effects
                    .iter()
                    .find(|previous| previous.kind == effect.kind && previous.duration != 0)
                    .is_some_and(|previous| {
                        let duration = if previous.duration < 0 {
                            previous.duration
                        } else {
                            previous.duration.saturating_sub(i32::from(elapsed))
                        };
                        duration == effect.duration
                            && previous.amplifier == effect.amplifier
                            && previous.ambient == effect.ambient
                            && previous.show_particles == effect.show_particles
                            && previous.show_icon == effect.show_icon
                            && previous.blend == effect.blend
                    })
            })
}

struct EffectParticles(Vec<i32>);

fn effects_ambient(living: &ActorLivingState) -> bool {
    let mut effects = living
        .effects
        .iter()
        .filter(|effect| effect.show_particles && effect.duration != 0)
        .peekable();
    effects.peek().is_some() && effects.all(|effect| effect.ambient)
}

impl pumpkin_protocol::java::client::play::MetadataSerializer for EffectParticles {
    fn write_metadata(
        &self,
        writer: &mut impl std::io::Write,
        _version: &JavaMinecraftVersion,
    ) -> Result<(), WritingError> {
        use pumpkin_protocol::ser::NetworkWriteExt;
        writer.write_var_int(&VarInt(self.0.len() as i32))?;
        for color in &self.0 {
            writer.write_var_int(&VarInt(
                pumpkin_data::particle::Particle::EntityEffect as i32,
            ))?;
            writer.write_i32_be(*color)?;
        }
        Ok(())
    }
}

fn visible_effect_colors(living: &ActorLivingState) -> Vec<(u32, u32)> {
    living
        .effects
        .iter()
        .filter(|effect| effect.show_particles && effect.duration != 0)
        .filter_map(|effect| {
            pumpkin_data::effect::StatusEffect::from_id(effect.kind)
                .map(|kind| (kind.color as u32, u32::from(effect.amplifier) + 1))
        })
        .collect()
}

fn write_java_status_effects(
    living: &ActorLivingState,
    version: JavaMinecraftVersion,
    bytes: &mut Vec<u8>,
) -> Result<(), WritingError> {
    let particles = pumpkin_data::tracked_data::living_entity::DATA_EFFECT_PARTICLES;
    if particles.id.get(&version) != 255 {
        Metadata::new(
            particles,
            EffectParticles(
                visible_effect_colors(living)
                    .into_iter()
                    .map(|(color, _)| (color | 0xff00_0000) as i32)
                    .collect(),
            ),
        )
        .write(&mut *bytes, &version)?;
    }
    let ambient = pumpkin_data::tracked_data::living_entity::DATA_EFFECT_AMBIENCE_ID;
    if ambient.id.get(&version) != 255 {
        Metadata::new(ambient, effects_ambient(living)).write(bytes, &version)?;
    }
    Ok(())
}

fn write_bedrock_status_effects(
    living: &ActorLivingState,
    metadata: &mut pumpkin_protocol::bedrock::client::set_actor_data::SyncedActorDataList,
) {
    use pumpkin_protocol::bedrock::client::set_actor_data::{MetadataValue, entity_data_key};
    let colors = visible_effect_colors(living);
    let weight = colors.iter().map(|(_, weight)| *weight).sum::<u32>();
    let color = if weight == 0 {
        0
    } else {
        [16_u32, 8, 0]
            .into_iter()
            .map(|shift| {
                (colors
                    .iter()
                    .map(|(color, weight)| ((color >> shift) & 255) * weight)
                    .sum::<u32>()
                    / weight)
                    << shift
            })
            .fold(0, |color, component| color | component)
    };
    metadata.set(
        entity_data_key::EFFECT_COLOR,
        MetadataValue::Int(color as i32),
    );
    metadata.set(
        entity_data_key::EFFECT_AMBIENCE,
        MetadataValue::Byte(i8::from(effects_ambient(living))),
    );
}

pub(super) fn player_pose(living: &ActorLivingState) -> EntityPose {
    if living.combat.health_milli == 0 {
        EntityPose::Dying
    } else if living
        .avatar
        .as_ref()
        .is_some_and(|avatar| avatar.presentation.sleeping.is_some())
    {
        EntityPose::Sleeping
    } else if living
        .avatar
        .as_ref()
        .is_some_and(|avatar| avatar.stance.gliding)
    {
        EntityPose::FallFlying
    } else if living
        .avatar
        .as_ref()
        .is_some_and(|avatar| avatar.stance.swimming || avatar.stance.crawling)
    {
        EntityPose::Swimming
    } else if living
        .avatar
        .as_ref()
        .is_some_and(|avatar| avatar.stance.sneaking)
    {
        EntityPose::Crouching
    } else {
        EntityPose::Standing
    }
}

pub(crate) fn java_flags(living: &ActorLivingState) -> u8 {
    let mut flags = u8::from(living.combat.timing.fire_ticks != 0);
    if let Some(avatar) = &living.avatar {
        flags |= u8::from(avatar.stance.sneaking) << 1;
        flags |= u8::from(avatar.stance.sprinting) << 3;
        flags |= u8::from(avatar.stance.swimming) << 4;
        flags |= u8::from(avatar.stance.gliding) << 7;
    }
    flags |= u8::from(living.effects.iter().any(|effect| {
        effect.duration != 0
            && effect.kind == u16::from(pumpkin_data::effect::StatusEffect::INVISIBILITY.id)
    })) << 5;
    flags |= u8::from(living.effects.iter().any(|effect| {
        effect.duration != 0
            && effect.kind == u16::from(pumpkin_data::effect::StatusEffect::GLOWING.id)
    })) << 6;
    flags
}

fn write_bedrock_flags(
    living: &ActorLivingState,
    metadata: &mut pumpkin_protocol::bedrock::client::set_actor_data::SyncedActorDataList,
) {
    use pumpkin_protocol::bedrock::client::set_actor_data::{entity_data_flag, entity_data_key};
    let flags = java_flags(living);
    if living.avatar.is_some() {
        metadata.set_flag(
            entity_data_key::FLAGS_TWO,
            (entity_data_flag::SLEEPING - 64) as u8,
            sleeping_position(living).is_some(),
        );
        metadata.set_flag(
            entity_data_key::FLAGS_TWO,
            (entity_data_flag::WANTS_TO_WAKE - 64) as u8,
            false,
        );
    }
    for (mask, flag) in [
        (1, entity_data_flag::ON_FIRE),
        (2, entity_data_flag::SNEAKING),
        (8, entity_data_flag::SPRINTING),
        (16, entity_data_flag::SWIMMING),
        (32, entity_data_flag::INVISIBLE),
        (128, entity_data_flag::GLIDING),
    ] {
        metadata.set_flag(entity_data_key::FLAGS, flag as u8, flags & mask != 0);
    }
}

fn sleeping_position(living: &ActorLivingState) -> Option<BlockPos> {
    if living.combat.health_milli == 0 {
        return None;
    }
    let bed = living.avatar.as_ref()?.presentation.sleeping?.bed;
    Some(BlockPos(Vector3::new(bed.x, bed.y, bed.z)))
}

pub(super) fn write_java_living_presentation(
    living: &ActorLivingState,
    version: JavaMinecraftVersion,
    bytes: &mut Vec<u8>,
) -> Result<(), WritingError> {
    write_java_item_use(living, version, bytes)?;
    write_java_status_effects(living, version, bytes)?;
    let flags = pumpkin_data::tracked_data::entity::DATA_SHARED_FLAGS_ID;
    if flags.id.get(&version) != 255 {
        Metadata::new(flags, java_flags(living)).write(&mut *bytes, &version)?;
    }
    let pose = pumpkin_data::tracked_data::entity::DATA_POSE;
    if pose.id.get(&version) != 255 {
        Metadata::new(pose, VarInt(player_pose(living) as i32)).write(&mut *bytes, &version)?;
    }
    Ok(())
}

pub(super) fn java_player_presentation(
    living: &ActorLivingState,
    version: JavaMinecraftVersion,
) -> Result<Vec<u8>, WritingError> {
    let mut bytes = Vec::new();
    write_java_item_use(living, version, &mut bytes)?;
    write_java_status_effects(living, version, &mut bytes)?;
    for (field, value) in [
        (
            pumpkin_data::tracked_data::entity::DATA_AIR_SUPPLY_ID,
            i32::from(living.environment.air_ticks),
        ),
        (
            pumpkin_data::tracked_data::entity::DATA_TICKS_FROZEN,
            i32::from(living.environment.frozen_ticks),
        ),
    ] {
        if field.id.get(&version) != 255 {
            Metadata::new(field, VarInt(value)).write(&mut bytes, &version)?;
        }
    }
    let flags = pumpkin_data::tracked_data::entity::DATA_SHARED_FLAGS_ID;
    if flags.id.get(&version) != 255 {
        Metadata::new(flags, java_flags(living)).write(&mut bytes, &version)?;
    }
    let pose = pumpkin_data::tracked_data::entity::DATA_POSE;
    if pose.id.get(&version) != 255 {
        Metadata::new(pose, VarInt(player_pose(living) as i32)).write(&mut bytes, &version)?;
    }
    let bed = pumpkin_data::tracked_data::player::SLEEPING_POS_ID;
    if bed.id.get(&version) != 255 {
        Metadata::new(bed, sleeping_position(living)).write(&mut bytes, &version)?;
    }
    bytes.push(if version < JavaMinecraftVersion::V_1_9 {
        127
    } else {
        255
    });
    Ok(bytes)
}

pub(super) fn bedrock_player_presentation(
    living: &ActorLivingState,
) -> pumpkin_protocol::bedrock::client::set_actor_data::SyncedActorDataList {
    use pumpkin_protocol::bedrock::client::set_actor_data::{
        MetadataValue, SyncedActorDataList, entity_data_key,
    };
    let pose = player_pose(living);
    let dimensions = player_dimensions(living, pose);
    let mut metadata = SyncedActorDataList::new();
    metadata.set(
        entity_data_key::SCALE,
        MetadataValue::Float(player_scale(living)),
    );
    write_bedrock_item_use(living, &mut metadata);
    write_bedrock_status_effects(living, &mut metadata);
    write_bedrock_flags(living, &mut metadata);
    metadata.set(
        entity_data_key::AIR_SUPPLY,
        MetadataValue::Short(living.environment.air_ticks),
    );
    metadata.set(entity_data_key::AIR_SUPPLY_MAX, MetadataValue::Short(300));
    metadata.set(
        entity_data_key::FREEZING_EFFECT_STRENGTH,
        MetadataValue::Float(f32::from(living.environment.frozen_ticks) / 140.0),
    );
    metadata.set(entity_data_key::POSE_INDEX, MetadataValue::Int(pose as i32));
    metadata.set(
        entity_data_key::WIDTH,
        MetadataValue::Float(dimensions.width),
    );
    metadata.set(
        entity_data_key::HEIGHT,
        MetadataValue::Float(dimensions.height),
    );
    metadata.set(
        entity_data_key::BED_POSITION,
        MetadataValue::ItemPos(
            sleeping_position(living).unwrap_or(BlockPos(Vector3::new(0, 0, 0))),
        ),
    );
    metadata
}

pub(super) fn bedrock_living_presentation(
    living: &ActorLivingState,
) -> pumpkin_protocol::bedrock::client::set_actor_data::SyncedActorDataList {
    use pumpkin_protocol::bedrock::client::set_actor_data::{
        SyncedActorDataList, entity_data_flag, entity_data_key,
    };
    let mut metadata = SyncedActorDataList::new();
    write_bedrock_item_use(living, &mut metadata);
    write_bedrock_status_effects(living, &mut metadata);
    write_bedrock_flags(living, &mut metadata);
    metadata.set_flag(
        entity_data_key::FLAGS,
        entity_data_flag::ON_FIRE as u8,
        living.combat.timing.fire_ticks != 0,
    );
    metadata
}

pub(super) fn write_java_item_use(
    living: &ActorLivingState,
    version: JavaMinecraftVersion,
    bytes: &mut Vec<u8>,
) -> Result<(), WritingError> {
    let tracked = pumpkin_data::tracked_data::living_entity::DATA_LIVING_ENTITY_FLAGS;
    if tracked.id.get(&version) != 255 {
        let flags = living.use_state.as_ref().map_or(0_u8, |active| {
            1 | if active.source.loc.inv == pumpkin_cluster::inventory::INV_OFFHAND {
                2
            } else {
                0
            }
        });
        Metadata::new(tracked, flags).write(bytes, &version)?;
    }
    Ok(())
}

pub(super) fn write_bedrock_item_use(
    living: &ActorLivingState,
    metadata: &mut pumpkin_protocol::bedrock::client::set_actor_data::SyncedActorDataList,
) {
    use pumpkin_protocol::bedrock::client::set_actor_data::{
        entity_data_flag as F, entity_data_key as K,
    };
    metadata.set_flag(K::FLAGS, F::USING_ITEM as u8, living.use_state.is_some());
    metadata.set_flag(
        K::FLAGS_TWO,
        (F::BLOCKING - 64) as u8,
        living
            .use_state
            .as_ref()
            .is_some_and(|active| active.mode == pumpkin_cluster::item_action::ItemUseMode::Block),
    );
}

#[cfg(test)]
mod replica_tests {
    use super::*;
    use pumpkin_cluster::identity::{GlobalPlayerId, PlayerSlot, ServerId};
    use pumpkin_protocol::bedrock::client::set_actor_data::{
        MetadataValue, entity_data_flag as F, entity_data_key as K,
    };

    fn living() -> ActorLivingState {
        super::super::cluster_actor_import::from_player_nbt(
            GlobalPlayerId::new(ServerId(2), PlayerSlot(7)),
            None,
        )
        .unwrap()
        .state
    }

    #[test]
    fn remote_death_and_restoration_project_on_both_protocols() {
        let mut state = living();
        state.environment.air_ticks = 101;
        state.environment.frozen_ticks = 70;
        let versions = [
            JavaMinecraftVersion::V_1_8,
            JavaMinecraftVersion::V_1_19,
            JavaMinecraftVersion::V_1_20_2,
            JavaMinecraftVersion::V_1_21_11,
        ];
        let baseline = versions.map(|version| java_player_presentation(&state, version).unwrap());
        state.combat.health_milli = 0;
        assert!(player_pose(&state) == EntityPose::Dying);
        let dead = bedrock_player_presentation(&state);
        assert!(
            matches!(dead.0.get(&K::POSE_INDEX), Some(MetadataValue::Int(pose)) if *pose == EntityPose::Dying as i32)
        );
        assert!(matches!(
            dead.0.get(&K::AIR_SUPPLY),
            Some(MetadataValue::Short(101))
        ));
        assert!(
            matches!(dead.0.get(&K::FREEZING_EFFECT_STRENGTH), Some(MetadataValue::Float(strength)) if *strength == 0.5)
        );
        for (index, version) in versions.into_iter().enumerate() {
            let bytes = java_player_presentation(&state, version).unwrap();
            if version >= JavaMinecraftVersion::V_1_14 {
                assert_ne!(bytes, baseline[index]);
            }
            assert_eq!(
                bytes.last(),
                Some(&if version < JavaMinecraftVersion::V_1_9 {
                    127
                } else {
                    255
                })
            );
        }
        state.combat.health_milli = 20_000;
        assert!(player_pose(&state) == EntityPose::Standing);
        let restored = bedrock_player_presentation(&state);
        assert!(
            matches!(restored.0.get(&K::POSE_INDEX), Some(MetadataValue::Int(pose)) if *pose == EntityPose::Standing as i32)
        );
        for (index, version) in versions.into_iter().enumerate() {
            assert_eq!(
                java_player_presentation(&state, version).unwrap(),
                baseline[index]
            );
        }
    }

    #[test]
    fn expired_remote_effects_clear_visual_flags() {
        let mut state = living();
        for kind in [
            pumpkin_data::effect::StatusEffect::INVISIBILITY.id,
            pumpkin_data::effect::StatusEffect::GLOWING.id,
        ] {
            state
                .effects
                .push(pumpkin_cluster::player_admin::EffectState {
                    kind: u16::from(kind),
                    duration: 1,
                    amplifier: 0,
                    ambient: false,
                    show_particles: true,
                    show_icon: true,
                    blend: false,
                });
        }
        assert_eq!(java_flags(&state) & 0x60, 0x60);
        let visible = bedrock_player_presentation(&state);
        assert!(
            matches!(visible.0.get(&K::FLAGS), Some(MetadataValue::Int64(flags)) if flags & (1_i64 << F::INVISIBLE) != 0)
        );
        state
            .effects
            .iter_mut()
            .for_each(|effect| effect.duration = 0);
        assert_eq!(java_flags(&state) & 0x60, 0);
        let expired = bedrock_player_presentation(&state);
        assert!(
            matches!(expired.0.get(&K::FLAGS), Some(MetadataValue::Int64(flags)) if flags & (1_i64 << F::INVISIBLE) == 0)
        );
    }
}

pub(crate) fn player_scale(living: &ActorLivingState) -> f32 {
    let attribute = &pumpkin_data::attributes::Attributes::SCALE;
    let id = u16::from(attribute.id);
    let base = living
        .combat
        .effective_attributes
        .get(&id)
        .copied()
        .map(f64::from_bits)
        .unwrap_or(attribute.default_value);
    living.attribute(id, base).clamp(0.0625, 16.0) as f32
}

pub(crate) fn player_dimensions(
    living: &ActorLivingState,
    pose: pumpkin_data::entity::EntityPose,
) -> pumpkin_util::math::boundingbox::EntityDimensions {
    let dimensions = crate::entity::Entity::get_entity_dimensions(pose);
    let scale = player_scale(living);
    pumpkin_util::math::boundingbox::EntityDimensions::new(
        dimensions.width * scale,
        dimensions.height * scale,
        dimensions.eye_height * scale,
    )
}

pub(crate) fn player_eye_height(living: &ActorLivingState) -> f32 {
    player_dimensions(living, player_pose(living)).eye_height
}

pub(super) use super::cluster_living_sleep::{
    prepare_player_sleep_transition, publish_player_sleep_transition,
};
