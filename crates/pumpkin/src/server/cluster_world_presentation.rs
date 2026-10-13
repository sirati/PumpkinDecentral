use pumpkin_cluster::canonical_action::Action;
use pumpkin_cluster::world_presentation::WorldPresentationIntent;
use pumpkin_util::math::{position::BlockPos, vector2::Vector2, vector3::Vector3};

use super::Server;

pub(super) fn publish(server: &Server, action: &Action) {
    publish_intents(
        server,
        pumpkin_cluster::action_identity::ActionIdentity::Issued(action.id),
        action.tick,
        &action.presentation,
    );
}

pub(super) fn publish_resolution(
    server: &Server,
    command: &pumpkin_cluster::action_command::ActionCommand,
    record: &pumpkin_cluster::action_command::ResolutionRecord,
) {
    use pumpkin_cluster::{action_command::ActionCommand, world_command::ToolOperation};
    let (operation, location) = match command {
        ActionCommand::StripLog(command) => (ToolOperation::StripLog, command.location),
        ActionCommand::ScrapeCopper(command) => (ToolOperation::ScrapeCopper, command.location),
        ActionCommand::RemoveWax(command) => (ToolOperation::RemoveWax, command.location),
        _ => return,
    };
    let intents =
        super::cluster_world_tool_transform::command_presentations(operation, location, record);
    if matches!(record.result, pumpkin_cluster::action_command::CommandResult::Applied(ref writes) if !writes.is_empty())
    {
        publish_intents(server, record.identity, record.tick, &intents);
    }
}

fn publish_intents(
    server: &Server,
    identity: pumpkin_cluster::action_identity::ActionIdentity,
    tick: pumpkin_cluster::time::TickStamp,
    intents: &[WorldPresentationIntent],
) {
    let worlds = server.worlds.load_full();
    let seed = presentation_seed(identity, tick);
    for event in intents {
        if !event.valid() {
            continue;
        }
        let chunk = event.chunk();
        let Some(world) = worlds.iter().find(|world| {
            world.level.cluster_world_key == chunk.world.key
                && world.level.cluster_dimension == chunk.world.dimension
        }) else {
            continue;
        };
        let chunk_position = Vector2::new(chunk.x(), chunk.z());
        match *event {
            WorldPresentationIntent::Explosion {
                center,
                power,
                block_count,
                ..
            } => {
                let center = center.map(f64::from_bits);
                let position = Vector3::new(center[0], center[1], center[2]);
                let power = f32::from_bits(power);
                let particle = if power < 2.0 {
                    pumpkin_data::particle::Particle::Explosion
                } else {
                    pumpkin_data::particle::Particle::ExplosionEmitter
                };
                world.broadcast_to_chunk_editioned(
                    chunk_position,
                    &pumpkin_protocol::java::client::play::CExplosion::new(
                        position,
                        power,
                        block_count as i32,
                        None,
                        pumpkin_protocol::codec::var_int::VarInt(particle as i32),
                        pumpkin_protocol::IdOr::Id(
                            pumpkin_data::sound::Sound::EntityGenericExplode as u16,
                        ),
                    ),
                    &pumpkin_protocol::bedrock::client::level_event::CLevelEvent {
                        event_id: pumpkin_protocol::codec::var_int::VarInt(
                            0x4000 + if power < 2.0 { 14 } else { 15 },
                        ),
                        position: Vector3::new(
                            center[0] as f32,
                            center[1] as f32,
                            center[2] as f32,
                        ),
                        data: pumpkin_protocol::codec::var_int::VarInt(0),
                    },
                );
                world.broadcast_to_chunk_bedrock(
                    chunk_position,
                    &pumpkin_protocol::bedrock::client::level_sound_event::CLevelSoundEvent {
                        sound_event: "explode".to_string(),
                        position: Vector3::new(
                            center[0] as f32,
                            center[1] as f32,
                            center[2] as f32,
                        ),
                        data: pumpkin_protocol::codec::var_int::VarInt(-1),
                        actor_identifier: String::new(),
                        is_baby: false,
                        is_global: false,
                        actor_unique_id: 0,
                        fire_at_position: Vector3::new(0.0, 0.0, 0.0),
                    },
                );
            }
            WorldPresentationIntent::CustomSound {
                position,
                ref name,
                range_milli,
                volume_milli,
                pitch_milli,
                category,
                ..
            } => {
                let position = BlockPos::new(position.x, position.y, position.z).to_centered_f64();
                world.broadcast_to_chunk(
                    chunk_position,
                    &pumpkin_protocol::java::client::play::CSoundEffect::new(
                        pumpkin_protocol::IdOr::Value(pumpkin_protocol::SoundEvent {
                            sound_name: name.clone(),
                            range: range_milli.map(|range| range as f32 / 1000.0),
                        }),
                        sound_category(category),
                        &position,
                        f32::from(volume_milli) / 1000.0,
                        f32::from(pitch_milli) / 1000.0,
                        seed,
                    ),
                );
                if let Some(packet) = pumpkin_protocol::bedrock::client::CPlaySound::at(
                    name.strip_prefix("minecraft:")
                        .unwrap_or(name.as_str())
                        .to_owned(),
                    position,
                    f32::from(volume_milli) / 1000.0,
                    f32::from(pitch_milli) / 1000.0,
                ) {
                    world.broadcast_to_chunk_bedrock(chunk_position, &packet);
                }
            }
            WorldPresentationIntent::Sound {
                position,
                sound,
                volume_milli,
                pitch_milli,
                category,
                ..
            } => {
                if pumpkin_data::sound::Sound::slice()
                    .get(usize::from(sound))
                    .is_none()
                {
                    continue;
                }
                let position = BlockPos::new(position.x, position.y, position.z).to_centered_f64();
                world.broadcast_to_chunk(
                    chunk_position,
                    &pumpkin_protocol::java::client::play::CSoundEffect::new(
                        pumpkin_protocol::IdOr::Id(sound),
                        sound_category(category),
                        &position,
                        f32::from(volume_milli) / 1000.0,
                        f32::from(pitch_milli) / 1000.0,
                        seed,
                    ),
                );
                if let Some(sound) = pumpkin_data::sound::Sound::slice()
                    .get(usize::from(sound))
                    .copied()
                    && let Some(name) = super::cluster_world_sound::bedrock_name(sound)
                    && let Some(packet) = pumpkin_protocol::bedrock::client::CPlaySound::at(
                        name.to_owned(),
                        position,
                        f32::from(volume_milli) / 1000.0,
                        f32::from(pitch_milli) / 1000.0,
                    )
                {
                    world.broadcast_to_chunk_bedrock(chunk_position, &packet);
                }
            }
            WorldPresentationIntent::BreakParticles {
                position, state, ..
            } => {
                let Some(state_id) = pumpkin_data::BlockStateId::new(state) else {
                    continue;
                };
                let position = BlockPos::new(position.x, position.y, position.z);
                world.broadcast_to_chunk_editioned(
                    chunk_position,
                    &pumpkin_protocol::java::client::play::CWorldEvent::new(
                        2001,
                        position,
                        i32::from(state),
                        false,
                    ),
                    &pumpkin_protocol::bedrock::client::level_event::CLevelEvent {
                        event_id: pumpkin_protocol::codec::var_int::VarInt(2001),
                        position: Vector3::new(
                            position.0.x as f32,
                            position.0.y as f32,
                            position.0.z as f32,
                        ),
                        data: pumpkin_protocol::codec::var_int::VarInt(
                            pumpkin_data::BlockState::to_be_network_id(state_id) as i32,
                        ),
                    },
                );
            }
            WorldPresentationIntent::WorldEvent {
                position,
                event,
                data,
                ..
            } => {
                let Some(bedrock) = (match event {
                    3004 => Some(2031),
                    3005 => Some(2032),
                    _ => None,
                }) else {
                    continue;
                };
                if data != 0 {
                    continue;
                }
                let position = BlockPos::new(position.x, position.y, position.z);
                world.broadcast_to_chunk_editioned(
                    chunk_position,
                    &pumpkin_protocol::java::client::play::CWorldEvent::new(
                        i32::from(event),
                        position,
                        data,
                        false,
                    ),
                    &pumpkin_protocol::bedrock::client::level_event::CLevelEvent {
                        event_id: pumpkin_protocol::codec::var_int::VarInt(bedrock),
                        position: Vector3::new(
                            position.0.x as f32,
                            position.0.y as f32,
                            position.0.z as f32,
                        ),
                        data: pumpkin_protocol::codec::var_int::VarInt(data),
                    },
                );
            }
            WorldPresentationIntent::GameEvent { .. } => {}
        }
    }
}

fn sound_category(category: u8) -> pumpkin_data::sound::SoundCategory {
    use pumpkin_data::sound::SoundCategory;
    match category {
        0 => SoundCategory::Master,
        1 => SoundCategory::Music,
        2 => SoundCategory::Records,
        3 => SoundCategory::Weather,
        4 => SoundCategory::Blocks,
        5 => SoundCategory::Hostile,
        6 => SoundCategory::Neutral,
        7 => SoundCategory::Players,
        8 => SoundCategory::Ambient,
        9 => SoundCategory::Voice,
        _ => SoundCategory::Ui,
    }
}

fn presentation_seed(
    identity: pumpkin_cluster::action_identity::ActionIdentity,
    tick: pumpkin_cluster::time::TickStamp,
) -> i64 {
    use std::hash::Hasher;
    match identity {
        pumpkin_cluster::action_identity::ActionIdentity::Issued(id) => {
            pumpkin_cluster::order::rank_tick_actor(
                id.incarnation ^ (u64::from(id.sequence) << 32) ^ (u64::from(id.family) << 48),
                tick,
                id.actor,
            ) as i64
        }
        pumpkin_cluster::action_identity::ActionIdentity::Derived(id) => {
            let mut hash = xxhash_rust::xxh64::Xxh64::new(0);
            hash.write(&id.genesis);
            hash.write_u64(id.world.key);
            hash.write_u8(id.world.dimension);
            hash.write_i32(id.position.x);
            hash.write_i32(id.position.y);
            hash.write_i32(id.position.z);
            hash.write_u64(id.source_slot);
            hash.write_u8(id.kind as u8);
            hash.finish() as i64
        }
    }
}
