use std::sync::Arc;

use pumpkin_util::text::TextComponent;
use pumpkin_util::text::color::NamedColor;

use super::Server;
use crate::entity::player::Player;
use crate::net::ClientPlatform;
use pumpkin_util::math::vector3::Vector3;

pub struct PreparedBootstrap {
    chat: tokio::sync::mpsc::OwnedPermit<super::cluster_secure_chat::SecureChatInput>,
    world_packets: Vec<bytes::Bytes>,
}

pub fn prepare_bootstrap(
    server: &Server,
    player: &Player,
    world: &Arc<crate::world::World>,
) -> Option<PreparedBootstrap> {
    let world_packets = prepare_world_bootstrap(player, world)?;
    Some(PreparedBootstrap {
        chat: server.cluster_secure_chat.reserve_promote()?,
        world_packets,
    })
}

pub fn publish_bootstrap(server: &Arc<Server>, player: &Arc<Player>, prepared: PreparedBootstrap) {
    let snapshot = server.cluster_presence.snapshot();
    for packet in prepared.world_packets {
        player.client.try_enqueue_packet(packet);
    }
    let level = snapshot.permission_for_uuid(player.gameprofile.id.as_bytes());
    player.set_permission_lvl(server, level, &server.command_dispatcher.load());
    prepared
        .chat
        .send(super::cluster_secure_chat::SecureChatInput::Promote {
            player: player.clone(),
        });
}

pub fn retired(
    server: &Arc<Server>,
    player: &Arc<Player>,
    fire_event: bool,
    source: crate::command::CommandSource,
) -> bool {
    let Some(gid) = player.cluster_gid() else {
        return false;
    };
    let Some(presence) = server.cluster_presence.try_reserve() else {
        return false;
    };
    let uuid = player.gameprofile.id.into_bytes();
    let session = player
        .cluster_owner_install_generation
        .load(std::sync::atomic::Ordering::Acquire);
    let player = player.clone();
    let server = server.clone();
    tokio::spawn(async move {
        let message = TextComponent::translate_cross(
            pumpkin_data::translation::java::MULTIPLAYER_PLAYER_LEFT,
            pumpkin_data::translation::bedrock::MULTIPLAYER_PLAYER_LEFT,
            [TextComponent::text(player.gameprofile.name.clone())],
        )
        .color_named(NamedColor::Yellow);
        let message = if fire_event {
            let mut event = crate::plugin::api::events::player::player_leave::PlayerLeaveEvent::new(
                player.clone(),
                message,
            );
            super::cluster_permissions::with_source(
                &server,
                &source,
                server.plugin_manager.fire(&server, &mut event),
            )
            .await;
            (!event.cancelled).then_some(event.leave_message)
        } else {
            None
        };
        let _ = server.cluster_secure_chat.remove(&player);
        presence.send(super::cluster_presence::PresenceCommand::LocalDeparture {
            gid,
            uuid,
            session,
            message,
        });
    });
    true
}

pub(crate) struct BedLeaveCallback {
    player: Arc<Player>,
    source: crate::command::CommandSource,
    bed: pumpkin_util::math::position::BlockPos,
}

pub(crate) fn reserve_bed_leave(
    server: &Server,
) -> Option<tokio::sync::mpsc::OwnedPermit<BedLeaveCallback>> {
    server
        .cluster_bed_leave_callbacks
        .clone()
        .try_reserve_owned()
        .ok()
}

pub(crate) fn publish_bed_leave(
    _server: &Server,
    player: &Arc<Player>,
    permit: tokio::sync::mpsc::OwnedPermit<BedLeaveCallback>,
    source: crate::command::CommandSource,
    bed: pumpkin_util::math::position::BlockPos,
) {
    permit.send(BedLeaveCallback {
        player: player.clone(),
        source,
        bed,
    });
}

pub(crate) fn spawn_bed_leave(
    server: &Arc<Server>,
    mut callbacks: tokio::sync::mpsc::Receiver<BedLeaveCallback>,
) {
    let weak = Arc::downgrade(server);
    server.spawn_task(async move {
        while let Some(callback) = callbacks.recv().await {
            let Some(server) = weak.upgrade() else {
                break;
            };
            let mut event =
                crate::plugin::api::events::player::player_bed::PlayerBedLeaveEvent::new(
                    callback.player,
                    callback.bed,
                );
            super::cluster_permissions::with_source(
                &server,
                &callback.source,
                server.plugin_manager.fire(&server, &mut event),
            )
            .await;
        }
    });
}

fn prepare_world_bootstrap(
    player: &Player,
    world: &Arc<crate::world::World>,
) -> Option<Vec<bytes::Bytes>> {
    let environment = world.cluster_environment_view.load();
    let snapshot = environment.snapshot.as_ref()?;
    let border = &environment.border;
    let slot = super::cluster_world_rules::disciplined_slot().unwrap_or(snapshot.slot);
    let spawn = &environment.spawn;
    let position = pumpkin_util::math::position::BlockPos::new(spawn.x, spawn.y, spawn.z);
    let mut packets = match player.client.as_ref() {
        ClientPlatform::Java(client) => vec![
            client
                .serialize_packet(
                    &pumpkin_protocol::java::client::play::CPlayerSpawnPosition::new(
                        position,
                        spawn.yaw,
                        spawn.pitch,
                        world.dimension.minecraft_name.to_owned(),
                    ),
                )
                .ok()?,
            client
                .serialize_packet(
                    &pumpkin_protocol::java::client::play::CInitializeWorldBorder::new(
                        border.center_x,
                        border.center_z,
                        border.current_diameter(slot),
                        border.new_diameter,
                        border.remaining_millis(slot).into(),
                        29_999_984.into(),
                        (border.warning_blocks as i32).into(),
                        (border.warning_time as i32).into(),
                    ),
                )
                .ok()?,
        ],
        ClientPlatform::Bedrock(client) => vec![
            client
                .serialize_packet(&pumpkin_protocol::bedrock::client::CSetSpawnPosition {
                    spawn_position_type:
                        pumpkin_protocol::bedrock::client::SpawnPositionType::WorldRespawn,
                    block_position: position,
                    dimension_type: match world.dimension.minecraft_name {
                        "minecraft:the_nether" => 1.into(),
                        "minecraft:the_end" => 2.into(),
                        _ => 0.into(),
                    },
                    spawn_block_pos: position,
                })
                .ok()?,
        ],
    };
    packets.extend(prepare_environment(player, world, &environment)?);
    Some(packets)
}

pub(crate) fn publish_world_bootstrap(player: &Player, world: &Arc<crate::world::World>) -> bool {
    let Some(packets) = prepare_world_bootstrap(player, world) else {
        return false;
    };
    for packet in packets {
        player.client.try_enqueue_packet(packet);
    }
    true
}

fn prepare_environment(
    player: &Player,
    world: &crate::world::World,
    environment: &super::cluster_environment::EnvironmentView,
) -> Option<Vec<bytes::Bytes>> {
    let server = world.server.upgrade()?;
    let config = super::cluster_gamerules::view(&server)?;
    let packets = match player.client.as_ref() {
        ClientPlatform::Java(client) => {
            use pumpkin_protocol::java::client::play::{CGameEvent, CUpdateTime, GameEvent};
            let (ticks, partial, rate) = environment
                .time
                .pack_network_state(config.rules.advance_time);
            let time = CUpdateTime::new_clock(environment.time.world_age, 0, ticks, partial, rate);
            let rain = CGameEvent::new(
                if environment.weather.raining {
                    GameEvent::BeginRaining
                } else {
                    GameEvent::EndRaining
                },
                0.0,
            );
            let rain_level =
                CGameEvent::new(GameEvent::RainLevelChange, environment.weather.rain_level);
            let thunder = CGameEvent::new(
                GameEvent::ThunderLevelChange,
                environment.weather.thunder_level,
            );
            let (Ok(time), Ok(rain), Ok(rain_level), Ok(thunder)) = (
                client.serialize_packet(&time),
                client.serialize_packet(&rain),
                client.serialize_packet(&rain_level),
                client.serialize_packet(&thunder),
            ) else {
                return None;
            };
            let difficulty = pumpkin_protocol::java::client::play::CChangeDifficulty::new(
                config.difficulty as u8,
                config.difficulty_locked,
            );
            let Ok(difficulty) = client.serialize_packet(&difficulty) else {
                return None;
            };
            vec![time, rain, rain_level, thunder, difficulty]
        }
        ClientPlatform::Bedrock(client) => {
            use pumpkin_protocol::bedrock::client::gamerules_changed::{
                CGamerulesChanged, GameRule, RuleValue,
            };
            let rules = &config.rules;
            let mut rule_data = [
                ("dodaylightcycle", rules.advance_time),
                ("doweathercycle", rules.advance_weather),
                ("doimmediaterespawn", rules.immediate_respawn),
                ("keepinventory", rules.keep_inventory),
                ("showdeathmessages", rules.show_death_messages),
                ("commandblockoutput", rules.command_block_output),
                ("sendcommandfeedback", rules.send_command_feedback),
                ("mobgriefing", rules.mob_griefing),
                ("domobloot", rules.mob_drops),
                ("doentitydrops", rules.entity_drops),
                ("dotiledrops", rules.block_drops),
                ("domobspawning", rules.spawn_mobs),
                ("falldamage", rules.fall_damage),
                ("firedamage", rules.fire_damage),
                ("drowningdamage", rules.drowning_damage),
                ("freezedamage", rules.freeze_damage),
            ]
            .into_iter()
            .map(|(name, value)| GameRule {
                rule_name: name.to_owned(),
                rule_can_be_modified: false,
                rule_value: RuleValue::Bool(value),
            })
            .collect::<Vec<_>>();
            for (name, value) in [
                ("randomtickspeed", rules.random_tick_speed),
                ("spawnradius", rules.respawn_radius),
                ("maxcommandchainlength", rules.max_command_sequence_length),
            ] {
                rule_data.push(GameRule {
                    rule_name: name.to_owned(),
                    rule_can_be_modified: false,
                    rule_value: RuleValue::Int(value.clamp(0, i64::from(u32::MAX)) as u32),
                });
            }
            let rules = CGamerulesChanged { rule_data };
            let Ok(rules) = client.serialize_packet(&rules) else {
                return None;
            };
            let time = pumpkin_protocol::bedrock::client::set_time::CSetTime::new(
                environment.time.time_of_day as i32,
            );
            let position = Vector3::new(0.0, 0.0, 0.0);
            let rain = pumpkin_protocol::bedrock::client::level_event::CLevelEvent {
                event_id: (if environment.weather.raining {
                    3001
                } else {
                    3003
                })
                .into(),
                position,
                data: ((environment.weather.rain_level * 65535.0).round() as i32).into(),
            };
            let thunder = pumpkin_protocol::bedrock::client::level_event::CLevelEvent {
                event_id: (if environment.weather.thundering {
                    3002
                } else {
                    3004
                })
                .into(),
                position,
                data: ((environment.weather.thunder_level * 65535.0).round() as i32).into(),
            };
            let (Ok(time), Ok(rain), Ok(thunder)) = (
                client.serialize_packet(&time),
                client.serialize_packet(&rain),
                client.serialize_packet(&thunder),
            ) else {
                return None;
            };
            let difficulty = pumpkin_protocol::bedrock::client::CSetDifficulty {
                difficulty: (config.difficulty as u32).into(),
            };
            let title = pumpkin_protocol::bedrock::client::CSetTitle::new(
                pumpkin_protocol::bedrock::client::TitleType::Clear,
                String::new(),
                0,
                0,
                0,
            );
            let (Ok(difficulty), Ok(title)) = (
                client.serialize_packet(&difficulty),
                client.serialize_packet(&title),
            ) else {
                return None;
            };
            vec![time, rain, thunder, rules, difficulty, title]
        }
    };
    Some(packets)
}
