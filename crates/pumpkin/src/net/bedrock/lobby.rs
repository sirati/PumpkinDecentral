use std::num::NonZero;
use std::sync::Arc;

use bytes::Bytes;
use pumpkin_data::Block;
use pumpkin_protocol::bedrock::client::{
    CBiomeDefinitionList, CChangeDimension, CChunkRadiusUpdated, CNetworkChunkPublisherUpdate,
    CPlayStatus, CSetTime, CSetTitle, GameType, TitleType,
    start_game::{CStartGame, Experiments, GamePublishSetting, LevelSettings, ServerTelemetryData},
};
use pumpkin_protocol::bedrock::server::{
    command_request::SCommandRequest, loading_screen::SLoadingScreen,
    request_chunk_radius::SRequestChunkRadius, resource_pack_client_response::SResourcePackClientResponse,
    set_local_player_as_initialized::SSetLocalPlayerAsInitialized, text::SText,
};
use pumpkin_protocol::codec::{var_int::VarInt, var_long::VarLong, var_uint::VarUInt, var_ulong::VarULong};
use pumpkin_protocol::{Packet, serial::PacketRead};
use pumpkin_util::math::{position::BlockPos, vector3::Vector3};
use pumpkin_world::chunk::ChunkData;
use pumpkin_world::level::SyncChunk;
use uuid::Uuid;

use super::{BedrockClient, CLevelChunk};
use crate::server::cluster_lobby::LobbyWaiter;
use crate::server::Server;
use crate::world::World;

const LOBBY_BLOCK_X: i32 = 1_000_008;
const LOBBY_BLOCK_Z: i32 = 1_000_008;
const LOBBY_PLATFORM_Y: i32 = 100;
const LOBBY_EYE_Y: f32 = 102.0;
const LOBBY_LOADING_SCREEN_ID: u32 = 0x4C4F4242;

fn lobby_level_settings(dimension: i32) -> LevelSettings {
    LevelSettings {
        seed: 0,
        spawn_biome_type: 0,
        custom_biome_name: String::new(),
        dimension: VarInt(dimension),
        generator_type: VarInt(5),
        world_gamemode: GameType::Spectator,
        hardcore: false,
        difficulty: VarInt(0),
        spawn_position: BlockPos::new(LOBBY_BLOCK_X, LOBBY_PLATFORM_Y + 1, LOBBY_BLOCK_Z),
        has_achievements_disabled: true,
        editor_world_type: VarInt(0),
        is_created_in_editor: false,
        is_exported_from_editor: false,
        day_cycle_stop_time: VarInt(6_000),
        education_edition_offer: VarUInt(0),
        has_education_features_enabled: false,
        education_product_id: String::new(),
        rain_level: 0.0,
        lightning_level: 0.0,
        has_confirmed_platform_locked_content: false,
        was_multiplayer_intended: true,
        was_lan_broadcasting_intended: false,
        xbox_live_broadcast_setting: GamePublishSetting::Public,
        platform_broadcast_setting: GamePublishSetting::Public,
        commands_enabled: true,
        is_texture_packs_required: false,
        rule_data: Vec::new(),
        experiments: Experiments::default(),
        bonus_chest: false,
        has_start_with_map_enabled: false,
        permission_level: 0,
        server_simulation_distance: 2,
        has_locked_behavior_pack: false,
        has_locked_resource_pack: false,
        is_from_locked_world_template: false,
        is_using_msa_gamertags_only: false,
        is_from_world_template: false,
        is_world_template_option_locked: false,
        is_only_spawning_v1_villagers: false,
        is_disabling_personas: false,
        is_disabling_custom_skins: false,
        emote_chat_muted: false,
        game_version: pumpkin_world::CURRENT_BEDROCK_MC_VERSION.to_string(),
        limited_world_width: 0,
        limited_world_height: 0,
        new_nether: true,
        edu_shared_uri_button_name: String::new(),
        edu_shared_uri_link_uri: String::new(),
        override_force_experimental_gameplay_has_value: false,
        chat_restriction_level: 0,
        disable_player_interactions: true,
        server_editor_connection_policy: VarInt(0),
        allow_anonymous_block_drops_in_editor_worlds: false,
    }
}

fn virtual_lobby_chunk() -> ChunkData {
    let chunk_x = LOBBY_BLOCK_X >> 4;
    let chunk_z = LOBBY_BLOCK_Z >> 4;
    let chunk = ChunkData::empty(chunk_x, chunk_z);
    let portal = Block::END_PORTAL.default_state.id;
    let blocks = (0..16).flat_map(|x| (0..16).map(move |z| (x, LOBBY_PLATFORM_Y, z, portal)));
    chunk.set_blocks_batch(blocks);
    chunk
}

impl BedrockClient {
    pub fn show_virtual_lobby_status(&self, status: &str) {
        self.try_enqueue_client_packet(&CSetTitle::new(
            TitleType::Times,
            String::new(),
            0,
            20,
            5,
        ));
        self.try_enqueue_client_packet(&CSetTitle::new(
            TitleType::Title,
            "Loading".to_string(),
            0,
            20,
            5,
        ));
        self.try_enqueue_client_packet(&CSetTitle::new(
            TitleType::Subtitle,
            status.to_string(),
            0,
            20,
            5,
        ));
    }

    pub fn start_virtual_lobby(&self, waiter: &LobbyWaiter) -> bool {
        let runtime_id = waiter.entity_id as u64;
        let position = Vector3::new(LOBBY_BLOCK_X as f32 + 0.5, LOBBY_EYE_Y, LOBBY_BLOCK_Z as f32 + 0.5);
        let start_game = CStartGame {
            entity_id: VarLong(waiter.entity_id as i64),
            runtime_entity_id: VarULong(runtime_id),
            player_gamemode: GameType::Spectator,
            position,
            pitch: 90.0,
            yaw: 0.0,
            level_settings: lobby_level_settings(waiter.bedrock_lobby_dimension()),
            level_id: "pumpkin:cluster_lobby".to_string(),
            level_name: "Loading".to_string(),
            premium_world_template_id: String::new(),
            is_trial: false,
            rewind_history_size: VarInt(0),
            server_authoritative_block_breaking: true,
            current_level_time: 6_000,
            enchantment_seed: VarInt(0),
            block_properties_size: VarUInt(0),
            multiplayer_correlation_id: Uuid::nil().to_string(),
            enable_itemstack_net_manager: true,
            server_version: "Pumpkin Rust Server".to_string(),
            compound_id: 10,
            compound_len: VarUInt(0),
            compound_end: 0,
            block_registry_checksum: 0,
            world_template_id: Uuid::nil(),
            enable_clientside_generation: false,
            blocknetwork_ids_are_hashed: false,
            server_auth_sounds: true,
            server_join_information: None,
            telemetry: ServerTelemetryData {
                server_id: String::new(),
                scenario_id: String::new(),
                world_id: String::new(),
                owner_id: String::new(),
            },
        };
        let chunk = virtual_lobby_chunk();
        let packets = [
            self.serialize_packet(&start_game),
            self.serialize_packet(&CBiomeDefinitionList),
            self.serialize_packet(&CSetTime::new(6_000)),
            self.serialize_packet(&CNetworkChunkPublisherUpdate::new(
                BlockPos::new(LOBBY_BLOCK_X, LOBBY_PLATFORM_Y + 1, LOBBY_BLOCK_Z),
                16,
            )),
            self.serialize_packet(&CLevelChunk {
                dimension: waiter.bedrock_lobby_dimension(),
                cache_enabled: false,
                chunk: &chunk,
                block_actors: &[],
            }),
            self.serialize_packet(&CPlayStatus::PlayerSpawn),
        ];
        let Ok(packets) = packets.into_iter().collect::<Result<Vec<Bytes>, _>>() else {
            return false;
        };
        for packet in packets {
            self.try_enqueue_packet(packet);
        }
        self.show_virtual_lobby_status("Preparing world");
        true
    }

    pub fn start_virtual_lobby_handoff(
        &self,
        waiter: &LobbyWaiter,
        world: &World,
        chunks: &[SyncChunk],
        position: Vector3<f32>,
        dimension: i32,
    ) -> bool {
        let mut packets = Vec::with_capacity(chunks.len() + 2);
        let change = CChangeDimension {
            dimension_id: VarInt(dimension),
            position,
            respawn: false,
            loading_screen_id: Some(LOBBY_LOADING_SCREEN_ID),
        };
        let Ok(change) = self.serialize_packet(&change) else {
            return false;
        };
        packets.push(change);
        let publisher = CNetworkChunkPublisherUpdate::new(
            BlockPos::new(position.x.floor() as i32, position.y.floor() as i32, position.z.floor() as i32),
            16,
        );
        let Ok(publisher) = self.serialize_packet(&publisher) else {
            return false;
        };
        packets.push(publisher);
        for chunk in chunks {
            let block_actors = world.bedrock_chunk_block_actors(chunk);
            let packet = CLevelChunk {
                dimension,
                cache_enabled: false,
                chunk,
                block_actors: &block_actors,
            };
            let Ok(packet) = self.serialize_packet(&packet) else {
                return false;
            };
            packets.push(packet);
        }
        if !waiter.bedrock_start_handoff() {
            return false;
        }
        for packet in packets {
            self.try_enqueue_packet(packet);
        }
        self.show_virtual_lobby_status("Loading");
        true
    }

    pub async fn progress_virtual_lobby_packets(
        self: &Arc<Self>,
        server: &Arc<Server>,
        waiter: Arc<LobbyWaiter>,
    ) -> bool {
        let handoff = waiter.handoff_token();
        loop {
            tokio::select! {
                () = handoff.cancelled() => return true,
                () = self.close_token.cancelled() => return false,
                packet = self.get_packet() => {
                    let Some(packet) = packet else {
                        return false;
                    };
                    let reader = &mut &packet.payload[..];
                    match packet.id {
                        SResourcePackClientResponse::PACKET_ID => {
                            let Ok(packet) = SResourcePackClientResponse::read(reader) else {
                                return false;
                            };
                            if packet.response == SResourcePackClientResponse::STATUS_COMPLETED {
                                if !self.start_virtual_lobby(&waiter) {
                                    return false;
                                }
                            } else {
                                self.handle_resource_pack_response(packet, server).await;
                            }
                        }
                        SSetLocalPlayerAsInitialized::PACKET_ID => {
                            let Ok(packet) = SSetLocalPlayerAsInitialized::read(reader) else {
                                return false;
                            };
                            if packet.player_id.0 == waiter.entity_id as u64 {
                                waiter.bedrock_mark_ready();
                            }
                        }
                        SLoadingScreen::PACKET_ID => {
                            let Ok(packet) = SLoadingScreen::read(reader) else {
                                return false;
                            };
                            if packet.is_loading_done() {
                                waiter.bedrock_finish_handoff();
                            }
                        }
                        SRequestChunkRadius::PACKET_ID => {
                            let Ok(packet) = SRequestChunkRadius::read(reader) else {
                                return false;
                            };
                            let maximum = NonZero::<i32>::from(server.advanced_config.networking.bedrock.view_distance).get();
                            self.try_enqueue_client_packet(&CChunkRadiusUpdated {
                                chunk_radius: VarInt(packet.chunk_radius.0.clamp(2, maximum)),
                            });
                        }
                        SCommandRequest::PACKET_ID => {
                            let Ok(packet) = SCommandRequest::read(reader) else {
                                return false;
                            };
                            crate::server::cluster_lobby::execute_lobby_command(
                                server,
                                waiter.clone(),
                                packet.command.strip_prefix('/').unwrap_or(&packet.command).to_string(),
                            );
                        }
                        SText::PACKET_ID => {
                            let Ok(packet) = SText::read(reader) else {
                                return false;
                            };
                            if let Some(command) = packet.message.strip_prefix('/') {
                                crate::server::cluster_lobby::execute_lobby_command(
                                    server,
                                    waiter.clone(),
                                    command.to_string(),
                                );
                            } else {
                                crate::server::cluster_lobby::broadcast_lobby_chat(
                                    server,
                                    &waiter,
                                    packet.message.to_string(),
                                );
                            }
                        }
                        _ => {}
                    }
                }
            }
        }
    }
}
