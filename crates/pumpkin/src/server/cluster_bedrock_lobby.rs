use std::sync::Arc;

use bytes::Bytes;
use pumpkin_protocol::bedrock::client::GameType;
use pumpkin_protocol::bedrock::client::start_game::{
    CStartGame, Experiments, GamePublishSetting, LevelSettings, ServerTelemetryData,
};
use pumpkin_protocol::codec::{
    var_int::VarInt, var_long::VarLong, var_uint::VarUInt, var_ulong::VarULong,
};
use pumpkin_util::math::{position::BlockPos, vector3::Vector3};
use uuid::Uuid;

use crate::net::bedrock::BedrockClient;
use crate::server::Server;

pub(crate) struct BedrockLobbyImage {
    pub chunks: Arc<[Bytes]>,
    pub registry: Bytes,
}

impl BedrockLobbyImage {
    pub fn for_dimension(
        compression: (usize, u32),
        dimension: &pumpkin_data::dimension::Dimension,
    ) -> Self {
        use pumpkin_world::chunk::palette::{BiomePalette, BlockPalette};
        let center = super::cluster_lobby::lobby_center_chunk();
        let chunks = (-4..=4)
            .flat_map(|z| (-4..=4).map(move |x| (center.x + x, center.y + z)))
            .map(|(x, z)| {
                let section_count = (dimension.height / 16) as usize;
                let mut blocks = vec![BlockPalette::default(); section_count];
                let mut block_actors = Vec::new();
                if x == center.x && z == center.y {
                    for dz in 0..16 {
                        for dx in 0..16 {
                            blocks[((100 - dimension.min_y) / 16) as usize].set(
                                dx,
                                ((100 - dimension.min_y) % 16) as usize,
                                dz,
                                pumpkin_data::Block::END_PORTAL.default_state.id,
                            );
                            let mut actor = pumpkin_nbt::compound::NbtCompound::new();
                            actor.put_string("id", "EndPortal".into());
                            actor.put_int("x", x * 16 + dx as i32);
                            actor.put_int("y", 100);
                            actor.put_int("z", z * 16 + dz as i32);
                            block_actors.push(actor);
                        }
                    }
                }
                let (payload, _) = crate::net::bedrock::level_chunk::CLevelChunk::encode_owned(
                    x,
                    z,
                    dimension.min_y,
                    &blocks,
                    &vec![BiomePalette::default(); section_count],
                    dimension_id(dimension),
                    false,
                    &block_actors,
                )
                .expect("virtual Bedrock lobby chunk image");
                let mut encoder =
                    pumpkin_protocol::bedrock::packet_encoder::BedrockBatchEncoder::new();
                encoder.set_compression(compression);
                let mut frame = Vec::new();
                encoder
                    .write_game_packet(
                        58,
                        pumpkin_protocol::bedrock::SubClient::Main,
                        pumpkin_protocol::bedrock::SubClient::Main,
                        &payload,
                        &mut frame,
                    )
                    .expect("virtual Bedrock lobby framing");
                Bytes::from(frame)
            })
            .collect::<Vec<_>>();
        use pumpkin_protocol::bedrock::client::{CItemRegistry, ItemData};
        let registry = CItemRegistry {
            items: pumpkin_data::item::BedrockItem::ALL_BEDROCK_ITEMS
                .iter()
                .map(|item| ItemData {
                    item_name: item.registry_key.into(),
                    item_id: item.id,
                    is_component_based: item.component_based,
                    item_version: VarInt::from(match item.version {
                        pumpkin_data::item::BedrockItemVersion::Legacy => 0,
                        pumpkin_data::item::BedrockItemVersion::DataDriven => 1,
                        pumpkin_data::item::BedrockItemVersion::None => 2,
                    }),
                    component_data: item.definition_components.into(),
                })
                .collect(),
        };
        let mut encoder = pumpkin_protocol::bedrock::packet_encoder::BedrockBatchEncoder::new();
        encoder.set_compression(compression);
        let registry = encoder
            .serialize_packet(&registry)
            .expect("virtual Bedrock item registry");
        Self {
            chunks: chunks.into(),
            registry,
        }
    }
}

pub(crate) fn dimension_id(dimension: &pumpkin_data::dimension::Dimension) -> i32 {
    match dimension.minecraft_name {
        "minecraft:the_nether" => 1,
        "minecraft:the_end" => 2,
        _ => 0,
    }
}

pub(crate) fn start(
    client: &BedrockClient,
    entity_id: i32,
    server: &Server,
    dimension: &pumpkin_data::dimension::Dimension,
) {
    let settings = LevelSettings {
        seed: 0,
        spawn_biome_type: 0,
        custom_biome_name: String::new(),
        dimension: VarInt(dimension_id(dimension)),
        generator_type: VarInt(1),
        world_gamemode: GameType::Spectator,
        hardcore: server.basic_config.hardcore,
        difficulty: VarInt(0),
        spawn_position: BlockPos::new(1_000_008, 100, 1_000_008),
        has_achievements_disabled: true,
        editor_world_type: VarInt(0),
        is_created_in_editor: false,
        is_exported_from_editor: false,
        day_cycle_stop_time: VarInt(6000),
        education_edition_offer: VarUInt(0),
        has_education_features_enabled: false,
        education_product_id: String::new(),
        rain_level: 0.0,
        lightning_level: 0.0,
        has_confirmed_platform_locked_content: false,
        was_multiplayer_intended: true,
        was_lan_broadcasting_intended: true,
        xbox_live_broadcast_setting: GamePublishSetting::Public,
        platform_broadcast_setting: GamePublishSetting::Public,
        commands_enabled: true,
        is_texture_packs_required: false,
        rule_data: Vec::new(),
        experiments: Experiments {
            toggles: Vec::new(),
            experiments_ever_toggled: false,
        },
        bonus_chest: false,
        has_start_with_map_enabled: false,
        permission_level: 2,
        server_simulation_distance: 0,
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
        game_version: pumpkin_world::CURRENT_BEDROCK_MC_VERSION.into(),
        limited_world_width: 0,
        limited_world_height: 0,
        new_nether: true,
        edu_shared_uri_button_name: String::new(),
        edu_shared_uri_link_uri: String::new(),
        override_force_experimental_gameplay_has_value: false,
        chat_restriction_level: 0,
        disable_player_interactions: false,
        server_editor_connection_policy: VarInt(0),
        allow_anonymous_block_drops_in_editor_worlds: false,
    };
    client.try_enqueue_client_packet(&CStartGame {
        entity_id: VarLong(i64::from(entity_id)),
        runtime_entity_id: VarULong(entity_id as u64),
        player_gamemode: GameType::Spectator,
        position: Vector3::new(1_000_008.0, 102.0, 1_000_008.0),
        pitch: 90.0,
        yaw: 0.0,
        level_settings: settings,
        level_id: String::new(),
        level_name: "Pumpkin world".into(),
        premium_world_template_id: String::new(),
        is_trial: false,
        rewind_history_size: VarInt(0),
        server_authoritative_block_breaking: true,
        current_level_time: 6000,
        enchantment_seed: VarInt(0),
        block_properties_size: VarUInt(0),
        multiplayer_correlation_id: Uuid::nil().to_string(),
        enable_itemstack_net_manager: true,
        server_version: "Pumpkin Rust Server".into(),
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
    });
    client.try_enqueue_client_packet(&pumpkin_protocol::bedrock::client::CBiomeDefinitionList);
    if let Some(image) = server
        .cluster_lobby_images
        .bedrock
        .get(dimension.minecraft_name)
    {
        client.try_enqueue_packet(image.registry.clone());
    }
}

#[cfg(test)]
mod tests {
    use std::collections::HashSet;
    use std::io::Cursor;

    use pumpkin_protocol::bedrock::packet_decoder::BedrockBatchDecoder;
    use pumpkin_protocol::codec::{var_int::VarInt, var_uint::VarUInt};
    use pumpkin_protocol::serial::PacketRead;

    use super::BedrockLobbyImage;

    fn read_storage(bytes: &mut &[u8]) -> Vec<u16> {
        let bits = u8::read(bytes).unwrap() >> 1;
        let entries_per_word = if bits == 0 { 0 } else { 32 / usize::from(bits) };
        let words = (0..if bits == 0 {
            0
        } else {
            4096usize.div_ceil(entries_per_word)
        })
            .map(|_| u32::read(bytes).unwrap())
            .collect::<Vec<_>>();
        let palette_len = if bits == 0 {
            1
        } else {
            VarInt::read(bytes).unwrap().0 as usize
        };
        let palette = (0..palette_len)
            .map(|_| VarInt::read(bytes).unwrap().0 as u16)
            .collect::<Vec<_>>();
        (0..4096)
            .map(|index| {
                if bits == 0 {
                    palette[0]
                } else {
                    let word = words[index / entries_per_word];
                    let slot = ((word >> ((index % entries_per_word) * usize::from(bits)))
                        & ((1 << bits) - 1)) as usize;
                    palette[slot]
                }
            })
            .collect()
    }

    #[tokio::test]
    async fn bedrock_lobby_frames_decode_without_world_state() {
        let center = crate::server::cluster_lobby::lobby_center_chunk();
        for (dimension, wire_dimension, first_section, section_count) in [
            (&pumpkin_data::dimension::Dimension::OVERWORLD, 0, -4i8, 24),
            (&pumpkin_data::dimension::Dimension::THE_NETHER, 1, 0i8, 16),
            (&pumpkin_data::dimension::Dimension::THE_END, 2, 0i8, 16),
        ] {
            let image = BedrockLobbyImage::for_dimension((256, 6), dimension);
            let mut decoder = BedrockBatchDecoder::new();
            decoder.set_compression(256);
            let mut positions = HashSet::new();
            let mut portal_actors = 0;
            for frame in image.chunks.iter() {
                let payload = decoder.get_packet_payload(frame.to_vec()).await.unwrap();
                let mut batch = Cursor::new(payload);
                let packet = decoder.get_game_packet(&mut batch).unwrap();
                assert_eq!(packet.id, 58);
                assert_eq!(batch.position() as usize, batch.get_ref().len());
                let mut bytes = packet.payload.as_ref();
                let x = VarInt::read(&mut bytes).unwrap().0;
                let z = VarInt::read(&mut bytes).unwrap().0;
                assert!(positions.insert((x, z)));
                assert!((x - center.x).abs() <= 4 && (z - center.y).abs() <= 4);
                assert_eq!(VarInt::read(&mut bytes).unwrap().0, wire_dimension);
                assert_eq!(VarUInt::read(&mut bytes).unwrap().0, section_count as u32);
                assert!(!bool::read(&mut bytes).unwrap());
                assert!(!bool::read(&mut bytes).unwrap());
                assert_eq!(VarUInt::read(&mut bytes).unwrap().0, 0);
                let data_len = VarUInt::read(&mut bytes).unwrap().0 as usize;
                assert_eq!(data_len, bytes.len());
                let portal = pumpkin_data::BlockState::to_be_network_id(
                    pumpkin_data::Block::END_PORTAL.default_state.id,
                );
                let air = pumpkin_data::BlockState::to_be_network_id(
                    pumpkin_data::Block::AIR.default_state.id,
                );
                let mut sections = bytes;
                for section in first_section..first_section + section_count {
                    assert_eq!(u8::read(&mut sections).unwrap(), 9);
                    let layers = u8::read(&mut sections).unwrap();
                    assert_eq!(i8::read(&mut sections).unwrap(), section);
                    let states = read_storage(&mut sections);
                    for (index, state) in states.into_iter().enumerate() {
                        let y = index % 16;
                        let expected = if (x, z) == (center.x, center.y) && section == 6 && y == 4 {
                            portal
                        } else {
                            air
                        };
                        assert_eq!(
                            state, expected,
                            "chunk ({x},{z}) section {section} local index {index}"
                        );
                    }
                    for _ in 1..layers {
                        read_storage(&mut sections);
                    }
                }
                let actors = bytes
                    .windows(b"EndPortal".len())
                    .filter(|part| *part == b"EndPortal")
                    .count();
                if (x, z) == (center.x, center.y) {
                    assert_eq!(actors, 256);
                } else {
                    assert_eq!(actors, 0);
                }
                portal_actors += actors;
            }
            assert_eq!(positions.len(), 81);
            assert_eq!(portal_actors, 256);
            let registry = decoder
                .get_packet_payload(image.registry.to_vec())
                .await
                .unwrap();
            let mut registry = Cursor::new(registry);
            assert_eq!(decoder.get_game_packet(&mut registry).unwrap().id, 162);
            assert_eq!(registry.position() as usize, registry.get_ref().len());
        }
    }
}
