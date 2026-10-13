use super::util::write_compound_nbt;
use pumpkin_data::block_state_remap::remap_block_state_for_version;
use pumpkin_data::packet::CURRENT_MC_VERSION;
use pumpkin_nbt::compound::NbtCompound;
use pumpkin_protocol::codec::var_int::VarInt;
use pumpkin_protocol::java::client::play::{ChunkHeightmaps, LightData};
use pumpkin_protocol::ser::NetworkWriteExt;
use pumpkin_protocol::ser::WritingError;
use pumpkin_protocol::{ClientPacket, MultiVersionJavaPacket};
use pumpkin_util::math::position::BlockPos;
use pumpkin_util::math::position::get_local_cord;
use pumpkin_util::version::JavaMinecraftVersion;
use pumpkin_world::chunk::palette::NetworkPalette;
use pumpkin_world::chunk::palette::{BiomePalette, BlockPalette};
use std::io::Write;

#[expect(clippy::too_many_lines)]
pub fn write_chunk_data(
    x: i32,
    z: i32,
    heightmaps: &ChunkHeightmaps,
    block_sections: &[BlockPalette],
    biome_sections: &[BiomePalette],
    block_entities: &[(BlockPos, NbtCompound)],
    light: &LightData,
    mut write: impl Write,
    version: &JavaMinecraftVersion,
) -> Result<(), WritingError> {
    write.write_i32_be(x)?;
    write.write_i32_be(z)?;

    if version >= &JavaMinecraftVersion::V_1_21_5 {
        write.write_var_int(&VarInt(3))?;

        let mut write_heightmap = |index: i32, data: &[i64]| -> Result<(), WritingError> {
            write.write_var_int(&VarInt(index))?;
            write.write_var_int(&VarInt(data.len() as i32))?;
            for val in data {
                write.write_i64_be(*val)?;
            }
            Ok(())
        };

        write_heightmap(1, heightmaps.world_surface.as_deref().unwrap_or(&[0; 37]))?;
        write_heightmap(4, heightmaps.motion_blocking.as_deref().unwrap_or(&[0; 37]))?;
        write_heightmap(
            5,
            heightmaps
                .motion_blocking_no_leaves
                .as_deref()
                .unwrap_or(&[0; 37]),
        )?;
    } else {
        let mut comp = pumpkin_nbt::compound::NbtCompound::new();
        if let Some(ref ws) = heightmaps.world_surface {
            comp.put(
                "WORLD_SURFACE",
                pumpkin_nbt::tag::NbtTag::LongArray(ws.to_vec()),
            );
        }
        if let Some(ref mb) = heightmaps.motion_blocking {
            comp.put(
                "MOTION_BLOCKING",
                pumpkin_nbt::tag::NbtTag::LongArray(mb.to_vec()),
            );
        }
        if let Some(ref mbnl) = heightmaps.motion_blocking_no_leaves {
            comp.put(
                "MOTION_BLOCKING_NO_LEAVES",
                pumpkin_nbt::tag::NbtTag::LongArray(mbnl.to_vec()),
            );
        }
        write_compound_nbt(&mut write, &comp, *version)?;
    }

    {
        let mut blocks_and_biomes_buf = Vec::new();
        let mut zero_bytes_count = 0;

        for (block_palette, biome_palette) in block_sections.iter().zip(biome_sections.iter()) {
            let non_empty_block_count = block_palette.non_air_block_count() as i16;
            blocks_and_biomes_buf.write_i16_be(non_empty_block_count)?;
            if version >= &JavaMinecraftVersion::V_26_1 {
                let liquid_count = block_palette.liquid_block_count() as i16;
                blocks_and_biomes_buf.write_i16_be(liquid_count)?;
            }

            let mut block_network = block_palette.convert_network();
            if version < &CURRENT_MC_VERSION {
                match &mut block_network.palette {
                    NetworkPalette::Single(registry_id) => {
                        *registry_id = remap_block_state_for_version(*registry_id, *version);
                    }
                    NetworkPalette::Indirect(palette) => {
                        for registry_id in palette.iter_mut() {
                            *registry_id = remap_block_state_for_version(*registry_id, *version);
                        }
                    }
                    NetworkPalette::Direct => {
                        let bits_per_entry = usize::from(block_network.bits_per_entry);
                        let values_per_i64 = 64 / bits_per_entry;
                        let id_mask = (1u64 << bits_per_entry) - 1;

                        for packed_word in &mut block_network.packed_data {
                            let mut remapped_word = 0u64;
                            let packed_word_u64 = *packed_word as u64;
                            for index in 0..values_per_i64 {
                                let shift = index * bits_per_entry;
                                let state_id = ((packed_word_u64 >> shift) & id_mask) as u16;
                                let remapped_id = remap_block_state_for_version(state_id, *version);
                                remapped_word |= u64::from(remapped_id) << shift;
                            }
                            *packed_word = remapped_word as i64;
                        }
                    }
                }
            }
            blocks_and_biomes_buf.write_u8(block_network.bits_per_entry)?;

            match block_network.palette {
                NetworkPalette::Single(registry_id) => {
                    blocks_and_biomes_buf.write_var_int(&registry_id.into())?;
                }
                NetworkPalette::Indirect(palette) => {
                    blocks_and_biomes_buf.write_var_int(&palette.len().try_into().map_err(
                        |_| {
                            WritingError::Message(format!(
                                "{} is not representable as a VarInt!",
                                palette.len()
                            ))
                        },
                    )?)?;
                    for registry_id in palette {
                        blocks_and_biomes_buf.write_var_int(&registry_id.into())?;
                    }
                }
                NetworkPalette::Direct => {}
            }

            if version <= &JavaMinecraftVersion::V_1_21_4 {
                blocks_and_biomes_buf.write_list(&block_network.packed_data, |buf, &packed| {
                    buf.write_i64_be(packed)
                })?;
            } else {
                for packed in &block_network.packed_data {
                    blocks_and_biomes_buf.write_i64_be(*packed)?;
                }
            }

            let biome_network = biome_palette.convert_network();
            blocks_and_biomes_buf.write_u8(biome_network.bits_per_entry)?;

            match biome_network.palette {
                NetworkPalette::Single(registry_id) => {
                    blocks_and_biomes_buf.write_var_int(&registry_id.into())?;
                }
                NetworkPalette::Indirect(palette) => {
                    blocks_and_biomes_buf.write_var_int(&palette.len().try_into().map_err(
                        |_| {
                            WritingError::Message(format!(
                                "{} is not representable as a VarInt!",
                                palette.len()
                            ))
                        },
                    )?)?;
                    for registry_id in palette {
                        blocks_and_biomes_buf.write_var_int(&registry_id.into())?;
                    }
                }
                NetworkPalette::Direct => {}
            }

            if version <= &JavaMinecraftVersion::V_1_21_4 {
                blocks_and_biomes_buf.write_list(&biome_network.packed_data, |buf, &packed| {
                    buf.write_i64_be(packed)
                })?;
            } else {
                for packed in &biome_network.packed_data {
                    blocks_and_biomes_buf.write_i64_be(*packed)?;
                }
            }

            if version == &JavaMinecraftVersion::V_1_21_5 {
                let block_storage_len = block_network.packed_data.len() as i32;
                let biome_storage_len = biome_network.packed_data.len() as i32;
                zero_bytes_count += VarInt(block_storage_len).written_size()
                    + VarInt(biome_storage_len).written_size();
            }
        }

        if version == &JavaMinecraftVersion::V_1_21_5 && zero_bytes_count > 0 {
            blocks_and_biomes_buf.resize(blocks_and_biomes_buf.len() + zero_bytes_count, 0);
        }

        write.write_var_int(&blocks_and_biomes_buf.len().try_into().map_err(|_| {
            WritingError::Message(format!(
                "{} is not representable as a VarInt!",
                blocks_and_biomes_buf.len()
            ))
        })?)?;
        write.write_slice(&blocks_and_biomes_buf)?;
    };

    write.write_var_int(&VarInt(block_entities.len() as i32))?;
    for (pos, nbt) in block_entities.iter() {
        let local_xz = ((get_local_cord(pos.0.x) & 0xF) << 4) | (get_local_cord(pos.0.z) & 0xF);

        write.write_u8(local_xz as u8)?;
        write.write_i16_be(pos.0.y as i16)?;

        let id = nbt.get_string("id").map_or(0, |id_str| {
            let name = id_str.split(':').next_back().unwrap_or(id_str);
            pumpkin_data::block_properties::BLOCK_ENTITY_TYPES
                .iter()
                .position(|&n| n == name)
                .unwrap_or(0)
        });
        let remapped_id =
            pumpkin_data::block_entity_type_id_remap::remap_block_entity_type_id_for_version(
                id as u32, *version,
            );

        write.write_var_int(&VarInt(remapped_id as i32))?;

        let mut client_nbt = nbt.clone();
        client_nbt.child_tags.remove("id");
        client_nbt.child_tags.remove("x");
        client_nbt.child_tags.remove("y");
        client_nbt.child_tags.remove("z");
        client_nbt.child_tags.remove("LootTable");
        client_nbt.child_tags.remove("LootTableSeed");
        client_nbt.child_tags.remove("PumpkinCustomData");
        client_nbt.child_tags.remove("BukkitValues");
        write_compound_nbt(&mut write, &client_nbt, *version)?;
    }

    light.write(&mut write, version)?;
    Ok(())
}

pub fn encode(
    position: pumpkin_cluster::protocol::ChunkKey,
    document: &pumpkin_nbt::compound::NbtCompound,
    blocks: &crate::server::cluster_chunk_state::OwnedChunkBlocks,
    entities: &[(
        pumpkin_cluster::protocol::BlockPos,
        pumpkin_cluster::canonical_action::Cell,
    )],
    lighting: Option<&pumpkin_world::lighting::owned::OwnedChunkLighting>,
    version: JavaMinecraftVersion,
) -> Result<(bytes::Bytes, Option<bytes::Bytes>), WritingError> {
    if blocks.block_palettes().len() != blocks.biome_palettes().len() {
        return Err(WritingError::Message(
            "chunk palette height mismatch".into(),
        ));
    }
    let heightmaps = heightmaps_from_palettes(blocks);
    let block_entities = entities
        .iter()
        .map(|(position, cell)| {
            let pumpkin_cluster::canonical_action::Value::BlockEntity(bytes) = &cell.value else {
                return Err(WritingError::Message("chunk block entity cell type".into()));
            };
            let mut reader = pumpkin_nbt::deserializer::NbtReadHelperJava::new(
                std::io::Cursor::new(bytes.as_slice()),
            );
            let nbt = pumpkin_nbt::Nbt::read_unnamed(&mut reader).map_err(|error| {
                WritingError::Message(format!("chunk block entity document: {error}"))
            })?;
            Ok((
                BlockPos::new(position.x, position.y, position.z),
                nbt.root_tag,
            ))
        })
        .collect::<Result<Vec<_>, WritingError>>()?;
    let light = light_from_document(
        document,
        blocks.min_y(),
        blocks.block_palettes().len(),
        lighting,
    )?;
    let mut buffer = Vec::with_capacity(32 * 1024);
    buffer.write_var_int(&VarInt(super::CChunkData::to_id(version)))?;
    let light_payload = if version >= JavaMinecraftVersion::V_1_18 {
        write_chunk_data(
            position.x(),
            position.z(),
            &heightmaps,
            blocks.block_palettes(),
            blocks.biome_palettes(),
            &block_entities,
            &light,
            &mut buffer,
            &version,
        )?;
        None
    } else {
        let containers = |mask: &pumpkin_protocol::codec::bit_set::BitSet, arrays: &[Vec<u8>]| {
            let mut source = arrays.iter();
            (0..blocks.block_palettes().len())
                .map(|index| {
                    if mask.get_bit(index + 1) {
                        pumpkin_world::chunk::format::LightContainer::Full(
                            source
                                .next()
                                .cloned()
                                .unwrap_or_else(|| vec![0; 2048])
                                .into_boxed_slice(),
                        )
                    } else {
                        pumpkin_world::chunk::format::LightContainer::Empty(0)
                    }
                })
                .collect::<Vec<_>>()
        };
        let block = containers(&light.block_light_mask, &light.block_light_arrays);
        let sky = containers(&light.sky_light_mask, &light.sky_light_arrays);
        if version >= JavaMinecraftVersion::V_1_9 {
            super::owned_v1_9::write_chunk_data(
                position.x(),
                position.z(),
                blocks.min_y(),
                blocks.block_palettes(),
                blocks.biome_palettes(),
                &block,
                &sky,
                &heightmaps,
                &block_entities,
                &mut buffer,
                &version,
            )?;
        } else if version == JavaMinecraftVersion::V_1_8 {
            super::owned_v1_8::write_chunk_data(
                position.x(),
                position.z(),
                blocks.min_y(),
                blocks.block_palettes(),
                blocks.biome_palettes(),
                &block,
                &sky,
                &heightmaps,
                &block_entities,
                &mut buffer,
                &version,
            )?;
        } else {
            super::owned_v1_7::write_chunk_data(
                position.x(),
                position.z(),
                blocks.min_y(),
                blocks.block_palettes(),
                blocks.biome_palettes(),
                &block,
                &sky,
                &heightmaps,
                &block_entities,
                &mut buffer,
                &version,
            )?;
        }
        if version >= JavaMinecraftVersion::V_1_14 {
            let legacy = legacy_light(&block, &sky, blocks.min_y());
            let packet = pumpkin_protocol::java::client::play::CLightUpdate::new(
                VarInt(position.x()),
                VarInt(position.z()),
                legacy,
            );
            let mut bytes = Vec::new();
            bytes.write_var_int(&VarInt(
                pumpkin_protocol::java::client::play::CLightUpdate::to_id(version),
            ))?;
            packet.write_packet_data(&mut bytes, &version)?;
            Some(bytes::Bytes::from(bytes))
        } else {
            None
        }
    };
    Ok((bytes::Bytes::from(buffer), light_payload))
}

pub(crate) fn light_from_document(
    document: &NbtCompound,
    min_y: i32,
    count: usize,
    overlay: Option<&pumpkin_world::lighting::owned::OwnedChunkLighting>,
) -> Result<LightData, WritingError> {
    if count == 0
        || count > 62
        || overlay.is_some_and(|value| value.sky.len() != count || value.block.len() != count)
    {
        return Err(WritingError::Message(
            "chunk lighting height mismatch".into(),
        ));
    }
    let mut sky = vec![None; count];
    let mut block = vec![None; count];
    if let Some(overlay) = overlay {
        for index in 0..count {
            sky[index] = overlay.sky[index]
                .iter()
                .any(|value| *value != 0)
                .then(|| overlay.sky[index].to_vec());
            block[index] = overlay.block[index]
                .iter()
                .any(|value| *value != 0)
                .then(|| overlay.block[index].to_vec());
        }
    } else if let Some(sections) = document.get_list("sections") {
        for tag in sections {
            let pumpkin_nbt::tag::NbtTag::Compound(section) = tag else {
                continue;
            };
            let Some(y) = section.get_byte("Y") else {
                continue;
            };
            let Ok(index) = usize::try_from(i32::from(y) - min_y.div_euclid(16)) else {
                continue;
            };
            if index >= count {
                continue;
            }
            for (name, target) in [("SkyLight", &mut sky), ("BlockLight", &mut block)] {
                if let Some(array) = section.get(name).and_then(|tag| tag.extract_byte_array()) {
                    if array.len() != 2048 {
                        return Err(WritingError::Message("chunk lighting array length".into()));
                    }
                    target[index] = Some(array.iter().map(|value| *value as u8).collect());
                }
            }
        }
    }
    let mask = |arrays: &[Option<Vec<u8>>]| {
        arrays
            .iter()
            .enumerate()
            .fold(0_u64, |mask, (index, value)| {
                if value.is_some() {
                    mask | (1_u64 << (index + 1))
                } else {
                    mask
                }
            })
    };
    let sky_mask = mask(&sky);
    let block_mask = mask(&block);
    let all = if count == 62 {
        u64::MAX
    } else {
        (1_u64 << (count + 2)) - 1
    };
    let arrays = |values: Vec<Option<Vec<u8>>>| values.into_iter().flatten().collect();
    Ok(LightData::new(
        true,
        pumpkin_protocol::codec::bit_set::BitSet(Box::new([sky_mask as i64])),
        pumpkin_protocol::codec::bit_set::BitSet(Box::new([block_mask as i64])),
        pumpkin_protocol::codec::bit_set::BitSet(Box::new([(all ^ sky_mask) as i64])),
        pumpkin_protocol::codec::bit_set::BitSet(Box::new([(all ^ block_mask) as i64])),
        arrays(sky),
        arrays(block),
    ))
}

fn legacy_light(
    block: &[pumpkin_world::chunk::format::LightContainer],
    sky: &[pumpkin_world::chunk::format::LightContainer],
    min_y: i32,
) -> LightData {
    let collect = |sections: &[pumpkin_world::chunk::format::LightContainer]| {
        let mut mask = 0_u64;
        let mut arrays = Vec::new();
        for index in 0..18 {
            let source = (-min_y.div_euclid(16)) + index - 1;
            if let Ok(source) = usize::try_from(source) {
                if let Some(pumpkin_world::chunk::format::LightContainer::Full(bytes)) =
                    sections.get(source)
                {
                    mask |= 1 << index;
                    arrays.push(bytes.to_vec());
                }
            }
        }
        (mask, arrays)
    };
    let (block_mask, block_arrays) = collect(block);
    let (sky_mask, sky_arrays) = collect(sky);
    let all = (1_u64 << 18) - 1;
    LightData::new(
        true,
        pumpkin_protocol::codec::bit_set::BitSet(Box::new([sky_mask as i64])),
        pumpkin_protocol::codec::bit_set::BitSet(Box::new([block_mask as i64])),
        pumpkin_protocol::codec::bit_set::BitSet(Box::new([(all ^ sky_mask) as i64])),
        pumpkin_protocol::codec::bit_set::BitSet(Box::new([(all ^ block_mask) as i64])),
        sky_arrays,
        block_arrays,
    )
}

fn heightmaps_from_palettes(
    blocks: &crate::server::cluster_chunk_state::OwnedChunkBlocks,
) -> ChunkHeightmaps {
    use pumpkin_world::chunk::ChunkHeightmapType;
    let mut maps = pumpkin_world::chunk::ChunkHeightmaps::default();
    let kinds = [
        ChunkHeightmapType::WorldSurface,
        ChunkHeightmapType::MotionBlocking,
        ChunkHeightmapType::MotionBlockingNoLeaves,
    ];
    for z in 0..16 {
        for x in 0..16 {
            let mut found = [false; 3];
            for (section_index, section) in blocks.block_palettes().iter().enumerate().rev() {
                if section.has_only_air() {
                    continue;
                }
                for y in (0..16).rev() {
                    let state = pumpkin_data::BlockState::from_id(section.get(x, y, z));
                    for (index, kind) in kinds.iter().enumerate() {
                        if !found[index] && kind.is_opaque(state) {
                            maps.set(
                                *kind,
                                x as i32,
                                z as i32,
                                blocks.min_y() + section_index as i32 * 16 + y as i32,
                                blocks.min_y(),
                            );
                            found[index] = true;
                        }
                    }
                    if found.iter().all(|value| *value) {
                        break;
                    }
                }
                if found.iter().all(|value| *value) {
                    break;
                }
            }
            for (index, kind) in kinds.iter().enumerate() {
                if !found[index] {
                    maps.set(
                        *kind,
                        x as i32,
                        z as i32,
                        blocks.min_y() - 1,
                        blocks.min_y(),
                    );
                }
            }
        }
    }
    ChunkHeightmaps {
        world_surface: maps.world_surface.map(Vec::from),
        motion_blocking: maps.motion_blocking.map(Vec::from),
        motion_blocking_no_leaves: maps.motion_blocking_no_leaves.map(Vec::from),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn owned_palette_encoder_preserves_recovered_client_chunk_bytes() {
        let chunk = pumpkin_world::chunk::ChunkData::empty_sync(3, -2);
        chunk
            .section
            .set_block_absolute_y(0, 64, 0, pumpkin_data::Block::STONE.default_state.id);
        chunk
            .section
            .set_block_absolute_y(1, 80, 1, pumpkin_data::Block::WATER.default_state.id);
        chunk.section.set_block_absolute_y(
            2,
            80,
            2,
            pumpkin_data::Block::OAK_LEAVES.default_state.id,
        );
        *chunk.heightmap.lock().unwrap() = chunk.calculate_heightmap();
        let blocks = crate::server::cluster_chunk_state::OwnedChunkBlocks::from_chunk(&chunk);
        let position = pumpkin_cluster::protocol::ChunkKey::new(
            pumpkin_cluster::protocol::WorldId::new(0, 1),
            3,
            -2,
        );
        let image = pumpkin_world::level::cluster_encode_snapshot(&chunk);
        let image = pumpkin_world::level::OwnedChunkImage::decode(&image).unwrap();
        let document = &image.document().root_tag;
        for version in [
            JavaMinecraftVersion::V_1_7_2,
            JavaMinecraftVersion::V_1_8,
            JavaMinecraftVersion::V_1_9_3,
            JavaMinecraftVersion::V_1_14_4,
            JavaMinecraftVersion::V_1_17_1,
            JavaMinecraftVersion::V_1_18_2,
            JavaMinecraftVersion::V_1_21_5,
            JavaMinecraftVersion::V_26_2,
        ] {
            let mut expected = Vec::new();
            expected
                .write_var_int(&VarInt(super::super::CChunkData::to_id(version)))
                .unwrap();
            super::super::CChunkData(&chunk)
                .write_packet_data(&mut expected, &version)
                .unwrap();
            let (actual, light) = encode(position, document, &blocks, &[], None, version).unwrap();
            assert_eq!(actual.as_ref(), expected.as_slice(), "{version:?}");
            if version >= JavaMinecraftVersion::V_1_14 && version < JavaMinecraftVersion::V_1_18 {
                use crate::net::java::chunk_data::ChunkLightExt;
                let packet =
                    pumpkin_protocol::java::client::play::CLightUpdate::from_chunk(&chunk, version)
                        .unwrap();
                let mut expected_light = Vec::new();
                expected_light
                    .write_var_int(&VarInt(
                        pumpkin_protocol::java::client::play::CLightUpdate::to_id(version),
                    ))
                    .unwrap();
                packet
                    .write_packet_data(&mut expected_light, &version)
                    .unwrap();
                assert_eq!(
                    light.unwrap().as_ref(),
                    expected_light.as_slice(),
                    "{version:?}"
                );
            }
        }
    }
}

pub(crate) fn presentation_light(
    presentation: &crate::server::cluster_chunk_presentation::ChunkPresentation,
    version: JavaMinecraftVersion,
) -> Result<LightData, WritingError> {
    if !presentation.admission_lighting_valid && presentation.lighting.is_none() {
        return Err(WritingError::Message(
            "owned chunk lighting not ready".into(),
        ));
    }
    let count = presentation.blocks.block_palettes().len();
    let light = light_from_document(
        &presentation.image.document().root_tag,
        presentation.blocks.min_y(),
        count,
        presentation.lighting.as_deref(),
    )?;
    if version >= JavaMinecraftVersion::V_1_18 {
        return Ok(light);
    }
    let containers = |mask: &pumpkin_protocol::codec::bit_set::BitSet, arrays: &[Vec<u8>]| {
        let mut source = arrays.iter();
        (0..count)
            .map(|index| {
                if mask.get_bit(index + 1) {
                    pumpkin_world::chunk::format::LightContainer::Full(
                        source
                            .next()
                            .cloned()
                            .unwrap_or_else(|| vec![0; 2048])
                            .into_boxed_slice(),
                    )
                } else {
                    pumpkin_world::chunk::format::LightContainer::Empty(0)
                }
            })
            .collect::<Vec<_>>()
    };
    Ok(legacy_light(
        &containers(&light.block_light_mask, &light.block_light_arrays),
        &containers(&light.sky_light_mask, &light.sky_light_arrays),
        presentation.blocks.min_y(),
    ))
}
