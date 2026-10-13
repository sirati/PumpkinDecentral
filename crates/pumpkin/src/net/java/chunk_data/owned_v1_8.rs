use super::util::get_light_bytes;
use pumpkin_data::block_state_remap::remap_block_state_for_version;
use pumpkin_nbt::compound::NbtCompound;
use pumpkin_protocol::codec::var_int::VarInt;
use pumpkin_protocol::java::client::play::ChunkHeightmaps;
use pumpkin_protocol::ser::NetworkWriteExt;
use pumpkin_protocol::ser::WritingError;
use pumpkin_util::math::position::BlockPos;
use pumpkin_util::version::JavaMinecraftVersion;
use pumpkin_world::chunk::format::LightContainer;
use pumpkin_world::chunk::palette::{BiomePalette, BlockPalette};
use std::io::Write;

pub fn write_chunk_data(
    x: i32,
    z: i32,
    min_y: i32,
    block_sections: &[BlockPalette],
    biome_sections: &[BiomePalette],
    block_light: &[LightContainer],
    sky_light: &[LightContainer],
    _heightmaps: &ChunkHeightmaps,
    _block_entities: &[(BlockPos, NbtCompound)],
    mut write: impl Write,
    version: &JavaMinecraftVersion,
) -> Result<(), WritingError> {
    write.write_i32_be(x)?;
    write.write_i32_be(z)?;
    write.write_bool(true)?;

    let base_section = (0 - min_y).max(0) as usize / 16;

    let mut chunk_mask = 0u16;
    for i in 0..16 {
        let section_idx = base_section + i;
        if section_idx < block_sections.len() && !block_sections[section_idx].has_only_air() {
            chunk_mask |= 1 << i;
        }
    }
    write.write_u16_be(chunk_mask)?;

    let mut data_buf = Vec::new();
    for i in 0..16 {
        if (chunk_mask & (1 << i)) != 0 {
            let section_idx = base_section + i;
            let section = &block_sections[section_idx];
            for y in 0..16 {
                for z in 0..16 {
                    for x in 0..16 {
                        let state_id = section.get(x, y, z);
                        let remapped = remap_block_state_for_version(state_id.as_u16(), *version);
                        data_buf.write_all(&remapped.to_le_bytes())?;
                    }
                }
            }
        }
    }

    for i in 0..16 {
        if (chunk_mask & (1 << i)) != 0 {
            let section_idx = base_section + i;
            let block_light = get_light_bytes(block_light.get(section_idx), 0);
            data_buf.write_slice(&block_light)?;
        }
    }

    for i in 0..16 {
        if (chunk_mask & (1 << i)) != 0 {
            let section_idx = base_section + i;
            let sky_light = get_light_bytes(sky_light.get(section_idx), 15);
            data_buf.write_slice(&sky_light)?;
        }
    }

    for z in 0..16 {
        for x in 0..16 {
            let biome_id = if base_section < biome_sections.len() {
                biome_sections[base_section].get(x / 4, 0, z / 4)
            } else {
                0
            };
            data_buf.write_u8(biome_id)?;
        }
    }

    write.write_var_int(&VarInt(data_buf.len() as i32))?;
    write.write_slice(&data_buf)?;
    Ok(())
}
