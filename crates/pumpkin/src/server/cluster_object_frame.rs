use std::{collections::BTreeMap, io::Cursor};

use pumpkin_cluster::{entity_body::EntityBody, object_state::FrameState, protocol::ChunkKey};
use pumpkin_nbt::{
    Nbt,
    compound::NbtCompound,
    deserializer::{NbtReadHelper, NbtReadHelperBedrock},
};
use pumpkin_protocol::bedrock::{
    client::{CBlockActorData, CUpdateBlock},
    packet_encoder::BedrockBatchEncoder,
};

pub(super) struct BedrockFramePalette(Result<BTreeMap<(bool, u8), u32>, String>);

impl Default for BedrockFramePalette {
    fn default() -> Self {
        Self(Self::load())
    }
}

impl BedrockFramePalette {
    fn load() -> Result<BTreeMap<(bool, u8), u32>, String> {
        let bytes = include_bytes!("../../../../assets/bedrock/block_states.nbt");
        let mut reader = NbtReadHelperBedrock::new(Cursor::new(bytes.as_slice()));
        let mut states = BTreeMap::new();
        let mut runtime = 0_u32;
        while reader.reader().position() < bytes.len() as u64 {
            let root = Nbt::read(&mut reader).map_err(|error| error.to_string())?;
            let glow = match root.get_string("name") {
                Some("minecraft:frame") => Some(false),
                Some("minecraft:glow_frame") => Some(true),
                _ => None,
            };
            if let Some(glow) = glow {
                let properties = root
                    .get_compound("states")
                    .ok_or("frame palette states absent")?;
                if properties.get_byte("item_frame_map_bit") == Some(0)
                    && properties.get_byte("item_frame_photo_bit") == Some(0)
                {
                    let facing = properties
                        .get_int("facing_direction")
                        .and_then(|value| u8::try_from(value).ok())
                        .filter(|value| *value < 6)
                        .ok_or("frame palette facing invalid")?;
                    if states.insert((glow, facing), runtime).is_some() {
                        return Err("duplicate frame palette state".into());
                    }
                }
            }
            runtime = runtime
                .checked_add(1)
                .ok_or("frame palette runtime overflow")?;
        }
        if states.len() != 12 {
            return Err("incomplete frame palette".into());
        }
        Ok(states)
    }

    fn runtime(&self, frame: &FrameState) -> Result<u32, String> {
        self.0
            .as_ref()
            .map_err(Clone::clone)?
            .get(&(frame.glow, frame.facing))
            .copied()
            .ok_or_else(|| "frame palette state unavailable".into())
    }
}

fn data(frame: &FrameState) -> Result<NbtCompound, String> {
    let mut root = NbtCompound::new();
    root.put_string(
        "id",
        if frame.glow {
            "GlowItemFrame"
        } else {
            "ItemFrame"
        }
        .into(),
    );
    root.put_int("x", frame.attachment.x);
    root.put_int("y", frame.attachment.y);
    root.put_int("z", frame.attachment.z);
    root.put_byte("isMovable", 1);
    root.put_float("ItemRotation", f32::from(frame.rotation) * 45.0);
    root.put_float("ItemDropChance", f32::from_bits(frame.drop_chance));
    if !frame.item.is_empty() {
        let stack = super::cluster_invsee::item_stack_from_inventory_cell(&frame.item)
            .map_err(|error| format!("{error:?}"))?;
        let mapping =
            pumpkin_data::item::JavaToBedrockItemMapping::from_java_item_id(stack.item.id)
                .ok_or("frame item mapping absent")?;
        let translated = pumpkin_protocol::bedrock::network_item::ItemStackWrapper::from(&stack);
        let mut item = NbtCompound::new();
        item.put_string("Name", mapping.bedrock_item.registry_key.into());
        item.put_byte(
            "Count",
            i8::try_from(translated.stack_size).map_err(|error| error.to_string())?,
        );
        item.put_short(
            "Damage",
            i16::try_from(translated.aux_value.0).map_err(|error| error.to_string())?,
        );
        item.put_compound("tag", translated.nbt_data.root_tag);
        root.put_compound("Item", item);
    }
    Ok(root)
}

pub(super) fn project(
    body: &EntityBody,
    frame: &FrameState,
    removed: bool,
    blocks: &super::cluster_chunk_state::OwnedChunkStore,
    palette: &BedrockFramePalette,
    encoder: &BedrockBatchEncoder,
) -> Result<Vec<bytes::Bytes>, String> {
    let position = pumpkin_util::math::position::BlockPos::new(
        frame.attachment.x,
        frame.attachment.y,
        frame.attachment.z,
    );
    let runtime = if removed {
        let chunk = ChunkKey::new(
            body.spatial.world,
            frame.attachment.x.div_euclid(16),
            frame.attachment.z.div_euclid(16),
        );
        let state = blocks
            .block(chunk, frame.attachment)
            .map_err(|error| format!("frame restore block unavailable: {error:?}"))?;
        pumpkin_data::BlockState::to_be_network_id(state) as u32
    } else {
        palette.runtime(frame)?
    };
    let mut packets = vec![
        encoder
            .serialize_packet(&CUpdateBlock::new(position, runtime))
            .map_err(|error| error.to_string())?,
    ];
    if !removed {
        packets.push(
            encoder
                .serialize_packet(&CBlockActorData::new(position, data(frame)?))
                .map_err(|error| error.to_string())?,
        );
    }
    Ok(packets)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn embedded_palette_has_distinct_normal_and_glowing_frames_for_every_face() {
        let palette = BedrockFramePalette::load().unwrap();
        assert_eq!(palette.len(), 12);
        let unique = palette
            .values()
            .copied()
            .collect::<std::collections::BTreeSet<_>>();
        assert_eq!(unique.len(), 12);
        for facing in 0..6 {
            assert_ne!(palette[&(false, facing)], palette[&(true, facing)]);
        }
    }

    #[test]
    fn empty_frame_data_clears_item_and_preserves_rotation_and_glow_kind() {
        let frame = FrameState {
            attachment: pumpkin_cluster::protocol::BlockPos {
                x: -1,
                y: 80,
                z: 16,
            },
            facing: 5,
            glow: true,
            item: pumpkin_cluster::inventory::InventoryCell::empty(
                pumpkin_cluster::inventory::InvLoc::new(pumpkin_cluster::inventory::INV_MAIN, 0),
            ),
            rotation: 7,
            drop_chance: 0.25_f32.to_bits(),
            invisible: false,
            fixed: false,
        };
        let tags = data(&frame).unwrap();
        assert_eq!(tags.get_string("id"), Some("GlowItemFrame"));
        assert_eq!(tags.get_float("ItemRotation"), Some(315.0));
        assert_eq!(tags.get_float("ItemDropChance"), Some(0.25));
        assert!(tags.get_compound("Item").is_none());
        assert_eq!(tags.get_int("x"), Some(-1));
        assert_eq!(tags.get_int("z"), Some(16));
    }
}
