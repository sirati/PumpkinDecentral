// Protocol 2169. Mojang schema is unchanged from 2168:
// https://mojang.github.io/bedrock-protocol-docs/1.26.44/packets/play-sound-packet/
use pumpkin_macros::packet;
use pumpkin_util::math::{position::BlockPos, vector3::Vector3};

use crate::{codec::var_int::VarInt, serial::PacketWrite};

#[derive(PacketWrite)]
#[packet(86)]
pub struct CPlaySound {
    pub name: String,
    pub position: BlockPos,
    pub volume: f32,
    pub pitch: f32,
    pub loop_count: VarInt,
    pub server_sound_handle: Option<u64>,
}

impl CPlaySound {
    pub fn at(name: String, position: Vector3<f64>, volume: f32, pitch: f32) -> Option<Self> {
        let coordinates = [position.x, position.y, position.z];
        if name.is_empty()
            || name.len() > 256
            || !volume.is_finite()
            || volume < 0.0
            || !pitch.is_finite()
            || pitch < 0.0
            || coordinates
                .iter()
                .any(|coordinate| !coordinate.is_finite() || coordinate.abs() > 30_000_000.0)
        {
            return None;
        }
        // The wire BlockPos represents eighths of a block.
        let [x, y, z] = coordinates.map(|coordinate| (coordinate * 8.0).floor() as i32);
        Some(Self {
            name,
            position: BlockPos::new(x, y, z),
            volume,
            pitch,
            loop_count: VarInt(0),
            server_sound_handle: None,
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn one_shot_sound_serializes_fixed_point_position_and_optional_handle() {
        let packet =
            CPlaySound::at("test".into(), Vector3::new(-1.25, 2.5, 0.0), 0.5, 1.0).unwrap();
        assert_eq!(<CPlaySound as crate::Packet>::PACKET_ID, 86);
        let mut encoded = Vec::new();
        packet.write(&mut encoded).unwrap();
        assert_eq!(&encoded[..8], &[4, b't', b'e', b's', b't', 19, 40, 0]);
        assert_eq!(&encoded[8..12], &0.5_f32.to_le_bytes());
        assert_eq!(&encoded[12..16], &1.0_f32.to_le_bytes());
        assert_eq!(&encoded[16..], &[0, 0]);
        assert!(
            CPlaySound::at("test".into(), Vector3::new(f64::NAN, 0.0, 0.0), 1.0, 1.0).is_none()
        );
    }
}
