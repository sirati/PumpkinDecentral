use pumpkin_data::packet::clientbound::play::SPAWN_EXPERIENCE_ORB;
use pumpkin_macros::java_packet;
use pumpkin_util::math::vector3::Vector3;
use pumpkin_util::version::JavaMinecraftVersion;

use crate::{
    ClientPacket, VarInt,
    ser::{NetworkWriteExt, WritingError},
};

#[java_packet(SPAWN_EXPERIENCE_ORB)]
pub struct CSpawnExperienceOrb {
    pub entity_id: VarInt,
    pub position: Vector3<f64>,
    pub amount: i16,
}

impl ClientPacket for CSpawnExperienceOrb {
    fn write_packet_data(
        &self,
        mut write: impl std::io::Write,
        version: &JavaMinecraftVersion,
    ) -> Result<(), WritingError> {
        if *version >= JavaMinecraftVersion::V_1_21_5 {
            return Err(WritingError::UnsupportedVersion(*version));
        }
        let coordinates = [self.position.x, self.position.y, self.position.z];
        if self.amount <= 0
            || coordinates
                .iter()
                .any(|value| !value.is_finite() || value.abs() > 30_000_000.0)
        {
            return Err(WritingError::Message("invalid experience orb".into()));
        }
        write.write_var_int(&self.entity_id)?;
        for coordinate in coordinates {
            if *version < JavaMinecraftVersion::V_1_9 {
                write.write_i32_be((coordinate * 32.0).floor() as i32)?;
            } else {
                write.write_f64_be(coordinate)?;
            }
        }
        write.write_i16_be(self.amount)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::ser::NetworkReadExt;

    #[test]
    fn experience_orb_spawn_preserves_coordinates_and_value() {
        let packet = CSpawnExperienceOrb {
            entity_id: VarInt(300),
            position: Vector3::new(-1.25, 80.5, 3.75),
            amount: 2477,
        };
        for version in [JavaMinecraftVersion::V_1_8, JavaMinecraftVersion::V_1_21_4] {
            let mut bytes = Vec::new();
            packet.write_packet_data(&mut bytes, &version).unwrap();
            let mut read = bytes.as_slice();
            assert_eq!(read.get_var_int().unwrap(), packet.entity_id);
            for coordinate in [packet.position.x, packet.position.y, packet.position.z] {
                let actual = if version < JavaMinecraftVersion::V_1_9 {
                    f64::from(read.get_i32_be().unwrap()) / 32.0
                } else {
                    read.get_f64_be().unwrap()
                };
                assert_eq!(actual, coordinate);
            }
            assert_eq!(read.get_i16_be().unwrap(), packet.amount);
            assert!(read.is_empty());
        }
        let mut bytes = Vec::new();
        assert!(
            packet
                .write_packet_data(&mut bytes, &JavaMinecraftVersion::V_1_21_5)
                .is_err()
        );
        assert!(bytes.is_empty());
    }
}
