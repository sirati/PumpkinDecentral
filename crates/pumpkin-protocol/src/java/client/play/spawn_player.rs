use pumpkin_data::packet::clientbound::play::SPAWN_PLAYER;
use pumpkin_macros::java_packet;
use pumpkin_util::{math::vector3::Vector3, version::JavaMinecraftVersion};

use crate::{
    ClientPacket, Property, VarInt,
    ser::{NetworkWriteExt, WritingError},
};

#[java_packet(SPAWN_PLAYER)]
pub struct CSpawnPlayer<'a> {
    pub entity_id: VarInt,
    pub uuid: uuid::Uuid,
    pub name: &'a str,
    pub properties: &'a [Property],
    pub position: Vector3<f64>,
    pub yaw: f32,
    pub pitch: f32,
}

impl ClientPacket for CSpawnPlayer<'_> {
    fn write_packet_data(
        &self,
        mut write: impl std::io::Write,
        version: &JavaMinecraftVersion,
    ) -> Result<(), WritingError> {
        if *version >= JavaMinecraftVersion::V_1_20_2 {
            return Err(WritingError::UnsupportedVersion(*version));
        }
        let coordinates = [self.position.x, self.position.y, self.position.z];
        if self.uuid.is_nil()
            || !self.yaw.is_finite()
            || !self.pitch.is_finite()
            || coordinates
                .iter()
                .any(|value| !value.is_finite() || value.abs() > 30_000_000.0)
        {
            return Err(WritingError::Message("invalid player spawn".into()));
        }
        write.write_var_int(&self.entity_id)?;
        if *version >= JavaMinecraftVersion::V_1_8 {
            write.write_uuid(&self.uuid)?;
        } else {
            write.write_string(&if *version < JavaMinecraftVersion::V_1_7_6 {
                self.uuid.simple().to_string()
            } else {
                self.uuid.hyphenated().to_string()
            })?;
            write.write_string_bounded(self.name, 16)?;
            if *version >= JavaMinecraftVersion::V_1_7_6 {
                write
                    .write_var_int(&VarInt(i32::try_from(self.properties.len()).map_err(
                        |_| WritingError::Message("player properties overflow".into()),
                    )?))?;
                for property in self.properties {
                    write.write_string(&property.name)?;
                    write.write_string(&property.value)?;
                    write.write_string(property.signature.as_deref().unwrap_or(""))?;
                }
            }
        }
        for coordinate in coordinates {
            if *version < JavaMinecraftVersion::V_1_9 {
                write.write_i32_be((coordinate * 32.0).floor() as i32)?;
            } else {
                write.write_f64_be(coordinate)?;
            }
        }
        write.write_u8((self.yaw.rem_euclid(360.0) * 256.0 / 360.0).floor() as u8)?;
        write.write_u8((self.pitch.rem_euclid(360.0) * 256.0 / 360.0).floor() as u8)?;
        if *version < JavaMinecraftVersion::V_1_9 {
            write.write_i16_be(0)?;
        }
        if *version < JavaMinecraftVersion::V_1_15 {
            write.write_u8(if *version < JavaMinecraftVersion::V_1_9 {
                0x7f
            } else {
                0xff
            })?;
        }
        Ok(())
    }
}
