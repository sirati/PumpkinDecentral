use crate::{
    codec::{var_int::VarInt, var_long::VarLong, var_ulong::VarULong},
    serial::PacketWrite,
};
use pumpkin_macros::packet;
use pumpkin_util::math::vector3::Vector3;

#[derive(PacketWrite)]
#[packet(22)]
pub struct CAddPainting {
    pub target_actor_id: VarLong,
    pub target_runtime_id: VarULong,
    pub position: Vector3<f32>,
    pub direction: VarInt,
    pub motif: String,
}
