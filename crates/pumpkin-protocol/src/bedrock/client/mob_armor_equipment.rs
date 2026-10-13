use crate::{
    bedrock::network_item::NetworkItemStackDescriptor, codec::var_ulong::VarULong,
    serial::PacketWrite,
};
use pumpkin_macros::packet;

#[derive(PacketWrite, Debug)]
#[packet(32)]
pub struct CMobArmorEquipment {
    pub target_runtime_id: VarULong,
    pub head: NetworkItemStackDescriptor,
    pub torso: NetworkItemStackDescriptor,
    pub legs: NetworkItemStackDescriptor,
    pub feet: NetworkItemStackDescriptor,
    pub body: NetworkItemStackDescriptor,
}
