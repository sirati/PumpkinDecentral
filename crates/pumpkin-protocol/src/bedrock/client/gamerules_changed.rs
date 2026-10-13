use pumpkin_macros::packet;

use crate::{codec::var_uint::VarUInt, serial::PacketWrite};

#[derive(PacketWrite)]
#[packet(72)]
pub struct CGamerulesChanged {
    pub rule_data: Vec<GameRule>,
}

#[derive(PacketWrite)]
pub struct GameRule {
    pub rule_name: String,
    pub rule_can_be_modified: bool,
    pub rule_value: RuleValue,
}

pub enum RuleValue {
    Null,
    Bool(bool),
    Int(u32),
    Float(f32),
}

impl PacketWrite for RuleValue {
    fn write<W: std::io::prelude::Write>(&self, writer: &mut W) -> Result<(), std::io::Error> {
        match self {
            Self::Null => VarUInt(0).write(writer),
            Self::Bool(value) => { VarUInt(1).write(&mut *writer)?; value.write(writer) }
            Self::Int(value) => { VarUInt(2).write(&mut *writer)?; VarUInt(*value).write(writer) }
            Self::Float(value) => { VarUInt(3).write(&mut *writer)?; value.write(writer) }
        }
    }
}
