use crate::{
    bedrock::client::GameType,
    codec::{var_long::VarLong, var_ulong::VarULong},
    serial::PacketWrite,
};
use pumpkin_macros::packet;

#[derive(PacketWrite)]
#[packet(151)]
pub struct CUpdatePlayerGameType {
    pub player_game_type: GameType,
    pub target_player: VarLong,
    pub tick: VarULong,
}
