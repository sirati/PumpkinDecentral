

use crate::actor::ActorId;
use crate::block_action::BlockActionIntent;
use crate::completion::ActionKey;
use crate::controller::{ControllerActorIntent, ControllerFrame, ViewIntent};
use crate::entity_transition::EntityChunkTransition;
use crate::global_admin::GlobalAdminAction;
use crate::inventory::{InventoryCell, InventoryOp, InventoryTarget, SemanticInventoryOp};
use crate::item_action::{
    ActorCombatState, ActorFoodState, ItemAction, ItemCooldownState, ItemProjectileSpawn,
    ItemUseState,
};
use crate::player_admin::PlayerAdministrativeAction;
use crate::protocol::{
    BlockPos, BreakBlockUpdate, ChunkKey, PlaceBlockUpdate, PlayerChunkTransition,
};
use crate::time::TickStamp;
use crate::world_delta::AdministrativeDelta;

#[derive(Debug, Clone)]
pub enum ActionScope {
    Chunks(Vec<ChunkKey>),
    Global,
}

#[derive(Debug, Clone)]
pub enum ActionPrecondition {
    Block { chunk: ChunkKey, position: BlockPos, state: u16, revision: u64 },
    Inventory { target: InventoryTarget, revision: u64, cell: InventoryCell },
    Food(ActorFoodState),
    Combat(ActorCombatState),
    Use { actor: ActorId, state: Option<ItemUseState> },
    Cooldown { actor: ActorId, group: String, state: Option<ItemCooldownState> },
    Controller { actor: ActorId, revision: u64, frame: ControllerFrame },
}

#[derive(Debug, Clone)]
pub enum ActionEffect {
    Block { chunk: ChunkKey, position: BlockPos, before: u16, after: u16 },
    Inventory(SemanticInventoryOp),
    Food { before: ActorFoodState, after: ActorFoodState },
    Combat { before: ActorCombatState, after: ActorCombatState },
    Use { actor: ActorId, before: Option<ItemUseState>, after: Option<ItemUseState> },
    Cooldown { actor: ActorId, before: Option<ItemCooldownState>, after: Option<ItemCooldownState> },
    Controller { actor: ActorId, before: ControllerFrame, after: ControllerFrame },
    View { actor: ActorId, before: Option<ViewIntent>, after: Option<ViewIntent> },
    PlayerAdministrative(PlayerAdministrativeAction),
    PlayerChunkTransition(PlayerChunkTransition),
    EntityChunkTransition(EntityChunkTransition),
    WorldAdministrative(AdministrativeDelta),
    ProjectileSpawn(ItemProjectileSpawn),
    Statistic { actor: crate::identity::GlobalPlayerId, category: i32, statistic: i32, amount: i32 },
}

#[derive(Debug, Clone)]
pub enum ActionPayload {
    Controller(ControllerActorIntent),
    View(ViewIntent),
    Block(BlockActionIntent),
    Break(BreakBlockUpdate),
    Place(PlaceBlockUpdate),
    Inventory(InventoryOp),
    Item(ItemAction),
    PlayerAdministrative(PlayerAdministrativeAction),
    PlayerChunkTransition(PlayerChunkTransition),
    EntityChunkTransition(EntityChunkTransition),
    WorldAdministrative(AdministrativeDelta),
    GlobalAdministrative(GlobalAdminAction),
}

#[derive(Debug, Clone)]
pub struct CanonicalAction {
    pub key: ActionKey,
    pub tick: TickStamp,
    pub actor: ActorId,
    pub scope: ActionScope,
    pub preconditions: Vec<ActionPrecondition>,
    pub forward: Vec<ActionEffect>,
    pub undo: Vec<ActionEffect>,
    pub payload: ActionPayload,
}

impl CanonicalAction {
    pub fn affected(&self) -> &[ChunkKey] {
        match &self.scope {
            ActionScope::Chunks(chunks) => chunks,
            ActionScope::Global => &[],
        }
    }
}
