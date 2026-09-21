use core::cmp::Ordering;

use serde::{Deserialize, Serialize};

use crate::identity::{ActionActor, ActionSeq};
use crate::interact::{
    AtomicInteractDecision, AtomicInteractUndo, InteractCodecError, InteractUpdate,
    judge_atomic_interact,
};
use crate::inventory::{InvCells, InvVerdict, replay};
use crate::order::order_action_actors;
use crate::protocol::{BlockPos, BlockUndo, ChunkAddr};
use crate::time::TickStamp;
use crate::world_delta::{
    BlockDelta, WorldDelta, WorldDeltaCodecError, apply_fallible_edit, apply_random_tick_edit,
    decode_frame as decode_delta_frame, encode_frame as encode_delta_frame, is_world_delta_frame,
};

pub const INTERACT_FRAME_MAGIC: [u8; 4] = [0x49, 0x4E, 0x54, 0x52];
pub const WORLD_DRIVEN_ACTION_MAGIC: [u8; 4] = [0x57, 0x44, 0x41, 0x31];

#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord)]
pub enum WorldDrivenKind {
    RandomTick,
    Redstone,
    Explosion,
    CausalRedstone,
    TransactionalExplosion,
    Door,
    Trapdoor,
    EndEye,
    Anchor,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum WorldDrivenFrame {
    Delta(WorldDelta),
    Interact(InteractUpdate),
}

impl WorldDrivenFrame {
    #[must_use]
    pub fn tick(&self) -> TickStamp {
        match self {
            Self::Delta(delta) => delta.tick(),
            Self::Interact(interact) => interact.tick(),
        }
    }

    #[must_use]
    pub fn chunk(&self) -> ChunkAddr {
        match self {
            Self::Delta(delta) => delta.chunk(),
            Self::Interact(interact) => interact.chunk(),
        }
    }

    #[must_use]
    pub fn kind(&self) -> WorldDrivenKind {
        match self {
            Self::Delta(WorldDelta::RandomTick(_)) => WorldDrivenKind::RandomTick,
            Self::Delta(WorldDelta::Redstone(_)) => WorldDrivenKind::Redstone,
            Self::Delta(WorldDelta::Explosion(_)) => WorldDrivenKind::Explosion,
            Self::Delta(WorldDelta::CausalRedstone(_)) => WorldDrivenKind::CausalRedstone,
            Self::Delta(WorldDelta::TransactionalExplosion(_)) => {
                WorldDrivenKind::TransactionalExplosion
            }
            Self::Interact(interact) => match interact {
                InteractUpdate::Door(_) => WorldDrivenKind::Door,
                InteractUpdate::Trapdoor(_) => WorldDrivenKind::Trapdoor,
                InteractUpdate::EndEye(_) => WorldDrivenKind::EndEye,
                InteractUpdate::Anchor(_) => WorldDrivenKind::Anchor,
                InteractUpdate::Atomic(atomic) => match atomic.kind {
                    crate::interact::AtomicInteractKind::Door => WorldDrivenKind::Door,
                    crate::interact::AtomicInteractKind::Trapdoor => WorldDrivenKind::Trapdoor,
                    crate::interact::AtomicInteractKind::EndEye => WorldDrivenKind::EndEye,
                    crate::interact::AtomicInteractKind::Anchor => WorldDrivenKind::Anchor,
                },
            },
        }
    }

    #[must_use]
    pub fn edits(&self) -> Vec<BlockDelta> {
        match self {
            Self::Delta(WorldDelta::RandomTick(delta)) => delta.edits.clone(),
            Self::Delta(WorldDelta::Redstone(delta)) => delta.edits.clone(),
            Self::Delta(WorldDelta::Explosion(delta)) => delta.edits.clone(),
            Self::Delta(WorldDelta::CausalRedstone(delta)) => delta.edits.clone(),
            Self::Delta(WorldDelta::TransactionalExplosion(delta)) => delta.edits.clone(),
            Self::Interact(InteractUpdate::Atomic(atomic)) => atomic
                .edits
                .iter()
                .map(|edit| BlockDelta {
                    pos: edit.pos,
                    old_state: edit.expected_old_state,
                    new_state: edit.new_state,
                })
                .collect(),
            Self::Interact(interact) => vec![BlockDelta {
                pos: interact.pos(),
                old_state: interact.expected_old_state(),
                new_state: interact.new_state(),
            }],
        }
    }

    #[must_use]
    pub fn dependencies(&self) -> &[crate::world_delta::WorldDependency] {
        match self {
            Self::Delta(WorldDelta::CausalRedstone(delta)) => &delta.dependencies,
            _ => &[],
        }
    }

    #[must_use]
    pub fn transactional_explosion(
        &self,
    ) -> Option<&crate::world_delta::TransactionalExplosionUpdate> {
        match self {
            Self::Delta(WorldDelta::TransactionalExplosion(update)) => Some(update),
            _ => None,
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum WorldDrivenUndo {
    Blocks(Vec<(BlockPos, BlockUndo)>),
    Atomic(AtomicInteractUndo),
    TransactionalExplosion(crate::world_delta::TransactionalExplosionUndo),
}

impl WorldDrivenUndo {
    #[must_use]
    pub fn blocks(&self) -> Vec<(BlockPos, BlockUndo)> {
        match self {
            Self::Blocks(blocks) => blocks.clone(),
            Self::Atomic(atomic) => atomic.edits.clone(),
            Self::TransactionalExplosion(explosion) => explosion.blocks.clone(),
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct WorldDrivenAction {
    pub actor: ActionActor,
    pub seq: ActionSeq,
    pub tick: TickStamp,
    pub chunk: ChunkAddr,
    pub kind: WorldDrivenKind,
    pub bytes: Vec<u8>,
    pub undo: WorldDrivenUndo,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum WorldDrivenCodecError {
    Delta(WorldDeltaCodecError),
    Interact(InteractCodecError),
    Wire(postcard::Error),
    Unknown,
}

impl core::fmt::Display for WorldDrivenCodecError {
    fn fmt(&self, formatter: &mut core::fmt::Formatter<'_>) -> core::fmt::Result {
        match self {
            Self::Delta(error) => error.fmt(formatter),
            Self::Interact(error) => error.fmt(formatter),
            Self::Wire(error) => error.fmt(formatter),
            Self::Unknown => formatter.write_str("decode world-driven frame: unknown frame"),
        }
    }
}

impl std::error::Error for WorldDrivenCodecError {}

pub fn is_interact_frame(bytes: &[u8]) -> bool {
    bytes.len() > INTERACT_FRAME_MAGIC.len() && bytes.starts_with(&INTERACT_FRAME_MAGIC)
}

pub fn encode_interact_frame(update: &InteractUpdate) -> Result<Vec<u8>, InteractCodecError> {
    let mut bytes = Vec::with_capacity(INTERACT_FRAME_MAGIC.len());
    bytes.extend_from_slice(&INTERACT_FRAME_MAGIC);
    bytes.extend_from_slice(&crate::interact::encode_interact(update)?);
    Ok(bytes)
}

pub fn decode_interact_frame(bytes: &[u8]) -> Result<InteractUpdate, InteractCodecError> {
    if !is_interact_frame(bytes) {
        return Err(InteractCodecError {
            message: "decode interact frame: missing interact magic".to_owned(),
        });
    }
    crate::interact::decode_interact(&bytes[INTERACT_FRAME_MAGIC.len()..])
}

#[derive(Debug, Clone, Serialize, Deserialize)]
struct WorldDrivenWire {
    actor: ActionActor,
    seq: ActionSeq,
    frame: Vec<u8>,
}

fn undo_from_frame(frame: &WorldDrivenFrame) -> WorldDrivenUndo {
    match frame {
        WorldDrivenFrame::Interact(InteractUpdate::Atomic(atomic)) => {
            WorldDrivenUndo::Atomic(AtomicInteractUndo {
                edits: atomic
                    .edits
                    .iter()
                    .map(|edit| {
                        (
                            edit.pos,
                            BlockUndo {
                                old_state: edit.expected_old_state,
                                count_before: crate::interact::INTERACT_NO_INVENTORY,
                            },
                        )
                    })
                    .collect(),
                inventory: atomic
                    .inventory
                    .as_ref()
                    .map(|inventory| inventory.precondition.clone()),
            })
        }
        WorldDrivenFrame::Delta(WorldDelta::TransactionalExplosion(explosion)) => {
            WorldDrivenUndo::TransactionalExplosion(
                crate::world_delta::transactional_explosion_undo(explosion),
            )
        }
        _ => WorldDrivenUndo::Blocks(
            frame
                .edits()
                .into_iter()
                .map(|edit| {
                    (
                        edit.pos,
                        BlockUndo {
                            old_state: edit.old_state,
                            count_before: crate::world_delta::WORLD_DELTA_NO_INVENTORY,
                        },
                    )
                })
                .collect(),
        ),
    }
}

pub fn encode_action(
    actor: ActionActor,
    seq: ActionSeq,
    frame: &WorldDrivenFrame,
) -> Result<WorldDrivenAction, WorldDrivenCodecError> {
    let frame_bytes = match frame {
        WorldDrivenFrame::Delta(delta) => encode_delta_frame(delta).map_err(WorldDrivenCodecError::Delta)?,
        WorldDrivenFrame::Interact(interact) => {
            encode_interact_frame(interact).map_err(WorldDrivenCodecError::Interact)?
        }
    };
    let wire = WorldDrivenWire {
        actor,
        seq,
        frame: frame_bytes,
    };
    let mut bytes = Vec::with_capacity(WORLD_DRIVEN_ACTION_MAGIC.len());
    bytes.extend_from_slice(&WORLD_DRIVEN_ACTION_MAGIC);
    let bytes = postcard::to_extend(&wire, bytes).map_err(WorldDrivenCodecError::Wire)?;
    Ok(WorldDrivenAction {
        actor,
        seq,
        tick: frame.tick(),
        chunk: frame.chunk(),
        kind: frame.kind(),
        bytes,
        undo: undo_from_frame(frame),
    })
}

pub fn decode_action(bytes: &[u8]) -> Result<(WorldDrivenAction, WorldDrivenFrame), WorldDrivenCodecError> {
    if !bytes.starts_with(&WORLD_DRIVEN_ACTION_MAGIC) {
        return Err(WorldDrivenCodecError::Unknown);
    }
    let wire: WorldDrivenWire = postcard::from_bytes(&bytes[WORLD_DRIVEN_ACTION_MAGIC.len()..])
        .map_err(WorldDrivenCodecError::Wire)?;
    let frame = if is_world_delta_frame(&wire.frame) {
        WorldDrivenFrame::Delta(decode_delta_frame(&wire.frame).map_err(WorldDrivenCodecError::Delta)?)
    } else if is_interact_frame(&wire.frame) {
        WorldDrivenFrame::Interact(decode_interact_frame(&wire.frame).map_err(WorldDrivenCodecError::Interact)?)
    } else {
        return Err(WorldDrivenCodecError::Unknown);
    };
    let action = WorldDrivenAction {
        actor: wire.actor,
        seq: wire.seq,
        tick: frame.tick(),
        chunk: frame.chunk(),
        kind: frame.kind(),
        bytes: bytes.to_vec(),
        undo: undo_from_frame(&frame),
    };
    Ok((action, frame))
}

#[must_use]
pub fn order_actions(
    cluster_seed: u64,
    left: &(WorldDrivenAction, WorldDrivenFrame),
    right: &(WorldDrivenAction, WorldDrivenFrame),
) -> Ordering {
    order_action_actors(cluster_seed, left.0.tick, left.0.actor, right.0.actor)
        .then_with(|| left.0.seq.cmp(&right.0.seq))
        .then_with(|| left.0.kind.cmp(&right.0.kind))
        .then_with(|| left.0.chunk.cmp(&right.0.chunk))
        .then_with(|| left.0.bytes.cmp(&right.0.bytes))
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum WorldDrivenDecision {
    Accept { undo: WorldDrivenUndo },
    RejectInvalid,
    RejectBlock {
        pos: BlockPos,
        expected: u16,
        current: Option<u16>,
    },
    RejectInventory,
}

pub fn judge_action(
    frame: &WorldDrivenFrame,
    mut state_at: impl FnMut(BlockPos) -> Option<u16>,
    inventory: Option<&dyn InvCells>,
) -> WorldDrivenDecision {
    match frame {
        WorldDrivenFrame::Delta(WorldDelta::RandomTick(delta)) => {
            let mut undo = Vec::with_capacity(delta.edits.len());
            for edit in &delta.edits {
                let Some(current) = state_at(edit.pos) else {
                    return WorldDrivenDecision::RejectBlock {
                        pos: edit.pos,
                        expected: edit.old_state,
                        current: None,
                    };
                };
                if let crate::world_delta::RandomTickDecision::Apply { undo: one } =
                    apply_random_tick_edit(current, edit)
                {
                    undo.push((edit.pos, one));
                }
            }
            WorldDrivenDecision::Accept {
                undo: WorldDrivenUndo::Blocks(undo),
            }
        }
        WorldDrivenFrame::Delta(WorldDelta::Redstone(_))
        | WorldDrivenFrame::Delta(WorldDelta::Explosion(_)) => WorldDrivenDecision::RejectInvalid,
        WorldDrivenFrame::Delta(WorldDelta::CausalRedstone(delta)) if !delta.is_valid() => {
            WorldDrivenDecision::RejectInvalid
        }
        WorldDrivenFrame::Delta(WorldDelta::CausalRedstone(delta)) => {
            judge_fallible(&delta.edits, &mut state_at)
        }
        WorldDrivenFrame::Delta(WorldDelta::TransactionalExplosion(_)) => {
            WorldDrivenDecision::RejectInvalid
        }
        WorldDrivenFrame::Interact(InteractUpdate::Atomic(atomic)) => {
            match judge_atomic_interact(atomic, state_at, inventory) {
                AtomicInteractDecision::Accept { undo } => WorldDrivenDecision::Accept {
                    undo: WorldDrivenUndo::Atomic(undo),
                },
                AtomicInteractDecision::RejectInvalid => WorldDrivenDecision::RejectInvalid,
                AtomicInteractDecision::RejectBlock {
                    pos,
                    expected,
                    current,
                } => WorldDrivenDecision::RejectBlock {
                    pos,
                    expected,
                    current,
                },
                AtomicInteractDecision::RejectInventory { .. } => WorldDrivenDecision::RejectInventory,
            }
        }
        WorldDrivenFrame::Interact(interact) => {
            let pos = interact.pos();
            let Some(current) = state_at(pos) else {
                return WorldDrivenDecision::RejectBlock {
                    pos,
                    expected: interact.expected_old_state(),
                    current: None,
                };
            };
            match crate::interact::apply_interact_edit(current, interact.expected_old_state()) {
                crate::interact::InteractDecision::Accept { undo } => WorldDrivenDecision::Accept {
                    undo: WorldDrivenUndo::Blocks(vec![(pos, undo)]),
                },
                crate::interact::InteractDecision::RejectStale { expected, current } => {
                    WorldDrivenDecision::RejectBlock {
                        pos,
                        expected,
                        current: Some(current),
                    }
                }
                crate::interact::InteractDecision::RejectAtomic => WorldDrivenDecision::RejectInvalid,
            }
        }
    }
}

fn judge_fallible(
    edits: &[BlockDelta],
    state_at: &mut impl FnMut(BlockPos) -> Option<u16>,
) -> WorldDrivenDecision {
    let mut undo = Vec::with_capacity(edits.len());
    for edit in edits {
        let Some(current) = state_at(edit.pos) else {
            return WorldDrivenDecision::RejectBlock {
                pos: edit.pos,
                expected: edit.old_state,
                current: None,
            };
        };
        match apply_fallible_edit(current, edit) {
            crate::world_delta::FallibleDecision::Apply { undo: one } => undo.push((edit.pos, one)),
            crate::world_delta::FallibleDecision::Conflict { expected, current } => {
                return WorldDrivenDecision::RejectBlock {
                    pos: edit.pos,
                    expected,
                    current: Some(current),
                };
            }
        }
    }
    WorldDrivenDecision::Accept {
        undo: WorldDrivenUndo::Blocks(undo),
    }
}

pub fn apply_accepted(
    frame: &WorldDrivenFrame,
    mut set_state: impl FnMut(BlockPos, u16) -> bool,
    inventory: Option<&mut dyn InvCells>,
) -> bool {
    match frame {
        WorldDrivenFrame::Delta(WorldDelta::Redstone(_))
        | WorldDrivenFrame::Delta(WorldDelta::Explosion(_))
        | WorldDrivenFrame::Delta(WorldDelta::TransactionalExplosion(_)) => false,
        WorldDrivenFrame::Interact(InteractUpdate::Atomic(atomic)) => {
            let Some(use_inventory) = &atomic.inventory else {
                let mut applied: Vec<&crate::interact::InteractEdit> =
                    Vec::with_capacity(atomic.edits.len());
                for edit in &atomic.edits {
                    if !set_state(edit.pos, edit.new_state) {
                        for prior in applied.into_iter().rev() {
                            let _ = set_state(prior.pos, prior.expected_old_state);
                        }
                        return false;
                    }
                    applied.push(edit);
                }
                return true;
            };
            let Some(inventory) = inventory else {
                return false;
            };
            let mut cells = DynamicInvCells(inventory);
            if replay(&mut cells, &use_inventory.op) != InvVerdict::Applied {
                return false;
            }
            let mut applied: Vec<&crate::interact::InteractEdit> =
                Vec::with_capacity(atomic.edits.len());
            for edit in &atomic.edits {
                if !set_state(edit.pos, edit.new_state) {
                        for prior in applied.into_iter().rev() {
                            let _ = set_state(prior.pos, prior.expected_old_state);
                        }
                    let _ = cells.set_stack(
                        use_inventory.precondition.loc,
                        use_inventory.precondition.stack.clone(),
                    );
                    return false;
                }
                applied.push(edit);
            }
            true
        }
        _ => frame
            .edits()
            .into_iter()
            .all(|edit| set_state(edit.pos, edit.new_state)),
    }
}

struct DynamicInvCells<'a>(&'a mut dyn InvCells);

impl InvCells for DynamicInvCells<'_> {
    fn cell(&self, loc: crate::inventory::InvLoc) -> Option<(u16, u8)> {
        self.0.cell(loc)
    }

    fn set_cell(&mut self, loc: crate::inventory::InvLoc, item: u16, count: u8) -> bool {
        self.0.set_cell(loc, item, count)
    }

    fn stack(&self, loc: crate::inventory::InvLoc) -> Option<crate::inventory::InventoryStack> {
        self.0.stack(loc)
    }

    fn set_stack(
        &mut self,
        loc: crate::inventory::InvLoc,
        stack: crate::inventory::InventoryStack,
    ) -> bool {
        self.0.set_stack(loc, stack)
    }
}

pub fn revert_local(
    undo: &WorldDrivenUndo,
    mut set_state: impl FnMut(BlockPos, u16) -> bool,
    inventory: Option<&mut dyn InvCells>,
) -> bool {
    if matches!(undo, WorldDrivenUndo::TransactionalExplosion(_)) {
        return false;
    }
    let mut reverted = true;
    for (pos, block_undo) in undo.blocks().into_iter().rev() {
        reverted &= set_state(pos, block_undo.old_state);
    }
    if let WorldDrivenUndo::Atomic(atomic) = undo
        && let Some(precondition) = &atomic.inventory
    {
        let Some(inventory) = inventory else {
            return false;
        };
        reverted &= inventory.set_stack(precondition.loc, precondition.stack.clone());
    }
    reverted
}
