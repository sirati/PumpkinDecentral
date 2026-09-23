use std::collections::HashMap;

use pumpkin_cluster::block_action::{BlockActionIntent, BlockActionKind};
use pumpkin_cluster::completion::{ActionKey, ManifestError, TickFinish};
use pumpkin_cluster::identity::{GlobalPlayerId, PlayerSeq};
use pumpkin_cluster::protocol::{BlockPos, BreakBlockUpdate, TickBatch};
use tokio::sync::mpsc;

use super::Server;

pub(super) struct BlockActionOwner {
    pub(super) receiver: mpsc::Receiver<BlockActionIntent>,
    sequences: HashMap<GlobalPlayerId, PlayerSeq>,
    active: HashMap<GlobalPlayerId, (BlockPos, u16)>,
    client_sequences: HashMap<ActionKey, Option<i32>>,
}

impl BlockActionOwner {
    pub(super) fn attach(server: &Server) -> Self {
        Self {
            receiver: super::cluster_block_action::attach_block_action_owner(server),
            sequences: HashMap::new(),
            active: HashMap::new(),
            client_sequences: HashMap::new(),
        }
    }

    pub(super) fn capture(
        &mut self,
        intent: BlockActionIntent,
        blocks: &super::cluster_chunk_state::OwnedChunkStore,
    ) -> Result<Option<(TickBatch, TickFinish)>, ManifestError> {
        let Ok(state) = blocks.block(intent.target_chunk, intent.position) else {
            return Ok(None);
        };
        let authoritative = state.as_u16();
        if state == pumpkin_data::BlockStateId::AIR
            || intent
                .expected_block_state
                .is_some_and(|expected| expected != authoritative)
        {
            return Ok(None);
        }
        match intent.kind {
            BlockActionKind::StartBreak | BlockActionKind::PredictBreak => {
                self.active
                    .insert(intent.actor, (intent.position, authoritative));
                Ok(None)
            }
            BlockActionKind::AbortBreak => {
                self.active.remove(&intent.actor);
                Ok(None)
            }
            BlockActionKind::ContinueBreak => Ok(None),
            BlockActionKind::StopBreak => {
                let Some((position, expected)) = self.active.remove(&intent.actor) else {
                    return Ok(None);
                };
                if position != intent.position || expected != authoritative {
                    return Ok(None);
                }
                self.finish(intent, authoritative)
            }
            BlockActionKind::CreativeBreak => self.finish(intent, authoritative),
        }
    }

    fn finish(
        &mut self,
        intent: BlockActionIntent,
        expected_old_state: u16,
    ) -> Result<Option<(TickBatch, TickFinish)>, ManifestError> {
        let sequence = if let Some(sequence) = intent.input_sequence {
            self.sequences.insert(intent.actor, sequence);
            sequence
        } else {
            *self
                .sequences
                .entry(intent.actor)
                .and_modify(|current| current.0 = current.0.wrapping_add(1))
                .or_insert(PlayerSeq(0))
        };
        let update = BreakBlockUpdate {
            gid: intent.actor,
            seq: sequence,
            tick: intent.tick,
            pos: intent.position,
            expected_old_state,
            actor_chunk: intent.actor_chunk,
            chunk: intent.target_chunk,
        };
        let mut batch = TickBatch::new(intent.tick);
        batch.break_block.push(update);
        let finish = TickFinish::try_from_batch(&batch)?;
        self.client_sequences
            .insert(ActionKey::break_block(&update), intent.client_sequence);
        Ok(Some((batch, finish)))
    }

    pub(super) fn take_client_sequence(&mut self, update: &BreakBlockUpdate) -> Option<i32> {
        self.client_sequences
            .remove(&ActionKey::break_block(update))
            .flatten()
    }
}
