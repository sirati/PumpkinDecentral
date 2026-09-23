use serde::{Deserialize, Serialize};
use std::collections::HashMap;

use crate::identity::{GlobalPlayerId, PlayerSeq};
use crate::protocol::{BlockPos, ChunkKey};
use crate::time::TickStamp;

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub enum BlockActionKind {
    StartBreak,
    AbortBreak,
    StopBreak,
    ContinueBreak,
    PredictBreak,
    CreativeBreak,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub struct BlockActionIntent {
    pub actor: GlobalPlayerId,
    pub tick: TickStamp,
    pub actor_chunk: ChunkKey,
    pub target_chunk: ChunkKey,
    pub position: BlockPos,
    pub face: u8,
    pub expected_block_state: Option<u16>,
    pub kind: BlockActionKind,
    pub input_sequence: Option<PlayerSeq>,
    pub client_sequence: Option<i32>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct MiningEvidence {
    pub block_state: u16,
    pub tool_revision: u64,
    pub speed_milli_per_tick: u32,
    pub can_break: bool,
    pub creative: bool,
    pub predicted_threshold_milli: u32,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct MiningSession {
    pub position: BlockPos,
    pub block_state: u16,
    pub tool_revision: u64,
    pub start_tick: TickStamp,
    pub last_tick: TickStamp,
    pub progress_milli: u32,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum MiningDecision {
    Started { stage: u8 },
    Advanced { stage: u8 },
    Broken { expected_block_state: u16 },
    Aborted,
    Rejected,
    Ignored,
}

#[derive(Default)]
pub struct MiningSessions {
    active: HashMap<GlobalPlayerId, MiningSession>,
}

impl MiningSessions {
    pub fn apply(&mut self, intent: BlockActionIntent, evidence: MiningEvidence) -> MiningDecision {
        if intent.kind == BlockActionKind::AbortBreak {
            return if self.active.remove(&intent.actor).is_some() {
                MiningDecision::Aborted
            } else {
                MiningDecision::Ignored
            };
        }
        if !evidence.can_break
            || evidence.block_state == 0
            || intent
                .expected_block_state
                .is_some_and(|expected| expected != evidence.block_state)
        {
            self.active.remove(&intent.actor);
            return MiningDecision::Rejected;
        }
        if matches!(
            intent.kind,
            BlockActionKind::StartBreak | BlockActionKind::CreativeBreak
        ) {
            self.active.remove(&intent.actor);
            if intent.kind == BlockActionKind::CreativeBreak && !evidence.creative {
                return MiningDecision::Rejected;
            }
            if evidence.creative || evidence.speed_milli_per_tick >= 1_000 {
                return MiningDecision::Broken {
                    expected_block_state: evidence.block_state,
                };
            }
            let progress = evidence.speed_milli_per_tick;
            self.active.insert(
                intent.actor,
                MiningSession {
                    position: intent.position,
                    block_state: evidence.block_state,
                    tool_revision: evidence.tool_revision,
                    start_tick: intent.tick,
                    last_tick: intent.tick,
                    progress_milli: progress,
                },
            );
            return MiningDecision::Started {
                stage: stage(progress),
            };
        }
        let Some(session) = self.active.get_mut(&intent.actor) else {
            return MiningDecision::Rejected;
        };
        if session.position != intent.position
            || session.block_state != evidence.block_state
            || session.tool_revision != evidence.tool_revision
        {
            self.active.remove(&intent.actor);
            return MiningDecision::Rejected;
        }
        if intent.tick != session.last_tick {
            if !intent.tick.is_after(session.last_tick) {
                return MiningDecision::Rejected;
            }
            let elapsed = u32::from(intent.tick.0.wrapping_sub(session.last_tick.0));
            session.progress_milli = session
                .progress_milli
                .saturating_add(evidence.speed_milli_per_tick.saturating_mul(elapsed));
            session.last_tick = intent.tick;
        }
        let threshold = if intent.kind == BlockActionKind::PredictBreak {
            evidence.predicted_threshold_milli.clamp(1, 1_000)
        } else {
            1_000
        };
        if matches!(
            intent.kind,
            BlockActionKind::StopBreak | BlockActionKind::PredictBreak
        ) && session.progress_milli >= threshold
        {
            self.active.remove(&intent.actor);
            MiningDecision::Broken {
                expected_block_state: evidence.block_state,
            }
        } else if intent.kind == BlockActionKind::StopBreak {
            self.active.remove(&intent.actor);
            MiningDecision::Rejected
        } else {
            MiningDecision::Advanced {
                stage: stage(session.progress_milli),
            }
        }
    }

    pub fn active(&self, actor: GlobalPlayerId) -> Option<MiningSession> {
        self.active.get(&actor).copied()
    }
}

fn stage(progress_milli: u32) -> u8 {
    progress_milli.min(999).saturating_mul(10).div_euclid(1_000) as u8
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::identity::{PlayerSlot, ServerId};
    use crate::protocol::WorldId;

    fn intent(tick: u16, kind: BlockActionKind) -> BlockActionIntent {
        let world = WorldId::new(0, 1);
        BlockActionIntent {
            actor: GlobalPlayerId::new(ServerId(1), PlayerSlot(2)),
            tick: TickStamp(tick),
            actor_chunk: ChunkKey::new(world, 0, 0),
            target_chunk: ChunkKey::new(world, 0, 0),
            position: BlockPos { x: 1, y: 64, z: 1 },
            face: 1,
            expected_block_state: None,
            kind,
            input_sequence: None,
            client_sequence: Some(i32::from(tick)),
        }
    }

    fn evidence() -> MiningEvidence {
        MiningEvidence {
            block_state: 42,
            tool_revision: 7,
            speed_milli_per_tick: 200,
            can_break: true,
            creative: false,
            predicted_threshold_milli: 650,
        }
    }

    #[test]
    fn progressive_break_requires_same_block_and_tool_and_enough_ticks() {
        let mut sessions = MiningSessions::default();
        assert_eq!(
            sessions.apply(intent(10, BlockActionKind::StartBreak), evidence()),
            MiningDecision::Started { stage: 2 }
        );
        assert_eq!(
            sessions.apply(intent(11, BlockActionKind::ContinueBreak), evidence()),
            MiningDecision::Advanced { stage: 4 }
        );
        assert_eq!(
            sessions.apply(intent(12, BlockActionKind::StopBreak), evidence()),
            MiningDecision::Rejected
        );
        assert!(
            sessions
                .active(intent(10, BlockActionKind::StartBreak).actor)
                .is_none()
        );
        sessions.apply(intent(20, BlockActionKind::StartBreak), evidence());
        assert_eq!(
            sessions.apply(intent(23, BlockActionKind::PredictBreak), evidence()),
            MiningDecision::Broken {
                expected_block_state: 42
            }
        );
    }

    #[test]
    fn stale_block_or_tool_invalidates_session_without_breaking() {
        let mut sessions = MiningSessions::default();
        sessions.apply(intent(10, BlockActionKind::StartBreak), evidence());
        let mut changed = evidence();
        changed.block_state = 43;
        assert_eq!(
            sessions.apply(intent(15, BlockActionKind::StopBreak), changed),
            MiningDecision::Rejected
        );
        assert!(
            sessions
                .active(intent(10, BlockActionKind::StartBreak).actor)
                .is_none()
        );
        sessions.apply(intent(20, BlockActionKind::StartBreak), evidence());
        changed = evidence();
        changed.tool_revision = 8;
        assert_eq!(
            sessions.apply(intent(25, BlockActionKind::StopBreak), changed),
            MiningDecision::Rejected
        );
        assert!(
            sessions
                .active(intent(10, BlockActionKind::StartBreak).actor)
                .is_none()
        );
    }

    #[test]
    fn instant_and_wrapped_tick_progress_do_not_need_stop() {
        let mut sessions = MiningSessions::default();
        let mut fast = evidence();
        fast.speed_milli_per_tick = 1_000;
        assert_eq!(
            sessions.apply(intent(1, BlockActionKind::CreativeBreak), fast),
            MiningDecision::Rejected
        );
        assert_eq!(
            sessions.apply(intent(1, BlockActionKind::StartBreak), fast),
            MiningDecision::Broken {
                expected_block_state: 42
            }
        );
        sessions.apply(intent(u16::MAX, BlockActionKind::StartBreak), evidence());
        assert_eq!(
            sessions.apply(intent(0, BlockActionKind::ContinueBreak), evidence()),
            MiningDecision::Advanced { stage: 4 }
        );
        assert_eq!(
            sessions.apply(intent(u16::MAX, BlockActionKind::StopBreak), evidence()),
            MiningDecision::Rejected
        );
    }
}
