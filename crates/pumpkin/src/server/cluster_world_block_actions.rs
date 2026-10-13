use std::collections::HashMap;

use pumpkin_cluster::block_action::BlockActionIntent;
use pumpkin_cluster::identity::GlobalPlayerId;

pub(super) struct BlockActionOwner {
    sessions: pumpkin_cluster::block_action::MiningSessions,
    tools: HashMap<GlobalPlayerId, (u16, Vec<u8>, u64)>,
}

impl BlockActionOwner {
    pub(super) fn new() -> Self {
        Self {
            sessions: pumpkin_cluster::block_action::MiningSessions::default(),
            tools: HashMap::new(),
        }
    }

    pub(super) fn retire(&mut self, actor: GlobalPlayerId) {
        self.sessions.retire(actor);
        self.tools.remove(&actor);
    }

    pub(super) fn stage(
        &mut self,
        intent: BlockActionIntent,
        mut evidence: pumpkin_cluster::block_action::MiningEvidence,
        tool: &pumpkin_cluster::inventory::InventoryCell,
    ) -> pumpkin_cluster::block_action::MiningDecision {
        let entry = self
            .tools
            .entry(intent.actor)
            .or_insert_with(|| (tool.item, tool.nbt.clone(), 1));
        if entry.0 != tool.item || entry.1 != tool.nbt {
            let Some(version) = entry.2.checked_add(1) else {
                self.sessions.retire(intent.actor);
                return pumpkin_cluster::block_action::MiningDecision::Rejected;
            };
            *entry = (tool.item, tool.nbt.clone(), version);
        }
        evidence.tool_revision = entry.2;
        self.sessions.apply(intent, evidence)
    }
}
