use std::collections::BTreeSet;

use crate::actor::ActorId;
use crate::canonical_action::{Cell, ChunkPart, Target, Value};

pub fn valid(tags: &BTreeSet<String>) -> bool {
    tags.len() <= 1024 && tags.iter().all(|tag| !tag.is_empty() && tag.len() <= 1024)
}

pub fn body_target(target: Target) -> Option<Target> {
    let Target::Chunk {
        key,
        part: ChunkPart::ActorTags { actor },
        inventory: None,
    } = target
    else {
        return None;
    };
    let part = match actor {
        ActorId::Player(id) => ChunkPart::Player(id),
        ActorId::Entity(id) => ChunkPart::Entity(id),
        ActorId::Peer(_) => return None,
    };
    Some(Target::Chunk {
        key,
        part,
        inventory: None,
    })
}

pub fn cell(body: &Cell) -> Option<Cell> {
    let (revision, tags) = match &body.value {
        Value::Living(state) if state.avatar.is_some() => (state.tags_revision, &state.tags),
        Value::Entity(state) => (state.tags_revision, &state.tags),
        _ => return None,
    };
    valid(tags).then(|| Cell {
        revision,
        value: Value::ActorTags(tags.clone()),
    })
}

pub fn write(body: &mut Cell, facet: &Cell) -> bool {
    let Value::ActorTags(tags) = &facet.value else {
        return false;
    };
    if !valid(tags) {
        return false;
    }
    match &mut body.value {
        Value::Living(state) if state.avatar.is_some() => {
            state.tags = tags.clone();
            state.tags_revision = facet.revision;
        }
        Value::Entity(state) => {
            state.tags = tags.clone();
            state.tags_revision = facet.revision;
        }
        _ => return false,
    }
    true
}

pub fn patch_cell(body: &mut Cell, facet: &Cell) -> bool {
    write(body, facet)
}

pub fn normalize_body_versions(before: Option<&Cell>, after: &mut Cell) -> bool {
    let Some(current) = cell(after) else {
        return true;
    };
    let previous = before.and_then(cell);
    let revision = match previous {
        Some(previous) if previous.value == current.value => previous.revision,
        Some(previous) => match previous.revision.checked_add(1) {
            Some(revision) => revision,
            None => return false,
        },
        None => 0,
    };
    write(
        after,
        &Cell {
            revision,
            value: current.value,
        },
    )
}

#[derive(Debug, Clone, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
pub struct ActorTagsIntent {
    pub actor: ActorId,
    pub chunk: crate::protocol::ChunkKey,
    pub tag: String,
    pub present: bool,
}

impl ActorTagsIntent {
    pub fn target(&self) -> Target {
        Target::Chunk {
            key: self.chunk,
            part: ChunkPart::ActorTags { actor: self.actor },
            inventory: None,
        }
    }

    pub fn valid(&self, id: crate::canonical_action::ActionId) -> bool {
        id.actor == self.actor
            && id.incarnation != 0
            && match self.actor {
                ActorId::Player(_) => true,
                ActorId::Entity(entity) => id.incarnation == u64::from(entity.generation),
                ActorId::Peer(_) => false,
            }
            && !self.tag.is_empty()
            && self.tag.len() <= 1024
    }

    pub fn targets(&self) -> [Target; 1] {
        [self.target()]
    }

    pub fn derive(
        &self,
        id: crate::canonical_action::ActionId,
        _tick: crate::time::TickStamp,
        values: &std::collections::BTreeMap<Target, Cell>,
    ) -> Result<Vec<crate::canonical_action::Effect>, crate::canonical_action::ApplyFailure> {
        use crate::canonical_action::{ApplyFailure, Effect};
        if !self.valid(id) {
            return Err(ApplyFailure::InvalidAction);
        }
        let target = self.target();
        let Some(Cell {
            value: Value::ActorTags(previous),
            ..
        }) = values.get(&target)
        else {
            return Err(ApplyFailure::Type(target));
        };
        if !valid(previous) {
            return Err(ApplyFailure::InvalidAction);
        }
        let mut tags = previous.clone();
        if self.present {
            tags.insert(self.tag.clone());
        } else {
            tags.remove(&self.tag);
        }
        if !valid(&tags) {
            return Err(ApplyFailure::Capacity);
        }
        Ok(vec![Effect::Put {
            target,
            value: Value::ActorTags(tags),
        }])
    }

    pub fn capture(
        &self,
        state: &impl crate::canonical_action::ActionState,
        capture: crate::canonical_inventory::InventoryActionCapture,
    ) -> Result<crate::canonical_action::Action, crate::canonical_action::ApplyFailure> {
        use crate::canonical_action::*;
        let target = self.target();
        let authorized = match self.actor {
            ActorId::Player(gid) => {
                capture.player_leases.len() == 1
                    && capture.player_leases[0].gid == gid
                    && capture.player_leases[0].incarnation == capture.id.incarnation
            }
            ActorId::Entity(entity) => {
                capture.player_leases.is_empty()
                    && capture.guards.iter().any(|condition| {
                        condition.target
                            == Target::Chunk {
                                key: self.chunk,
                                part: ChunkPart::Entity(entity),
                                inventory: None,
                            }
                            && matches!(&condition.expected,
                        Expected::ActorControlled { birth, issuer }
                            if birth.source.server == entity.origin
                                && birth.source.generation != 0
                                && !birth.uuid.is_nil()
                                && issuer.generation != 0)
                    })
            }
            ActorId::Peer(_) => false,
        };
        if !self.valid(capture.id) || capture.actor_chunk != Some(self.chunk) || !authorized {
            return Err(ApplyFailure::InvalidAction);
        }
        let before = state
            .read(TruthView::Local, target)
            .ok_or(ApplyFailure::Condition(target))?;
        let effects = self.derive(capture.id, capture.tick, &[(target, before.clone())].into())?;
        let mut conditions = capture.guards;
        let mut dependencies = conditions
            .iter()
            .filter_map(|condition| match &condition.expected {
                Expected::ActorControlled { birth, .. } => Some(birth.action),
                _ => None,
            })
            .collect::<Vec<_>>();
        dependencies.sort_unstable();
        dependencies.dedup();
        conditions.push(Condition {
            target,
            expected: Expected::Exact(before),
        });
        let action = Action {
            id: capture.id,
            tick: capture.tick,
            edit_authority: capture.edit_authority,
            class: ActionClass::Replayable,
            chunk_transition: false,
            actor_chunk: capture.actor_chunk,
            player_leases: capture.player_leases,
            player_chunks: capture.player_chunks,
            stakeholders: vec![Stakeholder::Chunk(self.chunk)],
            dependencies,
            conditions,
            effects,
            counterfactual_conditions: self.targets().to_vec(),
            counterfactual_effects: self.targets().to_vec(),
            semantic_intent: Some(SemanticIntent::ActorTags(self.clone())),
            factual_result: None,
            undo: vec![UndoProcedure::RestoreWrittenTargets],
            kind: ActionKind::PlayerAdministration,
            client_origin: capture.client_origin,
            presentation: Vec::new(),
        };
        action.validate()?;
        Ok(action)
    }
}
