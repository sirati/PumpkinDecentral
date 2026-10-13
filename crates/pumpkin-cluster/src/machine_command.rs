use std::collections::BTreeSet;

use crate::action_command::{ActionFootprint, CommandContext, CommandResult, MachineTick, ResolutionRecord};
use crate::action_identity::ActionIdentity;
use crate::canonical_action::{ActionState, ApplyFailure, ChunkPart, Target};

impl MachineTick {
    pub fn footprint<S: ActionState>(&self, context: &CommandContext<'_, S>) -> Result<ActionFootprint, ApplyFailure> {
        if !self.identity.valid() || context.identity != ActionIdentity::Derived(self.identity)
            || context.tick != self.identity.tick()
            || context.state.machine_genesis(self.identity.chunk())? != self.identity.genesis {
            return Err(ApplyFailure::InvalidAction);
        }
        let footprint = context.state.machine_tick_footprint(self.identity, context.values)?;
        let root = Target::Chunk { key: self.identity.chunk(), part: ChunkPart::Block(self.identity.position), inventory: None };
        if !footprint.reads.contains(&root)
            || footprint.reads.iter().chain(&footprint.writes).chain(&footprint.counterfactual_reads).chain(&footprint.counterfactual_writes)
                .any(|target| !target.is_valid() || !matches!(target, Target::Chunk { key, .. } if key.world == self.identity.world)) {
            return Err(ApplyFailure::InvalidAction);
        }
        Ok(footprint)
    }

    pub fn derive<S: ActionState>(&self, context: &CommandContext<'_, S>) -> Result<ResolutionRecord, ApplyFailure> {
        let footprint = self.footprint(context)?;
        let writes = context.state.derive_machine_tick(self.identity, context.values)?;
        let mut targets = BTreeSet::new();
        for write in &writes {
            if !targets.insert(write.target) || !footprint.writes.contains(&write.target)
                || write.before.as_ref() != context.values.get(&write.target) {
                return Err(ApplyFailure::InvalidAction);
            }
            if write.before.is_none() && !context.known_absent.contains(&write.target) {
                return Err(ApplyFailure::Condition(write.target));
            }
            if let Some(after) = &write.after {
                let revision = match &write.before {
                    Some(before) => before.revision.checked_add(1).ok_or(ApplyFailure::Overflow(write.target))?,
                    None => 0,
                };
                if after.revision != revision { return Err(ApplyFailure::InvalidAction); }
            }
        }
        Ok(ResolutionRecord { identity: context.identity, tick: context.tick, footprint, result: CommandResult::Applied(writes) })
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::collections::BTreeMap;
    use crate::action_identity::{MachineActionId, MachineKind};
    use crate::canonical_action::{AppliedWrite, Cell, TruthView, Value};
    use crate::protocol::{BlockPos, ChunkKey, WorldId};

    struct State { writes: Vec<AppliedWrite> }
    impl ActionState for State {
        fn read(&self, _: TruthView, _: Target) -> Option<Cell> { None }
        fn commit(&mut self, _: crate::canonical_action::ActionContext, _: TruthView, _: &[AppliedWrite]) -> Result<(), ApplyFailure> { Err(ApplyFailure::InvalidAction) }
        fn machine_genesis(&self, _: ChunkKey) -> Result<[u8; 32], ApplyFailure> { Ok([1; 32]) }
        fn machine_tick_footprint(&self, identity: MachineActionId, _: &BTreeMap<Target, Cell>) -> Result<ActionFootprint, ApplyFailure> {
            let target = Target::Chunk { key: identity.chunk(), part: ChunkPart::Block(identity.position), inventory: None };
            let locations = BTreeSet::from([target]);
            Ok(ActionFootprint { reads: locations.clone(), writes: locations, counterfactual_reads: BTreeSet::new(), counterfactual_writes: BTreeSet::new() })
        }
        fn derive_machine_tick(&self, _: MachineActionId, _: &BTreeMap<Target, Cell>) -> Result<Vec<AppliedWrite>, ApplyFailure> { Ok(self.writes.clone()) }
    }

    #[test]
    fn machine_result_uses_full_derived_identity_and_exact_undo() {
        let identity = MachineActionId { genesis: [1; 32], world: WorldId::new(0, 0), position: BlockPos { x: 0, y: 64, z: 0 }, source_slot: 65_636, kind: MachineKind::Furnace };
        let target = Target::Chunk { key: identity.chunk(), part: ChunkPart::Block(identity.position), inventory: None };
        let before = Cell { revision: 4, value: Value::Block(0) };
        let after = Cell { revision: 5, value: Value::Block(1) };
        let write = AppliedWrite { target, before: Some(before.clone()), after: Some(after.clone()) };
        let state = State { writes: vec![write.clone()] };
        let values = BTreeMap::from([(target, before.clone())]);
        let context = CommandContext { known_absent: &std::collections::BTreeSet::new(), state: &state, identity: ActionIdentity::Derived(identity), tick: identity.tick(), values: &values };
        let result = MachineTick { identity }.derive(&context).unwrap();
        assert_eq!(result.identity, ActionIdentity::Derived(identity));
        assert!(!result.identity.requires_creation_transmission());
        assert_eq!(result.undo_writes().collect::<Vec<_>>(), vec![AppliedWrite { target, before: Some(after), after: Some(before) }]);
        let forged = MachineActionId { genesis: [2; 32], ..identity };
        let forged_context = CommandContext { known_absent: &std::collections::BTreeSet::new(), identity: ActionIdentity::Derived(forged), ..context };
        assert!(MachineTick { identity: forged }.derive(&forged_context).is_err());
        let duplicated = State { writes: vec![write.clone(), write] };
        let duplicate_context = CommandContext { known_absent: &std::collections::BTreeSet::new(), state: &duplicated, ..context };
        assert!(MachineTick { identity }.derive(&duplicate_context).is_err());
        let exhausted_before = Cell { revision: u64::MAX, value: Value::Block(0) };
        let exhausted = State { writes: vec![AppliedWrite { target, before: Some(exhausted_before.clone()), after: Some(Cell { revision: 0, value: Value::Block(1) }) }] };
        let exhausted_values = BTreeMap::from([(target, exhausted_before)]);
        let exhausted_context = CommandContext { state: &exhausted, values: &exhausted_values, ..context };
        assert_eq!(MachineTick { identity }.derive(&exhausted_context), Err(ApplyFailure::Overflow(target)));
    }
}
