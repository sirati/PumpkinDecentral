use std::collections::BTreeSet;

use serde::{Deserialize, Serialize};

use crate::action_identity::{ActionIdentity, MachineActionId};
use crate::canonical_action::{AppliedWrite, LivePlayerLease, SpatialEpoch, Target};
use crate::entity_body::EntityBirth;
use crate::protocol::{ChunkKey, GlobalEntityId};
use crate::realm_completion::PeerGeneration;
use crate::time::TickStamp;

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub enum ActorReference {
    Player {
        lease: LivePlayerLease,
        chunk: ChunkKey,
        epoch: SpatialEpoch,
        owner: PeerGeneration,
    },
    Entity {
        id: GlobalEntityId,
        birth: EntityBirth,
        chunk: ChunkKey,
        epoch: SpatialEpoch,
        owner: PeerGeneration,
    },
}

impl ActorReference {
    pub fn actor(&self) -> crate::actor::ActorId {
        match self {
            Self::Player { lease, .. } => crate::actor::ActorId::Player(lease.gid),
            Self::Entity { id, .. } => crate::actor::ActorId::Entity(*id),
        }
    }

    pub fn body_target(&self) -> Target {
        Target::Chunk {
            key: self.chunk(),
            part: match self {
                Self::Player { lease, .. } => crate::canonical_action::ChunkPart::Player(lease.gid),
                Self::Entity { id, .. } => crate::canonical_action::ChunkPart::Entity(*id),
            },
            inventory: None,
        }
    }

    pub fn epoch(&self) -> SpatialEpoch {
        match self {
            Self::Player { epoch, .. } | Self::Entity { epoch, .. } => *epoch,
        }
    }

    pub fn chunk(&self) -> ChunkKey {
        match self {
            Self::Player { chunk, .. } | Self::Entity { chunk, .. } => *chunk,
        }
    }

    pub fn owner(&self) -> PeerGeneration {
        match self {
            Self::Player { owner, .. } | Self::Entity { owner, .. } => *owner,
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub enum Hand {
    Main,
    Off,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct ItemCommandSource {
    pub actor: ActorReference,
    pub hand: Hand,
    pub cut: crate::actor_occupancy::ActorOccupancyCut,
}

pub struct CommandContext<'a, S> {
    pub state: &'a S,
    pub identity: ActionIdentity,
    pub tick: TickStamp,
    pub values: &'a std::collections::BTreeMap<Target, crate::canonical_action::Cell>,
    pub known_absent: &'a BTreeSet<Target>,
}

impl<'a, S> CommandContext<'a, S> {
    pub fn lookup(
        &self,
        target: Target,
    ) -> Result<Option<&'a crate::canonical_action::Cell>, crate::canonical_action::ApplyFailure>
    {
        match (
            self.values.get(&target),
            self.known_absent.contains(&target),
        ) {
            (Some(_), true) => Err(crate::canonical_action::ApplyFailure::InvalidAction),
            (Some(cell), false) => Ok(Some(cell)),
            (None, true) => Ok(None),
            (None, false) => Err(crate::canonical_action::ApplyFailure::MissingStakeholder(
                target.stakeholder(),
            )),
        }
    }
}

pub trait OriginalCommandState: crate::canonical_action::ActionState {
    fn original_source_slot(&self) -> Option<u64>;
    fn lookup_original(
        &self,
        target: Target,
    ) -> Result<
        Option<std::borrow::Cow<'_, crate::canonical_action::Cell>>,
        crate::canonical_action::ApplyFailure,
    >;
}

pub struct RetainedCommandScope<S> {
    identity: ActionIdentity,
    source_slot: u64,
    original: std::sync::Arc<S>,
    footprint: std::sync::Arc<ActionFootprint>,
}

impl<S> Clone for RetainedCommandScope<S> {
    fn clone(&self) -> Self {
        Self {
            identity: self.identity,
            source_slot: self.source_slot,
            original: self.original.clone(),
            footprint: self.footprint.clone(),
        }
    }
}

impl<S: OriginalCommandState> RetainedCommandScope<S> {
    pub fn capture(
        identity: ActionIdentity,
        tick: TickStamp,
        source_slot: u64,
        original: std::sync::Arc<S>,
        footprint: ActionFootprint,
    ) -> Result<Self, crate::canonical_action::ApplyFailure> {
        use crate::canonical_action::ApplyFailure;
        let identity_matches = match identity {
            ActionIdentity::Issued(_) => tick == TickStamp(source_slot as u16),
            ActionIdentity::Derived(id) => {
                id.valid() && id.source_slot == source_slot && id.tick() == tick
            }
        };
        if !identity_matches || original.original_source_slot() != Some(source_slot) {
            return Err(ApplyFailure::InvalidAction);
        }
        for target in footprint
            .reads
            .iter()
            .chain(&footprint.writes)
            .chain(&footprint.counterfactual_reads)
            .chain(&footprint.counterfactual_writes)
        {
            if !target.is_valid() {
                return Err(ApplyFailure::InvalidAction);
            }
            original.lookup_original(*target)?;
        }
        Ok(Self {
            identity,
            source_slot,
            original,
            footprint: std::sync::Arc::new(footprint),
        })
    }

    pub fn identity(&self) -> ActionIdentity {
        self.identity
    }
    pub fn source_slot(&self) -> u64 {
        self.source_slot
    }
    pub fn footprint(&self) -> &ActionFootprint {
        &self.footprint
    }
    pub fn original_state(&self) -> &S {
        &self.original
    }

    pub fn lookup(
        &self,
        target: Target,
    ) -> Result<
        Option<std::borrow::Cow<'_, crate::canonical_action::Cell>>,
        crate::canonical_action::ApplyFailure,
    > {
        use crate::canonical_action::ApplyFailure;
        if !self.footprint.reads.contains(&target)
            && !self.footprint.writes.contains(&target)
            && !self.footprint.counterfactual_reads.contains(&target)
            && !self.footprint.counterfactual_writes.contains(&target)
        {
            return Err(ApplyFailure::InvalidAction);
        }
        self.original.lookup_original(target)
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct MotionSpan {
    pub actor: ActorReference,
    pub cut: crate::actor_occupancy::ActorOccupancyCut,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub struct MachineTick {
    pub identity: MachineActionId,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub enum ActionCommand {
    UseObject(crate::object_command::UseObject),
    MotionSpan(MotionSpan),
    NaturalMobSpawn(crate::mob_spawn_command::NaturalMobSpawn),
    StripLog(crate::world_command::StripLog),
    ScrapeCopper(crate::world_command::ScrapeCopper),
    RemoveWax(crate::world_command::RemoveWax),
    BreakBlock(crate::world_command::BreakBlock),
    PlaceBlock(crate::world_command::PlaceBlock),
    MachineTick(MachineTick),
    BeaconSelection(crate::menu_command::BeaconSelection),
    TakeResult(crate::menu_command::TakeResult),
}

#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct ActionFootprint {
    pub reads: BTreeSet<Target>,
    pub writes: BTreeSet<Target>,
    pub counterfactual_reads: BTreeSet<Target>,
    pub counterfactual_writes: BTreeSet<Target>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum CommandResult {
    Applied(Vec<AppliedWrite>),
    NotApplied,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ResolutionRecord {
    pub identity: ActionIdentity,
    pub tick: TickStamp,
    pub footprint: ActionFootprint,
    pub result: CommandResult,
}

impl ResolutionRecord {
    pub fn normalize_footprint(&mut self) -> Result<(), crate::canonical_action::ApplyFailure> {
        let writes = match &self.result {
            CommandResult::Applied(writes) => writes.as_slice(),
            CommandResult::NotApplied => &[],
        };
        let actual = writes
            .iter()
            .map(|write| write.target)
            .collect::<BTreeSet<_>>();
        if actual.len() != writes.len()
            || actual.iter().any(|target| {
                !target.is_valid()
                    || !self.footprint.writes.contains(target)
                        && !self.footprint.counterfactual_writes.contains(target)
            })
        {
            return Err(crate::canonical_action::ApplyFailure::InvalidAction);
        }
        self.footprint
            .counterfactual_writes
            .extend(self.footprint.writes.iter().copied());
        self.footprint.writes = actual;
        self.footprint
            .counterfactual_writes
            .retain(|target| !self.footprint.writes.contains(target));
        self.footprint
            .counterfactual_reads
            .retain(|target| !self.footprint.reads.contains(target));
        Ok(())
    }

    pub fn undo_writes(&self) -> impl DoubleEndedIterator<Item = AppliedWrite> + '_ {
        let writes = match &self.result {
            CommandResult::Applied(writes) => writes.as_slice(),
            CommandResult::NotApplied => &[],
        };
        writes.iter().rev().map(|write| AppliedWrite {
            target: write.target,
            before: write.after.clone(),
            after: write.before.clone(),
        })
    }
}

impl ActionFootprint {
    pub fn chunks(
        &self,
        player_chunk: impl Fn(LivePlayerLease) -> Option<ChunkKey>,
    ) -> Result<BTreeSet<ChunkKey>, crate::canonical_action::ApplyFailure> {
        self.reads
            .iter()
            .chain(&self.writes)
            .chain(&self.counterfactual_reads)
            .chain(&self.counterfactual_writes)
            .map(|target| match target {
                Target::Chunk { key, .. } => Ok(*key),
                Target::Player {
                    lease: Some(lease), ..
                } => player_chunk(*lease).ok_or(
                    crate::canonical_action::ApplyFailure::MissingStakeholder(target.stakeholder()),
                ),
                Target::Player { .. } => Err(
                    crate::canonical_action::ApplyFailure::MissingStakeholder(target.stakeholder()),
                ),
            })
            .collect()
    }
}

#[cfg(test)]
mod context_tests {
    use super::*;
    use crate::canonical_action::{ActionId, ApplyFailure, Cell, Value};
    use crate::identity::ServerId;
    use crate::protocol::{BlockPos, WorldId};

    struct Original {
        slot: u64,
        facts: std::collections::BTreeMap<Target, Option<Cell>>,
    }

    impl crate::canonical_action::ActionState for Original {
        fn read(&self, _: crate::canonical_action::TruthView, target: Target) -> Option<Cell> {
            self.facts.get(&target).cloned().flatten()
        }
        fn commit(
            &mut self,
            _: crate::canonical_action::ActionContext,
            _: crate::canonical_action::TruthView,
            _: &[AppliedWrite],
        ) -> Result<(), ApplyFailure> {
            panic!("immutable original source")
        }
    }

    impl OriginalCommandState for Original {
        fn original_source_slot(&self) -> Option<u64> {
            Some(self.slot)
        }
        fn lookup_original(
            &self,
            target: Target,
        ) -> Result<Option<std::borrow::Cow<'_, Cell>>, ApplyFailure> {
            self.facts
                .get(&target)
                .map(|cell| cell.as_ref().map(std::borrow::Cow::Borrowed))
                .ok_or(ApplyFailure::MissingStakeholder(target.stakeholder()))
        }
    }

    #[test]
    fn retained_scope_shares_originals_and_preserves_absence_after_winning_deletion() {
        let chunk = ChunkKey::new(WorldId::new(0, 7), 0, 0);
        let target = |x| Target::Chunk {
            key: chunk,
            part: crate::canonical_action::ChunkPart::Block(BlockPos { x, y: 80, z: 0 }),
            inventory: None,
        };
        let original = std::sync::Arc::new(Original {
            slot: 65_537,
            facts: std::collections::BTreeMap::from([
                (
                    target(1),
                    Some(Cell {
                        revision: 7,
                        value: Value::Block(2),
                    }),
                ),
                (target(2), None),
            ]),
        });
        let identity = ActionIdentity::Issued(ActionId {
            actor: crate::actor::ActorId::Peer(ServerId(1)),
            incarnation: 1,
            sequence: 0,
            family: 0,
        });
        let footprint = ActionFootprint {
            reads: BTreeSet::from([target(1), target(2)]),
            ..Default::default()
        };
        let scope = RetainedCommandScope::capture(
            identity,
            TickStamp(1),
            65_537,
            original.clone(),
            footprint.clone(),
        )
        .unwrap();
        let retained = scope.clone();
        assert!(std::ptr::eq(
            scope.original_state(),
            retained.original_state()
        ));
        assert!(std::ptr::eq(scope.footprint(), retained.footprint()));
        assert_eq!(scope.lookup(target(1)).unwrap().unwrap().revision, 7);
        assert!(scope.lookup(target(2)).unwrap().is_none());
        assert_eq!(scope.lookup(target(3)), Err(ApplyFailure::InvalidAction));
        assert!(matches!(
            RetainedCommandScope::capture(identity, TickStamp(1), 1, original.clone(), footprint),
            Err(ApplyFailure::InvalidAction)
        ));
        assert!(matches!(
            RetainedCommandScope::capture(
                identity,
                TickStamp(1),
                65_537,
                original,
                ActionFootprint {
                    reads: BTreeSet::from([target(3)]),
                    ..Default::default()
                }
            ),
            Err(ApplyFailure::MissingStakeholder(_))
        ));
    }

    #[test]
    fn symbolic_lookup_distinguishes_absence_from_unavailable_original_input() {
        let chunk = ChunkKey::new(WorldId::new(0, 7), 0, 0);
        let target = |x| Target::Chunk {
            key: chunk,
            part: crate::canonical_action::ChunkPart::Block(BlockPos { x, y: 80, z: 0 }),
            inventory: None,
        };
        let present = target(1);
        let absent = target(2);
        let unavailable = target(3);
        let values = std::collections::BTreeMap::from([(
            present,
            Cell {
                revision: 0,
                value: Value::Block(1),
            },
        )]);
        let known_absent = BTreeSet::from([absent]);
        let identity = ActionIdentity::Issued(ActionId {
            actor: crate::actor::ActorId::Peer(ServerId(1)),
            incarnation: 1,
            sequence: 0,
            family: 0,
        });
        let context = CommandContext {
            state: &(),
            identity,
            tick: TickStamp(1),
            values: &values,
            known_absent: &known_absent,
        };
        assert_eq!(context.lookup(present).unwrap(), values.get(&present));
        assert_eq!(context.lookup(absent).unwrap(), None);
        assert_eq!(
            context.lookup(unavailable),
            Err(ApplyFailure::MissingStakeholder(unavailable.stakeholder()))
        );
        let conflicting = BTreeSet::from([present]);
        let context = CommandContext {
            known_absent: &conflicting,
            ..context
        };
        assert_eq!(context.lookup(present), Err(ApplyFailure::InvalidAction));
    }
    #[test]
    fn resolved_factual_writes_are_exact_and_other_possible_outputs_remain_counterfactual() {
        let chunk = ChunkKey::new(WorldId::new(0, 7), 0, 0);
        let target = |x| Target::Chunk {
            key: chunk,
            part: crate::canonical_action::ChunkPart::Block(BlockPos { x, y: 80, z: 0 }),
            inventory: None,
        };
        let identity = ActionIdentity::Issued(ActionId {
            actor: crate::actor::ActorId::Peer(ServerId(1)),
            incarnation: 1,
            sequence: 0,
            family: 0,
        });
        let scope = ActionFootprint {
            reads: BTreeSet::from([target(1)]),
            writes: BTreeSet::from([target(1), target(2)]),
            counterfactual_reads: BTreeSet::from([target(1), target(2)]),
            counterfactual_writes: BTreeSet::from([target(2), target(3)]),
        };
        let mut record = ResolutionRecord {
            identity,
            tick: TickStamp(1),
            footprint: scope.clone(),
            result: CommandResult::Applied(vec![AppliedWrite {
                target: target(1),
                before: None,
                after: Some(Cell {
                    revision: 0,
                    value: Value::Block(1),
                }),
            }]),
        };
        record.normalize_footprint().unwrap();
        assert_eq!(record.footprint.writes, BTreeSet::from([target(1)]));
        assert_eq!(
            record.footprint.counterfactual_writes,
            BTreeSet::from([target(2), target(3)])
        );
        assert_eq!(
            record.footprint.counterfactual_reads,
            BTreeSet::from([target(2)])
        );
        record.result = CommandResult::NotApplied;
        record.normalize_footprint().unwrap();
        assert!(record.footprint.writes.is_empty());
        assert_eq!(
            record.footprint.counterfactual_writes,
            BTreeSet::from([target(1), target(2), target(3)])
        );
        record.result = CommandResult::Applied(vec![AppliedWrite {
            target: target(4),
            before: None,
            after: None,
        }]);
        let original = record.clone();
        assert_eq!(
            record.normalize_footprint(),
            Err(ApplyFailure::InvalidAction)
        );
        assert_eq!(record, original);
    }
}
