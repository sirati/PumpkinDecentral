use std::collections::{BTreeMap, BTreeSet};

use crate::action_command::{
    ActionFootprint, ActorReference, CommandContext, CommandResult, MotionSpan, ResolutionRecord,
};
use crate::action_identity::ActionIdentity;
use crate::actor_occupancy::{ActorOccupancyEntry, OccupancyIdentity};
use crate::canonical_action::{
    ActionState, AppliedWrite, ApplyFailure, Cell, ChunkPart, Target, Value,
};

impl MotionSpan {
    fn entries<S: ActionState>(
        &self,
        context: &CommandContext<'_, S>,
    ) -> Result<(ActorOccupancyEntry, ActorOccupancyEntry), ApplyFailure> {
        let ActionIdentity::Issued(id) = context.identity else {
            return Err(ApplyFailure::InvalidAction);
        };
        if id.actor != self.actor.actor()
            || !self.cut.valid()
            || self.cut.sample_tick != context.tick
        {
            return Err(ApplyFailure::InvalidAction);
        }
        let body_target = self.actor.body_target();
        let root_target = Target::Chunk {
            key: self.actor.chunk(),
            part: ChunkPart::ActorOccupancy,
            inventory: None,
        };
        let body = context
            .values
            .get(&body_target)
            .ok_or(ApplyFailure::Condition(body_target))?;
        let root = context
            .values
            .get(&root_target)
            .ok_or(ApplyFailure::Condition(root_target))?;
        let Value::ActorOccupancy(root) = &root.value else {
            return Err(ApplyFailure::Type(root_target));
        };
        if !root.valid() || !root.is_complete() {
            return Err(ApplyFailure::Type(root_target));
        }
        let old = root
            .entries
            .get(&self.actor.actor())
            .ok_or(ApplyFailure::Condition(root_target))?;
        if old.body_target() != body_target
            || old.owner != self.actor.owner()
            || old.spatial_epoch != self.actor.epoch()
            || self.cut.sample_tick != old.cut.sample_tick
                && !self.cut.sample_tick.is_after(old.cut.sample_tick)
            || self.cut.sample_tick == old.cut.sample_tick
                && self.cut.simulation_sequence != old.cut.simulation_sequence
                && !self
                    .cut
                    .simulation_sequence
                    .is_newer_than(old.cut.simulation_sequence)
        {
            return Err(ApplyFailure::Condition(root_target));
        }
        match (&self.actor, &old.identity, &body.value) {
            (
                ActorReference::Player { lease, epoch, .. },
                OccupancyIdentity::Player(original),
                Value::Living(living),
            ) if original == lease
                && id.incarnation == lease.incarnation
                && context.state.validate_live_player_lease(*lease)
                && living.combat.actor == self.actor.actor()
                && living.avatar.as_ref().is_some_and(|avatar| {
                    avatar.spatial_epoch == *epoch && avatar.world == self.actor.chunk().world
                }) => {}
            (
                ActorReference::Entity {
                    id: entity,
                    birth,
                    owner,
                    epoch,
                    ..
                },
                OccupancyIdentity::Entity(original),
                Value::Entity(body),
            ) if original == birth
                && body.id == *entity
                && body.birth == *birth
                && body.owner == *owner
                && id.incarnation == owner.generation
                && body.spatial_epoch == *epoch => {}
            _ => return Err(ApplyFailure::Condition(body_target)),
        }
        if context
            .state
            .actor_collision_shape(body_target, &body.value)?
            != old.shape
        {
            return Err(ApplyFailure::Condition(root_target));
        }
        let mut next = old.clone();
        next.cut = self.cut;
        if !next.valid() {
            return Err(ApplyFailure::InvalidAction);
        }
        Ok((old.clone(), next))
    }

    pub fn footprint<S: ActionState>(
        &self,
        context: &CommandContext<'_, S>,
    ) -> Result<ActionFootprint, ApplyFailure> {
        let (old, next) = self.entries(context)?;
        let targets = crate::derived_actor_occupancy::root_targets(&old)
            .chain(crate::derived_actor_occupancy::root_targets(&next))
            .chain([self.actor.body_target()])
            .collect::<BTreeSet<_>>();
        crate::canonical_action::check_logical_budget(
            targets.len(),
            targets
                .len()
                .checked_mul(std::mem::size_of::<Target>())
                .ok_or(ApplyFailure::ResourceLimit)?,
        )?;
        Ok(ActionFootprint {
            reads: targets.clone(),
            writes: targets.clone(),
            counterfactual_reads: BTreeSet::new(),
            counterfactual_writes: targets,
        })
    }

    pub fn derive<S: ActionState>(
        &self,
        context: &CommandContext<'_, S>,
    ) -> Result<ResolutionRecord, ApplyFailure> {
        let (old, next) = self.entries(context)?;
        let footprint = self.footprint(context)?;
        let target = self.actor.body_target();
        let before = context
            .values
            .get(&target)
            .ok_or(ApplyFailure::Condition(target))?;
        let mut after = before.clone();
        after.revision = before
            .revision
            .checked_add(1)
            .ok_or(ApplyFailure::Overflow(target))?;
        match &mut after.value {
            Value::Living(living) => {
                let avatar = living.avatar.as_mut().ok_or(ApplyFailure::Type(target))?;
                avatar.position = self.cut.position;
                avatar.movement_tick = self.cut.sample_tick;
                avatar.movement_sequence = self.cut.simulation_sequence;
            }
            Value::Entity(body) => body.spatial.position = self.cut.position,
            _ => return Err(ApplyFailure::Type(target)),
        }
        let mut writes = vec![AppliedWrite {
            target,
            before: Some(before.clone()),
            after: Some(after),
        }];
        for target in footprint
            .reads
            .iter()
            .copied()
            .filter(|target| *target != self.actor.body_target())
        {
            let Target::Chunk { key, .. } = target else {
                return Err(ApplyFailure::Type(target));
            };
            let before = context
                .values
                .get(&target)
                .ok_or(ApplyFailure::Condition(target))?;
            let Value::ActorOccupancy(root) = &before.value else {
                return Err(ApplyFailure::Type(target));
            };
            if !root.valid()
                || !root.is_complete()
                || root.entries.get(&old.actor) != old.covers_chunk(key).then_some(&old)
            {
                return Err(ApplyFailure::Condition(target));
            }
            let changes =
                BTreeMap::from([(old.actor, next.covers_chunk(key).then_some(next.clone()))]);
            let (inverse, forward) = root
                .transition(&changes)
                .ok_or(ApplyFailure::Type(target))?;
            writes.push(AppliedWrite {
                target,
                before: Some(Cell {
                    revision: before.revision,
                    value: Value::ActorOccupancy(inverse),
                }),
                after: Some(Cell {
                    revision: before
                        .revision
                        .checked_add(1)
                        .ok_or(ApplyFailure::Overflow(target))?,
                    value: Value::ActorOccupancy(forward),
                }),
            });
        }
        writes.sort_unstable_by_key(|write| write.target);
        let records = crate::logical_records::count(&writes)?;
        let bytes = postcard::experimental::serialized_size(&writes)
            .map_err(|_| ApplyFailure::InvalidAction)?;
        crate::canonical_action::check_logical_budget(records, bytes)?;
        Ok(ResolutionRecord {
            identity: context.identity,
            tick: context.tick,
            footprint,
            result: CommandResult::Applied(writes),
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::actor::ActorId;
    use crate::actor_occupancy::{ActorOccupancyCut, ActorOccupancyRoot, CollisionShape};
    use crate::canonical_action::{ActionContext, ActionId, ActionRef, SpatialEpoch, TruthView};
    use crate::entity_body::{
        EntityBirth, EntityBody, EntityPayload, EntitySpatial, VehicleKind, VehicleState,
    };
    use crate::identity::{PlayerSeq, ServerId};
    use crate::protocol::{ChunkKey, GlobalEntityId, WorldId};
    use crate::realm_completion::PeerGeneration;
    use crate::time::TickStamp;

    struct State(CollisionShape);
    impl ActionState for State {
        fn read(&self, _: TruthView, _: Target) -> Option<Cell> {
            unreachable!()
        }
        fn commit(
            &mut self,
            _: ActionContext,
            _: TruthView,
            _: &[AppliedWrite],
        ) -> Result<(), ApplyFailure> {
            unreachable!()
        }
        fn actor_collision_shape(
            &self,
            _: Target,
            _: &Value,
        ) -> Result<CollisionShape, ApplyFailure> {
            Ok(self.0.clone())
        }
        fn actor_occupancy_cut(
            &self,
            _: Target,
            _: &Value,
        ) -> Result<(PeerGeneration, ActorOccupancyCut), ApplyFailure> {
            panic!("motion before state must use certified root anchor")
        }
    }

    #[test]
    fn motion_uses_certified_before_anchor_and_joint_span_roots_without_changing_epoch() {
        let world = WorldId::new(0, 7);
        let home = ChunkKey::new(world, 0, 0);
        let destination = ChunkKey::new(world, 1, 0);
        let owner = PeerGeneration {
            server: ServerId(4),
            generation: 11,
        };
        let entity = GlobalEntityId {
            origin: owner.server,
            local_id: 2,
            generation: 9,
        };
        let birth_ref = ActionRef {
            tick: TickStamp(3),
            id: ActionId {
                actor: ActorId::Entity(entity),
                incarnation: owner.generation,
                sequence: 2,
                family: 1,
            },
        };
        let birth = EntityBirth {
            action: birth_ref,
            source: owner,
            uuid: uuid::Uuid::from_u128(37),
        };
        let epoch = SpatialEpoch::Action(birth_ref);
        let original = ActorOccupancyCut {
            position: [8.0_f64, 0.0, 8.0].map(f64::to_bits),
            sample_tick: TickStamp(3),
            simulation_sequence: PlayerSeq(2),
        };
        let shape = CollisionShape {
            offsets: [
                [-0.7_f64, 0.0, -0.7].map(f64::to_bits),
                [0.7_f64, 0.6, 0.7].map(f64::to_bits),
            ],
            blocks_building: true,
            hanging: false,
        };
        let actor = ActorReference::Entity {
            id: entity,
            birth: birth.clone(),
            chunk: home,
            epoch,
            owner,
        };
        let entry = ActorOccupancyEntry {
            actor: actor.actor(),
            identity: OccupancyIdentity::Entity(birth.clone()),
            spatial_epoch: epoch,
            association: home,
            owner,
            cut: original,
            shape: shape.clone(),
        };
        let body = EntityBody {
            mount: None,
            tags: BTreeSet::new(),
            tags_revision: 0,
            relation_versions: BTreeMap::new(),
            passengers: BTreeMap::new(),
            id: entity,
            birth,
            owner,
            kind: 1,
            spatial: EntitySpatial {
                world,
                position: original.position,
                velocity_micro: [0; 3],
                yaw: 0,
                pitch: 0,
                on_ground: true,
            },
            spatial_epoch: epoch,
            payload: EntityPayload::Vehicle(VehicleState::new(VehicleKind::Boat)),
        };
        let root = |key| Target::Chunk {
            key,
            part: ChunkPart::ActorOccupancy,
            inventory: None,
        };
        let mut values = BTreeMap::from([
            (
                actor.body_target(),
                Cell {
                    revision: 4,
                    value: Value::Entity(Box::new(body)),
                },
            ),
            (
                root(home),
                Cell {
                    revision: 8,
                    value: Value::ActorOccupancy(
                        ActorOccupancyRoot::from_entries(vec![entry]).unwrap(),
                    ),
                },
            ),
            (
                root(destination),
                Cell {
                    revision: 2,
                    value: Value::ActorOccupancy(ActorOccupancyRoot::empty()),
                },
            ),
        ]);
        let command = MotionSpan {
            actor,
            cut: ActorOccupancyCut {
                position: [15.8_f64, 0.0, 8.0].map(f64::to_bits),
                sample_tick: TickStamp(4),
                simulation_sequence: PlayerSeq(3),
            },
        };
        let state = State(shape);
        let identity = ActionIdentity::Issued(ActionId {
            actor: command.actor.actor(),
            incarnation: owner.generation,
            sequence: 3,
            family: 1,
        });
        let context = CommandContext { known_absent: &std::collections::BTreeSet::new(),
            state: &state,
            identity,
            tick: TickStamp(4),
            values: &values,
        };
        let record = command.derive(&context).unwrap();
        assert_eq!(
            record.footprint.chunks(|_| None).unwrap(),
            BTreeSet::from([home, destination])
        );
        let CommandResult::Applied(writes) = &record.result else {
            panic!()
        };
        assert_eq!(writes.len(), 3);
        let body_write = writes
            .iter()
            .find(|write| write.target == command.actor.body_target())
            .unwrap();
        let Value::Entity(after) = &body_write.after.as_ref().unwrap().value else {
            panic!()
        };
        assert_eq!(after.spatial_epoch, epoch);
        assert_eq!(after.spatial.position, command.cut.position);
        for write in writes
            .iter()
            .filter(|write| write.target != command.actor.body_target())
        {
            let Value::ActorOccupancy(before) = &values[&write.target].value else {
                panic!()
            };
            let Value::ActorOccupancy(forward) = &write.after.as_ref().unwrap().value else {
                panic!()
            };
            let projected = forward.resolve_against(before).unwrap();
            assert_eq!(
                projected.entries.get(&command.actor.actor()).unwrap().cut,
                command.cut
            );
            let Value::ActorOccupancy(inverse) = &write.before.as_ref().unwrap().value else {
                panic!()
            };
            assert_eq!(inverse.resolve_against(&projected).unwrap(), *before);
        }
        values.remove(&root(destination));
        let context = CommandContext { known_absent: &std::collections::BTreeSet::new(),
            state: &state,
            identity,
            tick: TickStamp(4),
            values: &values,
        };
        assert_eq!(
            command.derive(&context).unwrap_err(),
            ApplyFailure::Condition(root(destination))
        );
    }
}
