use crate::actor::ActorId;
use crate::actor_occupancy::{ActorOccupancyEntry, OccupancyIdentity};
use crate::canonical_action::{
    ActionState, AppliedWrite, ApplyFailure, Cell, ChunkPart, SpatialEpoch, Target, TruthView,
    Value,
};
use std::collections::{BTreeMap, BTreeSet};

pub fn body_target(target: Target) -> Option<Target> {
    use crate::living_property::LivingProperty as P;
    match target {
        Target::Chunk {
            part: ChunkPart::Entity(_) | ChunkPart::Player(_),
            inventory: None,
            ..
        } => Some(target),
        Target::Chunk {
            part:
                ChunkPart::LivingProperty {
                    property:
                        P::Health
                        | P::GameMode
                        | P::PlayerStance
                        | P::StatusEffects
                        | P::Attribute(_)
                        | P::PermanentAttributeModifiers(_)
                        | P::ArmorStand(_)
                        | P::MobSpecies
                        | P::MobSpeciesProperty(_)
                        | P::MobReproduction
                        | P::MobContinuation
                        | P::Weapon
                        | P::ArmorProtection,
                    ..
                },
            inventory: None,
            ..
        }
        | Target::Chunk {
            part: ChunkPart::ObjectProperty { .. },
            inventory: None,
            ..
        } => crate::derived_living::actor_body_target(target),
        _ => None,
    }
}

pub fn root_alias(target: Target) -> Option<Target> {
    let Target::Chunk { key, .. } = body_target(target)? else {
        return None;
    };
    Some(Target::Chunk {
        key,
        part: ChunkPart::ActorOccupancy,
        inventory: None,
    })
}

fn actor(target: Target) -> Result<ActorId, ApplyFailure> {
    match target {
        Target::Chunk {
            part: ChunkPart::Player(gid),
            inventory: None,
            ..
        } => Ok(ActorId::Player(gid)),
        Target::Chunk {
            part: ChunkPart::Entity(id),
            inventory: None,
            ..
        } => Ok(ActorId::Entity(id)),
        _ => Err(ApplyFailure::Type(target)),
    }
}

fn entry_with_cut<S: ActionState>(
    owners: &S,
    target: Target,
    value: &Value,
    prior: Option<&ActorOccupancyEntry>,
    explicit_cut: Option<(
        crate::realm_completion::PeerGeneration,
        crate::actor_occupancy::ActorOccupancyCut,
    )>,
) -> Result<ActorOccupancyEntry, ApplyFailure> {
    let actor = actor(target)?;
    let (identity, spatial_epoch) = match (actor, value) {
        (ActorId::Player(gid), Value::Living(living))
            if living.combat.actor == actor && living.food.actor == actor =>
        {
            let avatar = living.avatar.as_ref().ok_or(ApplyFailure::Type(target))?;
            let lease = match avatar.spatial_epoch {
                SpatialEpoch::Admission { lease, .. } if lease.gid == gid => lease,
                SpatialEpoch::Action(_) => match prior.map(|entry| &entry.identity) {
                    Some(OccupancyIdentity::Player(lease)) if lease.gid == gid => *lease,
                    _ => return Err(ApplyFailure::Type(target)),
                },
                _ => return Err(ApplyFailure::Type(target)),
            };
            (OccupancyIdentity::Player(lease), avatar.spatial_epoch)
        }
        (ActorId::Entity(id), Value::Entity(body)) if body.id == id => (
            OccupancyIdentity::Entity(body.birth.clone()),
            body.spatial_epoch,
        ),
        _ => return Err(ApplyFailure::Type(target)),
    };
    let Target::Chunk {
        key: association, ..
    } = target
    else {
        return Err(ApplyFailure::Type(target));
    };
    let world = match value {
        Value::Living(living) => living.avatar.as_ref().map(|avatar| avatar.world),
        Value::Entity(body) => Some(body.spatial.world),
        _ => None,
    };
    if world != Some(association.world) {
        return Err(ApplyFailure::Type(target));
    }
    let (owner, cut) = match explicit_cut {
        Some(cut) => cut,
        None => owners.actor_occupancy_cut(target, value)?,
    };
    let entry = ActorOccupancyEntry {
        association,
        owner,
        cut,
        actor,
        identity,
        spatial_epoch,
        shape: owners.actor_collision_shape(target, value)?,
    };
    if !entry.valid() {
        return Err(ApplyFailure::Type(target));
    }
    Ok(entry)
}

fn entry<S: ActionState>(
    owners: &S,
    target: Target,
    value: &Value,
    prior: Option<&ActorOccupancyEntry>,
) -> Result<ActorOccupancyEntry, ApplyFailure> {
    entry_with_cut(owners, target, value, prior, None)
}

fn load_root<'a, S: ActionState>(
    owners: &S,
    view: TruthView,
    roots: &'a mut BTreeMap<Target, Cell>,
    target: Target,
) -> Result<&'a Cell, ApplyFailure> {
    if !roots.contains_key(&target) {
        let cell = owners
            .read(view, target)
            .ok_or(ApplyFailure::Condition(target))?;
        if !matches!(&cell.value, Value::ActorOccupancy(root) if root.valid() && root.is_complete())
        {
            return Err(ApplyFailure::Type(target));
        }
        roots.insert(target, cell);
    }
    Ok(&roots[&target])
}

pub fn root_targets(entry: &ActorOccupancyEntry) -> impl Iterator<Item = Target> + '_ {
    entry.covered_chunks().map(|key| Target::Chunk {
        key,
        part: ChunkPart::ActorOccupancy,
        inventory: None,
    })
}

fn same_index_facts(left: &ActorOccupancyEntry, right: &ActorOccupancyEntry) -> bool {
    left.actor == right.actor
        && left.identity == right.identity
        && left.spatial_epoch == right.spatial_epoch
        && left.association == right.association
        && left.owner == right.owner
        && left.shape == right.shape
}

pub fn normalize<S: ActionState>(
    owners: &S,
    view: TruthView,
    writes: &mut Vec<AppliedWrite>,
) -> Result<(), ApplyFailure> {
    normalize_with_cuts(owners, view, writes, &BTreeMap::new())
}

pub fn normalize_with_cuts<S: ActionState>(
    owners: &S,
    view: TruthView,
    writes: &mut Vec<AppliedWrite>,
    cuts: &BTreeMap<
        ActorId,
        (
            crate::realm_completion::PeerGeneration,
            crate::actor_occupancy::ActorOccupancyCut,
        ),
    >,
) -> Result<(), ApplyFailure> {
    let bodies = writes
        .iter()
        .filter_map(|write| body_target(write.target))
        .collect::<BTreeSet<_>>();
    if bodies.is_empty() {
        return Ok(());
    }
    let mut roots = BTreeMap::<Target, Cell>::new();
    let mut prior = BTreeMap::<ActorId, ActorOccupancyEntry>::new();
    let mut states = BTreeMap::new();
    for body in bodies {
        let id = actor(body)?;
        let target = root_alias(body).ok_or(ApplyFailure::Type(body))?;
        let root_cell = load_root(owners, view, &mut roots, target)?;
        let Value::ActorOccupancy(root) = &root_cell.value else {
            unreachable!()
        };
        let before = owners.borrow(view, body);
        if let Some(before) = &before {
            let old = root
                .entries
                .get(&id)
                .ok_or(ApplyFailure::Condition(target))?;
            if old.body_target() != body {
                return Err(ApplyFailure::Condition(target));
            }
            let observed = entry(owners, body, &before.value, Some(old))?;
            if !same_index_facts(old, &observed)
                || !old.accepts_cut(observed.cut)
                || prior.insert(id, old.clone()).is_some()
            {
                return Err(ApplyFailure::Condition(target));
            }
        } else if root
            .entries
            .get(&id)
            .is_some_and(|old| old.body_target() == body)
        {
            return Err(ApplyFailure::Condition(target));
        }
        let after = crate::derived_living::project_body_writes(body, before.as_deref(), writes)?;
        states.insert(body, after);
    }
    let mut next = BTreeMap::<ActorId, ActorOccupancyEntry>::new();
    for (body, after) in states {
        let id = actor(body)?;
        if let Some(after) = after {
            let mut new = entry_with_cut(
                owners,
                body,
                &after.value,
                prior.get(&id),
                cuts.get(&id).copied(),
            )?;
            if let Some(old) = prior
                .get(&id)
                .filter(|old| same_index_facts(old, &new) && old.accepts_cut(new.cut))
            {
                new.cut = old.cut;
            }
            if next.insert(id, new).is_some() {
                return Err(ApplyFailure::Type(body));
            }
        }
    }
    let actors = prior
        .keys()
        .chain(next.keys())
        .copied()
        .collect::<BTreeSet<_>>();
    let mut changes = BTreeMap::<Target, BTreeMap<ActorId, Option<ActorOccupancyEntry>>>::new();
    let mut records = writes.len();
    for id in actors {
        let old = prior.get(&id);
        let new = next.get(&id);
        if old == new {
            continue;
        }
        let targets = old
            .into_iter()
            .flat_map(root_targets)
            .chain(new.into_iter().flat_map(root_targets))
            .collect::<BTreeSet<_>>();
        for target in targets {
            records = records.checked_add(1).ok_or(ApplyFailure::ResourceLimit)?;
            if records > crate::canonical_action::MAX_LOGICAL_RECORDS {
                return Err(ApplyFailure::ResourceLimit);
            }
            let Target::Chunk { key, .. } = target else {
                unreachable!()
            };
            let cell = load_root(owners, view, &mut roots, target)?;
            let Value::ActorOccupancy(root) = &cell.value else {
                unreachable!()
            };
            let expected = old.filter(|entry| entry.covers_chunk(key));
            if root.entries.get(&id) != expected {
                return Err(ApplyFailure::Condition(target));
            }
            changes
                .entry(target)
                .or_default()
                .insert(id, new.filter(|entry| entry.covers_chunk(key)).cloned());
        }
    }
    for (target, changes) in changes {
        let mut before = roots
            .remove(&target)
            .ok_or(ApplyFailure::Condition(target))?;
        let Value::ActorOccupancy(root) = &before.value else {
            return Err(ApplyFailure::Type(target));
        };
        let (inverse, forward) = root
            .transition(&changes)
            .ok_or(ApplyFailure::Type(target))?;
        before.value = Value::ActorOccupancy(inverse);
        let revision = before
            .revision
            .checked_add(1)
            .ok_or(ApplyFailure::Overflow(target))?;
        writes.push(AppliedWrite {
            target,
            before: Some(before),
            after: Some(Cell {
                revision,
                value: Value::ActorOccupancy(forward),
            }),
        });
    }
    writes.sort_unstable_by_key(|write| write.target);
    let bytes =
        postcard::experimental::serialized_size(writes).map_err(|_| ApplyFailure::InvalidAction)?;
    crate::canonical_action::check_logical_budget(records, bytes)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::actor_occupancy::ActorOccupancyRoot;
    use crate::canonical_action::{ActionContext, ActionId, ActionRef};
    use crate::entity_body::{
        EntityBirth, EntityBody, EntityPayload, EntitySpatial, VehicleKind, VehicleState,
    };
    use crate::identity::ServerId;
    use crate::protocol::{ChunkKey, GlobalEntityId, WorldId};
    use crate::realm_completion::PeerGeneration;
    use crate::time::TickStamp;

    #[derive(Default)]
    struct State(
        BTreeMap<Target, Cell>,
        Option<crate::actor_occupancy::CollisionShape>,
    );
    impl ActionState for State {
        fn actor_occupancy_cut(
            &self,
            target: Target,
            value: &Value,
        ) -> Result<(PeerGeneration, crate::actor_occupancy::ActorOccupancyCut), ApplyFailure>
        {
            let Value::Entity(body) = value else {
                return Err(ApplyFailure::Type(target));
            };
            let SpatialEpoch::Action(reference) = body.spatial_epoch else {
                return Err(ApplyFailure::Type(target));
            };
            Ok((
                body.owner,
                crate::actor_occupancy::ActorOccupancyCut {
                    position: body.spatial.position,
                    sample_tick: reference.tick,
                    simulation_sequence: crate::identity::PlayerSeq(reference.id.sequence as u16),
                },
            ))
        }
        fn read(&self, _: TruthView, target: Target) -> Option<Cell> {
            self.0.get(&target).cloned()
        }
        fn actor_collision_shape(
            &self,
            target: Target,
            value: &Value,
        ) -> Result<crate::actor_occupancy::CollisionShape, ApplyFailure> {
            if let Some(shape) = &self.1 {
                return Ok(shape.clone());
            }
            let Value::Entity(body) = value else {
                return Err(ApplyFailure::Type(target));
            };
            let EntityPayload::Vehicle(vehicle) = &body.payload else {
                return Err(ApplyFailure::Type(target));
            };
            let radius = if vehicle.kind == VehicleKind::Boat {
                0.7_f64
            } else {
                0.49_f64
            };
            Ok(crate::actor_occupancy::CollisionShape {
                offsets: [
                    [-radius, 0.0, -radius].map(f64::to_bits),
                    [radius, 0.6, radius].map(f64::to_bits),
                ],
                blocks_building: true,
                hanging: false,
            })
        }
        fn commit(
            &mut self,
            _: ActionContext,
            _: TruthView,
            _: &[AppliedWrite],
        ) -> Result<(), ApplyFailure> {
            unreachable!()
        }
    }
    fn chunk(x: i32) -> ChunkKey {
        ChunkKey::new(WorldId::new(0, 7), x, 0)
    }
    fn fixture() -> (Target, Cell) {
        let id = GlobalEntityId {
            origin: ServerId(4),
            local_id: 2,
            generation: 9,
        };
        let reference = ActionRef {
            tick: TickStamp(3),
            id: ActionId {
                actor: ActorId::Entity(id),
                incarnation: 9,
                sequence: 2,
                family: 1,
            },
        };
        let owner = PeerGeneration {
            server: ServerId(4),
            generation: 11,
        };
        let body = EntityBody {
            mount: None,
            tags: BTreeSet::new(),
            tags_revision: 0,
            relation_versions: BTreeMap::new(),
            passengers: BTreeMap::new(),
            id,
            birth: EntityBirth {
                action: reference,
                source: owner,
                uuid: uuid::Uuid::from_u128(37),
            },
            owner,
            kind: 1,
            spatial: EntitySpatial {
                world: chunk(0).world,
                position: [8.0_f64.to_bits(), 0.0_f64.to_bits(), 8.0_f64.to_bits()],
                velocity_micro: [0; 3],
                yaw: 0,
                pitch: 0,
                on_ground: true,
            },
            spatial_epoch: SpatialEpoch::Action(reference),
            payload: EntityPayload::Vehicle(VehicleState::new(VehicleKind::Boat)),
        };
        (
            Target::Chunk {
                key: chunk(0),
                part: ChunkPart::Entity(id),
                inventory: None,
            },
            Cell {
                revision: 4,
                value: Value::Entity((body).into()),
            },
        )
    }
    fn seed_root(state: &mut State, body: Target, cells: Vec<ActorOccupancyEntry>) {
        state.0.insert(
            root_alias(body).unwrap(),
            Cell {
                revision: 8,
                value: Value::ActorOccupancy(ActorOccupancyRoot::from_entries(cells).unwrap()),
            },
        );
    }
    #[test]
    fn distant_large_actor_is_indexed_at_every_intersected_chunk_and_motion_is_joint() {
        use crate::actor_occupancy::{
            ActorOccupancyCut, ActorOccupancyPose, ActorOccupancyRoot, ActorOccupancyWitness,
            CollisionBox, CollisionShape,
        };
        let (original_target, mut body_cell) = fixture();
        let Target::Chunk { part, .. } = original_target else {
            panic!()
        };
        let target = Target::Chunk {
            key: chunk(3),
            part,
            inventory: None,
        };
        let Value::Entity(body) = &mut body_cell.value else {
            panic!()
        };
        body.spatial.position = [48.0_f64, 0.0, 0.5].map(f64::to_bits);
        let id = ActorId::Entity(body.id);
        let owner = body.owner;
        let mut state = State::default();
        state.1 = Some(CollisionShape {
            offsets: [
                [-48.0_f64, 0.0, -0.5].map(f64::to_bits),
                [48.0_f64, 1.0, 0.5].map(f64::to_bits),
            ],
            blocks_building: true,
            hanging: false,
        });
        for x in 0..=5 {
            state.0.insert(
                Target::Chunk {
                    key: chunk(x),
                    part: ChunkPart::ActorOccupancy,
                    inventory: None,
                },
                Cell {
                    revision: 0,
                    value: Value::ActorOccupancy(ActorOccupancyRoot::empty()),
                },
            );
        }
        let mut writes = vec![AppliedWrite {
            target,
            before: None,
            after: Some(body_cell.clone()),
        }];
        normalize(&state, TruthView::Local, &mut writes).unwrap();
        assert_eq!(writes.len(), 7);
        for write in writes.iter().filter(|write| write.target != target) {
            let complete =
                crate::actor_occupancy::materialize_write(write, &state.0[&write.target]).unwrap();
            state.0.insert(write.target, complete);
        }
        state.0.insert(target, body_cell.clone());
        let queried = Target::Chunk {
            key: chunk(0),
            part: ChunkPart::ActorOccupancy,
            inventory: None,
        };
        let cell = &state.0[&queried];
        let Value::ActorOccupancy(root) = &cell.value else {
            panic!()
        };
        let member = root.entries.get(&id).unwrap().clone();
        assert_eq!(member.association, chunk(3));
        let witness = ActorOccupancyWitness {
            key: chunk(0),
            revision: cell.revision,
            poses: vec![ActorOccupancyPose {
                sample_tick: member.cut.sample_tick,
                position: member.cut.position,
                simulation_sequence: member.cut.simulation_sequence.0,
                entry: member,
            }],
            blocks: vec![CollisionBox {
                bounds: [
                    [0.0_f64, 0.0, 0.0].map(f64::to_bits),
                    [1.0_f64, 1.0, 1.0].map(f64::to_bits),
                ],
            }],
        };
        assert!(witness.matches_domain(cell));
        assert!(!witness.matches_clear(cell));
        let mut moved = body_cell.clone();
        let Value::Entity(body) = &mut moved.value else {
            panic!()
        };
        body.spatial.position[0] = 63.0_f64.to_bits();
        let cut = ActorOccupancyCut {
            position: body.spatial.position,
            sample_tick: TickStamp(4),
            simulation_sequence: crate::identity::PlayerSeq(3),
        };
        let cuts = BTreeMap::from([(id, (owner, cut))]);
        let movement = AppliedWrite {
            target,
            before: Some(body_cell),
            after: Some(moved),
        };
        let mut writes = vec![movement.clone()];
        assert!(
            matches!(normalize_with_cuts(&state, TruthView::Local, &mut writes, &cuts), Err(ApplyFailure::Condition(Target::Chunk { key, part: ChunkPart::ActorOccupancy, .. })) if key == chunk(6))
        );
        assert_eq!(writes, vec![movement.clone()]);
        let extra = Target::Chunk {
            key: chunk(6),
            part: ChunkPart::ActorOccupancy,
            inventory: None,
        };
        state.0.insert(
            extra,
            Cell {
                revision: 0,
                value: Value::ActorOccupancy(ActorOccupancyRoot::empty()),
            },
        );
        let mut writes = vec![movement];
        normalize_with_cuts(&state, TruthView::Local, &mut writes, &cuts).unwrap();
        assert_eq!(writes.len(), 8);
        for write in writes.iter().filter(|write| write.target != target) {
            let complete =
                crate::actor_occupancy::materialize_write(write, &state.0[&write.target]).unwrap();
            let Value::ActorOccupancy(root) = &complete.value else {
                panic!()
            };
            let member = root.entries.get(&id).unwrap();
            assert_eq!(member.cut, cut);
            assert_eq!(member.owner, owner);
            assert_eq!(member.association, chunk(3));
            assert!(witness.poses[0].entry.identity == member.identity);
        }
    }

    #[test]
    fn birth_requires_complete_root_and_retains_exact_undo_root() {
        let (body, after) = fixture();
        let mut state = State::default();
        let birth = AppliedWrite {
            target: body,
            before: None,
            after: Some(after.clone()),
        };
        assert!(matches!(
            normalize(&state, TruthView::Local, &mut vec![birth.clone()]),
            Err(ApplyFailure::Condition(_))
        ));
        seed_root(&mut state, body, vec![]);
        let original = state.0[&root_alias(body).unwrap()].clone();
        let mut writes = vec![birth];
        normalize(&state, TruthView::Local, &mut writes).unwrap();
        let root = writes
            .iter()
            .find(|write| write.target == root_alias(body).unwrap())
            .unwrap();
        assert_eq!(root.before.as_ref(), Some(&original));
        assert_eq!(root.after.as_ref().unwrap().revision, original.revision + 1);
        let Value::ActorOccupancy(membership) = &root.after.as_ref().unwrap().value else {
            panic!()
        };
        assert_eq!(
            membership.entries.iter().cloned().collect::<Vec<_>>(),
            vec![entry(&state, body, &after.value, None).unwrap()]
        );
    }
    #[test]
    fn joint_chunk_move_updates_both_roots_and_preserves_birth_identity() {
        let (source, before) = fixture();
        let Target::Chunk { part, .. } = source else {
            panic!()
        };
        let destination = Target::Chunk {
            key: chunk(1),
            part,
            inventory: None,
        };
        let mut state = State::default();
        let old = entry(&state, source, &before.value, None).unwrap();
        state.0.insert(source, before.clone());
        seed_root(&mut state, source, vec![old.clone()]);
        seed_root(&mut state, destination, vec![]);
        let mut after = before.clone();
        let Value::Entity(body) = &mut after.value else {
            panic!()
        };
        body.spatial.position[0] = 17.0_f64.to_bits();
        let SpatialEpoch::Action(reference) = &mut body.spatial_epoch else {
            panic!()
        };
        reference.id.sequence += 1;
        let mut writes = vec![
            AppliedWrite {
                target: source,
                before: Some(before),
                after: None,
            },
            AppliedWrite {
                target: destination,
                before: None,
                after: Some(after),
            },
        ];
        normalize(&state, TruthView::Ground, &mut writes).unwrap();
        let roots = writes
            .iter()
            .filter(|write| {
                matches!(
                    write.target,
                    Target::Chunk {
                        part: ChunkPart::ActorOccupancy,
                        ..
                    }
                )
            })
            .collect::<Vec<_>>();
        assert_eq!(roots.len(), 2);
        for write in roots {
            assert_eq!(write.before.as_ref(), state.0.get(&write.target));
            let Value::ActorOccupancy(root) = &write.after.as_ref().unwrap().value else {
                panic!()
            };
            if write.target == root_alias(source).unwrap() {
                assert!(root.entries.is_empty());
            } else {
                assert_eq!(root.entries.len(), 1);
                assert_eq!(root.entries[0].identity, old.identity);
                assert_ne!(root.entries[0].spatial_epoch, old.spatial_epoch);
            }
        }
    }
    #[test]
    fn shape_change_invalidates_membership_but_nonshape_update_does_not() {
        let (target, before) = fixture();
        let mut state = State::default();
        let old = entry(&state, target, &before.value, None).unwrap();
        state.0.insert(target, before.clone());
        seed_root(&mut state, target, vec![old]);
        let mut after = before.clone();
        let Value::Entity(body) = &mut after.value else {
            panic!()
        };
        let EntityPayload::Vehicle(vehicle) = &mut body.payload else {
            panic!()
        };
        vehicle.damage_milli = 100;
        let mut writes = vec![AppliedWrite {
            target,
            before: Some(before.clone()),
            after: Some(after.clone()),
        }];
        normalize(&state, TruthView::Local, &mut writes).unwrap();
        assert_eq!(writes.len(), 1);
        let Value::Entity(body) = &mut after.value else {
            panic!()
        };
        let EntityPayload::Vehicle(vehicle) = &mut body.payload else {
            panic!()
        };
        vehicle.kind = VehicleKind::Minecart;
        let mut writes = vec![AppliedWrite {
            target,
            before: Some(before),
            after: Some(after),
        }];
        normalize(&state, TruthView::Local, &mut writes).unwrap();
        assert_eq!(writes.len(), 2);
    }
}
