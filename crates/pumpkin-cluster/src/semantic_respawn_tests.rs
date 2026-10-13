use super::*;
use crate::actor_occupancy::{ActorOccupancyEntry, ActorOccupancyRoot, OccupancyIdentity};
use crate::canonical_action::{
    ActionContext, AppliedWrite, OwnedStakeholderSet, TruthView, Value, apply_action, undo_action,
};
use crate::combat_action::tests::{RespawnState, fixture, respawn_shape};
use crate::combat_action::{CombatAttributeModifier, CombatModifierOperation};
use crate::item_action::PlayerRespawnState;
use crate::protocol::BlockPos;
use crate::realm_completion::PeerGeneration;
use crate::world_respawn::WorldRespawnResolution;
use crate::world_time::WorldBorderWitness;
use std::collections::{BTreeMap, BTreeSet};

struct State {
    inner: RespawnState,
    known_missing: BTreeSet<Target>,
}
impl ActionState for State {
    fn read(&self, view: TruthView, target: Target) -> Option<Cell> {
        if self.known_missing.contains(&target) { return None; }
        self.inner.read(view, target)
    }
    fn death_inventory_rule(&self, _: ActionRef, _: crate::world_rules::WorldRulesWitness, keep: bool, _: Target) -> Result<(), ApplyFailure> {
        if keep { Ok(()) } else { Err(ApplyFailure::InvalidAction) }
    }
    fn actor_collision_shape(
        &self,
        target: Target,
        value: &Value,
    ) -> Result<crate::actor_occupancy::CollisionShape, ApplyFailure> {
        self.inner.actor_collision_shape(target, value)
    }
    fn actor_occupancy_cut(&self, target: Target, value: &Value) -> Result<(PeerGeneration, crate::actor_occupancy::ActorOccupancyCut), ApplyFailure> {
        self.inner.actor_occupancy_cut(target, value)
    }
    fn status_effect_modifiers(
        &self,
        effects: &crate::active_effects::ActiveEffectSet,
    ) -> Result<Vec<CombatAttributeModifier>, ApplyFailure> {
        self.inner.status_effect_modifiers(effects)
    }
    fn validate_world_border(
        &self,
        witness: WorldBorderWitness,
        _: ActionRef,
    ) -> Result<(), ApplyFailure> {
        if witness.revision == 1 {
            Ok(())
        } else {
            Err(ApplyFailure::InvalidAction)
        }
    }
    fn resolve_world_respawn(
        &self,
        plan: WorldRespawnPlan,
        values: &BTreeMap<Target, Cell>,
    ) -> Result<Option<WorldRespawnResolution>, ApplyFailure> {
        if !matches!(
            values
                .get(&plan.target(plan.point.position))
                .map(|cell| &cell.value),
            Some(Value::Block(0))
        ) {
            return Ok(None);
        }
        Ok(Some(WorldRespawnResolution {
            position: [
                f64::from(plan.point.position.x) + 0.5,
                f64::from(plan.point.position.y),
                f64::from(plan.point.position.z) + 0.5,
            ]
            .map(f64::to_bits),
            yaw: plan.point.yaw,
            anchor: None,
        }))
    }
    fn commit(
        &mut self,
        context: ActionContext,
        view: TruthView,
        writes: &[AppliedWrite],
    ) -> Result<(), ApplyFailure> {
        self.inner.commit(context, view, writes)
    }
}
fn root(target: Target) -> Target {
    crate::derived_actor_occupancy::root_alias(target).unwrap()
}
fn setup() -> (State, SemanticRespawn, ActionId) {
    let (lease, mut dead, contact) = fixture(1);
    let id = ActionId {
        actor: contact.actor(),
        incarnation: lease.incarnation,
        sequence: 3,
        family: 7,
    };
    dead.combat.health_milli = 0;
    dead.combat.timing.death_cause = Some(ActionRef {
        tick: TickStamp(99),
        id: ActionId { sequence: 2, ..id },
    });
    dead.combat.timing.death_inventory_processed = true;
    dead.combat
        .effective_attributes
        .insert(6, 20.0_f64.to_bits());
    let plan = |x| WorldRespawnPlan {
        point: PlayerRespawnState {
            world: contact.world,
            position: BlockPos { x, y: 80, z: 8 },
            yaw: 0.0_f32.to_bits(),
            forced: true,
        },
        border: WorldBorderWitness {
            world: contact.world,
            issuer: PeerGeneration {
                server: lease.gid.server,
                generation: 1,
            },
            revision: 1,
            slot: 100,
            center: [0.0_f64.to_bits(); 2],
            diameter: 1000.0_f64.to_bits(),
        },
        min_y: 0,
        max_y: 128,
        half_width: 0.3_f64.to_bits(),
        height: 1.8_f64.to_bits(),
        bed_works: true,
        anchor_works: true,
        config_version: 1,
    };
    let intent = SemanticRespawn {
        lease,
        source: contact.chunk,
        preferred: Some(plan(24)),
        fallback: plan(40),
        avatar: dead.avatar.clone().unwrap(),
        max_health_attribute: 6,
        detach: None,
        death_inventory: None,
        light_plans: vec![],
    };
    let mut truth = OwnedStakeholderSet::default();
    for target in intent.targets().unwrap() {
        if matches!(
            target,
            Target::Chunk {
                part: ChunkPart::Block(_),
                ..
            }
        ) {
            truth.local.insert(
                target,
                Cell {
                    revision: 0,
                    value: Value::Block(0),
                },
            );
        }
        if let Target::Chunk {
            key,
            part: ChunkPart::Player(_),
            ..
        } = target
        {
            truth
                .local
                .entry(Target::Chunk {
                    key,
                    part: ChunkPart::ActorOccupancy,
                    inventory: None,
                })
                .or_insert(Cell {
                    revision: 0,
                    value: Value::ActorOccupancy(ActorOccupancyRoot::empty()),
                });
        }
    }
    truth.local.insert(
        contact.target(),
        Cell {
            revision: 4,
            value: Value::Living((dead.clone()).into()),
        },
    );
    truth.local.insert(
        root(contact.target()),
        Cell {
            revision: 8,
            value: Value::ActorOccupancy(
                ActorOccupancyRoot::from_entries(vec![ActorOccupancyEntry {
                    association: contact.chunk,
                    owner: contact.owner,
                    cut: crate::actor_occupancy::ActorOccupancyCut {
                        position: dead.avatar.as_ref().unwrap().position,
                        sample_tick: dead.avatar.as_ref().unwrap().movement_tick,
                        simulation_sequence: dead.avatar.as_ref().unwrap().movement_sequence,
                    },
                    actor: contact.actor(),
                    identity: OccupancyIdentity::Player(lease),
                    spatial_epoch: contact.epoch,
                    shape: respawn_shape(&dead),
                }])
                .unwrap(),
            ),
        },
    );
    truth.ground = truth.local.clone();
    (
        State {
            known_missing: BTreeSet::new(),
            inner: RespawnState {
                truth,
                removed_modifier: CombatAttributeModifier {
                    attribute: 6,
                    slot: 0,
                    id: "unused".into(),
                    amount: 0.0_f64.to_bits(),
                    operation: CombatModifierOperation::Add,
                },
            },
        },
        intent,
        id,
    )
}
fn destination(effects: &[Effect]) -> Target {
    effects
        .iter()
        .find_map(|effect| {
            if let Effect::RespawnPlayerAvatar { destination, .. } = effect {
                Some(*destination)
            } else {
                None
            }
        })
        .unwrap()
}
#[test]
fn captured_semantic_respawn_moves_complete_membership_in_both_truths_and_undoes() {
    let (mut state, intent, id) = setup();
    let action = intent.capture_action(&state, id, TickStamp(100)).unwrap();
    let chosen = destination(&action.effects);
    let Target::Chunk { key, .. } = chosen else {
        panic!()
    };
    assert_eq!(key.position.x, 1);
    for view in [TruthView::Local, TruthView::Ground] {
        let original = match view {
            TruthView::Local => state.inner.truth.local.clone(),
            TruthView::Ground => state.inner.truth.ground.clone(),
        };
        let applied = apply_action(&mut state, &action, view).unwrap();
        let Value::Living(living) = state.read(view, chosen).unwrap().value else {
            panic!()
        };
        assert_eq!(living.combat.health_milli, 20000);
        let Value::ActorOccupancy(membership) = state.read(view, root(chosen)).unwrap().value
        else {
            panic!()
        };
        assert_eq!(membership.entries.len(), 1);
        assert!(membership.entries[0].shape.blocks_building);
        assert_eq!(
            membership.entries[0].identity,
            OccupancyIdentity::Player(action.player_leases[0])
        );
        undo_action(&mut state, &applied).unwrap();
        assert_eq!(
            match view {
                TruthView::Local => &state.inner.truth.local,
                TruthView::Ground => &state.inner.truth.ground,
            },
            &original
        );
    }
}
#[test]
fn replay_selects_fallback_with_predeclared_body_and_occupancy_scope() {
    let (mut state, intent, id) = setup();
    let action = intent
        .clone()
        .capture_action(&state, id, TickStamp(100))
        .unwrap();
    let preferred = intent.preferred.unwrap();
    state
        .inner
        .truth
        .local
        .get_mut(&preferred.target(preferred.point.position))
        .unwrap()
        .value = Value::Block(1);
    let mut graph = crate::action_graph::TickActionGraph::default();
    graph.insert(action.clone()).unwrap();
    let mut targets = action.targets();
    targets.extend(action.counterfactual_conditions.iter().copied());
    targets.extend(action.counterfactual_effects.iter().copied());
    targets.extend(
        targets
            .clone()
            .into_iter()
            .filter_map(crate::derived_actor_occupancy::root_alias),
    );
    let anchors = targets
        .into_iter()
        .map(|target| (target, state.read(TruthView::Local, target)))
        .collect();
    let solved = crate::action_solver::solve_component_with_context(
        &graph,
        &BTreeSet::from([id]),
        &anchors,
        7,
        |_| Some(true),
        &state,
    )
    .unwrap();
    let crate::action_solver::SolvedVariant::Applied {
        effects,
        result,
        replayed,
        ..
    } = &solved.variants[&id]
    else {
        panic!()
    };
    assert!(*replayed);
    let fallback = destination(effects);
    let Target::Chunk { key, .. } = fallback else {
        panic!()
    };
    assert_eq!(key.position.x, 2);
    assert!(action.counterfactual_effects.contains(&fallback));
    assert!(
        result
            .writes
            .iter()
            .any(|write| write.target == root(fallback))
    );
    assert!(
        !result
            .writes
            .iter()
            .any(|write| write.target == root(destination(&action.effects)))
    );
}

#[test]
fn retained_close_replays_old_virtual_input_after_prior_death_is_undone() {
    use crate::canonical_inventory::InventorySlotBinding;
    use crate::canonical_inventory_close::{InventoryCloseDrop, InventoryCloseIntent};
    use crate::inventory::{InvLoc, InventoryCell, InventoryScreenSession, InventorySlotRef, InventoryTarget, INV_MAIN};
    use crate::living_property::LivingProperty as P;
    use crate::canonical_action::{Expected, InventoryId};
    let (mut state, mut respawn, id) = setup();
    let lease = respawn.lease;
    let (_, _, contact) = fixture(1);
    let source = contact.target();
    let menu_target = Target::Chunk { key: respawn.source, part: ChunkPart::LivingProperty { actor: id.actor, property: P::MenuSession }, inventory: None };
    let virtual_slot = InventorySlotBinding {
        slot: InventorySlotRef::new(InventoryTarget::LeasedVirtual { host: lease.gid.server, key: lease.key.0, player: lease.gid,
            incarnation: lease.incarnation, session: 7, kind: 10 }, InvLoc::new(INV_MAIN, 0)),
        target: Target::Player { key: lease.key, lease: Some(lease), inventory: Some(InventoryId::Virtual {
            host: lease.gid.server, kind: 10, session: 7, inv: INV_MAIN, slot: 0 }) }, max_stack: 64,
    };
    let destinations = (0..36).map(|slot| InventorySlotBinding {
        slot: InventorySlotRef::new(InventoryTarget::LeasedPlayer { key: lease.key.0, player: lease.gid, incarnation: lease.incarnation }, InvLoc::new(INV_MAIN, slot)),
        target: Target::Player { key: lease.key, lease: Some(lease), inventory: Some(InventoryId::Player { inv: INV_MAIN, slot }) }, max_stack: 64,
    }).collect::<Vec<_>>();
    let owner = contact.owner;
    let spatial = crate::entity_body::EntitySpatial { world: contact.world, position: contact.position, velocity_micro: [0; 3], yaw: 0, pitch: 0, on_ground: false };
    let entity = crate::protocol::GlobalEntityId { origin: owner.server, local_id: 31, generation: 1 };
    let drop_target = Target::Chunk { key: respawn.source, part: ChunkPart::Entity(entity), inventory: None };
    let reference = ActionRef { tick: TickStamp(100), id };
    let template = crate::entity_body::EntityBody {
        id: entity, birth: crate::entity_body::EntityBirth { action: reference, source: owner, uuid: uuid::Uuid::from_u128(31) }, owner,
        mount: None, tags: BTreeSet::new(), tags_revision: 0, relation_versions: BTreeMap::new(), passengers: BTreeMap::new(),
        kind: 1, spatial: spatial.clone(), spatial_epoch: contact.epoch,
        payload: crate::entity_body::EntityPayload::Item(crate::entity_body::DroppedItem { item: 1, count: 1, components: vec![0],
            age: 0, last_lifetime_tick: None, pickup_delay: 40, health_milli: 5_000, never_despawn: false, never_pickup: false, fire_immune: false }),
    };
    let close = InventoryCloseIntent { lease, session: InventoryScreenSession { actor: lease.gid, session: 7 }, token: 7, departure: false,
        menu: menu_target, window_kind: 9, sources: vec![virtual_slot.clone()], destinations: destinations.clone(), results: Vec::new(),
        overflow: vec![InventoryCloseDrop { source: virtual_slot.target, target: drop_target, template }] };
    let rules = crate::world_rules::WorldRulesWitness { rules: crate::world_rules::WorldRulesId { world: contact.world, issuer: owner, epoch: 1 }, slot: 100 };
    let death_cause = match &state.inner.truth.local[&source].value { Value::Living(body) => body.combat.timing.death_cause.unwrap(), _ => panic!() };
    let death = crate::death_inventory::DeathInventoryIntent { identity: crate::combat_action::LivingTickIdentity::from_contact(&contact),
        spatial, death_cause, keep_inventory: true, rules, drops: Vec::new(), equipment: Vec::new(), entity_equipment: None, close: Some(close.clone()) };
    let Value::Living(body) = &mut state.inner.truth.local.get_mut(&source).unwrap().value else { panic!() };
    body.combat.timing.death_inventory_processed = false;
    body.menu = crate::canonical_menu::MenuSessionState { session: 7, window_kind: 9, source: crate::canonical_menu::MenuSource::Virtual { kind: 10 }, ..Default::default() };
    for binding in &destinations { state.inner.truth.local.insert(binding.target, Cell { revision: 0, value: Value::Inventory(InventoryCell::empty(binding.slot.loc)) }); }
    state.inner.truth.local.insert(virtual_slot.target, Cell { revision: 0, value: Value::Inventory(InventoryCell { loc: virtual_slot.slot.loc, item: 1, count: 1, nbt: vec![0] }) });
    let mut prior = death.clone();
    state.inner.truth.ground = state.inner.truth.local.clone();
    let prior_id = ActionId { sequence: id.sequence - 1, ..id };
    prior.close.as_mut().unwrap().overflow[0].template.birth.action.id = prior_id;
    let prior_action = crate::death_inventory::capture(&state, prior_id, TickStamp(100), prior).unwrap();
    let applied = apply_action(&mut state, &prior_action, TruthView::Local).unwrap();
    let applied_ground = apply_action(&mut state, &prior_action, TruthView::Ground).unwrap();
    state.known_missing.insert(virtual_slot.target);
    respawn.death_inventory = Some(death);
    let action = respawn.clone().capture_action(&state, id, TickStamp(100)).unwrap();
    assert!(action.conditions.iter().any(|condition| condition.target == virtual_slot.target && condition.expected == Expected::Missing));
    state.known_missing.clear();
    undo_action(&mut state, &applied).unwrap();
    undo_action(&mut state, &applied_ground).unwrap();
    let mut graph = crate::action_graph::TickActionGraph::default();
    graph.insert(action.clone()).unwrap();
    let mut targets = action.targets();
    targets.extend(action.counterfactual_conditions.iter().copied());
    targets.extend(action.counterfactual_effects.iter().copied());
    targets.extend(targets.clone().into_iter().filter_map(crate::derived_actor_occupancy::root_alias));
    let anchors = targets.into_iter().map(|target| (target, state.read(TruthView::Local, target))).collect();
    let solved = crate::action_solver::solve_component_with_context(&graph, &BTreeSet::from([id]), &anchors, 9, |_| Some(true), &state).unwrap();
    let crate::action_solver::SolvedVariant::Applied { result, replayed, effects, .. } = &solved.variants[&id] else { panic!() };
    assert!(*replayed);
    let value = |target| result.writes.iter().find(|write| write.target == target).and_then(|write| write.after.as_ref()).map(|cell| &cell.value);
    assert!(matches!(value(virtual_slot.target), Some(Value::Inventory(cell)) if cell.is_empty()));
    assert!(matches!(value(destinations[0].target), Some(Value::Inventory(cell)) if cell.item == 1 && cell.count == 1));
    assert!(result.writes.iter().filter_map(|write| write.after.as_ref()).any(|cell| matches!(&cell.value,
        Value::Living(body) if body.menu.session == 0 && body.menu.last_close == Some(reference)
            && body.menu.closed_sessions.iter().any(|closed| closed.session == 7 && closed.action == reference))));
    let replay_capture = respawn.capture_action(&state, id, TickStamp(100)).unwrap();
    assert_eq!(&replay_capture.effects, effects);
    let relocated = destination(effects);
    for view in [TruthView::Local, TruthView::Ground] {
        let accepted = apply_action(&mut state, &replay_capture, view).unwrap();
        let Value::Living(body) = state.read(view, relocated).unwrap().value else { panic!() };
        assert_eq!(body.menu.last_close, Some(reference));
        assert_eq!(body.menu.closed_sessions.iter().filter(|closed| closed.session == 7).count(), 1);
        let Value::Inventory(item) = state.read(view, destinations[0].target).unwrap().value else { panic!() };
        assert_eq!(item.count, 1);
        undo_action(&mut state, &accepted).unwrap();
    }
}
