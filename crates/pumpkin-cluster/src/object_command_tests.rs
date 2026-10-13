use super::*;
use crate::{
    action_command::Hand,
    canonical_action::{
        ActionContext, ActionId, ActionRef, LivePlayerLease, OwnedStakeholderSet, SpatialEpoch,
        TruthView,
    },
    entity_body::{EntityBirth, EntityBody, EntitySpatial},
    identity::PlayerSeq,
    inventory::{INV_MAIN, InvLoc, InventoryCell},
    object_state::{FrameState, ObjectState},
    protocol::{BlockPos, GlobalEntityId},
    time::TickStamp,
};

struct Registry;
impl ObjectInteractionRegistry for Registry {
    fn object_interaction_attributes(&self) -> Result<ObjectInteractionAttributes, ApplyFailure> {
        Ok(ObjectInteractionAttributes { scale: 0, reach: 1 })
    }
    fn object_interaction_eye_height(&self, _: &ActorLivingState) -> Result<f64, ApplyFailure> {
        Ok(1.62)
    }
}
struct State {
    values: OwnedStakeholderSet,
    lease: LivePlayerLease,
}
impl ActionState for State {
    fn read(&self, view: TruthView, target: Target) -> Option<Cell> {
        self.values.read(view, target)
    }
    fn validate_live_player_lease(&self, lease: LivePlayerLease) -> bool {
        lease == self.lease
    }
    fn actor_collision_shape(
        &self,
        _: Target,
        _: &Value,
    ) -> Result<crate::actor_occupancy::CollisionShape, ApplyFailure> {
        Ok(crate::actor_occupancy::CollisionShape {
            offsets: [
                [-0.03125_f64, -0.375, -0.375].map(f64::to_bits),
                [0.03125_f64, 0.375, 0.375].map(f64::to_bits),
            ],
            blocks_building: false,
            hanging: true,
        })
    }
    fn commit(
        &mut self,
        context: ActionContext,
        view: TruthView,
        writes: &[AppliedWrite],
    ) -> Result<(), ApplyFailure> {
        self.values.commit(context, view, writes)
    }
}

fn fixture() -> (State, UseObject, ItemCommandSource, ActionId) {
    let (lease, mut living, contact) = crate::combat_action::tests::fixture(1);
    living
        .combat
        .effective_attributes
        .extend([(0, 1.0_f64.to_bits()), (1, 3.0_f64.to_bits())]);
    let source = ItemCommandSource {
        actor: ActorReference::Player {
            lease,
            chunk: contact.chunk,
            epoch: contact.epoch,
            owner: contact.owner,
        },
        hand: Hand::Main,
        cut: ActorOccupancyCut {
            position: contact.position,
            sample_tick: contact.simulation_tick,
            simulation_sequence: PlayerSeq(0),
        },
    };
    let entity = GlobalEntityId {
        origin: contact.owner.server,
        generation: contact.owner.generation,
        local_id: 8,
    };
    let birth_ref = ActionRef {
        id: ActionId {
            actor: ActorId::Entity(entity),
            incarnation: u64::from(entity.generation),
            sequence: 0,
            family: 1,
        },
        tick: TickStamp(0),
    };
    let birth = EntityBirth {
        action: birth_ref,
        source: contact.owner,
        uuid: uuid::Uuid::from_u128(8),
    };
    let position = [2.0_f64, 81.62, 1.0].map(f64::to_bits);
    let body = EntityBody {
        id: entity,
        birth: birth.clone(),
        owner: contact.owner,
        kind: 1,
        spatial: EntitySpatial {
            world: contact.world,
            position,
            velocity_micro: [0; 3],
            yaw: 0.0_f32.to_bits(),
            pitch: 0.0_f32.to_bits(),
            on_ground: true,
        },
        spatial_epoch: SpatialEpoch::Action(birth_ref),
        mount: None,
        passengers: Default::default(),
        relation_versions: Default::default(),
        tags: Default::default(),
        tags_revision: 1,
        payload: EntityPayload::Object(ObjectState {
            simulation_tick: None,
            property_versions: Default::default(),
            kind: ObjectKind::ItemFrame(FrameState {
                attachment: BlockPos { x: 2, y: 81, z: 1 },
                facing: 4,
                glow: false,
                item: InventoryCell {
                    loc: InvLoc::new(INV_MAIN, 0),
                    item: 1,
                    count: 1,
                    nbt: vec![0],
                },
                rotation: 7,
                drop_chance: 1.0_f32.to_bits(),
                invisible: false,
                fixed: false,
            }),
        }),
    };
    assert!(matches!(&body.payload, EntityPayload::Object(object) if object.valid()));
    let command = UseObject {
        target: ActorReference::Entity {
            id: entity,
            birth,
            owner: contact.owner,
            chunk: contact.chunk,
            epoch: body.spatial_epoch,
        },
        cut: ActorOccupancyCut {
            position,
            sample_tick: contact.simulation_tick,
            simulation_sequence: PlayerSeq(0),
        },
    };
    let id = ActionId {
        actor: ActorId::Player(lease.gid),
        incarnation: lease.incarnation,
        sequence: 1,
        family: 15,
    };
    let mut cells = std::collections::BTreeMap::new();
    cells.insert(
        source.actor.body_target(),
        Cell {
            revision: 1,
            value: Value::Living((living.clone()).into()),
        },
    );
    for property in [
        LivingProperty::Health,
        LivingProperty::GameMode,
        LivingProperty::PlayerStance,
        LivingProperty::Attribute(0),
        LivingProperty::PermanentAttributeModifiers(0),
        LivingProperty::Attribute(1),
        LivingProperty::PermanentAttributeModifiers(1),
    ] {
        cells.insert(
            UseObject::source_property(&source, property).unwrap(),
            Cell {
                revision: 1,
                value: Value::LivingProperty(property.read(&living)),
            },
        );
    }
    let EntityPayload::Object(ObjectState {
        kind: ObjectKind::ItemFrame(frame),
        ..
    }) = &body.payload
    else {
        unreachable!()
    };
    cells.insert(
        command.frame_inventory().unwrap(),
        Cell {
            revision: 1,
            value: Value::Inventory(frame.item.clone()),
        },
    );
    let value = Value::Entity((body).into());
    for property in [
        ObjectProperty::FramePolicy,
        ObjectProperty::FrameItem,
        ObjectProperty::FrameRotation,
        ObjectProperty::Attachment,
    ] {
        cells.insert(
            command.facet(property).unwrap(),
            Cell {
                revision: 1,
                value: Value::ObjectProperty(
                    crate::object_property::read(&value, property).unwrap(),
                ),
            },
        );
    }
    cells.insert(command.target.body_target(), Cell { revision: 1, value });
    (
        State {
            values: OwnedStakeholderSet {
                local: cells.clone(),
                ground: cells,
            },
            lease,
        },
        command,
        source,
        id,
    )
}

fn derive(
    state: &State,
    command: &UseObject,
    source: &ItemCommandSource,
    id: ActionId,
    view: TruthView,
) -> ResolutionRecord {
    let values = match view {
        TruthView::Local => &state.values.local,
        TruthView::Ground => &state.values.ground,
    };
    command
        .derive(
            &CommandContext {
                known_absent: &std::collections::BTreeSet::new(),
                state,
                identity: ActionIdentity::Issued(id),
                tick: TickStamp(100),
                values,
            },
            source,
            &Registry,
        )
        .unwrap()
}

#[test]
fn rotation_rederives_winning_facet_and_exactly_undoes_both_truth_views() {
    let (mut state, command, source, id) = fixture();
    let original = state.values.clone();
    for view in [TruthView::Local, TruthView::Ground] {
        let first = derive(&state, &command, &source, id, view);
        let CommandResult::Applied(writes) = &first.result else {
            panic!("rotation refused");
        };
        assert_eq!(writes.len(), 1);
        assert_eq!(
            writes[0].after.as_ref().unwrap().value,
            Value::ObjectProperty(ObjectPropertyValue::Rotation(0))
        );
        let context = ActionContext {
            id,
            tick: TickStamp(100),
            actor_chunk: Some(source.actor.chunk()),
        };
        state.commit(context, view, writes).unwrap();
        let second = derive(
            &state,
            &command,
            &source,
            ActionId { sequence: 2, ..id },
            view,
        );
        let CommandResult::Applied(writes) = &second.result else {
            panic!("second rotation refused");
        };
        assert_eq!(
            writes[0].after.as_ref().unwrap().value,
            Value::ObjectProperty(ObjectPropertyValue::Rotation(1))
        );
        state.commit(context, view, writes).unwrap();
        state
            .commit(context, view, &second.undo_writes().collect::<Vec<_>>())
            .unwrap();
        state
            .commit(context, view, &first.undo_writes().collect::<Vec<_>>())
            .unwrap();
    }
    assert_eq!(state.values.local, original.local);
    assert_eq!(state.values.ground, original.ground);
}

#[test]
fn changed_winning_range_or_fixed_policy_refuses_without_writes() {
    let (mut state, command, source, id) = fixture();
    let range = UseObject::source_property(&source, LivingProperty::Attribute(1)).unwrap();
    let original = state.values.local.clone();
    state.values.local.insert(
        range,
        Cell {
            revision: 2,
            value: Value::LivingProperty(crate::living_property::LivingPropertyValue::Attribute {
                base: Some(0.25_f64.to_bits()),
                modifiers: Vec::new(),
                transient_modifiers: Vec::new(),
            }),
        },
    );
    assert_eq!(
        derive(&state, &command, &source, id, TruthView::Local).result,
        CommandResult::NotApplied
    );
    state.values.local = original;
    let policy = command.facet(ObjectProperty::FramePolicy).unwrap();
    state.values.local.insert(
        policy,
        Cell {
            revision: 2,
            value: Value::ObjectProperty(ObjectPropertyValue::FramePolicy {
                fixed: true,
                invisible: false,
                drop_chance: 1.0_f32.to_bits(),
            }),
        },
    );
    let before = state.values.local.clone();
    assert_eq!(
        derive(&state, &command, &source, id, TruthView::Local).result,
        CommandResult::NotApplied
    );
    assert_eq!(state.values.local, before);
}

#[test]
fn command_wire_roundtrip_has_no_serialized_resolution_state() {
    let (_, command, _, _) = fixture();
    let bytes = postcard::to_allocvec(&command).unwrap();
    assert!(bytes.len() < 192);
    assert_eq!(postcard::from_bytes::<UseObject>(&bytes).unwrap(), command);
}

#[test]
fn empty_canonical_frame_slot_refuses_despite_nonempty_render_mirror() {
    let (mut state, command, source, id) = fixture();
    state.values.local.insert(
        command.frame_inventory().unwrap(),
        Cell {
            revision: 2,
            value: Value::Inventory(InventoryCell::empty(InvLoc::new(INV_MAIN, 0))),
        },
    );
    let before = state.values.local.clone();
    assert_eq!(
        derive(&state, &command, &source, id, TruthView::Local).result,
        CommandResult::NotApplied
    );
    assert_eq!(state.values.local, before);
}

#[test]
fn unavailable_or_contradictory_source_evidence_refuses_without_writes() {
    let (mut state, command, source, id) = fixture();
    let missing = UseObject::source_property(&source, LivingProperty::Health).unwrap();
    state.values.local.remove(&missing);
    let known_absent = BTreeSet::new();
    let context = CommandContext {
        state: &state,
        identity: ActionIdentity::Issued(id),
        tick: TickStamp(100),
        values: &state.values.local,
        known_absent: &known_absent,
    };
    assert_eq!(
        command.derive(&context, &source, &Registry),
        Err(ApplyFailure::MissingStakeholder(missing.stakeholder()))
    );
    let absent = BTreeSet::from([missing]);
    let absent_context = CommandContext {
        known_absent: &absent,
        ..context
    };
    assert_eq!(
        command.derive(&absent_context, &source, &Registry),
        Err(ApplyFailure::Condition(missing))
    );
    let contradictory = BTreeSet::from([command.target.body_target()]);
    let context = CommandContext {
        known_absent: &contradictory,
        ..context
    };
    assert_eq!(
        command.derive(&context, &source, &Registry),
        Err(ApplyFailure::InvalidAction)
    );
}
