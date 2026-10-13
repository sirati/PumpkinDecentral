use crate::actor::ActorId;
use crate::canonical_action::*;
use crate::identity::ServerId;
use crate::protocol::{BlockPos, ChunkKey, WorldId};
use crate::realm_completion::PeerGeneration;
use crate::time::TickStamp;

fn fixture(
    other_pending: bool,
) -> (
    OwnedStakeholderSet,
    Action,
    Target,
    ScheduledTickFact,
    ScheduledTickFact,
) {
    let chunk = ChunkKey::new(WorldId::new(0, 1), 0, 0);
    let target = Target::Chunk {
        key: chunk,
        part: ChunkPart::Whole,
        inventory: None,
    };
    let source = PeerGeneration {
        server: ServerId(1),
        generation: 11,
    };
    let id = ActionId {
        actor: ActorId::Peer(source.server),
        incarnation: source.generation,
        sequence: 9,
        family: 8,
    };
    let tick = TickStamp(20);
    let current = ScheduledTickFact {
        executor: source,
        origin: ScheduledTickOrigin::GenesisPrimary,
        kind: ScheduledTickKind::Block,
        id: 7,
        position: BlockPos { x: 2, y: 64, z: 2 },
        due: tick,
        priority: 0,
        source_order: 0,
    };
    let next = ScheduledTickFact {
        origin: ScheduledTickOrigin::Action {
            cause: ActionRef { id, tick },
            producer: source,
        },
        due: TickStamp(22),
        source_order: 9,
        ..current
    };
    let mut queue = vec![current];
    if other_pending {
        queue.push(next);
    }
    let mut state = OwnedStakeholderSet::default();
    for view in [&mut state.local, &mut state.ground] {
        view.insert(
            target,
            Cell {
                revision: 5,
                value: Value::ScheduledQueue(queue.clone()),
            },
        );
    }
    let mut effects = vec![Effect::CompleteScheduledTick {
        target,
        fact: current,
    }];
    if !other_pending {
        effects.push(Effect::ScheduleTick { target, tick: next });
    }
    let action = Action {
        edit_authority: None,
        id,
        tick,
        class: ActionClass::Undoable,
        chunk_transition: false,
        actor_chunk: Some(chunk),
        player_leases: Vec::new(),
        player_chunks: Vec::new(),
        stakeholders: vec![Stakeholder::Chunk(chunk)],
        dependencies: Vec::new(),
        conditions: vec![
            Condition {
                target,
                expected: Expected::ScheduledTickFact {
                    fact: current,
                    present: true,
                },
            },
            Condition {
                target,
                expected: Expected::ScheduledTickPresenceExcept {
                    kind: current.kind,
                    id: current.id,
                    position: current.position,
                    except: current,
                    present: other_pending,
                },
            },
        ],
        effects,
        counterfactual_conditions: Vec::new(),
        counterfactual_effects: Vec::new(),
        semantic_intent: None,
        factual_result: None,
        undo: vec![UndoProcedure::RestoreWrittenTargets],
        kind: ActionKind::ScheduledTick,
        client_origin: ClientOrigin::None,
        presentation: Vec::new(),
    };
    (state, action, target, current, next)
}

#[test]
fn scheduled_callback_requeues_after_exact_current_fact_completion_and_undo_restores_it() {
    for view in [TruthView::Local, TruthView::Ground] {
        let (mut state, action, target, current, next) = fixture(false);
        let applied = apply_action(&mut state, &action, view).unwrap();
        assert!(
            matches!(state.read(view, target).unwrap().value, Value::ScheduledQueue(queue) if queue == vec![next])
        );
        undo_action(&mut state, &applied).unwrap();
        assert!(
            matches!(state.read(view, target).unwrap().value, Value::ScheduledQueue(queue) if queue == vec![current])
        );
    }
}

#[test]
fn another_matching_pending_fact_survives_without_duplicate_requeue() {
    let (mut state, action, target, _, next) = fixture(true);
    apply_action(&mut state, &action, TruthView::Local).unwrap();
    assert!(
        matches!(state.read(TruthView::Local, target).unwrap().value, Value::ScheduledQueue(queue) if queue == vec![next])
    );
}

#[test]
fn missing_current_fact_rejects_without_mutating_another_schedule() {
    let (mut state, action, target, _, next) = fixture(true);
    state.local.insert(
        target,
        Cell {
            revision: 6,
            value: Value::ScheduledQueue(vec![next]),
        },
    );
    let before = state.local.clone();
    assert!(apply_action(&mut state, &action, TruthView::Local).is_err());
    assert_eq!(state.local, before);
}
