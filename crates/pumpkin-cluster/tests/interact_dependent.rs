#![allow(
    clippy::unwrap_used,
    clippy::expect_used,
    clippy::panic,
    clippy::uninlined_format_args
)]

use std::collections::HashMap;

use pumpkin_cluster::dependent::{
    AppliedDependent, DependentBatch, DependentCause, DependentDecision, DependentUpdate,
    decode_dependent, decode_dependent_batch, encode_dependent,
    encode_dependent_batch, is_dependent_due, join_dependent_batch, judge_dependent,
    make_dependent_undo, partition_due, revoke_cause_for, sort_dependents,
    split_dependent_batch, DependentAcceptance, DependentMetrics, capture_comparator,
    capture_observer, decode_comparator, decode_observer, encode_comparator, encode_observer,
    fire_tick_for,
};
use pumpkin_cluster::identity::{GlobalPlayerId, PlayerSeq, PlayerSlot, ServerId};
use pumpkin_cluster::interact::{
    InteractAcceptance, InteractBatch, InteractDecision, InteractMetrics, InteractSeqClock,
    InteractUpdate, apply_interact_batch_to_map, apply_remote_interact, capture_anchor_charge,
    capture_door_toggle, capture_end_eye, capture_trapdoor_toggle, decode_interact,
    decode_interact_batch, elect_interact_claim, encode_interact, encode_interact_batch,
    interact_claim_for, interact_loser_revert_plan, make_interact_undo,
    revert_interact_undos_to_map,
};
use pumpkin_cluster::protocol::{BlockPos, ChunkAddr, StreamKind};
use pumpkin_cluster::time::TickStamp;

const fn gid(server: u16, player: u16) -> GlobalPlayerId {
    GlobalPlayerId::new(ServerId(server), PlayerSlot(player))
}

const fn cell(x: i32, y: i32, z: i32) -> BlockPos {
    BlockPos { x, y, z }
}

const fn chunk(x: i32, z: i32) -> ChunkAddr {
    ChunkAddr { x, z }
}

#[test]
fn interact_captures_encode_and_gate_on_acceptance() {
    let door = capture_door_toggle(
        gid(1, 2),
        PlayerSeq(7),
        TickStamp(9),
        cell(1, 64, 3),
        11,
        12,
        chunk(0, 0),
    )
    .unwrap();
    assert!(encode_interact(&InteractUpdate::Door(door)).is_ok());
    let back = decode_interact(&encode_interact(&InteractUpdate::Door(door)).unwrap()).unwrap();
    assert_eq!(back, InteractUpdate::Door(door));
    assert!(
        capture_door_toggle(
            gid(1, 2),
            PlayerSeq(7),
            TickStamp(9),
            cell(1, 64, 3),
            11,
            11,
            chunk(0, 0)
        )
        .is_none()
    );

    let metrics = InteractMetrics::new();
    let decision = apply_remote_interact(11, &InteractUpdate::Door(door), &metrics);
    assert_eq!(
        decision,
        InteractDecision::Accept {
            undo: make_interact_undo(11)
        }
    );
    let stale = apply_remote_interact(99, &InteractUpdate::Door(door), &metrics);
    assert_eq!(
        stale,
        InteractDecision::RejectStale {
            expected: 11,
            current: 99
        }
    );
    assert_eq!(metrics.accepted(), 1);
    assert_eq!(metrics.rejected_stale(), 1);

    let mut acceptance = InteractAcceptance::new();
    let tick = TickStamp(9);
    acceptance.require(tick, chunk(0, 0), &[1, 2]);
    assert!(!acceptance.is_complete(tick));
    acceptance.accept(tick, chunk(0, 0), 1, &[]);
    acceptance.accept(tick, chunk(0, 0), 2, &[]);
    assert!(acceptance.is_complete(tick));
    assert_eq!(InteractBatch::stream_kind(), StreamKind::PlayerWorld);
}

#[test]
fn interact_batch_applies_winners_and_reverts_losers() {
    let trapdoor = capture_trapdoor_toggle(
        gid(1, 3),
        PlayerSeq(8),
        TickStamp(9),
        cell(2, 64, 3),
        21,
        22,
        chunk(0, 0),
    )
    .unwrap();
    let eye = capture_end_eye(
        gid(2, 1),
        PlayerSeq(3),
        TickStamp(10),
        cell(5, 64, 5),
        31,
        32,
        chunk(0, 0),
    )
    .unwrap();
    let anchor = capture_anchor_charge(
        gid(2, 2),
        PlayerSeq(4),
        TickStamp(11),
        cell(7, 64, 7),
        41,
        42,
        3,
        chunk(0, 0),
    )
    .unwrap();
    let mut batch = InteractBatch::new(TickStamp(9));
    batch.push_door(
        capture_door_toggle(
            gid(1, 2),
            PlayerSeq(7),
            TickStamp(9),
            cell(1, 64, 3),
            11,
            12,
            chunk(0, 0),
        )
        .unwrap(),
    );
    batch.push_trapdoor(trapdoor);
    batch.push_end_eye(eye);
    batch.push_anchor(anchor);
    assert_eq!(batch.len(), 4);
    batch.normalize();
    assert_eq!(batch.len(), 4);
    let bytes = encode_interact_batch(&batch).unwrap();
    assert_eq!(decode_interact_batch(&bytes).unwrap(), batch);

    let mut states = HashMap::new();
    states.insert(cell(1, 64, 3), 11);
    states.insert(cell(2, 64, 3), 77);
    states.insert(cell(5, 64, 5), 31);
    states.insert(cell(7, 64, 7), 41);
    let (applied, conflicts) = apply_interact_batch_to_map(&mut states, &batch);
    assert_eq!(applied.len(), 3);
    assert_eq!(conflicts.len(), 1);
    assert_eq!(conflicts[0], (cell(2, 64, 3), 21, 77));
    assert_eq!(states[&cell(1, 64, 3)], 12);
    revert_interact_undos_to_map(&mut states, &applied);
    assert_eq!(states[&cell(1, 64, 3)], 11);
    assert_eq!(states[&cell(5, 64, 5)], 31);
    let plan = interact_loser_revert_plan(&applied);
    assert_eq!(plan.len(), 3);
}

#[test]
fn interact_election_and_clocks_agree() {
    let left = interact_claim_for(
        &InteractUpdate::Door(
            capture_door_toggle(
                gid(1, 2),
                PlayerSeq(7),
                TickStamp(9),
                cell(1, 64, 3),
                11,
                12,
                chunk(0, 0),
            )
            .unwrap(),
        ),
        make_interact_undo(11),
    );
    let right = interact_claim_for(
        &InteractUpdate::Trapdoor(
            capture_trapdoor_toggle(
                gid(1, 3),
                PlayerSeq(8),
                TickStamp(9),
                cell(2, 64, 3),
                21,
                22,
                chunk(0, 0),
            )
            .unwrap(),
        ),
        make_interact_undo(21),
    );
    assert_eq!(
        elect_interact_claim(1234, &left, &right),
        elect_interact_claim(1234, &right, &left)
    );
    let mut clock = InteractSeqClock::new();
    assert_eq!(clock.issue(gid(1, 2)), PlayerSeq(0));
    assert_eq!(clock.issue(gid(1, 2)), PlayerSeq(1));
    assert_eq!(clock.issue(gid(1, 3)), PlayerSeq(0));
}

#[test]
fn dependents_encode_order_delay_and_revert_on_cause_loss() {
    let cause = DependentCause::new(gid(1, 2), PlayerSeq(7), TickStamp(40), cell(0, 64, 0));
    let comparator = capture_comparator(
        ServerId(1),
        chunk(0, 0),
        TickStamp(40),
        cell(1, 64, 0),
        cause,
        2,
        100,
        115,
    )
    .unwrap();
    let observer = capture_observer(
        ServerId(1),
        chunk(0, 0),
        TickStamp(40),
        cell(2, 64, 0),
        cause,
        1,
        200,
        201,
    )
    .unwrap();
    assert_eq!(fire_tick_for(TickStamp(40), 2), TickStamp(42));
    assert!(!encode_comparator(&comparator).unwrap().is_empty());
    assert_eq!(decode_comparator(&encode_comparator(&comparator).unwrap()).unwrap(), comparator);
    assert_eq!(decode_observer(&encode_observer(&observer).unwrap()).unwrap(), observer);

    let early = DependentUpdate::Observer(observer);
    let late = DependentUpdate::Comparator(comparator);
    assert_eq!(decode_dependent(&encode_dependent(&early).unwrap()).unwrap(), early);
    assert_eq!(decode_dependent(&encode_dependent(&late).unwrap()).unwrap(), late);
    assert!(is_dependent_due(early.fire_tick(), TickStamp(41)));
    assert!(!is_dependent_due(late.fire_tick(), TickStamp(41)));
    let mut ordered = vec![late, early];
    sort_dependents(&mut ordered);
    assert_eq!(ordered, vec![early, late]);
    let (due, pending) = partition_due(vec![early, late], TickStamp(41));
    assert_eq!(due, vec![early]);
    assert_eq!(pending, vec![late]);

    let metrics = DependentMetrics::new();
    let wait = judge_dependent(200, &early, TickStamp(40), &metrics);
    assert_eq!(
        wait,
        DependentDecision::Wait {
            fire_tick: TickStamp(41),
            now: TickStamp(40)
        }
    );
    let apply = judge_dependent(200, &early, TickStamp(41), &metrics);
    assert_eq!(
        apply,
        DependentDecision::Apply {
            undo: make_dependent_undo(200)
        }
    );
    let conflict = judge_dependent(999, &late, TickStamp(42), &metrics);
    assert_eq!(
        conflict,
        DependentDecision::Conflict {
            expected: 100,
            current: 999
        }
    );

    let mut pending_updates = vec![late];
    let mut applied_updates = vec![AppliedDependent {
        update: early,
        undo: make_dependent_undo(200),
    }];
    let revocation = revoke_cause_for(&mut pending_updates, &mut applied_updates, &cause);
    assert_eq!(revocation.dropped_pending, vec![late]);
    assert_eq!(revocation.revert.len(), 1);
    assert!(pending_updates.is_empty());
    assert!(applied_updates.is_empty());

    let mut states = HashMap::new();
    states.insert(cell(2, 64, 0), 201);
    pumpkin_cluster::dependent::revert_dependent_undos_to_map(&mut states, &revocation.revert);
    assert_eq!(states[&cell(2, 64, 0)], 200);
}

#[test]
fn dependent_batches_stay_homogeneous_and_drain_in_fire_order() {
    let cause = DependentCause::new(gid(1, 2), PlayerSeq(7), TickStamp(40), cell(0, 64, 0));
    let comparator = capture_comparator(
        ServerId(1),
        chunk(0, 0),
        TickStamp(40),
        cell(1, 64, 0),
        cause,
        2,
        100,
        115,
    )
    .unwrap();
    let observer = capture_observer(
        ServerId(1),
        chunk(0, 0),
        TickStamp(40),
        cell(2, 64, 0),
        cause,
        1,
        200,
        201,
    )
    .unwrap();
    let mut batch = DependentBatch::new(TickStamp(40));
    batch.push_comparator(comparator);
    batch.push_observer(observer);
    assert_eq!(batch.len(), 2);
    let bytes = encode_dependent_batch(&batch).unwrap();
    assert_eq!(decode_dependent_batch(&bytes).unwrap(), batch);
    assert_eq!(DependentBatch::stream_kind(), StreamKind::PlayerWorld);

    let unified = split_dependent_batch(&batch);
    assert_eq!(unified.len(), 2);
    assert_eq!(unified[0].fire_tick(), TickStamp(41));
    assert_eq!(join_dependent_batch(TickStamp(40), unified), batch);

    let first_due = batch.drain_due(TickStamp(41));
    assert_eq!(first_due, vec![DependentUpdate::Observer(observer)]);
    assert_eq!(batch.len(), 1);
    let second_due = batch.drain_due(TickStamp(42));
    assert_eq!(second_due, vec![DependentUpdate::Comparator(comparator)]);
    assert!(batch.is_empty());

    let mut acceptance = DependentAcceptance::new();
    acceptance.require(TickStamp(40), chunk(0, 0), &[5]);
    assert!(!acceptance.is_complete(TickStamp(40)));
    acceptance.accept(TickStamp(40), chunk(0, 0), 5, &[]);
    assert!(acceptance.is_complete(TickStamp(40)));
}
