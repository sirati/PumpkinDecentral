#![allow(
    clippy::unwrap_used,
    clippy::expect_used,
    clippy::panic,
    clippy::uninlined_format_args
)]

use std::collections::HashMap;
use std::time::Duration;

use pumpkin_cluster::banks::{Fuse, bank_pipes};
use pumpkin_cluster::buckets::BucketTable;
use pumpkin_cluster::codec::{decode_batch, encode_batch};
use pumpkin_cluster::identity::{GlobalPlayerId, PlayerSeq, PlayerSlot, ServerId};
use pumpkin_cluster::membership::{JoinVotes, Membership, can_leave_network};
use pumpkin_cluster::protocol::{
    BlockPos, BlockUndo, BreakBlockUpdate, ChunkKey, PlaceBlockUpdate, PosUpdate, WorldId,
};
use pumpkin_cluster::resolve::{ConflictKey, ContenderKind, Resolver};
use pumpkin_cluster::time::TickStamp;
use tokio::sync::mpsc;

const CLUSTER_SEED: u64 = 0x9E37_79B9_7F4A_7C15;
const DIRT_STATE: u16 = 1;
const WOOD_STATE: u16 = 2;

const fn gid(server: u16, player: u16) -> GlobalPlayerId {
    GlobalPlayerId::new(ServerId(server), PlayerSlot(player))
}

const fn world() -> WorldId {
    WorldId::new(0, 1)
}

const fn chunk(x: i32, z: i32) -> ChunkKey {
    ChunkKey::new(world(), x, z)
}

const fn cell() -> BlockPos {
    BlockPos { x: 4, y: 64, z: -2 }
}

fn place_update(player: GlobalPlayerId, seq: u16, tick: TickStamp, state: u16) -> PlaceBlockUpdate {
    PlaceBlockUpdate {
        gid: player,
        seq: PlayerSeq(seq),
        tick,
        pos: cell(),
        expected_old_state: 0,
        new_state: state,
        inv: pumpkin_cluster::inventory::INV_MAIN,
        slot: 0,
        item: 40,
        count_before: 64,
        count_after: 63,
        actor_chunk: chunk(0, 0),
        chunk: chunk(0, -1),
    }
}

fn break_update(player: GlobalPlayerId, expected_old_state: u16) -> BreakBlockUpdate {
    BreakBlockUpdate {
        gid: player,
        seq: PlayerSeq(7),
        tick: TickStamp(12),
        pos: cell(),
        expected_old_state,
        actor_chunk: chunk(0, 0),
        chunk: chunk(0, -1),
    }
}

fn break_accepted(current_state: u16, update: &BreakBlockUpdate) -> bool {
    update.expected_old_state == current_state
}

#[test]
fn double_place_same_cell_converges() {
    let tick = TickStamp(41);
    let first = gid(1, 1);
    let second = gid(2, 1);
    let contender_first = place_update(first, 1, tick, 5);
    let contender_second = place_update(second, 1, tick, 6);

    let resolve = |arrival: [PlaceBlockUpdate; 2]| {
        let mut resolver = Resolver::new(CLUSTER_SEED, tick);
        for update in arrival {
            resolver.ingest_place(
                update,
                BlockUndo {
                    old_state: 0,
                    count_before: update.count_before,
                },
            );
        }
        let mut ground = HashMap::new();
        ground.insert(ConflictKey::Block(chunk(0, -1), cell()), 0);
        resolver.resolve_with_context(&ground, &[])
    };
    let resolved_a = resolve([contender_first, contender_second]);
    let resolved_b = resolve([contender_second, contender_first]);
    assert_eq!(resolved_a, resolved_b);
    let winner = resolved_a[0].winner.expect("one valid placement wins");
    let ContenderKind::Place(winner) = winner.kind else {
        panic!("placement conflict produced a break winner");
    };
    assert_eq!(resolved_a[0].losers.len(), 1);
    assert!(winner.new_state == 5 || winner.new_state == 6);
}

#[test]
fn stale_break_expected_state_rejected() {
    let current = WOOD_STATE;
    let stale = break_update(gid(1, 3), DIRT_STATE);
    let fresh = break_update(gid(2, 1), WOOD_STATE);
    assert!(!break_accepted(current, &stale));
    assert!(break_accepted(current, &fresh));
    assert_ne!(stale.expected_old_state, current);
}

#[tokio::test]
async fn bank_fuse_batch_decode_roundtrip() {
    let tick = TickStamp(21);
    let (mut worker_a, fuse_a) = bank_pipes();
    let (mut worker_b, fuse_b) = bank_pipes();

    let mover = gid(1, 1);
    worker_a.bank.pos.push(PosUpdate {
        gid: mover,
        seq: PlayerSeq(1),
        tick,
        world: world(),
        pos: [8.0, 64.0, 8.0],
        vel: [0.0, 0.0, 0.0],
        yaw: 90.0,
        head_yaw: 90.0,
        pitch: 0.0,
        on_ground: true,
    });
    let builder = gid(2, 4);
    worker_b
        .bank
        .place_block
        .push(place_update(builder, 2, tick, 9));
    worker_a.end_tick();
    worker_b.end_tick();

    let (tick_tx, mut tick_rx) = mpsc::channel::<TickStamp>(4);
    let (out_tx, mut out_rx) = mpsc::channel::<Vec<u8>>(4);
    let fuse_task = tokio::spawn(async move {
        let mut fuse = Fuse::new();
        let mut ends = [fuse_a, fuse_b];
        fuse.run(&mut tick_rx, &mut ends, &out_tx).await;
    });

    tick_tx.send(tick).await.unwrap();
    drop(tick_tx);

    let bytes = tokio::time::timeout(Duration::from_secs(5), out_rx.recv())
        .await
        .unwrap()
        .unwrap();
    fuse_task.await.unwrap();

    let batch = decode_batch(&bytes).unwrap();
    assert_eq!(batch.tick, tick);
    assert_eq!(batch.pos.len(), 1);
    assert_eq!(batch.place_block.len(), 1);
    assert_eq!(batch.pos[0].gid, mover);
    assert_eq!(batch.place_block[0].gid, builder);
    assert_eq!(batch.place_block[0].new_state, 9);

    let recoded = encode_batch(&batch).unwrap();
    assert_eq!(decode_batch(&recoded).unwrap(), batch);
}

#[test]
fn bucket_late_holder_reopens_then_closes() {
    let mut table = BucketTable::new();
    let tick = TickStamp(7);
    let watched = chunk(0, 0);
    table.require(tick, watched, &[1]);
    table.accept(tick, watched, 1, &[]);
    assert!(table.is_tick_complete(tick));

    table.accept(tick, watched, 1, &[2]);
    assert!(!table.is_tick_complete(tick));

    table.accept(tick, watched, 2, &[]);
    assert!(table.is_tick_complete(tick));
    assert!(table.remove_tick(tick));
    assert_eq!(table.pending_ticks(), 0);
}

#[test]
fn unanimous_join_admits_and_occupied_leave_blocked() {
    let mut members = Membership::new(&[1, 2]);
    let mut votes = JoinVotes::new();
    assert!(!votes.record(&members, 7, 1));
    assert!(votes.record(&members, 7, 2));
    members.admit(7);
    assert!(members.contains(7));

    assert!(!votes.record(&members, 8, 1));

    assert!(!can_leave_network(1, 0));
    assert!(!can_leave_network(0, 1));
    assert!(can_leave_network(0, 0));
}

#[tokio::test]
async fn delayed_accept_still_closes() {
    let tick = TickStamp(9);
    let watched = chunk(3, -1);
    let mut table = BucketTable::new();
    table.require(tick, watched, &[11, 12]);
    table.accept(tick, watched, 11, &[]);
    assert!(!table.is_tick_complete(tick));

    let (accept_tx, mut accept_rx) = mpsc::channel::<(u16, Vec<u16>)>(2);
    tokio::spawn(async move {
        tokio::time::sleep(Duration::from_millis(200)).await;
        accept_tx.send((12, Vec::new())).await.unwrap();
    });

    let (peer, new_holders) = tokio::time::timeout(Duration::from_secs(5), accept_rx.recv())
        .await
        .unwrap()
        .unwrap();
    table.accept(tick, watched, peer, &new_holders);
    assert!(table.is_tick_complete(tick));
    assert!(table.remove_tick(tick));
}
