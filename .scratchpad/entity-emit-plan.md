# Entity emit plan (scratchpad)

Goal: wire the missing cluster ENTITY emit path (local non-player entities to
peers). Receive side is live; nothing in the server crate constructs
`EntitySpawn`, `EntityPosUpdate`, `EntityVisualUpdate`,
`EntityTransientUpdate`, `EntityCombatUpdate`, or `EntityDespawn` for sending,
so mobs spawned on one secondary are invisible on the others.

Design correction applied throughout: entity emit mirrors the PLAYER wiring,
not the chat pattern. Per-tick `EntityPosUpdate`s ride the same fused
per-tick datagram flow as players (thread-local bank accumulation,
tick-boundary fuse, datagram fanout). Discrete events (spawn, despawn,
visual, transient, combat) use the shared per-family streams, accumulated
during the tick and flushed at the tick boundary. No locks, no blocking or
waiting code anywhere: thread-local banks plus bounded channels with the
fixed-length-1 lossy handoff discipline from the movement path.

## Verified ground truths (checked against current code, no assumed APIs)

- Movement sampler: `crates/pumpkin/src/server/cluster_movement_sample.rs`.
  Thread-local `WORKER`/`PARKED_FUSE`/`FUSED`/`SEQS`, `bank_pipes()` from
  `pumpkin_cluster::banks`, `sample_local` pushes `PosUpdate` into the write
  bank, `end_tick` hands the boxed bank over mpsc, `pump_movement_fuse`
  folds handed-off banks into one `TickBatch`, `encode_batch`, then
  `cluster::forward_movement_batch`.
- Bank discipline: `crates/pumpkin-cluster/src/banks.rs`. `FULL_CAP = 1`,
  `EMPTY_CAP = 1`. `WorkerEnds::end_tick` never blocks/locks: reaps a
  recycled box or allocates (`empty_missed`), `mem::swap`s, `try_send`s;
  on full it keeps the filled bank locally (newest wins) and counts
  `fuse_backpressure` with a 1/min warn.
- `Bank` / `TickBatch` are PLAYER-only today: `pos: Vec<PosUpdate>` etc.,
  keyed by `GlobalPlayerId` (`banks.rs:Bank`, `protocol.rs:TickBatch`,
  `TickBatch::append_bank`). There is no entity row anywhere in the
  bank/fuse/codec path. Fusing entity pos requires extending these types.
- Fuse + datagram fanout: `cluster.rs::fuse_task` takes fused bytes,
  calls `cluster_datagram::forward_fused_batch` (decodes the batch, slices
  `batch.pos` into `MAX_POS_PER_DATAGRAM = 16` chunks via `encode_pos`,
  `MAX_DATAGRAM_BYTES = 1200`, `try_send`s one datagram per payload per
  peer, drops + counts on backpressure), then re-sends the fused bytes as
  `StreamKind::Control` parcels to every peer.
- Tick order: `Server::tick` (`server/mod.rs:1228`) calls
  `cluster_movement_sample::sample_server_tick(self)` BEFORE `tick_worlds`.
  Entity sampling plugs in at the same call site and therefore observes
  prior-tick state, exactly like players. `sample_server_tick` is a
  no-op when `advanced_config.cluster.enabled` is false.
- Spawn paths (`world/mod.rs`): `spawn_entity` (line ~5209, plugin event +
  `init_data_tracker`) delegates to `add_entity_silent` (line ~5228,
  UUID-dedup guard, live-only comment). `spawn_entity_non_save`
  (line ~5197) is a SEPARATE inline path that does NOT call
  `add_entity_silent`. Both must be hooked. `remove_entity` (line ~5252)
  swaps `removal_reason` to `Discarded` and is the despawn hook.
  `broadcast_entity_event` (~line 658) and `broadcast_damage_event`
  (~line 676) are the world-level transient/combat stage points.
- `World.entities` is `ArcSwap<Vec<Arc<dyn EntityBase>>>` and its doc
  comment (`world/mod.rs:256-258`) states it does NOT include players, so
  iterating it for sampling can never emit player rows (player movement
  sampling stays the sole owner of player traffic).
- Per-tick client broadcast for mobs is `Entity::send_pos_rot`
  (`entity/mod.rs:1741`), called from living ticks for non-players only
  (`entity/living.rs:3394`, `if !is_player`). Per the correction this path
  is NOT hooked inline; sampling reads atomics (`pos`, `velocity`, `yaw`,
  `pitch`, `chunk_pos`) from the tick thread instead.
- Entity identity for `EntityRef` (`protocol.rs:51`: `owner`, `local_id`,
  `chunk`): owner = `ServerId(advanced_config.cluster.server_id)`,
  `local_id` = `entity.entity_id` (i32), chunk from
  `entity.chunk_pos.load()` (`Vector2<i32>` -> `ChunkAddr{x, z}`).
  Tick = `server.tick_count.load(Relaxed) as u16`, exactly as the movement
  sampler does. `EntityType.id` is `u16`
  (`pumpkin-data/src/generated/entity_type.rs:12`) while
  `EntitySpawn.kind` is `u8`: needs an explicit mapping decision (open item).
- Demux: all four entity kinds funnel into ONE queue
  (`streams.rs:577-580`, `demux_channels`, `demux_receivers.entity`),
  consumed by `spawn_entity_apply` (`cluster.rs:232`,
  `cluster_entity_apply.rs`). `ENTITY_STREAM_KINDS` has exactly 4 entries;
  there is NO stream kind for spawn/despawn, and `entities.rs` is owned by
  another agent (untouchable). Spawn/despawn transport therefore needs a
  multiplex rule on an existing kind, proven by roundtrip tests.
- Receive today (`cluster_entity_apply.rs`): `apply_pos_bytes` inserts
  unknown entities as ghosts (`kind: 0`); visual/transient/combat applies
  only refresh liveness; `despawn()` and `spawn()` on `EntityGhosts` are
  never called from the apply path. Ghosts never become client-visible:
  `apply_cluster_ghost_samples` (`world/entity_tracker.rs:646`) only
  handles PLAYER `AcceptedPos`. Ghost creation for entities is unsolved
  work, owned by subtask D.
- `OwnerTable` / `plan_handoff` (`entities.rs:289`) have ZERO users in
  `crates/pumpkin/src` (verified by grep). Handoff interplay means wiring
  them up or explicitly deferring them; it is not "already live" on the
  emit side.
- Routing helpers (untouchable, in `entities.rs`): `is_owner_sender`,
  `is_holder`, `should_accept`, `route_to_holders`, `fanout_to_holders`.
  Outbound rule: sender speaks only for entities with `owner == local`.
- The chunk holder `Directory` (`pumpkin-cluster/src/chunks.rs`) is owned
  by `chunk_task` in `cluster.rs`. The tick thread cannot consult it
  without shared state, which the no-locks constraint forbids. Emit
  therefore fans out to all known peers; holder filtering stays enforced
  on receivers via the unchanged `should_accept` / `holds_chunk` gates.
- Outbox install precedent: `maybe_bootstrap` (`cluster.rs:239-242`)
  installs chat/admin/invsee/moderation outboxes next to each other;
  `cluster_datagram::install_datagram_outbox` is at `cluster.rs:202`.
- Existing tests that must stay green: `pumpkin-cluster entities`
  (`spawn_registers_ghost`, `pos_update_moves_ghost_across_chunks`,
  `all_four_streams_refresh_liveness`, `despawn_removes_ghost`,
  `fanout_reaches_only_holders`, `handoff_targets_lowest_holder_besides_self`,
  `single_stream_per_update_family`, `owner_gating_accepts_only_owner_sender`,
  `inbound_gate_needs_owner_and_holder`,
  `fanout_sends_only_to_holders_minus_sender`,
  `owner_table_tracks_local_entities`), `streams` demux tests,
  `cluster_datagram` tests (`chunks_updates_into_bounded_payloads`,
  `empty_batch_yields_no_payloads`, `movement_payloads_preserve_pos_head_vel`,
  `forward_without_outbox_is_noop`), `cluster_movement_sample` test
  (`samples_land_in_write_bank`).

## Constraints (all subtasks)

- Transport discipline is `docs/cluster-plan-v2.md` Part 1 (operator
  collage, highest authority; Part 2 is elaboration only) — single source
  of truth, mandatory, no exceptions. (`docs/cluster-plan.md` is deleted;
  verbatim sources live in `docs/operator-messages-2026-09-18.md`.)
  Design detail in each subtask must comply; nothing restated here.
- Local single-server behavior stays identical: emit is additive and a
  no-op when the mesh is disabled or no outbox/peers are installed.
- Do NOT touch: `pumpkin-world/src/chunk_system/schedule.rs`,
  `pumpkin-world/src/level.rs`, `pumpkin-cluster/src/entities.rs`, chat
  files, player movement sampling (`cluster_movement_sample.rs`).
- Code-as-docs for new code (no comments, self-evident names; never strip
  existing docs).

## Subtask A: entity wire format in the fused flow

- Goal: extend the bank/fuse/codec types so entity per-tick pos rows ride
  the same fused `TickBatch` as players, plus encode/decode for entity pos
  datagrams and a proven multiplex rule for spawn/despawn on an existing
  entity stream kind.
- Owns (disjoint): `crates/pumpkin-cluster/src/protocol.rs`,
  `crates/pumpkin-cluster/src/banks.rs`,
  `crates/pumpkin-cluster/src/codec.rs`. No other subtask touches these.
- Work: add `entity_pos: Vec<EntityPosUpdate>` to `Bank` (with
  `clear`/`is_empty`) and to `TickBatch` (with `new`/`is_empty`/
  `append_bank` mirroring every existing row); add entity pos datagram
  encode/decode beside `encode_pos`/`decode_pos`/`PosDatagram`
  (`EntityPosDatagram` over `EntityPosUpdate`, same `count` consistency
  shape); specify the spawn/despawn multiplex rule (proposal: both ride
  `StreamKind::EntityPos` with a decode cascade spawn -> despawn -> pos,
  relying on postcard `from_bytes` rejecting trailing bytes; the cascade
  MUST be proven, not assumed, by the roundtrip tests below).
- Interfaces: B pushes `EntityPosUpdate` rows into `Bank.entity_pos` and
  reads them via `TickBatch.entity_pos`; E slices
  `TickBatch.entity_pos` into datagrams with the new codec fns; D applies
  the multiplex cascade with the new decode fns. `StreamKind` set and
  `ENTITY_STREAM_KINDS` unchanged.
- Verified by: `cargo test -p pumpkin-cluster` fully green (all existing
  entities/streams/banks/codec tests listed above) plus new unit tests in
  the touched files: `entity_pos_bank_roundtrip` (Bank -> TickBatch ->
  `encode_batch` -> `decode_batch` preserves rows),
  `entity_pos_datagram_chunks` (slicing/count-consistency mirroring the
  datagram tests), `spawn_despawn_pos_cascade` (each of the three payloads
  decodes to exactly its own type and rejects the other two).
- Wire-compat warning (feeds Rollout): adding a `Vec` field to `TickBatch`
  changes the postcard encoding, so mixed-version meshes cannot
  interoperate once this lands. No version negotiation exists; rollout is
  a joint restart (see Rollout).

## Subtask B: entity sampler (bank staging + tick-boundary flush)

- Goal: per-tick entity sampling and discrete-event staging/flush,
  mirroring `cluster_movement_sample.rs` but for entities. This is where
  the bank/fuse lifecycle plugs in.
- Owns (disjoint): NEW `crates/pumpkin/src/server/cluster_entity_emit.rs`
  plus `crates/pumpkin/src/server/mod.rs` (only the `pub mod` decl line
  beside the other cluster mods and the one-line sample call beside
  `sample_server_tick` in `Server::tick`). No other subtask touches these.
- Work (bank/fuse lifecycle, mirroring the movement path point for point):
  thread-local entity `WorkerEnds`/`FuseEnds` pair from `bank_pipes()`
  (separate from the player pair; sampling never blocks the next tick);
  `sample_entities_tick(&Arc<Server>)` called from `Server::tick`
  immediately next to `sample_server_tick`, sampling `world.entities`
  (all worlds, lock-free `ArcSwap` load; players excluded by construction)
  into `Bank.entity_pos` rows, then `end_tick` handoff, then a pump that
  folds ready entity banks into a `TickBatch` carrying only the entity
  rows and forwards the encoded bytes through the SAME fused-bytes path
  the movement pump uses (`cluster::forward_movement_batch`), so entity
  rows reach `fuse_task`/`forward_fused_batch` with zero new channels.
  Discrete events: a bounded shared mpsc event queue (capacity fixed,
  `try_send` only, drop + counter when full) fed by C's hooks from ANY
  thread; the same tick-boundary call drains it with a bounded `try_recv`
  loop and flushes one parcel per family stream per peer through the
  entity stream outbox (installed by E) via `try_send` only. Nothing is
  ever sent inline from game logic. No-op when cluster disabled or no
  outbox installed. Owner gating at stage time: stage only rows with
  `owner == local` (trivially true for `world.entities`; the check exists
  so the invariant is explicit and matches `is_owner_sender` semantics).
- Interfaces: consumes A's `Bank.entity_pos` / `TickBatch` APIs; exposes
  `stage_spawn`, `stage_despawn`, `stage_visual`, `stage_transient`,
  `stage_combat` taking `pumpkin_cluster::entities` structs for C's hooks;
  exposes `install_entity_stream_outbox(local, peers, outbound)` for E;
  exposes `sample_entities_tick` for the `Server::tick` call site.
  `EntityType.id` (u16) -> `EntitySpawn.kind` (u8) mapping lives here as
  one well-named helper (proposal: saturating cast; kind is opaque
  metadata to the mesh until D defines ghost creation).
- Verified by: `cargo check -p pumpkin` green; new unit tests in the emit
  file: `entity_sampling_noop_when_disabled`,
  `event_queue_drops_on_full` (bounded queue + drop counter, no block),
  `flush_without_outbox_is_noop`; existing `samples_land_in_write_bank`
  and all `cluster_datagram` tests stay green (player path untouched).

## Subtask C: world hooks (spawn / despawn / discrete events)

- Goal: stage discrete entity events from the canonical world paths. No
  inline sends; hooks only build the event struct and call B's stage fns.
- Owns (disjoint): `crates/pumpkin/src/world/mod.rs`. No other subtask
  touches it.
- Work: stage `EntitySpawn` at the end of BOTH `add_entity_silent` and
  `spawn_entity_non_save` (two separate paths; hooking only the former
  misses spawns), stage `EntityDespawn` in `remove_entity` (after the
  `removal_reason` swap so double-removes stage once),
  stage `EntityTransientUpdate` in `broadcast_entity_event` and
  `EntityCombatUpdate` in `broadcast_damage_event` (mapping status/damage
  ids to the `action`/`value` and `kind`/`amount` fields; exact mapping is
  this subtask's call, documented in the stage call). Visual staging:
  equipment/metadata sends live outside this file, so C stages visual
  updates only where world-level code owns them, if at all, and records
  the gap for a follow-up instead of reaching into other files. Every hook
  isADDITIVE and after the local state change, so single-server behavior
  is identical and a full event queue can never break local logic.
- Interfaces: calls only B's `stage_*` fns; constructs `EntityRef` with
  owner = local server id read from `self.server.upgrade()` (same pattern
  `spawn_entity` already uses), `local_id` = `entity_id`,
  chunk = `chunk_pos.load()`, tick passed by B at flush time (hooks do not
  read the tick counter; B stamps flush-time tick... note: B stamps the
  tick at flush, matching how the movement pump stamps the batch tick).
  Hmm, correction: movement sampler stamps per-sample tick at sample time
  and the batch carries its own tick. C/B must pick ONE convention and
  document it: proposal is B stamps both sample and flush ticks at the
  tick-boundary call (all events in one flush share the flush tick),
  keeping hooks free of server/clock access beyond the owner id.
- Verified by: `cargo check -p pumpkin` green; no pre-existing world unit
  tests are renamed/altered; runtime evidence on a single server (spawn
  and kill mobs, use status/damage paths; behavior and packets identical
  with the mesh disabled). Mesh-enabled evidence belongs to the joint
  rollout test, not this subtask.

## Subtask D: entity receive (decode, gates, ghost creation, handoff)

- Goal: apply inbound entity traffic: multiplex-cascade decode, unchanged
  owner/holder gates, ghost creation that makes remote mobs visible to
  local players, and handoff interplay.
- Owns (disjoint): `crates/pumpkin/src/server/cluster_entity_apply.rs`
  plus `crates/pumpkin/src/server/cluster_ghost.rs` (entity pos datagrams
  share the `datagram_in_rx` channel today consumed only by
  `spawn_ghost_apply`; this subtask owns the discrimination between player
  `PosDatagram` and entity pos datagrams). No other subtask touches these.
- Work: implement A's multiplex cascade on the `EntityPos` stream
  (spawn -> despawn -> pos) with owner gate FIRST (`owned_by_sender`,
  matching `is_owner_sender`: reject `owner == local` loopback and
  `peer != owner` spoofing) then holder gate (`holds_chunk`, unchanged);
  keep the unknown-pos insert fallback (it already creates `kind: 0`
  ghosts). Ghost creation: define how an applied `EntitySpawn`/ghost row
  becomes visible to tracking local players (proposal: pair ghost rows
  into the local `entity_tracker` flow so holders' clients receive spawn
  + pos packets; full simulation of foreign entities is explicitly OUT of
  scope). Handoff interplay: `OwnerTable`/`plan_handoff` are currently
  unused server-side; this subtask either instantiates an `OwnerTable`
  for locally-owned entities (insert on staged spawn, `note_moved` on
  chunk change, `remove` on staged despawn) and exposes the handoff plan
  to the (future) handoff sender, or records a named deferral with the
  reason. Holder routing on the SEND side is intentionally all-peer
  fanout (see ground truths); this subtask documents that receivers drop
  non-held rows, which is what keeps the routing rules intact.
- Interfaces: consumes A's decode fns and cascade rule; consumes B/E's
  traffic; cold-path decode failures use the existing `note_dropped`
  counters (no hot-path cost: cascade attempts run per parcel, not per
  tick per entity).
- Verified by: `cargo check -p pumpkin` green; new unit tests in
  `cluster_entity_apply.rs`: `spawn_cascade_registers_ghost`,
  `despawn_cascade_evicts_ghost`, `loopback_and_spoof_rejected`
  (owner-gate matrix incl. `owner == local`), `non_held_chunk_dropped`;
  entity pos datagram discrimination test in `cluster_ghost.rs`
  (`entity_datagram_does_not_break_player_ghosts`); runtime evidence:
  two secondaries, spawn mob on one, observe spawn + movement + despawn
  on the other, plus mob spawned on a chunk the observer does not hold
  stays invisible.

## Subtask E: mesh plumbing (bootstrap + datagram slicing)

- Goal: install the entity outbox, slice entity pos rows into datagrams,
  and leave the fuse/demux topology otherwise untouched.
- Owns (disjoint): `crates/pumpkin/src/server/cluster.rs` (only the
  `maybe_bootstrap` install block beside the chat/admin/invsee installs)
  plus `crates/pumpkin/src/server/cluster_datagram.rs` (entity payload
  slicing beside `pos_payloads`, entity branch in `forward_fused_batch`).
  No other subtask touches these.
- Work: call B's `install_entity_stream_outbox(local, fallback peers,
  outbound_tx.clone())` next to the other outbox installs; extend
  `forward_fused_batch` to also slice `batch.entity_pos` with A's codec
  into bounded datagrams and `try_send` one per payload per peer (same
  drop + count discipline as player payloads; the fused-bytes `Control`
  copy in `fuse_task` needs no change and carries entity rows exactly like
  player rows today). Demux needs NO change: the four entity kinds already
  funnel to `demux_receivers.entity` and `spawn_entity_apply` is already
  installed. Confirm `ENTITY_DATAGRAMS`/`ENTITY_APPLIED`/`ENTITY_DROPPED`
  counters cover the new traffic or extend them in D's file, not here.
- Interfaces: B's install fn; A's codec fns and `TickBatch.entity_pos`;
  D's receive path (no code shared, wire format is the contract).
- Verified by: `cargo check -p pumpkin` green; all existing
  `cluster_datagram` tests green (`chunks_updates_into_bounded_payloads`,
  `empty_batch_yields_no_payloads`,
  `movement_payloads_preserve_pos_head_vel`,
  `forward_without_outbox_is_noop`) plus a new
  `entity_payloads_bounded_and_consistent` test mirroring the chunking
  test for entity rows.

## Sequenced rollout order

1. Land A first (-alone it is inert: new fields stay empty, cascade
   untested by traffic). Requires the joint-restart flag (wire change).
2. Land B + C together (emit stages and samples; no-op without E's
   install; single-server behavior identical by construction).
3. Land D (receivers tolerate the new traffic; cascade accepts old-style
   pos-only parcels since plain `EntityPosUpdate` bytes still decode as
   the cascade's final arm).
4. Land E last (install + slicing activates the flow). Restart ALL mesh
   peers together: A's `TickBatch` encoding change is wire-incompatible
   with older peers, and there is no version negotiation.
5. Joint runtime acceptance (all peers on the new build): two secondaries
   sharing a chunk border; spawn/move/status/damage/despawn a mob on one;
   observe spawn, per-tick movement, transient/combat effects, and despawn
   on the other; confirm a mob on an unheld chunk stays invisible
   (holder gate) and a spoofed/loopback parcel is dropped (counters).
6. Deferred follow-ups (not this job): `OwnerTable`-driven handoff
   sender if D defers it; `kind: u8` versus full `EntityType.id`
   fidelity; visual-update sourcing for equipment/metadata paths outside
  `world/mod.rs`; per-chunk-holder fanout on the send side if all-peer
   fanout proves too chatty (requires a lock-free directory snapshot
   feed, designed under the same no-locks rule).

---

# Section 2: PLAYER wiring end-to-end audit

Scope: audit every player channel emit-side and apply-side with call-site
evidence; each gap becomes its own fix subtask with disjoint file scope.
Nothing in Section 1 changes.

## Channel verdicts (wired or missing, with evidence)

Per-tick pos/vel/facing datagrams, BOTH directions: WIRED.
Emit is `sample_server_tick` (`server/mod.rs:1229`, platform-agnostic via
`server.get_all_players`, so Java and Bedrock players are sampled alike),
mesh is the datagram fanout (`cluster_datagram::forward_fused_batch`),
apply is `spawn_ghost_apply` -> `apply_cluster_ghost_samples`
(`world/entity_tracker.rs:646`), which writes pos, velocity
(+ `velocity_dirty`), yaw, and pitch into the tracked entity. No subtask.

Visual updates, apply side: WIRED. `cluster_visual.rs` implements
`apply_armor`, `apply_held`, `apply_sneak`, `apply_blocking`,
`apply_swing`, `apply_skin` off `decode_batch` on the `PlayerVisual`
stream. No subtask.

Visual updates, emit side: BROKEN in two places.
(a) Missing call sites: `emit_armor`, `emit_held`, `emit_blocking`,
`emit_skin` have zero callers outside `pumpkin-cluster/src/visual.rs`
(definition + its own unit test only; verified by repo-wide grep).
Only `emit_sprint` (`net/java/play/player_command.rs:29,48`),
`emit_sneak` (`player_command.rs:90`), and `emit_swing`
(`net/java/play/swing_arm.rs:61`) are ever called.
(b) Missing drain/forward leg: staged rows accumulate in the
`VISUAL_BANK` thread-local (`pumpkin-cluster/src/visual.rs`), whose
`drain_visual_bank` has zero callers outside its own test; nothing sends
fused bytes on the `PlayerVisual` stream (`fuse_task` in `cluster.rs`
sends `Control` parcels only). The apply side is therefore starved even
for the staged sprint/sneak/swing rows. Fix: P1 (drain + forward) + P2
(call sites).

Discrete actions (eat start/abort, break anim start/stop): apply WIRED
(`cluster_transient.rs`: `apply_eat_start`, `apply_eat_abort`,
`apply_break_anim` off the `PlayerTransient` stream), emit BROKEN.
`emit_eat_start` is called once (`net/java/play/use_item.rs:116`);
`emit_eat_abort`, `emit_break_anim`, `emit_break_anim_stop` have zero
callers outside `pumpkin-cluster/src/transient.rs`. The transient bank
drain (`drain_transient_bank`) likewise has zero callers outside its
defining file, and nothing sends on the `PlayerTransient` stream. Fix:
P1 + P2. Verified hook points for P2: dig lifecycle
(`net/java/play/player_action.rs`: `StartedDigging` ~line 18,
`CancelledDigging` ~line 156, `FinishedDigging`); eat-abort adjacent to
the eat-start site in `use_item.rs`.

World-affecting actions (break block, place block): apply WIRED
(`cluster_world_apply.rs`: `apply_break_atomic`, `apply_place_atomic`,
tick buckets, `apply_remote_place` incl. the expected-count variant
`apply_remote_place_expected` in `place_emit.rs:183` for the atomic
inventory stack update). Emit BROKEN on the forward leg: break has only
snapshots (`record_cluster_break_snapshot` in `player_action.rs:296`,
`make_undo` in `world/mod.rs:5593`) and no `BreakBlockUpdate` is ever
constructed server-side (zero `BreakBlockUpdate{` outside the cluster
crate); place stages into `PLACE_OUTBOX`
(`net/java/play/use_item_on.rs:344-354`) but `drain_place_outbox`
(`use_item_on.rs:307`) has zero callers; nothing sends on the
`PlayerWorld` stream. Fix: P1 (drain + forward) + P2 (break construction
at the dig-finish path already owned for anim).

Interactions (hit player, hit entity, bow/crossbow fire): apply WIRED
(`cluster_combat_apply.rs`: `apply_hit_player`, `apply_hit_entity`,
`apply_fire`, with existing unit tests `bow_speed_scales_with_charge`
and `bad_dir_rejected_without_server`). Emit HALF-WIRED: hit player and
hit entity are captured AND staged (`net/java/play/attack.rs:115,119`
via `capture_hit_player`/`capture_hit_entity` + `stage_hit_player`/
`stage_hit_entity`, gated on cluster-enabled with `next_combat_seq`),
but `capture_fire`/`stage_fire` have zero callers outside
`pumpkin-cluster/src/combat.rs`, and the combat drain
(`drain_combat_updates` / `drain_ordered_combat`) has zero callers
outside `combat.rs`, so even staged hits never reach the mesh. Fix: P1
(ordered drain + forward; ordering must go through
`drain_ordered_combat`, which needs the cluster seed) + P2 (fire hook at
arrow spawn with shooter gid; `item/items/bow.rs` verified to contain
zero cluster references, arrow spawn ~lines 82-119).

Bedrock parity: MISSING across the board. Only
`net/bedrock/play/chat_message.rs` references the cluster crate; no
bedrock handler stages sprint/sneak/swing/eat/attack/place/dig/fire.
Position is covered (sampler is platform-agnostic), everything discrete
is Java-only. Relevant bedrock handlers exist (`player_auth_input.rs`,
`animate.rs`, `interaction.rs`, `player_action.rs`,
`player_block_action.rs`, `inventory_action.rs`, `item_stack_request.rs`,
`mob_equipment.rs`). Fix: P3.

## Fix subtasks (disjoint scopes, no file shared with Section 1 or peers)

### Subtask P1: player staged-bank drain + family-stream forward

- Goal: close the forward leg for every staged player row so the live
  apply sides stop starving. Single tick-boundary flush, same no-locks
  discipline as the movement pump.
- Owns (disjoint): NEW `crates/pumpkin/src/server/cluster_player_flush.rs`
  ONLY. Does not touch `cluster.rs`, `cluster_datagram.rs`,
  `cluster_movement_sample.rs`, or any apply file.
- Work: one `flush_player_tick()` called once per tick from the same
  tick-boundary site as the movement/entity samplers (invocation is an
  interface to the tick-site owner: B's `sample_entities_tick` call
  sequence in Section 1; P1 only provides the function). It drains
  `drain_visual_bank`, `drain_transient_bank`,
  `drain_ordered_combat(cluster_seed, tick)`, and `drain_place_outbox`
  (plus the break rows P2 stages), folds them into ONE `TickBatch`,
  encodes once, and fans the bytes out on the four family streams
  (`PlayerVisual`, `PlayerTransient`, `PlayerCombat`, `PlayerWorld`) via
  the outbound sender with `try_send` only, drop + counter on full.
  Outbox install for these streams is E's bootstrap edit (one more line
  in E's install block; interface E<->P1). Empty flush sends nothing, so
  idle ticks cost nothing. Break rows reuse the existing
  `sort_breaks_for_tick` ordering on the apply side; place rows keep the
  capture-time `count_after` so the atomic stack update is preserved.
- Interfaces: consumes cluster-crate drain/codec APIs only; provides
  `flush_player_tick()` + the outbox-install signature E calls.
- Verified by: `cargo check -p pumpkin` green; new unit tests in the
  flush file (`empty_flush_sends_nothing`,
  `flush_drops_on_full_outbox`); ALL existing apply-side tests stay
  green, including `cluster_combat_apply` tests
  (`bow_speed_scales_with_charge`, `bad_dir_rejected_without_server`)
  and the `cluster_datagram` + `pumpkin-cluster` suites from Section 1;
  runtime evidence: two peers, sprint/sneak/swing/eat/hit/place on one,
  observe on the other.

### Subtask P2: missing Java emit call sites (+ break construction)

- Goal: stage every currently unstaged Java player event into the banks
  P1 drains. Staging only (`emit_*`/`capture_*`/`stage_*` + outbox
  push); no sends, no channel code.
- Owns (disjoint): `crates/pumpkin/src/net/java/play/player_action.rs`
  (break-anim start/stop at the `StartedDigging`/`CancelledDigging`/
  `FinishedDigging` arms + `BreakBlockUpdate` construction on the
  dig-finish path next to `record_cluster_break_snapshot`),
  `crates/pumpkin/src/net/java/play/set_held_item.rs` (`emit_held` in
  `handle_set_held_item`), `crates/pumpkin/src/net/java/play/use_item.rs`
  (`emit_eat_abort` on the use-stop path beside the existing eat-start
  site), `crates/pumpkin/src/net/java/play/client_information.rs`
  (`emit_skin` where `skin_parts` are already handled, ~lines 30/56),
  `crates/pumpkin/src/item/items/bow.rs` (`capture_fire` + `stage_fire`
  at arrow spawn with the shooter's gid/seq/tick), plus the armor-set
  path in the player inventory code and the shield
  blocking start/stop transitions (subtask owns discovery of those two
  hook points; files must not overlap Section 1 or P1/P3 scopes).
  Does not touch `world/mod.rs`, `cluster.rs`, or any apply file.
- Interfaces: calls only the existing cluster-crate `emit_*`/
  `capture_*`/`stage_*` fns (gid via `player.cluster_gid()`, tick via
  the established `tick_from_counter` / `combat_tick` / `cluster_tick_stamp`
  helpers at each site); rows flow to P1's flush with no new API.
- Verified by: `cargo check -p pumpkin` green; no handler behavior
  changes with the mesh disabled (each site keeps its existing
  early-return pattern, cf. `attack.rs:103`); runtime evidence with P1
  landed: armor/held/skin/blocking changes, eat abort, break anim, and
  arrow fire replicate to the peer.

### Subtask P3: Bedrock emit parity

- Goal: stage the same discrete events from Bedrock handlers that P2
  covers for Java. Position needs nothing (sampler is shared).
- Owns (disjoint): `crates/pumpkin/src/net/bedrock/play/*.rs` handler
  files ONLY (`player_auth_input.rs` for sneak/sprint edges,
  `animate.rs` for swing, `interaction.rs` for attack/use, dig handlers
  for break anim + break/place, equipment/inventory handlers for
  held/armor). Does not touch Java handlers, apply files, or flush code.
- Interfaces: same cluster-crate staging fns as P2; rows flow to P1's
  flush. Bedrock GIDs use the same `cluster_gid()` source as Java.
- Verified by: `cargo check -p pumpkin` green; unit-staging tests where
  the handlers are constructible, otherwise runtime evidence with a
  Bedrock client against two peers (sprint + swing + break replicate).
  Explicit non-goal: Bedrock-only features with no Java/cluster
  counterpart (e.g. emotes) stay local.

## Player rollout (after Section 1 rollout)

1. Land P1 first (inert without staged rows beyond today's sprint/sneak/
   swing/eat/hit/place trickle; those immediately start replicating,
   which is the first visible win and the P1 acceptance test).
2. Land P2 (new staged rows flow through P1's flush with no further
   plumbing).
3. Land P3 (Bedrock parity; Java behavior untouched).
4. Joint acceptance: two peers, one Java + one Bedrock client; walk
   every channel in the verdict table (pos, armor, held, sneak/sprint,
   blocking, swing, skin, eat start/abort, break anim, break, place +
   stack count, hit player, hit entity, arrow fire) and confirm each
   replicates (loss-tolerant fire-and-forget: at-least-once in practice,
   drops and duplicates are acceptable and must never trigger repair,
   retry, or acknowledgement traffic; acceptance is observational only).
