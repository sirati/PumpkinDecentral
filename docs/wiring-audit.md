# Cluster Wiring Audit — chat / combat / visual / world / entity apply paths

Spec: `docs/cluster-plan-v2.md` (Part 1 operator collage is highest authority; Part 2 elaboration is lower authority).
Scope: read-only audit. No Rust code was edited for this audit.
Rule checked throughout: protocol exists + unit-tested is NOT enough; every producer needs a broadcast hook in the
gameplay handler, and every apply path needs a delivery/apply task actually spawned at runtime (`docs/cluster-plan-v2.md`
§2.4 "NOT FULLY WIRING"). Method: `rg` over `crates/pumpkin-cluster/src` (definitions) joined against callers in
`crates/pumpkin/src` (gameplay handlers, `Server::tick`, `maybe_bootstrap` task spawns). A path counts as live only if a
non-test caller outside the defining module reaches it at runtime.

Runtime spawn proof (all live, `crates/pumpkin/src/server/cluster.rs`):
- `spawn_ghost_apply` at `crates/pumpkin/src/server/cluster.rs:271` (pos datagrams)
- `run_demux` at `crates/pumpkin/src/server/cluster.rs:250`
- `spawn_transient_apply` at `crates/pumpkin/src/server/cluster.rs:257`
- `spawn_visual_apply` at `crates/pumpkin/src/server/cluster.rs:281`
- `spawn_world_apply` at `crates/pumpkin/src/server/cluster.rs:283` (name varies; world receiver)
- `spawn_combat_apply` at `crates/pumpkin/src/server/cluster.rs:298`
- `spawn_entity_apply` at `crates/pumpkin/src/server/cluster.rs:299`
- `spawn_chat_delivery` at `crates/pumpkin/src/server/cluster.rs:328`
- `Server::tick` pumps at `crates/pumpkin/src/server/mod.rs:1259-1260` (`sample_server_tick`, `sample_entities_tick`)
- Ticker fuse drain at `crates/pumpkin/src/server/ticker.rs:77-81` (`end_tick_all` + `forward_fused`)

## 1. Chat — WIRED (deliver path complete; several helpers dead but harmless)

Producers (all live):
- Public: `crates/pumpkin/src/net/java/play/chat_message.rs:62` and
  `crates/pumpkin/src/net/bedrock/play/chat_message.rs:31`
  -> `broadcast_public_chat_from_player` (`crates/pumpkin/src/server/cluster_chat_out.rs:75`)
  -> `broadcast_public_chat` (`crates/pumpkin/src/server/cluster_chat_out.rs:42`) -> `try_send` to mesh.
- Private: `crates/pumpkin/src/command/commands/msg.rs:36`
  -> `send_private_from_command` (`crates/pumpkin/src/server/cluster_chat_pm.rs:173`)
  -> `send_private_chat` (`crates/pumpkin/src/server/cluster_chat_out.rs:84`)
  -> `private_parcel_for_host` (`crates/pumpkin-cluster/src/chat_sync.rs:204`).
- Team: `crates/pumpkin/src/command/commands/teammsg.rs:84`
  -> `broadcast_team_from_player` (`crates/pumpkin/src/server/cluster_chat_pm.rs:230`)
  -> `broadcast_team_chat` (`crates/pumpkin/src/server/cluster_chat_out.rs:83+`)
  -> `team_parcels_for_peers` (`crates/pumpkin-cluster/src/chat_sync.rs:190`) + `emote_parcels_for_peers`
  (`crates/pumpkin-cluster/src/chat_sync.rs:197`) for emotes.
- Emote produce/consume both live: `broadcast_emote_chat`/`broadcast_emote_from_player`
  (`crates/pumpkin/src/server/cluster_chat_out.rs:83-119`) and
  `ChatInboundEffect::EmoteDelivery` in `crates/pumpkin/src/server/cluster_chat_in.rs:153`.

Consumer (live):
- `chat_delivery_task` / `spawn_chat_delivery` (`crates/pumpkin/src/server/cluster_chat_in.rs`)
  -> `apply_chat_bytes` (`crates/pumpkin/src/server/cluster_chat_in.rs:24`)
  -> `handle_chat_bytes` (`crates/pumpkin-cluster/src/chat_sync.rs:397`) handles
  Public / Private / Team / Emote / Ignored. Each delivery increments `CHAT_DELIVERED` and logs.

Dead helpers (defined, unit-tested, zero non-test callers in `crates/pumpkin/src`):
- `crates/pumpkin-cluster/src/chat_sync.rs:171` `control_parcels_for_peers` — no runtime caller (outbox path encodes directly).
- `crates/pumpkin-cluster/src/chat_sync.rs:183` `public_parcels_for_peers` — no runtime caller (public path encodes directly).
- `crates/pumpkin-cluster/src/chat_sync.rs:215` `broadcast_control_message` (async) — no runtime caller.
- `crates/pumpkin-cluster/src/chat_sync.rs:231` `try_broadcast_control_message` — no runtime caller (admin/presence have own copies).
- `crates/pumpkin-cluster/src/chat_sync.rs:365` `resolve_private_by_id` — only `resolve_private_by_name`
  (`crates/pumpkin-cluster/src/chat_sync.rs:342`) is called at runtime (`cluster_chat_pm.rs:174`).
- `crates/pumpkin-cluster/src/chat_sync.rs:431` `answer_completion_query` — no runtime caller.
- `crates/pumpkin-cluster/src/chat_sync.rs:436` `query_completion_names` — no runtime caller.

## 2. Combat — APPLY WIRED, FIRE PRODUCER MISSING, BANK PUMP MISSING

Producers:
- Hit-player / hit-entity WIRED (Java only): `crates/pumpkin/src/net/java/play/attack.rs:109-119`
  (`next_combat_seq` at `crates/pumpkin-cluster/src/combat.rs:108`, `capture_hit_player` at
  `crates/pumpkin-cluster/src/combat.rs:14`, `capture_hit_entity` at `crates/pumpkin-cluster/src/combat.rs:31`,
  `stage_hit_player` at `crates/pumpkin-cluster/src/combat.rs:277`, `stage_hit_entity` at
  `crates/pumpkin-cluster/src/combat.rs:281`) via `attack_and_replicate`
  (`crates/pumpkin/src/net/java/play/attack.rs:93`), also called from
  `crates/pumpkin/src/net/java/play/interact.rs:83`. No Bedrock caller of `attack_and_replicate` exists.
- Fire (bow/crossbow) MISSING: `capture_fire` (`crates/pumpkin-cluster/src/combat.rs:48`) and `stage_fire`
  (`crates/pumpkin-cluster/src/combat.rs:285`) have zero callers outside `combat.rs` tests. No bow/crossbow/use-item
  handler stages `FireProjectileUpdate`. The apply side (`apply_fire` in
  `crates/pumpkin/src/server/cluster_combat_apply.rs:197`) is therefore unreachable at runtime.

Consumer (live task, starved for fire + starved by missing pump — see §6):
- `spawn_combat_apply` (`crates/pumpkin/src/server/cluster.rs:298`)
  -> `apply_batch_bytes` (`crates/pumpkin/src/server/cluster_combat_apply.rs`) decodes via `decode_batch` and calls
  `apply_hit_player` / `apply_hit_entity` / `apply_fire`, each gated by `validate_hit_player`
  (`crates/pumpkin-cluster/src/combat.rs:197`), `validate_hit_entity`
  (`crates/pumpkin-cluster/src/combat.rs:220`), `validate_fire` (`crates/pumpkin-cluster/src/combat.rs:240`).
  `is_dir_sane` (`crates/pumpkin-cluster/src/combat.rs:191`) is called. Ordering helpers
  `rank_combat_attacker` (`crates/pumpkin-cluster/src/combat.rs:347`), `order_hit_player`
  (`crates/pumpkin-cluster/src/combat.rs:378`), `order_hit_entity` (`crates/pumpkin-cluster/src/combat.rs:398`),
  `order_fire` (`crates/pumpkin-cluster/src/combat.rs:418`), `sort_*` (`435/447/459/473`) are only exercised by
  `drain_ordered_combat` (`crates/pumpkin-cluster/src/combat.rs:489`), which itself has no runtime caller (see §6).

## 3. Visual — APPLY WIRED, 4/7 PRODUCERS MISSING, BANK PUMP MISSING

Producers (Java only, 3/7 live):
- LIVE: `emit_swing` from `crates/pumpkin/src/net/java/play/swing_arm.rs:61`;
  `emit_sprint` from `crates/pumpkin/src/net/java/play/player_command.rs:29,48`;
  `emit_sneak` from `crates/pumpkin/src/net/java/play/player_command.rs:90`.
- MISSING (zero gameplay callers; definitions only + tests):
  - `crates/pumpkin-cluster/src/visual.rs:209` `emit_armor`
  - `crates/pumpkin-cluster/src/visual.rs:215` `emit_held`
  - `crates/pumpkin-cluster/src/visual.rs:233` `emit_blocking`
  - `crates/pumpkin-cluster/src/visual.rs:245` `emit_skin`
  No inventory-equip, hotbar-select, block-use, or client-information handler calls these.

Consumer (live task):
- `spawn_visual_apply` (`crates/pumpkin/src/server/cluster.rs:281`)
  -> `visual_apply_task` -> `apply_parcel` -> `apply_batch_bytes`
  (`crates/pumpkin/src/server/cluster_visual.rs:300-411`) fans out to `apply_armor` / `apply_held` / `apply_sneak` /
  `apply_sprint` / `apply_blocking` / `apply_swing` / `apply_skin`. All seven apply functions are reachable when a batch
  arrives; armor/held/blocking/skin simply never arrive because their producers are missing.

## 4. World (block / random-tick / redstone / explosion deltas) — WIRED end to end

Producers (all live):
- Random-tick: `crates/pumpkin/src/world/mod.rs:2082` -> `emit_random_tick_delta`
  (`crates/pumpkin/src/server/cluster_world_delta.rs:100`) -> `capture_random_tick_delta`
  (`crates/pumpkin-cluster/src/world_delta.rs:231`) -> `encode_frame`
  (`crates/pumpkin-cluster/src/world_delta.rs:402`).
- Redstone: `crates/pumpkin/src/block/blocks/redstone/lever.rs:36`,
  `crates/pumpkin/src/block/blocks/redstone/buttons.rs:61,103`,
  `crates/pumpkin/src/block/blocks/redstone/redstone_wire.rs:293` -> `emit_redstone_write`
  (`crates/pumpkin/src/server/cluster_world_delta.rs:118`) -> `capture_redstone_update`
  (`crates/pumpkin-cluster/src/world_delta.rs:245`).
- Explosion: `crates/pumpkin/src/world/explosion.rs:542` (gated by `mesh_active` at `:535`) ->
  `emit_explosion_blocks` (`crates/pumpkin/src/server/cluster_world_delta.rs:136`) ->
  `capture_explosion_update` (`crates/pumpkin-cluster/src/world_delta.rs:267`).
- Break/place/inventory ops ride `TickBatch` via `break_emit`/`place_emit`/`inventory` and are applied by
  `apply_break_atomic` / `apply_place_atomic` in `crates/pumpkin/src/server/cluster_world_apply.rs:484-566`.

Consumer (live):
- `spawn_world_apply` (`crates/pumpkin/src/server/cluster.rs:283`)
  -> `apply_batch_bytes` (`crates/pumpkin/src/server/cluster_world_apply.rs:700+`) handles both branches:
  `is_world_delta_frame` (`crates/pumpkin-cluster/src/world_delta.rs:390`) -> `decode_frame`
  (`crates/pumpkin-cluster/src/world_delta.rs:439`) -> `apply_world_delta_frame` (RandomTick/Redstone/Explosion via
  `apply_random_tick_delta` at `cluster_world_apply.rs:137` and `apply_fallible_delta` at `:153`, election via
  `elect_fallible_claim` at `world_delta.rs:576` / `cluster_world_apply.rs:186`); else `decode_batch` ->
  `WorldTickBuckets::push_batch/pop_ready` -> `apply_batch_to_server` (inv ops via `InvLedger::apply_op`, break/place
  via atomics, winners grouped by `group_winners_by_chunk` and promoted via `cluster_promote_tick`).

Dead helpers (test-only, never called from `crates/pumpkin/src` runtime):
- `crates/pumpkin-cluster/src/world_delta.rs:221` `normalize_edits`
- `crates/pumpkin-cluster/src/world_delta.rs:515` `apply_random_tick_to_map`
- `crates/pumpkin-cluster/src/world_delta.rs:530` `apply_fallible_to_map`
- `crates/pumpkin-cluster/src/world_delta.rs:551` `revert_undos_to_map`
- `crates/pumpkin-cluster/src/world_delta.rs:656` `loser_revert_plan` (runtime uses per-edit ledger path instead)
- `crates/pumpkin-cluster/src/interact.rs:757` `apply_interact_to_map` and
  `crates/pumpkin-cluster/src/interact.rs:792` `apply_interact_batch_to_map` have no runtime callers either
  (interact apply rides the world-apply path when wired).

## 5. Entity (ghosts + spawn/pos/visual/transient/combat) — WIRED end to end

Producers (all live, pumped every tick from `Server::tick` at `crates/pumpkin/src/server/mod.rs:1260`):
- `sample_entities_tick` (`crates/pumpkin/src/server/cluster_entity_emit.rs:431`)
  -> `sample_entity_rows` (`:238`) -> `sample_entity_visual` (`:218`); `flush_staged_events` + `fuse_entity_pos`.
- Spawn: `stage_spawn_if_cluster` from `crates/pumpkin/src/world/mod.rs:5334,5384`.
- Despawn: `stage_despawn_if_cluster` from `crates/pumpkin/src/world/mod.rs:5407`.
- Transient: `stage_transient_if_cluster` from `crates/pumpkin/src/world/mod.rs:680`.
- Combat: `stage_combat_if_cluster` from `crates/pumpkin/src/world/mod.rs:708`.
- Emit: `emit_entity_parcel` (`crates/pumpkin/src/server/cluster_entity_apply.rs:101`) via
  `cluster_entity_emit.rs:286`; outbox installed at `crates/pumpkin/src/server/cluster.rs:284`.

Consumers (both live):
- Player ghosts: `spawn_ghost_apply` (`crates/pumpkin/src/server/cluster_ghost.rs:64`, spawned at `cluster.rs:271`)
  -> `apply_datagram_bytes` -> `RemotePlayerTable::apply_pos_datagram`
  (`crates/pumpkin-cluster/src/movement.rs:62`) -> `EntityTracker::apply_cluster_ghost_samples`
  (`crates/pumpkin/src/world/entity_tracker.rs:646`).
- Entity ghosts: `spawn_entity_apply` (`crates/pumpkin/src/server/cluster_entity_apply.rs`, spawned at
  `cluster.rs:299`) -> `apply_parcel` fans out by `StreamKind`: `EntityPos` -> `apply_lifecycle_bytes`
  (spawn at `:210`, despawn at `:240`, pos at `:72`); `EntityVisual` -> `apply_visual_bytes` (`:114`);
  `EntityTransient` -> `apply_transient_bytes` (`:146`); `EntityCombat` -> `apply_combat_bytes` (`:178`).
  Each calls `EntityGhosts::spawn` (`entities.rs:170`), `apply_pos` (`:190`), `apply_visual` (`:200`),
  `apply_transient` (`:204`), `apply_combat` (`:208`), `despawn` (`:212`). Datagram pos is bridged via
  `install_entity_pos_bridge` (`cluster_entity_apply.rs:50`) / `forward_entity_pos_datagram` (`:96`),
  consumed from `cluster_ghost.rs:55`.

Dead helpers (defined + tested, zero runtime callers in `crates/pumpkin/src`):
- `crates/pumpkin-cluster/src/entities.rs:263` `OwnerTable::insert`
- `crates/pumpkin-cluster/src/entities.rs:268` `OwnerTable::chunk_of`
- `crates/pumpkin-cluster/src/entities.rs:272` `OwnerTable::note_moved`
- `crates/pumpkin-cluster/src/entities.rs:280` `OwnerTable::remove`
- `crates/pumpkin-cluster/src/entities.rs:289` `OwnerTable::plan_handoff`
- `crates/pumpkin-cluster/src/entities.rs:331` `is_owner_sender` (runtime uses local `owned_by_sender` in
  `cluster_entity_apply.rs` instead)
- `crates/pumpkin-cluster/src/entities.rs:344` `is_holder`
- `crates/pumpkin-cluster/src/entities.rs:357` `should_accept`
- `crates/pumpkin-cluster/src/entities.rs:389` `fanout_to_holders` (`route_to_holders` at `:374` IS used by
  `holds_chunk` in `cluster_entity_apply.rs`)

## 6. Cross-cutting missing pump (visual / combat / transient isolated banks never drained)

`visual`, `combat`, and `transient` each stage into their own thread-local `Bank` that the per-tick fuse never reads:
- `VISUAL_BANK` via `with_visual_bank` (`crates/pumpkin-cluster/src/visual.rs:161`); drain is
  `drain_visual_bank` (`crates/pumpkin-cluster/src/visual.rs:171`) — zero callers in `crates/pumpkin/src`.
- `LOCAL_COMBAT_BANK` via `with_combat_bank` (`crates/pumpkin-cluster/src/combat.rs:265`); drains are
  `drain_combat_updates` (`crates/pumpkin-cluster/src/combat.rs:309`) and
  `drain_ordered_combat` (`crates/pumpkin-cluster/src/combat.rs:489`) — zero callers in `crates/pumpkin/src`.
- `TRANSIENT_BANK` via `with_transient_bank` (`crates/pumpkin-cluster/src/transient.rs:122`); drain is
  `drain_transient_bank` (`crates/pumpkin-cluster/src/transient.rs:132`) — zero callers in `crates/pumpkin/src`.
- Same shape for movement's `LOCAL_POS_BANK` (`crates/pumpkin-cluster/src/movement.rs:97-132`), but movement has a live
  pump (`pump_local_batch` at `movement.rs:175`, `pump_movement_fuse`/`sample_server_tick` in
  `crates/pumpkin/src/server/cluster_movement_sample.rs:191-248`, called every tick from
  `crates/pumpkin/src/server/mod.rs:1259`). Visual/combat/transient have no equivalent `pump_*` wired into
  `Server::tick` — `Server::tick` (`crates/pumpkin/src/server/mod.rs:1258-1261`) only calls `sample_server_tick`
  (movement pos) and `sample_entities_tick` (entity ghosts). Staged visual/combat/transient rows are therefore
  drained-and-discarded in tests only. The receiving `*_apply` tasks (§2-§3) are live but starved for these kinds.
  Break/place/inv paths do not share this defect (they ride the world `TickBatch` / `WorldDelta` paths in §4).

## 7. Every missing hook (file:line)

| # | file:line | symbol | kind |
|---|-----------|--------|------|
| 1 | `crates/pumpkin-cluster/src/visual.rs:209` | `emit_armor` | producer never called (no equip handler) |
| 2 | `crates/pumpkin-cluster/src/visual.rs:215` | `emit_held` | producer never called (no hotbar-select handler) |
| 3 | `crates/pumpkin-cluster/src/visual.rs:233` | `emit_blocking` | producer never called (no block-use handler) |
| 4 | `crates/pumpkin-cluster/src/visual.rs:245` | `emit_skin` | producer never called (no client-information handler) |
| 5 | `crates/pumpkin-cluster/src/visual.rs:171` | `drain_visual_bank` | staged visual bank never pumped to mesh |
| 6 | `crates/pumpkin-cluster/src/combat.rs:48` | `capture_fire` | fire producer never called (no bow/crossbow hook) |
| 7 | `crates/pumpkin-cluster/src/combat.rs:285` | `stage_fire` | fire producer never called |
| 8 | `crates/pumpkin-cluster/src/combat.rs:309` | `drain_combat_updates` | staged combat bank never pumped to mesh |
| 9 | `crates/pumpkin-cluster/src/combat.rs:489` | `drain_ordered_combat` | staged combat bank never pumped to mesh |
| 10 | `crates/pumpkin-cluster/src/transient.rs:132` | `drain_transient_bank` | staged transient bank never pumped to mesh |
| 11 | `crates/pumpkin-cluster/src/chat_sync.rs:171` | `control_parcels_for_peers` | dead helper, no runtime caller |
| 12 | `crates/pumpkin-cluster/src/chat_sync.rs:183` | `public_parcels_for_peers` | dead helper, no runtime caller |
| 13 | `crates/pumpkin-cluster/src/chat_sync.rs:215` | `broadcast_control_message` | dead helper, no runtime caller |
| 14 | `crates/pumpkin-cluster/src/chat_sync.rs:231` | `try_broadcast_control_message` | dead helper, no runtime caller |
| 15 | `crates/pumpkin-cluster/src/chat_sync.rs:365` | `resolve_private_by_id` | dead helper, only by-name path used |
| 16 | `crates/pumpkin-cluster/src/chat_sync.rs:431` | `answer_completion_query` | dead helper, no runtime caller |
| 17 | `crates/pumpkin-cluster/src/chat_sync.rs:436` | `query_completion_names` | dead helper, no runtime caller |
| 18 | `crates/pumpkin-cluster/src/world_delta.rs:221` | `normalize_edits` | dead helper, no runtime caller |
| 19 | `crates/pumpkin-cluster/src/world_delta.rs:515` | `apply_random_tick_to_map` | dead helper, runtime uses per-edit ledger path |
| 20 | `crates/pumpkin-cluster/src/world_delta.rs:530` | `apply_fallible_to_map` | dead helper, runtime uses per-edit ledger path |
| 21 | `crates/pumpkin-cluster/src/world_delta.rs:551` | `revert_undos_to_map` | dead helper, no runtime caller |
| 22 | `crates/pumpkin-cluster/src/world_delta.rs:656` | `loser_revert_plan` | dead helper, no runtime caller |
| 23 | `crates/pumpkin-cluster/src/interact.rs:757` | `apply_interact_to_map` | dead helper, no runtime caller |
| 24 | `crates/pumpkin-cluster/src/interact.rs:792` | `apply_interact_batch_to_map` | dead helper, no runtime caller |
| 25 | `crates/pumpkin-cluster/src/entities.rs:263` | `OwnerTable::insert` | dead helper, no runtime caller |
| 26 | `crates/pumpkin-cluster/src/entities.rs:268` | `OwnerTable::chunk_of` | dead helper, no runtime caller |
| 27 | `crates/pumpkin-cluster/src/entities.rs:272` | `OwnerTable::note_moved` | dead helper, no runtime caller |
| 28 | `crates/pumpkin-cluster/src/entities.rs:280` | `OwnerTable::remove` | dead helper, no runtime caller |
| 29 | `crates/pumpkin-cluster/src/entities.rs:289` | `OwnerTable::plan_handoff` | dead helper, no runtime caller |
| 30 | `crates/pumpkin-cluster/src/entities.rs:331` | `is_owner_sender` | dead helper, runtime uses local `owned_by_sender` |
| 31 | `crates/pumpkin-cluster/src/entities.rs:344` | `is_holder` | dead helper, no runtime caller |
| 32 | `crates/pumpkin-cluster/src/entities.rs:357` | `should_accept` | dead helper, no runtime caller |
| 33 | `crates/pumpkin-cluster/src/entities.rs:389` | `fanout_to_holders` | dead helper, no runtime caller |
| 34 | Bedrock attack path (no file:line to list — absence) | `attack_and_replicate` has no Bedrock caller; only `crates/pumpkin/src/net/java/play/attack.rs:63` and `crates/pumpkin/src/net/java/play/interact.rs:83` call it | Bedrock hits never staged |

Notes:
- Items 5, 8–10 are the highest severity: even the wired producers in §2–§3 (§2 hit-player/hit-entity, §3
  sneak/sprint/swing, transient eat-start at `crates/pumpkin/src/net/java/play/use_item.rs:116`) stage into banks that
  no per-tick pump forwards, so the live apply tasks are starved. Fix is a pump (drain + `TickBatch::append_bank` +
  `encode_batch` + forward), not a new protocol.
- Items 1–4, 6–7 are missing gameplay broadcast hooks.
- Items 11–33 are dead-code helpers: unit-tested but unreachable at runtime. They do not break live paths but violate
  the spec's "no dead code" bar and should be wired or removed by a Rust-owning agent (out of scope for this doc-only
  audit).
