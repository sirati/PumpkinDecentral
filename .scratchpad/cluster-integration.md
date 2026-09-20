# Cluster integration tracker (scratchpad)

Not part of the repo docs; working notes for the parallel build.

## Finished (4)
- chunk transfer (Franklin), integration tests (Anscombe),
  entity routing (Euclid), conflict resolver (Confucius).

## Running (13)
Singer, Dalton, Halley, Schrodinger, James (movement DONE),
Ramanujan, Chandrasekhar, Fermat, Archimedes, Popper, Arendt (acceptance DONE), Banach, Locke, Darwin, Huygens.

## Integration debts
- InboundParcel/OutboundParcel defined in xfer.rs; streams agent may
  define its own -> add From shim at call sites.
- Dual-copy ingest_snapshot signature assumed by xfer agent ->
  reconcile names when dual agent lands.
- Test-agent GAPs: resolve_place_conflict, break_accepted predicate,
  async bucket waiter, ACCEPT sender, apply_accepted_tick,
  fetch_with_retry -> follow-up tasks.
- lib.rs gets one `pub mod` line per agent -> verify no dupes on merge.
- movement.rs E0382 flagged by two finished agents -> fix at integration
  if James doesn't.

## Mid-build breakage log
- streams.rs E0433/E0015: FIXED by streams agent (gone as of check).
- mesh.rs:65 E0106 missing lifetime on peer_by_cert: still red, owner running, do NOT touch.

- lifecycle (Darwin) DONE: membership.rs JoinHello/Vote/Admit + lifecycle.rs Lifecycle/LifecycleState/JoinVotes/plan_handoff shim/mpsc driver; 18 tests green in scratch harness (real crate blocked by streams.rs). Open: re-run cargo check once streams lands; /tmp/lcprobe scratch crate cleanup (declined for now).

- misc (Huygens) DONE: reconcile.rs ReconcilePlan + metrics.rs ClusterMetrics (atomic, slow-fuse 60s cooldown) + ClusterConfig::validate + main.rs --cluster-off/--cluster-role + plan §10.1 cert checklist. NOTE: tracker-assist 5-liner spec for movement agent saved in report above.

- movement (James) DONE: movement.rs RemotePlayerTable/apply_pos_datagram/last-writer-wins/thread-local with_bank shim/sample/drain/ingest_channel; player.rs cluster_gid ArcSwap field+getter/setter; entity_tracker ghost-apply block; workspace+pumpkin Cargo.toml pumpkin-cluster deps. Movement E0382 appears GONE (current errors only streams/dual/primary). NOTE: with_bank shim must be reconciled when tick agent lands.

- block-break (Fermat) DONE: break_emit.rs capture_break/make_undo(count_before=u8::MAX sentinel)/apply_remote_break(SKIP_DROPS, loot at origin)/BreakMetrics/BreakSeqClock; world/mod.rs expected_old_state+BlockUndo+apply_cluster_remote_break+CLUSTER_BREAK_METRICS; player_action.rs Started/FinishedDigging snapshot hooks + CLUSTER-TODO(transient) at set_block_destroy_stage (anim agent fills it).

- dual-copy (Banach) DONE: dual.rs DualChunk/DualStore/BlockEdit/DualChunkData/copy_blocks + 6 tests; level.rs ClusterDualState + Level methods + field. pumpkin-world check green. NOTE: rebase/fork copy blocks only (no biomes/light); loaded_chunks readers converge via future reconcile. lib.rs line deleted once mid-task by sibling writer — re-verify lib.rs mod list at integration.

- streams (Dalton) DONE: streams.rs StreamHeader/Registry/Demux/Mux, InboundParcel/OutboundParcel (matches xfer.rs names — check for dup definitions at integration); run_demux is task body (no tokio rt). NOTE: reports a `transport.rs` in tree — not in lib.rs mod list seen earlier; verify at integration.
- combat (Popper) DONE: combat.rs capture_hit_player/hit_entity/fire + CombatSeqClock + striped next_combat_seq + validate_* + thread-local Bank staging; attack.rs/interact.rs hooks run Player::attack first then stage. NOTE: combat_tick uses wall-clock zero offset until clock agent lands; EntityRef.owner = local server_id.

- block-place (Archimedes) DONE: place_emit.rs capture_place/apply_remote_place(RemotePlaceVerdict)/PlaceSeqClock+next_place_seq/chunk_of_block; player_inventory consume_for_place(try_lock only); use_item_on hook + PLACE_OUTBOX SegQueue + drain_place_outbox. NOTE: slot = interim UUID-low-bits until login counters; tick = local clock until NTP. TREE STATUS: 172 tests pass, 3 fail (ntp x2, transport x1).

- clock/NTP (Halley) DONE: ntp.rs NtpSync/NtpHandle/watch-based, 30s poll, median+EMA/4, tick_now via NtpDiscipline. TREE: 174 pass, 1 fail (transport loopback timeout). NOTE: v1 trusts any 48B reply (no stratum/KoD).

- visual (Ramanujan) DONE: visual.rs capture_*/emit_*/next_seq(thread-local)/drain_visual_bank; hooks player_command sprint/sneak edges + swing_arm; entity_equipment mapping helpers. OPEN: HeldUpdate has no firing hook (hotbar path outside scope); second thread-local Bank shim (tick agent reconciles with movement/combat shims).

- primary/persistence (Locke) DONE: PrimarySaveHandle/Inbox/channel (mpsc only, poll_fn no select!); server/mod.rs hooks (role log, add_player kick on primary, save_all skip + player_data tick skip on secondary, shutdown signals saver). OPEN: ground-truth apply on primary is a stub (needs dual Level wrapper wiring); handle_player_leave still saves on secondaries (lib.rs owner follow-up).

## USER RULE (late, applies at integration): code as docs — no comments/docstrings; rewrite to be self-evident. CONFLICT: repo lints deny missing docs; 17 landed modules are documented. Integration must: remove deny(missing_docs) for pumpkin-cluster scope + strip docs/comments from new cluster code. Does NOT touch pre-existing repo files' docs.
- transient (Chandrasekhar) DONE: transient.rs capture_eat_start/abort/break_anim(+stop, 255)/next_seq/emit_*/RemoteEating(eat_finished inferred)/RemoteBreakAnims; use_item.rs eat-start hook + CLUSTER-TODO(combat) fire hook. OPEN: eat-abort trigger lives in others' files; bow-fire wiring still with combat agent.

- streams comment pass (Dalton, reinstated then closed to free slot) DONE: 152 added comment lines stripped; renames StreamRegistry::classify->should_deliver, split_header_frame->HeaderFrame{header,len}; check exit 0, streams 13/13, full 175/175. Files on disk, agent closed.
- Anscombe (tests) + Euclid (entities) reinstated for comment pass, messaged with own-additions-only + keep-upstream-docs rule.

- tick banks (Schrodinger) DONE: tick.rs new (390 lines, comment-free), lib.rs mod, ticker.rs end_tick_all hook, tokio rt feature for Handle::try_current+fuse spawn. check cluster+pumpkin clean, 178 unit + 6 e2e pass. NOTE: batch stamp wall-clock offset 0 until time-sync wires discipline; one-tick pipeline delay inherent; take_fused_outbox once from ticker thread.

- tree strip (Linnaeus) DONE: all new files + added hunks stripped, upstream docs intact; transport/mesh/streams excluded; pumpkin-config scoped allow(missing_docs) on cluster module. cargo check workspace green; tests 178 pass. ALL AGENTS DONE (impl + comment passes).

- RUNTIME BOOTSTRAP (Descartes, running): server/cluster.rs bootstrap (MeshConfig from ClusterConfig, Transport::bind, router/demux/fuse/accept/lifecycle tasks), primary chunk serve, secondary fetch gate in level.rs fetch_chunk + fetch-vs-generate counter. Linnaeus closed (completed, recorded) to free slot.

## Remote facts
- local tailnet IP: 100.88.80.24; tslaptop tailnet IP: 100.83.143.24; ssh OK; nix 2.34.8 on laptop. nix build running in background (/tmp/nix-build.log).

- GLOBAL ADMIN (Carson, running): op/deop + ban/unban replicated + persisted on primary, global /kick /spectate; new admin_sync.rs, lib.rs append-only. INVSEE (Turing, running): /invsee cross-secondary read-only view; new invsee.rs. Anscombe + Euclid closed (both completed+recorded) to free slots.

- GLOBAL CHAT (Peirce, running): public chat cluster-wide, /msg pm + /tm routed by GlobalPlayerId, global tab-complete; new chat_sync.rs. Schrodinger reaped by system (slot was free).

- GLOBAL ADMIN (Carson) DONE: admin_sync.rs (AdminMutation op/ban over Control, primary persist via PrimarySaveHandle, OpStore/BanStore hooks, kick/spectate by GlobalPlayerId, PlayerDirectorySnapshot); 194 tests pass. NOTE: workspace check fails in pumpkin-world (missing ClusterFetchRequest/CLUSTER_*) — Descartes's in-progress bootstrap, his to fix.

- OPERATOR: sirati97 (online Java player) must be op in the test run. Descartes told to seed + replicate it.

- INVSEE (Turing) DONE: invsee.rs req/resp/write over Control + command with armor/offhand/glass layout, edit perm; both perms default Op(Two). 217 tests green, workspace check green. OPEN: remote serve/routing needs Descartes bootstrap wiring.
- NIX BUILD GREEN: nix store path 39fyjds8s7nxd63gjscpj2y2imi6kp18, bin/pumpkin starts correctly. No stray server running.

- LOCAL MESH PROVEN: secondary fetch requested->completed->fetched from holding peer; primary served snapshot; zero generation lines on secondary. Pins exchanged, both Member.

## Acceptance bar (runtime, not tests)
- Five streams currently drained+discarded (visual/transient/world/combat/entity) must be applied instead; chat hook+delivery; movement sample->fuse->ghosts; live chunk gate, generate path deleted.
- Baseline violation: node-1 holds 14M region files + data/*.json; must go to zero (diskless secondaries).
- node-2 peers empty -> three-way pins; fetch-miss-then-generate observed, must become impossible.
- Primary: no MC listeners, secondary takes default ports, tailnet binds. Join only when all proven in behavior.
