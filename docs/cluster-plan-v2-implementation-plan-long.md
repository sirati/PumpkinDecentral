**Every edit to this plan requires the user's explicit signoff on the exact diff before it is applied.**

# Spec-Compliant Decentral Player and Entity Action Mesh

After every context compaction, re-read this complete plan before continuing implementation.

## Summary

Rebuild the cluster implementation incrementally on recovered `master`. Use `broken-ghost` only to study protocols and necessary features; do not transplant its lobby lifecycle, synchronization patterns, or broad entity rewrite.

The finished system must preserve the recovered lobby’s appearance and speed, replace tracker-only ghosts with fully registered replicas, implement deterministic actions for players and entities, keep local and ground-truth state on every chunk holder, and route actor updates only to relevant holders. The primary remains a persistence and chunk source process with no Minecraft listener or players.

New cluster code must use owned mutable state, tick phases, bounded message passing, and end-of-tick publication. Do not introduce locks, waits, per-field atomic state, new explanatory comments, or doc strings. Preserve existing upstream documentation.

## Architecture and Interfaces

### Runtime ownership and tick phases

- Give each world shard and cluster connection task exclusive ownership of its mutable state. Packet handlers submit decoded input; they never mutate shared players, entities, chunks, membership, or inventories.
- Process each tick through fixed phases:
  1. Drain network and client inputs.
  2. Store client-claimed controller state.
  3. Run controller filters and produce effective controller state.
  4. Apply movement and physics.
  5. Freeze the tick’s spatial and chunk-holder snapshot.
  6. Capture player, entity-AI, random-tick, redstone, explosion, and interaction intents.
  7. Resolve same-tick dependencies and conflicts deterministically.
  8. Apply accepted optimistic updates and targeted undo operations.
  9. Advance globally resolved changes into ground truth.
  10. Publish immutable render/network snapshots and route outbound batches.
- Where same-tick feedback is required, divide resolution and application into explicit bounded subphases. Do not solve it with atomics or concurrent mutation.
- Partition gameplay work by affected chunk for every phase. A single-chunk work unit exclusively owns the chunk and every loaded player/entity state belonging to it while it runs. An interaction that affects more than one chunk becomes one deterministic joint work unit owning the complete participating chunk set for that phase.
- Build disjoint work sets from those chunk and joint-chunk units. Run disjoint sets in parallel with Rayon and order overlapping units by deterministic action order. Return owned results at the phase boundary; gameplay update processing must not use atomics to coordinate workers.
- Retain thread-local double banks only where multiple execution threads actually produce tick input. Transfer an owned bank with nonblocking bounded channels; a missed rotation is an invariant diagnostic and never causes a wait.
- Limit atomics to cancellation flags or transport counters that cross task boundaries. Player, living-entity, inventory, and replicated world state use ordinary fields owned by their tick shard.

### Actor state and controller model

- Introduce a shared `ActorId` covering `Player(GlobalPlayerId)` and globally identified entities.
- Every peer holding an actor’s current chunk stores:
  - `ground`: state advanced only by globally accepted tick resolutions.
  - `local`: ground plus optimistic accepted-local changes.
  - pending actions and exact undo records.
- Entity ownership determines which peer runs AI and produces intents. It does not grant exclusive truth or bypass holder-side deterministic resolution.
- Split local player control into:
  - `ClientClaimedState`: the movement, orientation, input, and action state claimed by the client, retained with tick and sequence information.
  - `EffectiveControllerState`: the state after movement validation, policy filters, and future anti-cheat filters.
  - filter decisions linking each effective state to its claimed input.
- Simulation and outbound movement use the effective state. Claimed and effective state may intentionally diverge, including through feature-gated test filters that simulate hacked-client behavior.
- Replace tracker ghosts with `RemotePlayerReplica` objects registered in the normal world entity registry, combat lookup, selectors, tracker, and chunk lifecycle. Preserve profile UUID, name, properties, skin textures, equipment, pose, animation, and game mode.
- Use the same local/ground representation and action machinery for mobs and players. Sheep eating grass, entity attacks, environmental damage, item pickup, and other world interactions become fallible actions instead of direct owner-only world mutation.

### Wire protocol and routing

- Version the cluster protocol and reject incompatible peers during connection setup.
- Use fixed-width, validated zero-copy wire views for hot paths. Each tick envelope contains arrays grouped by focused update type so a type tag is not repeated per record.
- Retain separate reliable unidirectional streams for visual state, player actions, world actions, combat, inventory, acceptance, membership, presence, administration, chunks, and entity updates.
- Movement uses multiple QUIC datagrams per tick:
  - Compute the usable application payload from the negotiated QUIC datagram limit and a configured 1200-byte UDP path budget after worst-case QUIC overhead.
  - Greedily pack complete movement records without splitting a record.
  - Emit as many independent datagrams as required.
  - Never ask QUIC/IP to fragment an oversized movement batch.
  - Preserve actor ID, wrapping sequence, synchronized tick, position, velocity, body rotation, and head rotation in every record.
- Losing one datagram only loses the records in that datagram. Later sequence numbers supersede missed movement without retransmission.
- Route player and entity movement, visual state, actions, and snapshots only to peers that hold the actor’s current chunk.
- Cross-chunk movement uses a reliable boundary event: send leave to old holders, send a full actor snapshot to new holders, and begin new-holder datagrams only after the snapshot is enqueued. Peers outside the old/new holder sets receive no actor update traffic.
- Presence, profiles, tab-list state, moderation, and other explicitly global features remain cluster-global.

### Deterministic actions and ground truth

- Represent all fallible player, entity, inventory, and world changes as action envelopes containing actor, synchronized tick, sequence, affected chunks, exact preconditions, forward delta, and exact undo delta.
- Resolve conflicts with the specified hash of tick and participating actor IDs, with stable actor-ID ordering as the collision tie-breaker. The result must be independent of message arrival and pairwise evaluation order.
- This existing hash-based, pairwise, order-independent conflict mechanism must converge after incomplete same-tick delivery. Given peers A, B, and C producing conflicting actions a, b, and c, arrivals may initially be A={a,b}, B={b,c}, and C={c,a}, causing different provisional local resolutions. When the missing action reaches each peer during the next tick, every peer must independently settle on the same result by applying the existing conflict mechanism. No resolution exchange or other additional communication is required after delivery of the missing actions.
- Each loaded chunk keeps per-tick references to every action concerning it. Entities count as actions of the chunk containing them. An action concerning multiple chunks is referenced by every affected chunk. When a peer has the complete action set from every peer recorded as holding a chunk for that tick, and all prior ticks for the chunk are resolved, it knows locally that the tick can advance into that chunk's ground truth.
- A multi-chunk action d can keep a chunk unresolved while another affected chunk remains unresolved. However, when the existing deterministic conflict mechanism proves that d has lower priority than all relevant single-chunk actions and therefore cannot change the settled result of a, b, and c, a, b, and c may advance into ground truth while the tick remains partially unresolved only for d and its other affected chunks.
- Add a settlement-dependency protocol for actions whose complete entity/chunk dependency set is not held by one peer. A partial holder announces the unresolved action and missing sides to every advertised holder of its current chunk and every advertised holder of the other entities' chunks. These notices mark exactly which settlement the peer is waiting for; they do not acknowledge or serialize action delivery.
- Any peer holding the complete dependency set resolves both or all sides through the normal deterministic action mechanism, advances the involved sides into ground truth when eligible, and publishes that settlement to every recorded waiter.
- If no peer holds every involved chunk, notify the primaries holding portions of the dependency set. The primaries deterministically choose a resolution owner using their current load, the chosen primary requests the missing chunks from the other primaries, and it resolves only after it holds the complete state and action set. Chunks acquired for this purpose remain loaded for five seconds after settlement.
- A secondary may unload a chunk referenced by an unsettled transaction after its normal drop publication. A primary retaining any part of that transaction cannot unload it until settlement, ensuring that at least one primary keeps the complete transaction log and no action is lost. Existing global holder advertisements identify the holders without a discovery round trip; extra communication is limited to dependency notices, primary resolution-owner selection/chunk acquisition, and settlement publication.
- After finishing a tick, each peer sends all relevant peers one holistic tick-finish message. It lists, per chunk, every action the sender issued for that tick. Entity actions are listed under the entity's chunk and multi-chunk actions under every affected chunk. Zero-action chunk entries are omitted, but the message itself is holistic: its presence proves that no omitted action from that sender remains outstanding for the tick. Together with the known holder set, this lets every peer determine locally when it has received the complete action set without acknowledgements and without waiting indefinitely for an action that was never issued.
- A valid QUIC connection resumption is a continuation of the existing peer connection. It does not count as a reconnect, replacement, disconnect, membership change, or interruption for synchronization and action-resolution state. A peer is declared dropped after sixty seconds without a keepalive or successful QUIC resumption. A separate new connection authenticating as the same peer is rejected while the existing connection remains active. Such a new connection may be accepted only after the existing connection has produced no keepalive for three seconds. Accepting it starts the dropping procedure for the old connection, and the peer is considered dropped.
- When a peer is declared dropped, every remaining peer broadcasts the chunk/tick pairs for which it still awaits that peer's actions according to a received tick-finish message, plus the chunk/tick pairs for which it received actions from that peer without receiving its holistic tick-finish message. The remaining peers fill the reported gaps from their retained action records, apply every recovered action through the existing deterministic conflict mechanism, mark all remaining chunk/tick gaps as no longer waiting for the dropped peer, and remove the dropped peer from chunk-holder and entity-owner state. Every remaining peer writes an error to its configured log output identifying the dropped peer, the drop reason, and the outstanding chunk/tick gaps reconciled during recovery.
- Movement validated by the actor’s owning secondary is infallible. World interactions remain fallible even when produced by an entity owner.
- An owner-issued entity chunk transition is also infallible and travels on the reliable direct entity channel; player entities follow the same rule. Apply it after every other action of its tick so the new chunk association begins with the next tick. Until the transition enters ground truth, retain it in a departure index belonging to the chunk the entity left. If same-tick or future-tick actions have already been indexed under the old chunk association, use the retained transition to move those action references to the new chunk locally without further communication. Because exactly one peer controls each entity, conflicting chunk-transition actions cannot be produced.
- Validate break actions against the exact expected block state. Validate placement and item use jointly against the target world state and semantic inventory precondition.
- Resolve redstone, comparator, observer, explosion, random-tick, door/trapdoor, end-frame, respawn-anchor, and comparable behavior through typed actions and dependencies.
- Run random ticks only on the primary, which emits the resulting typed world actions for holder-side resolution. Secondaries remain physically unable to generate or persist chunks.
- Send reliable per-chunk acceptance batches once per tick. A tick advances into each holder’s ground truth only after all peers recorded as holders for that resolution have accepted it.
- Late chunk holders receive the ground snapshot plus unresolved resolutions known by the provider. Conflict repair uses iterative deltas and targeted undo, never whole-chunk retransmission or replaying local truth from an action queue.

### Membership and ownership

- Treat configured peers only as bootstrap addresses and certificate pins. Configuration never defines the admission quorum.
- Every live QUIC connection publishes its currently connected peer set. For a candidate, compute the dynamic required set as the union of peers advertised by all currently connected peers, excluding the candidate itself.
- Admit a candidate through either path:
  - immediately when it has direct QUIC connections to every peer in the dynamic required set; or
  - when every peer in that set has acknowledged the candidate and ten seconds have elapsed since the candidate began joining.
- Required-set expansion adds the new peer’s connection or acknowledgement requirement without restarting the ten-second timer. Removal of a disconnected peer shrinks the set immediately and cannot stall admission for the remaining component.
- Recompute admission from live advertisements after every connect, disconnect, and advertisement update. A dropped node never prevents the remaining connected nodes from accepting players or admitting another node.
- A node may start with its virtual lobby available before admission, but it may hand users into real players only after admission.
- Graceful departure requires zero hosted players and transfer of every owned entity to a peer holding its chunk. Holder replicas already contain local and ground state used to complete that transfer.

### Chunk acquisition, retention, and lobby

- Keep the lobby fully virtual: no server-side player entity, playerdata load, real chunk, or primary connection is required to display it.
- Preserve the recovered scene exactly: the 9×9 fake chunk area, 16×16 end-portal platform and block entities, normal build height, camera entity, downward view, and the chunk-middle anchor around X/Z `1,000,008`.
- Give lobby sessions the normal play-state packet dispatcher for all entity-free behavior:
  - command graph and suggestions;
  - global player list and profile textures;
  - chat and global player-name completion;
  - `/exitlobby`;
  - entity-free moderation and administrative commands allowed by permission.
- Entity-dependent commands fail normally because no player entity exists until handoff.
- Pin one immutable resume target and playerdata revision for the session before real chunk acquisition begins. Prefer the retained authoritative logout target. If none exists, pin spawn once. Never begin at spawn and silently retarget later.
- A revision mismatch is an invariant failure. Emit one bounded diagnostic containing session, expected revision, received revision, and target; abort the candidate handoff and disconnect cleanly rather than creating a partial player or silently choosing another target.
- Request each missing chunk once from the lowest-latency advertised holder. A chunk response or explicit holder-drop event completes that request. On a drop, choose another holder; use the primary only when no secondary advertises the chunk. Do not use retry timers.
- Retain the reconnect chunk set through the grace period and cancel expiry when the same target returns, so relogging reuses the held chunks instead of restarting at zero.
- Pipeline Java real-chunk delivery without serializing on `SChunkBatch`:
  - Continue encoding and enqueueing bounded batches while previous batches are unacknowledged.
  - Track each `CChunkBatchEnd` in a FIFO ledger with its session revision and chunk positions.
  - Treat each ordered `SChunkBatch` as acknowledgement of the oldest outstanding batch and update the client’s requested rate.
  - Do not make an individual acknowledgement a prerequisite for preparing or sending the next batch.
  - Declare handoff ready only after every target chunk belongs to an acknowledged batch.
- Keep the lobby responsive while acknowledgements or cluster chunks are outstanding. Missing acknowledgement prevents handoff but does not block a task, resend data, or stall commands/chat.
- Send exactly one `CLogin` for the play session. At handoff, do not send `CRespawn`, another login/configuration transition, dimension change, or `StartWaitingChunks`.
- Once all target chunks are acknowledged and membership permits handoff, create/load the real player and use an ordinary same-dimension `CPlayerPosition` teleport, followed by inventory, game mode, camera restoration, time, weather, border, permissions, and entity state in TCP order. The client must never return to a loading-terrain screen during this transition.

### Inventory, health, and global server features

- Add a general server inventory service used by commands and future features. Its public operations cover snapshot, subscription/delta delivery, and semantic mutations such as move, split, swap, consume, equip, and cursor transfer.
- Address inventories and slots explicitly and include expected item, count, and revision preconditions. Local and remote edits travel through the same action/resolution layer and cannot duplicate items.
- Make `/invsee` a UI adapter over this service. Viewing and editing are mandatory, including remote armor, storage, hotbar, offhand, cursor behavior, and conflict handling. Keep separate view/edit permissions, granted to operators by default.
- Replace the light-gray filler with unnamed gray stained-glass panes.
- Add primary-owned `clusterHealthMask` configuration with `off`, `players`, and `all` modes. Default to `players`; masked required health fields report `20`.
- Expose `/gamerule clusterHealthMask off|players|all`. A validated command submits a versioned global configuration mutation; the primary persists and distributes it to all secondaries.
- Never expose the real numeric health of a masked actor:
  - omit optional health fields;
  - put `20` in required Java and Bedrock fields;
  - keep actual health in server and cluster state.
- Always send accurate health to a player for their own HUD and for the entity they are riding.
- Health-driven textures and animations must reflect real health. If the server sends that visual state separately, derive it from real health while masking the numeric field. If the vanilla client can derive the visual only from numeric health, exempt that entity type so the visual remains correct.
- Preserve and verify UUID-based global op/deop, ban/pardon, kick, spectate, chat, private/team messages, global autocomplete, MOTD counts, join/leave messages, `/hide`, profile texture properties, and `/syncstats`.
- The primary saves world, playerdata, moderation data, and cluster policy but opens no Minecraft port. Secondaries open game ports and have no chunk-generation or persistence code path.

## Implementation Sequence

1. Establish the owned `ClusterRuntime`, tick phases, protocol versioning, actor identities, holder routing, and claimed/effective controller split without changing the recovered lobby.
2. Replace tracker ghosts with registered remote player replicas. Prove lookup, selection, collision, damage, skin properties, and despawn before adding the complete action mesh.
3. Implement bounded movement datagrams and reliable visual/boundary streams, scoped strictly to chunk holders.
4. Introduce shared player/entity local-and-ground state, deterministic action envelopes, undo, acceptance, and entity-produced actions. Move gameplay producers onto it subsystem by subsystem.
5. Implement one-shot chunk transfer, holder adverts/drops, retained reconnect targets, primary persistence, and immutable lobby revisions.
6. Add pipelined lobby chunk acknowledgements and same-dimension handoff while preserving the recovered fake-world packets and normal entity-free play dispatcher.
7. Move inventories and `/invsee` onto the general semantic inventory service; then complete global commands, presence, health masking, and membership admission.
8. Remove superseded ghost and direct-mutation paths only after their replacements pass live-process tests. Do not copy broad files from `broken-ghost`; use individual behavior and protocol ideas only after review.

## Test and Acceptance Plan

- Unit and property tests cover:
  - zero-copy validation and protocol-version rejection;
  - wrapping ticks and movement sequences;
  - movement batches never exceeding the computed single-datagram budget;
  - loss of one movement datagram affecting only its contained records;
  - conflict results under every arrival permutation, including three-way conflicts;
  - exact undo without local-state replay;
  - player and entity actions against stale blocks, inventories, and entity state;
  - holder-only routing and cross-chunk boundary ordering;
  - one-shot chunk request/drop/reroute behavior;
  - FIFO chunk-batch acknowledgement tracking with several batches in flight;
  - dynamic membership admission, direct full-mesh admission, acknowledgement-plus-ten-second admission, advertised-set expansion, and peer removal;
  - all health-mask modes, self/mount exceptions, and health-driven visuals;
  - semantic inventory edits and duplicate prevention.
- Add the Cargo feature `cluster-e2e`. Compile and register `/clustertest <test-id>` only with that feature; normal binaries contain neither its command node nor its preparation code.
- Each test ID owns its preparation. The command assumes the two configured test clients are connected and locates them through the test registry; it takes no player arguments.
- The `arena` fixture creates a flat deterministic world with two one-block-deep standing positions separated by one block, faces the clients toward each other, and places both inside interaction range. Other IDs prepare block conflicts, stale breaks, inventory conflicts, entity/world interactions, chunk-boundary transfer, reconnect retention, and anti-cheat divergence.
- Run a real primary plus two secondaries and two Java Azalea clients. Assert:
  - both clients see and can interact with the remote player without an invalid-entity kick;
  - attacks, death, equipment, pose, projectiles, mobs, and world/entity actions converge;
  - clients outside an actor’s loaded chunk receive no actor movement or update packets;
  - skins and profile properties remain intact;
  - reconnect uses retained chunks and does not restart at zero.
- While clients are still in the lobby, assert receipt and function of commands, command suggestions, global player-name completion, tab list, `/exitlobby`, `/ban`, `/op`, `/deop`, and `/pardon`.
- Capture the lobby packet sequence and assert the recovered platform blocks and block entities. Between lobby display and real-world teleport, reject any second `CLogin`, `CRespawn`, configuration switch, dimension change, or terrain-loading trigger.
- Simulate 280 ms RTT. Require the virtual lobby to become usable in under one second and ensure chunk delivery remains pipelined rather than adding one RTT per batch. Compare complete handoff timing against the recovered implementation and reject material regression.
- Test the inventory service directly and through editable local/remote `/invsee`, including armor, offhand, concurrent edits, and gray filler slots.
- Test Java and Bedrock protocol serialization for controller input, movement, lobby state, health masking, inventory, and entity updates. Full live multi-node E2E remains Java; Bedrock receives protocol and handler integration coverage.
- Bound diagnostics by failure key and cooldown. Tests hold a persistent missing route/revision/entity condition across many ticks and assert it produces one diagnostic rather than console spam.
- Prove runtime wiring in live processes for every producer, transport, receiver, resolver, and apply path; protocol-only tests do not count as completion.
- Run formatting, focused crate tests, workspace tests, Java E2E, Bedrock integration tests, and the existing Nix development build as foreground commands with completion timeouts.
- Build the final development closure through the existing Nix scripts, copy it to the laptop, and restart the primary, local secondary, and laptop secondary with the existing launchers. Use a single foreground orchestration command that exits on readiness or timeout; do not poll detached processes, logs, process tables, or tmux state.

## Compatibility and Defaults

- Mixed old/new cluster wire versions are rejected; deployment requires restarting the complete cluster.
- `clusterHealthMask` defaults to `players`, with masked health reported as `20`.
- Lobby chunk acknowledgements are completion evidence only; they never serialize chunk production or transmission.
- The dynamic connected-peer view defines admission. Static configuration supplies bootstrap and certificate information only.
- `broken-ghost` remains a reference branch. Recovered `master` is the implementation base and all behavior is reintroduced in independently reviewable stages.

## Expanded Implementation Contract

The following details are mandatory completion criteria. They retain the concrete interfaces, regression protections, fixture design, and delivery gates from the original long plan. Where earlier wording conflicts with the decisions above, the decisions above govern: movement is split into MTU-sized datagrams, chunk acknowledgements are tracked without serializing transmission, admission uses the dynamic advertised connected set, entity actions use the same truth model as player actions, health masking is configurable, and the test command is `/clustertest <test-id>`.

### Executable requirement matrix

- Convert every normative rule in `cluster-plan-v2.md` into a checked matrix containing its runtime owner, producer, consumer, wire path, automated proof, and bounded failure diagnostic.
- Add characterization tests before replacing recovered behavior. They cover exact lobby packet ordering and layout, the command graph, suggestions, global player profiles and textures, initial lobby timing, chunk acquisition, and warm local reuse.
- Keep a forbidden-regression checklist containing every symptom and cause from the recovered-versus-broken comparison.
- A protocol type is incomplete until a live runtime producer and consumer are exercised by E2E. Unit-tested but unwired streams do not satisfy the requirement.

### Owned runtime components

`ClusterRuntime` owns single-writer components for membership and peer lifecycle, presence and profiles, chunk holders and acquisitions, resume metadata, entity ownership and replicas, controller filtering, tick buckets and resolutions, global administration, diagnostics, and metrics.

- State remains in owned maps, queues, and phase-local values.
- Shard update state by chunk, including the players and entities loaded in that chunk. Cross-chunk actions are deterministic joint shard categories; only categories with disjoint chunk sets run concurrently. Rayon workers receive exclusive owned work and return owned results at the phase boundary rather than coordinating update application with atomics.
- Gameplay submits bounded inputs with `try_send`.
- Read-heavy publication uses immutable end-of-tick snapshots. Do not add per-field `ArcSwap` or atomics where a phase boundary is sufficient.
- Remove mutable cluster globals and `OnceLock` outboxes as their adapters move into `ClusterRuntime`.
- Network tasks may await their owned sockets. Tick, login, logout, status ping, capture, chunk routing, and apply paths never await cluster state.
- Audit cluster-reachable world APIs. Replace lock-taking calls with phase-owned or snapshot-based alternatives before calling them from cluster progress paths.

### Player, avatar, replica, and lobby interfaces

The player model has four explicit layers:

- `PlayerAvatar`: entity, living state, UUID, complete profile properties, game mode, equipment, visual metadata, combat state, and optional global ID.
- `Player`: the local controller with connection, claimed/effective input tracking, inventory, permissions, and loaded playerdata.
- `RemotePlayerReplica`: an avatar with origin and owner identity, replicated local/ground state, and no client or local controller.
- `LobbySession`: authenticated connection, UUID, profile properties, permissions, presence, shared play dispatcher, immutable handoff target, and progress only. It has no avatar, inventory, decoded playerdata, or world entity.

World lookup, tracking, combat, interactions, selectors, visibility, and packets operate on an avatar or typed global target. They do not assume that every player-shaped entity owns a local `Player`.

Every remote replica receives a normal local entity ID on each observing secondary, is present in entity lookup and tracking, maps back to `GlobalPlayerId`, has texture properties before its spawn packet, follows deterministic chunk visibility, and never processes client input or persists playerdata.

The shared play-session dispatcher handles keepalive, client settings, chat, chat commands, command suggestions, tab-list state, valid plugin messages, disconnect, and rate limiting for both lobby and joined sessions. World and entity packets become available only after handoff. Do not reintroduce a narrow lobby packet loop.

`CommandSender::Lobby` represents the authenticated UUID and response connection. It reads UUID-based global permission state without playerdata. The complete permission-visible command tree is sent. Commands needing an entity remain discoverable and return the standard missing-player/entity error. Entity-free commands, moderation, `/list`, `/exitlobby`, chat, and suggestions use the normal dispatchers and global directory. Lobby users appear in presence, status counts, and the tab list with complete properties while remaining absent from entity selectors and world registries.

Build immutable lobby packet images for every supported Java version during startup. Do not use lazy locks, unsafe atomic pointers, or hand-written section encodings. Preserve the exact recovered 9×9 scene, platform, heightmaps, lighting, 256 portal updates, 256 block-entity packets, camera, spectator presentation, clock, title, coordinates, and normal world height.

### Resume metadata and atomic handoff

`PlayerResumeHint` contains UUID, dimension, position, rotation, view radius, playerdata revision, previous owner, retained chunk set, and expiry. The primary persists it atomically with playerdata and distributes immutable revisioned updates.

The complete handoff is:

1. Display the virtual lobby immediately.
2. Freeze one resume hint, or one spawn hint for a genuinely new player.
3. Acquire that exact target chunk set without blocking the session.
4. Request playerdata only after the target chunks are locally installed.
5. Require the exact frozen playerdata revision.
6. Construct the local `Player` and avatar off-registry.
7. Insert them into all registries exactly once.
8. Enqueue real chunks, player information, inventory, accurate self health, abilities, game mode, time, weather, border, spawn state, and visible entities.
9. Confirm all real Java chunk batches through the FIFO acknowledgement ledger while continuing to pipeline batches.
10. Enqueue an ordinary same-dimension teleport and camera restoration on the existing ordered connection, remove the fake marker, then publish active presence.

No revision mismatch may leave an inserted avatar, loaded playerdata, or partially registered player. `/exitlobby` only clears a manual or permitted forced hold; it never bypasses readiness.

On disconnect, freeze the exact watched set and resume hint before registry removal, retain the chunks for 30 seconds, continue advertising the holds, retain the primary claim for 30 seconds, and save playerdata plus hint under one revision. A same-secondary reconnect during grace reports 100% on its first progress update. A different secondary fetches from the advertised former holder using measured latency. Never fetch spawn while a hint is unresolved, retarget after acquisition starts, retry by timeout, or unload before claim expiry.

### Wire layer and stream inventory

Add `pumpkin_cluster::wire` with endian-stable headers, `Bytes`-backed borrowed readers, fixed-layout records, validated offsets and lengths for variable data, and record/payload limits checked before allocation. Hot-path batches group records by update type. Unsafe parsing is forbidden.

The stream inventory includes per-player visual, use/break, world-action, and interaction/combat streams; entity streams grouped by update type; reliable control streams for membership, presence, holder changes, administration, chunk transfer, ownership, health policy, and accepted ticks; and multiple independently decodable movement datagrams each tick.

Malformed input produces one keyed, rate-limited diagnostic containing peer, stream kind, record count when available, and decode reason.

Movement records contain global actor ID, wrapping sequence, disciplined tick, position, velocity, body rotation, head rotation, and on-ground state. Receivers reject only stale or duplicate sequences, tolerate missing datagrams without retransmission, and immediately update registered replica local state and tracker output. The sender partitions records greedily to the negotiated QUIC application limit and conservative 1200-byte path budget including overhead.

Reliable visual records cover equipment, armor, hand selection, sneak, sprint, swim, glide, blocking, arm swing, skin layers, item-use start/abort, and break-animation start/stop. Every visual type requires a remote-avatar and client-visible assertion.

### Actor actions, interaction families, and truth

All players and entities use the same action envelope, conflict ordering, local truth, ground truth, unresolved buckets, holder capture, global acceptance, and mechanical undo. Entity AI is an action producer. Ownership only decides which peer runs that AI.

Action coverage includes:

- exact-state block breaking;
- placement atomically coupled to semantic inventory consumption;
- doors, trapdoors, end-portal frames, respawn anchors, buttons, levers, and comparable block interactions;
- redstone outcomes and delayed comparator/observer dependencies;
- explosion block deltas;
- primary-produced random-tick deltas;
- sheep grazing and other entity-caused world changes;
- bow, crossbow, projectile, and item-use effects;
- direct player and entity attacks;
- sweeping damage, knockback, ignition, cooldown, durability, statistics, advancements, death, and respawn;
- item pickup and drop;
- click, split, merge, swap, cursor, armor, offhand, container, and invsee inventory transfers.

Every fallible action carries actor identity and sequence, disciplined tick, affected world/chunk keys, exact preconditions, optimistic result, semantic transition, and exact undo. Conflict order is `xxhash(cluster_seed, tick, actor_id)` followed by actor ID. Property tests cover all permutations of at least three conflicts.

Individual actions never receive replies. Reliable accepted-tick batches are the sole confirmation. A losing optimistic action runs only its stored undo before applying the winning resolution. Local truth is never regenerated through replay and conflicts never trigger whole-chunk repair.

Inventory cluster truth is formed only from semantic operations with source, destination, item identity, count, cursor state, and before-state validation. Raw client slot packets are input claims, not truth. The general inventory service exposes snapshots, subscriptions, and mutations independently of `/invsee`; the command is one authorized UI consumer.

### Locally derived inventory-block actions

- Furnaces, hoppers, and other blocks that change inventories are simulated locally by every peer holding the chunks involved, including secondaries. Every holder deterministically generates the same fallible processing actions.
- These use the same per-tick chunk or joint-chunk buckets, dependency graphs, conflict resolution, undo, replay, Local truth, and Ground truth as other actions.
- Only transmission at creation is suppressed. Neither processing-generated inventory changes nor block-state changes such as a furnace becoming lit or unlit are broadcast. External actions, including player inventory changes, placement, and activation, are communicated normally to the relevant holders.
- Locally generated processing actions otherwise are just regular actions and are treated without any special casing: they remain in the normal retained graphs and are included when those graphs are sent with a requested chunk.
- When an input such as furnace activation arrives for an earlier tick, simulate the affected block through the intervening ticks and insert its locally generated processing actions into the local action backlog at their correct ticks.
- Changes to an input's resolution must propagate through these processing actions and their dependents. If an activation is undone and another action activates the furnace two ticks later instead, the resulting smelting and item-change actions must also move two ticks later. Undo the earlier effects and resolve the correctly timed replacement actions and all affected dependencies through the normal backlog mechanism.
- Visual burn and smelt progress is derived locally from the synchronized inputs and processing history. Timer advancement requires no network update.

### Chunk state machine and snapshots

For each acquisition, select the lowest-latency advertised holder, record it, and send exactly one request. Complete on snapshot arrival or an explicit dropped/unavailable response. Only that response permits selection of another holder. Use the primary when no secondary advertises the chunk. After 30 seconds without either outcome, emit one error and leave the request intact; do not retry automatically.

A snapshot contains current ground truth, every unresolved update known by the provider, all known holders, and its ground-truth revision. Advertise a hold only after validating and installing the snapshot. Advertise a drop before deleting the last local copy.

Secondaries use a level implementation with no generator, chunk writer, or playerdata writer. The primary alone loads and generates absent chunks, runs random ticks, retains claimed chunks, and writes accepted truth.

### Membership, ownership, and always-synced regions

The admission set comes exclusively from the transitive live peer views advertised over current QUIC connections. Static configuration provides addresses and certificate pins only. The acknowledgement-plus-ten-second path and direct-full-mesh path use the precise rules above; losing one peer immediately removes its stale requirement and never stops the remaining component.

Graceful leave requires no hosted players, transfer of every owned entity to a holder, completion of the membership transition, and publication of final holder and ownership snapshots. Entity identity consists of origin server, origin-local ID, and generation so reuse cannot collide. Ownership transfers only during graceful leave in this version.

Entities, players, and chunks always have local and ground state on every peer that loads their chunk. Only the owner ticks an entity, but all holders resolve its world actions. Movement and updates go only to current holders.

Always-synced regions are explicit world regions. Spawn chunks register automatically and keep relevant entity and world streams active on every peer.

### Global services and health policy

Complete UUID-based `/op`, `/deop`, bans, pardons, primary persistence, global `/kick`, `/spectate`, public/private/team chat, suggestions, tab list, MOTD count/sample, `/hide`, `/invsee`, and `/syncstats [all|chunk [x z]]`. `/hide` removes the player from visible entities, tab list, suggestions, status sample/count, and online surfaces, and emits the cross-peer leave/join presentation on toggle.

Lobby sessions participate in presence, chat, tab list, suggestions, moderation, and status counts without entering entity selectors or simulation.

Health masking applies at Java and Bedrock serialization boundaries. `clusterHealthMask` has `off`, `players`, and `all`, defaults to `players`, is persisted by the primary, distributed by version, and is changed with `/gamerule clusterHealthMask off|players|all`. Required masked fields contain `20`; optional numeric health fields are omitted. Actual health remains in local and ground truth. A viewer’s own HUD and current mount remain accurate. Independently encoded health-driven animation and texture state uses actual health. An entity whose required vanilla visual can only be derived from numeric health is exempt so its visible behavior remains correct.

### Tick banks, phases, and backpressure

Each worker owns two preallocated typed banks. At a tick boundary, swap only if the collector already returned the processed bank through the capacity-one channel. Use `try_send` and `try_recv` only. If no bank is available, retain later updates in distinct tick-indexed buckets inside the current owned bank. Do not sleep, await, spin, discard actions, or allocate an unbounded emergency bank. Emit one diagnostic per minute with delayed tick count and high-water values. The collector fuses type groups and returns emptied banks to their originating workers.

Within a shard, apply network inputs, controller filters, physics, spatial snapshots, intents, resolution, actions, ground promotion, publication, and routing in the defined subphases. Defer changes that do not require same-tick visibility to the end of tick. Add an explicit subphase only where gameplay semantics require same-tick feedback.

Each subphase partitions updates into single-chunk categories containing that chunk's loaded actors and joint categories containing the full chunk set of a cross-chunk interaction. Schedule only mutually disjoint categories together on Rayon. Transfer exclusive ownership into each worker and collect owned results before advancing the phase; no gameplay update uses atomic mutation or shared synchronization.

### Diagnostics and code quality gates

- Normal ticks, actions, movement, and chunk progress emit no info logs.
- Expected startup and membership transitions emit one structured event each.
- Failure keys include peer, stream, actor or chunk, and reason. Repetition increments counters instead of repeating lines within the cooldown.
- An unresolved acceptance emits a warning after it has exceeded ten seconds and at every subsequent ten-second age band. Diagnostics use error level once actual delay exceeds sixty seconds. A key's last emitted age suppresses the same or any lower age for five minutes, while a newly reached higher age band bypasses that cooldown. Observation must never remove, retry, or otherwise change acceptance state.
- Do not introduce unsafe code, new explanatory comments, or new doc strings in implementation code. Preserve upstream documentation.
- Keep domain and protocol logic in `pumpkin-cluster`; keep server adapters small and split by replica, entity, chunk, combat, inventory, world action, and administration.
- Add a CI audit rejecting locks, blocking calls, unbounded channels, and forbidden awaits in cluster runtime paths.

## Complete Test-Only Interface

Add the Cargo feature `cluster-e2e`; normal and `pumpkin-dev` builds do not enable it. A separate Nix E2E output enables it.

Only under that feature, register `/clustertest <test-id>` with level-four permission. It is absent from normal registration and `CCommands`. It assumes the configured two test players are connected and uses the test registry rather than command arguments. Every test ID performs its own setup through normal cluster world-action, inventory, health, policy, and teleport APIs and reports completion only through an accepted state observed by the harness.

The `arena` preparation uses the flat test world at Y=80, clears the surrounding volume, creates a stone deck, makes 1×1 standing pits at X=-1 and X=1 separated by the solid X=0 block, places the two players at pit centers facing each other within normal attack reach, and resets health, hunger, effects, survival mode, and deterministic combat equipment.

Additional IDs prepare stale block conflicts, simultaneous placement, inventory conflicts, entity/world interactions, chunk-boundary transfer, reconnect retention, and intentional claimed/effective controller divergence.

## Full Completion Gates

### Unit, property, and protocol tests

- Wire round trips, malformed offsets, oversized batches, allocation bounds, and fuzzed decoders.
- Wrapping actor sequences and disciplined ticks.
- Two-way and three-way conflict order under every input permutation.
- Exact undo and convergence without replay.
- Inventory conservation across accepted, rejected, and conflicting operations.
- One-request chunk acquisition and explicit holder-drop handling.
- Dynamic admission, peer loss, leave invariants, identity reuse, and ownership transfer.
- Resume-hint/playerdata revision matching and atomic insertion failure.
- Lobby and real-player command capabilities.
- UUID moderation and profile-property propagation.
- Lobby packet images for every supported Java version.
- Java and Bedrock lobby, presence, handoff, health, entity, controller, inventory, and action state.

### Three-process fixture

Use one primary with no Java or Bedrock listener, two secondaries, and one Java Azalea client per secondary. Generate certificates, run a local SNTP responder, generate and distribute a `minecraft:flat` world, mount secondary persistence locations read-only, capture structured child output, parse logs only after process exit, impose one global deadline with stage diagnostics, and build the server with `cluster-e2e`.

Use Bungee forwarding to attach deterministic synthetic texture properties to both offline profiles. Inspect decoded command, suggestion, player-info, chunk, teleport, respawn, and disconnect packets through Azalea packet events.

### Lobby regression scenario

Before the primary is available, both clients must enter the exact virtual lobby, have no player entities, receive complete `CCommands`, see `/exitlobby` and moderation commands, get standard errors for entity-required commands, autocomplete global names, see both profiles and byte-identical textures in tab, exchange chat across secondaries, satisfy keepalives, and load no real chunk or playerdata. Starting the primary afterward must allow admission and handoff without reconnecting either client.

### Latency scenario

Route Java TCP through 140 ms delay in each direction. Require final configuration receipt to first lobby enqueue within 50 ms and client lobby display within one RTT plus 250 ms. Lobby entry performs no cluster request, disk read, playerdata request, or acknowledgement wait. Real chunks pipeline with several batches in flight; acknowledgements confirm completion without adding one RTT per batch. Handoff uses a teleport and never exposes a loading-terrain transition.

### Replica and arena scenario

Run `/clustertest arena`, observe accepted setup on both clients, and verify each remote avatar’s UUID, global ID, local entity ID, full textures, position, rotation, lookup registration, and interaction mapping. Exercise jump, look, sneak, swing, hand selection, block, use start/abort, right-click, attacks, death, and respawn. Verify health, knockback, cooldown, durability, statistics, death presentation, and convergence. Neither client may disconnect and the former invalid-entity error must never occur.

### Action and convergence scenarios

Exercise same-tick different-block placement, the dirt-break/wood-place/late-dirt-break case, concurrent inventory moves and placement, block interactions, redstone dependencies, explosions, primary random ticks, entity-produced block changes, secondary-owned summoned entities, primary-owned naturally spawned entities, graceful ownership transfer, datagram loss/reordering, explicit holder drop with one replacement request, and identical accepted ground truth across all processes.

### Global, reconnect, persistence, and logging scenarios

Exercise completion and suggestions for `/ban`, `/pardon`, `/op`, `/deop`, `/kick`, `/spectate`, chat, `/msg`, team messages, `/hide`, `/invsee` view/edit, `/syncstats`, and status counts/samples. Restart all processes and verify primary persistence.

Disconnect a player after a complete view, reconnect to the same secondary inside 30 seconds, require the first progress update to be 100%, then reconnect through the other secondary and prove it acquires from the prior holder. Hash secondary data directories before and after and require no writes and zero generation counters.

A successful E2E run permits no unexpected cluster warning/error, no routine info logs, at most one occurrence of each keyed failure diagnostic, and fixed bounds on startup/membership events. Parse process output after shutdown rather than polling it.

### Verification and delivery order

1. Commit the requirement matrix and failing characterization/regression tests.
2. Introduce owned runtime actors, tick phases, and borrowed wire framing.
3. Implement the shared lobby dispatcher and exact presentation.
4. Implement resume hints, retention, revision checks, pipelined acknowledgement tracking, and atomic teleport handoff.
5. Replace tracker ghosts with first-class avatars and replicas.
6. Add holder-routed MTU movement and reliable visual streams.
7. Add player/entity combat, interactions, world actions, inventory, and truth resolution.
8. Complete chunk, membership, ownership, always-synced region, and persistence invariants.
9. Complete global services and health masking.
10. Pass formatting, workspace compilation, targeted unit/property tests, cluster simulations, Java live E2E with and without latency, Bedrock protocol/state tests, Nix normal development build, and Nix E2E build.
11. Deploy locally, sync the Nix closure to the laptop, and restart only after all automated gates pass. Use one foreground orchestration command with readiness success or a timeout, followed by one bounded final capture confirming membership, holder exchange, and no repeated diagnostics.

Fixed assumptions: implementation starts from recovered `master` commit `88b2694da`; `broken-ghost` is reference material only; Java receives full live-client coverage; Bedrock shares core state machines and receives protocol/state coverage; the harness creates the flat world; test commands exist only with `cluster-e2e`; and working recovered behavior remains until its replacement passes equivalent tests.


okay here is a proper design that solves this easily. when requesting a chunk from a peer the peer sends its ground truth chunk, and all graphs it has at that very moment including all ticks done messages. by that the peer now knows from which peers for all ticks up to the tick it was sent at. now the only issue is the following that needs to be solved: during transmission of the chunk and till the receiving peer broadcasts that it now holds the chunk time will pass. during this time new ticks will happen and updates sent out, and the new holder does not get these because it is not known to others that. i suggest the following resolution: a) split up being a chunk holder and being  a subscriber to updates. so when a peer wants a chunk it sends one peer the holds the chunk the request for the chunk, and to all others a list of all chunks it starts being a subscriber (including the current tick + 1) for (one-directional no ack). when a peer broadcasts that is drops a chunk that also unsubscribes it. if a peer gets actions for a chunk it is not subscribed to it will once per chunk send an unsubscribe notice only to the peer that gave it actions for the chunk if it didnt send one our for that chunk the last 2 seconds). this does not solve our problem however unless we change it so that the chunk provider only sends out its graph up until the tick before the requested tick, and all other peers send their actions for starting including the requested tick if they hold the chunk. this mostly solves the gap, besides for those actions the peer who sent the chunk didnt have yet. but this can also be solved: using a special subscription relay status the peer will relay all actions / tick done packets it gets from the moment after it sent the chunk. this still has a problem if we sent chunks in parallel to processing actions. we must not do that. so incomming actions are not immidiately resolved and added to the graph (they should not anyway for performance reasons) isntead at one phase per tick we send out the chunks, and in a phase before that we process the actions we accumulated since last time. with other words the moment we starting resolving actions all new arrivals are collecting only for the next tick.  so we can never lose actions here. so now the peer can just relay and this insures the peer gets all updates as long as the relaying peer does not drop in the meantime. another issue is that other peers will have send out actions in the meantime so they must be able to identify the actions they send out. lets resolve this as following: so far we only have ground truth <-> local peer truth. we can solve this by going one step further we have an implied actor-local truth. this applies to both local entities and players. when we process an entity AI in order to make it do somethign they get a only a readonly ref to chunks. if they interact with anything this must happen through actions. in order for the code to be able to see a local view of the world and see its changes directly apply we just save whatever it did via diffs. if it changes any block/blockstate we just have hashmap of location and new blockstate, if it does anything to an entity we do with via CoW, if any property of an entity or tileentity is changed we just copy the whole entity and apply the change. so when the actor requests an (tile)entity we give it a CoW wrapping  RO ref if it did not modify the entity yet, or the local copy. as we issue actions for all changes, we can and must drop these local diff and copied entities the moment the entity finishes processing for the tick (i.e. all processing of an actor on an entity must happen directly after another in the same scope.), during this we also already have crated the actions and queued them for sending just as we did so far, but we no longer send the client packets like as we would before so no optimistic view yet. the only difference now is any action queued for sending will also be collected for processing in the local tick to the local and ground truth, just as if we received these from another peer - no difference. given whenever we apply any action we need to send the locally connected players the updates, this is why we cannot do the optimistic update, earlier otherwise we would have to treat these different. as we treat them just like actions that came from a different peer, this now gives us the same behaviour these a new actions so our local players do not know about them yet, so for these there is nothing to undo yet, and if we now accept the action we do need to send the update. so everything just works out. and this gives us something amazing: we can not complete remove the locks from the chunk manager. we have four phases when we only consider chunk management. 1. adding/dropping chunks if they finished generation or loading on a primary or got received by a peer, or processing the list of chunks to mark as requested. (yes this means entities cannot interact with chunks that are not available yet, and as we do not wait during actor/block update propagation ever - but this is already solved by minecrafts two rings of chunks on the outside: first no entity processing, then no update processing, and queing the update) so when we need a chunk that we dont have for block updates we just request it, and queue to update together with the tick, so when the chunk arrives, we can just as normal finish all of these retrospectively for the tick they happened in - the only case where this does not work and we accept that if we have a block update cascade e.g. redstone tricks for zero-trick transmission and the needed chunk does something that propergates back into the loaded world and we in the mean time declared the chunk as done and applied ground truth - this will nearly never happen tho, because redstone is done by the primary, and it does have the world locally so it can load the chunks likely fast enough for this to never happen, given that a chunk can only get done if the remote peer also processed it)  2. processing local actors they all get only a readonly view as specified now, no locks required 3. resolving actionson the graphs and updating local truth 4. done marking and applying actions to ground truth   given that we process actins based on the chunk they require with rayon, no locks are required anymore. with other words this enabled us to have 100% parallelism in entity and player actor processing. cool stuff. there still is one failure case we need to resolve: re requested a chunk and relaying peer drops before we get all actions and tick done envelopes. in that case there is nothing we can do. the only way to solve this is to send out another new package to all holders of the chunk and ask them to send the ground truth they currently have IFF it is after the tick for which we miss actions i.e. the tick we requested the chunk earlier. all peers broadcast the answer to all holders but the requestor saying what tick has finished completely processing for their ground truth of that chunk rn. after the receive that from all holders or a timeout of 1s, they use the hash resolution to decide locally without communication which peer sends the ground truth to the requester. newest ground truth takes preference, hash used for deciding between ties. if none have the ground truth ready for that tick yet the winner waits till it gets the ground truth to finishf or that tick. then it sends to the reqester. if we are waiting due to winning with a timeout and during the wait more come in and this makes us lose, then we accept this as a loss. (i.e. if we have network problems with one-way taking longer than 1 seconds then we may have multiple peers sending the ground truth, that is okay and a tradeoff we need to make to bound the time this process takes) of course peers that have the same problem instead answer not with a tick they have, but that they cannot participate at all. there updates send only the ground truth and the tick for which the ground truth it is, as well the graphs for all marked done ticks, not the graph after those. the receiving peer now drops the old ground truth and soon but not yet also  graphs for the marked done ticks. then it rebuilds the local truth by replaying all actions including the marked done graph (marked done graphs are frozen but there may still be cross chunk actons that needs resolving). this requires a special procedure for the marked done graphs, as the accepted action may disagree with the graph we will soon discard, to solve this we first create a mapping between the new and old graph (they are not the same size as applied to ground truth actions get deleted so far as by the spexc. actually this makes me realise we should not delete actions when applying them to the ground truth but only when for a tick all actions have been applied to the ground truth, so this overriddes the previous instruction, now instead of dropping immidiately they are instead marked as applied_to_ground_truth), this now solves the problem: the received marked done tick now contains the graph that strictly contains all actions contained in the graph we will soon discard.  by having a mapping between the new graph and the to be dropped graph, we can now find conflicts. the new graph wins and we do not even need to progagate locally so we can just accumulate the actions that changed resolution (delta pool) for cross chuck/entity actions. this delta we can then start with at the next tick for the other chunk/entity in step 3. of these actions, only those marked as applied_to_ground_truth in the received done graphs will get the always_wins attribute i.e. in the resolution for the other chunk we dont use the hash resolution but always let this action win. this works because while the other side in this rare case has not yet settled, we know by receiving this from the other client, that eventually it will settle into the state the always_win_action. (such always win actions may still need to replay the effect in the not-marked-done if a condition there changes for correct local-truth, but once marked they will always win, but replay still happens, just not in the chunks we received for this rare drop-resolution. after we replayed the graph of done, and the not dropped graphs after that, we have the new local truth, and because we compare the graphs and find the delta, we can even do the local delta procedure to modifying the local truth from the wrong state to the correct state. (please quote this message verbatim into both plan docs, and the human message md file)

rejected. not my message. you did not collate you came up with random stuff. also please tell the agent that they must have actions be applied at two locations.  both the ground truth and local truth. for the local truth an action coming in at the current tick is checked for conflicts within the current tick and if there are resolved via the specs hash alg. if an action is undone player clients get the information to undo what they see. if an action arrived for a previous tick that tick has to searched for conflicting actions and resolved any action that would be undone, or replayed differently itself then has to looked for dependent. the important info here is that we do not have any chunk/entity information at all for that tick at this search step, but we dont need to because we know a) the action was valid on the peer that issued it, and b) all actions already here have been made valid. as actions save their conditions and effect, we know for a fact that and condition must be satisfied locally unless a same tick action changed that location. in order to be able to solve this efficiently, per tick, and then separately per chunk, entity and inventory a directed graph is build during resolution, with all locations (i.e. block location, inv slots, entity properties) of either conditions or effects being the entry points into this graph, importantly conditions and effects are a different list as only effect and conditions can conflict, not conditions with conditions or effect with effects (yes this means place block has a condition and effect on the same location). the action contains the result of the current resolution. so to add the new actions we just look at the location of the opposite type and if we match then we have an entry point of a potential conflict to resolve - the condition could still be satisfied by the effects we fould at the solution. with other words these are a multiset. if no locations matches or any of the potential conflicts turns out to be orthogonal then we keep this action in the pool for the resolution with the tick after this one, as well as to the delta pool, and add it to the graph and add the entry points. if we do conflict everything is resolved by the hash spec, if that results in an action being undone the action is replaced with an undoaction wrapping it, and the undo action core still was in the graph so we can just look what its edges go to, to find all actions that depended on its effect so we can directly continue with resolution (yes that means an actions undo normally doesnt change locations, if it does change locations as more commonly for replays then we have to start form the roots again), other actions on failure are instead replayed the same applies but it more likely that the replay changes locations e.g. shift inv move changing the slot location(s) where the shifted itemstack lands. importantly for any replay or undo we also have to add it to the pool for the next tick, as well as the delta pool. if an action also has effects that changed effecting another realm e.g. another chunk /entity/inv then we need to add the replay/undo to the realm actions that need to be resolved. next we finish all resolution for the current tick, i.e. if we have undo/replay that cover multiple realms e.g. mutiple chunks  we also have to now do the joint resoltion, (an undone action gets effectly removed but it shold not be removed from the graph, because undos are also themself undoable i.e. a redo which can happen if an action taking priority causing our undo is itself undone). once we finished all of those we proceed to the next tick.  here we now have the pool of actions that caused this resolution cascade but also all ations that got undo/redo/replayed due to it. we now need to do the whole process again for these actions, with the difference that these actions do not get integrated into the graph, and they always win (they happened earlier in time). this proceeds till we are back at the current tick. now we have collected the delta pool. (remember so far we have not changed the world at all, we only operated semantically on actions deciding which ones get undo/redo/replay or for new actions applied/not-applied (the second is not ignored, it just means its an undoable action and it got undone, but as its new it never had an affect so its just not applied). we also have the local truth. now we need to take the pool and starting at the current tick applies the deltas i.e. replay means undoing the old effect applying the new effect, sending it out to clients, and undo/redo/apply just means doing that and sending it to clients. then we go to previous tick back in time and do the same, till the whole pool is done. now we have updated our local truth and also the clients as they see the local truth via our send out deltas. the other one is the gorund truth. here everything is a lot a lot easier. once we have finished doing this here, we just check if for each chunk/entity we got from all peers the number of actions as they posted they sent out that tick. if any are missing we do nothing and we are done. if none are missing we mark that chunk/entity/inv for that tick as done. after we finished marking we go over all done chunks/entity/inv now we take our already built action graph and apply each action which does not depend on any action which depends on a different chunk/entity/inv not marked done.  i.e. if for an action all parts are marked done we apply it to the ground truth. now something very very important comes up: earlier we had replay actions that could change which location they apply to on a replay. these have to be determinstic and finitely many locations. because besided for our two entry multisets to the graph we actually have a third, and whenever we add an action to the graph we also need to consider theentry positions for wiring dependencies. a replayable action that may change position when it gets added needs to mark all locations it could ever replay to i.e. have an effect on, or depend on i.e. depend on depnding on the effect it ends up having. e.g. for an inv this could be the whole inv e.g. shift op. during our earlier resolution we didnt need to consider these as they are counterfactuals, but we cannot commit any action to the ground truth that depnds on a location that was marked by a replayable action as that it could possibly conflict. so in total we actually have 4 multisets 2 for the factuals and 2 for the counter factuals. to proceed if we have a replayable action that gets applied to the ground truth we mark it as applied_to_ground_truth and retain it in the graph and all sets until every action of that tick has been applied to the ground truth. marking the action settled can unblock later actions; the graph and sets are removed only after the whole tick drains. if an action would be applyable but there is a counterfactual, it checks if the counterfactual action has all its depdendcies done, because even though it did not apply yet its just after us in order, so we can apply (all its realm are marked done so no new actions can arrive as such it could never be triggered to replay). this must ebe precisely impl like this no differnt. any other approach will produce wrong results. actually it must be impl performantly, so for all of this you must consider how to avoid having to check everything i.e. the graph. we also dont want to do this procedure multiple times. so we only do it once per tick with all till then collected data from other peers. please take this and the last message and quote it to the human messages, also add both verbatim to both plans without any edits
