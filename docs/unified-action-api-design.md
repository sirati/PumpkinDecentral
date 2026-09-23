# Unified action API: reviewed design

**Edits to this document are forbidden unless the human has explicitly signed off on the exact diff.**

Status: design only. Reviewed by world/chunks, item actions, inventory service, inventory ingress, controller architecture/wiring, and global services. Normative sources remain both cluster-plan-v2 files; reread after compaction.

## Single public path

Every Java/Bedrock player, entity AI, primary random tick, redstone, explosion, command, and inventory producer submits through one bounded `submit_action`. This accepts a complete owner-created `Action` or a client-claimed `ActionProposal`; the owner materializes authoritative conditions, effects, and undo before admission. A client packet cannot claim ground truth. One `apply_action` interpreter checks all conditions against a stable owned view, computes all results, validates all invariants, then commits every write atomically. Failure changes no stakeholder. The same interpreter applies optimistic local and accepted ground state. One `undo_action` interprets the inverse stored in each successful application. Family-specific codecs and constructors never create independent routing, conflict, apply, or correction pipelines.

```rust
enum ActionInput { Proposal(ActionProposal), Complete(Action) }
fn submit_action(input: ActionInput) -> Result<(), QueueError<ActionInput>>;
fn apply_action(owners: &mut OwnedStakeholderSet, action: &Action, view: TruthView)
    -> Result<AppliedAction, ApplyFailure>;
fn undo_action(owners: &mut OwnedStakeholderSet, applied: &AppliedAction)
    -> Result<(), InvariantFailure>;

struct Action {
    id: ActionId,             // actor, live incarnation, wrapping sequence, family
    tick: TickStamp,
    class: ActionClass,       // fallible, owner-accepted movement, owner transition
    stakeholders: NonEmptySortedSet<Stakeholder>,
    dependencies: Vec<ActionId>,
    conditions: Vec<Condition>,
    effects: Vec<Effect>,
    undo: Vec<UndoProcedure>, // serializable inverse recipe
    kind: ActionKind,         // typed wire grouping only
    client_origin: ClientOrigin,
}
enum Stakeholder { Chunk(ChunkKey), Player(PlayerKey) }
struct PlayerKey(Uuid);
struct LivePlayerLease { key: PlayerKey, gid: GlobalPlayerId, incarnation: u64 }
enum ChunkPart { Block(BlockPos), Bounds(BlockBox), Entity(GlobalEntityId) }
enum Target {
    Chunk { key: ChunkKey, part: ChunkPart, inventory: Option<InventoryId> },
    Player { key: PlayerKey, inventory: Option<InventoryId> },
}
enum TruthView { Local, Ground }
```

The top-level stakeholder is **only Chunk or Player**. A chunk's entity, block, or bounding box is selected at the next level. Inventory selection occurs only beneath the actual player/entity/block/virtual owner. A typed virtual-inventory locator names its true Player or Chunk stakeholder. Construction validates parentage, so a block inventory cannot hide under a Player stakeholder. A player inventory and physical container require both stakeholders for a transfer. A live player's body is chunk-owned; controller and persistent inventory are player-owned. The UUID-stable PlayerKey handles offline state, while a live lease prevents a delayed action from mutating a reused GID slot.

`ActionProposal` preserves claimed input, live lease, tick/sequence, screen generation, targets, and typed client correlation: Java block sequence, Java click session/state ID, Bedrock stack request ID, Bedrock legacy transaction ID, or none. The owner captures exact block/actor/inventory/permission/revision before-state. A rejected proposal publishes local `ActionOutcome` for correction even though no Action was committed. That publication is never a mesh ACK. Java/Bedrock adapters pre-serialize correction packets after owner commit; egress failure disconnects the viewer rather than rolling back accepted truth. Mining start/abort decisions remain distinct from accepted Break.

## Atomic state primitives and exact undo

The interpreter supports bounded typed conditions/effects over block cells/bounds/block entities, living/combat/food/physics state, both ClientClaimedState and EffectiveControllerState plus filter provenance, visual/use/cooldown, entity spawn/despawn, item entities, statistics/advancements, delayed world effects, and semantic inventory operations. An inventory mutation includes all touched slots/cursor/ammo/remainder, exact before/after item identity and counts, full components with count-independent canonical identity, target revision, stack caps, and conservation. Item drop couples inventory consume with item-entity spawn; placement couples inventory and block. Health masking belongs to egress, not actions.

The Action contains its serializable undo *procedure*. Successful apply materializes exact versioned before/after writes in AppliedAction; producer-time literal inverse values can be stale after semantic rebase. Every effect has inverse coverage. Compute without mutation, then commit all writes. If a condition, conservation rule, target-parent rule, lease, or revision fails, commit none. Undo verifies the application identity/version before reversing, so it cannot overwrite a later dependent write.

Local and ground may have different predecessor versions. Each action declares predecessors and retains semantic operations rather than requiring one literal local-before value to match ground. At promotion, all predecessors have settled; the same interpreter materializes ground-specific writes or a deterministic failed-dependency result. A losing local action executes its stored undo. If a later write depends on it, the resolver computes a bounded dependency cone and emits targeted corrections or derived actions; it must **not** replay the pending action queue or regenerate local truth. This exact dependency-correction algorithm requires proof before implementation. Generic reapply of later actions is explicitly excluded by the plan.

Owner-validated movement and owner-issued entity/player chunk transitions are infallible action classes. A lossy movement datagram never gates holistic completion; each holder advances observed movement on superseding sequence or boundary snapshot. Reliable transitions are indexed under old/new chunks, apply last for their tick, take effect next tick, and remain in old-chunk departure index until older/same-tick references settle. After admission, infallible classes return Applied or invariant failure, never ordinary Rejected.

## Ownership, routing, and settlement

The tick engine partitions by full stakeholder sets. Single-chunk work owns its chunk and contained actors; joint work owns all participating chunks and player state. Disjoint categories run on Rayon with exclusive ownership, and owned results rejoin at phase boundary. Overlap follows deterministic conflict order `xxhash(cluster_seed, tick, actor_id)`, actor ID tie-breaker, then actor sequence. No gameplay lock, wait, shared-state atomic, or network response is involved. A, b, c converge after late delivery regardless of earlier provisional arrival order. Settled a,b,c can advance while lower-priority cross-chunk d remains unresolved only if d is proven unable to affect that subset.

The same action ID is indexed under **every** Chunk and Player stakeholder it touches. Player has a separately frozen holder/completion domain, including offline UUID mutations; it cannot be inferred from the actor's current chunk after movement. UUID moderation's Player-key holders are all currently connected peers. Joint ground promotion requires complete frozen-holder evidence and accepted ground state for every leg. A late snapshot imports unresolved refs as observer, never a retroactive voter. Chunk hold advertisement follows atomic install of ground blocks/block entities, player/block inventories, pending refs, and holder set.

Reliable one-way streams group actions by kind and route to relevant Chunk/Player holders. No individual mesh action verification answer or missing-verification kick exists. Holistic tick-finish lists each sender's actions per affected stakeholder, and proves omitted zero-action entries; it is an expectation manifest, **not** acceptance or resolution. Finish and actions may arrive in either order. With complete frozen-holder evidence, peers resolve independently. Cross-chunk dependency notices, primary resolution-owner selection when nobody holds all legs, and dropped-peer gap recovery remain the only extra communication exceptions. Global accepted-tick batches alone announce accepted truth.

The owner publishes local ActionOutcome for client correction and AppliedAction projection for controller/world output: effective pose, view/chunk delta, accepted admin state, and correction. Departure drain waits for that applied projection before final playerdata. These local events are not peer ACKs. Client-protocol acknowledgments such as Java mining sequence/Bedrock stack response derive from owner decisions, not from mesh verification.

## Control-plane boundary

UUID-targeted op/deop, ban/pardon, and playerdata policy use Player(UUID) stakeholders; issuer and target are included when issuer permission ground is read. `/kick` adds exact live lease side effect; `/spectate` jointly targets viewer Player and observed actor Chunk. Console issuer is authenticated metadata. Membership, presence, `/ban-ip`, whitelist, and cluster-wide health policy are infallible cluster-wide control messages. They are outside the action system and require no Chunk, Player, or fabricated Global stakeholder.

## Current-tree seam and fixture

`pumpkin-cluster/src/canonical_action.rs` has an envelope shape but permits Global, lacks parent validation/interpreter, and is not live-wired. `pumpkin/src/server/cluster_world_owner.rs::submit_action` only sends to a queue and has no call sites. Block, item, inventory, administration, and world modules run separate family pipelines; `actor.rs::ReplicaState` uses unvalidated apply/undo callbacks. Build the interpreter and owner adapter first, convert producers one family at a time to ActionInput, and remove each legacy direct-mutation route once that family's producer→transport→receiver→apply→client correction→ground promotion is live. Feature-only `/clustertest item-place` and `item-stale` setup must submit their world/inventory mutations through this same owner entry; their harness assertions can inspect applied outcomes but no private fixture mutation path.

Feedback incorporated: world/chunks required Player holder domain, all-leg promotion, late observers, semantic rebase, complete snapshot, transition index; item required typed paths, apply-time inverse, combat/projectile/statistic primitives; inventory required entity/block/virtual ownership, full conservation, item-drop coupling, no generic replay; ingress required proposals, typed client correlation, local corrections; controller required claimed/effective provenance, lease, movement outside completion, applied projection; global services required UUID Player completion domain and no fake stakeholder for control plane.
