# World-sync bug record

This records the observed failures from the multi-node test so they can be
reproduced and tracked without relying on chat history.

## Observed symptoms

### Block/world changes do not replicate

Changes made to the world by a player on one node do not become visible to a
player connected to another node.  This includes changes made after both
players have left the lobby and entered the game world.

Expected behaviour: nodes present one shared world.  A world change must be
replicated to the relevant peers and then to their local clients.

Status: unresolved.  No root cause has been established in this record.

### Players on different nodes are invisible to each other

Two players in the game world, each connected to a different node, cannot see
each other.  Related cross-node operations can resolve a player's name but
cannot find that player's local entity, for example `/tp <remote-player>`
returns `No entity was found`.

Expected behaviour: player presence and the entity state needed for visibility
and cross-node interaction are replicated to peers.  A command must distinguish
between a player who is online remotely and an entity that is genuinely absent.

Status: unresolved.  Name completion proves some peer player information is
available, but does not establish that entity replication is working.

### Mob populations diverge; secondary nodes appear to spawn mobs locally

The mobs visible to players on different nodes are different, which indicates
that entity state is not being shared correctly.  The observed result is also
consistent with natural mob spawning occurring independently on secondary
nodes; that still needs confirmation from code and logs.

Expected behaviour: natural mob spawning is primary-only.  An entity is
controlled by the server that spawned it, and its state is replicated to peers
so every node presents the same entities.

Status: unresolved.  The apparent local secondary spawning is a hypothesis
from the test result, not yet a confirmed root cause.

## Scope and follow-up

These failures are separate from local lobby handoff.  They affect the shared
game world after players have entered it.  Investigation should trace the
outbound and inbound paths for block mutations, player/entity spawn and state
updates, despawns, and ownership routing, with primary-only natural spawning
enforced explicitly.
