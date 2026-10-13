# Operator messages (user-authored, spec-relevant)

## Interview Q&A (2026-09-17, Plan mode; answers recorded in tool outputs)

### Q01 wire_codec: Which zero-copy codec for inter-server streams/datagrams?
Answer: postcard + serde

### Q02 quic_stack: Which QUIC implementation and trust model?
Answer: quinn + self-signed/PKI

### Q03 roles: Should primary ever host players?
Answer: Disk-only primary

### Q04 chunk_wire: What should the chunk-data stream carry?
Answer: Serialized ChunkData snapshot
Note: current ground truth send via existing chunkdata snapshot, attached all updates the providing peers knows about rn as well as all peers that also hold the chunk. the requester can then get the local ground truth by replaying the attached updates

### Q05 clock_sync: How should secondaries discipline tick timestamps?
Answer: None of the above
Note: use online timeservers directly

### Q06 accept_delay: How long should a tick bucket wait before global-accept?
Answer: Wait for all-peer ACK
Note: while it does add a RTT its not a direct round trip, its more like each server has to accept the update. accepts are also send as a big batch once per tick. currently we also do not send updates concerning only chunks a peer does not hold, but they have have requested a chunk in the meantime. i would suggest that updates have addresses which is the chunk / entity they are affecting, an update can affect either one, two, or even possibly three chunks. i would suggest that instead acceptance is queued per chunk. when a peer aquired a chunk it also broadcast that is now hild a chunk the moment it requests it. so a new peer can be added to the peers needing to ack late so that could even delay it further. the good thing is that whoever they request the chunk from must have already been known at that chunk tick, so when each peer sends up an update on resolutions they also additionally attach the list of peers that got hold of the chunk before the resolution (and after the previous resolution for that tick): this was it can be known that the when every peer accepts a resolution signed of with a tick timestamp (that one is not a later tick given by that the resolution can only happen after updated arrive which arrive delayed) - so once every other peer we knew was holding the chunk has send a resolution that confirm that all peers they know off signed off we know no new peer could have aquired the chunk without also these resolved updates being attached to the send ground truth chunk

### Q07 stream_map: Confirm per-player uni-stream split visual / transient / world-mutate / combat + per-tick datagram?
Answer: Yes, lock 4+datagram
Note: 4 streams per player. 4 streams for all locally authoritatively managed entities. important for each update to always consider if it is possible at all to be fallible. one interaction we do not consider e.g. is an entity moving onto a block location so the block could no longer be placed, but at cross resolution time they already moved off it. (if they are still on it then the block placement instead gets rolled back, movement is always infallible - movement is checked by the server the player is connected to the server very well may reject player movement, but if the server accepted it, its accepted.

### Q08 entity_scope: Entity updates owner-ticked, no migration except on leave?
Answer: Owner-ticks, handoff on leave

### Q09 membership: Lock join/leave to unanimous-admit + drain-before-leave?
Answer: Yes, strict gating
Note: this is for servers. players joining however is an infallible action

## U001 (was U011) [2026-09-17T10:48:30.367Z]

currently pumpkin runs as a single process on a single computer. i would to change this to a decentral model. the focus here is only player actions i.e. we wont run mobs decentraly or tnt explosions. the idea is that all servers connect with each other via QUIC with max streams set to a high number. how there is one primary, the primary is responsible for saving / loading the world and playerdata. to make this work we use multiple unidirectional streams per player for events. first and most important is every tick we send a packet over the datagram (i.e. delivery not ensured) of the players position, head facing, and velocity. players on login get assigned a global id which is comprised of u16 server id and u16 player id i.e. this way no communication needs to be made to assign player ids - these ids are separate from any other ids currently used by pumpkin i.e. a player entity still gets a uuid - just for intra-server comms we use this separate player id. so the player update package on the datagram that is sent out once per tick is u16 len updated [PosUpdate] struct posupdate {newtype(server_id, server_player_id), perplayer_wrapping_increment_counter u16 newtype, pos, vel, facing  }. the per player we use a stream to send Updated about actions that update the player visual stuff (armor, hands, start/stop sneak/sprint, blocking start/end, swinging arm, changing skin layer) while some others are not a continuous update stream but only the action a player takes e.g. start eating, abort eating, (finish not needed that time+absense of abort), start/stop block break animation, then there are actions that affect the world: break block, place block (atomic together with updating the stack size in the inventory), and then on the next channel interactions hit player , hit other entity, fire box/crossbow. all of these should be encoded zero-copy e.g. with something like cbor (ium) but that support zero copy. the whole idea here is that each server has a local copy of everything, so it authoritatively resolves any player interaction (e.g. one secondary in in europe, another in japan, players connect to their local secondary and all get low ping) and accept the action optimistically. each per tick update block on a stream comes with a rounded to 1/20s timestamp (as u16 wrapping) this timestamp is calculated from realtime getting from time servers to offset the computer local time being wrong i.e. all secondaries are to have a sync clock. if there is a conflict like two players placing a block at the same position at the same timestamp a deterministic pseudorandom number is generated based on the timestamp and all involved players in the conflict. other conflicts are possible: for a block the be broken the player has to be breaking that block type exactly i.e. if the player A, B start breaking block dirt with B two ticks late, then A break it first and 1 tick later places wood, then the late breaking of player B is rejected as they tried breaking dirt not wood. (i realise regarding player action conflicts its probably better to have a function that based on tick -> random total ordering of all players i.e. the random win is decided by if hash(tick .. playerA) > hash(tick .. playerB) and on equals their server+player id break the ordering (biased by birthday problem already so unlikely that it will never happen). this way if a event conflicts between 3 players on different servers it does not matter in which order each server processes the conflict and conflicts can already be resolved in pairs. as for secondaries getting chunks, use 1 uni_dir stream to request a chunk for a peer and another stream stream for the chunk-data. all secondaries also communicate which peer has which chunk available (and also when they drop a chunk) - asking for chunk is done by asking the peer with the lowest peer that has it - if a peer get a request for a dropped chunk they just reply that they dont have it anymore. important: each secondary has two copies of a chunk: the ground truth and the local truth based on all updates applied. the local truth is not regenerated by replaying the update queue. instead if there is a conflict we preform only the undo operation of the local action, then apply the accepted resolution. once an update has been applied / accepted by all peers it is applied to the ground truth. to do this correctly updates are not just queued, they get into  bucket based on tick they happened in. a bucket holds a queue for not-globally-accepted updates, once a tick is marked globally resolved it means each peer got all updates resolved and thereby has updated its local queue with all the resolutions from others. s such they do agree now and contain nothing conflciting, so they are all applied at once to the local ground truth copy. important: there is nothing like an authorative primary. there is a primary responsible for saving load/data from disk (that one does not accept any players, instead it just gets the whole copy of an accepted tick). peers cannot join or leave on their own at any time. a peer can only start accepting players once all other peers have accepted them joining, and they can only leave if they host not players or entities. regrading entities: their logic for now is done always by the peer who spawned them. when asking to leave the network, such entities are transferred to other peers that hold the chunk, for now that is the only time that can happen. on the other servers the entity is just like players but all entities get a single stream by stream type, peers only get updates if they hold the chunk a player or entity that created or is effected by an update.   please go through the current source code, and plan this change in detail.

## U002 (was U012) [2026-09-17T11:18:22.572Z]

you need to properly investigate the code and plamn all changes that need to be made, so the changes can be impl in parallel (without git worktrees i.e. in the same folder)  by 19 subagents

## U003 (was U013) [2026-09-17T11:30:01.975Z]

you ommitted most everything of the prior plan losing that context. please also state all my messages verbatim with minimal curated context in the plan. as for updates they should be small focusses structs each focussing on a single update. when sending the update an array should be sent per struct so that the type doesnt need to be repeated over and over. also rn the plan doesnt say anything about efficient multi-threaded and async coding. i suggest doing the following: accum updates threadlocal during a tick, once a tick would end a new task runs that collects all threadlocal queues and fuses them. as to not block the next tick each thread has two banks, so while bank A accumulated, the next tick is already running using bank B. it doesnt change anything about how the current code sends or queues updates to players, those are likely done before and during the bank operations i.e. the player already got the server provide the answers to their actions during the tick or after (however it is done rn). important: we must never use locks for anything. we use the paradigm or mpsc and onehots. as for the bank the same applies. the bank switch happen by taking the threadlocal owning reference i.e. Box<X> and sending it to the consumer, replacing it with the owning reference that came back via a a channel with a queue allocation of fixed length 1. i.e. the bank switch task only ever would have to wait if accum the updates and sending them took longer than a tick (having to wait there ought to be logged (cooldown one log per minute) i.e. the hot path is without await)

## U004 (was U015) [2026-09-17T11:32:51.800Z]

please write the plan to a file. then and only after tht the task is to properly investigate the current server impl and the changes that need to be made, because your current plan just assumed that this split works without ever doing a proper investigating

## U005 (was U016) [2026-09-17T11:33:46.130Z]

also for quinn every server generated the certs locally. then the public cert need to be shared with the other peers via a third channel i.e. whoever is doing the setup

## U006 (was U045) [2026-09-17T12:20:41.039Z]

rule: code as docs. comments or docs strings are not allowed. code that requires them must be rewritten to be self-evident and fulfil code as docs

## U007 (was U049) [2026-09-17T12:22:09.620Z]

must not strip docs that already exist in the code upstream

## U008 (was U066) [2026-09-17T12:28:50.758Z]

no my goal ask ed for two processes. primary and secondary are seperate processes as by my spec!

## U009 (was U074) [2026-09-17T12:35:45.335Z]

spawn a subagent so that stuff like op/deop ban/unban are also globally synced and saved on the primary. also so that stuff like /spectate /kick work globally

## U010 (was U075) [2026-09-17T12:35:57.992Z]

spawn another suabgent to impl /invsee {player} command

## U011 (was U079) [2026-09-17T12:38:47.957Z]

invsee should also show the armor slots top left and offhand top right, and inbetween have the slots blocked by a light grey glass panel renamed so it has a display name that renders as an empty string. invsee also under a separate permission is to allow editing the inv. again this must work globally. both perms are included in op by default

## U012 (was U082) [2026-09-17T12:40:06.133Z]

oh we need to impl global chat including pm and tm also the player autocomplete is to be global

## U013 (was U106) [2026-09-17T12:59:37.190Z]

how often have i tell you that you forbidden from waiting with a timeout, or if you have not been setting it but there is default then set our timeout to 1h

## U014 (was U116) [2026-09-17T13:23:32.295Z]

why does primary even listen to a mc port?!

## U015 (was U118) [2026-09-17T13:23:52.846Z]

while also hogging the default port delegating the real server to another

## U016 (was U124) [2026-09-17T13:34:39.288Z]

i mean it looks like the "secondaries" are generating chucks, which ought to be 100% impossible

## U017 (was U132) [2026-09-17T13:36:33.263Z]

secondaries ought not even have a generated world or save any chunks or playerdata to disk

## U018 (was U368) [2026-09-18T04:46:06.262Z]

as i was saying btw its forbidden to use any locks or waiting code that holds up

## U019 (was U372) [2026-09-18T04:51:21.742Z]

this should not mirror the chat pattern. it ought to mirror the player wiring

## U020 (was U378) [2026-09-18T04:57:43.725Z]

spawn a suabgent to impl a new command /syncstats optional all|(chunk optional chunk coordinate 2d)  by default its all, for chunk by default its the chunk the player is in. it should lists stats about how many actions exist for a chunk / overall - and how many ticks behind the ground truth is

## U021 (was U380) [2026-09-18T04:58:20.855Z]

is using public external timeservers in order to get accurate time for all peers imple as by the spec? soawn a subagent to audit and fix all missing stuff and wireing

## U022 (was U389) [2026-09-18T05:12:36.032Z]

please give the command to look at the tmux of the primary and node1. once euler is done their next job would be to impl a system to mark certain regions of a world as always to be synced, where the spawn chunk are automatically registered as such a region.

## U023 (was U391) [2026-09-18T05:14:45.802Z]

"16:17:14  INFO cluster admin seed grant applied locally server_id=0 name="sirati97" level=4"  says the name, that suggest to me that ops are synced by name string not by player UUID as they must. also on the primary i do not see any players joining via a secondary

## U024 (was U393) [2026-09-18T05:19:42.174Z]

node1 spams chunk fetch missed. how is that even possible?! my spec makes clear how chunk fetching works: one directional request to peer that holds the chunk. i do not see how it can claim a chunk fetch missed that would require an answer and i dont see how that spec compliant. indeed it is required that a peer who no longer holds the chunk to tell the other node that they dont hold the chunk anymore but that is not by a chunk fetch answer, its by the normal broadcast about chunks aquired and dropped independent of any fetching. much worse my locking / waiting rule has been violated. both player clients have long disconnected yet node 1 claims they are online. that can only mean that these players as blocked on waiting for the chunks as thus no task can aquire the player handle stopping the server from processing the disconnect. blatent violation of my rule!!!!

## U025 (was U395) [2026-09-18T05:27:34.181Z]

please spawn a new agent that implements a lobby wait room, it answers a loggin in player with that they are in spectator mode, spectating a third entity (so that they cannot move) and that they are at a a very high coordinate like 1m 1m standing 1 block above end portal looking straight down. the chat tells them that their data is laoding, with another message after 5 seconds since the last message, or when the required loaded chunks proceeded by 10%. at most 1 message per second, so if all chunks load within 1 seconds after the "Your data is loading" message, then they do not get an % update messages. if all load in 1 seconds after the 10% message similarily they dont see the other % messages. loaded chunks are directly sent to the client (even though wrong location they keep the chunks for some time), so once all chunks are loaded by the server, and all are sent to the client, we just need to teleport them, send the inv, fix gamemode and other player data, and they get an instant load

## U026 (was U397) [2026-09-18T05:28:40.262Z]

the important is that all of this is never kept on the server, we just sent the client fake data the server doesnt know and doesnt track

## U027 (was U399) [2026-09-18T05:30:24.177Z]

ohh instead of chat messages, we can use the minecraft client features that displays a message on their screen i think its called title message or something like this. then we do not need to throttle at all. we can give live updated once per tick, so the chat part doesnt apply

## U028 (was U401) [2026-09-18T05:31:01.818Z]

so the title can be Loading... and the subtitle the percentage

## U029 (was U402) [2026-09-18T05:32:14.032Z]

btw so far the motd also didnt show the synced player count, that needs to be fixed (new subagent)

## U030 (was U405) [2026-09-18T05:32:34.554Z]

rn it looks like void damage is applied by all peers not only the peer owning the entity (new subagent to fix)

## U031 (was U407) [2026-09-18T05:35:46.214Z]

new subagent: we need specialised packets for updates to chunks due to random tick updates, changes due to redstone (calculated as a fallible (i.e. revertable) update by the server holding the entity triggering), and block changes due to explosions again as an update (fallible - not the explosion, but e.g. an intermittently placed obsidian changes the explosion).

## U032 (was U409) [2026-09-18T05:40:54.645Z]

another new subagent: player interactions like opening/closing a door/dropdoor, placing endereye in end frame, respawn anchor adding glowstone,  need specialised update packages. stuff like a comparator or observer triggering are delayed dependent updates to have such a dependency is a new thing we need to model

## U033 (was U411) [2026-09-18T05:44:19.277Z]

new subagent: we already have player inventory updates (btw these must be semantic updated like x was moved from y to cursor, or x was moved from inv a slot b half stack to inv c slot d so that they can be replayed even on conflict. they must not be undates just matching the player actions the client sends. this is also to ensure that we cannot accidentally dublicate items. that also means item use / block place has a dependency on the inv content i.e. another way to be fallible. the /invsee must properly use this API

## U034 (was U413) [2026-09-18T05:44:44.103Z]

btw random ticks only are done on the primary never on secondaries

## U035 (was U416) [2026-09-18T05:47:44.284Z]

also please tell agents again, they must strictly adhere to the spec for all changes they are still supposed to do. that included that updates are always send over onedirectional channels (i.e. never confirmed) updates instead are only confirmed by the global update type inspecific of accepting updates and mutating the ground truth. further waiting for something is always forbidden, locks are completely banned

## U036 (was U419) [2026-09-18T05:48:31.343Z]

not only binding for remaining work. binding for all work. violation are not allowed you idiot

## U037 (was U429) [2026-09-18T06:00:18.758Z]

just to make the chunk thing. its clear right by the spec on how it works? a peer broadcasts that it holds a chunk AFTER it received it. and broadcasts when dropped it. as such any server waiting for chunk just requests it a single time, and doesnt do it again by marking from which peer it requested and when. now it will either get the chunk from that peer OR it will get the message that the peer dropped it, in that case it can request it again from another peer. if no peer holds it the chunk gets requested from the primary. so there is no ambiguity at all, and no need for timeouts or automatic retried either

## U038 (was U432) [2026-09-18T06:01:46.798Z]

fetch from lowest ping holder was what i specified.... ,  and no negative reply is wrong. the spec ought to include my verbatim text does it not?  also did you ever update the spec fiel to include my

## U039 [2026-09-18T06:31:00Z]

i think another important thing i believe is that for clients its important to know what players are online and this is partially separate from the player list shown with {tap}. this needs to be kept consistent between peers, but players should only get to see what they need. e.g. add a /hide command, ops have that permission by default, when doing /hide the player entity is removed from the players send to to clients as online, removed from the player tap list, removed from the modt player list, removed from the player count, and a player logged out message is broadcasted (btw these also need to be cross peer!) doing /hide again undoes that. its very important that the player is removed from the list of online players (i think entity list knows by client) - as hacked client use that detect /hide moderation functionality

## U040 [2026-09-18T09:00:00Z]

"ntp_servers = ["pool.ntp.org"] max_offset_millis = 250" we generally want to have a max offset of 1/40 a second i.e. half a tick. - importantly we want to use locally close timeservers at each node, so that they share a common time, trusting that public time providers can give that to use. ik. that means that communication always has stuff arriving late, but sharing the same clock has advantages like saying in the wrapping u16 tick=0 was at time X so even thouhg this package arrives later the other peer then knows what tick it is at this exact time

## U041 lobby wait room corrections

Lobby player is not logged in and has no player entity yet: /gamemode, /kill and any entity-requiring command must not work; chat and entity-free commands (e.g. /tp {other} pos, not /tp pos) do. Must see the end portal via the spectated third entity at 1m 1m looking down; server sends portal chunk plus 5x5 air-filled ring. Title updates once per tick with 1s timeout, Done on finish. All lobby state is fake client-side data, never stored server-side.

## U042 lobby time freeze

Lobby world time is frozen; world settings including time update to the real world only on real join after loading.

## U043 lobby-only chunks on login

Login sends only lobby chunks in a single packet with all lobby MC packets, never real-location chunks, so no loading-terrain delay.

## U044 lobby height

Lobby spectate position is head exactly 1 block above the end portal, not height 482.

lets say an action depends on two entities but one peer only holds one of them. then it never knows how to settle the other side of the interaction. in this case it should message holders of both the current and the other entity's chunk to mark that it is waiting on the resolution of the side(s) it doesnt hold. if a peer settles both sides of the interaction i.e. applies them to the ground truth it messages the dependences on the settlement of those actions. if no peer holds both chunks the primaries who hold legs are messaged (this can only happen if there are multiple primaries otherwise the primary would be holding all of them always), then the primaries decide between themselves which ones gets the resolution task based on load, then it requests the chunks from primaries and loads them, now having both sides and all updates it can resolve and message the waiting peers, such a chunk keeps being loaded for 5 seconds after the transaction was settled. btw chunks with unsettled transactions can only be unloaded by secondaries to insure that is always at least a primary who keeps the whole transaction log so that no actions or lost. this works because each peer always knows for all other peers which chunk they hold without extra communication as defined by the current spec.

## U045 entity chunk transitions

btw given that entities move there is another type of infallible action, and that is an entity transitioning from one chunk to another, but this one goes over the one direct channel for entities (maybe even player entities are included here). this action is special because it has the possibility of changing the chunk associated with an entity, and another peer may have already issued actions for the same or a future tick . this must be handled gracefully. this requires that until this action is applied to the ground truth it must be kept in a special list ofr the chunk the entity left, so that any actions referring to the old chunk can be updated to the new chunk without communication. for this work entity chunk change actions must always be applies after all actions of the same tick. i.e the action only appies the change starting with the next tick. as each entity is controlled exactly by one peer there never can be conflicting entity chunk change actions

## U046 [2026-09-23T17:19:28Z]

"The canonical settlement path now promotes one action at a time and leaves failed actions pending." this is a spec violation. spec does not allow for failed actions to remain pending. please let them read the step for proper treatment. no action can ever fail. either it is accepted as is, it is replayed on a changed state that matches the actions intend, or it is rolled back as if it never happened

## U047 [2026-09-23T18:21:28Z]

rejected. not my message. you did not collate you came up with random stuff. also please tell the agent that they must have actions be applied at two locations.  both the ground truth and local truth. for the local truth an action coming in at the current tick is checked for conflicts within the current tick and if there are resolved via the specs hash alg. if an action is undone player clients get the information to undo what they see. if an action arrived for a previous tick that tick has to searched for conflicting actions and resolved any action that would be undone, or replayed differently itself then has to looked for dependent. the important info here is that we do not have any chunk/entity information at all for that tick at this search step, but we dont need to because we know a) the action was valid on the peer that issued it, and b) all actions already here have been made valid. as actions save their conditions and effect, we know for a fact that and condition must be satisfied locally unless a same tick action changed that location. in order to be able to solve this efficiently, per tick, and then separately per chunk, entity and inventory a directed graph is build during resolution, with all locations (i.e. block location, inv slots, entity properties) of either conditions or effects being the entry points into this graph, importantly conditions and effects are a different list as only effect and conditions can conflict, not conditions with conditions or effect with effects (yes this means place block has a condition and effect on the same location). the action contains the result of the current resolution. so to add the new actions we just look at the location of the opposite type and if we match then we have an entry point of a potential conflict to resolve - the condition could still be satisfied by the effects we fould at the solution. with other words these are a multiset. if no locations matches or any of the potential conflicts turns out to be orthogonal then we keep this action in the pool for the resolution with the tick after this one, as well as to the delta pool, and add it to the graph and add the entry points. if we do conflict everything is resolved by the hash spec, if that results in an action being undone the action is replaced with an undoaction wrapping it, and the undo action core still was in the graph so we can just look what its edges go to, to find all actions that depended on its effect so we can directly continue with resolution (yes that means an actions undo normally doesnt change locations, if it does change locations as more commonly for replays then we have to start form the roots again), other actions on failure are instead replayed the same applies but it more likely that the replay changes locations e.g. shift inv move changing the slot location(s) where the shifted itemstack lands. importantly for any replay or undo we also have to add it to the pool for the next tick, as well as the delta pool. if an action also has effects that changed effecting another realm e.g. another chunk /entity/inv then we need to add the replay/undo to the realm actions that need to be resolved. next we finish all resolution for the current tick, i.e. if we have undo/replay that cover multiple realms e.g. mutiple chunks  we also have to now do the joint resoltion, (an undone action gets effectly removed but it shold not be removed from the graph, because undos are also themself undoable i.e. a redo which can happen if an action taking priority causing our undo is itself undone). once we finished all of those we proceed to the next tick.  here we now have the pool of actions that caused this resolution cascade but also all ations that got undo/redo/replayed due to it. we now need to do the whole process again for these actions, with the difference that these actions do not get integrated into the graph, and they always win (they happened earlier in time). this proceeds till we are back at the current tick. now we have collected the delta pool. (remember so far we have not changed the world at all, we only operated semantically on actions deciding which ones get undo/redo/replay or for new actions applied/not-applied (the second is not ignored, it just means its an undoable action and it got undone, but as its new it never had an affect so its just not applied). we also have the local truth. now we need to take the pool and starting at the current tick applies the deltas i.e. replay means undoing the old effect applying the new effect, sending it out to clients, and undo/redo/apply just means doing that and sending it to clients. then we go to previous tick back in time and do the same, till the whole pool is done. now we have updated our local truth and also the clients as they see the local truth via our send out deltas. the other one is the gorund truth. here everything is a lot a lot easier. once we have finished doing this here, we just check if for each chunk/entity we got from all peers the number of actions as they posted they sent out that tick. if any are missing we do nothing and we are done. if none are missing we mark that chunk/entity/inv for that tick as done. after we finished marking we go over all done chunks/entity/inv now we take our already built action graph and apply each action which does not depend on any action which depends on a different chunk/entity/inv not marked done.  i.e. if for an action all parts are marked done we apply it to the ground truth. now something very very important comes up: earlier we had replay actions that could change which location they apply to on a replay. these have to be determinstic and finitely many locations. because besided for our two entry multisets to the graph we actually have a third, and whenever we add an action to the graph we also need to consider theentry positions for wiring dependencies. a replayable action that may change position when it gets added needs to mark all locations it could ever replay to i.e. have an effect on, or depend on i.e. depend on depnding on the effect it ends up having. e.g. for an inv this could be the whole inv e.g. shift op. during our earlier resolution we didnt need to consider these as they are counterfactuals, but we cannot commit any action to the ground truth that depnds on a location that was marked by a replayable action as that it could possibly conflict. so in total we actually have 4 multisets 2 for the factuals and 2 for the counter factuals. to proceed if we have a replayable action that gets applied to the ground truth as usual we remove it from the graph and all sets i.e it gets dropped its done after all. but as this frees up the counter factual pools more and more actions become applyable and the ticks graphs drain. if an action would be applyable but there is a counterfactual, it checks if the counterfactual action has all its depdendcies done, because even though it did not apply yet its just after us in order, so we can apply (all its realm are marked done so no new actions can arrive as such it could never be triggered to replay). this must ebe precisely impl like this no differnt. any other approach will produce wrong results. actually it must be impl performantly, so for all of this you must consider how to avoid having to check everything i.e. the graph. we also dont want to do this procedure multiple times. so we only do it once per tick with all till then collected data from other peers. please take this and the last message and quote it to the human messages, also add both verbatim to both plans without any edits

## U048 [2026-09-24T08:58:22Z]

okay here is a proper design that solves this easily. when requesting a chunk from a peer the peer sends its ground truth chunk, and all graphs it has at that very moment including all ticks done messages. by that the peer now knows from which peers for all ticks up to the tick it was sent at. now the only issue is the following that needs to be solved: during transmission of the chunk and till the receiving peer broadcasts that it now holds the chunk time will pass. during this time new ticks will happen and updates sent out, and the new holder does not get these because it is not known to others that. i suggest the following resolution: a) split up being a chunk holder and being  a subscriber to updates. so when a peer wants a chunk it sends one peer the holds the chunk the request for the chunk, and to all others a list of all chunks it starts being a subscriber (including the current tick + 1) for (one-directional no ack). when a peer broadcasts that is drops a chunk that also unsubscribes it. if a peer gets actions for a chunk it is not subscribed to it will once per chunk send an unsubscribe notice only to the peer that gave it actions for the chunk if it didnt send one our for that chunk the last 2 seconds). this does not solve our problem however unless we change it so that the chunk provider only sends out its graph up until the tick before the requested tick, and all other peers send their actions for starting including the requested tick if they hold the chunk. this mostly solves the gap, besides for those actions the peer who sent the chunk didnt have yet. but this can also be solved: using a special subscription relay status the peer will relay all actions / tick done packets it gets from the moment after it sent the chunk. this still has a problem if we sent chunks in parallel to processing actions. we must not do that. so incomming actions are not immidiately resolved and added to the graph (they should not anyway for performance reasons) isntead at one phase per tick we send out the chunks, and in a phase before that we process the actions we accumulated since last time. with other words the moment we starting resolving actions all new arrivals are collecting only for the next tick.  so we can never lose actions here. so now the peer can just relay and this insures the peer gets all updates as long as the relaying peer does not drop in the meantime. another issue is that other peers will have send out actions in the meantime so they must be able to identify the actions they send out. lets resolve this as following: so far we only have ground truth <-> local peer truth. we can solve this by going one step further we have an implied actor-local truth. this applies to both local entities and players. when we process an entity AI in order to make it do somethign they get a only a readonly ref to chunks. if they interact with anything this must happen through actions. in order for the code to be able to see a local view of the world and see its changes directly apply we just save whatever it did via diffs. if it changes any block/blockstate we just have hashmap of location and new blockstate, if it does anything to an entity we do with via CoW, if any property of an entity or tileentity is changed we just copy the whole entity and apply the change. so when the actor requests an (tile)entity we give it a CoW wrapping  RO ref if it did not modify the entity yet, or the local copy. as we issue actions for all changes, we can and must drop these local diff and copied entities the moment the entity finishes processing for the tick (i.e. all processing of an actor on an entity must happen directly after another in the same scope.), during this we also already have crated the actions and queued them for sending just as we did so far, but we no longer send the client packets like as we would before so no optimistic view yet. the only difference now is any action queued for sending will also be collected for processing in the local tick to the local and ground truth, just as if we received these from another peer - no difference. given whenever we apply any action we need to send the locally connected players the updates, this is why we cannot do the optimistic update, earlier otherwise we would have to treat these different. as we treat them just like actions that came from a different peer, this now gives us the same behaviour these a new actions so our local players do not know about them yet, so for these there is nothing to undo yet, and if we now accept the action we do need to send the update. so everything just works out. and this gives us something amazing: we can not complete remove the locks from the chunk manager. we have four phases when we only consider chunk management. 1. adding/dropping chunks if they finished generation or loading on a primary or got received by a peer, or processing the list of chunks to mark as requested. (yes this means entities cannot interact with chunks that are not available yet, and as we do not wait during actor/block update propagation ever - but this is already solved by minecrafts two rings of chunks on the outside: first no entity processing, then no update processing, and queing the update) so when we need a chunk that we dont have for block updates we just request it, and queue to update together with the tick, so when the chunk arrives, we can just as normal finish all of these retrospectively for the tick they happened in - the only case where this does not work and we accept that if we have a block update cascade e.g. redstone tricks for zero-trick transmission and the needed chunk does something that propergates back into the loaded world and we in the mean time declared the chunk as done and applied ground truth - this will nearly never happen tho, because redstone is done by the primary, and it does have the world locally so it can load the chunks likely fast enough for this to never happen, given that a chunk can only get done if the remote peer also processed it)  2. processing local actors they all get only a readonly view as specified now, no locks required 3. resolving actionson the graphs and updating local truth 4. done marking and applying actions to ground truth   given that we process actins based on the chunk they require with rayon, no locks are required anymore. with other words this enabled us to have 100% parallelism in entity and player actor processing. cool stuff. there still is one failure case we need to resolve: re requested a chunk and relaying peer drops before we get all actions and tick done envelopes. in that case there is nothing we can do. the only way to solve this is to send out another new package to all holders of the chunk and ask them to send the ground truth they currently have IFF it is after the tick for which we miss actions i.e. the tick we requested the chunk earlier. all peers broadcast the answer to all holders but the requestor saying what tick has finished completely processing for their ground truth of that chunk rn. after the receive that from all holders or a timeout of 1s, they use the hash resolution to decide locally without communication which peer sends the ground truth to the requester. newest ground truth takes preference, hash used for deciding between ties. if none have the ground truth ready for that tick yet the winner waits till it gets the ground truth to finishf or that tick. then it sends to the reqester. if we are waiting due to winning with a timeout and during the wait more come in and this makes us lose, then we accept this as a loss. (i.e. if we have network problems with one-way taking longer than 1 seconds then we may have multiple peers sending the ground truth, that is okay and a tradeoff we need to make to bound the time this process takes) of course peers that have the same problem instead answer not with a tick they have, but that they cannot participate at all. there updates send only the ground truth and the tick for which the ground truth it is, as well the graphs for all marked done ticks, not the graph after those. the receiving peer now drops the old ground truth and soon but not yet also  graphs for the marked done ticks. then it rebuilds the local truth by replaying all actions including the marked done graph (marked done graphs are frozen but there may still be cross chunk actons that needs resolving). this requires a special procedure for the marked done graphs, as the accepted action may disagree with the graph we will soon discard, to solve this we first create a mapping between the new and old graph (they are not the same size as applied to ground truth actions get deleted so far as by the spexc. actually this makes me realise we should not delete actions when applying them to the ground truth but only when for a tick all actions have been applied to the ground truth, so this overriddes the previous instruction, now instead of dropping immidiately they are instead marked as applied_to_ground_truth), this now solves the problem: the received marked done tick now contains the graph that strictly contains all actions contained in the graph we will soon discard.  by having a mapping between the new graph and the to be dropped graph, we can now find conflicts. the new graph wins and we do not even need to progagate locally so we can just accumulate the actions that changed resolution (delta pool) for cross chuck/entity actions. this delta we can then start with at the next tick for the other chunk/entity in step 3. of these actions, only those marked as applied_to_ground_truth in the received done graphs will get the always_wins attribute i.e. in the resolution for the other chunk we dont use the hash resolution but always let this action win. this works because while the other side in this rare case has not yet settled, we know by receiving this from the other client, that eventually it will settle into the state the always_win_action. (such always win actions may still need to replay the effect in the not-marked-done if a condition there changes for correct local-truth, but once marked they will always win, but replay still happens, just not in the chunks we received for this rare drop-resolution. after we replayed the graph of done, and the not dropped graphs after that, we have the new local truth, and because we compare the graphs and find the delta, we can even do the local delta procedure to modifying the local truth from the wrong state to the correct state. (please quote this message verbatim into both plan docs, and the human message md file)
