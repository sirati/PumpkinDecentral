# Lobby send-world research (Dalton, 2026-09-18)

## Our burst (`crates/pumpkin/src/server/cluster_lobby.rs`)
Order: `CCenterChunk(62500,62500)` -> `CPlayerPosition(id 0, 1000000.5/100.38/1000000.5, yaw 0 pitch 90)` -> optional `CChunkBatchStart` -> 25x `CChunkData(ChunkData::empty)` 5x5 r=2 -> `CChunkBatchEnd(25)` -> 25x `CBlockUpdate(END_PORTAL y=100)` -> `CSpawnEntity(MARKER -1000001)` -> `GameEvent::ChangeGameMode(Spectator)` -> `CSetCamera(marker)` -> frozen `CUpdateTime` -> `Loading` title.
Confirmed: no `GameEvent::StartWaitingChunks` anywhere in lobby path. Normal join sends it at `crates/pumpkin/src/world/mod.rs:4079,4256`, explicitly skipped when `in_lobby`.

## Reference send-world (code read, not theorized)
- `Nan1t/NanoLimbo` `ClientConnection.spawnPlayer`: `JoinGame` -> `Abilities` -> `PlayerPosAndLook(0,400,0)` -> `SpawnPosition` -> `PlayerInfo`/`DeclareCommands`/brand/bossbar -> only for `>=1.20.3`: `START_WAITING_CHUNKS(type 13)` then 3x3 chunks at `0,0`. No center packet, no batch framing, no gamemode/camera/time/title/entity/block-updates in the pre-chunk window. Position coords match chunk origin; small coords.
- `Nan1t/NanoLimbo` `PacketSnapshots`: `For 1.19 we need to spawn player outside the world to avoid stuck in terrain loading`, hence `y=400` void spawn.
- `LOOHP/Limbo` `ClientConnection`: `JoinGame` -> `GameStateChange LEVEL_CHUNKS_LOAD_START` (event 13) before `SpawnPosition` + `PositionAndLook`, chunks streamed after. Same shape: event before world bytes, position matches world origin.
- `Nan1t/NanoLimbo` `PacketChunkWithLight.encode`: heightmaps always `Map.of(MOTION_BLOCKING, new long[37])` even when empty; blocks `writeShort(0)` + single palettes + zero storage; block entities `0`; light `sky=null, block=null, emptySky=null, emptyBlock=all(sections+2 bits set)`, zero arrays.

## Diff, prime suspect: chunk content validity (matches observed 4/25 accepted)
- `ChunkData::empty` (`crates/pumpkin-world/src/chunk/mod.rs:602`): sections air 24x `min_y -64`, `ChunkHeightmaps::default` = all `None`, `ChunkLight::default` = empty `Box<[]>` (len 0), `light_populated=false`.
- Heightmaps: our `v1_18::write_chunk_data` sends empty NBT compound (zero entries) on `<1.21.5` when all `None`; reference always sends at least `MOTION_BLOCKING[37]`.
- Light: our `light_data_from_chunk` `>=1.18` branch with `num_sections=0` sets only bit 0 + bit 1 empty masks, zero arrays; masks cover 2 positions while block payload carries 24 sections. Reference sets `sections+2` empty-block bits so mask length matches section count. Mask/section-count mismatch is the strongest accept/reject candidate.
- Sections/biomes/block-entities match reference (single-value air palette, zero storage, 0 entities).

## Diff, order hazards
- `StartWaitingChunks` placement: references put event 13 before chunks; ours never sends it. On 1.20.2+ this gates the receiving screen plus batch-ack accounting.
- Position matches chunk origin with small coords in all references; ours uses far coords (62500/1M). No reference uses far coords.
- Early packets references never send pre-exit: block updates to not-yet-accepted chunks, marker spawn, spectator mode, camera, frozen time, titles. Spectator plus camera lock stay per operator ruling; keep them out of the chunk-acceptance window if possible.

## Correct send-world (scope: only how to SEND world)
Keep JoinGame/exit/teleport design untouched. Copy only: position matching chunk origin -> spawn position matching -> StartWaitingChunks before any chunk -> SetCenterChunk(origin) -> ChunkBatchStart -> chunks each with >=1 `MOTION_BLOCKING[37]` heightmap + light empty-masks covering `sections+2` + single-palette air sections, small `0,0`-style coords -> ChunkBatchEnd(count) -> KeepAlive.
