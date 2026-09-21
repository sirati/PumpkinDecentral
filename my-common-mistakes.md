# My common mistakes

1. Reporting a finding instead of fixing it. Identifying the problem is not
   the job. The job is the fix landing. No status without an accompanying
   action.
2. Seeing the independent path and not starting it. When one side does not
   depend on the other (laptop builds its final stage remotely), the
   independent work starts immediately instead of waiting on the local path.

## Concrete examples (2026-09-18)

1. Pasted chunk-log analysis (379 replace warnings, channel-full) as a
   report while the old binary still ran, instead of driving the
   build-plus-restart that was the only way any of it goes live.
2. Knew the laptop builds its final stage remotely and does not depend on
   the local link, yet waited on the local build and only started the
   remote sync after being ordered to.
3. Running nix builds detached. A detached build never notifies on completion, so completion is missed and a second build gets started while the first still holds the lock. Builds run only as bare foreground `nix build`, one at a time.

## When to do it myself vs delegate
Small, well-understood edits with all context already in hand (e.g. a flake.nix change after reading the derivation) go faster done directly: no briefing cost, no verification round-trip, no waiting on an agent queue. Delegate only when the task is large, parallelizable, or needs exploration I have not done. A subagent for a 10-line change I already understand is pure overhead.
