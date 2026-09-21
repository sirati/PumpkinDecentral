//! Secondary no-generate / no-persist guard.
//!
//! Spec: `docs/cluster-plan-v2.md` §2.4 (`GENERATION OR PERSISTENCE ON
//! SECONDARIES`): secondaries must be physically incapable of generating a
//! chunk or writing chunks/playerdata to disk; the code path must not exist,
//! not merely be avoided. Secondaries hold a local copy only and fetch every
//! chunk from the holding peer (primary snapshot / holder fetch path).
//!
//! Audit of `pumpkin-world` (read-only, no changes there):
//!
//! - `crates/pumpkin-world/src/chunk/io/file_manager.rs` (`save_chunks`):
//!   returns `Ok(())` immediately when `level::is_cluster_secondary()` is set,
//!   so the region backend (`Linear`/`Anvil`/`Pump`) is unreachable.
//! - `crates/pumpkin-world/src/level.rs`: skips `region`/`entities`/`poi`
//!   directory creation on secondaries; `read_chunk_sync` returns an empty
//!   chunk plus `request_chunk_fetch`; `load_single_entity_chunk` returns
//!   `ChunkNotExist`; `receive_entity_chunks` reports `Missing`;
//!   `write_chunks`/`write_entity_chunks` clear dirty flags and discard;
//!   `request_chunk_fetch` routes through `CLUSTER_FETCH_SENDER` only.
//! - `crates/pumpkin-world/src/chunk_system/schedule.rs`: `generation_pool`
//!   is `None` on secondaries; `secondary_must_stay_parked` keeps generation
//!   nodes parked; `secondary_dispatch` parks the node and calls
//!   `secondary_request_fetch`; `secondary_poll_fetches` completes holders via
//!   `cluster_ingest_snapshot`; `save_all_chunk`/`save` early-return.
//! - `crates/pumpkin-cluster/src/primary.rs`: disk-only primary owns
//!   persistence plus snapshots and hosts no players.
//!
//! This module is the cluster-side counterpart: a tiny policy kernel that
//! secondary chunk plumbing must consult before generating or persisting. It
//! has no I/O and no dependency on `pumpkin-world`, so the rule stays
//! enforceable without wiring world I/O into the cluster crate.

use std::error::Error;
use std::fmt::{self, Display, Formatter};

use serde::{Deserialize, Serialize};

/// Which process a chunk operation runs on.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub enum NodeRole {
    /// Disk owner. The only role allowed to generate or persist.
    Primary,
    /// Diskless follower. Must fetch every chunk from a holder.
    Secondary,
}

/// Chunk operation subject to the no-generate / no-persist rule.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub enum SecondaryChunkOp {
    /// Build chunk content locally (terrain, features, lighting).
    Generate {
        /// Chunk X coordinate.
        chunk_x: i32,
        /// Chunk Z coordinate.
        chunk_z: i32,
    },
    /// Write region chunks to disk.
    PersistChunks {
        /// Number of chunks the caller tried to persist.
        count: usize,
    },
    /// Write playerdata blobs to disk.
    PersistPlayerdata {
        /// Number of player blobs the caller tried to persist.
        count: usize,
    },
    /// Fetch chunk bytes from the holding peer / primary snapshot path.
    Fetch {
        /// Chunk X coordinate.
        chunk_x: i32,
        /// Chunk Z coordinate.
        chunk_z: i32,
    },
}

impl SecondaryChunkOp {
    /// Reports whether this operation builds chunk content locally.
    #[must_use]
    pub const fn is_generate(self) -> bool {
        matches!(self, Self::Generate { .. })
    }

    /// Reports whether this operation writes chunks or playerdata to disk.
    #[must_use]
    pub const fn is_persist(self) -> bool {
        matches!(
            self,
            Self::PersistChunks { .. } | Self::PersistPlayerdata { .. }
        )
    }

    /// Reports whether this operation is the single legal secondary path.
    #[must_use]
    pub const fn is_fetch(self) -> bool {
        matches!(self, Self::Fetch { .. })
    }
}

/// Rejection returned when a secondary attempts to generate or persist.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct NoGenerateViolation {
    /// Operation that was refused.
    pub op: SecondaryChunkOp,
}

impl NoGenerateViolation {
    /// Builds the rejection for a refused operation.
    #[must_use]
    pub const fn new(op: SecondaryChunkOp) -> Self {
        Self { op }
    }

    /// Human-readable reason pointing at the fetch path.
    #[must_use]
    pub const fn reason(self) -> &'static str {
        match self.op {
            SecondaryChunkOp::Generate { .. } => {
                "cluster secondary must not generate chunks; fetch from the holding peer"
            }
            SecondaryChunkOp::PersistChunks { .. }
            | SecondaryChunkOp::PersistPlayerdata { .. } => {
                "cluster secondary must not persist to disk; the primary owns saves"
            }
            SecondaryChunkOp::Fetch { .. } => "fetch is allowed",
        }
    }
}

impl Display for NoGenerateViolation {
    fn fmt(&self, formatter: &mut Formatter<'_>) -> fmt::Result {
        write!(formatter, "secondary chunk incapability: {}", self.reason())
    }
}

impl Error for NoGenerateViolation {}

/// Compile-time policy: secondaries never generate chunks.
pub const SECONDARY_MAY_GENERATE: bool = false;

/// Compile-time policy: secondaries never write chunks/playerdata to disk.
pub const SECONDARY_MAY_PERSIST: bool = false;

/// Compile-time policy: secondaries obtain every chunk via fetch.
pub const SECONDARY_FETCHES_CHUNKS: bool = true;

/// Reports whether a secondary may generate chunk content. Always `false`.
#[must_use]
pub const fn secondary_may_generate() -> bool {
    SECONDARY_MAY_GENERATE
}

/// Reports whether a secondary may persist chunks/playerdata. Always `false`.
#[must_use]
pub const fn secondary_may_persist() -> bool {
    SECONDARY_MAY_PERSIST
}

/// Reports whether a secondary must fetch chunks. Always `true`.
#[must_use]
pub const fn secondary_must_fetch() -> bool {
    SECONDARY_FETCHES_CHUNKS
}

/// Authorizes one chunk operation for the given role.
///
/// The primary path is unaffected (`Ok(())` for every op). The secondary path
/// accepts only [`SecondaryChunkOp::Fetch`] and rejects generation plus both
/// persistence kinds with [`NoGenerateViolation`].
pub fn authorize(
    role: NodeRole,
    op: SecondaryChunkOp,
) -> Result<(), NoGenerateViolation> {
    match role {
        NodeRole::Primary => Ok(()),
        NodeRole::Secondary => check_secondary_op(op),
    }
}

/// Enforces the secondary rule without naming a role.
///
/// Accepts only [`SecondaryChunkOp::Fetch`]; generation and persistence are
/// rejected so secondary plumbing cannot grow a local generate/save path.
pub fn check_secondary_op(op: SecondaryChunkOp) -> Result<(), NoGenerateViolation> {
    if op.is_fetch() {
        Ok(())
    } else {
        Err(NoGenerateViolation::new(op))
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn secondary_generate_rejected() {
        let op = SecondaryChunkOp::Generate {
            chunk_x: 3,
            chunk_z: -7,
        };
        assert!(op.is_generate());
        assert!(!op.is_fetch());
        assert!(!secondary_may_generate());
        let err = check_secondary_op(op).unwrap_err();
        assert_eq!(err, NoGenerateViolation::new(op));
        assert_eq!(
            authorize(NodeRole::Secondary, op).unwrap_err(),
            NoGenerateViolation::new(op)
        );
    }

    #[test]
    fn secondary_persist_chunks_rejected() {
        let op = SecondaryChunkOp::PersistChunks { count: 4 };
        assert!(op.is_persist());
        assert!(!secondary_may_persist());
        let err = check_secondary_op(op).unwrap_err();
        assert_eq!(err.op, op);
        assert!(authorize(NodeRole::Secondary, op).is_err());
    }

    #[test]
    fn secondary_persist_playerdata_rejected() {
        let op = SecondaryChunkOp::PersistPlayerdata { count: 2 };
        assert!(op.is_persist());
        assert!(check_secondary_op(op).is_err());
        assert!(authorize(NodeRole::Secondary, op).is_err());
    }

    #[test]
    fn secondary_fetch_allowed() {
        let op = SecondaryChunkOp::Fetch {
            chunk_x: 0,
            chunk_z: 0,
        };
        assert!(op.is_fetch());
        assert!(!op.is_generate());
        assert!(!op.is_persist());
        assert!(secondary_must_fetch());
        assert!(check_secondary_op(op).is_ok());
        assert!(authorize(NodeRole::Secondary, op).is_ok());
    }

    #[test]
    fn primary_path_unaffected() {
        assert!(
            authorize(
                NodeRole::Primary,
                SecondaryChunkOp::Generate {
                    chunk_x: 1,
                    chunk_z: 1
                }
            )
            .is_ok()
        );
        assert!(
            authorize(
                NodeRole::Primary,
                SecondaryChunkOp::PersistChunks { count: 1 }
            )
            .is_ok()
        );
        assert!(
            authorize(
                NodeRole::Primary,
                SecondaryChunkOp::PersistPlayerdata { count: 1 }
            )
            .is_ok()
        );
    }

    #[test]
    fn audit_pins_secondary_incapability() {
        assert!(!SECONDARY_MAY_GENERATE);
        assert!(!SECONDARY_MAY_PERSIST);
        assert!(SECONDARY_FETCHES_CHUNKS);
        assert!(!secondary_may_generate());
        assert!(!secondary_may_persist());
        assert!(secondary_must_fetch());
        for op in [
            SecondaryChunkOp::Generate {
                chunk_x: 0,
                chunk_z: 0,
            },
            SecondaryChunkOp::PersistChunks { count: 1 },
            SecondaryChunkOp::PersistPlayerdata { count: 1 },
        ] {
            assert!(check_secondary_op(op).is_err(), "op allowed: {op:?}");
        }
    }
}
