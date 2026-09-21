use std::collections::{HashMap, HashSet};
use std::sync::{Arc, Mutex};

use crate::protocol::ChunkAddr;

/// Ground-truth plus local-truth dual copy for one chunk.
///
/// Ground-truth carries agreed ticks. Local-truth carries ground-truth plus
/// optimistic local edits. Optimistic edits apply to local only. Promotion
/// moves agreed ticks to ground, undoes superseded local edits from ground,
/// and reapplies survivors. Promotion never regenerates local by full copy
/// or replay.

pub const DUAL_CHUNK_WIDTH: usize = 16;
pub const DUAL_SECTION_HEIGHT: i32 = 16;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct BlockEdit {
    pub x: usize,
    pub y: i32,
    pub z: usize,
    pub state: u16,
}

impl BlockEdit {
    /// Optimistic block write at chunk-relative coordinates.
    #[must_use]
    pub const fn new(x: usize, y: i32, z: usize, state: u16) -> Self {
        Self { x, y, z, state }
    }

    /// Dual-copy bounds check for the chunk footprint.
    #[must_use]
    pub const fn is_in_bounds(&self) -> bool {
        self.x < DUAL_CHUNK_WIDTH && self.z < DUAL_CHUNK_WIDTH
    }

    /// Position key used to match optimism against ground-truth.
    #[must_use]
    pub const fn as_position(&self) -> (usize, i32, usize) {
        (self.x, self.y, self.z)
    }
}

/// Block storage backing one side of the dual copy.
pub trait DualChunkData {
    /// Reads ground- or local-truth without crossing sides.
    fn read_block(&self, x: usize, y: i32, z: usize) -> Option<u16>;
    /// Writes one side only; callers decide ground versus local.
    fn write_block(&self, x: usize, y: i32, z: usize, state: u16) -> Option<u16>;
    fn set_dirty(&self, flag: bool);
    fn is_dirty(&self) -> bool;
    fn min_y(&self) -> i32;
    fn section_count(&self) -> usize;
}

/// Full ground-to-local copy used only to fork a fresh dual copy.
///
/// Promotion does not use this path; promotion undoes and reapplies single
/// blocks instead of regenerating local truth.
#[must_use]
pub fn copy_blocks<C: DualChunkData + ?Sized>(source: &C, dest: &C) -> bool {
    let mut changed = false;
    let start = dest.min_y();
    let sections = dest.section_count();
    for section in 0..sections {
        for dy in 0..DUAL_SECTION_HEIGHT {
            let y = start.saturating_add(sections_saturating_offset(section, dy));
            for x in 0..DUAL_CHUNK_WIDTH {
                for z in 0..DUAL_CHUNK_WIDTH {
                    if let Some(state) = source.read_block(x, y, z)
                        && let Some(old) = dest.write_block(x, y, z, state)
                        && old != state
                    {
                        changed = true;
                    }
                }
            }
        }
    }
    changed
}

fn sections_saturating_offset(section: usize, dy: i32) -> i32 {
    let per_section = i32::try_from(section).unwrap_or(i32::MAX);
    per_section
        .saturating_mul(DUAL_SECTION_HEIGHT)
        .saturating_add(dy)
}

/// Ground-truth plus local-truth chunk pair sharing one chunk address.
#[derive(Debug)]
pub struct DualChunk<C> {
    /// Agreed ticks only.
    pub ground: Arc<C>,
    /// Ground plus optimistic local edits.
    pub local: Arc<C>,
}

impl<C> DualChunk<C> {
    #[must_use]
    pub fn new(ground: Arc<C>, local: Arc<C>) -> Self {
        Self { ground, local }
    }

    #[must_use]
    pub fn ground(&self) -> &Arc<C> {
        &self.ground
    }

    #[must_use]
    pub fn local(&self) -> &Arc<C> {
        &self.local
    }
}

impl<C> Clone for DualChunk<C> {
    fn clone(&self) -> Self {
        Self {
            ground: Arc::clone(&self.ground),
            local: Arc::clone(&self.local),
        }
    }
}

/// Dual-copy registry tracking optimistic edits per chunk.
#[derive(Debug)]
pub struct DualStore<C> {
    chunks: Mutex<HashMap<ChunkAddr, DualChunk<C>>>,
    pending: Mutex<HashMap<ChunkAddr, Vec<BlockEdit>>>,
}

impl<C> Default for DualStore<C> {
    fn default() -> Self {
        Self::new()
    }
}

impl<C> DualStore<C> {
    #[must_use]
    pub fn new() -> Self {
        Self {
            chunks: Mutex::new(HashMap::new()),
            pending: Mutex::new(HashMap::new()),
        }
    }
}

impl<C: DualChunkData> DualStore<C> {
    pub fn ingest(&self, chunk: ChunkAddr, pair: DualChunk<C>, pending: Vec<BlockEdit>) {
        if let Ok(mut chunks) = self.chunks.lock() {
            chunks.insert(chunk, pair);
        }
        if let Ok(mut pendings) = self.pending.lock() {
            pendings.insert(chunk, pending);
        }
    }

    #[must_use]
    pub fn remove(&self, chunk: ChunkAddr) -> bool {
        let had_pair = if let Ok(mut chunks) = self.chunks.lock() {
            chunks.remove(&chunk).is_some()
        } else {
            false
        };
        let had_pending = if let Ok(mut pendings) = self.pending.lock() {
            pendings.remove(&chunk).is_some()
        } else {
            false
        };
        had_pair || had_pending
    }

    #[must_use]
    pub fn contains(&self, chunk: ChunkAddr) -> bool {
        if let Ok(chunks) = self.chunks.lock() {
            chunks.contains_key(&chunk)
        } else {
            false
        }
    }

    #[must_use]
    pub fn pair(&self, chunk: ChunkAddr) -> Option<DualChunk<C>> {
        if let Ok(chunks) = self.chunks.lock() {
            chunks.get(&chunk).cloned()
        } else {
            None
        }
    }

    #[must_use]
    pub fn pending_for(&self, chunk: ChunkAddr) -> Vec<BlockEdit> {
        if let Ok(pendings) = self.pending.lock() {
            pendings.get(&chunk).cloned().unwrap_or_default()
        } else {
            Vec::new()
        }
    }

    /// Applies optimistic edits to local-truth immediately and tracks them as pending.
    ///
    /// Ground-truth stays untouched until promotion agrees on the tick.
    pub fn apply_local(&self, chunk: ChunkAddr, edits: &[BlockEdit]) -> bool {
        let Some(pair) = self.pair(chunk) else {
            return false;
        };
        let mut changed = false;
        for edit in edits {
            if !edit.is_in_bounds() {
                continue;
            }
            if let Some(old) = pair.local.write_block(edit.x, edit.y, edit.z, edit.state)
                && old != edit.state
            {
                changed = true;
            }
        }
        if changed {
            pair.local.set_dirty(true);
        }
        if let Ok(mut pendings) = self.pending.lock() {
            pendings
                .entry(chunk)
                .or_default()
                .extend(edits.iter().copied().filter(BlockEdit::is_in_bounds));
            true
        } else {
            false
        }
    }

    /// Promotes agreed ticks to ground-truth, then undoes superseded optimism and reapplies survivors.
    ///
    /// Ground-truth holds the agreed world. Local-truth holds ground-truth plus
    /// still-pending optimistic edits. Accepted edits move to ground. Positions
    /// that were optimistically edited but are no longer pending are undone by
    /// copying ground back to local. Survivor edits are reapplied on top. Local
    /// is never rebuilt by full copy or replay regeneration here.
    pub fn promote_tick(
        &self,
        chunk: ChunkAddr,
        accepted: &[BlockEdit],
        still_pending: &[BlockEdit],
    ) -> bool {
        let Some(pair) = self.pair(chunk) else {
            return false;
        };
        let previous_pending = self.pending_for(chunk);
        let mut ground_changed = false;
        for edit in accepted {
            if !edit.is_in_bounds() {
                continue;
            }
            if let Some(old) = pair.ground.write_block(edit.x, edit.y, edit.z, edit.state)
                && old != edit.state
            {
                ground_changed = true;
            }
        }
        if ground_changed {
            pair.ground.set_dirty(true);
        }
        let survivors: Vec<BlockEdit> = still_pending
            .iter()
            .copied()
            .filter(BlockEdit::is_in_bounds)
            .collect();
        let survivor_positions: HashSet<(usize, i32, usize)> = survivors
            .iter()
            .map(BlockEdit::as_position)
            .collect();
        let mut superseded_positions: HashSet<(usize, i32, usize)> = HashSet::new();
        for edit in previous_pending.iter().chain(accepted.iter()) {
            if !edit.is_in_bounds() {
                continue;
            }
            let position = edit.as_position();
            if !survivor_positions.contains(&position) {
                superseded_positions.insert(position);
            }
        }
        let mut local_changed = false;
        for (x, y, z) in superseded_positions {
            if let Some(ground_state) = pair.ground.read_block(x, y, z)
                && let Some(old) = pair.local.write_block(x, y, z, ground_state)
                && old != ground_state
            {
                local_changed = true;
            }
        }
        if self
            .pending
            .lock()
            .map(|mut pendings| {
                pendings.insert(chunk, survivors.clone());
            })
            .is_err()
        {
            return false;
        }
        for edit in &survivors {
            if let Some(old) = pair.local.write_block(edit.x, edit.y, edit.z, edit.state)
                && old != edit.state
            {
                local_changed = true;
            }
        }
        if local_changed {
            pair.local.set_dirty(true);
        }
        true
    }

    #[must_use]
    pub fn len(&self) -> usize {
        if let Ok(chunks) = self.chunks.lock() {
            chunks.len()
        } else {
            0
        }
    }

    #[must_use]
    pub fn is_empty(&self) -> bool {
        self.len() == 0
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::Mutex as StdMutex;
    use std::sync::atomic::{AtomicBool, Ordering};

    struct MockChunk {
        blocks: StdMutex<HashMap<(usize, i32, usize), u16>>,
        dirty: AtomicBool,
    }

    impl MockChunk {
        fn new() -> Arc<Self> {
            Arc::new(Self {
                blocks: StdMutex::new(HashMap::new()),
                dirty: AtomicBool::new(false),
            })
        }

        fn tracked(entries: &[((usize, i32, usize), u16)]) -> Arc<Self> {
            let chunk = Self::new();
            {
                let mut blocks = chunk.blocks.lock().unwrap();
                for (pos, state) in entries {
                    blocks.insert(*pos, *state);
                }
            }
            chunk
        }
    }

    impl DualChunkData for MockChunk {
        fn read_block(&self, x: usize, y: i32, z: usize) -> Option<u16> {
            self.blocks.lock().ok()?.get(&(x, y, z)).copied()
        }

        fn write_block(&self, x: usize, y: i32, z: usize, state: u16) -> Option<u16> {
            let mut blocks = self.blocks.lock().ok()?;
            if y < -64 || x >= DUAL_CHUNK_WIDTH || z >= DUAL_CHUNK_WIDTH {
                return None;
            }
            Some(blocks.insert((x, y, z), state).unwrap_or(0))
        }

        fn set_dirty(&self, flag: bool) {
            self.dirty.store(flag, Ordering::Relaxed);
        }

        fn is_dirty(&self) -> bool {
            self.dirty.load(Ordering::Relaxed)
        }

        fn min_y(&self) -> i32 {
            -64
        }

        fn section_count(&self) -> usize {
            24
        }
    }

    fn addr() -> ChunkAddr {
        ChunkAddr { x: 3, z: -2 }
    }

    fn store_with_pair() -> (DualStore<MockChunk>, ChunkAddr, Arc<MockChunk>, Arc<MockChunk>) {
        let store = DualStore::new();
        let chunk = addr();
        let ground = MockChunk::tracked(&[((1, 0, 1), 7)]);
        let local = MockChunk::tracked(&[((1, 0, 1), 7)]);
        store.ingest(chunk, DualChunk::new(Arc::clone(&ground), Arc::clone(&local)), Vec::new());
        (store, chunk, ground, local)
    }

    #[test]
    fn apply_local_touches_local_only() {
        let (store, chunk, ground, local) = store_with_pair();
        assert!(store.apply_local(chunk, &[BlockEdit::new(1, 0, 1, 9)]));
        assert_eq!(ground.read_block(1, 0, 1), Some(7));
        assert_eq!(local.read_block(1, 0, 1), Some(9));
        assert!(!ground.is_dirty());
        assert!(local.is_dirty());
        assert_eq!(store.pending_for(chunk).len(), 1);
    }

    #[test]
    fn promote_tick_moves_accepted_to_ground_and_rebases() {
        let (store, chunk, ground, local) = store_with_pair();
        assert!(store.apply_local(chunk, &[BlockEdit::new(2, 0, 2, 5)]));
        assert!(store.apply_local(chunk, &[BlockEdit::new(1, 0, 1, 9)]));
        let accepted = [BlockEdit::new(1, 0, 1, 9)];
        let still_pending = [BlockEdit::new(2, 0, 2, 5)];
        assert!(store.promote_tick(chunk, &accepted, &still_pending));
        assert_eq!(ground.read_block(1, 0, 1), Some(9));
        assert_eq!(local.read_block(1, 0, 1), Some(9));
        assert_eq!(local.read_block(2, 0, 2), Some(5));
        assert!(ground.is_dirty());
        assert_eq!(store.pending_for(chunk), still_pending);
    }

    #[test]
    fn rebase_drops_superseded_local_edits() {
        let (store, chunk, _ground, local) = store_with_pair();
        assert!(store.apply_local(chunk, &[BlockEdit::new(1, 0, 1, 9)]));
        assert!(store.promote_tick(chunk, &[BlockEdit::new(1, 0, 1, 4)], &[]));
        assert_eq!(local.read_block(1, 0, 1), Some(4));
        assert!(store.pending_for(chunk).is_empty());
    }

    #[test]
    fn ingest_replaces_both_copies() {
        let (store, chunk, _ground, _local) = store_with_pair();
        let fresh_ground = MockChunk::tracked(&[((4, 0, 4), 11)]);
        let fresh_local = MockChunk::tracked(&[((4, 0, 4), 11)]);
        store.ingest(
            chunk,
            DualChunk::new(Arc::clone(&fresh_ground), Arc::clone(&fresh_local)),
            vec![BlockEdit::new(4, 0, 4, 11)],
        );
        let pair = store.pair(chunk).unwrap();
        assert_eq!(pair.ground.read_block(4, 0, 4), Some(11));
        assert_eq!(pair.local.read_block(4, 0, 4), Some(11));
        assert_eq!(store.pending_for(chunk).len(), 1);
    }

    #[test]
    fn missing_chunk_operations_report_false() {
        let store: DualStore<MockChunk> = DualStore::new();
        assert!(!store.apply_local(addr(), &[BlockEdit::new(0, 0, 0, 1)]));
        assert!(!store.promote_tick(addr(), &[], &[]));
        assert!(!store.remove(addr()));
        assert!(store.pair(addr()).is_none());
    }

    #[test]
    fn out_of_bounds_edits_are_ignored() {
        let (store, chunk, ground, local) = store_with_pair();
        assert!(store.apply_local(chunk, &[BlockEdit::new(16, 0, 0, 1)]));
        assert!(!local.is_dirty());
        assert!(store.pending_for(chunk).is_empty());
        assert_eq!(ground.read_block(1, 0, 1), Some(7));
    }
}
