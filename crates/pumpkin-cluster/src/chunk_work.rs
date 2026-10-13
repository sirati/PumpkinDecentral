use crate::protocol::ChunkKey;

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ChunkWork<T> {
    pub chunks: Vec<ChunkKey>,
    pub updates: Vec<T>,
}

fn normalize(chunks: &mut Vec<ChunkKey>) {
    chunks.sort_unstable();
    chunks.dedup();
}

#[cfg(test)]
fn overlaps(left: &[ChunkKey], right: &[ChunkKey]) -> bool {
    left.iter().any(|chunk| right.binary_search(chunk).is_ok())
}

fn representative(parents: &mut [usize], mut index: usize) -> usize {
    while parents[index] != index {
        parents[index] = parents[parents[index]];
        index = parents[index];
    }
    index
}

#[must_use]
pub fn categorize<T>(updates: impl IntoIterator<Item = (Vec<ChunkKey>, T)>) -> Vec<ChunkWork<T>> {
    let mut updates = updates.into_iter().collect::<Vec<_>>();
    let mut parents = (0..updates.len()).collect::<Vec<_>>();
    let mut by_chunk = std::collections::BTreeMap::new();
    for (index, (chunks, _)) in updates.iter_mut().enumerate() {
        normalize(chunks);
        for chunk in chunks {
            if let Some(previous) = by_chunk.insert(*chunk, index) {
                let left = representative(&mut parents, previous);
                let right = representative(&mut parents, index);
                parents[right] = left;
            }
        }
    }
    let mut categories = std::collections::BTreeMap::<usize, ChunkWork<T>>::new();
    for (index, (mut chunks, update)) in updates.into_iter().enumerate() {
        let category = categories.entry(representative(&mut parents, index))
            .or_insert_with(|| ChunkWork { chunks: Vec::new(), updates: Vec::new() });
        category.chunks.append(&mut chunks);
        category.updates.push(update);
    }
    let mut categories = categories.into_values().collect::<Vec<_>>();
    for category in &mut categories { normalize(&mut category.chunks); }
    categories.sort_by(|left, right| left.chunks.cmp(&right.chunks));
    categories
}

#[derive(Debug)]
pub struct OwnedChunkWork<T, S> {
    pub chunks: std::collections::BTreeMap<ChunkKey, S>,
    pub updates: Vec<T>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ExtractionFailure {
    EmptyCategory,
    MissingChunk(ChunkKey),
    RepeatedChunk(ChunkKey),
}

pub fn extract<T, S>(
    categories: Vec<ChunkWork<T>>,
    shards: &mut std::collections::BTreeMap<ChunkKey, S>,
) -> Result<Vec<OwnedChunkWork<T, S>>, (ExtractionFailure, Vec<ChunkWork<T>>)> {
    let mut seen = std::collections::BTreeSet::new();
    for category in &categories {
        if category.chunks.is_empty() {
            return Err((ExtractionFailure::EmptyCategory, categories));
        }
        for chunk in &category.chunks {
            if !seen.insert(*chunk) {
                return Err((ExtractionFailure::RepeatedChunk(*chunk), categories));
            }
            if !shards.contains_key(chunk) {
                return Err((ExtractionFailure::MissingChunk(*chunk), categories));
            }
        }
    }
    Ok(categories.into_iter().map(|category| OwnedChunkWork {
        chunks: category.chunks.into_iter().map(|chunk| {
            (chunk, shards.remove(&chunk).unwrap())
        }).collect(),
        updates: category.updates,
    }).collect())
}

#[cfg(test)]
mod tests {
    use super::*;

    fn chunk(x: i32) -> ChunkKey {
        ChunkKey::new(crate::protocol::WorldId::new(0, 7), x, 0)
    }

    #[test]
    fn bridge_action_forms_one_joint_category_without_merging_disjoint_work() {
        let categories = categorize([
            (vec![chunk(0)], 1),
            (vec![chunk(2)], 2),
            (vec![chunk(0), chunk(2)], 3),
            (vec![chunk(9)], 4),
        ]);
        assert_eq!(categories.len(), 2);
        assert_eq!(categories[0].chunks, vec![chunk(0), chunk(2)]);
        assert_eq!(categories[0].updates, vec![1, 2, 3]);
        assert_eq!(categories[1].chunks, vec![chunk(9)]);
        assert_eq!(categories[1].updates, vec![4]);
        assert!(!overlaps(&categories[0].chunks, &categories[1].chunks));
    }
    #[test]
    fn extraction_moves_whole_joint_shards_and_preserves_missing_work() {
        let categories = categorize([
            (vec![chunk(0), chunk(1)], 1),
            (vec![chunk(1)], 2),
            (vec![chunk(9)], 3),
        ]);
        let mut shards = std::collections::BTreeMap::from([
            (chunk(0), vec![10, 11]),
            (chunk(1), vec![20]),
        ]);
        let (failure, categories) = extract(categories, &mut shards).unwrap_err();
        assert_eq!(failure, ExtractionFailure::MissingChunk(chunk(9)));
        assert_eq!(shards.len(), 2);
        shards.insert(chunk(9), vec![90]);
        let units = extract(categories, &mut shards).unwrap();
        assert!(shards.is_empty());
        assert_eq!(units.len(), 2);
        assert_eq!(units[0].updates, vec![1, 2]);
        assert_eq!(units[0].chunks[&chunk(0)], vec![10, 11]);
        assert_eq!(units[0].chunks[&chunk(1)], vec![20]);
        assert_eq!(units[1].chunks[&chunk(9)], vec![90]);
    }

}
