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

fn overlaps(left: &[ChunkKey], right: &[ChunkKey]) -> bool {
    left.iter().any(|chunk| right.binary_search(chunk).is_ok())
}

#[must_use]
pub fn categorize<T>(updates: impl IntoIterator<Item = (Vec<ChunkKey>, T)>) -> Vec<ChunkWork<T>> {
    let mut categories: Vec<ChunkWork<T>> = Vec::new();
    for (mut chunks, update) in updates {
        normalize(&mut chunks);
        let mut matching: Vec<usize> = categories
            .iter()
            .enumerate()
            .filter_map(|(index, category)| overlaps(&chunks, &category.chunks).then_some(index))
            .collect();
        if matching.is_empty() {
            categories.push(ChunkWork {
                chunks,
                updates: vec![update],
            });
            continue;
        }
        let mut merged = Vec::new();
        matching.sort_unstable();
        for index in matching.into_iter().rev() {
            merged.push(categories.remove(index));
        }
        merged.reverse();
        let mut joined = ChunkWork {
            chunks,
            updates: Vec::new(),
        };
        for mut category in merged {
            joined.chunks.append(&mut category.chunks);
            joined.updates.append(&mut category.updates);
        }
        joined.updates.push(update);
        normalize(&mut joined.chunks);
        categories.push(joined);
    }
    categories.sort_by(|left, right| left.chunks.cmp(&right.chunks));
    categories
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
}
