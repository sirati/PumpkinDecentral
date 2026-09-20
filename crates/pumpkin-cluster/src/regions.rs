use serde::{Deserialize, Serialize};

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct SyncedRegion {
    pub world: String,
    pub min_x: i32,
    pub max_x: i32,
    pub min_z: i32,
    pub max_z: i32,
}

impl SyncedRegion {
    #[must_use]
    pub fn new(world: String, min_x: i32, max_x: i32, min_z: i32, max_z: i32) -> Self {
        Self {
            world,
            min_x: min_x.min(max_x),
            max_x: min_x.max(max_x),
            min_z: min_z.min(max_z),
            max_z: min_z.max(max_z),
        }
    }

    #[must_use]
    pub fn spawn_area(world: String, center_x: i32, center_z: i32, radius: i32) -> Self {
        let extent = radius.max(0);
        Self::new(
            world,
            center_x.saturating_sub(extent),
            center_x.saturating_add(extent),
            center_z.saturating_sub(extent),
            center_z.saturating_add(extent),
        )
    }

    #[must_use]
    pub fn contains(&self, world: &str, x: i32, z: i32) -> bool {
        self.world == world
            && x >= self.min_x
            && x <= self.max_x
            && z >= self.min_z
            && z <= self.max_z
    }

    #[must_use]
    pub fn chunk_count(&self) -> u64 {
        let width = self.max_x as i64 - self.min_x as i64 + 1;
        let depth = self.max_z as i64 - self.min_z as i64 + 1;
        (width.max(0) as u64).saturating_mul(depth.max(0) as u64)
    }

    pub fn chunks(&self) -> impl Iterator<Item = (i32, i32)> + '_ {
        (self.min_x..=self.max_x).flat_map(|x| (self.min_z..=self.max_z).map(move |z| (x, z)))
    }
}

#[derive(Debug, Default)]
pub struct RegionRegistry {
    regions: Vec<SyncedRegion>,
}

impl RegionRegistry {
    #[must_use]
    pub fn new() -> Self {
        Self::default()
    }

    pub fn register(&mut self, region: SyncedRegion) {
        if !self.regions.contains(&region) {
            self.regions.push(region);
        }
    }

    #[must_use]
    pub fn is_synced(&self, world: &str, x: i32, z: i32) -> bool {
        self.regions
            .iter()
            .any(|region| region.contains(world, x, z))
    }

    pub fn remove(&mut self, region: &SyncedRegion) -> bool {
        let before = self.regions.len();
        self.regions.retain(|candidate| candidate != region);
        self.regions.len() != before
    }

    #[must_use]
    pub fn regions(&self) -> &[SyncedRegion] {
        &self.regions
    }

    #[must_use]
    pub fn len(&self) -> usize {
        self.regions.len()
    }

    #[must_use]
    pub fn is_empty(&self) -> bool {
        self.regions.is_empty()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn new_normalizes_reversed_bounds() {
        let region = SyncedRegion::new("world".to_owned(), 5, -5, 3, -3);
        assert_eq!((region.min_x, region.max_x), (-5, 5));
        assert_eq!((region.min_z, region.max_z), (-3, 3));
    }

    #[test]
    fn spawn_area_centers_with_radius() {
        let region = SyncedRegion::spawn_area("world".to_owned(), 10, -4, 2);
        assert_eq!((region.min_x, region.max_x), (8, 12));
        assert_eq!((region.min_z, region.max_z), (-6, -2));
        assert_eq!(region.chunk_count(), 25);
    }

    #[test]
    fn spawn_area_zero_radius_is_single_chunk() {
        let region = SyncedRegion::spawn_area("world".to_owned(), 7, 7, 0);
        assert_eq!(region.chunk_count(), 1);
        assert!(region.contains("world", 7, 7));
    }

    #[test]
    fn contains_rejects_other_world_and_outside() {
        let region = SyncedRegion::new("world".to_owned(), 0, 2, 0, 2);
        assert!(region.contains("world", 2, 2));
        assert!(!region.contains("world_nether", 1, 1));
        assert!(!region.contains("world", 3, 1));
        assert!(!region.contains("world", 1, -1));
    }

    #[test]
    fn chunks_iterates_every_chunk_once() {
        let region = SyncedRegion::new("world".to_owned(), 0, 1, 0, 2);
        let mut visited: Vec<(i32, i32)> = region.chunks().collect();
        visited.sort_unstable();
        assert_eq!(
            visited,
            vec![(0, 0), (0, 1), (0, 2), (1, 0), (1, 1), (1, 2)]
        );
        assert_eq!(visited.len() as u64, region.chunk_count());
    }

    #[test]
    fn registry_removes_only_matching_region() {
        let mut registry = RegionRegistry::new();
        let first = SyncedRegion::new("world".to_owned(), 0, 1, 0, 1);
        let second = SyncedRegion::new("world".to_owned(), 5, 6, 5, 6);
        registry.register(first.clone());
        registry.register(second.clone());
        assert!(registry.remove(&first));
        assert!(!registry.remove(&first));
        assert_eq!(registry.regions(), &[second]);
    }

    #[test]
    fn registry_dedupes_and_matches() {
        let mut registry = RegionRegistry::new();
        assert!(registry.is_empty());
        let region = SyncedRegion::new("world".to_owned(), 0, 1, 0, 1);
        registry.register(region.clone());
        registry.register(region.clone());
        assert_eq!(registry.len(), 1);
        assert!(registry.is_synced("world", 1, 1));
        assert!(!registry.is_synced("world", 5, 5));
        assert_eq!(registry.regions(), &[region]);
    }
}
