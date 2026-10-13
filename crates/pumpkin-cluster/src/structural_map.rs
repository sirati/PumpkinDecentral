use std::sync::Arc;

type Link<K, V> = Option<Arc<Node<K, V>>>;

#[derive(Clone, Debug)]
struct Node<K, V> {
    key: K,
    value: Arc<V>,
    left: Link<K, V>,
    right: Link<K, V>,
    height: u8,
    count: usize,
}

#[derive(Clone, Debug)]
pub struct StructuralMap<K, V> {
    root: Link<K, V>,
}

impl<K, V> Default for StructuralMap<K, V> {
    fn default() -> Self { Self { root: None } }
}

fn height<K, V>(link: &Link<K, V>) -> u8 { link.as_ref().map_or(0, |node| node.height) }
fn count<K, V>(link: &Link<K, V>) -> usize { link.as_ref().map_or(0, |node| node.count) }

fn node<K, V>(key: K, value: Arc<V>, left: Link<K, V>, right: Link<K, V>) -> Arc<Node<K, V>> {
    Arc::new(Node { key, value, height: 1 + height(&left).max(height(&right)), count: 1 + count(&left) + count(&right), left, right })
}

fn rotate_left<K: Clone, V>(root: Arc<Node<K, V>>) -> Arc<Node<K, V>> {
    let right = root.right.as_ref().expect("structural map right rotation child");
    let left = node(root.key.clone(), root.value.clone(), root.left.clone(), right.left.clone());
    node(right.key.clone(), right.value.clone(), Some(left), right.right.clone())
}

fn rotate_right<K: Clone, V>(root: Arc<Node<K, V>>) -> Arc<Node<K, V>> {
    let left = root.left.as_ref().expect("structural map left rotation child");
    let right = node(root.key.clone(), root.value.clone(), left.right.clone(), root.right.clone());
    node(left.key.clone(), left.value.clone(), left.left.clone(), Some(right))
}

fn balanced<K: Clone, V>(mut root: Arc<Node<K, V>>) -> Arc<Node<K, V>> {
    if height(&root.left) > height(&root.right) + 1 {
        let left = root.left.as_ref().expect("structural map unbalanced left child");
        if height(&left.right) > height(&left.left) {
            root = node(root.key.clone(), root.value.clone(), Some(rotate_left(left.clone())), root.right.clone());
        }
        rotate_right(root)
    } else if height(&root.right) > height(&root.left) + 1 {
        let right = root.right.as_ref().expect("structural map unbalanced right child");
        if height(&right.left) > height(&right.right) {
            root = node(root.key.clone(), root.value.clone(), root.left.clone(), Some(rotate_right(right.clone())));
        }
        rotate_left(root)
    } else { root }
}

fn inserted<K: Ord + Clone, V>(root: &Link<K, V>, key: K, value: Arc<V>) -> Arc<Node<K, V>> {
    let Some(root) = root else { return node(key, value, None, None); };
    balanced(match key.cmp(&root.key) {
        std::cmp::Ordering::Less => node(root.key.clone(), root.value.clone(), Some(inserted(&root.left, key, value)), root.right.clone()),
        std::cmp::Ordering::Greater => node(root.key.clone(), root.value.clone(), root.left.clone(), Some(inserted(&root.right, key, value))),
        std::cmp::Ordering::Equal => node(key, value, root.left.clone(), root.right.clone()),
    })
}

fn removed<K: Ord + Clone, V>(root: &Link<K, V>, key: &K) -> Link<K, V> {
    let root = root.as_ref()?;
    let changed = match key.cmp(&root.key) {
        std::cmp::Ordering::Less => node(root.key.clone(), root.value.clone(), removed(&root.left, key), root.right.clone()),
        std::cmp::Ordering::Greater => node(root.key.clone(), root.value.clone(), root.left.clone(), removed(&root.right, key)),
        std::cmp::Ordering::Equal => match (&root.left, &root.right) {
            (None, _) => return root.right.clone(),
            (_, None) => return root.left.clone(),
            (Some(_), Some(right)) => {
                let mut successor = right.as_ref();
                while let Some(left) = &successor.left { successor = left; }
                node(successor.key.clone(), successor.value.clone(), root.left.clone(), removed(&root.right, &successor.key))
            }
        },
    };
    Some(balanced(changed))
}

impl<K: Ord + Clone, V: Clone> StructuralMap<K, V> {
    pub fn get(&self, key: &K) -> Option<&V> {
        let mut next = self.root.as_deref();
        while let Some(node) = next {
            match key.cmp(&node.key) {
                std::cmp::Ordering::Less => next = node.left.as_deref(),
                std::cmp::Ordering::Greater => next = node.right.as_deref(),
                std::cmp::Ordering::Equal => return Some(&node.value),
            }
        }
        None
    }

    pub fn get_mut(&mut self, key: &K) -> Option<&mut V> {
        let mut next = &mut self.root;
        loop {
            let node = Arc::make_mut(next.as_mut()?);
            match key.cmp(&node.key) {
                std::cmp::Ordering::Less => next = &mut node.left,
                std::cmp::Ordering::Greater => next = &mut node.right,
                std::cmp::Ordering::Equal => return Some(Arc::make_mut(&mut node.value)),
            }
        }
    }

    pub fn insert(&mut self, key: K, value: V) -> Option<V> {
        let previous = self.get(&key).cloned();
        self.root = Some(inserted(&self.root, key, Arc::new(value)));
        previous
    }

    pub fn remove(&mut self, key: &K) -> Option<V> {
        let previous = self.get(key)?.clone();
        self.root = removed(&self.root, key);
        Some(previous)
    }

    pub fn contains_key(&self, key: &K) -> bool { self.get(key).is_some() }
    pub fn len(&self) -> usize { count(&self.root) }
    pub fn is_empty(&self) -> bool { self.root.is_none() }

    pub fn get_or_insert_default(&mut self, key: K) -> &mut V where V: Default {
        if !self.contains_key(&key) { self.insert(key.clone(), V::default()); }
        self.get_mut(&key).expect("inserted structural map key")
    }

    pub fn iter(&self) -> StructuralIter<'_, K, V> {
        let mut iterator = StructuralIter { stack: Vec::new() };
        iterator.descend(self.root.as_deref());
        iterator
    }

    pub fn keys(&self) -> impl Iterator<Item = &K> { self.iter().map(|(key, _)| key) }
    pub fn values(&self) -> impl Iterator<Item = &V> { self.iter().map(|(_, value)| value) }
}

pub struct StructuralIter<'a, K, V> {
    stack: Vec<&'a Node<K, V>>,
}

impl<'a, K, V> StructuralIter<'a, K, V> {
    fn descend(&mut self, mut next: Option<&'a Node<K, V>>) {
        while let Some(node) = next { self.stack.push(node); next = node.left.as_deref(); }
    }
}

impl<'a, K, V> Iterator for StructuralIter<'a, K, V> {
    type Item = (&'a K, &'a V);
    fn next(&mut self) -> Option<Self::Item> {
        let node = self.stack.pop()?;
        self.descend(node.right.as_deref());
        Some((&node.key, &node.value))
    }
}

impl<K: Ord + Clone, V: Clone> StructuralMap<K, V> {
    pub fn entry(&mut self, key: K) -> StructuralEntry<'_, K, V> { StructuralEntry { map: self, key } }

    pub fn range<R: std::ops::RangeBounds<K>>(&self, bounds: R) -> StructuralRange<'_, K, V, R> {
        let mut iterator = StructuralIter { stack: Vec::new() };
        let mut next = self.root.as_deref();
        while let Some(node) = next {
            let before = match bounds.start_bound() {
                std::ops::Bound::Included(start) => node.key < *start,
                std::ops::Bound::Excluded(start) => node.key <= *start,
                std::ops::Bound::Unbounded => false,
            };
            if before { next = node.right.as_deref(); }
            else { iterator.stack.push(node); next = node.left.as_deref(); }
        }
        StructuralRange { iterator, bounds }
    }
}

pub struct StructuralEntry<'a, K, V> {
    map: &'a mut StructuralMap<K, V>,
    key: K,
}

impl<'a, K: Ord + Clone, V: Clone> StructuralEntry<'a, K, V> {
    pub fn or_insert_with(self, create: impl FnOnce() -> V) -> &'a mut V {
        if !self.map.contains_key(&self.key) { self.map.insert(self.key.clone(), create()); }
        self.map.get_mut(&self.key).expect("inserted structural entry")
    }
    pub fn or_insert(self, value: V) -> &'a mut V { self.or_insert_with(|| value) }
    pub fn or_default(self) -> &'a mut V where V: Default { self.or_insert_with(V::default) }
    pub fn and_modify(self, update: impl FnOnce(&mut V)) -> Self {
        if let Some(value) = self.map.get_mut(&self.key) { update(value); }
        self
    }
}

pub struct StructuralRange<'a, K, V, R> {
    iterator: StructuralIter<'a, K, V>,
    bounds: R,
}

impl<'a, K: Ord, V, R: std::ops::RangeBounds<K>> Iterator for StructuralRange<'a, K, V, R> {
    type Item = (&'a K, &'a V);
    fn next(&mut self) -> Option<Self::Item> {
        let (key, value) = self.iterator.next()?;
        let after = match self.bounds.end_bound() {
            std::ops::Bound::Included(end) => key > end,
            std::ops::Bound::Excluded(end) => key >= end,
            std::ops::Bound::Unbounded => false,
        };
        if after { self.iterator.stack.clear(); None } else { Some((key, value)) }
    }
}

impl<K: Ord + Clone, V: Clone> FromIterator<(K, V)> for StructuralMap<K, V> {
    fn from_iter<T: IntoIterator<Item = (K, V)>>(values: T) -> Self {
        let mut result = Self::default();
        for (key, value) in values { result.insert(key, value); }
        result
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::collections::BTreeMap;
    use std::sync::atomic::{AtomicUsize, Ordering};

    #[derive(Debug)]
    struct Counted {
        value: usize,
        clones: Arc<AtomicUsize>,
    }
    impl Clone for Counted {
        fn clone(&self) -> Self {
            self.clones.fetch_add(1, Ordering::Relaxed);
            Self { value: self.value, clones: self.clones.clone() }
        }
    }

    #[test]
    fn mutations_and_ranges_preserve_retained_versions() {
        let mut actual = StructuralMap::default();
        let mut expected = BTreeMap::new();
        let mut retained = Vec::new();
        let mut seed = 73_u64;
        for turn in 0..8192 {
            seed = seed.wrapping_mul(6364136223846793005).wrapping_add(1);
            let key = (seed >> 32) as u16 % 1000;
            match seed & 3 {
                0 => { assert_eq!(actual.remove(&key), expected.remove(&key)); }
                1 => {
                    if let Some(value) = actual.get_mut(&key) { *value += 1; }
                    if let Some(value) = expected.get_mut(&key) { *value += 1; }
                }
                _ => { assert_eq!(actual.insert(key, turn), expected.insert(key, turn)); }
            }
            if turn % 512 == 0 { retained.push((actual.clone(), expected.clone())); }
            if turn % 64 == 0 {
                assert_eq!(actual.iter().map(|(key, value)| (*key, *value)).collect::<Vec<_>>(), expected.iter().map(|(key, value)| (*key, *value)).collect::<Vec<_>>());
                assert_eq!(actual.range(200..=400).map(|(key, value)| (*key, *value)).collect::<Vec<_>>(), expected.range(200..=400).map(|(key, value)| (*key, *value)).collect::<Vec<_>>());
            }
        }
        for (actual, expected) in retained {
            assert_eq!(actual.iter().map(|(key, value)| (*key, *value)).collect::<Vec<_>>(), expected.into_iter().collect::<Vec<_>>());
        }
    }

    #[test]
    fn hundred_thousand_record_snapshot_copies_only_changed_payload() {
        let clones = Arc::new(AtomicUsize::new(0));
        let mut map = StructuralMap::default();
        for key in 0..100_000 { map.insert(key, Counted { value: key, clones: clones.clone() }); }
        let original = map.clone();
        assert!(Arc::ptr_eq(original.root.as_ref().unwrap(), map.root.as_ref().unwrap()));
        assert_eq!(clones.load(Ordering::Relaxed), 0);
        map.get_mut(&50_000).unwrap().value = 999;
        assert_eq!(clones.load(Ordering::Relaxed), 1);
        assert_eq!(original.get(&50_000).unwrap().value, 50_000);
        assert_eq!(map.get(&50_000).unwrap().value, 999);
        assert!(height(&map.root) < 32);
    }
}

impl<'a, K: Ord + Clone, V: Clone> IntoIterator for &'a StructuralMap<K, V> {
    type Item = (&'a K, &'a V);
    type IntoIter = StructuralIter<'a, K, V>;
    fn into_iter(self) -> Self::IntoIter { self.iter() }
}

impl<K: Ord + Clone, V: Clone + PartialEq> PartialEq for StructuralMap<K, V> {
    fn eq(&self, other: &Self) -> bool {
        if let (Some(left), Some(right)) = (&self.root, &other.root) {
            if Arc::ptr_eq(left, right) { return true; }
        }
        self.len() == other.len() && self.iter().eq(other.iter())
    }
}
impl<K: Ord + Clone, V: Clone + Eq> Eq for StructuralMap<K, V> {}

impl<K: Ord + Clone, V: Clone> std::ops::Index<&K> for StructuralMap<K, V> {
    type Output = V;
    fn index(&self, key: &K) -> &V {
        self.get(key).expect("structural map indexed key")
    }
}

impl<K: Ord + Clone, V: Clone> StructuralMap<K, V> {
    pub fn clear(&mut self) {
        self.root = None;
    }

    pub fn retain(&mut self, mut keep: impl FnMut(&K, &V) -> bool) {
        let rejected = self.iter().filter_map(|(key, value)| {
            (!keep(key, value)).then(|| key.clone())
        }).collect::<Vec<_>>();
        for key in rejected {
            self.root = removed(&self.root, &key);
        }
    }
}

impl<K: Ord + Clone, V: Clone> Extend<(K, V)> for StructuralMap<K, V> {
    fn extend<T: IntoIterator<Item = (K, V)>>(&mut self, entries: T) {
        for (key, value) in entries {
            self.insert(key, value);
        }
    }
}

impl<K: Ord + Clone, V: Clone> StructuralMap<K, V> {
    pub fn get_shared(&self, key: &K) -> Option<Arc<V>> {
        let mut next = self.root.as_deref();
        while let Some(node) = next {
            match key.cmp(&node.key) {
                std::cmp::Ordering::Less => next = node.left.as_deref(),
                std::cmp::Ordering::Greater => next = node.right.as_deref(),
                std::cmp::Ordering::Equal => return Some(node.value.clone()),
            }
        }
        None
    }

    pub fn take_shared(&mut self, key: &K) -> Option<Arc<V>> {
        let previous = self.get_shared(key)?;
        self.root = removed(&self.root, key);
        Some(previous)
    }

    pub fn insert_shared(&mut self, key: K, value: Arc<V>) -> Option<Arc<V>> {
        let previous = self.get_shared(&key);
        self.root = Some(inserted(&self.root, key, value));
        previous
    }
}

pub trait OrderedPrefix<P>: Ord {
    fn ordered_prefix(&self) -> &P;
}

impl<P: Ord, A: Ord> OrderedPrefix<P> for (P, A) {
    fn ordered_prefix(&self) -> &P { &self.0 }
}

impl<P: Ord, A: Ord, B: Ord> OrderedPrefix<P> for (P, A, B) {
    fn ordered_prefix(&self) -> &P { &self.0 }
}

pub struct StructuralPrefix<'a, K, V, P> {
    stack: Vec<&'a Node<K, V>>,
    prefix: P,
}

impl<'a, K: OrderedPrefix<P>, V, P: Ord> StructuralPrefix<'a, K, V, P> {
    fn descend(&mut self, mut next: Option<&'a Node<K, V>>) {
        while let Some(node) = next {
            if node.key.ordered_prefix() < &self.prefix {
                next = node.right.as_deref();
            } else {
                self.stack.push(node);
                next = node.left.as_deref();
            }
        }
    }
}

impl<'a, K: OrderedPrefix<P>, V, P: Ord> Iterator for StructuralPrefix<'a, K, V, P> {
    type Item = (&'a K, &'a V);

    fn next(&mut self) -> Option<Self::Item> {
        let node = self.stack.pop()?;
        if node.key.ordered_prefix() != &self.prefix {
            self.stack.clear();
            return None;
        }
        self.descend(node.right.as_deref());
        Some((&node.key, &node.value))
    }
}

impl<K: Ord + Clone, V: Clone> StructuralMap<K, V> {
    pub fn prefix<P: Ord>(&self, prefix: P) -> StructuralPrefix<'_, K, V, P>
    where K: OrderedPrefix<P> {
        let mut entries = StructuralPrefix { stack: Vec::new(), prefix };
        entries.descend(self.root.as_deref());
        entries
    }

    pub fn remove_prefix<P: Ord>(&mut self, prefix: P) -> usize
    where K: OrderedPrefix<P> {
        let keys = self.prefix(prefix).map(|(key, _)| key.clone()).collect::<Vec<_>>();
        let count = keys.len();
        for key in keys { self.root = removed(&self.root, &key); }
        count
    }
}

#[cfg(test)]
mod prefix_tests {
    use super::*;

    struct NoPayloadClone;
    impl Clone for NoPayloadClone {
        fn clone(&self) -> Self { panic!("prefix removal must not clone payloads") }
    }

    #[test]
    fn prefix_iteration_and_removal_preserve_other_ticks_and_retained_roots() {
        let mut map = (0..8).flat_map(|tick| (0..16).map(move |key| ((tick, key), NoPayloadClone)))
            .collect::<StructuralMap<_, _>>();
        let retained = map.clone();
        assert_eq!(map.prefix(4).map(|(key, _)| *key).collect::<Vec<_>>(), (0..16).map(|key| (4, key)).collect::<Vec<_>>());
        assert_eq!(map.prefix(0).count(), 16);
        assert_eq!(map.prefix(7).count(), 16);
        assert_eq!(map.prefix(9).count(), 0);
        assert_eq!(map.remove_prefix(4), 16);
        assert_eq!(map.prefix(4).count(), 0);
        assert_eq!(map.prefix(5).count(), 16);
        assert_eq!(retained.prefix(4).count(), 16);
        assert_eq!(map.len(), 112);
        assert_eq!(map.remove_prefix(4), 0);
    }
}
