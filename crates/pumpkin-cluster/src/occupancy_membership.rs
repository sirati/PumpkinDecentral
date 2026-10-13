use super::actor_occupancy::{ActorOccupancyEntry, MAX_OCCUPANCY_ENTRIES};
use crate::actor::ActorId;
use serde::{Deserialize, Deserializer, Serialize, Serializer};
use sha2::{Digest, Sha256};
use std::sync::Arc;

#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct OccupancyCommitment {
    pub hash: [u8; 32],
    #[serde(deserialize_with = "bounded_count")]
    pub count: usize,
}
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct OccupancyChange {
    pub actor: ActorId,
    pub before: Option<ActorOccupancyEntry>,
    pub after: Option<ActorOccupancyEntry>,
}
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct OccupancyPatch {
    pub base: OccupancyCommitment,
    #[serde(deserialize_with = "bounded_vec")]
    pub changes: Vec<OccupancyChange>,
}
#[derive(Clone)]
struct Node {
    key: [u8; 15],
    commitment: OccupancyCommitment,
    kind: NodeKind,
}
#[derive(Clone)]
enum NodeKind {
    Leaf(ActorOccupancyEntry),
    Branch {
        bit: u8,
        left: Arc<Node>,
        right: Arc<Node>,
    },
}
fn actor_key(actor: ActorId) -> [u8; 15] {
    let mut key = [0; 15];
    match actor {
        ActorId::Player(id) => {
            key[1..3].copy_from_slice(&id.server.0.to_be_bytes());
            key[3..5].copy_from_slice(&id.player.0.to_be_bytes());
        }
        ActorId::Entity(id) => {
            key[0] = 1;
            key[1..3].copy_from_slice(&id.origin.0.to_be_bytes());
            key[3..7].copy_from_slice(&((id.local_id as u32) ^ 0x8000_0000).to_be_bytes());
            key[7..15].copy_from_slice(&id.generation.to_be_bytes());
        }
        ActorId::Peer(id) => {
            key[0] = 2;
            key[1..3].copy_from_slice(&id.0.to_be_bytes());
        }
    }
    key
}
fn bit(key: &[u8; 15], index: u8) -> bool {
    key[usize::from(index) / 8] & (0x80 >> (index % 8)) != 0
}
fn differing_bit(a: &[u8; 15], b: &[u8; 15]) -> Option<u8> {
    a.iter().zip(b).enumerate().find_map(|(index, (a, b))| {
        let xor = a ^ b;
        (xor != 0).then(|| (index * 8 + xor.leading_zeros() as usize) as u8)
    })
}
fn empty_commitment() -> OccupancyCommitment {
    OccupancyCommitment {
        hash: Sha256::digest(b"Pumpkin.ActorOccupancy.Empty.v1").into(),
        count: 0,
    }
}
fn leaf(entry: ActorOccupancyEntry) -> Arc<Node> {
    let bytes = postcard::to_allocvec(&entry).expect("bounded occupancy entry");
    let mut hash = Sha256::new();
    hash.update(b"Pumpkin.ActorOccupancy.Leaf.v1");
    hash.update((bytes.len() as u64).to_be_bytes());
    hash.update(bytes);
    Arc::new(Node {
        key: actor_key(entry.actor),
        commitment: OccupancyCommitment {
            hash: hash.finalize().into(),
            count: 1,
        },
        kind: NodeKind::Leaf(entry),
    })
}
fn branch(index: u8, left: Arc<Node>, right: Arc<Node>) -> Arc<Node> {
    let count = left.commitment.count + right.commitment.count;
    let mut hash = Sha256::new();
    hash.update(b"Pumpkin.ActorOccupancy.Branch.v1");
    hash.update([index]);
    for child in [&left, &right] {
        hash.update((child.commitment.count as u64).to_be_bytes());
        hash.update(child.commitment.hash);
    }
    Arc::new(Node {
        key: left.key,
        commitment: OccupancyCommitment {
            hash: hash.finalize().into(),
            count,
        },
        kind: NodeKind::Branch {
            bit: index,
            left,
            right,
        },
    })
}
fn insert(node: &Arc<Node>, incoming: Arc<Node>) -> Arc<Node> {
    let difference = differing_bit(&node.key, &incoming.key);
    let branch_bit = match &node.kind {
        NodeKind::Leaf(_) => None,
        NodeKind::Branch { bit, .. } => Some(*bit),
    };
    if let Some(index) = difference.filter(|index| branch_bit.is_none_or(|branch| *index < branch))
    {
        return if bit(&incoming.key, index) {
            branch(index, node.clone(), incoming)
        } else {
            branch(index, incoming, node.clone())
        };
    }
    match &node.kind {
        NodeKind::Leaf(_) => incoming,
        NodeKind::Branch {
            bit: index,
            left,
            right,
        } => {
            if bit(&incoming.key, *index) {
                branch(*index, left.clone(), insert(right, incoming))
            } else {
                branch(*index, insert(left, incoming), right.clone())
            }
        }
    }
}
fn remove(node: &Arc<Node>, key: &[u8; 15]) -> Option<Arc<Node>> {
    match &node.kind {
        NodeKind::Leaf(_) => (node.key != *key).then(|| node.clone()),
        NodeKind::Branch {
            bit: index,
            left,
            right,
        } => {
            if differing_bit(&node.key, key).is_some_and(|difference| difference < *index) {
                return Some(node.clone());
            }
            if bit(key, *index) {
                Some(
                    remove(right, key)
                        .map_or_else(|| left.clone(), |next| branch(*index, left.clone(), next)),
                )
            } else {
                Some(
                    remove(left, key)
                        .map_or_else(|| right.clone(), |next| branch(*index, next, right.clone())),
                )
            }
        }
    }
}
#[derive(Clone)]
pub struct OccupancyEntries {
    tree: Option<Arc<Node>>,
    commitment: OccupancyCommitment,
    complete: bool,
    patch: Option<Arc<OccupancyPatch>>,
}
impl std::fmt::Debug for OccupancyEntries {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("OccupancyEntries")
            .field("commitment", &self.commitment)
            .field("complete", &self.complete)
            .field("patch", &self.patch)
            .finish()
    }
}
impl PartialEq for OccupancyEntries {
    fn eq(&self, other: &Self) -> bool {
        self.commitment == other.commitment
    }
}
impl Eq for OccupancyEntries {}
impl Default for OccupancyEntries {
    fn default() -> Self {
        Self {
            tree: None,
            commitment: empty_commitment(),
            complete: true,
            patch: None,
        }
    }
}
pub struct OccupancyIter<'a> {
    stack: Vec<&'a Node>,
}
impl<'a> Iterator for OccupancyIter<'a> {
    type Item = &'a ActorOccupancyEntry;
    fn next(&mut self) -> Option<Self::Item> {
        while let Some(node) = self.stack.pop() {
            match &node.kind {
                NodeKind::Leaf(entry) => return Some(entry),
                NodeKind::Branch { left, right, .. } => {
                    self.stack.push(right);
                    self.stack.push(left);
                }
            }
        }
        None
    }
}
impl OccupancyEntries {
    pub fn len(&self) -> usize {
        self.commitment.count
    }
    pub fn is_empty(&self) -> bool {
        self.len() == 0
    }
    pub fn commitment(&self) -> OccupancyCommitment {
        self.commitment
    }
    pub fn is_complete(&self) -> bool {
        self.complete && self.patch.is_none()
    }
    pub fn changes(&self) -> Option<&[OccupancyChange]> {
        self.patch.as_ref().map(|patch| patch.changes.as_slice())
    }
    pub fn patch_base(&self) -> Option<OccupancyCommitment> {
        self.patch.as_ref().map(|patch| patch.base)
    }
    pub fn iter(&self) -> OccupancyIter<'_> {
        OccupancyIter {
            stack: self.tree.as_deref().into_iter().collect(),
        }
    }
    pub fn get(&self, actor: &ActorId) -> Option<&ActorOccupancyEntry> {
        let key = actor_key(*actor);
        let mut node = self.tree.as_deref()?;
        loop {
            match &node.kind {
                NodeKind::Leaf(entry) => return (entry.actor == *actor).then_some(entry),
                NodeKind::Branch {
                    bit: index,
                    left,
                    right,
                } => node = if bit(&key, *index) { right } else { left },
            }
        }
    }
    pub fn valid(&self) -> bool {
        self.len() <= MAX_OCCUPANCY_ENTRIES
            && if self.complete {
                self.tree
                    .as_ref()
                    .map_or_else(empty_commitment, |node| node.commitment)
                    == self.commitment
            } else {
                self.patch.is_some()
            }
            && self.patch.as_ref().is_none_or(|patch| {
                patch.base.count <= MAX_OCCUPANCY_ENTRIES
                    && patch
                        .base
                        .count
                        .checked_sub(
                            patch
                                .changes
                                .iter()
                                .filter(|change| change.before.is_some() && change.after.is_none())
                                .count(),
                        )
                        .and_then(|count| {
                            count.checked_add(
                                patch
                                    .changes
                                    .iter()
                                    .filter(|change| {
                                        change.before.is_none() && change.after.is_some()
                                    })
                                    .count(),
                            )
                        })
                        == Some(self.commitment.count)
                    && (!patch.changes.is_empty() || patch.base == self.commitment)
                    && patch.changes.len() <= MAX_OCCUPANCY_ENTRIES
                    && patch
                        .changes
                        .windows(2)
                        .all(|pair| pair[0].actor < pair[1].actor)
                    && patch.changes.iter().all(|change| {
                        change.before != change.after
                            && change
                                .before
                                .iter()
                                .chain(change.after.iter())
                                .all(|entry| entry.actor == change.actor && entry.valid())
                    })
            })
    }
    pub fn from_sorted(entries: Vec<ActorOccupancyEntry>) -> Option<Self> {
        if entries.len() > MAX_OCCUPANCY_ENTRIES
            || entries.iter().any(|entry| !entry.valid())
            || entries
                .windows(2)
                .any(|pair| pair[0].actor >= pair[1].actor)
        {
            return None;
        }
        let mut root = Self::default();
        if !entries.is_empty() {
            root.tree = Some(build_sorted(&entries)?);
        }
        root.commitment = root
            .tree
            .as_ref()
            .map_or_else(empty_commitment, |node| node.commitment);
        Some(root)
    }
    pub fn transition(
        &self,
        changes: &std::collections::BTreeMap<ActorId, Option<ActorOccupancyEntry>>,
    ) -> Option<(Self, Self)> {
        if !self.is_complete() || !self.valid() {
            return None;
        }
        let patch = changes
            .iter()
            .filter_map(|(actor, after)| {
                let before = self.get(actor).cloned();
                (before != *after).then(|| OccupancyChange {
                    actor: *actor,
                    before,
                    after: after.clone(),
                })
            })
            .collect::<Vec<_>>();
        let next = self.apply_changes(&patch)?;
        let inverse = patch
            .iter()
            .map(|change| OccupancyChange {
                actor: change.actor,
                before: change.after.clone(),
                after: change.before.clone(),
            })
            .collect();
        let mut before = self.clone();
        before.patch = Some(Arc::new(OccupancyPatch {
            base: next.commitment,
            changes: inverse,
        }));
        let mut after = next;
        after.patch = Some(Arc::new(OccupancyPatch {
            base: self.commitment,
            changes: patch,
        }));
        Some((before, after))
    }
    fn apply_changes(&self, changes: &[OccupancyChange]) -> Option<Self> {
        if !self.is_complete() {
            return None;
        }
        let mut tree = self.tree.clone();
        for change in changes {
            if self.get(&change.actor) != change.before.as_ref() || change.before == change.after {
                return None;
            }
            tree = match &change.after {
                Some(entry) if entry.actor == change.actor && entry.valid() => Some(match tree {
                    Some(node) => insert(&node, leaf(entry.clone())),
                    None => leaf(entry.clone()),
                }),
                Some(_) => return None,
                None => tree.and_then(|node| remove(&node, &actor_key(change.actor))),
            };
        }
        let commitment = tree
            .as_ref()
            .map_or_else(empty_commitment, |node| node.commitment);
        (commitment.count <= MAX_OCCUPANCY_ENTRIES).then_some(Self {
            tree,
            commitment,
            complete: true,
            patch: None,
        })
    }
    pub fn resolve_against(&self, current: &Self) -> Option<Self> {
        if !self.valid() || !current.is_complete() || !current.valid() {
            return None;
        }
        let patch = self.patch.as_ref()?;
        if patch.base != current.commitment {
            return None;
        }
        let resolved = current.apply_changes(&patch.changes)?;
        (resolved.commitment == self.commitment).then_some(resolved)
    }
}
impl std::ops::Index<usize> for OccupancyEntries {
    type Output = ActorOccupancyEntry;
    fn index(&self, mut index: usize) -> &Self::Output {
        let mut node = self.tree.as_deref().expect("complete occupancy");
        loop {
            match &node.kind {
                NodeKind::Leaf(entry) => {
                    assert_eq!(index, 0);
                    return entry;
                }
                NodeKind::Branch { left, right, .. } => {
                    if index < left.commitment.count {
                        node = left;
                    } else {
                        index -= left.commitment.count;
                        node = right;
                    }
                }
            }
        }
    }
}
#[derive(Serialize, Deserialize)]
enum WireEntries {
    Complete {
        commitment: OccupancyCommitment,
        #[serde(deserialize_with = "bounded_vec")]
        entries: Vec<ActorOccupancyEntry>,
    },
    Delta {
        commitment: OccupancyCommitment,
        patch: OccupancyPatch,
    },
}
impl Serialize for OccupancyEntries {
    fn serialize<S: Serializer>(&self, serializer: S) -> Result<S::Ok, S::Error> {
        if !self.valid() {
            return Err(serde::ser::Error::custom("invalid occupancy root"));
        }
        match &self.patch {
            Some(patch) => WireEntries::Delta {
                commitment: self.commitment,
                patch: patch.as_ref().clone(),
            }
            .serialize(serializer),
            None if self.complete => WireEntries::Complete {
                commitment: self.commitment,
                entries: self.iter().cloned().collect(),
            }
            .serialize(serializer),
            None => Err(serde::ser::Error::custom("unmaterialized occupancy root")),
        }
    }
}
impl<'de> Deserialize<'de> for OccupancyEntries {
    fn deserialize<D: Deserializer<'de>>(deserializer: D) -> Result<Self, D::Error> {
        let value = match WireEntries::deserialize(deserializer)? {
            WireEntries::Complete {
                commitment,
                entries,
            } => {
                if entries.len() != commitment.count {
                    return Err(serde::de::Error::custom("occupancy count mismatch"));
                }
                let root = Self::from_sorted(entries)
                    .ok_or_else(|| serde::de::Error::custom("invalid complete occupancy"))?;
                if root.commitment != commitment {
                    return Err(serde::de::Error::custom("occupancy commitment mismatch"));
                }
                root
            }
            WireEntries::Delta { commitment, patch } => Self {
                tree: None,
                commitment,
                complete: false,
                patch: Some(Arc::new(patch)),
            },
        };
        if !value.valid() {
            return Err(serde::de::Error::custom("invalid occupancy delta"));
        }
        Ok(value)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::actor_occupancy::{CollisionShape, OccupancyIdentity};
    use crate::canonical_action::{LivePlayerLease, PlayerKey, SpatialEpoch};
    use crate::identity::{GlobalPlayerId, PlayerSlot, ServerId};
    use crate::time::TickStamp;
    use std::collections::BTreeMap;
    fn entry(slot: u16) -> ActorOccupancyEntry {
        let gid = GlobalPlayerId::new(ServerId(3), PlayerSlot(slot));
        let lease = LivePlayerLease {
            key: PlayerKey(uuid::Uuid::from_u128(u128::from(slot) + 1)),
            gid,
            incarnation: 4,
        };
        ActorOccupancyEntry {
            association: crate::protocol::ChunkKey::new(crate::protocol::WorldId::new(0, 7), 0, 0),
            owner: crate::realm_completion::PeerGeneration {
                server: gid.server,
                generation: 12,
            },
            cut: crate::actor_occupancy::ActorOccupancyCut {
                position: [8.0_f64, 0.0, 8.0].map(f64::to_bits),
                sample_tick: TickStamp(2),
                simulation_sequence: crate::identity::PlayerSeq(0),
            },
            actor: ActorId::Player(gid),
            identity: OccupancyIdentity::Player(lease),
            spatial_epoch: SpatialEpoch::Admission {
                lease,
                admitted_at: TickStamp(2),
            },
            shape: CollisionShape {
                offsets: [
                    [-0.3_f64, 0.0, -0.3].map(f64::to_bits),
                    [0.3_f64, 1.8, 0.3].map(f64::to_bits),
                ],
                blocks_building: true,
                hanging: false,
            },
        }
    }
    #[test]
    fn canonical_commitment_is_independent_of_update_history_and_full_wire_roundtrips() {
        let entries = (0..64).map(entry).collect::<Vec<_>>();
        let expected = OccupancyEntries::from_sorted(entries.clone()).unwrap();
        let mut actual = OccupancyEntries::default();
        for entry in entries.into_iter().rev() {
            let (_, after) = actual
                .transition(&BTreeMap::from([(entry.actor, Some(entry))]))
                .unwrap();
            actual = after.resolve_against(&actual).unwrap();
        }
        assert_eq!(actual, expected);
        assert_eq!(
            actual.iter().map(|entry| entry.actor).collect::<Vec<_>>(),
            expected.iter().map(|entry| entry.actor).collect::<Vec<_>>()
        );
        let bytes = postcard::to_allocvec(&actual).unwrap();
        let decoded: OccupancyEntries = postcard::from_bytes(&bytes).unwrap();
        assert!(decoded.is_complete());
        assert_eq!(decoded, actual);
    }
    #[test]
    fn dense_root_deltas_are_small_and_reconstruct_forward_and_inverse_exactly() {
        let original = OccupancyEntries::from_sorted((0..16384).map(entry).collect()).unwrap();
        let mut replacement = entry(127);
        replacement.shape.blocks_building = false;
        let (before, after) = original
            .transition(&BTreeMap::from([(
                replacement.actor,
                Some(replacement.clone()),
            )]))
            .unwrap();
        let NodeKind::Branch {
            right: original_right,
            ..
        } = &original.tree.as_ref().unwrap().kind
        else {
            panic!()
        };
        let NodeKind::Branch {
            right: next_right, ..
        } = &after.tree.as_ref().unwrap().kind
        else {
            panic!()
        };
        assert!(Arc::ptr_eq(original_right, next_right));
        let bytes = postcard::to_allocvec(&(before, after)).unwrap();
        assert!(bytes.len() < 2048);
        let (before, after): (OccupancyEntries, OccupancyEntries) =
            postcard::from_bytes(&bytes).unwrap();
        assert!(!before.is_complete());
        assert!(!after.is_complete());
        let current = after.resolve_against(&original).unwrap();
        assert_eq!(current.get(&replacement.actor), Some(&replacement));
        assert_eq!(before.resolve_against(&current).unwrap(), original);
        assert!(after.resolve_against(&current).is_none());
        let mut wrong_count = after.clone();
        wrong_count.commitment.count += 1;
        assert!(!wrong_count.valid());
        let mut forged = after.clone();
        forged.commitment.hash[0] ^= 1;
        assert!(forged.resolve_against(&original).is_none());
        let mut forged = after;
        Arc::make_mut(forged.patch.as_mut().unwrap()).changes[0].before = None;
        assert!(forged.resolve_against(&original).is_none());
    }
    #[test]
    fn persistent_removal_and_reinsertion_restore_canonical_root() {
        let original = OccupancyEntries::from_sorted((0..512).map(entry).collect()).unwrap();
        let changes = (0..512)
            .step_by(2)
            .map(|slot| (entry(slot).actor, None))
            .collect();
        let (inverse, forward) = original.transition(&changes).unwrap();
        let removed = forward.resolve_against(&original).unwrap();
        assert_eq!(removed.len(), 256);
        assert!(
            removed
                .iter()
                .all(|entry| if let ActorId::Player(id) = entry.actor {
                    id.player.0 % 2 == 1
                } else {
                    false
                })
        );
        assert_eq!(inverse.resolve_against(&removed).unwrap(), original);
    }
    #[test]
    fn compact_transition_composes_different_prefixes_and_identity() {
        let original = OccupancyEntries::from_sorted((0..4096).map(entry).collect()).unwrap();
        let (_, first) = original
            .transition(&BTreeMap::from([(entry(7).actor, None)]))
            .unwrap();
        let first = first.resolve_against(&original).unwrap();
        let (_, second) = first
            .transition(&BTreeMap::from([(entry(8192).actor, Some(entry(8192)))]))
            .unwrap();
        let second = second.resolve_against(&first).unwrap();
        let (inverse, forward) = original.transition_to(&second).unwrap();
        assert!(
            postcard::to_allocvec(&(inverse.clone(), forward.clone()))
                .unwrap()
                .len()
                < 2048
        );
        assert_eq!(forward.resolve_against(&original).unwrap(), second);
        assert_eq!(inverse.resolve_against(&second).unwrap(), original);
        let (inverse, forward) = original.transition_to(&original).unwrap();
        assert_eq!(forward.resolve_against(&original).unwrap(), original);
        assert_eq!(inverse.resolve_against(&original).unwrap(), original);
    }
    #[test]
    fn compact_root_commit_and_undo_are_complete_and_atomic_after_wire_decode() {
        use crate::canonical_action::{
            ActionContext, ActionId, ActionState, AppliedWrite, Cell, ChunkPart,
            OwnedStakeholderSet, Target, TruthView, Value,
        };
        use crate::protocol::{BlockPos, ChunkKey, WorldId};
        let complete = OccupancyEntries::from_sorted((0..128).map(entry).collect()).unwrap();
        let (_, next) = complete
            .transition(&BTreeMap::from([(entry(3).actor, None)]))
            .unwrap();
        let next = next.resolve_against(&complete).unwrap();
        let chunk = ChunkKey::new(WorldId::new(0, 2), 0, 0);
        let root = Target::Chunk {
            key: chunk,
            part: ChunkPart::ActorOccupancy,
            inventory: None,
        };
        let block = Target::Chunk {
            key: chunk,
            part: ChunkPart::Block(BlockPos { x: 1, y: 64, z: 1 }),
            inventory: None,
        };
        let before = Cell {
            revision: 7,
            value: Value::ActorOccupancy(crate::actor_occupancy::ActorOccupancyRoot {
                entries: complete,
            }),
        };
        let after = Cell {
            revision: 9,
            value: Value::ActorOccupancy(crate::actor_occupancy::ActorOccupancyRoot {
                entries: next,
            }),
        };
        let (before_patch, after_patch) =
            crate::actor_occupancy::compact_transition(&before, &after).unwrap();
        let original = BTreeMap::from([
            (root, before.clone()),
            (
                block,
                Cell {
                    revision: 0,
                    value: Value::Block(0),
                },
            ),
        ]);
        let writes = vec![
            AppliedWrite {
                target: block,
                before: original.get(&block).cloned(),
                after: Some(Cell {
                    revision: 1,
                    value: Value::Block(1),
                }),
            },
            AppliedWrite {
                target: root,
                before: Some(before_patch),
                after: Some(after_patch),
            },
        ];
        let writes: Vec<AppliedWrite> =
            postcard::from_bytes(&postcard::to_allocvec(&writes).unwrap()).unwrap();
        let context = ActionContext {
            id: ActionId {
                actor: ActorId::Peer(ServerId(1)),
                incarnation: 1,
                sequence: 1,
                family: 1,
            },
            tick: TickStamp(3),
            actor_chunk: Some(chunk),
        };
        for view in [TruthView::Local, TruthView::Ground] {
            let mut state = OwnedStakeholderSet {
                local: original.clone(),
                ground: original.clone(),
            };
            state.commit(context, view, &writes).unwrap();
            assert_eq!(state.read(view, root), Some(after.clone()));
            let Value::ActorOccupancy(current) = state.read(view, root).unwrap().value else {
                panic!()
            };
            assert!(current.is_complete());
            let inverse = writes
                .iter()
                .map(|write| AppliedWrite {
                    target: write.target,
                    before: write.after.clone(),
                    after: write.before.clone(),
                })
                .collect::<Vec<_>>();
            state.commit(context, view, &inverse).unwrap();
            assert_eq!(state.read(view, root), Some(before.clone()));
            let mut forged = writes.clone();
            let Value::ActorOccupancy(root) = &mut forged[1].after.as_mut().unwrap().value else {
                panic!()
            };
            root.entries.commitment.hash[0] ^= 1;
            assert!(state.commit(context, view, &forged).is_err());
            assert_eq!(state.local, original);
            assert_eq!(state.ground, original);
        }
    }
    #[test]
    fn malformed_delta_count_and_oversized_length_reject_before_entries() {
        let original = OccupancyEntries::from_sorted(vec![entry(1)]).unwrap();
        let (_, after) = original
            .transition(&BTreeMap::from([(entry(1).actor, None)]))
            .unwrap();
        let mut patch = after.patch.as_ref().unwrap().as_ref().clone();
        patch.base.count = MAX_OCCUPANCY_ENTRIES + 1;
        let bytes = postcard::to_allocvec(&WireEntries::Delta {
            commitment: after.commitment,
            patch,
        })
        .unwrap();
        assert!(postcard::from_bytes::<OccupancyEntries>(&bytes).is_err());
        let mut bytes = postcard::to_allocvec(&0_u8).unwrap();
        bytes.extend(postcard::to_allocvec(&empty_commitment()).unwrap());
        bytes.extend(postcard::to_allocvec(&(MAX_OCCUPANCY_ENTRIES + 1)).unwrap());
        assert!(postcard::from_bytes::<OccupancyEntries>(&bytes).is_err());
    }
}

fn bounded_count<'de, D: Deserializer<'de>>(deserializer: D) -> Result<usize, D::Error> {
    let count = usize::deserialize(deserializer)?;
    if count > MAX_OCCUPANCY_ENTRIES {
        return Err(serde::de::Error::custom("occupancy count exceeds bound"));
    }
    Ok(count)
}
fn bounded_vec<'de, D: Deserializer<'de>, T: Deserialize<'de>>(
    deserializer: D,
) -> Result<Vec<T>, D::Error> {
    struct Bounded<T>(std::marker::PhantomData<T>);
    impl<'de, T: Deserialize<'de>> serde::de::Visitor<'de> for Bounded<T> {
        type Value = Vec<T>;
        fn expecting(&self, formatter: &mut std::fmt::Formatter) -> std::fmt::Result {
            formatter.write_str("bounded occupancy entries")
        }
        fn visit_seq<A: serde::de::SeqAccess<'de>>(
            self,
            mut sequence: A,
        ) -> Result<Self::Value, A::Error> {
            if sequence
                .size_hint()
                .is_some_and(|count| count > MAX_OCCUPANCY_ENTRIES)
            {
                return Err(serde::de::Error::custom("occupancy entries exceed bound"));
            }
            let mut entries = Vec::with_capacity(sequence.size_hint().unwrap_or(0).min(4096));
            while let Some(entry) = sequence.next_element()? {
                if entries.len() == MAX_OCCUPANCY_ENTRIES {
                    return Err(serde::de::Error::custom("occupancy entries exceed bound"));
                }
                entries.push(entry);
            }
            Ok(entries)
        }
    }
    deserializer.deserialize_seq(Bounded(std::marker::PhantomData))
}

fn changed_subtree(
    node: &Node,
    remove: bool,
    changes: &mut std::collections::BTreeMap<ActorId, Option<ActorOccupancyEntry>>,
) {
    match &node.kind {
        NodeKind::Leaf(entry) => {
            changes.insert(entry.actor, (!remove).then(|| entry.clone()));
        }
        NodeKind::Branch { left, right, .. } => {
            changed_subtree(left, remove, changes);
            changed_subtree(right, remove, changes);
        }
    }
}
fn structural_changes(
    before: Option<&Node>,
    after: Option<&Node>,
    changes: &mut std::collections::BTreeMap<ActorId, Option<ActorOccupancyEntry>>,
) {
    match (before, after) {
        (None, None) => (),
        (Some(before), None) => changed_subtree(before, true, changes),
        (None, Some(after)) => changed_subtree(after, false, changes),
        (Some(before), Some(after)) => {
            if before.commitment == after.commitment {
                return;
            }
            let before_bit = match &before.kind {
                NodeKind::Leaf(_) => 120,
                NodeKind::Branch { bit, .. } => *bit,
            };
            let after_bit = match &after.kind {
                NodeKind::Leaf(_) => 120,
                NodeKind::Branch { bit, .. } => *bit,
            };
            if differing_bit(&before.key, &after.key)
                .is_some_and(|bit| bit < before_bit.min(after_bit))
            {
                changed_subtree(before, true, changes);
                changed_subtree(after, false, changes);
                return;
            }
            match (&before.kind, &after.kind) {
                (NodeKind::Leaf(_), NodeKind::Leaf(entry)) => {
                    changes.insert(entry.actor, Some(entry.clone()));
                }
                (
                    NodeKind::Branch { left, right, .. },
                    NodeKind::Branch {
                        left: next_left,
                        right: next_right,
                        ..
                    },
                ) if before_bit == after_bit => {
                    structural_changes(Some(left), Some(next_left), changes);
                    structural_changes(Some(right), Some(next_right), changes);
                }
                (NodeKind::Branch { left, right, .. }, _) if before_bit < after_bit => {
                    if bit(&after.key, before_bit) {
                        changed_subtree(left, true, changes);
                        structural_changes(Some(right), Some(after), changes);
                    } else {
                        structural_changes(Some(left), Some(after), changes);
                        changed_subtree(right, true, changes);
                    }
                }
                (_, NodeKind::Branch { left, right, .. }) => {
                    if bit(&before.key, after_bit) {
                        changed_subtree(left, false, changes);
                        structural_changes(Some(before), Some(right), changes);
                    } else {
                        structural_changes(Some(before), Some(left), changes);
                        changed_subtree(right, false, changes);
                    }
                }
                _ => unreachable!(),
            }
        }
    }
}
impl OccupancyEntries {
    pub fn transition_to(&self, after: &Self) -> Option<(Self, Self)> {
        if !self.is_complete() || !after.is_complete() || !self.valid() || !after.valid() {
            return None;
        }
        let mut changes = std::collections::BTreeMap::new();
        structural_changes(self.tree.as_deref(), after.tree.as_deref(), &mut changes);
        let (before, next) = self.transition(&changes)?;
        (next.commitment == after.commitment).then_some((before, next))
    }
}

fn build_sorted(entries: &[ActorOccupancyEntry]) -> Option<Arc<Node>> {
    if entries.len() == 1 {
        return Some(leaf(entries[0].clone()));
    }
    let first = actor_key(entries.first()?.actor);
    let last = actor_key(entries.last()?.actor);
    let index = differing_bit(&first, &last)?;
    let split = entries.partition_point(|entry| !bit(&actor_key(entry.actor), index));
    Some(branch(
        index,
        build_sorted(&entries[..split])?,
        build_sorted(&entries[split..])?,
    ))
}
